// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"context"
	"fmt"
	"math"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/schstatus"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const (
	// Node count is calculated using the 8c machine as the baseline.
	baseCores = 8.0
	// Each node should handle at least 2 subtasks, each 100GiB data.
	// For every additional 200 GiB of data, add 1 node.
	baseDataSize = 200 * units.GiB
	// To improve performance for small tasks, we assume that on a 8c machine,
	// importing 200 GiB of data requires full utilization of a single node’s resources.
	// Therefore, for every additional 25 GiB, add 1 slot as an estimate for task's
	// required slots.
	baseSizePerConc = 25 * units.GiB
	// The maximum number of nodes that can be used for add-index.
	maxNodeCountLimitForAddIndex = 30
	// The maximum number of nodes that can be used for import-into.
	// this value is based on previous performance test, for a quite common scenario,
	// to import 100TiB data within 24 hours, we need about 32 8c nodes.
	// Note: import speed is affected by many factors, such as row length, index
	// count, table schema complexity, etc.
	maxNodeCountLimitForImportInto = 32
	// this value is calculated by 256/8, we have test on a 8c machine with 256
	// concurrency, it's fast enough for checksum. we can tune this later if needed.
	maxDistSQLConcurrencyPerCore = 32
)

// ResourceCalc is used to calculate the resource required for a DXF task
// in nextgen.
type ResourceCalc struct {
	// dataSize is the input data size in bytes.
	// for import-into, it's the size of the source data.
	// for add-index, it's the size of the table to be indexed.
	dataSize int64
	// nodeCPU is the number of CPU cores of each execution node.
	nodeCPU int
	// indexSizeRatio is the ratio of index KV size to data KV size, we get it
	// through sampling and estimation.
	// it's used by import-into, for add-index, it's always 0.
	indexSizeRatio float64
	// TuneFactors is the tuning factors for resource calculation.
	factors schstatus.TuneFactors
}

// NewRCCalcForAddIndex creates a new ResourceCalc for add-index task.
func NewRCCalcForAddIndex(dataSize int64, nodeCPU int, factors *schstatus.TuneFactors) *ResourceCalc {
	return NewRCCalc(dataSize, nodeCPU, 0, factors)
}

// NewRCCalc creates a new ResourceCalc.
func NewRCCalc(dataSize int64, nodeCPU int, indexSizeRatio float64, factors *schstatus.TuneFactors) *ResourceCalc {
	return &ResourceCalc{
		dataSize:       dataSize,
		nodeCPU:        nodeCPU,
		indexSizeRatio: indexSizeRatio,
		factors:        *factors,
	}
}

// CalcMaxNodeCountForAddIndex calculates the maximum number of nodes to execute add-index.
func (rc *ResourceCalc) CalcMaxNodeCountForAddIndex() int {
	size := rc.getAmplifiedDataSize()
	limit := rc.factors.AmplifyFactor * maxNodeCountLimitForAddIndex
	return rc.calcMaxNodeCountBySize(size, limit)
}

// CalcMaxNodeCountForImportInto calculates the maximum number of nodes to execute import-into.
func (rc *ResourceCalc) CalcMaxNodeCountForImportInto() int {
	size := rc.getAmplifiedDataSize()
	limit := rc.factors.AmplifyFactor * maxNodeCountLimitForImportInto
	return rc.calcMaxNodeCountBySize(size, limit)
}

func (rc *ResourceCalc) getAmplifiedDataSize() int64 {
	return int64(rc.factors.AmplifyFactor * (1 + rc.indexSizeRatio) * float64(rc.dataSize))
}

func (rc *ResourceCalc) calcMaxNodeCountBySize(size int64, limit float64) int {
	if rc.nodeCPU <= 0 {
		return 0
	}
	r := baseCores / float64(rc.nodeCPU)
	nodeCnt := float64(size) * r / baseDataSize
	nodeCnt = min(nodeCnt, limit*r)
	nodeCnt = max(nodeCnt, 1)
	return int(math.Round(nodeCnt))
}

// CalcMaxNodeCountByStoresNum calculates the maximum number of nodes to execute DXF based on the number of stores.
func CalcMaxNodeCountByStoresNum(ctx context.Context, store kv.Storage) int {
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		logutil.Logger(ctx).Warn("store does not implement tikv.Storage interface",
			zap.String("storeType", fmt.Sprintf("%T", store)))
		return 0
	}
	pdClient := tikvStore.GetRegionCache().PDClient()
	if pdClient == nil {
		logutil.Logger(ctx).Warn("pd client is nil, cannot calculate max node count",
			zap.String("storeType", fmt.Sprintf("%T", store)))
		return 0
	}
	stores, err := pdClient.GetAllStores(context.Background())
	if err != nil {
		logutil.Logger(ctx).Warn("failed to get all stores for calculating max node count", zap.Error(err))
		return 0
	}
	return max(3, len(stores)/3)
}

// CalcRequiredSlots calculates the required slots based on the data size.
func (rc *ResourceCalc) CalcRequiredSlots() int {
	size := rc.getAmplifiedDataSize()
	if size <= 0 {
		return 4
	}
	slots := float64(size) / baseSizePerConc
	slots = min(slots, float64(rc.nodeCPU))
	slots = max(slots, 1)
	return int(math.Round(slots))
}

// GetExecCPUNode returns the number of CPU cores on the system keyspace node.
func GetExecCPUNode(ctx context.Context) (int, error) {
	mgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		if intest.InTest {
			logutil.Logger(ctx).Warn("failed to get DXFSvcTaskMgr in test mode, returning default CPU count")
			return cpu.GetCPUCount(), nil
		}
		return 0, errors.Trace(err)
	}
	cpuNode, err := mgr.GetCPUCountOfNodeByRole(ctx, handle.GetTargetScope())
	if err != nil {
		return 0, errors.Trace(err)
	}
	return cpuNode, nil
}

// CalcDistSQLConcurrency calculates the DistSQL concurrency based on the thread
// count, max node count and CPU cores of each node.
// when maxNodeCnt <= 1,we use task concurrency times DefDistSQLScanConcurrency,
// else, we use a linear interpolation method to gradually increase the concurrency
// to maxDistSQLConcurrencyPerCore*nodeCPU.
func CalcDistSQLConcurrency(threadCnt, maxNodeCnt, nodeCPU int) int {
	if maxNodeCnt <= 1 {
		return threadCnt * vardef.DefDistSQLScanConcurrency
	}

	start := vardef.DefDistSQLScanConcurrency * nodeCPU
	interval := nodeCPU * (maxDistSQLConcurrencyPerCore - vardef.DefDistSQLScanConcurrency)
	totalStepCount := maxNodeCountLimitForImportInto - 1
	stepCount := min(totalStepCount, maxNodeCnt-1)
	return int(float64(start) + float64(interval)*float64(stepCount)/float64(totalStepCount))
}
