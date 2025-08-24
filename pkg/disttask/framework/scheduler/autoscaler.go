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

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
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
	// To improve performance for small tasks, we assume that on an 8c machine,
	// importing 200 GiB of data requires full utilization of a single node’s resources.
	// Therefore, for every additional 25 GiB, add 1 concurrency unit as an estimate for task concurrency.
	baseSizePerConc = 25 * units.GiB
)

// CalcMaxNodeCountByTableSize calculates the maximum number of nodes to execute DXF based on the table size.
func CalcMaxNodeCountByTableSize(size int64, coresPerNode int) int {
	return calcMaxNodeCountBySize(size, coresPerNode, 30)
}

// CalcMaxNodeCountByDataSize calculates the maximum number of nodes to execute DXF based on the data size.
func CalcMaxNodeCountByDataSize(size int64, coresPerNode int) int {
	return calcMaxNodeCountBySize(size, coresPerNode, 32)
}

func calcMaxNodeCountBySize(size int64, coresPerNode int, factor float64) int {
	if coresPerNode <= 0 {
		return 0
	}
	r := baseCores / float64(coresPerNode)
	nodeCnt := float64(size) * r / baseDataSize
	nodeCnt = min(nodeCnt, factor*r)
	nodeCnt = max(nodeCnt, 1)
	return int(nodeCnt)
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

// CalcConcurrencyByDataSize calculates the concurrency based on the data size.
func CalcConcurrencyByDataSize(size int64, coresPerNode int) int {
	if size <= 0 {
		return 4
	}
	concurrency := size / baseSizePerConc
	concurrency = min(concurrency, int64(coresPerNode))
	concurrency = max(concurrency, 1)
	return int(concurrency)
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
