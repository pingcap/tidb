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
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// CalcMaxNodeCountByTableSize calculates the maximum number of nodes to execute DXF based on the table size.
func CalcMaxNodeCountByTableSize(ctx context.Context, dataSizeInBytes int64) int {
	coresPerNode := getSystemKeyspaceCPUNode(ctx)
	nodeCnt := calcMaxNodeCountByTableSize(dataSizeInBytes, int64(coresPerNode))
	logutil.Logger(ctx).Info("calculated max node count for dist task execution",
		zap.Int64("tableSize", dataSizeInBytes),
		zap.Int64("coresPerNode", coresPerNode),
		zap.Int64("nodeCnt", nodeCnt),
	)
	return int(nodeCnt)
}

func calcMaxNodeCountByTableSize(tableSizeInBytes int64, coresPerNode int64) int64 {
	if coresPerNode <= 0 {
		return 0
	}
	r := 8.0 / float64(coresPerNode)
	nodeCnt := float64(tableSizeInBytes) * r / (200 * units.GiB)
	nodeCnt = min(nodeCnt, 30*r)
	nodeCnt = max(nodeCnt, 1)
	return int64(nodeCnt)
}

// CalcMaxNodeCountByDataSize calculates the maximum number of nodes to execute DXF based on the data size.
func CalcMaxNodeCountByDataSize(ctx context.Context, dataSizeInBytes int64) int {
	coresPerNode := getSystemKeyspaceCPUNode(ctx)
	nodeCnt := calcMaxNodeCountByDataSize(dataSizeInBytes, coresPerNode)
	logutil.Logger(ctx).Info("calculated max node count for dist task execution",
		zap.Int64("storageSize", dataSizeInBytes),
		zap.Int64("coresPerNode", coresPerNode),
		zap.Int64("nodeCnt", nodeCnt),
	)
	return int(nodeCnt)
}

func calcMaxNodeCountByDataSize(dataSizeInBytes int64, coresPerNode int64) int64 {
	if coresPerNode <= 0 {
		return 0
	}
	r := 8.0 / float64(coresPerNode)
	nodeCnt := float64(dataSizeInBytes) * r / (200 * units.GiB)
	nodeCnt = min(nodeCnt, 32*r)
	nodeCnt = max(nodeCnt, 1)
	return int64(nodeCnt)
}

// CalcMaxNodeCountByStoresNum calculates the maximum number of nodes to execute DXF based on the number of stores.
func CalcMaxNodeCountByStoresNum(ctx context.Context, store kv.Storage) int {
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		logutil.Logger(ctx).Error("store does not implement tikv.Storage interface",
			zap.String("storeType", fmt.Sprintf("%T", store)))
		return 0
	}
	pdClient := tikvStore.GetRegionCache().PDClient()
	if pdClient == nil {
		logutil.Logger(ctx).Error("pd client is nil, cannot calculate max node count",
			zap.String("storeType", fmt.Sprintf("%T", store)))
		return 0
	}
	stores, err := pdClient.GetAllStores(context.Background())
	if err != nil {
		logutil.Logger(ctx).Error("failed to get all stores for calculating max node count", zap.Error(err))
		return 0
	}
	return max(3, len(stores)/3)
}

// CalcConcurrencyByDataSize calculates the concurrency based on the data size.
func CalcConcurrencyByDataSize(ctx context.Context, dataSizeInBytes int64) int {
	coresPerNode := getSystemKeyspaceCPUNode(ctx)
	return calcConcurrencyByDataSize(dataSizeInBytes, coresPerNode)
}

func calcConcurrencyByDataSize(dataSizeInBytes int64, coresPerNode int64) int {
	if dataSizeInBytes <= 0 {
		return 4
	}
	concurrency := dataSizeInBytes / (25 * units.GiB)
	concurrency = min(concurrency, coresPerNode-1)
	concurrency = max(concurrency, 1)
	return int(concurrency)
}

func getSystemKeyspaceCPUNode(ctx context.Context) int64 {
	var cpuNode int
	mgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		logutil.Logger(ctx).Warn("failed to get dxf service task manager", zap.Error(err))
		cpuNode = cpu.GetCPUCount()
	}
	cpuNode, err = mgr.GetCPUCountOfNodeByRole(ctx, handle.NextGenTargetScope)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to get cpu count of dxf service nodes", zap.Error(err))
		cpuNode = cpu.GetCPUCount()
	}
	return int64(cpuNode)
}
