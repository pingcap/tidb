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

package workloadlearning

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// TableReadCostCache stores the cached workload learning metrics
type TableReadCostCache struct {
	TableReadCostMetrics map[int64]*TableReadCostMetrics // key: tableID
	Version              uint64
}

// WLCacheWorker the worker to cache all workload-related metrics
// Now it is also used to save the cache data of table cost metrics.
type WLCacheWorker struct {
	sysSessionPool     util.DestroyableSessionPool
	tableReadCostCache *TableReadCostCache
	sync.RWMutex
}

// NewWLCacheWorker Create a new workload learning cache worker to cache all workload-related metrics
// from storage mysql.tidb_workload_values to memory
func NewWLCacheWorker(pool util.DestroyableSessionPool) *WLCacheWorker {
	cache := &TableReadCostCache{
		TableReadCostMetrics: make(map[int64]*TableReadCostMetrics),
		Version:              0,
	}
	return &WLCacheWorker{
		pool, cache, sync.RWMutex{}}
}

// UpdateTableReadCostCache refreshes the cached workload learning metrics
func (cw *WLCacheWorker) UpdateTableReadCostCache() {
	// Get latest metrics from storage
	se, err := cw.sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn("Get system session failed when updating table cost cache", zap.Error(err))
		return
	}
	defer func() {
		if err == nil { // only recycle when no error
			cw.sysSessionPool.Put(se)
		} else {
			// Note: Otherwise, the session will be leaked.
			cw.sysSessionPool.Destroy(se)
		}
	}()

	sctx := se.(sessionctx.Context)
	exec := sctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnWorkloadLearning)

	// Whether to update table cost metrics
	// Get the latest latestVersionInStorage in the storage
	// TODO(elsa): Add the index of (category, type, version) to mysql.tidb_workload_values
	sql := `SELECT version FROM mysql.tidb_workload_values
            WHERE category = %? AND type = %?
            ORDER BY version DESC LIMIT 1`
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql, feedbackCategory, tableReadCost)
	if err != nil {
		logutil.ErrVerboseLogger().Warn("Failed to get the latest table cost version", zap.Error(err))
		return
	}
	// Case: no metrics belongs to this feedback category and type
	if len(rows) != 1 {
		logutil.BgLogger().Warn("The result of latest table cost version query is not 1",
			zap.Int("result_rows", len(rows)))
		return
	}
	// If the latest latestVersionInStorage is the same as the cached latestVersionInStorage, no need to update
	latestVersionInStorage := rows[0].GetUint64(0)
	if latestVersionInStorage <= cw.tableReadCostCache.Version {
		logutil.BgLogger().Info("The latest table cost version in storage is the same as the cached version, no need to update",
			zap.Uint64("latest_version_in_storage", latestVersionInStorage),
			zap.Uint64("cached_version", cw.tableReadCostCache.Version))
		return
	}

	// Get the latest table cost of metrics
	sql = `SELECT table_id, value FROM mysql.tidb_workload_values
            WHERE category = %? AND type = %? AND version = %?`
	rows, _, err = exec.ExecRestrictedSQL(ctx, nil, sql, feedbackCategory, tableReadCost, latestVersionInStorage)
	if err != nil {
		logutil.ErrVerboseLogger().Warn("Failed to get the latest table cost metrics",
			zap.Error(err))
		return
	}
	newMetrics := make(map[int64]*TableReadCostMetrics)
	for _, row := range rows {
		tableID := row.GetInt64(0)
		value := row.GetJSON(1).String()

		metric := &TableReadCostMetrics{}
		if err := json.Unmarshal([]byte(value), metric); err != nil {
			logutil.ErrVerboseLogger().Warn("Failed to unmarshal table cost metrics",
				zap.Int64("table_id", tableID),
				zap.String("value", value),
				zap.Error(err))
			continue
		}
		newMetrics[tableID] = metric
	}

	// Update cache atomically
	cw.updateTableReadCostCacheWithMetrics(newMetrics, latestVersionInStorage)
}

func (cw *WLCacheWorker) updateTableReadCostCacheWithMetrics(newMetrics map[int64]*TableReadCostMetrics,
	latestVersionInStorage uint64) {
	cw.RWMutex.Lock()
	defer cw.RWMutex.Unlock()
	cw.tableReadCostCache.TableReadCostMetrics = newMetrics
	cw.tableReadCostCache.Version = latestVersionInStorage
}

// GetTableReadCostMetrics returns the cached metrics for a given table ID
func (cw *WLCacheWorker) GetTableReadCostMetrics(tableID int64) *TableReadCostMetrics {
	cw.RWMutex.RLock()
	defer cw.RWMutex.RUnlock()
	metric, exists := cw.tableReadCostCache.TableReadCostMetrics[tableID]
	if !exists {
		return nil
	}
	// deep copy for metrics to protect the cache
	result := &TableReadCostMetrics{
		TableScanTime: metric.TableScanTime,
		TableMemUsage: metric.TableMemUsage,
		ReadFrequency: metric.ReadFrequency,
		TableReadCost: metric.TableReadCost,
	}
	return result
}
