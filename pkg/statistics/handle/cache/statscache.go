// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a Copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"context"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/metrics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	handle_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// StatsCacheImpl implements util.StatsCache.
type StatsCacheImpl struct {
	atomic.Pointer[StatsCache]

	statsHandle types.StatsHandle
}

// NewStatsCacheImpl creates a new StatsCache.
func NewStatsCacheImpl(statsHandle types.StatsHandle) (types.StatsCache, error) {
	newCache, err := NewStatsCache()
	if err != nil {
		return nil, err
	}

	result := &StatsCacheImpl{
		statsHandle: statsHandle,
	}
	result.Store(newCache)

	return result, nil
}

// NewStatsCacheImplForTest creates a new StatsCache for test.
func NewStatsCacheImplForTest() (types.StatsCache, error) {
	return NewStatsCacheImpl(nil)
}

// Update reads stats meta from store and updates the stats map.
func (s *StatsCacheImpl) Update(ctx context.Context, is infoschema.InfoSchema, tableAndPartitionIDs ...int64) error {
	start := time.Now()
	lastVersion := s.GetNextCheckVersionWithOffset()
	var (
		skipMoveForwardStatsCache bool
		rows                      []chunk.Row
		err                       error
	)
	if err := util.CallWithSCtx(s.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		query := "SELECT version, table_id, modify_count, count, snapshot from mysql.stats_meta where version > %? "
		args := []any{lastVersion}

		if len(tableAndPartitionIDs) > 0 {
			// When updating specific tables, we skip incrementing the max stats version to avoid missing
			// delta updates for other tables. The max version only advances when doing a full update.
			skipMoveForwardStatsCache = true
			// Sort and deduplicate the table IDs to remove duplicates
			slices.Sort(tableAndPartitionIDs)
			tableAndPartitionIDs = slices.Compact(tableAndPartitionIDs)
			// Convert table IDs to strings since the SQL executor only accepts string arrays for IN clauses
			tableStringIDs := make([]string, 0, len(tableAndPartitionIDs))
			for _, tableID := range tableAndPartitionIDs {
				tableStringIDs = append(tableStringIDs, strconv.FormatInt(tableID, 10))
			}
			query += "and table_id in (%?) "
			args = append(args, tableStringIDs)
		}
		query += "order by version"
		rows, _, err = util.ExecRows(sctx, query, args...)
		return err
	}); err != nil {
		return errors.Trace(err)
	}

	tables := make([]*statistics.Table, 0, len(rows))
	deletedTableIDs := make([]int64, 0, len(rows))

	for _, row := range rows {
		version := row.GetUint64(0)
		physicalID := row.GetInt64(1)
		modifyCount := row.GetInt64(2)
		count := row.GetInt64(3)
		snapshot := row.GetUint64(4)

		// Detect the context cancel signal, since it may take a long time for the loop.
		// TODO: add context to TableInfoByID and remove this code block?
		if ctx.Err() != nil {
			return ctx.Err()
		}

		table, ok := s.statsHandle.TableInfoByID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug(
				"unknown physical ID in stats meta table, maybe it has been dropped",
				zap.Int64("ID", physicalID),
			)
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tableInfo := table.Meta()
		// If the table is not updated, we can skip it.
		if oldTbl, ok := s.Get(physicalID); ok &&
			oldTbl.Version >= version &&
			tableInfo.UpdateTS == oldTbl.TblInfoUpdateTS {
			continue
		}
		tbl, err := s.statsHandle.TableStatsFromStorage(
			tableInfo,
			physicalID,
			false,
			0,
		)
		// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
		if err != nil {
			statslogutil.StatsLogger().Error(
				"error occurred when read table stats",
				zap.String("table", tableInfo.Name.O),
				zap.Error(err),
			)
			continue
		}
		if tbl == nil {
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tbl.Version = version
		tbl.RealtimeCount = count
		tbl.ModifyCount = modifyCount
		tbl.TblInfoUpdateTS = tableInfo.UpdateTS
		// It only occurs in the following situations:
		// 1. The table has already been analyzed,
		//	but because the predicate columns feature is turned on, and it doesn't have any columns or indexes analyzed,
		//	it only analyzes _row_id and refreshes stats_meta, in which case the snapshot is not zero.
		// 2. LastAnalyzeVersion is 0 because it has never been loaded.
		// In this case, we can initialize LastAnalyzeVersion to the snapshot,
		//	otherwise auto-analyze will assume that the table has never been analyzed and try to analyze it again.
		if tbl.LastAnalyzeVersion == 0 && snapshot != 0 {
			tbl.LastAnalyzeVersion = snapshot
		}
		tables = append(tables, tbl)
	}

	s.UpdateStatsCache(types.CacheUpdate{
		Updated: tables,
		Deleted: deletedTableIDs,
		Options: types.UpdateOptions{
			SkipMoveForward: skipMoveForwardStatsCache,
		},
	})
	dur := time.Since(start)
	tidbmetrics.StatsDeltaLoadHistogram.Observe(dur.Seconds())
	return nil
}

// GetNextCheckVersionWithOffset gets the last version with offset.
func (s *StatsCacheImpl) GetNextCheckVersionWithOffset() uint64 {
	// Get the greatest version of the stats meta table.
	lastVersion := s.MaxTableStatsVersion()
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than five lease.
	offset := util.DurationToTS(5 * s.statsHandle.Lease()) // 5 lease is 15s.
	if s.MaxTableStatsVersion() >= offset {
		lastVersion = lastVersion - offset
	} else {
		lastVersion = 0
	}

	return lastVersion
}

// Replace replaces this cache.
func (s *StatsCacheImpl) Replace(cache types.StatsCache) {
	x := cache.(*StatsCacheImpl)
	s.replace(x.Load())
}

// replace replaces the cache with the new cache.
func (s *StatsCacheImpl) replace(newCache *StatsCache) {
	old := s.Swap(newCache)
	if old != nil {
		old.Close()
	}
	metrics.CostGauge.Set(float64(newCache.Cost()))
}

// UpdateStatsCache updates the cache with the new cache.
func (s *StatsCacheImpl) UpdateStatsCache(cacheUpdate types.CacheUpdate) {
	if enableQuota := config.GetGlobalConfig().Performance.EnableStatsCacheMemQuota; enableQuota {
		s.Load().Update(cacheUpdate.Updated, cacheUpdate.Deleted, cacheUpdate.Options.SkipMoveForward)
	} else {
		// TODO: remove this branch because we will always enable quota.
		newCache := s.Load().CopyAndUpdate(cacheUpdate.Updated, cacheUpdate.Deleted)
		s.replace(newCache)
	}
}

// Close closes this cache.
func (s *StatsCacheImpl) Close() {
	s.Load().Close()
}

// Clear clears this cache.
// Create a empty cache and replace the old one.
func (s *StatsCacheImpl) Clear() {
	cache, err := NewStatsCache()
	if err != nil {
		logutil.BgLogger().Warn("create stats cache failed", zap.Error(err))
		return
	}
	s.replace(cache)
}

// MemConsumed returns its memory usage.
func (s *StatsCacheImpl) MemConsumed() (size int64) {
	return s.Load().Cost()
}

// Get returns the specified table's stats.
func (s *StatsCacheImpl) Get(tableID int64) (*statistics.Table, bool) {
	failpoint.Inject("StatsCacheGetNil", func() {
		failpoint.Return(nil, false)
	})
	return s.Load().Get(tableID)
}

// Put puts this table stats into the cache.
func (s *StatsCacheImpl) Put(id int64, t *statistics.Table) {
	s.Load().put(id, t)
}

// TriggerEvict triggers the cache to evict some items.
func (s *StatsCacheImpl) TriggerEvict() {
	s.Load().TriggerEvict()
}

// MaxTableStatsVersion returns the version of the current cache, which is defined as
// the max table stats version the cache has in its lifecycle.
func (s *StatsCacheImpl) MaxTableStatsVersion() uint64 {
	return s.Load().Version()
}

// Values returns all values in this cache.
func (s *StatsCacheImpl) Values() []*statistics.Table {
	return s.Load().Values()
}

// Len returns the length of this cache.
func (s *StatsCacheImpl) Len() int {
	return s.Load().Len()
}

// SetStatsCacheCapacity sets the cache's capacity.
func (s *StatsCacheImpl) SetStatsCacheCapacity(c int64) {
	s.Load().SetCapacity(c)
}

// UpdateStatsHealthyMetrics updates stats healthy distribution metrics according to stats cache.
func (s *StatsCacheImpl) UpdateStatsHealthyMetrics() {
	distribution := make([]int64, 9)
	uneligibleAnalyze := 0
	for _, tbl := range s.Values() {
		distribution[7]++ // total table count
		isEligibleForAnalysis := tbl.IsEligibleForAnalysis()
		if !isEligibleForAnalysis {
			uneligibleAnalyze++
			continue
		}
		healthy, ok := tbl.GetStatsHealthy()
		if !ok {
			continue
		}
		if healthy < 50 {
			distribution[0]++
		} else if healthy < 55 {
			distribution[1]++
		} else if healthy < 60 {
			distribution[2]++
		} else if healthy < 70 {
			distribution[3]++
		} else if healthy < 80 {
			distribution[4]++
		} else if healthy < 100 {
			distribution[5]++
		} else {
			distribution[6]++
		}
	}
	for i, val := range distribution {
		handle_metrics.StatsHealthyGauges[i].Set(float64(val))
	}
	handle_metrics.StatsHealthyGauges[8].Set(float64(uneligibleAnalyze))
}
