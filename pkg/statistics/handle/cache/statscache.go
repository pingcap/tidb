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
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/metrics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	handle_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// LeaseOffset represents the time offset for the stats cache to load statistics from the store.
// This value is crucial to ensure that the stats are retrieved at the correct interval.
// See more at where it is used.
const LeaseOffset = 5

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

// cacheOfBatchUpdate is a cache for batch update the stats cache.
// We should not insert a item based on a item which we get from the cache long time ago.
// It may cause the cache to be inconsistent.
// The item should be quickly modified and inserted back to the cache.
type cacheOfBatchUpdate struct {
	op        func(toUpdate []*statistics.Table, toDelete []int64)
	toUpdate  []*statistics.Table
	toDelete  []int64
	batchSize int
}

const batchSizeOfUpdateBatch = 10

func (t *cacheOfBatchUpdate) internalFlush() {
	t.op(t.toUpdate, t.toDelete)
	t.toUpdate = t.toUpdate[:0]
	t.toDelete = t.toDelete[:0]
}

func (t *cacheOfBatchUpdate) addToUpdate(table *statistics.Table) {
	if len(t.toUpdate) == t.batchSize {
		t.internalFlush()
	}
	t.toUpdate = append(t.toUpdate, table)
}

func (t *cacheOfBatchUpdate) addToDelete(tableID int64) {
	if len(t.toDelete) == t.batchSize {
		t.internalFlush()
	}
	t.toDelete = append(t.toDelete, tableID)
}

func (t *cacheOfBatchUpdate) flush() {
	if len(t.toUpdate) > 0 || len(t.toDelete) > 0 {
		t.internalFlush()
	}
}

func newCacheOfBatchUpdate(batchSize int, op func(toUpdate []*statistics.Table, toDelete []int64)) cacheOfBatchUpdate {
	return cacheOfBatchUpdate{
		op:        op,
		toUpdate:  make([]*statistics.Table, 0, batchSize),
		toDelete:  make([]int64, 0, batchSize),
		batchSize: batchSize,
	}
}

// Update reads stats meta from store and updates the stats map.
func (s *StatsCacheImpl) Update(ctx context.Context, is infoschema.InfoSchema, tableAndPartitionIDs ...int64) error {
	onlyForAnalyzedTables := len(tableAndPartitionIDs) > 0
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		tidbmetrics.StatsDeltaLoadHistogram.Observe(dur.Seconds())
	}()
	lastVersion := s.GetNextCheckVersionWithOffset()
	var (
		skipMoveForwardStatsCache bool
		rows                      []chunk.Row
		err                       error
	)
	if err := util.CallWithSCtx(s.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		query := "SELECT"
		if onlyForAnalyzedTables {
			query += " /*+ use_index(mysql.stats_meta, tbl) */"
		} else {
			query += " /*+ use_index(mysql.stats_meta, idx_ver) */"
		}
		query += " version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta where version > %? "
		args := []any{lastVersion}

		if onlyForAnalyzedTables {
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

	tblToUpdateOrDelete := newCacheOfBatchUpdate(batchSizeOfUpdateBatch, func(toUpdate []*statistics.Table, toDelete []int64) {
		s.UpdateStatsCache(types.CacheUpdate{
			Updated: toUpdate,
			Deleted: toDelete,
			Options: types.UpdateOptions{
				SkipMoveForward: skipMoveForwardStatsCache,
			},
		})
	})

	for _, row := range rows {
		version := row.GetUint64(0)
		physicalID := row.GetInt64(1)
		modifyCount := row.GetInt64(2)
		count := row.GetInt64(3)
		snapshot := row.GetUint64(4)
		var latestHistUpdateVersion uint64
		if !row.IsNull(5) {
			latestHistUpdateVersion = row.GetUint64(5)
		}

		// Detect the context cancel signal, since it may take a long time for the loop.
		// TODO: add context to TableInfoByID and remove this code block?
		if ctx.Err() != nil {
			return ctx.Err()
		}

		table, ok := s.statsHandle.TableInfoByID(is, physicalID)
		if !ok {
			statslogutil.StatsLogger().Debug(
				"unknown physical ID in stats meta table, maybe it has been dropped",
				zap.Int64("ID", physicalID),
			)
			tblToUpdateOrDelete.addToDelete(physicalID)
			continue
		}
		tableInfo := table.Meta()
		// If the table is not updated, we can skip it.

		oldTbl, ok := s.Get(physicalID)
		if ok && oldTbl.Version >= version &&
			tableInfo.UpdateTS == oldTbl.TblInfoUpdateTS {
			continue
		}
		var tbl *statistics.Table
		needLoadColAndIdxStats := true
		// If the column/index stats has not been updated, we can reuse the old table stats.
		// Only need to update the count and modify count.
		if ok && latestHistUpdateVersion > 0 && oldTbl.LastStatsHistVersion >= latestHistUpdateVersion {
			tbl = oldTbl.CopyAs(statistics.MetaOnly)
			// count and modify count is updated in finalProcess
			needLoadColAndIdxStats = false
		}
		if needLoadColAndIdxStats {
			tbl, err = s.statsHandle.TableStatsFromStorage(
				tableInfo,
				physicalID,
				false,
				0,
			)
			// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
			if err != nil {
				statslogutil.StatsLogger().Warn(
					"error occurred when read table stats",
					zap.String("table", tableInfo.Name.O),
					zap.Error(err),
				)
				continue
			}
			if tbl == nil {
				tblToUpdateOrDelete.addToDelete(physicalID)
				continue
			}
		}
		tbl.Version = version
		tbl.LastStatsHistVersion = latestHistUpdateVersion
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
		tblToUpdateOrDelete.addToUpdate(tbl)
	}
	tblToUpdateOrDelete.flush()
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
	offset := util.DurationToTS(LeaseOffset * s.statsHandle.Lease())
	if lastVersion >= offset {
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
		statslogutil.StatsLogger().Warn("create stats cache failed", zap.Error(err))
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

// UpdateStatsHealthyMetrics refreshes handle_metrics.StatsHealthyGauges. We
// treat never-analyzed tables as healthy=0, count unanalyzed tables that fall below the
// auto-analyze minimal count threshold as "unneeded analyze", and keep pseudo tables as a separate category.
// The gauges satisfy: total tables = pseudo tables + unneeded analyze tables + tables in healthy buckets.
func (s *StatsCacheImpl) UpdateStatsHealthyMetrics() {
	var buckets [handle_metrics.StatsHealthyBucketCount]int64
	for _, tbl := range s.Load().Values() {
		buckets[handle_metrics.StatsHealthyBucketTotal]++

		// Pseudo entries usually disappear after DDL processing or table updates load
		// stats meta from storage, so usually you won't see many pseudo tables here.
		if tbl.Pseudo {
			buckets[handle_metrics.StatsHealthyBucketPseudo]++
			continue
		}
		// Even if a table is ineligible for analysis, count it in the distribution once it has been analyzed before.
		// Otherwise this metric may mislead users into thinking those tables are still unanalyzed.
		if !tbl.MeetAutoAnalyzeMinCnt() && !tbl.IsAnalyzed() {
			buckets[handle_metrics.StatsHealthyBucketUnneededAnalyze]++
			continue
		}
		// NOTE: Tables that haven't been analyzed yet start from 0 healthy.
		healthy, ok := tbl.GetStatsHealthy()
		if !ok {
			continue
		}
		buckets[statsHealthyBucketIndex(healthy)]++
	}
	for idx, gauge := range handle_metrics.StatsHealthyGauges {
		gauge.Set(float64(buckets[idx]))
	}
}

func statsHealthyBucketIndex(healthy int64) int {
	intest.Assert(healthy >= 0 && healthy <= 100, "healthy value out of range: %d", healthy)
	for _, cfg := range handle_metrics.HealthyBucketConfigs {
		if cfg.UpperBound <= 0 {
			continue
		}
		if healthy < cfg.UpperBound {
			return cfg.Index
		}
	}
	return handle_metrics.StatsHealthyBucket100To100
}
