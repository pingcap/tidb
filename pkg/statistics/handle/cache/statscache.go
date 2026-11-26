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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/metrics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	handle_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// Cache keeps the stats of tables in memory, and loads their new versions from the storage.
type Cache struct {
	inner atomic.Pointer[statsCache]

	pool   util.SessionPool
	lease  util.LeaseGetter
	tables util.TableInfoGetter
}

// New creates a Cache that loads the stats in the sessions of pool.
func New(pool util.SessionPool, lease util.LeaseGetter, tables util.TableInfoGetter) (*Cache, error) {
	newCache, err := newStatsCache()
	if err != nil {
		return nil, err
	}

	result := &Cache{
		pool:   pool,
		lease:  lease,
		tables: tables,
	}
	result.inner.Store(newCache)

	return result, nil
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

// Update reads stats meta from store and updates the stats cache.
func (s *Cache) Update(ctx context.Context, is infoschema.InfoSchema, tableAndPartitionIDs ...int64) (err error) {
	// loadStatsWorker does not restart after a panic, so return the panic as an error.
	// The worker then logs the error and tries again at the next tick.
	defer tidbutil.Recover(tidbmetrics.LabelStats, "StatsCacheImpl.Update", func() {
		err = errors.New("panic in StatsCacheImpl.Update")
	}, false)
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
	)
	if err := util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
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

		table, ok := s.tables.TableInfoByID(is, physicalID)
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
			tbl, err = s.TableStatsFromStorage(
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
func (s *Cache) GetNextCheckVersionWithOffset() uint64 {
	// Get the greatest version of the stats meta table.
	lastVersion := s.MaxTableStatsVersion()
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than five lease.
	offset := util.DurationToTS(util.LeaseOffset * s.lease.Lease())
	if lastVersion >= offset {
		lastVersion = lastVersion - offset
	} else {
		lastVersion = 0
	}

	return lastVersion
}

// Replace replaces this cache.
func (s *Cache) Replace(cache *Cache) {
	s.replace(cache.inner.Load())
}

// replace replaces the cache with the new cache.
func (s *Cache) replace(newCache *statsCache) {
	old := s.inner.Swap(newCache)
	if old != nil {
		old.Close()
	}
	metrics.CostGauge.Set(float64(newCache.Cost()))
}

// UpdateStatsCache applies the updates and deletions to the current cache in place.
func (s *Cache) UpdateStatsCache(cacheUpdate types.CacheUpdate) {
	s.inner.Load().Update(cacheUpdate.Updated, cacheUpdate.Deleted, cacheUpdate.Options.SkipMoveForward)
}

// Close closes this cache.
func (s *Cache) Close() {
	s.inner.Load().Close()
}

// Clear clears this cache.
// Create a empty cache and replace the old one.
func (s *Cache) Clear() {
	cache, err := newStatsCache()
	if err != nil {
		statslogutil.StatsLogger().Warn("create stats cache failed", zap.Error(err))
		return
	}
	s.replace(cache)
}

// MemConsumed returns its memory usage.
func (s *Cache) MemConsumed() (size int64) {
	return s.inner.Load().Cost()
}

// TableStatsFromStorage loads table stats info from storage. It starts from the cached stats of
// the table.
func (s *Cache) TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error) {
	return storage.TableStatsFromStorageWithCache(s.pool, s, s.lease, tableInfo, physicalID, loadAll, snapshot)
}

// Get returns the specified table's stats.
func (s *Cache) Get(tableID int64) (*statistics.Table, bool) {
	failpoint.Inject("StatsCacheGetNil", func() {
		failpoint.Return(nil, false)
	})
	return s.inner.Load().Get(tableID)
}

// Put puts this table stats into the cache.
func (s *Cache) Put(id int64, t *statistics.Table) {
	s.inner.Load().put(id, t)
}

// TriggerEvict triggers the cache to evict some items.
func (s *Cache) TriggerEvict() {
	s.inner.Load().TriggerEvict()
}

// WaitForAsyncUpdates blocks until buffered asynchronous cache writes are visible to later Get calls.
// Use it after adding new items when following reads depend on them. LFU/Ristretto admits
// non-resident items asynchronously; init stats calls this between load phases/chunks.
func (s *Cache) WaitForAsyncUpdates() {
	s.inner.Load().WaitForAsyncUpdates()
}

// MaxTableStatsVersion returns the version of the current cache, which is defined as
// the max table stats version the cache has in its lifecycle.
func (s *Cache) MaxTableStatsVersion() uint64 {
	return s.inner.Load().Version()
}

// Values returns all values in this cache.
func (s *Cache) Values() []*statistics.Table {
	return s.inner.Load().Values()
}

// Len returns the length of this cache.
func (s *Cache) Len() int {
	return s.inner.Load().Len()
}

// SetStatsCacheCapacity sets the cache's capacity.
func (s *Cache) SetStatsCacheCapacity(c int64) {
	s.inner.Load().SetCapacity(c)
}

// UpdateStatsHealthyMetrics refreshes handle_metrics.StatsHealthyGauges. We
// treat never-analyzed tables as healthy=0, count unanalyzed tables that fall below the
// auto-analyze minimal count threshold as "unneeded analyze", and keep pseudo tables as a separate category.
// The gauges satisfy: total tables = pseudo tables + unneeded analyze tables + tables in healthy buckets.
func (s *Cache) UpdateStatsHealthyMetrics() {
	var buckets [handle_metrics.StatsHealthyBucketCount]int64
	for _, tbl := range s.inner.Load().Values() {
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
