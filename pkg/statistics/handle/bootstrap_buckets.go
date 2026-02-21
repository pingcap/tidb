// Copyright 2017 PingCAP, Inc.
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

package handle

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/initstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

func (*Handle) initStatsBuckets4Chunk(cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
	var table *statistics.Table
	var (
		hasErr        bool
		failedTableID int64
		failedHistID  int64
	)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tableID, histID := row.GetInt64(0), row.GetInt64(1)
		if table == nil || table.PhysicalID != tableID {
			if table != nil {
				table.SetAllIndexFullLoadForBootstrap()
				cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
			}
			var ok bool
			table, ok = cache.Get(tableID)
			if !ok {
				continue
			}
			// existing idx histogram is modified have to use deep copy
			table = table.CopyAs(statistics.AllDataWritable)
		}
		var lower, upper types.Datum
		var hist *statistics.Histogram
		index := table.GetIdx(histID)
		if index == nil {
			continue
		}
		hist = &index.Histogram
		lower, upper = types.NewBytesDatum(row.GetBytes(4) /*lower_bound*/), types.NewBytesDatum(row.GetBytes(5) /*upper_bound*/)
		hist.AppendBucketWithNDV(&lower, &upper, row.GetInt64(2) /*count*/, row.GetInt64(3) /*repeats*/, row.GetInt64(6) /*ndv*/)
	}
	if table != nil {
		table.SetAllIndexFullLoadForBootstrap()
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
	if hasErr {
		logutil.BgLogger().Error("failed to convert datum for at least one histogram bucket", zap.Int64("table ID", failedTableID), zap.Int64("column ID", failedHistID))
	}
}

// genInitStatsBucketsSQLForIndexes generates the SQL to load all stats_buckets records for indexes.
// We only need to load the indexes' since we only record the existence of columns in ColAndIdxExistenceMap.
// The stats of the column is not loaded during the bootstrap process.
func genInitStatsBucketsSQLForIndexes(isPaging bool, tableRange [2]int64) string {
	selectPrefix := "select /*+ ORDER_INDEX(mysql.stats_buckets,tbl) */ HIGH_PRIORITY table_id, hist_id, count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where is_index=1"
	orderSuffix := " order by table_id"
	if !isPaging {
		return selectPrefix + orderSuffix
	}
	intest.Assert(tableRange[0] < tableRange[1], "invalid table range")
	rangeStartClause := " and table_id >= " + strconv.FormatInt(tableRange[0], 10)
	rangeEndClause := " and table_id < " + strconv.FormatInt(tableRange[1], 10)
	return selectPrefix + rangeStartClause + rangeEndClause + orderSuffix
}

func (h *Handle) initStatsBucketsAndCalcPreScalar(cache statstypes.StatsCache, totalMemory uint64, concurrency int, strategy loadStrategy) error {
	if IsFullCacheFunc(cache, totalMemory) {
		return nil
	}
	err := h.initStatsBucketsConcurrently(cache, totalMemory, concurrency, strategy)
	if err != nil {
		return errors.Trace(err)
	}

	tables := cache.Values()
	for _, table := range tables {
		table.CalcPreScalar()
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
	return nil
}

func (h *Handle) initStatsBucketsByPaging(cache statstypes.StatsCache, task initstats.Task) error {
	return h.Pool.SPool().WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			return h.initStatsBucketsByPagingWithSCtx(sctx, cache, task)
		})
	})
}

// initStatsBucketsByPagingWithSCtx contains the core business logic for initStatsBucketsByPaging.
// This method preserves git blame history by keeping the original logic intact.
func (h *Handle) initStatsBucketsByPagingWithSCtx(sctx sessionctx.Context, cache statstypes.StatsCache, task initstats.Task) error {
	sql := genInitStatsBucketsSQLForIndexes(true, [2]int64{task.StartTid, task.EndTid})
	rc, err := util.Exec(sctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(rc.Close)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	req := rc.NewChunk(nil)
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc.Next(ctx, req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		h.initStatsBuckets4Chunk(cache, iter)
	}
	return nil
}

func (h *Handle) initStatsBucketsConcurrently(cache statstypes.StatsCache, totalMemory uint64, concurrency int, strategy loadStrategy) error {
	failpoint.Inject("mockBucketsLoadMemoryLimit", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			totalMemory = uint64(v)
		}
	})
	if IsFullCacheFunc(cache, totalMemory) {
		return nil
	}
	totalTaskCnt := strategy.calculateTotalTaskCnt()
	ls := initstats.NewRangeWorker(
		"bucket",
		func(task initstats.Task) error {
			if IsFullCacheFunc(cache, totalMemory) {
				return nil
			}
			return h.initStatsBucketsByPaging(cache, task)
		},
		concurrency,
		totalTaskCnt,
		initStatsPercentageInterval,
	)
	ls.LoadStats()
	strategy.generateAndSendTasks(ls)
	ls.Wait()
	return nil
}

// InitStatsLite initiates the stats cache. The function is liter and faster than InitStats.
// 1. Basic stats meta data is loaded.(count, modify count, etc.)
// 2. Column/index stats are marked as existing or not by initializing the table.ColAndIdxExistenceMap, based on data from mysql.stats_histograms)
// 3. TopN, Bucket, FMSketch are not loaded.
// And to work with auto analyze's needs, we need to read all the tables' stats meta into memory.
// The sync/async load of the stats or other process haven't done a full initialization of the table.ColAndIdxExistenceMap. So we need to it here.
func (h *Handle) InitStatsLite(ctx context.Context, tableIDs ...int64) error {
	return h.Pool.SPool().WithForceBlockGCSession(ctx, func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			return h.initStatsLiteWithSession(ctx, sctx, tableIDs...)
		})
	})
}

func (h *Handle) initStatsLiteWithSession(ctx context.Context, sctx sessionctx.Context, tableIDs ...int64) (err error) {
	defer func() {
		_, err1 := util.Exec(sctx, "commit")
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	_, err = util.Exec(sctx, "begin")
	if err != nil {
		return err
	}
	failpoint.Inject("beforeInitStatsLite", func() {})
	start := time.Now()
	cache, _, err := h.initStatsMeta(ctx, sctx, tableIDs...)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("Complete loading the stats meta in the lite mode", zap.Duration("duration", time.Since(start)))
	start = time.Now()
	err = h.initStatsHistogramsLite(ctx, sctx, cache, tableIDs...)
	if err != nil {
		cache.Close()
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("Complete loading the histogram in the lite mode", zap.Duration("duration", time.Since(start)))
	// If tableIDs is empty, it means we load all the tables' stats meta and histograms.
	// So we can replace the global cache with the new cache.
	if len(tableIDs) == 0 {
		h.Replace(cache)
	} else {
		tables := cache.Values()
		for _, table := range tables {
			intest.Assert(table != nil, "table should not be nil")
			h.Put(table.PhysicalID, table)
		}
		// Do not forget to close the new cache. Otherwise it would cause the goroutine leak issue.
		cache.Close()
	}
	return nil
}

// InitStats initiates the stats cache.
// 1. Basic stats meta data is loaded.(count, modify count, etc.)
// 2. Index stats are fully loaded. (histogram, topn, buckets)
// 2. Column stats are marked as existing or not by initializing the table.ColAndIdxExistenceMap, based on data from mysql.stats_histograms)
// To work with auto-analyze's needs, we need to read all stats meta info into memory.
// The sync/async load of the stats or other process haven't done a full initialization of the table.ColAndIdxExistenceMap. So we need to it here.
// If tableIDs is provided, we only load the stats for the specified tables.
func (h *Handle) InitStats(ctx context.Context, is infoschema.InfoSchema, tableIDs ...int64) error {
	return h.Pool.SPool().WithForceBlockGCSession(ctx, func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			return h.initStatsWithSession(ctx, sctx, is, tableIDs...)
		})
	})
}

func (h *Handle) initStatsWithSession(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, tableIDs ...int64) (err error) {
	initstats.InitStatsPercentage.Store(0)
	defer initstats.InitStatsPercentage.Store(100)
	totalMemory, err := memory.MemTotal()
	if err != nil {
		return err
	}
	defer func() {
		_, err1 := util.Exec(sctx, "commit")
		if err == nil && err1 != nil {
			err = err1
		}
	}()

	_, err = util.Exec(sctx, "begin")
	if err != nil {
		return err
	}
	failpoint.Inject("beforeInitStats", func() {})

	start := time.Now()
	cache, maxTableID, err := h.initStatsMeta(ctx, sctx, tableIDs...)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("Complete loading the stats meta", zap.Duration("duration", time.Since(start)))
	initstats.InitStatsPercentage.Store(initStatsPercentageInterval)

	concurrency := initstats.GetConcurrency()
	strategy := newLoadStrategy(maxTableID, tableIDs)
	start = time.Now()
	err = h.initStatsHistogramsConcurrently(is, cache, totalMemory, concurrency, strategy)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("Complete loading the histogram", zap.Duration("duration", time.Since(start)))

	start = time.Now()
	err = h.initStatsTopNConcurrently(cache, totalMemory, concurrency, strategy)
	if err != nil {
		return err
	}
	initstats.InitStatsPercentage.Store(initStatsPercentageInterval * 2)
	statslogutil.StatsLogger().Info("Complete loading the topn", zap.Duration("duration", time.Since(start)))

	start = time.Now()
	err = h.initStatsBucketsAndCalcPreScalar(cache, totalMemory, concurrency, strategy)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("Complete loading the bucket", zap.Duration("duration", time.Since(start)))

	// If tableIDs is empty, it means we load all the tables' stats.
	// So we can replace the global cache with the new cache.
	if len(tableIDs) == 0 {
		h.Replace(cache)
	} else {
		tables := cache.Values()
		for _, table := range tables {
			intest.Assert(table != nil, "table should not be nil")
			h.Put(table.PhysicalID, table)
		}
		// Do not forget to close the new cache. Otherwise it would cause the goroutine leak issue.
		cache.Close()
	}
	return nil
}

// IsFullCacheFunc is whether the cache is full or not. but we can only change it when to test
var IsFullCacheFunc func(cache statstypes.StatsCache, total uint64) bool = isFullCache

func isFullCache(cache statstypes.StatsCache, total uint64) bool {
	memQuota := vardef.StatsCacheMemQuota.Load()
	return (uint64(cache.MemConsumed()) >= total/4) || (cache.MemConsumed() >= memQuota && memQuota != 0)
}
