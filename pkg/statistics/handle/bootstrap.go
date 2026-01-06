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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/initstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	tableinfo "github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/slice"
	"go.uber.org/zap"
)

const (
	// initStatsStep is the step to load stats by paging.
	initStatsStep = int64(500)
	// initStatsPercentageInterval is the interval to print the percentage of loading stats.
	initStatsPercentageInterval = float64(33)
)

func (*Handle) initStatsMeta4Chunk(cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) int64 {
	var (
		physicalID    int64
		maxPhysicalID int64
	)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		physicalID = row.GetInt64(1)
		maxPhysicalID = max(physicalID, maxPhysicalID)
		newHistColl := *statistics.NewHistColl(physicalID, row.GetInt64(3), row.GetInt64(2), 4, 4)
		// During the initialization phase, we need to initialize LastAnalyzeVersion with the snapshot,
		// which ensures that we don't duplicate the auto-analyze of a particular type of table.
		// When the predicate columns feature is turned on, if a table has neither predicate columns nor indexes,
		// then auto-analyze will only analyze the _row_id and refresh stats_meta,
		// but since we don't have any histograms or topn's created for _row_id at the moment.
		// So if we don't initialize LastAnalyzeVersion with the snapshot here,
		// it will stay at 0 and auto-analyze won't be able to detect that the table has been analyzed.
		// But in the future, we maybe will create some records for _row_id, see:
		// https://github.com/pingcap/tidb/issues/51098
		snapshot := row.GetUint64(4)
		lastAnalyzeVersion, lastStatsHistUpdateVersion := snapshot, snapshot
		if !row.IsNull(5) {
			lastStatsHistUpdateVersion = max(lastStatsHistUpdateVersion, row.GetUint64(5))
		}
		tbl := &statistics.Table{
			HistColl:              newHistColl,
			Version:               row.GetUint64(0),
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMapWithoutSize(),
			LastAnalyzeVersion:    lastAnalyzeVersion,
			LastStatsHistVersion:  lastStatsHistUpdateVersion,
		}
		cache.Put(physicalID, tbl) // put this table again since it is updated
	}
	return maxPhysicalID
}

func genInitStatsMetaSQL(tableIDs ...int64) string {
	selectPrefix := "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta"
	if len(tableIDs) == 0 {
		return selectPrefix
	}
	whereClausePrefix := " where table_id in ("
	inListStr := strings.Join(slice.Int64sToStrings(tableIDs), ",")
	whereClauseSuffix := ")"
	return selectPrefix + whereClausePrefix + inListStr + whereClauseSuffix
}

func (h *Handle) initStatsMeta(ctx context.Context, sctx sessionctx.Context, tableIDs ...int64) (statstypes.StatsCache, int64, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	sql := genInitStatsMetaSQL(tableIDs...)
	rc, err := util.Exec(sctx, sql)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer terror.Call(rc.Close)
	cache, err := cache.NewStatsCacheImpl(h)
	if err != nil {
		return nil, 0, err
	}
	req := rc.NewChunk(nil)
	iter := chunk.NewIterator4Chunk(req)
	var maxPhysicalID int64
	for {
		err := rc.Next(ctx, req)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		chunkMax := h.initStatsMeta4Chunk(cache, iter)
		maxPhysicalID = max(maxPhysicalID, chunkMax)
	}
	return cache, maxPhysicalID, nil
}

func (*Handle) initStatsHistograms4ChunkLite(cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
	var table *statistics.Table
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tblID := row.GetInt64(0)
		if table == nil || table.PhysicalID != tblID {
			if table != nil {
				cache.Put(table.PhysicalID, table) // put this table in the cache because all statistics of the table have been read.
			}
			var ok bool
			table, ok = cache.Get(tblID)
			if !ok {
				continue
			}
			// optimization: doesn't need to copy and can do in place changes since
			// 1. initStatsHistograms4ChunkLite is a single thread populating entries in a local cache, and later the
			// cache will be called h.Replace(cache) to be the global cache
			// 2. following logic only modifies ColAndIdxExistenceMap, so it won't impact the cache memory cost
			// calculation
		}
		isIndex := row.GetInt64(1)
		id := row.GetInt64(2)
		ndv := row.GetInt64(3)
		nullCount := row.GetInt64(5)
		statsVer := row.GetInt64(8)
		// All the objects in the table share the same stats version.
		if statsVer != statistics.Version0 {
			table.StatsVer = int(statsVer)
		}
		if isIndex > 0 {
			table.ColAndIdxExistenceMap.InsertIndex(id, statsVer != statistics.Version0)
			if statsVer != statistics.Version0 {
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, row.GetUint64(4))
			}
		} else {
			// Column stats can be synthesized when adding a column with default values, which keeps statsVer at 0 but
			// still records NDV/null counts, so mark them as existing whenever any value is present.
			table.ColAndIdxExistenceMap.InsertCol(id, statistics.IsColumnAnalyzedOrSynthesized(statsVer, ndv, nullCount))
			if statsVer != statistics.Version0 {
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, row.GetUint64(4))
			}
		}
	}
	if table != nil {
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statistics of the table have been read.
	}
}

func (h *Handle) initStatsHistograms4Chunk(is infoschema.InfoSchema, cache statstypes.StatsCache, iter *chunk.Iterator4Chunk, isCacheFull bool) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("panic when initStatsHistograms4Chunk", zap.Any("r", r),
				zap.Stack("stack"))
		}
	}()
	var (
		table        *statistics.Table
		tblInfo      tableinfo.Table
		tblInfoValid bool
	)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tblID, statsVer := row.GetInt64(0), row.GetInt64(8)
		if table == nil || table.PhysicalID != tblID {
			tblInfoValid = false
			if table != nil {
				cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
			}
			var ok bool
			// This table must be already in the cache since we load stats_meta first.
			table, ok = cache.Get(tblID)
			if !ok {
				continue
			}
			table = table.CopyAs(statistics.BothMapsWritable)
			// Fetch table info only once per table instead of once per row
			tblInfo, ok = h.TableInfoByIDForInitStats(is, tblID)
			if !ok {
				// Table not found - likely dropped but stats metadata not yet garbage collected. Skip loading stats for this table.
				statslogutil.StatsSampleLogger().Warn("table info not found during stats initialization, skipping", zap.Int64("physicalID", table.PhysicalID))
				continue
			}
			tblInfoValid = true
		}
		// Skip all rows for tables that could not find table info.
		// This happens when a table is dropped but its stats metadata is not yet garbage collected.
		if !tblInfoValid {
			continue
		}
		// All the objects in the table share the same stats version.
		if statsVer != statistics.Version0 {
			table.StatsVer = int(statsVer)
		}
		id, ndv, nullCount, version, totColSize := row.GetInt64(2), row.GetInt64(3), row.GetInt64(5), row.GetUint64(4), row.GetInt64(7)
		if row.GetInt64(1) > 0 {
			var idxInfo *model.IndexInfo
			for _, idx := range tblInfo.Meta().Indices {
				if idx.ID == id {
					idxInfo = idx
					break
				}
			}
			if idxInfo == nil {
				continue
			}

			var cms *statistics.CMSketch
			var topN *statistics.TopN
			var err error
			if !isCacheFull {
				// stats cache is full. we should not put it into cache. but we must set LastAnalyzeVersion
				cms, topN, err = statistics.DecodeCMSketchAndTopN(row.GetBytes(6), nil)
				if err != nil {
					cms = nil
					terror.Log(errors.Trace(err))
				}
			}
			hist := statistics.NewHistogram(id, ndv, nullCount, version, types.NewFieldType(mysql.TypeBlob), chunk.InitialCapacity, 0)
			index := &statistics.Index{
				Histogram:  *hist,
				CMSketch:   cms,
				TopN:       topN,
				Info:       idxInfo,
				StatsVer:   statsVer,
				PhysicalID: tblID,
			}
			if statsVer != statistics.Version0 {
				// We first set the StatsLoadedStatus as AllEvicted. when completing to load bucket, we will set it as ALlLoad.
				index.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, version)
			}
			table.SetIdx(idxInfo.ID, index)
			table.ColAndIdxExistenceMap.InsertIndex(idxInfo.ID, statsVer != statistics.Version0)
		} else {
			var colInfo *model.ColumnInfo
			for _, col := range tblInfo.Meta().Columns {
				if col.ID == id {
					colInfo = col
					break
				}
			}
			if colInfo == nil {
				continue
			}
			hist := statistics.NewHistogram(id, ndv, nullCount, version, &colInfo.FieldType, 0, totColSize)
			hist.Correlation = row.GetFloat64(9)
			col := &statistics.Column{
				Histogram:  *hist,
				PhysicalID: table.PhysicalID,
				Info:       colInfo,
				IsHandle:   tblInfo.Meta().PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				StatsVer:   statsVer,
			}
			table.SetCol(hist.ID, col)
			table.ColAndIdxExistenceMap.InsertCol(colInfo.ID, statistics.IsColumnAnalyzedOrSynthesized(statsVer, ndv, nullCount))
			if statsVer != statistics.Version0 {
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, version)
				// We will also set int primary key's loaded status to evicted.
				col.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			} else if col.NDV > 0 || col.NullCount > 0 {
				// If NDV > 0 or NullCount > 0, we also treat it as the one having its statistics. See the comments of StatsAvailable in column.go.
				// So we align its status as evicted too.
				col.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
			// Otherwise the column's stats is not initialized.
		}
	}
	if table != nil {
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
}

// nolint:fieldalignment
type genHistSQLOptions struct {
	// isPaging indicates whether to generate the SQL with paging.
	isPaging bool
	// tableRange only works when isPaging is true. It indicates the range of table IDs to load.
	tableRange [2]int64

	// tableIDs only works when isPaging is false. It indicates the list of table IDs to load.
	tableIDs []int64
}

func newGenHistSQLOptionsForPaging(tableRange [2]int64) genHistSQLOptions {
	return genHistSQLOptions{true, tableRange, nil}
}

func newGenHistSQLOptionsForTableIDs(tableIDs []int64) genHistSQLOptions {
	return genHistSQLOptions{false, [2]int64{}, tableIDs}
}

func (o genHistSQLOptions) assert() {
	if o.isPaging {
		// Must have a valid range [start, end)
		intest.Assert(o.tableRange[0] < o.tableRange[1],
			"invalid genHistSQLOptions: paging requires a valid range [start, end), got [%d, %d)",
			o.tableRange[0], o.tableRange[1])

		// Must not provide tableIDs
		intest.Assert(len(o.tableIDs) == 0,
			"invalid genHistSQLOptions: paging requires empty tableIDs, got %d items",
			len(o.tableIDs))
		return
	}

	// Non-paging mode
	// Range must be zero value
	intest.Assert(o.tableRange == [2]int64{},
		"invalid genHistSQLOptions: non-paging requires zero tableRange, got [%d, %d)",
		o.tableRange[0], o.tableRange[1])

	// tableIDs can be empty or not; optional: check non-negative IDs
	for i, id := range o.tableIDs {
		intest.Assert(id >= 0,
			"invalid genHistSQLOptions: tableIDs[%d]=%d must be non-negative", i, id)
	}
}

// genInitStatsHistogramsSQL generates the SQL to load all stats_histograms records.
// We need to read all the records since we need to do initialization of table.ColAndIdxExistenceMap.
func genInitStatsHistogramsSQL(options genHistSQLOptions) string {
	options.assert()
	selectPrefix := "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation from mysql.stats_histograms"
	orderSuffix := " order by table_id"

	// Load all records (non-paging with no specific table IDs).
	if !options.isPaging && len(options.tableIDs) == 0 {
		return selectPrefix + orderSuffix
	}

	// Paging
	if options.isPaging {
		rangeStartClause := " where table_id >= " + strconv.FormatInt(options.tableRange[0], 10)
		rangeEndClause := " and table_id < " + strconv.FormatInt(options.tableRange[1], 10)
		return selectPrefix + rangeStartClause + rangeEndClause + orderSuffix
	}

	// Non-paging with specific table IDs
	inListStr := strings.Join(slice.Int64sToStrings(options.tableIDs), ",")
	return selectPrefix + " where table_id in (" + inListStr + ")" + orderSuffix
}

func (h *Handle) initStatsHistogramsLite(ctx context.Context, sctx sessionctx.Context, cache statstypes.StatsCache, tableIDs ...int64) error {
	sql := genInitStatsHistogramsSQL(newGenHistSQLOptionsForTableIDs(tableIDs))
	rc, err := util.Exec(sctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(rc.Close)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
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
		h.initStatsHistograms4ChunkLite(cache, iter)
	}
	return nil
}

func (h *Handle) initStatsHistogramsByPaging(is infoschema.InfoSchema, cache statstypes.StatsCache, task initstats.Task, totalMemory uint64) error {
	return h.Pool.SPool().WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			return h.initStatsHistogramsByPagingWithSCtx(sctx, is, cache, task, totalMemory)
		})
	})
}

// initStatsHistogramsByPagingWithSCtx contains the core business logic for initStatsHistogramsByPaging.
// This method preserves git blame history by keeping the original logic intact.
func (h *Handle) initStatsHistogramsByPagingWithSCtx(sctx sessionctx.Context, is infoschema.InfoSchema, cache statstypes.StatsCache, task initstats.Task, totalMemory uint64) error {
	sql := genInitStatsHistogramsSQL(newGenHistSQLOptionsForPaging([2]int64{task.StartTid, task.EndTid}))
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
		h.initStatsHistograms4Chunk(is, cache, iter, isFullCache(cache, totalMemory))
	}
	return nil
}

type loadStrategy interface {
	calculateTotalTaskCnt() uint64
	generateAndSendTasks(worker *initstats.RangeWorker)
}

func newLoadStrategy(maxTableID int64, tableIDs []int64) loadStrategy {
	if len(tableIDs) == 0 {
		return newMaxTidStrategy(maxTableID)
	}
	return newTableListStrategy(tableIDs)
}

// maxTidStrategy is to load stats by paging using the max tid.
// It is used for full load. Backup and restore also use this strategy for refreshing stats.
type maxTidStrategy struct {
	maxTid int64
}

func newMaxTidStrategy(maxTid int64) maxTidStrategy {
	intest.Assert(maxTid >= 0, "maxTid should be non-negative")
	return maxTidStrategy{maxTid: maxTid}
}

func (m maxTidStrategy) calculateTotalTaskCnt() uint64 {
	intest.Assert(m.maxTid >= 0, "maxTid should be non-negative")
	maxTid := m.maxTid
	totalTaskCnt := int64(1)
	if maxTid > initStatsStep*2 {
		totalTaskCnt = maxTid / initStatsStep
	}
	return uint64(totalTaskCnt)
}

func (m maxTidStrategy) generateAndSendTasks(worker *initstats.RangeWorker) {
	tid := int64(0)
	for tid <= m.maxTid {
		worker.SendTask(initstats.Task{
			StartTid: tid,
			EndTid:   tid + initStatsStep,
		})
		tid += initStatsStep
	}
}

// tableListStrategy is to load stats for a list of table IDs.
// Used for partial loading. Applicable only when the refresh stats command is run with a specified database or table.
type tableListStrategy struct {
	tableIDs []int64
}

func newTableListStrategy(tableIDs []int64) tableListStrategy {
	return tableListStrategy{
		tableIDs: tableIDs,
	}
}

func (t tableListStrategy) calculateTotalTaskCnt() uint64 {
	intest.Assert(len(t.tableIDs) > 0, "tableIDs should not be empty")
	return uint64(len(t.tableIDs))
}

func (t tableListStrategy) generateAndSendTasks(worker *initstats.RangeWorker) {
	for _, tableID := range t.tableIDs {
		worker.SendTask(initstats.Task{
			StartTid: tableID,
			EndTid:   tableID + 1,
		})
	}
}

func (h *Handle) initStatsHistogramsConcurrently(is infoschema.InfoSchema, cache statstypes.StatsCache, totalMemory uint64, concurrency int, strategy loadStrategy) error {
	totalTaskCnt := strategy.calculateTotalTaskCnt()
	ls := initstats.NewRangeWorker(
		"histogram",
		func(task initstats.Task) error {
			return h.initStatsHistogramsByPaging(is, cache, task, totalMemory)
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

func (*Handle) initStatsTopN4Chunk(cache statstypes.StatsCache, iter *chunk.Iterator4Chunk, totalMemory uint64, tablesWithBuckets map[int64]struct{}) {
	if IsFullCacheFunc(cache, totalMemory) {
		return
	}
	markAllIndexesFullLoadIfNoBuckets := func(table *statistics.Table) {
		if _, exists := tablesWithBuckets[table.PhysicalID]; !exists {
			// If the table has no buckets, we consider all indexes are fully loaded during bootstrap.
			table.SetAllIndexFullLoadForBootstrap()
		}
	}
	affectedIndexes := make(map[*statistics.Index]struct{})
	var table *statistics.Table
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tblID := row.GetInt64(0)
		if table == nil || table.PhysicalID != tblID {
			if table != nil {
				markAllIndexesFullLoadIfNoBuckets(table)
				cache.Put(table.PhysicalID, table) // put this table in the cache because all statistics of the table have been read.
			}
			var ok bool
			table, ok = cache.Get(tblID)
			if !ok {
				continue
			}
			// existing idx histogram is modified have to use deep copy
			table = table.CopyAs(statistics.AllDataWritable)
		}
		idx := table.GetIdx(row.GetInt64(1))
		if idx == nil || (idx.CMSketch == nil && idx.StatsVer <= statistics.Version1) {
			continue
		}
		if idx.TopN == nil {
			idx.TopN = statistics.NewTopN(32)
		}
		affectedIndexes[idx] = struct{}{}
		data := make([]byte, len(row.GetBytes(2)))
		copy(data, row.GetBytes(2))
		idx.TopN.AppendTopN(data, row.GetUint64(3))
	}
	if table != nil {
		markAllIndexesFullLoadIfNoBuckets(table)
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statistics of the table have been read.
	}
	for idx := range affectedIndexes {
		idx.TopN.Sort()
	}
}

// genInitStatsTopNSQLForIndexes generates the SQL to load all stats_top_n records for indexes.
// We only need to load the indexes' since we only record the existence of columns in ColAndIdxExistenceMap.
// The stats of the column is not loaded during the bootstrap process.
func genInitStatsTopNSQLForIndexes(isPaging bool, tableRange [2]int64) string {
	selectPrefix := "select /*+ ORDER_INDEX(mysql.stats_top_n,tbl) */ HIGH_PRIORITY table_id, hist_id, value, count from mysql.stats_top_n where is_index = 1"
	orderSuffix := " order by table_id"
	if !isPaging {
		return selectPrefix + orderSuffix
	}
	intest.Assert(tableRange[0] < tableRange[1], "invalid table range")
	rangeStartClause := " and table_id >= " + strconv.FormatInt(tableRange[0], 10)
	rangeEndClause := " and table_id < " + strconv.FormatInt(tableRange[1], 10)
	return selectPrefix + rangeStartClause + rangeEndClause + orderSuffix
}

// getTablesWithBucketsInRange checks which tables in the given range have buckets.
// Returns a map where keys are table IDs that have bucket entries.
func getTablesWithBucketsInRange(sctx sessionctx.Context, tableRange [2]int64) (map[int64]struct{}, error) {
	// Query to find table_ids that have buckets in the given range
	sql := "select /*+ USE_INDEX(stats_buckets, tbl) */ distinct table_id from mysql.stats_buckets" +
		" where is_index = 1" +
		" and table_id >= " + strconv.FormatInt(tableRange[0], 10) +
		" and table_id < " + strconv.FormatInt(tableRange[1], 10)

	rc, err := util.Exec(sctx, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer terror.Call(rc.Close)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	req := rc.NewChunk(nil)
	tablesWithBuckets := make(map[int64]struct{})

	for {
		err := rc.Next(ctx, req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		iter := chunk.NewIterator4Chunk(req)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			tableID := row.GetInt64(0)
			tablesWithBuckets[tableID] = struct{}{}
		}
	}

	return tablesWithBuckets, nil
}

func (h *Handle) initStatsTopNByPaging(cache statstypes.StatsCache, task initstats.Task, totalMemory uint64) error {
	return h.Pool.SPool().WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			return h.initStatsTopNByPagingWithSCtx(sctx, cache, task, totalMemory)
		})
	})
}

// initStatsTopNByPagingWithSCtx contains the core business logic for initStatsTopNByPaging.
// This method preserves git blame history by keeping the original logic intact.
func (h *Handle) initStatsTopNByPagingWithSCtx(sctx sessionctx.Context, cache statstypes.StatsCache, task initstats.Task, totalMemory uint64) error {
	// First, get the tables with buckets in this range
	tablesWithBuckets, err := getTablesWithBucketsInRange(sctx, [2]int64{task.StartTid, task.EndTid})
	if err != nil {
		return errors.Trace(err)
	}

	sql := genInitStatsTopNSQLForIndexes(true, [2]int64{task.StartTid, task.EndTid})
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
		h.initStatsTopN4Chunk(cache, iter, totalMemory, tablesWithBuckets)
	}
	return nil
}

func (h *Handle) initStatsTopNConcurrently(cache statstypes.StatsCache, totalMemory uint64, concurrency int, strategy loadStrategy) error {
	if IsFullCacheFunc(cache, totalMemory) {
		return nil
	}
	totalTaskCnt := strategy.calculateTotalTaskCnt()
	ls := initstats.NewRangeWorker(
		"TopN",
		func(task initstats.Task) error {
			if IsFullCacheFunc(cache, totalMemory) {
				return nil
			}
			return h.initStatsTopNByPaging(cache, task, totalMemory)
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
