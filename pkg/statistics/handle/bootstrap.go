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
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/initstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

const (
	// initStatsStep is the step to load stats by paging.
	initStatsStep = int64(500)
	// initStatsPercentageInterval is the interval to print the percentage of loading stats.
	initStatsPercentageInterval = float64(33)
)

var maxTidRecord MaxTidRecord

// MaxTidRecord is to record the max tid.
type MaxTidRecord struct {
	mu  sync.Mutex
	tid atomic.Int64
}

func (h *Handle) initStatsMeta4Chunk(ctx context.Context, is infoschema.InfoSchema, cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
	var physicalID, maxPhysicalID int64
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		physicalID = row.GetInt64(1)

		// Detect the context cancel signal, since it may take a long time for the loop.
		// TODO: add context to TableInfoByID and remove this code block?
		if ctx.Err() != nil {
			return
		}

		// The table is read-only. Please do not modify it.
		table, ok := h.TableInfoByID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			continue
		}
		maxPhysicalID = max(physicalID, maxPhysicalID)
		tableInfo := table.Meta()
		newHistColl := *statistics.NewHistColl(physicalID, true, row.GetInt64(3), row.GetInt64(2), 4, 4)
		snapshot := row.GetUint64(4)
		tbl := &statistics.Table{
			HistColl:              newHistColl,
			Version:               row.GetUint64(0),
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(len(tableInfo.Columns), len(tableInfo.Indices)),
			IsPkIsHandle:          tableInfo.PKIsHandle,
			// During the initialization phase, we need to initialize LastAnalyzeVersion with the snapshot,
			// which ensures that we don't duplicate the auto-analyze of a particular type of table.
			// When the predicate columns feature is turned on, if a table has neither predicate columns nor indexes,
			// then auto-analyze will only analyze the _row_id and refresh stats_meta,
			// but since we don't have any histograms or topn's created for _row_id at the moment.
			// So if we don't initialize LastAnalyzeVersion with the snapshot here,
			// it will stay at 0 and auto-analyze won't be able to detect that the table has been analyzed.
			// But in the future, we maybe will create some records for _row_id, see:
			// https://github.com/pingcap/tidb/issues/51098
			LastAnalyzeVersion: snapshot,
		}
		cache.Put(physicalID, tbl) // put this table again since it is updated
	}
	maxTidRecord.mu.Lock()
	defer maxTidRecord.mu.Unlock()
	if maxTidRecord.tid.Load() < maxPhysicalID {
		maxTidRecord.tid.Store(physicalID)
	}
}

func (h *Handle) initStatsMeta(ctx context.Context, is infoschema.InfoSchema) (statstypes.StatsCache, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	sql := "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot from mysql.stats_meta"
	rc, err := util.Exec(h.initStatsCtx, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer terror.Call(rc.Close)
	tables, err := cache.NewStatsCacheImpl(h)
	if err != nil {
		return nil, err
	}
	req := rc.NewChunk(nil)
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc.Next(ctx, req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		h.initStatsMeta4Chunk(ctx, is, tables, iter)
	}
	return tables, nil
}

func (h *Handle) initStatsHistograms4ChunkLite(is infoschema.InfoSchema, cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
	var table *statistics.Table
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tblID := row.GetInt64(0)
		if table == nil || table.PhysicalID != tblID {
			if table != nil {
				cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
			}
			var ok bool
			table, ok = cache.Get(tblID)
			if !ok {
				continue
			}
			table = table.Copy()
		}
		isIndex := row.GetInt64(1)
		id := row.GetInt64(2)
		ndv := row.GetInt64(3)
		nullCount := row.GetInt64(5)
		statsVer := row.GetInt64(7)
		tbl, _ := h.TableInfoByID(is, table.PhysicalID)
		// All the objects in the table share the same stats version.
		if statsVer != statistics.Version0 {
			table.StatsVer = int(statsVer)
		}
		if isIndex > 0 {
			var idxInfo *model.IndexInfo
			for _, idx := range tbl.Meta().Indices {
				if idx.ID == id {
					idxInfo = idx
					break
				}
			}
			if idxInfo == nil {
				continue
			}
			table.ColAndIdxExistenceMap.InsertIndex(idxInfo.ID, idxInfo, statsVer != statistics.Version0)
			if statsVer != statistics.Version0 {
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, row.GetUint64(4))
			}
		} else {
			var colInfo *model.ColumnInfo
			for _, col := range tbl.Meta().Columns {
				if col.ID == id {
					colInfo = col
					break
				}
			}
			if colInfo == nil {
				continue
			}
			table.ColAndIdxExistenceMap.InsertCol(colInfo.ID, colInfo, statsVer != statistics.Version0 || ndv > 0 || nullCount > 0)
			if statsVer != statistics.Version0 {
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, row.GetUint64(4))
			}
		}
	}
	if table != nil {
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
}

func (h *Handle) initStatsHistograms4Chunk(is infoschema.InfoSchema, cache statstypes.StatsCache, iter *chunk.Iterator4Chunk, isCacheFull bool) {
	var table *statistics.Table
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tblID, statsVer := row.GetInt64(0), row.GetInt64(8)
		if table == nil || table.PhysicalID != tblID {
			if table != nil {
				cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
			}
			var ok bool
			table, ok = cache.Get(tblID)
			if !ok {
				continue
			}
			table = table.Copy()
		}
		// All the objects in the table share the same stats version.
		if statsVer != statistics.Version0 {
			table.StatsVer = int(statsVer)
		}
		id, ndv, nullCount, version, totColSize := row.GetInt64(2), row.GetInt64(3), row.GetInt64(5), row.GetUint64(4), row.GetInt64(7)
		lastAnalyzePos := row.GetDatum(11, types.NewFieldType(mysql.TypeBlob))
		tbl, _ := h.TableInfoByID(is, table.PhysicalID)
		if row.GetInt64(1) > 0 {
			var idxInfo *model.IndexInfo
			for _, idx := range tbl.Meta().Indices {
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
				Flag:       row.GetInt64(10),
				PhysicalID: tblID,
			}
			if statsVer != statistics.Version0 {
				// We first set the StatsLoadedStatus as AllEvicted. when completing to load bucket, we will set it as ALlLoad.
				index.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, version)
			}
			lastAnalyzePos.Copy(&index.LastAnalyzePos)
			table.SetIdx(idxInfo.ID, index)
			table.ColAndIdxExistenceMap.InsertIndex(idxInfo.ID, idxInfo, statsVer != statistics.Version0)
		} else {
			var colInfo *model.ColumnInfo
			for _, col := range tbl.Meta().Columns {
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
				IsHandle:   tbl.Meta().PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				Flag:       row.GetInt64(10),
				StatsVer:   statsVer,
			}
			// primary key column has no stats info, because primary key's is_index is false. so it cannot load the topn
			col.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			table.SetCol(hist.ID, col)
			table.ColAndIdxExistenceMap.InsertCol(colInfo.ID, colInfo, statsVer != statistics.Version0 || ndv > 0 || nullCount > 0)
			if statsVer != statistics.Version0 {
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, version)
			}
		}
	}
	if table != nil {
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
}

func (h *Handle) initStatsHistogramsLite(ctx context.Context, is infoschema.InfoSchema, cache statstypes.StatsCache) error {
	sql := "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl)*/ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, correlation, flag, last_analyze_pos from mysql.stats_histograms order by table_id"
	rc, err := util.Exec(h.initStatsCtx, sql)
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
		h.initStatsHistograms4ChunkLite(is, cache, iter)
	}
	return nil
}

func (h *Handle) initStatsHistograms(is infoschema.InfoSchema, cache statstypes.StatsCache) error {
	sql := "select  /*+ ORDER_INDEX(mysql.stats_histograms,tbl)*/ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation, flag, last_analyze_pos from mysql.stats_histograms order by table_id"
	rc, err := util.Exec(h.initStatsCtx, sql)
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
		h.initStatsHistograms4Chunk(is, cache, iter, false)
	}
	return nil
}

func (h *Handle) initStatsHistogramsByPaging(is infoschema.InfoSchema, cache statstypes.StatsCache, task initstats.Task, totalMemory uint64) error {
	se, err := h.Pool.SPool().Get()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil { // only recycle when no error
			h.Pool.SPool().Put(se)
		}
	}()

	sctx := se.(sessionctx.Context)
	// Why do we need to add `is_index=1` in the SQL?
	// because it is aligned to the `initStatsTopN` function, which only loads the topn of the index too.
	// the other will be loaded by sync load.
	sql := "select HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation, flag, last_analyze_pos from mysql.stats_histograms where table_id >= %? and table_id < %? and is_index=1"
	rc, err := util.Exec(sctx, sql, task.StartTid, task.EndTid)
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

func (h *Handle) initStatsHistogramsConcurrency(is infoschema.InfoSchema, cache statstypes.StatsCache, totalMemory uint64) error {
	var maxTid = maxTidRecord.tid.Load()
	tid := int64(0)
	ls := initstats.NewRangeWorker("histogram", func(task initstats.Task) error {
		return h.initStatsHistogramsByPaging(is, cache, task, totalMemory)
	}, uint64(maxTid), uint64(initStatsStep), initStatsPercentageInterval)
	ls.LoadStats()
	for tid <= maxTid {
		ls.SendTask(initstats.Task{
			StartTid: tid,
			EndTid:   tid + initStatsStep,
		})
		tid += initStatsStep
	}
	ls.Wait()
	return nil
}

func (*Handle) initStatsTopN4Chunk(cache statstypes.StatsCache, iter *chunk.Iterator4Chunk, totalMemory uint64) {
	if isFullCache(cache, totalMemory) {
		return
	}
	affectedIndexes := make(map[*statistics.Index]struct{})
	var table *statistics.Table
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tblID := row.GetInt64(0)
		if table == nil || table.PhysicalID != tblID {
			if table != nil {
				cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
			}
			var ok bool
			table, ok = cache.Get(tblID)
			if !ok {
				continue
			}
			table = table.Copy()
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
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
	for idx := range affectedIndexes {
		idx.TopN.Sort()
	}
}

func (h *Handle) initStatsTopN(cache statstypes.StatsCache, totalMemory uint64) error {
	sql := "select /*+ ORDER_INDEX(mysql.stats_top_n,tbl)*/  HIGH_PRIORITY table_id, hist_id, value, count from mysql.stats_top_n where is_index = 1 order by table_id"
	rc, err := util.Exec(h.initStatsCtx, sql)
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
		h.initStatsTopN4Chunk(cache, iter, totalMemory)
	}
	return nil
}

func (h *Handle) initStatsTopNByPaging(cache statstypes.StatsCache, task initstats.Task, totalMemory uint64) error {
	se, err := h.Pool.SPool().Get()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil { // only recycle when no error
			h.Pool.SPool().Put(se)
		}
	}()
	sctx := se.(sessionctx.Context)
	sql := "select HIGH_PRIORITY table_id, hist_id, value, count from mysql.stats_top_n where is_index = 1 and table_id >= %? and table_id < %? order by table_id"
	rc, err := util.Exec(sctx, sql, task.StartTid, task.EndTid)
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
		h.initStatsTopN4Chunk(cache, iter, totalMemory)
	}
	return nil
}

func (h *Handle) initStatsTopNConcurrency(cache statstypes.StatsCache, totalMemory uint64) error {
	if isFullCache(cache, totalMemory) {
		return nil
	}
	var maxTid = maxTidRecord.tid.Load()
	tid := int64(0)
	ls := initstats.NewRangeWorker("TopN", func(task initstats.Task) error {
		if isFullCache(cache, totalMemory) {
			return nil
		}
		return h.initStatsTopNByPaging(cache, task, totalMemory)
	}, uint64(maxTid), uint64(initStatsStep), initStatsPercentageInterval)
	ls.LoadStats()
	for tid <= maxTid {
		if isFullCache(cache, totalMemory) {
			break
		}
		ls.SendTask(initstats.Task{
			StartTid: tid,
			EndTid:   tid + initStatsStep,
		})
		tid += initStatsStep
	}
	ls.Wait()
	return nil
}

func (*Handle) initStatsFMSketch4Chunk(cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		table, ok := cache.Get(row.GetInt64(0))
		if !ok {
			continue
		}
		fms, err := statistics.DecodeFMSketch(row.GetBytes(3))
		if err != nil {
			fms = nil
			terror.Log(errors.Trace(err))
		}

		isIndex := row.GetInt64(1)
		id := row.GetInt64(2)
		if isIndex == 1 {
			if idxStats := table.GetIdx(id); idxStats != nil {
				idxStats.FMSketch = fms
			}
		} else {
			if colStats := table.GetCol(id); colStats != nil {
				colStats.FMSketch = fms
			}
		}
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
}

func (h *Handle) initStatsFMSketch(cache statstypes.StatsCache) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	sql := "select HIGH_PRIORITY table_id, is_index, hist_id, value from mysql.stats_fm_sketch"
	rc, err := util.Exec(h.initStatsCtx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(rc.Close)
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
		h.initStatsFMSketch4Chunk(cache, iter)
	}
	return nil
}

func (*Handle) initStatsBuckets4Chunk(cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
	var table *statistics.Table
	unspecifiedLengthTp := types.NewFieldType(mysql.TypeBlob)
	var (
		hasErr        bool
		failedTableID int64
		failedHistID  int64
	)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tableID, isIndex, histID := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)
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
			table = table.Copy()
		}
		var lower, upper types.Datum
		var hist *statistics.Histogram
		if isIndex > 0 {
			index := table.GetIdx(histID)
			if index == nil {
				continue
			}
			hist = &index.Histogram
			lower, upper = types.NewBytesDatum(row.GetBytes(5)), types.NewBytesDatum(row.GetBytes(6))
		} else {
			column := table.GetCol(histID)
			if column == nil {
				continue
			}
			if !mysql.HasPriKeyFlag(column.Info.GetFlag()) {
				continue
			}
			hist = &column.Histogram
			d := types.NewBytesDatum(row.GetBytes(5))
			var err error
			if column.Info.FieldType.EvalType() == types.ETString && column.Info.FieldType.GetType() != mysql.TypeEnum && column.Info.FieldType.GetType() != mysql.TypeSet {
				// For new collation data, when storing the bounds of the histogram, we store the collate key instead of the
				// original value.
				// But there's additional conversion logic for new collation data, and the collate key might be longer than
				// the FieldType.flen.
				// If we use the original FieldType here, there might be errors like "Invalid utf8mb4 character string"
				// or "Data too long".
				// So we change it to TypeBlob to bypass those logics here.
				lower, err = d.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, unspecifiedLengthTp)
			} else {
				lower, err = d.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, &column.Info.FieldType)
			}
			if err != nil {
				hasErr = true
				failedTableID = tableID
				failedHistID = histID
				table.DelCol(histID)
				continue
			}
			d = types.NewBytesDatum(row.GetBytes(6))
			if column.Info.FieldType.EvalType() == types.ETString && column.Info.FieldType.GetType() != mysql.TypeEnum && column.Info.FieldType.GetType() != mysql.TypeSet {
				upper, err = d.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, unspecifiedLengthTp)
			} else {
				upper, err = d.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, &column.Info.FieldType)
			}
			if err != nil {
				hasErr = true
				failedTableID = tableID
				failedHistID = histID
				table.DelCol(histID)
				continue
			}
		}
		hist.AppendBucketWithNDV(&lower, &upper, row.GetInt64(3), row.GetInt64(4), row.GetInt64(7))
	}
	if table != nil {
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
	if hasErr {
		logutil.BgLogger().Error("failed to convert datum for at least one histogram bucket", zap.Int64("table ID", failedTableID), zap.Int64("column ID", failedHistID))
	}
}

func (h *Handle) initStatsBuckets(cache statstypes.StatsCache, totalMemory uint64) error {
	if isFullCache(cache, totalMemory) {
		return nil
	}
	if config.GetGlobalConfig().Performance.ConcurrentlyInitStats {
		err := h.initStatsBucketsConcurrency(cache, totalMemory)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		sql := "select /*+ ORDER_INDEX(mysql.stats_buckets,tbl)*/ HIGH_PRIORITY table_id, is_index, hist_id, count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets order by table_id, is_index, hist_id, bucket_id"
		rc, err := util.Exec(h.initStatsCtx, sql)
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
	}
	tables := cache.Values()
	for _, table := range tables {
		table.CalcPreScalar()
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
	return nil
}

func (h *Handle) initStatsBucketsByPaging(cache statstypes.StatsCache, task initstats.Task) error {
	se, err := h.Pool.SPool().Get()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil { // only recycle when no error
			h.Pool.SPool().Put(se)
		}
	}()
	sctx := se.(sessionctx.Context)
	sql := "select HIGH_PRIORITY table_id, is_index, hist_id, count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where table_id >= %? and table_id < %? order by table_id, is_index, hist_id, bucket_id"
	rc, err := util.Exec(sctx, sql, task.StartTid, task.EndTid)
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

func (h *Handle) initStatsBucketsConcurrency(cache statstypes.StatsCache, totalMemory uint64) error {
	if isFullCache(cache, totalMemory) {
		return nil
	}
	var maxTid = maxTidRecord.tid.Load()
	tid := int64(0)
	ls := initstats.NewRangeWorker("bucket", func(task initstats.Task) error {
		if isFullCache(cache, totalMemory) {
			return nil
		}
		return h.initStatsBucketsByPaging(cache, task)
	}, uint64(maxTid), uint64(initStatsStep), initStatsPercentageInterval)
	ls.LoadStats()
	for tid <= maxTid {
		ls.SendTask(initstats.Task{
			StartTid: tid,
			EndTid:   tid + initStatsStep,
		})
		tid += initStatsStep
		if isFullCache(cache, totalMemory) {
			break
		}
	}
	ls.Wait()
	return nil
}

// InitStatsLite initiates the stats cache. The function is liter and faster than InitStats.
// 1. Basic stats meta data is loaded.(count, modify count, etc.)
// 2. Column/index stats are loaded. (only histogram)
// 3. TopN, Bucket, FMSketch are not loaded.
func (h *Handle) InitStatsLite(ctx context.Context, is infoschema.InfoSchema) (err error) {
	defer func() {
		_, err1 := util.Exec(h.initStatsCtx, "commit")
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	_, err = util.Exec(h.initStatsCtx, "begin")
	if err != nil {
		return err
	}
	failpoint.Inject("beforeInitStatsLite", func() {})
	cache, err := h.initStatsMeta(ctx, is)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("complete to load the meta in the lite mode")
	err = h.initStatsHistogramsLite(ctx, is, cache)
	if err != nil {
		cache.Close()
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("complete to load the histogram in the lite mode")
	h.Replace(cache)
	return nil
}

// InitStats initiates the stats cache.
// 1. Basic stats meta data is loaded.(count, modify count, etc.)
// 2. Column/index stats are loaded. (histogram, topn, buckets, FMSketch)
func (h *Handle) InitStats(ctx context.Context, is infoschema.InfoSchema) (err error) {
	totalMemory, err := memory.MemTotal()
	if err != nil {
		return err
	}
	loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
	defer func() {
		_, err1 := util.Exec(h.initStatsCtx, "commit")
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	_, err = util.Exec(h.initStatsCtx, "begin")
	if err != nil {
		return err
	}
	failpoint.Inject("beforeInitStats", func() {})
	cache, err := h.initStatsMeta(ctx, is)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("complete to load the meta")
	initstats.InitStatsPercentage.Store(initStatsPercentageInterval)
	if config.GetGlobalConfig().Performance.ConcurrentlyInitStats {
		err = h.initStatsHistogramsConcurrency(is, cache, totalMemory)
	} else {
		err = h.initStatsHistograms(is, cache)
	}
	statslogutil.StatsLogger().Info("complete to load the histogram")
	if err != nil {
		return errors.Trace(err)
	}
	if config.GetGlobalConfig().Performance.ConcurrentlyInitStats {
		err = h.initStatsTopNConcurrency(cache, totalMemory)
	} else {
		err = h.initStatsTopN(cache, totalMemory)
	}
	initstats.InitStatsPercentage.Store(initStatsPercentageInterval * 2)
	statslogutil.StatsLogger().Info("complete to load the topn")
	if err != nil {
		return err
	}
	if loadFMSketch {
		err = h.initStatsFMSketch(cache)
		if err != nil {
			return err
		}
		statslogutil.StatsLogger().Info("complete to load the FM Sketch")
	}
	err = h.initStatsBuckets(cache, totalMemory)
	statslogutil.StatsLogger().Info("complete to load the bucket")
	if err != nil {
		return errors.Trace(err)
	}
	h.Replace(cache)
	return nil
}

func isFullCache(cache statstypes.StatsCache, total uint64) bool {
	memQuota := variable.StatsCacheMemQuota.Load()
	return (uint64(cache.MemConsumed()) >= total/4) || (cache.MemConsumed() >= memQuota && memQuota != 0)
}
