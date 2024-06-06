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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/initstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// initStatsStep is the step to load stats by paging.
const initStatsStep = int64(500)

var maxTidRecord MaxTidRecord

// MaxTidRecord is to record the max tid.
type MaxTidRecord struct {
	mu  sync.Mutex
	tid atomic.Int64
}

func (h *Handle) initStatsMeta4Chunk(is infoschema.InfoSchema, cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
	var physicalID, maxPhysicalID int64
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		physicalID = row.GetInt64(1)
		// The table is read-only. Please do not modify it.
		table, ok := h.TableInfoByID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			continue
		}
		maxPhysicalID = max(physicalID, maxPhysicalID)
		tableInfo := table.Meta()
		newHistColl := statistics.HistColl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			RealtimeCount:  row.GetInt64(3),
			ModifyCount:    row.GetInt64(2),
			Columns:        make(map[int64]*statistics.Column, 4),
			Indices:        make(map[int64]*statistics.Index, 4),
		}
		tbl := &statistics.Table{
			HistColl:              newHistColl,
			Version:               row.GetUint64(0),
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(len(tableInfo.Columns), len(tableInfo.Indices)),
			IsPkIsHandle:          tableInfo.PKIsHandle,
		}
		cache.Put(physicalID, tbl) // put this table again since it is updated
	}
	maxTidRecord.mu.Lock()
	defer maxTidRecord.mu.Unlock()
	if maxTidRecord.tid.Load() < maxPhysicalID {
		maxTidRecord.tid.Store(physicalID)
	}
}

func (h *Handle) initStatsMeta(is infoschema.InfoSchema) (statstypes.StatsCache, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	sql := "select HIGH_PRIORITY version, table_id, modify_count, count from mysql.stats_meta"
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
		h.initStatsMeta4Chunk(is, tables, iter)
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

func (h *Handle) initStatsHistograms4Chunk(is infoschema.InfoSchema, cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
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
			cms, topN, err := statistics.DecodeCMSketchAndTopN(row.GetBytes(6), nil)
			if err != nil {
				cms = nil
				terror.Log(errors.Trace(err))
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
				index.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
				// The LastAnalyzeVersion is added by ALTER table so its value might be 0.
				table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, version)
			}
			lastAnalyzePos.Copy(&index.LastAnalyzePos)
			table.Indices[hist.ID] = index
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
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			table.Columns[hist.ID] = col
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

func (h *Handle) initStatsHistogramsLite(is infoschema.InfoSchema, cache statstypes.StatsCache) error {
	sql := "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl)*/ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, correlation, flag, last_analyze_pos from mysql.stats_histograms order by table_id"
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
		h.initStatsHistograms4Chunk(is, cache, iter)
	}
	return nil
}

func (h *Handle) initStatsHistogramsByPaging(is infoschema.InfoSchema, cache statstypes.StatsCache, task initstats.Task) error {
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
		h.initStatsHistograms4Chunk(is, cache, iter)
	}
	return nil
}

func (h *Handle) initStatsHistogramsConcurrency(is infoschema.InfoSchema, cache statstypes.StatsCache) error {
	var maxTid = maxTidRecord.tid.Load()
	tid := int64(0)
	ls := initstats.NewRangeWorker("histogram", func(task initstats.Task) error {
		return h.initStatsHistogramsByPaging(is, cache, task)
	}, uint64(maxTid), uint64(initStatsStep))
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

func (*Handle) initStatsTopN4Chunk(cache statstypes.StatsCache, iter *chunk.Iterator4Chunk) {
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
		idx, ok := table.Indices[row.GetInt64(1)]
		if !ok || (idx.CMSketch == nil && idx.StatsVer <= statistics.Version1) {
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

func (h *Handle) initStatsTopN(cache statstypes.StatsCache) error {
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
		h.initStatsTopN4Chunk(cache, iter)
	}
	return nil
}

func (h *Handle) initStatsTopNByPaging(cache statstypes.StatsCache, task initstats.Task) error {
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
		h.initStatsTopN4Chunk(cache, iter)
	}
	return nil
}

func (h *Handle) initStatsTopNConcurrency(cache statstypes.StatsCache) error {
	var maxTid = maxTidRecord.tid.Load()
	tid := int64(0)
	ls := initstats.NewRangeWorker("TopN", func(task initstats.Task) error {
		return h.initStatsTopNByPaging(cache, task)
	}, uint64(maxTid), uint64(initStatsStep))
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
			if idxStats, ok := table.Indices[id]; ok {
				idxStats.FMSketch = fms
			}
		} else {
			if colStats, ok := table.Columns[id]; ok {
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
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tableID, isIndex, histID := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)
		if table == nil || table.PhysicalID != tableID {
			if table != nil {
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
			index, ok := table.Indices[histID]
			if !ok {
				continue
			}
			hist = &index.Histogram
			lower, upper = types.NewBytesDatum(row.GetBytes(5)), types.NewBytesDatum(row.GetBytes(6))
		} else {
			column, ok := table.Columns[histID]
			if !ok {
				continue
			}
			if !mysql.HasPriKeyFlag(column.Info.GetFlag()) {
				continue
			}
			hist = &column.Histogram
			d := types.NewBytesDatum(row.GetBytes(5))
			var err error
			lower, err = d.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, &column.Info.FieldType)
			if err != nil {
				logutil.BgLogger().Debug("decode bucket lower bound failed", zap.Error(err))
				delete(table.Columns, histID)
				continue
			}
			d = types.NewBytesDatum(row.GetBytes(6))
			upper, err = d.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, &column.Info.FieldType)
			if err != nil {
				logutil.BgLogger().Debug("decode bucket upper bound failed", zap.Error(err))
				delete(table.Columns, histID)
				continue
			}
		}
		hist.AppendBucketWithNDV(&lower, &upper, row.GetInt64(3), row.GetInt64(4), row.GetInt64(7))
	}
	if table != nil {
		cache.Put(table.PhysicalID, table) // put this table in the cache because all statstics of the table have been read.
	}
}

func (h *Handle) initStatsBuckets(cache statstypes.StatsCache) error {
	if config.GetGlobalConfig().Performance.ConcurrentlyInitStats {
		err := h.initStatsBucketsConcurrency(cache)
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
		for _, idx := range table.Indices {
			for i := 1; i < idx.Len(); i++ {
				idx.Buckets[i].Count += idx.Buckets[i-1].Count
			}
			idx.PreCalculateScalar()
		}
		for _, col := range table.Columns {
			for i := 1; i < col.Len(); i++ {
				col.Buckets[i].Count += col.Buckets[i-1].Count
			}
			col.PreCalculateScalar()
		}
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

func (h *Handle) initStatsBucketsConcurrency(cache statstypes.StatsCache) error {
	var maxTid = maxTidRecord.tid.Load()
	tid := int64(0)
	ls := initstats.NewRangeWorker("bucket", func(task initstats.Task) error {
		return h.initStatsBucketsByPaging(cache, task)
	}, uint64(maxTid), uint64(initStatsStep))
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

// InitStatsLite initiates the stats cache. The function is liter and faster than InitStats.
// 1. Basic stats meta data is loaded.(count, modify count, etc.)
// 2. Column/index stats are loaded. (only histogram)
// 3. TopN, Bucket, FMSketch are not loaded.
func (h *Handle) InitStatsLite(is infoschema.InfoSchema) (err error) {
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
	cache, err := h.initStatsMeta(is)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("complete to load the meta in the lite mode")
	err = h.initStatsHistogramsLite(is, cache)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("complete to load the histogram in the lite mode")
	h.Replace(cache)
	return nil
}

// InitStats initiates the stats cache.
// 1. Basic stats meta data is loaded.(count, modify count, etc.)
// 2. Column/index stats are loaded. (histogram, topn, buckets, FMSketch)
func (h *Handle) InitStats(is infoschema.InfoSchema) (err error) {
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
	cache, err := h.initStatsMeta(is)
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("complete to load the meta")
	if config.GetGlobalConfig().Performance.ConcurrentlyInitStats {
		err = h.initStatsHistogramsConcurrency(is, cache)
	} else {
		err = h.initStatsHistograms(is, cache)
	}
	statslogutil.StatsLogger().Info("complete to load the histogram")
	if err != nil {
		return errors.Trace(err)
	}
	if config.GetGlobalConfig().Performance.ConcurrentlyInitStats {
		err = h.initStatsTopNConcurrency(cache)
	} else {
		err = h.initStatsTopN(cache)
	}
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
	err = h.initStatsBuckets(cache)
	statslogutil.StatsLogger().Info("complete to load the bucket")
	if err != nil {
		return errors.Trace(err)
	}
	// Set columns' stats status.
	for _, table := range cache.Values() {
		for _, col := range table.Columns {
			if col.StatsAvailable() {
				// primary key column has no stats info, because primary key's is_index is false. so it cannot load the topn
				col.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
		}
	}
	h.Replace(cache)
	return nil
}
