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
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"context"
	"fmt"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	//max histogram(index/column) number
	maxLoadHistogram = maxMemoryLimit / 40960
	topNnum          = 20
	bucketsSize      = 256
)

func (h *Handle) initStatsMeta4Chunk(is infoschema.InfoSchema, tables map[int64]*statistics.Table, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		physicalID := row.GetInt64(1)
		table, ok := h.getTableByPhysicalID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			continue
		}
		tableInfo := table.Meta()
		newHistColl := statistics.HistColl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			Count:          row.GetInt64(3),
			ModifyCount:    row.GetInt64(2),
			Columns:        make(map[int64]*statistics.Column, len(tableInfo.Columns)),
			Indices:        make(map[int64]*statistics.Index, len(tableInfo.Indices)),
		}
		tbl := &statistics.Table{
			HistColl: newHistColl,
			Version:  row.GetUint64(0),
			Name:     getFullTableName(is, tableInfo),
		}

		//Ignore the memory usage, it will be caculate later
		tables[tbl.PhysicalID] = tbl

	}
}

func (h *Handle) initStatsMeta(is infoschema.InfoSchema) (map[int64]*statistics.Table, error) {
	sql := "select HIGH_PRIORITY version, table_id, modify_count, count from mysql.stats_meta"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	tables := make(map[int64]*statistics.Table)

	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
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
func (h *Handle) initStatsHistogramssMeta4Chunk(is infoschema.InfoSchema, tables map[int64]*statistics.Table, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		table, ok := tables[row.GetInt64(0)]
		if !ok {
			continue
		}
		id, ndv, nullCount, version, totColSize := row.GetInt64(2), row.GetInt64(3), row.GetInt64(5), row.GetUint64(4), row.GetInt64(6)
		lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
		tbl, _ := h.getTableByPhysicalID(is, table.PhysicalID)
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
			//load the meta data of every histogram
			hist := statistics.NewHistogram(id, ndv, nullCount, version, types.NewFieldType(mysql.TypeBlob), 0, 0)
			index := &statistics.Index{
				Histogram:  *hist,
				PhysicalID: table.PhysicalID,
				Info:       idxInfo,
				StatsVer:   row.GetInt64(7),
				Flag:       row.GetInt64(9),
			}
			lastAnalyzePos.Copy(&index.LastAnalyzePos)
			table.Indices[hist.ID] = index
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
			hist.Correlation = row.GetFloat64(8)
			col := &statistics.Column{
				Histogram:  *hist,
				PhysicalID: table.PhysicalID,
				Info:       colInfo,
				Count:      nullCount,
				IsHandle:   tbl.Meta().PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:       row.GetInt64(9),
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			table.Columns[hist.ID] = col
		}
	}
}

// load ALL the meta data without cm_sketch
func (h *Handle) initStatsHistogramsMeta(is infoschema.InfoSchema, tables map[int64]*statistics.Table) error {
	sql := "select HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version," +
		" null_count, tot_col_size, stats_ver, correlation, flag, last_analyze_pos " +
		"from mysql.stats_histograms"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		h.initStatsHistogramssMeta4Chunk(is, tables, iter)
	}
	return nil
}
func (h *Handle) initStatsHistograms4Chunk(is infoschema.InfoSchema, tables map[int64]*statistics.Table, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		table, ok := tables[row.GetInt64(0)]
		if !ok {
			continue
		}
		id := row.GetInt64(2)
		tbl, _ := h.getTableByPhysicalID(is, table.PhysicalID)
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
			idx := table.Indices[id]
			if idx == nil {
				continue
			}
			cms, err := statistics.DecodeCMSketch(row.GetBytes(3), nil)
			if err != nil {
				cms = nil
				terror.Log(errors.Trace(err))
			}
			idx.CMSketch = cms
		}
	}
}

func (h *Handle) initStatsHistograms(is infoschema.InfoSchema, tables map[int64]*statistics.Table) error {
	// indcies should be load first
	// is_index = 1 load first
	sql := "select HIGH_PRIORITY table_id, is_index, hist_id, cm_sketch " +
		"from mysql.stats_histograms where is_index = 1 " +
		fmt.Sprintf("order by table_id, hist_id limit %d", maxLoadHistogram)
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		h.initStatsHistograms4Chunk(is, tables, iter)
	}
	return nil
}

func (h *Handle) initStatsTopN4Chunk(tables map[int64]*statistics.Table, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		table, ok := tables[row.GetInt64(0)]
		if !ok {
			continue
		}
		idx, ok := table.Indices[row.GetInt64(1)]
		// if idx.CMSketch == nil, the index is not loaded.
		if !ok || idx.CMSketch == nil {
			continue
		}
		data := make([]byte, len(row.GetBytes(2)))
		copy(data, row.GetBytes(2))
		idx.CMSketch.AppendTopN(data, row.GetUint64(3))
	}
}

func (h *Handle) initStatsTopN(tables map[int64]*statistics.Table) error {
	// default topN = 20
	// 54000 = 26215*20
	sql := "select HIGH_PRIORITY table_id, hist_id, value, count " +
		"from mysql.stats_top_n " +
		fmt.Sprintf("where is_index = 1 order by table_id, hist_id limit %d", maxLoadHistogram*topNnum)
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		h.initStatsTopN4Chunk(tables, iter)
	}
	return nil
}

func initStatsBuckets4MetaChunk(ctx sessionctx.Context, tables map[int64]*statistics.Table, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tableID, histID, Count := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)
		table, ok := tables[tableID]
		if !ok {
			continue
		}
		column, ok := table.Columns[histID]
		if !ok {
			continue
		}
		column.Count += Count
	}
}

//initStatsBucketsMeta load columns meta data (column count) only for every column
func (h *Handle) initStatsBucketsMeta(tables map[int64]*statistics.Table) (err error) {

	sql := "select HIGH_PRIORITY table_id, hist_id, count, bucket_id " +
		" from mysql.stats_buckets where is_index = 0 order by table_id, hist_id"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		initStatsBuckets4MetaChunk(h.mu.ctx, tables, iter)
	}
	return nil
}
func initStatsBuckets4Chunk(ctx sessionctx.Context, tables map[int64]*statistics.Table, iter *chunk.Iterator4Chunk, isOverload bool) {
	var lastHist *statistics.Histogram = nil
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tableID, isIndex, histID := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)
		table, ok := tables[tableID]
		if !ok {
			continue
		}
		var lower, upper types.Datum
		var hist *statistics.Histogram
		if isIndex > 0 {
			index, ok := table.Indices[histID]
			if !ok || index.CMSketch == nil {
				continue
			}
			hist = &index.Histogram
			lower, upper = types.NewBytesDatum(row.GetBytes(5)), types.NewBytesDatum(row.GetBytes(6))
		} else {
			column, ok := table.Columns[histID]
			if !ok || !mysql.HasPriKeyFlag(column.Info.Flag) {
				continue
			}
			hist = &column.Histogram
			d := types.NewBytesDatum(row.GetBytes(5))
			var err error
			lower, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				logutil.BgLogger().Debug("decode bucket lower bound failed", zap.Error(err))
				delete(table.Columns, histID)
				continue
			}
			d = types.NewBytesDatum(row.GetBytes(6))
			upper, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				logutil.BgLogger().Debug("decode bucket upper bound failed", zap.Error(err))
				delete(table.Columns, histID)
				continue
			}
		}
		hist.AppendBucket(&lower, &upper, row.GetInt64(3), row.GetInt64(4))
		lastHist = hist
	}
	// if row number is over maxLoadHistogram,
	// delete the last data of loaded
	// the data might not cover all the bucket data of lastHist
	if lastHist != nil && isOverload {
		*lastHist = *lastHist.CopyMeta()
	}

}

func (h *Handle) initStatsBuckets(tables map[int64]*statistics.Table) (err error) {
	sql := "select HIGH_PRIORITY table_id, is_index, hist_id, count, repeats, lower_bound," +
		"upper_bound from mysql.stats_buckets " +
		fmt.Sprintf("order by is_index^1, table_id, hist_id, bucket_id limit %d", maxLoadHistogram*bucketsSize)
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		isOverload := maxLoadHistogram*256 == req.NumRows()
		initStatsBuckets4Chunk(h.mu.ctx, tables, iter, isOverload)
	}
	return nil
}
func (h *Handle) initStatsBucketsCalc(tables map[int64]*statistics.Table) (lastVersion uint64, err error) {
	lastVersion = uint64(0)
	for _, table := range tables {
		//version is loaded by initStatsHistogramsMeta
		lastVersion = mathutil.MaxUint64(lastVersion, table.Version)
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
	}
	return lastVersion, nil
}

// InitStats will init the stats cache using full load strategy.
func (h *Handle) InitStats(is infoschema.InfoSchema) (err error) {
	h.mu.Lock()
	defer func() {
		_, err1 := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), "commit")
		if err == nil && err1 != nil {
			err = err1
		}
		h.mu.Unlock()
	}()
	_, err = h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), "begin")
	if err != nil {
		return err
	}
	tables, err := h.initStatsMeta(is)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsHistogramsMeta(is, tables)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsHistograms(is, tables)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsTopN(tables)
	if err != nil {
		return err
	}
	err = h.initStatsBucketsMeta(tables)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsBuckets(tables)
	if err != nil {
		return errors.Trace(err)
	}
	version, err := h.initStatsBucketsCalc(tables)
	if err != nil {
		return errors.Trace(err)
	}
	h.statsCache.InitStatsCache(tables, version)
	return nil
}

func getFullTableName(is infoschema.InfoSchema, tblInfo *model.TableInfo) string {
	for _, schema := range is.AllSchemas() {
		if t, err := is.TableByName(schema.Name, tblInfo.Name); err == nil {
			if t.Meta().ID == tblInfo.ID {
				return schema.Name.O + "." + tblInfo.Name.O
			}
		}
	}
	return fmt.Sprintf("%d", tblInfo.ID)
}
