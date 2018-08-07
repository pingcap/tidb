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

package statistics

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

func initStatsMeta4Chunk(is infoschema.InfoSchema, pid2tid map[int64]int64, tables statsCache, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		physicalID := row.GetInt64(1)
		table, ok := getTableByPhysicalID(is, pid2tid, physicalID)
		if !ok {
			log.Debugf("Unknown physical ID %d in stats meta table, maybe it has been dropped", physicalID)
			continue
		}
		tableInfo := table.Meta()
		newHistColl := HistColl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			Count:          row.GetInt64(3),
			ModifyCount:    row.GetInt64(2),
			Columns:        make(map[int64]*Column, len(tableInfo.Columns)),
			Indices:        make(map[int64]*Index, len(tableInfo.Indices)),
			colName2Idx:    make(map[string]int64, len(tableInfo.Columns)),
			colName2ID:     make(map[string]int64, len(tableInfo.Columns)),
		}
		tbl := &Table{
			HistColl: newHistColl,
			Version:  row.GetUint64(0),
		}
		tables[physicalID] = tbl
	}
}

func (h *Handle) initStatsMeta(is infoschema.InfoSchema, pid2tid map[int64]int64) (statsCache, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sql := "select version, table_id, modify_count, count from mysql.stats_meta"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	tables := statsCache{}
	chk := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(chk)
	for {
		err := rc[0].Next(context.TODO(), chk)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		initStatsMeta4Chunk(is, pid2tid, tables, iter)
	}
	return tables, nil
}

func initStatsHistograms4Chunk(is infoschema.InfoSchema, pid2tid map[int64]int64, tables statsCache, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		table, ok := tables[row.GetInt64(0)]
		if !ok {
			continue
		}
		id, ndv, nullCount, version, totColSize := row.GetInt64(2), row.GetInt64(3), row.GetInt64(5), row.GetUint64(4), row.GetInt64(7)
		tbl, _ := getTableByPhysicalID(is, pid2tid, table.PhysicalID)
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
			cms, err := decodeCMSketch(row.GetBytes(6))
			if err != nil {
				cms = nil
				terror.Log(errors.Trace(err))
			}
			hist := NewHistogram(id, ndv, nullCount, version, types.NewFieldType(mysql.TypeBlob), chunk.InitialCapacity, 0)
			table.Indices[hist.ID] = &Index{Histogram: *hist, CMSketch: cms, Info: idxInfo, statsVer: row.GetInt64(8)}
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
			hist := NewHistogram(id, ndv, nullCount, version, &colInfo.FieldType, 0, totColSize)
			table.Columns[hist.ID] = &Column{Histogram: *hist, Info: colInfo, Count: nullCount}
		}
	}
}

func (h *Handle) initStatsHistograms(is infoschema.InfoSchema, pid2tid map[int64]int64, tables statsCache) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	sql := "select table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver from mysql.stats_histograms"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	chk := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(chk)
	for {
		err := rc[0].Next(context.TODO(), chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		initStatsHistograms4Chunk(is, pid2tid, tables, iter)
	}
	return nil
}

func initStatsBuckets4Chunk(ctx sessionctx.Context, tables statsCache, iter *chunk.Iterator4Chunk) {
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		tableID, isIndex, histID := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)
		table, ok := tables[tableID]
		if !ok {
			continue
		}
		var lower, upper types.Datum
		var hist *Histogram
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
			column.Count += row.GetInt64(3)
			if !mysql.HasPriKeyFlag(column.Info.Flag) {
				continue
			}
			hist = &column.Histogram
			d := types.NewBytesDatum(row.GetBytes(5))
			var err error
			lower, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				log.Debugf("decode bucket lower bound failed: %s", errors.ErrorStack(err))
				delete(table.Columns, histID)
				continue
			}
			d = types.NewBytesDatum(row.GetBytes(6))
			upper, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				log.Debugf("decode bucket upper bound failed: %s", errors.ErrorStack(err))
				delete(table.Columns, histID)
				continue
			}
		}
		hist.AppendBucket(&lower, &upper, row.GetInt64(3), row.GetInt64(4))
	}
}

func (h *Handle) initStatsBuckets(tables statsCache) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	sql := "select table_id, is_index, hist_id, count, repeats, lower_bound, upper_bound from mysql.stats_buckets order by table_id, is_index, hist_id, bucket_id"
	rc, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	chk := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(chk)
	for {
		err := rc[0].Next(context.TODO(), chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		initStatsBuckets4Chunk(h.mu.ctx, tables, iter)
	}
	for _, table := range tables {
		if h.mu.lastVersion < table.Version {
			h.mu.lastVersion = table.Version
		}
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
		table.buildColNameMapper()
	}
	return nil
}

// InitStats will init the stats cache using full load strategy.
func (h *Handle) InitStats(is infoschema.InfoSchema) error {
	pid2tid := buildPartitionID2TableID(is)
	tables, err := h.initStatsMeta(is, pid2tid)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsHistograms(is, pid2tid, tables)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsBuckets(tables)
	if err != nil {
		return errors.Trace(err)
	}
	h.statsCache.Store(tables)
	return nil
}

func getTableByPhysicalID(is infoschema.InfoSchema, pid2tid map[int64]int64, physicalID int64) (table.Table, bool) {
	if id, ok := pid2tid[physicalID]; ok {
		return is.TableByID(id)
	}
	return is.TableByID(physicalID)
}

func buildPartitionID2TableID(is infoschema.InfoSchema) map[int64]int64 {
	mapper := make(map[int64]int64)
	for _, db := range is.AllSchemas() {
		tbls := db.Tables
		for _, tbl := range tbls {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				continue
			}
			for _, def := range pi.Definitions {
				mapper[def.ID] = tbl.ID
			}
		}
	}
	return mapper
}
