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
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

func (h *Handle) initStatsMeta(is infoschema.InfoSchema) (statsCache, error) {
	sql := "select version, table_id, modify_count, count from mysql.stats_meta"
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tables := make(statsCache, len(rows))
	for _, row := range rows {
		tableID := row.GetInt64(1)
		table, ok := is.TableByID(tableID)
		if !ok {
			log.Debugf("Unknown table ID %d in stats meta table, maybe it has been dropped", tableID)
			continue
		}
		tableInfo := table.Meta()
		tbl := &Table{
			TableID:     tableID,
			Columns:     make(map[int64]*Column, len(tableInfo.Columns)),
			Indices:     make(map[int64]*Index, len(tableInfo.Indices)),
			Count:       row.GetInt64(3),
			ModifyCount: row.GetInt64(2),
			Version:     row.GetUint64(0),
		}
		tables[tableID] = tbl
	}
	return tables, nil
}

func (h *Handle) initStatsHistograms(is infoschema.InfoSchema, tables statsCache) error {
	sql := "select table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch from mysql.stats_histograms"
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		table, ok := tables[row.GetInt64(0)]
		if !ok {
			continue
		}
		hist := Histogram{
			ID:                row.GetInt64(2),
			NDV:               row.GetInt64(3),
			NullCount:         row.GetInt64(5),
			LastUpdateVersion: row.GetUint64(4),
		}
		tbl, _ := is.TableByID(table.TableID)
		if row.GetInt64(1) > 0 {
			var idxInfo *model.IndexInfo
			for _, idx := range tbl.Meta().Indices {
				if idx.ID == hist.ID {
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
			table.Indices[hist.ID] = &Index{Histogram: hist, CMSketch: cms, Info: idxInfo}
		} else {
			var colInfo *model.ColumnInfo
			for _, col := range tbl.Meta().Columns {
				if col.ID == hist.ID {
					colInfo = col
					break
				}
			}
			if colInfo == nil {
				continue
			}
			table.Columns[hist.ID] = &Column{Histogram: hist, Info: colInfo}
		}
	}
	return nil
}

func (h *Handle) initStatsBuckets(tables statsCache) error {
	sql := "select table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound from mysql.stats_buckets"
	rows, fields, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		tableID, isIndex, histID, bucketID := row.GetInt64(0), row.GetInt64(1), row.GetInt64(2), row.GetInt64(3)
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
			lower, upper = row.GetDatum(6, &fields[6].Column.FieldType), row.GetDatum(7, &fields[7].Column.FieldType)
		} else {
			column, ok := table.Columns[histID]
			if !ok {
				continue
			}
			column.Count += row.GetInt64(4)
			if !mysql.HasPriKeyFlag(column.Info.Flag) {
				continue
			}
			hist = &column.Histogram
			d := row.GetDatum(6, &fields[6].Column.FieldType)
			lower, err = d.ConvertTo(h.ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				log.Debugf("decode bucket lower bound failed: %s", errors.ErrorStack(err))
				delete(table.Columns, histID)
				continue
			}
			d = row.GetDatum(7, &fields[7].Column.FieldType)
			upper, err = d.ConvertTo(h.ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				log.Debugf("decode bucket upper bound failed: %s", errors.ErrorStack(err))
				delete(table.Columns, histID)
				continue
			}
		}
		for i := len(hist.Buckets); i <= int(bucketID); i++ {
			hist.Buckets = append(hist.Buckets, Bucket{})
		}
		lowerScalar, upperScalar, commonLength := preCalculateDatumScalar(&lower, &upper)
		hist.Buckets[bucketID] = Bucket{
			Count:        row.GetInt64(4),
			UpperBound:   upper,
			LowerBound:   lower,
			Repeats:      row.GetInt64(5),
			lowerScalar:  lowerScalar,
			upperScalar:  upperScalar,
			commonPfxLen: commonLength,
		}
	}
	for _, table := range tables {
		if h.LastVersion < table.Version {
			h.LastVersion = table.Version
		}
		for _, idx := range table.Indices {
			for i := 1; i < len(idx.Buckets); i++ {
				idx.Buckets[i].Count += idx.Buckets[i-1].Count
			}
		}
		for _, col := range table.Columns {
			for i := 1; i < len(col.Buckets); i++ {
				col.Buckets[i].Count += col.Buckets[i-1].Count
			}
		}
	}
	return nil
}

// InitStats will init the stats cache using full load strategy.
func (h *Handle) InitStats(is infoschema.InfoSchema) error {
	tables, err := h.initStatsMeta(is)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsHistograms(is, tables)
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
