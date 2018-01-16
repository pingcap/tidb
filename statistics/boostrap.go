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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
)

func (h *Handle) initStatsMeta(is infoschema.InfoSchema) (statsCache, error) {
	sql := "select version, table_id, modify_count, count from mysql.stats_meta"
	rs, err := h.ctx.(sqlexec.SQLExecutor).Execute(sql)
	if len(rs) > 0 {
		defer terror.Call(rs[0].Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	tables := statsCache{}
	for {
		row, err := rs[0].Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		tableID := row.Data[1].GetInt64()
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
			Count:       row.Data[3].GetInt64(),
			ModifyCount: row.Data[2].GetInt64(),
			Version:     row.Data[0].GetUint64(),
		}
		tables[tableID] = tbl
	}
	return tables, errors.Trace(rs[0].Close())
}

func (h *Handle) initStatsHistograms(is infoschema.InfoSchema, tables statsCache) error {
	sql := "select table_id, is_index, hist_id, distinct_count, version, null_count from mysql.stats_histograms"
	rs, err := h.ctx.(sqlexec.SQLExecutor).Execute(sql)
	if len(rs) > 0 {
		defer terror.Call(rs[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	for {
		row, err := rs[0].Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		table, ok := tables[row.Data[0].GetInt64()]
		if !ok {
			continue
		}
		hist := Histogram{
			ID:                row.Data[2].GetInt64(),
			NDV:               row.Data[3].GetInt64(),
			NullCount:         row.Data[5].GetInt64(),
			LastUpdateVersion: row.Data[4].GetUint64(),
		}
		tbl, _ := is.TableByID(table.TableID)
		if row.Data[1].GetInt64() > 0 {
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
			table.Indices[hist.ID] = &Index{Histogram: hist, Info: idxInfo}
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
	return errors.Trace(rs[0].Close())
}

func (h *Handle) initStatsBuckets(tables statsCache) error {
	sql := "select table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound from mysql.stats_buckets"
	rs, err := h.ctx.(sqlexec.SQLExecutor).Execute(sql)
	if len(rs) > 0 {
		defer terror.Call(rs[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	for {
		row, err := rs[0].Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		tableID, isIndex, histID, bucketID := row.Data[0].GetInt64(), row.Data[1].GetInt64(), row.Data[2].GetInt64(), row.Data[3].GetInt64()
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
			lower, upper = row.Data[6], row.Data[7]
		} else {
			column, ok := table.Columns[histID]
			if !ok {
				continue
			}
			hist = &column.Histogram
			d := row.Data[6]
			lower, err = d.ConvertTo(h.ctx.GetSessionVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				log.Debugf("decode bucket lower bound failed: %s", errors.ErrorStack(err))
				delete(table.Columns, histID)
				continue
			}
			d = row.Data[7]
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
			Count:        row.Data[4].GetInt64(),
			UpperBound:   upper,
			LowerBound:   lower,
			Repeats:      row.Data[5].GetInt64(),
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
	return errors.Trace(rs[0].Close())
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
