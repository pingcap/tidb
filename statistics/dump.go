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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

// JSONTable is used for dumping statistics.
type JSONTable struct {
	DatabaseName string                 `json:"database_name"`
	TableName    string                 `json:"table_name"`
	Columns      map[string]*jsonColumn `json:"columns"`
	Indices      map[string]*jsonIndex  `json:"indices"`
	Count        int64                  `json:"count"`
	ModifyCount  int64                  `json:"modify_count"`
	Version      uint64                 `json:"version"`
}

type jsonColumn struct {
	Histogram         *tipb.Histogram `json:"histogram"`
	CMSketch          *tipb.CMSketch  `json:"cm_sketch"`
	AlreadyLoad       bool            `json:"already_load"`
	NullCount         int64           `json:"null_count"`
	LastUpdateVersion uint64          `json:"last_update_version"`
}

type jsonIndex struct {
	Histogram         *tipb.Histogram `json:"histogram"`
	CMSketch          *tipb.CMSketch  `json:"cm_sketch"`
	NullCount         int64           `json:"null_count"`
	LastUpdateVersion uint64          `json:"last_update_version"`
}

// ConvertTo converts Bucket's LowerBound and UpperBound to type t.
func (b *Bucket) ConvertTo(h *Handle, t *types.FieldType) (err error) {
	b.LowerBound, err = b.LowerBound.ConvertTo(h.ctx.GetSessionVars().StmtCtx, t)
	if err != nil {
		return errors.Trace(err)
	}
	b.UpperBound, err = b.UpperBound.ConvertTo(h.ctx.GetSessionVars().StmtCtx, t)
	return errors.Trace(err)
}

// DumpStatsToJSON dumps statistic to json.
func (h *Handle) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo) (*JSONTable, error) {
	statsTable := h.statsCache.Load().(statsCache)[tableInfo.ID]
	tbl, err := h.tableStatsFromStorage(tableInfo, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Columns:      make(map[string]*jsonColumn, len(tbl.Columns)),
		Indices:      make(map[string]*jsonIndex, len(tbl.Indices)),
		Count:        tbl.Count,
		ModifyCount:  tbl.ModifyCount,
		Version:      tbl.Version,
	}
	for id, col := range tbl.Columns {
		hist := &Histogram{
			NDV:     col.NDV,
			Buckets: make([]Bucket, len(col.Histogram.Buckets)),
		}

		// Convert Datum to BytesDatum.
		for i, v := range col.Buckets {
			hist.Buckets[i] = v
			if err := hist.Buckets[i].ConvertTo(h, types.NewFieldType(mysql.TypeBlob)); err != nil {
				return nil, errors.Trace(err)
			}
		}
		// c is the Column in Cache.
		c := statsTable.Columns[id]
		jsonCol := &jsonColumn{
			Histogram:         HistogramToProto(hist),
			AlreadyLoad:       !(c.Count > 0 && len(c.Buckets) == 0),
			NullCount:         col.NullCount,
			LastUpdateVersion: col.LastUpdateVersion,
		}
		if col.CMSketch != nil {
			jsonCol.CMSketch = CMSketchToProto(col.CMSketch)
		}
		jsonTbl.Columns[col.Info.Name.L] = jsonCol
	}

	for _, idx := range tbl.Indices {
		jsonIdx := &jsonIndex{
			Histogram:         HistogramToProto(&idx.Histogram),
			NullCount:         idx.NullCount,
			LastUpdateVersion: idx.LastUpdateVersion,
		}
		if idx.CMSketch != nil {
			jsonIdx.CMSketch = CMSketchToProto(idx.CMSketch)
		}
		jsonTbl.Indices[idx.Info.Name.L] = jsonIdx
	}
	return jsonTbl, nil
}

func getCMSketch(c *tipb.CMSketch) (cms *CMSketch) {
	if c != nil {
		cms = CMSketchFromProto(c)
		if c.Rows[0] == nil {
			return cms
		}
		for _, val := range c.Rows[0].Counters {
			cms.count += uint64(val)
		}
	}
	return
}

// LoadStatsFromJSON load statistic from json.
func (h *Handle) LoadStatsFromJSON(tableInfo *model.TableInfo, jsonTbl *JSONTable) (*Table, error) {
	tbl := &Table{
		TableID:     tableInfo.ID,
		Columns:     make(map[int64]*Column, len(jsonTbl.Columns)),
		Indices:     make(map[int64]*Index, len(jsonTbl.Indices)),
		Count:       jsonTbl.Count,
		Version:     jsonTbl.Version,
		ModifyCount: jsonTbl.ModifyCount,
	}

	for key, val := range jsonTbl.Indices {
		for _, idxInfo := range tableInfo.Indices {
			if idxInfo.Name.L != key {
				continue
			}
			idx := &Index{
				Histogram: Histogram{ID: idxInfo.ID, NullCount: val.NullCount, LastUpdateVersion: val.LastUpdateVersion, NDV: val.Histogram.Ndv},
				CMSketch:  getCMSketch(val.CMSketch),
				Info:      idxInfo,
			}
			idx.Histogram.Buckets = HistogramFromProto(val.Histogram).Buckets

			for i := range idx.Buckets {
				bk := &idx.Buckets[i]
				bk.lowerScalar, bk.upperScalar, bk.commonPfxLen = preCalculateDatumScalar(&bk.LowerBound, &bk.UpperBound)
			}

			tbl.Indices[idx.ID] = idx
		}
	}
	for key, val := range jsonTbl.Columns {
		for _, colInfo := range tableInfo.Columns {
			if colInfo.Name.L != key {
				continue
			}
			col := &Column{
				Histogram: Histogram{ID: colInfo.ID, NullCount: val.NullCount, LastUpdateVersion: val.LastUpdateVersion, NDV: val.Histogram.Ndv},
				Info:      colInfo,
			}
			hist := HistogramFromProto(val.Histogram)
			col.Count = int64(hist.totalRowCount())
			if !val.AlreadyLoad {
				tbl.Columns[col.ID] = col
				continue
			}
			col.CMSketch = getCMSketch(val.CMSketch)
			col.Histogram.Buckets = hist.Buckets
			for i := range col.Buckets {
				if err := col.Buckets[i].ConvertTo(h, &colInfo.FieldType); err != nil {
					return nil, errors.Trace(err)
				}
				bk := &col.Buckets[i]
				bk.lowerScalar, bk.upperScalar, bk.commonPfxLen = preCalculateDatumScalar(&bk.LowerBound, &bk.UpperBound)
			}
			tbl.Columns[col.ID] = col
		}
	}
	return tbl, nil
}
