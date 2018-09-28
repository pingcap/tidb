// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

// JSONTable is used for dumping statistics.
type JSONTable struct {
	DatabaseName string                 `json:"database_name"`
	TableName    string                 `json:"table_name"`
	Columns      map[string]*jsonColumn `json:"columns"`
	Indices      map[string]*jsonColumn `json:"indices"`
	Count        int64                  `json:"count"`
	ModifyCount  int64                  `json:"modify_count"`
	Version      uint64                 `json:"version"`
}

type jsonColumn struct {
	Histogram         *tipb.Histogram `json:"histogram"`
	CMSketch          *tipb.CMSketch  `json:"cm_sketch"`
	NullCount         int64           `json:"null_count"`
	TotColSize        int64           `json:"tot_col_size"`
	LastUpdateVersion uint64          `json:"last_update_version"`
}

func dumpJSONCol(hist *Histogram, CMSketch *CMSketch) *jsonColumn {
	jsonCol := &jsonColumn{
		Histogram:         HistogramToProto(hist),
		NullCount:         hist.NullCount,
		TotColSize:        hist.TotColSize,
		LastUpdateVersion: hist.LastUpdateVersion,
	}
	if CMSketch != nil {
		jsonCol.CMSketch = CMSketchToProto(CMSketch)
	}
	return jsonCol
}

// DumpStatsToJSON dumps statistic to json.
func (h *Handle) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo) (*JSONTable, error) {
	tbl, err := h.tableStatsFromStorage(tableInfo, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Columns:      make(map[string]*jsonColumn, len(tbl.Columns)),
		Indices:      make(map[string]*jsonColumn, len(tbl.Indices)),
		Count:        tbl.Count,
		ModifyCount:  tbl.ModifyCount,
		Version:      tbl.Version,
	}
	for _, col := range tbl.Columns {
		hist, err := col.ConvertTo(new(stmtctx.StatementContext), types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return nil, errors.Trace(err)
		}
		jsonTbl.Columns[col.Info.Name.L] = dumpJSONCol(hist, col.CMSketch)
	}

	for _, idx := range tbl.Indices {
		jsonTbl.Indices[idx.Info.Name.L] = dumpJSONCol(&idx.Histogram, idx.CMSketch)
	}
	return jsonTbl, nil
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

	for id, jsonIdx := range jsonTbl.Indices {
		for _, idxInfo := range tableInfo.Indices {
			if idxInfo.Name.L != id {
				continue
			}
			hist := HistogramFromProto(jsonIdx.Histogram)
			hist.ID, hist.NullCount, hist.LastUpdateVersion = idxInfo.ID, jsonIdx.NullCount, jsonIdx.LastUpdateVersion
			idx := &Index{
				Histogram: *hist,
				CMSketch:  CMSketchFromProto(jsonIdx.CMSketch),
				Info:      idxInfo,
			}
			tbl.Indices[idx.ID] = idx
		}
	}
	for id, jsonCol := range jsonTbl.Columns {
		for _, colInfo := range tableInfo.Columns {
			if colInfo.Name.L != id {
				continue
			}
			hist := HistogramFromProto(jsonCol.Histogram)
			count := int64(hist.totalRowCount())
			hist, err := hist.ConvertTo(new(stmtctx.StatementContext), &colInfo.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			hist.ID, hist.NullCount, hist.LastUpdateVersion, hist.TotColSize = colInfo.ID, jsonCol.NullCount, jsonCol.LastUpdateVersion, jsonCol.TotColSize
			col := &Column{
				Histogram: *hist,
				CMSketch:  CMSketchFromProto(jsonCol.CMSketch),
				Info:      colInfo,
				Count:     count,
			}
			tbl.Columns[col.ID] = col
		}
	}
	return tbl, nil
}
