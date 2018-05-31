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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/infoschema"
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
	if tbl == nil {
		return nil, nil
	}
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Columns:      make(map[string]*jsonColumn, len(tbl.Columns)),
		Indices:      make(map[string]*jsonColumn, len(tbl.Indices)),
		Count:        tbl.Count,
		ModifyCount:  tbl.ModifyCount,
	}

	for _, col := range tbl.Columns {
		sc := &stmtctx.StatementContext{TimeZone: time.UTC}
		hist, err := col.ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
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

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
func (h *Handle) LoadStatsFromJSON(is infoschema.InfoSchema, jsonTbl *JSONTable) error {
	tableInfo, err := is.TableByName(model.NewCIStr(jsonTbl.DatabaseName), model.NewCIStr(jsonTbl.TableName))
	if err != nil {
		return errors.Trace(err)
	}
	tbl, err := h.LoadStatsFromJSONToTable(tableInfo.Meta(), jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	if h.Lease > 0 {
		hists := make([]*Histogram, 0, len(tbl.Columns))
		cms := make([]*CMSketch, 0, len(tbl.Columns))
		for _, col := range tbl.Columns {
			hists = append(hists, &col.Histogram)
			cms = append(cms, col.CMSketch)
		}
		h.AnalyzeResultCh() <- &AnalyzeResult{
			TableID: tbl.TableID,
			Hist:    hists,
			Cms:     cms,
			Count:   tbl.Count,
			IsIndex: 0,
			Err:     nil,
		}

		hists = make([]*Histogram, 0, len(tbl.Indices))
		cms = make([]*CMSketch, 0, len(tbl.Indices))
		for _, idx := range tbl.Indices {
			hists = append(hists, &idx.Histogram)
			cms = append(cms, idx.CMSketch)
		}
		h.AnalyzeResultCh() <- &AnalyzeResult{
			TableID: tbl.TableID,
			Hist:    hists,
			Cms:     cms,
			Count:   tbl.Count,
			IsIndex: 1,
			Err:     nil,
		}

		h.LoadMetaCh() <- &LoadMeta{
			TableID:     tbl.TableID,
			Count:       tbl.Count,
			ModifyCount: tbl.ModifyCount,
		}
		return errors.Trace(err)
	}
	for _, col := range tbl.Columns {
		err = SaveStatsToStorage(h.ctx, tbl.TableID, tbl.Count, 0, &col.Histogram, col.CMSketch)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		err = SaveStatsToStorage(h.ctx, tbl.TableID, tbl.Count, 1, &idx.Histogram, idx.CMSketch)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = SaveMetaToStorage(h.ctx, tbl.TableID, tbl.Count, tbl.ModifyCount)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(h.Update(is))
}

// LoadStatsFromJSONToTable load statistic from JSONTable and return the Table of statistic.
func (h *Handle) LoadStatsFromJSONToTable(tableInfo *model.TableInfo, jsonTbl *JSONTable) (*Table, error) {
	tbl := &Table{
		TableID:     tableInfo.ID,
		Columns:     make(map[int64]*Column, len(jsonTbl.Columns)),
		Indices:     make(map[int64]*Index, len(jsonTbl.Indices)),
		Count:       jsonTbl.Count,
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
			sc := &stmtctx.StatementContext{TimeZone: time.UTC}
			hist, err := hist.ConvertTo(sc, &colInfo.FieldType)
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

// LoadMeta is the statistic meta loaded from json file.
type LoadMeta struct {
	TableID     int64
	Count       int64
	ModifyCount int64
}
