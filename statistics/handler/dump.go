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

package handler

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// JSONTable is used for dumping statistics.
type JSONTable struct {
	DatabaseName string                 `json:"database_name"`
	TableName    string                 `json:"table_name"`
	Columns      map[string]*jsonColumn `json:"columns"`
	Indices      map[string]*jsonColumn `json:"indices"`
	Count        int64                  `json:"count"`
	ModifyCount  int64                  `json:"modify_count"`
	Partitions   map[string]*JSONTable  `json:"partitions"`
}

type jsonColumn struct {
	Histogram         *tipb.Histogram `json:"histogram"`
	CMSketch          *tipb.CMSketch  `json:"cm_sketch"`
	NullCount         int64           `json:"null_count"`
	TotColSize        int64           `json:"tot_col_size"`
	LastUpdateVersion uint64          `json:"last_update_version"`
}

func dumpJSONCol(hist *statistics.Histogram, CMSketch *statistics.CMSketch) *jsonColumn {
	jsonCol := &jsonColumn{
		Histogram:         statistics.HistogramToProto(hist),
		NullCount:         hist.NullCount,
		TotColSize:        hist.TotColSize,
		LastUpdateVersion: hist.LastUpdateVersion,
	}
	if CMSketch != nil {
		jsonCol.CMSketch = statistics.CMSketchToProto(CMSketch)
	}
	return jsonCol
}

// DumpStatsToJSON dumps statistic to json.
func (h *Handle) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo) (*JSONTable, error) {
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return h.tableStatsToJSON(dbName, tableInfo, tableInfo.ID)
	}
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*JSONTable, len(pi.Definitions)),
	}
	for _, def := range pi.Definitions {
		tbl, err := h.tableStatsToJSON(dbName, tableInfo, def.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if tbl == nil {
			continue
		}
		jsonTbl.Partitions[def.Name.L] = tbl
	}
	return jsonTbl, nil
}

func (h *Handle) tableStatsToJSON(dbName string, tableInfo *model.TableInfo, physicalID int64) (*JSONTable, error) {
	tbl, err := h.tableStatsFromStorage(tableInfo, physicalID, true)
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
	table, err := is.TableByName(model.NewCIStr(jsonTbl.DatabaseName), model.NewCIStr(jsonTbl.TableName))
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, jsonTbl)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		if jsonTbl.Partitions == nil {
			return errors.New("No partition statistics")
		}
		for _, def := range pi.Definitions {
			tbl := jsonTbl.Partitions[def.Name.L]
			if tbl == nil {
				continue
			}
			err := h.loadStatsFromJSON(tableInfo, def.ID, tbl)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return errors.Trace(h.Update(is))
}

func (h *Handle) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *JSONTable) error {
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range tbl.Columns {
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.Count, 0, &col.Histogram, col.CMSketch, 1)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.Count, 1, &idx.Histogram, idx.CMSketch, 1)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = h.SaveMetaToStorage(tbl.PhysicalID, tbl.Count, tbl.ModifyCount)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// TableStatsFromJSON loads statistic from JSONTable and return the Table of statistic.
func TableStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *JSONTable) (*statistics.Table, error) {
	newHistColl := statistics.HistColl{
		PhysicalID:     physicalID,
		HavePhysicalID: true,
		Count:          jsonTbl.Count,
		ModifyCount:    jsonTbl.ModifyCount,
		Columns:        make(map[int64]*statistics.Column, len(jsonTbl.Columns)),
		Indices:        make(map[int64]*statistics.Index, len(jsonTbl.Indices)),
	}
	tbl := &statistics.Table{
		HistColl: newHistColl,
	}
	for id, jsonIdx := range jsonTbl.Indices {
		for _, idxInfo := range tableInfo.Indices {
			if idxInfo.Name.L != id {
				continue
			}
			hist := statistics.HistogramFromProto(jsonIdx.Histogram)
			hist.ID, hist.NullCount, hist.LastUpdateVersion = idxInfo.ID, jsonIdx.NullCount, jsonIdx.LastUpdateVersion
			idx := &statistics.Index{
				Histogram: *hist,
				CMSketch:  statistics.CMSketchFromProto(jsonIdx.CMSketch),
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
			hist := statistics.HistogramFromProto(jsonCol.Histogram)
			count := int64(hist.TotalRowCount())
			sc := &stmtctx.StatementContext{TimeZone: time.UTC}
			hist, err := hist.ConvertTo(sc, &colInfo.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			hist.ID, hist.NullCount, hist.LastUpdateVersion, hist.TotColSize = colInfo.ID, jsonCol.NullCount, jsonCol.LastUpdateVersion, jsonCol.TotColSize
			col := &statistics.Column{
				PhysicalID: physicalID,
				Histogram:  *hist,
				CMSketch:   statistics.CMSketchFromProto(jsonCol.CMSketch),
				Info:       colInfo,
				Count:      count,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
			}
			tbl.Columns[col.ID] = col
		}
	}
	return tbl, nil
}

func (h *Handle) dumpRangeFeedback(sc *stmtctx.StatementContext, ran *ranger.Range, rangeCount float64, q *statistics.QueryFeedback) error {
	lowIsNull := ran.LowVal[0].IsNull()
	if q.Tp == statistics.IndexType {
		lower, err := codec.EncodeKey(sc, nil, ran.LowVal[0])
		if err != nil {
			return errors.Trace(err)
		}
		upper, err := codec.EncodeKey(sc, nil, ran.HighVal[0])
		if err != nil {
			return errors.Trace(err)
		}
		ran.LowVal[0].SetBytes(lower)
		ran.HighVal[0].SetBytes(upper)
	} else {
		if !statistics.SupportColumnType(q.Hist.Tp) {
			return nil
		}
		if ran.LowVal[0].Kind() == types.KindMinNotNull {
			ran.LowVal[0] = statistics.GetMinValue(q.Hist.Tp)
		}
		if ran.HighVal[0].Kind() == types.KindMaxValue {
			ran.HighVal[0] = statistics.GetMaxValue(q.Hist.Tp)
		}
	}
	ranges := q.Hist.SplitRange(sc, []*ranger.Range{ran}, q.Tp == statistics.IndexType)
	counts := make([]float64, 0, len(ranges))
	sum := 0.0
	for i, r := range ranges {
		// Though after `SplitRange`, we may have ranges like `[l, r]`, we still use
		// `betweenRowCount` to compute the estimation since the ranges of feedback are all in `[l, r)`
		// form, that is to say, we ignore the exclusiveness of ranges from `SplitRange` and just use
		// its result of boundary values.
		count := q.Hist.BetweenRowCount(r.LowVal[0], r.HighVal[0])
		// We have to include `NullCount` of histogram for [l, r) cases where l is null because `betweenRowCount`
		// does not include null values of lower bound.
		if i == 0 && lowIsNull {
			count += float64(q.Hist.NullCount)
		}
		sum += count
		counts = append(counts, count)
	}
	if sum <= 1 {
		return nil
	}
	// We assume that each part contributes the same error rate.
	adjustFactor := rangeCount / sum
	for i, r := range ranges {
		q.Feedback = append(q.Feedback, statistics.Feedback{Lower: &r.LowVal[0], Upper: &r.HighVal[0], Count: int64(counts[i] * adjustFactor)})
	}
	return errors.Trace(h.DumpFeedbackToKV(q))
}

// DumpFeedbackForIndex dumps the feedback for index.
// For queries that contains both equality and range query, we will split them and Update accordingly.
func DumpFeedbackForIndex(h *Handle, q *statistics.QueryFeedback, t *statistics.Table) error {
	idx, ok := t.Indices[q.Hist.ID]
	if !ok {
		return nil
	}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	if idx.CMSketch == nil || idx.StatsVer != statistics.Version1 {
		return h.DumpFeedbackToKV(q)
	}
	ranges, err := q.DecodeToRanges(true)
	if err != nil {
		logutil.Logger(context.Background()).Debug("decode feedback ranges fail", zap.Error(err))
		return nil
	}
	for i, ran := range ranges {
		rangePosition := statistics.GetOrdinalOfRangeCond(sc, ran)
		// only contains range or equality query
		if rangePosition == 0 || rangePosition == len(ran.LowVal) {
			continue
		}

		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			logutil.Logger(context.Background()).Debug("encode keys fail", zap.Error(err))
			continue
		}
		equalityCount := float64(idx.CMSketch.QueryBytes(bytes)) * idx.GetIncreaseFactor(t.Count)
		rang := ranger.Range{
			LowVal:  []types.Datum{ran.LowVal[rangePosition]},
			HighVal: []types.Datum{ran.HighVal[rangePosition]},
		}
		colName := idx.Info.Columns[rangePosition].Name.L
		var rangeCount float64
		rangeFB := &statistics.QueryFeedback{PhysicalID: q.PhysicalID}
		// prefer index stats over column stats
		if idx := t.IndexStartWithColumn(colName); idx != nil && idx.Histogram.Len() != 0 {
			rangeCount, err = t.GetRowCountByIndexRanges(sc, idx.ID, []*ranger.Range{&rang})
			rangeFB.Tp, rangeFB.Hist = statistics.IndexType, &idx.Histogram
		} else if col := t.ColumnByName(colName); col != nil && col.Histogram.Len() != 0 {
			rangeCount, err = t.GetRowCountByColumnRanges(sc, col.ID, []*ranger.Range{&rang})
			rangeFB.Tp, rangeFB.Hist = statistics.ColType, &col.Histogram
		} else {
			continue
		}
		if err != nil {
			logutil.Logger(context.Background()).Debug("get row count by ranges fail", zap.Error(err))
			continue
		}

		equalityCount, rangeCount = getNewCountForIndex(equalityCount, rangeCount, float64(t.Count), float64(q.Feedback[i].Count))
		value := types.NewBytesDatum(bytes)
		q.Feedback[i] = statistics.Feedback{Lower: &value, Upper: &value, Count: int64(equalityCount)}
		err = h.dumpRangeFeedback(sc, &rang, rangeCount, rangeFB)
		if err != nil {
			logutil.Logger(context.Background()).Debug("dump range feedback fail", zap.Error(err))
			continue
		}
	}
	return errors.Trace(h.DumpFeedbackToKV(q))
}

// minAdjustFactor is the minimum adjust factor of each index feedback.
// We use it to avoid adjusting too much when the assumption of independence failed.
const minAdjustFactor = 0.7

// getNewCountForIndex adjust the estimated `eqCount` and `rangeCount` according to the real count.
// We assumes that `eqCount` and `rangeCount` contribute the same error rate.
func getNewCountForIndex(eqCount, rangeCount, totalCount, realCount float64) (float64, float64) {
	estimate := (eqCount / totalCount) * (rangeCount / totalCount) * totalCount
	if estimate <= 1 {
		return eqCount, rangeCount
	}
	adjustFactor := math.Sqrt(realCount / estimate)
	adjustFactor = math.Max(adjustFactor, minAdjustFactor)
	return eqCount * adjustFactor, rangeCount * adjustFactor
}
