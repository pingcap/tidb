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

package executor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/v2/oracle"
)

func (e *ShowExec) fetchShowStatsExtended() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tblInfo := range db.Tables {
			pi := tblInfo.GetPartitionInfo()
			// Extended statistics for partitioned table is not supported now.
			if pi != nil {
				continue
			}
			e.appendTableForStatsExtended(db.Name.L, tblInfo, h.GetTableStats(tblInfo))
		}
	}
	return nil
}

func (e *ShowExec) appendTableForStatsExtended(dbName string, tbl *model.TableInfo, statsTbl *statistics.Table) {
	if statsTbl.Pseudo || statsTbl.ExtendedStats == nil || len(statsTbl.ExtendedStats.Stats) == 0 {
		return
	}
	colID2Name := make(map[int64]string, len(tbl.Columns))
	for _, col := range tbl.Columns {
		colID2Name[col.ID] = col.Name.L
	}
	var sb strings.Builder
	for statsName, item := range statsTbl.ExtendedStats.Stats {
		sb.Reset()
		sb.WriteString("[")
		allColsExist := true
		for i, colID := range item.ColIDs {
			name, ok := colID2Name[colID]
			if !ok {
				allColsExist = false
				break
			}
			sb.WriteString(name)
			if i != len(item.ColIDs)-1 {
				sb.WriteString(",")
			}
		}
		// The column may have been dropped, while the extended stats have not been removed by GC yet.
		if !allColsExist {
			continue
		}
		sb.WriteString("]")
		colNames := sb.String()
		var statsType, statsVal string
		switch item.Tp {
		case ast.StatsTypeCorrelation:
			statsType = "correlation"
			statsVal = fmt.Sprintf("%f", item.ScalarVals)
		case ast.StatsTypeDependency:
			statsType = "dependency"
			statsVal = item.StringVals
		case ast.StatsTypeCardinality:
			statsType = "cardinality"
			statsVal = item.StringVals
		}
		e.appendRow([]interface{}{
			dbName,
			tbl.Name.L,
			statsName,
			colNames,
			statsType,
			statsVal,
			// Same LastUpdateVersion for records of the same table, mainly for debug purpose on product env.
			statsTbl.ExtendedStats.LastUpdateVersion,
		})
	}
}

func (e *ShowExec) fetchShowStatsMeta() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.ctx.GetSessionVars().UseDynamicPartitionPrune() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				e.appendTableForStatsMeta(db.Name.O, tbl.Name.O, partitionName, h.GetTableStats(tbl))
				if pi != nil {
					for _, def := range pi.Definitions {
						e.appendTableForStatsMeta(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsMeta(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendTableForStatsMeta(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	if statsTbl.Pseudo {
		return
	}
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		e.versionToTime(statsTbl.Version),
		statsTbl.ModifyCount,
		statsTbl.Count,
	})
}

func (e *ShowExec) fetchShowStatsHistogram() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.ctx.GetSessionVars().UseDynamicPartitionPrune() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				e.appendTableForStatsHistograms(db.Name.O, tbl.Name.O, partitionName, h.GetTableStats(tbl))
				if pi != nil {
					for _, def := range pi.Definitions {
						e.appendTableForStatsHistograms(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsHistograms(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendTableForStatsHistograms(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	if statsTbl.Pseudo {
		return
	}
	for _, col := range stableColsStats(statsTbl.Columns) {
		// Pass a nil StatementContext to avoid column stats being marked as needed.
		if col.IsInvalid(nil, false) {
			continue
		}
		e.histogramToRow(dbName, tblName, partitionName, col.Info.Name.O, 0, col.Histogram, col.AvgColSize(statsTbl.Count, false))
	}
	for _, idx := range stableIdxsStats(statsTbl.Indices) {
		e.histogramToRow(dbName, tblName, partitionName, idx.Info.Name.O, 1, idx.Histogram, 0)
	}
}

func (e *ShowExec) histogramToRow(dbName, tblName, partitionName, colName string, isIndex int, hist statistics.Histogram, avgColSize float64) {
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		colName,
		isIndex,
		e.versionToTime(hist.LastUpdateVersion),
		hist.NDV,
		hist.NullCount,
		avgColSize,
		hist.Correlation,
	})
}

func (e *ShowExec) versionToTime(version uint64) types.Time {
	t := oracle.GetTimeFromTS(version)
	return types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, 0)
}

func (e *ShowExec) fetchShowStatsBuckets() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.ctx.GetSessionVars().UseDynamicPartitionPrune() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				if err := e.appendTableForStatsBuckets(db.Name.O, tbl.Name.O, partitionName, h.GetTableStats(tbl)); err != nil {
					return err
				}
				if pi != nil {
					for _, def := range pi.Definitions {
						if err := e.appendTableForStatsBuckets(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
							return err
						}
					}
				}
			} else {
				for _, def := range pi.Definitions {
					if err := e.appendTableForStatsBuckets(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendTableForStatsBuckets(dbName, tblName, partitionName string, statsTbl *statistics.Table) error {
	if statsTbl.Pseudo {
		return nil
	}
	colNameToType := make(map[string]byte, len(statsTbl.Columns))
	for _, col := range stableColsStats(statsTbl.Columns) {
		err := e.bucketsToRows(dbName, tblName, partitionName, col.Info.Name.O, 0, col.Histogram, nil)
		if err != nil {
			return errors.Trace(err)
		}
		colNameToType[col.Info.Name.O] = col.Histogram.Tp.GetType()
	}
	for _, idx := range stableIdxsStats(statsTbl.Indices) {
		idxColumnTypes := make([]byte, 0, len(idx.Info.Columns))
		for i := 0; i < len(idx.Info.Columns); i++ {
			idxColumnTypes = append(idxColumnTypes, colNameToType[idx.Info.Columns[i].Name.O])
		}
		err := e.bucketsToRows(dbName, tblName, partitionName, idx.Info.Name.O, len(idx.Info.Columns), idx.Histogram, idxColumnTypes)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *ShowExec) fetchShowStatsTopN() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.ctx.GetSessionVars().UseDynamicPartitionPrune() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				if err := e.appendTableForStatsTopN(db.Name.O, tbl.Name.O, partitionName, h.GetTableStats(tbl)); err != nil {
					return err
				}
				if pi != nil {
					for _, def := range pi.Definitions {
						if err := e.appendTableForStatsTopN(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
							return err
						}
					}
				}
			} else {
				for _, def := range pi.Definitions {
					if err := e.appendTableForStatsTopN(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendTableForStatsTopN(dbName, tblName, partitionName string, statsTbl *statistics.Table) error {
	if statsTbl.Pseudo {
		return nil
	}
	colNameToType := make(map[string]byte, len(statsTbl.Columns))
	for _, col := range stableColsStats(statsTbl.Columns) {
		err := e.topNToRows(dbName, tblName, partitionName, col.Info.Name.O, 1, 0, col.TopN, []byte{col.Histogram.Tp.GetType()})
		if err != nil {
			return errors.Trace(err)
		}
		colNameToType[col.Info.Name.O] = col.Histogram.Tp.GetType()
	}
	for _, idx := range stableIdxsStats(statsTbl.Indices) {
		idxColumnTypes := make([]byte, 0, len(idx.Info.Columns))
		for i := 0; i < len(idx.Info.Columns); i++ {
			idxColumnTypes = append(idxColumnTypes, colNameToType[idx.Info.Columns[i].Name.O])
		}
		err := e.topNToRows(dbName, tblName, partitionName, idx.Info.Name.O, len(idx.Info.Columns), 1, idx.TopN, idxColumnTypes)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func stableColsStats(colStats map[int64]*statistics.Column) (cols []*statistics.Column) {
	for _, col := range colStats {
		cols = append(cols, col)
	}
	sort.Slice(cols, func(i, j int) bool { return cols[i].ID < cols[j].ID })
	return
}

func stableIdxsStats(idxStats map[int64]*statistics.Index) (idxs []*statistics.Index) {
	for _, idx := range idxStats {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i].ID < idxs[j].ID })
	return
}

func (e *ShowExec) topNToRows(dbName, tblName, partitionName, colName string, numOfCols int, isIndex int, topN *statistics.TopN, columnTypes []byte) error {
	if topN == nil {
		return nil
	}
	var tmpDatum types.Datum
	for i := 0; i < len(topN.TopN); i++ {
		tmpDatum.SetBytes(topN.TopN[i].Encoded)
		valStr, err := statistics.ValueToString(e.ctx.GetSessionVars(), &tmpDatum, numOfCols, columnTypes)
		if err != nil {
			return err
		}
		e.appendRow([]interface{}{
			dbName,
			tblName,
			partitionName,
			colName,
			isIndex,
			valStr,
			topN.TopN[i].Count,
		})
	}
	return nil
}

// bucketsToRows converts histogram buckets to rows. If the histogram is built from index, then numOfCols equals to number
// of index columns, else numOfCols is 0.
func (e *ShowExec) bucketsToRows(dbName, tblName, partitionName, colName string, numOfCols int, hist statistics.Histogram, idxColumnTypes []byte) error {
	isIndex := 0
	if numOfCols > 0 {
		isIndex = 1
	}
	for i := 0; i < hist.Len(); i++ {
		lowerBoundStr, err := statistics.ValueToString(e.ctx.GetSessionVars(), hist.GetLower(i), numOfCols, idxColumnTypes)
		if err != nil {
			return errors.Trace(err)
		}
		upperBoundStr, err := statistics.ValueToString(e.ctx.GetSessionVars(), hist.GetUpper(i), numOfCols, idxColumnTypes)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]interface{}{
			dbName,
			tblName,
			partitionName,
			colName,
			isIndex,
			i,
			hist.Buckets[i].Count,
			hist.Buckets[i].Repeat,
			lowerBoundStr,
			upperBoundStr,
			hist.Buckets[i].NDV,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowStatsHealthy() {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemas()
	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.ctx.GetSessionVars().UseDynamicPartitionPrune() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				e.appendTableForStatsHealthy(db.Name.O, tbl.Name.O, partitionName, h.GetTableStats(tbl))
				if pi != nil {
					for _, def := range pi.Definitions {
						e.appendTableForStatsHealthy(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsHealthy(db.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
}

func (e *ShowExec) appendTableForStatsHealthy(dbName, tblName, partitionName string, statsTbl *statistics.Table) {
	healthy, ok := statsTbl.GetStatsHealthy()
	if !ok {
		return
	}
	e.appendRow([]interface{}{
		dbName,
		tblName,
		partitionName,
		healthy,
	})
}

func (e *ShowExec) fetchShowHistogramsInFlight() {
	e.appendRow([]interface{}{statistics.HistogramNeededColumns.Length()})
}

func (e *ShowExec) fetchShowAnalyzeStatus() error {
	rows, err := dataForAnalyzeStatusHelper(e.baseExecutor.ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		for i := range row {
			e.result.AppendDatum(i, &row[i])
		}
	}
	return nil
}

func (e *ShowExec) fetchShowColumnStatsUsage() error {
	do := domain.GetDomain(e.ctx)
	h := do.StatsHandle()
	colStatsMap, err := h.LoadColumnStatsUsage(e.ctx.GetSessionVars().Location())
	if err != nil {
		return err
	}
	dbs := do.InfoSchema().AllSchemas()

	appendTableForColumnStatsUsage := func(dbName string, tbl *model.TableInfo, global bool, def *model.PartitionDefinition) {
		tblID := tbl.ID
		if def != nil {
			tblID = def.ID
		}
		partitionName := ""
		if def != nil {
			partitionName = def.Name.O
		} else if global {
			partitionName = "global"
		}
		for _, col := range tbl.Columns {
			tblColID := model.TableColumnID{TableID: tblID, ColumnID: col.ID}
			colStatsUsage, ok := colStatsMap[tblColID]
			if !ok {
				continue
			}
			row := []interface{}{dbName, tbl.Name.O, partitionName, col.Name.O}
			if colStatsUsage.LastUsedAt != nil {
				row = append(row, *colStatsUsage.LastUsedAt)
			} else {
				row = append(row, nil)
			}
			if colStatsUsage.LastAnalyzedAt != nil {
				row = append(row, *colStatsUsage.LastAnalyzedAt)
			} else {
				row = append(row, nil)
			}
			e.appendRow(row)
		}
	}

	for _, db := range dbs {
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			// Though partition tables in static pruning mode don't have global stats, we dump predicate columns of partitions with table ID
			// rather than partition ID. Hence appendTableForColumnStatsUsage needs to be called for both partition and global in both dynamic
			// and static pruning mode.
			appendTableForColumnStatsUsage(db.Name.O, tbl, pi != nil, nil)
			if pi != nil {
				for i := range pi.Definitions {
					appendTableForColumnStatsUsage(db.Name.O, tbl, false, &pi.Definitions[i])
				}
			}
		}
	}
	return nil
}
