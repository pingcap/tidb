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
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/statistics"
	statsStorage "github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/tikv/client-go/v2/oracle"
)

func (e *ShowExec) fetchShowStatsExtended() error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemaNames()
	for _, db := range dbs {
		tables := do.InfoSchema().SchemaTables(db)
		for _, tblInfo := range tables {
			tblInfo := tblInfo.Meta()
			pi := tblInfo.GetPartitionInfo()
			// Extended statistics for partitioned table is not supported now.
			if pi != nil {
				continue
			}
			e.appendTableForStatsExtended(db.L, tblInfo, h.GetTableStats(tblInfo))
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
		e.appendRow([]any{
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
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemaNames()
	for _, db := range dbs {
		tables := do.InfoSchema().SchemaTables(db)
		for _, tbl := range tables {
			tbl := tbl.Meta()
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.Ctx().GetSessionVars().IsDynamicPartitionPruneEnabled() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				e.appendTableForStatsMeta(db.O, tbl.Name.O, partitionName, h.GetTableStats(tbl))
				if pi != nil {
					for _, def := range pi.Definitions {
						e.appendTableForStatsMeta(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsMeta(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
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
	if !statsTbl.IsAnalyzed() {
		e.appendRow([]any{
			dbName,
			tblName,
			partitionName,
			e.versionToTime(statsTbl.Version),
			statsTbl.ModifyCount,
			statsTbl.RealtimeCount,
			nil,
		})
	} else {
		e.appendRow([]any{
			dbName,
			tblName,
			partitionName,
			e.versionToTime(statsTbl.Version),
			statsTbl.ModifyCount,
			statsTbl.RealtimeCount,
			e.versionToTime(statsTbl.LastAnalyzeVersion),
		})
	}
}

func (e *ShowExec) appendTableForStatsLocked(dbName, tblName, partitionName string) {
	e.appendRow([]any{
		dbName,
		tblName,
		partitionName,
		"locked",
	})
}

func (e *ShowExec) fetchShowStatsLocked() error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemaNames()

	type LockedTableInfo struct {
		dbName        string
		tblName       string
		partitionName string
	}
	tableInfo := make(map[int64]*LockedTableInfo)

	for _, db := range dbs {
		tables := do.InfoSchema().SchemaTables(db)
		for _, tbl := range tables {
			tbl := tbl.Meta()
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.Ctx().GetSessionVars().IsDynamicPartitionPruneEnabled() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				tableInfo[tbl.ID] = &LockedTableInfo{db.O, tbl.Name.O, partitionName}
				if pi != nil {
					for _, def := range pi.Definitions {
						tableInfo[def.ID] = &LockedTableInfo{db.O, tbl.Name.O, def.Name.O}
					}
				}
			} else {
				for _, def := range pi.Definitions {
					tableInfo[def.ID] = &LockedTableInfo{db.O, tbl.Name.O, def.Name.O}
				}
			}
		}
	}

	tids := make([]int64, 0, len(tableInfo))
	for tid := range tableInfo {
		tids = append(tids, tid)
	}

	lockedTables, err := h.GetLockedTables(tids...)
	if err != nil {
		return err
	}

	// Sort the table IDs to make the output stable.
	slices.Sort(tids)
	for _, tid := range tids {
		if _, ok := lockedTables[tid]; ok {
			info := tableInfo[tid]
			e.appendTableForStatsLocked(info.dbName, info.tblName, info.partitionName)
		}
	}

	return nil
}

func (e *ShowExec) fetchShowStatsHistogram() error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemaNames()
	for _, db := range dbs {
		tables := do.InfoSchema().SchemaTables(db)
		for _, tbl := range tables {
			tbl := tbl.Meta()
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.Ctx().GetSessionVars().IsDynamicPartitionPruneEnabled() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				e.appendTableForStatsHistograms(db.O, tbl.Name.O, partitionName, h.GetTableStats(tbl))
				if pi != nil {
					for _, def := range pi.Definitions {
						e.appendTableForStatsHistograms(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsHistograms(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
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
		if !col.IsStatsInitialized() {
			continue
		}
		e.histogramToRow(dbName, tblName, partitionName, col.Info.Name.O, 0, col.Histogram, cardinality.AvgColSize(col, statsTbl.RealtimeCount, false),
			col.StatsLoadedStatus.StatusToString(), col.MemoryUsage())
	}
	for _, idx := range stableIdxsStats(statsTbl.Indices) {
		if !idx.IsStatsInitialized() {
			continue
		}
		e.histogramToRow(dbName, tblName, partitionName, idx.Info.Name.O, 1, idx.Histogram, 0,
			idx.StatsLoadedStatus.StatusToString(), idx.MemoryUsage())
	}
}

func (e *ShowExec) histogramToRow(dbName, tblName, partitionName, colName string, isIndex int, hist statistics.Histogram,
	avgColSize float64, loadStatus string, memUsage statistics.CacheItemMemoryUsage) {
	e.appendRow([]any{
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
		loadStatus,
		memUsage.TotalMemoryUsage(),
		memUsage.HistMemUsage(),
		memUsage.TopnMemUsage(),
		memUsage.CMSMemUsage(),
	})
}

func (*ShowExec) versionToTime(version uint64) types.Time {
	t := oracle.GetTimeFromTS(version)
	return types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, 0)
}

func (e *ShowExec) fetchShowStatsBuckets() error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemaNames()
	for _, db := range dbs {
		tables := do.InfoSchema().SchemaTables(db)
		for _, tbl := range tables {
			tbl := tbl.Meta()
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.Ctx().GetSessionVars().IsDynamicPartitionPruneEnabled() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				if err := e.appendTableForStatsBuckets(db.O, tbl.Name.O, partitionName, h.GetTableStats(tbl)); err != nil {
					return err
				}
				if pi != nil {
					for _, def := range pi.Definitions {
						if err := e.appendTableForStatsBuckets(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
							return err
						}
					}
				}
			} else {
				for _, def := range pi.Definitions {
					if err := e.appendTableForStatsBuckets(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
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
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemaNames()
	for _, db := range dbs {
		tables := do.InfoSchema().SchemaTables(db)
		for _, tbl := range tables {
			tbl := tbl.Meta()
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.Ctx().GetSessionVars().IsDynamicPartitionPruneEnabled() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				if err := e.appendTableForStatsTopN(db.O, tbl.Name.O, partitionName, h.GetTableStats(tbl)); err != nil {
					return err
				}
				if pi != nil {
					for _, def := range pi.Definitions {
						if err := e.appendTableForStatsTopN(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
							return err
						}
					}
				}
			} else {
				for _, def := range pi.Definitions {
					if err := e.appendTableForStatsTopN(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
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
	slices.SortFunc(cols, func(i, j *statistics.Column) int { return cmp.Compare(i.ID, j.ID) })
	return
}

func stableIdxsStats(idxStats map[int64]*statistics.Index) (idxs []*statistics.Index) {
	for _, idx := range idxStats {
		idxs = append(idxs, idx)
	}
	slices.SortFunc(idxs, func(i, j *statistics.Index) int { return cmp.Compare(i.ID, j.ID) })
	return
}

func (e *ShowExec) topNToRows(dbName, tblName, partitionName, colName string, numOfCols int, isIndex int, topN *statistics.TopN, columnTypes []byte) error {
	if topN == nil {
		return nil
	}
	var tmpDatum types.Datum
	for i := 0; i < len(topN.TopN); i++ {
		tmpDatum.SetBytes(topN.TopN[i].Encoded)
		valStr, err := statistics.ValueToString(e.Ctx().GetSessionVars(), &tmpDatum, numOfCols, columnTypes)
		if err != nil {
			return err
		}
		e.appendRow([]any{
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
		lowerBoundStr, err := statistics.ValueToString(e.Ctx().GetSessionVars(), hist.GetLower(i), numOfCols, idxColumnTypes)
		if err != nil {
			return errors.Trace(err)
		}
		upperBoundStr, err := statistics.ValueToString(e.Ctx().GetSessionVars(), hist.GetUpper(i), numOfCols, idxColumnTypes)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]any{
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
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	dbs := do.InfoSchema().AllSchemaNames()
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)
	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	for _, db := range dbs {
		if fieldFilter != "" && db.L != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(db.L) {
			continue
		}
		tables := do.InfoSchema().SchemaTables(db)
		for _, tbl := range tables {
			tbl := tbl.Meta()
			pi := tbl.GetPartitionInfo()
			if pi == nil || e.Ctx().GetSessionVars().IsDynamicPartitionPruneEnabled() {
				partitionName := ""
				if pi != nil {
					partitionName = "global"
				}
				e.appendTableForStatsHealthy(db.O, tbl.Name.O, partitionName, h.GetTableStats(tbl))
				if pi != nil {
					for _, def := range pi.Definitions {
						e.appendTableForStatsHealthy(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
					}
				}
			} else {
				for _, def := range pi.Definitions {
					e.appendTableForStatsHealthy(db.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
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
	e.appendRow([]any{
		dbName,
		tblName,
		partitionName,
		healthy,
	})
}

func (e *ShowExec) fetchShowHistogramsInFlight() {
	statsHandle := domain.GetDomain(e.Ctx()).StatsHandle()
	e.appendRow([]any{statsStorage.CleanFakeItemsForShowHistInFlights(statsHandle)})
}

func (e *ShowExec) fetchShowAnalyzeStatus(ctx context.Context) error {
	rows, err := dataForAnalyzeStatusHelper(ctx, e.BaseExecutor.Ctx())
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
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()
	colStatsMap, err := h.LoadColumnStatsUsage(e.Ctx().GetSessionVars().Location())
	if err != nil {
		return err
	}
	dbs := do.InfoSchema().AllSchemaNames()

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
			tblColID := model.TableItemID{TableID: tblID, ID: col.ID, IsIndex: false}
			colStatsUsage, ok := colStatsMap[tblColID]
			if !ok {
				continue
			}
			row := []any{dbName, tbl.Name.O, partitionName, col.Name.O}
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
		tables := do.InfoSchema().SchemaTables(db)
		for _, tbl := range tables {
			tbl := tbl.Meta()
			pi := tbl.GetPartitionInfo()
			// Though partition tables in static pruning mode don't have global stats, we dump predicate columns of partitions with table ID
			// rather than partition ID. Hence appendTableForColumnStatsUsage needs to be called for both partition and global in both dynamic
			// and static pruning mode.
			appendTableForColumnStatsUsage(db.O, tbl, pi != nil, nil)
			if pi != nil {
				for i := range pi.Definitions {
					appendTableForColumnStatsUsage(db.O, tbl, false, &pi.Definitions[i])
				}
			}
		}
	}
	return nil
}
