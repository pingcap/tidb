// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

func BuildHandleColsForAnalyze(
	_ base.PlanContext, tblInfo *model.TableInfo, allColumns bool, colsInfo []*model.ColumnInfo,
) util.HandleCols {
	var handleCols util.HandleCols
	switch {
	case tblInfo.PKIsHandle:
		pkCol := tblInfo.GetPkColInfo()
		var index int
		if allColumns {
			// If all the columns need to be analyzed, we just set index to pkCol.Offset.
			index = pkCol.Offset
		} else {
			// If only a part of the columns need to be analyzed, we need to set index according to colsInfo.
			index = getColOffsetForAnalyze(colsInfo, pkCol.ID)
		}
		handleCols = util.NewIntHandleCols(&expression.Column{
			ID:      pkCol.ID,
			RetType: &pkCol.FieldType,
			Index:   index,
		})
	case tblInfo.IsCommonHandle:
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		pkColLen := len(pkIdx.Columns)
		columns := make([]*expression.Column, pkColLen)
		for i := range pkColLen {
			colInfo := tblInfo.Columns[pkIdx.Columns[i].Offset]
			var index int
			if allColumns {
				// If all the columns need to be analyzed, we just set index to colInfo.Offset.
				index = colInfo.Offset
			} else {
				// If only a part of the columns need to be analyzed, we need to set index according to colsInfo.
				index = getColOffsetForAnalyze(colsInfo, colInfo.ID)
			}
			columns[i] = &expression.Column{
				ID:      colInfo.ID,
				RetType: &colInfo.FieldType,
				Index:   index,
			}
		}
		// We don't modify IndexColumn.Offset for CommonHandleCols.idxInfo according to colsInfo. There are two reasons.
		// The first reason is that we use Column.Index of CommonHandleCols.columns, rather than IndexColumn.Offset, to get
		// column value from row samples when calling (*CommonHandleCols).BuildHandleByDatums in (*AnalyzeColumnsExec).buildSamplingStats.
		// The second reason is that in (cb *CommonHandleCols).BuildHandleByDatums, tablecodec.TruncateIndexValues(cb.tblInfo, cb.idxInfo, datumBuf)
		// is called, which asks that IndexColumn.Offset of cb.idxInfo must be according to cb,tblInfo.
		// TODO: find a better way to find handle columns in ANALYZE rather than use Column.Index
		handleCols = util.NewCommonHandlesColsWithoutColsAlign(tblInfo, pkIdx, columns)
	}
	return handleCols
}

// GetPhysicalIDsAndPartitionNames returns physical IDs and names of these partitions.
func GetPhysicalIDsAndPartitionNames(tblInfo *model.TableInfo, partitionNames []ast.CIStr) ([]int64, []string, error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		if len(partitionNames) != 0 {
			return nil, nil, errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
		}
		return []int64{tblInfo.ID}, []string{""}, nil
	}

	// If the PartitionNames is empty, we will return all partitions.
	if len(partitionNames) == 0 {
		ids := make([]int64, 0, len(pi.Definitions))
		names := make([]string, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
			names = append(names, def.Name.O)
		}
		return ids, names, nil
	}
	ids := make([]int64, 0, len(partitionNames))
	names := make([]string, 0, len(partitionNames))
	for _, name := range partitionNames {
		found := false
		for _, def := range pi.Definitions {
			if def.Name.L == name.L {
				found = true
				ids = append(ids, def.ID)
				names = append(names, def.Name.O)
				break
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("can not found the specified partition name %s in the table definition", name.O)
		}
	}

	return ids, names, nil
}

type calcOnceMap struct {
	data       map[int64]struct{}
	calculated bool
}

// addColumnsWithVirtualExprs adds columns to cols.data and processes their virtual expressions recursively.
// columnSelector is a function that determines which columns to include and their virtual expressions.
func (b *PlanBuilder) addColumnsWithVirtualExprs(tbl *resolve.TableNameW, cols *calcOnceMap, columnSelector func([]*expression.Column) []expression.Expression) error {
	intest.Assert(cols.data != nil, "cols.data should not be nil before adding columns")
	tblInfo := tbl.TableInfo
	columns, _, err := expression.ColumnInfos2ColumnsAndNames(b.ctx.GetExprCtx(), tbl.Schema, tbl.Name, tblInfo.Columns, tblInfo)
	if err != nil {
		return err
	}

	virtualExprs := columnSelector(columns)
	relatedCols := expression.GetUniqueIDToColumnMap()
	defer expression.PutUniqueIDToColumnMap(relatedCols)
	for len(virtualExprs) > 0 {
		expression.ExtractColumnsMapFromExpressionsWithReusedMap(relatedCols, nil, virtualExprs...)
		virtualExprs = virtualExprs[:0]
		for _, col := range relatedCols {
			cols.data[col.ID] = struct{}{}
			if col.VirtualExpr != nil {
				virtualExprs = append(virtualExprs, col.VirtualExpr)
			}
		}
		clear(relatedCols)
	}
	return nil
}

// getMustAnalyzedColumns puts the columns whose statistics must be collected into `cols` if `cols` has not been calculated.
func (b *PlanBuilder) getMustAnalyzedColumns(tbl *resolve.TableNameW, cols *calcOnceMap) (map[int64]struct{}, error) {
	if cols.calculated {
		return cols.data, nil
	}
	tblInfo := tbl.TableInfo
	cols.data = make(map[int64]struct{}, len(tblInfo.Columns))
	if len(tblInfo.Indices) > 0 {
		// Add indexed columns.
		// Some indexed columns are generated columns so we also need to add the columns that make up those generated columns.
		err := b.addColumnsWithVirtualExprs(tbl, cols, func(columns []*expression.Column) []expression.Expression {
			virtualExprs := make([]expression.Expression, 0, len(tblInfo.Columns))
			for _, idx := range tblInfo.Indices {
				// for an index in state public, we can always analyze it for internal analyze session.
				// for an index in state WriteReorg, we can only analyze it under variable EnableDDLAnalyze is on.
				indexStateAnalyzable := idx.State == model.StatePublic ||
					(idx.State == model.StateWriteReorganization && b.ctx.GetSessionVars().EnableDDLAnalyzeExecOpt)
				// for mv index and ci index fail it first, then analyze those analyzable indexes.
				if idx.MVIndex || idx.IsColumnarIndex() || !indexStateAnalyzable {
					continue
				}
				for _, idxCol := range idx.Columns {
					colInfo := tblInfo.Columns[idxCol.Offset]
					cols.data[colInfo.ID] = struct{}{}
					if expr := columns[idxCol.Offset].VirtualExpr; expr != nil {
						virtualExprs = append(virtualExprs, expr)
					}
				}
			}
			return virtualExprs
		})
		if err != nil {
			return nil, err
		}
	}
	if tblInfo.PKIsHandle {
		pkCol := tblInfo.GetPkColInfo()
		cols.data[pkCol.ID] = struct{}{}
	}
	if b.ctx.GetSessionVars().EnableExtendedStats {
		// Add the columns related to extended stats.
		// TODO: column_ids read from mysql.stats_extended in optimization phase may be different from that in execution phase((*Handle).BuildExtendedStats)
		// if someone inserts data into mysql.stats_extended between the two time points, the new added extended stats may not be computed.
		statsHandle := domain.GetDomain(b.ctx).StatsHandle()
		extendedStatsColIDs, err := statsHandle.CollectColumnsInExtendedStats(tblInfo.ID)
		if err != nil {
			return nil, err
		}
		for _, colID := range extendedStatsColIDs {
			cols.data[colID] = struct{}{}
		}
	}
	cols.calculated = true
	return cols.data, nil
}

// getPredicateColumns gets the columns used in predicates.
func (b *PlanBuilder) getPredicateColumns(tbl *resolve.TableNameW, cols *calcOnceMap) (map[int64]struct{}, error) {
	// Already calculated in the previous call.
	if cols.calculated {
		return cols.data, nil
	}
	tblInfo := tbl.TableInfo
	cols.data = make(map[int64]struct{}, len(tblInfo.Columns))
	do := domain.GetDomain(b.ctx)
	h := do.StatsHandle()
	colList, err := h.GetPredicateColumns(tblInfo.ID)
	if err != nil {
		return nil, err
	}
	if len(colList) == 0 {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(
			errors.NewNoStackErrorf(
				"No predicate column has been collected yet for table %s.%s, so only indexes and the columns composing the indexes will be analyzed",
				tbl.Schema.L,
				tbl.Name.L,
			),
		)
	} else {
		// Some predicate columns are generated columns so we also need to add the columns that make up those generated columns.
		err := b.addColumnsWithVirtualExprs(tbl, cols, func(columns []*expression.Column) []expression.Expression {
			virtualExprs := make([]expression.Expression, 0, len(tblInfo.Columns))
			for _, id := range colList {
				columnInfo := tblInfo.GetColumnByID(id)
				intest.Assert(columnInfo != nil, "column %d not found in table %s.%s", id, tbl.Schema.L, tbl.Name.L)
				if columnInfo == nil {
					statslogutil.StatsLogger().Warn("Column not found in table while getting predicate columns",
						zap.Int64("columnID", id),
						zap.String("tableSchema", tbl.Schema.L),
						zap.String("table", tblInfo.Name.O),
					)
					// This should not happen, but we handle it gracefully.
					continue
				}
				cols.data[columnInfo.ID] = struct{}{}
				if expr := columns[columnInfo.Offset].VirtualExpr; expr != nil {
					virtualExprs = append(virtualExprs, expr)
				}
			}
			return virtualExprs
		})
		if err != nil {
			return nil, err
		}
	}
	cols.calculated = true
	return cols.data, nil
}

func getAnalyzeColumnList(specifiedColumns []ast.CIStr, tbl *resolve.TableNameW) ([]*model.ColumnInfo, error) {
	colList := make([]*model.ColumnInfo, 0, len(specifiedColumns))
	for _, colName := range specifiedColumns {
		colInfo := model.FindColumnInfo(tbl.TableInfo.Columns, colName.L)
		if colInfo == nil {
			return nil, plannererrors.ErrAnalyzeMissColumn.GenWithStackByArgs(colName.O, tbl.TableInfo.Name.O)
		}
		colList = append(colList, colInfo)
	}
	return colList, nil
}

// getFullAnalyzeColumnsInfo decides which columns need to be analyzed.
// The first return value is the columns which need to be analyzed and the second return value is the columns which need to
// be record in mysql.analyze_options(only for the case of analyze table t columns c1, .., cn).
func (b *PlanBuilder) getFullAnalyzeColumnsInfo(
	tbl *resolve.TableNameW,
	columnChoice ast.ColumnChoice,
	specifiedCols []*model.ColumnInfo,
	predicateCols, mustAnalyzedCols *calcOnceMap,
	mustAllColumns bool,
	warning bool,
) (_, _ []*model.ColumnInfo, _ error) {
	if mustAllColumns && warning && (columnChoice == ast.PredicateColumns || columnChoice == ast.ColumnList) {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("Table %s.%s has version 1 statistics so all the columns must be analyzed to overwrite the current statistics", tbl.Schema.L, tbl.Name.L))
	}

	switch columnChoice {
	case ast.DefaultChoice:
		columnOptions := vardef.AnalyzeColumnOptions.Load()
		switch columnOptions {
		case ast.AllColumns.String():
			return tbl.TableInfo.Columns, nil, nil
		case ast.PredicateColumns.String():
			columns, err := b.getColumnsBasedOnPredicateColumns(
				tbl,
				predicateCols,
				mustAnalyzedCols,
				mustAllColumns,
			)
			if err != nil {
				return nil, nil, err
			}
			return columns, nil, nil
		default:
			// Usually, this won't happen.
			statslogutil.StatsLogger().Warn("Unknown default column choice, analyze all columns", zap.String("choice", columnOptions))
			return tbl.TableInfo.Columns, nil, nil
		}
	case ast.AllColumns:
		return tbl.TableInfo.Columns, nil, nil
	case ast.PredicateColumns:
		columns, err := b.getColumnsBasedOnPredicateColumns(
			tbl,
			predicateCols,
			mustAnalyzedCols,
			mustAllColumns,
		)
		if err != nil {
			return nil, nil, err
		}
		return columns, nil, nil
	case ast.ColumnList:
		colSet := getColumnSetFromSpecifiedCols(specifiedCols)
		mustAnalyzed, err := b.getMustAnalyzedColumns(tbl, mustAnalyzedCols)
		if err != nil {
			return nil, nil, err
		}
		if warning {
			missing := getMissingColumns(colSet, mustAnalyzed)
			if len(missing) > 0 {
				missingNames := getColumnNamesFromIDs(tbl.TableInfo.Columns, missing)
				warningMsg := fmt.Sprintf("Columns %s are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats", strings.Join(missingNames, ","))
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError(warningMsg))
			}
		}
		colSet = combineColumnSets(colSet, mustAnalyzed)
		colList := getColumnListFromSet(tbl.TableInfo.Columns, colSet)
		if mustAllColumns {
			return tbl.TableInfo.Columns, colList, nil
		}
		return colList, colList, nil
	}

	return nil, nil, nil
}
func (b *PlanBuilder) getColumnsBasedOnPredicateColumns(
	tbl *resolve.TableNameW,
	predicateCols, mustAnalyzedCols *calcOnceMap,
	rewriteAllStatsNeeded bool,
) ([]*model.ColumnInfo, error) {
	if rewriteAllStatsNeeded {
		return tbl.TableInfo.Columns, nil
	}
	predicate, err := b.getPredicateColumns(tbl, predicateCols)
	if err != nil {
		return nil, err
	}
	mustAnalyzed, err := b.getMustAnalyzedColumns(tbl, mustAnalyzedCols)
	if err != nil {
		return nil, err
	}
	colSet := combineColumnSets(predicate, mustAnalyzed)
	return getColumnListFromSet(tbl.TableInfo.Columns, colSet), nil
}

// Helper function to combine two column sets.
func combineColumnSets(sets ...map[int64]struct{}) map[int64]struct{} {
	result := make(map[int64]struct{})
	for _, set := range sets {
		for colID := range set {
			result[colID] = struct{}{}
		}
	}
	return result
}

// Helper function to extract column IDs from specified columns.
func getColumnSetFromSpecifiedCols(cols []*model.ColumnInfo) map[int64]struct{} {
	colSet := make(map[int64]struct{}, len(cols))
	for _, colInfo := range cols {
		colSet[colInfo.ID] = struct{}{}
	}
	return colSet
}

// Helper function to get missing columns from a set.
func getMissingColumns(colSet, mustAnalyzed map[int64]struct{}) map[int64]struct{} {
	missing := make(map[int64]struct{})
	for colID := range mustAnalyzed {
		if _, ok := colSet[colID]; !ok {
			missing[colID] = struct{}{}
		}
	}
	return missing
}

// Helper function to get column names from IDs.
func getColumnNamesFromIDs(columns []*model.ColumnInfo, colIDs map[int64]struct{}) []string {
	var missingNames []string
	for _, col := range columns {
		if _, ok := colIDs[col.ID]; ok {
			missingNames = append(missingNames, col.Name.O)
		}
	}
	return missingNames
}

// Helper function to get a list of column infos from a set of column IDs.
func getColumnListFromSet(columns []*model.ColumnInfo, colSet map[int64]struct{}) []*model.ColumnInfo {
	colList := make([]*model.ColumnInfo, 0, len(colSet))
	for _, colInfo := range columns {
		if _, ok := colSet[colInfo.ID]; ok {
			colList = append(colList, colInfo)
		}
	}
	return colList
}

