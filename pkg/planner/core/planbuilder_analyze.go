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
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
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

func getColOffsetForAnalyze(colsInfo []*model.ColumnInfo, colID int64) int {
	for i, col := range colsInfo {
		if colID == col.ID {
			return i
		}
	}
	return -1
}

// getModifiedIndexesInfoForAnalyze returns indexesInfo for ANALYZE.
// 1. If allColumns is true, we just return public indexes in tblInfo.Indices.
// 2. If allColumns is false, colsInfo indicate the columns whose stats need to be collected. colsInfo is a subset of tbl.Columns. For each public index
// in tblInfo.Indices, index.Columns[i].Offset is set according to tblInfo.Columns. Since we decode row samples according to colsInfo rather than tbl.Columns
// in the execution phase of ANALYZE, we need to modify index.Columns[i].Offset according to colInfos.
// TODO: find a better way to find indexed columns in ANALYZE rather than use IndexColumn.Offset
// For multi-valued index, we need to collect it separately here and analyze it as independent index analyze task.
// For a special global index, we also need to analyze it as independent index analyze task.
// See comments for AnalyzeResults.ForMVIndex for more details.
func getModifiedIndexesInfoForAnalyze(
	sCtx base.PlanContext,
	tblInfo *model.TableInfo,
	allColumns bool,
	colsInfo []*model.ColumnInfo,
) (idxsInfo, independentIdxsInfo, specialGlobalIdxsInfo []*model.IndexInfo) {
	idxsInfo = make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	independentIdxsInfo = make([]*model.IndexInfo, 0)
	specialGlobalIdxsInfo = make([]*model.IndexInfo, 0)
	for _, originIdx := range tblInfo.Indices {
		// for an index in state public, we can always analyze it for internal analyze session.
		// for an index in state WriteReorg, we can only analyze it under variable EnableDDLAnalyze is on.
		indexStateAnalyzable := originIdx.State == model.StatePublic ||
			(originIdx.State == model.StateWriteReorganization && sCtx.GetSessionVars().EnableDDLAnalyzeExecOpt)
		if !indexStateAnalyzable {
			continue
		}
		if handleutil.IsSpecialGlobalIndex(originIdx, tblInfo) {
			specialGlobalIdxsInfo = append(specialGlobalIdxsInfo, originIdx)
			continue
		}
		if originIdx.MVIndex {
			independentIdxsInfo = append(independentIdxsInfo, originIdx)
			continue
		}
		if originIdx.IsColumnarIndex() {
			sCtx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing columnar index is not supported, skip %s", originIdx.Name.L))
			continue
		}
		if allColumns {
			// If all the columns need to be analyzed, we don't need to modify IndexColumn.Offset.
			idxsInfo = append(idxsInfo, originIdx)
			continue
		}
		// If only a part of the columns need to be analyzed, we need to set IndexColumn.Offset according to colsInfo.
		idx := originIdx.Clone()
		for i, idxCol := range idx.Columns {
			colID := tblInfo.Columns[idxCol.Offset].ID
			idx.Columns[i].Offset = getColOffsetForAnalyze(colsInfo, colID)
		}
		idxsInfo = append(idxsInfo, idx)
	}
	return idxsInfo, independentIdxsInfo, specialGlobalIdxsInfo
}

// filterSkipColumnTypes filters out columns whose types are in the skipTypes list.
func (b *PlanBuilder) filterSkipColumnTypes(origin []*model.ColumnInfo, tbl *resolve.TableNameW, mustAnalyzedCols *calcOnceMap) (result []*model.ColumnInfo, skipCol []*model.ColumnInfo) {
	// For auto-analyze, it uses @@global.tidb_analyze_skip_column_types to obtain the skipTypes list.
	// This is already handled before executing the query by the CallWithSCtx utility function.
	skipTypes := b.ctx.GetSessionVars().AnalyzeSkipColumnTypes
	mustAnalyze, err1 := b.getMustAnalyzedColumns(tbl, mustAnalyzedCols)
	if err1 != nil {
		logutil.BgLogger().Error("getting must-analyzed columns failed", zap.Error(err1))
		result = origin
		return
	}
	// If one column's type is in the skipTypes list and it doesn't exist in mustAnalyzedCols, we will skip it.
	for _, colInfo := range origin {
		// Vector type is skip by hardcoded. Just because that collecting it is meanless for current TiDB.
		if colInfo.FieldType.GetType() == mysql.TypeTiDBVectorFloat32 {
			skipCol = append(skipCol, colInfo)
			continue
		}
		_, skip := skipTypes[types.TypeToStr(colInfo.FieldType.GetType(), colInfo.FieldType.GetCharset())]
		// Currently, if the column exists in some index(except MV Index), we need to bring the column's sample values
		// into TiDB to build the index statistics.
		_, keep := mustAnalyze[colInfo.ID]
		if skip && !keep {
			skipCol = append(skipCol, colInfo)
			continue
		}
		result = append(result, colInfo)
	}
	skipColNameMap := make(map[string]struct{}, len(skipCol))
	for _, colInfo := range skipCol {
		skipColNameMap[colInfo.Name.L] = struct{}{}
	}

	// Filter out generated columns that depend on skipped columns
	filteredResult := make([]*model.ColumnInfo, 0, len(result))
	for _, colInfo := range result {
		shouldSkip := false
		if colInfo.IsGenerated() {
			// Check if any dependency is in the skip list
			for depName := range colInfo.Dependences {
				if _, exists := skipColNameMap[depName]; exists {
					skipCol = append(skipCol, colInfo)
					shouldSkip = true
					break
				}
			}
		}
		if !shouldSkip {
			filteredResult = append(filteredResult, colInfo)
		}
	}
	result = filteredResult
	return
}

// This function is to check whether all indexes is special global index or not.
// A special global index is an index that is both a global index and an expression index or a prefix index.
func checkIsAllSpecialGlobalIndex(as *ast.AnalyzeTableStmt, tbl *resolve.TableNameW) (bool, error) {
	isAnalyzeTable := len(as.PartitionNames) == 0

	// For `Analyze table t index`
	if as.IndexFlag && len(as.IndexNames) == 0 {
		for _, idx := range tbl.TableInfo.Indices {
			if idx.State != model.StatePublic {
				continue
			}
			if !handleutil.IsSpecialGlobalIndex(idx, tbl.TableInfo) {
				return false, nil
			}
			// For `Analyze table t partition p0 index`
			if !isAnalyzeTable {
				return false, errors.NewNoStackErrorf("Analyze global index '%s' can't work with analyze specified partitions", idx.Name.O)
			}
		}
	} else {
		for _, idxName := range as.IndexNames {
			idx := tbl.TableInfo.FindIndexByName(idxName.L)
			if idx == nil || idx.State != model.StatePublic {
				return false, plannererrors.ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tbl.Name.O)
			}
			if !handleutil.IsSpecialGlobalIndex(idx, tbl.TableInfo) {
				return false, nil
			}
			// For `Analyze table t partition p0 index idx0`
			if !isAnalyzeTable {
				return false, errors.NewNoStackErrorf("Analyze global index '%s' can't work with analyze specified partitions", idx.Name.O)
			}
		}
	}
	return true, nil
}

func (b *PlanBuilder) buildAnalyzeFullSamplingTask(
	as *ast.AnalyzeTableStmt,
	analyzePlan *Analyze,
	physicalIDs []int64,
	partitionNames []string,
	tbl *resolve.TableNameW,
	version int,
	persistOpts bool,
) error {
	// Version 2 doesn't support incremental analyze.
	// And incremental analyze will be deprecated in the future.
	if as.Incremental {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The version 2 stats would ignore the INCREMENTAL keyword and do full sampling"))
	}

	isAnalyzeTable := len(as.PartitionNames) == 0

	allSpecialGlobalIndex, err := checkIsAllSpecialGlobalIndex(as, tbl)
	if err != nil {
		return err
	}

	astOpts, err := handleAnalyzeOptionsV2(as.AnalyzeOpts)
	if err != nil {
		return err
	}
	// Get all column info which need to be analyzed.
	astColList, err := getAnalyzeColumnList(as.ColumnNames, tbl)
	if err != nil {
		return err
	}

	var predicateCols, mustAnalyzedCols calcOnceMap
	ver := version
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	// If the statistics of the table is version 1, we must analyze all columns to overwrites all of old statistics.
	mustAllColumns := !statsHandle.CheckAnalyzeVersion(tbl.TableInfo, physicalIDs, &ver)

	astColsInfo, _, err := b.getFullAnalyzeColumnsInfo(tbl, as.ColumnChoice, astColList, &predicateCols, &mustAnalyzedCols, mustAllColumns, true)
	if err != nil {
		return err
	}

	optionsMap, colsInfoMap, err := b.genV2AnalyzeOptions(persistOpts, tbl, isAnalyzeTable, physicalIDs, astOpts, as.ColumnChoice, astColList, &predicateCols, &mustAnalyzedCols, mustAllColumns)
	if err != nil {
		return err
	}
	maps.Copy(analyzePlan.OptionsMap, optionsMap)

	var indexes, independentIndexes, specialGlobalIndexes []*model.IndexInfo

	needAnalyzeCols := !(as.IndexFlag && allSpecialGlobalIndex)

	if needAnalyzeCols {
		if as.IndexFlag {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("The version 2 would collect all statistics not only the selected indexes"))
		}
		// Build tasks for each partition.
		for i, id := range physicalIDs {
			physicalID := id
			if id == tbl.TableInfo.ID {
				id = statistics.NonPartitionTableID
			}
			info := AnalyzeInfo{
				DBName:        tbl.Schema.O,
				TableName:     tbl.Name.O,
				PartitionName: partitionNames[i],
				TableID:       statistics.AnalyzeTableID{TableID: tbl.TableInfo.ID, PartitionID: id},
				StatsVersion:  version,
			}
			if optsV2, ok := optionsMap[physicalID]; ok {
				info.V2Options = &optsV2
			}
			execColsInfo := astColsInfo
			if colsInfo, ok := colsInfoMap[physicalID]; ok {
				execColsInfo = colsInfo
			}
			var skipColsInfo []*model.ColumnInfo
			execColsInfo, skipColsInfo = b.filterSkipColumnTypes(execColsInfo, tbl, &mustAnalyzedCols)
			allColumns := len(tbl.TableInfo.Columns) == len(execColsInfo)
			indexes, independentIndexes, specialGlobalIndexes = getModifiedIndexesInfoForAnalyze(b.ctx, tbl.TableInfo, allColumns, execColsInfo)
			handleCols := BuildHandleColsForAnalyze(b.ctx, tbl.TableInfo, allColumns, execColsInfo)
			newTask := AnalyzeColumnsTask{
				HandleCols:   handleCols,
				ColsInfo:     execColsInfo,
				AnalyzeInfo:  info,
				TblInfo:      tbl.TableInfo,
				Indexes:      indexes,
				SkipColsInfo: skipColsInfo,
			}
			if newTask.HandleCols == nil {
				extraCol := model.NewExtraHandleColInfo()
				// Always place _tidb_rowid at the end of colsInfo, this is corresponding to logics in `analyzeColumnsPushdown`.
				newTask.ColsInfo = append(newTask.ColsInfo, extraCol)
				newTask.HandleCols = util.NewIntHandleCols(colInfoToColumn(extraCol, len(newTask.ColsInfo)-1))
			}
			analyzePlan.ColTasks = append(analyzePlan.ColTasks, newTask)
			for _, indexInfo := range independentIndexes {
				newIdxTask := AnalyzeIndexTask{
					IndexInfo:   indexInfo,
					TblInfo:     tbl.TableInfo,
					AnalyzeInfo: info,
				}
				analyzePlan.IdxTasks = append(analyzePlan.IdxTasks, newIdxTask)
			}
		}
	}

	if isAnalyzeTable {
		if needAnalyzeCols {
			// When `needAnalyzeCols == true`, non-global indexes already covered by previous loop,
			// deal with global index here.
			for _, indexInfo := range specialGlobalIndexes {
				analyzePlan.IdxTasks = append(analyzePlan.IdxTasks, generateIndexTasks(indexInfo, as, tbl.TableInfo, nil, nil, version)...)
			}
		} else {
			// For `analyze table t index idx1[, idx2]` and all indexes are global index.
			for _, idxName := range as.IndexNames {
				idx := tbl.TableInfo.FindIndexByName(idxName.L)
				if idx == nil || !handleutil.IsSpecialGlobalIndex(idx, tbl.TableInfo) {
					continue
				}
				analyzePlan.IdxTasks = append(analyzePlan.IdxTasks, generateIndexTasks(idx, as, tbl.TableInfo, nil, nil, version)...)
			}
		}
	}

	return nil
}

func (b *PlanBuilder) genV2AnalyzeOptions(
	persist bool,
	tbl *resolve.TableNameW,
	isAnalyzeTable bool,
	physicalIDs []int64,
	astOpts map[ast.AnalyzeOptionType]uint64,
	astColChoice ast.ColumnChoice,
	astColList []*model.ColumnInfo,
	predicateCols, mustAnalyzedCols *calcOnceMap,
	mustAllColumns bool,
) (map[int64]V2AnalyzeOptions, map[int64][]*model.ColumnInfo, error) {
	optionsMap := make(map[int64]V2AnalyzeOptions, len(physicalIDs))
	colsInfoMap := make(map[int64][]*model.ColumnInfo, len(physicalIDs))
	if !persist {
		return optionsMap, colsInfoMap, nil
	}

	// In dynamic mode, we collect statistics for all partitions of the table as a global statistics.
	// In static mode, each partition generates its own execution plan, which is then combined with PartitionUnion.
	// Because the plan is generated for each partition individually, each partition uses its own statistics;
	// In dynamic mode, there is no partitioning, and a global plan is generated for the whole table, so a global statistic is needed;
	dynamicPrune := variable.PartitionPruneMode(b.ctx.GetSessionVars().PartitionPruneMode.Load()) == variable.Dynamic
	if !isAnalyzeTable && dynamicPrune && (len(astOpts) > 0 || astColChoice != ast.DefaultChoice) {
		astOpts = make(map[ast.AnalyzeOptionType]uint64, 0)
		astColChoice = ast.DefaultChoice
		astColList = make([]*model.ColumnInfo, 0)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("Ignore columns and options when analyze partition in dynamic mode"))
	}

	// Get the analyze options which are saved in mysql.analyze_options.
	tblSavedOpts, tblSavedColChoice, tblSavedColList, err := b.getSavedAnalyzeOpts(tbl.TableInfo.ID, tbl.TableInfo)
	if err != nil {
		return nil, nil, err
	}
	tblOpts := tblSavedOpts
	tblColChoice := tblSavedColChoice
	tblColList := tblSavedColList
	if isAnalyzeTable {
		tblOpts = mergeAnalyzeOptions(astOpts, tblSavedOpts)
		tblColChoice, tblColList = pickColumnList(astColChoice, astColList, tblSavedColChoice, tblSavedColList)
	}

	tblFilledOpts := fillAnalyzeOptionsV2(tblOpts)

	tblColsInfo, tblColList, err := b.getFullAnalyzeColumnsInfo(tbl, tblColChoice, tblColList, predicateCols, mustAnalyzedCols, mustAllColumns, false)
	if err != nil {
		return nil, nil, err
	}

	tblAnalyzeOptions := V2AnalyzeOptions{
		PhyTableID:  tbl.TableInfo.ID,
		RawOpts:     tblOpts,
		FilledOpts:  tblFilledOpts,
		ColChoice:   tblColChoice,
		ColumnList:  tblColList,
		IsPartition: false,
	}
	optionsMap[tbl.TableInfo.ID] = tblAnalyzeOptions
	colsInfoMap[tbl.TableInfo.ID] = tblColsInfo

	for _, physicalID := range physicalIDs {
		// This is a partitioned table, we need to collect statistics for each partition.
		if physicalID != tbl.TableInfo.ID {
			// In dynamic mode, we collect statistics for all partitions of the table as a global statistics.
			// So we use the same options as the table level.
			if dynamicPrune {
				parV2Options := V2AnalyzeOptions{
					PhyTableID:  physicalID,
					RawOpts:     tblOpts,
					FilledOpts:  tblFilledOpts,
					ColChoice:   tblColChoice,
					ColumnList:  tblColList,
					IsPartition: true,
				}
				optionsMap[physicalID] = parV2Options
				colsInfoMap[physicalID] = tblColsInfo
				continue
			}
			parSavedOpts, parSavedColChoice, parSavedColList, err := b.getSavedAnalyzeOpts(physicalID, tbl.TableInfo)
			if err != nil {
				return nil, nil, err
			}
			// merge partition level options with table level options firstly
			savedOpts := mergeAnalyzeOptions(parSavedOpts, tblSavedOpts)
			savedColChoice, savedColList := pickColumnList(parSavedColChoice, parSavedColList, tblSavedColChoice, tblSavedColList)
			// then merge statement level options
			mergedOpts := mergeAnalyzeOptions(astOpts, savedOpts)
			filledMergedOpts := fillAnalyzeOptionsV2(mergedOpts)
			finalColChoice, mergedColList := pickColumnList(astColChoice, astColList, savedColChoice, savedColList)
			finalColsInfo, finalColList, err := b.getFullAnalyzeColumnsInfo(tbl, finalColChoice, mergedColList, predicateCols, mustAnalyzedCols, mustAllColumns, false)
			if err != nil {
				return nil, nil, err
			}
			parV2Options := V2AnalyzeOptions{
				PhyTableID: physicalID,
				RawOpts:    mergedOpts,
				FilledOpts: filledMergedOpts,
				ColChoice:  finalColChoice,
				ColumnList: finalColList,
			}
			optionsMap[physicalID] = parV2Options
			colsInfoMap[physicalID] = finalColsInfo
		}
	}

	return optionsMap, colsInfoMap, nil
}

// getSavedAnalyzeOpts gets the analyze options which are saved in mysql.analyze_options.
func (b *PlanBuilder) getSavedAnalyzeOpts(physicalID int64, tblInfo *model.TableInfo) (map[ast.AnalyzeOptionType]uint64, ast.ColumnChoice, []*model.ColumnInfo, error) {
	analyzeOptions := map[ast.AnalyzeOptionType]uint64{}
	exec := b.ctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select sample_num,sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id = %?", physicalID)
	if err != nil {
		return nil, ast.DefaultChoice, nil, err
	}
	if len(rows) <= 0 {
		return analyzeOptions, ast.DefaultChoice, nil, nil
	}

	row := rows[0]
	sampleNum := row.GetInt64(0)
	if sampleNum > 0 {
		analyzeOptions[ast.AnalyzeOptNumSamples] = uint64(sampleNum)
	}
	sampleRate := row.GetFloat64(1)
	if sampleRate > 0 {
		analyzeOptions[ast.AnalyzeOptSampleRate] = math.Float64bits(sampleRate)
	}
	buckets := row.GetInt64(2)
	if buckets > 0 {
		analyzeOptions[ast.AnalyzeOptNumBuckets] = uint64(buckets)
	}
	topn := row.GetInt64(3)
	if topn >= 0 {
		analyzeOptions[ast.AnalyzeOptNumTopN] = uint64(topn)
	}
	colType := row.GetEnum(4)
	switch colType.Name {
	case "ALL":
		return analyzeOptions, ast.AllColumns, tblInfo.Columns, nil
	case "LIST":
		colIDStrs := strings.Split(row.GetString(5), ",")
		colList := make([]*model.ColumnInfo, 0, len(colIDStrs))
		for _, colIDStr := range colIDStrs {
			colID, _ := strconv.ParseInt(colIDStr, 10, 64)
			colInfo := model.FindColumnInfoByID(tblInfo.Columns, colID)
			if colInfo != nil {
				colList = append(colList, colInfo)
			}
		}
		return analyzeOptions, ast.ColumnList, colList, nil
	case "PREDICATE":
		return analyzeOptions, ast.PredicateColumns, nil, nil
	default:
		return analyzeOptions, ast.DefaultChoice, nil, nil
	}
}

func mergeAnalyzeOptions(stmtOpts map[ast.AnalyzeOptionType]uint64, savedOpts map[ast.AnalyzeOptionType]uint64) map[ast.AnalyzeOptionType]uint64 {
	merged := map[ast.AnalyzeOptionType]uint64{}
	for optType := range ast.AnalyzeOptionString {
		if stmtOpt, ok := stmtOpts[optType]; ok {
			merged[optType] = stmtOpt
		} else if savedOpt, ok := savedOpts[optType]; ok {
			merged[optType] = savedOpt
		}
	}
	return merged
}

// pickColumnList picks the column list to be analyzed.
// If the column list is specified in the statement, we will use it.
func pickColumnList(astColChoice ast.ColumnChoice, astColList []*model.ColumnInfo, tblSavedColChoice ast.ColumnChoice, tblSavedColList []*model.ColumnInfo) (ast.ColumnChoice, []*model.ColumnInfo) {
	if astColChoice != ast.DefaultChoice {
		return astColChoice, astColList
	}
	return tblSavedColChoice, tblSavedColList
}

// buildAnalyzeTable constructs analyze tasks for each table.
func (b *PlanBuilder) buildAnalyzeTable(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (base.Plan, error) {
	p := &Analyze{Opts: opts}
	p.OptionsMap = make(map[int64]V2AnalyzeOptions)
	usePersistedOptions := vardef.PersistAnalyzeOptions.Load()

	// Construct tasks for each table.
	for _, tbl := range as.TableNames {
		tnW := b.resolveCtx.GetTableName(tbl)
		if tnW.TableInfo.IsView() {
			return nil, errors.Errorf("analyze view %s is not supported now", tbl.Name.O)
		}
		if tnW.TableInfo.IsSequence() {
			return nil, errors.Errorf("analyze sequence %s is not supported now", tbl.Name.O)
		}

		idxInfo, colInfo := b.getColsInfo(tbl)
		physicalIDs, partitionNames, err := GetPhysicalIDsAndPartitionNames(tnW.TableInfo, as.PartitionNames)
		if err != nil {
			return nil, err
		}
		var commonHandleInfo *model.IndexInfo
		if version == statistics.Version2 {
			err = b.buildAnalyzeFullSamplingTask(as, p, physicalIDs, partitionNames, tnW, version, usePersistedOptions)
			if err != nil {
				return nil, err
			}
			continue
		}

		// Version 1 analyze.
		if as.ColumnChoice == ast.PredicateColumns {
			return nil, errors.Errorf("Only the version 2 of analyze supports analyzing predicate columns")
		}
		if as.ColumnChoice == ast.ColumnList {
			return nil, errors.Errorf("Only the version 2 of analyze supports analyzing the specified columns")
		}
		for _, idx := range idxInfo {
			// For prefix common handle. We don't use analyze mixed to handle it with columns. Because the full value
			// is read by coprocessor, the prefix index would get wrong stats in this case.
			if idx.Primary && tnW.TableInfo.IsCommonHandle && !idx.HasPrefixIndex() {
				commonHandleInfo = idx
				continue
			}
			if idx.MVIndex {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing multi-valued indexes is not supported, skip %s", idx.Name.L))
				continue
			}
			if idx.IsColumnarIndex() {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing columnar index is not supported, skip %s", idx.Name.L))
				continue
			}
			p.IdxTasks = append(p.IdxTasks, generateIndexTasks(idx, as, tnW.TableInfo, partitionNames, physicalIDs, version)...)
		}
		handleCols := BuildHandleColsForAnalyze(b.ctx, tnW.TableInfo, true, nil)
		if len(colInfo) > 0 || handleCols != nil {
			for i, id := range physicalIDs {
				if id == tnW.TableInfo.ID {
					id = statistics.NonPartitionTableID
				}
				info := AnalyzeInfo{
					DBName:        tbl.Schema.O,
					TableName:     tbl.Name.O,
					PartitionName: partitionNames[i],
					TableID:       statistics.AnalyzeTableID{TableID: tnW.TableInfo.ID, PartitionID: id},
					StatsVersion:  version,
				}
				p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{
					HandleCols:       handleCols,
					CommonHandleInfo: commonHandleInfo,
					ColsInfo:         colInfo,
					AnalyzeInfo:      info,
					TblInfo:          tnW.TableInfo,
				})
			}
		}
	}

	return p, nil
}

func (b *PlanBuilder) buildAnalyzeIndex(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (base.Plan, error) {
	p := &Analyze{Opts: opts}
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	if statsHandle == nil {
		return nil, errors.Errorf("statistics hasn't been initialized, please try again later")
	}
	tnW := b.resolveCtx.GetTableName(as.TableNames[0])
	tblInfo := tnW.TableInfo
	physicalIDs, names, err := GetPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	versionIsSame := statsHandle.CheckAnalyzeVersion(tblInfo, physicalIDs, &version)
	if !versionIsSame {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	}
	if version == statistics.Version2 {
		return b.buildAnalyzeTable(as, opts, version)
	}
	for _, idxName := range as.IndexNames {
		if isPrimaryIndex(idxName) {
			handleCols := BuildHandleColsForAnalyze(b.ctx, tblInfo, true, nil)
			// FIXME: How about non-int primary key?
			if handleCols != nil && handleCols.IsInt() {
				for i, id := range physicalIDs {
					if id == tblInfo.ID {
						id = statistics.NonPartitionTableID
					}
					info := AnalyzeInfo{
						DBName:        as.TableNames[0].Schema.O,
						TableName:     as.TableNames[0].Name.O,
						PartitionName: names[i], TableID: statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
						StatsVersion: version,
					}
					p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{HandleCols: handleCols, AnalyzeInfo: info, TblInfo: tblInfo})
				}
				continue
			}
		}
		idx := tblInfo.FindIndexByName(idxName.L)
		if idx == nil || idx.State != model.StatePublic {
			return nil, plannererrors.ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tblInfo.Name.O)
		}
		if idx.MVIndex {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing multi-valued indexes is not supported, skip %s", idx.Name.L))
			continue
		}
		if idx.IsColumnarIndex() {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing columnar index is not supported, skip %s", idx.Name.L))
			continue
		}
		p.IdxTasks = append(p.IdxTasks, generateIndexTasks(idx, as, tblInfo, names, physicalIDs, version)...)
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (base.Plan, error) {
	p := &Analyze{Opts: opts}
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	if statsHandle == nil {
		return nil, errors.Errorf("statistics hasn't been initialized, please try again later")
	}
	tnW := b.resolveCtx.GetTableName(as.TableNames[0])
	tblInfo := tnW.TableInfo
	physicalIDs, names, err := GetPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	versionIsSame := statsHandle.CheckAnalyzeVersion(tblInfo, physicalIDs, &version)
	if !versionIsSame {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	}
	if version == statistics.Version2 {
		return b.buildAnalyzeTable(as, opts, version)
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			if idx.MVIndex {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing multi-valued indexes is not supported, skip %s", idx.Name.L))
				continue
			}
			if idx.IsColumnarIndex() {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing columnar index is not supported, skip %s", idx.Name.L))
				continue
			}

			p.IdxTasks = append(p.IdxTasks, generateIndexTasks(idx, as, tblInfo, names, physicalIDs, version)...)
		}
	}
	handleCols := BuildHandleColsForAnalyze(b.ctx, tblInfo, true, nil)
	if handleCols != nil {
		for i, id := range physicalIDs {
			if id == tblInfo.ID {
				id = statistics.NonPartitionTableID
			}
			info := AnalyzeInfo{
				DBName:        as.TableNames[0].Schema.O,
				TableName:     as.TableNames[0].Name.O,
				PartitionName: names[i],
				TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
				StatsVersion:  version,
			}
			p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{HandleCols: handleCols, AnalyzeInfo: info, TblInfo: tblInfo})
		}
	}
	return p, nil
}

func generateIndexTasks(idx *model.IndexInfo, as *ast.AnalyzeTableStmt, tblInfo *model.TableInfo, names []string, physicalIDs []int64, version int) []AnalyzeIndexTask {
	if idx.Global {
		info := AnalyzeInfo{
			DBName:        as.TableNames[0].Schema.O,
			TableName:     as.TableNames[0].Name.O,
			PartitionName: "",
			TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: statistics.NonPartitionTableID},
			StatsVersion:  version,
		}
		return []AnalyzeIndexTask{{IndexInfo: idx, AnalyzeInfo: info, TblInfo: tblInfo}}
	}

	indexTasks := make([]AnalyzeIndexTask, 0, len(physicalIDs))
	for i, id := range physicalIDs {
		if id == tblInfo.ID {
			id = statistics.NonPartitionTableID
		}
		info := AnalyzeInfo{
			DBName:        as.TableNames[0].Schema.O,
			TableName:     as.TableNames[0].Name.O,
			PartitionName: names[i],
			TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
			StatsVersion:  version,
		}
		indexTasks = append(indexTasks, AnalyzeIndexTask{IndexInfo: idx, AnalyzeInfo: info, TblInfo: tblInfo})
	}
	return indexTasks
}

// CMSketchSizeLimit indicates the size limit of CMSketch.
var CMSketchSizeLimit = kv.TxnEntrySizeLimit.Load() / binary.MaxVarintLen32

var analyzeOptionLimit = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    100000,
	ast.AnalyzeOptNumTopN:       100000,
	ast.AnalyzeOptCMSketchWidth: CMSketchSizeLimit,
	ast.AnalyzeOptCMSketchDepth: CMSketchSizeLimit,
	ast.AnalyzeOptNumSamples:    5000000,
	ast.AnalyzeOptSampleRate:    math.Float64bits(1),
}

// TODO(0xPoe): give some explanation about the default value.
var analyzeOptionDefault = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    256,
	ast.AnalyzeOptNumTopN:       20,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    10000,
	ast.AnalyzeOptSampleRate:    math.Float64bits(0),
}

// TopN reduced from 500 to 100 due to concerns over large number of TopN values collected for customers with many tables.
// 100 is more inline with other databases. 100-256 is also common for NumBuckets with other databases.
var analyzeOptionDefaultV2 = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    statistics.DefaultHistogramBuckets,
	ast.AnalyzeOptNumTopN:       statistics.DefaultTopNValue,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    0,
	ast.AnalyzeOptSampleRate:    math.Float64bits(-1),
}

// GetAnalyzeOptionDefaultV2ForTest returns the default analyze options for test.
func GetAnalyzeOptionDefaultV2ForTest() map[ast.AnalyzeOptionType]uint64 {
	return analyzeOptionDefaultV2
}

// This function very similar to handleAnalyzeOptions, but it's used for analyze version 2.
// Remove this function after we remove the support of analyze version 1.
func handleAnalyzeOptionsV2(opts []ast.AnalyzeOpt) (map[ast.AnalyzeOptionType]uint64, error) {
	optMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	sampleNum, sampleRate := uint64(0), 0.0
	for _, opt := range opts {
		datumValue := opt.Value.(*driver.ValueExpr).Datum
		switch opt.Type {
		case ast.AnalyzeOptNumTopN:
			v := datumValue.GetUint64()
			if v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should not be larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		case ast.AnalyzeOptSampleRate:
			// Only Int/Float/decimal is accepted, so pass nil here is safe.
			fVal, err := datumValue.ToFloat64(types.DefaultStmtNoWarningContext)
			if err != nil {
				return nil, err
			}
			limit := math.Float64frombits(analyzeOptionLimit[opt.Type])
			if fVal <= 0 || fVal > limit {
				return nil, errors.Errorf("Value of analyze option %s should not larger than %f, and should be greater than 0", ast.AnalyzeOptionString[opt.Type], limit)
			}
			sampleRate = fVal
			optMap[opt.Type] = math.Float64bits(fVal)
		default:
			v := datumValue.GetUint64()
			if opt.Type == ast.AnalyzeOptNumSamples {
				sampleNum = v
			}
			if v == 0 || v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should be positive and not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		}
	}
	if sampleNum > 0 && sampleRate > 0 {
		return nil, errors.Errorf("You can only either set the value of the sample num or set the value of the sample rate. Don't set both of them")
	}

	return optMap, nil
}

func fillAnalyzeOptionsV2(optMap map[ast.AnalyzeOptionType]uint64) map[ast.AnalyzeOptionType]uint64 {
	filledMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	for key, defaultVal := range analyzeOptionDefaultV2 {
		if val, ok := optMap[key]; ok {
			filledMap[key] = val
		} else {
			filledMap[key] = defaultVal
		}
	}
	return filledMap
}

func handleAnalyzeOptions(opts []ast.AnalyzeOpt, statsVer int) (map[ast.AnalyzeOptionType]uint64, error) {
	optMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	if statsVer == statistics.Version1 {
		maps.Copy(optMap, analyzeOptionDefault)
	} else {
		maps.Copy(optMap, analyzeOptionDefaultV2)
	}
	sampleNum, sampleRate := uint64(0), 0.0
	for _, opt := range opts {
		datumValue := opt.Value.(*driver.ValueExpr).Datum
		switch opt.Type {
		case ast.AnalyzeOptNumTopN:
			v := datumValue.GetUint64()
			if v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should not be larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		case ast.AnalyzeOptSampleRate:
			// Only Int/Float/decimal is accepted, so pass nil here is safe.
			fVal, err := datumValue.ToFloat64(types.DefaultStmtNoWarningContext)
			if err != nil {
				return nil, err
			}
			if fVal > 0 && statsVer == statistics.Version1 {
				return nil, errors.Errorf("Version 1's statistics doesn't support the SAMPLERATE option, please set tidb_analyze_version to 2")
			}
			limit := math.Float64frombits(analyzeOptionLimit[opt.Type])
			if fVal <= 0 || fVal > limit {
				return nil, errors.Errorf("Value of analyze option %s should not larger than %f, and should be greater than 0", ast.AnalyzeOptionString[opt.Type], limit)
			}
			sampleRate = fVal
			optMap[opt.Type] = math.Float64bits(fVal)
		default:
			v := datumValue.GetUint64()
			if opt.Type == ast.AnalyzeOptNumSamples {
				sampleNum = v
			}
			if v == 0 || v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should be positive and not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		}
	}
	if sampleNum > 0 && sampleRate > 0 {
		return nil, errors.Errorf("You can only either set the value of the sample num or set the value of the sample rate. Don't set both of them")
	}
	// Only version 1 has cmsketch.
	if statsVer == statistics.Version1 && optMap[ast.AnalyzeOptCMSketchWidth]*optMap[ast.AnalyzeOptCMSketchDepth] > CMSketchSizeLimit {
		return nil, errors.Errorf("cm sketch size(depth * width) should not larger than %d", CMSketchSizeLimit)
	}
	return optMap, nil
}

func (b *PlanBuilder) buildAnalyze(as *ast.AnalyzeTableStmt) (base.Plan, error) {
	if as.NoWriteToBinLog {
		return nil, dbterror.ErrNotSupportedYet.GenWithStackByArgs("[NO_WRITE_TO_BINLOG | LOCAL]")
	}
	if as.Incremental {
		return nil, errors.Errorf("the incremental analyze feature has already been removed in TiDB v7.5.0, so this will have no effect")
	}
	statsVersion := b.ctx.GetSessionVars().AnalyzeVersion
	// Require INSERT and SELECT privilege for tables.
	b.requireInsertAndSelectPriv(as.TableNames)

	opts, err := handleAnalyzeOptions(as.AnalyzeOpts, statsVersion)
	if err != nil {
		return nil, err
	}

	if as.IndexFlag {
		if len(as.IndexNames) == 0 {
			return b.buildAnalyzeAllIndex(as, opts, statsVersion)
		}
		return b.buildAnalyzeIndex(as, opts, statsVersion)
	}
	return b.buildAnalyzeTable(as, opts, statsVersion)
}
