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
	"maps"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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
