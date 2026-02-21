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

package core

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	sliceutil "github.com/pingcap/tidb/pkg/util/slice"
	"go.uber.org/zap"
)

// convertToIndexMergeScan builds the index merge scan for intersection or union cases.
func convertToIndexMergeScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) (task base.Task, err error) {
	if prop.IsFlashProp() || prop.TaskTp == property.CopSingleReadTaskType {
		return base.InvalidTask, nil
	}
	// lift the limitation of that double read can not build index merge **COP** task with intersection.
	// that means we can output a cop task here without encapsulating it as root task, for the convenience of attaching limit to its table side.

	intest.Assert(candidate.matchPropResult != property.PropMatchedNeedMergeSort,
		"index merge path should not match property using merge sort")

	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
		return base.InvalidTask, nil
	}
	// while for now, we still can not push the sort prop to the intersection index plan side, temporarily banned here.
	if !prop.IsSortItemEmpty() && candidate.path.IndexMergeIsIntersection {
		return base.InvalidTask, nil
	}
	failpoint.Inject("forceIndexMergeKeepOrder", func(_ failpoint.Value) {
		if len(candidate.path.PartialIndexPaths) > 0 && !candidate.path.IndexMergeIsIntersection {
			if prop.IsSortItemEmpty() {
				failpoint.Return(base.InvalidTask, nil)
			}
		}
	})
	path := candidate.path
	scans := make([]base.PhysicalPlan, 0, len(path.PartialIndexPaths))
	cop := &physicalop.CopTask{
		IndexPlanFinished: false,
		TblColHists:       ds.TblColHists,
	}
	cop.PhysPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}
	// Add sort items for index scan for merge-sort operation between partitions.
	byItems := make([]*util.ByItems, 0, len(prop.SortItems))
	for _, si := range prop.SortItems {
		byItems = append(byItems, &util.ByItems{
			Expr: si.Col,
			Desc: si.Desc,
		})
	}
	globalRemainingFilters := make([]expression.Expression, 0, 3)
	for _, partPath := range path.PartialIndexPaths {
		var scan base.PhysicalPlan
		if partPath.IsTablePath() {
			scan = convertToPartialTableScan(ds, prop, partPath, candidate.matchPropResult, byItems)
		} else {
			var remainingFilters []expression.Expression
			scan, remainingFilters, err = physicalop.ConvertToPartialIndexScan(ds, cop.PhysPlanPartInfo, prop, partPath, candidate.matchPropResult, byItems)
			if err != nil {
				return base.InvalidTask, err
			}
			if prop.TaskTp != property.RootTaskType && len(remainingFilters) > 0 {
				return base.InvalidTask, nil
			}
			globalRemainingFilters = append(globalRemainingFilters, remainingFilters...)
		}
		scans = append(scans, scan)
	}
	totalRowCount := path.CountAfterAccess
	// Add an arbitrary tolerance factor to account for comparison with floating point
	if (prop.ExpectedCnt + cost.ToleranceFactor) < ds.StatsInfo().RowCount {
		totalRowCount *= prop.ExpectedCnt / ds.StatsInfo().RowCount
	}
	ts, remainingFilters2, moreColumn, err := physicalop.BuildIndexMergeTableScan(ds, path.TableFilters, totalRowCount, candidate.matchPropResult == property.PropMatched)
	if err != nil {
		return base.InvalidTask, err
	}
	if prop.TaskTp != property.RootTaskType && len(remainingFilters2) > 0 {
		return base.InvalidTask, nil
	}
	globalRemainingFilters = append(globalRemainingFilters, remainingFilters2...)
	cop.KeepOrder = candidate.matchPropResult == property.PropMatched
	cop.TablePlan = ts
	cop.IdxMergePartPlans = scans
	cop.IdxMergeIsIntersection = path.IndexMergeIsIntersection
	cop.IdxMergeAccessMVIndex = path.IndexMergeAccessMVIndex
	if moreColumn {
		cop.NeedExtraProj = true
		cop.OriginSchema = ds.Schema()
	}
	if len(globalRemainingFilters) != 0 {
		cop.RootTaskConds = globalRemainingFilters
	}
	// after we lift the limitation of intersection and cop-type task in the code in this
	// function above, we could set its index plan finished as true once we found its table
	// plan is pure table scan below.
	// And this will cause cost underestimation when we estimate the cost of the entire cop
	// task plan in function `getTaskPlanCost`.
	if prop.TaskTp == property.RootTaskType {
		cop.IndexPlanFinished = true
		task = cop.ConvertToRootTask(ds.SCtx())
	} else {
		_, pureTableScan := ts.(*physicalop.PhysicalTableScan)
		if !pureTableScan {
			cop.IndexPlanFinished = true
		}
		task = cop
	}
	return task, nil
}

func checkColinSchema(cols []*expression.Column, schema *expression.Schema) bool {
	for _, col := range cols {
		if schema.ColumnIndex(col) == -1 {
			return false
		}
	}
	return true
}

func convertToPartialTableScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, path *util.AccessPath, matchProp property.PhysicalPropMatchResult, byItems []*util.ByItems) (tablePlan base.PhysicalPlan) {
	intest.Assert(matchProp != property.PropMatchedNeedMergeSort,
		"partial paths of index merge path should not match property using merge sort")
	ts, rowCount := physicalop.GetOriginalPhysicalTableScan(ds, prop, path, matchProp.Matched())
	overwritePartialTableScanSchema(ds, ts)
	// remove ineffetive filter condition after overwriting physicalscan schema
	newFilterConds := make([]expression.Expression, 0, len(path.TableFilters))
	for _, cond := range ts.FilterCondition {
		cols := expression.ExtractColumns(cond)
		if checkColinSchema(cols, ts.Schema()) {
			newFilterConds = append(newFilterConds, cond)
		}
	}
	ts.FilterCondition = newFilterConds
	if matchProp.Matched() {
		if ts.Table.GetPartitionInfo() != nil && ts.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
			tmpColumns, tmpSchema, _ := physicalop.AddExtraPhysTblIDColumn(ts.SCtx(), ts.Columns, ts.Schema())
			ts.Columns = tmpColumns
			ts.SetSchema(tmpSchema)
		}
		ts.ByItems = byItems
	}
	if len(ts.FilterCondition) > 0 {
		selectivity, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, ts.FilterCondition, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		tablePlan = physicalop.PhysicalSelection{Conditions: ts.FilterCondition}.Init(ts.SCtx(), ts.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), selectivity*rowCount), ds.QueryBlockOffset())
		tablePlan.SetChildren(ts)
		return tablePlan
	}
	tablePlan = ts
	return tablePlan
}

// overwritePartialTableScanSchema change the schema of partial table scan to handle columns.
func overwritePartialTableScanSchema(ds *logicalop.DataSource, ts *physicalop.PhysicalTableScan) {
	handleCols := ds.HandleCols
	if handleCols == nil {
		if ds.Table.Type().IsClusterTable() {
			// For cluster tables without handles, use the first column from the schema.
			// Cluster tables don't support ExtraHandleID (-1) as they are memory tables.
			if len(ds.Columns) > 0 && len(ds.Schema().Columns) > 0 {
				ts.SetSchema(expression.NewSchema(ds.Schema().Columns[0]))
				ts.Columns = []*model.ColumnInfo{ds.Columns[0]}
			}
			return
		}
		handleCols = util.NewIntHandleCols(ds.NewExtraHandleSchemaCol())
	}
	hdColNum := handleCols.NumCols()
	exprCols := make([]*expression.Column, 0, hdColNum)
	infoCols := make([]*model.ColumnInfo, 0, hdColNum)
	for i := range hdColNum {
		col := handleCols.GetCol(i)
		exprCols = append(exprCols, col)
		if c := model.FindColumnInfoByID(ds.TableInfo.Columns, col.ID); c != nil {
			infoCols = append(infoCols, c)
		} else {
			infoCols = append(infoCols, col.ToInfo())
		}
	}
	ts.SetSchema(expression.NewSchema(exprCols...))
	ts.Columns = infoCols
}

// convertToIndexScan converts the DataSource to index scan with idx.
func convertToIndexScan(ds *logicalop.DataSource, prop *property.PhysicalProperty,
	candidate *candidatePath) (task base.Task, err error) {
	if candidate.path.Index.MVIndex {
		// MVIndex is special since different index rows may return the same _row_id and this can break some assumptions of IndexReader.
		// Currently only support using IndexMerge to access MVIndex instead of IndexReader.
		// TODO: make IndexReader support accessing MVIndex directly.
		return base.InvalidTask, nil
	}
	if !candidate.path.IsSingleScan {
		// If it's parent requires single read task, return max cost.
		if prop.TaskTp == property.CopSingleReadTaskType {
			return base.InvalidTask, nil
		}
	} else if prop.TaskTp == property.CopMultiReadTaskType {
		// If it's parent requires double read task, return max cost.
		return base.InvalidTask, nil
	}
	// Check if sort items can be matched. If not, return Invalid task
	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
		return base.InvalidTask, nil
	}
	// If we need to keep order for the index scan, we should forbid the non-keep-order index scan when we try to generate the path.
	if !prop.NeedKeepOrder() && candidate.path.ForceKeepOrder {
		return base.InvalidTask, nil
	}
	// If we don't need to keep order for the index scan, we should forbid the non-keep-order index scan when we try to generate the path.
	if prop.NeedKeepOrder() && candidate.path.ForceNoKeepOrder {
		return base.InvalidTask, nil
	}
	// If we want to force partial order, then we should remove all others property candidate path such as: full order and no order.
	if candidate.path.ForcePartialOrder && prop.PartialOrderInfo == nil {
		return base.InvalidTask, nil
	}
	// For partial order property
	// We **don't need to check** the partial order property is matched in here.
	// Because if the index scan cannot satisfy partial order, it will be pruned at the SkylinePruning phase
	// (which is the previous phase before this function).
	// So, if an index can enter this function and also contains the requirement of a partial order property,
	// then it must meet the requirements.

	path := candidate.path
	is := physicalop.GetOriginalPhysicalIndexScan(ds, prop, path, candidate.matchPropResult.Matched(), candidate.path.IsSingleScan)
	cop := &physicalop.CopTask{
		IndexPlan:   is,
		TblColHists: ds.TblColHists,
		TblCols:     ds.TblCols,
		ExpectCnt:   uint64(prop.ExpectedCnt),
	}
	// Store partial order match result in CopTask for use in attach2Task
	if candidate.partialOrderMatchResult.Matched {
		cop.PartialOrderMatchResult = &candidate.partialOrderMatchResult
	}
	cop.PhysPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}

	if !candidate.path.IsSingleScan {
		// On this way, it's double read case.
		ts := physicalop.PhysicalTableScan{
			Columns:         sliceutil.DeepClone(ds.Columns),
			Table:           is.Table,
			TableAsName:     ds.TableAsName,
			DBName:          ds.DBName,
			PhysicalTableID: ds.PhysicalTableID,
			TblCols:         ds.TblCols,
			TblColHists:     ds.TblColHists,
		}.Init(ds.SCtx(), is.QueryBlockOffset())
		ts.SetIsPartition(ds.PartitionDefIdx != nil)
		ts.SetSchema(ds.Schema().Clone())
		// We set `StatsVersion` here and fill other fields in `(*copTask).finishIndexPlan`. Since `copTask.indexPlan` may
		// change before calling `(*copTask).finishIndexPlan`, we don't know the stats information of `ts` currently and on
		// the other hand, it may be hard to identify `StatsVersion` of `ts` in `(*copTask).finishIndexPlan`.
		ts.SetStats(&property.StatsInfo{StatsVersion: ds.TableStats.StatsVersion})
		usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
		if usedStats != nil && usedStats.GetUsedInfo(ts.PhysicalTableID) != nil {
			ts.UsedStatsInfo = usedStats.GetUsedInfo(ts.PhysicalTableID)
		}
		cop.TablePlan = ts
		cop.IndexLookUpPushDownBy = candidate.path.IndexLookUpPushDownBy
	}
	task = cop
	if cop.TablePlan != nil && ds.TableInfo.IsCommonHandle {
		cop.CommonHandleCols = ds.CommonHandleCols
		commonHandle := ds.HandleCols.(*util.CommonHandleCols)
		for _, col := range commonHandle.GetColumns() {
			if ds.Schema().ColumnIndex(col) == -1 {
				ts := cop.TablePlan.(*physicalop.PhysicalTableScan)
				ts.Schema().Append(col)
				ts.Columns = append(ts.Columns, col.ToInfo())
				cop.NeedExtraProj = true
			}
		}
	}
	// handles both normal sort (SortItems) and partial order (PartialOrderInfo)
	if prop.NeedKeepOrder() {
		cop.KeepOrder = true
		if cop.TablePlan != nil && !ds.TableInfo.IsCommonHandle {
			col, isNew := cop.TablePlan.(*physicalop.PhysicalTableScan).AppendExtraHandleCol(ds)
			cop.ExtraHandleCol = col
			cop.NeedExtraProj = cop.NeedExtraProj || isNew
		}

		// Add sort items for index scan for the merge-sort operation
		// Case 1: keep order between partitions (only required for local index)
		// Case 2: keep order between range groups (see comments of PropMatchedNeedMergeSort and
		// AccessPath.GroupedRanges for details)
		// Case 3: both
		if (ds.TableInfo.GetPartitionInfo() != nil && !is.Index.Global) ||
			candidate.matchPropResult == property.PropMatchedNeedMergeSort {
			sortItems := prop.GetSortItemsForKeepOrder()
			byItems := make([]*util.ByItems, 0, len(sortItems))
			for _, si := range sortItems {
				byItems = append(byItems, &util.ByItems{
					Expr: si.Col,
					Desc: si.Desc,
				})
			}
			cop.IndexPlan.(*physicalop.PhysicalIndexScan).ByItems = byItems
		}
		if candidate.matchPropResult == property.PropMatchedNeedMergeSort {
			is.GroupedRanges = candidate.path.GroupedRanges
			is.GroupByColIdxs = candidate.path.GroupByColIdxs
		}
		if ds.TableInfo.GetPartitionInfo() != nil {
			if cop.TablePlan != nil && ds.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
				if !is.Index.Global {
					tmpColumns, tmpSchema, _ := physicalop.AddExtraPhysTblIDColumn(is.SCtx(), is.Columns, is.Schema())
					is.Columns = tmpColumns
					is.SetSchema(tmpSchema)
				}
				// global index for tableScan with keepOrder also need PhysicalTblID
				ts := cop.TablePlan.(*physicalop.PhysicalTableScan)
				tmpColumns, tmpSchema, succ := physicalop.AddExtraPhysTblIDColumn(ts.SCtx(), ts.Columns, ts.Schema())
				ts.Columns = tmpColumns
				ts.SetSchema(tmpSchema)
				cop.NeedExtraProj = cop.NeedExtraProj || succ
			}
		}
	}
	if cop.NeedExtraProj {
		cop.OriginSchema = ds.Schema()
	}
	// prop.IsSortItemEmpty() would always return true when coming to here,
	// so we can just use prop.ExpectedCnt as parameter of AddPushedDownSelection.
	finalStats := ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt)
	if err = addPushedDownSelection4PhysicalIndexScan(is, cop, ds, path, finalStats); err != nil {
		return base.InvalidTask, err
	}
	if prop.TaskTp == property.RootTaskType {
		task = task.ConvertToRootTask(ds.SCtx())
	} else if _, ok := task.(*physicalop.RootTask); ok {
		return base.InvalidTask, nil
	}
	return task, nil
}

// addPushedDownSelection is to add pushdown selection
func addPushedDownSelection4PhysicalIndexScan(is *physicalop.PhysicalIndexScan, copTask *physicalop.CopTask, p *logicalop.DataSource, path *util.AccessPath, finalStats *property.StatsInfo) error {
	// Add filter condition to table plan now.
	indexConds, tableConds := path.IndexFilters, path.TableFilters
	tableConds, copTask.RootTaskConds = physicalop.SplitSelCondsWithVirtualColumn(tableConds)

	var newRootConds []expression.Expression
	pctx := util.GetPushDownCtx(is.SCtx())
	indexConds, newRootConds = expression.PushDownExprs(pctx, indexConds, kv.TiKV)
	copTask.RootTaskConds = append(copTask.RootTaskConds, newRootConds...)

	tableConds, newRootConds = expression.PushDownExprs(pctx, tableConds, kv.TiKV)
	copTask.RootTaskConds = append(copTask.RootTaskConds, newRootConds...)

	// Add a `Selection` for `IndexScan` with global index.
	// It should pushdown to TiKV, DataSource schema doesn't contain partition id column.
	indexConds, err := is.AddSelectionConditionForGlobalIndex(p, copTask.PhysPlanPartInfo, indexConds)
	if err != nil {
		return err
	}

	if len(indexConds) != 0 {
		var selectivity float64
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		count := is.StatsInfo().RowCount * selectivity
		stats := p.TableStats.ScaleByExpectCnt(p.SCtx().GetSessionVars(), count)
		indexSel := physicalop.PhysicalSelection{Conditions: indexConds}.Init(is.SCtx(), stats, is.QueryBlockOffset())
		indexSel.SetChildren(is)
		copTask.IndexPlan = indexSel
	}
	if len(tableConds) > 0 {
		copTask.FinishIndexPlan()
		tableSel := physicalop.PhysicalSelection{Conditions: tableConds}.Init(is.SCtx(), finalStats, is.QueryBlockOffset())
		if len(copTask.RootTaskConds) != 0 {
			selectivity, err := cardinality.Selectivity(is.SCtx(), copTask.TblColHists, tableConds, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = cost.SelectionFactor
			}
			tableSel.SetStats(copTask.Plan().StatsInfo().Scale(is.SCtx().GetSessionVars(), selectivity))
		}
		tableSel.SetChildren(copTask.TablePlan)
		copTask.TablePlan = tableSel
	}
	return nil
}

func splitIndexFilterConditions(ds *logicalop.DataSource, conditions []expression.Expression, indexColumns []*expression.Column,
	idxColLens []int) (indexConds, tableConds []expression.Expression) {
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		var covered bool
		if ds.SCtx().GetSessionVars().OptPrefixIndexSingleScan {
			covered = ds.IsIndexCoveringCondition(cond, indexColumns, idxColLens)
		} else {
			covered = ds.IsIndexCoveringColumns(expression.ExtractColumns(cond), indexColumns, idxColLens)
		}
		if covered {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// isPointGetPath indicates whether the conditions are point-get-able.
// eg: create table t(a int, b int,c int unique, primary (a,b))
// select * from t where a = 1 and b = 1 and c =1;
// the datasource can access by primary key(a,b) or unique key c which are both point-get-able
func isPointGetPath(ds *logicalop.DataSource, path *util.AccessPath) bool {
	if len(path.Ranges) < 1 {
		return false
	}
	if !path.IsIntHandlePath {
		if path.Index == nil {
			return false
		}
		if !path.Index.Unique || path.Index.HasPrefixIndex() {
			return false
		}
		idxColsLen := len(path.Index.Columns)
		for _, ran := range path.Ranges {
			if len(ran.LowVal) != idxColsLen {
				return false
			}
		}
	}
	tc := ds.SCtx().GetSessionVars().StmtCtx.TypeCtx()
	for _, ran := range path.Ranges {
		if !ran.IsPointNonNullable(tc) {
			return false
		}
	}
	return true
}

// convertToTableScan converts the DataSource to table scan.
