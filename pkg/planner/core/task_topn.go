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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
)

// ContainHeavyFunction check if the expr contains a function that need to do HeavyFunctionOptimize. Currently this only applies
// to Vector data types and their functions. The HeavyFunctionOptimize eliminate the usage of the function in TopN operators
// to avoid vector distance re-calculation of TopN in the root task.
func ContainHeavyFunction(expr expression.Expression) bool {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if _, ok := HeavyFunctionNameMap[sf.FuncName.L]; ok {
		return true
	}
	return slices.ContainsFunc(sf.GetArgs(), ContainHeavyFunction)
}

// canPushToIndexPlan checks if this TopN can be pushed to the index side of copTask.
// It can be pushed to the index side when all columns used by ByItems are available from the index side and there's no prefix index column.
func canPushToIndexPlan(indexPlan base.PhysicalPlan, byItemCols []*expression.Column) bool {
	// If we call canPushToIndexPlan and there's no index plan, we should go into the index merge case.
	// Index merge case is specially handled for now. So we directly return false here.
	// So we directly return false.
	if indexPlan == nil {
		return false
	}
	schema := indexPlan.Schema()
	for _, col := range byItemCols {
		pos := schema.ColumnIndex(col)
		if pos == -1 {
			return false
		}
		if schema.Columns[pos].IsPrefix {
			return false
		}
	}
	return true
}

// canExpressionConvertedToPB checks whether each of the the expression in TopN can be converted to pb.
func canExpressionConvertedToPB(p *physicalop.PhysicalTopN, storeTp kv.StoreType) bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	return expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), exprs, storeTp)
}

// containVirtualColumn checks whether TopN.ByItems contains virtual generated columns.
func containVirtualColumn(p *physicalop.PhysicalTopN, tCols []*expression.Column) bool {
	tColSet := make(map[int64]struct{}, len(tCols))
	for _, tCol := range tCols {
		if tCol.ID > 0 && tCol.VirtualExpr != nil {
			tColSet[tCol.ID] = struct{}{}
		}
	}
	for _, by := range p.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if _, ok := tColSet[col.ID]; ok {
				// A column with ID > 0 indicates that the column can be resolved by data source.
				return true
			}
		}
	}
	return false
}

// canPushDownToTiKV checks whether this topN can be pushed down to TiKV.
func canPushDownToTiKV(p *physicalop.PhysicalTopN, copTask *physicalop.CopTask) bool {
	if !canExpressionConvertedToPB(p, kv.TiKV) {
		return false
	}
	if len(copTask.RootTaskConds) != 0 {
		return false
	}
	if !copTask.IndexPlanFinished && len(copTask.IdxMergePartPlans) > 0 {
		for _, partialPlan := range copTask.IdxMergePartPlans {
			if containVirtualColumn(p, partialPlan.Schema().Columns) {
				return false
			}
		}
	} else if containVirtualColumn(p, copTask.Plan().Schema().Columns) {
		return false
	}
	return true
}

// canPushDownToTiFlash checks whether this topN can be pushed down to TiFlash.
func canPushDownToTiFlash(p *physicalop.PhysicalTopN, mppTask *physicalop.MppTask) bool {
	if !canExpressionConvertedToPB(p, kv.TiFlash) {
		return false
	}
	if containVirtualColumn(p, mppTask.Plan().Schema().Columns) {
		return false
	}
	return true
}

// For https://github.com/pingcap/tidb/issues/51723,
// This function only supports `CLUSTER_SLOW_QUERY`,
// it will change plan from
// TopN -> TableReader -> TableFullScan[cop] to
// TopN -> TableReader -> Limit[cop] -> TableFullScan[cop] + keepOrder
func pushLimitDownToTiDBCop(p *physicalop.PhysicalTopN, copTsk *physicalop.CopTask) (base.Task, bool) {
	if copTsk.IndexPlan != nil || copTsk.TablePlan == nil {
		return nil, false
	}

	var (
		selOnTblScan   *physicalop.PhysicalSelection
		selSelectivity float64
		tblScan        *physicalop.PhysicalTableScan
		err            error
		ok             bool
	)

	copTsk.TablePlan, err = copTsk.TablePlan.Clone(p.SCtx())
	if err != nil {
		return nil, false
	}
	finalTblScanPlan := copTsk.TablePlan
	for len(finalTblScanPlan.Children()) > 0 {
		selOnTblScan, _ = finalTblScanPlan.(*physicalop.PhysicalSelection)
		finalTblScanPlan = finalTblScanPlan.Children()[0]
	}

	if tblScan, ok = finalTblScanPlan.(*physicalop.PhysicalTableScan); !ok {
		return nil, false
	}

	// Check the table is `CLUSTER_SLOW_QUERY` or not.
	if tblScan.Table.Name.O != infoschema.ClusterTableSlowLog {
		return nil, false
	}

	colsProp, ok := physicalop.GetPropByOrderByItems(p.ByItems)
	if !ok {
		return nil, false
	}
	// For cluster tables, HandleCols may be nil. Skip this optimization if HandleCols is not available.
	if tblScan.HandleCols == nil {
		return nil, false
	}
	if len(colsProp.SortItems) != 1 || !colsProp.SortItems[0].Col.Equal(p.SCtx().GetExprCtx().GetEvalCtx(), tblScan.HandleCols.GetCol(0)) {
		return nil, false
	}
	if selOnTblScan != nil && tblScan.StatsInfo().RowCount > 0 {
		selSelectivity = selOnTblScan.StatsInfo().RowCount / tblScan.StatsInfo().RowCount
	}
	tblScan.Desc = colsProp.SortItems[0].Desc
	tblScan.KeepOrder = true

	childProfile := copTsk.Plan().StatsInfo()
	newCount := p.Offset + p.Count
	stats := property.DeriveLimitStats(childProfile, float64(newCount))
	pushedLimit := physicalop.PhysicalLimit{
		Count: newCount,
	}.Init(p.SCtx(), stats, p.QueryBlockOffset())
	pushedLimit.SetSchema(copTsk.TablePlan.Schema())
	copTsk = attachPlan2Task(pushedLimit, copTsk).(*physicalop.CopTask)
	child := pushedLimit.Children()[0]
	child.SetStats(child.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), float64(newCount)))
	if selSelectivity > 0 && selSelectivity < 1 {
		scaledRowCount := child.StatsInfo().RowCount / selSelectivity
		tblScan.SetStats(tblScan.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), scaledRowCount))
	}
	rootTask := copTsk.ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p, rootTask), true
}

// Attach2Task implements the PhysicalPlan interface.
func attach2Task4PhysicalTopN(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalTopN)
	t := tasks[0].Copy()

	// Handle partial order TopN first: when CopTask carries PartialOrderMatchResult,
	// it means skylinePruning has found a prefix index that can provide partial order.
	if copTask, ok := t.(*physicalop.CopTask); ok {
		if copTask.PartialOrderMatchResult != nil && copTask.PartialOrderMatchResult.Matched {
			return handlePartialOrderTopN(p, copTask)
		}
	}

	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	needPushDown := len(cols) > 0
	if copTask, ok := t.(*physicalop.CopTask); ok && needPushDown && copTask.GetStoreType() == kv.TiDB && len(copTask.RootTaskConds) == 0 {
		newTask, changed := pushLimitDownToTiDBCop(p, copTask)
		if changed {
			return newTask
		}
	}
	if copTask, ok := t.(*physicalop.CopTask); ok && needPushDown && canPushDownToTiKV(p, copTask) && len(copTask.RootTaskConds) == 0 {
		// If all columns in topN are from index plan, we push it to index plan, otherwise we finish the index plan and
		// push it to table plan.
		var pushedDownTopN *physicalop.PhysicalTopN
		var newGlobalTopN *physicalop.PhysicalTopN
		if !copTask.IndexPlanFinished && canPushToIndexPlan(copTask.IndexPlan, cols) {
			pushedDownTopN, newGlobalTopN = getPushedDownTopN(p, copTask.IndexPlan, copTask.GetStoreType())
			copTask.IndexPlan = pushedDownTopN
			if newGlobalTopN != nil {
				rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
				// Skip TopN with partition on the root. This is a derived topN and window function
				// will take care of the filter.
				if len(p.GetPartitionBy()) > 0 {
					return t
				}
				return attachPlan2Task(newGlobalTopN, rootTask)
			}
		} else {
			// It works for both normal index scan and index merge scan.
			copTask.FinishIndexPlan()
			pushedDownTopN, newGlobalTopN = getPushedDownTopN(p, copTask.TablePlan, copTask.GetStoreType())
			copTask.TablePlan = pushedDownTopN
			if newGlobalTopN != nil {
				rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
				// Skip TopN with partition on the root. This is a derived topN and window function
				// will take care of the filter.
				if len(p.GetPartitionBy()) > 0 {
					return t
				}
				return attachPlan2Task(newGlobalTopN, rootTask)
			}
		}
	} else if mppTask, ok := t.(*physicalop.MppTask); ok && needPushDown && canPushDownToTiFlash(p, mppTask) {
		pushedDownTopN, newGlobalTopN := getPushedDownTopN(p, mppTask.Plan(), kv.TiFlash)
		mppTask.SetPlan(pushedDownTopN)
		if newGlobalTopN != nil {
			rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
			// Skip TopN with partition on the root. This is a derived topN and window function
			// will take care of the filter.
			if len(p.GetPartitionBy()) > 0 {
				return t
			}
			return attachPlan2Task(newGlobalTopN, rootTask)
		}
	}
	rootTask := t.ConvertToRootTask(p.SCtx())
	// Skip TopN with partition on the root. This is a derived topN and window function
	// will take care of the filter.
	if len(p.GetPartitionBy()) > 0 {
		return t
	}
	return attachPlan2Task(p, rootTask)
}

// handlePartialOrderTopN handles the partial order TopN scenario.
// It fills the partial-order-related fields on the TopN itself and, when possible,
// pushes down a special Limit with prefix information to the index side.
// There are two different cases:
//
// Case1: Two phase TopN, where TiDB keeps TopN and TiKV applies a partial-order Limit:
//
//	TopN(with partial info)
//	  └─IndexLookUp
//	     └─Limit(with partial info)
//	... (other operators)
//
// Case2: One phase TopN, where the whole TopN can only be executed in TiDB:
//
//	TopN(with partial info)
//	  ├─IndexPlan
//	  └─TablePlan
func handlePartialOrderTopN(p *physicalop.PhysicalTopN, copTask *physicalop.CopTask) base.Task {
	matchResult := copTask.PartialOrderMatchResult

	// Init partial order params PrefixCol and PrefixLen.
	// PartialOrderedLimit = Count + Offset.
	// TopN(with partial order) executor will short-cut read when it already handle "p.Count + p.Offset" rows.
	// Also it need to read X more rows (which prefix value is same as the last line prefix value) to ensure correctness.
	partialOrderedLimit := p.Count + p.Offset
	p.PrefixLen = matchResult.PrefixLen
	// Find the corresponding prefix column in TopN's schema.
	// matchResult.PrefixCol is from IndexScan's schema, but
	// Projection operators may remap columns. We need to find the column in TopN's
	// schema that has the same UniqueID as matchResult.PrefixCol.
	// Column UniqueID remains unchanged even after Projection remapping.
	p.PrefixCol = nil
	for _, col := range p.Schema().Columns {
		if col.UniqueID == matchResult.PrefixCol.UniqueID {
			p.PrefixCol = col
			break
		}
	}
	// Fallback: if not found in rootTask schema (should not happen)
	if p.PrefixCol == nil {
		return base.InvalidTask
	}

	// Decide whether we can push a special Limit down to the index plan.
	// Conditions:
	//   - Not an IndexMerge.
	//   - IndexPlan is not finished (IndexPlanFinished == false).
	//   - No root task conditions.
	// Since the output of the table plan is not ordered. So if limit can be pushed down, it must be pushed down to the index plan.
	// Therefore, we performed this related check here.
	canPushLimit := false
	if len(copTask.IdxMergePartPlans) == 0 &&
		!copTask.IndexPlanFinished &&
		len(copTask.RootTaskConds) == 0 &&
		copTask.IndexPlan != nil {
		canPushLimit = true
	}

	if canPushLimit {
		// Two-layer mode: TiDB TopN(with partial order info.) + TiKV limit(with partial order info.)
		// The estRows of partial order TopN : N + X
		// N: The partialOrderedLimit, N means the value of TopN, N = Count + Offset.
		// X: The estimated extra rows to read to fulfill the TopN.
		// We need to read more prefix values that are the same as the last line
		// to ensure the correctness of the final calculation of the Top n rows.
		maxX := estimateMaxXForPartialOrder()
		estimatedRows := float64(partialOrderedLimit) + float64(maxX)
		childProfile := copTask.IndexPlan.StatsInfo()
		limitStats := property.DeriveLimitStats(childProfile, estimatedRows)

		pushedDownLimit := physicalop.PhysicalLimit{
			Count:     partialOrderedLimit,
			PrefixCol: p.PrefixCol,
			PrefixLen: matchResult.PrefixLen,
		}.Init(p.SCtx(), limitStats, p.QueryBlockOffset())
		pushedDownLimit.SetChildren(copTask.IndexPlan)
		pushedDownLimit.SetSchema(copTask.IndexPlan.Schema())
		copTask.IndexPlan = pushedDownLimit
	}

	// Always keep TopN in TiDB as the upper layer.
	rootTask := copTask.ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p, rootTask)
}

// estimateMaxXForPartialOrder estimates the extra rows X to read for partial order optimization.
// This value is used for statistics (row count estimation).
func estimateMaxXForPartialOrder() uint64 {
	// TODO: implement it by TopN/buckets and adjust it by session variable.
	return 0
}
