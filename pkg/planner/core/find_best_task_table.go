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
	"math"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func convertToTableScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) (base.Task, error) {
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CopMultiReadTaskType {
		return base.InvalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
		return base.InvalidTask, nil
	}
	// If we need to keep order for the index scan, we should forbid the non-keep-order index scan when we try to generate the path.
	if prop.IsSortItemEmpty() && candidate.path.ForceKeepOrder {
		return base.InvalidTask, nil
	}
	// If we don't need to keep order for the index scan, we should forbid the non-keep-order index scan when we try to generate the path.
	if !prop.IsSortItemEmpty() && candidate.path.ForceNoKeepOrder {
		return base.InvalidTask, nil
	}
	ts, _ := physicalop.GetOriginalPhysicalTableScan(ds, prop, candidate.path, candidate.matchPropResult.Matched())
	// In disaggregated tiflash mode, only MPP is allowed, cop and batchCop is deprecated.
	// So if prop.TaskTp is RootTaskType, have to use mppTask then convert to rootTask.
	isTiFlashPath := ts.StoreType == kv.TiFlash
	canMppConvertToRoot := prop.TaskTp == property.RootTaskType && ds.SCtx().GetSessionVars().IsMPPAllowed() && isTiFlashPath
	canMppConvertToRootForDisaggregatedTiFlash := config.GetGlobalConfig().DisaggregatedTiFlash && canMppConvertToRoot
	canMppConvertToRootForWhenTiFlashCopIsBanned := ds.SCtx().GetSessionVars().IsTiFlashCopBanned() && canMppConvertToRoot

	// Fast checks
	if isTiFlashPath && ts.KeepOrder && (ts.Desc || ds.SCtx().GetSessionVars().TiFlashFastScan) {
		// TiFlash fast mode(https://github.com/pingcap/tidb/pull/35851) does not support keep order in TableScan
		return base.InvalidTask, nil
	}
	if prop.TaskTp == property.MppTaskType || canMppConvertToRootForDisaggregatedTiFlash || canMppConvertToRootForWhenTiFlashCopIsBanned {
		if ts.KeepOrder {
			return base.InvalidTask, nil
		}
		if prop.MPPPartitionTp != property.AnyType {
			return base.InvalidTask, nil
		}
		// If it has vector property, we need to check the candidate.matchProperty.
		if candidate.path.Index != nil && candidate.path.Index.VectorInfo != nil && !candidate.matchPropResult.Matched() {
			return base.InvalidTask, nil
		}
	} else {
		if isTiFlashPath && config.GetGlobalConfig().DisaggregatedTiFlash || isTiFlashPath && ds.SCtx().GetSessionVars().IsTiFlashCopBanned() {
			// prop.TaskTp is cop related, just return base.InvalidTask.
			return base.InvalidTask, nil
		}
		if isTiFlashPath && candidate.matchPropResult.Matched() && ds.TableInfo.GetPartitionInfo() != nil {
			// TableScan on partition table on TiFlash can't keep order.
			return base.InvalidTask, nil
		}
	}

	// MPP task
	if prop.TaskTp == property.MppTaskType || canMppConvertToRootForDisaggregatedTiFlash || canMppConvertToRootForWhenTiFlashCopIsBanned {
		if candidate.path.Index != nil && candidate.path.Index.VectorInfo != nil {
			// Only the corresponding index can generate a valid task.
			intest.Assert(ts.Table.Columns[candidate.path.Index.Columns[0].Offset].ID == prop.VectorProp.Column.ID, "The passed vector column is not matched with the index")
			distanceMetric := model.IndexableFnNameToDistanceMetric[prop.VectorProp.DistanceFnName.L]
			distanceMetricPB := tipb.VectorDistanceMetric_value[string(distanceMetric)]
			intest.Assert(distanceMetricPB != 0, "unexpected distance metric")

			ts.UsedColumnarIndexes = append(ts.UsedColumnarIndexes, buildVectorIndexExtra(
				candidate.path.Index,
				tipb.ANNQueryType_OrderBy,
				tipb.VectorDistanceMetric(distanceMetricPB),
				prop.VectorProp.TopK,
				ts.Table.Columns[candidate.path.Index.Columns[0].Offset].Name.L,
				prop.VectorProp.Vec.SerializeTo(nil),
				tidbutil.ColumnToProto(prop.VectorProp.Column.ToInfo(), false, false),
			))
			ts.SetStats(property.DeriveLimitStats(ts.StatsInfo(), float64(prop.VectorProp.TopK)))
		}
		// ********************************** future deprecated start **************************/
		var hasVirtualColumn bool
		for _, col := range ts.Schema().Columns {
			if col.VirtualExpr != nil {
				ds.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because column `" + col.OrigName + "` is a virtual column which is not supported now.")
				hasVirtualColumn = true
				break
			}
		}
		// in general, since MPP has supported the Gather operator to fill the virtual column, we should full lift restrictions here.
		// we left them here, because cases like:
		// parent-----+
		//            V  (when parent require a root task type here, we need convert mpp task to root task)
		//    projection [mpp task] [a]
		//      table-scan [mpp task] [a(virtual col as: b+1), b]
		// in the process of converting mpp task to root task, the encapsulated table reader will use its first children schema [a]
		// as its schema, so when we resolve indices later, the virtual column 'a' itself couldn't resolve itself anymore.
		//
		if hasVirtualColumn && !canMppConvertToRootForDisaggregatedTiFlash && !canMppConvertToRootForWhenTiFlashCopIsBanned {
			return base.InvalidTask, nil
		}
		// ********************************** future deprecated end **************************/
		mppTask := physicalop.NewMppTask(ts, property.AnyType, nil, ds.TblColHists, nil)
		ts.PlanPartInfo = &physicalop.PhysPlanPartInfo{
			PruningConds:   ds.AllConds,
			PartitionNames: ds.PartitionNames,
			Columns:        ds.TblCols,
			ColumnNames:    ds.OutputNames(),
		}
		mppTask = addPushedDownSelectionToMppTask4PhysicalTableScan(ts, mppTask, ds.StatsInfo().ScaleByExpectCnt(ts.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.AstIndexHints)
		var task base.Task = mppTask
		if !mppTask.Invalid() {
			if prop.TaskTp == property.MppTaskType && len(mppTask.RootTaskConds) > 0 {
				// If got filters cannot be pushed down to tiflash, we have to make sure it will be executed in TiDB,
				// So have to return a rootTask, but prop requires mppTask, cannot meet this requirement.
				task = base.InvalidTask
			} else if prop.TaskTp == property.RootTaskType {
				// When got here, canMppConvertToRootX is true.
				// This is for situations like cannot generate mppTask for some operators.
				// Such as when the build side of HashJoin is Projection,
				// which cannot pushdown to tiflash(because TiFlash doesn't support some expr in Proj)
				// So HashJoin cannot pushdown to tiflash. But we still want TableScan to run on tiflash.
				task = mppTask
				task = task.ConvertToRootTask(ds.SCtx())
			}
		}
		return task, nil
	}
	// Cop task
	copTask := &physicalop.CopTask{
		TablePlan:         ts,
		IndexPlanFinished: true,
		TblColHists:       ds.TblColHists,
	}
	copTask.PhysPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}
	ts.PlanPartInfo = copTask.PhysPlanPartInfo
	var task base.Task = copTask
	if candidate.matchPropResult.Matched() {
		copTask.KeepOrder = true
		if ds.TableInfo.GetPartitionInfo() != nil || candidate.matchPropResult == property.PropMatchedNeedMergeSort {
			// Add sort items for table scan for merge-sort operation between partitions.
			byItems := make([]*util.ByItems, 0, len(prop.SortItems))
			for _, si := range prop.SortItems {
				byItems = append(byItems, &util.ByItems{
					Expr: si.Col,
					Desc: si.Desc,
				})
			}
			ts.ByItems = byItems
		}
		if candidate.matchPropResult == property.PropMatchedNeedMergeSort {
			ts.GroupedRanges = candidate.path.GroupedRanges
			ts.GroupByColIdxs = candidate.path.GroupByColIdxs
		}
	}
	addPushedDownSelection4PhysicalTableScan(ts, copTask, ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.AstIndexHints)
	if prop.IsFlashProp() && len(copTask.RootTaskConds) != 0 {
		return base.InvalidTask, nil
	}
	if prop.TaskTp == property.RootTaskType {
		task = task.ConvertToRootTask(ds.SCtx())
	} else if _, ok := task.(*physicalop.RootTask); ok {
		return base.InvalidTask, nil
	}
	return task, nil
}

func convertToSampleTable(ds *logicalop.DataSource, prop *property.PhysicalProperty,
	candidate *candidatePath) (base.Task, error) {
	if prop.TaskTp == property.CopMultiReadTaskType {
		return base.InvalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
		return base.InvalidTask, nil
	}
	if candidate.matchPropResult.Matched() {
		// Disable keep order property for sample table path.
		return base.InvalidTask, nil
	}
	p := physicalop.PhysicalTableSample{
		TableSampleInfo: ds.SampleInfo,
		TableInfo:       ds.Table,
		PhysicalTableID: ds.PhysicalTableID,
		Desc:            candidate.matchPropResult.Matched() && prop.SortItems[0].Desc,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	p.SetSchema(ds.Schema())
	rt := &physicalop.RootTask{}
	rt.SetPlan(p)
	return rt, nil
}

func convertToPointGet(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) base.Task {
	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
		return base.InvalidTask
	}
	if prop.TaskTp == property.CopMultiReadTaskType && candidate.path.IsSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.path.IsSingleScan {
		return base.InvalidTask
	}

	if metadef.IsMemDB(ds.DBName.L) {
		return base.InvalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(1))
	pointGetPlan := &physicalop.PointGetPlan{
		AccessConditions: candidate.path.AccessConds,
		DBName:           ds.DBName.L,
		TblInfo:          ds.TableInfo,
		LockWaitTime:     ds.SCtx().GetSessionVars().LockWaitTimeout,
		Columns:          ds.Columns,
	}
	pointGetPlan.SetSchema(ds.Schema().Clone())
	pointGetPlan.SetOutputNames(ds.OutputNames())
	pointGetPlan = pointGetPlan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), accessCnt), ds.QueryBlockOffset())
	if ds.PartitionDefIdx != nil {
		pointGetPlan.PartitionIdx = ds.PartitionDefIdx
	}
	pointGetPlan.PartitionNames = ds.PartitionNames
	rTsk := &physicalop.RootTask{}
	rTsk.SetPlan(pointGetPlan)
	if candidate.path.IsIntHandlePath {
		pointGetPlan.Handle = kv.IntHandle(candidate.path.Ranges[0].LowVal[0].GetInt64())
		pointGetPlan.UnsignedHandle = mysql.HasUnsignedFlag(ds.HandleCols.GetCol(0).RetType.GetFlag())
		pointGetPlan.SetAccessCols(ds.TblCols)
		found := false
		for i := range ds.Columns {
			if ds.Columns[i].ID == ds.HandleCols.GetCol(0).ID {
				pointGetPlan.HandleColOffset = ds.Columns[i].Offset
				found = true
				break
			}
		}
		if !found {
			return base.InvalidTask
		}
		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			sel := physicalop.PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(pointGetPlan)
			rTsk.SetPlan(sel)
		}
	} else {
		pointGetPlan.IndexInfo = candidate.path.Index
		pointGetPlan.SetNoncacheableReason(candidate.path.NoncacheableReason)
		pointGetPlan.IdxCols = candidate.path.IdxCols
		pointGetPlan.IdxColLens = candidate.path.IdxColLens
		pointGetPlan.IndexValues = candidate.path.Ranges[0].LowVal
		if candidate.path.IsSingleScan {
			pointGetPlan.SetAccessCols(candidate.path.IdxCols)
		} else {
			pointGetPlan.SetAccessCols(ds.TblCols)
		}
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			sel := physicalop.PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(pointGetPlan)
			rTsk.SetPlan(sel)
		}
	}

	return rTsk
}

func convertToBatchPointGet(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) base.Task {
	// For batch point get, we don't try to satisfy the sort property with an extra merge sort,
	// so only PropMatched is allowed.
	if !prop.IsSortItemEmpty() && candidate.matchPropResult != property.PropMatched {
		return base.InvalidTask
	}
	if prop.TaskTp == property.CopMultiReadTaskType && candidate.path.IsSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.path.IsSingleScan {
		return base.InvalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(len(candidate.path.Ranges)))
	batchPointGetPlan := &physicalop.BatchPointGetPlan{
		DBName:           ds.DBName.L,
		AccessConditions: candidate.path.AccessConds,
		TblInfo:          ds.TableInfo,
		KeepOrder:        !prop.IsSortItemEmpty(),
		Columns:          ds.Columns,
		PartitionNames:   ds.PartitionNames,
	}
	batchPointGetPlan.SetCtx(ds.SCtx())
	if ds.PartitionDefIdx != nil {
		batchPointGetPlan.SinglePartition = true
		batchPointGetPlan.PartitionIdxs = []int{*ds.PartitionDefIdx}
	}
	if batchPointGetPlan.KeepOrder {
		batchPointGetPlan.Desc = prop.SortItems[0].Desc
	}
	rTsk := &physicalop.RootTask{}
	if candidate.path.IsIntHandlePath {
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.Handles = append(batchPointGetPlan.Handles, kv.IntHandle(ran.LowVal[0].GetInt64()))
		}
		batchPointGetPlan.SetAccessCols(ds.TblCols)
		found := false
		for i := range ds.Columns {
			if ds.Columns[i].ID == ds.HandleCols.GetCol(0).ID {
				batchPointGetPlan.HandleColOffset = ds.Columns[i].Offset
				found = true
				break
			}
		}
		if !found {
			return base.InvalidTask
		}

		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			batchPointGetPlan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), accessCnt), ds.Schema().Clone(), ds.OutputNames(), ds.QueryBlockOffset())
			sel := physicalop.PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(batchPointGetPlan)
			rTsk.SetPlan(sel)
		}
	} else {
		batchPointGetPlan.IndexInfo = candidate.path.Index
		batchPointGetPlan.SetNoncacheableReason(candidate.path.NoncacheableReason)
		batchPointGetPlan.IdxCols = candidate.path.IdxCols
		batchPointGetPlan.IdxColLens = candidate.path.IdxColLens
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.IndexValues = append(batchPointGetPlan.IndexValues, ran.LowVal)
		}
		if !prop.IsSortItemEmpty() {
			batchPointGetPlan.KeepOrder = true
			batchPointGetPlan.Desc = prop.SortItems[0].Desc
		}
		if candidate.path.IsSingleScan {
			batchPointGetPlan.SetAccessCols(candidate.path.IdxCols)
		} else {
			batchPointGetPlan.SetAccessCols(ds.TblCols)
		}
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			batchPointGetPlan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), accessCnt), ds.Schema().Clone(), ds.OutputNames(), ds.QueryBlockOffset())
			sel := physicalop.PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(batchPointGetPlan)
			rTsk.SetPlan(sel)
		}
	}
	if rTsk.GetPlan() == nil {
		tmpP := batchPointGetPlan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), accessCnt), ds.Schema().Clone(), ds.OutputNames(), ds.QueryBlockOffset())
		rTsk.SetPlan(tmpP)
	}

	return rTsk
}

func addPushedDownSelectionToMppTask4PhysicalTableScan(ts *physicalop.PhysicalTableScan, mpp *physicalop.MppTask, stats *property.StatsInfo, indexHints []*ast.IndexHint) *physicalop.MppTask {
	filterCondition, rootTaskConds := physicalop.SplitSelCondsWithVirtualColumn(ts.FilterCondition)
	var newRootConds []expression.Expression
	filterCondition, newRootConds = expression.PushDownExprs(util.GetPushDownCtx(ts.SCtx()), filterCondition, ts.StoreType)
	mpp.RootTaskConds = append(rootTaskConds, newRootConds...)

	ts.FilterCondition = filterCondition
	// Add filter condition to table plan now.
	if len(ts.FilterCondition) > 0 {
		if sel := ts.BuildPushedDownSelection(stats, indexHints); sel != nil {
			sel.SetChildren(ts)
			mpp.SetPlan(sel)
		} else {
			mpp.SetPlan(ts)
		}
	}
	return mpp
}

func addPushedDownSelection4PhysicalTableScan(ts *physicalop.PhysicalTableScan, copTask *physicalop.CopTask, stats *property.StatsInfo, indexHints []*ast.IndexHint) {
	ts.FilterCondition, copTask.RootTaskConds = physicalop.SplitSelCondsWithVirtualColumn(ts.FilterCondition)
	var newRootConds []expression.Expression
	ts.FilterCondition, newRootConds = expression.PushDownExprs(util.GetPushDownCtx(ts.SCtx()), ts.FilterCondition, ts.StoreType)
	copTask.RootTaskConds = append(copTask.RootTaskConds, newRootConds...)

	// Add filter condition to table plan now.
	if len(ts.FilterCondition) > 0 {
		sel := ts.BuildPushedDownSelection(stats, indexHints)
		if sel == nil {
			copTask.TablePlan = ts
			return
		}
		if len(copTask.RootTaskConds) != 0 {
			selectivity, err := cardinality.Selectivity(ts.SCtx(), copTask.TblColHists, sel.Conditions, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = cost.SelectionFactor
			}
			sel.SetStats(ts.StatsInfo().Scale(ts.SCtx().GetSessionVars(), selectivity))
		}
		sel.SetChildren(ts)
		copTask.TablePlan = sel
	}
}

func validateTableSamplePlan(ds *logicalop.DataSource, t base.Task, err error) error {
	if err != nil {
		return err
	}
	if ds.SampleInfo != nil && !t.Invalid() {
		if _, ok := t.Plan().(*physicalop.PhysicalTableSample); !ok {
			return expression.ErrInvalidTableSample.GenWithStackByArgs("plan not supported")
		}
	}
	return nil
}

// mockLogicalPlan4Test is a LogicalPlan which is used for unit test.
// The basic assumption:
//  1. mockLogicalPlan4Test can generate tow kinds of physical plan: physicalPlan1 and
//     physicalPlan2. physicalPlan1 can pass the property only when they are the same
//     order; while physicalPlan2 cannot match any of the property(in other words, we can
//     generate it only when then property is empty).
//  2. We have a hint for physicalPlan2.
//  3. If the property is empty, we still need to check `canGeneratePlan2` to decide
//     whether it can generate physicalPlan2.
type mockLogicalPlan4Test struct {
	logicalop.BaseLogicalPlan
	// hasHintForPlan2 indicates whether this mockPlan contains hint.
	// This hint is used to generate physicalPlan2. See the implementation
	// of ExhaustPhysicalPlans().
	hasHintForPlan2 bool
	// canGeneratePlan2 indicates whether this plan can generate physicalPlan2.
	canGeneratePlan2 bool
	// costOverflow indicates whether this plan will generate physical plan whose cost is overflowed.
	costOverflow bool
}
