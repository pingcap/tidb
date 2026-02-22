// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

func constructInnerIndexScanTask(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	wrapper *indexJoinInnerChildWrapper,
	path *util.AccessPath,
	ranges ranger.Ranges,
	filterConds []expression.Expression,
	idxOffset2joinKeyOffset []int,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
	maxOneRow bool,
) base.Task {
	copTask := constructDS2IndexScanTask(wrapper.ds, path, ranges, filterConds, idxOffset2joinKeyOffset, rangeInfo, keepOrder, desc, rowCount, maxOneRow)
	if copTask == nil {
		return nil
	}
	return constructIndexJoinInnerSideTaskWithAggCheck(p, prop, copTask.(*physicalop.CopTask), wrapper.ds, path, wrapper)
}

// constructDS2IndexScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func constructDS2IndexScanTask(
	ds *logicalop.DataSource,
	path *util.AccessPath,
	ranges ranger.Ranges,
	filterConds []expression.Expression,
	idxOffset2joinKeyOffset []int,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
	maxOneRow bool,
) base.Task {
	// If `ds.TableInfo.GetPartitionInfo() != nil`,
	// it means the data source is a partition table reader.
	// If the inner task need to keep order, the partition table reader can't satisfy it.
	if keepOrder && ds.TableInfo.GetPartitionInfo() != nil {
		return nil
	}
	is := physicalop.PhysicalIndexScan{
		Table:            ds.TableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            path.Index,
		IdxCols:          path.IdxCols,
		IdxColLens:       path.IdxColLens,
		DataSourceSchema: ds.Schema(),
		KeepOrder:        keepOrder,
		Ranges:           ranges,
		RangeInfo:        rangeInfo,
		Desc:             desc,
		IsPartition:      ds.PartitionDefIdx != nil,
		PhysicalTableID:  ds.PhysicalTableID,
		TblColHists:      ds.TblColHists,
		PkIsHandleCol:    ds.GetPKIsHandleCol(),
	}.Init(ds.SCtx(), ds.QueryBlockOffset())

	is.SetNoncacheableReason(path.NoncacheableReason)

	cop := &physicalop.CopTask{
		IndexPlan:   is,
		TblColHists: ds.TblColHists,
		TblCols:     ds.TblCols,
		KeepOrder:   is.KeepOrder,
	}
	cop.PhysPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}
	if !path.IsSingleScan {
		// On this way, it's double read case.
		ts := physicalop.PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			TableAsName:     ds.TableAsName,
			DBName:          ds.DBName,
			PhysicalTableID: ds.PhysicalTableID,
			TblCols:         ds.TblCols,
			TblColHists:     ds.TblColHists,
		}.Init(ds.SCtx(), ds.QueryBlockOffset())
		ts.SetSchema(is.DataSourceSchema.Clone())
		ts.SetIsPartition(ds.PartitionDefIdx != nil)
		if ds.TableInfo.IsCommonHandle {
			commonHandle := ds.HandleCols.(*util.CommonHandleCols)
			for _, col := range commonHandle.GetColumns() {
				if ts.Schema().ColumnIndex(col) == -1 {
					ts.Schema().Append(col)
					ts.Columns = append(ts.Columns, col.ToInfo())
					cop.NeedExtraProj = true
				}
			}
		}
		// We set `StatsVersion` here and fill other fields in `(*copTask).finishIndexPlan`. Since `copTask.IndexPlan` may
		// change before calling `(*copTask).finishIndexPlan`, we don't know the stats information of `ts` currently and on
		// the other hand, it may be hard to identify `StatsVersion` of `ts` in `(*copTask).finishIndexPlan`.
		ts.SetStats(&property.StatsInfo{StatsVersion: ds.TableStats.StatsVersion})
		usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
		if usedStats != nil && usedStats.GetUsedInfo(ts.PhysicalTableID) != nil {
			ts.UsedStatsInfo = usedStats.GetUsedInfo(ts.PhysicalTableID)
		}
		// If inner cop task need keep order, the extraHandleCol should be set.
		if cop.KeepOrder && !ds.TableInfo.IsCommonHandle {
			var needExtraProj bool
			cop.ExtraHandleCol, needExtraProj = ts.AppendExtraHandleCol(ds)
			cop.NeedExtraProj = cop.NeedExtraProj || needExtraProj
		}
		if cop.NeedExtraProj {
			cop.OriginSchema = ds.Schema()
		}
		cop.TablePlan = ts
	}
	if cop.TablePlan != nil && ds.TableInfo.IsCommonHandle {
		cop.CommonHandleCols = ds.CommonHandleCols
	}
	is.InitSchema(append(path.FullIdxCols, ds.CommonHandleCols...), cop.TablePlan != nil)
	indexConds, tblConds := splitIndexFilterConditions(ds, filterConds, path.FullIdxCols, path.FullIdxColLens)

	// Note: due to a regression in JOB workload, we use the optimizer fix control to enable this for now.
	//
	// Because we are estimating an average row count of the inner side corresponding to each row from the outer side,
	// the estimated row count of the IndexScan should be no larger than (total row count / NDV of join key columns).
	// We can calculate the lower bound of the NDV therefore we can get an upper bound of the row count here.
	rowCountUpperBound := -1.0
	fixControlOK := fixcontrol.GetBoolWithDefault(ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix44855, false)
	ds.SCtx().GetSessionVars().RecordRelevantOptFix(fixcontrol.Fix44855)
	if fixControlOK && ds.TableStats != nil {
		usedColIDs := make([]int64, 0)
		// We only consider columns in this index that (1) are used to probe as join key,
		// and (2) are not prefix column in the index (for which we can't easily get a lower bound)
		for idxOffset, joinKeyOffset := range idxOffset2joinKeyOffset {
			if joinKeyOffset < 0 ||
				path.FullIdxColLens[idxOffset] != types.UnspecifiedLength ||
				path.FullIdxCols[idxOffset] == nil {
				continue
			}
			usedColIDs = append(usedColIDs, path.FullIdxCols[idxOffset].UniqueID)
		}
		joinKeyNDV := getColsNDVLowerBoundFromHistColl(usedColIDs, ds.TableStats.HistColl)
		if joinKeyNDV > 0 {
			rowCountUpperBound = ds.TableStats.RowCount / float64(joinKeyNDV)
		}
	}

	if rowCountUpperBound > 0 {
		rowCount = math.Min(rowCount, rowCountUpperBound)
	}
	if maxOneRow {
		// Theoretically, this line is unnecessary because row count estimation of join should guarantee rowCount is not larger
		// than 1.0; however, there may be rowCount larger than 1.0 in reality, e.g, pseudo statistics cases, which does not reflect
		// unique constraint in NDV.
		rowCount = math.Min(rowCount, 1.0)
	}
	tmpPath := &util.AccessPath{
		IndexFilters:        indexConds,
		TableFilters:        tblConds,
		CountAfterIndex:     rowCount,
		CountAfterAccess:    rowCount,
		MinCountAfterAccess: 0,
		MaxCountAfterAccess: 0,
	}
	// Assume equal conditions used by index join and other conditions are independent.
	if len(tblConds) > 0 {
		selectivity, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, tblConds, ds.PossibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ds.TableAsName.L))
			selectivity = cost.SelectionFactor
		}
		// rowCount is computed from result row count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterIndex * selectivity`.
		cnt := rowCount / selectivity
		if rowCountUpperBound > 0 {
			cnt = math.Min(cnt, rowCountUpperBound)
		}
		if maxOneRow {
			cnt = math.Min(cnt, 1.0)
		}
		tmpPath.CountAfterIndex = cnt
		tmpPath.CountAfterAccess = cnt
	}
	if len(indexConds) > 0 {
		selectivity, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, indexConds, ds.PossibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ds.TableAsName.L))
			selectivity = cost.SelectionFactor
		}
		cnt := tmpPath.CountAfterIndex / selectivity
		if rowCountUpperBound > 0 {
			cnt = math.Min(cnt, rowCountUpperBound)
		}
		if maxOneRow {
			cnt = math.Min(cnt, 1.0)
		}
		tmpPath.CountAfterAccess = cnt
	}
	is.SetStats(ds.TableStats.ScaleByExpectCnt(is.SCtx().GetSessionVars(), tmpPath.CountAfterAccess))
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(is.PhysicalTableID) != nil {
		is.UsedStatsInfo = usedStats.GetUsedInfo(is.PhysicalTableID)
	}
	finalStats := ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), rowCount)
	if err := addPushedDownSelection4PhysicalIndexScan(is, cop, ds, tmpPath, finalStats); err != nil {
		logutil.BgLogger().Warn("unexpected error happened during addPushedDownSelection4PhysicalIndexScan function", zap.Error(err))
		return nil
	}
	return cop
}

// construct the inner join task by inner child plan tree
// The Logical include two parts: logicalplan->physicalplan, physicalplan->task
// Step1: whether agg can be pushed down to coprocessor
//
//	Step1.1: If the agg can be pushded down to coprocessor, we will build a copTask and attach the agg to the copTask
//	There are two kinds of agg: stream agg and hash agg. Stream agg depends on some conditions, such as the group by cols
//
// Step2: build other inner plan node to task
func constructIndexJoinInnerSideTaskWithAggCheck(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, dsCopTask *physicalop.CopTask, ds *logicalop.DataSource, path *util.AccessPath, wrapper *indexJoinInnerChildWrapper) base.Task {
	var la *logicalop.LogicalAggregation
	var canPushAggToCop bool
	if len(wrapper.zippedChildren) > 0 {
		la, canPushAggToCop = wrapper.zippedChildren[len(wrapper.zippedChildren)-1].(*logicalop.LogicalAggregation)
		if la != nil && la.HasDistinct() {
			// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
			// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
			if !la.SCtx().GetSessionVars().AllowDistinctAggPushDown {
				canPushAggToCop = false
			}
		}
	}

	// If the bottom plan is not aggregation or the aggregation can't be pushed to coprocessor, we will construct a root task directly.
	if !canPushAggToCop {
		result := dsCopTask.ConvertToRootTask(ds.SCtx()).(*physicalop.RootTask)
		return constructIndexJoinInnerSideTask(result, prop, wrapper.zippedChildren, false)
	}

	numAgg := 0
	for _, child := range wrapper.zippedChildren {
		if _, ok := child.(*logicalop.LogicalAggregation); ok {
			numAgg++
		}
	}
	if numAgg > 1 {
		// can't support this case now, see #61669.
		return base.InvalidTask
	}

	// Try stream aggregation first.
	// We will choose the stream aggregation if the following conditions are met:
	// 1. Force hint stream agg by /*+ stream_agg() */
	// 2. Other conditions copy from getStreamAggs() in exhaust_physical_plans.go
	_, preferStream := la.ResetHintIfConflicted()
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.Mode == aggregation.FinalMode {
			preferStream = false
			break
		}
	}
	// group by a + b is not interested in any order.
	groupByCols := la.GetGroupByCols()
	if len(groupByCols) != len(la.GroupByItems) {
		preferStream = false
	}
	if la.HasDistinct() && !la.DistinctArgsMeetsProperty() {
		preferStream = false
	}
	// sort items must be the super set of group by items
	if path != nil && path.Index != nil && !path.Index.MVIndex &&
		ds.TableInfo.GetPartitionInfo() == nil {
		if len(path.IdxCols) < len(groupByCols) {
			preferStream = false
		} else {
			sctx := p.SCtx()
			for i, groupbyCol := range groupByCols {
				if path.IdxColLens[i] != types.UnspecifiedLength ||
					!groupbyCol.EqualByExprAndID(sctx.GetExprCtx().GetEvalCtx(), path.IdxCols[i]) {
					preferStream = false
				}
			}
		}
	} else {
		preferStream = false
	}

	// build physical agg and attach to task
	var aggTask base.Task
	// build stream agg and change ds keep order to true
	stats := la.StatsInfo()
	if dsCopTask.IndexPlan != nil {
		stats = stats.ScaleByExpectCnt(p.SCtx().GetSessionVars(), dsCopTask.IndexPlan.StatsInfo().RowCount)
	} else if dsCopTask.TablePlan != nil {
		stats = stats.ScaleByExpectCnt(p.SCtx().GetSessionVars(), dsCopTask.TablePlan.StatsInfo().RowCount)
	}
	if preferStream {
		newGbyItems := make([]expression.Expression, len(la.GroupByItems))
		copy(newGbyItems, la.GroupByItems)
		newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
		copy(newAggFuncs, la.AggFuncs)
		baseAgg := &physicalop.BasePhysicalAgg{
			GroupByItems: newGbyItems,
			AggFuncs:     newAggFuncs,
		}
		streamAgg := baseAgg.InitForStream(la.SCtx(), la.StatsInfo(), la.QueryBlockOffset(), la.Schema().Clone(), prop)
		// change to keep order for index scan and dsCopTask
		if dsCopTask.IndexPlan != nil {
			// get the index scan from dsCopTask.IndexPlan
			physicalIndexScan, _ := dsCopTask.IndexPlan.(*physicalop.PhysicalIndexScan)
			if physicalIndexScan == nil && len(dsCopTask.IndexPlan.Children()) == 1 {
				physicalIndexScan, _ = dsCopTask.IndexPlan.Children()[0].(*physicalop.PhysicalIndexScan)
			}
			// The double read case should change the table plan together if we want to build stream agg,
			// so it need to find out the table scan
			// Try to get the physical table scan from dsCopTask.TablePlan
			// now, we only support the pattern tablescan and tablescan+selection
			var physicalTableScan *physicalop.PhysicalTableScan
			if dsCopTask.TablePlan != nil {
				physicalTableScan, _ = dsCopTask.TablePlan.(*physicalop.PhysicalTableScan)
				if physicalTableScan == nil && len(dsCopTask.TablePlan.Children()) == 1 {
					physicalTableScan, _ = dsCopTask.TablePlan.Children()[0].(*physicalop.PhysicalTableScan)
				}
				// We may not be able to build stream agg, break here and directly build hash agg
				if physicalTableScan == nil {
					goto buildHashAgg
				}
			}
			if physicalIndexScan != nil {
				physicalIndexScan.KeepOrder = true
				dsCopTask.KeepOrder = true
				// Fix issue #60297, if index lookup(double read) as build side and table key is not common handle(row_id),
				// we need to reset extraHandleCol and needExtraProj.
				// The reason why the reset cop task needs to be specially modified here is that:
				// The cop task has been constructed in the previous logic,
				// but it was not possible to determine whether the stream agg was needed (that is, whether keep order was true).
				// Therefore, when updating the keep order, the relevant properties in the cop task need to be modified at the same time.
				// The following code is copied from the logic when keep order is true in function constructDS2IndexScanTask.
				if dsCopTask.TablePlan != nil && physicalTableScan != nil && !ds.TableInfo.IsCommonHandle {
					var needExtraProj bool
					dsCopTask.ExtraHandleCol, needExtraProj = physicalTableScan.AppendExtraHandleCol(ds)
					dsCopTask.NeedExtraProj = dsCopTask.NeedExtraProj || needExtraProj
				}
				if dsCopTask.NeedExtraProj {
					dsCopTask.OriginSchema = ds.Schema()
				}
				streamAgg.SetStats(stats)
				aggTask = streamAgg.Attach2Task(dsCopTask)
			}
		}
	}

buildHashAgg:
	// build hash agg, when the stream agg is illegal such as the order by prop is not matched
	if aggTask == nil {
		physicalHashAgg := physicalop.NewPhysicalHashAgg(la, stats, prop)
		physicalHashAgg.SetSchema(la.Schema().Clone())
		aggTask = physicalHashAgg.Attach2Task(dsCopTask)
	}

	// build other inner plan node to task
	result, ok := aggTask.(*physicalop.RootTask)
	if !ok {
		return nil
	}
	return constructIndexJoinInnerSideTask(result, prop, wrapper.zippedChildren, true)
}
