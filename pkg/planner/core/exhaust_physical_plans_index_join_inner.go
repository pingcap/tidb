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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

// completeIndexJoinFeedBackInfo completes the IndexJoinInfo for the innerTask.
// indexJoin
//
//	+--- outer child
//	+--- inner child (say: projection ------------> unionScan -------------> ds)
//	        <-------RootTask(IndexJoinInfo) <--RootTask(IndexJoinInfo) <--copTask(IndexJoinInfo)
//
// when we build the underlying datasource as table-scan, we will return wrap it and
// return as a CopTask, inside which the index join contains some index path chosen
// information which will be used in indexJoin execution runtime: ref IndexJoinInfo
// declaration for more information.
// the indexJoinInfo will be filled back to the innerTask, passed upward to RootTask
// once this copTask is converted to RootTask type, and finally end up usage in the
// indexJoin's attach2Task with calling completePhysicalIndexJoin.
func completeIndexJoinFeedBackInfo(innerTask *physicalop.CopTask, indexJoinResult *indexJoinPathResult, ranges ranger.MutableRanges, keyOff2IdxOff []int) {
	info := innerTask.IndexJoinInfo
	if info == nil {
		info = &physicalop.IndexJoinInfo{}
	}
	if indexJoinResult != nil {
		if indexJoinResult.chosenPath != nil {
			info.IdxColLens = indexJoinResult.chosenPath.IdxColLens
		}
		info.CompareFilters = indexJoinResult.lastColManager
	}
	info.Ranges = ranges
	info.KeyOff2IdxOff = keyOff2IdxOff
	// fill it back to the bottom-up Task.
	innerTask.IndexJoinInfo = info
}

// buildIndexJoinInner2TableScan builds a TableScan as the inner child for an
// IndexJoin if possible.
// If the inner side of an index join is a TableScan, only one tuple will be
// fetched from the inner side for every tuple from the outer side. This will be
// promised to be no worse than building IndexScan as the inner child.
func buildIndexJoinInner2TableScan(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty, wrapper *indexJoinInnerChildWrapper,
	innerJoinKeys, outerJoinKeys []*expression.Column,
	outerIdx int, avgInnerRowCnt float64) (joins []base.PhysicalPlan) {
	ds := wrapper.ds
	var tblPath *util.AccessPath
	for _, path := range ds.PossibleAccessPaths {
		if path.IsTablePath() && path.StoreType == kv.TiKV {
			tblPath = path
			break
		}
	}
	if tblPath == nil {
		return nil
	}
	var keyOff2IdxOff []int
	var ranges ranger.MutableRanges = ranger.Ranges{}
	var innerTask, innerTask2 base.Task
	var indexJoinResult *indexJoinPathResult
	if ds.TableInfo.IsCommonHandle {
		indexJoinResult, keyOff2IdxOff = getBestIndexJoinPathResult(p, ds, innerJoinKeys, outerJoinKeys, func(path *util.AccessPath) bool { return path.IsCommonHandlePath })
		if indexJoinResult == nil {
			return nil
		}
		rangeInfo, maxOneRow := indexJoinPathGetRangeInfoAndMaxOneRow(p.SCtx(), outerJoinKeys, indexJoinResult)
		innerTask = constructInnerTableScanTask(p, prop, wrapper, indexJoinResult.chosenRanges.Range(), rangeInfo, false, false, avgInnerRowCnt, maxOneRow)
		// The index merge join's inner plan is different from index join, so we
		// should construct another inner plan for it.
		// Because we can't keep order for union scan, if there is a union scan in inner task,
		// we can't construct index merge join.
		if !wrapper.hasDitryWrite {
			innerTask2 = constructInnerTableScanTask(p, prop, wrapper, indexJoinResult.chosenRanges.Range(), rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt, maxOneRow)
		}
		ranges = indexJoinResult.chosenRanges
	} else {
		var (
			ok bool
			// note: pk col doesn't have mutableRanges, the global var(ranges) which will be handled as empty range in constructIndexJoin.
			localRanges ranger.Ranges
		)
		keyOff2IdxOff, outerJoinKeys, localRanges, _, ok = getIndexJoinIntPKPathInfo(ds, innerJoinKeys, outerJoinKeys, func(path *util.AccessPath) bool { return path.IsIntHandlePath })
		if !ok {
			return nil
		}
		// For IntHandle (integer primary key), it's always a unique match.
		maxOneRow := true
		rangeInfo := indexJoinIntPKRangeInfo(p.SCtx().GetExprCtx().GetEvalCtx(), outerJoinKeys)
		innerTask = constructInnerTableScanTask(p, prop, wrapper, localRanges, rangeInfo, false, false, avgInnerRowCnt, maxOneRow)
		// The index merge join's inner plan is different from index join, so we
		// should construct another inner plan for it.
		// Because we can't keep order for union scan, if there is a union scan in inner task,
		// we can't construct index merge join.
		if !wrapper.hasDitryWrite {
			innerTask2 = constructInnerTableScanTask(p, prop, wrapper, localRanges, rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt, maxOneRow)
		}
	}
	var (
		path       *util.AccessPath
		lastColMng *physicalop.ColWithCmpFuncManager
	)
	if indexJoinResult != nil {
		path = indexJoinResult.chosenPath
		lastColMng = indexJoinResult.lastColManager
	}
	joins = make([]base.PhysicalPlan, 0, 3)
	joins = append(joins, constructIndexJoin(p, prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, lastColMng, true)...)
	// We can reuse the `innerTask` here since index nested loop hash join
	// do not need the inner child to promise the order.
	joins = append(joins, constructIndexHashJoin(p, prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, lastColMng)...)
	if innerTask2 != nil {
		joins = append(joins, constructIndexMergeJoin(p, prop, outerIdx, innerTask2, ranges, keyOff2IdxOff, path, lastColMng)...)
	}
	return joins
}

func buildIndexJoinInner2IndexScan(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty, wrapper *indexJoinInnerChildWrapper, innerJoinKeys, outerJoinKeys []*expression.Column,
	outerIdx int, avgInnerRowCnt float64) (joins []base.PhysicalPlan) {
	ds := wrapper.ds
	indexValid := func(path *util.AccessPath) bool {
		if path.IsTablePath() {
			return false
		}
		// if path is index path. index path currently include two kind of, one is normal, and the other is mv index.
		// for mv index like mvi(a, json, b), if driving condition is a=1, and we build a prefix scan with range [1,1]
		// on mvi, it will return many index rows which breaks handle-unique attribute here.
		//
		// the basic rule is that: mv index can be and can only be accessed by indexMerge operator. (embedded handle duplication)
		if !path.IsIndexJoinUnapplicable() {
			return true // not a MVIndex path, it can successfully be index join probe side.
		}
		return false
	}
	indexJoinResult, keyOff2IdxOff := getBestIndexJoinPathResult(p, ds, innerJoinKeys, outerJoinKeys, indexValid)
	if indexJoinResult == nil {
		return nil
	}
	joins = make([]base.PhysicalPlan, 0, 3)
	rangeInfo, maxOneRow := indexJoinPathGetRangeInfoAndMaxOneRow(p.SCtx(), outerJoinKeys, indexJoinResult)
	innerTask := constructInnerIndexScanTask(p, prop, wrapper, indexJoinResult.chosenPath, indexJoinResult.chosenRanges.Range(), indexJoinResult.chosenRemained, indexJoinResult.idxOff2KeyOff, rangeInfo, false, false, avgInnerRowCnt, maxOneRow)
	if innerTask != nil {
		joins = append(joins, constructIndexJoin(p, prop, outerIdx, innerTask, indexJoinResult.chosenRanges, keyOff2IdxOff, indexJoinResult.chosenPath, indexJoinResult.lastColManager, true)...)
		// We can reuse the `innerTask` here since index nested loop hash join
		// do not need the inner child to promise the order.
		joins = append(joins, constructIndexHashJoin(p, prop, outerIdx, innerTask, indexJoinResult.chosenRanges, keyOff2IdxOff, indexJoinResult.chosenPath, indexJoinResult.lastColManager)...)
	}
	// The index merge join's inner plan is different from index join, so we
	// should construct another inner plan for it.
	// Because we can't keep order for union scan, if there is a union scan in inner task,
	// we can't construct index merge join.
	if !wrapper.hasDitryWrite {
		innerTask2 := constructInnerIndexScanTask(p, prop, wrapper, indexJoinResult.chosenPath, indexJoinResult.chosenRanges.Range(), indexJoinResult.chosenRemained, indexJoinResult.idxOff2KeyOff, rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt, maxOneRow)
		if innerTask2 != nil {
			joins = append(joins, constructIndexMergeJoin(p, prop, outerIdx, innerTask2, indexJoinResult.chosenRanges, keyOff2IdxOff, indexJoinResult.chosenPath, indexJoinResult.lastColManager)...)
		}
	}
	return joins
}

// constructInnerTableScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func constructInnerTableScanTask(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	wrapper *indexJoinInnerChildWrapper,
	ranges ranger.Ranges,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
	maxOneRow bool,
) base.Task {
	copTask := constructDS2TableScanTask(wrapper.ds, ranges, rangeInfo, keepOrder, desc, rowCount, maxOneRow)
	if copTask == nil {
		return nil
	}
	return constructIndexJoinInnerSideTaskWithAggCheck(p, prop, copTask.(*physicalop.CopTask), wrapper.ds, nil, wrapper)
}

// constructInnerTableScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func constructDS2TableScanTask(
	ds *logicalop.DataSource,
	ranges ranger.Ranges,
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
	ts := physicalop.PhysicalTableScan{
		Table:           ds.TableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		FilterCondition: ds.PushedDownConds,
		Ranges:          ranges,
		RangeInfo:       rangeInfo,
		KeepOrder:       keepOrder,
		Desc:            desc,
		PhysicalTableID: ds.PhysicalTableID,
		TblCols:         ds.TblCols,
		TblColHists:     ds.TblColHists,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	ts.SetIsPartition(ds.PartitionDefIdx != nil)
	ts.SetSchema(ds.Schema().Clone())
	if rowCount <= 0 {
		rowCount = float64(1)
	}
	selectivity := float64(1)
	countAfterAccess := rowCount
	if len(ts.FilterCondition) > 0 {
		var err error
		selectivity, err = cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, ts.FilterCondition, ds.PossibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ts.TableAsName.L))
			selectivity = cost.SelectionFactor
		}
		// rowCount is computed from result row count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterAccess * selectivity`.
		countAfterAccess = rowCount / selectivity
	}
	// Only apply the 1-row limit when we can guarantee at most one row per outer row.
	// For CommonHandle, this requires matching ALL primary key columns with equality conditions.
	// For prefix scans (e.g., only matching first column of a composite PK), we trust the statistical estimation.
	finalRowCount := countAfterAccess
	if maxOneRow {
		finalRowCount = math.Min(1.0, countAfterAccess)
	}
	ts.SetStats(&property.StatsInfo{
		RowCount:     finalRowCount,
		StatsVersion: ds.StatsInfo().StatsVersion,
		// NDV would not be used in cost computation of IndexJoin, set leave it as default nil.
	})
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(ts.PhysicalTableID) != nil {
		ts.UsedStatsInfo = usedStats.GetUsedInfo(ts.PhysicalTableID)
	}
	copTask := &physicalop.CopTask{
		TablePlan:         ts,
		IndexPlanFinished: true,
		TblColHists:       ds.TblColHists,
		KeepOrder:         ts.KeepOrder,
	}
	copTask.PhysPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}
	ts.PlanPartInfo = copTask.PhysPlanPartInfo
	selStats := ts.StatsInfo().Scale(ds.SCtx().GetSessionVars(), selectivity)
	addPushedDownSelection4PhysicalTableScan(ts, copTask, selStats, ds.AstIndexHints)
	return copTask
}

func constructIndexJoinInnerSideTask(curTask base.Task, prop *property.PhysicalProperty, zippedChildren []base.LogicalPlan, skipAgg bool) base.Task {
	for i := len(zippedChildren) - 1; i >= 0; i-- {
		switch x := zippedChildren[i].(type) {
		case *logicalop.LogicalUnionScan:
			curTask = constructInnerUnionScan(prop, x, curTask.Plan()).Attach2Task(curTask)
		case *logicalop.LogicalProjection:
			curTask = constructInnerProj(prop, x, curTask.Plan()).Attach2Task(curTask)
		case *logicalop.LogicalSelection:
			curTask = constructInnerSel(prop, x, curTask.Plan()).Attach2Task(curTask)
		case *logicalop.LogicalAggregation:
			if skipAgg {
				continue
			}
			curTask = constructInnerAgg(prop, x, curTask.Plan()).Attach2Task(curTask)
		}
		if curTask.Invalid() {
			return nil
		}
	}
	return curTask
}

func constructInnerAgg(prop *property.PhysicalProperty, logicalAgg *logicalop.LogicalAggregation, child base.PhysicalPlan) base.PhysicalPlan {
	if logicalAgg == nil {
		return child
	}
	physicalHashAgg := physicalop.NewPhysicalHashAgg(logicalAgg, logicalAgg.StatsInfo(), prop)
	physicalHashAgg.SetSchema(logicalAgg.Schema().Clone())
	return physicalHashAgg
}

func constructInnerSel(prop *property.PhysicalProperty, sel *logicalop.LogicalSelection, child base.PhysicalPlan) base.PhysicalPlan {
	if sel == nil {
		return child
	}
	physicalSel := physicalop.PhysicalSelection{
		Conditions: sel.Conditions,
	}.Init(sel.SCtx(), sel.StatsInfo(), sel.QueryBlockOffset(), prop)
	return physicalSel
}

func constructInnerProj(prop *property.PhysicalProperty, proj *logicalop.LogicalProjection, child base.PhysicalPlan) base.PhysicalPlan {
	if proj == nil {
		return child
	}
	physicalProj := physicalop.PhysicalProjection{
		Exprs:            proj.Exprs,
		CalculateNoDelay: proj.CalculateNoDelay,
	}.Init(proj.SCtx(), proj.StatsInfo(), proj.QueryBlockOffset(), prop)
	physicalProj.SetSchema(proj.Schema())
	return physicalProj
}

func constructInnerUnionScan(prop *property.PhysicalProperty, us *logicalop.LogicalUnionScan, childPlan base.PhysicalPlan) base.PhysicalPlan {
	if us == nil {
		return childPlan
	}
	// Use `reader.StatsInfo()` instead of `us.StatsInfo()` because it should be more accurate. No need to specify
	// childrenReqProps now since we have got reader already.
	physicalUnionScan := physicalop.PhysicalUnionScan{
		Conditions: us.Conditions,
		HandleCols: us.HandleCols,
	}.Init(us.SCtx(), childPlan.StatsInfo(), us.QueryBlockOffset(), prop)
	return physicalUnionScan
}

// getColsNDVLowerBoundFromHistColl tries to get a lower bound of the NDV of columns (whose uniqueIDs are colUIDs).
func getColsNDVLowerBoundFromHistColl(colUIDs []int64, histColl *statistics.HistColl) int64 {
	if len(colUIDs) == 0 || histColl == nil {
		return -1
	}

	// 1. Try to get NDV from column stats if it's a single column.
	if len(colUIDs) == 1 && histColl.ColNum() > 0 {
		uid := colUIDs[0]
		if colStats := histColl.GetCol(uid); colStats != nil && colStats.IsStatsInitialized() {
			return colStats.NDV
		}
	}

	slices.Sort(colUIDs)

	// 2. Try to get NDV from index stats.
	// Note that we don't need to specially handle prefix index here, because the NDV of a prefix index is
	// equal or less than the corresponding normal index, and that's safe here since we want a lower bound.
	for idxID, idxCols := range histColl.Idx2ColUniqueIDs {
		if len(idxCols) != len(colUIDs) {
			continue
		}
		orderedIdxCols := make([]int64, len(idxCols))
		copy(orderedIdxCols, idxCols)
		slices.Sort(orderedIdxCols)
		if !slices.Equal(orderedIdxCols, colUIDs) {
			continue
		}
		if idxStats := histColl.GetIdx(idxID); idxStats != nil && idxStats.IsStatsInitialized() {
			return idxStats.NDV
		}
	}

	// TODO: if there's an index that contains the expected columns, we can also make use of its NDV.
	// For example, NDV(a,b,c) / NDV(c) is a safe lower bound of NDV(a,b).

	// 3. If we still haven't got an NDV, we use the maximum NDV in the column stats as a lower bound.
	maxNDV := int64(-1)
	for _, uid := range colUIDs {
		colStats := histColl.GetCol(uid)
		if colStats == nil || !colStats.IsStatsInitialized() {
			continue
		}
		maxNDV = max(maxNDV, colStats.NDV)
	}
	return maxNDV
}

// constructInnerIndexScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
