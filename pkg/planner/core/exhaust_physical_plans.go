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
	"fmt"
	"math"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

// exhaustPhysicalPlans generates all possible plans that can match the required property.
// It will return:
// 1. All possible plans that can match the required property.
// 2. Whether the SQL hint can work. Return true if there is no hint.
func exhaustPhysicalPlans(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	switch x := lp.(type) {
	case *logicalop.LogicalCTE:
		return physicalop.ExhaustPhysicalPlans4LogicalCTE(x, prop)
	case *logicalop.LogicalSort:
		return exhaustPhysicalPlans4LogicalSort(x, prop)
	case *logicalop.LogicalTopN:
		return exhaustPhysicalPlans4LogicalTopN(x, prop)
	case *logicalop.LogicalLock:
		return exhaustPhysicalPlans4LogicalLock(x, prop)
	case *logicalop.LogicalJoin:
		return physicalop.ExhaustPhysicalPlans4LogicalJoin(x, prop)
	case *logicalop.LogicalApply:
		return exhaustPhysicalPlans4LogicalApply(x, prop)
	case *logicalop.LogicalLimit:
		return physicalop.ExhaustPhysicalPlans4LogicalLimit(x, prop)
	case *logicalop.LogicalWindow:
		return exhaustPhysicalPlans4LogicalWindow(x, prop)
	case *logicalop.LogicalExpand:
		return exhaustPhysicalPlans4LogicalExpand(x, prop)
	case *logicalop.LogicalUnionAll:
		return exhaustPhysicalPlans4LogicalUnionAll(x, prop)
	case *logicalop.LogicalSequence:
		return exhaustPhysicalPlans4LogicalSequence(x, prop)
	case *logicalop.LogicalSelection:
		return physicalop.ExhaustPhysicalPlans4LogicalSelection(x, prop)
	case *logicalop.LogicalMaxOneRow:
		return exhaustPhysicalPlans4LogicalMaxOneRow(x, prop)
	case *logicalop.LogicalUnionScan:
		return physicalop.ExhaustPhysicalPlans4LogicalUnionScan(x, prop)
	case *logicalop.LogicalProjection:
		return physicalop.ExhaustPhysicalPlans4LogicalProjection(x, prop)
	case *logicalop.LogicalAggregation:
		return physicalop.ExhaustPhysicalPlans4LogicalAggregation(x, prop)
	case *logicalop.LogicalPartitionUnionAll:
		return exhaustPhysicalPlans4LogicalPartitionUnionAll(x, prop)
	case *memo.GroupExpression:
		return x.ExhaustPhysicalPlans(prop)
	case *mockLogicalPlan4Test:
		return x.ExhaustPhysicalPlans(prop)
	default:
		panic("unreachable")
	}
}

// completePhysicalIndexJoin
// as you see, completePhysicalIndexJoin is called in attach2Task, when the inner plan of a physical index join or a
// physical index hash join is built bottom-up. Then indexJoin info is passed bottom-up within the Task to be filled.
// completePhysicalIndexJoin will fill physicalIndexJoin about all the runtime information it lacks in static enumeration
// phase.
// There are several things to be filled:
// 1. ranges:
// as the old comment said: when inner plan is TableReader, the parameter `ranges` will be nil. Because pk
// only have one column. So all of its range is generated during execution time. So set a new ranges{} when it is nil.
//
// 2. KeyOff2IdxOffs: fill the keyOff2IdxOffs' -1 and refill the eq condition back to other conditions and adjust inner
// or outer keys and info.KeyOff2IdxOff has been used above to re-derive newInnerKeys, newOuterKeys, newIsNullEQ,
// newOtherConds, newKeyOff.
//
//	physic.IsNullEQ = newIsNullEQ
//	physic.InnerJoinKeys = newInnerKeys
//	physic.OuterJoinKeys = newOuterKeys
//	physic.OtherConditions = newOtherConds
//	physic.KeyOff2IdxOff = newKeyOff
//
// 3. OuterHashKeys, InnerHashKeys:
// for indexHashJoin, those not used EQ condition which has been moved into the new other-conditions can be extracted out
// to build the hash table to avoid lazy evaluation as other conditions.
//
//  4. Info's Ranges, IdxColLens, CompareFilters:
//     the underlying ds chosen path's ranges, idxColLens, and compareFilters.
//     physic.Ranges = info.Ranges
//     physic.IdxColLens = info.IdxColLens
//     physic.CompareFilters = info.CompareFilters
func completePhysicalIndexJoin(physic *physicalop.PhysicalIndexJoin, rt *RootTask, innerS, outerS *expression.Schema, extractOtherEQ bool) base.PhysicalPlan {
	info := rt.IndexJoinInfo
	// runtime fill back ranges
	if info.Ranges == nil {
		info.Ranges = ranger.Ranges{} // empty range
	}
	// set the new key off according to the index join info's keyOff2IdxOff
	newKeyOff := make([]int, 0, len(info.KeyOff2IdxOff))
	// IsNullEQ & InnerJoinKeys & OuterJoinKeys in physic may change.
	newIsNullEQ := make([]bool, 0, len(physic.IsNullEQ))
	newInnerKeys := make([]*expression.Column, 0, len(physic.InnerJoinKeys))
	newOuterKeys := make([]*expression.Column, 0, len(physic.OuterJoinKeys))
	// OtherCondition may change because EQ can be leveraged in hash table retrieve.
	newOtherConds := make([]expression.Expression, len(physic.OtherConditions), len(physic.OtherConditions)+len(physic.EqualConditions))
	copy(newOtherConds, physic.OtherConditions)
	for keyOff, idxOff := range info.KeyOff2IdxOff {
		// if the keyOff is not used in the index join, we need to move the equal condition back to other conditions to eval them.
		if info.KeyOff2IdxOff[keyOff] < 0 {
			newOtherConds = append(newOtherConds, physic.EqualConditions[keyOff])
			continue
		}
		// collecting the really used inner keys, outer keys, isNullEQ, and keyOff2IdxOff.
		newInnerKeys = append(newInnerKeys, physic.InnerJoinKeys[keyOff])
		newOuterKeys = append(newOuterKeys, physic.OuterJoinKeys[keyOff])
		newIsNullEQ = append(newIsNullEQ, physic.IsNullEQ[keyOff])
		newKeyOff = append(newKeyOff, idxOff)
	}

	// we can use the `col <eq> col` in new `OtherCondition` to build the hashtable to avoid the unnecessary calculating.
	// for indexHashJoin, those not used EQ condition which has been moved into new other-conditions can be extracted out
	// to build the hash table.
	var outerHashKeys, innerHashKeys []*expression.Column
	outerHashKeys, innerHashKeys = make([]*expression.Column, len(newOuterKeys)), make([]*expression.Column, len(newInnerKeys))
	// used innerKeys and outerKeys can surely be the hashKeys, besides that, the EQ in otherConds can also be used as hashKeys.
	copy(outerHashKeys, newOuterKeys)
	copy(innerHashKeys, newInnerKeys)
	for i := len(newOtherConds) - 1; extractOtherEQ && i >= 0; i = i - 1 {
		switch c := newOtherConds[i].(type) {
		case *expression.ScalarFunction:
			if c.FuncName.L == ast.EQ {
				lhs, ok1 := c.GetArgs()[0].(*expression.Column)
				rhs, ok2 := c.GetArgs()[1].(*expression.Column)
				if ok1 && ok2 {
					if lhs.InOperand || rhs.InOperand {
						// if this other-cond is from a `[not] in` sub-query, do not convert it into eq-cond since
						// IndexJoin cannot deal with NULL correctly in this case; please see #25799 for more details.
						continue
					}
					// when it arrives here, we can sure that we got a EQ conditions and each side of them is a bare
					// column, while we don't know whether each of them comes from the inner or outer, so check it.
					outerSchema, innerSchema := outerS, innerS
					if outerSchema.Contains(lhs) && innerSchema.Contains(rhs) {
						outerHashKeys = append(outerHashKeys, lhs) // nozero
						innerHashKeys = append(innerHashKeys, rhs) // nozero
					} else if innerSchema.Contains(lhs) && outerSchema.Contains(rhs) {
						outerHashKeys = append(outerHashKeys, rhs) // nozero
						innerHashKeys = append(innerHashKeys, lhs) // nozero
					}
					// if not, this EQ function is useless, keep it in new other conditions.
					newOtherConds = slices.Delete(newOtherConds, i, i+1)
				}
			}
		default:
			continue
		}
	}
	// then, fill all newXXX runtime info back to the physic indexJoin.
	// info.KeyOff2IdxOff has been used above to derive newInnerKeys, newOuterKeys, newIsNullEQ, newOtherConds, newKeyOff.
	physic.IsNullEQ = newIsNullEQ
	physic.InnerJoinKeys = newInnerKeys
	physic.OuterJoinKeys = newOuterKeys
	physic.OtherConditions = newOtherConds
	physic.KeyOff2IdxOff = newKeyOff
	// the underlying ds chosen path's ranges, idxColLens, and compareFilters.
	physic.Ranges = info.Ranges
	physic.IdxColLens = info.IdxColLens
	physic.CompareFilters = info.CompareFilters
	// fill executing hashKeys, which containing inner/outer keys, and extracted EQ keys from otherConds if any.
	physic.OuterHashKeys = outerHashKeys
	physic.InnerHashKeys = innerHashKeys
	// the logical EqualConditions is not used anymore in later phase.
	physic.EqualConditions = nil
	// clear rootTask's indexJoinInfo in case of pushing upward, because physical index join is indexJoinInfo's consumer.
	rt.IndexJoinInfo = nil
	return physic
}

func constructIndexMergeJoin(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask base.Task,
	ranges ranger.MutableRanges,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *physicalop.ColWithCmpFuncManager,
) []base.PhysicalPlan {
	hintExists := false
	if (outerIdx == 1 && (p.PreferJoinType&h.PreferLeftAsINLMJInner) > 0) || (outerIdx == 0 && (p.PreferJoinType&h.PreferRightAsINLMJInner) > 0) {
		hintExists = true
	}
	indexJoins := constructIndexJoin(p, prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, compareFilters, !hintExists)
	indexMergeJoins := make([]base.PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*physicalop.PhysicalIndexJoin)
		// Index merge join can't handle hash keys. So we ban it heuristically.
		if len(join.InnerHashKeys) > len(join.InnerJoinKeys) {
			return nil
		}

		// EnumType/SetType Unsupported: merge join conflicts with index order.
		// ref: https://github.com/pingcap/tidb/issues/24473, https://github.com/pingcap/tidb/issues/25669
		for _, innerKey := range join.InnerJoinKeys {
			if innerKey.RetType.GetType() == mysql.TypeEnum || innerKey.RetType.GetType() == mysql.TypeSet {
				return nil
			}
		}
		for _, outerKey := range join.OuterJoinKeys {
			if outerKey.RetType.GetType() == mysql.TypeEnum || outerKey.RetType.GetType() == mysql.TypeSet {
				return nil
			}
		}

		hasPrefixCol := false
		for _, l := range join.IdxColLens {
			if l != types.UnspecifiedLength {
				hasPrefixCol = true
				break
			}
		}
		// If index column has prefix length, the merge join can not guarantee the relevance
		// between index and join keys. So we should skip this case.
		// For more details, please check the following code and comments.
		if hasPrefixCol {
			continue
		}

		// keyOff2KeyOffOrderByIdx is map the join keys offsets to [0, len(joinKeys)) ordered by the
		// join key position in inner index.
		keyOff2KeyOffOrderByIdx := make([]int, len(join.OuterJoinKeys))
		keyOffMapList := make([]int, len(join.KeyOff2IdxOff))
		copy(keyOffMapList, join.KeyOff2IdxOff)
		keyOffMap := make(map[int]int, len(keyOffMapList))
		for i, idxOff := range keyOffMapList {
			keyOffMap[idxOff] = i
		}
		slices.Sort(keyOffMapList)
		keyIsIndexPrefix := true
		for keyOff, idxOff := range keyOffMapList {
			if keyOff != idxOff {
				keyIsIndexPrefix = false
				break
			}
			keyOff2KeyOffOrderByIdx[keyOffMap[idxOff]] = keyOff
		}
		if !keyIsIndexPrefix {
			continue
		}
		// isOuterKeysPrefix means whether the outer join keys are the prefix of the prop items.
		isOuterKeysPrefix := len(join.OuterJoinKeys) <= len(prop.SortItems)
		compareFuncs := make([]expression.CompareFunc, 0, len(join.OuterJoinKeys))
		outerCompareFuncs := make([]expression.CompareFunc, 0, len(join.OuterJoinKeys))

		for i := range join.KeyOff2IdxOff {
			if isOuterKeysPrefix && !prop.SortItems[i].Col.EqualColumn(join.OuterJoinKeys[keyOff2KeyOffOrderByIdx[i]]) {
				isOuterKeysPrefix = false
			}
			compareFuncs = append(compareFuncs, expression.GetCmpFunction(p.SCtx().GetExprCtx(), join.OuterJoinKeys[i], join.InnerJoinKeys[i]))
			outerCompareFuncs = append(outerCompareFuncs, expression.GetCmpFunction(p.SCtx().GetExprCtx(), join.OuterJoinKeys[i], join.OuterJoinKeys[i]))
		}
		// canKeepOuterOrder means whether the prop items are the prefix of the outer join keys.
		canKeepOuterOrder := len(prop.SortItems) <= len(join.OuterJoinKeys)
		for i := 0; canKeepOuterOrder && i < len(prop.SortItems); i++ {
			if !prop.SortItems[i].Col.EqualColumn(join.OuterJoinKeys[keyOff2KeyOffOrderByIdx[i]]) {
				canKeepOuterOrder = false
			}
		}
		// Since index merge join requires prop items the prefix of outer join keys
		// or outer join keys the prefix of the prop items. So we need `canKeepOuterOrder` or
		// `isOuterKeysPrefix` to be true.
		if canKeepOuterOrder || isOuterKeysPrefix {
			indexMergeJoin := physicalop.PhysicalIndexMergeJoin{
				PhysicalIndexJoin:       *join,
				KeyOff2KeyOffOrderByIdx: keyOff2KeyOffOrderByIdx,
				NeedOuterSort:           !isOuterKeysPrefix,
				CompareFuncs:            compareFuncs,
				OuterCompareFuncs:       outerCompareFuncs,
				Desc:                    !prop.IsSortItemEmpty() && prop.SortItems[0].Desc,
			}.Init(p.SCtx())
			indexMergeJoins = append(indexMergeJoins, indexMergeJoin)
		}
	}
	return indexMergeJoins
}

func checkOpSelfSatisfyPropTaskTypeRequirement(p base.LogicalPlan, prop *property.PhysicalProperty) bool {
	switch prop.TaskTp {
	case property.MppTaskType:
		// when parent operator ask current op to be mppTaskType, check operator itself here.
		return logicalop.CanSelfBeingPushedToCopImpl(p, kv.TiFlash)
	case property.CopSingleReadTaskType, property.CopMultiReadTaskType:
		return logicalop.CanSelfBeingPushedToCopImpl(p, kv.TiKV)
	default:
		return true
	}
}

// admitIndexJoinInnerChildPattern is used to check whether current physical choosing is under an index join's
// probe side. If it is, and we ganna check the original inner pattern check here to keep compatible with the old.
// the @first bool indicate whether current logical plan is valid of index join inner side.
func admitIndexJoinInnerChildPattern(p base.LogicalPlan) bool {
	switch x := p.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan).Self().(type) {
	case *logicalop.DataSource:
		// DS that prefer tiFlash reading couldn't walk into index join.
		if x.PreferStoreType&h.PreferTiFlash != 0 {
			return false
		}
	case *logicalop.LogicalProjection, *logicalop.LogicalSelection, *logicalop.LogicalAggregation:
		if !p.SCtx().GetSessionVars().EnableINLJoinInnerMultiPattern {
			return false
		}
	case *logicalop.LogicalUnionScan:
	default: // index join inner side couldn't allow join, sort, limit, etc. todo: open it.
		return false
	}
	return true
}

// constructInnerTableScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func constructDS2TableScanTask(
	ds *logicalop.DataSource,
	ranges ranger.Ranges,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
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
		selectivity, _, err = cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, ts.FilterCondition, ds.PossibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ts.TableAsName.L))
			selectivity = cost.SelectionFactor
		}
		// rowCount is computed from result row count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterAccess * selectivity`.
		countAfterAccess = rowCount / selectivity
	}
	ts.SetStats(&property.StatsInfo{
		// TableScan as inner child of IndexJoin can return at most 1 tuple for each outer row.
		RowCount:     math.Min(1.0, countAfterAccess),
		StatsVersion: ds.StatsInfo().StatsVersion,
		// NDV would not be used in cost computation of IndexJoin, set leave it as default nil.
	})
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(ts.PhysicalTableID) != nil {
		ts.UsedStatsInfo = usedStats.GetUsedInfo(ts.PhysicalTableID)
	}
	copTask := &CopTask{
		tablePlan:         ts,
		indexPlanFinished: true,
		tblColHists:       ds.TblColHists,
		keepOrder:         ts.KeepOrder,
	}
	copTask.physPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}
	ts.PlanPartInfo = copTask.physPlanPartInfo
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

// construct the inner join task by inner child plan tree
// The Logical include two parts: logicalplan->physicalplan, physicalplan->task
// Step1: whether agg can be pushed down to coprocessor
//
//	Step1.1: If the agg can be pushded down to coprocessor, we will build a copTask and attach the agg to the copTask
//	There are two kinds of agg: stream agg and hash agg. Stream agg depends on some conditions, such as the group by cols
//
// Step2: build other inner plan node to task
func constructIndexJoinInnerSideTaskWithAggCheck(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, dsCopTask *CopTask, ds *logicalop.DataSource, path *util.AccessPath, wrapper *indexJoinInnerChildWrapper) base.Task {
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
		result := dsCopTask.ConvertToRootTask(ds.SCtx()).(*RootTask)
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
	if dsCopTask.indexPlan != nil {
		stats = stats.ScaleByExpectCnt(p.SCtx().GetSessionVars(), dsCopTask.indexPlan.StatsInfo().RowCount)
	} else if dsCopTask.tablePlan != nil {
		stats = stats.ScaleByExpectCnt(p.SCtx().GetSessionVars(), dsCopTask.tablePlan.StatsInfo().RowCount)
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
		if dsCopTask.indexPlan != nil {
			// get the index scan from dsCopTask.indexPlan
			physicalIndexScan, _ := dsCopTask.indexPlan.(*physicalop.PhysicalIndexScan)
			if physicalIndexScan == nil && len(dsCopTask.indexPlan.Children()) == 1 {
				physicalIndexScan, _ = dsCopTask.indexPlan.Children()[0].(*physicalop.PhysicalIndexScan)
			}
			// The double read case should change the table plan together if we want to build stream agg,
			// so it need to find out the table scan
			// Try to get the physical table scan from dsCopTask.tablePlan
			// now, we only support the pattern tablescan and tablescan+selection
			var physicalTableScan *physicalop.PhysicalTableScan
			if dsCopTask.tablePlan != nil {
				physicalTableScan, _ = dsCopTask.tablePlan.(*physicalop.PhysicalTableScan)
				if physicalTableScan == nil && len(dsCopTask.tablePlan.Children()) == 1 {
					physicalTableScan, _ = dsCopTask.tablePlan.Children()[0].(*physicalop.PhysicalTableScan)
				}
				// We may not be able to build stream agg, break here and directly build hash agg
				if physicalTableScan == nil {
					goto buildHashAgg
				}
			}
			if physicalIndexScan != nil {
				physicalIndexScan.KeepOrder = true
				dsCopTask.keepOrder = true
				// Fix issue #60297, if index lookup(double read) as build side and table key is not common handle(row_id),
				// we need to reset extraHandleCol and needExtraProj.
				// The reason why the reset cop task needs to be specially modified here is that:
				// The cop task has been constructed in the previous logic,
				// but it was not possible to determine whether the stream agg was needed (that is, whether keep order was true).
				// Therefore, when updating the keep order, the relevant properties in the cop task need to be modified at the same time.
				// The following code is copied from the logic when keep order is true in function constructDS2IndexScanTask.
				if dsCopTask.tablePlan != nil && physicalTableScan != nil && !ds.TableInfo.IsCommonHandle {
					var needExtraProj bool
					dsCopTask.extraHandleCol, needExtraProj = physicalTableScan.AppendExtraHandleCol(ds)
					dsCopTask.needExtraProj = dsCopTask.needExtraProj || needExtraProj
				}
				if dsCopTask.needExtraProj {
					dsCopTask.originSchema = ds.Schema()
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
	result, ok := aggTask.(*RootTask)
	if !ok {
		return nil
	}
	return constructIndexJoinInnerSideTask(result, prop, wrapper.zippedChildren, true)
}

func enumerationContainIndexJoin(candidates []base.PhysicalPlan) bool {
	return slices.ContainsFunc(candidates, func(candidate base.PhysicalPlan) bool {
		_, _, ok := getIndexJoinSideAndMethod(candidate)
		return ok
	})
}

func recordWarnings(lp base.LogicalPlan, prop *property.PhysicalProperty, inEnforce bool) error {
	switch x := lp.(type) {
	case *logicalop.LogicalAggregation:
		return recordAggregationHintWarnings(x)
	case *logicalop.LogicalTopN, *logicalop.LogicalLimit:
		return recordLimitToCopWarnings(lp)
	case *logicalop.LogicalJoin:
		return recordIndexJoinHintWarnings(x, prop, inEnforce)
	default:
		// no warnings to record
		return nil
	}
}

func recordAggregationHintWarnings(la *logicalop.LogicalAggregation) error {
	if la.PreferAggToCop {
		return plannererrors.ErrInternal.FastGen("Optimizer Hint AGG_TO_COP is inapplicable")
	}
	return nil
}

func recordLimitToCopWarnings(lp base.LogicalPlan) error {
	var preferPushDown *bool
	switch lp := lp.(type) {
	case *logicalop.LogicalTopN:
		preferPushDown = &lp.PreferLimitToCop
	case *logicalop.LogicalLimit:
		preferPushDown = &lp.PreferLimitToCop
	default:
		return nil
	}
	if *preferPushDown {
		return plannererrors.ErrInternal.FastGen("Optimizer Hint LIMIT_TO_COP is inapplicable")
	}
	return nil
}

// recordIndexJoinHintWarnings records the warnings msg if no valid preferred physic are picked.
// todo: extend recordIndexJoinHintWarnings to support all kind of operator's warnings handling.
func recordIndexJoinHintWarnings(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, inEnforce bool) error {
	// handle mpp join hints first.
	if (p.PreferJoinType&h.PreferShuffleJoin) > 0 || (p.PreferJoinType&h.PreferBCJoin) > 0 {
		var errMsg string
		if (p.PreferJoinType & h.PreferShuffleJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the shuffle_join() hint is invalid"
		} else {
			errMsg = "The join can not push down to the MPP side, the broadcast_join() hint is invalid"
		}
		return plannererrors.ErrInternal.FastGen(errMsg)
	}
	// handle index join hints.
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return nil // no force index join hints
	}
	// Cannot find any valid index join plan with these force hints.
	// Print warning message if any hints cannot work.
	// If the required property is not empty, we will enforce it and try the hint again.
	// So we only need to generate warning message when the property is empty.
	//
	// but for warnings handle inside findBestTask here, even the not-empty prop
	// will be reset to get the planNeedEnforce plans, but the prop passed down here will
	// still be the same, so here we change the admission to both.
	if prop.IsSortItemEmpty() || inEnforce {
		var indexJoinTables, indexHashJoinTables, indexMergeJoinTables []h.HintedTable
		if p.HintInfo != nil {
			t := p.HintInfo.IndexJoin
			indexJoinTables, indexHashJoinTables, indexMergeJoinTables = t.INLJTables, t.INLHJTables, t.INLMJTables
		}
		var errMsg string
		switch {
		case p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner): // prefer index join
			errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable", h.Restore2JoinHint(h.HintINLJ, indexJoinTables), h.Restore2JoinHint(h.TiDBIndexNestedLoopJoin, indexJoinTables))
		case p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner): // prefer index hash join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLHJ, indexHashJoinTables))
		case p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner): // prefer index merge join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLMJ, indexMergeJoinTables))
		default:
			// only record warnings for index join hint not working now.
			return nil
		}
		// Append inapplicable reason.
		if len(p.EqualConditions) == 0 {
			errMsg += " without column equal ON condition"
		}
		// Generate warning message to client.
		return plannererrors.ErrInternal.FastGen(errMsg)
	}
	return nil
}

func applyLogicalHintVarEigen(lp base.LogicalPlan, state *enumerateState, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	return applyLogicalJoinHint(lp, pp) ||
		applyLogicalTopNAndLimitHint(lp, state, pp, childTasks) ||
		applyLogicalAggregationHint(lp, pp, childTasks)
}

// Get the most preferred and efficient one by hint and low-cost priority.
// since hint applicable plan may greater than 1, like inl_join can suit for:
// index_join, index_hash_join, index_merge_join, we should chase the most efficient
// one among them.
// applyLogicalJoinHint is used to handle logic hint/prefer/variable, which is not a strong guide for optimization phase.
// It is changed from handleForceIndexJoinHints to handle the preferred join hint among several valid physical plan choices.
// It will return true if the hint can be applied when saw a real physic plan successfully built and returned up from child.
// we cache the most preferred one among this valid and preferred physic plans. If there is no preferred physic applicable
// for the logic hint, we will return false and the optimizer will continue to return the normal low-cost one.
func applyLogicalJoinHint(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	return preferMergeJoin(lp, physicPlan) || preferIndexJoinFamily(lp, physicPlan) ||
		preferHashJoin(lp, physicPlan)
}

func applyLogicalAggregationHint(lp base.LogicalPlan, physicPlan base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	la, ok := lp.(*logicalop.LogicalAggregation)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	if la.HasDistinct() {
		// TODO: remove after the cost estimation of distinct pushdown is implemented.
		if la.SCtx().GetSessionVars().AllowDistinctAggPushDown {
			// when AllowDistinctAggPushDown is true, we will not consider root task type as before.
			if _, ok := childTasks[0].(*CopTask); ok {
				return true
			}
		} else {
			switch childTasks[0].(type) {
			case *RootTask:
				// If the distinct agg can't be allowed to push down, we will consider root task type.
				// which is try to get the same behavior as before like types := {RootTask} only.
				return true
			case *MppTask:
				// If the distinct agg can't be allowed to push down, we will consider mpp task type too --- RootTask vs MPPTask
				// which is try to get the same behavior as before like types := {RootTask} and appended {MPPTask}.
				return true
			default:
				return false
			}
		}
	} else if la.PreferAggToCop {
		// If the aggregation is preferred to be pushed down to coprocessor, we will prefer it.
		if _, ok := childTasks[0].(*CopTask); ok {
			return true
		}
	}
	return false
}

func applyLogicalTopNAndLimitHint(lp base.LogicalPlan, state *enumerateState, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	hintPrefer, meetThreshold := pushLimitOrTopNForcibly(lp, pp)
	if hintPrefer {
		// if there is a user hint control, try to get the copTask as the prior.
		// here we don't assert task itself, because when topN attach 2 cop task, it will become root type automatically.
		if _, ok := childTasks[0].(*CopTask); ok {
			return true
		}
		return false
	}
	if meetThreshold {
		// previously, we set meetThreshold for pruning root task type but mpp task type. so:
		// 1: when one copTask exists, we will ignore root task type.
		// 2: when one copTask exists, another copTask should be cost compared with.
		// 3: mppTask is always in the cbo comparing.
		// 4: when none copTask exists, we will consider rootTask vs mppTask.
		// the following check priority logic is compatible with former pushLimitOrTopNForcibly prop pruning logic.
		_, isTopN := pp.(*physicalop.PhysicalTopN)
		if isTopN {
			if state.topNCopExist {
				if _, ok := childTasks[0].(*RootTask); ok {
					return false
				}
				// peer cop task should compare the cost with each other.
				if _, ok := childTasks[0].(*CopTask); ok {
					return true
				}
			} else {
				if _, ok := childTasks[0].(*CopTask); ok {
					state.topNCopExist = true
					return true
				}
				// when we encounter rootTask type while still see no topNCopExist.
				// that means there is no copTask valid before, we will consider rootTask here.
				if _, ok := childTasks[0].(*RootTask); ok {
					return true
				}
			}
			if _, ok := childTasks[0].(*MppTask); ok {
				return true
			}
			// shouldn't be here
			return false
		}
		// limit case:
		if state.limitCopExist {
			if _, ok := childTasks[0].(*RootTask); ok {
				return false
			}
			// peer cop task should compare the cost with each other.
			if _, ok := childTasks[0].(*CopTask); ok {
				return true
			}
		} else {
			if _, ok := childTasks[0].(*CopTask); ok {
				state.limitCopExist = true
				return true
			}
			// when we encounter rootTask type while still see no limitCopExist.
			// that means there is no copTask valid before, we will consider rootTask here.
			if _, ok := childTasks[0].(*RootTask); ok {
				return true
			}
		}
		if _, ok := childTasks[0].(*MppTask); ok {
			return true
		}
	}
	return false
}

// hash join has two types:
// one is hash join type: normal hash join, shuffle join, broadcast join
// another is the build side hint type: prefer left as build side, prefer right as build side
// the first one is used to control the join type, the second one is used to control the build side of hash join.
// the priority is:
// once the join type is set, we should respect them first, not this type are all ignored.
// after we see all the joins under this type, then we only consider the build side hints satisfied or not.
//
// for the priority among the hash join types, we will respect the join fine-grained hints first, then the normal hash join type,
// that is, the priority is: shuffle join / broadcast join > normal hash join.
func preferHashJoin(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	forceLeftToBuild := ((p.PreferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.PreferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		// for build hint conflict, restore all of them
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	physicalHashJoin, ok := physicPlan.(*physicalop.PhysicalHashJoin)
	if !ok {
		return false
	}
	// If the hint is set, we should prefer MPP shuffle join.
	preferShuffle := (p.PreferJoinType & h.PreferShuffleJoin) > 0
	preferBCJ := (p.PreferJoinType & h.PreferBCJoin) > 0
	if preferShuffle {
		if physicalHashJoin.StoreTp == kv.TiFlash && physicalHashJoin.MppShuffleJoin {
			// first: respect the shuffle join hint.
			// BCJ build side hint are handled in the enumeration phase.
			return true
		}
		return false
	}
	if preferBCJ {
		if physicalHashJoin.StoreTp == kv.TiFlash && !physicalHashJoin.MppShuffleJoin {
			// first: respect the broadcast join hint.
			// BCJ build side hint are handled in the enumeration phase.
			return true
		}
		return false
	}
	// Respect the join type and join side hints.
	if p.PreferJoinType&h.PreferHashJoin > 0 {
		// first: normal hash join hint are set.
		if forceLeftToBuild || forceRightToBuild {
			// second: respect the join side if join side hints are set.
			return (forceRightToBuild && physicalHashJoin.InnerChildIdx == 1) ||
				(forceLeftToBuild && physicalHashJoin.InnerChildIdx == 0)
		}
		// second: no join side hints are set, respect the join type is enough.
		return true
	}
	// no hash join type hint is set, we only need to respect the hash join side hints.
	return (forceRightToBuild && physicalHashJoin.InnerChildIdx == 1) ||
		(forceLeftToBuild && physicalHashJoin.InnerChildIdx == 0)
}

func preferMergeJoin(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	_, ok = physicPlan.(*physicalop.PhysicalMergeJoin)
	return ok && p.PreferJoinType&h.PreferMergeJoin > 0
}

func preferIndexJoinFamily(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return false // no force index join hints
	}
	innerSide, joinMethod, ok := getIndexJoinSideAndMethod(physicPlan)
	if !ok {
		return false
	}
	if (p.PreferAny(h.PreferLeftAsINLJInner) && innerSide == joinLeft && joinMethod == indexJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLJInner) && innerSide == joinRight && joinMethod == indexJoinMethod) ||
		(p.PreferAny(h.PreferLeftAsINLHJInner) && innerSide == joinLeft && joinMethod == indexHashJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLHJInner) && innerSide == joinRight && joinMethod == indexHashJoinMethod) ||
		(p.PreferAny(h.PreferLeftAsINLMJInner) && innerSide == joinLeft && joinMethod == indexMergeJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLMJInner) && innerSide == joinRight && joinMethod == indexMergeJoinMethod) {
		// valid physic for the hint
		return true
	}
	return false
}

func getJoinChildStatsAndSchema(ge base.GroupExpression, p base.LogicalPlan) (stats0, stats1 *property.StatsInfo, schema0, schema1 *expression.Schema) {
	if ge != nil {
		g := ge.(*memo.GroupExpression)
		stats0, schema0 = g.Inputs[0].GetLogicalProperty().Stats, g.Inputs[0].GetLogicalProperty().Schema
		stats1, schema1 = g.Inputs[1].GetLogicalProperty().Stats, g.Inputs[1].GetLogicalProperty().Schema
	} else {
		stats1, schema1 = p.Children()[1].StatsInfo(), p.Children()[1].Schema()
		stats0, schema0 = p.Children()[0].StatsInfo(), p.Children()[0].Schema()
	}
	return
}

func exhaustPhysicalPlans4LogicalExpand(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	p := lp.(*logicalop.LogicalExpand)
	// under the mpp task type, if the sort item is not empty, refuse it, cause expanded data doesn't support any sort items.
	if !prop.IsSortItemEmpty() {
		// false, meaning we can add a sort enforcer.
		return nil, false, nil
	}
	// when TiDB Expand execution is introduced: we can deal with two kind of physical plans.
	// RootTaskType means expand should be run at TiDB node.
	//	(RootTaskType is the default option, we can also generate a mpp candidate for it)
	// MPPTaskType means expand should be run at TiFlash node.
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil, true, nil
	}
	// now Expand mode can only be executed on TiFlash node.
	// Upper layer shouldn't expect any mpp partition from an Expand operator.
	// todo: data output from Expand operator should keep the origin data mpp partition.
	if prop.TaskTp == property.MppTaskType && prop.MPPPartitionTp != property.AnyType {
		return nil, true, nil
	}
	var physicalExpands []base.PhysicalPlan
	// for property.RootTaskType and property.MppTaskType with no partition option, we can give an MPP Expand.
	// we just remove whether subtree can be pushed to tiFlash check, and left child handle itself.
	if p.SCtx().GetSessionVars().IsMPPAllowed() {
		mppProp := prop.CloneEssentialFields()
		mppProp.TaskTp = property.MppTaskType
		expand := physicalop.PhysicalExpand{
			GroupingSets:          p.RollupGroupingSets,
			LevelExprs:            p.LevelExprs,
			ExtraGroupingColNames: p.ExtraGroupingColNames,
		}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), mppProp)
		expand.SetSchema(p.Schema())
		physicalExpands = append(physicalExpands, expand)
		// when the MppTaskType is required, we can return the physical plan directly.
		if prop.TaskTp == property.MppTaskType {
			return physicalExpands, true, nil
		}
	}
	// for property.RootTaskType, we can give a TiDB Expand.
	{
		taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType, property.MppTaskType, property.RootTaskType}
		for _, taskType := range taskTypes {
			// require cop task type for children.F
			tidbProp := prop.CloneEssentialFields()
			tidbProp.TaskTp = taskType
			expand := physicalop.PhysicalExpand{
				GroupingSets:          p.RollupGroupingSets,
				LevelExprs:            p.LevelExprs,
				ExtraGroupingColNames: p.ExtraGroupingColNames,
			}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), tidbProp)
			expand.SetSchema(p.Schema())
			physicalExpands = append(physicalExpands, expand)
		}
	}
	return physicalExpands, true, nil
}

func pushLimitOrTopNForcibly(p base.LogicalPlan, pp base.PhysicalPlan) (preferPushDown bool, meetThreshold bool) {
	switch lp := p.(type) {
	case *logicalop.LogicalTopN:
		preferPushDown = lp.PreferLimitToCop
		if _, isPhysicalLimit := pp.(*physicalop.PhysicalLimit); isPhysicalLimit {
			// For query using orderby + limit, the physicalop can be PhysicalLimit
			// when its corresponding logicalop is LogicalTopn.
			// And for PhysicalLimit, it's always better to let it pushdown to tikv.
			meetThreshold = true
		} else {
			meetThreshold = lp.Count+lp.Offset <= uint64(lp.SCtx().GetSessionVars().LimitPushDownThreshold)
		}
	case *logicalop.LogicalLimit:
		preferPushDown = lp.PreferLimitToCop
		// Always push Limit down in this case since it has no side effect
		meetThreshold = true
	default:
		return
	}

	// we remove the child subTree check, each logical operator only focus on themselves.
	// for current level, they prefer a push-down copTask.
	return
}

func getPhysTopN(lt *logicalop.LogicalTopN, prop *property.PhysicalProperty) []base.PhysicalPlan {
	// topN should always generate rootTaskType for:
	// case1: after v7.5, since tiFlash Cop has been banned, mppTaskType may return invalid task when there are some root conditions.
	// case2: for index merge case which can only be run in root type, topN and limit can't be pushed to the inside index merge when it's an intersection.
	// note: don't change the task enumeration order here.
	allTaskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType, property.RootTaskType}
	// we move the pushLimitOrTopNForcibly check to attach2Task to do the prefer choice.
	mppAllowed := lt.SCtx().GetSessionVars().IsMPPAllowed()
	if mppAllowed {
		allTaskTypes = append(allTaskTypes, property.MppTaskType)
	}
	ret := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: math.MaxFloat64,
			CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
		topN := physicalop.PhysicalTopN{
			ByItems:     lt.ByItems,
			PartitionBy: lt.PartitionBy,
			Count:       lt.Count,
			Offset:      lt.Offset,
		}.Init(lt.SCtx(), lt.StatsInfo(), lt.QueryBlockOffset(), resultProp)
		topN.SetSchema(lt.Schema())
		ret = append(ret, topN)
	}
	// If we can generate MPP task and there's vector distance function in the order by column.
	// We will try to generate a property for possible vector indexes.
	if mppAllowed {
		if len(lt.ByItems) != 1 {
			return ret
		}
		vs := expression.InterpretVectorSearchExpr(lt.ByItems[0].Expr)
		if vs == nil {
			return ret
		}
		// Currently vector index only accept ascending order.
		if lt.ByItems[0].Desc {
			return ret
		}
		// Currently, we only deal with the case the TopN is directly above a DataSource.
		ds, ok := lt.Children()[0].(*logicalop.DataSource)
		if !ok {
			return ret
		}
		// Reject any filters.
		if len(ds.PushedDownConds) > 0 {
			return ret
		}
		resultProp := &property.PhysicalProperty{
			TaskTp:            property.MppTaskType,
			ExpectedCnt:       math.MaxFloat64,
			CTEProducerStatus: prop.CTEProducerStatus,
		}
		resultProp.VectorProp.VSInfo = vs
		resultProp.VectorProp.TopK = uint32(lt.Count + lt.Offset)
		topN := physicalop.PhysicalTopN{
			ByItems:     lt.ByItems,
			PartitionBy: lt.PartitionBy,
			Count:       lt.Count,
			Offset:      lt.Offset,
		}.Init(lt.SCtx(), lt.StatsInfo(), lt.QueryBlockOffset(), resultProp)
		topN.SetSchema(lt.Schema())
		ret = append(ret, topN)
	}
	return ret
}

func getPhysLimits(lt *logicalop.LogicalTopN, prop *property.PhysicalProperty) []base.PhysicalPlan {
	p, canPass := GetPropByOrderByItems(lt.ByItems)
	if !canPass {
		return nil
	}
	// note: don't change the task enumeration order here.
	allTaskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType, property.RootTaskType}
	ret := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(lt.Count + lt.Offset), SortItems: p.SortItems,
			CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
		limit := physicalop.PhysicalLimit{
			Count:       lt.Count,
			Offset:      lt.Offset,
			PartitionBy: lt.GetPartitionBy(),
		}.Init(lt.SCtx(), lt.StatsInfo(), lt.QueryBlockOffset(), resultProp)
		limit.SetSchema(lt.Schema())
		ret = append(ret, limit)
	}
	return ret
}

// MatchItems checks if this prop's columns can match by items totally.
func MatchItems(p *property.PhysicalProperty, items []*util.ByItems) bool {
	if len(items) < len(p.SortItems) {
		return false
	}
	for i, col := range p.SortItems {
		sortItem := items[i]
		if sortItem.Desc != col.Desc || !col.Col.EqualColumn(sortItem.Expr) {
			return false
		}
	}
	return true
}

// exhaustPhysicalPlans4LogicalApply generates the physical plan for a logical apply.
func exhaustPhysicalPlans4LogicalApply(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ge, la := base.GetGEAndLogical[*logicalop.LogicalApply](super)
	_, _, schema0, _ := getJoinChildStatsAndSchema(ge, la)
	if !prop.AllColsFromSchema(schema0) || prop.IsFlashProp() { // for convenient, we don't pass through any prop
		la.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `Apply` is not supported now.")
		return nil, true, nil
	}
	if !prop.IsSortItemEmpty() && la.SCtx().GetSessionVars().EnableParallelApply {
		la.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("Parallel Apply rejects the possible order properties of its outer child currently"))
		return nil, true, nil
	}
	join := physicalop.GetHashJoin(ge, la, prop)
	var columns = make([]*expression.Column, 0, len(la.CorCols))
	for _, colColumn := range la.CorCols {
		// fix the liner warning.
		tmp := colColumn
		columns = append(columns, &tmp.Column)
	}
	cacheHitRatio := 0.0
	if la.StatsInfo().RowCount != 0 {
		ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(la.SCtx(), columns, la.Schema(), la.StatsInfo())
		// for example, if there are 100 rows and the number of distinct values of these correlated columns
		// are 70, then we can assume 30 rows can hit the cache so the cache hit ratio is 1 - (70/100) = 0.3
		cacheHitRatio = 1 - (ndv / la.StatsInfo().RowCount)
	}

	var canUseCache bool
	if cacheHitRatio > 0.1 && la.SCtx().GetSessionVars().MemQuotaApplyCache > 0 {
		canUseCache = true
	} else {
		canUseCache = false
	}

	apply := physicalop.PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.CorCols,
		CanUseCache:      canUseCache,
	}.Init(la.SCtx(),
		la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt),
		la.QueryBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: true},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
	apply.SetSchema(la.Schema())
	return []base.PhysicalPlan{apply}, true, nil
}

func tryToGetMppWindows(lw *logicalop.LogicalWindow, prop *property.PhysicalProperty) []base.PhysicalPlan {
	if !prop.IsSortItemAllForPartition() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil
	}
	if prop.MPPPartitionTp == property.BroadcastType {
		return nil
	}

	{
		allSupported := true
		sctx := lw.SCtx()
		for _, windowFunc := range lw.WindowFuncDescs {
			if !windowFunc.CanPushDownToTiFlash(util.GetPushDownCtx(sctx)) {
				lw.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
					"MPP mode may be blocked because window function `" + windowFunc.Name + "` or its arguments are not supported now.")
				allSupported = false
			} else if !expression.IsPushDownEnabled(windowFunc.Name, kv.TiFlash) {
				lw.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because window function `" + windowFunc.Name + "` is blocked by blacklist, check `table mysql.expr_pushdown_blacklist;` for more information.")
				return nil
			}
		}
		if !allSupported {
			return nil
		}

		if lw.Frame != nil && lw.Frame.Type == ast.Ranges {
			ctx := lw.SCtx().GetExprCtx()
			if _, err := expression.ExpressionsToPBList(ctx.GetEvalCtx(), lw.Frame.Start.CalcFuncs, lw.SCtx().GetClient()); err != nil {
				lw.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
					"MPP mode may be blocked because window function frame can't be pushed down, because " + err.Error())
				return nil
			}
			if !expression.CanExprsPushDown(util.GetPushDownCtx(sctx), lw.Frame.Start.CalcFuncs, kv.TiFlash) {
				lw.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
					"MPP mode may be blocked because window function frame can't be pushed down")
				return nil
			}
			if _, err := expression.ExpressionsToPBList(ctx.GetEvalCtx(), lw.Frame.End.CalcFuncs, lw.SCtx().GetClient()); err != nil {
				lw.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
					"MPP mode may be blocked because window function frame can't be pushed down, because " + err.Error())
				return nil
			}
			if !expression.CanExprsPushDown(util.GetPushDownCtx(sctx), lw.Frame.End.CalcFuncs, kv.TiFlash) {
				lw.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
					"MPP mode may be blocked because window function frame can't be pushed down")
				return nil
			}

			if !lw.CheckComparisonForTiFlash(lw.Frame.Start) || !lw.CheckComparisonForTiFlash(lw.Frame.End) {
				lw.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
					"MPP mode may be blocked because window function frame can't be pushed down, because Duration vs Datetime is invalid comparison as TiFlash can't handle it so far.")
				return nil
			}
		}
	}

	var byItems []property.SortItem
	byItems = append(byItems, lw.PartitionBy...)
	byItems = append(byItems, lw.OrderBy...)
	childProperty := &property.PhysicalProperty{
		ExpectedCnt:           math.MaxFloat64,
		CanAddEnforcer:        true,
		SortItems:             byItems,
		TaskTp:                property.MppTaskType,
		SortItemsForPartition: byItems,
		CTEProducerStatus:     prop.CTEProducerStatus,
	}
	if !prop.IsPrefix(childProperty) {
		return nil
	}

	if len(lw.PartitionBy) > 0 {
		partitionCols := lw.GetPartitionKeys()
		// trying to match the required partitions.
		if prop.MPPPartitionTp == property.HashType {
			matches := prop.IsSubsetOf(partitionCols)
			if len(matches) == 0 {
				// do not satisfy the property of its parent, so return empty
				return nil
			}
			partitionCols = property.ChoosePartitionKeys(partitionCols, matches)
		}
		childProperty.MPPPartitionTp = property.HashType
		childProperty.MPPPartitionCols = partitionCols
	} else {
		childProperty.MPPPartitionTp = property.SinglePartitionType
	}

	if prop.MPPPartitionTp == property.SinglePartitionType && childProperty.MPPPartitionTp != property.SinglePartitionType {
		return nil
	}

	window := physicalop.PhysicalWindow{
		WindowFuncDescs: lw.WindowFuncDescs,
		PartitionBy:     lw.PartitionBy,
		OrderBy:         lw.OrderBy,
		Frame:           lw.Frame,
		StoreTp:         kv.TiFlash,
	}.Init(lw.SCtx(), lw.StatsInfo().ScaleByExpectCnt(lw.SCtx().GetSessionVars(), prop.ExpectedCnt), lw.QueryBlockOffset(), childProperty)
	window.SetSchema(lw.Schema())

	return []base.PhysicalPlan{window}
}

func exhaustPhysicalPlans4LogicalWindow(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	lw := lp.(*logicalop.LogicalWindow)
	windows := make([]base.PhysicalPlan, 0, 2)

	// we lift the p.CanPushToCop(tiFlash) check here.
	if lw.SCtx().GetSessionVars().IsMPPAllowed() {
		mppWindows := tryToGetMppWindows(lw, prop)
		windows = append(windows, mppWindows...)
	}

	// if there needs a mpp task, we don't generate tidb window function.
	if prop.TaskTp == property.MppTaskType {
		return windows, true, nil
	}
	var byItems []property.SortItem
	byItems = append(byItems, lw.PartitionBy...)
	byItems = append(byItems, lw.OrderBy...)
	childProperty := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, SortItems: byItems,
		CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	if !prop.IsPrefix(childProperty) {
		return nil, true, nil
	}
	window := physicalop.PhysicalWindow{
		WindowFuncDescs: lw.WindowFuncDescs,
		PartitionBy:     lw.PartitionBy,
		OrderBy:         lw.OrderBy,
		Frame:           lw.Frame,
	}.Init(lw.SCtx(), lw.StatsInfo().ScaleByExpectCnt(lw.SCtx().GetSessionVars(), prop.ExpectedCnt), lw.QueryBlockOffset(), childProperty)
	window.SetSchema(lw.Schema())

	windows = append(windows, window)
	return windows, true, nil
}

func exhaustPhysicalPlans4LogicalLock(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	p := lp.(*logicalop.LogicalLock)
	if prop.IsFlashProp() {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `Lock` is not supported now.")
		return nil, true, nil
	}
	childProp := prop.CloneEssentialFields()
	lock := physicalop.PhysicalLock{
		Lock:               p.Lock,
		TblID2Handle:       p.TblID2Handle,
		TblID2PhysTblIDCol: p.TblID2PhysTblIDCol,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), childProp)
	return []base.PhysicalPlan{lock}, true, nil
}

func exhaustPhysicalPlans4LogicalUnionAll(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	p := lp.(*logicalop.LogicalUnionAll)
	// TODO: UnionAll can not pass any order, but we can change it to sort merge to keep order.
	if !prop.IsSortItemEmpty() || (prop.IsFlashProp() && prop.TaskTp != property.MppTaskType) {
		return nil, true, nil
	}
	// TODO: UnionAll can pass partition info, but for briefness, we prevent it from pushing down.
	if prop.TaskTp == property.MppTaskType && prop.MPPPartitionTp != property.AnyType {
		return nil, true, nil
	}
	// when arrived here, operator itself has already checked checkOpSelfSatisfyPropTaskTypeRequirement, we only need to feel allowMPP here.
	canUseMpp := p.SCtx().GetSessionVars().IsMPPAllowed()
	chReqProps := make([]*property.PhysicalProperty, 0, p.ChildLen())
	for range p.Children() {
		if canUseMpp && prop.TaskTp == property.MppTaskType {
			chReqProps = append(chReqProps, &property.PhysicalProperty{
				ExpectedCnt:       prop.ExpectedCnt,
				TaskTp:            property.MppTaskType,
				CTEProducerStatus: prop.CTEProducerStatus,
				NoCopPushDown:     prop.NoCopPushDown,
			})
		} else {
			chReqProps = append(chReqProps, &property.PhysicalProperty{ExpectedCnt: prop.ExpectedCnt,
				CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
		}
	}
	ua := physicalop.PhysicalUnionAll{
		Mpp: canUseMpp && prop.TaskTp == property.MppTaskType,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
	ua.SetSchema(p.Schema())
	if canUseMpp && prop.TaskTp == property.RootTaskType {
		chReqProps = make([]*property.PhysicalProperty, 0, p.ChildLen())
		for range p.Children() {
			chReqProps = append(chReqProps, &property.PhysicalProperty{
				ExpectedCnt:       prop.ExpectedCnt,
				TaskTp:            property.MppTaskType,
				CTEProducerStatus: prop.CTEProducerStatus,
				NoCopPushDown:     prop.NoCopPushDown,
			})
		}
		mppUA := physicalop.PhysicalUnionAll{Mpp: true}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
		mppUA.SetSchema(p.Schema())
		return []base.PhysicalPlan{ua, mppUA}, true, nil
	}
	return []base.PhysicalPlan{ua}, true, nil
}

func exhaustPhysicalPlans4LogicalPartitionUnionAll(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	p := lp.(*logicalop.LogicalPartitionUnionAll)
	uas, flagHint, err := p.LogicalUnionAll.ExhaustPhysicalPlans(prop)
	if err != nil {
		return nil, false, err
	}
	for _, ua := range uas {
		ua.(*physicalop.PhysicalUnionAll).SetTP(plancodec.TypePartitionUnion)
	}
	return uas, flagHint, nil
}

func exhaustPhysicalPlans4LogicalTopN(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	lt := lp.(*logicalop.LogicalTopN)
	if MatchItems(prop, lt.ByItems) {
		return append(getPhysTopN(lt, prop), getPhysLimits(lt, prop)...), true, nil
	}
	return nil, true, nil
}

func exhaustPhysicalPlans4LogicalSort(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ls := lp.(*logicalop.LogicalSort)
	switch prop.TaskTp {
	case property.RootTaskType:
		if MatchItems(prop, ls.ByItems) {
			ret := make([]base.PhysicalPlan, 0, 2)
			ret = append(ret, getPhysicalSort(ls, prop))
			ns := getNominalSort(ls, prop)
			if ns != nil {
				ret = append(ret, ns)
			}
			return ret, true, nil
		}
	case property.MppTaskType:
		// just enumerate mpp task type requirement for child.
		ps := getNominalSortSimple(ls, prop)
		if ps != nil {
			return []base.PhysicalPlan{ps}, true, nil
		}
	default:
		return nil, true, nil
	}
	return nil, true, nil
}

func getPhysicalSort(ls *logicalop.LogicalSort, prop *property.PhysicalProperty) base.PhysicalPlan {
	ps := physicalop.PhysicalSort{ByItems: ls.ByItems}.Init(ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(ls.SCtx().GetSessionVars(), prop.ExpectedCnt), ls.QueryBlockOffset(),
		&property.PhysicalProperty{TaskTp: prop.TaskTp, ExpectedCnt: math.MaxFloat64,
			CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
	return ps
}

func getNominalSort(ls *logicalop.LogicalSort, reqProp *property.PhysicalProperty) *physicalop.NominalSort {
	prop, canPass, onlyColumn := GetPropByOrderByItemsContainScalarFunc(ls.ByItems)
	if !canPass {
		return nil
	}
	prop.ExpectedCnt = reqProp.ExpectedCnt
	prop.NoCopPushDown = reqProp.NoCopPushDown
	ps := physicalop.NominalSort{OnlyColumn: onlyColumn, ByItems: ls.ByItems}.Init(
		ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(ls.SCtx().GetSessionVars(), prop.ExpectedCnt), ls.QueryBlockOffset(), prop)
	return ps
}

func getNominalSortSimple(ls *logicalop.LogicalSort, reqProp *property.PhysicalProperty) *physicalop.NominalSort {
	prop, canPass, onlyColumn := GetPropByOrderByItemsContainScalarFunc(ls.ByItems)
	if !canPass || !onlyColumn {
		return nil
	}
	newProp := reqProp.CloneEssentialFields()
	newProp.SortItems = prop.SortItems
	ps := physicalop.NominalSort{OnlyColumn: true, ByItems: ls.ByItems}.Init(
		ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(ls.SCtx().GetSessionVars(), reqProp.ExpectedCnt), ls.QueryBlockOffset(), newProp)
	return ps
}

func exhaustPhysicalPlans4LogicalMaxOneRow(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	p := lp.(*logicalop.LogicalMaxOneRow)
	if !prop.IsSortItemEmpty() || prop.IsFlashProp() {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because operator `MaxOneRow` is not supported now.")
		return nil, true, nil
	}
	mor := physicalop.PhysicalMaxOneRow{}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), &property.PhysicalProperty{ExpectedCnt: 2, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
	return []base.PhysicalPlan{mor}, true, nil
}

func exhaustPhysicalPlans4LogicalSequence(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	g, ls := base.GetGEAndLogical[*logicalop.LogicalSequence](super)
	possibleChildrenProps := make([][]*property.PhysicalProperty, 0, 2)
	anyType := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.AnyType, CanAddEnforcer: true,
		CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	if prop.TaskTp == property.MppTaskType {
		if prop.CTEProducerStatus == property.SomeCTEFailedMpp {
			return nil, true, nil
		}
		anyType.CTEProducerStatus = property.AllCTECanMpp
		possibleChildrenProps = append(possibleChildrenProps, []*property.PhysicalProperty{anyType, prop.CloneEssentialFields()})
	} else {
		copied := prop.CloneEssentialFields()
		copied.CTEProducerStatus = property.SomeCTEFailedMpp
		possibleChildrenProps = append(possibleChildrenProps, []*property.PhysicalProperty{{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64, CTEProducerStatus: property.SomeCTEFailedMpp}, copied})
	}

	if prop.TaskTp != property.MppTaskType && prop.CTEProducerStatus != property.SomeCTEFailedMpp &&
		ls.SCtx().GetSessionVars().IsMPPAllowed() && prop.IsSortItemEmpty() {
		possibleChildrenProps = append(possibleChildrenProps, []*property.PhysicalProperty{anyType, anyType.CloneEssentialFields()})
	}
	var seqSchema *expression.Schema
	if g != nil {
		ge := g.(*memo.GroupExpression)
		seqSchema = ge.Inputs[len(ge.Inputs)-1].GetLogicalProperty().Schema
	} else {
		seqSchema = ls.Children()[ls.ChildLen()-1].Schema()
	}
	seqs := make([]base.PhysicalPlan, 0, len(possibleChildrenProps))
	for _, propChoice := range possibleChildrenProps {
		childReqs := make([]*property.PhysicalProperty, 0, ls.ChildLen())
		for range ls.ChildLen() - 1 {
			childReqs = append(childReqs, propChoice[0].CloneEssentialFields())
		}
		childReqs = append(childReqs, propChoice[1])
		seq := physicalop.PhysicalSequence{}.Init(ls.SCtx(), ls.StatsInfo(), ls.QueryBlockOffset(), childReqs...)
		seq.SetSchema(seqSchema)
		seqs = append(seqs, seq)
	}
	return seqs, true, nil
}
