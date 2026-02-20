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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/joinversion"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// exhaustPhysicalPlans generates all possible plans that can match the required property.
// It will return:
// 1. All possible plans that can match the required property.
// 2. Whether the SQL hint can work. Return true if there is no hint.
func exhaustPhysicalPlans(lp base.LogicalPlan, prop *property.PhysicalProperty) (physicalPlans [][]base.PhysicalPlan, hintCanWork bool, err error) {
	var ops []base.PhysicalPlan

	switch x := lp.(type) {
	case *logicalop.LogicalCTE:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalCTE(x, prop)
	case *logicalop.LogicalSort:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalSort(x, prop)
	case *logicalop.LogicalTopN:
		// ExhaustPhysicalPlans4LogicalTopN return PhysicalLimit and PhysicalTopN in different slice.
		// So we can always choose limit plan with pushdown when comparing with a limit plan without pushdown directly,
		// and choose a better plan by checking their cost when comparing a limit plan and a topn plan.
		return physicalop.ExhaustPhysicalPlans4LogicalTopN(x, prop)
	case *logicalop.LogicalLock:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalLock(x, prop)
	case *logicalop.LogicalJoin:
		ops, hintCanWork, err = exhaustPhysicalPlans4LogicalJoin(x, prop)
	case *logicalop.LogicalApply:
		ops, hintCanWork, err = exhaustPhysicalPlans4LogicalApply(x, prop)
	case *logicalop.LogicalLimit:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalLimit(x, prop)
	case *logicalop.LogicalWindow:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalWindow(x, prop)
	case *logicalop.LogicalExpand:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalExpand(x, prop)
	case *logicalop.LogicalUnionAll:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalUnionAll(x, prop)
	case *logicalop.LogicalSequence:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalSequence(x, prop)
	case *logicalop.LogicalSelection:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalSelection(x, prop)
	case *logicalop.LogicalMaxOneRow:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalMaxOneRow(x, prop)
	case *logicalop.LogicalUnionScan:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalUnionScan(x, prop)
	case *logicalop.LogicalProjection:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalProjection(x, prop)
	case *logicalop.LogicalAggregation:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalAggregation(x, prop)
	case *logicalop.LogicalPartitionUnionAll:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalPartitionUnionAll(x, prop)
	case *memo.GroupExpression:
		return memo.ExhaustPhysicalPlans4GroupExpression(x, prop)
	case *mockLogicalPlan4Test:
		ops, hintCanWork, err = ExhaustPhysicalPlans4MockLogicalPlan(x, prop)
	default:
		return nil, false, errors.Errorf("unknown logical plan type %T", lp)
	}

	if len(ops) == 0 || err != nil {
		return nil, hintCanWork, err
	}
	return [][]base.PhysicalPlan{ops}, hintCanWork, nil
}

func getHashJoins(super base.LogicalPlan, prop *property.PhysicalProperty) (joins []base.PhysicalPlan, forced bool) {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	if !prop.IsSortItemEmpty() { // hash join doesn't promise any orders
		return
	}

	forceLeftToBuild := ((p.PreferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.PreferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Conflicting HASH_JOIN_BUILD and HASH_JOIN_PROBE hints detected. " +
			"Both sides cannot be specified to use the same table. Please review the hints")
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	joins = make([]base.PhysicalPlan, 0, 2)
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin:
		leftJoinKeys, _, isNullEQ, _ := p.GetJoinKeys()
		leftNAJoinKeys, _ := p.GetNAJoinKeys()
		if p.SCtx().GetSessionVars().UseHashJoinV2 && joinversion.IsHashJoinV2Supported() && physicalop.CanUseHashJoinV2(p.JoinType, leftJoinKeys, isNullEQ, leftNAJoinKeys) {
			if !forceLeftToBuild {
				joins = append(joins, getHashJoin(ge, p, prop, 1, false))
			}
			if !forceRightToBuild {
				joins = append(joins, getHashJoin(ge, p, prop, 1, true))
			}
		} else {
			joins = append(joins, getHashJoin(ge, p, prop, 1, false))
			if forceLeftToBuild || forceRightToBuild {
				p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(fmt.Sprintf(
					"The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for %s with hash join version 1. "+
						"Please remove these hints",
					p.JoinType))
				forceLeftToBuild = false
				forceRightToBuild = false
			}
		}
	case base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		joins = append(joins, getHashJoin(ge, p, prop, 1, false))
		if forceLeftToBuild || forceRightToBuild {
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(fmt.Sprintf(
				"HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for %s because the build side is fixed. "+
					"Please remove these hints",
				p.JoinType))
			forceLeftToBuild = false
			forceRightToBuild = false
		}
	case base.LeftOuterJoin:
		if !forceLeftToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 1, false))
		}
		if !forceRightToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 1, true))
		}
	case base.RightOuterJoin:
		if !forceLeftToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 0, true))
		}
		if !forceRightToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 0, false))
		}
	case base.InnerJoin:
		if forceLeftToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 0, false))
		} else if forceRightToBuild {
			joins = append(joins, getHashJoin(ge, p, prop, 1, false))
		} else {
			joins = append(joins, getHashJoin(ge, p, prop, 1, false))
			joins = append(joins, getHashJoin(ge, p, prop, 0, false))
		}
	}

	forced = (p.PreferJoinType&h.PreferHashJoin > 0) || forceLeftToBuild || forceRightToBuild
	shouldSkipHashJoin := physicalop.ShouldSkipHashJoin(p)
	if !forced && shouldSkipHashJoin {
		return nil, false
	} else if forced && shouldSkipHashJoin {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"A conflict between the HASH_JOIN hint and the NO_HASH_JOIN hint, " +
				"or the tidb_opt_enable_hash_join system variable, the HASH_JOIN hint will take precedence.")
	}
	return
}

func getHashJoin(ge base.GroupExpression, p *logicalop.LogicalJoin, prop *property.PhysicalProperty, innerIdx int, useOuterToBuild bool) *physicalop.PhysicalHashJoin {
	var stats0, stats1 *property.StatsInfo
	if ge != nil {
		stats0, stats1, _, _ = ge.GetJoinChildStatsAndSchema()
	} else {
		stats0, stats1, _, _ = p.GetJoinChildStatsAndSchema()
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	chReqProps[1-innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	var outerStats *property.StatsInfo
	if 1-innerIdx == 0 {
		outerStats = stats0
	} else {
		outerStats = stats1
	}
	if prop.ExpectedCnt < p.StatsInfo().RowCount {
		expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
		chReqProps[1-innerIdx].ExpectedCnt = outerStats.RowCount * expCntScale
	}
	hashJoin := physicalop.NewPhysicalHashJoin(p, innerIdx, useOuterToBuild, p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(p.Schema())
	return hashJoin
}

// constructIndexHashJoinStatic is used to enumerate current a physical index hash join with undecided inner plan. Via index join prop
// pushed down to the inner side, the inner plans will check the admission of valid indexJoinProp and enumerate admitted inner
// operator. This function is quite similar with constructIndexJoinStatic.
func constructIndexHashJoinStatic(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	outerIdx int,
	indexJoinProp *property.IndexJoinRuntimeProp,
	outerStats *property.StatsInfo,
) []base.PhysicalPlan {
	// new one index join with the same index join prop pushed down.
	indexJoins := constructIndexJoinStatic(p, prop, outerIdx, indexJoinProp, outerStats)
	indexHashJoins := make([]base.PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*physicalop.PhysicalIndexJoin)
		indexHashJoin := physicalop.PhysicalIndexHashJoin{
			PhysicalIndexJoin: *join,
			// Prop is empty means that the parent operator does not need the
			// join operator to provide any promise of the output order.
			KeepOuterOrder: !prop.IsSortItemEmpty(),
		}.Init(p.SCtx())
		indexHashJoins = append(indexHashJoins, indexHashJoin)
	}
	return indexHashJoins
}

// constructIndexJoinStatic is used to enumerate current a physical index join with undecided inner plan. Via index join prop
// pushed down to the inner side, the inner plans will check the admission of valid indexJoinProp and enumerate admitted inner
// operator. This function is quite similar with constructIndexJoin. While differing in following part:
//
// Since constructIndexJoin will fill the physicalIndexJoin some runtime detail even for adjusting the keys, hash-keys, move
// eq condition into other conditions because the underlying ds couldn't use it or something. This is because previously the
// index join enumeration can see the complete index chosen result after inner task is built. But for the refactored one, the
// enumerated physical index here can only see the info it owns. That's why we call the function constructIndexJoinStatic.
//
// The indexJoinProp is passed down to the inner side, which contains the runtime constant inner key, which is used to build the
// underlying index/pk range. When the inner side is built bottom up, it will return the indexJoinInfo, which contains the runtime
// information that this physical index join wants. That's introduce second function called completePhysicalIndexJoin, which will
// fill physicalIndexJoin about all the runtime information it lacks in static enumeration phase.
func constructIndexJoinStatic(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	outerIdx int,
	indexJoinProp *property.IndexJoinRuntimeProp,
	outerStats *property.StatsInfo,
) []base.PhysicalPlan {
	joinType := p.JoinType
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
		isNullEQ      []bool
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, isNullEQ, _ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, isNullEQ, _ = p.GetJoinKeys()
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	// outer side expected cnt will be amplified by the prop.ExpectedCnt / p.StatsInfo().RowCount with same ratio.
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64,
		SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	orderRatio := p.SCtx().GetSessionVars().OptOrderingIdxSelRatio
	// Record the variable usage for explain explore.
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptOrderingIdxSelRatio)
	outerRowCount := outerStats.RowCount
	estimatedRowCount := p.StatsInfo().RowCount
	if (prop.ExpectedCnt < estimatedRowCount) ||
		(orderRatio > 0 && outerRowCount > estimatedRowCount && prop.ExpectedCnt < outerRowCount && estimatedRowCount > 0) {
		// Apply the orderRatio to recognize that a large outer table scan may
		// read additional rows before the inner table reaches the limit values
		rowsToMeetFirst := max(0.0, (outerRowCount-estimatedRowCount)*orderRatio)
		expCntScale := prop.ExpectedCnt / estimatedRowCount
		expectedCnt := (outerRowCount * expCntScale) + rowsToMeetFirst
		chReqProps[outerIdx].ExpectedCnt = expectedCnt
	}

	// inner side should pass down the indexJoinProp, which contains the runtime constant inner key, which is used to build the underlying index/pk range.
	chReqProps[1-outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64,
		CTEProducerStatus: prop.CTEProducerStatus, IndexJoinProp: indexJoinProp, NoCopPushDown: prop.NoCopPushDown}

	// for old logic from constructIndexJoin like
	// 1. feeling the keyOff2IdxOffs' -1 and refill the eq condition back to other conditions and adjust inner or outer keys, we
	// move it to completeIndexJoin because it requires the indexJoinInfo which is generated by underlying ds and passed bottom-up
	// within the Task to be filled.
	// 2. extract the eq condition from new other conditions to build the hash join keys, this kind of eq can be used by hash key
	// mapping, we move it to completePhysicalIndexJoin because it requires the indexJoinInfo which is generated by underlying ds
	// and passed bottom-up within the Task to be filled as well.

	baseJoin := physicalop.BasePhysicalJoin{
		InnerChildIdx:   1 - outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		// for static enumeration here, we just pass down the original other conditions
		OtherConditions: p.OtherConditions,
		JoinType:        joinType,
		// for static enumeration here, we just pass down the original outerJoinKeys, innerJoinKeys, isNullEQ
		OuterJoinKeys: outerJoinKeys,
		InnerJoinKeys: innerJoinKeys,
		IsNullEQ:      isNullEQ,
		DefaultValues: p.DefaultValues,
	}

	join := physicalop.PhysicalIndexJoin{
		BasePhysicalJoin: baseJoin,
		// for static enumeration here, we don't need to fill inner plan anymore.
		// for static enumeration here, the KeyOff2IdxOff, Ranges, CompareFilters, OuterHashKeys, InnerHashKeys are
		// waiting for attach2Task's complement after see the inner plan's indexJoinInfo returned by underlying ds.
		//
		// for static enumeration here, we just pass down the original equal condition for condition adjustment rather
		// depend on the original logical join node.
		EqualConditions: p.EqualConditions,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
	join.SetSchema(p.Schema())
	return []base.PhysicalPlan{join}
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
func completePhysicalIndexJoin(physic *physicalop.PhysicalIndexJoin, rt *physicalop.RootTask, innerS, outerS *expression.Schema, extractOtherEQ bool) base.PhysicalPlan {
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
				lhs, rhs, ok := expression.IsColOpCol(c)
				if ok {
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

// When inner plan is TableReader, the parameter `ranges` will be nil. Because pk only have one column. So all of its range
// is generated during execution time.
func constructIndexJoin(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask base.Task,
	ranges ranger.MutableRanges,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *physicalop.ColWithCmpFuncManager,
	extractOtherEQ bool,
) []base.PhysicalPlan {
	if innerTask.Invalid() {
		return nil
	}
	if ranges == nil {
		ranges = ranger.Ranges{} // empty range
	}

	joinType := p.JoinType
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
		isNullEQ      []bool
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, isNullEQ, _ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, isNullEQ, _ = p.GetJoinKeys()
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64,
		SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
	if prop.ExpectedCnt < p.StatsInfo().RowCount {
		expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
		chReqProps[outerIdx].ExpectedCnt = p.Children()[outerIdx].StatsInfo().RowCount * expCntScale
	}
	newInnerKeys := make([]*expression.Column, 0, len(innerJoinKeys))
	newOuterKeys := make([]*expression.Column, 0, len(outerJoinKeys))
	newIsNullEQ := make([]bool, 0, len(isNullEQ))
	newKeyOff := make([]int, 0, len(keyOff2IdxOff))
	newOtherConds := make([]expression.Expression, len(p.OtherConditions), len(p.OtherConditions)+len(p.EqualConditions))
	copy(newOtherConds, p.OtherConditions)
	for keyOff, idxOff := range keyOff2IdxOff {
		if keyOff2IdxOff[keyOff] < 0 {
			newOtherConds = append(newOtherConds, p.EqualConditions[keyOff])
			continue
		}
		newInnerKeys = append(newInnerKeys, innerJoinKeys[keyOff])
		newOuterKeys = append(newOuterKeys, outerJoinKeys[keyOff])
		newIsNullEQ = append(newIsNullEQ, isNullEQ[keyOff])
		newKeyOff = append(newKeyOff, idxOff)
	}

	var outerHashKeys, innerHashKeys []*expression.Column
	outerHashKeys, innerHashKeys = make([]*expression.Column, len(newOuterKeys)), make([]*expression.Column, len(newInnerKeys))
	copy(outerHashKeys, newOuterKeys)
	copy(innerHashKeys, newInnerKeys)
	// we can use the `col <eq> col` in `OtherCondition` to build the hashtable to avoid the unnecessary calculating.
	for i := len(newOtherConds) - 1; extractOtherEQ && i >= 0; i = i - 1 {
		switch c := newOtherConds[i].(type) {
		case *expression.ScalarFunction:
			if c.FuncName.L == ast.EQ {
				lhs, rhs, ok := expression.IsColOpCol(c)
				if ok {
					if lhs.InOperand || rhs.InOperand {
						// if this other-cond is from a `[not] in` sub-query, do not convert it into eq-cond since
						// IndexJoin cannot deal with NULL correctly in this case; please see #25799 for more details.
						continue
					}
					outerSchema, innerSchema := p.Children()[outerIdx].Schema(), p.Children()[1-outerIdx].Schema()
					if outerSchema.Contains(lhs) && innerSchema.Contains(rhs) {
						outerHashKeys = append(outerHashKeys, lhs) // nozero
						innerHashKeys = append(innerHashKeys, rhs) // nozero
					} else if innerSchema.Contains(lhs) && outerSchema.Contains(rhs) {
						outerHashKeys = append(outerHashKeys, rhs) // nozero
						innerHashKeys = append(innerHashKeys, lhs) // nozero
					}
					newOtherConds = slices.Delete(newOtherConds, i, i+1)
				}
			}
		default:
			continue
		}
	}

	baseJoin := physicalop.BasePhysicalJoin{
		InnerChildIdx:   1 - outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: newOtherConds,
		JoinType:        joinType,
		OuterJoinKeys:   newOuterKeys,
		InnerJoinKeys:   newInnerKeys,
		IsNullEQ:        newIsNullEQ,
		DefaultValues:   p.DefaultValues,
	}

	join := physicalop.PhysicalIndexJoin{
		BasePhysicalJoin: baseJoin,
		InnerPlan:        innerTask.Plan(),
		KeyOff2IdxOff:    newKeyOff,
		Ranges:           ranges,
		CompareFilters:   compareFilters,
		OuterHashKeys:    outerHashKeys,
		InnerHashKeys:    innerHashKeys,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
	if path != nil {
		join.IdxColLens = path.IdxColLens
	}
	join.SetSchema(p.Schema())
	return []base.PhysicalPlan{join}
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
	_, _, _, hasNullEQ := p.GetJoinKeys()
	if hasNullEQ {
		return nil
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

func constructIndexHashJoin(
	p *logicalop.LogicalJoin,
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask base.Task,
	ranges ranger.MutableRanges,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *physicalop.ColWithCmpFuncManager,
) []base.PhysicalPlan {
	indexJoins := constructIndexJoin(p, prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, compareFilters, true)
	indexHashJoins := make([]base.PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*physicalop.PhysicalIndexJoin)
		indexHashJoin := physicalop.PhysicalIndexHashJoin{
			PhysicalIndexJoin: *join,
			// Prop is empty means that the parent operator does not need the
			// join operator to provide any promise of the output order.
			KeepOuterOrder: !prop.IsSortItemEmpty(),
		}.Init(p.SCtx())
		indexHashJoins = append(indexHashJoins, indexHashJoin)
	}
	return indexHashJoins
}

// enumerateIndexJoinByOuterIdx will enumerate temporary index joins by index join prop required for its inner child.
func enumerateIndexJoinByOuterIdx(super base.LogicalPlan, prop *property.PhysicalProperty, outerIdx int) (joins []base.PhysicalPlan) {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	stats0, stats1, schema0, schema1 := getJoinChildStatsAndSchema(ge, p)
	var outerSchema *expression.Schema
	var outerStats *property.StatsInfo
	if outerIdx == 0 {
		outerSchema = schema0
		outerStats = stats0
	} else {
		outerSchema = schema1
		outerStats = stats1
	}
	// need same order
	all, _ := prop.AllSameOrder()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.AllColsFromSchema(outerSchema) || !all {
		return nil
	}
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, _, _ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, _, _ = p.GetJoinKeys()
	}
	// computed the avgInnerRowCnt
	var avgInnerRowCnt float64
	if count := outerStats.RowCount; count > 0 {
		avgInnerRowCnt = p.EqualCondOutCnt / count
	}
	// for pk path
	indexJoinPropTS := &property.IndexJoinRuntimeProp{
		OtherConditions: p.OtherConditions,
		InnerJoinKeys:   innerJoinKeys,
		OuterJoinKeys:   outerJoinKeys,
		AvgInnerRowCnt:  avgInnerRowCnt,
		TableRangeScan:  true,
	}
	// for normal index path
	indexJoinPropIS := &property.IndexJoinRuntimeProp{
		OtherConditions: p.OtherConditions,
		InnerJoinKeys:   innerJoinKeys,
		OuterJoinKeys:   outerJoinKeys,
		AvgInnerRowCnt:  avgInnerRowCnt,
		TableRangeScan:  false,
	}
	indexJoins := constructIndexJoinStatic(p, prop, outerIdx, indexJoinPropTS, outerStats)
	indexJoins = append(indexJoins, constructIndexJoinStatic(p, prop, outerIdx, indexJoinPropIS, outerStats)...)
	indexJoins = append(indexJoins, constructIndexHashJoinStatic(p, prop, outerIdx, indexJoinPropTS, outerStats)...)
	indexJoins = append(indexJoins, constructIndexHashJoinStatic(p, prop, outerIdx, indexJoinPropIS, outerStats)...)
	return indexJoins
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child.
// First of all, we'll check whether the inner child is DataSource.
// Then, we will extract the join keys of p's equal conditions. Then check whether all of them are just the primary key
// or match some part of on index. If so we will choose the best one and construct a index join.
func getIndexJoinByOuterIdx(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, outerIdx int) (joins []base.PhysicalPlan) {
	outerChild, innerChild := p.Children()[outerIdx], p.Children()[1-outerIdx]
	all, _ := prop.AllSameOrder()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.AllColsFromSchema(outerChild.Schema()) || !all {
		return nil
	}
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, _, _ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, _, _ = p.GetJoinKeys()
	}
	innerChildWrapper := extractIndexJoinInnerChildPattern(p, innerChild)
	if innerChildWrapper == nil {
		return nil
	}

	var avgInnerRowCnt float64
	if outerChild.StatsInfo().RowCount > 0 {
		avgInnerRowCnt = p.EqualCondOutCnt / outerChild.StatsInfo().RowCount
	}
	joins = buildIndexJoinInner2TableScan(p, prop, innerChildWrapper, innerJoinKeys, outerJoinKeys, outerIdx, avgInnerRowCnt)
	if joins != nil {
		return
	}
	return buildIndexJoinInner2IndexScan(p, prop, innerChildWrapper, innerJoinKeys, outerJoinKeys, outerIdx, avgInnerRowCnt)
}

// indexJoinInnerChildWrapper is a wrapper for the inner child of an index join.
// It contains the lowest DataSource operator and other inner child operator
// which is flattened into a list structure from tree structure .
// For example, the inner child of an index join is a tree structure like:
//
//	Projection
//	       Aggregation
//				Selection
//					DataSource
//
// The inner child wrapper will be:
// DataSource: the lowest DataSource operator.
// hasDitryWrite: whether the inner child contains dirty data.
// zippedChildren: [Projection, Aggregation, Selection]
type indexJoinInnerChildWrapper struct {
	ds             *logicalop.DataSource
	hasDitryWrite  bool
	zippedChildren []base.LogicalPlan
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

// checkIndexJoinInnerTaskWithAgg checks if join key set is subset of group by items.
// Otherwise the aggregation group might be split into multiple groups by the join keys, which generate incorrect result.
// Current limitation:
// This check currently relies on UniqueID matching between:
// 1) columns extracted from GroupByItems, and
// 2) columns from DataSource that are used as inner join keys.
// It works for plain GROUP BY columns, but it is conservative for GROUP BY expressions or
// columns introduced/re-mapped by intermediate operators (for example, GROUP BY c1+c2).
// In those cases, semantically equivalent keys may carry different UniqueIDs, so we may
// reject some valid index join plans (false negatives) to keep correctness.
// TODO: use FunctionDependency/equivalence reasoning to replace pure UniqueID subset matching.
func checkIndexJoinInnerTaskWithAgg(la *logicalop.LogicalAggregation, indexJoinProp *property.IndexJoinRuntimeProp) bool {
	groupByCols := expression.ExtractColumnsMapFromExpressions(nil, la.GroupByItems...)

	var dataSourceSchema *expression.Schema
	var iterChild base.LogicalPlan = la
	for iterChild != nil {
		if ds, ok := iterChild.(*logicalop.DataSource); ok {
			dataSourceSchema = ds.Schema()
			break
		}
		if iterChild.Children() == nil || len(iterChild.Children()) != 1 {
			return false
		}
		iterChild = iterChild.Children()[0]
	}
	if dataSourceSchema == nil {
		return false
	}

	// Only check the inner keys that is from the DataSource, and newly generated keys like agg func or projection column
	// will not be considerted here. Because we only need to make sure the keys from DataSource is not split by group by,
	// and the newly generated keys will not cause the split.
	innerKeysFromDataSource := make(map[int64]struct{}, len(indexJoinProp.InnerJoinKeys))
	for _, key := range indexJoinProp.InnerJoinKeys {
		if expression.ExprFromSchema(key, dataSourceSchema) {
			innerKeysFromDataSource[key.UniqueID] = struct{}{}
		}
	}
	if len(innerKeysFromDataSource) > len(groupByCols) {
		return false
	}
	for keyColID := range innerKeysFromDataSource {
		if _, ok := groupByCols[keyColID]; !ok {
			return false
		}
	}
	return true
}

// admitIndexJoinInnerChildPattern is used to check whether current physical choosing is under an index join's
// probe side. If it is, and we ganna check the original inner pattern check here to keep compatible with the old.
// the @first bool indicate whether current logical plan is valid of index join inner side.
func admitIndexJoinInnerChildPattern(p base.LogicalPlan, indexJoinProp *property.IndexJoinRuntimeProp) bool {
	switch x := p.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan).Self().(type) {
	case *logicalop.DataSource:
		// DS that prefer tiFlash reading couldn't walk into index join.
		if x.PreferStoreType&h.PreferTiFlash != 0 {
			return false
		}
	case *logicalop.LogicalProjection, *logicalop.LogicalSelection:
		if !p.SCtx().GetSessionVars().EnableINLJoinInnerMultiPattern {
			return false
		}
	case *logicalop.LogicalAggregation:
		if !p.SCtx().GetSessionVars().EnableINLJoinInnerMultiPattern {
			return false
		}
		if !checkIndexJoinInnerTaskWithAgg(x, indexJoinProp) {
			return false
		}

	case *logicalop.LogicalUnionScan:
	default: // index join inner side couldn't allow join, sort, limit, etc. todo: open it.
		return false
	}
	return true
}

func extractIndexJoinInnerChildPattern(p *logicalop.LogicalJoin, innerChild base.LogicalPlan) *indexJoinInnerChildWrapper {
	wrapper := &indexJoinInnerChildWrapper{}
	nextChild := func(pp base.LogicalPlan) base.LogicalPlan {
		if len(pp.Children()) != 1 {
			return nil
		}
		return pp.Children()[0]
	}
childLoop:
	for curChild := innerChild; curChild != nil; curChild = nextChild(curChild) {
		switch child := curChild.(type) {
		case *logicalop.DataSource:
			wrapper.ds = child
			break childLoop
		case *logicalop.LogicalProjection, *logicalop.LogicalSelection, *logicalop.LogicalAggregation:
			if !p.SCtx().GetSessionVars().EnableINLJoinInnerMultiPattern {
				return nil
			}
			wrapper.zippedChildren = append(wrapper.zippedChildren, child)
		case *logicalop.LogicalUnionScan:
			wrapper.hasDitryWrite = true
			wrapper.zippedChildren = append(wrapper.zippedChildren, child)
		default:
			return nil
		}
	}
	if wrapper.ds == nil || wrapper.ds.PreferStoreType&h.PreferTiFlash != 0 {
		return nil
	}
	return wrapper
}

// buildDataSource2IndexScanByIndexJoinProp builds an IndexScan as the inner child for an
// IndexJoin based on IndexJoinProp included in prop if possible.
//
// buildDataSource2IndexScanByIndexJoinProp differs with buildIndexJoinInner2IndexScan in that
// the first one is try to build a single table scan as the inner child of an index join then return
// this inner task(raw table scan) bottom-up, which will be attached with other inner parents of an
// index join in attach2Task when bottom-up of enumerating the physical plans;
//
// while the second is try to build a table scan as the inner child of an index join, then build
// entire inner subtree of a index join out as innerTask instantly according those validated and
// zipped inner patterns with calling constructInnerIndexScanTask. That's not done yet, it also
// tries to enumerate kinds of index join operators based on the finished innerTask and un-decided
// outer child which will be physical-ed in the future.
func buildDataSource2IndexScanByIndexJoinProp(
	ds *logicalop.DataSource,
	prop *property.PhysicalProperty) base.Task {
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
	indexJoinResult, keyOff2IdxOff := getBestIndexJoinPathResultByProp(ds, prop.IndexJoinProp, indexValid)
	if indexJoinResult == nil {
		return base.InvalidTask
	}
	rangeInfo, maxOneRow := indexJoinPathGetRangeInfoAndMaxOneRow(ds.SCtx(), prop.IndexJoinProp.OuterJoinKeys, indexJoinResult)
	var innerTask base.Task
	if !prop.IsSortItemEmpty() && matchProperty(ds, indexJoinResult.chosenPath, prop) == property.PropMatched {
		innerTask = constructDS2IndexScanTask(ds, indexJoinResult.chosenPath, indexJoinResult.chosenRanges.Range(), indexJoinResult.chosenRemained, indexJoinResult.idxOff2KeyOff, rangeInfo, true, prop.SortItems[0].Desc, prop.IndexJoinProp.AvgInnerRowCnt, maxOneRow)
	} else {
		innerTask = constructDS2IndexScanTask(ds, indexJoinResult.chosenPath, indexJoinResult.chosenRanges.Range(), indexJoinResult.chosenRemained, indexJoinResult.idxOff2KeyOff, rangeInfo, false, false, prop.IndexJoinProp.AvgInnerRowCnt, maxOneRow)
	}
	// since there is a possibility that inner task can't be built and the returned value is nil, we just return base.InvalidTask.
	if innerTask == nil {
		return base.InvalidTask
	}
	// prepare the index path chosen information and wrap them as IndexJoinInfo and fill back to CopTask.
	// here we don't need to construct physical index join here anymore, because we will encapsulate it bottom-up.
	// chosenPath and lastColManager of indexJoinResult should be returned to the caller (seen by index join to keep
	// index join aware of indexColLens and compareFilters).
	completeIndexJoinFeedBackInfo(innerTask.(*physicalop.CopTask), indexJoinResult, indexJoinResult.chosenRanges, keyOff2IdxOff)
	return innerTask
}

// buildDataSource2TableScanByIndexJoinProp builds a TableScan as the inner child for an
// IndexJoin if possible.
// If the inner side of an index join is a TableScan, only one tuple will be
// fetched from the inner side for every tuple from the outer side. This will be
// promised to be no worse than building IndexScan as the inner child.
func buildDataSource2TableScanByIndexJoinProp(
	ds *logicalop.DataSource,
	prop *property.PhysicalProperty) base.Task {
	var tblPath *util.AccessPath
	for _, path := range ds.PossibleAccessPaths {
		if path.IsTablePath() && path.StoreType == kv.TiKV { // old logic
			tblPath = path
			break
		}
	}
	if tblPath == nil {
		return base.InvalidTask
	}
	var keyOff2IdxOff []int
	var ranges ranger.MutableRanges = ranger.Ranges{}
	var innerTask base.Task
	var indexJoinResult *indexJoinPathResult
	if ds.TableInfo.IsCommonHandle {
		// for the leaf datasource, we use old logic to get the indexJoinResult, which contain the chosen path and ranges.
		indexJoinResult, keyOff2IdxOff = getBestIndexJoinPathResultByProp(ds, prop.IndexJoinProp, func(path *util.AccessPath) bool { return path.IsCommonHandlePath })
		// if there is no chosen info, it means the leaf datasource couldn't even leverage this indexJoinProp, return InvalidTask.
		if indexJoinResult == nil {
			return base.InvalidTask
		}
		// prepare the range info with outer join keys, it shows like: [xxx] decided by:
		rangeInfo, maxOneRow := indexJoinPathGetRangeInfoAndMaxOneRow(ds.SCtx(), prop.IndexJoinProp.OuterJoinKeys, indexJoinResult)
		// construct the inner task with chosen path and ranges, note: it only for this leaf datasource.
		// like the normal way, we need to check whether the chosen path is matched with the prop, if so, we will set the `keepOrder` to true.
		if matchProperty(ds, indexJoinResult.chosenPath, prop) == property.PropMatched {
			innerTask = constructDS2TableScanTask(ds, indexJoinResult.chosenRanges.Range(), rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, prop.IndexJoinProp.AvgInnerRowCnt, maxOneRow)
		} else {
			innerTask = constructDS2TableScanTask(ds, indexJoinResult.chosenRanges.Range(), rangeInfo, false, false, prop.IndexJoinProp.AvgInnerRowCnt, maxOneRow)
		}
		ranges = indexJoinResult.chosenRanges
	} else {
		var (
			ok               bool
			chosenPath       *util.AccessPath
			newOuterJoinKeys []*expression.Column
			// note: pk col doesn't have mutableRanges, the global var(ranges) which will be handled as empty range in constructIndexJoin.
			localRanges ranger.Ranges
		)
		keyOff2IdxOff, newOuterJoinKeys, localRanges, chosenPath, ok = getIndexJoinIntPKPathInfo(ds, prop.IndexJoinProp.InnerJoinKeys, prop.IndexJoinProp.OuterJoinKeys, func(path *util.AccessPath) bool { return path.IsIntHandlePath })
		if !ok {
			return base.InvalidTask
		}
		// For IntHandle (integer primary key), it's always a unique match.
		maxOneRow := true
		rangeInfo := indexJoinIntPKRangeInfo(ds.SCtx().GetExprCtx().GetEvalCtx(), newOuterJoinKeys)
		if !prop.IsSortItemEmpty() && matchProperty(ds, chosenPath, prop) == property.PropMatched {
			innerTask = constructDS2TableScanTask(ds, localRanges, rangeInfo, true, prop.SortItems[0].Desc, prop.IndexJoinProp.AvgInnerRowCnt, maxOneRow)
		} else {
			innerTask = constructDS2TableScanTask(ds, localRanges, rangeInfo, false, false, prop.IndexJoinProp.AvgInnerRowCnt, maxOneRow)
		}
	}
	// since there is a possibility that inner task can't be built and the returned value is nil, we just return base.InvalidTask.
	if innerTask == nil {
		return base.InvalidTask
	}
	// prepare the index path chosen information and wrap them as IndexJoinInfo and fill back to CopTask.
	// here we don't need to construct physical index join here anymore, because we will encapsulate it bottom-up.
	// chosenPath and lastColManager of indexJoinResult should be returned to the caller (seen by index join to keep
	// index join aware of indexColLens and compareFilters).
	completeIndexJoinFeedBackInfo(innerTask.(*physicalop.CopTask), indexJoinResult, ranges, keyOff2IdxOff)
	return innerTask
}


func filterIndexJoinBySessionVars(sc base.PlanContext, indexJoins []base.PhysicalPlan) []base.PhysicalPlan {
	if sc.GetSessionVars().EnableIndexMergeJoin {
		return indexJoins
	}
	return slices.DeleteFunc(indexJoins, func(indexJoin base.PhysicalPlan) bool {
		_, ok := indexJoin.(*physicalop.PhysicalIndexMergeJoin)
		return ok
	})
}

const (
	joinLeft             = 0
	joinRight            = 1
	indexJoinMethod      = 0
	indexHashJoinMethod  = 1
	indexMergeJoinMethod = 2
)

func getIndexJoinSideAndMethod(join base.PhysicalPlan) (innerSide, joinMethod int, ok bool) {
	var innerIdx int
	switch ij := join.(type) {
	case *physicalop.PhysicalIndexJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexJoinMethod
	case *physicalop.PhysicalIndexHashJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexHashJoinMethod
	case *physicalop.PhysicalIndexMergeJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexMergeJoinMethod
	default:
		return 0, 0, false
	}
	ok = true
	innerSide = joinLeft
	if innerIdx == 1 {
		innerSide = joinRight
	}
	return
}

// tryToEnumerateIndexJoin returns all available index join plans, which will require inner indexJoinProp downside
// compared with original tryToGetIndexJoin.
