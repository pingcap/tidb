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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
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
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
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
		panic("unreachable")
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
		hasNullEQ     bool
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	}
	// TODO: support null equal join keys for index join
	if hasNullEQ {
		return nil
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
		hasNullEQ     bool
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	}
	// TODO: support null equal join keys for index join
	if hasNullEQ {
		return nil
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
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
			failpoint.Return(constructIndexHashJoin(p, prop, outerIdx, innerTask, nil, keyOff2IdxOff, path, lastColMng))
		}
	})
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
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL && innerTask != nil {
			failpoint.Return(constructIndexHashJoin(p, prop, outerIdx, innerTask, indexJoinResult.chosenRanges, keyOff2IdxOff, indexJoinResult.chosenPath, indexJoinResult.lastColManager))
		}
	})
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
func tryToEnumerateIndexJoin(super base.LogicalPlan, prop *property.PhysicalProperty) []base.PhysicalPlan {
	_, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.LeftOuterJoin:
		supportLeftOuter = true
	case base.RightOuterJoin:
		supportRightOuter = true
	case base.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	// according join type to enumerate index join with inner children's indexJoinProp.
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, enumerateIndexJoinByOuterIdx(super, prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, enumerateIndexJoinByOuterIdx(super, prop, 1)...)
	}
	// Pre-Handle hints and variables about index join, which try to detect the contradictory hint and variables
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables and rec warns.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.PreferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.PreferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.PreferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}
	// previously we will think about force index join hints here, but we have to wait the inner plans to be a valid
	// physical one/ones. Because indexJoinProp may not be admitted by its inner patterns, so we innovatively move all
	// hint related handling to the findBestTask function when we see the entire inner physical-ized plan tree. See xxx
	// for details.
	//
	// handleFilterIndexJoinHints is trying to avoid generating index join or index hash join when no-index-join related
	// hint is specified in the query. So we can do it in physic enumeration phase here.
	return handleFilterIndexJoinHints(p, candidates)
}

// tryToGetIndexJoin returns all available index join plans, and the second returned value indicates whether this plan is enforced by hints.
func tryToGetIndexJoin(p *logicalop.LogicalJoin, prop *property.PhysicalProperty) (indexJoins []base.PhysicalPlan, canForced bool) {
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.LeftOuterJoin:
		supportLeftOuter = true
	case base.RightOuterJoin:
		supportRightOuter = true
	case base.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, getIndexJoinByOuterIdx(p, prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, getIndexJoinByOuterIdx(p, prop, 1)...)
	}

	// Handle hints and variables about index join.
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables.
	// Handle hints conflict first.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.PreferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.PreferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.PreferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}

	candidates, canForced = handleForceIndexJoinHints(p, prop, candidates)
	if canForced {
		return candidates, canForced
	}
	candidates = handleFilterIndexJoinHints(p, candidates)
	// todo: if any variables banned it, why bother to generate it first?
	return filterIndexJoinBySessionVars(p.SCtx(), candidates), false
}

func enumerationContainIndexJoin(candidates [][]base.PhysicalPlan) bool {
	return slices.ContainsFunc(candidates, func(candidate []base.PhysicalPlan) bool {
		return slices.ContainsFunc(candidate, func(op base.PhysicalPlan) bool {
			_, _, ok := getIndexJoinSideAndMethod(op)
			return ok
		})
	})
}

// handleFilterIndexJoinHints is trying to avoid generating index join or index hash join when no-index-join related
// hint is specified in the query. So we can do it in physic enumeration phase.
func handleFilterIndexJoinHints(p *logicalop.LogicalJoin, candidates []base.PhysicalPlan) []base.PhysicalPlan {
	if !p.PreferAny(h.PreferNoIndexJoin, h.PreferNoIndexHashJoin, h.PreferNoIndexMergeJoin) {
		return candidates // no filter index join hints
	}
	filtered := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		_, joinMethod, ok := getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.PreferAny(h.PreferNoIndexJoin) && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferNoIndexHashJoin) && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferNoIndexMergeJoin) && joinMethod == indexMergeJoinMethod) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
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

func applyLogicalHintVarEigen(lp base.LogicalPlan, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	return applyLogicalJoinHint(lp, pp) ||
		applyLogicalTopNAndLimitHint(lp, pp, childTasks) ||
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
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				return true
			}
		} else {
			switch childTasks[0].(type) {
			case *physicalop.RootTask:
				// If the distinct agg can't be allowed to push down, we will consider root task type.
				// which is try to get the same behavior as before like types := {RootTask} only.
				return true
			case *physicalop.MppTask:
				// If the distinct agg can't be allowed to push down, we will consider mpp task type too --- RootTask vs MPPTask
				// which is try to get the same behavior as before like types := {RootTask} and appended {MPPTask}.
				return true
			default:
				return false
			}
		}
	} else if la.PreferAggToCop {
		// If the aggregation is preferred to be pushed down to coprocessor, we will prefer it.
		if _, ok := childTasks[0].(*physicalop.CopTask); ok {
			return true
		}
	}
	return false
}

func applyLogicalTopNAndLimitHint(lp base.LogicalPlan, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	hintPrefer, _ := pushLimitOrTopNForcibly(lp, pp)
	if hintPrefer {
		// if there is a user hint control, try to get the copTask as the prior.
		// here we don't assert task itself, because when topN attach 2 cop task, it will become root type automatically.
		if _, ok := childTasks[0].(*physicalop.CopTask); ok {
			return true
		}
		return false
	}
	return false
}

func hasNormalPreferTask(lp base.LogicalPlan, state *enumerateState, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	_, meetThreshold := pushLimitOrTopNForcibly(lp, pp)
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
				if _, ok := childTasks[0].(*physicalop.RootTask); ok {
					return false
				}
				// peer cop task should compare the cost with each other.
				if _, ok := childTasks[0].(*physicalop.CopTask); ok {
					return true
				}
			} else {
				if _, ok := childTasks[0].(*physicalop.CopTask); ok {
					state.topNCopExist = true
					return true
				}
				// when we encounter rootTask type while still see no topNCopExist.
				// that means there is no copTask valid before, we will consider rootTask here.
				if _, ok := childTasks[0].(*physicalop.RootTask); ok {
					return true
				}
			}
			if _, ok := childTasks[0].(*physicalop.MppTask); ok {
				return true
			}
			// shouldn't be here
			return false
		}
		// limit case:
		if state.limitCopExist {
			if _, ok := childTasks[0].(*physicalop.RootTask); ok {
				return false
			}
			// peer cop task should compare the cost with each other.
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				return true
			}
		} else {
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				state.limitCopExist = true
				return true
			}
			// when we encounter rootTask type while still see no limitCopExist.
			// that means there is no copTask valid before, we will consider rootTask here.
			if _, ok := childTasks[0].(*physicalop.RootTask); ok {
				return true
			}
		}
		if _, ok := childTasks[0].(*physicalop.MppTask); ok {
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

// handleForceIndexJoinHints handles the force index join hints and returns all plans that can satisfy the hints.
func handleForceIndexJoinHints(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, candidates []base.PhysicalPlan) (indexJoins []base.PhysicalPlan, canForced bool) {
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return candidates, false // no force index join hints
	}
	forced := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		innerSide, joinMethod, ok := getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.PreferAny(h.PreferLeftAsINLJInner) && innerSide == joinLeft && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLJInner) && innerSide == joinRight && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferLeftAsINLHJInner) && innerSide == joinLeft && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLHJInner) && innerSide == joinRight && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferLeftAsINLMJInner) && innerSide == joinLeft && joinMethod == indexMergeJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLMJInner) && innerSide == joinRight && joinMethod == indexMergeJoinMethod) {
			forced = append(forced, candidate)
		}
	}

	if len(forced) > 0 {
		return forced, true
	}
	// Cannot find any valid index join plan with these force hints.
	// Print warning message if any hints cannot work.
	// If the required property is not empty, we will enforce it and try the hint again.
	// So we only need to generate warning message when the property is empty.
	if prop.IsSortItemEmpty() {
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
		}
		// Append inapplicable reason.
		if len(p.EqualConditions) == 0 {
			errMsg += " without column equal ON condition"
		}
		// Generate warning message to client.
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
	}
	return candidates, false
}

func checkChildFitBC(pctx base.PlanContext, stats *property.StatsInfo, schema *expression.Schema) bool {
	if stats.HistColl == nil {
		return pctx.GetSessionVars().BroadcastJoinThresholdCount == -1 || stats.Count() < pctx.GetSessionVars().BroadcastJoinThresholdCount
	}
	avg := cardinality.GetAvgRowSize(pctx, stats.HistColl, schema.Columns, false, false)
	sz := avg * float64(stats.Count())
	return pctx.GetSessionVars().BroadcastJoinThresholdSize == -1 || sz < float64(pctx.GetSessionVars().BroadcastJoinThresholdSize)
}

func calcBroadcastExchangeSize(p base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	s := p.StatsInfo()
	row = float64(s.Count()) * float64(mppStoreCnt-1)
	if s.HistColl == nil {
		return row, 0, false
	}
	avg := cardinality.GetAvgRowSize(p.SCtx(), s.HistColl, p.Schema().Columns, false, false)
	size = avg * row
	return row, size, true
}

func calcBroadcastExchangeSizeByChild(p1 base.Plan, p2 base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	row1, size1, hasSize1 := calcBroadcastExchangeSize(p1, mppStoreCnt)
	row2, size2, hasSize2 := calcBroadcastExchangeSize(p2, mppStoreCnt)

	// broadcast exchange size:
	//   Build: (mppStoreCnt - 1) * sizeof(BuildTable)
	//   Probe: 0
	// choose the child plan with the maximum approximate value as Probe

	if hasSize1 && hasSize2 {
		return math.Min(row1, row2), math.Min(size1, size2), true
	}

	return math.Min(row1, row2), 0, false
}

func calcHashExchangeSize(p base.Plan, mppStoreCnt int) (row float64, sz float64, hasSize bool) {
	s := p.StatsInfo()
	row = float64(s.Count()) * float64(mppStoreCnt-1) / float64(mppStoreCnt)
	if s.HistColl == nil {
		return row, 0, false
	}
	avg := cardinality.GetAvgRowSize(p.SCtx(), s.HistColl, p.Schema().Columns, false, false)
	sz = avg * row
	return row, sz, true
}

func calcHashExchangeSizeByChild(p1 base.Plan, p2 base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	row1, size1, hasSize1 := calcHashExchangeSize(p1, mppStoreCnt)
	row2, size2, hasSize2 := calcHashExchangeSize(p2, mppStoreCnt)

	// hash exchange size:
	//   Build: sizeof(BuildTable) * (mppStoreCnt - 1) / mppStoreCnt
	//   Probe: sizeof(ProbeTable) * (mppStoreCnt - 1) / mppStoreCnt

	if hasSize1 && hasSize2 {
		return row1 + row2, size1 + size2, true
	}
	return row1 + row2, 0, false
}

// The size of `Build` hash table when using broadcast join is about `X`.
// The size of `Build` hash table when using shuffle join is about `X / (mppStoreCnt)`.
// It will cost more time to construct `Build` hash table and search `Probe` while using broadcast join.
// Set a scale factor (`mppStoreCnt^*`) when estimating broadcast join in `isJoinFitMPPBCJ` and `isJoinChildFitMPPBCJ` (based on TPCH benchmark, it has been verified in Q9).

func isJoinFitMPPBCJ(p *logicalop.LogicalJoin, mppStoreCnt int) bool {
	rowBC, szBC, hasSizeBC := calcBroadcastExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)
	rowHash, szHash, hasSizeHash := calcHashExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)
	if hasSizeBC && hasSizeHash {
		return szBC*float64(mppStoreCnt) <= szHash
	}
	return rowBC*float64(mppStoreCnt) <= rowHash
}

func isJoinChildFitMPPBCJ(p *logicalop.LogicalJoin, childIndexToBC int, mppStoreCnt int) bool {
	rowBC, szBC, hasSizeBC := calcBroadcastExchangeSize(p.Children()[childIndexToBC], mppStoreCnt)
	rowHash, szHash, hasSizeHash := calcHashExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)

	if hasSizeBC && hasSizeHash {
		return szBC*float64(mppStoreCnt) <= szHash
	}
	return rowBC*float64(mppStoreCnt) <= rowHash
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

// If we can use mpp broadcast join, that's our first choice.
func preferMppBCJ(super base.LogicalPlan) bool {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	if len(p.EqualConditions) == 0 && p.SCtx().GetSessionVars().AllowCartesianBCJ == 2 {
		return true
	}

	onlyCheckChild1 := p.JoinType == base.LeftOuterJoin || p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin
	onlyCheckChild0 := p.JoinType == base.RightOuterJoin

	if p.SCtx().GetSessionVars().PreferBCJByExchangeDataSize {
		mppStoreCnt, err := p.SCtx().GetMPPClient().GetMPPStoreCount()

		// No need to exchange data if there is only ONE mpp store. But the behavior of optimizer is unexpected if use broadcast way forcibly, such as tpch q4.
		// TODO: always use broadcast way to exchange data if there is only ONE mpp store.

		if err == nil && mppStoreCnt > 0 {
			if !(onlyCheckChild1 || onlyCheckChild0) {
				return isJoinFitMPPBCJ(p, mppStoreCnt)
			}
			if mppStoreCnt > 1 {
				if onlyCheckChild1 {
					return isJoinChildFitMPPBCJ(p, 1, mppStoreCnt)
				} else if onlyCheckChild0 {
					return isJoinChildFitMPPBCJ(p, 0, mppStoreCnt)
				}
			}
			// If mppStoreCnt is ONE and only need to check one child plan, rollback to original way.
			// Otherwise, the plan of tpch q4 may be unexpected.
		}
	}
	stats0, stats1, schema0, schema1 := getJoinChildStatsAndSchema(ge, p)
	pctx := p.SCtx()
	if onlyCheckChild1 {
		return checkChildFitBC(pctx, stats1, schema1)
	} else if onlyCheckChild0 {
		return checkChildFitBC(pctx, stats0, schema0)
	}
	return checkChildFitBC(pctx, stats0, schema0) || checkChildFitBC(pctx, stats1, schema1)
}

func canExprsInJoinPushdown(p *logicalop.LogicalJoin, storeType kv.StoreType) bool {
	equalExprs := make([]expression.Expression, 0, len(p.EqualConditions))
	for _, eqCondition := range p.EqualConditions {
		if eqCondition.FuncName.L == ast.NullEQ {
			return false
		}
		equalExprs = append(equalExprs, eqCondition)
	}
	pushDownCtx := util.GetPushDownCtx(p.SCtx())
	if !expression.CanExprsPushDown(pushDownCtx, equalExprs, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.LeftConditions, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.RightConditions, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.OtherConditions, storeType) {
		return false
	}
	return true
}

func tryToGetMppHashJoin(super base.LogicalPlan, prop *property.PhysicalProperty, useBCJ bool) []base.PhysicalPlan {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil
	}

	if !expression.IsPushDownEnabled(p.JoinType.String(), kv.TiFlash) {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because join type `" + p.JoinType.String() + "` is blocked by blacklist, check `table mysql.expr_pushdown_blacklist;` for more information.")
		return nil
	}

	if p.JoinType != base.InnerJoin && p.JoinType != base.LeftOuterJoin && p.JoinType != base.RightOuterJoin && p.JoinType != base.SemiJoin && p.JoinType != base.AntiSemiJoin && p.JoinType != base.LeftOuterSemiJoin && p.JoinType != base.AntiLeftOuterSemiJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because join type `" + p.JoinType.String() + "` is not supported now.")
		return nil
	}

	if len(p.EqualConditions) == 0 {
		if !useBCJ {
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variables `tidb_broadcast_join_threshold_size` and `tidb_broadcast_join_threshold_count`.")
			return nil
		}
		if p.SCtx().GetSessionVars().AllowCartesianBCJ == 0 {
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variable `tidb_opt_broadcast_cartesian_join`.")
			return nil
		}
	}
	if len(p.LeftConditions) != 0 && p.JoinType != base.LeftOuterJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there is a join that is not `left join` but has left conditions, which is not supported by mpp now, see github.com/pingcap/tidb/issues/26090 for more information.")
		return nil
	}
	if len(p.RightConditions) != 0 && p.JoinType != base.RightOuterJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there is a join that is not `right join` but has right conditions, which is not supported by mpp now.")
		return nil
	}

	if prop.MPPPartitionTp == property.BroadcastType {
		return nil
	}
	if !canExprsInJoinPushdown(p, kv.TiFlash) {
		return nil
	}
	lkeys, rkeys, _, _ := p.GetJoinKeys()
	lNAkeys, rNAKeys := p.GetNAJoinKeys()
	// check match property
	baseJoin := physicalop.BasePhysicalJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		DefaultValues:   p.DefaultValues,
		LeftJoinKeys:    lkeys,
		RightJoinKeys:   rkeys,
		LeftNAJoinKeys:  lNAkeys,
		RightNAJoinKeys: rNAKeys,
	}
	// It indicates which side is the build side.
	forceLeftToBuild := ((p.PreferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.PreferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are conflicts, please check the hints")
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	preferredBuildIndex := 0
	fixedBuildSide := false // Used to indicate whether the build side for the MPP join is fixed or not.
	stats0, stats1, _, _ := getJoinChildStatsAndSchema(ge, p)
	if p.JoinType == base.InnerJoin {
		if stats0.Count() > stats1.Count() {
			preferredBuildIndex = 1
		}
	} else if p.JoinType.IsSemiJoin() {
		if !useBCJ && !p.IsNAAJ() && len(p.EqualConditions) > 0 && (p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin) {
			// TiFlash only supports Non-null_aware non-cross semi/anti_semi join to use both sides as build side
			preferredBuildIndex = 1
			// MPPOuterJoinFixedBuildSide default value is false
			// use MPPOuterJoinFixedBuildSide here as a way to disable using left table as build side
			if !p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide && stats1.Count() > stats0.Count() {
				preferredBuildIndex = 0
			}
		} else {
			preferredBuildIndex = 1
			fixedBuildSide = true
		}
	}
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		// TiFlash does not require that the build side must be the inner table for outer join.
		// so we can choose the build side based on the row count, except that:
		// 1. it is a broadcast join(for broadcast join, it makes sense to use the broadcast side as the build side)
		// 2. or session variable MPPOuterJoinFixedBuildSide is set to true
		// 3. or nullAware/cross joins
		if useBCJ || p.IsNAAJ() || len(p.EqualConditions) == 0 || p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide {
			if !p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide {
				// The hint has higher priority than variable.
				fixedBuildSide = true
			}
			if p.JoinType == base.LeftOuterJoin {
				preferredBuildIndex = 1
			}
		} else if stats0.Count() > stats1.Count() {
			preferredBuildIndex = 1
		}
	}

	if forceLeftToBuild || forceRightToBuild {
		match := (forceLeftToBuild && preferredBuildIndex == 0) || (forceRightToBuild && preferredBuildIndex == 1)
		if !match {
			if fixedBuildSide {
				// A warning will be generated if the build side is fixed, but we attempt to change it using the hint.
				p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
					"Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints")
			} else {
				// The HASH_JOIN_BUILD OR HASH_JOIN_PROBE hints can take effective.
				preferredBuildIndex = 1 - preferredBuildIndex
			}
		}
	}

	// set preferredBuildIndex for test
	failpoint.Inject("mockPreferredBuildIndex", func(val failpoint.Value) {
		if !p.SCtx().GetSessionVars().InRestrictedSQL {
			preferredBuildIndex = val.(int)
		}
	})

	baseJoin.InnerChildIdx = preferredBuildIndex
	childrenProps := make([]*property.PhysicalProperty, 2)
	if useBCJ {
		childrenProps[preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.BroadcastType, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
		expCnt := math.MaxFloat64
		if prop.ExpectedCnt < p.StatsInfo().RowCount {
			expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
			var targetStats *property.StatsInfo
			if 1-preferredBuildIndex == 0 {
				targetStats = stats0
			} else {
				targetStats = stats1
			}
			expCnt = targetStats.RowCount * expCntScale
		}
		if prop.MPPPartitionTp == property.HashType {
			lPartitionKeys, rPartitionKeys := p.GetPotentialPartitionKeys()
			hashKeys := rPartitionKeys
			if preferredBuildIndex == 1 {
				hashKeys = lPartitionKeys
			}
			matches := prop.IsSubsetOf(hashKeys)
			if len(matches) == 0 {
				return nil
			}
			childrenProps[1-preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: expCnt, MPPPartitionTp: property.HashType, MPPPartitionCols: prop.MPPPartitionCols, CTEProducerStatus: prop.CTEProducerStatus}
		} else {
			childrenProps[1-preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: expCnt, MPPPartitionTp: property.AnyType, CTEProducerStatus: prop.CTEProducerStatus}
		}
	} else {
		lPartitionKeys, rPartitionKeys := p.GetPotentialPartitionKeys()
		if prop.MPPPartitionTp == property.HashType {
			var matches []int
			switch p.JoinType {
			case base.InnerJoin:
				if matches = prop.IsSubsetOf(lPartitionKeys); len(matches) == 0 {
					matches = prop.IsSubsetOf(rPartitionKeys)
				}
			case base.RightOuterJoin:
				// for right out join, only the right partition keys can possibly matches the prop, because
				// the left partition keys will generate NULL values randomly
				// todo maybe we can add a null-sensitive flag in the MPPPartitionColumn to indicate whether the partition column is
				//  null-sensitive(used in aggregation) or null-insensitive(used in join)
				matches = prop.IsSubsetOf(rPartitionKeys)
			default:
				// for left out join, only the left partition keys can possibly matches the prop, because
				// the right partition keys will generate NULL values randomly
				// for semi/anti semi/left out semi/anti left out semi join, only left partition keys are returned,
				// so just check the left partition keys
				matches = prop.IsSubsetOf(lPartitionKeys)
			}
			if len(matches) == 0 {
				return nil
			}
			lPartitionKeys = property.ChoosePartitionKeys(lPartitionKeys, matches)
			rPartitionKeys = property.ChoosePartitionKeys(rPartitionKeys, matches)
		}
		childrenProps[0] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: lPartitionKeys, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
		childrenProps[1] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: rPartitionKeys, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
	}
	join := physicalop.PhysicalHashJoin{
		BasePhysicalJoin:  baseJoin,
		Concurrency:       uint(p.SCtx().GetSessionVars().CopTiFlashConcurrencyFactor),
		EqualConditions:   p.EqualConditions,
		NAEqualConditions: p.NAEQConditions,
		StoreTp:           kv.TiFlash,
		MppShuffleJoin:    !useBCJ,
		// Mpp Join has quite heavy cost. Even limit might not suspend it in time, so we don't scale the count.
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), childrenProps...)
	join.SetSchema(p.Schema())
	return []base.PhysicalPlan{join}
}

// it can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func exhaustPhysicalPlans4LogicalJoin(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
			indexJoins, _ := tryToGetIndexJoin(p, prop)
			failpoint.Return(indexJoins, true, nil)
		}
	})

	if !isJoinHintSupportedInMPPMode(p.PreferJoinType) {
		if hasMPPJoinHints(p.PreferJoinType) {
			// If there are MPP hints but has some conflicts join method hints, all the join hints are invalid.
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("The MPP join hints are in conflict, and you can only specify join method hints that are currently supported by MPP mode now")
			p.PreferJoinType = 0
		} else {
			// If there are no MPP hints but has some conflicts join method hints, the MPP mode will be blocked.
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because you have used hint to specify a join algorithm which is not supported by mpp now.")
			if prop.IsFlashProp() {
				return nil, false, nil
			}
		}
	}
	if prop.MPPPartitionTp == property.BroadcastType {
		return nil, false, nil
	}
	joins := make([]base.PhysicalPlan, 0, 8)
	// we lift the p.canPushToTiFlash check here, because we want to generate all the plans to be decided by the attachment layer.
	if p.SCtx().GetSessionVars().IsMPPAllowed() {
		// prefer hint should be handled in the attachment layer. because the enumerated mpp join may couldn't be built bottom-up.
		if hasMPPJoinHints(p.PreferJoinType) {
			// generate them all for later attachment prefer picking. cause underlying ds may not have tiFlash path.
			// even all mpp join is invalid, they can still resort to root joins as an alternative.
			joins = append(joins, tryToGetMppHashJoin(super, prop, true)...)
			joins = append(joins, tryToGetMppHashJoin(super, prop, false)...)
		} else {
			// join don't have a mpp join hints, only generate preferMppBCJ mpp joins.
			if preferMppBCJ(super) {
				joins = append(joins, tryToGetMppHashJoin(super, prop, true)...)
			} else {
				joins = append(joins, tryToGetMppHashJoin(super, prop, false)...)
			}
		}
	} else {
		hasMppHints := false
		var errMsg string
		if (p.PreferJoinType & h.PreferShuffleJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the shuffle_join() hint is invalid"
			hasMppHints = true
		}
		if (p.PreferJoinType & h.PreferBCJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the broadcast_join() hint is invalid"
			hasMppHints = true
		}
		if hasMppHints {
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
	if prop.IsFlashProp() {
		return joins, true, nil
	}

	if !p.IsNAAJ() {
		// naaj refuse merge join and index join.
		stats0, stats1, _, _ := getJoinChildStatsAndSchema(ge, p)
		mergeJoins := physicalop.GetMergeJoin(p, prop, p.Schema(), p.StatsInfo(), stats0, stats1)
		if (p.PreferJoinType&h.PreferMergeJoin) > 0 && len(mergeJoins) > 0 {
			return mergeJoins, true, nil
		}
		joins = append(joins, mergeJoins...)

		if p.SCtx().GetSessionVars().EnhanceIndexJoinBuildV2 {
			indexJoins := tryToEnumerateIndexJoin(super, prop)
			joins = append(joins, indexJoins...)

			failpoint.Inject("MockOnlyEnableIndexHashJoinV2", func(val failpoint.Value) {
				if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
					indexHashJoin := make([]base.PhysicalPlan, 0, len(indexJoins))
					for _, one := range indexJoins {
						if _, ok := one.(*physicalop.PhysicalIndexHashJoin); ok {
							indexHashJoin = append(indexHashJoin, one)
						}
					}
					failpoint.Return(indexHashJoin, true, nil)
				}
			})
		} else {
			indexJoins, forced := tryToGetIndexJoin(p, prop)
			if forced {
				return indexJoins, true, nil
			}
			joins = append(joins, indexJoins...)
		}
	}

	hashJoins, forced := getHashJoins(super, prop)
	if forced && len(hashJoins) > 0 {
		return hashJoins, true, nil
	}
	joins = append(joins, hashJoins...)

	if p.PreferJoinType > 0 {
		// If we reach here, it means we have a hint that doesn't work.
		// It might be affected by the required property, so we enforce
		// this property and try the hint again.
		return joins, false, nil
	}
	return joins, true, nil
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

// GetHashJoin is public for cascades planner.
func GetHashJoin(ge base.GroupExpression, la *logicalop.LogicalApply, prop *property.PhysicalProperty) *physicalop.PhysicalHashJoin {
	return getHashJoin(ge, &la.LogicalJoin, prop, 1, false)
}

// exhaustPhysicalPlans4LogicalApply generates the physical plan for a logical apply.
func exhaustPhysicalPlans4LogicalApply(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ge, la := base.GetGEAndLogicalOp[*logicalop.LogicalApply](super)
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
	join := GetHashJoin(ge, la, prop)
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
		NoDecorrelate:    la.NoDecorrelate,
	}.Init(la.SCtx(),
		la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt),
		la.QueryBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: true},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
	apply.SetSchema(la.Schema())
	return []base.PhysicalPlan{apply}, true, nil
}
