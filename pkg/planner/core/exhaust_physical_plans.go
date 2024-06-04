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
	"bytes"
	"fmt"
	"math"
	"slices"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalUnionScan) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if prop.IsFlashProp() {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `UnionScan` is not supported now.")
		return nil, true, nil
	}
	childProp := prop.CloneEssentialFields()
	us := PhysicalUnionScan{
		Conditions: p.conditions,
		HandleCols: p.handleCols,
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), childProp)
	return []base.PhysicalPlan{us}, true, nil
}

func getMaxSortPrefix(sortCols, allCols []*expression.Column) []int {
	tmpSchema := expression.NewSchema(allCols...)
	sortColOffsets := make([]int, 0, len(sortCols))
	for _, sortCol := range sortCols {
		offset := tmpSchema.ColumnIndex(sortCol)
		if offset == -1 {
			return sortColOffsets
		}
		sortColOffsets = append(sortColOffsets, offset)
	}
	return sortColOffsets
}

func findMaxPrefixLen(candidates [][]*expression.Column, keys []*expression.Column) int {
	maxLen := 0
	for _, candidateKeys := range candidates {
		matchedLen := 0
		for i := range keys {
			if !(i < len(candidateKeys) && keys[i].EqualColumn(candidateKeys[i])) {
				break
			}
			matchedLen++
		}
		if matchedLen > maxLen {
			maxLen = matchedLen
		}
	}
	return maxLen
}

func (p *LogicalJoin) moveEqualToOtherConditions(offsets []int) []expression.Expression {
	// Construct used equal condition set based on the equal condition offsets.
	usedEqConds := set.NewIntSet()
	for _, eqCondIdx := range offsets {
		usedEqConds.Insert(eqCondIdx)
	}

	// Construct otherConds, which is composed of the original other conditions
	// and the remained unused equal conditions.
	numOtherConds := len(p.OtherConditions) + len(p.EqualConditions) - len(usedEqConds)
	otherConds := make([]expression.Expression, len(p.OtherConditions), numOtherConds)
	copy(otherConds, p.OtherConditions)
	for eqCondIdx := range p.EqualConditions {
		if !usedEqConds.Exist(eqCondIdx) {
			otherConds = append(otherConds, p.EqualConditions[eqCondIdx])
		}
	}

	return otherConds
}

// Only if the input required prop is the prefix fo join keys, we can pass through this property.
func (p *PhysicalMergeJoin) tryToGetChildReqProp(prop *property.PhysicalProperty) ([]*property.PhysicalProperty, bool) {
	all, desc := prop.AllSameOrder()
	lProp := property.NewPhysicalProperty(property.RootTaskType, p.LeftJoinKeys, desc, math.MaxFloat64, false)
	rProp := property.NewPhysicalProperty(property.RootTaskType, p.RightJoinKeys, desc, math.MaxFloat64, false)
	lProp.CTEProducerStatus = prop.CTEProducerStatus
	rProp.CTEProducerStatus = prop.CTEProducerStatus
	if !prop.IsSortItemEmpty() {
		// sort merge join fits the cases of massive ordered data, so desc scan is always expensive.
		if !all {
			return nil, false
		}
		if !prop.IsPrefix(lProp) && !prop.IsPrefix(rProp) {
			return nil, false
		}
		if prop.IsPrefix(rProp) && p.JoinType == LeftOuterJoin {
			return nil, false
		}
		if prop.IsPrefix(lProp) && p.JoinType == RightOuterJoin {
			return nil, false
		}
	}

	return []*property.PhysicalProperty{lProp, rProp}, true
}

func (*LogicalJoin) checkJoinKeyCollation(leftKeys, rightKeys []*expression.Column) bool {
	// if a left key and its corresponding right key have different collation, don't use MergeJoin since
	// the their children may sort their records in different ways
	for i := range leftKeys {
		lt := leftKeys[i].RetType
		rt := rightKeys[i].RetType
		if (lt.EvalType() == types.ETString && rt.EvalType() == types.ETString) &&
			(leftKeys[i].RetType.GetCharset() != rightKeys[i].RetType.GetCharset() ||
				leftKeys[i].RetType.GetCollate() != rightKeys[i].RetType.GetCollate()) {
			return false
		}
	}
	return true
}

// GetMergeJoin convert the logical join to physical merge join based on the physical property.
func (p *LogicalJoin) GetMergeJoin(prop *property.PhysicalProperty, schema *expression.Schema, statsInfo *property.StatsInfo, leftStatsInfo *property.StatsInfo, rightStatsInfo *property.StatsInfo) []base.PhysicalPlan {
	joins := make([]base.PhysicalPlan, 0, len(p.leftProperties)+1)
	// The leftProperties caches all the possible properties that are provided by its children.
	leftJoinKeys, rightJoinKeys, isNullEQ, hasNullEQ := p.GetJoinKeys()

	// EnumType/SetType Unsupported: merge join conflicts with index order.
	// ref: https://github.com/pingcap/tidb/issues/24473, https://github.com/pingcap/tidb/issues/25669
	for _, leftKey := range leftJoinKeys {
		if leftKey.RetType.GetType() == mysql.TypeEnum || leftKey.RetType.GetType() == mysql.TypeSet {
			return nil
		}
	}
	for _, rightKey := range rightJoinKeys {
		if rightKey.RetType.GetType() == mysql.TypeEnum || rightKey.RetType.GetType() == mysql.TypeSet {
			return nil
		}
	}

	// TODO: support null equal join keys for merge join
	if hasNullEQ {
		return nil
	}
	for _, lhsChildProperty := range p.leftProperties {
		offsets := getMaxSortPrefix(lhsChildProperty, leftJoinKeys)
		// If not all equal conditions hit properties. We ban merge join heuristically. Because in this case, merge join
		// may get a very low performance. In executor, executes join results before other conditions filter it.
		if len(offsets) < len(leftJoinKeys) {
			continue
		}

		leftKeys := lhsChildProperty[:len(offsets)]
		rightKeys := expression.NewSchema(rightJoinKeys...).ColumnsByIndices(offsets)
		newIsNullEQ := make([]bool, 0, len(offsets))
		for _, offset := range offsets {
			newIsNullEQ = append(newIsNullEQ, isNullEQ[offset])
		}

		prefixLen := findMaxPrefixLen(p.rightProperties, rightKeys)
		if prefixLen == 0 {
			continue
		}

		leftKeys = leftKeys[:prefixLen]
		rightKeys = rightKeys[:prefixLen]
		newIsNullEQ = newIsNullEQ[:prefixLen]
		if !p.checkJoinKeyCollation(leftKeys, rightKeys) {
			continue
		}
		offsets = offsets[:prefixLen]
		baseJoin := basePhysicalJoin{
			JoinType:        p.JoinType,
			LeftConditions:  p.LeftConditions,
			RightConditions: p.RightConditions,
			DefaultValues:   p.DefaultValues,
			LeftJoinKeys:    leftKeys,
			RightJoinKeys:   rightKeys,
			IsNullEQ:        newIsNullEQ,
		}
		mergeJoin := PhysicalMergeJoin{basePhysicalJoin: baseJoin}.Init(p.SCtx(), statsInfo.ScaleByExpectCnt(prop.ExpectedCnt), p.QueryBlockOffset())
		mergeJoin.SetSchema(schema)
		mergeJoin.OtherConditions = p.moveEqualToOtherConditions(offsets)
		mergeJoin.initCompareFuncs()
		if reqProps, ok := mergeJoin.tryToGetChildReqProp(prop); ok {
			// Adjust expected count for children nodes.
			if prop.ExpectedCnt < statsInfo.RowCount {
				expCntScale := prop.ExpectedCnt / statsInfo.RowCount
				reqProps[0].ExpectedCnt = leftStatsInfo.RowCount * expCntScale
				reqProps[1].ExpectedCnt = rightStatsInfo.RowCount * expCntScale
			}
			mergeJoin.childrenReqProps = reqProps
			_, desc := prop.AllSameOrder()
			mergeJoin.Desc = desc
			joins = append(joins, mergeJoin)
		}
	}

	if p.preferJoinType&h.PreferNoMergeJoin > 0 {
		if p.preferJoinType&h.PreferMergeJoin == 0 {
			return nil
		}
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Some MERGE_JOIN and NO_MERGE_JOIN hints conflict, NO_MERGE_JOIN is ignored")
	}

	// If TiDB_SMJ hint is existed, it should consider enforce merge join,
	// because we can't trust lhsChildProperty completely.
	if (p.preferJoinType&h.PreferMergeJoin) > 0 ||
		p.shouldSkipHashJoin() { // if hash join is not allowed, generate as many other types of join as possible to avoid 'cant-find-plan' error.
		joins = append(joins, p.getEnforcedMergeJoin(prop, schema, statsInfo)...)
	}

	return joins
}

// Change JoinKeys order, by offsets array
// offsets array is generate by prop check
func getNewJoinKeysByOffsets(oldJoinKeys []*expression.Column, offsets []int) []*expression.Column {
	newKeys := make([]*expression.Column, 0, len(oldJoinKeys))
	for _, offset := range offsets {
		newKeys = append(newKeys, oldJoinKeys[offset])
	}
	for pos, key := range oldJoinKeys {
		isExist := false
		for _, p := range offsets {
			if p == pos {
				isExist = true
				break
			}
		}
		if !isExist {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

func getNewNullEQByOffsets(oldNullEQ []bool, offsets []int) []bool {
	newNullEQ := make([]bool, 0, len(oldNullEQ))
	for _, offset := range offsets {
		newNullEQ = append(newNullEQ, oldNullEQ[offset])
	}
	for pos, key := range oldNullEQ {
		isExist := false
		for _, p := range offsets {
			if p == pos {
				isExist = true
				break
			}
		}
		if !isExist {
			newNullEQ = append(newNullEQ, key)
		}
	}
	return newNullEQ
}

func (p *LogicalJoin) getEnforcedMergeJoin(prop *property.PhysicalProperty, schema *expression.Schema, statsInfo *property.StatsInfo) []base.PhysicalPlan {
	// Check whether SMJ can satisfy the required property
	leftJoinKeys, rightJoinKeys, isNullEQ, hasNullEQ := p.GetJoinKeys()
	// TODO: support null equal join keys for merge join
	if hasNullEQ {
		return nil
	}
	offsets := make([]int, 0, len(leftJoinKeys))
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	for _, item := range prop.SortItems {
		isExist, hasLeftColInProp, hasRightColInProp := false, false, false
		for joinKeyPos := 0; joinKeyPos < len(leftJoinKeys); joinKeyPos++ {
			var key *expression.Column
			if item.Col.Equal(evalCtx, leftJoinKeys[joinKeyPos]) {
				key = leftJoinKeys[joinKeyPos]
				hasLeftColInProp = true
			}
			if item.Col.Equal(evalCtx, rightJoinKeys[joinKeyPos]) {
				key = rightJoinKeys[joinKeyPos]
				hasRightColInProp = true
			}
			if key == nil {
				continue
			}
			for i := 0; i < len(offsets); i++ {
				if offsets[i] == joinKeyPos {
					isExist = true
					break
				}
			}
			if !isExist {
				offsets = append(offsets, joinKeyPos)
			}
			isExist = true
			break
		}
		if !isExist {
			return nil
		}
		// If the output wants the order of the inner side. We should reject it since we might add null-extend rows of that side.
		if p.JoinType == LeftOuterJoin && hasRightColInProp {
			return nil
		}
		if p.JoinType == RightOuterJoin && hasLeftColInProp {
			return nil
		}
	}
	// Generate the enforced sort merge join
	leftKeys := getNewJoinKeysByOffsets(leftJoinKeys, offsets)
	rightKeys := getNewJoinKeysByOffsets(rightJoinKeys, offsets)
	newNullEQ := getNewNullEQByOffsets(isNullEQ, offsets)
	otherConditions := make([]expression.Expression, len(p.OtherConditions), len(p.OtherConditions)+len(p.EqualConditions))
	copy(otherConditions, p.OtherConditions)
	if !p.checkJoinKeyCollation(leftKeys, rightKeys) {
		// if the join keys' collation are conflicted, we use the empty join key
		// and move EqualConditions to OtherConditions.
		leftKeys = nil
		rightKeys = nil
		newNullEQ = nil
		otherConditions = append(otherConditions, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
	}
	lProp := property.NewPhysicalProperty(property.RootTaskType, leftKeys, desc, math.MaxFloat64, true)
	rProp := property.NewPhysicalProperty(property.RootTaskType, rightKeys, desc, math.MaxFloat64, true)
	baseJoin := basePhysicalJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftJoinKeys:    leftKeys,
		RightJoinKeys:   rightKeys,
		IsNullEQ:        newNullEQ,
		OtherConditions: otherConditions,
	}
	enforcedPhysicalMergeJoin := PhysicalMergeJoin{basePhysicalJoin: baseJoin, Desc: desc}.Init(p.SCtx(), statsInfo.ScaleByExpectCnt(prop.ExpectedCnt), p.QueryBlockOffset())
	enforcedPhysicalMergeJoin.SetSchema(schema)
	enforcedPhysicalMergeJoin.childrenReqProps = []*property.PhysicalProperty{lProp, rProp}
	enforcedPhysicalMergeJoin.initCompareFuncs()
	return []base.PhysicalPlan{enforcedPhysicalMergeJoin}
}

func (p *PhysicalMergeJoin) initCompareFuncs() {
	p.CompareFuncs = make([]expression.CompareFunc, 0, len(p.LeftJoinKeys))
	for i := range p.LeftJoinKeys {
		p.CompareFuncs = append(p.CompareFuncs, expression.GetCmpFunction(p.SCtx().GetExprCtx(), p.LeftJoinKeys[i], p.RightJoinKeys[i]))
	}
}

func (p *LogicalJoin) shouldSkipHashJoin() bool {
	return (p.preferJoinType&h.PreferNoHashJoin) > 0 || (p.SCtx().GetSessionVars().DisableHashJoin)
}

func (p *LogicalJoin) getHashJoins(prop *property.PhysicalProperty) (joins []base.PhysicalPlan, forced bool) {
	if !prop.IsSortItemEmpty() { // hash join doesn't promise any orders
		return
	}

	forceLeftToBuild := ((p.preferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.preferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.preferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.preferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are conflicts, please check the hints")
		forceLeftToBuild = false
		forceRightToBuild = false
	}

	joins = make([]base.PhysicalPlan, 0, 2)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		joins = append(joins, p.getHashJoin(prop, 1, false))
		if forceLeftToBuild || forceRightToBuild {
			// Do not support specifying the build and probe side for semi join.
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(fmt.Sprintf("We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for %s, please check the hint", p.JoinType))
			forceLeftToBuild = false
			forceRightToBuild = false
		}
	case LeftOuterJoin:
		if !forceLeftToBuild {
			joins = append(joins, p.getHashJoin(prop, 1, false))
		}
		if !forceRightToBuild {
			joins = append(joins, p.getHashJoin(prop, 1, true))
		}
	case RightOuterJoin:
		if !forceLeftToBuild {
			joins = append(joins, p.getHashJoin(prop, 0, true))
		}
		if !forceRightToBuild {
			joins = append(joins, p.getHashJoin(prop, 0, false))
		}
	case InnerJoin:
		if forceLeftToBuild {
			joins = append(joins, p.getHashJoin(prop, 0, false))
		} else if forceRightToBuild {
			joins = append(joins, p.getHashJoin(prop, 1, false))
		} else {
			joins = append(joins, p.getHashJoin(prop, 1, false))
			joins = append(joins, p.getHashJoin(prop, 0, false))
		}
	}

	forced = (p.preferJoinType&h.PreferHashJoin > 0) || forceLeftToBuild || forceRightToBuild
	if !forced && p.shouldSkipHashJoin() {
		return nil, false
	} else if forced && p.shouldSkipHashJoin() {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"A conflict between the HASH_JOIN hint and the NO_HASH_JOIN hint, " +
				"or the tidb_opt_enable_hash_join system variable, the HASH_JOIN hint will take precedence.")
	}
	return
}

func (p *LogicalJoin) getHashJoin(prop *property.PhysicalProperty, innerIdx int, useOuterToBuild bool) *PhysicalHashJoin {
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus}
	chReqProps[1-innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus}
	if prop.ExpectedCnt < p.StatsInfo().RowCount {
		expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
		chReqProps[1-innerIdx].ExpectedCnt = p.Children()[1-innerIdx].StatsInfo().RowCount * expCntScale
	}
	hashJoin := NewPhysicalHashJoin(p, innerIdx, useOuterToBuild, p.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(p.schema)
	return hashJoin
}

// When inner plan is TableReader, the parameter `ranges` will be nil. Because pk only have one column. So all of its range
// is generated during execution time.
func (p *LogicalJoin) constructIndexJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask base.Task,
	ranges ranger.MutableRanges,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *ColWithCmpFuncManager,
	extractOtherEQ bool,
) []base.PhysicalPlan {
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
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64, SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus}
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
				lhs, ok1 := c.GetArgs()[0].(*expression.Column)
				rhs, ok2 := c.GetArgs()[1].(*expression.Column)
				if ok1 && ok2 {
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
					newOtherConds = append(newOtherConds[:i], newOtherConds[i+1:]...)
				}
			}
		default:
			continue
		}
	}

	baseJoin := basePhysicalJoin{
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

	join := PhysicalIndexJoin{
		basePhysicalJoin: baseJoin,
		innerTask:        innerTask,
		KeyOff2IdxOff:    newKeyOff,
		Ranges:           ranges,
		CompareFilters:   compareFilters,
		OuterHashKeys:    outerHashKeys,
		InnerHashKeys:    innerHashKeys,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
	if path != nil {
		join.IdxColLens = path.IdxColLens
	}
	join.SetSchema(p.schema)
	return []base.PhysicalPlan{join}
}

func (p *LogicalJoin) constructIndexMergeJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask base.Task,
	ranges ranger.MutableRanges,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *ColWithCmpFuncManager,
) []base.PhysicalPlan {
	hintExists := false
	if (outerIdx == 1 && (p.preferJoinType&h.PreferLeftAsINLMJInner) > 0) || (outerIdx == 0 && (p.preferJoinType&h.PreferRightAsINLMJInner) > 0) {
		hintExists = true
	}
	indexJoins := p.constructIndexJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, compareFilters, !hintExists)
	indexMergeJoins := make([]base.PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*PhysicalIndexJoin)
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
			indexMergeJoin := PhysicalIndexMergeJoin{
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

func (p *LogicalJoin) constructIndexHashJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask base.Task,
	ranges ranger.MutableRanges,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *ColWithCmpFuncManager,
) []base.PhysicalPlan {
	indexJoins := p.constructIndexJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, compareFilters, true)
	indexHashJoins := make([]base.PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*PhysicalIndexJoin)
		indexHashJoin := PhysicalIndexHashJoin{
			PhysicalIndexJoin: *join,
			// Prop is empty means that the parent operator does not need the
			// join operator to provide any promise of the output order.
			KeepOuterOrder: !prop.IsSortItemEmpty(),
		}.Init(p.SCtx())
		indexHashJoins = append(indexHashJoins, indexHashJoin)
	}
	return indexHashJoins
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child.
// First of all, we'll check whether the inner child is DataSource.
// Then, we will extract the join keys of p's equal conditions. Then check whether all of them are just the primary key
// or match some part of on index. If so we will choose the best one and construct a index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(prop *property.PhysicalProperty, outerIdx int) (joins []base.PhysicalPlan) {
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
	innerChildWrapper := p.extractIndexJoinInnerChildPattern(innerChild)
	if innerChildWrapper == nil {
		return nil
	}

	var avgInnerRowCnt float64
	if outerChild.StatsInfo().RowCount > 0 {
		avgInnerRowCnt = p.equalCondOutCnt / outerChild.StatsInfo().RowCount
	}
	joins = p.buildIndexJoinInner2TableScan(prop, innerChildWrapper, innerJoinKeys, outerJoinKeys, outerIdx, avgInnerRowCnt)
	if joins != nil {
		return
	}
	return p.buildIndexJoinInner2IndexScan(prop, innerChildWrapper, innerJoinKeys, outerJoinKeys, outerIdx, avgInnerRowCnt)
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
	ds             *DataSource
	hasDitryWrite  bool
	zippedChildren []base.LogicalPlan
}

func (p *LogicalJoin) extractIndexJoinInnerChildPattern(innerChild base.LogicalPlan) *indexJoinInnerChildWrapper {
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
		case *DataSource:
			wrapper.ds = child
			break childLoop
		case *LogicalProjection, *LogicalSelection, *LogicalAggregation:
			if !p.SCtx().GetSessionVars().EnableINLJoinInnerMultiPattern {
				return nil
			}
			wrapper.zippedChildren = append(wrapper.zippedChildren, child)
		case *LogicalUnionScan:
			wrapper.hasDitryWrite = true
			wrapper.zippedChildren = append(wrapper.zippedChildren, child)
		default:
			return nil
		}
	}
	if wrapper.ds == nil || wrapper.ds.preferStoreType&h.PreferTiFlash != 0 {
		return nil
	}
	return wrapper
}

func (p *LogicalJoin) getIndexJoinBuildHelper(ds *DataSource, innerJoinKeys []*expression.Column, checkPathValid func(path *util.AccessPath) bool, outerJoinKeys []*expression.Column) (*indexJoinBuildHelper, []int) {
	helper := &indexJoinBuildHelper{
		join:      p,
		innerPlan: ds,
	}
	for _, path := range ds.possibleAccessPaths {
		if checkPathValid(path) {
			emptyRange, err := helper.analyzeLookUpFilters(path, ds, innerJoinKeys, outerJoinKeys, false)
			if emptyRange {
				return nil, nil
			}
			if err != nil {
				logutil.BgLogger().Warn("build index join failed", zap.Error(err))
			}
		}
	}
	if helper.chosenPath == nil {
		return nil, nil
	}
	keyOff2IdxOff := make([]int, len(innerJoinKeys))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = -1
	}
	for idxOff, keyOff := range helper.idxOff2KeyOff {
		if keyOff != -1 {
			keyOff2IdxOff[keyOff] = idxOff
		}
	}
	return helper, keyOff2IdxOff
}

// buildIndexJoinInner2TableScan builds a TableScan as the inner child for an
// IndexJoin if possible.
// If the inner side of a index join is a TableScan, only one tuple will be
// fetched from the inner side for every tuple from the outer side. This will be
// promised to be no worse than building IndexScan as the inner child.
func (p *LogicalJoin) buildIndexJoinInner2TableScan(
	prop *property.PhysicalProperty, wrapper *indexJoinInnerChildWrapper, innerJoinKeys, outerJoinKeys []*expression.Column,
	outerIdx int, avgInnerRowCnt float64) (joins []base.PhysicalPlan) {
	ds := wrapper.ds
	var tblPath *util.AccessPath
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath() && path.StoreType == kv.TiKV {
			tblPath = path
			break
		}
	}
	if tblPath == nil {
		return nil
	}
	keyOff2IdxOff := make([]int, len(innerJoinKeys))
	newOuterJoinKeys := make([]*expression.Column, 0)
	var ranges ranger.MutableRanges = ranger.Ranges{}
	var innerTask, innerTask2 base.Task
	var helper *indexJoinBuildHelper
	if ds.tableInfo.IsCommonHandle {
		helper, keyOff2IdxOff = p.getIndexJoinBuildHelper(ds, innerJoinKeys, func(path *util.AccessPath) bool { return path.IsCommonHandlePath }, outerJoinKeys)
		if helper == nil {
			return nil
		}
		rangeInfo := helper.buildRangeDecidedByInformation(helper.chosenPath.IdxCols, outerJoinKeys)
		innerTask = p.constructInnerTableScanTask(wrapper, helper.chosenRanges.Range(), outerJoinKeys, rangeInfo, false, false, avgInnerRowCnt)
		// The index merge join's inner plan is different from index join, so we
		// should construct another inner plan for it.
		// Because we can't keep order for union scan, if there is a union scan in inner task,
		// we can't construct index merge join.
		if !wrapper.hasDitryWrite {
			innerTask2 = p.constructInnerTableScanTask(wrapper, helper.chosenRanges.Range(), outerJoinKeys, rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt)
		}
		ranges = helper.chosenRanges
	} else {
		pkMatched := false
		pkCol := ds.getPKIsHandleCol()
		if pkCol == nil {
			return nil
		}
		for i, key := range innerJoinKeys {
			if !key.EqualColumn(pkCol) {
				keyOff2IdxOff[i] = -1
				continue
			}
			pkMatched = true
			keyOff2IdxOff[i] = 0
			// Add to newOuterJoinKeys only if conditions contain inner primary key. For issue #14822.
			newOuterJoinKeys = append(newOuterJoinKeys, outerJoinKeys[i])
		}
		outerJoinKeys = newOuterJoinKeys
		if !pkMatched {
			return nil
		}
		ranges := ranger.FullIntRange(mysql.HasUnsignedFlag(pkCol.RetType.GetFlag()))
		var buffer strings.Builder
		buffer.WriteString("[")
		for i, key := range outerJoinKeys {
			if i != 0 {
				buffer.WriteString(" ")
			}
			buffer.WriteString(key.String())
		}
		buffer.WriteString("]")
		rangeInfo := buffer.String()
		innerTask = p.constructInnerTableScanTask(wrapper, ranges, outerJoinKeys, rangeInfo, false, false, avgInnerRowCnt)
		// The index merge join's inner plan is different from index join, so we
		// should construct another inner plan for it.
		// Because we can't keep order for union scan, if there is a union scan in inner task,
		// we can't construct index merge join.
		if !wrapper.hasDitryWrite {
			innerTask2 = p.constructInnerTableScanTask(wrapper, ranges, outerJoinKeys, rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt)
		}
	}
	var (
		path       *util.AccessPath
		lastColMng *ColWithCmpFuncManager
	)
	if helper != nil {
		path = helper.chosenPath
		lastColMng = helper.lastColManager
	}
	joins = make([]base.PhysicalPlan, 0, 3)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
			failpoint.Return(p.constructIndexHashJoin(prop, outerIdx, innerTask, nil, keyOff2IdxOff, path, lastColMng))
		}
	})
	joins = append(joins, p.constructIndexJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, lastColMng, true)...)
	// We can reuse the `innerTask` here since index nested loop hash join
	// do not need the inner child to promise the order.
	joins = append(joins, p.constructIndexHashJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, lastColMng)...)
	if innerTask2 != nil {
		joins = append(joins, p.constructIndexMergeJoin(prop, outerIdx, innerTask2, ranges, keyOff2IdxOff, path, lastColMng)...)
	}
	return joins
}

func (p *LogicalJoin) buildIndexJoinInner2IndexScan(
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
		if !isMVIndexPath(path) {
			return true // not a MVIndex path, it can successfully be index join probe side.
		}
		return false
	}
	helper, keyOff2IdxOff := p.getIndexJoinBuildHelper(ds, innerJoinKeys, indexValid, outerJoinKeys)
	if helper == nil {
		return nil
	}
	joins = make([]base.PhysicalPlan, 0, 3)
	rangeInfo := helper.buildRangeDecidedByInformation(helper.chosenPath.IdxCols, outerJoinKeys)
	maxOneRow := false
	if helper.chosenPath.Index.Unique && helper.usedColsLen == len(helper.chosenPath.FullIdxCols) {
		l := len(helper.chosenAccess)
		if l == 0 {
			maxOneRow = true
		} else {
			sf, ok := helper.chosenAccess[l-1].(*expression.ScalarFunction)
			maxOneRow = ok && (sf.FuncName.L == ast.EQ)
		}
	}
	innerTask := p.constructInnerIndexScanTask(wrapper, helper.chosenPath, helper.chosenRanges.Range(), helper.chosenRemained, innerJoinKeys, helper.idxOff2KeyOff, rangeInfo, false, false, avgInnerRowCnt, maxOneRow)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL && innerTask != nil {
			failpoint.Return(p.constructIndexHashJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastColManager))
		}
	})
	if innerTask != nil {
		joins = append(joins, p.constructIndexJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastColManager, true)...)
		// We can reuse the `innerTask` here since index nested loop hash join
		// do not need the inner child to promise the order.
		joins = append(joins, p.constructIndexHashJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastColManager)...)
	}
	// The index merge join's inner plan is different from index join, so we
	// should construct another inner plan for it.
	// Because we can't keep order for union scan, if there is a union scan in inner task,
	// we can't construct index merge join.
	if !wrapper.hasDitryWrite {
		innerTask2 := p.constructInnerIndexScanTask(wrapper, helper.chosenPath, helper.chosenRanges.Range(), helper.chosenRemained, innerJoinKeys, helper.idxOff2KeyOff, rangeInfo, true, !prop.IsSortItemEmpty() && prop.SortItems[0].Desc, avgInnerRowCnt, maxOneRow)
		if innerTask2 != nil {
			joins = append(joins, p.constructIndexMergeJoin(prop, outerIdx, innerTask2, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastColManager)...)
		}
	}
	return joins
}

type indexJoinBuildHelper struct {
	join      *LogicalJoin
	innerPlan *DataSource

	usedColsLen    int
	usedColsNDV    float64
	chosenAccess   []expression.Expression
	chosenRemained []expression.Expression
	idxOff2KeyOff  []int
	lastColManager *ColWithCmpFuncManager
	chosenRanges   ranger.MutableRanges
	chosenPath     *util.AccessPath

	curPossibleUsedKeys []*expression.Column
	curNotUsedIndexCols []*expression.Column
	curNotUsedColLens   []int
	// Store the corresponding innerKeys offset of each column in the current path, reset by "resetContextForIndex()"
	curIdxOff2KeyOff []int
}

func (ijHelper *indexJoinBuildHelper) buildRangeDecidedByInformation(idxCols []*expression.Column, outerJoinKeys []*expression.Column) string {
	buffer := bytes.NewBufferString("[")
	isFirst := true
	for idxOff, keyOff := range ijHelper.idxOff2KeyOff {
		if keyOff == -1 {
			continue
		}
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		fmt.Fprintf(buffer, "eq(%v, %v)", idxCols[idxOff], outerJoinKeys[keyOff])
	}
	for _, access := range ijHelper.chosenAccess {
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		fmt.Fprintf(buffer, "%v", access)
	}
	buffer.WriteString("]")
	return buffer.String()
}

// constructInnerTableScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerTableScanTask(
	wrapper *indexJoinInnerChildWrapper,
	ranges ranger.Ranges,
	_ []*expression.Column,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
) base.Task {
	ds := wrapper.ds
	// If `ds.tableInfo.GetPartitionInfo() != nil`,
	// it means the data source is a partition table reader.
	// If the inner task need to keep order, the partition table reader can't satisfy it.
	if keepOrder && ds.tableInfo.GetPartitionInfo() != nil {
		return nil
	}
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		filterCondition: ds.pushedDownConds,
		Ranges:          ranges,
		rangeInfo:       rangeInfo,
		KeepOrder:       keepOrder,
		Desc:            desc,
		physicalTableID: ds.physicalTableID,
		isPartition:     ds.partitionDefIdx != nil,
		tblCols:         ds.TblCols,
		tblColHists:     ds.TblColHists,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	ts.SetSchema(ds.schema.Clone())
	if rowCount <= 0 {
		rowCount = float64(1)
	}
	selectivity := float64(1)
	countAfterAccess := rowCount
	if len(ts.filterCondition) > 0 {
		var err error
		selectivity, _, err = cardinality.Selectivity(ds.SCtx(), ds.tableStats.HistColl, ts.filterCondition, ds.possibleAccessPaths)
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
	usedStats := p.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(ts.physicalTableID) != nil {
		ts.usedStatsInfo = usedStats.GetUsedInfo(ts.physicalTableID)
	}
	copTask := &CopTask{
		tablePlan:         ts,
		indexPlanFinished: true,
		tblColHists:       ds.TblColHists,
		keepOrder:         ts.KeepOrder,
	}
	copTask.physPlanPartInfo = PhysPlanPartInfo{
		PruningConds:   ds.allConds,
		PartitionNames: ds.partitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.names,
	}
	ts.PlanPartInfo = copTask.physPlanPartInfo
	selStats := ts.StatsInfo().Scale(selectivity)
	ts.addPushedDownSelection(copTask, selStats)
	return p.constructIndexJoinInnerSideTask(copTask, ds, nil, wrapper)
}

func (p *LogicalJoin) constructInnerByZippedChildren(zippedChildren []base.LogicalPlan, child base.PhysicalPlan) base.PhysicalPlan {
	for i := len(zippedChildren) - 1; i >= 0; i-- {
		switch x := zippedChildren[i].(type) {
		case *LogicalUnionScan:
			child = p.constructInnerUnionScan(x, child)
		case *LogicalProjection:
			child = p.constructInnerProj(x, child)
		case *LogicalSelection:
			child = p.constructInnerSel(x, child)
		case *LogicalAggregation:
			child = p.constructInnerAgg(x, child)
		}
	}
	return child
}

func (*LogicalJoin) constructInnerAgg(logicalAgg *LogicalAggregation, child base.PhysicalPlan) base.PhysicalPlan {
	if logicalAgg == nil {
		return child
	}
	physicalHashAgg := NewPhysicalHashAgg(logicalAgg, logicalAgg.StatsInfo(), nil)
	physicalHashAgg.SetSchema(logicalAgg.schema.Clone())
	physicalHashAgg.SetChildren(child)
	return physicalHashAgg
}

func (*LogicalJoin) constructInnerSel(sel *LogicalSelection, child base.PhysicalPlan) base.PhysicalPlan {
	if sel == nil {
		return child
	}
	physicalSel := PhysicalSelection{
		Conditions: sel.Conditions,
	}.Init(sel.SCtx(), sel.StatsInfo(), sel.QueryBlockOffset(), nil)
	physicalSel.SetChildren(child)
	return physicalSel
}

func (*LogicalJoin) constructInnerProj(proj *LogicalProjection, child base.PhysicalPlan) base.PhysicalPlan {
	if proj == nil {
		return child
	}
	physicalProj := PhysicalProjection{
		Exprs:                proj.Exprs,
		CalculateNoDelay:     proj.CalculateNoDelay,
		AvoidColumnEvaluator: proj.AvoidColumnEvaluator,
	}.Init(proj.SCtx(), proj.StatsInfo(), proj.QueryBlockOffset(), nil)
	physicalProj.SetChildren(child)
	physicalProj.SetSchema(proj.schema)
	return physicalProj
}

func (*LogicalJoin) constructInnerUnionScan(us *LogicalUnionScan, reader base.PhysicalPlan) base.PhysicalPlan {
	if us == nil {
		return reader
	}
	// Use `reader.StatsInfo()` instead of `us.StatsInfo()` because it should be more accurate. No need to specify
	// childrenReqProps now since we have got reader already.
	physicalUnionScan := PhysicalUnionScan{
		Conditions: us.conditions,
		HandleCols: us.handleCols,
	}.Init(us.SCtx(), reader.StatsInfo(), us.QueryBlockOffset(), nil)
	physicalUnionScan.SetChildren(reader)
	return physicalUnionScan
}

// getColsNDVLowerBoundFromHistColl tries to get a lower bound of the NDV of columns (whose uniqueIDs are colUIDs).
func getColsNDVLowerBoundFromHistColl(colUIDs []int64, histColl *statistics.HistColl) int64 {
	if len(colUIDs) == 0 || histColl == nil {
		return -1
	}

	// 1. Try to get NDV from column stats if it's a single column.
	if len(colUIDs) == 1 && histColl.Columns != nil {
		uid := colUIDs[0]
		if colStats, ok := histColl.Columns[uid]; ok && colStats != nil && colStats.IsStatsInitialized() {
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
		if idxStats, ok := histColl.Indices[idxID]; ok && idxStats != nil && idxStats.IsStatsInitialized() {
			return idxStats.NDV
		}
	}

	// TODO: if there's an index that contains the expected columns, we can also make use of its NDV.
	// For example, NDV(a,b,c) / NDV(c) is a safe lower bound of NDV(a,b).

	// 3. If we still haven't got an NDV, we use the maximum NDV in the column stats as a lower bound.
	maxNDV := int64(-1)
	for _, uid := range colUIDs {
		colStats := histColl.Columns[uid]
		if colStats == nil || !colStats.IsStatsInitialized() {
			continue
		}
		maxNDV = max(maxNDV, colStats.NDV)
	}
	return maxNDV
}

// constructInnerIndexScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerIndexScanTask(
	wrapper *indexJoinInnerChildWrapper,
	path *util.AccessPath,
	ranges ranger.Ranges,
	filterConds []expression.Expression,
	_ []*expression.Column,
	idxOffset2joinKeyOffset []int,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
	maxOneRow bool,
) base.Task {
	ds := wrapper.ds
	// If `ds.tableInfo.GetPartitionInfo() != nil`,
	// it means the data source is a partition table reader.
	// If the inner task need to keep order, the partition table reader can't satisfy it.
	if keepOrder && ds.tableInfo.GetPartitionInfo() != nil {
		return nil
	}
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            path.Index,
		IdxCols:          path.IdxCols,
		IdxColLens:       path.IdxColLens,
		dataSourceSchema: ds.schema,
		KeepOrder:        keepOrder,
		Ranges:           ranges,
		rangeInfo:        rangeInfo,
		Desc:             desc,
		isPartition:      ds.partitionDefIdx != nil,
		physicalTableID:  ds.physicalTableID,
		tblColHists:      ds.TblColHists,
		pkIsHandleCol:    ds.getPKIsHandleCol(),
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	cop := &CopTask{
		indexPlan:   is,
		tblColHists: ds.TblColHists,
		tblCols:     ds.TblCols,
		keepOrder:   is.KeepOrder,
	}
	cop.physPlanPartInfo = PhysPlanPartInfo{
		PruningConds:   ds.allConds,
		PartitionNames: ds.partitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.names,
	}
	if !path.IsSingleScan {
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			TableAsName:     ds.TableAsName,
			DBName:          ds.DBName,
			isPartition:     ds.partitionDefIdx != nil,
			physicalTableID: ds.physicalTableID,
			tblCols:         ds.TblCols,
			tblColHists:     ds.TblColHists,
		}.Init(ds.SCtx(), ds.QueryBlockOffset())
		ts.schema = is.dataSourceSchema.Clone()
		if ds.tableInfo.IsCommonHandle {
			commonHandle := ds.handleCols.(*util.CommonHandleCols)
			for _, col := range commonHandle.GetColumns() {
				if ts.schema.ColumnIndex(col) == -1 {
					ts.Schema().Append(col)
					ts.Columns = append(ts.Columns, col.ToInfo())
					cop.needExtraProj = true
				}
			}
		}
		// We set `StatsVersion` here and fill other fields in `(*copTask).finishIndexPlan`. Since `copTask.indexPlan` may
		// change before calling `(*copTask).finishIndexPlan`, we don't know the stats information of `ts` currently and on
		// the other hand, it may be hard to identify `StatsVersion` of `ts` in `(*copTask).finishIndexPlan`.
		ts.SetStats(&property.StatsInfo{StatsVersion: ds.tableStats.StatsVersion})
		usedStats := p.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
		if usedStats != nil && usedStats.GetUsedInfo(ts.physicalTableID) != nil {
			ts.usedStatsInfo = usedStats.GetUsedInfo(ts.physicalTableID)
		}
		// If inner cop task need keep order, the extraHandleCol should be set.
		if cop.keepOrder && !ds.tableInfo.IsCommonHandle {
			var needExtraProj bool
			cop.extraHandleCol, needExtraProj = ts.appendExtraHandleCol(ds)
			cop.needExtraProj = cop.needExtraProj || needExtraProj
		}
		if cop.needExtraProj {
			cop.originSchema = ds.schema
		}
		cop.tablePlan = ts
	}
	if cop.tablePlan != nil && ds.tableInfo.IsCommonHandle {
		cop.commonHandleCols = ds.commonHandleCols
	}
	is.initSchema(append(path.FullIdxCols, ds.commonHandleCols...), cop.tablePlan != nil)
	indexConds, tblConds := ds.splitIndexFilterConditions(filterConds, path.FullIdxCols, path.FullIdxColLens)

	// Note: due to a regression in JOB workload, we use the optimizer fix control to enable this for now.
	//
	// Because we are estimating an average row count of the inner side corresponding to each row from the outer side,
	// the estimated row count of the IndexScan should be no larger than (total row count / NDV of join key columns).
	// We can calculate the lower bound of the NDV therefore we can get an upper bound of the row count here.
	rowCountUpperBound := -1.0
	fixControlOK := fixcontrol.GetBoolWithDefault(ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix44855, false)
	if fixControlOK && ds.tableStats != nil {
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
		joinKeyNDV := getColsNDVLowerBoundFromHistColl(usedColIDs, ds.tableStats.HistColl)
		if joinKeyNDV > 0 {
			rowCountUpperBound = ds.tableStats.RowCount / float64(joinKeyNDV)
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
		IndexFilters:     indexConds,
		TableFilters:     tblConds,
		CountAfterIndex:  rowCount,
		CountAfterAccess: rowCount,
	}
	// Assume equal conditions used by index join and other conditions are independent.
	if len(tblConds) > 0 {
		selectivity, _, err := cardinality.Selectivity(ds.SCtx(), ds.tableStats.HistColl, tblConds, ds.possibleAccessPaths)
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
		selectivity, _, err := cardinality.Selectivity(ds.SCtx(), ds.tableStats.HistColl, indexConds, ds.possibleAccessPaths)
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
	is.SetStats(ds.tableStats.ScaleByExpectCnt(tmpPath.CountAfterAccess))
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(is.physicalTableID) != nil {
		is.usedStatsInfo = usedStats.GetUsedInfo(is.physicalTableID)
	}
	finalStats := ds.tableStats.ScaleByExpectCnt(rowCount)
	if err := is.addPushedDownSelection(cop, ds, tmpPath, finalStats); err != nil {
		logutil.BgLogger().Warn("unexpected error happened during addPushedDownSelection function", zap.Error(err))
		return nil
	}
	return p.constructIndexJoinInnerSideTask(cop, ds, path, wrapper)
}

// construct the inner join task by inner child plan tree
// The Logical include two parts: logicalplan->physicalplan, physicalplan->task
// Step1: whether agg can be pushed down to coprocessor
//
//	Step1.1: If the agg can be pushded down to coprocessor, we will build a copTask and attach the agg to the copTask
//	There are two kinds of agg: stream agg and hash agg. Stream agg depends on some conditions, such as the group by cols
//
// Step2: build other inner plan node to task
func (p *LogicalJoin) constructIndexJoinInnerSideTask(dsCopTask *CopTask, ds *DataSource, path *util.AccessPath, wrapper *indexJoinInnerChildWrapper) base.Task {
	var la *LogicalAggregation
	var canPushAggToCop bool
	if len(wrapper.zippedChildren) > 0 {
		la, canPushAggToCop = wrapper.zippedChildren[len(wrapper.zippedChildren)-1].(*LogicalAggregation)
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
		result.SetPlan(p.constructInnerByZippedChildren(wrapper.zippedChildren, result.GetPlan()))
		return result
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
	if la.HasDistinct() && !la.distinctArgsMeetsProperty() {
		preferStream = false
	}
	// sort items must be the super set of group by items
	if path != nil && path.Index != nil && !path.Index.MVIndex &&
		ds.tableInfo.GetPartitionInfo() == nil {
		if len(path.IdxCols) < len(groupByCols) {
			preferStream = false
		}
		sctx := p.SCtx()
		for i, groupbyCol := range groupByCols {
			if path.IdxColLens[i] != types.UnspecifiedLength ||
				!groupbyCol.EqualByExprAndID(sctx.GetExprCtx().GetEvalCtx(), path.IdxCols[i]) {
				preferStream = false
			}
		}
	} else {
		preferStream = false
	}

	// build physical agg and attach to task
	var aggTask base.Task
	// build stream agg and change ds keep order to true
	if preferStream {
		newGbyItems := make([]expression.Expression, len(la.GroupByItems))
		copy(newGbyItems, la.GroupByItems)
		newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
		copy(newAggFuncs, la.AggFuncs)
		streamAgg := basePhysicalAgg{
			GroupByItems: newGbyItems,
			AggFuncs:     newAggFuncs,
		}.initForStream(la.SCtx(), la.StatsInfo(), la.QueryBlockOffset(), nil)
		streamAgg.SetSchema(la.schema.Clone())
		// change to keep order for index scan and dsCopTask
		if dsCopTask.indexPlan != nil {
			// get the index scan from dsCopTask.indexPlan
			physicalIndexScan, _ := dsCopTask.indexPlan.(*PhysicalIndexScan)
			if physicalIndexScan == nil && len(dsCopTask.indexPlan.Children()) == 1 {
				physicalIndexScan, _ = dsCopTask.indexPlan.Children()[0].(*PhysicalIndexScan)
			}
			if physicalIndexScan != nil {
				physicalIndexScan.KeepOrder = true
				dsCopTask.keepOrder = true
				aggTask = streamAgg.Attach2Task(dsCopTask)
			}
		}
	}

	// build hash agg, when the stream agg is illegal such as the order by prop is not matched
	if aggTask == nil {
		physicalHashAgg := NewPhysicalHashAgg(la, la.StatsInfo(), nil)
		physicalHashAgg.SetSchema(la.schema.Clone())
		aggTask = physicalHashAgg.Attach2Task(dsCopTask)
	}

	// build other inner plan node to task
	result, ok := aggTask.(*RootTask)
	if !ok {
		return nil
	}
	result.SetPlan(p.constructInnerByZippedChildren(wrapper.zippedChildren[0:len(wrapper.zippedChildren)-1], result.p))
	return result
}

var symmetricOp = map[string]string{
	ast.LT: ast.GT,
	ast.GE: ast.LE,
	ast.GT: ast.LT,
	ast.LE: ast.GE,
}

// ColWithCmpFuncManager is used in index join to handle the column with compare functions(>=, >, <, <=).
// It stores the compare functions and build ranges in execution phase.
type ColWithCmpFuncManager struct {
	TargetCol         *expression.Column
	colLength         int
	OpType            []string
	opArg             []expression.Expression
	TmpConstant       []*expression.Constant
	affectedColSchema *expression.Schema
	compareFuncs      []chunk.CompareFunc
}

func (cwc *ColWithCmpFuncManager) appendNewExpr(opName string, arg expression.Expression, affectedCols []*expression.Column) {
	cwc.OpType = append(cwc.OpType, opName)
	cwc.opArg = append(cwc.opArg, arg)
	cwc.TmpConstant = append(cwc.TmpConstant, &expression.Constant{RetType: cwc.TargetCol.RetType})
	for _, col := range affectedCols {
		if cwc.affectedColSchema.Contains(col) {
			continue
		}
		cwc.compareFuncs = append(cwc.compareFuncs, chunk.GetCompareFunc(col.RetType))
		cwc.affectedColSchema.Append(col)
	}
}

// CompareRow compares the rows for deduplicate.
func (cwc *ColWithCmpFuncManager) CompareRow(lhs, rhs chunk.Row) int {
	for i, col := range cwc.affectedColSchema.Columns {
		ret := cwc.compareFuncs[i](lhs, col.Index, rhs, col.Index)
		if ret != 0 {
			return ret
		}
	}
	return 0
}

// BuildRangesByRow will build range of the given row. It will eval each function's arg then call BuildRange.
func (cwc *ColWithCmpFuncManager) BuildRangesByRow(ctx *rangerctx.RangerContext, row chunk.Row) ([]*ranger.Range, error) {
	exprs := make([]expression.Expression, len(cwc.OpType))
	exprCtx := ctx.ExprCtx
	for i, opType := range cwc.OpType {
		constantArg, err := cwc.opArg[i].Eval(exprCtx.GetEvalCtx(), row)
		if err != nil {
			return nil, err
		}
		cwc.TmpConstant[i].Value = constantArg
		newExpr, err := expression.NewFunction(exprCtx, opType, types.NewFieldType(mysql.TypeTiny), cwc.TargetCol, cwc.TmpConstant[i])
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, newExpr) // nozero
	}
	// We already limit range mem usage when buildTemplateRange for inner table of IndexJoin in optimizer phase, so we
	// don't need and shouldn't limit range mem usage when we refill inner ranges during the execution phase.
	ranges, _, _, err := ranger.BuildColumnRange(exprs, ctx, cwc.TargetCol.RetType, cwc.colLength, 0)
	if err != nil {
		return nil, err
	}
	return ranges, nil
}

func (cwc *ColWithCmpFuncManager) resolveIndices(schema *expression.Schema) (err error) {
	for i := range cwc.opArg {
		cwc.opArg[i], err = cwc.opArg[i].ResolveIndices(schema)
		if err != nil {
			return err
		}
	}
	return nil
}

// String implements Stringer interface.
func (cwc *ColWithCmpFuncManager) String() string {
	buffer := bytes.NewBufferString("")
	for i := range cwc.OpType {
		fmt.Fprintf(buffer, "%v(%v, %v)", cwc.OpType[i], cwc.TargetCol, cwc.opArg[i])
		if i < len(cwc.OpType)-1 {
			buffer.WriteString(" ")
		}
	}
	return buffer.String()
}

const emptyColWithCmpFuncManagerSize = int64(unsafe.Sizeof(ColWithCmpFuncManager{}))

// MemoryUsage return the memory usage of ColWithCmpFuncManager
func (cwc *ColWithCmpFuncManager) MemoryUsage() (sum int64) {
	if cwc == nil {
		return
	}

	sum = emptyColWithCmpFuncManagerSize + int64(cap(cwc.compareFuncs))*size.SizeOfFunc
	if cwc.TargetCol != nil {
		sum += cwc.TargetCol.MemoryUsage()
	}
	if cwc.affectedColSchema != nil {
		sum += cwc.affectedColSchema.MemoryUsage()
	}

	for _, str := range cwc.OpType {
		sum += int64(len(str))
	}
	for _, expr := range cwc.opArg {
		sum += expr.MemoryUsage()
	}
	for _, cst := range cwc.TmpConstant {
		sum += cst.MemoryUsage()
	}
	return
}

// Reset the 'curIdxOff2KeyOff', 'curNotUsedIndexCols' and 'curNotUsedColLens' by innerKeys and idxCols
/*
For each idxCols,
  If column can be found in innerKeys
	save offset of innerKeys in 'curIdxOff2KeyOff'
  Else,
	save -1 in 'curIdxOff2KeyOff'
*/
// For example, innerKeys[t1.a, t1.sum_b, t1.c], idxCols [a, b, c]
// 'curIdxOff2KeyOff' = [0, -1, 2]
func (ijHelper *indexJoinBuildHelper) resetContextForIndex(innerKeys []*expression.Column, idxCols []*expression.Column, colLens []int, outerKeys []*expression.Column) {
	tmpSchema := expression.NewSchema(innerKeys...)
	ijHelper.curIdxOff2KeyOff = make([]int, len(idxCols))
	ijHelper.curNotUsedIndexCols = make([]*expression.Column, 0, len(idxCols))
	ijHelper.curNotUsedColLens = make([]int, 0, len(idxCols))
	for i, idxCol := range idxCols {
		ijHelper.curIdxOff2KeyOff[i] = tmpSchema.ColumnIndex(idxCol)
		if ijHelper.curIdxOff2KeyOff[i] >= 0 {
			// Don't use the join columns if their collations are unmatched and the new collation is enabled.
			if collate.NewCollationEnabled() && types.IsString(idxCol.RetType.GetType()) && types.IsString(outerKeys[ijHelper.curIdxOff2KeyOff[i]].RetType.GetType()) {
				et, err := expression.CheckAndDeriveCollationFromExprs(ijHelper.innerPlan.SCtx().GetExprCtx(), "equal", types.ETInt, idxCol, outerKeys[ijHelper.curIdxOff2KeyOff[i]])
				if err != nil {
					logutil.BgLogger().Error("Unexpected error happened during constructing index join", zap.Stack("stack"))
				}
				if !collate.CompatibleCollate(idxCol.GetStaticType().GetCollate(), et.Collation) {
					ijHelper.curIdxOff2KeyOff[i] = -1
				}
			}
			continue
		}
		ijHelper.curNotUsedIndexCols = append(ijHelper.curNotUsedIndexCols, idxCol)
		ijHelper.curNotUsedColLens = append(ijHelper.curNotUsedColLens, colLens[i])
	}
}

// findUsefulEqAndInFilters analyzes the pushedDownConds held by inner child and split them to three parts.
// usefulEqOrInFilters is the continuous eq/in conditions on current unused index columns.
// remainedEqOrIn is part of usefulEqOrInFilters, which needs to be evaluated again in selection.
// remainingRangeCandidates is the other conditions for future use.
func (ijHelper *indexJoinBuildHelper) findUsefulEqAndInFilters(innerPlan *DataSource) (usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates []expression.Expression, emptyRange bool) {
	// Extract the eq/in functions of possible join key.
	// you can see the comment of ExtractEqAndInCondition to get the meaning of the second return value.
	usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, _, emptyRange = ranger.ExtractEqAndInCondition(
		innerPlan.SCtx().GetRangerCtx(), innerPlan.pushedDownConds,
		ijHelper.curNotUsedIndexCols,
		ijHelper.curNotUsedColLens,
	)
	return usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, emptyRange
}

// buildLastColManager analyze the `OtherConditions` of join to see whether there're some filters can be used in manager.
// The returned value is just for outputting explain information
func (ijHelper *indexJoinBuildHelper) buildLastColManager(nextCol *expression.Column,
	innerPlan *DataSource, cwc *ColWithCmpFuncManager) []expression.Expression {
	var lastColAccesses []expression.Expression
loopOtherConds:
	for _, filter := range ijHelper.join.OtherConditions {
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok || !(sf.FuncName.L == ast.LE || sf.FuncName.L == ast.LT || sf.FuncName.L == ast.GE || sf.FuncName.L == ast.GT) {
			continue
		}
		var funcName string
		var anotherArg expression.Expression
		if lCol, ok := sf.GetArgs()[0].(*expression.Column); ok && lCol.EqualColumn(nextCol) {
			anotherArg = sf.GetArgs()[1]
			funcName = sf.FuncName.L
		} else if rCol, ok := sf.GetArgs()[1].(*expression.Column); ok && rCol.EqualColumn(nextCol) {
			anotherArg = sf.GetArgs()[0]
			// The column manager always build expression in the form of col op arg1.
			// So we need use the symmetric one of the current function.
			funcName = symmetricOp[sf.FuncName.L]
		} else {
			continue
		}
		affectedCols := expression.ExtractColumns(anotherArg)
		if len(affectedCols) == 0 {
			continue
		}
		for _, col := range affectedCols {
			if innerPlan.schema.Contains(col) {
				continue loopOtherConds
			}
		}
		lastColAccesses = append(lastColAccesses, sf)
		cwc.appendNewExpr(funcName, anotherArg, affectedCols)
	}
	return lastColAccesses
}

// removeUselessEqAndInFunc removes the useless eq/in conditions. It's designed for the following case:
//
//	t1 join t2 on t1.a=t2.a and t1.c=t2.c where t1.b > t2.b-10 and t1.b < t2.b+10 there's index(a, b, c) on t1.
//	In this case the curIdxOff2KeyOff is [0 -1 1] and the notKeyEqAndIn is [].
//	It's clearly that the column c cannot be used to access data. So we need to remove it and reset the IdxOff2KeyOff to
//	[0 -1 -1].
//	So that we can use t1.a=t2.a and t1.b > t2.b-10 and t1.b < t2.b+10 to build ranges then access data.
func (ijHelper *indexJoinBuildHelper) removeUselessEqAndInFunc(idxCols []*expression.Column, notKeyEqAndIn []expression.Expression) (usefulEqAndIn, uselessOnes []expression.Expression) {
	ijHelper.curPossibleUsedKeys = make([]*expression.Column, 0, len(idxCols))
	for idxColPos, notKeyColPos := 0, 0; idxColPos < len(idxCols); idxColPos++ {
		if ijHelper.curIdxOff2KeyOff[idxColPos] != -1 {
			ijHelper.curPossibleUsedKeys = append(ijHelper.curPossibleUsedKeys, idxCols[idxColPos])
			continue
		}
		if notKeyColPos < len(notKeyEqAndIn) && ijHelper.curNotUsedIndexCols[notKeyColPos].EqualColumn(idxCols[idxColPos]) {
			notKeyColPos++
			continue
		}
		for i := idxColPos + 1; i < len(idxCols); i++ {
			ijHelper.curIdxOff2KeyOff[i] = -1
		}
		remained := make([]expression.Expression, 0, len(notKeyEqAndIn)-notKeyColPos)
		remained = append(remained, notKeyEqAndIn[notKeyColPos:]...)
		notKeyEqAndIn = notKeyEqAndIn[:notKeyColPos]
		return notKeyEqAndIn, remained
	}
	return notKeyEqAndIn, nil
}

type mutableIndexJoinRange struct {
	ranges ranger.Ranges

	buildHelper   *indexJoinBuildHelper
	path          *util.AccessPath
	innerJoinKeys []*expression.Column
	outerJoinKeys []*expression.Column
}

func (mr *mutableIndexJoinRange) Range() ranger.Ranges {
	return mr.ranges
}

func (mr *mutableIndexJoinRange) Rebuild() error {
	empty, err := mr.buildHelper.analyzeLookUpFilters(mr.path, mr.buildHelper.innerPlan, mr.innerJoinKeys, mr.outerJoinKeys, true)
	if err != nil {
		return err
	}
	if empty { // empty ranges are dangerous for plan-cache, it's better to optimize the whole plan again in this case
		return errors.New("failed to rebuild range: empty range")
	}
	newRanges := mr.buildHelper.chosenRanges.Range()
	if len(mr.ranges) != len(newRanges) || (len(mr.ranges) > 0 && mr.ranges[0].Width() != newRanges[0].Width()) {
		// some access conditions cannot be used to calculate the range after parameters change, return an error in this case for safety.
		return errors.New("failed to rebuild range: range width changed")
	}
	mr.ranges = mr.buildHelper.chosenRanges.Range()
	return nil
}

func (ijHelper *indexJoinBuildHelper) createMutableIndexJoinRange(relatedExprs []expression.Expression, ranges []*ranger.Range, path *util.AccessPath, innerKeys, outerKeys []*expression.Column) ranger.MutableRanges {
	// if the plan-cache is enabled and these ranges depend on some parameters, we have to rebuild these ranges after changing parameters
	if expression.MaybeOverOptimized4PlanCache(ijHelper.join.SCtx().GetExprCtx(), relatedExprs) {
		// assume that path, innerKeys and outerKeys will not be modified in the follow-up process
		return &mutableIndexJoinRange{
			ranges:        ranges,
			buildHelper:   &indexJoinBuildHelper{innerPlan: ijHelper.innerPlan, join: ijHelper.join},
			path:          path,
			innerJoinKeys: innerKeys,
			outerJoinKeys: outerKeys,
		}
	}
	return ranger.Ranges(ranges)
}

func (ijHelper *indexJoinBuildHelper) updateByTemplateRangeResult(tempRangeRes *templateRangeResult,
	accesses, remained []expression.Expression) (lastColPos int, newAccesses, newRemained []expression.Expression) {
	lastColPos = tempRangeRes.keyCntInRange + tempRangeRes.eqAndInCntInRange
	ijHelper.curPossibleUsedKeys = ijHelper.curPossibleUsedKeys[:tempRangeRes.keyCntInRange]
	for i := lastColPos; i < len(ijHelper.curIdxOff2KeyOff); i++ {
		ijHelper.curIdxOff2KeyOff[i] = -1
	}
	newAccesses = accesses[:tempRangeRes.eqAndInCntInRange]
	newRemained = ranger.AppendConditionsIfNotExist(remained, accesses[tempRangeRes.eqAndInCntInRange:])
	return
}

func (ijHelper *indexJoinBuildHelper) analyzeLookUpFilters(path *util.AccessPath, innerPlan *DataSource, innerJoinKeys []*expression.Column, outerJoinKeys []*expression.Column, rebuildMode bool) (emptyRange bool, err error) {
	if len(path.IdxCols) == 0 {
		return false, nil
	}
	accesses := make([]expression.Expression, 0, len(path.IdxCols))
	ijHelper.resetContextForIndex(innerJoinKeys, path.IdxCols, path.IdxColLens, outerJoinKeys)
	notKeyEqAndIn, remained, rangeFilterCandidates, emptyRange := ijHelper.findUsefulEqAndInFilters(innerPlan)
	if emptyRange {
		return true, nil
	}
	var remainedEqAndIn []expression.Expression
	notKeyEqAndIn, remainedEqAndIn = ijHelper.removeUselessEqAndInFunc(path.IdxCols, notKeyEqAndIn)
	matchedKeyCnt := len(ijHelper.curPossibleUsedKeys)
	// If no join key is matched while join keys actually are not empty. We don't choose index join for now.
	if matchedKeyCnt <= 0 && len(innerJoinKeys) > 0 {
		return false, nil
	}
	accesses = append(accesses, notKeyEqAndIn...)
	remained = ranger.AppendConditionsIfNotExist(remained, remainedEqAndIn)
	lastColPos := matchedKeyCnt + len(notKeyEqAndIn)
	// There should be some equal conditions. But we don't need that there must be some join key in accesses here.
	// A more strict check is applied later.
	if lastColPos <= 0 {
		return false, nil
	}
	rangeMaxSize := ijHelper.join.SCtx().GetSessionVars().RangeMaxSize
	if rebuildMode {
		// When rebuilding ranges for plan cache, we don't restrict range mem limit.
		rangeMaxSize = 0
	}
	// If all the index columns are covered by eq/in conditions, we don't need to consider other conditions anymore.
	if lastColPos == len(path.IdxCols) {
		// If there's no join key matching index column, then choosing hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1. And t2 has index(a, b).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		remained = append(remained, rangeFilterCandidates...)
		tempRangeRes := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, false, rangeMaxSize)
		if tempRangeRes.err != nil || tempRangeRes.emptyRange || tempRangeRes.keyCntInRange <= 0 {
			return tempRangeRes.emptyRange, tempRangeRes.err
		}
		lastColPos, accesses, remained = ijHelper.updateByTemplateRangeResult(tempRangeRes, accesses, remained)
		mutableRange := ijHelper.createMutableIndexJoinRange(accesses, tempRangeRes.ranges, path, innerJoinKeys, outerJoinKeys)
		ijHelper.updateBestChoice(mutableRange, path, accesses, remained, nil, lastColPos, rebuildMode)
		return false, nil
	}
	lastPossibleCol := path.IdxCols[lastColPos]
	lastColManager := &ColWithCmpFuncManager{
		TargetCol:         lastPossibleCol,
		colLength:         path.IdxColLens[lastColPos],
		affectedColSchema: expression.NewSchema(),
	}
	lastColAccess := ijHelper.buildLastColManager(lastPossibleCol, innerPlan, lastColManager)
	// If the column manager holds no expression, then we fallback to find whether there're useful normal filters
	if len(lastColAccess) == 0 {
		// If there's no join key matching index column, then choosing hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1 and t2.c > 10 and t2.c < 20. And t2 has index(a, b, c).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		colAccesses, colRemained := ranger.DetachCondsForColumn(ijHelper.join.SCtx().GetRangerCtx(), rangeFilterCandidates, lastPossibleCol)
		var nextColRange []*ranger.Range
		var err error
		if len(colAccesses) > 0 {
			var colRemained2 []expression.Expression
			nextColRange, colAccesses, colRemained2, err = ranger.BuildColumnRange(colAccesses, ijHelper.join.SCtx().GetRangerCtx(), lastPossibleCol.RetType, path.IdxColLens[lastColPos], rangeMaxSize)
			if err != nil {
				return false, err
			}
			if len(colRemained2) > 0 {
				colRemained = append(colRemained, colRemained2...)
				nextColRange = nil
			}
		}
		tempRangeRes := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nextColRange, false, rangeMaxSize)
		if tempRangeRes.err != nil || tempRangeRes.emptyRange || tempRangeRes.keyCntInRange <= 0 {
			return tempRangeRes.emptyRange, tempRangeRes.err
		}
		lastColPos, accesses, remained = ijHelper.updateByTemplateRangeResult(tempRangeRes, accesses, remained)
		// update accesses and remained by colAccesses and colRemained.
		remained = append(remained, colRemained...)
		if tempRangeRes.nextColInRange {
			if path.IdxColLens[lastColPos] != types.UnspecifiedLength {
				remained = append(remained, colAccesses...)
			}
			accesses = append(accesses, colAccesses...)
			lastColPos = lastColPos + 1
		} else {
			remained = append(remained, colAccesses...)
		}
		mutableRange := ijHelper.createMutableIndexJoinRange(accesses, tempRangeRes.ranges, path, innerJoinKeys, outerJoinKeys)
		ijHelper.updateBestChoice(mutableRange, path, accesses, remained, nil, lastColPos, rebuildMode)
		return false, nil
	}
	tempRangeRes := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, true, rangeMaxSize)
	if tempRangeRes.err != nil || tempRangeRes.emptyRange {
		return tempRangeRes.emptyRange, tempRangeRes.err
	}
	lastColPos, accesses, remained = ijHelper.updateByTemplateRangeResult(tempRangeRes, accesses, remained)

	remained = append(remained, rangeFilterCandidates...)
	if tempRangeRes.extraColInRange {
		accesses = append(accesses, lastColAccess...)
		lastColPos = lastColPos + 1
	} else {
		if tempRangeRes.keyCntInRange <= 0 {
			return false, nil
		}
		lastColManager = nil
	}
	mutableRange := ijHelper.createMutableIndexJoinRange(accesses, tempRangeRes.ranges, path, innerJoinKeys, outerJoinKeys)
	ijHelper.updateBestChoice(mutableRange, path, accesses, remained, lastColManager, lastColPos, rebuildMode)
	return false, nil
}

func (ijHelper *indexJoinBuildHelper) updateBestChoice(ranges ranger.MutableRanges, path *util.AccessPath, accesses,
	remained []expression.Expression, lastColManager *ColWithCmpFuncManager, usedColsLen int, rebuildMode bool) {
	if rebuildMode { // rebuild the range for plan-cache, update the chosenRanges anyway
		ijHelper.chosenPath = path
		ijHelper.chosenRanges = ranges
		ijHelper.chosenAccess = accesses
		ijHelper.idxOff2KeyOff = ijHelper.curIdxOff2KeyOff
		return
	}

	// Notice that there may be the cases like `t1.a = t2.a and b > 2 and b < 1`, so ranges can be nil though the conditions are valid.
	// Obviously when the range is nil, we don't need index join.
	if len(ranges.Range()) == 0 {
		return
	}
	var innerNDV float64
	if stats := ijHelper.innerPlan.StatsInfo(); stats != nil && stats.StatsVersion != statistics.PseudoVersion {
		innerNDV, _ = cardinality.EstimateColsNDVWithMatchedLen(path.IdxCols[:usedColsLen], ijHelper.innerPlan.Schema(), stats)
	}
	// We choose the index by the NDV of the used columns, the larger the better.
	// If NDVs are same, we choose index which uses more columns.
	// Note that these 2 heuristic rules are too simple to cover all cases,
	// since the NDV of outer join keys are not considered, and the detached access conditions
	// may contain expressions like `t1.a > t2.a`. It's pretty hard to evaluate the join selectivity
	// of these non-column-equal conditions, so I prefer to keep these heuristic rules simple at least for now.
	if innerNDV < ijHelper.usedColsNDV || (innerNDV == ijHelper.usedColsNDV && usedColsLen <= ijHelper.usedColsLen) {
		return
	}
	ijHelper.chosenPath = path
	ijHelper.usedColsLen = len(ranges.Range()[0].LowVal)
	ijHelper.usedColsNDV = innerNDV
	ijHelper.chosenRanges = ranges
	ijHelper.chosenAccess = accesses
	ijHelper.chosenRemained = remained
	ijHelper.idxOff2KeyOff = ijHelper.curIdxOff2KeyOff
	ijHelper.lastColManager = lastColManager
}

type templateRangeResult struct {
	ranges            ranger.Ranges
	emptyRange        bool
	keyCntInRange     int
	eqAndInCntInRange int
	nextColInRange    bool
	extraColInRange   bool
	err               error
}

// appendTailTemplateRange appends empty datum for each range in originRanges.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit.
// If the second return value is true, it means that the estimated memory after appending datums to originRanges exceeds
// rangeMaxSize and the function rejects appending datums to originRanges.
func appendTailTemplateRange(originRanges ranger.Ranges, rangeMaxSize int64) (ranger.Ranges, bool) {
	if rangeMaxSize > 0 && originRanges.MemUsage()+(types.EmptyDatumSize*2+16)*int64(len(originRanges)) > rangeMaxSize {
		return originRanges, true
	}
	for _, ran := range originRanges {
		ran.LowVal = append(ran.LowVal, types.Datum{})
		ran.HighVal = append(ran.HighVal, types.Datum{})
		ran.Collators = append(ran.Collators, nil)
	}
	return originRanges, false
}

func (ijHelper *indexJoinBuildHelper) buildTemplateRange(matchedKeyCnt int, eqAndInFuncs []expression.Expression, nextColRange []*ranger.Range,
	haveExtraCol bool, rangeMaxSize int64) (res *templateRangeResult) {
	res = &templateRangeResult{}
	ctx := ijHelper.join.SCtx()
	sc := ctx.GetSessionVars().StmtCtx
	defer func() {
		if sc.MemTracker != nil && res != nil && len(res.ranges) > 0 {
			sc.MemTracker.Consume(2 * types.EstimatedMemUsage(res.ranges[0].LowVal, len(res.ranges)))
		}
	}()
	pointLength := matchedKeyCnt + len(eqAndInFuncs)
	ranges := ranger.Ranges{&ranger.Range{}}
	for i, j := 0, 0; i+j < pointLength; {
		if ijHelper.curIdxOff2KeyOff[i+j] != -1 {
			// This position is occupied by join key.
			var fallback bool
			ranges, fallback = appendTailTemplateRange(ranges, rangeMaxSize)
			if fallback {
				ctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
				res.ranges = ranges
				res.keyCntInRange = i
				res.eqAndInCntInRange = j
				return
			}
			i++
		} else {
			exprs := []expression.Expression{eqAndInFuncs[j]}
			oneColumnRan, _, remained, err := ranger.BuildColumnRange(exprs, ijHelper.join.SCtx().GetRangerCtx(), ijHelper.curNotUsedIndexCols[j].RetType, ijHelper.curNotUsedColLens[j], rangeMaxSize)
			if err != nil {
				return &templateRangeResult{err: err}
			}
			if len(oneColumnRan) == 0 {
				return &templateRangeResult{emptyRange: true}
			}
			if sc.MemTracker != nil {
				sc.MemTracker.Consume(2 * types.EstimatedMemUsage(oneColumnRan[0].LowVal, len(oneColumnRan)))
			}
			if len(remained) > 0 {
				res.ranges = ranges
				res.keyCntInRange = i
				res.eqAndInCntInRange = j
				return
			}
			var fallback bool
			ranges, fallback = ranger.AppendRanges2PointRanges(ranges, oneColumnRan, rangeMaxSize)
			if fallback {
				ctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
				res.ranges = ranges
				res.keyCntInRange = i
				res.eqAndInCntInRange = j
				return
			}
			j++
		}
	}
	if len(nextColRange) > 0 {
		var fallback bool
		ranges, fallback = ranger.AppendRanges2PointRanges(ranges, nextColRange, rangeMaxSize)
		if fallback {
			ctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
		}
		res.ranges = ranges
		res.keyCntInRange = matchedKeyCnt
		res.eqAndInCntInRange = len(eqAndInFuncs)
		res.nextColInRange = !fallback
		return
	}
	if haveExtraCol {
		var fallback bool
		ranges, fallback = appendTailTemplateRange(ranges, rangeMaxSize)
		if fallback {
			ctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
		}
		res.ranges = ranges
		res.keyCntInRange = matchedKeyCnt
		res.eqAndInCntInRange = len(eqAndInFuncs)
		res.extraColInRange = !fallback
		return
	}
	res.ranges = ranges
	res.keyCntInRange = matchedKeyCnt
	res.eqAndInCntInRange = len(eqAndInFuncs)
	return
}

func filterIndexJoinBySessionVars(sc base.PlanContext, indexJoins []base.PhysicalPlan) []base.PhysicalPlan {
	if sc.GetSessionVars().EnableIndexMergeJoin {
		return indexJoins
	}
	for i := len(indexJoins) - 1; i >= 0; i-- {
		if _, ok := indexJoins[i].(*PhysicalIndexMergeJoin); ok {
			indexJoins = append(indexJoins[:i], indexJoins[i+1:]...)
		}
	}
	return indexJoins
}

func (p *LogicalJoin) preferAny(joinFlags ...uint) bool {
	for _, flag := range joinFlags {
		if p.preferJoinType&flag > 0 {
			return true
		}
	}
	return false
}

const (
	joinLeft             = 0
	joinRight            = 1
	indexJoinMethod      = 0
	indexHashJoinMethod  = 1
	indexMergeJoinMethod = 2
)

func (*LogicalJoin) getIndexJoinSideAndMethod(join base.PhysicalPlan) (innerSide, joinMethod int, ok bool) {
	var innerIdx int
	switch ij := join.(type) {
	case *PhysicalIndexJoin:
		innerIdx = ij.getInnerChildIdx()
		joinMethod = indexJoinMethod
	case *PhysicalIndexHashJoin:
		innerIdx = ij.getInnerChildIdx()
		joinMethod = indexHashJoinMethod
	case *PhysicalIndexMergeJoin:
		innerIdx = ij.getInnerChildIdx()
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

// tryToGetIndexJoin returns all available index join plans, and the second returned value indicates whether this plan is enforced by hints.
func (p *LogicalJoin) tryToGetIndexJoin(prop *property.PhysicalProperty) (indexJoins []base.PhysicalPlan, canForced bool) {
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		supportLeftOuter = true
	case RightOuterJoin:
		supportRightOuter = true
	case InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, p.getIndexJoinByOuterIdx(prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, p.getIndexJoinByOuterIdx(prop, 1)...)
	}

	// Handle hints and variables about index join.
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables.
	// Handle hints conflict first.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.preferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.preferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.preferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.preferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.preferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.preferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}

	candidates, canForced = p.handleForceIndexJoinHints(prop, candidates)
	if canForced {
		return candidates, canForced
	}
	candidates = p.handleFilterIndexJoinHints(candidates)
	return filterIndexJoinBySessionVars(p.SCtx(), candidates), false
}

func (p *LogicalJoin) handleFilterIndexJoinHints(candidates []base.PhysicalPlan) []base.PhysicalPlan {
	if !p.preferAny(h.PreferNoIndexJoin, h.PreferNoIndexHashJoin, h.PreferNoIndexMergeJoin) {
		return candidates // no filter index join hints
	}
	filtered := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		_, joinMethod, ok := p.getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.preferAny(h.PreferNoIndexJoin) && joinMethod == indexJoinMethod) ||
			(p.preferAny(h.PreferNoIndexHashJoin) && joinMethod == indexHashJoinMethod) ||
			(p.preferAny(h.PreferNoIndexMergeJoin) && joinMethod == indexMergeJoinMethod) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}

// handleForceIndexJoinHints handles the force index join hints and returns all plans that can satisfy the hints.
func (p *LogicalJoin) handleForceIndexJoinHints(prop *property.PhysicalProperty, candidates []base.PhysicalPlan) (indexJoins []base.PhysicalPlan, canForced bool) {
	if !p.preferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return candidates, false // no force index join hints
	}
	forced := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		innerSide, joinMethod, ok := p.getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.preferAny(h.PreferLeftAsINLJInner) && innerSide == joinLeft && joinMethod == indexJoinMethod) ||
			(p.preferAny(h.PreferRightAsINLJInner) && innerSide == joinRight && joinMethod == indexJoinMethod) ||
			(p.preferAny(h.PreferLeftAsINLHJInner) && innerSide == joinLeft && joinMethod == indexHashJoinMethod) ||
			(p.preferAny(h.PreferRightAsINLHJInner) && innerSide == joinRight && joinMethod == indexHashJoinMethod) ||
			(p.preferAny(h.PreferLeftAsINLMJInner) && innerSide == joinLeft && joinMethod == indexMergeJoinMethod) ||
			(p.preferAny(h.PreferRightAsINLMJInner) && innerSide == joinRight && joinMethod == indexMergeJoinMethod) {
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
		if p.hintInfo != nil {
			t := p.hintInfo.IndexJoin
			indexJoinTables, indexHashJoinTables, indexMergeJoinTables = t.INLJTables, t.INLHJTables, t.INLMJTables
		}
		var errMsg string
		switch {
		case p.preferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner): // prefer index join
			errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable", h.Restore2JoinHint(h.HintINLJ, indexJoinTables), h.Restore2JoinHint(h.TiDBIndexNestedLoopJoin, indexJoinTables))
		case p.preferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner): // prefer index hash join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLHJ, indexHashJoinTables))
		case p.preferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner): // prefer index merge join
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

func checkChildFitBC(p base.Plan) bool {
	if p.StatsInfo().HistColl == nil {
		return p.SCtx().GetSessionVars().BroadcastJoinThresholdCount == -1 || p.StatsInfo().Count() < p.SCtx().GetSessionVars().BroadcastJoinThresholdCount
	}
	avg := cardinality.GetAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, p.Schema().Columns, false, false)
	sz := avg * float64(p.StatsInfo().Count())
	return p.SCtx().GetSessionVars().BroadcastJoinThresholdSize == -1 || sz < float64(p.SCtx().GetSessionVars().BroadcastJoinThresholdSize)
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

func isJoinFitMPPBCJ(p *LogicalJoin, mppStoreCnt int) bool {
	rowBC, szBC, hasSizeBC := calcBroadcastExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)
	rowHash, szHash, hasSizeHash := calcHashExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)
	if hasSizeBC && hasSizeHash {
		return szBC*float64(mppStoreCnt) <= szHash
	}
	return rowBC*float64(mppStoreCnt) <= rowHash
}

func isJoinChildFitMPPBCJ(p *LogicalJoin, childIndexToBC int, mppStoreCnt int) bool {
	rowBC, szBC, hasSizeBC := calcBroadcastExchangeSize(p.Children()[childIndexToBC], mppStoreCnt)
	rowHash, szHash, hasSizeHash := calcHashExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)

	if hasSizeBC && hasSizeHash {
		return szBC*float64(mppStoreCnt) <= szHash
	}
	return rowBC*float64(mppStoreCnt) <= rowHash
}

// If we can use mpp broadcast join, that's our first choice.
func (p *LogicalJoin) preferMppBCJ() bool {
	if len(p.EqualConditions) == 0 && p.SCtx().GetSessionVars().AllowCartesianBCJ == 2 {
		return true
	}

	onlyCheckChild1 := p.JoinType == LeftOuterJoin || p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin
	onlyCheckChild0 := p.JoinType == RightOuterJoin

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

	if onlyCheckChild1 {
		return checkChildFitBC(p.Children()[1])
	} else if onlyCheckChild0 {
		return checkChildFitBC(p.Children()[0])
	}
	return checkChildFitBC(p.Children()[0]) || checkChildFitBC(p.Children()[1])
}

// ExhaustPhysicalPlans implements LogicalPlan interface
// it can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
			indexJoins, _ := p.tryToGetIndexJoin(prop)
			failpoint.Return(indexJoins, true, nil)
		}
	})

	if !isJoinHintSupportedInMPPMode(p.preferJoinType) {
		if hasMPPJoinHints(p.preferJoinType) {
			// If there are MPP hints but has some conflicts join method hints, all the join hints are invalid.
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("The MPP join hints are in conflict, and you can only specify join method hints that are currently supported by MPP mode now")
			p.preferJoinType = 0
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
	canPushToTiFlash := p.CanPushToCop(kv.TiFlash)
	if p.SCtx().GetSessionVars().IsMPPAllowed() && canPushToTiFlash {
		if (p.preferJoinType & h.PreferShuffleJoin) > 0 {
			if shuffleJoins := p.tryToGetMppHashJoin(prop, false); len(shuffleJoins) > 0 {
				return shuffleJoins, true, nil
			}
		}
		if (p.preferJoinType & h.PreferBCJoin) > 0 {
			if bcastJoins := p.tryToGetMppHashJoin(prop, true); len(bcastJoins) > 0 {
				return bcastJoins, true, nil
			}
		}
		if p.preferMppBCJ() {
			mppJoins := p.tryToGetMppHashJoin(prop, true)
			joins = append(joins, mppJoins...)
		} else {
			mppJoins := p.tryToGetMppHashJoin(prop, false)
			joins = append(joins, mppJoins...)
		}
	} else {
		hasMppHints := false
		var errMsg string
		if (p.preferJoinType & h.PreferShuffleJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the shuffle_join() hint is invalid"
			hasMppHints = true
		}
		if (p.preferJoinType & h.PreferBCJoin) > 0 {
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

	if !p.isNAAJ() {
		// naaj refuse merge join and index join.
		mergeJoins := p.GetMergeJoin(prop, p.schema, p.StatsInfo(), p.Children()[0].StatsInfo(), p.Children()[1].StatsInfo())
		if (p.preferJoinType&h.PreferMergeJoin) > 0 && len(mergeJoins) > 0 {
			return mergeJoins, true, nil
		}
		joins = append(joins, mergeJoins...)

		indexJoins, forced := p.tryToGetIndexJoin(prop)
		if forced {
			return indexJoins, true, nil
		}
		joins = append(joins, indexJoins...)
	}

	hashJoins, forced := p.getHashJoins(prop)
	if forced && len(hashJoins) > 0 {
		return hashJoins, true, nil
	}
	joins = append(joins, hashJoins...)

	if p.preferJoinType > 0 {
		// If we reach here, it means we have a hint that doesn't work.
		// It might be affected by the required property, so we enforce
		// this property and try the hint again.
		return joins, false, nil
	}
	return joins, true, nil
}

func canExprsInJoinPushdown(p *LogicalJoin, storeType kv.StoreType) bool {
	equalExprs := make([]expression.Expression, 0, len(p.EqualConditions))
	for _, eqCondition := range p.EqualConditions {
		if eqCondition.FuncName.L == ast.NullEQ {
			return false
		}
		equalExprs = append(equalExprs, eqCondition)
	}
	pushDownCtx := GetPushDownCtx(p.SCtx())
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

func (p *LogicalJoin) tryToGetMppHashJoin(prop *property.PhysicalProperty, useBCJ bool) []base.PhysicalPlan {
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

	if p.JoinType != InnerJoin && p.JoinType != LeftOuterJoin && p.JoinType != RightOuterJoin && p.JoinType != SemiJoin && p.JoinType != AntiSemiJoin && p.JoinType != LeftOuterSemiJoin && p.JoinType != AntiLeftOuterSemiJoin {
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
	if len(p.LeftConditions) != 0 && p.JoinType != LeftOuterJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there is a join that is not `left join` but has left conditions, which is not supported by mpp now, see github.com/pingcap/tidb/issues/26090 for more information.")
		return nil
	}
	if len(p.RightConditions) != 0 && p.JoinType != RightOuterJoin {
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
	baseJoin := basePhysicalJoin{
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
	forceLeftToBuild := ((p.preferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.preferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.preferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.preferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are conflicts, please check the hints")
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	preferredBuildIndex := 0
	fixedBuildSide := false // Used to indicate whether the build side for the MPP join is fixed or not.
	if p.JoinType == InnerJoin {
		if p.Children()[0].StatsInfo().Count() > p.Children()[1].StatsInfo().Count() {
			preferredBuildIndex = 1
		}
	} else if p.JoinType.IsSemiJoin() {
		if !useBCJ && !p.isNAAJ() && len(p.EqualConditions) > 0 && (p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin) {
			// TiFlash only supports Non-null_aware non-cross semi/anti_semi join to use both sides as build side
			preferredBuildIndex = 1
			// MPPOuterJoinFixedBuildSide default value is false
			// use MPPOuterJoinFixedBuildSide here as a way to disable using left table as build side
			if !p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide && p.Children()[1].StatsInfo().Count() > p.Children()[0].StatsInfo().Count() {
				preferredBuildIndex = 0
			}
		} else {
			preferredBuildIndex = 1
			fixedBuildSide = true
		}
	}
	if p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin {
		// TiFlash does not require that the build side must be the inner table for outer join.
		// so we can choose the build side based on the row count, except that:
		// 1. it is a broadcast join(for broadcast join, it makes sense to use the broadcast side as the build side)
		// 2. or session variable MPPOuterJoinFixedBuildSide is set to true
		// 3. or nullAware/cross joins
		if useBCJ || p.isNAAJ() || len(p.EqualConditions) == 0 || p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide {
			if !p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide {
				// The hint has higher priority than variable.
				fixedBuildSide = true
			}
			if p.JoinType == LeftOuterJoin {
				preferredBuildIndex = 1
			}
		} else if p.Children()[0].StatsInfo().Count() > p.Children()[1].StatsInfo().Count() {
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
		childrenProps[preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.BroadcastType, CanAddEnforcer: true, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
		expCnt := math.MaxFloat64
		if prop.ExpectedCnt < p.StatsInfo().RowCount {
			expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
			expCnt = p.Children()[1-preferredBuildIndex].StatsInfo().RowCount * expCntScale
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
			childrenProps[1-preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: expCnt, MPPPartitionTp: property.HashType, MPPPartitionCols: prop.MPPPartitionCols, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
		} else {
			childrenProps[1-preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: expCnt, MPPPartitionTp: property.AnyType, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
		}
	} else {
		lPartitionKeys, rPartitionKeys := p.GetPotentialPartitionKeys()
		if prop.MPPPartitionTp == property.HashType {
			var matches []int
			if p.JoinType == InnerJoin {
				if matches = prop.IsSubsetOf(lPartitionKeys); len(matches) == 0 {
					matches = prop.IsSubsetOf(rPartitionKeys)
				}
			} else if p.JoinType == RightOuterJoin {
				// for right out join, only the right partition keys can possibly matches the prop, because
				// the left partition keys will generate NULL values randomly
				// todo maybe we can add a null-sensitive flag in the MPPPartitionColumn to indicate whether the partition column is
				//  null-sensitive(used in aggregation) or null-insensitive(used in join)
				matches = prop.IsSubsetOf(rPartitionKeys)
			} else {
				// for left out join, only the left partition keys can possibly matches the prop, because
				// the right partition keys will generate NULL values randomly
				// for semi/anti semi/left out semi/anti left out semi join, only left partition keys are returned,
				// so just check the left partition keys
				matches = prop.IsSubsetOf(lPartitionKeys)
			}
			if len(matches) == 0 {
				return nil
			}
			lPartitionKeys = choosePartitionKeys(lPartitionKeys, matches)
			rPartitionKeys = choosePartitionKeys(rPartitionKeys, matches)
		}
		childrenProps[0] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: lPartitionKeys, CanAddEnforcer: true, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
		childrenProps[1] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: rPartitionKeys, CanAddEnforcer: true, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
	}
	join := PhysicalHashJoin{
		basePhysicalJoin:  baseJoin,
		Concurrency:       uint(p.SCtx().GetSessionVars().CopTiFlashConcurrencyFactor),
		EqualConditions:   p.EqualConditions,
		NAEqualConditions: p.NAEQConditions,
		storeTp:           kv.TiFlash,
		mppShuffleJoin:    !useBCJ,
		// Mpp Join has quite heavy cost. Even limit might not suspend it in time, so we don't scale the count.
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), childrenProps...)
	join.SetSchema(p.schema)
	return []base.PhysicalPlan{join}
}

func choosePartitionKeys(keys []*property.MPPPartitionColumn, matches []int) []*property.MPPPartitionColumn {
	newKeys := make([]*property.MPPPartitionColumn, 0, len(matches))
	for _, id := range matches {
		newKeys = append(newKeys, keys[id])
	}
	return newKeys
}

// TryToGetChildProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *LogicalProjection) TryToGetChildProp(prop *property.PhysicalProperty) (*property.PhysicalProperty, bool) {
	newProp := prop.CloneEssentialFields()
	newCols := make([]property.SortItem, 0, len(prop.SortItems))
	for _, col := range prop.SortItems {
		idx := p.schema.ColumnIndex(col.Col)
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, property.SortItem{Col: expr, Desc: col.Desc})
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.SortItems = newCols
	return newProp, true
}

// ExhaustPhysicalPlans enumerate all the possible physical plan for expand operator (currently only mpp case is supported)
func (p *LogicalExpand) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	// under the mpp task type, if the sort item is not empty, refuse it, cause expanded data doesn't support any sort items.
	if !prop.IsSortItemEmpty() {
		// false, meaning we can add a sort enforcer.
		return nil, false, nil
	}
	// RootTaskType is the default one, meaning no option. (we can give them a mpp choice)
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil, true, nil
	}
	// now Expand mode can only be executed on TiFlash node.
	// Upper layer shouldn't expect any mpp partition from an Expand operator.
	// todo: data output from Expand operator should keep the origin data mpp partition.
	if prop.TaskTp == property.MppTaskType && prop.MPPPartitionTp != property.AnyType {
		return nil, true, nil
	}
	// for property.RootTaskType and property.MppTaskType with no partition option, we can give an MPP Expand.
	if p.SCtx().GetSessionVars().IsMPPAllowed() {
		mppProp := prop.CloneEssentialFields()
		mppProp.TaskTp = property.MppTaskType
		expand := PhysicalExpand{
			GroupingSets:          p.rollupGroupingSets,
			LevelExprs:            p.LevelExprs,
			ExtraGroupingColNames: p.ExtraGroupingColNames,
		}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), p.QueryBlockOffset(), mppProp)
		expand.SetSchema(p.Schema())
		return []base.PhysicalPlan{expand}, true, nil
	}
	// if MPP switch is shutdown, nothing can be generated.
	return nil, true, nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalProjection) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	newProp, ok := p.TryToGetChildProp(prop)
	if !ok {
		return nil, true, nil
	}
	newProps := []*property.PhysicalProperty{newProp}
	// generate a mpp task candidate if mpp mode is allowed
	ctx := p.SCtx()
	pushDownCtx := GetPushDownCtx(ctx)
	if newProp.TaskTp != property.MppTaskType && ctx.GetSessionVars().IsMPPAllowed() && p.CanPushToCop(kv.TiFlash) &&
		expression.CanExprsPushDown(pushDownCtx, p.Exprs, kv.TiFlash) {
		mppProp := newProp.CloneEssentialFields()
		mppProp.TaskTp = property.MppTaskType
		newProps = append(newProps, mppProp)
	}
	if newProp.TaskTp != property.CopSingleReadTaskType && ctx.GetSessionVars().AllowProjectionPushDown && p.CanPushToCop(kv.TiKV) &&
		expression.CanExprsPushDown(pushDownCtx, p.Exprs, kv.TiKV) && !expression.ContainVirtualColumn(p.Exprs) {
		copProp := newProp.CloneEssentialFields()
		copProp.TaskTp = property.CopSingleReadTaskType
		newProps = append(newProps, copProp)
	}

	ret := make([]base.PhysicalPlan, 0, len(newProps))
	for _, newProp := range newProps {
		proj := PhysicalProjection{
			Exprs:                p.Exprs,
			CalculateNoDelay:     p.CalculateNoDelay,
			AvoidColumnEvaluator: p.AvoidColumnEvaluator,
		}.Init(ctx, p.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), p.QueryBlockOffset(), newProp)
		proj.SetSchema(p.schema)
		ret = append(ret, proj)
	}
	return ret, true, nil
}

func pushLimitOrTopNForcibly(p base.LogicalPlan) bool {
	var meetThreshold bool
	var preferPushDown *bool
	switch lp := p.(type) {
	case *LogicalTopN:
		preferPushDown = &lp.PreferLimitToCop
		meetThreshold = lp.Count+lp.Offset <= uint64(lp.SCtx().GetSessionVars().LimitPushDownThreshold)
	case *LogicalLimit:
		preferPushDown = &lp.PreferLimitToCop
		meetThreshold = true // always push Limit down in this case since it has no side effect
	default:
		return false
	}

	if *preferPushDown || meetThreshold {
		if p.CanPushToCop(kv.TiKV) {
			return true
		}
		if *preferPushDown {
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer Hint LIMIT_TO_COP is inapplicable")
			*preferPushDown = false
		}
	}

	return false
}

func (lt *LogicalTopN) getPhysTopN(prop *property.PhysicalProperty) []base.PhysicalPlan {
	allTaskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType}
	if !pushLimitOrTopNForcibly(lt) {
		allTaskTypes = append(allTaskTypes, property.RootTaskType)
	}
	if lt.SCtx().GetSessionVars().IsMPPAllowed() {
		allTaskTypes = append(allTaskTypes, property.MppTaskType)
	}
	ret := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus}
		topN := PhysicalTopN{
			ByItems:     lt.ByItems,
			PartitionBy: lt.PartitionBy,
			Count:       lt.Count,
			Offset:      lt.Offset,
		}.Init(lt.SCtx(), lt.StatsInfo(), lt.QueryBlockOffset(), resultProp)
		ret = append(ret, topN)
	}
	return ret
}

func (lt *LogicalTopN) getPhysLimits(prop *property.PhysicalProperty) []base.PhysicalPlan {
	p, canPass := GetPropByOrderByItems(lt.ByItems)
	if !canPass {
		return nil
	}

	allTaskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType}
	if !pushLimitOrTopNForcibly(lt) {
		allTaskTypes = append(allTaskTypes, property.RootTaskType)
	}
	ret := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(lt.Count + lt.Offset), SortItems: p.SortItems, CTEProducerStatus: prop.CTEProducerStatus}
		limit := PhysicalLimit{
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

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (lt *LogicalTopN) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if MatchItems(prop, lt.ByItems) {
		return append(lt.getPhysTopN(prop), lt.getPhysLimits(prop)...), true, nil
	}
	return nil, true, nil
}

// GetHashJoin is public for cascades planner.
func (la *LogicalApply) GetHashJoin(prop *property.PhysicalProperty) *PhysicalHashJoin {
	return la.LogicalJoin.getHashJoin(prop, 1, false)
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (la *LogicalApply) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if !prop.AllColsFromSchema(la.Children()[0].Schema()) || prop.IsFlashProp() { // for convenient, we don't pass through any prop
		la.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `Apply` is not supported now.")
		return nil, true, nil
	}
	if !prop.IsSortItemEmpty() && la.SCtx().GetSessionVars().EnableParallelApply {
		la.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("Parallel Apply rejects the possible order properties of its outer child currently"))
		return nil, true, nil
	}
	disableAggPushDownToCop(la.Children()[0])
	join := la.GetHashJoin(prop)
	var columns = make([]*expression.Column, 0, len(la.CorCols))
	for _, colColumn := range la.CorCols {
		columns = append(columns, &colColumn.Column)
	}
	cacheHitRatio := 0.0
	if la.StatsInfo().RowCount != 0 {
		ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(columns, la.schema, la.StatsInfo())
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

	apply := PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.CorCols,
		CanUseCache:      canUseCache,
	}.Init(la.SCtx(),
		la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt),
		la.QueryBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus})
	apply.SetSchema(la.schema)
	return []base.PhysicalPlan{apply}, true, nil
}

func disableAggPushDownToCop(p base.LogicalPlan) {
	for _, child := range p.Children() {
		disableAggPushDownToCop(child)
	}
	if agg, ok := p.(*LogicalAggregation); ok {
		agg.noCopPushDown = true
	}
}

// GetPartitionKeys gets partition keys for a logical window, it will assign column id for expressions.
func (lw *LogicalWindow) GetPartitionKeys() []*property.MPPPartitionColumn {
	partitionByCols := make([]*property.MPPPartitionColumn, 0, len(lw.GetPartitionByCols()))
	for _, item := range lw.PartitionBy {
		partitionByCols = append(partitionByCols, &property.MPPPartitionColumn{
			Col:       item.Col,
			CollateID: property.GetCollateIDByNameForPartition(item.Col.GetStaticType().GetCollate()),
		})
	}

	return partitionByCols
}

// Duration vs Datetime is invalid comparison as TiFlash can't handle it so far.
func (lw *LogicalWindow) checkComparisonForTiFlash(frameBound *FrameBound) bool {
	if len(frameBound.CompareCols) > 0 {
		orderByEvalType := lw.OrderBy[0].Col.GetStaticType().EvalType()
		calFuncEvalType := frameBound.CalcFuncs[0].GetType(lw.SCtx().GetExprCtx().GetEvalCtx()).EvalType()

		if orderByEvalType == types.ETDuration && (calFuncEvalType == types.ETDatetime || calFuncEvalType == types.ETTimestamp) {
			return false
		} else if calFuncEvalType == types.ETDuration && (orderByEvalType == types.ETDatetime || orderByEvalType == types.ETTimestamp) {
			return false
		}
	}
	return true
}

func (lw *LogicalWindow) tryToGetMppWindows(prop *property.PhysicalProperty) []base.PhysicalPlan {
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
			if !windowFunc.CanPushDownToTiFlash(GetPushDownCtx(sctx)) {
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
			if _, err := expression.ExpressionsToPBList(ctx.GetEvalCtx(), lw.Frame.End.CalcFuncs, lw.SCtx().GetClient()); err != nil {
				lw.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
					"MPP mode may be blocked because window function frame can't be pushed down, because " + err.Error())
				return nil
			}

			if !lw.checkComparisonForTiFlash(lw.Frame.Start) || !lw.checkComparisonForTiFlash(lw.Frame.End) {
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
			partitionCols = choosePartitionKeys(partitionCols, matches)
		}
		childProperty.MPPPartitionTp = property.HashType
		childProperty.MPPPartitionCols = partitionCols
	} else {
		childProperty.MPPPartitionTp = property.SinglePartitionType
	}

	if prop.MPPPartitionTp == property.SinglePartitionType && childProperty.MPPPartitionTp != property.SinglePartitionType {
		return nil
	}

	window := PhysicalWindow{
		WindowFuncDescs: lw.WindowFuncDescs,
		PartitionBy:     lw.PartitionBy,
		OrderBy:         lw.OrderBy,
		Frame:           lw.Frame,
		storeTp:         kv.TiFlash,
	}.Init(lw.SCtx(), lw.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), lw.QueryBlockOffset(), childProperty)
	window.SetSchema(lw.Schema())

	return []base.PhysicalPlan{window}
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (lw *LogicalWindow) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	windows := make([]base.PhysicalPlan, 0, 2)

	canPushToTiFlash := lw.CanPushToCop(kv.TiFlash)
	if lw.SCtx().GetSessionVars().IsMPPAllowed() && canPushToTiFlash {
		mppWindows := lw.tryToGetMppWindows(prop)
		windows = append(windows, mppWindows...)
	}

	// if there needs a mpp task, we don't generate tidb window function.
	if prop.TaskTp == property.MppTaskType {
		return windows, true, nil
	}
	var byItems []property.SortItem
	byItems = append(byItems, lw.PartitionBy...)
	byItems = append(byItems, lw.OrderBy...)
	childProperty := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, SortItems: byItems, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
	if !prop.IsPrefix(childProperty) {
		return nil, true, nil
	}
	window := PhysicalWindow{
		WindowFuncDescs: lw.WindowFuncDescs,
		PartitionBy:     lw.PartitionBy,
		OrderBy:         lw.OrderBy,
		Frame:           lw.Frame,
	}.Init(lw.SCtx(), lw.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), lw.QueryBlockOffset(), childProperty)
	window.SetSchema(lw.Schema())

	windows = append(windows, window)
	return windows, true, nil
}

func canPushToCopImpl(lp base.LogicalPlan, storeTp kv.StoreType, considerDual bool) bool {
	p := lp.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan)
	ret := true
	for _, ch := range p.Children() {
		switch c := ch.(type) {
		case *DataSource:
			validDs := false
			indexMergeIsIntersection := false
			for _, path := range c.possibleAccessPaths {
				if path.StoreType == storeTp {
					validDs = true
				}
				if len(path.PartialIndexPaths) > 0 && path.IndexMergeIsIntersection {
					indexMergeIsIntersection = true
				}
			}
			ret = ret && validDs

			_, isTopN := p.Self().(*LogicalTopN)
			_, isLimit := p.Self().(*LogicalLimit)
			if (isTopN || isLimit) && indexMergeIsIntersection {
				return false // TopN and Limit cannot be pushed down to the intersection type IndexMerge
			}

			if c.tableInfo.TableCacheStatusType != model.TableCacheStatusDisable {
				// Don't push to cop for cached table, it brings more harm than good:
				// 1. Those tables are small enough, push to cop can't utilize several TiKV to accelerate computation.
				// 2. Cached table use UnionScan to read the cache data, and push to cop is not supported when an UnionScan exists.
				// Once aggregation is pushed to cop, the cache data can't be use any more.
				return false
			}
		case *LogicalUnionAll:
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && canPushToCopImpl(&c.BaseLogicalPlan, storeTp, true)
		case *LogicalSort:
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && canPushToCopImpl(&c.BaseLogicalPlan, storeTp, true)
		case *LogicalProjection:
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && canPushToCopImpl(&c.BaseLogicalPlan, storeTp, considerDual)
		case *LogicalExpand:
			// Expand itself only contains simple col ref and literal projection. (always ok, check its child)
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && canPushToCopImpl(&c.BaseLogicalPlan, storeTp, considerDual)
		case *LogicalTableDual:
			return storeTp == kv.TiFlash && considerDual
		case *LogicalAggregation, *LogicalSelection, *LogicalJoin, *LogicalWindow:
			if storeTp != kv.TiFlash {
				return false
			}
			ret = ret && c.CanPushToCop(storeTp)
		// These operators can be partially push down to TiFlash, so we don't raise warning for them.
		case *LogicalLimit, *LogicalTopN:
			return false
		case *LogicalSequence:
			return storeTp == kv.TiFlash
		case *LogicalCTE:
			if storeTp != kv.TiFlash {
				return false
			}
			if c.cte.recursivePartLogicalPlan != nil || !c.cte.seedPartLogicalPlan.CanPushToCop(storeTp) {
				return false
			}
			return true
		default:
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
				"MPP mode may be blocked because operator `" + c.TP() + "` is not supported now.")
			return false
		}
	}
	return ret
}

// CanPushToCop implements LogicalPlan interface.
func (la *LogicalAggregation) CanPushToCop(storeTp kv.StoreType) bool {
	return la.BaseLogicalPlan.CanPushToCop(storeTp) && !la.noCopPushDown
}

func (la *LogicalAggregation) getEnforcedStreamAggs(prop *property.PhysicalProperty) []base.PhysicalPlan {
	if prop.IsFlashProp() {
		return nil
	}
	_, desc := prop.AllSameOrder()
	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	enforcedAggs := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt:    math.Max(prop.ExpectedCnt*la.inputCount/la.StatsInfo().RowCount, prop.ExpectedCnt),
		CanAddEnforcer: true,
		SortItems:      property.SortItemsFromCols(la.GetGroupByCols(), desc),
	}
	if !prop.IsPrefix(childProp) {
		return enforcedAggs
	}
	taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType}
	if la.HasDistinct() {
		// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
		// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
		if !la.CanPushToCop(kv.TiKV) || !la.SCtx().GetSessionVars().AllowDistinctAggPushDown {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
	} else if !la.PreferAggToCop {
		taskTypes = append(taskTypes, property.RootTaskType)
	}
	for _, taskTp := range taskTypes {
		copiedChildProperty := new(property.PhysicalProperty)
		*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
		copiedChildProperty.TaskTp = taskTp

		newGbyItems := make([]expression.Expression, len(la.GroupByItems))
		copy(newGbyItems, la.GroupByItems)
		newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
		copy(newAggFuncs, la.AggFuncs)

		agg := basePhysicalAgg{
			GroupByItems: newGbyItems,
			AggFuncs:     newAggFuncs,
		}.initForStream(la.SCtx(), la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), la.QueryBlockOffset(), copiedChildProperty)
		agg.SetSchema(la.schema.Clone())
		enforcedAggs = append(enforcedAggs, agg)
	}
	return enforcedAggs
}

func (la *LogicalAggregation) distinctArgsMeetsProperty() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			for _, distinctArg := range aggFunc.Args {
				if !expression.Contains(la.GroupByItems, distinctArg) {
					return false
				}
			}
		}
	}
	return true
}

func (la *LogicalAggregation) getStreamAggs(prop *property.PhysicalProperty) []base.PhysicalPlan {
	// TODO: support CopTiFlash task type in stream agg
	if prop.IsFlashProp() {
		return nil
	}
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}

	for _, aggFunc := range la.AggFuncs {
		if aggFunc.Mode == aggregation.FinalMode {
			return nil
		}
	}
	// group by a + b is not interested in any order.
	groupByCols := la.GetGroupByCols()
	if len(groupByCols) != len(la.GroupByItems) {
		return nil
	}

	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	streamAggs := make([]base.PhysicalPlan, 0, len(la.possibleProperties)*(len(allTaskTypes)-1)+len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.inputCount/la.StatsInfo().RowCount, prop.ExpectedCnt),
	}

	for _, possibleChildProperty := range la.possibleProperties {
		childProp.SortItems = property.SortItemsFromCols(possibleChildProperty[:len(groupByCols)], desc)
		if !prop.IsPrefix(childProp) {
			continue
		}
		// The table read of "CopDoubleReadTaskType" can't promises the sort
		// property that the stream aggregation required, no need to consider.
		taskTypes := []property.TaskType{property.CopSingleReadTaskType}
		if la.HasDistinct() {
			// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
			// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
			if !la.SCtx().GetSessionVars().AllowDistinctAggPushDown || !la.CanPushToCop(kv.TiKV) {
				// if variable doesn't allow DistinctAggPushDown, just produce root task type.
				// if variable does allow DistinctAggPushDown, but OP itself can't be pushed down to tikv, just produce root task type.
				taskTypes = []property.TaskType{property.RootTaskType}
			} else if !la.distinctArgsMeetsProperty() {
				continue
			}
		} else if !la.PreferAggToCop {
			taskTypes = append(taskTypes, property.RootTaskType)
		}
		if !la.CanPushToCop(kv.TiKV) && !la.CanPushToCop(kv.TiFlash) {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
		for _, taskTp := range taskTypes {
			copiedChildProperty := new(property.PhysicalProperty)
			*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
			copiedChildProperty.TaskTp = taskTp

			newGbyItems := make([]expression.Expression, len(la.GroupByItems))
			copy(newGbyItems, la.GroupByItems)
			newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
			copy(newAggFuncs, la.AggFuncs)

			agg := basePhysicalAgg{
				GroupByItems: newGbyItems,
				AggFuncs:     newAggFuncs,
			}.initForStream(la.SCtx(), la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), la.QueryBlockOffset(), copiedChildProperty)
			agg.SetSchema(la.schema.Clone())
			streamAggs = append(streamAggs, agg)
		}
	}
	// If STREAM_AGG hint is existed, it should consider enforce stream aggregation,
	// because we can't trust possibleChildProperty completely.
	if (la.PreferAggType & h.PreferStreamAgg) > 0 {
		streamAggs = append(streamAggs, la.getEnforcedStreamAggs(prop)...)
	}
	return streamAggs
}

// TODO: support more operators and distinct later
func (la *LogicalAggregation) checkCanPushDownToMPP() bool {
	hasUnsupportedDistinct := false
	for _, agg := range la.AggFuncs {
		// MPP does not support distinct except count distinct now
		if agg.HasDistinct {
			if agg.Name != ast.AggFuncCount && agg.Name != ast.AggFuncGroupConcat {
				hasUnsupportedDistinct = true
			}
		}
		// MPP does not support AggFuncApproxCountDistinct now
		if agg.Name == ast.AggFuncApproxCountDistinct {
			hasUnsupportedDistinct = true
		}
	}
	if hasUnsupportedDistinct {
		warnErr := errors.NewNoStackError("Aggregation can not be pushed to storage layer in mpp mode because it contains agg function with distinct")
		if la.SCtx().GetSessionVars().StmtCtx.InExplainStmt {
			la.SCtx().GetSessionVars().StmtCtx.AppendWarning(warnErr)
		} else {
			la.SCtx().GetSessionVars().StmtCtx.AppendExtraWarning(warnErr)
		}
		return false
	}
	return CheckAggCanPushCop(la.SCtx(), la.AggFuncs, la.GroupByItems, kv.TiFlash)
}

func (la *LogicalAggregation) tryToGetMppHashAggs(prop *property.PhysicalProperty) (hashAggs []base.PhysicalPlan) {
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil
	}
	if prop.MPPPartitionTp == property.BroadcastType {
		return nil
	}

	// Is this aggregate a final stage aggregate?
	// Final agg can't be split into multi-stage aggregate
	hasFinalAgg := len(la.AggFuncs) > 0 && la.AggFuncs[0].Mode == aggregation.FinalMode
	// count final agg should become sum for MPP execution path.
	// In the traditional case, TiDB take up the final agg role and push partial agg to TiKV,
	// while TiDB can tell the partialMode and do the sum computation rather than counting but MPP doesn't
	finalAggAdjust := func(aggFuncs []*aggregation.AggFuncDesc) {
		for i, agg := range aggFuncs {
			if agg.Mode == aggregation.FinalMode && agg.Name == ast.AggFuncCount {
				oldFT := agg.RetTp
				aggFuncs[i], _ = aggregation.NewAggFuncDesc(la.SCtx().GetExprCtx(), ast.AggFuncSum, agg.Args, false)
				aggFuncs[i].TypeInfer4FinalCount(oldFT)
			}
		}
	}
	// ref: https://github.com/pingcap/tiflash/blob/3ebb102fba17dce3d990d824a9df93d93f1ab
	// 766/dbms/src/Flash/Coprocessor/AggregationInterpreterHelper.cpp#L26
	validMppAgg := func(mppAgg *PhysicalHashAgg) bool {
		isFinalAgg := true
		if mppAgg.AggFuncs[0].Mode != aggregation.FinalMode && mppAgg.AggFuncs[0].Mode != aggregation.CompleteMode {
			isFinalAgg = false
		}
		for _, one := range mppAgg.AggFuncs[1:] {
			otherIsFinalAgg := one.Mode == aggregation.FinalMode || one.Mode == aggregation.CompleteMode
			if isFinalAgg != otherIsFinalAgg {
				// different agg mode detected in mpp side.
				return false
			}
		}
		return true
	}

	if len(la.GroupByItems) > 0 {
		partitionCols := la.GetPotentialPartitionKeys()
		// trying to match the required partitions.
		if prop.MPPPartitionTp == property.HashType {
			// partition key required by upper layer is subset of current layout.
			matches := prop.IsSubsetOf(partitionCols)
			if len(matches) == 0 {
				// do not satisfy the property of its parent, so return empty
				return nil
			}
			partitionCols = choosePartitionKeys(partitionCols, matches)
		} else if prop.MPPPartitionTp != property.AnyType {
			return nil
		}
		// TODO: permute various partition columns from group-by columns
		// 1-phase agg
		// If there are no available partition cols, but still have group by items, that means group by items are all expressions or constants.
		// To avoid mess, we don't do any one-phase aggregation in this case.
		// If this is a skew distinct group agg, skip generating 1-phase agg, because skew data will cause performance issue
		//
		// Rollup can't be 1-phase agg: cause it will append grouping_id to the schema, and expand each row as multi rows with different grouping_id.
		// In a general, group items should also append grouping_id as its group layout, let's say 1-phase agg has grouping items as <a,b,c>, and
		// lower OP can supply <a,b> as original partition layout, when we insert Expand logic between them:
		// <a,b>             -->    after fill null in Expand    --> and this shown two rows should be shuffled to the same node (the underlying partition is not satisfied yet)
		// <1,1> in node A           <1,null,gid=1> in node A
		// <1,2> in node B           <1,null,gid=1> in node B
		if len(partitionCols) != 0 && !la.SCtx().GetSessionVars().EnableSkewDistinctAgg {
			childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: partitionCols, CanAddEnforcer: true, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
			agg.SetSchema(la.schema.Clone())
			agg.MppRunMode = Mpp1Phase
			finalAggAdjust(agg.AggFuncs)
			if validMppAgg(agg) {
				hashAggs = append(hashAggs, agg)
			}
		}

		// Final agg can't be split into multi-stage aggregate, so exit early
		if hasFinalAgg {
			return
		}

		// 2-phase agg
		// no partition property down，record partition cols inside agg itself, enforce shuffler latter.
		childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.AnyType, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
		agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
		agg.SetSchema(la.schema.Clone())
		agg.MppRunMode = Mpp2Phase
		agg.MppPartitionCols = partitionCols
		if validMppAgg(agg) {
			hashAggs = append(hashAggs, agg)
		}

		// agg runs on TiDB with a partial agg on TiFlash if possible
		if prop.TaskTp == property.RootTaskType {
			childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
			agg.SetSchema(la.schema.Clone())
			agg.MppRunMode = MppTiDB
			hashAggs = append(hashAggs, agg)
		}
	} else if !hasFinalAgg {
		// TODO: support scalar agg in MPP, merge the final result to one node
		childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
		agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
		agg.SetSchema(la.schema.Clone())
		if la.HasDistinct() || la.HasOrderBy() {
			// mpp scalar mode means the data will be pass through to only one tiFlash node at last.
			agg.MppRunMode = MppScalar
		} else {
			agg.MppRunMode = MppTiDB
		}
		hashAggs = append(hashAggs, agg)
	}

	// handle MPP Agg hints
	var preferMode AggMppRunMode
	var prefer bool
	if la.PreferAggType&h.PreferMPP1PhaseAgg > 0 {
		preferMode, prefer = Mpp1Phase, true
	} else if la.PreferAggType&h.PreferMPP2PhaseAgg > 0 {
		preferMode, prefer = Mpp2Phase, true
	}
	if prefer {
		var preferPlans []base.PhysicalPlan
		for _, agg := range hashAggs {
			if hg, ok := agg.(*PhysicalHashAgg); ok && hg.MppRunMode == preferMode {
				preferPlans = append(preferPlans, hg)
			}
		}
		hashAggs = preferPlans
	}
	return
}

// getHashAggs will generate some kinds of taskType here, which finally converted to different task plan.
// when deciding whether to add a kind of taskType, there is a rule here. [Not is Not, Yes is not Sure]
// eg: which means
//
//	1: when you find something here that block hashAgg to be pushed down to XXX, just skip adding the XXXTaskType.
//	2: when you find nothing here to block hashAgg to be pushed down to XXX, just add the XXXTaskType here.
//	for 2, the final result for this physical operator enumeration is chosen or rejected is according to more factors later (hint/variable/partition/virtual-col/cost)
//
// That is to say, the non-complete positive judgement of canPushDownToMPP/canPushDownToTiFlash/canPushDownToTiKV is not that for sure here.
func (la *LogicalAggregation) getHashAggs(prop *property.PhysicalProperty) []base.PhysicalPlan {
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp == property.MppTaskType && !la.checkCanPushDownToMPP() {
		return nil
	}
	hashAggs := make([]base.PhysicalPlan, 0, len(prop.GetAllPossibleChildTaskTypes()))
	taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType}
	canPushDownToTiFlash := la.CanPushToCop(kv.TiFlash)
	canPushDownToMPP := canPushDownToTiFlash && la.SCtx().GetSessionVars().IsMPPAllowed() && la.checkCanPushDownToMPP()
	if la.HasDistinct() {
		// TODO: remove after the cost estimation of distinct pushdown is implemented.
		if !la.SCtx().GetSessionVars().AllowDistinctAggPushDown || !la.CanPushToCop(kv.TiKV) {
			// if variable doesn't allow DistinctAggPushDown, just produce root task type.
			// if variable does allow DistinctAggPushDown, but OP itself can't be pushed down to tikv, just produce root task type.
			taskTypes = []property.TaskType{property.RootTaskType}
		}
	} else if !la.PreferAggToCop {
		taskTypes = append(taskTypes, property.RootTaskType)
	}
	if !la.CanPushToCop(kv.TiKV) && !canPushDownToTiFlash {
		taskTypes = []property.TaskType{property.RootTaskType}
	}
	if canPushDownToMPP {
		taskTypes = append(taskTypes, property.MppTaskType)
	} else {
		hasMppHints := false
		var errMsg string
		if la.PreferAggType&h.PreferMPP1PhaseAgg > 0 {
			errMsg = "The agg can not push down to the MPP side, the MPP_1PHASE_AGG() hint is invalid"
			hasMppHints = true
		}
		if la.PreferAggType&h.PreferMPP2PhaseAgg > 0 {
			errMsg = "The agg can not push down to the MPP side, the MPP_2PHASE_AGG() hint is invalid"
			hasMppHints = true
		}
		if hasMppHints {
			la.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
	if prop.IsFlashProp() {
		taskTypes = []property.TaskType{prop.TaskTp}
	}

	for _, taskTp := range taskTypes {
		if taskTp == property.MppTaskType {
			mppAggs := la.tryToGetMppHashAggs(prop)
			if len(mppAggs) > 0 {
				hashAggs = append(hashAggs, mppAggs...)
			}
		} else {
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, TaskTp: taskTp, CTEProducerStatus: prop.CTEProducerStatus})
			agg.SetSchema(la.schema.Clone())
			hashAggs = append(hashAggs, agg)
		}
	}
	return hashAggs
}

// ResetHintIfConflicted resets the PreferAggType if they are conflicted,
// and returns the two PreferAggType hints.
func (la *LogicalAggregation) ResetHintIfConflicted() (preferHash bool, preferStream bool) {
	preferHash = (la.PreferAggType & h.PreferHashAgg) > 0
	preferStream = (la.PreferAggType & h.PreferStreamAgg) > 0
	if preferHash && preferStream {
		la.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer aggregation hints are conflicted")
		la.PreferAggType = 0
		preferHash, preferStream = false, false
	}
	return
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (la *LogicalAggregation) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if la.PreferAggToCop {
		if !la.CanPushToCop(kv.TiKV) {
			la.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
				"Optimizer Hint AGG_TO_COP is inapplicable")
			la.PreferAggToCop = false
		}
	}

	preferHash, preferStream := la.ResetHintIfConflicted()

	hashAggs := la.getHashAggs(prop)
	if hashAggs != nil && preferHash {
		return hashAggs, true, nil
	}

	streamAggs := la.getStreamAggs(prop)
	if streamAggs != nil && preferStream {
		return streamAggs, true, nil
	}

	aggs := append(hashAggs, streamAggs...)

	if streamAggs == nil && preferStream && !prop.IsSortItemEmpty() {
		la.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer Hint STREAM_AGG is inapplicable")
	}

	return aggs, !(preferStream || preferHash), nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalSelection) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	newProps := make([]*property.PhysicalProperty, 0, 2)
	childProp := prop.CloneEssentialFields()
	newProps = append(newProps, childProp)

	if prop.TaskTp != property.MppTaskType &&
		p.SCtx().GetSessionVars().IsMPPAllowed() &&
		p.canPushDown(kv.TiFlash) {
		childPropMpp := prop.CloneEssentialFields()
		childPropMpp.TaskTp = property.MppTaskType
		newProps = append(newProps, childPropMpp)
	}

	ret := make([]base.PhysicalPlan, 0, len(newProps))
	for _, newProp := range newProps {
		sel := PhysicalSelection{
			Conditions: p.Conditions,
		}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), p.QueryBlockOffset(), newProp)
		ret = append(ret, sel)
	}
	return ret, true, nil
}

// utility function to check whether we can push down Selection to TiKV or TiFlash
func (p *LogicalSelection) canPushDown(storeTp kv.StoreType) bool {
	return !expression.ContainVirtualColumn(p.Conditions) &&
		p.CanPushToCop(storeTp) &&
		expression.CanExprsPushDown(GetPushDownCtx(p.SCtx()), p.Conditions, storeTp)
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalLimit) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if !prop.IsSortItemEmpty() {
		return nil, true, nil
	}

	allTaskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType}
	if !pushLimitOrTopNForcibly(p) {
		allTaskTypes = append(allTaskTypes, property.RootTaskType)
	}
	if p.CanPushToCop(kv.TiFlash) && p.SCtx().GetSessionVars().IsMPPAllowed() {
		allTaskTypes = append(allTaskTypes, property.MppTaskType)
	}
	ret := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(p.Count + p.Offset), CTEProducerStatus: prop.CTEProducerStatus}
		limit := PhysicalLimit{
			Offset:      p.Offset,
			Count:       p.Count,
			PartitionBy: p.GetPartitionBy(),
		}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), resultProp)
		limit.SetSchema(p.Schema())
		ret = append(ret, limit)
	}
	return ret, true, nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalLock) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if prop.IsFlashProp() {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `Lock` is not supported now.")
		return nil, true, nil
	}
	childProp := prop.CloneEssentialFields()
	lock := PhysicalLock{
		Lock:               p.Lock,
		TblID2Handle:       p.tblID2Handle,
		TblID2PhysTblIDCol: p.tblID2PhysTblIDCol,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), childProp)
	return []base.PhysicalPlan{lock}, true, nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalUnionAll) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	// TODO: UnionAll can not pass any order, but we can change it to sort merge to keep order.
	if !prop.IsSortItemEmpty() || (prop.IsFlashProp() && prop.TaskTp != property.MppTaskType) {
		return nil, true, nil
	}
	// TODO: UnionAll can pass partition info, but for briefness, we prevent it from pushing down.
	if prop.TaskTp == property.MppTaskType && prop.MPPPartitionTp != property.AnyType {
		return nil, true, nil
	}
	canUseMpp := p.SCtx().GetSessionVars().IsMPPAllowed() && canPushToCopImpl(&p.BaseLogicalPlan, kv.TiFlash, true)
	chReqProps := make([]*property.PhysicalProperty, 0, p.ChildLen())
	for range p.Children() {
		if canUseMpp && prop.TaskTp == property.MppTaskType {
			chReqProps = append(chReqProps, &property.PhysicalProperty{
				ExpectedCnt:       prop.ExpectedCnt,
				TaskTp:            property.MppTaskType,
				RejectSort:        true,
				CTEProducerStatus: prop.CTEProducerStatus,
			})
		} else {
			chReqProps = append(chReqProps, &property.PhysicalProperty{ExpectedCnt: prop.ExpectedCnt, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus})
		}
	}
	ua := PhysicalUnionAll{
		mpp: canUseMpp && prop.TaskTp == property.MppTaskType,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
	ua.SetSchema(p.Schema())
	if canUseMpp && prop.TaskTp == property.RootTaskType {
		chReqProps = make([]*property.PhysicalProperty, 0, p.ChildLen())
		for range p.Children() {
			chReqProps = append(chReqProps, &property.PhysicalProperty{
				ExpectedCnt:       prop.ExpectedCnt,
				TaskTp:            property.MppTaskType,
				RejectSort:        true,
				CTEProducerStatus: prop.CTEProducerStatus,
			})
		}
		mppUA := PhysicalUnionAll{mpp: true}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
		mppUA.SetSchema(p.Schema())
		return []base.PhysicalPlan{ua, mppUA}, true, nil
	}
	return []base.PhysicalPlan{ua}, true, nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalPartitionUnionAll) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	uas, flagHint, err := p.LogicalUnionAll.ExhaustPhysicalPlans(prop)
	if err != nil {
		return nil, false, err
	}
	for _, ua := range uas {
		ua.(*PhysicalUnionAll).SetTP(plancodec.TypePartitionUnion)
	}
	return uas, flagHint, nil
}

func (ls *LogicalSort) getPhysicalSort(prop *property.PhysicalProperty) *PhysicalSort {
	ps := PhysicalSort{ByItems: ls.ByItems}.Init(ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ls.QueryBlockOffset(), &property.PhysicalProperty{TaskTp: prop.TaskTp, ExpectedCnt: math.MaxFloat64, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus})
	return ps
}

func (ls *LogicalSort) getNominalSort(reqProp *property.PhysicalProperty) *NominalSort {
	prop, canPass, onlyColumn := GetPropByOrderByItemsContainScalarFunc(ls.ByItems)
	if !canPass {
		return nil
	}
	prop.RejectSort = true
	prop.ExpectedCnt = reqProp.ExpectedCnt
	ps := NominalSort{OnlyColumn: onlyColumn, ByItems: ls.ByItems}.Init(
		ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ls.QueryBlockOffset(), prop)
	return ps
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (ls *LogicalSort) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if prop.TaskTp == property.RootTaskType {
		if MatchItems(prop, ls.ByItems) {
			ret := make([]base.PhysicalPlan, 0, 2)
			ret = append(ret, ls.getPhysicalSort(prop))
			ns := ls.getNominalSort(prop)
			if ns != nil {
				ret = append(ret, ns)
			}
			return ret, true, nil
		}
	} else if prop.TaskTp == property.MppTaskType && prop.RejectSort {
		if canPushToCopImpl(&ls.BaseLogicalPlan, kv.TiFlash, true) {
			newProp := prop.CloneEssentialFields()
			newProp.RejectSort = true
			ps := NominalSort{OnlyColumn: true, ByItems: ls.ByItems}.Init(
				ls.SCtx(), ls.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ls.QueryBlockOffset(), newProp)
			return []base.PhysicalPlan{ps}, true, nil
		}
	}
	return nil, true, nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalMaxOneRow) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if !prop.IsSortItemEmpty() || prop.IsFlashProp() {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because operator `MaxOneRow` is not supported now.")
		return nil, true, nil
	}
	mor := PhysicalMaxOneRow{}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), &property.PhysicalProperty{ExpectedCnt: 2, CTEProducerStatus: prop.CTEProducerStatus})
	return []base.PhysicalPlan{mor}, true, nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalCTE) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	pcte := PhysicalCTE{CTE: p.cte}.Init(p.SCtx(), p.StatsInfo())
	if prop.IsFlashProp() {
		pcte.storageSender = PhysicalExchangeSender{
			ExchangeType: tipb.ExchangeType_Broadcast,
		}.Init(p.SCtx(), p.StatsInfo())
	}
	pcte.SetSchema(p.schema)
	pcte.childrenReqProps = []*property.PhysicalProperty{prop.CloneEssentialFields()}
	return []base.PhysicalPlan{(*PhysicalCTEStorage)(pcte)}, true, nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalSequence) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	possibleChildrenProps := make([][]*property.PhysicalProperty, 0, 2)
	anyType := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.AnyType, CanAddEnforcer: true, RejectSort: true, CTEProducerStatus: prop.CTEProducerStatus}
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
		p.SCtx().GetSessionVars().IsMPPAllowed() && prop.IsSortItemEmpty() {
		possibleChildrenProps = append(possibleChildrenProps, []*property.PhysicalProperty{anyType, anyType.CloneEssentialFields()})
	}
	seqs := make([]base.PhysicalPlan, 0, 2)
	for _, propChoice := range possibleChildrenProps {
		childReqs := make([]*property.PhysicalProperty, 0, p.ChildLen())
		for i := 0; i < p.ChildLen()-1; i++ {
			childReqs = append(childReqs, propChoice[0].CloneEssentialFields())
		}
		childReqs = append(childReqs, propChoice[1])
		seq := PhysicalSequence{}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), childReqs...)
		seq.SetSchema(p.Children()[p.ChildLen()-1].Schema())
		seqs = append(seqs, seq)
	}
	return seqs, true, nil
}
