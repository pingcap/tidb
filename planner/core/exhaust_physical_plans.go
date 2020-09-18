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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
)

func (p *LogicalUnionScan) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if prop.IsFlashOnlyProp() {
		return nil, true
	}
	childProp := prop.Clone()
	us := PhysicalUnionScan{
		Conditions: p.conditions,
		HandleCol:  p.handleCol,
	}.Init(p.ctx, p.stats, p.blockOffset, childProp)
	return []PhysicalPlan{us}, true
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
			if i < len(candidateKeys) && keys[i].Equal(nil, candidateKeys[i]) {
				matchedLen++
			} else {
				break
			}
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
	if !prop.IsEmpty() {
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

func (p *LogicalJoin) checkJoinKeyCollation(leftKeys, rightKeys []*expression.Column) bool {
	// if a left key and its corresponding right key have different collation, don't use MergeJoin since
	// the their children may sort their records in different ways
	for i := range leftKeys {
		lt := leftKeys[i].RetType
		rt := rightKeys[i].RetType
		if (lt.EvalType() == types.ETString && rt.EvalType() == types.ETString) &&
			(leftKeys[i].RetType.Charset != rightKeys[i].RetType.Charset ||
				leftKeys[i].RetType.Collate != rightKeys[i].RetType.Collate) {
			return false
		}
	}
	return true
}

// GetMergeJoin convert the logical join to physical merge join based on the physical property.
func (p *LogicalJoin) GetMergeJoin(prop *property.PhysicalProperty, schema *expression.Schema, statsInfo *property.StatsInfo, leftStatsInfo *property.StatsInfo, rightStatsInfo *property.StatsInfo) []PhysicalPlan {
	joins := make([]PhysicalPlan, 0, len(p.leftProperties)+1)
	// The leftProperties caches all the possible properties that are provided by its children.
	leftJoinKeys, rightJoinKeys := p.GetJoinKeys()
	for _, lhsChildProperty := range p.leftProperties {
		offsets := getMaxSortPrefix(lhsChildProperty, leftJoinKeys)
		if len(offsets) == 0 {
			continue
		}

		leftKeys := lhsChildProperty[:len(offsets)]
		rightKeys := expression.NewSchema(rightJoinKeys...).ColumnsByIndices(offsets)

		prefixLen := findMaxPrefixLen(p.rightProperties, rightKeys)
		if prefixLen == 0 {
			continue
		}

		leftKeys = leftKeys[:prefixLen]
		rightKeys = rightKeys[:prefixLen]
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
		}
		mergeJoin := PhysicalMergeJoin{basePhysicalJoin: baseJoin}.Init(p.ctx, statsInfo.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset)
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
	// If TiDB_SMJ hint is existed, it should consider enforce merge join,
	// because we can't trust lhsChildProperty completely.
	if (p.preferJoinType & preferMergeJoin) > 0 {
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

func (p *LogicalJoin) getEnforcedMergeJoin(prop *property.PhysicalProperty, schema *expression.Schema, statsInfo *property.StatsInfo) []PhysicalPlan {
	// Check whether SMJ can satisfy the required property
	leftJoinKeys, rightJoinKeys := p.GetJoinKeys()
	offsets := make([]int, 0, len(leftJoinKeys))
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}
	for _, item := range prop.Items {
		isExist := false
		for joinKeyPos := 0; joinKeyPos < len(leftJoinKeys); joinKeyPos++ {
			var key *expression.Column
			if item.Col.Equal(p.ctx, leftJoinKeys[joinKeyPos]) {
				key = leftJoinKeys[joinKeyPos]
			}
			if item.Col.Equal(p.ctx, rightJoinKeys[joinKeyPos]) {
				key = rightJoinKeys[joinKeyPos]
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
	}
	// Generate the enforced sort merge join
	leftKeys := getNewJoinKeysByOffsets(leftJoinKeys, offsets)
	rightKeys := getNewJoinKeysByOffsets(rightJoinKeys, offsets)
	otherConditions := make([]expression.Expression, len(p.OtherConditions), len(p.OtherConditions)+len(p.EqualConditions))
	copy(otherConditions, p.OtherConditions)
	if !p.checkJoinKeyCollation(leftKeys, rightKeys) {
		// if the join keys' collation are conflicted, we use the empty join key
		// and move EqualConditions to OtherConditions.
		leftKeys = nil
		rightKeys = nil
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
		OtherConditions: otherConditions,
	}
	enforcedPhysicalMergeJoin := PhysicalMergeJoin{basePhysicalJoin: baseJoin, Desc: desc}.Init(p.ctx, statsInfo.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset)
	enforcedPhysicalMergeJoin.SetSchema(schema)
	enforcedPhysicalMergeJoin.childrenReqProps = []*property.PhysicalProperty{lProp, rProp}
	enforcedPhysicalMergeJoin.initCompareFuncs()
	return []PhysicalPlan{enforcedPhysicalMergeJoin}
}

func (p *PhysicalMergeJoin) initCompareFuncs() {
	p.CompareFuncs = make([]expression.CompareFunc, 0, len(p.LeftJoinKeys))
	for i := range p.LeftJoinKeys {
		p.CompareFuncs = append(p.CompareFuncs, expression.GetCmpFunction(p.ctx, p.LeftJoinKeys[i], p.RightJoinKeys[i]))
	}
}

// ForceUseOuterBuild4Test is a test option to control forcing use outer input as build.
// TODO: use hint and remove this variable
var ForceUseOuterBuild4Test = false

// ForcedHashLeftJoin4Test is a test option to force using HashLeftJoin
// TODO: use hint and remove this variable
var ForcedHashLeftJoin4Test = false

func (p *LogicalJoin) getHashJoins(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.IsEmpty() { // hash join doesn't promise any orders
		return nil
	}
	joins := make([]PhysicalPlan, 0, 2)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		joins = append(joins, p.getHashJoin(prop, 1, false))
	case LeftOuterJoin:
		if ForceUseOuterBuild4Test {
			joins = append(joins, p.getHashJoin(prop, 1, true))
		} else {
			joins = append(joins, p.getHashJoin(prop, 1, false))
			joins = append(joins, p.getHashJoin(prop, 1, true))
		}
	case RightOuterJoin:
		if ForceUseOuterBuild4Test {
			joins = append(joins, p.getHashJoin(prop, 0, true))
		} else {
			joins = append(joins, p.getHashJoin(prop, 0, false))
			joins = append(joins, p.getHashJoin(prop, 0, true))
		}
	case InnerJoin:
		if ForcedHashLeftJoin4Test {
			joins = append(joins, p.getHashJoin(prop, 1, false))
		} else {
			joins = append(joins, p.getHashJoin(prop, 1, false))
			joins = append(joins, p.getHashJoin(prop, 0, false))
		}
	}
	return joins
}

func (p *LogicalJoin) getHashJoin(prop *property.PhysicalProperty, innerIdx int, useOuterToBuild bool) *PhysicalHashJoin {
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	chReqProps[1-innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	if prop.ExpectedCnt < p.stats.RowCount {
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		chReqProps[1-innerIdx].ExpectedCnt = p.children[1-innerIdx].statsInfo().RowCount * expCntScale
	}
	hashJoin := NewPhysicalHashJoin(p, innerIdx, useOuterToBuild, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(p.schema)
	return hashJoin
}

// When inner plan is TableReader, the parameter `ranges` will be nil. Because pk only have one column. So all of its range
// is generated during execution time.
func (p *LogicalJoin) constructIndexJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask task,
	ranges []*ranger.Range,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *ColWithCmpFuncManager,
) []PhysicalPlan {
	joinType := p.JoinType
	var (
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys = p.GetJoinKeys()
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64, Items: prop.Items}
	if prop.ExpectedCnt < p.stats.RowCount {
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		chReqProps[outerIdx].ExpectedCnt = p.children[outerIdx].statsInfo().RowCount * expCntScale
	}
	newInnerKeys := make([]*expression.Column, 0, len(innerJoinKeys))
	newOuterKeys := make([]*expression.Column, 0, len(outerJoinKeys))
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
		newKeyOff = append(newKeyOff, idxOff)
	}
	baseJoin := basePhysicalJoin{
		InnerChildIdx:   1 - outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: newOtherConds,
		JoinType:        joinType,
		OuterJoinKeys:   newOuterKeys,
		InnerJoinKeys:   newInnerKeys,
		DefaultValues:   p.DefaultValues,
	}
	join := PhysicalIndexJoin{
		basePhysicalJoin: baseJoin,
		innerTask:        innerTask,
		KeyOff2IdxOff:    newKeyOff,
		Ranges:           ranges,
		CompareFilters:   compareFilters,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, chReqProps...)
	if path != nil {
		join.IdxColLens = path.IdxColLens
	}
	join.SetSchema(p.schema)
	return []PhysicalPlan{join}
}

func (p *LogicalJoin) constructIndexMergeJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask task,
	ranges []*ranger.Range,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *ColWithCmpFuncManager,
) []PhysicalPlan {
	indexJoins := p.constructIndexJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, compareFilters)
	indexMergeJoins := make([]PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*PhysicalIndexJoin)
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
		sort.Slice(keyOffMapList, func(i, j int) bool { return keyOffMapList[i] < keyOffMapList[j] })
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
		isOuterKeysPrefix := len(join.OuterJoinKeys) <= len(prop.Items)
		compareFuncs := make([]expression.CompareFunc, 0, len(join.OuterJoinKeys))
		outerCompareFuncs := make([]expression.CompareFunc, 0, len(join.OuterJoinKeys))

		for i := range join.KeyOff2IdxOff {
			if isOuterKeysPrefix && !prop.Items[i].Col.Equal(nil, join.OuterJoinKeys[keyOff2KeyOffOrderByIdx[i]]) {
				isOuterKeysPrefix = false
			}
			compareFuncs = append(compareFuncs, expression.GetCmpFunction(p.ctx, join.OuterJoinKeys[i], join.InnerJoinKeys[i]))
			outerCompareFuncs = append(outerCompareFuncs, expression.GetCmpFunction(p.ctx, join.OuterJoinKeys[i], join.OuterJoinKeys[i]))
		}
		// canKeepOuterOrder means whether the prop items are the prefix of the outer join keys.
		canKeepOuterOrder := len(prop.Items) <= len(join.OuterJoinKeys)
		for i := 0; canKeepOuterOrder && i < len(prop.Items); i++ {
			if !prop.Items[i].Col.Equal(nil, join.OuterJoinKeys[keyOff2KeyOffOrderByIdx[i]]) {
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
				Desc:                    !prop.IsEmpty() && prop.Items[0].Desc,
			}.Init(p.ctx)
			indexMergeJoins = append(indexMergeJoins, indexMergeJoin)
		}
	}
	return indexMergeJoins
}

func (p *LogicalJoin) constructIndexHashJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask task,
	ranges []*ranger.Range,
	keyOff2IdxOff []int,
	path *util.AccessPath,
	compareFilters *ColWithCmpFuncManager,
) []PhysicalPlan {
	indexJoins := p.constructIndexJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, compareFilters)
	indexHashJoins := make([]PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*PhysicalIndexJoin)
		indexHashJoin := PhysicalIndexHashJoin{
			PhysicalIndexJoin: *join,
			// Prop is empty means that the parent operator does not need the
			// join operator to provide any promise of the output order.
			KeepOuterOrder: !prop.IsEmpty(),
		}.Init(p.ctx)
		indexHashJoins = append(indexHashJoins, indexHashJoin)
	}
	return indexHashJoins
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child.
// First of all, we'll check whether the inner child is DataSource.
// Then, we will extract the join keys of p's equal conditions. Then check whether all of them are just the primary key
// or match some part of on index. If so we will choose the best one and construct a index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(prop *property.PhysicalProperty, outerIdx int) (joins []PhysicalPlan) {
	outerChild, innerChild := p.children[outerIdx], p.children[1-outerIdx]
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
		outerJoinKeys, innerJoinKeys = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys = p.GetJoinKeys()
	}
	ds, isDataSource := innerChild.(*DataSource)
	us, isUnionScan := innerChild.(*LogicalUnionScan)
	if (!isDataSource && !isUnionScan) || (isDataSource && ds.preferStoreType&preferTiFlash != 0) {
		return nil
	}
	if isUnionScan {
		// The child of union scan may be union all for partition table.
		ds, isDataSource = us.Children()[0].(*DataSource)
		if !isDataSource {
			return nil
		}
		// If one of the union scan children is a TiFlash table, then we can't choose index join.
		for _, child := range us.Children() {
			if ds, ok := child.(*DataSource); ok && ds.preferStoreType&preferTiFlash != 0 {
				return nil
			}
		}
	}
	var avgInnerRowCnt float64
	if outerChild.statsInfo().RowCount > 0 {
		avgInnerRowCnt = p.equalCondOutCnt / outerChild.statsInfo().RowCount
	}
	joins = p.buildIndexJoinInner2TableScan(prop, ds, innerJoinKeys, outerJoinKeys, outerIdx, us, avgInnerRowCnt)
	if joins != nil {
		return
	}
	return p.buildIndexJoinInner2IndexScan(prop, ds, innerJoinKeys, outerJoinKeys, outerIdx, us, avgInnerRowCnt)
}

// buildIndexJoinInner2TableScan builds a TableScan as the inner child for an
// IndexJoin if possible.
// If the inner side of a index join is a TableScan, only one tuple will be
// fetched from the inner side for every tuple from the outer side. This will be
// promised to be no worse than building IndexScan as the inner child.
func (p *LogicalJoin) buildIndexJoinInner2TableScan(
	prop *property.PhysicalProperty, ds *DataSource, innerJoinKeys, outerJoinKeys []*expression.Column,
	outerIdx int, us *LogicalUnionScan, avgInnerRowCnt float64) (joins []PhysicalPlan) {
	var tblPath *util.AccessPath
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath && path.StoreType == kv.TiKV {
			tblPath = path
			break
		}
	}
	if tblPath == nil {
		return nil
	}
	pkCol := ds.getPKIsHandleCol()
	if pkCol == nil {
		return nil
	}
	keyOff2IdxOff := make([]int, len(innerJoinKeys))
	newOuterJoinKeys := make([]*expression.Column, 0)
	pkMatched := false
	for i, key := range innerJoinKeys {
		if !key.Equal(nil, pkCol) {
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
	joins = make([]PhysicalPlan, 0, 3)
	innerTask := p.constructInnerTableScanTask(ds, pkCol, outerJoinKeys, us, false, false, avgInnerRowCnt)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(p.constructIndexHashJoin(prop, outerIdx, innerTask, nil, keyOff2IdxOff, nil, nil))
		}
	})
	joins = append(joins, p.constructIndexJoin(prop, outerIdx, innerTask, nil, keyOff2IdxOff, nil, nil)...)
	// The index merge join's inner plan is different from index join, so we
	// should construct another inner plan for it.
	// Because we can't keep order for union scan, if there is a union scan in inner task,
	// we can't construct index merge join.
	if us == nil {
		innerTask2 := p.constructInnerTableScanTask(ds, pkCol, outerJoinKeys, us, true, !prop.IsEmpty() && prop.Items[0].Desc, avgInnerRowCnt)
		joins = append(joins, p.constructIndexMergeJoin(prop, outerIdx, innerTask2, nil, keyOff2IdxOff, nil, nil)...)
	}
	// We can reuse the `innerTask` here since index nested loop hash join
	// do not need the inner child to promise the order.
	joins = append(joins, p.constructIndexHashJoin(prop, outerIdx, innerTask, nil, keyOff2IdxOff, nil, nil)...)
	return joins
}

func (p *LogicalJoin) buildIndexJoinInner2IndexScan(
	prop *property.PhysicalProperty, ds *DataSource, innerJoinKeys, outerJoinKeys []*expression.Column,
	outerIdx int, us *LogicalUnionScan, avgInnerRowCnt float64) (joins []PhysicalPlan) {
	helper := &indexJoinBuildHelper{join: p}
	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath {
			continue
		}
		emptyRange, err := helper.analyzeLookUpFilters(path, ds, innerJoinKeys)
		if emptyRange {
			return nil
		}
		if err != nil {
			logutil.BgLogger().Warn("build index join failed", zap.Error(err))
		}
	}
	if helper.chosenPath == nil {
		return nil
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
	joins = make([]PhysicalPlan, 0, 3)
	rangeInfo := helper.buildRangeDecidedByInformation(helper.chosenPath.IdxCols, outerJoinKeys)
	maxOneRow := false
	if helper.chosenPath.Index.Unique && helper.maxUsedCols == len(helper.chosenPath.FullIdxCols) {
		l := len(helper.chosenAccess)
		if l == 0 {
			maxOneRow = true
		} else {
			sf, ok := helper.chosenAccess[l-1].(*expression.ScalarFunction)
			maxOneRow = ok && (sf.FuncName.L == ast.EQ)
		}
	}
	innerTask := p.constructInnerIndexScanTask(ds, helper.chosenPath, helper.chosenRemained, outerJoinKeys, us, rangeInfo, false, false, avgInnerRowCnt, maxOneRow)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(p.constructIndexHashJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastColManager))
		}
	})
	joins = append(joins, p.constructIndexJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastColManager)...)
	// The index merge join's inner plan is different from index join, so we
	// should construct another inner plan for it.
	// Because we can't keep order for union scan, if there is a union scan in inner task,
	// we can't construct index merge join.
	if us == nil {
		innerTask2 := p.constructInnerIndexScanTask(ds, helper.chosenPath, helper.chosenRemained, outerJoinKeys, us, rangeInfo, true, !prop.IsEmpty() && prop.Items[0].Desc, avgInnerRowCnt, maxOneRow)
		joins = append(joins, p.constructIndexMergeJoin(prop, outerIdx, innerTask2, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastColManager)...)
	}
	// We can reuse the `innerTask` here since index nested loop hash join
	// do not need the inner child to promise the order.
	joins = append(joins, p.constructIndexHashJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastColManager)...)
	return joins
}

type indexJoinBuildHelper struct {
	join *LogicalJoin

	chosenIndexInfo *model.IndexInfo
	maxUsedCols     int
	chosenAccess    []expression.Expression
	chosenRemained  []expression.Expression
	idxOff2KeyOff   []int
	lastColManager  *ColWithCmpFuncManager
	chosenRanges    []*ranger.Range
	chosenPath      *util.AccessPath

	curPossibleUsedKeys []*expression.Column
	curNotUsedIndexCols []*expression.Column
	curNotUsedColLens   []int
	curIdxOff2KeyOff    []int
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
		buffer.WriteString(fmt.Sprintf("eq(%v, %v)", idxCols[idxOff], outerJoinKeys[keyOff]))
	}
	for _, access := range ijHelper.chosenAccess {
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		buffer.WriteString(fmt.Sprintf("%v", access))
	}
	buffer.WriteString("]")
	return buffer.String()
}

// constructInnerTableScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerTableScanTask(
	ds *DataSource,
	pk *expression.Column,
	outerJoinKeys []*expression.Column,
	us *LogicalUnionScan,
	keepOrder bool,
	desc bool,
	rowCount float64,
) task {
	ranges := ranger.FullIntRange(mysql.HasUnsignedFlag(pk.RetType.Flag))
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		filterCondition: ds.pushedDownConds,
		Ranges:          ranges,
		rangeDecidedBy:  outerJoinKeys,
		KeepOrder:       keepOrder,
		Desc:            desc,
		physicalTableID: ds.physicalTableID,
		isPartition:     ds.isPartition,
	}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.schema.Clone())
	if rowCount <= 0 {
		rowCount = float64(1)
	}
	selectivity := float64(1)
	countAfterAccess := rowCount
	if len(ts.filterCondition) > 0 {
		var err error
		selectivity, _, err = ds.tableStats.HistColl.Selectivity(ds.ctx, ts.filterCondition, ds.possibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ts.TableAsName.L))
			selectivity = SelectionFactor
		}
		// rowCount is computed from result row count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterAccess * selectivity`.
		countAfterAccess = rowCount / selectivity
	}
	ts.stats = &property.StatsInfo{
		// TableScan as inner child of IndexJoin can return at most 1 tuple for each outer row.
		RowCount:     math.Min(1.0, countAfterAccess),
		StatsVersion: ds.stats.StatsVersion,
		// Cardinality would not be used in cost computation of IndexJoin, set leave it as default nil.
	}
	rowSize := ds.TblColHists.GetTableAvgRowSize(p.ctx, ds.TblCols, ts.StoreType, true)
	sessVars := ds.ctx.GetSessionVars()
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
		cst:               sessVars.ScanFactor * rowSize * ts.stats.RowCount,
		tblColHists:       ds.TblColHists,
		keepOrder:         ts.KeepOrder,
	}
	selStats := ts.stats.Scale(selectivity)
	ts.addPushedDownSelection(copTask, selStats)
	t := finishCopTask(ds.ctx, copTask).(*rootTask)
	reader := t.p
	t.p = p.constructInnerUnionScan(us, reader)
	return t
}

func (p *LogicalJoin) constructInnerUnionScan(us *LogicalUnionScan, reader PhysicalPlan) PhysicalPlan {
	if us == nil {
		return reader
	}
	// Use `reader.stats` instead of `us.stats` because it should be more accurate. No need to specify
	// childrenReqProps now since we have got reader already.
	physicalUnionScan := PhysicalUnionScan{
		Conditions: us.conditions,
		HandleCol:  us.handleCol,
	}.Init(us.ctx, reader.statsInfo(), us.blockOffset, nil)
	physicalUnionScan.SetChildren(reader)
	return physicalUnionScan
}

// constructInnerIndexScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerIndexScanTask(
	ds *DataSource,
	path *util.AccessPath,
	filterConds []expression.Expression,
	outerJoinKeys []*expression.Column,
	us *LogicalUnionScan,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
	maxOneRow bool,
) task {
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
		Ranges:           ranger.FullRange(),
		rangeInfo:        rangeInfo,
		Desc:             desc,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
	}.Init(ds.ctx, ds.blockOffset)
	cop := &copTask{
		indexPlan:   is,
		tblColHists: ds.TblColHists,
		tblCols:     ds.TblCols,
		keepOrder:   is.KeepOrder,
	}
	if !isCoveringIndex(ds.schema.Columns, path.FullIdxCols, path.FullIdxColLens, is.Table.PKIsHandle) {
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			TableAsName:     ds.TableAsName,
			isPartition:     ds.isPartition,
			physicalTableID: ds.physicalTableID,
		}.Init(ds.ctx, ds.blockOffset)
		ts.schema = is.dataSourceSchema.Clone()
		// If inner cop task need keep order, the extraHandleCol should be set.
		if cop.keepOrder {
			cop.extraHandleCol, cop.doubleReadNeedProj = ts.appendExtraHandleCol(ds)
		}
		cop.tablePlan = ts
	}
	is.initSchema(path.Index, path.FullIdxCols, cop.tablePlan != nil)
	indexConds, tblConds := splitIndexFilterConditions(filterConds, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo)
	// Specially handle cases when input rowCount is 0, which can only happen in 2 scenarios:
	// - estimated row count of outer plan is 0;
	// - estimated row count of inner "DataSource + filters" is 0;
	// if it is the first case, it does not matter what row count we set for inner task, since the cost of index join would
	// always be 0 then;
	// if it is the second case, HashJoin should always be cheaper than IndexJoin then, so we set row count of inner task
	// to table size, to simply make it more expensive.
	if rowCount <= 0 {
		rowCount = ds.tableStats.RowCount
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
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, tblConds, ds.possibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ds.TableAsName.L))
			selectivity = SelectionFactor
		}
		// rowCount is computed from result row count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterIndex * selectivity`.
		cnt := rowCount / selectivity
		if maxOneRow {
			cnt = math.Min(cnt, 1.0)
		}
		tmpPath.CountAfterIndex = cnt
		tmpPath.CountAfterAccess = cnt
	}
	if len(indexConds) > 0 {
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, indexConds, ds.possibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ds.TableAsName.L))
			selectivity = SelectionFactor
		}
		cnt := tmpPath.CountAfterIndex / selectivity
		if maxOneRow {
			cnt = math.Min(cnt, 1.0)
		}
		tmpPath.CountAfterAccess = cnt
	}
	is.stats = ds.tableStats.ScaleByExpectCnt(tmpPath.CountAfterAccess)
	rowSize := is.indexScanRowSize(path.Index, ds, true)
	sessVars := ds.ctx.GetSessionVars()
	cop.cst = tmpPath.CountAfterAccess * rowSize * sessVars.ScanFactor
	finalStats := ds.tableStats.ScaleByExpectCnt(rowCount)
	is.addPushedDownSelection(cop, ds, tmpPath, finalStats)
	t := finishCopTask(ds.ctx, cop).(*rootTask)
	reader := t.p
	t.p = p.constructInnerUnionScan(us, reader)
	return t
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
func (cwc *ColWithCmpFuncManager) BuildRangesByRow(ctx sessionctx.Context, row chunk.Row) ([]*ranger.Range, error) {
	exprs := make([]expression.Expression, len(cwc.OpType))
	for i, opType := range cwc.OpType {
		constantArg, err := cwc.opArg[i].Eval(row)
		if err != nil {
			return nil, err
		}
		cwc.TmpConstant[i].Value = constantArg
		newExpr, err := expression.NewFunction(ctx, opType, types.NewFieldType(mysql.TypeTiny), cwc.TargetCol, cwc.TmpConstant[i])
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, newExpr)
	}
	ranges, err := ranger.BuildColumnRange(exprs, ctx.GetSessionVars().StmtCtx, cwc.TargetCol.RetType, cwc.colLength)
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
		buffer.WriteString(fmt.Sprintf("%v(%v, %v)", cwc.OpType[i], cwc.TargetCol, cwc.opArg[i]))
		if i < len(cwc.OpType)-1 {
			buffer.WriteString(" ")
		}
	}
	return buffer.String()
}

func (ijHelper *indexJoinBuildHelper) resetContextForIndex(innerKeys []*expression.Column, idxCols []*expression.Column, colLens []int) {
	tmpSchema := expression.NewSchema(innerKeys...)
	ijHelper.curIdxOff2KeyOff = make([]int, len(idxCols))
	ijHelper.curNotUsedIndexCols = make([]*expression.Column, 0, len(idxCols))
	ijHelper.curNotUsedColLens = make([]int, 0, len(idxCols))
	for i, idxCol := range idxCols {
		ijHelper.curIdxOff2KeyOff[i] = tmpSchema.ColumnIndex(idxCol)
		if ijHelper.curIdxOff2KeyOff[i] >= 0 {
			continue
		}
		ijHelper.curNotUsedIndexCols = append(ijHelper.curNotUsedIndexCols, idxCol)
		ijHelper.curNotUsedColLens = append(ijHelper.curNotUsedColLens, colLens[i])
	}
}

// findUsefulEqAndInFilters analyzes the pushedDownConds held by inner child and split them to three parts.
// usefulEqOrInFilters is the continuous eq/in conditions on current unused index columns.
// uselessFilters is the conditions which cannot be used for building ranges.
// remainingRangeCandidates is the other conditions for future use.
func (ijHelper *indexJoinBuildHelper) findUsefulEqAndInFilters(innerPlan *DataSource) (usefulEqOrInFilters, uselessFilters, remainingRangeCandidates []expression.Expression) {
	uselessFilters = make([]expression.Expression, 0, len(innerPlan.pushedDownConds))
	var remainedEqOrIn []expression.Expression
	// Extract the eq/in functions of possible join key.
	// you can see the comment of ExtractEqAndInCondition to get the meaning of the second return value.
	usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, _ = ranger.ExtractEqAndInCondition(
		innerPlan.ctx, innerPlan.pushedDownConds,
		ijHelper.curNotUsedIndexCols,
		ijHelper.curNotUsedColLens,
	)
	uselessFilters = append(uselessFilters, remainedEqOrIn...)
	return usefulEqOrInFilters, uselessFilters, remainingRangeCandidates
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
		if lCol, ok := sf.GetArgs()[0].(*expression.Column); ok && lCol.Equal(nil, nextCol) {
			anotherArg = sf.GetArgs()[1]
			funcName = sf.FuncName.L
		} else if rCol, ok := sf.GetArgs()[1].(*expression.Column); ok && rCol.Equal(nil, nextCol) {
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
//   t1 join t2 on t1.a=t2.a and t1.c=t2.c where t1.b > t2.b-10 and t1.b < t2.b+10 there's index(a, b, c) on t1.
//   In this case the curIdxOff2KeyOff is [0 -1 1] and the notKeyEqAndIn is [].
//   It's clearly that the column c cannot be used to access data. So we need to remove it and reset the IdxOff2KeyOff to
//   [0 -1 -1].
//   So that we can use t1.a=t2.a and t1.b > t2.b-10 and t1.b < t2.b+10 to build ranges then access data.
func (ijHelper *indexJoinBuildHelper) removeUselessEqAndInFunc(
	idxCols []*expression.Column,
	notKeyEqAndIn []expression.Expression) (
	usefulEqAndIn, uselessOnes []expression.Expression,
) {
	ijHelper.curPossibleUsedKeys = make([]*expression.Column, 0, len(idxCols))
	for idxColPos, notKeyColPos := 0, 0; idxColPos < len(idxCols); idxColPos++ {
		if ijHelper.curIdxOff2KeyOff[idxColPos] != -1 {
			ijHelper.curPossibleUsedKeys = append(ijHelper.curPossibleUsedKeys, idxCols[idxColPos])
			continue
		}
		if notKeyColPos < len(notKeyEqAndIn) && ijHelper.curNotUsedIndexCols[notKeyColPos].Equal(nil, idxCols[idxColPos]) {
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

func (ijHelper *indexJoinBuildHelper) analyzeLookUpFilters(path *util.AccessPath, innerPlan *DataSource, innerJoinKeys []*expression.Column) (emptyRange bool, err error) {
	if len(path.IdxCols) == 0 {
		return false, nil
	}
	accesses := make([]expression.Expression, 0, len(path.IdxCols))
	ijHelper.resetContextForIndex(innerJoinKeys, path.IdxCols, path.IdxColLens)
	notKeyEqAndIn, remained, rangeFilterCandidates := ijHelper.findUsefulEqAndInFilters(innerPlan)
	var remainedEqAndIn []expression.Expression
	notKeyEqAndIn, remainedEqAndIn = ijHelper.removeUselessEqAndInFunc(path.IdxCols, notKeyEqAndIn)
	matchedKeyCnt := len(ijHelper.curPossibleUsedKeys)
	// If no join key is matched while join keys actually are not empty. We don't choose index join for now.
	if matchedKeyCnt <= 0 && len(innerJoinKeys) > 0 {
		return false, nil
	}
	accesses = append(accesses, notKeyEqAndIn...)
	remained = append(remained, remainedEqAndIn...)
	lastColPos := matchedKeyCnt + len(notKeyEqAndIn)
	// There should be some equal conditions. But we don't need that there must be some join key in accesses here.
	// A more strict check is applied later.
	if lastColPos <= 0 {
		return false, nil
	}
	// If all the index columns are covered by eq/in conditions, we don't need to consider other conditions anymore.
	if lastColPos == len(path.IdxCols) {
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1. And t2 has index(a, b).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		remained = append(remained, rangeFilterCandidates...)
		ranges, emptyRange, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, false)
		if err != nil {
			return false, err
		}
		if emptyRange {
			return true, nil
		}
		ijHelper.updateBestChoice(ranges, path, accesses, remained, nil)
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
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1 and t2.c > 10 and t2.c < 20. And t2 has index(a, b, c).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		colAccesses, colRemained := ranger.DetachCondsForColumn(ijHelper.join.ctx, rangeFilterCandidates, lastPossibleCol)
		var ranges, nextColRange []*ranger.Range
		var err error
		if len(colAccesses) > 0 {
			nextColRange, err = ranger.BuildColumnRange(colAccesses, ijHelper.join.ctx.GetSessionVars().StmtCtx, lastPossibleCol.RetType, path.IdxColLens[lastColPos])
			if err != nil {
				return false, err
			}
		}
		ranges, emptyRange, err = ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nextColRange, false)
		if err != nil {
			return false, err
		}
		if emptyRange {
			return true, nil
		}
		remained = append(remained, colRemained...)
		if path.IdxColLens[lastColPos] != types.UnspecifiedLength {
			remained = append(remained, colAccesses...)
		}
		accesses = append(accesses, colAccesses...)
		ijHelper.updateBestChoice(ranges, path, accesses, remained, nil)
		return false, nil
	}
	accesses = append(accesses, lastColAccess...)
	remained = append(remained, rangeFilterCandidates...)
	ranges, emptyRange, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, true)
	if err != nil {
		return false, err
	}
	if emptyRange {
		return true, nil
	}
	ijHelper.updateBestChoice(ranges, path, accesses, remained, lastColManager)
	return false, nil
}

func (ijHelper *indexJoinBuildHelper) updateBestChoice(ranges []*ranger.Range, path *util.AccessPath, accesses,
	remained []expression.Expression, lastColManager *ColWithCmpFuncManager) {
	// We choose the index by the number of used columns of the range, the much the better.
	// Notice that there may be the cases like `t1.a=t2.a and b > 2 and b < 1`. So ranges can be nil though the conditions are valid.
	// But obviously when the range is nil, we don't need index join.
	if len(ranges) > 0 && len(ranges[0].LowVal) > ijHelper.maxUsedCols {
		ijHelper.chosenPath = path
		ijHelper.maxUsedCols = len(ranges[0].LowVal)
		ijHelper.chosenRanges = ranges
		ijHelper.chosenAccess = accesses
		ijHelper.chosenRemained = remained
		ijHelper.idxOff2KeyOff = ijHelper.curIdxOff2KeyOff
		ijHelper.lastColManager = lastColManager
	}
}

func (ijHelper *indexJoinBuildHelper) buildTemplateRange(matchedKeyCnt int, eqAndInFuncs []expression.Expression, nextColRange []*ranger.Range, haveExtraCol bool) (ranges []*ranger.Range, emptyRange bool, err error) {
	pointLength := matchedKeyCnt + len(eqAndInFuncs)
	if nextColRange != nil {
		for _, colRan := range nextColRange {
			// The range's exclude status is the same with last col's.
			ran := &ranger.Range{
				LowVal:      make([]types.Datum, pointLength, pointLength+1),
				HighVal:     make([]types.Datum, pointLength, pointLength+1),
				LowExclude:  colRan.LowExclude,
				HighExclude: colRan.HighExclude,
			}
			ran.LowVal = append(ran.LowVal, colRan.LowVal[0])
			ran.HighVal = append(ran.HighVal, colRan.HighVal[0])
			ranges = append(ranges, ran)
		}
	} else if haveExtraCol {
		// Reserve a position for the last col.
		ranges = append(ranges, &ranger.Range{
			LowVal:  make([]types.Datum, pointLength+1),
			HighVal: make([]types.Datum, pointLength+1),
		})
	} else {
		ranges = append(ranges, &ranger.Range{
			LowVal:  make([]types.Datum, pointLength),
			HighVal: make([]types.Datum, pointLength),
		})
	}
	sc := ijHelper.join.ctx.GetSessionVars().StmtCtx
	for i, j := 0, 0; j < len(eqAndInFuncs); i++ {
		// This position is occupied by join key.
		if ijHelper.curIdxOff2KeyOff[i] != -1 {
			continue
		}
		oneColumnRan, err := ranger.BuildColumnRange([]expression.Expression{eqAndInFuncs[j]}, sc, ijHelper.curNotUsedIndexCols[j].RetType, ijHelper.curNotUsedColLens[j])
		if err != nil {
			return nil, false, err
		}
		if len(oneColumnRan) == 0 {
			return nil, true, nil
		}
		for _, ran := range ranges {
			ran.LowVal[i] = oneColumnRan[0].LowVal[0]
			ran.HighVal[i] = oneColumnRan[0].HighVal[0]
		}
		curRangeLen := len(ranges)
		for ranIdx := 1; ranIdx < len(oneColumnRan); ranIdx++ {
			newRanges := make([]*ranger.Range, 0, curRangeLen)
			for oldRangeIdx := 0; oldRangeIdx < curRangeLen; oldRangeIdx++ {
				newRange := ranges[oldRangeIdx].Clone()
				newRange.LowVal[i] = oneColumnRan[ranIdx].LowVal[0]
				newRange.HighVal[i] = oneColumnRan[ranIdx].HighVal[0]
				newRanges = append(newRanges, newRange)
			}
			ranges = append(ranges, newRanges...)
		}
		j++
	}
	return ranges, false, nil
}

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin(prop *property.PhysicalProperty) (indexJoins []PhysicalPlan, canForced bool) {
	inljRightOuter := (p.preferJoinType & preferLeftAsINLJInner) > 0
	inljLeftOuter := (p.preferJoinType & preferRightAsINLJInner) > 0
	hasINLJHint := inljLeftOuter || inljRightOuter

	inlhjRightOuter := (p.preferJoinType & preferLeftAsINLHJInner) > 0
	inlhjLeftOuter := (p.preferJoinType & preferRightAsINLHJInner) > 0
	hasINLHJHint := inlhjLeftOuter || inlhjRightOuter

	inlmjRightOuter := (p.preferJoinType & preferLeftAsINLMJInner) > 0
	inlmjLeftOuter := (p.preferJoinType & preferRightAsINLMJInner) > 0
	hasINLMJHint := inlmjLeftOuter || inlmjRightOuter

	forceLeftOuter := inljLeftOuter || inlhjLeftOuter || inlmjLeftOuter
	forceRightOuter := inljRightOuter || inlhjRightOuter || inlmjRightOuter
	needForced := forceLeftOuter || forceRightOuter

	defer func() {
		// refine error message
		// If the required property is not empty, we will enforce it and try the hint again.
		// So we only need to generate warning message when the property is empty.
		if !canForced && needForced && prop.IsEmpty() {
			// Construct warning message prefix.
			var errMsg string
			switch {
			case hasINLJHint:
				errMsg = "Optimizer Hint INL_JOIN or TIDB_INLJ is inapplicable"
			case hasINLHJHint:
				errMsg = "Optimizer Hint INL_HASH_JOIN is inapplicable"
			case hasINLMJHint:
				errMsg = "Optimizer Hint INL_MERGE_JOIN is inapplicable"
			}
			if p.hintInfo != nil {
				t := p.hintInfo.indexNestedLoopJoinTables
				switch {
				case len(t.inljTables) != 0:
					errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable",
						restore2JoinHint(HintINLJ, t.inljTables), restore2JoinHint(TiDBIndexNestedLoopJoin, t.inljTables))
				case len(t.inlhjTables) != 0:
					errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", restore2JoinHint(HintINLHJ, t.inlhjTables))
				case len(t.inlmjTables) != 0:
					errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", restore2JoinHint(HintINLMJ, t.inlmjTables))
				}
			}

			// Append inapplicable reason.
			if len(p.EqualConditions) == 0 {
				errMsg += " without column equal ON condition"
			}

			// Generate warning message to client.
			warning := ErrInternal.GenWithStack(errMsg)
			p.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
		}
	}()

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

	var allLeftOuterJoins, allRightOuterJoins, forcedLeftOuterJoins, forcedRightOuterJoins []PhysicalPlan
	if supportLeftOuter {
		allLeftOuterJoins = p.getIndexJoinByOuterIdx(prop, 0)
		forcedLeftOuterJoins = make([]PhysicalPlan, 0, len(allLeftOuterJoins))
		for _, j := range allLeftOuterJoins {
			switch j.(type) {
			case *PhysicalIndexJoin:
				if inljLeftOuter {
					forcedLeftOuterJoins = append(forcedLeftOuterJoins, j)
				}
			case *PhysicalIndexHashJoin:
				if inlhjLeftOuter {
					forcedLeftOuterJoins = append(forcedLeftOuterJoins, j)
				}
			case *PhysicalIndexMergeJoin:
				if inlmjLeftOuter {
					forcedLeftOuterJoins = append(forcedLeftOuterJoins, j)
				}
			}
		}
		switch {
		case len(forcedLeftOuterJoins) == 0 && !supportRightOuter:
			return allLeftOuterJoins, false
		case len(forcedLeftOuterJoins) != 0 && (!supportRightOuter || (forceLeftOuter && !forceRightOuter)):
			return forcedLeftOuterJoins, true
		}
	}
	if supportRightOuter {
		allRightOuterJoins = p.getIndexJoinByOuterIdx(prop, 1)
		forcedRightOuterJoins = make([]PhysicalPlan, 0, len(allRightOuterJoins))
		for _, j := range allRightOuterJoins {
			switch j.(type) {
			case *PhysicalIndexJoin:
				if inljRightOuter {
					forcedRightOuterJoins = append(forcedRightOuterJoins, j)
				}
			case *PhysicalIndexHashJoin:
				if inlhjRightOuter {
					forcedRightOuterJoins = append(forcedRightOuterJoins, j)
				}
			case *PhysicalIndexMergeJoin:
				if inlmjRightOuter {
					forcedRightOuterJoins = append(forcedRightOuterJoins, j)
				}
			}
		}
		switch {
		case len(forcedRightOuterJoins) == 0 && !supportLeftOuter:
			return allRightOuterJoins, false
		case len(forcedRightOuterJoins) != 0 && (!supportLeftOuter || (forceRightOuter && !forceLeftOuter)):
			return forcedRightOuterJoins, true
		}
	}

	canForceLeft := len(forcedLeftOuterJoins) != 0 && forceLeftOuter
	canForceRight := len(forcedRightOuterJoins) != 0 && forceRightOuter
	canForced = canForceLeft || canForceRight
	if canForced {
		return append(forcedLeftOuterJoins, forcedRightOuterJoins...), true
	}
	return append(allLeftOuterJoins, allRightOuterJoins...), false
}

// LogicalJoin can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) {
			indexJoins, _ := p.tryToGetIndexJoin(prop)
			failpoint.Return(indexJoins, true)
		}
	})

	if prop.IsFlashOnlyProp() && ((p.preferJoinType&preferBCJoin) == 0 && p.preferJoinType > 0) {
		return nil, false
	}
	joins := make([]PhysicalPlan, 0, 8)
	if p.ctx.GetSessionVars().AllowBCJ {
		broadCastJoins := p.tryToGetBroadCastJoin(prop)
		if (p.preferJoinType & preferBCJoin) > 0 {
			return broadCastJoins, true
		}
		joins = append(joins, broadCastJoins...)
	}
	if prop.IsFlashOnlyProp() {
		return joins, true
	}

	mergeJoins := p.GetMergeJoin(prop, p.schema, p.Stats(), p.children[0].statsInfo(), p.children[1].statsInfo())
	if (p.preferJoinType&preferMergeJoin) > 0 && len(mergeJoins) > 0 {
		return mergeJoins, true
	}
	joins = append(joins, mergeJoins...)

	indexJoins, forced := p.tryToGetIndexJoin(prop)
	if forced {
		return indexJoins, true
	}
	joins = append(joins, indexJoins...)

	hashJoins := p.getHashJoins(prop)
	if (p.preferJoinType&preferHashJoin) > 0 && len(hashJoins) > 0 {
		return hashJoins, true
	}
	joins = append(joins, hashJoins...)

	if p.preferJoinType > 0 {
		// If we reach here, it means we have a hint that doesn't work.
		// It might be affected by the required property, so we enforce
		// this property and try the hint again.
		return joins, false
	}
	return joins, true
}

func (p *LogicalJoin) tryToGetBroadCastJoin(prop *property.PhysicalProperty) []PhysicalPlan {
	/// todo remove this restriction after join on new collation is supported in TiFlash
	if collate.NewCollationEnabled() {
		return nil
	}
	if !prop.IsEmpty() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && !prop.IsFlashOnlyProp() {
		return nil
	}

	// for left join the global idx must be 1, and for right join the global idx must be 0
	if (p.JoinType != InnerJoin && p.JoinType != LeftOuterJoin && p.JoinType != RightOuterJoin) || len(p.LeftConditions) != 0 || len(p.RightConditions) != 0 || len(p.OtherConditions) != 0 || len(p.EqualConditions) == 0 {
		return nil
	}

	if hasPrefer, idx := p.getPreferredBCJLocalIndex(); hasPrefer {
		if (idx == 0 && p.JoinType == RightOuterJoin) || (idx == 1 && p.JoinType == LeftOuterJoin) {
			return nil
		}
		return p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 1-idx)
	}
	if p.JoinType == InnerJoin {
		results := p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 0)
		results = append(results, p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 1)...)
		return results
	} else if p.JoinType == LeftOuterJoin {
		return p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 1)
	}
	return p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 0)
}

func (p *LogicalJoin) tryToGetBroadCastJoinByPreferGlobalIdx(prop *property.PhysicalProperty, preferredGlobalIndex int) []PhysicalPlan {
	lkeys, rkeys := p.GetJoinKeys()
	baseJoin := basePhysicalJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftJoinKeys:    lkeys,
		RightJoinKeys:   rkeys,
	}

	preferredBuildIndex := 0
	if p.children[0].statsInfo().Count() > p.children[1].statsInfo().Count() {
		preferredBuildIndex = 1
	}
	baseJoin.InnerChildIdx = preferredBuildIndex
	childrenReqProps := make([]*property.PhysicalProperty, 2)
	childrenReqProps[preferredGlobalIndex] = &property.PhysicalProperty{TaskTp: property.CopTiFlashGlobalReadTaskType, ExpectedCnt: math.MaxFloat64}
	if prop.TaskTp == property.CopTiFlashGlobalReadTaskType {
		childrenReqProps[1-preferredGlobalIndex] = &property.PhysicalProperty{TaskTp: property.CopTiFlashGlobalReadTaskType, ExpectedCnt: math.MaxFloat64}
	} else {
		childrenReqProps[1-preferredGlobalIndex] = &property.PhysicalProperty{TaskTp: property.CopTiFlashLocalReadTaskType, ExpectedCnt: math.MaxFloat64}
	}
	if prop.ExpectedCnt < p.stats.RowCount {
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		childrenReqProps[1-baseJoin.InnerChildIdx].ExpectedCnt = p.children[1-baseJoin.InnerChildIdx].statsInfo().RowCount * expCntScale
	}

	join := PhysicalBroadCastJoin{
		basePhysicalJoin: baseJoin,
		globalChildIndex: preferredGlobalIndex,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, childrenReqProps...)
	return []PhysicalPlan{join}
}

// TryToGetChildProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *LogicalProjection) TryToGetChildProp(prop *property.PhysicalProperty) (*property.PhysicalProperty, bool) {
	if prop.IsFlashOnlyProp() {
		return nil, false
	}
	newProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: prop.ExpectedCnt}
	newCols := make([]property.Item, 0, len(prop.Items))
	for _, col := range prop.Items {
		idx := p.schema.ColumnIndex(col.Col)
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, property.Item{Col: expr, Desc: col.Desc})
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.Items = newCols
	return newProp, true
}

func (p *LogicalProjection) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	newProp, ok := p.TryToGetChildProp(prop)
	if !ok {
		return nil, true
	}
	proj := PhysicalProjection{
		Exprs:                p.Exprs,
		CalculateNoDelay:     p.CalculateNoDelay,
		AvoidColumnEvaluator: p.AvoidColumnEvaluator,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, newProp)
	proj.SetSchema(p.schema)
	return []PhysicalPlan{proj}, true
}

func (lt *LogicalTopN) getPhysTopN(prop *property.PhysicalProperty) []PhysicalPlan {
	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	ret := make([]PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: math.MaxFloat64}
		topN := PhysicalTopN{
			ByItems: lt.ByItems,
			Count:   lt.Count,
			Offset:  lt.Offset,
		}.Init(lt.ctx, lt.stats, lt.blockOffset, resultProp)
		ret = append(ret, topN)
	}
	return ret
}

func (lt *LogicalTopN) getPhysLimits(prop *property.PhysicalProperty) []PhysicalPlan {
	p, canPass := GetPropByOrderByItems(lt.ByItems)
	if !canPass {
		return nil
	}
	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	ret := make([]PhysicalPlan, 0, 3)
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(lt.Count + lt.Offset), Items: p.Items}
		limit := PhysicalLimit{
			Count:  lt.Count,
			Offset: lt.Offset,
		}.Init(lt.ctx, lt.stats, lt.blockOffset, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

// MatchItems checks if this prop's columns can match by items totally.
func MatchItems(p *property.PhysicalProperty, items []*util.ByItems) bool {
	if len(items) < len(p.Items) {
		return false
	}
	for i, col := range p.Items {
		sortItem := items[i]
		if sortItem.Desc != col.Desc || !sortItem.Expr.Equal(nil, col.Col) {
			return false
		}
	}
	return true
}

func (lt *LogicalTopN) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if MatchItems(prop, lt.ByItems) {
		return append(lt.getPhysTopN(prop), lt.getPhysLimits(prop)...), true
	}
	return nil, true
}

// GetHashJoin is public for cascades planner.
func (la *LogicalApply) GetHashJoin(prop *property.PhysicalProperty) *PhysicalHashJoin {
	return la.LogicalJoin.getHashJoin(prop, 1, false)
}

func (la *LogicalApply) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if !prop.AllColsFromSchema(la.children[0].Schema()) || prop.IsFlashOnlyProp() { // for convenient, we don't pass through any prop
		return nil, true
	}
	join := la.GetHashJoin(prop)
	apply := PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.CorCols,
	}.Init(la.ctx,
		la.stats.ScaleByExpectCnt(prop.ExpectedCnt),
		la.blockOffset,
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: prop.Items},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	apply.SetSchema(la.schema)
	return []PhysicalPlan{apply}, true
}

func (p *LogicalWindow) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if prop.IsFlashOnlyProp() {
		return nil, true
	}
	var byItems []property.Item
	byItems = append(byItems, p.PartitionBy...)
	byItems = append(byItems, p.OrderBy...)
	childProperty := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: byItems, Enforced: true}
	if !prop.IsPrefix(childProperty) {
		return nil, true
	}
	window := PhysicalWindow{
		WindowFuncDescs: p.WindowFuncDescs,
		PartitionBy:     p.PartitionBy,
		OrderBy:         p.OrderBy,
		Frame:           p.Frame,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, childProperty)
	window.SetSchema(p.Schema())
	return []PhysicalPlan{window}, true
}

// exhaustPhysicalPlans is only for implementing interface. DataSource and Dual generate task in `findBestTask` directly.
func (p *baseLogicalPlan) exhaustPhysicalPlans(_ *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	panic("baseLogicalPlan.exhaustPhysicalPlans() should never be called.")
}

func (la *LogicalAggregation) canPushToCop() bool {
	// At present, only Aggregation, Limit, TopN can be pushed to cop task, and Projection will be supported in the future.
	// When we push task to coprocessor, finishCopTask will close the cop task and create a root task in the current implementation.
	// Thus, we can't push two different tasks to coprocessor now, and can only push task to coprocessor when the child is Datasource.

	// TODO: develop this function after supporting push several tasks to coprecessor and supporting Projection to coprocessor.
	_, ok := la.children[0].(*DataSource)
	return ok
}

func (la *LogicalAggregation) getEnforcedStreamAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	if prop.IsFlashOnlyProp() {
		return nil
	}
	_, desc := prop.AllSameOrder()
	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	enforcedAggs := make([]PhysicalPlan, 0, len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.inputCount/la.stats.RowCount, prop.ExpectedCnt),
		Enforced:    true,
		Items:       property.ItemsFromCols(la.groupByCols, desc),
	}
	if !prop.IsPrefix(childProp) {
		return enforcedAggs
	}
	taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopDoubleReadTaskType}
	if la.HasDistinct() {
		// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
		// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
		if !la.canPushToCop() || !la.ctx.GetSessionVars().AllowDistinctAggPushDown {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
	} else if !la.aggHints.preferAggToCop {
		taskTypes = append(taskTypes, property.RootTaskType)
	}
	for _, taskTp := range taskTypes {
		copiedChildProperty := new(property.PhysicalProperty)
		*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
		copiedChildProperty.TaskTp = taskTp

		agg := basePhysicalAgg{
			GroupByItems: la.GroupByItems,
			AggFuncs:     la.AggFuncs,
		}.initForStream(la.ctx, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), la.blockOffset, copiedChildProperty)
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

func (la *LogicalAggregation) getStreamAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	// TODO: support CopTiFlash task type in stream agg
	if prop.IsFlashOnlyProp() {
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
	if len(la.groupByCols) != len(la.GroupByItems) {
		return nil
	}

	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	streamAggs := make([]PhysicalPlan, 0, len(la.possibleProperties)*(len(allTaskTypes)-1)+len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.inputCount/la.stats.RowCount, prop.ExpectedCnt),
	}

	for _, possibleChildProperty := range la.possibleProperties {
		childProp.Items = property.ItemsFromCols(possibleChildProperty[:len(la.groupByCols)], desc)
		if !prop.IsPrefix(childProp) {
			continue
		}
		// The table read of "CopDoubleReadTaskType" can't promises the sort
		// property that the stream aggregation required, no need to consider.
		taskTypes := []property.TaskType{property.CopSingleReadTaskType}
		if la.HasDistinct() {
			// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
			// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
			if !la.canPushToCop() || !la.ctx.GetSessionVars().AllowDistinctAggPushDown {
				taskTypes = []property.TaskType{property.RootTaskType}
			} else {
				if !la.distinctArgsMeetsProperty() {
					continue
				}
			}
		} else if !la.aggHints.preferAggToCop {
			taskTypes = append(taskTypes, property.RootTaskType)
		}
		for _, taskTp := range taskTypes {
			copiedChildProperty := new(property.PhysicalProperty)
			*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
			copiedChildProperty.TaskTp = taskTp

			agg := basePhysicalAgg{
				GroupByItems: la.GroupByItems,
				AggFuncs:     la.AggFuncs,
			}.initForStream(la.ctx, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), la.blockOffset, copiedChildProperty)
			agg.SetSchema(la.schema.Clone())
			streamAggs = append(streamAggs, agg)
		}
	}
	// If STREAM_AGG hint is existed, it should consider enforce stream aggregation,
	// because we can't trust possibleChildProperty completely.
	if (la.aggHints.preferAggType & preferStreamAgg) > 0 {
		streamAggs = append(streamAggs, la.getEnforcedStreamAggs(prop)...)
	}
	return streamAggs
}

func (la *LogicalAggregation) getHashAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.IsEmpty() {
		return nil
	}
	hashAggs := make([]PhysicalPlan, 0, len(prop.GetAllPossibleChildTaskTypes()))
	taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopDoubleReadTaskType}
	if la.ctx.GetSessionVars().AllowBCJ {
		taskTypes = append(taskTypes, property.CopTiFlashLocalReadTaskType)
	}
	if la.HasDistinct() {
		// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
		// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
		if !la.canPushToCop() || !la.ctx.GetSessionVars().AllowDistinctAggPushDown {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
	} else if !la.aggHints.preferAggToCop {
		taskTypes = append(taskTypes, property.RootTaskType)
	}
	if prop.IsFlashOnlyProp() {
		taskTypes = []property.TaskType{prop.TaskTp}
	}
	for _, taskTp := range taskTypes {
		agg := NewPhysicalHashAgg(la, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, TaskTp: taskTp})
		agg.SetSchema(la.schema.Clone())
		hashAggs = append(hashAggs, agg)
	}
	return hashAggs
}

// ResetHintIfConflicted resets the aggHints.preferAggType if they are conflicted,
// and returns the two preferAggType hints.
func (la *LogicalAggregation) ResetHintIfConflicted() (preferHash bool, preferStream bool) {
	preferHash = (la.aggHints.preferAggType & preferHashAgg) > 0
	preferStream = (la.aggHints.preferAggType & preferStreamAgg) > 0
	if preferHash && preferStream {
		errMsg := "Optimizer aggregation hints are conflicted"
		warning := ErrInternal.GenWithStack(errMsg)
		la.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
		la.aggHints.preferAggType = 0
		preferHash, preferStream = false, false
	}
	return
}

func (la *LogicalAggregation) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if la.aggHints.preferAggToCop {
		if !la.canPushToCop() {
			errMsg := "Optimizer Hint AGG_TO_COP is inapplicable"
			warning := ErrInternal.GenWithStack(errMsg)
			la.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
			la.aggHints.preferAggToCop = false
		}
	}

	preferHash, preferStream := la.ResetHintIfConflicted()

	hashAggs := la.getHashAggs(prop)
	if hashAggs != nil && preferHash {
		return hashAggs, true
	}

	streamAggs := la.getStreamAggs(prop)
	if streamAggs != nil && preferStream {
		return streamAggs, true
	}

	aggs := append(hashAggs, streamAggs...)

	if streamAggs == nil && preferStream && !prop.IsEmpty() {
		errMsg := "Optimizer Hint STREAM_AGG is inapplicable"
		warning := ErrInternal.GenWithStack(errMsg)
		la.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
	}

	return aggs, !(preferStream || preferHash)
}

func (p *LogicalSelection) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	childProp := prop.Clone()
	sel := PhysicalSelection{
		Conditions: p.Conditions,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, childProp)
	return []PhysicalPlan{sel}, true
}

func (p *LogicalLimit) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if !prop.IsEmpty() {
		return nil, true
	}
	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	ret := make([]PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(p.Count + p.Offset)}
		limit := PhysicalLimit{
			Offset: p.Offset,
			Count:  p.Count,
		}.Init(p.ctx, p.stats, p.blockOffset, resultProp)
		ret = append(ret, limit)
	}
	return ret, true
}

func (p *LogicalLock) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if prop.IsFlashOnlyProp() {
		return nil, true
	}
	childProp := prop.Clone()
	lock := PhysicalLock{
		Lock:             p.Lock,
		TblID2Handle:     p.tblID2Handle,
		PartitionedTable: p.partitionedTable,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProp)
	return []PhysicalPlan{lock}, true
}

func (p *LogicalUnionAll) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	// TODO: UnionAll can not pass any order, but we can change it to sort merge to keep order.
	if !prop.IsEmpty() || prop.IsFlashOnlyProp() {
		return nil, true
	}
	chReqProps := make([]*property.PhysicalProperty, 0, len(p.children))
	for range p.children {
		chReqProps = append(chReqProps, &property.PhysicalProperty{ExpectedCnt: prop.ExpectedCnt})
	}
	ua := PhysicalUnionAll{}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, chReqProps...)
	ua.SetSchema(p.Schema())
	return []PhysicalPlan{ua}, true
}

func (p *LogicalPartitionUnionAll) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	uas, flagHint := p.LogicalUnionAll.exhaustPhysicalPlans(prop)
	for _, ua := range uas {
		ua.(*PhysicalUnionAll).tp = plancodec.TypePartitionUnion
	}
	return uas, flagHint
}

func (ls *LogicalSort) getPhysicalSort(prop *property.PhysicalProperty) *PhysicalSort {
	ps := PhysicalSort{ByItems: ls.ByItems}.Init(ls.ctx, ls.stats.ScaleByExpectCnt(prop.ExpectedCnt), ls.blockOffset, &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	return ps
}

func (ls *LogicalSort) getNominalSort(reqProp *property.PhysicalProperty) *NominalSort {
	prop, canPass, onlyColumn := GetPropByOrderByItemsContainScalarFunc(ls.ByItems)
	if !canPass {
		return nil
	}
	prop.ExpectedCnt = reqProp.ExpectedCnt
	ps := NominalSort{OnlyColumn: onlyColumn, ByItems: ls.ByItems}.Init(
		ls.ctx, ls.stats.ScaleByExpectCnt(prop.ExpectedCnt), ls.blockOffset, prop)
	return ps
}

func (ls *LogicalSort) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if MatchItems(prop, ls.ByItems) {
		ret := make([]PhysicalPlan, 0, 2)
		ret = append(ret, ls.getPhysicalSort(prop))
		ns := ls.getNominalSort(prop)
		if ns != nil {
			ret = append(ret, ns)
		}
		return ret, true
	}
	return nil, true
}

func (p *LogicalMaxOneRow) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if !prop.IsEmpty() || prop.IsFlashOnlyProp() {
		return nil, true
	}
	mor := PhysicalMaxOneRow{}.Init(p.ctx, p.stats, p.blockOffset, &property.PhysicalProperty{ExpectedCnt: 2})
	return []PhysicalPlan{mor}, true
}
