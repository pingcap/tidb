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

package plan

import (
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
)

func (p *LogicalUnionScan) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	us := PhysicalUnionScan{Conditions: p.conditions}.init(p.ctx, prop)
	us.SetSchema(p.schema)
	return []PhysicalPlan{us}
}

func getPermutation(cols1, cols2 []*expression.Column) ([]int, []*expression.Column) {
	tmpSchema := expression.NewSchema(cols2...)
	permutation := make([]int, 0, len(cols1))
	for i, col1 := range cols1 {
		offset := tmpSchema.ColumnIndex(col1)
		if offset == -1 {
			return permutation, cols1[:i]
		}
		permutation = append(permutation, offset)
	}
	return permutation, cols1
}

func findMaxPrefixLen(candidates [][]*expression.Column, keys []*expression.Column) int {
	maxLen := 0
	for _, candidateKeys := range candidates {
		matchedLen := 0
		for i := range keys {
			if i < len(candidateKeys) && keys[i].Equal(candidateKeys[i], nil) {
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

func (p *LogicalJoin) getEqAndOtherCondsByOffsets(offsets []int) ([]*expression.ScalarFunction, []expression.Expression) {
	var (
		eqConds    = make([]*expression.ScalarFunction, 0, len(p.EqualConditions))
		otherConds = make([]expression.Expression, len(p.OtherConditions))
	)
	copy(otherConds, p.OtherConditions)
	for i, eqCond := range p.EqualConditions {
		match := false
		for _, offset := range offsets {
			if i == offset {
				match = true
				break
			}
		}
		if !match {
			otherConds = append(otherConds, eqCond)
		} else {
			eqConds = append(eqConds, eqCond)
		}
	}
	return eqConds, otherConds
}

// Only if the input required prop is the prefix fo join keys, we can pass through this property.
func (p *PhysicalMergeJoin) tryToGetChildReqProp(prop *requiredProp) ([]*requiredProp, bool) {
	lProp := &requiredProp{taskTp: rootTaskType, cols: p.leftKeys, expectedCnt: math.MaxFloat64}
	rProp := &requiredProp{taskTp: rootTaskType, cols: p.rightKeys, expectedCnt: math.MaxFloat64}
	if !prop.isEmpty() {
		// sort merge join fits the cases of massive ordered data, so desc scan is always expensive.
		if prop.desc {
			return nil, false
		}
		if !prop.isPrefix(lProp) && !prop.isPrefix(rProp) {
			return nil, false
		}
		if prop.isPrefix(rProp) && p.JoinType == LeftOuterJoin {
			return nil, false
		}
		if prop.isPrefix(lProp) && p.JoinType == RightOuterJoin {
			return nil, false
		}
	}

	return []*requiredProp{lProp, rProp}, true
}

func (p *LogicalJoin) getMergeJoin(prop *requiredProp) []PhysicalPlan {
	joins := make([]PhysicalPlan, 0, len(p.leftProperties))
	// The leftProperties caches all the possible properties that are provided by its children.
	for _, leftCols := range p.leftProperties {
		offsets, leftKeys := getPermutation(leftCols, p.LeftJoinKeys)
		if len(offsets) == 0 {
			continue
		}
		rightKeys := expression.NewSchema(p.RightJoinKeys...).ColumnsByIndices(offsets)
		prefixLen := findMaxPrefixLen(p.rightProperties, rightKeys)
		if prefixLen == 0 {
			continue
		}
		leftKeys = leftKeys[:prefixLen]
		rightKeys = rightKeys[:prefixLen]
		offsets = offsets[:prefixLen]
		mergeJoin := PhysicalMergeJoin{
			JoinType:        p.JoinType,
			LeftConditions:  p.LeftConditions,
			RightConditions: p.RightConditions,
			DefaultValues:   p.DefaultValues,
			leftKeys:        leftKeys,
			rightKeys:       rightKeys,
		}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt))
		mergeJoin.SetSchema(p.schema)
		mergeJoin.EqualConditions, mergeJoin.OtherConditions = p.getEqAndOtherCondsByOffsets(offsets)
		if reqProps, ok := mergeJoin.tryToGetChildReqProp(prop); ok {
			mergeJoin.childrenReqProps = reqProps
			joins = append(joins, mergeJoin)
		}
	}
	return joins
}

func (p *LogicalJoin) getHashSemiJoin() PhysicalPlan {
	semiJoin := PhysicalHashSemiJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		rightChOffset:   p.children[0].Schema().Len(),
		WithAux:         p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin,
		Anti:            p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin,
	}.init(p.ctx)
	semiJoin.SetSchema(p.schema)
	return semiJoin
}

func (p *LogicalJoin) getHashJoins(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() { // hash join doesn't promise any orders
		return nil
	}
	joins := make([]PhysicalPlan, 0, 2)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		joins = append(joins, p.getHashJoin(prop, 1))
	case RightOuterJoin:
		joins = append(joins, p.getHashJoin(prop, 0))
	case InnerJoin:
		joins = append(joins, p.getHashJoin(prop, 1))
		joins = append(joins, p.getHashJoin(prop, 0))
	}
	return joins
}

func (p *LogicalJoin) getHashJoin(prop *requiredProp, innerIdx int) PhysicalPlan {
	chReqProps := make([]*requiredProp, 2)
	chReqProps[innerIdx] = &requiredProp{expectedCnt: math.MaxFloat64}
	chReqProps[1-innerIdx] = &requiredProp{expectedCnt: prop.expectedCnt}
	hashJoin := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        p.JoinType,
		Concurrency:     JoinConcurrency,
		DefaultValues:   p.DefaultValues,
		SmallChildIdx:   innerIdx,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), chReqProps...)
	hashJoin.SetSchema(p.schema)
	return hashJoin
}

// joinKeysMatchIndex checks if all keys match columns in index.
func joinKeysMatchIndex(keys []*expression.Column, index *model.IndexInfo) []int {
	if len(index.Columns) < len(keys) {
		return nil
	}
	matchOffsets := make([]int, len(keys))
	for i, idxCol := range index.Columns {
		if idxCol.Length != types.UnspecifiedLength {
			return nil
		}
		found := false
		for j, key := range keys {
			if idxCol.Name.L == key.ColName.L {
				matchOffsets[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		if i+1 == len(keys) {
			break
		}
	}
	return matchOffsets
}

func (p *LogicalJoin) constructIndexJoin(prop *requiredProp, innerJoinKeys, outerJoinKeys []*expression.Column, outerIdx int, innerPlan PhysicalPlan) []PhysicalPlan {
	joinType := p.JoinType
	outerSchema := p.children[outerIdx].Schema()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.allColsFromSchema(outerSchema) {
		return nil
	}
	chReqProps := make([]*requiredProp, 2)
	chReqProps[outerIdx] = &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt, cols: prop.cols, desc: prop.desc}
	join := PhysicalIndexJoin{
		OuterIndex:      outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        joinType,
		OuterJoinKeys:   outerJoinKeys,
		InnerJoinKeys:   innerJoinKeys,
		DefaultValues:   p.DefaultValues,
		innerPlan:       innerPlan,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), chReqProps...)
	join.SetSchema(p.schema)
	if !prop.isEmpty() {
		join.KeepOrder = true
	}
	return []PhysicalPlan{join}
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child,
// because we will swap the children of join when the right child is outer child.
// First of all, we will extract the join keys for p's equal conditions. If the join keys can match some of the indices or PK
// column of inner child, we can apply the index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(prop *requiredProp, outerIdx int) []PhysicalPlan {
	innerChild := p.children[1-outerIdx].(LogicalPlan)
	var (
		usedIndexInfo *model.IndexInfo
		innerJoinKeys []*expression.Column
		outerJoinKeys []*expression.Column
	)
	if outerIdx == 0 {
		outerJoinKeys = p.LeftJoinKeys
		innerJoinKeys = p.RightJoinKeys
	} else {
		innerJoinKeys = p.LeftJoinKeys
		outerJoinKeys = p.RightJoinKeys
	}
	x, ok := innerChild.(*DataSource)
	if !ok {
		return nil
	}
	indices := x.availableIndices.indices
	includeTableScan := x.availableIndices.includeTableScan
	if includeTableScan && len(innerJoinKeys) == 1 {
		pkCol := x.getPKIsHandleCol()
		if pkCol != nil && innerJoinKeys[0].Equal(pkCol, nil) {
			innerPlan := x.forceToTableScan()
			return p.constructIndexJoin(prop, innerJoinKeys, outerJoinKeys, outerIdx, innerPlan)
		}
	}
	for _, indexInfo := range indices {
		matchedOffsets := joinKeysMatchIndex(innerJoinKeys, indexInfo)
		if matchedOffsets == nil {
			continue
		}
		usedIndexInfo = indexInfo
		newOuterJoinKeys := make([]*expression.Column, len(outerJoinKeys))
		newInnerJoinKeys := make([]*expression.Column, len(innerJoinKeys))
		for i, offset := range matchedOffsets {
			newOuterJoinKeys[i] = outerJoinKeys[offset]
			newInnerJoinKeys[i] = innerJoinKeys[offset]
		}
		outerJoinKeys = newOuterJoinKeys
		innerJoinKeys = newInnerJoinKeys
		break
	}
	if usedIndexInfo != nil {
		innerPlan := x.forceToIndexScan(usedIndexInfo)
		return p.constructIndexJoin(prop, innerJoinKeys, outerJoinKeys, outerIdx, innerPlan)
	}
	return nil
}

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin(prop *requiredProp) ([]PhysicalPlan, bool) {
	if len(p.EqualConditions) == 0 {
		return nil, false
	}
	plans := make([]PhysicalPlan, 0, 2)
	leftOuter := (p.preferJoinType & preferLeftAsIndexOuter) > 0
	rightOuter := (p.preferJoinType & preferRightAsIndexOuter) > 0
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		join := p.getIndexJoinByOuterIdx(prop, 0)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if leftOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
	case RightOuterJoin:
		join := p.getIndexJoinByOuterIdx(prop, 1)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if rightOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
	case InnerJoin:
		join := p.getIndexJoinByOuterIdx(prop, 0)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if leftOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
		join = p.getIndexJoinByOuterIdx(prop, 1)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if rightOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
	}
	return plans, false
}

// LogicalJoin can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	mergeJoins := p.getMergeJoin(prop)
	if (p.preferJoinType&preferMergeJoin) > 0 && len(mergeJoins) > 0 {
		return mergeJoins
	}
	joins := make([]PhysicalPlan, 0, 5)
	joins = append(joins, mergeJoins...)

	indexJoins, forced := p.tryToGetIndexJoin(prop)
	if forced {
		return indexJoins
	}
	joins = append(joins, indexJoins...)

	hashJoins := p.getHashJoins(prop)
	if (p.preferJoinType & preferHashJoin) > 0 {
		return hashJoins
	}
	joins = append(joins, hashJoins...)
	return joins
}

// tryToGetChildProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *LogicalProjection) tryToGetChildProp(prop *requiredProp) (*requiredProp, bool) {
	newProp := &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt}
	newCols := make([]*expression.Column, 0, len(prop.cols))
	for _, col := range prop.cols {
		idx := p.schema.ColumnIndex(col)
		if idx == -1 {
			return nil, false
		}
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, expr)
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.cols = newCols
	newProp.desc = prop.desc
	return newProp, true
}

func (p *LogicalProjection) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	newProp, ok := p.tryToGetChildProp(prop)
	if !ok {
		return nil
	}
	proj := PhysicalProjection{
		Exprs: p.Exprs,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), newProp)
	proj.SetSchema(p.schema)
	return []PhysicalPlan{proj}
}

func (p *LogicalTopN) getPhysTopN() []PhysicalPlan {
	ret := make([]PhysicalPlan, 0, 3)
	for _, tp := range wholeTaskTypes {
		resultProp := &requiredProp{taskTp: tp, expectedCnt: math.MaxFloat64}
		topN := PhysicalTopN{
			ByItems: p.ByItems,
			Count:   p.Count,
			Offset:  p.Offset,
			partial: p.partial,
		}.init(p.ctx, p.stats, resultProp)
		topN.SetSchema(p.schema)
		ret = append(ret, topN)
	}
	return ret
}

func (p *LogicalTopN) getPhysLimits() []PhysicalPlan {
	prop, canPass := getPropByOrderByItems(p.ByItems)
	if !canPass {
		return nil
	}
	ret := make([]PhysicalPlan, 0, 3)
	for _, tp := range wholeTaskTypes {
		resultProp := &requiredProp{taskTp: tp, expectedCnt: float64(p.Count + p.Offset), cols: prop.cols, desc: prop.desc}
		limit := PhysicalLimit{
			Count:   p.Count,
			Offset:  p.Offset,
			partial: p.partial,
		}.init(p.ctx, p.stats, resultProp)
		limit.SetSchema(p.schema)
		ret = append(ret, limit)
	}
	return ret
}

func (p *LogicalTopN) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if prop.matchItems(p.ByItems) {
		return append(p.getPhysTopN(), p.getPhysLimits()...)
	}
	return nil
}

func (p *LogicalApply) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if !prop.allColsFromSchema(p.children[0].Schema()) { // for convenient, we don't pass through any prop
		return nil
	}
	var join PhysicalPlan
	if p.JoinType == SemiJoin || p.JoinType == LeftOuterSemiJoin ||
		p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		join = p.getHashSemiJoin()
	} else {
		join = p.getHashJoin(prop, 1)
	}
	apply := PhysicalApply{
		PhysicalJoin:  join,
		OuterSchema:   p.corCols,
		rightChOffset: p.children[0].Schema().Len(),
	}.init(p.ctx,
		p.stats.scaleByExpectCnt(prop.expectedCnt),
		&requiredProp{expectedCnt: math.MaxFloat64, cols: prop.cols, desc: prop.desc},
		&requiredProp{expectedCnt: math.MaxFloat64})
	apply.SetSchema(p.schema)
	return []PhysicalPlan{apply}
}

// genPhysPlansByReqProp is only for implementing interface. DataSource and Dual generate task in `convert2NewPhysicalPlan` directly.
func (p *baseLogicalPlan) genPhysPlansByReqProp(_ *requiredProp) []PhysicalPlan {
	panic("This function should not be called")
}

func (p *LogicalAggregation) getStreamAggs(prop *requiredProp) []PhysicalPlan {
	if len(p.possibleProperties) == 0 {
		return nil
	}
	for _, aggFunc := range p.AggFuncs {
		if aggFunc.GetMode() == aggregation.FinalMode {
			return nil
		}
	}
	// group by a + b is not interested in any order.
	if len(p.groupByCols) != len(p.GroupByItems) {
		return nil
	}
	streamAggs := make([]PhysicalPlan, 0, len(p.possibleProperties))
	for _, cols := range p.possibleProperties {
		_, keys := getPermutation(cols, p.groupByCols)
		if len(keys) != len(p.groupByCols) {
			continue
		}
		childProp := &requiredProp{
			cols:        keys,
			desc:        prop.desc,
			expectedCnt: prop.expectedCnt * p.inputCount / p.stats.count,
		}
		if !prop.isPrefix(childProp) {
			continue
		}
		agg := basePhysicalAgg{
			GroupByItems: p.GroupByItems,
			AggFuncs:     p.AggFuncs,
		}.initForStream(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), childProp)
		agg.SetSchema(p.schema.Clone())
		streamAggs = append(streamAggs, agg)
	}
	return streamAggs
}

func (p *LogicalAggregation) getHashAggs(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	hashAggs := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, taskTp := range wholeTaskTypes {
		agg := basePhysicalAgg{
			GroupByItems: p.GroupByItems,
			AggFuncs:     p.AggFuncs,
		}.initForHash(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), &requiredProp{expectedCnt: math.MaxFloat64, taskTp: taskTp})
		agg.SetSchema(p.schema.Clone())
		hashAggs = append(hashAggs, agg)
	}
	return hashAggs
}

func (p *LogicalAggregation) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	aggs := make([]PhysicalPlan, 0, len(p.possibleProperties)+1)
	aggs = append(aggs, p.getHashAggs(prop)...)

	streamAggs := p.getStreamAggs(prop)
	aggs = append(aggs, streamAggs...)

	return aggs
}

func (p *LogicalSelection) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	sel := PhysicalSelection{
		Conditions: p.Conditions,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), prop)
	sel.SetSchema(p.Schema())
	return []PhysicalPlan{sel}
}

func (p *LogicalLimit) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	ret := make([]PhysicalPlan, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		resultProp := &requiredProp{taskTp: tp, expectedCnt: float64(p.Count + p.Offset)}
		limit := PhysicalLimit{
			Offset:  p.Offset,
			Count:   p.Count,
			partial: p.partial,
		}.init(p.ctx, p.stats, resultProp)
		limit.SetSchema(p.Schema())
		ret = append(ret, limit)
	}
	return ret
}

func (p *LogicalLock) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	lock := PhysicalLock{
		Lock: p.Lock,
	}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), prop)
	lock.SetSchema(p.schema)
	return []PhysicalPlan{lock}
}

func (p *LogicalUnionAll) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	chReqProps := make([]*requiredProp, 0, len(p.children))
	for range p.children {
		chReqProps = append(chReqProps, prop)
	}
	ua := PhysicalUnionAll{}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), chReqProps...)
	ua.SetSchema(p.schema)
	return []PhysicalPlan{ua}
}

func (p *LogicalSort) getPhysicalSort(prop *requiredProp) *PhysicalSort {
	ps := PhysicalSort{ByItems: p.ByItems}.init(p.ctx, p.stats.scaleByExpectCnt(prop.expectedCnt), &requiredProp{expectedCnt: math.MaxFloat64})
	ps.SetSchema(p.schema)
	return ps
}

func (p *LogicalSort) getNominalSort(reqProp *requiredProp) *NominalSort {
	prop, canPass := getPropByOrderByItems(p.ByItems)
	if !canPass {
		return nil
	}
	prop.expectedCnt = reqProp.expectedCnt
	ps := NominalSort{}.init(p.ctx, prop)
	return ps
}

func (p *LogicalSort) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if prop.matchItems(p.ByItems) {
		ret := make([]PhysicalPlan, 0, 2)
		ret = append(ret, p.getPhysicalSort(prop))
		ns := p.getNominalSort(prop)
		if ns != nil {
			ret = append(ret, ns)
		}
		return ret
	}
	return nil
}

func (p *LogicalExists) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	exists := PhysicalExists{}.init(p.ctx, p.stats, &requiredProp{expectedCnt: 1})
	exists.SetSchema(p.schema)
	return []PhysicalPlan{exists}
}

func (p *LogicalMaxOneRow) genPhysPlansByReqProp(prop *requiredProp) []PhysicalPlan {
	if !prop.isEmpty() {
		return nil
	}
	mor := PhysicalMaxOneRow{}.init(p.ctx, p.stats, &requiredProp{expectedCnt: 2})
	mor.SetSchema(p.schema)
	return []PhysicalPlan{mor}
}
