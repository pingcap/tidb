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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
)

func (p *LogicalUnionScan) generatePhysicalPlans() []PhysicalPlan {
	us := PhysicalUnionScan{Conditions: p.conditions}.init(p.ctx)
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

func (p *LogicalJoin) getMergeJoin() []PhysicalPlan {
	joins := make([]PhysicalPlan, 0, len(p.leftProperties))
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
		}.init(p.ctx)
		mergeJoin.SetSchema(p.schema)
		mergeJoin.profile = p.profile
		mergeJoin.EqualConditions, mergeJoin.OtherConditions = p.getEqAndOtherCondsByOffsets(offsets)
		joins = append(joins, mergeJoin)
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
	semiJoin.profile = p.profile
	return semiJoin
}

func (p *LogicalJoin) getHashJoins() []PhysicalPlan {
	joins := make([]PhysicalPlan, 0, 2)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		joins = append(joins, p.getHashJoin(1))
	case RightOuterJoin:
		joins = append(joins, p.getHashJoin(0))
	case InnerJoin:
		joins = append(joins, p.getHashJoin(1))
		joins = append(joins, p.getHashJoin(0))
	}
	return joins
}

func (p *LogicalJoin) getHashJoin(smallTable int) PhysicalPlan {
	hashJoin := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        p.JoinType,
		Concurrency:     JoinConcurrency,
		DefaultValues:   p.DefaultValues,
		SmallChildIdx:   smallTable,
	}.init(p.ctx)
	hashJoin.SetSchema(p.schema)
	hashJoin.profile = p.profile
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

func (p *LogicalJoin) constructIndexJoin(innerJoinKeys, outerJoinKeys []*expression.Column, outerIdx int, innerPlan PhysicalPlan) []PhysicalPlan {
	joinType := p.JoinType
	join := PhysicalIndexJoin{
		OuterIndex:      outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		JoinType:        joinType,
		OuterJoinKeys:   outerJoinKeys,
		InnerJoinKeys:   innerJoinKeys,
		DefaultValues:   p.DefaultValues,
		outerSchema:     p.children[outerIdx].Schema(),
		innerPlan:       innerPlan,
	}.init(p.ctx, p.children...)
	join.SetSchema(p.schema)
	join.profile = p.profile
	orderJoin := join.Copy().(*PhysicalIndexJoin)
	orderJoin.KeepOrder = true
	return []PhysicalPlan{join, orderJoin}
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child,
// because we will swap the children of join when the right child is outer child.
// First of all, we will extract the join keys for p's equal conditions. If the join keys can match some of the indices or PK
// column of inner child, we can apply the index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(outerIdx int) []PhysicalPlan {
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
			return p.constructIndexJoin(innerJoinKeys, outerJoinKeys, outerIdx, innerPlan)
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
		return p.constructIndexJoin(innerJoinKeys, outerJoinKeys, outerIdx, innerPlan)
	}
	return nil
}

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin() ([]PhysicalPlan, bool) {
	if len(p.EqualConditions) == 0 {
		return nil, false
	}
	plans := make([]PhysicalPlan, 0, 2)
	leftOuter := (p.preferJoinType & preferLeftAsIndexOuter) > 0
	rightOuter := (p.preferJoinType & preferRightAsIndexOuter) > 0
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		join := p.getIndexJoinByOuterIdx(0)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if leftOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
	case RightOuterJoin:
		join := p.getIndexJoinByOuterIdx(1)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if rightOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
	case InnerJoin:
		join := p.getIndexJoinByOuterIdx(0)
		if join != nil {
			// If the plan is not nil and matches the hint, return it directly.
			if leftOuter {
				return join, true
			}
			plans = append(plans, join...)
		}
		join = p.getIndexJoinByOuterIdx(1)
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

func (p *LogicalJoin) generatePhysicalPlans() []PhysicalPlan {
	mergeJoins := p.getMergeJoin()
	if (p.preferJoinType&preferMergeJoin) > 0 && len(mergeJoins) > 0 {
		return mergeJoins
	}
	joins := make([]PhysicalPlan, 0, 5)
	joins = append(joins, mergeJoins...)

	indexJoins, forced := p.tryToGetIndexJoin()
	if forced {
		return indexJoins
	}
	joins = append(joins, indexJoins...)

	hashJoins := p.getHashJoins()
	if (p.preferJoinType & preferHashJoin) > 0 {
		return hashJoins
	}
	joins = append(joins, hashJoins...)
	return joins
}

func (p *LogicalProjection) generatePhysicalPlans() []PhysicalPlan {
	proj := PhysicalProjection{
		Exprs: p.Exprs,
	}.init(p.ctx)
	proj.profile = p.profile
	proj.SetSchema(p.schema)
	return []PhysicalPlan{proj}
}

func (p *LogicalTopN) generatePhysicalPlans() []PhysicalPlan {
	topN := PhysicalTopN{
		ByItems: p.ByItems,
		Count:   p.Count,
		Offset:  p.Offset,
		partial: p.partial,
	}.init(p.ctx)
	topN.profile = p.profile
	topN.SetSchema(p.schema)
	plans := []PhysicalPlan{topN}
	if prop, canPass := getPropByOrderByItems(p.ByItems); canPass {
		limit := PhysicalLimit{
			Count:        p.Count,
			Offset:       p.Offset,
			partial:      p.partial,
			expectedProp: prop,
		}.init(p.ctx)
		limit.SetSchema(p.schema)
		limit.profile = p.profile
		plans = append(plans, limit)
	}
	return plans
}
func (p *LogicalApply) generatePhysicalPlans() []PhysicalPlan {
	var join PhysicalPlan
	if p.JoinType == SemiJoin || p.JoinType == LeftOuterSemiJoin ||
		p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		join = p.getHashSemiJoin()
	} else {
		join = p.getHashJoin(1)
	}
	apply := PhysicalApply{
		PhysicalJoin:  join,
		OuterSchema:   p.corCols,
		rightChOffset: p.children[0].Schema().Len(),
	}.init(p.ctx)
	apply.SetSchema(p.schema)
	apply.profile = p.profile
	return []PhysicalPlan{apply}
}

func (p *baseLogicalPlan) generatePhysicalPlans() []PhysicalPlan {
	np := p.basePlan.self.(PhysicalPlan).Copy()
	return []PhysicalPlan{np}
}

func (p *LogicalAggregation) getStreamAggs() []PhysicalPlan {
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
		agg := basePhysicalAgg{
			GroupByItems: p.GroupByItems,
			AggFuncs:     p.AggFuncs,
		}.initForStream(p.ctx, keys, p.inputCount)
		agg.SetSchema(p.schema.Clone())
		agg.profile = p.profile
		streamAggs = append(streamAggs, agg)
	}
	return streamAggs
}

func (p *LogicalAggregation) generatePhysicalPlans() []PhysicalPlan {
	aggs := make([]PhysicalPlan, 0, len(p.possibleProperties)+1)
	agg := basePhysicalAgg{
		GroupByItems: p.GroupByItems,
		AggFuncs:     p.AggFuncs,
	}.initForHash(p.ctx)
	agg.SetSchema(p.schema.Clone())
	agg.profile = p.profile
	aggs = append(aggs, agg)

	streamAggs := p.getStreamAggs()
	aggs = append(aggs, streamAggs...)

	return aggs
}

func (p *LogicalSelection) generatePhysicalPlans() []PhysicalPlan {
	sel := PhysicalSelection{
		Conditions: p.Conditions,
	}.init(p.ctx)
	sel.profile = p.profile
	sel.SetSchema(p.Schema())
	return []PhysicalPlan{sel}
}

func (p *LogicalLimit) generatePhysicalPlans() []PhysicalPlan {
	limit := PhysicalLimit{
		Offset:  p.Offset,
		Count:   p.Count,
		partial: p.partial,
	}.init(p.ctx)
	limit.profile = p.profile
	limit.SetSchema(p.Schema())
	return []PhysicalPlan{limit}
}

func (p *LogicalLock) generatePhysicalPlans() []PhysicalPlan {
	lock := PhysicalLock{
		Lock: p.Lock,
	}.init(p.ctx)
	lock.profile = p.profile
	lock.SetSchema(p.schema)
	return []PhysicalPlan{lock}
}

func (p *LogicalUnionAll) generatePhysicalPlans() []PhysicalPlan {
	ua := PhysicalUnionAll{childNum: len(p.children)}.init(p.ctx)
	ua.profile = p.profile
	ua.SetSchema(p.schema)
	return []PhysicalPlan{ua}
}

func (p *LogicalSort) getPhysicalSort() *PhysicalSort {
	ps := PhysicalSort{ByItems: p.ByItems}.init(p.ctx)
	ps.profile = p.profile
	ps.SetSchema(p.schema)
	return ps
}

func (p *LogicalSort) getNominalSort() *NominalSort {
	prop, canPass := getPropByOrderByItems(p.ByItems)
	if !canPass {
		return nil
	}
	ps := &NominalSort{prop: prop}
	return ps
}

func (p *LogicalSort) generatePhysicalPlans() []PhysicalPlan {
	ret := make([]PhysicalPlan, 0, 2)
	ret = append(ret, p.getPhysicalSort())
	ps := p.getNominalSort()
	if ps != nil {
		ret = append(ret, ps)
	}
	return ret
}

func (p *LogicalExists) generatePhysicalPlans() []PhysicalPlan {
	exists := PhysicalExists{}.init(p.ctx)
	exists.profile = p.profile
	exists.SetSchema(p.schema)
	return []PhysicalPlan{exists}
}

func (p *LogicalMaxOneRow) generatePhysicalPlans() []PhysicalPlan {
	mor := PhysicalMaxOneRow{}.init(p.ctx)
	mor.profile = p.profile
	mor.SetSchema(p.schema)
	return []PhysicalPlan{mor}
}

func (p *LogicalTableDual) generatePhysicalPlans() []PhysicalPlan {
	dual := PhysicalTableDual{RowCount: p.RowCount}.init(p.ctx)
	dual.profile = p.profile
	dual.SetSchema(p.schema)
	return []PhysicalPlan{dual}
}
