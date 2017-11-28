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

import "github.com/pingcap/tidb/expression"

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

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin() ([]PhysicalPlan, bool) {
	if len(p.EqualConditions) == 0 {
		return nil, false
	}
	plans := make([]PhysicalPlan, 0, 2)
	leftOuter := (p.preferINLJ & preferLeftAsOuter) > 0
	rightOuter := (p.preferINLJ & preferRightAsOuter) > 0
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
	if p.preferMergeJoin && len(mergeJoins) > 0 {
		return mergeJoins
	}
	joins := make([]PhysicalPlan, 0, 5)
	joins = append(joins, mergeJoins...)

	indexJoins, forced := p.tryToGetIndexJoin()
	if forced {
		return indexJoins
	}
	joins = append(joins, indexJoins...)

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

func (p *TopN) generatePhysicalPlans() []PhysicalPlan {
	plans := []PhysicalPlan{p.Copy()}
	if prop, canPass := getPropByOrderByItems(p.ByItems); canPass {
		limit := Limit{
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

func (p *LogicalAggregation) generatePhysicalPlans() []PhysicalPlan {
	aggs := make([]PhysicalPlan, 0, len(p.possibleProperties)+1)
	agg := PhysicalAggregation{
		GroupByItems: p.GroupByItems,
		AggFuncs:     p.AggFuncs,
		HasGby:       len(p.GroupByItems) > 0,
		AggType:      CompleteAgg,
	}.init(p.ctx)
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
