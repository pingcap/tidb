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
	"fmt"
	"math"

	"github.com/pingcap/tidb/expression"
	log "github.com/sirupsen/logrus"
)

// statsProfile stores the basic information of statistics for the a plan's output. It is used for cost estimation.
type statsProfile struct {
	count       float64
	cardinality []float64
}

func (s *statsProfile) String() string {
	return fmt.Sprintf("count %v, cardinality %v", s.count, s.cardinality)
}

// collapse receives a selectivity and multiple it with count and cardinality.
func (s *statsProfile) collapse(factor float64) *statsProfile {
	profile := &statsProfile{
		count:       s.count * factor,
		cardinality: make([]float64, len(s.cardinality)),
	}
	for i := range profile.cardinality {
		profile.cardinality[i] = s.cardinality[i] * factor
	}
	return profile
}

func (p *basePhysicalPlan) statsProfile() *statsProfile {
	profile := p.basePlan.profile
	expectedCnt := p.basePlan.expectedCnt
	if expectedCnt > 0 && expectedCnt < profile.count {
		factor := expectedCnt / profile.count
		result := &statsProfile{count: expectedCnt}
		for _, card := range profile.cardinality {
			result.cardinality = append(result.cardinality, card*factor)
		}
		return result
	}
	return profile
}

func (p *baseLogicalPlan) prepareStatsProfile() *statsProfile {
	if len(p.basePlan.children) == 0 {
		profile := &statsProfile{
			count:       float64(1),
			cardinality: make([]float64, p.basePlan.schema.Len()),
		}
		for i := range profile.cardinality {
			profile.cardinality[i] = float64(1)
		}
		p.basePlan.profile = profile
		return profile
	}
	p.basePlan.profile = p.basePlan.children[0].(LogicalPlan).prepareStatsProfile()
	return p.basePlan.profile
}

func (p *DataSource) getStatsProfileByFilter(conds expression.CNFExprs) *statsProfile {
	profile := &statsProfile{
		count:       float64(p.statisticTable.Count),
		cardinality: make([]float64, len(p.Columns)),
	}
	for i, col := range p.Columns {
		hist, ok := p.statisticTable.Columns[col.ID]
		if ok && hist.Count > 0 {
			factor := float64(p.statisticTable.Count) / float64(hist.Count)
			profile.cardinality[i] = float64(hist.NDV) * factor
		} else {
			profile.cardinality[i] = profile.count * distinctFactor
		}
	}
	selectivity, err := p.statisticTable.Selectivity(p.ctx, conds)
	if err != nil {
		log.Warnf("An error happened: %v, we have to use the default selectivity", err.Error())
		selectivity = selectionFactor
	}
	return profile.collapse(selectivity)
}

func (p *DataSource) prepareStatsProfile() *statsProfile {
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range p.pushedDownConds {
		p.pushedDownConds[i] = expression.PushDownNot(expr, false, nil)
	}
	p.profile = p.getStatsProfileByFilter(p.pushedDownConds)
	return p.profile
}

func (p *LogicalSelection) prepareStatsProfile() *statsProfile {
	childProfile := p.children[0].(LogicalPlan).prepareStatsProfile()
	p.profile = childProfile.collapse(selectionFactor)
	return p.profile
}

func (p *LogicalUnionAll) prepareStatsProfile() *statsProfile {
	p.profile = &statsProfile{
		cardinality: make([]float64, p.schema.Len()),
	}
	for _, child := range p.children {
		childProfile := child.(LogicalPlan).prepareStatsProfile()
		p.profile.count += childProfile.count
		for i := range p.profile.cardinality {
			p.profile.cardinality[i] += childProfile.cardinality[i]
		}
	}
	return p.profile
}

func (p *LogicalLimit) prepareStatsProfile() *statsProfile {
	childProfile := p.children[0].(LogicalPlan).prepareStatsProfile()
	p.profile = &statsProfile{
		count:       float64(p.Count),
		cardinality: make([]float64, len(childProfile.cardinality)),
	}
	if p.profile.count > childProfile.count {
		p.profile.count = childProfile.count
	}
	for i := range p.profile.cardinality {
		p.profile.cardinality[i] = childProfile.cardinality[i]
		if p.profile.cardinality[i] > p.profile.count {
			p.profile.cardinality[i] = p.profile.count
		}
	}
	return p.profile
}

func (p *LogicalTopN) prepareStatsProfile() *statsProfile {
	childProfile := p.children[0].(LogicalPlan).prepareStatsProfile()
	p.profile = &statsProfile{
		count:       float64(p.Count),
		cardinality: make([]float64, len(childProfile.cardinality)),
	}
	if p.profile.count > childProfile.count {
		p.profile.count = childProfile.count
	}
	for i := range p.profile.cardinality {
		p.profile.cardinality[i] = childProfile.cardinality[i]
		if p.profile.cardinality[i] > p.profile.count {
			p.profile.cardinality[i] = p.profile.count
		}
	}
	return p.profile
}

// getCardinality will return the cardinality of a couple of columns. We simply return the max one, because we cannot know
// the cardinality for multi-dimension attributes properly. This is a simple and naive scheme of cardinality estimation.
func getCardinality(cols []*expression.Column, schema *expression.Schema, profile *statsProfile) float64 {
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		log.Errorf("Cannot find column %s indices from schema %s", cols, schema)
		return 0
	}
	var cardinality = 1.0
	for _, idx := range indices {
		if cardinality < profile.cardinality[idx] {
			// It is a very elementary estimation.
			cardinality = profile.cardinality[idx]
		}
	}
	return cardinality
}

func (p *LogicalProjection) prepareStatsProfile() *statsProfile {
	childProfile := p.children[0].(LogicalPlan).prepareStatsProfile()
	p.profile = &statsProfile{
		count:       childProfile.count,
		cardinality: make([]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.profile.cardinality[i] = getCardinality(cols, p.children[0].Schema(), childProfile)
	}
	return p.profile
}

func (p *LogicalAggregation) prepareStatsProfile() *statsProfile {
	childProfile := p.children[0].(LogicalPlan).prepareStatsProfile()
	gbyCols := make([]*expression.Column, 0, len(p.GroupByItems))
	for _, gbyExpr := range p.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	cardinality := getCardinality(gbyCols, p.children[0].Schema(), childProfile)
	p.profile = &statsProfile{
		count:       cardinality,
		cardinality: make([]float64, p.schema.Len()),
	}
	// We cannot estimate the cardinality for every output, so we use a conservative strategy.
	for i := range p.profile.cardinality {
		p.profile.cardinality[i] = cardinality
	}
	p.inputCount = childProfile.count
	return p.profile
}

// prepareStatsProfile prepares stats profile.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) prepareStatsProfile() *statsProfile {
	leftProfile := p.children[0].(LogicalPlan).prepareStatsProfile()
	rightProfile := p.children[1].(LogicalPlan).prepareStatsProfile()
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.profile = &statsProfile{
			count:       leftProfile.count * selectionFactor,
			cardinality: make([]float64, len(leftProfile.cardinality)),
		}
		for i := range p.profile.cardinality {
			p.profile.cardinality[i] = leftProfile.cardinality[i] * selectionFactor
		}
		return p.profile
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.profile = &statsProfile{
			count:       leftProfile.count,
			cardinality: make([]float64, p.schema.Len()),
		}
		copy(p.profile.cardinality, leftProfile.cardinality)
		p.profile.cardinality[len(p.profile.cardinality)-1] = 2.0
		return p.profile
	}
	if 0 == len(p.EqualConditions) {
		p.profile = &statsProfile{
			count:       leftProfile.count * rightProfile.count,
			cardinality: append(leftProfile.cardinality, rightProfile.cardinality...),
		}
		return p.profile
	}
	leftKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	rightKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		leftKeys = append(leftKeys, eqCond.GetArgs()[0].(*expression.Column))
		rightKeys = append(rightKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	leftKeyCardinality := getCardinality(leftKeys, p.children[0].Schema(), leftProfile)
	rightKeyCardinality := getCardinality(rightKeys, p.children[1].Schema(), rightProfile)
	count := leftProfile.count * rightProfile.count / math.Max(leftKeyCardinality, rightKeyCardinality)
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.count)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.count)
	}
	cardinality := make([]float64, 0, p.schema.Len())
	cardinality = append(cardinality, leftProfile.cardinality...)
	cardinality = append(cardinality, rightProfile.cardinality...)
	for i := range cardinality {
		cardinality[i] = math.Min(cardinality[i], count)
	}
	p.profile = &statsProfile{
		count:       count,
		cardinality: cardinality,
	}
	return p.profile
}

func (p *LogicalApply) prepareStatsProfile() *statsProfile {
	leftProfile := p.children[0].(LogicalPlan).prepareStatsProfile()
	_ = p.children[1].(LogicalPlan).prepareStatsProfile()
	p.profile = &statsProfile{
		count:       leftProfile.count,
		cardinality: make([]float64, p.schema.Len()),
	}
	copy(p.profile.cardinality, leftProfile.cardinality)
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.profile.cardinality[len(p.profile.cardinality)-1] = 2.0
	} else {
		for i := p.children[0].Schema().Len(); i < p.schema.Len(); i++ {
			p.profile.cardinality[i] = leftProfile.count
		}
	}
	return p.profile
}

// TODO: Implement Exists, MaxOneRow plan.
