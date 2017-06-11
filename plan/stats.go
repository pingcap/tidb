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

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/expression"
)

// statsProfile stores the basic information of statistics for the a plan's output. It is used for cost estimation.
type statsProfile struct {
	count       float64
	cardinality []float64
}

// Sort will call this.
func (p *baseLogicalPlan) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	p.profile = p.basePlan.children[0].(LogicalPlan).statsProfile()
	return p.profile
}

func (p *DataSource) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	p.profile = &statsProfile{
		count:       float64(p.statisticTable.Count),
		cardinality: make([]float64, len(p.statisticTable.Columns)),
	}
	for i, col := range p.Columns {
		p.profile.cardinality[i] = float64(p.statisticTable.Columns[col.ID].NDV)
	}
	return p.profile
}

func (p *Selection) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	childProfile := p.children[0].(LogicalPlan).statsProfile()
	// TODO: estimate it by histogram.
	p.profile = &statsProfile{
		count:       childProfile.count * selectionFactor,
		cardinality: make([]float64, len(childProfile.cardinality)),
	}
	for i := range p.profile.cardinality {
		p.profile.cardinality[i] = childProfile.cardinality[i] * selectionFactor
	}
	return p.profile
}

func (p *Union) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	p.profile = &statsProfile{
		cardinality: make([]float64, p.schema.Len()),
	}
	for _, child := range p.children {
		childProfile := child.(LogicalPlan).statsProfile()
		p.profile.count += childProfile.count
		for i := range p.profile.cardinality {
			p.profile.cardinality[i] += childProfile.cardinality[i]
		}
	}
	return p.profile
}

func (p *Limit) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	childProfile := p.children[0].(LogicalPlan).statsProfile()
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

func (p *TopN) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	childProfile := p.children[0].(LogicalPlan).statsProfile()
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
	var cardinality float64
	for _, idx := range indices {
		if cardinality < profile.cardinality[idx] {
			// It is a very elementary estimation.
			cardinality = profile.cardinality[idx]
		}
	}
	return cardinality
}

func (p *Projection) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	childProfile := p.children[0].(LogicalPlan).statsProfile()
	p.profile = &statsProfile{
		count:       childProfile.count,
		cardinality: make([]float64, p.schema.Len()),
	}
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.profile.cardinality[i] = getCardinality(cols, p.children[0].Schema(), childProfile)
	}
	return p.profile
}

func (p *LogicalAggregation) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	childProfile := p.children[0].(LogicalPlan).statsProfile()
	var gbyCols []*expression.Column
	for _, gbyExpr := range p.GroupByItems {
		cols := expression.ExtractColumns(gbyExpr)
		gbyCols = append(gbyCols, cols...)
	}
	count := getCardinality(gbyCols, p.children[0].Schema(), childProfile)
	p.profile = &statsProfile{
		count:       count,
		cardinality: make([]float64, p.schema.Len()),
	}
	// We cannot estimate the cardinality for every output, so we use a conservative strategy.
	for i := range p.profile.cardinality {
		p.profile.cardinality[i] = count
	}
	return p.profile
}

// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) statsProfile() *statsProfile {
	if p.profile != nil {
		return p.profile
	}
	leftProfile := p.children[0].(LogicalPlan).statsProfile()
	rightProfile := p.children[1].(LogicalPlan).statsProfile()
	if p.JoinType == SemiJoin {
		p.profile = &statsProfile{
			count:       leftProfile.count * selectionFactor,
			cardinality: make([]float64, len(leftProfile.cardinality)),
		}
		for i := range p.profile.cardinality {
			p.profile.cardinality[i] = leftProfile.cardinality[i] * selectionFactor
		}
		return p.profile
	}
	if p.JoinType == LeftOuterSemiJoin {
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
	var leftKeys, rightKeys []*expression.Column
	for _, eqCond := range p.EqualConditions {
		leftKeys = append(leftKeys, eqCond.GetArgs()[0].(*expression.Column))
		rightKeys = append(rightKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	leftKeyCardinality := getCardinality(leftKeys, p.children[0].Schema(), leftProfile)
	rightKeyCardinality := getCardinality(rightKeys, p.children[1].Schema(), rightProfile)
	count := (leftProfile.count * rightProfile.count / leftKeyCardinality / rightKeyCardinality) * math.Min(leftKeyCardinality, rightKeyCardinality)
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

// TODO: Implement Exists, MaxOneRow plan.
