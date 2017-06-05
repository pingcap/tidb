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

// TODO: Implement Join, Exists, MaxOneRow plan.
