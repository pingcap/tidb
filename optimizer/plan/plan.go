// Copyright 2015 PingCAP, Inc.
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

	"github.com/pingcap/tidb/ast"
)

// Plan is a description of an execution flow.
// It is created from ast.Node first, then optimized by optimizer,
// then used by executor to create a Cursor which executes the statement.
type Plan interface {
	// Fields returns the result fields of the plan.
	Fields() []*ast.ResultField
	// SetFields sets the results fields of the plan.
	SetFields(fields []*ast.ResultField)
	// The cost before returning fhe first row.
	StartupCost() float64
	// The cost after returning all the rows.
	TotalCost() float64
	// The expected row count.
	RowCount() float64
	// SetLimit is used to push limit to upstream to estimate the cost.
	SetLimit(limit float64)

	AddParent(parent Plan)

	AddChild(children Plan)

	GetParentByIndex(index int) Plan

	GetChildByIndex(index int) Plan

	GetParents() []Plan

	GetChildren() []Plan
}

// basePlan implements base Plan interface.
// Should be used as embedded struct in Plan implementations.
type basePlan struct {
	fields      []*ast.ResultField
	startupCost float64
	totalCost   float64
	rowCount    float64
	limit       float64

	parents  []Plan
	children []Plan
}

// StartupCost implements Plan StartupCost interface.
func (p *basePlan) StartupCost() float64 {
	return p.startupCost
}

// TotalCost implements Plan TotalCost interface.
func (p *basePlan) TotalCost() float64 {
	return p.totalCost
}

// RowCount implements Plan RowCount interface.
func (p *basePlan) RowCount() float64 {
	if p.limit == 0 {
		return p.rowCount
	}
	return math.Min(p.rowCount, p.limit)
}

// SetLimit implements Plan SetLimit interface.
func (p *basePlan) SetLimit(limit float64) {
	p.limit = limit
}

// Fields implements Plan Fields interface.
func (p *basePlan) Fields() []*ast.ResultField {
	return p.fields
}

// SetFields implements Plan SetFields interface.
func (p *basePlan) SetFields(fields []*ast.ResultField) {
	p.fields = fields
}

func (p *basePlan) AddParent(parent Plan) {
	if parent != nil {
		p.parents = append(p.parents, parent)
	}
}

func (p *basePlan) AddChild(child Plan) {
	if child != nil {
		p.children = append(p.children, child)
	}
}

func (p *basePlan) GetParentByIndex(index int) (parent Plan) {
	if index < len(p.parents) && index >= 0 {
		return p.parents[index]
	}
	return nil
}

func (p *basePlan) GetChildByIndex(index int) (parent Plan) {
	if index < len(p.children) && index >= 0 {
		return p.children[index]
	}
	return nil
}

func (p *basePlan) GetParents() []Plan {
	return p.parents
}

func (p *basePlan) GetChildren() []Plan {
	return p.children
}
