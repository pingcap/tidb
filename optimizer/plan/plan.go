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
	"github.com/pingcap/tidb/expression"
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
	// AddParent means append a parent for plan.
	AddParent(parent Plan)
	// AddChild means append a child for plan.
	AddChild(children Plan)
	// ReplaceParent means replace a parent with another one.
	ReplaceParent(parent, newPar Plan) error
	// ReplaceChild means replace a child with another one.
	ReplaceChild(children, newChild Plan) error
	// Retrieve parent by index.
	GetParentByIndex(index int) Plan
	// Retrieve child by index.
	GetChildByIndex(index int) Plan
	// Get all the parents.
	GetParents() []Plan
	// Get all the children.
	GetChildren() []Plan
	// Set the schema.
	SetSchema(schema expression.Schema)
	// Get the schema.
	GetSchema() expression.Schema
	// Get ID.
	GetID() string
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

	schema expression.Schema
	id     string
}

func (p *basePlan) GetID() string {
	return p.id
}

func (p *basePlan) SetSchema(schema expression.Schema) {
	p.schema = schema
}

func (p *basePlan) GetSchema() expression.Schema {
	return p.schema
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

// AddParent implements Plan AddParent interface.
func (p *basePlan) AddParent(parent Plan) {
	p.parents = append(p.parents, parent)
}

// AddChild implements Plan AddChild interface.
func (p *basePlan) AddChild(child Plan) {
	p.children = append(p.children, child)
}

// ReplaceParent means replace a parent for another one.
func (p *basePlan) ReplaceParent(parent, newPar Plan) error {
	for i, par := range p.parents {
		if par == parent {
			p.parents[i] = newPar
			return nil
		}
	}
	return SystemInternalErrorType.Gen("RemoveParent Failed!")
}

// ReplaceChild means replace a child with another one.
func (p *basePlan) ReplaceChild(child, newChild Plan) error {
	for i, ch := range p.children {
		if ch == child {
			p.children[i] = newChild
			return nil
		}
	}
	return SystemInternalErrorType.Gen("RemoveChildren Failed!")
}

// GetParentByIndex implements Plan GetParentByIndex interface.
func (p *basePlan) GetParentByIndex(index int) (parent Plan) {
	if index < len(p.parents) && index >= 0 {
		return p.parents[index]
	}
	return nil
}

// GetChildByIndex implements Plan GetChildByIndex interface.
func (p *basePlan) GetChildByIndex(index int) (parent Plan) {
	if index < len(p.children) && index >= 0 {
		return p.children[index]
	}
	return nil
}

// GetParents implements Plan GetParents interface.
func (p *basePlan) GetParents() []Plan {
	return p.parents
}

// GetChildren implements Plan GetChildren interface.
func (p *basePlan) GetChildren() []Plan {
	return p.children
}
