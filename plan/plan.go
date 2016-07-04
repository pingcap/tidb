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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
)

const (
	// Sel is the type of Selection.
	Sel = "Selection"
	// Proj is the type of Projection.
	Proj = "Projection"
	// Agg is the type of Aggregation.
	Agg = "Aggregation"
	// Jn is the type of Join.
	Jn = "Join"
	// Un is the type of Union.
	Un = "Union"
	// Ts is the type of TableScan.
	Ts = "TableScan"
	// Idx is the type of IndexScan.
	Idx = "IndexScan"
	// Srt is the type of Sort.
	Srt = "Sort"
	// Lim is the type of Limit.
	Lim = "Limit"
	// App is the type of Apply.
	App = "Apply"
	// Dis is the type of Distinct.
	Dis = "Distinct"
	// Trm is the type of Trim.
	Trm = "Trim"
	// MOR is the type of MaxOneRow.
	MOR = "MaxOneRow"
	// Ext is the type of Exists.
	Ext = "Exists"
	// Dual is the type of TableDual.
	Dual = "TableDual"
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
	// Check weather this plan is correlated or not.
	IsCorrelated() bool
}

// LogicalPlan is a tree of logical operators.
// We can do a lot of logical optimization to it, like predicate push down and column pruning.
type LogicalPlan interface {
	Plan

	// PredicatePushDown push down predicates in where/on/having clause as deeply as possible.
	PredicatePushDown([]expression.Expression) ([]expression.Expression, error)

	// PruneColumnsAndResolveIndices prunes unused columns and resolves index for columns.
	// This function returns a column slice representing outer columns and an error.
	PruneColumnsAndResolveIndices([]*expression.Column) ([]*expression.Column, error)
	// TODO: implement Convert2PhysicalPlan()
}

// TODO: implement PhysicalPlan

type baseLogicalPlan struct {
	basePlan
}

func newBaseLogicalPlan(tp string, a *idAllocator) baseLogicalPlan {
	return baseLogicalPlan{
		basePlan: basePlan{
			tp:        tp,
			allocator: a,
		},
	}
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *baseLogicalPlan) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, error) {
	rest, err1 := p.GetChildByIndex(0).(LogicalPlan).PredicatePushDown(predicates)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}
	if len(rest) > 0 {
		err1 = addSelection(p, p.GetChildByIndex(0).(LogicalPlan), rest, p.allocator)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
	}
	return nil, nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *baseLogicalPlan) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	outer, err := p.GetChildByIndex(0).(LogicalPlan).PruneColumnsAndResolveIndices(parentUsedCols)
	p.SetSchema(p.GetChildByIndex(0).GetSchema())
	return outer, errors.Trace(err)
}

func (p *baseLogicalPlan) initID() {
	p.id = p.tp + p.allocator.allocID()
}

// basePlan implements base Plan interface.
// Should be used as embedded struct in Plan implementations.
type basePlan struct {
	fields      []*ast.ResultField
	startupCost float64
	totalCost   float64
	rowCount    float64
	limit       float64
	correlated  bool

	parents  []Plan
	children []Plan

	schema    expression.Schema
	tp        string
	id        string
	allocator *idAllocator
}

// IsCorrelated implements Plan IsCorrelated interface.
func (p *basePlan) IsCorrelated() bool {
	return p.correlated
}

// GetID implements Plan GetID interface.
func (p *basePlan) GetID() string {
	return p.id
}

// SetSchema implements Plan SetSchema interface.
func (p *basePlan) SetSchema(schema expression.Schema) {
	p.schema = schema
}

// GetSchema implements Plan GetSchema interface.
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
