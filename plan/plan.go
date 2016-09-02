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
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
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
	// Lock is the type of SelectLock.
	Lock = "SelectLock"
	// Load is the type of LoadData.
	Load = "LoadData"
	// Up is the type of Update.
	Up = "Update"
	// Del is the type of Delete.
	Del = "Delete"
)

// Plan is a description of an execution flow.
// It is created from ast.Node first, then optimized by optimizer,
// then used by executor to create a Cursor which executes the statement.
type Plan interface {
	// Fields returns the result fields of the plan.
	Fields() []*ast.ResultField
	// SetFields sets the results fields of the plan.
	SetFields(fields []*ast.ResultField)
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
	// SetParents sets parents for plan.
	SetParents(...Plan)
	// SetParents sets children for plan.
	SetChildren(...Plan)
}

type requiredProperty struct {
	props []*columnProp
	group bool
}

type physicalPlanInfo struct {
	p     PhysicalPlan
	cost  float64
	count *uint64 // count is a unique value
}

type columnProp struct {
	col  *expression.Column
	desc bool
}

// LogicalPlan is a tree of logical operators.
// We can do a lot of logical optimization to it, like predicate push down and column pruning.
type LogicalPlan interface {
	Plan

	// PredicatePushDown push down predicates in where/on/having clause as deeply as possible.
	// It will accept a predicate that is a expression slice, and return the expressions that can't be pushed.
	// Because it may change the root when exists having clause, we need return a plan that representing a new root.
	PredicatePushDown([]expression.Expression) ([]expression.Expression, LogicalPlan, error)

	// PruneColumnsAndResolveIndices prunes unused columns and resolves index for columns.
	// This function returns a column slice representing columns from outer env and an error.
	// We need return outer columns, because Apply plan will prune inner Planner and it will know
	// how many columns referenced by inner plan exactly.
	PruneColumnsAndResolveIndices([]*expression.Column) ([]*expression.Column, error)

	// convert2PhysicalPlan converts logical plan to physical plan. The arg prop means the required sort property.
	// This function returns two response. The first one is the best plan that matches the required property strictly.
	// The second one is the best plan that needn't matches the required property.
	convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error)
}

// PhysicalPlan is a tree of physical operators.
type PhysicalPlan interface {
	json.Marshaler
	Plan

	// matchProperty means that this physical plan will try to return the best plan that matches the required property.
	// rowCounts means the child row counts, and childPlanInfo means the plan infos returned by children.
	matchProperty(prop *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo

	// Copy copies the current plan.
	Copy() PhysicalPlan

	// PushLimit tries to push down limit as deeply as possible.
	PushLimit(l *Limit) PhysicalPlan
}

type baseLogicalPlan struct {
	basePlan
	planMap map[string]*physicalPlanInfo
	count   uint64
}

func (p *requiredProperty) getHashKey() ([]byte, error) {
	datums := make([]types.Datum, 0, len(p.props)*3+1)
	datums = append(datums, types.NewDatum(p.group))
	for _, c := range p.props {
		datums = append(datums, types.NewDatum(c.desc), types.NewDatum(c.col.FromID), types.NewDatum(c.col.Index))
	}
	bytes, err := codec.EncodeValue(nil, datums)
	return bytes, errors.Trace(err)
}

func (p *baseLogicalPlan) getPlanInfo(prop *requiredProperty) (*physicalPlanInfo, error) {
	key, err := prop.getHashKey()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return p.planMap[string(key)], nil
}

func (p *baseLogicalPlan) storePlanInfo(prop *requiredProperty, info *physicalPlanInfo) {
	key, _ := prop.getHashKey()
	p.planMap[string(key)] = info
}

func newBaseLogicalPlan(tp string, a *idAllocator) baseLogicalPlan {
	return baseLogicalPlan{
		count:   -1,
		planMap: make(map[string]*physicalPlanInfo),
		basePlan: basePlan{
			tp:        tp,
			allocator: a,
		},
	}
}

// PredicatePushDown implements LogicalPlan PredicatePushDown interface.
func (p *baseLogicalPlan) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	if len(p.GetChildren()) == 0 {
		return predicates, nil, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	rest, _, err := child.PredicatePushDown(predicates)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if len(rest) > 0 {
		err = addSelection(p, child, rest, p.allocator)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	return nil, nil, nil
}

// PruneColumnsAndResolveIndices implements LogicalPlan PruneColumnsAndResolveIndices interface.
func (p *baseLogicalPlan) PruneColumnsAndResolveIndices(parentUsedCols []*expression.Column) ([]*expression.Column, error) {
	outer, err := p.GetChildByIndex(0).(LogicalPlan).PruneColumnsAndResolveIndices(parentUsedCols)
	p.SetSchema(p.GetChildByIndex(0).GetSchema())
	return outer, errors.Trace(err)
}

func (p *basePlan) initID() {
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

// MarshalJSON implements json.Marshaler interface.
func (p *basePlan) MarshalJSON() ([]byte, error) {
	children, err := json.Marshal(p.children)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"id\": \"%s\",\n \"children\": %s", p.id, children))
	buffer.WriteString("}")
	return buffer.Bytes(), nil
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
		if par.GetID() == parent.GetID() {
			p.parents[i] = newPar
			return nil
		}
	}
	return SystemInternalErrorType.Gen("ReplaceParent Failed!")
}

// ReplaceChild means replace a child with another one.
func (p *basePlan) ReplaceChild(child, newChild Plan) error {
	for i, ch := range p.children {
		if ch.GetID() == child.GetID() {
			p.children[i] = newChild
			return nil
		}
	}
	return SystemInternalErrorType.Gen("ReplaceChildren Failed!")
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

// RemoveAllParents implements Plan RemoveAllParents interface.
func (p *basePlan) SetParents(pars ...Plan) {
	p.parents = pars
}

// RemoveAllParents implements Plan RemoveAllParents interface.
func (p *basePlan) SetChildren(children ...Plan) {
	p.children = children
}
