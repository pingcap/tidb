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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

// Plan is the description of an execution flow.
// It is created from ast.Node first, then optimized by the optimizer,
// finally used by the executor to create a Cursor which executes the statement.
type Plan interface {
	// AddParent means appending a parent for plan.
	AddParent(parent Plan)
	// AddChild means appending a child for plan.
	AddChild(children Plan)
	// ReplaceParent means replacing a parent with another one.
	ReplaceParent(parent, newPar Plan) error
	// ReplaceChild means replacing a child with another one.
	ReplaceChild(children, newChild Plan) error
	// Get all the parents.
	Parents() []Plan
	// Get all the children.
	Children() []Plan
	// Set the schema.
	SetSchema(schema *expression.Schema)
	// Get the schema.
	Schema() *expression.Schema
	// Get the ID.
	ID() int
	// Get the ID in explain statement
	ExplainID() string
	// Get id allocator
	Allocator() *idAllocator
	// SetParents sets the parents for the plan.
	SetParents(...Plan)
	// SetChildren sets the children for the plan.
	SetChildren(...Plan)
	// replaceExprColumns replace all the column reference in the plan's expression node.
	replaceExprColumns(replace map[string]*expression.Column)

	context() context.Context

	// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
	ResolveIndices()

	// findColumn finds the column in basePlan's schema.
	// If the column is not in the schema, returns error.
	findColumn(*ast.ColumnName) (*expression.Column, int, error)
}

type columnProp struct {
	col  *expression.Column
	desc bool
}

func (c *columnProp) equal(nc *columnProp, ctx context.Context) bool {
	return c.col.Equal(nc.col, ctx) && c.desc == nc.desc
}

// taskType is the type of execution task.
type taskType int

const (
	rootTaskType          taskType = iota
	copSingleReadTaskType          // TableScan and IndexScan
	copDoubleReadTaskType          // IndexLookUp
)

// String implements fmt.Stringer interface.
func (t taskType) String() string {
	switch t {
	case rootTaskType:
		return "rootTask"
	case copSingleReadTaskType:
		return "copSingleReadTask"
	case copDoubleReadTaskType:
		return "copDoubleReadTask"
	}
	return "UnknownTaskType"
}

// requiredProp stands for the required physical property by parents.
// It contains the orders, if the order is desc and the task types.
type requiredProp struct {
	cols []*expression.Column
	desc bool
	// taskTp means the type of task that an operator requires.
	// It needs to be specified because two different tasks can't be compared with cost directly.
	// e.g. If a copTask takes less cost than a rootTask, we can't sure that we must choose the former one. Because the copTask
	// must be finished and increase its cost in sometime, but we can't make sure the finishing time. So the best way
	// to let the comparison fair is to add taskType to required property.
	taskTp taskType
	// expectedCnt means this operator may be closed after fetching expectedCnt records.
	expectedCnt float64
	// hashcode stores the hash code of a requiredProp, will be lazily calculated when function "hashCode()" being called.
	hashcode []byte
}

func (p *requiredProp) isPrefix(prop *requiredProp) bool {
	if len(p.cols) > len(prop.cols) || p.desc != prop.desc {
		return false
	}
	if p.taskTp != prop.taskTp {
		return false
	}
	for i := range p.cols {
		if !p.cols[i].Equal(prop.cols[i], nil) {
			return false
		}
	}
	return true
}

func (p *requiredProp) isEmpty() bool {
	return len(p.cols) == 0
}

// hashCode calculates hash code for a requiredProp object.
func (p *requiredProp) hashCode() []byte {
	if p.hashcode != nil {
		return p.hashcode
	}
	hashcodeSize := 8 + 8 + 8 + 16*len(p.cols)
	p.hashcode = make([]byte, 0, hashcodeSize)
	if p.desc {
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
	} else {
		p.hashcode = codec.EncodeInt(p.hashcode, 0)
	}
	p.hashcode = codec.EncodeInt(p.hashcode, int64(p.taskTp))
	p.hashcode = codec.EncodeFloat(p.hashcode, p.expectedCnt)
	for i, length := 0, len(p.cols); i < length; i++ {
		p.hashcode = append(p.hashcode, p.cols[i].HashCode()...)
	}
	return p.hashcode
}

// String implements fmt.Stringer interface. Just for test.
func (p *requiredProp) String() string {
	return fmt.Sprintf("Prop{cols: %s, desc: %v, taskTp: %s, expectedCount: %v}", p.cols, p.desc, p.taskTp, p.expectedCnt)
}

// LogicalPlan is a tree of logical operators.
// We can do a lot of logical optimizations to it, like predicate pushdown and column pruning.
type LogicalPlan interface {
	Plan

	// PredicatePushDown pushes down the predicates in the where/on/having clauses as deeply as possible.
	// It will accept a predicate that is an expression slice, and return the expressions that can't be pushed.
	// Because it might change the root if the having clause exists, we need to return a plan that represents a new root.
	PredicatePushDown([]expression.Expression) ([]expression.Expression, LogicalPlan, error)

	// PruneColumns prunes the unused columns.
	PruneColumns([]*expression.Column)

	// convert2NewPhysicalPlan converts the logical plan to the physical plan. It's a new interface.
	// It is called recursively from the parent to the children to create the result physical plan.
	// Some logical plans will convert the children to the physical plans in different ways, and return the one
	// with the lowest cost.
	convert2NewPhysicalPlan(prop *requiredProp) (task, error)

	// buildKeyInfo will collect the information of unique keys into schema.
	buildKeyInfo()

	// pushDownTopN will push down the topN or limit operator during logical optimization.
	pushDownTopN(topN *TopN) LogicalPlan

	// prepareStatsProfile will prepare the stats for this plan.
	prepareStatsProfile() *statsProfile

	// preparePossibleProperties is only used for join and aggregation. Like group by a,b,c, all permutation of (a,b,c) is
	// valid, but the ordered indices in leaf plan is limited. So we can get all possible order properties by a pre-walking.
	preparePossibleProperties() [][]*expression.Column

	// generatePhysicalPlans generates all possible plans.
	generatePhysicalPlans() []PhysicalPlan

	extractCorrelatedCols() []*expression.CorrelatedColumn
}

// PhysicalPlan is a tree of the physical operators.
type PhysicalPlan interface {
	Plan

	// Copy copies the current plan.
	Copy() PhysicalPlan

	// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
	// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
	attach2Task(...task) task

	// ToPB converts physical plan to tipb executor.
	ToPB(ctx context.Context) (*tipb.Executor, error)

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// getChildrenPossibleProps tries to push the required properties to its children and return all the possible properties.
	getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp

	// statsProfile will return the stats for this plan.
	statsProfile() *statsProfile
}

type baseLogicalPlan struct {
	basePlan *basePlan
	taskMap  map[string]task
}

type basePhysicalPlan struct {
	basePlan *basePlan
}

// ExplainInfo implements PhysicalPlan interface.
func (bp *basePhysicalPlan) ExplainInfo() string {
	return ""
}

func (p *baseLogicalPlan) getTask(prop *requiredProp) task {
	key := prop.hashCode()
	return p.taskMap[string(key)]
}

func (p *baseLogicalPlan) storeTask(prop *requiredProp, task task) {
	key := prop.hashCode()
	p.taskMap[string(key)] = task
}

func (p *baseLogicalPlan) buildKeyInfo() {
	for _, child := range p.basePlan.children {
		child.(LogicalPlan).buildKeyInfo()
	}
	if len(p.basePlan.children) == 1 {
		switch p.basePlan.self.(type) {
		case *Exists, *LogicalAggregation, *Projection:
			p.basePlan.schema.Keys = nil
		case *SelectLock:
			p.basePlan.schema.Keys = p.basePlan.children[0].Schema().Keys
		default:
			p.basePlan.schema.Keys = p.basePlan.children[0].Schema().Clone().Keys
		}
	} else {
		p.basePlan.schema.Keys = nil
	}
}

func newBasePlan(tp string, allocator *idAllocator, ctx context.Context, p Plan) *basePlan {
	return &basePlan{
		tp:        tp,
		allocator: allocator,
		id:        allocator.allocID(),
		ctx:       ctx,
		self:      p,
	}
}

func newBaseLogicalPlan(basePlan *basePlan) baseLogicalPlan {
	return baseLogicalPlan{
		taskMap:  make(map[string]task),
		basePlan: basePlan,
	}
}

func newBasePhysicalPlan(basePlan *basePlan) basePhysicalPlan {
	return basePhysicalPlan{
		basePlan: basePlan,
	}
}

// PredicatePushDown implements LogicalPlan interface.
func (p *baseLogicalPlan) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, LogicalPlan, error) {
	if len(p.basePlan.children) == 0 {
		return predicates, p.basePlan.self.(LogicalPlan), nil
	}
	child := p.basePlan.children[0].(LogicalPlan)
	rest, _, err := child.PredicatePushDown(predicates)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if len(rest) > 0 {
		err = addSelection(p.basePlan.self, child, rest, p.basePlan.allocator)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	return nil, p.basePlan.self.(LogicalPlan), nil
}

func (p *baseLogicalPlan) extractCorrelatedCols() []*expression.CorrelatedColumn {
	var corCols []*expression.CorrelatedColumn
	for _, child := range p.basePlan.children {
		corCols = append(corCols, child.(LogicalPlan).extractCorrelatedCols()...)
	}
	return corCols
}

func (p *basePlan) Allocator() *idAllocator {
	return p.allocator
}

// PruneColumns implements LogicalPlan interface.
func (p *baseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column) {
	if len(p.basePlan.children) == 0 {
		return
	}
	child := p.basePlan.children[0].(LogicalPlan)
	child.PruneColumns(parentUsedCols)
	p.basePlan.SetSchema(child.Schema())
}

// basePlan implements base Plan interface.
// Should be used as embedded struct in Plan implementations.
type basePlan struct {
	parents  []Plan
	children []Plan

	schema    *expression.Schema
	tp        string
	id        int
	allocator *idAllocator
	ctx       context.Context
	self      Plan
	profile   *statsProfile
	// expectedCnt means this operator may be closed after fetching expectedCnt records.
	expectedCnt float64
}

func (p *basePlan) copy() *basePlan {
	np := *p
	return &np
}

func (p *basePlan) replaceExprColumns(replace map[string]*expression.Column) {
}

// MarshalJSON implements json.Marshaler interface.
func (p *basePlan) MarshalJSON() ([]byte, error) {
	children := make([]int, 0, len(p.children))
	for _, child := range p.children {
		children = append(children, child.ID())
	}
	childrenStrs, err := json.Marshal(children)
	if err != nil {
		return nil, errors.Trace(err)
	}
	buffer := bytes.NewBufferString("{")
	buffer.WriteString(fmt.Sprintf("\"children\": %s", childrenStrs))
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// ID implements Plan ID interface.
func (p *basePlan) ID() int {
	return p.id
}

func (p *basePlan) ExplainID() string {
	return fmt.Sprintf("%s_%d", p.tp, p.id)
}

// SetSchema implements Plan SetSchema interface.
func (p *basePlan) SetSchema(schema *expression.Schema) {
	p.schema = schema
}

// Schema implements Plan Schema interface.
func (p *basePlan) Schema() *expression.Schema {
	return p.schema
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
		if par.ID() == parent.ID() {
			p.parents[i] = newPar
			return nil
		}
	}
	return SystemInternalErrorType.Gen("ReplaceParent Failed: parent \"%s\" not found", parent.ExplainID())
}

// ReplaceChild means replace a child with another one.
func (p *basePlan) ReplaceChild(child, newChild Plan) error {
	for i, ch := range p.children {
		if ch.ID() == child.ID() {
			p.children[i] = newChild
			return nil
		}
	}
	return SystemInternalErrorType.Gen("ReplaceChildren Failed: child \"%s\" not found", child.ExplainID())
}

// Parents implements Plan Parents interface.
func (p *basePlan) Parents() []Plan {
	return p.parents
}

// Children implements Plan Children interface.
func (p *basePlan) Children() []Plan {
	return p.children
}

// SetParents implements Plan SetParents interface.
func (p *basePlan) SetParents(pars ...Plan) {
	p.parents = pars
}

// SetChildren implements Plan SetChildren interface.
func (p *basePlan) SetChildren(children ...Plan) {
	p.children = children
}

func (p *basePlan) context() context.Context {
	return p.ctx
}

func (p *basePlan) findColumn(column *ast.ColumnName) (*expression.Column, int, error) {
	col, idx, err := p.Schema().FindColumnAndIndex(column)
	if err == nil && col == nil {
		err = errors.Errorf("column %s not found", column.Name.O)
	}
	return col, idx, errors.Trace(err)
}
