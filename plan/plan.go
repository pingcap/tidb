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
	// Get the schema.
	Schema() *expression.Schema
	// Get the ID.
	ID() int
	// Get the ID in explain statement
	ExplainID() string
	// replaceExprColumns replace all the column reference in the plan's expression node.
	replaceExprColumns(replace map[string]*expression.Column)

	context() context.Context
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

func (p *requiredProp) allColsFromSchema(schema *expression.Schema) bool {
	return schema.ColumnsIndices(p.cols) != nil
}

func (p *requiredProp) isPrefix(prop *requiredProp) bool {
	if len(p.cols) > len(prop.cols) || p.desc != prop.desc {
		return false
	}
	for i := range p.cols {
		if !p.cols[i].Equal(prop.cols[i], nil) {
			return false
		}
	}
	return true
}

// Check if this prop's columns can match by items totally.
func (p *requiredProp) matchItems(items []*ByItems) bool {
	for i, col := range p.cols {
		sortItem := items[i]
		if sortItem.Desc != p.desc || !sortItem.Expr.Equal(col, nil) {
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
	PredicatePushDown([]expression.Expression) ([]expression.Expression, LogicalPlan)

	// PruneColumns prunes the unused columns.
	PruneColumns([]*expression.Column)

	// convert2PhysicalPlan converts the logical plan to the physical plan. It's a new interface.
	// It is called recursively from the parent to the children to create the result physical plan.
	// Some logical plans will convert the children to the physical plans in different ways, and return the one
	// with the lowest cost.
	convert2PhysicalPlan(prop *requiredProp) (task, error)

	// buildKeyInfo will collect the information of unique keys into schema.
	buildKeyInfo()

	// pushDownTopN will push down the topN or limit operator during logical optimization.
	pushDownTopN(topN *LogicalTopN) LogicalPlan

	// deriveStats derives statistic info between plans.
	deriveStats() *statsInfo

	// preparePossibleProperties is only used for join and aggregation. Like group by a,b,c, all permutation of (a,b,c) is
	// valid, but the ordered indices in leaf plan is limited. So we can get all possible order properties by a pre-walking.
	preparePossibleProperties() [][]*expression.Column

	// genPhysPlansByReqProp generates all possible plans that can match the required property.
	genPhysPlansByReqProp(*requiredProp) []PhysicalPlan

	extractCorrelatedCols() []*expression.CorrelatedColumn

	// MaxOneRow means whether this operator only returns max one row.
	MaxOneRow() bool

	// findColumn finds the column in basePlan's schema.
	// If the column is not in the schema, returns an error.
	findColumn(*ast.ColumnName) (*expression.Column, int, error)

	// Get all the children.
	Children() []LogicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...LogicalPlan)
}

// PhysicalPlan is a tree of the physical operators.
type PhysicalPlan interface {
	Plan

	// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
	// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
	attach2Task(...task) task

	// ToPB converts physical plan to tipb executor.
	ToPB(ctx context.Context) (*tipb.Executor, error)

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// getChildReqProps gets the required property by child index.
	getChildReqProps(idx int) *requiredProp

	// StatsInfo will return the statsInfo for this plan.
	StatsInfo() *statsInfo

	// Get all the children.
	Children() []PhysicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...PhysicalPlan)

	// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
	ResolveIndices()
}

type baseLogicalPlan struct {
	basePlan

	taskMap   map[string]task
	self      LogicalPlan
	maxOneRow bool
	children  []LogicalPlan
}

func (p *baseLogicalPlan) MaxOneRow() bool {
	return p.maxOneRow
}

type basePhysicalPlan struct {
	basePlan

	childrenReqProps []*requiredProp
	self             PhysicalPlan
	children         []PhysicalPlan
}

func (p *basePhysicalPlan) getChildReqProps(idx int) *requiredProp {
	return p.childrenReqProps[idx]
}

// ExplainInfo implements PhysicalPlan interface.
func (p *basePhysicalPlan) ExplainInfo() string {
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
	for _, child := range p.children {
		child.buildKeyInfo()
	}
	switch p.self.(type) {
	case *LogicalLock, *LogicalLimit, *LogicalSort, *LogicalSelection, *LogicalApply, *LogicalProjection:
		p.maxOneRow = p.children[0].MaxOneRow()
	case *LogicalMaxOneRow, *LogicalExists:
		p.maxOneRow = true
	}
}

func newBasePlan(tp string, ctx context.Context) basePlan {
	ctx.GetSessionVars().PlanID++
	id := ctx.GetSessionVars().PlanID
	return basePlan{
		tp:  tp,
		id:  id,
		ctx: ctx,
	}
}

func newBaseLogicalPlan(tp string, ctx context.Context, self LogicalPlan) baseLogicalPlan {
	return baseLogicalPlan{
		taskMap:  make(map[string]task),
		basePlan: newBasePlan(tp, ctx),
		self:     self,
	}
}

func newBasePhysicalPlan(tp string, ctx context.Context, self PhysicalPlan) basePhysicalPlan {
	return basePhysicalPlan{
		basePlan: newBasePlan(tp, ctx),
		self:     self,
	}
}

func (p *baseLogicalPlan) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.children))
	for _, child := range p.children {
		corCols = append(corCols, child.extractCorrelatedCols()...)
	}
	return corCols
}

// PruneColumns implements LogicalPlan interface.
func (p *baseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column) {
	if len(p.children) == 0 {
		return
	}
	p.children[0].PruneColumns(parentUsedCols)
}

// basePlan implements base Plan interface.
// Should be used as embedded struct in Plan implementations.
type basePlan struct {
	tp    string
	id    int
	ctx   context.Context
	stats *statsInfo
}

func (p *basePlan) replaceExprColumns(replace map[string]*expression.Column) {
}

// ID implements Plan ID interface.
func (p *basePlan) ID() int {
	return p.id
}

func (p *basePlan) ExplainID() string {
	return fmt.Sprintf("%s_%d", p.tp, p.id)
}

// Schema implements Plan Schema interface.
func (p *baseLogicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

// Schema implements Plan Schema interface.
func (p *basePhysicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

// Children implements LogicalPlan Children interface.
func (p *baseLogicalPlan) Children() []LogicalPlan {
	return p.children
}

// Children implements PhysicalPlan Children interface.
func (p *basePhysicalPlan) Children() []PhysicalPlan {
	return p.children
}

// SetChildren implements LogicalPlan SetChildren interface.
func (p *baseLogicalPlan) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// SetChildren implements PhysicalPlan SetChildren interface.
func (p *basePhysicalPlan) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

func (p *basePlan) context() context.Context {
	return p.ctx
}

func (p *baseLogicalPlan) findColumn(column *ast.ColumnName) (*expression.Column, int, error) {
	col, idx, err := p.self.Schema().FindColumnAndIndex(column)
	if err == nil && col == nil {
		err = errors.Errorf("column %s not found", column.Name.O)
	}
	return col, idx, errors.Trace(err)
}
