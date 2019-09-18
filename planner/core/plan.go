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

package core

import (
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/stringutil"
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

	// TP get the plan type.
	TP() string

	// Get the ID in explain statement
	ExplainID() fmt.Stringer

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// replaceExprColumns replace all the column reference in the plan's expression node.
	replaceExprColumns(replace map[string]*expression.Column)

	context() sessionctx.Context

	// property.StatsInfo will return the property.StatsInfo for this plan.
	statsInfo() *property.StatsInfo
}

func enforceProperty(p *property.PhysicalProperty, tsk task, ctx sessionctx.Context) task {
	if p.IsEmpty() || tsk.plan() == nil {
		return tsk
	}
	tsk = finishCopTask(ctx, tsk)
	sortReqProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, Items: p.Items, ExpectedCnt: math.MaxFloat64}
	sort := PhysicalSort{ByItems: make([]*util.ByItems, 0, len(p.Items))}.Init(ctx, tsk.plan().statsInfo(), sortReqProp)
	for _, col := range p.Items {
		sort.ByItems = append(sort.ByItems, &util.ByItems{Expr: col.Col, Desc: col.Desc})
	}
	return sort.attach2Task(tsk)
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
	PruneColumns([]*expression.Column) error

	// findBestTask converts the logical plan to the physical plan. It's a new interface.
	// It is called recursively from the parent to the children to create the result physical plan.
	// Some logical plans will convert the children to the physical plans in different ways, and return the one
	// with the lowest cost.
	findBestTask(prop *property.PhysicalProperty) (task, error)

	// buildKeyInfo will collect the information of unique keys into schema.
	buildKeyInfo()

	// pushDownTopN will push down the topN or limit operator during logical optimization.
	pushDownTopN(topN *LogicalTopN) LogicalPlan

	// recursiveDeriveStats derives statistic info between plans.
	recursiveDeriveStats() (*property.StatsInfo, error)

	// DeriveStats derives statistic info for current plan node given child stats.
	DeriveStats(childStats []*property.StatsInfo) (*property.StatsInfo, error)

	// preparePossibleProperties is only used for join and aggregation. Like group by a,b,c, all permutation of (a,b,c) is
	// valid, but the ordered indices in leaf plan is limited. So we can get all possible order properties by a pre-walking.
	// Please make sure that children's method is called though we may not need its return value,
	// so we can prepare possible properties for every LogicalPlan node.
	preparePossibleProperties() [][]*expression.Column

	// exhaustPhysicalPlans generates all possible plans that can match the required property.
	exhaustPhysicalPlans(*property.PhysicalProperty) []PhysicalPlan

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

	// SetChild sets the ith child for the plan.
	SetChild(i int, child LogicalPlan)
}

// PhysicalPlan is a tree of the physical operators.
type PhysicalPlan interface {
	Plan

	// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
	// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
	attach2Task(...task) task

	// ToPB converts physical plan to tipb executor.
	ToPB(ctx sessionctx.Context) (*tipb.Executor, error)

	// getChildReqProps gets the required property by child index.
	GetChildReqProps(idx int) *property.PhysicalProperty

	// StatsCount returns the count of property.StatsInfo for this plan.
	StatsCount() float64

	// Get all the children.
	Children() []PhysicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...PhysicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child PhysicalPlan)

	// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
	ResolveIndices() error

	// ExplainNormalizedInfo returns operator normalized information for generating digest.
	ExplainNormalizedInfo() string
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

	childrenReqProps []*property.PhysicalProperty
	self             PhysicalPlan
	children         []PhysicalPlan
}

// ExplainInfo implements Plan interface.
func (p *basePhysicalPlan) ExplainNormalizedInfo() string {
	return ""
}

func (p *basePhysicalPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return p.childrenReqProps[idx]
}

// ExplainInfo implements PhysicalPlan interface.
func (p *basePhysicalPlan) ExplainInfo() string {
	return ""
}

func (p *baseLogicalPlan) getTask(prop *property.PhysicalProperty) task {
	key := prop.HashCode()
	return p.taskMap[string(key)]
}

func (p *baseLogicalPlan) storeTask(prop *property.PhysicalProperty, task task) {
	key := prop.HashCode()
	p.taskMap[string(key)] = task
}

func (p *baseLogicalPlan) buildKeyInfo() {
	for _, child := range p.children {
		child.buildKeyInfo()
	}
	switch p.self.(type) {
	case *LogicalLock, *LogicalLimit, *LogicalSort, *LogicalSelection, *LogicalApply, *LogicalProjection:
		p.maxOneRow = p.children[0].MaxOneRow()
	case *LogicalMaxOneRow:
		p.maxOneRow = true
	}
}

func newBasePlan(ctx sessionctx.Context, tp string) basePlan {
	ctx.GetSessionVars().PlanID++
	id := ctx.GetSessionVars().PlanID
	return basePlan{
		tp:  tp,
		id:  id,
		ctx: ctx,
	}
}

func newBaseLogicalPlan(ctx sessionctx.Context, tp string, self LogicalPlan) baseLogicalPlan {
	return baseLogicalPlan{
		taskMap:  make(map[string]task),
		basePlan: newBasePlan(ctx, tp),
		self:     self,
	}
}

func newBasePhysicalPlan(ctx sessionctx.Context, tp string, self PhysicalPlan) basePhysicalPlan {
	return basePhysicalPlan{
		basePlan: newBasePlan(ctx, tp),
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
func (p *baseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column) error {
	if len(p.children) == 0 {
		return nil
	}
	return p.children[0].PruneColumns(parentUsedCols)
}

// basePlan implements base Plan interface.
// Should be used as embedded struct in Plan implementations.
type basePlan struct {
	tp    string
	id    int
	ctx   sessionctx.Context
	stats *property.StatsInfo
}

func (p *basePlan) replaceExprColumns(replace map[string]*expression.Column) {
}

// ID implements Plan ID interface.
func (p *basePlan) ID() int {
	return p.id
}

// property.StatsInfo implements the Plan interface.
func (p *basePlan) statsInfo() *property.StatsInfo {
	return p.stats
}

func (p *basePlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return p.tp + "_" + strconv.Itoa(p.id)
	})
}

// TP implements Plan interface.
func (p *basePlan) TP() string {
	return p.tp
}
func (p *basePlan) ExplainInfo() string {
	return "N/A"
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

// SetChild implements LogicalPlan SetChild interface.
func (p *baseLogicalPlan) SetChild(i int, child LogicalPlan) {
	p.children[i] = child
}

// SetChild implements PhysicalPlan SetChild interface.
func (p *basePhysicalPlan) SetChild(i int, child PhysicalPlan) {
	p.children[i] = child
}

func (p *basePlan) context() sessionctx.Context {
	return p.ctx
}

func (p *baseLogicalPlan) findColumn(column *ast.ColumnName) (*expression.Column, int, error) {
	col, idx, err := p.self.Schema().FindColumnAndIndex(column)
	if err == nil && col == nil {
		err = errors.Errorf("column %s not found", column.Name.O)
	}
	return col, idx, err
}
