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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	fd "github.com/pingcap/tidb/planner/funcdep"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/tracing"
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

	SCtx() sessionctx.Context

	// property.StatsInfo will return the property.StatsInfo for this plan.
	statsInfo() *property.StatsInfo

	// OutputNames returns the outputting names of each column.
	OutputNames() types.NameSlice

	// SetOutputNames sets the outputting name by the given slice.
	SetOutputNames(names types.NameSlice)

	SelectBlockOffset() int

	buildPlanTrace() *tracing.PlanTrace
}

func enforceProperty(p *property.PhysicalProperty, tsk task, ctx sessionctx.Context) task {
	if p.TaskTp == property.MppTaskType {
		mpp, ok := tsk.(*mppTask)
		if !ok || mpp.invalid() {
			return invalidTask
		}
		if !p.IsSortItemAllForPartition() {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because operator `Sort` is not supported now.")
			return invalidTask
		}
		tsk = mpp.enforceExchanger(p)
	}
	if p.IsSortItemEmpty() || tsk.plan() == nil {
		return tsk
	}
	if p.TaskTp != property.MppTaskType {
		tsk = tsk.convertToRootTask(ctx)
	}
	sortReqProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, SortItems: p.SortItems, ExpectedCnt: math.MaxFloat64}
	sort := PhysicalSort{
		ByItems:       make([]*util.ByItems, 0, len(p.SortItems)),
		IsPartialSort: p.IsSortItemAllForPartition(),
	}.Init(ctx, tsk.plan().statsInfo(), tsk.plan().SelectBlockOffset(), sortReqProp)
	for _, col := range p.SortItems {
		sort.ByItems = append(sort.ByItems, &util.ByItems{Expr: col.Col, Desc: col.Desc})
	}
	return sort.attach2Task(tsk)
}

// optimizeByShuffle insert `PhysicalShuffle` to optimize performance by running in a parallel manner.
func optimizeByShuffle(tsk task, ctx sessionctx.Context) task {
	if tsk.plan() == nil {
		return tsk
	}

	switch p := tsk.plan().(type) {
	case *PhysicalWindow:
		if shuffle := optimizeByShuffle4Window(p, ctx); shuffle != nil {
			return shuffle.attach2Task(tsk)
		}
	case *PhysicalMergeJoin:
		if shuffle := optimizeByShuffle4MergeJoin(p, ctx); shuffle != nil {
			return shuffle.attach2Task(tsk)
		}
	case *PhysicalStreamAgg:
		if shuffle := optimizeByShuffle4StreamAgg(p, ctx); shuffle != nil {
			return shuffle.attach2Task(tsk)
		}
	}
	return tsk
}

func optimizeByShuffle4Window(pp *PhysicalWindow, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().WindowConcurrency()
	if concurrency <= 1 {
		return nil
	}

	sort, ok := pp.Children()[0].(*PhysicalSort)
	if !ok {
		// Multi-thread executing on SORTED data source is not effective enough by current implementation.
		// TODO: Implement a better one.
		return nil
	}
	tail, dataSource := sort, sort.Children()[0]

	partitionBy := make([]*expression.Column, 0, len(pp.PartitionBy))
	for _, item := range pp.PartitionBy {
		partitionBy = append(partitionBy, item.Col)
	}
	NDV := int(getColsNDV(partitionBy, dataSource.Schema(), dataSource.statsInfo()))
	if NDV <= 1 {
		return nil
	}
	concurrency = mathutil.Min(concurrency, NDV)

	byItems := make([]expression.Expression, 0, len(pp.PartitionBy))
	for _, item := range pp.PartitionBy {
		byItems = append(byItems, item.Col)
	}
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        []PhysicalPlan{tail},
		DataSources:  []PhysicalPlan{dataSource},
		SplitterType: PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{byItems},
	}.Init(ctx, pp.statsInfo(), pp.SelectBlockOffset(), reqProp)
	return shuffle
}

func optimizeByShuffle4StreamAgg(pp *PhysicalStreamAgg, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().StreamAggConcurrency()
	if concurrency <= 1 {
		return nil
	}

	sort, ok := pp.Children()[0].(*PhysicalSort)
	if !ok {
		// Multi-thread executing on SORTED data source is not effective enough by current implementation.
		// TODO: Implement a better one.
		return nil
	}
	tail, dataSource := sort, sort.Children()[0]

	partitionBy := make([]*expression.Column, 0, len(pp.GroupByItems))
	for _, item := range pp.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			partitionBy = append(partitionBy, col)
		}
	}
	NDV := int(getColsNDV(partitionBy, dataSource.Schema(), dataSource.statsInfo()))
	if NDV <= 1 {
		return nil
	}
	concurrency = mathutil.Min(concurrency, NDV)

	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        []PhysicalPlan{tail},
		DataSources:  []PhysicalPlan{dataSource},
		SplitterType: PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{cloneExprs(pp.GroupByItems)},
	}.Init(ctx, pp.statsInfo(), pp.SelectBlockOffset(), reqProp)
	return shuffle
}

func optimizeByShuffle4MergeJoin(pp *PhysicalMergeJoin, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().MergeJoinConcurrency()
	if concurrency <= 1 {
		return nil
	}

	children := pp.Children()
	dataSources := make([]PhysicalPlan, len(children))
	tails := make([]PhysicalPlan, len(children))

	for i := range children {
		sort, ok := children[i].(*PhysicalSort)
		if !ok {
			// Multi-thread executing on SORTED data source is not effective enough by current implementation.
			// TODO: Implement a better one.
			return nil
		}
		tails[i], dataSources[i] = sort, sort.Children()[0]
	}

	leftByItemArray := make([]expression.Expression, 0, len(pp.LeftJoinKeys))
	for _, col := range pp.LeftJoinKeys {
		leftByItemArray = append(leftByItemArray, col.Clone())
	}
	rightByItemArray := make([]expression.Expression, 0, len(pp.RightJoinKeys))
	for _, col := range pp.RightJoinKeys {
		rightByItemArray = append(rightByItemArray, col.Clone())
	}
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        tails,
		DataSources:  dataSources,
		SplitterType: PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{leftByItemArray, rightByItemArray},
	}.Init(ctx, pp.statsInfo(), pp.SelectBlockOffset(), reqProp)
	return shuffle
}

// LogicalPlan is a tree of logical operators.
// We can do a lot of logical optimizations to it, like predicate pushdown and column pruning.
type LogicalPlan interface {
	Plan

	// HashCode encodes a LogicalPlan to fast compare whether a LogicalPlan equals to another.
	// We use a strict encode method here which ensures there is no conflict.
	HashCode() []byte

	// PredicatePushDown pushes down the predicates in the where/on/having clauses as deeply as possible.
	// It will accept a predicate that is an expression slice, and return the expressions that can't be pushed.
	// Because it might change the root if the having clause exists, we need to return a plan that represents a new root.
	PredicatePushDown([]expression.Expression, *logicalOptimizeOp) ([]expression.Expression, LogicalPlan)

	// PruneColumns prunes the unused columns.
	PruneColumns([]*expression.Column, *logicalOptimizeOp) error

	// findBestTask converts the logical plan to the physical plan. It's a new interface.
	// It is called recursively from the parent to the children to create the result physical plan.
	// Some logical plans will convert the children to the physical plans in different ways, and return the one
	// With the lowest cost and how many plans are found in this function.
	// planCounter is a counter for planner to force a plan.
	// If planCounter > 0, the clock_th plan generated in this function will be returned.
	// If planCounter = 0, the plan generated in this function will not be considered.
	// If planCounter = -1, then we will not force plan.
	findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, op *physicalOptimizeOp) (task, int64, error)

	// BuildKeyInfo will collect the information of unique keys into schema.
	// Because this method is also used in cascades planner, we cannot use
	// things like `p.schema` or `p.children` inside it. We should use the `selfSchema`
	// and `childSchema` instead.
	BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema)

	// pushDownTopN will push down the topN or limit operator during logical optimization.
	pushDownTopN(topN *LogicalTopN, opt *logicalOptimizeOp) LogicalPlan

	// recursiveDeriveStats derives statistic info between plans.
	recursiveDeriveStats(colGroups [][]*expression.Column) (*property.StatsInfo, error)

	// DeriveStats derives statistic info for current plan node given child stats.
	// We need selfSchema, childSchema here because it makes this method can be used in
	// cascades planner, where LogicalPlan might not record its children or schema.
	DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error)

	// ExtractColGroups extracts column groups from child operator whose DNVs are required by the current operator.
	// For example, if current operator is LogicalAggregation of `Group By a, b`, we indicate the child operators to maintain
	// and propagate the NDV info of column group (a, b), to improve the row count estimation of current LogicalAggregation.
	// The parameter colGroups are column groups required by upper operators, besides from the column groups derived from
	// current operator, we should pass down parent colGroups to child operator as many as possible.
	ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column

	// PreparePossibleProperties is only used for join and aggregation. Like group by a,b,c, all permutation of (a,b,c) is
	// valid, but the ordered indices in leaf plan is limited. So we can get all possible order properties by a pre-walking.
	PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column

	// exhaustPhysicalPlans generates all possible plans that can match the required property.
	// It will return:
	// 1. All possible plans that can match the required property.
	// 2. Whether the SQL hint can work. Return true if there is no hint.
	exhaustPhysicalPlans(*property.PhysicalProperty) (physicalPlans []PhysicalPlan, hintCanWork bool, err error)

	// ExtractCorrelatedCols extracts correlated columns inside the LogicalPlan.
	ExtractCorrelatedCols() []*expression.CorrelatedColumn

	// MaxOneRow means whether this operator only returns max one row.
	MaxOneRow() bool

	// Get all the children.
	Children() []LogicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...LogicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child LogicalPlan)

	// rollBackTaskMap roll back all taskMap's logs after TimeStamp TS.
	rollBackTaskMap(TS uint64)

	// canPushToCop check if we might push this plan to a specific store.
	canPushToCop(store kv.StoreType) bool

	// ExtractFD derive the FDSet from the tree bottom up.
	ExtractFD() *fd.FDSet
}

// PhysicalPlan is a tree of the physical operators.
type PhysicalPlan interface {
	Plan

	// GetPlanCost calculates the cost of the plan if it has not been calculated yet and returns the cost.
	GetPlanCost(taskType property.TaskType, costFlag uint64) (float64, error)

	// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
	// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
	attach2Task(...task) task

	// ToPB converts physical plan to tipb executor.
	ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error)

	// getChildReqProps gets the required property by child index.
	GetChildReqProps(idx int) *property.PhysicalProperty

	// StatsCount returns the count of property.StatsInfo for this plan.
	StatsCount() float64

	// ExtractCorrelatedCols extracts correlated columns inside the PhysicalPlan.
	ExtractCorrelatedCols() []*expression.CorrelatedColumn

	// Get all the children.
	Children() []PhysicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...PhysicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child PhysicalPlan)

	// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
	ResolveIndices() error

	// Stats returns the StatsInfo of the plan.
	Stats() *property.StatsInfo

	// Cost returns the estimated cost of the subplan.
	// Deprecated: use the new method GetPlanCost
	Cost() float64

	// SetCost set the cost of the subplan.
	// Deprecated: use the new method GetPlanCost
	SetCost(cost float64)

	// ExplainNormalizedInfo returns operator normalized information for generating digest.
	ExplainNormalizedInfo() string

	// Clone clones this physical plan.
	Clone() (PhysicalPlan, error)
}

type baseLogicalPlan struct {
	basePlan

	taskMap map[string]task
	// taskMapBak forms a backlog stack of taskMap, used to roll back the taskMap.
	taskMapBak []string
	// taskMapBakTS stores the timestamps of logs.
	taskMapBakTS []uint64
	self         LogicalPlan
	maxOneRow    bool
	children     []LogicalPlan
	// fdSet is a set of functional dependencies(FDs) which powers many optimizations,
	// including eliminating unnecessary DISTINCT operators, simplifying ORDER BY columns,
	// removing Max1Row operators, and mapping semi-joins to inner-joins.
	// for now, it's hard to maintain in individual operator, build it from bottom up when using.
	fdSet *fd.FDSet
}

// ExtractFD return the children[0]'s fdSet if there are no adding/removing fd in this logic plan.
func (p *baseLogicalPlan) ExtractFD() *fd.FDSet {
	if p.fdSet != nil {
		return p.fdSet
	}
	fds := &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
	for _, ch := range p.children {
		fds.AddFrom(ch.ExtractFD())
	}
	return fds
}

func (p *baseLogicalPlan) MaxOneRow() bool {
	return p.maxOneRow
}

// ExplainInfo implements Plan interface.
func (p *baseLogicalPlan) ExplainInfo() string {
	return ""
}

type basePhysicalPlan struct {
	basePlan

	childrenReqProps []*property.PhysicalProperty
	self             PhysicalPlan
	children         []PhysicalPlan
	cost             float64

	// used by the new cost interface
	planCostInit bool
	planCost     float64
}

// Cost implements PhysicalPlan interface.
func (p *basePhysicalPlan) Cost() float64 {
	return p.cost
}

// SetCost implements PhysicalPlan interface.
func (p *basePhysicalPlan) SetCost(cost float64) {
	p.cost = cost
}

func (p *basePhysicalPlan) cloneWithSelf(newSelf PhysicalPlan) (*basePhysicalPlan, error) {
	base := &basePhysicalPlan{
		basePlan: p.basePlan,
		self:     newSelf,
	}
	for _, child := range p.children {
		cloned, err := child.Clone()
		if err != nil {
			return nil, err
		}
		base.children = append(base.children, cloned)
	}
	for _, prop := range p.childrenReqProps {
		if prop == nil {
			continue
		}
		base.childrenReqProps = append(base.childrenReqProps, prop.CloneEssentialFields())
	}
	return base, nil
}

// Clone implements PhysicalPlan interface.
func (p *basePhysicalPlan) Clone() (PhysicalPlan, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p.self)
}

// ExplainInfo implements Plan interface.
func (p *basePhysicalPlan) ExplainInfo() string {
	return ""
}

// ExplainNormalizedInfo implements PhysicalPlan interface.
func (p *basePhysicalPlan) ExplainNormalizedInfo() string {
	return ""
}

func (p *basePhysicalPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return p.childrenReqProps[idx]
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *basePhysicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// GetLogicalTS4TaskMap get the logical TimeStamp now to help rollback the TaskMap changes after that.
func (p *baseLogicalPlan) GetLogicalTS4TaskMap() uint64 {
	p.ctx.GetSessionVars().StmtCtx.TaskMapBakTS += 1
	return p.ctx.GetSessionVars().StmtCtx.TaskMapBakTS
}

func (p *baseLogicalPlan) rollBackTaskMap(TS uint64) {
	if !p.ctx.GetSessionVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		return
	}
	if len(p.taskMapBak) > 0 {
		// Rollback all the logs with TimeStamp TS.
		N := len(p.taskMapBak)
		for i := 0; i < N; i++ {
			cur := p.taskMapBak[i]
			if p.taskMapBakTS[i] < TS {
				continue
			}

			// Remove the i_th log.
			p.taskMapBak = append(p.taskMapBak[:i], p.taskMapBak[i+1:]...)
			p.taskMapBakTS = append(p.taskMapBakTS[:i], p.taskMapBakTS[i+1:]...)
			i--
			N--

			// Roll back taskMap.
			p.taskMap[cur] = nil
		}
	}
	for _, child := range p.children {
		child.rollBackTaskMap(TS)
	}
}

func (p *baseLogicalPlan) getTask(prop *property.PhysicalProperty) task {
	key := prop.HashCode()
	return p.taskMap[string(key)]
}

func (p *baseLogicalPlan) storeTask(prop *property.PhysicalProperty, task task) {
	key := prop.HashCode()
	if p.ctx.GetSessionVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		// Empty string for useless change.
		TS := p.GetLogicalTS4TaskMap()
		p.taskMapBakTS = append(p.taskMapBakTS, TS)
		p.taskMapBak = append(p.taskMapBak, string(key))
	}
	p.taskMap[string(key)] = task
}

// HasMaxOneRow returns if the LogicalPlan will output at most one row.
func HasMaxOneRow(p LogicalPlan, childMaxOneRow []bool) bool {
	if len(childMaxOneRow) == 0 {
		// The reason why we use this check is that, this function
		// is used both in planner/core and planner/cascades.
		// In cascades planner, LogicalPlan may have no `children`.
		return false
	}
	switch x := p.(type) {
	case *LogicalLock, *LogicalLimit, *LogicalSort, *LogicalSelection,
		*LogicalApply, *LogicalProjection, *LogicalWindow, *LogicalAggregation:
		return childMaxOneRow[0]
	case *LogicalMaxOneRow:
		return true
	case *LogicalJoin:
		switch x.JoinType {
		case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
			return childMaxOneRow[0]
		default:
			return childMaxOneRow[0] && childMaxOneRow[1]
		}
	}
	return false
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *baseLogicalPlan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	childMaxOneRow := make([]bool, len(p.children))
	for i := range p.children {
		childMaxOneRow[i] = p.children[i].MaxOneRow()
	}
	p.maxOneRow = HasMaxOneRow(p.self, childMaxOneRow)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *logicalSchemaProducer) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)

	// default implementation for plans has only one child: proprgate child keys
	// multi-children plans are likely to have particular implementation.
	if len(childSchema) == 1 {
		for _, key := range childSchema[0].Keys {
			indices := selfSchema.ColumnsIndices(key)
			if indices == nil {
				continue
			}
			newKey := make([]*expression.Column, 0, len(key))
			for _, i := range indices {
				newKey = append(newKey, selfSchema.Columns[i])
			}
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
}

func newBasePlan(ctx sessionctx.Context, tp string, offset int) basePlan {
	ctx.GetSessionVars().PlanID++
	id := ctx.GetSessionVars().PlanID
	return basePlan{
		tp:          tp,
		id:          id,
		ctx:         ctx,
		blockOffset: offset,
	}
}

func newBaseLogicalPlan(ctx sessionctx.Context, tp string, self LogicalPlan, offset int) baseLogicalPlan {
	return baseLogicalPlan{
		taskMap:      make(map[string]task),
		taskMapBak:   make([]string, 0, 10),
		taskMapBakTS: make([]uint64, 0, 10),
		basePlan:     newBasePlan(ctx, tp, offset),
		self:         self,
	}
}

func newBasePhysicalPlan(ctx sessionctx.Context, tp string, self PhysicalPlan, offset int) basePhysicalPlan {
	return basePhysicalPlan{
		basePlan: newBasePlan(ctx, tp, offset),
		self:     self,
	}
}

func (p *baseLogicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *baseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column, opt *logicalOptimizeOp) error {
	if len(p.children) == 0 {
		return nil
	}
	return p.children[0].PruneColumns(parentUsedCols, opt)
}

// basePlan implements base Plan interface.
// Should be used as embedded struct in Plan implementations.
type basePlan struct {
	tp          string
	id          int
	ctx         sessionctx.Context
	stats       *property.StatsInfo
	blockOffset int
}

// OutputNames returns the outputting names of each column.
func (p *basePlan) OutputNames() types.NameSlice {
	return nil
}

func (p *basePlan) SetOutputNames(names types.NameSlice) {
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

// ExplainInfo implements Plan interface.
func (p *basePlan) ExplainInfo() string {
	return "N/A"
}

func (p *basePlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.ctx != nil && p.ctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.tp
		}
		return p.tp + "_" + strconv.Itoa(p.id)
	})
}

// TP implements Plan interface.
func (p *basePlan) TP() string {
	return p.tp
}

func (p *basePlan) SelectBlockOffset() int {
	return p.blockOffset
}

// Stats implements Plan Stats interface.
func (p *basePlan) Stats() *property.StatsInfo {
	return p.stats
}

// Schema implements Plan Schema interface.
func (p *baseLogicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

func (p *baseLogicalPlan) OutputNames() types.NameSlice {
	return p.children[0].OutputNames()
}

func (p *baseLogicalPlan) SetOutputNames(names types.NameSlice) {
	p.children[0].SetOutputNames(names)
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

// Context implements Plan Context interface.
func (p *basePlan) SCtx() sessionctx.Context {
	return p.ctx
}

// buildPlanTrace implements Plan
func (p *basePhysicalPlan) buildPlanTrace() *tracing.PlanTrace {
	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: p.TP(), ExplainInfo: p.self.ExplainInfo(), Cost: p.Cost()}
	for _, child := range p.Children() {
		planTrace.Children = append(planTrace.Children, child.buildPlanTrace())
	}
	return planTrace
}

// buildPlanTrace implements Plan
func (p *baseLogicalPlan) buildPlanTrace() *tracing.PlanTrace {
	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: p.TP(), ExplainInfo: p.self.ExplainInfo()}
	for _, child := range p.Children() {
		planTrace.Children = append(planTrace.Children, child.buildPlanTrace())
	}
	return planTrace
}

// buildPlanTrace implements Plan
func (p *basePlan) buildPlanTrace() *tracing.PlanTrace {
	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: p.TP()}
	return planTrace
}
