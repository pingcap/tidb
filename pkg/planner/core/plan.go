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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/core/internal/base"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
)

// PlanContext is the context for building plan.
type PlanContext = context.PlanContext

// AsSctx converts PlanContext to sessionctx.Context.
func AsSctx(pctx PlanContext) (sessionctx.Context, error) {
	sctx, ok := pctx.(sessionctx.Context)
	if !ok {
		return nil, errors.New("the current PlanContext cannot be converted to sessionctx.Context")
	}
	return sctx, nil
}

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

	// ReplaceExprColumns replace all the column reference in the plan's expression node.
	ReplaceExprColumns(replace map[string]*expression.Column)

	SCtx() PlanContext

	// StatsInfo will return the property.StatsInfo for this plan.
	StatsInfo() *property.StatsInfo

	// OutputNames returns the outputting names of each column.
	OutputNames() types.NameSlice

	// SetOutputNames sets the outputting name by the given slice.
	SetOutputNames(names types.NameSlice)

	// QueryBlockOffset is query block offset.
	// For example, in query
	//		`select /*+ use_index(@sel_2 t2, a) */ * from t1, (select a*2 as b from t2) tx where a>b`
	// the hint should be applied on the sub-query, whose query block is 2.
	QueryBlockOffset() int

	BuildPlanTrace() *tracing.PlanTrace
}

func enforceProperty(p *property.PhysicalProperty, tsk task, ctx PlanContext) task {
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
	// when task is double cop task warping a index merge reader, tsk.plan() may be nil when indexPlanFinished is marked
	// as false, while the real plan is in idxMergePartPlans. tsk.plan()==nil is not right here.
	if p.IsSortItemEmpty() || tsk.invalid() {
		return tsk
	}
	if p.TaskTp != property.MppTaskType {
		tsk = tsk.convertToRootTask(ctx)
	}
	sortReqProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, SortItems: p.SortItems, ExpectedCnt: math.MaxFloat64}
	sort := PhysicalSort{
		ByItems:       make([]*util.ByItems, 0, len(p.SortItems)),
		IsPartialSort: p.IsSortItemAllForPartition(),
	}.Init(ctx, tsk.plan().StatsInfo(), tsk.plan().QueryBlockOffset(), sortReqProp)
	for _, col := range p.SortItems {
		sort.ByItems = append(sort.ByItems, &util.ByItems{Expr: col.Col, Desc: col.Desc})
	}
	return sort.attach2Task(tsk)
}

// optimizeByShuffle insert `PhysicalShuffle` to optimize performance by running in a parallel manner.
func optimizeByShuffle(tsk task, ctx PlanContext) task {
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

func optimizeByShuffle4Window(pp *PhysicalWindow, ctx PlanContext) *PhysicalShuffle {
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
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(partitionBy, dataSource.Schema(), dataSource.StatsInfo())
	if ndv <= 1 {
		return nil
	}
	concurrency = min(concurrency, int(ndv))

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
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
	return shuffle
}

func optimizeByShuffle4StreamAgg(pp *PhysicalStreamAgg, ctx PlanContext) *PhysicalShuffle {
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
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(partitionBy, dataSource.Schema(), dataSource.StatsInfo())
	if ndv <= 1 {
		return nil
	}
	concurrency = min(concurrency, int(ndv))

	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        []PhysicalPlan{tail},
		DataSources:  []PhysicalPlan{dataSource},
		SplitterType: PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{util.CloneExprs(pp.GroupByItems)},
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
	return shuffle
}

func optimizeByShuffle4MergeJoin(pp *PhysicalMergeJoin, ctx PlanContext) *PhysicalShuffle {
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
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
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
	PredicatePushDown([]expression.Expression, *util.LogicalOptimizeOp) ([]expression.Expression, LogicalPlan)

	// PruneColumns prunes the unused columns, and return the new logical plan if changed, otherwise it's same.
	PruneColumns([]*expression.Column, *util.LogicalOptimizeOp) (LogicalPlan, error)

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
	pushDownTopN(topN *LogicalTopN, opt *util.LogicalOptimizeOp) LogicalPlan

	// deriveTopN derives an implicit TopN from a filter on row_number window function..
	deriveTopN(opt *util.LogicalOptimizeOp) LogicalPlan

	// predicateSimplification consolidates different predcicates on a column and its equivalence classes.
	predicateSimplification(opt *util.LogicalOptimizeOp) LogicalPlan

	// constantPropagation generate new constant predicate according to column equivalence relation
	constantPropagation(parentPlan LogicalPlan, currentChildIdx int, opt *util.LogicalOptimizeOp) (newRoot LogicalPlan)

	// pullUpConstantPredicates recursive find constant predicate, used for the constant propagation rule
	pullUpConstantPredicates() []expression.Expression

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

	// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost on model ver1.
	getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error)

	// getPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost on model ver2.
	getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (costVer2, error)

	// attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
	// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
	attach2Task(...task) task

	// ToPB converts physical plan to tipb executor.
	ToPB(ctx PlanContext, storeType kv.StoreType) (*tipb.Executor, error)

	// GetChildReqProps gets the required property by child index.
	GetChildReqProps(idx int) *property.PhysicalProperty

	// StatsCount returns the count of property.StatsInfo for this plan.
	StatsCount() float64

	// ExtractCorrelatedCols extracts correlated columns inside the PhysicalPlan.
	ExtractCorrelatedCols() []*expression.CorrelatedColumn

	// Children get all the children.
	Children() []PhysicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...PhysicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child PhysicalPlan)

	// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
	ResolveIndices() error

	// StatsInfo returns the StatsInfo of the plan.
	StatsInfo() *property.StatsInfo

	// SetStats sets basePlan.stats inside the basePhysicalPlan.
	SetStats(s *property.StatsInfo)

	// ExplainNormalizedInfo returns operator normalized information for generating digest.
	ExplainNormalizedInfo() string

	// Clone clones this physical plan.
	Clone() (PhysicalPlan, error)

	// appendChildCandidate append child physicalPlan into tracer in order to track each child physicalPlan which can't
	// be tracked during findBestTask or enumeratePhysicalPlans4Task
	appendChildCandidate(op *physicalOptimizeOp)

	// MemoryUsage return the memory usage of PhysicalPlan
	MemoryUsage() int64

	// Below three methods help to handle the inconsistency between row count in the StatsInfo and the recorded
	// actual row count.
	// For the children in the inner side (probe side) of Index Join and Apply, the row count in the StatsInfo
	// means the estimated row count for a single "probe", but the recorded actual row count is the total row
	// count for all "probes".
	// To handle this inconsistency without breaking anything else, we added a field `probeParents` of
	// type `[]PhysicalPlan` into all PhysicalPlan to record all operators that are (indirect or direct) parents
	// of this PhysicalPlan and will cause this inconsistency.
	// Using this information, we can convert the row count between the "single probe" row count and "all probes"
	// row count freely.

	// setProbeParents sets the above stated `probeParents` field.
	setProbeParents([]PhysicalPlan)
	// getEstRowCountForDisplay uses the "single probe" row count in StatsInfo and the probeParents to calculate
	// the "all probe" row count.
	// All places that display the row count for a PhysicalPlan are expected to use this method.
	getEstRowCountForDisplay() float64
	// getEstRowCountForDisplay uses the runtime stats and the probeParents to calculate the actual "probe" count.
	getActualProbeCnt(*execdetails.RuntimeStatsColl) int64
}

// NewDefaultPlanCostOption returns PlanCostOption
func NewDefaultPlanCostOption() *PlanCostOption {
	return &PlanCostOption{}
}

// PlanCostOption indicates option during GetPlanCost
type PlanCostOption struct {
	CostFlag uint64
	tracer   *physicalOptimizeOp
}

// WithCostFlag set costflag
func (op *PlanCostOption) WithCostFlag(flag uint64) *PlanCostOption {
	if op == nil {
		return nil
	}
	op.CostFlag = flag
	return op
}

// WithOptimizeTracer set tracer
func (op *PlanCostOption) WithOptimizeTracer(v *physicalOptimizeOp) *PlanCostOption {
	if op == nil {
		return nil
	}
	op.tracer = v
	if v != nil && v.tracer != nil {
		op.CostFlag |= CostFlagTrace
	}
	return op
}

type baseLogicalPlan struct {
	base.Plan

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
func (*baseLogicalPlan) ExplainInfo() string {
	return ""
}

func getEstimatedProbeCntFromProbeParents(probeParents []PhysicalPlan) float64 {
	res := float64(1)
	for _, pp := range probeParents {
		switch pp.(type) {
		case *PhysicalApply, *PhysicalIndexHashJoin, *PhysicalIndexMergeJoin, *PhysicalIndexJoin:
			if join, ok := pp.(interface{ getInnerChildIdx() int }); ok {
				outer := pp.Children()[1-join.getInnerChildIdx()]
				res *= outer.StatsInfo().RowCount
			}
		}
	}
	return res
}

func getActualProbeCntFromProbeParents(pps []PhysicalPlan, statsColl *execdetails.RuntimeStatsColl) int64 {
	res := int64(1)
	for _, pp := range pps {
		switch pp.(type) {
		case *PhysicalApply, *PhysicalIndexHashJoin, *PhysicalIndexMergeJoin, *PhysicalIndexJoin:
			if join, ok := pp.(interface{ getInnerChildIdx() int }); ok {
				outerChildID := pp.Children()[1-join.getInnerChildIdx()].ID()
				actRows := int64(1)
				if statsColl.ExistsRootStats(outerChildID) {
					actRows = statsColl.GetRootStats(outerChildID).GetActRows()
				}
				if statsColl.ExistsCopStats(outerChildID) {
					actRows = statsColl.GetCopStats(outerChildID).GetActRows()
				}
				// TODO: For PhysicalApply, we need to consider cache hit ratio in JoinRuntimeStats and use actRows/(1-ratio) here.
				res *= actRows
			}
		}
	}
	return res
}

type basePhysicalPlan struct {
	base.Plan

	childrenReqProps []*property.PhysicalProperty
	self             PhysicalPlan
	children         []PhysicalPlan

	// used by the new cost interface
	planCostInit bool
	planCost     float64
	planCostVer2 costVer2

	// probeParents records the IndexJoins and Applys with this operator in their inner children.
	// Please see comments in PhysicalPlan for details.
	probeParents []PhysicalPlan

	// Only for MPP. If TiFlashFineGrainedShuffleStreamCount > 0:
	// 1. For ExchangeSender, means its output will be partitioned by hash key.
	// 2. For ExchangeReceiver/Window/Sort, means its input is already partitioned.
	TiFlashFineGrainedShuffleStreamCount uint64
}

func (p *basePhysicalPlan) cloneWithSelf(newSelf PhysicalPlan) (*basePhysicalPlan, error) {
	base := &basePhysicalPlan{
		Plan:                                 p.Plan,
		self:                                 newSelf,
		TiFlashFineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		probeParents:                         p.probeParents,
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
func (*basePhysicalPlan) ExplainInfo() string {
	return ""
}

// ExplainNormalizedInfo implements PhysicalPlan interface.
func (*basePhysicalPlan) ExplainNormalizedInfo() string {
	return ""
}

func (p *basePhysicalPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return p.childrenReqProps[idx]
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (*basePhysicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// MemoryUsage return the memory usage of basePhysicalPlan
func (p *basePhysicalPlan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.Plan.MemoryUsage() + size.SizeOfSlice + int64(cap(p.childrenReqProps))*size.SizeOfPointer +
		size.SizeOfSlice + int64(cap(p.children)+1)*size.SizeOfInterface + size.SizeOfFloat64 +
		size.SizeOfUint64 + size.SizeOfBool

	for _, prop := range p.childrenReqProps {
		sum += prop.MemoryUsage()
	}
	for _, plan := range p.children {
		sum += plan.MemoryUsage()
	}
	return
}

func (p *basePhysicalPlan) getEstRowCountForDisplay() float64 {
	if p == nil {
		return 0
	}
	return p.StatsInfo().RowCount * getEstimatedProbeCntFromProbeParents(p.probeParents)
}

func (p *basePhysicalPlan) getActualProbeCnt(statsColl *execdetails.RuntimeStatsColl) int64 {
	if p == nil {
		return 1
	}
	return getActualProbeCntFromProbeParents(p.probeParents, statsColl)
}

func (p *basePhysicalPlan) setProbeParents(probeParents []PhysicalPlan) {
	p.probeParents = probeParents
}

// GetLogicalTS4TaskMap get the logical TimeStamp now to help rollback the TaskMap changes after that.
func (p *baseLogicalPlan) GetLogicalTS4TaskMap() uint64 {
	p.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS++
	return p.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS
}

func (p *baseLogicalPlan) rollBackTaskMap(ts uint64) {
	if !p.SCtx().GetSessionVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		return
	}
	if len(p.taskMapBak) > 0 {
		// Rollback all the logs with TimeStamp TS.
		n := len(p.taskMapBak)
		for i := 0; i < n; i++ {
			cur := p.taskMapBak[i]
			if p.taskMapBakTS[i] < ts {
				continue
			}

			// Remove the i_th log.
			p.taskMapBak = append(p.taskMapBak[:i], p.taskMapBak[i+1:]...)
			p.taskMapBakTS = append(p.taskMapBakTS[:i], p.taskMapBakTS[i+1:]...)
			i--
			n--

			// Roll back taskMap.
			p.taskMap[cur] = nil
		}
	}
	for _, child := range p.children {
		child.rollBackTaskMap(ts)
	}
}

func (p *baseLogicalPlan) getTask(prop *property.PhysicalProperty) task {
	key := prop.HashCode()
	return p.taskMap[string(key)]
}

func (p *baseLogicalPlan) storeTask(prop *property.PhysicalProperty, task task) {
	key := prop.HashCode()
	if p.SCtx().GetSessionVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		// Empty string for useless change.
		ts := p.GetLogicalTS4TaskMap()
		p.taskMapBakTS = append(p.taskMapBakTS, ts)
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
func (p *baseLogicalPlan) BuildKeyInfo(_ *expression.Schema, _ []*expression.Schema) {
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

func newBaseLogicalPlan(ctx PlanContext, tp string, self LogicalPlan, qbOffset int) baseLogicalPlan {
	return baseLogicalPlan{
		taskMap:      make(map[string]task),
		taskMapBak:   make([]string, 0, 10),
		taskMapBakTS: make([]uint64, 0, 10),
		Plan:         base.NewBasePlan(ctx, tp, qbOffset),
		self:         self,
	}
}

func newBasePhysicalPlan(ctx PlanContext, tp string, self PhysicalPlan, offset int) basePhysicalPlan {
	return basePhysicalPlan{
		Plan: base.NewBasePlan(ctx, tp, offset),
		self: self,
	}
}

func (*baseLogicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// PruneColumns implements LogicalPlan interface.
func (p *baseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column, opt *util.LogicalOptimizeOp) (LogicalPlan, error) {
	if len(p.children) == 0 {
		return p.self, nil
	}
	var err error
	p.children[0], err = p.children[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p.self, nil
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

// BuildPlanTrace implements Plan
func (p *basePhysicalPlan) BuildPlanTrace() *tracing.PlanTrace {
	tp := ""
	info := ""
	if p.self != nil {
		tp = p.self.TP()
		info = p.self.ExplainInfo()
	}

	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: tp, ExplainInfo: info}
	for _, child := range p.Children() {
		planTrace.Children = append(planTrace.Children, child.BuildPlanTrace())
	}
	return planTrace
}

// BuildPlanTrace implements Plan
func (p *baseLogicalPlan) BuildPlanTrace() *tracing.PlanTrace {
	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: p.TP(), ExplainInfo: p.self.ExplainInfo()}
	for _, child := range p.Children() {
		planTrace.Children = append(planTrace.Children, child.BuildPlanTrace())
	}
	return planTrace
}

func (p *basePhysicalPlan) appendChildCandidate(op *physicalOptimizeOp) {
	if len(p.Children()) < 1 {
		return
	}
	childrenID := make([]int, 0)
	for _, child := range p.Children() {
		childCandidate := &tracing.CandidatePlanTrace{
			PlanTrace: &tracing.PlanTrace{TP: child.TP(), ID: child.ID(),
				ExplainInfo: child.ExplainInfo()},
		}
		op.tracer.AppendCandidate(childCandidate)
		child.appendChildCandidate(op)
		childrenID = append(childrenID, child.ID())
	}
	op.tracer.Candidates[p.ID()].PlanTrace.AppendChildrenID(childrenID...)
}
