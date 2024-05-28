// Copyright 2024 PingCAP, Inc.
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

package base

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/context"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
)

// PlanContext is the context for building plan.
type PlanContext = context.PlanContext

// BuildPBContext is the context for building `*tipb.Executor`.
type BuildPBContext = context.BuildPBContext

// Note: appending the new adding method to the last, for the convenience of easy
// locating in other implementor from other package.

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

// PhysicalPlan is a tree of the physical operators.
type PhysicalPlan interface {
	Plan

	// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost on model ver1.
	GetPlanCostVer1(taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error)

	// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost on model ver2.
	GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error)

	// Attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
	// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
	Attach2Task(...Task) Task

	// ToPB converts physical plan to tipb executor.
	ToPB(ctx *BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error)

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

	// AppendChildCandidate append child physicalPlan into tracer in order to track each child physicalPlan which can't
	// be tracked during findBestTask or enumeratePhysicalPlans4Task
	AppendChildCandidate(op *optimizetrace.PhysicalOptimizeOp)

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

	// SetProbeParents sets the above stated `probeParents` field.
	SetProbeParents([]PhysicalPlan)
	// GetEstRowCountForDisplay uses the "single probe" row count in StatsInfo and the probeParents to calculate
	// the "all probe" row count.
	// All places that display the row count for a PhysicalPlan are expected to use this method.
	GetEstRowCountForDisplay() float64
	// GetActualProbeCnt uses the runtime stats and the probeParents to calculate the actual "probe" count.
	GetActualProbeCnt(*execdetails.RuntimeStatsColl) int64
}

// PlanCounterTp is used in hint nth_plan() to indicate which plan to use.
type PlanCounterTp int64

// Dec minus PlanCounterTp value by x.
func (c *PlanCounterTp) Dec(x int64) {
	if *c <= 0 {
		return
	}
	*c = PlanCounterTp(int64(*c) - x)
	if *c < 0 {
		*c = 0
	}
}

// Empty indicates whether the PlanCounterTp is clear now.
func (c *PlanCounterTp) Empty() bool {
	return *c == 0
}

// IsForce indicates whether to force a plan.
func (c *PlanCounterTp) IsForce() bool {
	return *c != -1
}

// LogicalPlan is a tree of logical operators.
// We can do a lot of logical optimizations to it, like predicate push-down and column pruning.
type LogicalPlan interface {
	Plan

	// HashCode encodes a LogicalPlan to fast compare whether a LogicalPlan equals to another.
	// We use a strict encode method here which ensures there is no conflict.
	HashCode() []byte

	// PredicatePushDown pushes down the predicates in the where/on/having clauses as deeply as possible.
	// It will accept a predicate that is an expression slice, and return the expressions that can't be pushed.
	// Because it might change the root if the having clause exists, we need to return a plan that represents a new root.
	PredicatePushDown([]expression.Expression, *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, LogicalPlan)

	// PruneColumns prunes the unused columns, and return the new logical plan if changed, otherwise it's same.
	PruneColumns([]*expression.Column, *optimizetrace.LogicalOptimizeOp) (LogicalPlan, error)

	// FindBestTask converts the logical plan to the physical plan. It's a new interface.
	// It is called recursively from the parent to the children to create the result physical plan.
	// Some logical plans will convert the children to the physical plans in different ways, and return the one
	// With the lowest cost and how many plans are found in this function.
	// planCounter is a counter for planner to force a plan.
	// If planCounter > 0, the clock_th plan generated in this function will be returned.
	// If planCounter = 0, the plan generated in this function will not be considered.
	// If planCounter = -1, then we will not force plan.
	FindBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, op *optimizetrace.PhysicalOptimizeOp) (Task, int64, error)

	// BuildKeyInfo will collect the information of unique keys into schema.
	// Because this method is also used in cascades planner, we cannot use
	// things like `p.schema` or `p.children` inside it. We should use the `selfSchema`
	// and `childSchema` instead.
	BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema)

	// PushDownTopN will push down the topN or limit operator during logical optimization.
	// interface definition should depend on concrete implementation type.
	PushDownTopN(topN LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) LogicalPlan

	// DeriveTopN derives an implicit TopN from a filter on row_number window function...
	DeriveTopN(opt *optimizetrace.LogicalOptimizeOp) LogicalPlan

	// PredicateSimplification consolidates different predcicates on a column and its equivalence classes.
	PredicateSimplification(opt *optimizetrace.LogicalOptimizeOp) LogicalPlan

	// ConstantPropagation generate new constant predicate according to column equivalence relation
	ConstantPropagation(parentPlan LogicalPlan, currentChildIdx int, opt *optimizetrace.LogicalOptimizeOp) (newRoot LogicalPlan)

	// PullUpConstantPredicates recursive find constant predicate, used for the constant propagation rule
	PullUpConstantPredicates() []expression.Expression

	// RecursiveDeriveStats derives statistic info between plans.
	RecursiveDeriveStats(colGroups [][]*expression.Column) (*property.StatsInfo, error)

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

	// ExhaustPhysicalPlans generates all possible plans that can match the required property.
	// It will return:
	// 1. All possible plans that can match the required property.
	// 2. Whether the SQL hint can work. Return true if there is no hint.
	ExhaustPhysicalPlans(*property.PhysicalProperty) (physicalPlans []PhysicalPlan, hintCanWork bool, err error)

	// ExtractCorrelatedCols extracts correlated columns inside the LogicalPlan.
	ExtractCorrelatedCols() []*expression.CorrelatedColumn

	// MaxOneRow means whether this operator only returns max one row.
	MaxOneRow() bool

	// Children Get all the children.
	Children() []LogicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...LogicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child LogicalPlan)

	// RollBackTaskMap roll back all taskMap's logs after TimeStamp TS.
	RollBackTaskMap(TS uint64)

	// CanPushToCop check if we might push this plan to a specific store.
	CanPushToCop(store kv.StoreType) bool

	// ExtractFD derive the FDSet from the tree bottom up.
	ExtractFD() *fd.FDSet

	// GetBaseLogicalPlan return the baseLogicalPlan inside each logical plan.
	GetBaseLogicalPlan() LogicalPlan

	// ConvertOuterToInnerJoin converts outer joins if the matching rows are filtered.
	ConvertOuterToInnerJoin(predicates []expression.Expression) LogicalPlan
}
