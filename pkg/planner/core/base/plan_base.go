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
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tipb/go-tipb"
)

// PlanContext is the context for building plan.
type PlanContext = planctx.PlanContext

// BuildPBContext is the context for building `*tipb.Executor`.
type BuildPBContext = planctx.BuildPBContext

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

	// SetID sets the ID
	SetID(id int)

	// TP get the plan type.
	TP(...bool) string

	// Get the ID in explain statement
	ExplainID(isChildOfINL ...bool) fmt.Stringer

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

	// CloneForPlanCache clones this Plan for Plan Cache.
	// Compared with Clone, CloneForPlanCache doesn't deep clone every fields, fields with tag
	// `plan-cache-shallow-clone:"true"` are allowed to be shallow cloned.
	CloneForPlanCache(newCtx PlanContext) (cloned Plan, ok bool)
}

// PhysicalPlan is a tree of the physical operators.
type PhysicalPlan interface {
	Plan

	// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost on model ver1.
	GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error)

	// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost on model ver2.
	GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

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
	Clone(newCtx PlanContext) (PhysicalPlan, error)

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

// LogicalPlan is a tree of logical operators.
// We can do a lot of logical optimizations to it, like predicate push-down and column pruning.
type LogicalPlan interface {
	Plan
	base.HashEquals

	// HashCode encodes a LogicalPlan to fast compare whether a LogicalPlan equals to another.
	// We use a strict encode method here which ensures there is no conflict.
	HashCode() []byte

	// PredicatePushDown pushes down the predicates in the where/on/having clauses as deeply as possible.
	// It will accept a predicate that is an expression slice, and return the expressions that can't be pushed.
	// Because it might change the root if the having clause exists, we need to return a plan that represents a new root.
	PredicatePushDown([]expression.Expression) ([]expression.Expression, LogicalPlan, error)

	// PruneColumns prunes the unused columns, and return the new logical plan if changed, otherwise it's same.
	PruneColumns([]*expression.Column) (LogicalPlan, error)

	// BuildKeyInfo will collect the information of unique keys into schema.
	// Because this method is also used in cascades planner, we cannot use
	// things like `p.schema` or `p.children` inside it. We should use the `selfSchema`
	// and `childSchema` instead.
	BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema)

	// PushDownTopN will push down the topN or limit operator during logical optimization.
	// interface definition should depend on concrete implementation type.
	PushDownTopN(topN LogicalPlan) LogicalPlan

	// DeriveTopN derives an implicit TopN from a filter on row_number window function...
	DeriveTopN() LogicalPlan

	// PredicateSimplification consolidates different predcicates on a column and its equivalence classes.
	PredicateSimplification() LogicalPlan

	// ConstantPropagation generate new constant predicate according to column equivalence relation
	ConstantPropagation(parentPlan LogicalPlan, currentChildIdx int) (newRoot LogicalPlan)

	// PullUpConstantPredicates recursive find constant predicate, used for the constant propagation rule
	PullUpConstantPredicates() []expression.Expression

	// RecursiveDeriveStats derives statistic info between plans.
	RecursiveDeriveStats(colGroups [][]*expression.Column) (*property.StatsInfo, bool, error)

	// DeriveStats derives statistic info for current plan node given child stats.
	// We need selfSchema, childSchema here because it makes this method can be used in
	// cascades planner, where LogicalPlan might not record its children or schema.
	DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error)

	// ExtractColGroups extracts column groups from child operator whose DNVs are required by the current operator.
	// For example, if current operator is LogicalAggregation of `Group By a, b`, we indicate the child operators to maintain
	// and propagate the NDV info of column group (a, b), to improve the row count estimation of current LogicalAggregation.
	// The parameter colGroups are column groups required by upper operators, besides from the column groups derived from
	// current operator, we should pass down parent colGroups to child operator as many as possible.
	ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column

	// PreparePossibleProperties is only used for join and aggregation. Like group by a,b,c, all permutation of (a,b,c) is
	// valid, but the ordered indices in leaf plan is limited. So we can get all possible order properties by a pre-walking.
	PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column

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
	// Deprecated: don't depend subtree based push check, see CanSelfBeingPushedToCopImpl based op-self push check.
	CanPushToCop(store kv.StoreType) bool

	// ExtractFD derive the FDSet from the tree bottom up.
	ExtractFD() *fd.FDSet

	// GetBaseLogicalPlan return the baseLogicalPlan inside each logical plan.
	GetBaseLogicalPlan() LogicalPlan

	// ConvertOuterToInnerJoin converts outer joins if the matching rows are filtered.
	ConvertOuterToInnerJoin(predicates []expression.Expression) LogicalPlan

	// SetPlanIDsHash set sub operator tree's ids hash64
	SetPlanIDsHash(uint64)

	// GetPlanIDsHash set sub operator tree's ids hash64
	GetPlanIDsHash() uint64

	// GetWrappedLogicalPlan return the wrapped logical plan inside a group expression.
	// For logicalPlan implementation, it just returns itself as well.
	GetWrappedLogicalPlan() LogicalPlan

	// GetChildStatsAndSchema gets the stats and schema of the first child.
	GetChildStatsAndSchema() (*property.StatsInfo, *expression.Schema)

	// GetJoinChildStatsAndSchema gets the stats and schema of both children.
	GetJoinChildStatsAndSchema() (stats0, stats1 *property.StatsInfo, schema0, schema1 *expression.Schema)
}

// GroupExpression is the interface for group expression.
type GroupExpression interface {
	LogicalPlan
	// IsExplored return whether this gE has explored rule i.
	IsExplored(i uint) bool
	// InputsLen returns the length of inputs.
	InputsLen() int
	// GetInputSchema returns the input logical's schema by index.
	GetInputSchema(idx int) *expression.Schema
}

// GetGEAndLogicalOp is get the possible group expression and logical operator from common super pointer.
func GetGEAndLogicalOp[T LogicalPlan](super LogicalPlan) (ge GroupExpression, logicalOp T) {
	switch x := super.(type) {
	case T:
		// previously, wrapped BaseLogicalPlan serve as the common part, so we need to use self()
		// to downcast as the every specific logical operator.
		logicalOp = x
	case GroupExpression:
		// currently, since GroupExpression wrap a LogicalPlan as its first field, we GE itself is
		// naturally can be referred as a LogicalPlan, and we need to use GetWrappedLogicalPlan to
		// get the specific logical operator inside.
		ge = x
		logicalOp = ge.GetWrappedLogicalPlan().(T)
	}
	return ge, logicalOp
}

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, SemiJoin, AntiJoin.
type JoinType int

const (
	// InnerJoin means inner join.
	InnerJoin JoinType = iota
	// LeftOuterJoin means left join.
	LeftOuterJoin
	// RightOuterJoin means right join.
	RightOuterJoin
	// SemiJoin means if row a in table A matches some rows in B, just output a.
	SemiJoin
	// AntiSemiJoin means if row a in table A does not match any row in B, then output a.
	AntiSemiJoin
	// LeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, true), otherwise, output (a, false).
	LeftOuterSemiJoin
	// AntiLeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, false), otherwise, output (a, true).
	AntiLeftOuterSemiJoin
)

// IsOuterJoin returns if this joiner is an outer joiner
func (tp JoinType) IsOuterJoin() bool {
	return tp == LeftOuterJoin || tp == RightOuterJoin ||
		tp == LeftOuterSemiJoin || tp == AntiLeftOuterSemiJoin
}

// IsSemiJoin returns if this joiner is a semi/anti-semi joiner
func (tp JoinType) IsSemiJoin() bool {
	return tp == SemiJoin || tp == AntiSemiJoin ||
		tp == LeftOuterSemiJoin || tp == AntiLeftOuterSemiJoin
}

// IsInnerJoin returns if this joiner is a inner joiner
func (tp JoinType) IsInnerJoin() bool {
	return tp == InnerJoin
}

func (tp JoinType) String() string {
	switch tp {
	case InnerJoin:
		return "inner join"
	case LeftOuterJoin:
		return "left outer join"
	case RightOuterJoin:
		return "right outer join"
	case SemiJoin:
		return "semi join"
	case AntiSemiJoin:
		return "anti semi join"
	case LeftOuterSemiJoin:
		return "left outer semi join"
	case AntiLeftOuterSemiJoin:
		return "anti left outer semi join"
	}
	return "unsupported join type"
}

// PhysicalJoin provides some common methods for join operators.
// Note that PhysicalApply is deliberately excluded from this interface.
type PhysicalJoin interface {
	PhysicalPlan
	PhysicalJoinImplement()
	GetInnerChildIdx() int
	GetJoinType() JoinType
}
