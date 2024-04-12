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

package operator

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/tracing"
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

	// ReplaceExprColumns replace all the column reference in the plan's expression node.
	ReplaceExprColumns(replace map[string]*expression.Column)

	SCtx() core.PlanContext

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
	GetPlanCostVer1(taskType property.TaskType, option *coreusage.PlanCostOption) (float64, error)

	// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost on model ver2.
	GetPlanCostVer2(taskType property.TaskType, option *coreusage.PlanCostOption) (coreusage.CostVer2, error)

	// Attach2Task makes the current physical plan as the father of task's physicalPlan and updates the cost of
	// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
	Attach2Task(...core.Task) core.Task

	// ToPB converts physical plan to tipb executor.
	ToPB(ctx *core.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error)

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
	AppendChildCandidate(op *coreusage.PhysicalOptimizeOp)

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
