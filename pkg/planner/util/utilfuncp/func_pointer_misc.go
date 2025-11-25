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

package utilfuncp

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hint"
)

// this file is used for passing function pointer at init(){} to avoid some import cycles.

// FindBestTask4BaseLogicalPlan will be called by baseLogicalPlan in logicalOp pkg.
// The logic inside covers Task, Property, LogicalOp and PhysicalOp, so it doesn't belong to logicalOp pkg.
// It should be kept in core pkg.
// todo: arenatlx, For clear division, we should remove Logical FindBestTask interface. Let core pkg to
// guide itself by receive logical tree.
var FindBestTask4BaseLogicalPlan func(p base.LogicalPlan,
	prop *property.PhysicalProperty) (bestTask base.Task, err error)

// FindBestTask4LogicalDataSource will be called by LogicalDataSource in logicalOp pkg.
var FindBestTask4LogicalDataSource func(lp base.LogicalPlan,
	prop *property.PhysicalProperty) (t base.Task, err error)

// ExhaustPhysicalPlans4LogicalJoin will be called by LogicalJoin in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalJoin func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalApply will be called by LogicalApply in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalApply func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ****************************************** stats related **********************************************

// DeriveStats4DataSource will be called by LogicalDataSource in logicalOp pkg.
var DeriveStats4DataSource func(lp base.LogicalPlan) (*property.StatsInfo, bool, error)

// DeriveStats4LogicalIndexScan will be called by LogicalIndexScan in logicalOp pkg.
var DeriveStats4LogicalIndexScan func(lp base.LogicalPlan, selfSchema *expression.Schema) (*property.StatsInfo,
	bool, error)

// DeriveStats4LogicalTableScan will be called by LogicalTableScan in logicalOp pkg.
var DeriveStats4LogicalTableScan func(lp base.LogicalPlan) (_ *property.StatsInfo, _ bool, err error)

// AddPrefix4ShardIndexes will be called by LogicalSelection in logicalOp pkg.
var AddPrefix4ShardIndexes func(lp base.LogicalPlan, sc base.PlanContext,
	conds []expression.Expression) []expression.Expression

// *************************************** physical op related *******************************************

// GetEstimatedProbeCntFromProbeParents will be called by BasePhysicalPlan in physicalOp pkg.
var GetEstimatedProbeCntFromProbeParents func(probeParents []base.PhysicalPlan) float64

// GetActualProbeCntFromProbeParents will be called by BasePhysicalPlan in physicalOp pkg.
var GetActualProbeCntFromProbeParents func(pps []base.PhysicalPlan, statsColl *execdetails.RuntimeStatsColl) int64

// GetPlanCost export the getPlanCost from core pkg for cascades usage.
// todo: remove this three func pointer when physical op are all migrated to physicalop pkg.
var GetPlanCost func(base.PhysicalPlan, property.TaskType, *costusage.PlanCostOption) (float64, error)

// Attach2Task4PhysicalSort will be called by PhysicalSort in physicalOp pkg.
var Attach2Task4PhysicalSort func(p base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalSort will be called by PhysicalSort in physicalOp pkg.
var GetCost4PhysicalSort func(p base.PhysicalPlan, count float64, schema *expression.Schema) float64

// GetPlanCostVer14PhysicalSort will be called by PhysicalSort in physicalOp pkg.
var GetPlanCostVer14PhysicalSort func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalSort represents the cost of a physical sort operation in version 2.
var GetPlanCostVer24PhysicalSort func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4NominalSort will be called by NominalSort in physicalOp pkg.
var Attach2Task4NominalSort func(base.PhysicalPlan, ...base.Task) base.Task

// Attach2Task4PhysicalUnionAll will be called by PhysicalUnionAll in physicalOp pkg.
var Attach2Task4PhysicalUnionAll func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalUnionAll will be called by PhysicalUnionAll in physicalOp pkg.
var GetPlanCostVer14PhysicalUnionAll func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalUnionAll will be called by PhysicalUnionAll in physicalOp pkg.
var GetPlanCostVer24PhysicalUnionAll func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error)

// GetPlanCostVer1PhysicalExchangeReceiver will be called by PhysicalExchangeReceiver in physicalOp pkg.
var GetPlanCostVer1PhysicalExchangeReceiver func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer2PhysicalExchangeReceiver will be called by PhysicalExchangeReceiver in physicalOp pkg.
var GetPlanCostVer2PhysicalExchangeReceiver func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error)

// ResolveIndices4PhysicalLimit will be called by PhysicalLimit in physicalOp pkg.
var ResolveIndices4PhysicalLimit func(pp base.PhysicalPlan) (err error)

// Attach2Task4PhysicalLimit will be called by PhysicalLimit in physicalOp pkg.
var Attach2Task4PhysicalLimit func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalTopN will be called by PhysicalLimit in physicalOp pkg.
var GetPlanCostVer14PhysicalTopN func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalTopN will be called by PhysicalLimit in physicalOp pkg.
var GetPlanCostVer24PhysicalTopN func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalTopN will be called by PhysicalTopN in physicalOp pkg.
var Attach2Task4PhysicalTopN func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// ResolveIndices4PhysicalTopN will be called by PhysicalTopN in physicalOp pkg.
var ResolveIndices4PhysicalTopN func(pp base.PhysicalPlan) (err error)

// Attach2Task4PhysicalExpand will be called by PhysicalExpand in physicalOp pkg.
var Attach2Task4PhysicalExpand func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// ResolveIndices4PhysicalSelection will be called by PhysicalSelection in physicalOp pkg.
var ResolveIndices4PhysicalSelection func(pp base.PhysicalPlan) (err error)

// Attach2Task4PhysicalSelection will be called by PhysicalSelection in physicalOp pkg.
var Attach2Task4PhysicalSelection func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalSelection will be called by PhysicalSelection in physicalOp pkg.
var GetPlanCostVer14PhysicalSelection func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalSelection will be called by PhysicalSelection in physicalOp pkg.
var GetPlanCostVer24PhysicalSelection func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalUnionScan will be called by PhysicalUnionScan in physicalOp pkg.
var Attach2Task4PhysicalUnionScan func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// ResolveIndices4PhysicalUnionScan will be called by PhysicalUnionScan in physicalOp pkg.
var ResolveIndices4PhysicalUnionScan func(pp base.PhysicalPlan) (err error)

// ResolveIndices4PhysicalProjection will be called by PhysicalProjection in physicalOp pkg.
var ResolveIndices4PhysicalProjection func(pp base.PhysicalPlan) (err error)

// GetCost4PhysicalProjection will be called by PhysicalProjection in physicalOp pkg.
var GetCost4PhysicalProjection func(pp base.PhysicalPlan, count float64) float64

// GetPlanCostVer14PhysicalProjection will be called by PhysicalProjection in physicalOp pkg.
var GetPlanCostVer14PhysicalProjection func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalProjection will be called by PhysicalProjection in physicalOp pkg.
var GetPlanCostVer24PhysicalProjection func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalProjection will be called by PhysicalProjection in physicalOp pkg.
var Attach2Task4PhysicalProjection func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalIndexJoin will be called by PhysicalIndexJoin in physicalOp pkg.
var GetCost4PhysicalIndexJoin func(pp base.PhysicalPlan,
	outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64

// GetPlanCostVer14PhysicalIndexJoin calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalIndexJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetIndexJoinCostVer24PhysicalIndexJoin will be called by PhysicalIndexJoin in physicalOp pkg.
var GetIndexJoinCostVer24PhysicalIndexJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, indexJoinType int) (costusage.CostVer2, error)

// Attach2Task4PhysicalIndexJoin will be called by PhysicalIndexJoin in physicalOp pkg.
var Attach2Task4PhysicalIndexJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// Attach2Task4PhysicalWindow will be called by PhysicalWindow in physicalOp pkg.
var Attach2Task4PhysicalWindow func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalMergeJoin computes cost of merge join operator itself.
var GetCost4PhysicalMergeJoin func(pp base.PhysicalPlan, lCnt, rCnt float64, costFlag uint64) float64

// Attach2Task4PhysicalMergeJoin will be called by PhysicalMergeJoin in physicalOp pkg.
var Attach2Task4PhysicalMergeJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalMergeJoin calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalMergeJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalMergeJoin returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
var GetPlanCostVer24PhysicalMergeJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error)

// GetPlanCostVer14PhysicalIndexScan calculates the cost of the plan if it has not been calculated yet
var GetPlanCostVer14PhysicalIndexScan func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalIndexScan returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
var GetPlanCostVer24PhysicalIndexScan func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error)

// GetPlanCostVer14PhysicalTableScan calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalTableScan func(pp base.PhysicalPlan,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalTableScan returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
var GetPlanCostVer24PhysicalTableScan func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// GetPlanCostVer14PhysicalIndexReader calculates the cost of the plan if it has not been calculated yet
var GetPlanCostVer14PhysicalIndexReader func(pp base.PhysicalPlan, _ property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalIndexReader returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
var GetPlanCostVer24PhysicalIndexReader func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error)

// GetCost4PhysicalHashJoin computes cost of hash join operator itself.
var GetCost4PhysicalHashJoin func(pp base.PhysicalPlan, lCnt, rCnt float64, costFlag uint64) float64

// GetPlanCostVer14PhysicalHashJoin calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalHashJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// Attach2Task4PhysicalHashJoin implements PhysicalPlan interface for PhysicalHashJoin.
var Attach2Task4PhysicalHashJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalIndexMergeReader calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalIndexMergeReader func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalIndexMergeReader returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
var GetPlanCostVer24PhysicalIndexMergeReader func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error)

// GetPlanCostVer24PhysicalHashJoin returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
var GetPlanCostVer24PhysicalHashJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (costusage.CostVer2, error)

// GetCost4PhysicalIndexHashJoin computes the cost of index merge join operator and its children.
var GetCost4PhysicalIndexHashJoin func(pp base.PhysicalPlan, outerCnt, innerCnt, outerCost, innerCost float64,
	costFlag uint64) float64

// GetPlanCostVer1PhysicalIndexHashJoin calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer1PhysicalIndexHashJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// Attach2Task4PhysicalIndexHashJoin will be called by PhysicalIndexHashJoin in physicalOp pkg.
var Attach2Task4PhysicalIndexHashJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalHashAgg computes the cost of hash aggregation considering CPU/memory.
var GetCost4PhysicalHashAgg func(pp base.PhysicalPlan, inputRows float64, isRoot, isMPP bool,
	costFlag uint64) float64

// GetPlanCostVer14PhysicalHashAgg calculates the cost of the plan if it has not been
// calculated yet and returns the cost.
var GetPlanCostVer14PhysicalHashAgg func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalHashAgg calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer24PhysicalHashAgg func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalHashAgg implements PhysicalPlan interface for PhysicalHashJoin.
var Attach2Task4PhysicalHashAgg func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalStreamAgg computes cost of stream aggregation considering CPU/memory.
var GetCost4PhysicalStreamAgg func(pp base.PhysicalPlan, inputRows float64, isRoot bool,
	costFlag uint64) float64

// GetPlanCostVer14PhysicalStreamAgg calculates the cost of the plan if it has not been
// calculated yet and returns the cost.
var GetPlanCostVer14PhysicalStreamAgg func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalStreamAgg returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + agg-cost + group-cost
var GetPlanCostVer24PhysicalStreamAgg func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalStreamAgg implements PhysicalPlan interface.
var Attach2Task4PhysicalStreamAgg func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// Attach2Task4PhysicalApply implements PhysicalPlan interface for PhysicalApply
var Attach2Task4PhysicalApply func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalApply computes the cost of apply operator.
var GetCost4PhysicalApply func(pp base.PhysicalPlan, lCount, rCount, lCost, rCost float64) float64

// GetPlanCostVer14PhysicalApply calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalApply func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalApply returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
var GetPlanCostVer24PhysicalApply func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (costusage.CostVer2, error)

// GetCost4PhysicalIndexLookUpReader computes the cost of index lookup reader operator.
var GetCost4PhysicalIndexLookUpReader func(pp base.PhysicalPlan, costFlag uint64) float64

// GetPlanCostVer14PhysicalIndexLookUpReader calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalIndexLookUpReader func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalIndexLookUpReader returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
var GetPlanCostVer24PhysicalIndexLookUpReader func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error)

// ResolveIndices4PhysicalIndexLookUpReader is used to resolve indices for PhysicalIndexLookUpReader.
var ResolveIndices4PhysicalIndexLookUpReader func(pp base.PhysicalPlan) (err error)

// Attach2Task4PhysicalSequence will be called by PhysicalSequence in physicalOp pkg.
var Attach2Task4PhysicalSequence func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer24PhysicalCTE will be called by PhysicalCTE in physicalOp pkg.
var GetPlanCostVer24PhysicalCTE func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalCTEStorage will be called in physicalOp pkg.
var Attach2Task4PhysicalCTEStorage func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer24PhysicalTableReader get the cost v2 for table reader.
var GetPlanCostVer24PhysicalTableReader func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error)

// GetPlanCostVer14PhysicalTableReader get the cost v1 for table reader.
var GetPlanCostVer14PhysicalTableReader func(pp base.PhysicalPlan,
	option *costusage.PlanCostOption) (float64, error)

// GetCost4PointGetPlan calculates the cost of the plan if it has not been calculated yet and returns the cost.
var GetCost4PointGetPlan func(pp base.PhysicalPlan) float64

// GetPlanCostVer14PointGetPlan calculates the cost of the plan if it has not been calculated yet and returns the cost.
var GetPlanCostVer14PointGetPlan func(pp base.PhysicalPlan, _ property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24PointGetPlan returns the plan-cost of this sub-plan, which is:
var GetPlanCostVer24PointGetPlan func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (costusage.CostVer2, error)

// GetCost4BatchPointGetPlan returns cost of the BatchPointGetPlan.
var GetCost4BatchPointGetPlan func(pp base.PhysicalPlan) float64

// GetPlanCostVer14BatchPointGetPlan calculates the cost of the plan if it has not
// been calculated yet and returns the cost.
var GetPlanCostVer14BatchPointGetPlan func(pp base.PhysicalPlan, _ property.TaskType,
	option *costusage.PlanCostOption) (float64, error)

// GetPlanCostVer24BatchPointGetPlan returns the plan-cost of this sub-plan, which is:
var GetPlanCostVer24BatchPointGetPlan func(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (costusage.CostVer2, error)

// GetCost4PhysicalIndexMergeJoin computes the cost of index merge join operator and its children.
var GetCost4PhysicalIndexMergeJoin func(pp base.PhysicalPlan,
	outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64

// GetPlanCostVer14PhysicalIndexMergeJoin computes the cost of index merge join operator and its children
var GetPlanCostVer14PhysicalIndexMergeJoin func(pp base.PhysicalPlan,
	taskType property.TaskType, option *costusage.PlanCostOption) (float64, error)

// Attach2Task4PhysicalIndexMergeJoin implements PhysicalPlan interface.
var Attach2Task4PhysicalIndexMergeJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// ****************************************** task related ***********************************************

// AttachPlan2Task will be called by BasePhysicalPlan in physicalOp pkg.
var AttachPlan2Task func(p base.PhysicalPlan, t base.Task) base.Task

// GetTaskPlanCost export the getTaskPlanCost from core pkg for cascades usage.
var GetTaskPlanCost func(t base.Task) (float64, bool, error)

// CompareTaskCost export the compareTaskCost from core pkg for cascades usage.
var CompareTaskCost func(curTask, bestTask base.Task) (
	curIsBetter bool, err error)

// GetPossibleAccessPaths is used in static pruning, when it is not needed, remove this func pointer.
var GetPossibleAccessPaths func(
	ctx base.PlanContext,
	tableHints *hint.PlanHints,
	indexHints []*ast.IndexHint,
	tbl table.Table,
	dbName, tblName ast.CIStr,
	check bool,
	hasFlagPartitionProcessor bool,
) ([]*util.AccessPath, bool, error)

// **************************************** plan clone related ********************************************

// CloneExpressionsForPlanCache is used to clone expressions for plan cache.
func CloneExpressionsForPlanCache(exprs, cloned []expression.Expression) []expression.Expression {
	if exprs == nil {
		return nil
	}
	allSafe := true
	for _, e := range exprs {
		if !e.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return exprs
	}
	if cloned == nil {
		cloned = make([]expression.Expression, 0, len(exprs))
	} else {
		cloned = cloned[:0]
	}
	for _, e := range exprs {
		if e.SafeToShareAcrossSession() {
			cloned = append(cloned, e)
		} else {
			cloned = append(cloned, e.Clone())
		}
	}
	return cloned
}

// CloneColumnsForPlanCache is used to clone columns for plan cache.
func CloneColumnsForPlanCache(cols, cloned []*expression.Column) []*expression.Column {
	if cols == nil {
		return nil
	}
	allSafe := true
	for _, c := range cols {
		if !c.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return cols
	}
	if cloned == nil {
		cloned = make([]*expression.Column, 0, len(cols))
	} else {
		cloned = cloned[:0]
	}
	for _, c := range cols {
		if c == nil {
			cloned = append(cloned, nil)
			continue
		}
		if c.SafeToShareAcrossSession() {
			cloned = append(cloned, c)
		} else {
			cloned = append(cloned, c.Clone().(*expression.Column))
		}
	}
	return cloned
}

// CloneConstantsForPlanCache is used to clone constants for plan cache.
func CloneConstantsForPlanCache(constants, cloned []*expression.Constant) []*expression.Constant {
	if constants == nil {
		return nil
	}
	allSafe := true
	for _, c := range constants {
		if c == nil {
			continue
		}
		if !c.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return constants
	}
	if cloned == nil {
		cloned = make([]*expression.Constant, 0, len(constants))
	} else {
		cloned = cloned[:0]
	}
	for _, c := range constants {
		if c.SafeToShareAcrossSession() {
			cloned = append(cloned, c)
		} else {
			cloned = append(cloned, c.Clone().(*expression.Constant))
		}
	}
	return cloned
}

// CloneScalarFunctionsForPlanCache is used clone scalar functions for plan cache
func CloneScalarFunctionsForPlanCache(scalarFuncs, cloned []*expression.ScalarFunction) []*expression.ScalarFunction {
	if scalarFuncs == nil {
		return nil
	}
	allSafe := true
	for _, f := range scalarFuncs {
		if !f.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return scalarFuncs
	}
	if cloned == nil {
		cloned = make([]*expression.ScalarFunction, 0, len(scalarFuncs))
	} else {
		cloned = cloned[:0]
	}
	for _, f := range scalarFuncs {
		if f.SafeToShareAcrossSession() {
			cloned = append(cloned, f)
		} else {
			cloned = append(cloned, f.Clone().(*expression.ScalarFunction))
		}
	}
	return cloned
}

// CloneExpression2DForPlanCache is used to clone 2D expressions for plan cache.
func CloneExpression2DForPlanCache(exprs [][]expression.Expression) [][]expression.Expression {
	if exprs == nil {
		return nil
	}
	cloned := make([][]expression.Expression, 0, len(exprs))
	for _, e := range exprs {
		cloned = append(cloned, CloneExpressionsForPlanCache(e, nil))
	}
	return cloned
}

// ****************************************** optimize portal *********************************************

// DoOptimize is to optimize a logical plan.
var DoOptimize func(
	ctx context.Context,
	sctx base.PlanContext,
	flag uint64,
	logic base.LogicalPlan,
) (base.LogicalPlan, base.PhysicalPlan, float64, error)
