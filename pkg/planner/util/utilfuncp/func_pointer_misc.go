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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tipb/go-tipb"
)

// this file is used for passing function pointer at init(){} to avoid some import cycles.

// FindBestTask4BaseLogicalPlan will be called by baseLogicalPlan in logicalOp pkg.
// The logic inside covers Task, Property, LogicalOp and PhysicalOp, so it doesn't belong to logicalOp pkg.
// It should be kept in core pkg.
// todo: arenatlx, For clear division, we should remove Logical FindBestTask interface. Let core pkg to
// guide itself by receive logical tree.
var FindBestTask4BaseLogicalPlan func(p base.LogicalPlan, prop *property.PhysicalProperty,
	planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (
	bestTask base.Task, cntPlan int64, err error)

// ExhaustPhysicalPlans4LogicalMaxOneRow will be called by LogicalMaxOneRow in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalMaxOneRow func(p base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// FindBestTask4LogicalCTETable will be called by LogicalCTETable in logicalOp pkg.
var FindBestTask4LogicalCTETable func(lp base.LogicalPlan, prop *property.PhysicalProperty, _ *base.PlanCounterTp,
	_ *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error)

// FindBestTask4LogicalMemTable will be called by LogicalMemTable in logicalOp pkg.
var FindBestTask4LogicalMemTable func(lp base.LogicalPlan, prop *property.PhysicalProperty,
	planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (t base.Task,
	cntPlan int64, err error)

// FindBestTask4LogicalShow will be called by LogicalShow in logicalOp pkg.
var FindBestTask4LogicalShow func(lp base.LogicalPlan, prop *property.PhysicalProperty, planCounter *base.PlanCounterTp,
	_ *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error)

// FindBestTask4LogicalShowDDLJobs will be called by LogicalShowDDLJobs in logicalOp pkg.
var FindBestTask4LogicalShowDDLJobs func(lp base.LogicalPlan, prop *property.PhysicalProperty,
	planCounter *base.PlanCounterTp, _ *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error)

// FindBestTask4LogicalCTE will be called by LogicalCTE in logicalOp pkg.
var FindBestTask4LogicalCTE func(lp base.LogicalPlan, prop *property.PhysicalProperty,
	counter *base.PlanCounterTp, pop *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error)

// FindBestTask4LogicalTableDual will be called by LogicalTableDual in logicalOp pkg.
var FindBestTask4LogicalTableDual func(lp base.LogicalPlan, prop *property.PhysicalProperty,
	planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error)

// FindBestTask4LogicalDataSource will be called by LogicalDataSource in logicalOp pkg.
var FindBestTask4LogicalDataSource func(lp base.LogicalPlan, prop *property.PhysicalProperty,
	planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error)

// ExhaustPhysicalPlans4LogicalSequence will be called by LogicalSequence in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalSequence func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalSort will be called by LogicalSort in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalSort func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalTopN will be called by LogicalTopN in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalTopN func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalLimit will be called by LogicalLimit in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalLimit func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalProjection will be called by LogicalLimit in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalProjection func(lp base.LogicalPlan,
	prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalWindow will be called by LogicalWindow in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalWindow func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalLock will be called by LogicalLock in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalLock func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalUnionScan will be called by LogicalUnionScan in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalUnionScan func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalSelection will be called by LogicalSelection in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalSelection func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalJoin will be called by LogicalJoin in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalJoin func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalAggregation will be called by LogicalAggregation in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalAggregation func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalApply will be called by LogicalApply in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalApply func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalPartitionUnionAll will be called by LogicalPartitionUnionAll in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalPartitionUnionAll func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalUnionAll will be called by LogicalUnionAll in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalUnionAll func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalExpand will be called by LogicalExpand in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalExpand func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// ExhaustPhysicalPlans4LogicalCTE will be called by LogicalCTE in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalCTE func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
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

// ApplyPredicateSimplification will be called by LogicalSelection in logicalOp pkg.
var ApplyPredicateSimplification func(base.PlanContext, []expression.Expression, bool) []expression.Expression

// IsSingleScan check whether the data source is a single scan.
var IsSingleScan func(ds base.LogicalPlan, indexColumns []*expression.Column, idxColLens []int) bool

// *************************************** physical op related *******************************************

// GetEstimatedProbeCntFromProbeParents will be called by BasePhysicalPlan in physicalOp pkg.
var GetEstimatedProbeCntFromProbeParents func(probeParents []base.PhysicalPlan) float64

// GetActualProbeCntFromProbeParents will be called by BasePhysicalPlan in physicalOp pkg.
var GetActualProbeCntFromProbeParents func(pps []base.PhysicalPlan, statsColl *execdetails.RuntimeStatsColl) int64

// GetPlanCost export the getPlanCost from core pkg for cascades usage.
// todo: remove this three func pointer when physical op are all migrated to physicalop pkg.
var GetPlanCost func(base.PhysicalPlan, property.TaskType, *optimizetrace.PlanCostOption) (float64, error)

// Attach2Task4PhysicalSort will be called by PhysicalSort in physicalOp pkg.
var Attach2Task4PhysicalSort func(p base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalSort will be called by PhysicalSort in physicalOp pkg.
var GetCost4PhysicalSort func(p base.PhysicalPlan, count float64, schema *expression.Schema) float64

// GetPlanCostVer14PhysicalSort will be called by PhysicalSort in physicalOp pkg.
var GetPlanCostVer14PhysicalSort func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalSort represents the cost of a physical sort operation in version 2.
var GetPlanCostVer24PhysicalSort func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// ToPB4PhysicalSort will be called by PhysicalSort in physicalOp pkg.
var ToPB4PhysicalSort func(pp base.PhysicalPlan, ctx *base.BuildPBContext,
	storeType kv.StoreType) (*tipb.Executor, error)

// ResolveIndicesForSort will be called by PhysicalSort in physicalOp pkg.
var ResolveIndicesForSort func(p base.PhysicalPlan) (err error)

// Attach2Task4NominalSort will be called by NominalSort in physicalOp pkg.
var Attach2Task4NominalSort func(base.PhysicalPlan, ...base.Task) base.Task

// Attach2Task4PhysicalUnionAll will be called by PhysicalUnionAll in physicalOp pkg.
var Attach2Task4PhysicalUnionAll func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalUnionAll will be called by PhysicalUnionAll in physicalOp pkg.
var GetPlanCostVer14PhysicalUnionAll func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalUnionAll will be called by PhysicalUnionAll in physicalOp pkg.
var GetPlanCostVer24PhysicalUnionAll func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, _ ...bool) (costusage.CostVer2, error)

// ResolveIndices4PhysicalLimit will be called by PhysicalLimit in physicalOp pkg.
var ResolveIndices4PhysicalLimit func(pp base.PhysicalPlan) (err error)

// Attach2Task4PhysicalLimit will be called by PhysicalLimit in physicalOp pkg.
var Attach2Task4PhysicalLimit func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalTopN will be called by PhysicalLimit in physicalOp pkg.
var GetPlanCostVer14PhysicalTopN func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalTopN will be called by PhysicalLimit in physicalOp pkg.
var GetPlanCostVer24PhysicalTopN func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalTopN will be called by PhysicalTopN in physicalOp pkg.
var Attach2Task4PhysicalTopN func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// ResolveIndices4PhysicalTopN will be called by PhysicalTopN in physicalOp pkg.
var ResolveIndices4PhysicalTopN func(pp base.PhysicalPlan) (err error)

// GetPlanCostVer24PhysicalSelection will be called by PhysicalSelection in physicalOp pkg.
var GetPlanCostVer24PhysicalSelection func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// ResolveIndices4PhysicalSelection will be called by PhysicalSelection in physicalOp pkg.
var ResolveIndices4PhysicalSelection func(pp base.PhysicalPlan) (err error)

// Attach2Task4PhysicalSelection will be called by PhysicalSelection in physicalOp pkg.
var Attach2Task4PhysicalSelection func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalSelection will be called by PhysicalSelection in physicalOp pkg.
var GetPlanCostVer14PhysicalSelection func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

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
	option *optimizetrace.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalProjection will be called by PhysicalProjection in physicalOp pkg.
var GetPlanCostVer24PhysicalProjection func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalProjection will be called by PhysicalProjection in physicalOp pkg.
var Attach2Task4PhysicalProjection func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalIndexJoin will be called by PhysicalIndexJoin in physicalOp pkg.
var GetCost4PhysicalIndexJoin func(pp base.PhysicalPlan,
	outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64

// GetPlanCostVer14PhysicalIndexJoin calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalIndexJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// GetIndexJoinCostVer24PhysicalIndexJoin will be called by PhysicalIndexJoin in physicalOp pkg.
var GetIndexJoinCostVer24PhysicalIndexJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, indexJoinType int) (costusage.CostVer2, error)

// Attach2Task4PhysicalIndexJoin will be called by PhysicalIndexJoin in physicalOp pkg.
var Attach2Task4PhysicalIndexJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// InitForStream will be called by BasePhysicalAgg in physicalOp pkg.
var InitForStream func(base base.PhysicalPlan, ctx base.PlanContext, stats *property.StatsInfo,
	offset int, schema *expression.Schema, props ...*property.PhysicalProperty) base.PhysicalPlan

// Attach2Task4PhysicalWindow will be called by PhysicalWindow in physicalOp pkg.
var Attach2Task4PhysicalWindow func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalMergeJoin computes cost of merge join operator itself.
var GetCost4PhysicalMergeJoin func(pp base.PhysicalPlan, lCnt, rCnt float64, costFlag uint64) float64

// Attach2Task4PhysicalMergeJoin will be called by PhysicalMergeJoin in physicalOp pkg.
var Attach2Task4PhysicalMergeJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer14PhysicalMergeJoin calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalMergeJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalMergeJoin returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
var GetPlanCostVer24PhysicalMergeJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, _ ...bool) (costusage.CostVer2, error)

// GetPlanCostVer14PhysicalTableScan calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalTableScan func(pp base.PhysicalPlan,
	option *optimizetrace.PlanCostOption) (float64, error)

// GetCost4PhysicalHashJoin computes cost of hash join operator itself.
var GetCost4PhysicalHashJoin func(pp base.PhysicalPlan, lCnt, rCnt float64, costFlag uint64,
	op *optimizetrace.PhysicalOptimizeOp) float64

// GetPlanCostVer14PhysicalHashJoin calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalHashJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// Attach2Task4PhysicalHashJoin implements PhysicalPlan interface for PhysicalHashJoin.
var Attach2Task4PhysicalHashJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetPlanCostVer24PhysicalTableScan returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
var GetPlanCostVer24PhysicalTableScan func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// GetPlanCostVer24PhysicalHashJoin returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
var GetPlanCostVer24PhysicalHashJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (costusage.CostVer2, error)

// GetCost4PhysicalIndexHashJoin computes the cost of index merge join operator and its children.
var GetCost4PhysicalIndexHashJoin func(pp base.PhysicalPlan, outerCnt, innerCnt, outerCost, innerCost float64,
	costFlag uint64) float64

// GetPlanCostVer1PhysicalIndexHashJoin calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer1PhysicalIndexHashJoin func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// Attach2Task4PhysicalIndexHashJoin will be called by PhysicalIndexHashJoin in physicalOp pkg.
var Attach2Task4PhysicalIndexHashJoin func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalHashAgg computes the cost of hash aggregation considering CPU/memory.
var GetCost4PhysicalHashAgg func(pp base.PhysicalPlan, inputRows float64, isRoot, isMPP bool,
	costFlag uint64) float64

// GetPlanCostVer14PhysicalHashAgg calculates the cost of the plan if it has not been
// calculated yet and returns the cost.
var GetPlanCostVer14PhysicalHashAgg func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalHashAgg calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer24PhysicalHashAgg func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error)

// Attach2Task4PhysicalHashAgg implements PhysicalPlan interface for PhysicalHashJoin.
var Attach2Task4PhysicalHashAgg func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// Attach2Task4PhysicalApply implements PhysicalPlan interface for PhysicalApply
var Attach2Task4PhysicalApply func(pp base.PhysicalPlan, tasks ...base.Task) base.Task

// GetCost4PhysicalApply computes the cost of apply operator.
var GetCost4PhysicalApply func(pp base.PhysicalPlan, lCount, rCount, lCost, rCost float64) float64

// GetPlanCostVer14PhysicalApply calculates the cost of the plan if it has not been calculated yet
// and returns the cost.
var GetPlanCostVer14PhysicalApply func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error)

// GetPlanCostVer24PhysicalApply returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
var GetPlanCostVer24PhysicalApply func(pp base.PhysicalPlan, taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (costusage.CostVer2, error)

// ****************************************** task related ***********************************************

// AttachPlan2Task will be called by BasePhysicalPlan in physicalOp pkg.
var AttachPlan2Task func(p base.PhysicalPlan, t base.Task) base.Task

// WindowIsTopN is used in DeriveTopNFromWindow rule.
// todo: @arenatlx: remove it after logical_datasource is migrated to logicalop.
var WindowIsTopN func(p base.LogicalPlan) (bool, uint64)

// GetTaskPlanCost export the getTaskPlanCost from core pkg for cascades usage.
var GetTaskPlanCost func(t base.Task, pop *optimizetrace.PhysicalOptimizeOp) (float64, bool, error)

// CompareTaskCost export the compareTaskCost from core pkg for cascades usage.
var CompareTaskCost func(curTask, bestTask base.Task, op *optimizetrace.PhysicalOptimizeOp) (
	curIsBetter bool, err error)

// **************************************** plan clone related ********************************************

// CloneExpressionsForPlanCache is used to clone expressions for plan cache.
var CloneExpressionsForPlanCache func(exprs, cloned []expression.Expression) []expression.Expression

// CloneColumnsForPlanCache is used to clone columns for plan cache.
var CloneColumnsForPlanCache func(cols, cloned []*expression.Column) []*expression.Column

// CloneConstantsForPlanCache is used to clone constants for plan cache.
var CloneConstantsForPlanCache func(constants, cloned []*expression.Constant) []*expression.Constant

// CloneScalarFunctionsForPlanCache is used clone scalar functions for plan cache
var CloneScalarFunctionsForPlanCache func(scalarFuncs, cloned []*expression.ScalarFunction) []*expression.ScalarFunction

// ****************************************** optimize portal *********************************************

// DoOptimize is to optimize a logical plan.
var DoOptimize func(
	ctx context.Context,
	sctx base.PlanContext,
	flag uint64,
	logic base.LogicalPlan,
) (base.LogicalPlan, base.PhysicalPlan, float64, error)

// Attach2Task4PhysicalSequence will be called by PhysicalSequence in physicalOp pkg.
var Attach2Task4PhysicalSequence func(pp base.PhysicalPlan, tasks ...base.Task) base.Task
