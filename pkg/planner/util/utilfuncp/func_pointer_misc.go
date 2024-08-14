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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

// this file is used for passing function pointer at init(){} to avoid some import cycles.

// HasMaxOneRowUtil is used in baseLogicalPlan implementation of LogicalPlan interface, while
// the original HasMaxOneRowUtil has some dependency of original core pkg: like Datasource which
// hasn't been moved out of core pkg, so associative func pointer is introduced.
// todo: (1) arenatlx, remove this func pointer when concrete Logical Operators moved out of core.
var HasMaxOneRowUtil func(p base.LogicalPlan, childMaxOneRow []bool) bool

// AppendCandidate4PhysicalOptimizeOp is used in all logicalOp's findBestTask to trace the physical
// optimizing steps. Since we try to move baseLogicalPlan out of core, then other concrete logical
// operators, this appendCandidate4PhysicalOptimizeOp will make logicalOp/pkg back import core/pkg;
// if we move appendCandidate4PhysicalOptimizeOp together with baseLogicalPlan to logicalOp/pkg, it
// will heavily depend on concrete other logical operators inside, which are still defined in core/pkg
// too.
// todo: (2) arenatlx, remove this func pointer when concrete Logical Operators moved out of core.
var AppendCandidate4PhysicalOptimizeOp func(pop *optimizetrace.PhysicalOptimizeOp, lp base.LogicalPlan,
	pp base.PhysicalPlan, prop *property.PhysicalProperty)

// GetTaskPlanCost returns the cost of this task.
// The new cost interface will be used if EnableNewCostInterface is true.
// The second returned value indicates whether this task is valid.
// todo: (3) arenatlx, remove this func pointer when Task pkg is moved out of core, and
// getTaskPlanCost can be some member function usage of its family.
var GetTaskPlanCost func(t base.Task, pop *optimizetrace.PhysicalOptimizeOp) (float64, bool, error)

// AddSelection will add a selection if necessary.
// This function is util function pointer that initialized by core functionality.
// todo: (4) arenatlx, remove this func pointer when inside referred LogicalSelection is moved out of core.
var AddSelection func(p base.LogicalPlan, child base.LogicalPlan, conditions []expression.Expression,
	chIdx int, opt *optimizetrace.LogicalOptimizeOp)

// PushDownTopNForBaseLogicalPlan will be called by baseLogicalPlan in logicalOp pkg. While the implementation
// of pushDownTopNForBaseLogicalPlan depends on concrete logical operators.
// todo: (5) arenatlx, Remove this util func pointer when logical operators are moved from core to logicalop.
var PushDownTopNForBaseLogicalPlan func(s base.LogicalPlan, topNLogicalPlan base.LogicalPlan,
	opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan

// FindBestTask will be called by baseLogicalPlan in logicalOp pkg. The logic inside covers Task, Property,
// LogicalOp and PhysicalOp, so it doesn't belong to logicalOp pkg. it should be kept in core pkg.
// todo: (6) arenatlx, For clear division, we should remove Logical FindBestTask interface. Let core pkg to guide
// todo: itself by receive logical tree.
var FindBestTask func(p base.LogicalPlan, prop *property.PhysicalProperty, planCounter *base.PlanCounterTp,
	opt *optimizetrace.PhysicalOptimizeOp) (bestTask base.Task, cntPlan int64, err error)

// CanPushToCopImpl will be called by baseLogicalPlan in logicalOp pkg. The logic inside covers concrete logical
// operators.
// todo: (7) arenatlx, remove this util func pointer when logical operators are all moved from core to logicalOp.
var CanPushToCopImpl func(p base.LogicalPlan, storeTp kv.StoreType, considerDual bool) bool

// PruneByItems will be called by baseLogicalPlan in logicalOp pkg. The logic current exists for rule logic
// inside core.
// todo: (8) arenatlx, when rule is moved out of core, we should direct ref the rule.Func instead of this
// util func pointer.
var PruneByItems func(p base.LogicalPlan, old []*util.ByItems, opt *optimizetrace.LogicalOptimizeOp) (
	byItems []*util.ByItems, parentUsedCols []*expression.Column)

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

// ExhaustPhysicalPlans4LogicalSequence will be called by LogicalSequence in logicalOp pkg.
var ExhaustPhysicalPlans4LogicalSequence func(lp base.LogicalPlan, prop *property.PhysicalProperty) (
	[]base.PhysicalPlan, bool, error)

// FindBestTask4LogicalTableDual will be called by LogicalTableDual in logicalOp pkg.
var FindBestTask4LogicalTableDual func(lp base.LogicalPlan, prop *property.PhysicalProperty,
	planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error)

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

// *************************************** physical op related *******************************************

// GetEstimatedProbeCntFromProbeParents will be called by BasePhysicalPlan in physicalOp pkg.
var GetEstimatedProbeCntFromProbeParents func(probeParents []base.PhysicalPlan) float64

// GetActualProbeCntFromProbeParents will be called by BasePhysicalPlan in physicalOp pkg.
var GetActualProbeCntFromProbeParents func(pps []base.PhysicalPlan, statsColl *execdetails.RuntimeStatsColl) int64

// ****************************************** task related ***********************************************

// AttachPlan2Task will be called by BasePhysicalPlan in physicalOp pkg.
var AttachPlan2Task func(p base.PhysicalPlan, t base.Task) base.Task

// WindowIsTopN is used in DeriveTopNFromWindow rule.
// todo: @arenatlx: remove it after logical_datasource is migrated to logicalop.
var WindowIsTopN func(p base.LogicalPlan) (bool, uint64)
