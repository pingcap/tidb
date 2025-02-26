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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/execdetails"
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
var ApplyPredicateSimplification func(base.PlanContext, []expression.Expression) []expression.Expression

// IsSingleScan check whether the data source is a single scan.
var IsSingleScan func(ds base.LogicalPlan, indexColumns []*expression.Column, idxColLens []int) bool

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

// ****************************************** optimize portal *********************************************

// DoOptimize is to optimize a logical plan.
var DoOptimize func(
	ctx context.Context,
	sctx base.PlanContext,
	flag uint64,
	logic base.LogicalPlan,
) (base.LogicalPlan, base.PhysicalPlan, float64, error)
