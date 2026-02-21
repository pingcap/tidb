// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tipb/go-tipb"
)

// getNumberOfRanges returns the total number of ranges of a physical plan tree.
// Some queries with large IN list like `a in (1, 2, 3...)` could generate a large number of ranges, which may slow down the query performance.
func getNumberOfRanges(pp base.PhysicalPlan) (totNumRanges int) {
	switch p := pp.(type) {
	case *physicalop.PhysicalTableReader:
		return getNumberOfRanges(p.TablePlan)
	case *physicalop.PhysicalIndexReader:
		return getNumberOfRanges(p.IndexPlan)
	case *physicalop.PhysicalIndexLookUpReader:
		return getNumberOfRanges(p.IndexPlan) + getNumberOfRanges(p.TablePlan)
	case *physicalop.PhysicalTableScan:
		return len(p.Ranges)
	case *physicalop.PhysicalIndexScan:
		return len(p.Ranges)
	}
	for _, child := range pp.Children() {
		totNumRanges += getNumberOfRanges(child)
	}
	return totNumRanges
}

// getPlanCostVer24PhysicalApply returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func getPlanCostVer24PhysicalApply(pp base.PhysicalPlan, taskType property.TaskType,
	option *costusage.PlanCostOption) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalApply)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	buildRows := getCardinality(p.Children()[0], option.CostFlag)
	probeRowsOne := getCardinality(p.Children()[1], option.CostFlag)
	probeRowsTot := buildRows * probeRowsOne
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(option, buildRows, p.LeftConditions, cpuFactor)
	buildChildCost, err := p.Children()[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	probeFilterCost := filterCostVer2(option, probeRowsTot, p.RightConditions, cpuFactor)
	probeChildCost, err := p.Children()[1].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	probeCost := costusage.MulCostVer2(probeChildCost, buildRows)

	p.PlanCostVer2 = costusage.SumCostVer2(buildChildCost, buildFilterCost, probeCost, probeFilterCost)
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalUnionAll calculates the cost of the plan if it has not been calculated yet and returns the cost.
// plan-cost = sum(child-cost) / concurrency
func getPlanCostVer24PhysicalUnionAll(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalUnionAll)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	concurrency := float64(p.SCtx().GetSessionVars().UnionConcurrency())
	childCosts := make([]costusage.CostVer2, 0, len(p.Children()))
	for _, child := range p.Children() {
		childCost, err := child.GetPlanCostVer2(taskType, option)
		if err != nil {
			return costusage.ZeroCostVer2, err
		}
		childCosts = append(childCosts, childCost)
	}
	p.PlanCostVer2 = costusage.DivCostVer2(costusage.SumCostVer2(childCosts...), concurrency)
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PointGetPlan returns the plan-cost of this sub-plan, which is:
func getPlanCostVer24PointGetPlan(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PointGetPlan)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	if p.AccessCols() == nil { // from fast plan code path
		p.PlanCostVer2 = costusage.ZeroCostVer2
		p.PlanCostInit = true
		return costusage.ZeroCostVer2, nil
	}
	rowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)

	p.PlanCostVer2 = netCostVer2(option, 1, rowSize, netFactor)
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

// getPlanCostVer2PhysicalExchangeReceiver returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + net-cost
func getPlanCostVer2PhysicalExchangeReceiver(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalExchangeReceiver)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)
	isBCast := false
	if sender, ok := p.Children()[0].(*physicalop.PhysicalExchangeSender); ok {
		isBCast = sender.ExchangeType == tipb.ExchangeType_Broadcast
	}
	numNode := float64(3) // TODO: remove this empirical value

	netCost := netCostVer2(option, rows, rowSize, netFactor)
	if isBCast {
		netCost = costusage.MulCostVer2(netCost, numNode)
	}
	childCost, err := p.Children()[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.SumCostVer2(childCost, netCost)
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

// getPlanCostVer24BatchPointGetPlan returns the plan-cost of this sub-plan, which is:
func getPlanCostVer24BatchPointGetPlan(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (costusage.CostVer2, error) {
	p := pp.(*physicalop.BatchPointGetPlan)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	if p.AccessCols() == nil { // from fast plan code path
		p.PlanCostVer2 = costusage.ZeroCostVer2
		p.PlanCostInit = true
		return costusage.ZeroCostVer2, nil
	}
	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)

	p.PlanCostVer2 = netCostVer2(option, rows, rowSize, netFactor)
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalCTE implements PhysicalPlan interface.
func getPlanCostVer24PhysicalCTE(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalCTE)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	inputRows := getCardinality(p, option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	projCost := filterCostVer2(option, inputRows, expression.Column2Exprs(p.Schema().Columns), cpuFactor)

	p.PlanCostVer2 = projCost
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

