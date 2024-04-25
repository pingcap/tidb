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
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tipb/go-tipb"
)

// GetPlanCost returns the cost of this plan.
func GetPlanCost(p base.PhysicalPlan, taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
	return getPlanCost(p, taskType, option)
}

// GenPlanCostTrace define a hook function to customize the cost calculation.
var GenPlanCostTrace func(p base.PhysicalPlan, costV *costusage.CostVer2, taskType property.TaskType, option *optimizetrace.PlanCostOption)

func getPlanCost(p base.PhysicalPlan, taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
	if p.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
		planCost, err := p.GetPlanCostVer2(taskType, option)
		if costusage.TraceCost(option) && GenPlanCostTrace != nil {
			GenPlanCostTrace(p, &planCost, taskType, option)
		}
		return planCost.GetCost(), err
	}
	return p.GetPlanCostVer1(taskType, option)
}

// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *basePhysicalPlan) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}
	childCosts := make([]costusage.CostVer2, 0, len(p.children))
	for _, child := range p.children {
		childCost, err := child.GetPlanCostVer2(taskType, option)
		if err != nil {
			return costusage.ZeroCostVer2, err
		}
		childCosts = append(childCosts, childCost)
	}
	if len(childCosts) == 0 {
		p.planCostVer2 = costusage.NewZeroCostVer2(costusage.TraceCost(option))
	} else {
		p.planCostVer2 = costusage.SumCostVer2(childCosts...)
	}
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + filter-cost
func (p *PhysicalSelection) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	filterCost := filterCostVer2(option, inputRows, p.Conditions, cpuFactor)

	childCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.SumCostVer2(filterCost, childCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + proj-cost / concurrency
// proj-cost = input-rows * len(expressions) * cpu-factor
func (p *PhysicalProjection) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	concurrency := float64(p.SCtx().GetSessionVars().ProjectionConcurrency())
	if concurrency == 0 {
		concurrency = 1 // un-parallel execution
	}

	projCost := filterCostVer2(option, inputRows, p.Exprs, cpuFactor)

	childCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.SumCostVer2(childCost, costusage.DivCostVer2(projCost, concurrency))
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalIndexScan) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p, option.CostFlag)
	rowSize := math.Max(getAvgRowSize(p.StatsInfo(), p.schema.Columns), 2.0) // consider all index columns
	scanFactor := getTaskScanFactorVer2(p, kv.TiKV, taskType)

	p.planCostVer2 = scanCostVer2(option, rows, rowSize, scanFactor)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalTableScan) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p, option.CostFlag)
	var rowSize float64
	if p.StoreType == kv.TiKV {
		rowSize = getAvgRowSize(p.StatsInfo(), p.tblCols) // consider all columns if TiKV
	} else { // TiFlash
		rowSize = getAvgRowSize(p.StatsInfo(), p.schema.Columns)
	}
	rowSize = math.Max(rowSize, 2.0)
	scanFactor := getTaskScanFactorVer2(p, p.StoreType, taskType)

	p.planCostVer2 = scanCostVer2(option, rows, rowSize, scanFactor)

	// give TiFlash a start-up cost to let the optimizer prefers to use TiKV to process small table scans.
	if p.StoreType == kv.TiFlash {
		p.planCostVer2 = costusage.SumCostVer2(p.planCostVer2, scanCostVer2(option, 10000, rowSize, scanFactor))
	}

	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func (p *PhysicalIndexReader) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p.indexPlan, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), p.schema.Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())

	netCost := netCostVer2(option, rows, rowSize, netFactor)

	childCost, err := p.indexPlan.GetPlanCostVer2(property.CopSingleReadTaskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.DivCostVer2(costusage.SumCostVer2(childCost, netCost), concurrency)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func (p *PhysicalTableReader) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p.tablePlan, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), p.schema.Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())
	childType := property.CopSingleReadTaskType
	if p.StoreType == kv.TiFlash { // mpp protocol
		childType = property.MppTaskType
	}

	netCost := netCostVer2(option, rows, rowSize, netFactor)

	childCost, err := p.tablePlan.GetPlanCostVer2(childType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.DivCostVer2(costusage.SumCostVer2(childCost, netCost), concurrency)
	p.planCostInit = true

	// consider tidb_enforce_mpp
	if p.StoreType == kv.TiFlash && p.SCtx().GetSessionVars().IsMPPEnforced() &&
		!hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) { // show the real cost in explain-statements
		p.planCostVer2 = costusage.DivCostVer2(p.planCostVer2, 1000000000)
	}
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = index-side-cost + (table-side-cost + double-read-cost) / double-read-concurrency
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
// double-read-cost = double-read-request-cost + double-read-cpu-cost
// double-read-request-cost = double-read-tasks * request-factor
// double-read-cpu-cost = index-rows * cpu-factor
// double-read-tasks = index-rows / batch-size * task-per-batch # task-per-batch is a magic number now
func (p *PhysicalIndexLookUpReader) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	indexRows := getCardinality(p.indexPlan, option.CostFlag)
	tableRows := getCardinality(p.indexPlan, option.CostFlag)
	indexRowSize := cardinality.GetAvgRowSize(p.SCtx(), getTblStats(p.indexPlan), p.indexPlan.Schema().Columns, true, false)
	tableRowSize := cardinality.GetAvgRowSize(p.SCtx(), getTblStats(p.tablePlan), p.tablePlan.Schema().Columns, false, false)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	netFactor := getTaskNetFactorVer2(p, taskType)
	requestFactor := getTaskRequestFactorVer2(p, taskType)
	distConcurrency := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())
	doubleReadConcurrency := float64(p.SCtx().GetSessionVars().IndexLookupConcurrency())

	// index-side
	indexNetCost := netCostVer2(option, indexRows, indexRowSize, netFactor)
	indexChildCost, err := p.indexPlan.GetPlanCostVer2(property.CopMultiReadTaskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	indexSideCost := costusage.DivCostVer2(costusage.SumCostVer2(indexNetCost, indexChildCost), distConcurrency)

	// table-side
	tableNetCost := netCostVer2(option, tableRows, tableRowSize, netFactor)
	tableChildCost, err := p.tablePlan.GetPlanCostVer2(property.CopMultiReadTaskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	tableSideCost := costusage.DivCostVer2(costusage.SumCostVer2(tableNetCost, tableChildCost), distConcurrency)

	doubleReadRows := indexRows
	doubleReadCPUCost := costusage.NewCostVer2(option, cpuFactor,
		indexRows*cpuFactor.Value,
		func() string { return fmt.Sprintf("double-read-cpu(%v*%v)", doubleReadRows, cpuFactor) })
	batchSize := float64(p.SCtx().GetSessionVars().IndexLookupSize)
	taskPerBatch := 32.0 // TODO: remove this magic number
	doubleReadTasks := doubleReadRows / batchSize * taskPerBatch
	doubleReadRequestCost := doubleReadCostVer2(option, doubleReadTasks, requestFactor)
	doubleReadCost := costusage.SumCostVer2(doubleReadCPUCost, doubleReadRequestCost)

	p.planCostVer2 = costusage.SumCostVer2(indexSideCost, costusage.DivCostVer2(costusage.SumCostVer2(tableSideCost, doubleReadCost), doubleReadConcurrency))

	if p.SCtx().GetSessionVars().EnablePaging && p.expectedCnt > 0 && p.expectedCnt <= paging.Threshold {
		// if the expectCnt is below the paging threshold, using paging API
		p.Paging = true // TODO: move this operation from cost model to physical optimization
		p.planCostVer2 = costusage.MulCostVer2(p.planCostVer2, 0.6)
	}

	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = table-side-cost + sum(index-side-cost)
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
func (p *PhysicalIndexMergeReader) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	netFactor := getTaskNetFactorVer2(p, taskType)
	distConcurrency := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())

	var tableSideCost costusage.CostVer2
	if tablePath := p.tablePlan; tablePath != nil {
		rows := getCardinality(tablePath, option.CostFlag)
		rowSize := getAvgRowSize(tablePath.StatsInfo(), tablePath.Schema().Columns)

		tableNetCost := netCostVer2(option, rows, rowSize, netFactor)
		tableChildCost, err := tablePath.GetPlanCostVer2(taskType, option)
		if err != nil {
			return costusage.ZeroCostVer2, err
		}
		tableSideCost = costusage.DivCostVer2(costusage.SumCostVer2(tableNetCost, tableChildCost), distConcurrency)
	}

	indexSideCost := make([]costusage.CostVer2, 0, len(p.partialPlans))
	for _, indexPath := range p.partialPlans {
		rows := getCardinality(indexPath, option.CostFlag)
		rowSize := getAvgRowSize(indexPath.StatsInfo(), indexPath.Schema().Columns)

		indexNetCost := netCostVer2(option, rows, rowSize, netFactor)
		indexChildCost, err := indexPath.GetPlanCostVer2(taskType, option)
		if err != nil {
			return costusage.ZeroCostVer2, err
		}
		indexSideCost = append(indexSideCost,
			costusage.DivCostVer2(costusage.SumCostVer2(indexNetCost, indexChildCost), distConcurrency))
	}
	sumIndexSideCost := costusage.SumCostVer2(indexSideCost...)

	p.planCostVer2 = costusage.SumCostVer2(tableSideCost, sumIndexSideCost)
	// give a bias to pushDown limit, since it will get the same cost with NON_PUSH_DOWN_LIMIT case via expect count.
	// push down limit case may reduce cop request consumption if any in some cases.
	//
	// for index merge intersection case, if we want to attach limit to table/index side, we should enumerate double-read-cop task type.
	// otherwise, the entire index-merge-reader will be encapsulated as root task, and limit can only be put outside of that.
	// while, since limit doesn't contain any physical cost, the expected cnt has already pushed down as a kind of physical property.
	// that means the 2 physical tree format:
	// 		limit -> index merge reader
	// 		index-merger-reader(with embedded limit)
	// will have the same cost, actually if limit are more close to the fetch side, the fewer rows that table plan need to read.
	// todo: refine the cost computation out from cost model.
	if p.PushedLimit != nil {
		p.planCostVer2 = costusage.MulCostVer2(p.planCostVer2, 0.99)
	}
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + sort-cpu-cost + sort-mem-cost + sort-disk-cost
// sort-cpu-cost = rows * log2(rows) * len(sort-items) * cpu-factor
// if no spill:
// 1. sort-mem-cost = rows * row-size * mem-factor
// 2. sort-disk-cost = 0
// else if spill:
// 1. sort-mem-cost = mem-quota * mem-factor
// 2. sort-disk-cost = rows * row-size * disk-factor
func (p *PhysicalSort) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := math.Max(getCardinality(p.children[0], option.CostFlag), 1)
	rowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	diskFactor := defaultVer2Factors.TiDBDisk
	oomUseTmpStorage := variable.EnableTmpStorageOnOOM.Load()
	memQuota := p.SCtx().GetSessionVars().MemTracker.GetBytesLimit()
	spill := taskType == property.RootTaskType && // only TiDB can spill
		oomUseTmpStorage && // spill is enabled
		memQuota > 0 && // mem-quota is set
		rowSize*rows > float64(memQuota) // exceed the mem-quota

	sortCPUCost := orderCostVer2(option, rows, rows, p.ByItems, cpuFactor)

	var sortMemCost, sortDiskCost costusage.CostVer2
	if !spill {
		sortMemCost = costusage.NewCostVer2(option, memFactor,
			rows*rowSize*memFactor.Value,
			func() string { return fmt.Sprintf("sortMem(%v*%v*%v)", rows, rowSize, memFactor) })
		sortDiskCost = costusage.ZeroCostVer2
	} else {
		sortMemCost = costusage.NewCostVer2(option, memFactor,
			float64(memQuota)*memFactor.Value,
			func() string { return fmt.Sprintf("sortMem(%v*%v)", memQuota, memFactor) })
		sortDiskCost = costusage.NewCostVer2(option, diskFactor,
			rows*rowSize*diskFactor.Value,
			func() string { return fmt.Sprintf("sortDisk(%v*%v*%v)", rows, rowSize, diskFactor) })
	}

	childCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.SumCostVer2(childCost, sortCPUCost, sortMemCost, sortDiskCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + topn-cpu-cost + topn-mem-cost
// topn-cpu-cost = rows * log2(N) * len(sort-items) * cpu-factor
// topn-mem-cost = N * row-size * mem-factor
func (p *PhysicalTopN) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p.children[0], option.CostFlag)
	n := max(1, float64(p.Count+p.Offset))
	if n > 10000 {
		// It's only used to prevent some extreme cases, e.g. `select * from t order by a limit 18446744073709551615`.
		// For normal cases, considering that `rows` may be under-estimated, better to keep `n` unchanged.
		n = min(n, rows)
	}
	rowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)

	topNCPUCost := orderCostVer2(option, rows, n, p.ByItems, cpuFactor)
	topNMemCost := costusage.NewCostVer2(option, memFactor,
		n*rowSize*memFactor.Value,
		func() string { return fmt.Sprintf("topMem(%v*%v*%v)", n, rowSize, memFactor) })

	childCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.SumCostVer2(childCost, topNCPUCost, topNMemCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + agg-cost + group-cost
func (p *PhysicalStreamAgg) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	aggCost := aggCostVer2(option, rows, p.AggFuncs, cpuFactor)
	groupCost := groupCostVer2(option, rows, p.GroupByItems, cpuFactor)

	childCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.SumCostVer2(childCost, aggCost, groupCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + (agg-cost + group-cost + hash-build-cost + hash-probe-cost) / concurrency
func (p *PhysicalHashAgg) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := getCardinality(p.children[0], option.CostFlag)
	outputRows := getCardinality(p, option.CostFlag)
	outputRowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	concurrency := float64(p.SCtx().GetSessionVars().HashAggFinalConcurrency())

	aggCost := aggCostVer2(option, inputRows, p.AggFuncs, cpuFactor)
	groupCost := groupCostVer2(option, inputRows, p.GroupByItems, cpuFactor)
	hashBuildCost := hashBuildCostVer2(option, outputRows, outputRowSize, float64(len(p.GroupByItems)), cpuFactor, memFactor)
	hashProbeCost := hashProbeCostVer2(option, inputRows, float64(len(p.GroupByItems)), cpuFactor)
	startCost := costusage.NewCostVer2(option, cpuFactor,
		10*3*cpuFactor.Value, // 10rows * 3func * cpuFactor
		func() string { return fmt.Sprintf("cpu(10*3*%v)", cpuFactor) })

	childCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.SumCostVer2(startCost, childCost, costusage.DivCostVer2(costusage.SumCostVer2(aggCost, groupCost, hashBuildCost, hashProbeCost), concurrency))
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
func (p *PhysicalMergeJoin) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	leftRows := getCardinality(p.children[0], option.CostFlag)
	rightRows := getCardinality(p.children[1], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	filterCost := costusage.SumCostVer2(filterCostVer2(option, leftRows, p.LeftConditions, cpuFactor),
		filterCostVer2(option, rightRows, p.RightConditions, cpuFactor))
	groupCost := costusage.SumCostVer2(groupCostVer2(option, leftRows, cols2Exprs(p.LeftJoinKeys), cpuFactor),
		groupCostVer2(option, rightRows, cols2Exprs(p.LeftJoinKeys), cpuFactor))

	leftChildCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	rightChildCost, err := p.children[1].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.SumCostVer2(leftChildCost, rightChildCost, filterCost, groupCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
func (p *PhysicalHashJoin) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	build, probe := p.children[0], p.children[1]
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	buildKeys, probeKeys := p.LeftJoinKeys, p.RightJoinKeys
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		build, probe = probe, build
		buildFilters, probeFilters = probeFilters, buildFilters
	}
	buildRows := getCardinality(build, option.CostFlag)
	probeRows := getCardinality(probe, option.CostFlag)
	buildRowSize := getAvgRowSize(build.StatsInfo(), build.Schema().Columns)
	tidbConcurrency := float64(p.Concurrency)
	mppConcurrency := float64(3) // TODO: remove this empirical value
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(option, buildRows, buildFilters, cpuFactor)
	buildHashCost := hashBuildCostVer2(option, buildRows, buildRowSize, float64(len(buildKeys)), cpuFactor, memFactor)

	probeFilterCost := filterCostVer2(option, probeRows, probeFilters, cpuFactor)
	probeHashCost := hashProbeCostVer2(option, probeRows, float64(len(probeKeys)), cpuFactor)

	buildChildCost, err := build.GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	probeChildCost, err := probe.GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	if taskType == property.MppTaskType { // BCast or Shuffle Join, use mppConcurrency
		p.planCostVer2 = costusage.SumCostVer2(buildChildCost, probeChildCost,
			costusage.DivCostVer2(costusage.SumCostVer2(buildHashCost, buildFilterCost, probeHashCost, probeFilterCost), mppConcurrency))
	} else { // TiDB HashJoin
		startCost := costusage.NewCostVer2(option, cpuFactor,
			10*3*cpuFactor.Value, // 10rows * 3func * cpuFactor
			func() string { return fmt.Sprintf("cpu(10*3*%v)", cpuFactor) })
		p.planCostVer2 = costusage.SumCostVer2(startCost, buildChildCost, probeChildCost, buildHashCost, buildFilterCost,
			costusage.DivCostVer2(costusage.SumCostVer2(probeFilterCost, probeHashCost), tidbConcurrency))
	}
	p.planCostInit = true
	return p.planCostVer2, nil
}

func (p *PhysicalIndexJoin) getIndexJoinCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption, indexJoinType int) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	build, probe := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	buildRows := getCardinality(build, option.CostFlag)
	buildRowSize := getAvgRowSize(build.StatsInfo(), build.Schema().Columns)
	probeRowsOne := getCardinality(probe, option.CostFlag)
	probeRowsTot := probeRowsOne * buildRows
	probeRowSize := getAvgRowSize(probe.StatsInfo(), probe.Schema().Columns)
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	probeConcurrency := float64(p.SCtx().GetSessionVars().IndexLookupJoinConcurrency())
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	requestFactor := getTaskRequestFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(option, buildRows, buildFilters, cpuFactor)
	buildChildCost, err := build.GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	buildTaskCost := costusage.NewCostVer2(option, cpuFactor,
		buildRows*10*cpuFactor.Value,
		func() string { return fmt.Sprintf("cpu(%v*10*%v)", buildRows, cpuFactor) })
	startCost := costusage.NewCostVer2(option, cpuFactor,
		10*3*cpuFactor.Value,
		func() string { return fmt.Sprintf("cpu(10*3*%v)", cpuFactor) })

	probeFilterCost := filterCostVer2(option, probeRowsTot, probeFilters, cpuFactor)
	probeChildCost, err := probe.GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	var hashTableCost costusage.CostVer2
	switch indexJoinType {
	case 1: // IndexHashJoin
		hashTableCost = hashBuildCostVer2(option, buildRows, buildRowSize, float64(len(p.RightJoinKeys)), cpuFactor, memFactor)
	case 2: // IndexMergeJoin
		hashTableCost = costusage.NewZeroCostVer2(costusage.TraceCost(option))
	default: // IndexJoin
		hashTableCost = hashBuildCostVer2(option, probeRowsTot, probeRowSize, float64(len(p.LeftJoinKeys)), cpuFactor, memFactor)
	}

	// IndexJoin executes a batch of rows at a time, so the actual cost of this part should be
	//  `innerCostPerBatch * numberOfBatches` instead of `innerCostPerRow * numberOfOuterRow`.
	// Use an empirical value batchRatio to handle this now.
	// TODO: remove this empirical value.
	batchRatio := 6.0
	probeCost := costusage.DivCostVer2(costusage.MulCostVer2(probeChildCost, buildRows), batchRatio)

	// Double Read Cost
	doubleReadCost := costusage.NewZeroCostVer2(costusage.TraceCost(option))
	if p.SCtx().GetSessionVars().IndexJoinDoubleReadPenaltyCostRate > 0 {
		batchSize := float64(p.SCtx().GetSessionVars().IndexJoinBatchSize)
		taskPerBatch := 1024.0 // TODO: remove this magic number
		doubleReadTasks := buildRows / batchSize * taskPerBatch
		doubleReadCost = doubleReadCostVer2(option, doubleReadTasks, requestFactor)
		doubleReadCost = costusage.MulCostVer2(doubleReadCost, p.SCtx().GetSessionVars().IndexJoinDoubleReadPenaltyCostRate)
	}

	p.planCostVer2 = costusage.SumCostVer2(startCost, buildChildCost, buildFilterCost, buildTaskCost, costusage.DivCostVer2(costusage.SumCostVer2(doubleReadCost, probeCost, probeFilterCost, hashTableCost), probeConcurrency))
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost +
// (probe-cost + probe-filter-cost) / concurrency
// probe-cost = probe-child-cost * build-rows / batchRatio
func (p *PhysicalIndexJoin) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	return p.getIndexJoinCostVer2(taskType, option, 0)
}

// GetPlanCostVer2 implements PhysicalPlan interface.
func (p *PhysicalIndexHashJoin) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	return p.getIndexJoinCostVer2(taskType, option, 1)
}

// GetPlanCostVer2 implements PhysicalPlan interface.
func (p *PhysicalIndexMergeJoin) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	return p.getIndexJoinCostVer2(taskType, option, 2)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func (p *PhysicalApply) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	buildRows := getCardinality(p.children[0], option.CostFlag)
	probeRowsOne := getCardinality(p.children[1], option.CostFlag)
	probeRowsTot := buildRows * probeRowsOne
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(option, buildRows, p.LeftConditions, cpuFactor)
	buildChildCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	probeFilterCost := filterCostVer2(option, probeRowsTot, p.RightConditions, cpuFactor)
	probeChildCost, err := p.children[1].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	probeCost := costusage.MulCostVer2(probeChildCost, buildRows)

	p.planCostVer2 = costusage.SumCostVer2(buildChildCost, buildFilterCost, probeCost, probeFilterCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost.
// plan-cost = sum(child-cost) / concurrency
func (p *PhysicalUnionAll) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	concurrency := float64(p.SCtx().GetSessionVars().UnionConcurrency())
	childCosts := make([]costusage.CostVer2, 0, len(p.children))
	for _, child := range p.children {
		childCost, err := child.GetPlanCostVer2(taskType, option)
		if err != nil {
			return costusage.ZeroCostVer2, err
		}
		childCosts = append(childCosts, childCost)
	}
	p.planCostVer2 = costusage.DivCostVer2(costusage.SumCostVer2(childCosts...), concurrency)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + net-cost
func (p *PhysicalExchangeReceiver) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)
	isBCast := false
	if sender, ok := p.children[0].(*PhysicalExchangeSender); ok {
		isBCast = sender.ExchangeType == tipb.ExchangeType_Broadcast
	}
	numNode := float64(3) // TODO: remove this empirical value

	netCost := netCostVer2(option, rows, rowSize, netFactor)
	if isBCast {
		netCost = costusage.MulCostVer2(netCost, numNode)
	}
	childCost, err := p.children[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.planCostVer2 = costusage.SumCostVer2(childCost, netCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
func (p *PointGetPlan) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	if p.accessCols == nil { // from fast plan code path
		p.planCostVer2 = costusage.ZeroCostVer2
		p.planCostInit = true
		return costusage.ZeroCostVer2, nil
	}
	rowSize := getAvgRowSize(p.StatsInfo(), p.schema.Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)

	p.planCostVer2 = netCostVer2(option, 1, rowSize, netFactor)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
func (p *BatchPointGetPlan) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	if p.accessCols == nil { // from fast plan code path
		p.planCostVer2 = costusage.ZeroCostVer2
		p.planCostInit = true
		return costusage.ZeroCostVer2, nil
	}
	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), p.schema.Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)

	p.planCostVer2 = netCostVer2(option, rows, rowSize, netFactor)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// GetPlanCostVer2 implements PhysicalPlan interface.
func (p *PhysicalCTE) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := getCardinality(p, option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	projCost := filterCostVer2(option, inputRows, expression.Column2Exprs(p.schema.Columns), cpuFactor)

	p.planCostVer2 = projCost
	p.planCostInit = true
	return p.planCostVer2, nil
}

func scanCostVer2(option *optimizetrace.PlanCostOption, rows, rowSize float64, scanFactor costusage.CostVer2Factor) costusage.CostVer2 {
	if rowSize < 1 {
		rowSize = 1
	}
	return costusage.NewCostVer2(option, scanFactor,
		// rows * log(row-size) * scanFactor, log2 from experiments
		rows*math.Log2(rowSize)*scanFactor.Value,
		func() string { return fmt.Sprintf("scan(%v*logrowsize(%v)*%v)", rows, rowSize, scanFactor) })
}

func netCostVer2(option *optimizetrace.PlanCostOption, rows, rowSize float64, netFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, netFactor,
		rows*rowSize*netFactor.Value,
		func() string { return fmt.Sprintf("net(%v*rowsize(%v)*%v)", rows, rowSize, netFactor) })
}

func filterCostVer2(option *optimizetrace.PlanCostOption, rows float64, filters []expression.Expression, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	numFuncs := numFunctions(filters)
	return costusage.NewCostVer2(option, cpuFactor,
		rows*numFuncs*cpuFactor.Value,
		func() string { return fmt.Sprintf("cpu(%v*filters(%v)*%v)", rows, numFuncs, cpuFactor) })
}

func aggCostVer2(option *optimizetrace.PlanCostOption, rows float64, aggFuncs []*aggregation.AggFuncDesc, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, cpuFactor,
		// TODO: consider types of agg-funcs
		rows*float64(len(aggFuncs))*cpuFactor.Value,
		func() string { return fmt.Sprintf("agg(%v*aggs(%v)*%v)", rows, len(aggFuncs), cpuFactor) })
}

func groupCostVer2(option *optimizetrace.PlanCostOption, rows float64, groupItems []expression.Expression, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	numFuncs := numFunctions(groupItems)
	return costusage.NewCostVer2(option, cpuFactor,
		rows*numFuncs*cpuFactor.Value,
		func() string { return fmt.Sprintf("group(%v*cols(%v)*%v)", rows, numFuncs, cpuFactor) })
}

func numFunctions(exprs []expression.Expression) float64 {
	num := 0.0
	for _, e := range exprs {
		if _, ok := e.(*expression.ScalarFunction); ok {
			num++
		} else { // Column and Constant
			num += 0.01 // an empirical value
		}
	}
	return num
}

func orderCostVer2(option *optimizetrace.PlanCostOption, rows, n float64, byItems []*util.ByItems, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	numFuncs := 0
	for _, byItem := range byItems {
		if _, ok := byItem.Expr.(*expression.ScalarFunction); ok {
			numFuncs++
		}
	}
	exprCost := costusage.NewCostVer2(option, cpuFactor,
		rows*float64(numFuncs)*cpuFactor.Value,
		func() string { return fmt.Sprintf("exprCPU(%v*%v*%v)", rows, numFuncs, cpuFactor) })
	orderCost := costusage.NewCostVer2(option, cpuFactor,
		rows*math.Log2(n)*cpuFactor.Value,
		func() string { return fmt.Sprintf("orderCPU(%v*log(%v)*%v)", rows, n, cpuFactor) })
	return costusage.SumCostVer2(exprCost, orderCost)
}

func hashBuildCostVer2(option *optimizetrace.PlanCostOption, buildRows, buildRowSize, nKeys float64, cpuFactor, memFactor costusage.CostVer2Factor) costusage.CostVer2 {
	// TODO: 1) consider types of keys, 2) dedicated factor for build-probe hash table
	hashKeyCost := costusage.NewCostVer2(option, cpuFactor,
		buildRows*nKeys*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashkey(%v*%v*%v)", buildRows, nKeys, cpuFactor) })
	hashMemCost := costusage.NewCostVer2(option, memFactor,
		buildRows*buildRowSize*memFactor.Value,
		func() string { return fmt.Sprintf("hashmem(%v*%v*%v)", buildRows, buildRowSize, memFactor) })
	hashBuildCost := costusage.NewCostVer2(option, cpuFactor,
		buildRows*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashbuild(%v*%v)", buildRows, cpuFactor) })
	return costusage.SumCostVer2(hashKeyCost, hashMemCost, hashBuildCost)
}

func hashProbeCostVer2(option *optimizetrace.PlanCostOption, probeRows, nKeys float64, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	// TODO: 1) consider types of keys, 2) dedicated factor for build-probe hash table
	hashKeyCost := costusage.NewCostVer2(option, cpuFactor,
		probeRows*nKeys*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashkey(%v*%v*%v)", probeRows, nKeys, cpuFactor) })
	hashProbeCost := costusage.NewCostVer2(option, cpuFactor,
		probeRows*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashprobe(%v*%v)", probeRows, cpuFactor) })
	return costusage.SumCostVer2(hashKeyCost, hashProbeCost)
}

// For simplicity and robust, only operators that need double-read like IndexLookup and IndexJoin consider this cost.
func doubleReadCostVer2(option *optimizetrace.PlanCostOption, numTasks float64, requestFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, requestFactor,
		numTasks*requestFactor.Value,
		func() string { return fmt.Sprintf("doubleRead(tasks(%v)*%v)", numTasks, requestFactor) })
}

// In Cost Ver2, we hide cost factors from users and deprecate SQL variables like `tidb_opt_scan_factor`.
type costVer2Factors struct {
	TiDBTemp      costusage.CostVer2Factor // operations on TiDB temporary table
	TiKVScan      costusage.CostVer2Factor // per byte
	TiKVDescScan  costusage.CostVer2Factor // per byte
	TiFlashScan   costusage.CostVer2Factor // per byte
	TiDBCPU       costusage.CostVer2Factor // per column or expression
	TiKVCPU       costusage.CostVer2Factor // per column or expression
	TiFlashCPU    costusage.CostVer2Factor // per column or expression
	TiDB2KVNet    costusage.CostVer2Factor // per byte
	TiDB2FlashNet costusage.CostVer2Factor // per byte
	TiFlashMPPNet costusage.CostVer2Factor // per byte
	TiDBMem       costusage.CostVer2Factor // per byte
	TiKVMem       costusage.CostVer2Factor // per byte
	TiFlashMem    costusage.CostVer2Factor // per byte
	TiDBDisk      costusage.CostVer2Factor // per byte
	TiDBRequest   costusage.CostVer2Factor // per net request
}

func (c costVer2Factors) tolist() (l []costusage.CostVer2Factor) {
	return append(l, c.TiDBTemp, c.TiKVScan, c.TiKVDescScan, c.TiFlashScan, c.TiDBCPU, c.TiKVCPU, c.TiFlashCPU,
		c.TiDB2KVNet, c.TiDB2FlashNet, c.TiFlashMPPNet, c.TiDBMem, c.TiKVMem, c.TiFlashMem, c.TiDBDisk, c.TiDBRequest)
}

var defaultVer2Factors = costVer2Factors{
	TiDBTemp:      costusage.CostVer2Factor{Name: "tidb_temp_table_factor", Value: 0.00},
	TiKVScan:      costusage.CostVer2Factor{Name: "tikv_scan_factor", Value: 40.70},
	TiKVDescScan:  costusage.CostVer2Factor{Name: "tikv_desc_scan_factor", Value: 61.05},
	TiFlashScan:   costusage.CostVer2Factor{Name: "tiflash_scan_factor", Value: 11.60},
	TiDBCPU:       costusage.CostVer2Factor{Name: "tidb_cpu_factor", Value: 49.90},
	TiKVCPU:       costusage.CostVer2Factor{Name: "tikv_cpu_factor", Value: 49.90},
	TiFlashCPU:    costusage.CostVer2Factor{Name: "tiflash_cpu_factor", Value: 2.40},
	TiDB2KVNet:    costusage.CostVer2Factor{Name: "tidb_kv_net_factor", Value: 3.96},
	TiDB2FlashNet: costusage.CostVer2Factor{Name: "tidb_flash_net_factor", Value: 2.20},
	TiFlashMPPNet: costusage.CostVer2Factor{Name: "tiflash_mpp_net_factor", Value: 1.00},
	TiDBMem:       costusage.CostVer2Factor{Name: "tidb_mem_factor", Value: 0.20},
	TiKVMem:       costusage.CostVer2Factor{Name: "tikv_mem_factor", Value: 0.20},
	TiFlashMem:    costusage.CostVer2Factor{Name: "tiflash_mem_factor", Value: 0.05},
	TiDBDisk:      costusage.CostVer2Factor{Name: "tidb_disk_factor", Value: 200.00},
	TiDBRequest:   costusage.CostVer2Factor{Name: "tidb_request_factor", Value: 6000000.00},
}

func getTaskCPUFactorVer2(_ base.PhysicalPlan, taskType property.TaskType) costusage.CostVer2Factor {
	switch taskType {
	case property.RootTaskType: // TiDB
		return defaultVer2Factors.TiDBCPU
	case property.MppTaskType: // TiFlash
		return defaultVer2Factors.TiFlashCPU
	default: // TiKV
		return defaultVer2Factors.TiKVCPU
	}
}

func getTaskMemFactorVer2(_ base.PhysicalPlan, taskType property.TaskType) costusage.CostVer2Factor {
	switch taskType {
	case property.RootTaskType: // TiDB
		return defaultVer2Factors.TiDBMem
	case property.MppTaskType: // TiFlash
		return defaultVer2Factors.TiFlashMem
	default: // TiKV
		return defaultVer2Factors.TiKVMem
	}
}

func getTaskScanFactorVer2(p base.PhysicalPlan, storeType kv.StoreType, taskType property.TaskType) costusage.CostVer2Factor {
	if isTemporaryTable(getTableInfo(p)) {
		return defaultVer2Factors.TiDBTemp
	}
	if storeType == kv.TiFlash {
		return defaultVer2Factors.TiFlashScan
	}
	switch taskType {
	case property.MppTaskType: // TiFlash
		return defaultVer2Factors.TiFlashScan
	default: // TiKV
		var desc bool
		if indexScan, ok := p.(*PhysicalIndexScan); ok {
			desc = indexScan.Desc
		}
		if tableScan, ok := p.(*PhysicalTableScan); ok {
			desc = tableScan.Desc
		}
		if desc {
			return defaultVer2Factors.TiKVDescScan
		}
		return defaultVer2Factors.TiKVScan
	}
}

func getTaskNetFactorVer2(p base.PhysicalPlan, _ property.TaskType) costusage.CostVer2Factor {
	if isTemporaryTable(getTableInfo(p)) {
		return defaultVer2Factors.TiDBTemp
	}
	if _, ok := p.(*PhysicalExchangeReceiver); ok { // TiFlash MPP
		return defaultVer2Factors.TiFlashMPPNet
	}
	if tblReader, ok := p.(*PhysicalTableReader); ok {
		if _, isMPP := tblReader.tablePlan.(*PhysicalExchangeSender); isMPP { // TiDB to TiFlash with mpp protocol
			return defaultVer2Factors.TiDB2FlashNet
		}
	}
	return defaultVer2Factors.TiDB2KVNet
}

func getTaskRequestFactorVer2(p base.PhysicalPlan, _ property.TaskType) costusage.CostVer2Factor {
	if isTemporaryTable(getTableInfo(p)) {
		return defaultVer2Factors.TiDBTemp
	}
	return defaultVer2Factors.TiDBRequest
}

func isTemporaryTable(tbl *model.TableInfo) bool {
	return tbl != nil && tbl.TempTableType != model.TempTableNone
}

func getTableInfo(p base.PhysicalPlan) *model.TableInfo {
	switch x := p.(type) {
	case *PhysicalIndexReader:
		return getTableInfo(x.indexPlan)
	case *PhysicalTableReader:
		return getTableInfo(x.tablePlan)
	case *PhysicalIndexLookUpReader:
		return getTableInfo(x.tablePlan)
	case *PhysicalIndexMergeReader:
		if x.tablePlan != nil {
			return getTableInfo(x.tablePlan)
		}
		return getTableInfo(x.partialPlans[0])
	case *PhysicalTableScan:
		return x.Table
	case *PhysicalIndexScan:
		return x.Table
	default:
		if len(x.Children()) == 0 {
			return nil
		}
		return getTableInfo(x.Children()[0])
	}
}

func cols2Exprs(cols []*expression.Column) []expression.Expression {
	exprs := make([]expression.Expression, 0, len(cols))
	for _, c := range cols {
		exprs = append(exprs, c)
	}
	return exprs
}
