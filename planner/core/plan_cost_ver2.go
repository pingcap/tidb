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
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + filter-cost
func (p *PhysicalSelection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	inputRows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	filterCost := filterCostVer2(inputRows, p.Conditions, cpuFactor)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = filterCost + childCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + proj-cost / concurrency
// proj-cost = input-rows * len(expressions) * cpu-factor
func (p *PhysicalProjection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	inputRows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().ProjectionConcurrency())

	projCost := inputRows * float64(len(p.Exprs)) * cpuFactor.Value

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + projCost/concurrency
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalIndexScan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p, option.CostFlag)
	rowSize := math.Max(p.getScanRowSize(), 2.0)
	scanFactor := getTaskScanFactorVer2(p, taskType)

	p.planCost = scanCostVer2(rows, rowSize, scanFactor)
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalTableScan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p, option.CostFlag)
	rowSize := math.Max(p.getScanRowSize(), 2.0)
	scanFactor := getTaskScanFactorVer2(p, taskType)

	p.planCost = scanCostVer2(rows, rowSize, scanFactor)

	// give TiFlash a start-up cost to let the optimizer prefers to use TiKV to process small table scans.
	if p.StoreType == kv.TiFlash {
		p.planCost += scanCostVer2(10000, rowSize, scanFactor)
	}

	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost + seek-cost) / concurrency
// net-cost = rows * row-size * net-factor
// seek-cost = num-tasks * seek-factor
func (p *PhysicalIndexReader) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p.indexPlan, option.CostFlag)
	rowSize := getAvgRowSize(p.indexPlan.Stats(), p.indexPlan.Schema())
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	netCost := netCostVer2(rows, rowSize, netFactor)
	seekCost := estimateNetSeekCost(p.indexPlan)

	childCost, err := p.indexPlan.GetPlanCost(property.CopSingleReadTaskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = (childCost + netCost + seekCost) / concurrency
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost + seek-cost) / concurrency
// net-cost = rows * row-size * net-factor
// seek-cost = num-tasks * seek-factor
func (p *PhysicalTableReader) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p.tablePlan, option.CostFlag)
	rowSize := getAvgRowSize(p.tablePlan.Stats(), p.tablePlan.Schema())
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	netCost := netCostVer2(rows, rowSize, netFactor)
	seekCost := estimateNetSeekCost(p.tablePlan)

	childCost, err := p.tablePlan.GetPlanCost(property.CopSingleReadTaskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = (childCost + netCost + seekCost) / concurrency
	p.planCostInit = true

	// consider tidb_enforce_mpp
	_, isMPP := p.tablePlan.(*PhysicalExchangeSender)
	if isMPP && p.ctx.GetSessionVars().IsMPPEnforced() &&
		!hasCostFlag(option.CostFlag, CostFlagRecalculate) { // show the real cost in explain-statements
		p.planCost /= 1000000000
	}
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = index-side-cost + (table-side-cost + double-read-cost) / double-read-concurrency
// index-side-cost = (index-child-cost + index-net-cost + index-seek-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost + table-seek-cost) / dist-concurrency # same with TableReader
// double-read-cost = double-read-seek-cost + double-read-cpu-cost
// double-read-seek-cost = double-read-tasks * seek-factor
// double-read-cpu-cost = index-rows * cpu-factor
// double-read-tasks = index-rows / batch-size * task-per-batch # task-per-batch is a magic number now
func (p *PhysicalIndexLookUpReader) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	indexRows := getCardinality(p.indexPlan, option.CostFlag)
	tableRows := getCardinality(p.indexPlan, option.CostFlag)
	indexRowSize := getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	tableRowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	netFactor := getTaskNetFactorVer2(p, taskType)
	seekFactor := getTaskSeekFactorVer2(p, taskType)
	distConcurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	doubleReadConcurrency := float64(p.ctx.GetSessionVars().IndexLookupConcurrency())

	// index-side
	indexNetCost := netCostVer2(indexRows, indexRowSize, netFactor)
	indexSeekCost := estimateNetSeekCost(p.indexPlan)
	indexChildCost, err := p.indexPlan.GetPlanCost(property.CopDoubleReadTaskType, option)
	if err != nil {
		return 0, err
	}
	indexSideCost := (indexNetCost + indexSeekCost + indexChildCost) / distConcurrency

	// table-side
	tableNetCost := netCostVer2(tableRows, tableRowSize, netFactor)
	tableSeekCost := estimateNetSeekCost(p.tablePlan)
	tableChildCost, err := p.tablePlan.GetPlanCost(property.CopDoubleReadTaskType, option)
	if err != nil {
		return 0, err
	}
	tableSideCost := (tableNetCost + tableSeekCost + tableChildCost) / distConcurrency

	// double-read
	doubleReadCPUCost := indexRows * cpuFactor.Value
	batchSize := float64(p.ctx.GetSessionVars().IndexLookupSize)
	taskPerBatch := 40.0 // TODO: remove this magic number
	doubleReadTasks := indexRows / batchSize * taskPerBatch
	doubleReadSeekCost := doubleReadTasks * seekFactor.Value
	doubleReadCost := doubleReadCPUCost + doubleReadSeekCost

	p.planCost = indexSideCost + (tableSideCost+doubleReadCost)/doubleReadConcurrency
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = table-side-cost + sum(index-side-cost)
// index-side-cost = (index-child-cost + index-net-cost + index-seek-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost + table-seek-cost) / dist-concurrency # same with TableReader
func (p *PhysicalIndexMergeReader) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	netFactor := getTaskNetFactorVer2(p, taskType)
	distConcurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	var tableSideCost float64
	if tablePath := p.tablePlan; tablePath != nil {
		rows := getCardinality(tablePath, option.CostFlag)
		rowSize := getAvgRowSize(tablePath.Stats(), tablePath.Schema())

		tableNetCost := netCostVer2(rows, rowSize, netFactor)
		tableSeekCost := estimateNetSeekCost(tablePath)
		tableChildCost, err := tablePath.GetPlanCost(taskType, option)
		if err != nil {
			return 0, err
		}
		tableSideCost = (tableNetCost + tableSeekCost + tableChildCost) / distConcurrency
	}

	var sumIndexSideCost float64
	for _, indexPath := range p.partialPlans {
		rows := getCardinality(indexPath, option.CostFlag)
		rowSize := getAvgRowSize(indexPath.Stats(), indexPath.Schema())

		indexNetCost := netCostVer2(rows, rowSize, netFactor)
		indexSeekCost := estimateNetSeekCost(indexPath)
		indexChildCost, err := indexPath.GetPlanCost(taskType, option)
		if err != nil {
			return 0, err
		}
		sumIndexSideCost += (indexNetCost + indexSeekCost + indexChildCost) / distConcurrency
	}

	p.planCost = tableSideCost + sumIndexSideCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + sort-cpu-cost + sort-mem-cost + sort-disk-cost
// sort-cpu-cost = rows * log2(rows) * len(sort-items) * cpu-factor
// if no spill:
// 1. sort-mem-cost = rows * row-size * mem-factor
// 2. sort-disk-cost = 0
// else if spill:
// 1. sort-mem-cost = mem-quota * mem-factor
// 2. sort-disk-cost = rows * row-size * disk-factor
func (p *PhysicalSort) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := math.Max(getCardinality(p.children[0], option.CostFlag), 1)
	rowSize := getAvgRowSize(p.statsInfo(), p.Schema())
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	diskFactor := defaultVer2Factors.TiDBDisk
	oomUseTmpStorage := variable.EnableTmpStorageOnOOM.Load()
	memQuota := p.ctx.GetSessionVars().StmtCtx.MemTracker.GetBytesLimit()
	spill := taskType == property.RootTaskType && // only TiDB can spill
		oomUseTmpStorage && // spill is enabled
		memQuota > 0 && // mem-quota is set
		rowSize*rows > float64(memQuota) // exceed the mem-quota

	sortCPUCost := rows * math.Log2(rows) * float64(len(p.ByItems)) * cpuFactor.Value

	var sortMemCost, sortDiskCost float64
	if !spill {
		sortMemCost = rows * rowSize * memFactor.Value
		sortDiskCost = 0
	} else {
		sortMemCost = float64(memQuota) * memFactor.Value
		sortDiskCost = rows * rowSize * diskFactor.Value
	}

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + sortCPUCost + sortMemCost + sortDiskCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + topn-cpu-cost + topn-mem-cost
// topn-cpu-cost = rows * log2(N) * len(sort-items) * cpu-factor
// topn-mem-cost = N * row-size * mem-factor
func (p *PhysicalTopN) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p.children[0], option.CostFlag)
	N := math.Max(1, float64(p.Count+p.Offset))
	rowSize := getAvgRowSize(p.statsInfo(), p.Schema())
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)

	topNCPUCost := rows * math.Log2(N) * float64(len(p.ByItems)) * cpuFactor.Value
	topNMemCost := N * rowSize * memFactor.Value

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + topNCPUCost + topNMemCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + agg-cost + group-cost
func (p *PhysicalStreamAgg) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	aggCost := aggCostVer2(rows, p.AggFuncs, cpuFactor)
	groupCost := groupCostVer2(rows, p.GroupByItems, cpuFactor)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + aggCost + groupCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + (agg-cost + group-cost + hash-build-cost + hash-probe-cost) / concurrency
func (p *PhysicalHashAgg) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	inputRows := getCardinality(p.children[0], option.CostFlag)
	outputRows := getCardinality(p, option.CostFlag)
	outputRowSize := getAvgRowSize(p.Stats(), p.Schema())
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	concurrency := p.ctx.GetSessionVars().GetConcurrencyFactor()

	aggCost := aggCostVer2(inputRows, p.AggFuncs, cpuFactor)
	groupCost := groupCostVer2(inputRows, p.GroupByItems, cpuFactor)
	hashBuildCost := hashBuildCostVer2(outputRows, outputRowSize, p.GroupByItems, cpuFactor, memFactor)
	hashProbeCost := hashProbeCostVer2(inputRows, p.GroupByItems, cpuFactor)

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + (aggCost+groupCost+hashBuildCost+hashProbeCost)/concurrency
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
func (p *PhysicalMergeJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	leftRows := getCardinality(p.children[0], option.CostFlag)
	rightRows := getCardinality(p.children[1], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	filterCost := filterCostVer2(leftRows, p.LeftConditions, cpuFactor) +
		filterCostVer2(rightRows, p.RightConditions, cpuFactor)
	groupCost := groupCostVer2(leftRows, cols2Exprs(p.LeftJoinKeys), cpuFactor) +
		groupCostVer2(rightRows, cols2Exprs(p.LeftJoinKeys), cpuFactor)

	leftChildCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	rightChildCost, err := p.children[1].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = leftChildCost + rightChildCost + filterCost + groupCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
func (p *PhysicalHashJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	build, probe := p.children[0], p.children[1]
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	buildKeys, probeKeys := p.LeftJoinKeys, p.RightJoinKeys
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		build, probe = probe, build
		buildFilters, probeFilters = probeFilters, buildFilters
	}
	buildRows := getCardinality(build, option.CostFlag)
	probeRows := getCardinality(probe, option.CostFlag)
	buildRowSize := getAvgRowSize(build.Stats(), build.Schema())
	concurrency := float64(p.Concurrency)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(buildRows, buildFilters, cpuFactor)
	buildHashCost := hashBuildCostVer2(buildRows, buildRowSize, cols2Exprs(buildKeys), cpuFactor, memFactor)

	probeFilterCost := filterCostVer2(probeRows, probeFilters, cpuFactor)
	probeHashCost := hashProbeCostVer2(probeRows, cols2Exprs(probeKeys), cpuFactor)

	buildChildCost, err := build.GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	probeChildCost, err := probe.GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = buildChildCost + probeChildCost + buildHashCost + buildFilterCost +
		(probeFilterCost+probeHashCost)/concurrency
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost +
// (probe-cost + probe-filter-cost) / concurrency
// probe-cost = probe-child-cost * build-rows / batchRatio
func (p *PhysicalIndexJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	build, probe := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	buildRows := getCardinality(build, option.CostFlag)
	probeRowsOne := getCardinality(probe, option.CostFlag)
	probeRowsTot := probeRowsOne * buildRows
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	probeConcurrency := float64(p.ctx.GetSessionVars().IndexLookupJoinConcurrency())
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(buildRows, buildFilters, cpuFactor)
	buildChildCost, err := build.GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	probeFilterCost := filterCostVer2(probeRowsTot, probeFilters, cpuFactor)
	probeChildCost, err := probe.GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	// IndexJoin executes a batch of rows at a time, so the actual cost of this part should be
	//  `innerCostPerBatch * numberOfBatches` instead of `innerCostPerRow * numberOfOuterRow`.
	// Use an empirical value batchRatio to handle this now.
	// TODO: remove this empirical value.
	batchRatio := 30.0
	probeCost := probeChildCost * buildRows / batchRatio

	p.planCost = buildChildCost + buildFilterCost + (probeCost+probeFilterCost)/probeConcurrency
	p.planCostInit = true
	return p.planCost, nil
}

func (p *PhysicalIndexHashJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	// TODO: distinguish IndexHashJoin with IndexJoin
	return p.PhysicalIndexJoin.getPlanCostVer2(taskType, option)
}

func (p *PhysicalIndexMergeJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	// TODO: distinguish IndexMergeJoin with IndexJoin
	return p.PhysicalIndexJoin.getPlanCostVer2(taskType, option)
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func (p *PhysicalApply) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	buildRows := getCardinality(p.children[0], option.CostFlag)
	probeRowsOne := getCardinality(p.children[1], option.CostFlag)
	probeRowsTot := buildRows * probeRowsOne
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(buildRows, p.LeftConditions, cpuFactor)
	buildChildCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	probeFilterCost := filterCostVer2(probeRowsTot, p.RightConditions, cpuFactor)
	probeChildCost, err := p.children[1].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}
	probeCost := probeChildCost * buildRows

	p.planCost = buildChildCost + buildFilterCost + probeCost + probeFilterCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + net-cost
func (p *PhysicalExchangeReceiver) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.stats, p.Schema())
	netFactor := getTaskNetFactorVer2(p, taskType)

	netCost := netCostVer2(rows, rowSize, netFactor)
	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + netCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = seek-cost + net-cost
func (p *PointGetPlan) getPlanCostVer2(taskType property.TaskType, _ *PlanCostOption) (float64, error) {
	if p.accessCols == nil { // from fast plan code path
		p.planCost = 0
		p.planCostInit = true
		return 0, nil
	}
	rowSize := getAvgRowSize(p.stats, p.schema)
	netFactor := getTaskNetFactorVer2(p, taskType)
	seekFactor := getTaskSeekFactorVer2(p, taskType)

	netCost := netCostVer2(1, rowSize, netFactor)
	seekCost := 1 * seekFactor.Value / 20 // 20 times faster than general request

	p.planCost = netCost + seekCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = seek-cost + net-cost
func (p *BatchPointGetPlan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	if p.accessCols == nil { // from fast plan code path
		p.planCost = 0
		p.planCostInit = true
		return 0, nil
	}
	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.stats, p.schema)
	netFactor := getTaskNetFactorVer2(p, taskType)
	seekFactor := getTaskSeekFactorVer2(p, taskType)

	netCost := netCostVer2(rows, rowSize, netFactor)
	seekCost := 1 * seekFactor.Value / 20 // in one batch

	p.planCost = netCost + seekCost
	p.planCostInit = true
	return p.planCost, nil
}

func scanCostVer2(rows, rowSize float64, scanFactor costVer2Factor) float64 {
	// log2 from experiments
	return rows * math.Log2(math.Max(1, rowSize)) * scanFactor.Value
}

func netCostVer2(rows, rowSize float64, netFactor costVer2Factor) float64 {
	return rows * rowSize * netFactor.Value
}

func filterCostVer2(rows float64, filters []expression.Expression, cpuFactor costVer2Factor) float64 {
	// TODO: consider types of filters
	return rows * float64(len(filters)) * cpuFactor.Value
}

func aggCostVer2(rows float64, aggFuncs []*aggregation.AggFuncDesc, cpuFactor costVer2Factor) float64 {
	// TODO: consider types of agg-funcs
	return rows * float64(len(aggFuncs)) * cpuFactor.Value
}

func groupCostVer2(rows float64, groupItems []expression.Expression, cpuFactor costVer2Factor) float64 {
	return rows * float64(len(groupItems)) * cpuFactor.Value
}

func hashBuildCostVer2(buildRows, buildRowSize float64, keys []expression.Expression, cpuFactor, memFactor costVer2Factor) float64 {
	// TODO: 1) consider types of keys, 2) dedicated factor for build-probe hash table
	hashKeyCost := buildRows * float64(len(keys)) * cpuFactor.Value
	hashMemCost := buildRows * buildRowSize * memFactor.Value
	hashBuildCost := buildRows * float64(len(keys)) * cpuFactor.Value
	return hashKeyCost + hashMemCost + hashBuildCost
}

func hashProbeCostVer2(probeRows float64, keys []expression.Expression, cpuFactor costVer2Factor) float64 {
	// TODO: 1) consider types of keys, 2) dedicated factor for build-probe hash table
	hashKeyCost := probeRows * float64(len(keys)) * cpuFactor.Value
	hashProbeCost := probeRows * float64(len(keys)) * cpuFactor.Value
	return hashKeyCost + hashProbeCost
}

type costVer2Factor struct {
	Name  string
	Value float64
}

// In Cost Ver2, we hide cost factors from users and deprecate SQL variables like `tidb_opt_scan_factor`.
type costVer2Factors struct {
	TiDBTemp      costVer2Factor // operations on TiDB temporary table
	TiKVScan      costVer2Factor // per byte
	TiKVDescScan  costVer2Factor // per byte
	TiFlashScan   costVer2Factor // per byte
	TiDBCPU       costVer2Factor // per column or expression
	TiKVCPU       costVer2Factor // per column or expression
	TiFlashCPU    costVer2Factor // per column or expression
	TiDB2KVNet    costVer2Factor // per byte
	TiDB2FlashNet costVer2Factor // per byte
	TiFlashMPPNet costVer2Factor // per byte
	TiDBMem       costVer2Factor // per byte
	TiKVMem       costVer2Factor // per byte
	TiFlashMem    costVer2Factor // per byte
	TiDBDisk      costVer2Factor // per byte
	TiDBRequest   costVer2Factor // per net request
}

var defaultVer2Factors = costVer2Factors{
	TiDBTemp:      costVer2Factor{"tidb_temp_table_factor", 0},
	TiKVScan:      costVer2Factor{"tikv_scan_factor", 100},
	TiKVDescScan:  costVer2Factor{"tikv_desc_scan_factor", 150},
	TiFlashScan:   costVer2Factor{"tiflash_scan_factor", 5},
	TiDBCPU:       costVer2Factor{"tidb_cpu_factor", 30},
	TiKVCPU:       costVer2Factor{"tikv_cpu_factor", 30},
	TiFlashCPU:    costVer2Factor{"tiflash_cpu_factor", 5},
	TiDB2KVNet:    costVer2Factor{"tidb_kv_net_factor", 8},
	TiDB2FlashNet: costVer2Factor{"tidb_flash_net_factor", 4},
	TiFlashMPPNet: costVer2Factor{"tiflash_mpp_net_factor", 4},
	TiDBMem:       costVer2Factor{"tidb_mem_factor", 1},
	TiKVMem:       costVer2Factor{"tikv_mem_factor", 1},
	TiFlashMem:    costVer2Factor{"tiflash_mem_factor", 1},
	TiDBDisk:      costVer2Factor{"tidb_disk_factor", 1000},
	TiDBRequest:   costVer2Factor{"tidb_request_factor", 9500000},
}

func getTaskCPUFactorVer2(p PhysicalPlan, taskType property.TaskType) costVer2Factor {
	switch taskType {
	case property.RootTaskType: // TiDB
		return defaultVer2Factors.TiDBCPU
	case property.MppTaskType: // TiFlash
		return defaultVer2Factors.TiFlashCPU
	default: // TiKV
		return defaultVer2Factors.TiKVCPU
	}
}

func getTaskMemFactorVer2(p PhysicalPlan, taskType property.TaskType) costVer2Factor {
	switch taskType {
	case property.RootTaskType: // TiDB
		return defaultVer2Factors.TiDBMem
	case property.MppTaskType: // TiFlash
		return defaultVer2Factors.TiFlashMem
	default: // TiKV
		return defaultVer2Factors.TiKVMem
	}
}

func getTaskScanFactorVer2(p PhysicalPlan, taskType property.TaskType) costVer2Factor {
	if isTemporaryTable(getTableInfo(p)) {
		return defaultVer2Factors.TiDBTemp
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

func getTaskNetFactorVer2(p PhysicalPlan, _ property.TaskType) costVer2Factor {
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

func getTaskSeekFactorVer2(p PhysicalPlan, _ property.TaskType) costVer2Factor {
	if isTemporaryTable(getTableInfo(p)) {
		return defaultVer2Factors.TiDBTemp
	}
	return defaultVer2Factors.TiDBRequest
}

func isTemporaryTable(tbl *model.TableInfo) bool {
	return tbl != nil && tbl.TempTableType != model.TempTableNone
}

func getTableInfo(p PhysicalPlan) *model.TableInfo {
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
