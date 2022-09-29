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

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + sel-cost
// sel-cost = input-rows * len(conditions) * cpu-factor
func (p *PhysicalSelection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	inputRows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactor(p, taskType)
	selCost := inputRows * float64(len(p.Conditions)) * cpuFactor

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = selCost + childCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + proj-cost / concurrency
// proj-cost = input-rows * len(expressions) * cpu-factor
func (p *PhysicalProjection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	inputRows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactor(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().ProjectionConcurrency())

	projCost := inputRows * float64(len(p.Exprs)) * cpuFactor

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
	scanFactor := getTaskScanFactor(p, taskType)
	rowSize := math.Max(p.getScanRowSize(), 2.0)
	logRowSize := math.Log2(rowSize)

	p.planCost = rows * logRowSize * scanFactor
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalTableScan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	rows := getCardinality(p, option.CostFlag)
	scanFactor := getTaskScanFactor(p, taskType)
	rowSize := math.Max(p.getScanRowSize(), 2.0)
	logRowSize := math.Log2(rowSize)

	p.planCost = rows * logRowSize * scanFactor

	// give TiFlash a start-up cost to let the optimizer prefers to use TiKV to process small table scans.
	if p.StoreType == kv.TiFlash {
		p.planCost += 10000 * logRowSize * scanFactor
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
	netFactor := getTaskNetFactor(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	netCost := rows * rowSize * netFactor
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
	netFactor := getTaskNetFactor(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	netCost := rows * rowSize * netFactor
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
	cpuFactor := getTaskCPUFactor(p, taskType)
	netFactor := getTaskNetFactor(p, taskType)
	seekFactor := getTaskSeekFactor(p, taskType)
	distConcurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	doubleReadConcurrency := float64(p.ctx.GetSessionVars().IndexLookupConcurrency())

	// index-side
	indexNetCost := indexRows * indexRowSize * netFactor
	indexSeekCost := estimateNetSeekCost(p.indexPlan)
	indexChildCost, err := p.indexPlan.GetPlanCost(property.CopDoubleReadTaskType, option)
	if err != nil {
		return 0, err
	}
	indexSideCost := (indexNetCost + indexSeekCost + indexChildCost) / distConcurrency

	// table-side
	tableNetCost := tableRows * tableRowSize * netFactor
	tableSeekCost := estimateNetSeekCost(p.tablePlan)
	tableChildCost, err := p.tablePlan.GetPlanCost(property.CopDoubleReadTaskType, option)
	if err != nil {
		return 0, err
	}
	tableSideCost := (tableNetCost + tableSeekCost + tableChildCost) / distConcurrency

	// double-read
	doubleReadCPUCost := indexRows * cpuFactor
	batchSize := float64(p.ctx.GetSessionVars().IndexLookupSize)
	taskPerBatch := 40.0 // TODO: remove this magic number
	doubleReadTasks := indexRows / batchSize * taskPerBatch
	doubleReadSeekCost := doubleReadTasks * seekFactor
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
	netFactor := getTaskNetFactor(p, taskType)
	distConcurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	var tableSideCost float64
	if tablePath := p.tablePlan; tablePath != nil {
		rows := getCardinality(tablePath, option.CostFlag)
		rowSize := getAvgRowSize(tablePath.Stats(), tablePath.Schema())

		tableNetCost := rows * rowSize * netFactor
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

		indexNetCost := rows * rowSize * netFactor
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
	cpuFactor := getTaskCPUFactor(p, taskType)
	memFactor := getTaskMemFactor(p, taskType)
	diskFactor := p.ctx.GetSessionVars().GetDiskFactor()
	oomUseTmpStorage := variable.EnableTmpStorageOnOOM.Load()
	memQuota := p.ctx.GetSessionVars().StmtCtx.MemTracker.GetBytesLimit()
	spill := taskType == property.RootTaskType && // only TiDB can spill
		oomUseTmpStorage && // spill is enabled
		memQuota > 0 && // mem-quota is set
		rowSize*rows > float64(memQuota) // exceed the mem-quota

	sortCPUCost := rows * math.Log2(rows) * float64(len(p.ByItems)) * cpuFactor

	var sortMemCost, sortDiskCost float64
	if !spill {
		sortMemCost = rows * rowSize * memFactor
		sortDiskCost = 0
	} else {
		sortMemCost = float64(memQuota) * memFactor
		sortDiskCost = rows * rowSize * diskFactor
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
	cpuFactor := getTaskCPUFactor(p, taskType)
	memFactor := getTaskMemFactor(p, taskType)

	topNCPUCost := rows * math.Log2(N) * float64(len(p.ByItems)) * cpuFactor
	topNMemCost := N * rowSize * memFactor

	childCost, err := p.children[0].GetPlanCost(taskType, option)
	if err != nil {
		return 0, err
	}

	p.planCost = childCost + topNCPUCost + topNMemCost
	p.planCostInit = true
	return p.planCost, nil
}

func getTaskCPUFactor(p PhysicalPlan, taskType property.TaskType) float64 {
	switch taskType {
	case property.RootTaskType: // TiDB
		return p.SCtx().GetSessionVars().GetCPUFactor()
	case property.MppTaskType: // TiFlash
		return p.SCtx().GetSessionVars().GetTiFlashCPUFactor()
	default: // TiKV
		return p.SCtx().GetSessionVars().GetCopCPUFactor()
	}
}

func getTaskMemFactor(p PhysicalPlan, taskType property.TaskType) float64 {
	// TODO: introduce a dedicated mem factor for TiFlash
	return p.SCtx().GetSessionVars().GetMemoryFactor()
}

func getTaskScanFactor(p PhysicalPlan, taskType property.TaskType) float64 {
	switch taskType {
	case property.MppTaskType: // TiFlash
		return p.SCtx().GetSessionVars().GetTiFlashScanFactor()
	default: // TiKV
		var desc bool
		var tbl *model.TableInfo
		if indexScan, ok := p.(*PhysicalIndexScan); ok {
			desc = indexScan.Desc
			tbl = indexScan.Table
		}
		if tableScan, ok := p.(*PhysicalTableScan); ok {
			desc = tableScan.Desc
			tbl = tableScan.Table
		}
		if desc {
			return p.SCtx().GetSessionVars().GetDescScanFactor(tbl)
		}
		return p.SCtx().GetSessionVars().GetScanFactor(tbl)
	}
}

func getTaskNetFactor(p PhysicalPlan, _ property.TaskType) float64 {
	// TODO: introduce a dedicated net factor for TiFlash
	return p.SCtx().GetSessionVars().GetNetworkFactor(getTableInfo(p))
}

func getTaskSeekFactor(p PhysicalPlan, _ property.TaskType) float64 {
	return p.SCtx().GetSessionVars().GetSeekFactor(getTableInfo(p))
}

func getTableInfo(p PhysicalPlan) *model.TableInfo {
	switch x := p.(type) {
	case *PhysicalIndexReader:
		return getTableInfo(x.indexPlan)
	case *PhysicalTableReader:
		return getTableInfo(x.tablePlan)
	case *PhysicalIndexLookUpReader:
		return getTableInfo(x.tablePlan)
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
