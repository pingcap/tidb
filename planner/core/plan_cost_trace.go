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
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/paging"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
)

func buildPlanCostDetail(tp string, id int, cost costVer2) *tracing.PhysicalPlanCostDetail {
	d := &tracing.PhysicalPlanCostDetail{}
	d.TP = tp
	d.ID = id
	d.Desc = cost.trace.formula
	d.Params = make(map[string]interface{})
	for name, value := range cost.trace.factorCosts {
		d.Params[name] = value
	}
	for name, param := range cost.trace.costParams {
		d.Params[name] = param
	}
	return d
}

func genPlanCostTrace(p PhysicalPlan, cost float64, taskType property.TaskType, option *PlanCostOption) error {
	ret, _ := getPlanCostTrace(p, taskType, option)

	if ret.GetCost() != cost {
		panic("costVer2 not equal with cost")
	}

	//p.planCostVer2.SetName(p.ExplainID().String())
	//p.planCostInit = true

	if option.tracer != nil {
		option.tracer.appendPlanCostDetail(buildPlanCostDetail(p.TP(), p.ID(), ret))
	}
	return nil
}

func getPlanCostTrace(p PhysicalPlan, taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	var ret costVer2
	var err error
	switch p.(type) {
	case *basePhysicalPlan:
		ret, err = p.(*basePhysicalPlan).getPlanCostTrace(taskType, option)
	case *PhysicalSelection:
		ret, err = p.(*PhysicalSelection).getPlanCostTrace(taskType, option)
	case *PhysicalProjection:
		ret, err = p.(*PhysicalProjection).getPlanCostTrace(taskType, option)
	case *PhysicalIndexScan:
		ret, err = p.(*PhysicalIndexScan).getPlanCostTrace(taskType, option)
	case *PhysicalTableScan:
		ret, err = p.(*PhysicalTableScan).getPlanCostTrace(taskType, option)
	case *PhysicalIndexReader:
		ret, err = p.(*PhysicalIndexReader).getPlanCostTrace(taskType, option)
	case *PhysicalTableReader:
		ret, err = p.(*PhysicalTableReader).getPlanCostTrace(taskType, option)
	case *PhysicalIndexLookUpReader:
		ret, err = p.(*PhysicalIndexLookUpReader).getPlanCostTrace(taskType, option)
	case *PhysicalIndexMergeReader:
		ret, err = p.(*PhysicalIndexMergeReader).getPlanCostTrace(taskType, option)
	case *PhysicalSort:
		ret, err = p.(*PhysicalSort).getPlanCostTrace(taskType, option)
	case *PhysicalTopN:
		ret, err = p.(*PhysicalTopN).getPlanCostTrace(taskType, option)
	case *PhysicalStreamAgg:
		ret, err = p.(*PhysicalStreamAgg).getPlanCostTrace(taskType, option)
	case *PhysicalHashAgg:
		ret, err = p.(*PhysicalHashAgg).getPlanCostTrace(taskType, option)
	case *PhysicalMergeJoin:
		ret, err = p.(*PhysicalMergeJoin).getPlanCostTrace(taskType, option)
	case *PhysicalHashJoin:
		ret, err = p.(*PhysicalHashJoin).getPlanCostTrace(taskType, option)
	case *PhysicalIndexJoin:
		ret, err = p.(*PhysicalIndexJoin).getPlanCostTrace(taskType, option)
	case *PhysicalIndexHashJoin:
		ret, err = p.(*PhysicalIndexHashJoin).getPlanCostTrace(taskType, option)
	case *PhysicalIndexMergeJoin:
		ret, err = p.(*PhysicalIndexMergeJoin).getPlanCostTrace(taskType, option)
	case *PhysicalApply:
		ret, err = p.(*PhysicalApply).getPlanCostTrace(taskType, option)
	case *PhysicalUnionAll:
		ret, err = p.(*PhysicalUnionAll).getPlanCostTrace(taskType, option)
	case *PhysicalExchangeReceiver:
		ret, err = p.(*PhysicalExchangeReceiver).getPlanCostTrace(taskType, option)
	case *PointGetPlan:
		ret, err = p.(*PointGetPlan).getPlanCostTrace(taskType, option)
	case *BatchPointGetPlan:
		ret, err = p.(*BatchPointGetPlan).getPlanCostTrace(taskType, option)
	default:
		ret, err = p.(*basePhysicalPlan).getPlanCostTrace(taskType, option)
	}
	return ret, err
}

// getPlanCostTrace calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *basePhysicalPlan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}
	childCosts := make([]CostItem, 0, len(p.children))
	builder := newCostBuilder(traceCost(option))
	for _, child := range p.children {
		childCost, err := getPlanCostTrace(child, taskType, option)
		if err != nil {
			return zeroCostVer2, err
		}
		childCosts = append(childCosts, &childCost)
	}
	if len(childCosts) == 0 {
		p.planCostVer2 = newZeroCostVer2(true)
	} else {
		p.planCostVer2 = builder.sumAll(childCosts...).Value()
	}
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + filter-cost
func (p *PhysicalSelection) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	builder := newCostBuilder(traceCost(option))

	builder.filterCostVer2(inputRows, p.Conditions, cpuFactor)

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	builder.plus(&childCost).setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + proj-cost / concurrency
// proj-cost = input-rows * len(expressions) * cpu-factor
func (p *PhysicalProjection) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().ProjectionConcurrency()))
	builder := newArgCostBuilder(inputRows, traceCost(option))

	builder.mul(newCostItem("expr_count", numFunctions(p.Exprs))).mul(&cpuFactor).div(concurrency)

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	builder.plus(&childCost).setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalIndexScan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	rowSize := newCostItem("rowSize", math.Max(getAvgRowSize(p.stats, p.schema.Columns), 2.0)) // consider all index columns
	scanFactor := getTaskScanFactorVer2(p, kv.TiKV, taskType)
	builder := newCostBuilder(traceCost(option))

	builder.scanCostVer2(rows, rowSize, scanFactor)
	builder.setName(p.ExplainID().String())

	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalTableScan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", getCardinality(p, option.CostFlag))
	var rowSize float64
	if p.StoreType == kv.TiKV {
		rowSize = getAvgRowSize(p.stats, p.tblCols) // consider all columns if TiKV
	} else { // TiFlash
		rowSize = getAvgRowSize(p.stats, p.schema.Columns)
	}
	myRowSize := newCostItem("rowSize", math.Max(rowSize, 2.0))
	scanFactor := getTaskScanFactorVer2(p, p.StoreType, taskType)
	builder := newCostBuilder(traceCost(option))

	builder.scanCostVer2(rows, myRowSize, scanFactor)
	builder.setName(p.ExplainID().String())

	p.planCostVer2 = builder.Value()

	// give TiFlash a start-up cost to let the optimizer prefers to use TiKV to process small table scans.
	if p.StoreType == kv.TiFlash {
		startRows := newCostItem("10000", 10000)
		builder.scanCostVer2(startRows, myRowSize, scanFactor)
		builder.plus(&p.planCostVer2).setName(p.ExplainID().String())
		p.planCostVer2 = builder.Value()
	}

	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func (p *PhysicalIndexReader) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", getCardinality(p.indexPlan, option.CostFlag))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.schema.Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().DistSQLScanConcurrency()))
	builder := newCostBuilder(traceCost(option))

	builder.netCostVer2(rows, rowSize, netFactor)

	childCost, err := getPlanCostTrace(p.indexPlan, property.CopSingleReadTaskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	builder.plus(&childCost).divA(concurrency).setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func (p *PhysicalTableReader) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", getCardinality(p.tablePlan, option.CostFlag))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.schema.Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().DistSQLScanConcurrency()))
	childType := property.CopSingleReadTaskType
	if p.StoreType == kv.TiFlash { // mpp protocol
		childType = property.MppTaskType
	}

	builder := newCostBuilder(traceCost(option))

	builder.netCostVer2(rows, rowSize, netFactor)

	childCost, err := getPlanCostTrace(p.tablePlan, childType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	builder.plus(&childCost).divA(concurrency).setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true

	// consider tidb_enforce_mpp
	if p.StoreType == kv.TiFlash && p.ctx.GetSessionVars().IsMPPEnforced() &&
		!hasCostFlag(option.CostFlag, CostFlagRecalculate) { // show the real cost in explain-statements
		p.planCostVer2 = builder.divA(newCostItem("1000000000", 1000000000)).setName(p.ExplainID().String()).Value()
	}
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = index-side-cost + (table-side-cost + double-read-cost) / double-read-concurrency
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
// double-read-cost = double-read-request-cost + double-read-cpu-cost
// double-read-request-cost = double-read-tasks * request-factor
// double-read-cpu-cost = index-rows * cpu-factor
// double-read-tasks = index-rows / batch-size * task-per-batch # task-per-batch is a magic number now
func (p *PhysicalIndexLookUpReader) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	indexRows := newCostItem("indexRows", getCardinality(p.indexPlan, option.CostFlag))
	tableRows := newCostItem("tableRows", getCardinality(p.indexPlan, option.CostFlag))
	indexRowSize := newCostItem("indexRowSize", getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false))
	tableRowSize := newCostItem("tableRowSize", getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	netFactor := getTaskNetFactorVer2(p, taskType)
	requestFactor := getTaskRequestFactorVer2(p, taskType)
	distConcurrency := newCostItem("distConcurrency", float64(p.ctx.GetSessionVars().DistSQLScanConcurrency()))
	doubleReadConcurrency := newCostItem("doubleReadConcurrency", float64(p.ctx.GetSessionVars().IndexLookupConcurrency()))
	builder := newCostBuilder(traceCost(option))

	// index-side
	builder.netCostVer2(indexRows, indexRowSize, netFactor)
	indexChildCost, err := getPlanCostTrace(p.indexPlan, property.CopMultiReadTaskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	indexSideCost := builder.plus(&indexChildCost).divA(distConcurrency).setName("indexSideCost").curr

	tableChildCost, err := getPlanCostTrace(p.tablePlan, property.CopMultiReadTaskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	// table-side
	builder.netCostVer2(tableRows, tableRowSize, netFactor)
	tableSideCost := builder.plus(&tableChildCost).divA(distConcurrency).setName("tableSideCost").curr

	doubleReadRows := newCostItem("doubleReadRows", indexRows.GetCost())
	builder.reset(doubleReadRows)
	doubleReadCPUCost := builder.mul(&cpuFactor).curr

	batchSize := float64(p.ctx.GetSessionVars().IndexLookupSize)
	taskPerBatch := 32.0 // TODO: remove this magic number
	doubleReadTasks := newCostItem("doubleReadTasks", float64(doubleReadRows.GetCost()/batchSize*taskPerBatch))
	builder.reset(doubleReadTasks).mul(&requestFactor).plus(doubleReadCPUCost).setName("doubleReadCost")
	builder.plus(tableSideCost).divA(doubleReadConcurrency).plus(indexSideCost).setName(p.ExplainID().String())

	p.planCostVer2 = builder.Value()

	if p.ctx.GetSessionVars().EnablePaging && p.expectedCnt > 0 && p.expectedCnt <= paging.Threshold {
		// if the expectCnt is below the paging threshold, using paging API
		p.Paging = true // TODO: move this operation from cost model to physical optimization
		p.planCostVer2 = builder.divA(newCostItem("0.6", 0.6)).setName(p.ExplainID().String()).Value()
	}

	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = table-side-cost + sum(index-side-cost)
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
func (p *PhysicalIndexMergeReader) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	netFactor := getTaskNetFactorVer2(p, taskType)
	distConcurrency := newCostItem("distConcurrency", float64(p.ctx.GetSessionVars().DistSQLScanConcurrency()))

	builder := newCostBuilder(traceCost(option))

	var tableSideCost CostItem
	if tablePath := p.tablePlan; tablePath != nil {
		rows := newCostItem("rows", getCardinality(tablePath, option.CostFlag))
		rowSize := newCostItem("rowSize", getAvgRowSize(tablePath.Stats(), tablePath.Schema().Columns))

		builder.netCostVer2(rows, rowSize, netFactor)
		tableChildCost, err := getPlanCostTrace(tablePath, taskType, option)
		if err != nil {
			return zeroCostVer2, err
		}
		tableSideCost = builder.plus(&tableChildCost).divA(distConcurrency).setName("tablePathCost").curr
	}

	indexSideCost := make([]CostItem, 0, len(p.partialPlans))
	for _, indexPath := range p.partialPlans {
		rows := newCostItem("rows", getCardinality(indexPath, option.CostFlag))
		rowSize := newCostItem("rowSize", getAvgRowSize(indexPath.Stats(), indexPath.Schema().Columns))

		indexChildCost, err := getPlanCostTrace(indexPath, taskType, option)
		if err != nil {
			return zeroCostVer2, err
		}

		builder.netCostVer2(rows, rowSize, netFactor)
		indexSideCost = append(indexSideCost, builder.plus(&indexChildCost).divA(distConcurrency).curr)
	}
	builder.reset(nil)
	builder.sumAll(indexSideCost...).plus(tableSideCost).setName(p.ExplainID().String())

	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + sort-cpu-cost + sort-mem-cost + sort-disk-cost
// sort-cpu-cost = rows * log2(rows) * len(sort-items) * cpu-factor
// if no spill:
// 1. sort-mem-cost = rows * row-size * mem-factor
// 2. sort-disk-cost = 0
// else if spill:
// 1. sort-mem-cost = mem-quota * mem-factor
// 2. sort-disk-cost = rows * row-size * disk-factor
func (p *PhysicalSort) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", math.Max(getCardinality(p.children[0], option.CostFlag), 1))
	rowSize := newCostItem("rows", getAvgRowSize(p.statsInfo(), p.Schema().Columns))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	diskFactor := defaultVer2Factors.TiDBDisk
	oomUseTmpStorage := variable.EnableTmpStorageOnOOM.Load()
	memQuota := p.ctx.GetSessionVars().MemTracker.GetBytesLimit()
	spill := taskType == property.RootTaskType && // only TiDB can spill
		oomUseTmpStorage && // spill is enabled
		memQuota > 0 && // mem-quota is set
		rowSize.GetCost()*rows.GetCost() > float64(memQuota) // exceed the mem-quota

	builder := newCostBuilder(traceCost(option))

	sortCPUCost := builder.orderCostVer2(rows, rows.GetCost(), p.ByItems, cpuFactor).setName("sortCPU").curr

	var sortMemCost, sortDiskCost CostItem
	if !spill {
		builder.reset(rows)
		builder.mul(rowSize).mul(&memFactor).setName("sortMem")
		sortMemCost = builder.curr
		sortDiskCost = &zeroCostVer2
	} else {
		builder.reset(rows)
		builder.mul(newCostItem("memQuota", float64(memQuota))).mul(&memFactor).setName("sortMem")
		sortMemCost = builder.curr
		builder.reset(rows)
		builder.mul(rowSize).mul(&diskFactor).setName("sortDisk")
		sortDiskCost = builder.curr
	}

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	builder.reset(rows)
	p.planCostVer2 = builder.sumAll(&childCost, sortCPUCost, sortMemCost, sortDiskCost).setName(p.ExplainID().String()).Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + topn-cpu-cost + topn-mem-cost
// topn-cpu-cost = rows * log2(N) * len(sort-items) * cpu-factor
// topn-mem-cost = N * row-size * mem-factor
func (p *PhysicalTopN) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	N := newCostItem("N", math.Max(1, float64(p.Count+p.Offset)))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.statsInfo(), p.Schema().Columns))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	builder := newCostBuilder(traceCost(option))

	builder.orderCostVer2(rows, N.GetCost(), p.ByItems, cpuFactor)
	builder.plus(N).mul(rowSize).mul(&memFactor)

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = builder.plus(&childCost).setName(p.ExplainID().String()).Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + agg-cost + group-cost
func (p *PhysicalStreamAgg) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	builder := newCostBuilder(traceCost(option))

	aggCost := builder.aggCostVer2(rows, p.AggFuncs, cpuFactor).curr

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	builder.groupCostVer2(rows, p.GroupByItems, cpuFactor)
	builder.sumAll(&childCost, aggCost).setName(p.ExplainID().String())

	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + (agg-cost + group-cost + hash-build-cost + hash-probe-cost) / concurrency
func (p *PhysicalHashAgg) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := newCostItem("inputRows", getCardinality(p.children[0], option.CostFlag))
	outputRows := newCostItem("outputRows", getCardinality(p, option.CostFlag))
	outputRowSize := newCostItem("outputRowSize", getAvgRowSize(p.Stats(), p.Schema().Columns))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().HashAggFinalConcurrency()))
	builder := newCostBuilder(traceCost(option))

	aggCost := builder.aggCostVer2(inputRows, p.AggFuncs, cpuFactor).setName("aggCost").curr
	groupCost := builder.groupCostVer2(inputRows, p.GroupByItems, cpuFactor).setName("groupCost").curr
	hashBuildCost := builder.hashBuildCostVer2(outputRows, outputRowSize, float64(len(p.GroupByItems)), cpuFactor, memFactor).setName("hashBuildCost").curr
	hashProbeCost := builder.hashProbeCostVer2(inputRows, float64(len(p.GroupByItems)), cpuFactor).setName("hashProbeCost").curr
	builder.reset(newCostItem("10", 10))
	startCost := builder.mul(newCostItem("3", 3)).mul(&cpuFactor).setName("startCost").curr

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	builder.reset(nil)
	builder.sumAll(aggCost, groupCost, hashBuildCost, hashProbeCost).divA(concurrency).plus(startCost).plus(&childCost).setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
func (p *PhysicalMergeJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	leftRows := newCostItem("leftRows", getCardinality(p.children[0], option.CostFlag))
	rightRows := newCostItem("rightRows", getCardinality(p.children[1], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	builder := newCostBuilder(traceCost(option))

	leftFilterCost := builder.filterCostVer2(leftRows, p.LeftConditions, cpuFactor).curr
	builder.filterCostVer2(rightRows, p.RightConditions, cpuFactor)
	filterCost := builder.plus(leftFilterCost).setName("filterCost").curr

	leftGroupCost := builder.groupCostVer2(leftRows, cols2Exprs(p.LeftJoinKeys), cpuFactor).curr

	leftChildCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	rightChildCost, err := getPlanCostTrace(p.children[1], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	builder.groupCostVer2(rightRows, cols2Exprs(p.LeftJoinKeys), cpuFactor)
	builder.plus(leftGroupCost).setName("groupCost")
	builder.sumAll(&leftChildCost, &rightChildCost, filterCost).setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
func (p *PhysicalHashJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	build, probe := p.children[0], p.children[1]
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	buildKeys, probeKeys := p.LeftJoinKeys, p.RightJoinKeys
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		build, probe = probe, build
		buildFilters, probeFilters = probeFilters, buildFilters
	}
	buildRows := newCostItem("build_rows", getCardinality(build, option.CostFlag))
	probeRows := newCostItem("probe_rows", getCardinality(probe, option.CostFlag))
	buildRowSize := newCostItem("build_row_size", getAvgRowSize(build.Stats(), build.Schema().Columns))
	tidbConcurrency := newCostItem("tidb_concurrency", float64(p.Concurrency))
	mppConcurrency := newCostItem("mpp_concurrency", float64(3)) // TODO: remove this empirical value
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)

	builder := newCostBuilder(traceCost(option))

	buildFilterCost := builder.filterCostVer2(buildRows, buildFilters, cpuFactor).setName("buildFilterCost").curr

	buildHashCost := builder.hashBuildCostVer2(buildRows, buildRowSize, float64(len(buildKeys)), cpuFactor, memFactor).setName("buildHashCost").curr

	probeFilterCost := builder.filterCostVer2(probeRows, probeFilters, cpuFactor).setName("probeFilterCost").curr

	probeHashCost := builder.hashProbeCostVer2(probeRows, float64(len(probeKeys)), cpuFactor).setName("probeHashCost").curr

	buildChildCost, err := getPlanCostTrace(build, taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	probeChildCost, err := getPlanCostTrace(probe, taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	if taskType == property.MppTaskType { // BCast or Shuffle Join, use mppConcurrency
		builder.reset(buildHashCost).sumAll(buildFilterCost, probeFilterCost, probeHashCost).divA(mppConcurrency).setName("probeCost").sumAll(&buildChildCost, &probeChildCost).setName(p.ExplainID().String())
		p.planCostVer2 = builder.Value()
	} else { // TiDB HashJoin
		startCost := builder.reset(&cpuFactor).mul(newCostItem("10", 10)).mul(newCostItem("3", 3)).setName("startCost").curr
		builder.reset(probeFilterCost).plus(probeHashCost).divA(tidbConcurrency).setName("probeCost")

		builder.sumAll(startCost, &buildChildCost, &probeChildCost, buildHashCost, buildFilterCost).setName(p.ExplainID().String())
		p.planCostVer2 = builder.Value()
	}
	p.planCostInit = true
	return p.planCostVer2, nil
}

func (p *PhysicalIndexJoin) getIndexJoinCostTrace(taskType property.TaskType, option *PlanCostOption, indexJoinType int) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	build, probe := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	buildRows := newCostItem("buildRows", getCardinality(build, option.CostFlag))
	buildRowSize := newCostItem("buildRowSize", getAvgRowSize(build.Stats(), build.Schema().Columns))
	probeRowsOne := newCostItem("probeRows", getCardinality(probe, option.CostFlag))
	probeRowsTot := newCostItem("probeRowsTot", probeRowsOne.GetCost()*buildRows.GetCost())
	probeRowSize := newCostItem("probeRowSize", getAvgRowSize(probe.Stats(), probe.Schema().Columns))
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	probeConcurrency := newCostItem("probeConcurrency", float64(p.ctx.GetSessionVars().IndexLookupJoinConcurrency()))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	requestFactor := getTaskRequestFactorVer2(p, taskType)

	builder := newCostBuilder(traceCost(option))

	buildFilterCost := builder.filterCostVer2(buildRows, buildFilters, cpuFactor).setName("buildFilterCost").curr
	buildChildCost, err := getPlanCostTrace(build, taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	buildTaskCost := builder.reset(buildRows).mul(newCostItem("10", 10)).mul(&cpuFactor).setName("buildTaskCost").curr
	startCost := builder.reset(&cpuFactor).mul(newCostItem("10", 10)).mul(newCostItem("3", 3)).setName("startCost").curr

	probeFilterCost := builder.filterCostVer2(probeRowsTot, probeFilters, cpuFactor).setName("probeFilterCost").curr
	probeChildCost, err := getPlanCostTrace(probe, taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	var hashTableCost CostItem
	switch indexJoinType {
	case 1: // IndexHashJoin
		hashTableCost = builder.hashBuildCostVer2(buildRows, buildRowSize, float64(len(p.RightJoinKeys)), cpuFactor, memFactor).curr
	case 2: // IndexMergeJoin
		hashTableCost = newZeroCostVer2Ptr()
	default: // IndexJoin
		hashTableCost = builder.hashBuildCostVer2(probeRowsTot, probeRowSize, float64(len(p.LeftJoinKeys)), cpuFactor, memFactor).curr
	}
	hashTableCost.SetName("hashTableCost")
	// IndexJoin executes a batch of rows at a time, so the actual cost of this part should be
	//  `innerCostPerBatch * numberOfBatches` instead of `innerCostPerRow * numberOfOuterRow`.
	// Use an empirical value batchRatio to handle this now.
	// TODO: remove this empirical value.
	batchRatio := 6.0
	probeCost := builder.reset(&probeChildCost).mul(buildRows).divA(newCostItem("batchRatio", batchRatio)).setName("probeCost").curr

	// Double Read Cost
	doubleReadCost := newCostItem("", 0)
	if p.ctx.GetSessionVars().IndexJoinDoubleReadPenaltyCostRate > 0 {
		batchSize := float64(p.ctx.GetSessionVars().IndexJoinBatchSize)
		taskPerBatch := 1024.0 // TODO: remove this magic number
		doubleReadTasks := newCostItem("doubleReadTasks", buildRows.GetCost()/batchSize*taskPerBatch)
		doubleReadCost = builder.reset(doubleReadTasks).mul(&requestFactor).mul(newCostItem("IndexJoinDoubleReadPenaltyCostRate", p.ctx.GetSessionVars().IndexJoinDoubleReadPenaltyCostRate)).setName("doubleReadCost").curr
	}

	p.planCostVer2 = builder.reset(doubleReadCost).sumAll(probeCost, probeFilterCost, hashTableCost).divA(probeConcurrency).sumAll(startCost, &buildChildCost, buildFilterCost, buildTaskCost).setName(p.ExplainID().String()).Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost +
// (probe-cost + probe-filter-cost) / concurrency
// probe-cost = probe-child-cost * build-rows / batchRatio
func (p *PhysicalIndexJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	return p.getIndexJoinCostTrace(taskType, option, 0)
}

func (p *PhysicalIndexHashJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	return p.getIndexJoinCostTrace(taskType, option, 1)
}

func (p *PhysicalIndexMergeJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	return p.getIndexJoinCostTrace(taskType, option, 2)
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func (p *PhysicalApply) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	buildRows := newCostItem("buildRows", getCardinality(p.children[0], option.CostFlag))
	probeRowsOne := newCostItem("probeRows", getCardinality(p.children[1], option.CostFlag))
	probeRowsTot := newCostItem("probeRowTot", buildRows.GetCost()*probeRowsOne.GetCost())
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	builder := newCostBuilder(traceCost(option))

	buildFilterCost := builder.filterCostVer2(buildRows, p.LeftConditions, cpuFactor).setName("buildFilterCost").curr
	buildChildCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	probeFilterCost := builder.filterCostVer2(probeRowsTot, p.RightConditions, cpuFactor).setName("probeFilterCost").curr
	probeChildCost, err := getPlanCostTrace(p.children[1], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	builder.reset(&probeChildCost).divA(buildRows).setName("probeCost")
	p.planCostVer2 = builder.sumAll(&buildChildCost, buildFilterCost, probeFilterCost).setName(p.ExplainID().String()).Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace calculates the cost of the plan if it has not been calculated yet and returns the cost.
// plan-cost = sum(child-cost) / concurrency
func (p *PhysicalUnionAll) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().UnionConcurrency()))
	childCosts := make([]CostItem, 0, len(p.children))
	for _, child := range p.children {
		childCost, err := getPlanCostTrace(child, taskType, option)
		if err != nil {
			return zeroCostVer2, err
		}
		childCosts = append(childCosts, &childCost)
	}

	builder := newCostBuilder(traceCost(option))
	p.planCostVer2 = builder.sumAll(childCosts...).divA(concurrency).setName(p.ExplainID().String()).Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + net-cost
func (p *PhysicalExchangeReceiver) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", getCardinality(p, option.CostFlag))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.Schema().Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)
	isBCast := false
	if sender, ok := p.children[0].(*PhysicalExchangeSender); ok {
		isBCast = sender.ExchangeType == tipb.ExchangeType_Broadcast
	}
	numNode := float64(3) // TODO: remove this empirical value

	builder := newCostBuilder(traceCost(option))

	builder.netCostVer2(rows, rowSize, netFactor)
	if isBCast {
		builder.mul(newCostItem("node_num", numNode))
	}

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = builder.plus(&childCost).setName(p.ExplainID().String()).Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
func (p *PointGetPlan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	if p.accessCols == nil { // from fast plan code path
		p.planCostVer2 = zeroCostVer2
		p.planCostInit = true
		return zeroCostVer2, nil
	}
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.schema.Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)

	builder := newCostBuilder(traceCost(option))
	builder.netCostVer2(newCostItem("1", 1), rowSize, netFactor)
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
func (p *BatchPointGetPlan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (costVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	if p.accessCols == nil { // from fast plan code path
		p.planCostVer2 = zeroCostVer2
		p.planCostInit = true
		return zeroCostVer2, nil
	}
	rows := newCostItem("rows", getCardinality(p, option.CostFlag))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.schema.Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)

	builder := newCostBuilder(traceCost(option))
	builder.netCostVer2(rows, rowSize, netFactor)
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

func (builder *CostBuilder) scanCostVer2(rows, rowSize CostItem, scanFactor costVer2Factor) *CostBuilder {
	if rowSize.GetCost() < 1 {
		rowSize.SetCost(1)
	}
	builder.reset(rows)
	builder.mul(newCostItem("log_row_size", math.Log2(rowSize.GetCost()))).mul(&scanFactor)
	return builder
}

func (builder *CostBuilder) netCostVer2(rows, rowSize CostItem, netFactor costVer2Factor) *CostBuilder {
	builder.reset(rows)
	builder.mul(rowSize).mul(&netFactor)
	return builder
}

func (builder *CostBuilder) filterCostVer2(rows CostItem, filters []expression.Expression, cpuFactor costVer2Factor) *CostBuilder {
	builder.reset(rows)
	builder.mul(newCostItem("function_num", numFunctions(filters))).mul(&cpuFactor)
	return builder
}

func (builder *CostBuilder) aggCostVer2(rows CostItem, aggFuncs []*aggregation.AggFuncDesc, cpuFactor costVer2Factor) *CostBuilder {
	builder.reset(rows)
	builder.mul(newCostItem("agg_func_num", float64(len(aggFuncs)))).mul(&cpuFactor)
	return builder
}

func (builder *CostBuilder) groupCostVer2(rows CostItem, groupItems []expression.Expression, cpuFactor costVer2Factor) *CostBuilder {
	builder.reset(rows)
	builder.mul(newCostItem("function_num", numFunctions(groupItems))).mul(&cpuFactor)
	return builder
}

func (builder *CostBuilder) orderCostVer2(rows CostItem, N float64, byItems []*util.ByItems, cpuFactor costVer2Factor) *CostBuilder {
	numFuncs := 0
	for _, byItem := range byItems {
		if _, ok := byItem.Expr.(*expression.ScalarFunction); ok {
			numFuncs++
		}
	}
	builder.reset(rows)
	builder.mul(newCostItem("func_num", float64(numFuncs))).mul(&cpuFactor).plus(rows).mul(newCostItem("logN", math.Log2(N))).mul(&cpuFactor)
	return builder
}

func (builder *CostBuilder) hashBuildCostVer2(rows, rowSize CostItem, nKeys float64, cpuFactor, memFactor costVer2Factor) *CostBuilder {
	builder.reset(rows)
	return builder.mul(newCostItem("key_nums", nKeys)).mul(&cpuFactor).plus(rows).mul(rowSize).mul(&memFactor).plus(rows).mul(&cpuFactor)
}

func (builder *CostBuilder) hashProbeCostVer2(rows CostItem, nKeys float64, cpuFactor costVer2Factor) *CostBuilder {
	builder.reset(rows)
	return builder.mul(newCostItem("key_nums", nKeys)).mul(&cpuFactor).plus(rows).mul(&cpuFactor)
}

type CostItem interface {
	IsSimple() bool
	GetCost() float64
	SetCost(v float64)
	AsFormula() string
	GetName() string
	SetName(v string)
}

// IsSimple used to indentify costVer2Factor with costVer2
func (costVer2Factor) IsSimple() bool {
	return true
}

func (f *costVer2Factor) GetCost() float64 {
	return f.Value
}

func (f *costVer2Factor) SetCost(v float64) {
	f.Value = v
}

func (f *costVer2Factor) AsFormula() string {
	return f.Name
}

func (f *costVer2Factor) GetName() string {
	return f.Name
}

func (f *costVer2Factor) SetName(v string) {
	f.Name = v
}

/*
type costVer2 struct {
	name    string
	cost    float64
	formula string // It used to trace the cost calculation.
	factors map[string]CostItem
}
*/

func (costVer2) IsSimple() bool {
	return false
}

func (c *costVer2) SetCost(v float64) {
	c.cost = v
}

func (c *costVer2) GetCost() float64 {
	return c.cost
}

func (c *costVer2) GetName() string {
	return c.trace.name
}

func (c *costVer2) AsFormula() string {
	if len(c.trace.name) > 0 {
		return c.trace.name
	}
	return c.trace.formula
}

func (c *costVer2) SetName(name string) {
	c.trace.name = name
}

func newCostItem(name string, cost float64) CostItem {
	return &costVer2Factor{Name: name, Value: cost}
}

func newZeroCostVer2Ptr() *costVer2 {
	return &costVer2{cost: 0, trace: nil}
}

func newCostVer2New(formula string, cost float64) *costVer2 {
	return &costVer2{
		cost: cost,
		trace: &costTrace{
			formula:     formula,
			costParams:  make(map[string]interface{}),
			factorCosts: make(map[string]float64),
		},
	}
}

const (
	PLUS = '+'
	SUB  = '-'
	MUL  = '*'
	DIV  = '/'
	MULA = '0'
	DIVA = '1'
)

// CostBuilder use to calculate cost and generate formula
type CostBuilder struct {
	curr    CostItem
	lastV   float64
	lastOp  byte
	isTrace bool
}

func newCostBuilder(isTrace bool) *CostBuilder {
	return &CostBuilder{
		curr:    nil,
		isTrace: isTrace,
	}
}

func newArgCostBuilder(init CostItem, isTrace bool) *CostBuilder {
	if !isTrace {
		// if no trace, calcCost will update Cost on the init CostItem, if const factor such as cupFactor/memFactor used to init CostBuilder, it's value will be changed, so just copy its value as a new CostItem
		return &CostBuilder{
			curr:    newCostItem(init.GetName(), init.GetCost()),
			isTrace: isTrace,
		}
	} else {
		return &CostBuilder{
			curr:    init,
			isTrace: isTrace,
		}
	}

}

func (builder *CostBuilder) reset(init CostItem) *CostBuilder {
	if !builder.isTrace && init.IsSimple() {
		builder.curr = newCostItem(init.GetName(), init.GetCost())
	} else {
		builder.curr = init
	}
	builder.lastV = 0
	builder.lastOp = 0
	return builder
}

func (builder *CostBuilder) Value() costVer2 {
	if builder.curr.IsSimple() {
		return costVer2{cost: builder.curr.GetCost(), trace: nil}
	}
	return *builder.curr.(*costVer2)
}

func (builder *CostBuilder) setName(name string) *CostBuilder {
	// whether isTrace enabled or not, if setName called, we will treat curr as an entity, think "a + b * c" named plan1Cost, "c + d * e" named plan2Cost, plan1Cost.mul(plan2Cost), the formula will be "plan1Cost * plan2Cost", but if we don't treat as an entity, the cost will be "a + b * c * (c + d * e)", got an incorrect result. so we should clear lastOp and lastV to get correct result
	builder.lastOp = 0
	builder.lastV = 0
	builder.curr.SetName(name)
	return builder
}

// calcCost use to calc cost
func (builder *CostBuilder) calcCost(curOp byte, cost float64) float64 {
	var curCost float64
	curCost = builder.curr.GetCost()

	switch curOp {
	case PLUS:
		builder.lastV = cost
		builder.lastOp = curOp
		builder.curr.SetCost(curCost + cost)
		return builder.curr.GetCost()
	case SUB:
		builder.lastV = cost
		builder.lastOp = curOp
		builder.curr.SetCost(curCost - cost)
		return builder.curr.GetCost()
	case MULA:
		builder.lastV = cost
		builder.lastOp = curOp
		builder.curr.SetCost(curCost * cost)
		return builder.curr.GetCost()
	case DIVA:
		builder.lastV = cost
		builder.lastOp = curOp
		builder.curr.SetCost(curCost / cost)
		return builder.curr.GetCost()
	}
	// We should do special action here. for example "a + b * c", we already calc the cost, and then mul("d") called, if using cost * d, the formula is "(a + b * c) * d", but "a + b * c * d" is expected. so I use lastV to record value since last +/-, lastV is "b * c" in this example, when mul/div called, firstly cost +/- lastV("b * c"), then cost -/+ lastV*d("b * c * d")
	switch builder.lastOp {
	case PLUS:
		curCost -= builder.lastV
		if curOp == MUL {
			builder.lastV *= cost
		} else if curOp == DIV {
			builder.lastV /= cost
		}
		curCost += builder.lastV
	case SUB:
		curCost += builder.lastV
		if curOp == MUL {
			builder.lastV *= cost
		} else if curOp == DIV {
			builder.lastV /= cost
		}
		curCost -= builder.lastV
	default:
		if curOp == MUL {
			curCost *= cost
		} else if curOp == DIV {
			curCost /= cost
		}
	}
	builder.curr.SetCost(curCost)
	return curCost
}

// isAnnoymous check whether CostItem have name or not, if no name, the formula will be complicated
func isAnnoymous(v CostItem) bool {
	return !v.IsSimple() && len(v.GetName()) == 0
}

// opFinish use to generate formula
func (builder *CostBuilder) opFinish(op byte, v CostItem, cost float64) {
	// if trace not enabled, skip to calc formula
	if !builder.isTrace {
		return
	}

	var formula string

	curFormula := builder.curr.AsFormula()
	vformula := v.AsFormula()
	// if not a named CostVer2, should add "()", otherwise formula is incorrect
	if isAnnoymous(v) {
		vformula = "( " + vformula + " )"
	}

	switch op {
	case MULA:
		if isAnnoymous(builder.curr) {
			curFormula = "( " + curFormula + " )"
		}
		formula = curFormula + " * " + vformula
	case DIVA:
		if isAnnoymous(builder.curr) {
			curFormula = "( " + curFormula + " )"
		}
		formula = curFormula + " / " + vformula
	default:
		formula = curFormula + " " + string(op) + " " + vformula
	}

	// if v is costVer2, allocate new one as parent
	if !v.IsSimple() {
		newCurr := newCostVer2New(formula, cost)
		// for costVer2Factor or named costVer2, we should add itself to factors,
		// otherwise we should move it's params to the newCurr
		if len(v.GetName()) > 0 {
			newCurr.trace.costParams[v.AsFormula()] = v
		} else {
			cv := v.(*costVer2)
			for k, v1 := range cv.trace.factorCosts {
				newCurr.trace.factorCosts[k] = v1
			}
			for k, v1 := range cv.trace.costParams {
				newCurr.trace.costParams[k] = v1
			}
		}

		// same logic with previous to handle builder.curr
		if len(builder.curr.GetName()) > 0 {
			newCurr.trace.costParams[builder.curr.AsFormula()] = builder.curr
		} else if !builder.curr.IsSimple() {
			cv := builder.curr.(*costVer2)
			for k, v := range cv.trace.factorCosts {
				newCurr.trace.factorCosts[k] = v
			}
			for k, v := range cv.trace.costParams {
				newCurr.trace.costParams[k] = v
			}
		}

		builder.curr = newCurr
	} else {
		// if v is a const item, only add to factors

		// if builder.curr is a simple item, create new costver2 to store factors
		if builder.curr.IsSimple() {
			newCurr := newCostVer2New(formula, cost)
			// add curr to factors
			newCurr.trace.factorCosts[builder.curr.AsFormula()] = builder.curr.GetCost()
			newCurr.trace.factorCosts[v.AsFormula()] = v.GetCost()
			builder.curr = newCurr
		} else {
			// just update formula and factors, cost already updated in calcCost
			cv := builder.curr.(*costVer2)
			cv.trace.formula = formula
			cv.trace.factorCosts[v.AsFormula()] = v.GetCost()
		}
	}
}

func (builder *CostBuilder) sumAll(params ...CostItem) *CostBuilder {
	for _, param := range params {
		if builder.curr == nil {
			builder.reset(param)
		} else {
			builder.plus(param)
		}
	}
	return builder
}

func (builder *CostBuilder) plus(v CostItem) *CostBuilder {
	cost := builder.calcCost(PLUS, v.GetCost())
	builder.opFinish(PLUS, v, cost)
	return builder
}

func (builder *CostBuilder) sub(v CostItem) *CostBuilder {
	cost := builder.calcCost(SUB, v.GetCost())
	builder.opFinish(SUB, v, cost)
	return builder
}

func (builder *CostBuilder) mul(v CostItem) *CostBuilder {
	cost := builder.calcCost(MUL, v.GetCost())
	builder.opFinish(MUL, v, cost)
	return builder
}

func (builder *CostBuilder) div(v CostItem) *CostBuilder {
	cost := builder.calcCost(DIV, v.GetCost())
	builder.opFinish(DIV, v, cost)
	return builder
}

func (builder *CostBuilder) mulA(v CostItem) *CostBuilder {
	cost := builder.calcCost(MULA, v.GetCost())
	builder.opFinish(MULA, v, cost)
	return builder
}

func (builder *CostBuilder) divA(v CostItem) *CostBuilder {
	cost := builder.calcCost(DIVA, v.GetCost())
	builder.opFinish(DIVA, v, cost)
	return builder
}

// for test only
func GetCostBuilderForTest(init CostItem, isTrace bool) *CostBuilder {
	return newArgCostBuilder(init, isTrace)
}

// for test only
func (builder *CostBuilder) EvalOpForTest(op string, v CostItem) {
	switch op {
	case "+":
		builder.plus(v)
	case "-":
		builder.sub(v)
	case "*":
		builder.mul(v)
	case "/":
		builder.div(v)
	case ")*":
		builder.mulA(v)
	case ")/":
		builder.divA(v)
	}
}

// for test only
func (builder *CostBuilder) SetNameForTest(name string) *CostBuilder {
	return builder.setName(name)
}

// for test only
func NewCostItemForTest(name string, cost float64) CostItem {
	return newCostItem(name, cost)
}
