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
	"strings"

	"github.com/pingcap/errors"
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

func convertToCostVer2(cv CostItem) *costVer2 {
	if cv.IsSimple() {
		return &costVer2{cost: cv.GetCost(), trace: &costTrace{cv.GetName(), make(map[string]float64), cv.ToString(), make(map[string]interface{})}}
	}
	vtrace := cv.(*CostVerTrace)
	ret := &costVer2{cost: vtrace.GetCost(), trace: &costTrace{vtrace.GetName(), make(map[string]float64), vtrace.ToString(), make(map[string]interface{})}}

	for name, value := range vtrace.factors {
		if value.IsSimple() {
			ret.trace.factorCosts[name] = value.GetCost()
		} else {
			ret.trace.costParams[name] = convertToCostVer2(value.(*CostVerTrace))
		}
	}
	return ret
}

func buildPlanCostParam(name string, cost CostItem) *tracing.PhysicalPlanCostParam {
	d := &tracing.PhysicalPlanCostParam{}
	d.Name = name
	d.Desc = cost.ToString()
	d.Cost = cost.GetCost()
	d.Params = make(map[string]interface{})
	if !cost.IsSimple() {
		vtrace := cost.(*CostVerTrace)
		for name, value := range vtrace.factors {
			if value.IsSimple() {
				d.Params[name] = value.GetCost()
			} else {
				if strings.Contains(name, "_") {
					continue
				}
				d.Params[name] = buildPlanCostParam(name, value)
			}
		}
	}
	return d
}

func buildPlanCostDetail(tp string, id int, cost CostItem) *tracing.PhysicalPlanCostDetail {
	d := &tracing.PhysicalPlanCostDetail{}
	d.TP = tp
	if id != -1 {
		d.ID = id
	}
	d.Desc = cost.ToString()
	d.Cost = cost.GetCost()
	d.Params = make(map[string]interface{})
	if !cost.IsSimple() {
		vtrace := cost.(*CostVerTrace)
		for name, value := range vtrace.factors {
			if value.IsSimple() {
				d.Params[name] = value.GetCost()
			} else {
				if strings.Contains(name, "_") {
					continue
				}
				d.Params[name] = buildPlanCostParam(name, value)
			}
		}
	}
	return d
}

func genPlanCostTrace(p PhysicalPlan, costV costVer2, taskType property.TaskType, option *PlanCostOption) {
	ret, _ := getPlanCostTrace(p, taskType, option)

	absDiff := math.Abs(costV.cost - ret.GetCost())
	ok := false
	if absDiff < 1 || absDiff/costV.cost < 0.01 {
		ok = true
	}
	if !ok {
		p.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("cost mismatch: trace cost(%v), actual cost(%v)", ret.GetCost(), costV.cost))
	}
}

func getPlanCostTrace(p PhysicalPlan, taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	var ret CostItem
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
		ret, err = getBasePlanCostTrace(p, taskType, option)
	}
	if option.tracer != nil {
		option.tracer.appendPlanCostDetail(buildPlanCostDetail(p.TP(), p.ID(), ret))
	}
	return ret, err
}

func isTraceEnabled(option *PlanCostOption) bool {
	return traceCost(option)
}

// getBasePlanCostTrace calculates the cost of the plan not implements getPlanCostTrace
func getBasePlanCostTrace(p PhysicalPlan, taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	childCosts := make([]CostItem, 0, len(p.Children()))
	builder := newCostBuilder(isTraceEnabled(option))
	for _, child := range p.Children() {
		childCost, err := getPlanCostTrace(child, taskType, option)
		if err != nil {
			return zeroCostItem, err
		}
		childCosts = append(childCosts, childCost)
	}
	var ret CostItem
	if len(childCosts) == 0 {
		ret = newCostItem("", 0)
	} else {
		ret = builder.sumAll(childCosts...).curr
	}
	return ret, nil
}

// getPlanCostTrace calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *basePhysicalPlan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	childCosts := make([]CostItem, 0, len(p.children))
	builder := newCostBuilder(isTraceEnabled(option))
	for _, child := range p.children {
		childCost, err := getPlanCostTrace(child, taskType, option)
		if err != nil {
			return zeroCostItem, err
		}
		childCosts = append(childCosts, childCost)
	}
	var ret CostItem
	if len(childCosts) == 0 {
		ret = newCostItem("", 0)
	} else {
		ret = builder.sumAll(childCosts...).curr
	}
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + filter-cost
func (p *PhysicalSelection) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	inputRows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	builder := newCostBuilder(isTraceEnabled(option))

	filterCost := builder.filterCostVer2(inputRows, p.Conditions, cpuFactor).curr

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	ret := builder.sumAll(filterCost, childCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + proj-cost / concurrency
// proj-cost = input-rows * len(expressions) * cpu-factor
func (p *PhysicalProjection) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	inputRows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().ProjectionConcurrency()))
	builder := newArgCostBuilder(inputRows, isTraceEnabled(option))

	exprCost := builder.mul(newCostItem("expr_count", numFunctions(p.Exprs))).mul(&cpuFactor).div(concurrency).curr

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	ret := builder.sumAll(exprCost, childCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalIndexScan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	rows := newCostItem("rows", getCardinality(p, option.CostFlag))
	rowSize := newCostItem("rowSize", math.Max(getAvgRowSize(p.stats, p.schema.Columns), 2.0)) // consider all index columns
	scanFactor := getTaskScanFactorVer2(p, kv.TiKV, taskType)
	builder := newCostBuilder(isTraceEnabled(option))

	ret := builder.scanCostVer2(rows, rowSize, scanFactor).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalTableScan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	rows := newCostItem("rows", getCardinality(p, option.CostFlag))
	var rowSize float64
	if p.StoreType == kv.TiKV {
		rowSize = getAvgRowSize(p.stats, p.tblCols) // consider all columns if TiKV
	} else { // TiFlash
		rowSize = getAvgRowSize(p.stats, p.schema.Columns)
	}
	myRowSize := newCostItem("rowSize", math.Max(rowSize, 2.0))
	scanFactor := getTaskScanFactorVer2(p, p.StoreType, taskType)
	builder := newCostBuilder(isTraceEnabled(option))

	ret := builder.scanCostVer2(rows, myRowSize, scanFactor).curr

	// give TiFlash a start-up cost to let the optimizer prefers to use TiKV to process small table scans.
	if p.StoreType == kv.TiFlash {
		startRows := newCostItem("10000", 10000)
		ret = builder.scanCostVer2(startRows, myRowSize, scanFactor).plus(ret).curr
	}

	ret.SetName(p.ExplainID().String())
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func (p *PhysicalIndexReader) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	rows := newCostItem("rows", getCardinality(p.indexPlan, option.CostFlag))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.schema.Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().DistSQLScanConcurrency()))
	builder := newCostBuilder(isTraceEnabled(option))

	netCost := builder.netCostVer2(rows, rowSize, netFactor).curr

	childCost, err := getPlanCostTrace(p.indexPlan, property.CopSingleReadTaskType, option)
	if err != nil {
		return zeroCostItem, err
	}
	ret := builder.sumAll(netCost, childCost).divA(concurrency).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func (p *PhysicalTableReader) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	rows := newCostItem("rows", getCardinality(p.tablePlan, option.CostFlag))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.schema.Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().DistSQLScanConcurrency()))
	childType := property.CopSingleReadTaskType
	if p.StoreType == kv.TiFlash { // mpp protocol
		childType = property.MppTaskType
	}

	builder := newCostBuilder(isTraceEnabled(option))

	netCost := builder.netCostVer2(rows, rowSize, netFactor).curr

	childCost, err := getPlanCostTrace(p.tablePlan, childType, option)
	if err != nil {
		return zeroCostItem, err
	}
	ret := builder.sumAll(netCost, childCost).divA(concurrency).curr

	// consider tidb_enforce_mpp
	if p.StoreType == kv.TiFlash && p.ctx.GetSessionVars().IsMPPEnforced() &&
		!hasCostFlag(option.CostFlag, CostFlagRecalculate) { // show the real cost in explain-statements
		ret = builder.reset(ret).divA(newCostItem("1000000000", 1000000000)).curr
	}
	ret.SetName(p.ExplainID().String())
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = index-side-cost + (table-side-cost + double-read-cost) / double-read-concurrency
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
// double-read-cost = double-read-request-cost + double-read-cpu-cost
// double-read-request-cost = double-read-tasks * request-factor
// double-read-cpu-cost = index-rows * cpu-factor
// double-read-tasks = index-rows / batch-size * task-per-batch # task-per-batch is a magic number now
func (p *PhysicalIndexLookUpReader) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	indexRows := newCostItem("indexRows", getCardinality(p.indexPlan, option.CostFlag))
	tableRows := newCostItem("tableRows", getCardinality(p.indexPlan, option.CostFlag))
	indexRowSize := newCostItem("indexRowSize", getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false))
	tableRowSize := newCostItem("tableRowSize", getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	netFactor := getTaskNetFactorVer2(p, taskType)
	requestFactor := getTaskRequestFactorVer2(p, taskType)
	distConcurrency := newCostItem("distConcurrency", float64(p.ctx.GetSessionVars().DistSQLScanConcurrency()))
	doubleReadConcurrency := newCostItem("doubleReadConcurrency", float64(p.ctx.GetSessionVars().IndexLookupConcurrency()))
	builder := newCostBuilder(isTraceEnabled(option))

	indexChildCost, err := getPlanCostTrace(p.indexPlan, property.CopMultiReadTaskType, option)
	if err != nil {
		return zeroCostItem, err
	}
	// index-side
	indexNetCost := builder.netCostVer2(indexRows, indexRowSize, netFactor).curr
	indexSideCost := builder.sumAll(indexNetCost, indexChildCost).divA(distConcurrency).setName("indexSideCost").curr

	tableChildCost, err := getPlanCostTrace(p.tablePlan, property.CopMultiReadTaskType, option)
	if err != nil {
		return zeroCostItem, err
	}
	// table-side
	tableNetCost := builder.netCostVer2(tableRows, tableRowSize, netFactor).curr
	tableSideCost := builder.sumAll(tableNetCost, tableChildCost).divA(distConcurrency).setName("tableSideCost").curr

	doubleReadRows := newCostItem("doubleReadRows", indexRows.GetCost())
	doubleReadCPUCost := builder.reset(doubleReadRows).mul(&cpuFactor).curr

	batchSize := float64(p.ctx.GetSessionVars().IndexLookupSize)
	taskPerBatch := 32.0 // TODO: remove this magic number
	doubleReadTasks := newCostItem("doubleReadTasks", float64(doubleReadRows.GetCost()/batchSize*taskPerBatch))
	doubleReadCost := builder.reset(doubleReadTasks).mul(&requestFactor).plus(doubleReadCPUCost).setName("doubleReadCost").curr
	ret := builder.sumAll(doubleReadCost, tableSideCost).divA(doubleReadConcurrency).plus(indexSideCost).curr

	if p.ctx.GetSessionVars().EnablePaging && p.expectedCnt > 0 && p.expectedCnt <= paging.Threshold {
		// if the expectCnt is below the paging threshold, using paging API
		p.Paging = true // TODO: move this operation from cost model to physical optimization
		ret = builder.reset(ret).divA(newCostItem("0.6", 0.6)).curr
	}
	ret.SetName(p.ExplainID().String())
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = table-side-cost + sum(index-side-cost)
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
func (p *PhysicalIndexMergeReader) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	netFactor := getTaskNetFactorVer2(p, taskType)
	distConcurrency := newCostItem("distConcurrency", float64(p.ctx.GetSessionVars().DistSQLScanConcurrency()))

	builder := newCostBuilder(isTraceEnabled(option))

	var tableSideCost CostItem
	if tablePath := p.tablePlan; tablePath != nil {
		rows := newCostItem("rows", getCardinality(tablePath, option.CostFlag))
		rowSize := newCostItem("rowSize", getAvgRowSize(tablePath.Stats(), tablePath.Schema().Columns))

		tableChildCost, err := getPlanCostTrace(tablePath, taskType, option)
		if err != nil {
			return zeroCostItem, err
		}
		tableNetCost := builder.netCostVer2(rows, rowSize, netFactor).curr
		tableSideCost = builder.sumAll(tableNetCost, tableChildCost).divA(distConcurrency).setName("tablePathCost").curr
	}

	indexSideCost := make([]CostItem, 0, len(p.partialPlans))
	for _, indexPath := range p.partialPlans {
		rows := newCostItem("rows", getCardinality(indexPath, option.CostFlag))
		rowSize := newCostItem("rowSize", getAvgRowSize(indexPath.Stats(), indexPath.Schema().Columns))

		indexChildCost, err := getPlanCostTrace(indexPath, taskType, option)
		if err != nil {
			return zeroCostItem, err
		}

		indexNetCost := builder.netCostVer2(rows, rowSize, netFactor).curr
		indexSideCost = append(indexSideCost, builder.sumAll(indexNetCost, indexChildCost).divA(distConcurrency).curr)
	}
	ret := builder.sumAll(indexSideCost...).plus(tableSideCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
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
func (p *PhysicalSort) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	rows := newCostItem("rows", math.Max(getCardinality(p.children[0], option.CostFlag), 1))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.statsInfo(), p.Schema().Columns))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	diskFactor := defaultVer2Factors.TiDBDisk
	oomUseTmpStorage := variable.EnableTmpStorageOnOOM.Load()
	memQuota := p.ctx.GetSessionVars().MemTracker.GetBytesLimit()
	spill := taskType == property.RootTaskType && // only TiDB can spill
		oomUseTmpStorage && // spill is enabled
		memQuota > 0 && // mem-quota is set
		rowSize.GetCost()*rows.GetCost() > float64(memQuota) // exceed the mem-quota

	builder := newCostBuilder(isTraceEnabled(option))

	sortCPUCost := builder.orderCostVer2(rows, rows.GetCost(), p.ByItems, cpuFactor).setName("sortCPU").curr

	var sortMemCost, sortDiskCost CostItem
	if !spill {
		builder.reset(rows)
		builder.mul(rowSize).mul(&memFactor).setName("sortMem")
		sortMemCost = builder.curr
		sortDiskCost = newCostItem("sortDisk", 0)
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
		return zeroCostItem, err
	}

	ret := builder.sumAll(childCost, sortCPUCost, sortMemCost, sortDiskCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + topn-cpu-cost + topn-mem-cost
// topn-cpu-cost = rows * log2(N) * len(sort-items) * cpu-factor
// topn-mem-cost = N * row-size * mem-factor
func (p *PhysicalTopN) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	rows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	N := newCostItem("N", math.Max(1, float64(p.Count+p.Offset)))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.statsInfo(), p.Schema().Columns))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	builder := newCostBuilder(isTraceEnabled(option))

	topNCPUCost := builder.orderCostVer2(rows, N.GetCost(), p.ByItems, cpuFactor).curr
	topNMemCost := builder.reset(N).mul(rowSize).mul(&memFactor).curr

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	ret := builder.sumAll(childCost, topNCPUCost, topNMemCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + agg-cost + group-cost
func (p *PhysicalStreamAgg) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	rows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	builder := newCostBuilder(isTraceEnabled(option))

	aggCost := builder.aggCostVer2(rows, p.AggFuncs, cpuFactor).curr
	groupCost := builder.groupCostVer2(rows, p.GroupByItems, cpuFactor).curr

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	ret := builder.sumAll(childCost, aggCost, groupCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + (agg-cost + group-cost + hash-build-cost + hash-probe-cost) / concurrency
func (p *PhysicalHashAgg) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	inputRows := newCostItem("inputRows", getCardinality(p.children[0], option.CostFlag))
	outputRows := newCostItem("outputRows", getCardinality(p, option.CostFlag))
	outputRowSize := newCostItem("outputRowSize", getAvgRowSize(p.Stats(), p.Schema().Columns))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().HashAggFinalConcurrency()))
	builder := newCostBuilder(isTraceEnabled(option))

	aggCost := builder.aggCostVer2(inputRows, p.AggFuncs, cpuFactor).setName("aggCost").curr
	groupCost := builder.groupCostVer2(inputRows, p.GroupByItems, cpuFactor).setName("groupCost").curr
	hashBuildCost := builder.hashBuildCostVer2(outputRows, outputRowSize, float64(len(p.GroupByItems)), cpuFactor, memFactor).setName("hashBuildCost").curr
	hashProbeCost := builder.hashProbeCostVer2(inputRows, float64(len(p.GroupByItems)), cpuFactor).setName("hashProbeCost").curr
	builder.reset(newCostItem("10", 10))
	startCost := builder.mul(newCostItem("3", 3)).mul(&cpuFactor).setName("startCost").curr

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}
	builder.reset(nil)
	ret := builder.sumAll(aggCost, groupCost, hashBuildCost, hashProbeCost).divA(concurrency).plus(startCost).plus(childCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
func (p *PhysicalMergeJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	leftRows := newCostItem("leftRows", getCardinality(p.children[0], option.CostFlag))
	rightRows := newCostItem("rightRows", getCardinality(p.children[1], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	builder := newCostBuilder(isTraceEnabled(option))

	leftFilterCost := builder.filterCostVer2(leftRows, p.LeftConditions, cpuFactor).curr
	rightFilterCost := builder.filterCostVer2(rightRows, p.RightConditions, cpuFactor).curr
	filterCost := builder.sumAll(leftFilterCost, rightFilterCost).setName("filterCost").curr

	leftGroupCost := builder.groupCostVer2(leftRows, cols2Exprs(p.LeftJoinKeys), cpuFactor).curr
	rightGroupCost := builder.groupCostVer2(rightRows, cols2Exprs(p.LeftJoinKeys), cpuFactor).curr
	groupCost := builder.sumAll(leftGroupCost, rightGroupCost).setName("groupCost").curr

	leftChildCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}
	rightChildCost, err := getPlanCostTrace(p.children[1], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	ret := builder.sumAll(leftChildCost, rightChildCost, filterCost, groupCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
func (p *PhysicalHashJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
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

	builder := newCostBuilder(isTraceEnabled(option))

	buildFilterCost := builder.filterCostVer2(buildRows, buildFilters, cpuFactor).setName("buildFilterCost").curr

	buildHashCost := builder.hashBuildCostVer2(buildRows, buildRowSize, float64(len(buildKeys)), cpuFactor, memFactor).setName("buildHashCost").curr

	probeFilterCost := builder.filterCostVer2(probeRows, probeFilters, cpuFactor).setName("probeFilterCost").curr

	probeHashCost := builder.hashProbeCostVer2(probeRows, float64(len(probeKeys)), cpuFactor).setName("probeHashCost").curr

	buildChildCost, err := getPlanCostTrace(build, taskType, option)
	if err != nil {
		return zeroCostItem, err
	}
	probeChildCost, err := getPlanCostTrace(probe, taskType, option)
	if err != nil {
		return zeroCostItem, err
	}
	var ret CostItem
	if taskType == property.MppTaskType { // BCast or Shuffle Join, use mppConcurrency
		probeCost := builder.sumAll(buildHashCost, buildFilterCost, probeFilterCost, probeHashCost).divA(mppConcurrency).setName("probeCost").curr
		ret = builder.sumAll(buildChildCost, probeChildCost, probeCost).setName(p.ExplainID().String()).curr
	} else { // TiDB HashJoin
		startCost := builder.reset(&cpuFactor).mul(newCostItem("10", 10)).mul(newCostItem("3", 3)).setName("startCost").curr
		probeCost := builder.sumAll(probeFilterCost, probeHashCost).divA(tidbConcurrency).setName("probeCost").curr
		ret = builder.sumAll(startCost, buildChildCost, probeChildCost, buildHashCost, buildFilterCost, probeCost).setName(p.ExplainID().String()).curr
	}
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

func (p *PhysicalIndexJoin) getIndexJoinCostVerTrace(taskType property.TaskType, option *PlanCostOption, indexJoinType int) (CostItem, error) {
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

	builder := newCostBuilder(isTraceEnabled(option))

	buildFilterCost := builder.filterCostVer2(buildRows, buildFilters, cpuFactor).setName("buildFilterCost").curr
	buildChildCost, err := getPlanCostTrace(build, taskType, option)
	if err != nil {
		return zeroCostItem, err
	}
	buildTaskCost := builder.reset(buildRows).mul(newCostItem("10", 10)).mul(&cpuFactor).setName("buildTaskCost").curr
	startCost := builder.reset(&cpuFactor).mul(newCostItem("10", 10)).mul(newCostItem("3", 3)).setName("startCost").curr

	probeFilterCost := builder.filterCostVer2(probeRowsTot, probeFilters, cpuFactor).setName("probeFilterCost").curr
	probeChildCost, err := getPlanCostTrace(probe, taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	var hashTableCost CostItem
	switch indexJoinType {
	case 1: // IndexHashJoin
		hashTableCost = builder.hashBuildCostVer2(buildRows, buildRowSize, float64(len(p.RightJoinKeys)), cpuFactor, memFactor).curr
	case 2: // IndexMergeJoin
		hashTableCost = newCostItem("", 0)
	default: // IndexJoin
		hashTableCost = builder.hashBuildCostVer2(probeRowsTot, probeRowSize, float64(len(p.LeftJoinKeys)), cpuFactor, memFactor).curr
	}
	hashTableCost.SetName("hashTableCost")
	// IndexJoin executes a batch of rows at a time, so the actual cost of this part should be
	//  `innerCostPerBatch * numberOfBatches` instead of `innerCostPerRow * numberOfOuterRow`.
	// Use an empirical value batchRatio to handle this now.
	// TODO: remove this empirical value.
	batchRatio := 6.0
	probeCost := builder.reset(probeChildCost).mul(buildRows).divA(newCostItem("batchRatio", batchRatio)).setName("probeCost").curr

	// Double Read Cost
	doubleReadCost := newCostItem("doubleReadCost", 0)
	if p.ctx.GetSessionVars().IndexJoinDoubleReadPenaltyCostRate > 0 {
		batchSize := float64(p.ctx.GetSessionVars().IndexJoinBatchSize)
		taskPerBatch := 1024.0 // TODO: remove this magic number
		doubleReadTasks := newCostItem("doubleReadTasks", buildRows.GetCost()/batchSize*taskPerBatch)
		doubleReadCost = builder.reset(doubleReadTasks).mul(&requestFactor).mul(newCostItem("IndexJoinDoubleReadPenaltyCostRate", p.ctx.GetSessionVars().IndexJoinDoubleReadPenaltyCostRate)).setName("doubleReadCost").curr
	}

	ret := builder.sumAll(doubleReadCost, probeCost, probeFilterCost, hashTableCost).divA(probeConcurrency).curr
	ret = builder.sumAll(ret, startCost, buildChildCost, buildFilterCost, buildTaskCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost +
// (probe-cost + probe-filter-cost) / concurrency
// probe-cost = probe-child-cost * build-rows / batchRatio
func (p *PhysicalIndexJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	return p.getIndexJoinCostVerTrace(taskType, option, 0)
}

func (p *PhysicalIndexHashJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	return p.getIndexJoinCostVerTrace(taskType, option, 1)
}

func (p *PhysicalIndexMergeJoin) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	return p.getIndexJoinCostVerTrace(taskType, option, 2)
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func (p *PhysicalApply) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	buildRows := newCostItem("buildRows", getCardinality(p.children[0], option.CostFlag))
	probeRowsOne := newCostItem("probeRows", getCardinality(p.children[1], option.CostFlag))
	probeRowsTot := newCostItem("probeRowTot", buildRows.GetCost()*probeRowsOne.GetCost())
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	builder := newCostBuilder(isTraceEnabled(option))

	buildFilterCost := builder.filterCostVer2(buildRows, p.LeftConditions, cpuFactor).setName("buildFilterCost").curr
	buildChildCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	probeFilterCost := builder.filterCostVer2(probeRowsTot, p.RightConditions, cpuFactor).setName("probeFilterCost").curr
	probeChildCost, err := getPlanCostTrace(p.children[1], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	probeCost := builder.reset(probeChildCost).divA(buildRows).setName("probeCost").curr
	ret := builder.sumAll(buildChildCost, buildFilterCost, probeCost, probeFilterCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace calculates the cost of the plan if it has not been calculated yet and returns the cost.
// plan-cost = sum(child-cost) / concurrency
func (p *PhysicalUnionAll) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().UnionConcurrency()))
	childCosts := make([]CostItem, 0, len(p.children))
	for _, child := range p.children {
		childCost, err := getPlanCostTrace(child, taskType, option)
		if err != nil {
			return zeroCostItem, err
		}
		childCosts = append(childCosts, childCost)
	}

	builder := newCostBuilder(isTraceEnabled(option))
	ret := builder.sumAll(childCosts...).divA(concurrency).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + net-cost
func (p *PhysicalExchangeReceiver) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	rows := newCostItem("rows", getCardinality(p, option.CostFlag))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.Schema().Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)
	isBCast := false
	if sender, ok := p.children[0].(*PhysicalExchangeSender); ok {
		isBCast = sender.ExchangeType == tipb.ExchangeType_Broadcast
	}
	numNode := float64(3) // TODO: remove this empirical value

	builder := newCostBuilder(isTraceEnabled(option))

	netCost := builder.netCostVer2(rows, rowSize, netFactor).curr
	if isBCast {
		builder.reset(netCost).mul(newCostItem("node_num", numNode))
	}

	childCost, err := getPlanCostTrace(p.children[0], taskType, option)
	if err != nil {
		return zeroCostItem, err
	}

	ret := builder.sumAll(childCost, netCost).setName(p.ExplainID().String()).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
func (p *PointGetPlan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	if p.accessCols == nil { // from fast plan code path
		return zeroCostItem, nil
	}
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.schema.Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)

	builder := newCostBuilder(isTraceEnabled(option))
	ret := builder.netCostVer2(newCostItem("1", 1), rowSize, netFactor).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
}

// getPlanCostTrace returns the plan-cost of this sub-plan, which is:
func (p *BatchPointGetPlan) getPlanCostTrace(taskType property.TaskType, option *PlanCostOption) (CostItem, error) {
	if p.accessCols == nil { // from fast plan code path
		return zeroCostItem, nil
	}
	rows := newCostItem("rows", getCardinality(p, option.CostFlag))
	rowSize := newCostItem("rowSize", getAvgRowSize(p.stats, p.schema.Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)

	builder := newCostBuilder(isTraceEnabled(option))
	ret := builder.netCostVer2(rows, rowSize, netFactor).curr
	p.planCostVer2 = *convertToCostVer2(ret)
	return ret, nil
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
	ToString() string
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

func (f costVer2Factor) ToString() string {
	return fmt.Sprintf("%s(%v)", f.Name, f.Value)
}

type CostVerTrace struct {
	name    string              `json:"name"`
	cost    float64             `json:"cost"`
	formula string              `json:"formula"`
	factors map[string]CostItem `json:"factors"`
}

func (CostVerTrace) IsSimple() bool {
	return false
}

func (c *CostVerTrace) SetCost(v float64) {
	c.cost = v
}

func (c *CostVerTrace) GetCost() float64 {
	return c.cost
}

func (c *CostVerTrace) GetName() string {
	return c.name
}

func (c *CostVerTrace) AsFormula() string {
	if len(c.name) > 0 {
		return c.name
	}
	return c.formula
}

func (c *CostVerTrace) SetName(name string) {
	c.name = name
}

func (f *CostVerTrace) ToString() string {
	formula := f.formula
	for k, v := range f.factors {
		val := fmt.Sprintf("%s(%v)", k, v.GetCost())
		formula = strings.ReplaceAll(formula, k, val)
	}
	return formula
}

func newCostItem(name string, cost float64) CostItem {
	return &costVer2Factor{Name: name, Value: cost}
}

func newCostVer2New(formula string, cost float64) *CostVerTrace {
	return &CostVerTrace{
		cost:    cost,
		name:    "",
		formula: formula,
		factors: make(map[string]CostItem),
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
	if init == nil {
		builder.curr = nil
	} else {
		if !builder.isTrace || init.IsSimple() {
			builder.curr = newCostItem(init.GetName(), init.GetCost())
		} else {
			builder.curr = init
		}
	}
	builder.lastV = 0
	builder.lastOp = 0
	return builder
}

func (builder *CostBuilder) Value() CostItem {
	return builder.curr
}

func (builder *CostBuilder) setName(name string) *CostBuilder {
	// whether isTrace enabled or not, if setName called, we will treat curr as an entity, think "a + b * c" named plan1Cost, "c + d * e" named plan2Cost, plan1Cost.mul(plan2Cost), the formula will be "plan1Cost * plan2Cost", but if we don't treat as an entity, the cost will be "a + b * c * (c + d * e)", got an incorrect result. so we should clear lastOp and lastV to get correct result
	builder.lastOp = 0
	builder.lastV = 0
	builder.curr.SetName(name)
	return builder
}

func (builder *CostBuilder) checkAndSetCost(cost float64) float64 {
	if !builder.isTrace {
		builder.curr.SetCost(cost)
	}
	return cost
}

// calcCost use to calc cost
func (builder *CostBuilder) calcCost(curOp byte, cost float64) float64 {
	var curCost float64
	curCost = builder.curr.GetCost()

	switch curOp {
	case PLUS:
		builder.lastV = cost
		builder.lastOp = curOp
		return builder.checkAndSetCost(curCost + cost)
	case SUB:
		builder.lastV = cost
		builder.lastOp = curOp
		return builder.checkAndSetCost(curCost - cost)
	case MULA:
		builder.lastV = cost
		builder.lastOp = curOp
		return builder.checkAndSetCost(curCost * cost)
	case DIVA:
		builder.lastV = cost
		builder.lastOp = curOp
		return builder.checkAndSetCost(curCost / cost)
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
	return builder.checkAndSetCost(curCost)
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
			newCurr.factors[v.AsFormula()] = v
		} else {
			cv := v.(*CostVerTrace)
			for k, v1 := range cv.factors {
				newCurr.factors[k] = v1
			}
		}

		// same logic with previous to handle builder.curr
		if len(builder.curr.GetName()) > 0 {
			newCurr.factors[builder.curr.AsFormula()] = builder.curr
		} else if !builder.curr.IsSimple() {
			cv := builder.curr.(*CostVerTrace)
			for k, v := range cv.factors {
				newCurr.factors[k] = v
			}
		}

		builder.curr = newCurr
	} else {
		// if v is a const item, only add to factors

		// if builder.curr is a simple item, create new costver2 to store factors
		if builder.curr.IsSimple() {
			newCurr := newCostVer2New(formula, cost)
			// add curr to factors
			newCurr.factors[builder.curr.AsFormula()] = builder.curr
			newCurr.factors[v.AsFormula()] = v
			builder.curr = newCurr
		} else {
			// just update formula, cost and factors
			cv := builder.curr.(*CostVerTrace)
			cv.cost = cost
			cv.formula = formula
			cv.factors[v.AsFormula()] = v
		}
	}
}

func (builder *CostBuilder) sumAll(params ...CostItem) *CostBuilder {
	builder.reset(nil)
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
	if v.GetCost() == 0 {
		return builder
	}
	cost := builder.calcCost(PLUS, v.GetCost())
	builder.opFinish(PLUS, v, cost)
	return builder
}

func (builder *CostBuilder) sub(v CostItem) *CostBuilder {
	if v.GetCost() == 0 {
		return builder
	}
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

var zeroCostItem = newCostItem("", 0)
