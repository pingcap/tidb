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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/paging"
)

const (
	// CostFlagRecalculate indicates the optimizer to ignore cached cost and recalculate it again.
	CostFlagRecalculate uint64 = 1 << iota

	// CostFlagUseTrueCardinality indicates the optimizer to use true cardinality to calculate the cost.
	CostFlagUseTrueCardinality

	// CostFlagTrace indicates whether to trace the cost calculation.
	CostFlagTrace
)

const (
	modelVer1 = 1
	modelVer2 = 2
)

func hasCostFlag(costFlag, flag uint64) bool {
	return (costFlag & flag) > 0
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *basePhysicalPlan) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		// just calculate the cost once and always reuse it
		return p.planCost, nil
	}
	p.planCost = 0 // the default implementation, the operator have no cost
	for _, child := range p.children {
		childCost, err := child.getPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalSelection) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	var selfCost float64
	var cpuFactor float64
	switch taskType {
	case property.RootTaskType, property.MppTaskType:
		cpuFactor = p.ctx.GetSessionVars().GetCPUFactor()
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		cpuFactor = p.ctx.GetSessionVars().GetCopCPUFactor()
	default:
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	selfCost = getCardinality(p.children[0], costFlag) * cpuFactor
	if p.fromDataSource {
		selfCost = 0 // for compatibility, see https://github.com/pingcap/tidb/issues/36243
	}

	childCost, err := p.children[0].getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost + selfCost
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes the cost of projection operator itself.
func (p *PhysicalProjection) GetCost(count float64) float64 {
	sessVars := p.ctx.GetSessionVars()
	cpuCost := count * sessVars.GetCPUFactor()
	concurrency := float64(sessVars.ProjectionConcurrency())
	if concurrency <= 0 {
		return cpuCost
	}
	cpuCost /= concurrency
	concurrencyCost := (1 + concurrency) * sessVars.GetConcurrencyFactor()
	return cpuCost + concurrencyCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalProjection) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(getCardinality(p, costFlag)) // projection cost
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes cost of index lookup operator itself.
func (p *PhysicalIndexLookUpReader) GetCost(costFlag uint64) (cost float64) {
	indexPlan, tablePlan := p.indexPlan, p.tablePlan
	ctx := p.ctx
	sessVars := ctx.GetSessionVars()
	// Add cost of building table reader executors. Handles are extracted in batch style,
	// each handle is a range, the CPU cost of building copTasks should be:
	// (indexRows / batchSize) * batchSize * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	indexRows := getCardinality(indexPlan, costFlag)
	idxCst := indexRows * sessVars.GetCPUFactor()
	// if the expectCnt is below the paging threshold, using paging API, recalculate idxCst.
	// paging API reduces the count of index and table rows, however introduces more seek cost.
	if ctx.GetSessionVars().EnablePaging && p.expectedCnt > 0 && p.expectedCnt <= paging.Threshold {
		p.Paging = true
		pagingCst := calcPagingCost(ctx, p.indexPlan, p.expectedCnt)
		// prevent enlarging the cost because we take paging as a better plan,
		// if the cost is enlarged, it'll be easier to go another plan.
		idxCst = math.Min(idxCst, pagingCst)
	}
	cost += idxCst
	// Add cost of worker goroutines in index lookup.
	numTblWorkers := float64(sessVars.IndexLookupConcurrency())
	cost += (numTblWorkers + 1) * sessVars.GetConcurrencyFactor()
	// When building table reader executor for each batch, we would sort the handles. CPU
	// cost of sort is:
	// CPUFactor * batchSize * Log2(batchSize) * (indexRows / batchSize)
	indexLookupSize := float64(sessVars.IndexLookupSize)
	batchSize := math.Min(indexLookupSize, indexRows)
	if batchSize > 2 {
		sortCPUCost := (indexRows * math.Log2(batchSize) * sessVars.GetCPUFactor()) / numTblWorkers
		cost += sortCPUCost
	}
	// Also, we need to sort the retrieved rows if index lookup reader is expected to return
	// ordered results. Note that row count of these two sorts can be different, if there are
	// operators above table scan.
	tableRows := getCardinality(tablePlan, costFlag)
	selectivity := tableRows / indexRows
	batchSize = math.Min(indexLookupSize*selectivity, tableRows)
	if p.keepOrder && batchSize > 2 {
		sortCPUCost := (tableRows * math.Log2(batchSize) * sessVars.GetCPUFactor()) / numTblWorkers
		cost += sortCPUCost
	}
	return
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexLookUpReader) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	p.planCost = 0
	// child's cost
	for _, child := range []PhysicalPlan{p.indexPlan, p.tablePlan} {
		childCost, err := child.getPlanCostVer1(property.CopDoubleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}

	// to keep compatible with the previous cost implementation, re-calculate table-scan cost by using index stats-count again (see copTask.finishIndexPlan).
	// TODO: amend table-side cost here later
	var tmp PhysicalPlan
	for tmp = p.tablePlan; len(tmp.Children()) > 0; tmp = tmp.Children()[0] {
	}
	ts := tmp.(*PhysicalTableScan)
	tblCost, err := ts.getPlanCostVer1(property.CopDoubleReadTaskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost -= tblCost
	p.planCost += getCardinality(p.indexPlan, costFlag) * ts.getScanRowSize() * p.SCtx().GetSessionVars().GetScanFactor(ts.Table)

	// index-side net I/O cost: rows * row-size * net-factor
	netFactor := getTableNetFactor(p.tablePlan)
	rowSize := getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	p.planCost += getCardinality(p.indexPlan, costFlag) * rowSize * netFactor

	// index-side net seek cost
	p.planCost += estimateNetSeekCost(p.indexPlan)

	// table-side net I/O cost: rows * row-size * net-factor
	tblRowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	p.planCost += getCardinality(p.tablePlan, costFlag) * tblRowSize * netFactor

	// table-side seek cost
	p.planCost += estimateNetSeekCost(p.tablePlan)

	// consider concurrency
	p.planCost /= float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	// lookup-cpu-cost in TiDB
	p.planCost += p.GetCost(costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexReader) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	var rowCount, rowSize, netFactor, indexPlanCost, netSeekCost float64
	sqlScanConcurrency := p.ctx.GetSessionVars().DistSQLScanConcurrency()
	// child's cost
	childCost, err := p.indexPlan.getPlanCostVer1(property.CopSingleReadTaskType, option)
	if err != nil {
		return 0, err
	}
	indexPlanCost = childCost
	p.planCost = indexPlanCost
	// net I/O cost: rows * row-size * net-factor
	tblStats := getTblStats(p.indexPlan)
	rowSize = tblStats.GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	rowCount = getCardinality(p.indexPlan, costFlag)
	netFactor = getTableNetFactor(p.indexPlan)
	p.planCost += rowCount * rowSize * netFactor
	// net seek cost
	netSeekCost = estimateNetSeekCost(p.indexPlan)
	p.planCost += netSeekCost
	// consider concurrency
	p.planCost /= float64(sqlScanConcurrency)

	if option.tracer != nil {
		setPhysicalIndexReaderCostDetail(p, option.tracer, rowCount, rowSize, netFactor, netSeekCost, indexPlanCost, sqlScanConcurrency)
	}
	p.planCostInit = true
	return p.planCost, nil
}

// GetNetDataSize calculates the cost of the plan in network data transfer.
func (p *PhysicalIndexReader) GetNetDataSize() float64 {
	tblStats := getTblStats(p.indexPlan)
	rowSize := tblStats.GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	return p.indexPlan.StatsCount() * rowSize
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableReader) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	p.planCost = 0
	netFactor := getTableNetFactor(p.tablePlan)
	var rowCount, rowSize, netSeekCost, tableCost float64
	sqlScanConcurrency := p.ctx.GetSessionVars().DistSQLScanConcurrency()
	storeType := p.StoreType
	switch storeType {
	case kv.TiKV:
		// child's cost
		childCost, err := p.tablePlan.getPlanCostVer1(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		tableCost = childCost
		p.planCost = childCost
		// net I/O cost: rows * row-size * net-factor
		rowSize = getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
		rowCount = getCardinality(p.tablePlan, costFlag)
		p.planCost += rowCount * rowSize * netFactor
		// net seek cost
		netSeekCost = estimateNetSeekCost(p.tablePlan)
		p.planCost += netSeekCost
		// consider concurrency
		p.planCost /= float64(sqlScanConcurrency)
	case kv.TiFlash:
		var concurrency, rowSize, seekCost float64
		_, isMPP := p.tablePlan.(*PhysicalExchangeSender)
		if isMPP {
			// mpp protocol
			concurrency = p.ctx.GetSessionVars().CopTiFlashConcurrencyFactor
			rowSize = collectRowSizeFromMPPPlan(p.tablePlan)
			seekCost = accumulateNetSeekCost4MPP(p.tablePlan)
			childCost, err := p.tablePlan.getPlanCostVer1(property.MppTaskType, option)
			if err != nil {
				return 0, err
			}
			p.planCost = childCost
		} else {
			// cop protocol
			concurrency = float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
			rowSize = getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
			seekCost = estimateNetSeekCost(p.tablePlan)
			tType := property.CopSingleReadTaskType
			childCost, err := p.tablePlan.getPlanCostVer1(tType, option)
			if err != nil {
				return 0, err
			}
			p.planCost = childCost
		}

		// net I/O cost
		p.planCost += getCardinality(p.tablePlan, costFlag) * rowSize * netFactor
		// net seek cost
		p.planCost += seekCost
		// consider concurrency
		p.planCost /= concurrency
		// consider tidb_enforce_mpp
		if isMPP && p.ctx.GetSessionVars().IsMPPEnforced() &&
			!hasCostFlag(costFlag, CostFlagRecalculate) { // show the real cost in explain-statements
			p.planCost /= 1000000000
		}
	}
	if option.tracer != nil {
		setPhysicalTableReaderCostDetail(p, option.tracer,
			rowCount, rowSize, netFactor, netSeekCost, tableCost,
			sqlScanConcurrency, storeType)
	}
	p.planCostInit = true
	return p.planCost, nil
}

// GetNetDataSize calculates the estimated total data size fetched from storage.
func (p *PhysicalTableReader) GetNetDataSize() float64 {
	rowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	return p.tablePlan.StatsCount() * rowSize
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexMergeReader) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	p.planCost = 0
	if tblScan := p.tablePlan; tblScan != nil {
		childCost, err := tblScan.getPlanCostVer1(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		netFactor := getTableNetFactor(tblScan)
		p.planCost += childCost // child's cost
		tblStats := getTblStats(tblScan)
		rowSize := tblStats.GetAvgRowSize(p.ctx, tblScan.Schema().Columns, false, false)
		p.planCost += getCardinality(tblScan, costFlag) * rowSize * netFactor // net I/O cost
	}
	for _, partialScan := range p.partialPlans {
		childCost, err := partialScan.getPlanCostVer1(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		var isIdxScan bool
		for p := partialScan; ; p = p.Children()[0] {
			_, isIdxScan = p.(*PhysicalIndexScan)
			if len(p.Children()) == 0 {
				break
			}
		}

		netFactor := getTableNetFactor(partialScan)
		p.planCost += childCost // child's cost
		tblStats := getTblStats(partialScan)
		rowSize := tblStats.GetAvgRowSize(p.ctx, partialScan.Schema().Columns, isIdxScan, false)
		p.planCost += getCardinality(partialScan, costFlag) * rowSize * netFactor // net I/O cost
	}

	// TODO: accumulate table-side seek cost

	// consider concurrency
	copIterWorkers := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	p.planCost /= copIterWorkers
	p.planCostInit = true
	return p.planCost, nil
}

// GetPartialReaderNetDataSize returns the estimated total response data size of a partial read.
func (p *PhysicalIndexMergeReader) GetPartialReaderNetDataSize(plan PhysicalPlan) float64 {
	_, isIdxScan := plan.(*PhysicalIndexScan)
	return plan.StatsCount() * getTblStats(plan).GetAvgRowSize(p.ctx, plan.Schema().Columns, isIdxScan, false)
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTableScan) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	var selfCost float64
	var rowCount, rowSize, scanFactor float64
	costModelVersion := p.ctx.GetSessionVars().CostModelVersion
	scanFactor = p.ctx.GetSessionVars().GetScanFactor(p.Table)
	if p.Desc && p.prop != nil && p.prop.ExpectedCnt >= smallScanThreshold {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(p.Table)
	}
	rowCount = getCardinality(p, costFlag)
	rowSize = p.getScanRowSize()
	selfCost = rowCount * rowSize * scanFactor
	if option.tracer != nil {
		setPhysicalTableOrIndexScanCostDetail(p, option.tracer, rowCount, rowSize, scanFactor, costModelVersion)
	}
	p.planCost = selfCost
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexScan) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}

	var selfCost float64
	var rowCount, rowSize, scanFactor float64
	costModelVersion := p.ctx.GetSessionVars().CostModelVersion
	scanFactor = p.ctx.GetSessionVars().GetScanFactor(p.Table)
	if p.Desc && p.prop != nil && p.prop.ExpectedCnt >= smallScanThreshold {
		scanFactor = p.ctx.GetSessionVars().GetDescScanFactor(p.Table)
	}
	rowCount = getCardinality(p, costFlag)
	rowSize = p.getScanRowSize()
	selfCost = rowCount * rowSize * scanFactor
	if option.tracer != nil {
		setPhysicalTableOrIndexScanCostDetail(p, option.tracer, rowCount, rowSize, scanFactor, costModelVersion)
	}
	p.planCost = selfCost
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes the cost of index join operator and its children.
func (p *PhysicalIndexJoin) GetCost(outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.GetCPUFactor() * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.GetCPUFactor() * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * CPUFactor
	batchSize := math.Min(float64(p.ctx.GetSessionVars().IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * sessVars.GetCPUFactor()
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.GetCPUFactor()
	// CPU cost of building hash table for inner results:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * innerCnt * CPUFactor
	innerCPUCost += outerCnt * distinctFactor * innerCnt * sessVars.GetCPUFactor()
	innerConcurrency := float64(p.ctx.GetSessionVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of probing hash table in main thread.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) {
		numPairs = getOperatorActRows(p)
	}
	probeCost := numPairs * sessVars.GetCPUFactor()
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + (innerConcurrency+1.0)*sessVars.GetConcurrencyFactor()
	// Memory cost of hash tables for inner rows. The computed result is the upper bound,
	// since the executor is pipelined and not all workers are always in full load.
	memoryCost := innerConcurrency * (batchSize * distinctFactor) * innerCnt * sessVars.GetMemoryFactor()
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerPlanCost := outerCnt * innerCost
	if p.ctx.GetSessionVars().CostModelVersion == 2 {
		// IndexJoin executes a batch of rows at a time, so the actual cost of this part should be
		//  `innerCostPerBatch * numberOfBatches` instead of `innerCostPerRow * numberOfOuterRow`.
		// Use an empirical value batchRatio to handle this now.
		// TODO: remove this empirical value.
		batchRatio := 30.0
		innerPlanCost /= batchRatio
	}
	return outerCost + innerPlanCost + cpuCost + memoryCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexJoin) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.planCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes the cost of index merge join operator and its children.
func (p *PhysicalIndexHashJoin) GetCost(outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.GetCPUFactor() * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.GetCPUFactor() * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * CPUFactor
	batchSize := math.Min(float64(sessVars.IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * sessVars.GetCPUFactor()
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.GetCPUFactor()
	concurrency := float64(sessVars.IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / concurrency
	// CPU cost of building hash table for outer results concurrently.
	// (outerCnt / batchSize) * (batchSize * CPUFactor)
	outerCPUCost := outerCnt * sessVars.GetCPUFactor()
	cpuCost += outerCPUCost / concurrency
	// Cost of probing hash table concurrently.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) {
		numPairs = getOperatorActRows(p)
	}
	// Inner workers do hash join in parallel, but they can only save ONE outer
	// batch results. So as the number of outer batch exceeds inner concurrency,
	// it would fall back to linear execution. In a word, the hash join only runs
	// in parallel for the first `innerConcurrency` number of inner tasks.
	var probeCost float64
	if outerCnt/batchSize >= concurrency {
		probeCost = (numPairs - batchSize*innerCnt*(concurrency-1)) * sessVars.GetCPUFactor()
	} else {
		probeCost = batchSize * innerCnt * sessVars.GetCPUFactor()
	}
	cpuCost += probeCost
	// Cost of additional concurrent goroutines.
	cpuCost += (concurrency + 1.0) * sessVars.GetConcurrencyFactor()
	// Memory cost of hash tables for outer rows. The computed result is the upper bound,
	// since the executor is pipelined and not all workers are always in full load.
	memoryCost := concurrency * (batchSize * distinctFactor) * innerCnt * sessVars.GetMemoryFactor()
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerPlanCost := outerCnt * innerCost
	return outerCost + innerPlanCost + cpuCost + memoryCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexHashJoin) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.planCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes the cost of index merge join operator and its children.
func (p *PhysicalIndexMergeJoin) GetCost(outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.GetCPUFactor() * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.GetCPUFactor() * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (sortFactor + 1.0) * batchSize * cpuFactor
	// If `p.NeedOuterSort` is true, the sortFactor is batchSize * Log2(batchSize).
	// Otherwise, it's 0.
	batchSize := math.Min(float64(p.ctx.GetSessionVars().IndexJoinBatchSize), outerCnt)
	sortFactor := 0.0
	if p.NeedOuterSort {
		sortFactor = math.Log2(batchSize)
	}
	if batchSize > 2 {
		innerCPUCost += outerCnt * (sortFactor + 1.0) * sessVars.GetCPUFactor()
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * cpuFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.GetCPUFactor()
	innerConcurrency := float64(p.ctx.GetSessionVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of merge join in inner worker.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) {
		numPairs = getOperatorActRows(p)
	}
	avgProbeCnt := numPairs / outerCnt
	var probeCost float64
	// Inner workers do merge join in parallel, but they can only save ONE outer batch
	// results. So as the number of outer batch exceeds inner concurrency, it would fall back to
	// linear execution. In a word, the merge join only run in parallel for the first
	// `innerConcurrency` number of inner tasks.
	if outerCnt/batchSize >= innerConcurrency {
		probeCost = (numPairs - batchSize*avgProbeCnt*(innerConcurrency-1)) * sessVars.GetCPUFactor()
	} else {
		probeCost = batchSize * avgProbeCnt * sessVars.GetCPUFactor()
	}
	cpuCost += probeCost + (innerConcurrency+1.0)*sessVars.GetConcurrencyFactor()

	// Index merge join save the join results in inner worker.
	// So the memory cost consider the results size for each batch.
	memoryCost := innerConcurrency * (batchSize * avgProbeCnt) * sessVars.GetMemoryFactor()

	innerPlanCost := outerCnt * innerCost
	return outerCost + innerPlanCost + cpuCost + memoryCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexMergeJoin) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.planCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes the cost of apply operator.
func (p *PhysicalApply) GetCost(lCount, rCount, lCost, rCost float64) float64 {
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if len(p.LeftConditions) > 0 {
		cpuCost += lCount * sessVars.GetCPUFactor()
		lCount *= SelectionFactor
	}
	if len(p.RightConditions) > 0 {
		cpuCost += lCount * rCount * sessVars.GetCPUFactor()
		rCount *= SelectionFactor
	}
	if len(p.EqualConditions)+len(p.OtherConditions)+len(p.NAEqualConditions) > 0 {
		if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
			p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
			cpuCost += lCount * rCount * sessVars.GetCPUFactor() * 0.5
		} else {
			cpuCost += lCount * rCount * sessVars.GetCPUFactor()
		}
	}
	// Apply uses a NestedLoop method for execution.
	// For every row from the left(outer) side, it executes
	// the whole right(inner) plan tree. So the cost of apply
	// should be : apply cost + left cost + left count * right cost
	return cpuCost + lCost + lCount*rCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalApply) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	outerChild, innerChild := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	outerCost, err := outerChild.getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	p.planCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost)
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes cost of merge join operator itself.
func (p *PhysicalMergeJoin) GetCost(lCnt, rCnt float64, costFlag uint64) float64 {
	outerCnt := lCnt
	innerCnt := rCnt
	innerKeys := p.RightJoinKeys
	innerSchema := p.children[1].Schema()
	innerStats := p.children[1].statsInfo()
	if p.JoinType == RightOuterJoin {
		outerCnt = rCnt
		innerCnt = lCnt
		innerKeys = p.LeftJoinKeys
		innerSchema = p.children[0].Schema()
		innerStats = p.children[0].statsInfo()
	}
	helper := &fullJoinRowCountHelper{
		sctx:          p.SCtx(),
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	numPairs := helper.estimate()
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) {
		numPairs = getOperatorActRows(p)
	}
	sessVars := p.ctx.GetSessionVars()
	probeCost := numPairs * sessVars.GetCPUFactor()
	// Cost of evaluating outer filters.
	var cpuCost float64
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		probeCost *= SelectionFactor
		cpuCost += outerCnt * sessVars.GetCPUFactor()
	}
	cpuCost += probeCost
	// For merge join, only one group of rows with same join key(not null) are cached,
	// we compute average memory cost using estimated group size.
	NDV, _ := getColsNDVWithMatchedLen(innerKeys, innerSchema, innerStats)
	memoryCost := (innerCnt / NDV) * sessVars.GetMemoryFactor()
	return cpuCost + memoryCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalMergeJoin) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = 0
	for _, child := range p.children {
		childCost, err := child.getPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), getCardinality(p.children[1], costFlag), costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes cost of hash join operator itself.
func (p *PhysicalHashJoin) GetCost(lCnt, rCnt float64, isMPP bool, costFlag uint64, op *physicalOptimizeOp) float64 {
	buildCnt, probeCnt := lCnt, rCnt
	build := p.children[0]
	// Taking the right as the inner for right join or using the outer to build a hash table.
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		buildCnt, probeCnt = rCnt, lCnt
		build = p.children[1]
	}
	sessVars := p.ctx.GetSessionVars()
	oomUseTmpStorage := variable.EnableTmpStorageOnOOM.Load()
	memQuota := sessVars.MemTracker.GetBytesLimit() // sessVars.MemQuotaQuery && hint
	rowSize := getAvgRowSize(build.statsInfo(), build.Schema().Columns)
	spill := oomUseTmpStorage && memQuota > 0 && rowSize*buildCnt > float64(memQuota) && p.storeTp != kv.TiFlash
	// Cost of building hash table.
	cpuFactor := sessVars.GetCPUFactor()
	diskFactor := sessVars.GetDiskFactor()
	memoryFactor := sessVars.GetMemoryFactor()
	concurrencyFactor := sessVars.GetConcurrencyFactor()

	cpuCost := buildCnt * cpuFactor
	memoryCost := buildCnt * memoryFactor
	diskCost := buildCnt * diskFactor * rowSize
	// Number of matched row pairs regarding the equal join conditions.
	helper := &fullJoinRowCountHelper{
		sctx:            p.SCtx(),
		cartesian:       false,
		leftProfile:     p.children[0].statsInfo(),
		rightProfile:    p.children[1].statsInfo(),
		leftJoinKeys:    p.LeftJoinKeys,
		rightJoinKeys:   p.RightJoinKeys,
		leftSchema:      p.children[0].Schema(),
		rightSchema:     p.children[1].Schema(),
		leftNAJoinKeys:  p.LeftNAJoinKeys,
		rightNAJoinKeys: p.RightNAJoinKeys,
	}
	numPairs := helper.estimate()
	// For semi-join class, if `OtherConditions` is empty, we already know
	// the join results after querying hash table, otherwise, we have to
	// evaluate those resulted row pairs after querying hash table; if we
	// find one pair satisfying the `OtherConditions`, we then know the
	// join result for this given outer row, otherwise we have to iterate
	// to the end of those pairs; since we have no idea about when we can
	// terminate the iteration, we assume that we need to iterate half of
	// those pairs in average.
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) {
		numPairs = getOperatorActRows(p)
	}
	// Cost of querying hash table is cheap actually, so we just compute the cost of
	// evaluating `OtherConditions` and joining row pairs.
	probeCost := numPairs * cpuFactor
	probeDiskCost := numPairs * diskFactor * rowSize
	// Cost of evaluating outer filter.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		// Input outer count for the above compution should be adjusted by SelectionFactor.
		probeCost *= SelectionFactor
		probeDiskCost *= SelectionFactor
		probeCost += probeCnt * cpuFactor
	}
	diskCost += probeDiskCost
	probeCost /= float64(p.Concurrency)
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + float64(p.Concurrency+1)*concurrencyFactor
	// Cost of traveling the hash table to resolve missing matched cases when building the hash table from the outer table
	if p.UseOuterToBuild {
		if spill {
			// It runs in sequence when build data is on disk. See handleUnmatchedRowsFromHashTableInDisk
			cpuCost += buildCnt * cpuFactor
		} else {
			cpuCost += buildCnt * cpuFactor / float64(p.Concurrency)
		}
		diskCost += buildCnt * diskFactor * rowSize
	}

	if spill {
		memoryCost *= float64(memQuota) / (rowSize * buildCnt)
	} else {
		diskCost = 0
	}
	if op != nil {
		setPhysicalHashJoinCostDetail(p, op, spill, buildCnt, probeCnt, cpuFactor, rowSize, numPairs,
			cpuCost, probeCost, memoryCost, diskCost, probeDiskCost,
			diskFactor, memoryFactor, concurrencyFactor,
			memQuota)
	}
	return cpuCost + memoryCost + diskCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashJoin) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = 0
	for _, child := range p.children {
		childCost, err := child.getPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		p.planCost += childCost
	}
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), getCardinality(p.children[1], costFlag),
		taskType == property.MppTaskType, costFlag, option.tracer)
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes cost of stream aggregation considering CPU/memory.
func (p *PhysicalStreamAgg) GetCost(inputRows float64, isRoot, isMPP bool, costFlag uint64) float64 {
	aggFuncFactor := p.getAggFuncCostFactor(false)
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.GetCPUFactor() * aggFuncFactor
	} else {
		cpuCost = inputRows * sessVars.GetCopCPUFactor() * aggFuncFactor
	}
	rowsPerGroup := inputRows / getCardinality(p, costFlag)
	memoryCost := rowsPerGroup * distinctFactor * sessVars.GetMemoryFactor() * float64(p.numDistinctFunc())
	return cpuCost + memoryCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalStreamAgg) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), taskType == property.RootTaskType, taskType == property.MppTaskType, costFlag)
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes the cost of hash aggregation considering CPU/memory.
func (p *PhysicalHashAgg) GetCost(inputRows float64, isRoot, isMPP bool, costFlag uint64) float64 {
	cardinality := getCardinality(p, costFlag)
	numDistinctFunc := p.numDistinctFunc()
	aggFuncFactor := p.getAggFuncCostFactor(isMPP)
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.GetCPUFactor() * aggFuncFactor
		divisor, con := p.cpuCostDivisor(numDistinctFunc > 0)
		if divisor > 0 {
			cpuCost /= divisor
			// Cost of additional goroutines.
			cpuCost += (con + 1) * sessVars.GetConcurrencyFactor()
		}
	} else {
		cpuCost = inputRows * sessVars.GetCopCPUFactor() * aggFuncFactor
	}
	memoryCost := cardinality * sessVars.GetMemoryFactor() * float64(len(p.AggFuncs))
	// When aggregation has distinct flag, we would allocate a map for each group to
	// check duplication.
	memoryCost += inputRows * distinctFactor * sessVars.GetMemoryFactor() * float64(numDistinctFunc)
	return cpuCost + memoryCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashAgg) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	statsCnt := getCardinality(p.children[0], costFlag)
	switch taskType {
	case property.RootTaskType:
		p.planCost += p.GetCost(statsCnt, true, false, costFlag)
	case property.CopSingleReadTaskType, property.CopDoubleReadTaskType:
		p.planCost += p.GetCost(statsCnt, false, false, costFlag)
	case property.MppTaskType:
		p.planCost += p.GetCost(statsCnt, false, true, costFlag)
	default:
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes the cost of in memory sort.
func (p *PhysicalSort) GetCost(count float64, schema *expression.Schema) float64 {
	if count < 2.0 {
		count = 2.0
	}
	sessVars := p.ctx.GetSessionVars()
	cpuCost := count * math.Log2(count) * sessVars.GetCPUFactor()
	memoryCost := count * sessVars.GetMemoryFactor()

	oomUseTmpStorage := variable.EnableTmpStorageOnOOM.Load()
	memQuota := sessVars.MemTracker.GetBytesLimit() // sessVars.MemQuotaQuery && hint
	rowSize := getAvgRowSize(p.statsInfo(), schema.Columns)
	spill := oomUseTmpStorage && memQuota > 0 && rowSize*count > float64(memQuota)
	diskCost := count * sessVars.GetDiskFactor() * rowSize
	if !spill {
		diskCost = 0
	} else {
		memoryCost *= float64(memQuota) / (rowSize * count)
	}
	return cpuCost + memoryCost + diskCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalSort) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), p.Schema())
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost computes cost of TopN operator itself.
func (p *PhysicalTopN) GetCost(count float64, isRoot bool) float64 {
	heapSize := float64(p.Offset + p.Count)
	if heapSize < 2.0 {
		heapSize = 2.0
	}
	sessVars := p.ctx.GetSessionVars()
	// Ignore the cost of `doCompaction` in current implementation of `TopNExec`, since it is the
	// special side-effect of our Chunk format in TiDB layer, which may not exist in coprocessor's
	// implementation, or may be removed in the future if we change data format.
	// Note that we are using worst complexity to compute CPU cost, because it is simpler compared with
	// considering probabilities of average complexity, i.e, we may not need adjust heap for each input
	// row.
	var cpuCost float64
	if isRoot {
		cpuCost = count * math.Log2(heapSize) * sessVars.GetCPUFactor()
	} else {
		cpuCost = count * math.Log2(heapSize) * sessVars.GetCopCPUFactor()
	}
	memoryCost := heapSize * sessVars.GetMemoryFactor()
	return cpuCost + memoryCost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalTopN) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	p.planCost += p.GetCost(getCardinality(p.children[0], costFlag), taskType == property.RootTaskType)
	p.planCostInit = true
	return p.planCost, nil
}

// GetCost returns cost of the PointGetPlan.
func (p *BatchPointGetPlan) GetCost(opt *physicalOptimizeOp) float64 {
	cols := p.accessCols
	if cols == nil {
		return 0 // the cost of BatchGet generated in fast plan optimization is always 0
	}
	sessVars := p.ctx.GetSessionVars()
	var rowSize, rowCount float64
	cost := 0.0
	if p.IndexInfo == nil {
		rowCount = float64(len(p.Handles))
		rowSize = p.stats.HistColl.GetTableAvgRowSize(p.ctx, cols, kv.TiKV, true)
	} else {
		rowCount = float64(len(p.IndexValues))
		rowSize = p.stats.HistColl.GetIndexAvgRowSize(p.ctx, cols, p.IndexInfo.Unique)
	}
	networkFactor := sessVars.GetNetworkFactor(p.TblInfo)
	seekFactor := sessVars.GetSeekFactor(p.TblInfo)
	scanConcurrency := sessVars.DistSQLScanConcurrency()
	cost += rowCount * rowSize * networkFactor
	cost += rowCount * seekFactor
	cost /= float64(scanConcurrency)
	if opt != nil {
		setBatchPointGetPlanCostDetail(p, opt, rowCount, rowSize, networkFactor, seekFactor, scanConcurrency)
	}
	return cost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *BatchPointGetPlan) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = p.GetCost(option.tracer)
	p.planCostInit = true
	return p.planCost, nil
}

// GetAvgRowSize return the average row size.
func (p *BatchPointGetPlan) GetAvgRowSize() float64 {
	cols := p.accessCols
	if cols == nil {
		return 0 // the cost of BatchGet generated in fast plan optimization is always 0
	}
	if p.IndexInfo == nil {
		return p.stats.HistColl.GetTableAvgRowSize(p.ctx, cols, kv.TiKV, true)
	}
	return p.stats.HistColl.GetIndexAvgRowSize(p.ctx, cols, p.IndexInfo.Unique)
}

// GetCost returns cost of the PointGetPlan.
func (p *PointGetPlan) GetCost(opt *physicalOptimizeOp) float64 {
	cols := p.accessCols
	if cols == nil {
		return 0 // the cost of PointGet generated in fast plan optimization is always 0
	}
	sessVars := p.ctx.GetSessionVars()
	var rowSize float64
	cost := 0.0
	if p.IndexInfo == nil {
		rowSize = p.stats.HistColl.GetTableAvgRowSize(p.ctx, cols, kv.TiKV, true)
	} else {
		rowSize = p.stats.HistColl.GetIndexAvgRowSize(p.ctx, cols, p.IndexInfo.Unique)
	}
	networkFactor := sessVars.GetNetworkFactor(p.TblInfo)
	seekFactor := sessVars.GetSeekFactor(p.TblInfo)
	cost += rowSize * networkFactor
	cost += seekFactor
	cost /= float64(sessVars.DistSQLScanConcurrency())
	if opt != nil {
		setPointGetPlanCostDetail(p, opt, rowSize, networkFactor, seekFactor)
	}
	return cost
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PointGetPlan) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	p.planCost = p.GetCost(option.tracer)
	p.planCostInit = true
	return p.planCost, nil
}

// GetAvgRowSize return the average row size.
func (p *PointGetPlan) GetAvgRowSize() float64 {
	cols := p.accessCols
	if cols == nil {
		return 0 // the cost of PointGet generated in fast plan optimization is always 0
	}
	if p.IndexInfo == nil {
		return p.stats.HistColl.GetTableAvgRowSize(p.ctx, cols, kv.TiKV, true)
	}
	return p.stats.HistColl.GetIndexAvgRowSize(p.ctx, cols, p.IndexInfo.Unique)
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalUnionAll) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	var childMaxCost float64
	for _, child := range p.children {
		childCost, err := child.getPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		childMaxCost = math.Max(childMaxCost, childCost)
	}
	p.planCost = childMaxCost + float64(1+len(p.children))*p.ctx.GetSessionVars().GetConcurrencyFactor()
	p.planCostInit = true
	return p.planCost, nil
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalExchangeReceiver) getPlanCostVer1(taskType property.TaskType, option *PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.planCostInit && !hasCostFlag(costFlag, CostFlagRecalculate) {
		return p.planCost, nil
	}
	childCost, err := p.children[0].getPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.planCost = childCost
	// accumulate net cost
	p.planCost += getCardinality(p.children[0], costFlag) * p.ctx.GetSessionVars().GetNetworkFactor(nil)
	p.planCostInit = true
	return p.planCost, nil
}

func getOperatorActRows(operator PhysicalPlan) float64 {
	if operator == nil {
		return 0
	}
	runtimeInfo := operator.SCtx().GetSessionVars().StmtCtx.RuntimeStatsColl
	id := operator.ID()
	actRows := 0.0
	if runtimeInfo.ExistsRootStats(id) {
		actRows = float64(runtimeInfo.GetRootStats(id).GetActRows())
	} else if runtimeInfo.ExistsCopStats(id) {
		actRows = float64(runtimeInfo.GetCopStats(id).GetActRows())
	} else {
		actRows = 0.0 // no data for this operator
	}
	return actRows
}

func getCardinality(operator PhysicalPlan, costFlag uint64) float64 {
	if hasCostFlag(costFlag, CostFlagUseTrueCardinality) {
		actualProbeCnt := operator.getActualProbeCnt(operator.SCtx().GetSessionVars().StmtCtx.RuntimeStatsColl)
		return getOperatorActRows(operator) / float64(actualProbeCnt)
	}
	rows := operator.StatsCount()
	if rows == 0 && operator.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
		// 0 est-row can lead to 0 operator cost which makes plan choice unstable.
		rows = 1
	}
	return rows
}

// estimateNetSeekCost calculates the net seek cost for the plan.
// for TiKV, it's len(access-range) * seek-factor,
// and for TiFlash, it's len(access-range) * len(access-column) * seek-factor.
func estimateNetSeekCost(copTaskPlan PhysicalPlan) float64 {
	switch x := copTaskPlan.(type) {
	case *PhysicalTableScan:
		if x.StoreType == kv.TiFlash { // the old TiFlash interface uses cop-task protocol
			return float64(len(x.Ranges)) * float64(len(x.Columns)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table)
		}
		return float64(len(x.Ranges)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table) // TiKV
	case *PhysicalIndexScan:
		return float64(len(x.Ranges)) * x.ctx.GetSessionVars().GetSeekFactor(x.Table) // TiKV
	default:
		return estimateNetSeekCost(copTaskPlan.Children()[0])
	}
}

// getTblStats returns the tbl-stats of this plan, which contains all columns before pruning.
func getTblStats(copTaskPlan PhysicalPlan) *statistics.HistColl {
	switch x := copTaskPlan.(type) {
	case *PhysicalTableScan:
		return x.tblColHists
	case *PhysicalIndexScan:
		return x.tblColHists
	default:
		return getTblStats(copTaskPlan.Children()[0])
	}
}

// getTableNetFactor returns the corresponding net factor of this table, it's mainly for temporary tables
func getTableNetFactor(copTaskPlan PhysicalPlan) float64 {
	switch x := copTaskPlan.(type) {
	case *PhysicalTableScan:
		return x.SCtx().GetSessionVars().GetNetworkFactor(x.Table)
	case *PhysicalIndexScan:
		return x.SCtx().GetSessionVars().GetNetworkFactor(x.Table)
	default:
		if len(x.Children()) == 0 {
			x.SCtx().GetSessionVars().GetNetworkFactor(nil)
		}
		return getTableNetFactor(x.Children()[0])
	}
}
