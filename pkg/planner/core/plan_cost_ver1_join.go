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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/util/paging"
)

// getCost4PhysicalIndexLookUpReader computes cost of index lookup operator itself.
func getCost4PhysicalIndexLookUpReader(pp base.PhysicalPlan, costFlag uint64) (cost float64) {
	p := pp.(*physicalop.PhysicalIndexLookUpReader)
	indexPlan, tablePlan := p.IndexPlan, p.TablePlan
	ctx := p.SCtx()
	sessVars := ctx.GetSessionVars()
	// Add cost of building table reader executors. Handles are extracted in batch style,
	// each handle is a range, the CPU cost of building copTasks should be:
	// (indexRows / batchSize) * batchSize * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	indexRows := getCardinality(indexPlan, costFlag)
	idxCst := indexRows * sessVars.GetCPUFactor()
	// if the expectCnt is below the paging threshold, using paging API, recalculate idxCst.
	// paging API reduces the count of index and table rows, however introduces more seek cost.
	if ctx.GetSessionVars().EnablePaging && p.ExpectedCnt > 0 && p.ExpectedCnt <= paging.Threshold {
		p.Paging = true
		pagingCst := calcPagingCost(ctx, p.IndexPlan, p.ExpectedCnt)
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
	if p.KeepOrder && batchSize > 2 {
		sortCPUCost := (tableRows * math.Log2(batchSize) * sessVars.GetCPUFactor()) / numTblWorkers
		cost += sortCPUCost
	}
	return
}

// getPlanCostVer14PhysicalIndexLookUpReader calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalIndexLookUpReader(pp base.PhysicalPlan, _ property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalIndexLookUpReader)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}

	p.PlanCost = 0
	// child's cost
	for _, child := range []base.PhysicalPlan{p.IndexPlan, p.TablePlan} {
		childCost, err := child.GetPlanCostVer1(property.CopMultiReadTaskType, option)
		if err != nil {
			return 0, err
		}
		p.PlanCost += childCost
	}

	// to keep compatible with the previous cost implementation, re-calculate table-scan cost by using index stats-count again (see copTask.finishIndexPlan).
	// TODO: amend table-side cost here later
	var tmp = p.TablePlan
	for len(tmp.Children()) > 0 {
		tmp = tmp.Children()[0]
	}
	ts := tmp.(*physicalop.PhysicalTableScan)
	tblCost, err := ts.GetPlanCostVer1(property.CopMultiReadTaskType, option)
	if err != nil {
		return 0, err
	}
	p.PlanCost -= tblCost
	p.PlanCost += getCardinality(p.IndexPlan, costFlag) * ts.GetScanRowSize() * p.SCtx().GetSessionVars().GetScanFactor(ts.Table)

	// index-side net I/O cost: rows * row-size * net-factor
	netFactor := getTableNetFactor(p.TablePlan)
	rowSize := cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.IndexPlan), p.IndexPlan.Schema().Columns, true, false)
	p.PlanCost += getCardinality(p.IndexPlan, costFlag) * rowSize * netFactor

	// index-side net seek cost
	p.PlanCost += estimateNetSeekCost(p.IndexPlan)

	// table-side net I/O cost: rows * row-size * net-factor
	tblRowSize := cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.TablePlan), p.TablePlan.Schema().Columns, false, false)
	p.PlanCost += getCardinality(p.TablePlan, costFlag) * tblRowSize * netFactor

	// table-side seek cost
	p.PlanCost += estimateNetSeekCost(p.TablePlan)

	// consider concurrency
	p.PlanCost /= float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())

	// lookup-cpu-cost in TiDB
	p.PlanCost += p.GetCost(costFlag)
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getPlanCostVer14PhysicalIndexReader calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalIndexReader(pp base.PhysicalPlan, _ property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalIndexReader)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}

	var rowCount, rowSize, netFactor, indexPlanCost, netSeekCost float64
	sqlScanConcurrency := p.SCtx().GetSessionVars().DistSQLScanConcurrency()
	// child's cost
	childCost, err := p.IndexPlan.GetPlanCostVer1(property.CopSingleReadTaskType, option)
	if err != nil {
		return 0, err
	}
	indexPlanCost = childCost
	p.PlanCost = indexPlanCost
	// net I/O cost: rows * row-size * net-factor
	tblStats := physicalop.GetTblStats(p.IndexPlan)
	rowSize = cardinality.GetAvgRowSize(p.SCtx(), tblStats, p.IndexPlan.Schema().Columns, true, false)
	rowCount = getCardinality(p.IndexPlan, costFlag)
	netFactor = getTableNetFactor(p.IndexPlan)
	p.PlanCost += rowCount * rowSize * netFactor
	// net seek cost
	netSeekCost = estimateNetSeekCost(p.IndexPlan)
	p.PlanCost += netSeekCost
	// consider concurrency
	p.PlanCost /= float64(sqlScanConcurrency)

	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getPlanCostVer14PhysicalTableReader calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalTableReader(pp base.PhysicalPlan, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalTableReader)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}

	p.PlanCost = 0
	netFactor := getTableNetFactor(p.TablePlan)
	var rowCount, rowSize, netSeekCost float64
	sqlScanConcurrency := p.SCtx().GetSessionVars().DistSQLScanConcurrency()
	storeType := p.StoreType
	switch storeType {
	case kv.TiKV:
		// child's cost
		childCost, err := p.TablePlan.GetPlanCostVer1(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		p.PlanCost = childCost
		// net I/O cost: rows * row-size * net-factor
		rowSize = cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.TablePlan), p.TablePlan.Schema().Columns, false, false)
		rowCount = getCardinality(p.TablePlan, costFlag)
		p.PlanCost += rowCount * rowSize * netFactor
		// net seek cost
		netSeekCost = estimateNetSeekCost(p.TablePlan)
		p.PlanCost += netSeekCost
		// consider concurrency
		p.PlanCost /= float64(sqlScanConcurrency)
	case kv.TiFlash:
		var concurrency, rowSize, seekCost float64
		_, isMPP := p.TablePlan.(*physicalop.PhysicalExchangeSender)
		if isMPP {
			// mpp protocol
			concurrency = p.SCtx().GetSessionVars().CopTiFlashConcurrencyFactor
			rowSize = collectRowSizeFromMPPPlan(p.TablePlan)
			seekCost = accumulateNetSeekCost4MPP(p.TablePlan)
			childCost, err := p.TablePlan.GetPlanCostVer1(property.MppTaskType, option)
			if err != nil {
				return 0, err
			}
			p.PlanCost = childCost
		} else {
			// cop protocol
			concurrency = float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())
			rowSize = cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.TablePlan), p.TablePlan.Schema().Columns, false, false)
			seekCost = estimateNetSeekCost(p.TablePlan)
			tType := property.CopSingleReadTaskType
			childCost, err := p.TablePlan.GetPlanCostVer1(tType, option)
			if err != nil {
				return 0, err
			}
			p.PlanCost = childCost
		}

		// net I/O cost
		p.PlanCost += getCardinality(p.TablePlan, costFlag) * rowSize * netFactor
		// net seek cost
		p.PlanCost += seekCost
		// consider concurrency
		p.PlanCost /= concurrency
		// consider tidb_enforce_mpp
		if isMPP && p.SCtx().GetSessionVars().IsMPPEnforced() &&
			!hasCostFlag(costFlag, costusage.CostFlagRecalculate) { // show the real cost in explain-statements
			p.PlanCost /= 1000000000
		}
	}
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// GetPlanCostVer14PhysicalIndexMergeReader calculates the cost of the plan if it has not been calculated yet and returns the cost.
func GetPlanCostVer14PhysicalIndexMergeReader(pp base.PhysicalPlan, _ property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalIndexMergeReader)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}

	p.PlanCost = 0
	if tblScan := p.TablePlan; tblScan != nil {
		childCost, err := tblScan.GetPlanCostVer1(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		netFactor := getTableNetFactor(tblScan)
		p.PlanCost += childCost // child's cost
		tblStats := physicalop.GetTblStats(tblScan)
		rowSize := cardinality.GetAvgRowSize(p.SCtx(), tblStats, tblScan.Schema().Columns, false, false)
		p.PlanCost += getCardinality(tblScan, costFlag) * rowSize * netFactor // net I/O cost
	}
	for _, partialScan := range p.PartialPlansRaw {
		childCost, err := partialScan.GetPlanCostVer1(property.CopSingleReadTaskType, option)
		if err != nil {
			return 0, err
		}
		var isIdxScan bool
		for p := partialScan; ; p = p.Children()[0] {
			_, isIdxScan = p.(*physicalop.PhysicalIndexScan)
			if len(p.Children()) == 0 {
				break
			}
		}

		netFactor := getTableNetFactor(partialScan)
		p.PlanCost += childCost // child's cost
		tblStats := physicalop.GetTblStats(partialScan)
		rowSize := cardinality.GetAvgRowSize(p.SCtx(), tblStats, partialScan.Schema().Columns, isIdxScan, false)
		p.PlanCost += getCardinality(partialScan, costFlag) * rowSize * netFactor // net I/O cost
	}

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
		p.PlanCost = p.PlanCost * 0.99
	}

	// TODO: accumulate table-side seek cost

	// consider concurrency
	copIterWorkers := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())
	p.PlanCost /= copIterWorkers
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getPlanCostVer14PhysicalTableScan calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalTableScan(pp base.PhysicalPlan, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalTableScan)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}

	var selfCost float64
	var rowCount, rowSize, scanFactor float64
	scanFactor = p.SCtx().GetSessionVars().GetScanFactor(p.Table)
	if p.Desc && p.Prop != nil && p.Prop.ExpectedCnt >= cost.SmallScanThreshold {
		scanFactor = p.SCtx().GetSessionVars().GetDescScanFactor(p.Table)
	}
	rowCount = getCardinality(p, costFlag)
	rowSize = p.GetScanRowSize()
	selfCost = rowCount * rowSize * scanFactor
	p.PlanCost = selfCost
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalIndexScan(pp base.PhysicalPlan, _ property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalIndexScan)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}

	var selfCost float64
	var rowCount, rowSize, scanFactor float64
	scanFactor = p.SCtx().GetSessionVars().GetScanFactor(p.Table)
	if p.Desc && p.Prop != nil && p.Prop.ExpectedCnt >= cost.SmallScanThreshold {
		scanFactor = p.SCtx().GetSessionVars().GetDescScanFactor(p.Table)
	}
	rowCount = getCardinality(p, costFlag)
	rowSize = p.GetScanRowSize()
	selfCost = rowCount * rowSize * scanFactor
	p.PlanCost = selfCost
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// GetCost computes the cost of index join operator and its children.
func getCost4PhysicalIndexJoin(pp base.PhysicalPlan, outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	p := pp.(*physicalop.PhysicalIndexJoin)
	var cpuCost float64
	sessVars := p.SCtx().GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.GetCPUFactor() * outerCnt
		outerCnt *= cost.SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.GetCPUFactor() * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * CPUFactor
	batchSize := math.Min(float64(p.SCtx().GetSessionVars().IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * sessVars.GetCPUFactor()
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * DistinctFactor) * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * cost.DistinctFactor * sessVars.GetCPUFactor()
	// CPU cost of building hash table for inner results:
	// (outerCnt / batchSize) * (batchSize * DistinctFactor) * innerCnt * CPUFactor
	innerCPUCost += outerCnt * cost.DistinctFactor * innerCnt * sessVars.GetCPUFactor()
	innerConcurrency := float64(p.SCtx().GetSessionVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of probing hash table in main thread.
	numPairs := outerCnt * innerCnt
	if p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin ||
		p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	if hasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) {
		numPairs = getOperatorActRows(p)
	}
	probeCost := numPairs * sessVars.GetCPUFactor()
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + (innerConcurrency+1.0)*sessVars.GetConcurrencyFactor()
	// Memory cost of hash tables for inner rows. The computed result is the upper bound,
	// since the executor is pipelined and not all workers are always in full load.
	memoryCost := innerConcurrency * (batchSize * cost.DistinctFactor) * innerCnt * sessVars.GetMemoryFactor()
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerPlanCost := outerCnt * innerCost
	if p.SCtx().GetSessionVars().CostModelVersion == 2 {
		// IndexJoin executes a batch of rows at a time, so the actual cost of this part should be
		//  `innerCostPerBatch * numberOfBatches` instead of `innerCostPerRow * numberOfOuterRow`.
		// Use an empirical value batchRatio to handle this now.
		// TODO: remove this empirical value.
		batchRatio := 30.0
		innerPlanCost /= batchRatio
	}
	return outerCost + innerPlanCost + cpuCost + memoryCost
}

// getPlanCostVer14PhysicalIndexJoin calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalIndexJoin(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalIndexJoin)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	outerChild, innerChild := p.Children()[1-p.InnerChildIdx], p.Children()[p.InnerChildIdx]
	outerCost, err := outerChild.GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.PlanCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4PhysicalIndexHashJoin computes the cost of index merge join operator and its children.
func getCost4PhysicalIndexHashJoin(pp base.PhysicalPlan, outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	p := pp.(*physicalop.PhysicalIndexHashJoin)
	var cpuCost float64
	sessVars := p.SCtx().GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.GetCPUFactor() * outerCnt
		outerCnt *= cost.SelectionFactor
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
	// (outerCnt / batchSize) * (batchSize * DistinctFactor) * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * cost.DistinctFactor * sessVars.GetCPUFactor()
	concurrency := float64(sessVars.IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / concurrency
	// CPU cost of building hash table for outer results concurrently.
	// (outerCnt / batchSize) * (batchSize * CPUFactor)
	outerCPUCost := outerCnt * sessVars.GetCPUFactor()
	cpuCost += outerCPUCost / concurrency
	// Cost of probing hash table concurrently.
	numPairs := outerCnt * innerCnt
	if p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin ||
		p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	if hasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) {
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
	memoryCost := concurrency * (batchSize * cost.DistinctFactor) * innerCnt * sessVars.GetMemoryFactor()
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerPlanCost := outerCnt * innerCost
	return outerCost + innerPlanCost + cpuCost + memoryCost
}

// getPlanCostVer1PhysicalIndexHashJoin calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer1PhysicalIndexHashJoin(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalIndexHashJoin)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	outerChild, innerChild := p.Children()[1-p.InnerChildIdx], p.Children()[p.InnerChildIdx]
	outerCost, err := outerChild.GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	innerCost, err := innerChild.GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	outerCnt := getCardinality(outerChild, costFlag)
	innerCnt := getCardinality(innerChild, costFlag)
	if hasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) && outerCnt > 0 {
		innerCnt /= outerCnt // corresponding to one outer row when calculating IndexJoin costs
		innerCost /= outerCnt
	}
	p.PlanCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost, costFlag)
	p.PlanCostInit = true
	return p.PlanCost, nil
}
