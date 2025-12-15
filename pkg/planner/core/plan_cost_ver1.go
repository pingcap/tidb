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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/paging"
)

const (
	modelVer1 = 1
	modelVer2 = 2
)

func hasCostFlag(costFlag, flag uint64) bool {
	return (costFlag & flag) > 0
}

// getPlanCostVer14PhysicalSelection calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalSelection(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalSelection)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}

	var selfCost float64
	var cpuFactor float64
	switch taskType {
	case property.RootTaskType, property.MppTaskType:
		cpuFactor = p.SCtx().GetSessionVars().GetCPUFactor()
	case property.CopSingleReadTaskType, property.CopMultiReadTaskType:
		cpuFactor = p.SCtx().GetSessionVars().GetCopCPUFactor()
	default:
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	selfCost = getCardinality(p.Children()[0], costFlag) * cpuFactor
	if p.FromDataSource {
		selfCost = 0 // for compatibility, see https://github.com/pingcap/tidb/issues/36243
	}

	childCost, err := p.Children()[0].GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.PlanCost = childCost + selfCost
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4PhysicalProjection computes the cost of projection operator itself.
func getCost4PhysicalProjection(pp base.PhysicalPlan, count float64) float64 {
	p := pp.(*physicalop.PhysicalProjection)
	sessVars := p.SCtx().GetSessionVars()
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
func getPlanCostVer14PhysicalProjection(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalProjection)
	intest.Assert(p.SCtx().GetSessionVars().CostModelVersion != 0)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	childCost, err := p.Children()[0].GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.PlanCost = childCost
	p.PlanCost += getCost4PhysicalProjection(p, getCardinality(p, costFlag)) // projection cost
	p.PlanCostInit = true
	return p.PlanCost, nil
}

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

// getCost4PhysicalIndexMergeJoin computes the cost of index merge join operator and its children.
func getCost4PhysicalIndexMergeJoin(pp base.PhysicalPlan, outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	p := pp.(*physicalop.PhysicalIndexMergeJoin)
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
	// (outerCnt / batchSize) * (sortFactor + 1.0) * batchSize * cpuFactor
	// If `p.NeedOuterSort` is true, the sortFactor is batchSize * Log2(batchSize).
	// Otherwise, it's 0.
	batchSize := math.Min(float64(p.SCtx().GetSessionVars().IndexJoinBatchSize), outerCnt)
	sortFactor := 0.0
	if p.NeedOuterSort {
		sortFactor = math.Log2(batchSize)
	}
	if batchSize > 2 {
		innerCPUCost += outerCnt * (sortFactor + 1.0) * sessVars.GetCPUFactor()
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * DistinctFactor) * cpuFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * cost.DistinctFactor * sessVars.GetCPUFactor()
	innerConcurrency := float64(p.SCtx().GetSessionVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of merge join in inner worker.
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

// getPlanCostVer14PhysicalIndexMergeJoin calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalIndexMergeJoin(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalIndexMergeJoin)
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

// getCost4PhysicalApply computes the cost of apply operator.
func getCost4PhysicalApply(pp base.PhysicalPlan, lCount, rCount, lCost, rCost float64) float64 {
	var cpuCost float64
	p := pp.(*physicalop.PhysicalApply)
	sessVars := p.SCtx().GetSessionVars()
	if len(p.LeftConditions) > 0 {
		cpuCost += lCount * sessVars.GetCPUFactor()
		lCount *= cost.SelectionFactor
	}
	if len(p.RightConditions) > 0 {
		cpuCost += lCount * rCount * sessVars.GetCPUFactor()
		rCount *= cost.SelectionFactor
	}
	if len(p.EqualConditions)+len(p.OtherConditions)+len(p.NAEqualConditions) > 0 {
		if p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin ||
			p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
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

// getPlanCostVer14PhysicalApply calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalApply(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalApply)
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
	p.PlanCost = p.GetCost(outerCnt, innerCnt, outerCost, innerCost)
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4PhysicalMergeJoin computes cost of merge join operator itself.
func getCost4PhysicalMergeJoin(pp base.PhysicalPlan, lCnt, rCnt float64, costFlag uint64) float64 {
	p := pp.(*physicalop.PhysicalMergeJoin)
	outerCnt := lCnt
	innerCnt := rCnt
	innerKeys := p.RightJoinKeys
	innerSchema := p.Children()[1].Schema()
	innerStats := p.Children()[1].StatsInfo()
	if p.JoinType == base.RightOuterJoin {
		outerCnt = rCnt
		innerCnt = lCnt
		innerKeys = p.LeftJoinKeys
		innerSchema = p.Children()[0].Schema()
		innerStats = p.Children()[0].StatsInfo()
	}
	numPairs := cardinality.EstimateFullJoinRowCount(p.SCtx(), false,
		p.Children()[0].StatsInfo(), p.Children()[1].StatsInfo(),
		p.LeftJoinKeys, p.RightJoinKeys,
		p.Children()[0].Schema(), p.Children()[1].Schema(),
		p.LeftNAJoinKeys, p.RightNAJoinKeys)
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
	sessVars := p.SCtx().GetSessionVars()
	probeCost := numPairs * sessVars.GetCPUFactor()
	// Cost of evaluating outer filters.
	var cpuCost float64
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		probeCost *= cost.SelectionFactor
		cpuCost += outerCnt * sessVars.GetCPUFactor()
	}
	cpuCost += probeCost
	// For merge join, only one group of rows with same join key(not null) are cached,
	// we compute average memory cost using estimated group size.
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(p.SCtx(), innerKeys, innerSchema, innerStats)
	memoryCost := (innerCnt / ndv) * sessVars.GetMemoryFactor()
	return cpuCost + memoryCost
}

// getPlanCostVer14PhysicalMergeJoin calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalMergeJoin(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalMergeJoin)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	p.PlanCost = 0
	for _, child := range p.Children() {
		childCost, err := child.GetPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		p.PlanCost += childCost
	}
	p.PlanCost += p.GetCost(getCardinality(p.Children()[0], costFlag), getCardinality(p.Children()[1], costFlag), costFlag)
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4PhysicalHashJoin computes cost of hash join operator itself.
func getCost4PhysicalHashJoin(pp base.PhysicalPlan, lCnt, rCnt float64, costFlag uint64) float64 {
	p := pp.(*physicalop.PhysicalHashJoin)
	buildCnt, probeCnt := lCnt, rCnt
	build := p.Children()[0]
	// Taking the right as the inner for right join or using the outer to build a hash table.
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		buildCnt, probeCnt = rCnt, lCnt
		build = p.Children()[1]
	}
	sessVars := p.SCtx().GetSessionVars()
	oomUseTmpStorage := vardef.EnableTmpStorageOnOOM.Load()
	memQuota := sessVars.MemTracker.GetBytesLimit() // sessVars.MemQuotaQuery && hint
	rowSize := getAvgRowSize(build.StatsInfo(), build.Schema().Columns)
	spill := oomUseTmpStorage && memQuota > 0 && rowSize*buildCnt > float64(memQuota) && p.StoreTp != kv.TiFlash
	// Cost of building hash table.
	cpuFactor := sessVars.GetCPUFactor()
	diskFactor := sessVars.GetDiskFactor()
	memoryFactor := sessVars.GetMemoryFactor()
	concurrencyFactor := sessVars.GetConcurrencyFactor()

	cpuCost := buildCnt * cpuFactor
	memoryCost := buildCnt * memoryFactor
	diskCost := buildCnt * diskFactor * rowSize
	// Number of matched row pairs regarding the equal join conditions.
	numPairs := cardinality.EstimateFullJoinRowCount(p.SCtx(), false,
		p.Children()[0].StatsInfo(), p.Children()[1].StatsInfo(),
		p.LeftJoinKeys, p.RightJoinKeys,
		p.Children()[0].Schema(), p.Children()[1].Schema(),
		p.LeftNAJoinKeys, p.RightNAJoinKeys)
	// For semi-join class, if `OtherConditions` is empty, we already know
	// the join results after querying hash table, otherwise, we have to
	// evaluate those resulted row pairs after querying hash table; if we
	// find one pair satisfying the `OtherConditions`, we then know the
	// join result for this given outer row, otherwise we have to iterate
	// to the end of those pairs; since we have no idea about when we can
	// terminate the iteration, we assume that we need to iterate half of
	// those pairs in average.
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
	// Cost of querying hash table is cheap actually, so we just compute the cost of
	// evaluating `OtherConditions` and joining row pairs.
	probeCost := numPairs * cpuFactor
	probeDiskCost := numPairs * diskFactor * rowSize
	// Cost of evaluating outer filter.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		// Input outer count for the above compution should be adjusted by SelectionFactor.
		probeCost *= cost.SelectionFactor
		probeDiskCost *= cost.SelectionFactor
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
	return cpuCost + memoryCost + diskCost
}

// getPlanCostVer14PhysicalHashJoin calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalHashJoin(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalHashJoin)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	p.PlanCost = 0
	for _, child := range p.Children() {
		childCost, err := child.GetPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		p.PlanCost += childCost
	}
	p.PlanCost += p.GetCost(getCardinality(p.Children()[0], costFlag), getCardinality(p.Children()[1], costFlag),
		taskType == property.MppTaskType, costFlag)
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4PhysicalStreamAgg computes cost of stream aggregation considering CPU/memory.
func getCost4PhysicalStreamAgg(pp base.PhysicalPlan, inputRows float64, isRoot bool, costFlag uint64) float64 {
	p := pp.(*physicalop.PhysicalStreamAgg)
	aggFuncFactor := p.GetAggFuncCostFactor(false)
	var cpuCost float64
	sessVars := p.SCtx().GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.GetCPUFactor() * aggFuncFactor
	} else {
		cpuCost = inputRows * sessVars.GetCopCPUFactor() * aggFuncFactor
	}
	rowsPerGroup := inputRows / getCardinality(p, costFlag)
	memoryCost := rowsPerGroup * cost.DistinctFactor * sessVars.GetMemoryFactor() * float64(p.NumDistinctFunc())
	return cpuCost + memoryCost
}

// getPlanCostVer14PhysicalStreamAgg calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalStreamAgg(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalStreamAgg)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	childCost, err := p.Children()[0].GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.PlanCost = childCost
	p.PlanCost += getCost4PhysicalStreamAgg(p, getCardinality(p.Children()[0], costFlag), taskType == property.RootTaskType, costFlag)
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4PhysicalHashAgg computes the cost of hash aggregation considering CPU/memory.
func getCost4PhysicalHashAgg(pp base.PhysicalPlan, inputRows float64, isRoot, isMPP bool, costFlag uint64) float64 {
	p := pp.(*physicalop.PhysicalHashAgg)
	cardinality := getCardinality(p, costFlag)
	numDistinctFunc := p.NumDistinctFunc()
	aggFuncFactor := p.GetAggFuncCostFactor(isMPP)
	var cpuCost float64
	sessVars := p.SCtx().GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.GetCPUFactor() * aggFuncFactor
		divisor, con := p.CPUCostDivisor(numDistinctFunc > 0)
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
	memoryCost += inputRows * cost.DistinctFactor * sessVars.GetMemoryFactor() * float64(numDistinctFunc)
	return cpuCost + memoryCost
}

// getPlanCostVer14PhysicalHashAgg calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalHashAgg(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalHashAgg)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	childCost, err := p.Children()[0].GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.PlanCost = childCost
	statsCnt := getCardinality(p.Children()[0], costFlag)
	switch taskType {
	case property.RootTaskType:
		p.PlanCost += getCost4PhysicalHashAgg(p, statsCnt, true, false, costFlag)
	case property.CopSingleReadTaskType, property.CopMultiReadTaskType:
		p.PlanCost += getCost4PhysicalHashAgg(p, statsCnt, false, false, costFlag)
	case property.MppTaskType:
		p.PlanCost += getCost4PhysicalHashAgg(p, statsCnt, false, true, costFlag)
	default:
		return 0, errors.Errorf("unknown task type %v", taskType)
	}
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4PhysicalSort computes the cost of in memory sort.
func getCost4PhysicalSort(pp base.PhysicalPlan, count float64, schema *expression.Schema) float64 {
	p := pp.(*physicalop.PhysicalSort)
	if count < 2.0 {
		count = 2.0
	}
	sessVars := p.SCtx().GetSessionVars()
	cpuCost := count * math.Log2(count) * sessVars.GetCPUFactor()
	memoryCost := count * sessVars.GetMemoryFactor()

	oomUseTmpStorage := vardef.EnableTmpStorageOnOOM.Load()
	memQuota := sessVars.MemTracker.GetBytesLimit() // sessVars.MemQuotaQuery && hint
	rowSize := getAvgRowSize(p.StatsInfo(), schema.Columns)
	spill := oomUseTmpStorage && memQuota > 0 && rowSize*count > float64(memQuota)
	diskCost := count * sessVars.GetDiskFactor() * rowSize
	if !spill {
		diskCost = 0
	} else {
		memoryCost *= float64(memQuota) / (rowSize * count)
	}
	return cpuCost + memoryCost + diskCost
}

// getPlanCostVer14PhysicalSort calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalSort(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalSort)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	childCost, err := p.Children()[0].GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.PlanCost = childCost
	p.PlanCost += p.GetCost(getCardinality(p.Children()[0], costFlag), p.Schema())
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalTopN(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalTopN)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	childCost, err := p.Children()[0].GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.PlanCost = childCost
	p.PlanCost += p.GetCost(getCardinality(p.Children()[0], costFlag), taskType == property.RootTaskType)
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4BatchPointGetPlan returns cost of the BatchPointGetPlan.
func getCost4BatchPointGetPlan(pp base.PhysicalPlan) float64 {
	p := pp.(*physicalop.BatchPointGetPlan)
	cols := p.AccessCols()
	if cols == nil {
		return 0 // the cost of BatchGet generated in fast plan optimization is always 0
	}
	sessVars := p.SCtx().GetSessionVars()
	var rowSize, rowCount float64
	cost := 0.0
	if p.IndexInfo == nil {
		rowCount = float64(len(p.Handles))
		rowSize = cardinality.GetTableAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, cols, kv.TiKV, true)
	} else {
		rowCount = float64(len(p.IndexValues))
		rowSize = cardinality.GetIndexAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, cols, p.IndexInfo.Unique)
	}
	networkFactor := sessVars.GetNetworkFactor(p.TblInfo)
	seekFactor := sessVars.GetSeekFactor(p.TblInfo)
	scanConcurrency := sessVars.DistSQLScanConcurrency()
	cost += rowCount * rowSize * networkFactor
	cost += rowCount * seekFactor
	cost /= float64(scanConcurrency)
	return cost
}

// getPlanCostVer14BatchPointGetPlan calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14BatchPointGetPlan(pp base.PhysicalPlan, _ property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.BatchPointGetPlan)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	p.PlanCost = p.GetCost()
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getCost4PointGetPlan returns cost of the PointGetPlan.
func getCost4PointGetPlan(pp base.PhysicalPlan) float64 {
	p := pp.(*physicalop.PointGetPlan)
	cols := p.AccessCols()
	if cols == nil {
		return 0 // the cost of PointGet generated in fast plan optimization is always 0
	}
	sessVars := p.SCtx().GetSessionVars()
	var rowSize float64
	cost := 0.0
	if p.IndexInfo == nil {
		rowSize = cardinality.GetTableAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, cols, kv.TiKV, true)
	} else {
		rowSize = cardinality.GetIndexAvgRowSize(p.SCtx(), p.StatsInfo().HistColl, cols, p.IndexInfo.Unique)
	}
	networkFactor := sessVars.GetNetworkFactor(p.TblInfo)
	seekFactor := sessVars.GetSeekFactor(p.TblInfo)
	cost += rowSize * networkFactor
	cost += seekFactor
	cost /= float64(sessVars.DistSQLScanConcurrency())
	return cost
}

// getPlanCostVer14PointGetPlan calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PointGetPlan(pp base.PhysicalPlan, _ property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PointGetPlan)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	p.PlanCost = p.GetCost()
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// getPlanCostVer14PhysicalUnionAll calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer14PhysicalUnionAll(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalUnionAll)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	var childMaxCost float64
	for _, child := range p.Children() {
		childCost, err := child.GetPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		childMaxCost = math.Max(childMaxCost, childCost)
	}
	p.PlanCost = childMaxCost + float64(1+len(p.Children()))*p.SCtx().GetSessionVars().GetConcurrencyFactor()
	p.PlanCostInit = true
	return p.PlanCost, nil
}

func getOperatorActRows(operator base.PhysicalPlan) float64 {
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

// getPlanCostVer1PhysicalExchangeReceiver calculates the cost of the plan if it has not been calculated yet and returns the cost.
func getPlanCostVer1PhysicalExchangeReceiver(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	p := pp.(*physicalop.PhysicalExchangeReceiver)
	costFlag := option.CostFlag
	if p.PlanCostInit && !hasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		return p.PlanCost, nil
	}
	childCost, err := p.Children()[0].GetPlanCostVer1(taskType, option)
	if err != nil {
		return 0, err
	}
	p.PlanCost = childCost
	// accumulate net cost
	p.PlanCost += getCardinality(p.Children()[0], costFlag) * p.SCtx().GetSessionVars().GetNetworkFactor(nil)
	p.PlanCostInit = true
	return p.PlanCost, nil
}

func getCardinality(operator base.PhysicalPlan, costFlag uint64) float64 {
	if hasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) {
		actualProbeCnt := operator.GetActualProbeCnt(operator.SCtx().GetSessionVars().StmtCtx.RuntimeStatsColl)
		if actualProbeCnt == 0 {
			return 0
		}
		return max(0, getOperatorActRows(operator)/float64(actualProbeCnt))
	}
	rows := operator.StatsCount()
	if rows <= 0 && operator.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
		// 0 est-row can lead to 0 operator cost which makes plan choice unstable.
		rows = 1
	}
	return rows
}

// getMaxCountAfterAccess returns the MaxCountAfterAccess for the operator, which represents
// an upper bound on CountAfterAccess accounting for risks that could lead to underestimation.
func getMaxCountAfterAccess(operator base.PhysicalPlan, costFlag uint64) float64 {
	if hasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) {
		// For runtime stats, MaxCountAfterAccess is not available, return 0
		// (MaxCountAfterAccess defaults to 0 when not set)
		return 0
	}
	// Try to get MaxCountAfterAccess from scan operators that store it
	switch p := operator.(type) {
	case *physicalop.PhysicalTableScan:
		return p.MaxCountAfterAccess
	case *physicalop.PhysicalIndexScan:
		return p.MaxCountAfterAccess
	case *physicalop.PhysicalIndexReader:
		// Traverse to IndexPlan to find the underlying scan operator
		return getMaxCountAfterAccess(p.IndexPlan, costFlag)
	case *physicalop.PhysicalTableReader:
		// Traverse to TablePlan to find the underlying scan operator
		return getMaxCountAfterAccess(p.TablePlan, costFlag)
	default:
		// For other operators, recursively check children
		if len(operator.Children()) > 0 {
			// Return the max of all children's MaxCountAfterAccess
			maxVal := 0.0
			for _, child := range operator.Children() {
				childMax := getMaxCountAfterAccess(child, costFlag)
				if childMax > maxVal {
					maxVal = childMax
				}
			}
			return maxVal
		}
		return 0
	}
}

// estimateNetSeekCost calculates the net seek cost for the plan.
// for TiKV, it's len(access-range) * seek-factor,
// and for TiFlash, it's len(access-range) * len(access-column) * seek-factor.
func estimateNetSeekCost(copTaskPlan base.PhysicalPlan) float64 {
	switch x := copTaskPlan.(type) {
	case *physicalop.PhysicalTableScan:
		if x.StoreType == kv.TiFlash { // the old TiFlash interface uses cop-task protocol
			return float64(len(x.Ranges)) * float64(len(x.Columns)) * x.SCtx().GetSessionVars().GetSeekFactor(x.Table)
		}
		return float64(len(x.Ranges)) * x.SCtx().GetSessionVars().GetSeekFactor(x.Table) // TiKV
	case *physicalop.PhysicalIndexScan:
		return float64(len(x.Ranges)) * x.SCtx().GetSessionVars().GetSeekFactor(x.Table) // TiKV
	default:
		return estimateNetSeekCost(copTaskPlan.Children()[0])
	}
}

// getTableNetFactor returns the corresponding net factor of this table, it's mainly for temporary tables
func getTableNetFactor(copTaskPlan base.PhysicalPlan) float64 {
	switch x := copTaskPlan.(type) {
	case *physicalop.PhysicalTableScan:
		return x.SCtx().GetSessionVars().GetNetworkFactor(x.Table)
	case *physicalop.PhysicalIndexScan:
		return x.SCtx().GetSessionVars().GetNetworkFactor(x.Table)
	default:
		if len(x.Children()) == 0 {
			x.SCtx().GetSessionVars().GetNetworkFactor(nil)
		}
		return getTableNetFactor(x.Children()[0])
	}
}
