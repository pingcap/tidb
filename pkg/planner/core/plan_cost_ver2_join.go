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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// getPlanCostVer24PhysicalSort returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + sort-cpu-cost + sort-mem-cost + sort-disk-cost
// sort-cpu-cost = rows * log2(rows) * len(sort-items) * cpu-factor
// if no spill:
// 1. sort-mem-cost = rows * row-size * mem-factor
// 2. sort-disk-cost = 0
// else if spill:
// 1. sort-mem-cost = mem-quota * mem-factor
// 2. sort-disk-cost = rows * row-size * disk-factor
func getPlanCostVer24PhysicalSort(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalSort)
	intest.Assert(p != nil)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	rows := max(MinNumRows, getCardinality(p.Children()[0], option.CostFlag))
	rowSize := max(MinRowSize, getAvgRowSize(p.StatsInfo(), p.Schema().Columns))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	diskFactor := defaultVer2Factors.TiDBDisk
	oomUseTmpStorage := vardef.EnableTmpStorageOnOOM.Load()
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

	childCost, err := p.Children()[0].GetPlanCostVer2(taskType, option, isChildOfINL...)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.SumCostVer2(childCost, sortCPUCost, sortMemCost, sortDiskCost)
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().SortCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptSortCostFactor)
	return p.PlanCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + topn-cpu-cost + topn-mem-cost
// topn-cpu-cost = rows * log2(N) * len(sort-items) * cpu-factor
// topn-mem-cost = N * row-size * mem-factor
func getPlanCostVer24PhysicalTopN(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalTopN)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	rows := max(MinNumRows, getCardinality(p.Children()[0], option.CostFlag))
	n := max(MinNumRows, float64(p.Count+p.Offset))
	minTopNThreshold := float64(100) // set a minimum of 100 to avoid too low n
	if n > minTopNThreshold {
		// `rows` may be under-estimated. We need to adjust 'n' to ensure we keep a reasonable value.
		if rows < float64(p.Offset) {
			n = rows + float64(p.Offset)
		} else {
			n = max(min(n, rows), minTopNThreshold)
		}
	}
	rowSize := max(MinRowSize, getAvgRowSize(p.StatsInfo(), p.Schema().Columns))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)

	topNCPUCost := orderCostVer2(option, rows, n, p.ByItems, cpuFactor)
	topNMemCost := costusage.NewCostVer2(option, memFactor,
		n*rowSize*memFactor.Value,
		func() string { return fmt.Sprintf("topMem(%v*%v*%v)", n, rowSize, memFactor) })

	childCost, err := p.Children()[0].GetPlanCostVer2(taskType, option, isChildOfINL...)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.SumCostVer2(childCost, topNCPUCost, topNMemCost)
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().TopNCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptTopNCostFactor)
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalStreamAgg returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + agg-cost + group-cost
func getPlanCostVer24PhysicalStreamAgg(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalStreamAgg)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	rows := getCardinality(p.Children()[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	aggCost := aggCostVer2(option, rows, p.AggFuncs, cpuFactor)
	groupCost := groupCostVer2(option, rows, p.GroupByItems, cpuFactor)

	childCost, err := p.Children()[0].GetPlanCostVer2(taskType, option, isChildOfINL...)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.SumCostVer2(childCost, aggCost, groupCost)
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().StreamAggCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptStreamAggCostFactor)
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalHashAgg returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + (agg-cost + group-cost + hash-build-cost + hash-probe-cost) / concurrency
func getPlanCostVer24PhysicalHashAgg(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalHashAgg)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	inputRows := max(MinNumRows, getCardinality(p.Children()[0], option.CostFlag))
	outputRows := max(MinNumRows, getCardinality(p, option.CostFlag))
	outputRowSize := max(MinRowSize, getAvgRowSize(p.StatsInfo(), p.Schema().Columns))
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

	childCost, err := p.Children()[0].GetPlanCostVer2(taskType, option, isChildOfINL...)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.SumCostVer2(startCost, childCost, costusage.DivCostVer2(costusage.SumCostVer2(aggCost, groupCost, hashBuildCost, hashProbeCost), concurrency))
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().HashAggCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptHashAggCostFactor)
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalMergeJoin returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
func getPlanCostVer24PhysicalMergeJoin(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalMergeJoin)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	leftRows := max(MinNumRows, getCardinality(p.Children()[0], option.CostFlag))
	rightRows := max(MinNumRows, getCardinality(p.Children()[1], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	filterCost := costusage.SumCostVer2(filterCostVer2(option, leftRows, p.LeftConditions, cpuFactor),
		filterCostVer2(option, rightRows, p.RightConditions, cpuFactor),
		filterCostVer2(option, leftRows+rightRows, p.OtherConditions, cpuFactor)) // OtherConditions are applied to both sides
	groupCost := costusage.SumCostVer2(groupCostVer2(option, leftRows, expression.Column2Exprs(p.LeftJoinKeys), cpuFactor),
		groupCostVer2(option, rightRows, expression.Column2Exprs(p.RightJoinKeys), cpuFactor))

	leftChildCost, err := p.Children()[0].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	rightChildCost, err := p.Children()[1].GetPlanCostVer2(taskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.SumCostVer2(leftChildCost, rightChildCost, filterCost, groupCost)
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().MergeJoinCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptMergeJoinCostFactor)
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalHashJoin returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
func getPlanCostVer24PhysicalHashJoin(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalHashJoin)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	build, probe := p.Children()[0], p.Children()[1]
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	buildKeys, probeKeys := p.LeftJoinKeys, p.RightJoinKeys
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		build, probe = probe, build
		buildFilters, probeFilters = probeFilters, buildFilters
	}
	buildRows := max(MinNumRows, getCardinality(build, option.CostFlag))
	probeRows := getCardinality(probe, option.CostFlag)
	buildRowSize := max(MinRowSize, getAvgRowSize(build.StatsInfo(), build.Schema().Columns))
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
		p.PlanCostVer2 = costusage.SumCostVer2(buildChildCost, probeChildCost,
			costusage.DivCostVer2(costusage.SumCostVer2(buildHashCost, buildFilterCost, probeHashCost, probeFilterCost), mppConcurrency))
	} else { // TiDB HashJoin
		startCost := costusage.NewCostVer2(option, cpuFactor,
			10*3*cpuFactor.Value, // 10rows * 3func * cpuFactor
			func() string { return fmt.Sprintf("cpu(10*3*%v)", cpuFactor) })
		p.PlanCostVer2 = costusage.SumCostVer2(startCost, buildChildCost, probeChildCost, buildHashCost, buildFilterCost,
			costusage.DivCostVer2(costusage.SumCostVer2(probeFilterCost, probeHashCost), tidbConcurrency))
	}
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().HashJoinCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptHashJoinCostFactor)
	return p.PlanCostVer2, nil
}

func getIndexJoinCostVer24PhysicalIndexJoin(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, indexJoinType int) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalIndexJoin)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	build, probe := p.Children()[1-p.InnerChildIdx], p.Children()[p.InnerChildIdx]
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
	scanFactor := getTaskScanFactorVer2(p, kv.TiKV, taskType)

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

	// consider the seeking cost of the probe side of index join,
	// since this part of cost might be magnified by index join, see #62499.
	numRanges := getNumberOfRanges(probe)
	seekingCost := indexJoinSeekingCostVer2(option, buildRows, float64(numRanges), scanFactor)

	p.PlanCostVer2 = costusage.SumCostVer2(startCost, buildChildCost, buildFilterCost, buildTaskCost, seekingCost, costusage.DivCostVer2(costusage.SumCostVer2(doubleReadCost, probeCost, probeFilterCost, hashTableCost), probeConcurrency))
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().IndexJoinCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptIndexJoinCostFactor)
	return p.PlanCostVer2, nil
}

