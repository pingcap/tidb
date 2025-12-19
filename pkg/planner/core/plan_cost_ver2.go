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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// GetPlanCost returns the cost of this plan.
func GetPlanCost(p base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return getPlanCost(p, taskType, option)
}

// GenPlanCostTrace define a hook function to customize the cost calculation.
var GenPlanCostTrace func(p base.PhysicalPlan, costV *costusage.CostVer2, taskType property.TaskType, option *costusage.PlanCostOption)

func getPlanCost(p base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	if p.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
		if p.SCtx().GetSessionVars().StmtCtx.ExplainFormat == types.ExplainFormatCostTrace && option != nil {
			option.WithCostFlag(costusage.CostFlagTrace)
		}
		planCost, err := p.GetPlanCostVer2(taskType, option)
		if costusage.TraceCost(option) && GenPlanCostTrace != nil {
			GenPlanCostTrace(p, &planCost, taskType, option)
		}
		return planCost.GetCost(), err
	}
	return p.GetPlanCostVer1(taskType, option)
}

// getPlanCostVer24PhysicalSelection returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + filter-cost
func getPlanCostVer24PhysicalSelection(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalSelection)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	inputRows := getCardinality(p.Children()[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	filterCost := filterCostVer2(option, inputRows, p.Conditions, cpuFactor)

	childCost, err := p.Children()[0].GetPlanCostVer2(taskType, option, isChildOfINL...)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.SumCostVer2(filterCost, childCost)
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalProjection returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + proj-cost / concurrency
// proj-cost = input-rows * len(expressions) * cpu-factor
func getPlanCostVer24PhysicalProjection(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalProjection)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	inputRows := getCardinality(p.Children()[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	concurrency := float64(p.SCtx().GetSessionVars().ProjectionConcurrency())
	if concurrency == 0 {
		concurrency = 1 // un-parallel execution
	}

	projCost := filterCostVer2(option, inputRows, p.Exprs, cpuFactor)

	childCost, err := p.Children()[0].GetPlanCostVer2(taskType, option, isChildOfINL...)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.SumCostVer2(childCost, costusage.DivCostVer2(projCost, concurrency))
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

const (
	// MinNumRows provides a minimum to avoid underestimation. As selectivity estimation approaches
	// zero, all plan choices result in a low cost - making it difficult to differentiate plan choices.
	// A low value of 1.0 here is used for most (non probe acceses) to reduce this risk.
	MinNumRows = 1.0
	// MinRowSize provides a minimum column length to ensure that any adjustment or calculation
	// in costing does not go below this value. 2.0 is used as a reasonable lowest column length.
	MinRowSize = 2.0
	// TiFlashStartupRowPenalty applies a startup penalty for TiFlash scan to encourage TiKV usage for small scans
	TiFlashStartupRowPenalty = 10000
	// MaxPenaltyRowCount applies a penalty for high risk scans
	MaxPenaltyRowCount = 1000
)

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func getPlanCostVer24PhysicalIndexScan(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalIndexScan)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns) // consider all index columns
	scanFactor := getTaskScanFactorVer2(p, kv.TiKV, taskType)

	p.PlanCostVer2 = scanCostVer2(option, rows, rowSize, scanFactor)
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().IndexScanCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptIndexScanCostFactor)
	// Add a small tie-breaker cost based on the last 2 digits of the index ID to differentiate
	// indexes that would otherwise cost the same (same predicates, and same length).
	// This cost is intentionally not included in the trace output.
	if p.Index != nil {
		tieBreakerValue := float64(p.Index.ID%100) / 1000000.0
		p.PlanCostVer2 = costusage.AddCostWithoutTrace(p.PlanCostVer2, tieBreakerValue)
	}
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalTableScan returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func getPlanCostVer24PhysicalTableScan(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalTableScan)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	var columns []*expression.Column
	if p.StoreType == kv.TiKV { // Assume all columns for TiKV
		columns = p.TblCols
	} else { // TiFlash
		columns = p.Schema().Columns
	}
	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), columns)
	// Ensure rows and rowSize have a reasonable minimum value to avoid underestimation
	if len(isChildOfINL) <= 0 || (len(isChildOfINL) > 0 && !isChildOfINL[0]) {
		rows = max(MinNumRows, rows)
		rowSize = max(rowSize, MinRowSize)
	}

	scanFactor := getTaskScanFactorVer2(p, p.StoreType, taskType)

	// For not TiFlash
	if p.StoreType != kv.TiFlash {
		var unsignedIntHandle bool
		if p.Table.PKIsHandle {
			if pkColInfo := p.Table.GetPkColInfo(); pkColInfo != nil {
				unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			}
		}
		hasFullRangeScan := ranger.HasFullRange(p.Ranges, unsignedIntHandle)
		p.PlanCostVer2 = scanCostVer2(option, rows, rowSize, scanFactor)
		if ((len(isChildOfINL) > 0 && !isChildOfINL[0]) || len(isChildOfINL) <= 0) && hasFullRangeScan {
			if newRowCount := getTableScanPenalty(p, rows); newRowCount > 0 {
				p.PlanCostVer2 = costusage.SumCostVer2(p.PlanCostVer2, scanCostVer2(option, newRowCount, rowSize, scanFactor))
			}
		}
		p.PlanCostInit = true
		// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
		if len(isChildOfINL) > 0 && isChildOfINL[0] {
			// This is a RowID table scan (child of IndexLookUp)
			p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().TableRowIDScanCostFactor)
			p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptTableRowIDScanCostFactor)
		} else if !hasFullRangeScan {
			// This is a table range scan (predicate exists on the PK)
			p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().TableRangeScanCostFactor)
			p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptTableRangeScanCostFactor)
		} else {
			// This is a table full scan
			p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().TableFullScanCostFactor)
			p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptTableFullScanCostFactor)
		}
		return p.PlanCostVer2, nil
	}

	// For TiFlash

	// for late materialization
	if len(p.LateMaterializationFilterCondition) > 0 {
		// For late materialization, we will scan all rows of filter columns in the table or given range first.
		// And the rows of rest columns after filtering may be discrete, so we need to add some penalty for it.
		// so late materialization cost = lmRowSize * scanFactor * totalRowCount + restRowSize * scanFactor * rows * lateMaterializationFactor
		// And for small tables, len(p.LateMaterializationFilterCondition) always equal to 0, so do
		cols := expression.ExtractColumnsFromExpressions(p.LateMaterializationFilterCondition, nil)
		lmRowSize := getAvgRowSize(p.StatsInfo(), cols)
		totalRowCount := rows/p.LateMaterializationSelectivity + TiFlashStartupRowPenalty
		p.PlanCostVer2 = costusage.NewCostVer2(option, scanFactor,
			totalRowCount*max(math.Log2(lmRowSize), 0)*scanFactor.Value,
			func() string {
				return fmt.Sprintf("lm_col_scan(%v*logrowsize(%v)*%v)", totalRowCount, lmRowSize, scanFactor)
			})
		p.PlanCostVer2 = costusage.SumCostVer2(p.PlanCostVer2, costusage.NewCostVer2(option, scanFactor,
			rows*max(math.Log2(rowSize-lmRowSize), 0)*scanFactor.Value*defaultVer2Factors.LateMaterializationScan.Value,
			func() string {
				return fmt.Sprintf("lm_rest_col_scan(%v*logrowsize(%v)*%v*lm_scan_factor(%v))", rows, rowSize-lmRowSize, scanFactor, defaultVer2Factors.LateMaterializationScan.Value)
			}))
	} else {
		p.PlanCostVer2 = scanCostVer2(option, rows, rowSize, scanFactor)
		// Apply TiFlash startup cost to prefer TiKV for small table scans
		p.PlanCostVer2 = costusage.SumCostVer2(p.PlanCostVer2, scanCostVer2(option, TiFlashStartupRowPenalty, rowSize, scanFactor))
	}

	// Add penalty for search index
	for _, idx := range p.UsedColumnarIndexes {
		if idx == nil {
			continue
		}
		if idx.IndexInfo.VectorInfo != nil {
			if idx.QueryInfo == nil {
				p.PlanCostVer2 = costusage.NewCostVer2(option, defaultVer2Factors.ANNIndexNoTopK, defaultVer2Factors.ANNIndexNoTopK.Value, func() string {
					return fmt.Sprintf("ann-index-no-topk(%v)", defaultVer2Factors.ANNIndexNoTopK)
				})
			} else {
				p.PlanCostVer2 = costusage.SumCostVer2(p.PlanCostVer2, costusage.NewCostVer2(option, defaultVer2Factors.ANNIndexStart, rows*defaultVer2Factors.ANNIndexStart.Value, func() string {
					return fmt.Sprintf("ann-index-start(%v*%v)", rows, defaultVer2Factors.ANNIndexStart)
				}))
				p.PlanCostVer2 = costusage.SumCostVer2(p.PlanCostVer2, costusage.NewCostVer2(option, defaultVer2Factors.ANNIndexScanRow, float64(idx.QueryInfo.GetAnnQueryInfo().TopK)*defaultVer2Factors.ANNIndexScanRow.Value, func() string {
					return fmt.Sprintf("ann-index-topk(%v*%v)", idx.QueryInfo.GetAnnQueryInfo().TopK, defaultVer2Factors.ANNIndexScanRow)
				}))
			}
		} else if idx.IndexInfo.InvertedInfo != nil {
			// penelty = rows * inverted-index-scan-factor + inverted-index-search-factor * rows
			p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, defaultVer2Factors.InvertedIndexScan.Value)
			p.PlanCostVer2 = costusage.SumCostVer2(p.PlanCostVer2, costusage.NewCostVer2(option, defaultVer2Factors.InvertedIndexSearch, rows*defaultVer2Factors.InvertedIndexSearch.Value, func() string {
				return fmt.Sprintf("inverted-index-search(%v*%v)", rows, defaultVer2Factors.InvertedIndexSearch)
			}))
		}
	}
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	// This is a table full scan
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().TableTiFlashScanCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptTableTiFlashScanCostFactor)
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalIndexReader returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func getPlanCostVer24PhysicalIndexReader(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalIndexReader)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	rows := getCardinality(p.IndexPlan, option.CostFlag)
	rowSize := getAvgRowSize(p.StatsInfo(), p.Schema().Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())

	netCost := netCostVer2(option, rows, rowSize, netFactor)

	childCost, err := p.IndexPlan.GetPlanCostVer2(property.CopSingleReadTaskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.DivCostVer2(costusage.SumCostVer2(childCost, netCost), concurrency)
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().IndexReaderCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptIndexReaderCostFactor)
	return p.PlanCostVer2, nil
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func getPlanCostVer24PhysicalTableReader(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalTableReader)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	rows := getCardinality(p.TablePlan, option.CostFlag)
	rowSize := max(MinRowSize, getAvgRowSize(p.StatsInfo(), p.Schema().Columns))
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())
	concurrency = min(concurrency, rows) // avoid too high concurrency for small table
	childType := property.CopSingleReadTaskType
	if p.StoreType == kv.TiFlash { // mpp protocol
		childType = property.MppTaskType
	}

	netCost := netCostVer2(option, rows, rowSize, netFactor)

	childCost, err := p.TablePlan.GetPlanCostVer2(childType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}

	p.PlanCostVer2 = costusage.DivCostVer2(costusage.SumCostVer2(childCost, netCost), concurrency)
	p.PlanCostInit = true

	// consider tidb_enforce_mpp
	if p.StoreType == kv.TiFlash && p.SCtx().GetSessionVars().IsMPPEnforced() &&
		!hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) { // show the real cost in explain-statements
		p.PlanCostVer2 = costusage.DivCostVer2(p.PlanCostVer2, 1000000000)
	}
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().TableReaderCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptTableReaderCostFactor)
	return p.PlanCostVer2, nil
}

// getPlanCostVer24PhysicalIndexLookUpReader returns the plan-cost of this sub-plan, which is:
// plan-cost = index-side-cost + (table-side-cost + double-read-cost) / double-read-concurrency
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
// double-read-cost = double-read-request-cost + double-read-cpu-cost
// double-read-request-cost = double-read-tasks * request-factor
// double-read-cpu-cost = index-rows * cpu-factor
// double-read-tasks = index-rows / batch-size * task-per-batch # task-per-batch is a magic number now
func getPlanCostVer24PhysicalIndexLookUpReader(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalIndexLookUpReader)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	indexRows := getCardinality(p.IndexPlan, option.CostFlag)
	tableRows := getCardinality(p.TablePlan, option.CostFlag)

	if p.PushedLimit != nil { // consider pushed down limit clause
		indexRows = min(indexRows, float64(p.PushedLimit.Count)) // rows returned from the index side
		tableRows = min(tableRows, float64(p.PushedLimit.Count)) // rows to scan on the table side
	}

	indexRowSize := cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.IndexPlan), p.IndexPlan.Schema().Columns, true, false)
	tableRowSize := cardinality.GetAvgRowSize(p.SCtx(), physicalop.GetTblStats(p.TablePlan), p.TablePlan.Schema().Columns, false, false)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	netFactor := getTaskNetFactorVer2(p, taskType)
	requestFactor := getTaskRequestFactorVer2(p, taskType)
	distConcurrency := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())
	doubleReadConcurrency := float64(p.SCtx().GetSessionVars().IndexLookupConcurrency())

	// index-side
	indexNetCost := netCostVer2(option, indexRows, indexRowSize, netFactor)
	indexChildCost, err := p.IndexPlan.GetPlanCostVer2(property.CopMultiReadTaskType, option)
	if err != nil {
		return costusage.ZeroCostVer2, err
	}
	indexSideCost := costusage.DivCostVer2(costusage.SumCostVer2(indexNetCost, indexChildCost), distConcurrency)

	// table-side
	tableNetCost := netCostVer2(option, tableRows, tableRowSize, netFactor)
	// set the isChildOfINL signal.
	tableChildCost, err := p.TablePlan.GetPlanCostVer2(property.CopMultiReadTaskType, option, true)
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

	p.PlanCostVer2 = costusage.SumCostVer2(indexSideCost, costusage.DivCostVer2(costusage.SumCostVer2(tableSideCost, doubleReadCost), doubleReadConcurrency))

	if p.SCtx().GetSessionVars().EnablePaging && p.ExpectedCnt > 0 && p.ExpectedCnt <= paging.Threshold {
		// if the expectCnt is below the paging threshold, using paging API
		p.Paging = true // TODO: move this operation from cost model to physical optimization
		p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, 0.6)
	}

	p.PlanCostInit = true
	if p.PushedLimit != nil && tableRows <= float64(p.PushedLimit.Count) {
		// Multiply by limit cost factor - defaults to 1, but can be increased/decreased to influence the cost model
		p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().LimitCostFactor)
		p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptLimitCostFactor)
	}
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().IndexLookupCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptIndexLookupCostFactor)
	return p.PlanCostVer2, nil
}

// GetPlanCostVer24PhysicalIndexMergeReader returns the plan-cost of this sub-plan, which is:
// plan-cost = table-side-cost + sum(index-side-cost)
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
func GetPlanCostVer24PhysicalIndexMergeReader(pp base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	p := pp.(*physicalop.PhysicalIndexMergeReader)
	if p.PlanCostInit && !hasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}

	netFactor := getTaskNetFactorVer2(p, taskType)
	distConcurrency := float64(p.SCtx().GetSessionVars().DistSQLScanConcurrency())

	var tableSideCost costusage.CostVer2
	if tablePath := p.TablePlan; tablePath != nil {
		rows := getCardinality(tablePath, option.CostFlag)
		rowSize := getAvgRowSize(tablePath.StatsInfo(), tablePath.Schema().Columns)

		tableNetCost := netCostVer2(option, rows, rowSize, netFactor)
		tableChildCost, err := tablePath.GetPlanCostVer2(taskType, option, true)
		if err != nil {
			return costusage.ZeroCostVer2, err
		}
		tableSideCost = costusage.DivCostVer2(costusage.SumCostVer2(tableNetCost, tableChildCost), distConcurrency)
	}

	indexSideCost := make([]costusage.CostVer2, 0, len(p.PartialPlansRaw))
	for _, indexPath := range p.PartialPlansRaw {
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

	p.PlanCostVer2 = costusage.SumCostVer2(tableSideCost, sumIndexSideCost)
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
		p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, 0.99)
		// Multiply by limit cost factor - defaults to 1, but can be increased/decreased to influence the cost model
		p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().LimitCostFactor)
		p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptLimitCostFactor)
	}
	p.PlanCostInit = true
	// Multiply by cost factor - defaults to 1, but can be increased/decreased to influence the cost model
	p.PlanCostVer2 = costusage.MulCostVer2(p.PlanCostVer2, p.SCtx().GetSessionVars().IndexMergeCostFactor)
	p.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptIndexMergeCostFactor)
	return p.PlanCostVer2, nil
}

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
	groupCost := costusage.SumCostVer2(groupCostVer2(option, leftRows, cols2Exprs(p.LeftJoinKeys), cpuFactor),
		groupCostVer2(option, rightRows, cols2Exprs(p.RightJoinKeys), cpuFactor))

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

func indexJoinSeekingCostVer2(option *costusage.PlanCostOption, buildRows, numRanges float64, scanFactor costusage.CostVer2Factor) costusage.CostVer2 {
	if buildRows <= 1 || numRanges <= 1 { // ignore seeking cost
		return costusage.ZeroCostVer2
	}
	// Large IN lists like `a in (1, 2, 3...)` could generate a large number of ranges and seeking operations, which could be magnified by IndexJoin
	// and slow down the query performance obviously, we need to consider this part of cost. Please see a case in issue #62499.
	// To simplify the calculation of seeking cost, we treat a seeking operation as a scan of 10 rows with 8 row-width.
	// 10 is based on a simple experiment, please see https://github.com/pingcap/tidb/issues/62499#issuecomment-3301796153.
	return costusage.NewCostVer2(option, scanFactor,
		buildRows*10*math.Log2(8)*numRanges*scanFactor.Value,
		func() string { return fmt.Sprintf("seeking(%v*%v*10*log2(8)*%v)", buildRows, numRanges, scanFactor) })
}

func scanCostVer2(option *costusage.PlanCostOption, rows, rowSize float64, scanFactor costusage.CostVer2Factor) costusage.CostVer2 {
	if rowSize < 1 {
		rowSize = 1
	}
	return costusage.NewCostVer2(option, scanFactor,
		// rows * log(row-size) * scanFactor, log2 from experiments
		rows*max(math.Log2(rowSize), 0)*scanFactor.Value,
		func() string { return fmt.Sprintf("scan(%v*logrowsize(%v)*%v)", rows, rowSize, scanFactor) })
}

func netCostVer2(option *costusage.PlanCostOption, rows, rowSize float64, netFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, netFactor,
		rows*rowSize*netFactor.Value,
		func() string { return fmt.Sprintf("net(%v*rowsize(%v)*%v)", rows, rowSize, netFactor) })
}

func filterCostVer2(option *costusage.PlanCostOption, rows float64, filters []expression.Expression, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	numFuncs := numFunctions(filters)
	return costusage.NewCostVer2(option, cpuFactor,
		rows*numFuncs*cpuFactor.Value,
		func() string { return fmt.Sprintf("cpu(%v*filters(%v)*%v)", rows, numFuncs, cpuFactor) })
}

func aggCostVer2(option *costusage.PlanCostOption, rows float64, aggFuncs []*aggregation.AggFuncDesc, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, cpuFactor,
		// TODO: consider types of agg-funcs
		rows*float64(len(aggFuncs))*cpuFactor.Value,
		func() string { return fmt.Sprintf("agg(%v*aggs(%v)*%v)", rows, len(aggFuncs), cpuFactor) })
}

func groupCostVer2(option *costusage.PlanCostOption, rows float64, groupItems []expression.Expression, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
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

func orderCostVer2(option *costusage.PlanCostOption, rows, n float64, byItems []*util.ByItems, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
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
		max(rows*math.Log2(n), 0)*cpuFactor.Value,
		func() string { return fmt.Sprintf("orderCPU(%v*log(%v)*%v)", rows, n, cpuFactor) })
	return costusage.SumCostVer2(exprCost, orderCost)
}

func hashBuildCostVer2(option *costusage.PlanCostOption, buildRows, buildRowSize, nKeys float64, cpuFactor, memFactor costusage.CostVer2Factor) costusage.CostVer2 {
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

func hashProbeCostVer2(option *costusage.PlanCostOption, probeRows, nKeys float64, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
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
func doubleReadCostVer2(option *costusage.PlanCostOption, numTasks float64, requestFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, requestFactor,
		numTasks*requestFactor.Value,
		func() string { return fmt.Sprintf("doubleRead(tasks(%v)*%v)", numTasks, requestFactor) })
}

func getTableScanPenalty(p *physicalop.PhysicalTableScan, rows float64) (rowPenalty float64) {
	// Apply cost penalty for full scans that carry high risk of underestimation. Exclude those
	// that are the child of an index scan or child is TableRangeScan
	if len(p.RangeInfo) > 0 {
		return float64(0)
	}
	sessionVars := p.SCtx().GetSessionVars()
	allowPreferRangeScan := sessionVars.GetAllowPreferRangeScan()
	tblColHists := p.TblColHists
	originalRows := int64(tblColHists.GetAnalyzeRowCount())

	// hasUnreliableStats is a check for pseudo or zero stats
	hasUnreliableStats := tblColHists.Pseudo || originalRows < 1
	// hasHighModifyCount tracks the high risk of a tablescan where auto-analyze had not yet updated the table row count
	hasHighModifyCount := tblColHists.ModifyCount > originalRows
	// hasLowEstimate is a check to capture a unique customer case where modifyCount is used for tablescan estimate (but it not adequately understood why)
	hasLowEstimate := rows > 1 && tblColHists.ModifyCount < originalRows && int64(rows) <= tblColHists.ModifyCount
	// preferRangeScan check here is same as in skylinePruning
	preferRangeScanCondition := allowPreferRangeScan && (hasUnreliableStats || hasHighModifyCount || hasLowEstimate)

	// differentiate a FullTableScan from a partition level scan - so we shouldn't penalize these
	hasPartitionScan := false
	if p.PlanPartInfo != nil {
		if len(p.PlanPartInfo.PruningConds) > 0 {
			hasPartitionScan = true
		}
	}

	// GetIndexForce assumes that the USE/FORCE index is to force a range scan, and thus the
	// penalty is applied to a full table scan (not range scan). This may also penalize a
	// full table scan where USE/FORCE was applied to the primary key.
	hasIndexForce := sessionVars.StmtCtx.GetIndexForce()
	shouldApplyPenalty := hasIndexForce || preferRangeScanCondition
	if shouldApplyPenalty {
		// MySQL will increase the cost of table scan if FORCE index is used. TiDB takes this one
		// step further - because we don't differentiate USE/FORCE - the added penalty applies to
		// both, and it also applies to any full table scan in the query. Use "max" to get the minimum
		// number of rows to add as a penalty to the table scan.
		minRows := max(MaxPenaltyRowCount, rows)
		if hasPartitionScan {
			return minRows
		}
		// If it isn't a partitioned table - choose the max that includes ModifyCount
		return max(minRows, float64(tblColHists.ModifyCount))
	}
	return float64(0)
}

// In Cost Ver2, we hide cost factors from users and deprecate SQL variables like `tidb_opt_scan_factor`.
type costVer2Factors struct {
	TiDBTemp                costusage.CostVer2Factor // operations on TiDB temporary table
	TiKVScan                costusage.CostVer2Factor // per byte
	TiKVDescScan            costusage.CostVer2Factor // per byte
	TiFlashScan             costusage.CostVer2Factor // per byte
	TiDBCPU                 costusage.CostVer2Factor // per column or expression
	TiKVCPU                 costusage.CostVer2Factor // per column or expression
	TiFlashCPU              costusage.CostVer2Factor // per column or expression
	TiDB2KVNet              costusage.CostVer2Factor // per byte
	TiDB2FlashNet           costusage.CostVer2Factor // per byte
	TiFlashMPPNet           costusage.CostVer2Factor // per byte
	TiDBMem                 costusage.CostVer2Factor // per byte
	TiKVMem                 costusage.CostVer2Factor // per byte
	TiFlashMem              costusage.CostVer2Factor // per byte
	TiDBDisk                costusage.CostVer2Factor // per byte
	TiDBRequest             costusage.CostVer2Factor // per net request
	ANNIndexStart           costusage.CostVer2Factor // ANN index's warmup cost, related to row num.
	ANNIndexScanRow         costusage.CostVer2Factor // ANN index's scan cost, by row.
	ANNIndexNoTopK          costusage.CostVer2Factor // special factor for ANN index without top-k: max uint64
	InvertedIndexSearch     costusage.CostVer2Factor // Search cost of inverted index, related to row num.
	InvertedIndexScan       costusage.CostVer2Factor // Scan cost penalty of inverted index, related to row num.
	LateMaterializationScan costusage.CostVer2Factor // Late materialization rest columns scan cost penalty
}

func (c costVer2Factors) tolist() (l []costusage.CostVer2Factor) {
	return append(l, c.TiDBTemp, c.TiKVScan, c.TiKVDescScan, c.TiFlashScan, c.TiDBCPU, c.TiKVCPU, c.TiFlashCPU,
		c.TiDB2KVNet, c.TiDB2FlashNet, c.TiFlashMPPNet, c.TiDBMem, c.TiKVMem, c.TiFlashMem, c.TiDBDisk, c.TiDBRequest)
}

var defaultVer2Factors = costVer2Factors{
	TiDBTemp:                costusage.CostVer2Factor{Name: "tidb_temp_table_factor", Value: 0.00},
	TiKVScan:                costusage.CostVer2Factor{Name: "tikv_scan_factor", Value: 40.70},
	TiKVDescScan:            costusage.CostVer2Factor{Name: "tikv_desc_scan_factor", Value: 61.05},
	TiFlashScan:             costusage.CostVer2Factor{Name: "tiflash_scan_factor", Value: 11.60},
	TiDBCPU:                 costusage.CostVer2Factor{Name: "tidb_cpu_factor", Value: 49.90},
	TiKVCPU:                 costusage.CostVer2Factor{Name: "tikv_cpu_factor", Value: 49.90},
	TiFlashCPU:              costusage.CostVer2Factor{Name: "tiflash_cpu_factor", Value: 2.40},
	TiDB2KVNet:              costusage.CostVer2Factor{Name: "tidb_kv_net_factor", Value: 3.96},
	TiDB2FlashNet:           costusage.CostVer2Factor{Name: "tidb_flash_net_factor", Value: 2.20},
	TiFlashMPPNet:           costusage.CostVer2Factor{Name: "tiflash_mpp_net_factor", Value: 1.00},
	TiDBMem:                 costusage.CostVer2Factor{Name: "tidb_mem_factor", Value: 0.20},
	TiKVMem:                 costusage.CostVer2Factor{Name: "tikv_mem_factor", Value: 0.20},
	TiFlashMem:              costusage.CostVer2Factor{Name: "tiflash_mem_factor", Value: 0.05},
	TiDBDisk:                costusage.CostVer2Factor{Name: "tidb_disk_factor", Value: 200.00},
	TiDBRequest:             costusage.CostVer2Factor{Name: "tidb_request_factor", Value: 6000000.00},
	ANNIndexStart:           costusage.CostVer2Factor{Name: "ann_index_start_factor", Value: 0.000144},
	ANNIndexScanRow:         costusage.CostVer2Factor{Name: "ann_index_scan_factor", Value: 1.65},
	ANNIndexNoTopK:          costusage.CostVer2Factor{Name: "ann_index_no_topk_factor", Value: math.MaxUint64},
	InvertedIndexSearch:     costusage.CostVer2Factor{Name: "inverted_index_search_factor", Value: 139.2}, // (8 + 4) * TiFlashScan, 8 for 8 bytes of key, 4 for 4 bytes of RowID
	InvertedIndexScan:       costusage.CostVer2Factor{Name: "inverted_index_scan_factor", Value: 1.5},
	LateMaterializationScan: costusage.CostVer2Factor{Name: "lm_scan_factor", Value: 1.5},
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
		if indexScan, ok := p.(*physicalop.PhysicalIndexScan); ok {
			desc = indexScan.Desc
		}
		if tableScan, ok := p.(*physicalop.PhysicalTableScan); ok {
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
	if _, ok := p.(*physicalop.PhysicalExchangeReceiver); ok { // TiFlash MPP
		return defaultVer2Factors.TiFlashMPPNet
	}
	if tblReader, ok := p.(*physicalop.PhysicalTableReader); ok {
		if _, isMPP := tblReader.TablePlan.(*physicalop.PhysicalExchangeSender); isMPP { // TiDB to TiFlash with mpp protocol
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
	case *physicalop.PhysicalIndexReader:
		return getTableInfo(x.IndexPlan)
	case *physicalop.PhysicalTableReader:
		return getTableInfo(x.TablePlan)
	case *physicalop.PhysicalIndexLookUpReader:
		return getTableInfo(x.TablePlan)
	case *physicalop.PhysicalIndexMergeReader:
		if x.TablePlan != nil {
			return getTableInfo(x.TablePlan)
		}
		return getTableInfo(x.PartialPlansRaw[0])
	case *physicalop.PhysicalTableScan:
		return x.Table
	case *physicalop.PhysicalIndexScan:
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
