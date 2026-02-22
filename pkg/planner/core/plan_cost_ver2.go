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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/ranger"
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
	// _tidb_commit_ts is not a real extra column stored in the disk, and it should not bring extra cost, so we exclude
	// it from the cost here.
	for i, col := range columns {
		if col.ID == model.ExtraCommitTSID {
			columns = slices.Delete(slices.Clone(columns), i, i+1)
			break
		}
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

