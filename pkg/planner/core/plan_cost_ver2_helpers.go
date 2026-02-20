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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
)

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
