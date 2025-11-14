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

package cost

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	// ModelVer1 indicate the cost model v1.
	ModelVer1 = 1
	// ModelVer2 indicate the cost model v2.
	ModelVer2 = 2
)

// HasCostFlag check whether the costFlag is indicated.
func HasCostFlag(costFlag, flag uint64) bool {
	return (costFlag & flag) > 0
}

// GetPlanCost returns the cost of this plan.
func GetPlanCost(p base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return getPlanCost(p, taskType, option)
}

// GenPlanCostTrace define a hook function to customize the cost calculation.
var GenPlanCostTrace func(p base.PhysicalPlan, costV *costusage.CostVer2, taskType property.TaskType, option *costusage.PlanCostOption)

func getPlanCost(p base.PhysicalPlan, taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	if p.SCtx().GetSessionVars().CostModelVersion == ModelVer2 {
		if p.SCtx().GetSessionVars().StmtCtx.EnableOptimizeTrace && option != nil {
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

// IndexJoinSeekingCostVer2 get the index join seek cost v2.
func IndexJoinSeekingCostVer2(option *costusage.PlanCostOption, buildRows, numRanges float64, scanFactor costusage.CostVer2Factor) costusage.CostVer2 {
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

// ScanCostVer2 get the scan cost factor v2.
func ScanCostVer2(option *costusage.PlanCostOption, rows, rowSize float64, scanFactor costusage.CostVer2Factor) costusage.CostVer2 {
	if rowSize < 1 {
		rowSize = 1
	}
	return costusage.NewCostVer2(option, scanFactor,
		// rows * log(row-size) * scanFactor, log2 from experiments
		rows*max(math.Log2(rowSize), 0)*scanFactor.Value,
		func() string { return fmt.Sprintf("scan(%v*logrowsize(%v)*%v)", rows, rowSize, scanFactor) })
}

// NetCostVer2 get the net cost v2.
func NetCostVer2(option *costusage.PlanCostOption, rows, rowSize float64, netFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, netFactor,
		rows*rowSize*netFactor.Value,
		func() string { return fmt.Sprintf("net(%v*rowsize(%v)*%v)", rows, rowSize, netFactor) })
}

// FilterCostVer2 get the filter cost factor v2.
func FilterCostVer2(option *costusage.PlanCostOption, rows float64, filters []expression.Expression, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	numFuncs := numFunctions(filters)
	return costusage.NewCostVer2(option, cpuFactor,
		rows*numFuncs*cpuFactor.Value,
		func() string { return fmt.Sprintf("cpu(%v*filters(%v)*%v)", rows, numFuncs, cpuFactor) })
}

// AggCostVer2 get agg cost v2.
func AggCostVer2(option *costusage.PlanCostOption, rows float64, aggFuncs []*aggregation.AggFuncDesc, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, cpuFactor,
		// TODO: consider types of agg-funcs
		rows*float64(len(aggFuncs))*cpuFactor.Value,
		func() string { return fmt.Sprintf("agg(%v*aggs(%v)*%v)", rows, len(aggFuncs), cpuFactor) })
}

// GroupCostVer2 get the group cost v2.
func GroupCostVer2(option *costusage.PlanCostOption, rows float64, groupItems []expression.Expression, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
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

// OrderCostVer2 get the order cost v2.
func OrderCostVer2(option *costusage.PlanCostOption, rows, n float64, byItems []*util.ByItems, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
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

// HashBuildCostVer2 get he hash build cost v2.
func HashBuildCostVer2(option *costusage.PlanCostOption, buildRows, buildRowSize, nKeys float64, cpuFactor, memFactor costusage.CostVer2Factor) costusage.CostVer2 {
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

// HashProbeCostVer2 get the hash probe cost v2.
func HashProbeCostVer2(option *costusage.PlanCostOption, probeRows, nKeys float64, cpuFactor costusage.CostVer2Factor) costusage.CostVer2 {
	// TODO: 1) consider types of keys, 2) dedicated factor for build-probe hash table
	hashKeyCost := costusage.NewCostVer2(option, cpuFactor,
		probeRows*nKeys*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashkey(%v*%v*%v)", probeRows, nKeys, cpuFactor) })
	hashProbeCost := costusage.NewCostVer2(option, cpuFactor,
		probeRows*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashprobe(%v*%v)", probeRows, cpuFactor) })
	return costusage.SumCostVer2(hashKeyCost, hashProbeCost)
}

// DoubleReadCostVer2 returns the double read cost v2.
// For simplicity and robust, only operators that need double-read like IndexLookup and IndexJoin consider this cost.
func DoubleReadCostVer2(option *costusage.PlanCostOption, numTasks float64, requestFactor costusage.CostVer2Factor) costusage.CostVer2 {
	return costusage.NewCostVer2(option, requestFactor,
		numTasks*requestFactor.Value,
		func() string { return fmt.Sprintf("doubleRead(tasks(%v)*%v)", numTasks, requestFactor) })
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

// ToList get all cost factors in v2.
func (c costVer2Factors) ToList() (l []costusage.CostVer2Factor) {
	return append(l, c.TiDBTemp, c.TiKVScan, c.TiKVDescScan, c.TiFlashScan, c.TiDBCPU, c.TiKVCPU, c.TiFlashCPU,
		c.TiDB2KVNet, c.TiDB2FlashNet, c.TiFlashMPPNet, c.TiDBMem, c.TiKVMem, c.TiFlashMem, c.TiDBDisk, c.TiDBRequest)
}

// DefaultVer2Factors exports the default cost ver 2 factors.
var DefaultVer2Factors = costVer2Factors{
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

// GetTaskCPUFactorVer2 get the plan cpu cost factor v2 of cpu.
func GetTaskCPUFactorVer2(_ base.PhysicalPlan, taskType property.TaskType) costusage.CostVer2Factor {
	switch taskType {
	case property.RootTaskType: // TiDB
		return DefaultVer2Factors.TiDBCPU
	case property.MppTaskType: // TiFlash
		return DefaultVer2Factors.TiFlashCPU
	default: // TiKV
		return DefaultVer2Factors.TiKVCPU
	}
}

// GetTaskMemFactorVer2 get the task mem factor v2.
func GetTaskMemFactorVer2(_ base.PhysicalPlan, taskType property.TaskType) costusage.CostVer2Factor {
	switch taskType {
	case property.RootTaskType: // TiDB
		return DefaultVer2Factors.TiDBMem
	case property.MppTaskType: // TiFlash
		return DefaultVer2Factors.TiFlashMem
	default: // TiKV
		return DefaultVer2Factors.TiKVMem
	}
}

// GetTaskScanFactorVer2 get the task scan factor v2.
func GetTaskScanFactorVer2(storeType kv.StoreType, taskType property.TaskType, isTmpTable, isDesc bool) costusage.CostVer2Factor {
	if isTmpTable {
		return DefaultVer2Factors.TiDBTemp
	}
	if storeType == kv.TiFlash {
		return DefaultVer2Factors.TiFlashScan
	}
	switch taskType {
	case property.MppTaskType: // TiFlash
		return DefaultVer2Factors.TiFlashScan
	default: // TiKV
		if isDesc {
			return DefaultVer2Factors.TiKVDescScan
		}
		return DefaultVer2Factors.TiKVScan
	}
}

// GetTaskNetFactorVer2 get the task net factor of v2.
func GetTaskNetFactorVer2(isTempTable, isMppNet, isFlashNet bool) costusage.CostVer2Factor {
	if isTempTable {
		return DefaultVer2Factors.TiDBTemp
	}
	if isMppNet { // TiFlash MPP
		return DefaultVer2Factors.TiFlashMPPNet
	}
	if isFlashNet { // TiDB to TiFlash with mpp protocol
		return DefaultVer2Factors.TiDB2FlashNet
	}
	return DefaultVer2Factors.TiDB2KVNet
}

// GetTaskRequestFactorVer2 get the task request factor v2.
func GetTaskRequestFactorVer2(isTempTable bool) costusage.CostVer2Factor {
	if isTempTable {
		return DefaultVer2Factors.TiDBTemp
	}
	return DefaultVer2Factors.TiDBRequest
}

// GetCardinality get the cardinality of a physical plan.
func GetCardinality(operator base.PhysicalPlan, costFlag uint64) float64 {
	if HasCostFlag(costFlag, costusage.CostFlagUseTrueCardinality) {
		actualProbeCnt := operator.GetActualProbeCnt(operator.SCtx().GetSessionVars().StmtCtx.RuntimeStatsColl)
		if actualProbeCnt == 0 {
			return 0
		}
		return max(0, getOperatorActRows(operator)/float64(actualProbeCnt))
	}
	rows := operator.StatsCount()
	if rows <= 0 && operator.SCtx().GetSessionVars().CostModelVersion == ModelVer2 {
		// 0 est-row can lead to 0 operator cost which makes plan choice unstable.
		rows = 1
	}
	return rows
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

// GetAvgRowSize rowSize for cost model ver2 is simplified, always use this function to calculate row size.
func GetAvgRowSize(stats *property.StatsInfo, cols []*expression.Column) (size float64) {
	if stats.HistColl != nil {
		size = max(cardinality.GetAvgRowSizeDataInDiskByRows(stats.HistColl, cols), 0)
	} else {
		// Estimate using just the type info.
		for _, col := range cols {
			size += max(float64(chunk.EstimateTypeWidth(col.GetStaticType())), 0)
		}
	}
	return
}
