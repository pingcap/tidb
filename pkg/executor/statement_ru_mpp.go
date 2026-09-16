// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"math"

	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

// MPP accounting uses the per-operator formulas in calculateStatementRUPlanChildFirst.
// These formulas produce work units, not CPU time: RU is their weighted sum using
// currentStatementRUWeights, just as for root and TiKV operators.
//
// Each plan ID's received task/stream summaries are summed before applying a
// formula. Input rows come from direct children; output rows come from the
// operator's own summary. This also applies to nonlinear Sort/TopN work: we do
// not sum per-task sort costs or multiply merged rows by task concurrency.
// Missing evidence contributes zero without proving that no work occurred;
// partial results remain calibration-Incomplete, while invalid evidence fails
// the calculation. Every supported flat-plan occurrence adds one OperatorNum,
// even if its other units are zero, regardless of the number of MPP tasks.
//
// MPP operator units belong to TiFlash. The TiDB TableReader separately collects
// its subtree's scan bytes once and attributes them to TiFlash; its own operator
// count remains TiDB work. Statement frontend and write units keep their existing
// ownership and are not charged again at each MPP operator.
func statementRUOperatorRunsAtMPP(operator *plannercore.FlatOperator) bool {
	return !operator.IsRoot && operator.StoreType == kv.TiFlash && operator.ReqType == physicalop.MPP
}

func collectStatementRUOperatorHashStateRows(operator *plannercore.FlatOperator, stats *execdetails.RuntimeStatsColl, calculator *statementRUCalculator) statementRUOperatorState {
	if !statementRUOperatorRunsAtMPP(operator) {
		return collectStatementRUHashStateRows(operator.Origin.ID(), stats, calculator)
	}
	if stats == nil {
		return statementRUOperatorComplete
	}
	units, _ := stats.GetTiFlashExecutionUnits(operator.Origin.ID())
	// HashJoin and HashAgg use reported hash-table sizes, not output-row counts:
	// Join V1 reports distinct build keys, V2 build rows, and Agg group-map entries.
	// The model intentionally sums producer sizes across hash implementations;
	// build-row counts are not converted to distinct-key counts.
	if units.Invalid || !addStatementRUHashStateRows(calculator, float64(units.HashDistinctEntries)+float64(units.HashBuildRows)) {
		return statementRUOperatorInvalid
	}
	return statementRUOperatorComplete
}

// The Reader adds ScanBytes = sum(UserReadBytes) over its MPP TableScan occurrences,
// including both sides of a join. Each scan snapshot already combines received
// task/stream reports, so no task-count multiplier or child-byte propagation is
// needed. The TableScan's own RU does not add these bytes again.
// user_read_bytes retains each scan producer's semantics: traditional TiFlash
// late-materialization/MVCC read bytes or columnar returned-block bytes. Neither
// is converted to physical disk bytes, and no key-size ratio is applied.
func collectStatementRUMPPScanBytes(tree plannercore.FlatPlanTree, index int, stats *execdetails.RuntimeStatsColl, calculator *statementRUCalculator) statementRUOperatorState {
	operator := tree[index]
	if !statementRUOperatorRunsAtMPP(operator) {
		return statementRUOperatorUnsupported
	}
	if _, scan := operator.Origin.(*physicalop.PhysicalTableScan); scan && stats != nil {
		units, _ := stats.GetTiFlashExecutionUnits(operator.Origin.ID())
		if units.Invalid || !addStatementRUScanBytes(calculator, float64(units.UserReadBytes)) {
			return statementRUOperatorInvalid
		}
		calculator.compute[statementRUTiFlash].scanBytes += float64(units.UserReadBytes)
	}
	for _, child := range operator.ChildrenIdx {
		if state := collectStatementRUMPPScanBytes(tree, child, stats, calculator); state != statementRUOperatorComplete {
			return state
		}
	}
	return statementRUOperatorComplete
}

// Exchange and remote-read profiles each expose send and receive bytes. Charge
// only their send side, including the root sender to TiDB, to avoid double counting.
// NetBytes = InnerZoneSendBytes + InterZoneSendBytes; broadcast replication is
// already reflected in these reported bytes. CrossAZNetBytes retains the inter-zone
// subset for observation, but currentStatementRUWeights supplies no cross-AZ premium.
func collectStatementRUMPPNetwork(planID int, stats *execdetails.RuntimeStatsColl, calculator *statementRUCalculator) bool {
	if stats == nil {
		return true
	}
	units, _ := stats.GetTiFlashExecutionUnits(planID)
	if units.Invalid {
		return false
	}
	calculator.units.NetBytes += float64(units.InnerZoneSendBytes) + float64(units.InterZoneSendBytes)
	calculator.units.CrossAZNetBytes += float64(units.InterZoneSendBytes)
	return !math.IsInf(calculator.units.NetBytes, 0) && !math.IsInf(calculator.units.CrossAZNetBytes, 0)
}
