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
	"github.com/pingcap/tidb/pkg/metrics"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/resourcegroup/ruv2"
)

type statementRUEngine uint8

const (
	statementRUTiDB statementRUEngine = iota
	statementRUTiKV
	statementRUTiFlash
	statementRUEngineCount
)

var statementRUEngineNames = [...]string{"tidb", "tikv", "tiflash"}

// Temporary multiplier for TiFlash RU experiments.
const statementRUTiFlashMultiplier = 10

// Operator work follows its execution engine. A TiDB Reader additionally owns
// remote scan evidence, which is attributed to the corresponding storage engine.
type statementRUComputeUnits struct {
	cpuWork         float64
	hashStateRows   float64
	operatorNum     float64
	joinOutputRows  float64
	scanBytes       float64
	netBytes        float64
	crossAZNetBytes float64
}

type statementRUEngineResult struct {
	TiDB    float64
	TiKV    float64
	TiFlash float64
}

// statementRUOperator groups physical operators for RU reporting and includes
// statement-level accounting entries such as sql_frontend and kv_write.
type statementRUOperator uint8

const (
	statementRUWrapper statementRUOperator = iota
	statementRUProjection
	statementRUSelection
	statementRULimit
	statementRUSort
	statementRUTopN
	statementRUWindow
	statementRUHashAgg
	statementRUStreamAgg
	statementRUHashJoin
	statementRUMergeJoin
	statementRULookupJoin
	statementRUReader
	statementRULookupReader
	statementRUUnionScan
	statementRUShuffle
	statementRURangeScan
	statementRUPointLookup
	statementRUWrite
	statementRUAnalyze
	statementRUFrontend
	statementRUCopTransport
	statementRUKVWrite
	statementRUOperatorCount
)

var statementRUOperatorNames = [...]string{
	"wrapper", "projection", "selection", "limit", "sort", "topn", "window",
	"hash_agg", "stream_agg", "hash_join", "merge_join", "lookup_join",
	"reader", "lookup_reader", "union_scan", "shuffle", "range_scan", "point_lookup",
	"write", "analyze", "sql_frontend", "coprocessor", "kv_write",
}

// statementRUFullReport is allocated only in full mode. Its bounded arrays own
// numeric values, never plans or runtime statistics. Finalization freezes a copy
// for the publisher; result mode never allocates either report.
type statementRUFullReport struct {
	units [statementRUEngineCount][statementRUOperatorCount]ruv2.StmtUnits
	seen  [statementRUEngineCount][statementRUOperatorCount]bool
}

func (report *statementRUFullReport) add(engine statementRUEngine, operator statementRUOperator, units ruv2.StmtUnits) {
	report.units[engine][operator] = report.units[engine][operator].Add(units)
	report.seen[engine][operator] = true
}

// statementRUOperatorForPlan runs exclusively in full mode. Operator labels are bounded
// independently of SQL text, plan IDs, table names, and index names.
func statementRUOperatorForPlan(plan base.Plan) statementRUOperator {
	switch plan.(type) {
	case *physicalop.PhysicalProjection:
		return statementRUProjection
	case *physicalop.PhysicalSelection:
		return statementRUSelection
	case *physicalop.PhysicalLimit, *physicalop.PhysicalMaxOneRow:
		return statementRULimit
	case *physicalop.PhysicalSort:
		return statementRUSort
	case *physicalop.PhysicalTopN:
		return statementRUTopN
	case *physicalop.PhysicalWindow:
		return statementRUWindow
	case *physicalop.PhysicalHashAgg:
		return statementRUHashAgg
	case *physicalop.PhysicalStreamAgg:
		return statementRUStreamAgg
	case *physicalop.PhysicalHashJoin:
		return statementRUHashJoin
	case *physicalop.PhysicalMergeJoin:
		return statementRUMergeJoin
	case *physicalop.PhysicalIndexJoin, *physicalop.PhysicalIndexHashJoin, *physicalop.PhysicalIndexMergeJoin:
		return statementRULookupJoin
	case *physicalop.PhysicalTableReader, *physicalop.PhysicalIndexReader:
		return statementRUReader
	case *physicalop.PhysicalIndexLookUpReader, *physicalop.PhysicalIndexMergeReader:
		return statementRULookupReader
	case *physicalop.PhysicalUnionScan:
		return statementRUUnionScan
	case *physicalop.PhysicalShuffle:
		return statementRUShuffle
	case *physicalop.PhysicalTableScan, *physicalop.PhysicalIndexScan:
		return statementRURangeScan
	case *physicalop.PointGetPlan, *physicalop.BatchPointGetPlan:
		return statementRUPointLookup
	case *physicalop.Insert, *physicalop.Update, *physicalop.Delete:
		return statementRUWrite
	case *plannercore.Analyze:
		return statementRUAnalyze
	default:
		return statementRUWrapper
	}
}

func (report *statementRUFullReport) addOperator(engine statementRUEngine, operator statementRUOperator, units ruv2.StmtUnits) {
	if engine == statementRUTiFlash {
		report.add(engine, operator, units)
		return
	}
	// A root Reader/PointGet owns the evidence, but the scan and payload are
	// TiKV work. Keep that ownership distinct from the local executor work.
	remote := ruv2.StmtUnits{ScanBytes: units.ScanBytes, NetBytes: units.NetBytes}
	units.ScanBytes, units.NetBytes = 0, 0
	report.add(engine, operator, units)
	if remote.ScanBytes != 0 || remote.NetBytes != 0 {
		report.add(statementRUTiKV, operator, remote)
	}
}

func (calculator statementRUCalculator) engineResult(weights ruv2.StmtWeights) statementRUEngineResult {
	tidb, tikv, tiflash := calculator.compute[statementRUTiDB], calculator.compute[statementRUTiKV], calculator.compute[statementRUTiFlash]
	units := calculator.units
	return statementRUEngineResult{
		TiDB: weights.CPUWork*tidb.cpuWork + weights.HashStateRow*tidb.hashStateRows +
			weights.OperatorNum*tidb.operatorNum + weights.JoinOutputRow*(units.JoinOutputRows-tiflash.joinOutputRows) +
			weights.FrontendCompileByte*units.FrontendCompileBytes + weights.WriteStatement*units.WriteStatement,
		TiKV: weights.CPUWork*tikv.cpuWork + weights.HashStateRow*tikv.hashStateRows +
			weights.OperatorNum*tikv.operatorNum + weights.ScanByte*(units.ScanBytes-tiflash.scanBytes) +
			weights.NetByte*(units.NetBytes-tiflash.netBytes) + weights.WriteKey*units.WriteKeys + weights.WriteByte*units.WriteBytes,
		TiFlash: calculator.tiFlashRU(weights),
	}
}

func (calculator statementRUCalculator) tiFlashRU(weights ruv2.StmtWeights) float64 {
	tiflash := calculator.compute[statementRUTiFlash]
	return weights.CPUWork*tiflash.cpuWork + weights.HashStateRow*tiflash.hashStateRows +
		weights.OperatorNum*tiflash.operatorNum + weights.JoinOutputRow*tiflash.joinOutputRows +
		weights.ScanByte*tiflash.scanBytes + weights.NetByte*tiflash.netBytes + weights.CrossAZNetByte*tiflash.crossAZNetBytes
}

// addStatementUnits accounts for evidence outside individual operators once.
func (report *statementRUFullReport) addStatementUnits(units ruv2.StmtUnits) {
	report.add(statementRUTiDB, statementRUFrontend, ruv2.StmtUnits{FrontendCompileBytes: units.FrontendCompileBytes})
	if units.WriteStatement != 0 {
		report.add(statementRUTiDB, statementRUWrite, ruv2.StmtUnits{WriteStatement: units.WriteStatement})
	}
	if units.WriteKeys != 0 || units.WriteBytes != 0 {
		report.add(statementRUTiKV, statementRUKVWrite, ruv2.StmtUnits{WriteKeys: units.WriteKeys, WriteBytes: units.WriteBytes})
	}
}

func publishStatementRUFullMetrics(finalized statementRUFinalizedSnapshot) {
	for engine, operators := range finalized.report.units {
		for operator, units := range operators {
			if !finalized.report.seen[engine][operator] {
				continue
			}
			for _, unit := range [...]struct {
				name  string
				value float64
			}{
				{metrics.LblRUV2UnitCPUWork, units.CPUWork},
				{metrics.LblRUV2UnitScanBytes, units.ScanBytes},
				{metrics.LblRUV2UnitNetBytes, units.NetBytes},
				{metrics.LblRUV2UnitCrossAZNetBytes, units.CrossAZNetBytes},
				{metrics.LblRUV2UnitFrontendCompileBytes, units.FrontendCompileBytes},
				{metrics.LblRUV2UnitHashStateRows, units.HashStateRows},
				{metrics.LblRUV2UnitJoinOutputRows, units.JoinOutputRows},
				{metrics.LblRUV2UnitWriteStatement, units.WriteStatement},
				{metrics.LblRUV2UnitOperatorNum, units.OperatorNum},
				{metrics.LblRUV2UnitWriteKeys, units.WriteKeys},
				{metrics.LblRUV2UnitWriteBytes, units.WriteBytes},
			} {
				if unit.value == 0 {
					continue
				}
				metrics.RUV2Unit.WithLabelValues(statementRUEngineNames[engine], statementRUOperatorNames[operator], unit.name).Add(unit.value)
			}
		}
	}
	metrics.RUV2Statements.WithLabelValues("success", finalized.calibrationState.String()).Inc()
}

// These reasons are terminal calculation outcomes, not claims of complete
// remote evidence. Successful snapshots currently all remain incomplete.
type statementRUFailureReason string

const (
	statementRUNotFinished    statementRUFailureReason = "not_finished"
	statementRUUnsupported    statementRUFailureReason = "unsupported_plan"
	statementRUInvalid        statementRUFailureReason = "invalid_plan_or_evidence"
	statementRUStatementError statementRUFailureReason = "statement_error"
	statementRUIneligible     statementRUFailureReason = "ineligible"
	statementRUPanic          statementRUFailureReason = "panic"
)

func statementRUFailed(state statementRUOperatorState) statementRUFinalizedSnapshot {
	reason := statementRUInvalid
	if state == statementRUOperatorUnsupported {
		reason = statementRUUnsupported
	}
	return statementRUFinalizedSnapshot{failure: reason}
}

func statementRUTerminalFailure(rootEOF bool) statementRUFinalizedSnapshot {
	if !rootEOF {
		return statementRUFinalizedSnapshot{failure: statementRUNotFinished}
	}
	return statementRUFailed(statementRUOperatorInvalid)
}

func publishStatementRUFailureSafely(reason statementRUFailureReason) {
	defer func() { _ = recover() }()
	status := "failed"
	if reason == statementRUIneligible || reason == statementRUUnsupported {
		status = "skipped"
	}
	metrics.RUV2Statements.WithLabelValues(status, string(reason)).Inc()
}
