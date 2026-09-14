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
	"github.com/pingcap/tidb/pkg/resourcegroup/ruv3"
)

type statementRUEngine uint8

const (
	statementRUTiDB statementRUEngine = iota
	statementRUTiKV
	statementRUEngineCount
)

var statementRUEngineNames = [...]string{"tidb", "tikv"}

// Only computation has location-dependent ownership. Storage/transport units
// always belong to TiKV; frontend and write-statement units belong to TiDB.
type statementRUComputeUnits struct {
	cpuWork       float64
	hashStateRows float64
	operatorNum   float64
}

type statementRUEngineResult struct {
	TiDB float64
	TiKV float64
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
	units [statementRUEngineCount][statementRUOperatorCount]ruv3.StmtUnits
	seen  [statementRUEngineCount][statementRUOperatorCount]bool
}

func (report *statementRUFullReport) add(engine statementRUEngine, operator statementRUOperator, units ruv3.StmtUnits) {
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

func (report *statementRUFullReport) addOperator(engine statementRUEngine, operator statementRUOperator, units ruv3.StmtUnits) {
	// A root Reader/PointGet owns the evidence, but the scan and payload are
	// TiKV work. Keep that ownership distinct from the local executor work.
	remote := ruv3.StmtUnits{ScanBytes: units.ScanBytes, NetBytes: units.NetBytes}
	units.ScanBytes, units.NetBytes = 0, 0
	report.add(engine, operator, units)
	if remote.ScanBytes != 0 || remote.NetBytes != 0 {
		report.add(statementRUTiKV, operator, remote)
	}
}

func (calculator statementRUCalculator) engineResult(weights ruv3.StmtWeights) statementRUEngineResult {
	tidb, tikv := calculator.compute[statementRUTiDB], calculator.compute[statementRUTiKV]
	units := calculator.units
	return statementRUEngineResult{
		TiDB: weights.CPUWork*tidb.cpuWork + weights.HashStateRow*tidb.hashStateRows +
			weights.OperatorNum*tidb.operatorNum + weights.JoinOutputRow*units.JoinOutputRows +
			weights.FrontendCompileByte*units.FrontendCompileBytes + weights.WriteStatement*units.WriteStatement,
		TiKV: weights.CPUWork*tikv.cpuWork + weights.HashStateRow*tikv.hashStateRows +
			weights.OperatorNum*tikv.operatorNum + weights.ScanByte*units.ScanBytes +
			weights.NetByte*units.NetBytes + weights.WriteKey*units.WriteKeys + weights.WriteByte*units.WriteBytes,
	}
}

// addStatementUnits accounts for evidence outside individual operators once.
func (report *statementRUFullReport) addStatementUnits(units ruv3.StmtUnits) {
	report.add(statementRUTiDB, statementRUFrontend, ruv3.StmtUnits{FrontendCompileBytes: units.FrontendCompileBytes})
	if units.WriteStatement != 0 {
		report.add(statementRUTiDB, statementRUWrite, ruv3.StmtUnits{WriteStatement: units.WriteStatement})
	}
	if units.WriteKeys != 0 || units.WriteBytes != 0 {
		report.add(statementRUTiKV, statementRUKVWrite, ruv3.StmtUnits{WriteKeys: units.WriteKeys, WriteBytes: units.WriteBytes})
	}
}

// statementRUSQLTypeForPlan classifies a successfully calculated statement by
// its executed plan. Prepared statements have already been unwrapped, and the
// type is independent of affected rows or whether a transaction wrote any keys.
func statementRUSQLTypeForPlan(plan base.Plan) string {
	switch plan := plan.(type) {
	case *physicalop.Insert:
		if plan.IsReplace {
			return "replace"
		}
		return "insert"
	case *physicalop.Update:
		return "update"
	case *physicalop.Delete:
		return "delete"
	case *plannercore.Analyze:
		return "analyze"
	case *plannercore.Simple:
		// COMMIT is the only supported Simple plan.
		return "commit"
	default:
		return "select"
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
				{metrics.LblRUV3UnitCPUWork, units.CPUWork},
				{metrics.LblRUV3UnitScanBytes, units.ScanBytes},
				{metrics.LblRUV3UnitNetBytes, units.NetBytes},
				{metrics.LblRUV3UnitFrontendCompileBytes, units.FrontendCompileBytes},
				{metrics.LblRUV3UnitHashStateRows, units.HashStateRows},
				{metrics.LblRUV3UnitJoinOutputRows, units.JoinOutputRows},
				{metrics.LblRUV3UnitWriteStatement, units.WriteStatement},
				{metrics.LblRUV3UnitOperatorNum, units.OperatorNum},
				{metrics.LblRUV3UnitWriteKeys, units.WriteKeys},
				{metrics.LblRUV3UnitWriteBytes, units.WriteBytes},
			} {
				if unit.value == 0 {
					continue
				}
				metrics.RUV3Unit.WithLabelValues(statementRUEngineNames[engine], statementRUOperatorNames[operator], unit.name).Add(unit.value)
			}
		}
	}
	metrics.RUV3Statements.WithLabelValues("success", finalized.calibrationState.String()).Inc()
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
	metrics.RUV3Statements.WithLabelValues(status, string(reason)).Inc()
}
