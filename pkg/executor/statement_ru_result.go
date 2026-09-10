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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
)

// These deliberately uncalibrated work weights keep the ResultOnly path
// executable. They are internal placeholders, not billing values. Update them
// only together with the external model documentation until a later PR adds a
// configured model.
const (
	statementRUCPUWorkWeight             = 1.0
	statementRUScanByteWeight            = 1.0
	statementRUNetByteWeight             = 1.0
	statementRUFrontendCompileByteWeight = 1.0
	statementRUHashStateRowWeight        = 1.0
	statementRUJoinOutputRowWeight       = 1.0
	statementRUWriteStatementWeight      = 1.0
	statementRUOperatorNumWeight         = 1.0
	statementRUWriteKeyWeight            = 1.0
	statementRUWriteByteWeight           = 1.0
)

type statementRURawUnits struct {
	// WriteStatement is one for a write DML, including one affecting no rows.
	WriteStatement float64
	// OperatorNum counts final plan occurrences, including pushed operators.
	OperatorNum float64
	// WriteKeys and WriteBytes describe committed TiKV payload. Explicit
	// transactions contribute these only on COMMIT, not on each DML.
	WriteKeys  float64
	WriteBytes float64
	// CPUWork is the sum of occurrence-local operator work from the supported
	// root and coprocessor operators in the flat plan.
	CPUWork float64
	// ScanBytes is the sum of physical-byte estimates from supported Reader
	// request components. Each contribution is collected once from the pushed
	// plan root recorded for that Reader.
	ScanBytes float64
	// NetBytes is statement transport evidence, not operator attribution. It is
	// the TiKV coprocessor response-body byte count finalized in statement-local
	// RUv2 metrics.
	NetBytes float64
	// FrontendCompileBytes is the UTF-8 byte length of the normalized SQL text.
	FrontendCompileBytes float64
	// HashStateRows counts entries admitted to completed, operator-owned hash
	// lookup or group-state structures.
	HashStateRows float64
	// JoinOutputRows counts rows produced by supported Join occurrences after
	// their join conditions and join-type semantics are applied.
	JoinOutputRows float64
}

type statementRUResultOnly struct {
	TotalRU float64
}

// The current producers cannot prove that all successful or canceled remote
// work contributed execution details. ResultOnly therefore publishes a
// best-effort value from visible evidence, while every supported snapshot is
// marked incomplete for the dormant calibration consumer. Missing execution
// opportunities can underestimate work, while merged scan ratios and nonlinear
// lifecycle formulas can estimate in either direction or overestimate. The
// result is therefore neither exact nor a mathematical upper or lower bound.
type statementRUCalibrationState uint8

const (
	// Unknown is an internal zero value and must never be published.
	statementRUCalibrationUnknown statementRUCalibrationState = iota
	statementRUCalibrationComplete
	statementRUCalibrationIncomplete
)

func (state statementRUCalibrationState) String() string {
	switch state {
	case statementRUCalibrationUnknown:
		return "unknown"
	case statementRUCalibrationComplete:
		return "complete"
	case statementRUCalibrationIncomplete:
		return "incomplete"
	default:
		return "invalid"
	}
}

type statementRUCalibrationSnapshot struct {
	State statementRUCalibrationState
	Units statementRURawUnits
}

// statementRUCalculationSetup is installed once for an eligible statement
// and cleared by the first terminal attempt. It contains no plan pointer,
// topology state, publication mode, or consumer.
type statementRUCalculationSetup struct {
	frontendCompileBytes float64
}

// statementRUFinalizedSnapshot contains only values. It cannot retain an ExecStmt,
// FlatOperator, Origin, flat plan, calculator, or ExecDetails pointer.
type statementRUFinalizedSnapshot struct {
	units            statementRURawUnits
	result           statementRUResultOnly
	calibrationState statementRUCalibrationState
	writeSQL         bool
}

func installStatementRUOwner(stmt *ExecStmt) {
	setup, ok := newStatementRUCalculationSetup(stmt)
	if !ok {
		return
	}
	owner := newStatementRUOwner(stmt)
	owner.calculationSetup = setup
	stmt.statementRUOwner = owner
}

func newStatementRUCalculationSetup(stmt *ExecStmt) (statementRUCalculationSetup, bool) {
	if stmt == nil || stmt.Ctx == nil || stmt.Plan == nil {
		return statementRUCalculationSetup{}, false
	}
	sessVars := stmt.Ctx.GetSessionVars()
	_, isAnalyze := stmt.Plan.(*plannercore.Analyze)
	if sessVars == nil || sessVars.StmtCtx == nil {
		return statementRUCalculationSetup{}, false
	}
	eligible := sessVars.StmtCtx.IsReadOnly || isAnalyze || statementRUIsWritePlan(stmt.Plan) || statementRUIsCommitPlan(stmt.Plan)
	if !eligible ||
		sessVars.InRestrictedSQL || sessVars.HasStatusFlag(mysql.ServerStatusCursorExists) ||
		sessVars.StmtCtx.GetFlatPlan() != nil {
		return statementRUCalculationSetup{}, false
	}

	return statementRUCalculationSetup{
		frontendCompileBytes: statementRUFrontendCompileBytes(stmt),
	}, true
}

// statementRUIsWritePlan classifies DML independently of affected rows.
func statementRUIsWritePlan(plan base.Plan) bool {
	// Prepared statements are unwrapped by Exec after owner installation.
	if execute, ok := plan.(*plannercore.Execute); ok {
		plan = execute.Plan
	}
	switch plan.(type) {
	case *physicalop.Insert, *physicalop.Update, *physicalop.Delete:
		return true
	}
	return false
}

func statementRUIsCommitPlan(plan base.Plan) bool {
	simple, ok := plan.(*plannercore.Simple)
	if !ok {
		return false
	}
	_, ok = simple.Statement.(*ast.CommitStmt)
	return ok
}

func statementRUFrontendCompileBytes(stmt *ExecStmt) float64 {
	if stmt == nil || stmt.StmtNode == nil {
		return 0
	}

	sql := stmt.StmtNode.OriginalText()
	if stmt.Ctx != nil {
		if sessVars := stmt.Ctx.GetSessionVars(); sessVars != nil &&
			sessVars.StmtCtx != nil && sessVars.StmtCtx.OriginalSQL != "" {
			stmtCtx := sessVars.StmtCtx
			normalizedSQL, _ := stmtCtx.SQLDigest()
			normalizedSQL = trimStatementRUExplainPrefix(normalizedSQL)
			if normalizedSQL != "" {
				return float64(len(normalizedSQL))
			}
			if sql == "" {
				sql = stmtCtx.OriginalSQL
			}
		}
	}
	if sql == "" {
		sql = stmt.StmtNode.Text()
	}
	return float64(len(sql))
}

func trimStatementRUExplainPrefix(normalizedSQL string) string {
	for _, normalizedPrefix := range [...]string{
		"explain analyze format = ? ",
		"explain analyze format = ru ",
	} {
		if len(normalizedSQL) > len(normalizedPrefix) && normalizedSQL[:len(normalizedPrefix)] == normalizedPrefix {
			return normalizedSQL[len(normalizedPrefix):]
		}
	}
	return normalizedSQL
}

// statementRUCalculator is terminal-local. It accumulates only typed scalar
// units; no plan or execution-detail pointer survives calculateStatementRU.
type statementRUCalculator struct {
	units statementRURawUnits
}

func newStatementRUCalculator(setup statementRUCalculationSetup) statementRUCalculator {
	return statementRUCalculator{
		units: statementRURawUnits{
			FrontendCompileBytes: setup.frontendCompileBytes,
		},
	}
}

type statementRUScanEvidenceState uint8

const (
	statementRUScanEvidenceInvalid statementRUScanEvidenceState = iota
	statementRUScanEvidenceUnavailable
	statementRUScanEvidenceValid
)

type statementRUScanEvidence struct {
	state     statementRUScanEvidenceState
	scanBytes float64
}

// classifyStatementRUScanEvidence converts a value copy of one Reader's scan
// evidence into one raw-unit contribution. Zero-valued fields have no presence
// bit, so a tuple that cannot satisfy the scan-byte formula is unavailable unless it is
// provably contradictory. No RuntimeStatsColl or ScanDetail pointer survives.
func classifyStatementRUScanEvidence(totalKeys, processedKeys, processedBytes int64) statementRUScanEvidence {
	if totalKeys < 0 || processedKeys < 0 || processedBytes < 0 {
		return statementRUScanEvidence{state: statementRUScanEvidenceInvalid}
	}
	if processedKeys == 0 {
		// A branch with no processed-key evidence contributes zero even when
		// TotalKeys is present. Processed bytes without processed keys is
		// contradictory evidence and remains fail closed.
		if processedBytes == 0 {
			return statementRUScanEvidence{state: statementRUScanEvidenceValid}
		}
		return statementRUScanEvidence{state: statementRUScanEvidenceInvalid}
	}
	if totalKeys == 0 || processedBytes == 0 {
		return statementRUScanEvidence{state: statementRUScanEvidenceUnavailable}
	}

	scanBytes := float64(processedBytes) / float64(processedKeys) * float64(totalKeys)
	if scanBytes < 0 || math.IsNaN(scanBytes) || math.IsInf(scanBytes, 0) {
		return statementRUScanEvidence{state: statementRUScanEvidenceInvalid}
	}
	return statementRUScanEvidence{state: statementRUScanEvidenceValid, scanBytes: scanBytes}
}

func (calculator statementRUCalculator) finalize() (statementRUFinalizedSnapshot, bool) {
	if !validStatementRURawUnits(calculator.units) {
		return statementRUFinalizedSnapshot{}, false
	}
	result := calculateStatementRUResultOnly(calculator.units)
	if result.TotalRU < 0 || math.IsNaN(result.TotalRU) || math.IsInf(result.TotalRU, 0) {
		return statementRUFinalizedSnapshot{}, false
	}
	return statementRUFinalizedSnapshot{
		units:            calculator.units,
		result:           result,
		calibrationState: statementRUCalibrationIncomplete,
		writeSQL:         calculator.units.WriteStatement != 0 || calculator.units.WriteKeys != 0,
	}, true
}

func validStatementRURawUnits(units statementRURawUnits) bool {
	for _, unit := range []float64{
		units.WriteStatement, units.OperatorNum, units.WriteKeys, units.WriteBytes,
		units.CPUWork,
		units.ScanBytes,
		units.NetBytes,
		units.FrontendCompileBytes,
		units.HashStateRows,
		units.JoinOutputRows,
	} {
		if unit < 0 || math.IsNaN(unit) || math.IsInf(unit, 0) {
			return false
		}
	}
	return true
}

func calculateStatementRUResultOnly(units statementRURawUnits) statementRUResultOnly {
	return statementRUResultOnly{TotalRU: statementRUCPUWorkWeight*units.CPUWork +
		statementRUScanByteWeight*units.ScanBytes +
		statementRUNetByteWeight*units.NetBytes +
		statementRUFrontendCompileByteWeight*units.FrontendCompileBytes +
		statementRUHashStateRowWeight*units.HashStateRows +
		statementRUJoinOutputRowWeight*units.JoinOutputRows +
		statementRUWriteStatementWeight*units.WriteStatement +
		statementRUOperatorNumWeight*units.OperatorNum +
		statementRUWriteKeyWeight*units.WriteKeys +
		statementRUWriteByteWeight*units.WriteBytes}
}

func publishStatementRUFinalizedSnapshot(
	stmt *ExecStmt,
	finalized statementRUFinalizedSnapshot,
) {
	reportStatementRUV3ConsumptionSafely(stmt, finalized.result.TotalRU)
	publishStatementRUMetricsSafely(finalized)
	publishStatementRUCalibrationSafely(stmt, statementRUCalibrationSnapshot{
		State: finalized.calibrationState,
		Units: finalized.units,
	})
}

func reportStatementRUV3ConsumptionSafely(stmt *ExecStmt, totalRU float64) {
	defer func() {
		_ = recover()
	}()
	if stmt == nil || stmt.Ctx == nil || totalRU <= 0 {
		return
	}
	dctx := stmt.Ctx.GetDistSQLCtx()
	if dctx == nil || dctx.RUConsumptionReporter == nil || len(dctx.ResourceGroupName) == 0 {
		return
	}
	// TODO: distinguish TiDB/KV/Flash RU.
	dctx.RUConsumptionReporter.ReportRUV2Consumption(dctx.ResourceGroupName, 0, totalRU, 0)
}

// publishStatementRUMetricsSafely projects one immutable finalized snapshot to
// the existing RU v3 counters. ResultOnly retains aggregate CPUWork rather than
// a site split, so publication preserves the producer-owned engine boundary:
// TiKV receives scan, network and committed write work; Total and SQLType receive the
// complete best-effort result.
func publishStatementRUMetricsSafely(finalized statementRUFinalizedSnapshot) {
	defer func() {
		_ = recover()
	}()
	totalRU := finalized.result.TotalRU
	metrics.RUV3Total.Add(totalRU)
	sqlType := metrics.LblSQLTypeRead
	if finalized.writeSQL {
		sqlType = metrics.LblSQLTypeWrite
	}
	metrics.RUV3BySQLType.WithLabelValues(sqlType).Add(totalRU)
	metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV).Add(
		statementRUScanByteWeight*finalized.units.ScanBytes +
			statementRUNetByteWeight*finalized.units.NetBytes +
			statementRUWriteKeyWeight*finalized.units.WriteKeys +
			statementRUWriteByteWeight*finalized.units.WriteBytes,
	)
	metrics.RUV3Unit.WithLabelValues(metrics.LblRUV3UnitCPUWork).Add(finalized.units.CPUWork)
	metrics.RUV3Unit.WithLabelValues(metrics.LblRUV3UnitScanBytes).Add(finalized.units.ScanBytes)
	metrics.RUV3Unit.WithLabelValues(metrics.LblRUV3UnitNetBytes).Add(finalized.units.NetBytes)
	metrics.RUV3Unit.WithLabelValues(metrics.LblRUV3UnitFrontendCompileBytes).Add(finalized.units.FrontendCompileBytes)
	metrics.RUV3Unit.WithLabelValues(metrics.LblRUV3UnitHashStateRows).Add(finalized.units.HashStateRows)
	metrics.RUV3Unit.WithLabelValues(metrics.LblRUV3UnitJoinOutputRows).Add(finalized.units.JoinOutputRows)
}

func publishStatementRUCalibrationSafely(
	stmt *ExecStmt,
	snapshot statementRUCalibrationSnapshot,
) {
	defer func() {
		_ = recover()
	}()
	// The typed calibration boundary is intentionally dormant until a later PR
	// installs the real consumer. This failpoint only observes the same production
	// call; it does not select a test-only calculation or publication mode.
	connectionID := uint64(0)
	if stmt != nil && stmt.Ctx != nil && stmt.Ctx.GetSessionVars() != nil {
		connectionID = stmt.Ctx.GetSessionVars().ConnectionID
	}
	failpoint.InjectCall(
		"observeStatementRUCalibrationUnitsForTest",
		connectionID,
		snapshot.State.String(),
		snapshot.Units.CPUWork,
		snapshot.Units.ScanBytes,
		snapshot.Units.NetBytes,
		snapshot.Units.FrontendCompileBytes,
		snapshot.Units.HashStateRows,
		snapshot.Units.JoinOutputRows,
		snapshot.Units.WriteStatement,
		snapshot.Units.OperatorNum,
		snapshot.Units.WriteKeys,
		snapshot.Units.WriteBytes,
	)
}
