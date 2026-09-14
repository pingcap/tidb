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
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/resourcegroup/ruv3"
)

// currentStatementRUWeights reads the loaded config rather than capturing
// package-initialization defaults. RU v3 shares the ru-v2 config section while
// replacing the legacy model; its statement weights are not dynamically reloadable.
func currentStatementRUWeights() ruv3.StmtWeights {
	weights := config.DefaultRUV2Config()
	if cfg := config.GetGlobalConfig(); cfg != nil {
		weights = cfg.RUV2
	}
	return ruv3.StmtWeights{
		CPUWork:             weights.StatementCPUWork,
		ScanByte:            weights.StatementScanBytes,
		NetByte:             weights.StatementNetBytes,
		FrontendCompileByte: weights.StatementFrontendCompileBytes,
		HashStateRow:        weights.StatementHashStateRows,
		JoinOutputRow:       weights.StatementJoinOutputRows,
		WriteStatement:      weights.StatementWriteStatement,
		OperatorNum:         weights.StatementOperatorNum,
		WriteKey:            weights.StatementWriteKeys,
		WriteByte:           weights.StatementWriteBytes,
	}
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
	Units ruv3.StmtUnits
}

// statementRUCalculationSetup is installed once for an eligible statement
// and cleared by the first terminal attempt. It snapshots the reporting mode
// and contains no plan pointer, topology state, or consumer.
type statementRUCalculationSetup struct {
	frontendCompileBytes float64
	fullReport           bool
}

// statementRUFinalizedSnapshot owns scalar results and an optional full-mode
// report of numeric values. It retains no plan, executor, or runtime statistics.
type statementRUFinalizedSnapshot struct {
	units            ruv3.StmtUnits
	result           ruv3.StmtResult
	calibrationState statementRUCalibrationState
	sqlType          string
	engineRU         statementRUEngineResult
	report           *statementRUFullReport
	failure          statementRUFailureReason
}

func installStatementRUOwner(stmt *ExecStmt) {
	setup, ok := newStatementRUCalculationSetup(stmt)
	fullReport := config.GetGlobalConfig().RUV2.ReportMode == config.RUReportModeFull
	if !ok {
		// Restricted work is outside the user-statement calibration population.
		if fullReport && stmt != nil && stmt.Ctx != nil && stmt.Ctx.GetSessionVars() != nil &&
			!stmt.Ctx.GetSessionVars().InRestrictedSQL {
			publishStatementRUFailureSafely(statementRUIneligible)
		}
		return
	}
	setup.fullReport = fullReport
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
	units   ruv3.StmtUnits
	compute [statementRUEngineCount]statementRUComputeUnits
	report  *statementRUFullReport
}

func newStatementRUCalculator(setup statementRUCalculationSetup) statementRUCalculator {
	calculator := statementRUCalculator{
		units: ruv3.StmtUnits{
			FrontendCompileBytes: setup.frontendCompileBytes,
		},
	}
	if setup.fullReport {
		calculator.report = new(statementRUFullReport)
	}
	return calculator
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
	weights := currentStatementRUWeights()
	result, ok := ruv3.Calculate(calculator.units, weights)
	if !ok {
		return statementRUFailed(statementRUOperatorInvalid), false
	}
	engineRU := calculator.engineResult(weights)
	for _, ru := range [...]float64{result.TotalRU, engineRU.TiDB, engineRU.TiKV} {
		if ru < 0 || math.IsNaN(ru) || math.IsInf(ru, 0) {
			return statementRUFailed(statementRUOperatorInvalid), false
		}
	}
	if calculator.report != nil {
		// Freeze full-mode details independently of the mutable accumulator.
		report := *calculator.report
		report.addStatementUnits(calculator.units)
		calculator.report = &report
	}
	return statementRUFinalizedSnapshot{
		units:            calculator.units,
		result:           result,
		engineRU:         engineRU,
		report:           calculator.report,
		calibrationState: statementRUCalibrationIncomplete,
		sqlType:          "select",
	}, true
}

func publishStatementRUFinalizedSnapshot(
	stmt *ExecStmt,
	finalized statementRUFinalizedSnapshot,
) {
	reportStatementRUV3ConsumptionSafely(stmt, finalized.engineRU)
	publishStatementRUMetricsSafely(finalized)
	if finalized.report == nil {
		return
	}
	publishStatementRUCalibrationSafely(stmt, statementRUCalibrationSnapshot{
		State: finalized.calibrationState,
		Units: finalized.units,
	})
}

func reportStatementRUV3ConsumptionSafely(stmt *ExecStmt, result statementRUEngineResult) {
	defer func() {
		_ = recover()
	}()
	if stmt == nil || stmt.Ctx == nil || (result.TiDB <= 0 && result.TiKV <= 0) {
		return
	}
	dctx := stmt.Ctx.GetDistSQLCtx()
	if dctx == nil || dctx.RUConsumptionReporter == nil || len(dctx.ResourceGroupName) == 0 {
		return
	}
	dctx.RUConsumptionReporter.ReportRUV2Consumption(dctx.ResourceGroupName, result.TiKV, result.TiDB, 0)
}

// publishStatementRUMetricsSafely publishes result metrics using cached counters.
// All label lookup and calibration projections live behind the full-mode guard.
func publishStatementRUMetricsSafely(finalized statementRUFinalizedSnapshot) {
	defer func() {
		if recover() != nil && finalized.report != nil {
			publishStatementRUFailureSafely(statementRUPanic)
		}
	}()
	metrics.AddRUV3Results(finalized.engineRU.TiKV, finalized.engineRU.TiDB, finalized.result.TotalRU, finalized.sqlType)
	if finalized.report != nil {
		publishStatementRUFullMetrics(finalized)
	}
}

func publishStatementRUCalibrationSafely(
	stmt *ExecStmt,
	snapshot statementRUCalibrationSnapshot,
) {
	defer func() {
		_ = recover()
	}()
	// Full-mode tests observe the same terminal units as the metrics consumer.
	// Result mode never calls this calibration-only projection.
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
