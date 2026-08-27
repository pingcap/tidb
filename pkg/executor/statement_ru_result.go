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
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// These deliberately uncalibrated weights keep the first ResultOnly path
// executable. They are internal placeholders, not billing values. Update them
// only together with the external model documentation until a later PR adds a
// configured model.
const (
	statementRUCPUWorkWeight             = 1.0
	statementRUScanByteWeight            = 1.0
	statementRUNetByteWeight             = 1.0
	statementRUFrontendCompileByteWeight = 1.0
)

type statementRURawUnits struct {
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
	// FrontendCompileBytes is the UTF-8 byte length of the source SQL text seen
	// by the compiler.
	FrontendCompileBytes float64
}

type statementRUResultOnly struct {
	TotalRU float64
}

// The current producers cannot prove that all successful or canceled remote
// work contributed execution details. ResultOnly therefore publishes a
// best-effort value from visible evidence, while every supported snapshot is
// marked incomplete for the dormant calibration consumer. "Best-effort lower
// bound" means missing units are not imputed; the aggregate scan-byte proxy is
// non-monotone, so it is not a strict mathematical bound.
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

// statementRUCalculationSetup is installed once for an eligible read statement
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
	if sessVars == nil || sessVars.StmtCtx == nil || !sessVars.StmtCtx.IsReadOnly ||
		sessVars.InRestrictedSQL || sessVars.HasStatusFlag(mysql.ServerStatusCursorExists) ||
		sessVars.StmtCtx.GetFlatPlan() != nil {
		return statementRUCalculationSetup{}, false
	}

	return statementRUCalculationSetup{
		frontendCompileBytes: statementRUFrontendCompileBytes(stmt),
	}, true
}

func statementRUFrontendCompileBytes(stmt *ExecStmt) float64 {
	if stmt == nil || stmt.StmtNode == nil {
		return 0
	}
	sql := stmt.StmtNode.OriginalText()
	if sql == "" && stmt.Ctx != nil && stmt.Ctx.GetSessionVars() != nil && stmt.Ctx.GetSessionVars().StmtCtx != nil {
		sql = stmt.Ctx.GetSessionVars().StmtCtx.OriginalSQL
	}
	if sql == "" {
		sql = stmt.StmtNode.Text()
	}
	if sql == "" {
		return 0
	}
	return float64(len(sql))
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
// bit, so a tuple that cannot run the demo formula is unavailable unless it is
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
	}, true
}

func validStatementRURawUnits(units statementRURawUnits) bool {
	for _, unit := range []float64{
		units.CPUWork,
		units.ScanBytes,
		units.NetBytes,
		units.FrontendCompileBytes,
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
		statementRUFrontendCompileByteWeight*units.FrontendCompileBytes}
}

func publishStatementRUFinalizedSnapshot(
	stmt *ExecStmt,
	finalized statementRUFinalizedSnapshot,
) {
	publishStatementRUMetricsSafely(finalized)
	publishStatementRUCalibrationSafely(stmt, statementRUCalibrationSnapshot{
		State: finalized.calibrationState,
		Units: finalized.units,
	})
}

// publishStatementRUMetricsSafely projects one immutable finalized snapshot to
// the existing RU v3 counters. ResultOnly retains aggregate CPUWork rather than
// a site split, so this layer preserves the lower-layer engine boundary:
// TiKV receives only scan and network work, while Total and SQLType receive the
// complete best-effort result.
func publishStatementRUMetricsSafely(finalized statementRUFinalizedSnapshot) {
	defer func() {
		_ = recover()
	}()
	totalRU := finalized.result.TotalRU
	metrics.RUV3Total.Add(totalRU)
	metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead).Add(totalRU)
	metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV).Add(
		statementRUScanByteWeight*finalized.units.ScanBytes +
			statementRUNetByteWeight*finalized.units.NetBytes,
	)
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
	)
}
