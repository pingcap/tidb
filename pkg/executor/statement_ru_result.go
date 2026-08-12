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
	"context"
	"math"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// These deliberately uncalibrated weights keep the first ResultOnly path
// executable. They are internal placeholders, not billing values. Update them
// only together with the external model documentation until a later PR adds a
// configured model.
const (
	statementRUScanByteWeight            = 1.0
	statementRUNetByteWeight             = 1.0
	statementRUFrontendCompileByteWeight = 1.0
)

type statementRURawUnits struct {
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

// Complete means the supported traversal consumed every unit required by the
// current model after the result reached EOF. Incomplete snapshots are still
// passed through the dormant calibration publication boundary so a future
// consumer can reject them explicitly.
type statementRUCalibrationState uint8

const (
	// Unknown is an internal zero value and must never be published.
	statementRUCalibrationUnknown statementRUCalibrationState = iota
	statementRUCalibrationComplete
	statementRUCalibrationIncomplete
)

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
	hasResult        bool
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
// units and invalid evidence; no plan or execution-detail pointer survives
// calculateStatementRU.
type statementRUCalculator struct {
	units           statementRURawUnits
	invalidEvidence bool
}

func newStatementRUCalculator(setup statementRUCalculationSetup) statementRUCalculator {
	return statementRUCalculator{
		units: statementRURawUnits{
			FrontendCompileBytes: setup.frontendCompileBytes,
		},
	}
}

// statementRUScanBytes converts a value copy of one Reader's scan evidence into
// one raw-unit contribution. It does not retain RuntimeStatsColl or ScanDetail.
func statementRUScanBytes(totalKeys, processedKeys, processedBytes int64) (float64, statementRUCalibrationState) {
	if totalKeys < 0 || processedKeys < 0 || processedBytes < 0 {
		return 0, statementRUCalibrationUnknown
	}
	if processedKeys == 0 && (totalKeys != 0 || processedBytes != 0) {
		return 0, statementRUCalibrationUnknown
	}
	if totalKeys == 0 || processedKeys == 0 || processedBytes == 0 {
		return 0, statementRUCalibrationIncomplete
	}

	scanBytes := float64(processedBytes) / float64(processedKeys) * float64(totalKeys)
	if scanBytes > math.MaxFloat64 {
		return 0, statementRUCalibrationUnknown
	}
	return scanBytes, statementRUCalibrationComplete
}

func (calculator statementRUCalculator) finalize(evidenceComplete bool) statementRUFinalizedSnapshot {
	finalized := statementRUFinalizedSnapshot{
		units:            calculator.units,
		calibrationState: statementRUCalibrationComplete,
	}
	if calculator.invalidEvidence {
		finalized.calibrationState = statementRUCalibrationUnknown
		return finalized
	}
	if !evidenceComplete || calculator.units.ScanBytes <= 0 {
		// A successful statement may still be closed before the client consumes
		// the result to EOF (for example, after a connection write failure). In
		// that case the recorded aggregates describe only the work observed before
		// close. Traversal also fails closed when it cannot associate the existing
		// statement evidence with a supported operator occurrence.
		finalized.calibrationState = statementRUCalibrationIncomplete
		return finalized
	}

	if finalized.units.FrontendCompileBytes == 0 {
		// Frontend text is optional for ResultOnly in this first slice, where its
		// contribution is explicitly projected as zero. Calibration still marks
		// the same finalized evidence incomplete rather than learning from that zero.
		finalized.calibrationState = statementRUCalibrationIncomplete
	}
	finalized.result = calculateStatementRUResultOnly(finalized.units)
	if finalized.result.TotalRU < 0 || math.IsNaN(finalized.result.TotalRU) || math.IsInf(finalized.result.TotalRU, 0) {
		finalized.calibrationState = statementRUCalibrationUnknown
		return finalized
	}
	finalized.hasResult = true
	return finalized
}

func calculateStatementRUResultOnly(units statementRURawUnits) statementRUResultOnly {
	return statementRUResultOnly{TotalRU: statementRUScanByteWeight*units.ScanBytes +
		statementRUNetByteWeight*units.NetBytes +
		statementRUFrontendCompileByteWeight*units.FrontendCompileBytes}
}

func publishStatementRUFinalizedSnapshot(
	stmt *ExecStmt,
	finalized statementRUFinalizedSnapshot,
) {
	if finalized.hasResult {
		publishStatementRUResultSafely(stmt, finalized.result)
	}
	if finalized.calibrationState != statementRUCalibrationUnknown {
		publishStatementRUCalibrationSafely(stmt, statementRUCalibrationSnapshot{
			State: finalized.calibrationState,
			Units: finalized.units,
		})
	}
}

func publishStatementRUResultSafely(stmt *ExecStmt, result statementRUResultOnly) {
	defer func() {
		_ = recover()
	}()
	connectionID := uint64(0)
	loggerContext := context.Background()
	if stmt != nil {
		if stmt.GoCtx != nil {
			loggerContext = stmt.GoCtx
		}
		if stmt.Ctx != nil && stmt.Ctx.GetSessionVars() != nil {
			connectionID = stmt.Ctx.GetSessionVars().ConnectionID
		}
	}
	logger := logutil.Logger(loggerContext)
	if checkedEntry := logger.Check(zap.DebugLevel, "uncalibrated statement RU result"); checkedEntry != nil {
		checkedEntry.Write(zap.Float64("total_ru", result.TotalRU))
	}
	// Test observation is independent of the configured log level.
	failpoint.InjectCall("observeStatementRUResultForTest", connectionID, result.TotalRU)
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
		uint8(snapshot.State),
		snapshot.Units.ScanBytes,
		snapshot.Units.NetBytes,
		snapshot.Units.FrontendCompileBytes,
	)
}
