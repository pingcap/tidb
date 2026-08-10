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
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const statementRUSimpleSelectExpectedMainOccurrences = uint32(2)

type statementRURawUnits struct {
	// ScanBytes estimates physical bytes scanned by the single TiKV table-scan
	// response from its ScanDetailV2 key counts and processed-key bytes.
	ScanBytes float64
	// NetBytes is the TiKV coprocessor response-body byte count finalized in the
	// statement-local RUv2 metrics.
	NetBytes float64
	// FrontendCompileBytes is the UTF-8 byte length of the source SQL text seen
	// by the compiler. A Complete calibration snapshot makes every field in this
	// value authoritative; consumers must reject the entire snapshot otherwise.
	FrontendCompileBytes float64
}

// statementRUTerminalExecDetailsView carries the shallow ExecDetails value
// already read by FinishExecuteStmt. It is borrowed only until that synchronous
// terminal call returns. Statement RU may read RequestCount and atomically load
// the three ScanDetail scalars; it must not retain aliases or add publisher use.
type statementRUTerminalExecDetailsView struct {
	execDetails execdetails.ExecDetails
}

type statementRUWeights struct {
	ScanBytes            float64
	NetBytes             float64
	FrontendCompileBytes float64
}

// statementRUPlaceholderWeightSnapshot returns deliberately uncalibrated
// internal-debug placeholders. Their total is not billing data, is not
// externally enabled, and is not promised to be comparable across commits.
func statementRUPlaceholderWeightSnapshot() statementRUWeights {
	return statementRUWeights{
		ScanBytes:            1,
		NetBytes:             1,
		FrontendCompileBytes: 1,
	}
}

type statementRUResultOnly struct {
	TotalRU float64
}

// statementRUCalibrationState is the complete publication contract for raw
// units. Complete means every field in Units is authoritative for this slice;
// a calibration consumer must reject the whole snapshot for every other state.
type statementRUCalibrationState uint8

const (
	statementRUCalibrationUnknown statementRUCalibrationState = iota
	statementRUCalibrationComplete
	statementRUCalibrationIncomplete
	statementRUCalibrationUnsupported
	statementRUCalibrationInvalid
)

type statementRUCalibrationSnapshot struct {
	State statementRUCalibrationState
	Units statementRURawUnits
}

// statementRUFailureReason is a bounded diagnostic dimension for the future
// failure counter. It neither authorizes calibration consumers to use Units
// nor says whether ResultOnly was published: for example, missing frontend
// text publishes ResultOnly with zero but still records IncompleteEvidence.
// This slice keeps one primary reason; calculator/evidence failures take
// precedence, and a publisher panic only fills an otherwise-unknown reason.
type statementRUFailureReason uint8

const (
	statementRUFailureUnknown statementRUFailureReason = iota
	statementRUFailureFlatPlanUnavailable
	statementRUFailureUnsupportedPlan
	statementRUFailureIncompleteEvidence
	statementRUFailureInvalidEvidence
	statementRUFailureCalculatorPanic
	statementRUFailureResultPublisherPanic
	statementRUFailureCalibrationPublisherPanic
)

type statementRUResultPublisher func(*zap.Logger, uint64, statementRUResultOnly)
type statementRUCalibrationPublisher func(statementRUCalibrationSnapshot)
type statementRUFailureRecorder func(statementRUFailureReason)

type statementRUSimpleSelectAccumulator struct {
	// These topology facts are transient calculator gates. They prove that the
	// current narrow formula applies, but are discarded at freeze and are never
	// retained in ResultOnly or a calibration snapshot.
	observedMain uint32
	unsupported  bool
	rootSeen     bool
	scanSeen     bool
}

// statementRUSimpleSelectPlanBinding is a freeze-local borrowed view of the
// reader and scan owned by the current ExecStmt.Plan. It is used only during
// the synchronous occurrence walk and never retained in the run, owner, or a
// publication. Plan ID is exposed only after this pointer binding succeeds and
// only as the runtime-stat lookup key.
type statementRUSimpleSelectPlanBinding struct {
	reader *physicalop.PhysicalTableReader
	scan   *physicalop.PhysicalTableScan
}

type statementRUSimpleSelectRun struct {
	frontendCompileBytes   float64
	frontendCompilePresent bool
	resultPublisher        statementRUResultPublisher
	calibrationPublisher   statementRUCalibrationPublisher
	// failureRecorder remains nil in production until the metrics PR connects
	// this fixed-reason seam to a bounded aggregate counter.
	failureRecorder statementRUFailureRecorder
}

type statementRUSimpleSelectPublication struct {
	resultPublisher      statementRUResultPublisher
	resultLogger         *zap.Logger
	connectionID         uint64
	result               statementRUResultOnly
	hasResult            bool
	calibrationPublisher statementRUCalibrationPublisher
	calibration          statementRUCalibrationSnapshot
	hasCalibration       bool
	failureRecorder      statementRUFailureRecorder
	failureReason        statementRUFailureReason
}

func installStatementRUSimpleSelectOwner(stmt *ExecStmt) {
	run, ok := newStatementRUSimpleSelectRun(stmt)
	if !ok {
		return
	}
	owner := newStatementRUSimpleSelectPlanWalkOwner(stmt, run)
	stmt.statementRUPlanWalkOwner = owner

	connectionID := stmt.Ctx.GetSessionVars().ConnectionID
	failpoint.InjectCall(
		"enableStatementRUCalibrationPublisherForTest",
		connectionID,
		func() bool {
			if owner.simpleSelectRun.calibrationPublisher != nil {
				return false
			}
			owner.simpleSelectRun.calibrationPublisher = newStatementRUCalibrationTestPublisher(connectionID)
			return true
		},
	)
}

func newStatementRUCalibrationTestPublisher(connectionID uint64) statementRUCalibrationPublisher {
	return func(snapshot statementRUCalibrationSnapshot) {
		failpoint.InjectCall(
			"observeStatementRUCalibrationUnitsForTest",
			connectionID,
			uint8(snapshot.State),
			snapshot.Units.ScanBytes,
			snapshot.Units.NetBytes,
			snapshot.Units.FrontendCompileBytes,
		)
	}
}

func newStatementRUSimpleSelectRun(stmt *ExecStmt) (statementRUSimpleSelectRun, bool) {
	if stmt == nil || stmt.Ctx == nil || stmt.Plan == nil || stmt.PsStmt != nil {
		return statementRUSimpleSelectRun{}, false
	}
	selectStmt, ok := stmt.StmtNode.(*ast.SelectStmt)
	if !ok || selectStmt.Kind != ast.SelectStmtKindSelect || selectStmt.With != nil ||
		selectStmt.LockInfo != nil || selectStmt.SelectIntoOpt != nil || selectStmt.AfterSetOperator != nil {
		return statementRUSimpleSelectRun{}, false
	}
	if _, ok := stmt.Plan.(*physicalop.PhysicalTableReader); !ok {
		return statementRUSimpleSelectRun{}, false
	}
	sessVars := stmt.Ctx.GetSessionVars()
	if sessVars == nil || sessVars.StmtCtx == nil || !plannercore.IsAutoCommitTxn(sessVars) ||
		sessVars.InRestrictedSQL || sessVars.HasStatusFlag(mysql.ServerStatusCursorExists) || sessVars.FoundInPlanCache ||
		sessVars.StmtCtx.UseCache() || sessVars.StmtCtx.GetFlatPlan() != nil {
		return statementRUSimpleSelectRun{}, false
	}

	frontendBytes, frontendPresent := statementRUFrontendCompileBytes(stmt)
	return statementRUSimpleSelectRun{
		frontendCompileBytes:   frontendBytes,
		frontendCompilePresent: frontendPresent,
		resultPublisher:        publishStatementRUDebugResult,
	}, true
}

func publishStatementRUDebugResult(logger *zap.Logger, connectionID uint64, result statementRUResultOnly) {
	if logger != nil {
		if checkedEntry := logger.Check(zap.DebugLevel, "uncalibrated statement RU result"); checkedEntry != nil {
			checkedEntry.Write(zap.Float64("total_ru", result.TotalRU))
		}
	}
	// Test observation is independent of the configured log level.
	failpoint.InjectCall("observeStatementRUResultForTest", connectionID, result.TotalRU)
}

func statementRUFrontendCompileBytes(stmt *ExecStmt) (float64, bool) {
	if stmt == nil || stmt.StmtNode == nil {
		return 0, false
	}
	sql := stmt.StmtNode.OriginalText()
	if sql == "" && stmt.Ctx != nil && stmt.Ctx.GetSessionVars() != nil && stmt.Ctx.GetSessionVars().StmtCtx != nil {
		sql = stmt.Ctx.GetSessionVars().StmtCtx.OriginalSQL
	}
	if sql == "" {
		sql = stmt.StmtNode.Text()
	}
	if sql == "" {
		return 0, false
	}
	return float64(len(sql)), true
}

func (run *statementRUSimpleSelectRun) freeze(
	stmt *ExecStmt,
	owner *statementRUPlanWalkOwner,
	flat *plannercore.FlatPhysicalPlan,
	execView *statementRUTerminalExecDetailsView,
) statementRUSimpleSelectPublication {
	connectionID := uint64(0)
	loggerContext := context.Background()
	if stmt != nil {
		if stmt.GoCtx != nil {
			loggerContext = stmt.GoCtx
		}
		if stmt.Ctx != nil {
			if sessVars := stmt.Ctx.GetSessionVars(); sessVars != nil {
				connectionID = sessVars.ConnectionID
			}
		}
	}
	failpoint.InjectCall("observeStatementRUFreezeForTest", connectionID)
	publication := statementRUSimpleSelectPublication{
		resultPublisher:      run.resultPublisher,
		resultLogger:         logutil.Logger(loggerContext),
		connectionID:         connectionID,
		calibrationPublisher: run.calibrationPublisher,
		failureRecorder:      run.failureRecorder,
	}
	if flat == nil {
		return projectStatementRUFreeze(
			statementRURawUnits{},
			statementRUCalibrationUnsupported,
			statementRUFailureFlatPlanUnavailable,
			publication,
		)
	}

	accumulator := statementRUSimpleSelectAccumulator{}
	accumulator.start(flat)
	binding, owned := bindStatementRUSimpleSelectPlan(stmt.Plan)
	if !owned {
		accumulator.unsupported = true
	}
	walkStatementRUFlatPlan(flat, func(
		treeKind statementRUPlanTreeKind,
		_ int,
		_ int,
		operator *plannercore.FlatOperator,
	) {
		accumulator.observe(binding, treeKind, operator)
	})
	accumulator.finish()

	units := statementRURawUnits{}
	if run.frontendCompilePresent {
		units.FrontendCompileBytes = run.frontendCompileBytes
	}

	if accumulator.unsupported {
		return projectStatementRUFreeze(
			units,
			statementRUCalibrationUnsupported,
			statementRUFailureUnsupportedPlan,
			publication,
		)
	}

	executionComplete, invalid := captureStatementRUExecutionEvidence(stmt, binding.runtimeStatsPlanID(), execView, &units)
	weights := statementRUPlaceholderWeightSnapshot()
	if invalid || !validStatementRUWeights(weights) {
		return projectStatementRUFreeze(
			units,
			statementRUCalibrationInvalid,
			statementRUFailureInvalidEvidence,
			publication,
		)
	}

	termination := statementRUResultTermination(owner.resultTermination.Load())
	coreComplete := termination == statementRUResultTerminationEOF && executionComplete
	if !coreComplete {
		// An early close or another evidence gap is a missing ResultOnly sample,
		// never a zero-RU result. Suppress ResultOnly and, when calibration is
		// attached, publish one Incomplete snapshot for downstream rejection.
		// Future aggregate reporting must count this failure separately; if it
		// needs actual consumed RU for early-closed statements, it must establish
		// a producer-coverage contract rather than treating missing as zero.
		return projectStatementRUFreeze(
			units,
			statementRUCalibrationIncomplete,
			statementRUFailureIncompleteEvidence,
			publication,
		)
	}

	frontendForResult := units.FrontendCompileBytes
	if !run.frontendCompilePresent {
		frontendForResult = 0
	}
	total, ok := weights.total(statementRURawUnits{
		ScanBytes:            units.ScanBytes,
		NetBytes:             units.NetBytes,
		FrontendCompileBytes: frontendForResult,
	})
	if !ok {
		return projectStatementRUFreeze(
			units,
			statementRUCalibrationInvalid,
			statementRUFailureInvalidEvidence,
			publication,
		)
	}
	publication.result = statementRUResultOnly{TotalRU: total}
	publication.hasResult = true
	state := statementRUCalibrationComplete
	reason := statementRUFailureUnknown
	if !run.frontendCompilePresent {
		state = statementRUCalibrationIncomplete
		reason = statementRUFailureIncompleteEvidence
	}
	return projectStatementRUFreeze(units, state, reason, publication)
}

func projectStatementRUFreeze(
	units statementRURawUnits,
	state statementRUCalibrationState,
	reason statementRUFailureReason,
	publication statementRUSimpleSelectPublication,
) statementRUSimpleSelectPublication {
	publication.failureReason = reason
	if publication.calibrationPublisher != nil {
		publication.calibration = statementRUCalibrationSnapshot{
			State: state,
			Units: units,
		}
		publication.hasCalibration = true
	}
	return publication
}

func captureStatementRUExecutionEvidence(
	stmt *ExecStmt,
	scanPlanID int,
	execView *statementRUTerminalExecDetailsView,
	units *statementRURawUnits,
) (complete bool, invalid bool) {
	// Counts and presence checks below are local proof inputs for this exact
	// one-response SELECT. Only their aggregate decision survives the freeze as
	// calibration State (and, separately, a FailureReason for future metrics).
	// Publishing the proof details would expose an accidental topology contract
	// without making a non-Complete sample usable.
	if stmt == nil || stmt.Ctx == nil || units == nil {
		return false, true
	}
	sessVars := stmt.Ctx.GetSessionVars()
	if sessVars == nil || sessVars.StmtCtx == nil {
		return false, true
	}
	var execDetails execdetails.ExecDetails
	if execView == nil {
		execDetails = sessVars.StmtCtx.GetExecDetails()
	} else {
		execDetails = execView.execDetails
	}
	if execDetails.RequestCount < 0 {
		return false, true
	}
	acceptedResponses := uint64(execDetails.RequestCount)
	observedScanTasks := uint64(0)
	if sessVars.StmtCtx.RuntimeStatsColl != nil {
		tasks, _ := sessVars.StmtCtx.RuntimeStatsColl.GetCopCountAndRows(scanPlanID)
		if tasks < 0 {
			return false, true
		}
		observedScanTasks = uint64(tasks)
	}
	scanBytesPresent := false
	if execDetails.ScanDetail != nil {
		totalKeys := atomic.LoadInt64(&execDetails.ScanDetail.TotalKeys)
		processedKeys := atomic.LoadInt64(&execDetails.ScanDetail.ProcessedKeys)
		processedBytes := atomic.LoadInt64(&execDetails.ScanDetail.ProcessedKeysSize)
		if totalKeys < 0 || processedKeys < 0 || processedBytes < 0 {
			return false, true
		}
		if totalKeys > 0 && processedKeys > 0 && processedBytes > 0 {
			scanBytes := float64(processedBytes) / float64(processedKeys) * float64(totalKeys)
			if !finitePositiveStatementRUValue(scanBytes) {
				return false, true
			}
			units.ScanBytes = scanBytes
			scanBytesPresent = true
		}
	}

	readerRequests := uint64(0)
	netBytesPresent := false
	metrics := sessVars.RUV2Metrics
	if metrics != nil && !metrics.Bypass() {
		readRequests := metrics.ResourceManagerReadCnt()
		netBytes := metrics.TiKVCoprocessorResponseBytes()
		if readRequests < 0 || netBytes < 0 {
			return false, true
		}
		readerRequests = uint64(readRequests)
		if netBytes > 0 {
			units.NetBytes = float64(netBytes)
			netBytesPresent = true
		}
	}
	return acceptedResponses == 1 && observedScanTasks == 1 && readerRequests == 1 &&
		scanBytesPresent && netBytesPresent, false
}

func (accumulator *statementRUSimpleSelectAccumulator) start(flat *plannercore.FlatPhysicalPlan) {
	if flat == nil || len(flat.Main) != int(statementRUSimpleSelectExpectedMainOccurrences) ||
		len(flat.CTEs) != 0 || len(flat.ScalarSubQueries) != 0 || flat.InExecute || flat.InExplain {
		accumulator.unsupported = true
	}
}

func bindStatementRUSimpleSelectPlan(
	currentPlan base.Plan,
) (statementRUSimpleSelectPlanBinding, bool) {
	reader, ok := currentPlan.(*physicalop.PhysicalTableReader)
	if !ok || reader.StoreType != kv.TiKV || reader.ReadReqType != physicalop.Cop ||
		len(reader.TablePlans) != 1 || reader.TablePlan == nil {
		return statementRUSimpleSelectPlanBinding{}, false
	}
	scan, ok := reader.TablePlan.(*physicalop.PhysicalTableScan)
	if !ok || reader.TablePlans[0] != scan || scan.Table == nil || scan.Table.GetPartitionInfo() != nil ||
		scan.IsMPPOrBatchCop || scan.StoreType != kv.TiKV {
		return statementRUSimpleSelectPlanBinding{}, false
	}
	return statementRUSimpleSelectPlanBinding{reader: reader, scan: scan}, true
}

func (binding statementRUSimpleSelectPlanBinding) runtimeStatsPlanID() int {
	return binding.scan.ID()
}

func (accumulator *statementRUSimpleSelectAccumulator) observe(
	binding statementRUSimpleSelectPlanBinding,
	treeKind statementRUPlanTreeKind,
	operator *plannercore.FlatOperator,
) {
	if treeKind != statementRUPlanTreeMain || operator == nil {
		accumulator.unsupported = true
		return
	}
	accumulator.observedMain++
	switch origin := operator.Origin.(type) {
	case *physicalop.PhysicalTableReader:
		if binding.reader == nil || origin != binding.reader || accumulator.rootSeen || !operator.IsPhysicalPlan || !operator.IsRoot ||
			operator.Depth != 0 || operator.StoreType != kv.TiDB {
			accumulator.unsupported = true
			return
		}
		accumulator.rootSeen = true
	case *physicalop.PhysicalTableScan:
		if binding.scan == nil || origin != binding.scan || accumulator.scanSeen || !operator.IsPhysicalPlan || operator.IsRoot ||
			operator.Depth != 1 || operator.StoreType != kv.TiKV || operator.ReqType != physicalop.Cop {
			accumulator.unsupported = true
			return
		}
		accumulator.scanSeen = true
	default:
		accumulator.unsupported = true
	}
}

func (accumulator *statementRUSimpleSelectAccumulator) finish() {
	if accumulator.observedMain != statementRUSimpleSelectExpectedMainOccurrences ||
		!accumulator.rootSeen || !accumulator.scanSeen {
		accumulator.unsupported = true
	}
}

func (weights statementRUWeights) total(units statementRURawUnits) (float64, bool) {
	total := weights.ScanBytes*units.ScanBytes +
		weights.NetBytes*units.NetBytes +
		weights.FrontendCompileBytes*units.FrontendCompileBytes
	return total, finiteNonNegativeStatementRUValue(total)
}

func validStatementRUWeights(weights statementRUWeights) bool {
	return finiteNonNegativeStatementRUValue(weights.ScanBytes) &&
		finiteNonNegativeStatementRUValue(weights.NetBytes) &&
		finiteNonNegativeStatementRUValue(weights.FrontendCompileBytes)
}

func finitePositiveStatementRUValue(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func finiteNonNegativeStatementRUValue(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func statementRUSimpleSelectPublicationAfterPanic(run *statementRUSimpleSelectRun) statementRUSimpleSelectPublication {
	if run == nil {
		return statementRUSimpleSelectPublication{}
	}
	return statementRUSimpleSelectPublication{
		failureRecorder: run.failureRecorder,
		failureReason:   statementRUFailureCalculatorPanic,
	}
}

func (publication statementRUSimpleSelectPublication) publish() {
	// Calculator/evidence failure is the primary statement reason. A publisher
	// panic fills an otherwise-unknown reason only; the later metrics integration
	// must use independent dimensions if it needs to count both failures.
	reason := publication.failureReason
	if publication.hasResult && publication.resultPublisher != nil {
		if !publishStatementRUResult(
			publication.resultPublisher,
			publication.resultLogger,
			publication.connectionID,
			publication.result,
		) && reason == statementRUFailureUnknown {
			reason = statementRUFailureResultPublisherPanic
		}
	}
	if publication.hasCalibration && publication.calibrationPublisher != nil {
		if !publishStatementRUCalibration(publication.calibrationPublisher, publication.calibration) && reason == statementRUFailureUnknown {
			reason = statementRUFailureCalibrationPublisherPanic
		}
	}
	if reason != statementRUFailureUnknown && publication.failureRecorder != nil {
		recordStatementRUFailure(publication.failureRecorder, reason)
	}
}

func publishStatementRUResult(
	publisher statementRUResultPublisher,
	logger *zap.Logger,
	connectionID uint64,
	result statementRUResultOnly,
) (ok bool) {
	ok = true
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	publisher(logger, connectionID, result)
	return ok
}

func publishStatementRUCalibration(
	publisher statementRUCalibrationPublisher,
	snapshot statementRUCalibrationSnapshot,
) (ok bool) {
	ok = true
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	publisher(snapshot)
	return ok
}

func recordStatementRUFailure(recorder statementRUFailureRecorder, reason statementRUFailureReason) {
	defer func() {
		_ = recover()
	}()
	recorder(reason)
}
