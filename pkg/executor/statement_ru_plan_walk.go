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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

type statementRUFinalOutcome uint32

const (
	statementRUFinalOutcomeUnknown statementRUFinalOutcome = iota
	statementRUFinalOutcomeSuccess
	statementRUFinalOutcomeFailure
)

// statementRUOwner owns the first-record-wins outcome and the first terminal
// finalization for one ExecStmt. Its calculation setup is released when the
// shared finishOnce is consumed.
type statementRUOwner struct {
	finishOnce   sync.Once
	finalOutcome atomic.Uint32
	rootEOF      atomic.Bool
	// Install-time snapshots reject transient restricted/cursor classifications
	// that may be restored before a delayed result-set terminal.
	restrictedSQLAtInstall bool
	cursorAtInstall        bool
	calculationSetup       statementRUCalculationSetup
}

func newStatementRUOwner(stmt *ExecStmt) *statementRUOwner {
	owner := &statementRUOwner{}
	if stmt == nil || stmt.Ctx == nil {
		return owner
	}
	sessVars := stmt.Ctx.GetSessionVars()
	if sessVars == nil {
		return owner
	}
	owner.restrictedSQLAtInstall = sessVars.InRestrictedSQL
	owner.cursorAtInstall = sessVars.HasStatusFlag(mysql.ServerStatusCursorExists)
	return owner
}

// RecordStatementRUFinalOutcome publishes the final session outcome for the
// statement-local RU finalization. It remains a nil-owner no-op outside the
// current calculation policy and tests. The first record wins and a recorded
// failure consumes the owner immediately; success must be recorded before the
// existing executor terminal can finalize RU.
func (a *ExecStmt) RecordStatementRUFinalOutcome(success bool) {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}

	outcome := statementRUFinalOutcomeFailure
	if success {
		outcome = statementRUFinalOutcomeSuccess
	}
	recorded := owner.finalOutcome.CompareAndSwap(
		uint32(statementRUFinalOutcomeUnknown),
		uint32(outcome),
	)
	if recorded && !success {
		a.abortStatementRU()
	}
}

// abortStatementRU consumes the owner without running RU calculation.
// RU-only failure paths use it instead of mutating legacy lastErrs or running
// a full executor terminal solely for this RU layer.
func (a *ExecStmt) abortStatementRU() {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}
	owner.finishOnce.Do(func() {
		owner.calculationSetup = statementRUCalculationSetup{}
	})
}

// recordStatementRURootEOF records that the root executor returned an empty
// chunk. A successful statement does not imply this: a caller can
// close a RecordSet cleanly before consuming all rows, for example when writing
// rows to the client fails. Publishing that partial work as the statement's RU
// would undercount, so the narrow ResultOnly path requires this independent bit.
func (a *ExecStmt) recordStatementRURootEOF() {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}
	owner.rootEOF.Store(true)
}

// TODO: when the statement-RU failure metric lands, count the bounded failure
// reasons at these fail-closed exits and publisher recoveries. Until then there
// is deliberately no recorder-shaped no-op API.
func (a *ExecStmt) finishStatementRU(terminalErr error) {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}

	var finalized statementRUFinalizedSnapshot
	publishFinalized := false
	owner.finishOnce.Do(func() {
		calculationSetup := owner.calculationSetup
		owner.calculationSetup = statementRUCalculationSetup{}

		// The entire hook is fail-closed. A panic in eligibility, flat-plan
		// lookup/generation, or calculation must neither make the owner retryable
		// nor interrupt existing terminal bookkeeping.
		defer func() {
			_ = recover()
		}()

		if statementRUFinalOutcome(owner.finalOutcome.Load()) != statementRUFinalOutcomeSuccess || terminalErr != nil {
			return
		}
		// a.Plan remains the statement eligibility guard even though the flat-plan
		// view below comes from StatementContext.
		if a.Ctx == nil || a.Plan == nil {
			return
		}

		sessVars := a.Ctx.GetSessionVars()
		// The snapshots catch eligibility that disappeared before terminal; the
		// live checks catch a classification entered after owner installation.
		if sessVars == nil || owner.restrictedSQLAtInstall || owner.cursorAtInstall ||
			sessVars.InRestrictedSQL || sessVars.HasStatusFlag(mysql.ServerStatusCursorExists) {
			return
		}
		flat := getFlatPlan(sessVars.StmtCtx)
		if flat == nil {
			return
		}

		// The fresh-session slice must use a flat plan rooted at this ExecStmt.
		// General flat-plan generation identity remains outside this layer.
		if len(flat.Main) == 0 || flat.Main[0] == nil || flat.Main[0].Origin != a.Plan {
			return
		}
		finalized, publishFinalized = calculateStatementRU(
			flat,
			sessVars.StmtCtx.RuntimeStatsColl,
			sessVars.RUV2Metrics,
			calculationSetup,
			owner.rootEOF.Load(),
		)
	})
	if publishFinalized {
		publishStatementRUFinalizedSnapshot(a, finalized)
	}
}

// calculateStatementRU directly walks borrowed flat-plan occurrences without
// retaining or mutating them and builds one value-only result. Scan evidence
// belongs to the Reader request component that produced it, so it is
// accumulated at the Reader occurrence; pushed operators and scans do not
// shuttle or consume a second copy.
func calculateStatementRU(
	flat *plannercore.FlatPhysicalPlan,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	metrics *execdetails.RUV2Metrics,
	setup statementRUCalculationSetup,
	rootEOF bool,
) (statementRUFinalizedSnapshot, bool) {
	if flat == nil || len(flat.Main) == 0 || len(flat.CTEs) != 0 || len(flat.ScalarSubQueries) != 0 {
		return statementRUFinalizedSnapshot{}, false
	}
	calculator := newStatementRUCalculator(setup)
	evidenceComplete := rootEOF
	// Transport bytes are statement evidence in both the current producer and
	// the demo model. Read them once; do not attribute the same aggregate to
	// every Reader occurrence.
	if metrics == nil || metrics.Bypass() {
		evidenceComplete = false
	} else {
		netBytes := metrics.TiKVCoprocessorResponseBytes()
		switch {
		case netBytes < 0:
			calculator.invalidEvidence = true
		case netBytes == 0:
			evidenceComplete = false
		default:
			calculator.units.NetBytes = float64(netBytes)
		}
	}

	planSupported, planEvidenceComplete := calculateStatementRUPlan(
		flat.Main,
		0,
		runtimeStatsColl,
		&calculator,
	)
	if !planSupported {
		return statementRUFinalizedSnapshot{}, false
	}
	evidenceComplete = evidenceComplete && planEvidenceComplete
	return calculator.finalize(evidenceComplete), true
}

// calculateStatementRUPlan evaluates one subtree from a canonical
// preorder-serialized tree, visiting children before their parent. A parent
// that needs direct-child output as formula input can use each ChildrenIdx
// occurrence's Origin.ID() with runtimeStatsColl; that evidence is not the
// child's already-calculated RU and does not require a retained child-result
// graph. FlatPhysicalPlan owns the
// serialized layout; this calculation follows only its explicit child edges.
func calculateStatementRUPlan(
	tree plannercore.FlatPlanTree,
	operatorIndex int,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	calculator *statementRUCalculator,
) (supported bool, evidenceComplete bool) {
	if operatorIndex < 0 || operatorIndex >= len(tree) || calculator == nil {
		return false, false
	}
	operator := tree[operatorIndex]
	if operator == nil || operator.Origin == nil {
		return false, false
	}

	supported = true
	evidenceComplete = true
	for _, childIndex := range operator.ChildrenIdx {
		childSupported, childEvidenceComplete := calculateStatementRUPlan(
			tree,
			childIndex,
			runtimeStatsColl,
			calculator,
		)
		supported = supported && childSupported
		evidenceComplete = evidenceComplete && childEvidenceComplete
	}

	switch origin := operator.Origin.(type) {
	case *physicalop.PhysicalTableReader:
		if !operator.IsRoot || origin.StoreType != kv.TiKV || origin.ReadReqType != physicalop.Cop ||
			origin.TablePlan == nil || len(operator.ChildrenIdx) != 1 {
			return false, evidenceComplete
		}
		childIndex := operator.ChildrenIdx[0]
		if childIndex < 0 || childIndex >= len(tree) || tree[childIndex] == nil ||
			tree[childIndex].Origin != origin.TablePlan {
			return false, evidenceComplete
		}
		if runtimeStatsColl == nil {
			return supported, false
		}
		detail, found := runtimeStatsColl.GetCopScanDetail(origin.TablePlan.ID())
		if !found {
			return supported, false
		}
		scanBytes, state := statementRUScanBytes(
			detail.TotalKeys,
			detail.ProcessedKeys,
			detail.ProcessedKeysSize,
		)
		switch state {
		case statementRUCalibrationUnknown:
			calculator.invalidEvidence = true
		case statementRUCalibrationIncomplete:
			evidenceComplete = false
		case statementRUCalibrationComplete:
			calculator.units.ScanBytes += scanBytes
		}
	case *physicalop.PhysicalSelection:
		if operator.IsRoot || len(operator.ChildrenIdx) != 1 {
			return false, evidenceComplete
		}
	case *physicalop.PhysicalTableScan:
		if operator.IsRoot || len(operator.ChildrenIdx) != 0 {
			return false, evidenceComplete
		}
	default:
		return false, evidenceComplete
	}
	return supported, evidenceComplete
}
