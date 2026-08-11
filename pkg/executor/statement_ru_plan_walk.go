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

type statementRUPlanTreeKind uint8

const (
	statementRUPlanTreeMain statementRUPlanTreeKind = iota
	statementRUPlanTreeCTE
	statementRUPlanTreeScalarSubQuery
)

// statementRUPlanVisitFunc is synchronous. OperatorIndex is only a coordinate
// for decoding ChildrenIdx edges in this borrowed tree; it is not operator
// identity. Operator and Origin must not be retained after the callback. The
// cached flat plan is not yet bound to the current ExecStmt generation.
type statementRUPlanVisitFunc func(
	treeKind statementRUPlanTreeKind,
	treeIndex int,
	operatorIndex int,
	operator *plannercore.FlatOperator,
)

// statementRUOwner owns the first-record-wins outcome and the first terminal
// finalization for one ExecStmt. Its optional plan observer and calculation
// setup are released when the shared finishOnce is consumed.
type statementRUOwner struct {
	finishOnce   sync.Once
	finalOutcome atomic.Uint32
	rootEOF      atomic.Bool
	// Install-time snapshots reject transient restricted/cursor classifications
	// that may be restored before a delayed result-set terminal.
	restrictedSQLAtInstall bool
	cursorAtInstall        bool
	visit                  statementRUPlanVisitFunc
	calculationSetup       statementRUCalculationSetup
}

func newStatementRUOwner(stmt *ExecStmt, visit statementRUPlanVisitFunc) *statementRUOwner {
	owner := &statementRUOwner{visit: visit}
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

// abortStatementRU consumes the owner without invoking its visitor.
// RU-only failure paths use it instead of mutating legacy lastErrs or running
// a full executor terminal solely for this RU layer.
func (a *ExecStmt) abortStatementRU() {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}
	owner.finishOnce.Do(func() {
		owner.visit = nil
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
func (a *ExecStmt) finishStatementRU(terminalErr error, execDetail execdetails.ExecDetails) {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}

	var finalized statementRUFinalizedSnapshot
	publishFinalized := false
	owner.finishOnce.Do(func() {
		visit := owner.visit
		calculationSetup := owner.calculationSetup
		owner.visit = nil
		owner.calculationSetup = statementRUCalculationSetup{}

		// The entire hook is fail-closed. A panic in eligibility, flat-plan
		// lookup/generation, framework traversal, or the visitor must neither make the
		// owner retryable nor interrupt existing terminal bookkeeping.
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

		calculator := newStatementRUCalculator(calculationSetup)
		// Transport bytes are statement evidence in both the current producer and
		// the demo model. Read them once; do not attribute the same aggregate to
		// every TableReader occurrence.
		evidenceComplete := owner.rootEOF.Load()
		if metrics := sessVars.RUV2Metrics; metrics != nil && !metrics.Bypass() {
			netBytes := metrics.TiKVCoprocessorResponseBytes()
			if netBytes < 0 {
				calculator.invalidEvidence = true
			} else if netBytes == 0 {
				evidenceComplete = false
			} else {
				calculator.units.NetBytes = float64(netBytes)
			}
		} else {
			evidenceComplete = false
		}
		// The fresh-session slice must use a flat plan rooted at this ExecStmt.
		// General flat-plan generation identity remains outside this layer.
		currentPlan := len(flat.Main) != 0 && flat.Main[0] != nil && flat.Main[0].Origin == a.Plan
		walkStatementRUFlatPlan(flat, func(
			treeKind statementRUPlanTreeKind,
			treeIndex int,
			operatorIndex int,
			operator *plannercore.FlatOperator,
			scanBytes float64,
		) float64 {
			if visit != nil {
				visit(treeKind, treeIndex, operatorIndex, operator)
			}
			if !currentPlan {
				return scanBytes
			}
			if treeKind != statementRUPlanTreeMain {
				// CTE and scalar-subquery ownership are intentionally outside this
				// layer. The recursive framework still visits their occurrences.
				evidenceComplete = false
				return scanBytes
			}

			switch operator.Origin.(type) {
			case *physicalop.PhysicalTableReader:
				// Calculate the Reader's statement scan aggregate here, then pass only
				// the scalar result to its one TablePlan child.
				detail := execDetail.ScanDetail
				if detail == nil {
					evidenceComplete = false
					return 0
				}
				scanBytes, state := statementRUScanBytes(
					atomic.LoadInt64(&detail.TotalKeys),
					atomic.LoadInt64(&detail.ProcessedKeys),
					atomic.LoadInt64(&detail.ProcessedKeysSize),
				)
				if state == statementRUCalibrationUnknown {
					calculator.invalidEvidence = true
					return 0
				}
				if state != statementRUCalibrationComplete {
					evidenceComplete = false
				}
				return scanBytes
			case *physicalop.PhysicalTableScan:
				if scanBytes <= 0 {
					evidenceComplete = false
					return scanBytes
				}
				calculator.units.ScanBytes += scanBytes
				return scanBytes
			default:
				// The current model defines units only for TableReader and
				// TableScan. Later operators extend this switch without changing
				// the traversal or calculator lifecycle.
				evidenceComplete = false
				return scanBytes
			}
		})
		if !currentPlan {
			return
		}
		finalized = calculator.finalize(evidenceComplete && calculator.units.ScanBytes > 0)
		publishFinalized = true
	})
	if publishFinalized {
		publishStatementRUFinalizedSnapshot(a, finalized)
	}
}

// walkStatementRUFlatPlan follows the canonical ChildrenIdx edges in depth-first
// order. A visitor can calculate scan bytes at a Reader and pass that scalar to
// its descendants; no traversal state is retained after the synchronous walk.
func walkStatementRUFlatPlan(
	flat *plannercore.FlatPhysicalPlan,
	visit func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator, float64) float64,
) {
	if flat == nil || visit == nil {
		return
	}
	walkTree := func(tree plannercore.FlatPlanTree, treeKind statementRUPlanTreeKind, treeIndex int) {
		if len(tree) == 0 {
			return
		}
		walkStatementRUFlatPlanNode(tree, treeKind, treeIndex, 0, 0, visit)
	}

	walkTree(flat.Main, statementRUPlanTreeMain, 0)
	for treeIndex, tree := range flat.CTEs {
		walkTree(tree, statementRUPlanTreeCTE, treeIndex)
	}
	for treeIndex, tree := range flat.ScalarSubQueries {
		walkTree(tree, statementRUPlanTreeScalarSubQuery, treeIndex)
	}
}

func walkStatementRUFlatPlanNode(
	tree plannercore.FlatPlanTree,
	treeKind statementRUPlanTreeKind,
	treeIndex int,
	operatorIndex int,
	scanBytes float64,
	visit func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator, float64) float64,
) {
	operator := tree[operatorIndex]
	if operator == nil || operator.Origin == nil {
		return
	}
	scanBytes = visit(treeKind, treeIndex, operatorIndex, operator, scanBytes)
	for _, childIndex := range operator.ChildrenIdx {
		walkStatementRUFlatPlanNode(tree, treeKind, treeIndex, childIndex, scanBytes, visit)
	}
}
