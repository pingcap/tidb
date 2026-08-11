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

// statementRUCalculationVisitResult controls the generic recursion after one
// operator is visited. scanBytes is inherited by every child when skipChildren
// is false. A Reader sets skipChildren only after it has recursively visited all
// of its own children with Reader-specific evidence. ok reports support without
// controlling whether the remaining plan is traversed.
type statementRUCalculationVisitResult struct {
	scanBytes    float64
	ok           bool
	skipChildren bool
}

type statementRUCalculationVisitFunc func(
	walk statementRUFlatPlanWalk,
	operatorIndex int,
	operator *plannercore.FlatOperator,
	scanBytes float64,
) statementRUCalculationVisitResult

// statementRUFlatPlanWalk is the synchronous state for one FlatPlanTree. It is
// exposed to the calculation visitor so a Reader can own recursion into its
// branches while reusing the same occurrence visitor and traversal invariants.
// It and the borrowed flat-plan nodes must not be retained after the walk.
type statementRUFlatPlanWalk struct {
	tree      plannercore.FlatPlanTree
	treeKind  statementRUPlanTreeKind
	treeIndex int
	visit     statementRUCalculationVisitFunc
}

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
func (a *ExecStmt) finishStatementRU(terminalErr error, _ execdetails.ExecDetails) {
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

		currentPlan := len(flat.Main) != 0 && flat.Main[0] != nil && flat.Main[0].Origin == a.Plan
		calculator := newStatementRUCalculator(calculationSetup, owner.rootEOF.Load())
		evidenceValid := true
		// Transport bytes are statement evidence in both the current producer and
		// the demo model. Read them once; do not attribute the same aggregate to
		// every TableReader occurrence.
		if currentPlan {
			if metrics := sessVars.RUV2Metrics; metrics != nil && !metrics.Bypass() {
				netBytes := metrics.TiKVCoprocessorResponseBytes()
				if netBytes < 0 {
					evidenceValid = false
				} else if netBytes == 0 {
					calculator.markEvidenceIncomplete()
				} else {
					calculator.units.NetBytes = float64(netBytes)
				}
			} else {
				calculator.markEvidenceIncomplete()
			}
		}

		// The fresh-session slice must use a flat plan rooted at this ExecStmt.
		// General flat-plan generation identity remains outside this layer.
		calculationVisit := func(
			walk statementRUFlatPlanWalk,
			operatorIndex int,
			operator *plannercore.FlatOperator,
			scanBytes float64,
		) statementRUCalculationVisitResult {
			if visit != nil {
				visit(walk.treeKind, walk.treeIndex, operatorIndex, operator)
			}
			if !currentPlan {
				return statementRUCalculationVisitResult{scanBytes: scanBytes, ok: true}
			}
			if !evidenceValid {
				return statementRUCalculationVisitResult{scanBytes: scanBytes}
			}
			return calculator.visitOperator(
				walk,
				operator,
				sessVars.StmtCtx.RuntimeStatsColl,
				scanBytes,
			)
		}
		walkOK := walkStatementRUFlatPlan(flat, calculationVisit)
		if !currentPlan || !evidenceValid || !walkOK {
			return
		}
		finalized = calculator.finalize()
		publishFinalized = true
	})
	if publishFinalized {
		publishStatementRUFinalizedSnapshot(a, finalized)
	}
}

func (calculator *statementRUCalculator) visitOperator(
	walk statementRUFlatPlanWalk,
	operator *plannercore.FlatOperator,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	scanBytes float64,
) statementRUCalculationVisitResult {
	if walk.treeKind != statementRUPlanTreeMain {
		return statementRUCalculationVisitResult{scanBytes: scanBytes}
	}

	switch origin := operator.Origin.(type) {
	case *physicalop.PhysicalTableReader:
		return calculator.visitTableReader(walk, operator, origin, runtimeStatsColl)
	case *physicalop.PhysicalTableScan:
		if scanBytes <= 0 {
			calculator.markEvidenceIncomplete()
			return statementRUCalculationVisitResult{scanBytes: scanBytes, ok: true}
		}
		calculator.units.ScanBytes += scanBytes
		return statementRUCalculationVisitResult{scanBytes: scanBytes, ok: true}
	default:
		// The current model defines units only for TableReader and TableScan.
		// Any other operator makes the whole statement RU calculation unsupported.
		return statementRUCalculationVisitResult{scanBytes: scanBytes}
	}
}

func (calculator *statementRUCalculator) visitTableReader(
	walk statementRUFlatPlanWalk,
	operator *plannercore.FlatOperator,
	reader *physicalop.PhysicalTableReader,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
) statementRUCalculationVisitResult {
	// DistSQL records a Reader's aggregate scan detail against the root of its
	// pushed-down TablePlan. It is the TableScan only for the direct topology.
	// Calculate the Reader-scoped scalar here, then recurse here as well so each
	// Reader type can eventually route distinct evidence to each of its branches.
	scanBytes := float64(0)
	readerOK := true
	switch {
	case reader.TablePlan == nil:
		readerOK = false
	case runtimeStatsColl == nil:
		calculator.markEvidenceIncomplete()
	default:
		detail, found := runtimeStatsColl.GetCopScanDetail(reader.TablePlan.ID())
		if !found {
			calculator.markEvidenceIncomplete()
			break
		}
		state := statementRUCalibrationUnknown
		scanBytes, state = statementRUScanBytes(
			detail.TotalKeys,
			detail.ProcessedKeys,
			detail.ProcessedKeysSize,
		)
		if state == statementRUCalibrationUnknown {
			readerOK = false
		} else if state != statementRUCalibrationComplete {
			calculator.markEvidenceIncomplete()
		}
	}

	childrenOK := true
	for _, childIndex := range operator.ChildrenIdx {
		childOK := walkStatementRUFlatPlanNode(walk, childIndex, scanBytes)
		childrenOK = childOK && childrenOK
	}
	return statementRUCalculationVisitResult{
		ok:           readerOK && childrenOK,
		skipChildren: true,
	}
}

// walkStatementRUFlatPlan follows the canonical ChildrenIdx edges in depth-first
// order. A visitor can pass scan bytes from a Reader to its descendants. A
// false result makes the whole walk unsuccessful without
// suppressing observation of the remaining occurrences; no traversal state is
// retained after the synchronous walk.
func walkStatementRUFlatPlan(
	flat *plannercore.FlatPhysicalPlan,
	visit statementRUCalculationVisitFunc,
) bool {
	if flat == nil || visit == nil {
		return false
	}
	walkTree := func(tree plannercore.FlatPlanTree, treeKind statementRUPlanTreeKind, treeIndex int) bool {
		if len(tree) == 0 {
			return true
		}
		walk := statementRUFlatPlanWalk{
			tree:      tree,
			treeKind:  treeKind,
			treeIndex: treeIndex,
			visit:     visit,
		}
		return walkStatementRUFlatPlanNode(walk, 0, 0)
	}

	walkOK := walkTree(flat.Main, statementRUPlanTreeMain, 0)
	for treeIndex, tree := range flat.CTEs {
		treeOK := walkTree(tree, statementRUPlanTreeCTE, treeIndex)
		walkOK = treeOK && walkOK
	}
	for treeIndex, tree := range flat.ScalarSubQueries {
		treeOK := walkTree(tree, statementRUPlanTreeScalarSubQuery, treeIndex)
		walkOK = treeOK && walkOK
	}
	return walkOK
}

// walkStatementRUFlatPlanNode visits one operator occurrence and normally
// follows its canonical ChildrenIdx edges with the returned scanBytes. A
// visitor may set skipChildren only after recursively visiting all children
// itself. Unsupported occurrences make the result false without short-circuiting
// the remaining siblings.
func walkStatementRUFlatPlanNode(
	walk statementRUFlatPlanWalk,
	operatorIndex int,
	scanBytes float64,
) bool {
	operator := walk.tree[operatorIndex]
	if operator == nil || operator.Origin == nil {
		return false
	}
	result := walk.visit(walk, operatorIndex, operator, scanBytes)
	if result.skipChildren {
		return result.ok
	}
	for _, childIndex := range operator.ChildrenIdx {
		childOK := walkStatementRUFlatPlanNode(walk, childIndex, result.scanBytes)
		result.ok = childOK && result.ok
	}
	return result.ok
}
