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

type statementRUResultTermination uint32

const (
	statementRUResultTerminationUnknown statementRUResultTermination = iota
	statementRUResultTerminationEOF
	statementRUResultTerminationEarlyClose
)

// statementRUPlanVisitFunc is synchronous. Its FlatOperator and Origin are
// borrowed from the getFlatPlan result and must not be retained after the call.
// That cached result is not yet bound to the current ExecStmt plan generation.
type statementRUPlanVisitFunc func(
	treeKind statementRUPlanTreeKind,
	treeIndex int,
	operatorIndex int,
	operator *plannercore.FlatOperator,
)

// statementRUPlanWalkOwner owns the first-record-wins outcome and the first
// abort/terminal consumption for one ExecStmt. Its synchronous visitor or
// production run is released when the shared finishOnce is consumed.
type statementRUPlanWalkOwner struct {
	finishOnce        sync.Once
	finalOutcome      atomic.Uint32
	rootEOF           atomic.Bool
	resultTermination atomic.Uint32
	// Install-time snapshots reject transient restricted/cursor classifications
	// that may be restored before a delayed result-set terminal.
	restrictedSQLAtInstall bool
	cursorAtInstall        bool
	visit                  statementRUPlanVisitFunc
	hasSimpleSelectRun     bool
	simpleSelectRun        statementRUSimpleSelectRun
}

func newStatementRUPlanWalkOwner(stmt *ExecStmt) *statementRUPlanWalkOwner {
	owner := &statementRUPlanWalkOwner{}
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

func newStatementRUPlanWalkVisitorOwner(
	stmt *ExecStmt,
	visit statementRUPlanVisitFunc,
) *statementRUPlanWalkOwner {
	owner := newStatementRUPlanWalkOwner(stmt)
	owner.visit = visit
	return owner
}

func newStatementRUSimpleSelectPlanWalkOwner(
	stmt *ExecStmt,
	run statementRUSimpleSelectRun,
) *statementRUPlanWalkOwner {
	owner := newStatementRUPlanWalkOwner(stmt)
	owner.hasSimpleSelectRun = true
	owner.simpleSelectRun = run
	return owner
}

// RecordStatementRUFinalOutcome publishes the final session outcome for the
// statement-local RU plan walk. It remains a nil-owner no-op outside the narrow
// production simple-SELECT slice and tests. The first record wins and a
// recorded failure consumes the owner immediately; success must be recorded
// before the existing executor terminal can walk the plan.
func (a *ExecStmt) RecordStatementRUFinalOutcome(success bool) {
	owner := a.statementRUPlanWalkOwner
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
		a.abortStatementRUPlanWalk()
	}
}

// abortStatementRUPlanWalk consumes the owner without invoking its visitor.
// RU-only failure paths use it instead of mutating legacy lastErrs or running
// a full executor terminal solely for this RU layer.
func (a *ExecStmt) abortStatementRUPlanWalk() {
	owner := a.statementRUPlanWalkOwner
	if owner == nil {
		return
	}
	owner.finishOnce.Do(func() {
		owner.visit = nil
		owner.hasSimpleSelectRun = false
		owner.simpleSelectRun = statementRUSimpleSelectRun{}
	})
}

// recordStatementRURootEOF records the sufficient EOF signal exposed by the
// exact supported root TableReader. The terminal calculator still requires the
// complete two-occurrence shape and single-response evidence before using it.
func (a *ExecStmt) recordStatementRURootEOF() {
	owner := a.statementRUPlanWalkOwner
	if owner == nil {
		return
	}
	owner.rootEOF.Store(true)
}

// sealStatementRUResultTermination records whether the public result was
// drained to an empty root chunk before recordSet.Finish closed its executor.
// A successful executor/session completion alone is insufficient: the caller
// may close a clean RecordSet before observing EOF, for example after a client
// connection fails while rows are being written. This is only a completeness
// signal for the new statement-RU pipeline; it does not change existing
// RUv1/RUv2 finalization or reporting.
func (a *ExecStmt) sealStatementRUResultTermination() {
	owner := a.statementRUPlanWalkOwner
	if owner == nil {
		return
	}
	termination := statementRUResultTerminationEarlyClose
	if owner.rootEOF.Load() {
		termination = statementRUResultTerminationEOF
	}
	owner.resultTermination.CompareAndSwap(
		uint32(statementRUResultTerminationUnknown),
		uint32(termination),
	)
}

func (a *ExecStmt) finishStatementRUPlanWalk(terminalErr error) {
	a.finishStatementRUPlanWalkWithExecDetails(terminalErr, nil)
}

func (a *ExecStmt) finishStatementRUPlanWalkWithExecDetails(
	terminalErr error,
	execDetails *statementRUTerminalExecDetailsView,
) {
	owner := a.statementRUPlanWalkOwner
	if owner == nil {
		return
	}

	won := false
	var publication statementRUSimpleSelectPublication
	owner.finishOnce.Do(func() {
		won = true
		visit := owner.visit
		var run *statementRUSimpleSelectRun
		if owner.hasSimpleSelectRun {
			run = &owner.simpleSelectRun
		}
		owner.visit = nil

		// The entire hook is fail-closed. A panic in eligibility, flat-plan
		// lookup/generation, framework traversal, or the visitor must neither make the
		// owner retryable nor interrupt existing terminal bookkeeping.
		defer func() {
			if recover() != nil {
				publication = statementRUSimpleSelectPublicationAfterPanic(run)
			}
			owner.hasSimpleSelectRun = false
			owner.simpleSelectRun = statementRUSimpleSelectRun{}
		}()

		if statementRUFinalOutcome(owner.finalOutcome.Load()) != statementRUFinalOutcomeSuccess || terminalErr != nil {
			return
		}
		// a.Plan remains the statement eligibility guard even though the flat-plan
		// view below comes from StatementContext.
		if a.Ctx == nil || a.Plan == nil || (visit == nil && run == nil) {
			return
		}

		sessVars := a.Ctx.GetSessionVars()
		// The snapshots catch eligibility that disappeared before terminal; the
		// live checks catch a classification entered after owner installation.
		if sessVars == nil || owner.restrictedSQLAtInstall || owner.cursorAtInstall ||
			sessVars.InRestrictedSQL || sessVars.HasStatusFlag(mysql.ServerStatusCursorExists) {
			return
		}
		if visit != nil && run != nil {
			return
		}
		if run != nil && (a.PsStmt != nil || a.isPreparedStmt || a.isSelectForUpdate || a.retryCount != 0) {
			return
		}

		// FinishExecuteStmt has just stored a.Plan in StmtCtx, but getFlatPlan may
		// return a cached flat plan from another plan generation because StmtCtx.plan
		// and StmtCtx.flatPlan are not bound or invalidated together. The production
		// installer therefore accepts only a fresh StmtCtx with no preloaded flat
		// cache, and the calculator requires the Main root Origin to equal a.Plan.
		// This proves only the minimal fresh simple-SELECT case; it does not solve
		// general plan-generation ownership.
		//
		// ScalarSubQueries have a second unresolved ownership boundary. On a cache
		// miss FlattenPhysicalPlan reads them from the mutable SessionVars.MapScalarSubQ
		// registry instead of a.Plan; on a cache hit it reuses the scalar trees captured
		// when that cache was built. Neither path proves statement/plan-generation
		// ownership. A later RU stack must fix both contracts before expanding the
		// eligibility boundary.
		flat := getFlatPlan(sessVars.StmtCtx)
		if run != nil {
			publication = run.freeze(a, owner, flat, execDetails)
			return
		}
		if flat == nil {
			return
		}
		walkStatementRUFlatPlan(flat, visit)
	})
	if won {
		publication.publish()
	}
}

func walkStatementRUFlatPlan(flat *plannercore.FlatPhysicalPlan, visit statementRUPlanVisitFunc) {
	if flat == nil || visit == nil {
		return
	}

	walkStatementRUFlatPlanTree(flat.Main, statementRUPlanTreeMain, 0, visit)
	for treeIndex, tree := range flat.CTEs {
		walkStatementRUFlatPlanTree(tree, statementRUPlanTreeCTE, treeIndex, visit)
	}
	for treeIndex, tree := range flat.ScalarSubQueries {
		walkStatementRUFlatPlanTree(tree, statementRUPlanTreeScalarSubQuery, treeIndex, visit)
	}
}

func walkStatementRUFlatPlanTree(
	tree plannercore.FlatPlanTree,
	treeKind statementRUPlanTreeKind,
	treeIndex int,
	visit statementRUPlanVisitFunc,
) {
	for operatorIndex, operator := range tree {
		if operator == nil || operator.Origin == nil {
			continue
		}
		visit(treeKind, treeIndex, operatorIndex, operator)
	}
}
