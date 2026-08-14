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
// abort/terminal consumption for one ExecStmt. Its synchronous visitor is
// released when the shared finishOnce is consumed.
type statementRUPlanWalkOwner struct {
	finishOnce   sync.Once
	finalOutcome atomic.Uint32
	// Install-time snapshots reject transient restricted/cursor classifications
	// that may be restored before a delayed result-set terminal.
	restrictedSQLAtInstall bool
	cursorAtInstall        bool
	visit                  statementRUPlanVisitFunc
}

func newStatementRUPlanWalkOwner(stmt *ExecStmt, visit statementRUPlanVisitFunc) *statementRUPlanWalkOwner {
	owner := &statementRUPlanWalkOwner{visit: visit}
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
// statement-local RU plan walk. Stack 1 installs no production owner, so this
// is normally a nil-owner no-op. The first record wins and a recorded failure
// consumes the owner immediately; success must be recorded before the existing
// executor terminal can walk the plan.
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
// a full executor terminal solely for this dark layer.
func (a *ExecStmt) abortStatementRUPlanWalk() {
	owner := a.statementRUPlanWalkOwner
	if owner == nil {
		return
	}
	owner.finishOnce.Do(func() {
		owner.visit = nil
	})
}

func (a *ExecStmt) finishStatementRUPlanWalk(terminalErr error) {
	owner := a.statementRUPlanWalkOwner
	if owner == nil {
		return
	}

	owner.finishOnce.Do(func() {
		visit := owner.visit
		owner.visit = nil

		// The entire dark hook is fail-closed. A panic in eligibility, flat-plan
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
		if a.Ctx == nil || a.Plan == nil || visit == nil {
			return
		}

		sessVars := a.Ctx.GetSessionVars()
		// The snapshots catch eligibility that disappeared before terminal; the
		// live checks catch a classification entered after owner installation.
		if sessVars == nil || owner.restrictedSQLAtInstall || owner.cursorAtInstall ||
			sessVars.InRestrictedSQL || sessVars.HasStatusFlag(mysql.ServerStatusCursorExists) {
			return
		}

		// Ownership is intentionally deferred while stack 1 remains production-dark.
		// FinishExecuteStmt has just stored a.Plan in StmtCtx, but getFlatPlan may
		// return a cached flat plan from another plan generation because StmtCtx.plan
		// and StmtCtx.flatPlan are not bound or invalidated together. Its Origin
		// pointers therefore must not be treated as owned by a.Plan yet.
		//
		// ScalarSubQueries have a second unresolved ownership boundary. On a cache
		// miss FlattenPhysicalPlan reads them from the mutable SessionVars.MapScalarSubQ
		// registry instead of a.Plan; on a cache hit it reuses the scalar trees captured
		// when that cache was built. Neither path proves statement/plan-generation
		// ownership. A later RU stack must fix both contracts before production enablement.
		flat := getFlatPlan(sessVars.StmtCtx)
		if flat == nil {
			return
		}
		walkStatementRUFlatPlan(flat, visit)
	})
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
