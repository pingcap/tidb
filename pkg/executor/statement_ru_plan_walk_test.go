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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func newStatementRUPlanForTest() (*ExecStmt, *atomic.Int64) {
	ctx := mock.NewContext()
	plan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
	ctx.GetSessionVars().StmtCtx.SetPlan(plan)
	visits := &atomic.Int64{}
	stmt := &ExecStmt{
		Ctx:   ctx,
		GoCtx: context.Background(),
		Plan:  plan,
	}
	stmt.statementRUPlanWalkOwner = newStatementRUPlanWalkVisitorOwner(
		stmt,
		func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator) {
			visits.Add(1)
		},
	)
	return stmt, visits
}

func statementRUPlanTreeKindForTest(kind statementRUPlanTreeKind) string {
	switch kind {
	case statementRUPlanTreeMain:
		return "main"
	case statementRUPlanTreeCTE:
		return "cte"
	case statementRUPlanTreeScalarSubQuery:
		return "scalar"
	default:
		return "unknown"
	}
}

type statementRUPanicOnceContext struct {
	context.Context
	panicked atomic.Bool
}

func (ctx *statementRUPanicOnceContext) Value(key any) any {
	if ctx.panicked.CompareAndSwap(false, true) {
		panic("statement RU recordSet panic test")
	}
	return ctx.Context.Value(key)
}

// InstallStatementRUPlanWalkOwnerForTest replaces any narrow production owner
// on an ExecStmt compiled by a test-only failpoint.
func InstallStatementRUPlanWalkOwnerForTest(
	stmt *ExecStmt,
	visit func(treeKind string, treeIndex int, operatorIndex int, operator *plannercore.FlatOperator),
) {
	stmt.statementRUPlanWalkOwner = newStatementRUPlanWalkVisitorOwner(
		stmt,
		func(kind statementRUPlanTreeKind, treeIndex, operatorIndex int, operator *plannercore.FlatOperator) {
			visit(statementRUPlanTreeKindForTest(kind), treeIndex, operatorIndex, operator)
		},
	)
}

func TestStatementRUPlanWalkOccurrences(t *testing.T) {
	ctx := mock.NewContext()
	duplicate := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
	duplicate.SetID(7)
	cte := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
	cte.SetID(7)
	scalar := &plannercore.ScalarSubqueryEvalCtx{}
	dml := &physicalop.Insert{}

	flat := &plannercore.FlatPhysicalPlan{
		Main: plannercore.FlatPlanTree{
			{Origin: dml},
			nil,
			{},
			{Origin: duplicate},
		},
		CTEs: []plannercore.FlatPlanTree{
			{{Origin: cte}},
			{{Origin: duplicate}},
		},
		ScalarSubQueries: []plannercore.FlatPlanTree{
			{{Origin: scalar}},
			{nil, {Origin: duplicate}},
		},
	}

	occurrences := make([]string, 0, 6)
	walkStatementRUFlatPlan(flat, func(kind statementRUPlanTreeKind, treeIndex, operatorIndex int, operator *plannercore.FlatOperator) {
		occurrences = append(occurrences, fmt.Sprintf(
			"%s/%d/%d/%T",
			statementRUPlanTreeKindForTest(kind),
			treeIndex,
			operatorIndex,
			operator.Origin,
		))
	})

	require.ElementsMatch(t, []string{
		"main/0/0/*physicalop.Insert",
		"main/0/3/*physicalop.PhysicalTableDual",
		"cte/0/0/*physicalop.PhysicalTableDual",
		"cte/1/0/*physicalop.PhysicalTableDual",
		"scalar/0/0/*core.ScalarSubqueryEvalCtx",
		"scalar/1/1/*physicalop.PhysicalTableDual",
	}, occurrences)
}

func TestStatementRUFinalOutcomeFirstRecordWins(t *testing.T) {
	t.Run("nil owner is off", func(t *testing.T) {
		stmt := &ExecStmt{}
		require.NotPanics(t, func() {
			stmt.RecordStatementRUFinalOutcome(true)
			stmt.finishStatementRUPlanWalk(nil)
		})
	})

	t.Run("unknown terminal consumes once", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.finishStatementRUPlanWalk(nil)
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("recorded failure consumes once", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(false)
		// Force the atomic state to success so this assertion specifically proves
		// failure consumed finishOnce, rather than only proving CAS first-wins.
		stmt.statementRUPlanWalkOwner.finalOutcome.Store(uint32(statementRUFinalOutcomeSuccess))
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	for _, tc := range []struct {
		name         string
		firstSuccess bool
		second       bool
		wantVisits   int64
	}{
		{name: "success then success", firstSuccess: true, second: true, wantVisits: 1},
		{name: "success then failure", firstSuccess: true, second: false, wantVisits: 1},
		{name: "failure then success", firstSuccess: false, second: true, wantVisits: 0},
		{name: "failure then failure", firstSuccess: false, second: false, wantVisits: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, visits := newStatementRUPlanForTest()
			stmt.RecordStatementRUFinalOutcome(tc.firstSuccess)
			stmt.RecordStatementRUFinalOutcome(tc.second)
			stmt.finishStatementRUPlanWalk(nil)
			require.Equal(t, tc.wantVisits, visits.Load())
		})
	}
}

func TestStatementRUPlanWalkTerminalFirstCallWins(t *testing.T) {
	t.Run("recordSet SQLKiller reaches terminal", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.Ctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
		t.Cleanup(func() { stmt.Ctx.GetSessionVars().SQLKiller.Reset() })
		rs := &recordSet{stmt: stmt}

		require.Error(t, rs.Next(context.Background(), nil))
		require.Empty(t, rs.lastErrs, "the RU-only abort must not change legacy terminal errors")
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("recordSet recovered panic reaches terminal", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		rs := &recordSet{stmt: stmt}
		ctx := &statementRUPanicOnceContext{Context: context.Background()}

		require.Error(t, rs.Next(ctx, nil))
		require.Empty(t, rs.lastErrs, "the RU-only abort must not change legacy terminal errors")
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("terminal error then success", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUPlanWalk(errors.New("terminal error"))
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("deadline then success", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUPlanWalk(context.DeadlineExceeded)
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("restricted then success", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.Ctx.GetSessionVars().InRestrictedSQL = true
		stmt.finishStatementRUPlanWalk(nil)
		stmt.Ctx.GetSessionVars().InRestrictedSQL = false
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("cursor then success", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.Ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
		stmt.finishStatementRUPlanWalk(nil)
		stmt.Ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, false)
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("nil plan then plan", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		plan := stmt.Plan
		stmt.Plan = nil
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUPlanWalk(nil)
		stmt.Plan = plan
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("empty plan then plan", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		plan := stmt.Plan
		stmt.Plan = &physicalop.Insert{}
		stmt.Ctx.GetSessionVars().StmtCtx.SetPlan(stmt.Plan)
		stmt.Ctx.GetSessionVars().StmtCtx.SetFlatPlan(nil)
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUPlanWalk(nil)
		stmt.Plan = plan
		stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("panic then retry", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.statementRUPlanWalkOwner.visit = func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator) {
			visits.Add(1)
			panic("statement RU visit panic")
		}
		require.NotPanics(t, func() { stmt.finishStatementRUPlanWalk(nil) })
		stmt.statementRUPlanWalkOwner.visit = func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator) {
			visits.Add(1)
		}
		stmt.finishStatementRUPlanWalk(nil)
		require.Equal(t, int64(1), visits.Load())
	})

	t.Run("success then terminal error", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUPlanWalk(nil)
		stmt.finishStatementRUPlanWalk(errors.New("late terminal error"))
		require.Equal(t, int64(1), visits.Load())
	})
}

func TestStatementRUPlanWalkUsesStmtCtxFlatPlanCache(t *testing.T) {
	ctx := mock.NewContext()
	stats := &property.StatsInfo{RowCount: 1}
	stalePlan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
	stalePlan.SetID(101)
	currentPlan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
	currentPlan.SetID(202)

	ctx.GetSessionVars().StmtCtx.SetPlan(currentPlan)
	ctx.GetSessionVars().StmtCtx.SetFlatPlan(plannercore.FlattenPhysicalPlan(stalePlan, false))

	var sawCurrent, sawStale bool
	stmt := &ExecStmt{
		Ctx:  ctx,
		Plan: currentPlan,
	}
	stmt.statementRUPlanWalkOwner = newStatementRUPlanWalkVisitorOwner(
		stmt,
		func(_ statementRUPlanTreeKind, _, _ int, operator *plannercore.FlatOperator) {
			sawCurrent = sawCurrent || operator.Origin == currentPlan
			sawStale = sawStale || operator.Origin == stalePlan
		},
	)
	stmt.RecordStatementRUFinalOutcome(true)
	stmt.finishStatementRUPlanWalk(nil)
	// This intentionally characterizes the current getFlatPlan contract. It does
	// not prove that the cached Origin belongs to the current ExecStmt generation.
	require.False(t, sawCurrent)
	require.True(t, sawStale)
}

func TestStatementRUPlanWalkConcurrentOwnerTerminal(t *testing.T) {
	stmt, visits := newStatementRUPlanForTest()
	stmt.RecordStatementRUFinalOutcome(true)

	const callers = 64
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			stmt.finishStatementRUPlanWalk(nil)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), visits.Load())
}
