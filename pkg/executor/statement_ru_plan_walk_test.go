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
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
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
	stmt.statementRUOwner = newStatementRUPlanWalkVisitorOwnerForTest(
		stmt,
		func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator) {
			visits.Add(1)
		},
	)
	return stmt, visits
}

func newStatementRUPlanWalkVisitorOwnerForTest(
	stmt *ExecStmt,
	visit statementRUPlanVisitFunc,
) *statementRUOwner {
	return newStatementRUOwner(stmt, visit)
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
	stmt.statementRUOwner = newStatementRUPlanWalkVisitorOwnerForTest(
		stmt,
		func(treeKind statementRUPlanTreeKind, treeIndex, operatorIndex int, operator *plannercore.FlatOperator) {
			visit(
				statementRUPlanTreeKindForTest(treeKind),
				treeIndex,
				operatorIndex,
				operator,
			)
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
			{Origin: dml, ChildrenIdx: []int{1}, ChildrenEndIdx: 1},
			{Origin: duplicate, ChildrenEndIdx: 1},
		},
		CTEs: []plannercore.FlatPlanTree{
			{{Origin: cte}},
			{{Origin: duplicate}},
		},
		ScalarSubQueries: []plannercore.FlatPlanTree{
			{{Origin: scalar}},
			{{Origin: duplicate}},
		},
	}

	occurrences := make([]string, 0, 6)
	walkOK := walkStatementRUFlatPlan(flat, func(
		walk statementRUFlatPlanWalk,
		operatorIndex int,
		operator *plannercore.FlatOperator,
		scanBytes float64,
	) statementRUCalculationVisitResult {
		occurrences = append(occurrences, fmt.Sprintf(
			"%s/%d/%d/%T",
			statementRUPlanTreeKindForTest(walk.treeKind),
			walk.treeIndex,
			operatorIndex,
			operator.Origin,
		))
		return statementRUCalculationVisitResult{scanBytes: scanBytes, ok: true}
	})

	require.True(t, walkOK)
	require.Equal(t, []string{
		"main/0/0/*physicalop.Insert",
		"main/0/1/*physicalop.PhysicalTableDual",
		"cte/0/0/*physicalop.PhysicalTableDual",
		"cte/1/0/*physicalop.PhysicalTableDual",
		"scalar/0/0/*core.ScalarSubqueryEvalCtx",
		"scalar/1/0/*physicalop.PhysicalTableDual",
	}, occurrences)

	occurrences = occurrences[:0]
	walkOK = walkStatementRUFlatPlan(flat, func(
		walk statementRUFlatPlanWalk,
		operatorIndex int,
		operator *plannercore.FlatOperator,
		scanBytes float64,
	) statementRUCalculationVisitResult {
		occurrences = append(occurrences, fmt.Sprintf(
			"%s/%d/%d/%T",
			statementRUPlanTreeKindForTest(walk.treeKind),
			walk.treeIndex,
			operatorIndex,
			operator.Origin,
		))
		return statementRUCalculationVisitResult{scanBytes: scanBytes, ok: operatorIndex == 0}
	})
	require.False(t, walkOK)
	require.Equal(t, []string{
		"main/0/0/*physicalop.Insert",
		"main/0/1/*physicalop.PhysicalTableDual",
		"cte/0/0/*physicalop.PhysicalTableDual",
		"cte/1/0/*physicalop.PhysicalTableDual",
		"scalar/0/0/*core.ScalarSubqueryEvalCtx",
		"scalar/1/0/*physicalop.PhysicalTableDual",
	}, occurrences)

	t.Run("visitor-owned child recursion skips generic recursion", func(t *testing.T) {
		branchFlat := &plannercore.FlatPhysicalPlan{Main: plannercore.FlatPlanTree{
			{Origin: dml, ChildrenIdx: []int{1, 2}, ChildrenEndIdx: 2},
			{Origin: duplicate, ChildrenEndIdx: 1},
			{Origin: duplicate, ChildrenEndIdx: 2},
		}}
		inputs := make([]float64, 0, 3)
		visit := func(
			walk statementRUFlatPlanWalk,
			operatorIndex int,
			operator *plannercore.FlatOperator,
			scanBytes float64,
		) statementRUCalculationVisitResult {
			inputs = append(inputs, scanBytes)
			if operatorIndex != 0 {
				return statementRUCalculationVisitResult{scanBytes: scanBytes, ok: true}
			}
			childScanBytes := [...]float64{10, 20}
			childrenOK := true
			for childOffset, childIndex := range operator.ChildrenIdx {
				childOK := walkStatementRUFlatPlanNode(walk, childIndex, childScanBytes[childOffset])
				childrenOK = childOK && childrenOK
			}
			return statementRUCalculationVisitResult{
				ok:           childrenOK,
				skipChildren: true,
			}
		}
		walkOK := walkStatementRUFlatPlan(branchFlat, visit)
		require.True(t, walkOK)
		require.Equal(t, []float64{0, 10, 20}, inputs)
	})

	t.Run("unsupported visitor-owned child still visits its siblings", func(t *testing.T) {
		branchFlat := &plannercore.FlatPhysicalPlan{Main: plannercore.FlatPlanTree{
			{Origin: dml, ChildrenIdx: []int{1, 2}, ChildrenEndIdx: 2},
			{Origin: duplicate, ChildrenEndIdx: 1},
			{Origin: duplicate, ChildrenEndIdx: 2},
		}}
		visited := make([]int, 0, 3)
		visit := func(
			walk statementRUFlatPlanWalk,
			operatorIndex int,
			operator *plannercore.FlatOperator,
			scanBytes float64,
		) statementRUCalculationVisitResult {
			visited = append(visited, operatorIndex)
			if operatorIndex != 0 {
				return statementRUCalculationVisitResult{
					scanBytes: scanBytes,
					ok:        operatorIndex == 2,
				}
			}
			childrenOK := true
			for _, childIndex := range operator.ChildrenIdx {
				childOK := walkStatementRUFlatPlanNode(walk, childIndex, scanBytes)
				childrenOK = childOK && childrenOK
			}
			return statementRUCalculationVisitResult{ok: childrenOK, skipChildren: true}
		}
		walkOK := walkStatementRUFlatPlan(branchFlat, visit)
		require.False(t, walkOK)
		require.Equal(t, []int{0, 1, 2}, visited)
	})
}

func TestStatementRUCalculationTraversal(t *testing.T) {
	t.Run("supported operator units are collected during the plan walk", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		// Statement-level ExecDetails is deliberately unrelated to the Reader's
		// own cop runtime stats and must not affect this calculation.
		fixture.mergeStatementScanDetail(&util.ScanDetail{
			TotalKeys:         100,
			ProcessedKeys:     100,
			ProcessedKeysSize: 10000,
		})
		var resultCount atomic.Int64
		var snapshot statementRUCalibrationSnapshot
		testfailpoint.EnableCall(t, statementRUResultFailpointForTest, func(uint64, float64) {
			resultCount.Add(1)
		})
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			snapshot = published
		})
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, int64(1), resultCount.Load())
		require.Equal(t, statementRURawUnits{
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		}, snapshot.Units)
	})

	t.Run("each Reader contributes its own cop scan aggregate", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		firstReader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		secondScan := physicalop.PhysicalTableScan{}.Init(planCtx, 0)
		secondReader := &physicalop.PhysicalTableReader{
			TablePlan: secondScan,
			StoreType: firstReader.StoreType,
		}
		fixture.recordReaderScanDetail(secondReader, 3, 1, 10)
		runtimeStatsColl := fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
		calculator := statementRUCalculator{
			evidenceComplete: true,
		}

		for _, reader := range []*physicalop.PhysicalTableReader{firstReader, secondReader} {
			flat := &plannercore.FlatPhysicalPlan{Main: plannercore.FlatPlanTree{
				{Origin: reader, ChildrenIdx: []int{1}, ChildrenEndIdx: 1},
				{Origin: reader.TablePlan, ChildrenEndIdx: 1},
			}}
			visit := func(
				walk statementRUFlatPlanWalk,
				_ int,
				operator *plannercore.FlatOperator,
				scanBytes float64,
			) statementRUCalculationVisitResult {
				return calculator.visitOperator(walk, operator, runtimeStatsColl, scanBytes)
			}
			require.True(t, walkStatementRUFlatPlan(flat, visit))
		}

		require.Equal(t, float64(40), calculator.units.ScanBytes)
	})

	t.Run("unsupported intermediate operator fails calculation", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		selection := physicalop.PhysicalSelection{}.Init(planCtx, &property.StatsInfo{RowCount: 1}, 0)
		selection.SetChildren(scan)
		reader.TablePlan = selection
		reader.TablePlans = physicalop.FlattenListPushDownPlan(selection)
		fixture.recordReaderScanDetail(reader, 1, 1, 10)
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.SetFlatPlan(plannercore.FlattenPhysicalPlan(reader, false))

		var resultCount, calibrationCount atomic.Int64
		testfailpoint.EnableCall(t, statementRUResultFailpointForTest, func(uint64, float64) {
			resultCount.Add(1)
		})
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Zero(t, resultCount.Load())
		require.Zero(t, calibrationCount.Load())
		require.Zero(t, fixture.owner.calculationSetup)
	})

}

func TestStatementRUFinalOutcomeFirstRecordWins(t *testing.T) {
	t.Run("nil owner is off", func(t *testing.T) {
		stmt := &ExecStmt{}
		require.NotPanics(t, func() {
			stmt.RecordStatementRUFinalOutcome(true)
			stmt.finishStatementRUForTest(nil)
		})
	})

	t.Run("unknown terminal consumes once", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.finishStatementRUForTest(nil)
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("recorded failure consumes once", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(false)
		// Force the atomic state to success so this assertion specifically proves
		// failure consumed finishOnce, rather than only proving CAS first-wins.
		stmt.statementRUOwner.finalOutcome.Store(uint32(statementRUFinalOutcomeSuccess))
		stmt.finishStatementRUForTest(nil)
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
			stmt.finishStatementRUForTest(nil)
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
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("recordSet recovered panic reaches terminal", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		rs := &recordSet{stmt: stmt}
		ctx := &statementRUPanicOnceContext{Context: context.Background()}

		require.Error(t, rs.Next(ctx, nil))
		require.Empty(t, rs.lastErrs, "the RU-only abort must not change legacy terminal errors")
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("terminal error then success", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(errors.New("terminal error"))
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("deadline then success", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(context.DeadlineExceeded)
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("restricted then success", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.Ctx.GetSessionVars().InRestrictedSQL = true
		stmt.finishStatementRUForTest(nil)
		stmt.Ctx.GetSessionVars().InRestrictedSQL = false
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("cursor then success", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.Ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
		stmt.finishStatementRUForTest(nil)
		stmt.Ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, false)
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("nil plan then plan", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		plan := stmt.Plan
		stmt.Plan = nil
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		stmt.Plan = plan
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("empty plan then plan", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		plan := stmt.Plan
		stmt.Plan = &physicalop.Insert{}
		stmt.Ctx.GetSessionVars().StmtCtx.SetPlan(stmt.Plan)
		stmt.Ctx.GetSessionVars().StmtCtx.SetFlatPlan(nil)
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		stmt.Plan = plan
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, visits.Load())
	})

	t.Run("panic then retry", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.statementRUOwner.visit = func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator) {
			visits.Add(1)
			panic("statement RU visit panic")
		}
		require.NotPanics(t, func() { stmt.finishStatementRUForTest(nil) })
		stmt.statementRUOwner.visit = func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator) {
			visits.Add(1)
		}
		stmt.finishStatementRUForTest(nil)
		require.Equal(t, int64(1), visits.Load())
	})

	t.Run("success then terminal error", func(t *testing.T) {
		stmt, visits := newStatementRUPlanForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		stmt.finishStatementRUForTest(errors.New("late terminal error"))
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
	stmt.statementRUOwner = newStatementRUPlanWalkVisitorOwnerForTest(
		stmt,
		func(_ statementRUPlanTreeKind, _, _ int, operator *plannercore.FlatOperator) {
			sawCurrent = sawCurrent || operator.Origin == currentPlan
			sawStale = sawStale || operator.Origin == stalePlan
		},
	)
	stmt.RecordStatementRUFinalOutcome(true)
	stmt.finishStatementRUForTest(nil)
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
			stmt.finishStatementRUForTest(nil)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), visits.Load())
}
