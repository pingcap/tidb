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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlmock "github.com/pingcap/tidb/pkg/util/topsql/collector/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

type stmtStatsTestContext struct {
	*mock.Context
	stmtStats *stmtstats.StatementStats
}

func (c *stmtStatsTestContext) GetStmtStats() *stmtstats.StatementStats {
	return c.stmtStats
}

func resetTopProfilingStateForTest(t *testing.T) {
	t.Helper()
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	t.Cleanup(func() {
		topsqlstate.DisableTopSQL()
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})
}

func newExecStmtWithStmtStatsForTest(goCtx context.Context, t *testing.T) (*ExecStmt, *stmtstats.StatementStats) {
	t.Helper()

	stats := stmtstats.CreateStatementStats()
	t.Cleanup(stats.SetFinished)

	sctx := mock.NewContext()
	sctx.GetSessionVars().User = &auth.UserIdentity{Username: "u1", Hostname: "%"}
	sc := sctx.GetSessionVars().StmtCtx
	sc.OriginalSQL = "select * from t where a = 1"
	_, sqlDigest := sc.SQLDigest()
	require.NotNil(t, sqlDigest)
	const normalizedPlan = "TableReader(table:t)->Selection(eq(test.t.a, ?))"
	planDigest := parser.NewDigest([]byte("topru-plan-digest"))
	sc.SetPlanDigest(normalizedPlan, planDigest)

	return &ExecStmt{
		Ctx: &stmtStatsTestContext{
			Context:   sctx,
			stmtStats: stats,
		},
		GoCtx: goCtx,
	}, stats
}

func ruKeyForStmt(t *testing.T, stmt *ExecStmt) stmtstats.RUKey {
	t.Helper()

	sqlDigest, planDigest := stmt.getSQLPlanDigest()
	require.NotNil(t, sqlDigest)
	require.NotNil(t, planDigest)
	return stmtstats.RUKey{
		User:       stmt.Ctx.GetSessionVars().User.String(),
		SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
		PlanDigest: stmtstats.BinaryDigest(planDigest),
	}
}

// TestObserveStmtBeginOnTopProfiling verifies observe stmt begin on top profiling and guards against regressions in begin-based RU accounting.
func TestObserveStmtBeginOnTopProfiling(t *testing.T) {
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.EnableTopRU()
	t.Cleanup(func() {
		topsqlstate.DisableTopSQL()
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})

	topCollector := topsqlmock.NewTopSQLCollector()
	topsql.SetupTopProfilingForTest(topCollector)

	sctx := mock.NewContext()
	sc := sctx.GetSessionVars().StmtCtx
	sc.OriginalSQL = "select * from t where a = 1"
	normalizedSQL, sqlDigest := sc.SQLDigest()
	require.NotNil(t, sqlDigest)
	const normalizedPlan = "TableReader(table:t)->Selection(eq(test.t.a, ?))"
	planDigest := parser.NewDigest([]byte("topru-plan-digest"))
	sc.SetPlanDigest(normalizedPlan, planDigest)

	stmt := &ExecStmt{
		Ctx:   sctx,
		GoCtx: context.Background(),
	}
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	require.Equal(t, normalizedSQL, topCollector.GetSQL(sqlDigest.Bytes()))
	require.Equal(t, normalizedPlan, topCollector.GetPlan(planDigest.Bytes()))
}

// TestObserveStmtFinishedOnTopProfiling verifies stale RU exec context is cleared
// before the first tick after re-enable.
// Flow: begin-on -> disable -> finish -> re-enable -> tick-before-new-begin
func TestObserveStmtFinishedOnTopProfiling(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	ru := util.NewRUDetailsWith(0, 0, 0)
	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, ru), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	topsqlstate.EnableTopRU()
	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)

	// Use a non-zero sentinel RU bump (5 is arbitrary) to prove stale execCtx
	// has been cleared; otherwise the next MergeRUInto would leak a positive delta.
	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	require.Len(t, stats.MergeRUInto(), 0)
}

// TestObserveStmtFinishedOnTopProfilingDoes verifies stale baseline is not reused
// across TopRU toggle windows.
// Flow: begin-on -> disable -> finish -> begin-off(same key) -> re-enable -> finish-on
func TestObserveStmtFinishedOnTopProfilingDoes(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	ruA := util.NewRUDetailsWith(0, 0, 0)
	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, ruA), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	ruA.Merge(util.NewRUDetailsWith(10, 0, 0))
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	// TopRU is still disabled here; begin-off must not create/reuse an RU execCtx.
	ruB := util.NewRUDetailsWith(20, 0, 0)
	stmt.GoCtx = context.WithValue(context.Background(), util.RUDetailsCtxKey, ruB)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	topsqlstate.EnableTopRU()
	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	// Expect only the begin-based execution count and no RU delta from stale baseline.
	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)
}

// TestObserveStmtFinishedOnTopProfilingKeeps verifies TopSQL-only finish stats are
// preserved when TopRU is disabled.
// Flow: begin(topSQL-on/topRU-off) -> finish -> verify duration stats
func TestObserveStmtFinishedOnTopProfilingKeeps(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopSQL()

	stmt, stats := newExecStmtWithStmtStatsForTest(context.Background(), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	vars := stmt.Ctx.GetSessionVars()
	vars.StartTime = time.Now().Add(-time.Second)

	stmt.observeStmtFinishedForTopProfiling()

	data := stats.Take()
	require.Len(t, data, 1)
	for _, item := range data {
		require.Equal(t, uint64(1), item.ExecCount)
		require.Equal(t, uint64(1), item.DurationCount)
		require.Greater(t, item.SumDurationNs, uint64(0))
	}
}

// TestObserveStmtFinishedOnTopProfilingIgnores verifies unexpected RUDetails context
// types do not panic and keep begin-based RU accounting stable.
// Flow: begin(topRU-on, bad RUDetails type) -> finish -> no panic -> begin-based RU stays stable
func TestObserveStmtFinishedOnTopProfilingIgnores(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, "bad-type"), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	require.NotPanics(t, func() {
		stmt.observeStmtFinishedForTopProfiling()
	})

	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)
}
