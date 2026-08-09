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

package executor_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

const statementRUOwnerInstallFailpoint = "github.com/pingcap/tidb/pkg/executor/installStatementRUPlanWalkOwnerForTest"

type statementRUObservation struct {
	stmt   *executor.ExecStmt
	visits atomic.Int64
	scalar atomic.Int64
}

func installStatementRUObservation(stmt *executor.ExecStmt) *statementRUObservation {
	observation := &statementRUObservation{stmt: stmt}
	executor.InstallStatementRUPlanWalkOwnerForTest(stmt, func(
		treeKind string,
		_ int,
		_ int,
		_ *plannercore.FlatOperator,
	) {
		observation.visits.Add(1)
		if treeKind == "scalar" {
			observation.scalar.Add(1)
		}
	})
	return observation
}

func requireStatementRUTerminalFlatPlan(t *testing.T, stmt *executor.ExecStmt) *plannercore.FlatPhysicalPlan {
	t.Helper()
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Ctx)
	flat, ok := stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	require.True(t, ok)
	require.NotNil(t, flat)
	return flat
}

func countStatementRUFlatOccurrences(flat *plannercore.FlatPhysicalPlan) (total, scalar int) {
	if flat == nil {
		return 0, 0
	}
	countTree := func(tree plannercore.FlatPlanTree) int {
		count := 0
		for _, operator := range tree {
			if operator != nil && operator.Origin != nil {
				count++
			}
		}
		return count
	}
	total += countTree(flat.Main)
	for _, tree := range flat.CTEs {
		total += countTree(tree)
	}
	for _, tree := range flat.ScalarSubQueries {
		count := countTree(tree)
		total += count
		scalar += count
	}
	return total, scalar
}

func drainStatementRURecordSet(t *testing.T, rs sqlexec.RecordSet) error {
	t.Helper()
	chk := rs.NewChunk(nil)
	for {
		chk.Reset()
		if err := rs.Next(context.Background(), chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			return nil
		}
	}
}

func TestStatementRUResultSetTerminalOutcomes(t *testing.T) {
	t.Run("post-compile panic consumes owner", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})
		connID := tk.Session().GetSessionVars().ConnectionID
		testfailpoint.Enable(
			t,
			"github.com/pingcap/tidb/pkg/session/statementRUPostCompilePanicForTest",
			fmt.Sprintf("return(%d)", connID),
		)

		require.PanicsWithValue(t, "statement RU post-compile test panic", func() {
			_, _ = tk.Exec("select 1")
		})
		require.NotNil(t, observation)
		require.Zero(t, observation.visits.Load())

		observation.stmt.RecordStatementRUFinalOutcome(true)
		observation.stmt.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load(), "the post-compile panic must consume the owner")
	})

	t.Run("aborted transaction early return consumes owner", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int)")

		var observation *statementRUObservation
		var lockExpire *uint32
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
			lockExpire = &tk.Session().GetSessionVars().TxnCtx.LockExpire
			atomic.StoreUint32(lockExpire, 1)
		})
		t.Cleanup(func() {
			if lockExpire != nil {
				atomic.StoreUint32(lockExpire, 0)
			}
		})

		rs, err := tk.Exec("select * from t")
		require.Error(t, err)
		require.True(t, terror.ErrorEqual(err, kv.ErrLockExpire), "unexpected error: %v", err)
		require.Nil(t, rs)
		require.NotNil(t, observation)
		require.Zero(t, observation.visits.Load())

		observation.stmt.RecordStatementRUFinalOutcome(true)
		observation.stmt.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load(), "the aborted-transaction return must consume the owner")
	})

	t.Run("finishStmt error", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int primary key, v int)")
		tk.MustExec("insert into t values (1, 1)")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})

		rs, err := tk.Exec("select v from t where id = 1")
		require.NoError(t, err)
		require.NotNil(t, observation)
		require.NoError(t, drainStatementRURecordSet(t, rs))

		connID := tk.Session().GetSessionVars().ConnectionID
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/finishStmtError", fmt.Sprintf("return(%d)", connID))
		require.Error(t, rs.Close())
		require.Zero(t, observation.visits.Load())

		observation.stmt.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load(), "the failed first terminal must consume the owner")
	})

	t.Run("SQLKiller error reaches terminal", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int primary key, v int)")
		tk.MustExec("insert into t values (1, 1)")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})

		rs, err := tk.Exec("select v from t where id = 1")
		require.NoError(t, err)
		tk.Session().GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
		t.Cleanup(func() { tk.Session().GetSessionVars().SQLKiller.Reset() })

		require.Error(t, drainStatementRURecordSet(t, rs))
		require.NoError(t, rs.Close())
		require.Zero(t, observation.visits.Load())
	})

	t.Run("execution returns result set and error", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int primary key, v int)")
		tk.MustExec("insert into t values (1, 1)")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})
		connID := tk.Session().GetSessionVars().ConnectionID
		testfailpoint.Enable(
			t,
			"github.com/pingcap/tidb/pkg/session/statementRUResultSetErrorForTest",
			fmt.Sprintf("return(%d)", connID),
		)

		rs, err := tk.Exec("select v from t")
		require.Error(t, err)
		require.NotNil(t, rs)
		require.NotNil(t, observation)
		require.NoError(t, rs.Close())
		require.Zero(t, observation.visits.Load())

		observation.stmt.RecordStatementRUFinalOutcome(true)
		observation.stmt.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load(), "the first execution failure must consume the owner")
	})

	t.Run("successful close and repeated terminal", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int primary key, v int)")
		tk.MustExec("insert into t values (1, 1)")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})

		rs, err := tk.Exec("select v from t where id = 1")
		require.NoError(t, err)
		require.NoError(t, drainStatementRURecordSet(t, rs))
		finisher, ok := rs.(interface{ Finish() error })
		require.True(t, ok)
		require.NoError(t, finisher.Finish())
		require.NoError(t, finisher.Finish())
		require.Zero(t, observation.visits.Load(), "session Finish publishes outcome but does not run the executor terminal")
		require.NoError(t, rs.Close())
		firstVisits := observation.visits.Load()
		require.Positive(t, firstVisits)

		observation.stmt.FinishExecuteStmt(0, nil, false)
		require.Equal(t, firstVisits, observation.visits.Load())
	})
}

func TestStatementRUFileTransferOutcomeHandoff(t *testing.T) {
	t.Run("successful session outcome is consumed by delayed terminal", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})
		tk.Session().SetValue(executor.LoadStatsVarKey, struct{}{})
		t.Cleanup(func() {
			tk.Session().SetValue(executor.LoadStatsVarKey, nil)
			tk.Session().SetValue(session.ExecStmtVarKey, nil)
		})

		rs, err := tk.Exec("do 1")
		require.NoError(t, err)
		require.Nil(t, rs)
		require.Zero(t, observation.visits.Load())

		delayed, ok := tk.Session().Value(session.ExecStmtVarKey).(*executor.ExecStmt)
		require.True(t, ok)
		require.Same(t, observation.stmt, delayed)
		delayed.FinishExecuteStmt(0, nil, false)
		require.Positive(t, observation.visits.Load())
	})

	t.Run("post-run panic consumes owner before delayed terminal", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})
		tk.Session().SetValue(executor.LoadStatsVarKey, struct{}{})
		t.Cleanup(func() {
			tk.Session().SetValue(executor.LoadStatsVarKey, nil)
			tk.Session().SetValue(session.ExecStmtVarKey, nil)
		})
		connID := tk.Session().GetSessionVars().ConnectionID
		testfailpoint.Enable(
			t,
			"github.com/pingcap/tidb/pkg/session/statementRUFileTransferPostRunPanicForTest",
			fmt.Sprintf("return(%d)", connID),
		)

		require.PanicsWithValue(t, "statement RU file-transfer post-run test panic", func() {
			_, _ = tk.Exec("do 1")
		})
		require.NotNil(t, observation)
		require.Zero(t, observation.visits.Load())

		delayed, ok := tk.Session().Value(session.ExecStmtVarKey).(*executor.ExecStmt)
		require.True(t, ok)
		require.Same(t, observation.stmt, delayed)
		delayed.RecordStatementRUFinalOutcome(true)
		delayed.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load(), "the file-transfer post-run panic must consume the owner")
	})

	t.Run("finishStmt failure is RU-consumed without a legacy terminal", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})
		tk.Session().SetValue(executor.LoadStatsVarKey, struct{}{})
		t.Cleanup(func() {
			tk.Session().SetValue(executor.LoadStatsVarKey, nil)
			tk.Session().SetValue(session.ExecStmtVarKey, nil)
		})
		connID := tk.Session().GetSessionVars().ConnectionID
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/finishStmtError", fmt.Sprintf("return(%d)", connID))

		rs, err := tk.Exec("do 1")
		require.Error(t, err)
		require.Nil(t, rs)
		require.Zero(t, observation.visits.Load())

		delayed, ok := tk.Session().Value(session.ExecStmtVarKey).(*executor.ExecStmt)
		require.True(t, ok, "preserve the pre-existing file-transfer handoff on finishStmt error")
		require.Same(t, observation.stmt, delayed)
		delayed.RecordStatementRUFinalOutcome(true)
		delayed.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load(), "the failed outcome must consume the owner")
	})

	t.Run("stale handler does not publish result-set success", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int)")

		var observations []*statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observations = append(observations, installStatementRUObservation(stmt))
		})
		tk.Session().SetValue(executor.LoadStatsVarKey, struct{}{})
		t.Cleanup(func() {
			tk.Session().SetValue(executor.LoadStatsVarKey, nil)
			tk.Session().SetValue(session.ExecStmtVarKey, nil)
		})
		connID := tk.Session().GetSessionVars().ConnectionID
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/finishStmtError", fmt.Sprintf("return(%d)", connID))

		rs, err := tk.Exec("do 1")
		require.Error(t, err)
		require.Nil(t, rs)
		require.Len(t, observations, 1)
		require.Zero(t, observations[0].visits.Load())
		require.NotNil(t, tk.Session().Value(executor.LoadStatsVarKey), "the failed file transfer leaves its handler for the server path")

		rs, err = tk.Exec("select id from t")
		require.NoError(t, err)
		require.NotNil(t, rs)
		require.Len(t, observations, 2)
		resultSetObservation := observations[1]
		require.NoError(t, drainStatementRURecordSet(t, rs))
		require.Error(t, rs.Close())
		require.Zero(t, resultSetObservation.visits.Load(), "stale file-transfer state must not publish result-set success")

		resultSetObservation.stmt.RecordStatementRUFinalOutcome(true)
		resultSetObservation.stmt.FinishExecuteStmt(0, nil, false)
		require.Zero(t, resultSetObservation.visits.Load(), "the result-set failure must consume the owner")
	})
}

func TestStatementRUPointGetWalksTerminalFlatPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("create table t(id int primary key, v int)")
	tk.MustExec("insert into t values (1, 1)")

	var observation *statementRUObservation
	var sawPointGet atomic.Bool
	testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
		if stmt.Ctx != tk.Session() {
			return
		}
		observation = &statementRUObservation{stmt: stmt}
		executor.InstallStatementRUPlanWalkOwnerForTest(stmt, func(
			_ string,
			_ int,
			_ int,
			operator *plannercore.FlatOperator,
		) {
			observation.visits.Add(1)
			if operator.Origin == stmt.Plan {
				sawPointGet.Store(true)
			}
		})
	})

	rs, err := tk.Exec("select v from t where id = ?", 1)
	require.NoError(t, err)
	require.NotNil(t, observation)
	require.IsType(t, &physicalop.PointGetPlan{}, observation.stmt.Plan)
	require.NoError(t, drainStatementRURecordSet(t, rs))
	stmtCtx := observation.stmt.Ctx.GetSessionVars().StmtCtx
	// Make lookup order observable: an RU hook before FinishExecuteStmt's SetPlan
	// would see neither a plan nor a flat cache and could not visit the PointGet.
	stmtCtx.SetPlan(nil)
	stmtCtx.SetFlatPlan(nil)
	require.NoError(t, rs.Close())
	expected, _ := countStatementRUFlatOccurrences(requireStatementRUTerminalFlatPlan(t, observation.stmt))
	require.Positive(t, expected)
	require.Equal(t, int64(expected), observation.visits.Load())
	require.True(t, sawPointGet.Load(), "terminal lookup must visit the effective PointGet after SetPlan")
	// FinishExecuteStmt publishes the effective plan to StmtCtx before the RU
	// hook. This does not prove that an independently cached flat plan owns it.
	require.Same(t, observation.stmt.Plan, stmtCtx.GetPlan())

	t.Run("post-execution panic consumes owner", func(t *testing.T) {
		observation = nil
		connID := tk.Session().GetSessionVars().ConnectionID
		testfailpoint.Enable(
			t,
			"github.com/pingcap/tidb/pkg/session/statementRUPointGetPostExecPanicForTest",
			fmt.Sprintf("return(%d)", connID),
		)

		require.PanicsWithValue(t, "statement RU PointGet post-exec test panic", func() {
			_, _ = tk.Exec("select v from t where id = ?", 1)
		})
		require.NotNil(t, observation)
		require.Zero(t, observation.visits.Load())

		observation.stmt.RecordStatementRUFinalOutcome(true)
		observation.stmt.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load(), "the PointGet post-exec panic must consume the owner")
	})

	observation = nil
	connID := tk.Session().GetSessionVars().ConnectionID
	testfailpoint.Enable(
		t,
		"github.com/pingcap/tidb/pkg/executor/statementRUPointGetErrorForTest",
		fmt.Sprintf("return(%d)", connID),
	)
	rs, err = tk.Exec("select v from t where id = ?", 1)
	require.Error(t, err)
	require.Nil(t, rs)
	require.NotNil(t, observation)
	require.Zero(t, observation.visits.Load())

	observation.stmt.RecordStatementRUFinalOutcome(true)
	observation.stmt.FinishExecuteStmt(0, nil, false)
	require.Zero(t, observation.visits.Load(), "the PointGet failure must consume only the RU owner")
}

func TestStatementRUScalarSubQueries(t *testing.T) {
	t.Run("real scalar SQL", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_opt_enable_non_eval_scalar_subquery = 1")
		tk.MustExec("create table t1(a int)")
		tk.MustExec("create table t2(a int)")
		tk.MustExec("insert into t1 values (1)")
		tk.MustExec("insert into t2 values (1)")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})

		rs, err := tk.Exec("select * from t1 where a = (select a from t2 limit 1)")
		require.NoError(t, err)
		require.NoError(t, drainStatementRURecordSet(t, rs))
		require.NoError(t, rs.Close())
		expectedTotal, expectedScalar := countStatementRUFlatOccurrences(
			requireStatementRUTerminalFlatPlan(t, observation.stmt),
		)
		require.Positive(t, expectedScalar)
		require.Equal(t, int64(expectedTotal), observation.visits.Load())
		require.Equal(t, int64(expectedScalar), observation.scalar.Load())
	})

	t.Run("prepared execute and rebuild use terminal-returned trees", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_opt_enable_non_eval_scalar_subquery = 1")
		tk.MustExec("set @@tidb_enable_prepared_plan_cache = 1")
		tk.MustExec("create table t(a int)")
		tk.MustExec("insert into t values (1)")

		query := "select a from t where a = (select a from t where a = ?)"
		stmtID, _, _, err := tk.Session().PrepareStmt(query)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tk.Session().DropPreparedStmt(stmtID)) })

		var observationsMu sync.Mutex
		observations := make([]*statementRUObservation, 0, 3)
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation := installStatementRUObservation(stmt)
			observationsMu.Lock()
			observations = append(observations, observation)
			observationsMu.Unlock()
		})

		getObservation := func(index int) *statementRUObservation {
			observationsMu.Lock()
			defer observationsMu.Unlock()
			require.Greater(t, len(observations), index)
			return observations[index]
		}
		ctx := context.Background()
		for execution := range 2 {
			rs, err := tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(1))
			require.NoError(t, err)
			fromCache := tk.Session().GetSessionVars().FoundInPlanCache
			observation := getObservation(execution)
			require.NoError(t, drainStatementRURecordSet(t, rs))
			require.NoError(t, rs.Close())
			expectedTotal, expectedScalar := countStatementRUFlatOccurrences(
				requireStatementRUTerminalFlatPlan(t, observation.stmt),
			)
			require.Equal(t, int64(expectedTotal), observation.visits.Load())
			require.Equal(t, int64(expectedScalar), observation.scalar.Load())
			t.Logf(
				"prepared execution %d (plan cache hit: %t) returned %d scalar occurrences",
				execution+1,
				fromCache,
				expectedScalar,
			)
		}

		prepStmt, err := tk.Session().GetSessionVars().GetPreparedStmtByID(stmtID)
		require.NoError(t, err)
		executeAST := &ast.ExecuteStmt{
			PrepStmt:   prepStmt,
			BinaryArgs: expression.Args2Expressions4Test(1),
		}
		require.NoError(t, tk.Session().PrepareTxnCtx(ctx, nil))
		compiler := executor.Compiler{Ctx: tk.Session()}
		stmt, err := compiler.Compile(ctx, executeAST)
		require.NoError(t, err)
		observation := getObservation(2)
		require.Same(t, stmt, observation.stmt)
		require.NoError(t, tk.Session().PrepareTxnCtx(ctx, nil))
		_, err = stmt.RebuildPlan(ctx)
		require.NoError(t, err)
		rs, err := stmt.Exec(ctx)
		require.NoError(t, err)
		stmt.RecordStatementRUFinalOutcome(true)
		require.NoError(t, drainStatementRURecordSet(t, rs))
		require.NoError(t, rs.Close())
		expectedTotal, expectedScalar := countStatementRUFlatOccurrences(
			requireStatementRUTerminalFlatPlan(t, stmt),
		)
		require.Equal(t, int64(expectedTotal), observation.visits.Load())
		require.Equal(t, int64(expectedScalar), observation.scalar.Load())
		t.Logf("prepared RebuildPlan returned %d scalar occurrences", expectedScalar)
	})
}

func TestStatementRUCursorExclusion(t *testing.T) {
	t.Run("current-session restricted result set", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int)")
		tk.MustExec("insert into t values (1)")

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})

		restricted := tk.Session().GetRestrictedSQLExecutor()
		rows, _, err := restricted.ExecRestrictedSQL(
			kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers),
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
			"select * from t",
		)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, observation)
		require.Zero(t, observation.visits.Load())

		observation.stmt.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load(), "the snapshotted restricted classification must consume once")
	})

	t.Run("eager cursor terminal consumes skip", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int)")
		tk.MustExec("insert into t values (1)")
		tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
		t.Cleanup(func() {
			tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, false)
		})

		var observation *statementRUObservation
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			observation = installStatementRUObservation(stmt)
		})

		rs, err := tk.Exec("select * from t")
		require.NoError(t, err)
		require.NoError(t, drainStatementRURecordSet(t, rs))
		require.NoError(t, rs.Close())
		require.Zero(t, observation.visits.Load())

		observation.stmt.FinishExecuteStmt(0, nil, false)
		require.Zero(t, observation.visits.Load())
	})

	t.Run("lazy cursor test installer rejects owner", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t(id int)")
		tk.MustExec("insert into t values (1)")
		tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
		t.Cleanup(func() {
			tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, false)
		})

		var rejected atomic.Int64
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.Ctx != tk.Session() {
				return
			}
			if stmt.Ctx.GetSessionVars().HasStatusFlag(mysql.ServerStatusCursorExists) {
				rejected.Add(1)
				return
			}
			installStatementRUObservation(stmt)
		})

		rs, err := tk.Exec("select * from t")
		require.NoError(t, err)
		detachable, ok := rs.(sqlexec.DetachableRecordSet)
		require.True(t, ok)
		detached, ok, err := detachable.TryDetach()
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, detached.Close())
		require.Equal(t, int64(1), rejected.Load())
	})
}

func TestStatementRURetryAndReplay(t *testing.T) {
	t.Run("pessimistic retry has one terminal walk", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		writer := testkit.NewTestKit(t, store)
		writer.MustExec("use test")
		writer.MustExec("create table t(id int primary key, v int)")
		writer.MustExec("insert into t values (1, 10)")
		writer.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")

		retrying := testkit.NewSteppedTestKit(t, store)
		retrying.MustExec("use test")
		retrying.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		retrying.MustExec("set @@tidb_pessimistic_txn_fair_locking = 0")
		retryingConnectionID := retrying.MustQuery("select connection_id()").Rows()[0][0].(string)
		retrying.MustExec("set autocommit = 0")
		t.Cleanup(func() { retrying.MustExec("rollback") })

		query := "select * from t where id = 1 for update"
		var observedStmt atomic.Pointer[executor.ExecStmt]
		var visits atomic.Int64
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.OriginText() != query ||
				fmt.Sprint(stmt.Ctx.GetSessionVars().ConnectionID) != retryingConnectionID {
				return
			}
			observedStmt.Store(stmt)
			executor.InstallStatementRUPlanWalkOwnerForTest(stmt, func(string, int, int, *plannercore.FlatOperator) {
				visits.Add(1)
			})
		})

		retrying.SetBreakPoints(
			sessiontxn.BreakPointBeforeExecutorFirstRun,
			sessiontxn.BreakPointOnStmtRetryAfterLockError,
		)
		retrying.SteppedMustQuery(query).
			ExpectStopOnBreakPoint(sessiontxn.BreakPointBeforeExecutorFirstRun)
		writer.MustExec("update t set v = v + 1 where id = 1")
		retrying.Continue().ExpectStopOnBreakPoint(sessiontxn.BreakPointOnStmtRetryAfterLockError)
		retrying.Continue().ExpectIdle()

		stmt := observedStmt.Load()
		require.NotNil(t, stmt)
		expected, _ := countStatementRUFlatOccurrences(requireStatementRUTerminalFlatPlan(t, stmt))
		require.Positive(t, expected)
		require.Equal(t, int64(expected), visits.Load())
	})

	t.Run("optimistic replay is not a second terminal", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk1 := testkit.NewTestKit(t, store)
		tk2 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		tk2.MustExec("use test")
		tk1.MustExec("create table t(id int primary key, v int)")
		tk1.MustExec("insert into t values (1, 0)")
		tk1.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk1.MustExec("set @@tidb_retry_limit = 2")
		// The deprecated tidb_disable_txn_auto_retry sysvar now validates every
		// attempted OFF value back to ON. Force only this transaction's source
		// eligibility so the test exercises the real history replay loop.
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/sessiontxn/isolation/injectOptimisticTxnRetryable", "return(true)")

		query := "update t set v = v + 1 where id = 1"
		var observedStmt atomic.Pointer[executor.ExecStmt]
		var visits atomic.Int64
		testfailpoint.EnableCall(t, statementRUOwnerInstallFailpoint, func(stmt *executor.ExecStmt) {
			if stmt.OriginText() != query || stmt.Ctx != tk1.Session() {
				return
			}
			observedStmt.Store(stmt)
			executor.InstallStatementRUPlanWalkOwnerForTest(stmt, func(string, int, int, *plannercore.FlatOperator) {
				visits.Add(1)
			})
		})

		tk1.MustExec("begin optimistic")
		tk1.MustExec(query)
		stmt := observedStmt.Load()
		require.NotNil(t, stmt)
		expected, _ := countStatementRUFlatOccurrences(requireStatementRUTerminalFlatPlan(t, stmt))
		require.Positive(t, expected)
		require.Equal(t, int64(expected), visits.Load())
		visitsAfterOriginalTerminal := visits.Load()

		tk2.MustExec(query)
		tk1.MustExec("commit")
		require.Equal(t, visitsAfterOriginalTerminal, visits.Load())
		tk2.MustQuery("select v from t where id = 1").Check(testkit.Rows("2"))
	})
}
