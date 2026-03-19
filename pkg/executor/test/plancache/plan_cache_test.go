// Copyright 2025 PingCAP, Inc.
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

package plancache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPointGetPreparedPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists ps_text")
	defer tk.MustExec("drop database if exists ps_text")
	tk.MustExec("create database ps_text")
	tk.MustExec("use ps_text")

	tk.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk.MustExec("insert into t values (1, 1, 1)")
	tk.MustExec("insert into t values (2, 2, 2)")
	tk.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk.Session().PrepareStmt("select * from t where a = ?")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[pspk1Id].(*plannercore.PlanCacheStmt).StmtCacheable = false
	pspk2Id, _, _, err := tk.Session().PrepareStmt("select * from t where ? = a ")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[pspk2Id].(*plannercore.PlanCacheStmt).StmtCacheable = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	// unique index
	psuk1Id, _, _, err := tk.Session().PrepareStmt("select * from t where b = ? ")
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[psuk1Id].(*plannercore.PlanCacheStmt).StmtCacheable = false

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// test schema changed, cached plan should be invalidated
	tk.MustExec("alter table t add column col4 int default 10 after c")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	tk.MustExec("alter table t drop index k_b")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	tk.MustExec(`insert into t values(4, 3, 3, 11)`)
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10", "4 3 3 11"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	tk.MustExec("delete from t where a = 4")
	tk.MustExec("alter table t add index k_b(b)")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, psuk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// use pk again
	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk2Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(3))
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3 3 3 10"))
}

func TestPointGetPreparedPlanWithCommitMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("drop database if exists ps_text")
	defer tk1.MustExec("drop database if exists ps_text")
	tk1.MustExec("create database ps_text")
	tk1.MustExec("use ps_text")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk1.Session().PrepareStmt("select * from t where a = ?")
	require.NoError(t, err)
	tk1.Session().GetSessionVars().PreparedStmts[pspk1Id].(*plannercore.PlanCacheStmt).StmtCacheable = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// update rows
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use ps_text")
	tk2.MustExec("update t set c = c + 10 where c = 1")

	// try to point get again
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// try to update in session 1
	tk1.MustExec("update t set c = c + 10 where c = 1")
	err = tk1.ExecToErr("commit")
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("error: %s", err))

	// verify
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(1))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1 11"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(2))
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("2 2 2"))

	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 11"))
}

func TestPointUpdatePreparedPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists pu_test")
	defer tk.MustExec("drop database if exists pu_test")
	tk.MustExec("create database pu_test")
	tk.MustExec("use pu_test")

	tk.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk.MustExec("insert into t values (1, 1, 1)")
	tk.MustExec("insert into t values (2, 2, 2)")
	tk.MustExec("insert into t values (3, 3, 3)")

	updateID1, pc, _, err := tk.Session().PrepareStmt(`update t set c = c + 1 where a = ?`)
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[updateID1].(*plannercore.PlanCacheStmt).StmtCacheable = false
	require.Equal(t, 1, pc)
	updateID2, pc, _, err := tk.Session().PrepareStmt(`update t set c = c + 2 where ? = a`)
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[updateID2].(*plannercore.PlanCacheStmt).StmtCacheable = false
	require.Equal(t, 1, pc)

	ctx := context.Background()
	// first time plan generated
	rs, err := tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	// using the generated plan but with different params
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// updateID2
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID2, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 8"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID2, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	// unique index
	updUkID1, _, _, err := tk.Session().PrepareStmt(`update t set c = c + 10 where b = ?`)
	require.NoError(t, err)
	tk.Session().GetSessionVars().PreparedStmts[updUkID1].(*plannercore.PlanCacheStmt).StmtCacheable = false
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 20"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 30"))

	// test schema changed, cached plan should be invalidated
	tk.MustExec("alter table t add column col4 int default 10 after c")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 31 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 32 10"))

	tk.MustExec("alter table t drop index k_b")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 42 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 52 10"))

	tk.MustExec("alter table t add unique index k_b(b)")
	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 62 10"))

	rs, err = tk.Session().ExecutePreparedStmt(ctx, updUkID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 72 10"))

	tk.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1 10"))
	tk.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2 10"))
}

func TestPointUpdatePreparedPlanWithCommitMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("drop database if exists pu_test2")
	defer tk1.MustExec("drop database if exists pu_test2")
	tk1.MustExec("create database pu_test2")
	tk1.MustExec("use pu_test2")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	ctx := context.Background()
	updateID1, _, _, err := tk1.Session().PrepareStmt(`update t set c = c + 1 where a = ?`)
	tk1.Session().GetSessionVars().PreparedStmts[updateID1].(*plannercore.PlanCacheStmt).StmtCacheable = false
	require.NoError(t, err)

	// first time plan generated
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// update rows
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use pu_test2")
	tk2.MustExec(`prepare pu2 from "update t set c = c + 2 where ? = a "`)
	tk2.MustExec("set @p3 = 3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))
	tk2.MustExec("execute pu2 using @p3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 7"))
	tk2.MustExec("execute pu2 using @p3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))

	// try to update in session 1
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))
	err = tk1.ExecToErr("commit")
	require.True(t, kv.ErrWriteConflict.Equal(err), fmt.Sprintf("error: %s", err))

	// verify
	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2"))
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2"))
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))

	// again next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, updateID1, expression.Args2Expressions4Test(3))
	require.Nil(t, rs)
	require.NoError(t, err)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
	tk1.MustExec("commit")

	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
}

func TestPreparedPlanCachePlanSelectionRegressions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	testPreparedNullParam(t, tk)
	testIssue29850(t, tk)
	testIssue28064(t, tk)
	testIssue29101(t, tk)
	testIssue57528(t, tk)
}

func TestPreparedPlanCacheSessionInteractions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	testPreparePlanCache4Blacklist(t, tk)
	testPreparePlanCache4Function(t, tk)
	testPreparePlanCache4DifferentSystemVars(t, tk)
	testPreparePC4Binding(t, tk)
	testPrepareWorkWithForeignKey(t, tk)
	testPrepareProtocolWorkWithForeignKey(t, tk)
}

func TestPreparedPlanCacheClusterIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeOn
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("create table t1(a varchar(20), b varchar(20), c varchar(20), primary key(a, b))")
	tk.MustExec("insert into t1 values('1','1','111'),('2','2','222'),('3','3','333')")

	tk.MustExec(`prepare stmt1 from "select * from t1 where t1.a = ? and t1.b > ?"`)
	tk.MustExec("set @v1 = '1'")
	tk.MustExec("set @v2 = '0'")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Rows("1 1 111"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set @v1 = '2'")
	tk.MustExec("set @v2 = '1'")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Rows("2 2 222"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @v1 = '3'")
	tk.MustExec("set @v2 = '2'")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Rows("3 3 333"))
	attachSessionManagerForExplain(tk)
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID)).Rows()
	require.Equal(t, 0, strings.Index(rows[len(rows)-1][4].(string), `range:("3" "2","3" +inf]`))

	tk.MustExec(`prepare stmt2 from "select * from t1 where t1.a = ? and t1.b = ?"`)
	tk.MustExec("set @v1 = '1'")
	tk.MustExec("set @v2 = '1'")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Rows("1 1 111"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set @v1 = '2'")
	tk.MustExec("set @v2 = '2'")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Rows("2 2 222"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @v1 = '3'")
	tk.MustExec("set @v2 = '3'")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Rows("3 3 333"))
	attachSessionManagerForExplain(tk)
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID)).Rows()
	require.Equal(t, 0, strings.Index(rows[len(rows)-1][0].(string), `Point_Get`))

	tk.MustExec(`drop table if exists ta, tb`)
	tk.MustExec(`create table ta (a varchar(8) primary key, b int)`)
	tk.MustExec(`insert ta values ('a', 1), ('b', 2)`)
	tk.MustExec(`create table tb (a varchar(8) primary key, b int)`)
	tk.MustExec(`insert tb values ('a', 1), ('b', 2)`)
	tk.MustExec(`prepare stmt1 from "select * from ta, tb where ta.a = tb.a and ta.a = ?"`)
	tk.MustExec(`set @v1 = 'a', @v2 = 'b'`)
	tk.MustQuery(`execute stmt1 using @v1`).Check(testkit.Rows("a 1 a 1"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Rows("b 2 b 2"))

	tk.MustExec(`drop table if exists ta, tb`)
	tk.MustExec(`create table ta (a varchar(10) primary key, b int not null)`)
	tk.MustExec(`insert ta values ('a', 1), ('b', 2)`)
	tk.MustExec(`create table tb (b int primary key, c int)`)
	tk.MustExec(`insert tb values (1, 1), (2, 2)`)
	tk.MustExec(`prepare stmt1 from "select * from ta, tb where ta.b = tb.b and ta.a = ?"`)
	tk.MustExec(`set @v1 = 'a', @v2 = 'b'`)
	tk.MustQuery(`execute stmt1 using @v1`).Check(testkit.Rows("a 1 1 1"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Rows("b 2 2 2"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Rows("b 2 2 2"))
	attachSessionManagerForExplain(tk)
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID)).Rows()
	require.True(t, strings.Contains(rows[3][0].(string), `TableRangeScan`))

	tk.MustExec(`drop table if exists ta, tb`)
	tk.MustExec(`create table ta (a varchar(10), b varchar(10), c int, primary key (a, b))`)
	tk.MustExec(`insert ta values ('a', 'a', 1), ('b', 'b', 2), ('c', 'c', 3)`)
	tk.MustExec(`create table tb (b int primary key, c int)`)
	tk.MustExec(`insert tb values (1, 1), (2, 2), (3,3)`)
	tk.MustExec(`prepare stmt1 from "select * from ta, tb where ta.c = tb.b and ta.a = ? and ta.b = ?"`)
	tk.MustExec(`set @v1 = 'a', @v2 = 'b', @v3 = 'c'`)
	tk.MustQuery(`execute stmt1 using @v1, @v1`).Check(testkit.Rows("a a 1 1 1"))
	tk.MustQuery(`execute stmt1 using @v2, @v2`).Check(testkit.Rows("b b 2 2 2"))
	tk.MustExec(`prepare stmt2 from "select * from ta, tb where ta.c = tb.b and (ta.a, ta.b) in ((?, ?), (?, ?))"`)
	tk.MustQuery(`execute stmt2 using @v1, @v1, @v2, @v2`).Check(testkit.Rows("a a 1 1 1", "b b 2 2 2"))
	tk.MustQuery(`execute stmt2 using @v2, @v2, @v3, @v3`).Check(testkit.Rows("b b 2 2 2", "c c 3 3 3"))

	tk.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeOn
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int, b int, c int, primary key(a, b))`)
	tk.MustExec(`insert into t1 values(1,1,111),(2,2,222),(3,3,333)`)
	tk.MustExec(`prepare stmt1 from "select * from t1 where t1.a = ? and t1.b = ?"`)
	tk.MustExec(`set @v1=1, @v2=1`)
	tk.MustQuery(`execute stmt1 using @v1,@v2`).Check(testkit.Rows("1 1 111"))
	tk.MustExec(`set @v1=2, @v2=2`)
	tk.MustQuery(`execute stmt1 using @v1,@v2`).Check(testkit.Rows("2 2 222"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`prepare stmt2 from "select * from t1 where (t1.a,t1.b) in ((?,?),(?,?))"`)
	tk.MustExec(`set @v1=1, @v2=1, @v3=2, @v4=2`)
	tk.MustQuery(`execute stmt2 using @v1,@v2,@v3,@v4`).Check(testkit.Rows("1 1 111", "2 2 222"))
	tk.MustExec(`set @v1=2, @v2=2, @v3=3, @v4=3`)
	tk.MustQuery(`execute stmt2 using @v1,@v2,@v3,@v4`).Check(testkit.Rows("2 2 222", "3 3 333"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestPreparedPlanCacheOperators(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	type execCase struct {
		parameters []string
	}
	type prepCase struct {
		stmt  string
		cases []execCase
	}

	cases := []prepCase{
		{"use test", nil},
		{"create table t (a int, b int, key(a))", nil},
		{"insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,null)", nil},
		{"select * from t where a=?", []execCase{{[]string{"1"}}, {[]string{"2"}}, {[]string{"3"}}}},
		{"select * from t where a in (?,?,?)", []execCase{{[]string{"1", "1", "1"}}, {[]string{"2", "3", "4"}}}},
		{"select /*+ HASH_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t1.b>?", []execCase{{[]string{"1"}}, {[]string{"3"}}}},
		{"select * from t t1 where t1.b>? and t1.a > (select min(t2.a) from t t2 where t2.b < t1.b)", []execCase{{[]string{"1"}}, {[]string{"3"}}}},
		{"drop table t", nil},
		{"create table t (name varchar(50), y int, sale decimal(14,2))", nil},
		{"insert into t values ('Bob',2016,2.4), ('Bob',2017,3.2), ('Alice',2016,1.4), ('Alice',2017,2), ('John',2016,4), ('John',2017,2.1)", nil},
		{"select *, sum(sale) over (partition by y order by sale+? rows 2 preceding) total from t order by y", []execCase{{[]string{"0.1"}}, {[]string{"0.5"}}}},
		{"select *, first_value(sale) over (partition by y order by sale rows ? preceding) total from t order by y", []execCase{{[]string{"1"}}, {[]string{"2"}}}},
		{"drop table t", nil},
		{"create table t (a int)", nil},
		{"insert into t values (1), (1), (2), (2), (3), (4), (5), (6)", nil},
		{"select * from t limit ?", []execCase{{[]string{"20"}}, {[]string{"30"}}}},
		{"drop table t", nil},
		{"create table t (a int, b int)", nil},
		{"insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)", nil},
		{"select * from t order by b+?", []execCase{{[]string{"1"}}, {[]string{"2"}}}},
		{"select * from t order by b limit ?", []execCase{{[]string{"1"}}, {[]string{"2"}}}},
	}

	for _, prep := range cases {
		if !strings.Contains(prep.stmt, "select") {
			tk.MustExec(prep.stmt)
			continue
		}
		tk.MustExec(fmt.Sprintf(`prepare stmt from '%v'`, prep.stmt))
		for _, exec := range prep.cases {
			usingStmt := ""
			if len(exec.parameters) > 0 {
				setStmt := "set "
				usingStmt = "using "
				for i, parameter := range exec.parameters {
					if i > 0 {
						setStmt += ", "
						usingStmt += ", "
					}
					setStmt += fmt.Sprintf("@x%v=%v", i, parameter)
					usingStmt += fmt.Sprintf("@x%v", i)
				}
				tk.MustExec(setStmt)
			}

			result := tk.MustQuery("execute stmt " + usingStmt).Sort().Rows()
			parts := strings.Split(prep.stmt, "?")
			require.Equal(t, len(exec.parameters)+1, len(parts))
			query := ""
			for i := range parts {
				query += parts[i]
				if i < len(exec.parameters) {
					query += exec.parameters[i]
				}
			}
			tk.MustQuery(query).Sort().Check(result)
		}
	}
}

func attachSessionManagerForExplain(tk *testkit.TestKit) {
	tkProcess := tk.Session().ShowProcess()
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: []*sessmgr.ProcessInfo{tkProcess}})
}

func requireExplainContainsOperator(t *testing.T, rows [][]any, operator string) []any {
	t.Helper()
	for _, row := range rows {
		if len(row) > 0 && strings.Contains(fmt.Sprint(row[0]), operator) {
			return row
		}
	}
	require.Failf(t, "missing operator in explain", "expected explain for connection to contain operator %q, got rows: %v", operator, rows)
	return nil
}

func cleanupPreparedPlanSelectionState(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, customer, warehouse, t28064")
}

func testPreparedNullParam(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	cleanupPreparedPlanSelectionState(t, tk)

	for _, flag := range []bool{false, true} {
		tk.MustExec(fmt.Sprintf(`set tidb_enable_prepared_plan_cache=%v`, flag))
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_enable_collect_execution_info=0")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (id int not null, KEY id (id))")
		tk.MustExec("insert into t values (1), (2), (3)")
		tk.MustExec("prepare stmt from 'select * from t where id = ?'")
		tk.MustExec("set @a= null")
		tk.MustQuery("execute stmt using @a").Check(testkit.Rows())
		attachSessionManagerForExplain(tk)
		tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID)).Check(testkit.Rows(
			"TableDual_7 0.00 root  rows:0"))
	}
}

func testIssue29850(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	cleanupPreparedPlanSelectionState(t, tk)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`set tidb_enable_clustered_index=on`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE customer (
	  c_id int(11) NOT NULL,
	  c_d_id int(11) NOT NULL,
	  c_first varchar(16) DEFAULT NULL,
	  c_w_id int(11) NOT NULL,
	  c_last varchar(16) DEFAULT NULL,
	  c_credit char(2) DEFAULT NULL,
	  c_discount decimal(4,4) DEFAULT NULL,
	  PRIMARY KEY (c_w_id,c_d_id,c_id),
	  KEY idx_customer (c_w_id,c_d_id,c_last,c_first))`)
	tk.MustExec(`CREATE TABLE warehouse (
	  w_id int(11) NOT NULL,
	  w_tax decimal(4,4) DEFAULT NULL,
	  PRIMARY KEY (w_id))`)
	tk.MustExec(`prepare stmt from 'SELECT c_discount, c_last, c_credit, w_tax
		FROM customer, warehouse
		WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?'`)
	tk.MustExec(`set @w_id=1262`)
	tk.MustExec(`set @c_d_id=7`)
	tk.MustExec(`set @c_id=1549`)
	tk.MustQuery(`execute stmt using @w_id, @c_d_id, @c_id`).Check(testkit.Rows())
	attachSessionManagerForExplain(tk)
	tk.MustQuery(fmt.Sprintf("explain format='brief' for connection %d", tk.Session().ShowProcess().ID)).Check(testkit.Rows(
		`Projection 1.00 root  test.customer.c_discount, test.customer.c_last, test.customer.c_credit, test.warehouse.w_tax`,
		`└─MergeJoin 1.00 root  inner join, left key:test.customer.c_w_id, right key:test.warehouse.w_id`,
		`  ├─Point_Get(Build) 1.00 root table:warehouse handle:1262`,
		`  └─Point_Get(Probe) 1.00 root table:customer, clustered index:PRIMARY(c_w_id, c_d_id, c_id) `))
	tk.MustQuery(`execute stmt using @w_id, @c_d_id, @c_id`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`create table t (a int primary key)`)
	tk.MustExec(`insert into t values (1), (2)`)
	tk.MustExec(`prepare stmt from 'select * from t where a>=? and a<=?'`)
	tk.MustExec(`set @a1=1, @a2=2`)
	tk.MustQuery(`execute stmt using @a1, @a1`).Check(testkit.Rows("1"))
	attachSessionManagerForExplain(tk)
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID)).Check(testkit.Rows(
		`Point_Get_6 1.00 root table:t handle:1`))
	tk.MustQuery(`execute stmt using @a1, @a2`).Check(testkit.Rows("1", "2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustExec(`prepare stmt from 'select * from t where a=? or a=?'`)
	tk.MustQuery(`execute stmt using @a1, @a1`).Check(testkit.Rows("1"))
	attachSessionManagerForExplain(tk)
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID)).Check(testkit.Rows(
		`Point_Get_6 1.00 root table:t handle:1`))
	tk.MustQuery(`execute stmt using @a1, @a2`).Check(testkit.Rows("1", "2"))
}

func testIssue28064(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	cleanupPreparedPlanSelectionState(t, tk)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t28064")
	tk.MustExec("CREATE TABLE `t28064` (" +
		"`a` decimal(10,0) DEFAULT NULL," +
		"`b` decimal(10,0) DEFAULT NULL," +
		"`c` decimal(10,0) DEFAULT NULL," +
		"`d` decimal(10,0) DEFAULT NULL," +
		"KEY `iabc` (`a`,`b`,`c`));")
	tk.MustExec("set @a='123', @b='234', @c='345';")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt1 from 'select * from t28064 use index (iabc) where a = ? and b = ? and c = ?';")
	tk.MustExec("execute stmt1 using @a, @b, @c;")
	attachSessionManagerForExplain(tk)
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID))
	rows.Check(testkit.Rows(
		"IndexLookUp_8 1.25 root  ",
		"├─IndexRangeScan_6(Build) 1.25 cop[tikv] table:t28064, index:iabc(a, b, c) range:[123 234 345,123 234 345], keep order:false, stats:pseudo",
		"└─TableRowIDScan_7(Probe) 1.25 cop[tikv] table:t28064 keep order:false, stats:pseudo"))
	tk.MustExec("execute stmt1 using @a, @b, @c;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func testIssue29101(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	cleanupPreparedPlanSelectionState(t, tk)
	tk.MustExec("set @old_tidb_opt_advanced_join_hint := @@tidb_opt_advanced_join_hint")
	defer func() {
		tk.MustExec("set @@tidb_opt_advanced_join_hint = @old_tidb_opt_advanced_join_hint")
	}()

	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec(`use test`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec(`CREATE TABLE customer (
	  c_id int(11) NOT NULL,
	  c_d_id int(11) NOT NULL,
	  c_w_id int(11) NOT NULL,
	  c_first varchar(16) DEFAULT NULL,
	  c_last varchar(16) DEFAULT NULL,
	  c_credit char(2) DEFAULT NULL,
	  c_discount decimal(4,4) DEFAULT NULL,
	  PRIMARY KEY (c_w_id,c_d_id,c_id) NONCLUSTERED,
	  KEY idx_customer (c_w_id,c_d_id,c_last,c_first)
	)`)
	tk.MustExec(`CREATE TABLE warehouse (
	  w_id int(11) NOT NULL,
	  w_tax decimal(4,4) DEFAULT NULL,
	  PRIMARY KEY (w_id)
	)`)
	tk.MustExec(`prepare s1 from 'SELECT /*+ TIDB_INLJ(customer,warehouse) */ c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?'`)
	tk.MustExec(`set @a=936,@b=7,@c=158`)
	tk.MustQuery(`execute s1 using @a,@b,@c`).Check(testkit.Rows())
	attachSessionManagerForExplain(tk)
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID)).Check(testkit.Rows(
		`Projection_6 1.00 root  test.customer.c_discount, test.customer.c_last, test.customer.c_credit, test.warehouse.w_tax`,
		`└─IndexJoin_18 1.00 root  inner join, inner:IndexLookUp_34, outer key:test.warehouse.w_id, inner key:test.customer.c_w_id, equal cond:eq(test.warehouse.w_id, test.customer.c_w_id)`,
		`  ├─Point_Get_35(Build) 1.00 root table:warehouse handle:936`,
		`  └─IndexLookUp_34(Probe) 1.00 root  `,
		`    ├─Selection_33(Build) 1.00 cop[tikv]  eq(test.customer.c_w_id, 936)`,
		`    │ └─IndexRangeScan_31 1.00 cop[tikv] table:customer, index:PRIMARY(c_w_id, c_d_id, c_id) range: decided by [eq(test.customer.c_w_id, test.warehouse.w_id) eq(test.customer.c_d_id, 7) eq(test.customer.c_id, 158)], keep order:false, stats:pseudo`,
		`    └─TableRowIDScan_32(Probe) 1.00 cop[tikv] table:customer keep order:false, stats:pseudo`))
	tk.MustQuery(`execute s1 using @a,@b,@c`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func testIssue57528(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	cleanupPreparedPlanSelectionState(t, tk)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=1")
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE customer (
	  c_id int(11) NOT NULL,
	  c_discount int(11) DEFAULT NULL,
	  PRIMARY KEY (c_id))`)
	tk.MustExec(`insert into customer values (1, 2)`)
	tk.MustExec(`prepare stmt from 'SELECT c_discount FROM customer WHERE c_id = ?'`)
	tk.MustExec(`set @c_id=1`)
	tk.MustQuery(`execute stmt using @c_id`).Check(testkit.Rows("2"))
	tk.MustQuery(`execute stmt using @c_id`).Check(testkit.Rows("2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	defer func() {
		tk.MustExec("set @@tidb_enable_collect_execution_info=1")
	}()
	tk.MustQuery(`execute stmt using @c_id`).Check(testkit.Rows("2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func testPreparePlanCache4Blacklist(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	defer func() {
		tk.MustExec("DELETE FROM mysql.opt_rule_blacklist;")
		tk.MustExec("DELETE FROM mysql.expr_pushdown_blacklist;")
		tk.MustExec("ADMIN reload opt_rule_blacklist;")
		tk.MustExec("ADMIN reload expr_pushdown_blacklist;")
	}()

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'select min(a) from t;';")
	tk.MustExec("execute stmt;")
	attachSessionManagerForExplain(tk)
	res := tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID))
	requireExplainContainsOperator(t, res.Rows(), "TopN")
	tk.MustExec("INSERT INTO mysql.opt_rule_blacklist VALUES('max_min_eliminate');")
	tk.MustExec("ADMIN reload opt_rule_blacklist;")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'SELECT * FROM t WHERE a < 2 and a > 2;';")
	tk.MustExec("execute stmt;")
	attachSessionManagerForExplain(tk)
	res = tk.MustQuery(fmt.Sprintf("explain for connection %d", tk.Session().ShowProcess().ID))
	require.Equal(t, 3, len(res.Rows()))
	tk.MustExec("INSERT INTO mysql.expr_pushdown_blacklist VALUES('<','tikv','');")
	tk.MustExec("ADMIN reload expr_pushdown_blacklist;")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func testPreparePlanCache4Function(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt from 'select rand()';")
	res := tk.MustQuery("execute stmt;")
	res1 := tk.MustQuery("execute stmt;")
	require.Equal(t, 1, len(res.Rows()))
	require.Equal(t, 1, len(res1.Rows()))
	require.NotEqual(t, res.Rows()[0][0], res1.Rows()[0][0])
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'SELECT IFNULL(?,0);';")
	tk.MustExec("set @a = 1, @b = null;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func testPreparePlanCache4DifferentSystemVars(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("set @old_sql_select_limit := @@sql_select_limit, @old_tidb_enable_index_merge := @@tidb_enable_index_merge, @old_tidb_enable_collect_execution_info := @@tidb_enable_collect_execution_info, @old_tidb_enable_parallel_apply := @@tidb_enable_parallel_apply")
	defer func() {
		tk.MustExec("set @@sql_select_limit = @old_sql_select_limit, @@tidb_enable_index_merge = @old_tidb_enable_index_merge, @@tidb_enable_collect_execution_info = @old_tidb_enable_collect_execution_info, @@tidb_enable_parallel_apply = @old_tidb_enable_parallel_apply")
	}()

	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("set @@sql_select_limit = 1")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(0), (1), (null);")
	tk.MustExec("prepare stmt from 'select a from t order by a;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>"))
	tk.MustExec("set @@sql_select_limit = 2")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>", "0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("set @@sql_select_limit = 18446744073709551615")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>", "0", "1"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_enable_index_merge = 1;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b));")
	tk.MustExec("prepare stmt from 'select * from t use index(idx_a, idx_b) where a > 1 or b > 1;';")
	tk.MustExec("execute stmt;")
	attachSessionManagerForExplain(tk)
	res := tk.MustQuery("explain for connection " + strconv.FormatUint(tk.Session().ShowProcess().ID, 10))
	requireExplainContainsOperator(t, res.Rows(), "IndexMerge")
	tk.MustExec("set @@tidb_enable_index_merge = 0;")
	tk.MustExec("execute stmt;")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)")
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("prepare stmt from 'select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a);';")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.Session().SetProcessInfo("", time.Now(), mysql.ComSleep, 0)
	attachSessionManagerForExplain(tk)
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tk.Session().ShowProcess().ID, 10))
	applyRow := requireExplainContainsOperator(t, res.Rows(), "Apply")
	require.Contains(t, fmt.Sprint(applyRow), "Concurrency")
	tk.MustExec("set tidb_enable_parallel_apply=false")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.Session().SetProcessInfo("", time.Now(), mysql.ComSleep, 0)
	attachSessionManagerForExplain(tk)
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tk.Session().ShowProcess().ID, 10))
	applyRow = requireExplainContainsOperator(t, res.Rows(), "Apply")
	require.NotContains(t, fmt.Sprint(applyRow), "Concurrency")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func testPreparePC4Binding(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("prepare stmt from \"select * from t\"")
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("create binding for select * from t using select * from t")
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
}

func testPrepareWorkWithForeignKey(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, key(a))")
	tk.MustExec("create table t2(a int, key(a))")
	tk.MustExec("prepare stmt from 'insert into t2 values (0)'")
	tk.MustExec("execute stmt")
	tk.MustExec("delete from t2")
	tk.MustExec("execute stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("delete from t2")
	tk.MustExec("alter table t2 add constraint fk foreign key (a) references t1(a)")
	tk.MustContainErrMsg("execute stmt", "Cannot add or update a child row: a foreign key constraint fails")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func testPrepareProtocolWorkWithForeignKey(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, key(a))")
	tk.MustExec("create table t2(a int, key(a))")
	stmtID, _, _, err := tk.Session().PrepareStmt("insert into t2 values (0)")
	require.NoError(t, err)
	_, err = tk.Session().ExecutePreparedStmt(context.Background(), stmtID, nil)
	require.NoError(t, err)
	tk.MustExec("delete from t2")
	_, err = tk.Session().ExecutePreparedStmt(context.Background(), stmtID, nil)
	require.NoError(t, err)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("delete from t2")
	tk.MustExec("alter table t2 add constraint fk foreign key (a) references t1(a)")
	_, err = tk.Session().ExecutePreparedStmt(context.Background(), stmtID, nil)
	require.Contains(t, err.Error(), "Cannot add or update a child row: a foreign key constraint fails")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}
