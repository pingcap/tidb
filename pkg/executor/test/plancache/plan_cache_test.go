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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
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
