// Copyright 2016 PingCAP, Inc.
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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPrepared(t *testing.T) {
	store := testkit.CreateMockStore(t)
	flags := []bool{false, true}
	ctx := context.Background()
	for _, flag := range flags {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(fmt.Sprintf(`set @@tidb_enable_prepared_plan_cache=%v`, flag))
		var err error

		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
		tk.MustExec("insert prepare_test (c1) values (1),(2),(NULL)")

		tk.MustExec(`prepare stmt_test_1 from 'select id from prepare_test where id > ?';`)
		tk.MustExec(`set @a = 1;`)
		tk.MustExec(`execute stmt_test_1 using @a;`)
		tk.MustExec(`prepare stmt_test_2 from 'select 1'`)
		// Prepare multiple statement is not allowed.
		tk.MustGetErrCode(`prepare stmt_test_3 from 'select id from prepare_test where id > ?;select id from prepare_test where id > ?;'`, errno.ErrPrepareMulti)

		// The variable count does not match.
		tk.MustExec(`prepare stmt_test_4 from 'select id from prepare_test where id > ? and id < ?';`)
		tk.MustExec(`set @a = 1;`)
		tk.MustGetErrCode(`execute stmt_test_4 using @a;`, errno.ErrWrongParamCount)
		// Prepare and deallocate prepared statement immediately.
		tk.MustExec(`prepare stmt_test_5 from 'select id from prepare_test where id > ?';`)
		tk.MustExec(`deallocate prepare stmt_test_5;`)

		// Statement not found.
		err = tk.ExecToErr("deallocate prepare stmt_test_5")
		require.True(t, plannererrors.ErrStmtNotFound.Equal(err))

		// incorrect SQLs in prepare. issue #3738, SQL in prepare stmt is parsed in DoPrepare.
		tk.MustGetErrMsg(`prepare p from "delete from t where a = 7 or 1=1/*' and b = 'p'";`,
			`[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use near '/*' and b = 'p'' at line 1`)

		// The `stmt_test5` should not be found.
		err = tk.ExecToErr(`set @a = 1; execute stmt_test_5 using @a;`)
		require.True(t, plannererrors.ErrStmtNotFound.Equal(err))

		// Use parameter marker with argument will run prepared statement.
		result := tk.MustQuery("select distinct c1, c2 from prepare_test where c1 = ?", 1)
		result.Check(testkit.Rows("1 <nil>"))

		// Call Session PrepareStmt directly to get stmtID.
		query := "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err := tk.Session().PrepareStmt(query)
		require.NoError(t, err)
		rs, err := tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 <nil>"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Session().PrepareStmt(query)
		require.NoError(t, err)

		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec(`set @@tidb_enable_prepared_plan_cache=true`)
		tk1.MustExec("use test")
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(3))
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Session().PrepareStmt(query)
		require.NoError(t, err)
		rs, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(3))
		require.NoError(t, err)
		require.NoError(t, rs.Close())
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(3))
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 in (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Session().PrepareStmt(query)
		require.NoError(t, err)
		rs, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(3))
		require.NoError(t, err)
		require.NoError(t, rs.Close())
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(3))
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("3"))

		tk.MustExec("begin")
		tk.MustExec("insert prepare_test (c1) values (4)")
		query = "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err = tk.Session().PrepareStmt(query)
		require.NoError(t, err)
		tk.MustExec("rollback")
		rs, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(4))
		require.NoError(t, err)
		tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows())

		prepStmt, err := tk.Session().GetSessionVars().GetPreparedStmtByID(stmtID)
		require.NoError(t, err)
		execStmt := &ast.ExecuteStmt{PrepStmt: prepStmt, BinaryArgs: expression.Args2Expressions4Test(1)}
		// Check that ast.Statement created by compiler.Compile has query text.
		compiler := executor.Compiler{Ctx: tk.Session()}
		stmt, err := compiler.Compile(context.TODO(), execStmt)
		require.NoError(t, err)

		// Check that rebuild plan works.
		err = tk.Session().PrepareTxnCtx(ctx)
		require.NoError(t, err)
		_, err = stmt.RebuildPlan(ctx)
		require.NoError(t, err)
		rs, err = stmt.Exec(ctx)
		require.NoError(t, err)
		req := rs.NewChunk(nil)
		err = rs.Next(ctx, req)
		require.NoError(t, err)
		require.NoError(t, rs.Close())

		// Make schema change.
		tk.MustExec("drop table if exists prepare2")
		tk.MustExec("create table prepare2 (a int)")

		// Should success as the changed schema do not affect the prepared statement.
		rs, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		if rs != nil {
			require.NoError(t, rs.Close())
		}

		// Drop a column so the prepared statement become invalid.
		query = "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err = tk.Session().PrepareStmt(query)
		require.NoError(t, err)
		tk.MustExec("alter table prepare_test drop column c2")

		_, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(1))
		require.True(t, plannererrors.ErrUnknownColumn.Equal(err))

		tk.MustExec("drop table prepare_test")
		_, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(1))
		require.True(t, plannererrors.ErrSchemaChanged.Equal(err))

		// issue 3381
		tk.MustExec("drop table if exists prepare3")
		tk.MustExec("create table prepare3 (a decimal(1))")
		tk.MustExec("prepare stmt from 'insert into prepare3 value(123)'")
		tk.MustExecToErr("execute stmt")

		_, _, fields, err := tk.Session().PrepareStmt("select a from prepare3")
		require.NoError(t, err)
		require.Equal(t, "test", fields[0].DBName.L)
		require.Equal(t, "prepare3", fields[0].TableAsName.L)
		require.Equal(t, "a", fields[0].ColumnAsName.L)

		_, _, fields, err = tk.Session().PrepareStmt("select a from prepare3 where ?")
		require.NoError(t, err)
		require.Equal(t, "test", fields[0].DBName.L)
		require.Equal(t, "prepare3", fields[0].TableAsName.L)
		require.Equal(t, "a", fields[0].ColumnAsName.L)

		_, _, fields, err = tk.Session().PrepareStmt("select (1,1) in (select 1,1)")
		require.NoError(t, err)
		require.Equal(t, "", fields[0].DBName.L)
		require.Equal(t, "", fields[0].TableAsName.L)
		require.Equal(t, "(1,1) in (select 1,1)", fields[0].ColumnAsName.L)

		_, _, fields, err = tk.Session().PrepareStmt("select a from prepare3 where a = (" +
			"select a from prepare2 where a = ?)")
		require.NoError(t, err)
		require.Equal(t, "test", fields[0].DBName.L)
		require.Equal(t, "prepare3", fields[0].TableAsName.L)
		require.Equal(t, "a", fields[0].ColumnAsName.L)

		_, _, fields, err = tk.Session().PrepareStmt("select * from prepare3 as t1 join prepare3 as t2")
		require.NoError(t, err)
		require.Equal(t, "test", fields[0].DBName.L)
		require.Equal(t, "t1", fields[0].TableAsName.L)
		require.Equal(t, "a", fields[0].ColumnAsName.L)
		require.Equal(t, "test", fields[1].DBName.L)
		require.Equal(t, "t2", fields[1].TableAsName.L)
		require.Equal(t, "a", fields[1].ColumnAsName.L)

		_, _, fields, err = tk.Session().PrepareStmt("update prepare3 set a = ?")
		require.NoError(t, err)
		require.Len(t, fields, 0)

		// issue 8074
		tk.MustExec("drop table if exists prepare1;")
		tk.MustExec("create table prepare1 (a decimal(1))")
		tk.MustExec("insert into prepare1 values(1);")
		tk.MustGetErrMsg("prepare stmt FROM @sql1",
			"[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 4 near \"NULL\" ")
		tk.MustExec("SET @sql = 'update prepare1 set a=5 where a=?';")
		tk.MustExec("prepare stmt FROM @sql")
		tk.MustExec("set @var=1;")
		tk.MustExec("execute stmt using @var")
		tk.MustQuery("select a from prepare1;").Check(testkit.Rows("5"))

		// issue 19371
		tk.MustExec("SET @sql = 'update prepare1 set a=a+1';")
		tk.MustExec("prepare stmt FROM @SQL")
		tk.MustExec("execute stmt")
		tk.MustQuery("select a from prepare1;").Check(testkit.Rows("6"))
		tk.MustExec("prepare stmt FROM @Sql")
		tk.MustExec("execute stmt")
		tk.MustQuery("select a from prepare1;").Check(testkit.Rows("7"))

		// Coverage.
		exec := &executor.ExecuteExec{}
		err = exec.Next(ctx, nil)
		require.NoError(t, err)
		err = exec.Close()
		require.NoError(t, err)

		// issue 8065
		stmtID, _, _, err = tk.Session().PrepareStmt("select ? from dual")
		require.NoError(t, err)
		_, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		stmtID, _, _, err = tk.Session().PrepareStmt("update prepare1 set a = ? where a = ?")
		require.NoError(t, err)
		_, err = tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(1, 1))
		require.NoError(t, err)
	}
}

func TestPreparedLimitOffset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	flags := []bool{false, true}
	ctx := context.Background()
	for _, flag := range flags {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(fmt.Sprintf(`set @@tidb_enable_prepared_plan_cache=%v`, flag))

		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
		tk.MustExec("insert prepare_test (c1) values (1),(2),(NULL)")
		tk.MustExec(`prepare stmt_test_1 from 'select id from prepare_test limit ? offset ?'; set @a = 1, @b=1;`)
		r := tk.MustQuery(`execute stmt_test_1 using @a, @b;`)
		r.Check(testkit.Rows("2"))

		tk.MustExec(`set @a=1.1`)
		_, err := tk.Exec(`execute stmt_test_1 using @a, @b;`)
		require.True(t, plannererrors.ErrWrongArguments.Equal(err))

		tk.MustExec(`set @c="-1"`)
		_, err = tk.Exec("execute stmt_test_1 using @c, @c")
		require.True(t, plannererrors.ErrWrongArguments.Equal(err))

		stmtID, _, _, err := tk.Session().PrepareStmt("select id from prepare_test limit ?")
		require.NoError(t, err)
		rs, err := tk.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test(1))
		require.NoError(t, err)
		rs.Close()
	}
}

func TestPrepareWithAggregation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	flags := []bool{false, true}
	for _, flag := range flags {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(fmt.Sprintf(`set @@tidb_enable_prepared_plan_cache=%v`, flag))

		se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
			PreparedPlanCache: plannercore.NewLRUPlanCache(100, 0.1, math.MaxUint64, tk.Session(), false),
		})
		require.NoError(t, err)
		tk.SetSession(se)

		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (id int primary key)")
		tk.MustExec("insert into t values (1), (2), (3)")
		tk.MustExec(`prepare stmt from 'select sum(id) from t where id = ?'`)

		tk.MustExec(`set @id="1"`)
		r := tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Rows("1"))

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Rows("1"))
	}
}

func TestPreparedInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	metrics.ResettablePlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(fmt.Sprintf(`set @@tidb_enable_prepared_plan_cache=%v`, flag))
		var err error

		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`prepare stmt_insert from 'insert into prepare_test values (?, ?)'`)
		tk.MustExec(`set @a=1,@b=1; execute stmt_insert using @a, @b;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(0), hit)
		}
		tk.MustExec(`set @a=2,@b=2; execute stmt_insert using @a, @b;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(1), hit)
		}
		tk.MustExec(`set @a=3,@b=3; execute stmt_insert using @a, @b;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(2), hit)
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(testkit.Rows("1 1"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(testkit.Rows("2 2"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(testkit.Rows("3 3"))

		tk.MustExec(`prepare stmt_insert_select from 'insert into prepare_test (id, c1) select id + 100, c1 + 100 from prepare_test where id = ?'`)
		tk.MustExec(`set @a=1; execute stmt_insert_select using @a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(2), hit)
		}
		tk.MustExec(`set @a=2; execute stmt_insert_select using @a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(3), hit)
		}
		tk.MustExec(`set @a=3; execute stmt_insert_select using @a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(4), hit)
		}

		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 101)
		result.Check(testkit.Rows("101 101"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 102)
		result.Check(testkit.Rows("102 102"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 103)
		result.Check(testkit.Rows("103 103"))
	}
}

func TestPreparedUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	metrics.ResettablePlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=0`) // affect hit counter in this UT.
		tk.MustExec(fmt.Sprintf(`set @@tidb_enable_prepared_plan_cache=%v`, flag))
		var err error

		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`insert into prepare_test values (1, 1)`)
		tk.MustExec(`insert into prepare_test values (2, 2)`)
		tk.MustExec(`insert into prepare_test values (3, 3)`)

		tk.MustExec(`prepare stmt_update from 'update prepare_test set c1 = c1 + ? where id = ?'`)
		tk.MustExec(`set @a=1,@b=100; execute stmt_update using @b,@a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(0), hit)
		}
		tk.MustExec(`set @a=2,@b=200; execute stmt_update using @b,@a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(1), hit)
		}
		tk.MustExec(`set @a=3,@b=300; execute stmt_update using @b,@a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(2), hit)
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(testkit.Rows("1 101"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(testkit.Rows("2 202"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(testkit.Rows("3 303"))
	}
}

func TestIssue21884(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=false`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists prepare_test")
	tk.MustExec("create table prepare_test(a bigint primary key, status bigint, last_update_time datetime)")
	tk.MustExec("insert into prepare_test values (100, 0, '2020-12-18 20:00:00')")
	tk.MustExec("prepare stmt from 'update prepare_test set status = ?, last_update_time = now() where a = 100'")
	tk.MustExec("set @status = 1")
	tk.MustExec("execute stmt using @status")
	updateTime := tk.MustQuery("select last_update_time from prepare_test").Rows()[0][0]
	// Sleep 1 second to make sure `last_update_time` is updated.
	time.Sleep(1 * time.Second)
	tk.MustExec("execute stmt using @status")
	newUpdateTime := tk.MustQuery("select last_update_time from prepare_test").Rows()[0][0]
	require.NotEqual(t, newUpdateTime, updateTime)
}

func TestPreparedDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	metrics.ResettablePlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=0`) // affect hit counter in this UT.
		tk.MustExec(fmt.Sprintf(`set @@tidb_enable_prepared_plan_cache=%v`, flag))
		var err error

		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`insert into prepare_test values (1, 1)`)
		tk.MustExec(`insert into prepare_test values (2, 2)`)
		tk.MustExec(`insert into prepare_test values (3, 3)`)

		tk.MustExec(`prepare stmt_delete from 'delete from prepare_test where id = ?'`)
		tk.MustExec(`set @a=1; execute stmt_delete using @a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(0), hit)
		}
		tk.MustExec(`set @a=2; execute stmt_delete using @a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(1), hit)
		}
		tk.MustExec(`set @a=3; execute stmt_delete using @a;`)
		if flag {
			err = counter.Write(pb)
			require.NoError(t, err)
			hit := pb.GetCounter().GetValue()
			require.Equal(t, float64(2), hit)
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(nil)
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(nil)
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(nil)
	}
}

func TestPrepareDealloc(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=true`)

	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: plannercore.NewLRUPlanCache(3, 0.1, math.MaxUint64, tk.Session(), false),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists prepare_test")
	tk.MustExec("create table prepare_test (id int PRIMARY KEY, c1 int)")

	require.Equal(t, 0, tk.Session().GetSessionPlanCache().Size())
	tk.MustExec(`prepare stmt1 from 'select id from prepare_test'`)
	tk.MustExec("execute stmt1")
	tk.MustExec(`prepare stmt2 from 'select c1 from prepare_test'`)
	tk.MustExec("execute stmt2")
	tk.MustExec(`prepare stmt3 from 'select id, c1 from prepare_test'`)
	tk.MustExec("execute stmt3")
	tk.MustExec(`prepare stmt4 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt4")
	require.Equal(t, 3, tk.Session().GetSessionPlanCache().Size())

	tk.MustExec("deallocate prepare stmt1")
	require.Equal(t, 3, tk.Session().GetSessionPlanCache().Size())
	tk.MustExec("deallocate prepare stmt2")
	tk.MustExec("deallocate prepare stmt3")
	tk.MustExec("deallocate prepare stmt4")
	require.Equal(t, 0, tk.Session().GetSessionPlanCache().Size())

	tk.MustExec(`prepare stmt1 from 'select * from prepare_test'`)
	tk.MustExec(`execute stmt1`)
	tk.MustExec(`prepare stmt2 from 'select * from prepare_test'`)
	tk.MustExec(`execute stmt2`)
	require.Equal(t, 1, tk.Session().GetSessionPlanCache().Size()) // use the same cached plan since they have the same statement

	tk.MustExec(`drop database if exists plan_cache`)
	tk.MustExec(`create database plan_cache`)
	tk.MustExec(`use plan_cache`)
	tk.MustExec(`create table prepare_test (id int PRIMARY KEY, c1 int)`)
	tk.MustExec(`prepare stmt3 from 'select * from prepare_test'`)
	tk.MustExec(`execute stmt3`)
	require.Equal(t, 2, tk.Session().GetSessionPlanCache().Size()) // stmt3 has different DB
}

func TestPreparedIssue8153(t *testing.T) {
	store := testkit.CreateMockStore(t)
	flags := []bool{false, true}
	for _, flag := range flags {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(fmt.Sprintf(`set @@tidb_enable_prepared_plan_cache=%v`, flag))
		var err error

		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a int, b int)")
		tk.MustExec("insert into t (a, b) values (1,3), (2,2), (3,1)")

		tk.MustExec(`prepare stmt from 'select * from t order by ? asc'`)
		r := tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Rows("1 3", "2 2", "3 1"))

		tk.MustExec(`set @param = 1`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Rows("1 3", "2 2", "3 1"))

		tk.MustExec(`set @param = 2`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Rows("3 1", "2 2", "1 3"))

		tk.MustExec(`set @param = 3`)
		_, err = tk.Exec(`execute stmt using @param;`)
		require.EqualError(t, err, "[planner:1054]Unknown column '?' in 'order clause'")

		tk.MustExec(`set @param = '##'`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Rows("1 3", "2 2", "3 1"))

		tk.MustExec("insert into t (a, b) values (1,1), (1,2), (2,1), (2,3), (3,2), (3,3)")
		tk.MustExec(`prepare stmt from 'select ?, sum(a) from t group by ?'`)

		tk.MustExec(`set @a=1,@b=1`)
		r = tk.MustQuery(`execute stmt using @a,@b;`)
		r.Check(testkit.Rows("1 18"))

		tk.MustExec(`set @a=1,@b=2`)
		_, err = tk.Exec(`execute stmt using @a,@b;`)
		require.EqualError(t, err, "[planner:1056]Can't group on 'sum(a)'")
	}
}

func TestPreparedIssue17419(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	ctx := context.Background()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t (a) values (1), (2), (3)")

	conn2 := server.CreateMockConn(t, sv)
	tk1 := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)

	query := "select * from test.t"
	stmtID, _, _, err := tk1.Session().PrepareStmt(query)
	require.NoError(t, err)

	dom.ExpensiveQueryHandle().SetSessionManager(sv)

	rs, err := tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.NoError(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1", "2", "3"))
	tk1.Session().SetProcessInfo("", time.Now(), mysql.ComStmtExecute, 0)

	dom.ExpensiveQueryHandle().LogOnQueryExceedMemQuota(tk.Session().GetSessionVars().ConnectionID)

	// After entirely fixing https://github.com/pingcap/tidb/issues/17419
	// require.NotNil(t, tk1.Session().ShowProcess().Plan)
	// _, ok := tk1.Session().ShowProcess().Plan.(*plannercore.Execute)
	// require.True(t, ok)
}
