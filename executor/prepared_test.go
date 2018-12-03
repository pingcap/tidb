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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"math"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/metrics"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/testkit"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/net/context"
)

func (s *testSuite) TestPrepared(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	flags := []bool{false, true}
	ctx := context.Background()
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		plannercore.PreparedPlanCacheCapacity = 100
		plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
		c.Assert(err, IsNil)
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
		tk.MustExec("insert prepare_test (c1) values (1),(2),(NULL)")

		tk.MustExec(`prepare stmt_test_1 from 'select id from prepare_test where id > ?'; set @a = 1; execute stmt_test_1 using @a;`)
		tk.MustExec(`prepare stmt_test_2 from 'select 1'`)
		// Prepare multiple statement is not allowed.
		_, err = tk.Exec(`prepare stmt_test_3 from 'select id from prepare_test where id > ?;select id from prepare_test where id > ?;'`)
		c.Assert(executor.ErrPrepareMulti.Equal(err), IsTrue)
		// The variable count does not match.
		_, err = tk.Exec(`prepare stmt_test_4 from 'select id from prepare_test where id > ? and id < ?'; set @a = 1; execute stmt_test_4 using @a;`)
		c.Assert(plannercore.ErrWrongParamCount.Equal(err), IsTrue)
		// Prepare and deallocate prepared statement immediately.
		tk.MustExec(`prepare stmt_test_5 from 'select id from prepare_test where id > ?'; deallocate prepare stmt_test_5;`)

		// Statement not found.
		_, err = tk.Exec("deallocate prepare stmt_test_5")
		c.Assert(plannercore.ErrStmtNotFound.Equal(err), IsTrue)

		// incorrect SQLs in prepare. issue #3738, SQL in prepare stmt is parsed in DoPrepare.
		_, err = tk.Exec(`prepare p from "delete from t where a = 7 or 1=1/*' and b = 'p'";`)
		c.Assert(terror.ErrorEqual(err, errors.New(`[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '/*' and b = 'p'' at line 1`)), IsTrue, Commentf("err %v", err))

		// The `stmt_test5` should not be found.
		_, err = tk.Exec(`set @a = 1; execute stmt_test_5 using @a;`)
		c.Assert(plannercore.ErrStmtNotFound.Equal(err), IsTrue)

		// Use parameter marker with argument will run prepared statement.
		result := tk.MustQuery("select distinct c1, c2 from prepare_test where c1 = ?", 1)
		result.Check(testkit.Rows("1 <nil>"))

		// Call Session PrepareStmt directly to get stmtId.
		query := "select c1, c2 from prepare_test where c1 = ?"
		stmtId, _, _, err := tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		rs, err := tk.Se.ExecutePreparedStmt(ctx, stmtId, 1)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 <nil>"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
		stmtId, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		tk1 := testkit.NewTestKitWithInit(c, s.store)
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 3)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
		stmtId, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 3)
		c.Assert(err, IsNil)
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 3)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 in (select c1 from prepare_test where c1 = ?)"
		stmtId, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 3)
		c.Assert(err, IsNil)
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 3)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3"))

		tk.MustExec("begin")
		tk.MustExec("insert prepare_test (c1) values (4)")
		query = "select c1, c2 from prepare_test where c1 = ?"
		stmtId, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		tk.MustExec("rollback")
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 4)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows())

		// Check that ast.Statement created by executor.CompileExecutePreparedStmt has query text.
		stmt, err := executor.CompileExecutePreparedStmt(tk.Se, stmtId, 1)
		c.Assert(err, IsNil)
		c.Assert(stmt.OriginText(), Equals, query)

		// Check that rebuild plan works.
		tk.Se.PrepareTxnCtx(ctx)
		_, err = stmt.RebuildPlan()
		c.Assert(err, IsNil)
		rs, err = stmt.Exec(ctx)
		c.Assert(err, IsNil)
		chk := rs.NewChunk()
		err = rs.Next(ctx, chk)
		c.Assert(err, IsNil)
		c.Assert(rs.Close(), IsNil)

		// Make schema change.
		tk.MustExec("drop table if exists prepare2")
		tk.Exec("create table prepare2 (a int)")

		// Should success as the changed schema do not affect the prepared statement.
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 1)
		c.Assert(err, IsNil)

		// Drop a column so the prepared statement become invalid.
		query = "select c1, c2 from prepare_test where c1 = ?"
		stmtId, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		tk.MustExec("alter table prepare_test drop column c2")

		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 1)
		c.Assert(plannercore.ErrUnknownColumn.Equal(err), IsTrue)

		tk.MustExec("drop table prepare_test")
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtId, 1)
		c.Assert(plannercore.ErrSchemaChanged.Equal(err), IsTrue)

		// issue 3381
		tk.MustExec("drop table if exists prepare3")
		tk.MustExec("create table prepare3 (a decimal(1))")
		tk.MustExec("prepare stmt from 'insert into prepare3 value(123)'")
		_, err = tk.Exec("execute stmt")
		c.Assert(err, NotNil)

		_, _, fields, err := tk.Se.PrepareStmt("select a from prepare3")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].TableAsName.L, Equals, "prepare3")
		c.Assert(fields[0].ColumnAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select a from prepare3 where ?")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].TableAsName.L, Equals, "prepare3")
		c.Assert(fields[0].ColumnAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select (1,1) in (select 1,1)")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "")
		c.Assert(fields[0].TableAsName.L, Equals, "")
		c.Assert(fields[0].ColumnAsName.L, Equals, "(1,1) in (select 1,1)")

		_, _, fields, err = tk.Se.PrepareStmt("select a from prepare3 where a = (" +
			"select a from prepare2 where a = ?)")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].TableAsName.L, Equals, "prepare3")
		c.Assert(fields[0].ColumnAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select * from prepare3 as t1 join prepare3 as t2")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].TableAsName.L, Equals, "t1")
		c.Assert(fields[0].ColumnAsName.L, Equals, "a")
		c.Assert(fields[1].DBName.L, Equals, "test")
		c.Assert(fields[1].TableAsName.L, Equals, "t2")
		c.Assert(fields[1].ColumnAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("update prepare3 set a = ?")
		c.Assert(err, IsNil)
		c.Assert(len(fields), Equals, 0)

		// issue 8074
		tk.MustExec("drop table if exists prepare1;")
		tk.MustExec("create table prepare1 (a decimal(1))")
		tk.MustExec("insert into prepare1 values(1);")
		_, err = tk.Exec("prepare stmt FROM @sql1")
		c.Assert(err.Error(), Equals, "line 1 column 4 near \"\" (total length 4)")
		tk.MustExec("SET @sql = 'update prepare1 set a=5 where a=?';")
		_, err = tk.Exec("prepare stmt FROM @sql")
		c.Assert(err, IsNil)
		tk.MustExec("set @var=1;")
		_, err = tk.Exec("execute stmt using @var")
		c.Assert(err, IsNil)
		tk.MustQuery("select a from prepare1;").Check(testkit.Rows("5"))

		// Coverage.
		exec := &executor.ExecuteExec{}
		exec.Next(ctx, nil)
		exec.Close()
	}
}

func (s *testSuite) TestPreparedLimitOffset(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	flags := []bool{false, true}
	ctx := context.Background()
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		plannercore.PreparedPlanCacheCapacity = 100
		plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
		c.Assert(err, IsNil)
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
		tk.MustExec("insert prepare_test (c1) values (1),(2),(NULL)")
		tk.MustExec(`prepare stmt_test_1 from 'select id from prepare_test limit ? offset ?'; set @a = 1, @b=1;`)
		r := tk.MustQuery(`execute stmt_test_1 using @a, @b;`)
		r.Check(testkit.Rows("2"))

		tk.MustExec(`set @a=1.1`)
		r = tk.MustQuery(`execute stmt_test_1 using @a, @b;`)
		r.Check(testkit.Rows("2"))

		tk.MustExec(`set @c="-1"`)
		_, err = tk.Exec("execute stmt_test_1 using @c, @c")
		c.Assert(plannercore.ErrWrongArguments.Equal(err), IsTrue)

		stmtID, _, _, err := tk.Se.PrepareStmt("select id from prepare_test limit ?")
		c.Assert(err, IsNil)
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, 1)
		c.Assert(err, IsNil)
	}
}

func (s *testSuite) TestPreparedNullParam(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		plannercore.PreparedPlanCacheCapacity = 100
		plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
		c.Assert(err, IsNil)
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (id int, KEY id (id))")
		tk.MustExec("insert into t values (1), (2), (3)")
		tk.MustExec(`prepare stmt from 'select * from t use index(id) where id = ?'`)

		r := tk.MustQuery(`execute stmt using @id;`)
		r.Check(nil)

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(nil)

		tk.MustExec(`set @id="1"`)
		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Rows("1"))

		r = tk.MustQuery(`execute stmt using @id2;`)
		r.Check(nil)

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Rows("1"))
	}
}

func (s *testSuite) TestPreparedNameResolver(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, KEY id (id))")
	tk.MustExec("prepare stmt from 'select * from t limit ? offset ?'")
	_, err := tk.Exec("prepare stmt from 'select b from t'")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'field list'")

	_, err = tk.Exec("prepare stmt from '(select * FROM t) union all (select * FROM t) order by a limit ?'")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'a' in 'order clause'")
}

func (s *testSuite) TestPrepareMaxParamCountCheck(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	normalSQL, normalParams := generateBatchSQL(math.MaxUint16)
	_, err := tk.Exec(normalSQL, normalParams...)
	c.Assert(err, IsNil)

	bigSQL, bigParams := generateBatchSQL(math.MaxUint16 + 2)
	_, err = tk.Exec(bigSQL, bigParams...)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[executor:1390]Prepared statement contains too many placeholders")
}

func (s *testSuite) TestPrepareWithAggregation(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		plannercore.PreparedPlanCacheCapacity = 100
		plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
		c.Assert(err, IsNil)
		tk := testkit.NewTestKit(c, s.store)
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

func generateBatchSQL(paramCount int) (sql string, paramSlice []interface{}) {
	params := make([]interface{}, 0, paramCount)
	placeholders := make([]string, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		params = append(params, i)
		placeholders = append(placeholders, "(?)")
	}
	return "insert into t values " + strings.Join(placeholders, ","), params
}

func (s *testSuite) TestPreparedIssue7579(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		plannercore.PreparedPlanCacheCapacity = 100
		plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
		c.Assert(err, IsNil)
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a int, b int, index a_idx(a))")
		tk.MustExec("insert into t values (1,1), (2,2), (null,3)")

		r := tk.MustQuery("select a, b from t order by b asc;")
		r.Check(testkit.Rows("1 1", "2 2", "<nil> 3"))

		tk.MustExec(`prepare stmt from 'select a, b from t where ? order by b asc'`)

		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)

		tk.MustExec(`set @param = true`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Rows("1 1", "2 2", "<nil> 3"))

		tk.MustExec(`set @param = false`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)

		tk.MustExec(`set @param = 1`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Rows("1 1", "2 2", "<nil> 3"))

		tk.MustExec(`set @param = 0`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)
	}
}

func (s *testSuite) TestPreparedInsert(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		plannercore.PreparedPlanCacheCapacity = 100
		plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
		c.Assert(err, IsNil)
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`prepare stmt_insert from 'insert into prepare_test values (?, ?)'`)
		tk.MustExec(`set @a=1,@b=1; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(0))
		}
		tk.MustExec(`set @a=2,@b=2; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(1))
		}
		tk.MustExec(`set @a=3,@b=3; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
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
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}
		tk.MustExec(`set @a=2; execute stmt_insert_select using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(3))
		}
		tk.MustExec(`set @a=3; execute stmt_insert_select using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(4))
		}

		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 101)
		result.Check(testkit.Rows("101 101"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 102)
		result.Check(testkit.Rows("102 102"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 103)
		result.Check(testkit.Rows("103 103"))
	}
}

func (s *testSuite) TestPreparedUpdate(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		plannercore.PreparedPlanCacheCapacity = 100
		plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
		c.Assert(err, IsNil)
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`insert into prepare_test values (1, 1)`)
		tk.MustExec(`insert into prepare_test values (2, 2)`)
		tk.MustExec(`insert into prepare_test values (3, 3)`)

		tk.MustExec(`prepare stmt_update from 'update prepare_test set c1 = c1 + ? where id = ?'`)
		tk.MustExec(`set @a=1,@b=100; execute stmt_update using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(0))
		}
		tk.MustExec(`set @a=2,@b=200; execute stmt_update using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(1))
		}
		tk.MustExec(`set @a=3,@b=300; execute stmt_update using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(testkit.Rows("1 101"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(testkit.Rows("2 202"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(testkit.Rows("3 303"))
	}
}

func (s *testSuite) TestPreparedDelete(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		plannercore.PreparedPlanCacheCapacity = 100
		plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
		c.Assert(err, IsNil)
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists prepare_test")
		tk.MustExec("create table prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`insert into prepare_test values (1, 1)`)
		tk.MustExec(`insert into prepare_test values (2, 2)`)
		tk.MustExec(`insert into prepare_test values (3, 3)`)

		tk.MustExec(`prepare stmt_delete from 'delete from prepare_test where id = ?'`)
		tk.MustExec(`set @a=1; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(0))
		}
		tk.MustExec(`set @a=2; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(1))
		}
		tk.MustExec(`set @a=3; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(nil)
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(nil)
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(nil)
	}
}

func (s *testSuite) TestPrepareDealloc(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	orgCapacity := plannercore.PreparedPlanCacheCapacity
	orgMemGuardRatio := plannercore.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := plannercore.PreparedPlanCacheMaxMemory
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
		plannercore.PreparedPlanCacheCapacity = orgCapacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		plannercore.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	var err error
	plannercore.SetPreparedPlanCache(true)
	plannercore.PreparedPlanCacheCapacity = 3
	plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
	plannercore.PreparedPlanCacheMaxMemory, err = memory.MemTotal()
	c.Assert(err, IsNil)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists prepare_test")
	tk.MustExec("create table prepare_test (id int PRIMARY KEY, c1 int)")

	c.Assert(tk.Se.PreparedPlanCache().Size(), Equals, 0)
	tk.MustExec(`prepare stmt1 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt1")
	tk.MustExec(`prepare stmt2 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt2")
	tk.MustExec(`prepare stmt3 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt3")
	tk.MustExec(`prepare stmt4 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt4")
	c.Assert(tk.Se.PreparedPlanCache().Size(), Equals, 3)

	tk.MustExec("deallocate prepare stmt1")
	c.Assert(tk.Se.PreparedPlanCache().Size(), Equals, 3)
	tk.MustExec("deallocate prepare stmt2")
	tk.MustExec("deallocate prepare stmt3")
	tk.MustExec("deallocate prepare stmt4")
	c.Assert(tk.Se.PreparedPlanCache().Size(), Equals, 0)
}
