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
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestPrepared(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists prepare_test")
	tk.MustExec("create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
	tk.MustExec("insert prepare_test (c1) values (1),(2),(NULL)")

	tk.MustExec(`prepare stmt_test_1 from 'select id from prepare_test where id > ?'; set @a = 1; execute stmt_test_1 using @a;`)
	tk.MustExec(`prepare stmt_test_2 from 'select 1'`)
	// Prepare multiple statement is not allowed.
	_, err := tk.Exec(`prepare stmt_test_3 from 'select id from prepare_test where id > ?;select id from prepare_test where id > ?;'`)
	c.Assert(executor.ErrPrepareMulti.Equal(err), IsTrue)
	// The variable count does not match.
	_, err = tk.Exec(`prepare stmt_test_4 from 'select id from prepare_test where id > ? and id < ?'; set @a = 1; execute stmt_test_4 using @a;`)
	c.Assert(executor.ErrWrongParamCount.Equal(err), IsTrue)
	// Prepare and deallocate prepared statement immediately.
	tk.MustExec(`prepare stmt_test_5 from 'select id from prepare_test where id > ?'; deallocate prepare stmt_test_5;`)

	// Statement not found.
	_, err = tk.Exec("deallocate prepare stmt_test_5")
	c.Assert(executor.ErrStmtNotFound.Equal(err), IsTrue)

	// incorrect SQLs in prepare. issue #3738, SQL in prepare stmt is parsed in DoPrepare.
	_, err = tk.Exec(`prepare p from "delete from t where a = 7 or 1=1/*' and b = 'p'";`)
	c.Assert(terror.ErrorEqual(err, errors.New(`[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '/*' and b = 'p'' at line 1`)), IsTrue)

	// The `stmt_test5` should not be found.
	_, err = tk.Exec(`set @a = 1; execute stmt_test_5 using @a;`)
	c.Assert(executor.ErrStmtNotFound.Equal(err), IsTrue)

	// Use parameter marker with argument will run prepared statement.
	result := tk.MustQuery("select distinct c1, c2 from prepare_test where c1 = ?", 1)
	result.Check(testkit.Rows("1 <nil>"))

	// Call Session PrepareStmt directly to get stmtId.
	query := "select c1, c2 from prepare_test where c1 = ?"
	stmtId, _, _, err := tk.Se.PrepareStmt(query)
	c.Assert(err, IsNil)
	_, err = tk.Se.ExecutePreparedStmt(stmtId, 1)
	c.Assert(err, IsNil)

	// Check that ast.Statement created by executor.CompileExecutePreparedStmt has query text.
	stmt := executor.CompileExecutePreparedStmt(tk.Se, stmtId, 1)
	c.Assert(stmt.OriginText(), Equals, query)

	// Make schema change.
	tk.Exec("create table prepare2 (a int)")

	// Should success as the changed schema do not affect the prepared statement.
	_, err = tk.Se.ExecutePreparedStmt(stmtId, 1)
	c.Assert(err, IsNil)

	// Drop a column so the prepared statement become invalid.
	tk.MustExec("alter table prepare_test drop column c2")

	// There should be schema changed error.
	_, err = tk.Se.ExecutePreparedStmt(stmtId, 1)
	c.Assert(executor.ErrSchemaChanged.Equal(err), IsTrue)

	// issue 3381
	tk.MustExec("create table prepare3 (a decimal(1))")
	tk.MustExec("prepare stmt from 'insert into prepare3 value(123)'")
	_, err = tk.Exec("execute stmt")
	c.Assert(err, NotNil)

	// Coverage.
	exec := &executor.ExecuteExec{}
	exec.Next()
	exec.Close()
}

func (s *testSuite) TestPreparedLimitOffset(c *C) {
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
	_, err := tk.Exec("execute stmt_test_1 using @c, @c")
	c.Assert(plan.ErrWrongArguments.Equal(err), IsTrue)

	stmtID, _, _, err := tk.Se.PrepareStmt("select id from prepare_test limit ?")
	c.Assert(err, IsNil)
	_, err = tk.Se.ExecutePreparedStmt(stmtID, 1)
	c.Assert(err, IsNil)
}
