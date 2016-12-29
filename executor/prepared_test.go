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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestPrepared(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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

	// The `stmt_test5` should not be found.
	_, err = tk.Exec(`set @a = 1; execute stmt_test_5 using @a;`)
	c.Assert(executor.ErrStmtNotFound.Equal(err), IsTrue)

	// Use parameter marker with argument will run prepared statement.
	result := tk.MustQuery("select distinct c1, c2 from prepare_test where c1 = ?", 1)
	result.Check([][]interface{}{{1, nil}})

	// Call Session PrepareStmt directly to get stmtId.
	stmtId, _, _, err := tk.Se.PrepareStmt("select c1, c2 from prepare_test where c1 = ?")
	c.Assert(err, IsNil)
	_, err = tk.Se.ExecutePreparedStmt(stmtId, 1)
	c.Assert(err, IsNil)

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

	// Coverage.
	exec := &executor.ExecuteExec{}
	exec.Next()
	exec.Close()
}
