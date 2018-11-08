// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestExportRowID(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.Se.GetSessionVars().AllowWriteRowID = true
	defer func() {
		tk.Se.GetSessionVars().AllowWriteRowID = false
	}()

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert t values (1, 7), (1, 8), (1, 9)")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "1 8 2", "1 9 3"))
	tk.MustExec("update t set a = 2 where _tidb_rowid = 2")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "2 8 2", "1 9 3"))

	tk.MustExec("delete from t where _tidb_rowid = 2")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "1 9 3"))

	tk.MustExec("insert t (a, b, _tidb_rowid) values (2, 2, 2), (5, 5, 5)")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "2 2 2", "1 9 3", "5 5 5"))

	// If PK is handle, _tidb_rowid is unknown column.
	tk.MustExec("create table s (a int primary key)")
	tk.MustExec("insert s values (1)")
	_, err := tk.Exec("insert s (a, _tidb_rowid) values (1, 2)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("select _tidb_rowid from s")
	c.Assert(err, NotNil)
	_, err = tk.Exec("update s set a = 2 where _tidb_rowid = 1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete from s where _tidb_rowid = 1")
	c.Assert(err, NotNil)

	// Make sure "AllowWriteRowID" is a session variable.
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	_, err = tk1.Exec("insert into t (a, _tidb_rowid) values(10, 1);")
	c.Assert(err.Error(), Equals, "insert, update and replace statements for _tidb_rowid are not supported.")
}

func (s *testSuite) TestNotAllowWriteRowID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table tt(id binary(10), c int, primary key(id));")
	tk.MustExec("insert tt values (1, 10);")
	// select statement
	tk.MustQuery("select *, _tidb_rowid from tt").
		Check(testkit.Rows("1\x00\x00\x00\x00\x00\x00\x00\x00\x00 10 1"))
	// insert statement
	_, err := tk.Exec("insert into tt (id, c, _tidb_rowid) values(30000,10,1);")
	c.Assert(err.Error(), Equals, "insert, update and replace statements for _tidb_rowid are not supported.")
	// replace statement
	_, err = tk.Exec("replace into tt (id, c, _tidb_rowid) values(30000,10,1);")
	c.Assert(err.Error(), Equals, "insert, update and replace statements for _tidb_rowid are not supported.")
	// update statement
	_, err = tk.Exec("update tt set id = 2, _tidb_rowid = 1 where _tidb_rowid = 1")
	c.Assert(err.Error(), Equals, "insert, update and replace statements for _tidb_rowid are not supported.")
	tk.MustExec("update tt set id = 2 where _tidb_rowid = 1")
	tk.MustExec("admin check table tt;")
	tk.MustExec("drop table tt")
	// There is currently no real support for inserting, updating, and replacing _tidb_rowid statements.
	// After we support it, the following operations must be passed.
	//	tk.MustExec("insert into tt (id, c, _tidb_rowid) values(30000,10,1);")
	//	tk.MustExec("admin check table tt;")
}

func (s *testSuite) TestRowIDIsRebasedAfterInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table tt(id varchar(10), c int, primary key(id));")
	tk.MustExec("insert tt values('one', 101), ('two', 102);")

	// check that _tidb_rowid is automatically populated
	tk.MustQuery("select *, _tidb_rowid from tt;").
		Check(testkit.Rows("one 101 1", "two 102 2"))

	// enable overwriting the _tidb_rowid column
	tk.MustExec("set @@tidb_opt_write_row_id = 1;")

	// check that _tidb_rowid takes the values from the insert statement
	tk.MustExec("insert tt (id, c, _tidb_rowid) values ('three', 103, 9), ('four', 104, 16), ('five', 105, 5);")
	tk.MustQuery("select *, _tidb_rowid from tt where c > 102;").
		Check(testkit.Rows("five 105 5", "three 103 9", "four 104 16"))

	// check that the new _tidb_rowid is rebased
	tk.MustExec("insert tt values ('six', 106), ('seven', 107);")
	tk.MustQuery("select *, _tidb_rowid from tt where c > 105;").
		Check(testkit.Rows("six 106 17", "seven 107 18"))

	tk.MustExec("admin check table tt;")
	tk.MustExec("drop table tt")
}

func (s *testSuite) TestRowIDPlusAutoIncIsRebasedAfterInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table tt(id varchar(10) primary key, c int auto_increment, index(c));")
	tk.MustExec("insert tt (id) values ('one'), ('two');")

	// check that both the auto_inc column and _tidb_rowid are automatically populated
	tk.MustQuery("select *, _tidb_rowid from tt;").
		Check(testkit.Rows("one 1 3", "two 2 4"))

	// enable overwriting the _tidb_rowid column
	tk.MustExec("set @@tidb_opt_write_row_id = 1;")

	// check behavior of rebasing the auto_inc column
	tk.MustExec("insert tt values ('three', 30), ('four', 40);")
	tk.MustExec("insert tt values ('five', 5), ('six', 6);")
	tk.MustQuery("select *, _tidb_rowid from tt;").
		Check(testkit.Rows("one 1 3", "two 2 4", "three 30 41", "four 40 42", "five 5 43", "six 6 44"))

	// check behavior of rebasing the _tidb_rowid column
	// (since the auto_inc column and _tidb_rowid share the same allocator, rebasing one will affect the other)
	tk.MustExec("insert tt (id, _tidb_rowid) values ('seven', 7), ('eight', 8);")
	tk.MustQuery("select *, _tidb_rowid from tt where c > 6;").
		Check(testkit.Rows("seven 45 7", "eight 46 8", "three 30 41", "four 40 42"))

	tk.MustExec("insert tt (id, _tidb_rowid) values ('nine', 90), ('ten', 10);")
	tk.MustQuery("select *, _tidb_rowid from tt where c > 46;").
		Check(testkit.Rows("ten 48 10", "nine 47 90"))

	// check behavior of implicitly filling in the columns again
	tk.MustExec("insert tt (id) values ('eleven'), ('twelve'), ('thirteen');")
	tk.MustQuery("select *, _tidb_rowid from tt where c > 48;").
		Check(testkit.Rows("eleven 91 94", "twelve 92 95", "thirteen 93 96"))

	tk.MustExec("admin check table tt;")
	tk.MustExec("drop table tt")
}
