// Copyright 2015 PingCAP, Inc.
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

package stmts_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/stmt/stmts"
)

func (s *testStmtSuite) TestReplace(c *C) {
	testSQL := `drop table if exists replace_test;
    create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);
    replace replace_test (c1) values (1),(2),(NULL);`
	mustExec(c, s.testDB, testSQL)

	errReplaceSelectSQL := `replace replace_test (c1) values ();`
	tx := mustBegin(c, s.testDB)
	_, err := tx.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errReplaceSelectSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errReplaceSelectSQL = `replace replace_test (xxx) values (3);`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errReplaceSelectSQL = `replace replace_test_xxx (c1) values ();`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	replaceSetSQL := `replace replace_test set c1 = 3;`
	mustExec(c, s.testDB, replaceSetSQL)

	stmtList, err := tidb.Compile(replaceSetSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok := stmtList[0].(*stmts.ReplaceIntoStmt)
	c.Assert(ok, IsTrue)
	c.Assert(testStmt.IsDDL(), IsFalse)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	errReplaceSelectSQL = `replace replace_test set c1 = 4, c1 = 5;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errReplaceSelectSQL = `replace replace_test set xxx = 6;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	replaceSelectSQL := `create table replace_test_1 (id int, c1 int); replace replace_test_1 select id, c1 from replace_test;`
	mustExec(c, s.testDB, replaceSelectSQL)

	replaceSelectSQL = `create table replace_test_2 (id int, c1 int); 
	replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`
	mustExec(c, s.testDB, replaceSelectSQL)

	errReplaceSelectSQL = `replace replace_test_1 select c1 from replace_test;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()
}
