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
)

func (s *testStmtSuite) TestReplace(c *C) {
	testSQL := `drop table if exists replace_test;
    create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	mustExec(c, s.testDB, testSQL)
	testSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	mustExec(c, s.testDB, testSQL)

	errReplaceSQL := `replace replace_test (c1) values ();`
	tx := mustBegin(c, s.testDB)
	_, err := tx.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errReplaceSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errReplaceSQL = `replace replace_test (xxx) values (3);`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errReplaceSQL = `replace replace_test_xxx (c1) values ();`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	replaceSetSQL := `replace replace_test set c1 = 3;`
	mustExec(c, s.testDB, replaceSetSQL)

	errReplaceSetSQL := `replace replace_test set c1 = 4, c1 = 5;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSetSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errReplaceSetSQL = `replace replace_test set xxx = 6;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSetSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	replaceSelectSQL := `create table replace_test_1 (id int, c1 int);`
	mustExec(c, s.testDB, replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test;`
	mustExec(c, s.testDB, replaceSelectSQL)

	replaceSelectSQL = `create table replace_test_2 (id int, c1 int);`
	mustExec(c, s.testDB, replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`
	mustExec(c, s.testDB, replaceSelectSQL)

	errReplaceSelectSQL := `replace replace_test_1 select c1 from replace_test;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	replaceUniqueIndexSQL := `create table replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2));`
	mustExec(c, s.testDB, replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	mustExec(c, s.testDB, replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	ret := mustExec(c, s.testDB, replaceUniqueIndexSQL)
	rows, err := ret.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(rows, Equals, int64(1))
	replaceUniqueIndexSQL = `replace into replace_test_3 set c1=1, c2=1;`
	ret = mustExec(c, s.testDB, replaceUniqueIndexSQL)
	rows, err = ret.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(rows, Equals, int64(2))

	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	mustExec(c, s.testDB, replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	ret = mustExec(c, s.testDB, replaceUniqueIndexSQL)
	rows, err = ret.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(rows, Equals, int64(1))

	replaceUniqueIndexSQL = `create table replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2));`
	mustExec(c, s.testDB, replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	mustExec(c, s.testDB, replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	ret = mustExec(c, s.testDB, replaceUniqueIndexSQL)
	rows, err = ret.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(rows, Equals, int64(1))

	replacePrimaryKeySQL := `create table replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2));`
	mustExec(c, s.testDB, replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	mustExec(c, s.testDB, replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	ret = mustExec(c, s.testDB, replacePrimaryKeySQL)
	rows, err = ret.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(rows, Equals, int64(1))
}
