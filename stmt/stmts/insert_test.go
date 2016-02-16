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

func (s *testStmtSuite) TestInsert(c *C) {
	testSQL := `drop table if exists insert_test;create table insert_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	mustExec(c, s.testDB, testSQL)
	testSQL = `insert insert_test (c1) values (1),(2),(NULL);`
	mustExec(c, s.testDB, testSQL)

	errInsertSelectSQL := `insert insert_test (c1) values ();`
	tx := mustBegin(c, s.testDB)
	_, err := tx.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errInsertSelectSQL = `insert insert_test (c1, c2) values (1,2),(1);`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errInsertSelectSQL = `insert insert_test (xxx) values (3);`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errInsertSelectSQL = `insert insert_test_xxx (c1) values ();`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	insertSetSQL := `insert insert_test set c1 = 3;`
	mustExec(c, s.testDB, insertSetSQL)

	errInsertSelectSQL = `insert insert_test set c1 = 4, c1 = 5;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errInsertSelectSQL = `insert insert_test set xxx = 6;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	insertSelectSQL := `create table insert_test_1 (id int, c1 int);`
	mustExec(c, s.testDB, insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test;`
	mustExec(c, s.testDB, insertSelectSQL)

	insertSelectSQL = `create table insert_test_2 (id int, c1 int);`
	mustExec(c, s.testDB, insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test union select id * 10, c1 * 10 from insert_test;`
	mustExec(c, s.testDB, insertSelectSQL)

	errInsertSelectSQL = `insert insert_test_1 select c1 from insert_test;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	insertSQL := `insert into insert_test (id, c2) values (1, 1) on duplicate key update c2=10;`
	mustExec(c, s.testDB, insertSQL)

	insertSQL = `insert into insert_test (id, c2) values (1, 1) on duplicate key update insert_test.c2=10;`
	mustExec(c, s.testDB, insertSQL)

	_, err = s.testDB.Exec(`insert into insert_test (id, c2) values(1, 1) on duplicate key update t.c2 = 10`)
	c.Assert(err, NotNil)
}

func (s *testStmtSuite) TestInsertAutoInc(c *C) {
	createSQL := `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	mustExec(c, s.testDB, createSQL)

	insertSQL := `insert into insert_autoinc_test(c1) values (1), (2)`
	mustExec(c, s.testDB, insertSQL)

	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query("select * from insert_autoinc_test;")
	c.Assert(err, IsNil)

	c.Assert(rows.Next(), IsTrue)
	var id, c1 int
	rows.Scan(&id, &c1)
	c.Assert(id, Equals, 1)
	c.Assert(c1, Equals, 1)

	c.Assert(rows.Next(), IsTrue)
	rows.Scan(&id, &c1)
	c.Assert(id, Equals, 2)
	c.Assert(c1, Equals, 2)

	c.Assert(rows.Next(), IsFalse)
	rows.Close()
	mustCommit(c, tx)
}
