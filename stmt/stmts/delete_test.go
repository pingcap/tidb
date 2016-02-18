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
	"database/sql"

	. "github.com/pingcap/check"
)

func (s *testStmtSuite) fillData(currDB *sql.DB, c *C) {
	mustExec(c, currDB, s.createDBSql)
	mustExec(c, currDB, s.useDBSql)
	mustExec(c, currDB, s.createTableSql)

	// insert data
	r := mustExec(c, currDB, `INSERT INTO test VALUES (1, "hello");`)
	checkResult(c, r, 1, 0)
	r = mustExec(c, currDB, `INSERT INTO test VALUES (2, "hello");`)
	checkResult(c, r, 1, 0)
}

func (s *testStmtSuite) fillDataMultiTable(currDB *sql.DB, c *C) {
	// Create db
	mustExec(c, currDB, s.createDBSql)
	// Use db
	mustExec(c, currDB, s.useDBSql)
	// Create and fill table t1
	mustExec(c, currDB, "CREATE TABLE t1 (id int, data int);")
	r := mustExec(c, currDB, "insert into t1 values (11, 121), (12, 122), (13, 123);")
	checkResult(c, r, 3, 0)
	// Create and fill table t2
	mustExec(c, currDB, "CREATE TABLE t2 (id int, data int);")
	r = mustExec(c, currDB, "insert into t2 values (11, 221), (22, 222), (23, 223);")
	checkResult(c, r, 3, 0)
	// Create and fill table t3
	mustExec(c, currDB, "CREATE TABLE t3 (id int, data int);")
	r = mustExec(c, currDB, "insert into t3 values (11, 321), (22, 322), (23, 323);")
	checkResult(c, r, 3, 0)
}

func (s *testStmtSuite) queryStrings(currDB *sql.DB, sql string, c *C) []string {
	tx := mustBegin(c, currDB)
	rows, err := tx.Query(sql)
	c.Assert(err, IsNil)
	defer rows.Close()

	var strs []string
	for rows.Next() {
		var str string
		rows.Scan(&str)
		strs = append(strs, str)
	}
	return strs
}

func (s *testStmtSuite) TestDelete(c *C) {
	s.fillData(s.testDB, c)

	r := mustExec(c, s.testDB, `UPDATE test SET name = "abc" where id = 2;`)
	checkResult(c, r, 1, 0)

	r = mustExec(c, s.testDB, `DELETE from test where id = 2 limit 1;`)
	checkResult(c, r, 1, 0)

	// Test delete with false condition
	r = mustExec(c, s.testDB, `DELETE from test where 0;`)
	checkResult(c, r, 0, 0)

	mustExec(c, s.testDB, "insert into test values (2, 'abc')")
	r = mustExec(c, s.testDB, `delete from test where test.id = 2 limit 1`)
	checkResult(c, r, 1, 0)

	// Select data
	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query(s.selectSql)
	c.Assert(err, IsNil)

	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		c.Assert(id, Equals, 1)
		c.Assert(name, Equals, "hello")
	}

	rows.Close()
	mustCommit(c, tx)

	r = mustExec(c, s.testDB, `DELETE from test;`)
	checkResult(c, r, 1, 0)
}

func (s *testStmtSuite) TestMultiTableDelete(c *C) {
	s.fillDataMultiTable(s.testDB, c)

	r := mustExec(c, s.testDB, `DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;`)
	checkResult(c, r, 2, 0)

	// Select data
	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query("select * from t3")
	c.Assert(err, IsNil)

	cnt := 0
	for rows.Next() {
		cnt++
	}
	c.Assert(cnt, Equals, 3)
	rows.Close()
}

func (s *testStmtSuite) TestQualifedDelete(c *C) {
	mustExec(c, s.testDB, "drop table if exists t1")
	mustExec(c, s.testDB, "drop table if exists t2")
	mustExec(c, s.testDB, "create table t1 (c1 int, c2 int, index (c1))")
	mustExec(c, s.testDB, "create table t2 (c1 int, c2 int)")
	mustExec(c, s.testDB, "insert into t1 values (1, 1), (2, 2)")

	// delete with index
	r := mustExec(c, s.testDB, "delete from t1 where t1.c1 = 1")
	checkResult(c, r, 1, 0)

	// delete with no index
	r = mustExec(c, s.testDB, "delete from t1 where t1.c2 = 2")
	checkResult(c, r, 1, 0)

	rows, err := s.testDB.Query("select * from t1")
	c.Assert(err, IsNil)
	cnt := 0
	for rows.Next() {
		cnt++
	}
	rows.Close()
	c.Assert(cnt, Equals, 0)

	_, err = s.testDB.Exec("delete from t1 as a where a.c1 = 1")
	c.Assert(err, NotNil)

	mustExec(c, s.testDB, "insert into t1 values (1, 1), (2, 2)")
	mustExec(c, s.testDB, "insert into t2 values (2, 1), (3,1)")

	r = mustExec(c, s.testDB, "delete t1, t2 from t1 join t2 where t1.c1 = t2.c2")
	checkResult(c, r, 3, 0)

	mustExec(c, s.testDB, "insert into t2 values (2, 1), (3,1)")
	r = mustExec(c, s.testDB, "delete a, b from t1 as a join t2 as b where a.c2 = b.c1")
	checkResult(c, r, 2, 0)

	_, err = s.testDB.Exec("delete t1, t2 from t1 as a join t2 as b where a.c2 = b.c1")
	c.Assert(err, NotNil)

	mustExec(c, s.testDB, "drop table t1, t2")
}
