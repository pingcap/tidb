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
	"github.com/pingcap/tidb"
)

func (s *testStmtSuite) TestUpdate(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"tmp/"+s.dbName)
	c.Assert(err, IsNil)

	s.fillData(testDB, c)

	updateStr := `UPDATE test SET name = "abc" where id > 0;`

	r := mustExec(c, testDB, updateStr)
	checkResult(c, r, 2, 0)

	// select data
	tx := mustBegin(c, testDB)
	rows, err := tx.Query(s.selectSql)
	c.Assert(err, IsNil)

	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		c.Assert(name, Equals, "abc")
	}

	rows.Close()
	mustCommit(c, tx)

	r = mustExec(c, testDB, `UPDATE test SET name = "foo"`)
	checkResult(c, r, 2, 0)
}

func (s *testStmtSuite) fillMultiTableForUpdate(currDB *sql.DB, c *C) {
	// Create db
	mustExec(c, currDB, s.createDBSql)
	// Use db
	mustExec(c, currDB, s.useDBSql)
	// Create and fill table items
	mustExec(c, currDB, "CREATE TABLE items (id int, price TEXT);")
	r := mustExec(c, currDB, `insert into items values (11, "items_price_11"), (12, "items_price_12"), (13, "items_price_13");`)
	checkResult(c, r, 3, 0)
	// Create and fill table month
	mustExec(c, currDB, "CREATE TABLE month (mid int, mprice TEXT);")
	r = mustExec(c, currDB, `insert into month values (11, "month_price_11"), (22, "month_price_22"), (13, "month_price_13");`)
	checkResult(c, r, 3, 0)
}

func (s *testStmtSuite) TestMultipleTableUpdate(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"tmp/"+s.dbName)
	c.Assert(err, IsNil)
	s.fillMultiTableForUpdate(testDB, c)

	r := mustExec(c, testDB, `UPDATE items, month  SET items.price=month.mprice WHERE items.id=month.mid;`)
	c.Assert(r, NotNil)

	tx := mustBegin(c, testDB)
	rows, err := tx.Query("SELECT * FROM items")
	c.Assert(err, IsNil)

	expectedResult := map[int]string{
		11: "month_price_11",
		12: "items_price_12",
		13: "month_price_13",
	}
	for rows.Next() {
		var (
			id    int
			price string
		)
		rows.Scan(&id, &price)
		c.Assert(price, Equals, expectedResult[id])
	}
	rows.Close()
	mustCommit(c, tx)

	// Single-table syntax but with multiple tables
	r = mustExec(c, testDB, `UPDATE items join month on items.id=month.mid SET items.price=month.mid;`)
	c.Assert(r, NotNil)
	tx = mustBegin(c, testDB)
	rows, err = tx.Query("SELECT * FROM items")
	c.Assert(err, IsNil)
	expectedResult = map[int]string{
		11: "11",
		12: "items_price_12",
		13: "13",
	}
	for rows.Next() {
		var (
			id    int
			price string
		)
		rows.Scan(&id, &price)
		c.Assert(price, Equals, expectedResult[id])
	}
	rows.Close()
	mustCommit(c, tx)

	// JoinTable with alias table name.
	r = mustExec(c, testDB, `UPDATE items T0 join month T1 on T0.id=T1.mid SET T0.price=T1.mprice;`)
	c.Assert(r, NotNil)
	tx = mustBegin(c, testDB)
	rows, err = tx.Query("SELECT * FROM items")
	c.Assert(err, IsNil)
	expectedResult = map[int]string{
		11: "month_price_11",
		12: "items_price_12",
		13: "month_price_13",
	}
	for rows.Next() {
		var (
			id    int
			price string
		)
		rows.Scan(&id, &price)
		c.Assert(price, Equals, expectedResult[id])
	}
	rows.Close()
	mustCommit(c, tx)
}

// For https://github.com/pingcap/tidb/issues/345
func (s *testStmtSuite) TestIssue345(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"tmp-issue345/"+s.dbName)
	c.Assert(err, IsNil)
	mustExec(c, testDB, `drop table if exists t1, t2`)
	mustExec(c, testDB, `create table t1 (c1 int);`)
	mustExec(c, testDB, `create table t2 (c2 int);`)
	mustExec(c, testDB, `insert into t1 values (1);`)
	mustExec(c, testDB, `insert into t2 values (2);`)
	mustExec(c, testDB, `update t1, t2 set t1.c1 = 2, t2.c2 = 1;`)
	mustExec(c, testDB, `update t1, t2 set c1 = 2, c2 = 1;`)
	mustExec(c, testDB, `update t1 as a, t2 as b set a.c1 = 2, b.c2 = 1;`)

	// Check t1 content
	tx := mustBegin(c, testDB)
	rows, err := tx.Query("SELECT * FROM t1;")
	matchRows(c, rows, [][]interface{}{{2}})
	rows.Close()
	mustCommit(c, tx)
	// Check t2 content
	tx = mustBegin(c, testDB)
	rows, err = tx.Query("SELECT * FROM t2;")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{1}})
	rows.Close()
	mustCommit(c, tx)

	mustExec(c, testDB, `update t1 as a, t2 as t1 set a.c1 = 1, t1.c2 = 2;`)
	// Check t1 content
	tx = mustBegin(c, testDB)
	rows, err = tx.Query("SELECT * FROM t1;")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{1}})
	rows.Close()
	mustCommit(c, tx)
	// Check t2 content
	tx = mustBegin(c, testDB)
	rows, err = tx.Query("SELECT * FROM t2;")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{2}})
	rows.Close()

	_, err = testDB.Exec(`update t1 as a, t2 set t1.c1 = 10;`)
	c.Assert(err, NotNil)

	mustCommit(c, tx)
}

func (s *testStmtSuite) TestMultiUpdate(c *C) {
	c.Skip("Need to change `tidb.Compile` function")
	// fix https://github.com/pingcap/tidb/issues/369
	testSQL := `
		DROP TABLE IF EXISTS t1, t2;
		create table t1 (c int);
		create table t2 (c varchar(256));
		insert into t1 values (1), (2);
		insert into t2 values ("a"), ("b");
		update t1, t2 set t1.c = 10, t2.c = "abc";`
	mustExec(c, s.testDB, testSQL)

	// fix https://github.com/pingcap/tidb/issues/376
	testSQL = `DROP TABLE IF EXISTS t1, t2;
		create table t1 (c1 int);
		create table t2 (c2 int);
		insert into t1 values (1), (2);
		insert into t2 values (1), (2);
		update t1, t2 set t1.c1 = 10, t2.c2 = 2 where t2.c2 = 1;`
	mustExec(c, s.testDB, testSQL)

	rows, err := s.testDB.Query("select * from t1")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{10}, {10}})
}
