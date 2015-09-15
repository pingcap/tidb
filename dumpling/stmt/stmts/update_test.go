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
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
)

func (s *testStmtSuite) TestUpdate(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"tmp/"+s.dbName)
	c.Assert(err, IsNil)

	s.fillData(testDB, c)

	updateStr := `UPDATE test SET name = "abc" where id > 0;`

	// Test compile
	stmtList, err := tidb.Compile(updateStr)
	c.Assert(err, IsNil)

	str := stmtList[0].OriginText()
	c.Assert(0, Less, len(str))

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

	// Should use index
	strs := s.queryStrings(testDB, `explain `+updateStr, c)
	var useIndex bool
	for _, str := range strs {
		if strings.Index(str, "index") > 0 {
			useIndex = true
		}
	}

	if !useIndex {
		c.Fatal(strs)
	}
	// Should not use index
	strs = s.queryStrings(testDB, `explain UPDATE test SET name = "abc"`, c)
	useIndex = false
	for _, str := range strs {
		if strings.Index(str, "index") > 0 {
			useIndex = true
		}
	}

	if useIndex {
		c.Fatal(strs)
	}

	// Test update without index
	r = mustExec(c, testDB, `explain UPDATE test SET name = "abc"`)
	checkResult(c, r, 0, 0)

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
	mustExec(c, currDB, "CREATE TABLE month (id int, price TEXT);")
	r = mustExec(c, currDB, `insert into month values (11, "month_price_11"), (22, "month_price_22"), (13, "month_price_13");`)
	checkResult(c, r, 3, 0)
}

func (s *testStmtSuite) TestMultipleTableUpdate(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"tmp/"+s.dbName)
	c.Assert(err, IsNil)
	s.fillMultiTableForUpdate(testDB, c)

	r := mustExec(c, testDB, `UPDATE items, month  SET items.price=month.price WHERE items.id=month.id;`)
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
}
