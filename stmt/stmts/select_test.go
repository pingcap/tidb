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

func (s *testStmtSuite) TestSelectWithoutFrom(c *C) {
	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query("select 1 + 2*3")
	c.Assert(err, IsNil)

	var result int
	for rows.Next() {
		rows.Scan(&result)
		c.Assert(result, Equals, 7)
	}

	rows.Close()
	mustCommit(c, tx)

	tx = mustBegin(c, s.testDB)
	rows, err = tx.Query(`select _utf8"string";`)
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{"string"}})
	rows.Close()
	mustCommit(c, tx)
}

func (s *testStmtSuite) TestSelectOrderBy(c *C) {
	s.fillData(s.testDB, c)

	tx := mustBegin(c, s.testDB)
	// Test star field
	rows, err := tx.Query("select * from test where id = 1 order by id limit 1 offset 0;")
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

	tx = mustBegin(c, s.testDB)
	// Test multiple field
	rows, err = tx.Query("select id, name from test where id = 1 group by id, name limit 1 offset 0;")
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
}

func (s *testStmtSuite) TestSelectDistinct(c *C) {
	s.fillData(s.testDB, c)

	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query("select distinct name from test;")
	c.Assert(err, IsNil)

	var cnt int
	for rows.Next() {
		var name string
		rows.Scan(&name)
		c.Assert(name, Equals, "hello")
		cnt++
	}
	c.Assert(cnt, Equals, 1)
	rows.Close()
	mustCommit(c, tx)
}

func (s *testStmtSuite) TestSelectHaving(c *C) {
	s.fillData(s.testDB, c)
	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query("select id, name from test where id in (1,3) having name like 'he%';")
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
}

func (s *testStmtSuite) TestSelectErrorRow(c *C) {
	tx := mustBegin(c, s.testDB)
	_, err := tx.Query("select row(1, 1) from test")
	c.Assert(err, NotNil)

	_, err = tx.Query("select * from test group by row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tx.Query("select * from test order by row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tx.Query("select * from test having row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tx.Query("select (select 1, 1) from test;")
	c.Assert(err, NotNil)

	_, err = tx.Query("select * from test group by (select 1, 1);")
	c.Assert(err, NotNil)

	_, err = tx.Query("select * from test order by (select 1, 1);")
	c.Assert(err, NotNil)

	_, err = tx.Query("select * from test having (select 1, 1);")
	c.Assert(err, NotNil)

	mustCommit(c, tx)
}
