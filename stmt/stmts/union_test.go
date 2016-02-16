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
	"fmt"

	. "github.com/pingcap/check"
)

func (s *testStmtSuite) TestUnion(c *C) {
	testSQL := `select 1 union select 0;`
	mustExec(c, s.testDB, testSQL)

	testSQL = `drop table if exists union_test; create table union_test(id int);`
	mustExec(c, s.testDB, testSQL)

	testSQL = `drop table if exists union_test;`
	mustExec(c, s.testDB, testSQL)
	testSQL = `create table union_test(id int);`
	mustExec(c, s.testDB, testSQL)
	testSQL = `insert union_test values (1),(2); select id from union_test union select 1;`
	mustExec(c, s.testDB, testSQL)

	testSQL = `select id from union_test union select id from union_test;`
	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query(testSQL)
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{1}, {2}})

	rows, err = tx.Query("select 1 union all select 1")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{1}, {1}})

	rows, err = tx.Query("select 1 union all select 1 union select 1")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{1}})

	rows, err = tx.Query("select 1 union (select 2) limit 1")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{1}})

	rows, err = tx.Query("select 1 union (select 2) limit 1, 1")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{2}})

	rows, err = tx.Query("select id from union_test union all (select 1) order by id desc")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{2}, {1}, {1}})

	rows, err = tx.Query("select id as a from union_test union (select 1) order by a desc")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{2}, {1}})

	rows, err = tx.Query(`select null union select "abc"`)
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{nil}, {"abc"}})

	rows, err = tx.Query(`select "abc" union select 1`)
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{"abc"}, {"1"}})

	mustCommit(c, tx)
}

func dumpRows(c *C, rows *sql.Rows) [][]interface{} {
	cols, err := rows.Columns()
	c.Assert(err, IsNil)
	ay := make([][]interface{}, 0)
	for rows.Next() {
		v := make([]interface{}, len(cols))
		for i := range v {
			v[i] = new(interface{})
		}
		err = rows.Scan(v...)
		c.Assert(err, IsNil)

		for i := range v {
			v[i] = *(v[i].(*interface{}))
		}
		ay = append(ay, v)
	}

	rows.Close()
	c.Assert(rows.Err(), IsNil)
	return ay
}

func matchRows(c *C, rows *sql.Rows, expected [][]interface{}) {
	ay := dumpRows(c, rows)
	c.Assert(len(ay), Equals, len(expected))
	for i := range ay {
		match(c, ay[i], expected[i]...)
	}
}

func match(c *C, row []interface{}, expected ...interface{}) {
	c.Assert(len(row), Equals, len(expected))
	for i := range row {
		got := fmt.Sprintf("%v", row[i])
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
	}
}
