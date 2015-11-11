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

package ddl_test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	_ "github.com/pingcap/tidb"
)

var _ = Suite(&testDBSuite{})

type testDBSuite struct {
	db *sql.DB
}

func (s *testDBSuite) SetUpSuite(c *C) {
	var err error
	s.db, err = sql.Open("tidb", "memory://test/test_db")
	c.Assert(err, IsNil)

	// force bootstrap
	s.mustExec(c, "use test_db")

	s.mustExec(c, "create table t1 (c1 int, c2 int, c3 int, primary key(c1))")
	s.mustExec(c, "create table t2 (c1 int, c2 int, c3 int)")

	// set schema lease to 1s, so we may use about 8s to finish a schema change.
	tidb.SetSchemaLease(1)
}

func (s *testDBSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testDBSuite) TestIndex(c *C) {
	s.testAddIndex(c)
	s.testDropIndex(c)
}

func (s *testDBSuite) testAddIndex(c *C) {
	done := make(chan struct{}, 1)

	num := 100
	// first add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}

	go func() {
		s.mustExec(c, "create index c3_index on t1 (c3)")
		done <- struct{}{}
	}()

	deletedKeys := make(map[int]struct{})

	ticker := time.NewTicker(time.Second / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case <-ticker.C:
			step := 10
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				deletedKeys[n] = struct{}{}
				s.mustExec(c, "delete from t1 where c1 = ?", n)
				s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	// get exists keys
	keys := make([]int, 0, num)
	for i := 0; i < num; i++ {
		if _, ok := deletedKeys[i]; ok {
			continue
		}
		keys = append(keys, i)
	}

	// test index key
	for _, key := range keys {
		rows := s.mustQuery(c, "select c1 from t1 where c3 = ?", key)
		matchRows(c, rows, [][]interface{}{{key}})
	}

	// test delete key not in index
	for key := range deletedKeys {
		rows := s.mustQuery(c, "select c1 from t1 where c3 = ?", key)
		matchRows(c, rows, nil)
	}

	// test index range
	for i := 0; i < 100; i++ {
		index := rand.Intn(len(keys) - 3)
		rows := s.mustQuery(c, "select c1 from t1 where c3 >= ? limit 3", keys[index])
		matchRows(c, rows, [][]interface{}{{keys[index]}, {keys[index+1]}, {keys[index+2]}})
	}

	rows := s.mustQuery(c, "explain select c1 from t1 where c3 >= 100")

	ay := dumpRows(c, rows)
	c.Assert(strings.Contains(fmt.Sprintf("%v", ay), "c3_index"), IsTrue)
}

func (s *testDBSuite) testDropIndex(c *C) {
	done := make(chan struct{}, 1)

	s.mustExec(c, "delete from t1")

	num := 100
	//  add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}

	go func() {
		s.mustExec(c, "drop index c3_index on t1")
		done <- struct{}{}
	}()

	ticker := time.NewTicker(time.Second / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case <-ticker.C:
			step := 10
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				s.mustExec(c, "update t1 set c2 = 1 where c1 = ?", n)
				s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	rows := s.mustQuery(c, "explain select c1 from t1 where c3 >= 0")

	ay := dumpRows(c, rows)
	c.Assert(strings.Contains(fmt.Sprintf("%v", ay), "c3_index"), IsFalse)
}

func (s *testDBSuite) showColumns(c *C, tableName string) [][]interface{} {
	rows := s.mustQuery(c, fmt.Sprintf("show columns from %s", tableName))
	values := dumpRows(c, rows)
	return values
}

func (s *testDBSuite) TestColumn(c *C) {
	s.testAddColumn(c)
	s.testDropColumn(c)
}

func (s *testDBSuite) testAddColumn(c *C) {
	done := make(chan struct{}, 1)

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?)", i, i, i)
	}

	go func() {
		s.mustExec(c, "alter table t2 add column c4 int default -1")
		done <- struct{}{}
	}()

	ticker := time.NewTicker(time.Second / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				// we may meet condition not match here
				_, err := s.db.Exec("delete from t2 where c1 = ?", n)
				c.Assert(err, IsNil, Commentf("%s", errors.ErrorStack(err)))

				_, err = s.db.Exec("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// if err is failed, the column number must be 4 now.
					values := s.showColumns(c, "t2")
					c.Assert(values, HasLen, 4)
				}
			}
			num += step
		}
	}

	// add data, here c4 must exist
	for i := num; i < num+step; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	rows := s.mustQuery(c, "select count(c4) from t2")
	values := dumpRows(c, rows)
	c.Assert(values, HasLen, 1)
	c.Assert(values[0], HasLen, 1)
	count, ok := values[0][0].(int64)
	c.Assert(ok, IsTrue)

	rows = s.mustQuery(c, "select count(c4) from t2 where c4 = -1")
	matchRows(c, rows, [][]interface{}{{count - int64(step)}})

	for i := num; i < num+step; i++ {
		rows := s.mustQuery(c, "select c4 from t2 where c4 = ?", i)
		matchRows(c, rows, [][]interface{}{{i}})
	}
}

func (s *testDBSuite) testDropColumn(c *C) {
	done := make(chan struct{}, 1)

	s.mustExec(c, "delete from t2")

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	go func() {
		s.mustExec(c, "alter table t2 drop column c4")
		done <- struct{}{}
	}()

	ticker := time.NewTicker(time.Second / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				_, err := s.db.Exec("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// if err is failed, the column number must be 4 now.
					values := s.showColumns(c, "t2")
					c.Assert(values, HasLen, 4)
				}
			}
			num += step
		}
	}

	// add data, here c4 must not exist
	for i := num; i < num+step; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?)", i, i, i)
	}
}

func (s *testDBSuite) mustExec(c *C, query string, args ...interface{}) sql.Result {
	r, err := s.db.Exec(query, args...)
	c.Assert(err, IsNil, Commentf("query %s, args %v", query, args))
	return r
}

func (s *testDBSuite) mustQuery(c *C, query string, args ...interface{}) *sql.Rows {
	r, err := s.db.Query(query, args...)
	c.Assert(err, IsNil, Commentf("query %s, args %v", query, args))
	return r
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
	c.Assert(rows.Err(), IsNil, Commentf("%v", ay))
	return ay
}

func matchRows(c *C, rows *sql.Rows, expected [][]interface{}) {
	ay := dumpRows(c, rows)
	c.Assert(len(ay), Equals, len(expected), Commentf("%v", expected))
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
