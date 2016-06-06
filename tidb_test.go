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

package tidb

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var store = flag.String("store", "memory", "registered store name, [memory, goleveldb, boltdb]")

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMainSuite{})

type testMainSuite struct {
	dbName         string
	createDBSQL    string
	dropDBSQL      string
	useDBSQL       string
	createTableSQL string
	selectSQL      string
}

type brokenStore struct{}

func (s *brokenStore) Open(schema string) (kv.Storage, error) {
	return nil, errors.New("try again later")
}

func (s *testMainSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.dbName = "test_main_db"
	s.createDBSQL = fmt.Sprintf("create database if not exists %s;", s.dbName)
	s.dropDBSQL = fmt.Sprintf("drop database %s;", s.dbName)
	s.useDBSQL = fmt.Sprintf("use %s;", s.dbName)
	s.createTableSQL = `
    CREATE TABLE tbl_test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));
    CREATE TABLE tbl_test1(id INT NOT NULL DEFAULT 2, name varchar(255), PRIMARY KEY(id), INDEX name(name));
    CREATE TABLE tbl_test2(id INT NOT NULL DEFAULT 3, name varchar(255), PRIMARY KEY(id));`
	s.selectSQL = `SELECT * from tbl_test;`
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.SetLevelByString("error")
}

func (s *testMainSuite) TearDownSuite(c *C) {
	defer testleak.AfterTest(c)()
	removeStore(c, s.dbName)
}

func checkResult(c *C, se Session, affectedRows uint64, insertID uint64) {
	gotRows := se.AffectedRows()
	c.Assert(gotRows, Equals, affectedRows)

	gotID := se.LastInsertID()
	c.Assert(gotID, Equals, insertID)
}

func (s *testMainSuite) TestConcurrent(c *C) {
	dbName := "test_concurrent_db"
	defer removeStore(c, dbName)
	store := newStore(c, dbName)
	se := newSession(c, store, dbName)
	defer store.Close()
	// create db
	createDBSQL := fmt.Sprintf("create database if not exists %s;", dbName)
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	useDBSQL := fmt.Sprintf("use %s;", dbName)
	createTableSQL := ` CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id)); `

	mustExecSQL(c, se, dropDBSQL)
	mustExecSQL(c, se, createDBSQL)
	mustExecSQL(c, se, useDBSQL)
	mustExecSQL(c, se, createTableSQL)
	wg := &sync.WaitGroup{}
	f := func(start, count int) {
		sess := newSession(c, store, dbName)
		for i := 0; i < count; i++ {
			// insert data
			mustExecSQL(c, sess, fmt.Sprintf(`INSERT INTO test VALUES (%d, "hello");`, start+i))
		}
		wg.Done()
	}
	step := 10
	for i := 0; i < step; i++ {
		wg.Add(1)
		go f(i*step, step)
	}
	wg.Wait()
	mustExecSQL(c, se, dropDBSQL)
}

func (s *testMainSuite) TestTableInfoMeta(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	defer store.Close()

	// create db
	mustExecSQL(c, se, s.createDBSQL)

	// use db
	mustExecSQL(c, se, s.useDBSQL)

	// create table
	mustExecSQL(c, se, s.createTableSQL)

	// insert data
	mustExecSQL(c, se, `INSERT INTO tbl_test VALUES (1, "hello");`)
	checkResult(c, se, 1, 0)

	mustExecSQL(c, se, `INSERT INTO tbl_test VALUES (2, "hello");`)
	checkResult(c, se, 1, 0)

	mustExecSQL(c, se, `UPDATE tbl_test SET name = "abc" where id = 2;`)
	checkResult(c, se, 1, 0)

	mustExecSQL(c, se, `DELETE from tbl_test where id = 2;`)
	checkResult(c, se, 1, 0)

	// select data
	mustExecMatch(c, se, s.selectSQL, [][]interface{}{{1, []byte("hello")}})

	// drop db
	mustExecSQL(c, se, s.dropDBSQL)
}

func (s *testMainSuite) TestInfoSchema(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	rs := mustExecSQL(c, se, "SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'")
	row, err := rs.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "utf8mb4")

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testMainSuite) TestCaseInsensitive(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	defer store.Close()
	mustExecSQL(c, se, "create table T (a text, B int)")
	mustExecSQL(c, se, "insert t (A, b) values ('aaa', 1)")
	rs := mustExecSQL(c, se, "select * from t")
	fields, err := rs.Fields()
	c.Assert(err, IsNil)
	c.Assert(fields[0].ColumnAsName.O, Equals, "a")
	c.Assert(fields[1].ColumnAsName.O, Equals, "B")
	rs = mustExecSQL(c, se, "select A, b from t")
	fields, err = rs.Fields()
	c.Assert(err, IsNil)
	c.Assert(fields[0].ColumnAsName.O, Equals, "A")
	c.Assert(fields[1].ColumnAsName.O, Equals, "b")
	mustExecSQL(c, se, "update T set b = B + 1")
	mustExecSQL(c, se, "update T set B = b + 1")
	rs = mustExecSQL(c, se, "select b from T")
	rows, err := GetRows(rs)
	c.Assert(err, IsNil)
	match(c, rows[0], 3)

	mustExecSQL(c, se, s.dropDBSQL)
	err = store.Close()
	c.Assert(err, IsNil)
}

// Testcase for delete panic
func (s *testMainSuite) TestDeletePanic(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	defer store.Close()
	mustExecSQL(c, se, "create table t (c int)")
	mustExecSQL(c, se, "insert into t values (1), (2), (3)")
	mustExecSQL(c, se, "delete from `t` where `c` = ?", 1)
	rs := mustExecSQL(c, se, "delete from `t` where `c` = ?", 2)
	c.Assert(rs, IsNil)
}

// Testcase for arg type.
func (s *testMainSuite) TestCheckArgs(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	defer store.Close()
	mustExecSQL(c, se, "create table if not exists t (c datetime)")
	mustExecSQL(c, se, "insert t values (?)", time.Now())
	mustExecSQL(c, se, "drop table t")
	checkArgs(nil, true, false, int8(1), int16(1), int32(1), int64(1), 1,
		uint8(1), uint16(1), uint32(1), uint64(1), uint(1), float32(1), float64(1),
		"abc", []byte("abc"), time.Now(), time.Hour, time.Local)
}

func (s *testMainSuite) TestIsQuery(c *C) {
	tbl := []struct {
		sql string
		ok  bool
	}{
		{"/*comment*/ select 1;", true},
		{"/*comment*/ /*comment*/ select 1;", true},
		{"select /*comment*/ 1 /*comment*/;", true},
		{"(select /*comment*/ 1 /*comment*/);", true},
	}
	for _, t := range tbl {
		c.Assert(IsQuery(t.sql), Equals, t.ok, Commentf(t.sql))
	}
}

func (s *testMainSuite) TestTrimSQL(c *C) {
	tbl := []struct {
		sql    string
		target string
	}{
		{"/*comment*/ select 1; ", "select 1;"},
		{"/*comment*/ /*comment*/ select 1;", "select 1;"},
		{"select /*comment*/ 1 /*comment*/;", "select /*comment*/ 1 /*comment*/;"},
		{"/*comment select 1; ", "/*comment select 1;"},
	}
	for _, t := range tbl {
		c.Assert(trimSQL(t.sql), Equals, t.target, Commentf(t.sql))
	}
}

func (s *testMainSuite) TestRetryOpenStore(c *C) {
	begin := time.Now()
	RegisterStore("dummy", &brokenStore{})
	_, err := newStoreWithRetry("dummy://dummy-store", 3)
	c.Assert(err, NotNil)
	elapse := time.Since(begin)
	c.Assert(uint64(elapse), GreaterEqual, uint64(3*time.Second))
}

func (s *testMainSuite) TestParseDSN(c *C) {
	tbl := []struct {
		dsn      string
		ok       bool
		storeDSN string
		dbName   string
	}{
		{"s://path/db", true, "s://path", "db"},
		{"s://path/db/", true, "s://path", "db"},
		{"s:///path/db", true, "s:///path", "db"},
		{"s:///path/db/", true, "s:///path", "db"},
		{"s://zk1,zk2/tbl/db", true, "s://zk1,zk2/tbl", "db"},
		{"s://zk1:80,zk2:81/tbl/db", true, "s://zk1:80,zk2:81/tbl", "db"},
		{"s://path/db?p=v", true, "s://path?p=v", "db"},
		{"s:///path/db?p1=v1&p2=v2", true, "s:///path?p1=v1&p2=v2", "db"},
		{"s://z,k,zk/tbl/db?p=v", true, "s://z,k,zk/tbl?p=v", "db"},
		{"", false, "", ""},
		{"/", false, "", ""},
		{"s://", false, "", ""},
		{"s:///", false, "", ""},
		{"s:///db", false, "", ""},
	}

	for _, t := range tbl {
		params, err := parseDriverDSN(t.dsn)
		if t.ok {
			c.Assert(err, IsNil, Commentf("dsn=%v", t.dsn))
			c.Assert(params.storePath, Equals, t.storeDSN, Commentf("dsn=%v", t.dsn))
			c.Assert(params.dbName, Equals, t.dbName, Commentf("dsn=%v", t.dsn))
			_, err = url.Parse(params.storePath)
			c.Assert(err, IsNil, Commentf("dsn=%v", t.dsn))
		} else {
			c.Assert(err, NotNil, Commentf("dsn=%v", t.dsn))
		}
	}
}

func (s *testMainSuite) TestTPS(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	defer store.Close()

	mustExecSQL(c, se, "set @@autocommit=0;")
	for i := 1; i < 6; i++ {
		for j := 0; j < 5; j++ {
			for k := 0; k < i; k++ {
				mustExecSQL(c, se, "begin;")
				mustExecSQL(c, se, "select 1;")
				mustExecSQL(c, se, "commit;")
			}
			time.Sleep(220 * time.Millisecond)
		}
		// It is hard to get the accurate tps because there is another timeline in tpsMetrics.
		// We could only get the upper/lower boundary for tps
		c.Assert(GetTPS(), GreaterEqual, int64(4*(i-1)))
	}
}
func sessionExec(c *C, se Session, sql string) ([]ast.RecordSet, error) {
	se.Execute("BEGIN;")
	r, err := se.Execute(sql)
	c.Assert(err, IsNil)
	se.Execute("COMMIT;")
	return r, err
}

func newStore(c *C, dbPath string) kv.Storage {
	store, err := NewStore(*store + "://" + dbPath)
	c.Assert(err, IsNil)
	return store
}

func newSession(c *C, store kv.Storage, dbName string) Session {
	se, err := CreateSession(store)
	c.Assert(err, IsNil)
	se.Auth("root@%", nil, []byte("012345678901234567890"))
	mustExecSQL(c, se, "create database if not exists "+dbName)
	mustExecSQL(c, se, "use "+dbName)
	return se
}

func removeStore(c *C, dbPath string) {
	os.RemoveAll(dbPath)
}

func exec(c *C, se Session, sql string, args ...interface{}) (ast.RecordSet, error) {
	if len(args) == 0 {
		rs, err := se.Execute(sql)
		if err == nil && len(rs) > 0 {
			return rs[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := se.PrepareStmt(sql)
	if err != nil {
		return nil, err
	}
	rs, err := se.ExecutePreparedStmt(stmtID, args...)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func mustExecSQL(c *C, se Session, sql string, args ...interface{}) ast.RecordSet {
	rs, err := exec(c, se, sql, args...)
	c.Assert(err, IsNil)
	return rs
}

func match(c *C, row []types.Datum, expected ...interface{}) {
	c.Assert(len(row), Equals, len(expected))
	for i := range row {
		got := fmt.Sprintf("%v", row[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
	}
}

func matches(c *C, rows [][]types.Datum, expected [][]interface{}) {
	c.Assert(len(rows), Equals, len(expected))
	for i := 0; i < len(rows); i++ {
		match(c, rows[i], expected[i]...)
	}
}

func mustExecMatch(c *C, se Session, sql string, expected [][]interface{}) {
	r := mustExecSQL(c, se, sql)
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	matches(c, rows, expected)
}

func mustExecFailed(c *C, se Session, sql string, args ...interface{}) {
	r, err := exec(c, se, sql, args...)
	if err == nil && r != nil {
		// sometimes we may meet error after executing first row.
		_, err = r.Next()
	}
	c.Assert(err, NotNil)
}
