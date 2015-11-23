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
	"database/sql"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/rset"
)

var store = flag.String("store", "memory", "registered store name, [memory, goleveldb, boltdb]")

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMainSuite{})

type testMainSuite struct {
	dbName string

	createDBSQL    string
	dropDBSQL      string
	useDBSQL       string
	createTableSQL string
	insertSQL      string
	selectSQL      string
}

func (s *testMainSuite) SetUpSuite(c *C) {
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
	removeStore(c, s.dbName)
}

func mustBegin(c *C, currDB *sql.DB) *sql.Tx {
	tx, err := currDB.Begin()
	c.Assert(err, IsNil)
	return tx
}

func mustCommit(c *C, tx *sql.Tx) {
	err := tx.Commit()
	c.Assert(err, IsNil)
}

func mustExecuteSQL(c *C, tx *sql.Tx, sql string, args ...interface{}) sql.Result {
	r, err := tx.Exec(sql, args...)
	c.Assert(err, IsNil)
	return r
}

func mustExec(c *C, currDB *sql.DB, sql string, args ...interface{}) sql.Result {
	tx := mustBegin(c, currDB)
	r := mustExecuteSQL(c, tx, sql, args...)
	mustCommit(c, tx)
	return r
}

func checkResult(c *C, r sql.Result, affectedRows int64, insertID int64) {
	gotRows, err := r.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(gotRows, Equals, affectedRows)

	gotID, err := r.LastInsertId()
	c.Assert(err, IsNil)
	c.Assert(gotID, Equals, insertID)
}

func (s *testMainSuite) TestConcurrent(c *C) {
	dbName := "test_concurrent_db"
	defer removeStore(c, dbName)

	testDB, err := sql.Open(DriverName, *store+"://"+dbName+"/"+dbName)
	c.Assert(err, IsNil)
	defer testDB.Close()

	var wg sync.WaitGroup
	// create db
	createDBSQL := fmt.Sprintf("create database if not exists %s;", dbName)
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	useDBSQL := fmt.Sprintf("use %s;", dbName)
	createTableSQL := ` CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id)); `

	mustExec(c, testDB, dropDBSQL)
	mustExec(c, testDB, createDBSQL)
	mustExec(c, testDB, useDBSQL)
	mustExec(c, testDB, createTableSQL)

	f := func(start, count int) {
		defer wg.Done()
		for i := 0; i < count; i++ {
			// insert data
			mustExec(c, testDB, fmt.Sprintf(`INSERT INTO test VALUES (%d, "hello");`, start+i))
		}
	}

	step := 10
	for i := 0; i < step; i++ {
		wg.Add(1)
		go f(i*step, step)
	}
	wg.Wait()
	mustExec(c, testDB, dropDBSQL)
}

func (s *testMainSuite) TestTableInfoMeta(c *C) {
	testDB, err := sql.Open(DriverName, *store+"://"+s.dbName+"/"+s.dbName)
	c.Assert(err, IsNil)
	defer testDB.Close()

	// create db
	mustExec(c, testDB, s.createDBSQL)

	// use db
	mustExec(c, testDB, s.useDBSQL)

	// create table
	mustExec(c, testDB, s.createTableSQL)

	// insert data
	r := mustExec(c, testDB, `INSERT INTO tbl_test VALUES (1, "hello");`)
	checkResult(c, r, 1, 0)

	r = mustExec(c, testDB, `INSERT INTO tbl_test VALUES (2, "hello");`)
	checkResult(c, r, 1, 0)

	r = mustExec(c, testDB, `UPDATE tbl_test SET name = "abc" where id = 2;`)
	checkResult(c, r, 1, 0)

	r = mustExec(c, testDB, `DELETE from tbl_test where id = 2;`)
	checkResult(c, r, 1, 0)

	// select data
	tx := mustBegin(c, testDB)
	rows, err := tx.Query(s.selectSQL)
	c.Assert(err, IsNil)
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		c.Assert(id, Equals, 1)
		c.Assert(name, Equals, "hello")
	}

	mustCommit(c, tx)

	// drop db
	mustExec(c, testDB, s.dropDBSQL)
}

func (s *testMainSuite) TestInfoSchema(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	rs := mustExecSQL(c, se, "SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'")
	row, err := rs.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "utf8mb4")
}

func (s *testMainSuite) TestCaseInsensitive(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "create table T (a text, B int)")
	mustExecSQL(c, se, "insert t (A, b) values ('aaa', 1)")
	rs := mustExecSQL(c, se, "select * from t")
	fields, err := rs.Fields()
	c.Assert(err, IsNil)
	c.Assert(fields[0].Name, Equals, "a")
	c.Assert(fields[1].Name, Equals, "B")
	rs = mustExecSQL(c, se, "select A, b from t")
	fields, err = rs.Fields()
	c.Assert(err, IsNil)
	c.Assert(fields[0].Name, Equals, "A")
	c.Assert(fields[1].Name, Equals, "b")
	mustExecSQL(c, se, "update T set b = B + 1")
	mustExecSQL(c, se, "update T set B = b + 1")
	rs = mustExecSQL(c, se, "select b from T")
	rows, err := rs.Rows(-1, 0)
	c.Assert(err, IsNil)
	match(c, rows[0], 3)
	mustExecSQL(c, se, s.dropDBSQL)
}

func (s *testMainSuite) TestDriverPrepare(c *C) {
	testDB, err := sql.Open(DriverName, *store+"://"+s.dbName+"/"+s.dbName)
	c.Assert(err, IsNil)
	defer testDB.Close()

	// create db
	mustExec(c, testDB, s.createDBSQL)

	// use db
	mustExec(c, testDB, s.useDBSQL)

	// create table
	mustExec(c, testDB, "create table t (a int, b int)")
	mustExec(c, testDB, "insert t values (?, ?)", 1, 2)
	row := testDB.QueryRow("select * from t where 1 = ?", 1)
	// TODO: This will fail test
	// row := testDB.QueryRow("select ?, ? from t", "a", "b")
	var a, b int
	err = row.Scan(&a, &b)
	c.Assert(err, IsNil)
	c.Assert(a, Equals, 1)
	c.Assert(b, Equals, 2)
	mustExec(c, testDB, s.dropDBSQL)
}

// Testcase for delete panic
func (s *testMainSuite) TestDeletePanic(c *C) {
	db, err := sql.Open("tidb", "memory://test/test")
	defer db.Close()
	_, err = db.Exec("create table t (c int)")
	c.Assert(err, IsNil)
	_, err = db.Exec("insert into t values (1), (2), (3)")
	c.Assert(err, IsNil)
	_, err = db.Query("delete from `t` where `c` = ?", 1)
	c.Assert(err, IsNil)
	rs, err := db.Query("delete from `t` where `c` = ?", 2)
	c.Assert(err, IsNil)
	cols, err := rs.Columns()
	c.Assert(err, IsNil)
	c.Assert(cols, HasLen, 0)
	c.Assert(rs.Next(), IsFalse)
	c.Assert(rs.Next(), IsFalse)
	c.Assert(rs.Close(), IsNil)
}

// Testcase for arg type.
func (s *testMainSuite) TestCheckArgs(c *C) {
	db, err := sql.Open("tidb", "memory://test/test")
	defer db.Close()
	c.Assert(err, IsNil)
	mustExec(c, db, "create table if not exists t (c datetime)")
	mustExec(c, db, "insert t values (?)", time.Now())
	mustExec(c, db, "drop table t")
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

func (s *testMainSuite) TestitrimSQL(c *C) {
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

func sessionExec(c *C, se Session, sql string) ([]rset.Recordset, error) {
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
	mustExecSQL(c, se, "create database if not exists "+dbName)
	mustExecSQL(c, se, "use "+dbName)
	return se
}

func removeStore(c *C, dbPath string) {
	os.RemoveAll(dbPath)
}

func exec(c *C, se Session, sql string, args ...interface{}) (rset.Recordset, error) {
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
	err = se.DropPreparedStmt(stmtID)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func mustExecSQL(c *C, se Session, sql string, args ...interface{}) rset.Recordset {
	rs, err := exec(c, se, sql, args...)
	c.Assert(err, IsNil)
	return rs
}

func match(c *C, row []interface{}, expected ...interface{}) {
	c.Assert(len(row), Equals, len(expected))
	for i := range row {
		got := fmt.Sprintf("%v", row[i])
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
	}
}

func matches(c *C, rows [][]interface{}, expected [][]interface{}) {
	c.Assert(len(rows), Equals, len(expected))
	for i := 0; i < len(rows); i++ {
		match(c, rows[i], expected[i]...)
	}
}

func mustExecMatch(c *C, se Session, sql string, expected [][]interface{}) {
	r := mustExecSQL(c, se, sql)
	rows, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	matches(c, rows, expected)
}

func mustExecFailed(c *C, se Session, sql string, args ...interface{}) {
	r, err := exec(c, se, sql, args...)
	if err == nil {
		// sometimes we may meet error after executing first row.
		_, err = r.FirstRow()
	}
	c.Assert(err, NotNil)
}
