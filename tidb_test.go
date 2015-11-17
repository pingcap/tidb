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
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var store = flag.String("store", "memory", "registered store name, [memory, goleveldb, boltdb]")

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMainSuite{})
var _ = Suite(&testSessionSuite{})

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

type testSessionSuite struct {
	dbName          string
	dbNameBootstrap string

	createDBSQL    string
	dropDBSQL      string
	useDBSQL       string
	createTableSQL string
	dropTableSQL   string
	insertSQL      string
	selectSQL      string
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

func (s *testSessionSuite) SetUpSuite(c *C) {
	s.dbName = "test_session_db"
	s.dbNameBootstrap = "test_main_db_bootstrap"
	s.createDBSQL = fmt.Sprintf("create database if not exists %s;", s.dbName)
	s.dropDBSQL = fmt.Sprintf("drop database %s;", s.dbName)
	s.useDBSQL = fmt.Sprintf("use %s;", s.dbName)
	s.dropTableSQL = `Drop TABLE if exists t;`
	s.createTableSQL = `CREATE TABLE t(id TEXT);`
	s.selectSQL = `SELECT * from t;`
}

func (s *testSessionSuite) TearDownSuite(c *C) {
	removeStore(c, s.dbName)
}

func (s *testSessionSuite) TestPrepare(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	// create table
	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, s.createTableSQL)
	// insert data
	mustExecSQL(c, se, `INSERT INTO t VALUES ("id");`)
	id, ps, _, err := se.PrepareStmt("select id+? from t")
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	c.Assert(ps, Equals, 1)
	mustExecSQL(c, se, `set @a=1`)
	_, err = se.ExecutePreparedStmt(id, "1")
	c.Assert(err, IsNil)
	err = se.DropPreparedStmt(id)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, s.dropDBSQL)
}

func (s *testSessionSuite) TestAffectedRows(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, s.createTableSQL)
	mustExecSQL(c, se, `INSERT INTO t VALUES ("a");`)
	c.Assert(int(se.AffectedRows()), Equals, 1)
	mustExecSQL(c, se, `INSERT INTO t VALUES ("b");`)
	c.Assert(int(se.AffectedRows()), Equals, 1)
	mustExecSQL(c, se, `UPDATE t set id = 'c' where id = 'a';`)
	c.Assert(int(se.AffectedRows()), Equals, 1)
	mustExecSQL(c, se, `UPDATE t set id = 'a' where id = 'a';`)
	c.Assert(int(se.AffectedRows()), Equals, 0)
	mustExecSQL(c, se, `SELECT * from t;`)
	c.Assert(int(se.AffectedRows()), Equals, 0)

	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, "create table t (id int, data int)")
	mustExecSQL(c, se, `INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	mustExecSQL(c, se, `UPDATE t set id = 1 where data = 0;`)
	c.Assert(int(se.AffectedRows()), Equals, 1)

	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, "create table t (id int, c1 timestamp);")
	mustExecSQL(c, se, `insert t values(1, 0);`)
	mustExecSQL(c, se, `UPDATE t set id = 1 where id = 1;`)
	c.Assert(int(se.AffectedRows()), Equals, 0)

	// With ON DUPLICATE KEY UPDATE, the affected-rows value per row is 1 if the row is inserted as a new row,
	// 2 if an existing row is updated, and 0 if an existing row is set to its current values.
	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, "create table t (c1 int PRIMARY KEY, c2 int);")
	mustExecSQL(c, se, `insert t values(1, 1);`)
	mustExecSQL(c, se, `insert into t values (1, 1) on duplicate key update c2=2;`)
	c.Assert(int(se.AffectedRows()), Equals, 2)
	mustExecSQL(c, se, `insert into t values (1, 1) on duplicate key update c2=2;`)
	c.Assert(int(se.AffectedRows()), Equals, 0)

	se.SetClientCapability(mysql.ClientFoundRows)
	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, "create table t (id int, data int)")
	mustExecSQL(c, se, `INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	mustExecSQL(c, se, `UPDATE t set id = 1 where data = 0;`)
	c.Assert(int(se.AffectedRows()), Equals, 2)

	sessionExec(c, se, s.dropDBSQL)

}

func (s *testSessionSuite) TestString(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	sessionExec(c, se, "select 1")
	// here to check the panic bug in String() when txn is nil after committed.
	c.Log(se.String())
}

func (s *testSessionSuite) TestResultField(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	// create table
	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, "create table t (id int);")

	mustExecSQL(c, se, `INSERT INTO t VALUES (1);`)
	mustExecSQL(c, se, `INSERT INTO t VALUES (2);`)
	r := mustExecSQL(c, se, `SELECT count(*) from t;`)
	c.Assert(r, NotNil)
	_, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	fields, err := r.Fields()
	c.Assert(err, IsNil)
	c.Assert(len(fields), Equals, 1)
	field := fields[0]
	c.Assert(field.Tp, Equals, mysql.TypeLonglong)
	c.Assert(field.Flen, Equals, 21)
	mustExecSQL(c, se, s.dropDBSQL)
}

func (s *testSessionSuite) TestPrimaryKeyAutoincrement(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL, name varchar(255) UNIQUE NOT NULL, status int)")
	mustExecSQL(c, se, "insert t (name) values (?)", "abc")
	id := se.LastInsertID()
	c.Check(id != 0, IsTrue)

	se2 := newSession(c, store, s.dbName)
	rs := mustExecSQL(c, se2, "select * from t")
	c.Assert(rs, NotNil)
	row, err := rs.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, id, "abc", nil)

	mustExecSQL(c, se, "update t set name = 'abc', status = 1 where id = ?", id)
	rs = mustExecSQL(c, se2, "select * from t")
	c.Assert(rs, NotNil)
	row, err = rs.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, id, "abc", 1)
	// Check for pass bool param to tidb prepared statement
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id tiny)")
	mustExecSQL(c, se, "insert t values (?)", true)
	rs = mustExecSQL(c, se, "select * from t")
	c.Assert(rs, NotNil)
	row, err = rs.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, int8(1))

	mustExecSQL(c, se, s.dropDBSQL)
}

func (s *testSessionSuite) TestAutoincrementID(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	mustExecSQL(c, se, "insert t values ()")
	mustExecSQL(c, se, "insert t values ()")
	mustExecSQL(c, se, "insert t values ()")
	se.Execute("drop table if exists t;")
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	mustExecSQL(c, se, "insert t values ()")
	lastID := se.LastInsertID()
	c.Assert(lastID, Less, uint64(4))
	mustExecSQL(c, se, "insert t () values ()")
	c.Assert(se.LastInsertID(), Greater, lastID)
	mustExecSQL(c, se, "insert t () select 100")
	mustExecSQL(c, se, s.dropDBSQL)
}

func checkTxn(c *C, se Session, stmt string, expect uint16) {
	mustExecSQL(c, se, stmt)
	if expect == 0 {
		c.Assert(se.(*session).txn, IsNil)
		return
	}
	c.Assert(se.(*session).txn, NotNil)
}

func checkAutocommit(c *C, se Session, expect uint16) {
	ret := variable.GetSessionVars(se.(*session)).Status & mysql.ServerStatusAutocommit
	c.Assert(ret, Equals, expect)
}

// See: https://dev.mysql.com/doc/internals/en/status-flags.html
func (s *testSessionSuite) TestAutocommit(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	checkTxn(c, se, "drop table if exists t;", 0)
	checkAutocommit(c, se, 2)
	checkTxn(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)", 0)
	checkAutocommit(c, se, 2)
	checkTxn(c, se, "insert t values ()", 0)
	checkAutocommit(c, se, 2)
	checkTxn(c, se, "begin", 1)
	checkAutocommit(c, se, 2)
	checkTxn(c, se, "insert t values ()", 1)
	checkAutocommit(c, se, 2)
	checkTxn(c, se, "drop table if exists t;", 0)
	checkAutocommit(c, se, 2)

	checkTxn(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)", 0)
	checkAutocommit(c, se, 2)
	checkTxn(c, se, "set autocommit=0;", 0)
	checkAutocommit(c, se, 0)
	checkTxn(c, se, "insert t values ()", 1)
	checkAutocommit(c, se, 0)
	checkTxn(c, se, "commit", 0)
	checkAutocommit(c, se, 0)
	checkTxn(c, se, "drop table if exists t;", 0)
	checkAutocommit(c, se, 0)
	checkTxn(c, se, "set autocommit=1;", 0)
	checkAutocommit(c, se, 2)

	mustExecSQL(c, se, s.dropDBSQL)
}

func checkInTrans(c *C, se Session, stmt string, expect uint16) {
	checkTxn(c, se, stmt, expect)
	ret := variable.GetSessionVars(se.(*session)).Status & mysql.ServerStatusInTrans
	c.Assert(ret, Equals, expect)
}

// See: https://dev.mysql.com/doc/internals/en/status-flags.html
func (s *testSessionSuite) TestInTrans(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	checkInTrans(c, se, "drop table if exists t;", 0)
	checkInTrans(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)", 0)
	checkInTrans(c, se, "insert t values ()", 0)
	checkInTrans(c, se, "begin", 1)
	checkInTrans(c, se, "insert t values ()", 1)
	checkInTrans(c, se, "drop table if exists t;", 0)
	checkInTrans(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)", 0)
	checkInTrans(c, se, "insert t values ()", 0)
	checkInTrans(c, se, "commit", 0)
	checkInTrans(c, se, "insert t values ()", 0)

	checkInTrans(c, se, "set autocommit=O;", 0)
	checkInTrans(c, se, "begin", 1)
	checkInTrans(c, se, "insert t values ()", 1)
	checkInTrans(c, se, "commit", 0)
	checkInTrans(c, se, "insert t values ()", 1)
	checkInTrans(c, se, "commit", 0)

	checkInTrans(c, se, "set autocommit=1;", 0)
	checkInTrans(c, se, "drop table if exists t;", 0)
	checkInTrans(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)", 0)
	checkInTrans(c, se, "begin", 1)
	checkInTrans(c, se, "insert t values ()", 1)
	checkInTrans(c, se, "rollback", 0)

	mustExecSQL(c, se, s.dropDBSQL)
}

// See: http://dev.mysql.com/doc/refman/5.7/en/commit.html
func (s *testSessionSuite) TestRowLock(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	se1 := newSession(c, store, s.dbName)
	se2 := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int)")
	mustExecSQL(c, se, "insert t values (11, 2, 3)")
	mustExecSQL(c, se, "insert t values (12, 2, 3)")
	mustExecSQL(c, se, "insert t values (13, 2, 3)")

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2=21 where c1=11")

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=211 where c1=11")
	mustExecSQL(c, se2, "commit")

	_, err := exec(c, se1, "commit")
	// se1 will retry and the final value is 21
	c.Assert(err, IsNil)
	// Check the result is correct
	se3 := newSession(c, store, s.dbName)
	r := mustExecSQL(c, se3, "select c2 from t where c1=11")
	rows, err := r.Rows(-1, 0)
	matches(c, rows, [][]interface{}{{21}})

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2=21 where c1=11")

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=22 where c1=12")
	mustExecSQL(c, se2, "commit")

	mustExecSQL(c, se1, "commit")

	mustExecSQL(c, se, s.dropDBSQL)
}

func (s *testSessionSuite) TestSelectForUpdate(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	se1 := newSession(c, store, s.dbName)
	se2 := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int)")
	mustExecSQL(c, se, "insert t values (11, 2, 3)")
	mustExecSQL(c, se, "insert t values (12, 2, 3)")
	mustExecSQL(c, se, "insert t values (13, 2, 3)")

	// conflict
	mustExecSQL(c, se1, "begin")
	rs, err := exec(c, se1, "select * from t where c1=11 for update")
	c.Assert(err, IsNil)
	_, err = rs.Rows(-1, 0)

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=211 where c1=11")
	mustExecSQL(c, se2, "commit")

	_, err = exec(c, se1, "commit")
	c.Assert(err, NotNil)
	err = se1.Retry()
	// retry should fail
	c.Assert(err, NotNil)

	// not conflict
	mustExecSQL(c, se1, "begin")
	rs, err = exec(c, se1, "select * from t where c1=11 for update")
	_, err = rs.Rows(-1, 0)

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=22 where c1=12")
	mustExecSQL(c, se2, "commit")

	mustExecSQL(c, se1, "commit")

	// not conflict, auto commit
	mustExecSQL(c, se1, "set @@autocommit=1;")
	rs, err = exec(c, se1, "select * from t where c1=11 for update")
	_, err = rs.Rows(-1, 0)

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=211 where c1=11")
	mustExecSQL(c, se2, "commit")

	_, err = exec(c, se1, "commit")
	c.Assert(err, IsNil)

	mustExecSQL(c, se, s.dropDBSQL)
	err = se.Close()
	c.Assert(err, IsNil)
	err = se1.Close()
	c.Assert(err, IsNil)
	err = se2.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestRow(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	r := mustExecSQL(c, se, "select row(1, 1) in (row(1, 1))")
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1)

	r = mustExecSQL(c, se, "select row(1, 1) in (row(1, 0))")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 0)

	r = mustExecSQL(c, se, "select row(1, 1) in (select 1, 1)")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1)

	r = mustExecSQL(c, se, "select row(1, 1) > row(1, 0)")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1)

	r = mustExecSQL(c, se, "select row(1, 1) > (select 1, 0)")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1)

	r = mustExecSQL(c, se, "select 1 > (select 1)")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 0)

	r = mustExecSQL(c, se, "select (select 1)")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1)
}

func (s *testSessionSuite) TestIndex(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "create table if not exists test_index (c1 int, c double, index(c1), index(c))")
	mustExecSQL(c, se, "insert into test_index values (1, 2), (3, null)")
	r := mustExecSQL(c, se, "select c1 from test_index where c > 0")
	rows, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 1)
	match(c, rows[0], 1)

	mustExecSQL(c, se, "drop table if exists t1, t2")
	mustExecSQL(c, se, `
			create table t1 (c1 int, primary key(c1));
			create table t2 (c2 int, primary key(c2));
			insert into t1 values (1), (2);
			insert into t2 values (2);`)

	r = mustExecSQL(c, se, "select * from t1 left join t2 on t1.c1 = t2.c2 order by t1.c1")
	rows, err = r.Rows(-1, 0)
	matches(c, rows, [][]interface{}{{1, nil}, {2, 2}})

	r = mustExecSQL(c, se, "select * from t1 left join t2 on t1.c1 = t2.c2 where t2.c2 < 10")
	rows, err = r.Rows(-1, 0)
	matches(c, rows, [][]interface{}{{2, 2}})
}

func (s *testSessionSuite) TestMySQLTypes(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	r := mustExecSQL(c, se, `select 0x01 + 1, x'4D7953514C' = "MySQL"`)
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 2, 1)

	r = mustExecSQL(c, se, `select 0b01 + 1, 0b01000001 = "A"`)
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 2, 1)
}

func (s *testSessionSuite) TestExpression(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	r := mustExecSQL(c, se, `select + (1 > 0), -(1 >0), + (1 < 0), - (1 < 0)`)
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1, -1, 0, 0)

	r = mustExecSQL(c, se, "select 1 <=> 1, 1 <=> null, null <=> null, null <=> (select null)")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1, 0, 1, 1)

}

func (s *testSessionSuite) TestSelect(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "create table if not exists t (c1 int, c2 int)")
	mustExecSQL(c, se, "create table if not exists t1 (c1 int, c2 int)")

	_, err := se.Execute("select * from t as a join t as a")
	c.Assert(err, NotNil)

	_, err = se.Execute("select * from t join t1 as t")
	c.Assert(err, NotNil)

	_, err = se.Execute("select * from t join test.t")
	c.Assert(err, NotNil)

	_, err = se.Execute("select * from t as a join (select 1) as a")
	c.Assert(err, IsNil)

	r := mustExecSQL(c, se, "select 1, 2 from dual")
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1, 2)

	r = mustExecSQL(c, se, "select 1, 2 from dual where not exists (select * from t where c1=2)")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1, 2)

	r = mustExecSQL(c, se, "select 1, 2")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1, 2)

	r = mustExecSQL(c, se, `select '''a''', """a""", 'pingcap ''-->'' tidb'`)
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, `'a'`, `"a"`, `pingcap '-->' tidb`)

	r = mustExecSQL(c, se, `select '\'a\'', "\"a\"";`)
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, `'a'`, `"a"`)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c varchar(20))")
	mustExecSQL(c, se, `insert t values("pingcap '-->' tidb")`)

	r = mustExecSQL(c, se, `select * from t where c like 'pingcap ''-->'' tidb'`)
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, `pingcap '-->' tidb`)

	mustExecSQL(c, se, "drop table if exists t1")
	mustExecSQL(c, se, "drop table if exists t2")
	mustExecSQL(c, se, "drop table if exists t3")
	mustExecSQL(c, se, "create table t1 (c1 int, c11 int)")
	mustExecSQL(c, se, "create table t2 (c2 int)")
	mustExecSQL(c, se, "create table t3 (c3 int)")
	mustExecSQL(c, se, "insert into t1 values (1, 1), (2, 2), (3, 3)")
	mustExecSQL(c, se, "insert into t2 values (1), (1), (2)")
	mustExecSQL(c, se, "insert into t3 values (1), (3)")

	r = mustExecSQL(c, se, "select * from t1 left join t2 on t1.c1 = t2.c2 left join t3 on t1.c1 = t3.c3 order by t1.c1, t2.c2, t3.c3")
	rows, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 4)
	match(c, rows[0], 1, 1, 1, 1)
	match(c, rows[1], 1, 1, 1, 1)
	match(c, rows[2], 2, 2, 2, nil)
	match(c, rows[3], 3, 3, nil, 3)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c float(8))")
	mustExecSQL(c, se, "insert into t values (3.12)")
	r = mustExecSQL(c, se, "select * from t")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 3.12)

	mustExecSQL(c, se, `drop table if exists t;create table t (c int);insert into t values (1);`)
	r = mustExecSQL(c, se, "select a.c from t as a where c between null and 2")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)

	mustExecSQL(c, se, "drop table if exists t1, t2, t3")
	mustExecSQL(c, se, `
		create table t1 (c1 int);
		create table t2 (c2 int);
		create table t3 (c3 int);
		insert into t1 values (1), (2);
		insert into t2 values (2);
		insert into t3 values (3);`)
	r = mustExecSQL(c, se, "select * from t1 left join t2 on t1.c1 = t2.c2 left join t3 on t1.c1 = t3.c3 order by t1.c1")
	rows, err = r.Rows(-1, 0)
	c.Assert(err, IsNil)
	matches(c, rows, [][]interface{}{{1, nil, nil}, {2, 2, nil}})

	mustExecFailed(c, se, "select * from t1 left join t2 on t1.c1 = t3.c3 left join on t3 on t1.c1 = t2.c2")

	// For issue 393
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (b blob)")
	mustExecSQL(c, se, `insert t values('\x01')`)

	r = mustExecSQL(c, se, `select length(b) from t`)
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 3)
}

func (s *testSessionSuite) TestSubQuery(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "create table if not exists t1 (c1 int, c2 int)")
	mustExecSQL(c, se, "create table if not exists t2 (c1 int, c2 int)")
	mustExecSQL(c, se, "insert into t1 values (1, 1), (2, 2)")
	mustExecSQL(c, se, "insert into t2 values (1, 1), (1, 2)")

	r := mustExecSQL(c, se, `select c1 from t1 where c1 = (select c2 from t2 where t1.c2 = t2.c2)`)
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, 1)

	r = mustExecSQL(c, se, `select (select count(c1) from t2 where t2.c1 != t1.c2) from t1`)
	rows, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 2)
	match(c, rows[0], 0)
	match(c, rows[1], 2)

	mustExecMatch(c, se, "select a.c1, a.c2 from (select c1 as c1, c1 as c2 from t1) as a", [][]interface{}{{1, 1}, {2, 2}})
}

func (s *testSessionSuite) TestShow(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "set global autocommit=1")
	r := mustExecSQL(c, se, "show global variables where variable_name = 'autocommit'")
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "autocommit", 1)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table if not exists t (c int)")
	r = mustExecSQL(c, se, `show columns from t`)
	rows, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 1)
	match(c, rows[0], "c", "int(11)", "YES", "", nil, "")

	r = mustExecSQL(c, se, "show collation where Charset = 'utf8' and Collation = 'utf8_bin'")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "utf8_bin", "utf8", 83, "", "Yes", 1)

	r = mustExecSQL(c, se, "show tables")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	c.Assert(row, HasLen, 1)

	r = mustExecSQL(c, se, "show full tables")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	c.Assert(row, HasLen, 2)

	r = mustExecSQL(c, se, "show create table t")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	c.Assert(row, HasLen, 2)
	c.Assert(row[0], Equals, "t")
}

func (s *testSessionSuite) TestTimeFunc(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	last := time.Now().Format(mysql.TimeFormat)
	r := mustExecSQL(c, se, "select now(), now(6), current_timestamp, current_timestamp(), current_timestamp(6), sysdate(), sysdate(6)")
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	for _, t := range row {
		n, ok := t.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(n.String(), GreaterEqual, last)
	}

	last = time.Now().Format(mysql.DateFormat)
	r = mustExecSQL(c, se, "select current_date, current_date(), curdate()")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	for _, t := range row {
		n, ok := t.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(n.String(), GreaterEqual, last)
	}
}

func (s *testSessionSuite) TestBit(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 bit(2))")
	mustExecSQL(c, se, "insert into t values (0), (1), (2), (3)")
	_, err := exec(c, se, "insert into t values (4)")
	c.Assert(err, NotNil)
	r := mustExecSQL(c, se, "select * from t where c1 = 2")
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	c.Assert(row[0], Equals, mysql.Bit{Value: 2, Width: 2})
}

func (s *testSessionSuite) TestBootstrap(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "USE mysql;")
	r := mustExecSQL(c, se, `select * from user;`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	match(c, row.Data, "localhost", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	match(c, row.Data, "127.0.0.1", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")
	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	mustExecSQL(c, se, "SELECT * from mysql.db;")
	mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	// Check privilege tables.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_variables;")
	v, err := r.FirstRow()
	c.Assert(err, IsNil)
	c.Assert(v[0], Equals, int64(len(variable.SysVars)))
}

// Create a new session on store but only do ddl works.
func (s *testSessionSuite) bootstrapWithError(store kv.Storage, c *C) {
	ss := &session{
		values: make(map[fmt.Stringer]interface{}),
		store:  store,
		sid:    atomic.AddInt64(&sessionID, 1),
	}
	domain, err := domap.Get(store)
	c.Assert(err, IsNil)
	sessionctx.BindDomain(ss, domain)
	variable.BindSessionVars(ss)
	variable.GetSessionVars(ss).SetStatusFlag(mysql.ServerStatusAutocommit, true)
	// session implements autocommit.Checker. Bind it to ctx
	autocommit.BindAutocommitChecker(ss, ss)
	sessionMu.Lock()
	defer sessionMu.Unlock()
	b, err := checkBootstrapped(ss)
	c.Assert(b, IsFalse)
	c.Assert(err, IsNil)
	doDDLWorks(ss)
	// Leave dml unfinished.
}

func (s *testSessionSuite) TestBootstrapWithError(c *C) {
	store := newStore(c, s.dbNameBootstrap)
	s.bootstrapWithError(store, c)
	se := newSession(c, store, s.dbNameBootstrap)
	mustExecSQL(c, se, "USE mysql;")
	r := mustExecSQL(c, se, `select * from user;`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	match(c, row.Data, "localhost", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	match(c, row.Data, "127.0.0.1", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")
	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	mustExecSQL(c, se, "SELECT * from mysql.db;")
	mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	// Check global variables.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_variables;")
	v, err := r.FirstRow()
	c.Assert(err, IsNil)
	c.Assert(v[0], Equals, int64(len(variable.SysVars)))
	// Check global status.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_status;")
	v, err = r.FirstRow()
	c.Assert(err, IsNil)
	statusVars, err := variable.GetDefaultStatusVars()
	c.Assert(err, IsNil)
	c.Assert(v[0], Equals, int64(len(statusVars)))
	r = mustExecSQL(c, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="bootstrapped";`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	c.Assert(row.Data, HasLen, 1)
	c.Assert(row.Data[0], Equals, "True")
}

func (s *testSessionSuite) TestEnum(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c enum('a', 'b', 'c'))")
	mustExecSQL(c, se, "insert into t values ('a'), (2), ('c')")
	r := mustExecSQL(c, se, "select * from t where c = 'a'")
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "a")

	r = mustExecSQL(c, se, "select c + 1 from t where c = 2")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "3")

	mustExecSQL(c, se, "delete from t")
	mustExecSQL(c, se, "insert into t values ()")
	mustExecSQL(c, se, "insert into t values (null), ('1')")
	r = mustExecSQL(c, se, "select c + 1 from t where c = 1")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "2")
}

func (s *testSessionSuite) TestSet(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c set('a', 'b', 'c'))")
	mustExecSQL(c, se, "insert into t values ('a'), (2), ('c'), ('a,b'), ('b,a')")
	r := mustExecSQL(c, se, "select * from t where c = 'a'")
	row, err := r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "a")

	r = mustExecSQL(c, se, "select * from t where c = 'a,b'")
	rows, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 2)

	r = mustExecSQL(c, se, "select c + 1 from t where c = 2")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "3")

	mustExecSQL(c, se, "delete from t")
	mustExecSQL(c, se, "insert into t values ()")
	mustExecSQL(c, se, "insert into t values (null), ('1')")
	r = mustExecSQL(c, se, "select c + 1 from t where c = 1")
	row, err = r.FirstRow()
	c.Assert(err, IsNil)
	match(c, row, "2")
}

func (s *testSessionSuite) TestDatabase(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	// Test database.
	mustExecSQL(c, se, "create database xxx")
	mustExecSQL(c, se, "drop database xxx")

	mustExecSQL(c, se, "drop database if exists xxx")
	mustExecSQL(c, se, "create database xxx")
	mustExecSQL(c, se, "create database if not exists xxx")
	mustExecSQL(c, se, "drop database if exists xxx")

	// Test schema.
	mustExecSQL(c, se, "create schema xxx")
	mustExecSQL(c, se, "drop schema xxx")

	mustExecSQL(c, se, "drop schema if exists xxx")
	mustExecSQL(c, se, "create schema xxx")
	mustExecSQL(c, se, "create schema if not exists xxx")
	mustExecSQL(c, se, "drop schema if exists xxx")
}

func (s *testSessionSuite) TestWhereLike(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t(c int)")
	mustExecSQL(c, se, "insert into t values (1),(2),(3),(-11),(11),(123),(211),(210)")
	mustExecSQL(c, se, "insert into t values ()")

	r := mustExecSQL(c, se, "select c from t where c like '%1%'")
	rows, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 6)
}

func (s *testSessionSuite) TestDefaultFlenBug(c *C) {
	// If set unspecified column flen to 0, it will cause bug in union.
	// This test is used to prevent the bug reappear.
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "create table t1 (c double);")
	mustExecSQL(c, se, "create table t2 (c double);")
	mustExecSQL(c, se, "insert into t1 value (73);")
	mustExecSQL(c, se, "insert into t2 value (930);")
	// The data in the second src will be casted as the type of the first src.
	// If use flen=0, it will be truncated.
	r := mustExecSQL(c, se, "select c from t1 union select c from t2;")
	rows, err := r.Rows(-1, 0)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 2)
	c.Assert(rows[1][0], Equals, float64(930))
}

func (s *testSessionSuite) TestExecRestrictedSQL(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName).(*session)
	r, err := se.ExecRestrictedSQL(se, "select 1;")
	c.Assert(r, NotNil)
	c.Assert(err, IsNil)
	_, err = se.ExecRestrictedSQL(se, "select 1; select 2;")
	c.Assert(err, NotNil)
	_, err = se.ExecRestrictedSQL(se, "")
	c.Assert(err, NotNil)
}

func (s *testSessionSuite) TestGroupBy(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 int, c2 int)")
	mustExecSQL(c, se, "insert into t values (1,1), (2,2), (1,2), (1,3)")
	mustExecMatch(c, se, "select nullif (count(*), 2);", [][]interface{}{{1}})
	mustExecMatch(c, se, "select 1 as a, sum(c1) as a from t group by a", [][]interface{}{{1, 5}})
	mustExecMatch(c, se, "select c1 as a, 1 as a, sum(c1) as a from t group by a", [][]interface{}{{1, 1, 5}})
	mustExecMatch(c, se, "select c1 as a, 1 as a, c2 as a from t group by a;", [][]interface{}{{1, 1, 1}})
	mustExecMatch(c, se, "select c1 as c2, sum(c1) as c2 from t group by c2;", [][]interface{}{{1, 1}, {2, 3}, {1, 1}})

	mustExecMatch(c, se, "select c1 as c2, c2 from t group by c2 + 1", [][]interface{}{{1, 1}, {2, 2}, {1, 3}})
	mustExecMatch(c, se, "select c1 as c2, count(c1) from t group by c2", [][]interface{}{{1, 1}, {2, 2}, {1, 1}})
	mustExecMatch(c, se, "select t.c1, c1 from t group by c1", [][]interface{}{{1, 1}, {2, 2}})
	mustExecMatch(c, se, "select t.c1 as a, c1 as a from t group by a", [][]interface{}{{1, 1}, {2, 2}})

	mustExecFailed(c, se, "select c1 as a, c2 as a from t group by a")
	mustExecFailed(c, se, "select c1 as c2, c2 from t group by c2")
	mustExecFailed(c, se, "select sum(c1) as a from t group by a")
	mustExecFailed(c, se, "select sum(c1) as a from t group by a + 1")
}

func (s *testSessionSuite) TestOrderBy(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 int, c2 int)")
	mustExecSQL(c, se, "insert into t values (1,2), (2, 1)")

	// Fix issue https://github.com/pingcap/tidb/issues/337
	mustExecMatch(c, se, "select c1 as a, c1 as b from t order by c1", [][]interface{}{{1, 1}, {2, 2}})

	mustExecMatch(c, se, "select c1 as a, t.c1 as a from t order by a desc", [][]interface{}{{2, 2}, {1, 1}})
	mustExecMatch(c, se, "select c1 as c2 from t order by c2", [][]interface{}{{1}, {2}})
	mustExecMatch(c, se, "select sum(c1) from t order by sum(c1)", [][]interface{}{{3}})
	mustExecMatch(c, se, "select c1 as c2 from t order by c2 + 1", [][]interface{}{{2}, {1}})

	mustExecFailed(c, se, "select c1 as a, c2 as a from t order by a")

	mustExecFailed(c, se, "(select c1 as c2, c2 from t) union (select c1, c2 from t) order by c2")
	mustExecFailed(c, se, "(select c1 as c2, c2 from t) union (select c1, c2 from t) order by c1")
}

func (s *testSessionSuite) TestHaving(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int)")
	mustExecSQL(c, se, "insert into t values (1,2,3), (2, 3, 1), (3, 1, 2)")

	mustExecMatch(c, se, "select c1 as c2, c3 from t having c2 = 2", [][]interface{}{{2, 1}})
	mustExecMatch(c, se, "select c1 as c2, c3 from t group by c2 having c2 = 2;", [][]interface{}{{1, 3}})
	mustExecMatch(c, se, "select c1 as c2, c3 from t group by c2 having sum(c2) = 2;", [][]interface{}{{1, 3}})
	mustExecMatch(c, se, "select c1 as c2, c3 from t group by c3 having sum(c2) = 2;", [][]interface{}{{1, 3}})
	mustExecMatch(c, se, "select c1 as c2, c3 from t group by c3 having sum(0) + c2 = 2;", [][]interface{}{{2, 1}})
	mustExecMatch(c, se, "select c1 as a from t having c1 = 1;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select t.c1 from t having c1 = 1;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select a.c1 from t as a having c1 = 1;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select c1 as a from t group by c3 having sum(a) = 1;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select c1 as a from t group by c3 having sum(a) + a = 2;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select a.c1 as c, a.c1 as d from t as a, t as b having c1 = 1 limit 1;", [][]interface{}{{1, 1}})

	mustExecMatch(c, se, "select sum(c1) from t group by c1 having sum(c1)", [][]interface{}{{1}, {2}, {3}})
	mustExecMatch(c, se, "select sum(c1) - 1 from t group by c1 having sum(c1) - 1", [][]interface{}{{1}, {2}})
	mustExecMatch(c, se, "select 1 from t group by c1 having sum(abs(c2 + c3)) = c1", [][]interface{}{{1}})

	mustExecFailed(c, se, "select c1 from t having c2")
	mustExecFailed(c, se, "select c1 from t having c2 + 1")
	mustExecFailed(c, se, "select c1 from t group by c2 + 1 having c2")
	mustExecFailed(c, se, "select c1 from t group by c2 + 1 having c2 + 1")
	mustExecFailed(c, se, "select c1 as c2, c2 from t having c2")
	mustExecFailed(c, se, "select c1 as c2, c2 from t having c2 + 1")
	mustExecFailed(c, se, "select c1 as a, c2 as a from t having a")
	mustExecFailed(c, se, "select c1 as a, c2 as a from t having a + 1")
	mustExecFailed(c, se, "select c1 + 1 from t having c1")
	mustExecFailed(c, se, "select c1 + 1 from t having c1 + 1")
	mustExecFailed(c, se, "select a.c1 as c, b.c1 as d from t as a, t as b having c1")
	mustExecFailed(c, se, "select 1 from t having sum(avg(c1))")
}

func (s *testSessionSuite) TestResultType(c *C) {
	// Testcase for https://github.com/pingcap/tidb/issues/325
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	rs := mustExecSQL(c, se, `select cast(null as char(30))`)
	c.Assert(rs, NotNil)
	row, err := rs.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data[0], IsNil)
	fs, err := rs.Fields()
	c.Assert(err, IsNil)
	c.Assert(fs[0].Col.FieldType.Tp, Equals, mysql.TypeString)
}

func (s *testSessionSuite) TestIssue461(c *C) {
	store := newStore(c, s.dbName)
	se1 := newSession(c, store, s.dbName)
	mustExecSQL(c, se1,
		`CREATE TABLE test ( id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, val int UNIQUE, PRIMARY KEY (id)); `)
	mustExecSQL(c, se1, "begin;")
	mustExecSQL(c, se1, "insert into test(id, val) values(1, 1);")
	se2 := newSession(c, store, s.dbName)
	mustExecSQL(c, se2, "begin;")
	mustExecSQL(c, se2, "insert into test(id, val) values(1, 1);")
	mustExecSQL(c, se2, "commit;")
	mustExecFailed(c, se1, "commit;")

	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table test;")
}

func (s *testSessionSuite) TestIssue177(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, `drop table if exists t1;`)
	mustExecSQL(c, se, `drop table if exists t2;`)
	mustExecSQL(c, se, "CREATE TABLE `t1` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	mustExecSQL(c, se, "CREATE TABLE `t2` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	mustExecSQL(c, se, `INSERT INTO t1 VALUES (1,1,1);`)
	mustExecSQL(c, se, `INSERT INTO t2 VALUES (1,1,1);`)
	mustExecSQL(c, se, `PREPARE my_stmt FROM "SELECT t1.b, count(*) FROM t1 group by t1.b having count(*) > ALL (SELECT COUNT(*) FROM t2 WHERE t2.a=1 GROUP By t2.b)";`)
	mustExecSQL(c, se, `EXECUTE my_stmt;`)
	mustExecSQL(c, se, `EXECUTE my_stmt;`)
	mustExecSQL(c, se, `deallocate prepare my_stmt;`)
	mustExecSQL(c, se, `drop table t1,t2;`)
}

func (s *testSessionSuite) TestBuiltin(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	// Testcase for https://github.com/pingcap/tidb/issues/382
	mustExecFailed(c, se, `select cast("xxx 10:10:10" as datetime)`)
}

func (s *testSessionSuite) TestFieldText(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (a int)")
	cases := []struct {
		sql   string
		field string
	}{
		{"select distinct(a) from t", "a"},
		{"select (1)", "1"},
		{"select (1+1)", "(1+1)"},
		{"select a from t", "a"},
		{"select        ((a+1))     from t", "((a+1))"},
	}
	for _, v := range cases {
		results, err := se.Execute(v.sql)
		c.Assert(err, IsNil)
		result := results[0]
		fields, err := result.Fields()
		c.Assert(err, IsNil)
		c.Assert(fields[0].Name, Equals, v.field)
	}
}

func (s *testSessionSuite) TestIndexPointLookup(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (a int)")
	mustExecSQL(c, se, "insert t values (1), (2), (3)")
	mustExecMatch(c, se, "select * from t where a = '1.1';", [][]interface{}{})
	mustExecMatch(c, se, "select * from t where a > '1.1';", [][]interface{}{{2}, {3}})
	mustExecMatch(c, se, "select * from t where a = '2';", [][]interface{}{{2}})
	mustExecMatch(c, se, "select * from t where a = 3;", [][]interface{}{{3}})
	mustExecMatch(c, se, "select * from t where a = 4;", [][]interface{}{})
	mustExecSQL(c, se, "drop table t")
}

// For https://github.com/pingcap/tidb/issues/571
func (s *testSessionSuite) TestIssue571(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c int)")
	mustExecSQL(c, se, "insert t values (1), (2), (3)")
	mustExecSQL(c, se, "commit")

	se1 := newSession(c, store, s.dbName)
	mustExecSQL(c, se1, "SET SESSION autocommit=1;")
	se2 := newSession(c, store, s.dbName)
	mustExecSQL(c, se2, "SET SESSION autocommit=1;")
	se3 := newSession(c, store, s.dbName)
	mustExecSQL(c, se3, "SET SESSION autocommit=0;")

	var wg sync.WaitGroup
	wg.Add(3)
	f1 := func() {
		defer wg.Done()
		// Unlimited retry times.
		se1.(*session).maxRetryCnt = -1
		for i := 0; i < 50; i++ {
			mustExecSQL(c, se1, "update t set c = 1;")
			v, ok := se1.(*session).debugInfos[retryEmptyHistoryList]
			if ok {
				c.Assert(v, IsFalse)
			}
		}
	}
	f2 := func() {
		defer wg.Done()
		// Unlimited retry times.
		se2.(*session).maxRetryCnt = -1
		for i := 0; i < 50; i++ {
			mustExecSQL(c, se2, "update t set c = 1;")
			v, ok := se2.(*session).debugInfos[retryEmptyHistoryList]
			if ok {
				c.Assert(v, IsFalse)
			}
		}
	}
	f3 := func() {
		defer wg.Done()
		// Unlimited retry times.
		se3.(*session).maxRetryCnt = -1
		for i := 0; i < 50; i++ {
			mustExecSQL(c, se3, "begin")
			mustExecSQL(c, se3, "update t set c = 1;")
			mustExecSQL(c, se3, "commit")
			v, ok := se3.(*session).debugInfos[retryEmptyHistoryList]
			if ok {
				c.Assert(v, IsFalse)
			}
		}
	}
	go f1()
	go f2()
	go f3()
	wg.Wait()
}

func newSession(c *C, store kv.Storage, dbName string) Session {
	se, err := CreateSession(store)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, "create database if not exists "+dbName)
	mustExecSQL(c, se, "use "+dbName)
	return se
}

// Testcase for session
func (s *testSessionSuite) TestSession(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "ROLLBACK;")
	err := se.Close()
	c.Assert(err, IsNil)
}

func newStore(c *C, dbPath string) kv.Storage {
	store, err := NewStore(*store + "://" + dbPath)
	c.Assert(err, IsNil)
	return store
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
