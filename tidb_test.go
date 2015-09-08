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

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/rset"
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
	dbName string

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

	testDB, err := sql.Open(DriverName, *store+"://"+dbName)
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
	testDB, err := sql.Open(DriverName, *store+"://"+s.dbName)
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
	testDB, err := sql.Open(DriverName, *store+"://"+s.dbName)
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
	db, err := sql.Open("tidb", "memory://test")
	defer db.Close()
	_, err = db.Exec("create table t (c int)")
	c.Assert(err, IsNil)
	_, err = db.Exec("insert into t values (1), (2), (3)")
	c.Assert(err, IsNil)
	_, err = db.Query("delete from `t` where `c` = ?", 1)
	c.Assert(err, IsNil)
	_, err = db.Query("delete from `t` where `c` = ?", 2)
	c.Assert(err, IsNil)
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
	c.Assert(se.LastInsertID(), Less, uint64(4))
	mustExecSQL(c, se, s.dropDBSQL)
}

// See: http://dev.mysql.com/doc/refman/5.7/en/commit.html
func (s *testSessionSuite) TestAutoicommit(c *C) {
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, Equals, nil)
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(se.(*session).txn, Equals, nil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, false)
	mustExecSQL(c, se, "insert t values ()")
	c.Assert(se.(*session).txn, IsNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, false)
	mustExecSQL(c, se, "begin")
	c.Assert(se.(*session).txn, NotNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, true)
	mustExecSQL(c, se, "insert t values ()")
	c.Assert(se.(*session).txn, NotNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, true)
	se.Execute("drop table if exists t;")
	c.Assert(se.(*session).txn, IsNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, true)
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(se.(*session).txn, IsNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, true)
	mustExecSQL(c, se, "insert t values ()")
	c.Assert(se.(*session).txn, NotNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, true)
	mustExecSQL(c, se, "commit")
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, false)
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "insert t values ()")
	c.Assert(se.(*session).txn, IsNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, false)

	se.Execute("drop table if exists t;")
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, false)
	mustExecSQL(c, se, "begin")
	c.Assert(se.(*session).txn, NotNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, true)
	mustExecSQL(c, se, "insert t values ()")
	c.Assert(se.(*session).txn, NotNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, true)
	mustExecSQL(c, se, "rollback")
	c.Assert(se.(*session).txn, IsNil)
	c.Assert(variable.GetSessionVars(se.(*session)).DisableAutocommit, Equals, false)

	mustExecSQL(c, se, s.dropDBSQL)
}

// See: http://dev.mysql.com/doc/refman/5.7/en/commit.html
func (s *testSessionSuite) TestRowLock(c *C) {
	c.Skip("Need retry feature")
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	se1 := newSession(c, store, s.dbName)
	se2 := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, Equals, nil)
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
	// row lock conflict but can still success
	c.Assert(err, IsNil)

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
	c.Assert(se.(*session).txn, Equals, nil)
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int)")
	mustExecSQL(c, se, "insert t values (11, 2, 3)")
	mustExecSQL(c, se, "insert t values (12, 2, 3)")
	mustExecSQL(c, se, "insert t values (13, 2, 3)")

	// conflict
	mustExecSQL(c, se1, "begin")
	rs, err := exec(c, se1, "select * from t where c1=11 for update")
	_, err = rs.Rows(-1, 0)

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=211 where c1=11")
	mustExecSQL(c, se2, "commit")

	_, err = exec(c, se1, "commit")
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
