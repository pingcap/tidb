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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testSessionSuite{})

type testSessionSuite struct {
	dbName          string
	dbNameBootstrap string

	createDBSQL    string
	dropDBSQL      string
	useDBSQL       string
	createTableSQL string
	dropTableSQL   string
	selectSQL      string
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
	log.SetLevelByString("error")
}

func (s *testSessionSuite) TearDownSuite(c *C) {
	removeStore(c, s.dbName)
}

func (s *testSessionSuite) TestPrepare(c *C) {
	defer testleak.AfterTest(c)()
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

	mustExecSQL(c, se, "prepare stmt from 'select 1+?'")
	mustExecSQL(c, se, "set @v1=100")
	rs := mustExecSQL(c, se, "execute stmt using @v1")
	r, err := rs.Next()
	c.Assert(err, IsNil)
	c.Assert(r.Data[0].GetFloat64(), Equals, float64(101))

	mustExecSQL(c, se, "set @v2=200")
	rs = mustExecSQL(c, se, "execute stmt using @v2")
	r, err = rs.Next()
	c.Assert(err, IsNil)
	c.Assert(r.Data[0].GetFloat64(), Equals, float64(201))

	mustExecSQL(c, se, "set @v3=300")
	rs = mustExecSQL(c, se, "execute stmt using @v3")
	r, err = rs.Next()
	c.Assert(err, IsNil)
	c.Assert(r.Data[0].GetFloat64(), Equals, float64(301))
	mustExecSQL(c, se, "deallocate prepare stmt")

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestAffectedRows(c *C) {
	defer testleak.AfterTest(c)()
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
	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	sessionExec(c, se, "select 1")
	// here to check the panic bug in String() when txn is nil after committed.
	c.Log(se.String())

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestResultField(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	// create table
	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, "create table t (id int);")

	mustExecSQL(c, se, `INSERT INTO t VALUES (1);`)
	mustExecSQL(c, se, `INSERT INTO t VALUES (2);`)
	r := mustExecSQL(c, se, `SELECT count(*) from t;`)
	c.Assert(r, NotNil)
	_, err := GetRows(r)
	c.Assert(err, IsNil)
	fields, err := r.Fields()
	c.Assert(err, IsNil)
	c.Assert(len(fields), Equals, 1)
	field := fields[0].Column
	c.Assert(field.Tp, Equals, mysql.TypeLonglong)
	c.Assert(field.Flen, Equals, 21)
	mustExecSQL(c, se, s.dropDBSQL)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestPrimaryKeyAutoincrement(c *C) {
	defer testleak.AfterTest(c)()
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
	row, err := rs.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, id, []byte("abc"), nil)

	mustExecSQL(c, se, "update t set name = 'abc', status = 1 where id = ?", id)
	rs = mustExecSQL(c, se2, "select * from t")
	c.Assert(rs, NotNil)
	row, err = rs.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, id, []byte("abc"), 1)
	// Check for pass bool param to tidb prepared statement
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id tiny)")
	mustExecSQL(c, se, "insert t values (?)", true)
	rs = mustExecSQL(c, se, "select * from t")
	c.Assert(rs, NotNil)
	row, err = rs.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, int8(1))

	mustExecSQL(c, se, s.dropDBSQL)
	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestAutoincrementID(c *C) {
	defer testleak.AfterTest(c)()
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

	err := store.Close()
	c.Assert(err, IsNil)
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
	defer testleak.AfterTest(c)()
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
	err := store.Close()
	c.Assert(err, IsNil)
}

func checkInTrans(c *C, se Session, stmt string, expect uint16) {
	checkTxn(c, se, stmt, expect)
	ret := variable.GetSessionVars(se.(*session)).Status & mysql.ServerStatusInTrans
	c.Assert(ret, Equals, expect)
}

// See: https://dev.mysql.com/doc/internals/en/status-flags.html
func (s *testSessionSuite) TestInTrans(c *C) {
	defer testleak.AfterTest(c)()
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

	checkInTrans(c, se, "set autocommit=0;", 0)
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
	err := store.Close()
	c.Assert(err, IsNil)
}

// See: http://dev.mysql.com/doc/refman/5.7/en/commit.html
func (s *testSessionSuite) TestRowLock(c *C) {
	defer testleak.AfterTest(c)()
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
	rows, err := GetRows(r)
	fmt.Println(rows)
	matches(c, rows, [][]interface{}{{21}})

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2=21 where c1=11")

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=22 where c1=12")
	mustExecSQL(c, se2, "commit")

	mustExecSQL(c, se1, "commit")

	mustExecSQL(c, se, s.dropDBSQL)
	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue827(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	se1 := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t1")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t1 (c2 int, c3 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	mustExecSQL(c, se, "insert into t1 set c2 = 30")

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t (c2 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	mustExecSQL(c, se, "insert into t (c2) values (1), (2), (3), (4), (5)")

	// insert values
	lastInsertID := se.LastInsertID()
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "insert into t (c2) values (11), (12), (13)")
	rs, err := exec(c, se, "select c1 from t where c2 = 11")
	c.Assert(err, IsNil)
	expect, err := GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(c, se, "update t set c2 = 33 where c2 = 1")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 22 where c2 = 1")
	mustExecSQL(c, se1, "commit")

	_, err = exec(c, se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(c, se, "select c1 from t where c2 = 11")
	c.Assert(err, IsNil)
	r, err := GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID := se.LastInsertID()
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	// insert set
	lastInsertID = se.LastInsertID()
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "insert into t set c2 = 31")
	rs, err = exec(c, se, "select c1 from t where c2 = 31")
	c.Assert(err, IsNil)
	expect, err = GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(c, se, "update t set c2 = 44 where c2 = 2")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 55 where c2 = 2")
	mustExecSQL(c, se1, "commit")

	_, err = exec(c, se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(c, se, "select c1 from t where c2 = 31")
	c.Assert(err, IsNil)
	r, err = GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID = se.LastInsertID()
	c.Assert(lastInsertID+1, Equals, currLastInsertID)

	// replace
	lastInsertID = se.LastInsertID()
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "insert into t (c2) values (21), (22), (23)")
	rs, err = exec(c, se, "select c1 from t where c2 = 21")
	c.Assert(err, IsNil)
	expect, err = GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(c, se, "update t set c2 = 66 where c2 = 3")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 77 where c2 = 3")
	mustExecSQL(c, se1, "commit")

	_, err = exec(c, se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(c, se, "select c1 from t where c2 = 21")
	c.Assert(err, IsNil)
	r, err = GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID = se.LastInsertID()
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	// update
	lastInsertID = se.LastInsertID()
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "insert into t set c2 = 41")
	mustExecSQL(c, se, "update t set c1 = 0 where c2 = 41")
	rs, err = exec(c, se, "select c1 from t where c2 = 41")
	c.Assert(err, IsNil)
	expect, err = GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(c, se, "update t set c2 = 88 where c2 = 4")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 99 where c2 = 4")
	mustExecSQL(c, se1, "commit")

	_, err = exec(c, se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(c, se, "select c1 from t where c2 = 41")
	c.Assert(err, IsNil)
	r, err = GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID = se.LastInsertID()
	c.Assert(lastInsertID+1, Equals, currLastInsertID)

	// prepare
	lastInsertID = se.LastInsertID()
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "prepare stmt from 'insert into t (c2) values (?)'")
	mustExecSQL(c, se, "set @v1=100")
	mustExecSQL(c, se, "set @v2=200")
	mustExecSQL(c, se, "set @v3=300")
	mustExecSQL(c, se, "execute stmt using @v1")
	mustExecSQL(c, se, "execute stmt using @v2")
	mustExecSQL(c, se, "execute stmt using @v3")
	mustExecSQL(c, se, "deallocate prepare stmt")
	rs, err = exec(c, se, "select c1 from t where c2 = 12")
	c.Assert(err, IsNil)
	expect, err = GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(c, se, "update t set c2 = 111 where c2 = 5")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 222 where c2 = 5")
	mustExecSQL(c, se1, "commit")

	_, err = exec(c, se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(c, se, "select c1 from t where c2 = 12")
	c.Assert(err, IsNil)
	r, err = GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID = se.LastInsertID()
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	mustExecSQL(c, se, s.dropDBSQL)
	err = se.Close()
	c.Assert(err, IsNil)
	err = se1.Close()
	c.Assert(err, IsNil)
	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue996(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t (c2 int, c3 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	mustExecSQL(c, se, "insert into t set c2 = 30")

	// insert values
	lastInsertID := se.LastInsertID()
	mustExecSQL(c, se, "prepare stmt1 from 'insert into t (c2) values (?)'")
	mustExecSQL(c, se, "set @v1=10")
	mustExecSQL(c, se, "set @v2=20")
	mustExecSQL(c, se, "execute stmt1 using @v1")
	mustExecSQL(c, se, "execute stmt1 using @v2")
	mustExecSQL(c, se, "deallocate prepare stmt1")
	rs, err := exec(c, se, "select c1 from t where c2 = 20")
	c.Assert(err, IsNil)
	r, err := GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	currLastInsertID := se.LastInsertID()
	c.Assert(r[0][0].GetValue(), DeepEquals, int64(currLastInsertID))
	c.Assert(lastInsertID+2, Equals, currLastInsertID)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue986(c *C) {
	defer testleak.AfterTest(c)()
	sqlText := `CREATE TABLE address (
 		id bigint(20) NOT NULL AUTO_INCREMENT,
 		PRIMARY KEY (id));`
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, sqlText)
	sqlText = `insert into address values ('10')`
	mustExecSQL(c, se, sqlText)

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue1089(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	r := mustExecSQL(c, se, "select cast(0.5 as unsigned)")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)
	r = mustExecSQL(c, se, "select cast(-0.5 as signed)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, -1)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue1135(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	se1 := newSession(c, store, s.dbName+"1")

	mustExecSQL(c, se1, "drop table if exists t")
	mustExecSQL(c, se1, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se1, "insert into t (F1) values ('1'), ('4');")

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	mustExecSQL(c, se, "delete m1 from t m2,t m1 where m1.F1>1;")
	r := mustExecSQL(c, se, "select * from t;")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'1'})

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	mustExecSQL(c, se, "delete m1 from t m1,t m2 where true and m1.F1<2;")
	r = mustExecSQL(c, se, "select * from t;")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'2'})

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	mustExecSQL(c, se, "delete m1 from t m1,t m2 where false;")
	r = mustExecSQL(c, se, "select * from t;")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'1'})
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'2'})

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	mustExecSQL(c, se, "delete m1, m2 from t m1,t m2 where m1.F1>m2.F1;")
	r = mustExecSQL(c, se, "select * from t;")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	sql := fmt.Sprintf("delete %s.t from %s.t inner join %s.t where %s.t.F1 > %s.t.F1",
		s.dbName+"1", s.dbName+"1", s.dbName, s.dbName+"1", s.dbName)
	mustExecSQL(c, se1, sql)
	r = mustExecSQL(c, se1, "select * from t;")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'1'})

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue1114(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "set @tmp = 0")
	mustExecSQL(c, se, "set @tmp := @tmp + 1")
	r := mustExecSQL(c, se, "select @tmp")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestSelectForUpdate(c *C) {
	defer testleak.AfterTest(c)()
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
	_, err = GetRows(rs)

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
	_, err = GetRows(rs)

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=22 where c1=12")
	mustExecSQL(c, se2, "commit")

	mustExecSQL(c, se1, "commit")

	// not conflict, auto commit
	mustExecSQL(c, se1, "set @@autocommit=1;")
	rs, err = exec(c, se1, "select * from t where c1=11 for update")
	_, err = GetRows(rs)

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
	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestRow(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	r := mustExecSQL(c, se, "select row(1, 1) in (row(1, 1))")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select row(1, 1) in (row(1, 0))")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 0)

	r = mustExecSQL(c, se, "select row(1, 1) in (select 1, 1)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select row(1, 1) > row(1, 0)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select row(1, 1) > (select 1, 0)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select 1 > (select 1)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 0)

	r = mustExecSQL(c, se, "select (select 1)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIndex(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "create table if not exists test_index (c1 int, c double, index(c1), index(c))")
	mustExecSQL(c, se, "insert into test_index values (1, 2), (3, null)")
	r := mustExecSQL(c, se, "select c1 from test_index where c > 0")
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 1)
	match(c, rows[0], 1)

	mustExecSQL(c, se, "drop table if exists t1, t2")
	mustExecSQL(c, se, `
			create table t1 (c1 int, primary key(c1));
			create table t2 (c2 int, primary key(c2));`)
	mustExecSQL(c, se, `
			insert into t1 values (1), (2);
			insert into t2 values (2);`)

	r = mustExecSQL(c, se, "select * from t1 left join t2 on t1.c1 = t2.c2 order by t1.c1")
	rows, err = GetRows(r)
	c.Assert(err, IsNil)
	matches(c, rows, [][]interface{}{{1, nil}, {2, 2}})

	r = mustExecSQL(c, se, "select * from t1 left join t2 on t1.c1 = t2.c2 where t2.c2 < 10")
	rows, err = GetRows(r)
	c.Assert(err, IsNil)
	matches(c, rows, [][]interface{}{{2, 2}})

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestMySQLTypes(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	r := mustExecSQL(c, se, `select 0x01 + 1, x'4D7953514C' = "MySQL"`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 2, 1)
	r.Close()

	r = mustExecSQL(c, se, `select 0b01 + 1, 0b01000001 = "A"`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 2, 1)
	r.Close()

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestExpression(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	r := mustExecSQL(c, se, `select + (1 > 0), -(1 >0), + (1 < 0), - (1 < 0)`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1, -1, 0, 0)
	r.Close()

	r = mustExecSQL(c, se, "select 1 <=> 1, 1 <=> null, null <=> null, null <=> (select null)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1, 0, 1, 1)
	r.Close()

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestSelect(c *C) {
	defer testleak.AfterTest(c)()
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
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1, 2)

	// Testcase For https://github.com/pingcap/tidb/issues/1071
	r = mustExecSQL(c, se, `select 1 from dual where "0.1"`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)
	r = mustExecSQL(c, se, "select 1 from dual where 0.8")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)
	r = mustExecSQL(c, se, "select 1, count(*) from dual where 0.1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1, 0)
	r = mustExecSQL(c, se, "select count(*), 1 from dual where 0.8")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1, 1)
	r = mustExecSQL(c, se, "select 1, 2 from dual where 0.1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)
	mustExecSQL(c, se, "create table if not exists t2 (c1 int, c2 int)")
	mustExecSQL(c, se, "insert into t2 (c1, c2) values(1, 1), (2, 2), (3, 3)")
	r = mustExecSQL(c, se, "select 1 from t2 where 0.1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)
	r = mustExecSQL(c, se, "select 1 from t2 where 0.9")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)
	r = mustExecSQL(c, se, "select sum(c1) from t2 where 0.1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, nil)
	r = mustExecSQL(c, se, "select sum(c1), c2 from t2 where 0.1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, nil, nil)
	r = mustExecSQL(c, se, "select 1+2, count(c1) from t2 where 0.1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 3, 0)

	r = mustExecSQL(c, se, "select 1, 2 from dual where not exists (select * from t where c1=2)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1, 2)

	r = mustExecSQL(c, se, "select 1, 2")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1, 2)

	r = mustExecSQL(c, se, `select '''a''', """a""", 'pingcap ''-->'' tidb'`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, `'a'`, `"a"`, `pingcap '-->' tidb`)

	r = mustExecSQL(c, se, `select '\'a\'', "\"a\"";`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, `'a'`, `"a"`)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c varchar(20))")
	mustExecSQL(c, se, `insert t values("pingcap '-->' tidb")`)

	r = mustExecSQL(c, se, `select * from t where c like 'pingcap ''-->'' tidb'`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []byte(`pingcap '-->' tidb`))

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
	rows, err := GetRows(r)
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
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 3.12)

	mustExecSQL(c, se, `drop table if exists t;create table t (c int);insert into t values (1);`)
	r = mustExecSQL(c, se, "select a.c from t as a where c between null and 2")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)

	mustExecSQL(c, se, "drop table if exists t1, t2, t3")
	mustExecSQL(c, se, `
		create table t1 (c1 int);
		create table t2 (c2 int);
		create table t3 (c3 int);`)
	mustExecSQL(c, se, `
		insert into t1 values (1), (2);
		insert into t2 values (2);
		insert into t3 values (3);`)
	r = mustExecSQL(c, se, "select * from t1 left join t2 on t1.c1 = t2.c2 left join t3 on t1.c1 = t3.c3 order by t1.c1")
	rows, err = GetRows(r)
	c.Assert(err, IsNil)
	matches(c, rows, [][]interface{}{{1, nil, nil}, {2, 2, nil}})

	mustExecFailed(c, se, "select * from t1 left join t2 on t1.c1 = t3.c3 left join on t3 on t1.c1 = t2.c2")

	// For issue 393
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (b blob)")
	mustExecSQL(c, se, `insert t values('\x01')`)

	r = mustExecSQL(c, se, `select length(b) from t`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 3)

	mustExecSQL(c, se, `select * from t1, t2 where t1.c1 is null`)
	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestSubQuery(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "create table if not exists t1 (c1 int, c2 int)")
	mustExecSQL(c, se, "create table if not exists t2 (c1 int, c2 int)")
	mustExecSQL(c, se, "insert into t1 values (1, 1), (2, 2)")
	mustExecSQL(c, se, "insert into t2 values (1, 1), (1, 2)")

	r := mustExecSQL(c, se, `select c1 from t1 where c1 = (select c2 from t2 where t1.c2 = t2.c2)`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, `select (select count(c1) from t2 where t2.c1 != t1.c2) from t1`)
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 2)
	match(c, rows[0], 0)
	match(c, rows[1], 2)

	mustExecMatch(c, se, "select a.c1, a.c2 from (select c1 as c1, c1 as c2 from t1) as a", [][]interface{}{{1, 1}, {2, 2}})

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestShow(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "set global autocommit=1")
	r := mustExecSQL(c, se, "show global variables where variable_name = 'autocommit'")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "autocommit", "ON")

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table if not exists t (c int)")
	r = mustExecSQL(c, se, `show columns from t`)
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 1)
	match(c, rows[0], "c", "int(11)", "YES", "", nil, "")

	r = mustExecSQL(c, se, "show collation where Charset = 'utf8' and Collation = 'utf8_bin'")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "utf8_bin", "utf8", 83, "", "Yes", 1)

	r = mustExecSQL(c, se, "show tables")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 1)

	r = mustExecSQL(c, se, "show full tables")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 2)

	r = mustExecSQL(c, se, "show create table t")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 2)
	c.Assert(row.Data[0].GetString(), Equals, "t")

	r = mustExecSQL(c, se, "show databases like 'test'")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 1)
	c.Assert(row.Data[0].GetString(), Equals, "test")

	r = mustExecSQL(c, se, "grant all on *.* to 'root'@'%'")
	r = mustExecSQL(c, se, "show grants")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 1)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestTimeFunc(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	last := time.Now().Format(mysql.TimeFormat)
	r := mustExecSQL(c, se, "select now(), now(6), current_timestamp, current_timestamp(), current_timestamp(6), sysdate(), sysdate(6)")
	row, err := r.Next()
	c.Assert(err, IsNil)
	for _, t := range row.Data {
		n := t.GetMysqlTime()
		c.Assert(n.String(), GreaterEqual, last)
	}

	last = time.Now().Format(mysql.DateFormat)
	r = mustExecSQL(c, se, "select current_date, current_date(), curdate()")
	row, err = r.Next()
	c.Assert(err, IsNil)
	for _, t := range row.Data {
		n := t.GetMysqlTime()
		c.Assert(n.String(), GreaterEqual, last)
	}

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestBit(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 bit(2))")
	mustExecSQL(c, se, "insert into t values (0), (1), (2), (3)")
	_, err := exec(c, se, "insert into t values (4)")
	c.Assert(err, NotNil)
	r := mustExecSQL(c, se, "select * from t where c1 = 2")
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data[0].GetMysqlBit(), Equals, mysql.Bit{Value: 2, Width: 2})

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestBootstrap(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "USE mysql;")
	r := mustExecSQL(c, se, `select * from user;`)
	c.Assert(r, NotNil)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	match(c, row.Data, []byte("%"), []byte("root"), []byte(""), "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")

	c.Assert(se.Auth("root@anyhost", []byte(""), []byte("")), IsTrue)
	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	mustExecSQL(c, se, "SELECT * from mysql.db;")
	mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	// Check privilege tables.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_variables;")
	c.Assert(r, NotNil)
	v, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(v.Data[0].GetInt64(), Equals, int64(len(variable.SysVars)))

	// Check a storage operations are default autocommit after the second start.
	mustExecSQL(c, se, "USE test;")
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id int)")
	delete(storeBootstrapped, store.UUID())
	se.Close()
	se, err = CreateSession(store)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, "USE test;")
	mustExecSQL(c, se, "insert t values (?)", 3)
	se, err = CreateSession(store)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, "USE test;")
	r = mustExecSQL(c, se, "select * from t")
	c.Assert(r, NotNil)
	v, err = r.Next()
	c.Assert(err, IsNil)
	match(c, v.Data, 3)
	mustExecSQL(c, se, "drop table if exists t")
	se.Close()

	// Try do bootstrap dml jobs on an already bootstraped TiDB system will not cause fatal.
	// For https://github.com/pingcap/tidb/issues/1096
	store = newStore(c, s.dbName)
	se, err = CreateSession(store)
	c.Assert(err, IsNil)
	doDMLWorks(se)

	err = store.Close()
	c.Assert(err, IsNil)
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
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbNameBootstrap)
	s.bootstrapWithError(store, c)
	se := newSession(c, store, s.dbNameBootstrap)
	mustExecSQL(c, se, "USE mysql;")
	r := mustExecSQL(c, se, `select * from user;`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	match(c, row.Data, []byte("%"), []byte("root"), []byte(""), "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")
	mustExecSQL(c, se, "USE test;")
	// Check privilege tables.
	mustExecSQL(c, se, "SELECT * from mysql.db;")
	mustExecSQL(c, se, "SELECT * from mysql.tables_priv;")
	mustExecSQL(c, se, "SELECT * from mysql.columns_priv;")
	// Check global variables.
	r = mustExecSQL(c, se, "SELECT COUNT(*) from mysql.global_variables;")
	v, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(v.Data[0].GetInt64(), Equals, int64(len(variable.SysVars)))

	r = mustExecSQL(c, se, `SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="bootstrapped";`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	c.Assert(row.Data, HasLen, 1)
	c.Assert(row.Data[0].GetBytes(), BytesEquals, []byte("True"))

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestEnum(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c enum('a', 'b', 'c'))")
	mustExecSQL(c, se, "insert into t values ('a'), (2), ('c')")
	r := mustExecSQL(c, se, "select * from t where c = 'a'")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "a")

	r = mustExecSQL(c, se, "select c + 1 from t where c = 2")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "3")

	mustExecSQL(c, se, "delete from t")
	mustExecSQL(c, se, "insert into t values ()")
	mustExecSQL(c, se, "insert into t values (null), ('1')")
	r = mustExecSQL(c, se, "select c + 1 from t where c = 1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "2")

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestSet(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c set('a', 'b', 'c'))")
	mustExecSQL(c, se, "insert into t values ('a'), (2), ('c'), ('a,b'), ('b,a')")
	r := mustExecSQL(c, se, "select * from t where c = 'a'")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "a")

	r = mustExecSQL(c, se, "select * from t where c = 'a,b'")
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 2)

	r = mustExecSQL(c, se, "select c + 1 from t where c = 2")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "3")

	mustExecSQL(c, se, "delete from t")
	mustExecSQL(c, se, "insert into t values ()")
	mustExecSQL(c, se, "insert into t values (null), ('1')")
	r = mustExecSQL(c, se, "select c + 1 from t where c = 1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "2")

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestDatabase(c *C) {
	defer testleak.AfterTest(c)()
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

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestWhereLike(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t(c int, index(c))")
	mustExecSQL(c, se, "insert into t values (1),(2),(3),(-11),(11),(123),(211),(210)")
	mustExecSQL(c, se, "insert into t values ()")

	r := mustExecSQL(c, se, "select c from t where c like '%1%'")
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 6)

	mustExecSQL(c, se, "select c from t where c like binary('abc')")

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestDefaultFlenBug(c *C) {
	defer testleak.AfterTest(c)()
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
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 2)
	c.Assert(rows[1][0].GetFloat64(), Equals, float64(930))

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestExecRestrictedSQL(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName).(*session)
	r, err := se.ExecRestrictedSQL(se, "select 1;")
	c.Assert(r, NotNil)
	c.Assert(err, IsNil)
	_, err = se.ExecRestrictedSQL(se, "select 1; select 2;")
	c.Assert(err, NotNil)
	_, err = se.ExecRestrictedSQL(se, "")
	c.Assert(err, NotNil)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestGroupBy(c *C) {
	defer testleak.AfterTest(c)()
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

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestOrderBy(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 varchar(20))")
	mustExecSQL(c, se, "insert into t values (1, 2, 'abc'), (2, 1, 'bcd')")

	// Fix issue https://github.com/pingcap/tidb/issues/337
	mustExecMatch(c, se, "select c1 as a, c1 as b from t order by c1", [][]interface{}{{1, 1}, {2, 2}})

	mustExecMatch(c, se, "select c1 as a, t.c1 as a from t order by a desc", [][]interface{}{{2, 2}, {1, 1}})
	mustExecMatch(c, se, "select c1 as c2 from t order by c2", [][]interface{}{{1}, {2}})
	mustExecMatch(c, se, "select sum(c1) from t order by sum(c1)", [][]interface{}{{3}})
	mustExecMatch(c, se, "select c1 as c2 from t order by c2 + 1", [][]interface{}{{2}, {1}})

	// Order by position
	mustExecMatch(c, se, "select * from t order by 1", [][]interface{}{{1, 2, []byte("abc")}, {2, 1, []byte("bcd")}})
	mustExecMatch(c, se, "select * from t order by 2", [][]interface{}{{2, 1, []byte("bcd")}, {1, 2, []byte("abc")}})
	mustExecFailed(c, se, "select * from t order by 0")
	mustExecFailed(c, se, "select * from t order by 4")

	mustExecFailed(c, se, "select c1 as a, c2 as a from t order by a")

	mustExecFailed(c, se, "(select c1 as c2, c2 from t) union (select c1, c2 from t) order by c2")
	mustExecFailed(c, se, "(select c1 as c2, c2 from t) union (select c1, c2 from t) order by c1")

	// Ordery by binary
	mustExecMatch(c, se, "select c1, c3 from t order by binary c1 desc", [][]interface{}{{2, []byte("bcd")}, {1, []byte("abc")}})
	mustExecMatch(c, se, "select c1, c2 from t order by binary c3", [][]interface{}{{1, 2}, {2, 1}})

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestHaving(c *C) {
	defer testleak.AfterTest(c)()
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

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestResultType(c *C) {
	defer testleak.AfterTest(c)()
	// Testcase for https://github.com/pingcap/tidb/issues/325
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	rs := mustExecSQL(c, se, `select cast(null as char(30))`)
	c.Assert(rs, NotNil)
	row, err := rs.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data[0].GetValue(), IsNil)
	fs, err := rs.Fields()
	c.Assert(err, IsNil)
	c.Assert(fs[0].Column.FieldType.Tp, Equals, mysql.TypeString)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue461(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se1 := newSession(c, store, s.dbName)
	mustExecSQL(c, se1,
		`CREATE TABLE test ( id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, val int UNIQUE, PRIMARY KEY (id)); `)
	mustExecSQL(c, se1, "begin;")
	mustExecSQL(c, se1, "insert into test(id, val) values(1, 1);")
	se2 := newSession(c, store, s.dbName)
	mustExecSQL(c, se2, "begin;")
	mustExecSQL(c, se2, "insert into test(id, val) values(2, 2);")
	se3 := newSession(c, store, s.dbName)
	mustExecSQL(c, se3, "begin;")
	mustExecSQL(c, se3, "insert into test(id, val) values(1, 2);")
	mustExecSQL(c, se3, "commit;")
	_, err := se1.Execute("commit")
	c.Assert(err, NotNil)
	// Check error type and error message
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1' for key 'PRIMARY'")

	_, err = se2.Execute("commit")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '2' for key 'val'")

	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table test;")
	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue463(c *C) {
	defer testleak.AfterTest(c)()
	// Testcase for https://github.com/pingcap/tidb/issues/463
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "DROP TABLE IF EXISTS test")
	mustExecSQL(c, se,
		`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val int UNIQUE,
			PRIMARY KEY (id)
		);`)
	mustExecSQL(c, se, "insert into test(id, val) values(1, 1);")
	mustExecFailed(c, se, "insert into test(id, val) values(2, 1);")
	mustExecSQL(c, se, "insert into test(id, val) values(2, 2);")

	mustExecSQL(c, se, "begin;")
	mustExecSQL(c, se, "insert into test(id, val) values(3, 3);")
	mustExecFailed(c, se, "insert into test(id, val) values(4, 3);")
	mustExecSQL(c, se, "insert into test(id, val) values(4, 4);")
	mustExecSQL(c, se, "commit;")
	se1 := newSession(c, store, s.dbName)
	mustExecSQL(c, se1, "begin;")
	mustExecSQL(c, se1, "insert into test(id, val) values(5, 6);")
	mustExecSQL(c, se, "begin;")
	mustExecSQL(c, se, "insert into test(id, val) values(20, 6);")
	mustExecSQL(c, se, "commit;")
	mustExecFailed(c, se1, "commit;")
	mustExecSQL(c, se1, "insert into test(id, val) values(5, 5);")

	mustExecSQL(c, se, "drop table test;")

	mustExecSQL(c, se,
		`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val1 int UNIQUE,
			val2 int UNIQUE,
			PRIMARY KEY (id)
		);`)
	mustExecSQL(c, se, "insert into test(id, val1, val2) values(1, 1, 1);")
	mustExecSQL(c, se, "insert into test(id, val1, val2) values(2, 2, 2);")
	mustExecFailed(c, se, "update test set val1 = 3, val2 = 2 where id = 1;")
	mustExecSQL(c, se, "insert into test(id, val1, val2) values(3, 3, 3);")
	mustExecSQL(c, se, "drop table test;")

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue177(c *C) {
	defer testleak.AfterTest(c)()
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

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestBuiltin(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	// Testcase for https://github.com/pingcap/tidb/issues/382
	mustExecFailed(c, se, `select cast("xxx 10:10:10" as datetime)`)
	mustExecMatch(c, se, "select locate('bar', 'foobarbar')", [][]interface{}{{4}})

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestFieldText(c *C) {
	defer testleak.AfterTest(c)()
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
		c.Assert(fields[0].ColumnAsName.O, Equals, v.field)
	}

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIndexPointLookup(c *C) {
	defer testleak.AfterTest(c)()
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

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue454(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "drop table if exists t1")
	mustExecSQL(c, se, "create table t1 (c1 int, c2 int, c3 int);")
	mustExecSQL(c, se, "insert into t1 set c1=1, c2=2, c3=1;")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int, primary key (c1));")
	mustExecSQL(c, se, "insert into t set c1=1, c2=4;")
	mustExecSQL(c, se, "insert into t select * from t1 limit 1 on duplicate key update c3=3333;")

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue456(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "drop table if exists t1")
	mustExecSQL(c, se, "create table t1 (c1 int, c2 int, c3 int);")
	mustExecSQL(c, se, "replace into t1 set c1=1, c2=2, c3=1;")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int, primary key (c1));")
	mustExecSQL(c, se, "replace into t set c1=1, c2=4;")
	mustExecSQL(c, se, "replace into t select * from t1 limit 1;")

	err := store.Close()
	c.Assert(err, IsNil)
}

// For https://github.com/pingcap/tidb/issues/571
func (s *testSessionSuite) TestIssue571(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c int)")
	mustExecSQL(c, se, "insert t values (1), (2), (3)")
	mustExecSQL(c, se, "commit")

	se1 := newSession(c, store, s.dbName)
	se1.(*session).maxRetryCnt = unlimitedRetryCnt
	mustExecSQL(c, se1, "SET SESSION autocommit=1;")
	se2 := newSession(c, store, s.dbName)
	se2.(*session).maxRetryCnt = unlimitedRetryCnt
	mustExecSQL(c, se2, "SET SESSION autocommit=1;")
	se3 := newSession(c, store, s.dbName)
	se3.(*session).maxRetryCnt = unlimitedRetryCnt
	mustExecSQL(c, se3, "SET SESSION autocommit=0;")

	var wg sync.WaitGroup
	wg.Add(3)
	f1 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			mustExecSQL(c, se1, "update t set c = 1;")
		}
	}
	f2 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			mustExecSQL(c, se2, "update t set c = ?;", 1)
		}
	}
	f3 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			mustExecSQL(c, se3, "begin")
			mustExecSQL(c, se3, "update t set c = 1;")
			mustExecSQL(c, se3, "commit")
		}
	}
	go f1()
	go f2()
	go f3()
	wg.Wait()

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue620(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t1")
	mustExecSQL(c, se, "drop table if exists t2")
	mustExecSQL(c, se, "drop table if exists t3")
	mustExecSQL(c, se, "create table t1(id int primary key auto_increment, c int);")
	mustExecSQL(c, se, "create table t2(c int);")
	mustExecSQL(c, se, "insert into t2 values (1);")
	mustExecSQL(c, se, "create table t3(id int, c int);")
	mustExecSQL(c, se, "insert into t3 values (2,2);")
	mustExecSQL(c, se, "insert into t1(c) select * from t2; insert into t1 select * from t3;")
	mustExecMatch(c, se, "select * from t1;", [][]interface{}{{1, 1}, {2, 2}})

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestRetryPreparedStmt(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	se1 := newSession(c, store, s.dbName)
	se2 := newSession(c, store, s.dbName)

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int)")
	mustExecSQL(c, se, "insert t values (11, 2, 3)")

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2=? where c1=11;", 21)

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=? where c1=11", 22)
	mustExecSQL(c, se2, "commit")

	mustExecSQL(c, se1, "commit")

	se3 := newSession(c, store, s.dbName)
	r := mustExecSQL(c, se3, "select c2 from t where c1=11")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 21)

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIssue893(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t1; create table t1(id int ); insert into t1 values (1);")
	mustExecMatch(c, se, "select * from t1;", [][]interface{}{{1}})

	err := store.Close()
	c.Assert(err, IsNil)
}

// Testcase for session
func (s *testSessionSuite) TestSession(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "ROLLBACK;")

	err := se.Close()
	c.Assert(err, IsNil)
	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestSessionAuth(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	defer se.Close()
	c.Assert(se.Auth("Any not exist username with zero password! @anyhost", []byte(""), []byte("")), IsFalse)

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestErrorRollback(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	s1 := newSession(c, store, s.dbName)

	defer s1.Close()

	mustExecSQL(c, s1, "drop table if exists t_rollback")
	mustExecSQL(c, s1, "create table t_rollback (c1 int, c2 int, primary key(c1))")

	_, err := s1.Execute("insert into t_rollback values (0, 0)")
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	cnt := 4
	wg.Add(cnt)
	num := 100

	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			se := newSession(c, store, s.dbName)
			// retry forever
			se.(*session).maxRetryCnt = unlimitedRetryCnt
			defer se.Close()

			for j := 0; j < num; j++ {
				// force generate a txn in session for later insert use.
				se.(*session).GetTxn(false)

				se.Execute("insert into t_rollback values (1, 1)")

				_, err1 := se.Execute("update t_rollback set c2 = c2 + 1 where c1 = 0")
				c.Assert(err1, IsNil)
			}
		}()
	}

	wg.Wait()

	mustExecMatch(c, s1, "select c2 from t_rollback where c1 = 0", [][]interface{}{{cnt * num}})

	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestMultiColumnIndex(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c1 int, c2 int);")
	mustExecSQL(c, se, "create index idx_c1_c2 on t (c1, c2)")
	mustExecSQL(c, se, "insert into t values (1, 5)")

	sql := "select c1 from t where c1 in (1) and c2 < 10"
	expectedExplain := "Index(t.idx_c1_c2)->Fields"
	checkPlan(c, se, sql, expectedExplain)
	mustExecMatch(c, se, sql, [][]interface{}{{1}})

	sql = "select c1 from t where c1 in (1) and c2 > 3"
	checkPlan(c, se, sql, expectedExplain)
	mustExecMatch(c, se, sql, [][]interface{}{{1}})

	sql = "select c1 from t where c1 in (1.1) and c2 > 3"
	checkPlan(c, se, sql, expectedExplain)
	mustExecMatch(c, se, sql, [][]interface{}{})

	sql = "select c1 from t where c1 in (1) and c2 < 5.1"
	checkPlan(c, se, sql, expectedExplain)
	mustExecMatch(c, se, sql, [][]interface{}{{1}})

	// Test varchar type.
	mustExecSQL(c, se, "drop table t;")
	mustExecSQL(c, se, "create table t (c1 varchar(64), c2 varchar(64), index c1_c2 (c1, c2));")
	mustExecSQL(c, se, "insert into t values ('abc', 'def')")
	sql = "select c1 from t where c1 = 'abc'"
	mustExecMatch(c, se, sql, [][]interface{}{{[]byte("abc")}})

	err := se.Close()
	c.Assert(err, IsNil)
	err = store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestSubstringIndexExpr(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c varchar(128));")
	mustExecSQL(c, se, `insert into t values ("www.pingcap.com");`)
	mustExecMatch(c, se, "SELECT DISTINCT SUBSTRING_INDEX(c, '.', 2) from t;", [][]interface{}{{"www.pingcap"}})

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestIgnoreForeignKey(c *C) {
	c.Skip("skip panic")
	defer testleak.AfterTest(c)()
	sqlText := `CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, sqlText)

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestJoinSubquery(c *C) {
	defer testleak.AfterTest(c)()
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecSQL(c, se, "CREATE TABLE table1 (id INTEGER key AUTO_INCREMENT, data VARCHAR(30))")
	mustExecSQL(c, se, "CREATE TABLE table2 (id INTEGER key AUTO_INCREMENT, data VARCHAR(30), t1id INTEGER)")
	sqlTxt := `SELECT table1.id AS table1_id, table1.data AS table1_data FROM
	table1 INNER JOIN (
		SELECT table2.id AS id, table2.data AS data, table2.t1id AS t1id FROM table2
	) AS anon_1 ON table1.id = anon_1.t1id;`
	mustExecSQL(c, se, sqlTxt)

	err := store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestGlobalVarAccessor(c *C) {
	defer testleak.AfterTest(c)()

	varName := "max_allowed_packet"
	varValue := "4194304" // This is the default value for max_allowed_packet
	varValue1 := "4194305"
	varValue2 := "4194306"

	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName).(*session)
	// Get globalSysVar twice and get the same value
	v, err := se.GetGlobalSysVar(se, varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue)
	v, err = se.GetGlobalSysVar(se, varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue)
	// Set global var to another value
	err = se.SetGlobalSysVar(se, varName, varValue1)
	c.Assert(err, IsNil)
	v, err = se.GetGlobalSysVar(se, varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue1)
	c.Assert(se.FinishTxn(false), IsNil)

	// Change global variable value in another session
	se1 := newSession(c, store, s.dbName).(*session)
	v, err = se1.GetGlobalSysVar(se1, varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue1)
	err = se1.SetGlobalSysVar(se1, varName, varValue2)
	c.Assert(err, IsNil)
	v, err = se1.GetGlobalSysVar(se1, varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue2)
	c.Assert(se1.FinishTxn(false), IsNil)

	// Make sure the change is visible to any client that accesses that global variable.
	v, err = se.GetGlobalSysVar(se, varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue2)

	err = store.Close()
	c.Assert(err, IsNil)
}

func checkPlan(c *C, se Session, sql, explain string) {
	ctx := se.(context.Context)
	stmts, err := Parse(ctx, sql)
	c.Assert(err, IsNil)
	stmt := stmts[0]
	is := sessionctx.GetDomain(ctx).InfoSchema()
	err = optimizer.Prepare(is, ctx, stmt)
	c.Assert(err, IsNil)
	p, err := optimizer.Optimize(ctx, stmt, executor.NewSubQueryBuilder(is))
	c.Assert(err, IsNil)
	c.Assert(plan.ToString(p), Equals, explain)
}

func mustExecMultiSQL(c *C, se Session, sql string) {
	ss := strings.Split(sql, "\n")
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if len(s) == 0 {
			continue
		}
		mustExecSQL(c, se, s)
	}
}

func (s *testSessionSuite) TestSqlLogicTestCase(c *C) {
	initSQL := `
		CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT)
		INSERT INTO tab1 VALUES(0,26,690.51)
		CREATE INDEX idx_tab1_1 on tab1 (col1)`
	store := newStore(c, s.dbName)
	se := newSession(c, store, s.dbName)
	mustExecMultiSQL(c, se, initSQL)

	sql := "SELECT col0 FROM tab1 WHERE 71*22 >= col1"
	mustExecMatch(c, se, sql, [][]interface{}{{"26"}})
}
