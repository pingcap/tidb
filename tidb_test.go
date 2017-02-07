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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var store = flag.String("store", "memory", "registered store name, [memory, goleveldb, boltdb]")

func TestT(t *testing.T) {
	logLevel := os.Getenv("log_level")
	log.SetLevelByString(logLevel)
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testMainSuite{})

type testMainSuite struct {
	dbName         string
	createTableSQL string
	selectSQL      string
	store          kv.Storage
	dom            *domain.Domain
}

type brokenStore struct{}

func (s *brokenStore) Open(schema string) (kv.Storage, error) {
	return nil, errors.New("try again later")
}

func (s *testMainSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.dbName = "test_main_db"
	s.createTableSQL = `
    CREATE TABLE tbl_test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));
    CREATE TABLE tbl_test1(id INT NOT NULL DEFAULT 2, name varchar(255), PRIMARY KEY(id), INDEX name(name));
    CREATE TABLE tbl_test2(id INT NOT NULL DEFAULT 3, name varchar(255), PRIMARY KEY(id));`
	s.selectSQL = `SELECT * from tbl_test;`
	s.store = newStore(c, s.dbName)
	dom, err := BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom = dom
}

func (s *testMainSuite) TearDownSuite(c *C) {
	defer testleak.AfterTest(c)()
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
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
	se := newSession(c, s.store, dbName)
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
		sess := newSession(c, s.store, dbName)
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
	dbName := "test_info_meta"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

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
	mustExecSQL(c, se, dropDBSQL)
}

func (s *testMainSuite) TestInfoSchema(c *C) {
	se := newSession(c, s.store, "test_info_schema")
	rs := mustExecSQL(c, se, "SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'")
	row, err := rs.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "utf8mb4")
}

func (s *testMainSuite) TestCaseInsensitive(c *C) {
	dbName := "test_case_insensitive"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)

	se := newSession(c, s.store, dbName)
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
	rs = mustExecSQL(c, se, "select a as A from t where A > 0")
	fields, err = rs.Fields()
	c.Assert(err, IsNil)
	c.Assert(fields[0].ColumnAsName.O, Equals, "A")
	mustExecSQL(c, se, "update T set b = B + 1")
	mustExecSQL(c, se, "update T set B = b + 1")
	rs = mustExecSQL(c, se, "select b from T")
	rows, err := GetRows(rs)
	c.Assert(err, IsNil)
	match(c, rows[0], 3)

	mustExecSQL(c, se, dropDBSQL)
}

// Testcase for delete panic
func (s *testMainSuite) TestDeletePanic(c *C) {
	se := newSession(c, s.store, "test_delete_panic")
	mustExecSQL(c, se, "create table t (c int)")
	mustExecSQL(c, se, "insert into t values (1), (2), (3)")
	mustExecSQL(c, se, "delete from `t` where `c` = ?", 1)
	rs := mustExecSQL(c, se, "delete from `t` where `c` = ?", 2)
	c.Assert(rs, IsNil)
}

// Testcase for arg type.
func (s *testMainSuite) TestCheckArgs(c *C) {
	dbName := "test_check_args"
	se := newSession(c, s.store, dbName)
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

// TODO: Merge TestIssue1435 in session test.
func (s *testMainSuite) TestSchemaValidity(c *C) {
	localstore.MockRemoteStore = true
	store := newStoreWithBootstrap(c, s.dbName+"schema_validity")
	dbName := "test_schema_validity"
	se := newSession(c, store, dbName)
	se1 := newSession(c, store, dbName)
	se2 := newSession(c, store, dbName)

	ctx := se.(context.Context)
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (a int);")
	mustExecSQL(c, se, "drop table if exists t1;")
	mustExecSQL(c, se, "create table t1 (a int);")
	mustExecSQL(c, se, "drop table if exists t2;")
	mustExecSQL(c, se, "create table t2 (a int);")
	startCh1 := make(chan struct{})
	startCh2 := make(chan struct{})
	endCh1 := make(chan error)
	endCh2 := make(chan error)
	execFailedFunc := func(s Session, tbl string, start chan struct{}, end chan error) {
		// execute successfully
		_, err := exec(s, "begin;")
		c.Check(err, IsNil)
		<-start
		<-start

		_, err = exec(s, fmt.Sprintf("insert into %s values(1)", tbl))
		c.Check(err, IsNil)

		// table t1 executes failed
		// table t2 executes successfully
		_, err = exec(s, "commit")
		end <- err
	}

	go execFailedFunc(se1, "t1", startCh1, endCh1)
	go execFailedFunc(se2, "t2", startCh2, endCh2)
	// Make sure two insert transactions are begin.
	startCh1 <- struct{}{}
	startCh2 <- struct{}{}

	select {
	case <-endCh1:
		// Make sure the first insert statement isn't finish.
		c.Error("The statement shouldn't be executed")
		c.FailNow()
	default:
	}
	// Make sure loading information schema is failed and server is invalid.
	sessionctx.GetDomain(ctx).MockReloadFailed.SetValue(true)
	sessionctx.GetDomain(ctx).Reload()
	lease := sessionctx.GetDomain(ctx).DDL().GetLease()
	time.Sleep(lease + time.Millisecond) // time.Sleep maybe not very reliable
	// Make sure insert to table t1 transaction executes.
	startCh1 <- struct{}{}
	// Make sure executing insert statement is failed when server is invalid.
	mustExecFailed(c, se, "insert t values (100);")
	err := <-endCh1
	c.Assert(err, NotNil)

	// recover
	select {
	case <-endCh2:
		// Make sure the second insert statement isn't finish.
		c.Error("The statement shouldn't be executed")
		c.FailNow()
	default:
	}

	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	c.Assert(ver, NotNil)
	sessionctx.GetDomain(ctx).MockReloadFailed.SetValue(false)
	sessionctx.GetDomain(ctx).Reload()
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (a int);")
	mustExecSQL(c, se, "insert t values (1);")
	// Make sure insert to table t2 transaction executes.
	startCh2 <- struct{}{}
	err = <-endCh2
	c.Assert(err, IsNil, Commentf("err:%v", err))

	err = se.Close()
	c.Assert(err, IsNil)
	err = se1.Close()
	c.Assert(err, IsNil)
	err = se2.Close()
	c.Assert(err, IsNil)
	sessionctx.GetDomain(ctx).Close()
	err = store.Close()
	c.Assert(err, IsNil)
	localstore.MockRemoteStore = false
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

func newStoreWithBootstrap(c *C, dbPath string) kv.Storage {
	store := newStore(c, dbPath)
	BootstrapSession(store)
	return store
}

var testConnID uint64

func newSession(c *C, store kv.Storage, dbName string) Session {
	se, err := CreateSession(store)
	id := atomic.AddUint64(&testConnID, 1)
	se.SetConnectionID(id)
	c.Assert(err, IsNil)
	se.GetSessionVars().SkipDDLWait = true
	se.Auth(`root@%`, nil, []byte("012345678901234567890"))
	mustExecSQL(c, se, "create database if not exists "+dbName)
	mustExecSQL(c, se, "use "+dbName)
	return se
}

func removeStore(c *C, dbPath string) {
	os.RemoveAll(dbPath)
}

func exec(se Session, sql string, args ...interface{}) (ast.RecordSet, error) {
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
	rs, err := exec(se, sql, args...)
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
	r, err := exec(se, sql, args...)
	if err == nil && r != nil {
		// sometimes we may meet error after executing first row.
		_, err = r.Next()
	}
	c.Assert(err, NotNil)
}
