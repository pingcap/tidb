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
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testDBSuite{})

const defaultBatchSize = 1024

type testDBSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	schemaName string
	tk         *testkit.TestKit
	s          tidb.Session
	lease      time.Duration
}

func (s *testDBSuite) SetUpSuite(c *C) {
	var err error

	s.lease = 200 * time.Millisecond
	tidb.SetSchemaLease(s.lease)
	s.schemaName = "test_db"
	s.store, err = tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	localstore.MockRemoteStore = true

	s.dom, err = tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	s.s, err = tidb.CreateSession(s.store)
	c.Assert(err, IsNil)

	_, err = s.s.Execute("create database test_db")
	c.Assert(err, IsNil)
	_, err = s.s.Execute("use " + s.schemaName)
	c.Assert(err, IsNil)
	_, err = s.s.Execute("create table t1 (c1 int, c2 int, c3 int, primary key(c1))")
	c.Assert(err, IsNil)
	_, err = s.s.Execute("create table t2 (c1 int, c2 int, c3 int)")
	c.Assert(err, IsNil)
}

func (s *testDBSuite) TearDownSuite(c *C) {
	localstore.MockRemoteStore = false
	s.s.Execute("drop database if exists test_db")
	s.s.Close()
	s.dom.Close()
	s.store.Close()
}

func (s *testDBSuite) testErrorCode(c *C, sql string, errCode int) {
	_, err := s.tk.Exec(sql)
	c.Assert(err, NotNil)
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	c.Assert(ok, IsTrue, Commentf("err: %T", originErr))
	c.Assert(tErr.ToSQLError().Code, DeepEquals, uint16(errCode), Commentf("MySQL code:%v", tErr.ToSQLError()))
}

func (s *testDBSuite) TestMySQLErrorCode(c *C) {
	defer testleak.AfterTest(c)
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	// create database
	sql := "create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s.testErrorCode(c, sql, tmysql.ErrTooLongIdent)
	sql = "create database test"
	s.testErrorCode(c, sql, tmysql.ErrDBCreateExists)
	// drop database
	sql = "drop database db_not_exist"
	s.testErrorCode(c, sql, tmysql.ErrDBDropExists)
	// crate table
	s.tk.MustExec("create table test_error_code_succ (c1 int, c2 int, c3 int, primary key(c3))")
	sql = "create table test_error_code_succ (c1 int, c2 int, c3 int)"
	s.testErrorCode(c, sql, tmysql.ErrTableExists)
	sql = "create table test_error_code1 (c1 int, c2 int, c2 int)"
	s.testErrorCode(c, sql, tmysql.ErrDupFieldName)
	sql = "create table test_error_code1 (c1 int, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)"
	s.testErrorCode(c, sql, tmysql.ErrTooLongIdent)
	sql = "create table aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa(a int)"
	s.testErrorCode(c, sql, tmysql.ErrTooLongIdent)
	sql = "create table test_error_code1 (c1 int, c2 int, key aa (c1, c2), key aa (c1))"
	s.testErrorCode(c, sql, tmysql.ErrDupKeyName)
	sql = "create table test_error_code1 (c1 int, c2 int, c3 int, key(c_not_exist))"
	s.testErrorCode(c, sql, tmysql.ErrKeyColumnDoesNotExits)
	sql = "create table test_error_code1 (c1 int, c2 int, c3 int, primary key(c_not_exist))"
	s.testErrorCode(c, sql, tmysql.ErrKeyColumnDoesNotExits)
	sql = "create table test_error_code1 (c1 int not null default '')"
	s.testErrorCode(c, sql, tmysql.ErrInvalidDefault)
	// add column
	sql = "alter table test_error_code_succ add column c1 int"
	s.testErrorCode(c, sql, tmysql.ErrDupFieldName)
	sql = "alter table test_error_code_succ add column aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int"
	s.testErrorCode(c, sql, tmysql.ErrTooLongIdent)
	// drop column
	sql = "alter table test_error_code_succ drop c_not_exist"
	s.testErrorCode(c, sql, tmysql.ErrCantDropFieldOrKey)
	// add index
	sql = "alter table test_error_code_succ add index idx (c_not_exist)"
	s.testErrorCode(c, sql, tmysql.ErrKeyColumnDoesNotExits)
	s.tk.Exec("alter table test_error_code_succ add index idx (c1)")
	sql = "alter table test_error_code_succ add index idx (c1)"
	s.testErrorCode(c, sql, tmysql.ErrDupKeyName)
	// drop index
	sql = "alter table test_error_code_succ drop index idx_not_exist"
	s.testErrorCode(c, sql, tmysql.ErrCantDropFieldOrKey)
	sql = "alter table test_error_code_succ drop column c3"
	s.testErrorCode(c, sql, int(tmysql.ErrUnknown))
	// modify column
	sql = "alter table test_error_code_succ modify testx.test_error_code_succ.c1 bigint"
	s.testErrorCode(c, sql, tmysql.ErrWrongDBName)
	sql = "alter table test_error_code_succ modify t.c1 bigint"
	s.testErrorCode(c, sql, tmysql.ErrWrongTableName)
}

func (s *testDBSuite) TestAddIndexAfterAddColumn(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("create table test_add_index_after_add_col(a int, b int not null default '0')")
	s.tk.MustExec("insert into test_add_index_after_add_col values(1, 2),(2,2)")
	s.tk.MustExec("alter table test_add_index_after_add_col add column c int not null default '0'")
	sql := "alter table test_add_index_after_add_col add unique index cc(c) "
	s.testErrorCode(c, sql, tmysql.ErrDupEntry)
}

func (s *testDBSuite) TestAddIndexWithPK(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("create table test_add_index_with_pk(a int not null, b int not null default '0', primary key(a))")
	s.tk.MustExec("insert into test_add_index_with_pk values(1, 2)")
	s.tk.MustExec("alter table test_add_index_with_pk add index idx (a)")
	s.tk.MustQuery("select a from test_add_index_with_pk").Check(testkit.Rows("1"))
	s.tk.MustExec("insert into test_add_index_with_pk values(2, 2)")
	s.tk.MustExec("alter table test_add_index_with_pk add index idx1 (a, b)")
	s.tk.MustQuery("select * from test_add_index_with_pk").Check(testkit.Rows("1 2", "2 2"))
	s.tk.MustExec("create table test_add_index_with_pk1(a int not null, b int not null default '0', c int, d int, primary key(c))")
	s.tk.MustExec("insert into test_add_index_with_pk1 values(1, 1, 1, 1)")
	s.tk.MustExec("alter table test_add_index_with_pk1 add index idx (c)")
	s.tk.MustExec("insert into test_add_index_with_pk1 values(2, 2, 2, 2)")
	s.tk.MustQuery("select * from test_add_index_with_pk1").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
	s.tk.MustExec("create table test_add_index_with_pk2(a int not null, b int not null default '0', c int unsigned, d int, primary key(c))")
	s.tk.MustExec("insert into test_add_index_with_pk2 values(1, 1, 1, 1)")
	s.tk.MustExec("alter table test_add_index_with_pk2 add index idx (c)")
	s.tk.MustExec("insert into test_add_index_with_pk2 values(2, 2, 2, 2)")
	s.tk.MustQuery("select * from test_add_index_with_pk2").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
}

func (s *testDBSuite) TestIndex(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.testAddIndex(c)
	s.testAddAnonymousIndex(c)
	s.testDropIndex(c)
	s.testAddUniqueIndexRollback(c)
	s.testAddIndexWithDupCols(c)
}

func (s *testDBSuite) testGetTable(c *C, name string) table.Table {
	ctx := s.s.(context.Context)
	domain := sessionctx.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := domain.Reload()
	c.Assert(err, IsNil)
	tbl, err := domain.InfoSchema().TableByName(model.NewCIStr(s.schemaName), model.NewCIStr(name))
	c.Assert(err, IsNil)
	return tbl
}

func backgroundExec(s kv.Storage, sql string, done chan error) {
	se, err := tidb.CreateSession(s)
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	defer se.Close()
	_, err = se.Execute("use test_db")
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	_, err = se.Execute(sql)
	done <- errors.Trace(err)
}

func (s *testDBSuite) testAddUniqueIndexRollback(c *C) {
	// t1 (c1 int, c2 int, c3 int, primary key(c1))
	s.mustExec(c, "delete from t1")
	// defaultBatchSize is equal to ddl.defaultBatchSize
	base := defaultBatchSize * 2
	count := base
	// add some rows
	for i := 0; i < count; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}
	// add some duplicate rows
	for i := count - 10; i < count; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i+10, i, i)
	}

	done := make(chan error, 1)
	go backgroundExec(s.store, "create unique index c3_index on t1 (c3)", done)

	times := 0
	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[kv:1062]Duplicate for key c3_index", Commentf("err:%v", err))
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 10
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				s.mustExec(c, "delete from t1 where c1 = ?", n)
				s.mustExec(c, "insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	t := s.testGetTable(c, "t1")
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, "c3_index"), IsFalse)
	}

	// delete duplicate rows, then add index
	for i := base - 10; i < base; i++ {
		s.mustExec(c, "delete from t1 where c1 = ?", i+10)
	}
	sessionExec(c, s.store, "create index c3_index on t1 (c3)")
}

func (s *testDBSuite) testAddAnonymousIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.mustExec(c, "create table t_anonymous_index (c1 int, c2 int, C3 int)")
	s.mustExec(c, "alter table t_anonymous_index add index (c1, c2)")
	// for dropping empty index
	_, err := s.tk.Exec("alter table t_anonymous_index drop index")
	c.Assert(err, NotNil)
	// The index name is c1 when adding index (c1, c2).
	s.mustExec(c, "alter table t_anonymous_index drop index c1")
	t := s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 0)
	// for adding some indices that the first column name is c1
	s.mustExec(c, "alter table t_anonymous_index add index (c1)")
	_, err = s.tk.Exec("alter table t_anonymous_index add index c1 (c2)")
	c.Assert(err, NotNil)
	t = s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 1)
	idx := t.Indices()[0].Meta().Name.L
	c.Assert(idx, Equals, "c1")
	// The MySQL will be a warning.
	s.mustExec(c, "alter table t_anonymous_index add index c1_3 (c1)")
	s.mustExec(c, "alter table t_anonymous_index add index (c1, c2, C3)")
	// The MySQL will be a warning.
	s.mustExec(c, "alter table t_anonymous_index add index (c1)")
	t = s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 4)
	s.mustExec(c, "alter table t_anonymous_index drop index c1")
	s.mustExec(c, "alter table t_anonymous_index drop index c1_2")
	s.mustExec(c, "alter table t_anonymous_index drop index c1_3")
	s.mustExec(c, "alter table t_anonymous_index drop index c1_4")
	// for case insensitive
	s.mustExec(c, "alter table t_anonymous_index add index (C3)")
	s.mustExec(c, "alter table t_anonymous_index drop index c3")
	s.mustExec(c, "alter table t_anonymous_index add index c3 (C3)")
	s.mustExec(c, "alter table t_anonymous_index drop index C3")
}

func (s *testDBSuite) testAlterLock(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.mustExec(c, "create table t_index_lock (c1 int, c2 int, C3 int)")
	s.mustExec(c, "alter table t_indx_lock add index (c1, c2), lock=none")
}

func (s *testDBSuite) testAddIndex(c *C) {
	done := make(chan error, 1)
	num := defaultBatchSize + 10
	// first add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}

	sessionExecInGoroutine(c, s.store, "create index c3_index on t1 (c3)", done)

	deletedKeys := make(map[int]struct{})

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// When the server performance is particularly poor,
			// the adding index operation can not be completed.
			// So here is a limit to the number of rows inserted.
			if num > defaultBatchSize*10 {
				break
			}
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
	expectedRows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		expectedRows = append(expectedRows, []interface{}{key})
	}
	rows := s.mustQuery(c, "select c1 from t1 where c3 >= 0")
	matchRows(c, rows, expectedRows)

	// test index range
	for i := 0; i < 100; i++ {
		index := rand.Intn(len(keys) - 3)
		rows := s.mustQuery(c, "select c1 from t1 where c3 >= ? limit 3", keys[index])
		matchRows(c, rows, [][]interface{}{{keys[index]}, {keys[index+1]}, {keys[index+2]}})
	}

	// TODO: Support explain in future.
	// rows := s.mustQuery(c, "explain select c1 from t1 where c3 >= 100")

	// ay := dumpRows(c, rows)
	// c.Assert(strings.Contains(fmt.Sprintf("%v", ay), "c3_index"), IsTrue)

	// get all row handles
	ctx := s.s.(context.Context)
	c.Assert(ctx.NewTxn(), IsNil)
	t := s.testGetTable(c, "t1")
	handles := make(map[int64]struct{})
	err := t.IterRecords(ctx, t.FirstKey(), t.Cols(),
		func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
			handles[h] = struct{}{}
			return true, nil
		})
	c.Assert(err, IsNil)

	// check in index
	var nidx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == "c3_index" {
			nidx = tidx
			break
		}
	}
	// Make sure there is index with name c3_index.
	c.Assert(nidx, NotNil)
	c.Assert(nidx.Meta().ID, Greater, int64(0))
	ctx.Txn().Rollback()

	c.Assert(ctx.NewTxn(), IsNil)
	defer ctx.Txn().Rollback()

	it, err := nidx.SeekFirst(ctx.Txn())
	c.Assert(err, IsNil)
	defer it.Close()

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		c.Assert(err, IsNil)
		_, ok := handles[h]
		c.Assert(ok, IsTrue)
		delete(handles, h)
	}

	c.Assert(handles, HasLen, 0)
}

func (s *testDBSuite) testDropIndex(c *C) {
	done := make(chan error, 1)
	s.mustExec(c, "delete from t1")

	num := 100
	//  add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}
	t := s.testGetTable(c, "t1")
	var c3idx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == "c3_index" {
			c3idx = tidx
			break
		}
	}
	c.Assert(c3idx, NotNil)

	sessionExecInGoroutine(c, s.store, "drop index c3_index on t1", done)

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
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
	c.Assert(strings.Contains(fmt.Sprintf("%v", rows), "c3_index"), IsFalse)

	// check in index, must no index in kv
	ctx := s.s.(context.Context)

	handles := make(map[int64]struct{})

	t = s.testGetTable(c, "t1")
	var nidx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == "c3_index" {
			nidx = tidx
			break
		}
	}
	// Make sure there is no index with name c3_index.
	c.Assert(nidx, IsNil)
	idx := tables.NewIndex(t.Meta(), c3idx.Meta())
	c.Assert(ctx.NewTxn(), IsNil)
	defer ctx.Txn().Rollback()

	it, err := idx.SeekFirst(ctx.Txn())
	c.Assert(err, IsNil)
	defer it.Close()

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		c.Assert(err, IsNil)
		handles[h] = struct{}{}
	}

	c.Assert(handles, HasLen, 0)
}

func (s *testDBSuite) testAddIndexWithDupCols(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	err1 := infoschema.ErrColumnExists.GenByArgs("b")
	err2 := infoschema.ErrColumnExists.GenByArgs("B")

	s.tk.MustExec("create table t (a int, b int)")
	_, err := s.tk.Exec("create index c on t(b, a, b)")
	c.Check(err1.Equal(err), Equals, true)

	_, err = s.tk.Exec("create index c on t(b, a, B)")
	c.Check(err2.Equal(err), Equals, true)

	_, err = s.tk.Exec("alter table t add index c (b, a, b)")
	c.Check(err1.Equal(err), Equals, true)

	_, err = s.tk.Exec("alter table t add index c (b, a, B)")
	c.Check(err2.Equal(err), Equals, true)
}

func (s *testDBSuite) showColumns(c *C, tableName string) [][]interface{} {
	return s.mustQuery(c, fmt.Sprintf("show columns from %s", tableName))
}

func (s *testDBSuite) TestIssue2293(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("create table t_issue_2293 (a int)")
	sql := "alter table t_issue_2293 add b int not null default 'a'"
	s.testErrorCode(c, sql, tmysql.ErrInvalidDefault)
	s.tk.MustExec("insert into t_issue_2293 value(1)")
	s.tk.MustQuery("select * from t_issue_2293").Check(testkit.Rows("1"))
}

func (s *testDBSuite) TestCreateIndexType(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	sql := `CREATE TABLE test_index (
		price int(5) DEFAULT '0' NOT NULL,
		area varchar(40) DEFAULT '' NOT NULL,
		type varchar(40) DEFAULT '' NOT NULL,
		transityes set('a','b'),
		shopsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		schoolsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		petsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		KEY price (price,area,type,transityes,shopsyes,schoolsyes,petsyes));`
	s.tk.MustExec(sql)
}

func (s *testDBSuite) TestColumn(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.testAddColumn(c)
	s.testDropColumn(c)
}

func sessionExec(c *C, s kv.Storage, sql string) {
	se, err := tidb.CreateSession(s)
	c.Assert(err, IsNil)
	_, err = se.Execute("use test_db")
	c.Assert(err, IsNil)
	rs, err := se.Execute(sql)
	c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
	c.Assert(rs, IsNil)
	se.Close()
}

func sessionExecInGoroutine(c *C, s kv.Storage, sql string, done chan error) {
	go func() {
		se, err := tidb.CreateSession(s)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		defer se.Close()
		_, err = se.Execute("use test_db")
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		rs, err := se.Execute(sql)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		if rs != nil {
			done <- errors.Errorf("RecordSet should be empty.")
			return
		}
		done <- nil
	}()
}

func (s *testDBSuite) testAddColumn(c *C) {
	done := make(chan error, 1)

	num := defaultBatchSize + 10
	// add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?)", i, i, i)
	}

	sessionExecInGoroutine(c, s.store, "alter table t2 add column c4 int default -1", done)

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				s.tk.MustExec("begin")
				s.tk.MustExec("delete from t2 where c1 = ?", n)
				s.tk.MustExec("commit")

				// Make sure that statement of insert and show use the same infoSchema.
				s.tk.MustExec("begin")
				_, err := s.tk.Exec("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// if err is failed, the column number must be 4 now.
					values := s.showColumns(c, "t2")
					c.Assert(values, HasLen, 4, Commentf("err:%v", errors.ErrorStack(err)))
				}
				s.tk.MustExec("commit")
			}
			num += step
		}
	}

	// add data, here c4 must exist
	for i := num; i < num+step; i++ {
		s.tk.MustExec("insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	rows := s.mustQuery(c, "select count(c4) from t2")
	c.Assert(rows, HasLen, 1)
	c.Assert(rows[0], HasLen, 1)
	count, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(count, Greater, int64(0))

	rows = s.mustQuery(c, "select count(c4) from t2 where c4 = -1")
	matchRows(c, rows, [][]interface{}{{count - int64(step)}})

	for i := num; i < num+step; i++ {
		rows := s.mustQuery(c, "select c4 from t2 where c4 = ?", i)
		matchRows(c, rows, [][]interface{}{{i}})
	}

	ctx := s.s.(context.Context)
	t := s.testGetTable(c, "t2")
	i := 0
	j := 0
	ctx.NewTxn()
	defer ctx.Txn().Rollback()
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(),
		func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
			i++
			// c4 must be -1 or > 0
			v, err1 := data[3].ToInt64(ctx.GetSessionVars().StmtCtx)
			c.Assert(err1, IsNil)
			if v == -1 {
				j++
			} else {
				c.Assert(v, Greater, int64(0))
			}
			return true, nil
		})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int(count))
	c.Assert(i, LessEqual, num+step)
	c.Assert(j, Equals, int(count)-step)

	// for modifying columns after adding columns
	s.tk.MustExec("alter table t2 modify c4 int default 11")
	for i := num + step; i < num+step+10; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}
	rows = s.mustQuery(c, "select count(c4) from t2 where c4 = -1")
	matchRows(c, rows, [][]interface{}{{count - int64(step)}})
}

func (s *testDBSuite) testDropColumn(c *C) {
	done := make(chan error, 1)
	s.mustExec(c, "delete from t2")

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	// get c4 column id
	sessionExecInGoroutine(c, s.store, "alter table t2 drop column c4", done)

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				// Make sure that statement of insert and show use the same infoSchema.
				s.tk.MustExec("begin")
				_, err := s.tk.Exec("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// If executing is failed, the column number must be 4 now.
					values := s.showColumns(c, "t2")
					c.Assert(values, HasLen, 4, Commentf("err:%v", errors.ErrorStack(err)))
				}
				s.tk.MustExec("commit")
			}
			num += step
		}
	}

	// add data, here c4 must not exist
	for i := num; i < num+step; i++ {
		s.mustExec(c, "insert into t2 values (?, ?, ?)", i, i, i)
	}

	rows := s.mustQuery(c, "select count(*) from t2")
	c.Assert(rows, HasLen, 1)
	c.Assert(rows[0], HasLen, 1)
	count, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(count, Greater, int64(0))
}

func (s *testDBSuite) TestPrimaryKey(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.mustExec(c, "create table primary_key_test (a int, b varchar(10))")
	_, err := s.tk.Exec("alter table primary_key_test add primary key(a)")
	c.Assert(ddl.ErrUnsupportedModifyPrimaryKey.Equal(err), IsTrue)
	_, err = s.tk.Exec("alter table primary_key_test drop primary key")
	c.Assert(ddl.ErrUnsupportedModifyPrimaryKey.Equal(err), IsTrue)
}

func (s *testDBSuite) TestChangeColumn(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.mustExec(c, "create table t3 (a int default '0', b varchar(10), d int not null default '0')")
	s.mustExec(c, "insert into t3 set b = 'a'")
	s.tk.MustQuery("select a from t3").Check(testkit.Rows("0"))
	s.mustExec(c, "alter table t3 change a aa bigint")
	s.mustExec(c, "insert into t3 set b = 'b'")
	s.tk.MustQuery("select aa from t3").Check(testkit.Rows("0", "<nil>"))
	// for no default flag
	s.mustExec(c, "alter table t3 change d dd bigint not null")
	ctx := s.tk.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colD := tblInfo.Columns[2]
	hasNoDefault := tmysql.HasNoDefaultValueFlag(colD.Flag)
	c.Assert(hasNoDefault, IsTrue)
	// for the following definitions: 'not null', 'null', 'default value' and 'comment'
	s.mustExec(c, "alter table t3 change b b varchar(20) null default 'c' comment 'my comment'")
	is = sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colB := tblInfo.Columns[1]
	c.Assert(colB.Comment, Equals, "my comment")
	hasNotNull := tmysql.HasNotNullFlag(colB.Flag)
	c.Assert(hasNotNull, IsFalse)
	s.mustExec(c, "insert into t3 set aa = 3, dd = 5")
	s.tk.MustQuery("select b from t3").Check(testkit.Rows("a", "b", "c"))
	// for timestamp
	s.mustExec(c, "alter table t3 add column c timestamp not null")
	s.mustExec(c, "alter table t3 change c c timestamp null default '2017-02-11' comment 'col c comment' on update current_timestamp")
	is = sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC := tblInfo.Columns[3]
	c.Assert(colC.Comment, Equals, "col c comment")
	hasNotNull = tmysql.HasNotNullFlag(colC.Flag)
	c.Assert(hasNotNull, IsFalse)
	// for enum
	s.mustExec(c, "alter table t3 add column en enum('a', 'b', 'c') not null default 'a'")

	// for failing tests
	sql := "alter table t3 change aa a bigint default ''"
	s.testErrorCode(c, sql, tmysql.ErrInvalidDefault)
	sql = "alter table t3 change a testx.t3.aa bigint"
	s.testErrorCode(c, sql, tmysql.ErrWrongDBName)
	sql = "alter table t3 change t.a aa bigint"
	s.testErrorCode(c, sql, tmysql.ErrWrongTableName)
	sql = "alter table t3 change aa a bigint not null"
	s.testErrorCode(c, sql, tmysql.ErrUnknown)
	sql = "alter table t3 modify en enum('a', 'z', 'b', 'c') not null default 'a'"
	s.testErrorCode(c, sql, tmysql.ErrUnknown)
}

func (s *testDBSuite) TestAlterColumn(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.mustExec(c, "create table test_alter_column (a int default 111, b varchar(8), c varchar(8) not null, d timestamp on update current_timestamp)")
	s.mustExec(c, "insert into test_alter_column set b = 'a', c = 'aa'")
	s.tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111"))
	ctx := s.tk.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colA := tblInfo.Columns[0]
	hasNoDefault := tmysql.HasNoDefaultValueFlag(colA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	s.mustExec(c, "alter table test_alter_column alter column a set default 222")
	s.mustExec(c, "insert into test_alter_column set b = 'b', c = 'bb'")
	s.tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222"))
	is = sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colA = tblInfo.Columns[0]
	hasNoDefault = tmysql.HasNoDefaultValueFlag(colA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	s.mustExec(c, "alter table test_alter_column alter column b set default null")
	s.mustExec(c, "insert into test_alter_column set c = 'cc'")
	s.tk.MustQuery("select b from test_alter_column").Check(testkit.Rows("a", "b", "<nil>"))
	is = sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC := tblInfo.Columns[2]
	hasNoDefault = tmysql.HasNoDefaultValueFlag(colC.Flag)
	c.Assert(hasNoDefault, IsTrue)
	s.mustExec(c, "alter table test_alter_column alter column c set default 'xx'")
	s.mustExec(c, "insert into test_alter_column set a = 123")
	s.tk.MustQuery("select c from test_alter_column").Check(testkit.Rows("aa", "bb", "cc", "xx"))
	is = sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC = tblInfo.Columns[2]
	hasNoDefault = tmysql.HasNoDefaultValueFlag(colC.Flag)
	c.Assert(hasNoDefault, IsFalse)
	// TODO: After fix issue 2606.
	// s.mustExec(c, "alter table test_alter_column alter column d set default null")
	s.mustExec(c, "alter table test_alter_column alter column a drop default")
	s.mustExec(c, "insert into test_alter_column set b = 'd', c = 'dd'")
	s.tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222", "222", "123", "<nil>"))

	// for failing tests
	sql := "alter table db_not_exist.test_alter_column alter column b set default 'c'"
	s.testErrorCode(c, sql, tmysql.ErrNoSuchTable)
	sql = "alter table test_not_exist alter column b set default 'c'"
	s.testErrorCode(c, sql, tmysql.ErrNoSuchTable)
	sql = "alter table test_alter_column alter column col_not_exist set default 'c'"
	s.testErrorCode(c, sql, tmysql.ErrBadField)
	sql = "alter table test_alter_column alter column c set default null"
	s.testErrorCode(c, sql, tmysql.ErrInvalidDefault)

	// The followings tests whether adding constraints via change / modify column
	// is forbidden as expected.
	s.mustExec(c, "drop table if exists mc")
	s.mustExec(c, "create table mc(a int key, b int, c int)")
	_, err = s.tk.Exec("alter table mc modify column a int key") // Adds a new primary key
	c.Assert(err, NotNil)
	_, err = s.tk.Exec("alter table mc modify column c int unique") // Adds a new unique key
	c.Assert(err, NotNil)
	result := s.tk.MustQuery("show create table mc")
	createSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	c.Assert(createSQL, Equals, expected)

	// Change / modify column should preserve index options.
	s.mustExec(c, "drop table if exists mc")
	s.mustExec(c, "create table mc(a int key, b int, c int unique)")
	s.mustExec(c, "alter table mc modify column a bigint") // NOT NULL & PRIMARY KEY should be preserved
	s.mustExec(c, "alter table mc modify column b bigint")
	s.mustExec(c, "alter table mc modify column c bigint") // Unique should be preserved
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(21) NOT NULL,\n  `b` bigint(21) DEFAULT NULL,\n  `c` bigint(21) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `c` (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	c.Assert(createSQL, Equals, expected)

	// Dropping or keeping auto_increment is allowed, however adding is not allowed.
	s.mustExec(c, "drop table if exists mc")
	s.mustExec(c, "create table mc(a int key auto_increment, b int)")
	s.mustExec(c, "alter table mc modify column a bigint auto_increment") // Keeps auto_increment
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(21) NOT NULL AUTO_INCREMENT,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	s.mustExec(c, "alter table mc modify column a bigint") // Drops auto_increment
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(21) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	c.Assert(createSQL, Equals, expected)
	_, err = s.tk.Exec("alter table mc modify column a bigint auto_increment") // Adds auto_increment should throw error
	c.Assert(err, NotNil)
}

func (s *testDBSuite) mustExec(c *C, query string, args ...interface{}) {
	s.tk.MustExec(query, args...)
}

func (s *testDBSuite) mustQuery(c *C, query string, args ...interface{}) [][]interface{} {
	r := s.tk.MustQuery(query, args...)
	return r.Rows()
}

func matchRows(c *C, rows [][]interface{}, expected [][]interface{}) {
	c.Assert(len(rows), Equals, len(expected), Commentf("got %v, expected %v", rows, expected))
	for i := range rows {
		match(c, rows[i], expected[i]...)
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

func (s *testDBSuite) TestUpdateMultipleTable(c *C) {
	defer testleak.AfterTest(c)
	store, err := tidb.NewStore("memory://update_multiple_table")
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("insert t1 values (1, 1), (2, 2)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert t2 values (1, 3), (2, 5)")
	ctx := tk.Se.(context.Context)
	domain := sessionctx.GetDomain(ctx)
	is := domain.InfoSchema()
	db, ok := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(ok, IsTrue)
	t1Tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t1Info := t1Tbl.Meta()

	// Add a new column in write only state.
	newColumn := &model.ColumnInfo{
		ID:           100,
		Name:         model.NewCIStr("c3"),
		Offset:       2,
		DefaultValue: 9,
		FieldType:    *types.NewFieldType(tmysql.TypeLonglong),
		State:        model.StateWriteOnly,
	}
	t1Info.Columns = append(t1Info.Columns, newColumn)

	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, t1Info), IsNil)
		return nil
	})
	err = domain.Reload()
	c.Assert(err, IsNil)

	tk.MustExec("update t1, t2 set t1.c1 = 8, t2.c2 = 10 where t1.c2 = t2.c1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1", "8 2"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 10", "2 10"))

	newColumn.State = model.StatePublic

	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, t1Info), IsNil)
		return nil
	})
	err = domain.Reload()
	c.Assert(err, IsNil)

	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1 9", "8 2 9"))
}

func (s *testDBSuite) TestCreateTableWithLike(c *C) {
	defer testleak.AfterTest(c)
	store, err := tidb.NewStore("memory://create_table_like")
	c.Assert(err, IsNil)
	s.tk = testkit.NewTestKit(c, store)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)

	// for the same database
	s.tk.MustExec("use test")
	s.tk.MustExec("create table tt(id int primary key)")
	s.tk.MustExec("create table t (c1 int not null auto_increment, c2 int, constraint cc foreign key (c2) references tt(id), primary key(c1)) auto_increment = 10")
	s.tk.MustExec("insert into t set c2=1")
	s.tk.MustExec("create table t1 like test.t")
	s.tk.MustExec("insert into t1 set c2=11")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("10 1"))
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	ctx := s.tk.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	c.Assert(tblInfo.ForeignKeys, IsNil)
	c.Assert(tblInfo.PKIsHandle, Equals, true)
	col := tblInfo.Columns[0]
	hasNotNull := tmysql.HasNotNullFlag(col.Flag)
	c.Assert(hasNotNull, IsTrue)
	// for different databases
	s.tk.MustExec("create database test1")
	s.tk.MustExec("use test1")
	s.tk.MustExec("create table t1 like test.t")
	s.tk.MustExec("insert into t1 set c2=11")
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	is = sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().ForeignKeys, IsNil)

	// for failure cases
	failSQL := fmt.Sprintf("create table t1 like test_not_exist.t")
	s.testErrorCode(c, failSQL, tmysql.ErrNoSuchTable)
	failSQL = fmt.Sprintf("create table t1 like test.t_not_exist")
	s.testErrorCode(c, failSQL, tmysql.ErrNoSuchTable)
	failSQL = fmt.Sprintf("create table test_not_exis.t1 like test.t")
	s.testErrorCode(c, failSQL, tmysql.ErrBadDB)
	failSQL = fmt.Sprintf("create table t1 like test.t")
	s.testErrorCode(c, failSQL, tmysql.ErrTableExists)
}

func (s *testDBSuite) TestTruncateTable(c *C) {
	defer testleak.AfterTest(c)
	store, err := tidb.NewStore("memory://truncate_table")
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2)")
	ctx := tk.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID

	tk.MustExec("truncate table t")

	tk.MustExec("insert t values (3, 3), (4, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("3 3", "4 4"))

	is = sessionctx.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Greater, oldTblID)

	// Verify that the old table data has been deleted by background worker.
	tablePrefix := tablecodec.EncodeTablePrefix(oldTblID)
	hasOldTableData := true
	for i := 0; i < 30; i++ {
		err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
			it, err1 := txn.Seek(tablePrefix)
			if err1 != nil {
				return err1
			}
			if !it.Valid() {
				hasOldTableData = false
			} else {
				hasOldTableData = it.Key().HasPrefix(tablePrefix)
			}
			it.Close()
			return nil
		})
		c.Assert(err, IsNil)
		if !hasOldTableData {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	c.Assert(hasOldTableData, IsFalse)
}

func (s *testDBSuite) TestRenameTable(c *C) {
	s.testRenameTable(c, "rename_table", "rename table %s to %s")
}

func (s *testDBSuite) TestAlterTableRenameTable(c *C) {
	s.testRenameTable(c, "alter_table_rename_table", "alter table %s rename to %s")
}

func (s *testDBSuite) testRenameTable(c *C, storeStr, sql string) {
	defer testleak.AfterTest(c)
	store, err := tidb.NewStore("memory://" + storeStr)
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	s.tk = testkit.NewTestKit(c, store)
	s.tk.MustExec("use test")

	// for different databases
	s.tk.MustExec("create table t (c1 int, c2 int)")
	s.tk.MustExec("insert t values (1, 1), (2, 2)")
	ctx := s.tk.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID
	s.tk.MustExec("create database test1")
	s.tk.MustExec("use test1")
	s.tk.MustExec(fmt.Sprintf(sql, "test.t", "test1.t1"))
	is = sessionctx.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	s.tk.MustExec("use test")
	s.tk.MustQuery("show tables").Check(testkit.Rows())

	// for the same database
	s.tk.MustExec("use test1")
	s.tk.MustExec(fmt.Sprintf(sql, "t1", "t2"))
	is = sessionctx.GetDomain(ctx).InfoSchema()
	newTblInfo, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	s.tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))
	isExist := is.TableExists(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(isExist, IsFalse)
	s.tk.MustQuery("show tables").Check(testkit.Rows("t2"))

	// for failure case
	failSQL := fmt.Sprintf(sql, "test_not_exist.t", "test_not_exist.t")
	s.testErrorCode(c, failSQL, tmysql.ErrFileNotFound)
	failSQL = fmt.Sprintf(sql, "test.t_not_exist", "test_not_exist.t")
	s.testErrorCode(c, failSQL, tmysql.ErrFileNotFound)
	failSQL = fmt.Sprintf(sql, "test1.t2", "test_not_exist.t")
	s.testErrorCode(c, failSQL, tmysql.ErrErrorOnRename)
	failSQL = fmt.Sprintf(sql, "test1.t2", "test1.t2")
	s.testErrorCode(c, failSQL, tmysql.ErrTableExists)
}

func (s *testDBSuite) TestAddNotNullColumn(c *C) {
	defer testleak.AfterTest(c)
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test_db")
	// for different databases
	s.tk.MustExec("create table tnn (c1 int primary key auto_increment, c2 int)")
	s.tk.MustExec("insert tnn (c2) values (0)" + strings.Repeat(",(0)", 99))
	done := make(chan error, 1)
	sessionExecInGoroutine(c, s.store, "alter table tnn add column c3 int not null default 3", done)
	updateCnt := 0
out:
	for {
		select {
		case err := <-done:
			c.Assert(err, IsNil)
			break out
		default:
			s.tk.MustExec("update tnn set c2 = c2 + 1 where c1 = 99")
			updateCnt++
		}
	}
	expected := fmt.Sprintf("%d %d", updateCnt, 3)
	s.tk.MustQuery("select c2, c3 from tnn where c1 = 99").Check(testkit.Rows(expected))
}

func (s *testDBSuite) TestIssue2858And2717(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("create table t_issue_2858_bit (a bit(64) default b'0')")
	s.tk.MustExec("insert into t_issue_2858_bit value ()")
	s.tk.MustExec(`insert into t_issue_2858_bit values (100), ('10'), ('\0')`)
	s.tk.MustQuery("select a+0 from t_issue_2858_bit").Check(testkit.Rows("0", "100", "12592", "0"))
	s.tk.MustExec(`alter table t_issue_2858_bit alter column a set default '\0'`)

	s.tk.MustExec("create table t_issue_2858_hex (a int default 0x123)")
	s.tk.MustExec("insert into t_issue_2858_hex value ()")
	s.tk.MustExec("insert into t_issue_2858_hex values (123), (0x321)")
	s.tk.MustQuery("select a from t_issue_2858_hex").Check(testkit.Rows("291", "123", "801"))
	s.tk.MustExec(`alter table t_issue_2858_hex alter column a set default 0x321`)
}

func (s *testDBSuite) TestChangeColumnPosition(c *C) {
	defer testleak.AfterTest(c)()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("create table position (a int default 1, b int default 2)")
	s.tk.MustExec("insert into position value ()")
	s.tk.MustExec("insert into position values (3,4)")
	s.tk.MustQuery("select * from position").Check(testkit.Rows("1 2", "3 4"))
	s.tk.MustExec("alter table position modify column b int first")
	s.tk.MustQuery("select * from position").Check(testkit.Rows("2 1", "4 3"))
	s.tk.MustExec("insert into position value ()")
	s.tk.MustQuery("select * from position").Check(testkit.Rows("2 1", "4 3", "<nil> 1"))

	s.tk.MustExec("drop table position")
	s.tk.MustExec("create table position (a int, b int, c double, d varchar(5))")
	s.tk.MustExec(`insert into position value (1, 2, 3.14, 'TiDB')`)
	s.tk.MustExec("alter table position modify column d varchar(5) after a")
	s.tk.MustQuery("select * from position").Check(testkit.Rows("1 TiDB 2 3.14"))
	s.tk.MustExec("alter table position modify column a int after c")
	s.tk.MustQuery("select * from position").Check(testkit.Rows("TiDB 2 3.14 1"))
	s.tk.MustExec("alter table position modify column c double first")
	s.tk.MustQuery("select * from position").Check(testkit.Rows("3.14 TiDB 2 1"))
	s.testErrorCode(c, "alter table position modify column b int after b", tmysql.ErrBadField)

	s.tk.MustExec("drop table position")
	s.tk.MustExec("create table position (a int, b int)")
	s.tk.MustExec("alter table position add index t(a, b)")
	s.tk.MustExec("alter table position modify column b int first")
	s.tk.MustExec("insert into position value (3, 5)")
	s.tk.MustQuery("select a from position where a = 3").Check(testkit.Rows())

	s.tk.MustExec("alter table position change column b c int first")
	s.tk.MustQuery("select * from position where c = 3").Check(testkit.Rows("3 5"))
	s.testErrorCode(c, "alter table position change column c b int after c", tmysql.ErrBadField)

	s.tk.MustExec("drop table position")
	s.tk.MustExec("create table position (a int default 2)")
	s.tk.MustExec("alter table position modify column a int default 5 first")
	s.tk.MustExec("insert into position value ()")
	s.tk.MustQuery("select * from position").Check(testkit.Rows("5"))

	s.tk.MustExec("drop table position")
	s.tk.MustExec("create table position (a int, b int)")
	s.tk.MustExec("alter table position add index t(b)")
	s.tk.MustExec("alter table position change column b c int first")
	createSQL := s.tk.MustQuery("show create table position").Rows()[0][1]
	exceptedSQL := []string{
		"CREATE TABLE `position` (",
		"  `c` int(11) DEFAULT NULL,",
		"  `a` int(11) DEFAULT NULL,",
		"  KEY `t` (`c`)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
	}
	c.Assert(createSQL, Equals, strings.Join(exceptedSQL, "\n"))
}

func (s *testDBSuite) TestGeneratedColumnDDL(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")

	// Check create table with virtual generated column.
	s.tk.MustExec(`CREATE TABLE test_gv_ddl(a int, b int as (a+8) virtual)`)

	// Check desc table with virtual generated column.
	result := s.tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`))

	// Check show create table with virtual generated column.
	result = s.tk.MustQuery(`show create table test_gv_ddl`)
	result.Check(testkit.Rows(
		"test_gv_ddl CREATE TABLE `test_gv_ddl` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) GENERATED ALWAYS AS (a+8) VIRTUAL DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
	))

	// Check alter table add a stored generated column.
	s.tk.MustExec(`alter table test_gv_ddl add column c int as (b+2) stored`)
	result = s.tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	genExprTests := []struct {
		stmt string
		err  int
	}{
		// drop/rename columns dependent by other column.
		{`alter table test_gv_ddl drop column a`, mysql.ErrDependentByGeneratedColumn},
		{`alter table test_gv_ddl change column a anew int`, mysql.ErrBadField},

		// modify/change stored status of generated columns.
		{`alter table test_gv_ddl modify column b bigint`, mysql.ErrUnsupportedOnGeneratedColumn},
		{`alter table test_gv_ddl change column c cnew bigint as (a+100)`, mysql.ErrUnsupportedOnGeneratedColumn},

		// modify/change generated columns breaking prior.
		{`alter table test_gv_ddl modify column b int as (c+100)`, mysql.ErrGeneratedColumnNonPrior},
		{`alter table test_gv_ddl change column b bnew int as (c+100)`, mysql.ErrGeneratedColumnNonPrior},

		// refer not exist columns in generation expression.
		{`create table test_gv_ddl_bad (a int, b int as (c+8))`, mysql.ErrBadField},

		// refer generated columns non prior.
		{`create table test_gv_ddl_bad (a int, b int as (c+1), c int as (a+1))`, mysql.ErrGeneratedColumnNonPrior},

		// virtual generated columns cannot be primary key.
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b) primary key)`, mysql.ErrUnsupportedOnGeneratedColumn},
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b), primary key(c))`, mysql.ErrUnsupportedOnGeneratedColumn},
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b), primary key(a, c))`, mysql.ErrUnsupportedOnGeneratedColumn},
	}
	for _, tt := range genExprTests {
		s.testErrorCode(c, tt.stmt, tt.err)
	}

	// Check alter table modify/change generated column.
	s.tk.MustExec(`alter table test_gv_ddl modify column c bigint as (b+200) stored`)
	result = s.tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c bigint(21) YES  <nil> STORED GENERATED`))

	s.tk.MustExec(`alter table test_gv_ddl change column b b bigint as (a+100) virtual`)
	result = s.tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(21) YES  <nil> VIRTUAL GENERATED`, `c bigint(21) YES  <nil> STORED GENERATED`))

	s.tk.MustExec(`alter table test_gv_ddl change column c cnew bigint`)
	result = s.tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(21) YES  <nil> VIRTUAL GENERATED`, `cnew bigint(21) YES  <nil> `))
}
