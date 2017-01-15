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
	"strings"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
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
	_, err := s.tk.Exec("alter table t add b int not null default ''")
	c.Assert(err, NotNil)
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
	s.testChangeColumn(c)
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
	count, ok := rows[0][0].(int64)
	c.Assert(ok, IsTrue)
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
	err := t.IterRecords(ctx, t.FirstKey(), t.Cols(),
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
	count, ok := rows[0][0].(int64)
	c.Assert(ok, IsTrue)
	c.Assert(count, Greater, int64(0))
}

func (s *testDBSuite) testChangeColumn(c *C) {
	s.mustExec(c, "create table t3 (a int, b varchar(10))")
	s.mustExec(c, "insert into t3 values(1, 'a'), (2, 'b')")
	s.tk.MustQuery("select a from t3").Check(testkit.Rows("1", "2"))
	s.mustExec(c, "alter table t3 change a aa bigint")
	s.tk.MustQuery("select aa from t3").Check(testkit.Rows("1", "2"))
	sql := "alter table t3 change a testx.t3.aa bigint"
	s.testErrorCode(c, sql, tmysql.ErrWrongDBName)
	sql = "alter table t3 change t.a aa bigint"
	s.testErrorCode(c, sql, tmysql.ErrWrongTableName)
}

func (s *testDBSuite) mustExec(c *C, query string, args ...interface{}) {
	s.tk.MustExec(query, args...)
}

func (s *testDBSuite) mustQuery(c *C, query string, args ...interface{}) [][]interface{} {
	r := s.tk.MustQuery(query, args...)
	return r.Rows()
}

func matchRows(c *C, rows [][]interface{}, expected [][]interface{}) {
	c.Assert(len(rows), Equals, len(expected), Commentf("%v", expected))
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

func (s *testDBSuite) TestTruncateTable(c *C) {
	defer testleak.AfterTest(c)
	store, err := tidb.NewStore("memory://truncate_table")
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
	defer testleak.AfterTest(c)
	store, err := tidb.NewStore("memory://rename_table")
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
	s.tk.MustExec("rename table test.t to test1.t1")
	is = sessionctx.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	s.tk.MustExec("use test")
	s.tk.MustQuery("show tables").Check(testkit.Rows())

	// for the same database
	s.tk.MustExec("use test1")
	s.tk.MustExec("rename table t1 to t2")
	is = sessionctx.GetDomain(ctx).InfoSchema()
	newTblInfo, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	s.tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))
	isExist := is.TableExists(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(isExist, IsFalse)
	s.tk.MustQuery("show tables").Check(testkit.Rows("t2"))

	// for failure case
	sql := "rename table test_not_exist.t to test_not_exist.t"
	s.testErrorCode(c, sql, tmysql.ErrFileNotFound)
	sql = "rename table test.t_not_exist to test_not_exist.t"
	s.testErrorCode(c, sql, tmysql.ErrFileNotFound)
	sql = "rename table test1.t2 to test_not_exist.t"
	s.testErrorCode(c, sql, tmysql.ErrErrorOnRename)
	sql = "rename table test1.t2 to test1.t2"
	s.testErrorCode(c, sql, tmysql.ErrTableExists)
}
