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
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/schemautil"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"golang.org/x/net/context"
)

const (
	// waitForCleanDataRound indicates how many times should we check data is cleaned or not.
	waitForCleanDataRound = 150
	// waitForCleanDataInterval is a min duration between 2 check for data clean.
	waitForCleanDataInterval = time.Millisecond * 100
)

var _ = Suite(&testDBSuite{})

const defaultBatchSize = 2048

type testDBSuite struct {
	cluster    *mocktikv.Cluster
	mvccStore  mocktikv.MVCCStore
	store      kv.Storage
	dom        *domain.Domain
	schemaName string
	tk         *testkit.TestKit
	s          session.Session
	lease      time.Duration
	autoIDStep int64
}

func (s *testDBSuite) SetUpSuite(c *C) {
	var err error
	testleak.BeforeTest()

	s.lease = 200 * time.Millisecond
	session.SetSchemaLease(s.lease)
	session.SetStatsLease(0)
	s.schemaName = "test_db"
	s.autoIDStep = autoid.GetStep()
	autoid.SetStep(5000)
	ddl.WaitTimeWhenErrorOccured = 1 * time.Microsecond

	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.s, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	_, err = s.s.Execute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)

	s.tk = testkit.NewTestKit(c, s.store)
}

func (s *testDBSuite) TearDownSuite(c *C) {
	s.s.Execute(context.Background(), "drop database if exists test_db")
	s.s.Close()
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
	autoid.SetStep(s.autoIDStep)
}

func assertErrorCode(c *C, tk *testkit.TestKit, sql string, errCode int) {
	_, err := tk.Exec(sql)
	c.Assert(err, NotNil)
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	c.Assert(ok, IsTrue, Commentf("err: %T", originErr))
	c.Assert(tErr.ToSQLError().Code, DeepEquals, uint16(errCode), Commentf("MySQL code:%v", tErr.ToSQLError()))
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
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	// create database
	sql := "create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s.testErrorCode(c, sql, tmysql.ErrTooLongIdent)
	sql = "create database test"
	s.testErrorCode(c, sql, tmysql.ErrDBCreateExists)
	sql = "create database test1 character set uft8;"
	s.testErrorCode(c, sql, tmysql.ErrUnknownCharacterSet)
	sql = "create database test2 character set gkb;"
	s.testErrorCode(c, sql, tmysql.ErrUnknownCharacterSet)
	sql = "create database test3 character set laitn1;"
	s.testErrorCode(c, sql, tmysql.ErrUnknownCharacterSet)
	// drop database
	sql = "drop database db_not_exist"
	s.testErrorCode(c, sql, tmysql.ErrDBDropExists)
	// create table
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
	sql = "CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT 2.0 DEFAULT now());"
	s.testErrorCode(c, sql, tmysql.ErrInvalidDefault)
	sql = "CREATE TABLE `t` (`a` double DEFAULT now());"
	s.testErrorCode(c, sql, tmysql.ErrInvalidDefault)
	sql = "create table t1(a int) character set uft8;"
	s.testErrorCode(c, sql, tmysql.ErrUnknownCharacterSet)
	sql = "create table t1(a int) character set gkb;"
	s.testErrorCode(c, sql, tmysql.ErrUnknownCharacterSet)
	sql = "create table t1(a int) character set laitn1;"
	s.testErrorCode(c, sql, tmysql.ErrUnknownCharacterSet)
	sql = "create table test_error_code (a int not null ,b int not null,c int not null, d int not null, foreign key (b, c) references product(id));"
	s.testErrorCode(c, sql, tmysql.ErrWrongFkDef)
	sql = "create table test_error_code_2;"
	s.testErrorCode(c, sql, tmysql.ErrTableMustHaveColumns)
	sql = "create table test_error_code_2 (unique(c1));"
	s.testErrorCode(c, sql, tmysql.ErrTableMustHaveColumns)
	sql = "create table test_error_code_2(c1 int, c2 int, c3 int, primary key(c1), primary key(c2));"
	s.testErrorCode(c, sql, tmysql.ErrMultiplePriKey)
	sql = "create table test_error_code_3(pt blob ,primary key (pt));"
	s.testErrorCode(c, sql, tmysql.ErrBlobKeyWithoutLength)
	sql = "create table test_error_code_3(a text, unique (a(3073)));"
	s.testErrorCode(c, sql, tmysql.ErrTooLongKey)
	sql = "create table test_error_code_3(`id` int, key `primary`(`id`));"
	s.testErrorCode(c, sql, tmysql.ErrWrongNameForIndex)
	sql = "create table t2(c1.c2 blob default null);"
	s.testErrorCode(c, sql, tmysql.ErrWrongTableName)
	sql = "create table t2 (id int default null primary key , age int);"
	s.testErrorCode(c, sql, tmysql.ErrInvalidDefault)
	sql = "create table t2 (id int null primary key , age int);"
	s.testErrorCode(c, sql, tmysql.ErrPrimaryCantHaveNull)
	sql = "create table t2 (id int default null, age int, primary key(id));"
	s.testErrorCode(c, sql, tmysql.ErrPrimaryCantHaveNull)
	sql = "create table t2 (id int null, age int, primary key(id));"
	s.testErrorCode(c, sql, tmysql.ErrPrimaryCantHaveNull)

	sql = "create table t2 (id int primary key , age int);"
	s.tk.MustExec(sql)

	// add column
	sql = "alter table test_error_code_succ add column c1 int"
	s.testErrorCode(c, sql, tmysql.ErrDupFieldName)
	sql = "alter table test_error_code_succ add column aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int"
	s.testErrorCode(c, sql, tmysql.ErrTooLongIdent)
	sql = "alter table test_comment comment 'test comment'"
	s.testErrorCode(c, sql, tmysql.ErrNoSuchTable)
	sql = "alter table test_error_code_succ add column `a ` int ;"
	s.testErrorCode(c, sql, tmysql.ErrWrongColumnName)
	s.tk.MustExec("create table test_on_update (c1 int, c2 int);")
	sql = "alter table test_on_update add column c3 int on update current_timestamp;"
	s.testErrorCode(c, sql, tmysql.ErrInvalidOnUpdate)
	sql = "create table test_on_update_2(c int on update current_timestamp);"
	s.testErrorCode(c, sql, tmysql.ErrInvalidOnUpdate)

	// drop column
	sql = "alter table test_error_code_succ drop c_not_exist"
	s.testErrorCode(c, sql, tmysql.ErrCantDropFieldOrKey)
	s.tk.MustExec("create table test_drop_column (c1 int );")
	sql = "alter table test_drop_column drop column c1;"
	s.testErrorCode(c, sql, tmysql.ErrCantRemoveAllFields)
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
	// insert value
	s.tk.MustExec("create table test_error_code_null(c1 char(100) not null);")
	sql = "insert into test_error_code_null (c1) values(null);"
	s.testErrorCode(c, sql, tmysql.ErrBadNull)
}

func (s *testDBSuite) TestAddIndexAfterAddColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("create table test_add_index_after_add_col(a int, b int not null default '0')")
	s.tk.MustExec("insert into test_add_index_after_add_col values(1, 2),(2,2)")
	s.tk.MustExec("alter table test_add_index_after_add_col add column c int not null default '0'")
	sql := "alter table test_add_index_after_add_col add unique index cc(c) "
	s.testErrorCode(c, sql, tmysql.ErrDupEntry)
	sql = "alter table test_add_index_after_add_col add index idx_test(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17);"
	s.testErrorCode(c, sql, tmysql.ErrTooManyKeyParts)
}

func (s *testDBSuite) TestAddIndexWithPK(c *C) {
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

func (s *testDBSuite) TestRenameIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("create table t (pk int primary key, c int default 1, c1 int default 1, unique key k1(c), key k2(c1))")

	// Test rename success
	s.tk.MustExec("alter table t rename index k1 to k3")
	s.tk.MustExec("admin check index t k3")

	// Test rename to the same name
	s.tk.MustExec("alter table t rename index k3 to k3")
	s.tk.MustExec("admin check index t k3")

	// Test rename on non-exists keys
	s.testErrorCode(c, "alter table t rename index x to x", mysql.ErrKeyDoesNotExist)

	// Test rename on already-exists keys
	s.testErrorCode(c, "alter table t rename index k3 to k2", mysql.ErrDupKeyName)

	s.tk.MustExec("alter table t rename index k2 to K2")
	s.testErrorCode(c, "alter table t rename key k3 to K2", mysql.ErrDupKeyName)
}

func (s *testDBSuite) testGetTableByName(c *C, db, table string) table.Table {
	return testGetTableByName(c, s.s, db, table)
}

func testGetTableByName(c *C, se sessionctx.Context, db, table string) table.Table {
	ctx := se.(sessionctx.Context)
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	c.Assert(err, IsNil)
	return tbl
}

func (s *testDBSuite) testGetTable(c *C, name string) table.Table {
	return s.testGetTableByName(c, s.schemaName, name)
}

func backgroundExec(s kv.Storage, sql string, done chan error) {
	se, err := session.CreateSession4Test(s)
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	defer se.Close()
	_, err = se.Execute(context.Background(), "use test_db")
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	_, err = se.Execute(context.Background(), sql)
	done <- errors.Trace(err)
}

func (s *testDBSuite) TestAddUniqueIndexRollback(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (c1 int, c2 int, c3 int, primary key(c1))")
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
	s.mustExec(c, "drop table t1")
}

func (s *testDBSuite) TestCancelAddIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (c1 int, c2 int, c3 int, primary key(c1))")
	// defaultBatchSize is equal to ddl.defaultBatchSize
	base := defaultBatchSize * 2
	count := base
	// add some rows
	for i := 0; i < count; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}

	var c3IdxInfo *model.IndexInfo
	hook := &ddl.TestDDLCallback{}
	oldReorgWaitTimeout := ddl.ReorgWaitTimeout
	// let hook.OnJobUpdatedExported has chance to cancel the job.
	// the hook.OnJobUpdatedExported is called when the job is updated, runReorgJob will wait ddl.ReorgWaitTimeout, then return the ddl.runDDLJob.
	// After that ddl call d.hook.OnJobUpdated(job), so that we can canceled the job in this test case.
	ddl.ReorgWaitTimeout = 50 * time.Millisecond
	var checkErr error
	hook.OnJobUpdatedExported, c3IdxInfo, checkErr = backgroundExecOnJobUpdatedExported(c, s, hook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundExec(s.store, "create unique index c3_index on t1 (c3)", done)

	times := 0
	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
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

	ctx := s.s.(sessionctx.Context)
	idx := tables.NewIndex(t.Meta().ID, t.Meta(), c3IdxInfo)
	checkDelRangeDone(c, ctx, idx)

	s.mustExec(c, "drop table t1")
	ddl.ReorgWaitTimeout = oldReorgWaitTimeout
	callback := &ddl.TestDDLCallback{}
	s.dom.DDL().(ddl.DDLForTest).SetHook(callback)
}

// TestCancelTruncateTable tests cancel ddl job which type is truncate table.
func (s *testDBSuite) TestCancelTruncateTable(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "create database if not exists test_truncate_table")
	s.mustExec(c, "drop table if exists t")
	s.mustExec(c, "create table t(c1 int, c2 int)")
	defer s.mustExec(c, "drop table t;")
	var checkErr error
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionTruncateTable && job.State == model.JobStateNone {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn()
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	_, err := s.tk.Exec("truncate table t")
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
	s.dom.DDL().(ddl.DDLForTest).SetHook(&ddl.TestDDLCallback{})
}

// TestCancelRenameIndex tests cancel ddl job which type is rename index.
func (s *testDBSuite) TestCancelRenameIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "create database if not exists test_rename_index")
	s.mustExec(c, "drop table if exists t")
	s.mustExec(c, "create table t(c1 int, c2 int)")
	defer s.mustExec(c, "drop table t;")
	for i := 0; i < 100; i++ {
		s.mustExec(c, "insert into t values (?, ?)", i, i)
	}
	s.mustExec(c, "alter table t add index idx_c2(c2)")
	var checkErr error
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionRenameIndex && job.State == model.JobStateNone {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn()
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	rs, err := s.tk.Exec("alter table t rename index idx_c2 to idx_c3")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
	s.dom.DDL().(ddl.DDLForTest).SetHook(&ddl.TestDDLCallback{})
	t := s.testGetTable(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c3"), IsFalse)
	}
	s.mustExec(c, "alter table t rename index idx_c2 to idx_c3")
}

// TestCancelAddIndex1 tests canceling ddl job when the add index worker is not started.
func (s *testDBSuite) TestCancelAddIndex1(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t")
	s.mustExec(c, "create table t(c1 int, c2 int)")
	defer s.mustExec(c, "drop table t;")

	for i := 0; i < 50; i++ {
		s.mustExec(c, "insert into t values (?, ?)", i, i)
	}

	var checkErr error
	oldReorgWaitTimeout := ddl.ReorgWaitTimeout
	ddl.ReorgWaitTimeout = 50 * time.Millisecond
	defer func() { ddl.ReorgWaitTimeout = oldReorgWaitTimeout }()
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer == 0 {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn()
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}

			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}

			checkErr = txn.Commit(context.Background())
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	rs, err := s.tk.Exec("alter table t add index idx_c2(c2)")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")

	s.dom.DDL().(ddl.DDLForTest).SetHook(&ddl.TestDDLCallback{})
	t := s.testGetTable(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c2"), IsFalse)
	}
	s.mustExec(c, "alter table t add index idx_c2(c2)")
	s.mustExec(c, "alter table t drop index idx_c2")
}

// TestCancelDropIndex tests cancel ddl job which type is drop index.
func (s *testDBSuite) TestCancelDropIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t")
	s.mustExec(c, "create table t(c1 int, c2 int)")
	defer s.mustExec(c, "drop table t;")
	for i := 0; i < 5; i++ {
		s.mustExec(c, "insert into t values (?, ?)", i, i)
	}
	testCases := []struct {
		needAddIndex   bool
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		// model.JobStateNone means the jobs is canceled before the first run.
		{true, model.JobStateNone, model.StateNone, true},
		{false, model.JobStateRunning, model.StateWriteOnly, true},
		{false, model.JobStateRunning, model.StateDeleteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteReorganization, false},
	}
	var checkErr error
	oldReorgWaitTimeout := ddl.ReorgWaitTimeout
	ddl.ReorgWaitTimeout = 50 * time.Millisecond
	defer func() { ddl.ReorgWaitTimeout = oldReorgWaitTimeout }()
	hook := &ddl.TestDDLCallback{}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionDropIndex && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobID = job.ID
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn()
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddIndex {
			s.mustExec(c, "alter table t add index idx_c2(c2)")
		}
		rs, err := s.tk.Exec("alter table t drop index idx_c2")
		if rs != nil {
			rs.Close()
		}
		t := s.testGetTable(c, "t")
		indexInfo := schemautil.FindIndexByName("idx_c2", t.Meta().Indices)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
			c.Assert(indexInfo, NotNil)
			c.Assert(indexInfo.State, Equals, model.StatePublic)
		} else {
			err1 := admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID)
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, err1.Error())
			c.Assert(indexInfo, IsNil)
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(&ddl.TestDDLCallback{})
	s.mustExec(c, "alter table t add index idx_c2(c2)")
	s.mustExec(c, "alter table t drop index idx_c2")
}

// TestCancelDropTable tests cancel ddl job which type is drop table.
func (s *testDBSuite) TestCancelDropTableAndSchema(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	testCases := []struct {
		needAddTableOrDB bool
		action           model.ActionType
		jobState         model.JobState
		JobSchemaState   model.SchemaState
		cancelSucc       bool
	}{
		// Check drop table.
		// model.JobStateNone means the jobs is canceled before the first run.
		{true, model.ActionDropTable, model.JobStateNone, model.StateNone, true},
		{false, model.ActionDropTable, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.ActionDropTable, model.JobStateRunning, model.StateDeleteOnly, false},

		// Check drop database.
		{true, model.ActionDropSchema, model.JobStateNone, model.StateNone, true},
		{false, model.ActionDropSchema, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.ActionDropSchema, model.JobStateRunning, model.StateDeleteOnly, false},
	}
	var checkErr error
	oldReorgWaitTimeout := ddl.ReorgWaitTimeout
	ddl.ReorgWaitTimeout = 50 * time.Millisecond
	defer func() { ddl.ReorgWaitTimeout = oldReorgWaitTimeout }()
	hook := &ddl.TestDDLCallback{}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == testCase.action && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn()
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originHook := s.dom.DDL().(ddl.DDLForTest).GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	var err error
	sql := ""
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddTableOrDB {
			s.mustExec(c, "create database if not exists test_drop_db")
			s.mustExec(c, "use test_drop_db")
			s.mustExec(c, "create table if not exists t(c1 int, c2 int)")
		}

		if testCase.action == model.ActionDropTable {
			sql = "drop table t;"
		} else if testCase.action == model.ActionDropSchema {
			sql = "drop database test_drop_db;"
		}

		_, err = s.tk.Exec(sql)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
			s.mustExec(c, "insert into t values (?, ?)", i, i)
		} else {
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID).Error())
			_, err = s.tk.Exec("insert into t values (?, ?)", i, i)
			c.Assert(err, NotNil)
		}
	}
}

func (s *testDBSuite) TestAddAnonymousIndex(c *C) {
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
	// for anonymous index with column name `primary`
	s.mustExec(c, "create table t_primary (`primary` int, key (`primary`))")
	t = s.testGetTable(c, "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	s.mustExec(c, "create table t_primary_2 (`primary` int, key primary_2 (`primary`), key (`primary`))")
	t = s.testGetTable(c, "t_primary_2")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	s.mustExec(c, "create table t_primary_3 (`primary_2` int, key(`primary_2`), `primary` int, key(`primary`));")
	t = s.testGetTable(c, "t_primary_3")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
}

// TestModifyColumnAfterAddIndex Issue 5134
func (s *testDBSuite) TestModifyColumnAfterAddIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.mustExec(c, "create table city (city VARCHAR(2) KEY);")
	s.mustExec(c, "alter table city change column city city varchar(50);")
	s.mustExec(c, `insert into city values ("abc"), ("abd");`)
}

func (s *testDBSuite) testAlterLock(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.mustExec(c, "create table t_index_lock (c1 int, c2 int, C3 int)")
	s.mustExec(c, "alter table t_indx_lock add index (c1, c2), lock=none")
}

func (s *testDBSuite) TestAddMultiColumnsIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("drop database if exists tidb;")
	s.tk.MustExec("create database tidb;")
	s.tk.MustExec("use tidb;")
	s.tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	s.tk.MustExec("insert tidb.test values (1, 1);")
	s.tk.MustExec("update tidb.test set b = b + 1 where a = 1;")
	s.tk.MustExec("insert into tidb.test values (2, 2);")
	// Test that the b value is nil.
	s.tk.MustExec("insert into tidb.test (a) values (3);")
	s.tk.MustExec("insert into tidb.test values (4, 4);")
	// Test that the b value is nil again.
	s.tk.MustExec("insert into tidb.test (a) values (5);")
	s.tk.MustExec("insert tidb.test values (6, 6);")
	s.tk.MustExec("alter table tidb.test add index idx1 (a, b);")
	s.tk.MustExec("admin check table test")
}

func (s *testDBSuite) TestAddIndex(c *C) {
	s.testAddIndex(c, false, "create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))")
	s.testAddIndex(c, true, `create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by range (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`)
}

func (s *testDBSuite) testAddIndex(c *C, testPartition bool, createTableSQL string) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("set @@tidb_enable_table_partition = 1")
	s.tk.MustExec("drop table if exists test_add_index")
	s.tk.MustExec(createTableSQL)

	done := make(chan error, 1)
	start := -10
	num := defaultBatchSize
	// first add some rows
	for i := start; i < num; i++ {
		sql := fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", i, i, i)
		s.mustExec(c, sql)
	}
	// Add some discrete rows.
	maxBatch := 20
	batchCnt := 100
	otherKeys := make([]int, 0, batchCnt*maxBatch)
	// Make sure there are no duplicate keys.
	base := defaultBatchSize * 20
	for i := 1; i < batchCnt; i++ {
		n := base + i*defaultBatchSize + i
		for j := 0; j < rand.Intn(maxBatch); j++ {
			n += j
			sql := fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", n, n, n)
			s.mustExec(c, sql)
			otherKeys = append(otherKeys, n)
		}
	}
	// Encounter the value of math.MaxInt64 in middle of
	v := math.MaxInt64 - defaultBatchSize/2
	sql := fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", v, v, v)
	s.mustExec(c, sql)
	otherKeys = append(otherKeys, v)

	sessionExecInGoroutine(c, s.store, "create index c3_index on test_add_index (c3)", done)

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
				sql := fmt.Sprintf("delete from test_add_index where c1 = %d", n)
				s.mustExec(c, sql)
				sql = fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", i, i, i)
				s.mustExec(c, sql)
			}
			num += step
		}
	}

	// get exists keys
	keys := make([]int, 0, num)
	for i := start; i < num; i++ {
		if _, ok := deletedKeys[i]; ok {
			continue
		}
		keys = append(keys, i)
	}
	keys = append(keys, otherKeys...)

	// test index key
	expectedRows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		expectedRows = append(expectedRows, []interface{}{key})
	}
	rows := s.mustQuery(c, fmt.Sprintf("select c1 from test_add_index where c3 >= %d order by c1", start))
	matchRows(c, rows, expectedRows)

	if testPartition {
		s.tk.MustExec("admin check table test_add_index")
		return
	}

	// test index range
	for i := 0; i < 100; i++ {
		index := rand.Intn(len(keys) - 3)
		rows := s.mustQuery(c, "select c1 from test_add_index where c3 >= ? limit 3", keys[index])
		matchRows(c, rows, [][]interface{}{{keys[index]}, {keys[index+1]}, {keys[index+2]}})
	}

	// TODO: Support explain in future.
	// rows := s.mustQuery(c, "explain select c1 from test_add_index where c3 >= 100")

	// ay := dumpRows(c, rows)
	// c.Assert(strings.Contains(fmt.Sprintf("%v", ay), "c3_index"), IsTrue)

	// get all row handles
	ctx := s.s.(sessionctx.Context)
	c.Assert(ctx.NewTxn(), IsNil)
	t := s.testGetTable(c, "test_add_index")
	handles := make(map[int64]struct{})
	startKey := t.RecordKey(math.MinInt64)
	err := t.IterRecords(ctx, startKey, t.Cols(),
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
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	txn.Rollback()

	c.Assert(ctx.NewTxn(), IsNil)
	defer txn.Rollback()

	it, err := nidx.SeekFirst(txn)
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

	s.tk.MustExec("drop table test_add_index")
}

func (s *testDBSuite) TestDropIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("drop table if exists test_drop_index")
	s.tk.MustExec("create table test_drop_index (c1 int, c2 int, c3 int, primary key(c1))")
	s.tk.MustExec("create index c3_index on test_drop_index (c3)")
	done := make(chan error, 1)
	s.mustExec(c, "delete from test_drop_index")

	num := 100
	//  add some rows
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into test_drop_index values (?, ?, ?)", i, i, i)
	}
	t := s.testGetTable(c, "test_drop_index")
	var c3idx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == "c3_index" {
			c3idx = tidx
			break
		}
	}
	c.Assert(c3idx, NotNil)

	sessionExecInGoroutine(c, s.store, "drop index c3_index on test_drop_index", done)

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
				s.mustExec(c, "update test_drop_index set c2 = 1 where c1 = ?", n)
				s.mustExec(c, "insert into test_drop_index values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	rows := s.mustQuery(c, "explain select c1 from test_drop_index where c3 >= 0")
	c.Assert(strings.Contains(fmt.Sprintf("%v", rows), "c3_index"), IsFalse)

	// check in index, must no index in kv
	ctx := s.s.(sessionctx.Context)

	// Make sure there is no index with name c3_index.
	t = s.testGetTable(c, "test_drop_index")
	var nidx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == "c3_index" {
			nidx = tidx
			break
		}
	}
	c.Assert(nidx, IsNil)

	idx := tables.NewIndex(t.Meta().ID, t.Meta(), c3idx.Meta())
	checkDelRangeDone(c, ctx, idx)
	s.tk.MustExec("drop table test_drop_index")
}

func checkDelRangeDone(c *C, ctx sessionctx.Context, idx table.Index) {
	startTime := time.Now()
	f := func() map[int64]struct{} {
		handles := make(map[int64]struct{})

		c.Assert(ctx.NewTxn(), IsNil)
		txn, err := ctx.Txn(true)
		c.Assert(err, IsNil)
		defer txn.Rollback()

		txn, err = ctx.Txn(true)
		c.Assert(err, IsNil)
		it, err := idx.SeekFirst(txn)
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
		return handles
	}

	var handles map[int64]struct{}
	for i := 0; i < waitForCleanDataRound; i++ {
		handles = f()
		if len(handles) != 0 {
			time.Sleep(waitForCleanDataInterval)
		} else {
			break
		}
	}
	c.Assert(handles, HasLen, 0, Commentf("take time %v", time.Since(startTime)))
}

// TestCancelAddTableAndDropTablePartition tests cancel ddl job which type is add/drop table partition.
func (s *testDBSuite) TestCancelAddTableAndDropTablePartition(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "create database if not exists test_partition_table")
	s.mustExec(c, "use test_partition_table")
	s.mustExec(c, "drop table if exists t_part")
	s.mustExec(c, `create table t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	);`)
	defer s.mustExec(c, "drop table t_part;")
	for i := 0; i < 10; i++ {
		s.mustExec(c, "insert into t_part values (?)", i)
	}

	testCases := []struct {
		action         model.ActionType
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		{model.ActionAddTablePartition, model.JobStateNone, model.StateNone, true},
		{model.ActionDropTablePartition, model.JobStateNone, model.StateNone, true},
		{model.ActionAddTablePartition, model.JobStateRunning, model.StatePublic, false},
		{model.ActionDropTablePartition, model.JobStateRunning, model.StatePublic, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{}
	testCase := &testCases[0]
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == testCase.action && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn()
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
		s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
		var err error
		sql := ""
		for i := range testCases {
			testCase = &testCases[i]
			if testCase.action == model.ActionAddTablePartition {
				sql = `alter table t_part add partition (
				partition p2 values less than (30)
				);`
			} else if testCase.action == model.ActionDropTablePartition {
				sql = "alter table t_part drop partition p1;"
			}
			_, err = s.tk.Exec(sql)
			if testCase.cancelSucc {
				c.Assert(checkErr, IsNil)
				c.Assert(err, NotNil)
				c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
				s.mustExec(c, "insert into t_part values (?)", i)
			} else {
				c.Assert(err, IsNil)
				c.Assert(checkErr, NotNil)
				c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID).Error())
				_, err = s.tk.Exec("insert into t_part values (?)", i)
				c.Assert(err, NotNil)
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(&ddl.TestDDLCallback{})
}

// TestCancelDropColumn tests cancel ddl job which type is drop column.
func (s *testDBSuite) TestCancelDropColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.mustExec(c, "drop table if exists test_drop_column")
	s.mustExec(c, "create table test_drop_column(c1 int, c2 int)")
	defer s.mustExec(c, "drop table test_drop_column;")
	testCases := []struct {
		needAddColumn  bool
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		{true, model.JobStateNone, model.StateNone, true},
		{false, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionDropColumn && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn()
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}

	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	var err1 error
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddColumn {
			s.mustExec(c, "alter table test_drop_column add column c3 int")
		}
		_, err1 = s.tk.Exec("alter table test_drop_column drop column c3")
		var col1 *table.Column
		t := s.testGetTable(c, "test_drop_column")
		for _, col := range t.Cols() {
			if strings.EqualFold(col.Name.L, "c3") {
				col1 = col
				break
			}
		}
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(col1, NotNil)
			c.Assert(col1.Name.L, Equals, "c3")
			c.Assert(err1.Error(), Equals, "[ddl:12]cancelled DDL job")
		} else {
			c.Assert(col1, IsNil)
			c.Assert(err1, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID).Error())
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(&ddl.TestDDLCallback{})
	s.mustExec(c, "alter table test_drop_column add column c3 int")
	s.mustExec(c, "alter table test_drop_column drop column c3")
}

func (s *testDBSuite) TestAddIndexWithDupCols(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	err1 := infoschema.ErrColumnExists.GenWithStackByArgs("b")
	err2 := infoschema.ErrColumnExists.GenWithStackByArgs("B")

	s.tk.MustExec("create table test_add_index_with_dup (a int, b int)")
	_, err := s.tk.Exec("create index c on test_add_index_with_dup(b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = s.tk.Exec("create index c on test_add_index_with_dup(b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	_, err = s.tk.Exec("alter table test_add_index_with_dup add index c (b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = s.tk.Exec("alter table test_add_index_with_dup add index c (b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	s.tk.MustExec("drop table test_add_index_with_dup")
}

func (s *testDBSuite) showColumns(c *C, tableName string) [][]interface{} {
	return s.mustQuery(c, fmt.Sprintf("show columns from %s", tableName))
}

func (s *testDBSuite) TestIssue2293(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("create table t_issue_2293 (a int)")
	sql := "alter table t_issue_2293 add b int not null default 'a'"
	s.testErrorCode(c, sql, tmysql.ErrInvalidDefault)
	s.tk.MustExec("insert into t_issue_2293 value(1)")
	s.tk.MustQuery("select * from t_issue_2293").Check(testkit.Rows("1"))
}

func (s *testDBSuite) TestIssue6101(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("create table t1 (quantity decimal(2) unsigned);")
	_, err := s.tk.Exec("insert into t1 values (500), (-500), (~0), (-1);")
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(tmysql.ErrWarnDataOutOfRange))
	s.tk.MustExec("drop table t1")

	s.tk.MustExec("set sql_mode=''")
	s.tk.MustExec("create table t1 (quantity decimal(2) unsigned);")
	s.tk.MustExec("insert into t1 values (500), (-500), (~0), (-1);")
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("99", "0", "99", "0"))
	s.tk.MustExec("drop table t1")
}

func (s *testDBSuite) TestCreateIndexType(c *C) {
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

func (s *testDBSuite) TestIssue3833(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("create table issue3833 (b char(0))")
	s.testErrorCode(c, "create index idx on issue3833 (b)", tmysql.ErrWrongKeyColumn)
	s.testErrorCode(c, "alter table issue3833 add index idx (b)", tmysql.ErrWrongKeyColumn)
	s.testErrorCode(c, "create table issue3833_2 (b char(0), index (b))", tmysql.ErrWrongKeyColumn)
}

func (s *testDBSuite) TestColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("create table t2 (c1 int, c2 int, c3 int)")
	s.testAddColumn(c)
	s.testDropColumn(c)
	s.tk.MustExec("drop table t2")
}

func (s *testDBSuite) TestAddColumnTooMany(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	count := ddl.TableColumnCountLimit - 1
	var cols []string
	for i := 0; i < count; i++ {
		cols = append(cols, fmt.Sprintf("a%d int", i))
	}
	createSQL := fmt.Sprintf("create table t_column_too_many (%s)", strings.Join(cols, ","))
	s.tk.MustExec(createSQL)
	s.tk.MustExec("alter table t_column_too_many add column a_512 int")
	alterSQL := "alter table t_column_too_many add column a_513 int"
	s.testErrorCode(c, alterSQL, tmysql.ErrTooManyFields)
}

func sessionExec(c *C, s kv.Storage, sql string) {
	se, err := session.CreateSession4Test(s)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db")
	c.Assert(err, IsNil)
	rs, err := se.Execute(context.Background(), sql)
	c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
	c.Assert(rs, IsNil)
	se.Close()
}

func sessionExecInGoroutine(c *C, s kv.Storage, sql string, done chan error) {
	execMultiSQLInGoroutine(c, s, "test_db", []string{sql}, done)
}

func execMultiSQLInGoroutine(c *C, s kv.Storage, dbName string, multiSQL []string, done chan error) {
	go func() {
		se, err := session.CreateSession4Test(s)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		defer se.Close()
		_, err = se.Execute(context.Background(), "use "+dbName)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		for _, sql := range multiSQL {
			rs, err := se.Execute(context.Background(), sql)
			if err != nil {
				done <- errors.Trace(err)
				return
			}
			if rs != nil {
				done <- errors.Errorf("RecordSet should be empty.")
				return
			}
			done <- nil
		}
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
		rows = s.mustQuery(c, "select c4 from t2 where c4 = ?", i)
		matchRows(c, rows, [][]interface{}{{i}})
	}

	ctx := s.s.(sessionctx.Context)
	t := s.testGetTable(c, "t2")
	i := 0
	j := 0
	ctx.NewTxn()
	defer func() {
		if txn, err1 := ctx.Txn(true); err1 == nil {
			txn.Rollback()
		}
	}()
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

	// add timestamp type column
	s.mustExec(c, "create table test_on_update_c (c1 int, c2 timestamp);")
	s.mustExec(c, "alter table test_on_update_c add column c3 timestamp null default '2017-02-11' on update current_timestamp;")
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_on_update_c"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colC := tblInfo.Columns[2]
	c.Assert(colC.Tp, Equals, mysql.TypeTimestamp)
	hasNotNull := tmysql.HasNotNullFlag(colC.Flag)
	c.Assert(hasNotNull, IsFalse)
	// add datetime type column
	s.mustExec(c, "create table test_on_update_d (c1 int, c2 datetime);")
	s.mustExec(c, "alter table test_on_update_d add column c3 datetime on update current_timestamp;")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_on_update_d"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC = tblInfo.Columns[2]
	c.Assert(colC.Tp, Equals, mysql.TypeDatetime)
	hasNotNull = tmysql.HasNotNullFlag(colC.Flag)
	c.Assert(hasNotNull, IsFalse)
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

// TestDropColumn is for inserting value with a to-be-dropped column when do drop column.
// Column info from schema in build-insert-plan should be public only,
// otherwise they will not be consist with Table.Col(), then the server will panic.
func (s *testDBSuite) TestDropColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("create database drop_col_db")
	s.tk.MustExec("use drop_col_db")
	s.tk.MustExec("create table t2 (c1 int, c2 int, c3 int)")
	num := 50
	dmlDone := make(chan error, num)
	ddlDone := make(chan error, num)

	multiDDL := make([]string, 0, num)
	for i := 0; i < num/2; i++ {
		multiDDL = append(multiDDL, "alter table t2 add column c4 int", "alter table t2 drop column c4")
	}
	execMultiSQLInGoroutine(c, s.store, "drop_col_db", multiDDL, ddlDone)
	for i := 0; i < num; i++ {
		execMultiSQLInGoroutine(c, s.store, "drop_col_db", []string{"insert into t2 set c1 = 1, c2 = 1, c3 = 1, c4 = 1"}, dmlDone)
	}
	for i := 0; i < num; i++ {
		select {
		case err := <-ddlDone:
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		}
	}

	// Test for drop partition table column.
	s.tk.MustExec("drop table if exists t1")
	s.tk.MustExec("set @@tidb_enable_table_partition = 1")
	s.tk.MustExec("create table t1 (a bigint, b int, c int generated always as (b+1)) partition by range(a) (partition p0 values less than (10));")
	_, err := s.tk.Exec("alter table t1 drop column a")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[expression:1054]Unknown column 'a' in 'expression'")

	s.tk.MustExec("drop database drop_col_db")
}

func (s *testDBSuite) TestPrimaryKey(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.mustExec(c, "create table primary_key_test (a int, b varchar(10))")
	_, err := s.tk.Exec("alter table primary_key_test add primary key(a)")
	c.Assert(ddl.ErrUnsupportedModifyPrimaryKey.Equal(err), IsTrue)
	_, err = s.tk.Exec("alter table primary_key_test drop primary key")
	c.Assert(ddl.ErrUnsupportedModifyPrimaryKey.Equal(err), IsTrue)
}

func (s *testDBSuite) TestChangeColumn(c *C) {
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
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colD := tblInfo.Columns[2]
	hasNoDefault := tmysql.HasNoDefaultValueFlag(colD.Flag)
	c.Assert(hasNoDefault, IsTrue)
	// for the following definitions: 'not null', 'null', 'default value' and 'comment'
	s.mustExec(c, "alter table t3 change b b varchar(20) null default 'c' comment 'my comment'")
	is = domain.GetDomain(ctx).InfoSchema()
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
	is = domain.GetDomain(ctx).InfoSchema()
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
	// Rename to an existing column.
	s.mustExec(c, "alter table t3 add column a bigint")
	sql = "alter table t3 change aa a bigint"
	s.testErrorCode(c, sql, tmysql.ErrDupFieldName)

	s.tk.MustExec("drop table t3")
}

func (s *testDBSuite) TestAlterColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.mustExec(c, "create table test_alter_column (a int default 111, b varchar(8), c varchar(8) not null, d timestamp on update current_timestamp)")
	s.mustExec(c, "insert into test_alter_column set b = 'a', c = 'aa'")
	s.tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111"))
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colA := tblInfo.Columns[0]
	hasNoDefault := tmysql.HasNoDefaultValueFlag(colA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	s.mustExec(c, "alter table test_alter_column alter column a set default 222")
	s.mustExec(c, "insert into test_alter_column set b = 'b', c = 'bb'")
	s.tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colA = tblInfo.Columns[0]
	hasNoDefault = tmysql.HasNoDefaultValueFlag(colA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	s.mustExec(c, "alter table test_alter_column alter column b set default null")
	s.mustExec(c, "insert into test_alter_column set c = 'cc'")
	s.tk.MustQuery("select b from test_alter_column").Check(testkit.Rows("a", "b", "<nil>"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC := tblInfo.Columns[2]
	hasNoDefault = tmysql.HasNoDefaultValueFlag(colC.Flag)
	c.Assert(hasNoDefault, IsTrue)
	s.mustExec(c, "alter table test_alter_column alter column c set default 'xx'")
	s.mustExec(c, "insert into test_alter_column set a = 123")
	s.tk.MustQuery("select c from test_alter_column").Check(testkit.Rows("aa", "bb", "cc", "xx"))
	is = domain.GetDomain(ctx).InfoSchema()
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
	expected := "CREATE TABLE `mc` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)

	// Change / modify column should preserve index options.
	s.mustExec(c, "drop table if exists mc")
	s.mustExec(c, "create table mc(a int key, b int, c int unique)")
	s.mustExec(c, "alter table mc modify column a bigint") // NOT NULL & PRIMARY KEY should be preserved
	s.mustExec(c, "alter table mc modify column b bigint")
	s.mustExec(c, "alter table mc modify column c bigint") // Unique should be preserved
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` bigint(20) DEFAULT NULL,\n  `c` bigint(20) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `c` (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)

	// Dropping or keeping auto_increment is allowed, however adding is not allowed.
	s.mustExec(c, "drop table if exists mc")
	s.mustExec(c, "create table mc(a int key auto_increment, b int)")
	s.mustExec(c, "alter table mc modify column a bigint auto_increment") // Keeps auto_increment
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL AUTO_INCREMENT,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	s.mustExec(c, "set @@tidb_allow_remove_auto_inc = on")
	s.mustExec(c, "alter table mc modify column a bigint") // Drops auto_increment
	s.mustExec(c, "set @@tidb_allow_remove_auto_inc = off")
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
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
	tk := testkit.NewTestKit(c, s.store)
	s.tk.MustExec("create database umt_db")
	tk.MustExec("use umt_db")
	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("insert t1 values (1, 1), (2, 2)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert t2 values (1, 3), (2, 5)")
	ctx := tk.Se.(sessionctx.Context)
	dom := domain.GetDomain(ctx)
	is := dom.InfoSchema()
	db, ok := is.SchemaByName(model.NewCIStr("umt_db"))
	c.Assert(ok, IsTrue)
	t1Tbl, err := is.TableByName(model.NewCIStr("umt_db"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t1Info := t1Tbl.Meta()

	// Add a new column in write only state.
	newColumn := &model.ColumnInfo{
		ID:                 100,
		Name:               model.NewCIStr("c3"),
		Offset:             2,
		DefaultValue:       9,
		OriginDefaultValue: 9,
		FieldType:          *types.NewFieldType(tmysql.TypeLonglong),
		State:              model.StateWriteOnly,
	}
	t1Info.Columns = append(t1Info.Columns, newColumn)

	kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, t1Info), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustExec("update t1, t2 set t1.c1 = 8, t2.c2 = 10 where t1.c2 = t2.c1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1", "8 2"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 10", "2 10"))

	newColumn.State = model.StatePublic

	kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, t1Info), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1 9", "8 2 9"))
	tk.MustExec("drop database umt_db")
}

func (s *testDBSuite) TestCreateTableTooLarge(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")

	sql := "create table t_too_large ("
	cnt := 3000
	for i := 1; i <= cnt; i++ {
		sql += fmt.Sprintf("a%d double, b%d double, c%d double, d%d double", i, i, i, i)
		if i != cnt {
			sql += ","
		}
	}
	sql += ");"
	s.testErrorCode(c, sql, tmysql.ErrTooManyFields)

	originLimit := ddl.TableColumnCountLimit
	ddl.TableColumnCountLimit = cnt * 4
	_, err := s.tk.Exec(sql)
	c.Assert(kv.ErrEntryTooLarge.Equal(err), IsTrue, Commentf("err:%v", err))
	ddl.TableColumnCountLimit = originLimit
}

func (s *testDBSuite) TestCreateTableWithLike(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	// for the same database
	s.tk.MustExec("create database ctwl_db")
	s.tk.MustExec("use ctwl_db")
	s.tk.MustExec("create table tt(id int primary key)")
	s.tk.MustExec("create table t (c1 int not null auto_increment, c2 int, constraint cc foreign key (c2) references tt(id), primary key(c1)) auto_increment = 10")
	s.tk.MustExec("insert into t set c2=1")
	s.tk.MustExec("create table t1 like ctwl_db.t")
	s.tk.MustExec("insert into t1 set c2=11")
	s.tk.MustExec("create table t2 (like ctwl_db.t1)")
	s.tk.MustExec("insert into t2 set c2=12")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("10 1"))
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	s.tk.MustQuery("select * from t2").Check(testkit.Rows("1 12"))
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("ctwl_db"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tbl1Info := tbl1.Meta()
	c.Assert(tbl1Info.ForeignKeys, IsNil)
	c.Assert(tbl1Info.PKIsHandle, Equals, true)
	col := tbl1Info.Columns[0]
	hasNotNull := tmysql.HasNotNullFlag(col.Flag)
	c.Assert(hasNotNull, IsTrue)
	tbl2, err := is.TableByName(model.NewCIStr("ctwl_db"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tbl2Info := tbl2.Meta()
	c.Assert(tbl2Info.ForeignKeys, IsNil)
	c.Assert(tbl2Info.PKIsHandle, Equals, true)
	c.Assert(tmysql.HasNotNullFlag(tbl2Info.Columns[0].Flag), IsTrue)

	// for different databases
	s.tk.MustExec("create database ctwl_db1")
	s.tk.MustExec("use ctwl_db1")
	s.tk.MustExec("create table t1 like ctwl_db.t")
	s.tk.MustExec("insert into t1 set c2=11")
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl1, err = is.TableByName(model.NewCIStr("ctwl_db1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(tbl1.Meta().ForeignKeys, IsNil)

	// for failure cases
	failSQL := fmt.Sprintf("create table t1 like test_not_exist.t")
	s.testErrorCode(c, failSQL, tmysql.ErrNoSuchTable)
	failSQL = fmt.Sprintf("create table t1 like test.t_not_exist")
	s.testErrorCode(c, failSQL, tmysql.ErrNoSuchTable)
	failSQL = fmt.Sprintf("create table t1 (like test_not_exist.t)")
	s.testErrorCode(c, failSQL, tmysql.ErrNoSuchTable)
	failSQL = fmt.Sprintf("create table test_not_exis.t1 like ctwl_db.t")
	s.testErrorCode(c, failSQL, tmysql.ErrBadDB)
	failSQL = fmt.Sprintf("create table t1 like ctwl_db.t")
	s.testErrorCode(c, failSQL, tmysql.ErrTableExists)

	s.tk.MustExec("drop database ctwl_db")
	s.tk.MustExec("drop database ctwl_db1")
}

// TestCreateTableWithLike2 tests create table with like when refer table have non-public column/index.
func (s *testDBSuite) TestCreateTableWithLike2(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test_db")
	s.tk.MustExec("drop table if exists t1,t2;")
	defer s.tk.MustExec("drop table if exists t1,t2;")
	s.tk.MustExec("create table t1 (a int, b int, c int, index idx1(c));")

	tbl1 := s.testGetTableByName(c, "test_db", "t1")
	doneCh := make(chan error, 2)
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type != model.ActionAddColumn && job.Type != model.ActionDropColumn && job.Type != model.ActionAddIndex && job.Type != model.ActionDropIndex {
			return
		}
		if job.TableID != tbl1.Meta().ID {
			return
		}
		if job.SchemaState == model.StateDeleteOnly {
			go backgroundExec(s.store, "create table t2 like t1", doneCh)
		}
	}
	originalHook := s.dom.DDL().(ddl.DDLForTest).GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	// create table when refer table add column
	s.tk.MustExec("alter table t1 add column d int")
	checkTbl2 := func() {
		err := <-doneCh
		c.Assert(err, IsNil)
		s.tk.MustExec("alter table t2 add column e int")
		t2Info := s.testGetTableByName(c, "test_db", "t2")
		c.Assert(len(t2Info.Meta().Columns), Equals, 4)
		c.Assert(len(t2Info.Meta().Columns), Equals, len(t2Info.Cols()))
	}
	checkTbl2()

	// create table when refer table drop column
	s.tk.MustExec("drop table t2;")
	s.tk.MustExec("alter table t1 drop column b;")
	checkTbl2()

	// create table when refer table add index
	s.tk.MustExec("drop table t2;")
	s.tk.MustExec("alter table t1 add index idx2(a);")
	checkTbl2 = func() {
		err := <-doneCh
		c.Assert(err, IsNil)
		s.tk.MustExec("alter table t2 add column e int")
		tbl2 := s.testGetTableByName(c, "test_db", "t2")
		c.Assert(len(tbl2.Meta().Columns), Equals, 4)
		c.Assert(len(tbl2.Meta().Columns), Equals, len(tbl2.Cols()))
		c.Assert(len(tbl2.Meta().Indices), Equals, 1)
		c.Assert(tbl2.Meta().Indices[0].Name.L, Equals, "idx1")
	}
	checkTbl2()

	// create table when refer table drop index.
	s.tk.MustExec("drop table t2;")
	s.tk.MustExec("alter table t1 drop index idx2;")
	checkTbl2()

}

func (s *testDBSuite) TestCreateTable(c *C) {
	s.tk.MustExec("use test")
	s.tk.MustExec("CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	s.tk.MustExec("CREATE TABLE IF NOT EXISTS `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	cols := tbl.Cols()

	c.Assert(len(cols), Equals, 1)
	col := cols[0]
	c.Assert(col.Name.L, Equals, "a")
	d, ok := col.DefaultValue.(string)
	c.Assert(ok, IsTrue)
	c.Assert(d, Equals, "2.0")

	s.tk.MustExec("drop table t")

	s.testErrorCode(c, "CREATE TABLE `t` (`a` int) DEFAULT CHARSET=abcdefg", mysql.ErrUnknownCharacterSet)

	s.tk.MustExec("CREATE TABLE `collateTest` (`a` int, `b` varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_slovak_ci")
	expects := "CREATE TABLE `collateTest` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8_slovak_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_slovak_ci"
	s.tk.MustQuery("show create table collateTest").Check(testkit.Rows(expects))

	s.testErrorCode(c, "CREATE TABLE `collateTest2` (`a` int) CHARSET utf8 COLLATE utf8mb4_unicode_ci", mysql.ErrCollationCharsetMismatch)
	s.testErrorCode(c, "CREATE TABLE `collateTest3` (`a` int) COLLATE utf8mb4_unicode_ci CHARSET utf8", mysql.ErrConflictingDeclarations)

	s.tk.MustExec("CREATE TABLE `collateTest4` (`a` int) COLLATE utf8_uniCOde_ci")
	expects = "CREATE TABLE `collateTest4` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	s.tk.MustQuery("show create table collateTest4").Check(testkit.Rows(expects))

	s.tk.MustExec("create database test2 default charset utf8 collate utf8_general_ci")
	s.tk.MustExec("use test2")
	s.tk.MustExec("create table dbCollateTest (a varchar(10))")

	result := s.tk.MustQuery("show create table dbCollateTest")
	got := result.Rows()[0][1]
	c.Assert(got, Equals, "CREATE TABLE `dbCollateTest` (\n  `a` varchar(10) COLLATE utf8_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci")

	// test for enum column
	s.tk.MustExec("use test")
	failSQL := "create table t_enum (a enum('e','e'));"
	s.tk.Exec(failSQL, tmysql.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a enum('e','E'));"
	s.testErrorCode(c, failSQL, tmysql.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a enum('abc','Abc'));"
	s.testErrorCode(c, failSQL, tmysql.ErrDuplicatedValueInType)
	// test for set column
	failSQL = "create table t_enum (a set('e','e'));"
	s.testErrorCode(c, failSQL, tmysql.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a set('e','E'));"
	s.testErrorCode(c, failSQL, tmysql.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a set('abc','Abc'));"
	s.testErrorCode(c, failSQL, tmysql.ErrDuplicatedValueInType)
}

func (s *testDBSuite) TestTableForeignKey(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("create table t1 (a int, b int);")
	// test create table with foreign key.
	failSQL := "create table t2 (c int, foreign key (a) references t1(a));"
	s.testErrorCode(c, failSQL, tmysql.ErrKeyColumnDoesNotExits)
	// test add foreign key.
	s.tk.MustExec("create table t3 (a int, b int);")
	failSQL = "alter table t1 add foreign key (c) REFERENCES t3(a);"
	s.testErrorCode(c, failSQL, tmysql.ErrKeyColumnDoesNotExits)
	// Test drop column with foreign key.
	s.tk.MustExec("create table t4 (c int,d int,foreign key (d) references t1 (b));")
	failSQL = "alter table t4 drop column d"
	s.testErrorCode(c, failSQL, mysql.ErrFkColumnCannotDrop)
	// Test change column with foreign key.
	failSQL = "alter table t4 change column d e bigint;"
	s.testErrorCode(c, failSQL, mysql.ErrFKIncompatibleColumns)
	// Test modify column with foreign key.
	failSQL = "alter table t4 modify column d bigint;"
	s.testErrorCode(c, failSQL, mysql.ErrFKIncompatibleColumns)
	s.tk.MustQuery("select count(*) from information_schema.KEY_COLUMN_USAGE;")
	s.tk.MustExec("alter table t4 drop foreign key d")
	s.tk.MustExec("alter table t4 modify column d bigint;")
	s.tk.MustExec("drop table if exists t1,t2,t3,t4;")
}

func (s *testDBSuite) TestBitDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_bit (c1 bit(10) default 250, c2 int);")
	tk.MustExec("insert into t_bit set c2=1;")
	tk.MustQuery("select bin(c1),c2 from t_bit").Check(testkit.Rows("11111010 1"))
	tk.MustExec("drop table t_bit")
	tk.MustExec(`create table testalltypes1 (
    field_1 bit default 1,
    field_2 tinyint null default null
	);`)
	tk.MustExec(`create table testalltypes2 (
    field_1 bit null default null,
    field_2 tinyint null default null,
    field_3 tinyint unsigned null default null,
    field_4 bigint null default null,
    field_5 bigint unsigned null default null,
    field_6 mediumblob null default null,
    field_7 longblob null default null,
    field_8 blob null default null,
    field_9 tinyblob null default null,
    field_10 varbinary(255) null default null,
    field_11 binary(255) null default null,
    field_12 mediumtext null default null,
    field_13 longtext null default null,
    field_14 text null default null,
    field_15 tinytext null default null,
    field_16 char(255) null default null,
    field_17 numeric null default null,
    field_18 decimal null default null,
    field_19 integer null default null,
    field_20 integer unsigned null default null,
    field_21 int null default null,
    field_22 int unsigned null default null,
    field_23 mediumint null default null,
    field_24 mediumint unsigned null default null,
    field_25 smallint null default null,
    field_26 smallint unsigned null default null,
    field_27 float null default null,
    field_28 double null default null,
    field_29 double precision null default null,
    field_30 real null default null,
    field_31 varchar(255) null default null,
    field_32 date null default null,
    field_33 time null default null,
    field_34 datetime null default null,
    field_35 timestamp null default null
	);`)
}

func (s *testDBSuite) TestCreateTableWithPartition(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test;")
	s.tk.MustExec("drop table if exists tp;")
	s.tk.MustExec(`CREATE TABLE tp (a int) PARTITION BY RANGE(a) (
	PARTITION p0 VALUES LESS THAN (10),
	PARTITION p1 VALUES LESS THAN (20),
	PARTITION p2 VALUES LESS THAN (MAXVALUE)
	);`)
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Partition, NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, model.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`a`")
	for _, pdef := range part.Definitions {
		c.Assert(pdef.ID, Greater, int64(0))
	}
	c.Assert(part.Definitions, HasLen, 3)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "10")
	c.Assert(part.Definitions[0].Name.L, Equals, "p0")
	c.Assert(part.Definitions[1].LessThan[0], Equals, "20")
	c.Assert(part.Definitions[1].Name.L, Equals, "p1")
	c.Assert(part.Definitions[2].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[2].Name.L, Equals, "p2")

	s.tk.MustExec("drop table if exists employees;")
	sql1 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p2 values less than (2001)
	);`
	s.testErrorCode(c, sql1, tmysql.ErrSameNamePartition)

	sql2 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`
	s.testErrorCode(c, sql2, tmysql.ErrRangeNotIncreasing)

	sql3 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than maxvalue,
		partition p3 values less than (2001)
	);`
	s.testErrorCode(c, sql3, tmysql.ErrPartitionMaxvalue)

	sql4 := `create table t4 (
	a int not null,
  	b int not null
	)
	partition by range( id ) (
		partition p1 values less than maxvalue,
  		partition p2 values less than (1991),
  		partition p3 values less than (1995)
	);`
	s.testErrorCode(c, sql4, tmysql.ErrPartitionMaxvalue)

	_, err = s.tk.Exec(`CREATE TABLE rc (
    		a INT NOT NULL,
    		b INT NOT NULL,
			c INT NOT NULL
	)
	partition by range columns(a,b,c) (
    	partition p0 values less than (10,5,1),
    	partition p2 values less than (50,maxvalue,10),
    	partition p3 values less than (65,30,13),
    	partition p4 values less than (maxvalue,30,40)
	);`)
	c.Assert(err, IsNil)

	sql6 := `create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		 partition p0 values less than (6 , 10)
	);`
	s.testErrorCode(c, sql6, tmysql.ErrTooManyValues)

	sql7 := `create table t7 (
	a int not null,
  	b int not null
	)
	partition by range( id ) (
		partition p1 values less than (1991),
		partition p2 values less than maxvalue,
  		partition p3 values less than maxvalue,
  		partition p4 values less than (1995),
		partition p5 values less than maxvalue
	);`
	s.testErrorCode(c, sql7, tmysql.ErrPartitionMaxvalue)

	_, err = s.tk.Exec(`create table t8 (
	a int not null,
	b int not null
	)
	partition by range( id ) (
		partition p1 values less than (19xx91),
		partition p2 values less than maxvalue
	);`)
	c.Assert(ddl.ErrNotAllowedTypeInPartition.Equal(err), IsTrue)

	sql9 := `create TABLE t9 (
	col1 int
	)
	partition by range( case when col1 > 0 then 10 else 20 end ) (
		partition p0 values less than (2),
		partition p1 values less than (6)
	);`
	s.testErrorCode(c, sql9, tmysql.ErrPartitionFunctionIsNotAllowed)

	s.testErrorCode(c, `create TABLE t10 (c1 int,c2 int) partition by range(c1 / c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFunctionIsNotAllowed)

	s.tk.MustExec(`create TABLE t11 (c1 int,c2 int) partition by range(c1 div c2 ) (partition p0 values less than (2));`)
	s.tk.MustExec(`create TABLE t12 (c1 int,c2 int) partition by range(c1 + c2 ) (partition p0 values less than (2));`)
	s.tk.MustExec(`create TABLE t13 (c1 int,c2 int) partition by range(c1 - c2 ) (partition p0 values less than (2));`)
	s.tk.MustExec(`create TABLE t14 (c1 int,c2 int) partition by range(c1 * c2 ) (partition p0 values less than (2));`)
	s.tk.MustExec(`create TABLE t15 (c1 int,c2 int) partition by range( abs(c1) ) (partition p0 values less than (2));`)
	s.tk.MustExec(`create TABLE t16 (c1 int) partition by range( c1) (partition p0 values less than (10));`)

	s.testErrorCode(c, `create TABLE t17 (c1 int,c2 float) partition by range(c1 + c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	s.testErrorCode(c, `create TABLE t18 (c1 int,c2 float) partition by range( floor(c2) ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	s.tk.MustExec(`create TABLE t19 (c1 int,c2 float) partition by range( floor(c1) ) (partition p0 values less than (2));`)

	s.tk.MustExec(`create TABLE t20 (c1 int,c2 bit(10)) partition by range(c2) (partition p0 values less than (10));`)
	s.tk.MustExec(`create TABLE t21 (c1 int,c2 year) partition by range( c2 ) (partition p0 values less than (2000));`)

	s.testErrorCode(c, `create TABLE t24 (c1 float) partition by range( c1 ) (partition p0 values less than (2000));`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)

	// test check order. The sql below have 2 problem: 1. ErrFieldTypeNotAllowedAsPartitionField  2. ErrPartitionMaxvalue , mysql will return ErrPartitionMaxvalue.
	s.testErrorCode(c, `create TABLE t25 (c1 float) partition by range( c1 ) (partition p1 values less than maxvalue,partition p0 values less than (2000));`, tmysql.ErrPartitionMaxvalue)

	// Fix issue 7362.
	s.tk.MustExec("create table test_partition(id bigint, name varchar(255), primary key(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 PARTITION BY RANGE  COLUMNS(id) (PARTITION p1 VALUES LESS THAN (10) ENGINE = InnoDB);")

	// 'Less than' in partition expression could be a constant expression, notice that
	// the SHOW result changed.
	s.tk.MustExec(`create table t26 (a date)
			  partition by range(to_seconds(a))(
			  partition p0 values less than (to_seconds('2004-01-01')),
			  partition p1 values less than (to_seconds('2005-01-01')));`)
	s.tk.MustQuery("show create table t26").Check(
		testkit.Rows("t26 CREATE TABLE `t26` (\n  `a` date DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE ( to_seconds(`a`) ) (\n  PARTITION p0 VALUES LESS THAN (63240134400),\n  PARTITION p1 VALUES LESS THAN (63271756800)\n)"))
	s.tk.MustExec(`create table t27 (a bigint unsigned not null)
		  partition by range(a) (
		  partition p0 values less than (10),
		  partition p1 values less than (100),
		  partition p2 values less than (1000),
		  partition p3 values less than (18446744073709551000),
		  partition p4 values less than (18446744073709551614)
		);`)
	s.tk.MustExec(`create table t28 (a bigint unsigned not null)
		  partition by range(a) (
		  partition p0 values less than (10),
		  partition p1 values less than (100),
		  partition p2 values less than (1000),
		  partition p3 values less than (18446744073709551000 + 1),
		  partition p4 values less than (18446744073709551000 + 10)
		);`)

	// Only range type partition is now supported, range columns partition only implements the parser, so it will not be checked.
	// So the following SQL statements can be executed successfully.
	s.tk.MustExec(`create table t29 (
		a decimal
	)
	partition by range columns (a)
	(partition p0 values less than (0));`)

	s.testErrorCode(c, `create table t31 (a int not null) partition by range( a );`, tmysql.ErrPartitionsMustBeDefined)

	s.tk.MustExec(`create table t (a int) /*!50100 partition by list (a) (
partition p0 values in (1) ENGINE = InnoDB,
partition p1 values in (29) ENGINE = InnoDB,
partition p2 values in (2) ENGINE = InnoDB,
partition p3 values in (3) ENGINE = InnoDB) */`)
}

func (s *testDBSuite) TestCreateTableWithHashPartition(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test;")
	s.tk.MustExec("drop table if exists employees;")
	s.tk.MustExec(`
	create table employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash(store_id) partitions 4;`)

	s.tk.MustExec("drop table if exists employees;")
	s.tk.MustExec(`
	create table employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash( year(hired) ) partitions 4;`)
}

func (s *testDBSuite) TestCreateTableWithKeyPartition(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test;")
	s.tk.MustExec("drop table if exists tm1;")
	s.tk.MustExec(`create table tm1
	(
		s1 char(32) primary key
	)
	partition by key(s1) partitions 10;`)
}

func (s *testDBSuite) TestTableDDLWithFloatType(c *C) {
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")
	s.testErrorCode(c, "create table t (a decimal(1, 2))", tmysql.ErrMBiggerThanD)
	s.testErrorCode(c, "create table t (a float(1, 2))", tmysql.ErrMBiggerThanD)
	s.testErrorCode(c, "create table t (a double(1, 2))", tmysql.ErrMBiggerThanD)
	s.mustExec(c, "create table t (a double(1, 1))")
	s.testErrorCode(c, "alter table t add column b decimal(1, 2)", tmysql.ErrMBiggerThanD)
	// add multi columns now not support, so no case.
	s.testErrorCode(c, "alter table t modify column a float(1, 4)", tmysql.ErrMBiggerThanD)
	s.testErrorCode(c, "alter table t change column a aa float(1, 4)", tmysql.ErrMBiggerThanD)
	s.mustExec(c, "drop table t")
}

func (s *testDBSuite) TestTableDDLWithTimeType(c *C) {
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")
	s.testErrorCode(c, "create table t (a time(7))", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "create table t (a datetime(7))", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "create table t (a timestamp(7))", tmysql.ErrTooBigPrecision)
	_, err := s.tk.Exec("create table t (a time(-1))")
	c.Assert(err, NotNil)
	s.mustExec(c, "create table t (a datetime)")
	s.testErrorCode(c, "alter table t add column b time(7)", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "alter table t add column b datetime(7)", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "alter table t add column b timestamp(7)", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "alter table t modify column a time(7)", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "alter table t modify column a datetime(7)", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "alter table t modify column a timestamp(7)", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "alter table t change column a aa time(7)", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "alter table t change column a aa datetime(7)", tmysql.ErrTooBigPrecision)
	s.testErrorCode(c, "alter table t change column a aa timestamp(7)", tmysql.ErrTooBigPrecision)
	s.mustExec(c, "alter table t change column a aa datetime(0)")
	s.mustExec(c, "drop table t")
}

func (s *testDBSuite) TestTruncateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table truncate_table (c1 int, c2 int)")
	tk.MustExec("insert truncate_table values (1, 1), (2, 2)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("truncate_table"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID

	tk.MustExec("truncate table truncate_table")

	tk.MustExec("insert truncate_table values (3, 3), (4, 4)")
	tk.MustQuery("select * from truncate_table").Check(testkit.Rows("3 3", "4 4"))

	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("truncate_table"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Greater, oldTblID)

	// Verify that the old table data has been deleted by background worker.
	tablePrefix := tablecodec.EncodeTablePrefix(oldTblID)
	hasOldTableData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		err = kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
			it, err1 := txn.Iter(tablePrefix, nil)
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
		time.Sleep(waitForCleanDataInterval)
	}
	c.Assert(hasOldTableData, IsFalse)
}

func (s *testDBSuite) TestRenameTable(c *C) {
	isAlterTable := false
	s.testRenameTable(c, "rename table %s to %s", isAlterTable)
}

func (s *testDBSuite) TestAlterTableRenameTable(c *C) {
	isAlterTable := true
	s.testRenameTable(c, "alter table %s rename to %s", isAlterTable)
}

func (s *testDBSuite) testRenameTable(c *C, sql string, isAlterTable bool) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	// for different databases
	s.tk.MustExec("create table t (c1 int, c2 int)")
	s.tk.MustExec("insert t values (1, 1), (2, 2)")
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID
	s.tk.MustExec("create database test1")
	s.tk.MustExec("use test1")
	s.tk.MustExec(fmt.Sprintf(sql, "test.t", "test1.t1"))
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	s.tk.MustExec("use test")

	// Make sure t doesn't exist.
	s.tk.MustExec("create table t (c1 int, c2 int)")
	s.tk.MustExec("drop table t")

	// for the same database
	s.tk.MustExec("use test1")
	s.tk.MustExec(fmt.Sprintf(sql, "t1", "t2"))
	is = domain.GetDomain(ctx).InfoSchema()
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
	failSQL = fmt.Sprintf(sql, "test.test_not_exist", "test.test_not_exist")
	s.testErrorCode(c, failSQL, tmysql.ErrFileNotFound)
	failSQL = fmt.Sprintf(sql, "test.t_not_exist", "test_not_exist.t")
	s.testErrorCode(c, failSQL, tmysql.ErrFileNotFound)
	failSQL = fmt.Sprintf(sql, "test1.t2", "test_not_exist.t")
	s.testErrorCode(c, failSQL, tmysql.ErrErrorOnRename)

	// for the same table name
	s.tk.MustExec("use test1")
	s.tk.MustExec("create table if not exists t (c1 int, c2 int)")
	s.tk.MustExec("create table if not exists t1 (c1 int, c2 int)")
	if isAlterTable {
		s.tk.MustExec(fmt.Sprintf(sql, "test1.t", "t"))
		s.tk.MustExec(fmt.Sprintf(sql, "test1.t1", "test1.T1"))
	} else {
		s.testErrorCode(c, fmt.Sprintf(sql, "test1.t", "t"), tmysql.ErrTableExists)
		s.testErrorCode(c, fmt.Sprintf(sql, "test1.t1", "test1.T1"), tmysql.ErrTableExists)
	}

	// Test rename table name too long.
	s.testErrorCode(c, "rename table test1.t1 to test1.txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", tmysql.ErrTooLongIdent)
	s.testErrorCode(c, "alter  table test1.t1 rename to test1.txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", tmysql.ErrTooLongIdent)

	s.tk.MustExec("drop database test1")
}

func (s *testDBSuite) TestCheckConvertToCharacter(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")
	defer s.tk.MustExec("drop table t")
	s.tk.MustExec("create table t(a varchar(10) charset binary);")
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	rs, err := s.tk.Exec("alter table t modify column a varchar(10) charset utf8 collate utf8_bin")
	c.Assert(err, NotNil)
	rs, err = s.tk.Exec("alter table t modify column a varchar(10) charset utf8mb4 collate utf8mb4_bin")
	c.Assert(err, NotNil)
	rs, err = s.tk.Exec("alter table t modify column a varchar(10) charset latin collate latin1_bin")
	c.Assert(err, NotNil)
	if rs != nil {
		rs.Close()
	}
	c.Assert(t.Cols()[0].Charset, Equals, "binary")
}

func (s *testDBSuite) TestRenameMultiTables(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("create table t1(id int)")
	s.tk.MustExec("create table t2(id int)")
	// Currently it will fail only.
	sql := fmt.Sprintf("rename table t1 to t3, t2 to t4")
	_, err := s.tk.Exec(sql)
	c.Assert(err, NotNil)
	originErr := errors.Cause(err)
	c.Assert(originErr.Error(), Equals, "can't run multi schema change")

	s.tk.MustExec("drop table t1, t2")
}

func (s *testDBSuite) TestAddNotNullColumn(c *C) {
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

	s.tk.MustExec("drop table tnn")
}

func (s *testDBSuite) TestIssue2858And2717(c *C) {
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

func (s *testDBSuite) TestIssue4432(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("create table tx (col bit(10) default 'a')")
	s.tk.MustExec("insert into tx value ()")
	s.tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	s.tk.MustExec("drop table tx")

	s.tk.MustExec("create table tx (col bit(10) default 0x61)")
	s.tk.MustExec("insert into tx value ()")
	s.tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	s.tk.MustExec("drop table tx")

	s.tk.MustExec("create table tx (col bit(10) default 97)")
	s.tk.MustExec("insert into tx value ()")
	s.tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	s.tk.MustExec("drop table tx")

	s.tk.MustExec("create table tx (col bit(10) default 0b1100001)")
	s.tk.MustExec("insert into tx value ()")
	s.tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	s.tk.MustExec("drop table tx")
}

func (s *testDBSuite) TestChangeColumnPosition(c *C) {
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
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	}
	c.Assert(createSQL, Equals, strings.Join(exceptedSQL, "\n"))
}

func (s *testDBSuite) TestGeneratedColumnDDL(c *C) {
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
		"test_gv_ddl CREATE TABLE `test_gv_ddl` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) GENERATED ALWAYS AS (`a` + 8) VIRTUAL DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Check alter table add a stored generated column.
	s.tk.MustExec(`alter table test_gv_ddl add column c int as (b+2) stored`)
	result = s.tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	// Check generated expression with blanks.
	s.tk.MustExec("create table table_with_gen_col_blanks (a int, b char(20) as (cast( \r\n\t a \r\n\tas  char)))")
	result = s.tk.MustQuery(`show create table table_with_gen_col_blanks`)
	result.Check(testkit.Rows("table_with_gen_col_blanks CREATE TABLE `table_with_gen_col_blanks` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` char(20) GENERATED ALWAYS AS (CAST(`a` AS CHAR)) VIRTUAL DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

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
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c bigint(20) YES  <nil> STORED GENERATED`))

	s.tk.MustExec(`alter table test_gv_ddl change column b b bigint as (a+100) virtual`)
	result = s.tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(20) YES  <nil> VIRTUAL GENERATED`, `c bigint(20) YES  <nil> STORED GENERATED`))

	s.tk.MustExec(`alter table test_gv_ddl change column c cnew bigint`)
	result = s.tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(20) YES  <nil> VIRTUAL GENERATED`, `cnew bigint(20) YES  <nil> `))
}

func (s *testDBSuite) TestComment(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("drop table if exists ct, ct1")

	validComment := strings.Repeat("a", 1024)
	invalidComment := strings.Repeat("b", 1025)

	s.tk.MustExec("create table ct (c int, d int, e int, key (c) comment '" + validComment + "')")
	s.tk.MustExec("create index i on ct (d) comment '" + validComment + "'")
	s.tk.MustExec("alter table ct add key (e) comment '" + validComment + "'")

	s.testErrorCode(c, "create table ct1 (c int, key (c) comment '"+invalidComment+"')", tmysql.ErrTooLongIndexComment)
	s.testErrorCode(c, "create index i1 on ct (d) comment '"+invalidComment+"b"+"'", tmysql.ErrTooLongIndexComment)
	s.testErrorCode(c, "alter table ct add key (e) comment '"+invalidComment+"'", tmysql.ErrTooLongIndexComment)

	s.tk.MustExec("set @@sql_mode=''")
	s.tk.MustExec("create table ct1 (c int, d int, e int, key (c) comment '" + invalidComment + "')")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'c' is too long (max = 1024)"))
	s.tk.MustExec("create index i1 on ct1 (d) comment '" + invalidComment + "b" + "'")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'i1' is too long (max = 1024)"))
	s.tk.MustExec("alter table ct1 add key (e) comment '" + invalidComment + "'")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1688|Comment for index 'e' is too long (max = 1024)"))

	s.tk.MustExec("drop table if exists ct, ct1")
}

func (s *testDBSuite) TestRebaseAutoID(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("drop database if exists tidb;")
	s.tk.MustExec("create database tidb;")
	s.tk.MustExec("use tidb;")
	s.tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	s.tk.MustExec("insert tidb.test values (null, 1);")
	s.tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1"))
	s.tk.MustExec("alter table tidb.test auto_increment = 6000;")
	s.tk.MustExec("insert tidb.test values (null, 1);")
	s.tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1"))
	s.tk.MustExec("alter table tidb.test auto_increment = 5;")
	s.tk.MustExec("insert tidb.test values (null, 1);")
	s.tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1"))

	// Current range for table test is [11000, 15999].
	// Though it does not have a tuple "a = 15999", its global next auto increment id should be 16000.
	// Anyway it is not compatible with MySQL.
	s.tk.MustExec("alter table tidb.test auto_increment = 12000;")
	s.tk.MustExec("insert tidb.test values (null, 1);")
	s.tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1", "16000 1"))

	s.tk.MustExec("create table tidb.test2 (a int);")
	s.testErrorCode(c, "alter table tidb.test2 add column b int auto_increment key, auto_increment=10;", tmysql.ErrUnknown)
}

func (s *testDBSuite) TestZeroFillCreateTable(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists abc;")
	s.tk.MustExec("create table abc(y year, z tinyint(10) zerofill, primary key(y));")
	is := s.dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("abc"))
	c.Assert(err, IsNil)
	var yearCol, zCol *model.ColumnInfo
	for _, col := range tbl.Meta().Columns {
		if col.Name.String() == "y" {
			yearCol = col
		}
		if col.Name.String() == "z" {
			zCol = col
		}
	}
	c.Assert(yearCol, NotNil)
	c.Assert(yearCol.Tp, Equals, mysql.TypeYear)
	c.Assert(mysql.HasUnsignedFlag(yearCol.Flag), IsTrue)

	c.Assert(zCol, NotNil)
	c.Assert(mysql.HasUnsignedFlag(zCol.Flag), IsTrue)
}

func (s *testDBSuite) TestCheckColumnDefaultValue(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test;")
	s.tk.MustExec("drop table if exists text_default_text;")
	s.testErrorCode(c, "create table text_default_text(c1 text not null default '');", tmysql.ErrBlobCantHaveDefault)
	s.testErrorCode(c, "create table text_default_text(c1 text not null default 'scds');", tmysql.ErrBlobCantHaveDefault)

	s.tk.MustExec("drop table if exists text_default_json;")
	s.testErrorCode(c, "create table text_default_json(c1 json not null default '');", tmysql.ErrBlobCantHaveDefault)
	s.testErrorCode(c, "create table text_default_json(c1 json not null default 'dfew555');", tmysql.ErrBlobCantHaveDefault)

	s.tk.MustExec("drop table if exists text_default_blob;")
	s.testErrorCode(c, "create table text_default_blob(c1 blob not null default '');", tmysql.ErrBlobCantHaveDefault)
	s.testErrorCode(c, "create table text_default_blob(c1 blob not null default 'scds54');", tmysql.ErrBlobCantHaveDefault)

	s.tk.MustExec("set sql_mode='';")
	s.tk.MustExec("drop table if exists text_default_text;")
	s.tk.MustExec("create table text_default_text(c1 text not null default '');")
	s.tk.MustQuery(`show create table text_default_text`).Check(testutil.RowsWithSep("|",
		"text_default_text CREATE TABLE `text_default_text` (\n"+
			"  `c1` text NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_text"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, "")

	s.tk.MustExec("drop table if exists text_default_blob;")
	s.tk.MustExec("create table text_default_blob(c1 blob not null default '');")
	s.tk.MustQuery(`show create table text_default_blob`).Check(testutil.RowsWithSep("|",
		"text_default_blob CREATE TABLE `text_default_blob` (\n"+
			"  `c1` blob NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_blob"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, "")

	s.tk.MustExec("drop table if exists text_default_json;")
	s.tk.MustExec("create table text_default_json(c1 json not null default '');")
	s.tk.MustQuery(`show create table text_default_json`).Check(testutil.RowsWithSep("|",
		"text_default_json CREATE TABLE `text_default_json` (\n"+
			"  `c1` json NOT NULL DEFAULT 'null'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_json"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, `null`)
}

func (s *testDBSuite) TestCharacterSetInColumns(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("drop database if exists varchar_test;")
	s.tk.MustExec("create database varchar_test;")
	s.tk.MustExec("use varchar_test")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustExec("create table t (c1 int, s1 varchar(10), s2 text)")
	s.tk.MustQuery("select count(*) from information_schema.columns where table_schema = 'varchar_test' and character_set_name != 'utf8mb4'").Check(testkit.Rows("0"))
	s.tk.MustQuery("select count(*) from information_schema.columns where table_schema = 'varchar_test' and character_set_name = 'utf8mb4'").Check(testkit.Rows("2"))

	s.tk.MustExec("drop table if exists t5")
	s.tk.MustExec("create table t5(id int) charset=UTF8;")
	s.tk.MustExec("drop table if exists t5")
	s.tk.MustExec("create table t5(id int) charset=BINARY;")
	s.tk.MustExec("drop table if exists t5")
	s.tk.MustExec("create table t5(id int) charset=LATIN1;")
	s.tk.MustExec("drop table if exists t5")
	s.tk.MustExec("create table t5(id int) charset=ASCII;")
	s.tk.MustExec("drop table if exists t5")
	s.tk.MustExec("create table t5(id int) charset=UTF8MB4;")

	s.tk.MustExec("drop table if exists t6")
	s.tk.MustExec("create table t6(id int) charset=utf8;")
	s.tk.MustExec("drop table if exists t6")
	s.tk.MustExec("create table t6(id int) charset=binary;")
	s.tk.MustExec("drop table if exists t6")
	s.tk.MustExec("create table t6(id int) charset=latin1;")
	s.tk.MustExec("drop table if exists t6")
	s.tk.MustExec("create table t6(id int) charset=ascii;")
	s.tk.MustExec("drop table if exists t6")
	s.tk.MustExec("create table t6(id int) charset=utf8mb4;")
}

func (s *testDBSuite) TestAddNotNullColumnWhileInsertOnDupUpdate(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use " + s.schemaName)
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use " + s.schemaName)
	closeCh := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	tk1.MustExec("create table nn (a int primary key, b int)")
	tk1.MustExec("insert nn values (1, 1)")
	var tk2Err error
	go func() {
		defer wg.Done()
		for {
			select {
			case <-closeCh:
				return
			default:
			}
			_, tk2Err = tk2.Exec("insert nn (a, b) values (1, 1) on duplicate key update a = 1, b = values(b) + 1")
			if tk2Err != nil {
				return
			}
		}
	}()
	tk1.MustExec("alter table nn add column c int not null default 3 after a")
	close(closeCh)
	wg.Wait()
	c.Assert(tk2Err, IsNil)
	tk1.MustQuery("select * from nn").Check(testkit.Rows("1 3 2"))
}

type testMaxTableRowIDContext struct {
	c   *C
	d   ddl.DDL
	tbl table.Table
}

func newTestMaxTableRowIDContext(c *C, d ddl.DDL, tbl table.Table) *testMaxTableRowIDContext {
	return &testMaxTableRowIDContext{
		c:   c,
		d:   d,
		tbl: tbl,
	}
}

func (s *testDBSuite) getMaxTableRowID(ctx *testMaxTableRowIDContext) (int64, bool) {
	c := ctx.c
	d := ctx.d
	tbl := ctx.tbl
	curVer, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	maxID, emptyTable, err := d.GetTableMaxRowID(curVer.Ver, tbl.(table.PhysicalTable))
	c.Assert(err, IsNil)
	return maxID, emptyTable
}

func (s *testDBSuite) checkGetMaxTableRowID(ctx *testMaxTableRowIDContext, expectEmpty bool, expectMaxID int64) {
	c := ctx.c
	maxID, emptyTable := s.getMaxTableRowID(ctx)
	c.Assert(emptyTable, Equals, expectEmpty)
	c.Assert(maxID, Equals, expectMaxID)
}

func (s *testDBSuite) TestGetTableEndHandle(c *C) {
	// TestGetTableEndHandle test ddl.GetTableMaxRowID method, which will return the max row id of the table.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	// Test PK is handle.
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")

	is := s.dom.InfoSchema()
	d := s.dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	testCtx := newTestMaxTableRowIDContext(c, d, tbl)
	// test empty table
	s.checkGetMaxTableRowID(testCtx, true, int64(math.MaxInt64))

	tk.MustExec("insert into t values(-1, 1)")
	s.checkGetMaxTableRowID(testCtx, false, int64(-1))

	tk.MustExec("insert into t values(9223372036854775806, 1)")
	s.checkGetMaxTableRowID(testCtx, false, int64(9223372036854775806))

	tk.MustExec("insert into t values(9223372036854775807, 1)")
	s.checkGetMaxTableRowID(testCtx, false, int64(9223372036854775807))

	tk.MustExec("insert into t values(10, 1)")
	tk.MustExec("insert into t values(102149142, 1)")
	s.checkGetMaxTableRowID(testCtx, false, int64(9223372036854775807))

	tk.MustExec("create table t1(a bigint PRIMARY KEY, b int)")

	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
	}
	is = s.dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	s.checkGetMaxTableRowID(testCtx, false, int64(999))

	// Test PK is not handle
	tk.MustExec("create table t2(a varchar(255))")

	is = s.dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	s.checkGetMaxTableRowID(testCtx, true, int64(math.MaxInt64))

	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", i))
	}

	result := tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable := s.getMaxTableRowID(testCtx)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec("insert into t2 values(100000)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = s.getMaxTableRowID(testCtx)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64-1))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = s.getMaxTableRowID(testCtx)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = s.getMaxTableRowID(testCtx)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec("insert into t2 values(100)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = s.getMaxTableRowID(testCtx)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)
}

func (s *testDBSuite) TestMultiRegionGetTableEndHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")

	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DDL()
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, 100)

	maxID, emptyTable := s.getMaxTableRowID(testCtx)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(999))

	tk.MustExec("insert into t values(10000, 1000)")
	maxID, emptyTable = s.getMaxTableRowID(testCtx)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(10000))

	tk.MustExec("insert into t values(-1, 1000)")
	maxID, emptyTable = s.getMaxTableRowID(testCtx)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(10000))
}

func (s *testDBSuite) getHistoryDDLJob(id int64) (*model.Job, error) {
	var job *model.Job

	err := kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDDLJob(id)
		return errors.Trace(err1)
	})

	return job, errors.Trace(err)
}

func (s *testDBSuite) TestBackwardCompatibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_backward_compatibility")
	defer tk.MustExec("drop database test_backward_compatibility")
	tk.MustExec("use test_backward_compatibility")
	tk.MustExec("create table t(a int primary key, b int)")
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// alter table t add index idx_b(b);
	is := s.dom.InfoSchema()
	schemaName := model.NewCIStr("test_backward_compatibility")
	tableName := model.NewCIStr("t")
	schema, ok := is.SchemaByName(schemaName)
	c.Assert(ok, IsTrue)
	tbl, err := is.TableByName(schemaName, tableName)
	c.Assert(err, IsNil)

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tbl.Meta().ID, 100)

	unique := false
	indexName := model.NewCIStr("idx_b")
	idxColName := &ast.IndexColName{
		Column: &ast.ColumnName{
			Schema: schemaName,
			Table:  tableName,
			Name:   model.NewCIStr("b"),
		},
		Length: types.UnspecifiedLength,
	}
	idxColNames := []*ast.IndexColName{idxColName}
	var indexOption *ast.IndexOption
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbl.Meta().ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, idxColNames, indexOption},
	}
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	job.ID, err = t.GenGlobalID()
	c.Assert(err, IsNil)
	job.Version = 1
	job.StartTS = txn.StartTS()

	// Simulate old TiDB init the add index job, old TiDB will not init the model.Job.ReorgMeta field,
	// if we set job.SnapshotVer here, can simulate the behavior.
	job.SnapshotVer = txn.StartTS()
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	ticker := time.NewTicker(s.lease)
	for range ticker.C {
		historyJob, err := s.getHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		if historyJob == nil {

			continue
		}
		c.Assert(historyJob.Error, IsNil)

		if historyJob.IsSynced() {
			break
		}
	}

	// finished add index
	tk.MustExec("admin check index t idx_b")
}

func (s *testDBSuite) TestAlterTableAddPartition(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test;")
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1")
	s.tk.MustExec("drop table if exists employees;")
	s.tk.MustExec(`create table employees (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	s.tk.MustExec(`alter table employees add partition (
    partition p4 values less than (2010),
    partition p5 values less than MAXVALUE
	);`)

	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("employees"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Partition, NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, model.PartitionTypeRange)

	c.Assert(part.Expr, Equals, "year(`hired`)")
	c.Assert(part.Definitions, HasLen, 5)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "1991")
	c.Assert(part.Definitions[0].Name, Equals, model.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "1996")
	c.Assert(part.Definitions[1].Name, Equals, model.NewCIStr("p2"))
	c.Assert(part.Definitions[2].LessThan[0], Equals, "2001")
	c.Assert(part.Definitions[2].Name, Equals, model.NewCIStr("p3"))
	c.Assert(part.Definitions[3].LessThan[0], Equals, "2010")
	c.Assert(part.Definitions[3].Name, Equals, model.NewCIStr("p4"))
	c.Assert(part.Definitions[4].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[4].Name, Equals, model.NewCIStr("p5"))

	s.tk.MustExec("drop table if exists table1;")
	s.tk.MustExec("create table table1(a int)")
	sql1 := `alter table table1 add partition (
		partition p1 values less than (2010),
		partition p2 values less than maxvalue
	);`
	s.testErrorCode(c, sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)

	sql2 := "alter table table1 add partition"
	s.testErrorCode(c, sql2, tmysql.ErrPartitionsMustBeDefined)

	s.tk.MustExec("drop table if exists table2;")
	s.tk.MustExec(`create table table2 (

	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p2 values less than maxvalue
	);`)

	sql3 := `alter table table2 add partition (
		partition p3 values less than (2010)
	);`
	s.testErrorCode(c, sql3, tmysql.ErrPartitionMaxvalue)

	s.tk.MustExec("drop table if exists table3;")
	s.tk.MustExec(`create table table3 (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p2 values less than (2001)
	);`)

	sql4 := `alter table table3 add partition (
		partition p3 values less than (1993)
	);`
	s.testErrorCode(c, sql4, tmysql.ErrRangeNotIncreasing)

	sql5 := `alter table table3 add partition (
		partition p1 values less than (1993)
	);`
	s.testErrorCode(c, sql5, tmysql.ErrSameNamePartition)

	sql6 := `alter table table3 add partition (
		partition p1 values less than (1993),
		partition p1 values less than (1995)
	);`
	s.testErrorCode(c, sql6, tmysql.ErrSameNamePartition)

	sql7 := `alter table table3 add partition (
		partition p4 values less than (1993),
		partition p1 values less than (1995),
		partition p5 values less than maxvalue
	);`
	s.testErrorCode(c, sql7, tmysql.ErrSameNamePartition)
}

func (s *testDBSuite) TestAlterTableDropPartition(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1")
	s.tk.MustExec("drop table if exists employees")
	s.tk.MustExec(`create table employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)

	s.tk.MustExec("alter table employees drop partition p3;")
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("employees"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().GetPartitionInfo(), NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, model.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`hired`")
	c.Assert(part.Definitions, HasLen, 2)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "1991")
	c.Assert(part.Definitions[0].Name, Equals, model.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "1996")
	c.Assert(part.Definitions[1].Name, Equals, model.NewCIStr("p2"))

	s.tk.MustExec("drop table if exists table1;")
	s.tk.MustExec("create table table1 (a int);")
	sql1 := "alter table table1 drop partition p10;"
	s.testErrorCode(c, sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)

	s.tk.MustExec("drop table if exists table2;")
	s.tk.MustExec(`create table table2 (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	sql2 := "alter table table2 drop partition p10;"
	s.testErrorCode(c, sql2, tmysql.ErrDropPartitionNonExistent)

	s.tk.MustExec("drop table if exists table3;")
	s.tk.MustExec(`create table table3 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (1991)
	);`)
	sql3 := "alter table table3 drop partition p1;"
	s.testErrorCode(c, sql3, tmysql.ErrDropLastPartition)

	s.tk.MustExec("drop table if exists table4;")
	s.tk.MustExec(`create table table4 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than MAXVALUE
	);`)

	s.tk.MustExec("alter table table4 drop partition p2;")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("table4"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().GetPartitionInfo(), NotNil)
	part = tbl.Meta().Partition
	c.Assert(part.Type, Equals, model.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`id`")
	c.Assert(part.Definitions, HasLen, 2)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "10")
	c.Assert(part.Definitions[0].Name, Equals, model.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[1].Name, Equals, model.NewCIStr("p3"))

	s.tk.MustExec("drop table if exists tr;")
	s.tk.MustExec(` create table tr(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	s.tk.MustExec(`INSERT INTO tr VALUES
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2014-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result := s.tk.MustQuery("select * from tr where purchased between '1995-01-01' and '1999-12-31';")
	result.Check(testkit.Rows(`2 alarm clock 1997-11-05`, `10 lava lamp 1998-12-25`))
	s.tk.MustExec("alter table tr drop partition p2;")
	result = s.tk.MustQuery("select * from tr where purchased between '1995-01-01' and '1999-12-31';")
	result.Check(testkit.Rows())

	result = s.tk.MustQuery("select * from tr where purchased between '2010-01-01' and '2014-12-31';")
	result.Check(testkit.Rows(`5 exercise bike 2014-05-09`, `7 espresso maker 2011-11-22`))
	s.tk.MustExec("alter table tr drop partition p5;")
	result = s.tk.MustQuery("select * from tr where purchased between '2010-01-01' and '2014-12-31';")
	result.Check(testkit.Rows())

	s.tk.MustExec("alter table tr drop partition p4;")
	result = s.tk.MustQuery("select * from tr where purchased between '2005-01-01' and '2009-12-31';")
	result.Check(testkit.Rows())

	s.tk.MustExec("drop table if exists table4;")
	s.tk.MustExec(`create table table4 (
		id int not null
	)
	partition by range( id ) (
		partition Par1 values less than (1991),
		partition pAR2 values less than (1992),
		partition Par3 values less than (1995),
		partition PaR5 values less than (1996)
	);`)
	s.tk.MustExec("alter table table4 drop partition Par2;")
	s.tk.MustExec("alter table table4 drop partition PAR5;")
	sql4 := "alter table table4 drop partition PAR0;"
	s.testErrorCode(c, sql4, tmysql.ErrDropPartitionNonExistent)
}

func (s *testDBSuite) TestAlterTableTruncatePartition(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists employees")
	s.tk.MustExec("set @@tidb_enable_table_partition = 1")
	s.tk.MustExec(`create table employees (
	  id int not null,
	  hired int not null
	) partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	)`)
	s.tk.MustExec("insert into employees values (1, 1990)")
	s.tk.MustExec("insert into employees values (2, 1995)")
	s.tk.MustExec("insert into employees values (3, 2000)")
	result := s.tk.MustQuery("select * from employees order by id")
	result.Check(testkit.Rows(`1 1990`, `2 1995`, `3 2000`))

	s.testErrorCode(c, "alter table employees truncate partition xxx", tmysql.ErrUnknownPartition)

	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("employees"))
	c.Assert(err, IsNil)
	oldPID := oldTblInfo.Meta().Partition.Definitions[0].ID

	s.tk.MustExec("alter table employees truncate partition p1")
	result = s.tk.MustQuery("select * from employees order by id")
	result.Check(testkit.Rows(`2 1995`, `3 2000`))

	partitionPrefix := tablecodec.EncodeTablePrefix(oldPID)
	hasOldPartitionData := checkPartitionDelRangeDone(c, s, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)

	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("employees"))
	c.Assert(err, IsNil)
	newPID := oldTblInfo.Meta().Partition.Definitions[0].ID
	c.Assert(oldPID != newPID, IsTrue)

	s.tk.MustExec("alter table employees truncate partition p3")
	result = s.tk.MustQuery("select * from employees")
	result.Check(testkit.Rows(`2 1995`))

	s.tk.MustExec("insert into employees values (1, 1984)")
	result = s.tk.MustQuery("select * from employees order by id")
	result.Check(testkit.Rows(`1 1984`, `2 1995`))
	s.tk.MustExec("insert into employees values (3, 2000)")
	result = s.tk.MustQuery("select * from employees order by id")
	result.Check(testkit.Rows(`1 1984`, `2 1995`, `3 2000`))

	s.tk.MustExec(`create table non_partition (id int)`)
	s.testErrorCode(c, "alter table non_partition truncate partition p0", tmysql.ErrPartitionMgmtOnNonpartitioned)
}

func (s *testDBSuite) TestAddPartitionTooManyPartitions(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1")
	count := ddl.PartitionCountLimit
	s.tk.MustExec("drop table if exists p1;")
	sql1 := `create table p1 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i <= count; i++ {
		sql1 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql1 += "partition p1025 values less than (1025) );"
	s.testErrorCode(c, sql1, tmysql.ErrTooManyPartitions)

	s.tk.MustExec("drop table if exists p2;")
	sql2 := `create table p2 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i < count; i++ {
		sql2 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql2 += "partition p1024 values less than (1024) );"

	s.tk.MustExec(sql2)
	sql3 := `alter table p2 add partition (
   	partition p1025 values less than (1025)
	);`
	s.testErrorCode(c, sql3, tmysql.ErrTooManyPartitions)
}

func checkPartitionDelRangeDone(c *C, s *testDBSuite, partitionPrefix kv.Key) bool {
	hasOldPartitionData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		err := kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
			it, err := txn.Iter(partitionPrefix, nil)
			if err != nil {
				return err
			}
			if !it.Valid() {
				hasOldPartitionData = false
			} else {
				hasOldPartitionData = it.Key().HasPrefix(partitionPrefix)
			}
			it.Close()
			return nil
		})
		c.Assert(err, IsNil)
		if !hasOldPartitionData {
			break
		}
		time.Sleep(waitForCleanDataInterval)
	}
	return hasOldPartitionData
}

func (s *testDBSuite) TestTruncatePartitionAndDropTable(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test;")
	// Test truncate common table.
	s.tk.MustExec("drop table if exists t1;")
	s.tk.MustExec("create table t1 (id int(11));")
	for i := 0; i < 100; i++ {
		s.mustExec(c, "insert into t1 values (?)", i)
	}
	result := s.tk.MustQuery("select count(*) from t1;")
	result.Check(testkit.Rows("100"))
	s.tk.MustExec("truncate table t1;")
	result = s.tk.MustQuery("select count(*) from t1")
	result.Check(testkit.Rows("0"))

	// Test drop common table.
	s.tk.MustExec("drop table if exists t2;")
	s.tk.MustExec("create table t2 (id int(11));")
	for i := 0; i < 100; i++ {
		s.mustExec(c, "insert into t2 values (?)", i)
	}
	result = s.tk.MustQuery("select count(*) from t2;")
	result.Check(testkit.Rows("100"))
	s.tk.MustExec("drop table t2;")
	s.testErrorCode(c, "select * from t2;", tmysql.ErrNoSuchTable)

	// Test truncate table partition.
	s.tk.MustExec("drop table if exists t3;")
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	s.tk.MustExec(`create table t3(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	s.tk.MustExec(`insert into t3 values
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2014-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result = s.tk.MustQuery("select count(*) from t3;")
	result.Check(testkit.Rows("10"))
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	oldPID := oldTblInfo.Meta().Partition.Definitions[0].ID
	s.tk.MustExec("truncate table t3;")
	partitionPrefix := tablecodec.EncodeTablePrefix(oldPID)
	hasOldPartitionData := checkPartitionDelRangeDone(c, s, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)

	// Test drop table partition.
	s.tk.MustExec("drop table if exists t4;")
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	s.tk.MustExec(`create table t4(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	s.tk.MustExec(`insert into t4 values
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2014-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result = s.tk.MustQuery("select count(*) from t4; ")
	result.Check(testkit.Rows("10"))
	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	oldPID = oldTblInfo.Meta().Partition.Definitions[1].ID
	s.tk.MustExec("drop table t4;")
	partitionPrefix = tablecodec.EncodeTablePrefix(oldPID)
	hasOldPartitionData = checkPartitionDelRangeDone(c, s, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)
	s.testErrorCode(c, "select * from t4;", tmysql.ErrNoSuchTable)

	// Test truncate table partition reassigns new partitionIDs.
	s.tk.MustExec("drop table if exists t5;")
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	s.tk.MustExec(`create table t5(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t5"))
	c.Assert(err, IsNil)
	oldPID = oldTblInfo.Meta().Partition.Definitions[0].ID
	s.tk.MustExec("truncate table t5;")
	is = domain.GetDomain(ctx).InfoSchema()
	c.Assert(err, IsNil)
	newTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t5"))
	newPID := newTblInfo.Meta().Partition.Definitions[0].ID
	c.Assert(oldPID != newPID, IsTrue)
}

func (s *testDBSuite) TestPartitionUniqueKeyNeedAllFieldsInPf(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test;")
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	s.tk.MustExec("drop table if exists part1;")
	s.tk.MustExec(`create table part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1, col2)
	)
	partition by range( col1 + col2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	s.tk.MustExec("drop table if exists part2;")
	s.tk.MustExec(`create table part2 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1, col2, col3),
  		unique key (col3)
	)
	partition by range( col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	s.tk.MustExec("drop table if exists part3;")
	s.tk.MustExec(`create table part3 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col2)
	)
	partition by range( col1 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	s.tk.MustExec("drop table if exists part4;")
	s.tk.MustExec(`create table part4 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col2),
		unique key(col2)
	)
	partition by range( year(col2)  ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	s.tk.MustExec("drop table if exists part5;")
	s.tk.MustExec(`create table part5 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col2, col4),
    	unique key(col2, col1)
	)
	partition by range( col1 + col2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	s.tk.MustExec("drop table if exists Part1;")
	sql1 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1, col2)
	)
	partition by range( col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	s.testErrorCode(c, sql1, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	s.tk.MustExec("drop table if exists Part1;")
	sql2 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1),
		unique key (col3)
	)
	partition by range( col1 + col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	s.testErrorCode(c, sql2, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	s.tk.MustExec("drop table if exists Part1;")
	sql3 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1),
		unique key (col3)
	)
	partition by range( col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	s.testErrorCode(c, sql3, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	s.tk.MustExec("drop table if exists Part1;")
	sql4 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		unique key (col1, col2, col3),
  		unique key (col3)
	)
	partition by range( col1 + col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	s.testErrorCode(c, sql4, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	s.tk.MustExec("drop table if exists Part1;")
	sql5 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col2)
	)
	partition by range( col3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	s.testErrorCode(c, sql5, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	s.tk.MustExec("drop table if exists Part1;")
	sql6 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col3),
		unique key(col2)
	)
	partition by range( year(col2)  ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	s.testErrorCode(c, sql6, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	s.tk.MustExec("drop table if exists Part1;")
	sql7 := `create table Part1 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		primary key(col1, col3, col4),
    	unique key(col2, col1)
	)
	partition by range( col1 + col2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	s.testErrorCode(c, sql7, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	s.tk.MustExec("drop table if exists part6;")
	sql8 := `create table part6 (
		col1 int not null,
		col2 date not null,
		col3 int not null,
		col4 int not null,
		col5 int not null,
		unique key(col1, col2),
		unique key(col1, col3)
	)
	partition by range( col1 + col2 ) (
	partition p1 values less than (11),
  	partition p2 values less than (15)
	);`
	s.testErrorCode(c, sql8, tmysql.ErrUniqueKeyNeedAllFieldsInPf)
}

func (s *testDBSuite) TestPartitionDropIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	done := make(chan error, 1)
	s.tk.MustExec("use " + s.schemaName)
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	s.tk.MustExec("drop table if exists partition_drop_idx;")
	s.tk.MustExec(`create table partition_drop_idx (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (3),
    	partition p1 values less than (5),
    	partition p2 values less than (7),
    	partition p3 values less than (11),
    	partition p4 values less than (15),
    	partition p5 values less than (20),
		partition p6 values less than (maxvalue)
   	);`)

	num := 20
	for i := 0; i < num; i++ {
		s.mustExec(c, "insert into partition_drop_idx values (?, ?, ?)", i, i, i)
	}
	s.tk.MustExec("alter table partition_drop_idx add index idx1 (c1)")

	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr(s.schemaName), model.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)

	var idx1 table.Index
	for _, pidx := range t.Indices() {
		if pidx.Meta().Name.L == "idx1" {
			idx1 = pidx
			break
		}
	}
	c.Assert(idx1, NotNil)

	sessionExecInGoroutine(c, s.store, "drop index idx1 on partition_drop_idx;", done)
	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err = <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			step := 10
			rand.Seed(time.Now().Unix())
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				s.mustExec(c, "update partition_drop_idx set c2 = 1 where c1 = ?", n)
				s.mustExec(c, "insert into partition_drop_idx values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	is = domain.GetDomain(ctx).InfoSchema()
	t, err = is.TableByName(model.NewCIStr(s.schemaName), model.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	var idxn table.Index
	t.Indices()
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == "idx1" {
			idxn = idx
			break
		}
	}
	c.Assert(idxn, IsNil)
	idx := tables.NewIndex(pid, t.Meta(), idx1.Meta())
	checkDelRangeDone(c, ctx, idx)
	s.tk.MustExec("drop table partition_drop_idx;")
}

func (s *testDBSuite) TestPartitionCancelAddIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "set @@session.tidb_enable_table_partition=1;")
	s.mustExec(c, "drop table if exists t1;")
	s.mustExec(c, `create table t1 (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (1024),
    	partition p1 values less than (2048),
    	partition p2 values less than (3072),
    	partition p3 values less than (4096),
		partition p4 values less than (maxvalue)
   	);`)
	base := defaultBatchSize * 2
	count := base
	// add some rows
	for i := 0; i < count; i++ {
		s.mustExec(c, "insert into t1 values (?, ?, ?)", i, i, i)
	}

	var checkErr error
	var c3IdxInfo *model.IndexInfo
	hook := &ddl.TestDDLCallback{}
	oldReorgWaitTimeout := ddl.ReorgWaitTimeout
	ddl.ReorgWaitTimeout = 10 * time.Millisecond
	hook.OnJobUpdatedExported, c3IdxInfo, checkErr = backgroundExecOnJobUpdatedExported(c, s, hook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundExec(s.store, "create index c3_index on t1 (c3)", done)

	times := 0
	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:12]cancelled DDL job")
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 10
			rand.Seed(time.Now().Unix())
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
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, "c3_index"), IsFalse)
	}

	ctx := s.s.(sessionctx.Context)
	idx := tables.NewIndex(pid, t.Meta(), c3IdxInfo)
	checkDelRangeDone(c, ctx, idx)

	s.mustExec(c, "drop table t1")
	ddl.ReorgWaitTimeout = oldReorgWaitTimeout
	callback := &ddl.TestDDLCallback{}
	s.dom.DDL().(ddl.DDLForTest).SetHook(callback)
}

func backgroundExecOnJobUpdatedExported(c *C, s *testDBSuite, hook *ddl.TestDDLCallback) (func(*model.Job), *model.IndexInfo, error) {
	var checkErr error
	first := true
	ddl.ReorgWaitTimeout = 10 * time.Millisecond
	c3IdxInfo := &model.IndexInfo{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		addIndexNotFirstReorg := job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0
		// If the action is adding index and the state is writing reorganization, it want to test the case of cancelling the job when backfilling indexes.
		// When the job satisfies this case of addIndexNotFirstReorg, the worker will start to backfill indexes.
		if !addIndexNotFirstReorg {
			// Get the index's meta.
			if c3IdxInfo != nil {
				return
			}
			t := s.testGetTable(c, "t1")
			for _, index := range t.WritableIndices() {
				if index.Meta().Name.L == "c3_index" {
					c3IdxInfo = index.Meta()
				}
			}
			return
		}
		// The job satisfies the case of addIndexNotFirst for the first time, the worker hasn't finished a batch of backfill indexes.
		if first {
			first = false
			return
		}
		if checkErr != nil {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.Store = s.store
		var err error
		err = hookCtx.NewTxn()
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		jobIDs := []int64{job.ID}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		errs, err := admin.CancelJobs(txn, jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		// It only tests cancel one DDL job.
		if errs[0] != nil {
			checkErr = errors.Trace(errs[0])
			return
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}
	return hook.OnJobUpdatedExported, c3IdxInfo, checkErr
}

func (s *testDBSuite) TestPartitionAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	tk.MustExec(`create table partition_add_idx (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p3 values less than (2001),
	partition p4 values less than (2004),
	partition p5 values less than (2008),
	partition p6 values less than (2012),
	partition p7 values less than (2018)
	);`)
	for i := 0; i < 500; i++ {
		tk.MustExec(fmt.Sprintf("insert into partition_add_idx values (%d, '%d-01-01')", i, 1988+rand.Intn(30)))
	}

	tk.MustExec("alter table partition_add_idx add index idx1 (hired)")
	tk.MustExec("alter table partition_add_idx add index idx2 (id, hired)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("partition_add_idx"))
	c.Assert(err, IsNil)
	var idx1 table.Index
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == "idx1" {
			idx1 = idx
			break
		}
	}
	c.Assert(idx1, NotNil)

	tk.MustQuery("select count(hired) from partition_add_idx use index(idx1)").Check(testkit.Rows("500"))
	tk.MustQuery("select count(id) from partition_add_idx use index(idx2)").Check(testkit.Rows("500"))

	tk.MustExec("admin check table partition_add_idx")
	tk.MustExec("drop table partition_add_idx")

	// Test hash partition for pr 10475.
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec("create table t1 (a int,b int,  primary key(a)) partition by hash(a) partitions 5;")
	tk.MustExec("insert into t1 values (0,0),(1,1),(2,2),(3,3);")
	tk.MustExec("alter table t1 add index idx(a)")
	tk.MustExec("admin check table t1;")

	// Test range partition for pr 10475.
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int,b int,  primary key(a)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20));")
	tk.MustExec("insert into t1 values (0,0);")
	tk.MustExec("alter table t1 add index idx(a)")
	tk.MustExec("admin check table t1;")
}

func (s *testDBSuite) TestAlterTableCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test_charset")
	defer tk.MustExec("drop database test_charset")
	tk.MustExec("use test_charset")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int) charset latin1")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test_charset"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(t.Meta().Charset, Equals, "latin1")
	defCollate, err := charset.GetDefaultCollation("latin1")
	c.Assert(err, IsNil)
	c.Assert(t.Meta().Collate, Equals, defCollate)

	rs, err := tk.Exec("alter table t charset utf8")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err, NotNil)
	is = domain.GetDomain(ctx).InfoSchema()
	t, err = is.TableByName(model.NewCIStr("test_charset"), model.NewCIStr("t"))
	c.Assert(t.Meta().Charset, Equals, "latin1")
	defCollate, err = charset.GetDefaultCollation("latin1")
	c.Assert(err, IsNil)
	c.Assert(t.Meta().Collate, Equals, defCollate)

	rs, err = tk.Exec("alter table t charset utf8mb4 collate utf8mb4_general_ci")
	is = domain.GetDomain(ctx).InfoSchema()
	t, err = is.TableByName(model.NewCIStr("test_charset"), model.NewCIStr("t"))
	c.Assert(t.Meta().Charset, Equals, "latin1")
	c.Assert(t.Meta().Collate, Equals, "latin1_bin")

	rs, err = tk.Exec("alter table t charset utf8")
	if rs != nil {
		rs.Close()
	}

	c.Assert(err.Error(), Equals, "[ddl:210]unsupported modify charset from latin1 to utf8")
}

func (s *testDBSuite) TestAlterColumnCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test_charset")
	defer tk.MustExec("drop database test_charset")
	tk.MustExec("use test_charset")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10) charset latin1)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test_charset"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	col := model.FindColumnInfo(t.Meta().Columns, "a")
	c.Assert(col, NotNil)
	c.Assert(col.Charset, Equals, "latin1")
	defCollate, err := charset.GetDefaultCollation("latin1")
	c.Assert(err, IsNil)
	c.Assert(col.Collate, Equals, defCollate)

	_, err = tk.Exec("alter table t modify column a char(10) charset utf8")
	is = domain.GetDomain(ctx).InfoSchema()
	t, err = is.TableByName(model.NewCIStr("test_charset"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	col = model.FindColumnInfo(t.Meta().Columns, "a")
	c.Assert(col, NotNil)
	c.Assert(col.Charset, Equals, "latin1")
	defCollate, err = charset.GetDefaultCollation("latin1")
	c.Assert(err, IsNil)
	c.Assert(col.Collate, Equals, defCollate)

	_, err = tk.Exec("alter table t modify column a char(10) charset utf8 collate utf8_general_ci")
	is = domain.GetDomain(ctx).InfoSchema()
	t, err = is.TableByName(model.NewCIStr("test_charset"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	col = model.FindColumnInfo(t.Meta().Columns, "a")
	c.Assert(col, NotNil)
	c.Assert(col.Charset, Equals, "latin1")
	c.Assert(col.Collate, Equals, "latin1_bin")

	_, err = tk.Exec("alter table t modify column a char(10) charset utf8mb4 collate utf8mb4_general_ci")
	is = domain.GetDomain(ctx).InfoSchema()
	t, err = is.TableByName(model.NewCIStr("test_charset"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	col = model.FindColumnInfo(t.Meta().Columns, "a")
	c.Assert(col, NotNil)
	c.Assert(col.Charset, Equals, "latin1")
	c.Assert(col.Collate, Equals, "latin1_bin")

	rs, err := tk.Exec("alter table t modify column a char(10) charset utf8")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err.Error(), Equals, "[ddl:210]unsupported modify charset from latin1 to utf8")
}

func (s *testDBSuite) TestDropSchemaWithPartitionTable(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("drop database if exists test_db_with_partition")
	s.tk.MustExec("create database test_db_with_partition")
	s.tk.MustExec("use test_db_with_partition")
	s.tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	s.tk.MustExec(`create table t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	s.tk.MustExec("insert into t_part values (1),(2),(11),(12);")
	ctx := s.s.(sessionctx.Context)
	tbl := s.testGetTableByName(c, "test_db_with_partition", "t_part")

	// check records num before drop database.
	recordsNum := getPartitionTableRecordsNum(c, ctx, tbl.(table.PartitionedTable))
	c.Assert(recordsNum, Equals, 4)

	s.tk.MustExec("drop database if exists test_db_with_partition")

	// check job args.
	rs, err := s.tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	rows, err := session.GetRows4Test(context.Background(), s.tk.Se, rs)
	c.Assert(err, IsNil)
	row := rows[0]
	c.Assert(row.GetString(3), Equals, "drop schema")
	jobID := row.GetInt64(0)
	kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(jobID)
		c.Assert(err, IsNil)
		var tableIDs []int64
		err = historyJob.DecodeArgs(&tableIDs)
		c.Assert(err, IsNil)
		// There is 2 partitions.
		c.Assert(len(tableIDs), Equals, 3)
		return nil
	})

	// check records num after drop database.
	for i := 0; i < waitForCleanDataRound; i++ {
		recordsNum = getPartitionTableRecordsNum(c, ctx, tbl.(table.PartitionedTable))
		if recordsNum != 0 {
			time.Sleep(waitForCleanDataInterval)
		} else {
			break
		}
	}
	c.Assert(recordsNum, Equals, 0)
}

func getPartitionTableRecordsNum(c *C, ctx sessionctx.Context, tbl table.PartitionedTable) int {
	num := 0
	info := tbl.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.(table.PartitionedTable).GetPartition(pid)
		startKey := partition.RecordKey(math.MinInt64)
		c.Assert(ctx.NewTxn(), IsNil)
		err := partition.IterRecords(ctx, startKey, partition.Cols(),
			func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
				num++
				return true, nil
			})
		c.Assert(err, IsNil)
	}
	return num
}

func (s *testDBSuite) TestTransactionOnAddDropColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1, t2")
	s.mustExec(c, "create table t1 (a int, b int);")
	s.mustExec(c, "create table t2 (a int, b int);")
	s.mustExec(c, "insert into t2 values (2,0)")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=1",
			"update t1 set b=1 where a=1",
			"commit",
		},
		{
			"begin",
			"insert into t1 select a,b from t2",
			"update t1 set b=2 where a=2",
			"commit",
		},
	}

	originHook := s.dom.DDL().(ddl.DDLForTest).GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		switch job.SchemaState {
		case model.StateWriteOnly, model.StateWriteReorganization, model.StateDeleteOnly, model.StateDeleteReorganization:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, sql := range transaction {
				s.mustExec(c, sql)
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t1 add column c int not null after a", done)
	err := <-done
	c.Assert(err, IsNil)
	s.tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
	s.mustExec(c, "delete from t1")

	// test transaction on drop column.
	go backgroundExec(s.store, "alter table t1 drop column c", done)
	err = <-done
	c.Assert(err, IsNil)
	s.tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
}

func (s *testDBSuite) TestTransactionWithWriteOnlyColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (a int key);")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=1",
			"update t1 set a=2 where a=1",
			"commit",
		},
	}

	originHook := s.dom.DDL().(ddl.DDLForTest).GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		switch job.SchemaState {
		case model.StateWriteOnly:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, sql := range transaction {
				s.mustExec(c, sql)
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t1 add column c int not null", done)
	err := <-done
	c.Assert(err, IsNil)
	s.tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
	s.mustExec(c, "delete from t1")

	// test transaction on drop column.
	go backgroundExec(s.store, "alter table t1 drop column c", done)
	err = <-done
	c.Assert(err, IsNil)
	s.tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
}

func (s *testDBSuite) TestCheckTooBigFieldLength(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists tr_01;")
	s.tk.MustExec("create table tr_01 (id int, name varchar(20000), purchased date )  default charset=utf8 collate=utf8_bin;")

	s.tk.MustExec("drop table if exists tr_02;")
	s.tk.MustExec("create table tr_02 (id int, name varchar(16000), purchased date )  default charset=utf8mb4 collate=utf8mb4_bin;")

	s.tk.MustExec("drop table if exists tr_03;")
	s.tk.MustExec("create table tr_03 (id int, name varchar(65534), purchased date ) default charset=latin1;")

	s.tk.MustExec("drop table if exists tr_03;")
	s.tk.MustExec("create table tr_03 (a varchar(16000) ) default charset utf8;")
	s.tk.MustExec("alter table tr_03 convert to character set utf8mb4;")

	s.tk.MustExec("drop table if exists tr_04;")
	s.tk.MustExec("create table tr_04 (a varchar(20000) ) default charset utf8;")
	s.testErrorCode(c, "alter table tr_04 add column b varchar(20000) charset utf8mb4;", tmysql.ErrTooBigFieldlength)
	s.testErrorCode(c, "alter table tr_04 convert to character set utf8mb4;", tmysql.ErrTooBigFieldlength)
	s.testErrorCode(c, "create table tr_05 (id int, name varchar(30000), purchased date )  default charset=utf8 collate=utf8_bin;", tmysql.ErrTooBigFieldlength)
	s.testErrorCode(c, "create table tr_05 (id int, name varchar(20000) charset utf8mb4, purchased date ) default charset=utf8 collate=utf8_bin;", tmysql.ErrTooBigFieldlength)
	s.testErrorCode(c, "create table tr_05 (id int, name varchar(65536), purchased date ) default charset=latin1;", tmysql.ErrTooBigFieldlength)

	s.tk.MustExec("drop table if exists tr_05;")
	s.tk.MustExec("create table tr_05 (a varchar(10000) ) default charset utf8mb4;")
	s.testErrorCode(c, "alter table tr_05 add column b varchar(20000);", tmysql.ErrTooBigFieldlength)
}

func (s *testDBSuite) TestAddColumn2(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (a int key, b int);")
	defer s.mustExec(c, "drop table if exists t1, t2")

	originHook := s.dom.DDL().(ddl.DDLForTest).GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{}
	var writeOnlyTable table.Table
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			writeOnlyTable, _ = s.dom.InfoSchema().TableByID(job.TableID)
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t1 add column c int not null", done)
	err := <-done
	c.Assert(err, IsNil)

	s.mustExec(c, "insert into t1 values (1,1,1)")
	s.tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 1 1"))

	// mock for outdated tidb update record.
	c.Assert(writeOnlyTable, NotNil)
	ctx := context.Background()
	err = s.tk.Se.NewTxn()
	c.Assert(err, IsNil)
	oldRow, err := writeOnlyTable.RowWithCols(s.tk.Se, 1, writeOnlyTable.WritableCols())
	c.Assert(err, IsNil)
	c.Assert(len(oldRow), Equals, 3)
	err = writeOnlyTable.RemoveRecord(s.tk.Se, 1, oldRow)
	c.Assert(err, IsNil)
	_, err = writeOnlyTable.AddRecord(s.tk.Se, types.MakeDatums(oldRow[0].GetInt64(), 2, oldRow[2].GetInt64()),
		&table.AddRecordOpt{IsUpdate: true})
	c.Assert(err, IsNil)
	err = s.tk.Se.StmtCommit()
	c.Assert(err, IsNil)
	err = s.tk.Se.CommitTxn(ctx)

	s.tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 2 1"))

	// Test for _tidb_rowid
	var re *testkit.Result
	s.mustExec(c, "create table t2 (a int);")
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		// allow write _tidb_rowid first
		s.mustExec(c, "set @@tidb_opt_write_row_id=1")
		s.mustExec(c, "begin")
		s.mustExec(c, "insert into t2 (a,_tidb_rowid) values (1,2);")
		re = s.tk.MustQuery(" select a,_tidb_rowid from t2;")
		s.mustExec(c, "commit")

	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	go backgroundExec(s.store, "alter table t2 add column b int not null default 3", done)
	err = <-done
	c.Assert(err, IsNil)
	re.Check(testkit.Rows("1 2"))
	s.tk.MustQuery("select a,b,_tidb_rowid from t2").Check(testkit.Rows("1 3 2"))
}

func (s *testDBSuite) TestAddIndexForGeneratedColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test_db")
	s.tk.MustExec("create table t(y year NOT NULL DEFAULT '2155')")
	defer s.mustExec(c, "drop table t;")
	for i := 0; i < 50; i++ {
		s.mustExec(c, "insert into t values (?)", i)
	}
	s.tk.MustExec("insert into t values()")
	s.tk.MustExec("ALTER TABLE t ADD COLUMN y1 year as (y + 2)")
	_, err := s.tk.Exec("ALTER TABLE t ADD INDEX idx_y(y1)")
	c.Assert(err.Error(), Equals, "[ddl:15]cannot decode index value, because cannot convert datum from unsigned bigint to type year.")

	t := s.testGetTable(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c2"), IsFalse)
	}
	s.mustExec(c, "delete from t where y = 2155")
	s.mustExec(c, "alter table t add index idx_y(y1)")
	s.mustExec(c, "alter table t drop index idx_y")

	// Fix issue 9311.
	s.tk.MustExec("create table gcai_table (id int primary key);")
	s.tk.MustExec("insert into gcai_table values(1);")
	s.tk.MustExec("ALTER TABLE gcai_table ADD COLUMN d date DEFAULT '9999-12-31';")
	s.tk.MustExec("ALTER TABLE gcai_table ADD COLUMN d1 date as (DATE_SUB(d, INTERVAL 31 DAY));")
	s.tk.MustExec("ALTER TABLE gcai_table ADD INDEX idx(d1);")
	s.tk.MustQuery("select * from gcai_table").Check(testkit.Rows("1 9999-12-31 9999-11-30"))
	s.tk.MustQuery("select d1 from gcai_table use index(idx)").Check(testkit.Rows("9999-11-30"))
	s.tk.MustExec("admin check table gcai_table")
	// The column is PKIsHandle in generated column expression.
	s.tk.MustExec("ALTER TABLE gcai_table ADD COLUMN id1 int as (id+5);")
	s.tk.MustExec("ALTER TABLE gcai_table ADD INDEX idx1(id1);")
	s.tk.MustQuery("select * from gcai_table").Check(testkit.Rows("1 9999-12-31 9999-11-30 6"))
	s.tk.MustQuery("select id1 from gcai_table use index(idx1)").Check(testkit.Rows("6"))
	s.tk.MustExec("admin check table gcai_table")
}

func (s *testDBSuite) TestModifyColumnCharset(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test_db")
	s.tk.MustExec("create table t_mcc(a varchar(8) charset utf8, b varchar(8) charset utf8)")
	defer s.mustExec(c, "drop table t_mcc;")

	result := s.tk.MustQuery(`show create table t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	s.tk.MustExec("alter table t_mcc modify column a varchar(8);")
	t := s.testGetTable(c, "t_mcc")
	t.Meta().Version = model.TableInfoVersion0
	// When the table version is TableInfoVersion0, the following statement don't change "b" charset.
	// So the behavior is not compatible with MySQL.
	s.tk.MustExec("alter table t_mcc modify column b varchar(8);")
	result = s.tk.MustQuery(`show create table t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

}

func (s *testDBSuite) TestCanceledJobTakeTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_cjtt(a int)")

	hook := &ddl.TestDDLCallback{}
	once := sync.Once{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		once.Do(func() {
			err := kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
				t := meta.NewMeta(txn)
				return t.DropTable(job.SchemaID, job.TableID, true)
			})
			c.Assert(err, IsNil)
		})
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	originalWT := ddl.WaitTimeWhenErrorOccured
	ddl.WaitTimeWhenErrorOccured = 1 * time.Second
	defer func() { ddl.WaitTimeWhenErrorOccured = originalWT }()
	startTime := time.Now()
	s.testErrorCode(c, "alter table t_cjtt add column b int", mysql.ErrNoSuchTable)
	sub := time.Since(startTime)
	c.Assert(sub, Less, ddl.WaitTimeWhenErrorOccured)

	hook = &ddl.TestDDLCallback{}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
}

func (s *testDBSuite) TestAlterShardRowIDBits(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	s.tk = testkit.NewTestKit(c, s.store)
	tk := s.tk

	tk.MustExec("use test")
	// Test alter shard_row_id_bits
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 5")
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	tk.MustExec("insert into t1 set a=1;")

	// Test increase shard_row_id_bits failed by overflow global auto ID.
	_, err := tk.Exec("alter table t1 SHARD_ROW_ID_BITS = 10;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:1467]shard_row_id_bits 10 will cause next global auto ID overflow")

	// Test reduce shard_row_id_bits will be ok.
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 3;")
	checkShardRowID := func(maxShardRowIDBits, shardRowIDBits uint64) {
		tbl := testGetTableByName(c, tk.Se, "test", "t1")
		c.Assert(tbl.Meta().MaxShardRowIDBits == maxShardRowIDBits, IsTrue)
		c.Assert(tbl.Meta().ShardRowIDBits == shardRowIDBits, IsTrue)
	}
	checkShardRowID(5, 3)

	// Test reduce shard_row_id_bits but calculate overflow should use the max record shard_row_id_bits.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 10")
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 5;")
	checkShardRowID(10, 5)
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	_, err = tk.Exec("insert into t1 set a=1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:1467]Failed to read auto-increment value from storage engine")
}

func (s *testDBSuite) TestDDLWithInvalidTableInfo(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	tk := s.tk

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_enable_table_partition = 1")
	// Test create with invalid expression.
	_, err := s.tk.Exec(`CREATE TABLE t (
		c0 int(11) ,
  		c1 int(11),
    	c2 decimal(16,4) GENERATED ALWAYS AS ((case when (c0 = 0) then 0when (c0 > 0) then (c1 / c0) end))
	);`)
	c.Assert(err, NotNil)
	//c.Assert(err.Error(), Equals, "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 4 column 88 near \"then (c1 / c0) end))\n\t);\" ")
	c.Assert(err.Error(), Equals, "line 4 column 88 near \" (c1 / c0) end))\n\t);\" (total length 155)")

	tk.MustExec("create table t (a bigint, b int, c int generated always as (b+1)) partition by range (a) (partition p0 values less than (10));")
	// Test drop partition column.
	_, err = tk.Exec("alter table t drop column a;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[expression:1054]Unknown column 'a' in 'expression'")
	// Test modify column with invalid expression.
	_, err = tk.Exec("alter table t modify column c int GENERATED ALWAYS AS ((case when (a = 0) then 0when (a > 0) then (b / a) end));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "line 1 column 97 near \" (b / a) end));\" (total length 112)")
	// Test add column with invalid expression.
	_, err = tk.Exec("alter table t add column d int GENERATED ALWAYS AS ((case when (a = 0) then 0when (a > 0) then (b / a) end));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "line 1 column 94 near \" (b / a) end));\" (total length 109)")
}
