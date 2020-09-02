// Copyright 2018 PingCAP, Inc.
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
	"math/rand"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"golang.org/x/net/context"
)

var _ = Suite(&testFailDBSuite{})

type testFailDBSuite struct {
	lease time.Duration
	store kv.Storage
	dom   *domain.Domain
	se    session.Session
	p     *parser.Parser
}

func (s *testFailDBSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.lease = 200 * time.Millisecond
	ddl.WaitTimeWhenErrorOccured = 1 * time.Microsecond
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.p = parser.New()
}

func (s *testFailDBSuite) TearDownSuite(c *C) {
	s.se.Execute(context.Background(), "drop database if exists test_db_state")
	s.se.Close()
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

// TestHalfwayCancelOperations tests the case that the schema is correct after the execution of operations are cancelled halfway.
func (s *testFailDBSuite) TestHalfwayCancelOperations(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/truncateTableErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/truncateTableErr"), IsNil)
	}()
	// test for truncating table
	_, err := s.se.Execute(context.Background(), "create database cancel_job_db")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use cancel_job_db")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table t(a int)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into t values(1)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "truncate table t")
	c.Assert(err, NotNil)
	// Make sure that the table's data has not been deleted.
	rs, err := s.se.Execute(context.Background(), "select count(*) from t")
	c.Assert(err, IsNil)
	chk := rs[0].NewChunk()
	err = rs[0].Next(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows() == 0, IsFalse)
	row := chk.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetInt64(0), DeepEquals, int64(1))
	c.Assert(rs[0].Close(), IsNil)
	// Execute ddl statement reload schema.
	_, err = s.se.Execute(context.Background(), "alter table t comment 'test1'")
	c.Assert(err, IsNil)
	err = s.dom.DDL().(ddl.DDLForTest).GetHook().OnChanged(nil)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use cancel_job_db")
	c.Assert(err, IsNil)
	// Test schema is correct.
	_, err = s.se.Execute(context.Background(), "select * from t")
	c.Assert(err, IsNil)

	// test for renaming table
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/renameTableErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/renameTableErr"), IsNil)
	}()

	_, err = s.se.Execute(context.Background(), "create table tx(a int)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tx values(1)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "rename table tx to ty")
	c.Assert(err, NotNil)
	// Make sure that the table's data has not been deleted.
	rs, err = s.se.Execute(context.Background(), "select count(*) from tx")
	c.Assert(err, IsNil)
	chk = rs[0].NewChunk()
	err = rs[0].Next(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows() == 0, IsFalse)
	row = chk.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetInt64(0), DeepEquals, int64(1))
	c.Assert(rs[0].Close(), IsNil)
	_, err = s.se.Execute(context.Background(), "alter table tx comment 'tx'")
	c.Assert(err, IsNil)
	err = s.dom.DDL().(ddl.DDLForTest).GetHook().OnChanged(nil)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use cancel_job_db")
	c.Assert(err, IsNil)
	// Test schema is correct.
	_, err = s.se.Execute(context.Background(), "select * from tx")
	c.Assert(err, IsNil)

	// clean up
	_, err = s.se.Execute(context.Background(), "drop database cancel_job_db")
	c.Assert(err, IsNil)
}

// TestInitializeOffsetAndState tests the case that the column's offset and state don't be initialized in the file of ddl_api.go when
// doing the operation of 'modify column'.
func (s *testStateChangeSuite) TestInitializeOffsetAndState(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table t(a int, b int, c int)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState", `return(true)`), IsNil)
	_, err = s.se.Execute(context.Background(), "ALTER TABLE t MODIFY COLUMN b int FIRST;")
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState"), IsNil)
}

func (s *testDBSuite) TestUpdateHandleFailed(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/errorUpdateReorgHandle", `1*return`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/errorUpdateReorgHandle"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_handle_failed")
	defer tk.MustExec("drop database test_handle_failed")
	tk.MustExec("use test_handle_failed")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values(-1, 1)")
	tk.MustExec("alter table t add index idx_b(b)")
	result := tk.MustQuery("select count(*) from t use index(idx_b)")
	result.Check(testkit.Rows("1"))
	tk.MustExec("admin check index t idx_b")
}

func (s *testDBSuite) TestAddIndexFailed(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockAddIndexErr", `1*return`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockAddIndexErr"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_add_index_failed")
	defer tk.MustExec("drop database test_add_index_failed")
	tk.MustExec("use test_add_index_failed")

	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_add_index_failed"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, 100)

	tk.MustExec("alter table t add index idx_b(b)")
	tk.MustExec("admin check index t idx_b")
	tk.MustExec("admin check table t")
}

// TestFailSchemaSyncer test when the schema syncer is done,
// should prohibit DML executing until the syncer is restartd by loadSchemaInLoop.
func (s *testDBSuite) TestFailSchemaSyncer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	defer tk.MustExec("drop table if exists t")
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsTrue)
	mockSyncer, ok := s.dom.DDL().SchemaSyncer().(*ddl.MockSchemaSyncer)
	c.Assert(ok, IsTrue)

	// make reload failed.
	s.dom.MockReloadFailed.SetValue(true)
	mockSyncer.CloseSession()
	// wait the schemaValidator is stopped.
	for i := 0; i < 50; i++ {
		if s.dom.SchemaValidator.IsStarted() == false {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	c.Assert(s.dom.SchemaValidator.IsStarted(), IsFalse)
	_, err := tk.Exec("insert into t values(1)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[domain:1]Information schema is out of date.")
	s.dom.MockReloadFailed.SetValue(false)
	// wait the schemaValidator is started.
	for i := 0; i < 50; i++ {
		if s.dom.SchemaValidator.IsStarted() == true {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsTrue)
	_, err = tk.Exec("insert into t values(1)")
	c.Assert(err, IsNil)
}

func (s *testDBSuite) TestAddIndexWorkerNum(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_db")
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists test_add_index")
	tk.MustExec("create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))")

	done := make(chan error, 1)
	start := -10
	num := 4096
	// first add some rows
	for i := start; i < num; i++ {
		sql := fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", i, i, i)
		tk.MustExec(sql)
	}

	is := s.dom.InfoSchema()
	schemaName := model.NewCIStr("test_db")
	tableName := model.NewCIStr("test_add_index")
	tbl, err := is.TableByName(schemaName, tableName)
	c.Assert(err, IsNil)

	splitCount := 100
	// Split table to multi region.
	s.cluster.SplitTable(s.mvccStore, tbl.Meta().ID, splitCount)

	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	originDDLAddIndexWorkerCnt := variable.GetDDLReorgWorkerCounter()
	lastSetWorkerCnt := originDDLAddIndexWorkerCnt
	atomic.StoreInt32(&ddl.TestCheckWorkerNumber, lastSetWorkerCnt)
	ddl.TestCheckWorkerNumber = lastSetWorkerCnt
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_worker_cnt=%d", originDDLAddIndexWorkerCnt))

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/checkIndexWorkerNum", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/checkIndexWorkerNum"), IsNil)
	}()

	sessionExecInGoroutine(c, s.store, "create index c3_index on test_add_index (c3)", done)
	checkNum := 0

LOOP:
	for {
		select {
		case err = <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ddl.TestCheckWorkerNumCh:
			lastSetWorkerCnt = int32(rand.Intn(8) + 8)
			tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_worker_cnt=%d", lastSetWorkerCnt))
			atomic.StoreInt32(&ddl.TestCheckWorkerNumber, lastSetWorkerCnt)
			checkNum++
		}
	}
	c.Assert(checkNum, Greater, 5)
	tk.MustExec("admin check table test_add_index")
	tk.MustExec("drop table test_add_index")
}

func (s *testDBSuite) TestTruncateTableUpdateSchemaVersionErr(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a int)")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockTruncateTableUpdateVersionError", `return(true)`), IsNil)
	defer c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockTruncateTableUpdateVersionError"), IsNil)
	tk.MustExec("truncate table t")
}
