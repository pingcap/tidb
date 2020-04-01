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
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/testutil"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

var _ = Suite(&testFailDBSuite{})

type testFailDBSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	lease     time.Duration
	store     kv.Storage
	dom       *domain.Domain
	se        session.Session
	p         *parser.Parser
}

func (s *testFailDBSuite) SetUpSuite(c *C) {
	s.lease = 200 * time.Millisecond
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	var err error
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.p = parser.New()
}

func (s *testFailDBSuite) TearDownSuite(c *C) {
	_, err := s.se.Execute(context.Background(), "drop database if exists test_db_state")
	c.Assert(err, IsNil)
	s.se.Close()
	s.dom.Close()
	s.store.Close()
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
	req := rs[0].NewChunk()
	err = rs[0].Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	row := req.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetInt64(0), DeepEquals, int64(1))
	c.Assert(rs[0].Close(), IsNil)
	// Execute ddl statement reload schema.
	_, err = s.se.Execute(context.Background(), "alter table t comment 'test1'")
	c.Assert(err, IsNil)
	err = s.dom.DDL().GetHook().OnChanged(nil)
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
	req = rs[0].NewChunk()
	err = rs[0].Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(row.GetInt64(0), DeepEquals, int64(1))
	c.Assert(rs[0].Close(), IsNil)
	// Execute ddl statement reload schema.
	_, err = s.se.Execute(context.Background(), "alter table tx comment 'tx'")
	c.Assert(err, IsNil)
	err = s.dom.DDL().GetHook().OnChanged(nil)
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
func (s *testFailDBSuite) TestInitializeOffsetAndState(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	defer tk.MustExec("drop table t")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState", `return(true)`), IsNil)
	tk.MustExec("ALTER TABLE t MODIFY COLUMN b int FIRST;")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState"), IsNil)
}

func (s *testFailDBSuite) TestUpdateHandleFailed(c *C) {
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

func (s *testFailDBSuite) TestAddIndexFailed(c *C) {
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
func (s *testFailDBSuite) TestFailSchemaSyncer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	defer tk.MustExec("drop table if exists t")
	originalRetryTimes := domain.SchemaOutOfDateRetryTimes
	domain.SchemaOutOfDateRetryTimes = 1
	defer func() {
		domain.SchemaOutOfDateRetryTimes = originalRetryTimes
	}()
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsTrue)
	mockSyncer, ok := s.dom.DDL().SchemaSyncer().(*ddl.MockSchemaSyncer)
	c.Assert(ok, IsTrue)

	// make reload failed.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed", `return(true)`), IsNil)
	mockSyncer.CloseSession()
	// wait the schemaValidator is stopped.
	for i := 0; i < 50; i++ {
		if !s.dom.SchemaValidator.IsStarted() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	c.Assert(s.dom.SchemaValidator.IsStarted(), IsFalse)
	_, err := tk.Exec("insert into t values(1)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[domain:8027]Information schema is out of date: schema failed to update in 1 lease, please make sure TiDB can connect to TiKV")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed"), IsNil)
	// wait the schemaValidator is started.
	for i := 0; i < 50; i++ {
		if s.dom.SchemaValidator.IsStarted() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsTrue)
	_, err = tk.Exec("insert into t values(1)")
	c.Assert(err, IsNil)
}

func (s *testFailDBSuite) TestGenGlobalIDFail(c *C) {
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockGenGlobalIDFail"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists gen_global_id_fail")
	tk.MustExec("use gen_global_id_fail")

	sql1 := "create table t1(a bigint PRIMARY KEY, b int)"
	sql2 := `create table t2(a bigint PRIMARY KEY, b int) partition by range (a) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than maxvalue)`
	sql3 := `truncate table t1`
	sql4 := `truncate table t2`

	testcases := []struct {
		sql     string
		table   string
		mockErr bool
	}{
		{sql1, "t1", true},
		{sql2, "t2", true},
		{sql1, "t1", false},
		{sql2, "t2", false},
		{sql3, "t1", true},
		{sql4, "t2", true},
		{sql3, "t1", false},
		{sql4, "t2", false},
	}

	for idx, test := range testcases {
		if test.mockErr {
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockGenGlobalIDFail", `return(true)`), IsNil)
			_, err := tk.Exec(test.sql)
			c.Assert(err, NotNil, Commentf("the %dth test case '%s' fail", idx, test.sql))
		} else {
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockGenGlobalIDFail", `return(false)`), IsNil)
			tk.MustExec(test.sql)
			tk.MustExec(fmt.Sprintf("insert into %s values (%d, 42)", test.table, rand.Intn(65536)))
			tk.MustExec(fmt.Sprintf("admin check table %s", test.table))
		}
	}
	tk.MustExec("admin check table t1")
	tk.MustExec("admin check table t2")
}

func batchInsert(tk *testkit.TestKit, tbl string, start, end int) {
	dml := fmt.Sprintf("insert into %s values", tbl)
	for i := start; i < end; i++ {
		dml += fmt.Sprintf("(%d, %d, %d)", i, i, i)
		if i != end-1 {
			dml += ","
		}
	}
	tk.MustExec(dml)
}

func (s *testFailDBSuite) TestAddIndexWorkerNum(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_db")
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists test_add_index")
	tk.MustExec("create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))")

	done := make(chan error, 1)
	start := -10
	// first add some rows
	for i := start; i < 4090; i += 100 {
		batchInsert(tk, "test_add_index", i, i+100)
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

	testutil.SessionExecInGoroutine(c, s.store, "create index c3_index on test_add_index (c3)", done)
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

// TestRunDDLJobPanic tests recover panic when run ddl job panic.
func (s *testFailDBSuite) TestRunDDLJobPanic(c *C) {
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockPanicInRunDDLJob"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockPanicInRunDDLJob", `1*panic("panic test")`), IsNil)
	_, err := tk.Exec("create table t(c1 int, c2 int)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]current error msg: Cancelled DDL job, original error msg: Cancelled DDL job")
}

func (s *testFailDBSuite) TestPartitionAddIndexGC(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table partition_add_idx (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p5 values less than (2008),
	partition p7 values less than (2018)
	);`)
	tk.MustExec("insert into partition_add_idx values(1, '2010-01-01'), (2, '1990-01-01'), (3, '2001-01-01')")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockUpdateCachedSafePoint", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockUpdateCachedSafePoint"), IsNil)
	}()
	tk.MustExec("alter table partition_add_idx add index idx (id, hired)")
}
