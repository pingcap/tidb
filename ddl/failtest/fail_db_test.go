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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	// TODO: Remove "github.com/pingcap/check" dependency from this file
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/testutil"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	newtestkit "github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	. "github.com/pingcap/tidb/util/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestTCopy(t *testing.T) {
	logLevel := os.Getenv("log_level")
	err := logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		t.Fatal(err)
	}
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	testleak.BeforeTest()
	s := &testFailDBSuite{}
	s.SetUpSuiteCopy(t)
	s.MyTestRunDDLJobPanic(t)
	s.MyTestPartitionAddIndexGC(t)
	s.MyTestModifyColumn(t)
	s.MyTestPartitionAddPanicCopy(t)
	s.TearDownSuiteCopy(t)
	testleak.AfterTestT(t)()
}

func TestT(t *testing.T) {
	// TODO: CustomVerboseFlag seems to come from "github.com/pingcap/check"
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	err := logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		t.Fatal(err)
	}
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	testleak.BeforeTest()
	// TODO: Testing(...) seems to come from "github.com/pingcap/check"
	TestingT(t)
	testleak.AfterTestT(t)()
}

// TODO: SerialSuites(...) seems to come from "github.com/pingcap/check"
var _ = SerialSuites(&testFailDBSuite{})

type testFailDBSuite struct {
	cluster testutils.Cluster
	lease   time.Duration
	store   kv.Storage
	dom     *domain.Domain
	se      session.Session
	p       *parser.Parser

	CommonHandleSuite
}

func (s *testFailDBSuite) SetUpSuiteCopy(t *testing.T) {
	s.lease = 200 * time.Millisecond
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	require.NoError(t, err)
	session.SetSchemaLease(s.lease)
	s.dom, err = session.BootstrapSession(s.store)
	require.NoError(t, err)
	s.se, err = session.CreateSession4Test(s.store)
	require.NoError(t, err)
	s.p = parser.New()
}

// TODO: type C seems to come from "github.com/pingcap/check"
func (s *testFailDBSuite) SetUpSuite(c *C) {
	s.lease = 200 * time.Millisecond
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	s.dom, err = session.BootstrapSession(s.store)
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)
	s.p = parser.New()
}

// TODO: type C seems to come from "github.com/pingcap/check"
func (s *testFailDBSuite) TearDownSuite(c *C) {
	_, err := s.se.Execute(context.Background(), "drop database if exists test_db_state")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)
	s.se.Close()
	s.dom.Close()
	s.store.Close()
}

func (s *testFailDBSuite) TearDownSuiteCopy(t *testing.T) {
	_, err := s.se.Execute(context.Background(), "drop database if exists test_db_state")
	require.NoError(t, err)
	s.se.Close()
	s.dom.Close()
	s.store.Close()
}

// TODO: type C seems to come from "github.com/pingcap/check"
// TestHalfwayCancelOperations tests the case that the schema is correct after the execution of operations are cancelled halfway.
func (s *testFailDBSuite) TestHalfwayCancelOperations(c *C) {
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/truncateTableErr", `return(true)`), IsNil)
	defer func() {
		// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/truncateTableErr"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database cancel_job_db")
	tk.MustExec("use cancel_job_db")

	// test for truncating table
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	_, err := tk.Exec("truncate table t")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, NotNil)

	// Make sure that the table's data has not been deleted.
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	// Execute ddl statement reload schema
	tk.MustExec("alter table t comment 'test1'")
	err = s.dom.DDL().GetHook().OnChanged(nil)
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)

	tk = testkit.NewTestKit(c, s.store)
	tk.MustExec("use cancel_job_db")
	// Test schema is correct.
	tk.MustExec("select * from t")
	// test for renaming table
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/renameTableErr", `return("ty")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/renameTableErr"), IsNil)
	}()
	tk.MustExec("create table tx(a int)")
	tk.MustExec("insert into tx values(1)")
	_, err = tk.Exec("rename table tx to ty")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, NotNil)
	tk.MustExec("create table ty(a int)")
	tk.MustExec("insert into ty values(2)")
	_, err = tk.Exec("rename table ty to tz, tx to ty")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, NotNil)
	_, err = tk.Exec("select * from tz")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, NotNil)
	_, err = tk.Exec("rename table tx to ty, ty to tz")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, NotNil)
	tk.MustQuery("select * from ty").Check(testkit.Rows("2"))
	// Make sure that the table's data has not been deleted.
	tk.MustQuery("select * from tx").Check(testkit.Rows("1"))
	// Execute ddl statement reload schema.
	tk.MustExec("alter table tx comment 'tx'")
	err = s.dom.DDL().GetHook().OnChanged(nil)
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)

	tk = testkit.NewTestKit(c, s.store)
	tk.MustExec("use cancel_job_db")
	tk.MustExec("select * from tx")
	// test for exchanging partition
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/exchangePartitionErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/exchangePartitionErr"), IsNil)
	}()
	tk.MustExec("create table pt(a int) partition by hash (a) partitions 2")
	tk.MustExec("insert into pt values(1), (3), (5)")
	tk.MustExec("create table nt(a int)")
	tk.MustExec("insert into nt values(7)")
	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition=0")
	_, err = tk.Exec("alter table pt exchange partition p1 with table nt")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, NotNil)

	tk.MustQuery("select * from pt").Check(testkit.Rows("1", "3", "5"))
	tk.MustQuery("select * from nt").Check(testkit.Rows("7"))
	// Execute ddl statement reload schema.
	tk.MustExec("alter table pt comment 'pt'")
	err = s.dom.DDL().GetHook().OnChanged(nil)
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)

	tk = testkit.NewTestKit(c, s.store)
	tk.MustExec("use cancel_job_db")
	// Test schema is correct.
	tk.MustExec("select * from pt")

	// clean up
	tk.MustExec("drop database cancel_job_db")
}

// TODO: type C seems to come from "github.com/pingcap/check"
// TestInitializeOffsetAndState tests the case that the column's offset and state don't be initialized in the file of ddl_api.go when
// doing the operation of 'modify column'.
func (s *testFailDBSuite) TestInitializeOffsetAndState(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	defer tk.MustExec("drop table t")

	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState", `return(true)`), IsNil)
	tk.MustExec("ALTER TABLE t MODIFY COLUMN b int FIRST;")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/uninitializedOffsetAndState"), IsNil)
}

// TODO: type C seems to come from "github.com/pingcap/check"
func (s *testFailDBSuite) TestUpdateHandleFailed(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/errorUpdateReorgHandle", `1*return`), IsNil)
	defer func() {
		// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
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

// TODO: type C seems to come from "github.com/pingcap/check"
func (s *testFailDBSuite) TestAddIndexFailed(c *C) {
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockBackfillRunErr", `1*return`), IsNil)
	defer func() {
		// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockBackfillRunErr"), IsNil)
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
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	s.cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)

	tk.MustExec("alter table t add index idx_b(b)")
	tk.MustExec("admin check index t idx_b")
	tk.MustExec("admin check table t")
}

// TODO: type C seems to come from "github.com/pingcap/check"
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
	// TODO: c, Assert and isTrue seem to come from "github.com/pingcap/check"
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsTrue)
	mockSyncer, ok := s.dom.DDL().SchemaSyncer().(*ddl.MockSchemaSyncer)
	// TODO: c, Assert and isTrue seem to come from "github.com/pingcap/check"
	c.Assert(ok, IsTrue)

	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
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

	// TODO: c, Assert and isFalse seem to come from "github.com/pingcap/check"
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsFalse)
	_, err := tk.Exec("insert into t values(1)")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, NotNil)
	// TODO: c, Assert and Equals seem to come from "github.com/pingcap/check"
	c.Assert(err.Error(), Equals, "[domain:8027]Information schema is out of date: schema failed to update in 1 lease, please make sure TiDB can connect to TiKV")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/domain/ErrorMockReloadFailed"), IsNil)
	// wait the schemaValidator is started.
	for i := 0; i < 50; i++ {
		if s.dom.SchemaValidator.IsStarted() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// TODO: c, Assert and isTrue seem to come from "github.com/pingcap/check"
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsTrue)
	_, err = tk.Exec("insert into t values(1)")
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)
}

// TODO: type C seems to come from "github.com/pingcap/check"
func (s *testFailDBSuite) TestGenGlobalIDFail(c *C) {
	defer func() {
		// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
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
			// TODO: c, Assert and NotNil and Commentf seem to come from "github.com/pingcap/check"
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

// TODO: type C seems to come from "github.com/pingcap/check"
func (s *testFailDBSuite) TestAddIndexWorkerNum(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_db")
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists test_add_index")
	if s.IsCommonHandle {
		tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
		tk.MustExec("create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1, c3))")
	} else {
		tk.MustExec("create table test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))")
	}

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
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)

	splitCount := 100
	// Split table to multi region.
	tableStart := tablecodec.GenTableRecordPrefix(tbl.Meta().ID)
	s.cluster.SplitKeys(tableStart, tableStart.PrefixNext(), splitCount)

	err = ddlutil.LoadDDLReorgVars(context.Background(), tk.Se)
	// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
	c.Assert(err, IsNil)
	originDDLAddIndexWorkerCnt := variable.GetDDLReorgWorkerCounter()
	lastSetWorkerCnt := originDDLAddIndexWorkerCnt
	atomic.StoreInt32(&ddl.TestCheckWorkerNumber, lastSetWorkerCnt)
	ddl.TestCheckWorkerNumber = lastSetWorkerCnt
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_worker_cnt=%d", originDDLAddIndexWorkerCnt))

	if !s.IsCommonHandle { // only enable failpoint once
		// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", `return(true)`), IsNil)
		defer func() {
			// TODO: c, Assert and isNil seem to come from "github.com/pingcap/check"
			c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum"), IsNil)
		}()
	}

	testutil.SessionExecInGoroutine(c, s.store, "create index c3_index on test_add_index (c3)", done)
	checkNum := 0

LOOP:
	for {
		select {
		case err = <-done:
			if err == nil {
				break LOOP
			}
			// TODO: c, Assert and isNil and Commentf seem to come from "github.com/pingcap/check"
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ddl.TestCheckWorkerNumCh:
			lastSetWorkerCnt = int32(rand.Intn(8) + 8)
			tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_worker_cnt=%d", lastSetWorkerCnt))
			atomic.StoreInt32(&ddl.TestCheckWorkerNumber, lastSetWorkerCnt)
			checkNum++
		}
	}
	// TODO: c, Assert and Greater seem to come from "github.com/pingcap/check"
	c.Assert(checkNum, Greater, 5)
	tk.MustExec("admin check table test_add_index")
	tk.MustExec("drop table test_add_index")

	s.RerunWithCommonHandleEnabled(c, s.TestAddIndexWorkerNum)
}

// TestRunDDLJobPanic tests recover panic when run ddl job panic.
func (s *testFailDBSuite) MyTestRunDDLJobPanic(t *testing.T) {
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockPanicInRunDDLJob"))
	}()
	tk := newtestkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockPanicInRunDDLJob", `1*panic("panic test")`))
	_, err := tk.Exec("create table t(c1 int, c2 int)")
	require.Error(t, err)
	require.EqualError(t, err, "[ddl:8214]Cancelled DDL job")
}

func (s *testFailDBSuite) MyTestPartitionAddIndexGC(t *testing.T) {
	tk := newtestkit.NewTestKit(t, s.store)
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

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockUpdateCachedSafePoint", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockUpdateCachedSafePoint"))
	}()
	tk.MustExec("alter table partition_add_idx add index idx (id, hired)")
}

func (s *testFailDBSuite) MyTestModifyColumn(t *testing.T) {
	tk := newtestkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")

	tk.MustExec("create table t (a int not null default 1, b int default 2, c int not null default 0, primary key(c), index idx(b), index idx1(a), index idx2(b, c))")
	tk.MustExec("insert into t values(1, 2, 3), (11, 22, 33)")
	_, err := tk.Exec("alter table t change column c cc mediumint")
	require.EqualError(t, err, "[ddl:8200]Unsupported modify column: this column has primary key flag")
	tk.MustExec("alter table t change column b bb mediumint first")
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	cols := tbl.Meta().Columns
	colsStr := ""
	idxsStr := ""
	for _, col := range cols {
		colsStr += col.Name.L + " "
	}
	for _, idx := range tbl.Meta().Indices {
		idxsStr += idx.Name.L + " "
	}
	require.Equal(t, 3, len(cols))
	require.Equal(t, 3, len(tbl.Meta().Indices))
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1 3", "22 11 33"))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `bb` mediumint(9) DEFAULT NULL,\n" +
		"  `a` int(11) NOT NULL DEFAULT '1',\n" +
		"  `c` int(11) NOT NULL DEFAULT '0',\n" +
		"  PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `idx` (`bb`),\n" +
		"  KEY `idx1` (`a`),\n" +
		"  KEY `idx2` (`bb`,`c`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("admin check table t")
	tk.MustExec("insert into t values(111, 222, 333)")
	_, err = tk.Exec("alter table t change column a aa tinyint after c")
	require.EqualError(t, err, "[types:1690]constant 222 overflows tinyint")
	tk.MustExec("alter table t change column a aa mediumint after c")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `bb` mediumint(9) DEFAULT NULL,\n" +
		"  `c` int(11) NOT NULL DEFAULT '0',\n" +
		"  `aa` mediumint(9) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `idx` (`bb`),\n" +
		"  KEY `idx1` (`aa`),\n" +
		"  KEY `idx2` (`bb`,`c`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t").Check(testkit.Rows("2 3 1", "22 33 11", "111 333 222"))
	tk.MustExec("admin check table t")

	// Test unsupport statements.
	tk.MustExec("create table t1(a int) partition by hash (a) partitions 2")
	_, err = tk.Exec("alter table t1 modify column a mediumint")
	require.EqualError(t, err, "[ddl:8200]Unsupported modify column: table is partition table")
	tk.MustExec("create table t2(id int, a int, b int generated always as (abs(a)) virtual, c int generated always as (a+1) stored)")
	_, err = tk.Exec("alter table t2 modify column b mediumint")
	require.EqualError(t, err, "[ddl:8200]Unsupported modify column: newCol IsGenerated false, oldCol IsGenerated true")
	_, err = tk.Exec("alter table t2 modify column c mediumint")
	require.EqualError(t, err, "[ddl:8200]Unsupported modify column: newCol IsGenerated false, oldCol IsGenerated true")
	_, err = tk.Exec("alter table t2 modify column a mediumint generated always as(id+1) stored")
	require.EqualError(t, err, "[ddl:8200]Unsupported modify column: newCol IsGenerated true, oldCol IsGenerated false")
	_, err = tk.Exec("alter table t2 modify column a mediumint")
	require.EqualError(t, err, "[ddl:8200]Unsupported modify column: oldCol is a dependent column 'a' for generated column")

	// Test multiple rows of data.
	tk.MustExec("create table t3(a int not null default 1, b int default 2, c int not null default 0, primary key(c), index idx(b), index idx1(a), index idx2(b, c))")
	// Add some discrete rows.
	maxBatch := 20
	batchCnt := 100
	// Make sure there are no duplicate keys.
	defaultBatchSize := variable.DefTiDBDDLReorgBatchSize * variable.DefTiDBDDLReorgWorkerCount
	base := defaultBatchSize * 20
	for i := 1; i < batchCnt; i++ {
		n := base + i*defaultBatchSize + i
		for j := 0; j < rand.Intn(maxBatch); j++ {
			n += j
			sql := fmt.Sprintf("insert into t3 values (%d, %d, %d)", n, n, n)
			tk.MustExec(sql)
		}
	}
	tk.MustExec("alter table t3 modify column a mediumint")
	tk.MustExec("admin check table t")

	// Test PointGet.
	tk.MustExec("create table t4(a bigint, b int, unique index idx(a));")
	tk.MustExec("insert into t4 values (1,1),(2,2),(3,3),(4,4),(5,5);")
	tk.MustExec("alter table t4 modify a bigint unsigned;")
	tk.MustQuery("select * from t4 where a=1;").Check(testkit.Rows("1 1"))

	// Test changing null to not null.
	tk.MustExec("create table t5(a bigint, b int, unique index idx(a));")
	tk.MustExec("insert into t5 values (1,1),(2,2),(3,3),(4,4),(5,5);")
	tk.MustExec("alter table t5 modify a int not null;")

	tk.MustExec("drop table t, t1, t2, t3, t4, t5")
}

func (s *testFailDBSuite) MyTestPartitionAddPanicCopy(t *testing.T) {
	tk := newtestkit.NewTestKit(t, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a int) partition by range(a) (partition p0 values less than (10));`)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/CheckPartitionByRangeErr", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/CheckPartitionByRangeErr"))
	}()

	_, err := tk.Exec(`alter table t add partition (partition p1 values less than (20));`)
	require.Error(t, err)
	result := tk.MustQuery("show create table t").Rows()[0][1]
	require.Regexp(t, regexp.MustCompile(`(?s).*PARTITION .p0. VALUES LESS THAN \(10\).*`), result)
	require.NotRegexp(t, regexp.MustCompile(`(?s).*PARTITION .p0. VALUES LESS THAN \(20\).*`), result)
}
