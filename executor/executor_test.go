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

package executor_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	tikvutil "github.com/pingcap/tidb/store/tikv/util"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	err := logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		t.Fatal(err)
	}
	autoid.SetStep(5000)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowThreshold = 30000 // 30s
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tmpDir := config.GetGlobalConfig().TempStoragePath
	_ = os.RemoveAll(tmpDir) // clean the uncleared temp file during the last run.
	_ = os.MkdirAll(tmpDir, 0755)
	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

var _ = Suite(&testSuite{&baseTestSuite{}})
var _ = Suite(&testSuiteP1{&baseTestSuite{}})
var _ = Suite(&testSuiteP2{&baseTestSuite{}})
var _ = Suite(&testSuite1{})
var _ = SerialSuites(&testSerialSuite2{})
var _ = Suite(&testSuite2{&baseTestSuite{}})
var _ = Suite(&testSuite3{&baseTestSuite{}})
var _ = Suite(&testSuite4{&baseTestSuite{}})
var _ = Suite(&testSuite5{&baseTestSuite{}})
var _ = Suite(&testSuiteJoin1{&baseTestSuite{}})
var _ = Suite(&testSuiteJoin2{&baseTestSuite{}})
var _ = Suite(&testSuiteJoin3{&baseTestSuite{}})
var _ = SerialSuites(&testSuiteJoinSerial{&baseTestSuite{}})
var _ = Suite(&testSuiteAgg{baseTestSuite: &baseTestSuite{}})
var _ = Suite(&testSuite6{&baseTestSuite{}})
var _ = Suite(&testSuite7{&baseTestSuite{}})
var _ = Suite(&testSuite8{&baseTestSuite{}})
var _ = SerialSuites(&testShowStatsSuite{&baseTestSuite{}})
var _ = Suite(&testBypassSuite{})
var _ = Suite(&testUpdateSuite{})
var _ = Suite(&testPointGetSuite{})
var _ = Suite(&testBatchPointGetSuite{})
var _ = SerialSuites(&testRecoverTable{})
var _ = SerialSuites(&testMemTableReaderSuite{&testClusterTableBase{}})
var _ = SerialSuites(&testFlushSuite{})
var _ = SerialSuites(&testAutoRandomSuite{&baseTestSuite{}})
var _ = SerialSuites(&testClusterTableSuite{})
var _ = SerialSuites(&testPrepareSerialSuite{&baseTestSuite{}})
var _ = SerialSuites(&testSplitTable{&baseTestSuite{}})
var _ = Suite(&testSuiteWithData{baseTestSuite: &baseTestSuite{}})
var _ = SerialSuites(&testSerialSuite1{&baseTestSuite{}})
var _ = SerialSuites(&testSlowQuery{&baseTestSuite{}})
var _ = Suite(&partitionTableSuite{&baseTestSuite{}})
var _ = SerialSuites(&tiflashTestSuite{})
var _ = SerialSuites(&globalIndexSuite{&baseTestSuite{}})
var _ = SerialSuites(&testSerialSuite{&baseTestSuite{}})
var _ = SerialSuites(&testStaleTxnSerialSuite{&baseTestSuite{}})
var _ = SerialSuites(&testCoprCache{})
var _ = SerialSuites(&testPrepareSuite{})

type testSuite struct{ *baseTestSuite }
type testSuiteP1 struct{ *baseTestSuite }
type testSuiteP2 struct{ *baseTestSuite }
type testSplitTable struct{ *baseTestSuite }
type testSuiteWithData struct {
	*baseTestSuite
	testData testutil.TestData
}
type testSlowQuery struct{ *baseTestSuite }
type partitionTableSuite struct{ *baseTestSuite }
type globalIndexSuite struct{ *baseTestSuite }
type testSerialSuite struct{ *baseTestSuite }
type testStaleTxnSerialSuite struct{ *baseTestSuite }
type testCoprCache struct {
	store kv.Storage
	dom   *domain.Domain
	cls   cluster.Cluster
}
type testPrepareSuite struct{ testData testutil.TestData }

type baseTestSuite struct {
	cluster cluster.Cluster
	store   kv.Storage
	domain  *domain.Domain
	*parser.Parser
	ctx *mock.Context // nolint:structcheck
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *baseTestSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		store, err := mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c cluster.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.SetSchemaLease(0)
		session.DisableStats4Test()
	}
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionLog
	})
}

func (s *testSuiteWithData) SetUpSuite(c *C) {
	s.baseTestSuite.SetUpSuite(c)
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "executor_suite")
	c.Assert(err, IsNil)
}

func (s *testSuiteWithData) TearDownSuite(c *C) {
	s.baseTestSuite.TearDownSuite(c)
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPrepareSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "prepare_suite")
	c.Assert(err, IsNil)
}

func (s *testPrepareSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *baseTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *globalIndexSuite) SetUpSuite(c *C) {
	s.baseTestSuite.SetUpSuite(c)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
}

func (s *testSuiteP1) TestPessimisticSelectForUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, a int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("begin PESSIMISTIC")
	tk.MustQuery("select a from t where id=1 for update").Check(testkit.Rows("1"))
	tk.MustExec("update t set a=a+1 where id=1")
	tk.MustExec("commit")
	tk.MustQuery("select a from t where id=1").Check(testkit.Rows("2"))
}

func (s *testSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuiteP1) TestBind(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists testbind")

	tk.MustExec("create table testbind(i int, s varchar(20))")
	tk.MustExec("create index index_t on testbind(i,s)")
	tk.MustExec("create global binding for select * from testbind using select * from testbind use index for join(index_t)")
	c.Assert(len(tk.MustQuery("show global bindings").Rows()), Equals, 1)

	tk.MustExec("create session binding for select * from testbind using select * from testbind use index for join(index_t)")
	c.Assert(len(tk.MustQuery("show session bindings").Rows()), Equals, 1)
	tk.MustExec("drop session binding for select * from testbind")
}

func (s *testSuiteP1) TestChange(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("alter table t change a b int")
	tk.MustExec("alter table t change b c bigint")
	c.Assert(tk.ExecToErr("alter table t change c d varchar(100)"), NotNil)
}

func (s *testSuiteP1) TestChangePumpAndDrainer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// change pump or drainer's state need connect to etcd
	// so will meet error "URL scheme must be http, https, unix, or unixs: /tmp/tidb"
	err := tk.ExecToErr("change pump to node_state ='paused' for node_id 'pump1'")
	c.Assert(err, ErrorMatches, "URL scheme must be http, https, unix, or unixs.*")
	err = tk.ExecToErr("change drainer to node_state ='paused' for node_id 'drainer1'")
	c.Assert(err, ErrorMatches, "URL scheme must be http, https, unix, or unixs.*")
}

func (s *testSuiteP1) TestLoadStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	c.Assert(tk.ExecToErr("load stats"), NotNil)
	c.Assert(tk.ExecToErr("load stats ./xxx.json"), NotNil)
}

func (s *testSuiteP1) TestShow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test_show;")
	tk.MustExec("use test_show")

	tk.MustQuery("show engines")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	c.Assert(len(tk.MustQuery("show index in t").Rows()), Equals, 1)
	c.Assert(len(tk.MustQuery("show index from t").Rows()), Equals, 1)

	tk.MustQuery("show charset").Check(testkit.Rows(
		"utf8 UTF-8 Unicode utf8_bin 3",
		"utf8mb4 UTF-8 Unicode utf8mb4_bin 4",
		"ascii US ASCII ascii_bin 1",
		"latin1 Latin1 latin1_bin 1",
		"binary binary binary 1"))
	c.Assert(len(tk.MustQuery("show master status").Rows()), Equals, 1)
	tk.MustQuery("show create database test_show").Check(testkit.Rows("test_show CREATE DATABASE `test_show` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk.MustQuery("show privileges").Check(testkit.Rows("Alter Tables To alter the table",
		"Alter routine Functions,Procedures To alter or drop stored functions/procedures",
		"Create Databases,Tables,Indexes To create new databases and tables",
		"Create routine Databases To use CREATE FUNCTION/PROCEDURE",
		"Create temporary tables Databases To use CREATE TEMPORARY TABLE",
		"Create view Tables To create new views",
		"Create user Server Admin To create new users",
		"Delete Tables To delete existing rows",
		"Drop Databases,Tables To drop databases, tables, and views",
		"Event Server Admin To create, alter, drop and execute events",
		"Execute Functions,Procedures To execute stored routines",
		"File File access on server To read and write files on the server",
		"Grant option Databases,Tables,Functions,Procedures To give to other users those privileges you possess",
		"Index Tables To create or drop indexes",
		"Insert Tables To insert data into tables",
		"Lock tables Databases To use LOCK TABLES (together with SELECT privilege)",
		"Process Server Admin To view the plain text of currently executing queries",
		"Proxy Server Admin To make proxy user possible",
		"References Databases,Tables To have references on tables",
		"Reload Server Admin To reload or refresh tables, logs and privileges",
		"Replication client Server Admin To ask where the slave or master servers are",
		"Replication slave Server Admin To read binary log events from the master",
		"Select Tables To retrieve rows from table",
		"Show databases Server Admin To see all databases with SHOW DATABASES",
		"Show view Tables To see views with SHOW CREATE VIEW",
		"Shutdown Server Admin To shut down the server",
		"Super Server Admin To use KILL thread, SET GLOBAL, CHANGE MASTER, etc.",
		"Trigger Tables To use triggers",
		"Create tablespace Server Admin To create/alter/drop tablespaces",
		"Update Tables To update existing rows",
		"Usage Server Admin No privileges - allow connect only",
		"BACKUP_ADMIN Server Admin ",
		"SYSTEM_VARIABLES_ADMIN Server Admin ",
		"ROLE_ADMIN Server Admin ",
		"CONNECTION_ADMIN Server Admin ",
		"RESTRICTED_TABLES_ADMIN Server Admin ",
		"RESTRICTED_STATUS_ADMIN Server Admin ",
		"RESTRICTED_VARIABLES_ADMIN Server Admin ",
		"RESTRICTED_USER_ADMIN Server Admin ",
	))
	c.Assert(len(tk.MustQuery("show table status").Rows()), Equals, 1)
}

func (s *testSuite3) TestAdmin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("insert admin_test (c1) values (1),(2),(NULL)")

	ctx := context.Background()
	// cancel DDL jobs test
	r, err := tk.Exec("admin cancel ddl jobs 1")
	c.Assert(err, IsNil, Commentf("err %v", err))
	req := r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row := req.GetRow(0)
	c.Assert(row.Len(), Equals, 2)
	c.Assert(row.GetString(0), Equals, "1")
	c.Assert(row.GetString(1), Matches, "*DDL Job:1 not found")

	// show ddl test;
	r, err = tk.Exec("admin show ddl")
	c.Assert(err, IsNil)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 6)
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	ddlInfo, err := admin.GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(row.GetInt64(0), Equals, ddlInfo.SchemaVer)
	// TODO: Pass this test.
	// rowOwnerInfos := strings.Split(row.Data[1].GetString(), ",")
	// ownerInfos := strings.Split(ddlInfo.Owner.String(), ",")
	// c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	serverInfo, err := infosync.GetServerInfoByID(ctx, row.GetString(1))
	c.Assert(err, IsNil)
	c.Assert(row.GetString(2), Equals, serverInfo.IP+":"+
		strconv.FormatUint(uint64(serverInfo.Port), 10))
	c.Assert(row.GetString(3), Equals, "")
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsTrue)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	// show DDL jobs test
	r, err = tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 11)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	historyJobs, err := admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	c.Assert(len(historyJobs), Greater, 1)
	c.Assert(len(row.GetString(1)), Greater, 0)
	c.Assert(err, IsNil)
	c.Assert(row.GetInt64(0), Equals, historyJobs[0].ID)
	c.Assert(err, IsNil)

	r, err = tk.Exec("admin show ddl jobs 20")
	c.Assert(err, IsNil)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 11)
	c.Assert(row.GetInt64(0), Equals, historyJobs[0].ID)
	c.Assert(err, IsNil)

	// show DDL job queries test
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test2")
	tk.MustExec("create table admin_test2 (c1 int, c2 int, c3 int default 1, index (c1))")
	result := tk.MustQuery(`admin show ddl job queries 1, 1, 1`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`admin show ddl job queries 1, 2, 3, 4`)
	result.Check(testkit.Rows())
	historyJobs, err = admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	result = tk.MustQuery(fmt.Sprintf("admin show ddl job queries %d", historyJobs[0].ID))
	result.Check(testkit.Rows(historyJobs[0].Query))
	c.Assert(err, IsNil)

	// check table test
	tk.MustExec("create table admin_test1 (c1 int, c2 int default 1, index (c1))")
	tk.MustExec("insert admin_test1 (c1) values (21),(22)")
	r, err = tk.Exec("admin check table admin_test, admin_test1")
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
	// error table name
	err = tk.ExecToErr("admin check table admin_test_error")
	c.Assert(err, NotNil)
	// different index values
	sctx := tk.Se.(sessionctx.Context)
	dom := domain.GetDomain(sctx)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("admin_test"))
	c.Assert(err, IsNil)
	c.Assert(tb.Indices(), HasLen, 1)
	_, err = tb.Indices()[0].Create(mock.NewContext(), txn, types.MakeDatums(int64(10)), kv.IntHandle(1), nil)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	errAdmin := tk.ExecToErr("admin check table admin_test")
	c.Assert(errAdmin, NotNil)

	if config.CheckTableBeforeDrop {
		err = tk.ExecToErr("drop table admin_test")
		c.Assert(err.Error(), Equals, errAdmin.Error())

		// Drop inconsistency index.
		tk.MustExec("alter table admin_test drop index c1")
		tk.MustExec("admin check table admin_test")
	}
	// checksum table test
	tk.MustExec("create table checksum_with_index (id int, count int, PRIMARY KEY(id), KEY(count))")
	tk.MustExec("create table checksum_without_index (id int, count int, PRIMARY KEY(id))")
	r, err = tk.Exec("admin checksum table checksum_with_index, checksum_without_index")
	c.Assert(err, IsNil)
	res := tk.ResultSetToResult(r, Commentf("admin checksum table"))
	// Mocktikv returns 1 for every table/index scan, then we will xor the checksums of a table.
	// For "checksum_with_index", we have two checksums, so the result will be 1^1 = 0.
	// For "checksum_without_index", we only have one checksum, so the result will be 1.
	res.Sort().Check(testkit.Rows("test checksum_with_index 0 2 2", "test checksum_without_index 1 1 1"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (c2 BOOL, PRIMARY KEY (c2));")
	tk.MustExec("INSERT INTO t1 SET c2 = '0';")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c3 DATETIME NULL DEFAULT '2668-02-03 17:19:31';")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx2 (c3);")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c4 bit(10) default 127;")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx3 (c4);")
	tk.MustExec("admin check table t1;")

	// Test admin show ddl jobs table name after table has been droped.
	tk.MustExec("drop table if exists t1;")
	re := tk.MustQuery("admin show ddl jobs 1")
	rows := re.Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][2], Equals, "t1")

	// Test for reverse scan get history ddl jobs when ddl history jobs queue has multiple regions.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	historyJobs, err = admin.GetHistoryDDLJobs(txn, 20)
	c.Assert(err, IsNil)

	// Split region for history ddl job queues.
	m := meta.NewMeta(txn)
	startKey := meta.DDLJobHistoryKey(m, 0)
	endKey := meta.DDLJobHistoryKey(m, historyJobs[0].ID)
	s.cluster.SplitKeys(startKey, endKey, int(historyJobs[0].ID/5))

	historyJobs2, err := admin.GetHistoryDDLJobs(txn, 20)
	c.Assert(err, IsNil)
	c.Assert(historyJobs, DeepEquals, historyJobs2)
}

func (s *testSuiteP2) TestAdminShowDDLJobs(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_admin_show_ddl_jobs")
	tk.MustExec("use test_admin_show_ddl_jobs")
	tk.MustExec("create table t (a int);")

	re := tk.MustQuery("admin show ddl jobs 1")
	row := re.Rows()[0]
	c.Assert(row[1], Equals, "test_admin_show_ddl_jobs")
	jobID, err := strconv.Atoi(row[0].(string))
	c.Assert(err, IsNil)

	err = kv.RunInNewTxn(context.Background(), s.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		job, err := t.GetHistoryDDLJob(int64(jobID))
		c.Assert(err, IsNil)
		c.Assert(job, NotNil)
		// Test for compatibility. Old TiDB version doesn't have SchemaName field, and the BinlogInfo maybe nil.
		// See PR: 11561.
		job.BinlogInfo = nil
		job.SchemaName = ""
		err = t.AddHistoryDDLJob(job, true)
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	re = tk.MustQuery("admin show ddl jobs 1")
	row = re.Rows()[0]
	c.Assert(row[1], Equals, "test_admin_show_ddl_jobs")

	re = tk.MustQuery("admin show ddl jobs 1 where job_type='create table'")
	row = re.Rows()[0]
	c.Assert(row[1], Equals, "test_admin_show_ddl_jobs")
	c.Assert(row[9], Equals, "<nil>")

	// Test the START_TIME and END_TIME field.
	re = tk.MustQuery("admin show ddl jobs where job_type = 'create table' and start_time > str_to_date('20190101','%Y%m%d%H%i%s')")
	row = re.Rows()[0]
	c.Assert(row[2], Equals, "t")
	c.Assert(row[9], Equals, "<nil>")
}

func (s *testSuiteP2) TestAdminChecksumOfPartitionedTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS admin_checksum_partition_test;")
	tk.MustExec("CREATE TABLE admin_checksum_partition_test (a INT) PARTITION BY HASH(a) PARTITIONS 4;")
	tk.MustExec("INSERT INTO admin_checksum_partition_test VALUES (1), (2);")

	r := tk.MustQuery("ADMIN CHECKSUM TABLE admin_checksum_partition_test;")
	r.Check(testkit.Rows("test admin_checksum_partition_test 1 5 5"))
}

func (s *baseTestSuite) fillData(tk *testkit.TestKit, table string) {
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("create table %s(id int not null default 1, name varchar(255), PRIMARY KEY(id));", table))

	// insert data
	tk.MustExec(fmt.Sprintf("insert INTO %s VALUES (1, \"hello\");", table))
	tk.CheckExecResult(1, 0)
	tk.MustExec(fmt.Sprintf("insert into %s values (2, \"hello\");", table))
	tk.CheckExecResult(1, 0)
}

type testCase struct {
	data1       []byte
	data2       []byte
	expected    []string
	restData    []byte
	expectedMsg string
}

func checkCases(tests []testCase, ld *executor.LoadDataInfo,
	c *C, tk *testkit.TestKit, ctx sessionctx.Context, selectSQL, deleteSQL string) {
	origin := ld.IgnoreLines
	for _, tt := range tests {
		ld.IgnoreLines = origin
		c.Assert(ctx.NewTxn(context.Background()), IsNil)
		ctx.GetSessionVars().StmtCtx.DupKeyAsWarning = true
		ctx.GetSessionVars().StmtCtx.BadNullAsWarning = true
		ctx.GetSessionVars().StmtCtx.InLoadDataStmt = true
		ctx.GetSessionVars().StmtCtx.InDeleteStmt = false
		data, reachLimit, err1 := ld.InsertData(context.Background(), tt.data1, tt.data2)
		c.Assert(err1, IsNil)
		c.Assert(reachLimit, IsFalse)
		err1 = ld.CheckAndInsertOneBatch(context.Background(), ld.GetRows(), ld.GetCurBatchCnt())
		c.Assert(err1, IsNil)
		ld.SetMaxRowsInBatch(20000)
		if tt.restData == nil {
			c.Assert(data, HasLen, 0,
				Commentf("data1:%v, data2:%v, data:%v", string(tt.data1), string(tt.data2), string(data)))
		} else {
			c.Assert(data, DeepEquals, tt.restData,
				Commentf("data1:%v, data2:%v, data:%v", string(tt.data1), string(tt.data2), string(data)))
		}
		ld.SetMessage()
		tk.CheckLastMessage(tt.expectedMsg)
		ctx.StmtCommit()
		txn, err := ctx.Txn(true)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		r := tk.MustQuery(selectSQL)
		r.Check(testutil.RowsWithSep("|", tt.expected...))
		tk.MustExec(deleteSQL)
	}
}

func (s *testSuiteP1) TestSelectWithoutFrom(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	r := tk.MustQuery("select 1 + 2*3;")
	r.Check(testkit.Rows("7"))

	r = tk.MustQuery(`select _utf8"string";`)
	r.Check(testkit.Rows("string"))

	r = tk.MustQuery("select 1 order by 1;")
	r.Check(testkit.Rows("1"))
}

// TestSelectBackslashN Issue 3685.
func (s *testSuiteP1) TestSelectBackslashN(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select \N;`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "NULL")
	c.Assert(rs.Close(), IsNil)

	sql = `select "\N";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)
	c.Assert(rs.Close(), IsNil)

	tk.MustExec("use test;")
	tk.MustExec("create table test (`\\N` int);")
	tk.MustExec("insert into test values (1);")
	tk.CheckExecResult(1, 0)
	sql = "select * from test;"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `\N`)
	c.Assert(rs.Close(), IsNil)

	sql = `select \N from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)
	c.Assert(rs.Close(), IsNil)

	sql = `select (\N) from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)
	c.Assert(rs.Close(), IsNil)

	sql = "select `\\N` from test;"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `\N`)
	c.Assert(rs.Close(), IsNil)

	sql = "select (`\\N`) from test;"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `\N`)
	c.Assert(rs.Close(), IsNil)

	sql = `select '\N' from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)
	c.Assert(rs.Close(), IsNil)

	sql = `select ('\N') from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)
	c.Assert(rs.Close(), IsNil)
}

// TestSelectNull Issue #4053.
func (s *testSuiteP1) TestSelectNull(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select nUll;`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)
	c.Assert(rs.Close(), IsNil)

	sql = `select (null);`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)
	c.Assert(rs.Close(), IsNil)

	sql = `select null+NULL;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `null+NULL`)
	c.Assert(rs.Close(), IsNil)
}

// TestSelectStringLiteral Issue #3686.
func (s *testSuiteP1) TestSelectStringLiteral(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select 'abc';`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("abc"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `abc`)
	c.Assert(rs.Close(), IsNil)

	sql = `select (('abc'));`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("abc"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `abc`)
	c.Assert(rs.Close(), IsNil)

	sql = `select 'abc'+'def';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("0"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `'abc'+'def'`)
	c.Assert(rs.Close(), IsNil)

	// Below checks whether leading invalid chars are trimmed.
	sql = "select '\n';"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("\n"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "")
	c.Assert(rs.Close(), IsNil)

	sql = "select '\t   col';" // Lowercased letter is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "col")
	c.Assert(rs.Close(), IsNil)

	sql = "select '\t   Col';" // Uppercased letter is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "Col")
	c.Assert(rs.Close(), IsNil)

	sql = "select '\n\t   中文 col';" // Chinese char is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "中文 col")
	c.Assert(rs.Close(), IsNil)

	sql = "select ' \r\n  .col';" // Punctuation is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, ".col")
	c.Assert(rs.Close(), IsNil)

	sql = "select '   😆col';" // Emoji is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "😆col")
	c.Assert(rs.Close(), IsNil)

	// Below checks whether trailing invalid chars are preserved.
	sql = `select 'abc   ';`
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "abc   ")
	c.Assert(rs.Close(), IsNil)

	sql = `select '  abc   123   ';`
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "abc   123   ")
	c.Assert(rs.Close(), IsNil)

	// Issue #4239.
	sql = `select 'a' ' ' 'string';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("a string"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "a")
	c.Assert(rs.Close(), IsNil)

	sql = `select 'a' " " "string";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("a string"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "a")
	c.Assert(rs.Close(), IsNil)

	sql = `select 'string' 'string';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("stringstring"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "string")
	c.Assert(rs.Close(), IsNil)

	sql = `select "ss" "a";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")
	c.Assert(rs.Close(), IsNil)

	sql = `select "ss" "a" "b";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssab"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")
	c.Assert(rs.Close(), IsNil)

	sql = `select "ss" "a" ' ' "b";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa b"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")
	c.Assert(rs.Close(), IsNil)

	sql = `select "ss" "a" ' ' "b" ' ' "d";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa b d"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")
	c.Assert(rs.Close(), IsNil)
}

func (s *testSuiteP1) TestSelectLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_limit")

	tk.MustExec("insert INTO select_limit VALUES (3, \"hello\");")
	tk.CheckExecResult(1, 0)
	tk.MustExec("insert INTO select_limit VALUES (4, \"hello\");")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("select * from select_limit limit 1;")
	r.Check(testkit.Rows("1 hello"))

	r = tk.MustQuery("select id from (select * from select_limit limit 1) k where id != 1;")
	r.Check(testkit.Rows())

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 0;")
	r.Check(testkit.Rows("1 hello", "2 hello", "3 hello", "4 hello"))

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 1;")
	r.Check(testkit.Rows("2 hello", "3 hello", "4 hello"))

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 3;")
	r.Check(testkit.Rows("4 hello"))

	err := tk.ExecToErr("select * from select_limit limit 18446744073709551616 offset 3;")
	c.Assert(err, NotNil)
}

func (s *testSuiteP1) TestSelectOrderBy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_order_test")

	// Test star field
	r := tk.MustQuery("select * from select_order_test where id = 1 order by id limit 1 offset 0;")
	r.Check(testkit.Rows("1 hello"))

	r = tk.MustQuery("select id from select_order_test order by id desc limit 1 ")
	r.Check(testkit.Rows("2"))

	r = tk.MustQuery("select id from select_order_test order by id + 1 desc limit 1 ")
	r.Check(testkit.Rows("2"))

	// Test limit
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 0;")
	r.Check(testkit.Rows("1 hello"))

	// Test limit
	r = tk.MustQuery("select id as c1, name from select_order_test order by 2, id limit 1 offset 0;")
	r.Check(testkit.Rows("1 hello"))

	// Test limit overflow
	r = tk.MustQuery("select * from select_order_test order by name, id limit 100 offset 0;")
	r.Check(testkit.Rows("1 hello", "2 hello"))

	// Test offset overflow
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 100;")
	r.Check(testkit.Rows())

	// Test limit exceeds int range.
	r = tk.MustQuery("select id from select_order_test order by name, id limit 18446744073709551615;")
	r.Check(testkit.Rows("1", "2"))

	// Test multiple field
	r = tk.MustQuery("select id, name from select_order_test where id = 1 group by id, name limit 1 offset 0;")
	r.Check(testkit.Rows("1 hello"))

	// Test limit + order by
	for i := 3; i <= 10; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"zz\");", i))
	}
	tk.MustExec("insert INTO select_order_test VALUES (10086, \"hi\");")
	for i := 11; i <= 20; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"hh\");", i))
	}
	for i := 21; i <= 30; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"zz\");", i))
	}
	tk.MustExec("insert INTO select_order_test VALUES (1501, \"aa\");")
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 3;")
	r.Check(testkit.Rows("11 hh"))
	tk.MustExec("drop table select_order_test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (1, 3)")
	r = tk.MustQuery("select 1-d as d from t order by d;")
	r.Check(testkit.Rows("-2", "-1", "0"))
	r = tk.MustQuery("select 1-d as d from t order by d + 1;")
	r.Check(testkit.Rows("0", "-1", "-2"))
	r = tk.MustQuery("select t.d from t order by d;")
	r.Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert t values (1, 2, 3)")
	r = tk.MustQuery("select b from (select a,b from t order by a,c) t")
	r.Check(testkit.Rows("2"))
	r = tk.MustQuery("select b from (select a,b from t order by a,c limit 1) t")
	r.Check(testkit.Rows("2"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("insert into t values(1, 1), (2, 2)")
	tk.MustQuery("select * from t where 1 order by b").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select * from t where a between 1 and 2 order by a desc").Check(testkit.Rows("2 2", "1 1"))

	// Test double read and topN is pushed down to first read plannercore.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values(1, 3, 1)")
	tk.MustExec("insert into t values(2, 2, 2)")
	tk.MustExec("insert into t values(3, 1, 3)")
	tk.MustQuery("select * from t use index(idx) order by a desc limit 1").Check(testkit.Rows("3 1 3"))

	// Test double read which needs to keep order.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key b (b))")
	tk.Se.GetSessionVars().IndexLookupSize = 3
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d, %d)", i, 10-i))
	}
	tk.MustQuery("select a from t use index(b) order by b").Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"))
}

func (s *testSuiteP1) TestOrderBy(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int, c3 varchar(20))")
	tk.MustExec("insert into t values (1, 2, 'abc'), (2, 1, 'bcd')")

	// Fix issue https://github.com/pingcap/tidb/issues/337
	tk.MustQuery("select c1 as a, c1 as b from t order by c1").Check(testkit.Rows("1 1", "2 2"))

	tk.MustQuery("select c1 as a, t.c1 as a from t order by a desc").Check(testkit.Rows("2 2", "1 1"))
	tk.MustQuery("select c1 as c2 from t order by c2").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select sum(c1) from t order by sum(c1)").Check(testkit.Rows("3"))
	tk.MustQuery("select c1 as c2 from t order by c2 + 1").Check(testkit.Rows("2", "1"))

	// Order by position.
	tk.MustQuery("select * from t order by 1").Check(testkit.Rows("1 2 abc", "2 1 bcd"))
	tk.MustQuery("select * from t order by 2").Check(testkit.Rows("2 1 bcd", "1 2 abc"))

	// Order by binary.
	tk.MustQuery("select c1, c3 from t order by binary c1 desc").Check(testkit.Rows("2 bcd", "1 abc"))
	tk.MustQuery("select c1, c2 from t order by binary c3").Check(testkit.Rows("1 2", "2 1"))
}

func (s *testSuiteP1) TestSelectErrorRow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	err := tk.ExecToErr("select row(1, 1) from test")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test group by row(1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test order by row(1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test having row(1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select (select 1, 1) from test;")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test group by (select 1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test order by (select 1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test having (select 1, 1);")
	c.Assert(err, NotNil)
}

// TestIssue2612 is related with https://github.com/pingcap/tidb/issues/2612
func (s *testSuiteP1) TestIssue2612(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (
		create_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00',
		finish_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00');`)
	tk.MustExec(`insert into t values ('2016-02-13 15:32:24',  '2016-02-11 17:23:22');`)
	rs, err := tk.Exec(`select timediff(finish_at, create_at) from t;`)
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(req.GetRow(0).GetDuration(0, 0).String(), Equals, "-46:09:02")
	c.Assert(rs.Close(), IsNil)
}

// TestIssue345 is related with https://github.com/pingcap/tidb/issues/345
func (s *testSuiteP1) TestIssue345(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1 (c1 int);`)
	tk.MustExec(`create table t2 (c2 int);`)
	tk.MustExec(`insert into t1 values (1);`)
	tk.MustExec(`insert into t2 values (2);`)
	tk.MustExec(`update t1, t2 set t1.c1 = 2, t2.c2 = 1;`)
	tk.MustExec(`update t1, t2 set c1 = 2, c2 = 1;`)
	tk.MustExec(`update t1 as a, t2 as b set a.c1 = 2, b.c2 = 1;`)

	// Check t1 content
	r := tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Rows("2"))
	// Check t2 content
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Rows("1"))

	tk.MustExec(`update t1 as a, t2 as t1 set a.c1 = 1, t1.c2 = 2;`)
	// Check t1 content
	r = tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Rows("1"))
	// Check t2 content
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Rows("2"))

	_, err := tk.Exec(`update t1 as a, t2 set t1.c1 = 10;`)
	c.Assert(err, NotNil)
}

func (s *testSuiteP1) TestIssue5055(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1 (a int);`)
	tk.MustExec(`create table t2 (a int);`)
	tk.MustExec(`insert into t1 values(1);`)
	tk.MustExec(`insert into t2 values(1);`)
	result := tk.MustQuery("select tbl1.* from (select t1.a, 1 from t1) tbl1 left join t2 tbl2 on tbl1.a = tbl2.a order by tbl1.a desc limit 1;")
	result.Check(testkit.Rows("1 1"))
}

func (s *testSuiteWithData) TestSetOperation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1, t2, t3`)
	tk.MustExec(`create table t1(a int)`)
	tk.MustExec(`create table t2 like t1`)
	tk.MustExec(`create table t3 like t1`)
	tk.MustExec(`insert into t1 values (1),(1),(2),(3),(null)`)
	tk.MustExec(`insert into t2 values (1),(2),(null),(null)`)
	tk.MustExec(`insert into t3 values (2),(3)`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testSuiteWithData) TestSetOperationOnDiffColType(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1, t2, t3`)
	tk.MustExec(`create table t1(a int, b int)`)
	tk.MustExec(`create table t2(a int, b varchar(20))`)
	tk.MustExec(`create table t3(a int, b decimal(30,10))`)
	tk.MustExec(`insert into t1 values (1,1),(1,1),(2,2),(3,3),(null,null)`)
	tk.MustExec(`insert into t2 values (1,'1'),(2,'2'),(null,null),(null,'3')`)
	tk.MustExec(`insert into t3 values (2,2.1),(3,3)`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

// issue-23038: wrong key range of index scan for year column
func (s *testSuiteWithData) TestIndexScanWithYearCol(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 year(4), c2 int, key(c1));")
	tk.MustExec("insert into t values(2001, 1);")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testSuiteP2) TestUnion(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	testSQL := `drop table if exists union_test; create table union_test(id int);`
	tk.MustExec(testSQL)

	testSQL = `drop table if exists union_test;`
	tk.MustExec(testSQL)
	testSQL = `create table union_test(id int);`
	tk.MustExec(testSQL)
	testSQL = `insert union_test values (1),(2)`
	tk.MustExec(testSQL)

	testSQL = `select * from (select id from union_test union select id from union_test) t order by id;`
	r := tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1", "2"))

	r = tk.MustQuery("select 1 union all select 1")
	r.Check(testkit.Rows("1", "1"))

	r = tk.MustQuery("select 1 union all select 1 union select 1")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select 1 as a union (select 2) order by a limit 1")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select 1 as a union (select 2) order by a limit 1, 1")
	r.Check(testkit.Rows("2"))

	r = tk.MustQuery("select id from union_test union all (select 1) order by id desc")
	r.Check(testkit.Rows("2", "1", "1"))

	r = tk.MustQuery("select id as a from union_test union (select 1) order by a desc")
	r.Check(testkit.Rows("2", "1"))

	r = tk.MustQuery(`select null as a union (select "abc") order by a`)
	r.Check(testkit.Rows("<nil>", "abc"))

	r = tk.MustQuery(`select "abc" as a union (select 1) order by a`)
	r.Check(testkit.Rows("1", "abc"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c int, d int)")
	tk.MustExec("insert t1 values (NULL, 1)")
	tk.MustExec("insert t1 values (1, 1)")
	tk.MustExec("insert t1 values (1, 2)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (c int, d int)")
	tk.MustExec("insert t2 values (1, 3)")
	tk.MustExec("insert t2 values (1, 1)")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (c int, d int)")
	tk.MustExec("insert t3 values (3, 2)")
	tk.MustExec("insert t3 values (4, 3)")
	r = tk.MustQuery(`select sum(c1), c2 from (select c c1, d c2 from t1 union all select d c1, c c2 from t2 union all select c c1, d c2 from t3) x group by c2 order by c2`)
	r.Check(testkit.Rows("5 1", "4 2", "4 3"))

	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1 (a int primary key)")
	tk.MustExec("create table t2 (a int primary key)")
	tk.MustExec("create table t3 (a int primary key)")
	tk.MustExec("insert t1 values (7), (8)")
	tk.MustExec("insert t2 values (1), (9)")
	tk.MustExec("insert t3 values (2), (3)")
	r = tk.MustQuery("select * from t1 union all select * from t2 union all (select * from t3) order by a limit 2")
	r.Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert t1 values (2), (1)")
	tk.MustExec("insert t2 values (3), (4)")
	r = tk.MustQuery("select * from t1 union all (select * from t2) order by a limit 1")
	r.Check(testkit.Rows("1"))
	r = tk.MustQuery("select (select * from t1 where a != t.a union all (select * from t2 where a != t.a) order by a limit 1) from t1 t")
	r.Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int unsigned primary key auto_increment, c1 int, c2 int, index c1_c2 (c1, c2))")
	tk.MustExec("insert into t (c1, c2) values (1, 1)")
	tk.MustExec("insert into t (c1, c2) values (1, 2)")
	tk.MustExec("insert into t (c1, c2) values (2, 3)")
	r = tk.MustQuery("select * from (select * from t where t.c1 = 1 union select * from t where t.id = 1) s order by s.id")
	r.Check(testkit.Rows("1 1 1", "2 1 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (f1 DATE)")
	tk.MustExec("INSERT INTO t VALUES ('1978-11-26')")
	r = tk.MustQuery("SELECT f1+0 FROM t UNION SELECT f1+0 FROM t")
	r.Check(testkit.Rows("19781126"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b int)")
	tk.MustExec("INSERT INTO t VALUES ('1', '1')")
	r = tk.MustQuery("select b from (SELECT * FROM t UNION ALL SELECT a, b FROM t order by a) t")
	r.Check(testkit.Rows("1", "1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a DECIMAL(4,2))")
	tk.MustExec("INSERT INTO t VALUE(12.34)")
	r = tk.MustQuery("SELECT 1 AS c UNION select a FROM t")
	r.Sort().Check(testkit.Rows("1.00", "12.34"))

	// #issue3771
	r = tk.MustQuery("SELECT 'a' UNION SELECT CONCAT('a', -4)")
	r.Sort().Check(testkit.Rows("a", "a-4"))

	// test race
	tk.MustQuery("SELECT @x:=0 UNION ALL SELECT @x:=0 UNION ALL SELECT @x")

	// test field tp
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("CREATE TABLE t1 (a date)")
	tk.MustExec("CREATE TABLE t2 (a date)")
	tk.MustExec("SELECT a from t1 UNION select a FROM t2")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" + "  `a` date DEFAULT NULL\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Move from session test.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c double);")
	tk.MustExec("create table t2 (c double);")
	tk.MustExec("insert into t1 value (73);")
	tk.MustExec("insert into t2 value (930);")
	// If set unspecified column flen to 0, it will cause bug in union.
	// This test is used to prevent the bug reappear.
	tk.MustQuery("select c from t1 union (select c from t2) order by c").Check(testkit.Rows("73", "930"))

	// issue 5703
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a date)")
	tk.MustExec("insert into t value ('2017-01-01'), ('2017-01-02')")
	r = tk.MustQuery("(select a from t where a < 0) union (select a from t where a > 0) order by a")
	r.Check(testkit.Rows("2017-01-01", "2017-01-02"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t value(0),(0)")
	tk.MustQuery("select 1 from (select a from t union all select a from t) tmp").Check(testkit.Rows("1", "1", "1", "1"))
	tk.MustQuery("select 10 as a from dual union select a from t order by a desc limit 1 ").Check(testkit.Rows("10"))
	tk.MustQuery("select -10 as a from dual union select a from t order by a limit 1 ").Check(testkit.Rows("-10"))
	tk.MustQuery("select count(1) from (select a from t union all select a from t) tmp").Check(testkit.Rows("4"))

	err := tk.ExecToErr("select 1 from (select a from t limit 1 union all select a from t limit 1) tmp")
	c.Assert(err, NotNil)
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(mysql.ErrWrongUsage))

	err = tk.ExecToErr("select 1 from (select a from t order by a union all select a from t limit 1) tmp")
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(mysql.ErrWrongUsage))

	_, err = tk.Exec("(select a from t order by a) union all select a from t limit 1 union all select a from t limit 1")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrWrongUsage), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("(select a from t limit 1) union all select a from t limit 1")
	c.Assert(err, IsNil)
	_, err = tk.Exec("(select a from t order by a) union all select a from t order by a")
	c.Assert(err, IsNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t value(1),(2),(3)")

	tk.MustQuery("(select a from t order by a limit 2) union all (select a from t order by a desc limit 2) order by a desc limit 1,2").Check(testkit.Rows("2", "2"))
	tk.MustQuery("select a from t union all select a from t order by a desc limit 5").Check(testkit.Rows("3", "3", "2", "2", "1"))
	tk.MustQuery("(select a from t order by a desc limit 2) union all select a from t group by a order by a").Check(testkit.Rows("1", "2", "2", "3", "3"))
	tk.MustQuery("(select a from t order by a desc limit 2) union all select 33 as a order by a desc limit 2").Check(testkit.Rows("33", "3"))

	tk.MustQuery("select 1 union select 1 union all select 1").Check(testkit.Rows("1", "1"))
	tk.MustQuery("select 1 union all select 1 union select 1").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`create table t1(a bigint, b bigint);`)
	tk.MustExec(`create table t2(a bigint, b bigint);`)
	tk.MustExec(`insert into t1 values(1, 1);`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t2 values(1, 1);`)
	tk.MustExec(`set @@tidb_init_chunk_size=2;`)
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustQuery(`select count(*) from (select t1.a, t1.b from t1 left join t2 on t1.a=t2.a union all select t1.a, t1.a from t1 left join t2 on t1.a=t2.a) tmp;`).Check(testkit.Rows("128"))
	tk.MustQuery(`select tmp.a, count(*) from (select t1.a, t1.b from t1 left join t2 on t1.a=t2.a union all select t1.a, t1.a from t1 left join t2 on t1.a=t2.a) tmp;`).Check(testkit.Rows("1 128"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t value(1 ,2)")
	tk.MustQuery("select a, b from (select a, 0 as d, b from t union all select a, 0 as d, b from t) test;").Check(testkit.Rows("1 2", "1 2"))

	// #issue 8141
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t1 value(1,2),(1,1),(2,2),(2,2),(3,2),(3,2)")
	tk.MustExec("set @@tidb_init_chunk_size=2;")
	tk.MustQuery("select count(*) from (select a as c, a as d from t1 union all select a, b from t1) t;").Check(testkit.Rows("12"))

	// #issue 8189 and #issue 8199
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("CREATE TABLE t1 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustExec("CREATE TABLE t2 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t2 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustQuery("select a from t1 union select a from t1 order by (select a+1);").Check(testkit.Rows("1", "2", "3"))

	// #issue 8201
	for i := 0; i < 4; i++ {
		tk.MustQuery("SELECT(SELECT 0 AS a FROM dual UNION SELECT 1 AS a FROM dual ORDER BY a ASC  LIMIT 1) AS dev").Check(testkit.Rows("0"))
	}

	// #issue 8231
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t1 (uid int(1))")
	tk.MustExec("INSERT INTO t1 SELECT 150")
	tk.MustQuery("SELECT 'a' UNION SELECT uid FROM t1 order by 1 desc;").Check(testkit.Rows("a", "150"))

	// #issue 8196
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("CREATE TABLE t1 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustExec("CREATE TABLE t2 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t2 values(3,'c'),(4,'d'),(5,'f'),(6,'e')")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	_, err = tk.Exec("(select a,b from t1 limit 2) union all (select a,b from t2 order by a limit 1) order by t1.b")
	c.Assert(err.Error(), Equals, "[planner:1250]Table 't1' from one of the SELECTs cannot be used in global ORDER clause")

	// #issue 9900
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b decimal(6, 3))")
	tk.MustExec("insert into t values(1, 1.000)")
	tk.MustQuery("select count(distinct a), sum(distinct a), avg(distinct a) from (select a from t union all select b from t) tmp;").Check(testkit.Rows("1 1.000 1.0000000"))

	// #issue 23832
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bit(20), b float, c double, d int)")
	tk.MustExec("insert into t values(10, 10, 10, 10), (1, -1, 2, -2), (2, -2, 1, 1), (2, 1.1, 2.1, 10.1)")
	tk.MustQuery("select a from t union select 10 order by a").Check(testkit.Rows("1", "2", "10"))
}

func (s *testSuite2) TestUnionLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists union_limit")
	tk.MustExec("create table union_limit (id int) partition by hash(id) partitions 30")
	for i := 0; i < 60; i++ {
		tk.MustExec(fmt.Sprintf("insert into union_limit values (%d)", i))
	}
	// Cover the code for worker count limit in the union executor.
	tk.MustQuery("select * from union_limit limit 10")
}

func (s *testSuiteP1) TestNeighbouringProj(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 value(1, 1), (2, 2)")
	tk.MustExec("insert into t2 value(1, 1), (2, 2)")
	tk.MustQuery("select sum(c) from (select t1.a as a, t1.a as c, length(t1.b) from t1  union select a, b, b from t2) t;").Check(testkit.Rows("5"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint, b bigint, c bigint);")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3);")
	rs := tk.MustQuery("select cast(count(a) as signed), a as another, a from t group by a order by cast(count(a) as signed), a limit 10;")
	rs.Check(testkit.Rows("1 1 1", "1 2 2", "1 3 3"))
}

func (s *testSuiteP1) TestIn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (c1 int primary key, c2 int, key c (c2));`)
	for i := 0; i <= 200; i++ {
		tk.MustExec(fmt.Sprintf("insert t values(%d, %d)", i, i))
	}
	queryStr := `select c2 from t where c1 in ('7', '10', '112', '111', '98', '106', '100', '9', '18', '17') order by c2`
	r := tk.MustQuery(queryStr)
	r.Check(testkit.Rows("7", "9", "10", "17", "18", "98", "100", "106", "111", "112"))

	queryStr = `select c2 from t where c1 in ('7a')`
	tk.MustQuery(queryStr).Check(testkit.Rows("7"))
}

func (s *testSuiteP1) TestTablePKisHandleScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int PRIMARY KEY AUTO_INCREMENT)")
	tk.MustExec("insert t values (),()")
	tk.MustExec("insert t values (-100),(0)")

	tests := []struct {
		sql    string
		result [][]interface{}
	}{
		{
			"select * from t",
			testkit.Rows("-100", "1", "2", "3"),
		},
		{
			"select * from t where a = 1",
			testkit.Rows("1"),
		},
		{
			"select * from t where a != 1",
			testkit.Rows("-100", "2", "3"),
		},
		{
			"select * from t where a >= '1.1'",
			testkit.Rows("2", "3"),
		},
		{
			"select * from t where a < '1.1'",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a > '-100.1' and a < 2",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a is null",
			testkit.Rows(),
		}, {
			"select * from t where a is true",
			testkit.Rows("-100", "1", "2", "3"),
		}, {
			"select * from t where a is false",
			testkit.Rows(),
		},
		{
			"select * from t where a in (1, 2)",
			testkit.Rows("1", "2"),
		},
		{
			"select * from t where a between 1 and 2",
			testkit.Rows("1", "2"),
		},
	}

	for _, tt := range tests {
		result := tk.MustQuery(tt.sql)
		result.Check(tt.result)
	}
}

func (s *testSuite8) TestIndexScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique)")
	tk.MustExec("insert t values (-1), (2), (3), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select a from t where a < 0 or (a >= 2.1 and a < 5.1) or ( a > 5.9 and a <= 7.9) or a > '8.1'")
	result.Check(testkit.Rows("-1", "3", "5", "6", "7", "9"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique)")
	tk.MustExec("insert t values (0)")
	result = tk.MustQuery("select NULL from t ")
	result.Check(testkit.Rows("<nil>"))
	// test for double read
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique, b int)")
	tk.MustExec("insert t values (5, 0)")
	tk.MustExec("insert t values (4, 0)")
	tk.MustExec("insert t values (3, 0)")
	tk.MustExec("insert t values (2, 0)")
	tk.MustExec("insert t values (1, 0)")
	tk.MustExec("insert t values (0, 0)")
	result = tk.MustQuery("select * from t order by a limit 3")
	result.Check(testkit.Rows("0 0", "1 0", "2 0"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique, b int)")
	tk.MustExec("insert t values (0, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (2, 1)")
	tk.MustExec("insert t values (3, 2)")
	tk.MustExec("insert t values (4, 1)")
	tk.MustExec("insert t values (5, 2)")
	result = tk.MustQuery("select * from t where a < 5 and b = 1 limit 2")
	result.Check(testkit.Rows("0 1", "2 1"))
	tk.MustExec("drop table if exists tab1")
	tk.MustExec("CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col3 INTEGER, col4 FLOAT)")
	tk.MustExec("CREATE INDEX idx_tab1_0 on tab1 (col0)")
	tk.MustExec("CREATE INDEX idx_tab1_1 on tab1 (col1)")
	tk.MustExec("CREATE INDEX idx_tab1_3 on tab1 (col3)")
	tk.MustExec("CREATE INDEX idx_tab1_4 on tab1 (col4)")
	tk.MustExec("INSERT INTO tab1 VALUES(1,37,20.85,30,10.69)")
	result = tk.MustQuery("SELECT pk FROM tab1 WHERE ((col3 <= 6 OR col3 < 29 AND (col0 < 41)) OR col3 > 42) AND col1 >= 96.1 AND col3 = 30 AND col3 > 17 AND (col0 BETWEEN 36 AND 42)")
	result.Check(testkit.Rows())
	tk.MustExec("drop table if exists tab1")
	tk.MustExec("CREATE TABLE tab1(pk INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	tk.MustExec("CREATE INDEX idx_tab1_0 on tab1 (a)")
	tk.MustExec("INSERT INTO tab1 VALUES(1,1,1)")
	tk.MustExec("INSERT INTO tab1 VALUES(2,2,1)")
	tk.MustExec("INSERT INTO tab1 VALUES(3,1,2)")
	tk.MustExec("INSERT INTO tab1 VALUES(4,2,2)")
	result = tk.MustQuery("SELECT * FROM tab1 WHERE pk <= 3 AND a = 1")
	result.Check(testkit.Rows("1 1 1", "3 1 2"))
	result = tk.MustQuery("SELECT * FROM tab1 WHERE pk <= 4 AND a = 1 AND b = 2")
	result.Check(testkit.Rows("3 1 2"))
	tk.MustExec("CREATE INDEX idx_tab1_1 on tab1 (b, a)")
	result = tk.MustQuery("SELECT pk FROM tab1 WHERE b > 1")
	result.Check(testkit.Rows("3", "4"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a varchar(3), index(a))")
	tk.MustExec("insert t values('aaa'), ('aab')")
	result = tk.MustQuery("select * from t where a >= 'aaaa' and a < 'aabb'")
	result.Check(testkit.Rows("aab"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int primary key, b int, c int, index(c))")
	tk.MustExec("insert t values(1, 1, 1), (2, 2, 2), (4, 4, 4), (3, 3, 3), (5, 5, 5)")
	// Test for double read and top n.
	result = tk.MustQuery("select a from t where c >= 2 order by b desc limit 1")
	result.Check(testkit.Rows("5"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(50) primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values('aa', 1, 1)")
	tk.MustQuery("select * from t use index(idx) where a > 'a'").Check(testkit.Rows("aa 1 1"))

	// fix issue9636
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (a int, KEY (a))")
	result = tk.MustQuery(`SELECT * FROM (SELECT * FROM (SELECT a as d FROM t WHERE a IN ('100')) AS x WHERE x.d < "123" ) tmp_count`)
	result.Check(testkit.Rows())
}

func (s *testSuiteP1) TestIndexReverseOrder(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int, index idx (b))")
	tk.MustExec("insert t (b) values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select b from t order by b desc")
	result.Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"))
	result = tk.MustQuery("select b from t where b <3 or (b >=6 and b < 8) order by b desc")
	result.Check(testkit.Rows("7", "6", "2", "1", "0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx (b, a))")
	tk.MustExec("insert t values (0, 2), (1, 2), (2, 2), (0, 1), (1, 1), (2, 1), (0, 0), (1, 0), (2, 0)")
	result = tk.MustQuery("select b, a from t order by b, a desc")
	result.Check(testkit.Rows("0 2", "0 1", "0 0", "1 2", "1 1", "1 0", "2 2", "2 1", "2 0"))
}

func (s *testSuiteP1) TestTableReverseOrder(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int)")
	tk.MustExec("insert t (b) values (1), (2), (3), (4), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select b from t order by a desc")
	result.Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1"))
	result = tk.MustQuery("select a from t where a <3 or (a >=6 and a < 8) order by a desc")
	result.Check(testkit.Rows("7", "6", "2", "1"))
}

func (s *testSuiteP1) TestDefaultNull(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int default 1, c int)")
	tk.MustExec("insert t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("update t set b = NULL where a = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustExec("update t set c = 1")
	tk.MustQuery("select * from t ").Check(testkit.Rows("1 <nil> 1"))
	tk.MustExec("delete from t where a = 1")
	tk.MustExec("insert t (a) values (1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 <nil>"))
}

func (s *testSuiteP1) TestUnsignedPKColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unsigned primary key, b int, c int, key idx_ba (b, c, a));")
	tk.MustExec("insert t values (1, 1, 1)")
	result := tk.MustQuery("select * from t;")
	result.Check(testkit.Rows("1 1 1"))
	tk.MustExec("update t set c=2 where a=1;")
	result = tk.MustQuery("select * from t where b=1;")
	result.Check(testkit.Rows("1 1 2"))
}

func (s *testSuiteP1) TestJSON(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_json")
	tk.MustExec("create table test_json (id int, a json)")
	tk.MustExec(`insert into test_json (id, a) values (1, '{"a":[1,"2",{"aa":"bb"},4],"b":true}')`)
	tk.MustExec(`insert into test_json (id, a) values (2, "null")`)
	tk.MustExec(`insert into test_json (id, a) values (3, null)`)
	tk.MustExec(`insert into test_json (id, a) values (4, 'true')`)
	tk.MustExec(`insert into test_json (id, a) values (5, '3')`)
	tk.MustExec(`insert into test_json (id, a) values (5, '4.0')`)
	tk.MustExec(`insert into test_json (id, a) values (6, '"string"')`)

	result := tk.MustQuery(`select tj.a from test_json tj order by tj.id`)
	result.Check(testkit.Rows(`{"a": [1, "2", {"aa": "bb"}, 4], "b": true}`, "null", "<nil>", "true", "3", "4", `"string"`))

	// Check json_type function
	result = tk.MustQuery(`select json_type(a) from test_json tj order by tj.id`)
	result.Check(testkit.Rows("OBJECT", "NULL", "<nil>", "BOOLEAN", "INTEGER", "DOUBLE", "STRING"))

	// Check json compare with primitives.
	result = tk.MustQuery(`select a from test_json tj where a = 3`)
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery(`select a from test_json tj where a = 4.0`)
	result.Check(testkit.Rows("4"))
	result = tk.MustQuery(`select a from test_json tj where a = true`)
	result.Check(testkit.Rows("true"))
	result = tk.MustQuery(`select a from test_json tj where a = "string"`)
	result.Check(testkit.Rows(`"string"`))

	// Check cast(true/false as JSON).
	result = tk.MustQuery(`select cast(true as JSON)`)
	result.Check(testkit.Rows(`true`))
	result = tk.MustQuery(`select cast(false as JSON)`)
	result.Check(testkit.Rows(`false`))

	// Check two json grammar sugar.
	result = tk.MustQuery(`select a->>'$.a[2].aa' as x, a->'$.b' as y from test_json having x is not null order by id`)
	result.Check(testkit.Rows(`bb true`))
	result = tk.MustQuery(`select a->'$.a[2].aa' as x, a->>'$.b' as y from test_json having x is not null order by id`)
	result.Check(testkit.Rows(`"bb" true`))

	// Check some DDL limits for TEXT/BLOB/JSON column.
	var err error
	var terr *terror.Error

	_, err = tk.Exec(`create table test_bad_json(a json default '{}')`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(mysql.ErrBlobCantHaveDefault))

	_, err = tk.Exec(`create table test_bad_json(a blob default 'hello')`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(mysql.ErrBlobCantHaveDefault))

	_, err = tk.Exec(`create table test_bad_json(a text default 'world')`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(mysql.ErrBlobCantHaveDefault))

	// check json fields cannot be used as key.
	_, err = tk.Exec(`create table test_bad_json(id int, a json, key (a))`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(mysql.ErrJSONUsedAsKey))

	// check CAST AS JSON.
	result = tk.MustQuery(`select CAST('3' AS JSON), CAST('{}' AS JSON), CAST(null AS JSON)`)
	result.Check(testkit.Rows(`3 {} <nil>`))

	tk.MustQuery("select a, count(1) from test_json group by a order by a").Check(testkit.Rows(
		"<nil> 1",
		"null 1",
		"3 1",
		"4 1",
		`"string" 1`,
		"{\"a\": [1, \"2\", {\"aa\": \"bb\"}, 4], \"b\": true} 1",
		"true 1"))

	// Check cast json to decimal.
	// NOTE: this test case contains a bug, it should be uncommented after the bug is fixed.
	// TODO: Fix bug https://github.com/pingcap/tidb/issues/12178
	// tk.MustExec("drop table if exists test_json")
	// tk.MustExec("create table test_json ( a decimal(60,2) as (JSON_EXTRACT(b,'$.c')), b json );")
	// tk.MustExec(`insert into test_json (b) values
	//	('{"c": "1267.1"}'),
	//	('{"c": "1267.01"}'),
	//	('{"c": "1267.1234"}'),
	//	('{"c": "1267.3456"}'),
	//	('{"c": "1234567890123456789012345678901234567890123456789012345"}'),
	//	('{"c": "1234567890123456789012345678901234567890123456789012345.12345"}');`)
	//
	// tk.MustQuery("select a from test_json;").Check(testkit.Rows("1267.10", "1267.01", "1267.12",
	//	"1267.35", "1234567890123456789012345678901234567890123456789012345.00",
	//	"1234567890123456789012345678901234567890123456789012345.12"))
}

func (s *testSuiteP1) TestMultiUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_mu (a int primary key, b int, c int)`)
	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)`)

	// Test INSERT ... ON DUPLICATE UPDATE set_lists.
	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE b = 3, c = b`)
	result := tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 3 3`, `4 5 6`, `7 8 9`))

	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE c = 2, b = c+5`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 5 6`, `7 8 9`))

	// Test UPDATE ... set_lists.
	tk.MustExec(`UPDATE test_mu SET b = 0, c = b WHERE a = 4`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 0 5`, `7 8 9`))

	tk.MustExec(`UPDATE test_mu SET c = 8, b = c WHERE a = 4`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 5 8`, `7 8 9`))

	tk.MustExec(`UPDATE test_mu SET c = b, b = c WHERE a = 7`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 5 8`, `7 9 8`))
}

func (s *testSuiteP1) TestGeneratedColumnWrite(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	_, err := tk.Exec(`CREATE TABLE test_gc_write (a int primary key auto_increment, b int, c int as (a+8) virtual)`)
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs("c").Error())
	tk.MustExec(`CREATE TABLE test_gc_write (a int primary key auto_increment, b int, c int as (b+8) virtual)`)
	tk.MustExec(`CREATE TABLE test_gc_write_1 (a int primary key, b int, c int)`)

	tests := []struct {
		stmt string
		err  int
	}{
		// Can't modify generated column by values.
		{`insert into test_gc_write (a, b, c) values (1, 1, 1)`, mysql.ErrBadGeneratedColumn},
		{`insert into test_gc_write values (1, 1, 1)`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by select clause.
		{`insert into test_gc_write select 1, 1, 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by on duplicate clause.
		{`insert into test_gc_write (a, b) values (1, 1) on duplicate key update c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by set.
		{`insert into test_gc_write set a = 1, b = 1, c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by update clause.
		{`update test_gc_write set c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by multi-table update clause.
		{`update test_gc_write, test_gc_write_1 set test_gc_write.c = 1`, mysql.ErrBadGeneratedColumn},

		// Can insert without generated columns.
		{`insert into test_gc_write (a, b) values (1, 1)`, 0},
		{`insert into test_gc_write set a = 2, b = 2`, 0},
		{`insert into test_gc_write (b) select c from test_gc_write`, 0},
		// Can update without generated columns.
		{`update test_gc_write set b = 2 where a = 2`, 0},
		{`update test_gc_write t1, test_gc_write_1 t2 set t1.b = 3, t2.b = 4`, 0},

		// But now we can't do this, just as same with MySQL 5.7:
		{`insert into test_gc_write values (1, 1)`, mysql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write select 1, 1`, mysql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write (c) select a, b from test_gc_write`, mysql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write (b, c) select a, b from test_gc_write`, mysql.ErrBadGeneratedColumn},
	}
	for _, tt := range tests {
		_, err := tk.Exec(tt.stmt)
		if tt.err != 0 {
			c.Assert(err, NotNil, Commentf("sql is `%v`", tt.stmt))
			terr := errors.Cause(err).(*terror.Error)
			c.Assert(terr.Code(), Equals, errors.ErrCode(tt.err), Commentf("sql is %v", tt.stmt))
		} else {
			c.Assert(err, IsNil)
		}
	}
}

// TestGeneratedColumnRead tests select generated columns from table.
// They should be calculated from their generation expressions.
func (s *testSuiteP1) TestGeneratedColumnRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_gc_read(a int primary key, b int, c int as (a+b), d int as (a*b) stored, e int as (c*2))`)

	result := tk.MustQuery(`SELECT generation_expression FROM information_schema.columns WHERE table_name = 'test_gc_read' AND column_name = 'd'`)
	result.Check(testkit.Rows("`a` * `b`"))

	// Insert only column a and b, leave c and d be calculated from them.
	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (0,null),(1,2),(3,4)`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`))

	tk.MustExec(`INSERT INTO test_gc_read SET a = 5, b = 10`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 10 15 50 30`))

	tk.MustExec(`REPLACE INTO test_gc_read (a, b) VALUES (5, 6)`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 6 11 30 22`))

	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (5, 8) ON DUPLICATE KEY UPDATE b = 9`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 9 14 45 28`))

	// Test select only-generated-column-without-dependences.
	result = tk.MustQuery(`SELECT c, d FROM test_gc_read`)
	result.Check(testkit.Rows(`<nil> <nil>`, `3 2`, `7 12`, `14 45`))

	// Test select only virtual generated column that refers to other virtual generated columns.
	result = tk.MustQuery(`SELECT e FROM test_gc_read`)
	result.Check(testkit.Rows(`<nil>`, `6`, `14`, `28`))

	// Test order of on duplicate key update list.
	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (5, 8) ON DUPLICATE KEY UPDATE a = 6, b = a`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `6 6 12 36 24`))

	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (6, 8) ON DUPLICATE KEY UPDATE b = 8, a = b`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	// Test where-conditions on virtual/stored generated columns.
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 7`)
	result.Check(testkit.Rows(`3 4 7 12 14`))

	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE d = 64`)
	result.Check(testkit.Rows(`8 8 16 64 32`))

	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE e = 6`)
	result.Check(testkit.Rows(`1 2 3 2 6`))

	// Test update where-conditions on virtual/generated columns.
	tk.MustExec(`UPDATE test_gc_read SET a = a + 100 WHERE c = 7`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 107`)
	result.Check(testkit.Rows(`103 4 107 412 214`))

	// Test update where-conditions on virtual/generated columns.
	tk.MustExec(`UPDATE test_gc_read m SET m.a = m.a + 100 WHERE c = 107`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 207`)
	result.Check(testkit.Rows(`203 4 207 812 414`))

	tk.MustExec(`UPDATE test_gc_read SET a = a - 200 WHERE d = 812`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE d = 12`)
	result.Check(testkit.Rows(`3 4 7 12 14`))

	tk.MustExec(`INSERT INTO test_gc_read set a = 4, b = d + 1`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`,
		`4 <nil> <nil> <nil> <nil>`, `8 8 16 64 32`))
	tk.MustExec(`DELETE FROM test_gc_read where a = 4`)

	// Test on-conditions on virtual/stored generated columns.
	tk.MustExec(`CREATE TABLE test_gc_help(a int primary key, b int, c int, d int, e int)`)
	tk.MustExec(`INSERT INTO test_gc_help(a, b, c, d, e) SELECT * FROM test_gc_read`)

	result = tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.c = t2.c ORDER BY t1.a`)
	result.Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	result = tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.d = t2.d ORDER BY t1.a`)
	result.Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	result = tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.e = t2.e ORDER BY t1.a`)
	result.Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	// Test generated column in subqueries.
	result = tk.MustQuery(`SELECT * FROM test_gc_read t WHERE t.a not in (SELECT t.a FROM test_gc_read t where t.c > 5)`)
	result.Sort().Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`))

	result = tk.MustQuery(`SELECT * FROM test_gc_read t WHERE t.c in (SELECT t.c FROM test_gc_read t where t.c > 5)`)
	result.Sort().Check(testkit.Rows(`3 4 7 12 14`, `8 8 16 64 32`))

	result = tk.MustQuery(`SELECT tt.b FROM test_gc_read tt WHERE tt.a = (SELECT max(t.a) FROM test_gc_read t WHERE t.c = tt.c) ORDER BY b`)
	result.Check(testkit.Rows(`2`, `4`, `8`))

	// Test aggregation on virtual/stored generated columns.
	result = tk.MustQuery(`SELECT c, sum(a) aa, max(d) dd, sum(e) ee FROM test_gc_read GROUP BY c ORDER BY aa`)
	result.Check(testkit.Rows(`<nil> 0 <nil> <nil>`, `3 1 2 6`, `7 3 12 14`, `16 8 64 32`))

	result = tk.MustQuery(`SELECT a, sum(c), sum(d), sum(e) FROM test_gc_read GROUP BY a ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 3 2 6`, `3 7 12 14`, `8 16 64 32`))

	// Test multi-update on generated columns.
	tk.MustExec(`UPDATE test_gc_read m, test_gc_read n SET m.b = m.b + 10, n.b = n.b + 10`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 12 13 12 26`, `3 14 17 42 34`, `8 18 26 144 52`))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(8)")
	tk.MustExec("update test_gc_read set a = a+1 where a in (select a from t)")
	result = tk.MustQuery("select * from test_gc_read order by a")
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 12 13 12 26`, `3 14 17 42 34`, `9 18 27 162 54`))

	// Test different types between generation expression and generated column.
	tk.MustExec(`CREATE TABLE test_gc_read_cast(a VARCHAR(255), b VARCHAR(255), c INT AS (JSON_EXTRACT(a, b)), d INT AS (JSON_EXTRACT(a, b)) STORED)`)
	tk.MustExec(`INSERT INTO test_gc_read_cast (a, b) VALUES ('{"a": "3"}', '$.a')`)
	result = tk.MustQuery(`SELECT c, d FROM test_gc_read_cast`)
	result.Check(testkit.Rows(`3 3`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_1(a VARCHAR(255), b VARCHAR(255), c ENUM("red", "yellow") AS (JSON_UNQUOTE(JSON_EXTRACT(a, b))))`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_1 (a, b) VALUES ('{"a": "yellow"}', '$.a')`)
	result = tk.MustQuery(`SELECT c FROM test_gc_read_cast_1`)
	result.Check(testkit.Rows(`yellow`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_2( a JSON, b JSON AS (a->>'$.a'))`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_2(a) VALUES ('{"a": "{    \\\"key\\\": \\\"\\u6d4b\\\"    }"}')`)
	result = tk.MustQuery(`SELECT b FROM test_gc_read_cast_2`)
	result.Check(testkit.Rows(`{"key": "测"}`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_3( a JSON, b JSON AS (a->>'$.a'), c INT AS (b * 3.14) )`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_3(a) VALUES ('{"a": "5"}')`)
	result = tk.MustQuery(`SELECT c FROM test_gc_read_cast_3`)
	result.Check(testkit.Rows(`16`))

	_, err := tk.Exec(`INSERT INTO test_gc_read_cast_1 (a, b) VALUES ('{"a": "invalid"}', '$.a')`)
	c.Assert(err, NotNil)

	// Test read generated columns after drop some irrelevant column
	tk.MustExec(`DROP TABLE IF EXISTS test_gc_read_m`)
	tk.MustExec(`CREATE TABLE test_gc_read_m (a int primary key, b int, c int as (a+1), d int as (c*2))`)
	tk.MustExec(`INSERT INTO test_gc_read_m(a) values (1), (2)`)
	tk.MustExec(`ALTER TABLE test_gc_read_m DROP b`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read_m`)
	result.Check(testkit.Rows(`1 2 4`, `2 3 6`))

	// Test not null generated columns.
	tk.MustExec(`CREATE TABLE test_gc_read_1(a int primary key, b int, c int as (a+b) not null, d int as (a*b) stored)`)
	tk.MustExec(`CREATE TABLE test_gc_read_2(a int primary key, b int, c int as (a+b), d int as (a*b) stored not null)`)
	tests := []struct {
		stmt string
		err  int
	}{
		// Can't insert these records, because generated columns are not null.
		{`insert into test_gc_read_1(a, b) values (1, null)`, mysql.ErrBadNull},
		{`insert into test_gc_read_2(a, b) values (1, null)`, mysql.ErrBadNull},
	}
	for _, tt := range tests {
		_, err := tk.Exec(tt.stmt)
		if tt.err != 0 {
			c.Assert(err, NotNil)
			terr := errors.Cause(err).(*terror.Error)
			c.Assert(terr.Code(), Equals, errors.ErrCode(tt.err))
		} else {
			c.Assert(err, IsNil)
		}
	}
}

// TestGeneratedColumnRead tests generated columns using point get and batch point get
func (s *testSuiteP1) TestGeneratedColumnPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tu")
	tk.MustExec("CREATE TABLE tu(a int, b int, c int GENERATED ALWAYS AS (a + b) VIRTUAL, d int as (a * b) stored, " +
		"e int GENERATED ALWAYS as (b * 2) VIRTUAL, PRIMARY KEY (a), UNIQUE KEY ukc (c), unique key ukd(d), key ke(e))")
	tk.MustExec("insert into tu(a, b) values(1, 2)")
	tk.MustExec("insert into tu(a, b) values(5, 6)")
	tk.MustQuery("select * from tu for update").Check(testkit.Rows("1 2 3 2 4", "5 6 11 30 12"))
	tk.MustQuery("select * from tu where a = 1").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where a in (1, 2)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where c in (1, 2, 3)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where c = 3").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select d, e from tu where c = 3").Check(testkit.Rows("2 4"))
	tk.MustQuery("select * from tu where d in (1, 2, 3)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where d = 2").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select c, d from tu where d = 2").Check(testkit.Rows("3 2"))
	tk.MustQuery("select d, e from tu where e = 4").Check(testkit.Rows("2 4"))
	tk.MustQuery("select * from tu where e = 4").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustExec("update tu set a = a + 1, b = b + 1 where c = 11")
	tk.MustQuery("select * from tu for update").Check(testkit.Rows("1 2 3 2 4", "6 7 13 42 14"))
	tk.MustQuery("select * from tu where a = 6").Check(testkit.Rows("6 7 13 42 14"))
	tk.MustQuery("select * from tu where c in (5, 6, 13)").Check(testkit.Rows("6 7 13 42 14"))
	tk.MustQuery("select b, c, e, d from tu where c = 13").Check(testkit.Rows("7 13 14 42"))
	tk.MustQuery("select a, e, d from tu where c in (5, 6, 13)").Check(testkit.Rows("6 14 42"))
	tk.MustExec("drop table if exists tu")
}

func (s *testSuiteP2) TestToPBExpr(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a decimal(10,6), b decimal, index idx_b (b))")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values (1.1, 1.1)")
	tk.MustExec("insert t values (2.4, 2.4)")
	tk.MustExec("insert t values (3.3, 2.7)")
	result := tk.MustQuery("select * from t where a < 2.399999")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where a > 1.5")
	result.Check(testkit.Rows("2.400000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where a <= 1.1")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where b >= 3")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where not (b = 1)")
	result.Check(testkit.Rows("2.400000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where b&1 = a|1")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where b != 2 and b <=> 3")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where b in (3)")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where b not in (1, 2)")
	result.Check(testkit.Rows("3.300000 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(255), b int)")
	tk.MustExec("insert t values ('abc123', 1)")
	tk.MustExec("insert t values ('ab123', 2)")
	result = tk.MustQuery("select * from t where a like 'ab%'")
	result.Check(testkit.Rows("abc123 1", "ab123 2"))
	result = tk.MustQuery("select * from t where a like 'ab_12'")
	result.Check(nil)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	tk.MustExec("insert t values (1)")
	tk.MustExec("insert t values (2)")
	result = tk.MustQuery("select * from t where not (a = 1)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select * from t where not(not (a = 1))")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t where not(a != 1 and a != 2)")
	result.Check(testkit.Rows("1", "2"))
}

func (s *testSuiteP2) TestDatumXAPI(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a decimal(10,6), b decimal, index idx_b (b))")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values (1.1, 1.1)")
	tk.MustExec("insert t values (2.2, 2.2)")
	tk.MustExec("insert t values (3.3, 2.7)")
	result := tk.MustQuery("select * from t where a > 1.5")
	result.Check(testkit.Rows("2.200000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where b > 1.5")
	result.Check(testkit.Rows("2.200000 2", "3.300000 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a time(3), b time, index idx_a (a))")
	tk.MustExec("insert t values ('11:11:11', '11:11:11')")
	tk.MustExec("insert t values ('11:11:12', '11:11:12')")
	tk.MustExec("insert t values ('11:11:13', '11:11:13')")
	result = tk.MustQuery("select * from t where a > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12.000 11:11:12", "11:11:13.000 11:11:13"))
	result = tk.MustQuery("select * from t where b > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12.000 11:11:12", "11:11:13.000 11:11:13"))
}

func (s *testSuiteP2) TestSQLMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a tinyint not null)")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	_, err := tk.Exec("insert t values ()")
	c.Check(err, NotNil)

	_, err = tk.Exec("insert t values ('1000')")
	c.Check(err, NotNil)

	tk.MustExec("create table if not exists tdouble (a double(3,2))")
	_, err = tk.Exec("insert tdouble values (10.23)")
	c.Check(err, NotNil)

	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values ()")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value"))
	tk.MustExec("insert t values (null)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null"))
	tk.MustExec("insert ignore t values (null)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null"))
	tk.MustExec("insert t select null")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null"))
	tk.MustExec("insert t values (1000)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("0", "0", "0", "0", "127"))

	tk.MustExec("insert tdouble values (10.23)")
	tk.MustQuery("select * from tdouble").Check(testkit.Rows("9.99"))

	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	tk.MustExec("set @@global.sql_mode = ''")

	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")
	tk2.MustExec("drop table if exists t2")
	tk2.MustExec("create table t2 (a varchar(3))")
	tk2.MustExec("insert t2 values ('abcd')")
	tk2.MustQuery("select * from t2").Check(testkit.Rows("abc"))

	// session1 is still in strict mode.
	_, err = tk.Exec("insert t2 values ('abcd')")
	c.Check(err, NotNil)
	// Restore original global strict mode.
	tk.MustExec("set @@global.sql_mode = 'STRICT_TRANS_TABLES'")
}

func (s *testSuiteP2) TestTableDual(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	result := tk.MustQuery("Select 1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select 1 from dual")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select count(*) from dual")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select 1 from dual where 1")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustQuery("select t1.* from t t1, t t2 where t1.a=t2.a and 1=0").Check(testkit.Rows())
}

func (s *testSuiteP2) TestTableScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use information_schema")
	result := tk.MustQuery("select * from schemata")
	// There must be these tables: information_schema, mysql, performance_schema and test.
	c.Assert(len(result.Rows()), GreaterEqual, 4)
	tk.MustExec("use test")
	tk.MustExec("create database mytest")
	rowStr1 := fmt.Sprintf("%s %s %s %s %v", "def", "mysql", "utf8mb4", "utf8mb4_bin", nil)
	rowStr2 := fmt.Sprintf("%s %s %s %s %v", "def", "mytest", "utf8mb4", "utf8mb4_bin", nil)
	tk.MustExec("use information_schema")
	result = tk.MustQuery("select * from schemata where schema_name = 'mysql'")
	result.Check(testkit.Rows(rowStr1))
	result = tk.MustQuery("select * from schemata where schema_name like 'my%'")
	result.Check(testkit.Rows(rowStr1, rowStr2))
	result = tk.MustQuery("select 1 from tables limit 1")
	result.Check(testkit.Rows("1"))
}

func (s *testSuiteP2) TestAdapterStatement(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Check(err, IsNil)
	se.GetSessionVars().TxnCtx.InfoSchema = domain.GetDomain(se).InfoSchema()
	compiler := &executor.Compiler{Ctx: se}
	stmtNode, err := s.ParseOneStmt("select 1", "", "")
	c.Check(err, IsNil)
	stmt, err := compiler.Compile(context.TODO(), stmtNode)
	c.Check(err, IsNil)
	c.Check(stmt.OriginText(), Equals, "select 1")

	stmtNode, err = s.ParseOneStmt("create table test.t (a int)", "", "")
	c.Check(err, IsNil)
	stmt, err = compiler.Compile(context.TODO(), stmtNode)
	c.Check(err, IsNil)
	c.Check(stmt.OriginText(), Equals, "create table test.t (a int)")
}

func (s *testSuiteP2) TestIsPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use mysql")
	ctx := tk.Se.(sessionctx.Context)
	tests := map[string]bool{
		"select * from help_topic where name='aaa'":         false,
		"select 1 from help_topic where name='aaa'":         false,
		"select * from help_topic where help_topic_id=1":    true,
		"select * from help_topic where help_category_id=1": false,
	}
	infoSchema := ctx.GetSessionVars().GetInfoSchema().(infoschema.InfoSchema)

	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		c.Check(err, IsNil)
		err = plannercore.Preprocess(ctx, stmtNode, infoSchema)
		c.Check(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, infoSchema)
		c.Check(err, IsNil)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, result)
	}
}

func (s *testSuiteP2) TestClusteredIndexIsPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_cluster_index_is_point_get;")
	tk.MustExec("create database test_cluster_index_is_point_get;")
	tk.MustExec("use test_cluster_index_is_point_get;")

	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255), b int, c char(10), primary key (c, a));")
	ctx := tk.Se.(sessionctx.Context)

	tests := map[string]bool{
		"select 1 from t where a='x'":                   false,
		"select * from t where c='x'":                   false,
		"select * from t where a='x' and c='x'":         true,
		"select * from t where a='x' and c='x' and b=1": false,
	}
	infoSchema := ctx.GetSessionVars().GetInfoSchema().(infoschema.InfoSchema)
	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		c.Check(err, IsNil)
		err = plannercore.Preprocess(ctx, stmtNode, infoSchema)
		c.Check(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, infoSchema)
		c.Check(err, IsNil)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, result)
	}
}

func (s *testSerialSuite) TestPointGetRepeatableRead(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	tk1.MustExec(`create table point_get (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into point_get values (1, 1, 1)")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	var (
		step1 = "github.com/pingcap/tidb/executor/pointGetRepeatableReadTest-step1"
		step2 = "github.com/pingcap/tidb/executor/pointGetRepeatableReadTest-step2"
	)

	c.Assert(failpoint.Enable(step1, "return"), IsNil)
	c.Assert(failpoint.Enable(step2, "pause"), IsNil)

	updateWaitCh := make(chan struct{})
	go func() {
		ctx := context.WithValue(context.Background(), "pointGetRepeatableReadTest", updateWaitCh)
		ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
			return fpname == step1 || fpname == step2
		})
		rs, err := tk1.Se.Execute(ctx, "select c from point_get where b = 1")
		c.Assert(err, IsNil)
		result := tk1.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute sql fail"))
		result.Check(testkit.Rows("1"))
	}()

	<-updateWaitCh // Wait `POINT GET` first time `get`
	c.Assert(failpoint.Disable(step1), IsNil)
	tk2.MustExec("update point_get set b = 2, c = 2 where a = 1")
	c.Assert(failpoint.Disable(step2), IsNil)
}

func (s *testSerialSuite) TestBatchPointGetRepeatableRead(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	tk1.MustExec(`create table batch_point_get (a int, b int, c int, unique key k_b(a, b, c))`)
	tk1.MustExec("insert into batch_point_get values (1, 1, 1), (2, 3, 4), (3, 4, 5)")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	var (
		step1 = "github.com/pingcap/tidb/executor/batchPointGetRepeatableReadTest-step1"
		step2 = "github.com/pingcap/tidb/executor/batchPointGetRepeatableReadTest-step2"
	)

	c.Assert(failpoint.Enable(step1, "return"), IsNil)
	c.Assert(failpoint.Enable(step2, "pause"), IsNil)

	updateWaitCh := make(chan struct{})
	go func() {
		ctx := context.WithValue(context.Background(), "batchPointGetRepeatableReadTest", updateWaitCh)
		ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
			return fpname == step1 || fpname == step2
		})
		rs, err := tk1.Se.Execute(ctx, "select c from batch_point_get where (a, b, c) in ((1, 1, 1))")
		c.Assert(err, IsNil)
		result := tk1.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute sql fail"))
		result.Check(testkit.Rows("1"))
	}()

	<-updateWaitCh // Wait `POINT GET` first time `get`
	c.Assert(failpoint.Disable(step1), IsNil)
	tk2.MustExec("update batch_point_get set b = 2, c = 2 where a = 1")
	c.Assert(failpoint.Disable(step2), IsNil)
}

func (s *testSerialSuite) TestSplitRegionTimeout(c *C) {
	c.Assert(tikvutil.MockSplitRegionTimeout.Enable(`return(true)`), IsNil)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(100),b int, index idx1(b,a))")
	tk.MustExec(`split table t index idx1 by (10000,"abcd"),(10000000);`)
	tk.MustExec(`set @@tidb_wait_split_region_timeout=1`)
	// result 0 0 means split 0 region and 0 region finish scatter regions before timeout.
	tk.MustQuery(`split table t between (0) and (10000) regions 10`).Check(testkit.Rows("0 0"))
	err := tikvutil.MockSplitRegionTimeout.Disable()
	c.Assert(err, IsNil)

	// Test scatter regions timeout.
	c.Assert(tikvutil.MockScatterRegionTimeout.Enable(`return(true)`), IsNil)
	tk.MustQuery(`split table t between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	err = tikvutil.MockScatterRegionTimeout.Disable()
	c.Assert(err, IsNil)

	// Test pre-split with timeout.
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	c.Assert(tikvutil.MockScatterRegionTimeout.Enable(`return(true)`), IsNil)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	start := time.Now()
	tk.MustExec("create table t (a int, b int) partition by hash(a) partitions 5;")
	c.Assert(time.Since(start).Seconds(), Less, 10.0)
	err = tikvutil.MockScatterRegionTimeout.Disable()
	c.Assert(err, IsNil)
}

func (s *testSuiteP2) TestRow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 3)")
	tk.MustExec("insert t values (2, 1)")
	tk.MustExec("insert t values (2, 3)")
	result := tk.MustQuery("select * from t where (c, d) < (2,2)")
	result.Check(testkit.Rows("1 1", "1 3", "2 1"))
	result = tk.MustQuery("select * from t where (1,2,3) > (3,2,1)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t where row(1,2,3) > (3,2,1)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t where (c, d) = (select * from t where (c,d) = (1,1))")
	result.Check(testkit.Rows("1 1"))
	result = tk.MustQuery("select * from t where (c, d) = (select * from t k where (t.c,t.d) = (c,d))")
	result.Check(testkit.Rows("1 1", "1 3", "2 1", "2 3"))
	result = tk.MustQuery("select (1, 2, 3) < (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 3, 3)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 1, 4)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (2, 3, 4) >= (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) = (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) != (2, 3, 4)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select row(1, 1) in (row(1, 1))")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 0) in (row(1, 1))")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select row(1, 1) in (select 1, 1)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 1) > row(1, 0)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 1) > (select 1, 0)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select 1 > (select 1)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (select 1)")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("insert t1 values (1,2),(1,null)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (c int, d int)")
	tk.MustExec("insert t2 values (0,0)")

	tk.MustQuery("select * from t2 where (1,2) in (select * from t1)").Check(testkit.Rows("0 0"))
	tk.MustQuery("select * from t2 where (1,2) not in (select * from t1)").Check(testkit.Rows())
	tk.MustQuery("select * from t2 where (1,1) not in (select * from t1)").Check(testkit.Rows())
	tk.MustQuery("select * from t2 where (1,null) in (select * from t1)").Check(testkit.Rows())
	tk.MustQuery("select * from t2 where (null,null) in (select * from t1)").Check(testkit.Rows())

	tk.MustExec("delete from t1 where a=1 and b=2")
	tk.MustQuery("select (1,1) in (select * from t2) from t1").Check(testkit.Rows("0"))
	tk.MustQuery("select (1,1) not in (select * from t2) from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select (1,1) in (select 1,1 from t2) from t1").Check(testkit.Rows("1"))
	tk.MustQuery("select (1,1) not in (select 1,1 from t2) from t1").Check(testkit.Rows("0"))

	// MySQL 5.7 returns 1 for these 2 queries, which is wrong.
	tk.MustQuery("select (1,null) not in (select 1,1 from t2) from t1").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select (t1.a,null) not in (select 1,1 from t2) from t1").Check(testkit.Rows("<nil>"))

	tk.MustQuery("select (1,null) in (select * from t1)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select (1,null) not in (select * from t1)").Check(testkit.Rows("<nil>"))
}

func (s *testSuiteP2) TestColumnName(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	// disable only full group by
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'")
	rs, err := tk.Exec("select 1 + c, count(*) from t")
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 2)
	c.Check(fields[0].Column.Name.L, Equals, "1 + c")
	c.Check(fields[0].ColumnAsName.L, Equals, "1 + c")
	c.Check(fields[1].Column.Name.L, Equals, "count(*)")
	c.Check(fields[1].ColumnAsName.L, Equals, "count(*)")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select (c) > all (select c from t) from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.L, Equals, "(c) > all (select c from t)")
	c.Check(fields[0].ColumnAsName.L, Equals, "(c) > all (select c from t)")
	c.Assert(rs.Close(), IsNil)
	tk.MustExec("begin")
	tk.MustExec("insert t values(1,1)")
	rs, err = tk.Exec("select c d, d c from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 2)
	c.Check(fields[0].Column.Name.L, Equals, "c")
	c.Check(fields[0].ColumnAsName.L, Equals, "d")
	c.Check(fields[1].Column.Name.L, Equals, "d")
	c.Check(fields[1].ColumnAsName.L, Equals, "c")
	c.Assert(rs.Close(), IsNil)
	// Test case for query a column of a table.
	// In this case, all attributes have values.
	rs, err = tk.Exec("select c as a from t as t2")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(fields[0].Column.Name.L, Equals, "c")
	c.Check(fields[0].ColumnAsName.L, Equals, "a")
	c.Check(fields[0].Table.Name.L, Equals, "t")
	c.Check(fields[0].TableAsName.L, Equals, "t2")
	c.Check(fields[0].DBName.L, Equals, "test")
	c.Assert(rs.Close(), IsNil)
	// Test case for query a expression which only using constant inputs.
	// In this case, the table, org_table and database attributes will all be empty.
	rs, err = tk.Exec("select hour(1) as a from t as t2")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(fields[0].Column.Name.L, Equals, "a")
	c.Check(fields[0].ColumnAsName.L, Equals, "a")
	c.Check(fields[0].Table.Name.L, Equals, "")
	c.Check(fields[0].TableAsName.L, Equals, "")
	c.Check(fields[0].DBName.L, Equals, "")
	c.Assert(rs.Close(), IsNil)
	// Test case for query a column wrapped with parentheses and unary plus.
	// In this case, the column name should be its original name.
	rs, err = tk.Exec("select (c), (+c), +(c), +(+(c)), ++c from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	for i := 0; i < 5; i++ {
		c.Check(fields[i].Column.Name.L, Equals, "c")
		c.Check(fields[i].ColumnAsName.L, Equals, "c")
	}
	c.Assert(rs.Close(), IsNil)

	// Test issue https://github.com/pingcap/tidb/issues/9639 .
	// Both window function and expression appear in final result field.
	tk.MustExec("set @@tidb_enable_window_function = 1")
	rs, err = tk.Exec("select 1+1, row_number() over() num from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Assert(fields[0].Column.Name.L, Equals, "1+1")
	c.Assert(fields[0].ColumnAsName.L, Equals, "1+1")
	c.Assert(fields[1].Column.Name.L, Equals, "num")
	c.Assert(fields[1].ColumnAsName.L, Equals, "num")
	tk.MustExec("set @@tidb_enable_window_function = 0")
	c.Assert(rs.Close(), IsNil)

	rs, err = tk.Exec("select if(1,c,c) from t;")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Assert(fields[0].Column.Name.L, Equals, "if(1,c,c)")
	// It's a compatibility issue. Should be empty instead.
	c.Assert(fields[0].ColumnAsName.L, Equals, "if(1,c,c)")
	c.Assert(rs.Close(), IsNil)
}

func (s *testSuiteP2) TestSelectVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (d int)")
	tk.MustExec("insert into t values(1), (2), (1)")
	// This behavior is different from MySQL.
	result := tk.MustQuery("select @a, @a := d+1 from t")
	result.Check(testkit.Rows("<nil> 2", "2 3", "3 2"))
	// Test for PR #10658.
	tk.MustExec("select SQL_BIG_RESULT d from t group by d")
	tk.MustExec("select SQL_SMALL_RESULT d from t group by d")
	tk.MustExec("select SQL_BUFFER_RESULT d from t group by d")
}

func (s *testSuiteP2) TestHistoryRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists history_read")
	tk.MustExec("create table history_read (a int)")
	tk.MustExec("insert history_read values (1)")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	// Set snapshot to a time before save point will fail.
	_, err := tk.Exec("set @@tidb_snapshot = '2006-01-01 15:04:05.999999'")
	c.Assert(terror.ErrorEqual(err, variable.ErrSnapshotTooOld), IsTrue, Commentf("err %v", err))
	// SnapshotTS Is not updated if check failed.
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, uint64(0))

	curVer1, _ := s.store.CurrentVersion(oracle.GlobalTxnScope)
	time.Sleep(time.Millisecond)
	snapshotTime := time.Now()
	time.Sleep(time.Millisecond)
	curVer2, _ := s.store.CurrentVersion(oracle.GlobalTxnScope)
	tk.MustExec("insert history_read values (2)")
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1", "2"))
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	ctx := tk.Se.(sessionctx.Context)
	snapshotTS := ctx.GetSessionVars().SnapshotTS
	c.Assert(snapshotTS, Greater, curVer1.Ver)
	c.Assert(snapshotTS, Less, curVer2.Ver)
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1"))
	_, err = tk.Exec("insert history_read values (2)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("update history_read set a = 3 where a = 1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete from history_read where a = 1")
	c.Assert(err, NotNil)
	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1", "2"))
	tk.MustExec("insert history_read values (3)")
	tk.MustExec("update history_read set a = 4 where a = 3")
	tk.MustExec("delete from history_read where a = 1")

	time.Sleep(time.Millisecond)
	snapshotTime = time.Now()
	time.Sleep(time.Millisecond)
	tk.MustExec("alter table history_read add column b int")
	tk.MustExec("insert history_read values (8, 8), (9, 9)")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2 <nil>", "4 <nil>", "8 8", "9 9"))
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2", "4"))
	tsoStr := strconv.FormatUint(oracle.EncodeTSO(snapshotTime.UnixNano()/int64(time.Millisecond)), 10)

	tk.MustExec("set @@tidb_snapshot = '" + tsoStr + "'")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2", "4"))

	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2 <nil>", "4 <nil>", "8 8", "9 9"))
}

func (s *testSuite2) TestLowResolutionTSORead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@autocommit=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists low_resolution_tso")
	tk.MustExec("create table low_resolution_tso(a int)")
	tk.MustExec("insert low_resolution_tso values (1)")

	// enable low resolution tso
	c.Assert(tk.Se.GetSessionVars().LowResolutionTSO, IsFalse)
	_, err := tk.Exec("set @@tidb_low_resolution_tso = 'on'")
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().LowResolutionTSO, IsTrue)

	time.Sleep(3 * time.Second)
	tk.MustQuery("select * from low_resolution_tso").Check(testkit.Rows("1"))
	_, err = tk.Exec("update low_resolution_tso set a = 2")
	c.Assert(err, NotNil)
	tk.MustExec("set @@tidb_low_resolution_tso = 'off'")
	tk.MustExec("update low_resolution_tso set a = 2")
	tk.MustQuery("select * from low_resolution_tso").Check(testkit.Rows("2"))
}

func (s *testSuite) TestScanControlSelection(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx_b(b))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select (select count(1) k from t s where s.b = t1.c) from t t1").Sort().Check(testkit.Rows("0", "1", "3", "3"))
}

func (s *testSuite) TestSimpleDAG(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select a from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("select * from t where a = 4").Check(testkit.Rows("4 2 3"))
	tk.MustQuery("select a from t limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t order by a desc").Check(testkit.Rows("4", "3", "2", "1"))
	tk.MustQuery("select a from t order by a desc limit 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t order by b desc limit 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t where a < 3").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where b > 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t where b > 1 and a < 3").Check(testkit.Rows())
	tk.MustQuery("select count(*) from t where b > 1 and a < 3").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*), c from t group by c order by c").Check(testkit.Rows("2 1", "1 2", "1 3"))
	tk.MustQuery("select sum(c) as s from t group by b order by s").Check(testkit.Rows("3", "4"))
	tk.MustQuery("select avg(a) as s from t group by b order by s").Check(testkit.Rows("2.0000", "4.0000"))
	tk.MustQuery("select sum(distinct c) from t group by b").Check(testkit.Rows("3", "3"))

	tk.MustExec("create index i on t(c,b)")
	tk.MustQuery("select a from t where c = 1").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where c = 1 and a < 2").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t where c = 1 order by a limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t where c = 1 ").Check(testkit.Rows("2"))
	tk.MustExec("create index i1 on t(b)")
	tk.MustQuery("select c from t where b = 2").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where b = 2").Check(testkit.Rows("4 2 3"))
	tk.MustQuery("select count(*) from t where b = 1").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where b = 1 and a > 1 limit 1").Check(testkit.Rows("2 1 1"))

	// Test time push down.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, c1 datetime);")
	tk.MustExec("insert into t values (1, '2015-06-07 12:12:12')")
	tk.MustQuery("select id from t where c1 = '2015-06-07 12:12:12'").Check(testkit.Rows("1"))

	// Test issue 17816
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT)")
	tk.MustExec("INSERT INTO t0 VALUES (100000)")
	tk.MustQuery("SELECT * FROM t0 WHERE NOT SPACE(t0.c0)").Check(testkit.Rows("100000"))
}

func (s *testSuite) TestTimestampTimeZone(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (ts timestamp)")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t values ('2017-04-27 22:40:42')")
	// The timestamp will get different value if time_zone session variable changes.
	tests := []struct {
		timezone string
		expect   string
	}{
		{"+10:00", "2017-04-28 08:40:42"},
		{"-6:00", "2017-04-27 16:40:42"},
	}
	for _, tt := range tests {
		tk.MustExec(fmt.Sprintf("set time_zone = '%s'", tt.timezone))
		tk.MustQuery("select * from t").Check(testkit.Rows(tt.expect))
	}

	// For issue https://github.com/pingcap/tidb/issues/3467
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
 	      id bigint(20) NOT NULL AUTO_INCREMENT,
 	      uid int(11) DEFAULT NULL,
 	      datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 	      ip varchar(128) DEFAULT NULL,
 	    PRIMARY KEY (id),
 	      KEY i_datetime (datetime),
 	      KEY i_userid (uid)
 	    );`)
	tk.MustExec(`INSERT INTO t1 VALUES (123381351,1734,"2014-03-31 08:57:10","127.0.0.1");`)
	r := tk.MustQuery("select datetime from t1;") // Cover TableReaderExec
	r.Check(testkit.Rows("2014-03-31 08:57:10"))
	r = tk.MustQuery("select datetime from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("2014-03-31 08:57:10")) // Cover IndexReaderExec
	r = tk.MustQuery("select * from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("123381351 1734 2014-03-31 08:57:10 127.0.0.1")) // Cover IndexLookupExec

	// For issue https://github.com/pingcap/tidb/issues/3485
	tk.MustExec("set time_zone = 'Asia/Shanghai'")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
	    id bigint(20) NOT NULL AUTO_INCREMENT,
	    datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    PRIMARY KEY (id)
	  );`)
	tk.MustExec(`INSERT INTO t1 VALUES (123381351,"2014-03-31 08:57:10");`)
	r = tk.MustQuery(`select * from t1 where datetime="2014-03-31 08:57:10";`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	tk.MustExec(`alter table t1 add key i_datetime (datetime);`)
	r = tk.MustQuery(`select * from t1 where datetime="2014-03-31 08:57:10";`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	r = tk.MustQuery(`select * from t1;`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	r = tk.MustQuery("select datetime from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("2014-03-31 08:57:10"))
}

func (s *testSuite) TestTimestampDefaultValueTimeZone(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec(`create table t (a int, b timestamp default "2019-01-17 14:46:14")`)
	tk.MustExec("insert into t set a=1")
	r := tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2019-01-17 14:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t set a=2")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2019-01-17 06:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2019-01-17 06:46:14", "2 2019-01-17 06:46:14"))
	// Test the column's version is greater than ColumnInfoVersion1.
	sctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(sctx).InfoSchema()
	c.Assert(is, NotNil)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tb.Cols()[1].Version = model.ColumnInfoVersion1 + 1
	tk.MustExec("insert into t set a=3")
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2019-01-17 06:46:14", "2 2019-01-17 06:46:14", "3 2019-01-17 06:46:14"))
	tk.MustExec("delete from t where a=3")
	// Change time zone back.
	tk.MustExec("set time_zone = '+08:00'")
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2019-01-17 14:46:14", "2 2019-01-17 14:46:14"))
	tk.MustExec("set time_zone = '-08:00'")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2019-01-16 22:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// test zero default value in multiple time zone.
	defer tk.MustExec(fmt.Sprintf("set @@sql_mode='%s'", tk.MustQuery("select @@sql_mode").Rows()[0][0]))
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec(`create table t (a int, b timestamp default "0000-00-00 00")`)
	tk.MustExec("insert into t set a=1")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t set a=2")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '-08:00'")
	tk.MustExec("insert into t set a=3")
	r = tk.MustQuery(`show create table t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 0000-00-00 00:00:00", "2 0000-00-00 00:00:00", "3 0000-00-00 00:00:00"))

	// test add timestamp column default current_timestamp.
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`set time_zone = 'Asia/Shanghai'`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`insert into t set a=1`)
	tk.MustExec(`alter table t add column b timestamp not null default current_timestamp;`)
	timeIn8 := tk.MustQuery("select b from t").Rows()[0][0]
	tk.MustExec(`set time_zone = '+00:00'`)
	timeIn0 := tk.MustQuery("select b from t").Rows()[0][0]
	c.Assert(timeIn8 != timeIn0, IsTrue, Commentf("%v == %v", timeIn8, timeIn0))
	datumTimeIn8, err := expression.GetTimeValue(tk.Se, timeIn8, mysql.TypeTimestamp, 0)
	c.Assert(err, IsNil)
	tIn8To0 := datumTimeIn8.GetMysqlTime()
	timeZoneIn8, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	err = tIn8To0.ConvertTimeZone(timeZoneIn8, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(timeIn0 == tIn8To0.String(), IsTrue, Commentf("%v != %v", timeIn0, tIn8To0.String()))

	// test add index.
	tk.MustExec(`alter table t add index(b);`)
	tk.MustExec("admin check table t")
	tk.MustExec(`set time_zone = '+05:00'`)
	tk.MustExec("admin check table t")
}

func (s *testSuite) TestTiDBCurrentTS(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))
	tk.MustExec("begin")
	rows := tk.MustQuery("select @@tidb_current_ts").Rows()
	tsStr := rows[0][0].(string)
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(tsStr, Equals, fmt.Sprintf("%d", txn.StartTS()))
	tk.MustExec("begin")
	rows = tk.MustQuery("select @@tidb_current_ts").Rows()
	newTsStr := rows[0][0].(string)
	txn, err = tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(newTsStr, Equals, fmt.Sprintf("%d", txn.StartTS()))
	c.Assert(newTsStr, Not(Equals), tsStr)
	tk.MustExec("commit")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	_, err = tk.Exec("set @@tidb_current_ts = '1'")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))
}

func (s *testSuite) TestTiDBLastTxnInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	tk.MustQuery("select @@tidb_last_txn_info").Check(testkit.Rows(""))

	tk.MustExec("insert into t values (1)")
	rows1 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(rows1[0][0].(string), Greater, "0")
	c.Assert(rows1[0][0].(string), Less, rows1[0][1].(string))

	tk.MustExec("begin")
	tk.MustQuery("select a from t where a = 1").Check(testkit.Rows("1"))
	rows2 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts'), @@tidb_current_ts").Rows()
	tk.MustExec("commit")
	rows3 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(rows2[0][0], Equals, rows1[0][0])
	c.Assert(rows2[0][1], Equals, rows1[0][1])
	c.Assert(rows3[0][0], Equals, rows1[0][0])
	c.Assert(rows3[0][1], Equals, rows1[0][1])
	c.Assert(rows2[0][1], Less, rows2[0][2])

	tk.MustExec("begin")
	tk.MustExec("update t set a = a + 1 where a = 1")
	rows4 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts'), @@tidb_current_ts").Rows()
	tk.MustExec("commit")
	rows5 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(rows4[0][0], Equals, rows1[0][0])
	c.Assert(rows4[0][1], Equals, rows1[0][1])
	c.Assert(rows4[0][2], Equals, rows5[0][0])
	c.Assert(rows4[0][1], Less, rows4[0][2])
	c.Assert(rows4[0][2], Less, rows5[0][1])

	tk.MustExec("begin")
	tk.MustExec("update t set a = a + 1 where a = 2")
	tk.MustExec("rollback")
	rows6 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(rows6[0][0], Equals, rows5[0][0])
	c.Assert(rows6[0][1], Equals, rows5[0][1])

	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (2)")
	_, err := tk.Exec("commit")
	c.Assert(err, NotNil)
	rows7 := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts'), json_extract(@@tidb_last_txn_info, '$.error')").Rows()
	c.Assert(rows7[0][0], Greater, rows5[0][0])
	c.Assert(rows7[0][1], Equals, "0")
	c.Assert(strings.Contains(err.Error(), rows7[0][1].(string)), IsTrue)

	_, err = tk.Exec("set @@tidb_last_txn_info = '{}'")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))
}

func (s *testSerialSuite) TestTiDBLastTxnInfoCommitMode(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = time.Second
	})

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, v int)")
	tk.MustExec("insert into t values (1, 1)")

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows := tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	c.Log(rows)
	c.Assert(rows[0][0], Equals, `"async_commit"`)
	c.Assert(rows[0][1], Equals, "false")
	c.Assert(rows[0][2], Equals, "false")

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	c.Assert(rows[0][0], Equals, `"1pc"`)
	c.Assert(rows[0][1], Equals, "false")
	c.Assert(rows[0][2], Equals, "false")

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	c.Assert(rows[0][0], Equals, `"2pc"`)
	c.Assert(rows[0][1], Equals, "false")
	c.Assert(rows[0][2], Equals, "false")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
	})

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 0")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	c.Log(rows)
	c.Assert(rows[0][0], Equals, `"2pc"`)
	c.Assert(rows[0][1], Equals, "true")
	c.Assert(rows[0][2], Equals, "false")

	tk.MustExec("set @@tidb_enable_async_commit = 0")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	c.Log(rows)
	c.Assert(rows[0][0], Equals, `"2pc"`)
	c.Assert(rows[0][1], Equals, "false")
	c.Assert(rows[0][2], Equals, "true")

	tk.MustExec("set @@tidb_enable_async_commit = 1")
	tk.MustExec("set @@tidb_enable_1pc = 1")
	tk.MustExec("update t set v = v + 1 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.txn_commit_mode'), json_extract(@@tidb_last_txn_info, '$.async_commit_fallback'), json_extract(@@tidb_last_txn_info, '$.one_pc_fallback')").Rows()
	c.Log(rows)
	c.Assert(rows[0][0], Equals, `"2pc"`)
	c.Assert(rows[0][1], Equals, "true")
	c.Assert(rows[0][2], Equals, "true")
}

func (s *testSuite) TestTiDBLastQueryInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, v int)")
	tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.start_ts')").Check(testkit.Rows("0 0"))

	toUint64 := func(str interface{}) uint64 {
		res, err := strconv.ParseUint(str.(string), 10, 64)
		c.Assert(err, IsNil)
		return res
	}

	tk.MustExec("select * from t")
	rows := tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	c.Assert(toUint64(rows[0][0]), Greater, uint64(0))
	c.Assert(rows[0][0], Equals, rows[0][1])

	tk.MustExec("insert into t values (1, 10)")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	c.Assert(toUint64(rows[0][0]), Greater, uint64(0))
	c.Assert(rows[0][0], Equals, rows[0][1])
	// tidb_last_txn_info is still valid after checking query info.
	rows = tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts'), json_extract(@@tidb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(toUint64(rows[0][0]), Greater, uint64(0))
	c.Assert(rows[0][0].(string), Less, rows[0][1].(string))

	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	c.Assert(toUint64(rows[0][0]), Greater, uint64(0))
	c.Assert(rows[0][0], Equals, rows[0][1])

	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")
	tk2.MustExec("update t set v = 11 where a = 1")

	tk.MustExec("select * from t")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	c.Assert(toUint64(rows[0][0]), Greater, uint64(0))
	c.Assert(rows[0][0], Equals, rows[0][1])

	tk.MustExec("update t set v = 12 where a = 1")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	c.Assert(toUint64(rows[0][0]), Greater, uint64(0))
	c.Assert(toUint64(rows[0][0]), Less, toUint64(rows[0][1]))

	tk.MustExec("commit")

	tk.MustExec("set transaction isolation level read committed")
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t")
	rows = tk.MustQuery("select json_extract(@@tidb_last_query_info, '$.start_ts'), json_extract(@@tidb_last_query_info, '$.for_update_ts')").Rows()
	c.Assert(toUint64(rows[0][0]), Greater, uint64(0))
	c.Assert(toUint64(rows[0][0]), Less, toUint64(rows[0][1]))

	tk.MustExec("rollback")
}

func (s *testSuite) TestSelectForUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t, t1")

	txn, err := tk.Se.Txn(true)
	c.Assert(kv.ErrInvalidTxn.Equal(err), IsTrue)
	c.Assert(txn.Valid(), IsFalse)
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert t values (11, 2, 3)")
	tk.MustExec("insert t values (12, 2, 3)")
	tk.MustExec("insert t values (13, 2, 3)")

	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("insert t1 values (11)")

	// conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=11")
	tk2.MustExec("commit")

	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)

	// no conflict for subquery.
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where exists(select null from t1 where t1.c1=t.c1) for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// not conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=22 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// not conflict, auto commit
	tk1.MustExec("set @@autocommit=1;")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=11")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from (select * from t for update) t join t1 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t1 set c1 = 13")
	tk2.MustExec("commit")

	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)

}

func (s *testSuite) TestEmptyEnum(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (e enum('Y', 'N'))")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'")
	_, err := tk.Exec("insert into t values (0)")
	c.Assert(terror.ErrorEqual(err, types.ErrTruncated), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec("insert into t values ('abc')")
	c.Assert(terror.ErrorEqual(err, types.ErrTruncated), IsTrue, Commentf("err %v", err))

	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert into t values (0)")
	tk.MustQuery("select * from t").Check(testkit.Rows(""))
	tk.MustExec("insert into t values ('abc')")
	tk.MustQuery("select * from t").Check(testkit.Rows("", ""))
	tk.MustExec("insert into t values (null)")
	tk.MustQuery("select * from t").Check(testkit.Rows("", "", "<nil>"))
}

// TestIssue4024 This tests https://github.com/pingcap/tidb/issues/4024
func (s *testSuite) TestIssue4024(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("update t, test2.t set test2.t.a=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustQuery("select * from test2.t").Check(testkit.Rows("2"))
	tk.MustExec("update test.t, test2.t set test.t.a=3")
	tk.MustQuery("select * from t").Check(testkit.Rows("3"))
	tk.MustQuery("select * from test2.t").Check(testkit.Rows("2"))
}

const (
	checkRequestOff = iota
	checkRequestSyncLog
	checkDDLAddIndexPriority
)

type checkRequestClient struct {
	tikv.Client
	priority       pb.CommandPri
	lowPriorityCnt uint32
	mu             struct {
		sync.RWMutex
		checkFlags uint32
		syncLog    bool
	}
}

func (c *checkRequestClient) setCheckPriority(priority pb.CommandPri) {
	atomic.StoreInt32((*int32)(&c.priority), int32(priority))
}

func (c *checkRequestClient) getCheckPriority() pb.CommandPri {
	return (pb.CommandPri)(atomic.LoadInt32((*int32)(&c.priority)))
}

func (c *checkRequestClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	c.mu.RLock()
	checkFlags := c.mu.checkFlags
	c.mu.RUnlock()
	if checkFlags == checkRequestSyncLog {
		switch req.Type {
		case tikvrpc.CmdPrewrite, tikvrpc.CmdCommit:
			c.mu.RLock()
			syncLog := c.mu.syncLog
			c.mu.RUnlock()
			if syncLog != req.SyncLog {
				return nil, errors.New("fail to set sync log")
			}
		}
	} else if checkFlags == checkDDLAddIndexPriority {
		if req.Type == tikvrpc.CmdScan {
			if c.getCheckPriority() != req.Priority {
				return nil, errors.New("fail to set priority")
			}
		} else if req.Type == tikvrpc.CmdPrewrite {
			if c.getCheckPriority() == pb.CommandPri_Low {
				atomic.AddUint32(&c.lowPriorityCnt, 1)
			}
		}
	}
	return resp, err
}

type testSuiteWithCliBase struct {
	store kv.Storage
	dom   *domain.Domain
	cli   *checkRequestClient
}

type testSuite1 struct {
	testSuiteWithCliBase
}

type testSerialSuite2 struct {
	testSuiteWithCliBase
}

func (s *testSuiteWithCliBase) SetUpSuite(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	session.SetStatsLease(0)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom.SetStatsUpdating(true)
}

func (s *testSuiteWithCliBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testSuiteWithCliBase) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuite2) TestAddIndexPriority(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}

	store, err := mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
	}()

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int, v int)")

	// Insert some data to make sure plan build IndexLookup for t1.
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
	}

	cli.mu.Lock()
	cli.mu.checkFlags = checkDDLAddIndexPriority
	cli.mu.Unlock()

	cli.setCheckPriority(pb.CommandPri_Low)
	tk.MustExec("alter table t1 add index t1_index (id);")

	c.Assert(atomic.LoadUint32(&cli.lowPriorityCnt) > 0, IsTrue)

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()

	tk.MustExec("alter table t1 drop index t1_index;")
	tk.MustExec("SET SESSION tidb_ddl_reorg_priority = 'PRIORITY_NORMAL'")

	cli.mu.Lock()
	cli.mu.checkFlags = checkDDLAddIndexPriority
	cli.mu.Unlock()

	cli.setCheckPriority(pb.CommandPri_Normal)
	tk.MustExec("alter table t1 add index t1_index (id);")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()

	tk.MustExec("alter table t1 drop index t1_index;")
	tk.MustExec("SET SESSION tidb_ddl_reorg_priority = 'PRIORITY_HIGH'")

	cli.mu.Lock()
	cli.mu.checkFlags = checkDDLAddIndexPriority
	cli.mu.Unlock()

	cli.setCheckPriority(pb.CommandPri_High)
	tk.MustExec("alter table t1 add index t1_index (id);")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()
}

func (s *testSuite1) TestAlterTableComment(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_1")
	tk.MustExec("create table t_1 (c1 int, c2 int, c3 int default 1, index (c1)) comment = 'test table';")
	tk.MustExec("alter table `t_1` comment 'this is table comment';")
	result := tk.MustQuery("select table_comment from information_schema.tables where table_name = 't_1';")
	result.Check(testkit.Rows("this is table comment"))
	tk.MustExec("alter table `t_1` comment 'table t comment';")
	result = tk.MustQuery("select table_comment from information_schema.tables where table_name = 't_1';")
	result.Check(testkit.Rows("table t comment"))
}

func (s *testSuite) TestTimezonePushDown(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (ts timestamp)")
	defer tk.MustExec("drop table t")
	tk.MustExec(`insert into t values ("2018-09-13 10:02:06")`)

	systemTZ := timeutil.SystemLocation()
	c.Assert(systemTZ.String(), Not(Equals), "System")
	c.Assert(systemTZ.String(), Not(Equals), "Local")
	ctx := context.Background()
	count := 0
	ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *kv.Request) {
		count += 1
		dagReq := new(tipb.DAGRequest)
		err := proto.Unmarshal(req.Data, dagReq)
		c.Assert(err, IsNil)
		c.Assert(dagReq.GetTimeZoneName(), Equals, systemTZ.String())
	})
	_, err := tk.Se.Execute(ctx1, `select * from t where ts = "2018-09-13 10:02:06"`)
	c.Assert(err, IsNil)

	tk.MustExec(`set time_zone="System"`)
	_, err = tk.Se.Execute(ctx1, `select * from t where ts = "2018-09-13 10:02:06"`)
	c.Assert(err, IsNil)

	c.Assert(count, Equals, 2) // Make sure the hook function is called.
}

func (s *testSuite) TestNotFillCacheFlag(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	defer tk.MustExec("drop table t")
	tk.MustExec("insert into t values (1)")

	tests := []struct {
		sql    string
		expect bool
	}{
		{"select SQL_NO_CACHE * from t", true},
		{"select SQL_CACHE * from t", false},
		{"select * from t", false},
	}
	count := 0
	ctx := context.Background()
	for _, test := range tests {
		ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *kv.Request) {
			count++
			if req.NotFillCache != test.expect {
				c.Errorf("sql=%s, expect=%v, get=%v", test.sql, test.expect, req.NotFillCache)
			}
		})
		rs, err := tk.Se.Execute(ctx1, test.sql)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs[0], Commentf("sql: %v", test.sql))
	}
	c.Assert(count, Equals, len(tests)) // Make sure the hook function is called.
}

func (s *testSuite1) TestSyncLog(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	cli := s.cli
	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestSyncLog
	cli.mu.syncLog = true
	cli.mu.Unlock()
	tk.MustExec("create table t (id int primary key)")
	cli.mu.Lock()
	cli.mu.syncLog = false
	cli.mu.Unlock()
	tk.MustExec("insert into t values (1)")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()
}

func (s *testSuite) TestHandleTransfer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1), (2), (4)")
	tk.MustExec("begin")
	tk.MustExec("update t set a = 3 where a = 4")
	// test table scan read whose result need handle.
	tk.MustQuery("select * from t ignore index(idx)").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("insert into t values(4)")
	// test single read whose result need handle
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("select * from t use index(idx) order by a desc").Check(testkit.Rows("4", "3", "2", "1"))
	tk.MustExec("update t set a = 5 where a = 3")
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1", "2", "4", "5"))
	tk.MustExec("commit")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("insert into t values(3, 3), (1, 1), (2, 2)")
	// Second test double read.
	tk.MustQuery("select * from t use index(idx) order by a").Check(testkit.Rows("1 1", "2 2", "3 3"))
}

func (s *testSuite) TestBit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(2))")
	tk.MustExec("insert into t values (0), (1), (2), (3)")
	_, err := tk.Exec("insert into t values (4)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values ('a')")
	c.Assert(err, NotNil)
	r, err := tk.Exec("select * from t where c1 = 2")
	c.Assert(err, IsNil)
	req := r.NewChunk()
	err = r.Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(types.BinaryLiteral(req.GetRow(0).GetBytes(0)), DeepEquals, types.NewBinaryLiteralFromUint(2, -1))
	r.Close()

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(31))")
	tk.MustExec("insert into t values (0x7fffffff)")
	_, err = tk.Exec("insert into t values (0x80000000)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (0xffffffff)")
	c.Assert(err, NotNil)
	tk.MustExec("insert into t values ('123')")
	tk.MustExec("insert into t values ('1234')")
	_, err = tk.Exec("insert into t values ('12345)")
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(62))")
	tk.MustExec("insert into t values ('12345678')")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(61))")
	_, err = tk.Exec("insert into t values ('12345678')")
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(32))")
	tk.MustExec("insert into t values (0x7fffffff)")
	tk.MustExec("insert into t values (0xffffffff)")
	_, err = tk.Exec("insert into t values (0x1ffffffff)")
	c.Assert(err, NotNil)
	tk.MustExec("insert into t values ('1234')")
	_, err = tk.Exec("insert into t values ('12345')")
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(64))")
	tk.MustExec("insert into t values (0xffffffffffffffff)")
	tk.MustExec("insert into t values ('12345678')")
	_, err = tk.Exec("insert into t values ('123456789')")
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(64))")
	tk.MustExec("insert into t values (0xffffffffffffffff)")
	tk.MustExec("insert into t values ('12345678')")
	tk.MustQuery("select * from t where c1").Check(testkit.Rows("\xff\xff\xff\xff\xff\xff\xff\xff", "12345678"))
}

func (s *testSuite) TestEnum(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c enum('a', 'b', 'c'))")
	tk.MustExec("insert into t values ('a'), (2), ('c')")
	tk.MustQuery("select * from t where c = 'a'").Check(testkit.Rows("a"))

	tk.MustQuery("select c + 1 from t where c = 2").Check(testkit.Rows("3"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (null), ('1')")
	tk.MustQuery("select c + 1 from t where c = 1").Check(testkit.Rows("2"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(1), (2), (3)")
	tk.MustQuery("select * from t where c").Check(testkit.Rows("a", "b", "c"))
}

func (s *testSuite) TestSet(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c set('a', 'b', 'c'))")
	tk.MustExec("insert into t values ('a'), (2), ('c'), ('a,b'), ('b,a')")
	tk.MustQuery("select * from t where c = 'a'").Check(testkit.Rows("a"))

	tk.MustQuery("select * from t where c = 'a,b'").Check(testkit.Rows("a,b", "a,b"))

	tk.MustQuery("select c + 1 from t where c = 2").Check(testkit.Rows("3"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (null), ('1')")
	tk.MustQuery("select c + 1 from t where c = 1").Check(testkit.Rows("2"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(3)")
	tk.MustQuery("select * from t where c").Check(testkit.Rows("a,b"))
}

func (s *testSuite) TestSubqueryInValues(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, name varchar(20))")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (gid int)")

	tk.MustExec("insert into t1 (gid) value (1)")
	tk.MustExec("insert into t (id, name) value ((select gid from t1) ,'asd')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 asd"))
}

func (s *testSuite) TestEnhancedRangeAccess(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values(1, 2), (2, 1)")
	tk.MustQuery("select * from t where (a = 1 and b = 2) or (a = 2 and b = 1)").Check(testkit.Rows("1 2", "2 1"))
	tk.MustQuery("select * from t where (a = 1 and b = 1) or (a = 2 and b = 2)").Check(nil)
}

// TestMaxInt64Handle Issue #4810
func (s *testSuite) TestMaxInt64Handle(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id bigint, PRIMARY KEY (id))")
	tk.MustExec("insert into t values(9223372036854775807)")
	tk.MustExec("select * from t where id = 9223372036854775807")
	tk.MustQuery("select * from t where id = 9223372036854775807;").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("select * from t").Check(testkit.Rows("9223372036854775807"))
	_, err := tk.Exec("insert into t values(9223372036854775807)")
	c.Assert(err, NotNil)
	tk.MustExec("delete from t where id = 9223372036854775807")
	tk.MustQuery("select * from t").Check(nil)
}

func (s *testSuite) TestTableScanWithPointRanges(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, PRIMARY KEY (id))")
	tk.MustExec("insert into t values(1), (5), (10)")
	tk.MustQuery("select * from t where id in(1, 2, 10)").Check(testkit.Rows("1", "10"))
}

func (s *testSuite) TestUnsignedPk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id bigint unsigned primary key)")
	var num1, num2 uint64 = math.MaxInt64 + 1, math.MaxInt64 + 2
	tk.MustExec(fmt.Sprintf("insert into t values(%v), (%v), (1), (2)", num1, num2))
	num1Str := strconv.FormatUint(num1, 10)
	num2Str := strconv.FormatUint(num2, 10)
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1", "2", num1Str, num2Str))
	tk.MustQuery("select * from t where id not in (2)").Check(testkit.Rows(num1Str, num2Str, "1"))
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a bigint unsigned primary key, b int, index idx(b))")
	tk.MustExec("insert into t values(9223372036854775808, 1), (1, 1)")
	tk.MustQuery("select * from t use index(idx) where b = 1 and a < 2").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t use index(idx) where b = 1 order by b, a").Check(testkit.Rows("1 1", "9223372036854775808 1"))
}

func (s *testSuite) TestSignedCommonHandle(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(k1 int, k2 int, primary key(k1, k2))")
	tk.MustExec("insert into t(k1, k2) value(-100, 1), (-50, 1), (0, 0), (1, 1), (3, 3)")
	tk.MustQuery("select k1 from t order by k1").Check(testkit.Rows("-100", "-50", "0", "1", "3"))
	tk.MustQuery("select k1 from t order by k1 desc").Check(testkit.Rows("3", "1", "0", "-50", "-100"))
	tk.MustQuery("select k1 from t where k1 < -51").Check(testkit.Rows("-100"))
	tk.MustQuery("select k1 from t where k1 < -1").Check(testkit.Rows("-100", "-50"))
	tk.MustQuery("select k1 from t where k1 <= 0").Check(testkit.Rows("-100", "-50", "0"))
	tk.MustQuery("select k1 from t where k1 < 2").Check(testkit.Rows("-100", "-50", "0", "1"))
	tk.MustQuery("select k1 from t where k1 < -1 and k1 > -90").Check(testkit.Rows("-50"))
}

func (s *testSuite) TestIssue5666(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@profiling=1")
	tk.MustQuery("SELECT QUERY_ID, SUM(DURATION) AS SUM_DURATION FROM INFORMATION_SCHEMA.PROFILING GROUP BY QUERY_ID;").Check(testkit.Rows("0 0"))
}

func (s *testSuite) TestIssue5341(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop table if exists test.t")
	tk.MustExec("create table test.t(a char)")
	tk.MustExec("insert into test.t value('a')")
	tk.MustQuery("select * from test.t where a < 1 order by a limit 0;").Check(testkit.Rows())
}

func (s *testSuite) TestContainDotColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.t1")
	tk.MustExec("create table test.t1(t1.a char)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a char, t2.b int)")

	tk.MustExec("drop table if exists t3")
	_, err := tk.Exec("create table t3(s.a char);")
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(mysql.ErrWrongTableName))
}

func (s *testSuite) TestCheckIndex(c *C) {
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()

	_, err = se.Execute(context.Background(), "create database test_admin")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_admin")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "create table t (pk int primary key, c int default 1, c1 int default 1, unique key c(c))")
	c.Assert(err, IsNil)
	is := s.domain.InfoSchema()
	db := model.NewCIStr("test_admin")
	dbInfo, ok := is.SchemaByName(db)
	c.Assert(ok, IsTrue)
	tblName := model.NewCIStr("t")
	tbl, err := is.TableByName(db, tblName)
	c.Assert(err, IsNil)
	tbInfo := tbl.Meta()

	alloc := autoid.NewAllocator(s.store, dbInfo.ID, false, autoid.RowIDAllocType)
	tb, err := tables.TableFromMeta(autoid.NewAllocators(alloc), tbInfo)
	c.Assert(err, IsNil)

	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(err, IsNil)

	_, err = se.Execute(context.Background(), "admin check index t C")
	c.Assert(err, IsNil)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20)
	// table     data (handle, data): (1, 10), (2, 20)
	recordVal1 := types.MakeDatums(int64(1), int64(10), int64(11))
	recordVal2 := types.MakeDatums(int64(2), int64(20), int64(21))
	c.Assert(s.ctx.NewTxn(context.Background()), IsNil)
	_, err = tb.AddRecord(s.ctx, recordVal1)
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(s.ctx, recordVal2)
	c.Assert(err, IsNil)
	txn, err := s.ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	mockCtx := mock.NewContext()
	idx := tb.Indices()[0]
	sc := &stmtctx.StatementContext{TimeZone: time.Local}

	_, err = se.Execute(context.Background(), "admin check index t idx_inexistent")
	c.Assert(strings.Contains(err.Error(), "not exist"), IsTrue)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(30)), kv.IntHandle(3), nil)
	c.Assert(err, IsNil)
	key := tablecodec.EncodeRowKey(tb.Meta().ID, kv.IntHandle(4).Encoded())
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "handle 3, index:types.Datum{k:0x1, decimal:0x0, length:0x0, i:30, collation:\"\", b:[]uint8(nil), x:interface {}(nil)} != record:<nil>")

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(40)), kv.IntHandle(4), nil)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(strings.Contains(err.Error(), "table count 3 != index(c) count 4"), IsTrue)

	// set data to:
	// index     data (handle, data): (1, 10), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = idx.Delete(sc, txn, types.MakeDatums(int64(30)), kv.IntHandle(3))
	c.Assert(err, IsNil)
	err = idx.Delete(sc, txn, types.MakeDatums(int64(20)), kv.IntHandle(2))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(strings.Contains(err.Error(), "table count 3 != index(c) count 2"), IsTrue)

	// TODO: pass the case below：
	// set data to:
	// index     data (handle, data): (1, 10), (4, 40), (2, 30)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
}

func setColValue(c *C, txn kv.Transaction, key kv.Key, v types.Datum) {
	row := []types.Datum{v, {}}
	colIDs := []int64{2, 3}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	rd := rowcodec.Encoder{Enable: true}
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil, &rd)
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
}

func (s *testSuite) TestCheckTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// Test 'admin check table' when the table has a unique index with null values.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test;")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2));")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (NULL, NULL);")
	tk.MustExec("admin check table admin_test;")
}

func (s *testSuite) TestCheckTableClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test;")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists admin_test;")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key (c1, c2), index (c1), unique key(c2));")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("admin check table admin_test;")
}

func (s *testSuite) TestCoprocessorStreamingFlag(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int, value int, index idx(id))")
	// Add some data to make statistics work.
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	tests := []struct {
		sql    string
		expect bool
	}{
		{"select * from t", true},                         // TableReader
		{"select * from t where id = 5", true},            // IndexLookup
		{"select * from t where id > 5", true},            // Filter
		{"select * from t limit 3", false},                // Limit
		{"select avg(id) from t", false},                  // Aggregate
		{"select * from t order by value limit 3", false}, // TopN
	}

	ctx := context.Background()
	for _, test := range tests {
		ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *kv.Request) {
			if req.Streaming != test.expect {
				c.Errorf("sql=%s, expect=%v, get=%v", test.sql, test.expect, req.Streaming)
			}
		})
		rs, err := tk.Se.Execute(ctx1, test.sql)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs[0], Commentf("sql: %v", test.sql))
	}
}

func (s *testSuite) TestIncorrectLimitArg(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint);`)
	tk.MustExec(`prepare stmt1 from 'select * from t limit ?';`)
	tk.MustExec(`prepare stmt2 from 'select * from t limit ?, ?';`)
	tk.MustExec(`set @a = -1;`)
	tk.MustExec(`set @b =  1;`)

	var err error
	_, err = tk.Se.Execute(context.TODO(), `execute stmt1 using @a;`)
	c.Assert(err.Error(), Equals, `[planner:1210]Incorrect arguments to LIMIT`)

	_, err = tk.Se.Execute(context.TODO(), `execute stmt2 using @b, @a;`)
	c.Assert(err.Error(), Equals, `[planner:1210]Incorrect arguments to LIMIT`)
}

func (s *testSuite) TestLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint, b bigint);`)
	tk.MustExec(`insert into t values(1, 1), (2, 2), (3, 30), (4, 40), (5, 5), (6, 6);`)
	tk.MustQuery(`select * from t order by a limit 1, 1;`).Check(testkit.Rows(
		"2 2",
	))
	tk.MustQuery(`select * from t order by a limit 1, 2;`).Check(testkit.Rows(
		"2 2",
		"3 30",
	))
	tk.MustQuery(`select * from t order by a limit 1, 3;`).Check(testkit.Rows(
		"2 2",
		"3 30",
		"4 40",
	))
	tk.MustQuery(`select * from t order by a limit 1, 4;`).Check(testkit.Rows(
		"2 2",
		"3 30",
		"4 40",
		"5 5",
	))

	// test inline projection
	tk.MustQuery(`select a from t where a > 0 limit 1, 1;`).Check(testkit.Rows(
		"2",
	))
	tk.MustQuery(`select a from t where a > 0 limit 1, 2;`).Check(testkit.Rows(
		"2",
		"3",
	))
	tk.MustQuery(`select b from t where a > 0 limit 1, 3;`).Check(testkit.Rows(
		"2",
		"30",
		"40",
	))
	tk.MustQuery(`select b from t where a > 0 limit 1, 4;`).Check(testkit.Rows(
		"2",
		"30",
		"40",
		"5",
	))

	// test @@tidb_init_chunk_size=2
	tk.MustExec(`set @@tidb_init_chunk_size=2;`)
	tk.MustQuery(`select * from t where a > 0 limit 2, 1;`).Check(testkit.Rows(
		"3 30",
	))
	tk.MustQuery(`select * from t where a > 0 limit 2, 2;`).Check(testkit.Rows(
		"3 30",
		"4 40",
	))
	tk.MustQuery(`select * from t where a > 0 limit 2, 3;`).Check(testkit.Rows(
		"3 30",
		"4 40",
		"5 5",
	))
	tk.MustQuery(`select * from t where a > 0 limit 2, 4;`).Check(testkit.Rows(
		"3 30",
		"4 40",
		"5 5",
		"6 6",
	))

	// test inline projection
	tk.MustQuery(`select a from t order by a limit 2, 1;`).Check(testkit.Rows(
		"3",
	))
	tk.MustQuery(`select b from t order by a limit 2, 2;`).Check(testkit.Rows(
		"30",
		"40",
	))
	tk.MustQuery(`select a from t order by a limit 2, 3;`).Check(testkit.Rows(
		"3",
		"4",
		"5",
	))
	tk.MustQuery(`select b from t order by a limit 2, 4;`).Check(testkit.Rows(
		"30",
		"40",
		"5",
		"6",
	))
}

func (s *testSuite) TestCoprocessorStreamingWarning(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a double)")
	tk.MustExec("insert into t value(1.2)")
	tk.MustExec("set @@session.tidb_enable_streaming = 1")

	result := tk.MustQuery("select * from t where a/0 > 1")
	result.Check(testkit.Rows())
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1365|Division by 0"))
}

func (s *testSuite3) TestYearTypeDeleteIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a YEAR, PRIMARY KEY(a));")
	tk.MustExec("insert into t set a = '2151';")
	tk.MustExec("delete from t;")
	tk.MustExec("admin check table t")
}

func (s *testSuite3) TestForSelectScopeInUnion(c *C) {
	// A union B for update, the "for update" option belongs to union statement, so
	// it should works on both A and B.
	tk1 := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t")
	tk1.MustExec("create table t(a int)")
	tk1.MustExec("insert into t values (1)")

	tk1.MustExec("begin")
	// 'For update' would act on the second select.
	tk1.MustQuery("select 1 as a union select a from t for update")

	tk2.MustExec("use test")
	tk2.MustExec("update t set a = a + 1")

	// As tk1 use select 'for update', it should detect conflict and fail.
	_, err := tk1.Exec("commit")
	c.Assert(err, NotNil)

	tk1.MustExec("begin")
	tk1.MustQuery("select 1 as a union select a from t limit 5 for update")
	tk1.MustQuery("select 1 as a union select a from t order by a for update")

	tk2.MustExec("update t set a = a + 1")

	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)
}

func (s *testSuite3) TestUnsignedDecimalOverflow(c *C) {
	tests := []struct {
		input  interface{}
		hasErr bool
		err    string
	}{{
		-1,
		true,
		"Out of range value for column",
	}, {
		"-1.1e-1",
		true,
		"Out of range value for column",
	}, {
		-1.1,
		true,
		"Out of range value for column",
	}, {
		-0,
		false,
		"",
	},
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(10,2) unsigned)")
	for _, t := range tests {
		res, err := tk.Exec("insert into t values (?)", t.input)
		if res != nil {
			defer res.Close()
		}
		if t.hasErr {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), t.err), IsTrue)
		} else {
			c.Assert(err, IsNil)
		}
		if res != nil {
			c.Assert(res.Close(), IsNil)
		}
	}

	tk.MustExec("set sql_mode=''")
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values (?)", -1)
	r := tk.MustQuery("select a from t limit 1")
	r.Check(testkit.Rows("0.00"))
}

func (s *testSuite3) TestIndexJoinTableDualPanic(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists a")
	tk.MustExec("create table a (f1 int, f2 varchar(32), primary key (f1))")
	tk.MustExec("insert into a (f1,f2) values (1,'a'), (2,'b'), (3,'c')")
	// TODO here: index join cause the data race of txn.
	tk.MustQuery("select /*+ inl_merge_join(a) */ a.* from a inner join (select 1 as k1,'k2-1' as k2) as k on a.f1=k.k1;").
		Check(testkit.Rows("1 a"))
}

func (s *testSuite3) TestSortLeftJoinWithNullColumnInRightChildPanic(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("insert into t1(a) select 1;")
	tk.MustQuery("select b.n from t1 left join (select a as a, null as n from t2) b on b.a = t1.a order by t1.a").
		Check(testkit.Rows("<nil>"))
}

func (s *testSuiteP1) TestUnionAutoSignedCast(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (id int, i int, b bigint, d double, dd decimal)")
	tk.MustExec("create table t2 (id int, i int unsigned, b bigint unsigned, d double unsigned, dd decimal unsigned)")
	tk.MustExec("insert into t1 values(1, -1, -1, -1.1, -1)")
	tk.MustExec("insert into t2 values(2, 1, 1, 1.1, 1)")
	tk.MustQuery("select * from t1 union select * from t2 order by id").
		Check(testkit.Rows("1 -1 -1 -1.1 -1", "2 1 1 1.1 1"))
	tk.MustQuery("select id, i, b, d, dd from t2 union select id, i, b, d, dd from t1 order by id").
		Check(testkit.Rows("1 0 0 0 -1", "2 1 1 1.1 1"))
	tk.MustQuery("select id, i from t2 union select id, cast(i as unsigned int) from t1 order by id").
		Check(testkit.Rows("1 18446744073709551615", "2 1"))
	tk.MustQuery("select dd from t2 union all select dd from t2").
		Check(testkit.Rows("1", "1"))

	tk.MustExec("drop table if exists t3,t4")
	tk.MustExec("create table t3 (id int, v int)")
	tk.MustExec("create table t4 (id int, v double unsigned)")
	tk.MustExec("insert into t3 values (1, -1)")
	tk.MustExec("insert into t4 values (2, 1)")
	tk.MustQuery("select id, v from t3 union select id, v from t4 order by id").
		Check(testkit.Rows("1 -1", "2 1"))
	tk.MustQuery("select id, v from t4 union select id, v from t3 order by id").
		Check(testkit.Rows("1 0", "2 1"))

	tk.MustExec("drop table if exists t5,t6,t7")
	tk.MustExec("create table t5 (id int, v bigint unsigned)")
	tk.MustExec("create table t6 (id int, v decimal)")
	tk.MustExec("create table t7 (id int, v bigint)")
	tk.MustExec("insert into t5 values (1, 1)")
	tk.MustExec("insert into t6 values (2, -1)")
	tk.MustExec("insert into t7 values (3, -1)")
	tk.MustQuery("select id, v from t5 union select id, v from t6 order by id").
		Check(testkit.Rows("1 1", "2 -1"))
	tk.MustQuery("select id, v from t5 union select id, v from t7 union select id, v from t6 order by id").
		Check(testkit.Rows("1 1", "2 -1", "3 -1"))
}

func (s *testSuiteP1) TestUpdateClustered(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	type resultChecker struct {
		check  string
		assert []string
	}

	for _, clustered := range []string{"", "clustered"} {
		tests := []struct {
			initSchema  []string
			initData    []string
			dml         string
			resultCheck []resultChecker
		}{
			{ // left join + update both + match & unmatched + pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 int, k2 int, v int)",
					fmt.Sprintf("create table b (a int not null, k1 int, k2 int, v int, primary key(k1, k2) %s)", clustered),
				},
				[]string{
					"insert into a values (1, 1, 1), (2, 2, 2)", // unmatched + matched
					"insert into b values (2, 2, 2, 2)",
				},
				"update a left join b on a.k1 = b.k1 and a.k2 = b.k2 set a.v = 20, b.v = 100, a.k1 = a.k1 + 1, b.k1 = b.k1 + 1, a.k2 = a.k2 + 2, b.k2 = b.k2 + 2",
				[]resultChecker{
					{
						"select * from b",
						[]string{"2 3 4 100"},
					},
					{
						"select * from a",
						[]string{"2 3 20", "3 4 20"},
					},
				},
			},
			{ // left join + update both + match & unmatched + pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 int, k2 int, v int)",
					fmt.Sprintf("create table b (a int not null, k1 int, k2 int, v int, primary key(k1, k2) %s)", clustered),
				},
				[]string{
					"insert into a values (1, 1, 1), (2, 2, 2)", // unmatched + matched
					"insert into b values (2, 2, 2, 2)",
				},
				"update a left join b on a.k1 = b.k1 and a.k2 = b.k2 set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, b.k1 = b.k1 + 1, b.k2 = b.k2 + 2,  a.v = 20, b.v = 100",
				[]resultChecker{
					{
						"select * from b",
						[]string{"2 3 4 100"},
					},
					{
						"select * from a",
						[]string{"2 3 20", "3 4 20"},
					},
				},
			},
			{ // left join + update both + match & unmatched + prefix pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 varchar(100), k2 varchar(100), v varchar(100))",
					fmt.Sprintf("create table b (a varchar(100) not null, k1 varchar(100), k2 varchar(100), v varchar(100), primary key(k1(1), k2(1)) %s, key kk1(k1(1), v(1)))", clustered),
				},
				[]string{
					"insert into a values ('11', '11', '11'), ('22', '22', '22')", // unmatched + matched
					"insert into b values ('22', '22', '22', '22')",
				},
				"update a left join b on a.k1 = b.k1 and a.k2 = b.k2 set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, b.k1 = b.k1 + 1, b.k2 = b.k2 + 2, a.v = 20, b.v = 100",
				[]resultChecker{
					{
						"select * from b",
						[]string{"22 23 24 100"},
					},
					{
						"select * from a",
						[]string{"12 13 20", "23 24 20"},
					},
				},
			},
			{ // right join + update both + match & unmatched + prefix pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 varchar(100), k2 varchar(100), v varchar(100))",
					fmt.Sprintf("create table b (a varchar(100) not null, k1 varchar(100), k2 varchar(100), v varchar(100), primary key(k1(1), k2(1)) %s, key kk1(k1(1), v(1)))", clustered),
				},
				[]string{
					"insert into a values ('11', '11', '11'), ('22', '22', '22')", // unmatched + matched
					"insert into b values ('22', '22', '22', '22')",
				},
				"update b right join a on a.k1 = b.k1 and a.k2 = b.k2 set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, b.k1 = b.k1 + 1, b.k2 = b.k2 + 2, a.v = 20, b.v = 100",
				[]resultChecker{
					{
						"select * from b",
						[]string{"22 23 24 100"},
					},
					{
						"select * from a",
						[]string{"12 13 20", "23 24 20"},
					},
				},
			},
			{ // inner join + update both + match & unmatched + prefix pk
				[]string{
					"drop table if exists a, b",
					"create table a (k1 varchar(100), k2 varchar(100), v varchar(100))",
					fmt.Sprintf("create table b (a varchar(100) not null, k1 varchar(100), k2 varchar(100), v varchar(100), primary key(k1(1), k2(1)) %s, key kk1(k1(1), v(1)))", clustered),
				},
				[]string{
					"insert into a values ('11', '11', '11'), ('22', '22', '22')", // unmatched + matched
					"insert into b values ('22', '22', '22', '22')",
				},
				"update b join a on a.k1 = b.k1 and a.k2 = b.k2 set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, b.k1 = b.k1 + 1, b.k2 = b.k2 + 2, a.v = 20, b.v = 100",
				[]resultChecker{
					{
						"select * from b",
						[]string{"22 23 24 100"},
					},
					{
						"select * from a",
						[]string{"11 11 11", "23 24 20"},
					},
				},
			},
			{
				[]string{
					"drop table if exists a, b",
					"create table a (k1 varchar(100), k2 varchar(100), v varchar(100))",
					fmt.Sprintf("create table b (a varchar(100) not null, k1 varchar(100), k2 varchar(100), v varchar(100), primary key(k1(1), k2(1)) %s, key kk1(k1(1), v(1)))", clustered),
				},
				[]string{
					"insert into a values ('11', '11', '11'), ('22', '22', '22')", // unmatched + matched
					"insert into b values ('22', '22', '22', '22')",
				},
				"update a set a.k1 = a.k1 + 1, a.k2 = a.k2 + 2, a.v = 20 where exists (select 1 from b where a.k1 = b.k1 and a.k2 = b.k2)",
				[]resultChecker{
					{
						"select * from b",
						[]string{"22 22 22 22"},
					},
					{
						"select * from a",
						[]string{"11 11 11", "23 24 20"},
					},
				},
			},
		}

		for _, test := range tests {
			for _, s := range test.initSchema {
				tk.MustExec(s)
			}
			for _, s := range test.initData {
				tk.MustExec(s)
			}
			tk.MustExec(test.dml)
			for _, checker := range test.resultCheck {
				tk.MustQuery(checker.check).Check(testkit.Rows(checker.assert...))
			}
			tk.MustExec("admin check table a")
			tk.MustExec("admin check table b")
		}
	}
}

func (s *testSuite6) TestUpdateJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4, t5, t6, t7")
	tk.MustExec("create table t1(k int, v int)")
	tk.MustExec("create table t2(k int, v int)")
	tk.MustExec("create table t3(id int auto_increment, k int, v int, primary key(id))")
	tk.MustExec("create table t4(k int, v int)")
	tk.MustExec("create table t5(v int, k int, primary key(k))")
	tk.MustExec("insert into t1 values (1, 1)")
	tk.MustExec("insert into t4 values (3, 3)")
	tk.MustExec("create table t6 (id int, v longtext)")
	tk.MustExec("create table t7 (x int, id int, v longtext, primary key(id))")

	// test the normal case that update one row for a single table.
	tk.MustExec("update t1 set v = 0 where k = 1")
	tk.MustQuery("select k, v from t1 where k = 1").Check(testkit.Rows("1 0"))

	// test the case that the table with auto_increment or none-null columns as the right table of left join.
	tk.MustExec("update t1 left join t3 on t1.k = t3.k set t1.v = 1")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select id, k, v from t3").Check(testkit.Rows())

	// test left join and the case that the right table has no matching record but has updated the right table columns.
	tk.MustExec("update t1 left join t2 on t1.k = t2.k set t1.v = t2.v, t2.v = 3")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test the case that the update operation in the left table references data in the right table while data of the right table columns is modified.
	tk.MustExec("update t1 left join t2 on t1.k = t2.k set t2.v = 3, t1.v = t2.v")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test right join and the case that the left table has no matching record but has updated the left table columns.
	tk.MustExec("update t2 right join t1 on t2.k = t1.k set t2.v = 4, t1.v = 0")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 0"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test the case of right join and left join at the same time.
	tk.MustExec("update t1 left join t2 on t1.k = t2.k right join t4 on t4.k = t2.k set t1.v = 4, t2.v = 4, t4.v = 4")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 0"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())
	tk.MustQuery("select k, v from t4").Check(testkit.Rows("3 4"))

	// test normal left join and the case that the right table has matching rows.
	tk.MustExec("insert t2 values (1, 10)")
	tk.MustExec("update t1 left join t2 on t1.k = t2.k set t2.v = 11")
	tk.MustQuery("select k, v from t2").Check(testkit.Rows("1 11"))

	// test the case of continuously joining the same table and updating the unmatching records.
	tk.MustExec("update t1 t11 left join t2 on t11.k = t2.k left join t1 t12 on t2.v = t12.k set t12.v = 233, t11.v = 111")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 111"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows("1 11"))

	// test the left join case that the left table has records but all records are null.
	tk.MustExec("delete from t1")
	tk.MustExec("delete from t2")
	tk.MustExec("insert into t1 values (null, null)")
	tk.MustExec("update t1 left join t2 on t1.k = t2.k set t1.v = 1")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("<nil> 1"))

	// test the case that the right table of left join has an primary key.
	tk.MustExec("insert t5 values(0, 0)")
	tk.MustExec("update t1 left join t5 on t1.k = t5.k set t1.v = 2")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("<nil> 2"))
	tk.MustQuery("select k, v from t5").Check(testkit.Rows("0 0"))

	tk.MustExec("insert into t6 values (1, NULL)")
	tk.MustExec("insert into t7 values (5, 1, 'a')")
	tk.MustExec("update t6, t7 set t6.v = t7.v where t6.id = t7.id and t7.x = 5")
	tk.MustQuery("select v from t6").Check(testkit.Rows("a"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int primary key, v int, gv int GENERATED ALWAYS AS (v * 2) STORED)")
	tk.MustExec("create table t2(id int, v int)")
	tk.MustExec("update t1 tt1 inner join (select count(t1.id) a, t1.id from t1 left join t2 on t1.id = t2.id group by t1.id) x on tt1.id = x.id set tt1.v = tt1.v + x.a")
}

func (s *testSuite3) TestMaxOneRow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`drop table if exists t2`)
	tk.MustExec(`create table t1(a double, b double);`)
	tk.MustExec(`create table t2(a double, b double);`)
	tk.MustExec(`insert into t1 values(1, 1), (2, 2), (3, 3);`)
	tk.MustExec(`insert into t2 values(0, 0);`)
	tk.MustExec(`set @@tidb_init_chunk_size=1;`)
	rs, err := tk.Exec(`select (select t1.a from t1 where t1.a > t2.a) as a from t2;`)
	c.Assert(err, IsNil)

	err = rs.Next(context.TODO(), rs.NewChunk())
	c.Assert(err.Error(), Equals, "[executor:1242]Subquery returns more than 1 row")

	c.Assert(rs.Close(), IsNil)
}

func (s *testSuiteP2) TestCurrentTimestampValueSelection(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t,t1")

	tk.MustExec("create table t (id int, t0 timestamp null default current_timestamp, t1 timestamp(1) null default current_timestamp(1), t2 timestamp(2) null default current_timestamp(2) on update current_timestamp(2))")
	tk.MustExec("insert into t (id) values (1)")
	rs := tk.MustQuery("select t0, t1, t2 from t where id = 1")
	t0 := rs.Rows()[0][0].(string)
	t1 := rs.Rows()[0][1].(string)
	t2 := rs.Rows()[0][2].(string)
	c.Assert(len(strings.Split(t0, ".")), Equals, 1)
	c.Assert(len(strings.Split(t1, ".")[1]), Equals, 1)
	c.Assert(len(strings.Split(t2, ".")[1]), Equals, 2)
	tk.MustQuery("select id from t where t0 = ?", t0).Check(testkit.Rows("1"))
	tk.MustQuery("select id from t where t1 = ?", t1).Check(testkit.Rows("1"))
	tk.MustQuery("select id from t where t2 = ?", t2).Check(testkit.Rows("1"))
	time.Sleep(time.Second)
	tk.MustExec("update t set t0 = now() where id = 1")
	rs = tk.MustQuery("select t2 from t where id = 1")
	newT2 := rs.Rows()[0][0].(string)
	c.Assert(newT2 != t2, IsTrue)

	tk.MustExec("create table t1 (id int, a timestamp, b timestamp(2), c timestamp(3))")
	tk.MustExec("insert into t1 (id, a, b, c) values (1, current_timestamp(2), current_timestamp, current_timestamp(3))")
	rs = tk.MustQuery("select a, b, c from t1 where id = 1")
	a := rs.Rows()[0][0].(string)
	b := rs.Rows()[0][1].(string)
	d := rs.Rows()[0][2].(string)
	c.Assert(len(strings.Split(a, ".")), Equals, 1)
	c.Assert(strings.Split(b, ".")[1], Equals, "00")
	c.Assert(len(strings.Split(d, ".")[1]), Equals, 3)
}

func (s *testSuite3) TestRowID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec(`create table t(a varchar(10), b varchar(10), c varchar(1), index idx(a, b, c));`)
	tk.MustExec(`insert into t values('a', 'b', 'c');`)
	tk.MustExec(`insert into t values('a', 'b', 'c');`)
	tk.MustQuery(`select b, _tidb_rowid from t use index(idx) where a = 'a';`).Check(testkit.Rows(
		`b 1`,
		`b 2`,
	))
	tk.MustExec(`begin;`)
	tk.MustExec(`select * from t for update`)
	tk.MustQuery(`select distinct b from t use index(idx) where a = 'a';`).Check(testkit.Rows(`b`))
	tk.MustExec(`commit;`)

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(5) primary key)`)
	tk.MustExec(`insert into t values('a')`)
	tk.MustQuery("select *, _tidb_rowid from t use index(`primary`) where _tidb_rowid=1").Check(testkit.Rows("a 1"))
}

func (s *testSuite3) TestDoSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int)`)
	_, err := tk.Exec(`do 1 in (select * from t)`)
	c.Assert(err, IsNil, Commentf("err %v", err))
	tk.MustExec(`insert into t values(1)`)
	r, err := tk.Exec(`do 1 in (select * from t)`)
	c.Assert(err, IsNil, Commentf("err %v", err))
	c.Assert(r, IsNil, Commentf("result of Do not empty"))
}

func (s *testSuite3) TestSubqueryTableAlias(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)

	tk.MustExec("set sql_mode = ''")
	tk.MustGetErrCode("select a, b from (select 1 a) ``, (select 2 b) ``;", mysql.ErrDerivedMustHaveAlias)
	tk.MustGetErrCode("select a, b from (select 1 a) `x`, (select 2 b) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select a, b from (select 1 a), (select 2 b);", mysql.ErrDerivedMustHaveAlias)
	// ambiguous column name
	tk.MustGetErrCode("select a from (select 1 a) ``, (select 2 a) ``;", mysql.ErrDerivedMustHaveAlias)
	tk.MustGetErrCode("select a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select x.a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select a from (select 1 a), (select 2 a);", mysql.ErrDerivedMustHaveAlias)

	tk.MustExec("set sql_mode = 'oracle';")
	tk.MustQuery("select a, b from (select 1 a) ``, (select 2 b) ``;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select a, b from (select 1 a) `x`, (select 2 b) `x`;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select a, b from (select 1 a), (select 2 b);").Check(testkit.Rows("1 2"))
	// ambiguous column name
	tk.MustGetErrCode("select a from (select 1 a) ``, (select 2 a) ``;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select x.a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select a from (select 1 a), (select 2 a);", mysql.ErrNonUniq)
}

func (s *testSerialSuite) TestTSOFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int)`)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockGetTSFail", "return"), IsNil)
	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return fpname == "github.com/pingcap/tidb/session/mockGetTSFail"
	})
	_, err := tk.Se.Execute(ctx, `select * from t`)
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockGetTSFail"), IsNil)
}

func (s *testSuite3) TestSelectHashPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists th`)
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec(`create table th (a int, b int) partition by hash(a) partitions 3;`)
	defer tk.MustExec(`drop table if exists th`)
	tk.MustExec(`insert into th values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);`)
	tk.MustExec("insert into th values (-1,-1),(-2,-2),(-3,-3),(-4,-4),(-5,-5),(-6,-6),(-7,-7),(-8,-8);")
	tk.MustQuery("select b from th order by a").Check(testkit.Rows("-8", "-7", "-6", "-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5", "6", "7", "8"))
	tk.MustQuery(" select * from th where a=-2;").Check(testkit.Rows("-2 -2"))
	tk.MustQuery(" select * from th where a=5;").Check(testkit.Rows("5 5"))
}

func (s *testSuiteP1) TestSelectPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists th, tr, tl`)
	tk.MustExec("set @@session.tidb_enable_list_partition = ON;")
	tk.MustExec(`create table th (a int, b int) partition by hash(a) partitions 3;`)
	tk.MustExec(`create table tr (a int, b int)
							partition by range (a) (
							partition r0 values less than (4),
							partition r1 values less than (7),
							partition r3 values less than maxvalue)`)
	tk.MustExec(`create table tl (a int, b int, unique index idx(a)) partition by list  (a) (
					    partition p0 values in (3,5,6,9,17),
					    partition p1 values in (1,2,10,11,19,20),
					    partition p2 values in (4,12,13,14,18),
					    partition p3 values in (7,8,15,16,null));`)
	defer tk.MustExec(`drop table if exists th, tr, tl`)
	tk.MustExec(`insert into th values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);`)
	tk.MustExec("insert into th values (-1,-1),(-2,-2),(-3,-3),(-4,-4),(-5,-5),(-6,-6),(-7,-7),(-8,-8);")
	tk.MustExec(`insert into tr values (-3,-3),(3,3),(4,4),(7,7),(8,8);`)
	tk.MustExec(`insert into tl values (3,3),(1,1),(4,4),(7,7),(8,8),(null,null);`)
	// select 1 partition.
	tk.MustQuery("select b from th partition (p0) order by a").Check(testkit.Rows("-6", "-3", "0", "3", "6"))
	tk.MustQuery("select b from tr partition (r0) order by a").Check(testkit.Rows("-3", "3"))
	tk.MustQuery("select b from tl partition (p0) order by a").Check(testkit.Rows("3"))
	tk.MustQuery("select b from th partition (p0,P0) order by a").Check(testkit.Rows("-6", "-3", "0", "3", "6"))
	tk.MustQuery("select b from tr partition (r0,R0,r0) order by a").Check(testkit.Rows("-3", "3"))
	tk.MustQuery("select b from tl partition (p0,P0,p0) order by a").Check(testkit.Rows("3"))
	// select multi partition.
	tk.MustQuery("select b from th partition (P2,p0) order by a").Check(testkit.Rows("-8", "-6", "-5", "-3", "-2", "0", "2", "3", "5", "6", "8"))
	tk.MustQuery("select b from tr partition (r1,R3) order by a").Check(testkit.Rows("4", "7", "8"))
	tk.MustQuery("select b from tl partition (p0,P3) order by a").Check(testkit.Rows("<nil>", "3", "7", "8"))

	// test select unknown partition error
	err := tk.ExecToErr("select b from th partition (p0,p4)")
	c.Assert(err.Error(), Equals, "[table:1735]Unknown partition 'p4' in table 'th'")
	err = tk.ExecToErr("select b from tr partition (r1,r4)")
	c.Assert(err.Error(), Equals, "[table:1735]Unknown partition 'r4' in table 'tr'")
	err = tk.ExecToErr("select b from tl partition (p0,p4)")
	c.Assert(err.Error(), Equals, "[table:1735]Unknown partition 'p4' in table 'tl'")

	// test select partition table in transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into th values (10,10),(11,11)")
	tk.MustQuery("select a, b from th where b>10").Check(testkit.Rows("11 11"))
	tk.MustExec("commit")
	tk.MustQuery("select a, b from th where b>10").Check(testkit.Rows("11 11"))

	// test partition function is scalar func
	tk.MustExec("drop table if exists tscalar")
	tk.MustExec(`create table tscalar (c1 int) partition by range (c1 % 30) (
								partition p0 values less than (0),
								partition p1 values less than (10),
								partition p2 values less than (20),
								partition pm values less than (maxvalue));`)
	tk.MustExec("insert into tscalar values(0), (10), (40), (50), (55)")
	// test IN expression
	tk.MustExec("insert into tscalar values(-0), (-10), (-40), (-50), (-55)")
	tk.MustQuery("select * from tscalar where c1 in (55, 55)").Check(testkit.Rows("55"))
	tk.MustQuery("select * from tscalar where c1 in (40, 40)").Check(testkit.Rows("40"))
	tk.MustQuery("select * from tscalar where c1 in (40)").Check(testkit.Rows("40"))
	tk.MustQuery("select * from tscalar where c1 in (-40)").Check(testkit.Rows("-40"))
	tk.MustQuery("select * from tscalar where c1 in (-40, -40)").Check(testkit.Rows("-40"))
	tk.MustQuery("select * from tscalar where c1 in (-1)").Check(testkit.Rows())
}

func (s *testSuiteP1) TestDeletePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1 (a int) partition by range (a) (
 partition p0 values less than (10),
 partition p1 values less than (20),
 partition p2 values less than (30),
 partition p3 values less than (40),
 partition p4 values less than MAXVALUE
 )`)
	tk.MustExec("insert into t1 values (1),(11),(21),(31)")
	tk.MustExec("delete from t1 partition (p4)")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("1", "11", "21", "31"))
	tk.MustExec("delete from t1 partition (p0) where a > 10")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("1", "11", "21", "31"))
	tk.MustExec("delete from t1 partition (p0,p1,p2)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("31"))
}

func (s *testSuite) TestSelectView(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table view_t (a int,b int)")
	tk.MustExec("insert into view_t values(1,2)")
	tk.MustExec("create definer='root'@'localhost' view view1 as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view2(c,d) as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view3(c,d) as select a,b from view_t")
	tk.MustExec("create definer='root'@'localhost' view view4 as select * from (select * from (select * from view_t) tb1) tb;")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop table view_t;")
	tk.MustExec("create table view_t(c int,d int)")
	err := tk.ExecToErr("select * from view1")
	c.Assert(err.Error(), Equals, "[planner:1356]View 'test.view1' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")
	err = tk.ExecToErr("select * from view2")
	c.Assert(err.Error(), Equals, "[planner:1356]View 'test.view2' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")
	err = tk.ExecToErr("select * from view3")
	c.Assert(err.Error(), Equals, plannercore.ErrViewInvalid.GenWithStackByArgs("test", "view3").Error())
	tk.MustExec("drop table view_t;")
	tk.MustExec("create table view_t(a int,b int,c int)")
	tk.MustExec("insert into view_t values(1,2,3)")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("alter table view_t drop column a")
	tk.MustExec("alter table view_t add column a int after b")
	tk.MustExec("update view_t set a=1;")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop table view_t;")
	tk.MustExec("drop view view1,view2,view3,view4;")

	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("create definer='root'@'localhost' view v as select a, first_value(a) over(rows between 1 preceding and 1 following), last_value(a) over(rows between 1 preceding and 1 following) from t")
	result := tk.MustQuery("select * from v")
	result.Check(testkit.Rows("1 1 1", "1 1 2", "2 1 2", "2 2 2"))
	tk.MustExec("drop view v;")
}

type testSuite2 struct {
	*baseTestSuite
}

func (s *testSuite2) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSuite3 struct {
	*baseTestSuite
}

func (s *testSuite3) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSuite4 struct {
	*baseTestSuite
}

func (s *testSuite4) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSuite5 struct {
	*baseTestSuite
}

func (s *testSuite5) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSuite6 struct {
	*baseTestSuite
}

func (s *testSuite6) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSuite7 struct {
	*baseTestSuite
}

func (s *testSuite7) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSuite8 struct {
	*baseTestSuite
}

func (s *testSuite8) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSerialSuite1 struct {
	*baseTestSuite
}

func (s *testSerialSuite1) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

func (s *testSuiteP2) TestStrToDateBuiltin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%!') from dual`).Check(testkit.Rows("2019-01-01"))
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%f') from dual`).Check(testkit.Rows("2019-01-01 00:00:00.000000"))
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%H%i%s') from dual`).Check(testkit.Rows("2019-01-01 00:00:00"))
	tk.MustQuery(`select str_to_date('18/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('a18/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('69/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('70/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("1970-10-22"))
	tk.MustQuery(`select str_to_date('8/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2008-10-22"))
	tk.MustQuery(`select str_to_date('8/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2008-10-22"))
	tk.MustQuery(`select str_to_date('18/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('a18/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('69/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('70/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("1970-10-22"))
	tk.MustQuery(`select str_to_date('018/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("0018-10-22"))
	tk.MustQuery(`select str_to_date('2018/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('018/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18/10/22','%y0/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18/10/22','%Y0/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18a/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18a/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('20188/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2018510522','%Y5%m5%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018^10^22','%Y^%m^%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018@10@22','%Y@%m@%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018%10%22','%Y%%m%%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2018(10(22','%Y(%m(%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018\10\22','%Y\%m\%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2018=10=22','%Y=%m=%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018+10+22','%Y+%m+%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('2018_10_22','%Y_%m_%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('69510522','%y5%m5%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('69^10^22','%y^%m^%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('18@10@22','%y@%m@%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('18%10%22','%y%%m%%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18(10(22','%y(%m(%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('18\10\22','%y\%m\%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18+10+22','%y+%m+%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('18=10=22','%y=%m=%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`select str_to_date('18_10_22','%y_%m_%d') from dual`).Check(testkit.Rows("2018-10-22"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 11:22:33 PM', '%Y-%m-%d %r')`).Check(testkit.Rows("2020-07-04 23:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 12:22:33 AM', '%Y-%m-%d %r')`).Check(testkit.Rows("2020-07-04 00:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 12:22:33', '%Y-%m-%d %T')`).Check(testkit.Rows("2020-07-04 12:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 00:22:33', '%Y-%m-%d %T')`).Check(testkit.Rows("2020-07-04 00:22:33"))
}

func (s *testSuiteP2) TestAddDateBuiltinWithWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@sql_mode='NO_ZERO_DATE'")
	result := tk.MustQuery(`select date_add('2001-01-00', interval -2 hour);`)
	result.Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '2001-01-00'"))
}

func (s *testSuiteP2) TestStrToDateBuiltinWithWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@sql_mode='NO_ZERO_DATE'")
	tk.MustExec("use test")
	tk.MustQuery(`SELECT STR_TO_DATE('0000-1-01', '%Y-%m-%d');`).Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1411 Incorrect datetime value: '0000-1-01' for function str_to_date"))
}

func (s *testSuiteP2) TestReadPartitionedTable(c *C) {
	// Test three reader on partitioned table.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists pt")
	tk.MustExec("create table pt (a int, b int, index i_b(b)) partition by range (a) (partition p1 values less than (2), partition p2 values less than (4), partition p3 values less than (6))")
	for i := 0; i < 6; i++ {
		tk.MustExec(fmt.Sprintf("insert into pt values(%d, %d)", i, i))
	}
	// Table reader
	tk.MustQuery("select * from pt order by a").Check(testkit.Rows("0 0", "1 1", "2 2", "3 3", "4 4", "5 5"))
	// Index reader
	tk.MustQuery("select b from pt where b = 3").Check(testkit.Rows("3"))
	// Index lookup
	tk.MustQuery("select a from pt where b = 3").Check(testkit.Rows("3"))
}

func (s *testSplitTable) TestSplitRegion(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a varchar(100),b int, index idx1(b,a))")
	tk.MustExec(`split table t index idx1 by (10000,"abcd"),(10000000);`)
	_, err := tk.Exec(`split table t index idx1 by ("abcd");`)
	c.Assert(err, NotNil)
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(mysql.WarnDataTruncated))

	// Test for split index region.
	// Check min value is more than max value.
	tk.MustExec(`split table t index idx1 between (0) and (1000000000) regions 10`)
	tk.MustGetErrCode(`split table t index idx1 between (2,'a') and (1,'c') regions 10`, errno.ErrInvalidSplitRegionRanges)

	// Check min value is invalid.
	_, err = tk.Exec(`split table t index idx1 between () and (1) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index `idx1` region lower value count should more than 0")

	// Check max value is invalid.
	_, err = tk.Exec(`split table t index idx1 between (1) and () regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index `idx1` region upper value count should more than 0")

	// Check pre-split region num is too large.
	_, err = tk.Exec(`split table t index idx1 between (0) and (1000000000) regions 10000`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index region num exceeded the limit 1000")

	// Check pre-split region num 0 is invalid.
	_, err = tk.Exec(`split table t index idx1 between (0) and (1000000000) regions 0`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index region num should more than 0")

	// Test truncate error msg.
	_, err = tk.Exec(`split table t index idx1 between ("aa") and (1000000000) regions 0`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1265]Incorrect value: 'aa' for column 'b'")

	// Test for split table region.
	tk.MustExec(`split table t between (0) and (1000000000) regions 10`)
	// Check the lower value is more than the upper value.
	tk.MustGetErrCode(`split table t between (2) and (1) regions 10`, errno.ErrInvalidSplitRegionRanges)

	// Check the lower value is invalid.
	_, err = tk.Exec(`split table t between () and (1) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split table region lower value count should be 1")

	// Check upper value is invalid.
	_, err = tk.Exec(`split table t between (1) and () regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split table region upper value count should be 1")

	// Check pre-split region num is too large.
	_, err = tk.Exec(`split table t between (0) and (1000000000) regions 10000`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split table region num exceeded the limit 1000")

	// Check pre-split region num 0 is invalid.
	_, err = tk.Exec(`split table t between (0) and (1000000000) regions 0`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split table region num should more than 0")

	// Test truncate error msg.
	_, err = tk.Exec(`split table t between ("aa") and (1000000000) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1265]Incorrect value: 'aa' for column '_tidb_rowid'")

	// Test split table region step is too small.
	tk.MustGetErrCode(`split table t between (0) and (100) regions 10`, errno.ErrInvalidSplitRegionRanges)

	// Test split region by syntax.
	tk.MustExec(`split table t by (0),(1000),(1000000)`)

	// Test split region twice to test for multiple batch split region requests.
	tk.MustExec("create table t1(a int, b int)")
	tk.MustQuery("split table t1 between(0) and (10000) regions 10;").Check(testkit.Rows("9 1"))
	tk.MustQuery("split table t1 between(10) and (10010) regions 5;").Check(testkit.Rows("4 1"))

	// Test split region for partition table.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int) partition by hash(a) partitions 5;")
	tk.MustQuery("split table t between (0) and (1000000) regions 5;").Check(testkit.Rows("20 1"))
	// Test for `split for region` syntax.
	tk.MustQuery("split region for partition table t between (1000000) and (100000000) regions 10;").Check(testkit.Rows("45 1"))

	// Test split region for partition table with specified partition.
	tk.MustQuery("split table t partition (p1,p2) between (100000000) and (1000000000) regions 5;").Check(testkit.Rows("8 1"))
	// Test for `split for region` syntax.
	tk.MustQuery("split region for partition table t partition (p3,p4) between (100000000) and (1000000000) regions 5;").Check(testkit.Rows("8 1"))
}

func (s *testSplitTable) TestSplitRegionEdgeCase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint(20) auto_increment primary key);")
	tk.MustExec("split table t between (-9223372036854775808) and (9223372036854775807) regions 16;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int(20) auto_increment primary key);")
	tk.MustGetErrCode("split table t between (-9223372036854775808) and (9223372036854775807) regions 16;", errno.ErrDataOutOfRange)
}

func (s *testSplitTable) TestClusterIndexSplitTableIntegration(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_cluster_index_index_split_table_integration;")
	tk.MustExec("create database test_cluster_index_index_split_table_integration;")
	tk.MustExec("use test_cluster_index_index_split_table_integration;")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t (a varchar(255), b double, c int, primary key (a, b));")

	// Value list length not match.
	lowerMsg := "Split table region lower value count should be 2"
	upperMsg := "Split table region upper value count should be 2"
	tk.MustGetErrMsg("split table t between ('aaa') and ('aaa', 100.0) regions 10;", lowerMsg)
	tk.MustGetErrMsg("split table t between ('aaa', 1.0) and ('aaa', 100.0, 11) regions 10;", upperMsg)

	// Value type not match.
	errMsg := "[types:1265]Incorrect value: 'aaa' for column 'b'"
	tk.MustGetErrMsg("split table t between ('aaa', 0.0) and (100.0, 'aaa') regions 10;", errMsg)

	// lower bound >= upper bound.
	errMsg = "[executor:8212]Failed to split region ranges: Split table `t` region lower value (aaa,0) should less than the upper value (aaa,0)"
	tk.MustGetErrMsg("split table t between ('aaa', 0.0) and ('aaa', 0.0) regions 10;", errMsg)
	errMsg = "[executor:8212]Failed to split region ranges: Split table `t` region lower value (bbb,0) should less than the upper value (aaa,0)"
	tk.MustGetErrMsg("split table t between ('bbb', 0.0) and ('aaa', 0.0) regions 10;", errMsg)

	// Exceed limit 1000.
	errMsg = "Split table region num exceeded the limit 1000"
	tk.MustGetErrMsg("split table t between ('aaa', 0.0) and ('aaa', 0.1) regions 100000;", errMsg)

	// Split on null values.
	errMsg = "[planner:1048]Column 'a' cannot be null"
	tk.MustGetErrMsg("split table t between (null, null) and (null, null) regions 1000;", errMsg)
	tk.MustGetErrMsg("split table t by (null, null);", errMsg)

	// Success.
	tk.MustExec("split table t between ('aaa', 0.0) and ('aaa', 100.0) regions 10;")
	tk.MustExec("split table t by ('aaa', 0.0), ('aaa', 20.0), ('aaa', 100.0);")
	tk.MustExec("split table t by ('aaa', 100.0), ('qqq', 20.0), ('zzz', 100.0), ('zzz', 1000.0);")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, c int, d int, primary key(a, c, d));")
	tk.MustQuery("split table t between (0, 0, 0) and (0, 0, 1) regions 1000;").Check(testkit.Rows("999 1"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, c int, d int, primary key(d, a, c));")
	tk.MustQuery("split table t by (0, 0, 0), (1, 2, 3), (65535, 65535, 65535);").Check(testkit.Rows("3 1"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255), b decimal, c int, primary key (a, b));")
	errMsg = "[types:1265]Incorrect value: '' for column 'b'"
	tk.MustGetErrMsg("split table t by ('aaa', '')", errMsg)
}

func (s *testSplitTable) TestClusterIndexShowTableRegion(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("set global tidb_scatter_region = 1")
	tk.MustExec("drop database if exists cluster_index_regions;")
	tk.MustExec("create database cluster_index_regions;")
	tk.MustExec("use cluster_index_regions;")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (a int, b int, c int, primary key(a, b));")
	tk.MustExec("insert t values (1, 1, 1), (2, 2, 2);")
	tk.MustQuery("split table t between (1, 0) and (2, 3) regions 2;").Check(testkit.Rows("1 1"))
	rows := tk.MustQuery("show table t regions").Rows()
	tbl := testGetTableByName(c, tk.Se, "cluster_index_regions", "t")
	// Check the region start key.
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_r_03800000000000000183800000000000", tbl.Meta().ID))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustQuery("split table t between (0) and (100000) regions 2;").Check(testkit.Rows("1 1"))
	rows = tk.MustQuery("show table t regions").Rows()
	tbl = testGetTableByName(c, tk.Se, "cluster_index_regions", "t")
	// Check the region start key is int64.
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_r_50000", tbl.Meta().ID))
}

func (s *testSuiteWithData) TestClusterIndexOuterJoinElimination(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (a int, b int, c int, primary key(a,b))")
	rows := tk.MustQuery(`explain format = 'brief' select t1.a from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b`).Rows()
	rowStrs := s.testData.ConvertRowsToStrings(rows)
	for _, row := range rowStrs {
		// outer join has been eliminated.
		c.Assert(strings.Index(row, "Join"), Equals, -1)
	}
}

func (s *testSplitTable) TestShowTableRegion(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_regions")
	tk.MustExec("set global tidb_scatter_region = 1")
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("create table t_regions (a int key, b int, c int, index idx(b), index idx2(c))")
	_, err := tk.Exec("split partition table t_regions partition (p1,p2) index idx between (0) and (20000) regions 2;")
	c.Assert(err.Error(), Equals, plannercore.ErrPartitionClauseOnNonpartitioned.Error())

	// Test show table regions.
	tk.MustQuery(`split table t_regions between (-10000) and (10000) regions 4;`).Check(testkit.Rows("4 1"))
	re := tk.MustQuery("show table t_regions regions")
	rows := re.Rows()
	// Table t_regions should have 5 regions now.
	// 4 regions to store record data.
	// 1 region to store index data.
	c.Assert(len(rows), Equals, 5)
	c.Assert(len(rows[0]), Equals, 11)
	tbl := testGetTableByName(c, tk.Se, "test", "t_regions")
	// Check the region start key.
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_-5000", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_0", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID))
	c.Assert(rows[4][2], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))

	// Test show table index regions.
	tk.MustQuery(`split table t_regions index idx between (-1000) and (1000) regions 4;`).Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t_regions index idx regions")
	rows = re.Rows()
	// The index `idx` of table t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	// Check the region start key.
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d.*", tbl.Meta().ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))

	re = tk.MustQuery("show table t_regions regions")
	rows = re.Rows()
	// The index `idx` of table t_regions should have 9 regions now.
	// 4 regions to store record data.
	// 4 region to store index idx data.
	// 1 region to store index idx2 data.
	c.Assert(len(rows), Equals, 9)
	// Check the region start key.
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_-5000", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_0", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID))
	c.Assert(rows[4][1], Matches, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[7][2], Equals, fmt.Sprintf("t_%d_i_2_", tbl.Meta().ID))
	c.Assert(rows[8][2], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))

	// Test unsigned primary key and wait scatter finish.
	tk.MustExec("drop table if exists t_regions")
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("create table t_regions (a int unsigned key, b int, index idx(b))")

	// Test show table regions.
	tk.MustExec(`set @@session.tidb_wait_split_region_finish=1;`)
	tk.MustQuery(`split table t_regions by (2500),(5000),(7500);`).Check(testkit.Rows("3 1"))
	re = tk.MustQuery("show table t_regions regions")
	rows = re.Rows()
	// Table t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	tbl = testGetTableByName(c, tk.Se, "test", "t_regions")
	// Check the region start key.
	c.Assert(rows[0][1], Matches, "t_.*")
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_2500", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_7500", tbl.Meta().ID))

	// Test show table index regions.
	tk.MustQuery(`split table t_regions index idx by (250),(500),(750);`).Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t_regions index idx regions")
	rows = re.Rows()
	// The index `idx` of table t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	// Check the region start key.
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))

	// Test show table regions for partition table when disable split region when create table.
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
	tk.MustExec("drop table if exists partition_t;")
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec("create table partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	re = tk.MustQuery("show table partition_t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Matches, "t_.*")

	// Test show table regions for partition table when enable split region when create table.
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	tk.MustExec("drop table if exists partition_t;")
	tk.MustExec("create table partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	re = tk.MustQuery("show table partition_t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 3)
	tbl = testGetTableByName(c, tk.Se, "test", "partition_t")
	partitionDef := tbl.Meta().GetPartitionInfo().Definitions
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[0].ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[1].ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[2].ID))

	// Test split partition region when add new partition.
	tk.MustExec("drop table if exists partition_t;")
	tk.MustExec(`create table partition_t (a int, b int,index(a)) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20),
		PARTITION p2 VALUES LESS THAN (30));`)
	tk.MustExec(`alter table partition_t add partition ( partition p3 values less than (40), partition p4 values less than (50) );`)
	re = tk.MustQuery("show table partition_t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 5)
	tbl = testGetTableByName(c, tk.Se, "test", "partition_t")
	partitionDef = tbl.Meta().GetPartitionInfo().Definitions
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[0].ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[1].ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[2].ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[3].ID))
	c.Assert(rows[4][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[4].ID))

	// Test pre-split table region when create table.
	tk.MustExec("drop table if exists t_pre")
	tk.MustExec("create table t_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2;")
	re = tk.MustQuery("show table t_pre regions")
	rows = re.Rows()
	// Table t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	tbl = testGetTableByName(c, tk.Se, "test", "t_pre")
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID))

	// Test pre-split table region when create table.
	tk.MustExec("drop table if exists pt_pre")
	tk.MustExec("create table pt_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2 partition by hash(a) partitions 3;")
	re = tk.MustQuery("show table pt_pre regions")
	rows = re.Rows()
	// Table t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 12)
	tbl = testGetTableByName(c, tk.Se, "test", "pt_pre")
	pi := tbl.Meta().GetPartitionInfo().Definitions
	c.Assert(len(pi), Equals, 3)
	for i, p := range pi {
		c.Assert(rows[1+4*i][1], Equals, fmt.Sprintf("t_%d_r_2305843009213693952", p.ID))
		c.Assert(rows[2+4*i][1], Equals, fmt.Sprintf("t_%d_r_4611686018427387904", p.ID))
		c.Assert(rows[3+4*i][1], Equals, fmt.Sprintf("t_%d_r_6917529027641081856", p.ID))
	}

	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)

	// Test split partition table.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int) partition by hash(a) partitions 5;")
	tk.MustQuery("split table t between (0) and (4000000) regions 4;").Check(testkit.Rows("15 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 20)
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(len(tbl.Meta().GetPartitionInfo().Definitions), Equals, 5)
	for i, p := range tbl.Meta().GetPartitionInfo().Definitions {
		c.Assert(rows[i*4+0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*4+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*4+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*4+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	}

	// Test split region for partition table with specified partition.
	tk.MustQuery("split table t partition (p4) between (1000000) and (2000000) regions 5;").Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 24)
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(len(tbl.Meta().GetPartitionInfo().Definitions), Equals, 5)
	for i := 0; i < 4; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*4+0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*4+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*4+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*4+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	}
	for i := 4; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*4+0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*4+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*4+2][1], Equals, fmt.Sprintf("t_%d_r_1200000", p.ID))
		c.Assert(rows[i*4+3][1], Equals, fmt.Sprintf("t_%d_r_1400000", p.ID))
		c.Assert(rows[i*4+4][1], Equals, fmt.Sprintf("t_%d_r_1600000", p.ID))
		c.Assert(rows[i*4+5][1], Equals, fmt.Sprintf("t_%d_r_1800000", p.ID))
		c.Assert(rows[i*4+6][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*4+7][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	}

	// Test for show table partition regions.
	for i := 0; i < 4; i++ {
		re = tk.MustQuery(fmt.Sprintf("show table t partition (p%v) regions", i))
		rows = re.Rows()
		c.Assert(len(rows), Equals, 4)
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	}
	re = tk.MustQuery("show table t partition (p0, p4) regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 12)
	p := tbl.Meta().GetPartitionInfo().Definitions[0]
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	p = tbl.Meta().GetPartitionInfo().Definitions[4]
	c.Assert(rows[4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[5][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
	c.Assert(rows[6][1], Equals, fmt.Sprintf("t_%d_r_1200000", p.ID))
	c.Assert(rows[7][1], Equals, fmt.Sprintf("t_%d_r_1400000", p.ID))
	c.Assert(rows[8][1], Equals, fmt.Sprintf("t_%d_r_1600000", p.ID))
	c.Assert(rows[9][1], Equals, fmt.Sprintf("t_%d_r_1800000", p.ID))
	c.Assert(rows[10][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
	c.Assert(rows[11][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	// Test for duplicate partition names.
	re = tk.MustQuery("show table t partition (p0, p0, p0) regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 4)
	p = tbl.Meta().GetPartitionInfo().Definitions[0]
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))

	// Test split partition table index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int,index idx(a)) partition by hash(a) partitions 5;")
	tk.MustQuery("split table t between (0) and (4000000) regions 4;").Check(testkit.Rows("20 1"))
	tk.MustQuery("split table t index idx between (0) and (4000000) regions 4;").Check(testkit.Rows("20 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 40)
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(len(tbl.Meta().GetPartitionInfo().Definitions), Equals, 5)
	for i := 0; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*8+0][1], Equals, fmt.Sprintf("t_%d_r", p.ID))
		c.Assert(rows[i*8+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*8+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*8+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*8+5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}

	// Test split index region for partition table with specified partition.
	tk.MustQuery("split table t partition (p4) index idx between (0) and (1000000) regions 5;").Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 44)
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(len(tbl.Meta().GetPartitionInfo().Definitions), Equals, 5)
	for i := 0; i < 4; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*8+0][1], Equals, fmt.Sprintf("t_%d_r", p.ID))
		c.Assert(rows[i*8+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*8+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*8+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*8+5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}
	for i := 4; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*8+0][1], Equals, fmt.Sprintf("t_%d_r", p.ID))
		c.Assert(rows[i*8+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*8+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*8+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*8+5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+8][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+9][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+10][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+11][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}

	// Test show table partition region on unknown-partition.
	err = tk.QueryToErr("show table t partition (p_unknown) index idx regions")
	c.Assert(terror.ErrorEqual(err, table.ErrUnknownPartition), IsTrue)

	// Test show table partition index.
	for i := 0; i < 4; i++ {
		re = tk.MustQuery(fmt.Sprintf("show table t partition (p%v) index idx regions", i))
		rows = re.Rows()
		c.Assert(len(rows), Equals, 4)
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}
	re = tk.MustQuery("show table t partition (p3,p4) index idx regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 12)
	p = tbl.Meta().GetPartitionInfo().Definitions[3]
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	p = tbl.Meta().GetPartitionInfo().Definitions[4]
	c.Assert(rows[4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[8][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[9][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[10][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[11][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))

	// Test split for the second index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int,index idx(a), index idx2(b))")
	tk.MustQuery("split table t index idx2 between (0) and (4000000) regions 2;").Check(testkit.Rows("3 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 4)
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_i_3_", tbl.Meta().ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_2_.*", tbl.Meta().ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_2_.*", tbl.Meta().ID))

	// Test show table partition region on non-partition table.
	err = tk.QueryToErr("show table t partition (p3,p4) index idx regions")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrPartitionClauseOnNonpartitioned), IsTrue)
}

func testGetTableByName(c *C, ctx sessionctx.Context, db, table string) table.Table {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	c.Assert(err, IsNil)
	return tbl
}

func (s *testSuiteP2) TestIssue10435(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(i int, j int, k int)")
	tk.MustExec("insert into t1 VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4)")
	tk.MustExec("INSERT INTO t1 SELECT 10*i,j,5*j FROM t1 UNION SELECT 20*i,j,5*j FROM t1 UNION SELECT 30*i,j,5*j FROM t1")

	tk.MustExec("set @@session.tidb_enable_window_function=1")
	tk.MustQuery("SELECT SUM(i) OVER W FROM t1 WINDOW w AS (PARTITION BY j ORDER BY i) ORDER BY 1+SUM(i) OVER w").Check(
		testkit.Rows("1", "2", "3", "4", "11", "22", "31", "33", "44", "61", "62", "93", "122", "124", "183", "244"),
	)
}

func (s *testSuiteP2) TestUnsignedFeedback(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	oriProbability := statistics.FeedbackProbability.Load()
	statistics.FeedbackProbability.Store(1.0)
	defer func() { statistics.FeedbackProbability.Store(oriProbability) }()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned, b int, primary key(a))")
	tk.MustExec("insert into t values (1,1),(2,2)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(distinct b) from t").Check(testkit.Rows("2"))
	result := tk.MustQuery("explain analyze select count(distinct b) from t")
	c.Assert(result.Rows()[2][4], Equals, "table:t")
	c.Assert(result.Rows()[2][6], Equals, "range:[0,+inf], keep order:false")
}

func (s *testSuiteP2) TestIssue23567(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	oriProbability := statistics.FeedbackProbability.Load()
	statistics.FeedbackProbability.Store(1.0)
	defer func() { statistics.FeedbackProbability.Store(oriProbability) }()
	failpoint.Enable("github.com/pingcap/tidb/statistics/feedbackNoNDVCollect", `return("")`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned, b int, primary key(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("analyze table t")
	// The SQL should not panic.
	tk.MustQuery("select count(distinct b) from t")
	failpoint.Disable("github.com/pingcap/tidb/statistics/feedbackNoNDVCollect")
}

func (s *testSuite) TestSummaryFailedUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int as(-a))")
	tk.MustExec("insert into t(a) values(1), (3), (7)")
	sm := &mockSessionManager1{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Se.SetSessionManager(sm)
	s.domain.ExpensiveQueryHandle().SetSessionManager(sm)
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionCancel
	})
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("set @@tidb_mem_quota_query=1")
	err := tk.ExecToErr("update t set t.a = t.a - 1 where t.a in (select a from t where a < 4)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
	tk.MustExec("set @@tidb_mem_quota_query=1000000000")
	tk.MustQuery("select stmt_type from information_schema.statements_summary where digest_text = 'update `t` set `t` . `a` = `t` . `a` - ? where `t` . `a` in ( select `a` from `t` where `a` < ? )'").Check(testkit.Rows("Update"))
}

func (s *testSuite) TestOOMPanicAction(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b double);")
	tk.MustExec("insert into t values (1,1)")
	sm := &mockSessionManager1{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Se.SetSessionManager(sm)
	s.domain.ExpensiveQueryHandle().SetSessionManager(sm)
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionCancel
	})
	tk.MustExec("set @@tidb_mem_quota_query=1;")
	err := tk.QueryToErr("select sum(b) from t group by a;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")

	// Test insert from select oom panic.
	tk.MustExec("drop table if exists t,t1")
	tk.MustExec("create table t (a bigint);")
	tk.MustExec("create table t1 (a bigint);")
	tk.MustExec("set @@tidb_mem_quota_query=200;")
	_, err = tk.Exec("insert into t1 values (1),(2),(3),(4),(5);")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
	_, err = tk.Exec("replace into t1 values (1),(2),(3),(4),(5);")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
	tk.MustExec("set @@tidb_mem_quota_query=10000")
	tk.MustExec("insert into t1 values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@tidb_mem_quota_query=10;")
	_, err = tk.Exec("insert into t select a from t1 order by a desc;")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
	_, err = tk.Exec("replace into t select a from t1 order by a desc;")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")

	tk.MustExec("set @@tidb_mem_quota_query=10000")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5);")
	// Set the memory quota to 244 to make this SQL panic during the DeleteExec
	// instead of the TableReaderExec.
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	_, err = tk.Exec("delete from t")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")

	tk.MustExec("set @@tidb_mem_quota_query=10000;")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	_, err = tk.Exec("delete t, t1 from t join t1 on t.a = t1.a")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")

	tk.MustExec("set @@tidb_mem_quota_query=100000;")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1),(2),(3)")
	// set the memory to quota to make the SQL panic during UpdateExec instead
	// of TableReader.
	tk.MustExec("set @@tidb_mem_quota_query=244;")
	_, err = tk.Exec("update t set a = 4")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
}

type testRecoverTable struct {
	store   kv.Storage
	dom     *domain.Domain
	cluster cluster.Cluster
	cli     *regionProperityClient
}

func (s *testRecoverTable) SetUpSuite(c *C) {
	cli := &regionProperityClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testRecoverTable) TearDownSuite(c *C) {
	s.store.Close()
	s.dom.Close()
}

func (s *testRecoverTable) mockGC(tk *testkit.TestKit) (string, string, string, func()) {
	originGC := ddl.IsEmulatorGCEnable()
	resetGC := func() {
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	ddl.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	timeAfterDrop := time.Now().Add(48 * 60 * 60 * time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")
	return timeBeforeDrop, timeAfterDrop, safePointSQL, resetGC
}

func (s *testRecoverTable) TestRecoverTable(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange")
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int);")

	timeBeforeDrop, timeAfterDrop, safePointSQL, resetGC := s.mockGC(tk)
	defer resetGC()

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	// if GC safe point is not exists in mysql.tidb
	_, err := tk.Exec("recover table t_recover")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "can not get 'tikv_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// Should recover, and we can drop it straight away.
	tk.MustExec("recover table t_recover")
	tk.MustExec("drop table t_recover")

	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeAfterDrop))
	_, err = tk.Exec("recover table t_recover")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "Can't find dropped/truncated table 't_recover' in GC safe point"), Equals, true)

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// if there is a new table with the same name, should return failed.
	tk.MustExec("create table t_recover (a int);")
	_, err = tk.Exec("recover table t_recover")
	c.Assert(err.Error(), Equals, infoschema.ErrTableExists.GenWithStackByArgs("t_recover").Error())

	// drop the new table with the same name, then recover table.
	tk.MustExec("rename table t_recover to t_recover2")

	// do recover table.
	tk.MustExec("recover table t_recover")

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_recover;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003"))

	// recover table by none exits job.
	_, err = tk.Exec(fmt.Sprintf("recover table by job %d", 10000000))
	c.Assert(err, NotNil)

	// Disable GC by manual first, then after recover table, the GC enable status should also be disabled.
	err = gcutil.DisableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop table t_recover")

	tk.MustExec("recover table t_recover")

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9"))

	// Recover truncate table.
	tk.MustExec("truncate table t_recover")
	tk.MustExec("rename table t_recover to t_recover_new")
	tk.MustExec("recover table t_recover")
	tk.MustExec("insert into t_recover values (10)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9", "10"))

	// Test for recover one table multiple time.
	tk.MustExec("drop table t_recover")
	tk.MustExec("flashback table t_recover to t_recover_tmp")
	_, err = tk.Exec("recover table t_recover")
	c.Assert(infoschema.ErrTableExists.Equal(err), IsTrue)

	gcEnable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(gcEnable, Equals, false)
}

func (s *testRecoverTable) TestFlashbackTable(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_flashback")
	tk.MustExec("use test_flashback")
	tk.MustExec("drop table if exists t_flashback")
	tk.MustExec("create table t_flashback (a int);")

	timeBeforeDrop, _, safePointSQL, resetGC := s.mockGC(tk)
	defer resetGC()

	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// Set GC enable.
	err := gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("insert into t_flashback values (1),(2),(3)")
	tk.MustExec("drop table t_flashback")

	// Test flash table with not_exist_table_name name.
	_, err = tk.Exec("flashback table t_not_exists")
	c.Assert(err.Error(), Equals, "Can't find dropped/truncated table: t_not_exists in DDL history jobs")

	// Test flashback table failed by there is already a new table with the same name.
	// If there is a new table with the same name, should return failed.
	tk.MustExec("create table t_flashback (a int);")
	_, err = tk.Exec("flashback table t_flashback")
	c.Assert(err.Error(), Equals, infoschema.ErrTableExists.GenWithStackByArgs("t_flashback").Error())

	// Drop the new table with the same name, then flashback table.
	tk.MustExec("rename table t_flashback to t_flashback_tmp")

	// Test for flashback table.
	tk.MustExec("flashback table t_flashback")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_flashback;").Check(testkit.Rows("1", "2", "3"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_flashback values (4),(5),(6)")
	tk.MustQuery("select * from t_flashback;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// Check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_flashback;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003"))

	// Test for flashback to new table.
	tk.MustExec("drop table t_flashback")
	tk.MustExec("create table t_flashback (a int);")
	tk.MustExec("flashback table t_flashback to t_flashback2")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_flashback2;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_flashback2 values (7),(8),(9)")
	tk.MustQuery("select * from t_flashback2;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	// Check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_flashback2;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003", "7 10001", "8 10002", "9 10003"))

	// Test for flashback one table multiple time.
	_, err = tk.Exec("flashback table t_flashback to t_flashback4")
	c.Assert(infoschema.ErrTableExists.Equal(err), IsTrue)

	// Test for flashback truncated table to new table.
	tk.MustExec("truncate table t_flashback2")
	tk.MustExec("flashback table t_flashback2 to t_flashback3")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_flashback3;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_flashback3 values (10),(11)")
	tk.MustQuery("select * from t_flashback3;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"))
	// Check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_flashback3;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003", "7 10001", "8 10002", "9 10003", "10 15001", "11 15002"))

	// Test for flashback drop partition table.
	tk.MustExec("drop table if exists t_p_flashback")
	tk.MustExec("create table t_p_flashback (a int) partition by hash(a) partitions 4;")
	tk.MustExec("insert into t_p_flashback values (1),(2),(3)")
	tk.MustExec("drop table t_p_flashback")
	tk.MustExec("flashback table t_p_flashback")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_p_flashback order by a;").Check(testkit.Rows("1", "2", "3"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_p_flashback values (4),(5)")
	tk.MustQuery("select a,_tidb_rowid from t_p_flashback order by a;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002"))

	// Test for flashback truncate partition table.
	tk.MustExec("truncate table t_p_flashback")
	tk.MustExec("flashback table t_p_flashback to t_p_flashback1")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_p_flashback1 order by a;").Check(testkit.Rows("1", "2", "3", "4", "5"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_p_flashback1 values (6)")
	tk.MustQuery("select a,_tidb_rowid from t_p_flashback1 order by a;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 10001"))

	tk.MustExec("drop database if exists Test2")
	tk.MustExec("create database Test2")
	tk.MustExec("use Test2")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1),(2)")
	tk.MustExec("drop table t")
	tk.MustExec("flashback table t")
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table t")
	tk.MustExec("drop database if exists Test3")
	tk.MustExec("create database Test3")
	tk.MustExec("use Test3")
	tk.MustExec("create table t (a int);")
	tk.MustExec("drop table t")
	tk.MustExec("drop database Test3")
	tk.MustExec("use Test2")
	tk.MustExec("flashback table t")
	tk.MustExec("insert into t values (3)")
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1", "2", "3"))
}

func (s *testRecoverTable) TestRecoverTempTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create global temporary table t_recover (a int) on commit delete rows;")

	timeBeforeDrop, _, safePointSQL, resetGC := s.mockGC(tk)
	defer resetGC()
	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	tk.MustExec("drop table t_recover")
	tk.MustGetErrCode("recover table t_recover;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("flashback table t_recover;", errno.ErrUnsupportedDDLOperation)
}

func (s *testSuiteP2) TestPointGetPreparedPlan(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("drop database if exists ps_text")
	defer tk1.MustExec("drop database if exists ps_text")
	tk1.MustExec("create database ps_text")
	tk1.MustExec("use ps_text")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk1.Se.PrepareStmt("select * from t where a = ?")
	c.Assert(err, IsNil)
	tk1.Se.GetSessionVars().PreparedStmts[pspk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	pspk2Id, _, _, err := tk1.Se.PrepareStmt("select * from t where ? = a ")
	c.Assert(err, IsNil)
	tk1.Se.GetSessionVars().PreparedStmts[pspk2Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3"))

	// unique index
	psuk1Id, _, _, err := tk1.Se.PrepareStmt("select * from t where b = ? ")
	c.Assert(err, IsNil)
	tk1.Se.GetSessionVars().PreparedStmts[psuk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	// test schema changed, cached plan should be invalidated
	tk1.MustExec("alter table t add column col4 int default 10 after c")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	tk1.MustExec("alter table t drop index k_b")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	tk1.MustExec(`insert into t values(4, 3, 3, 11)`)
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10", "4 3 3 11"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	tk1.MustExec("delete from t where a = 4")
	tk1.MustExec("alter table t add index k_b(b)")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Datum{types.NewDatum(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	// use pk again
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))
}

func (s *testSuiteP2) TestPointGetPreparedPlanWithCommitMode(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("drop database if exists ps_text")
	defer tk1.MustExec("drop database if exists ps_text")
	tk1.MustExec("create database ps_text")
	tk1.MustExec("use ps_text")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk1.Se.PrepareStmt("select * from t where a = ?")
	c.Assert(err, IsNil)
	tk1.Se.GetSessionVars().PreparedStmts[pspk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// update rows
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use ps_text")
	tk2.MustExec("update t set c = c + 10 where c = 1")

	// try to point get again
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// try to update in session 1
	tk1.MustExec("update t set c = c + 10 where c = 1")
	_, err = tk1.Exec("commit")
	c.Assert(kv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))

	// verify
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 11"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Datum{types.NewDatum(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2"))

	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 11"))
}

func (s *testSuiteP2) TestPointUpdatePreparedPlan(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("drop database if exists pu_test")
	defer tk1.MustExec("drop database if exists pu_test")
	tk1.MustExec("create database pu_test")
	tk1.MustExec("use pu_test")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	updateID1, pc, _, err := tk1.Se.PrepareStmt(`update t set c = c + 1 where a = ?`)
	c.Assert(err, IsNil)
	tk1.Se.GetSessionVars().PreparedStmts[updateID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	c.Assert(pc, Equals, 1)
	updateID2, pc, _, err := tk1.Se.PrepareStmt(`update t set c = c + 2 where ? = a`)
	c.Assert(err, IsNil)
	tk1.Se.GetSessionVars().PreparedStmts[updateID2].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	c.Assert(pc, Equals, 1)

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	// using the generated plan but with different params
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// updateID2
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID2, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 8"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID2, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	// unique index
	updUkID1, _, _, err := tk1.Se.PrepareStmt(`update t set c = c + 10 where b = ?`)
	c.Assert(err, IsNil)
	tk1.Se.GetSessionVars().PreparedStmts[updUkID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 20"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 30"))

	// test schema changed, cached plan should be invalidated
	tk1.MustExec("alter table t add column col4 int default 10 after c")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 31 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 32 10"))

	tk1.MustExec("alter table t drop index k_b")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 42 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 52 10"))

	tk1.MustExec("alter table t add unique index k_b(b)")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 62 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updUkID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 72 10"))

	tk1.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1 10"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2 10"))
}

func (s *testSuiteP2) TestPointUpdatePreparedPlanWithCommitMode(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("drop database if exists pu_test2")
	defer tk1.MustExec("drop database if exists pu_test2")
	tk1.MustExec("create database pu_test2")
	tk1.MustExec("use pu_test2")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	ctx := context.Background()
	updateID1, _, _, err := tk1.Se.PrepareStmt(`update t set c = c + 1 where a = ?`)
	tk1.Se.GetSessionVars().PreparedStmts[updateID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	c.Assert(err, IsNil)

	// first time plan generated
	rs, err := tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// update rows
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use pu_test2")
	tk2.MustExec(`prepare pu2 from "update t set c = c + 2 where ? = a "`)
	tk2.MustExec("set @p3 = 3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))
	tk2.MustExec("execute pu2 using @p3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 7"))
	tk2.MustExec("execute pu2 using @p3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))

	// try to update in session 1
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))
	_, err = tk1.Exec("commit")
	c.Assert(kv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))

	// verify
	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2"))
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2"))
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))

	// again next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, updateID1, []types.Datum{types.NewDatum(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
	tk1.MustExec("commit")

	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
}

func (s *testSuite1) TestPartitionHashCode(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`create table t(c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by hash (c1) partitions 4;`)
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk1 := testkit.NewTestKitWithInit(c, s.store)
			for i := 0; i < 5; i++ {
				tk1.MustExec("select * from t")
			}
		}()
	}
	wg.Wait()
}

func (s *testSuite1) TestAlterDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(a int, primary key(a))")
	tk.MustExec("insert into t(a) values(1)")
	tk.MustExec("alter table t add column b int default 1")
	tk.MustExec("alter table t alter b set default 2")
	tk.MustQuery("select b from t where a = 1").Check(testkit.Rows("1"))
}

type testClusterTableSuite struct {
	testSuiteWithCliBase
	rpcserver  *grpc.Server
	listenAddr string
}

func (s *testClusterTableSuite) SetUpSuite(c *C) {
	s.testSuiteWithCliBase.SetUpSuite(c)
	s.rpcserver, s.listenAddr = s.setUpRPCService(c, "127.0.0.1:0")
}

func (s *testClusterTableSuite) setUpRPCService(c *C, addr string) (*grpc.Server, string) {
	sm := &mockSessionManager1{}
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
	})
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	srv := server.NewRPCServer(config.GetGlobalConfig(), s.dom, sm)
	port := lis.Addr().(*net.TCPAddr).Port
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	go func() {
		err = srv.Serve(lis)
		c.Assert(err, IsNil)
	}()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
	})
	return srv, addr
}
func (s *testClusterTableSuite) TearDownSuite(c *C) {
	if s.rpcserver != nil {
		s.rpcserver.Stop()
		s.rpcserver = nil
	}
	s.testSuiteWithCliBase.TearDownSuite(c)
}

func (s *testSuiteP1) TestPrepareLoadData(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustGetErrCode(`prepare stmt from "load data local infile '/tmp/load_data_test.csv' into table test";`, mysql.ErrUnsupportedPs)
}

func (s *testClusterTableSuite) TestSlowQuery(c *C) {
	logData0 := ""
	logData1 := `
# Time: 2020-02-15T18:00:01.000000+08:00
select 1;
# Time: 2020-02-15T19:00:05.000000+08:00
select 2;`
	logData2 := `
# Time: 2020-02-16T18:00:01.000000+08:00
select 3;
# Time: 2020-02-16T18:00:05.000000+08:00
select 4;`
	logData3 := `
# Time: 2020-02-16T19:00:00.000000+08:00
select 5;
# Time: 2020-02-17T18:00:05.000000+08:00
select 6;`
	logData4 := `
# Time: 2020-05-14T19:03:54.314615176+08:00
select 7;`
	logData := []string{logData0, logData1, logData2, logData3, logData4}

	fileName0 := "tidb-slow-2020-02-14T19-04-05.01.log"
	fileName1 := "tidb-slow-2020-02-15T19-04-05.01.log"
	fileName2 := "tidb-slow-2020-02-16T19-04-05.01.log"
	fileName3 := "tidb-slow-2020-02-17T18-00-05.01.log"
	fileName4 := "tidb-slow.log"
	fileNames := []string{fileName0, fileName1, fileName2, fileName3, fileName4}

	prepareLogs(c, logData, fileNames)
	defer func() {
		removeFiles(fileNames)
	}()
	tk := testkit.NewTestKitWithInit(c, s.store)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	tk.Se.GetSessionVars().TimeZone = loc
	tk.MustExec("use information_schema")
	cases := []struct {
		prepareSQL string
		sql        string
		result     []string
	}{
		{
			sql:    "select count(*),min(time),max(time) from %s where time > '2019-01-26 21:51:00' and time < now()",
			result: []string{"7|2020-02-15 18:00:01.000000|2020-05-14 19:03:54.314615"},
		},
		{
			sql:    "select count(*),min(time),max(time) from %s where time > '2020-02-15 19:00:00' and time < '2020-02-16 18:00:02'",
			result: []string{"2|2020-02-15 19:00:05.000000|2020-02-16 18:00:01.000000"},
		},
		{
			sql:    "select count(*),min(time),max(time) from %s where time > '2020-02-16 18:00:02' and time < '2020-02-17 17:00:00'",
			result: []string{"2|2020-02-16 18:00:05.000000|2020-02-16 19:00:00.000000"},
		},
		{
			sql:    "select count(*),min(time),max(time) from %s where time > '2020-02-16 18:00:02' and time < '2020-02-17 20:00:00'",
			result: []string{"3|2020-02-16 18:00:05.000000|2020-02-17 18:00:05.000000"},
		},
		{
			sql:    "select count(*),min(time),max(time) from %s",
			result: []string{"1|2020-05-14 19:03:54.314615|2020-05-14 19:03:54.314615"},
		},
		{
			sql:    "select count(*),min(time) from %s where time > '2020-02-16 20:00:00'",
			result: []string{"1|2020-02-17 18:00:05.000000"},
		},
		{
			sql:    "select count(*) from %s where time > '2020-02-17 20:00:00'",
			result: []string{"0"},
		},
		{
			sql:    "select query from %s where time > '2019-01-26 21:51:00' and time < now()",
			result: []string{"select 1;", "select 2;", "select 3;", "select 4;", "select 5;", "select 6;", "select 7;"},
		},
		// Test for different timezone.
		{
			prepareSQL: "set @@time_zone = '+00:00'",
			sql:        "select time from %s where time = '2020-02-17 10:00:05.000000'",
			result:     []string{"2020-02-17 10:00:05.000000"},
		},
		{
			prepareSQL: "set @@time_zone = '+02:00'",
			sql:        "select time from %s where time = '2020-02-17 12:00:05.000000'",
			result:     []string{"2020-02-17 12:00:05.000000"},
		},
		// Test for issue 17224
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from %s where time = '2020-05-14 19:03:54.314615'",
			result:     []string{"2020-05-14 19:03:54.314615"},
		},
	}
	for _, cas := range cases {
		if len(cas.prepareSQL) > 0 {
			tk.MustExec(cas.prepareSQL)
		}
		sql := fmt.Sprintf(cas.sql, "slow_query")
		tk.MustQuery(sql).Check(testutil.RowsWithSep("|", cas.result...))
		sql = fmt.Sprintf(cas.sql, "cluster_slow_query")
		tk.MustQuery(sql).Check(testutil.RowsWithSep("|", cas.result...))
	}
}

func (s *testClusterTableSuite) TestIssue20236(c *C) {
	logData0 := ""
	logData1 := `
# Time: 2020-02-15T18:00:01.000000+08:00
select 1;
# Time: 2020-02-15T19:00:05.000000+08:00
select 2;
# Time: 2020-02-15T20:00:05.000000+08:00`
	logData2 := `select 3;
# Time: 2020-02-16T18:00:01.000000+08:00
select 4;
# Time: 2020-02-16T18:00:05.000000+08:00
select 5;`
	logData3 := `
# Time: 2020-02-16T19:00:00.000000+08:00
select 6;
# Time: 2020-02-17T18:00:05.000000+08:00
select 7;
# Time: 2020-02-17T19:00:00.000000+08:00`
	logData4 := `select 8;
# Time: 2020-02-17T20:00:00.000000+08:00
select 9
# Time: 2020-05-14T19:03:54.314615176+08:00
select 10;`
	logData := []string{logData0, logData1, logData2, logData3, logData4}

	fileName0 := "tidb-slow-2020-02-14T19-04-05.01.log"
	fileName1 := "tidb-slow-2020-02-15T19-04-05.01.log"
	fileName2 := "tidb-slow-2020-02-16T19-04-05.01.log"
	fileName3 := "tidb-slow-2020-02-17T18-00-05.01.log"
	fileName4 := "tidb-slow.log"
	fileNames := []string{fileName0, fileName1, fileName2, fileName3, fileName4}
	prepareLogs(c, logData, fileNames)
	defer func() {
		removeFiles(fileNames)
	}()
	tk := testkit.NewTestKitWithInit(c, s.store)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	tk.Se.GetSessionVars().TimeZone = loc
	tk.MustExec("use information_schema")
	cases := []struct {
		prepareSQL string
		sql        string
		result     []string
	}{
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where time > '2020-02-17 12:00:05.000000' and time < '2020-05-14 20:00:00.000000'",
			result:     []string{"2020-02-17 18:00:05.000000", "2020-02-17 19:00:00.000000", "2020-05-14 19:03:54.314615"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where time > '2020-02-17 12:00:05.000000' and time < '2020-05-14 20:00:00.000000' order by time desc",
			result:     []string{"2020-05-14 19:03:54.314615", "2020-02-17 19:00:00.000000", "2020-02-17 18:00:05.000000"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where (time > '2020-02-15 18:00:00' and time < '2020-02-15 20:01:00') or (time > '2020-02-17 18:00:00' and time < '2020-05-14 20:00:00') order by time",
			result:     []string{"2020-02-15 18:00:01.000000", "2020-02-15 19:00:05.000000", "2020-02-17 18:00:05.000000", "2020-02-17 19:00:00.000000", "2020-05-14 19:03:54.314615"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where (time > '2020-02-15 18:00:00' and time < '2020-02-15 20:01:00') or (time > '2020-02-17 18:00:00' and time < '2020-05-14 20:00:00') order by time desc",
			result:     []string{"2020-05-14 19:03:54.314615", "2020-02-17 19:00:00.000000", "2020-02-17 18:00:05.000000", "2020-02-15 19:00:05.000000", "2020-02-15 18:00:01.000000"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select count(*) from cluster_slow_query where time > '2020-02-15 18:00:00.000000' and time < '2020-05-14 20:00:00.000000' order by time desc",
			result:     []string{"9"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select count(*) from cluster_slow_query where (time > '2020-02-16 18:00:00' and time < '2020-05-14 20:00:00') or (time > '2020-02-17 18:00:00' and time < '2020-05-17 20:00:00')",
			result:     []string{"6"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select count(*) from cluster_slow_query where time > '2020-02-16 18:00:00.000000' and time < '2020-02-17 20:00:00.000000' order by time desc",
			result:     []string{"5"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where time > '2020-02-16 18:00:00.000000' and time < '2020-05-14 20:00:00.000000' order by time desc limit 3",
			result:     []string{"2020-05-14 19:03:54.314615", "2020-02-17 19:00:00.000000", "2020-02-17 18:00:05.000000"},
		},
	}
	for _, cas := range cases {
		if len(cas.prepareSQL) > 0 {
			tk.MustExec(cas.prepareSQL)
		}
		tk.MustQuery(cas.sql).Check(testutil.RowsWithSep("|", cas.result...))
	}
}

func prepareLogs(c *C, logData []string, fileNames []string) {
	writeFile := func(file string, data string) {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		c.Assert(err, IsNil)
		_, err = f.Write([]byte(data))
		c.Assert(f.Close(), IsNil)
		c.Assert(err, IsNil)
	}

	for i, log := range logData {
		writeFile(fileNames[i], log)
	}
}

func removeFiles(fileNames []string) {
	for _, fileName := range fileNames {
		os.Remove(fileName)
	}
}

func (s *testSuite1) TestIssue15718(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt(a decimal(10, 0), b varchar(1), c time);")
	tk.MustExec("insert into tt values(0, '2', null), (7, null, '1122'), (NULL, 'w', null), (NULL, '2', '3344'), (NULL, NULL, '0'), (7, 'f', '33');")
	tk.MustQuery("select a and b as d, a or c as e from tt;").Check(testkit.Rows("0 <nil>", "<nil> 1", "0 <nil>", "<nil> 1", "<nil> <nil>", "0 1"))

	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt(a decimal(10, 0), b varchar(1), c time);")
	tk.MustExec("insert into tt values(0, '2', '123'), (7, null, '1122'), (null, 'w', null);")
	tk.MustQuery("select a and b as d, a, b from tt order by d limit 1;").Check(testkit.Rows("<nil> 7 <nil>"))
	tk.MustQuery("select b or c as d, b, c from tt order by d limit 1;").Check(testkit.Rows("<nil> w <nil>"))

	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 FLOAT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (NULL);")
	tk.MustQuery("SELECT * FROM t0 WHERE NOT(0 OR t0.c0);").Check(testkit.Rows())
}

func (s *testSuite1) TestIssue15767(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table t(a int, b char);")
	tk.MustExec("insert into t values (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a');")
	tk.MustExec("insert into t select * from t;")
	tk.MustExec("insert into t select * from t;")
	tk.MustExec("insert into t select * from t;")
	tk.MustQuery("select b, count(*) from ( select b from t order by a limit 20 offset 2) as s group by b order by b;").Check(testkit.Rows("a 6", "c 7", "s 7"))
}

func (s *testSuite1) TestIssue16025(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 NUMERIC PRIMARY KEY);")
	tk.MustExec("INSERT IGNORE INTO t0(c0) VALUES (NULL);")
	tk.MustQuery("SELECT * FROM t0 WHERE c0;").Check(testkit.Rows())
}

func (s *testSuite1) TestIssue16854(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (	`a` enum('WAITING','PRINTED','STOCKUP','CHECKED','OUTSTOCK','PICKEDUP','WILLBACK','BACKED') DEFAULT NULL)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5),(6),(7);")
	for i := 0; i < 7; i++ {
		tk.MustExec("insert into t select * from t;")
	}
	tk.MustExec("set @@tidb_max_chunk_size=100;")
	tk.MustQuery("select distinct a from t order by a").Check(testkit.Rows("WAITING", "PRINTED", "STOCKUP", "CHECKED", "OUTSTOCK", "PICKEDUP", "WILLBACK"))
	tk.MustExec("drop table t")

	tk.MustExec("CREATE TABLE `t` (	`a` set('WAITING','PRINTED','STOCKUP','CHECKED','OUTSTOCK','PICKEDUP','WILLBACK','BACKED') DEFAULT NULL)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5),(6),(7);")
	for i := 0; i < 7; i++ {
		tk.MustExec("insert into t select * from t;")
	}
	tk.MustExec("set @@tidb_max_chunk_size=100;")
	tk.MustQuery("select distinct a from t order by a").Check(testkit.Rows("WAITING", "PRINTED", "WAITING,PRINTED", "STOCKUP", "WAITING,STOCKUP", "PRINTED,STOCKUP", "WAITING,PRINTED,STOCKUP"))
	tk.MustExec("drop table t")
}

func (s *testSuite) TestIssue16921(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a float);")
	tk.MustExec("create index a on t(a);")
	tk.MustExec("insert into t values (1.0), (NULL), (0), (2.0);")
	tk.MustQuery("select `a` from `t` use index (a) where !`a`;").Check(testkit.Rows("0"))
	tk.MustQuery("select `a` from `t` ignore index (a) where !`a`;").Check(testkit.Rows("0"))
	tk.MustQuery("select `a` from `t` use index (a) where `a`;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select `a` from `t` ignore index (a) where `a`;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not a is true;").Check(testkit.Rows("<nil>", "0"))
	tk.MustQuery("select a from t use index (a) where not not a is true;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not not a;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not not not a is true;").Check(testkit.Rows("<nil>", "0"))
	tk.MustQuery("select a from t use index (a) where not not not a;").Check(testkit.Rows("0"))
}

func (s *testSuite) TestIssue19100(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (c decimal);")
	tk.MustExec("create table t2 (c decimal, key(c));")
	tk.MustExec("insert into t1 values (null);")
	tk.MustExec("insert into t2 values (null);")
	tk.MustQuery("select count(*) from t1 where not c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t2 where not c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t1 where c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t2 where c;").Check(testkit.Rows("0"))
}

// this is from jira issue #5856
func (s *testSuite1) TestInsertValuesWithSubQuery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t2")

	// should not reference upper scope
	c.Assert(tk.ExecToErr("insert into t2 values (11, 8, (select not b))"), NotNil)
	c.Assert(tk.ExecToErr("insert into t2 set a = 11, b = 8, c = (select b))"), NotNil)

	// subquery reference target table is allowed
	tk.MustExec("insert into t2 values(1, 1, (select b from t2))")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("insert into t2 set a = 1, b = 1, c = (select b+1 from t2)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1 <nil>", "1 1 2"))

	// insert using column should work normally
	tk.MustExec("delete from t2")
	tk.MustExec("insert into t2 values(2, 4, a)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 4 2"))
	tk.MustExec("insert into t2 set a = 3, b = 5, c = b")
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 4 2", "3 5 5"))
}

func (s *testSuite1) TestDIVZeroInPartitionExpr(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int) partition by range (10 div a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert into t1 values (NULL), (0), (1)")
	tk.MustExec("set @@sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'")
	tk.MustGetErrCode("insert into t1 values (NULL), (0), (1)", mysql.ErrDivisionByZero)
}

func (s *testSuite1) TestInsertIntoGivenPartitionSet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1(
	a int(11) DEFAULT NULL,
	b varchar(10) DEFAULT NULL,
	UNIQUE KEY idx_a (a)) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)
	defer tk.MustExec("drop table if exists t1")

	// insert into
	tk.MustExec("insert into t1 partition(p0) values(1, 'a'), (2, 'b')")
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b"))
	tk.MustExec("insert into t1 partition(p0, p1) values(3, 'c'), (4, 'd')")
	tk.MustQuery("select * from t1 partition(p1)").Check(testkit.Rows())

	tk.MustGetErrMsg("insert into t1 values(1, 'a')", "[kv:1062]Duplicate entry '1' for key 'idx_a'")
	tk.MustGetErrMsg("insert into t1 partition(p0, p_non_exist) values(1, 'a')", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	tk.MustGetErrMsg("insert into t1 partition(p0, p1) values(40, 'a')", "[table:1748]Found a row not matching the given partition set")

	// replace into
	tk.MustExec("replace into t1 partition(p0) values(1, 'replace')")
	tk.MustExec("replace into t1 partition(p0, p1) values(3, 'replace'), (4, 'replace')")
	tk.MustExec("replace into t1 values(1, 'a')")
	tk.MustQuery("select * from t1 partition (p0) order by a").Check(testkit.Rows("1 a", "2 b", "3 replace", "4 replace"))

	tk.MustGetErrMsg("replace into t1 partition(p0, p_non_exist) values(1, 'a')", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	tk.MustGetErrMsg("replace into t1 partition(p0, p1) values(40, 'a')", "[table:1748]Found a row not matching the given partition set")

	tk.MustExec("truncate table t1")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b char(10))")
	defer tk.MustExec("drop table if exists t")

	// insert into general table
	tk.MustGetErrMsg("insert into t partition(p0, p1) values(1, 'a')", "[planner:1747]PARTITION () clause on non partitioned table")

	// insert into from select
	tk.MustExec("insert into t values(1, 'a'), (2, 'b')")
	tk.MustExec("insert into t1 partition(p0) select * from t")
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b"))

	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(3, 'c'), (4, 'd')")
	tk.MustExec("insert into t1 partition(p0, p1) select * from t")
	tk.MustQuery("select * from t1 partition(p1) order by a").Check(testkit.Rows())
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b", "3 c", "4 d"))

	tk.MustGetErrMsg("insert into t1 select 1, 'a'", "[kv:1062]Duplicate entry '1' for key 'idx_a'")
	tk.MustGetErrMsg("insert into t1 partition(p0, p_non_exist) select 1, 'a'", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	tk.MustGetErrMsg("insert into t1 partition(p0, p1) select 40, 'a'", "[table:1748]Found a row not matching the given partition set")

	// replace into from select
	tk.MustExec("replace into t1 partition(p0) select 1, 'replace'")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(3, 'replace'), (4, 'replace')")
	tk.MustExec("replace into t1 partition(p0, p1) select * from t")

	tk.MustExec("replace into t1 select 1, 'a'")
	tk.MustQuery("select * from t1 partition (p0) order by a").Check(testkit.Rows("1 a", "2 b", "3 replace", "4 replace"))
	tk.MustGetErrMsg("replace into t1 partition(p0, p_non_exist) select 1, 'a'", "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	tk.MustGetErrMsg("replace into t1 partition(p0, p1) select 40, 'a'", "[table:1748]Found a row not matching the given partition set")
}

func (s *testSuite1) TestUpdateGivenPartitionSet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1,t2,t3,t4")
	tk.MustExec(`create table t1(
	a int(11),
	b varchar(10) DEFAULT NULL,
	primary key idx_a (a)) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)

	tk.MustExec(`create table t2(
	a int(11) DEFAULT NULL,
	b varchar(10) DEFAULT NULL) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)

	tk.MustExec(`create table t3 (a int(11), b varchar(10) default null)`)

	defer tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("insert into t3 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	err := tk.ExecToErr("update t3 partition(p0) set a = 40 where a = 2")
	c.Assert(err.Error(), Equals, "[planner:1747]PARTITION () clause on non partitioned table")

	// update with primary key change
	tk.MustExec("insert into t1 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	err = tk.ExecToErr("update t1 partition(p0, p1) set a = 40")
	c.Assert(err.Error(), Equals, "[table:1748]Found a row not matching the given partition set")
	err = tk.ExecToErr("update t1 partition(p0) set a = 40 where a = 2")
	c.Assert(err.Error(), Equals, "[table:1748]Found a row not matching the given partition set")
	// test non-exist partition.
	err = tk.ExecToErr("update t1 partition (p0, p_non_exist) set a = 40")
	c.Assert(err.Error(), Equals, "[table:1735]Unknown partition 'p_non_exist' in table 't1'")
	// test join.
	err = tk.ExecToErr("update t1 partition (p0), t3 set t1.a = 40 where t3.a = 2")
	c.Assert(err.Error(), Equals, "[table:1748]Found a row not matching the given partition set")

	tk.MustExec("update t1 partition(p0) set a = 3 where a = 2")
	tk.MustExec("update t1 partition(p0, p3) set a = 33 where a = 1")

	// update without partition change
	tk.MustExec("insert into t2 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	err = tk.ExecToErr("update t2 partition(p0, p1) set a = 40")
	c.Assert(err.Error(), Equals, "[table:1748]Found a row not matching the given partition set")
	err = tk.ExecToErr("update t2 partition(p0) set a = 40 where a = 2")
	c.Assert(err.Error(), Equals, "[table:1748]Found a row not matching the given partition set")

	tk.MustExec("update t2 partition(p0) set a = 3 where a = 2")
	tk.MustExec("update t2 partition(p0, p3) set a = 33 where a = 1")

	tk.MustExec("create table t4(a int primary key, b int) partition by hash(a) partitions 2")
	tk.MustExec("insert into t4(a, b) values(1, 1),(2, 2),(3, 3);")
	err = tk.ExecToErr("update t4 partition(p0) set a = 5 where a = 2")
	c.Assert(err.Error(), Equals, "[table:1748]Found a row not matching the given partition set")
}

func (s *testSuiteP2) TestApplyCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values (1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("analyze table t;")
	result := tk.MustQuery("explain analyze SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t;")
	c.Assert(result.Rows()[1][0], Equals, "└─Apply_39")
	var (
		ind  int
		flag bool
	)
	value := (result.Rows()[1][5]).(string)
	for ind = 0; ind < len(value)-5; ind++ {
		if value[ind:ind+5] == "cache" {
			flag = true
			break
		}
	}
	c.Assert(flag, Equals, true)
	c.Assert(value[ind:], Equals, "cache:ON, cacheHitRatio:88.889%")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7),(8),(9);")
	tk.MustExec("analyze table t;")
	result = tk.MustQuery("explain analyze SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t;")
	c.Assert(result.Rows()[1][0], Equals, "└─Apply_39")
	flag = false
	value = (result.Rows()[1][5]).(string)
	for ind = 0; ind < len(value)-5; ind++ {
		if value[ind:ind+5] == "cache" {
			flag = true
			break
		}
	}
	c.Assert(flag, Equals, true)
	c.Assert(value[ind:], Equals, "cache:OFF")
}

// For issue 17256
func (s *testSuite) TestGenerateColumnReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int as (a + 1) virtual not null, unique index idx(b));")
	tk.MustExec("REPLACE INTO `t1` (`a`) VALUES (2);")
	tk.MustExec("REPLACE INTO `t1` (`a`) VALUES (2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2 3"))
	tk.MustExec("insert into `t1` (`a`) VALUES (2) on duplicate key update a = 3;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("3 4"))
}

func (s *testSlowQuery) TestSlowQueryWithoutSlowLog(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = "tidb-slow-not-exist.log"
	newCfg.Log.SlowThreshold = math.MaxUint64
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()
	tk.MustQuery("select query from information_schema.slow_query").Check(testkit.Rows())
	tk.MustQuery("select query from information_schema.slow_query where time > '2020-09-15 12:16:39' and time < now()").Check(testkit.Rows())
}

func (s *testSlowQuery) TestSlowQuerySensitiveQuery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := ioutil.TempFile("", "tidb-slow-*.log")
	c.Assert(err, IsNil)
	f.Close()
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
		config.StoreGlobalConfig(originCfg)
		err = os.Remove(newCfg.Log.SlowQueryFile)
		c.Assert(err, IsNil)
	}()
	err = logutil.InitLogger(newCfg.Log.ToLogConfig())
	c.Assert(err, IsNil)

	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustExec("drop user if exists user_sensitive;")
	tk.MustExec("create user user_sensitive identified by '123456789';")
	tk.MustExec("alter user 'user_sensitive'@'%' identified by 'abcdefg';")
	tk.MustExec("set password for 'user_sensitive'@'%' = 'xyzuvw';")
	tk.MustQuery("select query from `information_schema`.`slow_query` " +
		"where (query like 'set password%' or query like 'create user%' or query like 'alter user%') " +
		"and query like '%user_sensitive%' order by query;").
		Check(testkit.Rows(
			"alter user {user_sensitive@% password = ***};",
			"create user {user_sensitive@% password = ***};",
			"set password for user user_sensitive@%;",
		))
}

func (s *testSlowQuery) TestSlowQueryPrepared(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := ioutil.TempFile("", "tidb-slow-*.log")
	c.Assert(err, IsNil)
	f.Close()
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		tk.MustExec("set tidb_slow_log_threshold=300;")
		tk.MustExec("set tidb_redact_log=0;")
		config.StoreGlobalConfig(originCfg)
		os.Remove(newCfg.Log.SlowQueryFile)
	}()
	err = logutil.InitLogger(newCfg.Log.ToLogConfig())
	c.Assert(err, IsNil)

	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustExec(`prepare mystmt1 from 'select sleep(?), 1';`)
	tk.MustExec("SET @num = 0.01;")
	tk.MustExec("execute mystmt1 using @num;")
	tk.MustQuery("SELECT Query FROM `information_schema`.`slow_query` " +
		"where query like 'select%sleep%' order by time desc limit 1").
		Check(testkit.Rows(
			"select sleep(?), 1 [arguments: 0.01];",
		))

	tk.MustExec("set tidb_redact_log=1;")
	tk.MustExec(`prepare mystmt2 from 'select sleep(?), 2';`)
	tk.MustExec("execute mystmt2 using @num;")
	tk.MustQuery("SELECT Query FROM `information_schema`.`slow_query` " +
		"where query like 'select%sleep%' order by time desc limit 1").
		Check(testkit.Rows(
			"select `sleep` ( ? ) , ?;",
		))
}

func (s *testSlowQuery) TestLogSlowLogIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	f, err := ioutil.TempFile("", "tidb-slow-*.log")
	c.Assert(err, IsNil)
	f.Close()

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowQueryFile = f.Name()
	})
	err = logutil.InitLogger(config.GetGlobalConfig().Log.ToLogConfig())
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int,index idx(a));")
	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustQuery("select * from t use index (idx) where a in (1) union select * from t use index (idx) where a in (2,3);")
	tk.MustExec("set tidb_slow_log_threshold=300;")
	tk.MustQuery("select index_names from `information_schema`.`slow_query` " +
		"where query like 'select%union%' limit 1").
		Check(testkit.Rows(
			"[t:idx]",
		))
}

func (s *testSlowQuery) TestSlowQuery(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	f, err := ioutil.TempFile("", "tidb-slow-*.log")
	c.Assert(err, IsNil)
	_, err = f.WriteString(`
# Time: 2020-10-13T20:08:13.970563+08:00
select * from t;
# Time: 2020-10-16T20:08:13.970563+08:00
select * from t;
`)
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, IsNil)
	executor.ParseSlowLogBatchSize = 1
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		executor.ParseSlowLogBatchSize = 64
		config.StoreGlobalConfig(originCfg)
		err = os.Remove(newCfg.Log.SlowQueryFile)
		c.Assert(err, IsNil)
	}()
	err = logutil.InitLogger(newCfg.Log.ToLogConfig())
	c.Assert(err, IsNil)

	tk.MustQuery("select count(*) from `information_schema`.`slow_query` where time > '2020-10-16 20:08:13' and time < '2020-10-16 21:08:13'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from `information_schema`.`slow_query` where time > '2019-10-13 20:08:13' and time < '2020-10-16 21:08:13'").Check(testkit.Rows("2"))
}

func (s *testSerialSuite) TestKillTableReader(c *C) {
	var retry = "github.com/pingcap/tidb/store/tikv/mockRetrySendReqToRegion"
	defer func() {
		c.Assert(failpoint.Disable(retry), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1),(2),(3)")
	tk.MustExec("set @@tidb_distsql_scan_concurrency=1")
	atomic.StoreUint32(&tk.Se.GetSessionVars().Killed, 0)
	c.Assert(failpoint.Enable(retry, `return(true)`), IsNil)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)
		err := tk.QueryToErr("select * from t")
		c.Assert(err, NotNil)
		c.Assert(int(terror.ToSQLError(errors.Cause(err).(*terror.Error)).Code), Equals, int(executor.ErrQueryInterrupted.Code()))
	}()
	atomic.StoreUint32(&tk.Se.GetSessionVars().Killed, 1)
	wg.Wait()
}

func (s *testSerialSuite) TestPrevStmtDesensitization(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec(fmt.Sprintf("set @@session.%v=1", variable.TiDBRedactLog))
	defer tk.MustExec(fmt.Sprintf("set @@session.%v=0", variable.TiDBRedactLog))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, unique key (a))")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1),(2)")
	c.Assert(tk.Se.GetSessionVars().PrevStmt.String(), Equals, "insert into `t` values ( ? ) , ( ? )")
	c.Assert(tk.ExecToErr("insert into t values (1)").Error(), Equals, `[kv:1062]Duplicate entry '?' for key 'a'`)
}

func (s *testSuite) TestIssue19372(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (c_int int, c_str varchar(40), key(c_str));")
	tk.MustExec("create table t2 like t1;")
	tk.MustExec("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c');")
	tk.MustExec("insert into t2 select * from t1;")
	tk.MustQuery("select (select t2.c_str from t2 where t2.c_str <= t1.c_str and t2.c_int in (1, 2) order by t2.c_str limit 1) x from t1 order by c_int;").Check(testkit.Rows("a", "a", "a"))
}

func (s *testSerialSuite1) TestCollectCopRuntimeStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("set tidb_enable_collect_execution_info=1;")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreRespResult", `return(true)`), IsNil)
	rows := tk.MustQuery("explain analyze select * from t1").Rows()
	c.Assert(len(rows), Equals, 2)
	explain := fmt.Sprintf("%v", rows[0])
	c.Assert(explain, Matches, ".*rpc_num: 2, .*regionMiss:.*")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreRespResult"), IsNil)
}

func (s *testSerialSuite1) TestIndexLookupRuntimeStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, index(a))")
	tk.MustExec("insert into t1 values (1,2),(2,3),(3,4)")
	sql := "explain analyze select * from t1 use index(a) where a > 1;"
	rows := tk.MustQuery(sql).Rows()
	c.Assert(len(rows), Equals, 3)
	explain := fmt.Sprintf("%v", rows[0])
	c.Assert(explain, Matches, ".*time:.*loops:.*index_task:.*table_task: {total_time.*num.*concurrency.*}.*")
	indexExplain := fmt.Sprintf("%v", rows[1])
	tableExplain := fmt.Sprintf("%v", rows[2])
	c.Assert(indexExplain, Matches, ".*time:.*loops:.*cop_task:.*")
	c.Assert(tableExplain, Matches, ".*time:.*loops:.*cop_task:.*")
}

func (s *testSerialSuite1) TestHashAggRuntimeStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("insert into t1 values (1,2),(2,3),(3,4)")
	sql := "explain analyze SELECT /*+ HASH_AGG() */ count(*) FROM t1 WHERE a < 10;"
	rows := tk.MustQuery(sql).Rows()
	c.Assert(len(rows), Equals, 5)
	explain := fmt.Sprintf("%v", rows[0])
	c.Assert(explain, Matches, ".*time:.*loops:.*partial_worker:{wall_time:.*concurrency:.*task_num:.*tot_wait:.*tot_exec:.*tot_time:.*max:.*p95:.*}.*final_worker:{wall_time:.*concurrency:.*task_num:.*tot_wait:.*tot_exec:.*tot_time:.*max:.*p95:.*}.*")
}

func (s *testSerialSuite1) TestIndexMergeRuntimeStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("set @@tidb_enable_index_merge = 1")
	tk.MustExec("create table t1(id int primary key, a int, b int, c int, d int)")
	tk.MustExec("create index t1a on t1(a)")
	tk.MustExec("create index t1b on t1(b)")
	tk.MustExec("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	sql := "explain analyze select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4;"
	rows := tk.MustQuery(sql).Rows()
	c.Assert(len(rows), Equals, 4)
	explain := fmt.Sprintf("%v", rows[0])
	c.Assert(explain, Matches, ".*time:.*loops:.*index_task:{fetch_handle:.*, merge:.*}.*table_task:{num.*concurrency.*fetch_row.*wait_time.*}.*")
	tableRangeExplain := fmt.Sprintf("%v", rows[1])
	indexExplain := fmt.Sprintf("%v", rows[2])
	tableExplain := fmt.Sprintf("%v", rows[3])
	c.Assert(tableRangeExplain, Matches, ".*time:.*loops:.*cop_task:.*")
	c.Assert(indexExplain, Matches, ".*time:.*loops:.*cop_task:.*")
	c.Assert(tableExplain, Matches, ".*time:.*loops:.*cop_task:.*")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	sql = "select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4 order by a"
	tk.MustQuery(sql).Check(testkit.Rows("1 1 1 1 1", "5 5 5 5 5"))
}

func (s *testSuite) TestCollectDMLRuntimeStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, unique index (a))")

	testSQLs := []string{
		"insert ignore into t1 values (5,5);",
		"insert into t1 values (5,5) on duplicate key update a=a+1;",
		"replace into t1 values (5,6),(6,7)",
		"update t1 set a=a+1 where a=6;",
	}

	getRootStats := func() string {
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		p, ok := info.Plan.(plannercore.Plan)
		c.Assert(ok, IsTrue)
		stats := tk.Se.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(p.ID())
		return stats.String()
	}
	for _, sql := range testSQLs {
		tk.MustExec(sql)
		c.Assert(getRootStats(), Matches, "time.*loops.*Get.*num_rpc.*total_time.*")
	}

	// Test for lock keys stats.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t1 set b=b+1")
	c.Assert(getRootStats(), Matches, "time.*lock_keys.*time.* region.* keys.* lock_rpc:.* rpc_count.*")
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 for update").Check(testkit.Rows("5 6", "7 7"))
	c.Assert(getRootStats(), Matches, "time.*lock_keys.*time.* region.* keys.* lock_rpc:.* rpc_count.*")
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert ignore into t1 values (9,9)")
	c.Assert(getRootStats(), Matches, "time:.*, loops:.*, prepare:.*, check_insert: {total_time:.*, mem_insert_time:.*, prefetch:.*, rpc:{BatchGet:{num_rpc:.*, total_time:.*}}}.*")
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values (10,10) on duplicate key update a=a+1")
	c.Assert(getRootStats(), Matches, "time:.*, loops:.*, prepare:.*, check_insert: {total_time:.*, mem_insert_time:.*, prefetch:.*, rpc:{BatchGet:{num_rpc:.*, total_time:.*}.*")
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t1 values (1,2)")
	c.Assert(getRootStats(), Matches, "time:.*, loops:.*, prepare:.*, insert:.*")
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert ignore into t1 values(11,11) on duplicate key update `a`=`a`+1")
	c.Assert(getRootStats(), Matches, "time:.*, loops:.*, prepare:.*, check_insert: {total_time:.*, mem_insert_time:.*, prefetch:.*, rpc:.*}")
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("replace into t1 values (1,4)")
	c.Assert(getRootStats(), Matches, "time:.*, loops:.*, prefetch:.*, rpc:.*")
	tk.MustExec("rollback")
}

func (s *testSuite) TestIssue13758(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (pk int(11) primary key, a int(11) not null, b int(11), key idx_b(b), key idx_a(a))")
	tk.MustExec("insert into `t1` values (1,1,0),(2,7,6),(3,2,null),(4,1,null),(5,4,5)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t2 values (1),(null)")
	tk.MustQuery("select (select a from t1 use index(idx_a) where b >= t2.a order by a limit 1) as field from t2").Check(testkit.Rows(
		"4",
		"<nil>",
	))
}

func (s *testCoprCache) SetUpSuite(c *C) {
	originConfig := config.GetGlobalConfig()
	config.StoreGlobalConfig(config.NewConfig())
	defer config.StoreGlobalConfig(originConfig)
	cli := &regionProperityClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cls = c
		}),
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testCoprCache) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testCoprCache) TestIntegrationCopCache(c *C) {
	originConfig := config.GetGlobalConfig()
	config.StoreGlobalConfig(config.NewConfig())
	defer config.StoreGlobalConfig(originConfig)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	tblInfo, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tblInfo.Meta().ID
	tk.MustExec(`insert into t values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)`)
	tableStart := tablecodec.GenTableRecordPrefix(tid)
	s.cls.SplitKeys(tableStart, tableStart.PrefixNext(), 6)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/cophandler/mockCopCacheInUnistore", `return(123)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/cophandler/mockCopCacheInUnistore"), IsNil)
	}()

	rows := tk.MustQuery("explain analyze select * from t where t.a < 10").Rows()
	c.Assert(rows[0][2], Equals, "9")
	c.Assert(strings.Contains(rows[0][5].(string), "cop_task: {num: 5"), Equals, true)
	c.Assert(strings.Contains(rows[0][5].(string), "copr_cache_hit_ratio: 0.00"), Equals, true)

	rows = tk.MustQuery("explain analyze select * from t").Rows()
	c.Assert(rows[0][2], Equals, "12")
	c.Assert(strings.Contains(rows[0][5].(string), "cop_task: {num: 6"), Equals, true)
	hitRatioIdx := strings.Index(rows[0][5].(string), "copr_cache_hit_ratio:") + len("copr_cache_hit_ratio: ")
	c.Assert(hitRatioIdx >= len("copr_cache_hit_ratio: "), Equals, true)
	hitRatio, err := strconv.ParseFloat(rows[0][5].(string)[hitRatioIdx:hitRatioIdx+4], 64)
	c.Assert(err, IsNil)
	c.Assert(hitRatio > 0, Equals, true)

	// Test for cop cache disabled.
	cfg := config.NewConfig()
	cfg.TiKVClient.CoprCache.CapacityMB = 0
	config.StoreGlobalConfig(cfg)
	rows = tk.MustQuery("explain analyze select * from t where t.a < 10").Rows()
	c.Assert(rows[0][2], Equals, "9")
	c.Assert(strings.Contains(rows[0][5].(string), "copr_cache: disabled"), Equals, true)
}

func (s *testSerialSuite) TestCoprocessorOOMTicase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_wait_split_region_finish=1`)
	// create table for non keep-order case
	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5(id int)")
	tk.MustQuery(`split table t5 between (0) and (10000) regions 10`).Check(testkit.Rows("9 1"))
	// create table for keep-order case
	tk.MustExec("drop table if exists t6")
	tk.MustExec("create table t6(id int, index(id))")
	tk.MustQuery(`split table t6 between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	tk.MustQuery("split table t6 INDEX id between (0) and (10000) regions 10;").Check(testkit.Rows("10 1"))
	count := 10
	for i := 0; i < count; i++ {
		tk.MustExec(fmt.Sprintf("insert into t5 (id) values (%v)", i))
		tk.MustExec(fmt.Sprintf("insert into t6 (id) values (%v)", i))
	}
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionLog
	})
	testcases := []struct {
		name string
		sql  string
	}{
		{
			name: "keep Order",
			sql:  "select id from t6 order by id",
		},
		{
			name: "non keep Order",
			sql:  "select id from t5",
		},
	}

	f := func() {
		for _, testcase := range testcases {
			c.Log(testcase.name)
			// larger than one copResponse, smaller than 2 copResponse
			quota := 2*copr.MockResponseSizeForTest - 100
			se, err := session.CreateSession4Test(s.store)
			c.Check(err, IsNil)
			tk.Se = se
			tk.MustExec("use test")
			tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
			var expect []string
			for i := 0; i < count; i++ {
				expect = append(expect, fmt.Sprintf("%v", i))
			}
			tk.MustQuery(testcase.sql).Sort().Check(testkit.Rows(expect...))
			// assert oom action worked by max consumed > memory quota
			c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), Greater, int64(quota))
			se.Close()
		}
	}

	// ticase-4169, trigger oom action twice after workers consuming all the data
	err := failpoint.Enable("github.com/pingcap/tidb/store/copr/ticase-4169", `return(true)`)
	c.Assert(err, IsNil)
	f()
	err = failpoint.Disable("github.com/pingcap/tidb/store/copr/ticase-4169")
	c.Assert(err, IsNil)
	// ticase-4170, trigger oom action twice after iterator receiving all the data.
	err = failpoint.Enable("github.com/pingcap/tidb/store/copr/ticase-4170", `return(true)`)
	c.Assert(err, IsNil)
	f()
	err = failpoint.Disable("github.com/pingcap/tidb/store/copr/ticase-4170")
	c.Assert(err, IsNil)
	// ticase-4171, trigger oom before reading or consuming any data
	err = failpoint.Enable("github.com/pingcap/tidb/store/copr/ticase-4171", `return(true)`)
	c.Assert(err, IsNil)
	f()
	err = failpoint.Disable("github.com/pingcap/tidb/store/copr/ticase-4171")
	c.Assert(err, IsNil)
}

func (s *testSuite) TestIssue20237(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table t(a date, b float)")
	tk.MustExec("create table s(b float)")
	tk.MustExec(`insert into t values(NULL,-37), ("2011-11-04",105), ("2013-03-02",-22), ("2006-07-02",-56), (NULL,124), (NULL,111), ("2018-03-03",-5);`)
	tk.MustExec(`insert into s values(-37),(105),(-22),(-56),(124),(105),(111),(-5);`)
	tk.MustQuery(`select count(distinct t.a, t.b) from t join s on t.b= s.b;`).Check(testkit.Rows("4"))
}

func (s *testSerialSuite) TestIssue19148(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(16, 2));")
	tk.MustExec("select * from t where a > any_value(a);")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(int(tblInfo.Meta().Columns[0].Flag), Equals, 0)
}

func (s *testSuite) TestIssue19667(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a DATETIME)")
	tk.MustExec("INSERT INTO t VALUES('1988-04-17 01:59:59')")
	tk.MustQuery(`SELECT DATE_ADD(a, INTERVAL 1 SECOND) FROM t`).Check(testkit.Rows("1988-04-17 02:00:00"))
}

func issue20975Prepare(c *C, store kv.Storage) (*testkit.TestKit, *testkit.TestKit) {
	tk1 := testkit.NewTestKit(c, store)
	tk2 := testkit.NewTestKit(c, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t1, t2")
	tk2.MustExec("use test")
	tk1.MustExec("create table t1(id int primary key, c int)")
	tk1.MustExec("insert into t1 values(1, 10), (2, 20)")
	return tk1, tk2
}

func (s *testSuite) TestIssue20975UpdateNoChange(c *C) {
	tk1, tk2 := issue20975Prepare(c, s.store)
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("update t1 set c=c")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")
}

func (s *testSuite) TestIssue20975SelectForUpdate(c *C) {
	tk1, tk2 := issue20975Prepare(c, s.store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func (s *testSuite) TestIssue20975SelectForUpdatePointGet(c *C) {
	tk1, tk2 := issue20975Prepare(c, s.store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func (s *testSuite) TestIssue20975SelectForUpdateBatchPointGet(c *C) {
	tk1, tk2 := issue20975Prepare(c, s.store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id in (1, 2) for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id in (1, 2) for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func issue20975PreparePartitionTable(c *C, store kv.Storage) (*testkit.TestKit, *testkit.TestKit) {
	tk1 := testkit.NewTestKit(c, store)
	tk2 := testkit.NewTestKit(c, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t1, t2")
	tk2.MustExec("use test")
	tk1.MustExec(`create table t1(id int primary key, c int) partition by range (id) (
		partition p1 values less than (10),
		partition p2 values less than (20)
	)`)
	tk1.MustExec("insert into t1 values(1, 10), (2, 20), (11, 30), (12, 40)")
	return tk1, tk2
}

func (s *testSuite) TestIssue20975UpdateNoChangeWithPartitionTable(c *C) {
	tk1, tk2 := issue20975PreparePartitionTable(c, s.store)
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("update t1 set c=c")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")
}

func (s *testSuite) TestIssue20975SelectForUpdateWithPartitionTable(c *C) {
	tk1, tk2 := issue20975PreparePartitionTable(c, s.store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func (s *testSuite) TestIssue20975SelectForUpdatePointGetWithPartitionTable(c *C) {
	tk1, tk2 := issue20975PreparePartitionTable(c, s.store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id=12 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id=12 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func (s *testSuite) TestIssue20975SelectForUpdateBatchPointGetWithPartitionTable(c *C) {
	tk1, tk2 := issue20975PreparePartitionTable(c, s.store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id in (1, 2) for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id in (11, 12) for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id in (1, 11) for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id in (1, 2) for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id in (11, 12) for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id in (1, 11) for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func (s *testSuite) TestIssue20305(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t2 (a year(4))")
	tk.MustExec("insert into t2 values(69)")
	tk.MustQuery("select * from t2 where a <= 69").Check(testkit.Rows("2069"))
	// the following test is a regression test that matches MySQL's behavior.
	tk.MustExec("drop table if exists t3")
	tk.MustExec("CREATE TABLE `t3` (`y` year DEFAULT NULL, `a` int DEFAULT NULL)")
	tk.MustExec("INSERT INTO `t3` VALUES (2069, 70), (2010, 11), (2155, 2156), (2069, 69)")
	tk.MustQuery("SELECT * FROM `t3` where y <= a").Check(testkit.Rows("2155 2156"))
}

func (s *testSuite) TestIssue22817(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (a year)")
	tk.MustExec("insert into t3 values (1991), (\"1992\"), (\"93\"), (94)")
	tk.MustQuery("select * from t3 where a >= NULL").Check(testkit.Rows())
}

func (s *testSuite) TestIssue13953(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (`id` int(11) DEFAULT NULL, `tp_bigint` bigint(20) DEFAULT NULL )")
	tk.MustExec("insert into t values(0,1),(1,9215570218099803537)")
	tk.MustQuery("select A.tp_bigint,B.id from t A join t B on A.id < B.id * 16 where A.tp_bigint = B.id;").Check(
		testkit.Rows("1 1"))
}

func (s *testSuite) TestZeroDateTimeCompatibility(c *C) {
	SQLs := []string{
		`select YEAR(0000-00-00), YEAR("0000-00-00")`,
		`select MONTH(0000-00-00), MONTH("0000-00-00")`,
		`select DAYOFWEEK(0000-00-00), DAYOFWEEK("0000-00-00")`,
		`select DAYOFMONTH(0000-00-00), DAYOFMONTH("0000-00-00")`,
		`select DAYOFYEAR(0000-00-00), DAYOFYEAR("0000-00-00")`,
		`select QUARTER(0000-00-00), QUARTER("0000-00-00")`,
		`select EXTRACT(DAY FROM 0000-00-00), EXTRACT(DAY FROM "0000-00-00")`,
		`select EXTRACT(MONTH FROM 0000-00-00), EXTRACT(MONTH FROM "0000-00-00")`,
		`select EXTRACT(YEAR FROM 0000-00-00), EXTRACT(YEAR FROM "0000-00-00")`,
		`select EXTRACT(WEEK FROM 0000-00-00), EXTRACT(WEEK FROM "0000-00-00")`,
		`select EXTRACT(QUARTER FROM 0000-00-00), EXTRACT(QUARTER FROM "0000-00-00")`,
	}
	tk := testkit.NewTestKit(c, s.store)

	for _, t := range SQLs {
		fmt.Println(t)
		tk.MustQuery(t).Check(testkit.Rows("0 <nil>"))
		c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	}
}

// https://github.com/pingcap/tidb/issues/24165.
func (s *testSuite) TestInvalidDateValueInCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE';")
	tk.MustGetErrCode("create table t (a datetime default '2999-00-00 00:00:00');", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table t (a datetime default '2999-02-30 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("create table t (a datetime);")
	tk.MustGetErrCode("alter table t modify column a datetime default '2999-00-00 00:00:00';", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")

	tk.MustExec("set @@sql_mode = (select replace(@@sql_mode,'NO_ZERO_IN_DATE',''));")
	tk.MustExec("set @@sql_mode = (select replace(@@sql_mode,'NO_ZERO_DATE',''));")
	tk.MustExec("set @@sql_mode=(select concat(@@sql_mode, ',ALLOW_INVALID_DATES'));")
	// Test create table with zero datetime as a default value.
	tk.MustExec("create table t (a datetime default '2999-00-00 00:00:00');")
	tk.MustExec("drop table if exists t;")
	// Test create table with invalid datetime(02-30) as a default value.
	tk.MustExec("create table t (a datetime default '2999-02-30 00:00:00');")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime);")
	tk.MustExec("alter table t modify column a datetime default '2999-00-00 00:00:00';")
	tk.MustExec("drop table if exists t;")
}

func (s *testSuite) TestOOMActionPriority(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("drop table if exists t4")
	tk.MustExec("create table t0(a int)")
	tk.MustExec("insert into t0 values(1)")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("insert into t2 values(1)")
	tk.MustExec("create table t3(a int)")
	tk.MustExec("insert into t3 values(1)")
	tk.MustExec("create table t4(a int)")
	tk.MustExec("insert into t4 values(1)")
	tk.MustQuery("select * from t0 join t1 join t2 join t3 join t4 order by t0.a").Check(testkit.Rows("1 1 1 1 1"))
	action := tk.Se.GetSessionVars().StmtCtx.MemTracker.GetFallbackForTest()
	// check the first 5 actions is rate limit.
	for i := 0; i < 5; i++ {
		c.Assert(action.GetPriority(), Equals, int64(memory.DefRateLimitPriority))
		action = action.GetFallback()
	}
	for action.GetFallback() != nil {
		c.Assert(action.GetPriority(), Equals, int64(memory.DefSpillPriority))
		action = action.GetFallback()
	}
	c.Assert(action.GetPriority(), Equals, int64(memory.DefLogPriority))
}

func (s *testSuite) Test17780(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("create table t0 (c0 double)")
	tk.MustExec("insert into t0 values (1e30)")
	tk.MustExec("update t0 set c0=0 where t0.c0 like 0")
	// the update should not affect c0
	tk.MustQuery("select count(*) from t0 where c0 = 0").Check(testkit.Rows("0"))
}

func (s *testSuite) TestIssue9918(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a year)")
	tk.MustExec("insert into t values(0)")
	tk.MustQuery("select cast(a as char) from t").Check(testkit.Rows("0000"))
}

func (s *testSuite) Test13004(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-literals.html, timestamp here actually produces a datetime
	tk.MustQuery("SELECT TIMESTAMP '9999-01-01 00:00:00'").Check(testkit.Rows("9999-01-01 00:00:00"))
}

func (s *testSuite) Test12178(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ta")
	tk.MustExec("create table ta(id decimal(60,2))")
	tk.MustExec("insert into ta values (JSON_EXTRACT('{\"c\": \"1234567890123456789012345678901234567890123456789012345\"}', '$.c'))")
	tk.MustQuery("select * from ta").Check(testkit.Rows("1234567890123456789012345678901234567890123456789012345.00"))
}

func (s *testSuite) Test11883(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (f1 json)")
	tk.MustExec("insert into t1(f1) values ('\"asd\"'),('\"asdf\"'),('\"asasas\"')")
	tk.MustQuery("select f1 from t1 where json_extract(f1,\"$\") in (\"asd\",\"asasas\",\"asdf\")").Check(testkit.Rows("\"asd\"", "\"asdf\"", "\"asasas\""))
	tk.MustQuery("select f1 from t1 where json_extract(f1, '$') = 'asd'").Check(testkit.Rows("\"asd\""))
	// MySQL produces empty row for the following SQL, I doubt it should be MySQL's bug.
	tk.MustQuery("select f1 from t1 where case json_extract(f1,\"$\") when \"asd\" then 1 else 0 end").Check(testkit.Rows("\"asd\""))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 values ('{\"a\": 1}')")
	// the first value in the tuple should be interpreted as string instead of JSON, so no row will be returned
	tk.MustQuery("select f1 from t1 where f1 in ('{\"a\": 1}', 'asdf', 'asdf')").Check(testkit.Rows())
	// and if we explicitly cast it into a JSON value, the check will pass
	tk.MustQuery("select f1 from t1 where f1 in (cast('{\"a\": 1}' as JSON), 'asdf', 'asdf')").Check(testkit.Rows("{\"a\": 1}"))
	tk.MustQuery("select json_extract('\"asd\"', '$') = 'asd'").Check(testkit.Rows("1"))
	tk.MustQuery("select json_extract('\"asd\"', '$') <=> 'asd'").Check(testkit.Rows("1"))
	tk.MustQuery("select json_extract('\"asd\"', '$') <> 'asd'").Check(testkit.Rows("0"))
	tk.MustQuery("select json_extract('{\"f\": 1.0}', '$.f') = 1.0").Check(testkit.Rows("1"))
	tk.MustQuery("select json_extract('{\"f\": 1.0}', '$.f') = '1.0'").Check(testkit.Rows("0"))
	tk.MustQuery("select json_extract('{\"n\": 1}', '$') = '{\"n\": 1}'").Check(testkit.Rows("0"))
	tk.MustQuery("select json_extract('{\"n\": 1}', '$') <> '{\"n\": 1}'").Check(testkit.Rows("1"))
}

func (s *testSuite) Test15492(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (2, 20), (1, 10), (3, 30)")
	tk.MustQuery("select a + 1 as field1, a as field2 from t order by field1, field2 limit 2").Check(testkit.Rows("2 1", "3 2"))
}

func (s testSuite) TestTrackAggMemoryUsage(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")

	tk.MustExec("set tidb_track_aggregate_memory_usage = off;")
	rows := tk.MustQuery("explain analyze select /*+ HASH_AGG() */ sum(a) from t").Rows()
	c.Assert(rows[0][7], Equals, "N/A")
	rows = tk.MustQuery("explain analyze select /*+ STREAM_AGG() */ sum(a) from t").Rows()
	c.Assert(rows[0][7], Equals, "N/A")
	tk.MustExec("set tidb_track_aggregate_memory_usage = on;")
	rows = tk.MustQuery("explain analyze select /*+ HASH_AGG() */ sum(a) from t").Rows()
	c.Assert(rows[0][7], Not(Equals), "N/A")
	rows = tk.MustQuery("explain analyze select /*+ STREAM_AGG() */ sum(a) from t").Rows()
	c.Assert(rows[0][7], Not(Equals), "N/A")
}

func (s *testSuite) Test12201(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists e")
	tk.MustExec("create table e (e enum('a', 'b'))")
	tk.MustExec("insert into e values ('a'), ('b')")
	tk.MustQuery("select * from e where case 1 when 0 then e end").Check(testkit.Rows())
	tk.MustQuery("select * from e where case 1 when 1 then e end").Check(testkit.Rows("a", "b"))
	tk.MustQuery("select * from e where case e when 1 then e end").Check(testkit.Rows("a"))
	tk.MustQuery("select * from e where case 1 when e then e end").Check(testkit.Rows("a"))
}

func (s *testSuite) TestIssue21451(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (en enum('c', 'b', 'a'));")
	tk.MustExec("insert into t values ('a'), ('b'), ('c');")
	tk.MustQuery("select max(en) from t;").Check(testkit.Rows("c"))
	tk.MustQuery("select min(en) from t;").Check(testkit.Rows("a"))
	tk.MustQuery("select * from t order by en;").Check(testkit.Rows("c", "b", "a"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(s set('c', 'b', 'a'));")
	tk.MustExec("insert into t values ('a'), ('b'), ('c');")
	tk.MustQuery("select max(s) from t;").Check(testkit.Rows("c"))
	tk.MustQuery("select min(s) from t;").Check(testkit.Rows("a"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(id int, en enum('c', 'b', 'a'))")
	tk.MustExec("insert into t values (1, 'a'),(2, 'b'), (3, 'c'), (1, 'c');")
	tk.MustQuery("select id, max(en) from t where id=1 group by id;").Check(testkit.Rows("1 c"))
	tk.MustQuery("select id, min(en) from t where id=1 group by id;").Check(testkit.Rows("1 a"))
	tk.MustExec("drop table t")

	tk.MustExec("create table t(id int, s set('c', 'b', 'a'));")
	tk.MustExec("insert into t values (1, 'a'),(2, 'b'), (3, 'c'), (1, 'c');")
	tk.MustQuery("select id, max(s) from t where id=1 group by id;").Check(testkit.Rows("1 c"))
	tk.MustQuery("select id, min(s) from t where id=1 group by id;").Check(testkit.Rows("1 a"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(e enum('e','d','c','b','a'))")
	tk.MustExec("insert into t values ('e'),('d'),('c'),('b'),('a');")
	tk.MustQuery("select * from t order by e limit 1;").Check(testkit.Rows("e"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(s set('e', 'd', 'c', 'b', 'a'))")
	tk.MustExec("insert into t values ('e'),('d'),('c'),('b'),('a');")
	tk.MustQuery("select * from t order by s limit 1;").Check(testkit.Rows("e"))
	tk.MustExec("drop table t")
}

func (s *testSuite) TestIssue15563(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select distinct 0.7544678906163867 /  0.68234634;").Check(testkit.Rows("1.10569639842486251190"))
}

func (s *testSuite) TestIssue22231(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_issue_22231")
	tk.MustExec("create table t_issue_22231(a datetime)")
	tk.MustExec("insert into t_issue_22231 values('2020--05-20 01:22:12')")
	tk.MustQuery("select * from t_issue_22231 where a >= '2020-05-13 00:00:00 00:00:00' and a <= '2020-05-28 23:59:59 00:00:00'").Check(testkit.Rows("2020-05-20 01:22:12"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '2020-05-13 00:00:00 00:00:00'", "Warning 1292 Truncated incorrect datetime value: '2020-05-28 23:59:59 00:00:00'"))

	tk.MustQuery("select cast('2020-10-22 10:31-10:12' as datetime)").Check(testkit.Rows("2020-10-22 10:31:10"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '2020-10-22 10:31-10:12'"))
	tk.MustQuery("select cast('2020-05-28 23:59:59 00:00:00' as datetime)").Check(testkit.Rows("2020-05-28 23:59:59"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '2020-05-28 23:59:59 00:00:00'"))
	tk.MustExec("drop table if exists t_issue_22231")
}

func (s *testSuite) TestIssue22201(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustQuery("SELECT HEX(WEIGHT_STRING('ab' AS BINARY(1000000000000000000)));").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1301 Result of cast_as_binary() was larger than max_allowed_packet (67108864) - truncated"))
	tk.MustQuery("SELECT HEX(WEIGHT_STRING('ab' AS char(1000000000000000000)));").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1301 Result of weight_string() was larger than max_allowed_packet (67108864) - truncated"))
}

func (s *testSuiteP1) TestIssue22941(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists m, mp")
	tk.MustExec(`CREATE TABLE m (
		mid varchar(50) NOT NULL,
		ParentId varchar(50) DEFAULT NULL,
		PRIMARY KEY (mid),
		KEY ind_bm_parent (ParentId,mid)
	)`)
	// mp should have more columns than m
	tk.MustExec(`CREATE TABLE mp (
		mpid bigint(20) unsigned NOT NULL DEFAULT '0',
		mid varchar(50) DEFAULT NULL COMMENT '模块主键',
		sid int,
	PRIMARY KEY (mpid)
	);`)

	tk.MustExec(`insert into mp values("1","1","0");`)
	tk.MustExec(`insert into m values("0", "0");`)
	rs := tk.MustQuery(`SELECT ( SELECT COUNT(1) FROM m WHERE ParentId = c.mid ) expand,  bmp.mpid,  bmp.mpid IS NULL,bmp.mpid IS NOT NULL, sid FROM m c LEFT JOIN mp bmp ON c.mid = bmp.mid  WHERE c.ParentId = '0'`)
	rs.Check(testkit.Rows("1 <nil> 1 0 <nil>"))

	rs = tk.MustQuery(`SELECT  bmp.mpid,  bmp.mpid IS NULL,bmp.mpid IS NOT NULL FROM m c LEFT JOIN mp bmp ON c.mid = bmp.mid  WHERE c.ParentId = '0'`)
	rs.Check(testkit.Rows("<nil> 1 0"))
}

func (s *testSerialSuite) TestTxnWriteThroughputSLI(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int key, b int)")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/util/sli/CheckTxnWriteThroughput", "return(true)"), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/util/sli/CheckTxnWriteThroughput")
		c.Assert(err, IsNil)
	}()

	mustExec := func(sql string) {
		tk.MustExec(sql)
		tk.Se.GetTxnWriteThroughputSLI().FinishExecuteStmt(time.Second, tk.Se.AffectedRows(), tk.Se.GetSessionVars().InTxn())
	}
	errExec := func(sql string) {
		_, err := tk.Exec(sql)
		c.Assert(err, NotNil)
		tk.Se.GetTxnWriteThroughputSLI().FinishExecuteStmt(time.Second, tk.Se.AffectedRows(), tk.Se.GetSessionVars().InTxn())
	}

	// Test insert in small txn
	mustExec("insert into t values (1,3),(2,4)")
	writeSLI := tk.Se.GetTxnWriteThroughputSLI()
	c.Assert(writeSLI.IsInvalid(), Equals, false)
	c.Assert(writeSLI.IsSmallTxn(), Equals, true)
	c.Assert(tk.Se.GetTxnWriteThroughputSLI().String(), Equals, "invalid: false, affectRow: 2, writeSize: 58, readKeys: 0, writeKeys: 2, writeTime: 1s")
	tk.Se.GetTxnWriteThroughputSLI().Reset()

	// Test insert ... select ... from
	mustExec("insert into t select b, a from t")
	c.Assert(writeSLI.IsInvalid(), Equals, true)
	c.Assert(writeSLI.IsSmallTxn(), Equals, true)
	c.Assert(tk.Se.GetTxnWriteThroughputSLI().String(), Equals, "invalid: true, affectRow: 2, writeSize: 58, readKeys: 0, writeKeys: 2, writeTime: 1s")
	tk.Se.GetTxnWriteThroughputSLI().Reset()

	// Test for delete
	mustExec("delete from t")
	c.Assert(tk.Se.GetTxnWriteThroughputSLI().String(), Equals, "invalid: false, affectRow: 4, writeSize: 76, readKeys: 0, writeKeys: 4, writeTime: 1s")
	tk.Se.GetTxnWriteThroughputSLI().Reset()

	// Test insert not in small txn
	mustExec("begin")
	for i := 0; i < 20; i++ {
		mustExec(fmt.Sprintf("insert into t values (%v,%v)", i, i))
		c.Assert(writeSLI.IsSmallTxn(), Equals, true)
	}
	// The statement which affect rows is 0 shouldn't record into time.
	mustExec("select count(*) from t")
	mustExec("select * from t")
	mustExec("insert into t values (20,20)")
	c.Assert(writeSLI.IsSmallTxn(), Equals, false)
	mustExec("commit")
	c.Assert(writeSLI.IsInvalid(), Equals, false)
	c.Assert(tk.Se.GetTxnWriteThroughputSLI().String(), Equals, "invalid: false, affectRow: 21, writeSize: 609, readKeys: 0, writeKeys: 21, writeTime: 22s")
	tk.Se.GetTxnWriteThroughputSLI().Reset()

	// Test invalid when transaction has replace ... select ... from ... statement.
	mustExec("delete from t")
	tk.Se.GetTxnWriteThroughputSLI().Reset()
	mustExec("begin")
	mustExec("insert into t values (1,3),(2,4)")
	mustExec("replace into t select b, a from t")
	mustExec("commit")
	c.Assert(writeSLI.IsInvalid(), Equals, true)
	c.Assert(tk.Se.GetTxnWriteThroughputSLI().String(), Equals, "invalid: true, affectRow: 4, writeSize: 116, readKeys: 0, writeKeys: 4, writeTime: 3s")
	tk.Se.GetTxnWriteThroughputSLI().Reset()

	// Test clean last failed transaction information.
	err := failpoint.Disable("github.com/pingcap/tidb/util/sli/CheckTxnWriteThroughput")
	c.Assert(err, IsNil)
	mustExec("begin")
	mustExec("insert into t values (1,3),(2,4)")
	errExec("commit")
	c.Assert(tk.Se.GetTxnWriteThroughputSLI().String(), Equals, "invalid: false, affectRow: 0, writeSize: 0, readKeys: 0, writeKeys: 0, writeTime: 0s")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/util/sli/CheckTxnWriteThroughput", "return(true)"), IsNil)
	mustExec("begin")
	mustExec("insert into t values (5, 6)")
	mustExec("commit")
	c.Assert(tk.Se.GetTxnWriteThroughputSLI().String(), Equals, "invalid: false, affectRow: 1, writeSize: 29, readKeys: 0, writeKeys: 1, writeTime: 2s")

	// Test for reset
	tk.Se.GetTxnWriteThroughputSLI().Reset()
	c.Assert(tk.Se.GetTxnWriteThroughputSLI().String(), Equals, "invalid: false, affectRow: 0, writeSize: 0, readKeys: 0, writeKeys: 0, writeTime: 0s")
}

func (s *testSuite) TestIssue23993(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Real cast to time should return NULL
	tk.MustExec("drop table if exists t_issue_23993")
	tk.MustExec("create table t_issue_23993(a double)")
	tk.MustExec("insert into t_issue_23993 values(-790822912)")
	tk.MustQuery("select cast(a as time) from t_issue_23993").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select a from t_issue_23993 where cast(a as time)").Check(testkit.Rows())
	// Int cast to time should return NULL
	tk.MustExec("drop table if exists t_issue_23993")
	tk.MustExec("create table t_issue_23993(a int)")
	tk.MustExec("insert into t_issue_23993 values(-790822912)")
	tk.MustQuery("select cast(a as time) from t_issue_23993").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select a from t_issue_23993 where cast(a as time)").Check(testkit.Rows())
	// Decimal cast to time should return NULL
	tk.MustExec("drop table if exists t_issue_23993")
	tk.MustExec("create table t_issue_23993(a decimal)")
	tk.MustExec("insert into t_issue_23993 values(-790822912)")
	tk.MustQuery("select cast(a as time) from t_issue_23993").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select a from t_issue_23993 where cast(a as time)").Check(testkit.Rows())
	// String cast to time should not return NULL
	tk.MustExec("drop table if exists t_issue_23993")
	tk.MustExec("create table t_issue_23993(a varchar(255))")
	tk.MustExec("insert into t_issue_23993 values('-790822912')")
	tk.MustQuery("select cast(a as time) from t_issue_23993").Check(testkit.Rows("-838:59:59"))
	tk.MustQuery("select a from t_issue_23993 where cast(a as time)").Check(testkit.Rows("-790822912"))
}

func (s *testSuiteP2) TestProjectionBitType(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(k1 int, v bit(34) DEFAULT b'111010101111001001100111101111111', primary key(k1) clustered);")
	tk.MustExec("create table t1(k1 int, v bit(34) DEFAULT b'111010101111001001100111101111111', primary key(k1) nonclustered);")
	tk.MustExec("insert into t(k1) select 1;")
	tk.MustExec("insert into t1(k1) select 1;")

	tk.MustExec("set @@tidb_enable_vectorized_expression = 0;")
	// following SQL should returns same result
	tk.MustQuery("(select * from t where false) union(select * from t for update);").Check(testkit.Rows("1 \x01\xd5\xe4\xcf\u007f"))
	tk.MustQuery("(select * from t1 where false) union(select * from t1 for update);").Check(testkit.Rows("1 \x01\xd5\xe4\xcf\u007f"))

	tk.MustExec("set @@tidb_enable_vectorized_expression = 1;")
	tk.MustQuery("(select * from t where false) union(select * from t for update);").Check(testkit.Rows("1 \x01\xd5\xe4\xcf\u007f"))
	tk.MustQuery("(select * from t1 where false) union(select * from t1 for update);").Check(testkit.Rows("1 \x01\xd5\xe4\xcf\u007f"))
}

func (s *testSuite) TestIssue23609(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE `t1` (\n  `a` timestamp NULL DEFAULT NULL,\n  `b` year(4) DEFAULT NULL,\n  KEY `a` (`a`),\n  KEY `b` (`b`)\n)")
	tk.MustExec("insert into t1 values(\"2002-10-03 04:28:53\",2000), (\"2002-10-03 04:28:53\",2002), (NULL, 2002)")
	tk.MustQuery("select /*+ inl_join (x,y) */ * from t1 x cross join t1 y on x.a=y.b").Check(testkit.Rows())
	tk.MustQuery("select * from t1 x cross join t1 y on x.a>y.b order by x.a, x.b, y.a, y.b").Check(testkit.Rows("2002-10-03 04:28:53 2000 <nil> 2002", "2002-10-03 04:28:53 2000 2002-10-03 04:28:53 2000", "2002-10-03 04:28:53 2000 2002-10-03 04:28:53 2002", "2002-10-03 04:28:53 2002 <nil> 2002", "2002-10-03 04:28:53 2002 2002-10-03 04:28:53 2000", "2002-10-03 04:28:53 2002 2002-10-03 04:28:53 2002"))
	tk.MustQuery("select * from t1 where a = b").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where a < b").Check(testkit.Rows())
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))
}

func (s *testSuite1) TestIssue24091(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	defer tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int) partition by hash (a div 0) partitions 10;")
	tk.MustExec("insert into t values (NULL);")

	tk.MustQuery("select null div 0;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("<nil>"))
}

func (s *testSerialSuite) TestIssue24210(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// for ProjectionExec
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockProjectionExecBaseExecutorOpenReturnedError", `return(true)`), IsNil)
	_, err := tk.Exec("select a from (select 1 as a, 2 as b) t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "mock ProjectionExec.baseExecutor.Open returned error")
	err = failpoint.Disable("github.com/pingcap/tidb/executor/mockProjectionExecBaseExecutorOpenReturnedError")
	c.Assert(err, IsNil)

	// for HashAggExec
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockHashAggExecBaseExecutorOpenReturnedError", `return(true)`), IsNil)
	_, err = tk.Exec("select sum(a) from (select 1 as a, 2 as b) t group by b")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "mock HashAggExec.baseExecutor.Open returned error")
	err = failpoint.Disable("github.com/pingcap/tidb/executor/mockHashAggExecBaseExecutorOpenReturnedError")
	c.Assert(err, IsNil)

	// for StreamAggExec
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockStreamAggExecBaseExecutorOpenReturnedError", `return(true)`), IsNil)
	_, err = tk.Exec("select sum(a) from (select 1 as a, 2 as b) t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "mock StreamAggExec.baseExecutor.Open returned error")
	err = failpoint.Disable("github.com/pingcap/tidb/executor/mockStreamAggExecBaseExecutorOpenReturnedError")
	c.Assert(err, IsNil)

	// for SelectionExec
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockSelectionExecBaseExecutorOpenReturnedError", `return(true)`), IsNil)
	_, err = tk.Exec("select * from (select rand() as a) t where a > 0")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "mock SelectionExec.baseExecutor.Open returned error")
	err = failpoint.Disable("github.com/pingcap/tidb/executor/mockSelectionExecBaseExecutorOpenReturnedError")
	c.Assert(err, IsNil)

}

func (s testSerialSuite) TestExprBlackListForEnum(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a enum('a','b','c'), b enum('a','b','c'), c int, index idx(b,a));")
	tk.MustExec("insert into t values(1,1,1),(2,2,2),(3,3,3);")

	checkFuncPushDown := func(rows [][]interface{}, keyWord string) bool {
		for _, line := range rows {
			// Agg/Expr push down
			if line[2].(string) == "cop[tikv]" && strings.Contains(line[4].(string), keyWord) {
				return true
			}
			// access index
			if line[2].(string) == "cop[tikv]" && strings.Contains(line[3].(string), keyWord) {
				return true
			}
		}
		return false
	}

	// Test agg(enum) push down
	tk.MustExec("insert into mysql.expr_pushdown_blacklist(name) values('enum');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows := tk.MustQuery("desc format='brief' select /*+ HASH_AGG() */ max(a) from t;").Rows()
	c.Assert(checkFuncPushDown(rows, "max"), IsFalse)
	rows = tk.MustQuery("desc format='brief' select /*+ STREAM_AGG() */ max(a) from t;").Rows()
	c.Assert(checkFuncPushDown(rows, "max"), IsFalse)

	tk.MustExec("delete from mysql.expr_pushdown_blacklist;")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select /*+ HASH_AGG() */ max(a) from t;").Rows()
	c.Assert(checkFuncPushDown(rows, "max"), IsTrue)
	rows = tk.MustQuery("desc format='brief' select /*+ STREAM_AGG() */ max(a) from t;").Rows()
	c.Assert(checkFuncPushDown(rows, "max"), IsTrue)

	// Test expr(enum) push down
	tk.MustExec("insert into mysql.expr_pushdown_blacklist(name) values('enum');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select * from t where a + b;").Rows()
	c.Assert(checkFuncPushDown(rows, "plus"), IsFalse)
	rows = tk.MustQuery("desc format='brief' select * from t where a + b;").Rows()
	c.Assert(checkFuncPushDown(rows, "plus"), IsFalse)

	tk.MustExec("delete from mysql.expr_pushdown_blacklist;")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select * from t where a + b;").Rows()
	c.Assert(checkFuncPushDown(rows, "plus"), IsTrue)
	rows = tk.MustQuery("desc format='brief' select * from t where a + b;").Rows()
	c.Assert(checkFuncPushDown(rows, "plus"), IsTrue)

	// Test enum index
	tk.MustExec("insert into mysql.expr_pushdown_blacklist(name) values('enum');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select * from t where b = 1;").Rows()
	c.Assert(checkFuncPushDown(rows, "index:idx(b)"), IsFalse)
	rows = tk.MustQuery("desc format='brief' select * from t where b = 'a';").Rows()
	c.Assert(checkFuncPushDown(rows, "index:idx(b)"), IsFalse)
	rows = tk.MustQuery("desc format='brief' select * from t where b > 1;").Rows()
	c.Assert(checkFuncPushDown(rows, "index:idx(b)"), IsFalse)
	rows = tk.MustQuery("desc format='brief' select * from t where b > 'a';").Rows()
	c.Assert(checkFuncPushDown(rows, "index:idx(b)"), IsFalse)

	tk.MustExec("delete from mysql.expr_pushdown_blacklist;")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = tk.MustQuery("desc format='brief' select * from t where b = 1 and a = 1;").Rows()
	c.Assert(checkFuncPushDown(rows, "index:idx(b, a)"), IsTrue)
	rows = tk.MustQuery("desc format='brief' select * from t where b = 'a' and a = 'a';").Rows()
	c.Assert(checkFuncPushDown(rows, "index:idx(b, a)"), IsTrue)
	rows = tk.MustQuery("desc format='brief' select * from t where b = 1 and a > 1;").Rows()
	c.Assert(checkFuncPushDown(rows, "index:idx(b, a)"), IsTrue)
	rows = tk.MustQuery("desc format='brief' select * from t where b = 1 and a > 'a'").Rows()
	c.Assert(checkFuncPushDown(rows, "index:idx(b, a)"), IsTrue)
}
