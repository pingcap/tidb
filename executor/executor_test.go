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
	"math"
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
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	autoid.SetStep(5000)
	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

var _ = Suite(&testSuite{})
var _ = Suite(&testSuite1{})
var _ = Suite(&testSuite2{})
var _ = Suite(&testSuite3{})
var _ = Suite(&testSuite4{})
var _ = SerialSuites(&testShowStatsSuite{testSuite{}})
var _ = Suite(&testBypassSuite{})
var _ = Suite(&testUpdateSuite{})
var _ = Suite(&testOOMSuite{})
var _ = Suite(&testPointGetSuite{})
var _ = Suite(&testFlushSuite{})
var _ = SerialSuites(&testShowStatsSuite{})

type testSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *testSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
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
}

func (s *testSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testSuite) TestPessimisticSelectForUpdate(c *C) {
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

func (s *testSuite) TestAdmin(c *C) {
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
	c.Assert(row.GetString(1), Equals, "error: [admin:4]DDL Job:1 not found")

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
	do := domain.GetDomain(tk.Se.(sessionctx.Context))
	serverInfo, err := do.InfoSyncer().GetServerInfoByID(ctx, row.GetString(1))
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
	_, err = tb.Indices()[0].Create(mock.NewContext(), txn, types.MakeDatums(int64(10)), 1)
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
	s.cluster.SplitKeys(s.mvccStore, startKey, endKey, int(historyJobs[0].ID/5))

	historyJobs2, err := admin.GetHistoryDDLJobs(txn, 20)
	c.Assert(err, IsNil)
	c.Assert(historyJobs, DeepEquals, historyJobs2)
}

func (s *testSuite) TestAdminShowDDLJobs(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_admin_show_ddl_jobs")
	tk.MustExec("use test_admin_show_ddl_jobs")
	tk.MustExec("create table t (a int);")

	re := tk.MustQuery("admin show ddl jobs 1")
	row := re.Rows()[0]
	c.Assert(row[1], Equals, "test_admin_show_ddl_jobs")
	jobID, err := strconv.Atoi(row[0].(string))
	c.Assert(err, IsNil)

	c.Assert(tk.Se.NewTxn(context.Background()), IsNil)
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
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
	err = tk.Se.CommitTxn(context.Background())
	c.Assert(err, IsNil)

	re = tk.MustQuery("admin show ddl jobs 1")
	row = re.Rows()[0]
	c.Assert(row[1], Equals, "test_admin_show_ddl_jobs")
}

func (s *testSuite) TestAdminChecksumOfPartitionedTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS admin_checksum_partition_test;")
	tk.MustExec("CREATE TABLE admin_checksum_partition_test (a INT) PARTITION BY HASH(a) PARTITIONS 4;")
	tk.MustExec("INSERT INTO admin_checksum_partition_test VALUES (1), (2);")

	r := tk.MustQuery("ADMIN CHECKSUM TABLE admin_checksum_partition_test;")
	r.Check(testkit.Rows("test admin_checksum_partition_test 1 5 5"))
}

func (s *testSuite) fillData(tk *testkit.TestKit, table string) {
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
		if tt.restData == nil {
			c.Assert(data, HasLen, 0,
				Commentf("data1:%v, data2:%v, data:%v", string(tt.data1), string(tt.data2), string(data)))
		} else {
			c.Assert(data, DeepEquals, tt.restData,
				Commentf("data1:%v, data2:%v, data:%v", string(tt.data1), string(tt.data2), string(data)))
		}
		ld.SetMessage()
		tk.CheckLastMessage(tt.expectedMsg)
		err := ctx.StmtCommit()
		c.Assert(err, IsNil)
		txn, err := ctx.Txn(true)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		r := tk.MustQuery(selectSQL)
		r.Check(testutil.RowsWithSep("|", tt.expected...))
		tk.MustExec(deleteSQL)
	}
}

func (s *testSuite) TestSelectWithoutFrom(c *C) {
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
func (s *testSuite) TestSelectBackslashN(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select \N;`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "NULL")

	sql = `select "\N";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)

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

	sql = `select \N from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)

	sql = `select (\N) from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)

	sql = "select `\\N` from test;"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `\N`)

	sql = "select (`\\N`) from test;"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `\N`)

	sql = `select '\N' from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)

	sql = `select ('\N') from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)
}

// TestSelectNull Issue #4053.
func (s *testSuite) TestSelectNull(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select nUll;`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)

	sql = `select (null);`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)

	sql = `select null+NULL;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `null+NULL`)
}

// TestSelectStringLiteral Issue #3686.
func (s *testSuite) TestSelectStringLiteral(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select 'abc';`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("abc"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `abc`)

	sql = `select (('abc'));`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("abc"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `abc`)

	sql = `select 'abc'+'def';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("0"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `'abc'+'def'`)

	// Below checks whether leading invalid chars are trimmed.
	sql = "select '\n';"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("\n"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "")

	sql = "select '\t   col';" // Lowercased letter is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "col")

	sql = "select '\t   Col';" // Uppercased letter is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "Col")

	sql = "select '\n\t   中文 col';" // Chinese char is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "中文 col")

	sql = "select ' \r\n  .col';" // Punctuation is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, ".col")

	sql = "select '   😆col';" // Emoji is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "😆col")

	// Below checks whether trailing invalid chars are preserved.
	sql = `select 'abc   ';`
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "abc   ")

	sql = `select '  abc   123   ';`
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "abc   123   ")

	// Issue #4239.
	sql = `select 'a' ' ' 'string';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("a string"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "a")

	sql = `select 'a' " " "string";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("a string"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "a")

	sql = `select 'string' 'string';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("stringstring"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "string")

	sql = `select "ss" "a";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")

	sql = `select "ss" "a" "b";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssab"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")

	sql = `select "ss" "a" ' ' "b";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa b"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")

	sql = `select "ss" "a" ' ' "b" ' ' "d";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa b d"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")
}

func (s *testSuite) TestSelectLimit(c *C) {
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

func (s *testSuite) TestSelectOrderBy(c *C) {
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

func (s *testSuite) TestOrderBy(c *C) {
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

func (s *testSuite) TestSelectErrorRow(c *C) {
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
func (s *testSuite) TestIssue2612(c *C) {
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
	rs.Close()
}

// TestIssue345 is related with https://github.com/pingcap/tidb/issues/345
func (s *testSuite) TestIssue345(c *C) {
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

func (s *testSuite) TestIssue5055(c *C) {
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

func (s *testSuite) TestUnion(c *C) {
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
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrWrongUsage))

	err = tk.ExecToErr("select 1 from (select a from t order by a union all select a from t limit 1) tmp")
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrWrongUsage))

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
}

func (s *testSuite) TestUnionLimit(c *C) {
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

func (s *testSuite) TestNeighbouringProj(c *C) {
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

func (s *testSuite) TestIn(c *C) {
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

func (s *testSuite) TestTablePKisHandleScan(c *C) {
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

func (s *testSuite) TestIndexScan(c *C) {
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
	result = tk.MustQuery(`SELECT * FROM (SELECT * FROM (SELECT a as d FROM t WHERE a IN ('100')) AS x WHERE x.d < "123" ) tmp_count"`)
	result.Check(testkit.Rows())
}

func (s *testSuite) TestIndexReverseOrder(c *C) {
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

func (s *testSuite) TestTableReverseOrder(c *C) {
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

func (s *testSuite) TestDefaultNull(c *C) {
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

func (s *testSuite) TestUnsignedPKColumn(c *C) {
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

func (s *testSuite) TestJSON(c *C) {
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
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrBlobCantHaveDefault))

	_, err = tk.Exec(`create table test_bad_json(a blob default 'hello')`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrBlobCantHaveDefault))

	_, err = tk.Exec(`create table test_bad_json(a text default 'world')`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrBlobCantHaveDefault))

	// check json fields cannot be used as key.
	_, err = tk.Exec(`create table test_bad_json(id int, a json, key (a))`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrJSONUsedAsKey))

	// check CAST AS JSON.
	result = tk.MustQuery(`select CAST('3' AS JSON), CAST('{}' AS JSON), CAST(null AS JSON)`)
	result.Check(testkit.Rows(`3 {} <nil>`))

	// Check cast json to decimal.
	tk.MustExec("drop table if exists test_json")
	tk.MustExec("create table test_json ( a decimal(60,2) as (JSON_EXTRACT(b,'$.c')), b json );")
	tk.MustExec(`insert into test_json (b) values
		('{"c": "1267.1"}'),
		('{"c": "1267.01"}'),
		('{"c": "1267.1234"}'),
		('{"c": "1267.3456"}'),
		('{"c": "1234567890123456789012345678901234567890123456789012345"}'),
		('{"c": "1234567890123456789012345678901234567890123456789012345.12345"}');`)

	tk.MustQuery("select a from test_json;").Check(testkit.Rows("1267.10", "1267.01", "1267.12",
		"1267.35", "1234567890123456789012345678901234567890123456789012345.00",
		"1234567890123456789012345678901234567890123456789012345.12"))
}

func (s *testSuite) TestMultiUpdate(c *C) {
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
	result.Check(testkit.Rows(`1 7 2`, `4 0 0`, `7 8 9`))

	tk.MustExec(`UPDATE test_mu SET c = 8, b = c WHERE a = 4`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 8 8`, `7 8 9`))

	tk.MustExec(`UPDATE test_mu SET c = b, b = c WHERE a = 7`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 8 8`, `7 8 8`))
}

func (s *testSuite) TestGeneratedColumnWrite(c *C) {
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
			c.Assert(terr.Code(), Equals, terror.ErrCode(tt.err), Commentf("sql is %v", tt.stmt))
		} else {
			c.Assert(err, IsNil)
		}
	}
}

// TestGeneratedColumnRead tests select generated columns from table.
// They should be calculated from their generation expressions.
func (s *testSuite) TestGeneratedColumnRead(c *C) {
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
	tk.MustExec(`UPDATE test_gc_read m, test_gc_read n SET m.a = m.a + 10, n.a = n.a + 10`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`10 <nil> <nil> <nil> <nil>`, `11 2 13 22 26`, `13 4 17 52 34`, `18 8 26 144 52`))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(18)")
	tk.MustExec("update test_gc_read set a = a+1 where a in (select a from t)")
	result = tk.MustQuery("select * from test_gc_read order by a")
	result.Check(testkit.Rows(`10 <nil> <nil> <nil> <nil>`, `11 2 13 22 26`, `13 4 17 52 34`, `19 8 27 152 54`))

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
			c.Assert(terr.Code(), Equals, terror.ErrCode(tt.err))
		} else {
			c.Assert(err, IsNil)
		}
	}
}

func (s *testSuite) TestToPBExpr(c *C) {
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

func (s *testSuite) TestDatumXAPI(c *C) {
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

func (s *testSuite) TestSQLMode(c *C) {
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
	_, err = tk.Exec("insert t values (null)")
	c.Check(err, NotNil)
	tk.MustExec("insert ignore t values (null)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null"))
	tk.MustExec("insert t select null")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'a' cannot be null"))
	tk.MustExec("insert t values (1000)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("0", "0", "0", "127"))

	tk.MustExec("insert tdouble values (10.23)")
	tk.MustQuery("select * from tdouble").Check(testkit.Rows("9.99"))

	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	tk.MustExec("set @@global.sql_mode = ''")

	// Disable global variable cache, so load global session variable take effect immediate.
	s.domain.GetGlobalVarsCache().Disable()
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")
	tk2.MustExec("create table t2 (a varchar(3))")
	tk2.MustExec("insert t2 values ('abcd')")
	tk2.MustQuery("select * from t2").Check(testkit.Rows("abc"))

	// session1 is still in strict mode.
	_, err = tk.Exec("insert t2 values ('abcd')")
	c.Check(err, NotNil)
	// Restore original global strict mode.
	tk.MustExec("set @@global.sql_mode = 'STRICT_TRANS_TABLES'")
}

func (s *testSuite) TestTableDual(c *C) {
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

	tk.MustExec("create table t(a int primary key)")
	tk.MustQuery("select t1.* from t t1, t t2 where t1.a=t2.a and 1=0").Check(testkit.Rows())
}

func (s *testSuite) TestTableScan(c *C) {
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

func (s *testSuite) TestAdapterStatement(c *C) {
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

func (s *testSuite) TestIsPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use mysql")
	ctx := tk.Se.(sessionctx.Context)
	tests := map[string]bool{
		"select * from help_topic where name='aaa'":         false,
		"select 1 from help_topic where name='aaa'":         true,
		"select * from help_topic where help_topic_id=1":    true,
		"select * from help_topic where help_category_id=1": false,
	}
	infoSchema := executor.GetInfoSchema(ctx)

	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		c.Check(err, IsNil)
		err = plannercore.Preprocess(ctx, stmtNode, infoSchema)
		c.Check(err, IsNil)
		p, err := planner.Optimize(context.TODO(), ctx, stmtNode, infoSchema)
		c.Check(err, IsNil)
		ret, err := executor.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, result)
	}
}

func (s *testSuite) TestPointGetRepeatableRead(c *C) {
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

func (s *testSuite) TestSplitRegionTimeout(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/MockSplitRegionTimeout", `return(true)`), IsNil)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(100),b int, index idx1(b,a))")
	tk.MustExec(`split table t index idx1 by (10000,"abcd"),(10000000);`)
	tk.MustExec(`set @@tidb_wait_split_region_timeout=1`)
	// result 0 0 means split 0 region and 0 region finish scatter regions before timeout.
	tk.MustQuery(`split table t between (0) and (10000) regions 10`).Check(testkit.Rows("0 0"))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/MockSplitRegionTimeout"), IsNil)

	// Test scatter regions timeout.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/MockScatterRegionTimeout", `return(true)`), IsNil)
	tk.MustQuery(`split table t between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/MockScatterRegionTimeout"), IsNil)

	// Test pre-split with timeout.
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/MockScatterRegionTimeout", `return(true)`), IsNil)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	start := time.Now()
	tk.MustExec("create table t (a int, b int) partition by hash(a) partitions 5;")
	c.Assert(time.Since(start).Seconds(), Less, 10.0)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/MockScatterRegionTimeout"), IsNil)
}

func (s *testSuite) TestRow(c *C) {
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
}

func (s *testSuite) TestColumnName(c *C) {
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
	rs.Close()
	rs, err = tk.Exec("select (c) > all (select c from t) from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.L, Equals, "(c) > all (select c from t)")
	c.Check(fields[0].ColumnAsName.L, Equals, "(c) > all (select c from t)")
	rs.Close()
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
	rs.Close()
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
	rs.Close()
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
	rs.Close()
	// Test case for query a column wrapped with parentheses and unary plus.
	// In this case, the column name should be its original name.
	rs, err = tk.Exec("select (c), (+c), +(c), +(+(c)), ++c from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	for i := 0; i < 5; i++ {
		c.Check(fields[i].Column.Name.L, Equals, "c")
		c.Check(fields[i].ColumnAsName.L, Equals, "c")
	}
	rs.Close()

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
	rs.Close()

	rs, err = tk.Exec("select if(1,c,c) from t;")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Assert(fields[0].Column.Name.L, Equals, "if(1,c,c)")
	// It's a compatibility issue. Should be empty instead.
	c.Assert(fields[0].ColumnAsName.L, Equals, "if(1,c,c)")
}

func (s *testSuite) TestSelectVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (d int)")
	tk.MustExec("insert into t values(1), (2), (1)")
	// This behavior is different from MySQL.
	result := tk.MustQuery("select @a, @a := d+1 from t")
	result.Check(testkit.Rows("<nil> 2", "2 3", "3 2"))
}

func (s *testSuite) TestHistoryRead(c *C) {
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

	curVer1, _ := s.store.CurrentVersion()
	time.Sleep(time.Millisecond)
	snapshotTime := time.Now()
	time.Sleep(time.Millisecond)
	curVer2, _ := s.store.CurrentVersion()
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

func (s *testSuite) TestLowResolutionTSORead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@autocommit=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists low_resolution_tso")
	tk.MustExec("create table low_resolution_tso(a int)")
	tk.MustExec("insert low_resolution_tso values (1)")

	// enable low resolution tso
	c.Assert(tk.Se.GetSessionVars().LowResolutionTSO, IsFalse)
	tk.Exec("set @@tidb_low_resolution_tso = 'on'")
	c.Assert(tk.Se.GetSessionVars().LowResolutionTSO, IsTrue)

	time.Sleep(3 * time.Second)
	tk.MustQuery("select * from low_resolution_tso").Check(testkit.Rows("1"))
	_, err := tk.Exec("update low_resolution_tso set a = 2")
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
	c.Assert(terror.ErrorEqual(err, variable.ErrReadOnly), IsTrue, Commentf("err %v", err))
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
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec("insert into t values ('abc')")
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))

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

type testSuite1 struct {
	store kv.Storage
	dom   *domain.Domain
	cli   *checkRequestClient
}

func (s *testSuite1) SetUpSuite(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithHijackClient(hijackClient),
	)
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom.SetStatsUpdating(true)
}

func (s *testSuite1) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testSuite1) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuite1) TestAddIndexPriority(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}

	store, err := mockstore.NewMockTikvStore(
		mockstore.WithHijackClient(hijackClient),
	)
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
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
	tk.Se.Execute(ctx1, `select * from t where ts = "2018-09-13 10:02:06"`)

	tk.MustExec(`set time_zone="System"`)
	tk.Se.Execute(ctx1, `select * from t where ts = "2018-09-13 10:02:06"`)

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
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrWrongTableName))
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

	alloc := autoid.NewAllocator(s.store, dbInfo.ID, false)
	tb, err := tables.TableFromMeta(alloc, tbInfo)
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
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(30)), 3)
	c.Assert(err, IsNil)
	key := tablecodec.EncodeRowKey(tb.Meta().ID, codec.EncodeInt(nil, 4))
	setColValue(c, txn, key, types.NewDatum(int64(40)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "handle 3, index:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:30, b:[]uint8(nil), x:interface {}(nil)} != record:<nil>")

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// table     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	_, err = idx.Create(mockCtx, txn, types.MakeDatums(int64(40)), 4)
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
	err = idx.Delete(sc, txn, types.MakeDatums(int64(30)), 3, nil)
	c.Assert(err, IsNil)
	err = idx.Delete(sc, txn, types.MakeDatums(int64(20)), 2, nil)
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
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil)
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
	tk.MustExec(`insert into t values(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6);`)
	tk.MustQuery(`select * from t order by a limit 1, 1;`).Check(testkit.Rows(
		"2 2",
	))
	tk.MustQuery(`select * from t order by a limit 1, 2;`).Check(testkit.Rows(
		"2 2",
		"3 3",
	))
	tk.MustQuery(`select * from t order by a limit 1, 3;`).Check(testkit.Rows(
		"2 2",
		"3 3",
		"4 4",
	))
	tk.MustQuery(`select * from t order by a limit 1, 4;`).Check(testkit.Rows(
		"2 2",
		"3 3",
		"4 4",
		"5 5",
	))
	tk.MustExec(`set @@tidb_init_chunk_size=2;`)
	tk.MustQuery(`select * from t order by a limit 2, 1;`).Check(testkit.Rows(
		"3 3",
	))
	tk.MustQuery(`select * from t order by a limit 2, 2;`).Check(testkit.Rows(
		"3 3",
		"4 4",
	))
	tk.MustQuery(`select * from t order by a limit 2, 3;`).Check(testkit.Rows(
		"3 3",
		"4 4",
		"5 5",
	))
	tk.MustQuery(`select * from t order by a limit 2, 4;`).Check(testkit.Rows(
		"3 3",
		"4 4",
		"5 5",
		"6 6",
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
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1105|Division by 0"))
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
	// 'For update' would be ignored if 'order by' or 'limit' exists.
	tk1.MustQuery("select 1 as a union select a from t limit 5 for update")
	tk1.MustQuery("select 1 as a union select a from t order by a for update")

	tk2.MustExec("update t set a = a + 1")

	_, err = tk1.Exec("commit")
	c.Assert(err, IsNil)
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
			res.Close()
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
	tk.MustQuery("select a.* from a inner join (select 1 as k1,'k2-1' as k2) as k on a.f1=k.k1;").
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

func (s *testSuite3) TestUnionAutoSignedCast(c *C) {
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

func (s *testSuite3) TestUpdateJoin(c *C) {
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
	c.Assert(err.Error(), Equals, "subquery returns more than 1 row")

	err = rs.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite3) TestCurrentTimestampValueSelection(c *C) {
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

func (s *testSuite3) TestTSOFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int)`)

	ctx := context.Background()
	ctx = context.WithValue(ctx, "mockGetTSFail", struct{}{})
	_, err := tk.Se.Execute(ctx, `select * from t`)
	c.Assert(err, NotNil)
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

func (s *testSuite3) TestSelectPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists th, tr`)
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec(`create table th (a int, b int) partition by hash(a) partitions 3;`)
	tk.MustExec(`create table tr (a int, b int)
							partition by range (a) (
							partition r0 values less than (4),
							partition r1 values less than (7),
							partition r3 values less than maxvalue)`)
	defer tk.MustExec(`drop table if exists th, tr`)
	tk.MustExec(`insert into th values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);`)
	tk.MustExec("insert into th values (-1,-1),(-2,-2),(-3,-3),(-4,-4),(-5,-5),(-6,-6),(-7,-7),(-8,-8);")
	tk.MustExec(`insert into tr values (-3,-3),(3,3),(4,4),(7,7),(8,8);`)
	// select 1 partition.
	tk.MustQuery("select b from th partition (p0) order by a").Check(testkit.Rows("-6", "-3", "0", "3", "6"))
	tk.MustQuery("select b from tr partition (r0) order by a").Check(testkit.Rows("-3", "3"))
	tk.MustQuery("select b from th partition (p0,P0) order by a").Check(testkit.Rows("-6", "-3", "0", "3", "6"))
	tk.MustQuery("select b from tr partition (r0,R0,r0) order by a").Check(testkit.Rows("-3", "3"))
	// select multi partition.
	tk.MustQuery("select b from th partition (P2,p0) order by a").Check(testkit.Rows("-8", "-6", "-5", "-3", "-2", "0", "2", "3", "5", "6", "8"))
	tk.MustQuery("select b from tr partition (r1,R3) order by a").Check(testkit.Rows("4", "7", "8"))

	// test select unknown partition error
	err := tk.ExecToErr("select b from th partition (p0,p4)")
	c.Assert(err.Error(), Equals, "[table:1735]Unknown partition 'p4' in table 'th'")
	err = tk.ExecToErr("select b from tr partition (r1,r4)")
	c.Assert(err.Error(), Equals, "[table:1735]Unknown partition 'r4' in table 'tr'")

	// test select partition table in transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into th values (10,10),(11,11)")
	tk.MustQuery("select a, b from th where b>10").Check(testkit.Rows("11 11"))
	tk.MustExec("commit")
	tk.MustQuery("select a, b from th where b>10").Check(testkit.Rows("11 11"))
}

func (s *testSuite) TestSelectView(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table view_t (a int,b int)")
	tk.MustExec("insert into view_t values(1,2)")
	tk.MustExec("create definer='root'@'localhost' view view1 as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view2(c,d) as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view3(c,d) as select a,b from view_t")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
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
	tk.MustExec("alter table view_t drop column a")
	tk.MustExec("alter table view_t add column a int after b")
	tk.MustExec("update view_t set a=1;")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop table view_t;")
	tk.MustExec("drop view view1,view2,view3;")

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
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

func (s *testSuite2) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
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
}

func (s *testSuite2) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testSuite2) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSuite3 struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

func (s *testSuite3) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
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
}

func (s *testSuite3) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testSuite3) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

type testSuite4 struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

func (s *testSuite4) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
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
}

func (s *testSuite4) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testSuite4) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

func (s *testSuite) TestStrToDateBuiltin(c *C) {
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
}

func (s *testSuite) TestReadPartitionedTable(c *C) {
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

func (s *testSuite) TestSplitRegion(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a varchar(100),b int, index idx1(b,a))")
	tk.MustExec(`split table t index idx1 by (10000,"abcd"),(10000000);`)
	_, err := tk.Exec(`split table t index idx1 by ("abcd");`)
	c.Assert(err, NotNil)
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.WarnDataTruncated))

	// Test for split index region.
	// Check min value is more than max value.
	tk.MustExec(`split table t index idx1 between (0) and (1000000000) regions 10`)
	_, err = tk.Exec(`split table t index idx1 between (2,'a') and (1,'c') regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index `idx1` region lower value (2,a) should less than the upper value (1,c)")

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
	_, err = tk.Exec(`split table t between (2) and (1) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split table `t` region lower value 2 should less than the upper value 1")

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
	_, err = tk.Exec(`split table t between (0) and (100) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split table `t` region step value should more than 1000, step 10 is invalid")

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

func (s *testSuite) TestShowTableRegion(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_regions")
	tk.MustExec("create table t_regions (a int key, b int, c int, index idx(b), index idx2(c))")

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
	tk.MustQuery(`split table t_regions index idx between (-1000) and (1000) regions 4;`).Check(testkit.Rows("5 1"))
	re = tk.MustQuery("show table t_regions index idx regions")
	rows = re.Rows()
	// The index `idx` of table t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	// Check the region start key.
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_i_1_", tbl.Meta().ID))
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
	c.Assert(rows[4][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[7][2], Equals, fmt.Sprintf("t_%d_i_2_", tbl.Meta().ID))
	c.Assert(rows[8][2], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))

	// Test unsigned primary key and wait scatter finish.
	tk.MustExec("drop table if exists t_regions")
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
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_i_1_", tbl.Meta().ID))
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
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_i_1_", p.ID))
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
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_i_1_", p.ID))
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
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_i_1_", p.ID))
		c.Assert(rows[i*8+5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+8][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+9][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+10][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+11][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}
}

func (s *testSuite) TestChangePumpAndDrainer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// change pump or drainer's state need connect to etcd
	// so will meet error "URL scheme must be http, https, unix, or unixs: /tmp/tidb"
	err := tk.ExecToErr("change pump to node_state ='paused' for node_id 'pump1'")
	c.Assert(err, ErrorMatches, "URL scheme must be http, https, unix, or unixs.*")
	err = tk.ExecToErr("change drainer to node_state ='paused' for node_id 'drainer1'")
	c.Assert(err, ErrorMatches, "URL scheme must be http, https, unix, or unixs.*")
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

func (s *testSuite) TestIssue10435(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(i int, j int, k int)")
	tk.MustExec("insert into t1 VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4)")
	tk.MustExec("INSERT INTO t1 SELECT 10*i,j,5*j FROM t1 UNION SELECT 20*i,j,5*j FROM t1 UNION SELECT 30*i,j,5*j FROM t1")

	tk.MustExec("set @@session.tidb_enable_window_function=1")
	tk.MustQuery("SELECT SUM(i) OVER W FROM t1 WINDOW w AS (PARTITION BY j ORDER BY i) ORDER BY 1+SUM(i) OVER w").Check(
		testkit.Rows("1", "2", "3", "4", "11", "22", "31", "33", "44", "61", "62", "93", "122", "124", "183", "244"),
	)
}

func (s *testSuite) TestUnsignedFeedback(c *C) {
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
	c.Assert(result.Rows()[2][3], Equals, "table:t, range:[0,+inf], keep order:false")
}

type testOOMSuite struct {
	store kv.Storage
	do    *domain.Domain
	oom   *oomCapturer
}

func (s *testOOMSuite) SetUpSuite(c *C) {
	c.Skip("log.ReplaceGlobals(lg, r) in registerHook() may result in data race")
	testleak.BeforeTest()
	s.registerHook()
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.SetSchemaLease(0)
	domain.RunAutoAnalyze = false
	s.do, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testOOMSuite) registerHook() {
	conf := &log.Config{Level: "info", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	s.oom = &oomCapturer{r.Core, ""}
	lg := zap.New(s.oom)
	log.ReplaceGlobals(lg, r)
}

func (s *testOOMSuite) TestDistSQLMemoryControl(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3)")

	s.oom.tracker = ""
	tk.MustQuery("select * from t")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaDistSQL = 1
	tk.MustQuery("select * from t")
	c.Assert(s.oom.tracker, Equals, "TableReaderDistSQLTracker")
	tk.Se.GetSessionVars().MemQuotaDistSQL = -1

	s.oom.tracker = ""
	tk.MustQuery("select a from t")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaDistSQL = 1
	tk.MustQuery("select a from t use index(idx_a)")
	c.Assert(s.oom.tracker, Equals, "IndexReaderDistSQLTracker")
	tk.Se.GetSessionVars().MemQuotaDistSQL = -1

	s.oom.tracker = ""
	tk.MustQuery("select * from t")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetSessionVars().MemQuotaDistSQL = 1
	tk.MustQuery("select * from t use index(idx_a)")
	c.Assert(s.oom.tracker, Equals, "IndexLookupDistSQLTracker")
	tk.Se.GetSessionVars().MemQuotaDistSQL = -1
}

func setOOMAction(action string) {
	newConf := config.NewConfig()
	newConf.OOMAction = action
	config.StoreGlobalConfig(newConf)
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
	orgAction := config.GetGlobalConfig().OOMAction
	setOOMAction(config.OOMActionCancel)
	defer func() {
		setOOMAction(orgAction)
	}()
	tk.MustExec("set @@tidb_mem_quota_query=1;")
	err := tk.QueryToErr("select sum(b) from t group by a;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")

	// Test insert from select oom panic.
	tk.MustExec("drop table if exists t,t1")
	tk.MustExec("create table t (a bigint);")
	tk.MustExec("create table t1 (a bigint);")
	tk.MustExec("insert into t1 values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@tidb_mem_quota_query=200;")
	_, err = tk.Exec("insert into t select a from t1 order by a desc;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
}

type oomCapturer struct {
	zapcore.Core
	tracker string
}

func (h *oomCapturer) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if strings.Contains(entry.Message, "memory exceeds quota") {
		err, _ := fields[0].Interface.(error)
		str := err.Error()
		begin := strings.Index(str, "8001]")
		if begin == -1 {
			panic("begin not found")
		}
		end := strings.Index(str, " holds")
		if end == -1 {
			panic("end not found")
		}
		h.tracker = str[begin+len("8001]") : end]
	}
	return nil
}

func (h *oomCapturer) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(e.Level) {
		return ce.AddCore(e, h)
	}
	return ce
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

func (s *testSuite) TestKillTableReader(c *C) {
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
		c.Assert(int(errors.Cause(tk.QueryToErr("select * from t")).(*terror.Error).ToSQLError().Code), Equals, int(executor.ErrQueryInterrupted.Code()))
	}()
	time.Sleep(1 * time.Second)
	atomic.StoreUint32(&tk.Se.GetSessionVars().Killed, 1)
	wg.Wait()
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
