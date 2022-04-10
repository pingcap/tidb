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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true

	TestingT(t)
}

var _ = Suite(&testSuite{&baseTestSuite{}})
var _ = Suite(&testSuiteP2{&baseTestSuite{}})
var _ = SerialSuites(&testSuiteWithCliBaseCharset{})
var _ = Suite(&testSuite2{&baseTestSuite{}})
var _ = Suite(&testSuite3{&baseTestSuite{}})
var _ = SerialSuites(&testClusterTableSuite{})
var _ = SerialSuites(&testSerialSuite1{&baseTestSuite{}})
var _ = SerialSuites(&testSerialSuite{&baseTestSuite{}})

type testSuite struct{ *baseTestSuite }
type testSuiteP2 struct{ *baseTestSuite }
type testSerialSuite struct{ *baseTestSuite }

type baseTestSuite struct {
	cluster testutils.Cluster
	store   kv.Storage
	domain  *domain.Domain
	*parser.Parser
}

func (s *baseTestSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	s.store = store
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	se.Close()
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *baseTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	c.Assert(s.store.Close(), IsNil)
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
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row := req.GetRow(0)
	c.Assert(row.Len(), Equals, 2)
	c.Assert(row.GetString(0), Equals, "1")
	c.Assert(row.GetString(1), Matches, "*DDL Job:1 not found")

	// show ddl test;
	r, err = tk.Exec("admin show ddl")
	c.Assert(err, IsNil)
	req = r.NewChunk(nil)
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
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsTrue)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	// show DDL jobs test
	r, err = tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 12)
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
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 12)
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
	c.Assert(row[10], Equals, "<nil>")

	// Test the START_TIME and END_TIME field.
	tk.MustExec(`set @@time_zone = 'Asia/Shanghai'`)
	re = tk.MustQuery("admin show ddl jobs where end_time is not NULL")
	row = re.Rows()[0]
	createTime, err := types.ParseDatetime(nil, row[8].(string))
	c.Assert(err, IsNil)
	startTime, err := types.ParseDatetime(nil, row[9].(string))
	c.Assert(err, IsNil)
	endTime, err := types.ParseDatetime(nil, row[10].(string))
	c.Assert(err, IsNil)
	tk.MustExec(`set @@time_zone = 'Europe/Amsterdam'`)
	re = tk.MustQuery("admin show ddl jobs where end_time is not NULL")
	row2 := re.Rows()[0]
	c.Assert(row[8], Not(Equals), row2[8])
	c.Assert(row[9], Not(Equals), row2[9])
	c.Assert(row[10], Not(Equals), row2[10])
	createTime2, err := types.ParseDatetime(nil, row2[8].(string))
	c.Assert(err, IsNil)
	startTime2, err := types.ParseDatetime(nil, row2[9].(string))
	c.Assert(err, IsNil)
	endTime2, err := types.ParseDatetime(nil, row2[10].(string))
	c.Assert(err, IsNil)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	loc2, err := time.LoadLocation("Europe/Amsterdam")
	c.Assert(err, IsNil)
	t, err := createTime.GoTime(loc)
	c.Assert(err, IsNil)
	t2, err := createTime2.GoTime(loc2)
	c.Assert(err, IsNil)
	c.Assert(t.In(time.UTC), Equals, t2.In(time.UTC))
	t, err = startTime.GoTime(loc)
	c.Assert(err, IsNil)
	t2, err = startTime2.GoTime(loc2)
	c.Assert(err, IsNil)
	c.Assert(t.In(time.UTC), Equals, t2.In(time.UTC))
	t, err = endTime.GoTime(loc)
	c.Assert(err, IsNil)
	t2, err = endTime2.GoTime(loc2)
	c.Assert(err, IsNil)
	c.Assert(t.In(time.UTC), Equals, t2.In(time.UTC))
}

func (s *testSuiteP2) TestAdminShowDDLJobsInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_admin_show_ddl_jobs")
	tk.MustExec("drop placement policy if exists x")
	tk.MustExec("drop placement policy if exists y")
	defer func() {
		tk.MustExec("drop database if exists test_admin_show_ddl_jobs")
		tk.MustExec("drop placement policy if exists x")
		tk.MustExec("drop placement policy if exists y")
	}()

	// Test for issue: https://github.com/pingcap/tidb/issues/29915
	tk.MustExec("create placement policy x followers=4;")
	tk.MustExec("create placement policy y " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1, cn-east-2\" " +
		"FOLLOWERS=2")
	tk.MustExec("create database if not exists test_admin_show_ddl_jobs")
	tk.MustExec("use test_admin_show_ddl_jobs")

	tk.MustExec("create table t (a int);")
	tk.MustExec("create table t1 (a int);")

	tk.MustExec("alter table t placement policy x;")
	c.Assert(tk.MustQuery("admin show ddl jobs 1").Rows()[0][3], Equals, "alter table placement")

	tk.MustExec("rename table t to tt, t1 to tt1")
	c.Assert(tk.MustQuery("admin show ddl jobs 1").Rows()[0][3], Equals, "rename tables")

	tk.MustExec("create table tt2 (c int) PARTITION BY RANGE (c) " +
		"(PARTITION p0 VALUES LESS THAN (6)," +
		"PARTITION p1 VALUES LESS THAN (11)," +
		"PARTITION p2 VALUES LESS THAN (16)," +
		"PARTITION p3 VALUES LESS THAN (21));")
	tk.MustExec("alter table tt2 partition p0 placement policy y")
	c.Assert(tk.MustQuery("admin show ddl jobs 1").Rows()[0][3], Equals, "alter table partition placement")

	tk.MustExec("alter table tt1 cache")
	c.Assert(tk.MustQuery("admin show ddl jobs 1").Rows()[0][3], Equals, "alter table cache")
	tk.MustExec("alter table tt1 nocache")
	c.Assert(tk.MustQuery("admin show ddl jobs 1").Rows()[0][3], Equals, "alter table nocache")
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
	rowStr1 := fmt.Sprintf("%s %s %s %s %v %v", "def", "mysql", "utf8mb4", "utf8mb4_bin", nil, nil)
	rowStr2 := fmt.Sprintf("%s %s %s %s %v %v", "def", "mytest", "utf8mb4", "utf8mb4_bin", nil, nil)
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

	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		c.Check(err, IsNil)
		preprocessorReturn := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(ctx, stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		c.Check(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, preprocessorReturn.InfoSchema)
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
	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		c.Check(err, IsNil)
		preprocessorReturn := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(ctx, stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		c.Check(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, preprocessorReturn.InfoSchema)
		c.Check(err, IsNil)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, result)
	}
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

	// Setting snapshot to a time in the future will fail. (One day before the 2038 problem)
	_, err = tk.Exec("set @@tidb_snapshot = '2038-01-18 03:14:07'")
	c.Assert(err, ErrorMatches, "cannot set read timestamp to a future time")
	// SnapshotTS Is not updated if check failed.
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, uint64(0))

	curVer1, _ := s.store.CurrentVersion(kv.GlobalTxnScope)
	time.Sleep(time.Millisecond)
	snapshotTime := time.Now()
	time.Sleep(time.Millisecond)
	curVer2, _ := s.store.CurrentVersion(kv.GlobalTxnScope)
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
	tsoStr := strconv.FormatUint(oracle.GoTimeToTS(snapshotTime), 10)

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

func (s *testSuite2) TestStaleReadFutureTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Setting tx_read_ts to a time in the future will fail. (One day before the 2038 problem)
	_, err := tk.Exec("set @@tx_read_ts = '2038-01-18 03:14:07'")
	c.Assert(err, ErrorMatches, "cannot set read timestamp to a future time")
	// TxnReadTS Is not updated if check failed.
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))
}

const (
	checkDDLAddIndexPriority = 1
)

type checkRequestClient struct {
	tikv.Client
	priority       kvrpcpb.CommandPri
	lowPriorityCnt uint32
	mu             struct {
		sync.RWMutex
		checkFlags uint32
	}
}

func (c *checkRequestClient) getCheckPriority() kvrpcpb.CommandPri {
	return (kvrpcpb.CommandPri)(atomic.LoadInt32((*int32)(&c.priority)))
}

func (c *checkRequestClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	c.mu.RLock()
	checkFlags := c.mu.checkFlags
	c.mu.RUnlock()
	if checkFlags == checkDDLAddIndexPriority {
		if req.Type == tikvrpc.CmdScan {
			if c.getCheckPriority() != req.Priority {
				return nil, errors.New("fail to set priority")
			}
		} else if req.Type == tikvrpc.CmdPrewrite {
			if c.getCheckPriority() == kvrpcpb.CommandPri_Low {
				atomic.AddUint32(&c.lowPriorityCnt, 1)
			}
		}
	}
	return resp, err
}

type testSuiteWithCliBaseCharset struct {
	testSuiteWithCliBase
}

type testSuiteWithCliBase struct {
	store kv.Storage
	dom   *domain.Domain
	cli   *checkRequestClient
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

	err = rs.Next(context.TODO(), rs.NewChunk(nil))
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
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '2001-01-00'"))
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

func (s *testSuiteWithCliBaseCharset) TestCharsetFeature(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("set names gbk;")
	tk.MustQuery("select @@character_set_connection;").Check(testkit.Rows("gbk"))
	tk.MustQuery("select @@collation_connection;").Check(testkit.Rows("gbk_chinese_ci"))
	tk.MustExec("set @@character_set_client=gbk;")
	tk.MustQuery("select @@character_set_client;").Check(testkit.Rows("gbk"))
	tk.MustExec("set names utf8mb4;")
	tk.MustExec("set @@character_set_connection=gbk;")
	tk.MustQuery("select @@character_set_connection;").Check(testkit.Rows("gbk"))
	tk.MustQuery("select @@collation_connection;").Check(testkit.Rows("gbk_chinese_ci"))

	tk.MustGetErrCode("select _gbk 'a';", errno.ErrUnknownCharacterSet)

	tk.MustExec("use test")
	tk.MustExec("create table t1(a char(10) charset gbk);")
	tk.MustExec("create table t2(a char(10) charset gbk collate gbk_bin);")
	tk.MustExec("create table t3(a char(10)) charset gbk;")
	tk.MustExec("alter table t3 add column b char(10) charset gbk;")
	tk.MustQuery("show create table t3").Check(testkit.Rows("t3 CREATE TABLE `t3` (\n" +
		"  `a` char(10) DEFAULT NULL,\n" +
		"  `b` char(10) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci",
	))
	tk.MustExec("create table t4(a char(10));")
	tk.MustExec("alter table t4 add column b char(10) charset gbk;")
	tk.MustQuery("show create table t4").Check(testkit.Rows("t4 CREATE TABLE `t4` (\n" +
		"  `a` char(10) DEFAULT NULL,\n" +
		"  `b` char(10) CHARACTER SET gbk COLLATE gbk_chinese_ci DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("create table t5(a char(20), b char(20) charset utf8, c binary) charset gbk collate gbk_bin;")

	tk.MustExec("create database test_gbk charset gbk;")
	tk.MustExec("use test_gbk")
	tk.MustExec("create table t1(a char(10));")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` char(10) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci",
	))
}

func (s *testSuiteWithCliBaseCharset) TestCharsetFeatureCollation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t" +
		"(ascii_char char(10) character set ascii," +
		"gbk_char char(10) character set gbk collate gbk_bin," +
		"latin_char char(10) character set latin1," +
		"utf8mb4_char char(10) character set utf8mb4)",
	)
	tk.MustExec("insert into t values ('a', 'a', 'a', 'a'), ('a', '啊', '€', 'ㅂ');")
	tk.MustQuery("select collation(concat(ascii_char, gbk_char)) from t;").Check(testkit.Rows("gbk_bin", "gbk_bin"))
	tk.MustQuery("select collation(concat(gbk_char, ascii_char)) from t;").Check(testkit.Rows("gbk_bin", "gbk_bin"))
	tk.MustQuery("select collation(concat(utf8mb4_char, gbk_char)) from t;").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustQuery("select collation(concat(gbk_char, utf8mb4_char)) from t;").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustQuery("select collation(concat('啊', convert('啊' using gbk) collate gbk_bin));").Check(testkit.Rows("gbk_bin"))
	tk.MustQuery("select collation(concat(_latin1 'a', convert('啊' using gbk) collate gbk_bin));").Check(testkit.Rows("gbk_bin"))

	tk.MustGetErrCode("select collation(concat(latin_char, gbk_char)) from t;", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(convert('€' using latin1), convert('啊' using gbk) collate gbk_bin));", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(utf8mb4_char, gbk_char collate gbk_bin)) from t;", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat('ㅂ', convert('啊' using gbk) collate gbk_bin));", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(ascii_char collate ascii_bin, gbk_char)) from t;", mysql.ErrCantAggregate2collations)
}

func (s *testSuiteWithCliBaseCharset) TestCharsetWithPrefixIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20) charset gbk, b char(20) charset gbk, primary key (a(2)));")
	tk.MustExec("insert into t values ('a', '中文'), ('中文', '中文'), ('一二三', '一二三'), ('b', '一二三');")
	tk.MustQuery("select * from t").Check(testkit.Rows("a 中文", "中文 中文", "一二三 一二三", "b 一二三"))
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a char(20) charset gbk, b char(20) charset gbk, unique index idx_a(a(2)));")
	tk.MustExec("insert into t values ('a', '中文'), ('中文', '中文'), ('一二三', '一二三'), ('b', '一二三');")
	tk.MustQuery("select * from t").Check(testkit.Rows("a 中文", "中文 中文", "一二三 一二三", "b 一二三"))
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
		tk.MustQuery(sql).Check(testkit.RowsWithSep("|", cas.result...))
		sql = fmt.Sprintf(cas.sql, "cluster_slow_query")
		tk.MustQuery(sql).Check(testkit.RowsWithSep("|", cas.result...))
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
		tk.MustQuery(cas.sql).Check(testkit.RowsWithSep("|", cas.result...))
	}
}

func (s *testClusterTableSuite) TestSQLDigestTextRetriever(c *C) {
	tkInit := testkit.NewTestKitWithInit(c, s.store)
	tkInit.MustExec("set global tidb_enable_stmt_summary = 1")
	tkInit.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))
	tkInit.MustExec("drop table if exists test_sql_digest_text_retriever")
	tkInit.MustExec("create table test_sql_digest_text_retriever (id int primary key, v int)")

	tk := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("insert into test_sql_digest_text_retriever values (1, 1)")

	insertNormalized, insertDigest := parser.NormalizeDigest("insert into test_sql_digest_text_retriever values (1, 1)")
	_, updateDigest := parser.NormalizeDigest("update test_sql_digest_text_retriever set v = v + 1 where id = 1")
	r := &expression.SQLDigestTextRetriever{
		SQLDigestsMap: map[string]string{
			insertDigest.String(): "",
			updateDigest.String(): "",
		},
	}
	err := r.RetrieveLocal(context.Background(), tk.Se)
	c.Assert(err, IsNil)
	c.Assert(r.SQLDigestsMap[insertDigest.String()], Equals, insertNormalized)
	c.Assert(r.SQLDigestsMap[updateDigest.String()], Equals, "")
}

func (s *testClusterTableSuite) TestFunctionDecodeSQLDigests(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))
	tk.MustExec("drop table if exists test_func_decode_sql_digests")
	tk.MustExec("create table test_func_decode_sql_digests(id int primary key, v int)")

	q1 := "begin"
	norm1, digest1 := parser.NormalizeDigest(q1)
	q2 := "select @@tidb_current_ts"
	norm2, digest2 := parser.NormalizeDigest(q2)
	q3 := "select id, v from test_func_decode_sql_digests where id = 1 for update"
	norm3, digest3 := parser.NormalizeDigest(q3)

	// TIDB_DECODE_SQL_DIGESTS function doesn't actually do "decoding", instead it queries `statements_summary` and it's
	// variations for the corresponding statements.
	// Execute the statements so that the queries will be saved into statements_summary table.
	tk.MustExec(q1)
	// Save the ts to query the transaction from tidb_trx.
	ts, err := strconv.ParseUint(tk.MustQuery(q2).Rows()[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(ts, Greater, uint64(0))
	tk.MustExec(q3)
	tk.MustExec("rollback")

	// Test statements truncating.
	decoded := fmt.Sprintf(`["%s","%s","%s"]`, norm1, norm2, norm3)
	digests := fmt.Sprintf(`["%s","%s","%s"]`, digest1, digest2, digest3)
	tk.MustQuery("select tidb_decode_sql_digests(?, 0)", digests).Check(testkit.Rows(decoded))
	// The three queries are shorter than truncate length, equal to truncate length and longer than truncate length respectively.
	tk.MustQuery("select tidb_decode_sql_digests(?, ?)", digests, len(norm2)).Check(testkit.Rows(
		"[\"begin\",\"select @@tidb_current_ts\",\"select `id` , `v` from `...\"]"))

	// Empty array.
	tk.MustQuery("select tidb_decode_sql_digests('[]')").Check(testkit.Rows("[]"))

	// NULL
	tk.MustQuery("select tidb_decode_sql_digests(null)").Check(testkit.Rows("<nil>"))

	// Array containing wrong types and not-existing digests (maps to null).
	tk.MustQuery("select tidb_decode_sql_digests(?)", fmt.Sprintf(`["%s",1,null,"%s",{"a":1},[2],"%s","","abcde"]`, digest1, digest2, digest3)).
		Check(testkit.Rows(fmt.Sprintf(`["%s",null,null,"%s",null,null,"%s",null,null]`, norm1, norm2, norm3)))

	// Not JSON array (throws warnings)
	tk.MustQuery(`select tidb_decode_sql_digests('{"a":1}')`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1210 The argument can't be unmarshalled as JSON array: '{"a":1}'`))
	tk.MustQuery(`select tidb_decode_sql_digests('aabbccdd')`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1210 The argument can't be unmarshalled as JSON array: 'aabbccdd'`))

	// Invalid argument count.
	tk.MustGetErrCode("select tidb_decode_sql_digests('a', 1, 2)", 1582)
	tk.MustGetErrCode("select tidb_decode_sql_digests()", 1582)
}

func (s *testClusterTableSuite) TestFunctionDecodeSQLDigestsPrivilege(c *C) {
	dropUserTk := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(dropUserTk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)

	tk := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("create user 'testuser'@'localhost'")
	defer dropUserTk.MustExec("drop user 'testuser'@'localhost'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil), IsTrue)
	err := tk.ExecToErr("select tidb_decode_sql_digests('[\"aa\"]')")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[expression:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	tk = testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("create user 'testuser2'@'localhost'")
	defer dropUserTk.MustExec("drop user 'testuser2'@'localhost'")
	tk.MustExec("grant process on *.* to 'testuser2'@'localhost'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil), IsTrue)
	_ = tk.MustQuery("select tidb_decode_sql_digests('[\"aa\"]')")
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

func (s *testSuite) TestZeroDateTimeCompatibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	SQLs := []string{
		`select YEAR(0000-00-00), YEAR("0000-00-00")`,
		`select MONTH(0000-00-00), MONTH("0000-00-00")`,
		`select DAYOFMONTH(0000-00-00), DAYOFMONTH("0000-00-00")`,
		`select QUARTER(0000-00-00), QUARTER("0000-00-00")`,
		`select EXTRACT(DAY FROM 0000-00-00), EXTRACT(DAY FROM "0000-00-00")`,
		`select EXTRACT(MONTH FROM 0000-00-00), EXTRACT(MONTH FROM "0000-00-00")`,
		`select EXTRACT(YEAR FROM 0000-00-00), EXTRACT(YEAR FROM "0000-00-00")`,
		`select EXTRACT(WEEK FROM 0000-00-00), EXTRACT(WEEK FROM "0000-00-00")`,
		`select EXTRACT(QUARTER FROM 0000-00-00), EXTRACT(QUARTER FROM "0000-00-00")`,
	}
	for _, t := range SQLs {
		tk.MustQuery(t).Check(testkit.Rows("0 <nil>"))
		c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	}

	SQLs = []string{
		`select DAYOFWEEK(0000-00-00), DAYOFWEEK("0000-00-00")`,
		`select DAYOFYEAR(0000-00-00), DAYOFYEAR("0000-00-00")`,
	}
	for _, t := range SQLs {
		tk.MustQuery(t).Check(testkit.Rows("<nil> <nil>"))
		c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(2))
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(v1 datetime, v2 datetime(3))")
	tk.MustExec("insert ignore into t values(0,0)")

	SQLs = []string{
		`select YEAR(v1), YEAR(v2) from t`,
		`select MONTH(v1), MONTH(v2) from t`,
		`select DAYOFMONTH(v1), DAYOFMONTH(v2) from t`,
		`select QUARTER(v1), QUARTER(v2) from t`,
		`select EXTRACT(DAY FROM v1), EXTRACT(DAY FROM v2) from t`,
		`select EXTRACT(MONTH FROM v1), EXTRACT(MONTH FROM v2) from t`,
		`select EXTRACT(YEAR FROM v1), EXTRACT(YEAR FROM v2) from t`,
		`select EXTRACT(WEEK FROM v1), EXTRACT(WEEK FROM v2) from t`,
		`select EXTRACT(QUARTER FROM v1), EXTRACT(QUARTER FROM v2) from t`,
	}
	for _, t := range SQLs {
		tk.MustQuery(t).Check(testkit.Rows("0 0"))
		c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))
	}

	SQLs = []string{
		`select DAYOFWEEK(v1), DAYOFWEEK(v2) from t`,
		`select DAYOFYEAR(v1), DAYOFYEAR(v2) from t`,
	}
	for _, t := range SQLs {
		tk.MustQuery(t).Check(testkit.Rows("<nil> <nil>"))
		c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(2))
	}
}

// https://github.com/pingcap/tidb/issues/24165.
func (s *testSuite) TestInvalidDateValueInCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")

	// Test for sql mode 'NO_ZERO_IN_DATE'.
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE';")
	tk.MustGetErrCode("create table t (a datetime default '2999-00-00 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("create table t (a datetime);")
	tk.MustGetErrCode("alter table t modify column a datetime default '2999-00-00 00:00:00';", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")

	// Test for sql mode 'NO_ZERO_DATE'.
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE';")
	tk.MustGetErrCode("create table t (a datetime default '0000-00-00 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("create table t (a datetime);")
	tk.MustGetErrCode("alter table t modify column a datetime default '0000-00-00 00:00:00';", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")

	// Remove NO_ZERO_DATE and NO_ZERO_IN_DATE.
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES';")
	// Test create table with zero datetime as a default value.
	tk.MustExec("create table t (a datetime default '2999-00-00 00:00:00');")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime default '0000-00-00 00:00:00');")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime);")
	tk.MustExec("alter table t modify column a datetime default '2999-00-00 00:00:00';")
	tk.MustExec("alter table t modify column a datetime default '0000-00-00 00:00:00';")
	tk.MustExec("drop table if exists t;")

	// Test create table with invalid datetime(02-30) as a default value.
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES';")
	tk.MustGetErrCode("create table t (a datetime default '2999-02-30 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")
	// NO_ZERO_IN_DATE and NO_ZERO_DATE have nothing to do with invalid datetime(02-30).
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE';")
	tk.MustGetErrCode("create table t (a datetime default '2999-02-30 00:00:00');", errno.ErrInvalidDefault)
	tk.MustExec("drop table if exists t;")
	// ALLOW_INVALID_DATES allows invalid datetime(02-30).
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,ALLOW_INVALID_DATES';")
	tk.MustExec("create table t (a datetime default '2999-02-30 00:00:00');")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime);")
	tk.MustExec("alter table t modify column a datetime default '2999-02-30 00:00:00';")
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

func (s *testSuite) TestIssue24933(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop view if exists v;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1), (2), (3);")

	tk.MustExec("create definer='root'@'localhost' view v as select count(*) as c1 from t;")
	rows := tk.MustQuery("select * from v;")
	rows.Check(testkit.Rows("3"))

	// Test subquery and outer field is wildcard.
	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select count(*) from t) s;")
	rows = tk.MustQuery("select * from v order by 1;")
	rows.Check(testkit.Rows("3"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select avg(a) from t group by a) s;")
	rows = tk.MustQuery("select * from v order by 1;")
	rows.Check(testkit.Rows("1.0000", "2.0000", "3.0000"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select sum(a) from t group by a) s;")
	rows = tk.MustQuery("select * from v order by 1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select group_concat(a) from t group by a) s;")
	rows = tk.MustQuery("select * from v order by 1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	// Test alias names.
	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select count(0) as c1 from t) s;")
	rows = tk.MustQuery("select * from v order by 1;")
	rows.Check(testkit.Rows("3"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select count(*) as c1 from t) s;")
	rows = tk.MustQuery("select * from v order by 1;")
	rows.Check(testkit.Rows("3"))

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select group_concat(a) as `concat(a)` from t group by a) s;")
	rows = tk.MustQuery("select * from v order by 1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	// Test firstrow.
	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select a from t group by a) s;")
	rows = tk.MustQuery("select * from v order by 1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	// Test direct select.
	err := tk.ExecToErr("SELECT `s`.`count(a)` FROM (SELECT COUNT(`a`) FROM `test`.`t`) AS `s`")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 's.count(a)' in 'field list'")

	tk.MustExec("drop view v;")
	tk.MustExec("create definer='root'@'localhost' view v as select * from (select count(a) from t) s;")
	rows = tk.MustQuery("select * from v")
	rows.Check(testkit.Rows("3"))

	// Test window function.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(c1 int);")
	tk.MustExec("insert into t values(111), (222), (333);")
	tk.MustExec("drop view if exists v;")
	tk.MustExec("create definer='root'@'localhost' view v as (select * from (select row_number() over (order by c1) from t) s);")
	rows = tk.MustQuery("select * from v;")
	rows.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop view if exists v;")
	tk.MustExec("create definer='root'@'localhost' view v as (select * from (select c1, row_number() over (order by c1) from t) s);")
	rows = tk.MustQuery("select * from v;")
	rows.Check(testkit.Rows("111 1", "222 2", "333 3"))

	// Test simple expr.
	tk.MustExec("drop view if exists v;")
	tk.MustExec("create definer='root'@'localhost' view v as (select * from (select c1 or 0 from t) s)")
	rows = tk.MustQuery("select * from v;")
	rows.Check(testkit.Rows("1", "1", "1"))
	rows = tk.MustQuery("select `c1 or 0` from v;")
	rows.Check(testkit.Rows("1", "1", "1"))

	tk.MustExec("drop view v;")
}

func (s *testSuite) TestTableSampleTemporaryTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp1")
	tk.MustExec("create global temporary table tmp1 " +
		"(id int not null primary key, code int not null, value int default null, unique key code(code))" +
		"on commit delete rows")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp2")
	tk.MustExec("create temporary table tmp2 (id int not null primary key, code int not null, value int default null, unique key code(code));")

	// sleep 1us to make test stale
	time.Sleep(time.Microsecond)

	// test tablesample return empty for global temporary table
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustExec("insert into tmp1 values (1, 1, 1)")
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())
	tk.MustExec("commit")

	// tablesample for global temporary table should not return error for compatibility of tools like dumpling
	tk.MustExec("set @@tidb_snapshot=NOW(6)")
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustQuery("select * from tmp1 tablesample regions()").Check(testkit.Rows())
	tk.MustExec("commit")
	tk.MustExec("set @@tidb_snapshot=''")

	// test tablesample returns error for local temporary table
	tk.MustGetErrMsg("select * from tmp2 tablesample regions()", "TABLESAMPLE clause can not be applied to local temporary tables")

	tk.MustExec("begin")
	tk.MustExec("insert into tmp2 values (1, 1, 1)")
	tk.MustGetErrMsg("select * from tmp2 tablesample regions()", "TABLESAMPLE clause can not be applied to local temporary tables")
	tk.MustExec("commit")
	tk.MustGetErrMsg("select * from tmp2 tablesample regions()", "TABLESAMPLE clause can not be applied to local temporary tables")
}

func (s *testSuite) TestCTEWithIndexLookupJoinDeadLock(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int(11) default null,b int(11) default null,key b (b),key ba (b))")
	tk.MustExec("create table t1 (a int(11) default null,b int(11) default null,key idx_ab (a,b),key idx_a (a),key idx_b (b))")
	tk.MustExec("create table t2 (a int(11) default null,b int(11) default null,key idx_ab (a,b),key idx_a (a),key idx_b (b))")
	// It's easy to reproduce this problem in 30 times execution of IndexLookUpJoin.
	for i := 0; i < 30; i++ {
		tk.MustExec("with cte as (with cte1 as (select * from t2 use index(idx_ab) where a > 1 and b > 1) select * from cte1) select /*+use_index(t1 idx_ab)*/ * from cte join t1 on t1.a=cte.a;")
	}
}

func (s *testSuite) TestGetResultRowsCount(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	for i := 1; i <= 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v)", i))
	}
	cases := []struct {
		sql string
		row int64
	}{
		{"select * from t", 10},
		{"select * from t where a < 0", 0},
		{"select * from t where a <= 3", 3},
		{"insert into t values (11)", 0},
		{"replace into t values (12)", 0},
		{"update t set a=13 where a=12", 0},
	}

	for _, ca := range cases {
		if strings.HasPrefix(ca.sql, "select") {
			tk.MustQuery(ca.sql)
		} else {
			tk.MustExec(ca.sql)
		}
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		p, ok := info.Plan.(plannercore.Plan)
		c.Assert(ok, IsTrue)
		cnt := executor.GetResultRowsCount(tk.Se, p)
		c.Assert(ca.row, Equals, cnt, Commentf("sql: %v", ca.sql))
	}
}

func checkFileName(s string) bool {
	files := []string{
		"config.toml",
		"meta.txt",
		"stats/test.t_dump_single.json",
		"schema/test.t_dump_single.schema.txt",
		"variables.toml",
		"sqls.sql",
		"session_bindings.sql",
		"global_bindings.sql",
		"explain.txt",
	}
	for _, f := range files {
		if strings.Compare(f, s) == 0 {
			return true
		}
	}
	return false
}

// Test invoke Close without invoking Open before for each operators.
func (s *testSerialSuite) TestUnreasonablyClose(c *C) {
	defer testleak.AfterTest(c)()

	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable(), plannercore.MockUnsignedTable()})
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	// To enable the shuffleExec operator.
	_, err = se.Execute(context.Background(), "set @@tidb_merge_join_concurrency=4")
	c.Assert(err, IsNil)

	var opsNeedsCovered = []plannercore.PhysicalPlan{
		&plannercore.PhysicalHashJoin{},
		&plannercore.PhysicalMergeJoin{},
		&plannercore.PhysicalIndexJoin{},
		&plannercore.PhysicalIndexHashJoin{},
		&plannercore.PhysicalTableReader{},
		&plannercore.PhysicalIndexReader{},
		&plannercore.PhysicalIndexLookUpReader{},
		&plannercore.PhysicalIndexMergeReader{},
		&plannercore.PhysicalApply{},
		&plannercore.PhysicalHashAgg{},
		&plannercore.PhysicalStreamAgg{},
		&plannercore.PhysicalLimit{},
		&plannercore.PhysicalSort{},
		&plannercore.PhysicalTopN{},
		&plannercore.PhysicalCTE{},
		&plannercore.PhysicalCTETable{},
		&plannercore.PhysicalMaxOneRow{},
		&plannercore.PhysicalProjection{},
		&plannercore.PhysicalSelection{},
		&plannercore.PhysicalTableDual{},
		&plannercore.PhysicalWindow{},
		&plannercore.PhysicalShuffle{},
		&plannercore.PhysicalUnionAll{},
	}
	executorBuilder := executor.NewMockExecutorBuilderForTest(se, is, nil, math.MaxUint64, false, "global")

	var opsNeedsCoveredMask uint64 = 1<<len(opsNeedsCovered) - 1
	opsAlreadyCoveredMask := uint64(0)
	for i, tc := range []string{
		"select /*+ hash_join(t1)*/ * from t t1 join t t2 on t1.a = t2.a",
		"select /*+ merge_join(t1)*/ * from t t1 join t t2 on t1.f = t2.f",
		"select t.f from t use index(f)",
		"select /*+ inl_join(t1) */ * from t t1 join t t2 on t1.f=t2.f",
		"select /*+ inl_hash_join(t1) */ * from t t1 join t t2 on t1.f=t2.f",
		"SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t",
		"select /*+ hash_agg() */ count(f) from t group by a",
		"select /*+ stream_agg() */ count(f) from t group by a",
		"select * from t order by a, f",
		"select * from t order by a, f limit 1",
		"select * from t limit 1",
		"select (select t1.a from t t1 where t1.a > t2.a) as a from t t2;",
		"select a + 1 from t",
		"select count(*) a from t having a > 1",
		"select * from t where a = 1.1",
		"with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 0) select * from cte1",
		"select /*+use_index_merge(t, c_d_e, f)*/ * from t where c < 1 or f > 2",
		"select sum(f) over (partition by f) from t",
		"select /*+ merge_join(t1)*/ * from t t1 join t t2 on t1.d = t2.d",
		"select a from t union all select a from t",
	} {
		comment := Commentf("case:%v sql:%s", i, tc)
		c.Assert(err, IsNil, comment)
		stmt, err := s.ParseOneStmt(tc, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil, comment)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, is)
		c.Assert(err, IsNil, comment)
		// This for loop level traverses the plan tree to get which operators are covered.
		for child := []plannercore.PhysicalPlan{p.(plannercore.PhysicalPlan)}; len(child) != 0; {
			newChild := make([]plannercore.PhysicalPlan, 0, len(child))
			for _, ch := range child {
				found := false
				for k, t := range opsNeedsCovered {
					if reflect.TypeOf(t) == reflect.TypeOf(ch) {
						opsAlreadyCoveredMask |= 1 << k
						found = true
						break
					}
				}
				c.Assert(found, IsTrue, Commentf("case: %v sql: %s operator %v is not registered in opsNeedsCoveredMask", i, tc, reflect.TypeOf(ch)))
				switch x := ch.(type) {
				case *plannercore.PhysicalCTE:
					newChild = append(newChild, x.RecurPlan)
					newChild = append(newChild, x.SeedPlan)
					continue
				case *plannercore.PhysicalShuffle:
					newChild = append(newChild, x.DataSources...)
					newChild = append(newChild, x.Tails...)
					continue
				}
				newChild = append(newChild, ch.Children()...)
			}
			child = newChild
		}

		e := executorBuilder.Build(p)

		func() {
			defer func() {
				r := recover()
				buf := make([]byte, 4096)
				stackSize := runtime.Stack(buf, false)
				buf = buf[:stackSize]
				c.Assert(r, IsNil, Commentf("case: %v\n sql: %s\n error stack: %v", i, tc, string(buf)))
			}()
			c.Assert(e.Close(), IsNil, comment)
		}()
	}
	// The following code is used to make sure all the operators registered
	// in opsNeedsCoveredMask are covered.
	commentBuf := strings.Builder{}
	if opsAlreadyCoveredMask != opsNeedsCoveredMask {
		for i := range opsNeedsCovered {
			if opsAlreadyCoveredMask&(1<<i) != 1<<i {
				commentBuf.WriteString(fmt.Sprintf(" %v", reflect.TypeOf(opsNeedsCovered[i])))
			}
		}
	}
	c.Assert(opsAlreadyCoveredMask, Equals, opsNeedsCoveredMask, Commentf("these operators are not covered %s", commentBuf.String()))
}

func (s *testSerialSuite) TestEncodingSet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `enum-set` (`set` SET(" +
		"'x00','x01','x02','x03','x04','x05','x06','x07','x08','x09','x10','x11','x12','x13','x14','x15'," +
		"'x16','x17','x18','x19','x20','x21','x22','x23','x24','x25','x26','x27','x28','x29','x30','x31'," +
		"'x32','x33','x34','x35','x36','x37','x38','x39','x40','x41','x42','x43','x44','x45','x46','x47'," +
		"'x48','x49','x50','x51','x52','x53','x54','x55','x56','x57','x58','x59','x60','x61','x62','x63'" +
		")NOT NULL PRIMARY KEY)")
	tk.MustExec("INSERT INTO `enum-set` VALUES\n(\"x00,x59\");")
	tk.MustQuery("select `set` from `enum-set` use index(PRIMARY)").Check(testkit.Rows("x00,x59"))
	tk.MustExec("admin check table `enum-set`")
}

func (s *testSuite) TestDeleteWithMulTbl(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// Delete multiple tables from left joined table.
	// The result of left join is (3, null, null).
	// Because rows in t2 are not matched, so no row will be deleted in t2.
	// But row in t1 is matched, so it should be deleted.
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (c1 int);")
	tk.MustExec("create table t2 (c1 int primary key, c2 int);")
	tk.MustExec("insert into t1 values(3);")
	tk.MustExec("insert into t2 values(2, 2);")
	tk.MustExec("insert into t2 values(0, 0);")
	tk.MustExec("delete from t1, t2 using t1 left join t2 on t1.c1 = t2.c2;")
	tk.MustQuery("select * from t1 order by c1;").Check(testkit.Rows())
	tk.MustQuery("select * from t2 order by c1;").Check(testkit.Rows("0 0", "2 2"))

	// Rows in both t1 and t2 are matched, so will be deleted even if it's null.
	// NOTE: The null values are not generated by join.
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (c1 int);")
	tk.MustExec("create table t2 (c2 int);")
	tk.MustExec("insert into t1 values(null);")
	tk.MustExec("insert into t2 values(null);")
	tk.MustExec("delete from t1, t2 using t1 join t2 where t1.c1 is null;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows())
	tk.MustQuery("select * from t2;").Check(testkit.Rows())
}
