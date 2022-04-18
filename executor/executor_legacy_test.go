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
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true

	TestingT(t)
}

var _ = Suite(&testSuite{&baseTestSuite{}})
var _ = Suite(&testSuiteP2{&baseTestSuite{}})
var _ = Suite(&testSuite3{&baseTestSuite{}})

type testSuite struct{ *baseTestSuite }
type testSuiteP2 struct{ *baseTestSuite }

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
