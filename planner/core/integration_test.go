// Copyright 2019 PingCAP, Inc.
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

package core_test

import (
	"bytes"
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testIntegrationSuite{})
var _ = SerialSuites(&testIntegrationSerialSuite{})

type testIntegrationSuite struct {
	testData testutil.TestData
	store    kv.Storage
	dom      *domain.Domain
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "integration_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testIntegrationSuite) SetUpTest(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownTest(c *C) {
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

type testIntegrationSerialSuite struct {
	testData testutil.TestData
	store    kv.Storage
	dom      *domain.Domain
}

func (s *testIntegrationSerialSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "integration_serial_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSerialSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testIntegrationSerialSuite) SetUpTest(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testIntegrationSerialSuite) TearDownTest(c *C) {
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TestShowSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10), b int, c int)")
	tk.MustQuery("show columns from t where true").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
		"b int(11) YES  <nil> ",
		"c int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field = 'b'").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b')").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b') and true").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b') and false").Check(testkit.Rows())
	tk.MustExec("insert into t values('c', 0, 0)")
	tk.MustQuery("show columns from t where field < all (select a from t)").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
		"b int(11) YES  <nil> ",
	))
	tk.MustExec("insert into t values('b', 0, 0)")
	tk.MustQuery("show columns from t where field < all (select a from t)").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
	))
}

func (s *testIntegrationSuite) TestPpdWithSetVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 varchar(255))")
	tk.MustExec("insert into t values(1,'a'),(2,'d'),(3,'c')")

	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=1 and t01.c2='d'").Check(testkit.Rows())
	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=2 and t01.c2='d'").Check(testkit.Rows("2 d 2"))
}

func (s *testIntegrationSuite) TestBitColErrorMessage(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists bit_col_t")
	tk.MustExec("create table bit_col_t (a bit(64))")
	tk.MustExec("drop table bit_col_t")
	tk.MustExec("create table bit_col_t (a bit(1))")
	tk.MustExec("drop table bit_col_t")
	tk.MustGetErrCode("create table bit_col_t (a bit(0))", mysql.ErrInvalidFieldSize)
	tk.MustGetErrCode("create table bit_col_t (a bit(65))", mysql.ErrTooBigDisplaywidth)
}

func (s *testIntegrationSuite) TestPushLimitDownIndexLookUpReader(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
	tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl")
	tk.MustExec("create table tbl(a int, b int, c int, key idx_b_c(b,c))")
	tk.MustExec("insert into tbl values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
	tk.MustExec("analyze table tbl")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestIsFromUnixtimeNullRejective(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint, b bigint);`)
	s.runTestsWithTestData("TestIsFromUnixtimeNullRejective", tk, c)
}

func (s *testIntegrationSuite) runTestsWithTestData(caseName string, tk *testkit.TestKit, c *C) {
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCasesByName(caseName, c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestJoinNotNullFlag(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(x int not null)")
	tk.MustExec("create table t2(x int)")
	tk.MustExec("insert into t2 values (1)")

	tk.MustQuery("select IFNULL((select t1.x from t1 where t1.x = t2.x), 'xxx') as col1 from t2").Check(testkit.Rows("xxx"))
	tk.MustQuery("select ifnull(t1.x, 'xxx') from t2 left join t1 using(x)").Check(testkit.Rows("xxx"))
	tk.MustQuery("select ifnull(t1.x, 'xxx') from t2 natural left join t1").Check(testkit.Rows("xxx"))
}

func (s *testIntegrationSuite) TestAntiJoinConstProp(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int not null)")
	tk.MustExec("insert into t1 values (1,1)")
	tk.MustExec("create table t2(a int not null, b int not null)")
	tk.MustExec("insert into t2 values (2,2)")

	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.a = t1.a and t2.a > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b = t1.b and t2.a > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b = t1.b and t2.b > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select q.a in (select count(*) from t1 s where not exists (select 1 from t1 p where q.a > 1 and p.a = s.a)) from t1 q").Check(testkit.Rows(
		"1",
	))
	tk.MustQuery("select q.a in (select not exists (select 1 from t1 p where q.a > 1 and p.a = s.a) from t1 s) from t1 q").Check(testkit.Rows(
		"1",
	))

	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a int not null, b int)")
	tk.MustExec("insert into t1 values (1,null)")
	tk.MustExec("create table t2(a int not null, b int)")
	tk.MustExec("insert into t2 values (2,2)")

	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b > t1.b)").Check(testkit.Rows(
		"1 <nil>",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t1.a = 2)").Check(testkit.Rows(
		"1 <nil>",
	))
}

func (s *testIntegrationSuite) TestSimplifyOuterJoinWithCast(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b datetime default null)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSerialSuite) TestNoneAccessPathsFoundByIsolationRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")

	_, err := tk.Exec("select * from t")
	c.Assert(err, IsNil)

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")

	// Don't filter mysql.SystemDB by isolation read.
	tk.MustQuery("explain select * from mysql.stats_meta").Check(testkit.Rows(
		"TableReader_5 10000.00 root  data:TableFullScan_4",
		"└─TableFullScan_4 10000.00 cop[tikv] table:stats_meta keep order:false, stats:pseudo"))

	_, err = tk.Exec("select * from t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can not find access path matching 'tidb_isolation_read_engines'(value: 'tiflash'). Available values are 'tikv'.")

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash, tikv'")
	tk.MustExec("select * from t")
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tiflash"}
	})
	// Change instance config doesn't affect isolation read.
	tk.MustExec("select * from t")
}

func (s *testIntegrationSerialSuite) TestSelPushDownTiFlash(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(20))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSerialSuite) TestBroadcastJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists d1_t")
	tk.MustExec("create table d1_t(d1_k int, value int)")
	tk.MustExec("insert into d1_t values(1,2),(2,3)")
	tk.MustExec("analyze table d1_t")
	tk.MustExec("drop table if exists d2_t")
	tk.MustExec("create table d2_t(d2_k decimal(10,2), value int)")
	tk.MustExec("insert into d2_t values(10.11,2),(10.12,3)")
	tk.MustExec("analyze table d2_t")
	tk.MustExec("drop table if exists d3_t")
	tk.MustExec("create table d3_t(d3_k date, value int)")
	tk.MustExec("insert into d3_t values(date'2010-01-01',2),(date'2010-01-02',3)")
	tk.MustExec("analyze table d3_t")
	tk.MustExec("drop table if exists fact_t")
	tk.MustExec("create table fact_t(d1_k int, d2_k decimal(10,2), d3_k date, col1 int, col2 int, col3 int)")
	tk.MustExec("insert into fact_t values(1,10.11,date'2010-01-01',1,2,3),(1,10.11,date'2010-01-02',1,2,3),(1,10.12,date'2010-01-01',1,2,3),(1,10.12,date'2010-01-02',1,2,3)")
	tk.MustExec("insert into fact_t values(2,10.11,date'2010-01-01',1,2,3),(2,10.11,date'2010-01-02',1,2,3),(2,10.12,date'2010-01-01',1,2,3),(2,10.12,date'2010-01-02',1,2,3)")
	tk.MustExec("analyze table fact_t")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "fact_t" || tblInfo.Name.L == "d1_t" || tblInfo.Name.L == "d2_t" || tblInfo.Name.L == "d3_t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_batch_cop = 1")
	tk.MustExec("set @@session.tidb_opt_broadcast_join = 1")
	// make cbo force choose broadcast join since sql hint does not work for semi/anti-semi join
	tk.MustExec("set @@session.tidb_opt_cpu_factor=10000000;")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}

	// out table of out join should not be global
	_, err := tk.Exec("explain select /*+ broadcast_join(fact_t, d1_t), broadcast_join_local(d1_t) */ count(*) from fact_t left join d1_t on fact_t.d1_k = d1_t.d1_k")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can't find a proper physical plan for this query")
	// nullEQ not supported
	_, err = tk.Exec("explain select /*+ broadcast_join(fact_t, d1_t) */ count(*) from fact_t join d1_t on fact_t.d1_k <=> d1_t.d1_k")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can't find a proper physical plan for this query")
	// not supported if join condition has unsupported expr
	_, err = tk.Exec("explain select /*+ broadcast_join(fact_t, d1_t) */ count(*) from fact_t left join d1_t on fact_t.d1_k = d1_t.d1_k and sqrt(fact_t.col1) > 2")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can't find a proper physical plan for this query")
	// cartsian join not supported
	_, err = tk.Exec("explain select /*+ broadcast_join(fact_t, d1_t) */ count(*) from fact_t join d1_t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can't find a proper physical plan for this query")
}

func (s *testIntegrationSerialSuite) TestAggPushDownEngine(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(20))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")

	tk.MustQuery("desc select approx_count_distinct(a) from t").Check(testkit.Rows(
		"StreamAgg_16 1.00 root  funcs:approx_count_distinct(Column#5)->Column#3",
		"└─TableReader_17 1.00 root  data:StreamAgg_8",
		"  └─StreamAgg_8 1.00 cop[tiflash]  funcs:approx_count_distinct(test.t.a)->Column#5",
		"    └─TableFullScan_15 10000.00 cop[tiflash] table:t keep order:false, stats:pseudo"))

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv'")

	tk.MustQuery("desc select approx_count_distinct(a) from t").Check(testkit.Rows(
		"HashAgg_5 1.00 root  funcs:approx_count_distinct(test.t.a)->Column#3",
		"└─TableReader_11 10000.00 root  data:TableFullScan_10",
		"  └─TableFullScan_10 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
}

func (s *testIntegrationSerialSuite) TestIssue15110(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists crm_rd_150m")
	tk.MustExec(`CREATE TABLE crm_rd_150m (
	product varchar(256) DEFAULT NULL,
		uks varchar(16) DEFAULT NULL,
		brand varchar(256) DEFAULT NULL,
		cin varchar(16) DEFAULT NULL,
		created_date timestamp NULL DEFAULT NULL,
		quantity int(11) DEFAULT NULL,
		amount decimal(11,0) DEFAULT NULL,
		pl_date timestamp NULL DEFAULT NULL,
		customer_first_date timestamp NULL DEFAULT NULL,
		recent_date timestamp NULL DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "crm_rd_150m" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("explain SELECT count(*) FROM crm_rd_150m dataset_48 WHERE (CASE WHEN (month(dataset_48.customer_first_date)) <= 30 THEN '新客' ELSE NULL END) IS NOT NULL;")
}

func (s *testIntegrationSerialSuite) TestReadFromStorageHint(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, tt, ttt")
	tk.MustExec("create table t(a int, b int, index ia(a))")
	tk.MustExec("create table tt(a int, b int, primary key(a))")
	tk.MustExec("create table ttt(a int, primary key (a desc))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}

func (s *testIntegrationSerialSuite) TestReadFromStorageHintAndIsolationRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, tt, ttt")
	tk.MustExec("create table t(a int, b int, index ia(a))")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tikv\"")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		tk.Se.GetSessionVars().StmtCtx.SetWarnings(nil)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}

func (s *testIntegrationSerialSuite) TestIsolationReadTiFlashNotChoosePointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key (a))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSerialSuite) TestIsolationReadTiFlashUseIndexHint(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx(a));")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}

func (s *testIntegrationSerialSuite) TestIsolationReadDoNotFilterSystemDB(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_isolation_read_engines = \"tiflash\"")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestPartitionTableStats(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	{
		tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int)partition by range columns(a)(partition p0 values less than (10), partition p1 values less than(20), partition p2 values less than(30));")
		tk.MustExec("insert into t values(21, 1), (22, 2), (23, 3), (24, 4), (15, 5)")
		tk.MustExec("analyze table t")

		var input []string
		var output []struct {
			SQL    string
			Result []string
		}
		s.testData.GetTestCases(c, &input, &output)
		for i, tt := range input {
			s.testData.OnRecord(func() {
				output[i].SQL = tt
				output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
		}
	}
}

func (s *testIntegrationSuite) TestPartitionPruningForInExpr(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(11), b int) partition by range (a) (partition p0 values less than (4), partition p1 values less than(10), partition p2 values less than maxvalue);")
	tk.MustExec("insert into t values (1, 1),(10, 10),(11, 11)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSerialSuite) TestPartitionPruningWithDateType(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime) partition by range columns (a) (partition p1 values less than ('20000101'), partition p2 values less than ('2000-10-01'));")
	tk.MustExec("insert into t values ('20000201'), ('19000101');")

	// cannot get the statistical information immediately
	// tk.MustQuery(`SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't';`).Check(testkit.Rows("p1 1", "p2 1"))
	str := tk.MustQuery(`desc select * from t where a < '2000-01-01';`).Rows()[0][3].(string)
	c.Assert(strings.Contains(str, "partition:p1"), IsTrue)
}

func (s *testIntegrationSuite) TestPartitionPruningForEQ(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b int) partition by range(weekday(a)) (partition p0 values less than(10), partition p1 values less than (100))")

	is := infoschema.GetInfoSchema(tk.Se)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pt := tbl.(table.PartitionedTable)
	query, err := expression.ParseSimpleExprWithTableInfo(tk.Se, "a = '2020-01-01 00:00:00'", tbl.Meta())
	c.Assert(err, IsNil)
	dbName := model.NewCIStr(tk.Se.GetSessionVars().CurrentDB)
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(tk.Se, dbName, tbl.Meta().Name, tbl.Meta().Cols(), tbl.Meta())
	c.Assert(err, IsNil)
	// Even the partition is not monotonous, EQ condition should be prune!
	// select * from t where a = '2020-01-01 00:00:00'
	res, err := core.PartitionPruning(tk.Se, pt, []expression.Expression{query}, nil, columns, names)
	c.Assert(err, IsNil)
	c.Assert(res, HasLen, 1)
	c.Assert(res[0], Equals, 0)
}

func (s *testIntegrationSuite) TestErrNoDB(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create user test")
	_, err := tk.Exec("grant select on test1111 to test@'%'")
	c.Assert(errors.Cause(err), Equals, core.ErrNoDB)
	tk.MustExec("use test")
	tk.MustExec("create table test1111 (id int)")
	tk.MustExec("grant select on test1111 to test@'%'")
}

func (s *testIntegrationSuite) TestMaxMinEliminate(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustExec("set @@tidb_enable_clustered_index=1;")
	tk.MustExec("create table cluster_index_t(a int, b int, c int, primary key (a, b));")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestINLJHintSmallTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int, key(a))")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("create table t2(a int not null, b int, key(a))")
	tk.MustExec("insert into t2 values(1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("analyze table t1, t2")
	tk.MustExec("explain select /*+ TIDB_INLJ(t1) */ * from t1 join t2 on t1.a = t2.a")
}

func (s *testIntegrationSuite) TestIndexJoinUniqueCompositeIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("set @@tidb_enable_clustered_index=0")
	tk.MustExec("create table t1(a int not null, c int not null)")
	tk.MustExec("create table t2(a int not null, b int not null, c int not null, primary key(a,b))")
	tk.MustExec("insert into t1 values(1,1)")
	tk.MustExec("insert into t2 values(1,1,1),(1,2,1)")
	tk.MustExec("analyze table t1,t2")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestIndexMerge(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, unique index(a), unique index(b), primary key(c))")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestIndexMergeHint4CNF(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, a int, b int, c int, key(a), key(b), key(c))")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestInvisibleIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	// Optimizer cannot see invisible indexes.
	tk.MustExec("create table t(a int, b int, unique index i_a (a) invisible, unique index i_b(b))")
	tk.MustExec("insert into t values (1,2)")

	// Optimizer cannot use invisible indexes.
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1"))
	c.Check(tk.MustUseIndex("select a from t order by a", "i_a"), IsFalse)
	tk.MustQuery("select a from t where a > 0").Check(testkit.Rows("1"))
	c.Check(tk.MustUseIndex("select a from t where a > 1", "i_a"), IsFalse)

	// If use invisible indexes in index hint and sql hint, throw an error.
	errStr := "[planner:1176]Key 'i_a' doesn't exist in table 't'"
	tk.MustGetErrMsg("select * from t use index(i_a)", errStr)
	tk.MustGetErrMsg("select * from t force index(i_a)", errStr)
	tk.MustGetErrMsg("select * from t ignore index(i_a)", errStr)
	tk.MustQuery("select /*+ USE_INDEX(t, i_a) */ * from t")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, errStr)
	tk.MustQuery("select /*+ IGNORE_INDEX(t, i_a), USE_INDEX(t, i_b) */ a from t order by a")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, errStr)

	tk.MustExec("admin check table t")
	tk.MustExec("admin check index t i_a")
}

// for issue #14822
func (s *testIntegrationSuite) TestIndexJoinTableRange(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, primary key (a), key idx_t1_b (b))")
	tk.MustExec("create table t2(a int, b int, primary key (a), key idx_t1_b (b))")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestTopNByConstFunc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustQuery("select max(t.col) from (select 'a' as col union all select '' as col) as t").Check(testkit.Rows(
		"a",
	))
}

func (s *testIntegrationSuite) TestSubqueryWithTopN(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestIndexHintWarning(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, key a(a), key b(b))")
	tk.MustExec("create table t2(a int, b int, c int, key a(a), key b(b))")
	var input []string
	var output []struct {
		SQL      string
		Warnings []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			tk.MustQuery(tt)
			warns := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
			output[i].Warnings = make([]string, len(warns))
			for j := range warns {
				output[i].Warnings[j] = warns[j].Err.Error()
			}
		})
		tk.MustQuery(tt)
		warns := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warns), Equals, len(output[i].Warnings))
		for j := range warns {
			c.Assert(warns[j].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warns[j].Err.Error(), Equals, output[i].Warnings[j])
		}
	}
}

func (s *testIntegrationSuite) TestIssue15546(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, pt, vt")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("create table pt(a int primary key, b int) partition by range(a) (" +
		"PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20), PARTITION `p2` VALUES LESS THAN (30))")
	tk.MustExec("insert into pt values(1, 1), (11, 11), (21, 21)")
	tk.MustExec("create definer='root'@'localhost' view vt(a, b) as select a, b from t")
	tk.MustQuery("select * from pt, vt where pt.a = vt.a").Check(testkit.Rows("1 1 1 1"))
}

func (s *testIntegrationSuite) TestApproxCountDistinctInPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(11), b int) partition by range (a) (partition p0 values less than (3), partition p1 values less than maxvalue);")
	tk.MustExec("insert into t values(1, 1), (2, 1), (3, 1), (4, 2), (4, 2)")
	tk.MustExec("set session tidb_opt_agg_push_down=1")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
	tk.MustQuery("explain select approx_count_distinct(a), b from t group by b order by b desc").Check(testkit.Rows("Sort_12 16000.00 root  test.t.b:desc",
		"└─HashAgg_15 16000.00 root  group by:test.t.b, funcs:approx_count_distinct(Column#5)->Column#4, funcs:firstrow(Column#6)->test.t.b",
		"  └─PartitionUnion_16 16000.00 root  ",
		"    ├─HashAgg_17 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->Column#5, funcs:firstrow(test.t.b)->Column#6, funcs:firstrow(test.t.b)->test.t.b",
		"    │ └─TableReader_21 10000.00 root  data:TableFullScan_20",
		"    │   └─TableFullScan_20 10000.00 cop[tikv] table:t, partition:p0 keep order:false, stats:pseudo",
		"    └─HashAgg_24 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->Column#5, funcs:firstrow(test.t.b)->Column#6, funcs:firstrow(test.t.b)->test.t.b",
		"      └─TableReader_28 10000.00 root  data:TableFullScan_27",
		"        └─TableFullScan_27 10000.00 cop[tikv] table:t, partition:p1 keep order:false, stats:pseudo"))
	tk.MustQuery("select approx_count_distinct(a), b from t group by b order by b desc").Check(testkit.Rows("1 2", "3 1"))
}

func (s *testIntegrationSuite) TestApproxPercentile(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 1), (2, 1), (3, 2), (4, 2), (5, 2)")

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
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testIntegrationSuite) TestIssue17813(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists hash_partition_overflow")
	tk.MustExec("create table hash_partition_overflow (c0 bigint unsigned) partition by hash(c0) partitions 3")
	tk.MustExec("insert into hash_partition_overflow values (9223372036854775808)")
	tk.MustQuery("select * from hash_partition_overflow where c0 = 9223372036854775808").Check(testkit.Rows("9223372036854775808"))
	tk.MustQuery("select * from hash_partition_overflow where c0 in (1, 9223372036854775808)").Check(testkit.Rows("9223372036854775808"))
}

func (s *testIntegrationSuite) TestHintWithRequiredProperty(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
	tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, key b(b))")
	var input []string
	var output []struct {
		SQL      string
		Plan     []string
		Warnings []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
			output[i].Warnings = make([]string, len(warnings))
			for j, warning := range warnings {
				output[i].Warnings[j] = warning.Err.Error()
			}
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, len(output[i].Warnings))
		for j, warning := range warnings {
			c.Assert(output[i].Warnings[j], Equals, warning.Err.Error())
		}
	}
}

func (s *testIntegrationSuite) TestIssue15813(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("create table t0(c0 int primary key)")
	tk.MustExec("create table t1(c0 int primary key)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustExec("CREATE INDEX i0 ON t1(c0)")
	tk.MustQuery("select /*+ MERGE_JOIN(t0, t1) */ * from t0, t1 where t0.c0 = t1.c0").Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestFullGroupByOrderBy(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustQuery("select count(a) as b from t group by a order by b").Check(testkit.Rows())
	err := tk.ExecToErr("select count(a) as cnt from t group by a order by b")
	c.Assert(terror.ErrorEqual(err, core.ErrFieldNotInGroupBy), IsTrue)
}

func (s *testIntegrationSuite) TestHintWithoutTableWarning(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, key a(a))")
	tk.MustExec("create table t2(a int, b int, c int, key a(a))")
	var input []string
	var output []struct {
		SQL      string
		Warnings []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			tk.MustQuery(tt)
			warns := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
			output[i].Warnings = make([]string, len(warns))
			for j := range warns {
				output[i].Warnings[j] = warns[j].Err.Error()
			}
		})
		tk.MustQuery(tt)
		warns := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warns), Equals, len(output[i].Warnings))
		for j := range warns {
			c.Assert(warns[j].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warns[j].Err.Error(), Equals, output[i].Warnings[j])
		}
	}
}

func (s *testIntegrationSuite) TestIssue15858(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustExec("select * from t t1, (select a from t order by a+1) t2 where t1.a = t2.a")
}

func (s *testIntegrationSuite) TestIssue15846(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT UNIQUE);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))

	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))

	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("create unique index idx on t0(t0);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))
}

func (s *testIntegrationSuite) TestFloorUnixTimestampPruning(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists floor_unix_timestamp")
	tk.MustExec(`create table floor_unix_timestamp (ts timestamp(3))
partition by range (floor(unix_timestamp(ts))) (
partition p0 values less than (unix_timestamp('2020-04-05 00:00:00')),
partition p1 values less than (unix_timestamp('2020-04-12 00:00:00')),
partition p2 values less than (unix_timestamp('2020-04-15 00:00:00')))`)
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-04 00:00:00')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-04 23:59:59.999')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-05 00:00:00')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-05 00:00:00.001')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-12 01:02:03.456')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-14 00:00:42')")
	tk.MustQuery("select count(*) from floor_unix_timestamp where '2020-04-05 00:00:00.001' = ts").Check(testkit.Rows("1"))
	tk.MustQuery("select * from floor_unix_timestamp where ts > '2020-04-05 00:00:00' order by ts").Check(testkit.Rows("2020-04-05 00:00:00.001", "2020-04-12 01:02:03.456", "2020-04-14 00:00:42.000"))
	tk.MustQuery("select count(*) from floor_unix_timestamp where ts <= '2020-04-05 23:00:00'").Check(testkit.Rows("4"))
	tk.MustQuery("select * from floor_unix_timestamp partition(p1, p2) where ts > '2020-04-14 00:00:00'").Check(testkit.Rows("2020-04-14 00:00:42.000"))
}

func (s *testIntegrationSuite) TestIssue16290And16292(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, primary key(a));")
	tk.MustExec("insert into t values(1, 1);")

	for i := 0; i <= 1; i++ {
		tk.MustExec(fmt.Sprintf("set session tidb_opt_agg_push_down = %v", i))

		tk.MustQuery("select avg(a) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1.0000"))
		tk.MustQuery("select avg(b) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1.0000"))
		tk.MustQuery("select count(distinct a) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1"))
		tk.MustQuery("select count(distinct b) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1"))
	}
}

func (s *testIntegrationSuite) TestTableDualWithRequiredProperty(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int, b int) partition by range(a) " +
		"(partition p0 values less than(10), partition p1 values less than MAXVALUE)")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("select /*+ MERGE_JOIN(t1, t2) */ * from t1 partition (p0), t2  where t1.a > 100 and t1.a = t2.a")
}

func (s *testIntegrationSuite) TestIndexJoinInnerIndexNDV(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int not null, c int not null)")
	tk.MustExec("create table t2(a int not null, b int not null, c int not null, index idx1(a,b), index idx2(c))")
	tk.MustExec("insert into t1 values(1,1,1),(1,1,1),(1,1,1)")
	tk.MustExec("insert into t2 values(1,1,1),(1,1,2),(1,1,3)")
	tk.MustExec("analyze table t1, t2")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSerialSuite) TestIssue16837(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int,b int,c int,d int,e int,unique key idx_ab(a,b),unique key(c),unique key(d))")
	tk.MustQuery("explain select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows(
		"Projection_4 10.00 root  test.t.a, test.t.b, test.t.c, test.t.d, test.t.e",
		"└─IndexMerge_9 0.01 root  ",
		"  ├─IndexRangeScan_5(Build) 10.00 cop[tikv] table:t, index:idx_ab(a, b) range:[1,1], keep order:false, stats:pseudo",
		"  ├─IndexRangeScan_6(Build) 1.00 cop[tikv] table:t, index:c(c) range:[1,1], keep order:false, stats:pseudo",
		"  └─Selection_8(Probe) 0.01 cop[tikv]  eq(test.t.e, 1)",
		"    └─TableRowIDScan_7 11.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("insert into t values (2, 1, 1, 1, 2)")
	tk.MustQuery("select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows())
}

func (s *testIntegrationSerialSuite) TestIndexMerge(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, unique key(a), unique key(b))")
	tk.MustQuery("desc select /*+ use_index_merge(t) */ * from t where a =1 or (b=1 and b+2>1)").Check(testkit.Rows(
		"Projection_4 8000.00 root  test.t.a, test.t.b",
		"└─IndexMerge_9 2.00 root  ",
		"  ├─IndexRangeScan_5(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"  ├─Selection_7(Build) 0.80 cop[tikv]  1",
		"  │ └─IndexRangeScan_6 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"  └─TableRowIDScan_8(Probe) 2.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc select /*+ use_index_merge(t) */ * from t where a =1 or (b=1 and length(b)=1)").Check(testkit.Rows(
		"Projection_4 1.80 root  test.t.a, test.t.b",
		"└─IndexMerge_9 2.00 root  ",
		"  ├─IndexRangeScan_5(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"  ├─Selection_7(Build) 0.80 cop[tikv]  eq(length(cast(1)), 1)",
		"  │ └─IndexRangeScan_6 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"  └─TableRowIDScan_8(Probe) 2.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc select /*+ use_index_merge(t) */ * from t where (a=1 and length(a)=1) or (b=1 and length(b)=1)").Check(testkit.Rows(
		"Projection_4 1.60 root  test.t.a, test.t.b",
		"└─IndexMerge_10 2.00 root  ",
		"  ├─Selection_6(Build) 0.80 cop[tikv]  eq(length(cast(1)), 1)",
		"  │ └─IndexRangeScan_5 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"  ├─Selection_8(Build) 0.80 cop[tikv]  eq(length(cast(1)), 1)",
		"  │ └─IndexRangeScan_7 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"  └─TableRowIDScan_9(Probe) 2.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc select /*+ use_index_merge(t) */ * from t where (a=1 and length(b)=1) or (b=1 and length(a)=1)").Check(testkit.Rows(
		"Projection_4 1.60 root  test.t.a, test.t.b",
		"└─IndexMerge_9 1.60 root  ",
		"  ├─IndexRangeScan_5(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"  ├─IndexRangeScan_6(Build) 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"  └─Selection_8(Probe) 1.60 cop[tikv]  eq(length(cast(test.t.a)), 1), eq(length(cast(test.t.b)), 1)",
		"    └─TableRowIDScan_7 2.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func (s *testIntegrationSerialSuite) TestIssue16407(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int,b char(100),key(a),key(b(10)))")
	tk.MustQuery("explain select /*+ use_index_merge(t) */ * from t where a=10 or b='x'").Check(testkit.Rows(
		"Projection_4 19.99 root  test.t.a, test.t.b",
		"└─IndexMerge_9 0.02 root  ",
		"  ├─IndexRangeScan_5(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"  ├─IndexRangeScan_6(Build) 10.00 cop[tikv] table:t, index:b(b) range:[\"x\",\"x\"], keep order:false, stats:pseudo",
		"  └─Selection_8(Probe) 0.02 cop[tikv]  eq(test.t.b, \"x\")",
		"    └─TableRowIDScan_7 19.99 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("insert into t values (1, 'xx')")
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where a=10 or b='x'").Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestStreamAggProp(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(1),(2)")

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
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testIntegrationSuite) TestOptimizeHintOnPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (
					a int, b int, c varchar(20),
					primary key(a), key(b), key(c)
				) partition by range columns(a) (
					partition p0 values less than(6),
					partition p1 values less than(11),
					partition p2 values less than(16));`)
	tk.MustExec(`insert into t values (1,1,"1"), (2,2,"2"), (8,8,"8"), (11,11,"11"), (15,15,"15")`)

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Warn = s.testData.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warn...))
	}
}

func (s *testIntegrationSerialSuite) TestNotReadOnlySQLOnTiFlash(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b varchar(20))")
	tk.MustExec(`set @@tidb_isolation_read_engines = "tiflash"`)
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	err := tk.ExecToErr("select * from t for update")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `[planner:1815]Internal : Can not find access path matching 'tidb_isolation_read_engines'(value: 'tiflash'). Available values are 'tiflash, tikv'.`)

	err = tk.ExecToErr("insert into t select * from t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `[planner:1815]Internal : Can not find access path matching 'tidb_isolation_read_engines'(value: 'tiflash'). Available values are 'tiflash, tikv'.`)

	tk.MustExec("prepare stmt_insert from 'insert into t select * from t where t.a = ?'")
	tk.MustExec("set @a=1")
	err = tk.ExecToErr("execute stmt_insert using @a")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `[planner:1815]Internal : Can not find access path matching 'tidb_isolation_read_engines'(value: 'tiflash'). Available values are 'tiflash, tikv'.`)
}

func (s *testIntegrationSuite) TestSelectLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(1),(2)")

	// normal test
	tk.MustExec("set @@session.sql_select_limit=1")
	result := tk.MustQuery("select * from t order by a")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t order by a limit 2")
	result.Check(testkit.Rows("1", "1"))
	tk.MustExec("set @@session.sql_select_limit=default")
	result = tk.MustQuery("select * from t order by a")
	result.Check(testkit.Rows("1", "1", "2"))

	// test for subquery
	tk.MustExec("set @@session.sql_select_limit=1")
	result = tk.MustQuery("select * from (select * from t) s order by a")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from (select * from t limit 2) s order by a") // limit write in subquery, has no effect.
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (select * from t limit 1) s") // limit write in subquery, has no effect.
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t where t.a in (select * from t) limit 3") // select_limit will not effect subquery
	result.Check(testkit.Rows("1", "1", "2"))
	result = tk.MustQuery("select * from (select * from t) s limit 3") // select_limit will not effect subquery
	result.Check(testkit.Rows("1", "1", "2"))

	// test for union
	result = tk.MustQuery("select * from t union all select * from t limit 2") // limit outside subquery
	result.Check(testkit.Rows("1", "1"))
	result = tk.MustQuery("select * from t union all (select * from t limit 2)") // limit inside subquery
	result.Check(testkit.Rows("1"))

	// test for prepare & execute
	tk.MustExec("prepare s1 from 'select * from t where a = ?'")
	tk.MustExec("set @a = 1")
	result = tk.MustQuery("execute s1 using @a")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@session.sql_select_limit=default")
	result = tk.MustQuery("execute s1 using @a")
	result.Check(testkit.Rows("1", "1"))
	tk.MustExec("set @@session.sql_select_limit=1")
	tk.MustExec("prepare s2 from 'select * from t where a = ? limit 3'")
	result = tk.MustQuery("execute s2 using @a") // if prepare stmt has limit, select_limit takes no effect.
	result.Check(testkit.Rows("1", "1"))

	// test for create view
	tk.MustExec("set @@session.sql_select_limit=1")
	tk.MustExec("create definer='root'@'localhost' view s as select * from t") // select limit should not effect create view
	result = tk.MustQuery("select * from s")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@session.sql_select_limit=default")
	result = tk.MustQuery("select * from s")
	result.Check(testkit.Rows("1", "1", "2"))

	// test for DML
	tk.MustExec("set @@session.sql_select_limit=1")
	tk.MustExec("create table b (a int)")
	tk.MustExec("insert into b select * from t") // all values are inserted
	result = tk.MustQuery("select * from b limit 3")
	result.Check(testkit.Rows("1", "1", "2"))
	tk.MustExec("update b set a = 2 where a = 1") // all values are updated
	result = tk.MustQuery("select * from b limit 3")
	result.Check(testkit.Rows("2", "2", "2"))
	result = tk.MustQuery("select * from b")
	result.Check(testkit.Rows("2"))
	tk.MustExec("delete from b where a = 2") // all values are deleted
	result = tk.MustQuery("select * from b")
	result.Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestHintParserWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, key(a), key(b));")
	tk.MustExec("select /*+ use_index_merge() */ * from t where a = 1 or b = 1;")
	rows := tk.MustQuery("show warnings;").Rows()
	c.Assert(len(rows), Equals, 1)
}

func (s *testIntegrationSuite) TestIssue16935(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (1), (1), (1), (1), (1), (1);")
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v0(c0) AS SELECT NULL FROM t0;")

	tk.MustQuery("SELECT * FROM t0 LEFT JOIN v0 ON TRUE WHERE v0.c0 IS NULL;")
}

func (s *testIntegrationSuite) TestAccessPathOnClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_clustered_index = 1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b varchar(20), c decimal(40,10), d int, primary key(a,b), key(c))")
	tk.MustExec(`insert into t1 values (1,"111",1.1,11), (2,"222",2.2,12), (3,"333",3.3,13)`)
	tk.MustExec("analyze table t1")

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

func (s *testIntegrationSuite) TestClusterIndexUniqueDoubleRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database cluster_idx_unique_double_read;")
	tk.MustExec("use cluster_idx_unique_double_read;")
	defer tk.MustExec("drop database cluster_idx_unique_double_read;")
	tk.MustExec("set @@tidb_enable_clustered_index = 1")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a varchar(64), b varchar(64), uk int, v int, primary key(a, b), unique key uuk(uk));")
	tk.MustExec("insert t values ('a', 'a1', 1, 11), ('b', 'b1', 2, 22), ('c', 'c1', 3, 33);")
	tk.MustQuery("select * from t use index (uuk);").Check(testkit.Rows("a a1 1 11", "b b1 2 22", "c c1 3 33"))
}

func (s *testIntegrationSuite) TestIndexJoinOnClusteredIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_clustered_index = 1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t (a int, b varchar(20), c decimal(40,10), d int, primary key(a,b), key(c))")
	tk.MustExec(`insert into t values (1,"111",1.1,11), (2,"222",2.2,12), (3,"333",3.3,13)`)
	tk.MustExec("analyze table t")

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
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}
func (s *testIntegrationSerialSuite) TestIssue18984(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("set tidb_enable_clustered_index=1")
	tk.MustExec("create table t(a int, b int, c int, primary key(a, b))")
	tk.MustExec("create table t2(a int, b int, c int, d int, primary key(a,b), index idx(c))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")
	tk.MustExec("insert into t2 values(1,2,3,4), (2,4,3,5), (1,3,1,1)")
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ * from t right outer join t2 on t.a=t2.c").Check(testkit.Rows(
		"1 1 1 1 3 1 1",
		"3 3 3 1 2 3 4",
		"3 3 3 2 4 3 5"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t2) */ * from t left outer join t2 on t.a=t2.c").Check(testkit.Rows(
		"1 1 1 1 3 1 1",
		"2 2 2 <nil> <nil> <nil> <nil>",
		"3 3 3 1 2 3 4",
		"3 3 3 2 4 3 5"))
}

func (s *testIntegrationSuite) TestDistinctScalarFunctionPushDown(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int not null, b int not null, c int not null, primary key (a,c)) partition by range (c) (partition p0 values less than (5), partition p1 values less than (10))")
	tk.MustExec("insert into t values(1,1,1),(2,2,2),(3,1,3),(7,1,7),(8,2,8),(9,2,9)")
	tk.MustQuery("select count(distinct b+1) as col from t").Check(testkit.Rows(
		"2",
	))
}

func (s *testIntegrationSerialSuite) TestExplainAnalyzePointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(20))")
	tk.MustExec("insert into t values (1,1)")

	res := tk.MustQuery("explain analyze select * from t where a=1;")
	checkExplain := func(rpc string) {
		resBuff := bytes.NewBufferString("")
		for _, row := range res.Rows() {
			fmt.Fprintf(resBuff, "%s\n", row)
		}
		explain := resBuff.String()
		c.Assert(strings.Contains(explain, rpc+":{num_rpc:"), IsTrue, Commentf("%s", explain))
		c.Assert(strings.Contains(explain, "total_time:"), IsTrue, Commentf("%s", explain))
	}
	checkExplain("Get")
	res = tk.MustQuery("explain analyze select * from t where a in (1,2,3);")
	checkExplain("BatchGet")
}

func (s *testIntegrationSerialSuite) TestExplainAnalyzeDML(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(" create table t (a int, b int, unique index (a));")
	tk.MustExec("insert into t values (1,1)")

	res := tk.MustQuery("explain analyze select * from t where a=1;")
	checkExplain := func(rpc string) {
		resBuff := bytes.NewBufferString("")
		for _, row := range res.Rows() {
			fmt.Fprintf(resBuff, "%s\n", row)
		}
		explain := resBuff.String()
		c.Assert(strings.Contains(explain, rpc+":{num_rpc:"), IsTrue, Commentf("%s", explain))
		c.Assert(strings.Contains(explain, "total_time:"), IsTrue, Commentf("%s", explain))
	}
	checkExplain("Get")
	res = tk.MustQuery("explain analyze insert ignore into t values (1,1),(2,2),(3,3),(4,4);")
	checkExplain("BatchGet")
}

func (s *testIntegrationSuite) TestPartitionExplain(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table pt (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)

	tk.MustExec("set @@tidb_enable_index_merge = 1;")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestPartialBatchPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c_int int, c_str varchar(40), primary key(c_int, c_str))")
	tk.MustExec("insert into t values (3, 'bose')")
	tk.MustQuery("select * from t where c_int in (3)").Check(testkit.Rows(
		"3 bose",
	))
	tk.MustQuery("select * from t where c_int in (3) or c_str in ('yalow') and c_int in (1, 2)").Check(testkit.Rows(
		"3 bose",
	))
}

func (s *testIntegrationSuite) TestIssue19926(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ta;")
	tk.MustExec("drop table if exists tb;")
	tk.MustExec("drop table if exists tc;")
	tk.MustExec("drop view if exists v;")
	tk.MustExec("CREATE TABLE `ta`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("CREATE TABLE `tb`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("CREATE TABLE `tc`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("insert into ta values('1','1');")
	tk.MustExec("insert into tb values('1','1');")
	tk.MustExec("insert into tc values('1','1');")
	tk.MustExec("create definer='root'@'localhost' view v as\nselect \nconcat(`ta`.`status`,`tb`.`status`) AS `status`, \n`ta`.`id` AS `id`  from (`ta` join `tb`) \nwhere (`ta`.`id` = `tb`.`id`);")
	tk.MustQuery("SELECT tc.status,v.id FROM tc, v WHERE tc.id = v.id AND v.status = '11';").Check(testkit.Rows("1 1"))
}

func (s *testIntegrationSuite) TestDeleteUsingJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(2,2)")
	tk.MustExec("delete t1.* from t1 join t2 using (a)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 2"))
}

func (s *testIntegrationSerialSuite) Test19942(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("SET @@tidb_enable_clustered_index=1;")
	tk.MustExec("CREATE TABLE test.`t` (" +
		"  `a` int(11) NOT NULL," +
		"  `b` varchar(10) COLLATE utf8_general_ci NOT NULL," +
		"  `c` varchar(50) COLLATE utf8_general_ci NOT NULL," +
		"  `d` char(10) NOT NULL," +
		"  PRIMARY KEY (`c`)," +
		"  UNIQUE KEY `a_uniq` (`a`)," +
		"  UNIQUE KEY `b_uniq` (`b`)," +
		"  UNIQUE KEY `d_uniq` (`d`)," +
		"  KEY `a_idx` (`a`)," +
		"  KEY `b_idx` (`b`)," +
		"  KEY `d_idx` (`d`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (1, '1', '0', '1');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (2, ' 2', ' 0', ' 2');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (3, '  3 ', '  3 ', '  3 ');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (4, 'a', 'a   ', 'a');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (5, ' A  ', ' A   ', ' A  ');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (6, ' E', 'é        ', ' E');")

	mkr := func() [][]interface{} {
		return testutil.RowsWithSep("|",
			"3|  3 |  3 |  3",
			"2| 2  0| 2",
			"5| A  | A   | A",
			"1|1|0|1",
			"4|a|a   |a",
			"6| E|é        | E")
	}
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`a_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`b_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`d_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`a_idx`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`b_idx`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`d_idx`);").Check(mkr())
	tk.MustExec("admin check table t")
}

func (s *testIntegrationSuite) TestPartitionUnionWithPPruningColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (\n  `fid` bigint(36) NOT NULL,\n  `oty` varchar(30) DEFAULT NULL,\n  `oid` int(11) DEFAULT NULL,\n  `pid` bigint(20) DEFAULT NULL,\n  `bid` int(11) DEFAULT NULL,\n  `r5` varchar(240) DEFAULT '',\n  PRIMARY KEY (`fid`)\n)PARTITION BY HASH( `fid` ) PARTITIONS 4;")

	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (59, 'm',  441, 1,  2143,  'LE1264_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (135, 'm',  1121, 1,  2423,  'LE2008_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (139, 'm',  1125, 1,  2432, 'LE2005_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (143, 'm',  1129, 1,  2438,  'LE2006_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (147, 'm',  1133, 1,  2446,  'LE2014_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (167, 'm',  1178, 1,  2512,  'LE2055_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (171, 'm',  1321, 1,  2542,  'LE1006_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (179, 'm',  1466, 1,  2648,  'LE2171_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (187, 'm',  1567, 1,  2690,  'LE1293_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (57, 'm',  341, 1,  2102,  'LE1001_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (137, 'm',  1123, 1,  2427,  'LE2003_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (145, 'm',  1131, 1,  2442,  'LE2048_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (138, 'm',  1124, 1,  2429,  'LE2004_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (142, 'm',  1128, 1,  2436,  'LE2049_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (174, 'm',  1381, 1,  2602,  'LE2170_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (28, 'm',  81, 1,  2023,  'LE1009_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (60, 'm',  442, 1,  2145,  'LE1263_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (136, 'm',  1122, 1,  2425,  'LE2002_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (140, 'm',  1126, 1,  2434,  'LE2001_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (168, 'm',  1179, 1,  2514,  'LE2052_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (196, 'm',  3380, 1,  2890,  'LE1300_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (208, 'm',  3861, 1,  3150,  'LE1323_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (432, 'm',  4060, 1,  3290,  'LE1327_r5');")

	tk.MustQuery("SELECT DISTINCT t.bid, t.r5 FROM t left join t parent on parent.oid = t.pid WHERE t.oty = 'm';").Sort().Check(
		testkit.Rows("2023 LE1009_r5",
			"2102 LE1001_r5",
			"2143 LE1264_r5",
			"2145 LE1263_r5",
			"2423 LE2008_r5",
			"2425 LE2002_r5",
			"2427 LE2003_r5",
			"2429 LE2004_r5",
			"2432 LE2005_r5",
			"2434 LE2001_r5",
			"2436 LE2049_r5",
			"2438 LE2006_r5",
			"2442 LE2048_r5",
			"2446 LE2014_r5",
			"2512 LE2055_r5",
			"2514 LE2052_r5",
			"2542 LE1006_r5",
			"2602 LE2170_r5",
			"2648 LE2171_r5",
			"2690 LE1293_r5",
			"2890 LE1300_r5",
			"3150 LE1323_r5",
			"3290 LE1327_r5"))
}

func (s *testIntegrationSuite) TestIssue20139(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, c int) partition by range (id) (partition p0 values less than (4), partition p1 values less than (7))")
	tk.MustExec("insert into t values(3, 3), (5, 5)")
	plan := tk.MustQuery("explain select * from t where c = 1 and id = c")
	plan.Check(testkit.Rows(
		"TableReader_7 0.01 root partition:p0 data:Selection_6",
		"└─Selection_6 0.01 cop[tikv]  eq(test.t.c, 1), eq(test.t.id, 1)",
		"  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustExec("drop table t")
}

func (s *testIntegrationSuite) TestIssue14481(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default null, b int default null, c int default null)")
	plan := tk.MustQuery("explain select * from t where a = 1 and a = 2")
	plan.Check(testkit.Rows("TableDual_5 8000.00 root  rows:0"))
	tk.MustExec("drop table t")
}

func (s *testIntegrationSerialSuite) TestIssue20710(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists s;")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table s(a int, b int, index(a))")
	tk.MustExec("insert into t values(1,1),(1,2),(2,2)")
	tk.MustExec("insert into s values(1,1),(2,2),(2,1)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestQueryBlockTableAliasInHint(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	c.Assert(tk.HasPlan("select /*+ HASH_JOIN(@sel_1 t2) */ * FROM (select 1) t1 NATURAL LEFT JOIN (select 2) t2", "HashJoin"), IsTrue)
	tk.MustQuery("select /*+ HASH_JOIN(@sel_1 t2) */ * FROM (select 1) t1 NATURAL LEFT JOIN (select 2) t2").Check(testkit.Rows(
		"1 2",
	))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
}

func (s *testIntegrationSuite) TestIssue10448(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")

	tk.MustExec("create table t(pk int(11) primary key)")
	tk.MustExec("insert into t values(1),(2),(3)")
	tk.MustQuery("select a from (select pk as a from t) t1 where a = 18446744073709551615").Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestMultiUpdateOnPrimaryKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int not null primary key)")
	tk.MustExec("insert into t values (1)")
	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = m.a + 10, n.a = n.a + 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) not null primary key)")
	tk.MustExec("insert into t values ('abc')")
	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = 'def', n.a = 'xyz'`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, primary key (a, b))")
	tk.MustExec("insert into t values (1, 2)")
	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = m.a + 10, n.b = n.b + 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 2)")
	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = m.a + 10, n.a = n.a + 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec(`UPDATE t m, t n SET m.b = m.b + 10, n.b = n.b + 10`)
	tk.MustQuery("SELECT * FROM t").Check(testkit.Rows("1 12"))

	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = m.a + 1, n.b = n.b + 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)
	tk.MustGetErrMsg(`UPDATE t m, t n, t q SET m.a = m.a + 1, n.b = n.b + 10, q.b = q.b - 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)
	tk.MustGetErrMsg(`UPDATE t m, t n, t q SET m.b = m.b + 1, n.a = n.a + 10, q.b = q.b - 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)
	tk.MustGetErrMsg(`UPDATE t m, t n, t q SET m.b = m.b + 1, n.b = n.b + 10, q.a = q.a - 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'q'.`)
	tk.MustGetErrMsg(`UPDATE t q, t n, t m SET m.b = m.b + 1, n.b = n.b + 10, q.a = q.a - 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'q' and 'n'.`)

	tk.MustExec("update t m, t n set m.a = n.a+10 where m.a=n.a")
	tk.MustQuery("select * from t").Check(testkit.Rows("11 12"))
}

func (s *testIntegrationSuite) TestOrderByHavingNotInSelect(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ttest")
	tk.MustExec("create table ttest (v1 int, v2 int)")
	tk.MustExec("insert into ttest values(1, 2), (4,6), (1, 7)")
	tk.MustGetErrMsg("select v1 from ttest order by count(v2)",
		"[planner:3029]Expression #1 of ORDER BY contains aggregate function and applies to the result of a non-aggregated query")
	tk.MustGetErrMsg("select v1 from ttest having count(v2)",
		"[planner:8123]In aggregated query without GROUP BY, expression #1 of SELECT list contains nonaggregated column 'v1'; this is incompatible with sql_mode=only_full_group_by")
}

func (s *testIntegrationSuite) TestUpdateSetDefault(c *C) {
	// #20598
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table tt (x int, z int as (x+10) stored)")
	tk.MustExec("insert into tt(x) values (1)")
	tk.MustExec("update tt set x=2, z = default")
	tk.MustQuery("select * from tt").Check(testkit.Rows("2 12"))

	tk.MustGetErrMsg("update tt set z = 123",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
	tk.MustGetErrMsg("update tt as ss set z = 123",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
	tk.MustGetErrMsg("update tt as ss set x = 3, z = 13",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
	tk.MustGetErrMsg("update tt as s1, tt as s2 set s1.z = default, s2.z = 456",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
}

func (s *testIntegrationSuite) TestOrderByNotInSelectDistinct(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// #12442
	tk.MustExec("drop table if exists ttest")
	tk.MustExec("create table ttest (v1 int, v2 int)")
	tk.MustExec("insert into ttest values(1, 2), (4,6), (1, 7)")

	tk.MustGetErrMsg("select distinct v1 from ttest order by v2",
		"[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.ttest.v2' which is not in SELECT list; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct v1+1 from ttest order by v1",
		"[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.ttest.v1' which is not in SELECT list; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct v1+1 from ttest order by 1+v1",
		"[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.ttest.v1' which is not in SELECT list; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct v1+1 from ttest order by v1+2",
		"[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.ttest.v1' which is not in SELECT list; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct count(v1) from ttest group by v2 order by sum(v1)",
		"[planner:3066]Expression #1 of ORDER BY clause is not in SELECT list, contains aggregate function; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct sum(v1)+1 from ttest group by v2 order by sum(v1)",
		"[planner:3066]Expression #1 of ORDER BY clause is not in SELECT list, contains aggregate function; this is incompatible with DISTINCT")

	// Expressions in ORDER BY whole match some fields in DISTINCT.
	tk.MustQuery("select distinct v1+1 from ttest order by v1+1").Check(testkit.Rows("2", "5"))
	tk.MustQuery("select distinct count(v1) from ttest order by count(v1)").Check(testkit.Rows("3"))
	tk.MustQuery("select distinct count(v1) from ttest group by v2 order by count(v1)").Check(testkit.Rows("1"))
	tk.MustQuery("select distinct sum(v1) from ttest group by v2 order by sum(v1)").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select distinct v1, v2 from ttest order by 1, 2").Check(testkit.Rows("1 2", "1 7", "4 6"))
	tk.MustQuery("select distinct v1, v2 from ttest order by 2, 1").Check(testkit.Rows("1 2", "4 6", "1 7"))

	// Referenced columns of expressions in ORDER BY whole match some fields in DISTINCT,
	// both original expression and alias can be referenced.
	tk.MustQuery("select distinct v1 from ttest order by v1+1").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select distinct v1, v2 from ttest order by v1+1, v2").Check(testkit.Rows("1 2", "1 7", "4 6"))
	tk.MustQuery("select distinct v1+1 as z, v2 from ttest order by v1+1, z+v2").Check(testkit.Rows("2 2", "2 7", "5 6"))
	tk.MustQuery("select distinct sum(v1) as z from ttest group by v2 order by z+1").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select distinct sum(v1)+1 from ttest group by v2 order by sum(v1)+1").Check(testkit.Rows("2", "5"))
	tk.MustQuery("select distinct v1 as z from ttest order by v1+z").Check(testkit.Rows("1", "4"))
}

func (s *testIntegrationSuite) TestInvalidNamedWindowSpec(c *C) {
	// #12356
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS temptest")
	tk.MustExec("create table temptest (val int, val1 int)")
	tk.MustQuery("SELECT val FROM temptest WINDOW w AS (ORDER BY val RANGE 1 PRECEDING)").Check(testkit.Rows())
	tk.MustGetErrMsg("SELECT val FROM temptest WINDOW w AS (ORDER BY val, val1 RANGE 1 PRECEDING)",
		"[planner:3587]Window 'w' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type")
	tk.MustGetErrMsg("select val1, avg(val1) as a from temptest group by val1 window w as (order by a)",
		"[planner:1054]Unknown column 'a' in 'window order by'")
	tk.MustGetErrMsg("select val1, avg(val1) as a from temptest group by val1 window w as (partition by a)",
		"[planner:1054]Unknown column 'a' in 'window partition by'")
}

func (s *testIntegrationSuite) TestCorrelatedAggregate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// #18350
	tk.MustExec("DROP TABLE IF EXISTS tab, tab2")
	tk.MustExec("CREATE TABLE tab(i INT)")
	tk.MustExec("CREATE TABLE tab2(j INT)")
	tk.MustExec("insert into tab values(1),(2),(3)")
	tk.MustExec("insert into tab2 values(1),(2),(3),(15)")
	tk.MustQuery(`SELECT m.i,
       (SELECT COUNT(n.j)
           FROM tab2 WHERE j=15) AS o
    FROM tab m, tab2 n GROUP BY 1 order by m.i`).Check(testkit.Rows("1 4", "2 4", "3 4"))
	tk.MustQuery(`SELECT
         (SELECT COUNT(n.j)
             FROM tab2 WHERE j=15) AS o
    FROM tab m, tab2 n order by m.i`).Check(testkit.Rows("12"))

	// #17748
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (m int, n int)")
	tk.MustExec("insert into t1 values (2,2), (2,2), (3,3), (3,3), (3,3), (4,4)")
	tk.MustExec("insert into t2 values (1,11), (2,22), (3,32), (4,44), (4,44)")
	tk.MustExec("set @@sql_mode='TRADITIONAL'")

	tk.MustQuery(`select count(*) c, a,
		( select group_concat(count(a)) from t2 where m = a )
		from t1 group by a order by a`).
		Check(testkit.Rows("2 2 2", "3 3 3", "1 4 1,1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1,1),(2,1),(2,2),(3,1),(3,2),(3,3)")

	// Sub-queries in SELECT fields
	// from SELECT fields
	tk.MustQuery("select (select count(a)) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select (select (select count(a)))) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select (select count(n.a)) from t m order by count(m.b)) from t n").Check(testkit.Rows("6"))
	// from WHERE
	tk.MustQuery("select (select count(n.a) from t where count(n.a)=3) from t n").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select (select count(a) from t where count(distinct n.a)=3) from t n").Check(testkit.Rows("6"))
	// from HAVING
	tk.MustQuery("select (select count(n.a) from t having count(n.a)=6 limit 1) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select count(n.a) from t having count(distinct n.b)=3 limit 1) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select sum(distinct n.a) from t having count(distinct n.b)=3 limit 1) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select sum(distinct n.a) from t having count(distinct n.b)=6 limit 1) from t n").Check(testkit.Rows("<nil>"))
	// from ORDER BY
	tk.MustQuery("select (select count(n.a) from t order by count(n.b) limit 1) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select count(distinct n.b) from t order by count(n.b) limit 1) from t n").Check(testkit.Rows("3"))
	// from TableRefsClause
	tk.MustQuery("select (select cnt from (select count(a) cnt) s) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select count(cnt) from (select count(a) cnt) s) from t").Check(testkit.Rows("1"))
	// from sub-query inside aggregate
	tk.MustQuery("select (select sum((select count(a)))) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select sum((select count(a))+sum(a))) from t").Check(testkit.Rows("20"))
	// from GROUP BY
	tk.MustQuery("select (select count(a) from t group by count(n.a)) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select count(distinct a) from t group by count(n.a)) from t n").Check(testkit.Rows("3"))

	// Sub-queries in HAVING
	tk.MustQuery("select sum(a) from t having (select count(a)) = 0").Check(testkit.Rows())
	tk.MustQuery("select sum(a) from t having (select count(a)) > 0").Check(testkit.Rows("14"))

	// Sub-queries in ORDER BY
	tk.MustQuery("select count(a) from t group by b order by (select count(a))").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select count(a) from t group by b order by (select -count(a))").Check(testkit.Rows("3", "2", "1"))

	// Nested aggregate (correlated aggregate inside aggregate)
	tk.MustQuery("select (select sum(count(a))) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select sum(sum(a))) from t").Check(testkit.Rows("14"))

	// Combining aggregates
	tk.MustQuery("select count(a), (select count(a)) from t").Check(testkit.Rows("6 6"))
	tk.MustQuery("select sum(distinct b), count(a), (select count(a)), (select cnt from (select sum(distinct b) as cnt) n) from t").
		Check(testkit.Rows("6 6 6 6"))
}

func (s *testIntegrationSuite) TestCorrelatedColumnAggFuncPushDown(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1,1);")
	tk.MustQuery("select (select count(n.a + a) from t) from t n;").Check(testkit.Rows(
		"1",
	))
}

// Test for issue https://github.com/pingcap/tidb/issues/21607.
func (s *testIntegrationSuite) TestConditionColPruneInPhysicalUnionScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustQuery("select count(*) from t where b = 1 and b in (3);").
		Check(testkit.Rows("0"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int as (a + 1), c int as (b + 1));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t (a) values (1);")
	tk.MustQuery("select count(*) from t where b = 1 and b in (3);").
		Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t where c = 1 and c in (3);").
		Check(testkit.Rows("0"))
}

func (s *testIntegrationSuite) TestInvalidHint(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt(a int, key(a));")

	var input []string
	var output []struct {
		SQL      string
		Plan     []string
		Warnings []string
	}
	s.testData.GetTestCases(c, &input, &output)
	warning := "show warnings;"
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warnings = s.testData.ConvertRowsToStrings(tk.MustQuery(warning).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

// Test for issue https://github.com/pingcap/tidb/issues/18320
func (s *testIntegrationSuite) TestNonaggregateColumnWithSingleValueInOnlyFullGroupByMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 2, 3), (4, 5, 6), (7, 8, 9)")
	tk.MustQuery("select a, count(b) from t where a = 1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select a, count(b) from t where a = 10").Check(testkit.Rows("<nil> 0"))
	tk.MustQuery("select a, c, sum(b) from t where a = 1 group by c").Check(testkit.Rows("1 3 2"))
	tk.MustGetErrMsg("select a from t where a = 1 order by count(b)", "[planner:3029]Expression #1 of ORDER BY contains aggregate function and applies to the result of a non-aggregated query")
	tk.MustQuery("select a from t where a = 1 having count(b) > 0").Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestConvertRangeToPoint(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("create table t0 (a int, b int, index(a, b))")
	tk.MustExec("insert into t0 values (1, 1)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (3, 3)")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, c int, index(a, b, c))")

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a float, b float, index(a, b))")

	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (a char(10), b char(10), c char(10), index(a, b, c))")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}
