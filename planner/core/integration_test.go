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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
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

func (s *testIntegrationSuite) TestAggColumnPrune(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(2)")

	var input []string
	var output []struct {
		SQL string
		Res []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
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

func (s *testIntegrationSerialSuite) TestPushDownToTiFlashWithKeepOrder(c *C) {
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
	// join with non-equal condition not supported
	_, err = tk.Exec("explain select /*+ broadcast_join(fact_t, d1_t) */ count(*) from fact_t join d1_t on fact_t.d1_k = d1_t.d1_k and fact_t.col1 > d1_t.value")
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
		"  └─StreamAgg_8 1.00 batchCop[tiflash]  funcs:approx_count_distinct(test.t.a)->Column#5",
		"    └─TableFullScan_15 10000.00 batchCop[tiflash] table:t keep order:false, stats:pseudo"))

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
	str := tk.MustQuery(`desc select * from t where a < '2000-01-01';`).Rows()[2][3].(string)
	c.Assert(strings.Contains(str, "partition:p1"), IsTrue)
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
	tk.MustExec(fmt.Sprintf("set session tidb_opt_agg_push_down=1"))
	tk.MustQuery("explain select approx_count_distinct(a), b from t group by b order by b desc").Check(testkit.Rows("Sort_11 16000.00 root  test.t.b:desc",
		"└─HashAgg_14 16000.00 root  group by:test.t.b, funcs:approx_count_distinct(Column#5)->Column#4, funcs:firstrow(Column#6)->test.t.b",
		"  └─PartitionUnion_15 16000.00 root  ",
		"    ├─HashAgg_16 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->Column#5, funcs:firstrow(test.t.b)->Column#6, funcs:firstrow(test.t.b)->test.t.b",
		"    │ └─TableReader_20 10000.00 root  data:TableFullScan_19",
		"    │   └─TableFullScan_19 10000.00 cop[tikv] table:t, partition:p0 keep order:false, stats:pseudo",
		"    └─HashAgg_23 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->Column#5, funcs:firstrow(test.t.b)->Column#6, funcs:firstrow(test.t.b)->test.t.b",
		"      └─TableReader_27 10000.00 root  data:TableFullScan_26",
		"        └─TableFullScan_26 10000.00 cop[tikv] table:t, partition:p1 keep order:false, stats:pseudo"))
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

func (s *testIntegrationSuite) TestIssue15858(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustExec("select * from t t1, (select a from t order by a+1) t2 where t1.a = t2.a")
}

func (s *testIntegrationSerialSuite) TestIssue16837(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int,b int,c int,d int,e int,unique key idx_ab(a,b),unique key(c),unique key(d))")
	tk.MustQuery("explain select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows(
		"IndexMerge_9 8.80 root  ",
		"├─IndexRangeScan_5(Build) 10.00 cop[tikv] table:t, index:idx_ab(a, b) range:[1,1], keep order:false, stats:pseudo",
		"├─IndexRangeScan_6(Build) 1.00 cop[tikv] table:t, index:c(c) range:[1,1], keep order:false, stats:pseudo",
		"└─Selection_8(Probe) 8.80 cop[tikv]  or(eq(test.t.a, 1), and(eq(test.t.e, 1), eq(test.t.c, 1)))",
		"  └─TableRowIDScan_7 11.00 cop[tikv] table:t keep order:false, stats:pseudo"))
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
		"IndexMerge_9 1.80 root  ",
		"├─IndexRangeScan_5(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─Selection_7(Build) 0.80 cop[tikv]  gt(plus(test.t.b, 2), 1)",
		"│ └─IndexRangeScan_6 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan_8(Probe) 1.80 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc select /*+ use_index_merge(t) */ * from t where a =1 or (b=1 and length(b)=1)").Check(testkit.Rows(
		"IndexMerge_9 1.80 root  ",
		"├─IndexRangeScan_5(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─Selection_7(Build) 0.80 cop[tikv]  eq(length(cast(test.t.b)), 1)",
		"│ └─IndexRangeScan_6 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan_8(Probe) 1.80 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc select /*+ use_index_merge(t) */ * from t where (a=1 and length(a)=1) or (b=1 and length(b)=1)").Check(testkit.Rows(
		"IndexMerge_10 1.60 root  ",
		"├─Selection_6(Build) 0.80 cop[tikv]  eq(length(cast(test.t.a)), 1)",
		"│ └─IndexRangeScan_5 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─Selection_8(Build) 0.80 cop[tikv]  eq(length(cast(test.t.b)), 1)",
		"│ └─IndexRangeScan_7 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan_9(Probe) 1.60 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc select /*+ use_index_merge(t) */ * from t where (a=1 and length(b)=1) or (b=1 and length(a)=1)").Check(testkit.Rows(
		"IndexMerge_9 1.60 root  ",
		"├─IndexRangeScan_5(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─IndexRangeScan_6(Build) 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─Selection_8(Probe) 1.60 cop[tikv]  or(and(eq(test.t.a, 1), eq(length(cast(test.t.b)), 1)), and(eq(test.t.b, 1), eq(length(cast(test.t.a)), 1)))",
		"  └─TableRowIDScan_7 2.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func (s *testIntegrationSerialSuite) TestIssue16407(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int,b char(100),key(a),key(b(10)))")
	tk.MustQuery("explain select /*+ use_index_merge(t) */ * from t where a=10 or b='x'").Check(testkit.Rows(
		"IndexMerge_9 16.00 root  ",
		"├─IndexRangeScan_5(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan_6(Build) 10.00 cop[tikv] table:t, index:b(b) range:[\"x\",\"x\"], keep order:false, stats:pseudo",
		"└─Selection_8(Probe) 16.00 cop[tikv]  or(eq(test.t.a, 10), eq(test.t.b, \"x\"))",
		"  └─TableRowIDScan_7 20.00 cop[tikv] table:t keep order:false, stats:pseudo"))
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

func (s *testIntegrationSuite) TestIssue16935(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (1), (1), (1), (1), (1), (1);")
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v0(c0) AS SELECT NULL FROM t0;")

	tk.MustQuery("SELECT * FROM t0 LEFT JOIN v0 ON TRUE WHERE v0.c0 IS NULL;")
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

func (s *testIntegrationSuite) TestIssue24281(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists member, agent, deposit, view_member_agents")
	tk.MustExec("create table member(login varchar(50) NOT NULL, agent_login varchar(100) DEFAULT NULL, PRIMARY KEY(login))")
	tk.MustExec("create table agent(login varchar(50) NOT NULL, data varchar(100) DEFAULT NULL, share_login varchar(50) NOT NULL, PRIMARY KEY(login))")
	tk.MustExec("create table deposit(id varchar(50) NOT NULL, member_login varchar(50) NOT NULL, transfer_amount int NOT NULL, PRIMARY KEY(id), KEY midx(member_login, transfer_amount))")
	tk.MustExec("create definer='root'@'localhost' view view_member_agents (member, share_login) as select m.login as member, a.share_login AS share_login from member as m join agent as a on m.agent_login = a.login")

	tk.MustExec(" select s.member_login as v1, SUM(s.transfer_amount) AS v2 " +
		"FROM deposit AS s " +
		"JOIN view_member_agents AS v ON s.member_login = v.member " +
		"WHERE 1 = 1 AND v.share_login = 'somevalue' " +
		"GROUP BY s.member_login " +
		"UNION select 1 as v1, 2 as v2")
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

func (s *testIntegrationSuite) TestUpdateMultiUpdatePK(c *C) {
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

	tk.MustExec(`UPDATE t m, t n SET m.a = m.a + 1, n.b = n.b + 10`)
	tk.MustQuery("SELECT * FROM t").Check(testkit.Rows("2 12"))
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

func (s *testIntegrationSuite) TestIssue22040(c *C) {
	// #22040
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, primary key(a,b))")
	// valid case
	tk.MustExec("select * from t where (a,b) in ((1,2),(1,2))")
	// invalid case, column count doesn't match
	{
		err := tk.ExecToErr("select * from t where (a,b) in (1,2)")
		c.Assert(errors.Cause(err), FitsTypeOf, expression.ErrOperandColumns)
	}
	{
		err := tk.ExecToErr("select * from t where (a,b) in ((1,2),1)")
		c.Assert(errors.Cause(err), FitsTypeOf, expression.ErrOperandColumns)
	}
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
	tk.MustGetErrMsg("select v2, v1 from (select * from ttest) t1 join (select 1, 2) t2 group by v1",
		"[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t1.v2' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustGetErrMsg("select v2, v1 from (select t1.v1, t2.v2 from ttest t1 join ttest t2) t3 join (select 1, 2) t2 group by v1",
		"[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t3.v2' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
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

func (s *testIntegrationSuite) TestIssue22105(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t1 (
  key1 int(11) NOT NULL,
  key2 int(11) NOT NULL,
  key3 int(11) NOT NULL,
  key4 int(11) NOT NULL,
  key5 int(11) DEFAULT NULL,
  key6 int(11) DEFAULT NULL,
  key7 int(11) NOT NULL,
  key8 int(11) NOT NULL,
  KEY i1 (key1),
  KEY i2 (key2),
  KEY i3 (key3),
  KEY i4 (key4),
  KEY i5 (key5),
  KEY i6 (key6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)

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

func (s *testIntegrationSerialSuite) TestLimitIndexLookUpKeepOrder(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, c int, d int, index idx(a,b,c));")

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

func (s *testIntegrationSuite) TestReorderSimplifiedOuterJoins(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t1 (pk char(32) primary key, col1 char(32), col2 varchar(40), col3 char(32), key (col1), key (col3), key (col2,col3), key (col1,col3))")
	tk.MustExec("create table t2 (pk char(32) primary key, col1 varchar(100))")
	tk.MustExec("create table t3 (pk char(32) primary key, keycol varchar(100), pad1 tinyint(1) default null, pad2 varchar(40), key (keycol,pad1,pad2))")

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

func (s *testIntegrationSerialSuite) TestDeleteStmt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("delete t from t;")
	tk.MustExec("delete t from test.t as t;")
	tk.MustGetErrCode("delete test.t from test.t as t;", mysql.ErrUnknownTable)
	tk.MustExec("delete test.t from t;")
	tk.MustExec("create database db1")
	tk.MustExec("use db1")
	tk.MustExec("create table t(a int)")
	tk.MustGetErrCode("delete test.t from t;", mysql.ErrUnknownTable)
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

func (s *testIntegrationSuite) TestIssue22071(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values(1),(2),(5)")
	tk.MustQuery("select n in (1,2) from (select a in (1,2) as n from t) g;").Sort().Check(testkit.Rows("0", "1", "1"))
	tk.MustQuery("select n in (1,n) from (select a in (1,2) as n from t) g;").Check(testkit.Rows("1", "1", "1"))
}

func (s *testIntegrationSuite) TestIndexMergeTableFilter(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, c int, d int, key(a), key(b));")
	tk.MustExec("insert into t values(10,1,1,10)")

	tk.MustQuery("explain select /*+ use_index_merge(t) */ * from t where a=10 or (b=10 and c=10)").Check(testkit.Rows(
		"IndexMerge_9 16.00 root  ",
		"├─IndexRangeScan_5(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan_6(Build) 10.00 cop[tikv] table:t, index:b(b) range:[10,10], keep order:false, stats:pseudo",
		"└─Selection_8(Probe) 16.00 cop[tikv]  or(eq(test.t.a, 10), and(eq(test.t.b, 10), eq(test.t.c, 10)))",
		"  └─TableRowIDScan_7 20.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where a=10 or (b=10 and c=10)").Check(testkit.Rows(
		"10 1 1 10",
	))
	tk.MustQuery("explain select /*+ use_index_merge(t) */ * from t where (a=10 and d=10) or (b=10 and c=10)").Check(testkit.Rows(
		"IndexMerge_9 16.00 root  ",
		"├─IndexRangeScan_5(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan_6(Build) 10.00 cop[tikv] table:t, index:b(b) range:[10,10], keep order:false, stats:pseudo",
		"└─Selection_8(Probe) 16.00 cop[tikv]  or(and(eq(test.t.a, 10), eq(test.t.d, 10)), and(eq(test.t.b, 10), eq(test.t.c, 10)))",
		"  └─TableRowIDScan_7 20.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where (a=10 and d=10) or (b=10 and c=10)").Check(testkit.Rows(
		"10 1 1 10",
	))
}

func (s *testIntegrationSuite) TestIssue22850(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t1 (a int(11))")
	tk.MustQuery("SELECT @v:=(SELECT 1 FROM t1 t2 LEFT JOIN t1 ON t1.a GROUP BY t1.a) FROM t1").Check(testkit.Rows()) // work fine
}

// #22949: test HexLiteral Used in GetVar expr
func (s *testIntegrationSuite) TestGetVarExprWithHexLiteral(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1_no_idx;")
	tk.MustExec("create table t1_no_idx(id int, col_bit bit(16));")
	tk.MustExec("insert into t1_no_idx values(1, 0x3135);")
	tk.MustExec("insert into t1_no_idx values(2, 0x0f);")

	tk.MustExec("prepare stmt from 'select id from t1_no_idx where col_bit = ?';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 0x0F;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("2"))

	// same test, but use IN expr
	tk.MustExec("prepare stmt from 'select id from t1_no_idx where col_bit in (?)';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 0x0F;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("2"))

	// same test, but use table with index on col_bit
	tk.MustExec("drop table if exists t2_idx;")
	tk.MustExec("create table t2_idx(id int, col_bit bit(16), key(col_bit));")
	tk.MustExec("insert into t2_idx values(1, 0x3135);")
	tk.MustExec("insert into t2_idx values(2, 0x0f);")

	tk.MustExec("prepare stmt from 'select id from t2_idx where col_bit = ?';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 0x0F;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("2"))

	// same test, but use IN expr
	tk.MustExec("prepare stmt from 'select id from t2_idx where col_bit in (?)';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 0x0F;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("2"))

	// test col varchar with GetVar
	tk.MustExec("drop table if exists t_varchar;")
	tk.MustExec("create table t_varchar(id int, col_varchar varchar(100), key(col_varchar));")
	tk.MustExec("insert into t_varchar values(1, '15');")
	tk.MustExec("prepare stmt from 'select id from t_varchar where col_varchar = ?';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
}

// test BitLiteral used with GetVar
func (s *testIntegrationSuite) TestGetVarExprWithBitLiteral(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1_no_idx;")
	tk.MustExec("create table t1_no_idx(id int, col_bit bit(16));")
	tk.MustExec("insert into t1_no_idx values(1, 0x3135);")
	tk.MustExec("insert into t1_no_idx values(2, 0x0f);")

	tk.MustExec("prepare stmt from 'select id from t1_no_idx where col_bit = ?';")
	// 0b11000100110101 is 0x3135
	tk.MustExec("set @a = 0b11000100110101;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))

	// same test, but use IN expr
	tk.MustExec("prepare stmt from 'select id from t1_no_idx where col_bit in (?)';")
	tk.MustExec("set @a = 0b11000100110101;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestIssue23846(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varbinary(10),UNIQUE KEY(a))")
	tk.MustExec("insert into t values(0x00A4EEF4FA55D6706ED5)")
	tk.MustQuery("select * from t where a=0x00A4EEF4FA55D6706ED5").Check(testkit.Rows("\x00\xa4\xee\xf4\xfaU\xd6pn\xd5")) // not empty
}

func (s *testIntegrationSuite) TestIssue26559(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a timestamp, b datetime);")
	tk.MustExec("insert into t values('2020-07-29 09:07:01', '2020-07-27 16:57:36');")
	tk.MustQuery("select greatest(a, b) from t union select null;").Sort().Check(testkit.Rows("2020-07-29 09:07:01", "<nil>"))
}

func (s *testIntegrationSuite) TestNaturalJoinUpdateSameTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// The logic in this test was done in #31779 with more strict handling, compare to 5.0 or 5.1 version of this test
	tk.MustExec("create database natural_join_update")
	defer tk.MustExec("drop database natural_join_update")
	tk.MustExec("use natural_join_update")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustExec("update t1 as a natural join t1 b SET a.a = a.a + 2, b.b = 3")
	tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("3 1", "4 2"))
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int, b int, primary key (a,b))")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 b SET a.a = a.a + 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int, b int) partition by hash (a+b) partitions 3")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 b SET a.a = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (A int, b int) partition by hash (a+b) partitions 3")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 B SET a.A = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	_, err := tk.Exec(`update t1 as a natural join t1 B SET a.A = 2, b.b = 3`)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".planner:1706.Primary key/partition key update is not allowed since the table is updated both as 'a' and 'B'.")
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (A int, b int) partition by RANGE COLUMNS (A) (partition `pNeg` values less than (0),partition `pPos` values less than MAXVALUE)")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 B SET a.A = 2, b.a = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
}

func (s *testIntegrationSuite) TestIssue27797(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t27797")
	tk.MustExec("create table t27797(a int, b int, c int, d int) " +
		"partition by range columns(d) (" +
		"partition p0 values less than (20)," +
		"partition p1 values less than(40)," +
		"partition p2 values less than(60));")
	tk.MustExec("insert into t27797 values(1,1,1,1), (2,2,2,2), (22,22,22,22), (44,44,44,44);")
	tk.MustExec("set sql_mode='';")
	result := tk.MustQuery("select count(*) from (select a, b from t27797 where d > 1 and d < 60 and b > 0 group by b, c) tt;")
	result.Check(testkit.Rows("3"))

	tk.MustExec("drop table if exists IDT_HP24172")
	tk.MustExec("CREATE TABLE `IDT_HP24172` ( " +
		"`COL1` mediumint(16) DEFAULT NULL, " +
		"`COL2` varchar(20) DEFAULT NULL, " +
		"`COL4` datetime DEFAULT NULL, " +
		"`COL3` bigint(20) DEFAULT NULL, " +
		"`COL5` float DEFAULT NULL, " +
		"KEY `UM_COL` (`COL1`,`COL3`) " +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin " +
		"PARTITION BY HASH( `COL1`+`COL3` ) " +
		"PARTITIONS 8;")
	tk.MustExec("insert into IDT_HP24172(col1) values(8388607);")
	result = tk.MustQuery("select col2 from IDT_HP24172 where col1 = 8388607 and col1 in (select col1 from IDT_HP24172);")
	result.Check(testkit.Rows("<nil>"))
}

func (s *testIntegrationSuite) TestIssues29711(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tbl_29711")
	tk.MustExec("CREATE TABLE `tbl_29711` (" +
		"`col_250` text COLLATE utf8_unicode_ci NOT NULL," +
		"`col_251` varchar(10) COLLATE utf8_unicode_ci NOT NULL," +
		"PRIMARY KEY (`col_251`,`col_250`(1)));")
	tk.MustQuery("explain " +
		"select /*+ LIMIT_TO_COP() */ col_250,col_251 from tbl_29711 use index (primary) where col_251 between 'Bob' and 'David' order by col_250,col_251 limit 6;").
		Check(testkit.Rows(
			"TopN_9 6.00 root  test.tbl_29711.col_250:asc, test.tbl_29711.col_251:asc, offset:0, count:6",
			"└─IndexLookUp_15 6.00 root  ",
			"  ├─IndexRangeScan_12(Build) 250.00 cop[tikv] table:tbl_29711, index:PRIMARY(col_251, col_250) range:[\"Bob\",\"David\"], keep order:false, stats:pseudo",
			"  └─TopN_14(Probe) 6.00 cop[tikv]  test.tbl_29711.col_250:asc, test.tbl_29711.col_251:asc, offset:0, count:6",
			"    └─TableRowIDScan_13 250.00 cop[tikv] table:tbl_29711 keep order:false, stats:pseudo",
		))

	tk.MustExec("drop table if exists t29711")
	tk.MustExec("CREATE TABLE `t29711` (" +
		"`a` varchar(10) DEFAULT NULL," +
		"`b` int(11) DEFAULT NULL," +
		"`c` int(11) DEFAULT NULL," +
		"KEY `ia` (`a`(2)))")
	tk.MustQuery("explain select /*+ LIMIT_TO_COP() */ * from t29711 use index (ia) order by a limit 10;").
		Check(testkit.Rows(
			"TopN_8 10.00 root  test.t29711.a:asc, offset:0, count:10",
			"└─IndexLookUp_14 10.00 root  ",
			"  ├─IndexFullScan_11(Build) 10000.00 cop[tikv] table:t29711, index:ia(a) keep order:false, stats:pseudo",
			"  └─TopN_13(Probe) 10.00 cop[tikv]  test.t29711.a:asc, offset:0, count:10",
			"    └─TableRowIDScan_12 10000.00 cop[tikv] table:t29711 keep order:false, stats:pseudo",
		))

}
