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

func (s *testIntegrationSuite) TestAggPushDownLeftJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists customer")
	tk.MustExec("create table customer (C_CUSTKEY bigint(20) NOT NULL, C_NAME varchar(25) NOT NULL, " +
		"C_ADDRESS varchar(25) NOT NULL, PRIMARY KEY (`C_CUSTKEY`) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("drop table if exists orders")
	tk.MustExec("create table orders (O_ORDERKEY bigint(20) NOT NULL, O_CUSTKEY bigint(20) NOT NULL, " +
		"O_TOTALPRICE decimal(15,2) NOT NULL, PRIMARY KEY (`O_ORDERKEY`) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("insert into customer values (6, \"xiao zhang\", \"address1\");")
	tk.MustExec("set @@tidb_opt_agg_push_down=1;")

	tk.MustQuery("select c_custkey, count(o_orderkey) as c_count from customer left outer join orders " +
		"on c_custkey = o_custkey group by c_custkey").Check(testkit.Rows("6 0"))
	tk.MustQuery("explain format='brief' select c_custkey, count(o_orderkey) as c_count from customer left outer join orders " +
		"on c_custkey = o_custkey group by c_custkey").Check(testkit.Rows(
		"Projection 10000.00 root  test.customer.c_custkey, Column#7",
		"└─Projection 10000.00 root  if(isnull(Column#8), 0, 1)->Column#7, test.customer.c_custkey",
		"  └─HashJoin 10000.00 root  left outer join, equal:[eq(test.customer.c_custkey, test.orders.o_custkey)]",
		"    ├─HashAgg(Build) 8000.00 root  group by:test.orders.o_custkey, funcs:count(Column#9)->Column#8, funcs:firstrow(test.orders.o_custkey)->test.orders.o_custkey",
		"    │ └─TableReader 8000.00 root  data:HashAgg",
		"    │   └─HashAgg 8000.00 cop[tikv]  group by:test.orders.o_custkey, funcs:count(test.orders.o_orderkey)->Column#9",
		"    │     └─TableFullScan 10000.00 cop[tikv] table:orders keep order:false, stats:pseudo",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:customer keep order:false, stats:pseudo"))

	tk.MustQuery("select c_custkey, count(o_orderkey) as c_count from orders right outer join customer " +
		"on c_custkey = o_custkey group by c_custkey").Check(testkit.Rows("6 0"))
	tk.MustQuery("explain format='brief' select c_custkey, count(o_orderkey) as c_count from orders right outer join customer " +
		"on c_custkey = o_custkey group by c_custkey").Check(testkit.Rows(
		"Projection 10000.00 root  test.customer.c_custkey, Column#7",
		"└─Projection 10000.00 root  if(isnull(Column#8), 0, 1)->Column#7, test.customer.c_custkey",
		"  └─HashJoin 10000.00 root  right outer join, equal:[eq(test.orders.o_custkey, test.customer.c_custkey)]",
		"    ├─HashAgg(Build) 8000.00 root  group by:test.orders.o_custkey, funcs:count(Column#9)->Column#8, funcs:firstrow(test.orders.o_custkey)->test.orders.o_custkey",
		"    │ └─TableReader 8000.00 root  data:HashAgg",
		"    │   └─HashAgg 8000.00 cop[tikv]  group by:test.orders.o_custkey, funcs:count(test.orders.o_orderkey)->Column#9",
		"    │     └─TableFullScan 10000.00 cop[tikv] table:orders keep order:false, stats:pseudo",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:customer keep order:false, stats:pseudo"))
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

func (s *testIntegrationSuite) TestIssue22298(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a int, b int);`)
	tk.MustGetErrMsg(`select * from t where 0 and c = 10;`, "[planner:1054]Unknown column 'c' in 'where clause'")
}

func (s *testIntegrationSuite) TestIssue22828(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t (c int);`)
	tk.MustGetErrMsg(`select group_concat((select concat(c,group_concat(c)) FROM t where xxx=xxx)) FROM t;`, "[planner:1054]Unknown column 'xxx' in 'where clause'")
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
	tk.MustQuery("explain format = 'brief' select * from mysql.stats_meta").Check(testkit.Rows(
		"TableReader 10000.00 root  data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:stats_meta keep order:false, stats:pseudo"))

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
	tk.MustExec("set @@session.tidb_allow_mpp = 0")

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
	tk.MustExec("set @@session.tidb_allow_mpp = 0")
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

func (s *testIntegrationSerialSuite) TestMPPJoin(c *C) {
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
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
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

func (s *testIntegrationSerialSuite) TestMPPOuterJoinBuildSideForBroadcastJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists a")
	tk.MustExec("create table a(id int, value int)")
	tk.MustExec("insert into a values(1,2),(2,3)")
	tk.MustExec("analyze table a")
	tk.MustExec("drop table if exists b")
	tk.MustExec("create table b(id int, value int)")
	tk.MustExec("insert into b values(1,2),(2,3),(3,4)")
	tk.MustExec("analyze table b")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "a" || tblInfo.Name.L == "b" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 10000")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 10000")
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

func (s *testIntegrationSerialSuite) TestMPPOuterJoinBuildSideForShuffleJoinWithFixedBuildSide(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists a")
	tk.MustExec("create table a(id int, value int)")
	tk.MustExec("insert into a values(1,2),(2,3)")
	tk.MustExec("analyze table a")
	tk.MustExec("drop table if exists b")
	tk.MustExec("create table b(id int, value int)")
	tk.MustExec("insert into b values(1,2),(2,3),(3,4)")
	tk.MustExec("analyze table b")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "a" || tblInfo.Name.L == "b" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 1")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
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

func (s *testIntegrationSerialSuite) TestMPPOuterJoinBuildSideForShuffleJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists a")
	tk.MustExec("create table a(id int, value int)")
	tk.MustExec("insert into a values(1,2),(2,3)")
	tk.MustExec("analyze table a")
	tk.MustExec("drop table if exists b")
	tk.MustExec("create table b(id int, value int)")
	tk.MustExec("insert into b values(1,2),(2,3),(3,4)")
	tk.MustExec("analyze table b")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "a" || tblInfo.Name.L == "b" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
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

func (s *testIntegrationSerialSuite) TestMPPShuffledJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists d1_t")
	tk.MustExec("create table d1_t(d1_k int, value int)")
	tk.MustExec("insert into d1_t values(1,2),(2,3)")
	tk.MustExec("insert into d1_t values(1,2),(2,3)")
	tk.MustExec("analyze table d1_t")
	tk.MustExec("drop table if exists d2_t")
	tk.MustExec("create table d2_t(d2_k decimal(10,2), value int)")
	tk.MustExec("insert into d2_t values(10.11,2),(10.12,3)")
	tk.MustExec("insert into d2_t values(10.11,2),(10.12,3)")
	tk.MustExec("analyze table d2_t")
	tk.MustExec("drop table if exists d3_t")
	tk.MustExec("create table d3_t(d3_k date, value int)")
	tk.MustExec("insert into d3_t values(date'2010-01-01',2),(date'2010-01-02',3)")
	tk.MustExec("insert into d3_t values(date'2010-01-01',2),(date'2010-01-02',3)")
	tk.MustExec("analyze table d3_t")
	tk.MustExec("drop table if exists fact_t")
	tk.MustExec("create table fact_t(d1_k int, d2_k decimal(10,2), d3_k date, col1 int, col2 int, col3 int)")
	tk.MustExec("insert into fact_t values(1,10.11,date'2010-01-01',1,2,3),(1,10.11,date'2010-01-02',1,2,3),(1,10.12,date'2010-01-01',1,2,3),(1,10.12,date'2010-01-02',1,2,3)")
	tk.MustExec("insert into fact_t values(2,10.11,date'2010-01-01',1,2,3),(2,10.11,date'2010-01-02',1,2,3),(2,10.12,date'2010-01-01',1,2,3),(2,10.12,date'2010-01-02',1,2,3)")
	tk.MustExec("insert into fact_t values(2,10.11,date'2010-01-01',1,2,3),(2,10.11,date'2010-01-02',1,2,3),(2,10.12,date'2010-01-01',1,2,3),(2,10.12,date'2010-01-02',1,2,3)")
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
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")
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

func (s *testIntegrationSerialSuite) TestMPPJoinWithCanNotFoundColumnInSchemaColumnsError(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id int, v1 decimal(20,2), v2 decimal(20,2))")
	tk.MustExec("create table t2(id int, v1 decimal(10,2), v2 decimal(10,2))")
	tk.MustExec("create table t3(id int, v1 decimal(10,2), v2 decimal(10,2))")
	tk.MustExec("insert into t1 values(1,1,1),(2,2,2)")
	tk.MustExec("insert into t2 values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8)")
	tk.MustExec("insert into t3 values(1,1,1)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	tk.MustExec("analyze table t3")

	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t1" || tblInfo.Name.L == "t2" || tblInfo.Name.L == "t3" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_enforce_mpp = 1")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	tk.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 0")

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

func (s *testIntegrationSerialSuite) TestJoinNotSupportedByTiFlash(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("create table table_1(id int not null, bit_col bit(2) not null, datetime_col datetime not null)")
	tk.MustExec("insert into table_1 values(1,b'1','2020-01-01 00:00:00'),(2,b'0','2020-01-01 00:00:00')")
	tk.MustExec("analyze table table_1")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "table_1" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
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

	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}

	tk.MustExec("set @@session.tidb_allow_mpp = 0")
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_batch_cop = 1")
	tk.MustExec("set @@session.tidb_opt_broadcast_join = 1")
	// make cbo force choose broadcast join since sql hint does not work for semi/anti-semi join
	tk.MustExec("set @@session.tidb_opt_cpu_factor=10000000;")
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

func (s *testIntegrationSerialSuite) TestMPPWithHashExchangeUnderNewCollation(c *C) {
	defer collate.SetNewCollationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("create table table_1(id int not null, value char(10))")
	tk.MustExec("insert into table_1 values(1,'1'),(2,'2')")
	tk.MustExec("analyze table table_1")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "table_1" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	collate.SetNewCollationEnabledForTest(true)
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
	tk.MustExec("set @@session.tidb_opt_broadcast_join = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
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

func (s *testIntegrationSerialSuite) TestMPPWithBroadcastExchangeUnderNewCollation(c *C) {
	defer collate.SetNewCollationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("create table table_1(id int not null, value char(10))")
	tk.MustExec("insert into table_1 values(1,'1'),(2,'2')")
	tk.MustExec("analyze table table_1")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "table_1" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	collate.SetNewCollationEnabledForTest(true)
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
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

func (s *testIntegrationSerialSuite) TestMPPAvgRewrite(c *C) {
	defer collate.SetNewCollationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("create table table_1(id int not null, value decimal(10,2))")
	tk.MustExec("insert into table_1 values(1,1),(2,2)")
	tk.MustExec("analyze table table_1")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "table_1" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	collate.SetNewCollationEnabledForTest(true)
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
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
		"HashAgg_11 1.00 root  funcs:approx_count_distinct(Column#4)->Column#3",
		"└─TableReader_12 1.00 root  data:HashAgg_6",
		"  └─HashAgg_6 1.00 batchCop[tiflash]  funcs:approx_count_distinct(test.t.a)->Column#4",
		"    └─TableFullScan_10 10000.00 batchCop[tiflash] table:t keep order:false, stats:pseudo"))

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
	tk.MustExec("explain format = 'brief' SELECT count(*) FROM crm_rd_150m dataset_48 WHERE (CASE WHEN (month(dataset_48.customer_first_date)) <= 30 THEN '新客' ELSE NULL END) IS NOT NULL;")
}

func (s *testIntegrationSerialSuite) TestReadFromStorageHint(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, tt, ttt")
	tk.MustExec("set session tidb_allow_mpp=OFF")
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
		tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
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
	tk.MustExec("create table t(a int(11) not null, b int) partition by range (a) (partition p0 values less than (4), partition p1 values less than(10), partition p2 values less than maxvalue);")
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
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
	tk.MustExec("explain format = 'brief' select /*+ TIDB_INLJ(t1) */ * from t1 join t2 on t1.a = t2.a")
}

func (s *testIntegrationSuite) TestIndexJoinUniqueCompositeIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
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
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustQuery("explain format = 'brief' select approx_count_distinct(a), b from t group by b order by b desc").Check(testkit.Rows("Sort 16000.00 root  test.t.b:desc",
		"└─HashAgg 16000.00 root  group by:test.t.b, funcs:approx_count_distinct(Column#5)->Column#4, funcs:firstrow(Column#6)->test.t.b",
		"  └─PartitionUnion 16000.00 root  ",
		"    ├─HashAgg 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->Column#5, funcs:firstrow(test.t.b)->Column#6, funcs:firstrow(test.t.b)->test.t.b",
		"    │ └─TableReader 10000.00 root  data:TableFullScan",
		"    │   └─TableFullScan 10000.00 cop[tikv] table:t, partition:p0 keep order:false, stats:pseudo",
		"    └─HashAgg 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->Column#5, funcs:firstrow(test.t.b)->Column#6, funcs:firstrow(test.t.b)->test.t.b",
		"      └─TableReader 10000.00 root  data:TableFullScan",
		"        └─TableFullScan 10000.00 cop[tikv] table:t, partition:p1 keep order:false, stats:pseudo"))
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
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows(
		"IndexMerge 0.01 root  ",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:idx_ab(a, b) range:[1,1], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t, index:c(c) range:[1,1], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.01 cop[tikv]  or(eq(test.t.a, 1), and(eq(test.t.e, 1), eq(test.t.c, 1)))",
		"  └─TableRowIDScan 11.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("insert into t values (2, 1, 1, 1, 2)")
	tk.MustQuery("select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows())
}

func (s *testIntegrationSerialSuite) TestIndexMerge(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, unique key(a), unique key(b))")
	tk.MustQuery("desc format='brief' select /*+ use_index_merge(t) */ * from t where a =1 or (b=1 and b+2>1)").Check(testkit.Rows(
		"IndexMerge 2.00 root  ",
		"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─Selection(Build) 0.80 cop[tikv]  1",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 2.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc format='brief' select /*+ use_index_merge(t) */ * from t where a =1 or (b=1 and length(b)=1)").Check(testkit.Rows(
		"IndexMerge 2.00 root  ",
		"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─Selection(Build) 0.80 cop[tikv]  eq(length(cast(1)), 1)",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 2.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc format='brief' select /*+ use_index_merge(t) */ * from t where (a=1 and length(a)=1) or (b=1 and length(b)=1)").Check(testkit.Rows(
		"IndexMerge 2.00 root  ",
		"├─Selection(Build) 0.80 cop[tikv]  eq(length(cast(1)), 1)",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─Selection(Build) 0.80 cop[tikv]  eq(length(cast(1)), 1)",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 2.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("desc format='brief' select /*+ use_index_merge(t) */ * from t where (a=1 and length(b)=1) or (b=1 and length(a)=1)").Check(testkit.Rows(
		"IndexMerge 0.00 root  ",
		"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.00 cop[tikv]  or(and(eq(test.t.a, 1), eq(length(cast(test.t.b)), 1)), and(eq(test.t.b, 1), eq(length(cast(test.t.a)), 1)))",
		"  └─TableRowIDScan 2.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func (s *testIntegrationSerialSuite) TestIndexMergePartialScansClusteredIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, primary key (a, b) clustered, key idx_c(c));")
	tk.MustExec("insert into t values (1, 1, 1), (10, 10, 10), (100, 100, 100);")
	const queryTemplate = "select /*+ use_index_merge(t) */ %s from t where %s order by a, b;"
	projections := [][]string{{"a"}, {"b"}, {"c"}, {"a", "b"}, {"b", "c"}, {"c", "a"}, {"b", "a", "c"}}
	cases := []struct {
		condition string
		expected  []string
	}{
		{
			// 3 table scans
			"a < 2 or a < 10 or a > 11", []string{"1", "100"},
		},
		{
			// 3 index scans
			"c < 10 or c < 11 or c > 50", []string{"1", "10", "100"},
		},
		{
			// 1 table scan + 1 index scan
			"a < 2 or c > 10000", []string{"1"},
		},
		{
			// 2 table scans + 1 index scan
			"a < 2 or a > 88 or c > 10000", []string{"1", "100"},
		},
		{
			// 2 table scans + 2 index scans
			"a < 2 or (a >= 10 and b >= 10) or c > 100 or c < 1", []string{"1", "10", "100"},
		},
		{
			// 3 table scans + 2 index scans
			"a < 2 or (a >= 10 and b >= 10) or (a >= 20 and b < 10) or c > 100 or c < 1", []string{"1", "10", "100"},
		},
	}
	for _, p := range projections {
		for _, ca := range cases {
			query := fmt.Sprintf(queryTemplate, strings.Join(p, ","), ca.condition)
			tk.HasPlan(query, "IndexMerge")
			expected := make([]string, 0, len(ca.expected))
			for _, datum := range ca.expected {
				row := strings.Repeat(datum+" ", len(p))
				expected = append(expected, row[:len(row)-1])
			}
			tk.MustQuery(query).Check(testkit.Rows(expected...))
		}
	}
}

func (s *testIntegrationSerialSuite) TestIndexMergePartialScansTiDBRowID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, unique key (a, b), key idx_c(c));")
	tk.MustExec("insert into t values (1, 1, 1), (10, 10, 10), (100, 100, 100);")
	const queryTemplate = "select /*+ use_index_merge(t) */ %s from t where %s order by a;"
	projections := [][]string{{"a"}, {"b"}, {"c"}, {"a", "b"}, {"b", "c"}, {"c", "a"}, {"b", "a", "c"}}
	cases := []struct {
		condition string
		expected  []string
	}{
		{
			// 3 index scans
			"c < 10 or c < 11 or c > 50", []string{"1", "10", "100"},
		},
		{
			// 2 index scans
			"c < 10 or a < 2", []string{"1"},
		},
		{
			// 1 table scan + 1 index scan
			"_tidb_rowid < 2 or c > 10000", []string{"1"},
		},
		{
			// 2 table scans + 1 index scan
			"_tidb_rowid < 2 or _tidb_rowid < 10 or c > 11", []string{"1", "10", "100"},
		},
		{
			// 1 table scans + 3 index scans
			"_tidb_rowid < 2 or (a >= 10 and b >= 10) or c > 100 or c < 1", []string{"1", "10", "100"},
		},
		{
			// 1 table scans + 4 index scans
			"_tidb_rowid < 2 or (a >= 10 and b >= 10) or (a >= 20 and b < 10) or c > 100 or c < 1", []string{"1", "10", "100"},
		},
	}
	for _, p := range projections {
		for _, ca := range cases {
			query := fmt.Sprintf(queryTemplate, strings.Join(p, ","), ca.condition)
			tk.HasPlan(query, "IndexMerge")
			expected := make([]string, 0, len(ca.expected))
			for _, datum := range ca.expected {
				row := strings.Repeat(datum+" ", len(p))
				expected = append(expected, row[:len(row)-1])
			}
			tk.MustQuery(query).Check(testkit.Rows(expected...))
		}
	}
}

func (s *testIntegrationSerialSuite) TestIndexMergePartialScansPKIsHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, primary key (a), unique key (b), key idx_c(c));")
	tk.MustExec("insert into t values (1, 1, 1), (10, 10, 10), (100, 100, 100);")
	const queryTemplate = "select /*+ use_index_merge(t) */ %s from t where %s order by b;"
	projections := [][]string{{"a"}, {"b"}, {"c"}, {"a", "b"}, {"b", "c"}, {"c", "a"}, {"b", "a", "c"}}
	cases := []struct {
		condition string
		expected  []string
	}{
		{
			// 3 index scans
			"b < 10 or c < 11 or c > 50", []string{"1", "10", "100"},
		},
		{
			// 1 table scan + 1 index scan
			"a < 2 or c > 10000", []string{"1"},
		},
		{
			// 2 table scans + 1 index scan
			"a < 2 or a < 10 or b > 11", []string{"1", "100"},
		},
		{
			// 1 table scans + 3 index scans
			"a < 2 or b >= 10 or c > 100 or c < 1", []string{"1", "10", "100"},
		},
		{
			// 3 table scans + 2 index scans
			"a < 2 or a >= 10 or a >= 20 or c > 100 or b < 1", []string{"1", "10", "100"},
		},
	}
	for _, p := range projections {
		for _, ca := range cases {
			query := fmt.Sprintf(queryTemplate, strings.Join(p, ","), ca.condition)
			tk.HasPlan(query, "IndexMerge")
			expected := make([]string, 0, len(ca.expected))
			for _, datum := range ca.expected {
				row := strings.Repeat(datum+" ", len(p))
				expected = append(expected, row[:len(row)-1])
			}
			tk.MustQuery(query).Check(testkit.Rows(expected...))
		}
	}
}

func (s *testIntegrationSerialSuite) TestIssue23919(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")

	// Test for the minimal reproducible case.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, index(a), index(b)) partition by hash (a) partitions 2;")
	tk.MustExec("insert into t values (1, 5);")
	tk.MustQuery("select /*+ use_index_merge( t ) */ * from t where a in (3) or b in (5) order by a;").
		Check(testkit.Rows("1 5"))

	// Test for the original case.
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`CREATE TABLE t (
  col_5 text NOT NULL,
  col_6 tinyint(3) unsigned DEFAULT NULL,
  col_7 float DEFAULT '4779.165058537128',
  col_8 smallint(6) NOT NULL DEFAULT '-24790',
  col_9 date DEFAULT '2031-01-15',
  col_37 int(11) DEFAULT '1350204687',
  PRIMARY KEY (col_5(6),col_8) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY idx_6 (col_9,col_7,col_8),
  KEY idx_8 (col_8,col_6,col_5(6),col_9,col_7),
  KEY idx_9 (col_9,col_7,col_8)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY RANGE ( col_8 ) (
  PARTITION p0 VALUES LESS THAN (-17650),
  PARTITION p1 VALUES LESS THAN (-13033),
  PARTITION p2 VALUES LESS THAN (2521),
  PARTITION p3 VALUES LESS THAN (7510)
);`)
	tk.MustExec("insert into t values ('', NULL, 6304.0146, -24790, '2031-01-15', 1350204687);")
	tk.MustQuery("select  var_samp(col_7) aggCol from (select  /*+ use_index_merge( t ) */ * from t where " +
		"t.col_9 in ( '2002-06-22' ) or t.col_5 in ( 'PkfzI'  ) or t.col_8 in ( -24874 ) and t.col_6 > null and " +
		"t.col_5 > 'r' and t.col_9 in ( '1979-09-04' ) and t.col_7 < 8143.667552769195 or " +
		"t.col_5 in ( 'iZhfEjRWci' , 'T' , ''  ) or t.col_9 <> '1976-09-11' and t.col_7 = 8796.436181615773 and " +
		"t.col_8 = 7372 order by col_5,col_8  ) ordered_tbl group by col_6;").Check(testkit.Rows("<nil>"))
}

func (s *testIntegrationSerialSuite) TestIssue16407(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int,b char(100),key(a),key(b(10)))")
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where a=10 or b='x'").Check(testkit.Rows(
		"IndexMerge 0.04 root  ",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:b(b) range:[\"x\",\"x\"], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.04 cop[tikv]  or(eq(test.t.a, 10), eq(test.t.b, \"x\"))",
		"  └─TableRowIDScan 19.99 cop[tikv] table:t keep order:false, stats:pseudo"))
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
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
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

	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)

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
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Warn = s.testData.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
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
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testIntegrationSuite) TestClusterIndexUniqueDoubleRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database cluster_idx_unique_double_read;")
	tk.MustExec("use cluster_idx_unique_double_read;")
	defer tk.MustExec("drop database cluster_idx_unique_double_read;")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a varchar(64), b varchar(64), uk int, v int, primary key(a, b), unique key uuk(uk));")
	tk.MustExec("insert t values ('a', 'a1', 1, 11), ('b', 'b1', 2, 22), ('c', 'c1', 3, 33);")
	tk.MustQuery("select * from t use index (uuk);").Check(testkit.Rows("a a1 1 11", "b b1 2 22", "c c1 3 33"))
}

func (s *testIntegrationSuite) TestIndexJoinOnClusteredIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain  format = 'brief'" + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}
func (s *testIntegrationSerialSuite) TestIssue18984(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t2")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
	plan := tk.MustQuery("explain format = 'brief' select * from t where c = 1 and id = c")
	plan.Check(testkit.Rows(
		"TableReader 0.01 root partition:p0 data:Selection",
		"└─Selection 0.01 cop[tikv]  eq(test.t.c, 1), eq(test.t.id, 1)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustExec("drop table t")
}

func (s *testIntegrationSuite) TestIssue14481(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default null, b int default null, c int default null)")
	plan := tk.MustQuery("explain format = 'brief' select * from t where a = 1 and a = 2")
	plan.Check(testkit.Rows("TableDual 8000.00 root  rows:0"))
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
	tk.MustGetErrMsg("select v2, v1 from (select * from ttest) t1 join (select 1, 2) t2 group by v1",
		"[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t1.v2' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustGetErrMsg("select v2, v1 from (select t1.v1, t2.v2 from ttest t1 join ttest t2) t3 join (select 1, 2) t2 group by v1",
		"[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t3.v2' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

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

func (s *testIntegrationSuite) TestExtendedStatsSwitch(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null, key(a), key(b))")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6)")

	tk.MustExec("set session tidb_enable_extended_stats = off")
	tk.MustGetErrMsg("alter table t add stats_extended s1 correlation(a,b)",
		"Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	tk.MustGetErrMsg("alter table t drop stats_extended s1",
		"Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	tk.MustGetErrMsg("admin reload stats_extended",
		"Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")

	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"<nil> 0",
	))
	tk.MustExec("set session tidb_enable_extended_stats = off")
	// Analyze should not collect extended stats.
	tk.MustExec("analyze table t")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"<nil> 0",
	))
	tk.MustExec("set session tidb_enable_extended_stats = on")
	// Analyze would collect extended stats.
	tk.MustExec("analyze table t")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"1.000000 1",
	))
	// Estimated index scan count is 4 using extended stats.
	tk.MustQuery("explain format = 'brief' select * from t use index(b) where a > 3 order by b limit 1").Check(testkit.Rows(
		"Limit 1.00 root  offset:0, count:1",
		"└─Projection 1.00 root  test.t.a, test.t.b",
		"  └─IndexLookUp 1.00 root  ",
		"    ├─IndexFullScan(Build) 4.00 cop[tikv] table:t, index:b(b) keep order:true",
		"    └─Selection(Probe) 1.00 cop[tikv]  gt(test.t.a, 3)",
		"      └─TableRowIDScan 4.00 cop[tikv] table:t keep order:false",
	))
	tk.MustExec("set session tidb_enable_extended_stats = off")
	// Estimated index scan count is 2 using independent assumption.
	tk.MustQuery("explain format = 'brief' select * from t use index(b) where a > 3 order by b limit 1").Check(testkit.Rows(
		"Limit 1.00 root  offset:0, count:1",
		"└─Projection 1.00 root  test.t.a, test.t.b",
		"  └─IndexLookUp 1.00 root  ",
		"    ├─IndexFullScan(Build) 2.00 cop[tikv] table:t, index:b(b) keep order:true",
		"    └─Selection(Probe) 1.00 cop[tikv]  gt(test.t.a, 3)",
		"      └─TableRowIDScan 2.00 cop[tikv] table:t keep order:false",
	))
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

func (s *testIntegrationSuite) TestIssue22071(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values(1),(2),(5)")
	tk.MustQuery("select n in (1,2) from (select a in (1,2) as n from t) g;").Sort().Check(testkit.Rows("0", "1", "1"))
	tk.MustQuery("select n in (1,n) from (select a in (1,2) as n from t) g;").Check(testkit.Rows("1", "1", "1"))
}

func (s *testIntegrationSuite) TestIssue22199(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(i int primary key, j int, index idx_j(j))")
	tk.MustExec("create table t2(i int primary key, j int, index idx_j(j))")
	tk.MustGetErrMsg("select t1.*, (select t2.* from t1) from t1", "[planner:1051]Unknown table 't2'")
}

func (s *testIntegrationSuite) TestIssue22892(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int) partition by hash (a) partitions 5;")
	tk.MustExec("insert into t1 values (0);")
	tk.MustQuery("select * from t1 where a not between 1 and 2;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int) partition by hash (a) partitions 5;")
	tk.MustExec("insert into t2 values (0);")
	tk.MustQuery("select * from t2 where a not between 1 and 2;").Check(testkit.Rows("0"))
}

func (s *testIntegrationSerialSuite) TestPushDownProjectionForTiFlash(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, value decimal(6,3), name char(128))")
	tk.MustExec("analyze table t")
	tk.MustExec("set session tidb_allow_mpp=OFF")

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

	tk.MustExec("set @@tidb_opt_broadcast_join=1;")

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

func (s *testIntegrationSerialSuite) TestPushDownProjectionForMPP(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, value decimal(6,3), name char(128))")
	tk.MustExec("analyze table t")

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

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_opt_broadcast_join=0; set @@tidb_enforce_mpp=1;")

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

// Apply operator may got panic because empty Projection is eliminated.
func (s *testIntegrationSerialSuite) TestIssue23887(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values(1, 2), (3, 4);")
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

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index c2 (c2));")
	tk.MustQuery("select count(1) from (select count(1) from (select * from t1 where c3 = 100) k) k2;").Check(testkit.Rows("1"))
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

func (s *testIntegrationSuite) TestIndexMergeConstantTrue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int primary key, b int not null, key(b))")
	tk.MustExec("delete /*+ use_index_merge(t) */ FROM t WHERE a=1 OR (b < SOME (SELECT /*+ use_index_merge(t)*/ b FROM t WHERE a<2 OR b<2))")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null, key(a), key(b))")
	tk.MustExec("delete /*+ use_index_merge(t) */ FROM t WHERE a=1 OR (b < SOME (SELECT /*+ use_index_merge(t)*/ b FROM t WHERE a<2 OR b<2))")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int not null, c int, key(a), key(b,c))")
	tk.MustExec("delete /*+ use_index_merge(t) */ FROM t WHERE a=1 OR (a<2 and b<2)")
}

func (s *testIntegrationSerialSuite) TestPushDownAggForMPP(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, value decimal(6,3))")
	tk.MustExec("analyze table t")

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

	tk.MustExec(" set @@tidb_allow_mpp=1; set @@tidb_opt_broadcast_join=0; set @@tidb_broadcast_join_threshold_count = 1; set @@tidb_broadcast_join_threshold_size=1;")

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

func (s *testIntegrationSerialSuite) TestMppUnionAll(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t (a int not null, b int, c varchar(20))")
	tk.MustExec("create table t1 (a int, b int not null, c double)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" || tblInfo.Name.L == "t1" {
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

func (s *testIntegrationSerialSuite) TestMppJoinDecimal(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table t (c1 decimal(8, 5), c2 decimal(9, 5), c3 decimal(9, 4) NOT NULL, c4 decimal(8, 4) NOT NULL, c5 decimal(40, 20))")
	tk.MustExec("create table tt (pk int(11) NOT NULL AUTO_INCREMENT primary key,col_varchar_64 varchar(64),col_char_64_not_null char(64) NOT null, col_decimal_30_10_key decimal(30,10), col_tinyint tinyint, col_varchar_key varchar(1), key col_decimal_30_10_key (col_decimal_30_10_key), key col_varchar_key(col_varchar_key));")
	tk.MustExec("analyze table t")
	tk.MustExec("analyze table tt")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" || tblInfo.Name.L == "tt" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@tidb_allow_mpp=1;")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")

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

func (s *testIntegrationSerialSuite) TestMppAggTopNWithJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, value decimal(6,3))")
	tk.MustExec("analyze table t")

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

	tk.MustExec(" set @@tidb_allow_mpp=1;")

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

func (s *testIntegrationSuite) TestDecorrelateInnerJoinInSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null)")

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

func (s *testIntegrationSuite) TestIndexMergeTableFilter(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, c int, d int, key(a), key(b));")
	tk.MustExec("insert into t values(10,1,1,10)")

	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where a=10 or (b=10 and c=10)").Check(testkit.Rows(
		"IndexMerge 0.02 root  ",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:b(b) range:[10,10], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.02 cop[tikv]  or(eq(test.t.a, 10), and(eq(test.t.b, 10), eq(test.t.c, 10)))",
		"  └─TableRowIDScan 19.99 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where a=10 or (b=10 and c=10)").Check(testkit.Rows(
		"10 1 1 10",
	))
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where (a=10 and d=10) or (b=10 and c=10)").Check(testkit.Rows(
		"IndexMerge 0.00 root  ",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:b(b) range:[10,10], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.00 cop[tikv]  or(and(eq(test.t.a, 10), eq(test.t.d, 10)), and(eq(test.t.b, 10), eq(test.t.c, 10)))",
		"  └─TableRowIDScan 19.99 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where (a=10 and d=10) or (b=10 and c=10)").Check(testkit.Rows(
		"10 1 1 10",
	))
}

func (s *testIntegrationSuite) TestIndexMergeClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 float, c2 int, c3 int, primary key (c1) /*T![clustered_index] CLUSTERED */, key idx_1 (c2), key idx_2 (c3))")
	tk.MustExec("insert into t values(1.0,1,2),(2.0,2,1),(3.0,1,1),(4.0,2,2)")
	tk.MustQuery("select /*+ use_index_merge(t) */ c3 from t where c3 = 1 or c2 = 1").Sort().Check(testkit.Rows(
		"1",
		"1",
		"2",
	))
	tk.MustExec("drop table t")
	tk.MustExec("create table t (a int, b int, c int, primary key (a,b) /*T![clustered_index] CLUSTERED */, key idx_c(c))")
	tk.MustExec("insert into t values (0,1,2)")
	tk.MustQuery("select /*+ use_index_merge(t) */ c from t where c > 10 or a < 1").Check(testkit.Rows(
		"2",
	))
}

func (s *testIntegrationSuite) TestJoinSchemaChange(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int(11))")
	tk.MustExec("create table t2(a decimal(40,20) unsigned, b decimal(40,20))")
	tk.MustQuery("select count(*) as x from t1 group by a having x not in (select a from t2 where x = t2.b)").Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestIssue23736(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("create table t0(a int, b int, c int as (a + b) virtual, unique index (c) invisible);")
	tk.MustExec("create table t1(a int, b int, c int as (a + b) virtual);")
	tk.MustExec("insert into t0(a, b) values (12, -1), (8, 7);")
	tk.MustExec("insert into t1(a, b) values (12, -1), (8, 7);")
	tk.MustQuery("select /*+ stream_agg() */ count(1) from t0 where c > 10 and b < 2;").Check(testkit.Rows("1"))
	tk.MustQuery("select /*+ stream_agg() */ count(1) from t1 where c > 10 and b < 2;").Check(testkit.Rows("1"))
	tk.MustExec("delete from t0")
	tk.MustExec("insert into t0(a, b) values (5, 1);")
	tk.MustQuery("select /*+ nth_plan(3) */ count(1) from t0 where c > 10 and b < 2;").Check(testkit.Rows("0"))

	// Should not use invisible index
	c.Assert(tk.MustUseIndex("select /*+ stream_agg() */ count(1) from t0 where c > 10 and b < 2", "c"), IsFalse)
}

// https://github.com/pingcap/tidb/issues/23802
func (s *testIntegrationSuite) TestPanicWhileQueryTableWithIsNull(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists NT_HP27193")
	tk.MustExec("CREATE TABLE `NT_HP27193` (  `COL1` int(20) DEFAULT NULL,  `COL2` varchar(20) DEFAULT NULL,  `COL4` datetime DEFAULT NULL,  `COL3` bigint(20) DEFAULT NULL,  `COL5` float DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY HASH ( `COL1`%`COL3` ) PARTITIONS 10;")
	_, err := tk.Exec("select col1 from NT_HP27193 where col1 is null;")
	c.Assert(err, IsNil)
	tk.MustExec("INSERT INTO NT_HP27193 (COL2, COL4, COL3, COL5) VALUES ('m',  '2020-05-04 13:15:27', 8,  2602)")
	_, err = tk.Exec("select col1 from NT_HP27193 where col1 is null;")
	c.Assert(err, IsNil)
	tk.MustExec("drop table if exists NT_HP27193")
}

func (s *testIntegrationSuite) TestIssue23846(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varbinary(10),UNIQUE KEY(a))")
	tk.MustExec("insert into t values(0x00A4EEF4FA55D6706ED5)")
	tk.MustQuery("select count(*) from t where a=0x00A4EEF4FA55D6706ED5").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where a=0x00A4EEF4FA55D6706ED5").Check(testkit.Rows("\x00\xa4\xee\xf4\xfaU\xd6pn\xd5")) // not empty
}

func (s *testIntegrationSuite) TestConflictReadFromStorage(c *C) {
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
	tk.MustQuery(`explain select /*+ read_from_storage(tikv[t partition(p0)], tiflash[t partition(p1, p2)]) */ * from t`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Storage hints are conflict, you can only specify one storage type of table test.t"))
	tk.MustQuery(`explain select /*+ read_from_storage(tikv[t], tiflash[t]) */ * from t`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Storage hints are conflict, you can only specify one storage type of table test.t"))
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

func (s *testIntegrationSuite) TestIssue25799(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`create table t1 (a float default null, b smallint(6) DEFAULT NULL)`)
	tk.MustExec(`insert into t1 values (1, 1)`)
	tk.MustExec(`create table t2 (a float default null, b tinyint(4) DEFAULT NULL, key b (b))`)
	tk.MustExec(`insert into t2 values (null, 1)`)
	tk.HasPlan(`select /*+ TIDB_INLJ(t2@sel_2) */ t1.a, t1.b from t1 where t1.a not in (select t2.a from t2 where t1.b=t2.b)`, `IndexJoin`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t2@sel_2) */ t1.a, t1.b from t1 where t1.a not in (select t2.a from t2 where t1.b=t2.b)`).Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestLimitWindowColPrune(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustQuery("select count(a) f1, row_number() over (order by count(a)) as f2 from t limit 1").Check(testkit.Rows("1 1"))
}

func (s *testIntegrationSerialSuite) TestMergeContinuousSelections(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ts")
	tk.MustExec("create table ts (col_char_64 char(64), col_varchar_64_not_null varchar(64) not null, col_varchar_key varchar(1), id int primary key, col_varchar_64 varchar(64),col_char_64_not_null char(64) not null);")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "ts" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec(" set @@tidb_allow_mpp=1;")

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

func (s *testIntegrationSuite) TestIssue23839(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists BB")
	tk.MustExec("CREATE TABLE `BB` (\n" +
		"	`col_int` int(11) DEFAULT NULL,\n" +
		"	`col_varchar_10` varchar(10) DEFAULT NULL,\n" +
		"	`pk` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"	`col_int_not_null` int(11) NOT NULL,\n" +
		"	`col_decimal` decimal(10,0) DEFAULT NULL,\n" +
		"	`col_datetime` datetime DEFAULT NULL,\n" +
		"	`col_decimal_not_null` decimal(10,0) NOT NULL,\n" +
		"	`col_datetime_not_null` datetime NOT NULL,\n" +
		"	`col_varchar_10_not_null` varchar(10) NOT NULL,\n" +
		"	PRIMARY KEY (`pk`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=2000001")
	tk.Exec("explain SELECT OUTR . col2 AS X FROM (SELECT INNR . col1 as col1, SUM( INNR . col2 ) as col2 FROM (SELECT INNR . `col_int_not_null` + 1 as col1, INNR . `pk` as col2 FROM BB AS INNR) AS INNR GROUP BY col1) AS OUTR2 INNER JOIN (SELECT INNR . col1 as col1, MAX( INNR . col2 ) as col2 FROM (SELECT INNR . `col_int_not_null` + 1 as col1, INNR . `pk` as col2 FROM BB AS INNR) AS INNR GROUP BY col1) AS OUTR ON OUTR2.col1 = OUTR.col1 GROUP BY OUTR . col1, OUTR2 . col1 HAVING X <> 'b'")
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

func (s *testIntegrationSuite) TestIssue29834(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists IDT_MC21814;")
	tk.MustExec("CREATE TABLE `IDT_MC21814` (`COL1` year(4) DEFAULT NULL,`COL2` year(4) DEFAULT NULL,KEY `U_M_COL` (`COL1`,`COL2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into IDT_MC21814 values(1901, 2119), (2155, 2000);")
	tk.MustQuery("SELECT/*+ INL_JOIN(t1, t2), nth_plan(1) */ t2.* FROM IDT_MC21814 t1 LEFT JOIN IDT_MC21814 t2 ON t1.col1 = t2.col1 WHERE t2.col2 BETWEEN 2593 AND 1971 AND t1.col1 IN (2155, 1901, 1967);").Check(testkit.Rows())
	tk.MustQuery("SELECT/*+ INL_JOIN(t1, t2), nth_plan(2) */ t2.* FROM IDT_MC21814 t1 LEFT JOIN IDT_MC21814 t2 ON t1.col1 = t2.col1 WHERE t2.col2 BETWEEN 2593 AND 1971 AND t1.col1 IN (2155, 1901, 1967);").Check(testkit.Rows())
	// Only can generate one index join plan. Because the index join inner child can not be tableDual.
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The parameter of nth_plan() is out of range."))
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

func (s *testIntegrationSuite) TestIssue26559(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a timestamp, b datetime);")
	tk.MustExec("insert into t values('2020-07-29 09:07:01', '2020-07-27 16:57:36');")
	tk.MustQuery("select greatest(a, b) from t union select null;").Sort().Check(testkit.Rows("2020-07-29 09:07:01", "<nil>"))
}

func (s *testIntegrationSuite) TestGroupBySetVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (2), (3), (4), (5), (6);")
	rows := tk.MustQuery("select floor(dt.rn/2) rownum, count(c1) from (select @rownum := @rownum + 1 rn, c1 from (select @rownum := -1) drn, t1) dt group by floor(dt.rn/2) order by rownum;")
	rows.Check(testkit.Rows("0 2", "1 2", "2 2"))

	tk.MustExec("create table ta(a int, b int);")
	tk.MustExec("set sql_mode='';")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		res := tk.MustQuery("explain format = 'brief' " + tt)
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(res.Rows())
		})
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestIssue29705(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	origin := tk.MustQuery("SELECT @@session.tidb_partition_prune_mode")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@session.tidb_partition_prune_mode = '" + originStr + "'")
	}()
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(id int) partition by hash(id) partitions 4;")
	tk.MustExec("insert into t values(1);")
	result := tk.MustQuery("SELECT COUNT(1) FROM ( SELECT COUNT(1) FROM t b GROUP BY id) a;")
	result.Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestIssues29711(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tbl_29711")
	tk.MustExec("CREATE TABLE `tbl_29711` (" +
		"`col_250` text COLLATE utf8_unicode_ci NOT NULL," +
		"`col_251` varchar(10) COLLATE utf8_unicode_ci NOT NULL," +
		"PRIMARY KEY (`col_251`,`col_250`(1)) NONCLUSTERED);")
	tk.MustQuery("explain " +
		"select /*+ LIMIT_TO_COP() */ col_250,col_251 from tbl_29711 use index (primary) where col_251 between 'Bob' and 'David' order by col_250,col_251 limit 6;").
		Check(testkit.Rows(
			"TopN_9 6.00 root  test.tbl_29711.col_250, test.tbl_29711.col_251, offset:0, count:6",
			"└─IndexLookUp_16 6.00 root  ",
			"  ├─IndexRangeScan_13(Build) 250.00 cop[tikv] table:tbl_29711, index:PRIMARY(col_251, col_250) range:[\"Bob\",\"David\"], keep order:false, stats:pseudo",
			"  └─TopN_15(Probe) 6.00 cop[tikv]  test.tbl_29711.col_250, test.tbl_29711.col_251, offset:0, count:6",
			"    └─TableRowIDScan_14 250.00 cop[tikv] table:tbl_29711 keep order:false, stats:pseudo",
		))

	tk.MustExec("drop table if exists t29711")
	tk.MustExec("CREATE TABLE `t29711` (" +
		"`a` varchar(10) DEFAULT NULL," +
		"`b` int(11) DEFAULT NULL," +
		"`c` int(11) DEFAULT NULL," +
		"KEY `ia` (`a`(2)))")
	tk.MustQuery("explain select /*+ LIMIT_TO_COP() */ * from t29711 use index (ia) order by a limit 10;").
		Check(testkit.Rows(
			"TopN_8 10.00 root  test.t29711.a, offset:0, count:10",
			"└─IndexLookUp_15 10.00 root  ",
			"  ├─IndexFullScan_12(Build) 10000.00 cop[tikv] table:t29711, index:ia(a) keep order:false, stats:pseudo",
			"  └─TopN_14(Probe) 10.00 cop[tikv]  test.t29711.a, offset:0, count:10",
			"    └─TableRowIDScan_13 10000.00 cop[tikv] table:t29711 keep order:false, stats:pseudo",
		))

}

func (s *testIntegrationSuite) TestIssue27797(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	origin := tk.MustQuery("SELECT @@session.tidb_partition_prune_mode")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@session.tidb_partition_prune_mode = '" + originStr + "'")
	}()
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
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

// https://github.com/pingcap/tidb/issues/24095
func (s *testIntegrationSuite) TestIssue24095(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int, value decimal(10,5));")
	tk.MustExec("desc format = 'brief' select count(*) from t join (select t.id, t.value v1 from t join t t1 on t.id = t1.id order by t.value limit 1) v on v.id = t.id and v.v1 = t.value;")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}
<<<<<<< HEAD
=======

func TestIssue27083(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx_b(b))")
	tk.MustExec("insert into t values (1,2,3), (4,5,6), (7,8,9), (10, 11, 12), (13,14,15), (16, 17, 18)")
	do, _ := session.GetDomain(store)
	require.Nil(t, do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssues27130(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1( a enum('y','b','Abc','null'),b enum('y','b','Abc','null'),key(a));")
	tk.MustQuery(`explain format=brief select * from t1 where a like "A%"`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  like(test.t1.a, \"A%\", 92)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
	tk.MustQuery(`explain format=brief select * from t1 where b like "A%"`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  like(test.t1.b, \"A%\", 92)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2( a enum('y','b','Abc','null'),b enum('y','b','Abc','null'),key(a, b));")
	tk.MustQuery(`explain format=brief select * from t2 where a like "A%"`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  like(test.t2.a, \"A%\", 92)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
	))
	tk.MustQuery(`explain format=brief select * from t2 where a like "A%" and b like "A%"`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  like(test.t2.a, \"A%\", 92), like(test.t2.b, \"A%\", 92)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
	))

	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3( a int,b enum('y','b','Abc','null'), c enum('y','b','Abc','null'),key(a, b, c));")
	tk.MustQuery(`explain format=brief select * from t3 where a = 1 and b like "A%"`).Check(testkit.Rows(
		"IndexReader 8.00 root  index:Selection",
		"└─Selection 8.00 cop[tikv]  like(test.t3.b, \"A%\", 92)",
		"  └─IndexRangeScan 10.00 cop[tikv] table:t3, index:a(a, b, c) range:[1,1], keep order:false, stats:pseudo",
	))
}

func TestIssue27242(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists UK_MU16407")
	tk.MustExec("CREATE TABLE UK_MU16407 (COL3 timestamp NULL DEFAULT NULL, UNIQUE KEY U3(COL3));")
	defer tk.MustExec("DROP TABLE UK_MU16407")
	tk.MustExec(`insert into UK_MU16407 values("1985-08-31 18:03:27");`)
	tk.MustExec(`SELECT COL3 FROM UK_MU16407 WHERE COL3>_utf8mb4'2039-1-19 3:14:40';`)
}

func verifyTimestampOutOfRange(tk *testkit.TestKit) {
	tk.MustQuery(`select * from t28424 where t != "2038-1-19 3:14:08"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t < "2038-1-19 3:14:08"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t <= "2038-1-19 3:14:08"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t >= "2038-1-19 3:14:08"`).Check(testkit.Rows())
	tk.MustQuery(`select * from t28424 where t > "2038-1-19 3:14:08"`).Check(testkit.Rows())
	tk.MustQuery(`select * from t28424 where t != "1970-1-1 0:0:0"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t < "1970-1-1 0:0:0"`).Check(testkit.Rows())
	tk.MustQuery(`select * from t28424 where t <= "1970-1-1 0:0:0"`).Check(testkit.Rows())
	tk.MustQuery(`select * from t28424 where t >= "1970-1-1 0:0:0"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t > "1970-1-1 0:0:0"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
}

func TestIssue28424(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t28424, dt28242")

	tk.MustExec(`set time_zone='+00:00'`)
	tk.MustExec(`drop table if exists t28424,dt28424`)
	tk.MustExec(`create table t28424 (t timestamp)`)
	defer tk.MustExec("DROP TABLE t28424")
	tk.MustExec(`insert into t28424 values ("2038-01-19 03:14:07"), ("1970-01-01 00:00:01")`)

	verifyTimestampOutOfRange(tk)
	tk.MustExec(`alter table t28424 add unique index (t)`)
	verifyTimestampOutOfRange(tk)
	tk.MustExec(`create table dt28424 (dt datetime)`)
	defer tk.MustExec("DROP TABLE dt28424")
	tk.MustExec(`insert into dt28424 values ("2038-01-19 03:14:07"), ("1970-01-01 00:00:01")`)
	tk.MustExec(`insert into dt28424 values ("1969-12-31 23:59:59"), ("1970-01-01 00:00:00"), ("2038-03-19 03:14:08")`)
	tk.MustQuery(`select * from t28424 right join dt28424 on t28424.t = dt28424.dt`).Sort().Check(testkit.Rows(
		"1970-01-01 00:00:01 1970-01-01 00:00:01]\n" +
			"[2038-01-19 03:14:07 2038-01-19 03:14:07]\n" +
			"[<nil> 1969-12-31 23:59:59]\n" +
			"[<nil> 1970-01-01 00:00:00]\n" +
			"[<nil> 2038-03-19 03:14:08"))
}

func TestTemporaryTableForCte(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create temporary table tmp1(a int, b int, c int);")
	tk.MustExec("insert into tmp1 values (1,1,1),(2,2,2),(3,3,3),(4,4,4);")
	rows := tk.MustQuery("with cte1 as (with cte2 as (select * from tmp1) select * from cte2) select * from cte1 left join tmp1 on cte1.c=tmp1.c;")
	rows.Check(testkit.Rows("1 1 1 1 1 1", "2 2 2 2 2 2", "3 3 3 3 3 3", "4 4 4 4 4 4"))
	rows = tk.MustQuery("with cte1 as (with cte2 as (select * from tmp1) select * from cte2) select * from cte1 t1 left join cte1 t2 on t1.c=t2.c;")
	rows.Check(testkit.Rows("1 1 1 1 1 1", "2 2 2 2 2 2", "3 3 3 3 3 3", "4 4 4 4 4 4"))
	rows = tk.MustQuery("WITH RECURSIVE cte(a) AS (SELECT 1 UNION SELECT a+1 FROM tmp1 WHERE a < 5) SELECT * FROM cte order by a;")
	rows.Check(testkit.Rows("1", "2", "3", "4", "5"))
}

func TestGroupBySetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (2), (3), (4), (5), (6);")
	rows := tk.MustQuery("select floor(dt.rn/2) rownum, count(c1) from (select @rownum := @rownum + 1 rn, c1 from (select @rownum := -1) drn, t1) dt group by floor(dt.rn/2) order by rownum;")
	rows.Check(testkit.Rows("0 2", "1 2", "2 2"))

	tk.MustExec("create table ta(a int, b int);")
	tk.MustExec("set sql_mode='';")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		res := tk.MustQuery("explain format = 'brief' " + tt)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(res.Rows())
		})
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestPushDownGroupConcatToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ts")
	tk.MustExec("create table ts (col_0 char(64), col_1 varchar(64) not null, col_2 varchar(1), id int primary key);")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "ts" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@tidb_isolation_read_engines='tiflash,tidb'; set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")

	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))

		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		testdata.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = make([]string, len(warnings))
				for j, warning := range warnings {
					output[i].Warning[j] = warning.Err.Error()
				}
			}
		})
		if len(output[i].Warning) == 0 {
			require.Len(t, warnings, 0, comment)
		} else {
			require.Len(t, warnings, len(output[i].Warning), comment)
			for j, warning := range warnings {
				require.Equal(t, stmtctx.WarnLevelWarning, warning.Level, comment)
				require.EqualError(t, warning.Err, output[i].Warning[j], comment)
			}
		}
	}
}

func TestIssue27797(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	origin := tk.MustQuery("SELECT @@session.tidb_partition_prune_mode")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@session.tidb_partition_prune_mode = '" + originStr + "'")
	}()
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
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

func TestIssue27949(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t27949")
	tk.MustExec("create table t27949 (a int, b int, key(b))")
	tk.MustQuery("explain format = 'brief' select * from t27949 where b=1").Check(testkit.Rows("IndexLookUp 10.00 root  ",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t27949, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 10.00 cop[tikv] table:t27949 keep order:false, stats:pseudo"))
	tk.MustExec("create global binding for select * from t27949 where b=1 using select * from t27949 ignore index(b) where b=1")
	tk.MustQuery("explain format = 'brief' select * from t27949 where b=1").Check(testkit.Rows("TableReader 10.00 root  data:Selection",
		"└─Selection 10.00 cop[tikv]  eq(test.t27949.b, 1)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t27949 keep order:false, stats:pseudo"))
	tk.MustExec("set @@sql_select_limit=100")
	tk.MustQuery("explain format = 'brief' select * from t27949 where b=1").Check(testkit.Rows("Limit 10.00 root  offset:0, count:100",
		"└─TableReader 10.00 root  data:Limit",
		"  └─Limit 10.00 cop[tikv]  offset:0, count:100",
		"    └─Selection 10.00 cop[tikv]  eq(test.t27949.b, 1)",
		"      └─TableFullScan 10000.00 cop[tikv] table:t27949 keep order:false, stats:pseudo"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a));")
	tk.MustExec("create binding for select * from t  using select * from t use index(idx_a);")
	tk.MustExec("select * from t;")
	tk.MustQuery("select @@last_plan_from_binding;").Check(testkit.Rows("1"))
	tk.MustExec("prepare stmt from 'select * from t';")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_binding;").Check(testkit.Rows("1"))
}

func TestIssue28154(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer func() {
		tk.MustExec("drop table if exists t")
	}()
	tk.MustExec("create table t(a TEXT)")
	tk.MustExec("insert into t values('abc')")
	result := tk.MustQuery("select * from t where from_base64('')")
	result.Check(testkit.Rows())
	_, err := tk.Exec("update t set a = 'def' where from_base64('')")
	require.EqualError(t, err, "[types:1292]Truncated incorrect DOUBLE value: ''")
	result = tk.MustQuery("select * from t where from_base64('invalidbase64')")
	result.Check(testkit.Rows())
	tk.MustExec("update t set a = 'hig' where from_base64('invalidbase64')")
	result = tk.MustQuery("select * from t where from_base64('test')")
	result.Check(testkit.Rows())
	_, err = tk.Exec("update t set a = 'xyz' where from_base64('test')")
	require.Error(t, err)
	require.Regexp(t, "\\[types:1292\\]Truncated incorrect DOUBLE value.*", err.Error())
	result = tk.MustQuery("select * from t")
	result.Check(testkit.Rows("abc"))
}

func TestRejectSortForMPP(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, value decimal(6,3), name char(128))")
	tk.MustExec("analyze table t")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestRegardNULLAsPoint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tpk")
	tk.MustExec(`create table tuk (a int, b int, c int, unique key (a, b, c))`)
	tk.MustExec(`create table tik (a int, b int, c int, key (a, b, c))`)
	for _, va := range []string{"NULL", "1"} {
		for _, vb := range []string{"NULL", "1"} {
			for _, vc := range []string{"NULL", "1"} {
				tk.MustExec(fmt.Sprintf(`insert into tuk values (%v, %v, %v)`, va, vb, vc))
				tk.MustExec(fmt.Sprintf(`insert into tik values (%v, %v, %v)`, va, vb, vc))
				if va == "1" && vb == "1" && vc == "1" {
					continue
				}
				// duplicated NULL rows
				tk.MustExec(fmt.Sprintf(`insert into tuk values (%v, %v, %v)`, va, vb, vc))
				tk.MustExec(fmt.Sprintf(`insert into tik values (%v, %v, %v)`, va, vb, vc))
			}
		}
	}

	var input []string
	var output []struct {
		SQL          string
		PlanEnabled  []string
		PlanDisabled []string
		Result       []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			tk.MustExec(`set @@session.tidb_regard_null_as_point=true`)
			output[i].PlanEnabled = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())

			tk.MustExec(`set @@session.tidb_regard_null_as_point=false`)
			output[i].PlanDisabled = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
		})
		tk.MustExec(`set @@session.tidb_regard_null_as_point=true`)
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].PlanEnabled...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))

		tk.MustExec(`set @@session.tidb_regard_null_as_point=false`)
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].PlanDisabled...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIssues29711(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tbl_29711")
	tk.MustExec("CREATE TABLE `tbl_29711` (" +
		"`col_250` text COLLATE utf8_unicode_ci NOT NULL," +
		"`col_251` enum('Alice','Bob','Charlie','David') COLLATE utf8_unicode_ci NOT NULL DEFAULT 'Charlie'," +
		"PRIMARY KEY (`col_251`,`col_250`(1)) NONCLUSTERED);")
	tk.MustQuery("explain format=brief " +
		"select col_250,col_251 from tbl_29711 where col_251 between 'Bob' and 'David' order by col_250,col_251 limit 6;").
		Check(testkit.Rows(
			"TopN 6.00 root  test.tbl_29711.col_250, test.tbl_29711.col_251, offset:0, count:6",
			"└─IndexLookUp 6.00 root  ",
			"  ├─IndexRangeScan(Build) 30.00 cop[tikv] table:tbl_29711, index:PRIMARY(col_251, col_250) range:[\"Bob\",\"Bob\"], [\"Charlie\",\"Charlie\"], [\"David\",\"David\"], keep order:false, stats:pseudo",
			"  └─TopN(Probe) 6.00 cop[tikv]  test.tbl_29711.col_250, test.tbl_29711.col_251, offset:0, count:6",
			"    └─TableRowIDScan 30.00 cop[tikv] table:tbl_29711 keep order:false, stats:pseudo",
		))

	tk.MustExec("drop table if exists t29711")
	tk.MustExec("CREATE TABLE `t29711` (" +
		"`a` varchar(10) DEFAULT NULL," +
		"`b` int(11) DEFAULT NULL," +
		"`c` int(11) DEFAULT NULL," +
		"KEY `ia` (`a`(2)))")
	tk.MustQuery("explain format=brief select * from t29711 use index (ia) order by a limit 10;").
		Check(testkit.Rows(
			"TopN 10.00 root  test.t29711.a, offset:0, count:10",
			"└─IndexLookUp 10.00 root  ",
			"  ├─IndexFullScan(Build) 10000.00 cop[tikv] table:t29711, index:ia(a) keep order:false, stats:pseudo",
			"  └─TopN(Probe) 10.00 cop[tikv]  test.t29711.a, offset:0, count:10",
			"    └─TableRowIDScan 10000.00 cop[tikv] table:t29711 keep order:false, stats:pseudo",
		))
}

func TestIssue27313(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(100), b int, c int, index idx1(a(2), b), index idx2(a))")
	tk.MustExec("explain format = 'verbose' select * from t where a = 'abcdefghijk' and b > 4")
	// no warning indicates that idx2 is not pruned by idx1.
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestIssue30094(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t30094;`)
	tk.MustExec(`create table t30094(a varchar(10));`)
	tk.MustQuery(`explain format = 'brief' select * from t30094 where cast(a as float) and cast(a as char);`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  cast(test.t30094.a, float BINARY), cast(test.t30094.a, var_string(5))",
		"  └─TableFullScan 10000.00 cop[tikv] table:t30094 keep order:false, stats:pseudo",
	))
	tk.MustQuery(`explain format = 'brief' select * from t30094 where  concat(a,'1') = _binary 0xe59388e59388e59388 collate binary and concat(a,'1') = _binary 0xe598bfe598bfe598bf collate binary;`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  eq(concat(test.t30094.a, \"1\"), \"0xe59388e59388e59388\"), eq(concat(test.t30094.a, \"1\"), \"0xe598bfe598bfe598bf\")",
		"  └─TableFullScan 10000.00 cop[tikv] table:t30094 keep order:false, stats:pseudo",
	))
}

func TestIssue30200(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 varchar(100), c2 varchar(100), key(c1), key(c2), c3 varchar(100));")
	tk.MustExec("insert into t1 values('ab', '10', '10');")

	tk.MustExec("drop table if exists tt1;")
	tk.MustExec("create table tt1(c1 varchar(100), c2 varchar(100), c3 varchar(100), c4 varchar(100), key idx_0(c1), key idx_1(c2, c3));")
	tk.MustExec("insert into tt1 values('ab', '10', '10', '10');")

	tk.MustExec("drop table if exists tt2;")
	tk.MustExec("create table tt2 (c1 int , pk int, primary key( pk ) , unique key( c1));")
	tk.MustExec("insert into tt2 values(-3896405, -1), (-2, 1), (-1, -2);")

	tk.MustExec("drop table if exists tt3;")
	tk.MustExec("create table tt3(c1 int, c2 int, c3 int as (c1 + c2), key(c1), key(c2), key(c3));")
	tk.MustExec("insert into tt3(c1, c2) values(1, 1);")

	oriIndexMergeSwitcher := tk.MustQuery("select @@tidb_enable_index_merge;").Rows()[0][0].(string)
	tk.MustExec("set tidb_enable_index_merge = on;")
	defer func() {
		tk.MustExec(fmt.Sprintf("set tidb_enable_index_merge = %s;", oriIndexMergeSwitcher))
	}()

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format=brief " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain format=brief " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func TestIssue29705(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	origin := tk.MustQuery("SELECT @@session.tidb_partition_prune_mode")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@session.tidb_partition_prune_mode = '" + originStr + "'")
	}()
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(id int) partition by hash(id) partitions 4;")
	tk.MustExec("insert into t values(1);")
	result := tk.MustQuery("SELECT COUNT(1) FROM ( SELECT COUNT(1) FROM t b GROUP BY id) a;")
	result.Check(testkit.Rows("1"))
}

func TestIssue30271(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b char(10), c char(10), index (a, b, c)) collate utf8mb4_bin;")
	tk.MustExec("insert into t values ('b', 'a', '1'), ('b', 'A', '2'), ('c', 'a', '3');")
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci;")
	tk.MustQuery("select * from t where (a>'a' and b='a') or (b = 'A' and a < 'd') order by a,c;").Check(testkit.Rows("b a 1", "b A 2", "c a 3"))
}

func TestIssue30804(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	// minimal reproduction of https://github.com/pingcap/tidb/issues/30804
	tk.MustExec("select avg(0) over w from t1 window w as (order by (select 1))")
	// named window cannot be used in subquery
	err := tk.ExecToErr("select avg(0) over w from t1 where b > (select sum(t2.a) over w from t2) window w as (partition by t1.b)")
	require.True(t, core.ErrWindowNoSuchWindow.Equal(err))
	tk.MustExec("select avg(0) over w1 from t1 where b > (select sum(t2.a) over w2 from t2 window w2 as (partition by t2.b)) window w1 as (partition by t1.b)")
}

func TestIndexMergeWarning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("select /*+ use_index_merge(t1) */ * from t1 where c1 < 1 or c2 < 1")
	warningMsg := "Warning 1105 IndexMerge is inapplicable or disabled. No available filter or available index."
	tk.MustQuery("show warnings").Check(testkit.Rows(warningMsg))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 int, key(c1), key(c2))")
	tk.MustExec("select /*+ use_index_merge(t1), no_index_merge() */ * from t1 where c1 < 1 or c2 < 1")
	warningMsg = "Warning 1105 IndexMerge is inapplicable or disabled. Got no_index_merge hint or tidb_enable_index_merge is off."
	tk.MustQuery("show warnings").Check(testkit.Rows(warningMsg))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create temporary table t1(c1 int, c2 int, key(c1), key(c2))")
	tk.MustExec("select /*+ use_index_merge(t1) */ * from t1 where c1 < 1 or c2 < 1")
	warningMsg = "Warning 1105 IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table."
	tk.MustQuery("show warnings").Check(testkit.Rows(warningMsg))
}

func TestIndexMergeWithCorrelatedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(c1 int, c2 int, c3 int, primary key(c1), key(c2));")
	tk.MustExec("insert into t1 values(1, 1, 1);")
	tk.MustExec("insert into t1 values(2, 2, 2);")
	tk.MustExec("create table t2(c1 int, c2 int, c3 int);")
	tk.MustExec("insert into t2 values(1, 1, 1);")
	tk.MustExec("insert into t2 values(2, 2, 2);")

	tk.MustExec("drop table if exists tt1, tt2;")
	tk.MustExec("create table tt1  (c_int int, c_str varchar(40), c_datetime datetime, c_decimal decimal(12, 6), primary key(c_int), key(c_int), key(c_str), unique key(c_decimal), key(c_datetime));")
	tk.MustExec("create table tt2  like tt1 ;")
	tk.MustExec(`insert into tt1 (c_int, c_str, c_datetime, c_decimal) values (6, 'sharp payne', '2020-06-07 10:40:39', 6.117000) ,
			    (7, 'objective kare', '2020-02-05 18:47:26', 1.053000) ,
			    (8, 'thirsty pasteur', '2020-01-02 13:06:56', 2.506000) ,
			    (9, 'blissful wilbur', '2020-06-04 11:34:04', 9.144000) ,
			    (10, 'reverent mclean', '2020-02-12 07:36:26', 7.751000) ;`)
	tk.MustExec(`insert into tt2 (c_int, c_str, c_datetime, c_decimal) values (6, 'beautiful joliot', '2020-01-16 01:44:37', 5.627000) ,
			    (7, 'hopeful blackburn', '2020-05-23 21:44:20', 7.890000) ,
			    (8, 'ecstatic davinci', '2020-02-01 12:27:17', 5.648000) ,
			    (9, 'hopeful lewin', '2020-05-05 05:58:25', 7.288000) ,
			    (10, 'sharp jennings', '2020-01-28 04:35:03', 9.758000) ;`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format=brief " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain format=brief " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func TestIssue20510(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("CREATE TABLE t1 (a int PRIMARY KEY, b int)")
	tk.MustExec("CREATE TABLE t2 (a int PRIMARY KEY, b int)")
	tk.MustExec("INSERT INTO t1 VALUES (1,1), (2,1), (3,1), (4,2)")
	tk.MustExec("INSERT INTO t2 VALUES (1,2), (2,2)")

	tk.MustQuery("explain format=brief SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.a WHERE not(0+(t1.a=30 and t2.b=1));").Check(testkit.Rows(
		"Selection 8000.00 root  not(plus(0, and(eq(test.t1.a, 30), eq(test.t2.b, 1))))",
		"└─MergeJoin 10000.00 root  left outer join, left key:test.t1.a, right key:test.t2.a",
		"  ├─TableReader(Build) 8000.00 root  data:Selection",
		"  │ └─Selection 8000.00 cop[tikv]  not(istrue_with_null(plus(0, and(eq(test.t2.a, 30), eq(test.t2.b, 1)))))",
		"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:true, stats:pseudo",
		"  └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"    └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:true, stats:pseudo"))
	tk.MustQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.a WHERE not(0+(t1.a=30 and t2.b=1));").Check(testkit.Rows(
		"1 1 1 2",
		"2 1 2 2",
		"3 1 <nil> <nil>",
		"4 2 <nil> <nil>",
	))
}

func TestIssue31035(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 longtext, c2 decimal(37, 4), unique key(c1(10)), unique key(c2));")
	tk.MustExec("insert into t1 values('眐', -962541614831459.7458);")
	tk.MustQuery("select * from t1 order by c2 + 10;").Check(testkit.Rows("眐 -962541614831459.7458"))
}

// TestDNFCondSelectivityWithConst test selectivity calculation with DNF conditions with one is const.
// Close https://github.com/pingcap/tidb/issues/31096
func TestDNFCondSelectivityWithConst(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t1")
	testKit.MustExec("create table t1(a int, b int, c int);")
	testKit.MustExec("insert into t1 value(10,10,10)")
	for i := 0; i < 7; i++ {
		testKit.MustExec("insert into t1 select * from t1")
	}
	testKit.MustExec("insert into t1 value(1,1,1)")
	testKit.MustExec("analyze table t1")

	testKit.MustQuery("explain format = 'brief' select * from t1 where a=1 or b=1;").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(eq(test.t1.a, 1), eq(test.t1.b, 1))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where 0=1 or a=1 or b=1;").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(0, or(eq(test.t1.a, 1), eq(test.t1.b, 1)))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where null or a=1 or b=1;").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(0, or(eq(test.t1.a, 1), eq(test.t1.b, 1)))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a=1 or false or b=1;").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(eq(test.t1.a, 1), or(0, eq(test.t1.b, 1)))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a=1 or b=1 or \"false\";").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(eq(test.t1.a, 1), or(eq(test.t1.b, 1), 0))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where 1=1 or a=1 or b=1;").Check(testkit.Rows(
		"TableReader 129.00 root  data:Selection",
		"└─Selection 129.00 cop[tikv]  or(1, or(eq(test.t1.a, 1), eq(test.t1.b, 1)))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a=1 or b=1 or 1=1;").Check(testkit.Rows(
		"TableReader 129.00 root  data:Selection",
		"└─Selection 129.00 cop[tikv]  or(eq(test.t1.a, 1), or(eq(test.t1.b, 1), 1))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustExec("drop table if exists t1")
}

func TestIssue31202(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t31202(a int primary key, b int);")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t31202", L: "t31202"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	tk.MustQuery("explain format = 'brief' select * from t31202;").Check(testkit.Rows(
		"TableReader 10000.00 root  data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─TableFullScan 10000.00 mpp[tiflash] table:t31202 keep order:false, stats:pseudo"))

	tk.MustQuery("explain format = 'brief' select * from t31202 use index (primary);").Check(testkit.Rows(
		"TableReader 10000.00 root  data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:t31202 keep order:false, stats:pseudo"))
	tk.MustExec("drop table if exists t31202")
}

func TestNaturalJoinUpdateSameTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database natural_join_update")
	defer tk.MustExec("drop database natural_join_update")
	tk.MustExec("use natural_join_update")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustExec("update t1 as a natural join t1 b SET a.a = 2, b.b = 3")
	tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("2 3", "2 3"))
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int primary key, b int)")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 b SET a.a = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int, b int) partition by hash (a) partitions 3")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 b SET a.a = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (A int, b int) partition by hash (b) partitions 3")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 B SET a.A = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	_, err := tk.Exec(`update t1 as a natural join t1 B SET a.A = 2, b.b = 3`)
	require.Error(t, err)
	require.Regexp(t, ".planner:1706.Primary key/partition key update is not allowed since the table is updated both as 'a' and 'B'.", err.Error())
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (A int, b int) partition by RANGE COLUMNS (b) (partition `pNeg` values less than (0),partition `pPos` values less than MAXVALUE)")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 B SET a.A = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
}

func TestAggPushToCopForCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t32157(
  process_code varchar(8) NOT NULL,
  ctrl_class varchar(2) NOT NULL,
  ctrl_type varchar(1) NOT NULL,
  oper_no varchar(12) DEFAULT NULL,
  modify_date datetime DEFAULT NULL,
  d_c_flag varchar(2) NOT NULL,
  PRIMARY KEY (process_code,ctrl_class,d_c_flag));`)
	tk.MustExec("insert into t32157 values ('GDEP0071', '05', '1', '10000', '2016-06-29 00:00:00', 'C')")
	tk.MustExec("insert into t32157 values ('GDEP0071', '05', '0', '0000', '2016-06-01 00:00:00', 'D')")
	tk.MustExec("alter table t32157 cache")

	tk.MustQuery("explain format = 'brief' select /*+AGG_TO_COP()*/ count(*) from t32157 ignore index(primary) where process_code = 'GDEP0071'").Check(testkit.Rows(
		"StreamAgg 1.00 root  funcs:count(1)->Column#8]\n" +
			"[└─UnionScan 10.00 root  eq(test.t32157.process_code, \"GDEP0071\")]\n" +
			"[  └─TableReader 10.00 root  data:Selection]\n" +
			"[    └─Selection 10.00 cop[tikv]  eq(test.t32157.process_code, \"GDEP0071\")]\n" +
			"[      └─TableFullScan 10000.00 cop[tikv] table:t32157 keep order:false, stats:pseudo"))

	var readFromCacheNoPanic bool
	for i := 0; i < 10; i++ {
		tk.MustQuery("select /*+AGG_TO_COP()*/ count(*) from t32157 ignore index(primary) where process_code = 'GDEP0071'").Check(testkit.Rows("2"))
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			readFromCacheNoPanic = true
			break
		}
	}
	require.True(t, readFromCacheNoPanic)

	tk.MustExec("drop table if exists t31202")
}

func TestIssue31240(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t31240(a int, b int);")
	tk.MustExec("set @@tidb_allow_mpp = 0")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t31240", L: "t31240"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table if exists t31240")
}

func TestIssue32632(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `partsupp` (" +
		" `PS_PARTKEY` bigint(20) NOT NULL," +
		"`PS_SUPPKEY` bigint(20) NOT NULL," +
		"`PS_AVAILQTY` bigint(20) NOT NULL," +
		"`PS_SUPPLYCOST` decimal(15,2) NOT NULL," +
		"`PS_COMMENT` varchar(199) NOT NULL," +
		"PRIMARY KEY (`PS_PARTKEY`,`PS_SUPPKEY`) /*T![clustered_index] NONCLUSTERED */)")
	tk.MustExec("CREATE TABLE `supplier` (" +
		"`S_SUPPKEY` bigint(20) NOT NULL," +
		"`S_NAME` char(25) NOT NULL," +
		"`S_ADDRESS` varchar(40) NOT NULL," +
		"`S_NATIONKEY` bigint(20) NOT NULL," +
		"`S_PHONE` char(15) NOT NULL," +
		"`S_ACCTBAL` decimal(15,2) NOT NULL," +
		"`S_COMMENT` varchar(101) NOT NULL," +
		"PRIMARY KEY (`S_SUPPKEY`) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("analyze table partsupp;")
	tk.MustExec("analyze table supplier;")
	tk.MustExec("set @@tidb_enforce_mpp = 1")

	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "partsupp", L: "partsupp"})
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "supplier", L: "supplier"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	h := dom.StatsHandle()
	statsTbl1 := h.GetTableStats(tbl1.Meta())
	statsTbl1.Count = 800000
	statsTbl2 := h.GetTableStats(tbl2.Meta())
	statsTbl2.Count = 10000
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table if exists partsupp")
	tk.MustExec("drop table if exists supplier")
}

func TestTiFlashPartitionTableScan(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enforce_mpp = on")
	tk.MustExec("set @@tidb_allow_batch_cop = 2")
	tk.MustExec("drop table if exists rp_t;")
	tk.MustExec("drop table if exists hp_t;")
	tk.MustExec("create table rp_t(a int) partition by RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), PARTITION p3 VALUES LESS THAN (21));")
	tk.MustExec("create table hp_t(a int) partition by hash(a) partitions 4;")
	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "rp_t", L: "rp_t"})
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "hp_t", L: "hp_t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table rp_t;")
	tk.MustExec("drop table hp_t;")
}

func TestTiFlashFineGrainedShuffle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enforce_mpp = on")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int)")

	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t1", L: "t1"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := core.GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestTiFlashFineGrainedShuffleWithMaxTiFlashThreads(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enforce_mpp = on")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t1", L: "t1"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	sql := "explain select row_number() over w1 from t1 window w1 as (partition by c1);"

	getStreamCountFromExplain := func(rows [][]interface{}) (res []uint64) {
		re := regexp.MustCompile("stream_count: ([0-9]+)")
		for _, row := range rows {
			buf := bytes.NewBufferString("")
			_, _ = fmt.Fprintf(buf, "%s\n", row)
			if matched := re.FindStringSubmatch(buf.String()); matched != nil {
				require.Equal(t, len(matched), 2)
				c, err := strconv.ParseUint(matched[1], 10, 64)
				require.NoError(t, err)
				res = append(res, c)
			}
		}
		return res
	}

	// tiflash_fine_grained_shuffle_stream_count should be same with tidb_max_tiflash_threads.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = 0")
	tk.MustExec("set @@tidb_max_tiflash_threads = 10")
	rows := tk.MustQuery(sql).Rows()
	streamCount := getStreamCountFromExplain(rows)
	// require.Equal(t, len(streamCount), 1)
	require.Equal(t, uint64(10), streamCount[0])

	// tiflash_fine_grained_shuffle_stream_count should be default value when tidb_max_tiflash_threads is -1.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = 0")
	tk.MustExec("set @@tidb_max_tiflash_threads = -1")
	rows = tk.MustQuery(sql).Rows()
	streamCount = getStreamCountFromExplain(rows)
	// require.Equal(t, len(streamCount), 1)
	require.Equal(t, uint64(variable.DefStreamCountWhenMaxThreadsNotSet), streamCount[0])

	// tiflash_fine_grained_shuffle_stream_count should be default value when tidb_max_tiflash_threads is 0.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = 0")
	tk.MustExec("set @@tidb_max_tiflash_threads = 0")
	rows = tk.MustQuery(sql).Rows()
	streamCount = getStreamCountFromExplain(rows)
	// require.Equal(t, len(streamCount), 1)
	require.Equal(t, uint64(variable.DefStreamCountWhenMaxThreadsNotSet), streamCount[0])

	// Disabled when tiflash_fine_grained_shuffle_stream_count is -1.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = -1")
	tk.MustExec("set @@tidb_max_tiflash_threads = 10")
	rows = tk.MustQuery(sql).Rows()
	streamCount = getStreamCountFromExplain(rows)
	require.Equal(t, len(streamCount), 0)

	// Test when tiflash_fine_grained_shuffle_stream_count is greater than 0.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = 16")
	tk.MustExec("set @@tidb_max_tiflash_threads = 10")
	rows = tk.MustQuery(sql).Rows()
	streamCount = getStreamCountFromExplain(rows)
	// require.Equal(t, len(streamCount), 1)
	require.Equal(t, uint64(16), streamCount[0])
}

func TestIssue33175(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (id bigint(45) unsigned not null, c varchar(20), primary key(id));")
	tk.MustExec("insert into t values (9734095886065816707, 'a'), (10353107668348738101, 'b'), (0, 'c');")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (33, 'd');")
	tk.MustQuery("select max(id) from t;").Check(testkit.Rows("10353107668348738101"))
	tk.MustExec("rollback")

	tk.MustExec("alter table t cache")
	for {
		tk.MustQuery("select max(id) from t;").Check(testkit.Rows("10353107668348738101"))
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			break
		}
	}

	// // With subquery, like the original issue case.
	for {
		tk.MustQuery("select * from t where id > (select  max(id) from t where t.id > 0);").Check(testkit.Rows())
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			break
		}
	}

	// Test order by desc / asc.
	tk.MustQuery("select id from t order by id desc;").Check(testkit.Rows(
		"10353107668348738101",
		"9734095886065816707",
		"0"))

	tk.MustQuery("select id from t order by id asc;").Check(testkit.Rows(
		"0",
		"9734095886065816707",
		"10353107668348738101"))

	tk.MustExec("alter table t nocache")
	tk.MustExec("drop table t")

	// Cover more code that use union scan
	// TableReader/IndexReader/IndexLookup
	for idx, q := range []string{
		"create temporary table t (id bigint unsigned, c int default null, index(id))",
		"create temporary table t (id bigint unsigned primary key)",
	} {
		tk.MustExec(q)
		tk.MustExec("insert into t(id) values (1), (3), (9734095886065816707), (9734095886065816708)")
		tk.MustQuery("select min(id) from t").Check(testkit.Rows("1"))
		tk.MustQuery("select max(id) from t").Check(testkit.Rows("9734095886065816708"))
		tk.MustQuery("select id from t order by id asc").Check(testkit.Rows(
			"1", "3", "9734095886065816707", "9734095886065816708"))
		tk.MustQuery("select id from t order by id desc").Check(testkit.Rows(
			"9734095886065816708", "9734095886065816707", "3", "1"))
		if idx == 0 {
			tk.MustQuery("select * from t order by id asc").Check(testkit.Rows(
				"1 <nil>",
				"3 <nil>",
				"9734095886065816707 <nil>",
				"9734095886065816708 <nil>"))
			tk.MustQuery("select * from t order by id desc").Check(testkit.Rows(
				"9734095886065816708 <nil>",
				"9734095886065816707 <nil>",
				"3 <nil>",
				"1 <nil>"))
		}
		tk.MustExec("drop table t")
	}

	// More and more test
	tk.MustExec("create global temporary table `tmp1` (id bigint unsigned primary key) on commit delete rows;")
	tk.MustExec("begin")
	tk.MustExec("insert into tmp1 values (0),(1),(2),(65536),(9734095886065816707),(9734095886065816708);")
	tk.MustQuery("select * from tmp1 where id <= 65534 or (id > 65535 and id < 9734095886065816700) or id >= 9734095886065816707 order by id desc;").Check(testkit.Rows(
		"9734095886065816708", "9734095886065816707", "65536", "2", "1", "0"))

	tk.MustQuery("select * from tmp1 where id <= 65534 or (id > 65535 and id < 9734095886065816700) or id >= 9734095886065816707 order by id asc;").Check(testkit.Rows(
		"0", "1", "2", "65536", "9734095886065816707", "9734095886065816708"))

	tk.MustExec("create global temporary table `tmp2` (id bigint primary key) on commit delete rows;")
	tk.MustExec("begin")
	tk.MustExec("insert into tmp2 values(-2),(-1),(0),(1),(2);")
	tk.MustQuery("select * from tmp2 where id <= -1 or id > 0 order by id desc;").Check(testkit.Rows("2", "1", "-1", "-2"))
	tk.MustQuery("select * from tmp2 where id <= -1 or id > 0 order by id asc;").Check(testkit.Rows("-2", "-1", "1", "2"))
}

func TestIssue33042(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t1(id int primary key, col1 int)")
	tk.MustExec("create table t2(id int primary key, col1 int)")
	tk.MustQuery("explain format='brief' SELECT /*+ merge_join(t1, t2)*/ * FROM (t1 LEFT JOIN t2 ON t1.col1=t2.id) order by t2.id;").Check(
		testkit.Rows(
			"Sort 12500.00 root  test.t2.id",
			"└─MergeJoin 12500.00 root  left outer join, left key:test.t1.col1, right key:test.t2.id",
			"  ├─TableReader(Build) 10000.00 root  data:TableFullScan",
			"  │ └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:true, stats:pseudo",
			"  └─Sort(Probe) 10000.00 root  test.t1.col1",
			"    └─TableReader 10000.00 root  data:TableFullScan",
			"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		),
	)
}

func TestIssue29663(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (c int, d int)")
	tk.MustExec("insert into t1 values(1, 1), (1,2),(2,1),(2,2)")
	tk.MustExec("insert into t2 values(1, 3), (1,4),(2,5),(2,6)")

	tk.MustQuery("explain select one.a from t1 one order by (select two.d from t2 two where two.c = one.b)").Check(testkit.Rows(
		"Projection_16 10000.00 root  test.t1.a",
		"└─Sort_17 10000.00 root  test.t2.d",
		"  └─Apply_20 10000.00 root  CARTESIAN left outer join",
		"    ├─TableReader_22(Build) 10000.00 root  data:TableFullScan_21",
		"    │ └─TableFullScan_21 10000.00 cop[tikv] table:one keep order:false, stats:pseudo",
		"    └─MaxOneRow_23(Probe) 1.00 root  ",
		"      └─TableReader_26 2.00 root  data:Selection_25",
		"        └─Selection_25 2.00 cop[tikv]  eq(test.t2.c, test.t1.b)",
		"          └─TableFullScan_24 2000.00 cop[tikv] table:two keep order:false, stats:pseudo"))
}

func TestIssue31609(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustQuery("explain select rank() over (partition by table_name) from information_schema.tables").Check(testkit.Rows(
		"Projection_7 10000.00 root  Column#27",
		"└─Shuffle_11 10000.00 root  execution info: concurrency:5, data sources:[MemTableScan_9]",
		"  └─Window_8 10000.00 root  rank()->Column#27 over(partition by Column#3)",
		"    └─Sort_10 10000.00 root  Column#3",
		"      └─MemTableScan_9 10000.00 root table:TABLES ",
	))
}

func TestDecimalOverflow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table deci (a decimal(65,30),b decimal(65,0))")
	tk.MustExec("insert into deci values (1234567890.123456789012345678901234567890,987654321098765432109876543210987654321098765432109876543210)")
	tk.MustQuery("select a from deci union ALL select b from deci;").Sort().Check(testkit.Rows("1234567890.123456789012345678901234567890", "99999999999999999999999999999999999.999999999999999999999999999999"))
}

func TestIssue35083(t *testing.T) {
	defer func() {
		variable.SetSysVar(variable.TiDBOptProjectionPushDown, variable.BoolToOnOff(config.GetGlobalConfig().Performance.ProjectionPushDown))
	}()
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.ProjectionPushDown = true
	})
	variable.SetSysVar(variable.TiDBOptProjectionPushDown, variable.BoolToOnOff(config.GetGlobalConfig().Performance.ProjectionPushDown))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a varchar(100), b int)")
	tk.MustQuery("select @@tidb_opt_projection_push_down").Check(testkit.Rows("1"))
	tk.MustQuery("explain format = 'brief' select cast(a as datetime) from t1").Check(testkit.Rows(
		"TableReader 10000.00 root  data:Projection",
		"└─Projection 10000.00 cop[tikv]  cast(test.t1.a, datetime BINARY)->Column#4",
		"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
}

func TestIssue25813(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a json);")
	tk.MustExec("insert into t values('{\"id\": \"ish\"}');")
	tk.MustQuery("select t2.a from t t1 left join t t2 on t1.a=t2.a where t2.a->'$.id'='ish';").Check(testkit.Rows("{\"id\": \"ish\"}"))

	tk.MustQuery("explain format = 'brief' select * from t t1 left join t t2 on t1.a=t2.a where t2.a->'$.id'='ish';").Check(testkit.Rows(
		"Selection 8000.00 root  eq(json_extract(test.t.a, \"$.id\"), cast(\"ish\", json BINARY))",
		"└─HashJoin 10000.00 root  left outer join, equal:[eq(test.t.a, test.t.a)]",
		"  ├─TableReader(Build) 8000.00 root  data:Selection",
		"  │ └─Selection 8000.00 cop[tikv]  not(isnull(cast(test.t.a, var_string(4294967295))))",
		"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"    └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
}

func TestRepeatPushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(2147483647, 2)")
	tk.MustExec("insert into t values(12, 2)")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "repeat(cast(test.t.a, var_string(20)), test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select repeat(a,b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestIssue36194(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	// create virtual tiflash replica.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustQuery("explain format = 'brief' select * from t where a + 1 > 20 limit 100;;").Check(testkit.Rows(
		"Limit 100.00 root  offset:0, count:100",
		"└─TableReader 100.00 root  data:ExchangeSender",
		"  └─ExchangeSender 100.00 mpp[tiflash]  ExchangeType: PassThrough",
		"    └─Limit 100.00 mpp[tiflash]  offset:0, count:100",
		"      └─Selection 100.00 mpp[tiflash]  gt(plus(test.t.a, 1), 20)",
		"        └─TableFullScan 125.00 mpp[tiflash] table:t keep order:false, stats:pseudo"))
}

func TestGetFormatPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(location varchar(10));")
	tk.MustExec("insert into t values('USA'), ('JIS'), ('ISO'), ('EUR'), ('INTERNAL')")
	tk.MustExec("set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash';")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	tk.MustQuery("explain format = 'brief' select GET_FORMAT(DATE, location) from t;").Check(testkit.Rows(
		"TableReader 10000.00 root  data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Projection 10000.00 mpp[tiflash]  get_format(DATE, test.t.location)->Column#3",
		"    └─TableFullScan 10000.00 mpp[tiflash] table:t keep order:false, stats:pseudo"))
}

func TestAggWithJsonPushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json);")
	tk.MustExec("insert into t values(null);")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"HashAgg_6", "root", "funcs:avg(Column#4)->Column#3"},
		{"└─Projection_19", "root", "cast(test.t.a, double BINARY)->Column#4"},
		{"  └─TableReader_12", "root", "data:TableFullScan_11"},
		{"    └─TableFullScan_11", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select avg(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]interface{}{
		{"HashAgg_6", "root", "funcs:sum(Column#4)->Column#3"},
		{"└─Projection_19", "root", "cast(test.t.a, double BINARY)->Column#4"},
		{"  └─TableReader_12", "root", "data:TableFullScan_11"},
		{"    └─TableFullScan_11", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select sum(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]interface{}{
		{"HashAgg_6", "root", "funcs:group_concat(Column#4 separator \",\")->Column#3"},
		{"└─Projection_13", "root", "cast(test.t.a, var_string(4294967295))->Column#4"},
		{"  └─TableReader_10", "root", "data:TableFullScan_9"},
		{"    └─TableFullScan_9", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select /*+ hash_agg() */  group_concat(a) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestLeftShiftPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(2147483647, 32)")
	tk.MustExec("insert into t values(12, 2)")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "leftshift(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select a << b from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestIssue36609(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustExec("create table t2(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustExec("create table t3(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustExec("create table t4(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustExec("create table t5(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustQuery("select * from t3 straight_join t4 on t3.a = t4.b straight_join t2 on t3.d = t2.c straight_join t1 on t1.a = t2.b straight_join t5 on t4.c = t5.d where t2.b < 100 and t4.a = 10;")
	tk.MustQuery("select * from information_schema.statements_summary;")
}

func TestHexIntOrStrPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10));")
	tk.MustExec("insert into t values(1, 'tiflash');")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	rows := [][]interface{}{
		{"TableReader_9", "root", "data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "hex(test.t.a)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select hex(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]interface{}{
		{"TableReader_9", "root", "data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "hex(test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select hex(b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestEltPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(20))")
	tk.MustExec("insert into t values(2147483647, '32')")
	tk.MustExec("insert into t values(12, 'abc')")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "elt(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select elt(a, b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestCastTimeAsDurationToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a date, b datetime(4))")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26')")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26 11:11:11')")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26 11:11:11.111111')")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26 11:11:11.123456')")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26 11:11:11.999999')")

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "cast(test.t.a, time BINARY)->Column#4, cast(test.t.b, time BINARY)->Column#5"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select cast(a as time), cast(b as time) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestPartitionTableFallBackStatic(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("CREATE TABLE t (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11));")
	tk.MustExec("insert into t values (1),(2),(3),(4),(7),(8),(9),(10)")
	tk.MustExec("analyze table t")

	// use static plan in static mode
	rows := [][]interface{}{
		{"PartitionUnion", "", ""},
		{"├─TableReader", "", "data:TableFullScan"},
		{"│ └─TableFullScan", "table:t, partition:p0", "keep order:false"},
		{"└─TableReader", "", "data:TableFullScan"},
		{"  └─TableFullScan", "table:t, partition:p1", "keep order:false"},
	}
	tk.MustQuery("explain format='brief' select * from t").CheckAt([]int{0, 3, 4}, rows)

	tk.MustExec("CREATE TABLE t2 (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11));")
	tk.MustExec("insert into t2 values (1),(2),(3),(4),(7),(8),(9),(10)")
	tk.MustExec("analyze table t2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")

	// use static plan in dynamic mode due to having not global stats
	tk.MustQuery("explain format='brief' select * from t").CheckAt([]int{0, 3, 4}, rows)
	tk.MustExec("analyze table t")

	// use dynamic plan in dynamic mode with global stats
	rows = [][]interface{}{
		{"TableReader", "partition:all", "data:TableFullScan"},
		{"└─TableFullScan", "table:t", "keep order:false"},
	}
	tk.MustQuery("explain format='brief' select * from t").CheckAt([]int{0, 3, 4}, rows)

	rows = [][]interface{}{
		{"Union", "", ""},
		{"├─PartitionUnion", "", ""},
		{"│ ├─TableReader", "", "data:TableFullScan"},
		{"│ │ └─TableFullScan", "table:t, partition:p0", "keep order:false"},
		{"│ └─TableReader", "", "data:TableFullScan"},
		{"│   └─TableFullScan", "table:t, partition:p1", "keep order:false"},
		{"└─PartitionUnion", "", ""},
		{"  ├─TableReader", "", "data:TableFullScan"},
		{"  │ └─TableFullScan", "table:t2, partition:p0", "keep order:false"},
		{"  └─TableReader", "", "data:TableFullScan"},
		{"    └─TableFullScan", "table:t2, partition:p1", "keep order:false"},
	}
	// use static plan in dynamic mode due to t2 has no global stats
	tk.MustQuery("explain format='brief' select  * from t union all select * from t2;").CheckAt([]int{0, 3, 4}, rows)
}

func TestEnableTiFlashReadForWriteStmt(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 2)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enable_tiflash_read_for_write_stmt = ON")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t2", L: "t2"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	checkMpp := func(r [][]interface{}) {
		check := false
		for i := range r {
			if r[i][2] == "mpp[tiflash]" {
				check = true
				break
			}
		}
		require.Equal(t, check, true)
	}

	// Insert into ... select
	rs := tk.MustQuery("explain insert into t2 select a+b from t").Rows()
	checkMpp(rs)

	rs = tk.MustQuery("explain insert into t2 select t.a from t2 join t on t2.a = t.a").Rows()
	checkMpp(rs)

	// Replace into ... select
	rs = tk.MustQuery("explain replace into t2 select a+b from t").Rows()
	checkMpp(rs)

	// CTE
	rs = tk.MustQuery("explain update t set a=a+1 where b in (select a from t2 where t.a > t2.a)").Rows()
	checkMpp(rs)
}

func TestTableRangeFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int primary key, b int)")
	tk.MustExec("create table t2 (c int)")
	tk.MustQuery("explain format='brief' select * from t1 where a in (10, 20, 30, 40, 50) and b > 1").Check(testkit.Rows(
		"Selection 1.67 root  gt(test.t1.b, 1)",
		"└─Batch_Point_Get 5.00 root table:t1 handle:[10 20 30 40 50], keep order:false, desc:false"))
	tk.MustQuery("explain format='brief' select * from t1 join t2 on t1.b = t2.c where t1.a in (10, 20, 30, 40, 50)").Check(testkit.Rows(
		"HashJoin 6.24 root  inner join, equal:[eq(test.t1.b, test.t2.c)]",
		"├─Selection(Build) 5.00 root  not(isnull(test.t1.b))",
		"│ └─Batch_Point_Get 5.00 root table:t1 handle:[10 20 30 40 50], keep order:false, desc:false",
		"└─TableReader(Probe) 9990.00 root  data:Selection",
		"  └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.c))",
		"    └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo"))
	tk.MustExec("set @@tidb_opt_range_max_size=10")
	tk.MustQuery("explain format='brief' select * from t1 where a in (10, 20, 30, 40, 50) and b > 1").Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  gt(test.t1.b, 1), in(test.t1.a, 10, 20, 30, 40, 50)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 10 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
	tk.MustQuery("explain format='brief' select * from t1 join t2 on t1.b = t2.c where t1.a in (10, 20, 30, 40, 50)").Check(testkit.Rows(
		"HashJoin 10000.00 root  inner join, equal:[eq(test.t1.b, test.t2.c)]",
		"├─TableReader(Build) 8000.00 root  data:Selection",
		"│ └─Selection 8000.00 cop[tikv]  not(isnull(test.t2.c))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"└─TableReader(Probe) 8000.00 root  data:Selection",
		"  └─Selection 8000.00 cop[tikv]  in(test.t1.a, 10, 20, 30, 40, 50), not(isnull(test.t1.b))",
		"    └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 10 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
}

func TestPlanCacheForTableRangeFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@tidb_enable_prepared_plan_cache=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("set @@tidb_opt_range_max_size=10")
	tk.MustExec("prepare stmt from 'select * from t where a in (?, ?, ?, ?, ?) and b > 1'")
	tk.MustExec("set @a=10, @b=20, @c=30, @d=40, @e=50")
	tk.MustExec("execute stmt using @a, @b, @c, @d, @e")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 10 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
	tk.MustExec("execute stmt using @a, @b, @c, @d, @e")
	// The plan with range fallback is not cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue37760(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustExec("insert into t values (2), (4), (6)")
	tk.MustExec("set @@tidb_opt_range_max_size=1")
	tk.MustQuery("select * from t where a").Check(testkit.Rows("2", "4", "6"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
}

// TestExplainAnalyzeDMLCommit covers the issue #37373.
func TestExplainAnalyzeDMLCommit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int key, c2 int);")
	tk.MustExec("insert into t values (1, 1)")

	err := failpoint.Enable("github.com/pingcap/tidb/session/mockSleepBeforeTxnCommit", "return(500)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/session/mockSleepBeforeTxnCommit")
	}()
	// The commit is paused by the failpoint, after the fix the explain statement
	// execution should proceed after the commit finishes.
	_, err = tk.Exec("explain analyze delete from t;")
	require.NoError(t, err)
	tk.MustQuery("select * from t").Check(testkit.Rows())
}
>>>>>>> b0e073478... execution: commit the transaction before responding explain analyze results to the client (#38044)
