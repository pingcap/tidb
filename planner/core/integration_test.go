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
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testIntegrationSuite{})

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

func (s *testIntegrationSuite) TestPpdWithSetVar(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 varchar(255))")
	tk.MustExec("insert into t values(1,'a'),(2,'d'),(3,'c')")

	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=1 and t01.c2='d'").Check(testkit.Rows())
	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=2 and t01.c2='d'").Check(testkit.Rows("2 d 2"))
}

func (s *testIntegrationSuite) TestBitColErrorMessage(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists bit_col_t")
	tk.MustExec("create table bit_col_t (a bit(64))")
	tk.MustExec("drop table bit_col_t")
	tk.MustExec("create table bit_col_t (a bit(1))")
	tk.MustExec("drop table bit_col_t")
	tk.MustGetErrCode("create table bit_col_t (a bit(0))", mysql.ErrInvalidFieldSize)
	tk.MustGetErrCode("create table bit_col_t (a bit(65))", mysql.ErrTooBigDisplaywidth)
}

func (s *testIntegrationSuite) TestIsolationRead(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")

	// Don't filter mysql.SystemDB by isolation read.
	tk.MustQuery("explain select * from mysql.stats_meta").Check(testkit.Rows(
		"TableReader_5 10000.00 root data:TableScan_4",
		"└─TableScan_4 10000.00 cop[tikv] table:stats_meta, range:[-inf,+inf], keep order:false, stats:pseudo"))

	_, err = tk.Exec("select * from t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can not find access path matching 'tidb_isolation_read_engines'(value: 'tiflash') and tidb-server config isolation-read(engines: '[tikv tiflash]'). Available values are 'tikv'.")

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv, tiflash'")
	config.GetGlobalConfig().IsolationRead.Engines = []string{"tiflash"}
	_, err = tk.Exec("select * from t")
	config.GetGlobalConfig().IsolationRead.Engines = []string{"tikv", "tiflash"}
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can not find access path matching 'tidb_isolation_read_engines'(value: 'tikv,tiflash') and tidb-server config isolation-read(engines: '[tiflash]'). Available values are 'tikv'.")
}

func (s *testIntegrationSuite) TestSelPushDownTiFlash(c *C) {
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

	// All conditions should push tiflash.
	tk.MustQuery(`explain select * from t where t.a > 1 and t.b = "flash" or t.a + 3 * t.a = 5`).Check(testkit.Rows(
		"TableReader_7 8000.00 root data:Selection_6",
		"└─Selection_6 8000.00 cop[tiflash] or(and(gt(test.t.a, 1), eq(test.t.b, \"flash\")), eq(plus(test.t.a, mul(3, test.t.a)), 5))",
		"  └─TableScan_5 10000.00 cop[tiflash] table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
	))

	// Part of conditions should push tiflash.
	tk.MustQuery(`explain select * from t where cast(t.a as decimal(30, 5)) + 3 = 5.1`).Check(testkit.Rows(
		"Selection_5 8000.00 root eq(plus(cast(test.t.a), 3), 5.1)",
		"└─TableReader_7 10000.00 root data:TableScan_6",
		"  └─TableScan_6 10000.00 cop[tiflash] table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
}

func (s *testIntegrationSuite) TestIssue15110(c *C) {
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

func (s *testIntegrationSuite) TestReadFromStorageHint(c *C) {
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

func (s *testIntegrationSuite) TestReadFromStorageHintAndIsolationRead(c *C) {
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

<<<<<<< HEAD
=======
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

>>>>>>> 9dc6d9c... planner: check readEngines when building plan for index hint (#15723)
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

func (s *testIntegrationSuite) TestTopNByConstFunc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustQuery("select max(t.col) from (select 'a' as col union all select '' as col) as t").Check(testkit.Rows(
		"a",
	))
}

func (s *testIntegrationSuite) TestIssue15546(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, pt, vt")
	tk.MustExec("create table t(a1 int, b1 int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("create table pt(a2 int primary key, b2 int) partition by range(a2) (" +
		"PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20), PARTITION `p2` VALUES LESS THAN (30))")
	tk.MustExec("insert into pt values(1, 1), (11, 11), (21, 21)")
	tk.MustExec("create definer='root'@'localhost' view vt(a1, b1) as select a1, b1 from t")
	tk.MustQuery("select * from pt, vt where a2 = a1").Check(testkit.Rows("1 1 1 1"))
}
