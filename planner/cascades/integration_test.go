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

package cascades_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testIntegrationSuite{})

type testIntegrationSuite struct {
	store    kv.Storage
	testData testutil.TestData
}

func newStoreWithBootstrap() (kv.Storage, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, err
	}
	_, err = session.BootstrapSession(store)
	return store, err
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	s.store, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.testData, err = testutil.LoadTestSuiteData("testdata", "integration_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
	s.store.Close()
}

func (s *testIntegrationSuite) TestSimpleProjDual(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	tk.MustQuery("explain select 1").Check(testkit.Rows(
		"Projection_3 1.00 root  1->Column#1",
		"└─TableDual_4 1.00 root  rows:1",
	))
	tk.MustQuery("select 1").Check(testkit.Rows(
		"1",
	))
}

func (s *testIntegrationSuite) TestPKIsHandleRangeScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values(1,2),(3,4),(5,6)")
	tk.MustExec("analyze table t")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestIndexScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, d int, index idx_b(b), index idx_c_b(c, b))")
	tk.MustExec("insert into t values(1,2,3,100),(4,5,6,200),(7,8,9,300)")
	tk.MustExec("analyze table t")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestBasicShow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	tk.MustQuery("desc t").Check(testkit.Rows(
		"a int(11) NO PRI <nil> ",
		"b int(11) YES  <nil> ",
	))
}

func (s *testIntegrationSuite) TestSort(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("analyze table t")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestAggregation(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("analyze table t")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestPushdownDistinctEnable(c *C) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 1", variable.TiDBOptDistinctAggPushDown),
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testIntegrationSuite) TestPushdownDistinctDisable(c *C) {
	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 0", variable.TiDBOptDistinctAggPushDown),
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testIntegrationSuite) doTestPushdownDistinct(c *C, vars, input []string, output []struct {
	SQL    string
	Plan   []string
	Result []string
}) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index(c))")
	tk.MustExec("insert into t values (1, 1, 1), (1, 1, 3), (1, 2, 3), (2, 1, 3), (1, 2, NULL);")
	tk.MustExec("set session sql_mode=''")
	tk.MustExec(fmt.Sprintf("set session %s=1", variable.TiDBHashAggPartialConcurrency))
	tk.MustExec(fmt.Sprintf("set session %s=1", variable.TiDBHashAggFinalConcurrency))
	tk.MustExec("set session tidb_enable_cascades_planner = 1")

	for _, v := range vars {
		tk.MustExec(v)
	}

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestSimplePlans(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("analyze table t")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("insert into t2 values (1, 111), (2, 222), (3, 333), (5, 555)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestDoubleRead(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, key(a), key(b, c), key(c, b))")
	tk.MustExec("insert into t values (1, 11, 4), (4, 44, 1), (2, 22, 3), (3, 33, 2)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestIndexJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values (0, 0), (1, 0), (2, 0), (3, 0)")
	tk.MustExec("insert into t2 values (0, 111), (2, 222), (3, 333), (5, 555)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestApply(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("insert into t2 values (1, 11), (2, 22), (3, 33)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestMemTableScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestTopN(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int primary key, b int);")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33);")
	tk.MustExec("analyze table t")
	tk.MustExec("set session tidb_enable_cascades_planner = 1;")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestCascadePlannerHashedPartTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists pt1")
	tk.MustExec("create table pt1(a bigint, b bigint) partition by hash(a) partitions 4")
	tk.MustExec(`insert into pt1 values(1,10)`)
	tk.MustExec(`insert into pt1 values(2,20)`)
	tk.MustExec(`insert into pt1 values(3,30)`)
	tk.MustExec(`insert into pt1 values(4,40)`)
	tk.MustExec(`insert into pt1 values(5,50)`)

	tk.MustExec("set @@tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestInlineProjection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(a bigint, b bigint, index idx_a(a), index idx_b(b));")
	tk.MustExec("create table t2(a bigint, b bigint, index idx_a(a), index idx_b(b));")
	tk.MustExec("insert into t1 values (1, 1), (2, 2);")
	tk.MustExec("insert into t2 values (1, 1), (3, 3);")
	tk.MustExec("set session tidb_enable_cascades_planner = 1;")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestTiKVSelectionStats(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a int, b int, index idx_a_b(a,b))")
	tk.MustExec("insert into t values (0,0,0),(1,0,1),(2,0,2),(3,0,3),(4,0,4),(5,1,5),(6,1,6),(7,1,7),(8,1,8)")
	tk.MustExec("analyze table t")
	tk.MustExec("set session tidb_enable_cascades_planner = 1;")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestDecorrelate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("insert into t2 values (1, 11), (2, 22), (3, 33)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}
