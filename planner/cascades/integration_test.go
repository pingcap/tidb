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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cascades_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/planner/cascades"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
)

func TestSimpleProjDual(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	tk.MustQuery("explain select 1").Check(testkit.Rows(
		"Projection_3 1.00 root  1->Column#1",
		"└─TableDual_4 1.00 root  rows:1",
	))
	tk.MustQuery("select 1").Check(testkit.Rows("1"))
}

func TestPKIsHandleRangeScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values(1,2),(3,4),(5,6)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIndexScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, d int, index idx_b(b), index idx_c_b(c, b))")
	tk.MustExec("insert into t values(1,2,3,100),(4,5,6,200),(7,8,9,300)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestBasicShow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	tk.MustQuery("desc t").Check(testkit.Rows(
		"a int(11) NO PRI <nil> ",
		"b int(11) YES  <nil> ",
	))
}

func TestSort(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestAggregation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	tk.MustExec("set session tidb_executor_concurrency = 4")
	tk.MustExec("set @@session.tidb_hash_join_concurrency = 5")
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestPushdownDistinctEnable(t *testing.T) {
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 1", variable.TiDBOptDistinctAggPushDown),
	}
	doTestPushdownDistinct(t, vars, input, output)
}

func TestPushdownDistinctDisable(t *testing.T) {
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@session.%s = 0", variable.TiDBOptDistinctAggPushDown),
	}
	doTestPushdownDistinct(t, vars, input, output)
}

func doTestPushdownDistinct(t *testing.T, vars, input []string, output []struct {
	SQL    string
	Plan   []string
	Result []string
}) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func TestSimplePlans(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
	tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("insert into t2 values (1, 111), (2, 222), (3, 333), (5, 555)")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestApply(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestMemTableScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set session tidb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestTopN(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int primary key, b int);")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33);")
	tk.MustExec("set session tidb_enable_cascades_planner = 1;")
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestCascadePlannerHashedPartTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
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
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}

func TestInlineProjection(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	integrationSuiteData := cascades.GetIntegrationSuiteData()
	integrationSuiteData.GetTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + sql).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + sql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}
