// Copyright 2024 PingCAP, Inc.
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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestOuter2Inner(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t1(a1 int, b1 int, c1 int)")
		testKit.MustExec("create table t2(a2 int, b2 int, c2 int)")
		testKit.MustExec("create table t3(a3 int, b3 int, c3 int)")
		testKit.MustExec("create table t4(a4 int, b4 int, c4 int)")
		testKit.MustExec("create table ti(i int)")
		testKit.MustExec("CREATE TABLE lineitem (L_PARTKEY INTEGER ,L_QUANTITY DECIMAL(15,2),L_EXTENDEDPRICE  DECIMAL(15,2))")
		testKit.MustExec("CREATE TABLE part(P_PARTKEY INTEGER,P_BRAND CHAR(10),P_CONTAINER CHAR(10))")
		testKit.MustExec("CREATE TABLE d (pk int, col_blob blob, col_blob_key blob, col_varchar_key varchar(1) , col_date date, col_int_key int)")
		testKit.MustExec("CREATE TABLE dd (pk int, col_blob blob, col_blob_key blob, col_date date, col_int_key int)")
		testKit.MustExec("create table t0 (a0 int, b0 char, c0 char(2))")
		testKit.MustExec("create table t11 (a1 int, b1 char, c1 char)")

		var input Input
		var output []struct {
			SQL  string
			Plan []string
		}
		suiteData := GetOuter2InnerSuiteData()
		suiteData.LoadTestCasesByName("TestOuter2Inner", t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := testKit.MustQuery("explain format = 'brief' " + sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

// can not add this test case to TestOuter2Inner because the collation_connection is different
func TestOuter2InnerIssue55886(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t1")
		testKit.MustExec("drop table if exists t2")
		testKit.MustExec("create table t1(c_foveoe text, c_jbb text, c_cz text not null)")
		testKit.MustExec("create table t2(c_g7eofzlxn int)")
		testKit.MustExec("set collation_connection = 'latin1_bin'")

		var input Input
		var output []struct {
			SQL  string
			Plan []string
		}
		suiteData := GetOuter2InnerSuiteData()
		suiteData.LoadTestCasesByName("TestOuter2InnerIssue55886", t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := testKit.MustQuery("explain format = 'brief' " + sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIssue58836(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (id int primary key, name varchar(100));")
	testKit.MustExec("insert into t values (1, null), (2, 1);")
	testKit.MustQuery(`with tmp as (
select
row_number() over() as id,
(select '1' from dual where id in (2)) as name
from t
)
select 'ok' from dual
where ('1',1) in (select name, id from tmp);`).Check(testkit.Rows())
	testKit.MustQuery(`explain format = 'plan_tree' with tmp as (
select
row_number() over() as id,
(select '1' from dual where id in (2)) as name
from t
)
select 'ok' from dual
where ('1',1) in (select name, id from tmp);`).Check(testkit.Rows(
		`Projection root  ok->Column#13`,
		`└─HashJoin root  CARTESIAN inner join`,
		`  ├─TableDual(Build) root  rows:1`,
		`  └─HashAgg(Probe) root  group by:Column#11, Column#12, funcs:firstrow(1)->Column#18`,
		`    └─Selection root  eq("1", Column#11), eq(1, Column#12)`,
		`      └─Window root  row_number()->Column#12 over(rows between current row and current row)`,
		`        └─Apply root  CARTESIAN left outer join, left side:TableReader`,
		`          ├─TableReader(Build) root  data:TableFullScan`,
		`          │ └─TableFullScan cop[tikv] table:t keep order:false, stats:pseudo`,
		`          └─Projection(Probe) root  1->Column#11`,
		`            └─Selection root  eq(test.t.id, 2)`,
		`              └─TableDual root  rows:1`))
}
