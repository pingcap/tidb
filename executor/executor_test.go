// Copyright 2022 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestSetOperationOnDiffColType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1, t2, t3`)
	tk.MustExec(`create table t1(a int, b int)`)
	tk.MustExec(`create table t2(a int, b varchar(20))`)
	tk.MustExec(`create table t3(a int, b decimal(30,10))`)
	tk.MustExec(`insert into t1 values (1,1),(1,1),(2,2),(3,3),(null,null)`)
	tk.MustExec(`insert into t2 values (1,'1'),(2,'2'),(null,null),(null,'3')`)
	tk.MustExec(`insert into t3 values (2,2.1),(3,3)`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	executorSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

// issue-23038: wrong key range of index scan for year column
func TestIndexScanWithYearCol(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 year(4), c2 int, key(c1));")
	tk.MustExec("insert into t values(2001, 1);")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	executorSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func TestSetOperation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`drop table if exists t1, t2, t3`)
	tk.MustExec(`create table t1(a int)`)
	tk.MustExec(`create table t2 like t1`)
	tk.MustExec(`create table t3 like t1`)
	tk.MustExec(`insert into t1 values (1),(1),(2),(3),(null)`)
	tk.MustExec(`insert into t2 values (1),(2),(null),(null)`)
	tk.MustExec(`insert into t3 values (2),(3)`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	executorSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}

	// from https://github.com/pingcap/tidb/issues/40279
	tk.MustExec("CREATE TABLE `issue40279` (`a` char(155) NOT NULL DEFAULT 'on1unvbxp5sko6mbetn3ku26tuiyju7w3wc0olzto9ew7gsrx',`b` mediumint(9) NOT NULL DEFAULT '2525518',PRIMARY KEY (`b`,`a`) /*T![clustered_index] CLUSTERED */);")
	tk.MustExec("insert into `issue40279` values ();")
	tk.MustQuery("( select    `issue40279`.`b` as r0 , from_base64( `issue40279`.`a` ) as r1 from `issue40279` ) " +
		"except ( " +
		"select    `issue40279`.`a` as r0 , elt(2, `issue40279`.`a` , `issue40279`.`a` ) as r1 from `issue40279`);").
		Check(testkit.Rows("2525518 <nil>"))
	tk.MustExec("drop table if exists t2")

	tk.MustExec("CREATE TABLE `t2` (   `a` varchar(20) CHARACTER SET gbk COLLATE gbk_chinese_ci DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("insert into t2 values(0xCED2)")
	result := tk.MustQuery("(select elt(2,t2.a,t2.a) from t2) except (select 0xCED2  from t2)")
	rows := result.Rows()
	require.Len(t, rows, 0)
}

func TestCompareIssue38361(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists TEST1")
	tk.MustExec("create database TEST1")
	tk.MustExec("use TEST1")
	tk.MustExec("create table t(a datetime, b bigint, c bigint)")
	tk.MustExec("insert into t values(cast('2023-08-09 00:00:00' as datetime), 20230809, 20231310)")

	tk.MustQuery("select a > 20230809 from t").Check(testkit.Rows("0"))
	tk.MustQuery("select a = 20230809 from t").Check(testkit.Rows("1"))
	tk.MustQuery("select a < 20230810 from t").Check(testkit.Rows("1"))
	// 20231310 can't be converted to valid datetime, thus should be compared using real date type,and datetime will be
	// converted to something like 'YYYYMMDDHHMMSS', bigger than 20231310
	tk.MustQuery("select a < 20231310 from t").Check(testkit.Rows("0"))
	tk.MustQuery("select 20230809 < a from t").Check(testkit.Rows("0"))
	tk.MustQuery("select 20230809 = a from t").Check(testkit.Rows("1"))
	tk.MustQuery("select 20230810 > a from t").Check(testkit.Rows("1"))
	tk.MustQuery("select 20231310 > a from t").Check(testkit.Rows("0"))

	// constant datetime cmp numeric constant should be compared as real data type
	tk.MustQuery("select cast('2023-08-09 00:00:00' as datetime) > 20230809 from t").Check(testkit.Rows("1"))
	tk.MustQuery("select cast('2023-08-09 00:00:00' as datetime) = 20230809 from t").Check(testkit.Rows("0"))
	tk.MustQuery("select cast('2023-08-09 00:00:00' as datetime) < 20230810 from t").Check(testkit.Rows("0"))
	tk.MustQuery("select cast('2023-08-09 00:00:00' as datetime) < 20231310 from t").Check(testkit.Rows("0"))
	tk.MustQuery("select 20230809 < cast('2023-08-09 00:00:00' as datetime) from t").Check(testkit.Rows("1"))
	tk.MustQuery("select 20230809 = cast('2023-08-09 00:00:00' as datetime) from t").Check(testkit.Rows("0"))
	tk.MustQuery("select 20230810 > cast('2023-08-09 00:00:00' as datetime) from t").Check(testkit.Rows("0"))
	tk.MustQuery("select 20231310 > cast('2023-08-09 00:00:00' as datetime) from t").Check(testkit.Rows("0"))

	// datetime column cmp numeric column should be compared as real data type
	tk.MustQuery("select a > b from t").Check(testkit.Rows("1"))
	tk.MustQuery("select a = b from t").Check(testkit.Rows("0"))
	tk.MustQuery("select a < b + 1 from t").Check(testkit.Rows("0"))
	tk.MustQuery("select a < c from t").Check(testkit.Rows("0"))
	tk.MustQuery("select b < a from t").Check(testkit.Rows("1"))
	tk.MustQuery("select b = a from t").Check(testkit.Rows("0"))
	tk.MustQuery("select b > a from t").Check(testkit.Rows("0"))
	tk.MustQuery("select c > a from t").Check(testkit.Rows("0"))
}
