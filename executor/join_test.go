// Copyright 2016 PingCAP, Inc.
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
)

func TestNaturalJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (a int, c int)")
	tk.MustExec("insert t1 values (1,2), (10,20), (0,0)")
	tk.MustExec("insert t2 values (1,3), (100,200), (0,0)")

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

func TestUsingAndNaturalJoinSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.MustExec("create table t1 (c int, b int);")
	tk.MustExec("create table t2 (a int, b int);")
	tk.MustExec("create table t3 (b int, c int);")
	tk.MustExec("create table t4 (y int, c int);")

	tk.MustExec("insert into t1 values (10,1);")
	tk.MustExec("insert into t1 values (3 ,1);")
	tk.MustExec("insert into t1 values (3 ,2);")
	tk.MustExec("insert into t2 values (2, 1);")
	tk.MustExec("insert into t3 values (1, 3);")
	tk.MustExec("insert into t3 values (1,10);")
	tk.MustExec("insert into t4 values (11,3);")
	tk.MustExec("insert into t4 values (2, 3);")

	var input []string
	var output []struct {
		SQL string
		Res []string
	}
	executorSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func TestTiDBNAAJ(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_enable_null_aware_anti_join=0;")
	tk.MustExec("create table t(a decimal(40,0), b bigint(20) not null);")
	tk.MustExec("insert into t values(7,8),(7,8),(3,4),(3,4),(9,2),(9,2),(2,0),(2,0),(0,4),(0,4),(8,8),(8,8),(6,1),(6,1),(NULL, 0),(NULL,0);")
	tk.MustQuery("select ( table1 . a , table1 . b ) NOT IN ( SELECT 3 , 2 UNION  SELECT 9, 2 ) AS field2 from t as table1 order by field2;").Check(testkit.Rows(
		"0", "0", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"))
	tk.MustExec("set @@session.tidb_enable_null_aware_anti_join=1;")
	tk.MustQuery("select ( table1 . a , table1 . b ) NOT IN ( SELECT 3 , 2 UNION  SELECT 9, 2 ) AS field2 from t as table1 order by field2;").Check(testkit.Rows(
		"0", "0", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"))
}
