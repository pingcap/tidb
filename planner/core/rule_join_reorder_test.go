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

package core_test

import (
	"testing"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func runJoinReorderTestData(t *testing.T, tk *testkit.TestKit, name string) {
	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning []string
	}
	joinReorderSuiteData := plannercore.GetJoinReorderSuiteData()
	joinReorderSuiteData.GetTestCasesByName(name, t, &input, &output)
	require.Equal(t, len(input), len(output))
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + input[i]).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + input[i]).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

func TestStraightJoinHint(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3, t4;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	runJoinReorderTestData(t, tk, "TestStraightJoinHint")
}

func TestLeadingJoinHint(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6, t7, t8;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	tk.MustExec("create table t5(a int, b int, key(a));")
	tk.MustExec("create table t6(a int, b int, key(a));")
	tk.MustExec("create table t7(a int, b int, key(a));")
	tk.MustExec("create table t8(a int, b int, key(a));")
	runJoinReorderTestData(t, tk, "TestLeadingJoinHint")

	// test cases for outer join
	tk.MustExec("select /*+ leading(t1, t3) */ * from t1 left join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 leading hint is inapplicable when we have outer join"))
	tk.MustExec("select /*+ leading(t2) */ * from t1 left join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 leading hint is inapplicable when we have outer join"))
	tk.MustExec("select /*+ leading(t2, t3) */ * from t1 left join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 leading hint is inapplicable when we have outer join"))
	tk.MustExec("select /*+ leading(t1, t2, t3) */ * from t1 left join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 leading hint is inapplicable when we have outer join"))
	tk.MustExec("select /*+ leading(t1, t3) */ * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 leading hint is inapplicable when we have outer join"))
	tk.MustExec("select /*+ leading(t1, t2) */ * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 leading hint is inapplicable when we have outer join"))
	tk.MustExec("select /*+ leading(t3) */ * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 leading hint is inapplicable when we have outer join"))

	// test cases for multiple leading hints
	tk.MustExec("select /*+ leading(t1) leading(t2) */ * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid"))
}
