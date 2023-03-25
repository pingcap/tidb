// Copyright 2023 PingCAP, Inc.
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

package casetest

import (
	"testing"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestCBOPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t (a varchar(20), b int, c int, d int, primary key(a), unique key(b, c))")
	tk.MustExec("insert into t values('1',4,4,1), ('2',3,3,2), ('3',2,2,3), ('4',1,1,4)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	pointGetPlanData := GetPointGetPlanData()
	pointGetPlanData.LoadTestCases(t, &input, &output)
	require.Equal(t, len(input), len(output))
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		res := tk.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			output[i].Res = testdata.ConvertRowsToStrings(res.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
		res.Check(testkit.Rows(output[i].Res...))
	}
}

func TestCBOShouldNotUsePointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop tables if exists t1, t2, t3, t4, t5")
	tk.MustExec("create table t1(id varchar(20) primary key)")
	tk.MustExec("create table t2(id varchar(20), unique(id))")
	tk.MustExec("create table t3(id varchar(20), d varchar(20), unique(id, d))")
	tk.MustExec("create table t4(id int, d varchar(20), c varchar(20), unique(id, d))")
	tk.MustExec("create table t5(id bit(64) primary key)")
	tk.MustExec("insert into t1 values('asdf'), ('1asdf')")
	tk.MustExec("insert into t2 values('asdf'), ('1asdf')")
	tk.MustExec("insert into t3 values('asdf', 't'), ('1asdf', 't')")
	tk.MustExec("insert into t4 values(1, 'b', 'asdf'), (1, 'c', 'jkl'), (1, 'd', '1jkl')")
	tk.MustExec("insert into t5 values(48)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}

	pointGetPlanData := GetPointGetPlanData()
	pointGetPlanData.LoadTestCases(t, &input, &output)
	require.Equal(t, len(input), len(output))
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		res := tk.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			output[i].Res = testdata.ConvertRowsToStrings(res.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
		res.Check(testkit.Rows(output[i].Res...))
	}
}
