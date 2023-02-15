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

	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
)

// Rule should bot be applied
func TestPushDerivedTopnNegative(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("insert into t values(2,1)")
	tk.MustExec("insert into t values(3,2)")
	tk.MustExec("insert into t values(4,2)")
	tk.MustExec("insert into t values(5,2)")
	var input Input
	var output []struct {
		SQL  string
		Plan []string
	}
	suiteData := GetDerivedTopNSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

// Rule should be applied
func TestPushDerivedTopnPositive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("insert into t values(2,1)")
	tk.MustExec("insert into t values(3,2)")
	tk.MustExec("insert into t values(4,2)")
	tk.MustExec("insert into t values(5,2)")
	var input Input
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	suiteData := GetDerivedTopNSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
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
