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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/planner/cascades"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestCascadePlannerHashedPartTable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

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
	integrationSuiteData.LoadTestCases(t, &input, &output)
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
