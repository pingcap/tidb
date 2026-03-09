// Copyright 2026 PingCAP, Inc.
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

func TestYannakakisPlus(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists yannakakis_t1, yannakakis_t2, yannakakis_t3")
		tk.MustExec("create table yannakakis_t1 (a int, c int)")
		tk.MustExec("create table yannakakis_t2 (a int, b int)")
		tk.MustExec("create table yannakakis_t3 (b int, c int)")

		tk.MustExec("insert into yannakakis_t1 values (1, 10), (2, null), (3, 30)")
		tk.MustExec("insert into yannakakis_t2 values (1, 10), (1, 11), (2, 20), (4, 40)")
		tk.MustExec("insert into yannakakis_t3 values (10, 1000), (10, 1001), (11, 1100), (20, 2000), (30, 3000)")
		tk.MustExec("analyze table yannakakis_t1 all columns")
		tk.MustExec("analyze table yannakakis_t2 all columns")
		tk.MustExec("analyze table yannakakis_t3 all columns")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetYannakakisPlusSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)

		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("explain format = 'plan_tree' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}
