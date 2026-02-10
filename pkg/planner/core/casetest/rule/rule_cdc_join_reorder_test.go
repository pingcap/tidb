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
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func TestCDCJoinReorder(tt *testing.T) {
	testfailpoint.Enable(tt, "github.com/pingcap/tidb/pkg/planner/core/enableCDCJoinReorder", `return(true)`)
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3, t4")
		tk.MustExec("CREATE TABLE t1 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t2 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t3 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t4 (a INT, b INT)")

		tk.MustExec("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)")
		tk.MustExec("INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400)")
		tk.MustExec("INSERT INTO t3 VALUES (1, 1000), (3, 3000), (5, 5000)")
		tk.MustExec("INSERT INTO t4 VALUES (1, 10000), (4, 40000), (6, 60000)")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetCDCJoinReorderSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("EXPLAIN FORMAT='brief' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("EXPLAIN FORMAT='brief' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}
