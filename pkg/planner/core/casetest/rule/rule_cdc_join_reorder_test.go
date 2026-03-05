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
	"github.com/stretchr/testify/require"
)

func TestCDCJoinReorder(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3, t4, t5")
		tk.MustExec("CREATE TABLE t1 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t2 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t3 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t4 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t5 (a INT, b INT)")

		tk.MustExec("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)")
		tk.MustExec("INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400)")
		tk.MustExec("INSERT INTO t3 VALUES (1, 1000), (3, 3000), (5, 5000)")
		tk.MustExec("INSERT INTO t4 VALUES (1, 10000), (4, 40000), (6, 60000)")
		tk.MustExec("INSERT INTO t5 VALUES (2, 20000), (5, 50000), (7, 70000)")

		tk.MustExec("analyze table t1 all columns;")
		tk.MustExec("analyze table t2 all columns;")
		tk.MustExec("analyze table t3 all columns;")
		tk.MustExec("analyze table t4 all columns;")
		tk.MustExec("analyze table t5 all columns;")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetCDCJoinReorderSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)

		// Phase 1: Collect expected results using the old join reorder algorithm
		// (CD-C is NOT enabled yet). These serve as the ground-truth baseline.
		expectedResults := make([][]string, len(input))
		for i, sql := range input {
			expectedResults[i] = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		}

		// Phase 2: Enable CD-C algorithm, then verify both the plan and the
		// result correctness for every case.
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/enableCDCJoinReorder", `return(true)`)

		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Check(testkit.Rows(output[i].Plan...))

			// Run with CD-C and cross-validate against the old algorithm baseline.
			cdcResult := testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			require.Equalf(t, expectedResults[i], cdcResult,
				"CD-C result differs from old algorithm for case[%d]: %s", i, sql)
		}
	})
}
