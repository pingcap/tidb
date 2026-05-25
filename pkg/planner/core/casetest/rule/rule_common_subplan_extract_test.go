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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

// TestCommonSubplanExtract verifies that the common_subplan_extract rule rewrites
// identical UNION ALL branches into shared CTEs. The expected output is the
// EXPLAIN FORMAT='brief' plan string for each input SQL.
func TestCommonSubplanExtract(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3")
		tk.MustExec("create table t1 (a int, b int, c int)")
		tk.MustExec("create table t2 (a int, b int, c int)")
		tk.MustExec("create table t3 (a int, b int, c int)")

		var input []string
		var output []struct {
			SQL    string
			HasCTE bool
			Plan   []string
		}
		suite := GetCommonSubplanExtractSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)

		tk.MustExec("set @@tidb_opt_enable_common_subplan_extract = 1")

		for i, sql := range input {
			normalized := strings.ToLower(strings.TrimSpace(sql))
			if strings.HasPrefix(normalized, "set ") {
				tk.MustExec(sql)
				continue
			}

			rows := tk.MustQuery("explain format='brief' " + sql).Rows()
			plan := make([]string, 0, len(rows))
			for _, r := range rows {
				parts := make([]string, len(r))
				for j, col := range r {
					parts[j] = col.(string)
				}
				plan = append(plan, strings.Join(parts, "\t"))
			}

			hasCTE := planContainsCTE(plan)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].HasCTE = hasCTE
				output[i].Plan = plan
			})
			require.Equalf(t, output[i].HasCTE, hasCTE,
				"case[%d] %q: HasCTE mismatch (got %v, want %v)", i, sql, hasCTE, output[i].HasCTE)
		}
	})
}

// planContainsCTE returns true when the EXPLAIN output includes a CTEFullScan
// row, which signals that the common_subplan_extract rule rewrote a duplicate
// branch into a shared CTE.
func planContainsCTE(plan []string) bool {
	for _, row := range plan {
		if strings.Contains(row, "CTEFullScan") {
			return true
		}
	}
	return false
}
