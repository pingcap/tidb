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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

// TestCommonSubplanExtract exercises the L0 detection-only landing of the
// common subplan extractor rule. Each input SQL is executed with the rule
// enabled; the captured output is the filtered `SHOW WARNINGS` rows that
// carry the `common_subplan_extract` note. An empty Notes slice means the
// rule deliberately did not fire (negative case).
func TestCommonSubplanExtract(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3")
		tk.MustExec("create table t1 (a int, b int, c int)")
		tk.MustExec("create table t2 (a int, b int, c int)")
		tk.MustExec("create table t3 (a int, b int, c int)")

		var input []string
		var output []struct {
			SQL   string
			Notes []string
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
			tk.MustExec(sql)
			notes := collectCommonSubplanNotes(tk)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Notes = notes
			})
			require.Equalf(t, output[i].Notes, notes,
				"case[%d] %q notes mismatch", i, sql)
		}
	})
}

// collectCommonSubplanNotes returns the `common_subplan_extract` note
// messages currently on the session's warning list, preserving insertion
// order. Other warnings are ignored.
func collectCommonSubplanNotes(tk *testkit.TestKit) []string {
	rows := tk.MustQuery("show warnings").Rows()
	var out []string
	for _, r := range rows {
		if len(r) < 3 {
			continue
		}
		level, _ := r[0].(string)
		msg, _ := r[2].(string)
		if !strings.EqualFold(level, "Note") {
			continue
		}
		if strings.Contains(msg, "common_subplan_extract") {
			out = append(out, fmt.Sprintf("%s", msg))
		}
	}
	return out
}
