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

package scalarsubquery

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestExplainNonEvaledSubquery(t *testing.T) {
	var (
		input []struct {
			SQL              string
			IsExplainAnalyze bool
			HasErr           bool
		}
		output []struct {
			SQL   string
			Plan  []string
			Error string
		}
	)
	planSuiteData := GetPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int, c int)")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec("create table t3(a varchar(5), b varchar(5), c varchar(5))")
	tk.MustExec("set @@tidb_opt_enable_non_eval_scalar_subquery=true")

	cutExecutionInfoFromExplainAnalyzeOutput := func(rows [][]any) [][]any {
		// The columns are id, estRows, actRows, task type, access object, execution info, operator info, memory, disk
		// We need to cut the unstable output of execution info, memory and disk.
		for i := range rows {
			rows[i] = rows[i][:6] // cut the final memory and disk.
			rows[i] = append(rows[i][:5], rows[i][6:]...)
		}
		return rows
	}

	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts.SQL
			if ts.HasErr {
				err := tk.ExecToErr(ts.SQL)
				require.NotNil(t, err, fmt.Sprintf("Failed at case #%d", i))
				output[i].Error = err.Error()
				output[i].Plan = nil
			} else {
				rows := tk.MustQuery(ts.SQL).Rows()
				if ts.IsExplainAnalyze {
					rows = cutExecutionInfoFromExplainAnalyzeOutput(rows)
				}
				output[i].Plan = testdata.ConvertRowsToStrings(rows)
				output[i].Error = ""
			}
		})
		if ts.HasErr {
			err := tk.ExecToErr(ts.SQL)
			require.NotNil(t, err, fmt.Sprintf("Failed at case #%d", i))
		} else {
			rows := tk.MustQuery(ts.SQL).Rows()
			if ts.IsExplainAnalyze {
				rows = cutExecutionInfoFromExplainAnalyzeOutput(rows)
			}
			require.Equal(t,
				testdata.ConvertRowsToStrings(testkit.Rows(output[i].Plan...)),
				testdata.ConvertRowsToStrings(rows),
				fmt.Sprintf("Failed at case #%d, SQL: %v", i, ts.SQL),
			)
		}
	}
}
