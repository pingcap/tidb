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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestExplainAnalyzeRUFormat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")

	var input []struct {
		SQL string
	}
	var output []struct {
		SQL  string
		Rows [][]string
	}
	suiteData := core.GetExplainAnalyzeRUSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	require.Equal(t, len(input), len(output))

	toStringRows := func(rows [][]any) [][]string {
		stringRows := make([][]string, len(rows))
		for i, row := range rows {
			stringRows[i] = make([]string, len(row))
			for j, col := range row {
				stringRows[i][j] = col.(string)
			}
		}
		return stringRows
	}

	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt.SQL
			output[i].Rows = toStringRows(tk.MustQuery(tt.SQL).Rows())
		})
		require.Equal(t, tt.SQL, output[i].SQL)
		require.Equal(t, output[i].Rows, toStringRows(tk.MustQuery(tt.SQL).Rows()))
	}
}
