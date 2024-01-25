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

package explain

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestCheckActRowsWithUnistore(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableCollectExecutionInfo = true
	})

	store := testkit.CreateMockStore(t)
	// testSuite1 use default mockstore which is unistore
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t_unistore_act_rows")
	tk.MustExec("create table t_unistore_act_rows(a int, b int, index(a, b))")
	tk.MustExec("insert into t_unistore_act_rows values (1, 0), (1, 0), (2, 0), (2, 1)")
	tk.MustExec("analyze table t_unistore_act_rows")
	tk.MustExec("set @@tidb_merge_join_concurrency= 5;")

	time.Sleep(time.Second * 5)
	type testStruct struct {
		sql      string
		expected []string
	}

	tests := []testStruct{
		{
			sql:      "select * from t_unistore_act_rows",
			expected: []string{"4", "4"},
		},
		{
			sql:      "select * from t_unistore_act_rows where a > 1",
			expected: []string{"2", "2"},
		},
		{
			sql:      "select * from t_unistore_act_rows where a > 1 and b > 0",
			expected: []string{"1", "1", "2"},
		},
		{
			sql:      "select b from t_unistore_act_rows",
			expected: []string{"4", "4"},
		},
		{
			sql:      "select * from t_unistore_act_rows where b > 0",
			expected: []string{"1", "1", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows",
			expected: []string{"1", "1", "1", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows group by a",
			expected: []string{"2", "2", "2", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows group by b",
			expected: []string{"2", "4", "4"},
		},
		{
			sql:      "with cte(a) as (select a from t_unistore_act_rows) select (select 1 from cte limit 1) from cte;",
			expected: []string{"4", "1", "1", "1", "4", "4", "4", "4", "4"},
		},
		{
			sql:      "select a, row_number() over (partition by b) from t_unistore_act_rows;",
			expected: []string{"4", "4", "4", "4", "4", "4", "4"},
		},
		{
			sql:      "select /*+ merge_join(t1, t2) */ * from t_unistore_act_rows t1 join t_unistore_act_rows t2 on t1.b = t2.b;",
			expected: []string{"10", "10", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4"},
		},
	}

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	for _, test := range tests {
		checkActRows(t, tk, test.sql, test.expected)
	}
}

func checkActRows(t *testing.T, tk *testkit.TestKit, sql string, expected []string) {
	actRowsCol := 2
	rows := tk.MustQuery("explain analyze " + sql).Rows()
	require.Equal(t, len(expected), len(rows))
	for id, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}

		require.Equal(t, expected[id], strs[actRowsCol], fmt.Sprintf("error comparing %s", sql))
	}
}
