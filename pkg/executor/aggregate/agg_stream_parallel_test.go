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

package aggregate_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestParallelStreamAggBasic exercises the executor-level parallel StreamAgg
// path end-to-end via SQL. The coverage is attributed to the aggregate
// package because this test file lives in pkg/executor/aggregate/.
func TestParallelStreamAggBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c varchar(20), index idx_a(a))")
	tk.MustExec("set tidb_init_chunk_size=1")
	tk.MustExec("set tidb_max_chunk_size=32")

	// Insert 500 rows across 100 groups.
	var buf strings.Builder
	for i := range 500 {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "(%d,%d,'val%d')", i%100, i, i)
	}
	tk.MustExec("insert into t values " + buf.String())

	sqls := []string{
		"select /*+ stream_agg() */ count(b), sum(b) from t use index(idx_a) group by a order by a",
		"select /*+ stream_agg() */ max(b), min(b), avg(b) from t use index(idx_a) group by a order by a",
		"select /*+ stream_agg() */ count(distinct b) from t use index(idx_a) group by a order by a",
		"select /*+ stream_agg() */ group_concat(c order by c) from t use index(idx_a) group by a order by a",
	}

	for _, sql := range sqls {
		// Serial baseline.
		tk.MustExec("set @@tidb_streamagg_concurrency = 1")
		expected := tk.MustQuery(sql).Sort().Rows()

		// Parallel execution.
		for _, con := range []int{2, 4} {
			tk.MustExec(fmt.Sprintf("set @@tidb_streamagg_concurrency = %d", con))
			got := tk.MustQuery(sql).Sort().Rows()
			require.Equal(t, len(expected), len(got), "sql=%s con=%d", sql, con)
			for i := range expected {
				require.Equal(t, expected[i], got[i], "sql=%s con=%d row=%d", sql, con, i)
			}
		}
	}
}

// TestParallelStreamAggEdge exercises edge cases: empty table, single group,
// many single-row groups, and LIMIT (early close).
func TestParallelStreamAggEdge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_init_chunk_size=1")
	tk.MustExec("set tidb_max_chunk_size=32")
	tk.MustExec("set tidb_streamagg_concurrency=4")

	// Empty table.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	tk.MustQuery("select /*+ stream_agg() */ count(b) from t use index(idx_a) group by a order by a").
		Check(testkit.Rows())

	// Single group.
	tk.MustExec("insert into t values(1,10),(1,20),(1,30)")
	tk.MustQuery("select /*+ stream_agg() */ sum(b) from t use index(idx_a) group by a order by a").
		Check(testkit.Rows("60"))

	// Many single-row groups.
	tk.MustExec("truncate table t")
	for i := range 100 {
		tk.MustExec(fmt.Sprintf("insert into t values(%d,%d)", i, i*10))
	}
	tk.MustExec("set tidb_streamagg_concurrency=1")
	expected := tk.MustQuery("select /*+ stream_agg() */ sum(b), count(b) from t use index(idx_a) group by a order by a").Sort().Rows()
	tk.MustExec("set tidb_streamagg_concurrency=4")
	got := tk.MustQuery("select /*+ stream_agg() */ sum(b), count(b) from t use index(idx_a) group by a order by a").Sort().Rows()
	require.Equal(t, expected, got)

	// LIMIT (early close).
	result := tk.MustQuery("select /*+ stream_agg() */ sum(b) from t use index(idx_a) group by a order by a limit 5").Rows()
	require.Equal(t, 5, len(result))
}

// TestParallelStreamAggNoShuffle verifies that the executor-level parallel
// path is used (agg_worker in runtime stats) and Shuffle is NOT present.
func TestParallelStreamAggNoShuffle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	tk.MustExec("set tidb_init_chunk_size=1")
	tk.MustExec("set tidb_max_chunk_size=32")

	var buf strings.Builder
	for i := range 200 {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "(%d,%d)", i%50, i)
	}
	tk.MustExec("insert into t values " + buf.String())

	tk.MustExec("set @@tidb_streamagg_concurrency = 4")
	sql := "select /*+ stream_agg() */ sum(b) from t use index(idx_a) group by a order by a"

	rows := tk.MustQuery("explain analyze " + sql).Rows()
	foundAggWorker := false
	for _, row := range rows {
		line := fmt.Sprintf("%v", row)
		if strings.Contains(line, "agg_worker") {
			foundAggWorker = true
		}
		require.False(t, strings.Contains(line, "Shuffle"), "unexpected Shuffle in plan")
	}
	require.True(t, foundAggWorker, "expected agg_worker in runtime stats")
}
