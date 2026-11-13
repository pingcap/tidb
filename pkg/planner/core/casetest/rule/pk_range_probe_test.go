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
	"github.com/stretchr/testify/require"
)

// fillPKRangeProbeTable inserts rows a = 1..n with b = a div 10.
func fillPKRangeProbeTable(tk *testkit.TestKit, table string, n int) {
	tk.MustExec("set @@cte_max_recursion_depth = 10000")
	tk.MustExec(fmt.Sprintf("insert into %s(a, b) with recursive s(a) as (select 1 union all select a+1 from s where a < %d) select a, a div 10 from s", table, n))
}

func explainString(tk *testkit.TestKit, sql string) string {
	var sb strings.Builder
	for _, row := range tk.MustQuery("explain format='brief' " + sql).Rows() {
		sb.WriteString(fmt.Sprintln(row...))
	}
	return sb.String()
}

// analyzeScan returns the actual rows of the table scan and the execution
// info of the table reader from EXPLAIN ANALYZE.
func analyzeScan(t *testing.T, tk *testkit.TestKit, sql string) (scanRows string, readerInfo string) {
	for _, row := range tk.MustQuery("explain analyze " + sql).Rows() {
		id := row[0].(string)
		switch {
		case strings.Contains(id, "TableReader"):
			readerInfo = row[5].(string)
		case strings.Contains(id, "TableFullScan") || strings.Contains(id, "TableRangeScan"):
			scanRows = row[2].(string)
		}
	}
	require.NotEmpty(t, scanRows, sql)
	return scanRows, readerInfo
}

func TestPKRangeProbeHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int primary key clustered, b int, c int, key idx_b(b))")
		fillPKRangeProbeTable(tk, "t", 100)

		query := "select /*+ pk_filter(t, idx_b), use_index(t, primary) */ a from t where b = 4"
		plain := "select /*+ use_index(t, primary) */ a from t where b = 4"
		withProbe := explainString(tk, query)
		require.Contains(t, withProbe, "pk_range_probe(idx_b)")
		// The probe doesn't change the plan.
		require.Equal(t, explainString(tk, plain), strings.ReplaceAll(withProbe, ", pk_range_probe(idx_b)", ""))

		tk.MustQuery(query).Sort().Check(testkit.Rows("40", "41", "42", "43", "44", "45", "46", "47", "48", "49"))
		scanRows, info := analyzeScan(t, tk, query)
		require.Equal(t, "10", scanRows)
		require.Contains(t, info, "pk_range_probe:{result:narrowed")

		// Without the hint or the variable, nothing is probed.
		scanRows, info = analyzeScan(t, tk, plain)
		require.Equal(t, "100", scanRows)
		require.NotContains(t, info, "pk_range_probe")

		// No row matches.
		query = "select /*+ pk_filter(t, idx_b), use_index(t, primary) */ a from t where b = 999"
		tk.MustQuery(query).Check(testkit.Rows())
		scanRows, info = analyzeScan(t, tk, query)
		require.Equal(t, "0", scanRows)
		require.Contains(t, info, "pk_range_probe:{result:empty")

		// The bounds intersect the scan's own range.
		query = "select /*+ pk_filter(t, idx_b), use_index(t, primary) */ a from t where b = 4 and a > 45"
		tk.MustQuery(query).Sort().Check(testkit.Rows("46", "47", "48", "49"))
		scanRows, _ = analyzeScan(t, tk, query)
		require.Equal(t, "4", scanRows)

		// An index that the conditions don't fully cover isn't probed.
		tk.MustExec("alter table t add index idx_bc(b, c)")
		require.NotContains(t, explainString(tk, "select /*+ pk_filter(t, idx_bc), use_index(t, primary) */ a from t where b = 4"), "pk_range_probe")
		require.Contains(t, explainString(tk, "select /*+ pk_filter(t), use_index(t, primary) */ a from t where b = 4"), "pk_range_probe(idx_b)")
		require.Contains(t, explainString(tk, "select /*+ pk_filter(), use_index(t, primary) */ a from t where b = 4"), "pk_range_probe(idx_b)")
	})
}

func TestPKRangeProbeHandleKinds(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_common, t_rowid, t_uniq")
		// Composite clustered key with a case-insensitive string column.
		tk.MustExec("create table t_common(k varchar(10) collate utf8mb4_general_ci, a int, b int, primary key(k, a) clustered, key idx_b(b))")
		tk.MustExec("insert into t_common(k, a, b) with recursive s(a) as (select 1 union all select a+1 from s where a < 100) select if(a < 50, 'A', 'b'), a, a div 10 from s")
		query := "select /*+ pk_filter(t_common, idx_b), use_index(t_common, primary) */ k, a from t_common where b = 4"
		tk.MustQuery(query).Sort().Check(testkit.Rows("A 40", "A 41", "A 42", "A 43", "A 44", "A 45", "A 46", "A 47", "A 48", "A 49"))
		scanRows, info := analyzeScan(t, tk, query)
		require.Equal(t, "10", scanRows)
		require.Contains(t, info, "result:narrowed")

		// A table without a clustered key is bounded by _tidb_rowid.
		tk.MustExec("create table t_rowid(a int, b int, key idx_b(b))")
		fillPKRangeProbeTable(tk, "t_rowid", 100)
		query = "select /*+ pk_filter(t_rowid, idx_b), use_index(t_rowid) */ a from t_rowid where b = 4"
		tk.MustQuery(query).Sort().Check(testkit.Rows("40", "41", "42", "43", "44", "45", "46", "47", "48", "49"))
		scanRows, info = analyzeScan(t, tk, query)
		require.Equal(t, "10", scanRows)
		require.Contains(t, info, "result:narrowed")

		// A unique index stores the handle in the value. A prefix index
		// matches more values than the condition, which is still a bound.
		tk.MustExec("create table t_uniq(a int primary key, b int, c varchar(20) collate utf8mb4_general_ci, unique key uk_b(b), key idx_c(c(2)))")
		tk.MustExec("insert into t_uniq(a, b, c) with recursive s(a) as (select 1 union all select a+1 from s where a < 100) select a, a, if(a between 20 and 30, concat('xy', a), 'zz') from s")
		query = "select /*+ pk_filter(t_uniq, uk_b), use_index(t_uniq, primary) */ a from t_uniq where b = 42"
		tk.MustQuery(query).Check(testkit.Rows("42"))
		scanRows, _ = analyzeScan(t, tk, query)
		require.Equal(t, "1", scanRows)
		query = "select /*+ pk_filter(t_uniq, idx_c), use_index(t_uniq, primary) */ a from t_uniq where c = 'XY25'"
		tk.MustQuery(query).Check(testkit.Rows("25"))
		scanRows, _ = analyzeScan(t, tk, query)
		require.Equal(t, "11", scanRows)
	})
}

func TestPKRangeProbeSessionVar(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_small, t_big")
		tk.MustExec("create table t_small(a int primary key, b int, key idx_b(b))")
		fillPKRangeProbeTable(tk, "t_small", 100)
		tk.MustExec("create table t_big(a int primary key, b int, key idx_b(b))")
		fillPKRangeProbeTable(tk, "t_big", 5000)
		tk.MustExec("analyze table t_small, t_big")

		tk.MustExec("set tidb_opt_generate_pk_filter = ON")
		// Too few rows to scan for the probes to pay off.
		require.NotContains(t, explainString(tk, "select /*+ use_index(t_small, primary) */ a from t_small where b = 4"), "pk_range_probe")
		require.Contains(t, explainString(tk, "select /*+ pk_filter(t_small), use_index(t_small, primary) */ a from t_small where b = 4"), "pk_range_probe")

		query := "select /*+ use_index(t_big, primary) */ count(*) from t_big where b = 123"
		require.Contains(t, explainString(tk, query), "pk_range_probe(idx_b)")
		tk.MustQuery(query).Check(testkit.Rows("10"))
		scanRows, _ := analyzeScan(t, tk, query)
		require.Equal(t, "10", scanRows)

		// A plan that doesn't scan the table isn't touched.
		require.NotContains(t, explainString(tk, "select a from t_big where b = 123"), "pk_range_probe")

		tk.MustExec("set tidb_opt_generate_pk_filter = OFF")
		require.NotContains(t, explainString(tk, query), "pk_range_probe")
	})
}

func TestPKRangeProbePlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, key idx_b(b))")
	fillPKRangeProbeTable(tk, "t", 100)

	tk.MustExec("prepare stmt from 'select /*+ pk_filter(t, idx_b), use_index(t, primary) */ a from t where b = ?'")
	tk.MustExec("set @b = 4")
	tk.MustQuery("execute stmt using @b").Sort().Check(testkit.Rows("40", "41", "42", "43", "44", "45", "46", "47", "48", "49"))
	tk.MustExec("set @b = 7")
	tk.MustQuery("execute stmt using @b").Sort().Check(testkit.Rows("70", "71", "72", "73", "74", "75", "76", "77", "78", "79"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @b = 999")
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @b = 0")
	tk.MustQuery("execute stmt using @b").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestPKRangeProbeSkipped(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, key idx_b(b))")
	tk.MustExec("create table t2(a int primary key, b int)")
	fillPKRangeProbeTable(tk, "t", 100)
	fillPKRangeProbeTable(tk, "t2", 100)

	// Locking reads and writes may read rows newer than the probes see.
	require.NotContains(t, explainString(tk, "select /*+ pk_filter(t, idx_b), use_index(t, primary) */ a from t where b = 4 for update"), "pk_range_probe")
	require.NotContains(t, explainString(tk, "update /*+ pk_filter(t, idx_b), use_index(t, primary) */ t set a = a where b = 4"), "pk_range_probe")
	// The conditions must be constants the executor can evaluate again.
	require.NotContains(t, explainString(tk, "select /*+ pk_filter(t, idx_b), use_index(t, primary) */ a from t where b = floor(rand() * 10)"), "pk_range_probe")
	// The inner side of an index join reads ranges built from outer rows.
	require.NotContains(t, explainString(tk, "select /*+ inl_join(t), pk_filter(t, idx_b) */ t.a from t2 join t on t.a = t2.a where t.b = 4"), "pk_range_probe")

	// The transaction's own writes are read through a union scan, which the
	// bounds would cut off.
	query := "select /*+ pk_filter(t, idx_b), use_index(t, primary) */ a from t where b = 4"
	tk.MustExec("begin")
	tk.MustExec("insert into t values (500, 4)")
	tk.MustExec("update t set b = 4 where a = 1")
	tk.MustQuery(query).Sort().Check(testkit.Rows("1", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "500"))
	tk.MustExec("rollback")

	// Rows committed by another session after the plan was cached are found.
	tk.MustExec("prepare stmt from 'select /*+ pk_filter(t, idx_b), use_index(t, primary) */ a from t where b = ?'")
	tk.MustExec("set @b = 4")
	tk.MustQuery("execute stmt using @b").Sort().Check(testkit.Rows("40", "41", "42", "43", "44", "45", "46", "47", "48", "49"))
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("insert into t values (600, 4)")
	tk.MustQuery("execute stmt using @b").Sort().Check(testkit.Rows("40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "600"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestPKRangeProbeHintWarnings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, key idx_b(b))")

	tk.MustQuery("explain select /*+ pk_filter(t, idx_nonexistent) */ * from t where b = 1")
	tk.MustQuery("show warnings").CheckContain("idx_nonexistent")
	tk.MustQuery("explain select /*+ pk_filter(t, primary) */ * from t where b = 1")
	tk.MustQuery("show warnings").CheckContain("pk_filter")
	tk.MustQuery("explain select /*+ pk_filter(no_such_table, idx_b) */ * from t where b = 1")
	tk.MustQuery("show warnings").CheckContain("no_such_table")
}
