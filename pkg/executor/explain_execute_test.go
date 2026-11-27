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

package executor_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestExplainExecutePreparedStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table ta (a varchar(16), b int, c int, key idx_ab(a, b))")
	tk.MustExec("insert into ta values ('a', 1, 10), ('a', 5, 20), ('b', 3, 30)")

	// 1. EXPLAIN EXECUTE with USING user variables and without USING.
	tk.MustExec("prepare stmt_sel from 'select * from ta where a = ? and b >= ? and b <= ?'")
	tk.MustExec("set @a = 'a', @b = 2, @c = 10")

	explainRows := tk.MustQuery("explain execute stmt_sel using @a, @b, @c").Rows()
	// EXPLAIN without ANALYZE should not use or populate plan cache unless format='plan_cache'.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	expectedRows := tk.MustQuery("explain select * from ta where a = 'a' and b >= 2 and b <= 10").Rows()
	require.Equal(t, len(expectedRows), len(explainRows))
	joined := flattenExplainRows(explainRows)
	require.Contains(t, joined, "IndexRangeScan")
	require.Contains(t, joined, "idx_ab")

	// 2. EXPLAIN ANALYZE EXECUTE returns actual execution stats.
	analyzeRows := tk.MustQuery("explain analyze execute stmt_sel using @a, @b, @c").Rows()
	require.NotEmpty(t, analyzeRows)
	require.Equal(t, 9, len(analyzeRows[0]))
	// Only row ('a', 5, 20) matches b in [2, 10], so root operator actRows should be "1".
	require.Equal(t, "1", analyzeRows[0][2])

	// 3. No USING parameters.
	tk.MustExec("prepare stmt_noparam from 'select count(*) from ta'")
	noParamRows := tk.MustQuery("explain execute stmt_noparam").Rows()
	require.NotEmpty(t, noParamRows)
	noParamAnalyzeRows := tk.MustQuery("explain analyze execute stmt_noparam").Rows()
	require.NotEmpty(t, noParamAnalyzeRows)
	require.Equal(t, "1", noParamAnalyzeRows[0][2])

	// 4. Other EXPLAIN FORMAT options.
	briefRows := tk.MustQuery("explain format = 'brief' execute stmt_sel using @a, @b, @c").Rows()
	require.NotEmpty(t, briefRows)
	hintRows := tk.MustQuery("explain format = 'hint' execute stmt_sel using @a, @b, @c").Rows()
	require.Len(t, hintRows, 1)
	require.NotEmpty(t, hintRows[0][0])

	// 5. PointGet fast-plan prepared query.
	tk.MustExec("create table tp (id int primary key, v int)")
	tk.MustExec("insert into tp values (1, 100), (2, 200)")
	tk.MustExec("prepare stmt_pg from 'select * from tp where id = ?'")
	tk.MustExec("set @id = 1")
	pgRows := tk.MustQuery("explain execute stmt_pg using @id").Rows()
	require.Contains(t, flattenExplainRows(pgRows), "Point_Get")
	pgAnalyzeRows := tk.MustQuery("explain analyze execute stmt_pg using @id").Rows()
	require.NotEmpty(t, pgAnalyzeRows)
	require.Equal(t, "1", pgAnalyzeRows[0][2])

	// 6. DML: EXPLAIN EXECUTE does not mutate rows; EXPLAIN ANALYZE EXECUTE executes & commits.
	tk.MustExec("prepare stmt_upd from 'update tp set v = v + 1 where id = ?'")
	updExplain := tk.MustQuery("explain execute stmt_upd using @id").Rows()
	require.Contains(t, flattenExplainRows(updExplain), "Update")
	tk.MustQuery("select v from tp where id = 1").Check(testkit.Rows("100"))

	updAnalyze := tk.MustQuery("explain analyze execute stmt_upd using @id").Rows()
	require.NotEmpty(t, updAnalyze)
	tk.MustQuery("select v from tp where id = 1").Check(testkit.Rows("101"))

	// 7. EXPLAIN FORMAT = 'plan_cache' EXECUTE respects prepared plan cache.
	tk.MustExec("set @@tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("execute stmt_sel using @a, @b, @c")
	tk.MustQuery("explain format = 'plan_cache' execute stmt_sel using @a, @b, @c").Rows()
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Non-plan_cache EXPLAIN still reports 0 for last_plan_from_cache.
	tk.MustQuery("explain execute stmt_sel using @a, @b, @c").Rows()
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// 8. Error cases: missing stmt, wrong param count, non-explainable prepared stmt.
	require.Error(t, tk.ExecToErr("explain execute no_such_stmt"))
	require.Error(t, tk.ExecToErr("explain execute stmt_sel using @a"))

	tk.MustExec("prepare stmt_set from 'set @x = 99'")
	require.Error(t, tk.ExecToErr("explain execute stmt_set"))
	require.Error(t, tk.ExecToErr("explain analyze execute stmt_set"))
	tk.MustQuery("select @x").Check(testkit.Rows("<nil>"))

	tk.MustExec("prepare stmt_ddl from 'create table should_not_exist (a int)'")
	require.Error(t, tk.ExecToErr("explain analyze execute stmt_ddl"))
	tk.MustQuery("show tables like 'should_not_exist'").Check(testkit.Rows())
}

func flattenExplainRows(rows [][]any) string {
	var b strings.Builder
	for _, r := range rows {
		for _, c := range r {
			fmt.Fprintf(&b, "%v ", c)
		}
		b.WriteByte('\n')
	}
	return b.String()
}
