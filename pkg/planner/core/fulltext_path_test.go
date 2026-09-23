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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestFullTextIndexPathPlanning covers when the planner offers the full-text
// index path for MATCH ... AGAINST and when it must not.
func TestFullTextIndexPathPlanning(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("FULLTEXT indexes are held by the columnar engine on the next-gen kernel")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, k int, body text, title text, fulltext index idx_body (body), fulltext index idx_title (title) with parser ngram, key idx_k (k))")

	explain := func(sql string) string {
		var sb strings.Builder
		for _, row := range tk.MustQuery("explain format = 'brief' " + sql).Rows() {
			sb.WriteString(fmt.Sprintln(row...))
		}
		return sb.String()
	}
	usesIndex := func(sql, index string) {
		plan := explain(sql)
		require.Contains(t, plan, "FullTextIndexScan(Build)", plan)
		require.Contains(t, plan, "index:"+index, plan)
		// The MATCH stays a residual filter above the reader.
		require.Contains(t, plan, "Selection", plan)
		require.Contains(t, plan, "match_against(", plan)
	}
	scans := func(sql string) {
		plan := explain(sql)
		require.NotContains(t, plan, "FullTextIndexScan", plan)
	}

	// The index authorises the MATCH without the session variable, and
	// supplies the analyzer the MATCH compiles with.
	tk.MustQuery("select @@tidb_enable_local_match_against").Check(testkit.Rows("0"))
	usesIndex("select id from t where match(body) against('+hello' in boolean mode)", "idx_body(body)")
	usesIndex("select id from t where match(title) against('数据库' in boolean mode)", "idx_title(title)")
	usesIndex("select id from t where match(body) against('+hello' in boolean mode) and k = 1", "idx_body(body)")
	// Each MATCH conjunct offers its own path; one is chosen.
	plan := explain("select id from t where match(body) against('+hello' in boolean mode) and match(title) against('世界' in boolean mode)")
	require.Contains(t, plan, "FullTextIndexScan(Build)", plan)
	require.Equal(t, 1, strings.Count(plan, "FullTextIndexScan"), plan)

	// Soundness: a MATCH in negative position or in one branch of an OR
	// selects exactly the rows that must not be dropped.
	scans("select id from t where not match(body) against('+hello' in boolean mode)")
	scans("select id from t where match(body) against('+hello' in boolean mode) or k = 1")
	// Only a boolean-mode MATCH is a predicate the index can serve.
	scans("select id from t where match(body) against('hello')")
	tk.MustContainErrMsg("explain select id from t where match(body) against('hello' with query expansion)", "MATCH...AGAINST with this modifier")
	// A MATCH over a column without an index, or naming several columns.
	tk.MustExec("set @@tidb_enable_local_match_against = 1")
	scans("select id from t where match(body, title) against('+hello' in boolean mode)")
	tk.MustExec("set @@tidb_enable_local_match_against = 0")

	// Hints in both syntaxes can forbid the index, and force it.
	scans("select id from t ignore index (idx_body) where match(body) against('+hello' in boolean mode)")
	scans("select id from t use index (idx_k) where match(body) against('+hello' in boolean mode)")
	scans("select /*+ ignore_index(t, idx_body) */ id from t where match(body) against('+hello' in boolean mode)")
	usesIndex("select id from t use index (idx_body) where match(body) against('+hello' in boolean mode)", "idx_body(body)")
	usesIndex("select /*+ use_index(t, idx_body) */ id from t where match(body) against('+hello' in boolean mode)", "idx_body(body)")

	// The index cannot keep an order, even one on its own column.
	plan = explain("select id from t where match(body) against('+hello' in boolean mode) order by body")
	require.Contains(t, plan, "Sort", plan)

	// An invisible index is not used unless the session opts in.
	tk.MustExec("alter table t alter index idx_body invisible")
	scans("select id from t where match(body) against('+hello' in boolean mode)")
	tk.MustExec("set @@tidb_opt_use_invisible_indexes = 1")
	usesIndex("select id from t where match(body) against('+hello' in boolean mode)", "idx_body(body)")
	tk.MustExec("set @@tidb_opt_use_invisible_indexes = 0")
	tk.MustExec("alter table t alter index idx_body visible")

	// A plan with the index is never cached: the search string is baked in.
	tk.MustExec("prepare stmt from 'select id from t where match(body) against(''+hello'' in boolean mode)'")
	tk.MustQuery("execute stmt")
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}
