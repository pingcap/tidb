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
		// The MATCH is consumed by the path, not re-evaluated above it.
		require.NotRegexp(t, `Selection.*match_against`, plan)
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
	// The path competes on cost: a more selective condition on another index
	// wins, and the MATCH is then evaluated on the rows it returns.
	plan := explain("select id from t where match(body) against('+hello' in boolean mode) and k = 1")
	require.Contains(t, plan, "index:idx_k(k)", plan)
	require.NotContains(t, plan, "FullTextIndexScan", plan)
	require.Regexp(t, `Selection.*match_against`, plan)
	usesIndex("select id from t use index (idx_body) where match(body) against('+hello' in boolean mode) and k = 1", "idx_body(body)")
	// Each MATCH conjunct offers its own path; one is chosen.
	plan = explain("select id from t where match(body) against('+hello' in boolean mode) and match(title) against('世界' in boolean mode)")
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

	// Key columns ahead of the tokenized column: the path exists only when
	// every key column is pinned to one value, which the index then serves
	// in place of the table filter.
	tk.MustExec("create table tc (id int primary key, tenant_id int, region varchar(8), body text, fulltext index idx_tenant (tenant_id, body), fulltext index idx_region (tenant_id, region, body))")
	// Statistics let the index pinning more columns estimate fewer rows, and
	// make reading the postings of a rare term cheaper than scanning.
	values := make([]string, 0, 3000)
	for i := 1; i <= 3000; i++ {
		body := "world filler text"
		if i%50 == 0 {
			body = "hello world"
		}
		values = append(values, fmt.Sprintf("(%d, %d, '%s', '%s')", i, 7+i%3, []string{"eu", "us"}[i%2], body))
	}
	tk.MustExec("insert into tc values " + strings.Join(values, ","))
	tk.MustExec("analyze table tc")
	usesIndex("select id from tc where tenant_id = 7 and match(body) against('+hello' in boolean mode)", "idx_tenant(tenant_id, body)")
	plan = explain("select id from tc where tenant_id = 7 and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, "range:[7,7]", plan)
	require.NotContains(t, plan, "eq(test.tc.tenant_id", plan)
	usesIndex("select id from tc where tenant_id is null and match(body) against('+hello' in boolean mode)", "idx_tenant(tenant_id, body)")
	usesIndex("select id from tc where tenant_id = 7 and region = 'eu' and match(body) against('+hello' in boolean mode)", "idx_region(tenant_id, region, body)")
	plan = explain("select id from tc where tenant_id = 7 and region = 'eu' and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, `range:[7 "eu",7 "eu"]`, plan)
	require.Equal(t, 1, strings.Count(plan, "FullTextIndexScan"), plan)
	// A key column left unpinned, or pinned to several values, keeps the
	// scan; the index still authorises the MATCH.
	tk.MustQuery("select @@tidb_enable_local_match_against").Check(testkit.Rows("0"))
	scans("select id from tc where match(body) against('+hello' in boolean mode)")
	scans("select id from tc where region = 'eu' and match(body) against('+hello' in boolean mode)")
	scans("select id from tc where tenant_id in (7, 8) and match(body) against('+hello' in boolean mode)")
	scans("select id from tc where (tenant_id = 7 or tenant_id = 8) and match(body) against('+hello' in boolean mode)")
	scans("select id from tc where tenant_id > 7 and match(body) against('+hello' in boolean mode)")
	scans("select id from tc where tenant_id = 7 or match(body) against('+hello' in boolean mode)")
	usesIndex("select id from tc use index (idx_tenant) where tenant_id = 7 and region = 'eu' and match(body) against('+hello' in boolean mode)", "idx_tenant(tenant_id, body)")
	scans("select id from tc ignore index (idx_tenant, idx_region) where tenant_id = 7 and match(body) against('+hello' in boolean mode)")

	// The clustered handle ends every entry of the index, so conditions on
	// a leading prefix of its columns narrow each exact term's posting scan.
	// They show as the trailing range dimensions and, for a search of exact
	// terms, are served by the index in place of the table filter.
	tk.MustExec("create table td (tenant_id bigint, id bigint, body text, primary key (tenant_id, id) clustered, fulltext index idx_body (body))")
	usesIndex("select id from td where tenant_id = 42 and match(body) against('+hello' in boolean mode)", "idx_body(body)")
	plan = explain("select id from td where tenant_id = 42 and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, "range:[42,42]", plan)
	require.NotContains(t, plan, "eq(test.td.tenant_id", plan)
	plan = explain("select id from td where tenant_id in (42, 43) and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, "range:[42,42], [43,43]", plan)
	require.NotContains(t, plan, "in(test.td.tenant_id", plan)
	plan = explain("select id from td where tenant_id = 42 and id > 7 and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, "range:(42 7,42 +inf]", plan)
	require.NotContains(t, plan, "Selection", plan)
	// A prefix search reads every term with the prefix, which a handle range
	// cannot narrow, so the condition is also checked on the rows. Reading
	// the prefix across every tenant costs more than scanning the tenant.
	scans("select id from td where tenant_id = 42 and match(body) against('hel*' in boolean mode)")
	plan = explain("select id from td use index (idx_body) where tenant_id = 42 and match(body) against('hel*' in boolean mode)")
	require.Contains(t, plan, "FullTextIndexScan", plan)
	require.Contains(t, plan, "range:[42,42]", plan)
	require.Contains(t, plan, "eq(test.td.tenant_id, 42)", plan)
	// A condition on a later handle column alone selects no contiguous slice
	// of a term's postings, and a nonclustered primary key is not in the key.
	plan = explain("select id from td use index (idx_body) where id = 7 and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, "FullTextIndexScan", plan)
	require.NotContains(t, plan, "range:", plan)
	require.Contains(t, plan, "eq(test.td.id, 7)", plan)
	tk.MustExec("create table tnc (tenant_id bigint, id bigint, body text, primary key (tenant_id, id) nonclustered, fulltext index idx_body (body))")
	plan = explain("select id from tnc use index (idx_body) where tenant_id = 42 and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, "FullTextIndexScan", plan)
	require.NotContains(t, plan, "range:", plan)
	require.Contains(t, plan, "eq(test.tnc.tenant_id, 42)", plan)
	// An integer handle, alone and after pinned key columns. Without the
	// hint, reading the rows by their handles wins: the heuristics choose a
	// point lookup over every other path, including this one.
	for _, sql := range []string{
		"select id from t where id in (3, 4) and match(body) against('+hello' in boolean mode)",
		"select id from t where id = 3 and match(body) against('+hello' in boolean mode)",
	} {
		plan = explain(sql)
		require.Regexp(t, `Point_Get`, plan)
		require.Regexp(t, `Selection.*match_against`, plan)
	}
	plan = explain("select id from t use index (idx_body) where id in (3, 4) and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, "range:[3,3], [4,4]", plan)
	require.NotContains(t, plan, "in(test.t.id", plan)
	plan = explain("select id from tc use index (idx_tenant) where tenant_id = 7 and id = 1 and match(body) against('+hello' in boolean mode)")
	require.Contains(t, plan, "index:idx_tenant(tenant_id, body)", plan)
	require.Contains(t, plan, "range:[7 1,7 1]", plan)
	require.NotContains(t, plan, "Selection", plan)

	// The index competes on cost with the other paths. Reading the postings
	// of a rare term beats scanning the table and analyzing every document,
	// and beats a weakly selective index; the postings of a term in every
	// row lose to a selective condition on another index.
	tk.MustExec("create table tw (id int primary key, k int, body text, fulltext index idx_body (body), key idx_k (k))")
	values = values[:0]
	for i := 1; i <= 3000; i++ {
		body := "world filler text"
		if i%50 == 0 {
			body = "hello world"
		}
		values = append(values, fmt.Sprintf("(%d, %d, '%s')", i, i, body))
	}
	tk.MustExec("insert into tw values " + strings.Join(values, ","))
	tk.MustExec("analyze table tw")
	usesIndex("select id from tw where match(body) against('+hello' in boolean mode)", "idx_body(body)")
	usesIndex("select id from tw where k between 1 and 2000 and match(body) against('+hello' in boolean mode)", "idx_body(body)")
	plan = explain("select id from tw where k = 5 and match(body) against('+world' in boolean mode)")
	require.Contains(t, plan, "index:idx_k(k)", plan)
	require.NotContains(t, plan, "FullTextIndexScan", plan)
	require.Regexp(t, `Selection.*match_against`, plan)
	tk.MustQuery("select id from tw where k = 5 and match(body) against('+world' in boolean mode)").Check(testkit.Rows("5"))

	// A plan with the index is never cached: the search string is baked in.
	tk.MustExec("prepare stmt from 'select id from t where match(body) against(''+hello'' in boolean mode)'")
	tk.MustQuery("execute stmt")
	tk.MustQuery("execute stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}
