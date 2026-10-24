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

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// firstRowPerRangeQueries compute MIN or MAX per namespace. The derived-table
// forms put the aggregation, as a root stream aggregation, a root hash
// aggregation or a pushed-down hash aggregation, on the inner side of an index
// join, which is the shape the rewrite handles. On these small tables the
// scalar subquery forms pull the aggregation above the join instead, so only
// their results are checked. The ORDER BY ... LIMIT 1 forms become MIN or MAX
// on the inner side of an index join, instead of an Apply, only when the groups
// are large.
var firstRowPerRangeQueries = []struct {
	query      string
	isMax      bool
	aggIsInner bool
	limitOne   bool
}{
	{"select /*+ inl_join(s@sel_2) */ t.ns_id, (select min(s.id) from %[1]s s where s.ns_id = t.ns_id) m from t where t.ns_id > 0 order by t.ns_id limit 100", false, false, false},
	{"select /*+ inl_join(s@sel_2) */ t.ns_id, (select max(s.id) from %[1]s s where s.ns_id = t.ns_id) m from t where t.ns_id > 0 order by t.ns_id limit 100", true, false, false},
	{"select /*+ inl_join(m) */ t.ns_id, m.m from t left join (select ns_id, min(id) m from %[1]s group by ns_id) m on t.ns_id = m.ns_id where t.ns_id > 0 order by t.ns_id limit 100", false, true, false},
	{"select /*+ inl_join(m) */ t.ns_id, m.m from t left join (select /*+ stream_agg() */ ns_id, min(id) m from %[1]s group by ns_id) m on t.ns_id = m.ns_id where t.ns_id > 0 order by t.ns_id limit 100", false, true, false},
	{"select /*+ inl_join(m) */ t.ns_id, m.m from t left join (select /*+ hash_agg() agg_to_cop() */ ns_id, max(id) m from %[1]s group by ns_id) m on t.ns_id = m.ns_id where t.ns_id > 0 order by t.ns_id limit 100", true, true, false},
	{"select t.ns_id, (select s.id from %[1]s s where s.ns_id = t.ns_id order by s.ns_id, s.id limit 1) m from t where t.ns_id > 0 order by t.ns_id limit 100", false, false, true},
	{"select t.ns_id, (select s.id from %[1]s s where s.ns_id = t.ns_id order by s.id desc limit 1) m from t where t.ns_id > 0 order by t.ns_id limit 100", true, false, true},
}

func checkFirstRowPerRange(t *testing.T, tk *testkit.TestKit, table string, wantRewrite bool, minRows, maxRows [][]any) {
	for _, c := range firstRowPerRangeQueries {
		q := fmt.Sprintf(c.query, table)
		plan := tk.MustQuery("explain format='brief' " + q).Rows()
		var planText strings.Builder
		for _, r := range plan {
			planText.WriteString(fmt.Sprintln(r...))
		}
		// The ORDER BY ... LIMIT 1 forms are an Apply unless rewritten, and then
		// an index join reading the first row per range.
		aggIsInner := c.aggIsInner || (c.limitOne && wantRewrite)
		require.Equal(t, wantRewrite && aggIsInner, strings.Contains(planText.String(), "first row per range"), "%s\n%s", q, planText.String())
		if aggIsInner {
			require.Contains(t, planText.String(), "IndexJoin", q)
		}
		if c.limitOne {
			require.Equal(t, !wantRewrite, strings.Contains(planText.String(), "Apply"), "%s\n%s", q, planText.String())
		}
		want := minRows
		if c.isMax {
			want = maxRows
		}
		tk.MustQuery(q).Check(want)
	}
}

func TestIndexJoinMinFirstRowPerRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (ns_id bigint primary key, v int)")
	tk.MustExec("insert into t values (1,1),(2,2),(3,3),(4,4),(5,5)")

	// Small groups: reading each group whole is cheaper, so the rewrite is skipped.
	tk.MustExec("create table s (ns_id bigint not null, id bigint not null, a int, primary key (ns_id, id) clustered)")
	tk.MustExec("insert into s values (1,10,0),(1,5,0),(1,7,0),(2,3,0),(2,9,0),(4,1,0),(4,2,0)")

	// Large groups: 300 rows in each of namespaces 1, 2 and 4, ids k*1000+1 .. k*1000+300.
	tk.MustExec("create table big (ns_id bigint not null, id bigint not null, a int, primary key (ns_id, id) clustered)")
	tk.MustExec("insert into big with recursive r(n) as (select 1 union all select n+1 from r where n < 300) " +
		"select k.ns_id, k.ns_id*1000 + r.n, 0 from r join (select 1 ns_id union all select 2 union all select 4) k")
	tk.MustExec("analyze table t, s, big")
	// Split namespace 1 across regions so its range becomes two cop tasks.
	tk.MustQuery("split table s by (1, 7), (2, 0)").Check(testkit.Rows("2 1"))
	tk.MustQuery("split table big by (1, 1150), (2, 0)").Check(testkit.Rows("2 1"))

	checkFirstRowPerRange(t, tk, "s", false,
		testkit.Rows("1 5", "2 3", "3 <nil>", "4 1", "5 <nil>"),
		testkit.Rows("1 10", "2 9", "3 <nil>", "4 2", "5 <nil>"))
	checkFirstRowPerRange(t, tk, "big", true,
		testkit.Rows("1 1001", "2 2001", "3 <nil>", "4 4001", "5 <nil>"),
		testkit.Rows("1 1300", "2 2300", "3 <nil>", "4 4300", "5 <nil>"))

	// The ORDER BY ... LIMIT 1 form stays an Apply over the Limit when it returns
	// another column, when another correlated condition would keep the Apply
	// anyway, and with NO_DECORRELATE.
	for _, c := range []struct {
		query string
		want  [][]any
	}{
		{"select t.ns_id, (select s.a from big s where s.ns_id = t.ns_id order by s.ns_id, s.id limit 1) m from t order by t.ns_id",
			testkit.Rows("1 0", "2 0", "3 <nil>", "4 0", "5 <nil>")},
		{"select t.ns_id, (select s.id from big s where s.ns_id = t.ns_id and s.id > t.v*1000 + t.v order by s.id limit 1) m from t order by t.ns_id",
			testkit.Rows("1 1002", "2 2003", "3 <nil>", "4 4005", "5 <nil>")},
		{"select t.ns_id, (select /*+ no_decorrelate() */ s.id from big s where s.ns_id = t.ns_id order by s.id limit 1) m from t order by t.ns_id",
			testkit.Rows("1 1001", "2 2001", "3 <nil>", "4 4001", "5 <nil>")},
	} {
		var planText strings.Builder
		for _, r := range tk.MustQuery("explain format='brief' " + c.query).Rows() {
			planText.WriteString(fmt.Sprintln(r...))
		}
		require.Contains(t, planText.String(), "Apply", c.query)
		require.NotContains(t, planText.String(), "Agg", c.query)
		tk.MustQuery(c.query).Check(c.want)
	}

	// ENUM is stored in the index by member number but MIN and MAX compare its
	// string value, so with members 'z','a','m' the first row in index order
	// holds 'z' while MIN is 'a'. Neither rewrite may apply. The LIMIT 1 form
	// sorts by member number, so it stays an Apply and returns 'z'.
	tk.MustExec("create table en (id bigint primary key auto_increment, ns_id bigint not null, e enum('z','a','m') not null, key k (ns_id, e))")
	tk.MustExec("insert into en (ns_id, e) with recursive r(n) as (select 1 union all select n+1 from r where n < 300) " +
		"select k.ns_id, elt(1 + r.n % 3, 'z', 'a', 'm') from r join (select 1 ns_id union all select 2 union all select 4) k")
	// The primary key's leading column g is fixed only by a constant, so it
	// isn't a join key and the index join couldn't read the first row per range.
	tk.MustExec("create table big2 (g int not null, ns_id bigint not null, id bigint not null, primary key (g, ns_id, id) clustered)")
	tk.MustExec("insert into big2 select 1, ns_id, id from big")
	tk.MustExec("analyze table en, big2")
	for _, c := range []struct {
		query string
		apply bool
		want  [][]any
	}{
		{"select /*+ inl_join(m) */ t.ns_id, m.m from t left join (select ns_id, min(e) m from en group by ns_id) m on t.ns_id = m.ns_id order by t.ns_id",
			false, testkit.Rows("1 a", "2 a", "3 <nil>", "4 a", "5 <nil>")},
		{"select /*+ inl_join(m) */ t.ns_id, m.m from t left join (select ns_id, max(e) m from en group by ns_id) m on t.ns_id = m.ns_id order by t.ns_id",
			false, testkit.Rows("1 z", "2 z", "3 <nil>", "4 z", "5 <nil>")},
		{"select t.ns_id, (select s.e from en s where s.ns_id = t.ns_id order by s.ns_id, s.e limit 1) m from t order by t.ns_id",
			true, testkit.Rows("1 z", "2 z", "3 <nil>", "4 z", "5 <nil>")},
		{"select t.ns_id, (select s.id from big2 s where s.g = 1 and s.ns_id = t.ns_id order by s.id limit 1) m from t order by t.ns_id",
			true, testkit.Rows("1 1001", "2 2001", "3 <nil>", "4 4001", "5 <nil>")},
	} {
		var planText strings.Builder
		for _, r := range tk.MustQuery("explain format='brief' " + c.query).Rows() {
			planText.WriteString(fmt.Sprintln(r...))
		}
		require.NotContains(t, planText.String(), "first row per range", c.query)
		if c.apply {
			require.Contains(t, planText.String(), "Apply", c.query)
			require.NotContains(t, planText.String(), "Agg", c.query)
		}
		tk.MustQuery(c.query).Check(c.want)
	}
}
