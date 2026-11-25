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

package rule

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func runJoinReorderTestData(t *testing.T, tk *testkit.TestKit, name, cascades string) {
	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning []string
	}
	joinReorderSuiteData := GetJoinReorderSuiteData()
	joinReorderSuiteData.LoadTestCasesByName(name, t, &input, &output, cascades)
	require.Equal(t, len(input), len(output))
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' " + input[i]).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'plan_tree' " + input[i]).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

func TestJoinReorderProjectionRand(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table a(k double not null)")
	tk.MustExec("create table b(id int not null)")
	tk.MustExec("create table c(id int not null)")
	tk.MustExec("insert into a values (1),(2)")
	tk.MustExec("insert into b values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
	tk.MustExec("insert into c select * from b")
	tk.MustExec("analyze table a,b,c")
	for _, advanced := range []string{"off", "on"} {
		for _, threshold := range []string{"0", "10"} {
			for _, throughProj := range []string{"off", "on"} {
				t.Run("advanced="+advanced+"/threshold="+threshold+"/throughProj="+throughProj, func(t *testing.T) {
					tk.MustExec("set tidb_opt_enable_advanced_join_reorder=" + advanced)
					tk.MustExec("set tidb_opt_join_reorder_threshold=" + threshold)
					tk.MustExec("set tidb_opt_join_reorder_through_proj=" + throughProj)
					rows := tk.MustQuery(`select a.k, dt.r, dt.k from a join (
						select r.id, r.r, r.r + 1 as k
						from (select id, floor(rand(1) * 2) as r from b) r
						join c on r.id = c.id
					) dt on a.k = dt.k`).Rows()
					require.Len(t, rows, 10)
					for _, row := range rows {
						require.Equal(t, row[0], row[2], "join equality violated: %v", row)
					}
				})
			}
		}
	}
}

func TestJoinReorderSuite(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		t.Run("PlainCases", func(t *testing.T) {
			// test the global/session variable tidb_opt_enable_hash_join being set to no
			t.Run("TestOptEnableHashJoin", func(t *testing.T) {
				testKit.MustExec("drop table if exists t1, t2, t3, t4;")
				testKit.MustExec("set tidb_opt_enable_hash_join=off")
				defer testKit.MustExec("set tidb_opt_enable_hash_join=on")
				testKit.MustExec("create table t1(a int, b int, key(a));")
				testKit.MustExec("create table t2(a int, b int, key(a));")
				testKit.MustExec("create table t3(a int, b int, key(a));")
				testKit.MustExec("create table t4(a int, b int, key(a));")
				runJoinReorderTestData(t, testKit, "TestOptEnableHashJoin", cascades)
			})

			t.Run("TestJoinOrderHint4DynamicPartitionTable", func(t *testing.T) {
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
				testKit.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6;")
				testKit.MustExec("set tidb_opt_enable_hash_join=on")
				testKit.MustExec(`create table t(a int, b int) partition by hash(a) partitions 3`)
				testKit.MustExec(`create table t1(a int, b int) partition by hash(a) partitions 4`)
				testKit.MustExec(`create table t2(a int, b int) partition by hash(a) partitions 5`)
				testKit.MustExec(`create table t3(a int, b int) partition by hash(b) partitions 3`)
				testKit.MustExec(`create table t4(a int, b int) partition by hash(a) partitions 4`)
				testKit.MustExec(`create table t5(a int, b int) partition by hash(a) partitions 5`)
				testKit.MustExec(`create table t6(a int, b int) partition by hash(b) partitions 3`)

				testKit.MustExec(`set @@tidb_partition_prune_mode="dynamic"`)
				testKit.MustExec("set @@tidb_enable_outer_join_reorder=true")
				runJoinReorderTestData(t, testKit, "TestJoinOrderHint4DynamicPartitionTable", cascades)
			})

			t.Run("TestLeadingHintInapplicableKeepsOtherConds", func(t *testing.T) {
				testKit.MustExec("set @@tidb_enable_outer_join_reorder=true")
				testKit.MustExec("drop table if exists t0_lh, t1_lh, t2_lh, t3_lh;")
				testKit.MustExec("create table t0_lh(k0 int, k1 int, k2 int);")
				testKit.MustExec("create table t1_lh(k0 int, k1 int, k2 int);")
				testKit.MustExec("create table t2_lh(k0 int, k1 int, k2 int);")
				testKit.MustExec("create table t3_lh(k0 int, k1 int, k2 int);")

				testKit.MustQuery("explain format = 'plan_tree' " +
					"select /*+ leading(t0_lh, t2_lh, t3_lh, t1_lh) */ t1_lh.k0 " +
					"from t0_lh right join t2_lh on (t0_lh.k1 = t2_lh.k1) " +
					"join t3_lh on (t0_lh.k2 = t3_lh.k2 and t2_lh.k1 < t3_lh.k2) " +
					"left join t1_lh on (t0_lh.k0 <=> t1_lh.k0);").
					CheckContain("lt(test.t2_lh.k1, test.t3_lh.k2)")
				testKit.MustQuery("show warnings").CheckContain("leading hint is inapplicable")
			})

			t.Run("TestLeadingHintWithNonEqJoinUnderOuterJoin", func(t *testing.T) {
				testKit.MustExec("set @@tidb_enable_outer_join_reorder=true")
				testKit.MustExec("drop table if exists t1_56513, t2_56513, t3_56513;")
				testKit.MustExec("create table t1_56513(a int, b int, c int);")
				testKit.MustExec("create table t2_56513(a int, b int, c int);")
				testKit.MustExec("create table t3_56513(a int, b int, c int);")

				orCond := "or(eq(test.t1_56513.a, test.t2_56513.a), eq(test.t1_56513.a, test.t2_56513.b))"

				testKit.MustQuery("explain format = 'plan_tree' " +
					"select * from t1_56513 " +
					"join t2_56513 on (t1_56513.a = t2_56513.a or t1_56513.a = t2_56513.b) " +
					"left join t3_56513 on t1_56513.a = t3_56513.b;").
					CheckContain(orCond)

				plan132 := testKit.MustQuery("explain format = 'plan_tree' " +
					"select /*+ leading(t1_56513, t3_56513, t2_56513) */ * from t1_56513 " +
					"join t2_56513 on (t1_56513.a = t2_56513.a or t1_56513.a = t2_56513.b) " +
					"left join t3_56513 on t1_56513.a = t3_56513.b;")
				plan132.CheckContain(orCond)
				testKit.MustQuery("show warnings").CheckNotContain("leading hint is inapplicable")

				plan123 := testKit.MustQuery("explain format = 'plan_tree' " +
					"select /*+ leading(t1_56513, t2_56513, t3_56513) */ * from t1_56513 " +
					"join t2_56513 on (t1_56513.a = t2_56513.a or t1_56513.a = t2_56513.b) " +
					"left join t3_56513 on t1_56513.a = t3_56513.b;")
				plan123.CheckContain(orCond)
				testKit.MustQuery("show warnings").CheckNotContain("leading hint is inapplicable")
				require.NotEqual(t, plan132.Rows(), plan123.Rows(), caller)
			})

			t.Run("TestOuterJoinReorderNullExtendedNonEqSafety", func(t *testing.T) {
				testKit.MustExec("drop table if exists t1_66213, t2_66213, t3_66213;")
				testKit.MustExec("create table t1_66213(a int);")
				testKit.MustExec("create table t2_66213(a int, b int);")
				testKit.MustExec("create table t3_66213(b int);")
				testKit.MustExec("insert into t1_66213 values (1), (2);")
				testKit.MustExec("insert into t2_66213 values (1, 5);")
				testKit.MustExec("insert into t3_66213 values (5);")
				testKit.MustExec("analyze table t1_66213, t2_66213, t3_66213;")

				query := "select t1_66213.a, t2_66213.b, t3_66213.b from t1_66213 " +
					"left join t2_66213 on t1_66213.a = t2_66213.a " +
					"join t3_66213 on (t2_66213.b is null or t2_66213.b = t3_66213.b) " +
					"order by t1_66213.a, t2_66213.b, t3_66213.b"
				expectRows := testkit.Rows("1 5 5", "2 <nil> 5")

				testKit.MustExec("set @@tidb_enable_outer_join_reorder=off")
				testKit.MustQuery(query).Check(expectRows)
				testKit.MustExec("set @@tidb_enable_outer_join_reorder=on")
				testKit.MustQuery(query).Check(expectRows)

				testKit.MustQuery("select /*+ leading(t2_66213, t3_66213, t1_66213) */ " +
					"t1_66213.a, t2_66213.b, t3_66213.b from t1_66213 " +
					"left join t2_66213 on t1_66213.a = t2_66213.a " +
					"join t3_66213 on (t2_66213.b is null or t2_66213.b = t3_66213.b) " +
					"order by t1_66213.a, t2_66213.b, t3_66213.b").Check(expectRows)
				testKit.MustQuery("show warnings").CheckContain("leading hint is inapplicable")
			})
		})
	})

	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		t.Run("DomainCases", func(t *testing.T) {
			t.Run("TestJoinOrderHint4TiFlash", func(t *testing.T) {
				testKit.MustExec("drop table if exists t, t1, t2, t3;")
				testKit.MustExec("create table t(a int, b int, key(a));")
				testKit.MustExec("create table t1(a int, b int, key(a));")
				testKit.MustExec("create table t2(a int, b int, key(a));")
				testKit.MustExec("create table t3(a int, b int, key(a));")
				testKit.MustExec("create table t4(a int, b int, key(a));")
				testKit.MustExec("create table t5(a int, b int, key(a));")
				testKit.MustExec("create table t6(a int, b int, key(a));")
				testKit.MustExec("set @@tidb_enable_outer_join_reorder=true")

				testkit.SetTiFlashReplica(t, dom, "test", "t")
				testkit.SetTiFlashReplica(t, dom, "test", "t1")
				testkit.SetTiFlashReplica(t, dom, "test", "t2")
				testkit.SetTiFlashReplica(t, dom, "test", "t3")
				testkit.SetTiFlashReplica(t, dom, "test", "t4")
				testkit.SetTiFlashReplica(t, dom, "test", "t5")
				testkit.SetTiFlashReplica(t, dom, "test", "t6")

				testKit.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
				defer testKit.MustExec("set @@tidb_allow_mpp=0; set @@tidb_enforce_mpp=0;")
				runJoinReorderTestData(t, testKit, "TestJoinOrderHint4TiFlash", cascades)
			})

			t.Run("TestJoinOrderHint4NestedLeading", func(t *testing.T) {
				testKit.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6;")
				testKit.MustExec("set @@tidb_allow_mpp=0; set @@tidb_enforce_mpp=0;")
				testKit.MustExec("create table t(a int, b int, key(a));")
				testKit.MustExec("create table t1(a int, b int, key(a));")
				testKit.MustExec("create table t2(a int, b int, key(a));")
				testKit.MustExec("create table t3(a int, b int, key(a));")
				testKit.MustExec("create table t4(a int, b int, key(a));")
				testKit.MustExec("create table t5(a int, b int, key(a));")
				testKit.MustExec("create table t6(a int, b int, key(a));")
				runJoinReorderTestData(t, testKit, "TestJoinOrderHint4NestedLeading", cascades)
			})

			t.Run("TestJoinOrderHint4NestedLeadingPK", func(t *testing.T) {
				testKit.MustExec("drop table if exists t1, t2, t3, t4;")
				testKit.MustExec("set @@tidb_allow_mpp=0; set @@tidb_enforce_mpp=0;")
				testKit.MustExec("create table t1(a int not null, b int, key(a));")
				testKit.MustExec("create table t2(a int not null, b int, key(a));")
				testKit.MustExec("create table t3(a int not null, b int not null, primary key(a));")
				testKit.MustExec("create table t4(a int not null, b int not null, primary key(b));")
				runJoinReorderTestData(t, testKit, "TestJoinOrderHint4NestedLeadingPK", cascades)
			})
		})
	})
}
func TestGreedyOutputRowsAlternativeRound(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_opt_enable_advanced_join_reorder = on")
	tk.MustExec("set tidb_opt_join_reorder_threshold = 0")
	tk.MustExec("set tidb_enable_cascades_planner = off")

	columnID := regexp.MustCompile(`Column#\d+`)
	explain := func(query string) string {
		rows := testdata.ConvertRowsToStrings(tk.MustQuery("explain format='plan_tree' " + query).Rows())
		return columnID.ReplaceAllString(strings.Join(rows, "\n"), "Column#")
	}
	plan := func(alternative, query string) string {
		tk.MustExec("set tidb_opt_enable_alternative_logical_plans = " + alternative)
		return explain(query)
	}
	// roundRuns reports whether optimizing query attempts the greedy-output-rows round.
	roundRuns := func(t *testing.T, query string) (runs bool) {
		t.Run("round", func(t *testing.T) {
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/failIfAlternativeLogicalPlanRoundTriggered",
				fmt.Sprintf("return(%q)", "greedy-output-rows:"+query))
			err := tk.ExecToErr(query)
			if err != nil {
				require.ErrorContains(t, err, "unexpected alternative logical plan round")
			}
			runs = err != nil
		})
		return runs
	}

	t.Run("aggregation over a large join drives index lookups", func(t *testing.T) {
		tk.MustExec("create database jr_inflate")
		tk.MustExec("use jr_inflate")
		tk.MustExec("create table members (id bigint primary key, v int not null)")
		tk.MustExec("create table memberships (id bigint primary key, group_id bigint not null, member_id bigint not null, " +
			"member_type varchar(32) not null, key idx_group(group_id, member_id, member_type))")
		tk.MustExec("create table principals (id bigint primary key, owner_type varchar(32) not null, owner_id bigint not null, " +
			"key idx_owner(owner_type, owner_id))")
		tk.MustExec("create table roles (id bigint primary key, principal_type varchar(32) not null, principal_id bigint not null, " +
			"key idx_principal(principal_id, principal_type))")
		for _, name := range []string{"members", "memberships", "principals", "roles"} {
			require.NoError(t, testkit.LoadTableStats("inflate_"+name+".json", dom))
		}
		query := func(group int) string {
			return fmt.Sprintf("select r.id from roles r join principals p on r.principal_id = p.id "+
				"join (select distinct m.id from members m join memberships g on m.id = g.member_id "+
				"where g.group_id = %d and g.member_type = 'M') x on p.owner_id = x.id "+
				"where r.principal_type = 'P' and p.owner_type = 'M'", group)
		}

		// Group 12345 has 10 members, but by default its aggregation is costed with
		// the 1,000,000 member rows below it, so it is joined last and roles is scanned.
		small := query(12345)
		original := plan("off", small)
		require.Contains(t, original, "TableFullScan")
		alternative := plan("on", small)
		require.NotContains(t, alternative, "TableFullScan")
		require.Contains(t, alternative, "table:r, index:idx_principal(principal_id, principal_type)")
		require.True(t, roundRuns(t, small))
		// A LEADING hint that leaves one other input fixes the order, so the round is skipped.
		require.False(t, roundRuns(t, strings.Replace(small, "select ", "select /*+ leading(r, p) */ ", 1)))
		// A SET_VAR hint, which a binding can carry, enables the round for one statement.
		hinted := strings.Replace(small, "select ", "select /*+ SET_VAR(tidb_opt_enable_alternative_logical_plans=ON) */ ", 1)
		require.Equal(t, alternative, plan("off", hinted))

		// Group 0 has 50,000 members, so members is read in full in either order.
		big := query(0)
		for _, alternative := range []string{"off", "on"} {
			require.Contains(t, plan(alternative, big), "TableFullScan cop[tikv] table:m keep order:true")
		}

		// Only greedy join reorder of the advanced implementation arms the round.
		tk.MustExec("set tidb_opt_join_reorder_threshold = 10")
		require.False(t, roundRuns(t, small))
		tk.MustExec("set tidb_opt_join_reorder_threshold = 0")
		tk.MustExec("set tidb_opt_enable_advanced_join_reorder = off")
		require.False(t, roundRuns(t, small))
		tk.MustExec("set tidb_opt_enable_advanced_join_reorder = on")
	})

	t.Run("aggregation over a large table drives index probes", func(t *testing.T) {
		tk.MustExec("create database jr_repro")
		tk.MustExec("use jr_repro")
		tk.MustExec("create table anchors (k int primary key, v int not null)")
		tk.MustExec("create table fanout (id int primary key, k int not null, v int not null, index ix_k(k))")
		tk.MustExec("create table agg_input (id int primary key, k int not null, v int not null)")
		for _, name := range []string{"anchors", "fanout", "agg_input"} {
			require.NoError(t, testkit.LoadTableStats("greedy_"+name+".json", dom))
		}
		query := "select sum(a.v + b.v + x.mx) from anchors a join fanout b on a.k = b.k " +
			"join (select k, max(v) as mx from agg_input group by k) x on x.k = a.k"
		original := plan("off", query)
		alternative := plan("on", query)
		require.NotEqual(t, original, alternative)
		require.Contains(t, alternative, "table:b, index:ix_k(k)")
		// Base tables cost the same either way, so the round is skipped.
		require.False(t, roundRuns(t, "select sum(a.v + b.v + c.v) from anchors a join fanout b on a.k = b.k "+
			"join agg_input c on c.k = a.k"))
		// These aggregations are costed above their output rows, so the round runs.
		// They rank the same way under both costings, so the round builds the same
		// plan, and the plan of round 1 is kept.
		aggs := "select count(*) from (select k, max(v) as mx from agg_input group by k) x " +
			"join (select k, max(v) as mx from agg_input group by k) y on x.k = y.k " +
			"join (select k, max(v) as mx from agg_input group by k) z on y.k = z.k"
		require.True(t, roundRuns(t, aggs))
		require.Equal(t, plan("off", aggs), plan("on", aggs))
		// The round raises the same join hint conflicts, and only the warnings of
		// the chosen plan are kept.
		hinted := "select /*+ hash_join(x) merge_join(y) */ count(*) from (select k, max(v) as mx from agg_input group by k) x " +
			"join (select k, max(v) as mx from agg_input group by k) y on x.k = y.k " +
			"join (select k, max(v) as mx from agg_input group by k) z on y.k = z.k"
		offPlan := plan("off", hinted)
		offWarnings := tk.MustQuery("show warnings").Rows()
		require.NotEmpty(t, offWarnings)
		require.Equal(t, offPlan, plan("on", hinted))
		require.Equal(t, offWarnings, tk.MustQuery("show warnings").Rows())

		// A cached plan is only reused under the setting it was built with.
		tk.MustExec("prepare st from '" + query + "'")
		for _, alternative := range []string{"off", "on", "off"} {
			tk.MustExec("set tidb_opt_enable_alternative_logical_plans = " + alternative)
			tk.MustExec("execute st")
			tk.MustExec("execute st")
			require.True(t, tk.Session().GetSessionVars().FoundInPlanCache, alternative)
			process := tk.Session().ShowProcess()
			tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: []*sessmgr.ProcessInfo{process}})
			cached := testdata.ConvertRowsToStrings(tk.MustQuery(fmt.Sprintf("explain for connection %d", process.ID)).Rows())
			require.Equal(t, alternative == "on", strings.Contains(strings.Join(cached, "\n"), "table:b, index:ix_k(k)"), alternative)
		}
	})
}
