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
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
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

func TestAdvancedJoinReorderThroughProjection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a decimal(20,4), b int)")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("create table t3 (a int, b int)")
	tk.MustExec("set tidb_opt_enable_advanced_join_reorder=on")
	query := "select * from t3, t2, t1 where t3.a=t1.a and t2.b=t1.b"
	hinted := "select /*+ leading(t1,t2,t3) */ * from t3, t2, t1 where t3.a=t1.a and t2.b=t1.b"
	for _, threshold := range []string{"0", "10"} {
		t.Run("threshold="+threshold, func(t *testing.T) {
			tk.MustExec("set tidb_opt_join_reorder_threshold=" + threshold)
			tk.MustExec("set tidb_opt_join_reorder_through_proj=off")
			tk.MustQuery("explain format='brief' " + query).CheckContain("CARTESIAN")
			tk.MustExec("set tidb_opt_join_reorder_through_proj=on")
			tk.MustQuery("explain format='brief' " + query).CheckNotContain("CARTESIAN")
			if threshold == "0" {
				tk.MustQuery("explain format='brief' " + hinted).CheckNotContain("CARTESIAN")
				tk.MustQuery("show warnings").Check(testkit.Rows())
			}
		})
	}
	tk.MustExec("insert into t1 values (1,10),(2,20),(2,20),(3.5,30),(null,40),(4,null)")
	tk.MustExec("insert into t2 values (100,10),(200,20),(300,30),(400,40),(500,null)")
	tk.MustExec("insert into t3 values (1,1000),(2,2000),(3,3000),(null,4000),(4,5000)")
	for _, threshold := range []string{"0", "10"} {
		tk.MustExec("set tidb_opt_join_reorder_threshold=" + threshold)
		tk.MustQuery(query).Sort().Check(testkit.Rows("1 1000 100 10 1.0000 10", "2 2000 200 20 2.0000 20", "2 2000 200 20 2.0000 20"))
	}

	tk.MustExec("create table u1(a int)")
	tk.MustExec("create table u2(a int, b int)")
	tk.MustExec("create table u3(a int)")
	tk.MustExec("insert into u1 values (1),(2)")
	tk.MustExec("insert into u2 values (1,5),(3,null)")
	tk.MustExec("insert into u3 values (0),(1),(3),(5)")
	tk.MustExec("set tidb_enable_outer_join_reorder=on")
	for _, threshold := range []string{"0", "10"} {
		tk.MustExec("set tidb_opt_join_reorder_threshold=" + threshold)
		for _, through := range []string{"off", "on"} {
			tk.MustExec("set tidb_opt_join_reorder_through_proj=" + through)
			// Derived outputs and residual predicates must use the original definitions.
			tk.MustQuery("select d.k,d.v,t1.a from (select t3.a+0 k,t2.b+0 v from t3,t2) d " +
				"join t1 on d.k=t1.a and d.v=t1.b where d.k+d.v>10").Sort().Check(
				testkit.Rows("1 10 1.0000", "2 20 2.0000", "2 20 2.0000"))
			// A derived value computed on the nullable side must itself become NULL.
			tk.MustQuery("select x.a,d.k from u1 x left join " +
				"(select u2.a,ifnull(u2.b,0) k from u2 join u3 on u2.a=u3.a) d on x.a=d.a").Sort().Check(
				testkit.Rows("1 5", "2 <nil>"))
			tk.MustQuery("select x.a,d.k from " +
				"(select u2.a,ifnull(u2.b,0) k from u2 join u3 on u2.a=u3.a) d right join u1 x on x.a=d.a").Sort().Check(
				testkit.Rows("1 5", "2 <nil>"))
			// An expression above a nullable input must observe the manufactured NULL.
			tk.MustQuery("select d.a,d.k,z.a from " +
				"(select x.a,ifnull(y.b,0) k from u1 x left join u2 y on x.a=y.a) d " +
				"join u3 z on d.k=z.a").Sort().Check(testkit.Rows("1 5 5", "2 0 0"))
			// Inlining is safe on the preserved side of an outer join.
			tk.MustQuery("select d.k,z.a from " +
				"(select x.a+1 k from u1 x join u2 y on x.a=y.a) d " +
				"left join u3 z on d.k=z.a").Check(testkit.Rows("2 <nil>"))
		}
	}
	// Referencing a derived table by alias in LEADING must preserve its boundary.
	tk.MustExec("set tidb_opt_join_reorder_threshold=0")
	tk.MustQuery("select /*+ leading(t1,d) */ d.k,d.v,t1.a from " +
		"(select t3.a+0 k,t2.b+0 v from t3,t2) d join t1 on d.k=t1.a and d.v=t1.b").Sort().Check(
		testkit.Rows("1 10 1.0000", "2 20 2.0000", "2 20 2.0000"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	newGreedyTestKit := func(t *testing.T) *testkit.TestKit {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set tidb_opt_enable_advanced_join_reorder=on, tidb_opt_join_reorder_threshold=0")
		return tk
	}
	t.Run("evaluate advisory lock expression once", func(t *testing.T) {
		tk := newGreedyTestKit(t)
		tk.MustExec("create table lock_names(a int not null, n varchar(32) not null)")
		tk.MustExec("create table lock_keys(a int not null)")
		tk.MustExec("insert into lock_names values (1,'join_reorder_lock')")
		tk.MustExec("insert into lock_keys values (1)")
		for _, through := range []string{"off", "on"} {
			tk.MustExec("set tidb_opt_join_reorder_through_proj=" + through)
			tk.MustQuery("select get_lock('join_reorder_lock',0)").Check(testkit.Rows("1"))
			tk.MustQuery("select d.k from (select ifnull(release_lock(n.n),0) k " +
				"from lock_names n join lock_keys x on n.a=x.a) d join lock_keys y on d.k=y.a").Check(testkit.Rows("1"))
		}
	})

	t.Run("leading alias across query blocks", func(t *testing.T) {
		tk := newGreedyTestKit(t)
		tk.MustExec("create table leading_keys(a int not null)")
		tk.MustExec("insert into leading_keys values (1),(2)")
		for _, through := range []string{"off", "on"} {
			tk.MustExec("set tidb_opt_join_reorder_through_proj=" + through)
			tk.MustQuery("select /*+ leading(x,d) */ x.a,d.k from leading_keys x join " +
				"(select z.a+1 k from (select a from leading_keys limit 10) e " +
				"join leading_keys z on e.a=z.a) d on x.a=d.k").Check(testkit.Rows("2 2"))
			tk.MustQuery("show warnings").Check(testkit.Rows())
		}
	})
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
