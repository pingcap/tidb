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

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestDual(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT,d INT);")
		testKit.MustQuery("explain format = 'plan_tree' select a from (select d as a from t where d = 0) k where k.a = 5").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("select a from (select d as a from t where d = 0) k where k.a = 5").Check(testkit.Rows())
		testKit.MustQuery("explain format = 'plan_tree' select a from (select 1+2 as a from t where d = 0) k where k.a = 5").Check(testkit.Rows(
			"Projection root  3->Column",
			"└─TableDual root  rows:0"))
		testKit.MustQuery("select a from (select 1+2 as a from t where d = 0) k where k.a = 5").Check(testkit.Rows())
		testKit.MustQuery("explain format = 'plan_tree' select * from t where d != null;").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("explain format = 'plan_tree' select * from t where d > null;").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("explain format = 'plan_tree' select * from t where d >= null;").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("explain format = 'plan_tree' select * from t where d < null;").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("explain format = 'plan_tree' select * from t where d <= null;").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("explain format = 'plan_tree' select * from t where d = null;").Check(testkit.Rows(
			"TableDual root  rows:0"))
	})
}

func TestSetOprEmptyChild(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("CREATE TABLE t1 (id INT);")
		testKit.MustExec("CREATE TABLE t2 (id INT);")
		testKit.MustExec("INSERT INTO t1 VALUES (1),(2),(3);")
		testKit.MustExec("INSERT INTO t2 VALUES (2),(3),(4);")

		// INTERSECT: an empty right side means no possible match, so the
		// whole result is empty and t1 should not be scanned.
		testKit.MustQuery("explain format = 'plan_tree' select id from t1 intersect select id from t2 where false").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("select id from t1 intersect select id from t2 where false").Check(testkit.Rows())
		// INTERSECT: an empty left side also means the whole result is empty.
		testKit.MustQuery("explain format = 'plan_tree' select id from t1 where false intersect select id from t2").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("select id from t1 where false intersect select id from t2").Check(testkit.Rows())
		// EXCEPT: an empty left side means the whole result is empty,
		// regardless of the right side, so t2 should not be scanned.
		testKit.MustQuery("explain format = 'plan_tree' select id from t1 where false except select id from t2").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("select id from t1 where false except select id from t2").Check(testkit.Rows())

		// Negative case: EXCEPT with an empty RIGHT side must NOT collapse to
		// empty. `A EXCEPT empty` equals A, since no row of A is excluded.
		testKit.MustQuery("explain format = 'plan_tree' select id from t1 except select id from t2 where false").CheckNotContain("TableDual root  rows:0")
		testKit.MustQuery("select id from t1 except select id from t2 where false").Sort().Check(testkit.Rows("1", "2", "3"))

		// Baseline: non-empty operands must still compute the real result.
		testKit.MustQuery("select id from t1 intersect select id from t2").Sort().Check(testkit.Rows("2", "3"))
		testKit.MustQuery("select id from t1 except select id from t2").Sort().Check(testkit.Rows("1"))

		// Chained set operators: only one branch is empty; the surviving
		// combination must still be computed correctly.
		testKit.MustQuery("select id from t1 intersect select id from t2 intersect select id from t2 where false").Check(testkit.Rows())
		testKit.MustQuery("select id from t1 except select id from t2 where false except select id from t2").Sort().Check(testkit.Rows("1"))

		// Chained set operators where the empty branch is nested in the INNER
		// join, not the outer one: `(t1 INTERSECT empty) INTERSECT t2`. The
		// inner join must fold to an empty TableDual first, and the outer
		// join must then be re-checked against that now-empty child so it
		// also folds, instead of leaving a real join that still scans t2.
		testKit.MustQuery("explain format = 'plan_tree' select id from t1 intersect select id from t2 where false intersect select id from t2").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("select id from t1 intersect select id from t2 where false intersect select id from t2").Check(testkit.Rows())
		testKit.MustQuery("explain format = 'plan_tree' select id from t1 where false except select id from t2 except select id from t2").Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery("select id from t1 where false except select id from t2 except select id from t2").Check(testkit.Rows())

		// A correlated EXISTS alongside an unrelated empty-child INTERSECT in
		// the same statement must not be affected by the new rule.
		testKit.MustExec("CREATE TABLE t3 (id INT);")
		testKit.MustExec("INSERT INTO t3 VALUES (1);")
		testKit.MustQuery(`select * from t3 where exists (select 1 from t1 where t1.id = t3.id)
			and t3.id in (select id from t1 intersect select id from t2 where false)`).Check(testkit.Rows())
		testKit.MustQuery(`select * from t3 where exists (select 1 from t1 where t1.id = t3.id)`).Check(testkit.Rows("1"))

		// Multi-column operand: buildDistinct groups by every column, so the
		// grouped-Aggregation unwrap in isStaticallyEmpty must hold beyond a
		// single-column schema too.
		testKit.MustExec("CREATE TABLE t4 (id INT, val INT);")
		testKit.MustExec("CREATE TABLE t5 (id INT, val INT);")
		testKit.MustExec("INSERT INTO t4 VALUES (1,10),(2,20);")
		testKit.MustQuery(`explain format = 'plan_tree' select id, val from t4 intersect select id, val from t5 where false`).Check(testkit.Rows(
			"TableDual root  rows:0"))
		testKit.MustQuery(`select id, val from t4 intersect select id, val from t5 where false`).Check(testkit.Rows())

		// Resolve columns by name through the derived-table alias to exercise
		// OutputNames on the collapsed dual, not just its Schema.
		testKit.MustQuery(`select s.id, s.val from
			((select id, val from t4) intersect (select id, val from t5 where false)) s`).Check(testkit.Rows())

		// The rule's optimizer flag is statement-wide (set whenever the query
		// contains INTERSECT/EXCEPT), so it walks the whole plan once
		// enabled. It is still restricted to set-operator joins: each join
		// LogicalJoin.FromSetOperator is only set by buildSemiJoinForSetOperator,
		// so an ordinary IN/EXISTS semi-join elsewhere in the same statement
		// is never rewritten by this rule even if it is independently empty.
		// See TestEliminateSemiJoinEmptyChildRequiresFromSetOperator in
		// pkg/planner/core for a direct plan-tree proof of that guard; SQL
		// alone cannot isolate it well because non-correlated IN decorrelates
		// to InnerJoin and OR-combined EXISTS becomes LeftOuterSemiJoin,
		// neither of which this rule ever touches in the first place. The
		// two queries below only check that results stay correct with both
		// forms present in one statement.
		testKit.MustQuery(`select * from t3 where exists (select 1 from t1 where false)
			and t3.id in (select id from t1 intersect select id from t2 where false)`).Check(testkit.Rows())
		testKit.MustQuery(`select * from t3 where exists (select 1 from t1)
			and t3.id in (select id from t1 intersect select id from t2 where false)`).Check(testkit.Rows())
		testKit.MustQuery(`select * from t3 where exists (select 1 from t1)`).Check(testkit.Rows("1"))
	})
}
