// Copyright 2022 PingCAP, Inc.
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

package issuetest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// It's a case for Columns in tableScan and indexScan with double reader
func TestIssue43461(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index b(b), index b_c(b, c)) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t")

	stmt, err := parser.New().ParseOneStmt("select * from t use index(b) where b > 1 order by b limit 1", "", "")
	require.NoError(t, err)

	nodeW := resolve.NewNodeW(stmt)
	p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	var idxLookUpPlan *core.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*core.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(base.PhysicalPlan).Children()[0]
	}
	require.True(t, ok)

	is := idxLookUpPlan.IndexPlans[0].(*core.PhysicalIndexScan)
	ts := idxLookUpPlan.TablePlans[0].(*core.PhysicalTableScan)

	require.NotEqual(t, is.Columns, ts.Columns)
}

func Test53726(t *testing.T) {
	// test for RemoveUnnecessaryFirstRow
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t7(c int); ")
	tk.MustExec("insert into t7 values (575932053), (-258025139);")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_8 8000.00 root  group by:Column#7, Column#8, funcs:firstrow(Column#7)->Column#3, funcs:firstrow(Column#8)->Column#4",
			"└─TableReader_9 8000.00 root  data:HashAgg_4",
			"  └─HashAgg_4 8000.00 cop[tikv]  group by:cast(test.t7.c, bigint(22) BINARY), cast(test.t7.c, decimal(10,0) BINARY), ",
			"    └─TableFullScan_7 10000.00 cop[tikv] table:t7 keep order:false, stats:pseudo"))

	tk.MustExec("analyze table t7 all columns")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#11, Column#12, funcs:firstrow(Column#11)->Column#3, funcs:firstrow(Column#12)->Column#4",
			"└─Projection_12 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#11, cast(test.t7.c, bigint(22) BINARY)->Column#12",
			"  └─TableReader_11 2.00 root  data:TableFullScan_10",
			"    └─TableFullScan_10 2.00 cop[tikv] table:t7 keep order:false"))
}

func TestIssue54535(t *testing.T) {
	// test for tidb_enable_inl_join_inner_multi_pattern system variable
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set session tidb_enable_inl_join_inner_multi_pattern='ON'")
	tk.MustExec("create table ta(a1 int, a2 int, a3 int, index idx_a(a1))")
	tk.MustExec("create table tb(b1 int, b2 int, b3 int, index idx_b(b1))")
	tk.MustExec("analyze table ta")
	tk.MustExec("analyze table tb")

	tk.MustQuery("explain SELECT /*+ inl_join(tmp) */ * FROM ta, (SELECT b1, COUNT(b3) AS cnt FROM tb GROUP BY b1, b2) as tmp where ta.a1 = tmp.b1").
		Check(testkit.Rows(
			"Projection_9 9990.00 root  test.ta.a1, test.ta.a2, test.ta.a3, test.tb.b1, Column#9",
			"└─IndexJoin_16 9990.00 root  inner join, inner:HashAgg_14, outer key:test.ta.a1, inner key:test.tb.b1, equal cond:eq(test.ta.a1, test.tb.b1)",
			"  ├─TableReader_43(Build) 9990.00 root  data:Selection_42",
			"  │ └─Selection_42 9990.00 cop[tikv]  not(isnull(test.ta.a1))",
			"  │   └─TableFullScan_41 10000.00 cop[tikv] table:ta keep order:false, stats:pseudo",
			"  └─HashAgg_14(Probe) 79840080.00 root  group by:test.tb.b1, test.tb.b2, funcs:count(Column#11)->Column#9, funcs:firstrow(test.tb.b1)->test.tb.b1",
			"    └─IndexLookUp_15 79840080.00 root  ",
			"      ├─Selection_12(Build) 9990.00 cop[tikv]  not(isnull(test.tb.b1))",
			"      │ └─IndexRangeScan_10 10000.00 cop[tikv] table:tb, index:idx_b(b1) range: decided by [eq(test.tb.b1, test.ta.a1)], keep order:false, stats:pseudo",
			"      └─HashAgg_13(Probe) 79840080.00 cop[tikv]  group by:test.tb.b1, test.tb.b2, funcs:count(test.tb.b3)->Column#11",
			"        └─TableRowIDScan_11 9990.00 cop[tikv] table:tb keep order:false, stats:pseudo"))
	// test for issues/55169
	tk.MustExec("create table t1(col_1 int, index idx_1(col_1));")
	tk.MustExec("create table t2(col_1 int, col_2 int, index idx_2(col_1));")
	tk.MustQuery("select /*+ inl_join(tmp) */ * from t1 inner join (select col_1, group_concat(col_2) from t2 group by col_1) tmp on t1.col_1 = tmp.col_1;").Check(testkit.Rows())
	tk.MustQuery("select /*+ inl_join(tmp) */ * from t1 inner join (select col_1, group_concat(distinct col_2 order by col_2) from t2 group by col_1) tmp on t1.col_1 = tmp.col_1;").Check(testkit.Rows())
}

func TestIssue53175(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`set @@sql_mode = default`)
	tk.MustQuery(`select @@sql_mode REGEXP 'ONLY_FULL_GROUP_BY'`).Check(testkit.Rows("1"))
	tk.MustContainErrMsg(`select * from t group by null`, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec(`create view v as select * from t group by null`)
	tk.MustContainErrMsg(`select * from v`, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec(`set @@sql_mode = ''`)
	tk.MustQuery(`select * from t group by null`)
	tk.MustQuery(`select * from v`)
}
