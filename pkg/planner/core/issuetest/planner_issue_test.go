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

	p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	var idxLookUpPlan *core.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*core.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(core.PhysicalPlan).Children()[0]
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

	tk.MustExec("analyze table t7")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#13, Column#14, funcs:firstrow(Column#11)->Column#3, funcs:firstrow(Column#12)->Column#4",
			"└─Projection_12 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#11, cast(test.t7.c, bigint(22) BINARY)->Column#12, cast(test.t7.c, decimal(10,0) BINARY)->Column#13, cast(test.t7.c, bigint(22) BINARY)->Column#14",
			"  └─TableReader_11 2.00 root  data:TableFullScan_10",
			"    └─TableFullScan_10 2.00 cop[tikv] table:t7 keep order:false"))
}

func TestJoinReorderWithAddSelection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`create table t0(vkey integer, c3 varchar(0));`)
	tk.MustExec(`create table t1(vkey integer, c10 integer);`)
	tk.MustExec(`create table t2(c12 integer, c13 integer, c14 varchar(0), c15 double);`)
	tk.MustExec(`create table t3(vkey varchar(0), c20 integer);`)
	tk.MustQuery(`explain format='brief' select 0 from t2 join(t3 join t0 a on 0) left join(t1 b left join t1 c on 0) on(c20 = b.vkey) on(c13 = a.vkey) join(select c14 d from(t2 join t3 on c12 = vkey)) e on(c3 = d) where nullif(c15, case when(c.c10) then 0 end);`).Check(testkit.Rows(
		`Projection 0.00 root  0->Column#26`,
		`└─HashJoin 0.00 root  inner join, equal:[eq(Column#27, Column#28)]`,
		`  ├─HashJoin(Build) 0.00 root  inner join, equal:[eq(test.t0.c3, test.t2.c14)]`,
		`  │ ├─Selection(Build) 0.00 root  if(eq(test.t2.c15, cast(case(test.t1.c10, 0), double BINARY)), NULL, test.t2.c15)`,
		`  │ │ └─HashJoin 0.00 root  left outer join, equal:[eq(test.t3.c20, test.t1.vkey)]`,
		`  │ │   ├─HashJoin(Build) 0.00 root  inner join, equal:[eq(test.t0.vkey, test.t2.c13)]`,
		`  │ │   │ ├─TableDual(Build) 0.00 root  rows:0`,
		`  │ │   │ └─TableReader(Probe) 9990.00 root  data:Selection`,
		`  │ │   │   └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.c13))`,
		`  │ │   │     └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`,
		`  │ │   └─HashJoin(Probe) 9990.00 root  CARTESIAN left outer join`,
		`  │ │     ├─TableDual(Build) 0.00 root  rows:0`,
		`  │ │     └─TableReader(Probe) 9990.00 root  data:Selection`,
		`  │ │       └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.vkey))`,
		`  │ │         └─TableFullScan 10000.00 cop[tikv] table:b keep order:false, stats:pseudo`,
		`  │ └─Projection(Probe) 9990.00 root  test.t2.c14, cast(test.t2.c12, double BINARY)->Column#27`,
		`  │   └─TableReader 9990.00 root  data:Selection`,
		`  │     └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.c14))`,
		`  │       └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`,
		`  └─Projection(Probe) 10000.00 root  cast(test.t3.vkey, double BINARY)->Column#28`,
		`    └─TableReader 10000.00 root  data:TableFullScan`,
		`      └─TableFullScan 10000.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
}

func TestOnlyFullGroupCantFeelUnaryConstant(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int);")
		testKit.MustQuery("select a,min(a) from t where a=-1;").Check(testkit.Rows("<nil> <nil>"))
		testKit.MustQuery("select a,min(a) from t where -1=a;").Check(testkit.Rows("<nil> <nil>"))
	})
}
