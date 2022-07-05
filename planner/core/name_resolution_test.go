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

package core_test

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/hint"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestSomeOldCorrelatedAggCases(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_new_name_resolution=1")
	defer tk.MustExec("set @@tidb_enable_new_name_resolution=0")

	// we should keep the plan tree format as old ones. (the only thing changed should be the unique id)
	// the rest test will be accompanied by origin explain output.
	tk.MustExec("create table t(a int)")
	// +------------------------------+----------+-----------+---------------+---------------------------------+
	// | id                           | estRows  | task      | access object | operator info                   |
	// +------------------------------+----------+-----------+---------------+---------------------------------+
	// | HashJoin_11                  | 1.00     | root      |               | CARTESIAN left outer join       |
	// | ├─TableDual_28(Build)        | 1.00     | root      |               | rows:1                          |
	// | └─StreamAgg_24(Probe)        | 1.00     | root      |               | funcs:count(Column#8)->Column#3 |
	// |   └─TableReader_25           | 1.00     | root      |               | data:StreamAgg_16               |
	// |     └─StreamAgg_16           | 1.00     | cop[tikv] |               | funcs:count(test.t.a)->Column#8 |
	// |       └─TableFullScan_23     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo  |
	// +------------------------------+----------+-----------+---------------+---------------------------------+
	tk.MustQuery("explain select (select count(a)) from t;").Check(testkit.Rows(
		"HashJoin_12 1.00 root  CARTESIAN left outer join",
		"├─TableDual_29(Build) 1.00 root  rows:1",
		"└─StreamAgg_25(Probe) 1.00 root  funcs:count(Column#8)->Column#3",
		"  └─TableReader_26 1.00 root  data:StreamAgg_17",
		"    └─StreamAgg_17 1.00 cop[tikv]  funcs:count(test.t.a)->Column#8",
		"      └─TableFullScan_24 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// +----------------------------------+----------+-----------+---------------+---------------------------------+
	// | id                               | estRows  | task      | access object | operator info                   |
	// +----------------------------------+----------+-----------+---------------+---------------------------------+
	// | Projection_13                    | 1.00     | root      |               | Column#8                        |
	// | └─Apply_15                       | 1.00     | root      |               | CARTESIAN left outer join       |
	// |   ├─HashAgg_16(Build)            | 1.00     | root      |               | funcs:count(test.t.a)->Column#5 |
	// |   │ └─TableReader_19             | 10000.00 | root      |               | data:TableFullScan_18           |
	// |   │   └─TableFullScan_18         | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo  |
	// |   └─Projection_22(Probe)         | 1.00     | root      |               | Column#5                        |
	// |     └─Limit_23                   | 1.00     | root      |               | offset:0, count:1               |
	// |       └─TableReader_27           | 1.00     | root      |               | data:Limit_26                   |
	// |         └─Limit_26               | 1.00     | cop[tikv] |               | offset:0, count:1               |
	// |           └─TableFullScan_25     | 1.00     | cop[tikv] | table:t       | keep order:false, stats:pseudo  |
	// +----------------------------------+----------+-----------+---------------+---------------------------------+
	tk.MustQuery("explain select (select count(n.a) from t limit 1) from t n;").Check(testkit.Rows(
		"Projection_14 1.00 root  Column#6",
		"└─Apply_16 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_17(Build) 1.00 root  funcs:count(test.t.a)->Column#5",
		"  │ └─TableReader_20 10000.00 root  data:TableFullScan_19",
		"  │   └─TableFullScan_19 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_23(Probe) 1.00 root  Column#5",
		"    └─Limit_24 1.00 root  offset:0, count:1",
		"      └─TableReader_28 1.00 root  data:Limit_27",
		"        └─Limit_27 1.00 cop[tikv]  offset:0, count:1",
		"          └─TableFullScan_26 1.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// +------------------------------------+----------+-----------+---------------+---------------------------------+
	// | id                                 | estRows  | task      | access object | operator info                   |
	// +------------------------------------+----------+-----------+---------------+---------------------------------+
	// | Projection_14                      | 1.00     | root      |               | Column#8                        |
	// | └─Apply_16                         | 1.00     | root      |               | CARTESIAN left outer join       |
	// |   ├─HashAgg_17(Build)              | 1.00     | root      |               | funcs:count(test.t.a)->Column#5 |
	// |   │ └─TableReader_20               | 10000.00 | root      |               | data:TableFullScan_19           |
	// |   │   └─TableFullScan_19           | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo  |
	// |   └─Projection_23(Probe)           | 1.00     | root      |               | 1->Column#8                     |
	// |     └─Limit_24                     | 1.00     | root      |               | offset:0, count:1               |
	// |       └─TableReader_29             | 1.00     | root      |               | data:Limit_28                   |
	// |         └─Limit_28                 | 1.00     | cop[tikv] |               | offset:0, count:1               |
	// |           └─Selection_27           | 1.00     | cop[tikv] |               | gt(Column#5, 1)                 |
	// |             └─TableFullScan_26     | 1.25     | cop[tikv] | table:t       | keep order:false, stats:pseudo  |
	// +------------------------------------+----------+-----------+---------------+---------------------------------+
	tk.MustQuery("explain select (select 1 from t where count(n.a) > 1 limit 1) from t n;").Check(testkit.Rows(
		"Projection_15 1.00 root  Column#5",
		"└─Apply_17 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_18(Build) 1.00 root  funcs:count(test.t.a)->Column#6",
		"  │ └─TableReader_21 10000.00 root  data:TableFullScan_20",
		"  │   └─TableFullScan_20 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_24(Probe) 1.00 root  1->Column#5",
		"    └─Limit_25 1.00 root  offset:0, count:1",
		"      └─TableReader_30 1.00 root  data:Limit_29",
		"        └─Limit_29 1.00 cop[tikv]  offset:0, count:1",
		"          └─Selection_28 1.00 cop[tikv]  gt(Column#6, 1)",
		"            └─TableFullScan_27 1.25 cop[tikv] table:t keep order:false, stats:pseudo"))

	// +----------------------------------+----------+-----------+---------------+--------------------------------+
	// | id                               | estRows  | task      | access object | operator info                  |
	// +----------------------------------+----------+-----------+---------------+--------------------------------+
	// | Projection_15                    | 1.00     | root      |               | Column#8                       |
	// | └─Apply_17                       | 1.00     | root      |               | CARTESIAN left outer join      |
	// |   ├─HashAgg_18(Build)            | 1.00     | root      |               | funcs:count(1)->Column#16      |
	// |   │ └─TableReader_21             | 10000.00 | root      |               | data:TableFullScan_20          |
	// |   │   └─TableFullScan_20         | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo |
	// |   └─Projection_24(Probe)         | 1.00     | root      |               | 1->Column#8                    |
	// |     └─Limit_25                   | 1.00     | root      |               | offset:0, count:1              |
	// |       └─TableReader_29           | 1.00     | root      |               | data:Limit_28                  |
	// |         └─Limit_28               | 1.00     | cop[tikv] |               | offset:0, count:1              |
	// |           └─TableFullScan_27     | 1.00     | cop[tikv] | table:t       | keep order:false, stats:pseudo |
	// +----------------------------------+----------+-----------+---------------+--------------------------------+
	tk.MustQuery("explain select (select 1 from t order by count(n.a) limit 1) from t n;").Check(testkit.Rows(
		"Projection_16 1.00 root  Column#5",
		"└─Apply_18 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_19(Build) 1.00 root  funcs:count(1)->Column#14",
		"  │ └─TableReader_22 10000.00 root  data:TableFullScan_21",
		"  │   └─TableFullScan_21 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_25(Probe) 1.00 root  1->Column#5",
		"    └─Limit_26 1.00 root  offset:0, count:1",
		"      └─TableReader_30 1.00 root  data:Limit_29",
		"        └─Limit_29 1.00 cop[tikv]  offset:0, count:1",
		"          └─TableFullScan_28 1.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	// +------------------------------------+----------+-----------+---------------+---------------------------------+
	// | id                                 | estRows  | task      | access object | operator info                   |
	// +------------------------------------+----------+-----------+---------------+---------------------------------+
	// | Projection_15                      | 1.00     | root      |               | Column#8                        |
	// | └─Apply_17                         | 1.00     | root      |               | CARTESIAN left outer join       |
	// |   ├─HashAgg_18(Build)              | 1.00     | root      |               | funcs:count(test.t.a)->Column#5 |
	// |   │ └─TableReader_21               | 10000.00 | root      |               | data:TableFullScan_20           |
	// |   │   └─TableFullScan_20           | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo  |
	// |   └─Projection_24(Probe)           | 1.00     | root      |               | 1->Column#8                     |
	// |     └─Limit_25                     | 1.00     | root      |               | offset:0, count:1               |
	// |       └─TableReader_30             | 1.00     | root      |               | data:Limit_29                   |
	// |         └─Limit_29                 | 1.00     | cop[tikv] |               | offset:0, count:1               |
	// |           └─Selection_28           | 1.00     | cop[tikv] |               | gt(Column#5, 1)                 |
	// |             └─TableFullScan_27     | 1.25     | cop[tikv] | table:t       | keep order:false, stats:pseudo  |
	// +------------------------------------+----------+-----------+---------------+---------------------------------+
	// this case can be de-correlated. (pull the selection up since select list is constant)
	tk.MustQuery("explain select (select 1 from t having count(n.a) > 1 limit 1) from t n").Check(testkit.Rows(
		"Projection_16 1.00 root  Column#5",
		"└─Apply_18 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_19(Build) 1.00 root  funcs:count(test.t.a)->Column#6",
		"  │ └─TableReader_22 10000.00 root  data:TableFullScan_21",
		"  │   └─TableFullScan_21 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_25(Probe) 1.00 root  1->Column#5",
		"    └─Limit_26 1.00 root  offset:0, count:1",
		"      └─TableReader_31 1.00 root  data:Limit_30",
		"        └─Limit_30 1.00 cop[tikv]  offset:0, count:1",
		"          └─Selection_29 1.00 cop[tikv]  gt(Column#6, 1)",
		"            └─TableFullScan_28 1.25 cop[tikv] table:t keep order:false, stats:pseudo"))

	// +------------------------------+----------+-----------+---------------+---------------------------------+
	// | id                           | estRows  | task      | access object | operator info                   |
	// +------------------------------+----------+-----------+---------------+---------------------------------+
	// | HashJoin_15                  | 1.00     | root      |               | CARTESIAN left outer join       |
	// | ├─TableDual_32(Build)        | 1.00     | root      |               | rows:1                          |
	// | └─StreamAgg_28(Probe)        | 1.00     | root      |               | funcs:count(Column#9)->Column#4 |
	// |   └─TableReader_29           | 1.00     | root      |               | data:StreamAgg_20               |
	// |     └─StreamAgg_20           | 1.00     | cop[tikv] |               | funcs:count(test.t.a)->Column#9 |
	// |       └─TableFullScan_27     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo  |
	// +------------------------------+----------+-----------+---------------+---------------------------------+
	tk.MustQuery("explain select (select cnt from (select count(a) as cnt) n) from t;").Check(testkit.Rows(
		"HashJoin_13 1.00 root  CARTESIAN left outer join",
		"├─TableDual_30(Build) 1.00 root  rows:1",
		"└─StreamAgg_26(Probe) 1.00 root  funcs:count(Column#8)->Column#3",
		"  └─TableReader_27 1.00 root  data:StreamAgg_18",
		"    └─StreamAgg_18 1.00 cop[tikv]  funcs:count(test.t.a)->Column#8",
		"      └─TableFullScan_25 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// These aggregates should be evaluated in sub-query.
	// +--------------------------------+----------+-----------+---------------+-------------------------------------+
	// | id                             | estRows  | task      | access object | operator info                       |
	// +--------------------------------+----------+-----------+---------------+-------------------------------------+
	// | Projection_9                   | 10000.00 | root      |               | Column#10                           |
	// | └─Apply_11                     | 10000.00 | root      |               | CARTESIAN left outer join           |
	// |   ├─TableReader_13(Build)      | 10000.00 | root      |               | data:TableFullScan_12               |
	// |   │ └─TableFullScan_12         | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo      |
	// |   └─HashAgg_14(Probe)          | 1.00     | root      |               | funcs:count(Column#11)->Column#10   |
	// |     └─Projection_27            | 10000.00 | root      |               | plus(test.t.a, test.t.a)->Column#11 |
	// |       └─TableReader_20         | 10000.00 | root      |               | data:TableFullScan_19               |
	// |         └─TableFullScan_19     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo      |
	// +--------------------------------+----------+-----------+---------------+-------------------------------------+
	tk.MustQuery("explain select (select count(a + n.a) from t) from t n;").Check(testkit.Rows(
		"Projection_11 10000.00 root  Column#5",
		"└─Apply_13 10000.00 root  CARTESIAN left outer join",
		"  ├─TableReader_15(Build) 10000.00 root  data:TableFullScan_14",
		"  │ └─TableFullScan_14 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─HashAgg_16(Probe) 1.00 root  funcs:count(Column#7)->Column#5",
		"    └─Projection_29 10000.00 root  plus(test.t.a, test.t.a)->Column#7",
		"      └─TableReader_22 10000.00 root  data:TableFullScan_21",
		"        └─TableFullScan_21 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// +--------------------------+----------+-----------+---------------+--------------------------------+
	// | id                       | estRows  | task      | access object | operator info                  |
	// +--------------------------+----------+-----------+---------------+--------------------------------+
	// | Projection_25            | 10000.00 | root      |               | 0->Column#13                   |
	// | └─TableReader_27         | 10000.00 | root      |               | data:TableFullScan_26          |
	// |   └─TableFullScan_26     | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo |
	// +--------------------------+----------+-----------+---------------+--------------------------------+
	tk.MustQuery("explain select (select count(a) from t) from t n;").Check(testkit.Rows(
		"Projection_27 10000.00 root  0->Column#9",
		"└─TableReader_29 10000.00 root  data:TableFullScan_28",
		"  └─TableFullScan_28 10000.00 cop[tikv] table:n keep order:false, stats:pseudo"))

	// +--------------------------------+----------+-----------+---------------+------------------------------------------------+
	// | id                             | estRows  | task      | access object | operator info                                  |
	// +--------------------------------+----------+-----------+---------------+------------------------------------------------+
	// | Projection_11                  | 1.00     | root      |               | Column#4                                       |
	// | └─Apply_13                     | 1.00     | root      |               | CARTESIAN left outer join                      |
	// |   ├─HashAgg_14(Build)          | 1.00     | root      |               | funcs:count(test.t.a)->Column#3                |
	// |   │ └─TableReader_17           | 10000.00 | root      |               | data:TableFullScan_16                          |
	// |   │   └─TableFullScan_16       | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo                 |
	// |   └─StreamAgg_21(Probe)        | 1.00     | root      |               | funcs:sum(Column#6)->Column#4                  |
	// |     └─Projection_24            | 1.00     | root      |               | cast(Column#3, decimal(20,0) BINARY)->Column#6 |
	// |       └─TableDual_23           | 1.00     | root      |               | rows:1                                         |
	// +--------------------------------+----------+-----------+---------------+------------------------------------------------+
	// nested aggregates and internal count agg's eval context is different from sum agg's eval context, allowed.
	tk.MustQuery("explain select (select sum(count(a))) from t;").Check(testkit.Rows(
		"Projection_12 1.00 root  Column#4",
		"└─Apply_14 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_15(Build) 1.00 root  funcs:count(test.t.a)->Column#3",
		"  │ └─TableReader_18 10000.00 root  data:TableFullScan_17",
		"  │   └─TableFullScan_17 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
		"  └─StreamAgg_22(Probe) 1.00 root  funcs:sum(Column#6)->Column#4",
		"    └─Projection_25 1.00 root  cast(Column#3, decimal(20,0) BINARY)->Column#6",
		"      └─TableDual_24 1.00 root  rows:1"))
}

func TestNameResolutionIssues(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_new_name_resolution=1")
	defer tk.MustExec("set @@tidb_enable_new_name_resolution=0")

	// Close issue #35346
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (a int, b int);")
	tk.MustExec("INSERT INTO t1 VALUES (2,22),(1,11),(2,22);")
	tk.MustExec("SET @@sql_mode='ansi';")
	err := tk.ExecToErr("SELECT a FROM t1 WHERE (SELECT COUNT(b) FROM DUAL) > 0 GROUP BY a;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1111]Invalid use of group function")

}

func TestCorrelatedAggEvalContextCheck(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_new_name_resolution=1")
	defer tk.MustExec("set @@tidb_enable_new_name_resolution=0")

}

// todo: do the unit test here.
func TestMaxAggLevel(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	par := parser.New()
	par.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t (a int)")
	tk.MustExec("CREATE TABLE s (a int)")

	tests := []struct {
		sql         string
		MaxAggLevel int
	}{
		{
			sql:         "select (select count(t.a+s.a) from s) from t",
			MaxAggLevel: 1,
		},
		{
			sql:         "select (select count(t.a) from s) from t",
			MaxAggLevel: 0,
		},
		{
			sql:         "select (select sum((select t.a from s))) from t",
			MaxAggLevel: 0,
		},
	}

	ctx := context.TODO()
	is := testGetIS(t, tk.Session())
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := par.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		tk.Session().GetSessionVars().PlanID = 0
		tk.Session().GetSessionVars().PlanColumnID = 0
		err = plannercore.Preprocess(tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err, comment)
		require.NoError(t, sessiontxn.GetTxnManager(tk.Session()).AdviseWarmup())
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session(), is, &hint.BlockHintProcessor{})
		// extract FD to every OP
		_, err = builder.Build(ctx, stmt)
		require.NoError(t, err, comment)
	}
}

func testGetIS(t *testing.T, ctx sessionctx.Context) infoschema.InfoSchema {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	require.NoError(t, err)
	return dom.InfoSchema()
}

func checkAggInAst() {

}
