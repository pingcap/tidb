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
		"HashJoin_11 1.00 root  CARTESIAN left outer join",
		"├─TableDual_28(Build) 1.00 root  rows:1",
		"└─StreamAgg_24(Probe) 1.00 root  funcs:count(Column#7)->Column#3",
		"  └─TableReader_25 1.00 root  data:StreamAgg_16",
		"    └─StreamAgg_16 1.00 cop[tikv]  funcs:count(test.t.a)->Column#7",
		"      └─TableFullScan_23 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"Projection_13 1.00 root  Column#6",
		"└─Apply_15 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_16(Build) 1.00 root  funcs:count(test.t.a)->Column#5",
		"  │ └─TableReader_19 10000.00 root  data:TableFullScan_18",
		"  │   └─TableFullScan_18 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_22(Probe) 1.00 root  Column#5",
		"    └─Limit_23 1.00 root  offset:0, count:1",
		"      └─TableReader_27 1.00 root  data:Limit_26",
		"        └─Limit_26 1.00 cop[tikv]  offset:0, count:1",
		"          └─TableFullScan_25 1.00 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"Projection_14 1.00 root  Column#5",
		"└─Apply_16 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_17(Build) 1.00 root  funcs:count(test.t.a)->Column#6",
		"  │ └─TableReader_20 10000.00 root  data:TableFullScan_19",
		"  │   └─TableFullScan_19 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_23(Probe) 1.00 root  1->Column#5",
		"    └─Limit_24 1.00 root  offset:0, count:1",
		"      └─TableReader_29 1.00 root  data:Limit_28",
		"        └─Limit_28 1.00 cop[tikv]  offset:0, count:1",
		"          └─Selection_27 1.00 cop[tikv]  gt(Column#6, 1)",
		"            └─TableFullScan_26 1.25 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"Projection_15 1.00 root  Column#5",
		"└─Apply_17 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_18(Build) 1.00 root  funcs:count(1)->Column#10",
		"  │ └─TableReader_21 10000.00 root  data:TableFullScan_20",
		"  │   └─TableFullScan_20 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_24(Probe) 1.00 root  1->Column#5",
		"    └─Limit_25 1.00 root  offset:0, count:1",
		"      └─TableReader_29 1.00 root  data:Limit_28",
		"        └─Limit_28 1.00 cop[tikv]  offset:0, count:1",
		"          └─TableFullScan_27 1.00 cop[tikv] table:t keep order:false, stats:pseudo",
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
		"HashJoin_12 1.00 root  CARTESIAN left outer join",
		"├─TableDual_29(Build) 1.00 root  rows:1",
		"└─StreamAgg_25(Probe) 1.00 root  funcs:count(Column#7)->Column#3",
		"  └─TableReader_26 1.00 root  data:StreamAgg_17",
		"    └─StreamAgg_17 1.00 cop[tikv]  funcs:count(test.t.a)->Column#7",
		"      └─TableFullScan_24 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"Projection_10 10000.00 root  Column#5",
		"└─Apply_12 10000.00 root  CARTESIAN left outer join",
		"  ├─TableReader_14(Build) 10000.00 root  data:TableFullScan_13",
		"  │ └─TableFullScan_13 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─HashAgg_15(Probe) 1.00 root  funcs:count(Column#6)->Column#5",
		"    └─Projection_28 10000.00 root  plus(test.t.a, test.t.a)->Column#6",
		"      └─TableReader_21 10000.00 root  data:TableFullScan_20",
		"        └─TableFullScan_20 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// +--------------------------+----------+-----------+---------------+--------------------------------+
	// | id                       | estRows  | task      | access object | operator info                  |
	// +--------------------------+----------+-----------+---------------+--------------------------------+
	// | Projection_25            | 10000.00 | root      |               | 0->Column#13                   |
	// | └─TableReader_27         | 10000.00 | root      |               | data:TableFullScan_26          |
	// |   └─TableFullScan_26     | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo |
	// +--------------------------+----------+-----------+---------------+--------------------------------+
	tk.MustQuery("explain select (select count(a) from t) from t n;").Check(testkit.Rows(
		"Projection_26 10000.00 root  0->Column#8",
		"└─TableReader_28 10000.00 root  data:TableFullScan_27",
		"  └─TableFullScan_27 10000.00 cop[tikv] table:n keep order:false, stats:pseudo"))

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
		"Projection_11 1.00 root  Column#4",
		"└─Apply_13 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_14(Build) 1.00 root  funcs:count(test.t.a)->Column#3",
		"  │ └─TableReader_17 10000.00 root  data:TableFullScan_16",
		"  │   └─TableFullScan_16 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
		"  └─StreamAgg_21(Probe) 1.00 root  funcs:sum(Column#5)->Column#4",
		"    └─Projection_24 1.00 root  cast(Column#3, decimal(20,0) BINARY)->Column#5",
		"      └─TableDual_23 1.00 root  rows:1"))
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

	tk.MustExec("SET @@sql_mode='';")
	tk.MustQuery("explain SELECT a FROM t1 WHERE (SELECT COUNT(b) FROM DUAL) > 0 GROUP BY a order by a;").Check(testkit.Rows(
		"Sort_12 8000.00 root  test.t1.a",
		"└─HashAgg_15 8000.00 root  group by:test.t1.a, funcs:firstrow(test.t1.a)->test.t1.a",
		"  └─Apply_17 10000.00 root  CARTESIAN inner join",
		"    ├─TableReader_19(Build) 10000.00 root  data:TableFullScan_18",
		"    │ └─TableFullScan_18 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"    └─Selection_20(Probe) 0.80 root  gt(Column#4, 0)",
		"      └─StreamAgg_22 1.00 root  funcs:count(test.t1.b)->Column#4",
		"        └─TableDual_24 1.00 root  rows:1"))
	tk.MustQuery("SELECT a FROM t1 WHERE (SELECT COUNT(b) FROM DUAL) > 0 GROUP BY a order by a;").Check(testkit.Rows(
		"1", "2"))

	// Close issue 35347
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("CREATE TABLE t1 (a INT);")
	tk.MustExec("CREATE TABLE t2 (x INT);")
	tk.MustExec("INSERT INTO t1 values (1),(1),(1),(1);")
	tk.MustExec("INSERT INTO t1 values (1000),(1001),(1002);")
	err = tk.ExecToErr("SELECT SUM( (SELECT COUNT(a) FROM t2) ) FROM t1;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1111]Invalid use of group function")

	// Close issue 35099
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	// old plan:
	// +----------------------------------+----------+-----------+---------------+-------------------------------------------------+
	// | id                               | estRows  | task      | access object | operator info                                   |
	// +----------------------------------+----------+-----------+---------------+-------------------------------------------------+
	// | Projection_10                    | 3333.33  | root      |               | 1->Column#8                                     |
	// | └─HashAgg_11                     | 3333.33  | root      |               | group by:Column#7, funcs:firstrow(1)->Column#11 |
	// |   └─Apply_13                     | 3333.33  | root      |               | CARTESIAN left outer join                       |
	// |     ├─TableReader_16(Build)      | 3333.33  | root      |               | data:Selection_15                               |
	// |     │ └─Selection_15             | 3333.33  | cop[tikv] |               | gt(test.t.b, 0)                                 |
	// |     │   └─TableFullScan_14       | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo                  |
	// |     └─HashAgg_17(Probe)          | 1.00     | root      |               | funcs:count(test.t.b)->Column#7                 |
	// |       └─TableReader_23           | 10000.00 | root      |               | data:TableFullScan_22                           |
	// |         └─TableFullScan_22       | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo                  |
	// +----------------------------------+----------+-----------+---------------+-------------------------------------------------+
	// 9 rows in set, 2 warnings (0.02 sec)
	tk.MustQuery("explain select 1 from t n where b>0 group by (select count(n.b) from t);").Check(testkit.Rows(
		"Projection_10 3333.33 root  1->Column#4",
		"└─HashAgg_11 3333.33 root  group by:Column#8, funcs:firstrow(1)->Column#11",
		"  └─Apply_13 3333.33 root  CARTESIAN left outer join",
		"    ├─TableReader_16(Build) 3333.33 root  data:Selection_15",
		"    │ └─Selection_15 3333.33 cop[tikv]  gt(test.t.b, 0)",
		"    │   └─TableFullScan_14 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"    └─HashAgg_17(Probe) 1.00 root  funcs:count(test.t.b)->Column#8",
		"      └─TableReader_23 10000.00 root  data:TableFullScan_22",
		"        └─TableFullScan_22 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	// some explanation:
	// subq's count(n.b) couldn't be extracted to outer scope because it's context is in the group by clause, so it evaluated in subq.
	// having's count(n.b) can be extracted to outer scope because it's context is in the having clause.
	tk.MustQuery("explain select 1 from t n where b>0 group by (select count(n.b) from t) having count(n.b) >0;").Check(testkit.Rows(
		"Projection_14 2666.67 root  1->Column#10",
		"└─Selection_15 2666.67 root  gt(Column#9, 0)",
		"  └─HashAgg_16 3333.33 root  group by:Column#8, funcs:count(test.t.b)->Column#9",
		"    └─Apply_18 3333.33 root  CARTESIAN left outer join",
		"      ├─TableReader_21(Build) 3333.33 root  data:Selection_20",
		"      │ └─Selection_20 3333.33 cop[tikv]  gt(test.t.b, 0)",
		"      │   └─TableFullScan_19 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"      └─HashAgg_22(Probe) 1.00 root  funcs:count(test.t.b)->Column#8",
		"        └─TableReader_28 10000.00 root  data:TableFullScan_27",
		"          └─TableFullScan_27 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// Close issue 26945
	tk.MustExec("DROP TABLE IF EXISTS t1, t2;")
	tk.MustExec("CREATE TABLE t1 (a INT, b INT);")
	tk.MustExec("CREATE TABLE t2 (a INT, b INT);")
	tk.MustExec("INSERT INTO t1 VALUES (1, 1);")
	tk.MustExec("INSERT INTO t2 VALUES (1, 1);")
	// old plan
	// +----------------------------------+----------+-----------+---------------+--------------------------------+
	// | id                               | estRows  | task      | access object | operator info                  |
	// +----------------------------------+----------+-----------+---------------+--------------------------------+
	// | Projection_16                    | 10000.00 | root      |               | test.t1.a                      |
	// | └─Sort_17                        | 10000.00 | root      |               | test.t2.b                      |
	// |   └─Apply_20                     | 10000.00 | root      |               | CARTESIAN left outer join      |
	// |     ├─TableReader_22(Build)      | 10000.00 | root      |               | data:TableFullScan_21          |
	// |     │ └─TableFullScan_21         | 10000.00 | cop[tikv] | table:one     | keep order:false, stats:pseudo |
	// |     └─MaxOneRow_23(Probe)        | 1.00     | root      |               |                                |
	// |       └─TableReader_26           | 2.00     | root      |               | data:Selection_25              |
	// |         └─Selection_25           | 2.00     | cop[tikv] |               | eq(test.t2.a, test.t1.b)       |
	// |           └─TableFullScan_24     | 2000.00  | cop[tikv] | table:two     | keep order:false, stats:pseudo |
	// +----------------------------------+----------+-----------+---------------+--------------------------------+
	//9 rows in set (0.01 sec)
	tk.MustQuery("explain SELECT one.a FROM t1 one ORDER BY (SELECT two.b FROM t2 two WHERE two.a = one.b);").Check(testkit.Rows(
		"Projection_10 10000.00 root  test.t1.a",
		"└─Sort_11 10000.00 root  test.t2.b",
		"  └─Apply_14 10000.00 root  CARTESIAN left outer join",
		"    ├─TableReader_16(Build) 10000.00 root  data:TableFullScan_15",
		"    │ └─TableFullScan_15 10000.00 cop[tikv] table:one keep order:false, stats:pseudo",
		"    └─MaxOneRow_17(Probe) 1.00 root  ",
		"      └─TableReader_20 2.00 root  data:Selection_19",
		"        └─Selection_19 2.00 cop[tikv]  eq(test.t2.a, test.t1.b)",
		"          └─TableFullScan_18 2000.00 cop[tikv] table:two keep order:false, stats:pseudo"))
	tk.MustQuery("SELECT one.a FROM t1 one ORDER BY (SELECT two.b FROM t2 two WHERE two.a = one.b);").Check(testkit.Rows(
		"1"))

	// Close issue 30957
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("CREATE TABLE t1 (a int, b int);")
	tk.MustExec("CREATE TABLE t2 (m int, n int);")
	tk.MustExec("INSERT INTO t1 VALUES (2,2), (2,2), (3,3), (3,3), (3,3), (4,4);")
	tk.MustExec("INSERT INTO t2 VALUES (1,11), (2,22), (3,32), (4,44), (4,44);")
	tk.MustQuery("explain SELECT COUNT(*), a,(SELECT m FROM t2 WHERE m = count(*) LIMIT 1) FROM t1 GROUP BY a;").Check(testkit.Rows(
		"Apply_16 8000.00 root  CARTESIAN left outer join",
		"├─HashAgg_17(Build) 8000.00 root  group by:test.t1.a, funcs:count(1)->Column#4, funcs:firstrow(test.t1.a)->test.t1.a",
		"│ └─TableReader_19 10000.00 root  data:TableFullScan_18",
		"│   └─TableFullScan_18 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─Limit_20(Probe) 1.00 root  offset:0, count:1",
		"  └─TableReader_25 1.00 root  data:Limit_24",
		"    └─Limit_24 1.00 cop[tikv]  offset:0, count:1",
		"      └─Selection_23 1.00 cop[tikv]  eq(test.t2.m, Column#4)",
		"        └─TableFullScan_22 1000.00 cop[tikv] table:t2 keep order:false, stats:pseudo"))
	tk.MustQuery("SELECT COUNT(*), a,(SELECT m FROM t2 WHERE m = count(*) LIMIT 1) FROM t1 GROUP BY a order by a").Check(testkit.Rows(
		"2 2 2", "3 3 3", "1 4 1"))

	// Close issue 29084
	tk.MustExec("DROP TABLE if exists t1,t2,t3;")
	tk.MustExec("create table t1 (col1 int, col2 varchar(5), col_t1 int);")
	tk.MustExec("create table t2 (col1 int, col2 varchar(5), col_t2 int);")
	tk.MustExec("create table t3 (col1 int, col2 varchar(5), col_t3 int);")
	tk.MustExec("insert into t1 values(10,'hello',10);")
	tk.MustExec("insert into t1 values(20,'hello',20);")
	tk.MustExec("insert into t1 values(30,'hello',30);")
	tk.MustExec("insert into t1 values(10,'bye',10);")
	tk.MustExec("insert into t1 values(10,'sam',10);")
	tk.MustExec("insert into t1 values(10,'bob',10);")
	tk.MustExec("insert into t2 select * from t1;")
	tk.MustQuery("explain select sum(col1) from t1 group by col_t1,col1 having col_t1 in" +
		" (select sum(t2.col1) from t2 group by t2.col2, t2.col1 having t2.col1 = t1.col1) order by sum(col1);").Check(testkit.Rows(
		"Sort_15 6393.60 root  Column#5",
		"└─HashJoin_17 6393.60 root  semi join, equal:[eq(test.t1.col1, test.t2.col1) eq(Column#13, Column#10)]",
		"  ├─HashAgg_31(Build) 7992.00 root  group by:Column#27, Column#28, funcs:sum(Column#25)->Column#10, funcs:firstrow(Column#26)->test.t2.col1",
		"  │ └─Projection_40 9990.00 root  cast(test.t2.col1, decimal(10,0) BINARY)->Column#25, test.t2.col1, test.t2.col2, test.t2.col1",
		"  │   └─TableReader_38 9990.00 root  data:Selection_37",
		"  │     └─Selection_37 9990.00 cop[tikv]  not(isnull(test.t2.col1))",
		"  │       └─TableFullScan_36 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─Projection_18(Probe) 7992.00 root  Column#5, test.t1.col1, cast(test.t1.col_t1, decimal(20,0) BINARY)->Column#13",
		"    └─HashAgg_21 7992.00 root  group by:Column#23, Column#24, funcs:sum(Column#20)->Column#5, funcs:firstrow(Column#21)->test.t1.col1, funcs:firstrow(Column#22)->test.t1.col_t1",
		"      └─Projection_39 9990.00 root  cast(test.t1.col1, decimal(10,0) BINARY)->Column#20, test.t1.col1, test.t1.col_t1, test.t1.col_t1, test.t1.col1",
		"        └─TableReader_28 9990.00 root  data:Selection_27",
		"          └─Selection_27 9990.00 cop[tikv]  not(isnull(test.t1.col1))",
		"            └─TableFullScan_26 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("select sum(col1) from t1 group by col_t1,col1 having col_t1 in (select sum(t2.col1) from t2 group by t2.col2, t2.col1 having t2.col1 = t1.col1) order by sum(col1);").Check(testkit.Rows(
		"20", "30", "40"))
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
