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
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestSomeOldCorrelatedAggCases(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
		"HashJoin_10 1.00 root  CARTESIAN left outer join",
		"├─TableDual_27(Build) 1.00 root  rows:1",
		"└─StreamAgg_23(Probe) 1.00 root  funcs:count(Column#8)->Column#3",
		"  └─TableReader_24 1.00 root  data:StreamAgg_15",
		"    └─StreamAgg_15 1.00 cop[tikv]  funcs:count(test.t.a)->Column#8",
		"      └─TableFullScan_22 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"Projection_12 1.00 root  Column#6",
		"└─Apply_14 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_15(Build) 1.00 root  funcs:count(test.t.a)->Column#5",
		"  │ └─TableReader_18 10000.00 root  data:TableFullScan_17",
		"  │   └─TableFullScan_17 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_21(Probe) 1.00 root  Column#5",
		"    └─Limit_22 1.00 root  offset:0, count:1",
		"      └─TableReader_26 1.00 root  data:Limit_25",
		"        └─Limit_25 1.00 cop[tikv]  offset:0, count:1",
		"          └─TableFullScan_24 1.00 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"Projection_13 1.00 root  Column#6",
		"└─Apply_15 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_16(Build) 1.00 root  funcs:count(test.t.a)->Column#5",
		"  │ └─TableReader_19 10000.00 root  data:TableFullScan_18",
		"  │   └─TableFullScan_18 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_22(Probe) 1.00 root  1->Column#6",
		"    └─Limit_23 1.00 root  offset:0, count:1",
		"      └─TableReader_28 1.00 root  data:Limit_27",
		"        └─Limit_27 1.00 cop[tikv]  offset:0, count:1",
		"          └─Selection_26 1.00 cop[tikv]  gt(Column#5, 1)",
		"            └─TableFullScan_25 1.25 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"Projection_14 1.00 root  Column#6",
		"└─Apply_16 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_17(Build) 1.00 root  funcs:count(1)->Column#14",
		"  │ └─TableReader_20 10000.00 root  data:TableFullScan_19",
		"  │   └─TableFullScan_19 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_23(Probe) 1.00 root  1->Column#6",
		"    └─Limit_24 1.00 root  offset:0, count:1",
		"      └─TableReader_28 1.00 root  data:Limit_27",
		"        └─Limit_27 1.00 cop[tikv]  offset:0, count:1",
		"          └─TableFullScan_26 1.00 cop[tikv] table:t keep order:false, stats:pseudo",
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
		"Projection_14 1.00 root  Column#6",
		"└─Apply_16 1.00 root  CARTESIAN left outer join",
		"  ├─HashAgg_17(Build) 1.00 root  funcs:count(test.t.a)->Column#5",
		"  │ └─TableReader_20 10000.00 root  data:TableFullScan_19",
		"  │   └─TableFullScan_19 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─Projection_23(Probe) 1.00 root  1->Column#6",
		"    └─Limit_24 1.00 root  offset:0, count:1",
		"      └─TableReader_29 1.00 root  data:Limit_28",
		"        └─Limit_28 1.00 cop[tikv]  offset:0, count:1",
		"          └─Selection_27 1.00 cop[tikv]  gt(Column#5, 1)",
		"            └─TableFullScan_26 1.25 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"HashJoin_11 1.00 root  CARTESIAN left outer join",
		"├─TableDual_28(Build) 1.00 root  rows:1",
		"└─StreamAgg_24(Probe) 1.00 root  funcs:count(Column#8)->Column#3",
		"  └─TableReader_25 1.00 root  data:StreamAgg_16",
		"    └─StreamAgg_16 1.00 cop[tikv]  funcs:count(test.t.a)->Column#8",
		"      └─TableFullScan_23 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

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
		"Projection_9 10000.00 root  Column#5",
		"└─Apply_11 10000.00 root  CARTESIAN left outer join",
		"  ├─TableReader_13(Build) 10000.00 root  data:TableFullScan_12",
		"  │ └─TableFullScan_12 10000.00 cop[tikv] table:n keep order:false, stats:pseudo",
		"  └─HashAgg_14(Probe) 1.00 root  funcs:count(Column#7)->Column#5",
		"    └─Projection_27 10000.00 root  plus(test.t.a, test.t.a)->Column#7",
		"      └─TableReader_20 10000.00 root  data:TableFullScan_19",
		"        └─TableFullScan_19 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// +--------------------------+----------+-----------+---------------+--------------------------------+
	// | id                       | estRows  | task      | access object | operator info                  |
	// +--------------------------+----------+-----------+---------------+--------------------------------+
	// | Projection_25            | 10000.00 | root      |               | 0->Column#13                   |
	// | └─TableReader_27         | 10000.00 | root      |               | data:TableFullScan_26          |
	// |   └─TableFullScan_26     | 10000.00 | cop[tikv] | table:n       | keep order:false, stats:pseudo |
	// +--------------------------+----------+-----------+---------------+--------------------------------+
	tk.MustQuery("explain select (select count(a) from t) from t n;explain select (select count(a) from t) from t n;").Check(testkit.Rows(
		"Projection_24 10000.00 root  0->Column#8",
		"└─TableReader_26 10000.00 root  data:TableFullScan_25",
		"  └─TableFullScan_25 10000.00 cop[tikv] table:n keep order:false, stats:pseudo"))

	// nested aggregates is not allowed.
	err := tk.ExecToErr("select (select sum(count(a))) from t;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1111]Invalid use of group function")
}
