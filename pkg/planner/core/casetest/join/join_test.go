// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestSemiJoinOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1 (col0 int, col1 int);")
	tk.MustExec("create table t2 (col0 int, col1 int);")
	tk.MustExec("insert into t1 values (null, 3), (null, 5), (null, null), (1, 1), (1, 2), (1, null), (2, 1), (2, 2), (2, null), (3, 1), (3, 2), (3, 4), (3, null);")
	tk.MustExec("insert into t2 values (null, 3), (null, 4), (null, null), (1, 1), (3, 1), (3, 3), (3, null), (4, null), (4, 1), (4, 2), (4, 10);")
	result := testkit.Rows("1 <nil>",
		"1 1",
		"1 2",
		"3 <nil>",
		"3 1",
		"3 2",
		"3 4")
	tk.MustExec("set tidb_hash_join_version=optimized")
	tk.MustQuery("select * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
	tk.MustQuery("select /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
	tk.MustQuery("select /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
	tk.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
		"Sort 7992.00 root  test.t1.col0, test.t1.col1",
		"└─HashJoin 7992.00 root  semi join, equal:[eq(test.t1.col0, test.t2.col0)]",
		"  ├─TableReader(Build) 9990.00 root  data:Selection",
		"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
		"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 9990.00 root  data:Selection",
		"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
		"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1815 We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for semi join, please check the hint",
		"Warning 1815 We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for semi join, please check the hint"))
	tk.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
		"Sort 7992.00 root  test.t1.col0, test.t1.col1",
		"└─HashJoin 7992.00 root  semi join, equal:[eq(test.t1.col0, test.t2.col0)]",
		"  ├─TableReader(Build) 9990.00 root  data:Selection",
		"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
		"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 9990.00 root  data:Selection",
		"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
		"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1815 We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for semi join, please check the hint",
		"Warning 1815 We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for semi join, please check the hint"))
	tk.MustExec("set tidb_hash_join_version=legacy")
	tk.MustQuery("select * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
	tk.MustQuery("select /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
	tk.MustQuery("select /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
	tk.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
		"Sort 7992.00 root  test.t1.col0, test.t1.col1",
		"└─HashJoin 7992.00 root  semi join, equal:[eq(test.t1.col0, test.t2.col0)]",
		"  ├─TableReader(Build) 9990.00 root  data:Selection",
		"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
		"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 9990.00 root  data:Selection",
		"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
		"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1815 We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for semi join, please check the hint",
		"Warning 1815 We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for semi join, please check the hint"))
	tk.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
		"Sort 7992.00 root  test.t1.col0, test.t1.col1",
		"└─HashJoin 7992.00 root  semi join, equal:[eq(test.t1.col0, test.t2.col0)]",
		"  ├─TableReader(Build) 9990.00 root  data:Selection",
		"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
		"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 9990.00 root  data:Selection",
		"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
		"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1815 We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for semi join, please check the hint",
		"Warning 1815 We can't use the HASH_JOIN_BUILD or HASH_JOIN_PROBE hint for semi join, please check the hint"))
}

func TestJoinWithNullEQ(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	// https://github.com/pingcap/tidb/issues/57583
	tk.MustExec("create table t1(id int, v1 int, v2 int, v3 int);")
	tk.MustExec(" create table t2(id int, v1 int, v2 int, v3 int);")
	tk.MustQuery("explain select t1.id from t1 join t2 on t1.v1 = t2.v2 intersect select t1.id from t1 join t2 on t1.v1 = t2.v2;").Check(testkit.Rows(
		"HashJoin_15 6393.60 root  semi join, equal:[nulleq(test.t1.id, test.t1.id)]",
		"├─HashJoin_26(Build) 12487.50 root  inner join, equal:[eq(test.t1.v1, test.t2.v2)]",
		"│ ├─TableReader_33(Build) 9990.00 root  data:Selection_32",
		"│ │ └─Selection_32 9990.00 cop[tikv]  not(isnull(test.t2.v2))",
		"│ │   └─TableFullScan_31 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"│ └─TableReader_30(Probe) 9990.00 root  data:Selection_29",
		"│   └─Selection_29 9990.00 cop[tikv]  not(isnull(test.t1.v1))",
		"│     └─TableFullScan_28 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─HashAgg_16(Probe) 7992.00 root  group by:test.t1.id, funcs:firstrow(test.t1.id)->test.t1.id",
		"  └─HashJoin_17 12487.50 root  inner join, equal:[eq(test.t1.v1, test.t2.v2)]",
		"    ├─TableReader_24(Build) 9990.00 root  data:Selection_23",
		"    │ └─Selection_23 9990.00 cop[tikv]  not(isnull(test.t2.v2))",
		"    │   └─TableFullScan_22 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"    └─TableReader_21(Probe) 9990.00 root  data:Selection_20",
		"      └─Selection_20 9990.00 cop[tikv]  not(isnull(test.t1.v1))",
		"        └─TableFullScan_19 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))

	// https://github.com/pingcap/tidb/issues/60322
	tk.MustExec("CREATE TABLE tt0(c0 BOOL );")
	tk.MustExec("CREATE TABLE tt1(c0 CHAR );")
	tk.MustExec("INSERT INTO tt1 VALUES (NULL);")
	tk.MustExec("INSERT INTO tt0(c0) VALUES (false);")
	tk.MustQuery(`explain SELECT * FROM tt1
         LEFT JOIN (SELECT (0) AS col_0
                          FROM tt0) as subQuery1 ON ((subQuery1.col_0) = (tt1.c0))
         INNER JOIN tt0 ON (subQuery1.col_0 <=> tt0.c0);`).Check(testkit.Rows(
		"HashJoin_13 15625.00 root  inner join, equal:[nulleq(Column#7, test.tt0.c0)]",
		"├─TableReader_25(Build) 10000.00 root  data:TableFullScan_24",
		"│ └─TableFullScan_24 10000.00 cop[tikv] table:tt0 keep order:false, stats:pseudo",
		"└─HashJoin_17(Probe) 12500.00 root  left outer join, equal:[eq(Column#11, Column#12)]",
		"  ├─Projection_18(Build) 10000.00 root  test.tt1.c0, cast(test.tt1.c0, double BINARY)->Column#11",
		"  │ └─TableReader_20 10000.00 root  data:TableFullScan_19",
		"  │   └─TableFullScan_19 10000.00 cop[tikv] table:tt1 keep order:false, stats:pseudo",
		"  └─Projection_21(Probe) 10000.00 root  0->Column#7, 0->Column#12",
		"    └─TableReader_23 10000.00 root  data:TableFullScan_22",
		"      └─TableFullScan_22 10000.00 cop[tikv] table:tt0 keep order:false, stats:pseudo"))
	tk.MustQuery(`SELECT * FROM tt1
         LEFT JOIN (SELECT (0) AS col_0
                          FROM tt0) as subQuery1 ON ((subQuery1.col_0) = (tt1.c0))
         INNER JOIN tt0 ON (subQuery1.col_0 <=> tt0.c0);`).Check(testkit.Rows())
}
