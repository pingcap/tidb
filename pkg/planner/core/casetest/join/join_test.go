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
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test;")
		testKit.MustExec("create table t1 (col0 int, col1 int);")
		testKit.MustExec("create table t2 (col0 int, col1 int);")
		testKit.MustExec("insert into t1 values (null, 3), (null, 5), (null, null), (1, 1), (1, 2), (1, null), (2, 1), (2, 2), (2, null), (3, 1), (3, 2), (3, 4), (3, null);")
		testKit.MustExec("insert into t2 values (null, 3), (null, 4), (null, null), (1, 1), (3, 1), (3, 3), (3, null), (4, null), (4, 1), (4, 2), (4, 10);")
		result := testkit.Rows("1 <nil>",
			"1 1",
			"1 2",
			"3 <nil>",
			"3 1",
			"3 2",
			"3 4")
		testKit.MustExec("set tidb_hash_join_version=optimized")
		testKit.MustQuery("select * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		testKit.MustQuery("select /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		testKit.MustQuery("select /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		testKit.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
			"Sort 7992.00 root  test.t1.col0, test.t1.col1",
			"└─HashJoin 7992.00 root  semi join, left side:TableReader, equal:[eq(test.t1.col0, test.t2.col0)]",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 9990.00 root  data:Selection",
			"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
			"      └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo"))
		testKit.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints"))
		testKit.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
			"Sort 7992.00 root  test.t1.col0, test.t1.col1",
			"└─HashJoin 7992.00 root  semi join, left side:TableReader, equal:[eq(test.t1.col0, test.t2.col0)]",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 9990.00 root  data:Selection",
			"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
			"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		testKit.MustQuery("show warnings").Check(testkit.Rows())
		testKit.MustExec("set tidb_hash_join_version=legacy")
		testKit.MustQuery("select * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		testKit.MustQuery("select /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		testKit.MustQuery("select /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		testKit.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
			"Sort 7992.00 root  test.t1.col0, test.t1.col1",
			"└─HashJoin 7992.00 root  semi join, left side:TableReader, equal:[eq(test.t1.col0, test.t2.col0)]",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 9990.00 root  data:Selection",
			"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
			"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		testKit.MustQuery("show warnings").Check(testkit.Rows(
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for semi join with hash join version 1. Please remove these hints",
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for semi join with hash join version 1. Please remove these hints"))
		testKit.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
			"Sort 7992.00 root  test.t1.col0, test.t1.col1",
			"└─HashJoin 7992.00 root  semi join, left side:TableReader, equal:[eq(test.t1.col0, test.t2.col0)]",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 9990.00 root  data:Selection",
			"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
			"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		testKit.MustQuery("show warnings").Check(testkit.Rows(
			"Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for semi join with hash join version 1. Please remove these hints",
			"Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for semi join with hash join version 1. Please remove these hints"))
	})
}

func TestJoinWithNullEQ(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test;")
		// https://github.com/pingcap/tidb/issues/57583
		testKit.MustExec("create table t1(id int, v1 int, v2 int, v3 int);")
		testKit.MustExec(" create table t2(id int, v1 int, v2 int, v3 int);")
		testKit.MustQuery("explain format='brief' select t1.id from t1 join t2 on t1.v1 = t2.v2 intersect select t1.id from t1 join t2 on t1.v1 = t2.v2;").Check(testkit.Rows(
			"HashJoin 6393.60 root  semi join, left side:HashAgg, equal:[nulleq(test.t1.id, test.t1.id)]",
			"├─HashJoin(Build) 12487.50 root  inner join, equal:[eq(test.t1.v1, test.t2.v2)]",
			"│ ├─TableReader(Build) 9990.00 root  data:Selection",
			"│ │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.v2))",
			"│ │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
			"│ └─TableReader(Probe) 9990.00 root  data:Selection",
			"│   └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.v1))",
			"│     └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
			"└─HashAgg(Probe) 7992.00 root  group by:test.t1.id, funcs:firstrow(test.t1.id)->test.t1.id",
			"  └─HashJoin 12487.50 root  inner join, equal:[eq(test.t1.v1, test.t2.v2)]",
			"    ├─TableReader(Build) 9990.00 root  data:Selection",
			"    │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.v2))",
			"    │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
			"    └─TableReader(Probe) 9990.00 root  data:Selection",
			"      └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.v1))",
			"        └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))

		// https://github.com/pingcap/tidb/issues/60322
		testKit.MustExec("CREATE TABLE tt0(c0 BOOL );")
		testKit.MustExec("CREATE TABLE tt1(c0 CHAR );")
		testKit.MustExec("INSERT INTO tt1 VALUES (NULL);")
		testKit.MustExec("INSERT INTO tt0(c0) VALUES (false);")
		testKit.MustQuery(`explain format='brief' SELECT * FROM tt1
         LEFT JOIN (SELECT (0) AS col_0
                          FROM tt0) as subQuery1 ON ((subQuery1.col_0) = (tt1.c0))
         INNER JOIN tt0 ON (subQuery1.col_0 <=> tt0.c0);`).Check(testkit.Rows(
			"HashJoin 15625.00 root  inner join, equal:[nulleq(Column#5, test.tt0.c0)]",
			"├─TableReader(Build) 10000.00 root  data:TableFullScan",
			"│ └─TableFullScan 10000.00 cop[tikv] table:tt0 keep order:false, stats:pseudo",
			"└─HashJoin(Probe) 12500.00 root  left outer join, left side:Projection, equal:[eq(Column#8, Column#9)]",
			"  ├─Projection(Build) 10000.00 root  test.tt1.c0, cast(test.tt1.c0, double BINARY)->Column#8",
			"  │ └─TableReader 10000.00 root  data:TableFullScan",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:tt1 keep order:false, stats:pseudo",
			"  └─Projection(Probe) 10000.00 root  0->Column#5, 0->Column#9",
			"    └─TableReader 10000.00 root  data:TableFullScan",
			"      └─TableFullScan 10000.00 cop[tikv] table:tt0 keep order:false, stats:pseudo"))
		testKit.MustQuery(`SELECT * FROM tt1
         LEFT JOIN (SELECT (0) AS col_0
                          FROM tt0) as subQuery1 ON ((subQuery1.col_0) = (tt1.c0))
         INNER JOIN tt0 ON (subQuery1.col_0 <=> tt0.c0);`).Check(testkit.Rows())
	})
}

func TestJoinSimplifyCondition(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test;")
		testKit.MustExec(`CREATE TABLE t1 (a int(11) DEFAULT NULL,b int(11) DEFAULT NULL,c int(11) DEFAULT NULL,KEY idx_a (a));`)
		testKit.MustExec(`CREATE TABLE t2 (a int(11) DEFAULT NULL,b int(11) DEFAULT NULL,c int(11) DEFAULT NULL,KEY idx_a (a));`)
		testKit.MustQuery(`explain format='brief' select * from t1,t2 where t1.a=t2.a and t1.b = 1 or 1=2;`).
			Check(testkit.Rows(
				"IndexHashJoin 12.49 root  inner join, inner:IndexLookUp, outer key:test.t1.a, inner key:test.t2.a, equal cond:eq(test.t1.a, test.t2.a)",
				"├─TableReader(Build) 9.99 root  data:Selection",
				"│ └─Selection 9.99 cop[tikv]  eq(test.t1.b, 1), not(isnull(test.t1.a))",
				"│   └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
				"└─IndexLookUp(Probe) 12.49 root  ",
				"  ├─Selection(Build) 12.49 cop[tikv]  not(isnull(test.t2.a))",
				"  │ └─IndexRangeScan 12.50 cop[tikv] table:t2, index:idx_a(a) range: decided by [eq(test.t2.a, test.t1.a)], keep order:false, stats:pseudo",
				"  └─TableRowIDScan(Probe) 12.49 cop[tikv] table:t2 keep order:false, stats:pseudo"))
	})
}
