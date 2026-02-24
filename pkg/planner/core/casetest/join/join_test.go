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
	"github.com/stretchr/testify/require"
)

func TestSemiJoinOrder(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
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
			"└─HashJoin 7992.00 root  semi join, left side:TableReader, equal:[eq(test.t1.col0, test.t2.col0)]",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 9990.00 root  data:Selection",
			"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
			"      └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo"))
		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints"))
		tk.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
			"Sort 7992.00 root  test.t1.col0, test.t1.col1",
			"└─HashJoin 7992.00 root  semi join, left side:TableReader, equal:[eq(test.t1.col0, test.t2.col0)]",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 9990.00 root  data:Selection",
			"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
			"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustExec("set tidb_hash_join_version=legacy")
		tk.MustQuery("select * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		tk.MustQuery("select /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		tk.MustQuery("select /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(result)
		tk.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t1) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
			"Sort 7992.00 root  test.t1.col0, test.t1.col1",
			"└─HashJoin 7992.00 root  semi join, left side:TableReader, equal:[eq(test.t1.col0, test.t2.col0)]",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 9990.00 root  data:Selection",
			"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
			"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		tk.MustQuery("show warnings").Check(testkit.Rows(
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for semi join with hash join version 1. Please remove these hints",
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
			"Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for semi join with hash join version 1. Please remove these hints"))
		tk.MustQuery("explain format = 'brief' select  /*+ HASH_JOIN_BUILD(t2@sel_2) */ * from t1 where exists (select 1 from t2 where t1.col0 = t2.col0) order by t1.col0, t1.col1;").Check(testkit.Rows(
			"Sort 7992.00 root  test.t1.col0, test.t1.col1",
			"└─HashJoin 7992.00 root  semi join, left side:TableReader, equal:[eq(test.t1.col0, test.t2.col0)]",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.col0))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 9990.00 root  data:Selection",
			"    └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.col0))",
			"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		tk.MustQuery("show warnings").Check(testkit.Rows(
			"Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for semi join with hash join version 1. Please remove these hints",
			"Warning 1815 The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for semi join with hash join version 1. Please remove these hints"))
	})
}

func TestJoinWithNullEQ(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test;")
		// https://github.com/pingcap/tidb/issues/57583
		tk.MustExec("create table t1(id int, v1 int, v2 int, v3 int);")
		tk.MustExec(" create table t2(id int, v1 int, v2 int, v3 int);")
		tk.MustQuery("explain format='brief' select t1.id from t1 join t2 on t1.v1 = t2.v2 intersect select t1.id from t1 join t2 on t1.v1 = t2.v2;").Check(testkit.Rows(
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
		tk.MustExec("CREATE TABLE tt0(c0 BOOL );")
		tk.MustExec("CREATE TABLE tt1(c0 CHAR );")
		tk.MustExec("INSERT INTO tt1 VALUES (NULL);")
		tk.MustExec("INSERT INTO tt0(c0) VALUES (false);")
		tk.MustQuery(`explain format='brief' SELECT * FROM tt1
         LEFT JOIN (SELECT (0) AS col_0
                          FROM tt0) as subQuery1 ON ((subQuery1.col_0) = (tt1.c0))
         INNER JOIN tt0 ON (subQuery1.col_0 <=> tt0.c0);`).Check(testkit.Rows(
			"HashJoin 15625.00 root  inner join, equal:[nulleq(Column#7, test.tt0.c0)]",
			"├─TableReader(Build) 10000.00 root  data:TableFullScan",
			"│ └─TableFullScan 10000.00 cop[tikv] table:tt0 keep order:false, stats:pseudo",
			"└─HashJoin(Probe) 12500.00 root  left outer join, left side:Projection, equal:[eq(Column#11, Column#12)]",
			"  ├─Projection(Build) 10000.00 root  test.tt1.c0, cast(test.tt1.c0, double BINARY)->Column#11",
			"  │ └─TableReader 10000.00 root  data:TableFullScan",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:tt1 keep order:false, stats:pseudo",
			"  └─Projection(Probe) 10000.00 root  0->Column#7, 0->Column#12",
			"    └─TableReader 10000.00 root  data:TableFullScan",
			"      └─TableFullScan 10000.00 cop[tikv] table:tt0 keep order:false, stats:pseudo"))
		tk.MustQuery(`SELECT * FROM tt1
         LEFT JOIN (SELECT (0) AS col_0
                          FROM tt0) as subQuery1 ON ((subQuery1.col_0) = (tt1.c0))
         INNER JOIN tt0 ON (subQuery1.col_0 <=> tt0.c0);`).Check(testkit.Rows())
	})
}

func TestJoinSimplifyCondition(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test;")
		tk.MustExec(`CREATE TABLE t1 (a int(11) DEFAULT NULL,b int(11) DEFAULT NULL,c int(11) DEFAULT NULL,KEY idx_a (a));`)
		tk.MustExec(`CREATE TABLE t2 (a int(11) DEFAULT NULL,b int(11) DEFAULT NULL,c int(11) DEFAULT NULL,KEY idx_a (a));`)
		tk.MustQuery(`explain format='brief' select * from t1,t2 where t1.a=t2.a and t1.b = 1 or 1=2;`).
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

func TestKeepingJoinKeys(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`create table t1 (a int, b int, c int)`)
		tk.MustExec(`create table t2 (a int, b int, c int)`)
		tk.MustExec(`set @@tidb_opt_always_keep_join_key=true`)

		// join keys are kept
		tk.MustQuery(`explain format='plan_tree' select 1 from t1 left join t2 on t1.a=t2.a where t1.a=1`).Check(testkit.Rows(
			`Projection root  1->Column`,
			`└─HashJoin root  left outer join, left side:TableReader, equal:[eq(test.t1.a, test.t2.a)]`,
			`  ├─TableReader(Build) root  data:Selection`,
			`  │ └─Selection cop[tikv]  eq(test.t2.a, 1)`,
			`  │   └─TableFullScan cop[tikv] table:t2 keep order:false, stats:pseudo`,
			`  └─TableReader(Probe) root  data:Selection`,
			`    └─Selection cop[tikv]  eq(test.t1.a, 1)`,
			`      └─TableFullScan cop[tikv] table:t1 keep order:false, stats:pseudo`))
		tk.MustQuery(`explain format='plan_tree' select 1 from t1 left join t2 on t1.a=t2.a where t2.a=1`).Check(testkit.Rows(
			`Projection root  1->Column`,
			`└─HashJoin root  inner join, equal:[eq(test.t1.a, test.t2.a)]`,
			`  ├─TableReader(Build) root  data:Selection`,
			`  │ └─Selection cop[tikv]  eq(test.t2.a, 1)`,
			`  │   └─TableFullScan cop[tikv] table:t2 keep order:false, stats:pseudo`,
			`  └─TableReader(Probe) root  data:Selection`,
			`    └─Selection cop[tikv]  eq(test.t1.a, 1)`,
			`      └─TableFullScan cop[tikv] table:t1 keep order:false, stats:pseudo`))
		tk.MustQuery(`explain format='plan_tree' select 1 from t1, t2 where t1.a=1 and t1.a=t2.a`).Check(testkit.Rows(
			`Projection root  1->Column`,
			`└─HashJoin root  inner join, equal:[eq(test.t1.a, test.t2.a)]`,
			`  ├─TableReader(Build) root  data:Selection`,
			`  │ └─Selection cop[tikv]  eq(test.t2.a, 1)`,
			`  │   └─TableFullScan cop[tikv] table:t2 keep order:false, stats:pseudo`,
			`  └─TableReader(Probe) root  data:Selection`,
			`    └─Selection cop[tikv]  eq(test.t1.a, 1)`,
			`      └─TableFullScan cop[tikv] table:t1 keep order:false, stats:pseudo`))
	})
}

func TestJoinRegression(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")

		tk.MustExec(`CREATE TABLE t0(c0 BLOB);`)
		tk.MustExec(`CREATE definer='root'@'localhost' VIEW v0(c0) AS SELECT NULL FROM t0 GROUP BY NULL;`)
		tk.MustExec(`SELECT t0.c0 FROM t0 NATURAL JOIN v0 WHERE v0.c0 LIKE v0.c0;`) // no error
		tk.MustQuery(`explain format='brief' SELECT /* issue:46556 */ t0.c0 FROM t0 NATURAL JOIN v0 WHERE v0.c0 LIKE v0.c0`).Check(
			testkit.Rows(`HashJoin 0.00 root  inner join, equal:[eq(Column#7, test.t0.c0)]`,
				`├─Projection(Build) 0.00 root  <nil>->Column#7`,
				`│ └─TableDual 0.00 root  rows:0`,
				`└─TableReader(Probe) 9990.00 root  data:Selection`,
				`  └─Selection 9990.00 cop[tikv]  not(isnull(test.t0.c0))`,
				`    └─TableFullScan 10000.00 cop[tikv] table:t0 keep order:false, stats:pseudo`))

		tk.MustExec(`create table t1 (a int)`)
		tk.MustExec(`create table t2 (a int, b int, c int, d int, key ab(a, b), key abcd(a, b, c, d))`)
		tk.MustUseIndex(`select /* issue:63949 */ /*+ tidb_inlj(t2) */ t2.a from t1, t2 where t1.a=t2.a and t2.b=1 and t2.d=1`, "abcd")

		tk.MustExec(`
CREATE TABLE B (
  ROW_NO bigint NOT NULL AUTO_INCREMENT,
  RCRD_NO varchar(20) NOT NULL,
  FILE_NO varchar(20) DEFAULT NULL,
  BSTPRTFL_NO varchar(20) DEFAULT NULL,
  MDL_DT date DEFAULT NULL,
  TS varchar(19) DEFAULT NULL,
  LD varchar(19) DEFAULT NULL,
  MDL_NO varchar(50) DEFAULT NULL,
  TXN_NO varchar(90) DEFAULT NULL,
  SCR_NO varchar(20) DEFAULT NULL,
  DAM decimal(25, 8) DEFAULT NULL,
  DT date DEFAULT NULL,
  PRIMARY KEY (ROW_NO),
  KEY IDX1_ETF_FLR_PRCHRDMP_TXN_DTL (BSTPRTFL_NO, DT, MDL_DT, TS, LD),
  KEY IDX2_ETF_FLR_PRCHRDMP_TXN_DTL (BSTPRTFL_NO, MDL_DT, SCR_NO, TXN_NO),
  KEY IDX1_ETF_FLR_PRCHRDMP_TXNDTL (FILE_NO, BSTPRTFL_NO),
  KEY IDX_ETF_FLR_PRCHRDMP_TXN_FIX (MDL_NO, BSTPRTFL_NO, MDL_DT),
  UNIQUE UI_ETF_FLR_PRCHRDMP_TXN_DTLTB (RCRD_NO),
  KEY IDX3_ETF_FLR_PRCHRDMP_TXN_DTL (DT)
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE utf8mb4_bin AUTO_INCREMENT = 2085290754;`)
		tk.MustExec(`
CREATE TABLE A (
  ROW_NO bigint NOT NULL AUTO_INCREMENT,
  TEMP_NO varchar(20) NOT NULL,
  VCHR_TPCD varchar(19) DEFAULT NULL,
  LD varchar(19) DEFAULT NULL,
  BSTPRTFL_NO varchar(20) DEFAULT NULL,
  DAM decimal(25, 8) DEFAULT NULL,
  DT date DEFAULT NULL,
  CASH_RPLC_AMT decimal(19, 2) DEFAULT NULL,
  PCSG_BTNO_NO varchar(20) DEFAULT NULL,
  KEY INX_TEMP_NO (TEMP_NO),
  PRIMARY KEY (ROW_NO),
  KEY idx2_ETF_FNDTA_SALE_PA (PCSG_BTNO_NO, DT, VCHR_TPCD)
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE utf8mb4_bin AUTO_INCREMENT = 900006;`)
		tk.MustExec(`set tidb_opt_index_join_build_v2=off`)
		r := tk.MustQuery(`
explain SELECT /* issue:61669 */ *
FROM A A
JOIN
    (SELECT CASH_RPLC_AMT,
         S.BSTPRTFL_NO
    FROM
        (SELECT BSTPRTFL_NO,
         SUM(CASE
            WHEN LD IN ('03') THEN
            DAM
            ELSE 0 END) AS CASH_RPLC_AMT
        FROM
            (SELECT B.LD,
         SUM(B.DAM) DAM,
         B.BSTPRTFL_NO
            FROM B B
            GROUP BY  B.LD, B.BSTPRTFL_NO) ff
            GROUP BY  BSTPRTFL_NO) S ) f
            ON A.BSTPRTFL_NO = f.BSTPRTFL_NO
    WHERE A.PCSG_BTNO_NO = 'MXUU2022123043502318'`)
		require.True(t, len(r.Rows()) > 0) // no error

		tk.MustExec(`create table t1_issue60076 (a int, b int, c int)`)
		tk.MustExec(`create table t2_issue60076 (a int, b int, c int)`)
		tk.MustExec(`create table t3_issue60076 (a int, b int, c int)`)
		tk.MustExec(`create table t4_issue60076 (a int, b int, c int)`)
		tk.MustExec(`set @@tidb_opt_always_keep_join_key=true`)

		tk.MustQuery(`explain format='plan_tree' select /* issue:60076 */ /*+ leading(t1_issue60076, t4_issue60076) */ * from t1_issue60076 join t2_issue60076 on
             t1_issue60076.a=t2_issue60076.a join t3_issue60076 on t1_issue60076.a=t3_issue60076.a join t4_issue60076 on t1_issue60076.a=t4_issue60076.a`).Check(testkit.Rows(
			`Projection root  test.t1_issue60076.a, test.t1_issue60076.b, test.t1_issue60076.c, test.t2_issue60076.a, test.t2_issue60076.b, test.t2_issue60076.c, test.t3_issue60076.a, test.t3_issue60076.b, test.t3_issue60076.c, test.t4_issue60076.a, test.t4_issue60076.b, test.t4_issue60076.c`,
			`└─HashJoin root  inner join, equal:[eq(test.t1_issue60076.a, test.t3_issue60076.a)]`,
			`  ├─TableReader(Build) root  data:Selection`,
			`  │ └─Selection cop[tikv]  not(isnull(test.t3_issue60076.a))`,
			`  │   └─TableFullScan cop[tikv] table:t3_issue60076 keep order:false, stats:pseudo`,
			`  └─HashJoin(Probe) root  inner join, equal:[eq(test.t1_issue60076.a, test.t2_issue60076.a)]`,
			`    ├─TableReader(Build) root  data:Selection`,
			`    │ └─Selection cop[tikv]  not(isnull(test.t2_issue60076.a))`,
			`    │   └─TableFullScan cop[tikv] table:t2_issue60076 keep order:false, stats:pseudo`,
			`    └─HashJoin(Probe) root  inner join, equal:[eq(test.t1_issue60076.a, test.t4_issue60076.a)]`,
			`      ├─TableReader(Build) root  data:Selection`,
			`      │ └─Selection cop[tikv]  not(isnull(test.t4_issue60076.a))`,
			`      │   └─TableFullScan cop[tikv] table:t4_issue60076 keep order:false, stats:pseudo`,
			`      └─TableReader(Probe) root  data:Selection`,
			`        └─Selection cop[tikv]  not(isnull(test.t1_issue60076.a))`,
			`          └─TableFullScan cop[tikv] table:t1_issue60076 keep order:false, stats:pseudo`))
		tk.MustQuery(`show warnings`).Check(testkit.Rows())

		tk.MustQuery(`explain format='plan_tree' select /* issue:63314 */ /*+ leading(t1_issue60076, t3_issue60076) */ 1 from
			t1_issue60076 left join t2_issue60076 on t1_issue60076.a=t2_issue60076.a join t3_issue60076 on t1_issue60076.b=t3_issue60076.b where t1_issue60076.a=1`).Check(testkit.Rows(
			`Projection root  1->Column`,
			`└─HashJoin root  left outer join, left side:HashJoin, equal:[eq(test.t1_issue60076.a, test.t2_issue60076.a)]`,
			`  ├─TableReader(Build) root  data:Selection`,
			`  │ └─Selection cop[tikv]  eq(test.t2_issue60076.a, 1)`,
			`  │   └─TableFullScan cop[tikv] table:t2_issue60076 keep order:false, stats:pseudo`,
			`  └─HashJoin(Probe) root  inner join, equal:[eq(test.t1_issue60076.b, test.t3_issue60076.b)]`,
			`    ├─TableReader(Build) root  data:Selection`,
			`    │ └─Selection cop[tikv]  eq(test.t1_issue60076.a, 1), not(isnull(test.t1_issue60076.b))`,
			`    │   └─TableFullScan cop[tikv] table:t1_issue60076 keep order:false, stats:pseudo`,
			`    └─TableReader(Probe) root  data:Selection`,
			`      └─Selection cop[tikv]  not(isnull(test.t3_issue60076.b))`,
			`        └─TableFullScan cop[tikv] table:t3_issue60076 keep order:false, stats:pseudo`))
		tk.MustQuery(`show warnings`).Check(testkit.Rows())
	})
}
