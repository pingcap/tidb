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
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
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
	ts := idxLookUpPlan.TablePlans[0].(*physicalop.PhysicalTableScan)

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
			"HashAgg_10 8000.00 root  group by:Column#7, Column#8, funcs:firstrow(Column#7)->Column#3, funcs:firstrow(Column#8)->Column#4",
			"└─TableReader_11 8000.00 root  data:HashAgg_4",
			"  └─HashAgg_4 8000.00 cop[tikv]  group by:cast(test.t7.c, bigint(22) BINARY), cast(test.t7.c, decimal(10,0) BINARY), ",
			"    └─TableFullScan_9 10000.00 cop[tikv] table:t7 keep order:false, stats:pseudo"))

	tk.MustExec("analyze table t7 all columns")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#11, Column#12, funcs:firstrow(Column#11)->Column#3, funcs:firstrow(Column#12)->Column#4",
			"└─Projection_14 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#11, cast(test.t7.c, bigint(22) BINARY)->Column#12",
			"  └─TableReader_13 2.00 root  data:TableFullScan_12",
			"    └─TableFullScan_12 2.00 cop[tikv] table:t7 keep order:false"))
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

	tk.MustQuery("explain format='brief' SELECT /*+ inl_join(tmp) */ * FROM ta, (SELECT b1, COUNT(b3) AS cnt FROM tb GROUP BY b1, b2) as tmp where ta.a1 = tmp.b1").
		Check(testkit.Rows(
			"Projection 9990.00 root  test.ta.a1, test.ta.a2, test.ta.a3, test.tb.b1, Column#9",
			"└─IndexJoin 9990.00 root  inner join, inner:HashAgg, outer key:test.ta.a1, inner key:test.tb.b1, equal cond:eq(test.ta.a1, test.tb.b1)",
			"  ├─TableReader(Build) 9990.00 root  data:Selection",
			"  │ └─Selection 9990.00 cop[tikv]  not(isnull(test.ta.a1))",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:ta keep order:false, stats:pseudo",
			"  └─HashAgg(Probe) 9990.00 root  group by:test.tb.b1, test.tb.b2, funcs:count(test.tb.b3)->Column#9, funcs:firstrow(test.tb.b1)->test.tb.b1",
			"    └─IndexLookUp 9990.00 root  ",
			"      ├─Selection(Build) 9990.00 cop[tikv]  not(isnull(test.tb.b1))",
			"      │ └─IndexRangeScan 10000.00 cop[tikv] table:tb, index:idx_b(b1) range: decided by [eq(test.tb.b1, test.ta.a1)], keep order:false, stats:pseudo",
			"      └─TableRowIDScan(Probe) 9990.00 cop[tikv] table:tb keep order:false, stats:pseudo"))
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

func TestIssue58476(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("CREATE TABLE t3 (id int PRIMARY KEY,c1 varchar(256),c2 varchar(256) GENERATED ALWAYS AS (concat(c1, c1)) VIRTUAL,KEY (id));")
	tk.MustExec("insert into t3(id, c1) values (50, 'c');")
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(`t3`)*/ id FROM `t3` WHERE c2 BETWEEN 'a' AND 'b' GROUP BY id HAVING id < 100 or id > 0;").Check(testkit.Rows())
	tk.MustQuery("explain format='brief' SELECT /*+ USE_INDEX_MERGE(`t3`)*/ id FROM `t3` WHERE c2 BETWEEN 'a' AND 'b' GROUP BY id HAVING id < 100 or id > 0;").
		Check(testkit.Rows(
			`Projection 249.75 root  test.t3.id`,
			`└─Selection 249.75 root  ge(test.t3.c2, "a"), le(test.t3.c2, "b")`,
			`  └─Projection 9990.00 root  test.t3.id, test.t3.c2`,
			`    └─IndexMerge 9990.00 root  type: union`,
			`      ├─IndexRangeScan(Build) 3323.33 cop[tikv] table:t3, index:id(id) range:[-inf,100), keep order:false, stats:pseudo`,
			`      ├─TableRangeScan(Build) 3333.33 cop[tikv] table:t3 range:(0,+inf], keep order:false, stats:pseudo`,
			`      └─TableRowIDScan(Probe) 9990.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
}

func TestIssue59643(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustQuery(`explain format='brief' SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT distinct 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;
`).Check(testkit.Rows(
		"HashJoin 2.00 root  inner join, equal:[nulleq(Column#5, Column#11)]",
		"├─HashAgg(Build) 2.00 root  group by:Column#5, Column#6, funcs:firstrow(Column#5)->Column#5, funcs:firstrow(Column#6)->Column#6",
		"│ └─Union 2.00 root  ",
		"│   ├─HashAgg 1.00 root  group by:1, funcs:firstrow(1)->Column#1, funcs:firstrow(\"Alice\")->Column#2",
		"│   │ └─TableDual 1.00 root  rows:1",
		"│   └─Projection 1.00 root  <nil>->Column#5, Bob->Column#6",
		"│     └─TableDual 1.00 root  rows:1",
		"└─HashAgg(Probe) 2.00 root  group by:Column#11, Column#12, funcs:firstrow(Column#11)->Column#11, funcs:firstrow(Column#12)->Column#12",
		"  └─Union 2.00 root  ",
		"    ├─Projection 1.00 root  1->Column#11, 100->Column#12",
		"    │ └─TableDual 1.00 root  rows:1",
		"    └─Projection 1.00 root  <nil>->Column#11, <nil>->Column#12",
		"      └─TableDual 1.00 root  rows:1"))
	tk.MustQuery(`SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT distinct 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;`).Sort().Check(testkit.Rows(
		"1 Alice 1 100",
		"<nil> Bob <nil> <nil>"))
	tk.MustQuery(`SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;`).Sort().Check(testkit.Rows(
		"1 Alice 1 100",
		"<nil> Bob <nil> <nil>"))
}

func TestIssue58451(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1 (a1 int, b1 int);")
	tk.MustExec("create table t2 (a2 int, b2 int);")
	tk.MustExec("insert into t1 values(1,1);")
	tk.MustQuery(`explain format='brief'
SELECT (4,5) IN (SELECT 8,0 UNION SELECT 8, 8) AS field1
FROM t1 AS table1
WHERE (EXISTS (SELECT SUBQUERY2_t1.a1 AS SUBQUERY2_field1 FROM t1 AS SUBQUERY2_t1)) OR table1.b1 >= 55
GROUP BY field1;`).Check(testkit.Rows("HashJoin 2.00 root  CARTESIAN left outer semi join, left side:HashAgg",
		"├─HashAgg(Build) 1.00 root  group by:Column#18, Column#19, funcs:firstrow(1)->Column#45",
		"│ └─TableDual 0.00 root  rows:0",
		"└─HashAgg(Probe) 2.00 root  group by:Column#10, funcs:firstrow(1)->Column#42",
		"  └─HashJoin 10000.00 root  CARTESIAN left outer semi join, left side:TableReader",
		"    ├─HashAgg(Build) 1.00 root  group by:Column#8, Column#9, funcs:firstrow(1)->Column#44",
		"    │ └─TableDual 0.00 root  rows:0",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:table1 keep order:false, stats:pseudo"))
	tk.MustQuery(`SELECT (4,5) IN (SELECT 8,0 UNION SELECT 8, 8) AS field1
FROM t1 AS table1
WHERE (EXISTS (SELECT SUBQUERY2_t1.a1 AS SUBQUERY2_field1 FROM t1 AS SUBQUERY2_t1)) OR table1.b1 >= 55
GROUP BY field1;`).Check(testkit.Rows("0"))
}

func TestIssue59902(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(a int primary key, b int);")
	tk.MustExec("create table t2(a int, b int, key idx(a));")
	tk.MustExec(`INSERT INTO t1 (a, b) VALUES (1, 100), (2, 200), (3, 300);`)
	tk.MustExec(`INSERT INTO t2 (a, b) VALUES (1, 10), (1, 20), (2, 30), (4, 40);`)
	tk.MustExec("set tidb_enable_inl_join_inner_multi_pattern=on;")
	tk.MustQuery("explain format='brief' select t1.b,(select count(*) from t2 where t2.a=t1.a) as a from t1 where t1.a=1;").
		Check(testkit.Rows(
			"Projection 8.00 root  test.t1.b, ifnull(Column#9, 0)->Column#9",
			"└─HashJoin 8.00 root  CARTESIAN left outer join, left side:Point_Get",
			"  ├─Point_Get(Build) 1.00 root table:t1 handle:1",
			"  └─StreamAgg(Probe) 8.00 root  group by:test.t2.a, funcs:count(Column#11)->Column#9",
			"    └─IndexReader 8.00 root  index:StreamAgg",
			"      └─StreamAgg 8.00 cop[tikv]  group by:test.t2.a, funcs:count(1)->Column#11",
			"        └─IndexRangeScan 10.00 cop[tikv] table:t2, index:idx(a) range:[1,1], keep order:true, stats:pseudo"))
	tk.MustQuery("select t1.b,(select count(*) from t2 where t2.a=t1.a) as a from t1 where t1.a=1;").Check(testkit.Rows("100 2"))
}

func TestIssue61118(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test;")
	tk1.MustExec("set global tidb_enable_instance_plan_cache = 1;")
	tk1.MustExec("create table t(a timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), b int, c int, primary key(a), unique key(b,c));")
	tk1.MustExec("insert into t(b,c) value (1,1);")
	tk1.MustExec("prepare stmt from 'update t set a = NOW(6) where b = ? and c = ?';")
	tk1.MustExec("set @a = 1;")
	tk1.MustExec("execute stmt using @a, @a;")
	tk1.MustExec("set time_zone='+1:00';")
	tk1.MustExec("execute stmt using @a, @a;")
	tk1.MustExec("execute stmt using @a, @a;")
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test;")
	tk2.MustExec("prepare stmt from 'update t set a = NOW(6) where b = ? and c = ?';")
	tk2.MustExec("set @a = 1;")
	tk2.MustExec("execute stmt using @a, @a;")
	tk2.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk2.MustExec("admin check table t;")
}

func TestIssue61303VirtualGenerateColumnSubstitute(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t0(id int default 1, c0 NUMERIC UNSIGNED ZEROFILL , c1 DECIMAL UNSIGNED  AS (c0) VIRTUAL NOT NULL UNIQUE);")
	tk.MustExec("insert ignore into t0(c0) values (null);")
	tk.MustQuery("select * from t0;").Check(testkit.Rows("1 <nil> 0"))

	tk.MustExec("set @@tidb_enable_unsafe_substitute=1")
	tk.MustExec("CREATE TABLE t1(id int default 1, c0 char(10) , c1 char(10) AS (c0) VIRTUAL NOT NULL UNIQUE);")
	tk.MustExec("insert ignore into t1(c0) values (null);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 <nil> "))
}

func TestJoinReorderWithAddSelection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`create table t0(vkey integer, c3 varchar(0));`)
	tk.MustExec(`create table t1(vkey integer, c10 integer);`)
	tk.MustExec(`create table t2(c12 integer, c13 integer, c14 varchar(0), c15 double);`)
	tk.MustExec(`create table t3(vkey varchar(0), c20 integer);`)
	tk.MustQuery(`explain select 0 from t2 join(t3 join t0 a on 0) left join(t1 b left join t1 c on 0) on(c20 = b.vkey) on(c13 = a.vkey) join(select c14 d from(t2 join t3 on c12 = vkey)) e on(c3 = d) where nullif(c15, case when(c.c10) then 0 end);`).Check(testkit.Rows(
		`Projection_34 0.00 root  0->Column#26`,
		`└─HashJoin_50 0.00 root  inner join, equal:[eq(Column#27, Column#28)]`,
		`  ├─HashJoin_71(Build) 0.00 root  inner join, equal:[eq(test.t0.c3, test.t2.c14)]`,
		`  │ ├─Selection_72(Build) 0.00 root  if(eq(test.t2.c15, cast(case(test.t1.c10, 0), double BINARY)), NULL, test.t2.c15)`,
		`  │ │ └─HashJoin_82 0.00 root  left outer join, left side:HashJoin_97, equal:[eq(test.t3.c20, test.t1.vkey)]`,
		`  │ │   ├─HashJoin_97(Build) 0.00 root  inner join, equal:[eq(test.t0.vkey, test.t2.c13)]`,
		`  │ │   │ ├─TableDual_107(Build) 0.00 root  rows:0`,
		`  │ │   │ └─TableReader_106(Probe) 9990.00 root  data:Selection_105`,
		`  │ │   │   └─Selection_105 9990.00 cop[tikv]  not(isnull(test.t2.c13))`,
		`  │ │   │     └─TableFullScan_104 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`,
		`  │ │   └─HashJoin_115(Probe) 9990.00 root  CARTESIAN left outer join, left side:TableReader_119`,
		`  │ │     ├─TableDual_120(Build) 0.00 root  rows:0`,
		`  │ │     └─TableReader_119(Probe) 9990.00 root  data:Selection_118`,
		`  │ │       └─Selection_118 9990.00 cop[tikv]  not(isnull(test.t1.vkey))`,
		`  │ │         └─TableFullScan_117 10000.00 cop[tikv] table:b keep order:false, stats:pseudo`,
		`  │ └─Projection_126(Probe) 9990.00 root  test.t2.c14, cast(test.t2.c12, double BINARY)->Column#27`,
		`  │   └─TableReader_130 9990.00 root  data:Selection_129`,
		`  │     └─Selection_129 9990.00 cop[tikv]  not(isnull(test.t2.c14))`,
		`  │       └─TableFullScan_128 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`,
		`  └─Projection_133(Probe) 10000.00 root  cast(test.t3.vkey, double BINARY)->Column#28`,
		`    └─TableReader_136 10000.00 root  data:TableFullScan_135`,
		`      └─TableFullScan_135 10000.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
}

func Test53401(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`
CREATE TABLE t9b3fcac6 (
    col_93 varchar(465) COLLATE utf8_bin NOT NULL,
    col_94 json NOT NULL,
    col_95 mediumint(9) NOT NULL DEFAULT '4417786',
    col_96 smallint(5) unsigned DEFAULT '57888',
    col_97 tinyint(3) unsigned DEFAULT NULL,
    col_98 timestamp NULL DEFAULT NULL,
    col_99 varchar(273) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT 'JK*(IJy9aL',
    col_100 json DEFAULT NULL,
    col_101 text COLLATE utf8_unicode_ci DEFAULT NULL,
    col_102 char(97) COLLATE utf8_unicode_ci DEFAULT 'w4)3SY0%A+hUcc',
    PRIMARY KEY (col_95,col_93(4)) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
`)
	tk.MustExec(`INSERT INTO t9b3fcac6 VALUES('z','[\"0fmCealP8z5ctrWZ1X8t9uUizSndBiBLrvKEKuDOc078CnwylgXxNaeb5B6nGjvD\", \"2DBUU2WsjKC1v6mrzVP5hLXJeLVf8bM3eQcZGw4r2Z12IBlP7r4n2DOqGkFaBNet\", \"nXCmzHrFr8MlhSz3ZW6PIumERCohyxxXRNAa38NAXcjA2pzXPmtib5szhonyiqkL\"]',6405159,32767,114,'1990-02-03 00:00:00','jvTZm','[608309881796116346, 2998732967437720473]','=7o功R眨%^藳e','v^t1o'),('0mfKd~#U^1$4','[\"iUdaKTJBIH5e49TBSEHkhjyloUN8x8Fogc22FH0rp6xDOEZfuXIzbWpnosyWRDJ6\", \"8wbCq5DYlBQxSaIhZJ6srGlAKXe53sJYlqxBwwfowvjfBmJLDBIcRJNNuMi4MyPC\"]',4087215,62501,NULL,'2007-05-30 00:00:00','','[5542356536871022092]','D肅柰B','!5oJbc7kEgxnuY%kBq'),('','[\"leNfcC30HUjX8y9PZfnrTgvF3qxgdQKrO0E728bS6oBT0RKNRevmXldIhQUmoeVv\"]',-7104045,30349,65,'2005-12-30 00:00:00','Gtci4yEn2kt)%r','[9223372036854775807]','=INn媏N','vdXdw5G_SOabACZJwc*'),('2(0os=WPdnzYsay=6YV','[\"3pW68EMaOJkDpcDGYgfCTwO1p4ZzL0kBhoRlQGQ4q5xK4qayzgTUAEtFW9B7JEBo\"]',-1363656,13440,170,'2004-01-04 00:00:00','A@2+CWtc','[4514556792909307241, 2716682828455316774, 3963699667060472281, 9118559670860280054]','桘)GI鉶','HH!zLzX'),('JP5K8N6)','[\"83FuNhRFUoMte94GTfbljhBJCpmA9omcfBbAAm4ko6kEIye0W89c5aMud1WHDTk0\", \"LOcwalNCYv6h8l5pQLRQttWNo5nH5FS1KanjIVWCWM2GW2MgaxMJd0HI5zVpsTOC\", \"9gSnqZWCmQ3YpxYf1k51hLkO93wQygOCZFGYbTEwc2m34t991sZigvJDW0zTjRwz\", \"NPTM0ui3vgRYlk8DizVpWzvtCP4nR9AYRjBrOTB8fvEE0lptMZUEg1hlqPrsxNbI\", \"mdPugMayAHYxlM7qs9DERht17IGV2rmkn4HtsTt5M3kTs08iXMP4rLyPUj9Rk6sf\"]',7306671,31161,233,'1988-10-24 00:00:00','DXT(k_=I24lHZXy','[1037757700653569707, 178668751284080222, 3427688697153590921, 6216052819124724468, 8985701126791813617]','O1R8c驿絳7刔p磦G钝','noCpWO&%QG4QB#'),('u1%trWnE%UX','[\"1qxNcDIdBy4zK2DTSywrVMwn6Ah4fNPjgG2cuULPrxiIXS6rlcCFYvUfzB3oMfMF\"]',7447050,65535,49,'2023-04-05 00:00:00','5Bo$iE2=+K(o#','[4366515210142289973, 8775437872579037136]','O4rcQ樫do','EjyC&w^tX'),('y','[\"DTOBpJPOMAX9rlwXyMuQFycsw77QXmmaYWeekDBcDK1JrftYW5lDKVbThjy0Xydm\", \"eFKXQpA6jJuFFCI8QryDrq0jDuSOsnwUKKCtDQiLStd182nWMV0ZEsbzhKdC0UQa\"]',-5010388,19273,255,'2014-12-10 00:00:00','#_znoo!Ij#8Q*','[8638850385058334544, 8985467472156528615, 6023961358938282770, 5188885607972701641, 6893277626174079507]','tT瓦L賫徘t=(~Z擣','()v6nb~1fiy#^av'),('Fw#5ojy3-$NkJqq','[\"Dev0z9SBMtLxiITJpMB13donA2I5YU33KBAtlDvoQQ5AqfrY3f9ZkAF4LC6RqxDy\", \"aMzeEu4eUUZTbMwtneHz7qTqSp7H3OAOYSBigclZ2tH3o7I03py2fm5V7E5qVr3P\", \"z1gcnDAkSmyL3m21BKyve4a1ZQbqIBqzCTXZbmtVZTH4ULOGEefeaLTuOx12VK6H\", \"Kv9LyTE2Gy4DlG96vJ99bFlv1GufTPuFtsmN8CUvXEMugLSMUoBZFeTebubyN1Lx\"]',-1532214,2,0,'2010-12-18 00:00:00','iXIj','[930087644122037056, 3156376937034819909, 6884958576846987238]','%0獩','M'),('n$Q~VDoFRMiekw%Bw','[\"388GDKfgotLjnraILMFc9tyKyV0cMAjnrXBJ8yVRThbzkZJJQtl9b9V77s7CW59v\", \"UgmGsU1xOjrmqlodwjgpjLIcG5wzPjMOYaYDQiOvV9aFwouXx7bzdS9wxZPnOJTK\", \"qOQyBr2hRjBz9PoavdD7Bh01SEYh7i312PJtqXuyH0Kv8ohKjZNgBoxqFe07LUjG\", \"wsbJNTQicognBTlYp7Efa6bnPA6y7osyiIZSy3x09jjST4MUw4cIyVK1vDS450ht\"]',13401,61522,235,'1993-01-24 00:00:00','LWR!9r9','[2]','k睼凙玎*7@汆F','7WM'),('hiGfeEA&+Po!3=$','[\"sdDP38fLgeNmptmyo4L7uh1zSIr1dlVuhpW3MGE2e1UY82GgEo0nyNhsk3QLVJ2W\", \"L3B1N0L1PGAEWw3Rmh6XVDDuCI4gzjRM4kNHPTesaQQRB01gJl95N7p39Y2Lbq14\", \"44C7POj68AUpRZ5i7Au7RpD5iAC1Jpvk94GzLPulYkj73DfgMAbhFi274LJ2ohnj\", \"3NLAa3A4FQ1gRYSxvLNxhEBVmcmxGMSlDbr0ivIHHqWdIhqP7bRxCkxXfLLtyFB6\", \"sRhG1kuDioLnlaEksHefNhWayD4pWpmLseAfR4u2tRIIaOWSDBWRe9mzQ5a4IcZL\"]',-3956337,50174,95,'1990-02-03 00:00:00','(6XV7BOe#','[-9223372036854775808, 7625448082258388031, 5339433082427528031, 7534153148241732028, 2798958529691733042]','懠0K蟎i','l3^f*SNH'),('dB5zfyUECU-EPgRmk7D','[\"2staCI2RxPedrtaZpbbOwaMwg5jH7B8fx9mcNXpUeR2D7HqjYi9GXAihpC0wf59E\", \"HOV6DU1ZzvVwKbFJrvHWwSDB07JHEFNvwQ5xxz5VGoJURWs4BVcdAIjkIbWXInFA\", \"f9j7cAxqes2AtFvXIMWnvHvF4JX3pxkdKkOoUKaozJsdCuakug7DzyZ0CreSH9OM\", \"P90oBu1kmE9yEzndFFkTrlTG6C9jrq55r7ieGMFyMMPedObwccWKAjDj5PB8bb8Z\", \"4FY7rHbBDzTcXMdXj0PAoCw10SVfk2s0WD0rqMfYhfXhTDuk52XcQrWxNhTu8eqc\"]',7856817,16811,127,'1990-11-26 00:00:00','+H','[834248507926156632, -1, 1640849447809642095]','_9绯Bk髗侢',''),('&r7I=','[\"h0EAvLDvsr7sCWGnjcaH97OM7ak4UNfDyNkvUv5oLYUXCIl7pSZIb5txniZGCTxF\"]',-6288155,25780,94,'1981-09-03 00:00:00','w+hn+mbtfe','[425660201725926795, 4617351755804262238]','X坟bdL)U厴','l7Sp@y+m0'),('QVVInFn','[\"PGs3WnnGf4synQD1O9wUWshFR02CJF1nwBi6eRbYIaz2MCP0fK4HEgHK09vxbaOa\", \"OaVuTNfdNHhWE52BWRkScqUPKAzyCcNlvdcNMIZOK3r1d3LIIeBE8HZ4sLgh4nXx\"]',-8388608,41327,51,'1999-09-26 00:00:00','VG!1Y*v5HBrWCt*','[939512667291538880, 9152550669728636268, 9223372036854775807, 1549364993331134749]','*曲7mH^将W觤yU','5w3=qOJ3Qb#s%10'),('8(WETu','[\"g8UFUmbVojM6KtmY7C5UcW0FRJjILtuH7kkYlQsfX0yccLrJ6sZQd2gRd4kqNcEw\", \"fRkV9OKaiqhkdls19durX5dF559RxKy8IS7gZ2jedL2ZzPlzZ67roBkiGTRVbz2e\", \"WSJUnpplPfohhcbJf8xOij9NQElJQaV1yl7FsvmwAnjf3RJHvdBz1cVM8mfO62U1\", \"S9dmVaTYdOj2sjkPTuq52ttJ9h83ij8ntZTQIfADw6hvNdeitcXdfMYyzUtpJAZt\"]',-5252170,32767,73,'1992-05-07 00:00:00','QI4+q','[6321175438639668638, 8728043852818489180, 257983153824447423, 8335388396420671030]','hZl',''),('qG','[\"QoYF34IDB0zkWuZMPGuOltqwDxceJle2SBDwEJ3JHmKlVGrqarjje1FrQTnwhJOr\", \"Dl677kQhOG2eSBPE2eAxofPuQmxuHZE3DHVKBMFZG75PMZ9OCXeFs96pUnK5s3HA\", \"zMnIqd1v4ngaAMM63BPjM6T8wB4ySfPvHsMxZnGreemasHQuiFbBlG7POgus7fHp\", \"mbkOFLuqp7Jl6AXiCeItsc93Pz5SVxaD96flon86Z587vuebrzOfL06UlpbBD8jF\"]',-1331154,56147,247,'2036-03-20 00:00:00','rw*+L','[920927076325831939, 6888932852710392585, 1185084253045073642, 8607883145274861808]','yEU恛','GQv_VJ3z'),('TsTnap2Y7T@E#l','[\"xYZHl47IyX99QDEx2KTKEpwe1Q1OJXR300lGGbVoToJZWgaMT1bC2qgUaQt8gmpj\", \"5EEJpTh7UDXBm6pDfDwUN3bPgiKEwaRwjwvNwVAUpM2ePh2h65Yu6FWWqJmPbNEA\", \"dw4emmBFg0t3FUDmdf8WxI8fY6j5Qs17S0d6GUpOYT3NkGGCCgGBb3qWcGUOSNqH\", \"WEHbvyzCtv9Vi1iGuYVd8DWckkCwLq1AGYDUAvCQFgvFthPE7WDMYNgbqsjuWtlf\"]',-3568084,57100,54,'2021-05-14 00:00:00','J+HUiX~iXLHY','[4675828756177477245, 2800971363410800672, 6159445527983634271]','9軦Xy^BBZm忚','9(cd-'),('Ny@Z_#@L2_bYT','[\"If02xxN1RJBbApSAYbwypyAwrGR21tiK98MqNOVGgaQJNXEHKv0ZPe1aXh3x4fsY\"]',-6291814,24743,120,'1976-11-23 00:00:00','ALW9X','[7424406150738625483, -1, 6796720091498330364]','n6Ro觵壊tp戗','=f$kf5tgvOP'),('I*mfpco*-G8','[\"yxjWFhEOqocBLIr5B4ZpAVfSfK8n13RdRVFRbzu5MHi6F8MucQ3tXlEqAHMhyn8r\", \"pbcbVPcyCn41kvQk8XtJBHhls84N7TNKB1preUHVOqII2RnBsfp6XzTvm7NiZiAd\", \"7uhyGzH2rNHVoLwZyPVKUuvwDqZQaXUXSGWRtlixJtAUsiOonVSrCnZlYk8p9pwz\"]',-978979,40872,135,'2029-10-02 00:00:00','','[5125913368019896763, 5159765984123000806, 5640524721671659659, 6900182434205347471]','ivYu銨)Ub桧d韄6颉wy抓军據','xw(dH&psEssKyzu'),('B0V@-vg0i','[\"vsjqK4V7w9xFqr35S8pDdaR7Ho024WOrKut7DbGnpu3chETejzFMPcQP6Rr4A10V\", \"cMPCyn1WGPAQ7BCkNBfe5noxM7miHgYNKApfmwI1WtOBucDkcRBmt1hmF4HxAl4f\"]',-8388607,48249,156,'2014-03-01 00:00:00','M~xvgpO','[-1, 2718592028912551075, 5445628347986142609]','AG臇娨鴇@2mW牲鴾','JzQE~R'),('qwWf1GI__1nY','[\"1vq5e2KGMHgXrcCTCqKXentWamWVgJssti4KHqJcfuNffBpRJgsxftYCaXkkkYxE\", \"SyVnOgqhU5MQPkepalM22RleuuXo7STHjgJAegpP0K3Zk4HwNLiQYuJfVHkwO3iZ\", \"sEeCYuSio5pB8OygBOXP2Tmqoa9nyAS4AFRrpEpuVDLZEZzqGadDGN4cdX4g3aAF\", \"5wCALtMClKX6JOSJSkRSWlH6GlBI5fU4MFGbgg1oIsDFwaFfMmGGe8eT6Sv2wfGC\"]',-2485701,4958,166,'2003-06-18 00:00:00','oFl','[9087177970999681501]','睓u7魲lM儅箅鷬喪鮙x3T','i)=deA'),('WR!LoYzuJL','[\"JIbW1Jxl2dXMV8qpyDVHhxYu9d12JH0215wB4dPLLJvbRNjkc44hCceKoyT4Bgxt\", \"UtDZolb2INKy3yRYeTPLqjJFf4FyKBhtLzCNEIWJC2NsdgK978KJSZpl43W756vG\"]',5276694,4585,254,'2009-02-20 00:00:00','MX','[-1, -1]','A^1B褁eQ%C物','uxu~8XdR2*@B+CO'),('qXPvd','[\"2zaPwTwQSrtC61QHNYcyYinE2G1tAPJALrwdSXcVQGGilFNbkGgrzc34kZnzioxy\"]',6598695,1465,192,'1975-06-04 00:00:00','%nSm(&greGfK#7#sbR8','[9193711983261496951, 5139429401120771997, 8401474205410874248]','E^僩pdMv掓炱悛怨幦d鿙x晆粂r湗','su&fLGAnp'),('JEZ~bpeQ!KE~p8#6','[\"E7fa5smrrdm0C4UCVlcUHrjvjhCycia2HcQP87kYZkqQH5bOkbdfWIziwH7bzjbl\", \"c1KaLD1TOfcxtJ0qgarZRTPSLaJ3ivE84iREuhZdOZodNMW2ganTaRYoS4xbz3s2\"]',-8388607,30342,26,NULL,'1$5uyG%*&nn2-nj6IE=','[8875604895319879337, 5041930015647580613, 6490166881579494674]','#&KO3)KSB飞纑5T+*ophv','OMmGxoKyhQT'),('oSilY*NT','[\"xF6ERA2ARxH1uhufC4ua4c2WgmGeU9yZ2a2Qs1214Eat8voMjWyzSDkOTA95Akk2\", \"KOiz8q8Szv5ik6Yy6c4qehEDywuBpDiFsanldK5yema1b65ixwzZ9j5yZR7LxzXq\", \"IseyT0aDowmyo8CBpgQPDmgXamT0iC1XYD7BeqBjFR4t8sdNGDZJh9E7xiR3IrPZ\", \"cyANUkk8T2izFXTn7ITdYDHJd2LBLqLZiowlz8Xy2SnJGzZJHDY0OlaveAbzMSrA\", \"Z8IzWA8UOfwm15aKo6Kc0GukITFCb69dLEZ19848ou6bfz7SAdWiCSVFsRg8OqjC\"]',-322012,40423,73,'1976-01-08 00:00:00','BCbvZJxF^5','[-9223372036854775808, 3428967849246334368, 7977261981066544354, 7812196963753825956]','!iX+)vW5d','l'),('A','[\"edr0OXI6Akt0omUWMSe4Tzvz75edm3UPxrAOCMwQ0zJXM8IEWpocQ3NKXVUO4zWr\", \"BqrNHvkX9wWhuCD4lum7yx4J2m417kcylcd8vh3CVhaNS2UhszoSbszxIW31BCgn\", \"b5cJFhavMuDF1S8tSeShcHenWSvRTDTXE5OowvHm16y7TU7KwNl2G0xjXdMLemMi\", \"WA9PXGt0xFE0VsEMSCCJyhIh44acbRjgotUm22QRAqvsCsnYCPhhyVe4Bns7fnJc\", \"2vDcvZVoTgp9MNUmd6a7eludQVtUt8E91YMzi9MHPYddzdVkVSjjVzBon2izcNrj\"]',5629666,55167,16,'2034-02-02 00:00:00','jU-DQ3hx2LtI','[3543203734902429753]','uy!掼','g^M5L3hRlAVXOTNG'),('','[\"O8HDvMRPNVLJelmD9OjyZQmSL2x86v5q2N20EQ6rwidNHE1zxgRYBq1DS1fJex0o\", \"zrNlJ8mJEXzzq21SsNY1LO51kxPDgxe0XUTMS5R8cHXcdZNLO7Nrz61V6h6CvbGC\", \"z0YTIEMPr4kPdK3uCzkZNvMzz8zYTyM9SRLUFgMTOOgV2ye4E8RdeMhax7ujIsp2\"]',4936577,4910,34,'1990-10-04 00:00:00','Px!','[2, 1985209121005937668]','硲hh(J4巸奲Nz黜M',''),('$lb6JBgZB$73!jHz&','[\"hlwhBuWEoKIhve0xQ0NixNO3yLxXnfL4gMmGfWV7RZLVwEUkg69CRtXZexYc9n2p\", \"02k9sZcp6dBLiYA2KULyfxy16cHIXUjI49s3FLSPyqfSkakRzwCnakq9y1ck5MqF\"]',2686691,17614,208,'1987-03-07 00:00:00','_yRQVgkF$S%C4wy','[6414889053238398652, 3057070678015301362, 1238608930599182443, 6067403430368315610]','O浾p',''),('T#47=','[\"IOxa0XxxSF38nyeQoKAc9cvdYLjPR7W64Ufzt2pAtVAbl82WAFeZjmPFVAT9VExR\", \"HLvXxOxtpqokcUTLmRzgIi8XSpIWav7PncCeqyPynjXyYzyHl3wLtLFo2y5ot0f2\", \"NItuUXkdJW4tObD2NmkXqKC2bG70e1qlBK2waDpn0ekHXtNfawGOOlYSfWBiPAPx\", \"DYjdn6irYsx8jjRG5wbYZ2qvtonfdFKkvvoaFlUsayBlXYwYFpylDYZ51JrjkiRN\", \"FqMmsev5yIq2KzDn0fuZjXJA733m5IgdFdIcKmV0TgO9d2Sp8XakW9JSH0Q14Nzw\"]',230117,48479,243,'2005-01-19 00:00:00','W','[1034656589193371250, 6865195971339489815, 790678402762120888, 3110065480741069650, 8461357537373164275]','梽1磴跖鋀vLG@禛_磊','3gQ0^ERRUyAC-4q'),('@A+Ud','[\"BhrzptjmmKih90WINyAxXAeJEstNqGh2du93pySa9LZii22nyTqNrs7gJaiIJWgs\"]',4546381,29114,102,'2000-04-23 00:00:00','l=L1n^~Edm8QO%g','[8205792377265767278, 2463812433793498578, 7460381856614636526, 5767605436111758578, 7216822580212459593]','C泵蕁F瑴O9','yjdsu3'),('=i6C-x7iw','[\"i1CzSJ9PBqAy1j9hmFxNvAHDhJ8ik8GEXIhYm5A4SozaIIk7tDYa59K3EGWCDTa4\"]',5266640,20949,158,'1980-02-12 00:00:00','t01^3%oq','[4658401958355918006, 5234388649311540355, 1, 2211110413702340319, 6747632489281331115]','hg隝2',''),('dG$Mkgh3b','[\"rD0rmWJv8LZIx6VcPfSUJ8iVM8NJN1j2aFjLVEtm04N3AgYPzfQG8zc7rhP7U75A\", \"s7s9hjTShYxPHF3hAhAK7nCXAM6gEWFKf3ktvm5kSee9Vp4g0CT0vRdFH7ZWGQk1\"]',-4050213,1346,227,'1990-08-19 00:00:00','*h$7KMx2FhDfw^','[-9223372036854775808, 7322307350564100203, 4639972565209812244, 2519644375466994458, 3860971406009674274]','身z)戣G%~磌)ix秷賚R+V嘔3',NULL),('4','[\"V3SFxBRzlyzfSXEkNETuYNvgsoUTXzCpNk8MMTNZgNRNNmWe0TLEK12Z8ZAkinWE\", \"1NcZsYnBF3aFlNd6AMJ0XajGvNzOLEZC6GIZcWn2wlSKawljREqJFt0pHygYW622\"]',-3505879,1,65,'1989-04-20 00:00:00','','[977321087715353397, 4948531435289281852, 7946216421359356542, 6171915185636005968, 2999604236840599973]','9v鋲jGY3!@糭Bo霈唬v2',')a%a6YpfM-MhmG'),('jO*(','[\"acUpj3apzdSCbi9U3jymsQgggPOoyBFv4XOoIqfJPPJ7U1dYSnkh32rfN6HwvF1u\", \"GLvSdx8ukdDLDgGzKS70jmlgizJGB78JW2dQRjs3XCIAriQNyQ0lyP0LT09pVoVU\", \"nDsVAaapvfr9WzaJNN7p4Dzlj3LT9Bh0bIotajo41emPky8lzNG99CgcVmsh4dKO\", \"Y9f1Ydl8B27RPmFjGcLWKuLY9xT983Yk7egc7bwbivBf6rBpsOciP2aONw26T4G1\", \"TXHQnMtF16bborZcXnxhdKfZz2NymU81dwJ4fpuhfXlQ9T1MvxvjxHDcPWODXZ6K\"]',-3794478,52968,1,'1988-03-18 00:00:00',NULL,'[1252331535406000583, 7455462900185355324]','tC祜剷3c澅d','hGSdvTaP'),('wXT~@VIwaeHsmt','[\"l7B5Uzu5Fh9nF4ZyFKYLSIg7GXdvuksQ1bf7LLOy917nOsaQoq007k4bLijhWJjj\"]',2,3354,28,'2035-08-19 00:00:00','O$jUa8qMx&Kf0','[878877403553131300, 6297331375588134833, 5112288929764464445, 4944967792622966527, 2]','nA$gVDI壤','#rCfh^=4fl2Zo(I^4!1'),('%','[\"JW33S3DYuFWf6rpojomtM1oKBSSy4cb2fpvcETLxPA1xD6op1CxURnbMNSlnbVAC\", \"Pkkv9sSZziELcLamqT129uqGbacEK8lv4frZbsfEHvB7mwEWMo8h8pirffWfIhrw\", \"wanQ4uFAXh55035Q8FJEy3OLVlnL8pwX0YPPdUhf286udVYceB3FMvS1OgFbvmuG\", \"LKTjg3LVbilKsIZUMEDYijeIJgIAYdBzwRzllwWFsM3wNuhZ4xyTf5v25mlTctKO\", \"O9yBvu2Ku2oGqGr0KkrDLh4FoiBOLpI6D2KYiRCeWlqHOJMsy6MBGdj2A6IfDvNS\"]',7260722,40573,47,'1979-01-04 00:00:00','kR-T3dSMz98!#','[3811190391237131857, 3190482107020074123]','eg坝yXO薝R+ZqR襅鱂','8f@UUlYlN_uHt'),('^A6LD0^n_2Cezj$#u@','[\"qpY4BQ2Tt6klQJxk0OOAB5GsPPXpWXy008C14JfU9MILjrcxQcEukwkhj2pr8QlS\", \"gZUSpPn2FtOzna5uNtvYLRcUaIuKx964FvfhMjiz1WjcOU0oFHWyc7JA4qAXYmXL\", \"8pDbDwznHUZznvKOh6ADDGoyspIOMesZGwBTRo9FnpoD4hQNUbUtpSExCgvuBbbM\", \"1XMlvI43tOxq8bdycxefq956qbng0xxALudPWu3o7FzjQMZYGOkpaJqpjqUsCvne\"]',-4464979,32768,127,'1971-11-29 00:00:00','!nz9~d6u)*#','[3066383052324308397]','wghuh#邏裰vLT幞a','crNGF!yjT-U897(^Xl'),('G8@wJQV(Oq(GAAA-','[\"DN1bPr8pzfyyLqCd301VaN7aJS4bXYu8AhEv23KVuaCBW7s8KQAnAl5jrTODyR22\", \"VEB1j1eoYo0LqYikqzgQJcms8v28Bzm32POeZvcv3vEJlaj9gSDTVjg7PPr4Ar8M\", \"eMWEnPvUTUC86voOUpDzvcP4AMbDf5HpWAt8EvnqsneLPjbCoWmbFDr8A8qqDhz6\"]',-3739012,63310,166,'1982-07-12 00:00:00','!4hovvJe','[2354488839378843227, 2652475561985447660, -9223372036854775808, 839350888659313926]','聯唌+椉','1#'),('CnZsQX^','[\"0P8nIrOqkxnydelLzNQhOap6bd0ahpMa6BhIxoSNR51MFMRUZcooVEnkLRvToBOq\", \"3RpeQRzActPZpXbL5JbSvhUu6xjx3H43Hd6wIqefkQOuRCxFZYWMZxbAlMBI9qjJ\", \"FJcyKEGNfg9qvitixrWDUyUdmlwH5zX2jRxkKNg7fLyzhPHmD7If38mu01Tnlhfm\", \"Xi7IfWKfoaByV7Z1FsiSsoSKuKU4d96ixmjbgi9pFCYUAd0OzryG9qgasnUvundN\", \"94zwu74thJ1Dd3nh4uCigwC7Nd5sgne0wGp08XKkBC6X5TrNjpd34UkUAHO5pyZ6\"]',6779467,41152,9,'2027-11-15 00:00:00','','[340379361942536094, 9142268622644611607, 6682328617510770263, 9223372036854775807, 8606630757903694714]','u炡麽','p6wjmw@05NU'),('BfO~sY*Q5na)c6%%eV4','[\"E68ys3XYQ0JNj4HyQdwqxkAw1PepqZcVnE1XzLPYroUHvCTYqKC0K416RKaDgHVw\", \"nOfipi5KLqeDsHHQZJWZPOyqsduyVFurptk2oA6vskeg6ykdYsUBbBQ4oIoEuVTl\", \"hzpyCp84QnN7GfIyllgc3ovWShvfCJQeTpFjaWYcEvbbdsljpBK7Z25uZUfVMI8b\", \"wfujqFzlsHpzS90Aipi8fpN9Wke79pQ4bOo3vKYsFYSQA9XJ3pZBjtUa466DWHZc\"]',8138832,57972,145,'2029-01-02 00:00:00','j!S+c!k','[7615966882037719930]','D$擫鷁','j346Omidat1cPvarr'),('ybDYVMybQgGok','[\"bTrdQI0dnCbSv91BphoHoTUjmEZ9E3ymqSIh7zOtayPm3nsd8F9hlX79U3YMgnTl\", \"OM8zcVN9mu1oH6fqvGEWzujHv30X0Vu0xjECsa6OwOn2JVXdLNWMcWKxW69rnlg9\", \"Ssv04X23EUO2VE5JvULihmy1M03OJC81bqh725LPPPOu3HrIHfqgp4lRGKgc1FV6\", \"fpNd7CL9LjWpLo18hj9UDbn6ezoMyKzV0EO4utvw4zSH27TfFPMNXh3MvPE7Usvk\"]',-1472425,10261,122,'2013-04-12 00:00:00','7(','[2225773763628105298, 1967746980431351682, 1463612188947542831, 3434579309392973687, 1497676780737942426]','~G8B挬谉=蹧嗍Az巐i蟧Y','*NLQkLYaE'),('W','[\"nJGVviHquDJNcWOczwEBtaJwdq62At9cXdly4b6g9401ToYFWrW92wAUOnYacPTa\"]',-329972,3495,198,'1998-08-26 00:00:00','9B2SPVb)Qg','[-1, 7651519811248237613, 1, 2762350798199797399]','&鋓t~苀Is就dj-1X-eI','IDfZRi)fGrw$0E(hUN'),('','[\"vmFAqZZcgfJWXjxa7DmjtsqZRM6uPqZszMo90ePqw9rCRgShpQ1xuh5S38mez00e\", \"t7vUvrr0OAHLr9ThTcR6YPNUmHdmtR1OuFRqfdpp5OwLWigcXdResIYBoLE3Rib4\", \"1vcQEG1kf7AkOu6rAwXt3MOYmcT0q9FyXnbPTr3ammaWyOPibfkXjJuSomQ10xAU\", \"IKCxccYhgzJDU50IloetrSg5UK98oVysTkK0pobLHRiROuKhWnKZ8RvffRUcYXmP\"]',-2363433,62986,130,'2022-11-08 00:00:00','c4&Bi8zi*QB*yTE','[7633410550157683564, 8397087682899479843, 5914459271346326187, 7103026505123843957]','C~3g8悴觔妫獶砐艿NIZkcVy2','oE%9QPjLr'),('KcAwYJm3p!W0','[\"Q5XSEc8IkGhovUuQxbgTginpVOTytJaHqCIiryYdh79ISQ65z16TJrCMJILMkikz\", \"dqyREeQ4y1v7dkj3Wt3WKpSxM0qfYDKH0gKTHUsUjDMf627MZ28nUbWUesvFxlG8\"]',-3897966,29406,191,'1998-07-23 00:00:00','w','[2255848579622157765, 9223372036854775807, 1338007287011664998, 8312442467100780601]','+p(袩霡f菽','qiuOeV#95'),('sC~','[\"q5Vff6ptOpNhF9qHCnGeWewa2ddAGv3wB1hyc3mJFC2mAXACguI1Vl3oM441G7eU\", \"0Loo6DHEAUMli2w1pdByLtRXpjMWdlJjpiBBKIavxIQMpKArSkV0Lio2tmQLRQA8\", \"G7tVZ1vbeBvP3kgYyrWV3HomXtEXOmckq9nEM7MpjlkGR4HbW80xnota8f39falN\"]',7641496,7827,255,'1989-02-28 00:00:00','84=yI-)ZIqrrWK=78','[-9223372036854775807, 1826186808363170009]','锩U&昸P4','epn_Uq-mzVZsZtm'),('oB-NL','[\"PdhDbIAiARWqQRi19UQnvwObSGlNOkmT6EXcRL9Et4hO0nnDBubGHkWHzL9sUaen\"]',1659855,33001,17,'1985-10-29 00:00:00','&TjnWon*hj','[7521049878305267997, 2431954403794204873, 906181863189448518]','q编y粼vrK-h竁Ez','a2q)m6zdG7'),('urGR_p4zuwn_%J','[\"AJOWwFL4z61yQqcpmZAw8w0KDtNjyE22zY3Ksj8u5deoDJdiNTW7F4HzE2H4LbMA\", \"rN7rnWt7322N4C1V0UskDwlxtFSN8x4m5cIGN2iFMLfWRlUHm0dI9LtySe3okx6q\"]',-8103588,26273,1,'1991-05-29 00:00:00','Ip2(dFGiasx^','[7102804973261977010]','亽K濛','g6aDX)+3e'),('','[\"bcBpIOzuJZps13wg257Lnlx6uQzauNlFJFEcLf2X8J2WjFi0phGcJHowbalvA12B\", \"V4OKZjrjFYiRFpwn2M8sueZe9kOHT7P5L2ATsVdAiA0dLIcdWR06o856a0qwhVBA\", \"QAokjmaDoXIMgXMIkXxbbcD5MaK3DSsMrNNYMlN0kCwAx3UuvQ8ZhzTFRAZLXnGs\"]',7630929,39517,95,'2002-08-28 00:00:00','Uth)=tc','[3295755682795132677, 7109348944539992211]','g0B','IKkSw01'),('4*NdKWDnH!lr(','[\"z7Uk5hLwcfQlJ9NfsECLV0hxov8fRhqLc1ebljRXlWuSF4EsfqbI1dEF6u08ZmYF\", \"1xRJ7LvZriHTCRN0fEIbFXOY1eLS2pO3ug3ymP2c3jRNAtxTZtK4gqtkBV9u7nVE\", \"SLnN8t211FuQzM3XC8XWMkJFPAVhZR8XZizmBe4we3ZXCuxxkRKVri2dxfaF1Bu1\", \"RfvQQfWkJBeOYEBPj4qwNrAmDYD1yEqmNEkklbQlDwtp1QXJ9amLhzRJiZFhejb5\"]',-4907343,21960,39,'2016-03-17 00:00:00','k~rhrFUyljN@w5PiuZ','[6417148721221703249, 881357039711824060]','F凵QtgQr$Jc鵴+礔_倻','_'),('LglS3ww3','[\"wWSgOFH8gtIHvfF4aIPbRZ1s0H2EmZtBjfTnls7Hc4hnFnuOAChhMjttiaMrKF2u\", \"9JZ004hp3Wcyy0tKIXvElee0wj6tJSkCCbsPBjB55WdvZVhRPdRHHt0UKGrl9Ytg\", \"M5skx56foKlW5g5ufh8GNpO9PobETd1OhXb5nckhHRTpMcBHCsQgVkqXBJcuwCdi\", \"WQ0CD01qn57jqryCH0JgUYP0DGrQmzp8miCSeokV7EM0v1T2EwbpnvR0Z4nFNv2j\", \"UTaT8LqboXezHY5b6stMR6wueVhSBHyE0iimotDCbxTS6GxS8YfjpFKDvCaPeCP1\"]',1568136,49641,215,'2016-05-02 00:00:00','Yq1Awura!Z*zA3','[8972080584317275972, 3618564743598882682, 7567973133130383362]','*5Z5XPe膄W3赾K禞土砾zW櫼醓','-46'),('wp0','[\"fLY7MNWNs26c4xKU34wY1woTVCTmpjYv6OoD5WJEooNZnO3LzlDaH9v73D9mbqq8\", \"Z7EFK9C0uMzlTDrWpVdZgHB8xHYY39FKm3elhXgF3WlTk9FojjPdlpj0NoJl8MqR\", \"xlUxWMURtA0QGnWq4vvFDcAf1GOQ4oS9bjQkrdBQNB2nVMaVCtjqotkS1v5jRj3c\"]',-8323524,1,12,'2015-10-28 00:00:00','!TaeI#uc3','[22510255974661459]','6ej蔋鐸xI鈔xEN5O困哻I','C*vgQvgm6lr7j'),('o(03YJNz^^qf4ZsZe','[\"dX50o9PTrtyvWsrSgrChHlw9sxUVt49OZ6cdWeSHKpViqGjbDnmjOw0LJktS6O6H\", \"kseWgyBkbuiN6ebaPcJMuVbmLVyQZbJRSsgbW9AztVMbW4gMc02IT2RZWURvdqJo\", \"LAipe4dM8QdVBf92cy5gqJPsWwdaBO9oaiEX0vNeQQ9iTng0WA29kqenMU293SD5\", \"n057DPs4e8o1Q3ZG9ymrNb1eistmj1ZHeknDSNSKtQTgk5d2HG3ngQQL6ToV0HQH\"]',-2054034,47057,10,'2028-09-27 00:00:00','AipHf','[8122203815387924654]','JKmxA','C^%-V9'),('Ro','[\"6e03nT6umn4c8E2XLEprtsoPdxFoSImp1tL3TQw6Crnla47NH0x5ZVjN2dLdwrZL\", \"v4eEuLPDlzcMEIIm99GJcYFAOxKbkFnAaeEOASHukOXAtOrhnS4SGAcyN8fTGOaj\"]',103077,12720,173,'2000-11-12 00:00:00','6LS)','[3692843628273468174]','枲','m@YA'),('dvYWl)u','[\"fGu7XCFN3ZUMl95bYTpAoQLjCxzGcDME1SSFen6KiRd6wIabs4a1M55XEIguLgoX\", \"YZoQ1c6JOwBFSpuPxM45rfl81cJRBkLc8uMMpreEZ5lc5O42n2WMZEXTFZy0J0vk\", \"Na1B109CNLq1veBDL9X57mhE6GhhBLfPkFWOYktTZ6ksmI46QXeab8aG8TkFC2Fd\", \"DgQbfhg4sqXBIarGBQhFzpBEQBqO7mctXa3cfJ9Iw5AzTLK1mKr8kUhF53iO3TAE\", \"6rlzbSvDUTZE3DvTgw4ueAu3wbKHLBEWK0xfpsMh38Yqk5riNmGCq7BnYA77mi0n\"]',178510,22985,NULL,'2014-03-12 00:00:00','R$yoQ+!==L9Twu','[1174429955639074907, 5951795682879472730]','cw_&鴞4=9棬(Mu','Gs3EbI(SsX(Y$$H&kv'),('A)ghP','[\"o2kXXmhA0LpLtHdiLjWwJCrD2A7tWOFhmnCLeHwrFvPMMtLzsveUbjv9bToEiCWn\", \"U7liE5wA7HmXHz6bJibwbEfLse1FNWI3DRCMOEwP97Cjk13k8elMPQsiGxq8LcDJ\", \"lLvr7hmScmQkgR3FWrWK9DW3xb0bNVZIC92CHjoB0XMvudQSYB4bdGNHzCipco44\"]',8186253,13092,66,NULL,'+nwZu)1%Ty)!5kN','[7196556907333669137, 5244069320951386901, 9223372036854775807, 4154431631376327613]','pdcKCNH5o濨篑1q镮爣K跥鈚','U77@8JBgk8'),('G5ZLPlzik','[\"NQlL7c2PeQyFgD4NrU1EkuRQeYnoMBFA3Yntjkjo5YTifvNxsmG3oV4h5FAVMulX\"]',6626073,60846,107,'1976-02-13 00:00:00','_wXCUZ8))71~II~Ntt','[3863611462052356966, 0, -1, 8801084243252844004]','&悕R5x3VTJ嶩(戅怾眼险x=迌X','Y9y(-iwL-'),('Qb)','[\"OCLQenKFJ7RIX5sK0uNgqQ55doP2skRl995mH5BeB9eZ70UPojrcbbl3ZHSsAvT9\", \"3qjOaYGMaftz5tzdbgQKiPMA2i4KooKcfttUuq0uXLXJTUCKURdkwS6AwJ9JsrDX\", \"rFYNVFh2sezT3pVoInYZyjPhtWYO5XnaE9XitKjq2gHlRwYWeKcvhjdw0wT5aty4\", \"mtDp18JoaLohYmHOzfniR7GlMVoMyPXFtKc6FDzSZuV5HVUJh9aXu04E08VWOVFn\"]',-6249982,32767,0,'2029-08-04 00:00:00','2sE*LA%vgqewzxI-z+','[1083615363858371983, 8924663031337915647, 5978940559831513877, 5685413282400034004]','N頕Lo28鮥mt',''),('P','[\"IOdbtPhzj5kcG3jFjjYmpEIR6NaCHBp7il5hDUp0QvLEeVeyqneQvnxjytHj8B0j\", \"VwuYdiXaY1ZYwyRMquHAnSSw99TRntSBtKgim81Na3OooCR6hiVNnLuVOt1M9CJe\", \"pkrgupI6yUofrOLxmTwwVjswu4095hETr3d4u1YtFc7vgGHEgYL5r69zn5nueyK4\", \"oYzzvMeqr7XKqvSkayXiG8vNMJqiiNCbtD63lDAQ6hMUxQhhFgzRYukFyWt4bIkn\"]',8291232,11502,63,'2019-07-08 00:00:00','FiqQnB_lHy&)s','[6895710193136813073, 7183858777706028905, 8040269749860663283, 8086385906088899990, 5809837948452516448]','q','Z'),('+fetv=jUS','[\"hGuYRGuVdGCvSMcLwJi7Yv6qcV1o00AklkRxLh6dg6BkY5Cy2Vxb9zgmt1tahP1D\", \"5K7BLfzn71QUYCQNelKueaitEjjTKBF8NGSMSfrJRNNUl3NYnT94lth1wmDEi2cX\"]',-7569841,1,238,'2004-02-04 00:00:00','5pohlKw%S4','[-1, 2463925369618542282, 4145233564084591605, 1590518192082047170, 1233005025938714787]','o','9wEEmjk');`)
	tk.MustQuery(`explain SELECT
    derived_table.r1 <= 'kdAP@*^~Z!(0'
FROM (
    SELECT
        TO_BASE64(t9b3fcac6.col_99) AS r1
    FROM t9b3fcac6
    GROUP BY t9b3fcac6.col_99
) AS derived_table`).Check(testkit.Rows())
}
