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

	var idxLookUpPlan *physicalop.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*physicalop.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(base.PhysicalPlan).Children()[0]
	}
	require.True(t, ok)

	is := idxLookUpPlan.IndexPlans[0].(*physicalop.PhysicalIndexScan)
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
		"├─HashAgg(Build) 1.00 root  group by:Column#21, Column#22, funcs:firstrow(1)->Column#48",
		"│ └─TableDual 0.00 root  rows:0",
		"└─HashAgg(Probe) 2.00 root  group by:Column#10, funcs:firstrow(1)->Column#45",
		"  └─HashJoin 10000.00 root  CARTESIAN left outer semi join, left side:TableReader",
		"    ├─HashAgg(Build) 1.00 root  group by:Column#8, Column#9, funcs:firstrow(1)->Column#47",
		"    │ └─TableDual 0.00 root  rows:0",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:table1 keep order:false, stats:pseudo",
		"ScalarSubQuery N/A root  Output: ScalarQueryCol#14, ScalarQueryCol#15, ScalarQueryCol#16",
		"└─TableReader 10000.00 root  data:TableFullScan",
		"  └─TableFullScan 10000.00 cop[tikv] table:SUBQUERY2_t1 keep order:false, stats:pseudo"))
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
			"Projection 1.00 root  test.t1.b, ifnull(Column#9, 0)->Column#9",
			"└─MergeJoin 1.00 root  left outer join, left side:Point_Get, left key:test.t1.a, right key:test.t2.a",
			"  ├─StreamAgg(Build) 8.00 root  group by:test.t2.a, funcs:count(Column#10)->Column#9, funcs:firstrow(test.t2.a)->test.t2.a",
			"  │ └─IndexReader 8.00 root  index:StreamAgg",
			"  │   └─StreamAgg 8.00 cop[tikv]  group by:test.t2.a, funcs:count(1)->Column#10",
			"  │     └─IndexRangeScan 10.00 cop[tikv] table:t2, index:idx(a) range:[1,1], keep order:true, stats:pseudo",
			"  └─Point_Get(Probe) 1.00 root table:t1 handle:1"))
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

func TestOnlyFullGroupCantFeelUnaryConstant(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int);")
		testKit.MustQuery("select a,min(a) from t where a=-1;").Check(testkit.Rows("<nil> <nil>"))
		testKit.MustQuery("select a,min(a) from t where -1=a;").Check(testkit.Rows("<nil> <nil>"))
	})
}

func TestIssue56615(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE test_table (id INT PRIMARY KEY,value1 INT,value2 INT);")
	tk.MustExec(`CREATE GLOBAL BINDING FOR
SELECT t1.id, IFNULL(t1.value1, 0) AS value1, IFNULL(t2.value2, 0) AS value2
FROM test_table t1
JOIN test_table t2 ON t1.id = t2.id
USING
SELECT /*+ HASH_JOIN(t1, t2) */ t1.id, IFNULL(t1.value1, 0) AS value1, IFNULL(t2.value2, 0) AS value2
FROM test_table t1
JOIN test_table t2 ON t1.id = t2.id;
`)
	tk.MustQuery(`SELECT t1.id, IFNULL(t1.value1, 0) AS value1, IFNULL(t2.value2, 0) AS value2
FROM test_table t1
JOIN test_table t2 ON t1.id = t2.id;
`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk.MustQuery(`select original_sql from mysql.bind_info`).Check(testkit.Rows(
		"builtin_pseudo_sql_for_bind_lock",
		"SELECT `t1`.`id`,IFNULL(`t1`.`value1`, 0) AS `value1`,IFNULL(`t2`.`value2`, 0) AS `value2` FROM `test`.`test_table` AS `t1` JOIN `test`.`test_table` AS `t2` ON `t1`.`id` = `t2`.`id`",
	))
}
