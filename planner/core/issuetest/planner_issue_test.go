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
	"testing"

	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/size"
)

func TestIssue44051(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1(id int,col1 varchar(10),col2 varchar(10),col3 varchar(10));")
	tk.MustExec("CREATE TABLE t2(id int,col1 varchar(10),col2 varchar(10),col3 varchar(10));")
	tk.MustExec("INSERT INTO t1 values(1,NULL,NULL,null),(2,NULL,NULL,null),(3,NULL,NULL,null);")
	tk.MustExec("INSERT INTO t2 values(1,'a','aa','aaa'),(2,'b','bb','bbb'),(3,'c','cc','ccc');")

	rs := tk.MustQuery("WITH tmp AS (SELECT t2.* FROM t2) SELECT * FROM t1 WHERE t1.id = (select id from tmp where id = 1) or t1.id = (select id from tmp where id = 2) or t1.id = (select id from tmp where id = 3)")
	rs.Sort().Check(testkit.Rows("1 <nil> <nil> <nil>", "2 <nil> <nil> <nil>", "3 <nil> <nil> <nil>"))
}

func TestIssue42732(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1 (a INT, b INT)")
	tk.MustExec("CREATE TABLE t2 (a INT, b INT)")
	tk.MustExec("INSERT INTO t1 VALUES (1, 1)")
	tk.MustExec("INSERT INTO t2 VALUES (1, 1)")
	tk.MustQuery("SELECT one.a, one.b as b2 FROM t1 one ORDER BY (SELECT two.b FROM t2 two WHERE two.a = one.b)").Check(testkit.Rows("1 1"))
}

// https://github.com/pingcap/tidb/issues/45036
func TestIssue45036(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE ads_txn (\n" +
		"  `cusno` varchar(10) NOT NULL,\n" +
		"  `txn_dt` varchar(8) NOT NULL,\n" +
		"  `unn_trno` decimal(22,0) NOT NULL,\n" +
		"  `aml_cntpr_accno` varchar(64) DEFAULT NULL,\n" +
		"  `acpayr_accno` varchar(35) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`cusno`,`txn_dt`,`unn_trno`) NONCLUSTERED\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY LIST COLUMNS(`txn_dt`)\n" +
		"(PARTITION `p20000101` VALUES IN ('20000101'),\n" +
		"PARTITION `p20220101` VALUES IN ('20220101'),\n" +
		"PARTITION `p20230516` VALUES IN ('20230516'))")
	tk.MustExec("analyze table ads_txn")
	tk.MustExec("set autocommit=OFF;")
	tk.MustQuery("explain update ads_txn s set aml_cntpr_accno = trim(acpayr_accno) where s._tidb_rowid between 1 and 100000;").Check(testkit.Rows(
		"Update_5 N/A root  N/A",
		"└─Projection_6 8000.00 root  test.ads_txn.cusno, test.ads_txn.txn_dt, test.ads_txn.unn_trno, test.ads_txn.aml_cntpr_accno, test.ads_txn.acpayr_accno, test.ads_txn._tidb_rowid",
		"  └─SelectLock_7 8000.00 root  for update 0",
		"    └─TableReader_9 10000.00 root partition:all data:TableRangeScan_8",
		"      └─TableRangeScan_8 10000.00 cop[tikv] table:s range:[1,100000], keep order:false, stats:pseudo"))
}

func TestIssue45758(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE tb1 (cid INT, code INT, class VARCHAR(10))")
	tk.MustExec("CREATE TABLE tb2 (cid INT, code INT, class VARCHAR(10))")
	// result ok
	tk.MustExec("UPDATE tb1, (SELECT code AS cid, code, MAX(class) AS class FROM tb2 GROUP BY code) tb3 SET tb1.cid = tb3.cid, tb1.code = tb3.code, tb1.class = tb3.class")
}

func TestIssue46083(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TEMPORARY TABLE v0(v1 int)")
	tk.MustExec("INSERT INTO v0 WITH ta2 AS (TABLE v0) TABLE ta2 FOR UPDATE OF ta2;")
}

func TestIssue47781(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2")
	tk.MustExec("create table t (id int,name varchar(10))")
	tk.MustExec("insert into t values(1,'tt')")
	tk.MustExec("create table t1(id int,name varchar(10),name1 varchar(10),name2 varchar(10))")
	tk.MustExec("insert into t1 values(1,'tt','ttt','tttt'),(2,'dd','ddd','dddd')")
	tk.MustExec("create table t2(id int,name varchar(10),name1 varchar(10),name2 varchar(10),`date1` date)")
	tk.MustExec("insert into t2 values(1,'tt','ttt','tttt','2099-12-31'),(2,'dd','ddd','dddd','2099-12-31')")
	tk.MustQuery(`WITH bzzs AS (
  SELECT
    count(1) AS bzn
  FROM
    t c
),
tmp1 AS (
  SELECT
    t1.*
  FROM
    t1
    LEFT JOIN bzzs ON 1 = 1
  WHERE
    name IN ('tt')
    AND bzn <> 1
),
tmp2 AS (
  SELECT
    tmp1.*,
    date('2099-12-31') AS endate
  FROM
    tmp1
),
tmp3 AS (
  SELECT
    *
  FROM
    tmp2
  WHERE
    endate > CURRENT_DATE
  UNION ALL
  SELECT
    '1' AS id,
    'ss' AS name,
    'sss' AS name1,
    'ssss' AS name2,
    date('2099-12-31') AS endate
  FROM
    bzzs t1
  WHERE
    bzn = 1
)
SELECT
  c2.id,
  c3.id
FROM
  t2 db
  LEFT JOIN tmp3 c2 ON c2.id = '1'
  LEFT JOIN tmp3 c3 ON c3.id = '1';`).Check(testkit.Rows("1 1", "1 1"))
}

func TestIssue51560(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists A, B, C")
	tk.MustExec("create table A(a int primary key, b int);")
	tk.MustExec("create table B(b int primary key);")
	tk.MustExec("create table C(c int primary key, b int);")
	tk.MustExec("insert into A values (2, 1), (3, 2);")
	tk.MustExec("insert into B values (1), (2);")

	tk.MustQuery("select b.b from A a left join (B b left join C c on b.b = c.b) on b.b = a.b where a.a in (2, 3);").Sort().Check(testkit.Rows("1", "2"))
	tk.MustQuery("select b.b from A a left join (B b left join C c on b.b = c.b) on b.b = a.b where a.a in (2, 3, null);").Sort().Check(testkit.Rows("1", "2"))
}

func TestIssue50614(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt(a bigint, b bigint, c bigint, d bigint, e bigint, primary key(c,d));")
	tk.MustQuery("explain format = brief " +
		"update tt, (select 1 as c1 ,2 as c2 ,3 as c3, 4 as c4 union all select 2,3,4,5 union all select 3,4,5,6) tmp " +
		"set tt.a=tmp.c1, tt.b=tmp.c2 " +
		"where tt.c=tmp.c3 and tt.d=tmp.c4 and (tt.c,tt.d) in ((11,111),(22,222),(33,333),(44,444));").Check(
		testkit.Rows(
			"Update N/A root  N/A",
			"└─Projection 0.00 root  test.tt.a, test.tt.b, test.tt.c, test.tt.d, test.tt.e, Column#18, Column#19, Column#20, Column#21",
			"  └─IndexJoin 0.00 root  inner join, inner:TableReader, outer key:Column#20, Column#21, inner key:test.tt.c, test.tt.d, equal cond:eq(Column#20, test.tt.c), eq(Column#21, test.tt.d), other cond:or(or(and(eq(Column#20, 11), eq(test.tt.d, 111)), and(eq(Column#20, 22), eq(test.tt.d, 222))), or(and(eq(Column#20, 33), eq(test.tt.d, 333)), and(eq(Column#20, 44), eq(test.tt.d, 444)))), or(or(and(eq(test.tt.c, 11), eq(Column#21, 111)), and(eq(test.tt.c, 22), eq(Column#21, 222))), or(and(eq(test.tt.c, 33), eq(Column#21, 333)), and(eq(test.tt.c, 44), eq(Column#21, 444))))",
			"    ├─Union(Build) 0.00 root  ",
			"    │ ├─Projection 0.00 root  1->Column#18, 2->Column#19, 3->Column#20, 4->Column#21",
			"    │ │ └─TableDual 0.00 root  rows:0",
			"    │ ├─Projection 0.00 root  2->Column#18, 3->Column#19, 4->Column#20, 5->Column#21",
			"    │ │ └─TableDual 0.00 root  rows:0",
			"    │ └─Projection 0.00 root  3->Column#18, 4->Column#19, 5->Column#20, 6->Column#21",
			"    │   └─TableDual 0.00 root  rows:0",
			"    └─TableReader(Probe) 0.00 root  data:Selection",
			"      └─Selection 0.00 cop[tikv]  or(or(and(eq(test.tt.c, 11), eq(test.tt.d, 111)), and(eq(test.tt.c, 22), eq(test.tt.d, 222))), or(and(eq(test.tt.c, 33), eq(test.tt.d, 333)), and(eq(test.tt.c, 44), eq(test.tt.d, 444)))), or(or(eq(test.tt.c, 11), eq(test.tt.c, 22)), or(eq(test.tt.c, 33), eq(test.tt.c, 44))), or(or(eq(test.tt.d, 111), eq(test.tt.d, 222)), or(eq(test.tt.d, 333), eq(test.tt.d, 444)))",
			"        └─TableRangeScan 0.00 cop[tikv] table:tt range: decided by [eq(test.tt.c, Column#20) eq(test.tt.d, Column#21)], keep order:false, stats:pseudo",
		),
	)
}

func TestQBHintHandlerDuplicateObjects(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t_employees  (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, fname VARCHAR(25) NOT NULL, lname VARCHAR(25) NOT NULL, store_id INT NOT NULL, department_id INT NOT NULL);")
	tk.MustExec("ALTER TABLE t_employees ADD INDEX idx(department_id);")

	// Explain statement
	tk.MustQuery("EXPLAIN WITH t AS (SELECT /*+ inl_join(e) */ em.* FROM t_employees em JOIN t_employees e WHERE em.store_id = e.department_id) SELECT * FROM t;")
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestCTETableInvaildTask(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE p ( groupid bigint(20) DEFAULT NULL, KEY k1 (groupid));")
	tk.MustExec(`CREATE TABLE g (groupid bigint(20) DEFAULT NULL,parentid bigint(20) NOT NULL,KEY k1 (parentid),KEY k2 (groupid,parentid));`)
	tk.MustExec(`set tidb_opt_enable_hash_join=off;`)
	tk.MustQuery(`explain WITH RECURSIVE w(gid) AS (
  SELECT
    groupId
  FROM
    p
  UNION
  SELECT
    g.groupId
  FROM
    g
    JOIN w ON g.parentId = w.gid
)
SELECT
  1
FROM
  g
WHERE
  g.groupId IN (
    SELECT
      gid
    FROM
      w
  );`).Check(testkit.Rows(
		"Projection_54 9990.00 root  1->Column#17",
		"└─IndexJoin_59 9990.00 root  inner join, inner:IndexReader_58, outer key:test.p.groupid, inner key:test.g.groupid, equal cond:eq(test.p.groupid, test.g.groupid)",
		"  ├─HashAgg_75(Build) 12800.00 root  group by:test.p.groupid, funcs:firstrow(test.p.groupid)->test.p.groupid",
		"  │ └─Selection_72 12800.00 root  not(isnull(test.p.groupid))",
		"  │   └─CTEFullScan_73 16000.00 root CTE:w data:CTE_0",
		"  └─IndexReader_58(Probe) 9990.00 root  index:Selection_57",
		"    └─Selection_57 9990.00 cop[tikv]  not(isnull(test.g.groupid))",
		"      └─IndexRangeScan_56 10000.00 cop[tikv] table:g, index:k2(groupid, parentid) range: decided by [eq(test.g.groupid, test.p.groupid)], keep order:false, stats:pseudo",
		"CTE_0 16000.00 root  Recursive CTE",
		"├─IndexReader_24(Seed Part) 10000.00 root  index:IndexFullScan_23",
		"│ └─IndexFullScan_23 10000.00 cop[tikv] table:p, index:k1(groupid) keep order:false, stats:pseudo",
		"└─IndexHashJoin_34(Recursive Part) 10000.00 root  inner join, inner:IndexLookUp_31, outer key:test.p.groupid, inner key:test.g.parentid, equal cond:eq(test.p.groupid, test.g.parentid)",
		"  ├─Selection_51(Build) 8000.00 root  not(isnull(test.p.groupid))",
		"  │ └─CTETable_52 10000.00 root  Scan on CTE_0",
		"  └─IndexLookUp_31(Probe) 10000.00 root  ",
		"    ├─IndexRangeScan_29(Build) 10000.00 cop[tikv] table:g, index:k1(parentid) range: decided by [eq(test.g.parentid, test.p.groupid)], keep order:false, stats:pseudo",
		"    └─TableRowIDScan_30(Probe) 10000.00 cop[tikv] table:g keep order:false, stats:pseudo"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
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

func TestDisableReuseChunk(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int primary key, c2 mediumtext);")
	tk.MustExec(`insert into t1 values (1, "abc"), (2, "def");`)
	core.MaxMemoryLimitForOverlongType = 0
	tk.MustQuery(` select * from t1 where c1 = 1 and c2 = "abc";`).Check(testkit.Rows("1 abc"))
	tk.MustQuery(`select @@last_sql_use_alloc`).Check(testkit.Rows("1"))
	core.MaxMemoryLimitForOverlongType = 500 * size.GB
	tk.MustQuery(` select * from t1 where c1 = 1 and c2 = "abc";`).Check(testkit.Rows("1 abc"))
	tk.MustQuery(`select @@last_sql_use_alloc`).Check(testkit.Rows("0"))
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

func TestIssue53766(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0, t1;")
	tk.MustExec("CREATE TABLE t0(c0 int);")
	tk.MustExec("CREATE TABLE t1(c0 int);")
	tk.MustQuery("SELECT t0.c0, t1.c0 FROM t0 NATURAL JOIN t1 WHERE '1' AND (t0.c0 IN (SELECT c0 FROM t0));").Check(testkit.Rows())
}

func TestIssue59762(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("create table t1(a int primary key, b int, index idx(b));")
	tk.MustExec("insert into t values(1), (2), (123);")
	tk.MustExec("insert into t1 values(2, 123), (123, 2);")
	tk.MustExec("set tidb_opt_fix_control='44855:on';")
	tk.MustExec("explain select /*+ inl_join(t1), use_index(t1, idx) */ * from t join t1 on t.a = t1.a and t1.b = 123;")
	tk.MustQuery("select /*+ inl_join(t1), use_index(t1, idx) */ * from t join t1 on t.a = t1.a and t1.b = 123;").Check(testkit.Rows("2 2 123"))
}
