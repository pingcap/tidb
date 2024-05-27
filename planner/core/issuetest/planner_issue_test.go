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
	"sort"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/testkit"
)

// It's a case for index merge's order prop push down.
func TestIssue43178(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE aa311c3c (
		57fd8d09 year(4) DEFAULT '1913',
		afbdd7c3 char(220) DEFAULT 'gakkl6occ0yd2jmhi2qxog8szibtcqwxyxmga3hp4ktszjplmg3rjvu8v6lgn9q6hva2lekhw6napjejbut6svsr8q2j8w8rc551e5vq',
		43b06e99 date NOT NULL DEFAULT '3403-10-08',
		b80b3746 tinyint(4) NOT NULL DEFAULT '34',
		6302d8ac timestamp DEFAULT '2004-04-01 18:21:18',
		PRIMARY KEY (43b06e99,b80b3746) /*T![clustered_index] CLUSTERED */,
		KEY 3080c821 (57fd8d09,43b06e99,b80b3746),
		KEY a9af33a4 (57fd8d09,b80b3746,43b06e99),
		KEY 464b386e (b80b3746),
		KEY 19dc3c2d (57fd8d09)
	      ) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin COMMENT='320f8401'`)
	// Should not panic
	tk.MustExec("explain select  /*+ use_index_merge( `aa311c3c` ) */   `aa311c3c`.`43b06e99` as r0 , `aa311c3c`.`6302d8ac` as r1 from `aa311c3c` where IsNull( `aa311c3c`.`b80b3746` ) or not( `aa311c3c`.`57fd8d09` >= '2008' )   order by r0,r1 limit 95")
}

func TestIssue43645(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1(id int,col1 varchar(10),col2 varchar(10),col3 varchar(10));")
	tk.MustExec("CREATE TABLE t2(id int,col1 varchar(10),col2 varchar(10),col3 varchar(10));")
	tk.MustExec("INSERT INTO t1 values(1,NULL,NULL,null),(2,NULL,NULL,null),(3,NULL,NULL,null);")
	tk.MustExec("INSERT INTO t2 values(1,'a','aa','aaa'),(2,'b','bb','bbb'),(3,'c','cc','ccc');")

	rs := tk.MustQuery("WITH tmp AS (SELECT t2.* FROM t2) select (SELECT tmp.col1 FROM tmp WHERE tmp.id=t1.id ) col1, (SELECT tmp.col2 FROM tmp WHERE tmp.id=t1.id ) col2, (SELECT tmp.col3 FROM tmp WHERE tmp.id=t1.id ) col3 from t1;")
	rs.Sort().Check(testkit.Rows("a aa aaa", "b bb bbb", "c cc ccc"))
}

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

func TestIssue46083(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TEMPORARY TABLE v0(v1 int)")
	tk.MustExec("INSERT INTO v0 WITH ta2 AS (TABLE v0) TABLE ta2 FOR UPDATE OF ta2;")
}

func TestIssue48755(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_max_chunk_size=32")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values(1, 1);")
	tk.MustExec("insert into t select a+1, a+1 from t;")
	tk.MustExec("insert into t select a+2, a+2 from t;")
	tk.MustExec("insert into t select a+4, a+4 from t;")
	tk.MustExec("insert into t select a+8, a+8 from t;")
	tk.MustExec("insert into t select a+16, a+16 from t;")
	tk.MustExec("insert into t select a+32, a+32 from t;")
	rs := tk.MustQuery("select a from (select 100 as a, 100 as b union all select * from t) t where b != 0;")
	expectedResult := make([]string, 0, 65)
	for i := 1; i < 65; i++ {
		expectedResult = append(expectedResult, strconv.FormatInt(int64(i), 10))
	}
	expectedResult = append(expectedResult, "100")
	sort.Slice(expectedResult, func(i, j int) bool {
		return expectedResult[i] < expectedResult[j]
	})
	rs.Sort().Check(testkit.Rows(expectedResult...))
}

func TestIssue47881(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int,name varchar(10));")
	tk.MustExec("insert into t values(1,'tt');")
	tk.MustExec("create table t1(id int,name varchar(10),name1 varchar(10),name2 varchar(10));")
	tk.MustExec("insert into t1 values(1,'tt','ttt','tttt'),(2,'dd','ddd','dddd');")
	tk.MustExec("create table t2(id int,name varchar(10),name1 varchar(10),name2 varchar(10),`date1` date);")
	tk.MustExec("insert into t2 values(1,'tt','ttt','tttt','2099-12-31'),(2,'dd','ddd','dddd','2099-12-31');")
	rs := tk.MustQuery(`WITH bzzs AS (
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
		LEFT JOIN tmp3 c3 ON c3.id = '1';`)
	rs.Check(testkit.Rows("1 1", "1 1"))
}

func TestIssue48969(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop view if exists v1;")
	tk.MustExec("create view v1(id) as with recursive cte(a) as (select 1 union select a+1 from cte where a<3) select * from cte;")
	tk.MustExec("create table test2(id int,value int);")
	tk.MustExec("insert into test2 values(1,1),(2,2),(3,3),(4,4),(5,5);")
	tk.MustExec("update test2 set value=0 where test2.id in (select * from v1);")
	tk.MustQuery("select * from test2").Check(testkit.Rows("1 0", "2 0", "3 0", "4 4", "5 5"))
}

func TestIssue51670(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table A(a int primary key, b int);")
	tk.MustExec("create table B(b int primary key);")
	tk.MustExec("create table C(c int primary key, b int);")
	tk.MustExec("insert into A values (2, 1), (3, 2);")
	tk.MustExec("insert into B values (1), (2);")
	// The two should return the same result set.
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
			"  └─Projection 0.00 root  test.tt.a, test.tt.b, test.tt.c, test.tt.d, test.tt.e, Column#18, Column#19, Column#20, Column#21",
			"    └─IndexJoin 0.00 root  inner join, inner:TableReader, outer key:Column#20, Column#21, inner key:test.tt.c, test.tt.d, equal cond:eq(Column#20, test.tt.c), eq(Column#21, test.tt.d), other cond:or(or(and(eq(Column#20, 11), eq(test.tt.d, 111)), and(eq(Column#20, 22), eq(test.tt.d, 222))), or(and(eq(Column#20, 33), eq(test.tt.d, 333)), and(eq(Column#20, 44), eq(test.tt.d, 444)))), or(or(and(eq(test.tt.c, 11), eq(Column#21, 111)), and(eq(test.tt.c, 22), eq(Column#21, 222))), or(and(eq(test.tt.c, 33), eq(Column#21, 333)), and(eq(test.tt.c, 44), eq(Column#21, 444))))",
			"      ├─Union(Build) 0.00 root  ",
			"      │ ├─Projection 0.00 root  Column#6, Column#7, Column#8, Column#9",
			"      │ │ └─Projection 0.00 root  1->Column#6, 2->Column#7, 3->Column#8, 4->Column#9",
			"      │ │   └─TableDual 0.00 root  rows:0",
			"      │ ├─Projection 0.00 root  Column#10, Column#11, Column#12, Column#13",
			"      │ │ └─Projection 0.00 root  2->Column#10, 3->Column#11, 4->Column#12, 5->Column#13",
			"      │ │   └─TableDual 0.00 root  rows:0",
			"      │ └─Projection 0.00 root  Column#14, Column#15, Column#16, Column#17",
			"      │   └─Projection 0.00 root  3->Column#14, 4->Column#15, 5->Column#16, 6->Column#17",
			"      │     └─TableDual 0.00 root  rows:0",
			"      └─TableReader(Probe) 0.00 root  data:Selection",
			"        └─Selection 0.00 cop[tikv]  or(or(and(eq(test.tt.c, 11), eq(test.tt.d, 111)), and(eq(test.tt.c, 22), eq(test.tt.d, 222))), or(and(eq(test.tt.c, 33), eq(test.tt.d, 333)), and(eq(test.tt.c, 44), eq(test.tt.d, 444)))), or(or(eq(test.tt.c, 11), eq(test.tt.c, 22)), or(eq(test.tt.c, 33), eq(test.tt.c, 44))), or(or(eq(test.tt.d, 111), eq(test.tt.d, 222)), or(eq(test.tt.d, 333), eq(test.tt.d, 444)))",
			"          └─TableRangeScan 0.00 cop[tikv] table:tt range: decided by [eq(test.tt.c, Column#20) eq(test.tt.d, Column#21)], keep order:false, stats:pseudo",
		),
	)
}
