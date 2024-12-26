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

func TestIssue49109(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0, t1;")
	tk.MustExec("CREATE TABLE t0(c0 NUMERIC);")
	tk.MustExec("CREATE TABLE t1(c0 NUMERIC);")
	tk.MustExec("INSERT INTO t0 VALUES (0), (NULL), (1), (2);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (NULL), (3), (4), (5);")
	tk.MustExec("drop view if exists v0;")
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v0(c0) AS SELECT t0.c0 FROM t0;")

	tk.MustQuery("SELECT t0.c0 FROM v0, t0 LEFT JOIN t1 ON t0.c0 WHERE ((INET_ATON('5V')) IS NULL);").Check(testkit.Rows("0", "0", "0", "0", "<nil>", "<nil>", "<nil>", "<nil>", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2"))
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

// https://github.com/pingcap/tidb/issues/53236
func TestIssue53236(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("create table t1(id int primary key, a varchar(128));")
	tk.MustExec("create table t2(id int primary key, b varchar(128), c varchar(128));")
	tk.MustExec(`UPDATE
    t1
SET
    t1.a = IFNULL(
            (
                SELECT
                    t2.c
                FROM
                    t2
                WHERE
                    t2.b = t1.a
                ORDER BY
                    t2.b DESC,
                    t2.c DESC
                LIMIT
                    1
            ), ''
        )
WHERE
    t1.id = 1;`)
}

func TestIssue52687(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t_o9_7_f (
  c_ob5k0 int(11) NOT NULL,
  c_r5axbk tinyint(4) DEFAULT NULL,
  c_fulsthp7e text DEFAULT NULL,
  c_nylhnz double DEFAULT NULL,
  c_fd7zeyfs49 int(11) NOT NULL,
  c_wpmmiv tinyint(4) DEFAULT NULL,
  PRIMARY KEY (c_fd7zeyfs49) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY c_ob5k0 (c_ob5k0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`CREATE TABLE t_q1 (
  c__c_r38murv int(11) NOT NULL,
  c_i93u7f2yma double NOT NULL,
  c_v5mf4 double DEFAULT NULL,
  c_gprkp int(11) DEFAULT NULL,
  c_ru text NOT NULL,
  c_nml tinyint(4) DEFAULT NULL,
  c_z text DEFAULT NULL,
  c_ok double DEFAULT NULL,
  PRIMARY KEY (c__c_r38murv) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY c__c_r38murv_2 (c__c_r38murv),
  UNIQUE KEY c_nml (c_nml)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`CREATE TABLE t_yzyyqbo2u (
  c_c4l int(11) DEFAULT NULL,
  c_yb_ text DEFAULT NULL,
  c_pq4c1la6cv int(11) NOT NULL,
  c_kbcid int(11) DEFAULT NULL,
  c_um double DEFAULT NULL,
  c_zjmgh995_6 text DEFAULT NULL,
  c_fujjmh8m2 double NOT NULL,
  c_qkf4n double DEFAULT NULL,
  c__x9cqrnb0 double NOT NULL,
  c_b5qjz_jj0 double DEFAULT NULL,
  PRIMARY KEY (c_pq4c1la6cv) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY c__x9cqrnb0 (c__x9cqrnb0),
  UNIQUE KEY c_b5qjz_jj0 (c_b5qjz_jj0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=2 */;`)
	tk.MustExec(`CREATE TABLE t_kg74 (
  c_a1tv2 int(11) NOT NULL,
  c_eobbbypzbu tinyint(4) DEFAULT NULL,
  c_g double NOT NULL,
  c_ixy tinyint(4) DEFAULT NULL,
  c_if text NOT NULL,
  c_obnq8s7_s2 double DEFAULT NULL,
  c_xrgd2snrop tinyint(4) DEFAULT NULL,
  c_vqafa6o6 text DEFAULT NULL,
  c_ku44klry7o double NOT NULL,
  c_js835qkmjz tinyint(4) DEFAULT NULL,
  PRIMARY KEY (c_a1tv2));`)
	tk.MustExec(`update t_kg74 set
  c_eobbbypzbu = (t_kg74.c_js835qkmjz in (
    select
          (ref_0.c_yb_ <> 'mlp40j') as c0
        from
          t_yzyyqbo2u as ref_0
        where (89.25 && ref_0.c_pq4c1la6cv)
      union
      (select
          ((cast(null as double) != 1382756095))
            and ((1=1 <> (EXISTS (
                  select distinct
                      ref_2.c_zjmgh995_6 as c0,
                      ref_2.c_zjmgh995_6 as c1,
                      ref_2.c_kbcid as c2,
                      ref_1.c_r5axbk as c3,
                      -633150135 as c4,
                      ref_2.c_c4l as c5,
                      ref_1.c_fd7zeyfs49 as c6,
                      ref_1.c_nylhnz as c7,
                      ref_2.c_um as c8,
                      ref_2.c_c4l as c9
                    from
                      t_yzyyqbo2u as ref_2
                    where ((ref_1.c_ob5k0 <= ref_2.c_qkf4n))
                      and ((EXISTS (
                        select
                            ref_3.c_qkf4n as c0,
                            ref_3.c_kbcid as c1,
                            ref_3.c_qkf4n as c2,
                            ref_1.c_wpmmiv as c3,
                            ref_1.c_fd7zeyfs49 as c4,
                            ref_3.c_c4l as c5,
                            ref_1.c_r5axbk as c6,
                            ref_3.c_kbcid as c7
                          from
                            t_yzyyqbo2u as ref_3
                          where ((ref_2.c_qkf4n >= (
                              select distinct
                                    ref_4.c_b5qjz_jj0 as c0
                                  from
                                    t_yzyyqbo2u as ref_4
                                  where (ref_3.c__x9cqrnb0 not in (
                                    select
                                          ref_5.c_ok as c0
                                        from
                                          t_q1 as ref_5
                                        where 1=1
                                      union
                                      (select
                                          ref_6.c_b5qjz_jj0 as c0
                                        from
                                          t_yzyyqbo2u as ref_6
                                        where (ref_6.c_qkf4n not in (
                                          select
                                                ref_7.c_um as c0
                                              from
                                                t_yzyyqbo2u as ref_7
                                              where 1=1
                                            union
                                            (select
                                                ref_8.c_b5qjz_jj0 as c0
                                              from
                                                t_yzyyqbo2u as ref_8
                                              where (ref_8.c_yb_ not like 'nrry%m')))))))
                                union
                                (select
                                    ref_2.c_fujjmh8m2 as c0
                                  from
                                    t_q1 as ref_9
                                  where (ref_2.c_zjmgh995_6 like 'v8%3xn%_uc'))
                                order by c0 limit 1)))
                            or ((ref_1.c_fulsthp7e in (
                              select
                                    ref_10.c_ru as c0
                                  from
                                    t_q1 as ref_10
                                  where (55.34 >= 1580576276)
                                union
                                (select
                                    ref_11.c_ru as c0
                                  from
                                    t_q1 as ref_11
                                  where (ref_11.c_ru in (
                                    select distinct
                                          ref_12.c_zjmgh995_6 as c0
                                        from
                                          t_yzyyqbo2u as ref_12
                                        where 0<>0
                                      union
                                      (select
                                          ref_13.c_zjmgh995_6 as c0
                                        from
                                          t_yzyyqbo2u as ref_13
                                        where ('q2chm8gfsa' = ref_13.c_yb_))))))))))))))) as c0
        from
          t_o9_7_f as ref_1
        where (-9186514464458010455 <> 62.67)))),
  c_if = 'u1ah7',
  c_vqafa6o6 = (t_kg74.c_a1tv2 + (((t_kg74.c_a1tv2 between t_kg74.c_a1tv2 and t_kg74.c_a1tv2))
        or (1=1))
      and ((1288561802 <= t_kg74.c_a1tv2))),
  c_js835qkmjz = (t_kg74.c_vqafa6o6 in (
    select
        ref_14.c_z as c0
      from
        t_q1 as ref_14
      where (ref_14.c_z like 'o%fiah')))
where (t_kg74.c_obnq8s7_s2 = case when (t_kg74.c_a1tv2 is NULL) then t_kg74.c_g else t_kg74.c_obnq8s7_s2 end
      );`)
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
