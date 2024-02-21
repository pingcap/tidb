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
