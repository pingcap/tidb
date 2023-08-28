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

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
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

// It's a case for Columns in tableScan and indexScan with double reader
func TestIssue43461(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index b(b), index b_c(b, c)) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t")

	stmt, err := parser.New().ParseOneStmt("select * from t use index(b) where b > 1 order by b limit 1", "", "")
	require.NoError(t, err)

	p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	var idxLookUpPlan *core.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*core.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(core.PhysicalPlan).Children()[0]
	}
	require.True(t, ok)

	is := idxLookUpPlan.IndexPlans[0].(*core.PhysicalIndexScan)
	ts := idxLookUpPlan.TablePlans[0].(*core.PhysicalTableScan)

	require.NotEqual(t, is.Columns, ts.Columns)
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

func TestIssue29221(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_index_merge=on;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b));")
	tk.MustExec("set @@session.sql_select_limit=3;")
	var input []string
	var output []struct {
		SQL           string
		ExplainResult []string
	}
	issueTestData := GetIssueTestData()
	issueTestData.LoadTestCases(t, &input, &output)

	for i, in := range input {
		if len(in) < 7 || in[:7] != "explain" {
			tk.MustExec(in)
			testdata.OnRecord(func() {
				output[i].SQL = in
				output[i].ExplainResult = nil
			})
			continue
		}
		result := tk.MustQuery(in)
		testdata.OnRecord(func() {
			output[i].SQL = in
			output[i].ExplainResult = testdata.ConvertRowsToStrings(result.Rows())
		})
		result.Check(testkit.Rows(output[i].ExplainResult...))
	}
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
