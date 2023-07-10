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

	"github.com/pingcap/tidb/testkit"
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

func TestIssue45112(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a varchar(20), b varchar(20), c varchar(20), primary key(a, b, c))")
	tk.MustExec("create table t2(a varchar(20), b varchar(20), c varchar(20), primary key(a))")
	tk.MustExec("create table t3(aa varchar (20), bb varchar (20), cc varchar(20), primary key(aa, bb))")
	tk.MustExec("insert into t1 values('1','1','1');")
	tk.MustExec("insert into t2 values('1','1','1');")
	tk.MustExec("insert into t3 values('1','1','1');")

	tk.MustQuery("select (select max(b) from t2 where t2.a = t1.a), (select cc from t3 where t3.bb = t1.b and t3.aa = '1'), a from t1 where t1.a = '1' group by t1.a,  t1.b").Check(testkit.Rows("1 1 1"))
}
