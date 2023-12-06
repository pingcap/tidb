// Copyright 2016 PingCAP, Inc.
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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
)

func TestNaturalJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (a int, c int)")
	tk.MustExec("insert t1 values (1,2), (10,20), (0,0)")
	tk.MustExec("insert t2 values (1,3), (100,200), (0,0)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	executorSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func TestUsingAndNaturalJoinSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.MustExec("create table t1 (c int, b int);")
	tk.MustExec("create table t2 (a int, b int);")
	tk.MustExec("create table t3 (b int, c int);")
	tk.MustExec("create table t4 (y int, c int);")

	tk.MustExec("insert into t1 values (10,1);")
	tk.MustExec("insert into t1 values (3 ,1);")
	tk.MustExec("insert into t1 values (3 ,2);")
	tk.MustExec("insert into t2 values (2, 1);")
	tk.MustExec("insert into t3 values (1, 3);")
	tk.MustExec("insert into t3 values (1,10);")
	tk.MustExec("insert into t4 values (11,3);")
	tk.MustExec("insert into t4 values (2, 3);")

	var input []string
	var output []struct {
		SQL string
		Res []string
	}
	executorSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func TestTiDBNAAJ(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_enable_null_aware_anti_join=0;")
	tk.MustExec("create table t(a decimal(40,0), b bigint(20) not null);")
	tk.MustExec("insert into t values(7,8),(7,8),(3,4),(3,4),(9,2),(9,2),(2,0),(2,0),(0,4),(0,4),(8,8),(8,8),(6,1),(6,1),(NULL, 0),(NULL,0);")
	tk.MustQuery("select ( table1 . a , table1 . b ) NOT IN ( SELECT 3 , 2 UNION  SELECT 9, 2 ) AS field2 from t as table1 order by field2;").Check(testkit.Rows(
		"0", "0", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"))
	tk.MustExec("set @@session.tidb_enable_null_aware_anti_join=1;")
	tk.MustQuery("select ( table1 . a , table1 . b ) NOT IN ( SELECT 3 , 2 UNION  SELECT 9, 2 ) AS field2 from t as table1 order by field2;").Check(testkit.Rows(
		"0", "0", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"))
}

func TestIssue48991(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl_3")
	tk.MustExec("create table tbl_3 ( col_11 mediumint  unsigned not null default 8346281 ,col_12 enum ( 'Alice','Bob','Charlie','David' ) ,col_13 time   not null default '07:10:30.00' ,col_14 timestamp ,col_15 varbinary ( 194 )   not null default '-ZpCzjqdl4hsyo' , key idx_5 ( col_14 ,col_11 ,col_12 ) ,primary key  ( col_11 ,col_15 ) /*T![clustered_index] clustered */ ) charset utf8mb4 collate utf8mb4_bin partition by range ( col_11 ) ( partition p0 values less than (530262), partition p1 values less than (9415740), partition p2 values less than (11007444), partition p3 values less than (maxvalue) );")
	tk.MustExec("insert into tbl_3 values ( 8838143,'David','23:41:27.00','1993-02-23','g0q~Z0b*PpMPKJxYbIE' );")
	tk.MustExec("insert into tbl_3 values ( 9082223,'Alice','02:25:16.00','2035-11-08','i' );")
	tk.MustExec("insert into tbl_3 values ( 2483729,'Charlie','14:43:13.00','1970-09-10','w6o6WFYyL5' );")
	tk.MustExec("insert into tbl_3 values ( 4135401,'Charlie','19:30:34.00','2017-06-07','2FZmy9lanL8' );")
	tk.MustExec("insert into tbl_3 values ( 1479390,'Alice','20:40:08.00','1984-06-10','LeoVONgN~iJz&inj' );")
	tk.MustExec("insert into tbl_3 values ( 10427825,'Charlie','15:27:35.00','1986-12-25','tWJ' );")
	tk.MustExec("insert into tbl_3 values ( 12794792,'Charlie','04:10:08.00','2034-08-08','hvpXVQyuP' );")
	tk.MustExec("insert into tbl_3 values ( 4696775,'Charlie','05:07:43.00','1984-07-31','SKOW9I^sM$4xNk' );")
	tk.MustExec("insert into tbl_3 values ( 8963236,'Alice','08:18:31.00','2022-04-17','v4DsE' );")
	tk.MustExec("insert into tbl_3 values ( 9048951,'Alice','05:19:47.00','2018-09-22','sJ!vs' );")

	res := tk.MustQuery("SELECT `col_14` FROM `test`.`tbl_3` WHERE ((`tbl_3`.`col_15` < 'dV') AND `tbl_3`.`col_12` IN (SELECT `col_12` FROM `test`.`tbl_3` WHERE NOT (ISNULL(`tbl_3`.`col_12`)))) ORDER BY IF(ISNULL(`col_14`),0,1),`col_14`;")
	res.Check(testkit.Rows("1984-06-10 00:00:00", "1984-07-31 00:00:00", "2017-06-07 00:00:00"))
