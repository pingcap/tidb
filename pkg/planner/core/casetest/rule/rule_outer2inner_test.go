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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestOuter2Inner(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t1(a1 int, b1 int, c1 int)")
		tk.MustExec("create table t2(a2 int, b2 int, c2 int)")
		tk.MustExec("create table t3(a3 int, b3 int, c3 int)")
		tk.MustExec("create table t4(a4 int, b4 int, c4 int)")
		tk.MustExec("create table ti(i int)")
		tk.MustExec("CREATE TABLE lineitem (L_PARTKEY INTEGER ,L_QUANTITY DECIMAL(15,2),L_EXTENDEDPRICE  DECIMAL(15,2))")
		tk.MustExec("CREATE TABLE part(P_PARTKEY INTEGER,P_BRAND CHAR(10),P_CONTAINER CHAR(10))")
		tk.MustExec("CREATE TABLE d (pk int, col_blob blob, col_blob_key blob, col_varchar_key varchar(1) , col_date date, col_int_key int)")
		tk.MustExec("CREATE TABLE dd (pk int, col_blob blob, col_blob_key blob, col_date date, col_int_key int)")
		tk.MustExec("create table t0 (a0 int, b0 char, c0 char(2))")
		tk.MustExec("create table t11 (a1 int, b1 char, c1 char)")

		var input Input
		var output []struct {
			SQL  string
			Plan []string
		}
		suiteData := GetOuter2InnerSuiteData()
		suiteData.LoadTestCasesByName("TestOuter2Inner", t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := tk.MustQuery("explain format = 'brief' " + sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}

		tk.MustExec("drop table if exists t1, t2")
		tk.MustExec("create table t1 (k int, a int)")
		tk.MustExec("create table t2 (k int, b int, key(k))")
		tk.MustHavePlan(`select /* issue:49616 */ /*+ tidb_inlj(t2, t1) */ *
  from t2 left join t1 on t1.k=t2.k
  where a>0 or (a=0 and b>0)`, "IndexJoin")

		tk.MustExec("drop table if exists t_outer, t")
		tk.MustExec(`CREATE TABLE t_outer (
			id bigint(20) NOT NULL,
			scode varchar(64) NOT NULL,
			username varchar(60) NOT NULL,
			real_name varchar(100) NOT NULL DEFAULT '',
			KEY idx1 ((lower(real_name))),
			UNIQUE KEY idx2 (username,scode))`)
		tk.MustExec(`CREATE TABLE t (
			id int(11) unsigned NOT NULL,
			scode varchar(64) NOT NULL DEFAULT '',
			plat_id varchar(64) NOT NULL)`)
		tk.MustQuery(`EXPLAIN FORMAT='plan_tree' SELECT /* issue:65166 */ a.id FROM
			t AS a LEFT JOIN t_outer b ON b.username = a.plat_id
			AND b.scode = a.scode ORDER BY a.id`).CheckNotContain("Join")

		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (id int primary key, name varchar(100));")
		tk.MustExec("insert into t values (1, null), (2, 1);")
		tk.MustQuery(`with tmp as (
select
row_number() over() as id,
(select '1' from dual where id in (2)) as name
from t
)
select 'ok' from dual
where ('1',1) in (select name, id from tmp);`).Check(testkit.Rows())
		tk.MustQuery(`explain format = 'plan_tree' with tmp as (
select
row_number() over() as id,
(select '1' from dual where id in (2)) as name
from t
)
select 'ok' from dual
where ('1',1) in (select name, id from tmp);`).Check(testkit.Rows(
			`Projection root  ok->Column`,
			`└─HashJoin root  CARTESIAN inner join`,
			`  ├─TableDual(Build) root  rows:1`,
			`  └─HashAgg(Probe) root  group by:Column, Column, funcs:firstrow(1)->Column`,
			`    └─Selection root  eq(Column, "1"), eq(Column, 1)`,
			`      └─Window root  row_number()->Column#14 over(rows between current row and current row)`,
			`        └─Apply root  CARTESIAN left outer join, left side:TableReader`,
			`          ├─TableReader(Build) root  data:TableFullScan`,
			`          │ └─TableFullScan cop[tikv] table:t keep order:false, stats:pseudo`,
			`          └─Projection(Probe) root  1->Column`,
			`            └─Selection root  eq(test.t.id, 2)`,
			`              └─TableDual root  rows:1`))
		// issue:58836
		tk.MustExec("drop table if exists t0, t2, t3")
		tk.MustExec(`CREATE TABLE t0(c0 INT);`)
		tk.MustExec(`CREATE TABLE t2(c0 INT);`)
		tk.MustExec(`CREATE TABLE t3(c0 INT);`)
		tk.MustExec(`INSERT INTO t0 VALUES(0);`)
		tk.MustExec(`INSERT INTO t3 VALUES(3);`)
		tk.MustQuery(`explain format = 'plan_tree' SELECT *
FROM t0
         LEFT JOIN (SELECT NULL AS col_2
                    FROM t2) as subQuery1
                   ON true
         INNER JOIN t3 ON (((((CASE 1
                                   WHEN subQuery1.col_2 THEN t3.c0
                                   ELSE NULL END)) AND (((t0.c0))))) < 1);`).
			Check(testkit.Rows(`HashJoin root  CARTESIAN inner join, other cond:lt(and(case(eq(1, cast(Column, double BINARY)), test.t3.c0, NULL), test.t0.c0), 1)`,
				`├─TableReader(Build) root  data:TableFullScan`,
				`│ └─TableFullScan cop[tikv] table:t3 keep order:false, stats:pseudo`,
				`└─HashJoin(Probe) root  CARTESIAN left outer join, left side:TableReader`,
				`  ├─TableReader(Build) root  data:TableFullScan`,
				`  │ └─TableFullScan cop[tikv] table:t0 keep order:false, stats:pseudo`,
				`  └─Projection(Probe) root  <nil>->Column`,
				`    └─TableReader root  data:TableFullScan`,
				`      └─TableFullScan cop[tikv] table:t2 keep order:false, stats:pseudo`))
		tk.MustQuery(`SELECT *
FROM t0
         LEFT JOIN (SELECT NULL AS col_2
                    FROM t2) as subQuery1
                   ON true
         INNER JOIN t3 ON (((((CASE 1
                                   WHEN subQuery1.col_2 THEN t3.c0
                                   ELSE NULL END)) AND (((t0.c0))))) < 1);`).Check(testkit.Rows("0 <nil> 3"))
		tk.MustExec("create table chqin(id int, f1 date);")
		tk.MustExec("insert into chqin values (1,null);")
		tk.MustExec("insert into chqin values (2,null);")
		tk.MustExec("insert into chqin values (3,null);")
		tk.MustExec("create table chqin2(id int, f1 date);")
		tk.MustExec("insert into chqin2 values (1,'1990-11-27');")
		tk.MustExec("insert into chqin2 values (2,'1990-11-27');")
		tk.MustExec("insert into chqin2 values (3,'1990-11-27');")
		tk.MustQuery(`explain format='plan_tree' select 1 from chqin where  '2008-05-28' NOT IN
		(select a1.f1 from chqin a1 NATURAL RIGHT JOIN chqin2 a2 WHERE a2.f1  >='1990-11-27' union select f1 from chqin where id=5);`).
			Check(testkit.Rows(
				`Projection root  1->Column`,
				`└─HashJoin root  Null-aware anti semi join, left side:Projection, equal:[eq(Column, Column)]`,
				`  ├─HashAgg(Build) root  group by:Column, funcs:firstrow(Column)->Column`,
				`  │ └─Union root  `,
				`  │   ├─HashJoin root  right outer join, left side:TableReader, equal:[eq(test.chqin.id, test.chqin2.id) eq(test.chqin.f1, test.chqin2.f1)]`,
				`  │   │ ├─TableReader(Build) root  data:Selection`,
				`  │   │ │ └─Selection cop[tikv]  ge(test.chqin.f1, 1990-11-27 00:00:00.000000), not(isnull(test.chqin.f1)), not(isnull(test.chqin.id))`,
				`  │   │ │   └─TableFullScan cop[tikv] table:a1 keep order:false, stats:pseudo`,
				`  │   │ └─TableReader(Probe) root  data:Selection`,
				`  │   │   └─Selection cop[tikv]  ge(test.chqin2.f1, 1990-11-27 00:00:00.000000)`,
				`  │   │     └─TableFullScan cop[tikv] table:a2 keep order:false, stats:pseudo`,
				`  │   └─TableReader root  data:Projection`,
				`  │     └─Projection cop[tikv]  test.chqin.f1->Column`,
				`  │       └─Selection cop[tikv]  eq(test.chqin.id, 5)`,
				`  │         └─TableFullScan cop[tikv] table:chqin keep order:false, stats:pseudo`,
				`  └─Projection(Probe) root  2008-05-28 00:00:00.000000->Column`,
				`    └─TableReader root  data:TableFullScan`,
				`      └─TableFullScan cop[tikv] table:chqin keep order:false, stats:pseudo`))
		tk.MustQuery(`select 1 from chqin where  '2008-05-28' NOT IN
		(select a1.f1 from chqin a1 NATURAL RIGHT JOIN chqin2 a2 WHERE a2.f1  >='1990-11-27' union select f1 from chqin where id=5);`).
			Check(testkit.Rows())
	})
}

// can not add this test case to TestOuter2Inner because the collation_connection is different
func TestOuter2InnerIssue55886(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop table if exists t2")
		tk.MustExec("create table t1(c_foveoe text, c_jbb text, c_cz text not null)")
		tk.MustExec("create table t2(c_g7eofzlxn int)")
		tk.MustExec("set collation_connection = 'latin1_bin'")

		var input Input
		var output []struct {
			SQL  string
			Plan []string
		}
		suiteData := GetOuter2InnerSuiteData()
		suiteData.LoadTestCasesByName("TestOuter2InnerIssue55886", t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := tk.MustQuery("explain format = 'brief' " + sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}
