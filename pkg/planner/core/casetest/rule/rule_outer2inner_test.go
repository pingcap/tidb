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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

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
	suiteData.LoadTestCasesByName("TestOuter2Inner", t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

// can not add this test case to TestOuter2Inner because the collation_connection is different
func TestOuter2InnerIssue55886(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

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
	suiteData.LoadTestCasesByName("TestOuter2InnerIssue55886", t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssue58836(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	tk.MustQuery(`explain format = 'brief' with tmp as (
select
row_number() over() as id,
(select '1' from dual where id in (2)) as name
from t
)
select 'ok' from dual
where ('1',1) in (select name, id from tmp);`).Check(testkit.Rows(
		`Projection 8000.00 root  ok->Column#13`,
		`└─HashJoin 8000.00 root  CARTESIAN inner join`,
		`  ├─TableDual(Build) 1.00 root  rows:1`,
		`  └─HashAgg(Probe) 8000.00 root  group by:Column#11, Column#12, funcs:firstrow(1)->Column#18`,
		`    └─Selection 8000.00 root  eq("1", Column#11), eq(1, Column#12)`,
		`      └─Window 10000.00 root  row_number()->Column#12 over(rows between current row and current row)`,
		`        └─Apply 10000.00 root  CARTESIAN left outer join`,
		`          ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`          │ └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`,
		`          └─Projection(Probe) 8000.00 root  1->Column#11`,
		`            └─Selection 8000.00 root  eq(test.t.id, 2)`,
		`              └─TableDual 10000.00 root  rows:1`))
	// https://github.com/pingcap/tidb/issues/61327
	tk.MustExec(`CREATE TABLE t0(c0 INT);`)
	tk.MustExec(`CREATE TABLE t2(c0 INT);`)
	tk.MustExec(`CREATE TABLE t3(c0 INT);`)
	tk.MustExec(`INSERT INTO t0 VALUES(0);`)
	tk.MustExec(`INSERT INTO t3 VALUES(3);`)
	tk.MustQuery(`explain format = 'brief' SELECT *
FROM t0
         LEFT JOIN (SELECT NULL AS col_2
                    FROM t2) as subQuery1
                   ON true
         INNER JOIN t3 ON (((((CASE 1
                                   WHEN subQuery1.col_2 THEN t3.c0
                                   ELSE NULL END)) AND (((t0.c0))))) < 1);`).
		Check(testkit.Rows(
			`Projection 1000000000000.00 root  test.t0.c0, Column#5, test.t3.c0`,
			`└─HashJoin 1000000000000.00 root  CARTESIAN inner join, other cond:lt(and(case(eq(1, cast(Column#5, double BINARY)), test.t3.c0, NULL), test.t0.c0), 1)`,
			`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
			`  │ └─TableFullScan 10000.00 cop[tikv] table:t3 keep order:false, stats:pseudo`,
			`  └─HashJoin(Probe) 100000000.00 root  CARTESIAN left outer join`,
			`    ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
			`    │ └─TableFullScan 10000.00 cop[tikv] table:t0 keep order:false, stats:pseudo`,
			`    └─Projection(Probe) 10000.00 root  <nil>->Column#5`,
			`      └─TableReader 10000.00 root  data:TableFullScan`,
			`        └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`))
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
	tk.MustQuery(`explain format='brief' select 1 from chqin where  '2008-05-28' NOT IN
		(select a1.f1 from chqin a1 NATURAL RIGHT JOIN chqin2 a2 WHERE a2.f1  >='1990-11-27' union select f1 from chqin where id=5);`).
		Check(testkit.Rows(
			`Projection 8000.00 root  1->Column#14`,
			`└─HashJoin 8000.00 root  Null-aware anti semi join, equal:[eq(Column#16, Column#13)]`,
			`  ├─HashAgg(Build) 2672.00 root  group by:Column#13, funcs:firstrow(Column#13)->Column#13`,
			`  │ └─Union 4172.50 root  `,
			`  │   ├─HashJoin 4162.50 root  right outer join, equal:[eq(test.chqin.id, test.chqin2.id) eq(test.chqin.f1, test.chqin2.f1)]`,
			`  │   │ ├─TableReader(Build) 3330.00 root  data:Selection`,
			`  │   │ │ └─Selection 3330.00 cop[tikv]  ge(test.chqin.f1, 1990-11-27 00:00:00.000000), not(isnull(test.chqin.f1)), not(isnull(test.chqin.id))`,
			`  │   │ │   └─TableFullScan 10000.00 cop[tikv] table:a1 keep order:false, stats:pseudo`,
			`  │   │ └─TableReader(Probe) 3333.33 root  data:Selection`,
			`  │   │   └─Selection 3333.33 cop[tikv]  ge(test.chqin2.f1, 1990-11-27 00:00:00.000000)`,
			`  │   │     └─TableFullScan 10000.00 cop[tikv] table:a2 keep order:false, stats:pseudo`,
			`  │   └─TableReader 10.00 root  data:Projection`,
			`  │     └─Projection 10.00 cop[tikv]  test.chqin.f1->Column#13`,
			`  │       └─Selection 10.00 cop[tikv]  eq(test.chqin.id, 5)`,
			`  │         └─TableFullScan 10000.00 cop[tikv] table:chqin keep order:false, stats:pseudo`,
			`  └─Projection(Probe) 10000.00 root  2008-05-28 00:00:00.000000->Column#16`,
			`    └─TableReader 10000.00 root  data:TableFullScan`,
			`      └─TableFullScan 10000.00 cop[tikv] table:chqin keep order:false, stats:pseudo`))
	tk.MustQuery(`select 1 from chqin where  '2008-05-28' NOT IN
		(select a1.f1 from chqin a1 NATURAL RIGHT JOIN chqin2 a2 WHERE a2.f1  >='1990-11-27' union select f1 from chqin where id=5);`).
		Check(testkit.Rows())
}
