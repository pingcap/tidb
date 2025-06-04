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

func TestNullRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// https://github.com/pingcap/tidb/issues/60080
	// constant is not null rejected, so the outer to inner join should not be applied.
	tk.MustExec("use test;")
	tk.MustExec(`CREATE TABLE t0 (c0 int) ;`)
	tk.MustExec(`INSERT INTO t0 VALUES (61);`)
	tk.MustExec(`CREATE TABLE t1 (c1 int,c2 double );`)
	tk.MustExec("INSERT INTO `t1` VALUES (NULL,0);")
	tk.MustExec(`CREATE TABLE t2 (c3 varchar(100),c4 varchar(100)) ;`)
	tk.MustExec(`INSERT INTO t2 VALUES (NULL,NULL);`)
	sql := `select  
  subq_0.c_0 as c_0, 
  subq_0.c_1 as c_1, 
  subq_0.c_2 as c_2
from 
  (select  
        ref_1.c0 as c_0, 
        ref_0.c2 as c_1,  
        ref_0.c2 as c_2
      from 
        (t1 as ref_0
          right join (t0 as ref_1
            )
          on (ref_0.c1 = ref_1.c0 ))
       ) as subq_0
where ((('' = ( 
      select  
          (' n1')  as c_0
        from 
          (t2 as ref_3
            left join t2 as ref_4
            on (ref_3.c4 = ref_4.c3 ))
        where ((subq_0.c_1) <> (subq_0.c_2)) 
          or (true)
        order by c_0 desc
         limit 1)) is null)) `
	tk.MustQuery(`explain format='brief' ` + sql).Check(testkit.Rows(
		"Projection 9990.00 root  test.t0.c0, test.t1.c2, test.t1.c2",
		`└─Selection 9990.00 root  isnull(eq("", Column#12))`,
		"  └─Apply 12487.50 root  CARTESIAN left outer join, left side:HashJoin",
		"    ├─HashJoin(Build) 12487.50 root  right outer join, left side:TableReader, equal:[eq(test.t1.c1, test.t0.c0)]",
		"    │ ├─TableReader(Build) 10000.00 root  data:TableFullScan",
		"    │ │ └─TableFullScan 10000.00 cop[tikv] table:ref_1 keep order:false, stats:pseudo",
		"    │ └─TableReader(Probe) 9990.00 root  data:Selection",
		"    │   └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.c1))",
		"    │     └─TableFullScan 10000.00 cop[tikv] table:ref_0 keep order:false, stats:pseudo",
		"    └─Projection(Probe) 12487.50 root   n1->Column#12",
		"      └─Limit 12487.50 root  offset:0, count:1",
		"        └─HashJoin 12487.50 root  left outer join, left side:Limit, equal:[eq(test.t2.c4, test.t2.c3)]",
		"          ├─Limit(Build) 12487.50 root  offset:0, count:1",
		"          │ └─TableReader 12487.50 root  data:Limit",
		"          │   └─Limit 12487.50 cop[tikv]  offset:0, count:1",
		"          │     └─TableFullScan 12487.50 cop[tikv] table:ref_3 keep order:false, stats:pseudo",
		"          └─TableReader(Probe) 124750125.00 root  data:Selection",
		"            └─Selection 124750125.00 cop[tikv]  not(isnull(test.t2.c3))",
		"              └─TableFullScan 124875000.00 cop[tikv] table:ref_4 keep order:false, stats:pseudo"))
	tk.MustQuery(sql).Check(testkit.Rows())
}
