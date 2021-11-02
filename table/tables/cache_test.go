// Copyright 2021 PingCAP, Inc.
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

package tables_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestCacheTableBasicScan(t *testing.T) {
	t.Parallel()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tmp1")
	tk.MustExec("create  table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values" +
		"(1, 101, 1001), (3, 113, 1003), (5, 105, 1005), (7, 117, 1007), (9, 109, 1009)," +
		"(10, 110, 1010), (12, 112, 1012), (14, 114, 1014), (16, 116, 1016), (18, 118, 1018)",
	)
	tk.MustExec("alter table tmp1 cache")
	assertSelect := func() {
		// For TableReader
		// First read will read from original table
		tk.MustQuery("select * from tmp1 where id>3 order by id").Check(testkit.Rows(
			"5 105 1005", "7 117 1007", "9 109 1009",
			"10 110 1010", "12 112 1012", "14 114 1014", "16 116 1016", "18 118 1018",
		))
		// Test for join two cache table
		tk.MustExec("drop table if exists join_t1, join_t2, join_t3")
		tk.MustExec("create table join_t1  (id int)")
		tk.MustExec("insert into join_t1 values(1)")
		tk.MustExec("alter table join_t1 cache")
		tk.MustExec("select *from join_t1")
		tk.MustExec("create table join_t2  (id int)")
		tk.MustExec("insert into join_t2 values(2)")
		tk.MustExec("alter table join_t2 cache")
		tk.MustExec("select *from join_t2")
		tk.MustExec("create table join_t3 (id int)")
		tk.MustExec("insert into join_t3 values(3)")
		result := tk.MustQuery("explain format = 'brief' select *from join_t1 join join_t2")
		result.Check(testkit.Rows(
			"HashJoin 100000000.00 root  CARTESIAN inner join",
			"├─UnionScan(Build) 10000.00 root  ",
			"│ └─TableReader 10000.00 root  data:TableFullScan",
			"│   └─TableFullScan 10000.00 cop[tikv] table:join_t2 keep order:false, stats:pseudo",
			"└─UnionScan(Probe) 10000.00 root  ",
			"  └─TableReader 10000.00 root  data:TableFullScan",
			"    └─TableFullScan 10000.00 cop[tikv] table:join_t1 keep order:false, stats:pseudo"))
		tk.MustQuery("select *from join_t1 join join_t2").Check(testkit.Rows("1 2"))
		// Test for join a cache table and a normal table
		result = tk.MustQuery("explain format = 'brief' select *from join_t1 join join_t3")
		result.Check(testkit.Rows(
			"Projection 100000000.00 root  test.join_t1.id, test.join_t3.id",
			"└─HashJoin 100000000.00 root  CARTESIAN inner join",
			"  ├─UnionScan(Build) 10000.00 root  ",
			"  │ └─TableReader 10000.00 root  data:TableFullScan",
			"  │   └─TableFullScan 10000.00 cop[tikv] table:join_t1 keep order:false, stats:pseudo",
			"  └─TableReader(Probe) 10000.00 root  data:TableFullScan",
			"    └─TableFullScan 10000.00 cop[tikv] table:join_t3 keep order:false, stats:pseudo"))

		tk.MustQuery("select *from join_t1 join join_t3").Check(testkit.Rows("1 3"))
		// Second read will from cache table
		result = tk.MustQuery("explain format = 'brief' select * from tmp1 where id>4 order by id")
		result.Check(testkit.Rows("UnionScan 3333.33 root  gt(test.tmp1.id, 4)",
			"└─TableReader 3333.33 root  data:TableRangeScan",
			"  └─TableRangeScan 3333.33 cop[tikv] table:tmp1 range:(4,+inf], keep order:true, stats:pseudo"))
		tk.MustQuery("select * from tmp1 where id>4 order by id").Check(testkit.Rows(
			"5 105 1005", "7 117 1007", "9 109 1009",
			"10 110 1010", "12 112 1012", "14 114 1014", "16 116 1016", "18 118 1018",
		))
		// For IndexLookUpReader
		result = tk.MustQuery("explain format = 'brief' select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u")
		result.Check(testkit.Rows("UnionScan 3333.33 root  gt(test.tmp1.u, 101)",
			"└─IndexLookUp 3333.33 root  ",
			"  ├─IndexRangeScan(Build) 3333.33 cop[tikv] table:tmp1, index:u(u) range:(101,+inf], keep order:true, stats:pseudo",
			"  └─TableRowIDScan(Probe) 3333.33 cop[tikv] table:tmp1 keep order:false, stats:pseudo"))
		tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u").Check(testkit.Rows(
			"5 105 1005", "9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ id,u from tmp1 where u>101 order by id").Check(testkit.Rows(
			"3 113", "5 105", "7 117", "9 109", "10 110",
			"12 112", "14 114", "16 116", "18 118",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexMerge, temporary table should not use index merge
		tk.MustQuery("select /*+ use_index_merge(tmp1, primary, u) */ * from tmp1 where id>5 or u>110 order by u").Check(testkit.Rows(
			"9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))

		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 IndexMerge is inapplicable or disabled"))
	}
	assertSelect()

}
