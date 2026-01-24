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

package constantpropagation

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestConstantPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Missing Cast Expr
	tk.MustExec("create table tl50cb7440 (" +
		"  col_43 decimal(30,30) not null," +
		"  primary key (col_43) /*t![clustered_index] clustered */," +
		"  unique key idx_12 (col_43)," +
		"  key idx_13 (col_43)," +
		"  unique key idx_14 (col_43)" +
		") engine=innodb default charset=utf8 collate=utf8_bin;")
	tk.MustExec("insert into tl50cb7440 values(0.000000000000000000000000000000),(0.400000000000000000000000000000);")
	tk.MustQuery("with cte_8911 (col_47665) as" +
		"  (select mid(tl50cb7440.col_43, 6, 9) as r0" +
		"   from tl50cb7440" +
		"   where tl50cb7440.col_43 in (0, 0) and tl50cb7440.col_43 in (0))" +
		"  (select 1" +
		"   from cte_8911 where cte_8911.col_47665!='');").Check(testkit.Rows("1"))
	// The constant should skip the pushdown checking.
	tk.MustExec(`CREATE TABLE t373b8b5b (
  col_53 tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)
	tk.MustExec(` CREATE TABLE tafab9ab4 (
  col_32 json DEFAULT NULL,
  col_35 tinyint unsigned NOT NULL,
  col_36 binary(117) NOT NULL DEFAULT 'k#Vf)%G$9T6)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci`)
	tk.MustQuery(`explain format='plan_tree' select /*+ NO_HASH_JOIN( t373b8b5b , tafab9ab4 */ tafab9ab4.col_32 as r0 , substring_index( tafab9ab4.col_36 , ',' , 2 ) as r1 , tafab9ab4.col_32 as r2 from t373b8b5b join tafab9ab4 on t373b8b5b.col_53 = tafab9ab4.col_35 where tafab9ab4.col_35 in ( 78 ,177 ) and t373b8b5b.col_53 between 0 and 1 order by r0,r1,r2`).Check(testkit.Rows(
<<<<<<< HEAD
		`Sort root  test.tafab9ab4.col_32, Column#9`,
		`└─Projection root  test.tafab9ab4.col_32, substring_index(test.tafab9ab4.col_36, ,, 2)->Column#9, test.tafab9ab4.col_32`,
=======
		`Sort root  test.tafab9ab4.col_32, Column`,
		`└─Projection root  test.tafab9ab4.col_32, substring_index(test.tafab9ab4.col_36, ,, 2)->Column, test.tafab9ab4.col_32`,
>>>>>>> master
		`  └─HashJoin root  inner join, equal:[eq(test.t373b8b5b.col_53, test.tafab9ab4.col_35)]`,
		`    ├─TableReader(Build) root  data:Selection`,
		`    │ └─Selection cop[tikv]  ge(test.t373b8b5b.col_53, 0), in(test.t373b8b5b.col_53, 78, 177), le(test.t373b8b5b.col_53, 1)`,
		`    │   └─TableFullScan cop[tikv] table:t373b8b5b keep order:false, stats:pseudo`,
		`    └─TableReader(Probe) root  data:Selection`,
		`      └─Selection cop[tikv]  in(test.tafab9ab4.col_35, 78, 177), le(test.tafab9ab4.col_35, 1)`,
		`        └─TableFullScan cop[tikv] table:tafab9ab4 keep order:false, stats:pseudo`))
	tk.MustExec(`CREATE TABLE a1 (a int PRIMARY KEY, b int);`)
	tk.MustExec(`CREATE TABLE a2 (a int PRIMARY KEY, b int);`)
	for range 20 {
		tk.MustQuery(`EXPLAIN FORMAT='plan_tree' SELECT STRAIGHT_JOIN * FROM a1 LEFT JOIN a2 ON a1.a = a2.a WHERE a1.a IN(a2.a, a2.b);`).Check(testkit.Rows(
			`MergeJoin root  inner join, left key:test.a1.a, right key:test.a2.a`,
			`├─TableReader(Build) root  data:TableFullScan`,
			`│ └─TableFullScan cop[tikv] table:a2 keep order:true, stats:pseudo`,
			`└─TableReader(Probe) root  data:TableFullScan`,
			`  └─TableFullScan cop[tikv] table:a1 keep order:true, stats:pseudo`))
	}
}
