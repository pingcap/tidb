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
		`Sort root  test.tafab9ab4.col_32, Column`,
		`└─Projection root  test.tafab9ab4.col_32, substring_index(test.tafab9ab4.col_36, ,, 2)->Column, test.tafab9ab4.col_32`,
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

func TestBinaryColumnConstantPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// `user_id = 'u1'` compares bytes, so 'u1' replaces user_id in the OR, `user_id > 'u1'` folds to false and
	// `object_id > 1` becomes part of the index range instead of a filter.
	tk.MustExec("create table c (user_id varbinary(64), is_primary int, object_id bigint, key k (user_id, is_primary, object_id))")
	tk.MustExec("insert into c values ('u1', 1, 1), ('u1', 1, 2), ('u1', 1, 3), ('u1', 2, 1), ('u2', 1, 1)")
	keysetQuery := "select object_id from c where user_id = 'u1' and is_primary = 1 and " +
		"((user_id > 'u1') or (user_id = 'u1' and is_primary > 1) or (user_id = 'u1' and is_primary = 1 and object_id > 1)) " +
		"order by object_id limit 10"
	tk.MustQuery("explain format='plan_tree' " + keysetQuery).Check(testkit.Rows(
		`Limit root  offset:0, count:10`,
		`└─IndexReader root  index:Limit`,
		`  └─Limit cop[tikv]  offset:0, count:10`,
		`    └─IndexRangeScan cop[tikv] table:c, index:k(user_id, is_primary, object_id) range:("u1" 1 1,"u1" 1 +inf], keep order:true, stats:pseudo`))
	tk.MustQuery(keysetQuery).Check(testkit.Rows("2", "3"))

	// A binary column joined with a case-insensitive one: substituting 'a' into `t1.a = t2.a` would compare with
	// t2.a's collation and wrongly match 'A'.
	tk.MustExec("create table t1 (a varbinary(10), index (a))")
	tk.MustExec("create table t2 (a varchar(10) collate utf8mb4_general_ci, index (a))")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("insert into t2 values ('A'), ('a')")
	tk.MustQuery("select * from t1, t2 where t1.a = t2.a and t1.a = 'a'").Check(testkit.Rows("a a"))
	tk.MustQuery("select * from t1 left join t2 on t1.a = t2.a where t1.a = 'a'").Check(testkit.Rows("a a"))
	tk.MustQuery("select * from t2 left join t1 on t1.a = t2.a and t1.a = 'a' order by t2.a").Check(testkit.Rows("A <nil>", "a a"))

	// Comparisons that are not byte-wise must not propagate: an explicit COLLATE pads spaces, and a numeric
	// comparison matches '01' = 1.
	tk.MustExec("create table t3 (a varbinary(10))")
	tk.MustExec("insert into t3 values ('x'), ('x '), ('1'), ('01')")
	tk.MustQuery("select hex(a) from t3 where a = 'x' collate utf8mb4_bin and length(a) = 2").Check(testkit.Rows("7820"))
	tk.MustQuery("select a from t3 where a = 1 and length(a) = 2").Check(testkit.Rows("01"))

	// BINARY(n) stores its value padded with zero bytes.
	tk.MustExec("create table t4 (a binary(5))")
	tk.MustExec("insert into t4 values ('abc')")
	tk.MustQuery("select hex(a) from t4 where a = 'abc' and length(a) = 3").Check(testkit.Rows())
	tk.MustQuery("select hex(a), length(a) from t4 where a = 'abc\\0\\0' and length(a) = 5").Check(testkit.Rows("6162630000 5"))

	// A cached plan holds the propagated parameter, not its value at the first execution.
	tk.MustExec("prepare st from 'select object_id from c where user_id = ? and length(user_id) = ?'")
	tk.MustExec("set @u = 'u1', @n = 2")
	tk.MustQuery("execute st using @u, @n").Sort().Check(testkit.Rows("1", "1", "2", "3"))
	tk.MustExec("set @u = 'u12', @n = 2")
	tk.MustQuery("execute st using @u, @n").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @u = 'u2', @n = 2")
	tk.MustQuery("execute st using @u, @n").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}
