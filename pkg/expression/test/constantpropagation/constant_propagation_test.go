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

func TestLikeColumnPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	for _, collation := range []string{"utf8mb4_general_ci", "utf8mb4_bin"} {
		t.Run(collation, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists o, i")
			tk.MustExec("create table o (k varchar(10) not null) charset utf8mb4 collate " + collation)
			tk.MustExec("create table i (k varchar(10) not null) charset utf8mb4 collate " + collation)
			tk.MustExec("insert into o values ('a ')")
			tk.MustExec("insert into i values ('a')")
			for _, op := range []string{"like", "ilike"} {
				t.Run(op, func(t *testing.T) {
					tk := testkit.NewTestKit(t, store)
					tk.MustExec("use test")
					// Equality ignores trailing spaces, but pattern matching does not.
					tk.MustQuery("select hex(o.k), hex(i.k), o.k " + op + " 'a_' from o left join i on o.k = i.k").Check(testkit.Rows("6120 61 1"))
					for _, join := range []string{"o left join i", "i right join o", "o join i"} {
						tk.MustQuery("select hex(o.k), hex(i.k) from " + join + " on o.k = i.k where o.k " + op + " 'a_'").Check(testkit.Rows("6120 61"))
						tk.MustQuery("explain format='brief' select hex(o.k), hex(i.k) from " + join + " on o.k = i.k where o.k " + op + " 'a_'").CheckNotContain(op + "(test.i.k")
						// Also cover a pattern predicate nested under NOT.
						tk.MustQuery("select hex(o.k), hex(i.k) from " + join + " on o.k = i.k where o.k not " + op + " 'a'").Check(testkit.Rows("6120 61"))
						// The pattern operand must not be substituted either.
						tk.MustQuery("select hex(o.k), hex(i.k) from " + join + " on o.k = i.k where 'a ' " + op + " o.k").Check(testkit.Rows("6120 61"))
					}
					tk.MustQuery("select hex(o.k), hex(i.k) from o left join i on o.k = i.k and o.k " + op + " 'a_'").Check(testkit.Rows("6120 61"))
				})
			}
		})
	}
	for _, collation := range []string{"binary", "utf8mb4_0900_bin"} {
		t.Run(collation, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists o, i")
			charset := "utf8mb4"
			if collation == "binary" {
				charset = "binary"
			}
			tk.MustExec("create table o (k varchar(10) not null) charset " + charset + " collate " + collation)
			tk.MustExec("create table i (k varchar(10) not null) charset " + charset + " collate " + collation)
			tk.MustExec("insert into o values ('a ')")
			tk.MustExec("insert into i values ('a'), ('a ')")
			for _, op := range []string{"like", "ilike"} {
				for _, join := range []string{"o left join i", "i right join o", "o join i"} {
					query := "select hex(o.k), hex(i.k) from " + join + " on o.k = i.k where o.k " + op + " 'a_'"
					tk.MustQuery(query).Check(testkit.Rows("6120 6120"))
					tk.MustQuery("explain format='brief' " + query).CheckContain(op + "(test.i.k")
				}
			}
		})
	}
	t.Run("numeric_binary_collation", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists o, i")
		tk.MustExec("create table o (k decimal(4,2) not null)")
		tk.MustExec("create table i (k decimal(4,1) not null)")
		tk.MustExec("insert into o values (1)")
		tk.MustExec("insert into i values (1)")
		for _, op := range []string{"like", "ilike"} {
			tk.MustQuery("select o.k, i.k from o left join i on o.k = i.k where o.k " + op + " '1.__'").Check(testkit.Rows("1.00 1.0"))
		}
	})
}

func TestSignedZeroPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	// y is -0 for the stored 0 in x. DOUBLE equality treats the two zeros as equal,
	// but atan2 and a cast to string tell them apart, so the columns are not interchangeable there.
	tk.MustExec("create table t (x double, y double as (-x))")
	tk.MustExec("insert into t (x) values (0)")
	tk.MustQuery("select x = y, atan2(x, -1) > 0, atan2(y, -1) > 0 from t").Check(testkit.Rows("1 1 0"))

	// Column to column, in a filter and in the conditions of an inner and an outer join.
	for _, sql := range []string{
		"select count(*) from t where x = y and atan2(x, -1) > 0",
		"select count(*) from t where x = y and atan(x, -1) > 0",
		"select count(*) from t where x = y and atan2(x * 1, -1) > 0",
		"select count(*) from t where x = y and atan2(-1, x) < 0",
		"select count(*) from t where x = y and cast(x * 1 as char) = '0'",
		"select count(*) from t where x = y and concat(round(x), '') = '0'",
		"select count(*) from t a join t b on a.x = b.y where atan2(a.x, -1) > 0",
		"select count(b.x) from t a left join t b on a.x = b.y and atan2(a.x, -1) > 0",
		"select count(b.x) from t b right join t a on a.x = b.y and atan2(a.x, -1) > 0",
	} {
		tk.MustQuery(sql).Check(testkit.Rows("1"))
	}
	tk.MustQuery("select count(*) from t a join t b on a.x = b.y where atan2(b.y, -1) > 0").Check(testkit.Rows("0"))
	tk.MustQuery("explain format='brief' select count(*) from t where x = y and atan2(x, -1) > 0").CheckNotContain("atan2(test.t.y")

	// Column to constant: -'o' is -0, equal to the stored 0.
	tk.MustQuery("select count(*) from t where x = -'o' and atan2(x, -1) > 0").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t where x = 0 and atan2(x, -1) > 0").Check(testkit.Rows("1"))
	tk.MustQuery("select count(b.x) from t a left join t b on a.x = -'o' and atan2(a.x, -1) > 0").Check(testkit.Rows("1"))

	// Predicates that do not observe the sign of a zero are still propagated.
	tk.MustQuery("select count(*) from t where x = y and x + 1 > 0").Check(testkit.Rows("1"))
	tk.MustQuery("explain format='brief' select count(*) from t where x = y and x + 1 > 0").CheckContain("plus(test.t.y")
}

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
