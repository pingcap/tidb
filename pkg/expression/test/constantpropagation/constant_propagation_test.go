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

func TestPadSpaceColumnPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	predicates := []struct {
		name      string
		cond      string
		planToken string
	}{
		{"concat", "concat(o.k, 'x') = 'a x'", "concat(test.i.k"},
		{"concat_ws", "concat_ws('-', o.k, 'x') = 'a -x'", "concat_ws(-, test.i.k"},
		{"regexp", "o.k regexp '^a $'", "regexp(test.i.k"},
		{"regexp_like", "regexp_like(o.k, '^a $')", "regexp_like(test.i.k"},
		{"reverse", "reverse(o.k) = ' a'", "reverse(test.i.k"},
		{"repeat", "repeat(o.k, 2) = 'a a '", "repeat(test.i.k"},
		{"insert", "insert(o.k, 2, 0, 'x') = 'ax '", "insert(test.i.k"},
		{"lpad", "lpad(o.k, 3, '0') = '0a '", "lpad(test.i.k"},
	}

	for _, collation := range []string{"utf8mb4_general_ci", "utf8mb4_bin", "utf8mb4_unicode_ci"} {
		t.Run(collation, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists o, i")
			tk.MustExec("create table o (k varchar(10) not null) charset utf8mb4 collate " + collation)
			tk.MustExec("create table i (k varchar(10) not null) charset utf8mb4 collate " + collation)
			tk.MustExec("insert into o values ('a ')")
			tk.MustExec("insert into i values ('a')")

			for _, pred := range predicates {
				t.Run(pred.name, func(t *testing.T) {
					for _, join := range []string{"o left join i", "i right join o", "o join i"} {
						query := "select hex(o.k), hex(i.k) from " + join + " on o.k = i.k where " + pred.cond
						tk.MustQuery(query).Check(testkit.Rows("6120 61"))
						tk.MustQuery("explain format='brief' " + query).CheckNotContain(pred.planToken)
					}
					// ON clause, OR branch, IN subquery, and EXISTS subquery.
					tk.MustQuery("select hex(o.k), hex(i.k) from o left join i on o.k = i.k and " + pred.cond).Check(testkit.Rows("6120 61"))
					tk.MustQuery("select hex(o.k), hex(i.k) from o join i on o.k = i.k where o.k = 'z' or " + pred.cond).Check(testkit.Rows("6120 61"))
					tk.MustQuery("select hex(o.k) from o where o.k in (select k from i) and " + pred.cond).Check(testkit.Rows("6120"))
					tk.MustQuery("select hex(o.k) from o where exists (select 1 from i where i.k = o.k) and " + pred.cond).Check(testkit.Rows("6120"))
				})
			}

			// Plain comparisons under the same collation should still propagate.
			tk.MustQuery("explain format='brief' select hex(o.k), hex(i.k) from o join i on o.k = i.k where o.k > '0'").CheckContain("gt(test.i.k")
			tk.MustQuery("explain format='brief' select hex(o.k), hex(i.k) from o join i on o.k = i.k where o.k in ('a', 'b')").CheckContain("in(test.i.k")
		})
	}

	// Byte-exact collations (binary, utf8mb4_0900_bin) should continue propagating string functions.
	for _, collation := range []string{"binary", "utf8mb4_0900_bin"} {
		t.Run(collation, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists o, i")
			chs := "utf8mb4"
			if collation == "binary" {
				chs = "binary"
			}
			tk.MustExec("create table o (k varchar(10) not null) charset " + chs + " collate " + collation)
			tk.MustExec("create table i (k varchar(10) not null) charset " + chs + " collate " + collation)
			tk.MustExec("insert into o values ('a ')")
			tk.MustExec("insert into i values ('a'), ('a ')")

			for _, join := range []string{"o left join i", "i right join o", "o join i"} {
				query := "select hex(o.k), hex(i.k) from " + join + " on o.k = i.k where concat(o.k, 'x') = 'a x'"
				tk.MustQuery(query).Check(testkit.Rows("6120 6120"))
				tk.MustQuery("explain format='brief' " + query).CheckContain("concat(test.i.k")
			}
		})
	}
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
