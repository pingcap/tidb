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

package cte

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestCTEWithDifferentSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE USER 'db_a'@'%';")
	tk.MustExec("CREATE USER 'db_b'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `db_a`.* TO 'db_a'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `db_b`.* TO 'db_a'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `db_b`.* TO 'db_b'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `db_b`.* TO 'db_b'@'%';")
	tk.MustExec("create database db_a;")
	tk.MustExec("create database db_b;")
	tk.MustExec("use db_a;")
	tk.MustExec(`CREATE TABLE tmp_table1 (
   id decimal(18,0) NOT NULL,
   row_1 varchar(255) DEFAULT NULL,
   PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`create ALGORITHM=UNDEFINED DEFINER=db_a@'%' SQL SECURITY DEFINER VIEW view_test_v1 as (
                         with rs1 as(
                            select otn.*
                             from tmp_table1 otn
                          )
                        select ojt.* from rs1 ojt
                        )`)
	tk.MustExec("use db_b;")
	tk.MustQuery("explain format = 'plan_tree' select * from db_a.view_test_v1;").Check(testkit.Rows(
		"CTEFullScan root CTE:rs1 AS ojt data:CTE_0",
		"CTE_0 root  Non-Recursive CTE",
		"└─TableReader(Seed Part) root  data:TableFullScan",
		"  └─TableFullScan cop[tikv] table:otn keep order:false, stats:pseudo"))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_cte_predicate_pushdown")
	tk.MustExec("create table t_cte_predicate_pushdown(a int, b int, c int, key(a), key(b), key(c))")
	tk.MustExec("insert into t_cte_predicate_pushdown values (1, 2, 1), (1, 3, 2), (1, 0, 2), (2, 5, 1), (2, 6, 2), (3, 1, 1), (3, 2, 2)")
	tk.MustExec("set @@tidb_opt_force_inline_cte = 0")
	tk.MustQuery(`
		explain format = 'plan_tree'
		with cte as (
			select * from t_cte_predicate_pushdown
		)
		select *
		from cte c1
		join cte c2 on c1.a = c2.a
		where c1.b > 1 and c2.b > 1 and c1.c = 1 and c2.c = 2
	`).Check(testkit.Rows(
		"HashJoin root  inner join, equal:[eq(test.t_cte_predicate_pushdown.a, test.t_cte_predicate_pushdown.a)]",
		"├─Selection(Build) root  eq(test.t_cte_predicate_pushdown.c, 2)",
		"│ └─CTEFullScan root CTE:cte AS c2 data:CTE_0",
		"└─Selection(Probe) root  eq(test.t_cte_predicate_pushdown.c, 1)",
		"  └─CTEFullScan root CTE:cte AS c1 data:CTE_0",
		"CTE_0 root  Non-Recursive CTE",
		"└─IndexLookUp(Seed Part) root  ",
		"  ├─IndexRangeScan(Build) cop[tikv] table:t_cte_predicate_pushdown, index:c(c) range:[1,1], [2,2], keep order:false, stats:pseudo",
		"  └─Selection(Probe) cop[tikv]  gt(test.t_cte_predicate_pushdown.b, 1), not(isnull(test.t_cte_predicate_pushdown.a))",
		"    └─TableRowIDScan cop[tikv] table:t_cte_predicate_pushdown keep order:false, stats:pseudo"))
	tk.MustQuery(`
		with cte as (
			select * from t_cte_predicate_pushdown
		)
		select *
		from cte c1
		join cte c2 on c1.a = c2.a
		where c1.b > 1 and c2.b > 1 and c1.c = 1 and c2.c = 2
		order by c1.a, c1.b, c2.b
	`).Check(testkit.Rows(
		"1 2 1 1 3 2",
		"2 5 1 2 6 2"))

	tk.MustQuery(`
		explain format = 'plan_tree'
		with cte1 as (
			select * from t_cte_predicate_pushdown
		), cte2 as (
			select * from cte1
		)
		select *
		from cte1 b
		join cte2 c1 on b.a = c1.a
		join cte2 c2 on c1.a = c2.a
		where b.b > 1 and c1.b > 1 and c2.b > 1 and c1.c = 1 and c2.c = 2
	`).Check(testkit.Rows(
		"Projection root  test.t_cte_predicate_pushdown.a, test.t_cte_predicate_pushdown.b, test.t_cte_predicate_pushdown.c, test.t_cte_predicate_pushdown.a, test.t_cte_predicate_pushdown.b, test.t_cte_predicate_pushdown.c, test.t_cte_predicate_pushdown.a, test.t_cte_predicate_pushdown.b, test.t_cte_predicate_pushdown.c",
		"└─HashJoin root  inner join, equal:[eq(test.t_cte_predicate_pushdown.a, test.t_cte_predicate_pushdown.a)]",
		"  ├─Selection(Build) root  gt(test.t_cte_predicate_pushdown.b, 1), not(isnull(test.t_cte_predicate_pushdown.a))",
		"  │ └─CTEFullScan root CTE:cte1 AS b data:CTE_0",
		"  └─HashJoin(Probe) root  inner join, equal:[eq(test.t_cte_predicate_pushdown.a, test.t_cte_predicate_pushdown.a)]",
		"    ├─Selection(Build) root  eq(test.t_cte_predicate_pushdown.c, 2)",
		"    │ └─CTEFullScan root CTE:cte2 AS c2 data:CTE_1",
		"    └─Selection(Probe) root  eq(test.t_cte_predicate_pushdown.c, 1)",
		"      └─CTEFullScan root CTE:cte2 AS c1 data:CTE_1",
		"CTE_0 root  Non-Recursive CTE",
		"└─TableReader(Seed Part) root  data:TableFullScan",
		"  └─TableFullScan cop[tikv] table:t_cte_predicate_pushdown keep order:false, stats:pseudo",
		"CTE_1 root  Non-Recursive CTE",
		"└─Selection(Seed Part) root  gt(test.t_cte_predicate_pushdown.b, 1), not(isnull(test.t_cte_predicate_pushdown.a)), or(eq(test.t_cte_predicate_pushdown.c, 1), eq(test.t_cte_predicate_pushdown.c, 2))",
		"  └─CTEFullScan root CTE:cte1 data:CTE_0"))
	tk.MustQuery(`
		with cte1 as (
			select * from t_cte_predicate_pushdown
		), cte2 as (
			select * from cte1
		)
		select *
		from cte1 b
		join cte2 c1 on b.a = c1.a
		join cte2 c2 on c1.a = c2.a
		where b.b > 1 and c1.b > 1 and c2.b > 1 and c1.c = 1 and c2.c = 2
		order by b.a, b.b, c1.b, c2.b
	`).Check(testkit.Rows(
		"1 2 1 1 2 1 1 3 2",
		"1 3 2 1 2 1 1 3 2",
		"2 5 1 2 5 1 2 6 2",
		"2 6 2 2 5 1 2 6 2"))
}
