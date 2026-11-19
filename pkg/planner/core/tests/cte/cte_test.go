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

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
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
}

// TestCTEColumnPruning covers ordinary SQL without SQL_CALC_FOUND_ROWS.
func TestCTEColumnPruning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_opt_force_inline_cte=OFF")
	tk.MustExec("create table cte_columns(id int primary key,k int,payload varchar(100) not null)")
	tk.MustExec("insert into cte_columns values(1,1,'a'),(2,1,'a'),(3,2,'b')")
	readsPayload := func(plan base.PhysicalPlan) bool {
		seen := make(map[base.PhysicalPlan]bool)
		var visit func(base.PhysicalPlan) bool
		visit = func(p base.PhysicalPlan) bool {
			if p == nil || seen[p] {
				return false
			}
			seen[p] = true
			switch x := p.(type) {
			case *physicalop.PhysicalCTE:
				if visit(x.SeedPlan) || visit(x.RecurPlan) {
					return true
				}
			case *physicalop.PhysicalTableReader:
				for _, child := range x.TablePlans {
					if visit(child) {
						return true
					}
				}
			case *physicalop.PhysicalTableScan:
				for _, col := range x.Columns {
					if col.Name.L == "payload" {
						return true
					}
				}
			}
			for _, child := range p.Children() {
				if visit(child) {
					return true
				}
			}
			return false
		}
		return visit(plan)
	}
	for _, tc := range []struct {
		name, sql string
		rows      []string
		payload   bool
	}{
		{"count", "with c as(select id,k,payload from cte_columns) select count(*) from c a join c b on a.id=b.id", []string{"3"}, false},
		{"ordinary-select", "with c as(select id,k,payload from cte_columns) select a.id from c a join c b on a.id=b.id order by a.id", []string{"1", "2", "3"}, false},
		{"other-consumer", "with c as(select id,k,payload from cte_columns) select a.id,b.payload from c a join c b on a.id=b.id order by a.id", []string{"1 a", "2 a", "3 b"}, true},
		{"pushed-predicate", "with c as(select id,k,payload from cte_columns) select count(*) from c a join c b on a.id=b.id where a.payload='a'", []string{"2"}, true},
		{"nested", "with c as(select id,k,payload from cte_columns),d as(select a.id,a.payload from c a join c b on a.id=b.id) select count(*) from d x join d y on x.id=y.id", []string{"3"}, false},
		{"nested-shared", "with c as(select id,k,payload from cte_columns),d as(select id from c) select a.payload from c a join d x on a.id=x.id join d y on a.id=y.id order by a.id", []string{"a", "a", "b"}, true},
		{"nested-shared-reverse", "with c as(select id,k,payload from cte_columns),d as(select id from c) select a.payload from d x join d y on x.id=y.id join c a on a.id=x.id order by a.id", []string{"a", "a", "b"}, true},
		{"grouped-seed", "with c as(select k,id%2 as bucket,payload,count(*) n from cte_columns group by k,bucket,payload) select count(*) from c a join c b on a.k=b.k", []string{"5"}, true},
		{"distinct-seed", "with c as(select distinct k,payload from cte_columns) select count(*) from c a join c b on a.k=b.k", []string{"2"}, true},
		{"inner-limit", "with c as(select id,k,payload from cte_columns order by payload desc,id limit 2) select count(*) from c a join c b on a.id=b.id", []string{"2"}, true},
		{"recursive", "with recursive c(id,k,payload) as(select id,k,payload from cte_columns union all select id+10,k,payload from c where id<4) select count(*) from c a join c b on a.id=b.id", []string{"6"}, true},
		{"nested-recursive-consumer", "with recursive c as(select id,k,payload from cte_columns),d(id,payload) as(select id,payload from c union all select id+10,payload from d where id<4) select a.payload from c a join d x on a.id=x.id join d y on a.id=y.id order by a.id", []string{"a", "a", "b"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
			plan := tk.Session().GetSessionVars().StmtCtx.GetPlan().(base.PhysicalPlan)
			require.Equal(t, tc.payload, readsPayload(plan))
		})
	}
	tk.MustExec("set @cte_side_effect=0")
	tk.MustQuery("with c as(select id,@cte_side_effect:=@cte_side_effect+1 as n from cte_columns) select count(*) from c a join c b on a.id=b.id").Check(testkit.Rows("3"))
	tk.MustQuery("select @cte_side_effect").Check(testkit.Rows("3"))
	tk.MustExec("set @cte_side_effect=0")
	tk.MustQuery("with c as(select id,(select @cte_side_effect:=@cte_side_effect+1 where cte_columns.id>0) as n from cte_columns) select count(*) from c a join c b on a.id=b.id").Check(testkit.Rows("3"))
	tk.MustQuery("select @cte_side_effect").Check(testkit.Rows("3"))
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("prepare pruned_cte from 'with c as(select id,k,payload from cte_columns where id>=?) select count(*) from c a join c b on a.id=b.id'")
	for _, tc := range []struct{ cut, total, hit string }{{"1", "3", "0"}, {"1", "3", "1"}, {"3", "1", "1"}, {"3", "1", "1"}} {
		tk.MustExec("set @cut=" + tc.cut)
		tk.MustQuery("execute pruned_cte using @cut").Check(testkit.Rows(tc.total))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(tc.hit))
	}
}
