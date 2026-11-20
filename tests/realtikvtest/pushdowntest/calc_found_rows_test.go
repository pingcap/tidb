// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pushdowntest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
)

func TestSQLCalcFoundRowsPushdown(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists real_found_rows")
	tk.MustExec("create table real_found_rows (id int primary key, category int, payload varchar(100), key(category))")
	t.Cleanup(func() { tk.MustExec("drop table if exists real_found_rows") })
	tk.MustExec("insert into real_found_rows values (1,1,'a'),(2,1,'b'),(3,2,'c'),(4,2,'d')")
	query := "select SQL_CALC_FOUND_ROWS payload from real_found_rows force index(category) where category>=1 order by id limit 1"
	tk.MustQuery(query).Check(testkit.Rows("a"))
	tk.MustQuery("select found_rows()").Check(testkit.Rows("4"))
	tk.MustHavePlan(query, "CalcFoundRows")
	tk.MustHavePlan(query, "IndexLookUp")
	tk.MustExec("prepare st from 'select SQL_CALC_FOUND_ROWS payload from real_found_rows force index(category) where category>=? order by id limit 1'")
	tk.MustExec("set @category=1")
	tk.MustQuery("execute st using @category").Check(testkit.Rows("a"))
	tk.MustQuery("select found_rows()").Check(testkit.Rows("4"))
	tk.MustExec("set @category=2")
	tk.MustQuery("execute st using @category").Check(testkit.Rows("c"))
	tk.MustQuery("select found_rows(),@@last_plan_from_cache").Check(testkit.Rows("2 1"))
	tk.MustQuery("select SQL_CALC_FOUND_ROWS category,count(*) as n from real_found_rows group by category having n>=2 order by category limit 1").Check(testkit.Rows("1 2"))
	tk.MustQuery("select found_rows()").Check(testkit.Rows("2"))
}

func TestSQLCalcFoundRowsCTEPushdown(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	tk.MustExec("set tidb_opt_force_inline_cte=OFF")
	tk.MustExec("drop table if exists real_cte_found_rows")
	tk.MustExec("create table real_cte_found_rows(id int primary key,x int)")
	t.Cleanup(func() { tk.MustExec("drop table if exists real_cte_found_rows") })
	tk.MustExec("insert into real_cte_found_rows values(1,1),(2,1),(3,2),(4,2)")
	tk.MustQuery("with c as (select id from real_cte_found_rows order by id limit 3) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 20,1").Check(testkit.Rows())
	tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("set tidb_enable_plan_cache_for_subquery=ON")
	tk.MustExec("prepare cte_st from 'with c as (select id from real_cte_found_rows where id>=?) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 1'")
	tk.MustExec("set @cut=2")
	tk.MustQuery("execute cte_st using @cut").Check(testkit.Rows("2 3"))
	tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
	tk.MustQuery("execute cte_st using @cut").Check(testkit.Rows("2 3"))
	tk.MustQuery("select found_rows(),@@last_plan_from_cache").Check(testkit.Rows("3 1"))
	t.Run("parameter-rebind", func(t *testing.T) {
		t.Skip("materialized CTE parameter rebinding is blocked by #73112, including ordinary SELECT")
		tk := testkit.NewTestKitWithSession(t, store, tk.Session())
		tk.MustExec("set @cut=3")
		tk.MustQuery("execute cte_st using @cut").Check(testkit.Rows("3 4"))
		tk.MustQuery("select found_rows(),@@last_plan_from_cache").Check(testkit.Rows("1 1"))
	})
	// Keep cache reuse and changed-data coverage independent of parameter rebinding.
	tk.MustExec("set @cut=2")
	tk.MustExec("insert into real_cte_found_rows values(5,3)")
	tk.MustQuery("execute cte_st using @cut").Check(testkit.Rows("2 3"))
	tk.MustQuery("select found_rows(),@@last_plan_from_cache").Check(testkit.Rows("6 1"))
}
