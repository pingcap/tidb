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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSQLCalcFoundRowsPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("set tidb_enable_plan_cache_for_param_limit=ON")
	tk.MustExec("create table found_rows (id int primary key, category int, key(category))")
	tk.MustExec("insert into found_rows values (1, 1), (2, 1), (3, 2)")
	tk.MustExec("create table found_meta (id int, value int)")
	tk.MustExec("insert into found_meta values (1,1),(1,2),(2,1)")
	tk.MustExec("prepare st from 'select SQL_CALC_FOUND_ROWS id from found_rows force index(category) where category >= ? order by id limit ? offset ?'")
	tk.MustExec("set @category=1, @count=1, @offset=0")
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("1"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("3 0"))
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("1"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("3 1"))
	tk.MustExec("prepare joined from 'select SQL_CALC_FOUND_ROWS distinct t.id from found_rows t join found_meta m on t.id=m.id where m.value>=? order by t.id limit ?'")
	tk.MustExec("set @value=1, @count=1")
	tk.MustQuery("execute joined using @value,@count").Check(testkit.Rows("1"))
	tk.MustQuery("select found_rows()").Check(testkit.Rows("2"))
	tk.MustQuery("execute joined using @value,@count").Check(testkit.Rows("1"))
	tk.MustQuery("select found_rows(),@@last_plan_from_cache").Check(testkit.Rows("2 1"))
	tk.MustExec("set @value=2")
	tk.MustQuery("execute joined using @value,@count").Check(testkit.Rows("1"))
	tk.MustQuery("select found_rows(),@@last_plan_from_cache").Check(testkit.Rows("1 1"))

	tk.MustExec("insert into found_rows values (4, 1)")
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("1"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("4 1"))
	tk.MustExec("set @category=2")
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("3"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("1 1"))
	tk.MustExec("set @category=1, @count=2, @offset=1")
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select found_rows()").Check(testkit.Rows("4"))
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("4 1"))
	tk.MustExec("delete from found_rows where id=4")
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("3 1"))
	tk.MustExec("alter table found_rows add column extra int default 0")
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("3 0"))
	tk.MustQuery("execute st using @category, @count, @offset").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("3 1"))
}

func TestSQLCalcFoundRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table found_rows (id int primary key, category int)")
	tk.MustExec("insert into found_rows values (1, 1), (2, 1), (3, 2)")
	tk.MustExec("create table found_meta (id int, value int)")
	tk.MustExec("insert into found_meta values (1, 1), (1, 2), (2, 1)")
	cases := []struct {
		query string
		rows  []string
		total string
	}{
		{"select SQL_CALC_FOUND_ROWS * from found_rows order by id limit 1", []string{"1 1"}, "3"},
		{"select SQL_CALC_FOUND_ROWS * from found_rows order by field(id,3,1,2) limit 1", []string{"3 2"}, "3"},
		{"select SQL_CALC_FOUND_ROWS id from found_rows order by id limit 0", nil, "3"},
		{"select SQL_CALC_FOUND_ROWS id from found_rows order by id limit 20, 1", nil, "3"},
		{"select SQL_CALC_FOUND_ROWS id from found_rows where id > 10 limit 1", nil, "0"},
		{"select SQL_CALC_FOUND_ROWS id from found_rows where id > 1 order by id", []string{"2", "3"}, "2"},
		{"select SQL_CALC_FOUND_ROWS distinct category from found_rows order by category limit 1", []string{"1"}, "2"},
		{"select SQL_CALC_FOUND_ROWS category, count(*) as n from found_rows group by category having n >= 2 order by n limit 1", []string{"1 2"}, "1"},
		{"select SQL_CALC_FOUND_ROWS count(*) from found_rows limit 1", []string{"3"}, "1"},
		{"select SQL_CALC_FOUND_ROWS id,(select sum(t.id)) from found_rows t group by id order by id limit 1", []string{"1 1"}, "3"},
		{"select SQL_CALC_FOUND_ROWS id from (select id from found_rows order by id limit 2) s order by id limit 1", []string{"1"}, "2"},
		{"select SQL_CALC_FOUND_ROWS id from (select id from found_rows where id=1 union all select id from found_rows where id>1) s order by id limit 1", []string{"1"}, "3"},
		{"select SQL_CALC_FOUND_ROWS id from found_rows where exists (select 1 from found_meta m where m.id=found_rows.id) order by id limit 1", []string{"1"}, "2"},
		{"select SQL_CALC_FOUND_ROWS id from found_rows where id in (select id from found_meta) order by id limit 1", []string{"1"}, "2"},
		{"select SQL_CALC_FOUND_ROWS t.id from found_rows t join found_meta m on t.id=m.id order by t.id limit 1", []string{"1"}, "3"},
		{"select SQL_CALC_FOUND_ROWS distinct t.id from found_rows t join found_meta m on t.id=m.id order by t.id limit 1", []string{"1"}, "2"},
		{"select SQL_CALC_FOUND_ROWS t.id, count(*) as n from found_rows t join found_meta m on t.id=m.id group by t.id having n>1 order by n limit 1", []string{"1 2"}, "1"},
		{"select SQL_CALC_FOUND_ROWS 1 limit 0", nil, "1"},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			tk.MustQuery(tc.query).Check(testkit.Rows(tc.rows...))
			tk.MustQuery("select found_rows()").Check(testkit.Rows(tc.total))
		})
	}
	result := tk.MustQuery("select SQL_CALC_FOUND_ROWS id from found_rows order by rand() limit 1")
	require.Len(t, result.Rows(), 1)
	tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
	tk.MustQuery("select SQL_CALC_FOUND_ROWS id from found_rows limit 1")
	tk.MustQuery("show warnings")
	tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
	tk.MustExec("set sql_mode=''")
	tk.MustQuery("select SQL_CALC_FOUND_ROWS distinct category from found_rows order by id limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select found_rows()").Check(testkit.Rows("2"))
	t.Run("aggregate-order-by", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set sql_mode=''")
		for _, predicate := range []string{"", " where id>10"} {
			tk.MustQuery("select SQL_CALC_FOUND_ROWS 1 from found_rows" + predicate + " order by count(*) limit 5").Check(testkit.Rows("1"))
			tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		}
	})
	t.Run("rollup-derived-and-cte", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustQuery("select SQL_CALC_FOUND_ROWS dayname('2026-01-01') as d, count(*) from (select dayname('2026-01-01') as d from found_rows) s where s.d is not null group by dayname('2026-01-01') with rollup order by d limit 1").Check(testkit.Rows("<nil> 3"))
		tk.MustQuery("select found_rows()").Check(testkit.Rows("2"))
		tk.MustQuery("with s as (select dayname('2026-01-01') as d from found_rows) select SQL_CALC_FOUND_ROWS dayname('2026-01-01') as d, count(*) from s where s.d is not null group by dayname('2026-01-01') with rollup order by d limit 1").Check(testkit.Rows("<nil> 3"))
		tk.MustQuery("select found_rows()").Check(testkit.Rows("2"))
	})
}

func TestSQLCalcFoundRowsViews(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table view_found_rows(id int primary key)")
	tk.MustExec("insert into view_found_rows values (1),(2),(3),(4)")
	tk.MustExec("create view view_page as select id from view_found_rows order by id limit 2")
	tk.MustExec("create view view_nested as select id from view_page")
	// Simulate a definition persisted by a version where the modifier was a noop.
	db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), db.Name, ast.NewCIStr("view_page"))
	require.NoError(t, err)
	info := tbl.Meta().Clone()
	view := *info.View
	info.View = &view
	info.View.SelectStmt = "select SQL_CALC_FOUND_ROWS id from test.view_found_rows order by id limit 2"
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		version, err := m.GenSchemaVersion()
		if err != nil {
			return err
		}
		if err := m.UpdateTable(db.ID, info); err != nil {
			return err
		}
		return m.SetSchemaDiff(&model.SchemaDiff{
			Version: version, Type: model.ActionCreateView, SchemaID: db.ID, TableID: info.ID, OldTableID: info.ID,
		})
	}))
	require.NoError(t, dom.Reload())
	loaded, err := dom.InfoSchema().TableByName(context.Background(), db.Name, info.Name)
	require.NoError(t, err)
	require.Equal(t, info.View.SelectStmt, loaded.Meta().View.SelectStmt)
	t.Run("legacy-modifier", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		for _, name := range []string{"view_page", "view_nested"} {
			for _, tc := range []struct {
				query string
				rows  []string
				total string
			}{
				{"select id from " + name + " where id>99", nil, "0"},
				{"select id from " + name + " order by id limit 1", []string{"1"}, "1"},
				{"select SQL_CALC_FOUND_ROWS id from " + name + " order by id limit 1", []string{"1"}, "2"},
				{"select SQL_CALC_FOUND_ROWS id from " + name + " where id>99", nil, "0"},
			} {
				for _, prepared := range []bool{false, true} {
					query := tc.query
					if prepared {
						tk.MustExec("prepare view_stmt from '" + query + "'")
						query = "execute view_stmt"
					}
					tk.MustQuery(query).Check(testkit.Rows(tc.rows...))
					tk.MustQuery("select found_rows()").Check(testkit.Rows(tc.total))
				}
			}
		}
	})
	tk.MustExec("create view view_coin as select id from view_found_rows where rand()<0.5")
	tk.MustExec("create view view_coin_nested as select id from view_coin")
	tk.MustExec("create view view_token as select id, uuid() as token from view_found_rows")
	t.Run("non-repeatable", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		for _, name := range []string{"view_coin", "view_coin_nested", "view_token"} {
			query := "select SQL_CALC_FOUND_ROWS id from " + name + " order by id limit 1"
			tk.MustGetErrCode(query, mysql.ErrNotSupportedYet)
			tk.MustGetErrCode("explain "+query, mysql.ErrNotSupportedYet)
			tk.MustGetErrCode("prepare view_stmt from '"+query+"'", mysql.ErrNotSupportedYet)
		}
	})
}

func TestSQLCalcFoundRowsNonPreparedCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=ON")
	tk.MustExec("create table found_rows (id int primary key, category int, key(category))")
	tk.MustExec("insert into found_rows values (1, 1), (2, 1), (3, 2)")
	sql := "select SQL_CALC_FOUND_ROWS id from found_rows where category > 1 order by id limit 1"
	tk.MustQuery(sql).Check(testkit.Rows("3"))
	tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
	tk.MustQuery(sql).Check(testkit.Rows("3"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("1 1"))
	tk.MustQuery("select SQL_CALC_FOUND_ROWS id from found_rows where category > 0 order by id limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("3 1"))
}

func TestSQLCalcFoundRowsProtocolPrepare(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table found_rows (id int primary key)")
	tk.MustExec("insert into found_rows values (1), (2), (3)")
	id, _, _, err := tk.Session().PrepareStmt("select SQL_CALC_FOUND_ROWS id from found_rows where id >= ? order by id limit 1")
	require.NoError(t, err)
	for _, value := range []int{1, 2, 3, 4} {
		rs, err := tk.Session().ExecutePreparedStmt(context.Background(), id, expression.Args2Expressions4Test(value))
		require.NoError(t, err)
		result := tk.ResultSetToResult(rs, "protocol SQL_CALC_FOUND_ROWS")
		if value <= 3 {
			result.Check(testkit.Rows(fmt.Sprint(value)))
		} else {
			result.Check(testkit.Rows())
		}
		tk.MustQuery("select found_rows()").Check(testkit.Rows(fmt.Sprint(max(0, 4-value))))
	}
}

func TestSQLCalcFoundRowsStatementSnapshot(t *testing.T) {
	store := testkit.CreateMockStore(t)
	reader := testkit.NewTestKit(t, store)
	writer := testkit.NewTestKit(t, store)
	reader.MustExec("use test")
	writer.MustExec("use test")
	reader.MustExec("create table found_rows (id int primary key)")
	cases := []struct {
		name, sql, total string
		rows             []string
	}{
		{"ordinary", "select SQL_CALC_FOUND_ROWS id from found_rows order by id limit 4", "3", []string{"1", "2", "3"}},
		{"inline", "with c as (select id from found_rows) select SQL_CALC_FOUND_ROWS id from c order by id limit 1", "3", []string{"1"}},
		{"materialized", "with c as (select id from found_rows) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 1", "3", []string{"1 2"}},
		{"recursive", "with recursive c(id) as (select id from found_rows union all select id+10 from c where id<4) select SQL_CALC_FOUND_ROWS id from c order by id limit 1", "6", []string{"1"}},
	}
	for _, isolation := range []string{"READ-COMMITTED", "REPEATABLE-READ"} {
		for _, tc := range cases {
			t.Run(isolation+"/"+tc.name, func(t *testing.T) {
				reader.MustExec("delete from found_rows")
				reader.MustExec("insert into found_rows values (1), (2), (3)")
				reader.MustExec("set transaction_isolation='" + isolation + "'")
				reader.MustExec("set tidb_opt_force_inline_cte=OFF")
				require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData", func(sessionctx.Context) {
					writer.MustExec("insert into found_rows values (4)")
				}))
				defer func() {
					require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData"))
				}()
				reader.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
				reader.MustQuery("select found_rows()").Check(testkit.Rows(tc.total))
				reader.MustQuery("select count(*) from found_rows").Check(testkit.Rows("4"))
			})
		}
	}
}

func TestSQLCalcFoundRowsUnsupported(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table found_rows (id int primary key)")
	tk.MustExec("set tidb_enable_noop_functions=OFF")
	for _, sql := range []string{
		"select * from (select SQL_CALC_FOUND_ROWS 1 as id) c",
		"select (select SQL_CALC_FOUND_ROWS 1)",
		"select SQL_CALC_FOUND_ROWS * from (select SQL_CALC_FOUND_ROWS 1 as id) c",
		"with c as (select SQL_CALC_FOUND_ROWS 1 as id) select SQL_CALC_FOUND_ROWS * from c",
		"with c as (select SQL_CALC_FOUND_ROWS 1 as id) select * from c",
		"explain select * from (select SQL_CALC_FOUND_ROWS 1 as id) c",
		"select 1 union all select SQL_CALC_FOUND_ROWS 2",
		"insert into found_rows select * from (select SQL_CALC_FOUND_ROWS 1 as id) c",
		"create view found_rows_bad as select * from (select SQL_CALC_FOUND_ROWS 1 as id) c",
		"trace select (select SQL_CALC_FOUND_ROWS 1)",
		"plan replayer dump explain select (select SQL_CALC_FOUND_ROWS 1)",
	} {
		tk.MustGetErrCode(sql, mysql.ErrCantUseOptionHere)
		if !strings.HasPrefix(sql, "trace ") && !strings.HasPrefix(sql, "plan replayer ") {
			tk.MustGetErrCode("prepare invalid_found_rows from '"+sql+"'", mysql.ErrCantUseOptionHere)
		}
	}
	// Outer query blocks can still belong to unsupported statement shapes.
	for _, sql := range []string{
		"select SQL_CALC_FOUND_ROWS 1 union all select 2 limit 1",
		"(select SQL_CALC_FOUND_ROWS 1 union all select 2) union all select 3",
		"explain select SQL_CALC_FOUND_ROWS 1 union all select 2 limit 1",
		"select SQL_CALC_FOUND_ROWS * from found_rows lock in share mode",
		"insert into found_rows select SQL_CALC_FOUND_ROWS 1",
		"replace into found_rows select SQL_CALC_FOUND_ROWS 1",
		"explain insert into found_rows select SQL_CALC_FOUND_ROWS 1",
		"create view found_rows_view as select SQL_CALC_FOUND_ROWS 1 as id",
		"create or replace view found_rows_view as select SQL_CALC_FOUND_ROWS 1 as id",
		"insert into found_rows select SQL_CALC_FOUND_ROWS 1 union all select 2",
		"trace select SQL_CALC_FOUND_ROWS 1",
		"trace plan select SQL_CALC_FOUND_ROWS 1",
		"plan replayer dump explain select SQL_CALC_FOUND_ROWS 1",
	} {
		tk.MustGetErrCode(sql, mysql.ErrNotSupportedYet)
	}
	// CREATE TABLE ... SELECT is rejected before modifier validation.
	tk.MustContainErrMsg("create table found_rows_copy as select SQL_CALC_FOUND_ROWS 1 as id",
		"'CREATE TABLE ... SELECT' is not implemented yet")
	for _, sql := range []string{
		"select SQL_CALC_FOUND_ROWS * from found_rows for update",
		"select SQL_CALC_FOUND_ROWS * from found_rows where rand()<0.5 limit 1",
		"select SQL_CALC_FOUND_ROWS random_bytes(1) from found_rows limit 1",
		"select SQL_CALC_FOUND_ROWS @n:=id from found_rows limit 1",
		"select SQL_CALC_FOUND_ROWS id from found_rows where last_insert_id(id)>0 limit 1",
		"select SQL_CALC_FOUND_ROWS id from found_rows where is_free_lock('found_rows') limit 1",
		"select SQL_CALC_FOUND_ROWS row_number() over(order by id) from found_rows limit 1",
	} {
		tk.MustContainErrMsg(sql, "SQL_CALC_FOUND_ROWS")
	}
	for _, mode := range []string{"OFF", "ON", "WARN"} {
		t.Run("noop-"+mode, func(t *testing.T) {
			tk := testkit.NewTestKitWithSession(t, store, tk.Session())
			tk.MustExec("set tidb_enable_noop_functions=" + mode)
			tk.MustQuery("with c as (select 1 as id union all select 2) select SQL_CALC_FOUND_ROWS * from c order by id limit 1").Check(testkit.Rows("1"))
			tk.MustQuery("select found_rows()").Check(testkit.Rows("2"))
			tk.MustQuery("show warnings").Check(testkit.Rows())
			tk.MustGetErrCode("select (select SQL_CALC_FOUND_ROWS 1)", mysql.ErrCantUseOptionHere)
			unionSQL := "select SQL_CALC_FOUND_ROWS 1 as id union all select 2 order by id limit 1"
			if mode == "OFF" {
				tk.MustGetErrCode(unionSQL, mysql.ErrNotSupportedYet)
				return
			}
			tk.MustQuery(unionSQL).Check(testkit.Rows("1"))
			if mode == "WARN" {
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1235 function SQL_CALC_FOUND_ROWS has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions"))
			} else {
				tk.MustQuery("show warnings").Check(testkit.Rows())
			}
			tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		})
	}
	t.Run("noop-fallback-shapes", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table fr_t(id int primary key, g int, key(g))")
		tk.MustExec("create table fr_u(id int primary key)")
		tk.MustExec("create table fr_d(id int)")
		tk.MustExec("insert into fr_t values (1,1),(2,1),(3,2),(4,2)")
		tk.MustExec("insert into fr_u values (5),(6)")

		noopWarn := testkit.Rows("Warning 1235 function SQL_CALC_FOUND_ROWS has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=ON")
		tk.MustExec("set tidb_enable_prepared_plan_cache=ON")

		for _, mode := range []string{"ON", "WARN"} {
			t.Run(mode, func(t *testing.T) {
				tk := testkit.NewTestKitWithSession(t, store, tk.Session())
				tk.MustExec("set tidb_enable_noop_functions=" + mode)
				checkWarnings := func() {
					if mode == "WARN" {
						tk.MustQuery("show warnings").Check(noopWarn)
					} else {
						tk.MustQuery("show warnings").Check(testkit.Rows())
					}
				}

				// Supported query stays native with no warning.
				tk.MustQuery("select SQL_CALC_FOUND_ROWS id from fr_t order by id limit 1").Check(testkit.Rows("1"))
				tk.MustQuery("show warnings").Check(testkit.Rows())
				tk.MustQuery("select found_rows()").Check(testkit.Rows("4"))

				// Unsupported shapes fall back to noop.
				unionSQL := "select SQL_CALC_FOUND_ROWS id from fr_t union all select id from fr_u order by id limit 1"
				tk.MustQuery(unionSQL).Check(testkit.Rows("1"))
				checkWarnings()
				tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
				tk.MustNotHavePlan(unionSQL, "CalcFoundRows")

				winSQL := "select SQL_CALC_FOUND_ROWS id, row_number() over (order by id) rn from fr_t order by id limit 1"
				for range 2 {
					tk.MustQuery(winSQL).Check(testkit.Rows("1 1"))
					checkWarnings()
					tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
				}

				tk.MustExec("begin")
				tk.MustQuery("select SQL_CALC_FOUND_ROWS id from fr_t order by id limit 1 for update").Check(testkit.Rows("1"))
				checkWarnings()
				tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
				tk.MustQuery("select SQL_CALC_FOUND_ROWS id from fr_t where id = 1 for update").Check(testkit.Rows("1"))
				checkWarnings()
				tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
				tk.MustExec("rollback")

				tk.MustExec("delete from fr_d")
				tk.MustExec("insert into fr_d select SQL_CALC_FOUND_ROWS id from fr_t order by id limit 1")
				checkWarnings()
				tk.MustQuery("select id from fr_d").Check(testkit.Rows("1"))

				tk.MustExec("create or replace view fr_v1 as select SQL_CALC_FOUND_ROWS id from fr_t order by id limit 2")
				checkWarnings()
				require.Contains(t, tk.MustQuery("show create view fr_v1").Rows()[0][1], "SQL_CALC_FOUND_ROWS")
				tk.MustQuery("select id from fr_v1").Check(testkit.Rows("1", "2"))

				tk.MustQuery("select SQL_CALC_FOUND_ROWS id from fr_t where rand() < 2 order by id limit 1").Check(testkit.Rows("1"))
				checkWarnings()
				tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))

				tk.MustQuery("select SQL_CALC_FOUND_ROWS @fr_v := id from fr_t order by id limit 1").Check(testkit.Rows("1"))
				checkWarnings()
				tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
				tk.MustQuery("select @fr_v").Check(testkit.Rows("1"))
			})
		}

		// Prepared statements must re-check tidb_enable_noop_functions on EXECUTE.
		tk.MustExec("set tidb_enable_noop_functions=ON")
		tk.MustExec("prepare st_union from 'select SQL_CALC_FOUND_ROWS id from fr_t union all select id from fr_u order by id limit 1'")
		tk.MustExec("prepare st_win from 'select SQL_CALC_FOUND_ROWS id, row_number() over (order by id) rn from fr_t where id >= ? order by id limit 1'")
		tk.MustExec("prepare st_lock from 'select SQL_CALC_FOUND_ROWS id from fr_t where id = ? for update'")
		tk.MustExec("set @p = 1")
		tk.MustQuery("execute st_union").Check(testkit.Rows("1"))
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustQuery("execute st_win using @p").Check(testkit.Rows("1 1"))
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustQuery("execute st_lock using @p").Check(testkit.Rows("1"))
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("execute st_lock using @p").Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustQuery("select found_rows(), @@last_plan_from_cache").Check(testkit.Rows("1 0"))

		tk.MustExec("set tidb_enable_noop_functions=WARN")
		tk.MustQuery("execute st_union").Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(noopWarn)
		tk.MustQuery("execute st_win using @p").Check(testkit.Rows("1 1"))
		tk.MustQuery("show warnings").Check(noopWarn)
		tk.MustQuery("execute st_lock using @p").Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(noopWarn)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		tk.MustExec("set tidb_enable_noop_functions=OFF")
		tk.MustGetErrCode("execute st_union", mysql.ErrNotSupportedYet)
		tk.MustGetErrCode("execute st_win using @p", mysql.ErrNotSupportedYet)
		tk.MustGetErrCode("execute st_lock using @p", mysql.ErrNotSupportedYet)
		// Views created earlier under ON remain readable when OFF, while recreating is rejected.
		tk.MustQuery("select id from fr_v1").Check(testkit.Rows("1", "2"))
		tk.MustGetErrCode("create view fr_v2 as select SQL_CALC_FOUND_ROWS id from fr_t limit 2", mysql.ErrNotSupportedYet)

		// View expansion fallback (including nested views) must not poison prepared ASTs.
		tk.MustExec("create view fr_v_rand as select id from fr_t where rand() < 2")
		tk.MustExec("create view fr_v_rand_nested as select id from fr_v_rand")
		// When alternative logical plans discard round 1 and rebuild in a later round
		// (e.g. fts-like-fallback), LogicalPlanBuildState restoration must also reset
		// IgnoreCalcFoundRows so the winning round re-evaluates view fallback and
		// preserves Warning 1235 in WARN mode.
		tk.MustExec("create table fr_fts(id int primary key, txt varchar(32))")
		tk.MustExec("insert into fr_fts values (1, 'hello world'), (2, 'hello tidb')")
		tk.MustExec("set tidb_opt_enable_alternative_logical_plans=ON")
		altViewSQL := "select SQL_CALC_FOUND_ROWS f.id from fr_fts f join fr_v_rand_nested v on f.id = v.id where match(f.txt) against ('hello') order by f.id limit 1"
		tk.MustGetErrCode(altViewSQL, mysql.ErrNotSupportedYet)
		tk.MustExec("set tidb_enable_noop_functions=WARN")
		tk.MustQuery(altViewSQL).Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(noopWarn)
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustExec("set tidb_enable_noop_functions=ON")
		tk.MustQuery(altViewSQL).Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustExec("prepare st_view from 'select SQL_CALC_FOUND_ROWS id from fr_v_rand_nested order by id limit 1'")
		tk.MustQuery("execute st_view").Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustExec("set tidb_enable_noop_functions=WARN")
		for range 2 {
			tk.MustQuery("execute st_view").Check(testkit.Rows("1"))
			tk.MustQuery("show warnings").Check(noopWarn)
			tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		}
		tk.MustExec("set tidb_enable_noop_functions=OFF")
		tk.MustGetErrCode("execute st_view", mysql.ErrNotSupportedYet)
		// A volatile view referenced only in outer ORDER BY is expanded during the
		// data branch (countSel drops ORDER BY) and must also fall back cleanly.
		orderByViewSQL := "select SQL_CALC_FOUND_ROWS t.id from fr_t t order by (select v.id from fr_v_rand_nested v where v.id = t.id limit 1), t.id limit 1"
		tk.MustGetErrCode(orderByViewSQL, mysql.ErrNotSupportedYet)
		for _, mode := range []string{"ON", "WARN"} {
			tk.MustExec("set tidb_enable_noop_functions=" + mode)
			tk.MustQuery(orderByViewSQL).Check(testkit.Rows("1"))
			if mode == "WARN" {
				tk.MustQuery("show warnings").Check(noopWarn)
			} else {
				tk.MustQuery("show warnings").Check(testkit.Rows())
			}
			tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		}
		// A non-correlated scalar or EXISTS subquery over a side-effecting view
		// must evaluate only once when falling back to noop (and zero times when OFF).
		tk.MustExec("create sequence fr_seq")
		tk.MustExec("create view fr_v_seq as select nextval(fr_seq) as n")
		tk.MustExec("create view fr_v_seq_exists as select 1 as ok from dual where nextval(fr_seq) > 0")
		tk.MustExec("set tidb_enable_noop_functions=OFF")
		tk.MustGetErrCode("select SQL_CALC_FOUND_ROWS (select n from fr_v_seq)", mysql.ErrNotSupportedYet)
		tk.MustGetErrCode("select SQL_CALC_FOUND_ROWS 1 from fr_t where exists (select 1 from fr_v_seq_exists) limit 1", mysql.ErrNotSupportedYet)
		tk.MustExec("set tidb_enable_noop_functions=ON")
		tk.MustQuery("select SQL_CALC_FOUND_ROWS (select n from fr_v_seq)").Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustQuery("select SQL_CALC_FOUND_ROWS 1 from fr_t where exists (select 1 from fr_v_seq_exists) order by id limit 1").Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustExec("set tidb_enable_noop_functions=WARN")
		tk.MustQuery("select SQL_CALC_FOUND_ROWS (select n from fr_v_seq)").Check(testkit.Rows("3"))
		tk.MustQuery("show warnings").Check(noopWarn)
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustQuery("select SQL_CALC_FOUND_ROWS 1 from fr_t where exists (select 1 from fr_v_seq_exists) order by id limit 1").Check(testkit.Rows("1"))
		tk.MustQuery("show warnings").Check(noopWarn)
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustQuery("select nextval(fr_seq)").Check(testkit.Rows("5"))
		// Replacing the view with a repeatable definition must restore native counting
		// in both OFF and ON modes.
		tk.MustExec("create or replace view fr_v_rand as select id from fr_t")
		for _, mode := range []string{"OFF", "ON"} {
			tk.MustExec("set tidb_enable_noop_functions=" + mode)
			tk.MustQuery("execute st_view").Check(testkit.Rows("1"))
			tk.MustQuery("show warnings").Check(testkit.Rows())
			tk.MustQuery("select found_rows()").Check(testkit.Rows("4"))
		}
	})
}

func TestSQLCalcFoundRowsPlanningAndClose(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table found_rows (id int primary key)")
	tk.MustExec("insert into found_rows values (1), (2), (3)")
	sql := "select SQL_CALC_FOUND_ROWS id from found_rows order by id limit 1"
	t.Run("counts across chunks and statements", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("set tidb_init_chunk_size=1, tidb_max_chunk_size=32")
		cte := "with recursive c(n) as (select 1 union all select n+1 from c where n<70) "
		for range 2 {
			for _, tc := range []struct {
				query string
				count string
			}{
				{cte + "select n from c limit 35", "35"},
				{cte + "select SQL_CALC_FOUND_ROWS n from c limit 35", "70"},
				{cte + "select SQL_CALC_FOUND_ROWS n from c limit 0", "70"},
				{"select 1 limit 0", "0"},
				{"select 1", "1"},
				{"explain analyze " + cte + "select SQL_CALC_FOUND_ROWS n from c limit 35", "70"},
			} {
				tk.MustQuery(tc.query)
				tk.MustQuery("select found_rows()").Check(testkit.Rows(tc.count))
			}
		}
	})
	t.Run("explain counts", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		for _, prefix := range []string{"explain ", "describe ", "explain format='brief' "} {
			tk.MustQuery("select SQL_CALC_FOUND_ROWS id from found_rows where id<=2 limit 0")
			tk.MustQuery(prefix + sql)
			tk.MustQuery("select found_rows()").Check(testkit.Rows("2"))
		}
	})
	t.Run("explain analyze count", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustQuery("explain analyze " + sql)
		tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
		tk.MustQuery("explain analyze select SQL_CALC_FOUND_ROWS id from found_rows where id<0")
		tk.MustQuery("select found_rows()").Check(testkit.Rows("0"))
		planRows := tk.MustQuery("explain analyze select id from found_rows").Rows()
		tk.MustQuery("select found_rows()").Check(testkit.Rows(fmt.Sprint(len(planRows))))
	})
	calls := 0
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData", func(sessionctx.Context) {
		calls++
	}))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData"))
	}()
	tk.MustQuery("explain " + sql)
	tk.MustExec("prepare st from '" + sql + "'")
	require.Zero(t, calls, "EXPLAIN and PREPARE must not execute data")
	tk.MustQuery("execute st").Check(testkit.Rows("1"))
	require.Equal(t, 1, calls)
	tk.MustQuery("select found_rows()").Check(testkit.Rows("3"))
	// Closing an unread result must release the data child without counting.
	rs, err := tk.Exec(sql)
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	require.Equal(t, 1, calls)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData"))
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData", func(sessionctx.Context) {
		cancel()
	}))
	rs, err = tk.ExecWithContext(ctx, sql)
	require.NoError(t, err)
	chk := rs.NewChunk(nil)
	require.NoError(t, rs.Next(ctx, chk))
	require.Error(t, rs.Next(ctx, chk))
	require.NoError(t, rs.Close())
	// A later normal statement must not inherit the aborted count.
	tk.MustQuery("select 1")
	tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
}

func TestSQLCalcFoundRowsCTE(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setup := testkit.NewTestKit(t, store)
	setup.MustExec("use test")
	setup.MustExec("create table cte_probe(id int primary key,x int)")
	setup.MustExec("insert into cte_probe values(1,1),(2,1),(3,2),(4,2)")
	cases := []struct {
		name         string
		sql          string
		rows         []string
		total        string
		materialized bool
	}{
		{"inline", "with c as (select id from cte_probe where id<=3) select SQL_CALC_FOUND_ROWS id from c order by id limit 1", []string{"1"}, "3", false},
		{"inline-internal-limit", "with c as (select id from cte_probe order by id limit 2) select SQL_CALC_FOUND_ROWS id from c order by id limit 1", []string{"1"}, "2", false},
		{"nested-definitions", "with c as (select id from cte_probe where id<=3),d as (select id from c where id>=2) select SQL_CALC_FOUND_ROWS id from d order by id limit 1", []string{"2"}, "2", false},
		{"grouped", "with c as (select x,count(*) as n from cte_probe group by x) select SQL_CALC_FOUND_ROWS x from c where n>=2 order by x limit 1", []string{"1"}, "2", false},
		{"materialized-reused", "with c as (select id from cte_probe where id<=3) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 1", []string{"1 2"}, "3", true},
		{"materialized-internal-limit", "with c as (select id from cte_probe order by id limit 3) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 1", []string{"1 2"}, "3", true},
		{"recursive", "with recursive c(n) as (select 1 union all select n+1 from c where n<5) select SQL_CALC_FOUND_ROWS n from c order by n limit 2", []string{"1", "2"}, "5", true},
		{"recursive-internal-limit", "with recursive c(n) as (select 1 union all select n+1 from c limit 3) select SQL_CALC_FOUND_ROWS n from c order by n limit 1", []string{"1"}, "3", true},
		{"materialized-empty-page", "with c as (select id from cte_probe where id<=3) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 20,1", nil, "3", true},
		{"recursive-limit-zero", "with recursive c(n) as (select 1 union all select n+1 from c where n<5) select SQL_CALC_FOUND_ROWS n from c limit 0", nil, "5", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set tidb_opt_force_inline_cte=OFF")
			plain := strings.Replace(tc.sql, "SQL_CALC_FOUND_ROWS ", "", 1)
			tk.MustQuery(plain).Check(testkit.Rows(tc.rows...))
			tk.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
			tk.MustQuery("select found_rows()").Check(testkit.Rows(tc.total))
			if tc.materialized {
				plan := tk.MustQuery("explain " + tc.sql)
				require.Contains(t, fmt.Sprint(plan.Rows()), "CTEFullScan")
			}
		})
	}

	t.Run("release-storage-before-count", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set tidb_opt_force_inline_cte=OFF")
		fp := "github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData"
		calls := 0
		require.NoError(t, failpoint.EnableCall(fp, func(ctx sessionctx.Context) {
			calls++
			require.Zero(t, ctx.GetSessionVars().StmtCtx.MemTracker.BytesConsumed(),
				"data CTE storage must be released before allocating count storage")
		}))
		defer func() { require.NoError(t, failpoint.Disable(fp)) }()
		for _, sql := range []string{
			"with c as (select id from cte_probe) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 1",
			"with recursive c(n) as (select 1 union all select n+1 from c where n<5) select SQL_CALC_FOUND_ROWS n from c order by n limit 1",
		} {
			tk.MustQuery(sql)
		}
		require.Equal(t, 2, calls)
	})

	t.Run("close-and-cancel", func(t *testing.T) {
		for _, sql := range []string{
			"with c as (select id from cte_probe) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 1",
			"with recursive c(n) as (select 1 union all select n+1 from c where n<5) select SQL_CALC_FOUND_ROWS n from c order by n limit 1",
		} {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set tidb_opt_force_inline_cte=OFF")
			rs, err := tk.Exec(sql)
			require.NoError(t, err)
			chk := rs.NewChunk(nil)
			require.NoError(t, rs.Next(context.Background(), chk))
			require.Equal(t, 1, chk.NumRows())
			require.NoError(t, rs.Close())
			require.Zero(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())

			ctx, cancel := context.WithCancel(context.Background())
			fp := "github.com/pingcap/tidb/pkg/executor/afterCalcFoundRowsData"
			require.NoError(t, failpoint.EnableCall(fp, func(sessionctx.Context) { cancel() }))
			func() {
				defer cancel()
				defer func() { require.NoError(t, failpoint.Disable(fp)) }()
				rs, err = tk.ExecWithContext(ctx, sql)
				require.NoError(t, err)
				chk = rs.NewChunk(nil)
				require.NoError(t, rs.Next(ctx, chk))
				require.Equal(t, 1, chk.NumRows())
				require.Error(t, rs.Next(ctx, chk))
				require.NoError(t, rs.Close())
			}()
			require.Zero(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
			// Reusing the same session must rebuild and release both CTE storages.
			tk.MustQuery(sql)
			total := "5"
			if strings.Contains(sql, "join") {
				total = "6"
			}
			tk.MustQuery("select found_rows()").Check(testkit.Rows(total))
		}
	})

	t.Run("count-recursion-error", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set cte_max_recursion_depth=2")
		sql := "with recursive c(n) as (select 1 union all select n+1 from c where n<5) select SQL_CALC_FOUND_ROWS n from c limit 0"
		// LIMIT 0 skips the data branch; only the count can hit the recursion limit.
		err := tk.QueryToErr(sql)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Recursive query aborted")
		require.Zero(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
		tk.MustQuery("select 1")
		tk.MustQuery("select found_rows()").Check(testkit.Rows("1"))
		tk.MustExec("set cte_max_recursion_depth=1000")
		tk.MustQuery(sql).Check(testkit.Rows())
		tk.MustQuery("select found_rows()").Check(testkit.Rows("5"))
	})

	t.Run("spill", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		original := tk.MustQuery("select @@global.tidb_enable_tmp_storage_on_oom").Rows()[0][0].(string)
		tk.MustExec("set global tidb_enable_tmp_storage_on_oom=ON")
		defer tk.MustExec("set global tidb_enable_tmp_storage_on_oom=" + original)
		fp := "github.com/pingcap/tidb/pkg/executor/testCTEStorageSpill"
		require.NoError(t, failpoint.Enable(fp, "return(true)"))
		defer func() { require.NoError(t, failpoint.Disable(fp)) }()
		tk.MustExec("set tidb_mem_quota_query=40000")
		// The synchronization hook waits the producer's initial spill actions.
		// A nonrecursive producer keeps those actions until generation completes;
		// recursive iterations replace them and can cancel queued writes on reopen.
		tk.MustExec("create table cte_spill(id int primary key,payload varchar(2048))")
		values := make([]string, 100)
		for i := range values {
			values[i] = fmt.Sprintf("(%d,'%s')", i+1, strings.Repeat("x", 1024))
		}
		tk.MustExec("insert into cte_spill values " + strings.Join(values, ","))
		cases := []struct {
			name, sql, total string
			rows             []string
		}{
			{"page", "with c as (select id,payload from cte_spill) select SQL_CALC_FOUND_ROWS a.id,a.payload from c a join c b on a.id=b.id order by a.id limit 1", "100", []string{"1 " + strings.Repeat("x", 1024)}},
			{"count-only", "with c as (select id,payload from cte_spill) select distinct SQL_CALC_FOUND_ROWS a.payload from c a join c b on a.id=b.id limit 0", "1", nil},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				kit := testkit.NewTestKit(t, store)
				kit.MustExec("use test")
				kit.MustExec("set tidb_mem_quota_query=40000")
				for range 2 {
					kit.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
					stmt := kit.Session().GetSessionVars().StmtCtx
					require.Greater(t, stmt.DiskTracker.MaxConsumed(), int64(0))
					require.Zero(t, stmt.DiskTracker.BytesConsumed())
					require.Zero(t, stmt.MemTracker.BytesConsumed())
					kit.MustQuery("select found_rows()").Check(testkit.Rows(tc.total))
				}
			})
		}
	})
}

func TestSQLCalcFoundRowsCTEPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setup := testkit.NewTestKit(t, store)
	setup.MustExec("use test")
	setup.MustExec("create table cte_cache_probe(id int primary key)")
	setup.MustExec("insert into cte_cache_probe values(1),(2),(3),(4)")
	cases := []struct {
		name, sql      string
		rows2, rows3   []string
		total2, total3 string
	}{
		{"inline", "with c as (select id from cte_cache_probe where id>=?) select SQL_CALC_FOUND_ROWS id from c order by id limit 1", []string{"2"}, []string{"3"}, "3", "2"},
		{"materialized", "with c as (select id from cte_cache_probe where id>=?) select SQL_CALC_FOUND_ROWS a.id,b.id from c a join c b on a.id<b.id order by a.id,b.id limit 1", []string{"2 3"}, []string{"3 4"}, "3", "1"},
		{"recursive", "with recursive c(n) as (select 1 union all select n+1 from c where n<?) select SQL_CALC_FOUND_ROWS n from c order by n desc limit 1", []string{"2"}, []string{"3"}, "2", "3"},
	}
	for _, instance := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s/instance=%v", tc.name, instance), func(t *testing.T) {
				ctx := context.Background()
				if instance {
					ctx = context.WithValue(ctx, plannercore.PlanCacheKeyEnableInstancePlanCache{}, true)
				}
				tk := testkit.NewTestKit(t, store)
				tk.MustExecWithContext(ctx, "use test")
				tk.MustExecWithContext(ctx, "set tidb_enable_prepared_plan_cache=ON")
				tk.MustExecWithContext(ctx, "set tidb_enable_plan_cache_for_subquery=ON")
				tk.MustExecWithContext(ctx, "set tidb_opt_force_inline_cte=OFF")
				tk.MustExecWithContext(ctx, "prepare plain from '"+strings.Replace(tc.sql, "SQL_CALC_FOUND_ROWS ", "", 1)+"'")
				tk.MustExecWithContext(ctx, "prepare counted from '"+tc.sql+"'")
				for attempt, value := range []int{2, 2, 3, 3} {
					t.Run(fmt.Sprintf("bind-%d/attempt-%d", value, attempt), func(t *testing.T) {
						if tc.name == "materialized" && !instance && value == 3 {
							t.Skip("materialized CTE parameter rebinding is blocked by #73112, including ordinary SELECT")
						}
						bindTK := testkit.NewTestKitWithSession(t, store, tk.Session())
						bindTK.MustExecWithContext(ctx, fmt.Sprintf("set @v=%d", value))
						rows, total := tc.rows2, tc.total2
						if value == 3 {
							rows, total = tc.rows3, tc.total3
						}
						plainRows := bindTK.MustQueryWithContext(ctx, "execute plain using @v").Rows()
						plainHit := bindTK.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Rows()[0][0]
						countedRows := bindTK.MustQueryWithContext(ctx, "execute counted using @v").Rows()
						actual := bindTK.MustQueryWithContext(ctx, "select found_rows(),@@last_plan_from_cache").Rows()
						require.Equal(t, testkit.Rows(rows...), plainRows)
						require.Equal(t, testkit.Rows(rows...), countedRows)
						require.Equal(t, total, actual[0][0])
						require.Equal(t, plainHit, actual[0][1], "SQL_CALC_FOUND_ROWS must not reduce existing cache eligibility")
					})
				}
			})
		}
	}
}
