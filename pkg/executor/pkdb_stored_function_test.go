// Copyright 2026 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStoredFunctionReturnValueLeakAcrossRows(t *testing.T) {
	// Reproduce the "execution context leak across rows" problem for stored functions.
	//
	// The stored function is evaluated multiple times (per row) within *one* SQL statement.
	// If the procedure execution context (ProcedureCtx) is reused across rows without reset/clone,
	// the 2nd evaluation can incorrectly reuse the return value from the 1st evaluation.
	//
	// The function below only returns on one code path. When p=0, MySQL raises error 1321:
	//   "FUNCTION <db>.<name> ended without RETURN"
	//
	// With the leak bug, the 2nd row (p=0) silently returns the previous return value instead of error.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("create function sf_ret_leak_rows(p int) returns int begin if p = 1 then return 1; end if; end;")
	tk.MustQuery("select sf_ret_leak_rows(1)").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t_sf_ret_leak_rows")
	tk.MustExec("create table t_sf_ret_leak_rows(id int primary key, p int)")
	tk.MustExec("insert into t_sf_ret_leak_rows values (1, 1), (2, 0)")

	query := "select sf_ret_leak_rows(p) from t_sf_ret_leak_rows order by id"
	err := tk.QueryToErr(query)
	if err == nil {
		// On buggy implementation, the 2nd row reuses the return value from the 1st row and the query succeeds.
		rows := tk.MustQuery(query).Rows()
		t.Fatalf("expected \"ended without RETURN\" error on 2nd row, but query succeeded with rows %v", rows)
	}
	require.Contains(t, err.Error(), "ended without RETURN")
}

func TestStoredFunctionUsesStoredSQLMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop function if exists sf_sql_mode")
	tk.MustExec("create function sf_sql_mode(s char(20)) returns char(50) return concat('hello, ', s, '!')")
	tk.MustQuery("select sf_sql_mode('world')").Check(testkit.Rows("hello, world!"))
}

func TestStoredFunctionDerivedTablePredicateDoesNotPanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop function if exists instr_n")
	tk.MustExec("drop table if exists test1")
	tk.MustExec(`create function instr_n(str varchar(1000), sub varchar(255), start_pos int, occurrence int)
returns int
deterministic
begin
	declare pos int default 0;
	declare i int default 1;
	declare search_start int default start_pos;
	if occurrence <= 0 then
		return 0;
	end if;
	while i <= occurrence do
		set pos = locate(sub, str, search_start);
		if pos = 0 then
			return 0;
		end if;
		set search_start = pos + length(sub);
		set i = i + 1;
	end while;
	return pos;
end`)
	tk.MustExec("create table test1(name varchar(32))")

	direct := "select name from test1 where instr_n(name, 'FX', 1, 1) > 0 order by name"
	derived := "select name from (select name from test1 where instr_n(name, 'FX', 1, 1) > 0) t order by name"

	tk.MustQuery(direct).Check(testkit.Rows())
	tk.MustQuery(derived).Check(testkit.Rows())

	tk.MustExec("insert into test1 values ('AFX')")
	tk.MustQuery(direct).Check(testkit.Rows("AFX"))
	tk.MustQuery(derived).Check(testkit.Rows("AFX"))
}

func TestStoredFunctionZeroWidthDecimalZerofillUsesDefaultPrecision(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop function if exists sf_decimal_zerofill")
	tk.MustExec(`create function sf_decimal_zerofill(f1 decimal(0) unsigned zerofill)
returns decimal(0) unsigned zerofill
begin
	set f1 = (f1 / 2);
	set f1 = (f1 * 2);
	set f1 = (f1 - 10);
	set f1 = (f1 + 10);
	return f1;
end`)

	tk.MustQuery("select sf_decimal_zerofill(999999999)").Check(testkit.Rows("1000000000"))
}

func TestStoredFunctionDateReturnCanBePassedToProcedureArgument(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_sf_date_arg")
	tk.MustExec("create table t_sf_date_arg(id int primary key, hire_date date)")
	tk.MustExec("insert into t_sf_date_arg values (101, '2020-01-01')")

	tk.MustExec("drop function if exists sf_to_date")
	tk.MustExec(`create function sf_to_date(date_str varchar(20), format_str varchar(20))
returns date
begin
	set format_str = replace(replace(replace(format_str, 'YYYY', '%Y'), 'MM', '%m'), 'DD', '%d');
	return str_to_date(date_str, format_str);
end`)
	tk.MustExec("drop function if exists sf_date_literal")
	tk.MustExec(`create function sf_date_literal()
returns date
begin
	return str_to_date('2024-01-01', '%Y-%m-%d');
end`)

	tk.MustExec("drop procedure if exists sp_update_hire_date")
	tk.MustExec(`create procedure sp_update_hire_date(in emp_id int, in new_hire_date date)
begin
	update t_sf_date_arg set hire_date = new_hire_date where id = emp_id;
end`)

	tk.MustExec("drop procedure if exists sp_capture_hire_date")
	tk.MustExec(`create procedure sp_capture_hire_date(in new_hire_date date)
begin
	set @captured_hire_date = new_hire_date;
end`)

	tk.MustQuery("select sf_to_date('2024-01-01', 'YYYY-MM-DD')").Check(testkit.Rows("2024-01-01"))
	tk.MustQuery("select (sf_to_date('2024-01-01', 'YYYY-MM-DD'))").Check(testkit.Rows("2024-01-01"))
	d, err := plannercore.GetExprValue(context.Background(), plannercore.NewCacheExpr(true, "sf_to_date('2024-01-01', 'YYYY-MM-DD')", nil), types.NewFieldType(mysql.TypeDate), tk.Session(), "", nil)
	require.NoError(t, err)
	require.Equal(t, "2024-01-01", d.GetMysqlTime().String())
	tk.MustExec("set @captured_hire_date = null")
	tk.MustExec("call sp_capture_hire_date('2024-01-01')")
	tk.MustQuery("select @captured_hire_date").Check(testkit.Rows("2024-01-01"))
	tk.MustExec("set @captured_hire_date = null")
	tk.MustExec("call sp_capture_hire_date(sf_date_literal())")
	tk.MustQuery("select @captured_hire_date").Check(testkit.Rows("2024-01-01"))
	tk.MustExec("set @captured_hire_date = null")
	tk.MustExec("call sp_capture_hire_date(sf_to_date('2024-01-01', 'YYYY-MM-DD'))")
	tk.MustQuery("select @captured_hire_date").Check(testkit.Rows("2024-01-01"))
	tk.MustExec("call sp_update_hire_date(101, sf_to_date('2024-01-01', 'YYYY-MM-DD'))")
	tk.MustQuery("select hire_date from t_sf_date_arg where id = 101").Check(testkit.Rows("2024-01-01"))
}

func TestStoredFunctionDecimalReturnCanPopulateProcedureVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_sf_decimal_ctx")
	tk.MustExec("create table t_sf_decimal_ctx(id int primary key, department_id int, salary decimal(8,2))")
	tk.MustExec("insert into t_sf_decimal_ctx values (101, 60, 5000.00), (102, 60, 6000.00), (104, 60, 5500.00), (105, 60, 4500.00)")

	tk.MustExec("drop function if exists fn_get_department_avg_salary")
	tk.MustExec(`create function fn_get_department_avg_salary(dept_id int)
returns decimal(8,2)
begin
	declare avg_salary decimal(8,2);
	select avg(salary) into avg_salary from t_sf_decimal_ctx where department_id = dept_id;
	return avg_salary;
end`)

	tk.MustExec("drop procedure if exists sp_adjust_salary_by_department")
	tk.MustExec(`create procedure sp_adjust_salary_by_department(in dept_id int)
begin
	declare avg_sal decimal(8,2);
	set @captured_direct_avg = fn_get_department_avg_salary(dept_id);
	set avg_sal = fn_get_department_avg_salary(dept_id);
	set @captured_avg_sal = avg_sal;
	update t_sf_decimal_ctx set salary = salary * 1.1 where department_id = dept_id and salary < avg_sal;
end`)

	tk.MustExec("set @captured_avg_sal = null")
	tk.MustExec("set @captured_direct_avg = null")
	tk.MustExec("call sp_adjust_salary_by_department(60)")
	tk.MustQuery("select @captured_direct_avg").Check(testkit.Rows("5250.00"))
	tk.MustQuery("select @captured_avg_sal").Check(testkit.Rows("5250.00"))
	tk.MustQuery("select id, salary from t_sf_decimal_ctx order by id").Check(testkit.Rows(
		"101 5500.00",
		"102 6000.00",
		"104 5500.00",
		"105 4950.00",
	))
}

func TestStoredFunctionSubqueryAssignmentKeepsParentContext(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop function if exists sf_ctx_inner")
	tk.MustExec("drop function if exists sf_ctx_outer_direct")
	tk.MustExec("drop function if exists sf_ctx_outer_subquery")

	tk.MustExec(`create function sf_ctx_inner(p int) returns int
begin
	return p + 1;
end`)
	tk.MustExec(`create function sf_ctx_outer_direct(p int) returns int
begin
	declare v int default 0;
	set v = sf_ctx_inner(p);
	return v;
end`)
	tk.MustExec(`create function sf_ctx_outer_subquery(p int) returns int
begin
	declare v int default 0;
	set v = (select sf_ctx_inner(p));
	return v;
end`)

	tk.MustQuery("select sf_ctx_outer_direct(1)").Check(testkit.Rows("2"))
	tk.MustQuery("select sf_ctx_outer_subquery(1)").Check(testkit.Rows("2"))
}

func TestStoredFunctionOnlyFullGroupByIgnoresRoutineVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop function if exists sf_only_full_group_by_vars")
	tk.MustExec("drop table if exists t_sf_only_full_group_by_vars")
	tk.MustExec("create table t_sf_only_full_group_by_vars(a int)")
	tk.MustExec("insert into t_sf_only_full_group_by_vars values (1), (2), (1)")

	sqlMode := "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
	tk.MustExec("set @@session.sql_mode = '" + sqlMode + "'")
	tk.MustExec(`create function sf_only_full_group_by_vars(idv int, upperv int, limv int, divv int)
returns decimal(20,4)
begin
	declare vrq int;
	set vrq = upperv - 1;
	return (
		select sum(a.a) / divv
		from t_sf_only_full_group_by_vars a,
			 (select b.a as rqv
			  from t_sf_only_full_group_by_vars b
			  where b.a <= vrq
			  order by b.a desc
			  limit limv) c
		where a.a = idv
		  and a.a = c.rqv
	);
end`)

	tk.MustQuery("select sf_only_full_group_by_vars(1, 2, 1, 1)").Check(testkit.Rows("2.0000"))
}

func TestStoredFunctionOnlyFullGroupByTreatsRoutineVarsAsSingleValuePredicates(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop function if exists sf_only_full_group_by_single_value")
	tk.MustExec("drop table if exists t_sf_only_full_group_by_single_value")
	tk.MustExec("create table t_sf_only_full_group_by_single_value(a int)")
	tk.MustExec("insert into t_sf_only_full_group_by_single_value values (1), (2), (1)")

	sqlMode := "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
	tk.MustExec("set @@session.sql_mode = '" + sqlMode + "'")
	tk.MustExec(`create function sf_only_full_group_by_single_value(idv int)
returns int
begin
	return (
		select a + count(*)
		from t_sf_only_full_group_by_single_value
		where a = idv
	);
end`)

	tk.MustQuery("select sf_only_full_group_by_single_value(1)").Check(testkit.Rows("3"))
}

func TestStoredFunctionProjectionDoesNotRunInParallel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	// Before the fix, this shape could drive stored-function evaluation from parallel
	// projection workers and race on shared session-scoped routine / transaction state.
	tk.Session().GetSessionVars().MaxChunkSize = 1
	tk.MustExec("set @@tidb_executor_concurrency = 20")

	tk.MustExec("drop function if exists sf_projection_serial")
	tk.MustExec("drop table if exists t_sf_projection_serial")
	tk.MustExec("create table t_sf_projection_serial(id int primary key)")
	tk.MustExec("insert into t_sf_projection_serial values (1), (2), (3), (4), (5), (6), (7), (8)")
	tk.MustExec(`create function sf_projection_serial(p int) returns int
begin
	return p + coalesce((select 0 from dual where sleep(0.01)=0), 0);
end`)

	tk.MustQuery("select /*+ HASH_JOIN(a,b) */ sf_projection_serial(a.id) as v from t_sf_projection_serial a join t_sf_projection_serial b on a.id = b.id order by v").
		Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8"))
}

func TestViewQueryWithStoredFunction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop view if exists v_sf_view_query")
	tk.MustExec("drop function if exists sf_view_query")
	tk.MustExec("drop table if exists t_sf_view_query")

	tk.MustExec("create table t_sf_view_query(id int primary key, f2 varchar(100))")
	tk.MustExec("insert into t_sf_view_query values (1, 'abc')")
	tk.MustExec(`create function sf_view_query(prm_spell varchar(100)) returns varchar(100)
begin
	return concat(prm_spell, '123');
end`)
	tk.MustExec("create view v_sf_view_query as select id, sf_view_query(f2) as newf2 from t_sf_view_query")

	tk.MustQuery("select * from v_sf_view_query").Check(testkit.Rows("1 abc123"))

	tk.MustExec("drop function sf_view_query")
	tk.MustGetErrCode("select * from v_sf_view_query", mysql.ErrViewInvalid)
}

func TestCrossDatabaseViewQueryWithStoredFunction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop view if exists v_sf_view_cross_db")
	tk.MustExec("drop function if exists sf_view_cross_db")
	tk.MustExec("drop table if exists t_sf_view_cross_db")

	tk.MustExec("create table t_sf_view_cross_db(id int primary key, f2 varchar(100))")
	tk.MustExec("insert into t_sf_view_cross_db values (1, 'abc')")
	tk.MustExec(`create function sf_view_cross_db(prm_spell varchar(100)) returns varchar(100)
begin
	return concat(prm_spell, '123');
end`)
	tk.MustExec("create view v_sf_view_cross_db as select id, sf_view_cross_db(f2) as newf2 from t_sf_view_cross_db")

	tk.MustExec("use mysql")
	tk.MustQuery("select * from test.v_sf_view_cross_db").Check(testkit.Rows("1 abc123"))
}

func TestShowFieldsFromViewWithStoredFunction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop view if exists v_sf_view_show_fields")
	tk.MustExec("drop function if exists sf_view_show_fields")
	tk.MustExec("drop table if exists t_sf_view_show_fields")

	tk.MustExec("create table t_sf_view_show_fields(id int primary key, f2 varchar(100))")
	tk.MustExec("insert into t_sf_view_show_fields values (1, 'abc')")
	tk.MustExec(`create function sf_view_show_fields(prm_spell varchar(100)) returns varchar(100)
begin
	return concat(prm_spell, '123');
end`)
	tk.MustExec("create view v_sf_view_show_fields as select id, sf_view_show_fields(f2) as newf2 from t_sf_view_show_fields")

	rows := tk.MustQuery("show fields from v_sf_view_show_fields").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "id", rows[0][0])
	require.Equal(t, "newf2", rows[1][0])
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestInformationSchemaColumnsFromViewWithStoredFunction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("use test")

	tk.MustExec("drop view if exists v_sf_view_info_schema")
	tk.MustExec("drop function if exists sf_view_info_schema")
	tk.MustExec("drop table if exists t_sf_view_info_schema")

	tk.MustExec("create table t_sf_view_info_schema(id int primary key, f2 varchar(100))")
	tk.MustExec("insert into t_sf_view_info_schema values (1, 'abc')")
	tk.MustExec(`create function sf_view_info_schema(prm_spell varchar(100)) returns varchar(100)
begin
	return concat(prm_spell, '123');
end`)
	tk.MustExec("create view v_sf_view_info_schema as select id, sf_view_info_schema(f2) as newf2 from t_sf_view_info_schema")

	tk.MustQuery(`select column_name from information_schema.columns
where table_schema = 'test' and table_name = 'v_sf_view_info_schema'
order by ordinal_position`).Check(testkit.Rows("id", "newf2"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}
