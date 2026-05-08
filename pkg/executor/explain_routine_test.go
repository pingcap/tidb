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
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func setupExplainRoutineTestKit(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_procedure = ON")
	tk.MustExec("create table t_explain_routine (a int, b int, key idx_a(a))")
	tk.MustExec("insert into t_explain_routine values (1, 10), (2, 20), (3, 30)")
	return tk
}

func mustQueryColumns(t *testing.T, tk *testkit.TestKit, sql string) []string {
	rs, err := tk.Exec(sql)
	require.NoError(t, err)
	require.NotNil(t, rs)
	defer func() { require.NoError(t, rs.Close()) }()

	fields := rs.Fields()
	cols := make([]string, len(fields))
	for i, field := range fields {
		cols[i] = field.Column.Name.O
	}
	return cols
}

func rowsToStrings(rows [][]any) [][]string {
	result := make([][]string, len(rows))
	for i, row := range rows {
		result[i] = make([]string, len(row))
		for j, col := range row {
			result[i][j] = col.(string)
		}
	}
	return result
}

func requireAnyRow(t *testing.T, rows [][]string, match func([]string) bool) {
	t.Helper()
	for _, row := range rows {
		if match(row) {
			return
		}
	}
	require.Failf(t, "matching row not found", "rows=%v", rows)
}

func isPositiveOrdinal(value string) bool {
	n, err := strconv.Atoi(value)
	return err == nil && n > 0
}

func TestExplainAnalyzeRoutineSummaryFormatValidation(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create function f_explain_routine(id int) returns int
begin
	declare v int default 0;
	select b into v from t_explain_routine where a = id;
	return v;
end`)

	tk.MustGetErrMsg(
		"explain analyze format='brief' routine function f_explain_routine(1)",
		"explain analyze routine summary only supports format 'row'",
	)
	tk.MustGetErrMsg(
		"explain format='dot' routine function f_explain_routine(1)",
		"explain routine format 'dot' is not supported now",
	)
}

func TestExplainAnalyzeRoutineSummaryColumns(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create function f_explain_routine_analyze_summary(id int) returns int
begin
	declare v int default 0;
	select b into v from t_explain_routine where a = id;
	return v;
end`)

	cols := mustQueryColumns(t, tk, "explain analyze routine function f_explain_routine_analyze_summary(1)")
	require.Equal(t, []string{
		"ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "STMT_ORDINAL", "BLOCK_PATH", "STMT_KIND", "SQL_TEXT", "EXPLAINABLE", "NOTE",
		"RUNTIME_SQL_TEXT", "EXEC_COUNT", "TOTAL_TIME", "AVG_TIME", "ROWS_PRODUCED", "PLAN_VARIANTS",
	}, cols)
}

func TestExplainAnalyzeRoutineDrilldownColumns(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create function f_explain_routine_analyze_detail(id int) returns int
begin
	declare v int default 0;
	select b into v from t_explain_routine where a = id;
	return v;
end`)

	cols := mustQueryColumns(t, tk, "explain analyze routine function f_explain_routine_analyze_detail(1) for stmt 1")
	require.Equal(t, []string{
		"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk",
	}, cols)
}

func TestExplainRoutineFunctionSelectIntoIsExplainable(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create function f_explain_routine(id int) returns int
begin
	declare v int default 0;
	select b into v from t_explain_routine where a = id;
	return v;
end`)

	cols := mustQueryColumns(t, tk, "explain routine function f_explain_routine(1)")
	require.Equal(t, []string{
		"ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "STMT_ORDINAL", "BLOCK_PATH", "STMT_KIND", "SQL_TEXT", "EXPLAINABLE", "NOTE",
		"id", "estRows", "task", "access object", "operator info",
	}, cols)

	rows := rowsToStrings(tk.MustQuery("explain routine function f_explain_routine(1)").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		return len(row) == 14 &&
			row[0] == "test" &&
			row[1] == "f_explain_routine" &&
			row[2] == "FUNCTION" &&
			isPositiveOrdinal(row[3]) &&
			row[4] == "root" &&
			row[5] == "select" &&
			strings.Contains(strings.ToLower(row[6]), "select b into v from t_explain_routine") &&
			row[7] == "YES" &&
			row[8] == "" &&
			row[9] != ""
	})
}

func TestExplainRoutineListsStaticSQLWithBlockPath(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_blocks(id int)
begin
	if id > 1 then
		select * from t_explain_routine where a = id;
	else
		update t_explain_routine set b = b + 1 where a = id;
	end if;
	while id > 0 do
		select * from t_explain_routine where a = id;
		set id = id - 1;
	end while;
end`)

	rows := rowsToStrings(tk.MustQuery("explain routine procedure p_explain_routine_blocks(2)").Rows())
	require.NotEmpty(t, rows)

	requireAnyRow(t, rows, func(row []string) bool {
		return row[0] == "test" && row[1] == "p_explain_routine_blocks" && row[2] == "PROCEDURE" &&
			isPositiveOrdinal(row[3]) && row[4] == "root/if#1/then" && row[5] == "select" &&
			strings.Contains(strings.ToLower(row[6]), "select * from t_explain_routine") && row[7] == "YES" && row[8] == ""
	})
	requireAnyRow(t, rows, func(row []string) bool {
		return isPositiveOrdinal(row[3]) && row[4] == "root/if#1/else" && row[5] == "update" &&
			strings.Contains(strings.ToLower(row[6]), "update t_explain_routine") && row[7] == "YES"
	})
	requireAnyRow(t, rows, func(row []string) bool {
		return isPositiveOrdinal(row[3]) && row[4] == "root/while#1/body" && row[5] == "select" &&
			strings.Contains(strings.ToLower(row[6]), "select * from t_explain_routine") && row[7] == "YES"
	})
}

func TestExplainRoutineUsesEntryContextForNestedBlockLocalVars(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_nested_block_local()
begin
	begin
		declare inner_id int default 1;
		select * from t_explain_routine where a = inner_id;
	end;
end`)

	rows := rowsToStrings(tk.MustQuery("explain routine procedure p_explain_routine_nested_block_local()").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root/block#1/body" && row[5] == "select" &&
			strings.Contains(strings.ToLower(row[6]), "inner_id") &&
			row[7] == "YES" && row[8] == "" && row[9] != ""
	})
}

func TestExplainRoutineCoversCursorDeclarationSQL(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_cursor(id int)
begin
	declare c cursor for select * from t_explain_routine where a = id;
	if id > 0 then
		open c;
	end if;
	close c;
end`)

	rows := rowsToStrings(tk.MustQuery("explain routine procedure p_explain_routine_cursor(1)").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		return row[0] == "test" && row[1] == "p_explain_routine_cursor" && row[2] == "PROCEDURE" &&
			isPositiveOrdinal(row[3]) && row[4] == "root/if#1/then" && row[5] == "select" &&
			strings.Contains(strings.ToLower(row[6]), "select * from t_explain_routine where a = id") &&
			row[7] == "YES" && row[8] == ""
	})

	jsonRows := rowsToStrings(tk.MustQuery("explain format='tidb_json' routine procedure p_explain_routine_cursor(1)").Rows())
	require.Len(t, jsonRows, 1)
	require.True(t, isPositiveOrdinal(jsonRows[0][3]))
	require.Equal(t, "root/if#1/then", jsonRows[0][4])
	require.Equal(t, "select", jsonRows[0][5])
	require.Equal(t, "YES", jsonRows[0][7])
	require.Empty(t, jsonRows[0][8])
	require.True(t, json.Valid([]byte(jsonRows[0][9])), "invalid plan json: %s", jsonRows[0][9])
}

func TestExplainRoutineCoversReturnExprSubquery(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create function f_explain_routine_return_subquery(id int) returns int
begin
	if id > 0 then
		return (select b from t_explain_routine where a = id);
	end if;
	return 0;
end`)

	rows := rowsToStrings(tk.MustQuery("explain routine function f_explain_routine_return_subquery(1)").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		sqlText := strings.ToLower(row[6])
		return row[0] == "test" && row[1] == "f_explain_routine_return_subquery" && row[2] == "FUNCTION" &&
			isPositiveOrdinal(row[3]) && row[4] == "root/if#1/then" && row[5] == "return" &&
			strings.Contains(sqlText, "return (select") &&
			strings.Contains(sqlText, "from `t_explain_routine`") &&
			row[7] == "YES" && row[8] == "" && row[9] != ""
	})
	for _, row := range rows {
		require.NotContains(t, strings.ToLower(row[6]), "return 0", "constant RETURN should not be cataloged: %v", row)
	}

	jsonRows := rowsToStrings(tk.MustQuery("explain format='tidb_json' routine function f_explain_routine_return_subquery(1)").Rows())
	require.Len(t, jsonRows, 1)
	require.True(t, isPositiveOrdinal(jsonRows[0][3]))
	require.Equal(t, "root/if#1/then", jsonRows[0][4])
	require.Equal(t, "return", jsonRows[0][5])
	require.Equal(t, "YES", jsonRows[0][7])
	require.Empty(t, jsonRows[0][8])
	require.True(t, json.Valid([]byte(jsonRows[0][9])), "invalid plan json: %s", jsonRows[0][9])
}

func TestExplainAnalyzeRoutineFunctionReturnSubquery(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create function f_explain_routine_analyze_return_subquery(id int) returns int
begin
	return (select b from t_explain_routine where a = id);
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine function f_explain_routine_analyze_return_subquery(1)").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		sqlText := strings.ToLower(row[6])
		return row[0] == "test" && row[1] == "f_explain_routine_analyze_return_subquery" &&
			row[2] == "FUNCTION" && isPositiveOrdinal(row[3]) && row[4] == "root" && row[5] == "return" &&
			strings.Contains(sqlText, "return (select") && strings.Contains(sqlText, "from `t_explain_routine`") &&
			row[7] == "YES" && row[8] == "" && row[10] == "1" && row[13] == "1" && row[14] == "1"
	})
}

func TestExplainRoutineCoversDefaultAndConditionExprSubqueries(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_expr_sites(id int)
begin
	declare v int default (select count(*) from t_explain_routine where a = id);
	if (select count(*) from t_explain_routine where a = id) > 0 then
		select v;
	end if;
	while (select count(*) from t_explain_routine where a = id) > 1 do
		set id = id - 1;
	end while;
end`)

	rows := rowsToStrings(tk.MustQuery("explain routine procedure p_explain_routine_expr_sites(1)").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root" && row[5] == "declare_default" &&
			strings.Contains(strings.ToLower(row[6]), "select count(") &&
			strings.Contains(strings.ToLower(row[6]), "from `t_explain_routine`") &&
			row[7] == "YES"
	})
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root/if#1" && row[5] == "condition" &&
			strings.Contains(strings.ToLower(row[6]), "select count(") &&
			strings.Contains(strings.ToLower(row[6]), "from `t_explain_routine`") &&
			row[7] == "YES"
	})
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root/while#1" && row[5] == "condition" &&
			strings.Contains(strings.ToLower(row[6]), "select count(") &&
			strings.Contains(strings.ToLower(row[6]), "from `t_explain_routine`") &&
			row[7] == "YES"
	})
}

func TestExplainRoutineCoversSetSubqueries(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_set_subqueries(id int)
begin
	declare v int default 0;
	set v = (select b from t_explain_routine where a = id);
	set @explain_routine_set_count = (select count(*) from t_explain_routine where a <= id);
	set v = id + 1;
	select v;
end`)

	rows := rowsToStrings(tk.MustQuery("explain routine procedure p_explain_routine_set_subqueries(1)").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		sqlText := strings.ToLower(row[6])
		return row[4] == "root" && row[5] == "set" &&
			strings.Contains(sqlText, "set v") &&
			strings.Contains(sqlText, "select b") &&
			strings.Contains(sqlText, "t_explain_routine") &&
			row[7] == "YES" && row[8] == "" &&
			strings.Contains(strings.ToLower(strings.Join(row[9:], " ")), "t_explain_routine")
	})
	requireAnyRow(t, rows, func(row []string) bool {
		sqlText := strings.ToLower(row[6])
		return row[4] == "root" && row[5] == "set" &&
			strings.Contains(sqlText, "@explain_routine_set_count") &&
			strings.Contains(sqlText, "select count(") &&
			row[7] == "YES" && row[8] == "" &&
			strings.Contains(strings.ToLower(strings.Join(row[9:], " ")), "t_explain_routine")
	})
	for _, row := range rows {
		require.False(t,
			row[5] == "set" && strings.Contains(strings.ToLower(row[6]), "set v = id + 1"),
			"constant SET should not be cataloged: %v", row)
	}
}

func TestExplainRoutineShapeOnlyArgs(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_shape(id int, out out_b int)
begin
	select b into out_b from t_explain_routine where a = id;
end`)

	rows := rowsToStrings(tk.MustQuery("explain routine procedure p_explain_routine_shape").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		return row[3] == "1" && row[4] == "root" && row[5] == "select" &&
			strings.Contains(strings.ToLower(row[6]), "select b into out_b from t_explain_routine") && row[7] == "YES"
	})
}

func TestExplainRoutineSupportsVerboseAndTiDBJSONFormats(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_formats(id int)
begin
	select * from t_explain_routine where a = id;
	update t_explain_routine set b = b + 1 where a = id;
end`)

	briefCols := mustQueryColumns(t, tk, "explain format='brief' routine procedure p_explain_routine_formats(1)")
	require.Equal(t, []string{
		"ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "STMT_ORDINAL", "BLOCK_PATH", "STMT_KIND", "SQL_TEXT", "EXPLAINABLE", "NOTE",
		"id", "estRows", "task", "access object", "operator info",
	}, briefCols)

	verboseCols := mustQueryColumns(t, tk, "explain format='verbose' routine procedure p_explain_routine_formats(1)")
	require.Equal(t, []string{
		"ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "STMT_ORDINAL", "BLOCK_PATH", "STMT_KIND", "SQL_TEXT", "EXPLAINABLE", "NOTE",
		"id", "estRows", "estCost", "task", "access object", "operator info",
	}, verboseCols)

	jsonCols := mustQueryColumns(t, tk, "explain format='tidb_json' routine procedure p_explain_routine_formats(1)")
	require.Equal(t, []string{
		"ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "STMT_ORDINAL", "BLOCK_PATH", "STMT_KIND", "SQL_TEXT", "EXPLAINABLE", "NOTE", "PLAN_JSON",
	}, jsonCols)

	jsonRows := rowsToStrings(tk.MustQuery("explain format='tidb_json' routine procedure p_explain_routine_formats(1)").Rows())
	require.Len(t, jsonRows, 2)
	for _, row := range jsonRows {
		require.Len(t, row, 10)
		require.Equal(t, "YES", row[7])
		require.Empty(t, row[8])
		require.True(t, json.Valid([]byte(row[9])), "invalid plan json: %s", row[9])
	}
}

func TestExplainRoutineDynamicSQLPlaceholder(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_dynamic()
begin
	set @sql = 'select * from t_explain_routine where a = 1';
	prepare stmt1 from @sql;
	execute stmt1;
	deallocate prepare stmt1;
	select * from t_explain_routine where a = 1;
end`)

	rows := rowsToStrings(tk.MustQuery("explain routine procedure p_explain_routine_dynamic()").Rows())
	require.NotEmpty(t, rows)

	placeholderCount := 0
	for _, row := range rows {
		if row[5] == "dynamic_sql" {
			placeholderCount++
			require.Equal(t, "NO", row[7])
			require.Equal(t, "dynamic SQL is not explainable in v1", row[8])
		}
	}
	require.Equal(t, 2, placeholderCount)
	requireAnyRow(t, rows, func(row []string) bool {
		return row[5] == "select" && row[7] == "YES" && strings.Contains(strings.ToLower(row[6]), "select * from t_explain_routine where a = 1")
	})

	jsonRows := rowsToStrings(tk.MustQuery("explain format='tidb_json' routine procedure p_explain_routine_dynamic()").Rows())
	requireAnyRow(t, jsonRows, func(row []string) bool {
		return row[5] == "dynamic_sql" && row[7] == "NO" && row[8] == "dynamic SQL is not explainable in v1" && row[9] == "<nil>"
	})
}

func TestExplainRoutineCompileFailureDoesNotAbort(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec("create table t_explain_routine_drop (a int)")
	tk.MustExec(`create procedure p_explain_routine_compile_fail()
begin
	select * from t_explain_routine_drop;
	select * from t_explain_routine where a = 1;
end`)
	tk.MustExec("drop table t_explain_routine_drop")

	rows := rowsToStrings(tk.MustQuery("explain routine procedure p_explain_routine_compile_fail()").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		return row[3] == "1" && row[7] == "NO" && strings.HasPrefix(row[8], "compile failed:")
	})
	requireAnyRow(t, rows, func(row []string) bool {
		return row[3] == "2" && row[5] == "select" && row[7] == "YES" && row[9] != ""
	})
}

func TestExplainAnalyzeRoutineAllowsWriteStatements(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_write(id int)
begin
	update t_explain_routine set b = b + 1 where a = id;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_write(1)").Rows())
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root" && row[5] == "update" && row[7] == "YES" &&
			row[10] == "1" && row[13] == "0" && row[14] == "1"
	})
	tk.MustQuery("select b from t_explain_routine where a = 1").Check(testkit.Rows("11"))
}

func TestExplainAnalyzeRoutineSummaryRows(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_analyze_summary(id int)
begin
	if id > 10 then
		select * from t_explain_routine where a = id;
	end if;
	select * from t_explain_routine where a = id;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_summary(1)").Rows())
	require.Len(t, rows, 2)
	requireAnyRow(t, rows, func(row []string) bool {
		return row[3] != "" && row[4] == "root/if#1/then" && row[5] == "select" &&
			row[7] == "YES" && row[9] == "" && row[10] == "0" && row[11] == "" && row[12] == "" &&
			row[13] == "0" && row[14] == "0"
	})
	requireAnyRow(t, rows, func(row []string) bool {
		return row[3] != "" && row[4] == "root" && row[5] == "select" &&
			row[7] == "YES" && row[9] == "" && row[10] == "1" && row[11] != "" && row[12] != "" &&
			row[13] == "1" && row[14] == "1"
	})
}

func TestExplainAnalyzeRoutineDrainsUnconsumedResultRows(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec("create table t_explain_routine_many (a int primary key)")

	const rowCount = 1100
	var insertSQL strings.Builder
	insertSQL.WriteString("insert into t_explain_routine_many values ")
	for i := 1; i <= rowCount; i++ {
		if i > 1 {
			insertSQL.WriteString(",")
		}
		insertSQL.WriteString("(")
		insertSQL.WriteString(strconv.Itoa(i))
		insertSQL.WriteString(")")
	}
	tk.MustExec(insertSQL.String())

	tk.MustExec(`create procedure p_explain_routine_analyze_many_rows()
begin
	select * from t_explain_routine_many order by a;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_many_rows()").Rows())
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root" && row[5] == "select" &&
			row[7] == "YES" && row[10] == "1" && row[13] == strconv.Itoa(rowCount) && row[14] == "1"
	})
}

func TestExplainAnalyzeRoutineDrilldownRows(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_analyze_drilldown(id int)
begin
	select * from t_explain_routine where a = id;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_drilldown(1) for stmt 1").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		return len(row) == 9 && strings.Contains(strings.ToLower(strings.Join(row, " ")), "t_explain_routine")
	})
}

func TestExplainAnalyzeRoutineSetSubqueryDrilldownRows(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_analyze_set_subquery(id int)
begin
	declare v int default 0;
	set v = (select b from t_explain_routine where a = id);
	select v;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_set_subquery(1)").Rows())
	targetOrdinal := ""
	requireAnyRow(t, rows, func(row []string) bool {
		if row[5] != "set" || !strings.Contains(strings.ToLower(row[6]), "select b") {
			return false
		}
		targetOrdinal = row[3]
		return row[4] == "root" && row[7] == "YES" && row[8] == "" &&
			row[10] == "1" && row[13] == "0"
	})
	require.NotEmpty(t, targetOrdinal)

	drilldownRows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_set_subquery(1) for stmt " + targetOrdinal).Rows())
	require.NotEmpty(t, drilldownRows)
	requireAnyRow(t, drilldownRows, func(row []string) bool {
		return len(row) == 9 && strings.Contains(strings.ToLower(strings.Join(row, " ")), "t_explain_routine")
	})
}

func TestExplainAnalyzeRoutineDrilldownRejectsNotExecutedTarget(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_analyze_not_executed(id int)
begin
	if id > 10 then
		select * from t_explain_routine where a = id;
	end if;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_not_executed(1)").Rows())
	targetOrdinal := ""
	for _, row := range rows {
		if row[4] == "root/if#1/then" && row[5] == "select" {
			targetOrdinal = row[3]
			break
		}
	}
	require.NotEmpty(t, targetOrdinal)

	err := tk.QueryToErr("explain analyze routine procedure p_explain_routine_analyze_not_executed(1) for stmt " + targetOrdinal)
	require.EqualError(t, err, "explain analyze routine STMT_ORDINAL "+targetOrdinal+" was not executed")
}

func TestExplainAnalyzeRoutineDrilldownRejectsMultiplePlanVariants(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_analyze_plan_variants()
begin
	set @i = 0;
	while @i < 2 do
		if @i = 0 then
			set @sql = 'select * from t_explain_routine where a = 1';
		else
			set @sql = 'select * from t_explain_routine where b > 0';
		end if;
		prepare stmt1 from @sql;
		execute stmt1;
		deallocate prepare stmt1;
		set @i = @i + 1;
	end while;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_plan_variants()").Rows())
	targetOrdinal := ""
	requireAnyRow(t, rows, func(row []string) bool {
		if row[5] == "dynamic_sql" && strings.Contains(strings.ToLower(row[6]), "execute stmt1") {
			targetOrdinal = row[3]
			return row[10] == "2" && row[14] == "2"
		}
		return false
	})
	require.NotEmpty(t, targetOrdinal)

	err := tk.QueryToErr("explain analyze routine procedure p_explain_routine_analyze_plan_variants() for stmt " + targetOrdinal)
	require.EqualError(t, err, "explain analyze routine STMT_ORDINAL "+targetOrdinal+" observed multiple plan variants")
}

func TestExplainAnalyzeRoutineInvalidTargetDoesNotExecuteRoutine(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec("set @seen = 0")
	tk.MustExec(`create procedure p_explain_routine_analyze_invalid_target(id int)
begin
	set @seen = @seen + 1;
	select * from t_explain_routine where a = id;
end`)

	err := tk.QueryToErr("explain analyze routine procedure p_explain_routine_analyze_invalid_target(1) for stmt 99")
	require.EqualError(t, err, "explain analyze routine STMT_ORDINAL 99 not found")
	tk.MustQuery("select @seen").Check(testkit.Rows("0"))
}

func TestExplainAnalyzeRoutineRequiresInvocationArgs(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec("set @seen = 0")
	tk.MustExec(`create procedure p_explain_routine_missing_args(id int)
begin
	set @seen = @seen + 1;
	select * from t_explain_routine where a = id;
end`)

	err := tk.ExecToErr("explain analyze routine procedure p_explain_routine_missing_args")
	require.ErrorContains(t, err, "Incorrect number of arguments for PROCEDURE test.p_explain_routine_missing_args; expected 1, got 0")
	tk.MustQuery("select @seen").Check(testkit.Rows("0"))
}

func TestExplainAnalyzeRoutineAllowsSideEffects(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec("set @seen = 0")
	tk.MustExec(`create procedure p_explain_routine_analyze_write_preflight(id int)
begin
	set @seen = @seen + 1;
	update t_explain_routine set b = b + 1 where a = id;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_write_preflight(1)").Rows())
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root" && row[5] == "update" && row[7] == "YES" && row[10] == "1"
	})
	tk.MustQuery("select @seen").Check(testkit.Rows("1"))
	tk.MustQuery("select b from t_explain_routine where a = 1").Check(testkit.Rows("11"))
}

func TestExplainAnalyzeRoutineDrilldownKeepsTargetStatsIsolated(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_analyze_isolated(id int)
begin
	select * from t_explain_routine where a in (1, 2);
	select * from t_explain_routine where a = id;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_isolated(1) for stmt 2").Rows())
	require.NotEmpty(t, rows)
	requireAnyRow(t, rows, func(row []string) bool {
		return len(row) == 9 &&
			strings.Contains(row[0], "IndexLookUp") &&
			row[2] == "1"
	})
}

func TestExplainAnalyzeRoutineDynamicSQLRuntimeText(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_analyze_dynamic(id int)
begin
	set @sql = concat('select * from t_explain_routine where a = ', id);
	prepare stmt1 from @sql;
	execute stmt1;
	deallocate prepare stmt1;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_dynamic(1)").Rows())
	requireAnyRow(t, rows, func(row []string) bool {
		return row[5] == "dynamic_sql" &&
			strings.Contains(strings.ToLower(row[6]), "prepare stmt1") &&
			strings.Contains(strings.ToLower(row[9]), "select * from t_explain_routine where a = 1") &&
			row[7] == "NO" && row[10] == "1" && row[14] == "0"
	})
	requireAnyRow(t, rows, func(row []string) bool {
		return row[5] == "dynamic_sql" &&
			strings.Contains(strings.ToLower(row[6]), "execute stmt1") &&
			strings.Contains(strings.ToLower(row[9]), "select * from t_explain_routine where a = 1") &&
			row[7] == "YES" && row[8] == "" && row[10] == "1" && row[14] == "1"
	})
}

func TestExplainAnalyzeRoutineDynamicSQLUsing(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_analyze_dynamic_using(id int)
begin
	set @sql = 'select * from t_explain_routine where a = ?';
	set @id = id;
	prepare stmt1 from @sql;
	execute stmt1 using @id;
	deallocate prepare stmt1;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_dynamic_using(1)").Rows())
	targetOrdinal := ""
	requireAnyRow(t, rows, func(row []string) bool {
		if row[5] != "dynamic_sql" || !strings.Contains(strings.ToLower(row[6]), "execute stmt1 using @id") {
			return false
		}
		targetOrdinal = row[3]
		return strings.Contains(strings.ToLower(row[9]), "select * from t_explain_routine where a = ?") &&
			row[7] == "YES" && row[8] == "" && row[10] == "1" && row[13] == "1" && row[14] == "1"
	})
	require.NotEmpty(t, targetOrdinal)

	drilldownRows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_analyze_dynamic_using(1) for stmt " + targetOrdinal).Rows())
	requireAnyRow(t, drilldownRows, func(row []string) bool {
		return len(row) == 9 && strings.Contains(strings.ToLower(strings.Join(row, " ")), "t_explain_routine") && row[2] == "1"
	})
}

func TestExplainAnalyzeRoutineNestedReadOnlyCall(t *testing.T) {
	tk := setupExplainRoutineTestKit(t)
	tk.MustExec(`create procedure p_explain_routine_nested_helper(id int)
begin
	declare v int default 0;
	select b into v from t_explain_routine where a = id;
end`)
	tk.MustExec(`create procedure p_explain_routine_nested_parent(id int)
begin
	call p_explain_routine_nested_helper(id);
	select * from t_explain_routine where a = id;
end`)

	rows := rowsToStrings(tk.MustQuery("explain analyze routine procedure p_explain_routine_nested_parent(1)").Rows())
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root" && row[5] == "call" &&
			strings.Contains(strings.ToLower(row[6]), "call p_explain_routine_nested_helper") &&
			row[7] == "YES" && row[10] == "1" && row[13] == "0"
	})
	requireAnyRow(t, rows, func(row []string) bool {
		return row[4] == "root" && row[5] == "select" &&
			strings.Contains(strings.ToLower(row[6]), "select * from t_explain_routine") &&
			row[7] == "YES" && row[10] == "1" && row[13] == "1"
	})
}
