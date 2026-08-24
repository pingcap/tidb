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

package plancache

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestParameterize(t *testing.T) {
	cases := []struct {
		sql      string
		paramSQL string
		params   []any
	}{
		{
			"select * from t where a<10",
			"SELECT * FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			"select * from t",
			"SELECT * FROM `t`",
			[]any{},
		},
		{
			"select * from t where a<10 and b<20 and c=30 and d>40",
			"SELECT * FROM `t` WHERE `a`<? AND `b`<? AND `c`=? AND `d`>?",
			[]any{int64(10), int64(20), int64(30), int64(40)},
		},
		{
			"select * from t where a='a' and b='bbbbbbbbbbbbbbbbbbbbbbbb'",
			"SELECT * FROM `t` WHERE `a`=? AND `b`=?",
			[]any{"a", "bbbbbbbbbbbbbbbbbbbbbbbb"},
		},
		{
			"select 1, 2, 3 from t where a<10",
			"SELECT 1,2,3 FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			"select a+1 from t where a<10",
			"SELECT a+1 FROM `t` WHERE `a`<?",
			[]any{int64(10)},
		},
		{
			`select a+ "a b c" from t`,
			"SELECT a+ \"a b c\" FROM `t`",
			[]any{},
		},
		{
			`select a + 'a b c'+"x" from t`,
			"SELECT a + 'a b c'+\"x\" FROM `t`", // keep the original format for select fields
			[]any{},
		},
		{
			`select a + 'a b c'+"x" as 'xxx' from t`,
			"SELECT a + 'a b c'+\"x\" as 'xxx' FROM `t`", // keep the original format for select fields
			[]any{},
		},
		{
			`insert into t (a, B, c) values (1, 2, 3), (4, 5, 6)`,
			"INSERT INTO `t` (`a`,`B`,`c`) VALUES (?,?,?),(?,?,?)",
			[]any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
		},
		{
			`select * from t where a < date_format('2020-02-02', '%Y-%m-%d')`,
			"SELECT * FROM `t` WHERE `a`<date_format(?, '%Y-%m-%d')",
			[]any{"2020-02-02"},
		},
		{
			"select * from `txu#p#p1`",
			"SELECT * FROM `txu#p#p1`",
			[]any{},
		},

		// keep the original format for limit clauses
		{
			`select * from t limit 10`,
			"SELECT * FROM `t` LIMIT 10",
			[]any{},
		},
		{
			`select * from t limit 10, 20`,
			"SELECT * FROM `t` LIMIT 10,20",
			[]any{},
		},
		// TODO: more test cases
	}

	for _, c := range cases {
		stmt, err := parser.New().ParseOneStmt(c.sql, "", "")
		require.Nil(t, err)
		paramSQL, params, err := plannercore.ParameterizeAST(stmt)
		require.Nil(t, err)
		require.Equal(t, c.paramSQL, paramSQL)
		require.Equal(t, len(c.params), len(params))
		for i := range params {
			require.Equal(t, c.params[i], params[i].Datum.GetValue())
		}
	}
}

func TestGetParamSQLFromASTConcurrently(t *testing.T) {
	n := 50
	sqls := make([]string, 0, n)
	for i := range n {
		sqls = append(sqls, fmt.Sprintf(`insert into t values (%d, %d, %d)`, i*3+0, i*3+1, i*3+2))
	}
	stmts := make([]ast.StmtNode, 0, n)
	for _, sql := range sqls {
		stmt, err := parser.New().ParseOneStmt(sql, "", "")
		require.Nil(t, err)
		stmts = append(stmts, stmt)
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(id int) {
			for range 100 {
				_, vals, err := plannercore.GetParamSQLFromAST(stmts[id])
				require.Nil(t, err)
				require.Equal(t, len(vals), 3)
				require.Equal(t, vals[0].GetValue(), int64(id*3+0))
				require.Equal(t, vals[1].GetValue(), int64(id*3+1))
				require.Equal(t, vals[2].GetValue(), int64(id*3+2))
				time.Sleep(time.Millisecond + time.Duration(rand.Intn(int(time.Millisecond))))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func restorePlanCacheParamTestStmt(t *testing.T, stmt ast.StmtNode) string {
	var builder strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &builder)
	require.NoError(t, stmt.Restore(ctx))
	return builder.String()
}

func TestParameterizeForNonPreparedPlanCache(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().EnablePlanCacheForParamLimit = true

	testCases := []struct {
		name     string
		sql      string
		paramSQL string
		params   []any
	}{
		{
			name:     "filter and preserved clauses",
			sql:      "select 1 from t where a=2 group by 3 having sum(a)>4 order by 5 limit 6",
			paramSQL: "SELECT 1 FROM `t` WHERE `a`=? GROUP BY 3 HAVING sum(`a`)>? ORDER BY 5 LIMIT 6",
			params:   []any{int64(2), int64(4)},
		},
		{
			name:     "literal-list IN",
			sql:      "select * from t where a in (1, 2, 3) and b=4",
			paramSQL: "SELECT * FROM `t` WHERE `a` IN (?,?,?) AND `b`=?",
			params:   []any{int64(1), int64(2), int64(3), int64(4)},
		},
		{
			name:     "BETWEEN",
			sql:      "select * from t where a between 1 and 2 and b=3",
			paramSQL: "SELECT * FROM `t` WHERE `a` BETWEEN ? AND ? AND `b`=?",
			params:   []any{int64(1), int64(2), int64(3)},
		},
		{
			name:     "join and derived table",
			sql:      "select x.a from (select a from t where b=1) x join t2 on x.a=t2.a and t2.b=2 where x.a=3",
			paramSQL: "SELECT x.a FROM (SELECT a FROM `t` WHERE `b`=?) AS `x` JOIN `t2` ON `x`.`a`=`t2`.`a` AND `t2`.`b`=? WHERE `x`.`a`=?",
			params:   []any{int64(1), int64(2), int64(3)},
		},
		{
			name:     "cte",
			sql:      "with cte as (select a from t where b=1) select a from cte where a=2",
			paramSQL: "WITH `cte` AS (SELECT a FROM `t` WHERE `b`=?) SELECT a FROM `cte` WHERE `a`=?",
			params:   []any{int64(1), int64(2)},
		},
		{
			name:     "set operation",
			sql:      "select a from t where b=1 union select a from t where b=2 order by 1 limit 3",
			paramSQL: "SELECT a FROM `t` WHERE `b`=? UNION SELECT a FROM `t` WHERE `b`=? ORDER BY 1 LIMIT 3",
			params:   []any{int64(1), int64(2)},
		},
		{
			name:     "intersect",
			sql:      "select a from t where b=1 intersect select a from t where b=2",
			paramSQL: "SELECT a FROM `t` WHERE `b`=? INTERSECT SELECT a FROM `t` WHERE `b`=?",
			params:   []any{int64(1), int64(2)},
		},
		{
			name:     "except",
			sql:      "select a from t where b=1 except select a from t where b=2",
			paramSQL: "SELECT a FROM `t` WHERE `b`=? EXCEPT SELECT a FROM `t` WHERE `b`=?",
			params:   []any{int64(1), int64(2)},
		},
		{
			name:     "special literals",
			sql:      "select * from t where a=null or b=b'1' or c=x'0a' or d=4",
			paramSQL: "SELECT * FROM `t` WHERE `a`=NULL OR `b`=b'1' OR `c`=x'0a' OR `d`=?",
			params:   []any{int64(4)},
		},
		{
			name:     "binary literal token spelling",
			sql:      "select * from t where a=b'0001' or b=B'0001' or c=0b0001 or d=X'0A' or e=x'0A' or f=0x0A or g=4",
			paramSQL: "SELECT * FROM `t` WHERE `a`=b'0001' OR `b`=B'0001' OR `c`=0b0001 OR `d`=X'0A' OR `e`=x'0A' OR `f`=0x0A OR `g`=?",
			params:   []any{int64(4)},
		},
		{
			name:     "date format",
			sql:      "select * from t where a=date_format('2020-02-02', '%Y-%m-%d')",
			paramSQL: "SELECT * FROM `t` WHERE `a`=date_format(?, '%Y-%m-%d')",
			params:   []any{"2020-02-02"},
		},
		{
			name:     "generic function",
			sql:      "select * from t where coalesce(a, 1)=2",
			paramSQL: "SELECT * FROM `t` WHERE coalesce(`a`, ?)=?",
			params:   []any{int64(1), int64(2)},
		},
		{
			name:     "date format without arguments",
			sql:      "select * from t where date_format()",
			paramSQL: "SELECT * FROM `t` WHERE date_format()",
		},
		{
			name:     "window frame",
			sql:      "select sum(a) over (order by a rows 1 preceding) from t where b=2",
			paramSQL: "SELECT sum(a) over (order by a rows 1 preceding) FROM `t` WHERE `b`=?",
			params:   []any{int64(2)},
		},
		{
			name:     "named window",
			sql:      "select sum(a) over w from t where b=2 window w as (partition by a order by b)",
			paramSQL: "SELECT sum(a) over w FROM `t` WHERE `b`=? WINDOW `w` AS (PARTITION BY `a` ORDER BY `b`)",
			params:   []any{int64(2)},
		},
		{
			name:     "uncorrelated subquery",
			sql:      "select a from t where a in (select a from t2 where b=1) and a>2",
			paramSQL: "SELECT a FROM `t` WHERE `a` IN (SELECT a FROM `t2` WHERE `b`=?) AND `a`>?",
			params:   []any{int64(1), int64(2)},
		},
		{
			name:     "correlated subquery",
			sql:      "select a from t where a > (select max(a) from t2 where t2.b > t.b and t2.a > 1) and b=2",
			paramSQL: "SELECT a FROM `t` WHERE `a`>(SELECT max(a) FROM `t2` WHERE `t2`.`b`>`t`.`b` AND `t2`.`a`>?) AND `b`=?",
			params:   []any{int64(1), int64(2)},
		},
		{
			name:     "hint",
			sql:      "select /*+ use_index(t, idx_a) */ * from t where a=1",
			paramSQL: "SELECT /*+ use_index(`t` `idx_a`)*/ * FROM `t` WHERE `a`=?",
			params:   []any{int64(1)},
		},
		{
			name:     "insert values",
			sql:      "insert into t values (1, null, x'0a')",
			paramSQL: "INSERT INTO `t` VALUES (?,NULL,x'0a')",
			params:   []any{int64(1)},
		},
		{
			name:     "insert on duplicate key update",
			sql:      "insert into t (a, b) values (1, 2) on duplicate key update b=b+3",
			paramSQL: "INSERT INTO `t` (`a`,`b`) VALUES (?,?) ON DUPLICATE KEY UPDATE `b`=`b`+?",
			params:   []any{int64(1), int64(2), int64(3)},
		},
		{
			name:     "update assignment",
			sql:      "update t set a=1 where b=2 order by c+3 limit 4",
			paramSQL: "UPDATE `t` SET `a`=? WHERE `b`=? ORDER BY `c`+3 LIMIT 4",
			params:   []any{int64(1), int64(2)},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(testCase.sql, "", "")
			require.NoError(t, err)
			originalSQL := restorePlanCacheParamTestStmt(t, stmt)

			result, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
			require.NoError(t, err)
			require.True(t, supported, reason)
			require.Empty(t, reason)
			require.Equal(t, testCase.paramSQL, result.ParamSQL)
			require.Len(t, result.ParamValues, len(testCase.params))
			for i, param := range result.ParamValues {
				require.Equal(t, testCase.params[i], param.GetValue())
			}
			require.Equal(t, originalSQL, restorePlanCacheParamTestStmt(t, stmt))
		})
	}
}

func TestParameterizeForNonPreparedPlanCacheBypass(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().EnablePlanCacheForParamLimit = true

	testCases := []struct {
		sql    string
		reason string
	}{
		{"select _utf8mb4'a' from t where a=1", "under-score charset"},
		{"select * from t into outfile '/tmp/a'", "SELECT INTO"},
		{"update t1 join t2 on t1.a=t2.a set t1.b=1", "multiple-table UPDATE"},
		{"delete t1 from t1 join t2 on t1.a=t2.a where t1.b=1", "multiple-table DELETE"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.reason, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(testCase.sql, "", "")
			require.NoError(t, err)
			originalSQL := restorePlanCacheParamTestStmt(t, stmt)
			_, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
			require.NoError(t, err)
			require.False(t, supported, "sql: %s, reason: %s", testCase.sql, reason)
			require.Contains(t, reason, testCase.reason)
			require.Equal(t, originalSQL, restorePlanCacheParamTestStmt(t, stmt))
		})
	}

	stmt, err := parser.New().ParseOneStmt("select * from t where a = ?", "", "")
	require.NoError(t, err)
	_, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
	require.NoError(t, err)
	require.False(t, supported)
	require.Equal(t, "query has parameter markers", reason)

	sctx.GetSessionVars().EnablePlanCacheForParamLimit = false
	stmt, err = parser.New().ParseOneStmt("select * from t limit 1", "", "")
	require.NoError(t, err)
	_, supported, reason, err = plannercore.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
	require.NoError(t, err)
	require.False(t, supported)
	require.Equal(t, "query has 'limit ?' is un-cacheable", reason)
}

func TestParameterizeForNonPreparedPlanCacheDumpfileBypass(t *testing.T) {
	sctx := mock.NewContext()
	stmt, err := parser.New().ParseOneStmt("select * from t into outfile '/tmp/a'", "", "")
	require.NoError(t, err)
	selectStmt := stmt.(*ast.SelectStmt)
	// The current parser does not expose INTO DUMPFILE syntax, but the AST
	// supports the type. Keep the unified gate conservative for it as well.
	selectStmt.SelectIntoOpt.Tp = ast.SelectIntoDumpfile

	_, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
	require.NoError(t, err)
	require.False(t, supported)
	require.Equal(t, "SELECT INTO is not supported", reason)
}

// unknownPlanCacheExpr deliberately wraps a literal in an expression type that
// the selector does not know. It verifies the selector's preserve-by-default
// behavior for future AST nodes.
type unknownPlanCacheExpr struct {
	*ast.ParenthesesExpr
}

func (n *unknownPlanCacheExpr) Accept(v ast.Visitor) (ast.Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*unknownPlanCacheExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ast.ExprNode)
	return v.Leave(n)
}

func TestNonPreparedPlanCacheSelectorPreservesUnknownExpressionChildren(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().EnablePlanCacheForParamLimit = true
	stmt, err := parser.New().ParseOneStmt("select * from t where a=1 and b=2", "", "")
	require.NoError(t, err)
	where := stmt.(*ast.SelectStmt).Where.(*ast.BinaryOperationExpr)
	where.R = &unknownPlanCacheExpr{ParenthesesExpr: &ast.ParenthesesExpr{Expr: ast.NewValueExpr(int64(2), "", "")}}
	originalSQL := restorePlanCacheParamTestStmt(t, stmt)

	result, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
	require.NoError(t, err)
	require.True(t, supported, reason)
	require.Equal(t, "SELECT * FROM `t` WHERE `a`=? AND (2)", result.ParamSQL)
	require.Len(t, result.ParamValues, 1)
	require.Equal(t, int64(1), result.ParamValues[0].GetValue())
	require.Equal(t, originalSQL, restorePlanCacheParamTestStmt(t, stmt))
}

func TestNonPreparedPlanCacheParameterizerSelectsMultiTableDML(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		paramSQL string
		params   []any
	}{
		{
			name:     "update",
			sql:      "update t1 join t2 on t1.a=t2.a and t2.b=2 set t1.b=1 where t1.a=3",
			paramSQL: "UPDATE `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a` AND `t2`.`b`=? SET `t1`.`b`=? WHERE `t1`.`a`=?",
			params:   []any{int64(2), int64(1), int64(3)},
		},
		{
			name:     "delete",
			sql:      "delete t1 from t1 join t2 on t1.a=t2.a and t2.b=2 where t1.a=3",
			paramSQL: "DELETE `t1` FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a` AND `t2`.`b`=? WHERE `t1`.`a`=?",
			params:   []any{int64(2), int64(3)},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(testCase.sql, "", "")
			require.NoError(t, err)
			originalSQL := restorePlanCacheParamTestStmt(t, stmt)
			result, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(mock.NewContext().GetPlanCtx(), stmt)
			require.NoError(t, err)
			require.True(t, supported, reason)
			require.Equal(t, testCase.paramSQL, result.ParamSQL)
			require.Len(t, result.ParamValues, len(testCase.params))
			for i, value := range result.ParamValues {
				require.Equal(t, testCase.params[i], value.GetValue())
			}
			require.Equal(t, originalSQL, restorePlanCacheParamTestStmt(t, stmt))
		})
	}
}

func TestGetParamSQLFromASTRestoresAfterParamSQLRestoreFailure(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("select 1 from t where a=2", "", "")
	require.NoError(t, err)
	selectStmt := stmt.(*ast.SelectStmt)
	selectStmt.Fields.Fields[0].Expr = ast.NewValueExpr(struct{}{}, "", "")
	selectStmt.Fields.Fields[0].SetText(nil, "")

	_, _, err = plannercore.GetParamSQLFromAST(stmt)
	require.Error(t, err)
	binaryExpr := selectStmt.Where.(*ast.BinaryOperationExpr)
	require.IsType(t, &driver.ValueExpr{}, binaryExpr.R)

	sctx := mock.NewContext()
	sctx.GetSessionVars().EnablePlanCacheForParamLimit = true
	for i := 0; i < 8; i++ {
		_, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
		require.NoError(t, err)
		require.False(t, supported)
		require.Equal(t, "failed to restore parameterized SQL", reason)
		require.IsType(t, &driver.ValueExpr{}, binaryExpr.R)
	}
}

func TestRestoreASTWithParamsValidatesBeforeMutation(t *testing.T) {
	stmt, err := parser.New().ParseOneStmt("select * from t where a=1 and b=2", "", "")
	require.NoError(t, err)
	_, params, err := plannercore.ParameterizeAST(stmt)
	require.NoError(t, err)
	require.Len(t, params, 2)
	parameterizedSQL := restorePlanCacheParamTestStmt(t, stmt)

	where := stmt.(*ast.SelectStmt).Where.(*ast.BinaryOperationExpr)
	leftMarker := where.L.(*ast.BinaryOperationExpr).R.(*driver.ParamMarkerExpr)
	rightMarker := where.R.(*ast.BinaryOperationExpr).R.(*driver.ParamMarkerExpr)
	leftMarker.Offset = -1
	require.Error(t, plannercore.RestoreASTWithParams(stmt, params))
	require.Same(t, leftMarker, where.L.(*ast.BinaryOperationExpr).R)
	require.Same(t, rightMarker, where.R.(*ast.BinaryOperationExpr).R)
	require.Equal(t, parameterizedSQL, restorePlanCacheParamTestStmt(t, stmt))

	leftMarker.Offset = 0
	rightMarker.Offset = len(params)
	require.Error(t, plannercore.RestoreASTWithParams(stmt, params))
	require.Same(t, leftMarker, where.L.(*ast.BinaryOperationExpr).R)
	require.Same(t, rightMarker, where.R.(*ast.BinaryOperationExpr).R)
	require.Equal(t, parameterizedSQL, restorePlanCacheParamTestStmt(t, stmt))

	rightMarker.Offset = 1
	require.NoError(t, plannercore.RestoreASTWithParams(stmt, params))
	require.Equal(t, int64(1), where.L.(*ast.BinaryOperationExpr).R.(*driver.ValueExpr).GetInt64())
	require.Equal(t, int64(2), where.R.(*ast.BinaryOperationExpr).R.(*driver.ValueExpr).GetInt64())
}

func TestParameterizeForNonPreparedPlanCacheLiteralLimit(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().EnablePlanCacheForParamLimit = true
	values := make([]string, 201)
	for i := range values {
		values[i] = fmt.Sprintf("%d", i)
	}
	stmt, err := parser.New().ParseOneStmt("select * from t where a in ("+strings.Join(values, ",")+")", "", "")
	require.NoError(t, err)
	_, supported, reason, err := plannercore.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
	require.NoError(t, err)
	require.False(t, supported)
	require.Equal(t, "query has too many constants", reason)
}

func BenchmarkParameterizeSelect(b *testing.B) {
	paymentSelectCustomerForUpdate := `SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ?AND c_id = ? FOR UPDATE`
	stmt, err := parser.New().ParseOneStmt(paymentSelectCustomerForUpdate, "", "")
	require.Nil(b, err)
	_, _, err = plannercore.ParameterizeAST(stmt)
	require.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plannercore.ParameterizeAST(stmt)
	}
}

func BenchmarkParameterizeInsert(b *testing.B) {
	paymentInsertHistory := `INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (1, 2, 3, 4, 5, 6, 7, 8)`
	stmt, err := parser.New().ParseOneStmt(paymentInsertHistory, "", "")
	require.Nil(b, err)
	_, _, err = plannercore.ParameterizeAST(stmt)
	require.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plannercore.ParameterizeAST(stmt)
	}
}

func BenchmarkGetParamSQL(b *testing.B) {
	paymentInsertHistory := `INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (1, 2, 3, 4, 5, 6, 7, 8)`
	stmt, err := parser.New().ParseOneStmt(paymentInsertHistory, "", "")
	require.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plannercore.GetParamSQLFromAST(stmt)
	}
}
