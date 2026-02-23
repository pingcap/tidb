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
// See the License for the specific language governing permissions and
// limitations under the License.

package parser_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

// parseWithHandParser parses a SQL string with the hand-written parser.
func parseWithHandParser(t *testing.T, sql string) []ast.StmtNode {
	t.Helper()
	scanner := parser.NewScanner(sql)
	hp := parser.NewHandParser()
	hp.Init(scanner, sql)
	stmts, warns, err := hp.ParseSQL()
	require.NoError(t, err, "hand parser error for: %s", sql)
	if len(warns) > 0 {
		t.Logf("warns for %q: %v", sql, warns)
	}
	return stmts
}

func TestHandParserSimpleSelect(t *testing.T) {
	tests := []string{
		"SELECT 1",
		"SELECT 1, 2, 3",
		"SELECT a FROM t",
		"SELECT a, b FROM t WHERE a = 1",
		"SELECT * FROM t",
		"SELECT t.* FROM t",
		"SELECT a FROM t WHERE a > 1 AND b < 2",
		"SELECT a FROM t WHERE a = 1 OR b = 2",
		"SELECT a FROM t ORDER BY a",
		"SELECT a FROM t ORDER BY a DESC",
		"SELECT a FROM t LIMIT 10",
		"SELECT a FROM t LIMIT 10, 20",
		"SELECT a FROM t LIMIT 10 OFFSET 5",
		"SELECT a, b FROM t GROUP BY a",
		"SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
		"SELECT DISTINCT a FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmts := parseWithHandParser(t, sql)
			require.Len(t, stmts, 1, "expected 1 statement for: %s", sql)
			sel, ok := stmts[0].(*ast.SelectStmt)
			require.True(t, ok, "expected SelectStmt for: %s", sql)
			require.NotNil(t, sel.Fields, "expected Fields for: %s", sql)
		})
	}
}

func TestHandParserJoins(t *testing.T) {
	tests := []string{
		"SELECT a FROM t1 JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 RIGHT JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 INNER JOIN t2 ON t1.id = t2.id",
		"SELECT a FROM t1 CROSS JOIN t2",
		"SELECT a FROM t1, t2 WHERE t1.id = t2.id",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmts := parseWithHandParser(t, sql)
			require.Len(t, stmts, 1)
			sel := stmts[0].(*ast.SelectStmt)
			require.NotNil(t, sel.From)
			require.NotNil(t, sel.From.TableRefs)
		})
	}
}

func TestHandParserInsert(t *testing.T) {
	tests := []string{
		"INSERT INTO t VALUES (1, 2, 3)",
		"INSERT INTO t (a, b, c) VALUES (1, 2, 3)",
		"INSERT INTO t (a, b) VALUES (1, 2), (3, 4)",
		"INSERT INTO t SET a = 1, b = 2",
		"REPLACE INTO t VALUES (1, 2, 3)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmts := parseWithHandParser(t, sql)
			require.Len(t, stmts, 1)
			ins, ok := stmts[0].(*ast.InsertStmt)
			require.True(t, ok, "expected InsertStmt for: %s", sql)
			require.NotNil(t, ins.Table)
		})
	}

	// Regression test for F2: INSERT SET with multiple assignments must preserve all values.
	t.Run("INSERT_SET_multiple_values", func(t *testing.T) {
		stmts := parseWithHandParser(t, "INSERT INTO t SET a = 1, b = 2, c = 3")
		require.Len(t, stmts, 1)
		ins := stmts[0].(*ast.InsertStmt)
		require.Len(t, ins.Columns, 3, "expected 3 columns in INSERT SET")
		require.Len(t, ins.Lists, 1, "expected 1 row in INSERT SET")
		require.Len(t, ins.Lists[0], 3, "expected 3 values in INSERT SET row")
	})

	// Regression test for F1: Non-aggregate functions must produce FuncCallExpr.
	t.Run("scalar_function_call", func(t *testing.T) {
		stmts := parseWithHandParser(t, "SELECT CONCAT(a, b) FROM t")
		require.Len(t, stmts, 1)
		sel := stmts[0].(*ast.SelectStmt)
		require.NotNil(t, sel.Fields)
		require.Len(t, sel.Fields.Fields, 1)
		_, ok := sel.Fields.Fields[0].Expr.(*ast.FuncCallExpr)
		require.True(t, ok, "non-aggregate function should produce FuncCallExpr, not AggregateFuncExpr")
	})
}

func TestHandParserUpdate(t *testing.T) {
	tests := []string{
		"UPDATE t SET a = 1",
		"UPDATE t SET a = 1, b = 2 WHERE c = 3",
		"UPDATE t SET a = a + 1 ORDER BY b LIMIT 10",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmts := parseWithHandParser(t, sql)
			require.Len(t, stmts, 1)
			upd, ok := stmts[0].(*ast.UpdateStmt)
			require.True(t, ok, "expected UpdateStmt for: %s", sql)
			require.NotNil(t, upd.TableRefs)
			require.NotEmpty(t, upd.List)
		})
	}
}

func TestHandParserDelete(t *testing.T) {
	tests := []string{
		"DELETE FROM t WHERE a = 1",
		"DELETE FROM t ORDER BY a LIMIT 10",
		"DELETE FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmts := parseWithHandParser(t, sql)
			require.Len(t, stmts, 1)
			del, ok := stmts[0].(*ast.DeleteStmt)
			require.True(t, ok, "expected DeleteStmt for: %s", sql)
			require.NotNil(t, del.TableRefs)
		})
	}
}

func TestHandParserExpressions(t *testing.T) {
	tests := []string{
		"SELECT 1 + 2",
		"SELECT a * b + c",
		"SELECT a AND b OR c",
		"SELECT NOT a",
		"SELECT -a",
		"SELECT a IS NULL",
		"SELECT a IS NOT NULL",
		"SELECT a IN (1, 2, 3)",
		"SELECT a NOT IN (1, 2, 3)",
		"SELECT a BETWEEN 1 AND 10",
		"SELECT a NOT BETWEEN 1 AND 10",
		"SELECT a LIKE 'foo%'",
		"SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END",
		"SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END",
		"SELECT a > b",
		"SELECT a >= b",
		"SELECT a < b",
		"SELECT a <= b",
		"SELECT a != b",
		"SELECT a <> b",
		"SELECT a <=> b",
		"SELECT a = 1 AND (b = 2 OR c = 3)",
		"SELECT ?",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmts := parseWithHandParser(t, sql)
			require.Len(t, stmts, 1, "expected 1 statement for: %s", sql)
			sel := stmts[0].(*ast.SelectStmt)
			require.NotNil(t, sel.Fields)
			require.NotEmpty(t, sel.Fields.Fields)
		})
	}
}

func TestHandParserSubquery(t *testing.T) {
	tests := []string{
		"SELECT a FROM t WHERE a IN (SELECT b FROM t2)",
		"SELECT EXISTS (SELECT 1 FROM t)",
		"SELECT (SELECT 1)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmts := parseWithHandParser(t, sql)
			require.Len(t, stmts, 1)
		})
	}
}

func TestHandParserShow(t *testing.T) {
	tests := []string{
		"SHOW BUILTINS",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmts := parseWithHandParser(t, sql)
			require.Len(t, stmts, 1, "expected 1 statement for: %s", sql)
			show, ok := stmts[0].(*ast.ShowStmt)
			require.True(t, ok, "expected ShowStmt for: %s", sql)
			require.Equal(t, ast.ShowBuiltins, int(show.Tp))
		})
	}
}
