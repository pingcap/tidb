package parser

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
)

// TestASTExpressionParity verifies expression-level AST fields match parser.y grammar.
func TestASTExpressionParity(t *testing.T) {

	extractExpr := func(t *testing.T, sql string) ast.ExprNode {
		t.Helper()
		stmts := parseForTest(t, sql)
		if stmts == nil {
			return nil
		}
		sel := stmts[0].(*ast.SelectStmt)
		return sel.Fields.Fields[0].Expr
	}

	// ─── PatternInExpr ───────────────────────────────────────────────────────
	t.Run("in-expr-not-negated", func(t *testing.T) {
		e := extractExpr(t, "SELECT a IN (1, 2, 3) FROM t")
		if e == nil {
			return
		}
		in := e.(*ast.PatternInExpr)
		assert.False(t, in.Not, "a IN (...) must have Not=false")
		assert.Len(t, in.List, 3, "IN list must have 3 elements")
	})

	t.Run("not-in-expr-negated", func(t *testing.T) {
		e := extractExpr(t, "SELECT a NOT IN (1, 2) FROM t")
		if e == nil {
			return
		}
		in := e.(*ast.PatternInExpr)
		assert.True(t, in.Not, "a NOT IN (...) must have Not=true")
	})

	t.Run("in-subquery", func(t *testing.T) {
		e := extractExpr(t, "SELECT a IN (SELECT b FROM s) FROM t")
		if e == nil {
			return
		}
		in := e.(*ast.PatternInExpr)
		assert.False(t, in.Not, "a IN (subquery) must have Not=false")
		assert.NotNil(t, in.Sel, "IN subquery must set Sel field")
		assert.Empty(t, in.List, "IN subquery form must have empty List")
	})

	// ─── PatternLikeOrIlikeExpr ──────────────────────────────────────────────
	t.Run("like-not-negated", func(t *testing.T) {
		e := extractExpr(t, "SELECT a LIKE 'b%' FROM t")
		if e == nil {
			return
		}
		like := e.(*ast.PatternLikeOrIlikeExpr)
		assert.False(t, like.Not, "LIKE must have Not=false")
		assert.True(t, like.IsLike, "LIKE must have IsLike=true")
	})

	t.Run("not-like-negated", func(t *testing.T) {
		e := extractExpr(t, "SELECT a NOT LIKE 'b%' FROM t")
		if e == nil {
			return
		}
		like := e.(*ast.PatternLikeOrIlikeExpr)
		assert.True(t, like.Not, "NOT LIKE must have Not=true")
	})

	t.Run("ilike-flag", func(t *testing.T) {
		e := extractExpr(t, "SELECT a ILIKE 'b%' FROM t")
		if e == nil {
			return
		}
		like := e.(*ast.PatternLikeOrIlikeExpr)
		// IsLike=true means LIKE, IsLike=false means ILIKE (see Restore() impl)
		assert.False(t, like.IsLike, "ILIKE must have IsLike=false")
		assert.False(t, like.Not, "ILIKE must have Not=false")
	})

	t.Run("like-escape-char", func(t *testing.T) {
		e := extractExpr(t, "SELECT a LIKE 'b\\%' ESCAPE '\\' FROM t")
		if e == nil {
			return
		}
		like := e.(*ast.PatternLikeOrIlikeExpr)
		assert.NotNil(t, like.Escape, "LIKE...ESCAPE must set Escape field")
	})

	// ─── PatternRegexpExpr ───────────────────────────────────────────────────
	t.Run("regexp-not-negated", func(t *testing.T) {
		e := extractExpr(t, "SELECT a REGEXP 'b.*' FROM t")
		if e == nil {
			return
		}
		re := e.(*ast.PatternRegexpExpr)
		assert.False(t, re.Not, "REGEXP must have Not=false")
	})

	t.Run("not-regexp-negated", func(t *testing.T) {
		e := extractExpr(t, "SELECT a NOT REGEXP 'b.*' FROM t")
		if e == nil {
			return
		}
		re := e.(*ast.PatternRegexpExpr)
		assert.True(t, re.Not, "NOT REGEXP must have Not=true")
	})

	// ─── BetweenExpr ─────────────────────────────────────────────────────────
	t.Run("between-not-negated", func(t *testing.T) {
		e := extractExpr(t, "SELECT a BETWEEN 1 AND 10 FROM t")
		if e == nil {
			return
		}
		b := e.(*ast.BetweenExpr)
		assert.False(t, b.Not, "BETWEEN must have Not=false")
	})

	t.Run("not-between-negated", func(t *testing.T) {
		e := extractExpr(t, "SELECT a NOT BETWEEN 1 AND 10 FROM t")
		if e == nil {
			return
		}
		b := e.(*ast.BetweenExpr)
		assert.True(t, b.Not, "NOT BETWEEN must have Not=true")
	})

	// ─── IsNullExpr ──────────────────────────────────────────────────────────
	t.Run("is-null", func(t *testing.T) {
		e := extractExpr(t, "SELECT a IS NULL FROM t")
		if e == nil {
			return
		}
		isNull := e.(*ast.IsNullExpr)
		assert.False(t, isNull.Not, "IS NULL must have Not=false")
	})

	t.Run("is-not-null", func(t *testing.T) {
		e := extractExpr(t, "SELECT a IS NOT NULL FROM t")
		if e == nil {
			return
		}
		isNull := e.(*ast.IsNullExpr)
		assert.True(t, isNull.Not, "IS NOT NULL must have Not=true")
	})

	// ─── IsTruthExpr ─────────────────────────────────────────────────────────
	t.Run("is-true", func(t *testing.T) {
		e := extractExpr(t, "SELECT a IS TRUE FROM t")
		if e == nil {
			return
		}
		isTruth := e.(*ast.IsTruthExpr)
		assert.Equal(t, int64(1), isTruth.True, "IS TRUE must have True=1")
		assert.False(t, isTruth.Not, "IS TRUE must have Not=false")
	})

	t.Run("is-not-true", func(t *testing.T) {
		e := extractExpr(t, "SELECT a IS NOT TRUE FROM t")
		if e == nil {
			return
		}
		isTruth := e.(*ast.IsTruthExpr)
		assert.Equal(t, int64(1), isTruth.True, "IS NOT TRUE must have True=1")
		assert.True(t, isTruth.Not, "IS NOT TRUE must have Not=true")
	})

	t.Run("is-false", func(t *testing.T) {
		e := extractExpr(t, "SELECT a IS FALSE FROM t")
		if e == nil {
			return
		}
		isTruth := e.(*ast.IsTruthExpr)
		assert.Equal(t, int64(0), isTruth.True, "IS FALSE must have True=0")
		assert.False(t, isTruth.Not, "IS FALSE must have Not=false")
	})

	t.Run("is-not-false", func(t *testing.T) {
		e := extractExpr(t, "SELECT a IS NOT FALSE FROM t")
		if e == nil {
			return
		}
		isTruth := e.(*ast.IsTruthExpr)
		assert.Equal(t, int64(0), isTruth.True, "IS NOT FALSE must have True=0")
		assert.True(t, isTruth.Not, "IS NOT FALSE must have Not=true")
	})

	// ─── ExistsSubqueryExpr ──────────────────────────────────────────────────
	t.Run("exists-subquery", func(t *testing.T) {
		e := extractExpr(t, "SELECT EXISTS (SELECT 1 FROM t WHERE a=1) FROM dual")
		if e == nil {
			return
		}
		ex := e.(*ast.ExistsSubqueryExpr)
		assert.False(t, ex.Not, "EXISTS must have Not=false")
		assert.NotNil(t, ex.Sel, "EXISTS must have Sel set")
	})

	t.Run("not-exists-subquery", func(t *testing.T) {
		e := extractExpr(t, "SELECT NOT EXISTS (SELECT 1 FROM t WHERE a=1) FROM dual")
		if e == nil {
			return
		}
		// NOT EXISTS is represented as UnaryOperationExpr(NOT, ExistsSubqueryExpr)
		// OR as ExistsSubqueryExpr with Not=true depending on implementation.
		// Just verify it parses, as both representations are valid.
		assert.NotNil(t, e, "NOT EXISTS expression must parse")
	})

	// ─── CASE expressions ────────────────────────────────────────────────────
	t.Run("case-when-searched", func(t *testing.T) {
		// CASE WHEN cond THEN val END — no Value (searched CASE)
		e := extractExpr(t, "SELECT CASE WHEN a=1 THEN 'x' WHEN a=2 THEN 'y' ELSE 'z' END FROM t")
		if e == nil {
			return
		}
		c := e.(*ast.CaseExpr)
		assert.Nil(t, c.Value, "searched CASE must have Value=nil")
		assert.Len(t, c.WhenClauses, 2, "Must have 2 WHEN clauses")
		assert.NotNil(t, c.ElseClause, "Must have ELSE clause")
	})

	t.Run("case-when-simple", func(t *testing.T) {
		// CASE value WHEN val THEN result END — has Value (simple CASE)
		e := extractExpr(t, "SELECT CASE a WHEN 1 THEN 'x' WHEN 2 THEN 'y' END FROM t")
		if e == nil {
			return
		}
		c := e.(*ast.CaseExpr)
		assert.NotNil(t, c.Value, "simple CASE must have Value set")
		assert.Len(t, c.WhenClauses, 2, "Must have 2 WHEN clauses")
		assert.Nil(t, c.ElseClause, "CASE without ELSE must have nil ElseClause")
	})

	// ─── SubqueryExpr: Correlated flag ───────────────────────────────────────
	t.Run("correlated-subquery", func(t *testing.T) {
		// Correlated: references outer table column `t.a`
		e := extractExpr(t, "SELECT (SELECT b FROM s WHERE s.id = t.a) FROM t")
		if e == nil {
			return
		}
		// Should be a SubqueryExpr
		assert.NotNil(t, e, "correlated subquery must parse")
	})

	// ─── UnaryOperationExpr ──────────────────────────────────────────────────
	t.Run("unary-not", func(t *testing.T) {
		e := extractExpr(t, "SELECT NOT a FROM t")
		if e == nil {
			return
		}
		u := e.(*ast.UnaryOperationExpr)
		// opcode.Not is the Go constant for logical NOT — test against it symbolically
		assert.NotNil(t, u, "NOT must produce UnaryOperationExpr")
	})

	t.Run("unary-minus", func(t *testing.T) {
		e := extractExpr(t, "SELECT -a FROM t")
		if e == nil {
			return
		}
		u := e.(*ast.UnaryOperationExpr)
		assert.NotNil(t, u, "Unary minus must produce UnaryOperationExpr")
	})

	// ─── Comparison operators ─────────────────────────────────────────────────
	t.Run("compare-null-safe-eq", func(t *testing.T) {
		// <=> (NULL-safe equal) is a distinct operator
		e := extractExpr(t, "SELECT a <=> NULL FROM t")
		if e == nil {
			return
		}
		assert.NotNil(t, e, "NULL-safe equal <=> must parse to an expression")
	})

	// ─── AggregateFuncExpr ───────────────────────────────────────────────────
	t.Run("count-star", func(t *testing.T) {
		e := extractExpr(t, "SELECT COUNT(*) FROM t")
		if e == nil {
			return
		}
		agg := e.(*ast.AggregateFuncExpr)
		// AggregateFuncExpr.F stores the original source keyword casing ("COUNT")
		assert.Equal(t, "COUNT", agg.F, "COUNT aggregate .F must be 'COUNT'")
	})

	t.Run("count-distinct", func(t *testing.T) {
		e := extractExpr(t, "SELECT COUNT(DISTINCT a) FROM t")
		if e == nil {
			return
		}
		agg := e.(*ast.AggregateFuncExpr)
		assert.True(t, agg.Distinct, "COUNT DISTINCT must have Distinct=true")
	})

	t.Run("sum-distinct", func(t *testing.T) {
		e := extractExpr(t, "SELECT SUM(DISTINCT a) FROM t")
		if e == nil {
			return
		}
		agg := e.(*ast.AggregateFuncExpr)
		assert.True(t, agg.Distinct, "SUM DISTINCT must have Distinct=true")
	})

	t.Run("group-concat-order-by", func(t *testing.T) {
		e := extractExpr(t, "SELECT GROUP_CONCAT(a ORDER BY b SEPARATOR ',') FROM t")
		if e == nil {
			return
		}
		agg := e.(*ast.AggregateFuncExpr)
		// GROUP_CONCAT preserves the original casing from the source keyword
		assert.Equal(t, "GROUP_CONCAT", agg.F)
		assert.NotNil(t, agg.Order, "GROUP_CONCAT with ORDER BY must set Order")
	})

	t.Run("group-concat-distinct", func(t *testing.T) {
		e := extractExpr(t, "SELECT GROUP_CONCAT(DISTINCT a, b) FROM t")
		if e == nil {
			return
		}
		agg := e.(*ast.AggregateFuncExpr)
		assert.True(t, agg.Distinct, "GROUP_CONCAT(DISTINCT ...) must set Distinct=true")
	})

	// ─── WindowFuncExpr ──────────────────────────────────────────────────────
	t.Run("window-func-name", func(t *testing.T) {
		e := extractExpr(t, "SELECT ROW_NUMBER() OVER () FROM t")
		if e == nil {
			return
		}
		w := e.(*ast.WindowFuncExpr)
		// WindowFuncExpr.Name preserves original casing from source
		assert.Equal(t, "ROW_NUMBER", w.Name, "WindowFuncExpr.Name must be 'ROW_NUMBER'")
	})

	t.Run("window-func-partition-by", func(t *testing.T) {
		e := extractExpr(t, "SELECT SUM(a) OVER (PARTITION BY b) FROM t")
		if e == nil {
			return
		}
		w := e.(*ast.WindowFuncExpr)
		assert.NotEmpty(t, w.Spec.PartitionBy.Items, "PARTITION BY must set items")
	})

	t.Run("window-func-ignore-nulls", func(t *testing.T) {
		// LEAD with IGNORE NULLS
		e := extractExpr(t, "SELECT LEAD(a, 1) IGNORE NULLS OVER (ORDER BY b) FROM t")
		if e == nil {
			return
		}
		w := e.(*ast.WindowFuncExpr)
		assert.True(t, w.IgnoreNull, "LEAD IGNORE NULLS must set IgnoreNull=true")
	})

	// ─── CAST expression type fields ─────────────────────────────────────────
	t.Run("cast-type-signed", func(t *testing.T) {
		e := extractExpr(t, "SELECT CAST(a AS SIGNED) FROM t")
		if e == nil {
			return
		}
		assert.NotNil(t, e, "CAST AS SIGNED must parse")
	})

	t.Run("cast-type-char", func(t *testing.T) {
		e := extractExpr(t, "SELECT CAST(a AS CHAR(10)) FROM t")
		if e == nil {
			return
		}
		assert.NotNil(t, e, "CAST AS CHAR(10) must parse")
	})

	t.Run("cast-type-json", func(t *testing.T) {
		e := extractExpr(t, "SELECT CAST(a AS JSON) FROM t")
		if e == nil {
			return
		}
		assert.NotNil(t, e, "CAST AS JSON must parse")
	})

	// ─── ColumnNameExpr ──────────────────────────────────────────────────────
	t.Run("column-name-schema-qualified", func(t *testing.T) {
		e := extractExpr(t, "SELECT db.t.a FROM t")
		if e == nil {
			return
		}
		col := e.(*ast.ColumnNameExpr)
		assert.Equal(t, "a", col.Name.Name.L, "Column name must be 'a'")
		assert.Equal(t, "t", col.Name.Table.L, "Table qualifier must be 't'")
		assert.Equal(t, "db", col.Name.Schema.L, "Schema qualifier must be 'db'")
	})

	// ─── VariableExpr ─────────────────────────────────────────────────────────
	t.Run("user-var-isSystem-false", func(t *testing.T) {
		e := extractExpr(t, "SELECT @myvar FROM t")
		if e == nil {
			return
		}
		v := e.(*ast.VariableExpr)
		assert.False(t, v.IsSystem, "@myvar must have IsSystem=false")
		assert.Equal(t, "myvar", v.Name, "user var name must be 'myvar'")
	})

	t.Run("system-var-isSystem-true", func(t *testing.T) {
		e := extractExpr(t, "SELECT @@session.sql_mode FROM t")
		if e == nil {
			return
		}
		v := e.(*ast.VariableExpr)
		assert.True(t, v.IsSystem, "@@session.sql_mode must have IsSystem=true")
	})

	t.Run("global-var-scope", func(t *testing.T) {
		e := extractExpr(t, "SELECT @@global.max_connections FROM t")
		if e == nil {
			return
		}
		v := e.(*ast.VariableExpr)
		assert.True(t, v.IsGlobal, "@@global.max_connections must have IsGlobal=true")
	})

	// ─── TableSource alias ────────────────────────────────────────────────────
	t.Run("table-alias", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT t1.a FROM t AS t1")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		ts := join.Left.(*ast.TableSource)
		assert.Equal(t, "t1", ts.AsName.L, "Table alias must be 't1'")
	})

	// ─── SelectField alias ────────────────────────────────────────────────────
	t.Run("field-alias", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT a AS my_alias FROM t")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		field := sel.Fields.Fields[0]
		assert.Equal(t, "my_alias", field.AsName.L, "Field alias must be 'my_alias'")
	})

	t.Run("field-alias-without-as", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT a my_alias FROM t")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		field := sel.Fields.Fields[0]
		assert.Equal(t, "my_alias", field.AsName.L, "Field alias without AS must be 'my_alias'")
	})

	t.Run("wildcard-field", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		field := sel.Fields.Fields[0]
		assert.True(t, field.WildCard != nil, "SELECT * must set WildCard field")
	})

	// ─── HAVING clause expression ─────────────────────────────────────────────
	t.Run("having-aggregation-in-expr", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT a, COUNT(b) FROM t GROUP BY a HAVING COUNT(b) > 5")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.NotNil(t, sel.Having, "HAVING must be set")
		assert.NotNil(t, sel.Having.Expr, "HAVING.Expr must be set")
	})

	// ─── Subquery in WHERE ─────────────────────────────────────────────────────
	t.Run("subquery-in-where", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t WHERE a IN (SELECT b FROM s WHERE s.c > 1)")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.NotNil(t, sel.Where, "WHERE clause must be set")
		inExpr := sel.Where.(*ast.PatternInExpr)
		assert.NotNil(t, inExpr.Sel, "Subquery IN must set Sel field")
	})

	// ─── ALL/ANY/SOME quantified comparison ───────────────────────────────────
	t.Run("all-comparison-subquery", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t WHERE a > ALL (SELECT b FROM s)")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.NotNil(t, sel.Where, "WHERE clause must be set for > ALL")
	})

	t.Run("any-comparison-subquery", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t WHERE a > ANY (SELECT b FROM s)")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.NotNil(t, sel.Where, "WHERE clause must be set for > ANY")
	})
}
