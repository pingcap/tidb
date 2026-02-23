package parser

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	pformat "github.com/pingcap/tidb/pkg/parser/format"
	"github.com/stretchr/testify/assert"
)

func parseForTest(t *testing.T, sql string) []ast.StmtNode {
	t.Helper()
	hp := NewHandParser()
	scanner := NewScanner(sql)
	hp.Init(scanner, sql)
	stmts, _, err := hp.ParseSQL()
	assert.NoError(t, err, "SQL: %s", sql)
	if err != nil {
		return nil
	}
	return stmts
}

func restoreToStr(node ast.Node) string {
	sb := &strings.Builder{}
	ctx := pformat.NewRestoreCtx(pformat.DefaultRestoreFlags, sb)
	if err := node.Restore(ctx); err != nil {
		return "ERROR:" + err.Error()
	}
	return sb.String()
}

// TestASTStructuralParity checks that the parsed AST has the expected structural
// field values, not just that parsing succeeds. This enforces deeper parity.
func TestASTStructuralParity(t *testing.T) {
	t.Run("having-without-from", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT COUNT(*) HAVING COUNT(*) > 0")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.NotNil(t, sel.Having, "HAVING clause must be set")
		assert.Nil(t, sel.From, "FROM must be nil")
	})

	t.Run("having-without-group-by", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT 1 HAVING 1=1")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.NotNil(t, sel.Having, "HAVING clause must be set")
		assert.Nil(t, sel.GroupBy, "GroupBy must be nil when no GROUP BY is used")
	})

	t.Run("subselect-in-braces", func(t *testing.T) {
		stmts := parseForTest(t, "(SELECT 1)")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.True(t, sel.IsInBraces, "IsInBraces must be true for parenthesized SELECT")
	})

	t.Run("union-in-braces", func(t *testing.T) {
		stmts := parseForTest(t, "(SELECT 1 UNION SELECT 2)")
		if stmts == nil {
			return
		}
		setopr := stmts[0].(*ast.SetOprStmt)
		assert.True(t, setopr.IsInBraces, "SetOprStmt.IsInBraces must be true")
	})

	t.Run("with-attached-to-first-select", func(t *testing.T) {
		stmts := parseForTest(t, "WITH cte AS (SELECT 1 AS n) SELECT n FROM cte UNION ALL SELECT n+1 FROM cte WHERE n < 10")
		if stmts == nil {
			return
		}
		switch s := stmts[0].(type) {
		case *ast.SelectStmt:
			assert.NotNil(t, s.With, "SelectStmt.With must be set")
		case *ast.SetOprStmt:
			assert.NotNil(t, s.With, "SetOprStmt.With must be set")
		default:
			t.Fatalf("Expected SelectStmt or SetOprStmt, got %T", stmts[0])
		}
	})

	t.Run("update-single-table-limit", func(t *testing.T) {
		stmts := parseForTest(t, "UPDATE t SET a=1 ORDER BY b LIMIT 5")
		if stmts == nil {
			return
		}
		u := stmts[0].(*ast.UpdateStmt)
		assert.NotNil(t, u.Limit, "UpdateStmt.Limit must be set")
		assert.NotNil(t, u.Order, "UpdateStmt.Order must be set")
	})

	t.Run("update-multi-table-no-limit", func(t *testing.T) {
		stmts := parseForTest(t, "UPDATE t1, t2 SET t1.a=t2.a WHERE t1.id=t2.id")
		if stmts == nil {
			return
		}
		u := stmts[0].(*ast.UpdateStmt)
		assert.Nil(t, u.Limit, "multi-table UpdateStmt must not have Limit")
		assert.Nil(t, u.Order, "multi-table UpdateStmt must not have Order")
		assert.True(t, u.MultipleTable, "UpdateStmt.MultipleTable must be true")
	})

	t.Run("delete-multi-table-from-names", func(t *testing.T) {
		stmts := parseForTest(t, "DELETE t1 FROM t1 JOIN t2 ON t1.id=t2.id")
		if stmts == nil {
			return
		}
		d := stmts[0].(*ast.DeleteStmt)
		assert.True(t, d.IsMultiTable, "DeleteStmt.IsMultiTable must be true")
		assert.NotNil(t, d.Tables, "DeleteStmt.Tables must be set for multi-table form")
	})

	t.Run("delete-using-form", func(t *testing.T) {
		stmts := parseForTest(t, "DELETE FROM t1 USING t1 JOIN t2 ON t1.id=t2.id WHERE t1.a=1")
		if stmts == nil {
			return
		}
		d := stmts[0].(*ast.DeleteStmt)
		assert.True(t, d.IsMultiTable, "DeleteStmt.IsMultiTable must be true for USING form")
	})

	t.Run("insert-values-kind", func(t *testing.T) {
		stmts := parseForTest(t, "INSERT INTO t VALUES (1, 2)")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		assert.NotEmpty(t, ins.Lists, "InsertStmt.Lists must be set for VALUES form")
		assert.Nil(t, ins.Select, "InsertStmt.Select must be nil for VALUES form")
	})

	t.Run("insert-select-kind", func(t *testing.T) {
		stmts := parseForTest(t, "INSERT INTO t SELECT * FROM s")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		assert.NotNil(t, ins.Select, "InsertStmt.Select must be set for SELECT form")
		assert.Empty(t, ins.Lists, "InsertStmt.Lists must be empty for SELECT form")
	})

	t.Run("select-distinct", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT DISTINCT a, b FROM t")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.True(t, sel.Distinct, "SelectStmt.Distinct must be true")
	})

	t.Run("select-all", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT ALL a FROM t")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.False(t, sel.Distinct, "SelectStmt.Distinct must be false for ALL")
	})

	t.Run("sql-calc-found-rows", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT SQL_CALC_FOUND_ROWS * FROM t")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.True(t, sel.SelectStmtOpts.CalcFoundRows, "CalcFoundRows must be set")
	})

	t.Run("straight-join", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT STRAIGHT_JOIN * FROM t")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.True(t, sel.SelectStmtOpts.StraightJoin, "StraightJoin must be set")
	})

	t.Run("table-stmt-kind", func(t *testing.T) {
		stmts := parseForTest(t, "TABLE t ORDER BY a")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.Equal(t, ast.SelectStmtKindTable, sel.Kind, "TABLE stmt Kind must be SelectStmtKindTable")
		assert.NotNil(t, sel.OrderBy, "TABLE stmt must support ORDER BY")
	})

	t.Run("values-stmt-kind", func(t *testing.T) {
		stmts := parseForTest(t, "VALUES ROW(1, 2), ROW(3, 4) ORDER BY 1")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.Equal(t, ast.SelectStmtKindValues, sel.Kind, "VALUES stmt Kind must be SelectStmtKindValues")
		assert.NotEmpty(t, sel.Lists, "VALUES stmt Lists must be set")
		assert.NotNil(t, sel.OrderBy, "VALUES stmt must support ORDER BY")
	})

	t.Run("union-all-structure", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1 UNION ALL SELECT * FROM t2")
		if stmts == nil {
			return
		}
		setopr := stmts[0].(*ast.SetOprStmt)
		assert.Len(t, setopr.SelectList.Selects, 2, "UNION must have 2 selects")
	})

	t.Run("recursive-cte-flag", func(t *testing.T) {
		stmts := parseForTest(t, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 10) SELECT * FROM r")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.NotNil(t, sel.With, "SELECT.With must be set")
		assert.True(t, sel.With.IsRecursive, "WithClause.IsRecursive must be true")
		assert.Len(t, sel.With.CTEs, 1, "Must have 1 CTE")
		assert.True(t, sel.With.CTEs[0].IsRecursive, "CTE.IsRecursive must be true")
	})

	t.Run("explain-format-json", func(t *testing.T) {
		stmts := parseForTest(t, "EXPLAIN FORMAT=JSON SELECT * FROM t")
		if stmts == nil {
			return
		}
		exp := stmts[0].(*ast.ExplainStmt)
		assert.Equal(t, "json", strings.ToLower(exp.Format), "ExplainStmt.Format must be 'json'")
	})

	t.Run("explain-analyze-flag", func(t *testing.T) {
		stmts := parseForTest(t, "EXPLAIN ANALYZE SELECT * FROM t")
		if stmts == nil {
			return
		}
		exp := stmts[0].(*ast.ExplainStmt)
		assert.True(t, exp.Analyze, "ExplainStmt.Analyze must be true")
	})

	t.Run("grant-with-grant-option", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT ON t TO 'u'@'h' WITH GRANT OPTION")
		if stmts == nil {
			return
		}
		g := stmts[0].(*ast.GrantStmt)
		assert.True(t, g.WithGrant, "GrantStmt.WithGrant must be true")
	})

	t.Run("grant-without-grant-option", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT ON t TO 'u'@'h'")
		if stmts == nil {
			return
		}
		g := stmts[0].(*ast.GrantStmt)
		assert.False(t, g.WithGrant, "GrantStmt.WithGrant must be false by default")
	})

	t.Run("set-tx-isolation-one-shot", func(t *testing.T) {
		stmts := parseForTest(t, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.SetStmt)
		var found bool
		for _, v := range s.Variables {
			if v.Name == "tx_isolation_one_shot" {
				found = true
				break
			}
		}
		assert.True(t, found, "SET TRANSACTION ISOLATION LEVEL must use tx_isolation_one_shot as var name")
	})

	t.Run("set-session-tx-isolation-not-one-shot", func(t *testing.T) {
		stmts := parseForTest(t, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.SetStmt)
		for _, v := range s.Variables {
			assert.NotEqual(t, "tx_isolation_one_shot", v.Name, "SET SESSION TRANSACTION must NOT use tx_isolation_one_shot")
		}
	})

	t.Run("insert-ignore-err", func(t *testing.T) {
		stmts := parseForTest(t, "INSERT IGNORE INTO t VALUES (1)")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		assert.True(t, ins.IgnoreErr, "InsertStmt.IgnoreErr must be true for IGNORE")
	})

	t.Run("cte-roundtrip", func(t *testing.T) {
		stmts := parseForTest(t, "WITH cte AS (SELECT 1) SELECT * FROM cte")
		if stmts == nil {
			return
		}
		r := restoreToStr(stmts[0])
		assert.Contains(t, r, "WITH", "Restore must include WITH")
		assert.Contains(t, r, "SELECT", "Restore must include SELECT")
	})

	t.Run("select-full-restore", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT DISTINCT a FROM t ORDER BY a DESC LIMIT 10 OFFSET 5")
		if stmts == nil {
			return
		}
		r := restoreToStr(stmts[0])
		assert.Contains(t, r, "DISTINCT", "Restore must preserve DISTINCT")
		assert.Contains(t, r, "ORDER BY", "Restore must preserve ORDER BY")
		assert.Contains(t, r, "LIMIT", "Restore must preserve LIMIT")
	})

	t.Run("window-func-roundtrip", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) FROM t")
		if stmts == nil {
			return
		}
		r := restoreToStr(stmts[0])
		assert.Contains(t, r, "OVER", "Restore must preserve OVER")
		assert.Contains(t, r, "PARTITION BY", "Restore must preserve PARTITION BY")
	})

	t.Run("load-data-ignore-lines", func(t *testing.T) {
		stmts := parseForTest(t, "LOAD DATA INFILE '/t.csv' INTO TABLE t IGNORE 5 LINES")
		if stmts == nil {
			return
		}
		ld := stmts[0].(*ast.LoadDataStmt)
		if assert.NotNil(t, ld.IgnoreLines, "LoadDataStmt.IgnoreLines ptr must be non-nil") {
			assert.Equal(t, uint64(5), *ld.IgnoreLines, "LoadDataStmt.IgnoreLines must be 5")
		}
	})

	t.Run("prepare-from-string", func(t *testing.T) {
		stmts := parseForTest(t, "PREPARE s FROM 'SELECT 1'")
		if stmts == nil {
			return
		}
		p := stmts[0].(*ast.PrepareStmt)
		assert.Equal(t, "s", p.Name, "PrepareStmt.Name must be 's'")
		assert.NotNil(t, p.SQLText, "PrepareStmt.SQLText must be set for string form")
	})
}
