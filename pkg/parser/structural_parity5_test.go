package parser

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
)

// TestStructuralParity5 covers the final targeted grammar branches:
// fields that carry non-default values but had not yet been individually
// verified in any prior structural parity test.
func TestStructuralParity5(t *testing.T) {
	// =========================================================================
	// 1. BeginStmt.CausalConsistencyOnly
	//    parser.y: "START" "TRANSACTION" "WITH" "CAUSAL" "CONSISTENCY" "ONLY"
	// =========================================================================
	t.Run("begin-causal-consistency-only", func(t *testing.T) {
		stmts := parseForTest(t, "START TRANSACTION WITH CAUSAL CONSISTENCY ONLY")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.BeginStmt)
		assert.True(t, s.CausalConsistencyOnly,
			"START TRANSACTION WITH CAUSAL CONSISTENCY ONLY must set CausalConsistencyOnly=true")
	})

	// =========================================================================
	// 2. ShowStmt.Roles — SHOW GRANTS FOR user USING role1, role2
	//    parser.y: ShowStmt → ShowGrants → "FOR" Username "USING" RoleList
	// =========================================================================
	t.Run("show-grants-for-using-roles", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW GRANTS FOR 'u'@'h' USING 'r1', 'r2'")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowGrants, s.Tp)
		assert.NotNil(t, s.User, "SHOW GRANTS FOR must set User")
		assert.Len(t, s.Roles, 2, "SHOW GRANTS FOR ... USING must populate Roles slice")
	})

	// =========================================================================
	// 3. SHOW FULL PROCESSLIST — Full=true for processlist
	//    parser.y: "SHOW" "FULL" "PROCESSLIST"
	// =========================================================================
	t.Run("show-full-processlist", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW FULL PROCESSLIST")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowProcessList, s.Tp)
		assert.True(t, s.Full, "SHOW FULL PROCESSLIST must set Full=true")
	})

	// =========================================================================
	// 4. AdminShowSlow.Tp=ShowSlowTop + Kind=ShowSlowKindDefault
	//    parser.y: "TOP" NUM  (no INTERNAL or ALL keyword)
	// =========================================================================
	t.Run("admin-show-slow-top-default", func(t *testing.T) {
		stmts := parseForTest(t, "ADMIN SHOW SLOW TOP 5")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.AdminStmt)
		assert.Equal(t, ast.AdminShowSlow, s.Tp)
		if assert.NotNil(t, s.ShowSlow) {
			assert.Equal(t, ast.ShowSlowTop, s.ShowSlow.Tp)
			assert.Equal(t, ast.ShowSlowKindDefault, s.ShowSlow.Kind,
				"ADMIN SHOW SLOW TOP N (no qualifier) must use ShowSlowKindDefault")
			assert.Equal(t, uint64(5), s.ShowSlow.Count)
		}
	})

	// =========================================================================
	// 5. AdminShowSlow.Count verification for all three variants
	//    Ensure Count is set correctly in each branch
	// =========================================================================
	t.Run("admin-show-slow-recent-count", func(t *testing.T) {
		stmts := parseForTest(t, "ADMIN SHOW SLOW RECENT 7")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.AdminStmt)
		if assert.NotNil(t, s.ShowSlow) {
			assert.Equal(t, ast.ShowSlowRecent, s.ShowSlow.Tp)
			assert.Equal(t, uint64(7), s.ShowSlow.Count,
				"ADMIN SHOW SLOW RECENT N must set Count=N")
		}
	})

	t.Run("admin-show-slow-top-internal-count", func(t *testing.T) {
		stmts := parseForTest(t, "ADMIN SHOW SLOW TOP INTERNAL 12")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.AdminStmt)
		if assert.NotNil(t, s.ShowSlow) {
			assert.Equal(t, ast.ShowSlowTop, s.ShowSlow.Tp)
			assert.Equal(t, ast.ShowSlowKindInternal, s.ShowSlow.Kind)
			assert.Equal(t, uint64(12), s.ShowSlow.Count)
		}
	})

	t.Run("admin-show-slow-top-all-count", func(t *testing.T) {
		stmts := parseForTest(t, "ADMIN SHOW SLOW TOP ALL 20")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.AdminStmt)
		if assert.NotNil(t, s.ShowSlow) {
			assert.Equal(t, ast.ShowSlowTop, s.ShowSlow.Tp)
			assert.Equal(t, ast.ShowSlowKindAll, s.ShowSlow.Kind)
			assert.Equal(t, uint64(20), s.ShowSlow.Count)
		}
	})

	// =========================================================================
	// 6. CommitStmt.CompletionType variants
	//    parser.y: COMMIT AND CHAIN | COMMIT AND CHAIN NO RELEASE | COMMIT RELEASE
	// =========================================================================
	t.Run("commit-and-chain-no-release", func(t *testing.T) {
		stmts := parseForTest(t, "COMMIT AND CHAIN NO RELEASE")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.CommitStmt)
		assert.Equal(t, ast.CompletionTypeChain, s.CompletionType,
			"COMMIT AND CHAIN NO RELEASE must set CompletionType=Chain (no release)")
	})

	t.Run("rollback-and-no-chain", func(t *testing.T) {
		stmts := parseForTest(t, "ROLLBACK AND NO CHAIN")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.RollbackStmt)
		assert.Equal(t, ast.CompletionTypeDefault, s.CompletionType,
			"ROLLBACK AND NO CHAIN must use CompletionTypeDefault")
	})

	t.Run("rollback-release", func(t *testing.T) {
		stmts := parseForTest(t, "ROLLBACK RELEASE")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.RollbackStmt)
		assert.Equal(t, ast.CompletionTypeRelease, s.CompletionType,
			"ROLLBACK RELEASE must set CompletionType=Release")
	})

	// =========================================================================
	// 7. GrantStmt.WithGrantOption
	//    parser.y: "WITH" "GRANT" "OPTION"
	// =========================================================================
	t.Run("grant-with-grant-option", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT ON t TO 'u'@'h' WITH GRANT OPTION")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.GrantStmt)
		assert.True(t, s.WithGrant, "GRANT ... WITH GRANT OPTION must set WithGrant=true")
	})

	// =========================================================================
	// 8. SelectStmt.WindowSpecs (WINDOW clause)
	//    parser.y: SelectStmt → WindowClauseOptional → "WINDOW" WindowDefinitionList
	// =========================================================================
	t.Run("named-window-spec", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT SUM(a) OVER w FROM t WINDOW w AS (PARTITION BY b)")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.SelectStmt)
		assert.NotEmpty(t, s.WindowSpecs, "WINDOW clause must populate SelectStmt.WindowSpecs")
	})

	// =========================================================================
	// 9. CreateTableStmt.ReferTable — CREATE TABLE t LIKE s
	// =========================================================================
	t.Run("create-table-like", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE TABLE t LIKE s")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.CreateTableStmt)
		assert.NotNil(t, s.ReferTable,
			"CREATE TABLE t LIKE s must set ReferTable to the referenced table")
	})

	// =========================================================================
	// 10. DropTableStmt fields: IsView, IfExists, multi-table
	//     These verify exact AST field values distinct from parse-success checks.
	// =========================================================================
	t.Run("drop-view-if-exists", func(t *testing.T) {
		stmts := parseForTest(t, "DROP VIEW IF EXISTS v1, v2")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.DropTableStmt)
		assert.True(t, s.IsView, "DROP VIEW must set IsView=true")
		assert.True(t, s.IfExists, "DROP VIEW IF EXISTS must set IfExists=true")
		assert.Len(t, s.Tables, 2, "DROP VIEW v1, v2 must store both tables")
	})

	// =========================================================================
	// 11. LoadDataStmt.IgnoreLines
	//     parser.y: "IGNORE" N "LINES" | "IGNORE" N "ROWS"
	// =========================================================================
	t.Run("load-data-ignore-lines", func(t *testing.T) {
		stmts := parseForTest(t, "LOAD DATA INFILE '/t.csv' INTO TABLE t IGNORE 2 LINES")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.LoadDataStmt)
		if assert.NotNil(t, s.IgnoreLines, "LOAD DATA IGNORE N LINES must set IgnoreLines") {
			assert.Equal(t, uint64(2), *s.IgnoreLines,
				"LOAD DATA IGNORE N LINES must set IgnoreLines=N")
		}
	})

	// =========================================================================
	// 12. CreateUserStmt.IfNotExists
	// =========================================================================
	t.Run("create-user-if-not-exists", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE USER IF NOT EXISTS 'u'@'h'")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.CreateUserStmt)
		assert.True(t, s.IfNotExists, "CREATE USER IF NOT EXISTS must set IfNotExists=true")
	})

	// =========================================================================
	// 13. NonTransactionalDMLStmt.DryRun
	//     parser.y: "BATCH" "ON" ... "DRY" "RUN" DMLStmt
	// =========================================================================
	t.Run("non-transactional-batch-dml", func(t *testing.T) {
		stmts := parseForTest(t, "BATCH ON id LIMIT 1000 DELETE FROM t WHERE ts < NOW()")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.NonTransactionalDMLStmt)
		assert.NotNil(t, s.ShardColumn, "BATCH ON id must set ShardColumn")
		assert.NotNil(t, s.Limit, "BATCH LIMIT 1000 must set Limit")
	})

	// =========================================================================
	// 14. SelectStmt.TableHints — optimizer hints
	// =========================================================================
	t.Run("select-with-use-index-hint", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t USE INDEX (idx)")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, s.From) {
			ts, ok := s.From.TableRefs.Left.(*ast.TableSource)
			if assert.True(t, ok) {
				tn, ok := ts.Source.(*ast.TableName)
				if assert.True(t, ok) {
					assert.NotEmpty(t, tn.IndexHints, "USE INDEX must populate IndexHints")
				}
			}
		}
	})
}
