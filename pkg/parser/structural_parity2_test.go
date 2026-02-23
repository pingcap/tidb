package parser

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
)

// TestASTStructuralParity2 is the second batch of field-level parity checks.
// Each sub-test picks a specific AST field that corresponds to a grammar rule
// in parser.y and asserts the HandParser sets it correctly.
func TestASTStructuralParity2(t *testing.T) {

	// ─── InsertStmt.Setlist ───────────────────────────────────────────────────
	// parser.y: InsertValues → "SET" ColumnSetValueList sets Setlist=true
	t.Run("insert-set-form-setlist-true", func(t *testing.T) {
		stmts := parseForTest(t, "INSERT INTO t SET a=1, b=2")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		assert.True(t, ins.Setlist, "InsertStmt.Setlist must be true for SET form")
		assert.NotEmpty(t, ins.Columns, "Columns must be populated for SET form")
		assert.NotEmpty(t, ins.Lists, "Lists must be populated for SET form")
		assert.Len(t, ins.Lists, 1, "SET form must produce a single row in Lists")
		assert.Len(t, ins.Columns, 2, "Must have 2 columns from SET a=1, b=2")
	})

	t.Run("insert-values-form-setlist-false", func(t *testing.T) {
		stmts := parseForTest(t, "INSERT INTO t VALUES (1, 2)")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		assert.False(t, ins.Setlist, "InsertStmt.Setlist must be false for VALUES form")
	})

	t.Run("replace-set-form-setlist-true", func(t *testing.T) {
		stmts := parseForTest(t, "REPLACE INTO t SET a=1")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		assert.True(t, ins.IsReplace, "InsertStmt.IsReplace must be true for REPLACE")
		assert.True(t, ins.Setlist, "InsertStmt.Setlist must be true for REPLACE...SET form")
	})

	// ─── SelectLockInfo.LockType ──────────────────────────────────────────────
	// All 8 parser.y SelectLockOpt variants
	t.Run("lock-for-update", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t FOR UPDATE")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForUpdate, sel.LockInfo.LockType)
		}
	})

	t.Run("lock-for-share", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t FOR SHARE")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForShare, sel.LockInfo.LockType)
		}
	})

	t.Run("lock-in-share-mode", func(t *testing.T) {
		// parser.y: "LOCK" "IN" "SHARE" "MODE" → LockType = SelectLockForShare
		stmts := parseForTest(t, "SELECT * FROM t LOCK IN SHARE MODE")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForShare, sel.LockInfo.LockType,
				"LOCK IN SHARE MODE must map to SelectLockForShare same as FOR SHARE")
		}
	})

	t.Run("lock-for-update-nowait", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t FOR UPDATE NOWAIT")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForUpdateNoWait, sel.LockInfo.LockType)
		}
	})

	t.Run("lock-for-share-nowait", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t FOR SHARE NOWAIT")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForShareNoWait, sel.LockInfo.LockType)
		}
	})

	t.Run("lock-for-update-skip-locked", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t FOR UPDATE SKIP LOCKED")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForUpdateSkipLocked, sel.LockInfo.LockType)
		}
	})

	t.Run("lock-for-share-skip-locked", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t FOR SHARE SKIP LOCKED")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForShareSkipLocked, sel.LockInfo.LockType)
		}
	})

	t.Run("lock-for-update-wait-n", func(t *testing.T) {
		// parser.y: "FOR" "UPDATE" OfTablesOpt "WAIT" NUM → SelectLockForUpdateWaitN
		stmts := parseForTest(t, "SELECT * FROM t FOR UPDATE WAIT 5")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForUpdateWaitN, sel.LockInfo.LockType)
			assert.Equal(t, uint64(5), sel.LockInfo.WaitSec)
		}
	})

	t.Run("lock-for-update-of-tables", func(t *testing.T) {
		// parser.y: "FOR" "UPDATE" OfTablesOpt → Tables = list of table names
		stmts := parseForTest(t, "SELECT * FROM t1, t2 FOR UPDATE OF t1")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Equal(t, ast.SelectLockForUpdate, sel.LockInfo.LockType)
			assert.Len(t, sel.LockInfo.Tables, 1, "OF t1 must produce 1 table in LockInfo.Tables")
		}
	})

	t.Run("lock-for-update-no-tables", func(t *testing.T) {
		// OfTablesOpt → empty → LockInfo.Tables = []
		stmts := parseForTest(t, "SELECT * FROM t FOR UPDATE")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.LockInfo) {
			assert.Empty(t, sel.LockInfo.Tables, "Simple FOR UPDATE must have empty Tables list")
		}
	})

	// ─── Join AST fields ──────────────────────────────────────────────────────
	// parser.y JoinTable variants
	t.Run("inner-join-type", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1 INNER JOIN t2 ON t1.id=t2.id")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		// INNER JOIN is CrossJoin in TiDB AST (CrossJoin = 0)
		assert.Equal(t, ast.CrossJoin, join.Tp, "INNER JOIN → CrossJoin type")
		assert.NotNil(t, join.On, "INNER JOIN ON must set Join.On")
	})

	t.Run("left-join-type", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1 LEFT JOIN t2 ON t1.id=t2.id")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		assert.Equal(t, ast.LeftJoin, join.Tp, "LEFT JOIN → LeftJoin type")
		assert.NotNil(t, join.On, "LEFT JOIN ON must set Join.On")
	})

	t.Run("right-join-type", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id=t2.id")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		assert.Equal(t, ast.RightJoin, join.Tp, "RIGHT JOIN → RightJoin type")
	})

	t.Run("join-using-clause", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1 JOIN t2 USING (id)")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		assert.NotEmpty(t, join.Using, "USING clause must set Join.Using")
		assert.Nil(t, join.On, "USING form must not set Join.On")
	})

	t.Run("natural-join-flag", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1 NATURAL JOIN t2")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		assert.True(t, join.NaturalJoin, "NATURAL JOIN must set Join.NaturalJoin=true")
	})

	t.Run("straight-join-flag", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1 STRAIGHT_JOIN t2")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		assert.True(t, join.StraightJoin, "STRAIGHT_JOIN must set Join.StraightJoin=true")
	})

	t.Run("cross-join-no-condition", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1 CROSS JOIN t2")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		assert.Equal(t, ast.CrossJoin, join.Tp, "CROSS JOIN → CrossJoin type")
		assert.Nil(t, join.On, "CROSS JOIN must not set Join.On")
		assert.Empty(t, join.Using, "CROSS JOIN must not set Join.Using")
	})

	t.Run("comma-join-is-cross-join", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t1, t2")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		join := sel.From.TableRefs
		// Comma join: also CrossJoin in the AST
		assert.Equal(t, ast.CrossJoin, join.Tp, "Comma join , must produce CrossJoin")
	})

	// ─── FETCH FIRST N ROWS ONLY ─────────────────────────────────────────────
	// parser.y: "FETCH" FirstOrNext FetchFirstOpt RowOrRows "ONLY"
	t.Run("fetch-first-rows-only", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t ORDER BY a FETCH FIRST 10 ROWS ONLY")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.Limit, "FETCH FIRST N ROWS ONLY must produce a Limit") {
			assert.NotNil(t, sel.Limit.Count, "Limit.Count must be set")
			assert.Nil(t, sel.Limit.Offset, "FETCH FIRST has no OFFSET")
		}
	})

	t.Run("fetch-next-row-only-default-1", func(t *testing.T) {
		// FETCH NEXT ROW ONLY → FetchFirstOpt is empty → defaults to 1
		stmts := parseForTest(t, "SELECT * FROM t ORDER BY a FETCH NEXT ROW ONLY")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.NotNil(t, sel.Limit, "FETCH NEXT ROW ONLY must produce a Limit")
	})

	// ─── LIMIT with offset formats ────────────────────────────────────────────
	t.Run("limit-offset-comma-format", func(t *testing.T) {
		// LIMIT offset, count (MySQL format)
		stmts := parseForTest(t, "SELECT * FROM t LIMIT 5, 10")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.Limit) {
			assert.NotNil(t, sel.Limit.Offset, "LIMIT 5, 10 must set Offset")
			assert.NotNil(t, sel.Limit.Count, "LIMIT 5, 10 must set Count")
		}
	})

	t.Run("limit-offset-keyword-format", func(t *testing.T) {
		// LIMIT count OFFSET offset (SQL standard format)
		stmts := parseForTest(t, "SELECT * FROM t LIMIT 10 OFFSET 5")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.Limit) {
			assert.NotNil(t, sel.Limit.Offset, "LIMIT 10 OFFSET 5 must set Offset")
			assert.NotNil(t, sel.Limit.Count, "LIMIT 10 OFFSET 5 must set Count")
		}
	})

	// ─── DeleteStmt fields ────────────────────────────────────────────────────
	t.Run("delete-single-table-no-is-multitable", func(t *testing.T) {
		stmts := parseForTest(t, "DELETE FROM t WHERE a=1")
		if stmts == nil {
			return
		}
		d := stmts[0].(*ast.DeleteStmt)
		assert.False(t, d.IsMultiTable, "single-table DELETE must not set IsMultiTable")
		assert.False(t, d.BeforeFrom, "single-table DELETE must not set BeforeFrom")
	})

	t.Run("delete-before-from-form", func(t *testing.T) {
		// DELETE t1, t2 FROM t1, t2 WHERE ... → BeforeFrom=true
		stmts := parseForTest(t, "DELETE t1, t2 FROM t1 JOIN t2 ON t1.id=t2.id WHERE t1.a=1")
		if stmts == nil {
			return
		}
		d := stmts[0].(*ast.DeleteStmt)
		assert.True(t, d.IsMultiTable, "multi-table DELETE must set IsMultiTable")
		assert.True(t, d.BeforeFrom, "DELETE tbl FROM ... must set BeforeFrom=true")
		assert.NotNil(t, d.Tables, "DELETE tbl FROM form must set Tables")
	})

	t.Run("delete-using-form-not-before-from", func(t *testing.T) {
		// DELETE FROM t1 USING t1, t2 WHERE ... → BeforeFrom=false
		stmts := parseForTest(t, "DELETE FROM t1 USING t1 JOIN t2 ON t1.id=t2.id WHERE t1.a=1")
		if stmts == nil {
			return
		}
		d := stmts[0].(*ast.DeleteStmt)
		assert.True(t, d.IsMultiTable, "USING-form DELETE must set IsMultiTable")
		assert.False(t, d.BeforeFrom, "DELETE FROM t USING form must set BeforeFrom=false")
	})

	t.Run("delete-with-order-and-limit", func(t *testing.T) {
		stmts := parseForTest(t, "DELETE FROM t ORDER BY a DESC LIMIT 5")
		if stmts == nil {
			return
		}
		d := stmts[0].(*ast.DeleteStmt)
		assert.NotNil(t, d.Order, "DELETE with ORDER BY must set Order")
		assert.NotNil(t, d.Limit, "DELETE with LIMIT must set Limit")
	})

	// ─── UpdateStmt: JOIN form sets MultipleTable AND handles ON expression ───
	t.Run("update-join-form-multitable", func(t *testing.T) {
		stmts := parseForTest(t, "UPDATE t1 INNER JOIN t2 ON t1.id=t2.id SET t1.a=t2.a WHERE t1.b=1")
		if stmts == nil {
			return
		}
		u := stmts[0].(*ast.UpdateStmt)
		assert.True(t, u.MultipleTable, "JOIN-form UPDATE must set MultipleTable=true")
	})

	t.Run("update-ignore-flag", func(t *testing.T) {
		stmts := parseForTest(t, "UPDATE IGNORE t SET a=1 WHERE b=2")
		if stmts == nil {
			return
		}
		u := stmts[0].(*ast.UpdateStmt)
		assert.True(t, u.IgnoreErr, "UPDATE IGNORE must set IgnoreErr=true")
	})

	// ─── InsertStmt.OnDuplicate ───────────────────────────────────────────────
	t.Run("insert-on-duplicate", func(t *testing.T) {
		stmts := parseForTest(t, "INSERT INTO t (a, b) VALUES (1, 2) ON DUPLICATE KEY UPDATE b=VALUES(b)")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		assert.NotEmpty(t, ins.OnDuplicate, "ON DUPLICATE KEY UPDATE must set OnDuplicate")
	})

	t.Run("insert-no-on-duplicate", func(t *testing.T) {
		stmts := parseForTest(t, "INSERT INTO t VALUES (1, 2)")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		assert.Empty(t, ins.OnDuplicate, "Without ON DUPLICATE, OnDuplicate must be empty")
	})

	// ─── SelectStmt.WithClause propagates to UNION ───────────────────────────
	t.Run("with-clause-on-setopr", func(t *testing.T) {
		// With clause in UNION context must be on the SetOprStmt.With
		stmts := parseForTest(t, "WITH a AS (SELECT 1) SELECT * FROM a UNION ALL SELECT * FROM a ORDER BY 1 LIMIT 5")
		if stmts == nil {
			return
		}
		switch s := stmts[0].(type) {
		case *ast.SetOprStmt:
			assert.NotNil(t, s.With, "SetOprStmt.With must be set for WITH...UNION")
		case *ast.SelectStmt:
			assert.NotNil(t, s.With, "SelectStmt.With must be set for WITH...SELECT")
		default:
			t.Fatalf("unexpected type %T", stmts[0])
		}
	})

	// ─── SelectStmt field: SQL_SMALL_RESULT / SQL_BIG_RESULT / SQL_BUFFER_RESULT ─
	t.Run("sql-small-result", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT SQL_SMALL_RESULT a FROM t GROUP BY a")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.True(t, sel.SelectStmtOpts.SQLSmallResult, "SQLSmallResult must be set")
	})

	t.Run("sql-big-result", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT SQL_BIG_RESULT a FROM t GROUP BY a")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.True(t, sel.SelectStmtOpts.SQLBigResult, "SQLBigResult must be set")
	})

	t.Run("sql-buffer-result", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT SQL_BUFFER_RESULT a FROM t")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		assert.True(t, sel.SelectStmtOpts.SQLBufferResult, "SQLBufferResult must be set")
	})

	// ─── GROUP BY WITH ROLLUP ────────────────────────────────────────────────
	t.Run("group-by-rollup", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT a, SUM(b) FROM t GROUP BY a WITH ROLLUP")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.GroupBy) {
			assert.True(t, sel.GroupBy.Rollup, "GROUP BY WITH ROLLUP must set GroupBy.Rollup=true")
		}
	})

	t.Run("group-by-no-rollup", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT a, SUM(b) FROM t GROUP BY a")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.GroupBy) {
			assert.False(t, sel.GroupBy.Rollup, "GROUP BY without ROLLUP must have Rollup=false")
		}
	})

	// ─── ORDER BY item: Desc flag ─────────────────────────────────────────────
	t.Run("order-by-desc-flag", func(t *testing.T) {
		stmts := parseForTest(t, "SELECT * FROM t ORDER BY a ASC, b DESC")
		if stmts == nil {
			return
		}
		sel := stmts[0].(*ast.SelectStmt)
		if assert.NotNil(t, sel.OrderBy) && assert.Len(t, sel.OrderBy.Items, 2) {
			assert.False(t, sel.OrderBy.Items[0].Desc, "ASC item must have Desc=false")
			assert.True(t, sel.OrderBy.Items[1].Desc, "DESC item must have Desc=true")
		}
	})

	// ─── ExplainStmt: all formats produce correct Format string ──────────────
	t.Run("explain-format-row", func(t *testing.T) {
		stmts := parseForTest(t, "EXPLAIN FORMAT='ROW' SELECT * FROM t")
		if stmts == nil {
			return
		}
		exp := stmts[0].(*ast.ExplainStmt)
		assert.Equal(t, "ROW", exp.Format)
	})

	t.Run("explain-format-dot", func(t *testing.T) {
		stmts := parseForTest(t, "EXPLAIN FORMAT='DOT' SELECT * FROM t")
		if stmts == nil {
			return
		}
		exp := stmts[0].(*ast.ExplainStmt)
		assert.Equal(t, "DOT", exp.Format)
	})

	t.Run("explain-format-brief", func(t *testing.T) {
		stmts := parseForTest(t, "EXPLAIN FORMAT=BRIEF SELECT * FROM t")
		if stmts == nil {
			return
		}
		exp := stmts[0].(*ast.ExplainStmt)
		assert.Equal(t, "BRIEF", exp.Format)
	})

	// ─── PrepareStmt forms ────────────────────────────────────────────────────
	t.Run("prepare-from-user-var", func(t *testing.T) {
		stmts := parseForTest(t, "PREPARE s FROM @sql_text")
		if stmts == nil {
			return
		}
		p := stmts[0].(*ast.PrepareStmt)
		assert.NotNil(t, p.SQLVar, "PrepareStmt.SQLVar must be set for FROM @var form")
		// SQLText is a plain string (not *string), so it stays as empty string ""
		// when the @var form is used — this matches parser.y exactly:
		// sqlVar is set, sqlText stays at zero value "".
		assert.Empty(t, p.SQLText, "PrepareStmt.SQLText must be empty for @var form")
	})

	t.Run("prepare-from-string-literal", func(t *testing.T) {
		stmts := parseForTest(t, "PREPARE s FROM 'SELECT 1+1'")
		if stmts == nil {
			return
		}
		p := stmts[0].(*ast.PrepareStmt)
		assert.Nil(t, p.SQLVar, "PrepareStmt.SQLVar must be nil for string literal form")
		assert.NotNil(t, p.SQLText, "PrepareStmt.SQLText must be set for string literal form")
	})

	// ─── BeginStmt: ReadOnly and CausalConsistencyOnly ───────────────────────
	t.Run("begin-read-only", func(t *testing.T) {
		stmts := parseForTest(t, "START TRANSACTION READ ONLY")
		if stmts == nil {
			return
		}
		b := stmts[0].(*ast.BeginStmt)
		assert.True(t, b.AsOf == nil || !b.ReadOnly == false, "START TRANSACTION READ ONLY - check ReadOnly")
		// More precise check:
		assert.True(t, b.ReadOnly, "BeginStmt.ReadOnly must be true")
	})

	t.Run("begin-read-write", func(t *testing.T) {
		stmts := parseForTest(t, "START TRANSACTION READ WRITE")
		if stmts == nil {
			return
		}
		b := stmts[0].(*ast.BeginStmt)
		assert.False(t, b.ReadOnly, "START TRANSACTION READ WRITE must have ReadOnly=false")
	})

	t.Run("begin-causal-consistency", func(t *testing.T) {
		stmts := parseForTest(t, "START TRANSACTION WITH CAUSAL CONSISTENCY ONLY")
		if stmts == nil {
			return
		}
		b := stmts[0].(*ast.BeginStmt)
		assert.True(t, b.CausalConsistencyOnly, "BeginStmt.CausalConsistencyOnly must be true")
	})

	// ─── SubSelect preserves IsInBraces on inner SelectStmt ─────────────────
	t.Run("insert-subselect-is-in-braces", func(t *testing.T) {
		// INSERT INTO t (SELECT * FROM s) → inner select has IsInBraces=true
		stmts := parseForTest(t, "INSERT INTO t (SELECT * FROM s)")
		if stmts == nil {
			return
		}
		ins := stmts[0].(*ast.InsertStmt)
		if assert.NotNil(t, ins.Select, "InsertStmt.Select must be set") {
			if sel, ok := ins.Select.(*ast.SelectStmt); ok {
				assert.True(t, sel.IsInBraces, "Parenthesized INSERT SELECT must have IsInBraces=true")
			}
		}
	})

	// ─── CreateTable: IfNotExists ─────────────────────────────────────────────
	t.Run("create-table-if-not-exists", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE TABLE IF NOT EXISTS t (a INT)")
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		assert.True(t, c.IfNotExists, "CreateTableStmt.IfNotExists must be true")
	})

	t.Run("create-table-no-if-not-exists", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE TABLE t (a INT)")
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		assert.False(t, c.IfNotExists, "CreateTableStmt.IfNotExists must be false")
	})

	// ─── DropTableStmt fields ─────────────────────────────────────────────────
	t.Run("drop-table-if-exists", func(t *testing.T) {
		stmts := parseForTest(t, "DROP TABLE IF EXISTS t1, t2")
		if stmts == nil {
			return
		}
		d := stmts[0].(*ast.DropTableStmt)
		assert.True(t, d.IfExists, "DropTableStmt.IfExists must be true")
		assert.Len(t, d.Tables, 2, "Must have 2 tables in DropTableStmt")
	})

	t.Run("drop-view-not-table", func(t *testing.T) {
		stmts := parseForTest(t, "DROP VIEW v1")
		if stmts == nil {
			return
		}
		d := stmts[0].(*ast.DropTableStmt)
		assert.True(t, d.IsView, "DROP VIEW must set IsView=true")
	})

	// ─── AlterTableStmt: single vs multiple actions ───────────────────────────
	t.Run("alter-table-multi-actions", func(t *testing.T) {
		stmts := parseForTest(t, "ALTER TABLE t ADD COLUMN a INT, DROP COLUMN b, ADD INDEX idx (c)")
		if stmts == nil {
			return
		}
		a := stmts[0].(*ast.AlterTableStmt)
		assert.Len(t, a.Specs, 3, "ALTER TABLE with 3 actions must have 3 Specs")
	})

	// ─── RollbackStmt and CommitStmt ─────────────────────────────────────────
	t.Run("rollback-to-savepoint", func(t *testing.T) {
		stmts := parseForTest(t, "ROLLBACK TO SAVEPOINT sp1")
		if stmts == nil {
			return
		}
		r := stmts[0].(*ast.RollbackStmt)
		assert.Equal(t, "sp1", r.SavepointName, "RollbackStmt.SavepointName must be sp1")
	})

	t.Run("savepoint", func(t *testing.T) {
		stmts := parseForTest(t, "SAVEPOINT sp1")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.SavepointStmt)
		assert.Equal(t, "sp1", s.Name, "SavepointStmt.Name must be sp1")
	})

	t.Run("release-savepoint", func(t *testing.T) {
		stmts := parseForTest(t, "RELEASE SAVEPOINT sp1")
		if stmts == nil {
			return
		}
		r := stmts[0].(*ast.ReleaseSavepointStmt)
		assert.Equal(t, "sp1", r.Name, "ReleaseSavepointStmt.Name must be sp1")
	})

	// ─── UseStmt ──────────────────────────────────────────────────────────────
	t.Run("use-stmt-db-name", func(t *testing.T) {
		stmts := parseForTest(t, "USE mydb")
		if stmts == nil {
			return
		}
		u := stmts[0].(*ast.UseStmt)
		assert.Equal(t, "mydb", u.DBName, "UseStmt.DBName must be mydb")
	})
}
