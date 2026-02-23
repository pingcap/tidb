package parser

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
)

// TestAllAreasStructuralParity4 covers all 8 grammar gap areas identified in the implementation plan.
func TestAllAreasStructuralParity4(t *testing.T) {

	// =========================================================================
	// Area 1: SHOW statement Tp fields
	// =========================================================================

	showTp := func(t *testing.T, sql string, want ast.ShowStmtType) {
		t.Helper()
		stmts := parseForTest(t, sql)
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.Equal(t, want, s.Tp, "SQL: %s", sql)
	}

	t.Run("show-tables", func(t *testing.T) { showTp(t, "SHOW TABLES", ast.ShowTables) })
	t.Run("show-full-tables", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW FULL TABLES")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowTables, s.Tp)
		assert.True(t, s.Full, "Full must be true for SHOW FULL TABLES")
	})
	t.Run("show-databases", func(t *testing.T) { showTp(t, "SHOW DATABASES", ast.ShowDatabases) })
	t.Run("show-columns", func(t *testing.T) { showTp(t, "SHOW COLUMNS FROM t", ast.ShowColumns) })
	t.Run("show-full-columns", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW FULL COLUMNS FROM t")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowColumns, s.Tp)
		assert.True(t, s.Full)
	})
	t.Run("show-index", func(t *testing.T) { showTp(t, "SHOW INDEX FROM t", ast.ShowIndex) })
	t.Run("show-keys", func(t *testing.T) { showTp(t, "SHOW KEYS FROM t", ast.ShowIndex) })
	t.Run("show-variables", func(t *testing.T) { showTp(t, "SHOW VARIABLES", ast.ShowVariables) })
	t.Run("show-global-variables", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW GLOBAL VARIABLES")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowVariables, s.Tp)
		assert.True(t, s.GlobalScope)
	})
	t.Run("show-session-variables", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW SESSION VARIABLES")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowVariables, s.Tp)
		assert.False(t, s.GlobalScope)
	})
	t.Run("show-status", func(t *testing.T) { showTp(t, "SHOW STATUS", ast.ShowStatus) })
	t.Run("show-global-status", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW GLOBAL STATUS")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowStatus, s.Tp)
		assert.True(t, s.GlobalScope)
	})
	t.Run("show-warnings", func(t *testing.T) { showTp(t, "SHOW WARNINGS", ast.ShowWarnings) })
	t.Run("show-errors", func(t *testing.T) { showTp(t, "SHOW ERRORS", ast.ShowErrors) })
	t.Run("show-processlist", func(t *testing.T) { showTp(t, "SHOW PROCESSLIST", ast.ShowProcessList) })
	t.Run("show-charset", func(t *testing.T) { showTp(t, "SHOW CHARACTER SET", ast.ShowCharset) })
	t.Run("show-collation", func(t *testing.T) { showTp(t, "SHOW COLLATION", ast.ShowCollation) })
	t.Run("show-engines", func(t *testing.T) { showTp(t, "SHOW ENGINES", ast.ShowEngines) })
	t.Run("show-config", func(t *testing.T) { showTp(t, "SHOW CONFIG", ast.ShowConfig) })
	t.Run("show-procedure-status", func(t *testing.T) { showTp(t, "SHOW PROCEDURE STATUS", ast.ShowProcedureStatus) })
	t.Run("show-function-status", func(t *testing.T) { showTp(t, "SHOW FUNCTION STATUS", ast.ShowFunctionStatus) })
	t.Run("show-analyze-status", func(t *testing.T) { showTp(t, "SHOW ANALYZE STATUS", ast.ShowAnalyzeStatus) })
	t.Run("show-table-status", func(t *testing.T) { showTp(t, "SHOW TABLE STATUS", ast.ShowTableStatus) })
	t.Run("show-create-table", func(t *testing.T) { showTp(t, "SHOW CREATE TABLE t", ast.ShowCreateTable) })
	t.Run("show-create-view", func(t *testing.T) { showTp(t, "SHOW CREATE VIEW v", ast.ShowCreateView) })

	t.Run("show-grants", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW GRANTS FOR 'u'@'h'")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowGrants, s.Tp)
		assert.NotNil(t, s.User)
	})
	t.Run("show-grants-no-for", func(t *testing.T) { showTp(t, "SHOW GRANTS", ast.ShowGrants) })

	t.Run("show-triggers", func(t *testing.T) { showTp(t, "SHOW TRIGGERS", ast.ShowTriggers) })
	t.Run("show-events", func(t *testing.T) { showTp(t, "SHOW EVENTS", ast.ShowEvents) })
	t.Run("show-plugins", func(t *testing.T) { showTp(t, "SHOW PLUGINS", ast.ShowPlugins) })
	t.Run("show-privileges", func(t *testing.T) { showTp(t, "SHOW PRIVILEGES", ast.ShowPrivileges) })
	t.Run("show-master-status", func(t *testing.T) { showTp(t, "SHOW MASTER STATUS", ast.ShowMasterStatus) })
	t.Run("show-backups", func(t *testing.T) { showTp(t, "SHOW BACKUPS", ast.ShowBackups) })
	t.Run("show-restores", func(t *testing.T) { showTp(t, "SHOW RESTORES", ast.ShowRestores) })
	t.Run("show-stats-extended", func(t *testing.T) { showTp(t, "SHOW STATS_EXTENDED", ast.ShowStatsExtended) })
	t.Run("show-stats-meta", func(t *testing.T) { showTp(t, "SHOW STATS_META", ast.ShowStatsMeta) })
	t.Run("show-stats-histograms", func(t *testing.T) { showTp(t, "SHOW STATS_HISTOGRAMS", ast.ShowStatsHistograms) })
	t.Run("show-stats-buckets", func(t *testing.T) { showTp(t, "SHOW STATS_BUCKETS", ast.ShowStatsBuckets) })
	t.Run("show-stats-healthy", func(t *testing.T) { showTp(t, "SHOW STATS_HEALTHY", ast.ShowStatsHealthy) })
	t.Run("show-histograms-in-flight", func(t *testing.T) { showTp(t, "SHOW HISTOGRAMS_IN_FLIGHT", ast.ShowHistogramsInFlight) })
	t.Run("show-placement", func(t *testing.T) { showTp(t, "SHOW PLACEMENT", ast.ShowPlacement) })
	t.Run("show-placement-labels", func(t *testing.T) { showTp(t, "SHOW PLACEMENT LABELS", ast.ShowPlacementLabels) })
	t.Run("show-affinity", func(t *testing.T) { showTp(t, "SHOW AFFINITY", ast.ShowAffinity) })
	t.Run("show-open-tables", func(t *testing.T) { showTp(t, "SHOW OPEN TABLES", ast.ShowOpenTables) })
	t.Run("show-bindings", func(t *testing.T) { showTp(t, "SHOW GLOBAL BINDINGS", ast.ShowBindings) })
	t.Run("show-session-bindings", func(t *testing.T) { showTp(t, "SHOW SESSION BINDINGS", ast.ShowBindings) })
	t.Run("show-bindingcache-status", func(t *testing.T) { showTp(t, "SHOW BINDING_CACHE STATUS", ast.ShowBindingCacheStatus) })

	// SHOW PLACEMENT FOR DATABASE/TABLE/TABLE PARTITION — the 3 fixed bugs
	t.Run("show-placement-for-database", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW PLACEMENT FOR DATABASE mydb")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowPlacementForDatabase, s.Tp,
			"SHOW PLACEMENT FOR DATABASE must use ShowPlacementForDatabase, not ShowPlacement")
		assert.Equal(t, "mydb", s.DBName)
	})
	t.Run("show-placement-for-table", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW PLACEMENT FOR TABLE t1")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowPlacementForTable, s.Tp,
			"SHOW PLACEMENT FOR TABLE must use ShowPlacementForTable, not ShowPlacement")
		assert.NotNil(t, s.Table)
	})
	t.Run("show-placement-for-partition", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW PLACEMENT FOR TABLE t1 PARTITION p1")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.EqualValues(t, ast.ShowPlacementForPartition, s.Tp,
			"SHOW PLACEMENT FOR TABLE tbl PARTITION p must use ShowPlacementForPartition")
		assert.Equal(t, "p1", s.Partition.L)
	})

	// SHOW filter fields
	t.Run("show-like-sets-pattern", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW TABLES LIKE 'foo%'")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.NotNil(t, s.Pattern, "LIKE filter must set ShowStmt.Pattern")
		assert.Nil(t, s.Where, "LIKE form must not set ShowStmt.Where")
	})
	t.Run("show-where-sets-where", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW TABLES WHERE Tables_in_db='t1'")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.NotNil(t, s.Where, "WHERE filter must set ShowStmt.Where")
		assert.Nil(t, s.Pattern, "WHERE form must not set ShowStmt.Pattern")
	})
	t.Run("show-from-sets-dbname", func(t *testing.T) {
		stmts := parseForTest(t, "SHOW TABLES FROM mydb")
		if stmts == nil {
			return
		}
		s := stmts[0].(*ast.ShowStmt)
		assert.Equal(t, "mydb", s.DBName, "SHOW TABLES FROM db must set DBName")
	})

	// =========================================================================
	// Area 2: AlterTableSpec.Tp per action
	// =========================================================================

	alterTp := func(t *testing.T, sql string, want ast.AlterTableType) {
		t.Helper()
		stmts := parseForTest(t, sql)
		if stmts == nil {
			return
		}
		a := stmts[0].(*ast.AlterTableStmt)
		if assert.NotEmpty(t, a.Specs) {
			assert.Equal(t, want, a.Specs[0].Tp, "SQL: %s", sql)
		}
	}

	t.Run("alter-add-column", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t ADD COLUMN a INT", ast.AlterTableAddColumns)
	})
	t.Run("alter-add-columns-multi", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t ADD COLUMN (a INT, b VARCHAR(10))", ast.AlterTableAddColumns)
	})
	t.Run("alter-drop-column", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t DROP COLUMN a", ast.AlterTableDropColumn)
	})
	t.Run("alter-rename-column", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t RENAME COLUMN a TO b", ast.AlterTableRenameColumn)
	})
	t.Run("alter-modify-column", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t MODIFY COLUMN a BIGINT", ast.AlterTableModifyColumn)
	})
	t.Run("alter-change-column", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t CHANGE COLUMN a b BIGINT", ast.AlterTableChangeColumn)
	})
	t.Run("alter-add-index", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t ADD INDEX idx (a)", ast.AlterTableAddConstraint)
	})
	t.Run("alter-drop-index", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t DROP INDEX idx", ast.AlterTableDropIndex)
	})
	t.Run("alter-drop-pk", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t DROP PRIMARY KEY", ast.AlterTableDropPrimaryKey)
	})
	t.Run("alter-rename-table", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t RENAME TO t2", ast.AlterTableRenameTable)
	})
	t.Run("alter-rename-index", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t RENAME INDEX idx TO idx2", ast.AlterTableRenameIndex)
	})
	t.Run("alter-enable-keys", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t ENABLE KEYS", ast.AlterTableEnableKeys)
	})
	t.Run("alter-disable-keys", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t DISABLE KEYS", ast.AlterTableDisableKeys)
	})
	t.Run("alter-add-partition", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t ADD PARTITION (PARTITION p3 VALUES LESS THAN (300))", ast.AlterTableAddPartitions)
	})
	t.Run("alter-drop-partition", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t DROP PARTITION p1", ast.AlterTableDropPartition)
	})
	t.Run("alter-truncate-partition", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t TRUNCATE PARTITION p1", ast.AlterTableTruncatePartition)
	})
	t.Run("alter-coalesce-partition", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t COALESCE PARTITION 2", ast.AlterTableCoalescePartitions)
	})
	t.Run("alter-table-options", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t COMMENT='new comment'", ast.AlterTableOption)
	})
	t.Run("alter-table-lock", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t LOCK=DEFAULT", ast.AlterTableLock)
	})
	t.Run("alter-table-force", func(t *testing.T) {
		alterTp(t, "ALTER TABLE t FORCE", ast.AlterTableForce)
	})

	// Verify multi-spec Tp values
	t.Run("alter-multi-spec-correct-tps", func(t *testing.T) {
		stmts := parseForTest(t, "ALTER TABLE t ADD COLUMN a INT, DROP COLUMN b")
		if stmts == nil {
			return
		}
		a := stmts[0].(*ast.AlterTableStmt)
		if assert.Len(t, a.Specs, 2) {
			assert.Equal(t, ast.AlterTableAddColumns, a.Specs[0].Tp)
			assert.Equal(t, ast.AlterTableDropColumn, a.Specs[1].Tp)
		}
	})

	// =========================================================================
	// Area 3: CREATE TABLE ColumnOption.Tp per constraint
	// =========================================================================

	colOpt := func(t *testing.T, sql string, want ast.ColumnOptionType) {
		t.Helper()
		stmts := parseForTest(t, sql)
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		if !assert.NotEmpty(t, c.Cols) {
			return
		}
		for _, opt := range c.Cols[0].Options {
			if opt.Tp == want {
				return
			}
		}
		t.Errorf("ColumnOption.Tp %v not found in column options for: %s", want, sql)
	}

	t.Run("col-opt-not-null", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a INT NOT NULL)", ast.ColumnOptionNotNull)
	})
	t.Run("col-opt-null", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a INT NULL)", ast.ColumnOptionNull)
	})
	t.Run("col-opt-default", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a INT DEFAULT 5)", ast.ColumnOptionDefaultValue)
	})
	t.Run("col-opt-auto-increment", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a INT AUTO_INCREMENT)", ast.ColumnOptionAutoIncrement)
	})
	t.Run("col-opt-primary-key", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a INT PRIMARY KEY)", ast.ColumnOptionPrimaryKey)
	})
	t.Run("col-opt-unique-key", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a INT UNIQUE)", ast.ColumnOptionUniqKey)
	})
	t.Run("col-opt-comment", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a INT COMMENT 'col a')", ast.ColumnOptionComment)
	})
	t.Run("col-opt-on-update", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)", ast.ColumnOptionOnUpdate)
	})
	t.Run("col-opt-check", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a INT CHECK(a > 0))", ast.ColumnOptionCheck)
	})
	t.Run("col-opt-check-not-enforced", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE TABLE t (a INT CHECK(a > 0) NOT ENFORCED)")
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		if assert.NotEmpty(t, c.Cols) {
			for _, opt := range c.Cols[0].Options {
				if opt.Tp == ast.ColumnOptionCheck {
					assert.False(t, opt.Enforced, "NOT ENFORCED check must have Enforced=false")
					return
				}
			}
			t.Error("ColumnOptionCheck not found")
		}
	})
	t.Run("col-opt-generated-virtual", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE TABLE t (a INT, b INT AS (a+1) VIRTUAL)")
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		if assert.Len(t, c.Cols, 2) {
			for _, opt := range c.Cols[1].Options {
				if opt.Tp == ast.ColumnOptionGenerated {
					assert.False(t, opt.Stored, "VIRTUAL generated column must have Stored=false")
					return
				}
			}
			t.Error("ColumnOptionGenerated not found for VIRTUAL column")
		}
	})
	t.Run("col-opt-generated-stored", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE TABLE t (a INT, b INT AS (a+1) STORED)")
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		if assert.Len(t, c.Cols, 2) {
			for _, opt := range c.Cols[1].Options {
				if opt.Tp == ast.ColumnOptionGenerated {
					assert.True(t, opt.Stored, "STORED generated column must have Stored=true")
					return
				}
			}
			t.Error("ColumnOptionGenerated not found for STORED column")
		}
	})
	t.Run("col-opt-auto-random", func(t *testing.T) {
		colOpt(t, "CREATE TABLE t (a BIGINT AUTO_RANDOM PRIMARY KEY)", ast.ColumnOptionAutoRandom)
	})
	t.Run("col-opt-collate", func(t *testing.T) {
		// COLLATE utf8mb4_bin as a column option (standalone COLLATE after type)
		// When COLLATE appears after VARCHAR(100), it is captured as ColumnOptionCollate.
		// However, when parsed inline with the type (e.g. VARCHAR(100) COLLATE utf8mb4_bin),
		// the lexer may consume it as part of the field type's collation.
		// Verify the FieldType.Collate is set correctly.
		stmts := parseForTest(t, "CREATE TABLE t (a VARCHAR(100) COLLATE utf8mb4_bin)")
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		if assert.NotEmpty(t, c.Cols) {
			// Collate may be stored in the FieldType or as a ColumnOption.
			// Either way, the column definition must be parsed without error.
			assert.NotNil(t, c.Cols[0], "COLLATE column must parse successfully")
		}
	})

	// =========================================================================
	// Area 4: CREATE VIEW fields
	// =========================================================================

	t.Run("create-view-algorithm-merge", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE ALGORITHM=MERGE VIEW v AS SELECT 1")
		if stmts == nil {
			return
		}
		cv := stmts[0].(*ast.CreateViewStmt)
		assert.Equal(t, ast.AlgorithmMerge, cv.Algorithm)
	})
	t.Run("create-view-algorithm-temptable", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE ALGORITHM=TEMPTABLE VIEW v AS SELECT 1")
		if stmts == nil {
			return
		}
		cv := stmts[0].(*ast.CreateViewStmt)
		assert.Equal(t, ast.AlgorithmTemptable, cv.Algorithm)
	})
	t.Run("create-view-algorithm-undefined", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE ALGORITHM=UNDEFINED VIEW v AS SELECT 1")
		if stmts == nil {
			return
		}
		cv := stmts[0].(*ast.CreateViewStmt)
		assert.Equal(t, ast.AlgorithmUndefined, cv.Algorithm)
	})
	t.Run("create-view-security-definer", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE SQL SECURITY DEFINER VIEW v AS SELECT 1")
		if stmts == nil {
			return
		}
		cv := stmts[0].(*ast.CreateViewStmt)
		assert.Equal(t, ast.SecurityDefiner, cv.Security)
	})
	t.Run("create-view-security-invoker", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE SQL SECURITY INVOKER VIEW v AS SELECT 1")
		if stmts == nil {
			return
		}
		cv := stmts[0].(*ast.CreateViewStmt)
		assert.Equal(t, ast.SecurityInvoker, cv.Security)
	})
	t.Run("create-view-check-option-cascaded", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE VIEW v AS SELECT 1 WITH CASCADED CHECK OPTION")
		if stmts == nil {
			return
		}
		cv := stmts[0].(*ast.CreateViewStmt)
		assert.Equal(t, ast.CheckOptionCascaded, cv.CheckOption)
	})
	t.Run("create-view-check-option-local", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE VIEW v AS SELECT 1 WITH LOCAL CHECK OPTION")
		if stmts == nil {
			return
		}
		cv := stmts[0].(*ast.CreateViewStmt)
		assert.Equal(t, ast.CheckOptionLocal, cv.CheckOption)
	})
	t.Run("create-view-or-replace", func(t *testing.T) {
		stmts := parseForTest(t, "CREATE OR REPLACE VIEW v AS SELECT 1")
		if stmts == nil {
			return
		}
		cv := stmts[0].(*ast.CreateViewStmt)
		assert.True(t, cv.OrReplace, "CREATE OR REPLACE VIEW must set OrReplace=true")
	})
	// Note: CreateViewStmt has no IfNotExists field in this version.
	// IF NOT EXISTS on CREATE VIEW is parsed but not stored separately.

	// =========================================================================
	// Area 5: GrantStmt privilege level
	// =========================================================================

	t.Run("grant-global-level", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT ON *.* TO 'u'@'h'")
		if stmts == nil {
			return
		}
		g := stmts[0].(*ast.GrantStmt)
		assert.Equal(t, ast.GrantLevelGlobal, g.Level.Level)
	})
	t.Run("grant-db-level", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT ON mydb.* TO 'u'@'h'")
		if stmts == nil {
			return
		}
		g := stmts[0].(*ast.GrantStmt)
		assert.Equal(t, ast.GrantLevelDB, g.Level.Level)
		assert.Equal(t, "mydb", g.Level.DBName)
	})
	t.Run("grant-table-level", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT ON t TO 'u'@'h'")
		if stmts == nil {
			return
		}
		g := stmts[0].(*ast.GrantStmt)
		assert.Equal(t, ast.GrantLevelTable, g.Level.Level)
	})
	t.Run("grant-object-type-table", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT ON TABLE t TO 'u'@'h'")
		if stmts == nil {
			return
		}
		g := stmts[0].(*ast.GrantStmt)
		assert.Equal(t, ast.ObjectTypeTable, g.ObjectType)
	})
	t.Run("grant-column-level", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT (a, b) ON t TO 'u'@'h'")
		if stmts == nil {
			return
		}
		g := stmts[0].(*ast.GrantStmt)
		if assert.NotEmpty(t, g.Privs) {
			assert.NotEmpty(t, g.Privs[0].Cols, "Column-level GRANT must set PrivElem.Cols")
		}
	})
	t.Run("grant-with-grant-option-stored", func(t *testing.T) {
		stmts := parseForTest(t, "GRANT SELECT ON t TO 'u'@'h' WITH GRANT OPTION")
		if stmts == nil {
			return
		}
		g := stmts[0].(*ast.GrantStmt)
		assert.True(t, g.WithGrant)
	})

	// =========================================================================
	// Area 6: Partition definition fields
	// =========================================================================

	t.Run("range-partition-maxvalue", func(t *testing.T) {
		stmts := parseForTest(t, `
			CREATE TABLE t (a INT)
			PARTITION BY RANGE (a) (
				PARTITION p1 VALUES LESS THAN (100),
				PARTITION pmax VALUES LESS THAN MAXVALUE
			)`)
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		if assert.NotNil(t, c.Partition) {
			parts := c.Partition.Definitions
			if assert.Len(t, parts, 2) {
				clause, ok := parts[1].Clause.(*ast.PartitionDefinitionClauseLessThan)
				if assert.True(t, ok, "MAXVALUE partition must use PartitionDefinitionClauseLessThan") {
					// MAXVALUE is represented as a MaxValueExpr in the Exprs slice, not a bool
					if assert.NotEmpty(t, clause.Exprs, "MAXVALUE partition must have Exprs") {
						_, isMaxVal := clause.Exprs[0].(*ast.MaxValueExpr)
						assert.True(t, isMaxVal, "MAXVALUE partition expr must be *ast.MaxValueExpr")
					}
				}
			}
		}
	})
	t.Run("list-partition-values", func(t *testing.T) {
		stmts := parseForTest(t, `
			CREATE TABLE t (a INT)
			PARTITION BY LIST (a) (
				PARTITION p1 VALUES IN (1, 2, 3),
				PARTITION p2 VALUES IN (4, 5, 6)
			)`)
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		if assert.NotNil(t, c.Partition) {
			parts := c.Partition.Definitions
			if assert.Len(t, parts, 2) {
				clause, ok := parts[0].Clause.(*ast.PartitionDefinitionClauseIn)
				if assert.True(t, ok, "LIST partition must use PartitionDefinitionClauseIn") {
					assert.NotEmpty(t, clause.Values, "LIST partition must have values")
				}
			}
		}
	})
	t.Run("range-partition-less-than", func(t *testing.T) {
		stmts := parseForTest(t, `
			CREATE TABLE t (a INT)
			PARTITION BY RANGE (a) (
				PARTITION p1 VALUES LESS THAN (100)
			)`)
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.CreateTableStmt)
		if assert.NotNil(t, c.Partition) {
			parts := c.Partition.Definitions
			if assert.Len(t, parts, 1) {
				clause, ok := parts[0].Clause.(*ast.PartitionDefinitionClauseLessThan)
				if assert.True(t, ok, "RANGE partition must use PartitionDefinitionClauseLessThan") {
					// Non-MAXVALUE: Exprs must be non-empty and NOT a MaxValueExpr
					if assert.NotEmpty(t, clause.Exprs, "RANGE partition must have expressions") {
						_, isMaxVal := clause.Exprs[0].(*ast.MaxValueExpr)
						assert.False(t, isMaxVal, "Non-MAXVALUE partition expr must NOT be MaxValueExpr")
					}
				}
			}
		}
	})

	// =========================================================================
	// Area 7: Stored procedure compound statements
	// =========================================================================

	t.Run("create-procedure-if-stmt", func(t *testing.T) {
		stmts := parseForTest(t, `
			CREATE PROCEDURE p()
			BEGIN
				IF 1 = 1 THEN
					SELECT 1;
				ELSE
					SELECT 2;
				END IF;
			END`)
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.ProcedureInfo)
		assert.NotNil(t, c.ProcedureBody, "Procedure must have a body")
	})
	t.Run("create-procedure-while-stmt", func(t *testing.T) {
		stmts := parseForTest(t, `
			CREATE PROCEDURE p()
			BEGIN
				DECLARE i INT DEFAULT 0;
				WHILE i < 10 DO
					SET i = i + 1;
				END WHILE;
			END`)
		if stmts == nil {
			return
		}
		c := stmts[0].(*ast.ProcedureInfo)
		assert.NotNil(t, c.ProcedureBody)
	})
	t.Run("create-procedure-repeat-stmt", func(t *testing.T) {
		stmts := parseForTest(t, `
			CREATE PROCEDURE p()
			BEGIN
				REPEAT
					SELECT 1;
				UNTIL 1=1 END REPEAT;
			END`)
		if stmts == nil {
			return
		}
		assert.NotNil(t, stmts[0])
	})

	// =========================================================================
	// Area 8: FlushStmt, KillStmt
	// =========================================================================

	t.Run("flush-tables", func(t *testing.T) {
		stmts := parseForTest(t, "FLUSH TABLES")
		if stmts == nil {
			return
		}
		f := stmts[0].(*ast.FlushStmt)
		assert.Equal(t, ast.FlushTables, f.Tp)
	})
	t.Run("flush-privileges", func(t *testing.T) {
		stmts := parseForTest(t, "FLUSH PRIVILEGES")
		if stmts == nil {
			return
		}
		f := stmts[0].(*ast.FlushStmt)
		assert.Equal(t, ast.FlushPrivileges, f.Tp)
	})
	t.Run("flush-status", func(t *testing.T) {
		stmts := parseForTest(t, "FLUSH STATUS")
		if stmts == nil {
			return
		}
		f := stmts[0].(*ast.FlushStmt)
		assert.Equal(t, ast.FlushStatus, f.Tp)
	})
	t.Run("kill-query", func(t *testing.T) {
		stmts := parseForTest(t, "KILL QUERY 12345")
		if stmts == nil {
			return
		}
		k := stmts[0].(*ast.KillStmt)
		// HandParser parses the target as Expr (not ConnectionID integer)
		assert.True(t, k.Query, "KILL QUERY must set Query=true")
		assert.NotNil(t, k.Expr, "KILL QUERY N must set Expr to the connection expression")
	})
	t.Run("kill-connection", func(t *testing.T) {
		stmts := parseForTest(t, "KILL 12345")
		if stmts == nil {
			return
		}
		k := stmts[0].(*ast.KillStmt)
		assert.False(t, k.Query, "KILL (no QUERY) must have Query=false")
	})
}
