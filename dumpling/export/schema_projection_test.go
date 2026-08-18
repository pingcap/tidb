// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestPrepareColumnProjectionSchema(t *testing.T) {
	tctx, mock, baseConn := newMockDumpConn(t)
	conf := DefaultConfig()
	conf.Tables = NewDatabaseTables().AppendTables(database, []string{table, "plain"}, []uint64{0, 0})
	conf.columnFilter = newColumnFilterConfigForTest(t,
		columnFilterRule{Matcher: []string{database + "." + table}, Columns: []string{"id", "name"}},
	)

	mock.ExpectQuery("SHOW COLUMNS FROM").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "int(11)", "NO", "PRI", nil, "").
			AddRow("name", "varchar(12)", "YES", "", nil, "").
			AddRow("secret", "varchar(12)", "YES", "", nil, ""))
	mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(
		"SELECT `id`,`name`,`secret` FROM `%s`.`%s` LIMIT 1",
		database,
		table,
	))).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("id").OfType("INT", int64(0)),
		sqlmock.NewColumn("name").OfType("VARCHAR", ""),
		sqlmock.NewColumn("secret").OfType("VARCHAR", ""),
	).AddRow(1, "alice", "hidden"))
	mock.ExpectQuery("SHOW COLUMNS FROM").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "int(11)", "NO", "PRI", nil, ""))
	mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(
		"SELECT `id` FROM `%s`.`plain` LIMIT 1",
		database,
	))).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("id").OfType("INT", int64(0)),
	).AddRow(1))
	createSQL := "CREATE TABLE `test_table` (`id` INT PRIMARY KEY, `name` VARCHAR(12), `secret` VARCHAR(12))"
	mock.ExpectQuery("SHOW CREATE TABLE").
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(table, createSQL))
	plainCreateSQL := "CREATE TABLE `plain` (`id` INT PRIMARY KEY)"
	mock.ExpectQuery("SHOW CREATE TABLE").
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("plain", plainCreateSQL))

	require.NoError(t, prepareColumnProjection(tctx, conf, baseConn))

	meta, err := dumpTableMeta(tctx, conf, baseConn, database, &TableInfo{Type: TableTypeBase, Name: table})
	require.NoError(t, err)
	require.Contains(t, meta.ShowCreateTable(), "`id`")
	require.Contains(t, meta.ShowCreateTable(), "`name`")
	require.NotContains(t, meta.ShowCreateTable(), "`secret`")

	plainProjection := conf.columnProjection[tableName{db: database, table: "plain"}]
	require.Equal(t, plainCreateSQL, plainProjection.schemaSQL)
	plainMeta, err := dumpTableMeta(tctx, conf, baseConn, database, &TableInfo{Type: TableTypeBase, Name: "plain"})
	require.NoError(t, err)
	require.Equal(t, plainCreateSQL, plainMeta.ShowCreateTable())
	require.NoError(t, mock.ExpectationsWereMet())

	t.Run("foreign key validation preserves schema", func(t *testing.T) {
		tctx, mock, baseConn := newMockDumpConn(t)
		conf := DefaultConfig()
		conf.Tables = NewDatabaseTables().AppendTables(database, []string{"child", "parent"}, []uint64{0, 0})
		conf.columnFilter = newColumnFilterConfigForTest(t,
			columnFilterRule{Matcher: []string{database + ".parent"}, Columns: []string{"id", "name"}},
		)

		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, "").
				AddRow("parent_name", "varchar(12)", "YES", "", nil, ""))
		mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(
			"SELECT `id`,`parent_name` FROM `%s`.`child` LIMIT 1",
			database,
		))).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("id").OfType("INT", int64(0)),
			sqlmock.NewColumn("parent_name").OfType("VARCHAR", ""),
		).AddRow(1, "alice"))
		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, "").
				AddRow("name", "varchar(12)", "YES", "MUL", nil, "").
				AddRow("secret", "int(11)", "YES", "", nil, ""))
		mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(
			"SELECT `id`,`name`,`secret` FROM `%s`.`parent` LIMIT 1",
			database,
		))).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("id").OfType("INT", int64(0)),
			sqlmock.NewColumn("name").OfType("VARCHAR", ""),
			sqlmock.NewColumn("secret").OfType("INT", int64(0)),
		).AddRow(1, "alice", 2))
		mock.ExpectQuery("SHOW CREATE TABLE").
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(
				"child",
				"CREATE TABLE `child` (`id` INT PRIMARY KEY, `parent_name` VARCHAR(12), "+
					"FOREIGN KEY (`parent_name`) REFERENCES `parent` (`name`))",
			))
		mock.ExpectQuery("SHOW CREATE TABLE").
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(
				"parent",
				"CREATE TABLE `parent` (`id` INT PRIMARY KEY, `name` VARCHAR(12), `secret` INT, KEY (`name`))",
			))

		require.NoError(t, prepareColumnProjection(tctx, conf, baseConn))
		parentProjection := conf.columnProjection[tableName{db: database, table: "parent"}]
		require.NotContains(t, parentProjection.schemaSQL, "CHARACTER SET")
		require.NotContains(t, parentProjection.schemaSQL, "COLLATE")
		require.NotContains(t, parentProjection.schemaSQL, "`secret`")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGenerateProjectedSchema(t *testing.T) {
	t.Run("generated column dependencies", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (" +
			"`a` INT," +
			"`b` INT," +
			"`c` INT GENERATED ALWAYS AS (`a` + `b`) VIRTUAL," +
			"`d` INT GENERATED ALWAYS AS (`a` * 2) STORED," +
			"`e` INT GENERATED ALWAYS AS (`d` + 1) VIRTUAL," +
			"KEY `idx_c` (`c`)," +
			"KEY `idx_e` (`e`)," +
			"KEY `idx_ab` (`a`, `b`)," +
			"CONSTRAINT `chk_b` CHECK (`b` > 0)" +
			") ENGINE=InnoDB"
		projectedSQL, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"a"}, true, nil)
		require.NoError(t, err)

		stmt := parseCreateTableForTest(t, projectedSQL)
		require.Equal(t, []string{"a", "d", "e"}, createTableColumnNames(stmt))
		require.Equal(t, []string{"idx_e"}, createTableConstraintNames(stmt))
	})

	t.Run("partition dependency", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`a` INT, `b` INT) PARTITION BY HASH (`a`) PARTITIONS 4"
		_, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"b"}, true, nil)
		require.ErrorContains(t, err, "partition definition references a removed column")
	})

	t.Run("partition remains unchanged", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`a` INT, `b` INT) PARTITION BY HASH (`a`) PARTITIONS 4"
		projectedSQL, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"a"}, true, nil)
		require.NoError(t, err)
		require.Contains(t, projectedSQL, "PARTITION BY HASH (`a`) PARTITIONS 4")
	})

	t.Run("subpartition dependency", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`id` INT, `tenant_id` INT, PRIMARY KEY (`id`, `tenant_id`)) " +
			"PARTITION BY RANGE (`id`) " +
			"SUBPARTITION BY HASH (`tenant_id`) SUBPARTITIONS 2 " +
			"(PARTITION `p0` VALUES LESS THAN (100), PARTITION `pmax` VALUES LESS THAN MAXVALUE)"
		_, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"id"}, true, nil)
		require.ErrorContains(t, err, "partition definition references a removed column")
	})

	t.Run("TTL dependency", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`created_at` DATETIME, `a` INT) " +
			"TTL = `created_at` + INTERVAL 1 DAY"
		_, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"a"}, true, nil)
		require.ErrorContains(t, err, "TTL definition references removed column `created_at`")
	})

	t.Run("TTL remains unchanged", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`id` INT PRIMARY KEY, `created_at` DATETIME, `secret` INT) " +
			"TTL = `created_at` + INTERVAL 1 DAY"
		projectedSQL, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "created_at"}, true, nil,
		)
		require.NoError(t, err)
		require.Contains(t, projectedSQL, "/*T![ttl] TTL = `created_at` + INTERVAL 1 DAY */")
	})

	t.Run("retained default expression dependency", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`a` INT, `b` INT DEFAULT (`a`))"
		_, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"b"}, true, nil)
		require.ErrorContains(t, err, "column `b` expression references a removed column")
	})

	t.Run("default expression remains unchanged", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (" +
			"`id` INT PRIMARY KEY," +
			"`token` VARCHAR(32) DEFAULT (UUID())," +
			"`secret` INT" +
			")"
		projectedSQL, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "token"}, true, nil,
		)
		require.NoError(t, err)
		require.Contains(t, projectedSQL, "`token` VARCHAR(32) DEFAULT (UUID())")
	})

	t.Run("composite primary key removed", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (" +
			"`tenant_id` INT," +
			"`id` INT," +
			"`name` VARCHAR(32) DEFAULT NULL," +
			"PRIMARY KEY (`tenant_id`, `id`)" +
			")"
		projectedSQL, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "name"}, true, nil,
		)
		require.NoError(t, err)
		require.Empty(t, parseCreateTableForTest(t, projectedSQL).Constraints)
	})

	t.Run("auto increment remains without key", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (" +
			"`tenant_id` INT," +
			"`id` BIGINT AUTO_INCREMENT," +
			"PRIMARY KEY (`tenant_id`, `id`)" +
			")"
		projectedSQL, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id"}, true, nil,
		)
		require.NoError(t, err)
		require.Contains(t, projectedSQL, "`id` BIGINT AUTO_INCREMENT")
		require.Empty(t, parseCreateTableForTest(t, projectedSQL).Constraints)
	})

	t.Run("auto random key removed", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (" +
			"`id` BIGINT AUTO_RANDOM(3)," +
			"`tenant_id` BIGINT," +
			"PRIMARY KEY (`id`, `tenant_id`) CLUSTERED" +
			")"
		_, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id"}, true, nil,
		)
		require.ErrorContains(t, err, "auto_random is only supported on the tables with clustered primary key")
	})

	t.Run("foreign key target column removed", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT," +
			"`parent_secret` INT," +
			"CONSTRAINT `fk_secret` FOREIGN KEY (`parent_secret`) REFERENCES `parent` (`secret`)" +
			")"
		schemas := projectedTableSchemas{
			{db: "test", table: "parent"}: projectedTableSchemaForTest(
				t,
				"CREATE TABLE `parent` (`id` INT PRIMARY KEY, `secret` INT, KEY (`secret`))",
				[]string{"id"},
			),
		}

		_, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_secret"}, false, schemas,
		)
		require.ErrorContains(t, err, "foreign key references removed column `test`.`parent`.`secret`")
	})

	t.Run("self foreign key is case insensitive", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (" +
			"`id` INT," +
			"`parent_secret` INT," +
			"`secret` INT," +
			"FOREIGN KEY (`parent_secret`) REFERENCES `T` (`secret`)" +
			")"
		_, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_secret"}, true, nil,
		)
		require.ErrorContains(t, err, "foreign key references removed column `test`.`T`.`secret`")
	})

	t.Run("foreign key target generated column retained", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT," +
			"`parent_generated` INT," +
			"CONSTRAINT `fk_generated` FOREIGN KEY (`parent_generated`) REFERENCES `parent` (`generated`)" +
			")"
		schemas := projectedTableSchemas{
			{db: "test", table: "parent"}: projectedTableSchemaForTest(
				t,
				"CREATE TABLE `parent` (`id` INT, `generated` INT GENERATED ALWAYS AS (`id` + 1) STORED, KEY (`generated`))",
				[]string{"id"},
			),
		}

		projectedSQL, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_generated"}, false, schemas,
		)
		require.NoError(t, err)
		require.Len(t, parseCreateTableForTest(t, projectedSQL).Constraints, 1)
	})

	t.Run("foreign key actions remain unchanged", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT PRIMARY KEY," +
			"`parent_id` INT," +
			"FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) " +
			"ON DELETE CASCADE ON UPDATE SET NULL" +
			")"
		schemas := projectedTableSchemas{
			{db: "test", table: "parent"}: projectedTableSchemaForTest(
				t,
				"CREATE TABLE `parent` (`id` INT PRIMARY KEY)",
				[]string{"id"},
			),
		}

		projectedSQL, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_id"}, true, schemas,
		)
		require.NoError(t, err)
		require.Contains(t, projectedSQL, "ON DELETE CASCADE ON UPDATE SET NULL")
	})

	t.Run("foreign key supporting index removed", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT PRIMARY KEY," +
			"`parent_id` INT," +
			"FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)" +
			")"
		schemas := projectedTableSchemas{
			{db: "test", table: "parent"}: projectedTableSchemaForTest(
				t,
				"CREATE TABLE `parent` (`id` INT, `secret` INT, UNIQUE KEY (`id`, `secret`))",
				[]string{"id"},
			),
		}

		_, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_id"}, true, schemas,
		)
		require.ErrorContains(t, err, "referenced columns are not indexed")
	})

	t.Run("foreign key supporting index prefix retained", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT PRIMARY KEY," +
			"`parent_id` INT," +
			"FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)" +
			")"
		schemas := projectedTableSchemas{
			{db: "test", table: "parent"}: projectedTableSchemaForTest(
				t,
				"CREATE TABLE `parent` (`id` INT, `tenant_id` INT, `secret` INT, KEY (`id`, `tenant_id`))",
				[]string{"id", "tenant_id"},
			),
		}

		_, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_id"}, true, schemas,
		)
		require.NoError(t, err)
	})

	t.Run("foreign key prefix index is insufficient", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT PRIMARY KEY," +
			"`parent_name` VARCHAR(32)," +
			"FOREIGN KEY (`parent_name`) REFERENCES `parent` (`name`)" +
			")"
		schemas := projectedTableSchemas{
			{db: "test", table: "parent"}: projectedTableSchemaForTest(
				t,
				"CREATE TABLE `parent` (`name` VARCHAR(32), KEY (`name`(8)))",
				[]string{"name"},
			),
		}

		_, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_name"}, true, schemas,
		)
		require.ErrorContains(t, err, "referenced columns are not indexed")
	})

	t.Run("foreign key columnar index is insufficient", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT PRIMARY KEY," +
			"`parent_name` VARCHAR(32)," +
			"FOREIGN KEY (`parent_name`) REFERENCES `parent` (`name`)" +
			")"
		schemas := projectedTableSchemas{
			{db: "test", table: "parent"}: projectedTableSchemaForTest(
				t,
				"CREATE TABLE `parent` (`name` VARCHAR(32), FULLTEXT INDEX (`name`))",
				[]string{"name"},
			),
		}

		_, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_name"}, true, schemas,
		)
		require.ErrorContains(t, err, "referenced columns are not indexed")
	})

	t.Run("case distinct tables do not collide", func(t *testing.T) {
		upper := projectedTableSchemaForTest(t, "CREATE TABLE `Orders` (`id` INT PRIMARY KEY)", []string{"id"})
		lower := projectedTableSchemaForTest(t, "CREATE TABLE `orders` (`other_id` INT PRIMARY KEY)", []string{"other_id"})
		schemas := projectedTableSchemas{
			{db: "test", table: "Orders"}: upper,
			{db: "test", table: "orders"}: lower,
		}

		matched, ok, err := schemas.lookup("test", "Orders")
		require.NoError(t, err)
		require.True(t, ok)
		require.Same(t, upper, matched)
		matched, ok, err = schemas.lookup("test", "orders")
		require.NoError(t, err)
		require.True(t, ok)
		require.Same(t, lower, matched)
		_, _, err = schemas.lookup("test", "ORDERS")
		require.ErrorContains(t, err, "ambiguous under case-insensitive matching")
	})

	t.Run("no writable column removed", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`a` INT, `b` INT GENERATED ALWAYS AS (`a` + 1) VIRTUAL)"
		projectedSQL, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"a"}, false, nil)
		require.NoError(t, err)
		require.Equal(t, createSQL, projectedSQL)
	})
}

func generateProjectedSchemaForTest(
	t *testing.T,
	originSQL string,
	database string,
	selectedColumns []string,
	rewriteSchema bool,
	schemas projectedTableSchemas,
) (string, error) {
	t.Helper()
	if schemas == nil {
		schemas = make(projectedTableSchemas)
	}
	schema, err := buildProjectedTableSchema(parser.New(), originSQL, selectedColumns)
	if err != nil {
		return "", err
	}
	table := schema.createTable.Table.Name.O
	schemas[tableName{db: database, table: table}] = schema
	projectedSQL := originSQL
	if rewriteSchema {
		projectedSQL, err = restoreProjectedSchema(schema.createTable)
		if err != nil {
			return "", err
		}
		if _, err = schema.buildTableInfo(); err != nil {
			return "", err
		}
	}
	if err := validateForeignKeys(database, schema, schemas); err != nil {
		return "", err
	}
	if !rewriteSchema {
		return originSQL, nil
	}
	parseCreateTableForTest(t, projectedSQL)
	return projectedSQL, nil
}

func projectedTableSchemaForTest(t *testing.T, originSQL string, selectedColumns []string) *projectedTableSchema {
	t.Helper()
	schema, err := buildProjectedTableSchema(parser.New(), originSQL, selectedColumns)
	require.NoError(t, err)
	return schema
}

func parseCreateTableForTest(t *testing.T, sql string) *ast.CreateTableStmt {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	createTable, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok)
	return createTable
}

func createTableColumnNames(stmt *ast.CreateTableStmt) []string {
	names := make([]string, 0, len(stmt.Cols))
	for _, column := range stmt.Cols {
		names = append(names, column.Name.Name.O)
	}
	return names
}

func createTableConstraintNames(stmt *ast.CreateTableStmt) []string {
	names := make([]string, 0, len(stmt.Constraints))
	for _, constraint := range stmt.Constraints {
		names = append(names, constraint.Name)
	}
	return names
}
