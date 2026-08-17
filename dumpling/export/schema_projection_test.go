// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestGenerateProjectedSchema(t *testing.T) {
	t.Run("schema is prepared before table metadata", func(t *testing.T) {
		tctx, mock, baseConn := newMockDumpConn(t)
		conf := DefaultConfig()
		conf.ServerInfo.ServerType = version.ServerTypeMySQL
		conf.Tables = NewDatabaseTables().AppendTables(database, []string{table}, []uint64{0})
		conf.columnFilter = newColumnFilterConfigForTest(t,
			columnFilterRule{Matcher: []string{database + "." + table}, Columns: []string{"id", "name"}},
		)

		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, "").
				AddRow("name", "varchar(12)", "NO", "", nil, "").
				AddRow("secret", "varchar(12)", "NO", "", nil, ""))
		mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf("SELECT `id`,`name`,`secret` FROM `%s`.`%s` LIMIT 1", database, table))).
			WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
				sqlmock.NewColumn("name").OfType("VARCHAR", ""),
				sqlmock.NewColumn("secret").OfType("VARCHAR", ""),
			).AddRow(1, "alice", "hidden"))
		createSQL := "CREATE TABLE `test_table` (`id` INT PRIMARY KEY, `name` VARCHAR(12), `secret` VARCHAR(12))"
		mock.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table))).
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(table, createSQL))

		require.NoError(t, prepareColumnProjection(tctx, conf, baseConn))
		meta, err := dumpTableMeta(tctx, conf, baseConn, database, &TableInfo{Type: TableTypeBase, Name: table})
		require.NoError(t, err)
		require.NotContains(t, meta.ShowCreateTable(), "`secret`")
		require.NoError(t, mock.ExpectationsWereMet())
	})

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

	t.Run("TTL dependency", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`created_at` DATETIME, `a` INT) " +
			"TTL = `created_at` + INTERVAL 1 DAY"
		_, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"a"}, true, nil)
		require.ErrorContains(t, err, "TTL definition references removed column `created_at`")
	})

	t.Run("retained default expression dependency", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`a` INT, `b` INT DEFAULT (`a`))"
		_, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"b"}, true, nil)
		require.ErrorContains(t, err, "column `b` expression references a removed column")
	})

	t.Run("automatic ID loses composite index", func(t *testing.T) {
		createSQL := "CREATE TABLE `t` (`a` BIGINT AUTO_INCREMENT, `b` INT, KEY `idx_ab` (`a`, `b`))"
		_, err := generateProjectedSchemaForTest(t, createSQL, "test", []string{"a"}, true, nil)
		require.ErrorContains(t, err, "column `a` loses the index required by its automatic ID option")
	})

	t.Run("foreign key target column removed", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT," +
			"`parent_secret` INT," +
			"CONSTRAINT `fk_secret` FOREIGN KEY (`parent_secret`) REFERENCES `parent` (`secret`)" +
			")"
		schemaColumns := map[tableName]map[string]struct{}{
			{db: "test", table: "child"}:  {"id": {}, "parent_secret": {}},
			{db: "test", table: "parent"}: {"id": {}},
		}

		_, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_secret"}, false, schemaColumns,
		)
		require.ErrorContains(t, err, "foreign key references removed column `test`.`parent`.`secret`")
	})

	t.Run("foreign key target generated column retained", func(t *testing.T) {
		createSQL := "CREATE TABLE `child` (" +
			"`id` INT," +
			"`parent_generated` INT," +
			"CONSTRAINT `fk_generated` FOREIGN KEY (`parent_generated`) REFERENCES `parent` (`generated`)" +
			")"
		schemaColumns := map[tableName]map[string]struct{}{
			{db: "test", table: "child"}:  {"id": {}, "parent_generated": {}},
			{db: "test", table: "parent"}: {"id": {}, "generated": {}},
		}

		projectedSQL, err := generateProjectedSchemaForTest(
			t, createSQL, "test", []string{"id", "parent_generated"}, false, schemaColumns,
		)
		require.NoError(t, err)
		require.Len(t, parseCreateTableForTest(t, projectedSQL).Constraints, 1)
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
	projected bool,
	schemaColumns map[tableName]map[string]struct{},
) (string, error) {
	t.Helper()
	retainedColumns, err := collectProjectedSchemaColumns(originSQL, selectedColumns)
	if err != nil {
		return "", err
	}
	return generateProjectedSchema(originSQL, database, projected, retainedColumns, schemaColumns)
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
