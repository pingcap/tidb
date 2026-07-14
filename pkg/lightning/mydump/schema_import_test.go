// Copyright 2024 PingCAP, Inc.
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

package mydump

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSchemaImporter(t *testing.T) {
	db, mock, err := sqlmock.New()
	mock.MatchExpectationsInOrder(false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		// have to ignore the error here, as sqlmock doesn't allow set number of
		// expectations, and each opened connection requires a Close() call.
		_ = db.Close()
	})
	ctx := context.Background()
	tempDir := t.TempDir()
	store, err := objstore.NewLocalStorage(tempDir)
	require.NoError(t, err)
	logger := log.Logger{Logger: zap.NewExample()}
	importer := NewSchemaImporter(logger, mysql.SQLMode(0), db, store, 4)
	require.NoError(t, importer.Run(ctx, nil))

	t.Run("get existing schema err", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnError(errors.New("non retryable error"))
		require.ErrorContains(t, importer.Run(ctx, []*MDDatabaseMeta{{Name: "test"}}), "non retryable error")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database already exists", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test"))
		require.NoError(t, importer.Run(ctx, []*MDDatabaseMeta{{Name: "test"}}))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("create non exist database", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}))
		dbMetas := make([]*MDDatabaseMeta, 0, 10)
		for i := range 10 {
			mock.ExpectExec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `test%02d`", i)).
				WillReturnResult(sqlmock.NewResult(0, 0))
			dbMetas = append(dbMetas, &MDDatabaseMeta{Name: fmt.Sprintf("test%02d", i)})
		}
		require.NoError(t, importer.Run(ctx, dbMetas))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("break on database error", func(t *testing.T) {
		importer2 := NewSchemaImporter(logger, mysql.SQLMode(0), db, store, 1)
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}))
		fileName := "invalid-schema.sql"
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileName), []byte("CREATE invalid;"), 0o644))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileName}}},
			{Name: "test2"}, // not chance to run
		}
		require.ErrorContains(t, importer2.Run(ctx, dbMetas), "invalid schema statement")
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileName)))

		dbMetas = append([]*MDDatabaseMeta{{Name: "ttt"}}, dbMetas...)
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}))
		mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `ttt`").
			WillReturnError(errors.New("non retryable error"))
		err2 := importer2.Run(ctx, dbMetas)
		require.ErrorIs(t, err2, common.ErrCreateSchema)
		require.ErrorContains(t, err2, "non retryable error")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("table: no schema file for the table", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).
				AddRow("test01").AddRow("test02").AddRow("test03").
				AddRow("test04").AddRow("test05"))
		mock.ExpectQuery("SHOW CREATE TABLE `test02`.`t`").
			WillReturnError(&dmysql.MySQLError{Number: tmysql.ErrNoSuchTable})
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01"},
			{Name: "test02", Tables: []*MDTableMeta{{DB: "test02", Name: "t"}}},
		}
		require.ErrorContains(t, importer.Run(ctx, dbMetas), "schema not found")
		require.NoError(t, mock.ExpectationsWereMet())

		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).
				AddRow("test01").AddRow("test02").AddRow("test03").
				AddRow("test04").AddRow("test05"))
		mock.ExpectQuery("SHOW CREATE TABLE `test02`.`t`").
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("t", "CREATE TABLE `t` (a int);"))
		require.NoError(t, importer.Run(ctx, dbMetas))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("table: invalid schema file", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).
				AddRow("test01").AddRow("test02").AddRow("test03").
				AddRow("test04").AddRow("test05"))
		mock.ExpectQuery("SHOW CREATE TABLE `test01`.`t1`").
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("t1", "CREATE TABLE `t1` (a int);"))
		mock.ExpectQuery("SHOW CREATE TABLE `test01`.`T2`").
			WillReturnError(&dmysql.MySQLError{Number: tmysql.ErrNoSuchTable})
		fileName := "t2-invalid-schema.sql"
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileName), []byte("CREATE table t2 whatever;"), 0o644))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01", Tables: []*MDTableMeta{
				{DB: "test01", Name: "t1"},
				{DB: "test01", Name: "T2", charSet: "auto",
					SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileName}}},
			}},
		}
		require.ErrorContains(t, importer.Run(ctx, dbMetas), "line 1 column 24 near")
		require.NoError(t, mock.ExpectationsWereMet())

		// create table t2 downstream manually as workaround
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).
				AddRow("test01").AddRow("test02").AddRow("test03").
				AddRow("test04").AddRow("test05"))
		mock.ExpectQuery("SHOW CREATE TABLE `test01`.`t1`").
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("t1", "CREATE TABLE `t1` (a int);"))
		mock.ExpectQuery("SHOW CREATE TABLE `test01`.`T2`").
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("T2", "CREATE TABLE `t2` (a int);"))
		require.NoError(t, importer.Run(ctx, dbMetas))
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileName)))
	})

	t.Run("table: break on error", func(t *testing.T) {
		importer2 := NewSchemaImporter(logger, mysql.SQLMode(0), db, store, 1)
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).
				AddRow("test01").AddRow("test02").AddRow("test03").
				AddRow("test04").AddRow("test05"))
		fileNameT1 := "test01.t1-schema.sql"
		fileNameT2 := "test01.t2-schema.sql"
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameT1), []byte("CREATE table t1(a int);"), 0o644))
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameT2), []byte("CREATE table t2(a int);"), 0o644))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01", Tables: []*MDTableMeta{
				{DB: "test01", Name: "t1", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameT1}}},
				{DB: "test01", Name: "t2", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameT2}}},
			}},
		}
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test01`.`t1`").
			WillReturnError(errors.New("non retryable create table error"))
		require.ErrorContains(t, importer2.Run(ctx, dbMetas), "non retryable create table error")
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameT1)))
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameT2)))
	})

	t.Run("table: ignore drop table in schema file", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test01"))
		fileName := "test01.t1-schema.sql"
		require.NoError(t, os.WriteFile(
			path.Join(tempDir, fileName),
			[]byte("DROP TABLE t1; CREATE TABLE t1(a int);"),
			0o644,
		))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01", Tables: []*MDTableMeta{
				{DB: "test01", Name: "t1", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileName}}},
			}},
		}
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test01`.`t1`").
			WillReturnResult(sqlmock.NewResult(0, 0))
		require.NoError(t, importer.Run(ctx, dbMetas))
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileName)))
	})

	t.Run("table: ignore drop database in schema file", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test01"))
		fileName := "test01.t1-schema.sql"
		require.NoError(t, os.WriteFile(
			path.Join(tempDir, fileName),
			[]byte("DROP DATABASE test01; CREATE TABLE t1(a int);"),
			0o644,
		))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01", Tables: []*MDTableMeta{
				{DB: "test01", Name: "t1", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileName}}},
			}},
		}
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test01`.`t1`").
			WillReturnResult(sqlmock.NewResult(0, 0))
		require.NoError(t, importer.Run(ctx, dbMetas))
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileName)))
	})

	t.Run("view: get existing schema err", func(t *testing.T) {
		fileName := "test02.v-schema-view.sql"
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileName), []byte("create view v as select 1;"), 0o644))
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test01").AddRow("test02"))
		mock.ExpectQuery("^SELECT TABLE_NAME, TABLE_TYPE FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = 'test02'$").
			WillReturnError(errors.New("non retryable error"))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01"},
			{Name: "test02", Views: []*MDTableMeta{{DB: "test02", Name: "v", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileName}}}}},
		}
		require.ErrorContains(t, importer.Run(ctx, dbMetas), "non retryable error")
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileName)))
	})

	t.Run("view: fail on create", func(t *testing.T) {
		fileNameV1 := "invalid-schema.sql"
		fileNameV2 := "test02.v2-schema-view.sql"
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV1), []byte("xxxx;"), 0o644))
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV2), []byte("create view v2 as select * from t;"), 0o644))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01"},
			{Name: "test02", Views: []*MDTableMeta{
				{DB: "test02", Name: "v1", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV1}}},
				{DB: "test02", Name: "V2", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV2}}}}},
		}
		require.ErrorContains(t, importer.Run(ctx, dbMetas), `line 1 column 4 near "xxxx;"`)
		require.NoError(t, mock.ExpectationsWereMet())

		// skip v1 because it already exists downstream and create v2 in dependency order
		fileNameValidV1 := "test02.v1-schema-view.sql"
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameValidV1), []byte("create view v1 as select * from t;"), 0o644))
		validViews := []*MDDatabaseMeta{
			{Name: "test01"},
			{Name: "test02", Views: []*MDTableMeta{
				{DB: "test02", Name: "v1", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameValidV1}}},
				{DB: "test02", Name: "V2", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV2}}},
			}},
		}
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test01").AddRow("test02"))
		mock.ExpectQuery("^SELECT TABLE_NAME, TABLE_TYPE FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = 'test02'$").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE"}).
				AddRow("t", "BASE TABLE").
				AddRow("v1", "VIEW"))
		mock.ExpectExec("VIEW `test02`.`V2` AS SELECT").
			WillReturnResult(sqlmock.NewResult(0, 0))
		require.NoError(t, importer.Run(ctx, validViews))
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameV1)))
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameV2)))
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameValidV1)))
	})

	t.Run("view: skip existing view with different case", func(t *testing.T) {
		fileNameV2 := "test03.V2-schema-view.sql"
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV2), []byte("create view V2 as select * from t;"), 0o644))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test03", Tables: []*MDTableMeta{{DB: "test03", Name: "t"}}},
			{Name: "test03", Views: []*MDTableMeta{
				{DB: "test03", Name: "V2", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV2}}},
			}},
		}

		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test03"))
		mock.ExpectQuery("SHOW CREATE TABLE `test03`.`t`").
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("t", "CREATE TABLE `t` (a int);"))
		mock.ExpectQuery("^SELECT TABLE_NAME, TABLE_TYPE FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = 'test03'$").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE"}).
				AddRow("t", "BASE TABLE").
				AddRow("V2", "VIEW"))

		require.NoError(t, importer.Run(ctx, dbMetas))
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameV2)))
	})
}

func TestNewSchemaImportPlan(t *testing.T) {
	db, mock, err := sqlmock.New()
	mock.MatchExpectationsInOrder(false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})

	ctx := context.Background()
	tempDir := t.TempDir()
	store, err := objstore.NewLocalStorage(tempDir)
	require.NoError(t, err)

	fileNameV1 := "test.v1-schema-view.sql"
	fileNameV2 := "test.v2-schema-view.sql"
	require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV1), []byte("create view v1 as select * from t;"), 0o644))
	require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV2), []byte("create view v2 as select * from v1;"), 0o644))

	plan, err := NewSchemaImportPlan(ctx, store, mysql.SQLMode(0), []*MDDatabaseMeta{
		{
			Name: "test",
			Views: []*MDTableMeta{
				{DB: "test", Name: "v1", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV1}}},
				{DB: "test", Name: "v2", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV2}}},
			},
			Tables: []*MDTableMeta{
				{DB: "test", Name: "t", charSet: "auto"},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, plan.viewPlan)
	require.Len(t, plan.viewPlan.ordered, 2)
	require.Equal(t, "v1", plan.viewPlan.ordered[0].key.Name)
	require.Equal(t, "v2", plan.viewPlan.ordered[1].key.Name)
	require.Empty(t, plan.viewPlan.ordered[0].externalDeps)
}

func TestLoaderSetupDefersViewSchemaValidationUntilRun(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	store, err := objstore.NewLocalStorage(tempDir)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path.Join(tempDir, "db-schema-create.sql"), []byte("CREATE DATABASE db;"), 0o644))
	require.NoError(t, os.WriteFile(path.Join(tempDir, "db.v1-schema-view.sql"), nil, 0o644))

	cfg := LoaderConfig{
		SourceURL:        "file://" + filepath.ToSlash(tempDir),
		CharacterSet:     "auto",
		Filter:           []string{"*.*"},
		DefaultFileRules: true,
	}

	mdl, err := NewLoaderWithStore(ctx, cfg, store)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})
	importer := NewSchemaImporter(log.Logger{Logger: zap.NewExample()}, mysql.SQLMode(0), db, store, 1)

	err = importer.Run(ctx, mdl.GetDatabases())
	require.ErrorContains(t, err, "missing create view statement for `db`.`v1`")
}

func TestSchemaImporterManyTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	mock.MatchExpectationsInOrder(false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		// have to ignore the error here, as sqlmock doesn't allow set number of
		// expectations, and each opened connection requires a Close() call.
		_ = db.Close()
	})
	ctx := context.Background()
	tempDir := t.TempDir()
	store, err := objstore.NewLocalStorage(tempDir)
	require.NoError(t, err)
	logger := log.Logger{Logger: zap.NewExample()}
	importer := NewSchemaImporter(logger, mysql.SQLMode(0), db, store, 8)

	mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	dbMetas := make([]*MDDatabaseMeta, 0, 30)
	for i := range 30 {
		dbName := fmt.Sprintf("test%02d", i)
		dbMeta := &MDDatabaseMeta{Name: dbName, Tables: make([]*MDTableMeta, 0, 100)}
		mock.ExpectExec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		for j := range 50 {
			tblName := fmt.Sprintf("t%03d", j)
			fileName := fmt.Sprintf("%s.%s-schema.sql", dbName, tblName)
			require.NoError(t, os.WriteFile(path.Join(tempDir, fileName), []byte(fmt.Sprintf("CREATE TABLE %s(a int);", tblName)), 0o644))
			mock.ExpectExec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`", dbName, tblName)).
				WillReturnResult(sqlmock.NewResult(0, 0))
			dbMeta.Tables = append(dbMeta.Tables, &MDTableMeta{
				DB: dbName, Name: tblName, charSet: "auto",
				SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileName}},
			})
		}
		dbMetas = append(dbMetas, dbMeta)
	}
	require.NoError(t, importer.Run(ctx, dbMetas))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateTableIfNotExistsStmt(t *testing.T) {
	dbName := "testdb"
	p := parser.New()
	createSQLIfNotExistsStmt := func(createTable, tableName string) []string {
		res, err := createIfNotExistsStmt(p, createTable, dbName, tableName)
		require.NoError(t, err)
		return res
	}

	require.Equal(t, []string{"CREATE DATABASE IF NOT EXISTS `testdb` CHARACTER SET = utf8 COLLATE = utf8_general_ci;"},
		createSQLIfNotExistsStmt("CREATE DATABASE `foo` CHARACTER SET = utf8 COLLATE = utf8_general_ci;", ""))

	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` TINYINT(1));"},
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` TINYINT(1));", "foo"))

	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` TINYINT(1));"},
		createSQLIfNotExistsStmt("CREATE TABLE IF NOT EXISTS `foo`(`bar` TINYINT(1));", "foo"))

	// case insensitive
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`fOo` (`bar` TINYINT(1));"},
		createSQLIfNotExistsStmt("/* cOmmEnt */ creAte tablE `fOo`(`bar` TinyinT(1));", "fOo"))

	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`FoO` (`bAR` TINYINT(1));"},
		createSQLIfNotExistsStmt("/* coMMenT */ crEatE tAble If not EXISts `FoO`(`bAR` tiNyInT(1));", "FoO"))

	// only one "CREATE TABLE" is replaced
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) COMMENT 'CREATE TABLE');"},
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) COMMENT 'CREATE TABLE');", "foo"))

	// test clustered index consistency
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) PRIMARY KEY /*T![clustered_index] CLUSTERED */ COMMENT 'CREATE TABLE');"},
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) PRIMARY KEY CLUSTERED COMMENT 'CREATE TABLE');", "foo"))
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) COMMENT 'CREATE TABLE',PRIMARY KEY(`bar`) /*T![clustered_index] NONCLUSTERED */);"},
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) COMMENT 'CREATE TABLE', PRIMARY KEY (`bar`) NONCLUSTERED);", "foo"))
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) PRIMARY KEY /*T![clustered_index] NONCLUSTERED */ COMMENT 'CREATE TABLE');"},
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) PRIMARY KEY /*T![clustered_index] NONCLUSTERED */ COMMENT 'CREATE TABLE');", "foo"))
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) COMMENT 'CREATE TABLE',PRIMARY KEY(`bar`) /*T![clustered_index] CLUSTERED */);"},
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) COMMENT 'CREATE TABLE', PRIMARY KEY (`bar`) /*T![clustered_index] CLUSTERED */);", "foo"))

	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(2) */ COMMENT 'CREATE TABLE');"},
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) PRIMARY KEY AUTO_RANDOM(2) COMMENT 'CREATE TABLE');", "foo"))

	// upper case becomes shorter
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`ſ` (`ı` TINYINT(1));"},
		createSQLIfNotExistsStmt("CREATE TABLE `ſ`(`ı` TINYINT(1));", "ſ"))

	// upper case becomes longer
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`ɑ` (`ȿ` TINYINT(1));"},
		createSQLIfNotExistsStmt("CREATE TABLE `ɑ`(`ȿ` TINYINT(1));", "ɑ"))

	// non-utf-8
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`\xcc\xcc\xcc` (`???` TINYINT(1));"},
		createSQLIfNotExistsStmt("CREATE TABLE `\xcc\xcc\xcc`(`\xdd\xdd\xdd` TINYINT(1));", "\xcc\xcc\xcc"))

	// renaming a table
	require.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `testdb`.`ba``r` (`x` INT);"},
		createSQLIfNotExistsStmt("create table foo(x int);", "ba`r"))

	// conditional comments
	require.Equal(t, []string{
		"SET NAMES 'binary';",
		"SET @@SESSION.`FOREIGN_KEY_CHECKS`=0;",
		"CREATE TABLE IF NOT EXISTS `testdb`.`m` (`z` DOUBLE) ENGINE = InnoDB AUTO_INCREMENT = 8343230 DEFAULT CHARACTER SET = UTF8;",
	},
		createSQLIfNotExistsStmt(`
			/*!40101 SET NAMES binary*/;
			/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
			CREATE TABLE x.y (z double) ENGINE=InnoDB AUTO_INCREMENT=8343230 DEFAULT CHARSET=utf8;
		`, "m"))

	// create view
	require.Equal(t, []string{
		"SET NAMES 'binary';",
		"DROP TABLE IF EXISTS `testdb`.`m`;",
		"DROP VIEW IF EXISTS `testdb`.`m`;",
		"SET @`PREV_CHARACTER_SET_CLIENT`=@@`character_set_client`;",
		"SET @`PREV_CHARACTER_SET_RESULTS`=@@`character_set_results`;",
		"SET @`PREV_COLLATION_CONNECTION`=@@`collation_connection`;",
		"SET @@SESSION.`character_set_client`=`utf8`;",
		"SET @@SESSION.`character_set_results`=`utf8`;",
		"SET @@SESSION.`collation_connection`=`utf8_general_ci`;",
		"CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`192.168.198.178` SQL SECURITY DEFINER VIEW `testdb`.`m` (`s`) AS SELECT `s` FROM `db1`.`v1` WHERE `i`<2;",
		"SET @@SESSION.`character_set_client`=@`PREV_CHARACTER_SET_CLIENT`;",
		"SET @@SESSION.`character_set_results`=@`PREV_CHARACTER_SET_RESULTS`;",
		"SET @@SESSION.`collation_connection`=@`PREV_COLLATION_CONNECTION`;",
	},
		createSQLIfNotExistsStmt(`
			/*!40101 SET NAMES binary*/;
			DROP TABLE IF EXISTS v2;
			DROP VIEW IF EXISTS v2;
			SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;
			SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;
			SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;
			SET character_set_client = utf8;
			SET character_set_results = utf8;
			SET collation_connection = utf8_general_ci;
			CREATE ALGORITHM=UNDEFINED DEFINER=root@192.168.198.178 SQL SECURITY DEFINER VIEW v2 (s) AS SELECT s FROM db1.v1 WHERE i<2;
			SET character_set_client = @PREV_CHARACTER_SET_CLIENT;
			SET character_set_results = @PREV_CHARACTER_SET_RESULTS;
			SET collation_connection = @PREV_COLLATION_CONNECTION;
		`, "m"))
}

func TestSchemaImporterImportsViewsInDependencyOrderAfterPlaceholderPrune(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})

	ctx := context.Background()
	tempDir := t.TempDir()
	store, err := objstore.NewLocalStorage(tempDir)
	require.NoError(t, err)
	logger := log.Logger{Logger: zap.NewExample()}
	importer := NewSchemaImporter(logger, mysql.SQLMode(0), db, store, 1)

	fileNameT := "test.t-schema.sql"
	fileNameV1View := "test.v1-schema-view.sql"
	fileNameV2View := "test.v2-schema-view.sql"

	require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameT), []byte("CREATE TABLE t(id INT PRIMARY KEY);"), 0o644))
	require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV1View), []byte(`
/*!40101 SET NAMES binary*/;
DROP TABLE IF EXISTS v1;
DROP VIEW IF EXISTS v1;
CREATE ALGORITHM=UNDEFINED DEFINER=`+"`root`@`%`"+` SQL SECURITY DEFINER VIEW v1 (`+"`id`"+`) AS SELECT `+"`id`"+` FROM `+"`test`.`t`"+`;
`), 0o644))
	require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV2View), []byte(`
/*!40101 SET NAMES binary*/;
DROP TABLE IF EXISTS v2;
DROP VIEW IF EXISTS v2;
CREATE ALGORITHM=UNDEFINED DEFINER=`+"`root`@`%`"+` SQL SECURITY DEFINER VIEW v2 (`+"`id`"+`) AS SELECT `+"`id`"+` FROM `+"`test`.`v1`"+`;
`), 0o644))

	dbMetas := []*MDDatabaseMeta{
		{
			Name: "test",
			Tables: []*MDTableMeta{
				{DB: "test", Name: "t", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameT}}},
			},
			Views: []*MDTableMeta{
				{DB: "test", Name: "v1", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV1View}}},
				{DB: "test", Name: "v2", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV2View}}},
			},
		},
	}

	mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test"))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test`.`t`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("^SELECT TABLE_NAME, TABLE_TYPE FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = 'test'$").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE"}).
			AddRow("t", "BASE TABLE"))
	mock.ExpectExec("CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `test`.`v1`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `test`.`v2`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, importer.Run(ctx, dbMetas))
}
