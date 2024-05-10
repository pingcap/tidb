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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
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
	store, err := storage.NewLocalStorage(tempDir)
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
		for i := 0; i < 10; i++ {
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

	t.Run("table: get existing schema err", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).
				AddRow("test01").AddRow("test02").AddRow("test03").
				AddRow("test04").AddRow("test05"))
		mock.ExpectQuery("TABLES WHERE TABLE_SCHEMA = 'test02'").
			WillReturnError(errors.New("non retryable error"))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01"},
			{Name: "test02", Tables: []*MDTableMeta{{DB: "test02", Name: "t"}}},
		}
		require.ErrorContains(t, importer.Run(ctx, dbMetas), "non retryable error")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("table: invalid schema file", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).
				AddRow("test01").AddRow("test02").AddRow("test03").
				AddRow("test04").AddRow("test05"))
		mock.ExpectQuery("TABLES WHERE TABLE_SCHEMA = 'test01'").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("t1"))
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
		mock.ExpectQuery("TABLES WHERE TABLE_SCHEMA = 'test01'").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("t1").AddRow("t2"))
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
		mock.ExpectQuery("TABLES WHERE TABLE_SCHEMA = 'test01'").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}))
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test01`.`t1`").
			WillReturnError(errors.New("non retryable create table error"))
		require.ErrorContains(t, importer2.Run(ctx, dbMetas), "non retryable create table error")
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameT1)))
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameT2)))
	})

	t.Run("view: get existing schema err", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test01").AddRow("test02"))
		mock.ExpectQuery("VIEWS WHERE TABLE_SCHEMA = 'test02'").
			WillReturnError(errors.New("non retryable error"))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01"},
			{Name: "test02", Views: []*MDTableMeta{{DB: "test02", Name: "v"}}},
		}
		require.ErrorContains(t, importer.Run(ctx, dbMetas), "non retryable error")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("view: fail on create", func(t *testing.T) {
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test01").AddRow("test02"))
		mock.ExpectQuery("VIEWS WHERE TABLE_SCHEMA = 'test02'").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}))
		fileNameV0 := "empty-file.sql"
		fileNameV1 := "invalid-schema.sql"
		fileNameV2 := "test02.v2-schema-view.sql"
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV0), []byte(""), 0o644))
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV1), []byte("xxxx;"), 0o644))
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileNameV2), []byte("create view v2 as select * from t;"), 0o644))
		dbMetas := []*MDDatabaseMeta{
			{Name: "test01"},
			{Name: "test02", Views: []*MDTableMeta{
				{DB: "test02", Name: "V0", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV0}}},
				{DB: "test02", Name: "v1", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV1}}},
				{DB: "test02", Name: "V2", charSet: "auto", SchemaFile: FileInfo{FileMeta: SourceFileMeta{Path: fileNameV2}}}}},
		}
		require.ErrorContains(t, importer.Run(ctx, dbMetas), `line 1 column 4 near "xxxx;"`)
		require.NoError(t, mock.ExpectationsWereMet())

		// create view v2 downstream manually as workaround
		mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("test01").AddRow("test02"))
		mock.ExpectQuery("VIEWS WHERE TABLE_SCHEMA = 'test02'").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("V1"))
		mock.ExpectExec("VIEW `test02`.`V2` AS SELECT").
			WillReturnResult(sqlmock.NewResult(0, 0))
		require.NoError(t, importer.Run(ctx, dbMetas))
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameV0)))
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameV1)))
		require.NoError(t, os.Remove(path.Join(tempDir, fileNameV2)))
	})
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
	store, err := storage.NewLocalStorage(tempDir)
	require.NoError(t, err)
	logger := log.Logger{Logger: zap.NewExample()}
	importer := NewSchemaImporter(logger, mysql.SQLMode(0), db, store, 8)

	mock.ExpectQuery(`information_schema.SCHEMATA`).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	dbMetas := make([]*MDDatabaseMeta, 0, 30)
	for i := 0; i < 30; i++ {
		dbName := fmt.Sprintf("test%02d", i)
		dbMeta := &MDDatabaseMeta{Name: dbName, Tables: make([]*MDTableMeta, 0, 100)}
		mock.ExpectExec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(fmt.Sprintf("TABLES WHERE TABLE_SCHEMA = '%s'", dbName)).
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}))
		for j := 0; j < 50; j++ {
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
