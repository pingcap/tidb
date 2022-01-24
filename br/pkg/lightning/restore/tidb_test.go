// Copyright 2019 PingCAP, Inc.
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

package restore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&tidbSuite{})

type tidbSuite struct {
	mockDB sqlmock.Sqlmock
	timgr  *TiDBManager
	tiGlue glue.Glue
}

func TestTiDB(t *testing.T) {
	TestingT(t)
}

func (s *tidbSuite) SetUpTest(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	s.mockDB = mock
	defaultSQLMode, err := tmysql.GetSQLMode(tmysql.DefaultSQLMode)
	c.Assert(err, IsNil)

	s.timgr = NewTiDBManagerWithDB(db, defaultSQLMode)
	s.tiGlue = glue.NewExternalTiDBGlue(db, defaultSQLMode)
}

func (s *tidbSuite) TearDownTest(c *C) {
	s.timgr.Close()
	c.Assert(s.mockDB.ExpectationsWereMet(), IsNil)
}

func (s *tidbSuite) TestCreateTableIfNotExistsStmt(c *C) {
	dbName := "testdb"
	createSQLIfNotExistsStmt := func(createTable, tableName string) []string {
		res, err := createIfNotExistsStmt(s.tiGlue.GetParser(), createTable, dbName, tableName)
		c.Assert(err, IsNil)
		return res
	}

	c.Assert(
		createSQLIfNotExistsStmt("CREATE DATABASE `foo` CHARACTER SET = utf8 COLLATE = utf8_general_ci;", ""),
		DeepEquals,
		[]string{"CREATE DATABASE IF NOT EXISTS `testdb` CHARACTER SET = utf8 COLLATE = utf8_general_ci;"},
	)

	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` TINYINT(1));", "foo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` TINYINT(1));"},
	)

	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE IF NOT EXISTS `foo`(`bar` TINYINT(1));", "foo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` TINYINT(1));"},
	)

	// case insensitive
	c.Assert(
		createSQLIfNotExistsStmt("/* cOmmEnt */ creAte tablE `fOo`(`bar` TinyinT(1));", "fOo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`fOo` (`bar` TINYINT(1));"},
	)

	c.Assert(
		createSQLIfNotExistsStmt("/* coMMenT */ crEatE tAble If not EXISts `FoO`(`bAR` tiNyInT(1));", "FoO"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`FoO` (`bAR` TINYINT(1));"},
	)

	// only one "CREATE TABLE" is replaced
	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) COMMENT 'CREATE TABLE');", "foo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) COMMENT 'CREATE TABLE');"},
	)

	// test clustered index consistency
	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) PRIMARY KEY CLUSTERED COMMENT 'CREATE TABLE');", "foo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) PRIMARY KEY /*T![clustered_index] CLUSTERED */ COMMENT 'CREATE TABLE');"},
	)
	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) COMMENT 'CREATE TABLE', PRIMARY KEY (`bar`) NONCLUSTERED);", "foo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) COMMENT 'CREATE TABLE',PRIMARY KEY(`bar`) /*T![clustered_index] NONCLUSTERED */);"},
	)
	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) PRIMARY KEY /*T![clustered_index] NONCLUSTERED */ COMMENT 'CREATE TABLE');", "foo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) PRIMARY KEY /*T![clustered_index] NONCLUSTERED */ COMMENT 'CREATE TABLE');"},
	)
	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) COMMENT 'CREATE TABLE', PRIMARY KEY (`bar`) /*T![clustered_index] CLUSTERED */);", "foo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) COMMENT 'CREATE TABLE',PRIMARY KEY(`bar`) /*T![clustered_index] CLUSTERED */);"},
	)

	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `foo`(`bar` INT(1) PRIMARY KEY AUTO_RANDOM(2) COMMENT 'CREATE TABLE');", "foo"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`foo` (`bar` INT(1) PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(2) */ COMMENT 'CREATE TABLE');"},
	)

	// upper case becomes shorter
	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `ſ`(`ı` TINYINT(1));", "ſ"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`ſ` (`ı` TINYINT(1));"},
	)

	// upper case becomes longer
	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `ɑ`(`ȿ` TINYINT(1));", "ɑ"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`ɑ` (`ȿ` TINYINT(1));"},
	)

	// non-utf-8
	c.Assert(
		createSQLIfNotExistsStmt("CREATE TABLE `\xcc\xcc\xcc`(`\xdd\xdd\xdd` TINYINT(1));", "\xcc\xcc\xcc"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`\xcc\xcc\xcc` (`???` TINYINT(1));"},
	)

	// renaming a table
	c.Assert(
		createSQLIfNotExistsStmt("create table foo(x int);", "ba`r"),
		DeepEquals,
		[]string{"CREATE TABLE IF NOT EXISTS `testdb`.`ba``r` (`x` INT);"},
	)

	// conditional comments
	c.Assert(
		createSQLIfNotExistsStmt(`
			/*!40101 SET NAMES binary*/;
			/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
			CREATE TABLE x.y (z double) ENGINE=InnoDB AUTO_INCREMENT=8343230 DEFAULT CHARSET=utf8;
		`, "m"),
		DeepEquals,
		[]string{
			"SET NAMES 'binary';",
			"SET @@SESSION.`FOREIGN_KEY_CHECKS`=0;",
			"CREATE TABLE IF NOT EXISTS `testdb`.`m` (`z` DOUBLE) ENGINE = InnoDB AUTO_INCREMENT = 8343230 DEFAULT CHARACTER SET = UTF8;",
		},
	)

	// create view
	c.Assert(
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
		`, "m"),
		DeepEquals,
		[]string{
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
	)
}

func (s *tidbSuite) TestInitSchema(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("CREATE DATABASE IF NOT EXISTS `db`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectExec("\\QCREATE TABLE IF NOT EXISTS `db`.`t1` (`a` INT PRIMARY KEY,`b` VARCHAR(200));\\E").
		WillReturnResult(sqlmock.NewResult(2, 1))
	s.mockDB.
		ExpectExec("\\QSET @@SESSION.`FOREIGN_KEY_CHECKS`=0;\\E").
		WillReturnResult(sqlmock.NewResult(0, 0))
	s.mockDB.
		ExpectExec("\\QCREATE TABLE IF NOT EXISTS `db`.`t2` (`xx` TEXT) AUTO_INCREMENT = 11203;\\E").
		WillReturnResult(sqlmock.NewResult(2, 1))
	s.mockDB.
		ExpectClose()

	s.mockDB.MatchExpectationsInOrder(false) // maps are unordered.
	err := InitSchema(ctx, s.tiGlue, "db", map[string]string{
		"t1": "create table t1 (a int primary key, b varchar(200));",
		"t2": "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;CREATE TABLE `db`.`t2` (xx TEXT) AUTO_INCREMENT=11203;",
	})
	s.mockDB.MatchExpectationsInOrder(true)
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestInitSchemaSyntaxError(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("CREATE DATABASE IF NOT EXISTS `db`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := InitSchema(ctx, s.tiGlue, "db", map[string]string{
		"t1": "create table `t1` with invalid syntax;",
	})
	c.Assert(err, NotNil)
}

func (s *tidbSuite) TestInitSchemaErrorLost(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("CREATE DATABASE IF NOT EXISTS `db`").
		WillReturnResult(sqlmock.NewResult(1, 1))

	s.mockDB.
		ExpectExec("CREATE TABLE IF NOT EXISTS.*").
		WillReturnError(&mysql.MySQLError{
			Number:  tmysql.ErrTooBigFieldlength,
			Message: "Column length too big",
		})

	s.mockDB.
		ExpectClose()

	err := InitSchema(ctx, s.tiGlue, "db", map[string]string{
		"t1": "create table `t1` (a int);",
		"t2": "create table t2 (a int primary key, b varchar(200));",
	})
	c.Assert(err, ErrorMatches, ".*Column length too big.*")
}

func (s *tidbSuite) TestInitSchemaUnsupportedSchemaError(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("CREATE DATABASE IF NOT EXISTS `db`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectExec("CREATE TABLE IF NOT EXISTS `db`.`t1`.*").
		WillReturnError(&mysql.MySQLError{
			Number:  tmysql.ErrTooBigFieldlength,
			Message: "Column length too big",
		})
	s.mockDB.
		ExpectClose()

	err := InitSchema(ctx, s.tiGlue, "db", map[string]string{
		"t1": "create table `t1` (a VARCHAR(999999999));",
	})
	c.Assert(err, ErrorMatches, ".*Column length too big.*")
}

func (s *tidbSuite) TestDropTable(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("DROP TABLE `db`.`table`").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := s.timgr.DropTable(ctx, "`db`.`table`")
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestLoadSchemaInfo(c *C) {
	ctx := context.Background()

	tableCntBefore := metric.ReadCounter(metric.TableCounter.WithLabelValues(metric.TableStatePending, metric.TableResultSuccess))

	// Prepare the mock reply.
	nodes, _, err := s.timgr.parser.Parse(
		"CREATE TABLE `t1` (`a` INT PRIMARY KEY);"+
			"CREATE TABLE `t2` (`b` VARCHAR(20), `c` BOOL, KEY (`b`, `c`));"+
			// an extra table that not exists in dbMetas
			"CREATE TABLE `t3` (`d` VARCHAR(20), `e` BOOL);",
		"", "")
	c.Assert(err, IsNil)
	tableInfos := make([]*model.TableInfo, 0, len(nodes))
	sctx := mock.NewContext()
	for i, node := range nodes {
		c.Assert(node, FitsTypeOf, &ast.CreateTableStmt{})
		info, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), int64(i+100))
		c.Assert(err, IsNil)
		info.State = model.StatePublic
		tableInfos = append(tableInfos, info)
	}

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name: "db",
			Tables: []*mydump.MDTableMeta{
				{
					DB:   "db",
					Name: "t1",
				},
				{
					DB:   "db",
					Name: "t2",
				},
			},
		},
	}

	loaded, err := LoadSchemaInfo(ctx, dbMetas, func(ctx context.Context, schema string) ([]*model.TableInfo, error) {
		c.Assert(schema, Equals, "db")
		return tableInfos, nil
	})
	c.Assert(err, IsNil)
	c.Assert(loaded, DeepEquals, map[string]*checkpoints.TidbDBInfo{
		"db": {
			Name: "db",
			Tables: map[string]*checkpoints.TidbTableInfo{
				"t1": {
					ID:   100,
					DB:   "db",
					Name: "t1",
					Core: tableInfos[0],
				},
				"t2": {
					ID:   101,
					DB:   "db",
					Name: "t2",
					Core: tableInfos[1],
				},
			},
		},
	})

	tableCntAfter := metric.ReadCounter(metric.TableCounter.WithLabelValues(metric.TableStatePending, metric.TableResultSuccess))

	c.Assert(tableCntAfter-tableCntBefore, Equals, 2.0)
}

func (s *tidbSuite) TestLoadSchemaInfoMissing(c *C) {
	ctx := context.Background()

	_, err := LoadSchemaInfo(ctx, []*mydump.MDDatabaseMeta{{Name: "asdjalsjdlas"}}, func(ctx context.Context, schema string) ([]*model.TableInfo, error) {
		return nil, errors.Errorf("[schema:1049]Unknown database '%s'", schema)
	})
	c.Assert(err, ErrorMatches, ".*Unknown database.*")
}

func (s *tidbSuite) TestGetGCLifetime(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	s.mockDB.
		ExpectClose()

	res, err := ObtainGCLifeTime(ctx, s.timgr.db)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, "10m")
}

func (s *tidbSuite) TestSetGCLifetime(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("12m").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := UpdateGCLifeTime(ctx, s.timgr.db, "12m")
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestAlterAutoInc(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("\\QALTER TABLE `db`.`table` AUTO_INCREMENT=12345\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := AlterAutoIncrement(ctx, s.tiGlue.GetSQLExecutor(), "`db`.`table`", 12345)
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestAlterAutoRandom(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectExec("\\QALTER TABLE `db`.`table` AUTO_RANDOM_BASE=12345\\E").
		WillReturnResult(sqlmock.NewResult(1, 1))
	s.mockDB.
		ExpectClose()

	err := AlterAutoRandom(ctx, s.tiGlue.GetSQLExecutor(), "`db`.`table`", 12345)
	c.Assert(err, IsNil)
}

func (s *tidbSuite) TestObtainRowFormatVersionSucceed(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectBegin()
	s.mockDB.
		ExpectQuery(`SHOW VARIABLES WHERE Variable_name IN \(.*'tidb_row_format_version'.*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2").
			AddRow("max_allowed_packet", "1073741824").
			AddRow("div_precision_increment", "10").
			AddRow("time_zone", "-08:00").
			AddRow("lc_time_names", "ja_JP").
			AddRow("default_week_format", "1").
			AddRow("block_encryption_mode", "aes-256-cbc").
			AddRow("group_concat_max_len", "1073741824"))
	s.mockDB.
		ExpectCommit()
	s.mockDB.
		ExpectClose()

	sysVars := ObtainImportantVariables(ctx, s.tiGlue.GetSQLExecutor(), true)
	c.Assert(sysVars, DeepEquals, map[string]string{
		"tidb_row_format_version": "2",
		"max_allowed_packet":      "1073741824",
		"div_precision_increment": "10",
		"time_zone":               "-08:00",
		"lc_time_names":           "ja_JP",
		"default_week_format":     "1",
		"block_encryption_mode":   "aes-256-cbc",
		"group_concat_max_len":    "1073741824",
	})
}

func (s *tidbSuite) TestObtainRowFormatVersionFailure(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectBegin()
	s.mockDB.
		ExpectQuery(`SHOW VARIABLES WHERE Variable_name IN \(.*'tidb_row_format_version'.*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("time_zone", "+00:00"))
	s.mockDB.
		ExpectCommit()
	s.mockDB.
		ExpectClose()

	sysVars := ObtainImportantVariables(ctx, s.tiGlue.GetSQLExecutor(), true)
	c.Assert(sysVars, DeepEquals, map[string]string{
		"tidb_row_format_version": "1",
		"max_allowed_packet":      "67108864",
		"div_precision_increment": "4",
		"time_zone":               "+00:00",
		"lc_time_names":           "en_US",
		"default_week_format":     "0",
		"block_encryption_mode":   "aes-128-ecb",
		"group_concat_max_len":    "1024",
	})
}

func (s *tidbSuite) TestObtainNewCollationEnabled(c *C) {
	ctx := context.Background()

	s.mockDB.
		ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
		WillReturnError(errors.New("mock permission deny"))
	s.mockDB.
		ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
		WillReturnError(errors.New("mock permission deny"))
	s.mockDB.
		ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
		WillReturnError(errors.New("mock permission deny"))
	_, err := ObtainNewCollationEnabled(ctx, s.tiGlue.GetSQLExecutor())
	c.Assert(err, ErrorMatches, "obtain new collation enabled failed: mock permission deny")

	s.mockDB.
		ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"variable_value"}).RowError(0, sql.ErrNoRows))
	version, err := ObtainNewCollationEnabled(ctx, s.tiGlue.GetSQLExecutor())
	c.Assert(err, IsNil)
	c.Assert(version, Equals, false)

	kvMap := map[string]bool{
		"True":  true,
		"False": false,
	}
	for k, v := range kvMap {
		s.mockDB.
			ExpectQuery("\\QSELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'\\E").
			WillReturnRows(sqlmock.NewRows([]string{"variable_value"}).AddRow(k))

		version, err = ObtainNewCollationEnabled(ctx, s.tiGlue.GetSQLExecutor())
		c.Assert(err, IsNil)
		c.Assert(version, Equals, v)
	}
	s.mockDB.
		ExpectClose()
}
