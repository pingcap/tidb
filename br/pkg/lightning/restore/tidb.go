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
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	tmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"go.uber.org/zap"
)

// defaultImportantVariables is used in ObtainImportantVariables to retrieve the system
// variables from downstream which may affect KV encode result. The values record the default
// values if missing.
var defaultImportantVariables = map[string]string{
	"tidb_row_format_version": "1",
	"max_allowed_packet":      "67108864",
	"div_precision_increment": "4",
	"time_zone":               "SYSTEM",
	"lc_time_names":           "en_US",
	"default_week_format":     "0",
	"block_encryption_mode":   "aes-128-ecb",
	"group_concat_max_len":    "1024",
}

type TiDBManager struct {
	db     *sql.DB
	parser *parser.Parser
}

// getSQLErrCode returns error code if err is a mysql error
func getSQLErrCode(err error) (terror.ErrCode, bool) {
	mysqlErr, ok := errors.Cause(err).(*tmysql.MySQLError)
	if !ok {
		return -1, false
	}

	return terror.ErrCode(mysqlErr.Number), true
}

func isUnknownSystemVariableErr(err error) bool {
	code, ok := getSQLErrCode(err)
	if !ok {
		return strings.Contains(err.Error(), "Unknown system variable")
	}
	return code == mysql.ErrUnknownSystemVariable
}

func DBFromConfig(dsn config.DBStore) (*sql.DB, error) {
	param := common.MySQLConnectParam{
		Host:             dsn.Host,
		Port:             dsn.Port,
		User:             dsn.User,
		Password:         dsn.Psw,
		SQLMode:          dsn.StrSQLMode,
		MaxAllowedPacket: dsn.MaxAllowedPacket,
		TLS:              dsn.TLS,
		Vars: map[string]string{
			"tidb_build_stats_concurrency":       strconv.Itoa(dsn.BuildStatsConcurrency),
			"tidb_distsql_scan_concurrency":      strconv.Itoa(dsn.DistSQLScanConcurrency),
			"tidb_index_serial_scan_concurrency": strconv.Itoa(dsn.IndexSerialScanConcurrency),
			"tidb_checksum_table_concurrency":    strconv.Itoa(dsn.ChecksumTableConcurrency),

			// after https://github.com/pingcap/tidb/pull/17102 merge,
			// we need set session to true for insert auto_random value in TiDB Backend
			"allow_auto_random_explicit_insert": "1",
			// allow use _tidb_rowid in sql statement
			"tidb_opt_write_row_id": "1",
			// always set auto-commit to ON
			"autocommit": "1",
		},
	}
	db, err := param.Connect()
	if err != nil {
		if isUnknownSystemVariableErr(err) {
			// not support allow_auto_random_explicit_insert, retry connect
			delete(param.Vars, "allow_auto_random_explicit_insert")
			db, err = param.Connect()
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			return nil, errors.Trace(err)
		}
	}
	return db, nil
}

func NewTiDBManager(dsn config.DBStore, tls *common.TLS) (*TiDBManager, error) {
	db, err := DBFromConfig(dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewTiDBManagerWithDB(db, dsn.SQLMode), nil
}

// NewTiDBManagerWithDB creates a new TiDB manager with an existing database
// connection.
func NewTiDBManagerWithDB(db *sql.DB, sqlMode mysql.SQLMode) *TiDBManager {
	parser := parser.New()
	parser.SetSQLMode(sqlMode)

	return &TiDBManager{
		db:     db,
		parser: parser,
	}
}

func (timgr *TiDBManager) Close() {
	timgr.db.Close()
}

func InitSchema(ctx context.Context, g glue.Glue, database string, tablesSchema map[string]string) error {
	logger := log.With(zap.String("db", database))
	sqlExecutor := g.GetSQLExecutor()

	var createDatabase strings.Builder
	createDatabase.WriteString("CREATE DATABASE IF NOT EXISTS ")
	common.WriteMySQLIdentifier(&createDatabase, database)
	err := sqlExecutor.ExecuteWithLog(ctx, createDatabase.String(), "create database", logger)
	if err != nil {
		return errors.Trace(err)
	}

	task := logger.Begin(zap.InfoLevel, "create tables")
	var sqlCreateStmts []string
loopCreate:
	for tbl, sqlCreateTable := range tablesSchema {
		task.Debug("create table", zap.String("schema", sqlCreateTable))

		sqlCreateStmts, err = createTableIfNotExistsStmt(g.GetParser(), sqlCreateTable, database, tbl)
		if err != nil {
			break
		}

		// TODO: maybe we should put these createStems into a transaction
		for _, s := range sqlCreateStmts {
			err = sqlExecutor.ExecuteWithLog(
				ctx,
				s,
				"create table",
				logger.With(zap.String("table", common.UniqueTable(database, tbl))),
			)
			if err != nil {
				break loopCreate
			}
		}
	}
	task.End(zap.ErrorLevel, err)

	return errors.Trace(err)
}

func createDatabaseIfNotExistStmt(dbName string) string {
	var createDatabase strings.Builder
	createDatabase.WriteString("CREATE DATABASE IF NOT EXISTS ")
	common.WriteMySQLIdentifier(&createDatabase, dbName)
	return createDatabase.String()
}

func createTableIfNotExistsStmt(p *parser.Parser, createTable, dbName, tblName string) ([]string, error) {
	stmts, _, err := p.Parse(createTable, "", "")
	if err != nil {
		return []string{}, err
	}

	var res strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &res)

	retStmts := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		switch node := stmt.(type) {
		case *ast.CreateTableStmt:
			node.Table.Schema = model.NewCIStr(dbName)
			node.Table.Name = model.NewCIStr(tblName)
			node.IfNotExists = true
		case *ast.CreateViewStmt:
			node.ViewName.Schema = model.NewCIStr(dbName)
			node.ViewName.Name = model.NewCIStr(tblName)
		case *ast.DropTableStmt:
			node.Tables[0].Schema = model.NewCIStr(dbName)
			node.Tables[0].Name = model.NewCIStr(tblName)
			node.IfExists = true
		}
		if err := stmt.Restore(ctx); err != nil {
			return []string{}, err
		}
		ctx.WritePlain(";")
		retStmts = append(retStmts, res.String())
		res.Reset()
	}

	return retStmts, nil
}

func (timgr *TiDBManager) DropTable(ctx context.Context, tableName string) error {
	sql := common.SQLWithRetry{
		DB:     timgr.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	return sql.Exec(ctx, "drop table", "DROP TABLE "+tableName)
}

func LoadSchemaInfo(
	ctx context.Context,
	schemas []*mydump.MDDatabaseMeta,
	getTables func(context.Context, string) ([]*model.TableInfo, error),
) (map[string]*checkpoints.TidbDBInfo, error) {
	result := make(map[string]*checkpoints.TidbDBInfo, len(schemas))
	for _, schema := range schemas {
		tables, err := getTables(ctx, schema.Name)
		if err != nil {
			return nil, err
		}

		tableMap := make(map[string]*model.TableInfo, len(tables))
		for _, tbl := range tables {
			tableMap[tbl.Name.L] = tbl
		}

		dbInfo := &checkpoints.TidbDBInfo{
			Name:   schema.Name,
			Tables: make(map[string]*checkpoints.TidbTableInfo),
		}

		for _, tbl := range schema.Tables {
			tblInfo, ok := tableMap[strings.ToLower(tbl.Name)]
			if !ok {
				return nil, errors.Errorf("table '%s' schema not found", tbl.Name)
			}
			tableName := tblInfo.Name.String()
			if tblInfo.State != model.StatePublic {
				err := errors.Errorf("table [%s.%s] state is not public", schema.Name, tableName)
				metric.RecordTableCount(metric.TableStatePending, err)
				return nil, err
			}
			metric.RecordTableCount(metric.TableStatePending, err)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tableInfo := &checkpoints.TidbTableInfo{
				ID:   tblInfo.ID,
				DB:   schema.Name,
				Name: tableName,
				Core: tblInfo,
			}
			dbInfo.Tables[tableName] = tableInfo
		}

		result[schema.Name] = dbInfo
	}
	return result, nil
}

func ObtainGCLifeTime(ctx context.Context, db *sql.DB) (string, error) {
	var gcLifeTime string
	err := common.SQLWithRetry{DB: db, Logger: log.L()}.QueryRow(
		ctx,
		"obtain GC lifetime",
		"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		&gcLifeTime,
	)
	return gcLifeTime, err
}

func UpdateGCLifeTime(ctx context.Context, db *sql.DB, gcLifeTime string) error {
	sql := common.SQLWithRetry{
		DB:     db,
		Logger: log.With(zap.String("gcLifeTime", gcLifeTime)),
	}
	return sql.Exec(ctx, "update GC lifetime",
		"UPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		gcLifeTime,
	)
}

func ObtainImportantVariables(ctx context.Context, g glue.SQLExecutor) map[string]string {
	var query strings.Builder
	query.WriteString("SHOW VARIABLES WHERE Variable_name IN ('")
	first := true
	for k := range defaultImportantVariables {
		if first {
			first = false
		} else {
			query.WriteString("','")
		}
		query.WriteString(k)
	}
	query.WriteString("')")
	kvs, err := g.QueryStringsWithLog(ctx, query.String(), "obtain system variables", log.L())
	if err != nil {
		// error is not fatal
		log.L().Warn("obtain system variables failed, use default variables instead", log.ShortError(err))
	}

	// convert result into a map. fill in any missing variables with default values.
	result := make(map[string]string, len(defaultImportantVariables))
	for _, kv := range kvs {
		result[kv[0]] = kv[1]
	}
	for k, defV := range defaultImportantVariables {
		if _, ok := result[k]; !ok {
			result[k] = defV
		}
	}

	return result
}

func ObtainNewCollationEnabled(ctx context.Context, g glue.SQLExecutor) bool {
	newCollationEnabled := false
	newCollationVal, err := g.ObtainStringWithLog(
		ctx,
		"SELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'",
		"obtain new collation enabled",
		log.L(),
	)
	if err == nil && newCollationVal == "True" {
		newCollationEnabled = true
	}

	return newCollationEnabled
}

// AlterAutoIncrement rebase the table auto increment id
//
// NOTE: since tidb can make sure the auto id is always be rebase even if the `incr` value is smaller
// the the auto incremanet base in tidb side, we needn't fetch currently auto increment value here.
// See: https://github.com/pingcap/tidb/blob/64698ef9a3358bfd0fdc323996bb7928a56cadca/ddl/ddl_api.go#L2528-L2533
func AlterAutoIncrement(ctx context.Context, g glue.SQLExecutor, tableName string, incr int64) error {
	logger := log.With(zap.String("table", tableName), zap.Int64("auto_increment", incr))
	query := fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT=%d", tableName, incr)
	task := logger.Begin(zap.InfoLevel, "alter table auto_increment")
	err := g.ExecuteWithLog(ctx, query, "alter table auto_increment", logger)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_increment failed, please perform the query manually (this is needed no matter the table has an auto-increment column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}

func AlterAutoRandom(ctx context.Context, g glue.SQLExecutor, tableName string, randomBase int64) error {
	logger := log.With(zap.String("table", tableName), zap.Int64("auto_random", randomBase))
	query := fmt.Sprintf("ALTER TABLE %s AUTO_RANDOM_BASE=%d", tableName, randomBase)
	task := logger.Begin(zap.InfoLevel, "alter table auto_random")
	err := g.ExecuteWithLog(ctx, query, "alter table auto_random_base", logger)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_random_base failed, please perform the query manually (this is needed no matter the table has an auto-random column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}
