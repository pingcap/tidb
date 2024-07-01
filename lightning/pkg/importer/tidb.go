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

package importer

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// TiDBManager is a wrapper of *sql.DB which provides some helper methods for
type TiDBManager struct {
	db     *sql.DB
	parser *parser.Parser
}

// DBFromConfig creates a new connection to the TiDB database.
func DBFromConfig(ctx context.Context, dsn config.DBStore) (*sql.DB, error) {
	param := common.MySQLConnectParam{
		Host:                     dsn.Host,
		Port:                     dsn.Port,
		User:                     dsn.User,
		Password:                 dsn.Psw,
		SQLMode:                  dsn.StrSQLMode,
		MaxAllowedPacket:         dsn.MaxAllowedPacket,
		TLSConfig:                dsn.Security.TLSConfig,
		AllowFallbackToPlaintext: dsn.Security.AllowFallbackToPlaintext,
		Net:                      dsn.UUID,
	}

	db, err := param.Connect()
	if err != nil {
		return nil, errors.Trace(err)
	}

	vars := map[string]string{
		variable.TiDBBuildStatsConcurrency:      strconv.Itoa(dsn.BuildStatsConcurrency),
		variable.TiDBDistSQLScanConcurrency:     strconv.Itoa(dsn.DistSQLScanConcurrency),
		variable.TiDBIndexSerialScanConcurrency: strconv.Itoa(dsn.IndexSerialScanConcurrency),
		variable.TiDBChecksumTableConcurrency:   strconv.Itoa(dsn.ChecksumTableConcurrency),

		// after https://github.com/pingcap/tidb/pull/17102 merge,
		// we need set session to true for insert auto_random value in TiDB Backend
		variable.TiDBAllowAutoRandExplicitInsert: "1",
		// allow use _tidb_rowid in sql statement
		variable.TiDBOptWriteRowID: "1",
		// always set auto-commit to ON
		variable.AutoCommit: "1",
		// always set transaction mode to optimistic
		variable.TiDBTxnMode: "optimistic",
		// disable foreign key checks
		variable.ForeignKeyChecks:              "0",
		variable.TiDBExplicitRequestSourceType: util.ExplicitTypeLightning,
	}

	if dsn.Vars != nil {
		maps.Copy(vars, dsn.Vars)
	}

	for k, v := range vars {
		q := fmt.Sprintf("SET SESSION %s = '%s';", k, v)
		if _, err1 := db.ExecContext(ctx, q); err1 != nil {
			log.FromContext(ctx).Warn("set session variable failed, will skip this query", zap.String("query", q),
				zap.Error(err1))
			delete(vars, k)
		}
	}
	_ = db.Close()

	param.Vars = vars
	db, err = param.Connect()
	return db, errors.Trace(err)
}

// NewTiDBManager creates a new TiDB manager.
func NewTiDBManager(ctx context.Context, dsn config.DBStore, _ *common.TLS) (*TiDBManager, error) {
	db, err := DBFromConfig(ctx, dsn)
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

// Close closes the underlying database connection.
func (timgr *TiDBManager) Close() {
	timgr.db.Close()
}

// DropTable drops a table.
func (timgr *TiDBManager) DropTable(ctx context.Context, tableName string) error {
	sql := common.SQLWithRetry{
		DB:     timgr.db,
		Logger: log.FromContext(ctx).With(zap.String("table", tableName)),
	}
	return sql.Exec(ctx, "drop table", "DROP TABLE "+tableName)
}

// LoadSchemaInfo loads schema information from TiDB.
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
				return nil, common.ErrSchemaNotExists.GenWithStackByArgs(tbl.DB, tbl.Name)
			}
			tableName := tblInfo.Name.String()
			if tblInfo.State != model.StatePublic {
				err := errors.Errorf("table [%s.%s] state is not public", schema.Name, tableName)
				if m, ok := metric.FromContext(ctx); ok {
					m.RecordTableCount(metric.TableStatePending, err)
				}
				return nil, errors.Trace(err)
			}
			if m, ok := metric.FromContext(ctx); ok {
				m.RecordTableCount(metric.TableStatePending, err)
			}
			// Table names are case-sensitive in mydump.MDTableMeta.
			// We should always use the original tbl.Name in checkpoints.
			tableInfo := &checkpoints.TidbTableInfo{
				ID:      tblInfo.ID,
				DB:      schema.Name,
				Name:    tbl.Name,
				Core:    tblInfo,
				Desired: tblInfo,
			}
			dbInfo.Tables[tbl.Name] = tableInfo
		}

		result[schema.Name] = dbInfo
	}
	return result, nil
}

// ObtainImportantVariables obtains the important variables from TiDB.
func ObtainImportantVariables(ctx context.Context, db *sql.DB, needTiDBVars bool) map[string]string {
	var query strings.Builder
	query.WriteString("SHOW VARIABLES WHERE Variable_name IN ('")
	first := true
	for k := range common.DefaultImportantVariables {
		if first {
			first = false
		} else {
			query.WriteString("','")
		}
		query.WriteString(k)
	}
	if needTiDBVars {
		for k := range common.DefaultImportVariablesTiDB {
			query.WriteString("','")
			query.WriteString(k)
		}
	}
	query.WriteString("')")
	exec := common.SQLWithRetry{DB: db, Logger: log.FromContext(ctx)}
	kvs, err := exec.QueryStringRows(ctx, "obtain system variables", query.String())
	if err != nil {
		// error is not fatal
		log.FromContext(ctx).Warn("obtain system variables failed, use default variables instead", log.ShortError(err))
	}

	// convert result into a map. fill in any missing variables with default values.
	result := make(map[string]string, len(common.DefaultImportantVariables)+len(common.DefaultImportVariablesTiDB))
	for _, kv := range kvs {
		result[kv[0]] = kv[1]
	}

	setDefaultValue := func(res map[string]string, vars map[string]string) {
		for k, defV := range vars {
			if _, ok := res[k]; !ok {
				res[k] = defV
			}
		}
	}
	setDefaultValue(result, common.DefaultImportantVariables)
	if needTiDBVars {
		setDefaultValue(result, common.DefaultImportVariablesTiDB)
	}

	return result
}

// ObtainNewCollationEnabled obtains the new collation enabled status from TiDB.
func ObtainNewCollationEnabled(ctx context.Context, db *sql.DB) (bool, error) {
	newCollationEnabled := false
	var newCollationVal string
	exec := common.SQLWithRetry{DB: db, Logger: log.FromContext(ctx)}
	err := exec.QueryRow(ctx, "obtain new collation enabled", "SELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'", &newCollationVal)
	if err == nil && newCollationVal == "True" {
		newCollationEnabled = true
	} else if errors.ErrorEqual(err, sql.ErrNoRows) {
		// ignore if target variable is not found, this may happen if tidb < v4.0
		newCollationEnabled = false
		err = nil
	}

	return newCollationEnabled, errors.Trace(err)
}

// AlterAutoIncrement rebase the table auto increment id
//
// NOTE: since tidb can make sure the auto id is always be rebase even if the `incr` value is smaller
// than the auto increment base in tidb side, we needn't fetch currently auto increment value here.
// See: https://github.com/pingcap/tidb/blob/64698ef9a3358bfd0fdc323996bb7928a56cadca/ddl/ddl_api.go#L2528-L2533
func AlterAutoIncrement(ctx context.Context, db *sql.DB, tableName string, incr uint64) error {
	logger := log.FromContext(ctx).With(zap.String("table", tableName), zap.Uint64("auto_increment", incr))
	base := adjustIDBase(incr)
	var forceStr string
	if incr > math.MaxInt64 {
		// automatically set max value
		logger.Warn("auto_increment out of the maximum value TiDB supports, automatically set to the max", zap.Uint64("auto_increment", incr))
		forceStr = "FORCE"
	}
	query := fmt.Sprintf("ALTER TABLE %s %s AUTO_INCREMENT=%d", tableName, forceStr, base)
	task := logger.Begin(zap.InfoLevel, "alter table auto_increment")
	exec := common.SQLWithRetry{DB: db, Logger: logger}
	err := exec.Exec(ctx, "alter table auto_increment", query)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_increment failed, please perform the query manually (this is needed no matter the table has an auto-increment column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}

func adjustIDBase(incr uint64) int64 {
	if incr > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(incr)
}

// AlterAutoRandom rebase the table auto random id
func AlterAutoRandom(ctx context.Context, db *sql.DB, tableName string, randomBase uint64, maxAutoRandom uint64) error {
	logger := log.FromContext(ctx).With(zap.String("table", tableName), zap.Uint64("auto_random", randomBase))
	if randomBase == maxAutoRandom+1 {
		// insert a tuple with key maxAutoRandom
		randomBase = maxAutoRandom
	} else if randomBase > maxAutoRandom {
		// TiDB does nothing when inserting an overflow value
		logger.Warn("auto_random out of the maximum value TiDB supports")
		return nil
	}
	// if new base is smaller than current, this query will success with a warning
	query := fmt.Sprintf("ALTER TABLE %s AUTO_RANDOM_BASE=%d", tableName, randomBase)
	task := logger.Begin(zap.InfoLevel, "alter table auto_random")
	exec := common.SQLWithRetry{DB: db, Logger: logger}
	err := exec.Exec(ctx, "alter table auto_random_base", query)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_random_base failed, please perform the query manually (this is needed no matter the table has an auto-random column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}
