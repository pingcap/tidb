// Copyright 2022 PingCAP, Inc.
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

package dbutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// TableName returns `schema`.`table`
func TableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

// ColumnName returns `column`
func ColumnName(column string) string {
	return fmt.Sprintf("`%s`", escapeName(column))
}

func escapeName(name string) string {
	return strings.ReplaceAll(name, "`", "``")
}

// ReplacePlaceholder will use args to replace '?', used for log.
// tips: make sure the num of "?" is same with len(args)
func ReplacePlaceholder(str string, args []string) string {
	/*
		for example:
		str is "a > ? AND a < ?", args is {'1', '2'},
		this function will return "a > '1' AND a < '2'"
	*/
	newStr := strings.ReplaceAll(str, "?", "'%s'")
	return fmt.Sprintf(newStr, util.StringsToInterfaces(args)...)
}

// ExecSQLWithRetry executes sql with retry
func ExecSQLWithRetry(ctx context.Context, db DBExecutor, sql string, args ...any) (err error) {
	for i := range DefaultRetryTime {
		startTime := time.Now()
		_, err = db.ExecContext(ctx, sql, args...)
		takeDuration := time.Since(startTime)
		if takeDuration > SlowLogThreshold {
			log.Debug("exec sql slow", zap.String("sql", sql), zap.Reflect("args", args), zap.Duration("take", takeDuration))
		}
		if err == nil {
			return nil
		}

		if ignoreError(err) {
			log.Warn("ignore execute sql error", zap.Error(err))
			return nil
		}

		if !IsRetryableError(err) {
			return errors.Trace(err)
		}

		log.Warn("exe sql failed, will try again", zap.String("sql", sql), zap.Reflect("args", args), zap.Error(err))

		if i == DefaultRetryTime-1 {
			break
		}

		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	return errors.Trace(err)
}

// ExecuteSQLs executes some sqls in one transaction
func ExecuteSQLs(ctx context.Context, db DBExecutor, sqls []string, args [][]any) error {
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("exec sqls begin", zap.Error(err))
		return errors.Trace(err)
	}

	for i := range sqls {
		startTime := time.Now()

		_, err = txn.ExecContext(ctx, sqls[i], args[i]...)
		if err != nil {
			log.Error("exec sql", zap.String("sql", sqls[i]), zap.Reflect("args", args[i]), zap.Error(err))
			rerr := txn.Rollback()
			if rerr != nil {
				log.Error("rollback", zap.Error(err))
			}
			return errors.Trace(err)
		}

		takeDuration := time.Since(startTime)
		if takeDuration > SlowLogThreshold {
			log.Debug("exec sql slow", zap.String("sql", sqls[i]), zap.Reflect("args", args[i]), zap.Duration("take", takeDuration))
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Error("exec sqls commit", zap.Error(err))
		return errors.Trace(err)
	}

	return nil
}

func ignoreError(err error) bool {
	// TODO: now only ignore some ddl error, add some dml error later
	return ignoreDDLError(err)
}

func ignoreDDLError(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := errors.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrIndexExists.Code():
		return true
	case dbterror.ErrDupKeyName.Code():
		return true
	default:
		return false
	}
}

// DeleteRows delete rows in several times. Only can delete less than 300,000 one time in TiDB.
func DeleteRows(ctx context.Context, db DBExecutor, schemaName string, tableName string, where string, args []any) error {
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s limit %d;", TableName(schemaName, tableName), where, DefaultDeleteRowsNum)
	result, err := db.ExecContext(ctx, deleteSQL, args...)
	if err != nil {
		return errors.Trace(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Trace(err)
	}

	if rows < DefaultDeleteRowsNum {
		return nil
	}

	return DeleteRows(ctx, db, schemaName, tableName, where, args)
}

// getParser gets parser according to sql mode
func getParser(sqlModeStr string) (*parser.Parser, error) {
	if len(sqlModeStr) == 0 {
		return parser.New(), nil
	}

	sqlMode, err := tmysql.GetSQLMode(tmysql.FormatSQLModeStr(sqlModeStr))
	if err != nil {
		return nil, errors.Annotatef(err, "invalid sql mode %s", sqlModeStr)
	}
	parser2 := parser.New()
	parser2.SetSQLMode(sqlMode)
	return parser2, nil
}

// GetParserForDB discovers ANSI_QUOTES in db's session variables and returns a proper parser
func GetParserForDB(ctx context.Context, db QueryExecutor) (*parser.Parser, error) {
	mode, err := GetSQLMode(ctx, db)
	if err != nil {
		return nil, err
	}

	parser2 := parser.New()
	parser2.SetSQLMode(mode)
	return parser2, nil
}
