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

package common

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/format"
	"go.uber.org/zap"
)

const (
	retryTimeout = 3 * time.Second

	defaultMaxRetry = 3
)

// MySQLConnectParam records the parameters needed to connect to a MySQL database.
type MySQLConnectParam struct {
	Host                     string
	Port                     int
	User                     string
	Password                 string
	SQLMode                  string
	MaxAllowedPacket         uint64
	TLSConfig                *tls.Config
	AllowFallbackToPlaintext bool
	Net                      string
	Vars                     map[string]string
}

// ToDriverConfig converts the MySQLConnectParam to a mysql.Config.
func (param *MySQLConnectParam) ToDriverConfig() *mysql.Config {
	cfg := mysql.NewConfig()
	cfg.Params = make(map[string]string)

	cfg.User = param.User
	cfg.Passwd = param.Password
	cfg.Net = "tcp"
	if param.Net != "" {
		cfg.Net = param.Net
	}
	cfg.Addr = net.JoinHostPort(param.Host, strconv.Itoa(param.Port))
	cfg.Params["charset"] = "utf8mb4"
	cfg.Params["sql_mode"] = fmt.Sprintf("'%s'", param.SQLMode)
	cfg.MaxAllowedPacket = int(param.MaxAllowedPacket)

	cfg.TLS = param.TLSConfig
	cfg.AllowFallbackToPlaintext = param.AllowFallbackToPlaintext

	for k, v := range param.Vars {
		cfg.Params[k] = fmt.Sprintf("'%s'", v)
	}
	return cfg
}

func tryConnectMySQL(cfg *mysql.Config) (*sql.DB, error) {
	failpoint.Inject("MustMySQLPassword", func(val failpoint.Value) {
		pwd := val.(string)
		if cfg.Passwd != pwd {
			failpoint.Return(nil, &mysql.MySQLError{Number: tmysql.ErrAccessDenied, Message: "access denied"})
		}
		failpoint.Return(nil, nil)
	})
	c, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db := sql.OpenDB(c)
	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, errors.Trace(err)
	}
	return db, nil
}

// ConnectMySQL connects MySQL with the dsn. If access is denied and the password is a valid base64 encoding,
// we will try to connect MySQL with the base64 decoding of the password.
func ConnectMySQL(cfg *mysql.Config) (*sql.DB, error) {
	// Try plain password first.
	db, firstErr := tryConnectMySQL(cfg)
	if firstErr == nil {
		return db, nil
	}
	// If access is denied and password is encoded by base64, try the decoded string as well.
	if mysqlErr, ok := errors.Cause(firstErr).(*mysql.MySQLError); ok && mysqlErr.Number == tmysql.ErrAccessDenied {
		// If password is encoded by base64, try the decoded string as well.
		password, decodeErr := base64.StdEncoding.DecodeString(cfg.Passwd)
		if decodeErr == nil && string(password) != cfg.Passwd {
			cfg.Passwd = string(password)
			db2, err := tryConnectMySQL(cfg)
			if err == nil {
				return db2, nil
			}
		}
	}
	// If we can't connect successfully, return the first error.
	return nil, errors.Trace(firstErr)
}

// Connect creates a new connection to the database.
func (param *MySQLConnectParam) Connect() (*sql.DB, error) {
	db, err := ConnectMySQL(param.ToDriverConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// IsDirExists checks if dir exists.
func IsDirExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		return false
	}
	return f != nil && f.IsDir()
}

// IsEmptyDir checks if dir is empty.
func IsEmptyDir(name string) bool {
	entries, err := os.ReadDir(name)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

// SQLWithRetry constructs a retryable transaction.
type SQLWithRetry struct {
	// either *sql.DB or *sql.Conn
	DB           dbutil.DBExecutor
	Logger       log.Logger
	HideQueryLog bool
}

func (SQLWithRetry) perform(_ context.Context, parentLogger log.Logger, purpose string, action func() error) error {
	return Retry(purpose, parentLogger, action)
}

// Retry is shared by SQLWithRetry.perform, implementation of GlueCheckpointsDB and TiDB's glue implementation
func Retry(purpose string, parentLogger log.Logger, action func() error) error {
	var err error
outside:
	for i := 0; i < defaultMaxRetry; i++ {
		logger := parentLogger.With(zap.Int("retryCnt", i))

		if i > 0 {
			logger.Warn(purpose + " retry start")
			time.Sleep(retryTimeout)
		}

		err = action()
		switch {
		case err == nil:
			return nil
		// do not retry NotFound error
		case errors.IsNotFound(err):
			break outside
		case IsRetryableError(err):
			logger.Warn(purpose+" failed but going to try again", log.ShortError(err))
			continue
		default:
			logger.Warn(purpose+" failed with no retry", log.ShortError(err))
			break outside
		}
	}

	return errors.Annotatef(err, "%s failed", purpose)
}

// QueryRow executes a query that is expected to return at most one row.
func (t SQLWithRetry) QueryRow(ctx context.Context, purpose string, query string, dest ...any) error {
	logger := t.Logger
	if !t.HideQueryLog {
		logger = logger.With(zap.String("query", query))
	}
	return t.perform(ctx, logger, purpose, func() error {
		return t.DB.QueryRowContext(ctx, query).Scan(dest...)
	})
}

// QueryStringRows executes a query that is expected to return multiple rows
// whose every column is string.
func (t SQLWithRetry) QueryStringRows(ctx context.Context, purpose string, query string) ([][]string, error) {
	var res [][]string
	logger := t.Logger
	if !t.HideQueryLog {
		logger = logger.With(zap.String("query", query))
	}

	err := t.perform(ctx, logger, purpose, func() error {
		rows, err := t.DB.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		colNames, err := rows.Columns()
		if err != nil {
			return err
		}
		for rows.Next() {
			row := make([]string, len(colNames))
			refs := make([]any, 0, len(row))
			for i := range row {
				refs = append(refs, &row[i])
			}
			if err := rows.Scan(refs...); err != nil {
				return err
			}
			res = append(res, row)
		}
		return rows.Err()
	})

	return res, err
}

// Transact executes an action in a transaction, and retry if the
// action failed with a retryable error.
func (t SQLWithRetry) Transact(ctx context.Context, purpose string, action func(context.Context, *sql.Tx) error) error {
	return t.perform(ctx, t.Logger, purpose, func() error {
		txn, err := t.DB.BeginTx(ctx, nil)
		if err != nil {
			return errors.Annotate(err, "begin transaction failed")
		}

		err = action(ctx, txn)
		if err != nil {
			rerr := txn.Rollback()
			if rerr != nil {
				t.Logger.Error(purpose+" rollback transaction failed", log.ShortError(rerr))
			}
			// we should return the exec err, instead of the rollback rerr.
			// no need to errors.Trace() it, as the error comes from user code anyway.
			return err
		}

		err = txn.Commit()
		if err != nil {
			return errors.Annotate(err, "commit transaction failed")
		}

		return nil
	})
}

// Exec executes a single SQL with optional retry.
func (t SQLWithRetry) Exec(ctx context.Context, purpose string, query string, args ...any) error {
	logger := t.Logger
	if !t.HideQueryLog {
		logger = logger.With(zap.String("query", query), zap.Reflect("args", args))
	}
	return t.perform(ctx, logger, purpose, func() error {
		_, err := t.DB.ExecContext(ctx, query, args...)
		return errors.Trace(err)
	})
}

// IsContextCanceledError returns whether the error is caused by context
// cancellation. This function should only be used when the code logic is
// affected by whether the error is canceling or not.
//
// This function returns `false` (not a context-canceled error) if `err == nil`.
func IsContextCanceledError(err error) bool {
	return log.IsContextCanceledError(err)
}

// UniqueTable returns an unique table name.
func UniqueTable(schema string, table string) string {
	var builder strings.Builder
	WriteMySQLIdentifier(&builder, schema)
	builder.WriteByte('.')
	WriteMySQLIdentifier(&builder, table)
	return builder.String()
}

func escapeIdentifiers(identifier []string) []any {
	escaped := make([]any, len(identifier))
	for i, id := range identifier {
		escaped[i] = EscapeIdentifier(id)
	}
	return escaped
}

// SprintfWithIdentifiers escapes the identifiers and sprintf them. The input
// identifiers must not be escaped.
func SprintfWithIdentifiers(format string, identifiers ...string) string {
	return fmt.Sprintf(format, escapeIdentifiers(identifiers)...)
}

// FprintfWithIdentifiers escapes the identifiers and fprintf them. The input
// identifiers must not be escaped.
func FprintfWithIdentifiers(w io.Writer, format string, identifiers ...string) (int, error) {
	return fmt.Fprintf(w, format, escapeIdentifiers(identifiers)...)
}

// EscapeIdentifier quote and escape an sql identifier
func EscapeIdentifier(identifier string) string {
	var builder strings.Builder
	WriteMySQLIdentifier(&builder, identifier)
	return builder.String()
}

// WriteMySQLIdentifier writes a MySQL identifier into the string builder.
// Writes a MySQL identifier into the string builder.
// The identifier is always escaped into the form "`foo`".
func WriteMySQLIdentifier(builder *strings.Builder, identifier string) {
	builder.Grow(len(identifier) + 2)
	builder.WriteByte('`')

	// use a C-style loop instead of range loop to avoid UTF-8 decoding
	for i := 0; i < len(identifier); i++ {
		b := identifier[i]
		if b == '`' {
			builder.WriteString("``")
		} else {
			builder.WriteByte(b)
		}
	}

	builder.WriteByte('`')
}

// InterpolateMySQLString interpolates a string into a MySQL string literal.
func InterpolateMySQLString(s string) string {
	var builder strings.Builder
	builder.Grow(len(s) + 2)
	builder.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == '\'' {
			builder.WriteString("''")
		} else {
			builder.WriteByte(b)
		}
	}
	builder.WriteByte('\'')
	return builder.String()
}

// TableExists return whether table with specified name exists in target db
func TableExists(ctx context.Context, db dbutil.QueryExecutor, schema, table string) (bool, error) {
	query := "SELECT 1 from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var exist string
	err := db.QueryRowContext(ctx, query, schema, table).Scan(&exist)
	switch err {
	case nil:
		return true, nil
	case sql.ErrNoRows:
		return false, nil
	default:
		return false, errors.Annotatef(err, "check table exists failed")
	}
}

// SchemaExists return whether schema with specified name exists.
func SchemaExists(ctx context.Context, db dbutil.QueryExecutor, schema string) (bool, error) {
	query := "SELECT 1 from INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?"
	var exist string
	err := db.QueryRowContext(ctx, query, schema).Scan(&exist)
	switch err {
	case nil:
		return true, nil
	case sql.ErrNoRows:
		return false, nil
	default:
		return false, errors.Annotatef(err, "check schema exists failed")
	}
}

// GetJSON fetches a page and parses it as JSON. The parsed result will be
// stored into the `v`. The variable `v` must be a pointer to a type that can be
// unmarshalled from JSON.
//
// Example:
//
//	client := &http.Client{}
//	var resp struct { IP string }
//	if err := util.GetJSON(client, "http://api.ipify.org/?format=json", &resp); err != nil {
//		return errors.Trace(err)
//	}
//	fmt.Println(resp.IP)
func GetJSON(ctx context.Context, client *http.Client, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return errors.Trace(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Errorf("get %s http status code != 200, message %s", url, string(body))
	}

	return errors.Trace(json.NewDecoder(resp.Body).Decode(v))
}

// KillMySelf sends sigint to current process, used in integration test only
//
// Only works on Unix. Signaling on Windows is not supported.
func KillMySelf() error {
	proc, err := os.FindProcess(os.Getpid())
	if err == nil {
		err = proc.Signal(syscall.SIGINT)
	}
	return errors.Trace(err)
}

// KvPair contains a key-value pair and other fields that can be used to ingest
// KV pairs into TiKV.
type KvPair struct {
	// Key is the key of the KV pair
	Key []byte
	// Val is the value of the KV pair
	Val []byte
	// RowID identifies a KvPair in case two KvPairs are equal in Key and Val. It has
	// two sources:
	//
	// When the KvPair is generated from ADD INDEX, the RowID is the encoded handle.
	//
	// Otherwise, the RowID is related to the row number in the source files, and
	// encode the number with `codec.EncodeComparableVarint`.
	RowID []byte
}

// EncodeIntRowID encodes an int64 row id.
func EncodeIntRowID(rowID int64) []byte {
	return codec.EncodeComparableVarint(nil, rowID)
}

// TableHasAutoRowID return whether table has auto generated row id
func TableHasAutoRowID(info *model.TableInfo) bool {
	return !info.PKIsHandle && !info.IsCommonHandle
}

// TableHasAutoID return whether table has auto generated id.
func TableHasAutoID(info *model.TableInfo) bool {
	return TableHasAutoRowID(info) || info.GetAutoIncrementColInfo() != nil || info.ContainsAutoRandomBits()
}

// GetAutoRandomColumn return the column with auto_random, return nil if the table doesn't have it.
// todo: better put in ddl package, but this will cause import cycle since ddl package import lightning
func GetAutoRandomColumn(tblInfo *model.TableInfo) *model.ColumnInfo {
	if !tblInfo.ContainsAutoRandomBits() {
		return nil
	}
	if tblInfo.PKIsHandle {
		return tblInfo.GetPkColInfo()
	} else if tblInfo.IsCommonHandle {
		pk := tables.FindPrimaryIndex(tblInfo)
		if pk == nil {
			return nil
		}
		offset := pk.Columns[0].Offset
		return tblInfo.Columns[offset]
	}
	return nil
}

// GetDropIndexInfos returns the index infos that need to be dropped and the remain indexes.
func GetDropIndexInfos(
	tblInfo *model.TableInfo,
) (remainIndexes []*model.IndexInfo, dropIndexes []*model.IndexInfo) {
	cols := tblInfo.Columns
loop:
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.State != model.StatePublic {
			remainIndexes = append(remainIndexes, idxInfo)
			continue
		}
		// Primary key is a cluster index.
		if idxInfo.Primary && tblInfo.HasClusteredIndex() {
			remainIndexes = append(remainIndexes, idxInfo)
			continue
		}
		// Skip index that contains auto-increment column.
		// Because auto column must be defined as a key.
		for _, idxCol := range idxInfo.Columns {
			flag := cols[idxCol.Offset].GetFlag()
			if tmysql.HasAutoIncrementFlag(flag) {
				remainIndexes = append(remainIndexes, idxInfo)
				continue loop
			}
		}
		dropIndexes = append(dropIndexes, idxInfo)
	}
	return remainIndexes, dropIndexes
}

// BuildDropIndexSQL builds the SQL statement to drop index.
func BuildDropIndexSQL(dbName, tableName string, idxInfo *model.IndexInfo) string {
	if idxInfo.Primary {
		return SprintfWithIdentifiers("ALTER TABLE %s.%s DROP PRIMARY KEY", dbName, tableName)
	}
	return SprintfWithIdentifiers("ALTER TABLE %s.%s DROP INDEX %s", dbName, tableName, idxInfo.Name.O)
}

// BuildAddIndexSQL builds the SQL statement to create missing indexes.
// It returns both a single SQL statement that creates all indexes at once,
// and a list of SQL statements that creates each index individually.
func BuildAddIndexSQL(
	tableName string,
	curTblInfo,
	desiredTblInfo *model.TableInfo,
) (singleSQL string, multiSQLs []string) {
	addIndexSpecs := make([]string, 0, len(desiredTblInfo.Indices))
loop:
	for _, desiredIdxInfo := range desiredTblInfo.Indices {
		for _, curIdxInfo := range curTblInfo.Indices {
			if curIdxInfo.Name.L == desiredIdxInfo.Name.L {
				continue loop
			}
		}

		var buf bytes.Buffer
		if desiredIdxInfo.Primary {
			buf.WriteString("ADD PRIMARY KEY ")
		} else if desiredIdxInfo.Unique {
			buf.WriteString("ADD UNIQUE KEY ")
		} else {
			buf.WriteString("ADD KEY ")
		}
		// "primary" is a special name for primary key, we should not use it as index name.
		if desiredIdxInfo.Name.L != "primary" {
			buf.WriteString(EscapeIdentifier(desiredIdxInfo.Name.O))
		}

		colStrs := make([]string, 0, len(desiredIdxInfo.Columns))
		for _, col := range desiredIdxInfo.Columns {
			var colStr string
			if desiredTblInfo.Columns[col.Offset].Hidden {
				colStr = fmt.Sprintf("(%s)", desiredTblInfo.Columns[col.Offset].GeneratedExprString)
			} else {
				colStr = EscapeIdentifier(col.Name.O)
				if col.Length != types.UnspecifiedLength {
					colStr = fmt.Sprintf("%s(%s)", colStr, strconv.Itoa(col.Length))
				}
			}
			colStrs = append(colStrs, colStr)
		}
		fmt.Fprintf(&buf, "(%s)", strings.Join(colStrs, ","))

		if desiredIdxInfo.Invisible {
			fmt.Fprint(&buf, " INVISIBLE")
		}
		if desiredIdxInfo.Comment != "" {
			fmt.Fprintf(&buf, ` COMMENT '%s'`, format.OutputFormat(desiredIdxInfo.Comment))
		}
		addIndexSpecs = append(addIndexSpecs, buf.String())
	}
	if len(addIndexSpecs) == 0 {
		return "", nil
	}

	singleSQL = fmt.Sprintf("ALTER TABLE %s %s", tableName, strings.Join(addIndexSpecs, ", "))
	for _, spec := range addIndexSpecs {
		multiSQLs = append(multiSQLs, fmt.Sprintf("ALTER TABLE %s %s", tableName, spec))
	}
	return singleSQL, multiSQLs
}

// IsDupKeyError checks if err is a duplicate index error.
func IsDupKeyError(err error) bool {
	if merr, ok := errors.Cause(err).(*mysql.MySQLError); ok {
		switch merr.Number {
		case errno.ErrDupKeyName, errno.ErrMultiplePriKey, errno.ErrDupUnique:
			return true
		}
	}
	return false
}

// GetBackoffWeightFromDB gets the backoff weight from database.
func GetBackoffWeightFromDB(ctx context.Context, db *sql.DB) (int, error) {
	val, err := getSessionVariable(ctx, db, variable.TiDBBackOffWeight)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(val)
}

// GetExplicitRequestSourceTypeFromDB gets the explicit request source type from database.
func GetExplicitRequestSourceTypeFromDB(ctx context.Context, db *sql.DB) (string, error) {
	return getSessionVariable(ctx, db, variable.TiDBExplicitRequestSourceType)
}

// copy from dbutil to avoid import cycle
func getSessionVariable(ctx context.Context, db *sql.DB, variable string) (value string, err error) {
	query := fmt.Sprintf("SHOW VARIABLES LIKE '%s'", variable)
	rows, err := db.QueryContext(ctx, query)

	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/

	for rows.Next() {
		if err = rows.Scan(&variable, &value); err != nil {
			return "", errors.Trace(err)
		}
	}

	if err := rows.Err(); err != nil {
		return "", errors.Trace(err)
	}

	return value, nil
}

// IsFunctionNotExistErr checks if err is a function not exist error.
func IsFunctionNotExistErr(err error, functionName string) bool {
	return err != nil &&
		(strings.Contains(err.Error(), "No database selected") ||
			strings.Contains(err.Error(), fmt.Sprintf("%s does not exist", functionName)))
}

// IsRaftKV2 checks whether the raft-kv2 is enabled
func IsRaftKV2(ctx context.Context, db *sql.DB) (bool, error) {
	var (
		getRaftKvVersionSQL       = "show config where type = 'tikv' and name = 'storage.engine'"
		raftKv2                   = "raft-kv2"
		tp, instance, name, value string
	)

	rows, err := db.QueryContext(ctx, getRaftKvVersionSQL)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&tp, &instance, &name, &value); err != nil {
			return false, errors.Trace(err)
		}
		if value == raftKv2 {
			return true, nil
		}
	}
	return false, rows.Err()
}
