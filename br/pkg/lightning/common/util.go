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
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/utils"
	tmysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

const (
	retryTimeout = 3 * time.Second

	defaultMaxRetry = 3
)

// MySQLConnectParam records the parameters needed to connect to a MySQL database.
type MySQLConnectParam struct {
	Host             string
	Port             int
	User             string
	Password         string
	SQLMode          string
	MaxAllowedPacket uint64
	TLS              string
	Vars             map[string]string
}

func (param *MySQLConnectParam) ToDSN() string {
	hostPort := net.JoinHostPort(param.Host, strconv.Itoa(param.Port))
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&sql_mode='%s'&maxAllowedPacket=%d&tls=%s",
		param.User, param.Password, hostPort,
		param.SQLMode, param.MaxAllowedPacket, param.TLS)

	for k, v := range param.Vars {
		dsn += fmt.Sprintf("&%s='%s'", k, url.QueryEscape(v))
	}

	return dsn
}

func tryConnectMySQL(dsn string) (*sql.DB, error) {
	driverName := "mysql"
	failpoint.Inject("MockMySQLDriver", func(val failpoint.Value) {
		driverName = val.(string)
	})
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, errors.Trace(err)
	}
	return db, nil
}

// ConnectMySQL connects MySQL with the dsn. If access is denied and the password is a valid base64 encoding,
// we will try to connect MySQL with the base64 decoding of the password.
func ConnectMySQL(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Try plain password first.
	db, firstErr := tryConnectMySQL(dsn)
	if firstErr == nil {
		return db, nil
	}
	// If access is denied and password is encoded by base64, try the decoded string as well.
	if mysqlErr, ok := errors.Cause(firstErr).(*mysql.MySQLError); ok && mysqlErr.Number == tmysql.ErrAccessDenied {
		// If password is encoded by base64, try the decoded string as well.
		if password, decodeErr := base64.StdEncoding.DecodeString(cfg.Passwd); decodeErr == nil && string(password) != cfg.Passwd {
			cfg.Passwd = string(password)
			db, err = tryConnectMySQL(cfg.FormatDSN())
			if err == nil {
				return db, nil
			}
		}
	}
	// If we can't connect successfully, return the first error.
	return nil, errors.Trace(firstErr)
}

func (param *MySQLConnectParam) Connect() (*sql.DB, error) {
	db, err := ConnectMySQL(param.ToDSN())
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
	DB           utils.DBExecutor
	Logger       log.Logger
	HideQueryLog bool
}

func (t SQLWithRetry) perform(_ context.Context, parentLogger log.Logger, purpose string, action func() error) error {
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

func (t SQLWithRetry) QueryRow(ctx context.Context, purpose string, query string, dest ...interface{}) error {
	logger := t.Logger
	if !t.HideQueryLog {
		logger = logger.With(zap.String("query", query))
	}
	return t.perform(ctx, logger, purpose, func() error {
		return t.DB.QueryRowContext(ctx, query).Scan(dest...)
	})
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
func (t SQLWithRetry) Exec(ctx context.Context, purpose string, query string, args ...interface{}) error {
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

// EscapeIdentifier quote and escape an sql identifier
func EscapeIdentifier(identifier string) string {
	var builder strings.Builder
	WriteMySQLIdentifier(&builder, identifier)
	return builder.String()
}

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
func TableExists(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	query := "SELECT 1 from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var exist string
	err := db.QueryRowContext(ctx, query, schema, table).Scan(&exist)
	switch {
	case err == nil:
		return true, nil
	case err == sql.ErrNoRows:
		return false, nil
	default:
		return false, errors.Annotatef(err, "check table exists failed")
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
func GetJSON(ctx context.Context, client *http.Client, url string, v interface{}) error {
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

// KvPair is a pair of key and value.
type KvPair struct {
	// Key is the key of the KV pair
	Key []byte
	// Val is the value of the KV pair
	Val []byte
	// RowID is the row id of the KV pair.
	RowID int64
}

// TableHasAutoRowID return whether table has auto generated row id
func TableHasAutoRowID(info *model.TableInfo) bool {
	return !info.PKIsHandle && !info.IsCommonHandle
}

// StringSliceEqual checks if two string slices are equal.
func StringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
