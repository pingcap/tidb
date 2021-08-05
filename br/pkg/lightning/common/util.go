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

package common

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tmysql "github.com/pingcap/tidb/errno"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&sql_mode='%s'&maxAllowedPacket=%d&tls=%s",
		param.User, param.Password, param.Host, param.Port,
		param.SQLMode, param.MaxAllowedPacket, param.TLS)

	for k, v := range param.Vars {
		dsn += fmt.Sprintf("&%s=%s", k, url.QueryEscape(v))
	}

	return dsn
}

func (param *MySQLConnectParam) Connect() (*sql.DB, error) {
	db, err := sql.Open("mysql", param.ToDSN())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, errors.Trace(db.Ping())
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

type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type DBExecutor interface {
	QueryExecutor
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// SQLWithRetry constructs a retryable transaction.
type SQLWithRetry struct {
	// either *sql.DB or *sql.Conn
	DB           DBExecutor
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

// sqlmock uses fmt.Errorf to produce expectation failures, which will cause
// unnecessary retry if not specially handled >:(
var stdFatalErrorsRegexp = regexp.MustCompile(
	`^call to (?s:.*) was not expected|arguments do not match:|could not match actual sql`,
)
var stdErrorType = reflect.TypeOf(stderrors.New(""))

// IsRetryableError returns whether the error is transient (e.g. network
// connection dropped) or irrecoverable (e.g. user pressing Ctrl+C). This
// function returns `false` (irrecoverable) if `err == nil`.
//
// If the error is a multierr, returns true only if all suberrors are retryable.
func IsRetryableError(err error) bool {
	for _, singleError := range errors.Errors(err) {
		if !isSingleRetryableError(singleError) {
			return false
		}
	}
	return true
}

func isSingleRetryableError(err error) bool {
	err = errors.Cause(err)

	switch err {
	case nil, context.Canceled, context.DeadlineExceeded, io.EOF, sql.ErrNoRows:
		return false
	}

	switch nerr := err.(type) {
	case net.Error:
		return nerr.Timeout()
	case *mysql.MySQLError:
		switch nerr.Number {
		// ErrLockDeadlock can retry to commit while meet deadlock
		case tmysql.ErrUnknown, tmysql.ErrLockDeadlock, tmysql.ErrWriteConflictInTiDB, tmysql.ErrPDServerTimeout, tmysql.ErrTiKVServerTimeout, tmysql.ErrTiKVServerBusy, tmysql.ErrResolveLockTimeout, tmysql.ErrRegionUnavailable:
			return true
		default:
			return false
		}
	default:
		switch status.Code(err) {
		case codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.ResourceExhausted, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss:
			return true
		case codes.Unknown:
			if reflect.TypeOf(err) == stdErrorType {
				return !stdFatalErrorsRegexp.MatchString(err.Error())
			}
			return true
		default:
			return false
		}
	}
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
	// Offset is the row's offset in file.
	Offset int64
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
