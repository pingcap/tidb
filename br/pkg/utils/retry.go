// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"database/sql"
	stderrors "errors"
	"io"
	"net"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/errno"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var retryableServerError = []string{
	"server closed",
	"connection refused",
	"connection reset by peer",
	"channel closed",
	"error trying to connect",
	"connection closed before message completed",
	"body write aborted",
	"error during dispatch",
	"put object timeout",
}

// RetryableFunc presents a retryable operation.
type RetryableFunc func() error

// Backoffer implements a backoff policy for retrying operations.
type Backoffer interface {
	// NextBackoff returns a duration to wait before retrying again
	NextBackoff(err error) time.Duration
	// Attempt returns the remain attempt times
	Attempt() int
}

// WithRetry retries a given operation with a backoff policy.
//
// Returns nil if `retryableFunc` succeeded at least once. Otherwise, returns a
// multierr containing all errors encountered.
func WithRetry(
	ctx context.Context,
	retryableFunc RetryableFunc,
	backoffer Backoffer,
) error {
	var allErrors error
	for backoffer.Attempt() > 0 {
		err := retryableFunc()
		if err != nil {
			allErrors = multierr.Append(allErrors, err)
			select {
			case <-ctx.Done():
				return allErrors // nolint:wrapcheck
			case <-time.After(backoffer.NextBackoff(err)):
			}
		} else {
			return nil
		}
	}
	return allErrors // nolint:wrapcheck
}

// MessageIsRetryableStorageError checks whether the message returning from TiKV is retryable ExternalStorageError.
func MessageIsRetryableStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	// UNSAFE! TODO: Add a error type for retryable connection error.
	for _, errStr := range retryableServerError {
		if strings.Contains(msgLower, errStr) {
			return true
		}
	}
	return false
}

// sqlmock uses fmt.Errorf to produce expectation failures, which will cause
// unnecessary retry if not specially handled >:(
var stdFatalErrorsRegexp = regexp.MustCompile(
	`^call to (?s:.*) was not expected|arguments do not match:|could not match actual sql|mock non-retryable error`,
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
		case tmysql.ErrUnknown, tmysql.ErrLockDeadlock, tmysql.ErrWriteConflict, tmysql.ErrWriteConflictInTiDB,
			tmysql.ErrPDServerTimeout, tmysql.ErrTiKVServerTimeout, tmysql.ErrTiKVServerBusy, tmysql.ErrResolveLockTimeout,
			tmysql.ErrRegionUnavailable, tmysql.ErrInfoSchemaExpired, tmysql.ErrInfoSchemaChanged, tmysql.ErrTxnRetryable:
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
