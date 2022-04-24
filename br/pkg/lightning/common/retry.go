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
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	tmysql "github.com/pingcap/tidb/errno"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	case mysql.ErrInvalidConn, driver.ErrBadConn:
		return true
	}

	switch nerr := err.(type) {
	case net.Error:
		if nerr.Timeout() {
			return true
		}
		switch cause := nerr.(type) {
		case *net.OpError:
			syscallErr, ok := cause.Unwrap().(*os.SyscallError)
			if ok {
				return syscallErr.Err == syscall.ECONNREFUSED || syscallErr.Err == syscall.ECONNRESET
			}
		}
		return false
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
	case *errors.Error:
		switch {
		case berrors.Is(nerr, ErrKVEpochNotMatch), berrors.Is(nerr, ErrKVNotLeader),
			berrors.Is(nerr, ErrKVRegionNotFound), berrors.Is(nerr, ErrKVServerIsBusy):
			// common.ErrKVServerIsBusy is a little duplication with tmysql.ErrTiKVServerBusy
			// it's because the response of sst.ingest gives us a sst.IngestResponse which doesn't contain error code,
			// so we have to transform it into a defined code
			return true
		}
		return false
	default:
		switch status.Code(err) {
		case codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.ResourceExhausted, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss:
			return true
		case codes.Unknown:
			return false
		default:
			return false
		}
	}
}
