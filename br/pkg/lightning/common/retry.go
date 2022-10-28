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
	"strings"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/errno"
	drivererr "github.com/pingcap/tidb/store/driver/error"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// some component doesn't have an accurate named error or transform a named error into string,
// so we need to check by error message,
// such as distsql.Checksum which transforms tikv other-error into its own error
var retryableErrorMsgList = []string{
	"is not fully replicated",
	// for cluster >= 4.x, lightning calls distsql.Checksum to do checksum
	// this error happens on when distsql.Checksum calls TiKV
	// see https://github.com/pingcap/tidb/blob/2c3d4f1ae418881a95686e8b93d4237f2e76eec6/store/copr/coprocessor.go#L941
	"coprocessor task terminated due to exceeding the deadline",
}

func isRetryableFromErrorMessage(err error) bool {
	msg := err.Error()
	msgLower := strings.ToLower(msg)
	for _, errStr := range retryableErrorMsgList {
		if strings.Contains(msgLower, errStr) {
			return true
		}
	}
	return false
}

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

var retryableErrorIDs = map[errors.ErrorID]struct{}{
	ErrKVEpochNotMatch.ID():  {},
	ErrKVNotLeader.ID():      {},
	ErrKVRegionNotFound.ID(): {},
	// common.ErrKVServerIsBusy is a little duplication with tmysql.ErrTiKVServerBusy
	// it's because the response of sst.ingest gives us a sst.IngestResponse which doesn't contain error code,
	// so we have to transform it into a defined code
	ErrKVServerIsBusy.ID():        {},
	ErrKVReadIndexNotReady.ID():   {},
	ErrKVIngestFailed.ID():        {},
	ErrKVRaftProposalDropped.ID(): {},
	// during checksum coprocessor will transform error into driver error in handleCopResponse using ToTiDBErr
	// met ErrRegionUnavailable on free-tier import during checksum, others hasn't met yet
	drivererr.ErrRegionUnavailable.ID(): {},
	drivererr.ErrTiKVStaleCommand.ID():  {},
	drivererr.ErrTiKVServerTimeout.ID(): {},
	drivererr.ErrTiKVServerBusy.ID():    {},
	drivererr.ErrUnknown.ID():           {},
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
		if _, ok := retryableErrorIDs[nerr.ID()]; ok {
			return true
		}
		return false
	default:
		switch status.Code(err) {
		case codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.ResourceExhausted, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss:
			return true
		default:
			return isRetryableFromErrorMessage(err)
		}
	}
}
