// Copyright 2023 PingCAP, Inc.
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
	"database/sql/driver"
	goerrors "errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/pkg/errno"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// some component doesn't have an accurate named error or transform a named error into string,
// so we need to check by error message,
// such as distsql.Checksum which transforms tikv other-error into its own error
var retryableErrorMsgList = []string{
	// for cluster >= 4.x, lightning calls distsql.Checksum to do checksum
	// this error happens on when distsql.Checksum calls TiKV
	// see https://github.com/pingcap/tidb/blob/2c3d4f1ae418881a95686e8b93d4237f2e76eec6/store/copr/coprocessor.go#L941
	"coprocessor task terminated due to exceeding the deadline",
	// fix https://github.com/pingcap/tidb/issues/51383
	"rate: wait",
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
	ErrNoLeader.ID():         {},
	ErrKVRegionNotFound.ID(): {},
	// common.ErrKVServerIsBusy is a little duplication with tmysql.ErrTiKVServerBusy
	// it's because the response of sst.ingest gives us a sst.IngestResponse which doesn't contain error code,
	// so we have to transform it into a defined code
	ErrKVServerIsBusy.ID():        {},
	ErrKVReadIndexNotReady.ID():   {},
	ErrKVIngestFailed.ID():        {},
	ErrKVRaftProposalDropped.ID(): {},
	// litBackendCtxMgr.Register may return the error.
	ErrCreatePDClient.ID(): {},
	// during checksum coprocessor will transform error into driver error in handleCopResponse using ToTiDBErr
	// met ErrRegionUnavailable on free-tier import during checksum, others hasn't met yet
	drivererr.ErrRegionUnavailable.ID(): {},
	drivererr.ErrTiKVStaleCommand.ID():  {},
	drivererr.ErrTiKVServerTimeout.ID(): {},
	drivererr.ErrTiKVServerBusy.ID():    {},
	drivererr.ErrUnknown.ID():           {},
}

// ErrWriteTooSlow is used to get rid of the gRPC blocking issue.
// there are some strange blocking issues of gRPC like
// https://github.com/pingcap/tidb/issues/48352
// https://github.com/pingcap/tidb/issues/46321 and I don't know why 😭
var ErrWriteTooSlow = errors.New("write too slow, maybe gRPC is blocked forever")

func isSingleRetryableError(err error) bool {
	err = errors.Cause(err)

	switch err {
	case nil, context.Canceled, context.DeadlineExceeded, io.EOF, sql.ErrNoRows:
		return false
	case mysql.ErrInvalidConn, driver.ErrBadConn, ErrWriteTooSlow:
		return true
	}

	switch nerr := err.(type) {
	case net.Error:
		if nerr.Timeout() {
			return true
		}
		// the error might be nested, such as *url.Error -> *net.OpError -> *os.SyscallError
		var syscallErr *os.SyscallError
		if goerrors.As(nerr, &syscallErr) {
			return syscallErr.Err == syscall.ECONNREFUSED || syscallErr.Err == syscall.ECONNRESET
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
		rpcStatus, ok := status.FromError(err)
		if !ok {
			// non RPC error
			return isRetryableFromErrorMessage(err)
		}
		switch rpcStatus.Code() {
		case codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied,
			codes.ResourceExhausted, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss:
			return true
		case codes.Unknown:
			errMsg := rpcStatus.Message()
			if strings.Contains(errMsg, "DiskSpaceNotEnough") {
				return false
			}
			// cases we have met during import:
			// 1. in scatter region: rpc error: code = Unknown desc = region 31946583 is not fully replicated
			// 2. in write TiKV: rpc error: code = Unknown desc = EngineTraits(Engine(Status { code: IoError, sub_code:
			//    None, sev: NoError, state: \"IO error: No such file or directory: while stat a file for size:
			//    /...../63992d9c-fbc8-4708-b963-32495b299027_32279707_325_5280_write.sst: No such file or directory\"
			// 3. in write TiKV: rpc error: code = Unknown desc = Engine("request region 26 is staler than local region,
			//    local epoch conf_ver: 5 version: 65, request epoch conf_ver: 5 version: 64, please rescan region later")
			return true
		default:
			return false
		}
	}
}
