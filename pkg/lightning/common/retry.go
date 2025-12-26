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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
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
	// Mock error during test
	"injected random error",
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
			// the caller might call IsRetryableError on nil, or the task is cancelled,
			// no need to log more info in this case.
			if singleError != nil && !goerrors.Is(singleError, context.Canceled) {
				// we want to log its type and other info for better diagnosing and
				// direct us to determine how to add to the retryable error list.
				logutil.BgLogger().Info("meet un-retryable error", zap.Error(singleError),
					zap.String("info", fmt.Sprintf("type: %T, value: %#v", singleError, singleError)))
			}
			return false
		}
	}
	return true
}

var retryableErrorIDs = map[errors.ErrorID]struct{}{
	errdef.ErrKVEpochNotMatch.ID():  {},
	errdef.ErrKVNotLeader.ID():      {},
	errdef.ErrNoLeader.ID():         {},
	errdef.ErrKVRegionNotFound.ID(): {},
	// common.ErrKVServerIsBusy is a little duplication with tmysql.ErrTiKVServerBusy
	// it's because the response of sst.ingest gives us a sst.IngestResponse which doesn't contain error code,
	// so we have to transform it into a defined code
	errdef.ErrKVServerIsBusy.ID():        {},
	errdef.ErrKVReadIndexNotReady.ID():   {},
	errdef.ErrKVIngestFailed.ID():        {},
	errdef.ErrKVRaftProposalDropped.ID(): {},
	// litBackendCtxMgr.Register may return the error.
	ErrCreatePDClient.ID(): {},
	// during checksum coprocessor, either through ADMIN CHECKSUM TABLE or by calling
	// distsql directly, we will transform error into driver error in handleCopResponse
	// using ToTiDBErr. we ever met ErrRegionUnavailable on free-tier import during
	// checksum, and meet: "[tikv:9001]PD server timeout: start timestamp may fall
	// behind safe point" when the TiDB doing checksum and network partitioned
	// with the cluster, others hasn't met yet
	drivererr.ErrRegionUnavailable.ID(): {},
	drivererr.ErrTiKVStaleCommand.ID():  {},
	drivererr.ErrTiKVServerTimeout.ID(): {},
	drivererr.ErrTiKVServerBusy.ID():    {},
	drivererr.ErrPDServerTimeout.ID():   {},
	drivererr.ErrUnknown.ID():           {},
}

// ErrWriteTooSlow is used to get rid of the gRPC blocking issue.
// there are some strange blocking issues of gRPC like
// https://github.com/pingcap/tidb/issues/48352
// https://github.com/pingcap/tidb/issues/46321 and I don't know why 😭
var ErrWriteTooSlow = errors.New("write too slow, maybe gRPC is blocked forever")

// see https://github.com/golang/go/blob/b3251514531123d7fd007682389bce7428d159a0/src/net/http/transport.go#L1541-L1544
var nonRetryableURLInnerErrorMsg = []string{
	"net/http: request canceled",
	"net/http: request canceled while waiting for connection",
}

func isRetryableURLInnerError(err error) bool {
	if err == nil || err == io.EOF {
		// we retry when urlErr.Err is nil, the EOF error is a workaround
		// for https://github.com/golang/go/issues/53472
		return true
	}
	errMsg := err.Error()
	for _, msg := range nonRetryableURLInnerErrorMsg {
		if strings.Contains(errMsg, msg) {
			return false
		}
	}
	// such as:
	// Put "http://localhost:19000/write_sst?......": write tcp ......: use of closed network connection
	return true
}

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
		if nerr.Timeout() || nerr.Temporary() {
			return true
		}
		// the error might be nested, such as *url.Error -> *net.OpError -> *os.SyscallError
		var syscallErr *os.SyscallError
		if goerrors.As(nerr, &syscallErr) {
			return syscallErr.Err == syscall.ECONNREFUSED || syscallErr.Err == syscall.ECONNRESET ||
				syscallErr.Err == syscall.EPIPE
		}
		var urlErr *url.Error
		if goerrors.As(nerr, &urlErr) {
			return isRetryableURLInnerError(urlErr.Err)
		}
		return false
	case *mysql.MySQLError:
		switch nerr.Number {
		// ErrLockDeadlock can retry to commit while meet deadlock
		case tmysql.ErrUnknown, tmysql.ErrLockDeadlock, tmysql.ErrLockWaitTimeout, tmysql.ErrWriteConflict, tmysql.ErrWriteConflictInTiDB,
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
	case *errdef.HTTPStatusError:
		// all are retryable except 400 and 404
		if nerr.StatusCode == http.StatusBadRequest || nerr.StatusCode == http.StatusNotFound {
			return false
		}
		return true
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
			if strings.Contains(errMsg, "Keys must be added in strict ascending order") {
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
