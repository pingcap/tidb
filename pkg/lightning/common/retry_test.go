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
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryableError(t *testing.T) {
	// url errors
	require.True(t, IsRetryableError(&url.Error{}))
	require.True(t, IsRetryableError(&url.Error{Err: io.EOF}))
	require.False(t, IsRetryableError(&url.Error{Err: fmt.Errorf("net/http: request canceled")}))
	require.False(t, IsRetryableError(&url.Error{Err: fmt.Errorf("net/http: request canceled while waiting for connection")}))
	require.True(t, IsRetryableError(&url.Error{Err: fmt.Errorf("dummy error")}))
	require.True(t, IsRetryableError(&url.Error{Err: fmt.Errorf("use of closed network connection")}))

	require.False(t, IsRetryableError(context.Canceled))
	require.False(t, IsRetryableError(context.DeadlineExceeded))
	require.True(t, IsRetryableError(ErrWriteTooSlow))
	require.False(t, IsRetryableError(io.EOF))
	require.False(t, IsRetryableError(&net.AddrError{}))
	require.False(t, IsRetryableError(&net.DNSError{}))
	require.True(t, IsRetryableError(&net.DNSError{IsTimeout: true}))
	require.True(t, IsRetryableError(&net.DNSError{IsTemporary: true}))

	// inner syscall errors
	require.True(t, IsRetryableError(&net.DNSError{UnwrapErr: &os.SyscallError{Err: syscall.ECONNREFUSED}}))
	require.True(t, IsRetryableError(&net.DNSError{UnwrapErr: &os.SyscallError{Err: syscall.EPIPE}}))
	require.True(t, IsRetryableError(&net.DNSError{UnwrapErr: &os.SyscallError{Err: syscall.ECONNRESET}}))
	require.False(t, IsRetryableError(&net.DNSError{UnwrapErr: &os.SyscallError{Err: syscall.ENETDOWN}}))

	// request error
	require.False(t, IsRetryableError(errors.Trace(&errdef.HTTPStatusError{StatusCode: http.StatusBadRequest})))
	require.False(t, IsRetryableError(errors.Trace(&errdef.HTTPStatusError{StatusCode: http.StatusNotFound})))
	require.True(t, IsRetryableError(errors.Trace(&errdef.HTTPStatusError{StatusCode: http.StatusInternalServerError})))

	// kv errors
	require.True(t, IsRetryableError(errors.Annotatef(errdef.ErrNoLeader.GenWithStackByArgs(123), "when write to tikv, expected leader id %d", 111)))
	require.True(t, IsRetryableError(errdef.ErrKVNotLeader))
	require.True(t, IsRetryableError(errdef.ErrKVEpochNotMatch))
	require.True(t, IsRetryableError(errdef.ErrKVServerIsBusy))
	require.True(t, IsRetryableError(errdef.ErrKVRegionNotFound))
	require.True(t, IsRetryableError(errdef.ErrKVReadIndexNotReady))
	require.True(t, IsRetryableError(errdef.ErrKVIngestFailed))
	require.True(t, IsRetryableError(errdef.ErrKVRaftProposalDropped))
	require.True(t, IsRetryableError(errdef.ErrKVNotLeader.GenWithStack("test")))
	require.True(t, IsRetryableError(errdef.ErrKVEpochNotMatch.GenWithStack("test")))
	require.True(t, IsRetryableError(errdef.ErrKVServerIsBusy.GenWithStack("test")))
	require.True(t, IsRetryableError(errdef.ErrKVRegionNotFound.GenWithStack("test")))
	require.True(t, IsRetryableError(errdef.ErrKVReadIndexNotReady.GenWithStack("test")))
	require.True(t, IsRetryableError(errdef.ErrKVIngestFailed.GenWithStack("test")))
	require.True(t, IsRetryableError(errdef.ErrKVRaftProposalDropped.GenWithStack("test")))
	require.False(t, IsRetryableError(errdef.ErrKVDiskFull.GenWithStack("test")))

	for _, err := range []error{
		// tidb error
		drivererr.ErrRegionUnavailable,
		drivererr.ErrTiKVStaleCommand,
		drivererr.ErrTiKVServerTimeout,
		drivererr.ErrTiKVServerBusy,
		drivererr.ErrPDServerTimeout,
		drivererr.ErrUnknown,
	} {
		require.True(t, IsRetryableError(errors.Annotate(err, "failed")))
	}

	// net: connection refused
	_, err := net.Dial("tcp", "localhost:65533")
	require.Error(t, err)
	require.True(t, IsRetryableError(err))
	// wrap net.OpErr inside url.Error
	urlErr := &url.Error{Op: "post", Err: err}
	require.True(t, IsRetryableError(urlErr))

	// MySQL Errors
	require.False(t, IsRetryableError(&mysql.MySQLError{}))
	for _, errNumber := range []uint16{
		tmysql.ErrUnknown,
		tmysql.ErrLockDeadlock,
		tmysql.ErrPDServerTimeout,
		tmysql.ErrTiKVServerTimeout,
		tmysql.ErrTiKVServerBusy,
		tmysql.ErrResolveLockTimeout,
		tmysql.ErrRegionUnavailable,
		tmysql.ErrWriteConflictInTiDB,
		tmysql.ErrWriteConflict,
		tmysql.ErrInfoSchemaExpired,
		tmysql.ErrInfoSchemaChanged,
		tmysql.ErrTxnRetryable,
	} {
		require.True(t, IsRetryableError(&mysql.MySQLError{Number: errNumber}))
	}

	// gRPC Errors
	require.False(t, IsRetryableError(status.Error(codes.Canceled, "")))
	require.True(t, IsRetryableError(status.Error(codes.Unknown, "region 1234 is not fully replicated")))
	require.True(t, IsRetryableError(status.Error(codes.Unknown, "No such file or directory: while stat a file "+
		"for size: /...../63992d9c-fbc8-4708-b963-32495b299027_32279707_325_5280_write.sst: No such file or directory")))
	for _, code := range []codes.Code{
		codes.DeadlineExceeded,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.OutOfRange,
		codes.Unavailable,
		codes.DataLoss,
	} {
		require.True(t, IsRetryableError(status.Error(code, "")))
	}

	// sqlmock errors
	require.False(t, IsRetryableError(fmt.Errorf("call to database Close was not expected")))
	require.False(t, IsRetryableError(errors.New("call to database Close was not expected")))

	// stderr
	require.True(t, IsRetryableError(mysql.ErrInvalidConn))
	require.True(t, IsRetryableError(driver.ErrBadConn))
	require.False(t, IsRetryableError(fmt.Errorf("error")))

	// multierr
	require.False(t, IsRetryableError(multierr.Combine(context.Canceled, context.Canceled)))
	require.True(t, IsRetryableError(multierr.Combine(&net.DNSError{IsTimeout: true}, &net.DNSError{IsTimeout: true})))
	require.False(t, IsRetryableError(multierr.Combine(context.Canceled, &net.DNSError{IsTimeout: true})))

	require.True(t, IsRetryableError(errors.New("other error: Coprocessor task terminated due to exceeding the deadline")))

	// error from limiter
	l := rate.NewLimiter(rate.Limit(1), 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// context has 1 second timeout, can't wait for 10 seconds
	err = l.WaitN(ctx, 10)
	require.Error(t, err)
	require.True(t, IsRetryableError(err))
}
