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
	"net/url"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/pkg/errno"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryableError(t *testing.T) {
	require.False(t, IsRetryableError(context.Canceled))
	require.False(t, IsRetryableError(context.DeadlineExceeded))
	require.True(t, IsRetryableError(ErrWriteTooSlow))
	require.False(t, IsRetryableError(io.EOF))
	require.False(t, IsRetryableError(&net.AddrError{}))
	require.False(t, IsRetryableError(&net.DNSError{}))
	require.True(t, IsRetryableError(&net.DNSError{IsTimeout: true}))

	// kv errors
	require.True(t, IsRetryableError(ErrKVNotLeader))
	require.True(t, IsRetryableError(ErrKVEpochNotMatch))
	require.True(t, IsRetryableError(ErrKVServerIsBusy))
	require.True(t, IsRetryableError(ErrKVRegionNotFound))
	require.True(t, IsRetryableError(ErrKVReadIndexNotReady))
	require.True(t, IsRetryableError(ErrKVIngestFailed))
	require.True(t, IsRetryableError(ErrKVRaftProposalDropped))
	require.True(t, IsRetryableError(ErrKVNotLeader.GenWithStack("test")))
	require.True(t, IsRetryableError(ErrKVEpochNotMatch.GenWithStack("test")))
	require.True(t, IsRetryableError(ErrKVServerIsBusy.GenWithStack("test")))
	require.True(t, IsRetryableError(ErrKVRegionNotFound.GenWithStack("test")))
	require.True(t, IsRetryableError(ErrKVReadIndexNotReady.GenWithStack("test")))
	require.True(t, IsRetryableError(ErrKVIngestFailed.GenWithStack("test")))
	require.True(t, IsRetryableError(ErrKVRaftProposalDropped.GenWithStack("test")))

	// tidb error
	require.True(t, IsRetryableError(drivererr.ErrRegionUnavailable))
	require.True(t, IsRetryableError(drivererr.ErrTiKVStaleCommand))
	require.True(t, IsRetryableError(drivererr.ErrTiKVServerTimeout))
	require.True(t, IsRetryableError(drivererr.ErrTiKVServerBusy))
	require.True(t, IsRetryableError(drivererr.ErrUnknown))

	// net: connection refused
	_, err := net.Dial("tcp", "localhost:65533")
	require.Error(t, err)
	require.True(t, IsRetryableError(err))
	// wrap net.OpErr inside url.Error
	urlErr := &url.Error{Op: "post", Err: err}
	require.True(t, IsRetryableError(urlErr))

	// MySQL Errors
	require.False(t, IsRetryableError(&mysql.MySQLError{}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrUnknown}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrLockDeadlock}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrPDServerTimeout}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrTiKVServerTimeout}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrTiKVServerBusy}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrResolveLockTimeout}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrRegionUnavailable}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrWriteConflictInTiDB}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrWriteConflict}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrInfoSchemaExpired}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrInfoSchemaChanged}))
	require.True(t, IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrTxnRetryable}))

	// gRPC Errors
	require.False(t, IsRetryableError(status.Error(codes.Canceled, "")))
	require.True(t, IsRetryableError(status.Error(codes.Unknown, "region 1234 is not fully replicated")))
	require.True(t, IsRetryableError(status.Error(codes.Unknown, "No such file or directory: while stat a file "+
		"for size: /...../63992d9c-fbc8-4708-b963-32495b299027_32279707_325_5280_write.sst: No such file or directory")))
	require.True(t, IsRetryableError(status.Error(codes.DeadlineExceeded, "")))
	require.True(t, IsRetryableError(status.Error(codes.NotFound, "")))
	require.True(t, IsRetryableError(status.Error(codes.AlreadyExists, "")))
	require.True(t, IsRetryableError(status.Error(codes.PermissionDenied, "")))
	require.True(t, IsRetryableError(status.Error(codes.ResourceExhausted, "")))
	require.True(t, IsRetryableError(status.Error(codes.Aborted, "")))
	require.True(t, IsRetryableError(status.Error(codes.OutOfRange, "")))
	require.True(t, IsRetryableError(status.Error(codes.Unavailable, "")))
	require.True(t, IsRetryableError(status.Error(codes.DataLoss, "")))

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
