<<<<<<< HEAD
package utils

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryableError(t *testing.T) {
	require.False(t, IsRetryableError(context.Canceled))
	require.False(t, IsRetryableError(context.DeadlineExceeded))
	require.False(t, IsRetryableError(io.EOF))
	require.False(t, IsRetryableError(&net.AddrError{}))
	require.False(t, IsRetryableError(&net.DNSError{}))
	require.True(t, IsRetryableError(&net.DNSError{IsTimeout: true}))

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
	require.True(t, IsRetryableError(status.Error(codes.Unknown, "")))
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
	require.True(t, IsRetryableError(errors.New("call to database Close was not expected")))

	// multierr
	require.False(t, IsRetryableError(multierr.Combine(context.Canceled, context.Canceled)))
	require.True(t, IsRetryableError(multierr.Combine(&net.DNSError{IsTimeout: true}, &net.DNSError{IsTimeout: true})))
	require.False(t, IsRetryableError(multierr.Combine(context.Canceled, &net.DNSError{IsTimeout: true})))
=======
// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRetryAdapter(t *testing.T) {
	req := require.New(t)

	begin := time.Now()
	bo := utils.AdaptTiKVBackoffer(context.Background(), 200, errors.New("everything is alright"))
	// This should sleep for 100ms.
	bo.Inner().Backoff(tikv.BoTiKVRPC(), errors.New("TiKV is in a deep dream"))
	sleeped := bo.TotalSleepInMS()
	req.GreaterOrEqual(sleeped, 50)
	req.LessOrEqual(sleeped, 150)
	requestedBackOff := [...]int{10, 20, 5, 0, 42, 48}
	wg := new(sync.WaitGroup)
	wg.Add(len(requestedBackOff))
	for _, bms := range requestedBackOff {
		bms := bms
		go func() {
			bo.RequestBackOff(bms)
			wg.Done()
		}()
	}
	wg.Wait()
	req.Equal(bo.NextSleepInMS(), 48)
	req.NoError(bo.BackOff())
	req.Equal(bo.TotalSleepInMS(), sleeped+48)

	bo.RequestBackOff(150)
	req.NoError(bo.BackOff())

	bo.RequestBackOff(150)
	req.ErrorContains(bo.BackOff(), "everything is alright", "total = %d / %d", bo.TotalSleepInMS(), bo.MaxSleepInMS())

	req.Greater(time.Since(begin), 200*time.Millisecond)
>>>>>>> 1a94e5db786 (backup: fix retry of fine-grained backup (#43252))
}
