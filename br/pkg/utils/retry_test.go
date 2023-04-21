<<<<<<< HEAD
package utils

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/errno"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type utilSuite struct{}

var _ = Suite(&utilSuite{})

func (s *utilSuite) TestIsRetryableError(c *C) {
	c.Assert(IsRetryableError(context.Canceled), IsFalse)
	c.Assert(IsRetryableError(context.DeadlineExceeded), IsFalse)
	c.Assert(IsRetryableError(io.EOF), IsFalse)
	c.Assert(IsRetryableError(&net.AddrError{}), IsFalse)
	c.Assert(IsRetryableError(&net.DNSError{}), IsFalse)
	c.Assert(IsRetryableError(&net.DNSError{IsTimeout: true}), IsTrue)

	// MySQL Errors
	c.Assert(IsRetryableError(&mysql.MySQLError{}), IsFalse)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrUnknown}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrLockDeadlock}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrPDServerTimeout}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrTiKVServerTimeout}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrTiKVServerBusy}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrResolveLockTimeout}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrRegionUnavailable}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrWriteConflictInTiDB}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrWriteConflict}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrInfoSchemaExpired}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrInfoSchemaChanged}), IsTrue)
	c.Assert(IsRetryableError(&mysql.MySQLError{Number: tmysql.ErrTxnRetryable}), IsTrue)

	// gRPC Errors
	c.Assert(IsRetryableError(status.Error(codes.Canceled, "")), IsFalse)
	c.Assert(IsRetryableError(status.Error(codes.Unknown, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.DeadlineExceeded, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.NotFound, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.AlreadyExists, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.PermissionDenied, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.ResourceExhausted, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.Aborted, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.OutOfRange, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.Unavailable, "")), IsTrue)
	c.Assert(IsRetryableError(status.Error(codes.DataLoss, "")), IsTrue)

	// sqlmock errors
	c.Assert(IsRetryableError(fmt.Errorf("call to database Close was not expected")), IsFalse)
	c.Assert(IsRetryableError(errors.New("call to database Close was not expected")), IsTrue)

	// multierr
	c.Assert(IsRetryableError(multierr.Combine(context.Canceled, context.Canceled)), IsFalse)
	c.Assert(IsRetryableError(multierr.Combine(&net.DNSError{IsTimeout: true}, &net.DNSError{IsTimeout: true})), IsTrue)
	c.Assert(IsRetryableError(multierr.Combine(context.Canceled, &net.DNSError{IsTimeout: true})), IsFalse)
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
