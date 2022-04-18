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
}
