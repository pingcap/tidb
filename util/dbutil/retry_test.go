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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbutil

import (
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/require"
)

func TestIsRetryableError(t *testing.T) {
	cases := []struct {
		err       error
		retryable bool
	}{
		{
			err:       nil,
			retryable: false,
		},
		{
			err:       errors.New("custom error"),
			retryable: false,
		},
		{
			err:       newMysqlErr(errno.ErrNoDB, "No database selected"),
			retryable: false,
		},
		{
			err:       driver.ErrBadConn,
			retryable: false,
		},
		{
			err:       mysql.ErrInvalidConn,
			retryable: false,
		},
		// retryable
		{
			err:       newMysqlErr(errno.ErrLockDeadlock, "Deadlock found when trying to get lock; try restarting transaction"),
			retryable: true,
		},
		{
			err:       newMysqlErr(errno.ErrPDServerTimeout, "pd server timeout"),
			retryable: true,
		},
		{
			err:       newMysqlErr(errno.ErrTiKVServerBusy, "tikv server busy"),
			retryable: true,
		},
		{
			err:       newMysqlErr(errno.ErrResolveLockTimeout, "resolve lock timeout"),
			retryable: true,
		},
		{
			err:       newMysqlErr(errno.ErrWriteConflictInTiDB, "Write conflict, txnStartTS 412719757964869950 is stale"),
			retryable: true,
		},
		{
			err:       newMysqlErr(errno.ErrWriteConflict, "Write conflict, txnStartTS=412719757964869700, conflictStartTS=412719757964869700, conflictCommitTS=412719757964869950, key=488636541"),
			retryable: true,
		},
		// only retryable in some special cases, then we mark it as un-retryable
		{
			err:       newMysqlErr(errno.ErrTiKVServerTimeout, "tikv server timeout"),
			retryable: false,
		},
		{
			err:       newMysqlErr(errno.ErrTableLocked, "Table 'tbl' was locked in aaa by bbb"),
			retryable: false,
		},
		// un-retryable
		{
			err:       newMysqlErr(errno.ErrQueryInterrupted, "Query execution was interrupted"),
			retryable: false,
		},
		// unknown
		{
			err:       newMysqlErr(errno.ErrRegionUnavailable, "region unavailable"),
			retryable: false,
		},
		// 1105, un-retryable
		{
			err:       newMysqlErr(errno.ErrUnknown, "i/o timeout"),
			retryable: false,
		},
		// 1105, retryable
		{
			err:       newMysqlErr(errno.ErrUnknown, "Information schema is out of date"),
			retryable: true,
		},
		{
			err:       newMysqlErr(errno.ErrUnknown, "Information schema is changed"),
			retryable: true,
		},
		// 1105 --> unique error code
		{
			err:       newMysqlErr(errno.ErrInfoSchemaExpired, "Information schema is out of date"),
			retryable: true,
		},
		{
			err:       newMysqlErr(errno.ErrInfoSchemaChanged, "Information schema is changed"),
			retryable: true,
		},
		{
			err:       newMysqlErr(errno.ErrTxnRetryable, "KV error safe to retry Txn(Mvcc(TxnLockNotFound { start_ts: TimeStamp(425904341916582174), commit_ts: TimeStamp(425904342991372376)"),
			retryable: true,
		},
	}

	for _, cs := range cases {
		require.Equal(t, cs.retryable, IsRetryableError(cs.err))
	}
}
