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
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
)

var (
	// Retryable1105Msgs list the error messages of some retryable error with `1105` code (`ErrUnknown`).
	Retryable1105Msgs = []string{
		"Information schema is out of date",
		"Information schema is changed",
	}
)

// IsRetryableError checks whether the SQL statement can be retry directly when encountering this error.
// NOTE: this should be compatible with different TiDB versions.
// some errors are only retryable in some special cases, then we mark it as un-retryable:
// - errno.ErrTiKVServerTimeout
// - errno.ErrTableLocked
//
// some errors are un-retryable:
// - errno.ErrQueryInterrupted
//
// some errors are unknown:
// - errno.ErrRegionUnavailable
func IsRetryableError(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case errno.ErrLockDeadlock: // https://dev.mysql.com/doc/refman/5.7/en/innodb-deadlocks.html
		return true // retryable error in MySQL
	case errno.ErrPDServerTimeout,
		errno.ErrTiKVServerBusy,
		errno.ErrResolveLockTimeout,
		errno.ErrInfoSchemaExpired,
		errno.ErrInfoSchemaChanged,
		errno.ErrWriteConflictInTiDB,
		errno.ErrTxnRetryable,
		errno.ErrWriteConflict:
		return true // retryable error in TiDB
	case errno.ErrUnknown: // the old version of TiDB uses `1105` frequently, this should be compatible.
		for _, msg := range Retryable1105Msgs {
			if strings.Contains(mysqlErr.Message, msg) {
				return true
			}
		}
	}

	return false
}
