// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
)

var (
	// ErrBodyMissing response body is missing error
	ErrBodyMissing = errors.New("response body is missing")
)

// mismatchClusterID represents the message that the cluster ID of the PD client does not match the PD.
const mismatchClusterID = "mismatch cluster id"

// MySQL error instances.
var (
<<<<<<< HEAD
	ErrTiKVServerTimeout  = terror.ClassTiKV.New(mysql.ErrTiKVServerTimeout, mysql.MySQLErrName[mysql.ErrTiKVServerTimeout]+kv.TxnRetryableMark)
	ErrResolveLockTimeout = terror.ClassTiKV.New(mysql.ErrResolveLockTimeout, mysql.MySQLErrName[mysql.ErrResolveLockTimeout]+kv.TxnRetryableMark)
	ErrPDServerTimeout    = terror.ClassTiKV.New(mysql.ErrPDServerTimeout, mysql.MySQLErrName[mysql.ErrPDServerTimeout]+"%v")
	ErrRegionUnavailable  = terror.ClassTiKV.New(mysql.ErrRegionUnavailable, mysql.MySQLErrName[mysql.ErrRegionUnavailable]+kv.TxnRetryableMark)
	ErrTiKVServerBusy     = terror.ClassTiKV.New(mysql.ErrTiKVServerBusy, mysql.MySQLErrName[mysql.ErrTiKVServerBusy]+kv.TxnRetryableMark)
	ErrGCTooEarly         = terror.ClassTiKV.New(mysql.ErrGCTooEarly, mysql.MySQLErrName[mysql.ErrGCTooEarly])
=======
	ErrTiKVServerTimeout           = terror.ClassTiKV.New(mysql.ErrTiKVServerTimeout, mysql.MySQLErrName[mysql.ErrTiKVServerTimeout])
	ErrResolveLockTimeout          = terror.ClassTiKV.New(mysql.ErrResolveLockTimeout, mysql.MySQLErrName[mysql.ErrResolveLockTimeout])
	ErrPDServerTimeout             = terror.ClassTiKV.New(mysql.ErrPDServerTimeout, mysql.MySQLErrName[mysql.ErrPDServerTimeout])
	ErrRegionUnavailable           = terror.ClassTiKV.New(mysql.ErrRegionUnavailable, mysql.MySQLErrName[mysql.ErrRegionUnavailable])
	ErrTiKVServerBusy              = terror.ClassTiKV.New(mysql.ErrTiKVServerBusy, mysql.MySQLErrName[mysql.ErrTiKVServerBusy])
	ErrTiKVStaleCommand            = terror.ClassTiKV.New(mysql.ErrTiKVStaleCommand, mysql.MySQLErrName[mysql.ErrTiKVStaleCommand])
	ErrGCTooEarly                  = terror.ClassTiKV.New(mysql.ErrGCTooEarly, mysql.MySQLErrName[mysql.ErrGCTooEarly])
	ErrQueryInterrupted            = terror.ClassTiKV.New(mysql.ErrQueryInterrupted, mysql.MySQLErrName[mysql.ErrQueryInterrupted])
	ErrLockAcquireFailAndNoWaitSet = terror.ClassTiKV.New(mysql.ErrLockAcquireFailAndNoWaitSet, mysql.MySQLErrName[mysql.ErrLockAcquireFailAndNoWaitSet])
	ErrLockWaitTimeout             = terror.ClassTiKV.New(mysql.ErrLockWaitTimeout, mysql.MySQLErrName[mysql.ErrLockWaitTimeout])
	ErrTokenLimit                  = terror.ClassTiKV.New(mysql.ErrTiKVStoreLimit, mysql.MySQLErrName[mysql.ErrTiKVStoreLimit])
	ErrLockExpire                  = terror.ClassTiKV.New(mysql.ErrLockExpire, mysql.MySQLErrName[mysql.ErrLockExpire])
	ErrUnknown                     = terror.ClassTiKV.New(mysql.ErrUnknown, mysql.MySQLErrName[mysql.ErrUnknown])
>>>>>>> 14a4a4e... tikv: fix infinite retry when kv continuing to return staleCommand error (#16481)
)

func init() {
	tikvMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrTiKVServerTimeout:   mysql.ErrTiKVServerTimeout,
		mysql.ErrResolveLockTimeout:  mysql.ErrResolveLockTimeout,
		mysql.ErrPDServerTimeout:     mysql.ErrPDServerTimeout,
		mysql.ErrRegionUnavailable:   mysql.ErrRegionUnavailable,
		mysql.ErrTiKVServerBusy:      mysql.ErrTiKVServerBusy,
		mysql.ErrGCTooEarly:          mysql.ErrGCTooEarly,
		mysql.ErrTruncatedWrongValue: mysql.ErrTruncatedWrongValue,
		mysql.ErrDataOutOfRange:      mysql.ErrDataOutOfRange,
	}
	terror.ErrClassToMySQLCodes[terror.ClassTiKV] = tikvMySQLErrCodes
}
