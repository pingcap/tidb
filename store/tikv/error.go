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
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

var (
	// ErrBodyMissing response body is missing error
	ErrBodyMissing = errors.New("response body is missing")
	// When TiDB is closing and send request to tikv fail, do not retry, return this error.
	errTiDBShuttingDown = errors.New("tidb server shutting down")
)

// mismatchClusterID represents the message that the cluster ID of the PD client does not match the PD.
const mismatchClusterID = "mismatch cluster id"

// MySQL error instances.
var (
	ErrTiKVServerTimeout           = terror.ClassTiKV.New(mysql.ErrTiKVServerTimeout, mysql.MySQLErrName[mysql.ErrTiKVServerTimeout])
	ErrResolveLockTimeout          = terror.ClassTiKV.New(mysql.ErrResolveLockTimeout, mysql.MySQLErrName[mysql.ErrResolveLockTimeout])
	ErrPDServerTimeout             = terror.ClassTiKV.New(mysql.ErrPDServerTimeout, mysql.MySQLErrName[mysql.ErrPDServerTimeout])
	ErrRegionUnavailable           = terror.ClassTiKV.New(mysql.ErrRegionUnavailable, mysql.MySQLErrName[mysql.ErrRegionUnavailable])
	ErrTiKVServerBusy              = terror.ClassTiKV.New(mysql.ErrTiKVServerBusy, mysql.MySQLErrName[mysql.ErrTiKVServerBusy])
	ErrGCTooEarly                  = terror.ClassTiKV.New(mysql.ErrGCTooEarly, mysql.MySQLErrName[mysql.ErrGCTooEarly])
	ErrQueryInterrupted            = terror.ClassTiKV.New(mysql.ErrQueryInterrupted, mysql.MySQLErrName[mysql.ErrQueryInterrupted])
	ErrLockAcquireFailAndNoWaitSet = terror.ClassTiKV.New(mysql.ErrLockAcquireFailAndNoWaitSet, mysql.MySQLErrName[mysql.ErrLockAcquireFailAndNoWaitSet])
	ErrLockWaitTimeout             = terror.ClassTiKV.New(mysql.ErrLockWaitTimeout, mysql.MySQLErrName[mysql.ErrLockWaitTimeout])
	ErrTokenLimit                  = terror.ClassTiKV.New(mysql.ErrTiKVStoreLimit, mysql.MySQLErrName[mysql.ErrTiKVStoreLimit])
	ErrUnknown                     = terror.ClassTiKV.New(mysql.ErrUnknown, mysql.MySQLErrName[mysql.ErrUnknown])
)

// Registers error returned from TiKV.
var (
	_ = terror.ClassTiKV.NewStd(mysql.ErrDataOutOfRange)
	_ = terror.ClassTiKV.NewStd(mysql.ErrTruncatedWrongValue)
	_ = terror.ClassTiKV.NewStd(mysql.ErrDivisionByZero)
)

// ErrDeadlock wraps *kvrpcpb.Deadlock to implement the error interface.
// It also marks if the deadlock is retryable.
type ErrDeadlock struct {
	*kvrpcpb.Deadlock
	IsRetryable bool
}

func (d *ErrDeadlock) Error() string {
	return d.Deadlock.String()
}
