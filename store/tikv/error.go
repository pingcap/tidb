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
	"github.com/pingcap/kvproto/pkg/pdpb"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
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
	ErrTiKVServerTimeout           = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVServerTimeout)
	ErrTiFlashServerTimeout        = dbterror.ClassTiKV.NewStd(mysql.ErrTiFlashServerTimeout)
	ErrResolveLockTimeout          = dbterror.ClassTiKV.NewStd(mysql.ErrResolveLockTimeout)
	ErrPDServerTimeout             = dbterror.ClassTiKV.NewStd(mysql.ErrPDServerTimeout)
	ErrRegionUnavailable           = dbterror.ClassTiKV.NewStd(mysql.ErrRegionUnavailable)
	ErrTiKVServerBusy              = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVServerBusy)
	ErrTiFlashServerBusy           = dbterror.ClassTiKV.NewStd(mysql.ErrTiFlashServerBusy)
	ErrTiKVStaleCommand            = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVStaleCommand)
	ErrTiKVMaxTimestampNotSynced   = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVMaxTimestampNotSynced)
	ErrGCTooEarly                  = dbterror.ClassTiKV.NewStd(mysql.ErrGCTooEarly)
	ErrQueryInterrupted            = dbterror.ClassTiKV.NewStd(mysql.ErrQueryInterrupted)
	ErrLockAcquireFailAndNoWaitSet = dbterror.ClassTiKV.NewStd(mysql.ErrLockAcquireFailAndNoWaitSet)
	ErrLockWaitTimeout             = dbterror.ClassTiKV.NewStd(mysql.ErrLockWaitTimeout)
	ErrTokenLimit                  = dbterror.ClassTiKV.NewStd(mysql.ErrTiKVStoreLimit)
	ErrLockExpire                  = dbterror.ClassTiKV.NewStd(mysql.ErrLockExpire)
	ErrUnknown                     = dbterror.ClassTiKV.NewStd(mysql.ErrUnknown)
)

// Registers error returned from TiKV.
var (
	_ = dbterror.ClassTiKV.NewStd(mysql.ErrDataOutOfRange)
	_ = dbterror.ClassTiKV.NewStd(mysql.ErrTruncatedWrongValue)
	_ = dbterror.ClassTiKV.NewStd(mysql.ErrDivisionByZero)
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

// PDError wraps *pdpb.Error to implement the error interface.
type PDError struct {
	Err *pdpb.Error
}

func (d *PDError) Error() string {
	return d.Err.String()
}
