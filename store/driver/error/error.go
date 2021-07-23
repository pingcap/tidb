// Copyright 2021 PingCAP, Inc.
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

package error

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/dbterror"
	tikverr "github.com/tikv/client-go/v2/error"
)

// tikv error instance
var (
	// ErrTokenLimit is the error that token is up to the limit.
	ErrTokenLimit = dbterror.ClassTiKV.NewStd(errno.ErrTiKVStoreLimit)
	// ErrTiKVServerTimeout is the error when tikv server is timeout.
	ErrTiKVServerTimeout    = dbterror.ClassTiKV.NewStd(errno.ErrTiKVServerTimeout)
	ErrTiFlashServerTimeout = dbterror.ClassTiKV.NewStd(errno.ErrTiFlashServerTimeout)
	// ErrGCTooEarly is the error that GC life time is shorter than transaction duration
	ErrGCTooEarly = dbterror.ClassTiKV.NewStd(errno.ErrGCTooEarly)
	// ErrTiKVStaleCommand is the error that the command is stale in tikv.
	ErrTiKVStaleCommand = dbterror.ClassTiKV.NewStd(errno.ErrTiKVStaleCommand)
	// ErrQueryInterrupted is the error when the query is interrupted.
	ErrQueryInterrupted = dbterror.ClassTiKV.NewStd(errno.ErrQueryInterrupted)
	// ErrTiKVMaxTimestampNotSynced is the error that tikv's max timestamp is not synced.
	ErrTiKVMaxTimestampNotSynced = dbterror.ClassTiKV.NewStd(errno.ErrTiKVMaxTimestampNotSynced)
	// ErrLockAcquireFailAndNoWaitSet is the error that acquire the lock failed while no wait is setted.
	ErrLockAcquireFailAndNoWaitSet = dbterror.ClassTiKV.NewStd(errno.ErrLockAcquireFailAndNoWaitSet)
	ErrResolveLockTimeout          = dbterror.ClassTiKV.NewStd(errno.ErrResolveLockTimeout)
	// ErrLockWaitTimeout is the error that wait for the lock is timeout.
	ErrLockWaitTimeout = dbterror.ClassTiKV.NewStd(errno.ErrLockWaitTimeout)
	// ErrTiKVServerBusy is the error when tikv server is busy.
	ErrTiKVServerBusy = dbterror.ClassTiKV.NewStd(errno.ErrTiKVServerBusy)
	// ErrTiFlashServerBusy is the error that tiflash server is busy.
	ErrTiFlashServerBusy = dbterror.ClassTiKV.NewStd(errno.ErrTiFlashServerBusy)
	// ErrPDServerTimeout is the error when pd server is timeout.
	ErrPDServerTimeout = dbterror.ClassTiKV.NewStd(errno.ErrPDServerTimeout)
	// ErrRegionUnavailable is the error when region is not available.
	ErrRegionUnavailable = dbterror.ClassTiKV.NewStd(errno.ErrRegionUnavailable)
	// ErrUnknown is the unknow error.
	ErrUnknown = dbterror.ClassTiKV.NewStd(errno.ErrUnknown)
)

// Registers error returned from TiKV.
var (
	_ = dbterror.ClassTiKV.NewStd(errno.ErrDataOutOfRange)
	_ = dbterror.ClassTiKV.NewStd(errno.ErrTruncatedWrongValue)
	_ = dbterror.ClassTiKV.NewStd(errno.ErrDivisionByZero)
)

// ToTiDBErr checks and converts a tikv error to a tidb error.
func ToTiDBErr(err error) error {
	originErr := err
	if err == nil {
		return nil
	}
	err = errors.Cause(err)
	if tikverr.IsErrNotFound(err) {
		return kv.ErrNotExist
	}

	if e, ok := err.(*tikverr.ErrWriteConflictInLatch); ok {
		return kv.ErrWriteConflictInTiDB.FastGenByArgs(e.StartTS)
	}

	if e, ok := err.(*tikverr.ErrTxnTooLarge); ok {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(e.Size)
	}

	if errors.ErrorEqual(err, tikverr.ErrCannotSetNilValue) {
		return kv.ErrCannotSetNilValue
	}

	if e, ok := err.(*tikverr.ErrEntryTooLarge); ok {
		return kv.ErrEntryTooLarge.GenWithStackByArgs(e.Limit, e.Size)
	}

	if errors.ErrorEqual(err, tikverr.ErrInvalidTxn) {
		return kv.ErrInvalidTxn
	}

	if errors.ErrorEqual(err, tikverr.ErrTiKVServerTimeout) {
		return ErrTiKVServerTimeout
	}

	if e, ok := err.(*tikverr.ErrPDServerTimeout); ok {
		if len(e.Error()) == 0 {
			return ErrPDServerTimeout
		}
		return ErrPDServerTimeout.GenWithStackByArgs(e.Error())
	}

	if errors.ErrorEqual(err, tikverr.ErrTiFlashServerTimeout) {
		return ErrTiFlashServerTimeout
	}

	if errors.ErrorEqual(err, tikverr.ErrQueryInterrupted) {
		return ErrQueryInterrupted
	}

	if errors.ErrorEqual(err, tikverr.ErrTiKVServerBusy) {
		return ErrTiKVServerBusy
	}

	if errors.ErrorEqual(err, tikverr.ErrTiFlashServerBusy) {
		return ErrTiFlashServerBusy
	}

	if e, ok := err.(*tikverr.ErrGCTooEarly); ok {
		return ErrGCTooEarly.GenWithStackByArgs(e.TxnStartTS, e.GCSafePoint)
	}

	if errors.ErrorEqual(err, tikverr.ErrTiKVStaleCommand) {
		return ErrTiKVStaleCommand
	}

	if errors.ErrorEqual(err, tikverr.ErrTiKVMaxTimestampNotSynced) {
		return ErrTiKVMaxTimestampNotSynced
	}

	if errors.ErrorEqual(err, tikverr.ErrLockAcquireFailAndNoWaitSet) {
		return ErrLockAcquireFailAndNoWaitSet
	}

	if errors.ErrorEqual(err, tikverr.ErrResolveLockTimeout) {
		return ErrResolveLockTimeout
	}

	if errors.ErrorEqual(err, tikverr.ErrLockWaitTimeout) {
		return ErrLockWaitTimeout
	}

	if errors.ErrorEqual(err, tikverr.ErrRegionUnavailable) {
		return ErrRegionUnavailable
	}

	if e, ok := err.(*tikverr.ErrTokenLimit); ok {
		return ErrTokenLimit.GenWithStackByArgs(e.StoreID)
	}

	if errors.ErrorEqual(err, tikverr.ErrUnknown) {
		return ErrUnknown
	}

	return errors.Trace(originErr)
}
