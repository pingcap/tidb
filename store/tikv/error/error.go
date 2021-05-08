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

package error

import (
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/util/dbterror"
)

var (
	// ErrBodyMissing response body is missing error
	ErrBodyMissing = errors.New("response body is missing")
	// ErrTiDBShuttingDown is returned when TiDB is closing and send request to tikv fail, do not retry.
	ErrTiDBShuttingDown = errors.New("tidb server shutting down")
	// ErrNotExist means the related data not exist.
	ErrNotExist = errors.New("not exist")
	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = errors.New("can not set nil value")
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = errors.New("invalid transaction")
	// ErrTiKVServerTimeout is the error when tikv server is timeout.
	ErrTiKVServerTimeout = errors.New("tikv server timeout")
	// ErrTiFlashServerTimeout is the error when tiflash server is timeout.
	ErrTiFlashServerTimeout = errors.New("tiflash server timeout")
	// ErrTiKVStaleCommand is the error that the command is stale in tikv.
	ErrTiKVStaleCommand = errors.New("tikv stale command")
	// ErrTiKVMaxTimestampNotSynced is the error that tikv's max timestamp is not synced.
	ErrTiKVMaxTimestampNotSynced = errors.New("tikv max timestamp not synced")
	// ErrResolveLockTimeout is the error that resolve lock timeout.
	ErrResolveLockTimeout = errors.New("resolve lock timeout")
	// ErrTiKVServerBusy is the error when tikv server is busy.
	ErrTiKVServerBusy = errors.New("tikv server busy")
	// ErrTiFlashServerBusy is the error that tiflash server is busy.
	ErrTiFlashServerBusy = errors.New("tiflash server busy")
	// ErrRegionUnavailable is the error when region is not available.
	ErrRegionUnavailable = errors.New("region unavailable")
	// ErrUnknown is the unknow error.
	ErrUnknown = errors.New("unknow")
)

// MismatchClusterID represents the message that the cluster ID of the PD client does not match the PD.
const MismatchClusterID = "mismatch cluster id"

// error instances.
var (
	ErrQueryInterrupted            = dbterror.ClassTiKV.NewStd(CodeQueryInterrupted)
	ErrLockAcquireFailAndNoWaitSet = dbterror.ClassTiKV.NewStd(CodeLockAcquireFailAndNoWaitSet)
	ErrLockWaitTimeout             = dbterror.ClassTiKV.NewStd(CodeLockWaitTimeout)
	ErrTokenLimit                  = dbterror.ClassTiKV.NewStd(CodeTiKVStoreLimit)
)

// Registers error returned from TiKV.
var (
	_ = dbterror.ClassTiKV.NewStd(CodeDataOutOfRange)
	_ = dbterror.ClassTiKV.NewStd(CodeTruncatedWrongValue)
	_ = dbterror.ClassTiKV.NewStd(CodeDivisionByZero)
)

// IsErrNotFound checks if err is a kind of NotFound error.
func IsErrNotFound(err error) bool {
	return errors.ErrorEqual(err, ErrNotExist)
}

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

// ErrKeyExist wraps *pdpb.AlreadyExist to implement the error interface.
type ErrKeyExist struct {
	*kvrpcpb.AlreadyExist
}

func (k *ErrKeyExist) Error() string {
	return k.AlreadyExist.String()
}

// IsErrKeyExist returns true if it is ErrKeyExist.
func IsErrKeyExist(err error) bool {
	_, ok := errors.Cause(err).(*ErrKeyExist)
	return ok
}

// ErrWriteConflict wraps *kvrpcpb.ErrWriteConflict to implement the error interface.
type ErrWriteConflict struct {
	*kvrpcpb.WriteConflict
}

func (k *ErrWriteConflict) Error() string {
	return k.WriteConflict.String()
}

// IsErrWriteConflict returns true if it is ErrWriteConflict.
func IsErrWriteConflict(err error) bool {
	_, ok := errors.Cause(err).(*ErrWriteConflict)
	return ok
}

//NewErrWriteConfictWithArgs generates an ErrWriteConflict with args.
func NewErrWriteConfictWithArgs(startTs, conflictTs, conflictCommitTs uint64, key []byte) *ErrWriteConflict {
	conflict := kvrpcpb.WriteConflict{
		StartTs:          startTs,
		ConflictTs:       conflictTs,
		Key:              key,
		ConflictCommitTs: conflictCommitTs,
	}
	return &ErrWriteConflict{WriteConflict: &conflict}
}

// ErrWriteConflictInLatch is the error when the commit meets an write conflict error when local latch is enabled.
type ErrWriteConflictInLatch struct {
	StartTS uint64
}

func (e *ErrWriteConflictInLatch) Error() string {
	return fmt.Sprintf("write conflict in latch,startTS: %v", e.StartTS)
}

// ErrRetryable wraps *kvrpcpb.Retryable to implement the error interface.
type ErrRetryable struct {
	Retryable string
}

func (k *ErrRetryable) Error() string {
	return k.Retryable
}

// ErrTxnTooLarge is the error when transaction is too large, lock time reached the maximum value.
type ErrTxnTooLarge struct {
	Size int
}

func (e *ErrTxnTooLarge) Error() string {
	return fmt.Sprintf("txn too large, size: %v.", e.Size)
}

// ErrEntryTooLarge is the error when a key value entry is too large.
type ErrEntryTooLarge struct {
	Limit uint64
	Size  uint64
}

func (e *ErrEntryTooLarge) Error() string {
	return fmt.Sprintf("entry size too large, size: %v,limit: %v.", e.Size, e.Limit)
}

// ErrPDServerTimeout is the error when pd server is timeout.
type ErrPDServerTimeout struct {
	msg string
}

// NewErrPDServerTimeout creates an ErrPDServerTimeout.
func NewErrPDServerTimeout(msg string) error {
	return &ErrPDServerTimeout{msg}
}

func (e *ErrPDServerTimeout) Error() string {
	return e.msg
}

// ErrGCTooEarly is the error that GC life time is shorter than transaction duration
type ErrGCTooEarly struct {
	TxnStartTS  time.Time
	GCSafePoint time.Time
}

func (e *ErrGCTooEarly) Error() string {
	return fmt.Sprintf("GC life time is shorter than transaction duration, transaction starts at %v, GC safe point is %v", e.TxnStartTS, e.GCSafePoint)
}
