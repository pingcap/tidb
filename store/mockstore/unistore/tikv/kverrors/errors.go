// Copyright 2019-present PingCAP, Inc.
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

package kverrors

import (
	"encoding/hex"
	"fmt"

	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
)

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the lock then retry.
type ErrLocked struct {
	Key  []byte
	Lock *mvcc.Lock
}

// BuildLockErr generates ErrKeyLocked objects
func BuildLockErr(key []byte, lock *mvcc.Lock) *ErrLocked {
	errLocked := &ErrLocked{
		Key:  key,
		Lock: lock,
	}
	return errLocked
}

// Error formats the lock to a string.
func (e *ErrLocked) Error() string {
	lock := e.Lock
	return fmt.Sprintf(
		"key is locked, key: %q, Type: %v, primary: %q, startTS: %v, forUpdateTS: %v, useAsyncCommit: %v",
		e.Key, lock.Op, lock.Primary, lock.StartTS, lock.ForUpdateTS, lock.UseAsyncCommit,
	)
}

// ErrRetryable suggests that client may restart the txn. e.g. write conflict.
type ErrRetryable string

func (e ErrRetryable) Error() string {
	return fmt.Sprintf("retryable: %s", string(e))
}

// ErrRetryable
var (
	ErrLockNotFound    = ErrRetryable("lock not found")
	ErrAlreadyRollback = ErrRetryable("already rollback")
	ErrReplaced        = ErrRetryable("replaced by another transaction")
)

// ErrInvalidOp is returned when an operation cannot be completed.
type ErrInvalidOp struct {
	Op kvrpcpb.Op
}

func (e ErrInvalidOp) Error() string {
	return fmt.Sprintf("invalid op: %s", e.Op.String())
}

// ErrAlreadyCommitted is returned specially when client tries to rollback a
// committed lock.
type ErrAlreadyCommitted uint64

func (e ErrAlreadyCommitted) Error() string {
	return "txn already committed"
}

// ErrKeyAlreadyExists is returned when a key already exists.
type ErrKeyAlreadyExists struct {
	Key []byte
}

func (e ErrKeyAlreadyExists) Error() string {
	return "key already exists"
}

// ErrDeadlock is returned when deadlock is detected.
type ErrDeadlock struct {
	LockKey         []byte
	LockTS          uint64
	DeadlockKeyHash uint64
	WaitChain       []*deadlockpb.WaitForEntry
}

func (e ErrDeadlock) Error() string {
	return "deadlock"
}

// ErrConflict is the error when the commit meets an write conflict error.
type ErrConflict struct {
	StartTS          uint64
	ConflictTS       uint64
	ConflictCommitTS uint64
	Key              []byte
}

func (e *ErrConflict) Error() string {
	return "write conflict"
}

// ErrCommitExpire is returned when commit key commitTs smaller than lock.MinCommitTs
type ErrCommitExpire struct {
	StartTs     uint64
	CommitTs    uint64
	MinCommitTs uint64
	Key         []byte
}

func (e *ErrCommitExpire) Error() string {
	return "commit expired"
}

// ErrTxnNotFound is returned if the required txn info not found on storage
type ErrTxnNotFound struct {
	StartTS    uint64
	PrimaryKey []byte
}

func (e *ErrTxnNotFound) Error() string {
	return "txn not found"
}

// ErrAssertionFailed is returned if any assertion fails on a transaction request.
type ErrAssertionFailed struct {
	StartTS          uint64
	Key              []byte
	Assertion        kvrpcpb.Assertion
	ExistingStartTS  uint64
	ExistingCommitTS uint64
}

func (e *ErrAssertionFailed) Error() string {
	return fmt.Sprintf("AssertionFailed { StartTS: %v, Key: %v, Assertion: %v, ExistingStartTS: %v, ExistingCommitTS: %v }",
		e.StartTS, hex.EncodeToString(e.Key), e.Assertion.String(), e.ExistingStartTS, e.ExistingCommitTS)
}
