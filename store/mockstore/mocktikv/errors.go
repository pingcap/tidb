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

package mocktikv

import "fmt"

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the lock then retry.
type ErrLocked struct {
	Key     MvccKey
	Primary []byte
	StartTS uint64
	TTL     uint64
	TxnSize uint64
}

// Error formats the lock to a string.
func (e *ErrLocked) Error() string {
	return fmt.Sprintf("key is locked, key: %q, primary: %q, txnStartTS: %v", e.Key, e.Primary, e.StartTS)
}

// ErrKeyAlreadyExist is returned when key exists but this key has a constraint that
// it should not exist. Client should return duplicated entry error.
type ErrKeyAlreadyExist struct {
	Key []byte
}

func (e *ErrKeyAlreadyExist) Error() string {
	return fmt.Sprintf("key already exist, key: %q", e.Key)
}

// ErrRetryable suggests that client may restart the txn.
type ErrRetryable string

func (e ErrRetryable) Error() string {
	return fmt.Sprintf("retryable: %s", string(e))
}

// ErrAbort means something is wrong and client should abort the txn.
type ErrAbort string

func (e ErrAbort) Error() string {
	return fmt.Sprintf("abort: %s", string(e))
}

// ErrAlreadyCommitted is returned specially when client tries to rollback a
// committed lock.
type ErrAlreadyCommitted uint64

func (e ErrAlreadyCommitted) Error() string {
	return "txn already committed"
}

// ErrConflict is returned when the commitTS of key in the DB is greater than startTS.
type ErrConflict struct {
	StartTS    uint64
	ConflictTS uint64
	Key        []byte
}

func (e *ErrConflict) Error() string {
	return "write conflict"
}

// ErrDeadlock is returned when deadlock error is detected.
type ErrDeadlock struct {
	LockTS         uint64
	LockKey        []byte
	DealockKeyHash uint64
}

func (e *ErrDeadlock) Error() string {
	return "deadlock"
}
