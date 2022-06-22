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

package session

import (
	"context"

	"github.com/pingcap/tidb/parser/terror"
)

// Advisory Locks are the locks in GET_LOCK() and RELEASE_LOCK().
// We implement them in TiDB by using an INSERT into mysql.advisory_locks
// inside of a pessimistic transaction that is never committed.
//
// Each advisory lock requires its own session, since the pessimistic locks
// can be rolled back in any order (transactions can't release random locks
// like this even if savepoints was supported).
//
// We use referenceCount to track the number of references to the lock in the session.
// A little known feature of advisory locks is that you can call GET_LOCK
// multiple times on the same lock, and it will only be released when
// the reference count reaches zero.

type advisoryLock struct {
	ctx            context.Context
	session        *session
	referenceCount int
}

// IncrReferences increments the reference count for the advisory lock.
func (a *advisoryLock) IncrReferences() {
	a.referenceCount++
}

// DecrReferences decrements the reference count for the advisory lock.
func (a *advisoryLock) DecrReferences() {
	a.referenceCount--
}

// References returns the current reference count for the advisory lock.
func (a *advisoryLock) ReferenceCount() int {
	return a.referenceCount
}

// Close releases the advisory lock, which includes
// rolling back the transaction and closing the session.
func (a *advisoryLock) Close() {
	_, err := a.session.ExecuteInternal(a.ctx, "ROLLBACK")
	terror.Log(err)
	a.session.Close()
}

// GetLock acquires a new advisory lock using a pessimistic transaction.
// The timeout is implemented by using the pessimistic lock timeout.
// We will never COMMIT the transaction, but the err indicates
// if the lock was successfully acquired.
func (a *advisoryLock) GetLock(lockName string, timeout int64) error {
	_, err := a.session.ExecuteInternal(a.ctx, "SET innodb_lock_wait_timeout = %?", timeout)
	if err != nil {
		return err
	}
	_, err = a.session.ExecuteInternal(a.ctx, "BEGIN PESSIMISTIC")
	if err != nil {
		return err
	}
	_, err = a.session.ExecuteInternal(a.ctx, "INSERT INTO mysql.advisory_locks (lock_name) VALUES (%?)", lockName)
	if err != nil {
		// We couldn't acquire the LOCK so we close the session cleanly
		// and return the error to the caller. The caller will need to interpret
		// this differently if it is lock wait timeout or a deadlock.
		a.Close()
		return err
	}
	a.referenceCount++
	return nil
}
