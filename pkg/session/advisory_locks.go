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
	"strconv"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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
	clean          func()
	referenceCount int
	owner          uint64

	// Save/restore innodb_lock_wait_timeout to avoid polluting internal session pools.
	innodbLockWaitTimeoutOriginal       string
	innodbLockWaitTimeoutOriginalLoaded bool
}

// IncrReferences increments the reference count for the advisory lock.
func (a *advisoryLock) IncrReferences() {
	a.referenceCount++
}

// DecrReferences decrements the reference count for the advisory lock.
func (a *advisoryLock) DecrReferences() {
	a.referenceCount--
}

// ReferenceCount returns the current reference count for the advisory lock.
func (a *advisoryLock) ReferenceCount() int {
	return a.referenceCount
}

func (a *advisoryLock) setInnodbLockWaitTimeout(timeout int64) error {
	if !a.innodbLockWaitTimeoutOriginalLoaded {
		// Use GetSessionOrGlobalSystemVar because internal sessions lazily initialize `systems`.
		orig, err := a.session.sessionVars.GetSessionOrGlobalSystemVar(a.ctx, vardef.InnodbLockWaitTimeout)
		if err != nil {
			return err
		}
		a.innodbLockWaitTimeoutOriginal = orig
		a.innodbLockWaitTimeoutOriginalLoaded = true
	}
	return a.session.sessionVars.SetSystemVar(vardef.InnodbLockWaitTimeout, strconv.FormatInt(timeout, 10))
}

func (a *advisoryLock) restoreInnodbLockWaitTimeout() error {
	if !a.innodbLockWaitTimeoutOriginalLoaded {
		return nil
	}
	return a.session.sessionVars.SetSystemVar(vardef.InnodbLockWaitTimeout, a.innodbLockWaitTimeoutOriginal)
}

func (a *advisoryLock) destroySession() {
	dom := domain.GetDomain(a.session)
	if dom != nil {
		dom.SysSessionPool().Destroy(a.session)
		return
	}
	// Fallback (should not happen for advisory locks): close the session directly.
	a.session.Close()
}

// Close releases the advisory lock, which includes
// rolling back the transaction, restoring sysvars, and closing the session.
func (a *advisoryLock) Close() {
	_, rbErr := a.session.ExecuteInternal(a.ctx, "ROLLBACK")
	if rbErr != nil {
		terror.Log(rbErr)
	}
	restoreErr := a.restoreInnodbLockWaitTimeout()
	if restoreErr != nil {
		terror.Log(restoreErr)
	}
	if rbErr != nil || restoreErr != nil {
		// If rollback/restore fails, do not put the session back to the pool.
		a.destroySession()
		return
	}
	a.clean()
}

// GetLock acquires a new advisory lock using a pessimistic transaction.
// The timeout is implemented by using the pessimistic lock timeout.
// We will never COMMIT the transaction, but the err indicates
// if the lock was successfully acquired.
func (a *advisoryLock) GetLock(lockName string, timeout int64) error {
	a.ctx = kv.WithInternalSourceType(a.ctx, kv.InternalTxnOthers)
	if err := a.setInnodbLockWaitTimeout(timeout); err != nil {
		a.Close()
		return err
	}
	_, err := a.session.ExecuteInternal(a.ctx, "BEGIN PESSIMISTIC")
	if err != nil {
		a.Close()
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

// IsUsedLock checks if a lockName is already in use
func (a *advisoryLock) IsUsedLock(lockName string) error {
	defer a.Close() // Rollback
	a.ctx = kv.WithInternalSourceType(a.ctx, kv.InternalTxnOthers)
	if err := a.setInnodbLockWaitTimeout(1); err != nil {
		return err
	}
	_, err := a.session.ExecuteInternal(a.ctx, "BEGIN PESSIMISTIC")
	if err != nil {
		return err
	}
	_, err = a.session.ExecuteInternal(a.ctx, "INSERT INTO mysql.advisory_locks (lock_name) VALUES (%?)", lockName)
	if err != nil {
		return err
	}
	return nil
}
