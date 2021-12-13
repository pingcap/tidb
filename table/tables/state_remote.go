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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
)

// CachedTableLockType define the lock type for cached table
type CachedTableLockType int

const (
	// CachedTableLockNone means there is no lock.
	CachedTableLockNone CachedTableLockType = iota
	// CachedTableLockRead is the READ lock type.
	CachedTableLockRead
	// CachedTableLockIntend is the write INTEND, it exists when the changing READ to WRITE, and the READ lock lease is not expired..
	CachedTableLockIntend
	// CachedTableLockWrite is the WRITE lock type.
	CachedTableLockWrite
)

func (l CachedTableLockType) String() string {
	switch l {
	case CachedTableLockNone:
		return "NONE"
	case CachedTableLockRead:
		return "READ"
	case CachedTableLockIntend:
		return "INTEND"
	case CachedTableLockWrite:
		return "WRITE"
	}
	panic("invalid CachedTableLockType value")
}

// StateRemote is the interface to control the remote state of the cached table's lock meta information.
type StateRemote interface {
	// Load obtain the corresponding lock type and lease value according to the tableID
	Load(ctx context.Context, tid int64) (CachedTableLockType, uint64, error)

	// LockForRead try to add a read lock to the table with the specified tableID.
	// If this operation succeed, according to the protocol, the TiKV data will not be
	// modified until the lease expire. It's safe for the caller to load the table data,
	// cache and use the data.
	// The parameter `now` means the current tso. Because the tso is get from PD, in
	// the TiDB side, its value lags behind the real one forever, this doesn't matter.
	// Because `now` is only used to clean up the orphan lock, as long as it's smaller
	// than the real one, the correctness of the algorithm is not violated.
	LockForRead(ctx context.Context, tid int64, now, lease uint64) (bool, error)

	// LockForWrite try to add a write lock to the table with the specified tableID
	LockForWrite(ctx context.Context, tid int64, now, ts uint64) error

	// RenewLease attempt to renew the read / write lock on the table with the specified tableID
	RenewLease(ctx context.Context, tid int64, oldTs uint64, newTs uint64, op RenewLeaseType) (bool, error)
}

// mockStateRemoteHandle implement the StateRemote interface.
type mockStateRemoteHandle struct {
	ch chan remoteTask
}

var _ StateRemote = &mockStateRemoteHandle{}

func (r *mockStateRemoteHandle) Load(ctx context.Context, tid int64) (CachedTableLockType, uint64, error) {
	op := &loadOP{tid: tid}
	op.Add(1)
	r.ch <- op
	op.Wait()
	return op.lockType, op.lease, op.err
}

func (r *mockStateRemoteHandle) LockForRead(ctx context.Context, tid int64, now, ts uint64) (bool, error) {
	op := &lockForReadOP{tid: tid, now: now, ts: ts}
	op.Add(1)
	r.ch <- op
	op.Wait()
	return op.succ, op.err
}

func (r *mockStateRemoteHandle) LockForWrite(ctx context.Context, tid int64, now, ts uint64) error {
	op := &lockForWriteOP{tid: tid, now: now, ts: ts}
	op.Add(1)
	r.ch <- op
	op.Wait()
	if op.err != nil {
		return errors.Trace(op.err)
	}
	// No block, finish.
	if op.oldLease == 0 {
		return nil
	}

	// Wait for read lock to expire.
	t1 := oracle.GetTimeFromTS(op.oldLease)
	t2 := oracle.GetTimeFromTS(now)
	waitDuration := t1.Sub(t2)
	time.Sleep(waitDuration)

	// TODO: now should be a new ts
	op = &lockForWriteOP{tid: tid, now: op.oldLease + 1, ts: leaseFromTS(op.oldLease + 1)}
	op.Add(1)
	r.ch <- op
	op.Wait()
	// op.oldLease should be 0 this time.
	return op.err
}

func (r *mockStateRemoteHandle) RenewLease(ctx context.Context, tid int64, oldTs uint64, newTs uint64, op RenewLeaseType) (bool, error) {
	switch op {
	case RenewReadLease:
		op := &renewLeaseForReadOP{tid: tid, oldTs: oldTs, newTs: newTs}
		op.Add(1)
		r.ch <- op
		op.Wait()
		return op.succ, op.err
	case RenewWriteLease:
		// TODO : Renew Write Lease will implement in next pr.
	}
	return false, errors.New("not implemented yet")
}

func mockRemoteService(r *mockStateRemoteData, ch chan remoteTask) {
	for task := range ch {
		task.Exec(r)
	}
}

type remoteTask interface {
	Exec(data *mockStateRemoteData)
}

// loadOP is a kind of remoteTask
type loadOP struct {
	sync.WaitGroup
	// Input
	tid int64

	// Output
	lockType CachedTableLockType
	lease    uint64
	err      error
}

func (op *loadOP) Exec(data *mockStateRemoteData) {
	op.lockType, op.lease, op.err = data.Load(op.tid)
	op.Done()
}

// lockForReadOP is a kind of rmoteTask
type lockForReadOP struct {
	sync.WaitGroup
	// Input
	tid int64
	now uint64
	ts  uint64

	// Output
	succ bool
	err  error
}

func (op *lockForReadOP) Exec(r *mockStateRemoteData) {
	op.succ, op.err = r.LockForRead(op.tid, op.now, op.ts)
	op.Done()
}

// lockForWriteOP is a kind of remote task
type lockForWriteOP struct {
	sync.WaitGroup
	// Input
	tid int64
	now uint64
	ts  uint64

	// Output
	err      error
	oldLease uint64
}

func (op *lockForWriteOP) Exec(data *mockStateRemoteData) {
	op.oldLease, op.err = data.LockForWrite(op.tid, op.now, op.ts)
	op.Done()
}

// renewForReadOP is a kind of remote task
type renewLeaseForReadOP struct {
	sync.WaitGroup
	// Input
	tid   int64
	oldTs uint64
	newTs uint64

	// Output
	succ bool
	err  error
}

func (op *renewLeaseForReadOP) Exec(r *mockStateRemoteData) {
	op.succ, op.err = r.renewLeaseForRead(op.tid, op.oldTs, op.newTs)
	op.Done()
}

type mockStateRemoteData struct {
	mu   sync.Mutex
	data map[int64]*stateRecord
}

type stateRecord struct {
	lockLease    uint64
	oldReadLease uint64 // only use for intent lock, it means old read lease.
	lockType     CachedTableLockType
}

func newMockStateRemoteData() *mockStateRemoteData {
	return &mockStateRemoteData{
		data: make(map[int64]*stateRecord),
	}
}

func (r *mockStateRemoteData) Load(tid int64) (CachedTableLockType, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.data[tid]
	if !ok {
		return CachedTableLockNone, 0, nil
	}
	return record.lockType, record.lockLease, nil
}

func (r *mockStateRemoteData) LockForRead(tid int64, now, ts uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.data[tid]
	if !ok {
		record = &stateRecord{
			lockLease:    ts,
			oldReadLease: ts,
			lockType:     CachedTableLockRead,
		}
		r.data[tid] = record
		return true, nil
	}
	switch record.lockType {
	case CachedTableLockNone:
		// Add the read lock
		record.lockType = CachedTableLockRead
		record.lockLease = ts
		return true, nil
	case CachedTableLockRead:
		// Renew lease for this case.
		if record.lockLease < ts {
			record.lockLease = ts
			return true, nil
		}
		// Already read locked.
		return true, nil
	case CachedTableLockWrite, CachedTableLockIntend:
		if now > record.lockLease {
			// Outdated...clear orphan lock
			record.lockType = CachedTableLockRead
			record.lockLease = ts
			return true, nil
		}
		return false, nil
	}
	return false, errors.New("unknown lock type")
}

func (r *mockStateRemoteData) LockForWrite(tid int64, now, ts uint64) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.data[tid]
	if !ok {
		record = &stateRecord{
			lockType:  CachedTableLockWrite,
			lockLease: ts,
		}
		r.data[tid] = record
		return 0, nil
	}

	switch record.lockType {
	case CachedTableLockNone:
		record.lockType = CachedTableLockWrite
		record.lockLease = ts
		return 0, nil
	case CachedTableLockRead:
		if now > record.lockLease {
			// Outdated, clear orphan lock and add write lock directly.
			record.lockType = CachedTableLockWrite
			record.lockLease = ts
			return 0, nil
		}

		// Change state to intend, prevent renew lease operation.
		oldLease := record.lockLease
		record.lockType = CachedTableLockIntend
		record.lockLease = leaseFromTS(ts)
		record.oldReadLease = oldLease
		return oldLease, nil
	case CachedTableLockWrite:
		if ts > record.lockLease {
			record.lockLease = ts
		}
	case CachedTableLockIntend:
		// Add the write lock.
		if now > record.oldReadLease {
			record.lockType = CachedTableLockWrite
			record.lockLease = ts
		} else {
			return record.oldReadLease, nil
		}
	default:
		return 0, fmt.Errorf("wrong lock state %v", record.lockType)
	}
	return 0, nil
}

func (r *mockStateRemoteData) renewLeaseForRead(tid int64, oldTs uint64, newTs uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.data[tid]
	if !ok {
		record = &stateRecord{
			lockLease: newTs,
			lockType:  CachedTableLockRead,
		}
		r.data[tid] = record
		return true, nil
	}
	if record.lockType != CachedTableLockRead {
		return false, errors.New("The read lock can be renewed only in the read lock state")
	}
	if record.lockLease < oldTs {
		return false, errors.New("The remote Lease is invalid")
	}
	if record.lockLease <= newTs {
		record.lockLease = newTs
		return true, nil
	}
	return false, errors.New("The new lease is smaller than the old lease is an illegal contract renewal operation")
}

type sqlExec interface {
	AffectedRows() uint64
	ExecuteInternal(context.Context, string, ...interface{}) (sqlexec.RecordSet, error)
	GetStore() kv.Storage
}

type stateRemoteHandle struct {
	exec sqlExec
	sync.Mutex
}

// NewStateRemote creates a StateRemote object.
func NewStateRemote(exec sqlExec) *stateRemoteHandle {
	return &stateRemoteHandle{
		exec: exec,
	}
}

var _ StateRemote = &stateRemoteHandle{}

func (h *stateRemoteHandle) Load(ctx context.Context, tid int64) (CachedTableLockType, uint64, error) {
	lockType, lease, _, err := h.loadRow(ctx, tid)
	return lockType, lease, err
}

// LockForRead try to lock the table, if this operation succeed, the remote data
// is "read locked" and will not be modified according to the protocol, until the lease expire.
func (h *stateRemoteHandle) LockForRead(ctx context.Context, tid int64, now, ts uint64) ( /*succ*/ bool, error) {
	h.Lock()
	defer h.Unlock()
	succ := false
	err := h.runInTxn(ctx, func(ctx context.Context) error {
		lockType, lease, _, err := h.loadRow(ctx, tid)
		if err != nil {
			return errors.Trace(err)
		}
		// The old lock is outdated, clear orphan lock.
		if now > lease {
			succ = true
			if err := h.updateRow(ctx, tid, "READ", ts); err != nil {
				return errors.Trace(err)
			}
			return nil
		}

		switch lockType {
		case CachedTableLockNone:
		case CachedTableLockRead:
		case CachedTableLockWrite, CachedTableLockIntend:
			return nil
		}
		succ = true
		if ts > lease { // Note the check, don't decrease lease value!
			if err := h.updateRow(ctx, tid, "READ", ts); err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	})
	return succ, err
}

func (h *stateRemoteHandle) LockForWrite(ctx context.Context, tid int64, now, ts uint64) error {
	h.Lock()
	defer h.Unlock()
	for {
		waitAndRetry, err := h.lockForWriteOnce(ctx, tid, now, ts)
		if err != nil {
			return err
		}
		if waitAndRetry == 0 {
			break
		}

		time.Sleep(waitAndRetry)
		store := h.exec.GetStore()
		o := store.GetOracle()
		newTS, err := o.GetTimestamp(ctx, &oracle.Option{TxnScope: kv.GlobalTxnScope})
		if err != nil {
			return errors.Trace(err)
		}
		now, ts = newTS, leaseFromTS(newTS)
	}
	return nil
}

func (h *stateRemoteHandle) lockForWriteOnce(ctx context.Context, tid int64, now, ts uint64) (waitAndRetry time.Duration, err error) {
	err = h.runInTxn(ctx, func(ctx context.Context) error {
		lockType, lease, oldReadLease, err := h.loadRow(ctx, tid)
		if err != nil {
			return errors.Trace(err)
		}
		// The lease is outdated, so lock is invalid, clear orphan lock of any kind.
		if now > lease {
			if err := h.updateRow(ctx, tid, "WRITE", ts); err != nil {
				return errors.Trace(err)
			}
			return nil
		}

		// The lease is valid.
		switch lockType {
		case CachedTableLockNone:
			if err = h.updateRow(ctx, tid, "WRITE", ts); err != nil {
				return errors.Trace(err)
			}
		case CachedTableLockRead:
			// Change from READ to INTEND
			if _, err = h.execSQL(ctx, "update mysql.table_cache_meta set lock_type='INTEND', oldReadLease=%?, lease=%? where tid=%?", lease, ts, tid); err != nil {
				return errors.Trace(err)
			}
			// Wait for lease to expire, and then retry.
			waitAndRetry = waitForLeaseExpire(oldReadLease, now)
		case CachedTableLockIntend, CachedTableLockWrite:
			// `now` exceed `oldReadLease` means wait for READ lock lease is done, it's safe to read here.
			if now > oldReadLease {
				if lockType == CachedTableLockIntend {
					if err = h.updateRow(ctx, tid, "WRITE", ts); err != nil {
						return errors.Trace(err)
					}
				}
				return nil
			}
			// Otherwise, the WRITE should wait for the READ lease expire.
			// And then retry changing the lock to WRITE
			waitAndRetry = waitForLeaseExpire(oldReadLease, now)
		}
		return nil
	})

	return
}

func waitForLeaseExpire(oldReadLease, now uint64) time.Duration {
	if oldReadLease >= now {
		t1 := oracle.GetTimeFromTS(oldReadLease)
		t2 := oracle.GetTimeFromTS(now)
		waitDuration := t1.Sub(t2)
		return waitDuration
	}
	return 0
}

func (h *stateRemoteHandle) RenewLease(ctx context.Context, tid int64, now, newTs uint64, op RenewLeaseType) (bool, error) {
	h.Lock()
	defer h.Unlock()
	// TODO: `now` should use the real current tso to check the old lease is not expired.
	_, err := h.execSQL(ctx, "update mysql.table_cache_meta set lease = %? where tid = %? and lock_type ='READ'", newTs, tid)
	if err != nil {
		return false, errors.Trace(err)
	}
	succ := h.exec.AffectedRows() > 0
	return succ, err
}

func (h *stateRemoteHandle) beginTxn(ctx context.Context) error {
	_, err := h.execSQL(ctx, "begin")
	return err
}

func (h *stateRemoteHandle) commitTxn(ctx context.Context) error {
	_, err := h.execSQL(ctx, "commit")
	return err
}

func (h *stateRemoteHandle) rollbackTxn(ctx context.Context) error {
	_, err := h.execSQL(ctx, "rollback")
	return err
}

func (h *stateRemoteHandle) runInTxn(ctx context.Context, fn func(ctx context.Context) error) error {
	err := h.beginTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = fn(ctx)
	if err != nil {
		terror.Log(h.rollbackTxn(ctx))
		return errors.Trace(err)
	}

	return h.commitTxn(ctx)
}

func (h *stateRemoteHandle) loadRow(ctx context.Context, tid int64) (CachedTableLockType, uint64, uint64, error) {
	chunkRows, err := h.execSQL(ctx, "select lock_type, lease, oldReadLease from mysql.table_cache_meta where tid = %? for update", tid)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}
	if len(chunkRows) != 1 {
		return 0, 0, 0, errors.Errorf("table_cache_meta tid not exist %d", tid)
	}
	col1 := chunkRows[0].GetEnum(0)
	// Note, the MySQL enum value start from 1 rather than 0
	lockType := CachedTableLockType(col1.Value - 1)
	lease := chunkRows[0].GetUint64(1)
	oldReadLease := chunkRows[0].GetUint64(2)
	return lockType, lease, oldReadLease, nil
}

func (h *stateRemoteHandle) updateRow(ctx context.Context, tid int64, lockType string, lease uint64) error {
	_, err := h.execSQL(ctx, "update mysql.table_cache_meta set lock_type = %?, lease = %? where tid = %?", lockType, lease, tid)
	return err
}

func (h *stateRemoteHandle) execSQL(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	rs, err := h.exec.ExecuteInternal(ctx, sql, args...)
	if rs != nil {
		defer rs.Close()
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rs != nil {
		return sqlexec.DrainRecordSet(ctx, rs, 1)
	}
	return nil, nil
}
