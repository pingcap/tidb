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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
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

// StateRemote is the interface to control the remote state of the cached table lock meta information.
type StateRemote interface {
	Load(tid int) (CachedTableLockType, uint64, error)
	LockForRead(tid int, now, ts uint64) (bool, error)
	LockForWrite(tid int, now, ts uint64) error
	RenewLease(tid int, ts uint64) (bool, error)
}

type sqlExec interface {
	AffectedRows() uint64
	ExecuteInternal(context.Context, string, ...interface{}) (sqlexec.RecordSet, error)
}

type stateRemoteHandle struct {
	exec sqlExec
}

// NewStateRemote creates a StateRemote object.
func NewStateRemote(exec sqlExec) *stateRemoteHandle {
	return &stateRemoteHandle{
		exec: exec,
	}
}

// CreateMetaLockForCachedTable initializes the cached table meta lock information.
func CreateMetaLockForCachedTable(h sqlExec) error {
	createTable := "CREATE TABLE IF NOT EXISTS `mysql`.`table_cache_meta` (" +
		"`tid` int(11) NOT NULL DEFAULT 0," +
		"`lock_type` enum('NONE','READ', 'INTEND', 'WRITE') NOT NULL DEFAULT 'NONE'," +
		"`lease` bigint(20) NOT NULL DEFAULT 0," +
		"PRIMARY KEY (`tid`))"
	_, err := h.ExecuteInternal(context.Background(), createTable)
	return err
}

// InitRow add a new record into the cached table meta lock table.
func InitRow(ctx context.Context, exec sqlExec, tid int) error {
	_, err := exec.ExecuteInternal(ctx, "insert ignore into mysql.table_cache_meta values (%?, 'NONE', 0)", tid)
	return err
}

func (h *stateRemoteHandle) Load(ctx context.Context, tid int) (CachedTableLockType, uint64, error) {
	return h.loadRow(ctx, tid)
}

func (h *stateRemoteHandle) LockForRead(ctx context.Context, tid int, now, ts uint64) (bool, error) {
	if err := h.beginTxn(ctx); err != nil {
		return false, errors.Trace(err)
	}

	lockType, lease, err := h.loadRow(ctx, tid)
	if err != nil {
		return false, errors.Trace(err)
	}

	switch lockType {
	case CachedTableLockNone:
		if err := h.updateRow(ctx, tid, "READ", ts); err != nil {
			return false, errors.Trace(err)
		}
		if err := h.commitTxn(ctx); err != nil {
			return false, errors.Trace(err)
		}
	case CachedTableLockRead:
		// Update lease
		if err := h.updateRow(ctx, tid, "READ", ts); err != nil {
			return false, errors.Trace(err)
		}
		if err := h.commitTxn(ctx); err != nil {
			return false, errors.Trace(err)
		}
	case CachedTableLockWrite, CachedTableLockIntend:
		if now > lease {
			// Clear orphan lock
			if err := h.updateRow(ctx, tid, "READ", ts); err != nil {
				return false, errors.Trace(err)
			}
			if err := h.commitTxn(ctx); err != nil {
				return false, errors.Trace(err)
			}
		} else {
			// Fail to lock for read, others hold the write lock.
			return false, nil
		}
	}
	return true, nil
}

func (h *stateRemoteHandle) LockForWrite(ctx context.Context, tid int, now, ts uint64) error {
	if err := h.beginTxn(ctx); err != nil {
		return errors.Trace(err)
	}

	lockType, lease, err := h.loadRow(ctx, tid)
	if err != nil {
		return errors.Trace(err)
	}

	switch lockType {
	case CachedTableLockNone:
		if err := h.updateRow(ctx, tid, "WRITE", ts); err != nil {
			return errors.Trace(err)
		}
		if err := h.commitTxn(ctx); err != nil {
			return errors.Trace(err)
		}
	case CachedTableLockRead:
		// Change to READ to write INTEND
		if err := h.updateRow(ctx, tid, "INTEND", lease); err != nil {
			return errors.Trace(err)
		}
		if err := h.commitTxn(ctx); err != nil {
			return errors.Trace(err)
		}

		// Wait for lease to expire.

		// And then change the lock to WRITE
		if err := h.updateRow(ctx, tid, "WRITE", ts); err != nil {
			return errors.Trace(err)
		}
		// h.execSQL(ctx, "commit")
	case CachedTableLockIntend, CachedTableLockWrite:
		if now > lease {
			// Clear orphan lock
			if err := h.updateRow(ctx, tid, "WRITE", ts); err != nil {
				return errors.Trace(err)
			}
			if err := h.commitTxn(ctx); err != nil {
				return errors.Trace(err)
			}
		} else {
			return fmt.Errorf("fail to lock for write, curr state = %v", lockType)
		}
	}
	return err
}

func (h *stateRemoteHandle) RenewLease(ctx context.Context, tid int, ts uint64) (bool, error) {
	_, err := h.execSQL(ctx, "update mysql.table_cache_meta set lease = %? where tid = %? and lock_type ='READ'", ts, tid)
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

func (h *stateRemoteHandle) loadRow(ctx context.Context, tid int) (CachedTableLockType, uint64, error) {
	chunkRows, err := h.execSQL(ctx, "select lock_type, lease from mysql.table_cache_meta where tid = %? for update", tid)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if len(chunkRows) != 1 {
		return 0, 0, errors.Errorf("table_cache_meta tid not exist %d", tid)
	}
	col1 := chunkRows[0].GetEnum(0)
	// Note, the MySQL enum value start from 1 rather than 0
	lockType := CachedTableLockType(col1.Value - 1)
	lease := chunkRows[0].GetUint64(1)
	return lockType, lease, nil
}

func (h *stateRemoteHandle) updateRow(ctx context.Context, tid int, lockType string, lease uint64) error {
	_, err := h.execSQL(ctx, "update mysql.table_cache_meta set lock_type = %?, lease = %? where tid = %?", lockType, lease, tid)
	return err
}

func (h *stateRemoteHandle) execSQL(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	rs, err := h.exec.ExecuteInternal(ctx, sql, args...)
	if rs != nil {
		defer rs.Close()
	}
	if err != nil {
		return nil, err
	}
	if rs != nil {
		return sqlexec.DrainRecordSet(ctx, rs, 1)
	}
	return nil, nil
}
