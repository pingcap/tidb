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
	"fmt"
)

//CachedTableLockType define the lock type for cached table
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

type stateRemote interface {
	Load() (stateLocal, error)
	LockForRead(now, ts uint64) error
	PreLock(now, ts uint64) (uint64, error)
	LockForWrite(ts uint64) error
}

type stateRemoteHandle struct {
	lease    uint64
	lockType CachedTableLockType
}

func (h *stateRemoteHandle) Load() (stateLocal, error) {
	return stateLocal{
		lockType: h.lockType,
		lease:    h.lease,
	}, nil
}

func (h *stateRemoteHandle) LockForRead(now, ts uint64) error {

	// In the server side:
	switch h.lockType {
	case CachedTableLockNone:
		h.lockType = CachedTableLockRead
		h.lease = ts
	case CachedTableLockRead:
		h.lease = ts
	case CachedTableLockWrite, CachedTableLockIntend:
		if now > h.lease {
			// clear orphan lock
			h.lockType = CachedTableLockRead
			h.lease = ts
		} else {
			return fmt.Errorf("fail to lock for read, curr state = %v", h.lockType)
		}
	}
	return nil
}
func (h *stateRemoteHandle) PreLock(now, ts uint64) (uint64, error) {
	// In the server side:
	oldLease := h.lease
	if h.lockType == CachedTableLockNone {
		h.lockType = CachedTableLockIntend
		h.lease = ts
		return oldLease, nil
	}

	if h.lockType == CachedTableLockRead {
		h.lockType = CachedTableLockIntend
		h.lease = ts
		return oldLease, nil
	}

	return 0, fmt.Errorf("fail to add lock intent, curr state = %v %d %d", h.lockType, h.lease, now)
}

func (h *stateRemoteHandle) LockForWrite(ts uint64) error {
	if h.lockType == CachedTableLockIntend || h.lockType == CachedTableLockNone {
		h.lockType = CachedTableLockWrite
		h.lease = ts
		return nil
	}

	return fmt.Errorf("lock for write fail, lock intent is gone! %v", h.lockType)
}
