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
	"sync"
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
	Load(tblID int64) (stateLocal, error)
	LockForRead(tblID int64, now, ts uint64) error
	PreLock(tblID int64, now, ts uint64) (uint64, error)
	LockForWrite(tblID int64, ts uint64) error
	RenewLease(tblID int64, ts uint64) error
	ClearOrphanLock(tblID int64, ts uint64) error
}
type lockInfo struct {
	lockType CachedTableLockType
	lease    uint64
	sync.RWMutex
}
type stateRemoteHandle struct {
	remoteInfo map[int64]*lockInfo
}

func NewStateRemoteHandle(tblID int64) stateRemote {
	remote := &stateRemoteHandle{
		make(map[int64]*lockInfo),
	}
	var mu sync.RWMutex
	remote.remoteInfo[tblID] = &lockInfo{
		CachedTableLockNone,
		0,
		mu,
	}
	return remote
}
func (h *stateRemoteHandle) RenewLease(tblID int64, ts uint64) error {
	info := *h.remoteInfo[tblID]
	if info.lockType != CachedTableLockRead {
		return fmt.Errorf("can't renew lease in %s lock", info.lockType.String())
	}
	info.lockType = CachedTableLockRead
	info.lease = ts
	info.Lock()
	h.remoteInfo[tblID] = &info
	info.Unlock()
	return nil
}
func (h *stateRemoteHandle) Load(tblID int64) (stateLocal, error) {
	info := *h.remoteInfo[tblID]
	info.RLock()
	defer info.RUnlock()
	return stateLocal{info.lockType,
		info.lease,
	}, nil
}

func (h *stateRemoteHandle) LockForRead(tblID int64, now, ts uint64) error {
	// In the server side:
	info := *h.remoteInfo[tblID]
	switch info.lockType {
	case CachedTableLockNone:
		info.lockType = CachedTableLockRead
		info.lease = ts
	case CachedTableLockRead:
		info.lease = ts
	case CachedTableLockWrite, CachedTableLockIntend:
		if now > info.lease {
			// clear orphan lock
			info.lockType = CachedTableLockRead
			info.lease = ts
		} else {
			return fmt.Errorf("fail to lock for read, curr state = %v", info.lockType)
		}
	}
	info.Lock()
	h.remoteInfo[tblID] = &info
	info.Unlock()
	return nil
}
func (h *stateRemoteHandle) PreLock(tblID int64, now, ts uint64) (uint64, error) {
	// In the server side:
	info := *h.remoteInfo[tblID]
	oldLease := info.lease
	if info.lockType == CachedTableLockNone {
		info.lockType = CachedTableLockIntend
		info.lease = ts
		info.Lock()
		h.remoteInfo[tblID] = &info
		info.Unlock()
		return oldLease, nil
	}

	if info.lockType == CachedTableLockRead {
		info.lockType = CachedTableLockIntend
		info.lease = ts
		info.Lock()
		h.remoteInfo[tblID] = &info
		info.Unlock()
		return oldLease, nil
	}

	return 0, fmt.Errorf("fail to add lock intent, curr state = %v %d %d", info.lockType, info.lease, now)
}

func (h *stateRemoteHandle) LockForWrite(tblID int64, ts uint64) error {
	info := *h.remoteInfo[tblID]
	if info.lockType == CachedTableLockIntend || info.lockType == CachedTableLockNone {
		info.lockType = CachedTableLockWrite
		info.lease = ts
		info.Lock()
		h.remoteInfo[tblID] = &info
		info.Unlock()
		return nil
	}

	return fmt.Errorf("lock for write fail, lock intent is gone! %v", info.lockType)
}
func (h *stateRemoteHandle) ClearOrphanLock(tblID int64, ts uint64) error {
	info := *h.remoteInfo[tblID]
	if info.lockType != CachedTableLockWrite {
		return fmt.Errorf("only clear lock on wrtie lock and now is %s", info.lockType.String())
	}
	info.lockType = CachedTableLockNone
	info.Lock()
	h.remoteInfo[tblID] = &info
	info.Unlock()
	return nil
}
