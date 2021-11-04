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

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

var _ table.Table = &cachedTable{}
var _ table.CachedTable = &cachedTable{}

type cachedTable struct {
	TableCommon
	kv.MemBuffer
	mu sync.RWMutex
	*stateLocal
	stateRemote
	isReNewLease bool
}
type stateLocal struct {
	lockType CachedTableLockType
	lease    uint64
}

func (s *stateLocal) LockType() CachedTableLockType {
	return s.lockType
}

func (s *stateLocal) Lease() uint64 {
	return s.lease
}

func (c *cachedTable) IsLocalStale(tsNow uint64) bool {
	return isLocalStale(c.stateLocal, tsNow)
}

func (c *cachedTable) SyncState() error {
	s, err := c.stateRemote.Load(c.tableID)
	if err != nil {
		return err
	}
	c.stateLocal = &s
	fmt.Println("sync state here", c.stateLocal)
	return nil
}

func (c *cachedTable) PreLock(ts uint64) (uint64, error) {
	ts1 := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	// Lock remote.
	oldLease, err := c.stateRemote.PreLock(c.tableID, ts, ts1)
	if err == nil {
		// Update local on success
		c.stateLocal = &stateLocal{
			lockType: CachedTableLockIntend,
			lease:    ts1,
		}
	}
	return oldLease, err
}

func (c *cachedTable) lockForWrite(ts uint64) error {
	// Make sure the local state is accurate.
	if c.IsLocalStale(ts) {
		err := c.SyncState()
		if err != nil {
			return err
		}
	}

	switch c.stateLocal.LockType() {
	case CachedTableLockRead:
		oldLease, err := c.PreLock(ts)
		if err != nil {
			return err
		}

		if c.stateLocal.Lease() > ts {
			// should wait read lease expire
			t1 := oracle.GetTimeFromTS(oldLease)
			t2 := oracle.GetTimeFromTS(ts)
			d := t1.Sub(t2)
			fmt.Println("lease =", t1, "now = ", t2, "sleep = ", d)
			time.Sleep(d)
		}
		// newTs := oldLease + 3
		newTs := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
		if err := c.stateRemote.LockForWrite(c.tableID, newTs); err != nil {
			return err
		}
	case CachedTableLockNone:
		if err := c.stateRemote.LockForWrite(c.tableID, ts); err != nil {
			return c.lockForWrite(ts)
		}
	case CachedTableLockWrite:
		if c.stateLocal.Lease() > ts {
			fmt.Println("hold write lock, write directly")
			break
		}
		fmt.Println("write lock but lease is gone ...", c.stateLocal.Lease(), ts)
		// TODO: the whole steps
	}
	return nil
}

func (c *cachedTable) TryGetMemcache(ts uint64) (kv.MemBuffer, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.isReadFromCache(ts) {
		return c.MemBuffer, true
	}
	return nil, false
}
func (c *cachedTable) isReadFromCache(ts uint64) bool {
	// If first read cache table. directly return false, the backend goroutine will help us update the lock information
	// and read the data from the original table at the same time
	if c.MemBuffer == nil {
		return false
	}
	return isReadFromCache(c.stateLocal, ts, &c.isReNewLease)
}

// NewCachedTable creates a new CachedTable Instance
func NewCachedTable(tbl *TableCommon) (table.Table, error) {
	return &cachedTable{
		TableCommon: *tbl,
		// only a remote instance but not really write to kv
		stateRemote: NewStateRemoteHandle(tbl.tableID),
	}, nil
}

func (c *cachedTable) loadDataFromOriginalTable(ctx sessionctx.Context) error {
	prefix := tablecodec.GenTablePrefix(c.tableID)
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	buffTxn, err := ctx.GetStore().BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(0))
	if err != nil {
		return err
	}

	buffer := buffTxn.GetMemBuffer()
	it, err := txn.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()
	if !it.Valid() {
		return nil
	}
	for it.Valid() && it.Key().HasPrefix(prefix) {
		value := it.Value()
		err = buffer.Set(it.Key(), value)
		if err != nil {
			return err
		}
		err = it.Next()
		if err != nil {
			return err
		}
	}

	c.mu.Lock()
	c.MemBuffer = buffer
	c.mu.Unlock()
	return nil
}

func (c *cachedTable) UpdateLockForRead(ctx sessionctx.Context, ts uint64) error {
	// Load data from original table and the update lock information.
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	// Now only the data is re-load here, and the lock information is not updated. any read-lock information update will in the next pr.
	err := c.loadDataFromOriginalTable(ctx)
	if err != nil {
		return fmt.Errorf("reload data error")
	}
	ts1 := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	err = c.stateRemote.LockForRead(c.tableID, ts, ts1)
	if err == nil {
		// Update the local state here on success.
		c.stateLocal = &stateLocal{
			lockType: CachedTableLockRead,
			lease:    ts1,
		}
	} else {
		fmt.Println("warn, lock for read get", err)
	}
	return nil
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (c *cachedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	err = c.lockForWrite(txn.StartTS())
	if err != nil {
		return nil, err
	}

	record, err := c.TableCommon.AddRecord(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	err = c.ClearOrphanLock(c.tableID, txn.StartTS())
	if err != nil {
		return nil, err
	}
	return record, nil

}

// UpdateRecord implements table.Table
func (c *cachedTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error {
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}

	err = c.lockForWrite(txn.StartTS())
	if err != nil {
		return err
	}

	err = c.TableCommon.UpdateRecord(ctx, sctx, h, currData, newData, touched)
	if err != nil {
		return err
	}
	err = c.ClearOrphanLock(c.tableID, txn.StartTS())
	if err != nil {
		return err
	}
	return nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (c *cachedTable) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	ts := txn.StartTS()

	err = c.lockForWrite(ts)
	if err != nil {
		return err
	}
	err = c.TableCommon.RemoveRecord(ctx, h, r)
	if err != nil {
		return err
	}
	err = c.ClearOrphanLock(c.tableID, txn.StartTS())
	if err != nil {
		return err
	}
	return nil
}

func isLocalStale(s *stateLocal, tsNow uint64) bool {
	if s == nil {
		fmt.Println("local is stale due to nil")
		return true
	}
	lease := s.Lease()
	if lease <= tsNow {
		fmt.Println("local is stale, lease = ", lease, "now = ", tsNow)
		return true
	}
	return false
}

func isReadFromCache(s *stateLocal, ts uint64, isReNewLease *bool) bool {
	*isReNewLease = false
	if isLocalStale(s, ts) {
		return false
	}
	switch s.LockType() {
	case CachedTableLockRead:
		if s.Lease() > ts {
			sub := oracle.GetTimeFromTS(s.Lease()).Sub(oracle.GetTimeFromTS(ts))
			if sub < time.Second {
				*isReNewLease = true
				return true
			}
			return true
		} else {
			return false
		}
	case CachedTableLockNone:
		return false
	case CachedTableLockWrite:
		return false
	case CachedTableLockIntend:
		if s.Lease() > ts {
			*isReNewLease = false
			return true
		} else {
			return false
		}
	}
	panic("should never here")
}
