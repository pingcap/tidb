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
	"sync/atomic"
	"time"
	
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/oracle"
)

var _ table.Table = &cachedTable{}
var _ table.CachedTable = &cachedTable{}


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

type StateRemote interface {
	Load(tid int) (CachedTableLockType, uint64, error)
	LockForRead(tid int64, now, ts uint64) (bool, error)
	LockForWrite(tid int64, now, ts uint64) error
	RenewLease(tid int64, ts uint64) (bool, error)
}


type cachedTable struct {
	TableCommon

	stateRemoteHandle StateRemote
	cacheData atomic.Value
}

func (c *cachedTable) IsReadFromCache(ts uint64) *table.CacheData {
	// If first read cache table. directly return false, the backend goroutine will help us update the lock information
	// and read the data from the original table at the same time
	tmp := c.cacheData.Load()
	if tmp == nil {
		return nil
	}
	data := tmp.(*table.CacheData)
	if data.Lease > ts {
		return data
	}
	return nil
}

// func (c *cachedTable) GetMemCache() kv.MemBuffer {
// 	return c.MemBuffer
// }

// NewCachedTable creates a new CachedTable Instance
func NewCachedTable(tbl *TableCommon) (table.Table, error) {
	return &cachedTable{
		TableCommon: *tbl,
	}, nil
}

func newMemBuffer(store kv.Storage) (kv.MemBuffer, error) {
	// Here is a trick to get a MemBuffer data, because the internal API is not exposed.
	// Create a transaction with start ts 0, and take the MemBuffer out.
	buffTxn, err := store.BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(0))
	if err != nil {
		return nil, err
	}
	return buffTxn.GetMemBuffer(), nil
}

func loadDataFromOriginalTable(sctx sessionctx.Context, tid int64, lease uint64) (kv.MemBuffer, error) {
	prefix := tablecodec.GenTablePrefix(tid)
	// TODO: Should use a new internal transaction here,
	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if txn.StartTS() >= lease {
		return nil, errors.New("the loaded data is outdate for caching")
	}
	
	it, err := txn.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Close()

	mb, err := newMemBuffer(sctx.GetStore())
	if err != nil {
		return nil, errors.Trace(err)
	}
	for it.Valid() && it.Key().HasPrefix(prefix) {
		value := it.Value()
		err = mb.Set(it.Key(), value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = it.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return mb, nil
}

const defaultLeaseDuration time.Duration = 3*time.Second

func (c *cachedTable) UpdateLockForRead(ctx sessionctx.Context, ts uint64) error {
	// Now only the data is re-load here, and the lock information is not updated. any read-lock information update will in the next pr.
	tid := c.Meta().ID
	physicalTime := oracle.GetTimeFromTS(ts)
	lease := oracle.GoTimeToTS(physicalTime.Add(defaultLeaseDuration))
	succ, err := c.stateRemoteHandle.LockForRead(tid, ts, lease)
	if err != nil {
		return errors.Trace(err)
	}

	if succ {
		mb, err := loadDataFromOriginalTable(ctx, tid, lease)
		if err != nil {
			return errors.Trace(err)
		}

		c.cacheData.Store(&table.CacheData{
			Lease: lease,
			MemBuffer: mb,
		})
	}

	// Current status is not suitable to cache.
	return nil
}
