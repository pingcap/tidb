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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
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

	handle    StateRemote
	cacheData atomic.Value
}

func (c *cachedTable) TryReadFromCache(ts uint64) *table.CacheData {
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

var mockStateRemote = struct {
	Ch   chan remoteTask
	Data *mockStateRemoteData
}{}

func newMemBuffer(store kv.Storage) (kv.MemBuffer, error) {
	// Here is a trick to get a MemBuffer data, because the internal API is not exposed.
	// Create a transaction with start ts 0, and take the MemBuffer out.
	buffTxn, err := store.BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(0))
	if err != nil {
		return nil, err
	}
	return buffTxn.GetMemBuffer(), nil
}

func leaseFromTS(ts uint64) uint64 {
	const defaultLeaseDuration time.Duration = 3 * time.Second
	physicalTime := oracle.GetTimeFromTS(ts)
	lease := oracle.GoTimeToTS(physicalTime.Add(defaultLeaseDuration))
	return lease
}

// NewCachedTable creates a new CachedTable Instance
func NewCachedTable(tbl *TableCommon) (table.Table, error) {
	// Only for the first time.
	if mockStateRemote.Data == nil {
		mockStateRemote.Data = newMockStateRemoteData()
		mockStateRemote.Ch = make(chan remoteTask, 100)
		go mockRemoteService(mockStateRemote.Data, mockStateRemote.Ch)
	}

	ret := &cachedTable{
		TableCommon: *tbl,
		handle:      &mockStateRemoteHandle{mockStateRemote.Ch},
	}

	return ret, nil
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

func (c *cachedTable) UpdateLockForRead(ctx sessionctx.Context, ts uint64) error {
	tid := c.Meta().ID
	lease := leaseFromTS(ts)
	succ, err := c.handle.LockForRead(tid, ts, lease)
	if err != nil {
		return errors.Trace(err)
	}

	if succ {
		mb, err := loadDataFromOriginalTable(ctx, tid, lease)
		if err != nil {
			return errors.Trace(err)
		}

		c.cacheData.Store(&table.CacheData{
			Lease:     lease,
			MemBuffer: mb,
		})
	}

	// Current status is not suitable to cache.
	return nil
}

func (c *cachedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	now := txn.StartTS()
	err = c.handle.LockForWrite(c.Meta().ID, now, leaseFromTS(now))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.TableCommon.AddRecord(ctx, r, opts...)
}

// UpdateRecord updates a row which should contain only writable columns.
func (c *cachedTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, oldData, newData []types.Datum, touched []bool) error {
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}
	now := txn.StartTS()
	err = c.handle.LockForWrite(c.Meta().ID, now, leaseFromTS(now))
	if err != nil {
		return errors.Trace(err)
	}
	return c.TableCommon.UpdateRecord(ctx, sctx, h, oldData, newData, touched)
}

// RemoveRecord removes a row in the table.
func (c *cachedTable) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	now := txn.StartTS()
	err = c.handle.LockForWrite(c.Meta().ID, now, leaseFromTS(now))
	if err != nil {
		return errors.Trace(err)
	}
	return c.TableCommon.RemoveRecord(ctx, h, r)
}
