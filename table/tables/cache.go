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
	cacheData atomic.Value
	handle    StateRemote
}

func leaseFromTS(ts uint64) uint64 {
	// TODO make this configurable in the following PRs
	const defaultLeaseDuration time.Duration = 3 * time.Second
	physicalTime := oracle.GetTimeFromTS(ts)
	lease := oracle.GoTimeToTS(physicalTime.Add(defaultLeaseDuration))
	return lease
}

func (c *cachedTable) TryGetMemcache(ts uint64) (kv.MemBuffer, bool) {
	tmp := c.cacheData.Load()
	if tmp == nil {
		return nil, false
	}
	data := tmp.(*table.CacheData)
	if data.Lease > ts {
		return data.MemBuffer, true
	}
	return nil, false
}

var mockStateRemote = struct {
	Ch   chan remoteTask
	Data *mockStateRemoteData
}{}

// NewCachedTable creates a new CachedTable Instance
func NewCachedTable(tbl *TableCommon) (table.Table, error) {
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

func (c *cachedTable) loadDataFromOriginalTable(ctx sessionctx.Context, lease uint64) (kv.MemBuffer, error) {
	prefix := tablecodec.GenTablePrefix(c.tableID)
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	if txn.StartTS() >= lease {
		return nil, errors.New("the loaded data is outdate for caching")
	}

	buffTxn, err := ctx.GetStore().BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(0))
	if err != nil {
		return nil, err
	}

	buffer := buffTxn.GetMemBuffer()
	it, err := txn.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.Valid() && it.Key().HasPrefix(prefix) {
		value := it.Value()
		err = buffer.Set(it.Key(), value)
		if err != nil {
			return nil, err
		}
		err = it.Next()
		if err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

func (c *cachedTable) UpdateLockForRead(ctx sessionctx.Context, ts uint64) error {
	// Load data from original table and the update lock information.
	tid := c.Meta().ID
	lease := leaseFromTS(ts)
	succ, err := c.handle.LockForRead(tid, ts, lease)
	if err != nil {
		return errors.Trace(err)
	}
	if succ {
		mb, err := c.loadDataFromOriginalTable(ctx, lease)
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

// AddRecord implements the AddRecord method for the table.Table interface.
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

// UpdateRecord implements table.Table
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

// RemoveRecord implements table.Table RemoveRecord interface.
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
