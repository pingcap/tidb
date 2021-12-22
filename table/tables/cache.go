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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// RenewLeaseType define the type for renew lease.
type RenewLeaseType int

const (
	// RenewReadLease means renew read lease.
	RenewReadLease RenewLeaseType = iota + 1
	// RenewWriteLease means renew write lease.
	RenewWriteLease
)

var (
	_ table.CachedTable = &cachedTable{}
)

type cachedTable struct {
	TableCommon
	cacheData atomic.Value
	handle    StateRemote
	renewCh   chan func()
}

// cacheData pack the cache data and lease.
type cacheData struct {
	Start uint64
	Lease uint64
	kv.MemBuffer
}

func leaseFromTS(ts uint64) uint64 {
	// TODO make this configurable in the following PRs
	const defaultLeaseDuration time.Duration = 3 * time.Second
	physicalTime := oracle.GetTimeFromTS(ts)
	lease := oracle.GoTimeToTS(physicalTime.Add(defaultLeaseDuration))
	return lease
}

func newMemBuffer(store kv.Storage) (kv.MemBuffer, error) {
	// Here is a trick to get a MemBuffer data, because the internal API is not exposed.
	// Create a transaction with start ts 0, and take the MemBuffer out.
	buffTxn, err := store.Begin(tikv.WithStartTS(0))
	if err != nil {
		return nil, err
	}
	return buffTxn.GetMemBuffer(), nil
}

func (c *cachedTable) TryReadFromCache(ts uint64) kv.MemBuffer {
	tmp := c.cacheData.Load()
	if tmp == nil {
		return nil
	}
	data := tmp.(*cacheData)
	if ts >= data.Start && ts < data.Lease {
		leaseTime := oracle.GetTimeFromTS(data.Lease)
		nowTime := oracle.GetTimeFromTS(ts)
		distance := leaseTime.Sub(nowTime)
		// TODO make this configurable in the following PRs
		if distance >= 0 && distance <= (1500*time.Millisecond) {
			c.renewCh <- c.renewLease(ts, RenewReadLease, data)
		}
		return data.MemBuffer
	}
	return nil
}

// newCachedTable creates a new CachedTable Instance
func newCachedTable(tbl *TableCommon) (table.Table, error) {
	ret := &cachedTable{
		TableCommon: *tbl,
	}
	return ret, nil
}

// Init is an extra operation for cachedTable after TableFromMeta,
// Because cachedTable need some additional parameter that can't be passed in TableFromMeta.
func (c *cachedTable) Init(renewCh chan func(), exec sqlexec.SQLExecutor) error {
	c.renewCh = renewCh
	raw, ok := exec.(sqlExec)
	if !ok {
		return errors.New("Need sqlExec rather than sqlexec.SQLExecutor")
	}
	c.handle = NewStateRemote(raw)
	return nil
}

func (c *cachedTable) loadDataFromOriginalTable(store kv.Storage, lease uint64) (kv.MemBuffer, uint64, error) {
	buffer, err := newMemBuffer(store)
	if err != nil {
		return nil, 0, err
	}
	var startTS uint64
	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		prefix := tablecodec.GenTablePrefix(c.tableID)
		if err != nil {
			return errors.Trace(err)
		}
		startTS = txn.StartTS()
		if startTS >= lease {
			return errors.New("the loaded data is outdated for caching")
		}
		it, err := txn.Iter(prefix, prefix.PrefixNext())
		if err != nil {
			return errors.Trace(err)
		}
		defer it.Close()

		for it.Valid() && it.Key().HasPrefix(prefix) {
			value := it.Value()
			err = buffer.Set(it.Key(), value)
			if err != nil {
				return errors.Trace(err)
			}
			err = it.Next()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return buffer, startTS, nil
}

func (c *cachedTable) UpdateLockForRead(ctx context.Context, store kv.Storage, ts uint64) error {
	// Load data from original table and the update lock information.
	tid := c.Meta().ID
	lease := leaseFromTS(ts)
	succ, err := c.handle.LockForRead(ctx, tid, lease)
	if err != nil {
		return errors.Trace(err)
	}
	if succ {
		mb, startTS, err := c.loadDataFromOriginalTable(store, lease)
		if err != nil {
			return errors.Trace(err)
		}

		c.cacheData.Store(&cacheData{
			Start:     startTS,
			Lease:     lease,
			MemBuffer: mb,
		})
	}
	// Current status is not suitable to cache.
	return nil
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (c *cachedTable) AddRecord(sctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	txnCtxAddCachedTable(sctx, c.Meta().ID, c.handle)
	return c.TableCommon.AddRecord(sctx, r, opts...)
}

func txnCtxAddCachedTable(sctx sessionctx.Context, tid int64, handle StateRemote) {
	txnCtx := sctx.GetSessionVars().TxnCtx
	if txnCtx.CachedTables == nil {
		txnCtx.CachedTables = make(map[int64]interface{})
	}
	if _, ok := txnCtx.CachedTables[tid]; !ok {
		txnCtx.CachedTables[tid] = handle
	}
}

// UpdateRecord implements table.Table
func (c *cachedTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, oldData, newData []types.Datum, touched []bool) error {
	txnCtxAddCachedTable(sctx, c.Meta().ID, c.handle)
	return c.TableCommon.UpdateRecord(ctx, sctx, h, oldData, newData, touched)
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (c *cachedTable) RemoveRecord(sctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	txnCtxAddCachedTable(sctx, c.Meta().ID, c.handle)
	return c.TableCommon.RemoveRecord(sctx, h, r)
}

func (c *cachedTable) renewLease(ts uint64, op RenewLeaseType, data *cacheData) func() {
	return func() {
		tid := c.Meta().ID
		lease := leaseFromTS(ts)
		succ, err := c.handle.RenewLease(context.Background(), tid, lease, op)
		if err != nil {
			log.Warn("Renew read lease error", zap.Error(err))
		}
		if succ {
			c.cacheData.Store(&cacheData{
				Start:     data.Start,
				Lease:     lease,
				MemBuffer: data.MemBuffer,
			})
		}
	}
}
