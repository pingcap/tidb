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
	totalSize int64

	lockingForRead tokenLimit
	renewReadLease tokenLimit
}

type tokenLimit = chan struct{}

// cacheData pack the cache data and lease.
type cacheData struct {
	Start uint64
	Lease uint64
	kv.MemBuffer
}

func leaseFromTS(ts uint64, leaseDuration time.Duration) uint64 {
	physicalTime := oracle.GetTimeFromTS(ts)
	lease := oracle.GoTimeToTS(physicalTime.Add(leaseDuration))
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

func (c *cachedTable) TryReadFromCache(ts uint64, leaseDuration time.Duration) kv.MemBuffer {
	tmp := c.cacheData.Load()
	if tmp == nil {
		return nil
	}
	data := tmp.(*cacheData)
	if ts >= data.Start && ts < data.Lease {
		leaseTime := oracle.GetTimeFromTS(data.Lease)
		nowTime := oracle.GetTimeFromTS(ts)
		distance := leaseTime.Sub(nowTime)
		if distance >= 0 && distance <= leaseDuration/2 {
			select {
			case c.renewReadLease <- struct{}{}:
				go c.renewLease(ts, data, leaseDuration)
			default:
			}
		}
		return data.MemBuffer
	}
	return nil
}

// newCachedTable creates a new CachedTable Instance
func newCachedTable(tbl *TableCommon) (table.Table, error) {
	ret := &cachedTable{
		TableCommon:    *tbl,
		lockingForRead: make(chan struct{}, 1),
		renewReadLease: make(chan struct{}, 1),
	}
	return ret, nil
}

// Init is an extra operation for cachedTable after TableFromMeta,
// Because cachedTable need some additional parameter that can't be passed in TableFromMeta.
func (c *cachedTable) Init(exec sqlexec.SQLExecutor) error {
	raw, ok := exec.(sqlExec)
	if !ok {
		return errors.New("Need sqlExec rather than sqlexec.SQLExecutor")
	}
	raw.ExecuteInternal(context.Background(), "set @@session.tidb_retry_limit = 0")
	c.handle = NewStateRemote(raw)
	return nil
}

func (c *cachedTable) loadDataFromOriginalTable(store kv.Storage, lease uint64) (kv.MemBuffer, uint64, int64, error) {
	buffer, err := newMemBuffer(store)
	if err != nil {
		return nil, 0, 0, err
	}
	var startTS uint64
	totalSize := int64(0)
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
			key := it.Key()
			value := it.Value()
			err = buffer.Set(key, value)
			if err != nil {
				return errors.Trace(err)
			}
			totalSize += int64(len(key))
			totalSize += int64(len(value))
			err = it.Next()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, totalSize, err
	}

	return buffer, startTS, totalSize, nil
}

func (c *cachedTable) UpdateLockForRead(ctx context.Context, store kv.Storage, ts uint64, leaseDuration time.Duration) {
	select {
	case c.lockingForRead <- struct{}{}:
		go c.updateLockForRead(ctx, store, ts, leaseDuration)
	default:
		// There is a inflight calling already.
	}
}

func (c *cachedTable) updateLockForRead(ctx context.Context, store kv.Storage, ts uint64, leaseDuration time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
		<-c.lockingForRead
	}()

	// Load data from original table and the update lock information.
	tid := c.Meta().ID
	lease := leaseFromTS(ts, leaseDuration)
	succ, err := c.handle.LockForRead(ctx, tid, lease)
	if err != nil {
		log.Warn("lock cached table for read", zap.Error(err))
		return
	}
	if succ {
		mb, startTS, totalSize, err := c.loadDataFromOriginalTable(store, lease)
		if err != nil {
			return
		}

		c.cacheData.Store(&cacheData{
			Start:     startTS,
			Lease:     lease,
			MemBuffer: mb,
		})
		atomic.StoreInt64(&c.totalSize, totalSize)
	}
	// Current status is not suitable to cache.
}

const cachedTableSizeLimit = 64 * (1 << 20)

// AddRecord implements the AddRecord method for the table.Table interface.
func (c *cachedTable) AddRecord(sctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	if atomic.LoadInt64(&c.totalSize) > cachedTableSizeLimit {
		return nil, table.ErrOptOnCacheTable.GenWithStackByArgs("table too large")
	}
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
	// Prevent furthur writing when the table is already too large.
	if atomic.LoadInt64(&c.totalSize) > cachedTableSizeLimit {
		return table.ErrOptOnCacheTable.GenWithStackByArgs("table too large")
	}
	txnCtxAddCachedTable(sctx, c.Meta().ID, c.handle)
	return c.TableCommon.UpdateRecord(ctx, sctx, h, oldData, newData, touched)
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (c *cachedTable) RemoveRecord(sctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	txnCtxAddCachedTable(sctx, c.Meta().ID, c.handle)
	return c.TableCommon.RemoveRecord(sctx, h, r)
}

func (c *cachedTable) renewLease(ts uint64, data *cacheData, leaseDuration time.Duration) {
	defer func() { <-c.renewReadLease }()

	tid := c.Meta().ID
	lease := leaseFromTS(ts, leaseDuration)
	succ, err := c.handle.RenewLease(context.Background(), tid, lease, RenewReadLease)
	if err != nil && !kv.IsTxnRetryableError(err) {
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
