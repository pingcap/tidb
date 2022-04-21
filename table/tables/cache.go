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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	_ table.CachedTable = &cachedTable{}
)

type cachedTable struct {
	TableCommon
	cacheData atomic.Value
	totalSize int64
	// StateRemote is not thread-safe, this tokenLimit is used to keep only one visitor.
	tokenLimit
}

type tokenLimit chan StateRemote

func (t tokenLimit) TakeStateRemoteHandle() StateRemote {
	handle := <-t
	return handle
}

func (t tokenLimit) TakeStateRemoteHandleNoWait() StateRemote {
	select {
	case handle := <-t:
		return handle
	default:
		return nil
	}
}

func (t tokenLimit) PutStateRemoteHandle(handle StateRemote) {
	t <- handle
}

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

func (c *cachedTable) TryReadFromCache(ts uint64, leaseDuration time.Duration) (kv.MemBuffer, bool /*loading*/) {
	tmp := c.cacheData.Load()
	if tmp == nil {
		return nil, false
	}
	data := tmp.(*cacheData)
	if ts >= data.Start && ts < data.Lease {
		leaseTime := oracle.GetTimeFromTS(data.Lease)
		nowTime := oracle.GetTimeFromTS(ts)
		distance := leaseTime.Sub(nowTime)

		var triggerFailpoint bool
		failpoint.Inject("mockRenewLeaseABA1", func(_ failpoint.Value) {
			triggerFailpoint = true
		})

		if distance >= 0 && distance <= leaseDuration/2 || triggerFailpoint {
			if h := c.TakeStateRemoteHandleNoWait(); h != nil {
				go c.renewLease(h, ts, data, leaseDuration)
			}
		}
		// If data is not nil, but data.MemBuffer is nil, it means the data is being
		// loading by a background goroutine.
		return data.MemBuffer, data.MemBuffer == nil
	}
	return nil, false
}

// newCachedTable creates a new CachedTable Instance
func newCachedTable(tbl *TableCommon) (table.Table, error) {
	ret := &cachedTable{
		TableCommon: *tbl,
		tokenLimit:  make(chan StateRemote, 1),
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
	handle := NewStateRemote(raw)
	c.PutStateRemoteHandle(handle)
	return nil
}

func (c *cachedTable) loadDataFromOriginalTable(store kv.Storage) (kv.MemBuffer, uint64, int64, error) {
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
	if h := c.TakeStateRemoteHandle(); h != nil {
		go c.updateLockForRead(ctx, h, store, ts, leaseDuration)
	}
}

func (c *cachedTable) updateLockForRead(ctx context.Context, handle StateRemote, store kv.Storage, ts uint64, leaseDuration time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
		c.PutStateRemoteHandle(handle)
	}()

	// Load data from original table and the update lock information.
	tid := c.Meta().ID
	lease := leaseFromTS(ts, leaseDuration)
	succ, err := handle.LockForRead(ctx, tid, lease)
	if err != nil {
		log.Warn("lock cached table for read", zap.Error(err))
		return
	}
	if succ {
		c.cacheData.Store(&cacheData{
			Start:     ts,
			Lease:     lease,
			MemBuffer: nil, // Async loading, this will be set later.
		})

		// Make the load data process async, in case that loading data takes longer the
		// lease duration, then the loaded data get staled and that process repeats forever.
		go func() {
			start := time.Now()
			mb, startTS, totalSize, err := c.loadDataFromOriginalTable(store)
			metrics.LoadTableCacheDurationHistogram.Observe(time.Since(start).Seconds())
			if err != nil {
				log.Info("load data from table fail", zap.Error(err))
				return
			}

			tmp := c.cacheData.Load().(*cacheData)
			if tmp != nil && tmp.Start == ts {
				c.cacheData.Store(&cacheData{
					Start:     startTS,
					Lease:     tmp.Lease,
					MemBuffer: mb,
				})
				atomic.StoreInt64(&c.totalSize, totalSize)
			}
		}()
	}
	// Current status is not suitable to cache.
}

const cachedTableSizeLimit = 64 * (1 << 20)

// AddRecord implements the AddRecord method for the table.Table interface.
func (c *cachedTable) AddRecord(sctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	if atomic.LoadInt64(&c.totalSize) > cachedTableSizeLimit {
		return nil, table.ErrOptOnCacheTable.GenWithStackByArgs("table too large")
	}
	txnCtxAddCachedTable(sctx, c.Meta().ID, c)
	return c.TableCommon.AddRecord(sctx, r, opts...)
}

func txnCtxAddCachedTable(sctx sessionctx.Context, tid int64, handle *cachedTable) {
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
	txnCtxAddCachedTable(sctx, c.Meta().ID, c)
	return c.TableCommon.UpdateRecord(ctx, sctx, h, oldData, newData, touched)
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (c *cachedTable) RemoveRecord(sctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	txnCtxAddCachedTable(sctx, c.Meta().ID, c)
	return c.TableCommon.RemoveRecord(sctx, h, r)
}

// TestMockRenewLeaseABA2 is used by test function TestRenewLeaseABAFailPoint.
var TestMockRenewLeaseABA2 chan struct{}

func (c *cachedTable) renewLease(handle StateRemote, ts uint64, data *cacheData, leaseDuration time.Duration) {
	failpoint.Inject("mockRenewLeaseABA2", func(_ failpoint.Value) {
		c.PutStateRemoteHandle(handle)
		<-TestMockRenewLeaseABA2
		c.TakeStateRemoteHandle()
	})

	defer c.PutStateRemoteHandle(handle)

	tid := c.Meta().ID
	lease := leaseFromTS(ts, leaseDuration)
	newLease, err := handle.RenewReadLease(context.Background(), tid, data.Lease, lease)
	if err != nil && !kv.IsTxnRetryableError(err) {
		log.Warn("Renew read lease error", zap.Error(err))
	}
	if newLease > 0 {
		c.cacheData.Store(&cacheData{
			Start:     data.Start,
			Lease:     newLease,
			MemBuffer: data.MemBuffer,
		})
	}

	failpoint.Inject("mockRenewLeaseABA2", func(_ failpoint.Value) {
		TestMockRenewLeaseABA2 <- struct{}{}
	})
}

const cacheTableWriteLease = 5 * time.Second

func (c *cachedTable) WriteLockAndKeepAlive(ctx context.Context, exit chan struct{}, leasePtr *uint64, wg chan error) {
	writeLockLease, err := c.lockForWrite(ctx)
	atomic.StoreUint64(leasePtr, writeLockLease)
	wg <- err
	if err != nil {
		logutil.Logger(ctx).Warn("[cached table] lock for write lock fail", zap.Error(err))
		return
	}

	t := time.NewTicker(cacheTableWriteLease)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := c.renew(ctx, leasePtr); err != nil {
				logutil.Logger(ctx).Warn("[cached table] renew write lock lease fail", zap.Error(err))
				return
			}
		case <-exit:
			return
		}
	}
}

func (c *cachedTable) renew(ctx context.Context, leasePtr *uint64) error {
	oldLease := atomic.LoadUint64(leasePtr)
	physicalTime := oracle.GetTimeFromTS(oldLease)
	newLease := oracle.GoTimeToTS(physicalTime.Add(cacheTableWriteLease))

	h := c.TakeStateRemoteHandle()
	defer c.PutStateRemoteHandle(h)

	succ, err := h.RenewWriteLease(ctx, c.Meta().ID, newLease)
	if err != nil {
		return errors.Trace(err)
	}
	if succ {
		atomic.StoreUint64(leasePtr, newLease)
	}
	return nil
}

func (c *cachedTable) lockForWrite(ctx context.Context) (uint64, error) {
	handle := c.TakeStateRemoteHandle()
	defer c.PutStateRemoteHandle(handle)

	return handle.LockForWrite(ctx, c.Meta().ID, cacheTableWriteLease)
}
