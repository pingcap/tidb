// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package oracles

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var _ oracle.Oracle = &pdOracle{}

const slowDist = 30 * time.Millisecond

// pdOracle is an Oracle that uses a placement driver client as source.
type pdOracle struct {
	c pd.Client
	// txn_scope (string) -> lastTSPointer (*uint64)
	lastTSMap            sync.Map
	lastArrivalTSPointer *uint64
	quit                 chan struct{}
}

// NewPdOracle create an Oracle that uses a pd client source.
// Refer https://github.com/tikv/pd/blob/master/client/client.go for more details.
// PdOracle mantains `lastTS` to store the last timestamp got from PD server. If
// `GetTimestamp()` is not called after `updateInterval`, it will be called by
// itself to keep up with the timestamp on PD server.
func NewPdOracle(pdClient pd.Client, updateInterval time.Duration) (oracle.Oracle, error) {
	o := &pdOracle{
		c:    pdClient,
		quit: make(chan struct{}),
	}
	ctx := context.TODO()
	go o.updateTS(ctx, updateInterval)
	// Initialize the timestamp of the global txnScope by Get.
	_, err := o.GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	if err != nil {
		o.Close()
		return nil, errors.Trace(err)
	}
	return o, nil
}

// IsExpired returns whether lockTS+TTL is expired, both are ms. It uses `lastTS`
// to compare, may return false negative result temporarily.
func (o *pdOracle) IsExpired(lockTS, TTL uint64, opt *oracle.Option) bool {
	lastTS, exist := o.getLastTS(opt.TxnScope)
	if !exist {
		return true
	}
	return oracle.ExtractPhysical(lastTS) >= oracle.ExtractPhysical(lockTS)+int64(TTL)
}

// GetTimestamp gets a new increasing time.
func (o *pdOracle) GetTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	ts, err := o.getTimestamp(ctx, opt.TxnScope)
	if err != nil {
		return 0, errors.Trace(err)
	}
	tsArrival := o.getArrivalTimestamp(ctx)
	o.setLastTS(ts, opt.TxnScope)
	o.setLastArrivalTS(tsArrival)
	return ts, nil
}

type tsFuture struct {
	pd.TSFuture
	o        *pdOracle
	txnScope string
}

// Wait implements the oracle.Future interface.
func (f *tsFuture) Wait() (uint64, error) {
	now := time.Now()
	physical, logical, err := f.TSFuture.Wait()
	metrics.TSFutureWaitDuration.Observe(time.Since(now).Seconds())
	if err != nil {
		return 0, errors.Trace(err)
	}
	ts := oracle.ComposeTS(physical, logical)
	f.o.setLastTS(ts, f.txnScope)
	return ts, nil
}

func (o *pdOracle) GetTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	var ts pd.TSFuture
	if opt.TxnScope == oracle.GlobalTxnScope || opt.TxnScope == "" {
		ts = o.c.GetTSAsync(ctx)
	} else {
		ts = o.c.GetLocalTSAsync(ctx, opt.TxnScope)
	}
	return &tsFuture{ts, o, opt.TxnScope}
}

func (o *pdOracle) getTimestamp(ctx context.Context, txnScope string) (uint64, error) {
	now := time.Now()
	var (
		physical, logical int64
		err               error
	)
	if txnScope == oracle.GlobalTxnScope || txnScope == "" {
		physical, logical, err = o.c.GetTS(ctx)
	} else {
		physical, logical, err = o.c.GetLocalTS(ctx, txnScope)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		logutil.Logger(ctx).Warn("get timestamp too slow",
			zap.Duration("cost time", dist))
	}
	return oracle.ComposeTS(physical, logical), nil
}

func (o *pdOracle) getArrivalTimestamp(ctx context.Context) uint64 {
	return oracle.ComposeTS(oracle.GetPhysical(time.Now()), 0)
}

func (o *pdOracle) setLastTS(ts uint64, txnScope string) {
	if txnScope == "" {
		txnScope = oracle.GlobalTxnScope
	}
	lastTSInterface, ok := o.lastTSMap.Load(txnScope)
	if !ok {
		lastTSInterface, _ = o.lastTSMap.LoadOrStore(txnScope, new(uint64))
	}
	lastTSPointer := lastTSInterface.(*uint64)
	for {
		lastTS := atomic.LoadUint64(lastTSPointer)
		if ts <= lastTS {
			return
		}
		if atomic.CompareAndSwapUint64(lastTSPointer, lastTS, ts) {
			return
		}
	}
}

func (o *pdOracle) getLastTS(txnScope string) (uint64, bool) {
	if txnScope == "" {
		txnScope = oracle.GlobalTxnScope
	}
	lastTSInterface, ok := o.lastTSMap.Load(txnScope)
	if !ok {
		return 0, false
	}
	return atomic.LoadUint64(lastTSInterface.(*uint64)), true
}

func (o *pdOracle) updateTS(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Update the timestamp for each txnScope
			o.lastTSMap.Range(func(key, _ interface{}) bool {
				txnScope := key.(string)
				ts, err := o.getTimestamp(ctx, txnScope)
				if err != nil {
					logutil.Logger(ctx).Error("updateTS error", zap.String("txnScope", txnScope), zap.Error(err))
					return true
				}
				o.setLastTS(ts, txnScope)
				return true
			})
		case <-o.quit:
			return
		}
	}
}

// UntilExpired implement oracle.Oracle interface.
func (o *pdOracle) UntilExpired(lockTS uint64, TTL uint64, opt *oracle.Option) int64 {
	lastTS, ok := o.getLastTS(opt.TxnScope)
	if !ok {
		return 0
	}
	return oracle.ExtractPhysical(lockTS) + int64(TTL) - oracle.ExtractPhysical(lastTS)
}

func (o *pdOracle) Close() {
	close(o.quit)
}

// A future that resolves immediately to a low resolution timestamp.
type lowResolutionTsFuture struct {
	ts  uint64
	err error
}

// Wait implements the oracle.Future interface.
func (f lowResolutionTsFuture) Wait() (uint64, error) {
	return f.ts, f.err
}

// GetLowResolutionTimestamp gets a new increasing time.
func (o *pdOracle) GetLowResolutionTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	lastTS, ok := o.getLastTS(opt.TxnScope)
	if !ok {
		return 0, errors.Errorf("get low resolution timestamp fail, invalid txnScope = %s", opt.TxnScope)
	}
	return lastTS, nil
}

func (o *pdOracle) GetLowResolutionTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	lastTS, ok := o.getLastTS(opt.TxnScope)
	if !ok {
		return lowResolutionTsFuture{
			ts:  0,
			err: errors.Errorf("get low resolution timestamp async fail, invalid txnScope = %s", opt.TxnScope),
		}
	}
	return lowResolutionTsFuture{
		ts:  lastTS,
		err: nil,
	}
}

func (o *pdOracle) getStaleTimestamp(ctx context.Context, prevSecond int64) (uint64, error) {
	ts, ok := o.getLastTS(oracle.GlobalTxnScope)
	if !ok {
		return 0, errors.Errorf("get stale timestamp, invalid txnScope = %s", oracle.GlobalTxnScope)
	}
	tsArrival, ok := o.getLastArrivalTS()
	if !ok {
		return 0, errors.Errorf("get last arrival timestamp, invalid txnScope = %s", oracle.GlobalTxnScope)
	}
	arrivalTime := oracle.GetTimeFromTS(tsArrival)
	physicalTime := oracle.GetTimeFromTS(ts)
	if physicalTime.Unix() <= prevSecond {
		return 0, errors.Errorf("get invalid prevSecond, prevSecond must less than physicalTime, "+
			"get prevSecond: %d, physicalTime: %d", prevSecond, physicalTime.Unix())
	}

	staleTime := physicalTime.Add(-arrivalTime.Sub(time.Now().Add(-time.Duration(prevSecond) * time.Second)))

	return oracle.ComposeTS(oracle.GetPhysical(staleTime), 0), nil
}

// GetStaleTimestamp generate a TSO which represents for the TSO prevSecond secs ago.
func (o *pdOracle) GetStaleTimestamp(ctx context.Context, prevSecond int64) (ts uint64, err error) {
	ts, err = o.getStaleTimestamp(ctx, prevSecond)
	if err != nil {
		return 0, errors.Wrap(err, "")
	}
	return ts, nil
}

func (o *pdOracle) setLastArrivalTS(ts uint64) {
	if o.lastArrivalTSPointer == nil {
		o.lastArrivalTSPointer = new(uint64)
	}
	atomic.StoreUint64(o.lastArrivalTSPointer, ts)
}

func (o *pdOracle) getLastArrivalTS() (uint64, bool) {
	if o.lastArrivalTSPointer == nil {
		return 0, false
	}
	return atomic.LoadUint64(o.lastArrivalTSPointer), true
}
