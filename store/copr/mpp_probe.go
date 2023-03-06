// Copyright 2022 PingCAP, Inc.
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

package copr

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

// GlobalMPPFailedStoreProber mpp failed store probe
var GlobalMPPFailedStoreProber *MPPFailedStoreProber

const (
	// DetectPeriod detect period
	DetectPeriod = 3 * time.Second
	// DetectTimeoutLimit detect timeout
	DetectTimeoutLimit = 2 * time.Second
	// MaxRecoveryTimeLimit wait TiFlash recovery,more than MPPStoreFailTTL
	MaxRecoveryTimeLimit = 15 * time.Minute
	// MaxObsoletTimeLimit no request for a long time,that might be obsoleted
	MaxObsoletTimeLimit = time.Hour
)

// MPPStoreState the state for MPPStore.
type MPPStoreState struct {
	address    string // MPPStore TiFlash address
	tikvClient tikv.Client

	lock struct {
		sync.Mutex

		recoveryTime   time.Time
		lastLookupTime time.Time
		lastDetectTime time.Time
	}
}

// MPPFailedStoreProber use for detecting of failed TiFlash instance
type MPPFailedStoreProber struct {
	failedMPPStores *sync.Map
	lock            *sync.Mutex
	isStop          *atomic.Bool
	wg              *sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc

	detectPeriod         time.Duration
	detectTimeoutLimit   time.Duration
	maxRecoveryTimeLimit time.Duration
	maxObsoletTimeLimit  time.Duration
}

func (t *MPPStoreState) detect(ctx context.Context, detectPeriod time.Duration, detectTimeoutLimit time.Duration) {
	if time.Since(t.lock.lastDetectTime) < detectPeriod {
		return
	}

	defer func() { t.lock.lastDetectTime = time.Now() }()
	metrics.TiFlashFailedMPPStoreState.WithLabelValues(t.address).Set(0)
	ok := detectMPPStore(ctx, t.tikvClient, t.address, detectTimeoutLimit)
	if !ok {
		metrics.TiFlashFailedMPPStoreState.WithLabelValues(t.address).Set(1)
		t.lock.recoveryTime = time.Time{} // if detect failed,reset recovery time to zero.
		return
	}

	// record the time of the first recovery
	if t.lock.recoveryTime.IsZero() {
		t.lock.recoveryTime = time.Now()
	}
}

func (t *MPPStoreState) isRecovery(ctx context.Context, recoveryTTL time.Duration) bool {
	if !t.lock.TryLock() {
		return false
	}
	defer t.lock.Unlock()

	t.lock.lastLookupTime = time.Now()
	if !t.lock.recoveryTime.IsZero() && time.Since(t.lock.recoveryTime) > recoveryTTL {
		return true
	}
	logutil.Logger(ctx).Debug("Cannot detect store's availability "+
		"because the current time has not recovery or wait mppStoreFailTTL",
		zap.String("store address", t.address),
		zap.Time("recovery time", t.lock.recoveryTime),
		zap.Duration("MPPStoreFailTTL", recoveryTTL))
	return false
}

func (t MPPFailedStoreProber) scan(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Warn("mpp failed store probe scan error,will restart", zap.Any("recover", r), zap.Stack("stack"))
		}
	}()

	do := func(k, v any) {
		address := fmt.Sprint(k)
		state, ok := v.(*MPPStoreState)
		if !ok {
			logutil.BgLogger().Warn("MPPStoreState struct assert failed,will be clean",
				zap.String("address", address))
			t.Delete(address)
			return
		}

		if !state.lock.TryLock() {
			return
		}
		defer state.lock.Unlock()

		state.detect(ctx, t.detectPeriod, t.detectTimeoutLimit)

		// clean restored store
		if !state.lock.recoveryTime.IsZero() && time.Since(state.lock.recoveryTime) > t.maxRecoveryTimeLimit {
			t.Delete(address)
			// clean store that may be obsolete
		} else if state.lock.recoveryTime.IsZero() && time.Since(state.lock.lastLookupTime) > t.maxObsoletTimeLimit {
			t.Delete(address)
		}
	}

	f := func(k, v any) bool {
		go do(k, v)
		return true
	}

	metrics.TiFlashFailedMPPStoreState.WithLabelValues("probe").Set(-1) //probe heartbeat
	t.failedMPPStores.Range(f)
}

// Add add a store when sync probe failed
func (t *MPPFailedStoreProber) Add(ctx context.Context, address string, tikvClient tikv.Client) {
	state := MPPStoreState{
		address:    address,
		tikvClient: tikvClient,
	}
	state.lock.lastLookupTime = time.Now()
	logutil.Logger(ctx).Debug("add mpp store to failed list", zap.String("address", address))
	t.failedMPPStores.Store(address, &state)
}

// IsRecovery check whether the store is recovery
func (t *MPPFailedStoreProber) IsRecovery(ctx context.Context, address string, recoveryTTL time.Duration) bool {
	logutil.Logger(ctx).Debug("check failed store recovery",
		zap.String("address", address), zap.Duration("ttl", recoveryTTL))
	v, ok := t.failedMPPStores.Load(address)
	if !ok {
		// store not in failed map
		return true
	}

	state, ok := v.(*MPPStoreState)
	if !ok {
		logutil.BgLogger().Warn("MPPStoreState struct assert failed,will be clean",
			zap.String("address", address))
		t.Delete(address)
		return false
	}

	return state.isRecovery(ctx, recoveryTTL)
}

// Run a loop of scan
// there can be only one background task
func (t *MPPFailedStoreProber) Run() {
	if !t.lock.TryLock() {
		return
	}
	t.wg.Add(1)
	t.isStop.Swap(false)
	go func() {
		defer t.wg.Done()
		defer t.lock.Unlock()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-t.ctx.Done():
				logutil.BgLogger().Debug("ctx.done")
				return
			case <-ticker.C:
				t.scan(t.ctx)
			}
		}
	}()
	logutil.BgLogger().Debug("run a background probe process for mpp")
}

// Stop stop background goroutine
func (t *MPPFailedStoreProber) Stop() {
	if !t.isStop.CompareAndSwap(false, true) {
		return
	}
	t.cancel()
	t.wg.Wait()
	logutil.BgLogger().Debug("stop background task")
}

// Delete clean store from failed map
func (t *MPPFailedStoreProber) Delete(address string) {
	metrics.TiFlashFailedMPPStoreState.DeleteLabelValues(address)
	_, ok := t.failedMPPStores.LoadAndDelete(address)
	if !ok {
		logutil.BgLogger().Warn("Store is deleted", zap.String("address", address))
	}
}

// MPPStore detect function
func detectMPPStore(ctx context.Context, client tikv.Client, address string, detectTimeoutLimit time.Duration) bool {
	resp, err := client.SendRequest(ctx, address, &tikvrpc.Request{
		Type:    tikvrpc.CmdMPPAlive,
		StoreTp: tikvrpc.TiFlash,
		Req:     &mpp.IsAliveRequest{},
		Context: kvrpcpb.Context{},
	}, detectTimeoutLimit)
	if err != nil || !resp.Resp.(*mpp.IsAliveResponse).Available {
		if err == nil {
			err = fmt.Errorf("store not ready to serve")
		}
		logutil.BgLogger().Warn("Store is not ready",
			zap.String("store address", address),
			zap.String("err message", err.Error()))
		return false
	}
	return true
}

func init() {
	ctx, cancel := context.WithCancel(context.Background())
	isStop := atomic.Bool{}
	isStop.Swap(true)
	GlobalMPPFailedStoreProber = &MPPFailedStoreProber{
		failedMPPStores:      &sync.Map{},
		lock:                 &sync.Mutex{},
		isStop:               &isStop,
		ctx:                  ctx,
		cancel:               cancel,
		wg:                   &sync.WaitGroup{},
		detectPeriod:         DetectPeriod,
		detectTimeoutLimit:   DetectTimeoutLimit,
		maxRecoveryTimeLimit: MaxRecoveryTimeLimit,
		maxObsoletTimeLimit:  MaxObsoletTimeLimit,
	}
}
