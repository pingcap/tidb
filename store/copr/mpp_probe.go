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
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

var globalMPPFailedStoreProbe *MPPFailedStoreProbe

const (
	DetectTimeoutLimit   = 2 * time.Second
	DetectPeriod         = 3 * time.Second
	MaxRecoveryTimeLimit = 15 * time.Minute // wait TiFlash recovery,more than MPPStoreFailTTL
	MaxObsoletTimeLimit  = 24 * time.Hour   // no request for a long time,that might be obsoleted
)

// MPPSotreState the state for MPPStore.
type MPPSotreState struct {
	address string // MPPStore TiFlash address
	store   *kvStore

	recoveryTime   time.Time
	lastLookupTime time.Time
	lastDetectTime time.Time
}

// MPPFailedStoreProbe use for detecting of failed TiFlash instance
type MPPFailedStoreProbe struct {
	failedMPPStores *sync.Map
}

func (t *MPPSotreState) detect(ctx context.Context) {
	if time.Since(t.lastDetectTime) > DetectPeriod {
		return
	}

	defer func() { t.lastDetectTime = time.Now() }()

	err := detectMPPStore(ctx, t.store.GetTiKVClient(), t.address)
	if err != nil {
		t.recoveryTime = time.Time{} // if detect failed,reset recovery time to zero.
		return
	}

	// record the time of the first recovery
	if t.recoveryTime.IsZero() {
		t.recoveryTime = time.Now()
	}

}

func (t *MPPSotreState) isRecovery(ctx context.Context, recoveryTTL time.Duration) bool {
	t.lastLookupTime = time.Now()
	if !t.recoveryTime.IsZero() && time.Since(t.recoveryTime) > recoveryTTL {
		logutil.Logger(ctx).Debug("Cannot detect store's availability"+
			"because the current time has not reached recoveryTime + mppStoreFailTTL",
			zap.String("store address", t.address),
			zap.Time("recovery time", t.recoveryTime),
			zap.Duration("MPPStoreFailTTL", recoveryTTL))
		return true
	}
	return false
}

func (t MPPFailedStoreProbe) scan(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Warn("mpp failed store probe scan error", zap.Any("recover", r), zap.Stack("stack"))
		}
	}()

	do := func(k, v any) {
		address := fmt.Sprintln(k)
		state, ok := v.(MPPSotreState)
		if !ok {
			logutil.BgLogger().Warn("MPPSotreState struct Deserialization exception",
				zap.String("address", address),
				zap.Any("state", v))
			return
		}

		state.detect(ctx)

		// clean restored store
		if !state.recoveryTime.IsZero() && time.Since(state.recoveryTime) > MaxRecoveryTimeLimit {
			t.failedMPPStores.Delete(address)
			// clean store that may be obsolete
		} else if !state.recoveryTime.IsZero() && time.Since(state.lastLookupTime) > MaxObsoletTimeLimit {
			t.failedMPPStores.Delete(address)
		}
	}

	f := func(k, v any) bool {
		go do(k, v)
		return true
	}

	t.failedMPPStores.Range(f)
}

// Add add a store when sync probe failed
func (t MPPFailedStoreProbe) Add(ctx context.Context, address string, store *kvStore) {
	state := MPPSotreState{
		address: address,
		store:   store,
	}
	v, ok := t.failedMPPStores.LoadOrStore(address, state)
	if !ok {
		logutil.Logger(ctx).Debug("failed store repeat add", zap.String("address", address), zap.Any("state", v))
	}

}

// IsRecovery check whether the store is recovery
func (t MPPFailedStoreProbe) IsRecovery(ctx context.Context, address string, recoveryTTL time.Duration) bool {
	v, ok := t.failedMPPStores.Load(address)
	if !ok {
		// store not failed map
		return true
	}

	state, ok := v.(MPPSotreState)
	if !ok {
		logutil.Logger(ctx).Warn("MPPSotreState struct Deserialization exception",
			zap.String("address", address),
			zap.Any("state", v))
		return false
	}
	return state.isRecovery(ctx, recoveryTTL)
}

// Run a loop of scan
// there can be only one background task
func (t *MPPFailedStoreProbe) Run() {
	for {
		t.scan(context.Background())
		time.Sleep(DetectPeriod)
	}
}

// MPPStore detect function
func detectMPPStore(ctx context.Context, client tikv.Client, address string) error {
	resp, err := client.SendRequest(ctx, address, &tikvrpc.Request{
		Type:    tikvrpc.CmdMPPAlive,
		StoreTp: tikvrpc.TiFlash,
		Req:     &mpp.IsAliveRequest{},
		Context: kvrpcpb.Context{},
	}, DetectTimeoutLimit)
	if err != nil || !resp.Resp.(*mpp.IsAliveResponse).Available {
		if err == nil {
			err = fmt.Errorf("store not ready to serve")
		}
		logutil.BgLogger().Warn("Store is not ready",
			zap.String("store address", address),
			zap.String("err message", err.Error()))
		return err
	}
	return nil
}

func init() {
	globalMPPFailedStoreProbe = &MPPFailedStoreProbe{
		failedMPPStores: &sync.Map{},
	}
}
