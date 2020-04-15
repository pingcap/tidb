// Copyright 2018 PingCAP, Inc.
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

package mockoracle

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/v4/store/tikv/oracle"
)

var errStopped = errors.New("stopped")

// MockOracle is a mock oracle for test.
type MockOracle struct {
	sync.RWMutex
	stop   bool
	offset time.Duration
	lastTS uint64
}

// Enable enables the Oracle
func (o *MockOracle) Enable() {
	o.Lock()
	defer o.Unlock()
	o.stop = false
}

// Disable disables the Oracle
func (o *MockOracle) Disable() {
	o.Lock()
	defer o.Unlock()
	o.stop = true
}

// AddOffset adds the offset of the oracle.
func (o *MockOracle) AddOffset(d time.Duration) {
	o.Lock()
	defer o.Unlock()

	o.offset += d
}

// GetTimestamp implements oracle.Oracle interface.
func (o *MockOracle) GetTimestamp(context.Context) (uint64, error) {
	o.Lock()
	defer o.Unlock()

	if o.stop {
		return 0, errors.Trace(errStopped)
	}
	physical := oracle.GetPhysical(time.Now().Add(o.offset))
	ts := oracle.ComposeTS(physical, 0)
	if oracle.ExtractPhysical(o.lastTS) == physical {
		ts = o.lastTS + 1
	}
	o.lastTS = ts
	return ts, nil
}

type mockOracleFuture struct {
	o   *MockOracle
	ctx context.Context
}

func (m *mockOracleFuture) Wait() (uint64, error) {
	return m.o.GetTimestamp(m.ctx)
}

// GetTimestampAsync implements oracle.Oracle interface.
func (o *MockOracle) GetTimestampAsync(ctx context.Context) oracle.Future {
	return &mockOracleFuture{o, ctx}
}

// GetLowResolutionTimestamp implements oracle.Oracle interface.
func (o *MockOracle) GetLowResolutionTimestamp(ctx context.Context) (uint64, error) {
	return o.GetTimestamp(ctx)
}

// GetLowResolutionTimestampAsync implements oracle.Oracle interface.
func (o *MockOracle) GetLowResolutionTimestampAsync(ctx context.Context) oracle.Future {
	return o.GetTimestampAsync(ctx)
}

// IsExpired implements oracle.Oracle interface.
func (o *MockOracle) IsExpired(lockTimestamp uint64, TTL uint64) bool {
	o.RLock()
	defer o.RUnlock()

	return oracle.GetPhysical(time.Now().Add(o.offset)) >= oracle.ExtractPhysical(lockTimestamp)+int64(TTL)
}

// UntilExpired implement oracle.Oracle interface.
func (o *MockOracle) UntilExpired(lockTimeStamp uint64, TTL uint64) int64 {
	o.RLock()
	defer o.RUnlock()
	return oracle.ExtractPhysical(lockTimeStamp) + int64(TTL) - oracle.GetPhysical(time.Now().Add(o.offset))
}

// Close implements oracle.Oracle interface.
func (o *MockOracle) Close() {

}
