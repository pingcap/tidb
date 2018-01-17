// Copyright 2017 PingCAP, Inc.
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

package tikv

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/tikv/oracle"
	goctx "golang.org/x/net/context"
)

var errStopped = errors.New("stopped")

// MockOracle is a mock oracle for test.
type MockOracle struct {
	sync.RWMutex
	stop   bool
	offset time.Duration
	lastTS uint64
}

func (o *MockOracle) enable() {
	o.Lock()
	defer o.Unlock()
	o.stop = false
}

func (o *MockOracle) disable() {
	o.Lock()
	defer o.Unlock()
	o.stop = true
}

func (o *MockOracle) setOffset(offset time.Duration) {
	o.Lock()
	defer o.Unlock()

	o.offset = offset
}

// AddOffset adds the offset of the oracle.
func (o *MockOracle) AddOffset(d time.Duration) {
	o.Lock()
	defer o.Unlock()

	o.offset += d
}

// GetTimestamp implements oracle.Oracle interface.
func (o *MockOracle) GetTimestamp(goctx.Context) (uint64, error) {
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
	ctx goctx.Context
}

func (m *mockOracleFuture) Wait() (uint64, error) {
	return m.o.GetTimestamp(m.ctx)
}

// GetTimestampAsync implements oracle.Oracle interface.
func (o *MockOracle) GetTimestampAsync(ctx goctx.Context) oracle.Future {
	return &mockOracleFuture{o, ctx}
}

// IsExpired implements oracle.Oracle interface.
func (o *MockOracle) IsExpired(lockTimestamp uint64, TTL uint64) bool {
	o.RLock()
	defer o.RUnlock()

	return oracle.GetPhysical(time.Now().Add(o.offset)) >= oracle.ExtractPhysical(lockTimestamp)+int64(TTL)
}

// Close implements oracle.Oracle interface.
func (o *MockOracle) Close() {

}

// NewTestTiKVStorage creates a Storage for test.
func NewTestTiKVStorage(withTiKV bool, pdAddrs string) (Storage, error) {
	if !flag.Parsed() {
		flag.Parse()
	}

	if withTiKV {
		var d Driver
		store, err := d.Open(fmt.Sprintf("tikv://%s", pdAddrs))
		if err != nil {
			return nil, errors.Trace(err)
		}
		return store.(Storage), nil
	}
	store, err := NewMockTikvStore()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return store.(Storage), nil
}
