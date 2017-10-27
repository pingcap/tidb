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
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// TiKVStorage represent the kv.Storage runs on TiKV.
type TiKVStorage interface {
	kv.Storage

	// GetRegionCache gets the RegionCache.
	GetRegionCache() *RegionCache

	// SendReq sends a request to TiKV.
	SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)

	// GetLockResolver gets the LockResolver.
	GetLockResolver() *LockResolver

	// GetSafePointKV gets the SafePointKV.
	GetSafePointKV() SafePointKV

	// UpdateSPCache updates the cache of safe point.
	UpdateSPCache(cachedSP uint64, cachedTime time.Time)

	// GetGCHandler gets the GCHandler.
	GetGCHandler() GCHandler

	// SetOracle sets the Oracle.
	SetOracle(oracle oracle.Oracle)

	// Closed returns the closed channel.
	Closed() <-chan struct{}
}

// GCHandler runs garbage collection job.
type GCHandler interface {
	// Start starts the GCHandler, if enableGC is false, it does not do GC job.
	Start(enableGC bool)

	// Close closes the GCHandler
	Close()
}

// NewGCHandlerFunc creates a new GCHandler.
var NewGCHandlerFunc = func(storage TiKVStorage) (GCHandler, error) {
	return new(mockGCHandler), nil
}

type mockGCHandler struct{}

func (h *mockGCHandler) Start(enableGC bool) {}

func (h *mockGCHandler) Close() {}
