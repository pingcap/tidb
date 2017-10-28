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

// Storage represent the kv.Storage runs on TiKV.
type Storage interface {
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

	// SetTiKVClient sets the TiKV client.
	SetTiKVClient(client Client)

	// GetTiKVClient gets the TiKV client.
	GetTiKVClient() Client

	// Closed returns the closed channel.
	Closed() <-chan struct{}
}

// GCHandler runs garbage collection job.
type GCHandler interface {
	// Start starts the GCHandler, if enableGC is false, it does not do GC job.
	Start(enableGC bool)

	// Close closes the GCHandler.
	Close()
}

// NewGCHandlerFunc creates a new GCHandler, the default implementation only updates safe point cache time.
// To enable real GC, we should assign the function to `gcworker.NewGCWorker`.
var NewGCHandlerFunc = func(storage Storage) (GCHandler, error) {
	return &noGCHandler{
		store:   storage,
		closeCh: make(chan bool),
	}, nil
}

type noGCHandler struct {
	store   Storage
	closeCh chan bool
}

func (h *noGCHandler) Start(enableGC bool) {
	/// Regularly update safe point cache time to avoid tiemstamp fall behind GC safe point error.
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				h.store.UpdateSPCache(0, time.Now())
			case <-h.closeCh:
				return
			}
		}
	}()
}

func (h *noGCHandler) Close() {
	close(h.closeCh)
}
