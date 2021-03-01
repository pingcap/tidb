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
	"context"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// Storage represent the kv.Storage runs on TiKV.
type Storage interface {
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

	// SetOracle sets the Oracle.
	SetOracle(oracle oracle.Oracle)

	// SetTiKVClient sets the TiKV client.
	SetTiKVClient(client Client)

	// GetTiKVClient gets the TiKV client.
	GetTiKVClient() Client

	// Closed returns the closed channel.
	Closed() <-chan struct{}

	// Begin a global transaction
	Begin() (kv.Transaction, error)
	// Begin a transaction with the given txnScope (local or global)
	BeginWithTxnScope(txnScope string) (kv.Transaction, error)
	// BeginWithStartTS begins transaction with given txnScope and startTS.
	BeginWithStartTS(txnScope string, startTS uint64) (kv.Transaction, error)
	// BeginWithStalenessTS begins transaction with given staleness
	BeginWithExactStaleness(txnScope string, prevSec uint64) (kv.Transaction, error)
	// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
	// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
	GetSnapshot(ver kv.Version) kv.Snapshot
	// Close store
	Close() error
	// UUID return a unique ID which represents a Storage.
	UUID() string
	// CurrentVersion returns current max committed version with the given txnScope (local or global).
	CurrentVersion(txnScope string) (kv.Version, error)
	// GetOracle gets a timestamp oracle client.
	GetOracle() oracle.Oracle
	// SupportDeleteRange gets the storage support delete range or not.
	SupportDeleteRange() (supported bool)
	// ShowStatus returns the specified status of the storage
	ShowStatus(ctx context.Context, key string) (interface{}, error)
}
