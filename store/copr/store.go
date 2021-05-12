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
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type kvStore struct {
	store *tikv.KVStore
}

// GetRegionCache returns the region cache instance.
func (s *kvStore) GetRegionCache() *tikv.RegionCache {
	return s.store.GetRegionCache()
}

// CheckVisibility checks if it is safe to read using given ts.
func (s *kvStore) CheckVisibility(startTime uint64) error {
	err := s.store.CheckVisibility(startTime)
	return derr.ToTiDBErr(err)
}

// GetTiKVClient gets the client instance.
func (s *kvStore) GetTiKVClient() tikv.Client {
	client := s.store.GetTiKVClient()
	return &tikvClient{c: client}
}

type tikvClient struct {
	c tikv.Client
}

func (c *tikvClient) Close() error {
	err := c.c.Close()
	return derr.ToTiDBErr(err)
}

// SendRequest sends Request.
func (c *tikvClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	res, err := c.c.SendRequest(ctx, addr, req, timeout)
	return res, derr.ToTiDBErr(err)
}

// Store wraps tikv.KVStore and provides coprocessor utilities.
type Store struct {
	*kvStore
	coprCache       *coprCache
	replicaReadSeed uint32
}

// NewStore creates a new store instance.
func NewStore(s *tikv.KVStore, coprCacheConfig *config.CoprocessorCache) (*Store, error) {
	coprCache, err := newCoprCache(coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Store{
		kvStore:         &kvStore{store: s},
		coprCache:       coprCache,
		replicaReadSeed: rand.Uint32(),
	}, nil
}

// Close releases resources allocated for coprocessor.
func (s *Store) Close() {
	if s.coprCache != nil {
		s.coprCache.cache.Close()
	}
}

func (s *Store) nextReplicaReadSeed() uint32 {
	return atomic.AddUint32(&s.replicaReadSeed, 1)
}

// GetClient gets a client instance.
func (s *Store) GetClient() kv.Client {
	return &CopClient{
		store:           s,
		replicaReadSeed: s.nextReplicaReadSeed(),
	}
}

// GetMPPClient gets a mpp client instance.
func (s *Store) GetMPPClient() kv.MPPClient {
	return &MPPClient{
		store: s.kvStore,
	}
}

func getEndPointType(t kv.StoreType) tikvrpc.EndpointType {
	switch t {
	case kv.TiKV:
		return tikvrpc.TiKV
	case kv.TiFlash:
		return tikvrpc.TiFlash
	case kv.TiDB:
		return tikvrpc.TiDB
	default:
		return tikvrpc.TiKV
	}
}

// backoffer wraps tikv.Backoffer and converts the error which returns by the functions of tikv.Backoffer to tidb error.
type backoffer struct {
	b *tikv.Backoffer
}

// newBackofferWithVars creates a Backoffer with maximum sleep time(in ms) and kv.Variables.
func newBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *backoffer {
	b := tikv.NewBackofferWithVars(ctx, maxSleep, vars)
	return &backoffer{b: b}
}

func newBackoffer(ctx context.Context, maxSleep int) *backoffer {
	b := tikv.NewBackoffer(ctx, maxSleep)
	return &backoffer{b: b}
}

// TiKVBackoffer returns tikv.Backoffer.
func (b *backoffer) TiKVBackoffer() *tikv.Backoffer {
	return b.b
}

// Backoff sleeps a while base on the backoffType and records the error message.
// It returns a retryable error if total sleep time exceeds maxSleep.
func (b *backoffer) Backoff(typ tikv.BackoffType, err error) error {
	e := b.b.Backoff(typ, err)
	return derr.ToTiDBErr(e)
}

// BackoffWithMaxSleep sleeps a while base on the backoffType and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *backoffer) BackoffWithMaxSleep(typ tikv.BackoffType, maxSleepMs int, err error) error {
	e := b.b.BackoffWithMaxSleep(typ, maxSleepMs, err)
	return derr.ToTiDBErr(e)
}

// GetBackoffTimes returns a map contains backoff time count by type.
func (b *backoffer) GetBackoffTimes() map[tikv.BackoffType]int {
	return b.b.GetBackoffTimes()
}

// GetCtx returns the binded context.
func (b *backoffer) GetCtx() context.Context {
	return b.b.GetCtx()
}

// GetVars returns the binded vars.
func (b *backoffer) GetVars() *tikv.Variables {
	return b.b.GetVars()
}

// GetBackoffSleepMS returns a map contains backoff sleep time by type.
func (b *backoffer) GetBackoffSleepMS() map[tikv.BackoffType]int {
	return b.b.GetBackoffSleepMS()
}

// GetTotalSleep returns total sleep time.
func (b *backoffer) GetTotalSleep() int {
	return b.b.GetTotalSleep()
}
