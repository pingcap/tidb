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
	"math/rand"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// Store wraps tikv.KVStore and provides coprocessor utilities.
type Store struct {
	*tikv.KVStore
	coprCache       *coprCache
	replicaReadSeed uint32
}

// NewStore creates a new store instance.
func NewStore(kvStore *tikv.KVStore, coprCacheConfig *config.CoprocessorCache) (*Store, error) {
	coprCache, err := newCoprCache(coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Store{
		KVStore:         kvStore,
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
		store: s.KVStore,
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
