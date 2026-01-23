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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"context"
	"crypto/tls"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	tidb_config "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type kvStore struct {
	store          *tikv.KVStore
	mppStoreCnt    *mppStoreCnt
	TiCIShardCache *TiCIShardCache
	codec          tikv.Codec
	tlsConfig      *tls.Config
	versionedRPC   *versionedRPCClient
}

// GetRegionCache returns the region cache instance.
func (s *kvStore) GetRegionCache() *RegionCache {
	return &RegionCache{s.store.GetRegionCache()}
}

// GetTiCIShardCache returns the TiCIShardCache instance.
func (s *kvStore) GetTiCIShardCache() *TiCIShardCache {
	return s.TiCIShardCache
}

// CheckVisibility checks if it is safe to read using given ts.
func (s *kvStore) CheckVisibility(startTime uint64) error {
	err := s.store.CheckVisibility(startTime)
	return derr.ToTiDBErr(err)
}

// GetTiKVClient gets the client instance.
func (s *kvStore) GetTiKVClient() tikv.Client {
	client := s.store.GetTiKVClient()
	return &tikvClient{c: client, versionedRPC: s.versionedRPC, codec: s.codec}
}

type tikvClient struct {
	c tikv.Client
	// versionedRPC is used for TiCI version-aware lookups.
	versionedRPC *versionedRPCClient
	codec        tikv.Codec
}

func (c *tikvClient) Close() error {
	if c.versionedRPC != nil {
		c.versionedRPC.Close()
	}
	return c.c.Close()
}

func (c *tikvClient) CloseAddr(addr string) error {
	if c.versionedRPC != nil {
		c.versionedRPC.CloseAddr(addr)
	}
	return c.c.CloseAddr(addr)
}

// SendRequest sends Request.
func (c *tikvClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if c.versionedRPC != nil && isVersionedCoprocessorRequest(req) {
		return c.versionedRPC.SendRequest(ctx, addr, req, timeout, c.codec)
	}
	if c.versionedRPC != nil && isVersionedBatchCoprocessorRequest(req) {
		return c.versionedRPC.SendBatchRequest(ctx, addr, req, timeout, c.codec)
	}
	return c.c.SendRequest(ctx, addr, req, timeout)
}

// SendRequestAsync sends Request asynchronously.
func (c *tikvClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	c.c.SendRequestAsync(ctx, addr, req, cb)
}

func (c *tikvClient) SetEventListener(listener tikv.ClientEventListener) {
	c.c.SetEventListener(listener)
}

// Store wraps tikv.KVStore and provides coprocessor utilities.
type Store struct {
	*kvStore
	coprCache       *coprCache
	replicaReadSeed uint32
	numcpu          int
}

// NewStore creates a new store instance.
func NewStore(s *tikv.KVStore, tls *tls.Config, coprCacheConfig *config.CoprocessorCache, codec tikv.Codec) (*Store, error) {
	coprCache, err := newCoprCache(coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var ticiClient *TiCIShardCacheClient
	// Only create TiCIShardCacheClient if the storage is not a mock storage.
	if s.SupportDeleteRange() {
		pdAddr := s.GetPDClient().GetLeaderURL()
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints:        []string{pdAddr},
			DialTimeout:      5 * time.Second,
			TLS:              tls,
			AutoSyncInterval: 30 * time.Second,
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
		ticiClient, err = NewTiCIShardCacheClient(etcdClient, s.GetPDClient().(*tikv.CodecPDClient))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	/* #nosec G404 */
	return &Store{
		kvStore:         &kvStore{store: s, mppStoreCnt: &mppStoreCnt{}, TiCIShardCache: NewTiCIShardCache(ticiClient), codec: codec, tlsConfig: tls, versionedRPC: newVersionedRPCClient(tls)},
		coprCache:       coprCache,
		replicaReadSeed: rand.Uint32(),
		numcpu:          runtime.GOMAXPROCS(0),
	}, nil
}

// Close releases resources allocated for coprocessor.
func (s *Store) Close() {
	if s.coprCache != nil {
		s.coprCache.cache.Close()
	}
	if s.TiCIShardCache != nil {
		s.TiCIShardCache.client.Close()
	}
	if s.versionedRPC != nil {
		s.versionedRPC.Close()
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
		if tidb_config.GetGlobalConfig().DisaggregatedTiFlash {
			return tikvrpc.TiFlashCompute
		}
		return tikvrpc.TiFlash
	case kv.TiDB:
		return tikvrpc.TiDB
	default:
		return tikvrpc.TiKV
	}
}

// Backoffer wraps tikv.Backoffer and converts the error which returns by the functions of tikv.Backoffer to tidb error.
type Backoffer = backoff.Backoffer
