// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/errorpb"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
)

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates an TiKV storage with given path.
// Path example: tikv://etcd-node1:port,etcd-node2:port/pd-path?cluster=1
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	etcdAddrs, pdPath, clusterID, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v-%v-%v", etcdAddrs, pdPath, clusterID)
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	pdCli, err := pd.NewClient(etcdAddrs, pdPath, clusterID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s := newTikvStore(uuid, &codecPDClient{pdCli}, newRPCClient())
	mc.cache[uuid] = s
	return s, nil
}

type tikvStore struct {
	mu          sync.Mutex
	uuid        string
	oracle      oracle.Oracle
	client      Client
	regionCache *RegionCache
}

func newTikvStore(uuid string, pdClient pd.Client, client Client) *tikvStore {
	return &tikvStore{
		uuid:        uuid,
		oracle:      oracles.NewPdOracle(pdClient),
		client:      client,
		regionCache: NewRegionCache(pdClient),
	}
}

// NewMockTikvStore creates a mocked tikv store.
func NewMockTikvStore() kv.Storage {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	mvccStore := mocktikv.NewMvccStore()
	client := mocktikv.NewRPCClient(cluster, mvccStore)
	uuid := fmt.Sprintf("mock-tikv-store-:%v", time.Now().Unix())
	return newTikvStore(uuid, mocktikv.NewPDClient(cluster), client)
}

func (s *tikvStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	txn, err := newTiKVTxn(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

func (s *tikvStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	snapshot := newTiKVSnapshot(s, ver)
	return snapshot, nil
}

func (s *tikvStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.cache, s.uuid)
	if err := s.client.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *tikvStore) UUID() string {
	return s.uuid
}

func (s *tikvStore) CurrentVersion() (kv.Version, error) {
	startTS, err := s.getTimestampWithRetry()
	if err != nil {
		return kv.NewVersion(0), errors.Trace(err)
	}

	return kv.NewVersion(startTS), nil
}

func (s *tikvStore) getTimestampWithRetry() (uint64, error) {
	var backoffErr error
	for backoff := pdBackoff(); backoffErr == nil; backoffErr = backoff() {
		startTS, err := s.oracle.GetTimestamp()
		if err != nil {
			log.Warnf("get timestamp failed: %v, retry later", err)
			continue
		}
		return startTS, nil
	}
	return 0, errors.Annotate(backoffErr, txnRetryableMark)
}

func (s *tikvStore) checkTimestampExpiredWithRetry(ts uint64, TTL uint64) (bool, error) {
	var backoffErr error
	for backoff := pdBackoff(); backoffErr == nil; backoffErr = backoff() {
		expired, err := s.oracle.IsExpired(ts, TTL)
		if err != nil {
			log.Warnf("check expired failed: %v, retry later", err)
			continue
		}
		return expired, nil
	}
	return false, errors.Annotate(backoffErr, txnRetryableMark)
}

// sendKVReq sends req to tikv server. It will retry internally to find the right
// region leader if i) fails to establish a connection to server or ii) server
// returns `NotLeader`.
func (s *tikvStore) SendKVReq(req *pb.Request, regionID RegionVerID) (*pb.Response, error) {
	var backoffErr error
	for backoff := rpcBackoff(); backoffErr == nil; backoffErr = backoff() {
		region := s.regionCache.GetRegionByVerID(regionID)
		if region == nil {
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.
			return &pb.Response{
				Type:        req.GetType().Enum(),
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}, nil
		}
		req.Context = region.GetContext()
		resp, err := s.client.SendKVReq(region.GetAddress(), req)
		if err != nil {
			log.Warnf("send tikv request error: %v, ctx: %s, try next peer later", err, req.Context)
			s.regionCache.NextPeer(region.VerID())
			continue
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			// Retry if error is `NotLeader`.
			if notLeader := regionErr.GetNotLeader(); notLeader != nil {
				log.Warnf("tikv reports `NotLeader`: %s, ctx: %s, retry later", notLeader, req.Context)
				s.regionCache.UpdateLeader(region.VerID(), notLeader.GetLeader().GetId())
				continue
			}
			// For other errors, we only drop cache here.
			// Because caller may need to re-split the request.
			log.Warnf("tikv reports region error: %s, ctx: %s", resp.GetRegionError(), req.Context)
			s.regionCache.DropRegion(region.VerID())
			return resp, nil
		}
		if resp.GetType() != req.GetType() {
			return nil, errors.Trace(errMismatch(resp, req))
		}
		return resp, nil
	}
	return nil, errors.Trace(backoffErr)
}

func parsePath(path string) (etcdAddrs []string, pdPath string, clusterID uint64, err error) {
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if strings.ToLower(u.Scheme) != "tikv" {
		log.Errorf("Uri scheme expected[tikv] but found [%s]", u.Scheme)
		err = errors.Trace(err)
		return
	}
	clusterID, err = strconv.ParseUint(u.Query().Get("cluster"), 10, 64)
	if err != nil {
		log.Errorf("Parse clusterID error [%s]", err)
		err = errors.Trace(err)
		return
	}
	etcdAddrs = strings.Split(u.Host, ",")
	pdPath = u.Path
	return
}

func init() {
	mc.cache = make(map[string]*tikvStore)
	rand.Seed(time.Now().UnixNano())
}
