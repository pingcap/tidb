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
	sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

// Open opens or creates an TiKV storage with given path.
// Path example: tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.Lock()
	defer mc.Unlock()

	etcdAddrs, clusterID, disableGC, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v-%v", etcdAddrs, clusterID)
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	pdCli, err := pd.NewClient(etcdAddrs, clusterID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, newRPCClient(), !disableGC)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mc.cache[uuid] = s
	return s, nil
}

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

type tikvStore struct {
	uuid         string
	oracle       oracle.Oracle
	client       Client
	regionCache  *RegionCache
	lockResolver *LockResolver
	gcWorker     *GCWorker
}

func newTikvStore(uuid string, pdClient pd.Client, client Client, enableGC bool) (*tikvStore, error) {
	oracle, err := oracles.NewPdOracle(pdClient, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}

	store := &tikvStore{
		uuid:        uuid,
		oracle:      oracle,
		client:      client,
		regionCache: NewRegionCache(pdClient),
	}
	store.lockResolver = newLockResolver(store)
	if enableGC {
		store.gcWorker, err = NewGCWorker(store)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return store, nil
}

// NewMockTikvStore creates a mocked tikv store.
func NewMockTikvStore() (kv.Storage, error) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	mvccStore := mocktikv.NewMvccStore()
	client := mocktikv.NewRPCClient(cluster, mvccStore)
	uuid := fmt.Sprintf("mock-tikv-store-:%v", time.Now().Unix())
	return newTikvStore(uuid, mocktikv.NewPDClient(cluster), client, false)
}

func (s *tikvStore) Begin() (kv.Transaction, error) {
	txn, err := newTiKVTxn(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	txnCounter.Inc()
	return txn, nil
}

func (s *tikvStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	snapshot := newTiKVSnapshot(s, ver)
	snapshotCounter.Inc()
	return snapshot, nil
}

func (s *tikvStore) Close() error {
	mc.Lock()
	defer mc.Unlock()

	delete(mc.cache, s.uuid)
	if err := s.client.Close(); err != nil {
		return errors.Trace(err)
	}
	s.oracle.Close()
	if s.gcWorker != nil {
		s.gcWorker.Close()
	}
	return nil
}

func (s *tikvStore) UUID() string {
	return s.uuid
}

func (s *tikvStore) CurrentVersion() (kv.Version, error) {
	bo := NewBackoffer(tsoMaxBackoff)
	startTS, err := s.getTimestampWithRetry(bo)
	if err != nil {
		return kv.NewVersion(0), errors.Trace(err)
	}
	return kv.NewVersion(startTS), nil
}

func (s *tikvStore) getTimestampWithRetry(bo *Backoffer) (uint64, error) {
	for {
		startTS, err := s.oracle.GetTimestamp()
		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(boPDRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *tikvStore) GetClient() kv.Client {
	txnCmdCounter.WithLabelValues("get_client").Inc()
	return &CopClient{
		store: s,
	}
}

// sendKVReq sends req to tikv server. It will retry internally to find the right
// region leader if i) fails to establish a connection to server or ii) server
// returns `NotLeader`.
func (s *tikvStore) SendKVReq(bo *Backoffer, req *pb.Request, regionID RegionVerID, timeout time.Duration) (*pb.Response, error) {
	for {
		region := s.regionCache.GetRegionByVerID(regionID)
		if region == nil {
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.
			return &pb.Response{
				Type:        req.GetType(),
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}, nil
		}
		req.Context = region.GetContext()
		resp, err := s.client.SendKVReq(region.GetAddress(), req, timeout)
		if err != nil {
			s.regionCache.NextPeer(region.VerID())
			err = bo.Backoff(boTiKVRPC, errors.Errorf("send tikv request error: %v, ctx: %s, try next peer later", err, req.Context))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			reportRegionError(regionErr)
			// Retry if error is `NotLeader`.
			if notLeader := regionErr.GetNotLeader(); notLeader != nil {
				log.Warnf("tikv reports `NotLeader`: %s, ctx: %s, retry later", notLeader, req.Context)
				s.regionCache.UpdateLeader(region.VerID(), notLeader.GetLeader().GetId())
				if notLeader.GetLeader() == nil {
					err = bo.Backoff(boRegionMiss, errors.Errorf("not leader: %v, ctx: %s", notLeader, req.Context))
					if err != nil {
						return nil, errors.Trace(err)
					}
				}
				continue
			}
			if staleEpoch := regionErr.GetStaleEpoch(); staleEpoch != nil {
				log.Warnf("tikv reports `StaleEpoch`, ctx: %s, retry later", req.Context)
				err = s.regionCache.OnRegionStale(region, staleEpoch.NewRegions)
				if err != nil {
					return nil, errors.Trace(err)
				}
				continue
			}
			// Retry if the error is `ServerIsBusy`.
			if regionErr.GetServerIsBusy() != nil {
				log.Warnf("tikv reports `ServerIsBusy`, ctx: %s, retry later", req.Context)
				err = bo.Backoff(boServerBusy, errors.Errorf("server is busy"))
				if err != nil {
					return nil, errors.Trace(err)
				}
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
}

func parsePath(path string) (etcdAddrs []string, clusterID uint64, disableGC bool, err error) {
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if strings.ToLower(u.Scheme) != "tikv" {
		err = errors.Errorf("Uri scheme expected[tikv] but found [%s]", u.Scheme)
		log.Error(err)
		return
	}
	clusterID, err = strconv.ParseUint(u.Query().Get("cluster"), 10, 64)
	if err != nil {
		log.Errorf("Parse clusterID error [%s]", err)
		err = errors.Trace(err)
		return
	}
	switch strings.ToLower(u.Query().Get("disableGC")) {
	case "true":
		disableGC = true
	case "false", "":
	default:
		err = errors.New("disableGC flag should be true/false")
		return
	}
	etcdAddrs = strings.Split(u.Host, ",")
	return
}

func init() {
	mc.cache = make(map[string]*tikvStore)
	rand.Seed(time.Now().UnixNano())
}
