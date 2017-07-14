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
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	goctx "golang.org/x/net/context"
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

	etcdAddrs, disableGC, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(etcdAddrs)
	if err != nil {
		if strings.Contains(err.Error(), "i/o timeout") {
			return nil, errors.Annotate(err, txnRetryableMark)
		}
		return nil, errors.Trace(err)
	}

	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(goctx.TODO()))
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, newRPCClient(), !disableGC)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.etcdAddrs = etcdAddrs
	mc.cache[uuid] = s
	return s, nil
}

// MockDriver is in memory mock TiKV driver.
type MockDriver struct {
}

// Open creates a MockTiKV storage.
func (d MockDriver) Open(path string) (kv.Storage, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !strings.EqualFold(u.Scheme, "mocktikv") {
		return nil, errors.Errorf("Uri scheme expected(mocktikv) but found (%s)", u.Scheme)
	}
	return NewMockTikvStore()
}

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

type tikvStore struct {
	clusterID    uint64
	uuid         string
	oracle       oracle.Oracle
	client       Client
	regionCache  *RegionCache
	lockResolver *LockResolver
	gcWorker     *GCWorker
	etcdAddrs    []string
	mock         bool
}

func newTikvStore(uuid string, pdClient pd.Client, client Client, enableGC bool) (*tikvStore, error) {
	oracle, err := oracles.NewPdOracle(pdClient, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, mock := client.(*mocktikv.RPCClient)
	store := &tikvStore{
		clusterID:   pdClient.GetClusterID(goctx.TODO()),
		uuid:        uuid,
		oracle:      oracle,
		client:      client,
		regionCache: NewRegionCache(pdClient),
		mock:        mock,
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

func (s *tikvStore) EtcdAddrs() []string {
	return s.etcdAddrs
}

type mockOptions struct {
	cluster        *mocktikv.Cluster
	mvccStore      *mocktikv.MvccStore
	clientHijack   func(Client) Client
	pdClientHijack func(pd.Client) pd.Client
}

// MockTiKVStoreOption is used to control some behavior of mock tikv.
type MockTiKVStoreOption func(*mockOptions)

// WithHijackClient hijacks KV client's behavior, makes it easy to simulate the network
// problem between TiDB and TiKV.
func WithHijackClient(wrap func(Client) Client) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.clientHijack = wrap
	}
}

// WithHijackPDClient hijacks PD client's behavior, makes it easy to simulate the network
// problem between TiDB and PD, such as GetTS too slow, GetStore or GetRegion fail.
func WithHijackPDClient(wrap func(pd.Client) pd.Client) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.pdClientHijack = wrap
	}
}

// WithCluster provides the customized cluster.
func WithCluster(cluster *mocktikv.Cluster) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.cluster = cluster
	}
}

// WithMVCCStore provides the customized mvcc store.
func WithMVCCStore(store *mocktikv.MvccStore) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.mvccStore = store
	}
}

// NewMockTikvStore creates a mocked tikv store, the path is the file path to store the data.
// If path is an empty string, a memory storage will be created.
func NewMockTikvStore(options ...MockTiKVStoreOption) (kv.Storage, error) {
	var opt mockOptions
	for _, f := range options {
		f(&opt)
	}

	cluster := opt.cluster
	if cluster == nil {
		cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(cluster)
	}

	mvccStore := opt.mvccStore
	if mvccStore == nil {
		mvccStore = mocktikv.NewMvccStore()
	}

	client := Client(mocktikv.NewRPCClient(cluster, mvccStore))
	if opt.clientHijack != nil {
		client = opt.clientHijack(client)
	}

	// Make sure the uuid is unique.
	partID := fmt.Sprintf("%05d", rand.Intn(100000))
	uuid := fmt.Sprintf("mock-tikv-store-%v-%v", time.Now().Unix(), partID)
	pdCli := pd.Client(&codecPDClient{mocktikv.NewPDClient(cluster)})
	if opt.pdClientHijack != nil {
		pdCli = opt.pdClientHijack(pdCli)
	}

	return newTikvStore(uuid, pdCli, client, false)
}

func (s *tikvStore) Begin() (kv.Transaction, error) {
	txn, err := newTiKVTxn(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	txnCounter.Inc()
	return txn, nil
}

// BeginWithStartTS begins a transaction with startTS.
func (s *tikvStore) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	txn, err := newTikvTxnWithStartTS(s, startTS)
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
	s.oracle.Close()
	if s.gcWorker != nil {
		s.gcWorker.Close()
	}

	if err := s.client.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *tikvStore) UUID() string {
	return s.uuid
}

func (s *tikvStore) CurrentVersion() (kv.Version, error) {
	bo := NewBackoffer(tsoMaxBackoff, goctx.Background())
	startTS, err := s.getTimestampWithRetry(bo)
	if err != nil {
		return kv.NewVersion(0), errors.Trace(err)
	}
	return kv.NewVersion(startTS), nil
}

func (s *tikvStore) getTimestampWithRetry(bo *Backoffer) (uint64, error) {
	for {
		startTS, err := s.oracle.GetTimestamp(bo.ctx)
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

func (s *tikvStore) GetOracle() oracle.Oracle {
	return s.oracle
}

func (s *tikvStore) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	sender := NewRegionRequestSender(s.regionCache, s.client, kvrpcpb.IsolationLevel_SI)
	return sender.SendReq(bo, req, regionID, timeout)
}

func (s *tikvStore) LocateKey(bo *Backoffer, key []byte) (*KeyLocation, error) {
	return s.regionCache.LocateKey(bo, key)
}

// ParseEtcdAddr parses path to etcd address list
func ParseEtcdAddr(path string) (etcdAddrs []string, err error) {
	etcdAddrs, _, err = parsePath(path)
	return
}

func parsePath(path string) (etcdAddrs []string, disableGC bool, err error) {
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
