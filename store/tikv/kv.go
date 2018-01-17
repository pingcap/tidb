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
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type storeCache struct {
	sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

func createEtcdKV(addrs []string, tlsConfig *tls.Config) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   addrs,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
			grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
		},
		TLS: tlsConfig,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cli, nil
}

// Open opens or creates an TiKV storage with given path.
// Path example: tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.Lock()
	defer mc.Unlock()

	security := config.GetGlobalConfig().Security
	etcdAddrs, disableGC, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	})

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

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(security), !disableGC)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.etcdAddrs = etcdAddrs
	s.tlsConfig = tlsConfig

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
	return NewMockTikvStore(WithPath(u.Path))
}

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

type tikvStore struct {
	clusterID    uint64
	uuid         string
	oracle       oracle.Oracle
	client       Client
	pdClient     pd.Client
	regionCache  *RegionCache
	lockResolver *LockResolver
	gcWorker     GCHandler
	etcdAddrs    []string
	tlsConfig    *tls.Config
	mock         bool
	enableGC     bool

	kv        SafePointKV
	safePoint uint64
	spTime    time.Time
	spMutex   sync.RWMutex  // this is used to update safePoint and spTime
	closed    chan struct{} // this is used to nofity when the store is closed
}

func (s *tikvStore) UpdateSPCache(cachedSP uint64, cachedTime time.Time) {
	s.spMutex.Lock()
	s.safePoint = cachedSP
	s.spTime = cachedTime
	s.spMutex.Unlock()
}

func (s *tikvStore) CheckVisibility(startTime uint64) error {
	s.spMutex.RLock()
	cachedSafePoint := s.safePoint
	cachedTime := s.spTime
	s.spMutex.RUnlock()
	diff := time.Since(cachedTime)

	if diff > (GcSafePointCacheInterval - gcCPUTimeInaccuracyBound) {
		return ErrPDServerTimeout.GenByArgs("start timestamp may fall behind safe point")
	}

	if startTime < cachedSafePoint {
		return ErrGCTooEarly
	}

	return nil
}

func newTikvStore(uuid string, pdClient pd.Client, spkv SafePointKV, client Client, enableGC bool) (*tikvStore, error) {
	o, err := oracles.NewPdOracle(pdClient, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store := &tikvStore{
		clusterID:   pdClient.GetClusterID(goctx.TODO()),
		uuid:        uuid,
		oracle:      o,
		client:      client,
		pdClient:    pdClient,
		regionCache: NewRegionCache(pdClient),
		kv:          spkv,
		safePoint:   0,
		spTime:      time.Now(),
		closed:      make(chan struct{}),
	}
	store.lockResolver = newLockResolver(store)
	store.enableGC = enableGC

	go store.runSafePointChecker()

	return store, nil
}

func (s *tikvStore) EtcdAddrs() []string {
	return s.etcdAddrs
}

func (s *tikvStore) TLSConfig() *tls.Config {
	return s.tlsConfig
}

// StartGCWorker starts GC worker, it's called in BootstrapSession, don't call this function more than once.
func (s *tikvStore) StartGCWorker() error {
	if !s.enableGC || NewGCHandlerFunc == nil {
		return nil
	}

	gcWorker, err := NewGCHandlerFunc(s)
	if err != nil {
		return errors.Trace(err)
	}
	gcWorker.Start()
	s.gcWorker = gcWorker
	return nil
}

func (s *tikvStore) runSafePointChecker() {
	d := gcSafePointUpdateInterval
	for {
		select {
		case spCachedTime := <-time.After(d):
			cachedSafePoint, err := loadSafePoint(s.GetSafePointKV(), GcSavedSafePoint)
			if err == nil {
				loadSafepointCounter.WithLabelValues("ok").Inc()
				s.UpdateSPCache(cachedSafePoint, spCachedTime)
				d = gcSafePointUpdateInterval
			} else {
				loadSafepointCounter.WithLabelValues("fail").Inc()
				log.Errorf("[tikv] fail to load safepoint: %v", err)
				d = gcSafePointQuickRepeatInterval
			}
		case <-s.Closed():
			return
		}
	}
}

type mockOptions struct {
	cluster        *mocktikv.Cluster
	mvccStore      mocktikv.MVCCStore
	clientHijack   func(Client) Client
	pdClientHijack func(pd.Client) pd.Client
	path           string
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

// WithCluster provides the customized cluster.
func WithCluster(cluster *mocktikv.Cluster) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.cluster = cluster
	}
}

// WithMVCCStore provides the customized mvcc store.
func WithMVCCStore(store mocktikv.MVCCStore) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.mvccStore = store
	}
}

// WithPath specifies the mocktikv path.
func WithPath(path string) MockTiKVStoreOption {
	return func(c *mockOptions) {
		c.path = path
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
		var err error
		mvccStore, err = mocktikv.NewMVCCLevelDB(opt.path)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	client := Client(mocktikv.NewRPCClient(cluster, mvccStore))
	if opt.clientHijack != nil {
		client = opt.clientHijack(client)
	}

	// Make sure the uuid is unique.
	partID := fmt.Sprintf("%05d", rand.Intn(100000))
	uuid := fmt.Sprintf("mocktikv-store-%v-%v", time.Now().Unix(), partID)
	pdCli := pd.Client(&codecPDClient{mocktikv.NewPDClient(cluster)})
	if opt.pdClientHijack != nil {
		pdCli = opt.pdClientHijack(pdCli)
	}

	spkv := NewMockSafePointKV()
	tikvStore, err := newTikvStore(uuid, pdCli, spkv, client, false)
	tikvStore.mock = true
	return tikvStore, errors.Trace(err)
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
	s.pdClient.Close()
	if s.gcWorker != nil {
		s.gcWorker.Close()
	}

	close(s.closed)

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
		startTS, err := s.oracle.GetTimestamp(bo)
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

func (s *tikvStore) SupportDeleteRange() (supported bool) {
	if s.mock {
		return false
	}
	return true
}

func (s *tikvStore) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	sender := NewRegionRequestSender(s.regionCache, s.client)
	return sender.SendReq(bo, req, regionID, timeout)
}

func (s *tikvStore) GetRegionCache() *RegionCache {
	return s.regionCache
}

func (s *tikvStore) GetLockResolver() *LockResolver {
	return s.lockResolver
}

func (s *tikvStore) GetGCHandler() GCHandler {
	return s.gcWorker
}

func (s *tikvStore) Closed() <-chan struct{} {
	return s.closed
}

func (s *tikvStore) GetSafePointKV() SafePointKV {
	return s.kv
}

func (s *tikvStore) SetOracle(oracle oracle.Oracle) {
	s.oracle = oracle
}

func (s *tikvStore) SetTiKVClient(client Client) {
	s.client = client
}

func (s *tikvStore) GetTiKVClient() (client Client) {
	return s.client
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
