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
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/latch"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/execdetails"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type storeCache struct {
	sync.Mutex
	cache map[string]*KVStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

func createEtcdKV(addrs []string, tlsConfig *tls.Config) (*clientv3.Client, error) {
	cfg := config.GetGlobalConfig()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            addrs,
		AutoSyncInterval:     30 * time.Second,
		DialTimeout:          5 * time.Second,
		TLS:                  tlsConfig,
		DialKeepAliveTime:    time.Second * time.Duration(cfg.TiKVClient.GrpcKeepAliveTime),
		DialKeepAliveTimeout: time.Second * time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout),
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
	pdConfig := config.GetGlobalConfig().PDClient
	tikvConfig := config.GetGlobalConfig().TiKVClient
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	etcdAddrs, disableGC, err := config.ParsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	}, pd.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(tikvConfig.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(tikvConfig.GrpcKeepAliveTimeout) * time.Second,
		}),
	), pd.WithCustomTimeoutOption(time.Duration(pdConfig.PDServerTimeout)*time.Second))
	pdCli = execdetails.InterceptedPDClient{Client: pdCli}

	if err != nil {
		return nil, errors.Trace(err)
	}

	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))
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

	coprCacheConfig := &config.GetGlobalConfig().TiKVClient.CoprCache
	s, err := NewKVStore(uuid, &CodecPDClient{pdCli}, spkv, NewRPCClient(security), !disableGC, coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if txnLocalLatches.Enabled {
		s.EnableTxnLocalLatches(txnLocalLatches.Capacity)
	}
	s.etcdAddrs = etcdAddrs
	s.tlsConfig = tlsConfig

	mc.cache[uuid] = s
	return s, nil
}

// EtcdBackend is used for judging a storage is a real TiKV.
type EtcdBackend interface {
	EtcdAddrs() ([]string, error)
	TLSConfig() *tls.Config
	StartGCWorker() error
}

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

// KVStore contains methods to interact with a TiKV cluster.
type KVStore struct {
	clusterID    uint64
	uuid         string
	oracle       oracle.Oracle
	client       Client
	pdClient     pd.Client
	regionCache  *RegionCache
	coprCache    *coprCache
	lockResolver *LockResolver
	txnLatches   *latch.LatchesScheduler
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

	replicaReadSeed uint32        // this is used to load balance followers / learners when replica read is enabled
	memCache        kv.MemManager // this is used to query from memory
}

// UpdateSPCache updates cached safepoint.
func (s *KVStore) UpdateSPCache(cachedSP uint64, cachedTime time.Time) {
	s.spMutex.Lock()
	s.safePoint = cachedSP
	s.spTime = cachedTime
	s.spMutex.Unlock()
}

// CheckVisibility checks if it is safe to read using given ts.
func (s *KVStore) CheckVisibility(startTime uint64) error {
	s.spMutex.RLock()
	cachedSafePoint := s.safePoint
	cachedTime := s.spTime
	s.spMutex.RUnlock()
	diff := time.Since(cachedTime)

	if diff > (GcSafePointCacheInterval - gcCPUTimeInaccuracyBound) {
		return ErrPDServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point")
	}

	if startTime < cachedSafePoint {
		t1 := oracle.GetTimeFromTS(startTime)
		t2 := oracle.GetTimeFromTS(cachedSafePoint)
		return ErrGCTooEarly.GenWithStackByArgs(t1, t2)
	}

	return nil
}

// NewKVStore creates a new TiKV store instance.
func NewKVStore(uuid string, pdClient pd.Client, spkv SafePointKV, client Client, enableGC bool, coprCacheConfig *config.CoprocessorCache) (*KVStore, error) {
	o, err := oracles.NewPdOracle(pdClient, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store := &KVStore{
		clusterID:       pdClient.GetClusterID(context.TODO()),
		uuid:            uuid,
		oracle:          o,
		client:          reqCollapse{client},
		pdClient:        pdClient,
		regionCache:     NewRegionCache(pdClient),
		coprCache:       nil,
		kv:              spkv,
		safePoint:       0,
		spTime:          time.Now(),
		closed:          make(chan struct{}),
		replicaReadSeed: rand.Uint32(),
		memCache:        kv.NewCacheDB(),
	}
	store.lockResolver = newLockResolver(store)
	store.enableGC = enableGC

	coprCache, err := newCoprCache(coprCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store.coprCache = coprCache

	go store.runSafePointChecker()

	return store, nil
}

// EnableTxnLocalLatches enables txn latch. It should be called before using
// the store to serve any requests.
func (s *KVStore) EnableTxnLocalLatches(size uint) {
	s.txnLatches = latch.NewScheduler(size)
}

// IsLatchEnabled is used by mockstore.TestConfig.
func (s *KVStore) IsLatchEnabled() bool {
	return s.txnLatches != nil
}

var (
	ldflagGetEtcdAddrsFromConfig = "0" // 1:Yes, otherwise:No
)

// EtcdAddrs returns etcd server addresses.
func (s *KVStore) EtcdAddrs() ([]string, error) {
	if s.etcdAddrs == nil {
		return nil, nil
	}

	if ldflagGetEtcdAddrsFromConfig == "1" {
		// For automated test purpose.
		// To manipulate connection to etcd by mandatorily setting path to a proxy.
		cfg := config.GetGlobalConfig()
		return strings.Split(cfg.Path, ","), nil
	}

	ctx := context.Background()
	bo := NewBackoffer(ctx, GetAllMembersBackoff)
	etcdAddrs := make([]string, 0)
	pdClient := s.pdClient
	if pdClient == nil {
		return nil, errors.New("Etcd client not found")
	}
	for {
		members, err := pdClient.GetAllMembers(ctx)
		if err != nil {
			err := bo.Backoff(BoRegionMiss, err)
			if err != nil {
				return nil, err
			}
			continue
		}
		for _, member := range members {
			if len(member.ClientUrls) > 0 {
				u, err := url.Parse(member.ClientUrls[0])
				if err != nil {
					logutil.BgLogger().Error("fail to parse client url from pd members", zap.String("client_url", member.ClientUrls[0]), zap.Error(err))
					return nil, err
				}
				etcdAddrs = append(etcdAddrs, u.Host)
			}
		}
		return etcdAddrs, nil
	}
}

// TLSConfig returns the tls config to connect to etcd.
func (s *KVStore) TLSConfig() *tls.Config {
	return s.tlsConfig
}

// StartGCWorker starts GC worker, it's called in BootstrapSession, don't call this function more than once.
func (s *KVStore) StartGCWorker() error {
	if !s.enableGC || NewGCHandlerFunc == nil {
		return nil
	}

	gcWorker, err := NewGCHandlerFunc(s, s.pdClient)
	if err != nil {
		return errors.Trace(err)
	}
	gcWorker.Start()
	s.gcWorker = gcWorker
	return nil
}

func (s *KVStore) runSafePointChecker() {
	d := gcSafePointUpdateInterval
	for {
		select {
		case spCachedTime := <-time.After(d):
			cachedSafePoint, err := loadSafePoint(s.GetSafePointKV())
			if err == nil {
				metrics.TiKVLoadSafepointCounter.WithLabelValues("ok").Inc()
				s.UpdateSPCache(cachedSafePoint, spCachedTime)
				d = gcSafePointUpdateInterval
			} else {
				metrics.TiKVLoadSafepointCounter.WithLabelValues("fail").Inc()
				logutil.BgLogger().Error("fail to load safepoint from pd", zap.Error(err))
				d = gcSafePointQuickRepeatInterval
			}
		case <-s.Closed():
			return
		}
	}
}

// Begin a global transaction.
func (s *KVStore) Begin() (kv.Transaction, error) {
	return s.BeginWithTxnScope(oracle.GlobalTxnScope)
}

// BeginWithTxnScope begins a transaction with the given txnScope (local or
// global)
func (s *KVStore) BeginWithTxnScope(txnScope string) (kv.Transaction, error) {
	txn, err := newTiKVTxn(s, txnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

// BeginWithStartTS begins a transaction with startTS.
func (s *KVStore) BeginWithStartTS(txnScope string, startTS uint64) (kv.Transaction, error) {
	txn, err := newTiKVTxnWithStartTS(s, txnScope, startTS, s.nextReplicaReadSeed())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

// BeginWithExactStaleness begins transaction with given staleness
func (s *KVStore) BeginWithExactStaleness(txnScope string, prevSec uint64) (kv.Transaction, error) {
	txn, err := newTiKVTxnWithExactStaleness(s, txnScope, prevSec)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
func (s *KVStore) GetSnapshot(ver kv.Version) kv.Snapshot {
	snapshot := newTiKVSnapshot(s, ver, s.nextReplicaReadSeed())
	return snapshot
}

// Close store
func (s *KVStore) Close() error {
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

	if s.txnLatches != nil {
		s.txnLatches.Close()
	}
	s.regionCache.Close()
	if s.coprCache != nil {
		s.coprCache.cache.Close()
	}

	if err := s.kv.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// UUID return a unique ID which represents a Storage.
func (s *KVStore) UUID() string {
	return s.uuid
}

// CurrentVersion returns current max committed version with the given txnScope (local or global).
func (s *KVStore) CurrentVersion(txnScope string) (kv.Version, error) {
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	startTS, err := s.getTimestampWithRetry(bo, txnScope)
	if err != nil {
		return kv.NewVersion(0), errors.Trace(err)
	}
	return kv.NewVersion(startTS), nil
}

func (s *KVStore) getTimestampWithRetry(bo *Backoffer, txnScope string) (uint64, error) {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("TiKVStore.getTimestampWithRetry", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	for {
		startTS, err := s.oracle.GetTimestamp(bo.ctx, &oracle.Option{TxnScope: txnScope})
		// mockGetTSErrorInRetry should wait MockCommitErrorOnce first, then will run into retry() logic.
		// Then mockGetTSErrorInRetry will return retryable error when first retry.
		// Before PR #8743, we don't cleanup txn after meet error such as error like: PD server timeout
		// This may cause duplicate data to be written.
		failpoint.Inject("mockGetTSErrorInRetry", func(val failpoint.Value) {
			if val.(bool) && !kv.IsMockCommitErrorEnable() {
				err = ErrPDServerTimeout.GenWithStackByArgs("mock PD timeout")
			}
		})

		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(BoPDRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *KVStore) getStalenessTimestamp(bo *Backoffer, txnScope string, prevSec uint64) (uint64, error) {
	for {
		startTS, err := s.oracle.GetStaleTimestamp(bo.ctx, txnScope, prevSec)
		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(BoPDRPC, errors.Errorf("get staleness timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *KVStore) nextReplicaReadSeed() uint32 {
	return atomic.AddUint32(&s.replicaReadSeed, 1)
}

// GetClient gets a client instance.
func (s *KVStore) GetClient() kv.Client {
	return &CopClient{
		store:           s,
		replicaReadSeed: s.nextReplicaReadSeed(),
	}
}

// GetMPPClient gets a mpp client instance.
func (s *KVStore) GetMPPClient() kv.MPPClient {
	return &MPPClient{
		store: s,
	}
}

// GetOracle gets a timestamp oracle client.
func (s *KVStore) GetOracle() oracle.Oracle {
	return s.oracle
}

// Name gets the name of the storage engine
func (s *KVStore) Name() string {
	return "TiKV"
}

// Describe returns of brief introduction of the storage
func (s *KVStore) Describe() string {
	return "TiKV is a distributed transactional key-value database"
}

// ShowStatus returns the specified status of the storage
func (s *KVStore) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, kv.ErrNotImplemented
}

// SupportDeleteRange gets the storage support delete range or not.
func (s *KVStore) SupportDeleteRange() (supported bool) {
	return !s.mock
}

// SendReq sends a request to region.
func (s *KVStore) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	sender := NewRegionRequestSender(s.regionCache, s.client)
	return sender.SendReq(bo, req, regionID, timeout)
}

// GetRegionCache returns the region cache instance.
func (s *KVStore) GetRegionCache() *RegionCache {
	return s.regionCache
}

// GetLockResolver returns the lock resolver instance.
func (s *KVStore) GetLockResolver() *LockResolver {
	return s.lockResolver
}

// GetGCHandler returns the GC worker instance.
func (s *KVStore) GetGCHandler() GCHandler {
	return s.gcWorker
}

// Closed returns a channel that indicates if the store is closed.
func (s *KVStore) Closed() <-chan struct{} {
	return s.closed
}

// GetSafePointKV returns the kv store that used for safepoint.
func (s *KVStore) GetSafePointKV() SafePointKV {
	return s.kv
}

// SetOracle resets the oracle instance.
func (s *KVStore) SetOracle(oracle oracle.Oracle) {
	s.oracle = oracle
}

// SetTiKVClient resets the client instance.
func (s *KVStore) SetTiKVClient(client Client) {
	s.client = client
}

// GetTiKVClient gets the client instance.
func (s *KVStore) GetTiKVClient() (client Client) {
	return s.client
}

// GetMemCache return memory mamager of the storage
func (s *KVStore) GetMemCache() kv.MemManager {
	return s.memCache
}

func init() {
	mc.cache = make(map[string]*KVStore)
	rand.Seed(time.Now().UnixNano())
}
