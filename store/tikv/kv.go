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
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/tikv/client"
	"github.com/pingcap/tidb/store/tikv/config"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/latch"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
	"github.com/pingcap/tidb/store/tikv/retry"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// DCLabelKey indicates the key of label which represents the dc for Store.
const DCLabelKey = "zone"

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

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

// KVStore contains methods to interact with a TiKV cluster.
type KVStore struct {
	clusterID uint64
	uuid      string
	oracle    oracle.Oracle
	clientMu  struct {
		sync.RWMutex
		client Client
	}
	pdClient     pd.Client
	regionCache  *RegionCache
	lockResolver *LockResolver
	txnLatches   *latch.LatchesScheduler

	mock bool

	kv        SafePointKV
	safePoint uint64
	spTime    time.Time
	spMutex   sync.RWMutex  // this is used to update safePoint and spTime
	closed    chan struct{} // this is used to notify when the store is closed

	// storeID -> safeTS, stored as map[uint64]uint64
	// safeTS here will be used during the Stale Read process,
	// it indicates the safe timestamp point that can be used to read consistent but may not the latest data.
	safeTSMap sync.Map

	replicaReadSeed uint32 // this is used to load balance followers / learners when replica read is enabled
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
		return tikverr.NewErrPDServerTimeout("start timestamp may fall behind safe point")
	}

	if startTime < cachedSafePoint {
		t1 := oracle.GetTimeFromTS(startTime)
		t2 := oracle.GetTimeFromTS(cachedSafePoint)
		return &tikverr.ErrGCTooEarly{
			TxnStartTS:  t1,
			GCSafePoint: t2,
		}
	}

	return nil
}

// NewKVStore creates a new TiKV store instance.
func NewKVStore(uuid string, pdClient pd.Client, spkv SafePointKV, tikvclient Client) (*KVStore, error) {
	o, err := oracles.NewPdOracle(pdClient, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store := &KVStore{
		clusterID:       pdClient.GetClusterID(context.TODO()),
		uuid:            uuid,
		oracle:          o,
		pdClient:        pdClient,
		regionCache:     NewRegionCache(pdClient),
		kv:              spkv,
		safePoint:       0,
		spTime:          time.Now(),
		closed:          make(chan struct{}),
		replicaReadSeed: rand.Uint32(),
	}
	store.clientMu.client = client.NewReqCollapse(tikvclient)
	store.lockResolver = newLockResolver(store)

	go store.runSafePointChecker()
	go store.safeTSUpdater()

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
func (s *KVStore) Begin() (*KVTxn, error) {
	return s.BeginWithOption(DefaultStartTSOption())
}

// BeginWithOption begins a transaction with the given StartTSOption
func (s *KVStore) BeginWithOption(options StartTSOption) (*KVTxn, error) {
	return newTiKVTxnWithOptions(s, options)
}

// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
// if ts is MaxVersion or > current max committed version, we will use current version for this snapshot.
func (s *KVStore) GetSnapshot(ts uint64) *KVSnapshot {
	snapshot := newTiKVSnapshot(s, ts, s.nextReplicaReadSeed())
	return snapshot
}

// Close store
func (s *KVStore) Close() error {
	s.oracle.Close()
	s.pdClient.Close()

	close(s.closed)
	if err := s.GetTiKVClient().Close(); err != nil {
		return errors.Trace(err)
	}

	if s.txnLatches != nil {
		s.txnLatches.Close()
	}
	s.regionCache.Close()

	if err := s.kv.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// UUID return a unique ID which represents a Storage.
func (s *KVStore) UUID() string {
	return s.uuid
}

// CurrentTimestamp returns current timestamp with the given txnScope (local or global).
func (s *KVStore) CurrentTimestamp(txnScope string) (uint64, error) {
	bo := retry.NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	startTS, err := s.getTimestampWithRetry(bo, txnScope)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return startTS, nil
}

func (s *KVStore) getTimestampWithRetry(bo *Backoffer, txnScope string) (uint64, error) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("TiKVStore.getTimestampWithRetry", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	for {
		startTS, err := s.oracle.GetTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: txnScope})
		// mockGetTSErrorInRetry should wait MockCommitErrorOnce first, then will run into retry() logic.
		// Then mockGetTSErrorInRetry will return retryable error when first retry.
		// Before PR #8743, we don't cleanup txn after meet error such as error like: PD server timeout
		// This may cause duplicate data to be written.
		failpoint.Inject("mockGetTSErrorInRetry", func(val failpoint.Value) {
			if val.(bool) && !IsMockCommitErrorEnable() {
				err = tikverr.NewErrPDServerTimeout("mock PD timeout")
			}
		})

		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(retry.BoPDRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *KVStore) nextReplicaReadSeed() uint32 {
	return atomic.AddUint32(&s.replicaReadSeed, 1)
}

// GetOracle gets a timestamp oracle client.
func (s *KVStore) GetOracle() oracle.Oracle {
	return s.oracle
}

// GetPDClient returns the PD client.
func (s *KVStore) GetPDClient() pd.Client {
	return s.pdClient
}

// SupportDeleteRange gets the storage support delete range or not.
func (s *KVStore) SupportDeleteRange() (supported bool) {
	return !s.mock
}

// SendReq sends a request to region.
func (s *KVStore) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	sender := NewRegionRequestSender(s.regionCache, s.GetTiKVClient())
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
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	s.clientMu.client = client
}

// GetTiKVClient gets the client instance.
func (s *KVStore) GetTiKVClient() (client Client) {
	s.clientMu.RLock()
	defer s.clientMu.RUnlock()
	return s.clientMu.client
}

// GetMinSafeTS return the minimal safeTS of the storage with given txnScope.
func (s *KVStore) GetMinSafeTS(txnScope string) uint64 {
	stores := make([]*Store, 0)
	allStores := s.regionCache.getStoresByType(tikvrpc.TiKV)
	if txnScope != oracle.GlobalTxnScope {
		for _, store := range allStores {
			if store.IsLabelsMatch([]*metapb.StoreLabel{
				{
					Key:   DCLabelKey,
					Value: txnScope,
				},
			}) {
				stores = append(stores, store)
			}
		}
	} else {
		stores = allStores
	}
	return s.getMinSafeTSByStores(stores)
}

func (s *KVStore) getSafeTS(storeID uint64) uint64 {
	safeTS, ok := s.safeTSMap.Load(storeID)
	if !ok {
		return 0
	}
	return safeTS.(uint64)
}

// setSafeTS sets safeTs for store storeID, export for testing
func (s *KVStore) setSafeTS(storeID, safeTS uint64) {
	s.safeTSMap.Store(storeID, safeTS)
}

func (s *KVStore) getMinSafeTSByStores(stores []*Store) uint64 {
	failpoint.Inject("injectSafeTS", func(val failpoint.Value) {
		injectTS := val.(int)
		failpoint.Return(uint64(injectTS))
	})
	minSafeTS := uint64(math.MaxUint64)
	// when there is no store, return 0 in order to let minStartTS become startTS directly
	if len(stores) < 1 {
		return 0
	}
	for _, store := range stores {
		safeTS := s.getSafeTS(store.storeID)
		if safeTS < minSafeTS {
			minSafeTS = safeTS
		}
	}
	return minSafeTS
}

func (s *KVStore) safeTSUpdater() {
	t := time.NewTicker(time.Second * 2)
	defer t.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case <-s.Closed():
			return
		case <-t.C:
			s.updateSafeTS(ctx)
		}
	}
}

func (s *KVStore) updateSafeTS(ctx context.Context) {
	stores := s.regionCache.getStoresByType(tikvrpc.TiKV)
	tikvClient := s.GetTiKVClient()
	wg := &sync.WaitGroup{}
	wg.Add(len(stores))
	for _, store := range stores {
		storeID := store.storeID
		storeAddr := store.addr
		go func(ctx context.Context, wg *sync.WaitGroup, storeID uint64, storeAddr string) {
			defer wg.Done()
			resp, err := tikvClient.SendRequest(ctx, storeAddr, tikvrpc.NewRequest(tikvrpc.CmdStoreSafeTS, &kvrpcpb.StoreSafeTSRequest{KeyRange: &kvrpcpb.KeyRange{
				StartKey: []byte(""),
				EndKey:   []byte(""),
			}}), client.ReadTimeoutShort)
			storeIDStr := strconv.Itoa(int(storeID))
			if err != nil {
				metrics.TiKVSafeTSUpdateCounter.WithLabelValues("fail", storeIDStr).Inc()
				logutil.BgLogger().Debug("update safeTS failed", zap.Error(err), zap.Uint64("store-id", storeID))
				return
			}
			safeTSResp := resp.Resp.(*kvrpcpb.StoreSafeTSResponse)
			s.setSafeTS(storeID, safeTSResp.GetSafeTs())
			metrics.TiKVSafeTSUpdateCounter.WithLabelValues("success", storeIDStr).Inc()
			metrics.TiKVSafeTSUpdateStats.WithLabelValues(storeIDStr).Set(float64(safeTSResp.GetSafeTs()))
		}(ctx, wg, storeID, storeAddr)
	}
	wg.Wait()
}

// Variables defines the variables used by TiKV storage.
type Variables = kv.Variables
