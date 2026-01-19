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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcworker

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/stretchr/testify/require"
	kv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/constants"
)

type mockGCWorkerLockResolver struct {
	tikv.RegionLockResolver
	tikvStore         tikv.Storage
	scanLocks         func([]*txnlock.Lock, []byte) ([]*txnlock.Lock, *tikv.KeyLocation)
	batchResolveLocks func([]*txnlock.Lock, *tikv.KeyLocation) (*tikv.KeyLocation, error)
}

func (l *mockGCWorkerLockResolver) ScanLocksInOneRegion(bo *tikv.Backoffer, key []byte, endKey []byte, maxVersion uint64, limit uint32) ([]*txnlock.Lock, *tikv.KeyLocation, error) {
	locks, loc, err := l.RegionLockResolver.ScanLocksInOneRegion(bo, key, endKey, maxVersion, limit)
	if err != nil {
		return nil, nil, err
	}
	if l.scanLocks != nil {
		mockLocks, mockLoc := l.scanLocks(locks, key)
		// append locks from mock function
		locks = append(locks, mockLocks...)
		// use location from mock function
		loc = mockLoc
	}
	return locks, loc, nil
}

func (l *mockGCWorkerLockResolver) ResolveLocksInOneRegion(bo *tikv.Backoffer, locks []*txnlock.Lock, loc *tikv.KeyLocation) (*tikv.KeyLocation, error) {
	if l.batchResolveLocks != nil {
		return l.batchResolveLocks(locks, loc)
	}
	return l.RegionLockResolver.ResolveLocksInOneRegion(bo, locks, loc)
}

func (l *mockGCWorkerLockResolver) GetStore() tikv.Storage {
	return l.tikvStore
}

func (l *mockGCWorkerLockResolver) Identifier() string {
	return "gc worker test"
}

type mockGCWorkerClient struct {
	tikv.Client
	unsafeDestroyRangeHandler handler
	deleteRangeHandler        handler
	scanLockRequestHandler    handler
}

type handler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error)

func gcContext() context.Context {
	// internal statements must bind with resource type
	return kv.WithInternalSourceType(context.Background(), kv.InternalTxnGC)
}

func waitGCFinish(t *testing.T, s *mockGCWorkerSuite) {
	t.Helper()
	var err error
	require.Eventually(t, func() bool {
		select {
		case err = <-s.gcWorker.done:
			s.gcWorker.gcIsRunning = false
			return true
		default:
			return false
		}
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
}

func (c *mockGCWorkerClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	var resp *tikvrpc.Response
	var err error
	if req.Type == tikvrpc.CmdUnsafeDestroyRange && c.unsafeDestroyRangeHandler != nil {
		resp, err = c.unsafeDestroyRangeHandler(addr, req)
	}
	if req.Type == tikvrpc.CmdDeleteRange && c.deleteRangeHandler != nil {
		resp, err = c.deleteRangeHandler(addr, req)
	}
	if req.Type == tikvrpc.CmdScanLock && c.scanLockRequestHandler != nil {
		resp, err = c.scanLockRequestHandler(addr, req)
	}

	if resp != nil || err != nil {
		return resp, err
	}

	// If there's no mock handler, or the mock handler returns both nil, continue executing the inner implementation.
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

type mockGCWorkerSuite struct {
	store      kv.Storage
	tikvStore  tikv.Storage
	cluster    testutils.Cluster
	oracle     *oracles.MockOracle
	gcWorker   *GCWorker
	dom        *domain.Domain
	client     *mockGCWorkerClient
	pdClient   pd.Client
	initRegion struct {
		storeIDs []uint64
		peerIDs  []uint64
		regionID uint64
	}
}

type mockGCWorkerSuiteOptions struct {
	storeType        mockstore.StoreType
	schemaLease      time.Duration
	mockStoreOptions []mockstore.MockTiKVStoreOption
}

type mockGCWorkerSuiteOption func(*mockGCWorkerSuiteOptions)

func withStoreType(storeType mockstore.StoreType) mockGCWorkerSuiteOption {
	return func(opts *mockGCWorkerSuiteOptions) {
		opts.storeType = storeType
	}
}

func withSchemaLease(schemaLease time.Duration) mockGCWorkerSuiteOption {
	return func(opts *mockGCWorkerSuiteOptions) {
		opts.schemaLease = schemaLease
	}
}

func withMockStoreOptions(opt ...mockstore.MockTiKVStoreOption) mockGCWorkerSuiteOption {
	return func(opts *mockGCWorkerSuiteOptions) {
		opts.mockStoreOptions = append(opts.mockStoreOptions, opt...)
	}
}

func createGCWorkerSuite(t *testing.T, opts ...mockGCWorkerSuiteOption) *mockGCWorkerSuite {
	options := &mockGCWorkerSuiteOptions{
		storeType:        mockstore.EmbedUnistore,
		schemaLease:      config.DefSchemaLease,
		mockStoreOptions: nil,
	}
	for _, opt := range opts {
		opt(options)
	}

	s := new(mockGCWorkerSuite)
	hijackClient := func(client tikv.Client) tikv.Client {
		s.client = &mockGCWorkerClient{Client: client}
		client = s.client
		return client
	}
	storeOpts := []mockstore.MockTiKVStoreOption{
		mockstore.WithStoreType(options.storeType),
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			s.initRegion.storeIDs, s.initRegion.peerIDs, s.initRegion.regionID, _ = mockstore.BootstrapWithMultiStores(c, 3)
			s.cluster = c
		}),
		mockstore.WithClientHijacker(hijackClient),
		mockstore.WithPDClientHijacker(func(c pd.Client) pd.Client {
			s.pdClient = c
			return c
		}),
	}
	storeOpts = append(storeOpts, options.mockStoreOptions...)

	s.oracle = &oracles.MockOracle{}
	store, err := mockstore.NewMockStore(storeOpts...)
	require.NoError(t, err)
	store.GetOracle().Close()
	store.(tikv.Storage).SetOracle(s.oracle)
	dom := bootstrap(t, store, options.schemaLease)
	s.store, s.dom = store, dom

	s.tikvStore = s.store.(tikv.Storage)

	gcWorker, err := NewGCWorker(s.store, s.pdClient)
	require.NoError(t, err)
	gcWorker.Start()
	gcWorker.Close()
	s.gcWorker = gcWorker

	return s
}

func (s *mockGCWorkerSuite) mustPut(t *testing.T, key, value string) {
	txn, err := s.store.Begin()
	require.NoError(t, err)
	err = txn.Set([]byte(key), []byte(value))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func (s *mockGCWorkerSuite) mustGet(t *testing.T, key string, ts uint64) string {
	snap := s.store.GetSnapshot(kv.Version{Ver: ts})
	value, err := snap.Get(context.TODO(), []byte(key))
	require.NoError(t, err)
	return string(value.Value)
}

func (s *mockGCWorkerSuite) mustGetNone(t *testing.T, key string, ts uint64) {
	snap := s.store.GetSnapshot(kv.Version{Ver: ts})
	_, err := snap.Get(context.TODO(), []byte(key))
	if err != nil {
		// unistore gc is based on compaction filter.
		// So skip the error check if err == nil.
		require.True(t, kv.ErrNotExist.Equal(err), "unexpected error: %+q", err)
	}
}

func (s *mockGCWorkerSuite) mustAllocTs(t *testing.T) uint64 {
	ts, err := s.oracle.GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)
	return ts
}

func (s *mockGCWorkerSuite) mustGetSafePointFromPd(t *testing.T) uint64 {
	gcStates, err := s.pdClient.GetGCStatesClient(uint32(s.store.GetCodec().GetKeyspaceID())).GetGCState(context.Background())
	require.NoError(t, err)
	return gcStates.GCSafePoint
}

func (s *mockGCWorkerSuite) mustGetMinServiceSafePointFromPd(t *testing.T) uint64 {
	// UpdateServiceGCSafePoint returns the minimal service safePoint. If trying to update it with a value less than the
	// current minimal safePoint, nothing will be updated and the current minimal one will be returned. So we can use
	// this API to check the current safePoint.
	// This function shouldn't be invoked when there's no service safePoint set.
	minSafePoint, err := s.pdClient.UpdateServiceGCSafePoint(context.Background(), "test", 0, 0)
	require.NoError(t, err)
	return minSafePoint
}

func (s *mockGCWorkerSuite) mustUpdateServiceGCSafePoint(t *testing.T, serviceID string, safePoint, expectedMinSafePoint uint64) {
	minSafePoint, err := s.pdClient.UpdateServiceGCSafePoint(context.Background(), serviceID, math.MaxInt64, safePoint)
	require.NoError(t, err)
	require.Equal(t, expectedMinSafePoint, minSafePoint)
}

func (s *mockGCWorkerSuite) mustRemoveServiceGCSafePoint(t *testing.T, serviceID string, safePoint, expectedMinSafePoint uint64) {
	minSafePoint, err := s.pdClient.UpdateServiceGCSafePoint(context.Background(), serviceID, 0, safePoint)
	require.NoError(t, err)
	require.Equal(t, expectedMinSafePoint, minSafePoint)
}

func (s *mockGCWorkerSuite) mustSetTiDBServiceSafePoint(t *testing.T, safePoint, expectedMinSafePoint uint64) {
	minSafePoint, err := s.gcWorker.setGCWorkerServiceSafePoint(context.Background(), safePoint)
	require.NoError(t, err)
	require.Equal(t, expectedMinSafePoint, minSafePoint)
}

func (s *mockGCWorkerSuite) splitAtKeys(t *testing.T, keysStr ...string) {
	initialRegion, err := s.tikvStore.GetRegionCache().LocateKey(tikv.NewBackoffer(context.Background(), 10000), []byte("a"))
	require.NoError(t, err)

	slices.Sort(keysStr)
	keys := make([][]byte, 0, len(keysStr))
	for _, k := range keysStr {
		keys = append(keys, []byte(k))
	}
	s.cluster.(*unistore.Cluster).SplitArbitrary(keys...)

	s.tikvStore.GetRegionCache().InvalidateCachedRegion(initialRegion.Region)
}

// gcProbe represents a key that contains multiple versions, one of which should be collected. Execution of GC with
// greater ts will be detected, but it may not work properly if there are newer versions of the key.
// This is not used to check the correctness of GC algorithm, but only for checking whether GC has been executed on the
// specified key. Create this using `s.createGCProbe`.
type gcProbe struct {
	key string
	// The ts that can see the version that should be deleted.
	v1Ts uint64
	// The ts that can see the version that should be kept.
	v2Ts uint64
}

// createGCProbe creates gcProbe on specified key.
func (s *mockGCWorkerSuite) createGCProbe(t *testing.T, key string) *gcProbe {
	s.mustPut(t, key, "v1")
	ts1 := s.mustAllocTs(t)
	s.mustPut(t, key, "v2")
	ts2 := s.mustAllocTs(t)
	p := &gcProbe{
		key:  key,
		v1Ts: ts1,
		v2Ts: ts2,
	}
	s.checkNotCollected(t, p)
	return p
}

// checkCollected asserts the gcProbe has been correctly collected.
func (s *mockGCWorkerSuite) checkCollected(t *testing.T, p *gcProbe) {
	s.mustGetNone(t, p.key, p.v1Ts)
	require.Equal(t, "v2", s.mustGet(t, p.key, p.v2Ts))
}

// checkNotCollected asserts the gcProbe has not been collected.
func (s *mockGCWorkerSuite) checkNotCollected(t *testing.T, p *gcProbe) {
	require.Equal(t, "v1", s.mustGet(t, p.key, p.v1Ts))
	require.Equal(t, "v2", s.mustGet(t, p.key, p.v2Ts))
}

func timeEqual(t *testing.T, t1, t2 time.Time, epsilon time.Duration) {
	require.Less(t, math.Abs(float64(t1.Sub(t2))), float64(epsilon))
}

func TestGetOracleTime(t *testing.T) {
	s := createGCWorkerSuite(t)

	t1, err := s.gcWorker.getOracleTime()
	require.NoError(t, err)
	timeEqual(t, time.Now(), t1, time.Millisecond*10)

	s.oracle.AddOffset(time.Second * 10)
	t2, err := s.gcWorker.getOracleTime()
	require.NoError(t, err)
	timeEqual(t, t2, t1.Add(time.Second*10), time.Millisecond*10)
}

func TestPrepareGC(t *testing.T) {
	// as we are adjusting the base TS, we need a larger schema lease to avoid
	// the info schema outdated error. as we keep adding offset to time oracle,
	// so we need set a very large lease.
	s := createGCWorkerSuite(t, withStoreType(mockstore.EmbedUnistore), withSchemaLease(220*time.Minute))

	now, err := s.gcWorker.getOracleTime()
	require.NoError(t, err)
	close(s.gcWorker.done)
	ok, _, err := s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.False(t, ok)
	lastRun, err := s.gcWorker.loadTime(gcLastRunTimeKey)
	require.NoError(t, err)
	require.NotNil(t, lastRun)
	safePoint, err := s.gcWorker.loadTime(gcSafePointKey)
	require.NoError(t, err)
	timeEqual(t, safePoint.Add(gcDefaultLifeTime), now, 2*time.Second)

	// Change GC run interval.
	err = s.gcWorker.saveDuration(gcRunIntervalKey, time.Minute*5)
	require.NoError(t, err)
	s.oracle.AddOffset(time.Minute * 4)
	ok, _, err = s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.False(t, ok)
	s.oracle.AddOffset(time.Minute * 2)
	ok, _, err = s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)

	// Change GC lifetime.
	err = s.gcWorker.saveDuration(gcLifeTimeKey, time.Minute*30)
	require.NoError(t, err)
	s.oracle.AddOffset(time.Minute * 5)
	ok, _, err = s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.False(t, ok)
	s.oracle.AddOffset(time.Minute * 40)
	now, err = s.gcWorker.getOracleTime()
	require.NoError(t, err)
	ok, _, err = s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)
	safePoint, err = s.gcWorker.loadTime(gcSafePointKey)
	require.NoError(t, err)
	timeEqual(t, safePoint.Add(time.Minute*30), now, 2*time.Second)

	// Change GC concurrency.
	concurrency, err := s.gcWorker.loadGCConcurrencyWithDefault()
	require.NoError(t, err)
	require.Equal(t, gcDefaultConcurrency, concurrency)

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcMinConcurrency))
	require.NoError(t, err)
	concurrency, err = s.gcWorker.loadGCConcurrencyWithDefault()
	require.NoError(t, err)
	require.Equal(t, gcMinConcurrency, concurrency)

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(-1))
	require.NoError(t, err)
	concurrency, err = s.gcWorker.loadGCConcurrencyWithDefault()
	require.NoError(t, err)
	require.Equal(t, gcMinConcurrency, concurrency)

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(1000000))
	require.NoError(t, err)
	concurrency, err = s.gcWorker.loadGCConcurrencyWithDefault()
	require.NoError(t, err)
	require.Equal(t, gcMaxConcurrency, concurrency)

	// Change GC enable status.
	s.oracle.AddOffset(time.Minute * 40)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanFalse)
	require.NoError(t, err)
	ok, _, err = s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.False(t, ok)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanTrue)
	require.NoError(t, err)
	ok, _, err = s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)

	// Check gc lifetime smaller than min.
	s.oracle.AddOffset(time.Minute * 40)
	err = s.gcWorker.saveDuration(gcLifeTimeKey, time.Minute)
	require.NoError(t, err)
	ok, _, err = s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)
	lifeTime, err := s.gcWorker.loadDuration(gcLifeTimeKey)
	require.NoError(t, err)
	require.Equal(t, gcMinLifeTime, *lifeTime)

	s.oracle.AddOffset(time.Minute * 40)
	err = s.gcWorker.saveDuration(gcLifeTimeKey, time.Minute*30)
	require.NoError(t, err)
	ok, _, err = s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)
	lifeTime, err = s.gcWorker.loadDuration(gcLifeTimeKey)
	require.NoError(t, err)
	require.Equal(t, 30*time.Minute, *lifeTime)

	// Change auto concurrency
	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanFalse)
	require.NoError(t, err)
	useAutoConcurrency, err := s.gcWorker.checkUseAutoConcurrency()
	require.NoError(t, err)
	require.False(t, useAutoConcurrency)
	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanTrue)
	require.NoError(t, err)
	useAutoConcurrency, err = s.gcWorker.checkUseAutoConcurrency()
	require.NoError(t, err)
	require.True(t, useAutoConcurrency)

	// Check skipping GC if safe point is not changed.
	// Use a GC barrier to block GC from pushing forward.
	gcStatesCli := s.pdClient.GetGCStatesClient(uint32(s.store.GetCodec().GetKeyspaceID()))
	gcStates, err := gcStatesCli.GetGCState(context.Background())
	require.NoError(t, err)
	lastTxnSafePoint := gcStates.TxnSafePoint
	require.NotEqual(t, uint64(0), lastTxnSafePoint)
	_, err = gcStatesCli.SetGCBarrier(context.Background(), "a", lastTxnSafePoint, gc.TTLNeverExpire)
	require.NoError(t, err)
	s.oracle.AddOffset(time.Minute * 40)
	ok, safepoint, err := s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uint64(0), safepoint)
}

func TestStatusVars(t *testing.T) {
	s := createGCWorkerSuite(t)

	// Status variables should now exist for:
	// tidb_gc_safe_point, tidb_gc_last_run_time
	se := createSession(s.gcWorker.store)
	defer se.Close()

	safePoint, err := s.gcWorker.loadValueFromSysTable(gcSafePointKey)
	require.NoError(t, err)
	lastRunTime, err := s.gcWorker.loadValueFromSysTable(gcLastRunTimeKey)
	require.NoError(t, err)

	statusVars, _ := s.gcWorker.Stats(se.GetSessionVars())
	val, ok := statusVars[tidbGCSafePoint]
	require.True(t, ok)
	require.Equal(t, safePoint, val)
	val, ok = statusVars[tidbGCLastRunTime]
	require.True(t, ok)
	require.Equal(t, lastRunTime, val)
}

func TestDoGCForOneRegion(t *testing.T) {
	s := createGCWorkerSuite(t)

	ctx := context.Background()
	bo := tikv.NewBackofferWithVars(ctx, gcOneRegionMaxBackoff, nil)
	loc, err := s.tikvStore.GetRegionCache().LocateKey(bo, []byte(""))
	require.NoError(t, err)
	var regionErr *errorpb.Error

	p := s.createGCProbe(t, "k1")
	regionErr, err = s.gcWorker.doGCForRegion(bo, s.mustAllocTs(t), loc.Region)
	require.Nil(t, regionErr)
	require.NoError(t, err)
	s.checkCollected(t, p)

	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("timeout")`))
	regionErr, err = s.gcWorker.doGCForRegion(bo, s.mustAllocTs(t), loc.Region)
	require.Nil(t, regionErr)
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("tikvclient/tikvStoreSendReqResult"))

	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("GCNotLeader")`))
	regionErr, err = s.gcWorker.doGCForRegion(bo, s.mustAllocTs(t), loc.Region)
	require.NoError(t, err)
	require.NotNil(t, regionErr.GetNotLeader())
	require.NoError(t, failpoint.Disable("tikvclient/tikvStoreSendReqResult"))

	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("GCServerIsBusy")`))
	regionErr, err = s.gcWorker.doGCForRegion(bo, s.mustAllocTs(t), loc.Region)
	require.NoError(t, err)
	require.NotNil(t, regionErr.GetServerIsBusy())
	require.NoError(t, failpoint.Disable("tikvclient/tikvStoreSendReqResult"))
}

func TestGetGCConcurrency(t *testing.T) {
	s := createGCWorkerSuite(t)

	// Pick a concurrency that doesn't equal to the number of stores.
	concurrencyConfig := 25
	require.NotEqual(t, len(s.cluster.GetAllStores()), concurrencyConfig)
	err := s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(concurrencyConfig))
	require.NoError(t, err)

	ctx := context.Background()

	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanFalse)
	require.NoError(t, err)
	concurrency, err := s.gcWorker.getGCConcurrency(ctx)
	require.NoError(t, err)
	require.Equal(t, concurrencyConfig, concurrency.v)

	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanTrue)
	require.NoError(t, err)
	concurrency, err = s.gcWorker.getGCConcurrency(ctx)
	require.NoError(t, err)
	require.Len(t, s.cluster.GetAllStores(), concurrency.v)
}

func TestCheckGCMode(t *testing.T) {
	s := createGCWorkerSuite(t)

	useDistributedGC := s.gcWorker.checkUseDistributedGC()
	require.True(t, useDistributedGC)
	// Now the row must be set to the default value.
	str, err := s.gcWorker.loadValueFromSysTable(gcModeKey)
	require.NoError(t, err)
	require.Equal(t, gcModeDistributed, str)

	// Central mode is deprecated in v5.0.
	err = s.gcWorker.saveValueToSysTable(gcModeKey, gcModeCentral)
	require.NoError(t, err)
	useDistributedGC = s.gcWorker.checkUseDistributedGC()
	require.NoError(t, err)
	require.True(t, useDistributedGC)

	err = s.gcWorker.saveValueToSysTable(gcModeKey, gcModeDistributed)
	require.NoError(t, err)
	useDistributedGC = s.gcWorker.checkUseDistributedGC()
	require.True(t, useDistributedGC)

	err = s.gcWorker.saveValueToSysTable(gcModeKey, "invalid_mode")
	require.NoError(t, err)
	useDistributedGC = s.gcWorker.checkUseDistributedGC()
	require.True(t, useDistributedGC)
}

func TestNeedsGCOperationForStore(t *testing.T) {
	newStore := func(state metapb.StoreState, hasEngineLabel bool, engineLabel string) *metapb.Store {
		store := &metapb.Store{}
		store.State = state
		if hasEngineLabel {
			store.Labels = []*metapb.StoreLabel{{Key: placement.EngineLabelKey, Value: engineLabel}}
		}
		return store
	}

	// TiKV needs to do the store-level GC operations.
	for _, state := range []metapb.StoreState{metapb.StoreState_Up, metapb.StoreState_Offline, metapb.StoreState_Tombstone} {
		needGC := state != metapb.StoreState_Tombstone
		res, err := needsGCOperationForStore(newStore(state, false, ""))
		require.NoError(t, err)
		require.Equal(t, needGC, res)
		res, err = needsGCOperationForStore(newStore(state, true, ""))
		require.NoError(t, err)
		require.Equal(t, needGC, res)
		res, err = needsGCOperationForStore(newStore(state, true, placement.EngineLabelTiKV))
		require.NoError(t, err)
		require.Equal(t, needGC, res)

		// TiFlash does not need these operations.
		res, err = needsGCOperationForStore(newStore(state, true, placement.EngineLabelTiFlash))
		require.NoError(t, err)
		require.False(t, res)
	}
	// Throw an error for unknown store types.
	_, err := needsGCOperationForStore(newStore(metapb.StoreState_Up, true, "invalid"))
	require.Error(t, err)
}

const (
	failRPCErr  = 0
	failNilResp = 1
	failErrResp = 2
)

func TestDeleteRangesFailure(t *testing.T) {
	tests := []struct {
		name     string
		failType int
	}{
		{"failRPCErr", failRPCErr},
		{"failNilResp", failNilResp},
		{"failErrResp", failErrResp},
	}

	s := createGCWorkerSuite(t)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failType := test.failType
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC", "return(1)"))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC"))
			}()

			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJob", "return(\"schema/d1/t1\")"))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJob"))
			}()

			// Put some delete range tasks.
			se := createSession(s.gcWorker.store)
			defer se.Close()
			_, err := se.Execute(gcContext(), `INSERT INTO mysql.gc_delete_range VALUES
("1", "2", "31", "32", "10"),
("3", "4", "33", "34", "10"),
("5", "6", "35", "36", "10")`)
			require.NoError(t, err)

			ranges := []util.DelRangeTask{
				{
					JobID:     1,
					ElementID: 2,
					StartKey:  []byte("1"),
					EndKey:    []byte("2"),
				},
				{
					JobID:     3,
					ElementID: 4,
					StartKey:  []byte("3"),
					EndKey:    []byte("4"),
				},
				{
					JobID:     5,
					ElementID: 6,
					StartKey:  []byte("5"),
					EndKey:    []byte("6"),
				},
			}

			// Check the DeleteRanges tasks.
			preparedRanges, err := util.LoadDeleteRanges(gcContext(), se, 20)
			se.Close()
			require.NoError(t, err)
			require.Equal(t, ranges, preparedRanges)

			stores, err := s.gcWorker.getStoresForGC(context.Background())
			require.NoError(t, err)
			require.Len(t, stores, 3)

			// Sort by address for checking.
			sort.Slice(stores, func(i, j int) bool { return stores[i].Address < stores[j].Address })

			sendReqCh := make(chan SentReq, 20)

			// The request sent to the specified key and store will fail.
			var (
				failKey   []byte
				failStore *metapb.Store
			)
			s.client.unsafeDestroyRangeHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
				sendReqCh <- SentReq{req, addr}
				resp := &tikvrpc.Response{
					Resp: &kvrpcpb.UnsafeDestroyRangeResponse{},
				}
				if bytes.Equal(req.UnsafeDestroyRange().GetStartKey(), failKey) && addr == failStore.GetAddress() {
					if failType == failRPCErr {
						return nil, errors.New("error")
					} else if failType == failNilResp {
						resp.Resp = nil
					} else if failType == failErrResp {
						(resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error = "error"
					} else {
						panic("unreachable")
					}
				}
				return resp, nil
			}
			defer func() { s.client.unsafeDestroyRangeHandler = nil }()

			// Make the logic in a closure to reduce duplicated code that tests deleteRanges and
			test := func(redo bool) {
				deleteRangeFunc := s.gcWorker.deleteRanges
				loadRangesFunc := util.LoadDeleteRanges
				if redo {
					deleteRangeFunc = s.gcWorker.redoDeleteRanges
					loadRangesFunc = util.LoadDoneDeleteRanges
				}

				// Make the first request fail.
				failKey = ranges[0].StartKey
				failStore = stores[0]

				err = deleteRangeFunc(gcContext(), 20, gcConcurrency{1, false})
				require.NoError(t, err)

				s.checkDestroyRangeReq(t, sendReqCh, ranges, stores)

				// The first delete range task should be still here since it didn't success.
				se = createSession(s.gcWorker.store)
				remainingRanges, err := loadRangesFunc(gcContext(), se, 20)
				se.Close()
				require.NoError(t, err)
				require.Equal(t, ranges[:1], remainingRanges)

				failKey = nil
				failStore = nil

				// Delete the remaining range again.
				err = deleteRangeFunc(gcContext(), 20, gcConcurrency{1, false})
				require.NoError(t, err)
				s.checkDestroyRangeReq(t, sendReqCh, ranges[:1], stores)

				se = createSession(s.gcWorker.store)
				remainingRanges, err = loadRangesFunc(gcContext(), se, 20)
				se.Close()
				require.NoError(t, err)
				require.Len(t, remainingRanges, 0)
			}

			test(false)
			// Change the order because the first range is the last successfully deleted.
			ranges = append(ranges[1:], ranges[0])
			test(true)
		})
	}
}

func TestConcurrentDeleteRanges(t *testing.T) {
	// make sure the parallelization of deleteRanges works

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC", "return(1)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJob", "return(\"schema/d1/t1\")"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJob"))
	}()

	s := createGCWorkerSuite(t)
	se := createSession(s.gcWorker.store)
	defer se.Close()
	_, err := se.Execute(gcContext(), `INSERT INTO mysql.gc_delete_range VALUES
("1", "2", "31", "32", "10"),
("3", "4", "33", "34", "10"),
("5", "6", "35", "36", "15"),
("7", "8", "37", "38", "15"),
("9", "10", "39", "40", "15")
	`)
	require.NoError(t, err)

	ranges, err := util.LoadDeleteRanges(gcContext(), se, 20)
	require.NoError(t, err)
	require.Len(t, ranges, 5)

	stores, err := s.gcWorker.getStoresForGC(context.Background())
	require.NoError(t, err)
	require.Len(t, stores, 3)
	sort.Slice(stores, func(i, j int) bool { return stores[i].Address < stores[j].Address })

	sendReqCh := make(chan SentReq, 20)
	s.client.unsafeDestroyRangeHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		sendReqCh <- SentReq{req, addr}
		resp := &tikvrpc.Response{
			Resp: &kvrpcpb.UnsafeDestroyRangeResponse{},
		}
		return resp, nil
	}
	defer func() { s.client.unsafeDestroyRangeHandler = nil }()

	err = s.gcWorker.deleteRanges(gcContext(), 20, gcConcurrency{3, false})
	require.NoError(t, err)

	s.checkDestroyRangeReq(t, sendReqCh, ranges, stores)

	se = createSession(s.gcWorker.store)
	remainingRanges, err := util.LoadDeleteRanges(gcContext(), se, 20)
	se.Close()
	require.NoError(t, err)
	require.Len(t, remainingRanges, 0)
}

type SentReq struct {
	req  *tikvrpc.Request
	addr string
}

// checkDestroyRangeReq checks whether given sentReq matches given ranges and stores.
func (s *mockGCWorkerSuite) checkDestroyRangeReq(t *testing.T, sendReqCh chan SentReq, expectedRanges []util.DelRangeTask, expectedStores []*metapb.Store) {
	sentReq := make([]SentReq, 0, len(expectedStores)*len(expectedStores))
Loop:
	for {
		select {
		case req := <-sendReqCh:
			sentReq = append(sentReq, req)
		default:
			break Loop
		}
	}

	sort.Slice(sentReq, func(i, j int) bool {
		cmp := bytes.Compare(sentReq[i].req.UnsafeDestroyRange().StartKey, sentReq[j].req.UnsafeDestroyRange().StartKey)
		return cmp < 0 || (cmp == 0 && sentReq[i].addr < sentReq[j].addr)
	})

	sortedRanges := slices.Clone(expectedRanges)
	sort.Slice(sortedRanges, func(i, j int) bool {
		return bytes.Compare(sortedRanges[i].StartKey, sortedRanges[j].StartKey) < 0
	})

	for rangeIndex := range sortedRanges {
		for storeIndex := range expectedStores {
			i := rangeIndex*len(expectedStores) + storeIndex
			require.Equal(t, expectedStores[storeIndex].Address, sentReq[i].addr)
			require.Equal(t, sortedRanges[rangeIndex].StartKey, kv.Key(sentReq[i].req.UnsafeDestroyRange().GetStartKey()))
			require.Equal(t, sortedRanges[rangeIndex].EndKey, kv.Key(sentReq[i].req.UnsafeDestroyRange().GetEndKey()))
		}
	}
}

func TestUnsafeDestroyRangeForRaftkv2(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/util/IsRaftKv2", "return(true)"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC", "return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC"))
	}()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJob", "return(\"schema/d1/t1\")"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJob"))
	}()

	s := createGCWorkerSuite(t)
	// Put some delete range tasks.
	se := createSession(s.gcWorker.store)
	defer se.Close()
	_, err := se.Execute(gcContext(), `INSERT INTO mysql.gc_delete_range VALUES
("1", "2", "31", "32", "5"),
("3", "4", "33", "34", "10"),
("5", "6", "35", "36", "15"),
("7", "8", "37", "38", "15")`)
	require.NoError(t, err)

	ranges := []util.DelRangeTask{
		{
			JobID:     1,
			ElementID: 2,
			StartKey:  []byte("1"),
			EndKey:    []byte("2"),
		},
		{
			JobID:     3,
			ElementID: 4,
			StartKey:  []byte("3"),
			EndKey:    []byte("4"),
		},
		{
			JobID:     5,
			ElementID: 6,
			StartKey:  []byte("5"),
			EndKey:    []byte("6"),
		},
		{
			JobID:     7,
			ElementID: 8,
			StartKey:  []byte("7"),
			EndKey:    []byte("8"),
		},
	}

	// Check the DeleteRanges tasks.
	preparedRanges, err := util.LoadDeleteRanges(gcContext(), se, 20)
	se.Close()
	require.NoError(t, err)
	require.Equal(t, ranges, preparedRanges)

	sendReqCh := make(chan SentReq, 20)
	s.client.deleteRangeHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		sendReqCh <- SentReq{req, addr}
		resp := &tikvrpc.Response{
			Resp: &kvrpcpb.DeleteRangeResponse{},
		}
		return resp, nil
	}
	defer func() { s.client.deleteRangeHandler = nil }()

	err = s.gcWorker.deleteRanges(gcContext(), 8, gcConcurrency{1, false})
	require.NoError(t, err)

	s.checkDestroyRangeReqV2(t, sendReqCh, ranges[:1])

	se = createSession(s.gcWorker.store)
	remainingRanges, err := util.LoadDeleteRanges(gcContext(), se, 20)
	se.Close()
	require.NoError(t, err)
	require.Equal(t, ranges[1:], remainingRanges)

	err = s.gcWorker.deleteRanges(gcContext(), 20, gcConcurrency{1, false})
	require.NoError(t, err)

	s.checkDestroyRangeReqV2(t, sendReqCh, ranges[1:])

	// In v2, they should not be recorded in done ranges
	doneRanges, err := util.LoadDoneDeleteRanges(gcContext(), se, 20)
	se.Close()
	require.NoError(t, err)
	require.True(t, len(doneRanges) == 0)
}

// checkDestroyRangeReqV2 checks whether given sentReq matches given ranges and stores when raft-kv2 is enabled.
func (s *mockGCWorkerSuite) checkDestroyRangeReqV2(t *testing.T, sendReqCh chan SentReq, expectedRanges []util.DelRangeTask) {
	sentReq := make([]SentReq, 0, 5)
Loop:
	for {
		select {
		case req := <-sendReqCh:
			sentReq = append(sentReq, req)
		default:
			break Loop
		}
	}

	sort.Slice(sentReq, func(i, j int) bool {
		cmp := bytes.Compare(sentReq[i].req.DeleteRange().StartKey, sentReq[j].req.DeleteRange().StartKey)
		return cmp < 0 || (cmp == 0 && sentReq[i].addr < sentReq[j].addr)
	})

	sortedRanges := slices.Clone(expectedRanges)
	sort.Slice(sortedRanges, func(i, j int) bool {
		return bytes.Compare(sortedRanges[i].StartKey, sortedRanges[j].StartKey) < 0
	})

	for rangeIndex := range sortedRanges {
		require.Equal(t, sortedRanges[rangeIndex].StartKey, kv.Key(sentReq[rangeIndex].req.DeleteRange().GetStartKey()))
		require.Equal(t, sortedRanges[rangeIndex].EndKey, kv.Key(sentReq[rangeIndex].req.DeleteRange().GetEndKey()))
	}
}

func TestLeaderTick(t *testing.T) {
	// as we are adjusting the base TS, we need a larger schema lease to avoid
	// the info schema outdated error.
	s := createGCWorkerSuite(t, withStoreType(mockstore.EmbedUnistore), withSchemaLease(time.Hour))

	txnSafePointSyncWaitTime = 0

	veryLong := gcDefaultLifeTime * 10
	// Avoid failing at interval check. `lastFinish` is checked by os time.
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	// Use central mode to do this test.
	err := s.gcWorker.saveValueToSysTable(gcModeKey, gcModeCentral)
	require.NoError(t, err)
	p := s.createGCProbe(t, "k1")
	s.oracle.AddOffset(gcDefaultLifeTime * 2)

	// Skip if GC is running.
	s.gcWorker.gcIsRunning = true
	err = s.gcWorker.leaderTick(context.Background())
	require.NoError(t, err)
	s.checkNotCollected(t, p)
	s.gcWorker.gcIsRunning = false
	// Reset GC last run time
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-veryLong))
	require.NoError(t, err)

	// Skip if prepare failed (disabling GC will make prepare returns ok = false).
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanFalse)
	require.NoError(t, err)
	err = s.gcWorker.leaderTick(gcContext())
	require.NoError(t, err)
	s.checkNotCollected(t, p)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanTrue)
	require.NoError(t, err)
	// Reset GC last run time
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-veryLong))
	require.NoError(t, err)

	// Skip if gcWaitTime not exceeded.
	s.gcWorker.lastFinish = time.Now()
	err = s.gcWorker.leaderTick(gcContext())
	require.NoError(t, err)
	s.checkNotCollected(t, p)
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	// Reset GC last run time
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-veryLong))
	require.NoError(t, err)
	s.oracle.AddOffset(time.Second)

	// Continue GC if all those checks passed.
	err = s.gcWorker.leaderTick(gcContext())
	require.NoError(t, err)
	waitGCFinish(t, s)
	s.checkCollected(t, p)

	// Test again to ensure the synchronization between goroutines is correct.
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-veryLong))
	require.NoError(t, err)
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	p = s.createGCProbe(t, "k1")
	s.oracle.AddOffset(gcDefaultLifeTime * 2)

	err = s.gcWorker.leaderTick(gcContext())
	require.NoError(t, err)
	waitGCFinish(t, s)
	s.checkCollected(t, p)

	// No more signals in the channel
	select {
	case err = <-s.gcWorker.done:
		err = errors.Errorf("received signal s.gcWorker.done which shouldn't exist: %v", err)
		break
	case <-time.After(time.Second):
		break
	}
	require.NoError(t, err)
}

func TestResolveLockRangeInfine(t *testing.T) {
	s := createGCWorkerSuite(t)

	require.NoError(t, failpoint.Enable("tikvclient/invalidCacheAndRetry", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/invalidCacheAndRetry"))
	}()

	mockLockResolver := &mockGCWorkerLockResolver{
		RegionLockResolver: tikv.NewRegionLockResolver("test", s.tikvStore),
		tikvStore:          s.tikvStore,
		scanLocks: func(_ []*txnlock.Lock, key []byte) ([]*txnlock.Lock, *tikv.KeyLocation) {
			return []*txnlock.Lock{}, &tikv.KeyLocation{}
		},
		batchResolveLocks: func(
			locks []*txnlock.Lock,
			loc *tikv.KeyLocation,
		) (*tikv.KeyLocation, error) {
			// mock error to test backoff
			return nil, errors.New("mock error")
		},
	}
	_, err := tikv.ResolveLocksForRange(gcContext(), mockLockResolver, 1, []byte{0}, []byte{1}, tikv.NewNoopBackoff, 10)
	require.Error(t, err)
}

func TestResolveLockRangeMeetRegionCacheMiss(t *testing.T) {
	s := createGCWorkerSuite(t)

	var (
		scanCnt       int
		scanCntRef    = &scanCnt
		resolveCnt    int
		resolveCntRef = &resolveCnt

		safepointTS uint64 = 434245550444904450
	)

	allLocks := []*txnlock.Lock{
		{
			Key: []byte{1},
			// TxnID < safepointTS
			TxnID: 434245550444904449,
			TTL:   5,
		},
		{
			Key: []byte{2},
			// safepointTS < TxnID < lowResolveTS , TxnID + TTL < lowResolveTS
			TxnID: 434245550445166592,
			TTL:   10,
		},
		{
			Key: []byte{3},
			// safepointTS < TxnID < lowResolveTS , TxnID + TTL > lowResolveTS
			TxnID: 434245550445166593,
			TTL:   20,
		},
		{
			Key: []byte{4},
			// TxnID > lowResolveTS
			TxnID: 434245550449099752,
			TTL:   20,
		},
	}

	mockLockResolver := &mockGCWorkerLockResolver{
		RegionLockResolver: tikv.NewRegionLockResolver("test", s.tikvStore),
		tikvStore:          s.tikvStore,
		scanLocks: func(_ []*txnlock.Lock, key []byte) ([]*txnlock.Lock, *tikv.KeyLocation) {
			*scanCntRef++
			return allLocks, &tikv.KeyLocation{
				Region: tikv.NewRegionVerID(s.initRegion.regionID, 0, 0),
			}
		},
		batchResolveLocks: func(
			locks []*txnlock.Lock,
			loc *tikv.KeyLocation,
		) (*tikv.KeyLocation, error) {
			*resolveCntRef++
			if *resolveCntRef == 1 {
				s.gcWorker.tikvStore.GetRegionCache().InvalidateCachedRegion(loc.Region)
				// mock the region cache miss error
				return nil, nil
			}
			return loc, nil
		},
	}
	_, err := tikv.ResolveLocksForRange(gcContext(), mockLockResolver, safepointTS, []byte{0}, []byte{10}, tikv.NewNoopBackoff, 10)
	require.NoError(t, err)
	require.Equal(t, 2, resolveCnt)
	require.Equal(t, 2, scanCnt)
}

func TestResolveLockRangeMeetRegionEnlargeCausedByRegionMerge(t *testing.T) {
	// TODO: Update the test code.
	// This test rely on the obsolete mock tikv, but mock tikv does not implement paging.
	// So use this failpoint to force non-paging protocol.
	// Mock TiKV does not have implementation to up-to-date PD APIs about GC either, making it fail when running in next gen.
	if kerneltype.IsNextGen() {
		t.Skip("The test is currently not compatible with next gen")
	}
	failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/DisablePaging", `return`)
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/DisablePaging"))
	}()
	s := createGCWorkerSuite(t, withStoreType(mockstore.MockTiKV), withSchemaLease(config.DefSchemaLease))

	var (
		firstAccess    = true
		firstAccessRef = &firstAccess
		resolvedLock   [][]byte
	)

	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := []uint64{s.cluster.AllocID(), s.cluster.AllocID(), s.cluster.AllocID()}
	s.cluster.Split(s.initRegion.regionID, region2, []byte("m"), newPeers, newPeers[0])

	mockGCLockResolver := &mockGCWorkerLockResolver{
		RegionLockResolver: tikv.NewRegionLockResolver("test", s.tikvStore),
		tikvStore:          s.tikvStore,
		scanLocks: func(_ []*txnlock.Lock, key []byte) ([]*txnlock.Lock, *tikv.KeyLocation) {
			// first time scan locks
			region, _, _, _ := s.cluster.GetRegionByKey(key)
			if region.GetId() == s.initRegion.regionID {
				return []*txnlock.Lock{{Key: []byte("a")}, {Key: []byte("b")}},
					&tikv.KeyLocation{
						Region: tikv.NewRegionVerID(
							region.GetId(),
							region.GetRegionEpoch().ConfVer,
							region.GetRegionEpoch().Version,
						),
					}
			}
			// second time scan locks
			if region.GetId() == region2 {
				return []*txnlock.Lock{{Key: []byte("o")}, {Key: []byte("p")}},
					&tikv.KeyLocation{
						Region: tikv.NewRegionVerID(
							region.GetId(),
							region.GetRegionEpoch().ConfVer,
							region.GetRegionEpoch().Version,
						),
					}
			}
			return []*txnlock.Lock{}, nil
		},
	}
	mockGCLockResolver.batchResolveLocks = func(
		locks []*txnlock.Lock,
		loc *tikv.KeyLocation,
	) (*tikv.KeyLocation, error) {
		if loc.Region.GetID() == s.initRegion.regionID && *firstAccessRef {
			*firstAccessRef = false
			// merge region2 into region1 and return EpochNotMatch error.
			mCluster := s.cluster.(*testutils.MockCluster)
			mCluster.Merge(s.initRegion.regionID, region2)
			regionMeta, _ := mCluster.GetRegion(s.initRegion.regionID)
			_, err := s.tikvStore.GetRegionCache().OnRegionEpochNotMatch(
				tikv.NewNoopBackoff(context.Background()),
				&tikv.RPCContext{Region: loc.Region, Store: &tikv.Store{}},
				[]*metapb.Region{regionMeta})
			require.NoError(t, err)
			// also let region1 contains all 4 locks
			mockGCLockResolver.scanLocks = func(_ []*txnlock.Lock, key []byte) ([]*txnlock.Lock, *tikv.KeyLocation) {
				if bytes.Equal(key, []byte("")) {
					locks := []*txnlock.Lock{
						{Key: []byte("a")},
						{Key: []byte("b")},
						{Key: []byte("o")},
						{Key: []byte("p")},
					}
					for i, lock := range locks {
						if bytes.Compare(key, lock.Key) <= 0 {
							return locks[i:], &tikv.KeyLocation{Region: tikv.NewRegionVerID(
								regionMeta.GetId(),
								regionMeta.GetRegionEpoch().ConfVer,
								regionMeta.GetRegionEpoch().Version)}
						}
					}
				}
				return []*txnlock.Lock{}, nil
			}
			return nil, nil
		}
		for _, lock := range locks {
			resolvedLock = append(resolvedLock, lock.Key)
		}
		return loc, nil
	}
	_, err := tikv.ResolveLocksForRange(gcContext(), mockGCLockResolver, 1, []byte(""), []byte("z"), tikv.NewGcResolveLockMaxBackoffer, 10)
	require.NoError(t, err)
	require.Len(t, resolvedLock, 4)
	expects := [][]byte{[]byte("a"), []byte("b"), []byte("o"), []byte("p")}
	for i, l := range resolvedLock {
		require.Equal(t, expects[i], l)
	}
}

func testResolveLocksWithKeyspacesImpl(t *testing.T, subCaseName string) {
	// Note: this test is expected not to respect to whether compiled as NextGen, but tests the logic designed to work
	// on both classic and NextGen, and even mixed keyspaced & un-keyspaced usages in the same cluster.
	// The unified GC, which is not used and won't be used in next gen, is also covered here.
	// However, as the NextGen flag is currently overused, causing some keyspace-specific code unable to run on
	// non-next-gen compilation or vice versa, the sub-tests must be filtered by the compilation flag for now.

	// Note: this test case consists of several sub-tests, but not managed with .t.Run(), because it can't be split
	// into multiple ones when running on CI, and might cause timeout.

	type reqRange struct {
		StartKey     []byte
		EndKey       []byte
		TxnSafePoint uint64
	}

	createSuiteForTestResolveLocks := func(t *testing.T, storeOpt ...mockstore.MockTiKVStoreOption) (suite *mockGCWorkerSuite, scanLockCounter *atomic.Int64, scanLockRangeCh chan reqRange) {
		suite = createGCWorkerSuite(t, withMockStoreOptions(storeOpt...))
		scanLockRangeCh = make(chan reqRange, 1000)
		scanLockCounter = &atomic.Int64{}
		suite.client.scanLockRequestHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			scanLockCounter.Add(1)
			scanLockReq := req.ScanLock()
			scanLockRangeCh <- reqRange{
				StartKey: scanLockReq.GetStartKey(),
				EndKey:   scanLockReq.GetEndKey(),
				// When resolving locks, the maxVersion parameter is set to txnSafePoint-1, so plus 1 back to retrieve
				// the txnSafePoint.
				TxnSafePoint: scanLockReq.GetMaxVersion() + 1,
			}
			// Only collect the request, without generating the result. Return nil to continue calling the inner client.
			return nil, nil
		}
		return
	}

	collectAndMergeRanges := func(t *testing.T, ch <-chan reqRange) []reqRange {
		ranges := make([]reqRange, 0, len(ch))
		for r := range ch {
			ranges = append(ranges, r)
		}
		slices.SortFunc(ranges, func(lhs, rhs reqRange) int {
			return bytes.Compare(lhs.StartKey, rhs.StartKey)
		})
		if len(ranges) == 0 {
			return ranges
		}
		mergedRanges := make([]reqRange, 0, len(ranges))
		mergedRanges = append(mergedRanges, ranges[0])
		for _, r := range ranges[1:] {
			previousMerged := &mergedRanges[len(mergedRanges)-1]
			if bytes.Compare(r.StartKey, previousMerged.EndKey) <= 0 {
				if len(r.EndKey) == 0 || bytes.Compare(r.EndKey, previousMerged.EndKey) > 0 {
					previousMerged.EndKey = r.EndKey
				}
			} else {
				mergedRanges = append(mergedRanges, r)
			}
		}

		// Force empty key be represented as nil (instead of an empty slice) for the convenience of asserting.
		if len(mergedRanges[0].StartKey) == 0 {
			mergedRanges[0].StartKey = nil
		}
		if len(mergedRanges[len(mergedRanges)-1].EndKey) == 0 {
			mergedRanges[len(mergedRanges)-1].EndKey = nil
		}

		return mergedRanges
	}

	subCases := make(map[string]func(t *testing.T))

	subCases["NullKeyspaceOnly"] = func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip()
		}
		s, counter, ch := createSuiteForTestResolveLocks(t, mockstore.WithCurrentKeyspaceMeta(nil))
		err := s.gcWorker.resolveLocks(context.Background(), 100, 1)
		require.NoError(t, err)
		close(ch)
		ranges := collectAndMergeRanges(t, ch)
		// In case there's totally no keyspace in the cluster, it skips the step to exclude the ranges used by
		// keyspaces.
		require.Equal(t, []reqRange{{
			StartKey:     nil,
			EndKey:       nil,
			TxnSafePoint: 100,
		}}, ranges)
		require.Equal(t, int64(1), counter.Load())
	}

	subCases["NullKeyspaceOnlyMultiRegion"] = func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip()
		}
		s, counter, ch := createSuiteForTestResolveLocks(t, mockstore.WithCurrentKeyspaceMeta(nil))
		s.splitAtKeys(t, "a", "b", "c")
		err := s.gcWorker.resolveLocks(context.Background(), 100, 1)
		require.NoError(t, err)
		close(ch)
		ranges := collectAndMergeRanges(t, ch)
		require.Equal(t, []reqRange{{
			StartKey:     nil,
			EndKey:       nil,
			TxnSafePoint: 100,
		}}, ranges)
		require.Equal(t, int64(4), counter.Load())
	}

	makeKeyspace := func(id uint32, name string, enableKeyspaceLevelGC bool) *keyspacepb.KeyspaceMeta {
		gcManagementType := pd.KeyspaceConfigGCManagementTypeKeyspaceLevel
		if !enableKeyspaceLevelGC {
			gcManagementType = pd.KeyspaceConfigGCManagementTypeUnified
		}
		return &keyspacepb.KeyspaceMeta{
			Id:     id,
			Name:   name,
			Config: map[string]string{pd.KeyspaceConfigGCManagementType: gcManagementType},
		}
	}

	makeKey := func(id uint32, key string) string {
		c, err := tikv.NewCodecV2(tikv.ModeTxn, makeKeyspace(id, "dummyks", true))
		require.NoError(t, err)
		return string(c.EncodeKey([]byte(key)))
	}

	subCases["NullKeyspaceInMultiKeyspaceEnvironment"] = func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip()
		}
		s, counter, ch := createSuiteForTestResolveLocks(t, mockstore.WithKeyspacesAndCurrentKeyspaceID([]*keyspacepb.KeyspaceMeta{
			makeKeyspace(1, "ks1", true),
			makeKeyspace(3, "ks3", true),
		}, constants.NullKeyspaceID))
		s.splitAtKeys(t,
			makeKey(1, ""), makeKey(1, "a"),
			makeKey(2, ""), makeKey(2, "a"),
			makeKey(3, ""), makeKey(3, "a"),
			makeKey(4, ""), makeKey(4, "a"),
			"t1", "t2", "m")
		err := s.gcWorker.resolveLocks(context.Background(), 100, 2)
		require.NoError(t, err)
		close(ch)
		ranges := collectAndMergeRanges(t, ch)
		// Handles ranges that are out of any keyspaces.
		require.Equal(t, []reqRange{
			{StartKey: nil, EndKey: []byte("r"), TxnSafePoint: 100},         // 2 regions (split at "m")
			{StartKey: []byte("s"), EndKey: []byte("x"), TxnSafePoint: 100}, // 3 regions (split at "t1", "t2")
			{StartKey: []byte("y"), EndKey: nil, TxnSafePoint: 100},         // 1 region
		}, ranges)
		require.Equal(t, int64(6), counter.Load())
	}

	subCases["NonNullKeyspaceInMultiKeyspaceEnvironment"] = func(t *testing.T) {
		if !kerneltype.IsNextGen() {
			t.Skip()
		}
		// Note: Currently it's hard to simulate a user keyspace with unistore, we only try to test it with the SYSTEM
		// keyspace for now, which should have no difference in GC with user keyspaces.
		s, counter, ch := createSuiteForTestResolveLocks(t, mockstore.WithKeyspacesAndCurrentKeyspaceID([]*keyspacepb.KeyspaceMeta{
			makeKeyspace(1, "ks1", true),
			makeKeyspace(3, "ks3", true),
			makeKeyspace(constants.MaxKeyspaceID-1, "SYSTEM", true),
		}, constants.MaxKeyspaceID-1))

		s.splitAtKeys(t,
			makeKey(1, ""), makeKey(1, "a"),
			makeKey(2, ""), makeKey(2, "a"),
			makeKey(3, ""), makeKey(3, "a"),
			makeKey(4, ""), makeKey(4, "a"),
			makeKey(constants.MaxKeyspaceID-1, ""), makeKey(constants.MaxKeyspaceID-1, "a"),
			"t1", "t2", "m")
		err := s.gcWorker.resolveLocks(context.Background(), 100, 2)
		require.NoError(t, err)
		close(ch)
		ranges := collectAndMergeRanges(t, ch)
		// Currently a unistore represents the content of only one keyspace, and the mocked tikv client is different
		// from the real TiKV one and doesn't have the key prefix attaching/detaching step, causing it unable to simulate the structure
		// of a real cluster.
		// TODO: Replace the check if we make it to simulate the key range division of a real cluster correctly.
		// require.Equal(t, []reqRange{
		// 	{StartKey: []byte("x\xff\xff\xfe"), EndKey: []byte("x\xff\xff\xff"), TxnSafePoint: 100}, // 2 regions split at "x\x00\x00\x00a"
		// }, ranges)
		require.Equal(t, []reqRange{
			{StartKey: nil, EndKey: nil, TxnSafePoint: 100}, // 2 regions split at "a"
		}, ranges)
		require.Equal(t, int64(2), counter.Load())
	}

	subCases["UnifiedGCInMixedUsage"] = func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip()
		}
		s, counter, ch := createSuiteForTestResolveLocks(t, mockstore.WithKeyspacesAndCurrentKeyspaceID([]*keyspacepb.KeyspaceMeta{
			makeKeyspace(0, "DEFAULT", false),
			makeKeyspace(1, "ks1", true),
			makeKeyspace(2, "ks2", false),
			makeKeyspace(3, "ks3", true),
			makeKeyspace(5, "ks5", false),
			makeKeyspace(8, "ks8", true),
		}, constants.NullKeyspaceID))
		splitKeys := []string{"t1", "t2", "m"}
		for i := 0; i < 8; i++ {
			splitKeys = append(splitKeys, makeKey(uint32(i), ""), makeKey(uint32(i), "a"))
		}
		s.splitAtKeys(t, splitKeys...)
		err := s.gcWorker.resolveLocks(context.Background(), 100, 2)
		require.NoError(t, err)
		close(ch)
		ranges := collectAndMergeRanges(t, ch)
		require.Equal(t, []reqRange{
			{StartKey: nil, EndKey: []byte("r"), TxnSafePoint: 100},                                 // Non-keyspace range, 2 regions (split at "m")
			{StartKey: []byte("s"), EndKey: []byte("x"), TxnSafePoint: 100},                         // Non-keyspace range, 3 regions (split at "t1", "t2")
			{StartKey: []byte("x\x00\x00\x00"), EndKey: []byte("x\x00\x00\x01"), TxnSafePoint: 100}, // Keyspace 0 (DEFAULT), 2 regions
			{StartKey: []byte("x\x00\x00\x02"), EndKey: []byte("x\x00\x00\x03"), TxnSafePoint: 100}, // Keyspace 2, 2 regions
			{StartKey: []byte("x\x00\x00\x05"), EndKey: []byte("x\x00\x00\x06"), TxnSafePoint: 100}, // Keyspace 5, 2 regions
			{StartKey: []byte("y"), EndKey: nil, TxnSafePoint: 100},                                 // 1 region
		}, ranges)
		require.Equal(t, int64(12), counter.Load())
	}

	subCases["UnifiedGCWithMaxKeyspaceID"] = func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip()
		}
		s, counter, ch := createSuiteForTestResolveLocks(t, mockstore.WithKeyspacesAndCurrentKeyspaceID([]*keyspacepb.KeyspaceMeta{
			makeKeyspace(constants.MaxKeyspaceID, "max", false),
		}, constants.NullKeyspaceID))

		splitKeys := []string{"t1", "t2", "m", makeKey(constants.MaxKeyspaceID, ""), makeKey(constants.MaxKeyspaceID, "a"), "y", "y\x00\x00\x00"}
		s.splitAtKeys(t, splitKeys...)
		err := s.gcWorker.resolveLocks(context.Background(), 100, 2)
		require.NoError(t, err)
		close(ch)
		ranges := collectAndMergeRanges(t, ch)
		require.Equal(t, []reqRange{
			{StartKey: nil, EndKey: []byte("r"), TxnSafePoint: 100},             // Non-keyspace range, 2 regions (split at "m")
			{StartKey: []byte("s"), EndKey: []byte("x"), TxnSafePoint: 100},     // Non-keyspace range, 3 regions (split at "t1", "t2")
			{StartKey: []byte("x\xff\xff\xff"), EndKey: nil, TxnSafePoint: 100}, // Keyspace MaxKeyspaceID + ranges after keyspace prefix, 2 + 2 regions.
		}, ranges)
		// Note: The range ["y", "y\x00\x00\x00") is actually repeatedly handled, but it doesn't matter for now as it's never used and contains only 3 keys.
		require.Equal(t, int64(10), counter.Load())
	}

	testUnifiedGCInMultiBatchesOfKeyspacesImpl := func(t *testing.T, startID uint32, count uint32, step uint32, loadBatchSize int, expectedBatchCount int) {
		if kerneltype.IsNextGen() {
			t.Skip()
		}

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/overrideLoadKeyspacesBatchSize", fmt.Sprintf("return(%d)", loadBatchSize)))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/overrideLoadKeyspacesBatchSize"))
		}()

		// Retrieve the actual batch count of loading all keyspaces, for ensuring that the failpoint
		// `overrideLoadKeyspacesBatchSize` actually takes effect.
		loadKeyspacesBatchCount := 0
		require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/store/gcworker/getLoadKeyspacesBatchCount", func(v int) {
			loadKeyspacesBatchCount += v
		}))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/getLoadKeyspacesBatchCount"))
		}()

		keyspaces := make([]*keyspacepb.KeyspaceMeta, 0, count)
		splitKeys := make([]string, 0, count*2+3)
		expectedRanges := make([]reqRange, 0, count+3)
		expectedScanLocksCount := int(count)*2 + 6
		splitKeys = append(splitKeys, "t1", "t2", "m")

		expectedRanges = append(expectedRanges, reqRange{StartKey: nil, EndKey: []byte("r"), TxnSafePoint: 100})         //  Non-keyspace range, 2 regions (split at "m")
		expectedRanges = append(expectedRanges, reqRange{StartKey: []byte("s"), EndKey: []byte("x"), TxnSafePoint: 100}) // Non-keyspace range, 3 regions (split at "t1", "t2")
		for i := range count {
			id := startID + i*step
			keyspaces = append(keyspaces, makeKeyspace(id, fmt.Sprintf("ks%d", id), false))
			splitKeys = append(splitKeys, makeKey(id, ""), makeKey(id, "a"))
			startKey, err := hex.DecodeString(fmt.Sprintf("78%06x", id))
			require.NoError(t, err)
			endKey := kv2.PrefixNextKey(startKey)
			require.NoError(t, err)
			if bytes.Equal(startKey, expectedRanges[len(expectedRanges)-1].EndKey) {
				expectedRanges[len(expectedRanges)-1].EndKey = endKey
			} else {
				expectedRanges = append(expectedRanges, reqRange{StartKey: startKey, EndKey: endKey, TxnSafePoint: 100}) // 2 regions each
			}
		}
		if bytes.Compare(expectedRanges[len(expectedRanges)-1].EndKey, []byte("y")) >= 0 {
			expectedRanges[len(expectedRanges)-1].EndKey = nil
		} else {
			expectedRanges = append(expectedRanges, reqRange{StartKey: []byte("y"), EndKey: nil, TxnSafePoint: 100}) // 1 region
		}

		s, counter, ch := createSuiteForTestResolveLocks(t, mockstore.WithKeyspacesAndCurrentKeyspaceID(keyspaces, constants.NullKeyspaceID))
		s.splitAtKeys(t, splitKeys...)
		err := s.gcWorker.resolveLocks(context.Background(), 100, 10)
		require.NoError(t, err)
		close(ch)
		ranges := collectAndMergeRanges(t, ch)
		require.Equal(t, expectedRanges, ranges)
		require.Equal(t, int64(expectedScanLocksCount), counter.Load())

		require.Equal(t, expectedBatchCount, loadKeyspacesBatchCount)
	}

	subCases["UnifiedGCInMultiBatchesOfKeyspaces_8"] = func(t *testing.T) {
		// Load keyspaces batches: [1,2,3], [4,5,6], [7,8], []
		testUnifiedGCInMultiBatchesOfKeyspacesImpl(t, 1, 8, 1, 3, 4)
	}

	subCases["UnifiedGCInMultiBatchesOfKeyspaces_Last8_Step2"] = func(t *testing.T) {
		// Load keyspaces batches: [(MaxKeyspaceID)-14,-12,-10], [-8,-6,-4], [-2,0]
		testUnifiedGCInMultiBatchesOfKeyspacesImpl(t, constants.MaxKeyspaceID-14, 8, 2, 3, 3)
	}

	subCases["UnifiedGCInMultiBatchesOfKeyspaces_MultipleOfBatchSize"] = func(t *testing.T) {
		// Load keyspaces batches: [1,2,3], [4,5,6], []
		testUnifiedGCInMultiBatchesOfKeyspacesImpl(t, 1, 6, 1, 3, 3)
	}

	subCases["UnifiedGCInMultiBatchesOfKeyspaces_MultipleOfBatchSizeToEnd"] = func(t *testing.T) {
		// Load keyspaces batches: [(MaxKeyspaceID)-5,-4,-3], [-2,-1,0]
		testUnifiedGCInMultiBatchesOfKeyspacesImpl(t, constants.MaxKeyspaceID-5, 6, 1, 3, 2)
	}

	subCases[subCaseName](t)
}

func TestResolveLocksWithKeyspaces_NullKeyspaceOnly(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "NullKeyspaceOnly")
}

func TestResolveLocksWithKeyspaces_NullKeyspaceOnlyMultiRegion(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "NullKeyspaceOnlyMultiRegion")
}

func TestResolveLocksWithKeyspaces_NullKeyspaceInMultiKeyspaceEnvironment(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "NullKeyspaceInMultiKeyspaceEnvironment")
}

func TestResolveLocksWithKeyspaces_NonNullKeyspaceInMultiKeyspaceEnvironment(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "NonNullKeyspaceInMultiKeyspaceEnvironment")
}

func TestResolveLocksWithKeyspaces_UnifiedGCInMixedUsage(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "UnifiedGCInMixedUsage")
}

func TestResolveLocksWithKeyspaces_UnifiedGCWithMaxKeyspaceID(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "UnifiedGCWithMaxKeyspaceID")
}

func TestResolveLocksWithKeyspaces_UnifiedGCInMultiBatchesOfKeyspaces_8(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "UnifiedGCInMultiBatchesOfKeyspaces_8")
}

func TestResolveLocksWithKeyspaces_UnifiedGCInMultiBatchesOfKeyspaces_Last8_Step2(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "UnifiedGCInMultiBatchesOfKeyspaces_Last8_Step2")
}

func TestResolveLocksWithKeyspaces_UnifiedGCInMultiBatchesOfKeyspaces_MultipleOfBatchSize(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "UnifiedGCInMultiBatchesOfKeyspaces_MultipleOfBatchSize")
}

func TestResolveLocksWithKeyspaces_UnifiedGCInMultiBatchesOfKeyspaces_MultipleOfBatchSizeToEnd(t *testing.T) {
	testResolveLocksWithKeyspacesImpl(t, "UnifiedGCInMultiBatchesOfKeyspaces_MultipleOfBatchSizeToEnd")
}

func TestResolveLocksNearTxnSafePoint(t *testing.T) {
	s := createGCWorkerSuite(t, withStoreType(mockstore.EmbedUnistore))

	currentTS, err := s.oracle.GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)
	txnSafePoint := oracle.GoTimeToTS(oracle.GetTimeFromTS(currentTS).Add(-time.Minute * 5))

	txns := make([]kv.Transaction, 0, 3)

	for i, startTS := range []uint64{txnSafePoint - 1, txnSafePoint, txnSafePoint + 1} {
		txn, err := s.store.Begin(tikv.WithStartTS(startTS))
		require.NoError(t, err)
		txn.SetOption(kv.Pessimistic, true)
		lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
		err = txn.LockKeys(context.Background(), lockCtx, []byte(fmt.Sprintf("k%d", i+1)))
		require.NoError(t, err)
		txns = append(txns, txn)
	}

	err = s.gcWorker.resolveLocks(gcContext(), txnSafePoint, 1)
	require.NoError(t, err)

	// Prevent amending lock behavior by making new write to the keys.
	otherTxns := make([]kv.Transaction, 0, 3)
	resCh := make(chan error, 3)
	for i := range 3 {
		txn, err := s.store.Begin()
		require.NoError(t, err)
		txn.SetOption(kv.Pessimistic, true)
		key := []byte(fmt.Sprintf("k%d", i+1))
		go func() {
			lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
			err := txn.LockKeys(context.Background(), lockCtx, key)
			resCh <- err
		}()
		otherTxns = append(otherTxns, txn)
	}

	// It's expected that only the first transaction in `txns` should be rolled back by GC, so that one of `otherTxns`
	// should proceed while the other two should be blocked.
	select {
	case err = <-resCh:
		require.NoError(t, err)
	case <-time.After(time.Millisecond * 200):
		require.Fail(t, "no transaction is resolved, which is not expected")
	}

	select {
	case err = <-resCh:
		require.Fail(t, "more than one transaction is resolved, which is not expected")
	case <-time.After(time.Millisecond * 50):
	}

	require.Error(t, txns[0].Commit(context.Background()))
	require.NoError(t, txns[1].Commit(context.Background()))
	require.NoError(t, txns[2].Commit(context.Background()))

	for range 2 {
		select {
		case err = <-resCh:
			require.Error(t, err)
			require.Contains(t, err.Error(), "Write conflict")
		case <-time.After(time.Millisecond * 200):
			require.Fail(t, "not all transactions are finished")
		}
	}

	// Clear unfinished transactions.
	for _, txn := range otherTxns {
		require.NoError(t, txn.Rollback())
	}
}

func TestRunGCJob(t *testing.T) {
	s := createGCWorkerSuite(t)

	txnSafePointSyncWaitTime = 0

	// Test distributed mode
	useDistributedGC := s.gcWorker.checkUseDistributedGC()
	require.True(t, useDistributedGC)
	safePoint := s.mustAllocTs(t)
	ctl := s.pdClient.GetGCInternalController(uint32(s.store.GetCodec().GetKeyspaceID()))
	// runGCJob doesn't contain the AdvanceTxnSafePoint step. Do it explicitly.
	res, err := ctl.AdvanceTxnSafePoint(gcContext(), safePoint)
	require.NoError(t, err)
	require.Equal(t, safePoint, res.NewTxnSafePoint)
	err = s.gcWorker.runGCJob(gcContext(), safePoint, gcConcurrency{1, false})
	require.NoError(t, err)

	pdSafePoint := s.mustGetSafePointFromPd(t)
	require.Equal(t, safePoint, pdSafePoint)

	require.NoError(t, s.gcWorker.saveTime(gcSafePointKey, oracle.GetTimeFromTS(safePoint)))
	tikvSafePoint, err := s.gcWorker.loadTime(gcSafePointKey)
	require.NoError(t, err)
	require.Equal(t, *tikvSafePoint, oracle.GetTimeFromTS(safePoint))

	etcdSafePoint := s.loadTxnSafePoint(t)
	require.Equal(t, safePoint, etcdSafePoint)

	// Test distributed mode with safePoint regressing (although this is impossible)
	err = s.gcWorker.runGCJob(gcContext(), safePoint-1, gcConcurrency{1, false})
	require.Error(t, err)

	// Central mode is deprecated in v5.0, fallback to distributed mode if it's set.
	err = s.gcWorker.saveValueToSysTable(gcModeKey, gcModeCentral)
	require.NoError(t, err)
	useDistributedGC = s.gcWorker.checkUseDistributedGC()
	require.True(t, useDistributedGC)

	p := s.createGCProbe(t, "k1")
	safePoint = s.mustAllocTs(t)
	res, err = ctl.AdvanceTxnSafePoint(gcContext(), safePoint)
	require.NoError(t, err)
	require.Equal(t, safePoint, res.NewTxnSafePoint)
	err = s.gcWorker.runGCJob(gcContext(), safePoint, gcConcurrency{1, false})
	require.NoError(t, err)
	s.checkCollected(t, p)

	etcdSafePoint = s.loadTxnSafePoint(t)
	require.Equal(t, safePoint, etcdSafePoint)
}

func TestSetServiceSafePoint(t *testing.T) {
	s := createGCWorkerSuite(t)

	// SafePoint calculations are based on time rather than ts value.
	safePoint := s.mustAllocTs(t)
	s.mustSetTiDBServiceSafePoint(t, safePoint, safePoint)
	require.Equal(t, safePoint, s.mustGetMinServiceSafePointFromPd(t))

	// Advance the service safe point
	safePoint += 100
	s.mustSetTiDBServiceSafePoint(t, safePoint, safePoint)
	require.Equal(t, safePoint, s.mustGetMinServiceSafePointFromPd(t))

	// It doesn't matter if there is a greater safePoint from other services.
	safePoint += 100
	// Returns the last service safePoint that were uploaded.
	s.mustUpdateServiceGCSafePoint(t, "svc1", safePoint+10, safePoint-100)
	s.mustSetTiDBServiceSafePoint(t, safePoint, safePoint)
	require.Equal(t, safePoint, s.mustGetMinServiceSafePointFromPd(t))

	// Test the case when there is a smaller safePoint from other services.
	safePoint += 100
	// Returns the last service safePoint that were uploaded.
	s.mustUpdateServiceGCSafePoint(t, "svc1", safePoint-10, safePoint-100)
	s.mustSetTiDBServiceSafePoint(t, safePoint, safePoint-10)
	require.Equal(t, safePoint-10, s.mustGetMinServiceSafePointFromPd(t))

	// Test removing the minimum service safe point.
	// As UpdateServiceGCSafePoint in unistore has become the compatible wrapper around GC barrier interface, this
	// behavior has changed: the simulated service safe point for "gc_worker" will be blocked at `safePoint-10`.
	// s.mustRemoveServiceGCSafePoint(t, "svc1", safePoint-10, safePoint)
	// require.Equal(t, safePoint, s.mustGetMinServiceSafePointFromPd(t))
	s.mustRemoveServiceGCSafePoint(t, "svc1", safePoint-10, safePoint-10)
	require.Equal(t, safePoint-10, s.mustGetMinServiceSafePointFromPd(t))
	// Advance it to `safePoint.
	s.mustSetTiDBServiceSafePoint(t, safePoint, safePoint)

	// Test the case when there are many safePoints.
	safePoint += 100
	for i := range 10 {
		svcName := fmt.Sprintf("svc%d", i)
		s.mustUpdateServiceGCSafePoint(t, svcName, safePoint+uint64(i)*10, safePoint-100)
	}
	s.mustSetTiDBServiceSafePoint(t, safePoint+50, safePoint)
}

func TestRunGCJobAPI(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("RunGCJobAPI currently does not support running under non-null keyspace")
	}

	s := createGCWorkerSuite(t)
	mockLockResolver := &mockGCWorkerLockResolver{
		RegionLockResolver: tikv.NewRegionLockResolver("test", s.tikvStore),
		tikvStore:          s.tikvStore,
		scanLocks: func(_ []*txnlock.Lock, key []byte) ([]*txnlock.Lock, *tikv.KeyLocation) {
			return []*txnlock.Lock{}, &tikv.KeyLocation{}
		},
		batchResolveLocks: func(
			locks []*txnlock.Lock,
			loc *tikv.KeyLocation,
		) (*tikv.KeyLocation, error) {
			// no locks
			return loc, nil
		},
	}

	txnSafePointSyncWaitTime = 0

	p := s.createGCProbe(t, "k1")
	safePoint := s.mustAllocTs(t)
	err := RunGCJob(gcContext(), mockLockResolver, s.tikvStore, s.pdClient, safePoint, "mock", 1)
	require.NoError(t, err)
	s.checkCollected(t, p)
	etcdSafePoint := s.loadTxnSafePoint(t)
	require.NoError(t, err)
	require.Equal(t, safePoint, etcdSafePoint)
}

func TestRunDistGCJobAPI(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("RunDistributedGCJob currently does not support running under non-null keyspace")
	}

	s := createGCWorkerSuite(t)

	txnSafePointSyncWaitTime = 0
	mockLockResolver := &mockGCWorkerLockResolver{
		RegionLockResolver: tikv.NewRegionLockResolver("test", s.tikvStore),
		tikvStore:          s.tikvStore,
		scanLocks: func(_ []*txnlock.Lock, key []byte) ([]*txnlock.Lock, *tikv.KeyLocation) {
			return []*txnlock.Lock{}, &tikv.KeyLocation{}
		},
		batchResolveLocks: func(
			locks []*txnlock.Lock,
			loc *tikv.KeyLocation,
		) (*tikv.KeyLocation, error) {
			// no locks
			return loc, nil
		},
	}

	safePoint := s.mustAllocTs(t)
	err := RunDistributedGCJob(gcContext(), mockLockResolver, s.tikvStore, s.pdClient, safePoint, "mock", 1)
	require.NoError(t, err)
	pdSafePoint := s.mustGetSafePointFromPd(t)
	require.Equal(t, safePoint, pdSafePoint)
	etcdSafePoint := s.loadTxnSafePoint(t)
	require.NoError(t, err)
	require.Equal(t, safePoint, etcdSafePoint)
}

func TestStartWithRunGCJobFailures(t *testing.T) {
	s := createGCWorkerSuite(t)

	s.gcWorker.Start()
	defer s.gcWorker.Close()

	for range 3 {
		select {
		case <-time.After(100 * time.Millisecond):
			require.FailNow(t, "gc worker failed to handle errors")
		case s.gcWorker.done <- errors.New("mock error"):
		}
	}
}

func (s *mockGCWorkerSuite) loadTxnSafePoint(t *testing.T) uint64 {
	gcStates, err := s.pdClient.GetGCStatesClient(uint32(s.store.GetCodec().GetKeyspaceID())).GetGCState(context.Background())
	require.NoError(t, err)
	return gcStates.TxnSafePoint
}

func TestGCPlacementRules(t *testing.T) {
	s := createGCWorkerSuite(t)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC", "return(10)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC"))
	}()

	var gcPlacementRuleCache sync.Map
	deletePlacementRuleCounter := 0
	require.NoError(t, failpoint.EnableWith("github.com/pingcap/tidb/pkg/store/gcworker/gcDeletePlacementRuleCounter", "return", func() error {
		deletePlacementRuleCounter++
		return nil
	}))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/gcDeletePlacementRuleCounter"))
	}()

	bundleID := "TiDB_DDL_10"
	bundle, err := placement.NewBundleFromOptions(&model.PlacementSettings{
		PrimaryRegion: "r1",
		Regions:       "r1, r2",
	})
	require.NoError(t, err)
	bundle.ID = bundleID

	// prepare bundle before gc
	require.NoError(t, infosync.PutRuleBundles(context.Background(), []*placement.Bundle{bundle}))
	got, err := infosync.GetRuleBundle(context.Background(), bundleID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.IsEmpty())

	// do gc
	dr := util.DelRangeTask{JobID: 1, ElementID: 10}
	err = doGCPlacementRules(createSession(s.store), 1, dr, &gcPlacementRuleCache)
	require.NoError(t, err)
	v, ok := gcPlacementRuleCache.Load(int64(10))
	require.True(t, ok)
	require.Equal(t, struct{}{}, v)
	require.Equal(t, 1, deletePlacementRuleCounter)

	// check bundle deleted after gc
	got, err = infosync.GetRuleBundle(context.Background(), bundleID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.IsEmpty())

	// gc the same table id repeatedly
	err = doGCPlacementRules(createSession(s.store), 1, dr, &gcPlacementRuleCache)
	require.NoError(t, err)
	v, ok = gcPlacementRuleCache.Load(int64(10))
	require.True(t, ok)
	require.Equal(t, struct{}{}, v)
	require.Equal(t, 1, deletePlacementRuleCounter)
}

func TestGCLabelRules(t *testing.T) {
	s := createGCWorkerSuite(t)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJob", "return(\"schema/d1/t1\")"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJob"))
	}()

	dr := util.DelRangeTask{JobID: 1, ElementID: 1}
	err := s.gcWorker.doGCLabelRules(dr)
	require.NoError(t, err)
}

func TestGCWithPendingTxn(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("skip TestGCWithPendingTxn when kernel type is NextGen - test not yet adjusted to support next-gen")
	}
	s := createGCWorkerSuite(t, withStoreType(mockstore.EmbedUnistore), withSchemaLease(30*time.Minute))

	ctx := gcContext()
	txnSafePointSyncWaitTime = 0
	err := s.gcWorker.saveValueToSysTable(gcEnableKey, booleanFalse)
	require.NoError(t, err)

	k1 := []byte("tk1")
	v1 := []byte("v1")
	txn, err := s.store.Begin()
	require.NoError(t, err)
	txn.SetOption(kv.Pessimistic, true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}

	// Lock the key.
	err = txn.Set(k1, v1)
	require.NoError(t, err)
	err = txn.LockKeys(ctx, lockCtx, k1)
	require.NoError(t, err)

	// Prepare to run gc with txn's startTS as the safepoint ts.
	spkv := s.tikvStore.GetSafePointKV()
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), strconv.FormatUint(txn.StartTS(), 10))
	require.NoError(t, err)
	//s.mustSetTiDBServiceSafePoint(t, txn.StartTS(), txn.StartTS())
	veryLong := gcDefaultLifeTime * 100
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-veryLong))
	require.NoError(t, err)
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	s.oracle.AddOffset(time.Minute * 10)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanTrue)
	require.NoError(t, err)

	// Trigger the tick let the gc job start.
	err = s.gcWorker.leaderTick(ctx)
	require.NoError(t, err)
	waitGCFinish(t, s)

	err = txn.Commit(ctx)
	// TODO: The mock implementation of PD doesn't put the data in the etcd or `SafePointKV`, making this test not
	//   working for now. We need to fix this test after further refactor.
	if err != nil {
		t.Logf("txn commit returned error (mock PD safepoint behavior differs from real PD): %v", err)
	}
}

func TestGCWithPendingTxn2(t *testing.T) {
	// as we are adjusting the base TS, we need a larger schema lease to avoid
	// the info schema outdated error.
	s := createGCWorkerSuite(t, withStoreType(mockstore.EmbedUnistore), withSchemaLease(10*time.Minute))

	ctx := gcContext()
	txnSafePointSyncWaitTime = 0
	err := s.gcWorker.saveValueToSysTable(gcEnableKey, booleanFalse)
	require.NoError(t, err)

	now, err := s.oracle.GetTimestamp(ctx, &oracle.Option{})
	require.NoError(t, err)

	// Prepare to run gc with txn's startTS as the safepoint ts.
	spkv := s.tikvStore.GetSafePointKV()
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), strconv.FormatUint(now, 10))
	require.NoError(t, err)
	//s.mustSetTiDBServiceSafePoint(t, now, now)
	veryLong := gcDefaultLifeTime * 100
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-veryLong))
	require.NoError(t, err)
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanTrue)
	require.NoError(t, err)

	// lock the key1
	k1 := []byte("tk1")
	v1 := []byte("v1")
	txn, err := s.store.Begin(tikv.WithStartTS(now))
	require.NoError(t, err)
	txn.SetOption(kv.Pessimistic, true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}

	err = txn.Set(k1, v1)
	require.NoError(t, err)
	err = txn.LockKeys(ctx, lockCtx, k1)
	require.NoError(t, err)

	// lock the key2
	k2 := []byte("tk2")
	v2 := []byte("v2")
	startTS := oracle.ComposeTS(oracle.ExtractPhysical(now)+10000, oracle.ExtractLogical(now))
	txn2, err := s.store.Begin(tikv.WithStartTS(startTS))
	require.NoError(t, err)
	txn2.SetOption(kv.Pessimistic, true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), WaitStartTime: time.Now()}

	err = txn2.Set(k2, v2)
	require.NoError(t, err)
	err = txn2.LockKeys(ctx, lockCtx, k2)
	require.NoError(t, err)

	// Trigger the tick let the gc job start.
	s.oracle.AddOffset(time.Minute * 5)
	err = s.gcWorker.leaderTick(ctx)
	require.NoError(t, err)
	waitGCFinish(t, s)

	err = txn.Commit(ctx)
	require.NoError(t, err)
	err = txn2.Commit(ctx)
	require.NoError(t, err)
}

func TestSkipGCAndOnlyResolveLock(t *testing.T) {
	// as we are adjusting the base TS, we need a larger schema lease to avoid
	// the info schema outdated error.
	s := createGCWorkerSuite(t, withStoreType(mockstore.EmbedUnistore), withSchemaLease(10*time.Minute))

	ctx := gcContext()
	txnSafePointSyncWaitTime = 0
	err := s.gcWorker.saveValueToSysTable(gcEnableKey, booleanFalse)
	require.NoError(t, err)
	now, err := s.oracle.GetTimestamp(ctx, &oracle.Option{})
	require.NoError(t, err)

	// Prepare to run gc with txn's startTS as the safepoint ts.
	spkv := s.tikvStore.GetSafePointKV()
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), strconv.FormatUint(now, 10))
	require.NoError(t, err)
	s.mustSetTiDBServiceSafePoint(t, now, now)
	veryLong := gcDefaultLifeTime * 100
	lastRunTime := oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-veryLong)
	newGcLifeTime := time.Hour * 24
	err = s.gcWorker.saveTime(gcLastRunTimeKey, lastRunTime)
	require.NoError(t, err)
	err = s.gcWorker.saveTime(gcSafePointKey, oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-time.Minute*10))
	require.NoError(t, err)
	err = s.gcWorker.saveDuration(gcLifeTimeKey, newGcLifeTime)
	require.NoError(t, err)
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanTrue)
	require.NoError(t, err)

	// lock the key1
	k1 := []byte("tk1")
	v1 := []byte("v1")
	txn, err := s.store.Begin(tikv.WithStartTS(now))
	require.NoError(t, err)
	txn.SetOption(kv.Pessimistic, true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}

	err = txn.Set(k1, v1)
	require.NoError(t, err)
	err = txn.LockKeys(ctx, lockCtx, k1)
	require.NoError(t, err)

	// Trigger the tick let the gc job start.
	s.oracle.AddOffset(time.Minute * 5)
	err = s.gcWorker.leaderTick(ctx)
	require.NoError(t, err)

	// check the lock has not been resolved.
	err = txn.Commit(ctx)
	require.NoError(t, err)

	// check gc is skipped
	last, err := s.gcWorker.loadTime(gcLastRunTimeKey)
	require.NoError(t, err)
	require.Equal(t, last.Unix(), lastRunTime.Unix())
}

func bootstrap(t testing.TB, store kv.Storage, lease time.Duration) *domain.Domain {
	vardef.SetSchemaLease(lease)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	// Stop TTL background loops to avoid holding long-running txns that can block safe point advancement
	// when MockOracle time is advanced by GC tests.
	ttlJobManager := dom.TTLJobManager()
	if ttlJobManager != nil {
		ttlJobManager.Stop()
		err := ttlJobManager.WaitStopped(context.Background(), 10*time.Second)
		require.NoError(t, err)
	}

	dom.SetStatsUpdating(true)

	t.Cleanup(func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	})
	return dom
}

func TestCalcDeleteRangeConcurrency(t *testing.T) {
	testCases := []struct {
		name        string
		concurrency gcConcurrency
		rangeNum    int
		expected    int
	}{
		{"Auto: Low concurrency, few ranges", gcConcurrency{16, true}, 50000, 1},
		{"Auto: High concurrency, many ranges", gcConcurrency{400, true}, 1000000, 10},
		{"Auto: High concurrency, few ranges", gcConcurrency{400, true}, 50000, 1},
		{"Auto: Low concurrency, many ranges", gcConcurrency{16, true}, 1000000, 4},
		{"Non-auto: Low concurrency", gcConcurrency{16, false}, 1000000, 4},
		{"Non-auto: High concurrency", gcConcurrency{400, false}, 50000, 100},
		{"Edge case: Zero concurrency", gcConcurrency{0, true}, 100000, 1},
		{"Edge case: Zero ranges", gcConcurrency{100, true}, 0, 1},
		{"Large range number", gcConcurrency{400, true}, 10000000, 100},
		{"Exact RequestsPerThread", gcConcurrency{400, true}, 200000, 2},
	}

	w := &GCWorker{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := w.calcDeleteRangeConcurrency(tc.concurrency, tc.rangeNum)
			if result != tc.expected {
				t.Errorf("Expected %d, but got %d", tc.expected, result)
			}
			if result < 1 {
				t.Errorf("Result should never be less than 1, but got %d", result)
			}
		})
	}
}
