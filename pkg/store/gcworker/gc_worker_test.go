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
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
)

type mockGCWorkerLockResolver struct {
	tikv.RegionLockResolver
	tikvStore         tikv.Storage
	scanLocks         func([]*txnlock.Lock, []byte) ([]*txnlock.Lock, *tikv.KeyLocation)
	batchResolveLocks func([]*txnlock.Lock, *tikv.KeyLocation) (*tikv.KeyLocation, error)
}

func (l *mockGCWorkerLockResolver) ScanLocksInOneRegion(bo *tikv.Backoffer, key []byte, maxVersion uint64, limit uint32) ([]*txnlock.Lock, *tikv.KeyLocation, error) {
	locks, loc, err := l.RegionLockResolver.ScanLocksInOneRegion(bo, key, maxVersion, limit)
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
	unsafeDestroyRangeHandler   handler
	deleteRangeHandler          handler
	physicalScanLockHandler     handler
	registerLockObserverHandler handler
	checkLockObserverHandler    handler
	removeLockObserverHandler   handler
}

type handler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error)

func gcContext() context.Context {
	// internal statements must bind with resource type
	return kv.WithInternalSourceType(context.Background(), kv.InternalTxnGC)
}

func (c *mockGCWorkerClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdUnsafeDestroyRange && c.unsafeDestroyRangeHandler != nil {
		return c.unsafeDestroyRangeHandler(addr, req)
	}
	if req.Type == tikvrpc.CmdPhysicalScanLock && c.physicalScanLockHandler != nil {
		return c.physicalScanLockHandler(addr, req)
	}
	if req.Type == tikvrpc.CmdRegisterLockObserver && c.registerLockObserverHandler != nil {
		return c.registerLockObserverHandler(addr, req)
	}
	if req.Type == tikvrpc.CmdCheckLockObserver && c.checkLockObserverHandler != nil {
		return c.checkLockObserverHandler(addr, req)
	}
	if req.Type == tikvrpc.CmdRemoveLockObserver && c.removeLockObserverHandler != nil {
		return c.removeLockObserverHandler(addr, req)
	}
	if req.Type == tikvrpc.CmdDeleteRange && c.deleteRangeHandler != nil {
		return c.deleteRangeHandler(addr, req)
	}

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

func createGCWorkerSuite(t *testing.T) (s *mockGCWorkerSuite) {
	return createGCWorkerSuiteWithStoreType(t, mockstore.EmbedUnistore)
}

func createGCWorkerSuiteWithStoreType(t *testing.T, storeType mockstore.StoreType) (s *mockGCWorkerSuite) {
	s = new(mockGCWorkerSuite)
	hijackClient := func(client tikv.Client) tikv.Client {
		s.client = &mockGCWorkerClient{Client: client}
		client = s.client
		return client
	}
	opts := []mockstore.MockTiKVStoreOption{
		mockstore.WithStoreType(storeType),
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

	s.oracle = &oracles.MockOracle{}
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)
	store.GetOracle().Close()
	store.(tikv.Storage).SetOracle(s.oracle)
	dom := bootstrap(t, store, 0)
	s.store, s.dom = store, dom

	s.tikvStore = s.store.(tikv.Storage)

	gcWorker, err := NewGCWorker(s.store, s.pdClient)
	require.NoError(t, err)
	gcWorker.Start()
	gcWorker.Close()
	s.gcWorker = gcWorker

	return
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
	return string(value)
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
	// UpdateGCSafePoint returns the newest safePoint after the updating, which can be used to check whether the
	// safePoint is successfully uploaded.
	safePoint, err := s.pdClient.UpdateGCSafePoint(context.Background(), 0)
	require.NoError(t, err)
	return safePoint
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

func TestMinStartTS(t *testing.T) {
	s := createGCWorkerSuite(t)

	ctx := context.Background()
	spkv := s.tikvStore.GetSafePointKV()
	err := spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), strconv.FormatUint(math.MaxUint64, 10))
	require.NoError(t, err)
	now := oracle.GoTimeToTS(time.Now())
	sp := s.gcWorker.calcSafePointByMinStartTS(ctx, now)
	require.Equal(t, now, sp)
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), "0")
	require.NoError(t, err)
	sp = s.gcWorker.calcSafePointByMinStartTS(ctx, now)
	require.Equal(t, uint64(0), sp)

	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), "0")
	require.NoError(t, err)
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "b"), "1")
	require.NoError(t, err)
	sp = s.gcWorker.calcSafePointByMinStartTS(ctx, now)
	require.Equal(t, uint64(0), sp)

	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), strconv.FormatUint(now, 10))
	require.NoError(t, err)
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "b"), strconv.FormatUint(now-oracle.ComposeTS(20000, 0), 10))
	require.NoError(t, err)
	sp = s.gcWorker.calcSafePointByMinStartTS(ctx, now-oracle.ComposeTS(10000, 0))
	require.Equal(t, now-oracle.ComposeTS(20000, 0)-1, sp)
}

func TestPrepareGC(t *testing.T) {
	s := createGCWorkerSuite(t)

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
	safePointTime, err := s.gcWorker.loadTime(gcSafePointKey)
	minStartTS := oracle.GoTimeToTS(*safePointTime) + 1
	require.NoError(t, err)
	spkv := s.tikvStore.GetSafePointKV()
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), strconv.FormatUint(minStartTS, 10))
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
	require.Equal(t, concurrencyConfig, concurrency)

	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanTrue)
	require.NoError(t, err)
	concurrency, err = s.gcWorker.getGCConcurrency(ctx)
	require.NoError(t, err)
	require.Len(t, s.cluster.GetAllStores(), concurrency)
}

func TestDoGC(t *testing.T) {
	s := createGCWorkerSuite(t)

	ctx := context.Background()
	gcSafePointCacheInterval = 1

	p := s.createGCProbe(t, "k1")
	err := s.gcWorker.doGC(ctx, s.mustAllocTs(t), gcDefaultConcurrency)
	require.NoError(t, err)
	s.checkCollected(t, p)

	p = s.createGCProbe(t, "k1")
	err = s.gcWorker.doGC(ctx, s.mustAllocTs(t), gcMinConcurrency)
	require.NoError(t, err)
	s.checkCollected(t, p)

	p = s.createGCProbe(t, "k1")
	err = s.gcWorker.doGC(ctx, s.mustAllocTs(t), gcMaxConcurrency)
	require.NoError(t, err)
	s.checkCollected(t, p)
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

				err = deleteRangeFunc(gcContext(), 20, 1)
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
				err = deleteRangeFunc(gcContext(), 20, 1)
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

	sortedRanges := append([]util.DelRangeTask{}, expectedRanges...)
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

	err = s.gcWorker.deleteRanges(gcContext(), 8, 1)
	require.NoError(t, err)

	s.checkDestroyRangeReqV2(t, sendReqCh, ranges[:1])

	se = createSession(s.gcWorker.store)
	remainingRanges, err := util.LoadDeleteRanges(gcContext(), se, 20)
	se.Close()
	require.NoError(t, err)
	require.Equal(t, ranges[1:], remainingRanges)

	err = s.gcWorker.deleteRanges(gcContext(), 20, 1)
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

	sortedRanges := append([]util.DelRangeTask{}, expectedRanges...)
	sort.Slice(sortedRanges, func(i, j int) bool {
		return bytes.Compare(sortedRanges[i].StartKey, sortedRanges[j].StartKey) < 0
	})

	for rangeIndex := range sortedRanges {
		require.Equal(t, sortedRanges[rangeIndex].StartKey, kv.Key(sentReq[rangeIndex].req.DeleteRange().GetStartKey()))
		require.Equal(t, sortedRanges[rangeIndex].EndKey, kv.Key(sentReq[rangeIndex].req.DeleteRange().GetEndKey()))
	}
}

func TestLeaderTick(t *testing.T) {
	s := createGCWorkerSuite(t)

	gcSafePointCacheInterval = 0

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

	// Continue GC if all those checks passed.
	err = s.gcWorker.leaderTick(gcContext())
	require.NoError(t, err)
	// Wait for GC finish
	select {
	case err = <-s.gcWorker.done:
		s.gcWorker.gcIsRunning = false
		break
	case <-time.After(time.Second * 10):
		err = errors.New("receive from s.gcWorker.done timeout")
	}
	require.NoError(t, err)
	s.checkCollected(t, p)

	// Test again to ensure the synchronization between goroutines is correct.
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(t)).Add(-veryLong))
	require.NoError(t, err)
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	p = s.createGCProbe(t, "k1")
	s.oracle.AddOffset(gcDefaultLifeTime * 2)

	err = s.gcWorker.leaderTick(gcContext())
	require.NoError(t, err)
	// Wait for GC finish
	select {
	case err = <-s.gcWorker.done:
		s.gcWorker.gcIsRunning = false
		break
	case <-time.After(time.Second * 10):
		err = errors.New("receive from s.gcWorker.done timeout")
	}
	require.NoError(t, err)
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/DisablePaging", `return`)
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/DisablePaging"))
	}()
	s := createGCWorkerSuiteWithStoreType(t, mockstore.MockTiKV)

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

func TestRunGCJob(t *testing.T) {
	s := createGCWorkerSuite(t)

	gcSafePointCacheInterval = 0

	// Test distributed mode
	useDistributedGC := s.gcWorker.checkUseDistributedGC()
	require.True(t, useDistributedGC)
	safePoint := s.mustAllocTs(t)
	err := s.gcWorker.runGCJob(gcContext(), safePoint, 1)
	require.NoError(t, err)

	pdSafePoint := s.mustGetSafePointFromPd(t)
	require.Equal(t, safePoint, pdSafePoint)

	require.NoError(t, s.gcWorker.saveTime(gcSafePointKey, oracle.GetTimeFromTS(safePoint)))
	tikvSafePoint, err := s.gcWorker.loadTime(gcSafePointKey)
	require.NoError(t, err)
	require.Equal(t, *tikvSafePoint, oracle.GetTimeFromTS(safePoint))

	etcdSafePoint := s.loadEtcdSafePoint(t)
	require.Equal(t, safePoint, etcdSafePoint)

	// Test distributed mode with safePoint regressing (although this is impossible)
	err = s.gcWorker.runGCJob(gcContext(), safePoint-1, 1)
	require.Error(t, err)

	// Central mode is deprecated in v5.0, fallback to distributed mode if it's set.
	err = s.gcWorker.saveValueToSysTable(gcModeKey, gcModeCentral)
	require.NoError(t, err)
	useDistributedGC = s.gcWorker.checkUseDistributedGC()
	require.True(t, useDistributedGC)

	p := s.createGCProbe(t, "k1")
	safePoint = s.mustAllocTs(t)
	err = s.gcWorker.runGCJob(gcContext(), safePoint, 1)
	require.NoError(t, err)
	s.checkCollected(t, p)

	etcdSafePoint = s.loadEtcdSafePoint(t)
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
	s.mustRemoveServiceGCSafePoint(t, "svc1", safePoint-10, safePoint)
	require.Equal(t, safePoint, s.mustGetMinServiceSafePointFromPd(t))

	// Test the case when there are many safePoints.
	safePoint += 100
	for i := 0; i < 10; i++ {
		svcName := fmt.Sprintf("svc%d", i)
		s.mustUpdateServiceGCSafePoint(t, svcName, safePoint+uint64(i)*10, safePoint-100)
	}
	s.mustSetTiDBServiceSafePoint(t, safePoint+50, safePoint)
}

func TestRunGCJobAPI(t *testing.T) {
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

	gcSafePointCacheInterval = 0

	p := s.createGCProbe(t, "k1")
	safePoint := s.mustAllocTs(t)
	err := RunGCJob(gcContext(), mockLockResolver, s.tikvStore, s.pdClient, safePoint, "mock", 1)
	require.NoError(t, err)
	s.checkCollected(t, p)
	etcdSafePoint := s.loadEtcdSafePoint(t)
	require.NoError(t, err)
	require.Equal(t, safePoint, etcdSafePoint)
}

func TestRunDistGCJobAPI(t *testing.T) {
	s := createGCWorkerSuite(t)

	gcSafePointCacheInterval = 0
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
	etcdSafePoint := s.loadEtcdSafePoint(t)
	require.NoError(t, err)
	require.Equal(t, safePoint, etcdSafePoint)
}

func TestStartWithRunGCJobFailures(t *testing.T) {
	s := createGCWorkerSuite(t)

	s.gcWorker.Start()
	defer s.gcWorker.Close()

	for i := 0; i < 3; i++ {
		select {
		case <-time.After(100 * time.Millisecond):
			require.FailNow(t, "gc worker failed to handle errors")
		case s.gcWorker.done <- errors.New("mock error"):
		}
	}
}

func (s *mockGCWorkerSuite) loadEtcdSafePoint(t *testing.T) uint64 {
	val, err := s.gcWorker.tikvStore.GetSafePointKV().Get(tikv.GcSavedSafePoint)
	require.NoError(t, err)
	res, err := strconv.ParseUint(val, 10, 64)
	require.NoError(t, err)
	return res
}

func TestGCPlacementRules(t *testing.T) {
	s := createGCWorkerSuite(t)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC", "return(10)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockHistoryJobForGC"))
	}()

	gcPlacementRuleCache := make(map[int64]any)
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
	err = s.gcWorker.doGCPlacementRules(createSession(s.store), 1, dr, gcPlacementRuleCache)
	require.NoError(t, err)
	require.Equal(t, map[int64]any{10: struct{}{}}, gcPlacementRuleCache)
	require.Equal(t, 1, deletePlacementRuleCounter)

	// check bundle deleted after gc
	got, err = infosync.GetRuleBundle(context.Background(), bundleID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.IsEmpty())

	// gc the same table id repeatedly
	err = s.gcWorker.doGCPlacementRules(createSession(s.store), 1, dr, gcPlacementRuleCache)
	require.NoError(t, err)
	require.Equal(t, map[int64]any{10: struct{}{}}, gcPlacementRuleCache)
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
	s := createGCWorkerSuite(t)

	ctx := gcContext()
	gcSafePointCacheInterval = 0
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
	s.mustSetTiDBServiceSafePoint(t, txn.StartTS(), txn.StartTS())
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
	// Wait for GC finish
	select {
	case err = <-s.gcWorker.done:
		s.gcWorker.gcIsRunning = false
		break
	case <-time.After(time.Second * 10):
		err = errors.New("receive from s.gcWorker.done timeout")
	}
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

func TestGCWithPendingTxn2(t *testing.T) {
	s := createGCWorkerSuite(t)

	ctx := gcContext()
	gcSafePointCacheInterval = 0
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
	// Wait for GC finish
	select {
	case err = <-s.gcWorker.done:
		s.gcWorker.gcIsRunning = false
		break
	case <-time.After(time.Second * 10):
		err = errors.New("receive from s.gcWorker.done timeout")
	}
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)
	err = txn2.Commit(ctx)
	require.NoError(t, err)
}

func TestSkipGCAndOnlyResolveLock(t *testing.T) {
	s := createGCWorkerSuite(t)

	ctx := gcContext()
	gcSafePointCacheInterval = 0
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
	session.SetSchemaLease(lease)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	dom.SetStatsUpdating(true)

	t.Cleanup(func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	})
	return dom
}
