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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockoracle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	pd "github.com/tikv/pd/client"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testGCWorkerSuite struct {
	store    tikv.Storage
	cluster  cluster.Cluster
	oracle   *mockoracle.MockOracle
	gcWorker *GCWorker
	dom      *domain.Domain
	client   *testGCWorkerClient
	pdClient pd.Client
}

var _ = SerialSuites(&testGCWorkerSuite{})

func (s *testGCWorkerSuite) SetUpTest(c *C) {
	tikv.NewGCHandlerFunc = NewGCWorker

	hijackClient := func(client tikv.Client) tikv.Client {
		s.client = &testGCWorkerClient{
			Client: client,
		}
		client = s.client
		return client
	}

	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithMultiStores(c, 3)
			s.cluster = c
		}),
		mockstore.WithClientHijacker(hijackClient),
		mockstore.WithPDClientHijacker(func(c pd.Client) pd.Client {
			s.pdClient = c
			return c
		}),
	)
	c.Assert(err, IsNil)

	s.store = store.(tikv.Storage)
	c.Assert(err, IsNil)
	s.oracle = &mockoracle.MockOracle{}
	s.store.SetOracle(s.oracle)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	gcWorker, err := NewGCWorker(s.store, s.pdClient)
	c.Assert(err, IsNil)
	gcWorker.Start()
	gcWorker.Close()
	s.gcWorker = gcWorker.(*GCWorker)
}

func (s *testGCWorkerSuite) TearDownTest(c *C) {
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testGCWorkerSuite) timeEqual(c *C, t1, t2 time.Time, epsilon time.Duration) {
	c.Assert(math.Abs(float64(t1.Sub(t2))), Less, float64(epsilon))
}

func (s *testGCWorkerSuite) mustPut(c *C, key, value string) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte(key), []byte(value))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testGCWorkerSuite) mustGet(c *C, key string, ts uint64) string {
	snap := s.store.GetSnapshot(kv.Version{Ver: ts})
	value, err := snap.Get(context.TODO(), []byte(key))
	c.Assert(err, IsNil)
	return string(value)
}

func (s *testGCWorkerSuite) mustGetNone(c *C, key string, ts uint64) {
	snap := s.store.GetSnapshot(kv.Version{Ver: ts})
	_, err := snap.Get(context.TODO(), []byte(key))
	if err != nil {
		// Unistore's gc is based on compaction filter.
		// So skip the error check if err == nil.
		c.Assert(err, Equals, kv.ErrNotExist)
	}
}

func (s *testGCWorkerSuite) mustAllocTs(c *C) uint64 {
	ts, err := s.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	return ts
}

func (s *testGCWorkerSuite) mustGetSafePointFromPd(c *C) uint64 {
	// UpdateGCSafePoint returns the newest safePoint after the updating, which can be used to check whether the
	// safePoint is successfully uploaded.
	safePoint, err := s.pdClient.UpdateGCSafePoint(context.Background(), 0)
	c.Assert(err, IsNil)
	return safePoint
}

func (s *testGCWorkerSuite) mustGetMinServiceSafePointFromPd(c *C) uint64 {
	// UpdateServiceGCSafePoint returns the minimal service safePoint. If trying to update it with a value less than the
	// current minimal safePoint, nothing will be updated and the current minimal one will be returned. So we can use
	// this API to check the current safePoint.
	// This function shouldn't be invoked when there's no service safePoint set.
	minSafePoint, err := s.pdClient.UpdateServiceGCSafePoint(context.Background(), "test", 0, 0)
	c.Assert(err, IsNil)
	return minSafePoint
}

func (s *testGCWorkerSuite) mustUpdateServiceGCSafePoint(c *C, serviceID string, safePoint, expectedMinSafePoint uint64) {
	minSafePoint, err := s.pdClient.UpdateServiceGCSafePoint(context.Background(), serviceID, math.MaxInt64, safePoint)
	c.Assert(err, IsNil)
	c.Assert(minSafePoint, Equals, expectedMinSafePoint)
}

func (s *testGCWorkerSuite) mustRemoveServiceGCSafePoint(c *C, serviceID string, safePoint, expectedMinSafePoint uint64) {
	minSafePoint, err := s.pdClient.UpdateServiceGCSafePoint(context.Background(), serviceID, 0, safePoint)
	c.Assert(err, IsNil)
	c.Assert(minSafePoint, Equals, expectedMinSafePoint)
}

func (s *testGCWorkerSuite) mustSetTiDBServiceSafePoint(c *C, safePoint, expectedMinSafePoint uint64) {
	minSafePoint, err := s.gcWorker.setGCWorkerServiceSafePoint(context.Background(), safePoint)
	c.Assert(err, IsNil)
	c.Assert(minSafePoint, Equals, expectedMinSafePoint)
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
func (s *testGCWorkerSuite) createGCProbe(c *C, key string) *gcProbe {
	s.mustPut(c, key, "v1")
	ts1 := s.mustAllocTs(c)
	s.mustPut(c, key, "v2")
	ts2 := s.mustAllocTs(c)
	p := &gcProbe{
		key:  key,
		v1Ts: ts1,
		v2Ts: ts2,
	}
	s.checkNotCollected(c, p)
	return p
}

// checkCollected asserts the gcProbe has been correctly collected.
func (s *testGCWorkerSuite) checkCollected(c *C, p *gcProbe) {
	s.mustGetNone(c, p.key, p.v1Ts)
	c.Assert(s.mustGet(c, p.key, p.v2Ts), Equals, "v2")
}

// checkNotCollected asserts the gcProbe has not been collected.
func (s *testGCWorkerSuite) checkNotCollected(c *C, p *gcProbe) {
	c.Assert(s.mustGet(c, p.key, p.v1Ts), Equals, "v1")
	c.Assert(s.mustGet(c, p.key, p.v2Ts), Equals, "v2")
}

func (s *testGCWorkerSuite) TestGetOracleTime(c *C) {
	t1, err := s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	s.timeEqual(c, time.Now(), t1, time.Millisecond*10)

	s.oracle.AddOffset(time.Second * 10)
	t2, err := s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	s.timeEqual(c, t2, t1.Add(time.Second*10), time.Millisecond*10)
}

func (s *testGCWorkerSuite) TestMinStartTS(c *C) {
	ctx := context.Background()
	spkv := s.store.GetSafePointKV()
	err := spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), strconv.FormatUint(math.MaxUint64, 10))
	c.Assert(err, IsNil)
	now := time.Now()
	sp := s.gcWorker.calSafePointByMinStartTS(ctx, now)
	c.Assert(sp.Second(), Equals, now.Second())
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), "0")
	c.Assert(err, IsNil)
	sp = s.gcWorker.calSafePointByMinStartTS(ctx, now)
	zeroTime := time.Unix(0, oracle.ExtractPhysical(0)*1e6)
	c.Assert(sp, Equals, zeroTime)

	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), "0")
	c.Assert(err, IsNil)
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "b"), "1")
	c.Assert(err, IsNil)
	sp = s.gcWorker.calSafePointByMinStartTS(ctx, now)
	c.Assert(sp, Equals, zeroTime)

	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"),
		strconv.FormatUint(variable.GoTimeToTS(now), 10))
	c.Assert(err, IsNil)
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "b"),
		strconv.FormatUint(variable.GoTimeToTS(now.Add(-20*time.Second)), 10))
	c.Assert(err, IsNil)
	sp = s.gcWorker.calSafePointByMinStartTS(ctx, now.Add(-10*time.Second))
	c.Assert(sp.Second(), Equals, now.Add(-20*time.Second).Second())
}

func (s *testGCWorkerSuite) TestPrepareGC(c *C) {
	now, err := s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	close(s.gcWorker.done)
	ok, _, err := s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	lastRun, err := s.gcWorker.loadTime(gcLastRunTimeKey)
	c.Assert(err, IsNil)
	c.Assert(lastRun, NotNil)
	safePoint, err := s.gcWorker.loadTime(gcSafePointKey)
	c.Assert(err, IsNil)
	s.timeEqual(c, safePoint.Add(gcDefaultLifeTime), now, 2*time.Second)

	// Change GC run interval.
	err = s.gcWorker.saveDuration(gcRunIntervalKey, time.Minute*5)
	c.Assert(err, IsNil)
	s.oracle.AddOffset(time.Minute * 4)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	s.oracle.AddOffset(time.Minute * 2)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)

	// Change GC life time.
	err = s.gcWorker.saveDuration(gcLifeTimeKey, time.Minute*30)
	c.Assert(err, IsNil)
	s.oracle.AddOffset(time.Minute * 5)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	s.oracle.AddOffset(time.Minute * 40)
	now, err = s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)
	safePoint, err = s.gcWorker.loadTime(gcSafePointKey)
	c.Assert(err, IsNil)
	s.timeEqual(c, safePoint.Add(time.Minute*30), now, 2*time.Second)

	// Change GC concurrency.
	concurrency, err := s.gcWorker.loadGCConcurrencyWithDefault()
	c.Assert(err, IsNil)
	c.Assert(concurrency, Equals, gcDefaultConcurrency)

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcMinConcurrency))
	c.Assert(err, IsNil)
	concurrency, err = s.gcWorker.loadGCConcurrencyWithDefault()
	c.Assert(err, IsNil)
	c.Assert(concurrency, Equals, gcMinConcurrency)

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(-1))
	c.Assert(err, IsNil)
	concurrency, err = s.gcWorker.loadGCConcurrencyWithDefault()
	c.Assert(err, IsNil)
	c.Assert(concurrency, Equals, gcMinConcurrency)

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(1000000))
	c.Assert(err, IsNil)
	concurrency, err = s.gcWorker.loadGCConcurrencyWithDefault()
	c.Assert(err, IsNil)
	c.Assert(concurrency, Equals, gcMaxConcurrency)

	// Change GC enable status.
	s.oracle.AddOffset(time.Minute * 40)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanFalse)
	c.Assert(err, IsNil)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanTrue)
	c.Assert(err, IsNil)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)

	// Check gc life time small than min.
	s.oracle.AddOffset(time.Minute * 40)
	err = s.gcWorker.saveDuration(gcLifeTimeKey, time.Minute)
	c.Assert(err, IsNil)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)
	lifeTime, err := s.gcWorker.loadDuration(gcLifeTimeKey)
	c.Assert(err, IsNil)
	c.Assert(*lifeTime, Equals, gcMinLifeTime)

	s.oracle.AddOffset(time.Minute * 40)
	err = s.gcWorker.saveDuration(gcLifeTimeKey, time.Minute*30)
	c.Assert(err, IsNil)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)
	lifeTime, err = s.gcWorker.loadDuration(gcLifeTimeKey)
	c.Assert(err, IsNil)
	c.Assert(*lifeTime, Equals, 30*time.Minute)

	// Change auto concurrency
	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanFalse)
	c.Assert(err, IsNil)
	useAutoConcurrency, err := s.gcWorker.checkUseAutoConcurrency()
	c.Assert(err, IsNil)
	c.Assert(useAutoConcurrency, IsFalse)
	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanTrue)
	c.Assert(err, IsNil)
	useAutoConcurrency, err = s.gcWorker.checkUseAutoConcurrency()
	c.Assert(err, IsNil)
	c.Assert(useAutoConcurrency, IsTrue)
}

func (s *testGCWorkerSuite) TestDoGCForOneRegion(c *C) {
	ctx := context.Background()
	bo := tikv.NewBackofferWithVars(ctx, tikv.GcOneRegionMaxBackoff, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte(""))
	c.Assert(err, IsNil)
	var regionErr *errorpb.Error

	p := s.createGCProbe(c, "k1")
	regionErr, err = s.gcWorker.doGCForRegion(bo, s.mustAllocTs(c), loc.Region)
	c.Assert(regionErr, IsNil)
	c.Assert(err, IsNil)
	s.checkCollected(c, p)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("timeout")`), IsNil)
	regionErr, err = s.gcWorker.doGCForRegion(bo, s.mustAllocTs(c), loc.Region)
	c.Assert(regionErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult"), IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("GCNotLeader")`), IsNil)
	regionErr, err = s.gcWorker.doGCForRegion(bo, s.mustAllocTs(c), loc.Region)
	c.Assert(regionErr.GetNotLeader(), NotNil)
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult"), IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("GCServerIsBusy")`), IsNil)
	regionErr, err = s.gcWorker.doGCForRegion(bo, s.mustAllocTs(c), loc.Region)
	c.Assert(regionErr.GetServerIsBusy(), NotNil)
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult"), IsNil)
}

func (s *testGCWorkerSuite) TestGetGCConcurrency(c *C) {
	// Pick a concurrency that doesn't equal to the number of stores.
	concurrencyConfig := 25
	c.Assert(concurrencyConfig, Not(Equals), len(s.cluster.GetAllStores()))
	err := s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(concurrencyConfig))
	c.Assert(err, IsNil)

	ctx := context.Background()

	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanFalse)
	c.Assert(err, IsNil)
	concurrency, err := s.gcWorker.getGCConcurrency(ctx)
	c.Assert(err, IsNil)
	c.Assert(concurrency, Equals, concurrencyConfig)

	err = s.gcWorker.saveValueToSysTable(gcAutoConcurrencyKey, booleanTrue)
	c.Assert(err, IsNil)
	concurrency, err = s.gcWorker.getGCConcurrency(ctx)
	c.Assert(err, IsNil)
	c.Assert(concurrency, Equals, len(s.cluster.GetAllStores()))
}

func (s *testGCWorkerSuite) TestDoGC(c *C) {
	var err error
	ctx := context.Background()

	gcSafePointCacheInterval = 1

	p := s.createGCProbe(c, "k1")
	err = s.gcWorker.doGC(ctx, s.mustAllocTs(c), gcDefaultConcurrency)
	c.Assert(err, IsNil)
	s.checkCollected(c, p)

	p = s.createGCProbe(c, "k1")
	err = s.gcWorker.doGC(ctx, s.mustAllocTs(c), gcMinConcurrency)
	c.Assert(err, IsNil)
	s.checkCollected(c, p)

	p = s.createGCProbe(c, "k1")
	err = s.gcWorker.doGC(ctx, s.mustAllocTs(c), gcMaxConcurrency)
	c.Assert(err, IsNil)
	s.checkCollected(c, p)
}

func (s *testGCWorkerSuite) TestCheckGCMode(c *C) {
	useDistributedGC, err := s.gcWorker.checkUseDistributedGC()
	c.Assert(err, IsNil)
	c.Assert(useDistributedGC, Equals, true)
	// Now the row must be set to the default value.
	str, err := s.gcWorker.loadValueFromSysTable(gcModeKey)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, gcModeDistributed)

	err = s.gcWorker.saveValueToSysTable(gcModeKey, gcModeCentral)
	c.Assert(err, IsNil)
	useDistributedGC, err = s.gcWorker.checkUseDistributedGC()
	c.Assert(err, IsNil)
	c.Assert(useDistributedGC, Equals, false)

	err = s.gcWorker.saveValueToSysTable(gcModeKey, gcModeDistributed)
	c.Assert(err, IsNil)
	useDistributedGC, err = s.gcWorker.checkUseDistributedGC()
	c.Assert(err, IsNil)
	c.Assert(useDistributedGC, Equals, true)

	err = s.gcWorker.saveValueToSysTable(gcModeKey, "invalid_mode")
	c.Assert(err, IsNil)
	useDistributedGC, err = s.gcWorker.checkUseDistributedGC()
	c.Assert(err, IsNil)
	c.Assert(useDistributedGC, Equals, true)
}

func (s *testGCWorkerSuite) TestCheckScanLockMode(c *C) {
	usePhysical, err := s.gcWorker.checkUsePhysicalScanLock()
	c.Assert(err, IsNil)
	c.Assert(usePhysical, Equals, gcScanLockModeDefault == gcScanLockModePhysical)
	// Now the row must be set to the default value.
	str, err := s.gcWorker.loadValueFromSysTable(gcScanLockModeKey)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, gcScanLockModeDefault)

	err = s.gcWorker.saveValueToSysTable(gcScanLockModeKey, gcScanLockModeLegacy)
	c.Assert(err, IsNil)
	usePhysical, err = s.gcWorker.checkUsePhysicalScanLock()
	c.Assert(err, IsNil)
	c.Assert(usePhysical, Equals, false)

	err = s.gcWorker.saveValueToSysTable(gcScanLockModeKey, gcScanLockModePhysical)
	c.Assert(err, IsNil)
	usePhysical, err = s.gcWorker.checkUsePhysicalScanLock()
	c.Assert(err, IsNil)
	c.Assert(usePhysical, Equals, true)

	err = s.gcWorker.saveValueToSysTable(gcScanLockModeKey, "invalid_mode")
	c.Assert(err, IsNil)
	usePhysical, err = s.gcWorker.checkUsePhysicalScanLock()
	c.Assert(err, IsNil)
	c.Assert(usePhysical, Equals, false)
}

func (s *testGCWorkerSuite) TestNeedsGCOperationForStore(c *C) {
	newStore := func(state metapb.StoreState, hasEngineLabel bool, engineLabel string) *metapb.Store {
		store := &metapb.Store{}
		store.State = state
		if hasEngineLabel {
			store.Labels = []*metapb.StoreLabel{{Key: engineLabelKey, Value: engineLabel}}
		}
		return store
	}

	// TiKV needs to do the store-level GC operations.
	for _, state := range []metapb.StoreState{metapb.StoreState_Up, metapb.StoreState_Offline, metapb.StoreState_Tombstone} {
		needGC := state != metapb.StoreState_Tombstone
		res, err := needsGCOperationForStore(newStore(state, false, ""))
		c.Assert(err, IsNil)
		c.Assert(res, Equals, needGC)
		res, err = needsGCOperationForStore(newStore(state, true, ""))
		c.Assert(err, IsNil)
		c.Assert(res, Equals, needGC)
		res, err = needsGCOperationForStore(newStore(state, true, engineLabelTiKV))
		c.Assert(err, IsNil)
		c.Assert(res, Equals, needGC)

		// TiFlash does not need these operations.
		res, err = needsGCOperationForStore(newStore(state, true, engineLabelTiFlash))
		c.Assert(err, IsNil)
		c.Assert(res, IsFalse)
	}
	// Throw an error for unknown store types.
	_, err := needsGCOperationForStore(newStore(metapb.StoreState_Up, true, "invalid"))
	c.Assert(err, NotNil)
}

const (
	failRPCErr  = 0
	failNilResp = 1
	failErrResp = 2
)

func (s *testGCWorkerSuite) testDeleteRangesFailureImpl(c *C, failType int) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gcworker/mockHistoryJobForGC", "return(1)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gcworker/mockHistoryJobForGC"), IsNil)
	}()

	// Put some delete range tasks.
	se := createSession(s.gcWorker.store)
	defer se.Close()
	_, err := se.Execute(context.Background(), `INSERT INTO mysql.gc_delete_range VALUES
		("1", "2", "31", "32", "10"),
		("3", "4", "33", "34", "10"),
		("5", "6", "35", "36", "10")`)
	c.Assert(err, IsNil)

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

	// Check the delete range tasks.
	preparedRanges, err := util.LoadDeleteRanges(se, 20)
	se.Close()
	c.Assert(err, IsNil)
	c.Assert(preparedRanges, DeepEquals, ranges)

	stores, err := s.gcWorker.getStoresForGC(context.Background())
	c.Assert(err, IsNil)
	c.Assert(len(stores), Equals, 3)

	// Sort by address for checking.
	sort.Slice(stores, func(i, j int) bool { return stores[i].Address < stores[j].Address })

	sendReqCh := make(chan SentReq, 20)

	// The request sent to the specified key and store wil fail.
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

		err = deleteRangeFunc(context.Background(), 20, 1)
		c.Assert(err, IsNil)

		s.checkDestroyRangeReq(c, sendReqCh, ranges, stores)

		// The first delete range task should be still here since it didn't success.
		se = createSession(s.gcWorker.store)
		remainingRanges, err := loadRangesFunc(se, 20)
		se.Close()
		c.Assert(err, IsNil)
		c.Assert(remainingRanges, DeepEquals, ranges[:1])

		failKey = nil
		failStore = nil

		// Delete the remaining range again.
		err = deleteRangeFunc(context.Background(), 20, 1)
		c.Assert(err, IsNil)
		s.checkDestroyRangeReq(c, sendReqCh, ranges[:1], stores)

		se = createSession(s.gcWorker.store)
		remainingRanges, err = loadRangesFunc(se, 20)
		se.Close()
		c.Assert(err, IsNil)
		c.Assert(len(remainingRanges), Equals, 0)
	}

	test(false)
	// Change the order because the first range is the last successfully deleted.
	ranges = append(ranges[1:], ranges[0])
	test(true)
}

func (s *testGCWorkerSuite) TestDeleteRangesFailure(c *C) {
	s.testDeleteRangesFailureImpl(c, failRPCErr)
	s.testDeleteRangesFailureImpl(c, failNilResp)
	s.testDeleteRangesFailureImpl(c, failErrResp)
}

type SentReq struct {
	req  *tikvrpc.Request
	addr string
}

// checkDestroyRangeReq checks whether given sentReq matches given ranges and stores.
func (s *testGCWorkerSuite) checkDestroyRangeReq(c *C, sendReqCh chan SentReq, expectedRanges []util.DelRangeTask, expectedStores []*metapb.Store) {
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
			c.Assert(sentReq[i].addr, Equals, expectedStores[storeIndex].Address)
			c.Assert(kv.Key(sentReq[i].req.UnsafeDestroyRange().GetStartKey()), DeepEquals,
				sortedRanges[rangeIndex].StartKey)
			c.Assert(kv.Key(sentReq[i].req.UnsafeDestroyRange().GetEndKey()), DeepEquals,
				sortedRanges[rangeIndex].EndKey)
		}
	}
}

type testGCWorkerClient struct {
	tikv.Client
	unsafeDestroyRangeHandler   handler
	physicalScanLockHandler     handler
	registerLockObserverHandler handler
	checkLockObserverHandler    handler
	removeLockObserverHandler   handler
}

type handler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error)

func (c *testGCWorkerClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
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

	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (s *testGCWorkerSuite) TestLeaderTick(c *C) {
	gcSafePointCacheInterval = 0

	veryLong := gcDefaultLifeTime * 10
	// Avoid failing at interval check. `lastFinish` is checked by os time.
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	// Use central mode to do this test.
	err := s.gcWorker.saveValueToSysTable(gcModeKey, gcModeCentral)
	c.Assert(err, IsNil)
	p := s.createGCProbe(c, "k1")
	s.oracle.AddOffset(gcDefaultLifeTime * 2)

	// Skip if GC is running.
	s.gcWorker.gcIsRunning = true
	err = s.gcWorker.leaderTick(context.Background())
	c.Assert(err, IsNil)
	s.checkNotCollected(c, p)
	s.gcWorker.gcIsRunning = false
	// Reset GC last run time
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(c)).Add(-veryLong))
	c.Assert(err, IsNil)

	// Skip if prepare failed (disabling GC will make prepare returns ok = false).
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanFalse)
	c.Assert(err, IsNil)
	err = s.gcWorker.leaderTick(context.Background())
	c.Assert(err, IsNil)
	s.checkNotCollected(c, p)
	err = s.gcWorker.saveValueToSysTable(gcEnableKey, booleanTrue)
	c.Assert(err, IsNil)
	// Reset GC last run time
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(c)).Add(-veryLong))
	c.Assert(err, IsNil)

	// Skip if gcWaitTime not exceeded.
	s.gcWorker.lastFinish = time.Now()
	err = s.gcWorker.leaderTick(context.Background())
	c.Assert(err, IsNil)
	s.checkNotCollected(c, p)
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	// Reset GC last run time
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(c)).Add(-veryLong))
	c.Assert(err, IsNil)

	// Continue GC if all those checks passed.
	err = s.gcWorker.leaderTick(context.Background())
	c.Assert(err, IsNil)
	// Wait for GC finish
	select {
	case err = <-s.gcWorker.done:
		s.gcWorker.gcIsRunning = false
		break
	case <-time.After(time.Second * 10):
		err = errors.New("receive from s.gcWorker.done timeout")
	}
	c.Assert(err, IsNil)
	s.checkCollected(c, p)

	// Test again to ensure the synchronization between goroutines is correct.
	err = s.gcWorker.saveTime(gcLastRunTimeKey, oracle.GetTimeFromTS(s.mustAllocTs(c)).Add(-veryLong))
	c.Assert(err, IsNil)
	s.gcWorker.lastFinish = time.Now().Add(-veryLong)
	p = s.createGCProbe(c, "k1")
	s.oracle.AddOffset(gcDefaultLifeTime * 2)

	err = s.gcWorker.leaderTick(context.Background())
	c.Assert(err, IsNil)
	// Wait for GC finish
	select {
	case err = <-s.gcWorker.done:
		s.gcWorker.gcIsRunning = false
		break
	case <-time.After(time.Second * 10):
		err = errors.New("receive from s.gcWorker.done timeout")
	}
	c.Assert(err, IsNil)
	s.checkCollected(c, p)

	// No more signals in the channel
	select {
	case err = <-s.gcWorker.done:
		err = errors.Errorf("received signal s.gcWorker.done which shouldn't exist: %v", err)
		break
	case <-time.After(time.Second):
		break
	}
	c.Assert(err, IsNil)
}

func (s *testGCWorkerSuite) TestResolveLockRangeInfine(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/invalidCacheAndRetry", "return(true)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gcworker/setGcResolveMaxBackoff", "return(1)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/invalidCacheAndRetry"), IsNil)
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gcworker/setGcResolveMaxBackoff"), IsNil)
	}()
	_, err := s.gcWorker.resolveLocksForRange(context.Background(), 1, []byte{0}, []byte{1})
	c.Assert(err, NotNil)
}

func (s *testGCWorkerSuite) TestResolveLockRangeMeetRegionCacheMiss(c *C) {
	var (
		scanCnt       int
		scanCntRef    = &scanCnt
		resolveCnt    int
		resolveCntRef = &resolveCnt
	)
	s.gcWorker.testingKnobs.scanLocks = func(key []byte) []*tikv.Lock {
		*scanCntRef++
		return []*tikv.Lock{
			{
				Key: []byte{1},
			},
			{
				Key: []byte{1},
			},
		}
	}
	s.gcWorker.testingKnobs.resolveLocks = func(regionID tikv.RegionVerID) (ok bool, err error) {
		*resolveCntRef++
		if *resolveCntRef == 1 {
			s.gcWorker.store.GetRegionCache().InvalidateCachedRegion(regionID)
			// mock the region cache miss error
			return false, nil
		}
		return true, nil
	}
	_, err := s.gcWorker.resolveLocksForRange(context.Background(), 1, []byte{0}, []byte{10})
	c.Assert(err, IsNil)
	c.Assert(resolveCnt, Equals, 2)
	c.Assert(scanCnt, Equals, 1)
}

func (s *testGCWorkerSuite) TestRunGCJob(c *C) {
	gcSafePointCacheInterval = 0

	// Test distributed mode
	useDistributedGC, err := s.gcWorker.checkUseDistributedGC()
	c.Assert(err, IsNil)
	c.Assert(useDistributedGC, IsTrue)
	safePoint := s.mustAllocTs(c)
	err = s.gcWorker.runGCJob(context.Background(), safePoint, 1)
	c.Assert(err, IsNil)

	pdSafePoint := s.mustGetSafePointFromPd(c)
	c.Assert(pdSafePoint, Equals, safePoint)

	etcdSafePoint := s.loadEtcdSafePoint(c)
	c.Assert(etcdSafePoint, Equals, safePoint)

	// Test distributed mode with safePoint regressing (although this is impossible)
	err = s.gcWorker.runGCJob(context.Background(), safePoint-1, 1)
	c.Assert(err, NotNil)

	// Test central mode
	err = s.gcWorker.saveValueToSysTable(gcModeKey, gcModeCentral)
	c.Assert(err, IsNil)
	useDistributedGC, err = s.gcWorker.checkUseDistributedGC()
	c.Assert(err, IsNil)
	c.Assert(useDistributedGC, IsFalse)

	p := s.createGCProbe(c, "k1")
	safePoint = s.mustAllocTs(c)
	err = s.gcWorker.runGCJob(context.Background(), safePoint, 1)
	c.Assert(err, IsNil)
	s.checkCollected(c, p)

	etcdSafePoint = s.loadEtcdSafePoint(c)
	c.Assert(etcdSafePoint, Equals, safePoint)
}

func (s *testGCWorkerSuite) TestSetServiceSafePoint(c *C) {
	// SafePoint calculations are based on time rather than ts value.
	safePoint := s.mustAllocTs(c)
	s.mustSetTiDBServiceSafePoint(c, safePoint, safePoint)
	c.Assert(s.mustGetMinServiceSafePointFromPd(c), Equals, safePoint)

	// Advance the service safe point
	safePoint += 100
	s.mustSetTiDBServiceSafePoint(c, safePoint, safePoint)
	c.Assert(s.mustGetMinServiceSafePointFromPd(c), Equals, safePoint)

	// It doesn't matter if there is a greater safePoint from other services.
	safePoint += 100
	// Returns the last service safePoint that were uploaded.
	s.mustUpdateServiceGCSafePoint(c, "svc1", safePoint+10, safePoint-100)
	s.mustSetTiDBServiceSafePoint(c, safePoint, safePoint)
	c.Assert(s.mustGetMinServiceSafePointFromPd(c), Equals, safePoint)

	// Test the case when there is a smaller safePoint from other services.
	safePoint += 100
	// Returns the last service safePoint that were uploaded.
	s.mustUpdateServiceGCSafePoint(c, "svc1", safePoint-10, safePoint-100)
	s.mustSetTiDBServiceSafePoint(c, safePoint, safePoint-10)
	c.Assert(s.mustGetMinServiceSafePointFromPd(c), Equals, safePoint-10)

	// Test removing the minimum service safe point.
	s.mustRemoveServiceGCSafePoint(c, "svc1", safePoint-10, safePoint)
	c.Assert(s.mustGetMinServiceSafePointFromPd(c), Equals, safePoint)

	// Test the case when there are many safePoints.
	safePoint += 100
	for i := 0; i < 10; i++ {
		svcName := fmt.Sprintf("svc%d", i)
		s.mustUpdateServiceGCSafePoint(c, svcName, safePoint+uint64(i)*10, safePoint-100)
	}
	s.mustSetTiDBServiceSafePoint(c, safePoint+50, safePoint)
}

func (s *testGCWorkerSuite) TestRunGCJobAPI(c *C) {
	gcSafePointCacheInterval = 0

	p := s.createGCProbe(c, "k1")
	safePoint := s.mustAllocTs(c)
	err := RunGCJob(context.Background(), s.store, s.pdClient, safePoint, "mock", 1)
	c.Assert(err, IsNil)
	s.checkCollected(c, p)
	etcdSafePoint := s.loadEtcdSafePoint(c)
	c.Assert(err, IsNil)
	c.Assert(etcdSafePoint, Equals, safePoint)
}

func (s *testGCWorkerSuite) TestRunDistGCJobAPI(c *C) {
	gcSafePointCacheInterval = 0

	safePoint := s.mustAllocTs(c)
	err := RunDistributedGCJob(context.Background(), s.store, s.pdClient, safePoint, "mock", 1)
	c.Assert(err, IsNil)
	pdSafePoint := s.mustGetSafePointFromPd(c)
	c.Assert(pdSafePoint, Equals, safePoint)
	etcdSafePoint := s.loadEtcdSafePoint(c)
	c.Assert(err, IsNil)
	c.Assert(etcdSafePoint, Equals, safePoint)
}

func (s *testGCWorkerSuite) TestStartWithRunGCJobFailures(c *C) {
	s.gcWorker.Start()
	defer s.gcWorker.Close()

	for i := 0; i < 3; i++ {
		select {
		case <-time.After(100 * time.Millisecond):
			c.Fatal("gc worker failed to handle errors")
		case s.gcWorker.done <- errors.New("mock error"):
		}
	}
}

func (s *testGCWorkerSuite) loadEtcdSafePoint(c *C) uint64 {
	val, err := s.gcWorker.store.GetSafePointKV().Get(tikv.GcSavedSafePoint)
	c.Assert(err, IsNil)
	res, err := strconv.ParseUint(val, 10, 64)
	c.Assert(err, IsNil)
	return res
}

func makeMergedChannel(c *C, count int) (*mergeLockScanner, []chan scanLockResult, []uint64, <-chan []*tikv.Lock) {
	scanner := &mergeLockScanner{}
	channels := make([]chan scanLockResult, 0, count)
	receivers := make([]*receiver, 0, count)
	storeIDs := make([]uint64, 0, count)

	for i := 0; i < count; i++ {
		ch := make(chan scanLockResult, 10)
		receiver := &receiver{
			Ch:      ch,
			StoreID: uint64(i),
		}

		channels = append(channels, ch)
		receivers = append(receivers, receiver)
		storeIDs = append(storeIDs, uint64(i))
	}

	resultCh := make(chan []*tikv.Lock)
	// Initializing and getting result from scanner is blocking operations. Collect the result in a separated thread.
	go func() {
		scanner.startWithReceivers(receivers)
		// Get a batch of a enough-large size to get all results.
		result := scanner.NextBatch(1000)
		c.Assert(len(result), Less, 1000)
		resultCh <- result
	}()

	return scanner, channels, storeIDs, resultCh
}

func (s *testGCWorkerSuite) makeMergedMockClient(c *C, count int) (*mergeLockScanner, []chan scanLockResult, []uint64, <-chan []*tikv.Lock) {
	stores := s.cluster.GetAllStores()
	c.Assert(count, Equals, len(stores))
	storeIDs := make([]uint64, count)
	for i := 0; i < count; i++ {
		storeIDs[i] = stores[i].Id
	}

	const scanLockLimit = 3

	storesMap, err := s.gcWorker.getStoresMapForGC(context.Background())
	c.Assert(err, IsNil)
	scanner := newMergeLockScanner(100000, s.client, storesMap)
	scanner.scanLockLimit = scanLockLimit
	channels := make([]chan scanLockResult, 0, len(stores))

	for range stores {
		ch := make(chan scanLockResult, 10)

		channels = append(channels, ch)
	}

	s.client.physicalScanLockHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		for i, store := range stores {
			if store.Address == addr {
				locks := make([]*kvrpcpb.LockInfo, 0, 3)
				errStr := ""
				for j := 0; j < scanLockLimit; j++ {
					res, ok := <-channels[i]
					if !ok {
						break
					}
					if res.Err != nil {
						errStr = res.Err.Error()
						locks = nil
						break
					}
					lockInfo := &kvrpcpb.LockInfo{Key: res.Lock.Key, LockVersion: res.Lock.TxnID}
					locks = append(locks, lockInfo)
				}

				return &tikvrpc.Response{
					Resp: &kvrpcpb.PhysicalScanLockResponse{
						Locks: locks,
						Error: errStr,
					},
				}, nil
			}
		}
		return nil, errors.Errorf("No store in the cluster has address %v", addr)
	}

	resultCh := make(chan []*tikv.Lock)
	// Initializing and getting result from scanner is blocking operations. Collect the result in a separated thread.
	go func() {
		err := scanner.Start(context.Background())
		c.Assert(err, IsNil)
		// Get a batch of a enough-large size to get all results.
		result := scanner.NextBatch(1000)
		c.Assert(len(result), Less, 1000)
		resultCh <- result
	}()

	return scanner, channels, storeIDs, resultCh
}

func (s *testGCWorkerSuite) TestMergeLockScanner(c *C) {
	// Shortcuts to make the following test code simpler

	// Get stores by index, and get their store IDs.
	makeIDSet := func(storeIDs []uint64, indices ...uint64) map[uint64]interface{} {
		res := make(map[uint64]interface{})
		for _, i := range indices {
			res[storeIDs[i]] = nil
		}
		return res
	}

	makeLock := func(key string, ts uint64) *tikv.Lock {
		return &tikv.Lock{Key: []byte(key), TxnID: ts}
	}

	makeLockList := func(locks ...*tikv.Lock) []*tikv.Lock {
		res := make([]*tikv.Lock, 0, len(locks))
		for _, lock := range locks {
			res = append(res, lock)
		}
		return res
	}

	makeLockListByKey := func(keys ...string) []*tikv.Lock {
		res := make([]*tikv.Lock, 0, len(keys))
		for _, key := range keys {
			res = append(res, makeLock(key, 0))
		}
		return res
	}

	sendLocks := func(ch chan<- scanLockResult, locks ...*tikv.Lock) {
		for _, lock := range locks {
			ch <- scanLockResult{Lock: lock}
		}
	}

	sendLocksByKey := func(ch chan<- scanLockResult, keys ...string) []*tikv.Lock {
		locks := make([]*tikv.Lock, 0, len(keys))
		for _, key := range keys {
			locks = append(locks, makeLock(key, 0))
		}
		sendLocks(ch, locks...)
		return locks
	}

	sendErr := func(ch chan<- scanLockResult) {
		ch <- scanLockResult{Err: errors.New("error")}
	}

	// No lock.
	scanner, sendCh, storeIDs, resCh := makeMergedChannel(c, 1)
	close(sendCh[0])
	c.Assert(len(<-resCh), Equals, 0)
	c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs, 0))

	scanner, sendCh, storeIDs, resCh = makeMergedChannel(c, 1)
	locks := sendLocksByKey(sendCh[0], "a", "b", "c")
	close(sendCh[0])
	c.Assert(<-resCh, DeepEquals, locks)
	c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs, 0))

	// Send locks with error
	scanner, sendCh, storeIDs, resCh = makeMergedChannel(c, 1)
	locks = sendLocksByKey(sendCh[0], "a", "b", "c")
	sendErr(sendCh[0])
	close(sendCh[0])
	c.Assert(<-resCh, DeepEquals, locks)
	c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs))

	// Merge sort locks with different keys.
	scanner, sendCh, storeIDs, resCh = makeMergedChannel(c, 2)
	locks = sendLocksByKey(sendCh[0], "a", "c", "e")
	time.Sleep(time.Millisecond * 100)
	locks = append(locks, sendLocksByKey(sendCh[1], "b", "d", "f")...)
	close(sendCh[0])
	close(sendCh[1])
	sort.Slice(locks, func(i, j int) bool {
		return bytes.Compare(locks[i].Key, locks[j].Key) < 0
	})
	c.Assert(<-resCh, DeepEquals, locks)
	c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs, 0, 1))

	// Merge sort locks with different timestamps.
	scanner, sendCh, storeIDs, resCh = makeMergedChannel(c, 2)
	sendLocks(sendCh[0], makeLock("a", 0), makeLock("a", 1))
	time.Sleep(time.Millisecond * 100)
	sendLocks(sendCh[1], makeLock("a", 1), makeLock("a", 2), makeLock("b", 0))
	close(sendCh[0])
	close(sendCh[1])
	c.Assert(<-resCh, DeepEquals, makeLockList(makeLock("a", 0), makeLock("a", 1), makeLock("a", 2), makeLock("b", 0)))
	c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs, 0, 1))

	for _, useMock := range []bool{false, true} {
		channel := makeMergedChannel
		if useMock == true {
			channel = s.makeMergedMockClient
		}

		scanner, sendCh, storeIDs, resCh = channel(c, 3)
		sendLocksByKey(sendCh[0], "a", "d", "g", "h")
		time.Sleep(time.Millisecond * 100)
		sendLocksByKey(sendCh[1], "a", "d", "f", "h")
		time.Sleep(time.Millisecond * 100)
		sendLocksByKey(sendCh[2], "b", "c", "e", "h")
		close(sendCh[0])
		close(sendCh[1])
		close(sendCh[2])
		c.Assert(<-resCh, DeepEquals, makeLockListByKey("a", "b", "c", "d", "e", "f", "g", "h"))
		c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs, 0, 1, 2))

		scanner, sendCh, storeIDs, resCh = channel(c, 3)
		sendLocksByKey(sendCh[0], "a", "d", "g", "h")
		time.Sleep(time.Millisecond * 100)
		sendLocksByKey(sendCh[1], "a", "d", "f", "h")
		time.Sleep(time.Millisecond * 100)
		sendLocksByKey(sendCh[2], "b", "c", "e", "h")
		sendErr(sendCh[0])
		close(sendCh[0])
		close(sendCh[1])
		close(sendCh[2])
		c.Assert(<-resCh, DeepEquals, makeLockListByKey("a", "b", "c", "d", "e", "f", "g", "h"))
		c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs, 1, 2))

		scanner, sendCh, storeIDs, resCh = channel(c, 3)
		sendLocksByKey(sendCh[0], "a\x00", "a\x00\x00", "b", "b\x00")
		sendLocksByKey(sendCh[1], "a", "a\x00\x00", "a\x00\x00\x00", "c")
		sendLocksByKey(sendCh[2], "1", "a\x00", "a\x00\x00", "b")
		close(sendCh[0])
		close(sendCh[1])
		close(sendCh[2])
		c.Assert(<-resCh, DeepEquals, makeLockListByKey("1", "a", "a\x00", "a\x00\x00", "a\x00\x00\x00", "b", "b\x00", "c"))
		c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs, 0, 1, 2))

		scanner, sendCh, storeIDs, resCh = channel(c, 3)
		sendLocks(sendCh[0], makeLock("a", 0), makeLock("d", 0), makeLock("g", 0), makeLock("h", 0))
		sendLocks(sendCh[1], makeLock("a", 1), makeLock("b", 0), makeLock("c", 0), makeLock("d", 1))
		sendLocks(sendCh[2], makeLock("e", 0), makeLock("g", 1), makeLock("g", 2), makeLock("h", 0))
		close(sendCh[0])
		close(sendCh[1])
		close(sendCh[2])
		c.Assert(<-resCh, DeepEquals, makeLockList(makeLock("a", 0), makeLock("a", 1), makeLock("b", 0), makeLock("c", 0),
			makeLock("d", 0), makeLock("d", 1), makeLock("e", 0), makeLock("g", 0), makeLock("g", 1), makeLock("g", 2), makeLock("h", 0)))
		c.Assert(scanner.GetSucceededStores(), DeepEquals, makeIDSet(storeIDs, 0, 1, 2))
	}
}

func (s *testGCWorkerSuite) TestResolveLocksPhysical(c *C) {
	alwaysSucceedHanlder := func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		switch req.Type {
		case tikvrpc.CmdPhysicalScanLock:
			return &tikvrpc.Response{Resp: &kvrpcpb.PhysicalScanLockResponse{Locks: nil, Error: ""}}, nil
		case tikvrpc.CmdRegisterLockObserver:
			return &tikvrpc.Response{Resp: &kvrpcpb.RegisterLockObserverResponse{Error: ""}}, nil
		case tikvrpc.CmdCheckLockObserver:
			return &tikvrpc.Response{Resp: &kvrpcpb.CheckLockObserverResponse{Error: "", IsClean: true, Locks: nil}}, nil
		case tikvrpc.CmdRemoveLockObserver:
			return &tikvrpc.Response{Resp: &kvrpcpb.RemoveLockObserverResponse{Error: ""}}, nil
		default:
			panic("unreachable")
		}
	}
	alwaysFailHandler := func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		switch req.Type {
		case tikvrpc.CmdPhysicalScanLock:
			return &tikvrpc.Response{Resp: &kvrpcpb.PhysicalScanLockResponse{Locks: nil, Error: "error"}}, nil
		case tikvrpc.CmdRegisterLockObserver:
			return &tikvrpc.Response{Resp: &kvrpcpb.RegisterLockObserverResponse{Error: "error"}}, nil
		case tikvrpc.CmdCheckLockObserver:
			return &tikvrpc.Response{Resp: &kvrpcpb.CheckLockObserverResponse{Error: "error", IsClean: false, Locks: nil}}, nil
		case tikvrpc.CmdRemoveLockObserver:
			return &tikvrpc.Response{Resp: &kvrpcpb.RemoveLockObserverResponse{Error: "error"}}, nil
		default:
			panic("unreachable")
		}
	}
	reset := func() {
		s.client.physicalScanLockHandler = alwaysSucceedHanlder
		s.client.registerLockObserverHandler = alwaysSucceedHanlder
		s.client.checkLockObserverHandler = alwaysSucceedHanlder
		s.client.removeLockObserverHandler = alwaysSucceedHanlder
	}

	ctx := context.Background()
	var safePoint uint64 = 10000

	// No lock
	reset()
	physicalUsed, err := s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
	c.Assert(physicalUsed, IsTrue)
	c.Assert(err, IsNil)

	// Should fall back on the legacy mode when fails to register lock observers.
	reset()
	s.client.registerLockObserverHandler = alwaysFailHandler
	physicalUsed, err = s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
	c.Assert(physicalUsed, IsFalse)
	c.Assert(err, IsNil)

	// Should fall back when fails to resolve locks.
	reset()
	s.client.physicalScanLockHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		locks := []*kvrpcpb.LockInfo{{Key: []byte{0}}}
		return &tikvrpc.Response{Resp: &kvrpcpb.PhysicalScanLockResponse{Locks: locks, Error: ""}}, nil
	}
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gcworker/resolveLocksAcrossRegionsErr", "return(100)"), IsNil)
	physicalUsed, err = s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
	c.Assert(physicalUsed, IsFalse)
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gcworker/resolveLocksAcrossRegionsErr"), IsNil)

	// Shouldn't fall back when fails to scan locks less than 3 times.
	reset()
	var returnError uint32 = 1
	s.client.physicalScanLockHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		if atomic.CompareAndSwapUint32(&returnError, 1, 0) {
			return alwaysFailHandler(addr, req)
		}
		return alwaysSucceedHanlder(addr, req)
	}
	physicalUsed, err = s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
	c.Assert(physicalUsed, IsTrue)
	c.Assert(err, IsNil)

	// Should fall back if reaches retry limit
	reset()
	s.client.physicalScanLockHandler = alwaysFailHandler
	physicalUsed, err = s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
	c.Assert(physicalUsed, IsFalse)
	c.Assert(err, IsNil)

	// Should fall back when one registered store is dirty.
	reset()
	s.client.checkLockObserverHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		return &tikvrpc.Response{Resp: &kvrpcpb.CheckLockObserverResponse{Error: "", IsClean: false, Locks: nil}}, nil
	}
	physicalUsed, err = s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
	c.Assert(physicalUsed, IsFalse)
	c.Assert(err, IsNil)

	// When fails to check lock observer in a store, we assume the store is dirty.
	// Should fall back when fails to check lock observers.
	reset()
	s.client.checkLockObserverHandler = alwaysFailHandler
	physicalUsed, err = s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
	c.Assert(physicalUsed, IsFalse)
	c.Assert(err, IsNil)

	// Shouldn't fall back when the dirty store is newly added.
	reset()
	var wg sync.WaitGroup
	wg.Add(1)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gcworker/beforeCheckLockObservers", "pause"), IsNil)
	go func() {
		defer wg.Done()
		physicalUsed, err := s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
		c.Assert(physicalUsed, IsTrue)
		c.Assert(err, IsNil)
	}()
	// Sleep to let the goroutine pause.
	time.Sleep(500 * time.Millisecond)
	s.cluster.AddStore(100, "store100")
	once := true
	s.client.checkLockObserverHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		// The newly added store returns IsClean=false for the first time.
		if addr == "store100" && once {
			once = false
			return &tikvrpc.Response{Resp: &kvrpcpb.CheckLockObserverResponse{Error: "", IsClean: false, Locks: nil}}, nil
		}
		return alwaysSucceedHanlder(addr, req)
	}
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gcworker/beforeCheckLockObservers"), IsNil)
	wg.Wait()

	// Shouldn't fall back when a store is removed.
	reset()
	wg.Add(1)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gcworker/beforeCheckLockObservers", "pause"), IsNil)
	go func() {
		defer wg.Done()
		physicalUsed, err := s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
		c.Assert(physicalUsed, IsTrue)
		c.Assert(err, IsNil)
	}()
	// Sleep to let the goroutine pause.
	time.Sleep(500 * time.Millisecond)
	s.cluster.RemoveStore(100)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gcworker/beforeCheckLockObservers"), IsNil)
	wg.Wait()

	// Should fall back when a cleaned store becomes dirty.
	reset()
	wg.Add(1)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gcworker/beforeCheckLockObservers", "pause"), IsNil)
	go func() {
		defer wg.Done()
		physicalUsed, err := s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
		c.Assert(physicalUsed, IsFalse)
		c.Assert(err, IsNil)
	}()
	// Sleep to let the goroutine pause.
	time.Sleep(500 * time.Millisecond)
	store := s.cluster.GetAllStores()[0]
	var onceClean uint32 = 1
	s.cluster.AddStore(100, "store100")
	var onceDirty uint32 = 1
	s.client.checkLockObserverHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		switch addr {
		case "store100":
			// The newly added store returns IsClean=false for the first time.
			if atomic.CompareAndSwapUint32(&onceDirty, 1, 0) {
				return &tikvrpc.Response{Resp: &kvrpcpb.CheckLockObserverResponse{Error: "", IsClean: false, Locks: nil}}, nil
			}
			return alwaysSucceedHanlder(addr, req)
		case store.Address:
			// The store returns IsClean=true for the first time.
			if atomic.CompareAndSwapUint32(&onceClean, 1, 0) {
				return alwaysSucceedHanlder(addr, req)
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.CheckLockObserverResponse{Error: "", IsClean: false, Locks: nil}}, nil
		default:
			return alwaysSucceedHanlder(addr, req)
		}
	}
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gcworker/beforeCheckLockObservers"), IsNil)
	wg.Wait()

	// Shouldn't fall back when fails to remove lock observers.
	reset()
	s.client.removeLockObserverHandler = alwaysFailHandler
	physicalUsed, err = s.gcWorker.resolveLocks(ctx, safePoint, 3, true)
	c.Assert(physicalUsed, IsTrue)
	c.Assert(err, IsNil)
}

func (s *testGCWorkerSuite) TestPhyscailScanLockDeadlock(c *C) {
	ctx := context.Background()
	stores := s.cluster.GetAllStores()
	c.Assert(len(stores), Greater, 1)

	s.client.physicalScanLockHandler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		c.Assert(addr, Equals, stores[0].Address)
		scanReq := req.PhysicalScanLock()
		scanLockLimit := int(scanReq.Limit)
		locks := make([]*kvrpcpb.LockInfo, 0, scanReq.Limit)
		for i := 0; i < scanLockLimit; i++ {
			// The order of keys doesn't matter.
			locks = append(locks, &kvrpcpb.LockInfo{Key: []byte{byte(i)}})
		}
		return &tikvrpc.Response{
			Resp: &kvrpcpb.PhysicalScanLockResponse{
				Locks: locks,
				Error: "",
			},
		}, nil
	}

	// Sleep 1000ms to let the main goroutine block on sending tasks.
	// Inject error to the goroutine resolving locks so that the main goroutine will block forever if it doesn't handle channels properly.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gcworker/resolveLocksAcrossRegionsErr", "return(1000)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gcworker/resolveLocksAcrossRegionsErr"), IsNil)
	}()

	done := make(chan interface{})
	go func() {
		defer close(done)
		storesMap := map[uint64]*metapb.Store{stores[0].Id: stores[0]}
		succeeded, err := s.gcWorker.physicalScanAndResolveLocks(ctx, 10000, storesMap)
		c.Assert(succeeded, IsNil)
		c.Assert(err, ErrorMatches, "injectedError")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		c.Fatal("physicalScanAndResolveLocks blocks")
	}
}

func (s *testGCWorkerSuite) TestGCPlacementRules(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gcworker/mockHistoryJobForGC", "return(1)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gcworker/mockHistoryJobForGC"), IsNil)
	}()

	dr := util.DelRangeTask{JobID: 1, ElementID: 1}
	pid, err := s.gcWorker.doGCPlacementRules(dr)
	c.Assert(pid, Equals, int64(1))
	c.Assert(err, IsNil)
}
