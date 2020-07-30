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
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/v3/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockoracle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testGCWorkerSuite struct {
	store    tikv.Storage
	cluster  *mocktikv.Cluster
	oracle   *mockoracle.MockOracle
	gcWorker *GCWorker
	dom      *domain.Domain
	pdClient pd.Client
}

var _ = Suite(&testGCWorkerSuite{})

func (s *testGCWorkerSuite) SetUpTest(c *C) {
	tikv.NewGCHandlerFunc = NewGCWorker

	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	store, err := mockstore.NewMockTikvStore(mockstore.WithCluster(s.cluster))

	s.store = store.(tikv.Storage)
	c.Assert(err, IsNil)
	s.oracle = &mockoracle.MockOracle{}
	s.store.SetOracle(s.oracle)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	s.pdClient = mocktikv.NewPDClient(s.cluster)
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

func (s *testGCWorkerSuite) TestGetOracleTime(c *C) {
	t1, err := s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	s.timeEqual(c, time.Now(), t1, time.Millisecond*10)

	s.oracle.AddOffset(time.Second * 10)
	t2, err := s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	s.timeEqual(c, t2, t1.Add(time.Second*10), time.Millisecond*10)
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

	// Check gc life time small than config.max-txn-use-time
	s.oracle.AddOffset(time.Minute * 40)
	config.GetGlobalConfig().TiKVClient.MaxTxnTimeUse = 20*60 - 10 // 20min - 10s
	err = s.gcWorker.saveDuration(gcLifeTimeKey, time.Minute)
	c.Assert(err, IsNil)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)
	lifeTime, err = s.gcWorker.loadDuration(gcLifeTimeKey)
	c.Assert(err, IsNil)
	c.Assert(*lifeTime, Equals, 20*time.Minute)

	// check the tikv_gc_life_time more than config.max-txn-use-time situation.
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
	var successRegions int32
	var failedRegions int32
	taskWorker := newGCTaskWorker(s.store, nil, nil, s.gcWorker.uuid, &successRegions, &failedRegions)

	ctx := context.Background()
	bo := tikv.NewBackoffer(ctx, tikv.GcOneRegionMaxBackoff)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte(""))
	c.Assert(err, IsNil)
	var regionErr *errorpb.Error
	regionErr, err = taskWorker.doGCForRegion(bo, 20, loc.Region)
	c.Assert(regionErr, IsNil)
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("timeout")`), IsNil)
	regionErr, err = taskWorker.doGCForRegion(bo, 20, loc.Region)
	c.Assert(regionErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult"), IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("GCNotLeader")`), IsNil)
	regionErr, err = taskWorker.doGCForRegion(bo, 20, loc.Region)
	c.Assert(regionErr.GetNotLeader(), NotNil)
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult"), IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("GCServerIsBusy")`), IsNil)
	regionErr, err = taskWorker.doGCForRegion(bo, 20, loc.Region)
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

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcDefaultConcurrency))
	c.Assert(err, IsNil)
	concurrency, err := s.gcWorker.loadGCConcurrencyWithDefault()
	c.Assert(err, IsNil)
	err = s.gcWorker.doGC(ctx, 20, concurrency)
	c.Assert(err, IsNil)

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcMinConcurrency))
	c.Assert(err, IsNil)
	concurrency, err = s.gcWorker.loadGCConcurrencyWithDefault()
	c.Assert(err, IsNil)
	err = s.gcWorker.doGC(ctx, 20, concurrency)
	c.Assert(err, IsNil)

	err = s.gcWorker.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcMaxConcurrency))
	c.Assert(err, IsNil)
	concurrency, err = s.gcWorker.loadGCConcurrencyWithDefault()
	c.Assert(err, IsNil)
	err = s.gcWorker.doGC(ctx, 20, concurrency)
	c.Assert(err, IsNil)
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

func (s *testGCWorkerSuite) TestNeedsGCOperationForStore(c *C) {
	newStore := func(hasEngineLabel bool, engineLabel string) *metapb.Store {
		store := &metapb.Store{}
		if hasEngineLabel {
			store.Labels = []*metapb.StoreLabel{{Key: engineLabelKey, Value: engineLabel}}
		}
		return store
	}

	// TiKV needs to do the store-level GC operations.
	res, err := needsGCOperationForStore(newStore(false, ""))
	c.Assert(err, IsNil)
	c.Assert(res, IsTrue)
	res, err = needsGCOperationForStore(newStore(true, ""))
	c.Assert(err, IsNil)
	c.Assert(res, IsTrue)
	res, err = needsGCOperationForStore(newStore(true, engineLabelTiKV))
	c.Assert(err, IsNil)
	c.Assert(res, IsTrue)

	// TiFlash does not need these operations.
	res, err = needsGCOperationForStore(newStore(true, engineLabelTiFlash))
	c.Assert(err, IsNil)
	c.Assert(res, IsFalse)

	// Throw an error for unknown store types.
	_, err = needsGCOperationForStore(newStore(true, "invalid"))
	c.Assert(err, NotNil)
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
	err := RunGCJob(context.Background(), s.store, 0, "mock", 1)
	c.Assert(err, IsNil)
	gcWorker, err := NewGCWorker(s.store, s.pdClient)
	c.Assert(err, IsNil)
	gcWorker.Start()
	useDistributedGC, err := gcWorker.(*GCWorker).checkUseDistributedGC()
	c.Assert(useDistributedGC, IsTrue)
	c.Assert(err, IsNil)
	safePoint := uint64(time.Now().Unix())
	gcWorker.(*GCWorker).runGCJob(context.Background(), safePoint, 1)
	getSafePoint, err := loadSafePoint(gcWorker.(*GCWorker).store.GetSafePointKV())
	c.Assert(err, IsNil)
	c.Assert(getSafePoint, Equals, safePoint)
	gcWorker.Close()
}

func loadSafePoint(kv tikv.SafePointKV) (uint64, error) {
	val, err := kv.Get(tikv.GcSavedSafePoint)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(val, 10, 64)
}
