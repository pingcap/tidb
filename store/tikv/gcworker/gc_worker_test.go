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

<<<<<<< HEAD
=======
func (s *testGCWorkerSuite) TestCheckScanLockMode(c *C) {
	usePhysical, err := s.gcWorker.checkUsePhysicalScanLock()
	c.Assert(err, IsNil)
	c.Assert(usePhysical, Equals, true)
	// This is a hidden config, so default value will not be inserted to table.
	str, err := s.gcWorker.loadValueFromSysTable(gcScanLockModeKey)
	c.Assert(err, IsNil)
	c.Assert(str, Equals, "")

	err = s.gcWorker.saveValueToSysTable(gcScanLockModeKey, gcScanLockModePhysical)
	c.Assert(err, IsNil)
	usePhysical, err = s.gcWorker.checkUsePhysicalScanLock()
	c.Assert(err, IsNil)
	c.Assert(usePhysical, Equals, true)

	err = s.gcWorker.saveValueToSysTable(gcScanLockModeKey, gcScanLockModeLegacy)
	c.Assert(err, IsNil)
	usePhysical, err = s.gcWorker.checkUsePhysicalScanLock()
	c.Assert(err, IsNil)
	c.Assert(usePhysical, Equals, false)

	err = s.gcWorker.saveValueToSysTable(gcScanLockModeKey, "invalid_mode")
	c.Assert(err, IsNil)
	usePhysical, err = s.gcWorker.checkUsePhysicalScanLock()
	c.Assert(err, IsNil)
	c.Assert(usePhysical, Equals, false)
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

const (
	failRPCErr  = 0
	failNilResp = 1
	failErrResp = 2
)

func (s *testGCWorkerSuite) testDeleteRangesFailureImpl(c *C, failType int) {
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

	stores, err := s.gcWorker.getUpStoresForGC(context.Background())
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
	unsafeDestroyRangeHandler handler
	physicalScanLockHandler   handler
}

type handler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error)

func (c *testGCWorkerClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdUnsafeDestroyRange && c.unsafeDestroyRangeHandler != nil {
		return c.unsafeDestroyRangeHandler(addr, req)
	}
	if req.Type == tikvrpc.CmdPhysicalScanLock && c.physicalScanLockHandler != nil {
		return c.physicalScanLockHandler(addr, req)
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

>>>>>>> af5ebef... gc_worker: Skip TiFlash nodes when doing UnsafeDestroyRange and Green GC (#15505)
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
<<<<<<< HEAD
	c.Assert(getSafePoint, Equals, safePoint)
	gcWorker.Close()
=======
	return res
}

func makeMergedChannel(c *C, count int) (*mergeLockScanner, []chan scanLockResult, []uint64, <-chan []*tikv.Lock) {
	scanner := &mergeLockScanner{}
	channels := make([]chan scanLockResult, 0, count)
	receivers := make([]*receiver, 0, count)

	for i := 0; i < count; i++ {
		ch := make(chan scanLockResult, 10)
		receiver := &receiver{
			Ch:      ch,
			StoreID: uint64(i),
		}

		channels = append(channels, ch)
		receivers = append(receivers, receiver)
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

	storeIDs := make([]uint64, count)
	for i := 0; i < count; i++ {
		storeIDs[i] = uint64(i)
	}

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

	storesMap, err := s.gcWorker.getUpStoresMapForGC(context.Background())
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
					lockInfo := &kvrpcpb.LockInfo{Key: res.Lock.Key}
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
>>>>>>> af5ebef... gc_worker: Skip TiFlash nodes when doing UnsafeDestroyRange and Green GC (#15505)
}

func loadSafePoint(kv tikv.SafePointKV) (uint64, error) {
	val, err := kv.Get(tikv.GcSavedSafePoint)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(val, 10, 64)
}
