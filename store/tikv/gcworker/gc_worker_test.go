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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockoracle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
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
	client   *testGCWorkerClient
	pdClient pd.Client
}

var _ = Suite(&testGCWorkerSuite{})

func (s *testGCWorkerSuite) SetUpTest(c *C) {
	tikv.NewGCHandlerFunc = NewGCWorker

	hijackClient := func(client tikv.Client) tikv.Client {
		s.client = &testGCWorkerClient{
			Client: client,
		}
		client = s.client
		return client
	}

	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiStores(s.cluster, 3)
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithHijackClient(hijackClient))

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

func (s *testGCWorkerSuite) mustPut(c *C, key, value string) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte(key), []byte(value))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testGCWorkerSuite) mustGet(c *C, key string, ts uint64) string {
	snap, err := s.store.GetSnapshot(kv.Version{Ver: ts})
	c.Assert(err, IsNil)
	value, err := snap.Get(context.TODO(), []byte(key))
	c.Assert(err, IsNil)
	return string(value)
}

func (s *testGCWorkerSuite) mustGetNone(c *C, key string, ts uint64) {
	snap, err := s.store.GetSnapshot(kv.Version{Ver: ts})
	c.Assert(err, IsNil)
	_, err = snap.Get(context.TODO(), []byte(key))
	c.Assert(err, Equals, kv.ErrNotExist)
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
	spkv := s.store.GetSafePointKV()
	err := spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), strconv.FormatUint(math.MaxUint64, 10))
	c.Assert(err, IsNil)
	now := time.Now()
	sp := s.gcWorker.calSafePointByMinStartTS(now)
	c.Assert(sp.Second(), Equals, now.Second())
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), "0")
	c.Assert(err, IsNil)
	sp = s.gcWorker.calSafePointByMinStartTS(now)
	zeroTime := time.Unix(0, oracle.ExtractPhysical(0)*1e6)
	c.Assert(sp, Equals, zeroTime)

	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"), "0")
	c.Assert(err, IsNil)
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "b"), "1")
	c.Assert(err, IsNil)
	sp = s.gcWorker.calSafePointByMinStartTS(now)
	c.Assert(sp, Equals, zeroTime)

	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "a"),
		strconv.FormatUint(variable.GoTimeToTS(now), 10))
	c.Assert(err, IsNil)
	err = spkv.Put(fmt.Sprintf("%s/%s", infosync.ServerMinStartTSPath, "b"),
		strconv.FormatUint(variable.GoTimeToTS(now.Add(-20*time.Second)), 10))
	c.Assert(err, IsNil)
	sp = s.gcWorker.calSafePointByMinStartTS(now.Add(-10 * time.Second))
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
	bo := tikv.NewBackoffer(ctx, tikv.GcOneRegionMaxBackoff)
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

const (
	failRPCErr  = 0
	failNilResp = 1
	failErrResp = 2
)

func (s *testGCWorkerSuite) testDeleteRangesFailureImpl(c *C, failType int) {
	// Put some delete range tasks.
	_, err := s.gcWorker.session.Execute(context.Background(), `INSERT INTO mysql.gc_delete_range VALUES
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
	se := createSession(s.gcWorker.store)
	preparedRanges, err := util.LoadDeleteRanges(se, 20)
	se.Close()
	c.Assert(err, IsNil)
	c.Assert(preparedRanges, DeepEquals, ranges)

	stores, err := s.gcWorker.getUpStores(context.Background())
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
}

type handler = func(addr string, req *tikvrpc.Request) (*tikvrpc.Response, error)

func (c *testGCWorkerClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdUnsafeDestroyRange && c.unsafeDestroyRangeHandler != nil {
		return c.unsafeDestroyRangeHandler(addr, req)
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

func (s *testGCWorkerSuite) TestRunGCJobAPI(c *C) {
	gcSafePointCacheInterval = 0

	p := s.createGCProbe(c, "k1")
	safePoint := s.mustAllocTs(c)
	err := RunGCJob(context.Background(), s.store, safePoint, "mock", 1)
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

func (s *testGCWorkerSuite) loadEtcdSafePoint(c *C) uint64 {
	val, err := s.gcWorker.store.GetSafePointKV().Get(tikv.GcSavedSafePoint)
	c.Assert(err, IsNil)
	res, err := strconv.ParseUint(val, 10, 64)
	c.Assert(err, IsNil)
	return res
}
