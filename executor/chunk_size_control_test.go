// Copyright 2019 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/v4/domain"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/session"
	"github.com/pingcap/tidb/v4/store/mockstore"
	"github.com/pingcap/tidb/v4/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/v4/store/tikv"
	"github.com/pingcap/tidb/v4/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/v4/tablecodec"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/codec"
	"github.com/pingcap/tidb/v4/util/testkit"
)

var (
	_ = Suite(&testChunkSizeControlSuite{})
)

type testSlowClient struct {
	sync.RWMutex
	tikv.Client
	regionDelay map[uint64]time.Duration
}

func (c *testSlowClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	regionID := req.RegionId
	delay := c.GetDelay(regionID)
	if req.Type == tikvrpc.CmdCop && delay > 0 {
		time.Sleep(delay)
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *testSlowClient) SetDelay(regionID uint64, dur time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.regionDelay[regionID] = dur
}

func (c *testSlowClient) GetDelay(regionID uint64) time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.regionDelay[regionID]
}

// manipulateCluster splits this cluster's region by splitKeys and returns regionIDs after split
func manipulateCluster(cluster *mocktikv.Cluster, splitKeys [][]byte) []uint64 {
	if len(splitKeys) == 0 {
		return nil
	}
	region, _ := cluster.GetRegionByKey(splitKeys[0])
	for _, key := range splitKeys {
		if r, _ := cluster.GetRegionByKey(key); r.Id != region.Id {
			panic("all split keys should belong to the same region")
		}
	}
	allRegionIDs := []uint64{region.Id}
	for i, key := range splitKeys {
		newRegionID, newPeerID := cluster.AllocID(), cluster.AllocID()
		cluster.Split(allRegionIDs[i], newRegionID, key, []uint64{newPeerID}, newPeerID)
		allRegionIDs = append(allRegionIDs, newRegionID)
	}
	return allRegionIDs
}

func generateTableSplitKeyForInt(tid int64, splitNum []int) [][]byte {
	results := make([][]byte, 0, len(splitNum))
	for _, num := range splitNum {
		results = append(results, tablecodec.EncodeRowKey(tid, codec.EncodeInt(nil, int64(num))))
	}
	return results
}

func generateIndexSplitKeyForInt(tid, idx int64, splitNum []int) [][]byte {
	results := make([][]byte, 0, len(splitNum))
	for _, num := range splitNum {
		d := new(types.Datum)
		d.SetInt64(int64(num))
		b, err := codec.EncodeKey(nil, nil, *d)
		if err != nil {
			panic(err)
		}
		results = append(results, tablecodec.EncodeIndexSeekKey(tid, idx, b))
	}
	return results
}

type testChunkSizeControlKit struct {
	store   kv.Storage
	dom     *domain.Domain
	tk      *testkit.TestKit
	client  *testSlowClient
	cluster *mocktikv.Cluster
}

type testChunkSizeControlSuite struct {
	m map[string]*testChunkSizeControlKit
}

func (s *testChunkSizeControlSuite) SetUpSuite(c *C) {
	c.Skip("not stable because coprocessor may result in goroutine leak")
	tableSQLs := map[string]string{}
	tableSQLs["Limit&TableScan"] = "create table t (a int, primary key (a))"
	tableSQLs["Limit&IndexScan"] = "create table t (a int, index idx_a(a))"

	s.m = make(map[string]*testChunkSizeControlKit)
	for name, sql := range tableSQLs {
		// BootstrapSession is not thread-safe, so we have to prepare all resources in SetUp.
		kit := new(testChunkSizeControlKit)
		s.m[name] = kit
		kit.client = &testSlowClient{regionDelay: make(map[uint64]time.Duration)}
		kit.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(kit.cluster)

		var err error
		kit.store, err = mockstore.NewMockTikvStore(
			mockstore.WithCluster(kit.cluster),
			mockstore.WithHijackClient(func(c tikv.Client) tikv.Client {
				kit.client.Client = c
				return kit.client
			}),
		)
		c.Assert(err, IsNil)

		// init domain
		kit.dom, err = session.BootstrapSession(kit.store)
		c.Assert(err, IsNil)

		// create the test table
		kit.tk = testkit.NewTestKitWithInit(c, kit.store)
		kit.tk.MustExec(sql)
	}
}

func (s *testChunkSizeControlSuite) getKit(name string) (
	kv.Storage, *domain.Domain, *testkit.TestKit, *testSlowClient, *mocktikv.Cluster) {
	x := s.m[name]
	return x.store, x.dom, x.tk, x.client, x.cluster
}

func (s *testChunkSizeControlSuite) TestLimitAndTableScan(c *C) {
	_, dom, tk, client, cluster := s.getKit("Limit&TableScan")
	defer client.Close()
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tbl.Meta().ID

	// construct two regions split by 100
	splitKeys := generateTableSplitKeyForInt(tid, []int{100})
	regionIDs := manipulateCluster(cluster, splitKeys)

	noDelayThreshold := time.Millisecond * 100
	delayDuration := time.Second
	delayThreshold := delayDuration * 9 / 10
	tk.MustExec("insert into t values (1)") // insert one record into region1, and set a delay duration
	client.SetDelay(regionIDs[0], delayDuration)

	results := tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost := s.parseTimeCost(c, results.Rows()[0])
	c.Assert(cost, Not(Less), delayThreshold) // have to wait for region1

	tk.MustExec("insert into t values (101)") // insert one record into region2
	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost = s.parseTimeCost(c, results.Rows()[0])
	c.Assert(cost, Less, noDelayThreshold) // region2 return quickly

	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 2")
	cost = s.parseTimeCost(c, results.Rows()[0])
	c.Assert(cost, Not(Less), delayThreshold) // have to wait
}

func (s *testChunkSizeControlSuite) TestLimitAndIndexScan(c *C) {
	_, dom, tk, client, cluster := s.getKit("Limit&IndexScan")
	defer client.Close()
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tbl.Meta().ID
	idx := tbl.Meta().Indices[0].ID

	// construct two regions split by 100
	splitKeys := generateIndexSplitKeyForInt(tid, idx, []int{100})
	regionIDs := manipulateCluster(cluster, splitKeys)

	noDelayThreshold := time.Millisecond * 100
	delayDuration := time.Second
	delayThreshold := delayDuration * 9 / 10
	tk.MustExec("insert into t values (1)") // insert one record into region1, and set a delay duration
	client.SetDelay(regionIDs[0], delayDuration)

	results := tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost := s.parseTimeCost(c, results.Rows()[0])
	c.Assert(cost, Not(Less), delayThreshold) // have to wait for region1

	tk.MustExec("insert into t values (101)") // insert one record into region2
	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost = s.parseTimeCost(c, results.Rows()[0])
	c.Assert(cost, Less, noDelayThreshold) // region2 return quickly

	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 2")
	cost = s.parseTimeCost(c, results.Rows()[0])
	c.Assert(cost, Not(Less), delayThreshold) // have to wait
}

func (s *testChunkSizeControlSuite) parseTimeCost(c *C, line []interface{}) time.Duration {
	lineStr := fmt.Sprintf("%v", line)
	idx := strings.Index(lineStr, "time:")
	c.Assert(idx, Not(Equals), -1)
	lineStr = lineStr[idx+len("time:"):]
	idx = strings.Index(lineStr, ",")
	c.Assert(idx, Not(Equals), -1)
	timeStr := lineStr[:idx]
	d, err := time.ParseDuration(timeStr)
	c.Assert(err, IsNil)
	return d
}
