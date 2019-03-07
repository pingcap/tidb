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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
)

var (
	_ = Suite(&testChunkSizeControlSuite{})
)

type testSlowClient struct {
	tikv.Client
	regionDelay map[uint64]time.Duration
}

func (c *testSlowClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	regionID := req.RegionId
	if req.Type == tikvrpc.CmdCop && c.regionDelay[regionID] > 0 {
		time.Sleep(c.regionDelay[regionID])
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *testSlowClient) SetDelay(regionID uint64, dur time.Duration) {
	c.regionDelay[regionID] = dur
}

// manipulateCluster splits this cluster's region by splitKeys and returns regionIDs after split
func manipulateCluster(cluster *mocktikv.Cluster, splitKeys [][]byte) []uint64 {
	regions := cluster.GetAllRegions()
	if len(regions) != 1 {
		panic("this cluster has already split")
	}

	allRegionIDs := []uint64{regions[0].Meta.Id}
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

type testChunkSizeControlSuite struct{}

func (s *testChunkSizeControlSuite) SetUpSuite(c *C) {}

func (s *testChunkSizeControlSuite) initTable(c *C, tableSQL string) (
	kv.Storage, *domain.Domain, *testkit.TestKit, *testSlowClient, *mocktikv.Cluster) {
	// init store
	client := &testSlowClient{regionDelay: make(map[uint64]time.Duration)}
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
		mockstore.WithHijackClient(func(c tikv.Client) tikv.Client {
			client.Client = c
			return client
		}),
	)
	c.Assert(err, IsNil)

	// init domain
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)

	// create the test table
	tk := testkit.NewTestKitWithInit(c, store)
	tk.MustExec(tableSQL)
	return store, dom, tk, client, cluster
}

func (s *testChunkSizeControlSuite) TestLimitAndTableScan(c *C) {
	store, dom, tk, client, cluster := s.initTable(c, "create table t (a int, primary key (a))")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tbl.Meta().ID

	// construct two regions split by 100
	splitKeys := generateTableSplitKeyForInt(tid, []int{100})
	regionIDs := manipulateCluster(cluster, splitKeys)

	noDelayThreshold := time.Millisecond * 100
	delayThreshold := time.Second
	tk.MustExec("insert into t values (1)") // insert one record into region1, and set a delay duration
	client.SetDelay(regionIDs[0], delayThreshold)

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

	dom.Close()
	store.Close()
}

func (s *testChunkSizeControlSuite) TestLimitAndIndexScan(c *C) {
	store, dom, tk, client, cluster := s.initTable(c, "create table t (a int, index idx_a(a))")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tbl.Meta().ID
	idx := tbl.Meta().Indices[0].ID

	// construct two regions split by 100
	splitKeys := generateIndexSplitKeyForInt(tid, idx, []int{100})
	regionIDs := manipulateCluster(cluster, splitKeys)

	noDelayThreshold := time.Millisecond * 100
	delayThreshold := time.Second
	tk.MustExec("insert into t values (1)") // insert one record into region1, and set a delay duration
	client.SetDelay(regionIDs[0], delayThreshold)

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

	dom.Close()
	store.Close()
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
