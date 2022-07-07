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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// nolint: unused, deadcode
type testSlowClient struct {
	sync.RWMutex
	tikv.Client
	regionDelay map[uint64]time.Duration
}

// nolint: unused, deadcode
func (c *testSlowClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	regionID := req.RegionId
	delay := c.GetDelay(regionID)
	if req.Type == tikvrpc.CmdCop && delay > 0 {
		time.Sleep(delay)
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

// nolint: unused, deadcode
func (c *testSlowClient) SetDelay(regionID uint64, dur time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.regionDelay[regionID] = dur
}

// nolint: unused, deadcode
func (c *testSlowClient) GetDelay(regionID uint64) time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.regionDelay[regionID]
}

// manipulateCluster splits this cluster's region by splitKeys and returns regionIDs after split
func manipulateCluster(cluster testutils.Cluster, splitKeys [][]byte) []uint64 {
	if len(splitKeys) == 0 {
		return nil
	}
	region, _, _ := cluster.GetRegionByKey(splitKeys[0])
	for _, key := range splitKeys {
		if r, _, _ := cluster.GetRegionByKey(key); r.Id != region.Id {
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

func TestLimitAndTableScan(t *testing.T) {
	t.Skip("not stable because coprocessor may result in goroutine leak")
	kit, clean := createChunkSizeControlKit(t, "create table t (a int, primary key (a))")
	defer clean()
	tbl, err := kit.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tid := tbl.Meta().ID

	// construct two regions split by 100
	splitKeys := generateTableSplitKeyForInt(tid, []int{100})
	regionIDs := manipulateCluster(kit.cluster, splitKeys)

	noDelayThreshold := time.Millisecond * 100
	delayDuration := time.Second
	delayThreshold := delayDuration * 9 / 10
	kit.tk.MustExec("insert into t values (1)") // insert one record into region1, and set a delay duration
	kit.client.SetDelay(regionIDs[0], delayDuration)

	results := kit.tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost := parseTimeCost(t, results.Rows()[0])
	require.GreaterOrEqual(t, cost, delayThreshold) // have to wait for region1

	kit.tk.MustExec("insert into t values (101)") // insert one record into region2
	results = kit.tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost = parseTimeCost(t, results.Rows()[0])
	require.Less(t, cost, noDelayThreshold) // region2 return quickly

	results = kit.tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 2")
	cost = parseTimeCost(t, results.Rows()[0])
	require.GreaterOrEqual(t, cost, delayThreshold) // have to wait
}

func TestLimitAndIndexScan(t *testing.T) {
	t.Skip("not stable because coprocessor may result in goroutine leak")
	kit, clean := createChunkSizeControlKit(t, "create table t (a int, index idx_a(a))")
	defer clean()
	tbl, err := kit.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tid := tbl.Meta().ID
	idx := tbl.Meta().Indices[0].ID

	// construct two regions split by 100
	splitKeys := generateIndexSplitKeyForInt(tid, idx, []int{100})
	regionIDs := manipulateCluster(kit.cluster, splitKeys)

	noDelayThreshold := time.Millisecond * 100
	delayDuration := time.Second
	delayThreshold := delayDuration * 9 / 10
	kit.tk.MustExec("insert into t values (1)") // insert one record into region1, and set a delay duration
	kit.client.SetDelay(regionIDs[0], delayDuration)

	results := kit.tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost := parseTimeCost(t, results.Rows()[0])
	require.GreaterOrEqual(t, cost, delayThreshold) // have to wait for region1

	kit.tk.MustExec("insert into t values (101)") // insert one record into region2
	results = kit.tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost = parseTimeCost(t, results.Rows()[0])
	require.Less(t, cost, noDelayThreshold) // region2 return quickly

	results = kit.tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 2")
	cost = parseTimeCost(t, results.Rows()[0])
	require.GreaterOrEqual(t, cost, delayThreshold) // have to wait
}

// nolint: unused, deadcode
func parseTimeCost(t *testing.T, line []interface{}) time.Duration {
	lineStr := fmt.Sprintf("%v", line)
	idx := strings.Index(lineStr, "time:")
	require.NotEqual(t, -1, idx)
	lineStr = lineStr[idx+len("time:"):]
	idx = strings.Index(lineStr, ",")
	require.NotEqual(t, -1, idx)
	timeStr := lineStr[:idx]
	d, err := time.ParseDuration(timeStr)
	require.NoError(t, err)
	return d
}

// nolint: unused, deadcode
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

// nolint: unused, deadcode
type chunkSizeControlKit struct {
	store   kv.Storage
	dom     *domain.Domain
	tk      *testkit.TestKit
	client  *testSlowClient
	cluster testutils.Cluster
}

// nolint: unused, deadcode
func createChunkSizeControlKit(t *testing.T, sql string) (*chunkSizeControlKit, func()) {
	// BootstrapSession is not thread-safe, so we have to prepare all resources in SetUp.
	kit := new(chunkSizeControlKit)
	kit.client = &testSlowClient{regionDelay: make(map[uint64]time.Duration)}

	var err error
	kit.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			kit.cluster = c
		}),
		mockstore.WithClientHijacker(func(c tikv.Client) tikv.Client {
			kit.client.Client = c
			return kit.client
		}),
	)
	require.NoError(t, err)

	// init domain
	kit.dom, err = session.BootstrapSession(kit.store)
	require.NoError(t, err)

	// create the test table
	kit.tk = testkit.NewTestKit(t, kit.store)
	kit.tk.MustExec("use test")
	kit.tk.MustExec(sql)
	return kit, func() {
		kit.dom.Close()
		require.NoError(t, kit.store.Close())
	}
}
