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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
)

var (
	_ = Suite(&testChunkSizeControlSuite{})
)

type testChunkSizeControlClient struct {
	tikv.Client

	regionDelay map[uint64]time.Duration
}

func (c *testChunkSizeControlClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	regionID := req.RegionId
	if req.Type == tikvrpc.CmdCop && c.regionDelay[regionID] > 0 {
		time.Sleep(c.regionDelay[regionID])
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *testChunkSizeControlClient) SetDelay(regionID uint64, dur time.Duration) {
	c.regionDelay[regionID] = dur
}

func manipulateCluster(cluster *mocktikv.Cluster, region1ID uint64, splitKeys [][]byte) []uint64 {
	allRegionIDs := make([]uint64, 0, len(splitKeys)+1)
	allRegionIDs = append(allRegionIDs, region1ID)
	for i, key := range splitKeys {
		newRegionID := cluster.AllocID()
		newPeerID := cluster.AllocID()
		cluster.Split(allRegionIDs[i], newRegionID, key, []uint64{newPeerID}, newPeerID)
		allRegionIDs = append(allRegionIDs, newRegionID)
	}
	return allRegionIDs
}

func generateSplitKeyForInt(tid int64, splitNum []int) [][]byte {
	results := make([][]byte, 0, len(splitNum))
	for _, num := range splitNum {
		results = append(results, tablecodec.EncodeRowKey(tid, codec.EncodeInt(nil, int64(num))))
	}
	return results
}

type testChunkSizeControlSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testChunkSizeControlSuite) SetUpSuite(c *C) {
}

func (s *testChunkSizeControlSuite) initClusterAndStore(
	c *C, cluster *mocktikv.Cluster, client *testChunkSizeControlClient) {
	hijackClient := func(c tikv.Client) tikv.Client {
		client.Client = c
		return client
	}

	var err error
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
		mockstore.WithHijackClient(hijackClient),
	)
	c.Assert(err, IsNil)

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testChunkSizeControlSuite) TestChunkSizeControl(c *C) {
	// init store
	client := &testChunkSizeControlClient{regionDelay: make(map[uint64]time.Duration)}
	cluster := mocktikv.NewCluster()
	_, _, regionID := mocktikv.BootstrapWithSingleStore(cluster)
	s.initClusterAndStore(c, cluster, client)
	tk := testkit.NewTestKitWithInit(c, s.store)

	// create a test table
	tk.MustExec("create table t (a int, primary key (a))")
	tbl, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tbl.Meta().ID

	// construct two regions split by 100
	splitKeys := generateSplitKeyForInt(tid, []int{100})
	regionIDs := manipulateCluster(cluster, regionID, splitKeys)

	// insert one record into each regions
	tk.MustExec("insert into t values (1), (101)")

	noBlockThreshold := time.Millisecond * 100
	client.SetDelay(regionIDs[0], time.Second)
	results := tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost := s.parseTimeCost(c, results.Rows()[0])
	c.Assert(cost, Less, noBlockThreshold)

	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 2")
	cost = s.parseTimeCost(c, results.Rows()[0])
	c.Assert(cost, Not(Less), time.Second)
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
