// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testFastAnalyze{})

func (s *testSuite1) TestAnalyzePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("analyze table t")

	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi := table.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	do, err := session.GetDomain(s.store)
	c.Assert(err, IsNil)
	handle := do.StatsHandle()
	for _, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
		c.Assert(len(statsTbl.Columns), Equals, 3)
		c.Assert(len(statsTbl.Indices), Equals, 1)
		for _, col := range statsTbl.Columns {
			c.Assert(col.Len(), Greater, 0)
		}
		for _, idx := range statsTbl.Indices {
			c.Assert(idx.Len(), Greater, 0)
		}
	}

	tk.MustExec("drop table t")
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("alter table t analyze partition p0")
	is = executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi = table.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)

	for i, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
		if i == 0 {
			c.Assert(statsTbl.Pseudo, IsFalse)
			c.Assert(len(statsTbl.Columns), Equals, 3)
			c.Assert(len(statsTbl.Indices), Equals, 1)
		} else {
			c.Assert(statsTbl.Pseudo, IsTrue)
		}
	}
}

func (s *testSuite1) TestAnalyzeReplicaReadFollower(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	ctx := tk.Se.(sessionctx.Context)
	ctx.GetSessionVars().SetReplicaRead(kv.ReplicaReadFollower)
	tk.MustExec("analyze table t")
}

func (s *testSuite1) TestAnalyzeRestrict(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	ctx := tk.Se.(sessionctx.Context)
	ctx.GetSessionVars().InRestrictedSQL = true
	tk.MustExec("analyze table t")
}

func (s *testSuite1) TestAnalyzeParameters(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}
	tk.MustExec("insert into t values (19), (19), (19)")

	tk.MustExec("set @@tidb_enable_fast_analyze = 1")
	tk.MustExec("analyze table t with 30 samples")
	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := s.dom.StatsHandle().GetTableStats(tableInfo)
	col := tbl.Columns[1]
	c.Assert(col.Len(), Equals, 20)
	c.Assert(len(col.CMSketch.TopN()), Equals, 1)
	width, depth := col.CMSketch.GetWidthAndDepth()
	c.Assert(depth, Equals, int32(5))
	c.Assert(width, Equals, int32(2048))

	tk.MustExec("analyze table t with 4 buckets, 0 topn, 4 cmsketch width, 4 cmsketch depth")
	tbl = s.dom.StatsHandle().GetTableStats(tableInfo)
	col = tbl.Columns[1]
	c.Assert(col.Len(), Equals, 4)
	c.Assert(len(col.CMSketch.TopN()), Equals, 0)
	width, depth = col.CMSketch.GetWidthAndDepth()
	c.Assert(depth, Equals, int32(4))
	c.Assert(width, Equals, int32(4))
}

func (s *testSuite1) TestAnalyzeTooLongColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json)")
	value := fmt.Sprintf(`{"x":"%s"}`, strings.Repeat("x", mysql.MaxFieldVarCharLength))
	tk.MustExec(fmt.Sprintf("insert into t values ('%s')", value))

	tk.MustExec("analyze table t")
	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := s.dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.Columns[1].Len(), Equals, 0)
	c.Assert(tbl.Columns[1].TotColSize, Equals, int64(65559))
}

func (s *testSuite1) TestAnalyzeFastSample(c *C) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
	)
	c.Assert(err, IsNil)
	var dom *domain.Domain
	session.DisableStats4Test()
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	executor.RandSeed = 123

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	for i := 0; i < 60; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID

	// construct 5 regions split by {12, 24, 36, 48}
	splitKeys := generateTableSplitKeyForInt(tid, []int{12, 24, 36, 48})
	manipulateCluster(cluster, splitKeys)

	var pkCol *model.ColumnInfo
	var colsInfo []*model.ColumnInfo
	var indicesInfo []*model.IndexInfo
	for _, col := range tblInfo.Columns {
		if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			pkCol = col
		} else {
			colsInfo = append(colsInfo, col)
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	opts := make(map[ast.AnalyzeOptionType]uint64)
	opts[ast.AnalyzeOptNumSamples] = 20
	mockExec := &executor.AnalyzeTestFastExec{
		Ctx:             tk.Se.(sessionctx.Context),
		PKInfo:          pkCol,
		ColsInfo:        colsInfo,
		IdxsInfo:        indicesInfo,
		Concurrency:     1,
		PhysicalTableID: tbl.(table.PhysicalTable).GetPhysicalID(),
		TblInfo:         tblInfo,
		Opts:            opts,
	}
	err = mockExec.TestFastSample()
	c.Assert(err, IsNil)
	vals := make([][]string, 0)
	c.Assert(len(mockExec.Collectors), Equals, 3)
	for i := 0; i < 2; i++ {
		vals = append(vals, make([]string, 0))
		c.Assert(len(mockExec.Collectors[i].Samples), Equals, 20)
		for j := 0; j < 20; j++ {
			s, err := mockExec.Collectors[i].Samples[j].Value.ToString()
			c.Assert(err, IsNil)
			vals[i] = append(vals[i], s)
		}
	}
	c.Assert(fmt.Sprintln(vals), Equals, "[[0 4 6 9 10 11 12 14 17 24 25 29 30 34 35 44 52 54 57 58] [0 4 6 9 10 11 12 14 17 24 25 29 30 34 35 44 52 54 57 58]]\n")
}

func (s *testSuite1) TestFastAnalyze(c *C) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
	)
	c.Assert(err, IsNil)
	var dom *domain.Domain
	session.DisableStats4Test()
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	executor.RandSeed = 123

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c char(10), index index_b(b))")
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustExec("set @@session.tidb_build_stats_concurrency=1")
	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tblInfo.Meta().ID

	// construct 6 regions split by {10, 20, 30, 40, 50}
	splitKeys := generateTableSplitKeyForInt(tid, []int{10, 20, 30, 40, 50})
	manipulateCluster(cluster, splitKeys)

	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "char")`, i*3, i*3))
	}
	tk.MustExec("analyze table t with 5 buckets, 6 samples")

	is := executor.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.String(), Equals, "Table:43 Count:20\n"+
		"column:1 ndv:20 totColSize:0\n"+
		"num: 6 lower_bound: 3 upper_bound: 15 repeats: 1\n"+
		"num: 7 lower_bound: 18 upper_bound: 33 repeats: 1\n"+
		"num: 7 lower_bound: 39 upper_bound: 57 repeats: 1\n"+
		"column:2 ndv:20 totColSize:0\n"+
		"num: 6 lower_bound: 3 upper_bound: 15 repeats: 1\n"+
		"num: 7 lower_bound: 18 upper_bound: 33 repeats: 1\n"+
		"num: 7 lower_bound: 39 upper_bound: 57 repeats: 1\n"+
		"column:3 ndv:1 totColSize:72\n"+
		"num: 20 lower_bound: char upper_bound: char repeats: 18\n"+
		"index:1 ndv:20\n"+
		"num: 6 lower_bound: 3 upper_bound: 15 repeats: 1\n"+
		"num: 7 lower_bound: 18 upper_bound: 33 repeats: 1\n"+
		"num: 7 lower_bound: 39 upper_bound: 57 repeats: 1")

	// Test CM Sketch built from fast analyze.
	tk.MustExec("create table t1(a int, b int, index idx(a, b))")
	tk.MustExec("insert into t1 values (1,1),(1,1),(1,2),(1,2)")
	tk.MustExec("analyze table t1")
	tk.MustQuery("explain select a from t1 where a = 1").Check(testkit.Rows(
		"IndexReader_6 4.00 root index:IndexScan_5",
		"└─IndexScan_5 4.00 cop[tikv] table:t1, index:a, b, range:[1,1], keep order:false"))
	tk.MustQuery("explain select a, b from t1 where a = 1 and b = 1").Check(testkit.Rows(
		"IndexReader_6 2.00 root index:IndexScan_5",
		"└─IndexScan_5 2.00 cop[tikv] table:t1, index:a, b, range:[1 1,1 1], keep order:false"))
	tk.MustQuery("explain select a, b from t1 where a = 1 and b = 2").Check(testkit.Rows(
		"IndexReader_6 2.00 root index:IndexScan_5",
		"└─IndexScan_5 2.00 cop[tikv] table:t1, index:a, b, range:[1 2,1 2], keep order:false"))

	tk.MustExec("create table t2 (a bigint unsigned, primary key(a))")
	tk.MustExec("insert into t2 values (0), (18446744073709551615)")
	tk.MustExec("analyze table t2")
	tk.MustQuery("show stats_buckets where table_name = 't2'").Check(testkit.Rows(
		"test t2  a 0 0 1 1 0 0",
		"test t2  a 0 1 2 1 18446744073709551615 18446744073709551615"))
}

func (s *testSuite1) TestAnalyzeIncremental(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableStreaming = false
	s.testAnalyzeIncremental(tk, c)
}

func (s *testSuite1) TestAnalyzeIncrementalStreaming(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableStreaming = true
	s.testAnalyzeIncremental(tk, c)
}

func (s *testSuite1) testAnalyzeIncremental(tk *testkit.TestKit, c *C) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(a), index idx(b))")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows())
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  idx 1 0 1 1 1 1"))
	tk.MustExec("insert into t values (2,2)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  a 0 1 2 1 2 2", "test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2"))
	tk.MustExec("analyze incremental table t index")
	// Result should not change.
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  a 0 1 2 1 2 2", "test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2"))

	// Test analyze incremental with feedback.
	tk.MustExec("insert into t values (3,3)")
	oriProbability := statistics.FeedbackProbability.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
	}()
	statistics.FeedbackProbability.Store(1)
	is := s.dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	tk.MustQuery("select * from t use index(idx) where b = 3")
	tk.MustQuery("select * from t where a > 1")
	h := s.dom.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	c.Assert(h.HandleUpdateStats(is), IsNil)
	c.Assert(h.Update(is), IsNil)
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  a 0 1 3 0 2 2147483647", "test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2"))
	tblStats := h.GetTableStats(tblInfo)
	val, err := codec.EncodeKey(tk.Se.GetSessionVars().StmtCtx, nil, types.NewIntDatum(3))
	c.Assert(err, IsNil)
	c.Assert(tblStats.Indices[tblInfo.Indices[0].ID].CMSketch.QueryBytes(val), Equals, uint64(1))
	c.Assert(statistics.IsAnalyzed(tblStats.Indices[tblInfo.Indices[0].ID].Flag), IsFalse)
	c.Assert(statistics.IsAnalyzed(tblStats.Columns[tblInfo.Columns[0].ID].Flag), IsFalse)

	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1", "test t  a 0 1 2 1 2 2", "test t  a 0 2 3 1 3 3",
		"test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2", "test t  idx 1 2 3 1 3 3"))
	tblStats = h.GetTableStats(tblInfo)
	c.Assert(tblStats.Indices[tblInfo.Indices[0].ID].CMSketch.QueryBytes(val), Equals, uint64(1))
}

type testFastAnalyze struct {
	store   kv.Storage
	dom     *domain.Domain
	cluster *mocktikv.Cluster
	cli     *regionProperityClient
}

func (s *testFastAnalyze) SetUpSuite(c *C) {
	cli := &regionProperityClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithHijackClient(hijackClient),
		mockstore.WithCluster(s.cluster),
	)
	c.Assert(err, IsNil)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testFastAnalyze) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

type regionProperityClient struct {
	tikv.Client
	mu struct {
		sync.Mutex
		regionID uint64
		count    int64
	}
}

func (c *regionProperityClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdDebugGetRegionProperties {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.count++
		// Mock failure once.
		if req.DebugGetRegionProperties().RegionId == c.mu.regionID {
			c.mu.regionID = 0
			return &tikvrpc.Response{}, nil
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *regionProperityClient) setFailRegion(regionID uint64) {
	c.mu.Lock()
	c.mu.regionID = regionID
	c.mu.Unlock()
}

func (s *testFastAnalyze) TestFastAnalyzeRetryRowCount(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	c.Assert(s.dom.StatsHandle().Update(s.dom.InfoSchema()), IsNil)
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustExec("set @@session.tidb_build_stats_concurrency=1")
	tblInfo, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tblInfo.Meta().ID
	// construct 6 regions split by {6, 12, 18, 24, 30}
	splitKeys := generateTableSplitKeyForInt(tid, []int{6, 12, 18, 24, 30})
	regionIDs := manipulateCluster(s.cluster, splitKeys)
	for i := 0; i < 30; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}
	s.cli.setFailRegion(regionIDs[4])
	tk.MustExec("analyze table t")
	// 4 regions will be sampled, and it will retry the last failed region.
	c.Assert(s.cli.mu.count, Equals, int64(5))
	row := tk.MustQuery(`show stats_meta where db_name = "test" and table_name = "t"`).Rows()[0]
	c.Assert(row[5], Equals, "30")
}

func (s *testSuite1) TestFailedAnalyzeRequest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/buildStatsFromResult", `return(true)`), IsNil)
	_, err := tk.Exec("analyze table t")
	c.Assert(err.Error(), Equals, "mock buildStatsFromResult error")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/buildStatsFromResult"), IsNil)
}

func (s *testSuite1) TestExtractTopN(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, 0)", i+10))
	}
	tk.MustExec("analyze table t")
	is := s.dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	tblStats := s.dom.StatsHandle().GetTableStats(tblInfo)
	colStats := tblStats.Columns[tblInfo.Columns[1].ID]
	c.Assert(len(colStats.CMSketch.TopN()), Equals, 1)
	item := colStats.CMSketch.TopN()[0]
	c.Assert(item.Count, Equals, uint64(11))
	idxStats := tblStats.Indices[tblInfo.Indices[0].ID]
	c.Assert(len(idxStats.CMSketch.TopN()), Equals, 1)
	item = idxStats.CMSketch.TopN()[0]
	c.Assert(item.Count, Equals, uint64(11))
}
