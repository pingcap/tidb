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

package statistics_test

import (
	"fmt"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testStatsUpdateSuite{})

type testStatsUpdateSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (s *testStatsUpdateSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	s.store, s.do, err = newStoreWithBootstrap(0)
	c.Assert(err, IsNil)
}

func (s *testStatsUpdateSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testStatsUpdateSuite) TestSingleSessionInsert(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("create table t2 (c1 int, c2 int)")

	rowCount1 := 10
	rowCount2 := 20
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	for i := 0; i < rowCount2; i++ {
		testKit.MustExec("insert into t2 values(1, 2)")
	}

	is := s.do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()
	h := s.do.StatsHandle()

	h.HandleDDLEvent(<-h.DDLEventCh())
	h.HandleDDLEvent(<-h.DDLEventCh())

	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 := h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tableInfo2 := tbl2.Meta()
	stats2 := h.GetTableStats(tableInfo2.ID)
	c.Assert(stats2.Count, Equals, int64(rowCount2))

	testKit.MustExec("analyze table t1")
	// Test update in a txn.
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1*2))

	// Test IncreaseFactor.
	count, err := stats1.ColumnEqualRowCount(testKit.Se.GetSessionVars().StmtCtx, types.NewIntDatum(1), tableInfo1.Columns[0].ID)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(rowCount1*2))

	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	testKit.MustExec("commit")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1*3))

	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("delete from t1 limit 1")
	}
	for i := 0; i < rowCount2; i++ {
		testKit.MustExec("update t2 set c2 = c1")
	}
	testKit.MustExec("commit")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1*3))
	stats2 = h.GetTableStats(tableInfo2.ID)
	c.Assert(stats2.Count, Equals, int64(rowCount2))

	testKit.MustExec("begin")
	testKit.MustExec("delete from t1")
	testKit.MustExec("commit")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(0))

	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("40", "70"))
}

func (s *testStatsUpdateSuite) TestRollback(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("begin")
	testKit.MustExec("insert into t values (1,2)")
	testKit.MustExec("rollback")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := s.do.StatsHandle()
	h.HandleDDLEvent(<-h.DDLEventCh())
	h.DumpStatsDeltaToKV()
	h.Update(is)

	stats := h.GetTableStats(tableInfo.ID)
	c.Assert(stats.Count, Equals, int64(0))
	c.Assert(stats.ModifyCount, Equals, int64(0))
}

func (s *testStatsUpdateSuite) TestMultiSession(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")

	rowCount1 := 10
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}

	testKit1 := testkit.NewTestKit(c, s.store)
	for i := 0; i < rowCount1; i++ {
		testKit1.MustExec("insert into test.t1 values(1, 2)")
	}
	testKit2 := testkit.NewTestKit(c, s.store)
	for i := 0; i < rowCount1; i++ {
		testKit2.MustExec("delete from test.t1 limit 1")
	}
	is := s.do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()
	h := s.do.StatsHandle()

	h.HandleDDLEvent(<-h.DDLEventCh())

	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 := h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}

	for i := 0; i < rowCount1; i++ {
		testKit1.MustExec("insert into test.t1 values(1, 2)")
	}

	for i := 0; i < rowCount1; i++ {
		testKit2.MustExec("delete from test.t1 limit 1")
	}

	testKit.Se.Close()
	testKit2.Se.Close()

	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1*2))
	// The session in testKit is already Closed, set it to nil will create a new session.
	testKit.Se = nil
	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("60"))
}

func (s *testStatsUpdateSuite) TestTxnWithFailure(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int primary key, c2 int)")

	is := s.do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()
	h := s.do.StatsHandle()

	h.HandleDDLEvent(<-h.DDLEventCh())

	rowCount1 := 10
	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(?, 2)", i)
	}
	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 := h.GetTableStats(tableInfo1.ID)
	// have not commit
	c.Assert(stats1.Count, Equals, int64(0))
	testKit.MustExec("commit")

	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	_, err = testKit.Exec("insert into t1 values(0, 2)")
	c.Assert(err, NotNil)

	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	testKit.MustExec("insert into t1 values(-1, 2)")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1.ID)
	c.Assert(stats1.Count, Equals, int64(rowCount1+1))
}

func (s *testStatsUpdateSuite) TestAutoUpdate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int)")

	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()

	h.HandleDDLEvent(<-h.DDLEventCh())
	h.Update(is)
	stats := h.GetTableStats(tableInfo.ID)
	c.Assert(stats.Count, Equals, int64(0))

	_, err = testKit.Exec("insert into t values (1)")
	c.Assert(err, IsNil)
	h.DumpStatsDeltaToKV()
	h.Update(is)
	err = h.HandleAutoAnalyze(is)
	c.Assert(err, IsNil)
	h.Update(is)
	stats = h.GetTableStats(tableInfo.ID)
	c.Assert(stats.Count, Equals, int64(1))
	c.Assert(stats.ModifyCount, Equals, int64(0))

	_, err = testKit.Exec("insert into t values (1)")
	c.Assert(err, IsNil)
	h.DumpStatsDeltaToKV()
	h.Clear()
	// We set `Lease` here so that `Update` will use load by need strategy.
	h.Lease = time.Second
	h.Update(is)
	h.Lease = 0
	err = h.HandleAutoAnalyze(is)
	c.Assert(err, IsNil)
	h.Update(is)
	stats = h.GetTableStats(tableInfo.ID)
	c.Assert(stats.Count, Equals, int64(2))
	// Modify count is non-zero means that we do not analyze the table.
	c.Assert(stats.ModifyCount, Equals, int64(1))

	_, err = testKit.Exec("create index idx on t(a)")
	c.Assert(err, IsNil)
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	h.HandleAutoAnalyze(is)
	h.Update(is)
	stats = h.GetTableStats(tableInfo.ID)
	c.Assert(stats.Count, Equals, int64(2))
	c.Assert(stats.ModifyCount, Equals, int64(0))
	hg, ok := stats.Indices[tableInfo.Indices[0].ID]
	c.Assert(ok, IsTrue)
	c.Assert(hg.NDV, Equals, int64(1))
	c.Assert(hg.Len(), Equals, 1)
}

func appendBucket(h *statistics.Histogram, l, r int64) {
	lower, upper := types.NewIntDatum(l), types.NewIntDatum(r)
	h.AppendBucket(&lower, &upper, 0, 0)
}

func (s *testStatsUpdateSuite) TestSplitRange(c *C) {
	h := statistics.NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5)
	appendBucket(h, 1, 1)
	appendBucket(h, 2, 5)
	appendBucket(h, 7, 7)
	appendBucket(h, 8, 8)
	appendBucket(h, 10, 13)

	tests := []struct {
		points  []int64
		exclude []bool
		result  string
	}{
		{
			points:  []int64{1, 1},
			exclude: []bool{false, false},
			result:  "[1,1]",
		},
		{
			points:  []int64{0, 1, 3, 8, 8, 20},
			exclude: []bool{true, false, true, false, true, false},
			result:  "(0,1],(3,5],(5,7],(7,8],(8,20]",
		},
		{
			points:  []int64{8, 10, 20, 30},
			exclude: []bool{false, false, true, true},
			result:  "[8,8],(8,10],(20,30)",
		},
		{
			// test remove invalid range
			points:  []int64{8, 9},
			exclude: []bool{false, true},
			result:  "[8,8]",
		},
	}
	for _, t := range tests {
		ranges := make([]*ranger.NewRange, 0, len(t.points)/2)
		for i := 0; i < len(t.points); i += 2 {
			ranges = append(ranges, &ranger.NewRange{
				LowVal:      []types.Datum{types.NewIntDatum(t.points[i])},
				LowExclude:  t.exclude[i],
				HighVal:     []types.Datum{types.NewIntDatum(t.points[i+1])},
				HighExclude: t.exclude[i+1],
			})
		}
		ranges = h.SplitRange(ranges)
		var ranStrs []string
		for _, ran := range ranges {
			ranStrs = append(ranStrs, ran.String())
		}
		c.Assert(strings.Join(ranStrs, ","), Equals, t.result)
	}
}

func (s *testStatsUpdateSuite) TestQueryFeedback(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int, primary key(a), index idx(b))")
	testKit.MustExec("insert into t values (1,2),(2,2),(4,5)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t values (3,4)")

	h := s.do.StatsHandle()
	tests := []struct {
		sql     string
		hist    string
		colSize int
	}{
		{
			sql: "select * from t where t.a <= 5",
			hist: "column:1 ndv:3\n" +
				"num: 1\tlower_bound: 1\tupper_bound: 1\trepeats: 1\n" +
				"num: 2\tlower_bound: 2\tupper_bound: 2\trepeats: 1\n" +
				"num: 4\tlower_bound: 3\tupper_bound: 6\trepeats: 0",
			colSize: 0,
		},
		{
			sql: "select * from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:2\n" +
				"num: 2\tlower_bound: 2\tupper_bound: 2\trepeats: 2\n" +
				"num: 4\tlower_bound: 3\tupper_bound: 6\trepeats: 0",
			colSize: 1,
		},
		{
			sql: "select b from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:2\n" +
				"num: 2\tlower_bound: 2\tupper_bound: 2\trepeats: 2\n" +
				"num: 4\tlower_bound: 3\tupper_bound: 6\trepeats: 0",
			colSize: 1,
		},
	}
	for _, t := range tests {
		testKit.MustQuery(t.sql)
		h.DumpStatsDeltaToKV()
		feedback := h.GetQueryFeedback()
		c.Assert(len(feedback), Equals, 1)
		if t.colSize == 0 {
			c.Assert(feedback[0].DecodeInt(), IsNil)
		}
		c.Assert(statistics.UpdateHistogram(feedback[0].Hist(), feedback).ToString(t.colSize), Equals, t.hist)
	}

	// Feedback from limit executor may not be accurate.
	testKit.MustQuery("select * from t where t.a <= 2 limit 1")
	h.DumpStatsDeltaToKV()
	feedback := h.GetQueryFeedback()
	c.Assert(len(feedback), Equals, 0)
}

func (s *testStatsUpdateSuite) TestUpdateSystemTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("insert into t values (1,2)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("analyze table mysql.stats_histograms")
	h := s.do.StatsHandle()
	c.Assert(h.Update(s.do.InfoSchema()), IsNil)
	feedback := h.GetQueryFeedback()
	// We may have query feedback for system tables, but we do not need to store them.
	c.Assert(len(feedback), Equals, 0)
}

func (s *testStatsUpdateSuite) TestOutOfOrderUpdate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("insert into t values (1,2)")

	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	h.HandleDDLEvent(<-h.DDLEventCh())

	// Simulate the case that another tidb has inserted some value, but delta info has not been dumped to kv yet.
	testKit.MustExec("insert into t values (2,2),(4,5)")
	c.Assert(h.DumpStatsDeltaToKV(), IsNil)
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 1 where table_id = %d", tableInfo.ID))

	testKit.MustExec("delete from t")
	c.Assert(h.DumpStatsDeltaToKV(), IsNil)
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("1"))

	// Now another tidb has updated the delta info.
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 3 where table_id = %d", tableInfo.ID))

	c.Assert(h.DumpStatsDeltaToKV(), IsNil)
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("0"))
}
