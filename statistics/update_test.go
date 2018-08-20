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
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	log "github.com/sirupsen/logrus"
)

var _ = Suite(&testStatsUpdateSuite{})

type testStatsUpdateSuite struct {
	store kv.Storage
	do    *domain.Domain
	hook  logHook
}

func (s *testStatsUpdateSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	log.AddHook(&s.hook)
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

	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 := h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tableInfo2 := tbl2.Meta()
	stats2 := h.GetTableStats(tableInfo2)
	c.Assert(stats2.Count, Equals, int64(rowCount2))

	testKit.MustExec("analyze table t1")
	// Test update in a txn.
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
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
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
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
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1*3))
	stats2 = h.GetTableStats(tableInfo2)
	c.Assert(stats2.Count, Equals, int64(rowCount2))

	testKit.MustExec("begin")
	testKit.MustExec("delete from t1")
	testKit.MustExec("commit")
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(0))

	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("40", "70"))

	rs = testKit.MustQuery("select tot_col_size from mysql.stats_histograms")
	rs.Check(testkit.Rows("0", "0", "10", "10"))

	// test dump delta only when `modify count / count` is greater than the ratio.
	originValue := statistics.DumpStatsDeltaRatio
	statistics.DumpStatsDeltaRatio = 0.5
	defer func() {
		statistics.DumpStatsDeltaRatio = originValue
	}()
	statistics.DumpStatsDeltaRatio = 0.5
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values (1,2)")
	}
	h.DumpStatsDeltaToKV(statistics.DumpDelta)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	// not dumped
	testKit.MustExec("insert into t1 values (1,2)")
	h.DumpStatsDeltaToKV(statistics.DumpDelta)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	h.FlushStats()
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1+1))
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
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)

	stats := h.GetTableStats(tableInfo)
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

	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 := h.GetTableStats(tableInfo1)
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

	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
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
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 := h.GetTableStats(tableInfo1)
	// have not commit
	c.Assert(stats1.Count, Equals, int64(0))
	testKit.MustExec("commit")

	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	_, err = testKit.Exec("insert into t1 values(0, 2)")
	c.Assert(err, NotNil)

	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	testKit.MustExec("insert into t1 values(-1, 2)")
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1+1))
}

func (s *testStatsUpdateSuite) TestAutoUpdate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a varchar(20))")

	statistics.AutoAnalyzeMinCnt = 0
	testKit.MustExec("set global tidb_auto_analyze_ratio = 0.6")
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.0")
	}()

	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()

	h.HandleDDLEvent(<-h.DDLEventCh())
	h.Update(is)
	stats := h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(0))

	_, err = testKit.Exec("insert into t values ('ss')")
	c.Assert(err, IsNil)
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	err = h.HandleAutoAnalyze(is)
	c.Assert(err, IsNil)
	h.Update(is)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(1))
	c.Assert(stats.ModifyCount, Equals, int64(0))
	for _, item := range stats.Columns {
		// TotColSize = 2(length of 'ss') + 1(size of len byte).
		c.Assert(item.TotColSize, Equals, int64(3))
		break
	}

	// Test that even if the table is recently modified, we can still analyze the table.
	h.Lease = time.Second
	defer func() { h.Lease = 0 }()
	_, err = testKit.Exec("insert into t values ('fff')")
	c.Assert(err, IsNil)
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	err = h.HandleAutoAnalyze(is)
	c.Assert(err, IsNil)
	h.Update(is)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(2))
	c.Assert(stats.ModifyCount, Equals, int64(1))

	_, err = testKit.Exec("insert into t values ('fff')")
	c.Assert(err, IsNil)
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	err = h.HandleAutoAnalyze(is)
	c.Assert(err, IsNil)
	h.Update(is)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(3))
	c.Assert(stats.ModifyCount, Equals, int64(0))

	_, err = testKit.Exec("insert into t values ('eee')")
	c.Assert(err, IsNil)
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	err = h.HandleAutoAnalyze(is)
	c.Assert(err, IsNil)
	h.Update(is)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(4))
	// Modify count is non-zero means that we do not analyze the table.
	c.Assert(stats.ModifyCount, Equals, int64(1))
	for _, item := range stats.Columns {
		// TotColSize = 6, because the table has not been analyzed, and insert statement will add 3(length of 'eee') to TotColSize.
		c.Assert(item.TotColSize, Equals, int64(14))
		break
	}

	testKit.MustExec("analyze table t")
	_, err = testKit.Exec("create index idx on t(a)")
	c.Assert(err, IsNil)
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo = tbl.Meta()
	h.HandleAutoAnalyze(is)
	h.Update(is)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(4))
	c.Assert(stats.ModifyCount, Equals, int64(0))
	hg, ok := stats.Indices[tableInfo.Indices[0].ID]
	c.Assert(ok, IsTrue)
	c.Assert(hg.NDV, Equals, int64(3))
	c.Assert(hg.Len(), Equals, 3)
}

func (s *testStatsUpdateSuite) TestUpdateErrorRate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	h := s.do.StatsHandle()
	is := s.do.InfoSchema()
	h.Lease = 0
	h.Update(is)

	oriProbability := statistics.FeedbackProbability
	defer func() {
		statistics.FeedbackProbability = oriProbability
	}()
	statistics.FeedbackProbability = 1

	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))")
	h.HandleDDLEvent(<-h.DDLEventCh())

	testKit.MustExec("insert into t values (1, 3)")

	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	testKit.MustExec("analyze table t")

	testKit.MustExec("insert into t values (2, 3)")
	testKit.MustExec("insert into t values (5, 3)")
	testKit.MustExec("insert into t values (8, 3)")
	testKit.MustExec("insert into t values (12, 3)")
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	is = s.do.InfoSchema()
	h.Update(is)

	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	tbl := h.GetTableStats(tblInfo)
	aID := tblInfo.Columns[0].ID
	bID := tblInfo.Indices[0].ID

	// The statistic table is outdated now.
	c.Assert(tbl.Columns[aID].NotAccurate(), IsTrue)

	testKit.MustQuery("select * from t where a between 1 and 10")
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	c.Assert(h.HandleUpdateStats(is), IsNil)
	h.UpdateErrorRate(is)
	h.Update(is)
	tbl = h.GetTableStats(tblInfo)

	// The error rate of this column is not larger than MaxErrorRate now.
	c.Assert(tbl.Columns[aID].NotAccurate(), IsFalse)

	c.Assert(tbl.Indices[bID].NotAccurate(), IsTrue)
	testKit.MustQuery("select * from t where b between 2 and 10")
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	c.Assert(h.HandleUpdateStats(is), IsNil)
	h.UpdateErrorRate(is)
	h.Update(is)
	tbl = h.GetTableStats(tblInfo)
	c.Assert(tbl.Indices[bID].NotAccurate(), IsFalse)
	c.Assert(tbl.Indices[bID].QueryTotal, Equals, int64(1))

	testKit.MustExec("analyze table t")
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	h.Update(is)
	tbl = h.GetTableStats(tblInfo)
	c.Assert(tbl.Indices[bID].QueryTotal, Equals, int64(0))
}

func appendBucket(h *statistics.Histogram, l, r int64) {
	lower, upper := types.NewIntDatum(l), types.NewIntDatum(r)
	h.AppendBucket(&lower, &upper, 0, 0)
}

func (s *testStatsUpdateSuite) TestSplitRange(c *C) {
	h := statistics.NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5, 0)
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
		ranges := make([]*ranger.Range, 0, len(t.points)/2)
		for i := 0; i < len(t.points); i += 2 {
			ranges = append(ranges, &ranger.Range{
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
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))")
	testKit.MustExec("insert into t values (1,2),(2,2),(4,5)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t values (3,4)")

	h := s.do.StatsHandle()
	oriProbability := statistics.FeedbackProbability
	oriNumber := statistics.MaxNumberOfRanges
	defer func() {
		statistics.FeedbackProbability = oriProbability
		statistics.MaxNumberOfRanges = oriNumber
	}()
	statistics.FeedbackProbability = 1
	tests := []struct {
		sql     string
		hist    string
		idxCols int
	}{
		{
			// test primary key feedback
			sql: "select * from t where t.a <= 5",
			hist: "column:1 ndv:3 totColSize:0\n" +
				"num: 1 lower_bound: -9223372036854775808 upper_bound: 1 repeats: 0\n" +
				"num: 1 lower_bound: 2 upper_bound: 2 repeats: 1\n" +
				"num: 2 lower_bound: 3 upper_bound: 5 repeats: 0",
			idxCols: 0,
		},
		{
			// test index feedback by double read
			sql: "select * from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:2\n" +
				"num: 2 lower_bound: -inf upper_bound: 2 repeats: 0\n" +
				"num: 2 lower_bound: 3 upper_bound: 6 repeats: 0",
			idxCols: 1,
		},
		{
			// test index feedback by single read
			sql: "select b from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:2\n" +
				"num: 2 lower_bound: -inf upper_bound: 2 repeats: 0\n" +
				"num: 2 lower_bound: 3 upper_bound: 6 repeats: 0",
			idxCols: 1,
		},
	}
	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	for i, t := range tests {
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
		c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
		c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
		c.Assert(err, IsNil)
		h.Update(is)
		tblInfo := table.Meta()
		tbl := h.GetTableStats(tblInfo)
		if t.idxCols == 0 {
			c.Assert(tbl.Columns[tblInfo.Columns[0].ID].ToString(0), Equals, tests[i].hist)
		} else {
			c.Assert(tbl.Indices[tblInfo.Indices[0].ID].ToString(1), Equals, tests[i].hist)
		}
	}

	// Feedback from limit executor may not be accurate.
	testKit.MustQuery("select * from t where t.a <= 5 limit 1")
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	feedback := h.GetQueryFeedback()
	c.Assert(len(feedback), Equals, 0)

	// Test only collect for max number of ranges.
	statistics.MaxNumberOfRanges = 0
	for _, t := range tests {
		testKit.MustQuery(t.sql)
		h.DumpStatsDeltaToKV(statistics.DumpAll)
		feedback := h.GetQueryFeedback()
		c.Assert(len(feedback), Equals, 0)
	}

	// Test collect feedback by probability.
	statistics.FeedbackProbability = 0
	statistics.MaxNumberOfRanges = oriNumber
	for _, t := range tests {
		testKit.MustQuery(t.sql)
		h.DumpStatsDeltaToKV(statistics.DumpAll)
		feedback := h.GetQueryFeedback()
		c.Assert(len(feedback), Equals, 0)
	}

	// Test that the outdated feedback won't cause panic.
	statistics.FeedbackProbability = 1
	for _, t := range tests {
		testKit.MustQuery(t.sql)
	}
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	testKit.MustExec("drop table t")
	c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
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
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 1 where table_id = %d", tableInfo.ID))

	testKit.MustExec("delete from t")
	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("1"))

	// Now another tidb has updated the delta info.
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 3 where table_id = %d", tableInfo.ID))

	c.Assert(h.DumpStatsDeltaToKV(statistics.DumpAll), IsNil)
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("0"))
}

func (s *testStatsUpdateSuite) TestUpdateStatsByLocalFeedback(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))")
	testKit.MustExec("insert into t values (1,2),(2,2),(4,5)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t values (3,5)")
	h := s.do.StatsHandle()

	oriProbability := statistics.FeedbackProbability
	oriNumber := statistics.MaxNumberOfRanges
	defer func() {
		statistics.FeedbackProbability = oriProbability
		statistics.MaxNumberOfRanges = oriNumber
	}()
	statistics.FeedbackProbability = 1

	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	tblInfo := table.Meta()
	tbl := h.GetTableStats(tblInfo)

	testKit.MustQuery("select * from t use index(idx) where b <= 5")
	testKit.MustQuery("select * from t where a > 1")
	testKit.MustQuery("select * from t use index(idx) where b = 5")

	h.UpdateStatsByLocalFeedback(s.do.InfoSchema())
	tbl = h.GetTableStats(tblInfo)

	c.Assert(tbl.Columns[tblInfo.Columns[0].ID].ToString(0), Equals, "column:1 ndv:3 totColSize:0\n"+
		"num: 1 lower_bound: 1 upper_bound: 1 repeats: 1\n"+
		"num: 1 lower_bound: 2 upper_bound: 2 repeats: 1\n"+
		"num: 2 lower_bound: 3 upper_bound: 9223372036854775807 repeats: 0")
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	low, err := codec.EncodeKey(sc, nil, types.NewIntDatum(5))
	c.Assert(err, IsNil)

	c.Assert(tbl.Indices[tblInfo.Indices[0].ID].CMSketch.QueryBytes(low), Equals, uint32(2))

	c.Assert(tbl.Indices[tblInfo.Indices[0].ID].ToString(1), Equals, "index:1 ndv:2\n"+
		"num: 2 lower_bound: -inf upper_bound: 2 repeats: 0\n"+
		"num: 2 lower_bound: 3 upper_bound: 6 repeats: 0")

	// Test that it won't cause panic after update.
	testKit.MustQuery("select * from t use index(idx) where b > 0")
}

type logHook struct {
	results string
}

func (hook *logHook) Levels() []log.Level {
	return []log.Level{log.DebugLevel}
}

func (hook *logHook) Fire(entry *log.Entry) error {
	message := entry.Message
	if idx := strings.Index(message, "[stats"); idx != -1 {
		hook.results = hook.results + message[idx:]
	}
	return nil
}

func (s *testStatsUpdateSuite) TestLogDetailedInfo(c *C) {
	defer cleanEnv(c, s.store, s.do)

	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := statistics.MinLogScanCount
	oriMinError := statistics.MinLogErrorRate
	oriLevel := log.GetLevel()
	oriBucketNum := executor.GetMaxBucketSizeForTest()
	defer func() {
		statistics.FeedbackProbability = oriProbability
		statistics.MinLogScanCount = oriMinLogCount
		statistics.MinLogErrorRate = oriMinError
		executor.SetMaxBucketSizeForTest(oriBucketNum)
		log.SetLevel(oriLevel)
	}()
	executor.SetMaxBucketSizeForTest(4)
	statistics.FeedbackProbability = 1
	statistics.MinLogScanCount = 0
	statistics.MinLogErrorRate = 0

	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b), index idx_ba(b,a))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	testKit.MustExec("analyze table t")
	tests := []struct {
		sql    string
		result string
	}{
		{
			sql: "select * from t where t.a <= 15",
			result: "[stats-feedback] test.t, column: a, range: [-inf,7), actual: 8, expected: 7, buckets: {num: 8 lower_bound: 0 upper_bound: 7 repeats: 1}" +
				"[stats-feedback] test.t, column: a, range: [8,15), actual: 8, expected: 7, buckets: {num: 8 lower_bound: 8 upper_bound: 15 repeats: 1}",
		},
		{
			sql: "select * from t use index(idx) where t.b <= 15",
			result: "[stats-feedback] test.t, index: idx, range: [-inf,7), actual: 8, expected: 7, histogram: {num: 8 lower_bound: 0 upper_bound: 7 repeats: 1}" +
				"[stats-feedback] test.t, index: idx, range: [8,15), actual: 8, expected: 7, histogram: {num: 8 lower_bound: 8 upper_bound: 15 repeats: 1}",
		},
		{
			sql:    "select b from t use index(idx_ba) where b = 1 and a <= 5",
			result: "[stats-feedback] test.t, index: idx_ba, actual: 1, equality: 1, expected equality: 1, range: [-inf,6], actual: -1, expected: 6, buckets: {num: 8 lower_bound: 0 upper_bound: 7 repeats: 1}",
		},
		{
			sql:    "select b from t use index(idx_ba) where b = 1",
			result: "[stats-feedback] test.t, index: idx_ba, value: 1, actual: 1, expected: 1",
		},
	}
	log.SetLevel(log.DebugLevel)
	for _, t := range tests {
		s.hook.results = ""
		testKit.MustQuery(t.sql)
		c.Assert(s.hook.results, Equals, t.result)
	}
}
