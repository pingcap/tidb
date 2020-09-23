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

package handle_test

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ = Suite(&testStatsSuite{})

type testStatsSuite struct {
	store kv.Storage
	do    *domain.Domain
	hook  *logHook
}

func (s *testStatsSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	s.registerHook()
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testStatsSuite) registerHook() {
	conf := &log.Config{Level: os.Getenv("log_level"), File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	s.hook = &logHook{r.Core, ""}
	lg := zap.New(s.hook)
	log.ReplaceGlobals(lg, r)
}

func (s *testStatsSuite) TestSingleSessionInsert(c *C) {
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

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
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
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
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
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
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
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1*3))
	stats2 = h.GetTableStats(tableInfo2)
	c.Assert(stats2.Count, Equals, int64(rowCount2))

	testKit.MustExec("begin")
	testKit.MustExec("delete from t1")
	testKit.MustExec("commit")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(0))

	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("40", "70"))

	rs = testKit.MustQuery("select tot_col_size from mysql.stats_histograms").Sort()
	rs.Check(testkit.Rows("0", "0", "20", "20"))

	// test dump delta only when `modify count / count` is greater than the ratio.
	originValue := handle.DumpStatsDeltaRatio
	handle.DumpStatsDeltaRatio = 0.5
	defer func() {
		handle.DumpStatsDeltaRatio = originValue
	}()
	handle.DumpStatsDeltaRatio = 0.5
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values (1,2)")
	}
	h.DumpStatsDeltaToKV(handle.DumpDelta)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	// not dumped
	testKit.MustExec("insert into t1 values (1,2)")
	h.DumpStatsDeltaToKV(handle.DumpDelta)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	h.FlushStats()
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1+1))
}

func (s *testStatsSuite) TestRollback(c *C) {
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
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)

	stats := h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(0))
	c.Assert(stats.ModifyCount, Equals, int64(0))
}

func (s *testStatsSuite) TestMultiSession(c *C) {
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

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
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

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1*2))
	// The session in testKit is already Closed, set it to nil will create a new session.
	testKit.Se = nil
	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("60"))
}

func (s *testStatsSuite) TestTxnWithFailure(c *C) {
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
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 := h.GetTableStats(tableInfo1)
	// have not commit
	c.Assert(stats1.Count, Equals, int64(0))
	testKit.MustExec("commit")

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	_, err = testKit.Exec("insert into t1 values(0, 2)")
	c.Assert(err, NotNil)

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	testKit.MustExec("insert into t1 values(-1, 2)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1+1))
}

func (s *testStatsSuite) TestUpdatePartition(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b char(5)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11))`
	testKit.MustExec(createTable)
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	pi := tableInfo.GetPartitionInfo()
	c.Assert(len(pi.Definitions), Equals, 2)
	bColID := tableInfo.Columns[1].ID

	testKit.MustExec(`insert into t values (1, "a"), (7, "a")`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		c.Assert(statsTbl.ModifyCount, Equals, int64(1))
		c.Assert(statsTbl.Count, Equals, int64(1))
		c.Assert(statsTbl.Columns[bColID].TotColSize, Equals, int64(2))
	}

	testKit.MustExec(`update t set a = a + 1, b = "aa"`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		c.Assert(statsTbl.ModifyCount, Equals, int64(2))
		c.Assert(statsTbl.Count, Equals, int64(1))
		c.Assert(statsTbl.Columns[bColID].TotColSize, Equals, int64(3))
	}

	testKit.MustExec("delete from t")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		c.Assert(statsTbl.ModifyCount, Equals, int64(3))
		c.Assert(statsTbl.Count, Equals, int64(0))
		c.Assert(statsTbl.Columns[bColID].TotColSize, Equals, int64(0))
	}
}

func (s *testStatsSuite) TestAutoUpdate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a varchar(20))")

	handle.AutoAnalyzeMinCnt = 0
	testKit.MustExec("set global tidb_auto_analyze_ratio = 0.2")
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.0")
	}()

	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()

	h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(h.Update(is), IsNil)
	stats := h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(0))

	_, err = testKit.Exec("insert into t values ('ss'), ('ss'), ('ss'), ('ss'), ('ss')")
	c.Assert(err, IsNil)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	h.HandleAutoAnalyze(is)
	c.Assert(h.Update(is), IsNil)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(5))
	c.Assert(stats.ModifyCount, Equals, int64(0))
	for _, item := range stats.Columns {
		// TotColSize = 5*(2(length of 'ss') + 1(size of len byte)).
		c.Assert(item.TotColSize, Equals, int64(15))
		break
	}

	// Test that even if the table is recently modified, we can still analyze the table.
	h.SetLease(time.Second)
	defer func() { h.SetLease(0) }()
	_, err = testKit.Exec("insert into t values ('fff')")
	c.Assert(err, IsNil)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	h.HandleAutoAnalyze(is)
	c.Assert(h.Update(is), IsNil)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(6))
	c.Assert(stats.ModifyCount, Equals, int64(1))

	_, err = testKit.Exec("insert into t values ('fff')")
	c.Assert(err, IsNil)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	h.HandleAutoAnalyze(is)
	c.Assert(h.Update(is), IsNil)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(7))
	c.Assert(stats.ModifyCount, Equals, int64(0))

	_, err = testKit.Exec("insert into t values ('eee')")
	c.Assert(err, IsNil)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	h.HandleAutoAnalyze(is)
	c.Assert(h.Update(is), IsNil)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(8))
	// Modify count is non-zero means that we do not analyze the table.
	c.Assert(stats.ModifyCount, Equals, int64(1))
	for _, item := range stats.Columns {
		// TotColSize = 27, because the table has not been analyzed, and insert statement will add 3(length of 'eee') to TotColSize.
		c.Assert(item.TotColSize, Equals, int64(27))
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
	c.Assert(h.Update(is), IsNil)
	stats = h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(8))
	c.Assert(stats.ModifyCount, Equals, int64(0))
	hg, ok := stats.Indices[tableInfo.Indices[0].ID]
	c.Assert(ok, IsTrue)
	c.Assert(hg.NDV, Equals, int64(3))
	c.Assert(hg.Len(), Equals, 3)
}

func (s *testStatsSuite) TestAutoUpdatePartition(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))")
	testKit.MustExec("analyze table t")

	handle.AutoAnalyzeMinCnt = 0
	testKit.MustExec("set global tidb_auto_analyze_ratio = 0.6")
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.0")
	}()

	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	pi := tableInfo.GetPartitionInfo()
	h := do.StatsHandle()

	c.Assert(h.Update(is), IsNil)
	stats := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	c.Assert(stats.Count, Equals, int64(0))

	testKit.MustExec("insert into t values (1)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	h.HandleAutoAnalyze(is)
	stats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	c.Assert(stats.Count, Equals, int64(1))
	c.Assert(stats.ModifyCount, Equals, int64(0))
}

func (s *testStatsSuite) TestTableAnalyzed(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int)")
	testKit.MustExec("insert into t values (1)")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := s.do.StatsHandle()

	c.Assert(h.Update(is), IsNil)
	statsTbl := h.GetTableStats(tableInfo)
	c.Assert(handle.TableAnalyzed(statsTbl), IsFalse)

	testKit.MustExec("analyze table t")
	c.Assert(h.Update(is), IsNil)
	statsTbl = h.GetTableStats(tableInfo)
	c.Assert(handle.TableAnalyzed(statsTbl), IsTrue)

	h.Clear()
	oriLease := h.Lease()
	// set it to non-zero so we will use load by need strategy
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	c.Assert(h.Update(is), IsNil)
	statsTbl = h.GetTableStats(tableInfo)
	c.Assert(handle.TableAnalyzed(statsTbl), IsTrue)
}

func (s *testStatsSuite) TestUpdateErrorRate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	h := s.do.StatsHandle()
	is := s.do.InfoSchema()
	h.SetLease(0)
	c.Assert(h.Update(is), IsNil)
	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))")
	h.HandleDDLEvent(<-h.DDLEventCh())

	testKit.MustExec("insert into t values (1, 3)")

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")

	testKit.MustExec("insert into t values (2, 3)")
	testKit.MustExec("insert into t values (5, 3)")
	testKit.MustExec("insert into t values (8, 3)")
	testKit.MustExec("insert into t values (12, 3)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is = s.do.InfoSchema()
	c.Assert(h.Update(is), IsNil)

	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	tbl := h.GetTableStats(tblInfo)
	aID := tblInfo.Columns[0].ID
	bID := tblInfo.Indices[0].ID

	// The statistic table is outdated now.
	c.Assert(tbl.Columns[aID].NotAccurate(), IsTrue)

	testKit.MustQuery("select * from t where a between 1 and 10")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	c.Assert(h.HandleUpdateStats(is), IsNil)
	h.UpdateErrorRate(is)
	c.Assert(h.Update(is), IsNil)
	tbl = h.GetTableStats(tblInfo)

	// The error rate of this column is not larger than MaxErrorRate now.
	c.Assert(tbl.Columns[aID].NotAccurate(), IsFalse)

	c.Assert(tbl.Indices[bID].NotAccurate(), IsTrue)
	testKit.MustQuery("select * from t where b between 2 and 10")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	c.Assert(h.HandleUpdateStats(is), IsNil)
	h.UpdateErrorRate(is)
	c.Assert(h.Update(is), IsNil)
	tbl = h.GetTableStats(tblInfo)
	c.Assert(tbl.Indices[bID].NotAccurate(), IsFalse)
	c.Assert(tbl.Indices[bID].QueryTotal, Equals, int64(1))

	testKit.MustExec("analyze table t")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	tbl = h.GetTableStats(tblInfo)
	c.Assert(tbl.Indices[bID].QueryTotal, Equals, int64(0))
}

func (s *testStatsSuite) TestUpdatePartitionErrorRate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	h := s.do.StatsHandle()
	is := s.do.InfoSchema()
	h.SetLease(0)
	c.Assert(h.Update(is), IsNil)
	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
	testKit.MustExec("create table t (a bigint(64), primary key(a)) partition by range (a) (partition p0 values less than (30))")
	h.HandleDDLEvent(<-h.DDLEventCh())

	testKit.MustExec("insert into t values (1)")

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")

	testKit.MustExec("insert into t values (2)")
	testKit.MustExec("insert into t values (5)")
	testKit.MustExec("insert into t values (8)")
	testKit.MustExec("insert into t values (12)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is = s.do.InfoSchema()
	c.Assert(h.Update(is), IsNil)

	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	pid := tblInfo.Partition.Definitions[0].ID
	tbl := h.GetPartitionStats(tblInfo, pid)
	aID := tblInfo.Columns[0].ID

	// The statistic table is outdated now.
	c.Assert(tbl.Columns[aID].NotAccurate(), IsTrue)

	testKit.MustQuery("select * from t where a between 1 and 10")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	c.Assert(h.HandleUpdateStats(is), IsNil)
	h.UpdateErrorRate(is)
	c.Assert(h.Update(is), IsNil)
	tbl = h.GetPartitionStats(tblInfo, pid)

	// The error rate of this column is not larger than MaxErrorRate now.
	c.Assert(tbl.Columns[aID].NotAccurate(), IsFalse)
}

func appendBucket(h *statistics.Histogram, l, r int64) {
	lower, upper := types.NewIntDatum(l), types.NewIntDatum(r)
	h.AppendBucket(&lower, &upper, 0, 0)
}

func (s *testStatsSuite) TestSplitRange(c *C) {
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
			result:  "(0,1],(3,7),[7,8),[8,8],(8,10),[10,20]",
		},
		{
			points:  []int64{8, 10, 20, 30},
			exclude: []bool{false, false, true, true},
			result:  "[8,10),[10,10],(20,30)",
		},
		{
			// test remove invalid range
			points:  []int64{8, 9},
			exclude: []bool{false, true},
			result:  "[8,9)",
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
		ranges, _ = h.SplitRange(nil, ranges, false)
		var ranStrs []string
		for _, ran := range ranges {
			ranStrs = append(ranStrs, ran.String())
		}
		c.Assert(strings.Join(ranStrs, ","), Equals, t.result)
	}
}

func (s *testStatsSuite) TestQueryFeedback(c *C) {
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
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	defer func() {
		statistics.FeedbackProbability = oriProbability
		statistics.MaxNumberOfRanges = oriNumber
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0
	tests := []struct {
		sql     string
		hist    string
		idxCols int
	}{
		{
			// test primary key feedback
			sql: "select * from t where t.a <= 5 order by a desc",
			hist: "column:1 ndv:4 totColSize:0\n" +
				"num: 1 lower_bound: -9223372036854775808 upper_bound: 2 repeats: 0\n" +
				"num: 2 lower_bound: 2 upper_bound: 4 repeats: 0\n" +
				"num: 1 lower_bound: 4 upper_bound: 4 repeats: 1",
			idxCols: 0,
		},
		{
			// test index feedback by double read
			sql: "select * from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:2\n" +
				"num: 3 lower_bound: -inf upper_bound: 5 repeats: 0\n" +
				"num: 1 lower_bound: 5 upper_bound: 5 repeats: 1",
			idxCols: 1,
		},
		{
			// test index feedback by single read
			sql: "select b from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:2\n" +
				"num: 3 lower_bound: -inf upper_bound: 5 repeats: 0\n" +
				"num: 1 lower_bound: 5 upper_bound: 5 repeats: 1",
			idxCols: 1,
		},
	}
	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	for i, t := range tests {
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
		c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
		c.Assert(err, IsNil)
		c.Assert(h.Update(is), IsNil)
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
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	feedback := h.GetQueryFeedback()
	c.Assert(feedback.Size, Equals, 0)

	// Test only collect for max number of Ranges.
	statistics.MaxNumberOfRanges = 0
	for _, t := range tests {
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		feedback := h.GetQueryFeedback()
		c.Assert(feedback.Size, Equals, 0)
	}

	// Test collect feedback by probability.
	statistics.FeedbackProbability.Store(0)
	statistics.MaxNumberOfRanges = oriNumber
	for _, t := range tests {
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		feedback := h.GetQueryFeedback()
		c.Assert(feedback.Size, Equals, 0)
	}

	// Test that after drop stats, the feedback won't cause panic.
	statistics.FeedbackProbability.Store(1)
	for _, t := range tests {
		testKit.MustQuery(t.sql)
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	testKit.MustExec("drop stats t")
	c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)

	// Test that the outdated feedback won't cause panic.
	testKit.MustExec("analyze table t")
	for _, t := range tests {
		testKit.MustQuery(t.sql)
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	testKit.MustExec("drop table t")
	c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
}

func (s *testStatsSuite) TestQueryFeedbackForPartition(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
	testKit.MustExec(`create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))
			    partition by range (a) (
			    partition p0 values less than (3),
			    partition p1 values less than (6))`)
	testKit.MustExec("insert into t values (1,2),(2,2),(3,4),(4,1),(5,6)")
	testKit.MustExec("analyze table t")

	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	h := s.do.StatsHandle()
	tests := []struct {
		sql     string
		hist    string
		idxCols int
	}{
		{
			// test primary key feedback
			sql: "select * from t where t.a <= 5",
			hist: "column:1 ndv:2 totColSize:0\n" +
				"num: 1 lower_bound: -9223372036854775808 upper_bound: 2 repeats: 0\n" +
				"num: 1 lower_bound: 2 upper_bound: 5 repeats: 0",
			idxCols: 0,
		},
		{
			// test index feedback by double read
			sql: "select * from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:1\n" +
				"num: 2 lower_bound: -inf upper_bound: 6 repeats: 0",
			idxCols: 1,
		},
		{
			// test index feedback by single read
			sql: "select b from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:1\n" +
				"num: 2 lower_bound: -inf upper_bound: 6 repeats: 0",
			idxCols: 1,
		},
	}
	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	pi := tblInfo.GetPartitionInfo()
	c.Assert(pi, NotNil)

	// This test will check the result of partition p0.
	var pid int64
	for _, def := range pi.Definitions {
		if def.Name.L == "p0" {
			pid = def.ID
			break
		}
	}

	for i, t := range tests {
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
		c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
		c.Assert(err, IsNil)
		c.Assert(h.Update(is), IsNil)
		tbl := h.GetPartitionStats(tblInfo, pid)
		if t.idxCols == 0 {
			c.Assert(tbl.Columns[tblInfo.Columns[0].ID].ToString(0), Equals, tests[i].hist)
		} else {
			c.Assert(tbl.Indices[tblInfo.Indices[0].ID].ToString(1), Equals, tests[i].hist)
		}
	}
	testKit.MustExec("drop table t")
}

func (s *testStatsSuite) TestUpdateSystemTable(c *C) {
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
	c.Assert(feedback.Size, Equals, 0)
}

func (s *testStatsSuite) TestOutOfOrderUpdate(c *C) {
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
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 1 where table_id = %d", tableInfo.ID))

	testKit.MustExec("delete from t")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("1"))

	// Now another tidb has updated the delta info.
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 3 where table_id = %d", tableInfo.ID))

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("0"))
}

func (s *testStatsSuite) TestUpdateStatsByLocalFeedback(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))")
	testKit.MustExec("insert into t values (1,2),(2,2),(4,5)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t values (3,5)")
	h := s.do.StatsHandle()
	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	oriNumber := statistics.MaxNumberOfRanges
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
		statistics.MaxNumberOfRanges = oriNumber
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	tblInfo := table.Meta()
	h.GetTableStats(tblInfo)

	testKit.MustQuery("select * from t use index(idx) where b <= 5")
	testKit.MustQuery("select * from t where a > 1")
	testKit.MustQuery("select * from t use index(idx) where b = 5")

	h.UpdateStatsByLocalFeedback(s.do.InfoSchema())
	tbl := h.GetTableStats(tblInfo)

	c.Assert(tbl.Columns[tblInfo.Columns[0].ID].ToString(0), Equals, "column:1 ndv:3 totColSize:0\n"+
		"num: 1 lower_bound: 1 upper_bound: 1 repeats: 1\n"+
		"num: 2 lower_bound: 2 upper_bound: 4 repeats: 0\n"+
		"num: 1 lower_bound: 4 upper_bound: 9223372036854775807 repeats: 0")
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	low, err := codec.EncodeKey(sc, nil, types.NewIntDatum(5))
	c.Assert(err, IsNil)

	c.Assert(tbl.Indices[tblInfo.Indices[0].ID].CMSketch.QueryBytes(low), Equals, uint64(2))

	c.Assert(tbl.Indices[tblInfo.Indices[0].ID].ToString(1), Equals, "index:1 ndv:2\n"+
		"num: 2 lower_bound: -inf upper_bound: 5 repeats: 0\n"+
		"num: 1 lower_bound: 5 upper_bound: 5 repeats: 1")

	// Test that it won't cause panic after update.
	testKit.MustQuery("select * from t use index(idx) where b > 0")

	// Test that after drop stats, it won't cause panic.
	testKit.MustExec("drop stats t")
	h.UpdateStatsByLocalFeedback(s.do.InfoSchema())
}

func (s *testStatsSuite) TestUpdatePartitionStatsByLocalFeedback(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a)) partition by range (a) (partition p0 values less than (6))")
	testKit.MustExec("insert into t values (1,2),(2,2),(4,5)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t values (3,5)")
	h := s.do.StatsHandle()
	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	testKit.MustQuery("select * from t where a > 1")

	h.UpdateStatsByLocalFeedback(s.do.InfoSchema())

	tblInfo := table.Meta()
	pid := tblInfo.Partition.Definitions[0].ID
	tbl := h.GetPartitionStats(tblInfo, pid)

	c.Assert(tbl.Columns[tblInfo.Columns[0].ID].ToString(0), Equals, "column:1 ndv:3 totColSize:0\n"+
		"num: 1 lower_bound: 1 upper_bound: 1 repeats: 1\n"+
		"num: 2 lower_bound: 2 upper_bound: 4 repeats: 0\n"+
		"num: 1 lower_bound: 4 upper_bound: 9223372036854775807 repeats: 0")
}

type logHook struct {
	zapcore.Core
	results string
}

func (h *logHook) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	message := entry.Message
	if idx := strings.Index(message, "[stats"); idx != -1 {
		h.results = h.results + message
		for _, f := range fields {
			h.results = h.results + ", " + f.Key + "=" + h.field2String(f)
		}
	}
	return nil
}

func (h *logHook) field2String(field zapcore.Field) string {
	switch field.Type {
	case zapcore.StringType:
		return field.String
	case zapcore.Int64Type, zapcore.Int32Type, zapcore.Uint32Type, zapcore.Uint64Type:
		return fmt.Sprintf("%v", field.Integer)
	case zapcore.Float64Type:
		return fmt.Sprintf("%v", math.Float64frombits(uint64(field.Integer)))
	case zapcore.StringerType:
		return field.Interface.(fmt.Stringer).String()
	}
	return "not support"
}

func (h *logHook) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(e.Level) {
		return ce.AddCore(e, h)
	}
	return ce
}

func (s *testStatsSuite) TestLogDetailedInfo(c *C) {
	defer cleanEnv(c, s.store, s.do)

	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriMinError := handle.MinLogErrorRate
	oriLevel := log.GetLevel()
	oriLease := s.do.StatsHandle().Lease()
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriMinError
		s.do.StatsHandle().SetLease(oriLease)
		log.SetLevel(oriLevel)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0
	s.do.StatsHandle().SetLease(1)

	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), c bigint(64), primary key(a), index idx(b), index idx_ba(b,a), index idx_bc(b,c))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d, %d)", i, i, i))
	}
	testKit.MustExec("analyze table t with 4 buckets")
	tests := []struct {
		sql    string
		result string
	}{
		{
			sql: "select * from t where t.a <= 15",
			result: "[stats-feedback] test.t, column=a, rangeStr=range: [-inf,8), actual: 8, expected: 8, buckets: {num: 8 lower_bound: 0 upper_bound: 7 repeats: 1, num: 8 lower_bound: 8 upper_bound: 15 repeats: 1}" +
				"[stats-feedback] test.t, column=a, rangeStr=range: [8,15), actual: 8, expected: 7, buckets: {num: 8 lower_bound: 8 upper_bound: 15 repeats: 1}",
		},
		{
			sql: "select * from t use index(idx) where t.b <= 15",
			result: "[stats-feedback] test.t, index=idx, rangeStr=range: [-inf,8), actual: 8, expected: 8, histogram: {num: 8 lower_bound: 0 upper_bound: 7 repeats: 1, num: 8 lower_bound: 8 upper_bound: 15 repeats: 1}" +
				"[stats-feedback] test.t, index=idx, rangeStr=range: [8,16), actual: 8, expected: 8, histogram: {num: 8 lower_bound: 8 upper_bound: 15 repeats: 1, num: 4 lower_bound: 16 upper_bound: 19 repeats: 1}",
		},
		{
			sql:    "select b from t use index(idx_ba) where b = 1 and a <= 5",
			result: "[stats-feedback] test.t, index=idx_ba, actual=1, equality=1, expected equality=1, range=range: [-inf,6], actual: -1, expected: 6, buckets: {num: 8 lower_bound: 0 upper_bound: 7 repeats: 1}",
		},
		{
			sql:    "select b from t use index(idx_bc) where b = 1 and c <= 5",
			result: "[stats-feedback] test.t, index=idx_bc, actual=1, equality=1, expected equality=1, range=[-inf,6], pseudo count=7",
		},
		{
			sql:    "select b from t use index(idx_ba) where b = 1",
			result: "[stats-feedback] test.t, index=idx_ba, rangeStr=value: 1, actual: 1, expected: 1",
		},
	}
	log.SetLevel(zapcore.DebugLevel)
	for _, t := range tests {
		s.hook.results = ""
		testKit.MustQuery(t.sql)
		c.Assert(s.hook.results, Equals, t.result)
	}
}

func (s *testStatsSuite) TestNeedAnalyzeTable(c *C) {
	columns := map[int64]*statistics.Column{}
	columns[1] = &statistics.Column{Count: 1}
	tests := []struct {
		tbl    *statistics.Table
		ratio  float64
		limit  time.Duration
		start  string
		end    string
		now    string
		result bool
		reason string
	}{
		// table was never analyzed and has reach the limit
		{
			tbl:    &statistics.Table{Version: oracle.EncodeTSO(oracle.GetPhysical(time.Now()))},
			limit:  0,
			ratio:  0,
			start:  "00:00 +0800",
			end:    "00:01 +0800",
			now:    "00:00 +0800",
			result: true,
			reason: "table unanalyzed",
		},
		// table was never analyzed but has not reach the limit
		{
			tbl:    &statistics.Table{Version: oracle.EncodeTSO(oracle.GetPhysical(time.Now()))},
			limit:  time.Hour,
			ratio:  0,
			start:  "00:00 +0800",
			end:    "00:01 +0800",
			now:    "00:00 +0800",
			result: false,
			reason: "",
		},
		// table was already analyzed but auto analyze is disabled
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0,
			start:  "00:00 +0800",
			end:    "00:01 +0800",
			now:    "00:00 +0800",
			result: false,
			reason: "",
		},
		// table was already analyzed and but modify count is small
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 0, Count: 1}},
			limit:  0,
			ratio:  0.3,
			start:  "00:00 +0800",
			end:    "00:01 +0800",
			now:    "00:00 +0800",
			result: false,
			reason: "",
		},
		// table was already analyzed and but not within time period
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0.3,
			start:  "00:00 +0800",
			end:    "00:01 +0800",
			now:    "00:02 +0800",
			result: false,
			reason: "",
		},
		// table was already analyzed and but not within time period
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0.3,
			start:  "22:00 +0800",
			end:    "06:00 +0800",
			now:    "10:00 +0800",
			result: false,
			reason: "",
		},
		// table was already analyzed and within time period
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0.3,
			start:  "00:00 +0800",
			end:    "00:01 +0800",
			now:    "00:00 +0800",
			result: true,
			reason: "too many modifications",
		},
		// table was already analyzed and within time period
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0.3,
			start:  "22:00 +0800",
			end:    "06:00 +0800",
			now:    "23:00 +0800",
			result: true,
			reason: "too many modifications",
		},
	}
	for _, test := range tests {
		start, err := time.ParseInLocation(variable.FullDayTimeFormat, test.start, time.UTC)
		c.Assert(err, IsNil)
		end, err := time.ParseInLocation(variable.FullDayTimeFormat, test.end, time.UTC)
		c.Assert(err, IsNil)
		now, err := time.ParseInLocation(variable.FullDayTimeFormat, test.now, time.UTC)
		c.Assert(err, IsNil)
		needAnalyze, reason := handle.NeedAnalyzeTable(test.tbl, test.limit, test.ratio, start, end, now)
		c.Assert(needAnalyze, Equals, test.result)
		c.Assert(strings.HasPrefix(reason, test.reason), IsTrue)
	}
}

func (s *testStatsSuite) TestIndexQueryFeedback(c *C) {
	c.Skip("support update the topn of index equal conditions")
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)

	oriProbability := statistics.FeedbackProbability
	defer func() {
		statistics.FeedbackProbability = oriProbability
	}()
	statistics.FeedbackProbability.Store(1)

	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), c bigint(64), d float, e double, f decimal(17,2), " +
		"g time, h date, index idx_b(b), index idx_ab(a,b), index idx_ac(a,c), index idx_ad(a, d), index idx_ae(a, e), index idx_af(a, f)," +
		" index idx_ag(a, g), index idx_ah(a, h))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf(`insert into t values (1, %d, %d, %d, %d, %d, %d, "%s")`, i, i, i, i, i, i, fmt.Sprintf("1000-01-%02d", i+1)))
	}
	h := s.do.StatsHandle()
	h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t with 3 buckets")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf(`insert into t values (1, %d, %d, %d, %d, %d, %d, "%s")`, i, i, i, i, i, i, fmt.Sprintf("1000-01-%02d", i+1)))
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is := s.do.InfoSchema()
	c.Assert(h.Update(is), IsNil)
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	tests := []struct {
		sql     string
		hist    string
		idxCols int
		rangeID int64
		idxID   int64
		eqCount uint32
	}{
		{
			sql: "select * from t use index(idx_ab) where a = 1 and b < 21",
			hist: "index:1 ndv:20\n" +
				"num: 16 lower_bound: -inf upper_bound: 7 repeats: 0\n" +
				"num: 16 lower_bound: 8 upper_bound: 15 repeats: 0\n" +
				"num: 9 lower_bound: 16 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Indices[0].ID,
			idxID:   tblInfo.Indices[1].ID,
			idxCols: 1,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_ac) where a = 1 and c < 21",
			hist: "column:3 ndv:20 totColSize:40\n" +
				"num: 13 lower_bound: -9223372036854775808 upper_bound: 6 repeats: 0\n" +
				"num: 13 lower_bound: 7 upper_bound: 13 repeats: 0\n" +
				"num: 12 lower_bound: 14 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Columns[2].ID,
			idxID:   tblInfo.Indices[2].ID,
			idxCols: 0,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_ad) where a = 1 and d < 21",
			hist: "column:4 ndv:20 totColSize:320\n" +
				"num: 13 lower_bound: -10000000000000 upper_bound: 6 repeats: 0\n" +
				"num: 12 lower_bound: 7 upper_bound: 13 repeats: 0\n" +
				"num: 10 lower_bound: 14 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Columns[3].ID,
			idxID:   tblInfo.Indices[3].ID,
			idxCols: 0,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_ae) where a = 1 and e < 21",
			hist: "column:5 ndv:20 totColSize:320\n" +
				"num: 13 lower_bound: -100000000000000000000000 upper_bound: 6 repeats: 0\n" +
				"num: 12 lower_bound: 7 upper_bound: 13 repeats: 0\n" +
				"num: 10 lower_bound: 14 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Columns[4].ID,
			idxID:   tblInfo.Indices[4].ID,
			idxCols: 0,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_af) where a = 1 and f < 21",
			hist: "column:6 ndv:20 totColSize:400\n" +
				"num: 13 lower_bound: -999999999999999.99 upper_bound: 6.00 repeats: 0\n" +
				"num: 12 lower_bound: 7.00 upper_bound: 13.00 repeats: 0\n" +
				"num: 10 lower_bound: 14.00 upper_bound: 21.00 repeats: 0",
			rangeID: tblInfo.Columns[5].ID,
			idxID:   tblInfo.Indices[5].ID,
			idxCols: 0,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_ag) where a = 1 and g < 21",
			hist: "column:7 ndv:20 totColSize:196\n" +
				"num: 13 lower_bound: -838:59:59 upper_bound: 00:00:06 repeats: 0\n" +
				"num: 12 lower_bound: 00:00:07 upper_bound: 00:00:13 repeats: 0\n" +
				"num: 10 lower_bound: 00:00:14 upper_bound: 00:00:21 repeats: 0",
			rangeID: tblInfo.Columns[6].ID,
			idxID:   tblInfo.Indices[6].ID,
			idxCols: 0,
			eqCount: 30,
		},
		{
			sql: `select * from t use index(idx_ah) where a = 1 and h < "1000-01-21"`,
			hist: "column:8 ndv:20 totColSize:360\n" +
				"num: 13 lower_bound: 1000-01-01 upper_bound: 1000-01-07 repeats: 0\n" +
				"num: 12 lower_bound: 1000-01-08 upper_bound: 1000-01-14 repeats: 0\n" +
				"num: 10 lower_bound: 1000-01-15 upper_bound: 1000-01-21 repeats: 0",
			rangeID: tblInfo.Columns[7].ID,
			idxID:   tblInfo.Indices[7].ID,
			idxCols: 0,
			eqCount: 32,
		},
	}
	for i, t := range tests {
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
		c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
		c.Assert(h.Update(is), IsNil)
		tbl := h.GetTableStats(tblInfo)
		if t.idxCols == 0 {
			c.Assert(tbl.Columns[t.rangeID].ToString(0), Equals, tests[i].hist)
		} else {
			c.Assert(tbl.Indices[t.rangeID].ToString(1), Equals, tests[i].hist)
		}
		val, err := codec.EncodeKey(testKit.Se.GetSessionVars().StmtCtx, nil, types.NewIntDatum(1))
		c.Assert(err, IsNil)
		c.Assert(tbl.Indices[t.idxID].CMSketch.QueryBytes(val), Equals, uint64(t.eqCount))
	}
}

func (s *testStatsSuite) TestIndexQueryFeedback4TopN(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)

	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), index idx(a))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(`insert into t values (1)`)
	}
	h := s.do.StatsHandle()
	h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("set @@tidb_enable_fast_analyze = 1")
	testKit.MustExec("analyze table t with 3 buckets")
	for i := 0; i < 20; i++ {
		testKit.MustExec(`insert into t values (1)`)
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is := s.do.InfoSchema()
	c.Assert(h.Update(is), IsNil)
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()

	testKit.MustQuery("select * from t use index(idx) where a = 1")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
	c.Assert(h.Update(is), IsNil)
	tbl := h.GetTableStats(tblInfo)
	val, err := codec.EncodeKey(testKit.Se.GetSessionVars().StmtCtx, nil, types.NewIntDatum(1))
	c.Assert(err, IsNil)
	c.Assert(tbl.Indices[1].CMSketch.QueryBytes(val), Equals, uint64(40))
}

func (s *testStatsSuite) TestAbnormalIndexFeedback(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)

	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), index idx_ab(a,b))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i/5, i))
	}
	testKit.MustExec("analyze table t with 3 buckets")
	testKit.MustExec("delete from t where a = 1")
	testKit.MustExec("delete from t where b > 10")
	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	h := s.do.StatsHandle()
	tests := []struct {
		sql     string
		hist    string
		rangeID int64
		idxID   int64
		eqCount uint32
	}{
		{
			// The real count of `a = 1` is 0.
			sql: "select * from t where a = 1 and b < 21",
			hist: "column:2 ndv:20 totColSize:20\n" +
				"num: 5 lower_bound: -9223372036854775808 upper_bound: 7 repeats: 0\n" +
				"num: 4 lower_bound: 7 upper_bound: 14 repeats: 0\n" +
				"num: 4 lower_bound: 14 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Columns[1].ID,
			idxID:   tblInfo.Indices[0].ID,
			eqCount: 3,
		},
		{
			// The real count of `b > 10` is 0.
			sql: "select * from t where a = 2 and b > 10",
			hist: "column:2 ndv:20 totColSize:20\n" +
				"num: 5 lower_bound: -9223372036854775808 upper_bound: 7 repeats: 0\n" +
				"num: 4 lower_bound: 7 upper_bound: 14 repeats: 0\n" +
				"num: 5 lower_bound: 14 upper_bound: 9223372036854775807 repeats: 0",
			rangeID: tblInfo.Columns[1].ID,
			idxID:   tblInfo.Indices[0].ID,
			eqCount: 3,
		},
	}
	for i, t := range tests {
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
		c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
		c.Assert(h.Update(is), IsNil)
		tbl := h.GetTableStats(tblInfo)
		c.Assert(tbl.Columns[t.rangeID].ToString(0), Equals, tests[i].hist)
		val, err := codec.EncodeKey(testKit.Se.GetSessionVars().StmtCtx, nil, types.NewIntDatum(1))
		c.Assert(err, IsNil)
		c.Assert(tbl.Indices[t.idxID].CMSketch.QueryBytes(val), Equals, uint64(t.eqCount))
	}
}

func (s *testStatsSuite) TestFeedbackRanges(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	h := s.do.StatsHandle()
	oriProbability := statistics.FeedbackProbability
	oriNumber := statistics.MaxNumberOfRanges
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	defer func() {
		statistics.FeedbackProbability = oriProbability
		statistics.MaxNumberOfRanges = oriNumber
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	testKit.MustExec("use test")
	testKit.MustExec("create table t (a tinyint, b tinyint, primary key(a), index idx(a, b))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t with 3 buckets")
	for i := 30; i < 40; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tests := []struct {
		sql   string
		hist  string
		colID int64
	}{
		{
			sql: "select * from t where a <= 50 or (a > 130 and a < 140)",
			hist: "column:1 ndv:30 totColSize:0\n" +
				"num: 8 lower_bound: -128 upper_bound: 8 repeats: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 50 repeats: 0",
			colID: 1,
		},
		{
			sql: "select * from t where a >= 10",
			hist: "column:1 ndv:30 totColSize:0\n" +
				"num: 8 lower_bound: -128 upper_bound: 8 repeats: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 127 repeats: 0",
			colID: 1,
		},
		{
			sql: "select * from t use index(idx) where a = 1 and (b <= 50 or (b > 130 and b < 140))",
			hist: "column:2 ndv:20 totColSize:30\n" +
				"num: 8 lower_bound: -128 upper_bound: 7 repeats: 0\n" +
				"num: 8 lower_bound: 7 upper_bound: 14 repeats: 0\n" +
				"num: 7 lower_bound: 14 upper_bound: 51 repeats: 0",
			colID: 2,
		},
	}
	is := s.do.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	for i, t := range tests {
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
		c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
		c.Assert(err, IsNil)
		c.Assert(h.Update(is), IsNil)
		tblInfo := table.Meta()
		tbl := h.GetTableStats(tblInfo)
		c.Assert(tbl.Columns[t.colID].ToString(0), Equals, tests[i].hist)
	}
}

func (s *testStatsSuite) TestUnsignedFeedbackRanges(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	h := s.do.StatsHandle()

	oriProbability := statistics.FeedbackProbability
	oriMinLogCount := handle.MinLogScanCount
	oriErrorRate := handle.MinLogErrorRate
	oriNumber := statistics.MaxNumberOfRanges
	defer func() {
		statistics.FeedbackProbability = oriProbability
		handle.MinLogScanCount = oriMinLogCount
		handle.MinLogErrorRate = oriErrorRate
		statistics.MaxNumberOfRanges = oriNumber
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount = 0
	handle.MinLogErrorRate = 0

	testKit.MustExec("use test")
	testKit.MustExec("create table t (a tinyint unsigned, primary key(a))")
	testKit.MustExec("create table t1 (a bigint unsigned, primary key(a))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		testKit.MustExec(fmt.Sprintf("insert into t1 values (%d)", i))
	}
	h.HandleDDLEvent(<-h.DDLEventCh())
	h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t, t1 with 3 buckets")
	for i := 30; i < 40; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		testKit.MustExec(fmt.Sprintf("insert into t1 values (%d)", i))
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tests := []struct {
		sql     string
		hist    string
		tblName string
	}{
		{
			sql: "select * from t where a <= 50",
			hist: "column:1 ndv:30 totColSize:10\n" +
				"num: 8 lower_bound: 0 upper_bound: 8 repeats: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 50 repeats: 0",
			tblName: "t",
		},
		{
			sql: "select count(*) from t",
			hist: "column:1 ndv:30 totColSize:10\n" +
				"num: 8 lower_bound: 0 upper_bound: 8 repeats: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 255 repeats: 0",
			tblName: "t",
		},
		{
			sql: "select * from t1 where a <= 50",
			hist: "column:1 ndv:30 totColSize:10\n" +
				"num: 8 lower_bound: 0 upper_bound: 8 repeats: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 50 repeats: 0",
			tblName: "t1",
		},
		{
			sql: "select count(*) from t1",
			hist: "column:1 ndv:30 totColSize:10\n" +
				"num: 8 lower_bound: 0 upper_bound: 8 repeats: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 18446744073709551615 repeats: 0",
			tblName: "t1",
		},
	}
	is := s.do.InfoSchema()
	c.Assert(h.Update(is), IsNil)
	for i, t := range tests {
		table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr(t.tblName))
		c.Assert(err, IsNil)
		testKit.MustQuery(t.sql)
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
		c.Assert(h.HandleUpdateStats(s.do.InfoSchema()), IsNil)
		c.Assert(err, IsNil)
		c.Assert(h.Update(is), IsNil)
		tblInfo := table.Meta()
		tbl := h.GetTableStats(tblInfo)
		c.Assert(tbl.Columns[1].ToString(0), Equals, tests[i].hist)
	}
}

func (s *testStatsSuite) TestLoadHistCorrelation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	h := s.do.StatsHandle()
	origLease := h.Lease()
	h.SetLease(time.Second)
	defer func() { h.SetLease(origLease) }()
	testKit.MustExec("use test")
	testKit.MustExec("create table t(c int)")
	testKit.MustExec("insert into t values(1),(2),(3),(4),(5)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")
	h.Clear()
	c.Assert(h.Update(s.do.InfoSchema()), IsNil)
	result := testKit.MustQuery("show stats_histograms where Table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 0)
	testKit.MustExec("explain select * from t where c = 1")
	c.Assert(h.LoadNeededHistograms(), IsNil)
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][9], Equals, "1")
}

func (s *testStatsSuite) TestDeleteUpdateFeedback(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)

	oriProbability := statistics.FeedbackProbability
	defer func() {
		statistics.FeedbackProbability = oriProbability
	}()
	statistics.FeedbackProbability.Store(1)

	h := s.do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), index idx_ab(a,b))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i/5, i))
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t with 3 buckets")

	testKit.MustExec("delete from t where a = 1")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.GetQueryFeedback().Size, Equals, 0)
	testKit.MustExec("update t set a = 6 where a = 2")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.GetQueryFeedback().Size, Equals, 0)
	testKit.MustExec("explain analyze delete from t where a = 3")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.GetQueryFeedback().Size, Equals, 0)
}

func (s *testStatsSuite) BenchmarkHandleAutoAnalyze(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	h := s.do.StatsHandle()
	is := s.do.InfoSchema()
	for i := 0; i < c.N; i++ {
		h.HandleAutoAnalyze(is)
	}
}
