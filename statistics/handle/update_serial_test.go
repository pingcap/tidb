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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handle_test

import (
	"fmt"
	"github.com/pingcap/tidb/util/testkit"
	"math/rand"
	"strconv"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = SerialSuites(&testSerialStatsSuite{})

type testSerialStatsSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (s *testSerialStatsSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testSerialStatsSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testSerialStatsSuite) TestAutoAnalyzeOnEmptyTable(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	t := time.Now().Add(-1 * time.Minute)
	h, m := t.Hour(), t.Minute()
	start, end := fmt.Sprintf("%02d:%02d +0000", h, m), fmt.Sprintf("%02d:%02d +0000", h, m)
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", start))
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", end))
	s.do.StatsHandle().HandleAutoAnalyze(s.do.InfoSchema())

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, index idx(a))")
	// to pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	// to pass the AutoAnalyzeMinCnt check in autoAnalyzeTable
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", int(handle.AutoAnalyzeMinCnt)))
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(s.do.StatsHandle().Update(s.do.InfoSchema()), IsNil)

	// test if it will be limited by the time range
	c.Assert(s.do.StatsHandle().HandleAutoAnalyze(s.do.InfoSchema()), IsFalse)

	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	c.Assert(s.do.StatsHandle().HandleAutoAnalyze(s.do.InfoSchema()), IsTrue)
}

func (s *testSerialStatsSuite) TestAutoAnalyzeOutOfSpecifiedTime(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	t := time.Now().Add(-1 * time.Minute)
	h, m := t.Hour(), t.Minute()
	start, end := fmt.Sprintf("%02d:%02d +0000", h, m), fmt.Sprintf("%02d:%02d +0000", h, m)
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", start))
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", end))
	s.do.StatsHandle().HandleAutoAnalyze(s.do.InfoSchema())

	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	// to pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	// to pass the AutoAnalyzeMinCnt check in autoAnalyzeTable
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", int(handle.AutoAnalyzeMinCnt)))
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(s.do.StatsHandle().Update(s.do.InfoSchema()), IsNil)

	c.Assert(s.do.StatsHandle().HandleAutoAnalyze(s.do.InfoSchema()), IsFalse)
	tk.MustExec("analyze table t")

	tk.MustExec("alter table t add index ia(a)")
	c.Assert(s.do.StatsHandle().HandleAutoAnalyze(s.do.InfoSchema()), IsFalse)

	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	c.Assert(s.do.StatsHandle().HandleAutoAnalyze(s.do.InfoSchema()), IsTrue)
}

func (s *testSerialStatsSuite) TestIssue25700(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` ( `ldecimal` decimal(32,4) DEFAULT NULL, `rdecimal` decimal(32,4) DEFAULT NULL, `gen_col` decimal(36,4) GENERATED ALWAYS AS (`ldecimal` + `rdecimal`) VIRTUAL, `col_timestamp` timestamp(3) NULL DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("analyze table t")
	tk.MustExec("INSERT INTO `t` (`ldecimal`, `rdecimal`, `col_timestamp`) VALUES (2265.2200, 9843.4100, '1999-12-31 16:00:00')" + strings.Repeat(", (2265.2200, 9843.4100, '1999-12-31 16:00:00')", int(handle.AutoAnalyzeMinCnt)))
	c.Assert(s.do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(s.do.StatsHandle().Update(s.do.InfoSchema()), IsNil)

	c.Assert(s.do.StatsHandle().HandleAutoAnalyze(s.do.InfoSchema()), IsTrue)
	c.Assert(tk.MustQuery("show analyze status").Rows()[1][7], Equals, "finished")
}

func (s *testSerialStatsSuite) TestAutoAnalyzeOnChangeAnalyzeVer(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("set @@global.tidb_analyze_version = 1")
	do := s.do
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
	}()
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is := do.InfoSchema()
	err = h.UpdateSessionVar()
	c.Assert(err, IsNil)
	c.Assert(h.Update(is), IsNil)
	// Auto analyze when global ver is 1.
	h.HandleAutoAnalyze(is)
	c.Assert(h.Update(is), IsNil)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl1 := h.GetTableStats(tbl.Meta())
	// Check that all the version of t's stats are 1.
	for _, col := range statsTbl1.Columns {
		c.Assert(col.StatsVer, Equals, int64(1))
	}
	for _, idx := range statsTbl1.Indices {
		c.Assert(idx.StatsVer, Equals, int64(1))
	}
	tk.MustExec("set @@global.tidb_analyze_version = 2")
	err = h.UpdateSessionVar()
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1), (2), (3), (4)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	// Auto analyze t whose version is 1 after setting global ver to 2.
	h.HandleAutoAnalyze(is)
	c.Assert(h.Update(is), IsNil)
	statsTbl1 = h.GetTableStats(tbl.Meta())
	c.Assert(statsTbl1.Count, Equals, int64(5))
	// All of its statistics should still be version 1.
	for _, col := range statsTbl1.Columns {
		c.Assert(col.StatsVer, Equals, int64(1))
	}
	for _, idx := range statsTbl1.Indices {
		c.Assert(idx.StatsVer, Equals, int64(1))
	}
	// Add a new table after the analyze version set to 2.
	tk.MustExec("create table tt(a int, index idx(a))")
	tk.MustExec("insert into tt values(1), (2), (3), (4), (5)")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is = do.InfoSchema()
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tt"))
	c.Assert(err, IsNil)
	c.Assert(h.Update(is), IsNil)
	h.HandleAutoAnalyze(is)
	c.Assert(h.Update(is), IsNil)
	statsTbl2 := h.GetTableStats(tbl2.Meta())
	// Since it's a newly created table. Auto analyze should analyze it's statistics to version2.
	for _, idx := range statsTbl2.Indices {
		c.Assert(idx.StatsVer, Equals, int64(2))
	}
	for _, col := range statsTbl2.Columns {
		c.Assert(col.StatsVer, Equals, int64(2))
	}
	tk.MustExec("set @@global.tidb_analyze_version = 1")
}

func (s *testSerialStatsSuite) TestMergeTopN(c *C) {
	// Move this test to here to avoid race test.
	tests := []struct {
		topnNum    int
		n          int
		maxTopNVal int
		maxTopNCnt int
	}{
		{
			topnNum:    10,
			n:          5,
			maxTopNVal: 50,
			maxTopNCnt: 100,
		},
		{
			topnNum:    1,
			n:          5,
			maxTopNVal: 50,
			maxTopNCnt: 100,
		},
		{
			topnNum:    5,
			n:          5,
			maxTopNVal: 5,
			maxTopNCnt: 100,
		},
		{
			topnNum:    5,
			n:          5,
			maxTopNVal: 10,
			maxTopNCnt: 100,
		},
	}
	for _, t := range tests {
		topnNum, n := t.topnNum, t.n
		maxTopNVal, maxTopNCnt := t.maxTopNVal, t.maxTopNCnt

		// the number of maxTopNVal should be bigger than n.
		ok := maxTopNVal >= n
		c.Assert(ok, Equals, true)

		topNs := make([]*statistics.TopN, 0, topnNum)
		res := make(map[int]uint64)
		rand.Seed(time.Now().Unix())
		for i := 0; i < topnNum; i++ {
			topN := statistics.NewTopN(n)
			occur := make(map[int]bool)
			for j := 0; j < n; j++ {
				// The range of numbers in the topn structure is in [0, maxTopNVal)
				// But there cannot be repeated occurrences of value in a topN structure.
				randNum := rand.Intn(maxTopNVal)
				for occur[randNum] {
					randNum = rand.Intn(maxTopNVal)
				}
				occur[randNum] = true
				tString := []byte(fmt.Sprintf("%d", randNum))
				// The range of the number of occurrences in the topn structure is in [0, maxTopNCnt)
				randCnt := uint64(rand.Intn(maxTopNCnt))
				res[randNum] += randCnt
				topNMeta := statistics.TopNMeta{Encoded: tString, Count: randCnt}
				topN.TopN = append(topN.TopN, topNMeta)
			}
			topNs = append(topNs, topN)
		}
		topN, remainTopN := statistics.MergeTopN(topNs, uint32(n))
		cnt := len(topN.TopN)
		var minTopNCnt uint64
		for _, topNMeta := range topN.TopN {
			val, err := strconv.Atoi(string(topNMeta.Encoded))
			c.Assert(err, IsNil)
			c.Assert(topNMeta.Count, Equals, res[val])
			minTopNCnt = topNMeta.Count
		}
		if remainTopN != nil {
			cnt += len(remainTopN)
			for _, remainTopNMeta := range remainTopN {
				val, err := strconv.Atoi(string(remainTopNMeta.Encoded))
				c.Assert(err, IsNil)
				c.Assert(remainTopNMeta.Count, Equals, res[val])
				// The count of value in remainTopN may equal to the min count of value in TopN.
				ok = minTopNCnt >= remainTopNMeta.Count
				c.Assert(ok, Equals, true)
			}
		}
		c.Assert(cnt, Equals, len(res))
	}
}

func (s *testSerialStatsSuite) TestAutoUpdatePartitionInDynamicOnlyMode(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testkit.WithPruneMode(testKit, variable.DynamicOnly, func() {
		testKit.MustExec("use test")
		testKit.MustExec("set @@tidb_analyze_version = 2;")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec(`create table t (a int, b varchar(10), index idx_ab(a, b))
					partition by range (a) (
					partition p0 values less than (10),
					partition p1 values less than (20),
					partition p2 values less than (30))`)

		do := s.do
		is := do.InfoSchema()
		h := do.StatsHandle()
		c.Assert(h.RefreshVars(), IsNil)
		c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)

		testKit.MustExec("insert into t values (1, 'a'), (2, 'b'), (11, 'c'), (12, 'd'), (21, 'e'), (22, 'f')")
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		testKit.MustExec("set @@tidb_analyze_version = 2")
		testKit.MustExec("analyze table t")

		handle.AutoAnalyzeMinCnt = 0
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.1")
		defer func() {
			handle.AutoAnalyzeMinCnt = 1000
			testKit.MustExec("set global tidb_auto_analyze_ratio = 0.0")
		}()

		c.Assert(h.Update(is), IsNil)
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		c.Assert(err, IsNil)
		tableInfo := tbl.Meta()
		pi := tableInfo.GetPartitionInfo()
		globalStats := h.GetTableStats(tableInfo)
		partitionStats := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		c.Assert(globalStats.Count, Equals, int64(6))
		c.Assert(globalStats.ModifyCount, Equals, int64(0))
		c.Assert(partitionStats.Count, Equals, int64(2))
		c.Assert(partitionStats.ModifyCount, Equals, int64(0))

		testKit.MustExec("insert into t values (3, 'g')")
		c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
		c.Assert(h.Update(is), IsNil)
		globalStats = h.GetTableStats(tableInfo)
		partitionStats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		c.Assert(globalStats.Count, Equals, int64(7))
		c.Assert(globalStats.ModifyCount, Equals, int64(1))
		c.Assert(partitionStats.Count, Equals, int64(3))
		c.Assert(partitionStats.ModifyCount, Equals, int64(1))

		h.HandleAutoAnalyze(is)
		c.Assert(h.Update(is), IsNil)
		globalStats = h.GetTableStats(tableInfo)
		partitionStats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		c.Assert(globalStats.Count, Equals, int64(7))
		c.Assert(globalStats.ModifyCount, Equals, int64(0))
		c.Assert(partitionStats.Count, Equals, int64(3))
		c.Assert(partitionStats.ModifyCount, Equals, int64(0))
	})
}

func (s *testSerialStatsSuite) TestAutoAnalyzeRatio(c *C) {
	defer cleanEnv(c, s.store, s.do)
	tk := testkit.NewTestKit(c, s.store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	h := s.do.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19))
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	is := s.do.InfoSchema()
	c.Assert(h.Update(is), IsNil)
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	c.Assert(h.LoadNeededHistograms(), IsNil)
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 10))
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(h.HandleAutoAnalyze(is), IsTrue)

	tk.MustExec("delete from t limit 12")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(h.HandleAutoAnalyze(is), IsFalse)

	tk.MustExec("delete from t limit 4")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	c.Assert(h.HandleAutoAnalyze(s.do.InfoSchema()), IsTrue)
}
