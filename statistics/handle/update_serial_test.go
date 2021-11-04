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
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestAutoAnalyzeOnEmptyTable(t *testing.T) {
	tk, dom, clean := createTestKitAndDom(t)
	defer clean()

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	now := time.Now().Add(-1 * time.Minute)
	h, m := now.Hour(), now.Minute()
	start, end := fmt.Sprintf("%02d:%02d +0000", h, m), fmt.Sprintf("%02d:%02d +0000", h, m)
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", start))
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", end))
	dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema())

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, index idx(a))")
	// to pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	// to pass the AutoAnalyzeMinCnt check in autoAnalyzeTable
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", int(handle.AutoAnalyzeMinCnt)))
	require.Nil(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, dom.StatsHandle().Update(dom.InfoSchema()))

	// test if it will be limited by the time range
	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))

	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	require.True(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
}

func TestAutoAnalyzeOutOfSpecifiedTime(t *testing.T) {
	tk, dom, clean := createTestKitAndDom(t)
	defer clean()

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	now := time.Now().Add(-1 * time.Minute)
	h, m := now.Hour(), now.Minute()
	start, end := fmt.Sprintf("%02d:%02d +0000", h, m), fmt.Sprintf("%02d:%02d +0000", h, m)
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", start))
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", end))
	dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema())

	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	// to pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	// to pass the AutoAnalyzeMinCnt check in autoAnalyzeTable
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", int(handle.AutoAnalyzeMinCnt)))
	require.Nil(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, dom.StatsHandle().Update(dom.InfoSchema()))

	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
	tk.MustExec("analyze table t")

	tk.MustExec("alter table t add index ia(a)")
	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))

	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	require.True(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
}

func TestIssue25700(t *testing.T) {
	tk, dom, clean := createTestKitAndDom(t)
	defer clean()
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
	require.Nil(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, dom.StatsHandle().Update(dom.InfoSchema()))

	require.True(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
	require.Equal(t, "finished", tk.MustQuery("show analyze status").Rows()[1][7])
}

func TestAutoAnalyzeOnChangeAnalyzeVer(t *testing.T) {
	tk, do, clean := createTestKitAndDom(t)
	defer clean()
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("set @@global.tidb_analyze_version = 1")
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
	}()
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is := do.InfoSchema()
	err = h.UpdateSessionVar()
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	// Auto analyze when global ver is 1.
	h.HandleAutoAnalyze(is)
	require.Nil(t, h.Update(is))
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	statsTbl1 := h.GetTableStats(tbl.Meta())
	// Check that all the version of t's stats are 1.
	for _, col := range statsTbl1.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTbl1.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	tk.MustExec("set @@global.tidb_analyze_version = 2")
	err = h.UpdateSessionVar()
	require.NoError(t, err)
	tk.MustExec("insert into t values(1), (2), (3), (4)")
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, h.Update(is))
	// Auto analyze t whose version is 1 after setting global ver to 2.
	h.HandleAutoAnalyze(is)
	require.Nil(t, h.Update(is))
	statsTbl1 = h.GetTableStats(tbl.Meta())
	require.Equal(t, int64(5), statsTbl1.Count)
	// All of its statistics should still be version 1.
	for _, col := range statsTbl1.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTbl1.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	// Add a new table after the analyze version set to 2.
	tk.MustExec("create table tt(a int, index idx(a))")
	tk.MustExec("insert into tt values(1), (2), (3), (4), (5)")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is = do.InfoSchema()
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tt"))
	require.NoError(t, err)
	require.Nil(t, h.Update(is))
	h.HandleAutoAnalyze(is)
	require.Nil(t, h.Update(is))
	statsTbl2 := h.GetTableStats(tbl2.Meta())
	// Since it's a newly created table. Auto analyze should analyze it's statistics to version2.
	for _, idx := range statsTbl2.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	for _, col := range statsTbl2.Columns {
		require.Equal(t, int64(2), col.StatsVer)
	}
	tk.MustExec("set @@global.tidb_analyze_version = 1")
}

func TestMergeTopN(t *testing.T) {
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
	for _, tt := range tests {
		topnNum, n := tt.topnNum, tt.n
		maxTopNVal, maxTopNCnt := tt.maxTopNVal, tt.maxTopNCnt

		// the number of maxTopNVal should be bigger than n.
		ok := maxTopNVal >= n
		require.Equal(t, true, ok)

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
			require.NoError(t, err)
			require.Equal(t, res[val], topNMeta.Count)
			minTopNCnt = topNMeta.Count
		}
		if remainTopN != nil {
			cnt += len(remainTopN)
			for _, remainTopNMeta := range remainTopN {
				val, err := strconv.Atoi(string(remainTopNMeta.Encoded))
				require.NoError(t, err)
				require.Equal(t, res[val], remainTopNMeta.Count)
				// The count of value in remainTopN may equal to the min count of value in TopN.
				ok = minTopNCnt >= remainTopNMeta.Count
				require.Equal(t, true, ok)
			}
		}
		require.Equal(t, len(res), cnt)
	}
}

func TestAutoUpdatePartitionInDynamicOnlyMode(t *testing.T) {
	testKit, do, clean := createTestKitAndDom(t)
	defer clean()
	testkit.WithPruneMode(testKit, variable.DynamicOnly, func() {
		testKit.MustExec("use test")
		testKit.MustExec("set @@tidb_analyze_version = 2;")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec(`create table t (a int, b varchar(10), index idx_ab(a, b))
					partition by range (a) (
					partition p0 values less than (10),
					partition p1 values less than (20),
					partition p2 values less than (30))`)

		is := do.InfoSchema()
		h := do.StatsHandle()
		require.Nil(t, h.RefreshVars())
		require.Nil(t, h.HandleDDLEvent(<-h.DDLEventCh()))

		testKit.MustExec("insert into t values (1, 'a'), (2, 'b'), (11, 'c'), (12, 'd'), (21, 'e'), (22, 'f')")
		require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		testKit.MustExec("set @@tidb_analyze_version = 2")
		testKit.MustExec("analyze table t")

		handle.AutoAnalyzeMinCnt = 0
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.1")
		defer func() {
			handle.AutoAnalyzeMinCnt = 1000
			testKit.MustExec("set global tidb_auto_analyze_ratio = 0.0")
		}()

		require.Nil(t, h.Update(is))
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		pi := tableInfo.GetPartitionInfo()
		globalStats := h.GetTableStats(tableInfo)
		partitionStats := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(6), globalStats.Count)
		require.Equal(t, int64(0), globalStats.ModifyCount)
		require.Equal(t, int64(2), partitionStats.Count)
		require.Equal(t, int64(0), partitionStats.ModifyCount)

		testKit.MustExec("insert into t values (3, 'g')")
		require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.Nil(t, h.Update(is))
		globalStats = h.GetTableStats(tableInfo)
		partitionStats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(7), globalStats.Count)
		require.Equal(t, int64(1), globalStats.ModifyCount)
		require.Equal(t, int64(3), partitionStats.Count)
		require.Equal(t, int64(1), partitionStats.ModifyCount)

		h.HandleAutoAnalyze(is)
		require.Nil(t, h.Update(is))
		globalStats = h.GetTableStats(tableInfo)
		partitionStats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(7), globalStats.Count)
		require.Equal(t, int64(0), globalStats.ModifyCount)
		require.Equal(t, int64(3), partitionStats.Count)
		require.Equal(t, int64(0), partitionStats.ModifyCount)
	})
}

func TestAutoAnalyzeRatio(t *testing.T) {
	tk, dom, clean := createTestKitAndDom(t)
	defer clean()

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	require.Nil(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19))
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is := dom.InfoSchema()
	require.Nil(t, h.Update(is))
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	require.Nil(t, h.LoadNeededHistograms())
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 10))
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, h.Update(is))
	require.True(t, h.HandleAutoAnalyze(is))

	tk.MustExec("delete from t limit 12")
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, h.Update(is))
	require.False(t, h.HandleAutoAnalyze(is))

	tk.MustExec("delete from t limit 4")
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, h.Update(is))
	require.True(t, h.HandleAutoAnalyze(dom.InfoSchema()))
}
