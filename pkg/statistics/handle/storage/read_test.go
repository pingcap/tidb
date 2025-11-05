// Copyright 2021 PingCAP, Inc.
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

package storage_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestLoadStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	colAID := tableInfo.Columns[0].ID
	colCID := tableInfo.Columns[2].ID
	idxBID := tableInfo.Indices[0].ID
	h := dom.StatsHandle()

	// Index/column stats are not be loaded after analyze.
	stat := h.GetTableStats(tableInfo)
	require.True(t, stat.Columns[colAID].IsAllEvicted())
	hg := stat.Columns[colAID].Histogram
	require.Equal(t, hg.Len(), 0)
	cms := stat.Columns[colAID].CMSketch
	require.Nil(t, cms)
	require.True(t, stat.Indices[idxBID].IsAllEvicted())
	hg = stat.Indices[idxBID].Histogram
	require.Equal(t, hg.Len(), 0)
	cms = stat.Indices[idxBID].CMSketch
	topN := stat.Indices[idxBID].TopN
	require.Equal(t, cms.TotalCount()+topN.TotalCount(), uint64(0))
	require.True(t, stat.Columns[colCID].IsAllEvicted())
	hg = stat.Columns[colCID].Histogram
	require.Equal(t, 0, hg.Len())
	cms = stat.Columns[colCID].CMSketch
	require.Nil(t, cms)

	// Column stats are loaded after they are needed.
	_, err = cardinality.ColumnEqualRowCount(testKit.Session(), stat, types.NewIntDatum(1), colAID)
	require.NoError(t, err)
	_, err = cardinality.ColumnEqualRowCount(testKit.Session(), stat, types.NewIntDatum(1), colCID)
	require.NoError(t, err)
	require.NoError(t, h.LoadNeededHistograms())
	stat = h.GetTableStats(tableInfo)
	require.True(t, stat.Columns[colAID].IsFullLoad())
	hg = stat.Columns[colAID].Histogram
	require.Greater(t, hg.Len(), 0)
	// We don't maintain cmsketch for pk.
	cms = stat.Columns[colAID].CMSketch
	require.Nil(t, cms)
	require.True(t, stat.Columns[colCID].IsFullLoad())
	hg = stat.Columns[colCID].Histogram
	require.Greater(t, hg.Len(), 0)
	cms = stat.Columns[colCID].CMSketch
	require.NotNil(t, cms)

	// Index stats are loaded after they are needed.
	idx := stat.Indices[idxBID]
	hg = idx.Histogram
	cms = idx.CMSketch
	topN = idx.TopN
	require.Equal(t, float64(cms.TotalCount()+topN.TotalCount())+hg.TotalRowCount(), float64(0))
	require.False(t, idx.IsEssentialStatsLoaded())
	// IsInvalid adds the index to HistogramNeededItems.
	idx.IsInvalid(testKit.Session(), false)
	require.NoError(t, h.LoadNeededHistograms())
	stat = h.GetTableStats(tableInfo)
	idx = stat.Indices[tableInfo.Indices[0].ID]
	hg = idx.Histogram
	cms = idx.CMSketch
	topN = idx.TopN
	require.Greater(t, float64(cms.TotalCount()+topN.TotalCount())+hg.TotalRowCount(), float64(0))
	require.True(t, idx.IsFullLoad())
}

func TestReloadExtStatsLockRelease(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t") // no error
}

// TestLoadNonExistentIndexStats tests a bug fix for versions 7.5 and 6.5 caused by a missing condition check.
// Scenario: DDL event is lost (common when bulk creating tables on 7.5.x) → user adds index →
// sets tidb_opt_objective='determinate' → queries using that index.
func TestLoadNonExistentIndexStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	// Create table with an index. The index histogram will exist in the stats cache
	// but won't have actual histogram data loaded yet.
	tk.MustExec("create table if not exists t(a int, b int, index ia(a));")
	tk.MustExec("insert into t value(1,1), (2,2);")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	// Trigger async load of index histogram by using the index in a query.
	// When we set this variable to true, it causes the physical table to no longer use a fake physical table ID, which in turn triggers statistics loading.
	// See more at index's CheckStats and getStatsTable functions.
	tk.MustExec("set tidb_opt_objective='determinate';")
	tk.MustQuery("select * from t where a = 1 and b = 1;").Check(testkit.Rows("1 1"))
	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	addedIndexID := tableInfo.Indices[0].ID
	// Wait for the async load to add the index to HistogramNeededItems.
	// We should have 3 items: columns a, b, and index ia.
	require.Eventually(t, func() bool {
		items := statistics.HistogramNeededItems.AllItems()
		for _, item := range items {
			if item.IsIndex && item.TableID == tableInfo.ID && item.ID == addedIndexID {
				return len(items) == 3
			}
		}
		return false
	}, time.Second*5, time.Millisecond*100, "Index ia should be in HistogramNeededItems")

	// Verify that LoadNeededHistograms doesn't panic when the index exists in the cache
	// but doesn't have histogram data in mysql.stats_histograms yet.
	err = util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		require.NotPanics(t, func() {
			err := storage.LoadNeededHistograms(sctx, h, false)
			require.NoError(t, err)
		})
		return nil
	}, util.FlagWrapTxn)
	require.NoError(t, err)

	// Verify all items were removed from HistogramNeededItems after loading.
	items := statistics.HistogramNeededItems.AllItems()
	require.Equal(t, len(items), 0, "HistogramNeededItems should be empty after loading")
}
