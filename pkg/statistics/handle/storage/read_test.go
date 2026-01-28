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
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
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
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	colAID := tableInfo.Columns[0].ID
	colCID := tableInfo.Columns[2].ID
	idxBID := tableInfo.Indices[0].ID
	h := dom.StatsHandle()

	// Index/column stats are not be loaded after analyze.
	stat := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.True(t, stat.GetCol(colAID).IsAllEvicted())
	c := stat.GetCol(colAID)
	require.True(t, c == nil || c.Histogram.Len() == 0)
	require.True(t, c == nil || c.CMSketch == nil)
	require.True(t, stat.GetIdx(idxBID).IsAllEvicted())
	idx := stat.GetIdx(idxBID)
	require.True(t, idx == nil || idx.Histogram.Len() == 0)
	require.True(t, idx == nil || (idx.CMSketch.TotalCount()+idx.TopN.TotalCount() == 0))
	require.True(t, stat.GetCol(colCID).IsAllEvicted())
	c = stat.GetCol(colCID)
	require.True(t, c == nil || c.Histogram.Len() == 0)
	require.True(t, c == nil || c.CMSketch == nil)

	// Column stats are loaded after they are needed.
	pctx := testKit.Session().GetPlanCtx()
	statistics.ColumnStatsIsInvalid(stat.GetCol(colAID), pctx, &stat.HistColl, colAID)
	statistics.ColumnStatsIsInvalid(stat.GetCol(colCID), pctx, &stat.HistColl, colCID)
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	stat = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.True(t, stat.GetCol(colAID).IsFullLoad())
	hg := stat.GetCol(colAID).Histogram
	require.Greater(t, hg.Len(), 0)
	// We don't maintain cmsketch for pk.
	cms := stat.GetCol(colAID).CMSketch
	require.Nil(t, cms)
	require.True(t, stat.GetCol(colCID).IsFullLoad())
	hg = stat.GetCol(colCID).Histogram
	require.Greater(t, hg.Len(), 0)
	cms = stat.GetCol(colCID).CMSketch
	require.NotNil(t, cms)

	// Index stats are loaded after they are needed.
	idx = stat.GetIdx(idxBID)
	require.True(t, idx == nil || (float64(idx.CMSketch.TotalCount())+float64(idx.TopN.TotalCount())+idx.Histogram.TotalRowCount() == 0))
	require.False(t, idx != nil && idx.IsEssentialStatsLoaded())
	// IsInvalid adds the index to AsyncLoadHistogramNeededItems.
	statistics.IndexStatsIsInvalid(testKit.Session().GetPlanCtx(), idx, &stat.HistColl, idxBID)
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	stat = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	idx = stat.GetIdx(tableInfo.Indices[0].ID)
	hg = idx.Histogram
	cms = idx.CMSketch
	topN := idx.TopN
	require.Greater(t, float64(cms.TotalCount()+topN.TotalCount())+hg.TotalRowCount(), float64(0))
	require.True(t, idx.IsFullLoad())
}

func TestLoadNonExistentIndexStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Create table with an index. The index histogram doesn't exist in the system tables
	// because we didn't handle the create table DDL event (simulating a lost DDL event).
	tk.MustExec("create table if not exists t(a int, b int, index ia(a));")
	tk.MustExec("insert into t value(1,1), (2,2);")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	ctx := context.Background()
	require.NoError(t, h.Update(ctx, dom.InfoSchema()))
	// Trigger async load of index histogram by using the index in a query.
	// Setting this variable to determinate marks the pseudo table stats as able to trigger loading (CanNotTriggerLoad=false), which enables statistics loading.
	// See more at IndexStatsIsInvalid and GetStatsTable functions.
	tk.MustExec("set tidb_opt_objective='determinate';")
	tk.MustQuery("select * from t where a = 1 and b = 1;").Check(testkit.Rows("1 1"))
	table, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	addedIndexID := tableInfo.Indices[0].ID
	// Wait for the async load to add the index to AsyncLoadHistogramNeededItems.
	// We should have 3 items: columns a, b, and index ia.
	require.Eventually(t, func() bool {
		items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
		for _, item := range items {
			if item.IsIndex && item.TableID == tableInfo.ID && item.ID == addedIndexID {
				// NOTE: Because the unit test environment doesn't enable sync load by default,
				// columns a and b should also be in the AsyncLoadHistogramNeededItems.
				// Use >= 3 to make the test more robust. But it should be exactly 3.
				return len(items) >= 3
			}
		}
		return false
	}, time.Second*5, time.Millisecond*100, "Index ia should be in AsyncLoadHistogramNeededItems")

	// Verify that LoadNeededHistograms doesn't panic when the pseudo index stats exists in the cache
	// but doesn't have histogram data in mysql.stats_histograms yet.
	err = util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		require.NotPanics(t, func() {
			err := storage.LoadNeededHistograms(sctx, dom.InfoSchema(), h)
			require.NoError(t, err)
		})
		return nil
	}, util.FlagWrapTxn)
	require.NoError(t, err)

	// Verify all items were removed from AsyncLoadHistogramNeededItems after loading.
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	require.Equal(t, len(items), 0, "AsyncLoadHistogramNeededItems should be empty after loading")
}
