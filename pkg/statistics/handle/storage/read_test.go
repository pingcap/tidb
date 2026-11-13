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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
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
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
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
	_, err = cardinality.ColumnEqualRowCount(testKit.Session().GetPlanCtx(), stat, types.NewIntDatum(1), colAID)
	require.NoError(t, err)
	_, err = cardinality.ColumnEqualRowCount(testKit.Session().GetPlanCtx(), stat, types.NewIntDatum(1), colCID)
	require.NoError(t, err)
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

func TestReadColumnDistributionStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,1),(5,5,1)")
	testKit.MustExec("analyze table t all columns with 2 topn, 2 buckets")

	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()

	loaded, err := storage.ReadColumnDistributionStats(
		context.Background(), testKit.Session(), tableInfo.ID, tableInfo.Columns[1])
	require.NoError(t, err)
	require.Len(t, loaded.TopN.TopN, 2)
	require.Equal(t, uint64(1), loaded.TopN.TopN[0].Count)
	require.Len(t, loaded.Histogram.Buckets, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = storage.ReadColumnDistributionStats(
		ctx, testKit.Session(), tableInfo.ID, tableInfo.Columns[1])
	require.ErrorIs(t, err, context.Canceled)

	t.Run("negative NullCount aborts the whole load", func(t *testing.T) {
		testKit.MustExec(
			"update mysql.stats_histograms set null_count = -1 where table_id = ? and is_index = 0 and hist_id = ?",
			tableInfo.ID, tableInfo.Columns[1].ID)
		t.Cleanup(func() {
			testKit.MustExec(
				"update mysql.stats_histograms set null_count = 0 where table_id = ? and is_index = 0 and hist_id = ?",
				tableInfo.ID, tableInfo.Columns[1].ID)
		})

		loaded, loadErr := storage.ReadColumnDistributionStats(
			context.Background(), testKit.Session(), tableInfo.ID, tableInfo.Columns[1])
		require.ErrorContains(t, loadErr, "negative null count")
		require.Nil(t, loaded)
	})

	t.Run("TopN error aborts the whole load", func(t *testing.T) {
		loadCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const topNFailpointName = "github.com/pingcap/tidb/pkg/statistics/handle/storage/beforeTopNFromStorageWithParams"
		require.NoError(t, failpoint.EnableCall(topNFailpointName,
			func(tableID int64, _ bool, _ int64, _ int) {
				if tableID == tableInfo.ID {
					cancel()
				}
			}))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable(topNFailpointName))
		})

		histogramQueried := false
		const histogramFailpointName = "github.com/pingcap/tidb/pkg/statistics/handle/storage/beforeHistogramFromStorageWithParams"
		require.NoError(t, failpoint.EnableCall(histogramFailpointName,
			func(tableID int64, _ bool, _ int64) {
				if tableID == tableInfo.ID {
					histogramQueried = true
				}
			}))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable(histogramFailpointName))
		})

		loaded, loadErr := storage.ReadColumnDistributionStats(
			loadCtx, testKit.Session(), tableInfo.ID, tableInfo.Columns[1])
		require.ErrorIs(t, loadErr, context.Canceled)
		require.Nil(t, loaded)
		require.False(t, histogramQueried)
	})

	t.Run("Histogram error aborts the whole load", func(t *testing.T) {
		loadCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const failpointName = "github.com/pingcap/tidb/pkg/statistics/handle/storage/beforeHistogramFromStorageWithParams"
		require.NoError(t, failpoint.EnableCall(failpointName,
			func(tableID int64, _ bool, _ int64) {
				if tableID == tableInfo.ID {
					cancel()
				}
			}))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable(failpointName))
		})

		loaded, loadErr := storage.ReadColumnDistributionStats(
			loadCtx, testKit.Session(), tableInfo.ID, tableInfo.Columns[1])
		require.ErrorIs(t, loadErr, context.Canceled)
		require.Nil(t, loaded)
	})
}

func TestReadColumnDistributionStatsUsesOneSnapshot(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	writerTK := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	writerTK.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values " +
		"(1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1)," +
		"(11,2),(12,2),(13,2),(14,2),(15,2),(16,3),(17,4),(18,5),(19,6),(20,7)")
	tk.MustExec("analyze table t all columns with 2 topn, 2 buckets")

	tbl, err := dom.InfoSchema().TableByName(
		context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	colInfo := tblInfo.Columns[1]
	before, err := storage.ReadColumnDistributionStats(
		context.Background(), tk.Session(), tblInfo.ID, colInfo)
	require.NoError(t, err)

	entered := make(chan struct{})
	resume := make(chan struct{}, 1)
	blocked := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case resume <- struct{}{}:
		default:
		}
	})
	const failpointName = "github.com/pingcap/tidb/pkg/statistics/handle/storage/beforeTopNFromStorageWithParams"
	require.NoError(t, failpoint.EnableCall(failpointName,
		func(tableID int64, _ bool, _ int64, _ int) {
			if tableID != tblInfo.ID {
				return
			}
			select {
			case blocked <- struct{}{}:
				close(entered)
				<-resume
			default:
			}
		}))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable(failpointName))
	})

	type loadResult struct {
		stats *statistics.Column
		err   error
	}
	resultCh := make(chan loadResult, 1)
	go func() {
		stats, loadErr := storage.ReadColumnDistributionStats(
			context.Background(), tk.Session(), tblInfo.ID, colInfo)
		resultCh <- loadResult{stats: stats, err: loadErr}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("auto pre-split stats loader did not reach TopN loading")
	}

	// Use another session to replace the statistics while the loader is paused.
	writerTK.MustExec("update t set b = 9")
	writerTK.MustExec("analyze table t all columns with 2 topn, 2 buckets")
	resume <- struct{}{}
	result := <-resultCh
	require.NoError(t, result.err)
	require.Equal(t, before.LastUpdateVersion, result.stats.LastUpdateVersion)
	require.Equal(t, before.TopN, result.stats.TopN)
	require.Equal(t, before.Histogram, result.stats.Histogram)

	after, err := storage.ReadColumnDistributionStats(
		context.Background(), tk.Session(), tblInfo.ID, colInfo)
	require.NoError(t, err)
	require.NotEqual(t, before.LastUpdateVersion, after.LastUpdateVersion)
	require.Equal(t, uint64(20), after.TopN.TopN[0].Count)
}

func TestColumnStatsIsInvalidSkipsInternalColumnID(t *testing.T) {
	clearAsyncLoadHistogramNeededItems()
	t.Cleanup(clearAsyncLoadHistogramNeededItems)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	histColl := &statistics.HistColl{
		PhysicalID: 1,
	}
	statistics.ColumnStatsIsInvalid(nil, tk.Session().GetPlanCtx(), histColl, -1)

	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	for _, item := range items {
		require.False(t, !item.IsIndex && item.TableID == histColl.PhysicalID && item.ID <= 0)
	}
}

func TestLoadNeededHistogramsSkipsInternalColumnID(t *testing.T) {
	clearAsyncLoadHistogramNeededItems()
	t.Cleanup(clearAsyncLoadHistogramNeededItems)

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_stats_load_sync_wait = 0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t value(1,1), (2,2);")
	h := dom.StatsHandle()
	tk.MustExec("analyze table t")
	tk.MustQuery("select * from t where a = 2 and b = 2 and _tidb_rowid > 0;").Check(testkit.Rows("2 2"))

	table, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	colAID := tableInfo.Columns[0].ID
	colBID := tableInfo.Columns[1].ID
	// 1. query-triggered async loading should enqueue only real columns (a, b) and should never enqueue the internal pseudo column _tidb_rowid (ID=-1).
	require.Eventually(t, func() bool {
		items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
		hasA, hasB := false, false
		for _, item := range items {
			if item.TableID != tableInfo.ID || item.IsIndex {
				continue
			}
			// if column _tidb_rowid (ID=-1) should never enqueue,
			if item.ID <= 0 {
				return false
			}
			if item.ID == colAID {
				hasA = true
			}
			if item.ID == colBID {
				hasB = true
			}
		}
		return hasA && hasB
	}, 5*time.Second, 100*time.Millisecond)

	// Clear query-triggered items so this test can isolate the nil-sctx internal-column path.
	clearAsyncLoadHistogramNeededItems()

	// 2. even if an internal pseudo column item (ID=-1) is inserted into the queue by mistake,
	// LoadNeededHistograms should skip it safely and remove it without panic.
	statsTbl := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.NotNil(t, statsTbl)
	require.Equal(t, tableInfo.ID, statsTbl.PhysicalID)

	internalColumnItem := model.TableItemID{
		TableID: tableInfo.ID,
		ID:      -1,
	}
	asyncload.AsyncLoadHistogramNeededItems.Insert(internalColumnItem, true)

	require.NotPanics(t, func() {
		err = storage.LoadNeededHistograms(nil, dom.InfoSchema(), h, false)
		require.NoError(t, err)
	})
	require.NotContains(t, asyncload.AsyncLoadHistogramNeededItems.AllItems(), model.StatsLoadItem{
		TableItemID: internalColumnItem,
		FullLoad:    true,
	})
}

func clearAsyncLoadHistogramNeededItems() {
	for _, item := range asyncload.AsyncLoadHistogramNeededItems.AllItems() {
		asyncload.AsyncLoadHistogramNeededItems.Delete(item.TableItemID)
	}
}
