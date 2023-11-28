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

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
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

func TestReadPredicateStats(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	// no record
	item, err := storage.TablePredicateStatsFromStorage(testKit.Session(), 1, 123)
	require.Nil(t, err)
	require.Nil(t, item)
	// tblID: 1, stepHash: 1234
	rTbl := variable.ReadTableDelta{TableID: 1, Count: 10, StepHash: 1234, PredicateSelectivity: 0.01}
	txn, err := store.Begin()
	require.NoError(t, err)
	ver := txn.StartTS()
	err = storage.UpdatePredicateStatsMeta(testKit.Session(), ver, rTbl)
	require.Nil(t, err)
	item, err = storage.TablePredicateStatsFromStorage(testKit.Session(), rTbl.TableID, rTbl.StepHash)
	require.Nil(t, err)
	require.Equal(t, rTbl, item.ReadTableDelta)
	require.Equal(t, ver, item.Version)
	// tblID: 1, stepHash: 1234
	// tblID: 1, stepHash: 12345
	rTbl1 := variable.ReadTableDelta{TableID: 1, Count: 10, StepHash: 12345, PredicateSelectivity: 0.02}
	err = storage.UpdatePredicateStatsMeta(testKit.Session(), ver, rTbl1)
	require.Nil(t, err)
	item, err = storage.TablePredicateStatsFromStorage(testKit.Session(), rTbl1.TableID, rTbl1.StepHash)
	require.Nil(t, err)
	require.Equal(t, rTbl1, item.ReadTableDelta)
	require.Equal(t, ver, item.Version)
}
