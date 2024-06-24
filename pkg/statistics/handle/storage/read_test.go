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
	"github.com/pingcap/tidb/pkg/statistics"
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
	c, ok := stat.Columns[colAID]
	require.True(t, !ok || c.Histogram.Len() == 0)
	require.True(t, !ok || c.CMSketch == nil)
	require.True(t, stat.Indices[idxBID].IsAllEvicted())
	idx, ok := stat.Indices[idxBID]
	require.True(t, !ok || idx.Histogram.Len() == 0)
	require.True(t, !ok || (idx.CMSketch.TotalCount()+idx.TopN.TotalCount() == 0))
	require.True(t, stat.Columns[colCID].IsAllEvicted())
	c, ok = stat.Columns[colCID]
	require.True(t, !ok || c.Histogram.Len() == 0)
	require.True(t, !ok || c.CMSketch == nil)

	// Column stats are loaded after they are needed.
	_, err = cardinality.ColumnEqualRowCount(testKit.Session().GetPlanCtx(), stat, types.NewIntDatum(1), colAID)
	require.NoError(t, err)
	_, err = cardinality.ColumnEqualRowCount(testKit.Session().GetPlanCtx(), stat, types.NewIntDatum(1), colCID)
	require.NoError(t, err)
	require.NoError(t, h.LoadNeededHistograms())
	stat = h.GetTableStats(tableInfo)
	require.True(t, stat.Columns[colAID].IsFullLoad())
	hg := stat.Columns[colAID].Histogram
	require.Greater(t, hg.Len(), 0)
	// We don't maintain cmsketch for pk.
	cms := stat.Columns[colAID].CMSketch
	require.Nil(t, cms)
	require.True(t, stat.Columns[colCID].IsFullLoad())
	hg = stat.Columns[colCID].Histogram
	require.Greater(t, hg.Len(), 0)
	cms = stat.Columns[colCID].CMSketch
	require.NotNil(t, cms)

	// Index stats are loaded after they are needed.
	idx, ok = stat.Indices[idxBID]
	require.True(t, !ok || (float64(idx.CMSketch.TotalCount())+float64(idx.TopN.TotalCount())+idx.Histogram.TotalRowCount() == 0))
	require.False(t, ok && idx.IsEssentialStatsLoaded())
	// IsInvalid adds the index to AsyncLoadHistogramNeededItems.
	statistics.IndexStatsIsInvalid(testKit.Session().GetPlanCtx(), idx, &stat.HistColl, idxBID)
	require.NoError(t, h.LoadNeededHistograms())
	stat = h.GetTableStats(tableInfo)
	idx = stat.Indices[tableInfo.Indices[0].ID]
	hg = idx.Histogram
	cms = idx.CMSketch
	topN := idx.TopN
	require.Greater(t, float64(cms.TotalCount()+topN.TotalCount())+hg.TotalRowCount(), float64(0))
	require.True(t, idx.IsFullLoad())
}
