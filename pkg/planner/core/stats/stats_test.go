// Copyright 2026 PingCAP, Inc.
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

package stats_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type stubStatsProvider struct {
	tables     map[int64]*statistics.Table
	queriedIDs []int64
}

func (s *stubStatsProvider) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	s.queriedIDs = append(s.queriedIDs, physicalTableID)
	if tbl, ok := s.tables[physicalTableID]; ok {
		return tbl
	}
	tbl := statistics.PseudoTable(tblInfo, false, true)
	tbl.PhysicalID = physicalTableID
	return tbl
}

func makeAnalyzedStatsTable(physicalID, rowCount int64) *statistics.Table {
	histColl := *statistics.NewHistColl(physicalID, rowCount, 0, 1, 0)
	tbl := &statistics.Table{
		HistColl:              histColl,
		ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(1, 0),
	}
	tbl.StatsVer = statistics.Version2
	tbl.Pseudo = false
	tbl.Version = 100
	tbl.SetCol(1, &statistics.Column{
		PhysicalID:        physicalID,
		StatsVer:          statistics.Version2,
		StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
	})
	return tbl
}

func TestStatsProviderBindingAndFallback(t *testing.T) {
	ctx := mock.NewContext()
	require.Nil(t, ctx.GetDomain())
	require.Nil(t, stats.GetStatsProvider(ctx))

	tblInfo := &model.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("a"), State: model.StatePublic},
		},
	}

	// Without a Domain or bound provider, GetStatsTable falls back to pseudo stats.
	pseudo := stats.GetStatsTable(ctx, tblInfo, tblInfo.ID)
	require.True(t, pseudo.Pseudo)

	globalStats := makeAnalyzedStatsTable(100, 12345)
	partStats := makeAnalyzedStatsTable(101, 678)
	provider := &stubStatsProvider{
		tables: map[int64]*statistics.Table{
			100: globalStats,
			101: partStats,
		},
	}

	ctx.GetSessionVars().StatsProvider = provider
	require.Equal(t, provider, stats.GetStatsProvider(ctx))

	// Non-dynamic partition prune queries the requested physical partition ID.
	ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode = false
	gotPart := stats.GetStatsTable(ctx, tblInfo, 101)
	require.False(t, gotPart.Pseudo)
	require.Equal(t, int64(678), gotPart.RealtimeCount)

	// Dynamic partition prune queries the logical table ID (global stats).
	ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode = true
	gotGlobal := stats.GetStatsTable(ctx, tblInfo, 101)
	require.False(t, gotGlobal.Pseudo)
	require.Equal(t, int64(12345), gotGlobal.RealtimeCount)

	// Uncached table ID returns pseudo stats from the provider.
	ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode = false
	gotMissing := stats.GetStatsTable(ctx, tblInfo, 102)
	require.True(t, gotMissing.Pseudo)

	// Clearing the provider restores the default fallback behavior.
	ctx.GetSessionVars().StatsProvider = nil
	require.Nil(t, stats.GetStatsProvider(ctx))
	require.True(t, stats.GetStatsTable(ctx, tblInfo, tblInfo.ID).Pseudo)
}
