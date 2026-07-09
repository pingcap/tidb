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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type fakeAutoSplitStatsProvider map[int64]*statistics.Table

func (p fakeAutoSplitStatsProvider) GetPhysicalTableStats(physicalTableID int64, _ *model.TableInfo) *statistics.Table {
	return p[physicalTableID]
}

type fakeAutoSplitStore struct {
	kv.Storage

	regionIDs  []uint64
	splitErr   error
	scatterErr error
}

func (s *fakeAutoSplitStore) SplitRegions(context.Context, [][]byte, bool, *int64) ([]uint64, error) {
	return s.regionIDs, s.splitErr
}

func (s *fakeAutoSplitStore) WaitScatterRegionFinish(context.Context, uint64, int) error {
	return s.scatterErr
}

func (*fakeAutoSplitStore) CheckRegionInScattering(uint64) (bool, error) {
	return false, nil
}

func TestPlanAutoSplitIndexRegionsFullRange(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo()
	statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], true, nil)
	cfg := autoSplitHotRegionConfig{
		minTableRows:           10,
		rowsPerRegion:          25,
		maxRegionsPerPhysical:  4,
		maxTopNKeysPerPhysical: 0,
		maxSplitKeys:           32,
		minStatsHealthy:        80,
	}

	keys, reason, err := planAutoSplitIndexRegions(
		sctx, fakeAutoSplitStatsProvider{tblInfo.ID: statsTbl}, tblInfo, idxInfo, cfg)
	require.NoError(t, err)
	require.Equal(t, "auto split keys generated", reason)
	require.Equal(t, 3, countSplitKeysForIndex(t, keys, idxInfo.ID))
}

func TestPlanAutoSplitIndexRegionsTopN(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo()
	topN := statistics.NewTopN(3)
	topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 10), 11)
	topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 20), 50)
	topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 30), 40)
	topN.Sort()
	statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], true, topN)
	cfg := autoSplitHotRegionConfig{
		minTableRows:           10,
		rowsPerRegion:          1_000,
		maxRegionsPerPhysical:  4,
		maxTopNKeysPerPhysical: 2,
		maxSplitKeys:           32,
		topNMinCount:           10,
		topNMinRatio:           0.1,
		minStatsHealthy:        80,
	}

	keys, reason, err := planAutoSplitIndexRegions(
		sctx, fakeAutoSplitStatsProvider{tblInfo.ID: statsTbl}, tblInfo, idxInfo, cfg)
	require.NoError(t, err)
	require.Equal(t, "auto split keys generated", reason)
	require.Equal(t, 2, countSplitKeysForIndex(t, keys, idxInfo.ID))
	require.ElementsMatch(t, []string{"20", "30"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))

	defaultCfg := getAutoSplitHotRegionConfig()
	t.Run("default limits", func(t *testing.T) {
		require.Equal(t, 100, defaultCfg.maxRegionsPerPhysical)
		require.Equal(t, 100, defaultCfg.maxTopNKeysPerPhysical)
		require.Equal(t, 2560, defaultCfg.maxSplitKeys)
		require.Equal(t, uint64(1_000_000), defaultCfg.topNMinCount)
	})
	t.Run("default ratio threshold", func(t *testing.T) {
		topN := statistics.NewTopN(2)
		topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 40), 1_500_000)
		topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 50), 1_100_000)
		topN.Sort()
		statsTbl := buildAutoSplitTestStats(tblInfo.ID, 120_000_000, 0, tblInfo.Columns[1], true, topN)
		topNRows, err := buildAutoSplitTopNRows(
			sctx, statsTbl, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], defaultCfg)
		require.NoError(t, err)
		require.Len(t, topNRows, 1)
		require.Equal(t, int64(40), topNRows[0][0].GetInt64())
	})
	t.Run("default min count threshold", func(t *testing.T) {
		topN := statistics.NewTopN(1)
		topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 60), 900_000)
		topN.Sort()
		statsTbl := buildAutoSplitTestStats(tblInfo.ID, 10_000_000, 0, tblInfo.Columns[1], true, topN)
		topNRows, err := buildAutoSplitTopNRows(
			sctx, statsTbl, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], defaultCfg)
		require.NoError(t, err)
		require.Empty(t, topNRows)
	})
}

func TestPlanAutoSplitIndexRegionsSkipUnreliableStats(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo()
	cfg := autoSplitHotRegionConfig{
		minTableRows:           10,
		rowsPerRegion:          25,
		maxRegionsPerPhysical:  4,
		maxTopNKeysPerPhysical: 2,
		maxSplitKeys:           32,
		topNMinCount:           10,
		topNMinRatio:           0.1,
		minStatsHealthy:        80,
	}

	cases := []struct {
		name           string
		statsTbl       *statistics.Table
		reasonContains string
	}{
		{
			name:           "missing stats",
			reasonContains: "stats missing",
		},
		{
			name:           "pseudo stats",
			statsTbl:       buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], true, nil),
			reasonContains: "stats pseudo",
		},
		{
			name:           "outdated stats",
			statsTbl:       buildAutoSplitTestStats(tblInfo.ID, 100, 80, tblInfo.Columns[1], true, nil),
			reasonContains: "stats outdated",
		},
		{
			name:           "small table",
			statsTbl:       buildAutoSplitTestStats(tblInfo.ID, 5, 0, tblInfo.Columns[1], true, nil),
			reasonContains: "below threshold",
		},
		{
			name:           "unloaded leading column",
			statsTbl:       buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], false, nil),
			reasonContains: "not fully loaded",
		},
	}
	cases[1].statsTbl.Pseudo = true

	for _, tc := range cases {
		keys, reason, err := planAutoSplitIndexRegions(
			sctx, fakeAutoSplitStatsProvider{tblInfo.ID: tc.statsTbl}, tblInfo, idxInfo, cfg)
		require.NoError(t, err, tc.name)
		require.Empty(t, keys, tc.name)
		require.Contains(t, reason, tc.reasonContains, tc.name)
	}
}

func TestPreSplitIndexRegionsAutoGateAndManualOverride(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo()
	statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], true, nil)
	statsProvider := fakeAutoSplitStatsProvider{tblInfo.ID: statsTbl}
	reorgMeta := &model.DDLReorgMeta{}

	var capturedKeys [][]byte
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex", func(splitKeys [][]byte) {
		capturedKeys = append(capturedKeys, splitKeys...)
	})

	args := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{}}}
	err := preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, statsProvider, false)
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
	require.Empty(t, reorgMeta.AutoSplitHotRegionResults)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAutoSplitHotRegionConfig", "return(25)")
	store := &fakeAutoSplitStore{regionIDs: []uint64{1, 2, 3}}
	err = preSplitIndexRegions(
		context.Background(), sctx, store, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, statsProvider, true)
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
	require.Equal(t, []model.AutoSplitHotRegionResult{{
		IndexName:            "idx",
		IndexID:              idxInfo.ID,
		Status:               model.AutoSplitHotRegionStatusSplit,
		SplitKeyCount:        4,
		SplitRegionCount:     3,
		ScatteredRegionCount: 3,
	}}, reorgMeta.AutoSplitHotRegionResults)

	capturedKeys = nil
	store = &fakeAutoSplitStore{regionIDs: []uint64{1}, splitErr: context.DeadlineExceeded}
	err = preSplitIndexRegions(
		context.Background(), sctx, store, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, statsProvider, true)
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
	require.Equal(t, []model.AutoSplitHotRegionResult{{
		IndexName:        "idx",
		IndexID:          idxInfo.ID,
		Status:           model.AutoSplitHotRegionStatusFailed,
		SplitKeyCount:    4,
		SplitRegionCount: 1,
		Reason:           context.DeadlineExceeded.Error(),
	}}, reorgMeta.AutoSplitHotRegionResults)

	capturedKeys = nil
	err = preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, statsProvider, true)
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
	require.Equal(t, []model.AutoSplitHotRegionResult{{
		IndexName:     "idx",
		IndexID:       idxInfo.ID,
		Status:        model.AutoSplitHotRegionStatusUnsupported,
		SplitKeyCount: 4,
		Reason:        "storage does not support split regions",
	}}, reorgMeta.AutoSplitHotRegionResults)

	capturedKeys = nil
	manualArgs := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{
		SplitOpt: &model.IndexArgSplitOpt{Num: 4},
	}}}
	err = preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, manualArgs, nil, true)
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
	require.Empty(t, reorgMeta.AutoSplitHotRegionResults)
}

func buildAutoSplitTestTableInfo() (*model.TableInfo, *model.IndexInfo) {
	colA := &model.ColumnInfo{
		ID:        1,
		Name:      ast.NewCIStr("a"),
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	colB := &model.ColumnInfo{
		ID:        2,
		Name:      ast.NewCIStr("b"),
		Offset:    1,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	tblInfo := &model.TableInfo{
		ID:      100,
		Name:    ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{colA, colB},
		State:   model.StatePublic,
	}
	idxInfo := &model.IndexInfo{
		ID:    1,
		Name:  ast.NewCIStr("idx"),
		State: model.StateNone,
		Columns: []*model.IndexColumn{{
			Name:   colB.Name,
			Offset: colB.Offset,
			Length: types.UnspecifiedLength,
		}},
	}
	return tblInfo, idxInfo
}

func buildAutoSplitTestStats(
	physicalID int64,
	rowCount int64,
	modifyCount int64,
	colInfo *model.ColumnInfo,
	fullLoad bool,
	topN *statistics.TopN,
) *statistics.Table {
	histColl := statistics.NewHistColl(physicalID, rowCount, modifyCount, 1, 0)
	histogram := statistics.NewHistogram(colInfo.ID, 1, 0, 0, &colInfo.FieldType, 1, 0)
	lower := types.NewIntDatum(0)
	upper := types.NewIntDatum(rowCount)
	histogram.AppendBucket(&lower, &upper, rowCount, 1)
	colStats := &statistics.Column{
		PhysicalID: physicalID,
		Info:       colInfo,
		Histogram:  *histogram,
		StatsVer:   statistics.Version2,
		TopN:       topN,
	}
	if fullLoad {
		colStats.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
	} else {
		colStats.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
	}
	histColl.SetCol(colInfo.ID, colStats)
	return &statistics.Table{
		HistColl:           *histColl,
		Version:            1,
		LastAnalyzeVersion: 1,
	}
}

func encodeAutoSplitIntDatum(t *testing.T, loc *time.Location, val int64) []byte {
	encoded, err := codec.EncodeKey(loc, nil, types.NewIntDatum(val))
	require.NoError(t, err)
	return encoded
}

func countSplitKeysForIndex(t *testing.T, keys [][]byte, indexID int64) int {
	t.Helper()
	count := 0
	for _, key := range keys {
		decodedIndexID, err := tablecodec.DecodeIndexID(key)
		if err == nil && decodedIndexID == indexID {
			count++
		}
	}
	return count
}

func splitKeyFirstValuesForIndex(t *testing.T, keys [][]byte, indexID int64) []string {
	t.Helper()
	values := make([]string, 0)
	for _, key := range keys {
		_, decodedIndexID, decodedValues, err := tablecodec.DecodeIndexKey(key)
		if err == nil && decodedIndexID == indexID && len(decodedValues) > 0 {
			values = append(values, decodedValues[0])
		}
	}
	return values
}
