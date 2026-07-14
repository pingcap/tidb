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

func newAutoSplitTestConfig() autoSplitHotRegionConfig {
	return autoSplitHotRegionConfig{
		minTableRows:                   10,
		rowsPerRegion:                  25,
		maxFullRangeRegionsPerPhysical: 4,
		maxTopNKeysPerPhysical:         2,
		regionCandidateBudget:          32,
		topNMinCount:                   10,
		topNMinRatio:                   0.1,
		minStatsHealthy:                80,
	}
}

func TestPlanAutoSplitIndexRegionsFullRange(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo()
	statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
	cfg := newAutoSplitTestConfig()
	cfg.maxTopNKeysPerPhysical = 0

	keys, _, err := planAutoSplitIndexRegions(
		sctx, fakeAutoSplitStatsProvider{tblInfo.ID: statsTbl}, tblInfo, idxInfo, cfg)
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, keys, idxInfo.ID))

	partitionTblInfo, partitionIdxInfo := buildAutoSplitTestTableInfo()
	partitionTblInfo.Partition = &model.PartitionInfo{
		Enable: true,
		Definitions: []model.PartitionDefinition{
			{ID: 101, Name: ast.NewCIStr("p0")},
			{ID: 102, Name: ast.NewCIStr("p1")},
			{ID: 103, Name: ast.NewCIStr("p2")},
			{ID: 104, Name: ast.NewCIStr("p3")},
		},
	}
	partitionStats := fakeAutoSplitStatsProvider{
		101: buildAutoSplitTestStats(101, 79, 0, partitionTblInfo.Columns[1], nil),
		102: buildAutoSplitTestStats(102, 19, 0, partitionTblInfo.Columns[1], nil),
		103: buildAutoSplitTestStats(103, 2, 0, partitionTblInfo.Columns[1], nil),
		104: buildAutoSplitTestStats(104, 10_000, 0, partitionTblInfo.Columns[1], nil),
	}
	partitionStats[104].Pseudo = true
	partitionCfg := newAutoSplitTestConfig()
	partitionCfg.minTableRows = 2
	partitionCfg.rowsPerRegion = 1
	partitionCfg.maxFullRangeRegionsPerPhysical = 100
	partitionCfg.maxTopNKeysPerPhysical = 100
	partitionCfg.regionCandidateBudget = 20

	keys, _, err = planAutoSplitIndexRegions(
		sctx, partitionStats, partitionTblInfo, partitionIdxInfo, partitionCfg)
	require.NoError(t, err)
	require.Equal(t, 7, countSplitKeysForPhysicalTable(t, keys, 101))
	require.Equal(t, 2, countSplitKeysForPhysicalTable(t, keys, 102))
	require.Equal(t, 2, countSplitKeysForPhysicalTable(t, keys, 103))
	require.Equal(t, 0, countSplitKeysForPhysicalTable(t, keys, 104))
	// The candidate budget is a soft target. The minimum per-partition budget may make the final
	// split-key count exceed it, and no physical partition should be removed by a final truncation.
	partitionCfg.regionCandidateBudget = 4
	keys, _, err = planAutoSplitIndexRegions(
		sctx, partitionStats, partitionTblInfo, partitionIdxInfo, partitionCfg)
	require.NoError(t, err)
	require.Greater(t, len(keys), partitionCfg.regionCandidateBudget)
	for _, physicalID := range []int64{101, 102, 103} {
		require.Equal(t, 2, countSplitKeysForPhysicalTable(t, keys, physicalID))
	}
	require.Equal(t, 0, countSplitKeysForPhysicalTable(t, keys, 104))

	partitionIdxInfo.Global = true
	partitionStats[partitionTblInfo.ID] = statsTbl
	keys, _, err = planAutoSplitIndexRegions(
		sctx, partitionStats, partitionTblInfo, partitionIdxInfo, partitionCfg)
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	require.Equal(t, len(keys), countSplitKeysForPhysicalTable(t, keys, partitionTblInfo.ID))
}

func TestPlanAutoSplitIndexRegionsTopN(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo()
	topN := statistics.NewTopN(3)
	topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 10), 11)
	topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 20), 50)
	topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), 30), 40)
	topN.Sort()
	statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)
	cfg := newAutoSplitTestConfig()
	cfg.rowsPerRegion = 1_000

	keys, _, err := planAutoSplitIndexRegions(
		sctx, fakeAutoSplitStatsProvider{tblInfo.ID: statsTbl}, tblInfo, idxInfo, cfg)
	require.NoError(t, err)
	require.Equal(t, 2, countSplitKeysForIndex(t, keys, idxInfo.ID))
	require.ElementsMatch(t, []string{"20", "30"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))

	defaultCfg := getAutoSplitHotRegionConfig()
	require.Equal(t, 100, defaultCfg.maxFullRangeRegionsPerPhysical)
	require.Equal(t, 100, defaultCfg.maxTopNKeysPerPhysical)
	require.Equal(t, 2560, defaultCfg.regionCandidateBudget)
	require.Equal(t, uint64(500_000), defaultCfg.topNMinCount)

	for _, tc := range []struct {
		name     string
		rowCount int64
		values   []int64
		counts   []uint64
		expected int64
	}{
		{"ratio threshold", 120_000_000, []int64{40, 50}, []uint64{1_500_000, 1_100_000}, 40},
		{"min count threshold", 10_000_000, []int64{60, 70}, []uint64{500_000, 400_000}, 60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topN := statistics.NewTopN(len(tc.values))
			for i, value := range tc.values {
				topN.AppendTopN(encodeAutoSplitIntDatum(t, sctx.GetSessionVars().StmtCtx.TimeZone(), value), tc.counts[i])
			}
			topN.Sort()
			statsTbl := buildAutoSplitTestStats(tblInfo.ID, tc.rowCount, 0, tblInfo.Columns[1], topN)
			topNRows, err := buildAutoSplitTopNRows(sctx, statsTbl, topN, tblInfo.Columns[1], defaultCfg)
			require.NoError(t, err)
			require.Len(t, topNRows, 1)
			require.Equal(t, tc.expected, topNRows[0][0].GetInt64())
		})
	}
}

func TestPlanAutoSplitIndexRegionsSkipUnreliableStats(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo()
	cfg := newAutoSplitTestConfig()

	cases := []struct {
		name           string
		statsTbl       *statistics.Table
		reasonContains string
	}{
		{"missing stats", nil, "stats missing"},
		{"pseudo stats", buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil), "stats pseudo"},
		{"outdated stats", buildAutoSplitTestStats(tblInfo.ID, 100, 80, tblInfo.Columns[1], nil), "stats outdated"},
		{"small table", buildAutoSplitTestStats(tblInfo.ID, 5, 0, tblInfo.Columns[1], nil), "below threshold"},
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
	statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
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
	for _, tc := range []struct {
		name   string
		store  kv.Storage
		result model.AutoSplitHotRegionResult
	}{
		{
			name: "split", store: &fakeAutoSplitStore{regionIDs: []uint64{1, 2, 3}},
			result: model.AutoSplitHotRegionResult{
				Status: model.AutoSplitHotRegionStatusSplit, SplitRegionCount: 3, ScatteredRegionCount: 3},
		},
		{
			name: "failed", store: &fakeAutoSplitStore{regionIDs: []uint64{1}, splitErr: context.DeadlineExceeded},
			result: model.AutoSplitHotRegionResult{
				Status: model.AutoSplitHotRegionStatusFailed, SplitRegionCount: 1, Reason: context.DeadlineExceeded.Error()},
		},
		{
			name: "unsupported", result: model.AutoSplitHotRegionResult{
				Status: model.AutoSplitHotRegionStatusUnsupported, Reason: "storage does not support split regions"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capturedKeys = nil
			err := preSplitIndexRegions(
				context.Background(), sctx, tc.store, tblInfo, []*model.IndexInfo{idxInfo},
				reorgMeta, args, statsProvider, true)
			require.NoError(t, err)
			require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
			tc.result.IndexName = "idx"
			tc.result.SplitKeyCount = 4
			require.Equal(t, []model.AutoSplitHotRegionResult{tc.result}, reorgMeta.AutoSplitHotRegionResults)
		})
	}

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
	}
	colB := &model.ColumnInfo{
		ID:        2,
		Name:      ast.NewCIStr("b"),
		Offset:    1,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	tblInfo := &model.TableInfo{
		ID:      100,
		Name:    ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{colA, colB},
	}
	idxInfo := &model.IndexInfo{
		ID:   1,
		Name: ast.NewCIStr("idx"),
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
	topN *statistics.TopN,
) *statistics.Table {
	histColl := statistics.NewHistColl(physicalID, rowCount, modifyCount, 1, 0)
	histogram := statistics.NewHistogram(colInfo.ID, 1, 0, 0, &colInfo.FieldType, 1, 0)
	lower := types.NewIntDatum(0)
	upper := types.NewIntDatum(rowCount)
	histogram.AppendBucket(&lower, &upper, rowCount, 1)
	colStats := &statistics.Column{
		PhysicalID:        physicalID,
		Info:              colInfo,
		Histogram:         *histogram,
		StatsVer:          statistics.Version2,
		TopN:              topN,
		StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
	}
	histColl.SetCol(colInfo.ID, colStats)
	existenceMap := statistics.NewColAndIndexExistenceMap(1, 0)
	existenceMap.InsertCol(colInfo.ID, true)
	return &statistics.Table{
		HistColl:              *histColl,
		Version:               1,
		LastAnalyzeVersion:    1,
		ColAndIdxExistenceMap: existenceMap,
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

func countSplitKeysForPhysicalTable(t *testing.T, keys [][]byte, physicalID int64) int {
	t.Helper()
	count := 0
	for _, key := range keys {
		if tablecodec.DecodeTableID(key) == physicalID {
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
