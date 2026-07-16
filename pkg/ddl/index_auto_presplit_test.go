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
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

	regionIDs []uint64
	splitErr  error
}

func (s *fakeAutoSplitStore) SplitRegions(context.Context, [][]byte, bool, *int64) ([]uint64, error) {
	return s.regionIDs, s.splitErr
}

func (*fakeAutoSplitStore) WaitScatterRegionFinish(context.Context, uint64, int) error {
	return nil
}

func (*fakeAutoSplitStore) CheckRegionInScattering(uint64) (bool, error) {
	return false, nil
}

func newAutoSplitTestConfig() autoSplitHotRegionConfig {
	return autoSplitHotRegionConfig{
		minTableRows:           10,
		maxTopNKeysPerPhysical: 2,
		topNMinCount:           10,
		topNMinRatio:           0.1,
		minStatsHealthy:        80,
	}
}

func TestPlanAutoSplitIndexRegionsTopN(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo(t)
	topN := buildAutoSplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), []int64{10, 20, 30}, []uint64{11, 50, 40})
	statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)
	cfg := newAutoSplitTestConfig()

	keys, _, err := planAutoSplitIndexRegions(
		sctx, fakeAutoSplitStatsProvider{tblInfo.ID: statsTbl}, tblInfo, idxInfo, cfg)
	require.NoError(t, err)
	require.Equal(t, 2, countSplitKeysForIndex(t, keys, idxInfo.ID))
	require.ElementsMatch(t, []string{"20", "30"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))

	defaultCfg := getAutoSplitHotRegionConfig()
	require.Equal(t, 100, defaultCfg.maxTopNKeysPerPhysical)
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
			topN := buildAutoSplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), tc.values, tc.counts)
			statsTbl := buildAutoSplitTestStats(tblInfo.ID, tc.rowCount, 0, tblInfo.Columns[1], topN)
			topNRows, err := buildAutoSplitTopNRows(sctx, statsTbl, topN, tblInfo.Columns[1], defaultCfg)
			require.NoError(t, err)
			require.Len(t, topNRows, 1)
			require.Equal(t, tc.expected, topNRows[0][0].GetInt64())
		})
	}

	for _, tc := range []struct {
		name      string
		createSQL string
		collation string
	}{
		{
			name:      "general ci string",
			createSQL: "create table t(a bigint, b varchar(32) collate utf8mb4_general_ci, index idx(b))",
			collation: "utf8mb4_general_ci",
		},
		{
			name:      "binary string",
			createSQL: "create table t(a bigint, b varchar(32) collate utf8mb4_bin, index idx(b))",
			collation: "utf8mb4_bin",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tblInfo, idxInfo := buildAutoSplitTestTableInfoFromSQL(t, tc.createSQL)
			values := []string{"A", "B"}
			topN := buildAutoSplitStringTopN(
				t, sctx.GetSessionVars().StmtCtx.TimeZone(), &tblInfo.Columns[1].FieldType,
				values, []uint64{50, 40})
			statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)

			topNRows, err := buildAutoSplitTopNRows(sctx, statsTbl, topN, tblInfo.Columns[1], cfg)
			require.NoError(t, err)
			require.Equal(t, types.KindBytes, topNRows[0][0].Kind())

			keys, _, err := planAutoSplitIndexRegions(
				sctx, fakeAutoSplitStatsProvider{tblInfo.ID: statsTbl}, tblInfo, idxInfo, cfg)
			require.NoError(t, err)

			expectedRows := make([][]types.Datum, 0, len(values))
			for _, value := range values {
				expectedRows = append(expectedRows, []types.Datum{
					types.NewCollationStringDatum(value, tc.collation),
				})
			}
			expectedKeys, err := getSplitIdxKeysFromValueList(sctx, tblInfo, idxInfo, expectedRows)
			require.NoError(t, err)
			require.Equal(t, sortAndDedupeAutoSplitKeys(expectedKeys), keys)
		})
	}

	t.Run("string prefix index", func(t *testing.T) {
		tblInfo, idxInfo := buildAutoSplitTestTableInfoFromSQL(t,
			"create table t(a bigint, b varchar(32) collate utf8mb4_general_ci, index idx(b(3)))")
		topN := buildAutoSplitStringTopN(
			t, sctx.GetSessionVars().StmtCtx.TimeZone(), &tblInfo.Columns[1].FieldType,
			[]string{"abcdef"}, []uint64{50})
		statsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)

		keys, reason, err := planAutoSplitIndexRegions(
			sctx, fakeAutoSplitStatsProvider{tblInfo.ID: statsTbl}, tblInfo, idxInfo, cfg)
		require.NoError(t, err)
		require.Empty(t, keys)
		require.Equal(t, "leading string column uses prefix index", reason)
	})
}

func TestPlanAutoSplitIndexRegionsSkipUnreliableStats(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo(t)
	cfg := newAutoSplitTestConfig()
	pseudoStats := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
	pseudoStats.Pseudo = true

	cases := []struct {
		name     string
		statsTbl *statistics.Table
		reason   string
	}{
		{"missing stats", nil, "stats missing"},
		{"pseudo stats", pseudoStats, "stats pseudo"},
		{"outdated stats", buildAutoSplitTestStats(tblInfo.ID, 100, 80, tblInfo.Columns[1], nil), "stats outdated"},
		{"small table", buildAutoSplitTestStats(tblInfo.ID, 5, 0, tblInfo.Columns[1], nil), "row count 5 below threshold 10"},
	}

	for _, tc := range cases {
		keys, reason, err := planAutoSplitIndexRegions(
			sctx, fakeAutoSplitStatsProvider{tblInfo.ID: tc.statsTbl}, tblInfo, idxInfo, cfg)
		require.NoError(t, err, tc.name)
		require.Empty(t, keys, tc.name)
		require.Equal(t, tc.reason, reason, tc.name)
	}

	for _, tc := range []struct {
		name   string
		global bool
	}{
		{"partitioned local index", false},
		{"partitioned global index", true},
	} {
		partitionTblInfo, partitionIdxInfo := buildPartitionedAutoSplitTestTableInfo(t)
		partitionIdxInfo.Global = tc.global
		keys, reason, err := planAutoSplitIndexRegions(sctx, nil, partitionTblInfo, partitionIdxInfo, cfg)
		require.NoError(t, err, tc.name)
		require.Empty(t, keys, tc.name)
		require.Equal(t, "partitioned table", reason, tc.name)
	}
}

func TestPreSplitIndexRegionsAutoGateAndManualOverride(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoSplitTestTableInfo(t)
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

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockAutoSplitHotRegionConfig",
		func(_ *autoSplitHotRegionConfig, setMinRows func(int)) {
			setMinRows(25)
		})
	badTopN := statistics.NewTopN(1)
	badTopN.AppendTopN([]byte{0xff}, 50)
	badStatsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], badTopN)
	capturedKeys = nil
	err = preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, fakeAutoSplitStatsProvider{tblInfo.ID: badStatsTbl}, true)
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
	require.Len(t, reorgMeta.AutoSplitHotRegionResults, 1)
	result := reorgMeta.AutoSplitHotRegionResults[0]
	require.Equal(t, "idx", result.IndexName)
	require.Equal(t, model.AutoSplitHotRegionStatusFailed, result.Status)
	require.Contains(t, result.Reason, "failed to build TopN split keys")

	hotTopN := buildAutoSplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), []int64{20, 30}, []uint64{50, 40})
	hotStatsTbl := buildAutoSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], hotTopN)
	hotStatsProvider := fakeAutoSplitStatsProvider{tblInfo.ID: hotStatsTbl}
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
				reorgMeta, args, hotStatsProvider, true)
			require.NoError(t, err)
			require.Equal(t, 2, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
			tc.result.IndexName = "idx"
			tc.result.SplitKeyCount = 3
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

	tblInfo, idxInfo = buildPartitionedAutoSplitTestTableInfo(t)
	capturedKeys = nil
	err = preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, nil, true)
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
	require.Equal(t, []model.AutoSplitHotRegionResult{{
		IndexName: "idx",
		Status:    model.AutoSplitHotRegionStatusSkipped,
		Reason:    "partitioned table",
	}}, reorgMeta.AutoSplitHotRegionResults)

	capturedKeys = nil
	err = preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, manualArgs, nil, true)
	require.NoError(t, err)
	require.Equal(t, 4, countSplitKeysForPhysicalTable(t, capturedKeys, 101))
	require.Equal(t, 4, countSplitKeysForPhysicalTable(t, capturedKeys, 102))
	require.Equal(t, 6, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
	require.Empty(t, reorgMeta.AutoSplitHotRegionResults)
}

func buildAutoSplitTestTableInfo(t *testing.T) (*model.TableInfo, *model.IndexInfo) {
	return buildAutoSplitTestTableInfoFromSQL(t, "create table t(a bigint, b bigint, index idx(b))")
}

func buildAutoSplitTestTableInfoFromSQL(t *testing.T, createSQL string) (*model.TableInfo, *model.IndexInfo) {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	tblInfo, err := BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	tblInfo.ID = 100
	return tblInfo, tblInfo.Indices[0]
}

func buildPartitionedAutoSplitTestTableInfo(t *testing.T) (*model.TableInfo, *model.IndexInfo) {
	tblInfo, idxInfo := buildAutoSplitTestTableInfo(t)
	tblInfo.Partition = &model.PartitionInfo{
		Enable:      true,
		Definitions: []model.PartitionDefinition{{ID: 101, Name: ast.NewCIStr("p0")}, {ID: 102, Name: ast.NewCIStr("p1")}},
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
	lower, upper := types.NewIntDatum(0), types.NewIntDatum(rowCount)
	if types.IsString(colInfo.GetType()) {
		lower, upper = types.NewBytesDatum(nil), types.NewBytesDatum([]byte{0xff})
	}
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

func buildAutoSplitTopN(t *testing.T, loc *time.Location, values []int64, counts []uint64) *statistics.TopN {
	t.Helper()
	require.Len(t, counts, len(values))
	topN := statistics.NewTopN(len(values))
	for i, value := range values {
		encoded, err := codec.EncodeKey(loc, nil, types.NewIntDatum(value))
		require.NoError(t, err)
		topN.AppendTopN(encoded, counts[i])
	}
	topN.Sort()
	return topN
}

func buildAutoSplitStringTopN(
	t *testing.T,
	loc *time.Location,
	fieldType *types.FieldType,
	values []string,
	counts []uint64,
) *statistics.TopN {
	t.Helper()
	require.Len(t, counts, len(values))
	topN := statistics.NewTopN(len(values))
	for i, value := range values {
		collationKey := codec.ConvertByCollation([]byte(value), fieldType)
		encoded, err := codec.EncodeKey(loc, nil, types.NewBytesDatum(collationKey))
		require.NoError(t, err)
		topN.AppendTopN(encoded, counts[i])
	}
	topN.Sort()
	return topN
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
