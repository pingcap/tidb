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
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

const autoPresplitTestTableSQL = "create table t(a bigint, b bigint, index idx(b))"

type fakeAutoPresplitStatsProvider struct {
	stats          *statistics.Table
	loadColumnTopN func(context.Context, sessionctx.Context, int64, int64, int) (*statistics.TopN, error)
}

func (p fakeAutoPresplitStatsProvider) GetPhysicalTableStats(int64, *model.TableInfo) *statistics.Table {
	return p.stats
}

func (p fakeAutoPresplitStatsProvider) LoadColumnTopN(
	ctx context.Context,
	sctx sessionctx.Context,
	physicalTableID, columnID int64,
	limit int,
) (*statistics.TopN, error) {
	if p.loadColumnTopN == nil {
		return nil, errors.New("unexpected TopN load")
	}
	return p.loadColumnTopN(ctx, sctx, physicalTableID, columnID, limit)
}

type fakeAutoPresplitStore struct {
	kv.Storage

	regionIDs []uint64
	splitErr  error
	splitFunc func(context.Context) ([]uint64, error)
}

func (s *fakeAutoPresplitStore) SplitRegions(ctx context.Context, _ [][]byte, _ bool, _ *int64) ([]uint64, error) {
	if s.splitFunc != nil {
		return s.splitFunc(ctx)
	}
	return s.regionIDs, s.splitErr
}

func (*fakeAutoPresplitStore) WaitScatterRegionFinish(context.Context, uint64, int) error {
	return nil
}

func (*fakeAutoPresplitStore) CheckRegionInScattering(uint64) (bool, error) {
	return false, nil
}

func newAutoPresplitTestConfig() autoPresplitConfig {
	return autoPresplitConfig{
		minTableRows:           10,
		maxTopNKeysPerPhysical: 2,
		topNMinCount:           10,
		topNMinRatio:           0.1,
		minStatsHealthy:        80,
	}
}

func TestPlanAutoPresplitIndexRegionsTopN(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoPresplitTestTableInfoFromSQL(t, autoPresplitTestTableSQL)
	topN := buildAutoPresplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), []int64{10, 20, 30}, []uint64{11, 50, 40})
	statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)
	cfg := newAutoPresplitTestConfig()

	keys, _, err := planAutoPresplitIndexRegions(
		context.Background(), sctx, &fakeAutoPresplitStatsProvider{stats: statsTbl}, tblInfo, idxInfo, cfg)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"20", "30"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))

	t.Run("bounded cached TopN", func(t *testing.T) {
		const topNSize = 1000
		values := make([]int64, topNSize)
		counts := make([]uint64, topNSize)
		for i := range topNSize {
			values[i] = int64(i + 1)
			counts[i] = uint64(i + 1)
		}
		topN := buildAutoPresplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), values, counts)
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, topNSize, 0, tblInfo.Columns[1], topN)
		cfg := newAutoPresplitTestConfig()
		cfg.maxTopNKeysPerPhysical = 5
		cfg.topNMinCount = 0
		cfg.topNMinRatio = 0

		rows, err := buildAutoPresplitTopNRows(sctx, statsTbl, topN, tblInfo.Columns[1], cfg)
		require.NoError(t, err)
		require.Len(t, rows, cfg.maxTopNKeysPerPhysical)
		for i, row := range rows {
			require.Equal(t, int64(topNSize-i), row[0].GetInt64())
		}
	})

	t.Run("evicted TopN uses provider", func(t *testing.T) {
		topN := buildAutoPresplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), []int64{20, 30}, []uint64{50, 40})
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
		planCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		provider := &fakeAutoPresplitStatsProvider{stats: statsTbl}
		provider.loadColumnTopN = func(
			loadCtx context.Context,
			_ sessionctx.Context,
			physicalTableID, columnID int64,
			limit int,
		) (*statistics.TopN, error) {
			require.Equal(t, planCtx, loadCtx)
			require.Equal(t, tblInfo.ID, physicalTableID)
			require.Equal(t, tblInfo.Columns[1].ID, columnID)
			require.Equal(t, cfg.maxTopNKeysPerPhysical, limit)
			return topN, nil
		}

		keys, _, err := planAutoPresplitIndexRegions(planCtx, sctx, provider, tblInfo, idxInfo, cfg)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"20", "30"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
	})

	defaultCfg := getAutoPresplitConfig()
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
			topN := buildAutoPresplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), tc.values, tc.counts)
			statsTbl := buildAutoPresplitTestStats(tblInfo.ID, tc.rowCount, 0, tblInfo.Columns[1], topN)
			topNRows, err := buildAutoPresplitTopNRows(sctx, statsTbl, topN, tblInfo.Columns[1], defaultCfg)
			require.NoError(t, err)
			require.Len(t, topNRows, 1)
			require.Equal(t, tc.expected, topNRows[0][0].GetInt64())
		})
	}

	for _, collation := range []string{"utf8mb4_general_ci", "utf8mb4_bin"} {
		t.Run(collation, func(t *testing.T) {
			tblInfo, idxInfo := buildAutoPresplitTestTableInfoFromSQL(t,
				"create table t(a bigint, b varchar(32) collate "+collation+", index idx(b))")
			values := []string{"A", "B"}
			topN := buildAutoPresplitTopN(
				t, sctx.GetSessionVars().StmtCtx.TimeZone(), values, []uint64{50, 40},
				&tblInfo.Columns[1].FieldType)
			statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)

			topNRows, err := buildAutoPresplitTopNRows(sctx, statsTbl, topN, tblInfo.Columns[1], cfg)
			require.NoError(t, err)
			require.Equal(t, types.KindBytes, topNRows[0][0].Kind())

			keys, _, err := planAutoPresplitIndexRegions(
				context.Background(), sctx, &fakeAutoPresplitStatsProvider{stats: statsTbl}, tblInfo, idxInfo, cfg)
			require.NoError(t, err)

			expectedRows := make([][]types.Datum, 0, len(values))
			for _, value := range values {
				expectedRows = append(expectedRows, []types.Datum{
					types.NewCollationStringDatum(value, collation),
				})
			}
			expectedKeys, err := getSplitIdxKeysFromValueList(sctx, tblInfo, idxInfo, expectedRows)
			require.NoError(t, err)
			require.Equal(t, sortAndDedupeAutoPresplitKeys(expectedKeys), keys)
		})
	}
}

func TestPlanAutoPresplitIndexRegionsSkipUnreliableStats(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoPresplitTestTableInfoFromSQL(t, autoPresplitTestTableSQL)
	cfg := newAutoPresplitTestConfig()
	pseudoStats := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
	pseudoStats.Pseudo = true
	partitionTblInfo, partitionIdxInfo := buildAutoPresplitTestTableInfoFromSQL(t,
		"create table t(a bigint, b bigint, index idx(b)) partition by hash(a) partitions 2")
	partialTblInfo, partialIdxInfo := buildAutoPresplitTestTableInfoFromSQL(t,
		"create table t(a bigint, b bigint, index idx(b) where a = 1)")
	prefixTblInfo, prefixIdxInfo := buildAutoPresplitTestTableInfoFromSQL(t,
		"create table t(a bigint, b varchar(32) collate utf8mb4_general_ci, index idx(b(3)))")
	prefixStats := buildAutoPresplitTestStats(prefixTblInfo.ID, 100, 0, prefixTblInfo.Columns[1], nil)
	outdatedStats := buildAutoPresplitTestStats(tblInfo.ID, 100, 80, tblInfo.Columns[1], nil)
	smallStats := buildAutoPresplitTestStats(tblInfo.ID, 5, 0, tblInfo.Columns[1], nil)

	for _, tc := range []struct {
		provider autoPresplitStatsProvider
		tblInfo  *model.TableInfo
		idxInfo  *model.IndexInfo
		reason   string
	}{
		{&fakeAutoPresplitStatsProvider{}, tblInfo, idxInfo, "stats missing"},
		{&fakeAutoPresplitStatsProvider{stats: pseudoStats}, tblInfo, idxInfo, "stats pseudo"},
		{&fakeAutoPresplitStatsProvider{stats: outdatedStats}, tblInfo, idxInfo, "stats outdated"},
		{&fakeAutoPresplitStatsProvider{stats: smallStats}, tblInfo, idxInfo, "row count 5 below threshold 10"},
		{nil, partitionTblInfo, partitionIdxInfo, "partitioned table"},
		{nil, partialTblInfo, partialIdxInfo, "partial index"},
		{&fakeAutoPresplitStatsProvider{stats: prefixStats}, prefixTblInfo, prefixIdxInfo, "leading string column uses prefix index"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			keys, reason, err := planAutoPresplitIndexRegions(
				context.Background(), sctx, tc.provider, tc.tblInfo, tc.idxInfo, cfg)
			require.NoError(t, err)
			require.Empty(t, keys)
			require.Equal(t, tc.reason, reason)
		})
	}
}

func TestAutoPresplitIndexRegionsGateAndManualOverride(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoPresplitTestTableInfoFromSQL(t, autoPresplitTestTableSQL)
	statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
	statsProvider := &fakeAutoPresplitStatsProvider{stats: statsTbl}
	reorgMeta := &model.DDLReorgMeta{}
	args := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{}}}

	var capturedKeys [][]byte
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex", func(splitKeys [][]byte) {
		capturedKeys = append(capturedKeys, splitKeys...)
	})
	runAutoPresplit := func(ctx context.Context, store kv.Storage, statsProvider autoPresplitStatsProvider) error {
		capturedKeys = nil
		if ctx == nil {
			ctx = context.Background()
		}
		return preSplitIndexRegions(
			ctx, sctx, store, tblInfo, []*model.IndexInfo{idxInfo}, reorgMeta, args, statsProvider, true)
	}

	err := preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, statsProvider, false)
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
	require.Empty(t, reorgMeta.AutoPresplitIndexRegionResults)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAutoPresplitConfig", "return(25)")
	badTopN := statistics.NewTopN(1)
	badTopN.AppendTopN([]byte{0xff}, 50)
	badStatsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], badTopN)
	err = runAutoPresplit(nil, nil, &fakeAutoPresplitStatsProvider{stats: badStatsTbl})
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
	require.Len(t, reorgMeta.AutoPresplitIndexRegionResults, 1)
	result := reorgMeta.AutoPresplitIndexRegionResults[0]
	require.Equal(t, "idx", result.IndexName)
	require.Equal(t, model.AutoPresplitIndexRegionStatusFailed, result.Status)
	require.Contains(t, result.Reason, "failed to build TopN split keys")

	hotTopN := buildAutoPresplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), []int64{20, 30}, []uint64{50, 40})
	hotStatsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], hotTopN)
	hotStatsProvider := &fakeAutoPresplitStatsProvider{stats: hotStatsTbl}
	for _, tc := range []struct {
		name   string
		store  kv.Storage
		result model.AutoPresplitIndexRegionResult
	}{
		{
			name: "split", store: &fakeAutoPresplitStore{regionIDs: []uint64{1, 2, 3}},
			result: model.AutoPresplitIndexRegionResult{
				Status: model.AutoPresplitIndexRegionStatusSplit, SplitRegionCount: 3, ScatteredRegionCount: 3},
		},
		{
			name: "failed", store: &fakeAutoPresplitStore{regionIDs: []uint64{1}, splitErr: context.DeadlineExceeded},
			result: model.AutoPresplitIndexRegionResult{
				Status: model.AutoPresplitIndexRegionStatusFailed, SplitRegionCount: 1, Reason: context.DeadlineExceeded.Error()},
		},
		{
			name: "unsupported", result: model.AutoPresplitIndexRegionResult{
				Status: model.AutoPresplitIndexRegionStatusUnsupported, Reason: "storage does not support split regions"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runAutoPresplit(nil, tc.store, hotStatsProvider)
			require.NoError(t, err)
			require.Equal(t, 2, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
			tc.result.IndexName = "idx"
			tc.result.SplitKeyCount = 3
			require.Equal(t, []model.AutoPresplitIndexRegionResult{tc.result}, reorgMeta.AutoPresplitIndexRegionResults)
		})
	}

	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "parent cancel", cause: dbterror.ErrCancelledDDLJob},
		{name: "parent pause", cause: dbterror.ErrPausedDDLJob.FastGenByArgs(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			store := &fakeAutoPresplitStore{
				splitFunc: func(splitCtx context.Context) ([]uint64, error) {
					cancel(tc.cause)
					<-splitCtx.Done()
					return nil, splitCtx.Err()
				},
			}

			err := runAutoPresplit(ctx, store, hotStatsProvider)
			require.Same(t, tc.cause, err)
			require.Empty(t, reorgMeta.AutoPresplitIndexRegionResults)
		})
	}

	t.Run("TopN load cancellation", func(t *testing.T) {
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
		ctx, cancel := context.WithCancelCause(context.Background())
		pauseErr := dbterror.ErrPausedDDLJob.FastGenByArgs(2)
		provider := &fakeAutoPresplitStatsProvider{stats: statsTbl}
		provider.loadColumnTopN = func(
			loadCtx context.Context,
			_ sessionctx.Context,
			_, _ int64,
			_ int,
		) (*statistics.TopN, error) {
			cancel(pauseErr)
			<-loadCtx.Done()
			return nil, loadCtx.Err()
		}

		err := runAutoPresplit(ctx, nil, provider)
		require.True(t, dbterror.ErrPausedDDLJob.Equal(err))
		require.Empty(t, reorgMeta.AutoPresplitIndexRegionResults)
	})

	manualArgs := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{
		SplitOpt: &model.IndexArgSplitOpt{Num: 4},
	}}}
	capturedKeys = nil
	err = preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, manualArgs, nil, true)
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
	require.Empty(t, reorgMeta.AutoPresplitIndexRegionResults)

	err = runAutoPresplit(nil, nil, statsProvider)
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
	require.Equal(t, []model.AutoPresplitIndexRegionResult{{
		IndexName: "idx",
		Status:    model.AutoPresplitIndexRegionStatusSkipped,
		Reason:    "no split strategy matched",
	}}, reorgMeta.AutoPresplitIndexRegionResults)
}

func buildAutoPresplitTestTableInfoFromSQL(t *testing.T, createSQL string) (*model.TableInfo, *model.IndexInfo) {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	tblInfo, err := BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	tblInfo.ID = 100
	return tblInfo, tblInfo.Indices[0]
}

func buildAutoPresplitTestStats(
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

func buildAutoPresplitTopN[T int64 | string](
	t *testing.T,
	loc *time.Location,
	values []T,
	counts []uint64,
	fieldType ...*types.FieldType,
) *statistics.TopN {
	t.Helper()
	require.Len(t, counts, len(values))
	require.LessOrEqual(t, len(fieldType), 1)
	topN := statistics.NewTopN(len(values))
	for i, value := range values {
		var datum types.Datum
		switch value := any(value).(type) {
		case int64:
			datum = types.NewIntDatum(value)
		case string:
			require.Len(t, fieldType, 1)
			datum = types.NewBytesDatum(codec.ConvertByCollation([]byte(value), fieldType[0]))
		}
		encoded, err := codec.EncodeKey(loc, nil, datum)
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
