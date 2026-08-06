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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	statshandle "github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const autoPreSplitTestTableSQL = "create table t(a bigint, b bigint, index idx(b))"

type fakeAutoPreSplitStatsProvider struct {
	stats                 *statistics.Table
	getPhysicalTableStats func()
	loadColumnStats       func(context.Context, sessionctx.Context, int64, *model.ColumnInfo, int) (*statshandle.ColumnDistributionStats, error)
}

func (p *fakeAutoPreSplitStatsProvider) GetPhysicalTableStats(int64, *model.TableInfo) *statistics.Table {
	if p.getPhysicalTableStats != nil {
		p.getPhysicalTableStats()
	}
	return p.stats
}

func (p *fakeAutoPreSplitStatsProvider) LoadColumnDistributionStats(
	ctx context.Context,
	sctx sessionctx.Context,
	physicalTableID int64,
	colInfo *model.ColumnInfo,
	maxTopNKeys int,
) (*statshandle.ColumnDistributionStats, error) {
	if p.loadColumnStats == nil {
		return nil, errors.New("unexpected column stats load")
	}
	return p.loadColumnStats(ctx, sctx, physicalTableID, colInfo, maxTopNKeys)
}

type fakeAutoPreSplitStore struct {
	kv.Storage

	regionIDs []uint64
	splitErr  error
	splitFunc func(context.Context) ([]uint64, error)
}

func (s *fakeAutoPreSplitStore) SplitRegions(ctx context.Context, _ [][]byte, _ bool, _ *int64) ([]uint64, error) {
	if s.splitFunc != nil {
		return s.splitFunc(ctx)
	}
	return s.regionIDs, s.splitErr
}

func (*fakeAutoPreSplitStore) WaitScatterRegionFinish(context.Context, uint64, int) error {
	return nil
}

func (*fakeAutoPreSplitStore) CheckRegionInScattering(uint64) (bool, error) {
	return false, nil
}

func newAutoPreSplitTestConfig() autoPreSplitConfig {
	return autoPreSplitConfig{
		minTableRows:           10,
		maxTopNKeysPerPhysical: 100_000,
		statsLoadTimeout:       time.Minute,
		minStatsHealthy:        80,
		boundaryRatioStep:      0.2,
	}
}

func TestPlanAutoPreSplitIndexRegionsTopN(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoPreSplitTestTableInfoFromSQL(t, autoPreSplitTestTableSQL)
	cfg := newAutoPreSplitTestConfig()

	topN := buildAutoPreSplitTopN(
		t, sctx.GetSessionVars().StmtCtx.TimeZone(),
		[]int64{10, 50, 90}, []uint64{25, 14, 11})
	statsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)
	setAutoPreSplitTestHistogram(
		t, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 25,
		types.MakeDatums(20, 40, 60, 80, 100), []int64{10, 10, 10, 10, 10})
	keys, _, err := planAutoPreSplitIndexRegions(
		context.Background(), sctx, &fakeAutoPreSplitStatsProvider{stats: statsTbl},
		tblInfo, idxInfo, cfg)
	require.NoError(t, err)
	require.Equal(t, []string{"", "10", "50", "80"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))

	t.Run("bounded cached TopN", func(t *testing.T) {
		const topNSize = 1000
		values := make([]int64, topNSize)
		counts := make([]uint64, topNSize)
		for i := range topNSize {
			values[i] = int64(i + 1)
			counts[i] = uint64(i + 1)
		}
		topN := buildAutoPreSplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), values, counts)
		events, err := buildAutoPreSplitTopNEvents(sctx, topN, tblInfo.Columns[1], 5)
		require.NoError(t, err)
		require.Len(t, events, 5)
		for i, event := range events {
			require.Equal(t, int64(i+1), event.value.GetInt64())
		}
	})

	t.Run("evicted stats use one provider result", func(t *testing.T) {
		loadedTopN := buildAutoPreSplitTopN(
			t, sctx.GetSessionVars().StmtCtx.TimeZone(),
			[]int64{10, 50, 90}, []uint64{25, 14, 11})
		loadedStats := buildAutoPreSplitTestStats(
			tblInfo.ID, 100, 0, tblInfo.Columns[1], loadedTopN)
		setAutoPreSplitTestHistogram(
			t, loadedStats.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
			types.MakeDatums(20, 40, 60, 80, 100), []int64{10, 10, 10, 10, 10})
		statsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
		planCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		provider := &fakeAutoPreSplitStatsProvider{stats: statsTbl}
		provider.loadColumnStats = func(
			loadCtx context.Context,
			_ sessionctx.Context,
			physicalTableID int64,
			colInfo *model.ColumnInfo,
			maxTopNKeys int,
		) (*statshandle.ColumnDistributionStats, error) {
			deadline, ok := loadCtx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(cfg.statsLoadTimeout), deadline, time.Second)
			require.Equal(t, tblInfo.ID, physicalTableID)
			require.Equal(t, cfg.maxTopNKeysPerPhysical, maxTopNKeys)
			require.Same(t, tblInfo.Columns[1], colInfo)
			return &statshandle.ColumnDistributionStats{
				Column: loadedStats.GetCol(tblInfo.Columns[1].ID),
			}, nil
		}

		keys, _, err := planAutoPreSplitIndexRegions(
			planCtx, sctx, provider, tblInfo, idxInfo, cfg)
		require.NoError(t, err)
		require.Equal(t, []string{"10", "40", "60", "90"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
	})

	t.Run("statistics load timeout skips AUTO", func(t *testing.T) {
		timeoutCfg := cfg
		timeoutCfg.statsLoadTimeout = time.Millisecond
		statsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus =
			statistics.NewStatsAllEvictedStatus()
		provider := &fakeAutoPreSplitStatsProvider{stats: statsTbl}
		provider.loadColumnStats = func(
			loadCtx context.Context,
			_ sessionctx.Context,
			_ int64,
			_ *model.ColumnInfo,
			_ int,
		) (*statshandle.ColumnDistributionStats, error) {
			<-loadCtx.Done()
			return nil, loadCtx.Err()
		}

		keys, reason, err := planAutoPreSplitIndexRegions(
			context.Background(), sctx, provider, tblInfo, idxInfo, timeoutCfg)
		require.NoError(t, err)
		require.Empty(t, keys)
		require.Equal(t, "statistics loading timed out", reason)
	})

	defaultCfg := getAutoPreSplitConfig()
	require.Equal(t, int(vardef.MaxTiDBAnalyzeDefaultNumTopN), defaultCfg.maxTopNKeysPerPhysical)
	require.Equal(t, 30*time.Second, defaultCfg.statsLoadTimeout)

	t.Run("one event crossing multiple thresholds is emitted once", func(t *testing.T) {
		topN := buildAutoPreSplitTopN(
			t, sctx.GetSessionVars().StmtCtx.TimeZone(),
			[]int64{10}, []uint64{60})
		statsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)
		setAutoPreSplitTestHistogram(
			t, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
			types.MakeDatums(20, 40), []int64{20, 20})

		keys, _, err := planAutoPreSplitIndexRegions(
			context.Background(), sctx, &fakeAutoPreSplitStatsProvider{stats: statsTbl},
			tblInfo, idxInfo, cfg)
		require.NoError(t, err)
		require.Equal(t, []string{"10", "20"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
	})

	t.Run("equal TopN and Histogram values are merged", func(t *testing.T) {
		topN := buildAutoPreSplitTopN(
			t, sctx.GetSessionVars().StmtCtx.TimeZone(),
			[]int64{10}, []uint64{15})
		statsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 40, 0, tblInfo.Columns[1], topN)
		setAutoPreSplitTestHistogram(
			t, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
			types.MakeDatums(10, 20), []int64{5, 20})

		equalCfg := cfg
		equalCfg.boundaryRatioStep = 0.5
		keys, _, err := planAutoPreSplitIndexRegions(
			context.Background(), sctx, &fakeAutoPreSplitStatsProvider{stats: statsTbl},
			tblInfo, idxInfo, equalCfg)
		require.NoError(t, err)
		require.Equal(t, []string{"10"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
	})

	for _, tc := range []struct {
		name              string
		boundaryRatioStep float64
		expected          int
	}{
		{name: "0.01", boundaryRatioStep: 0.01, expected: 99},
		{name: "0.02", boundaryRatioStep: 0.02, expected: 49},
		{name: "0.05", boundaryRatioStep: 0.05, expected: 19},
		{name: "1", boundaryRatioStep: 1, expected: 0},
	} {
		t.Run("boundary ratio step "+tc.name, func(t *testing.T) {
			events := make([]autoPreSplitEvent, 0, 100)
			for value := int64(1); value <= 100; value++ {
				event, err := newAutoPreSplitEvent(
					sctx, types.NewIntDatum(value), 1, tblInfo.Columns[1])
				require.NoError(t, err)
				events = append(events, event)
			}
			rows := sampleAutoPreSplitEvents(events, 100, tc.boundaryRatioStep)
			require.Len(t, rows, tc.expected)
		})
	}

	t.Run("component failures are independent", func(t *testing.T) {
		componentCfg := cfg
		componentCfg.boundaryRatioStep = 0.5
		for _, tc := range []struct {
			name               string
			topNError          error
			histogramError     error
			nullCountError     error
			expectedBoundaries []string
		}{
			{
				name:               "TopN unavailable",
				topNError:          errors.New("TopN unavailable"),
				expectedBoundaries: []string{"20"},
			},
			{
				name:               "Histogram unavailable",
				histogramError:     errors.New("Histogram unavailable"),
				expectedBoundaries: []string{"10"},
			},
			{
				name:               "NullCount unavailable",
				nullCountError:     errors.New("NullCount unavailable"),
				expectedBoundaries: []string{"20"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				topN := buildAutoPreSplitTopN(
					t, sctx.GetSessionVars().StmtCtx.TimeZone(),
					[]int64{10, 30}, []uint64{50, 50})
				loadedStats := buildAutoPreSplitTestStats(
					tblInfo.ID, 250, 0, tblInfo.Columns[1], topN)
				setAutoPreSplitTestHistogram(
					t, loadedStats.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 50,
					types.MakeDatums(20, 40), []int64{50, 50})
				cachedStats := buildAutoPreSplitTestStats(
					tblInfo.ID, 250, 0, tblInfo.Columns[1], nil)
				cachedStats.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus =
					statistics.NewStatsAllEvictedStatus()
				provider := &fakeAutoPreSplitStatsProvider{stats: cachedStats}
				provider.loadColumnStats = func(
					context.Context,
					sessionctx.Context,
					int64,
					*model.ColumnInfo,
					int,
				) (*statshandle.ColumnDistributionStats, error) {
					return &statshandle.ColumnDistributionStats{
						Column:         loadedStats.GetCol(tblInfo.Columns[1].ID),
						TopNError:      tc.topNError,
						HistogramError: tc.histogramError,
						NullCountError: tc.nullCountError,
					}, nil
				}

				keys, _, err := planAutoPreSplitIndexRegions(
					context.Background(), sctx, provider, tblInfo, idxInfo, componentCfg)
				require.NoError(t, err)
				require.Equal(
					t, tc.expectedBoundaries,
					splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
			})
		}
	})

	for _, collation := range []string{"utf8mb4_general_ci", "utf8mb4_bin"} {
		t.Run(collation, func(t *testing.T) {
			collationCfg := cfg
			collationCfg.boundaryRatioStep = 0.4
			stringTblInfo, stringIdxInfo := buildAutoPreSplitTestTableInfoFromSQL(t,
				"create table t(a bigint, b varchar(32) collate "+collation+", index idx(b))")
			values := []string{"A", "B"}
			topN := buildAutoPreSplitTopN(
				t, sctx.GetSessionVars().StmtCtx.TimeZone(), values, []uint64{50, 40},
				&stringTblInfo.Columns[1].FieldType)
			statsTbl := buildAutoPreSplitTestStats(
				stringTblInfo.ID, 90, 0, stringTblInfo.Columns[1], topN)

			events, err := buildAutoPreSplitTopNEvents(
				sctx, topN, stringTblInfo.Columns[1], cfg.maxTopNKeysPerPhysical)
			require.NoError(t, err)
			require.Equal(t, types.KindBytes, events[0].value.Kind())

			keys, _, err := planAutoPreSplitIndexRegions(
				context.Background(), sctx, &fakeAutoPreSplitStatsProvider{stats: statsTbl},
				stringTblInfo, stringIdxInfo, collationCfg)
			require.NoError(t, err)

			expectedRows := make([][]types.Datum, 0, len(values))
			for _, value := range values {
				expectedRows = append(expectedRows, []types.Datum{
					types.NewCollationStringDatum(value, collation),
				})
			}
			expectedKeys, err := getSplitIdxKeysFromValueList(
				sctx, stringTblInfo, stringIdxInfo, expectedRows)
			require.NoError(t, err)
			require.Equal(t, sortAndDedupeAutoPreSplitKeys(expectedKeys), keys)
		})
	}

	t.Run("V1 statistics are skipped", func(t *testing.T) {
		statsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsVer = statistics.Version1
		keys, reason, err := planAutoPreSplitIndexRegions(
			context.Background(), sctx, &fakeAutoPreSplitStatsProvider{stats: statsTbl},
			tblInfo, idxInfo, cfg)
		require.NoError(t, err)
		require.Empty(t, keys)
		require.Equal(t, "leading column stats version 1 is not Analyze V2", reason)
	})
}

func TestPlanAutoPreSplitIndexRegionsSkipUnreliableStats(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoPreSplitTestTableInfoFromSQL(t, autoPreSplitTestTableSQL)
	cfg := newAutoPreSplitTestConfig()
	pseudoStats := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
	pseudoStats.Pseudo = true
	partitionTblInfo, partitionIdxInfo := buildAutoPreSplitTestTableInfoFromSQL(t,
		"create table t(a bigint, b bigint, index idx(b)) partition by hash(a) partitions 2")
	partialTblInfo, partialIdxInfo := buildAutoPreSplitTestTableInfoFromSQL(t,
		"create table t(a bigint, b bigint, index idx(b) where a = 1)")
	prefixTblInfo, prefixIdxInfo := buildAutoPreSplitTestTableInfoFromSQL(t,
		"create table t(a bigint, b varchar(32) collate utf8mb4_general_ci, index idx(b(3)))")
	prefixStats := buildAutoPreSplitTestStats(prefixTblInfo.ID, 100, 0, prefixTblInfo.Columns[1], nil)
	outdatedStats := buildAutoPreSplitTestStats(tblInfo.ID, 100, 80, tblInfo.Columns[1], nil)
	setAutoPreSplitTestHistogram(
		t, outdatedStats.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
		types.MakeDatums(100), []int64{100})
	smallStats := buildAutoPreSplitTestStats(tblInfo.ID, 5, 0, tblInfo.Columns[1], nil)

	for _, tc := range []struct {
		provider autoPreSplitStatsProvider
		tblInfo  *model.TableInfo
		idxInfo  *model.IndexInfo
		reason   string
	}{
		{&fakeAutoPreSplitStatsProvider{}, tblInfo, idxInfo, "stats missing"},
		{&fakeAutoPreSplitStatsProvider{stats: pseudoStats}, tblInfo, idxInfo, "stats pseudo"},
		{&fakeAutoPreSplitStatsProvider{stats: outdatedStats}, tblInfo, idxInfo, "stats outdated"},
		{&fakeAutoPreSplitStatsProvider{stats: smallStats}, tblInfo, idxInfo, "row count 5 below threshold 10"},
		{nil, partitionTblInfo, partitionIdxInfo, "partitioned table"},
		{nil, partialTblInfo, partialIdxInfo, "partial index"},
		{&fakeAutoPreSplitStatsProvider{stats: prefixStats}, prefixTblInfo, prefixIdxInfo, "leading string column uses prefix index"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			keys, reason, err := planAutoPreSplitIndexRegions(
				context.Background(), sctx, tc.provider, tc.tblInfo, tc.idxInfo, cfg)
			require.NoError(t, err)
			require.Empty(t, keys)
			require.Equal(t, tc.reason, reason)
		})
	}
}

func TestAutoPreSplitIndexRegionsGateAndManualOverride(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoPreSplitTestTableInfoFromSQL(t, autoPreSplitTestTableSQL)
	statsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
	statsProvider := &fakeAutoPreSplitStatsProvider{stats: statsTbl}
	reorgMeta := &model.DDLReorgMeta{}
	args := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{}}}
	autoArgs := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{
		AutoPreSplit: true,
	}}}

	var capturedKeys [][]byte
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex", func(splitKeys [][]byte) {
		capturedKeys = append(capturedKeys, splitKeys...)
	})
	runAutoPreSplit := func(ctx context.Context, store kv.Storage, statsProvider autoPreSplitStatsProvider) error {
		capturedKeys = nil
		if ctx == nil {
			ctx = context.Background()
		}
		return preSplitIndexRegions(
			ctx, sctx, store, tblInfo, []*model.IndexInfo{idxInfo}, reorgMeta,
			autoArgs, statsProvider)
	}

	statsAccessed := false
	noAutoProvider := &fakeAutoPreSplitStatsProvider{
		stats: statsTbl,
		getPhysicalTableStats: func() {
			statsAccessed = true
		},
	}
	err := preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, noAutoProvider)
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
	require.False(t, statsAccessed)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAutoPresplitConfig", "return(25)")
	badTopN := statistics.NewTopN(1)
	badTopN.AppendTopN([]byte{0xff}, 50)
	badStatsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], badTopN)
	err = runAutoPreSplit(nil, nil, &fakeAutoPreSplitStatsProvider{stats: badStatsTbl})
	require.NoError(t, err)
	require.Empty(t, capturedKeys)

	hotTopN := buildAutoPreSplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), []int64{20, 30}, []uint64{50, 40})
	hotStatsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], hotTopN)
	setAutoPreSplitTestHistogram(
		t, hotStatsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
		types.MakeDatums(40), []int64{10})
	hotStatsProvider := &fakeAutoPreSplitStatsProvider{stats: hotStatsTbl}
	t.Run("reuse boundaries for indexes sharing leading column", func(t *testing.T) {
		tblInfo, _ := buildAutoPreSplitTestTableInfoFromSQL(
			t, "create table t(a bigint, b bigint, index idx1(b), index idx2(b, a))")
		loadedStats := buildAutoPreSplitTestStats(
			tblInfo.ID, 100, 0, tblInfo.Columns[1], hotTopN)
		setAutoPreSplitTestHistogram(
			t, loadedStats.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
			types.MakeDatums(40), []int64{10})
		evictedStats := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		evictedStats.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus =
			statistics.NewStatsAllEvictedStatus()

		loadCount := 0
		provider := &fakeAutoPreSplitStatsProvider{stats: evictedStats}
		provider.loadColumnStats = func(
			context.Context,
			sessionctx.Context,
			int64,
			*model.ColumnInfo,
			int,
		) (*statshandle.ColumnDistributionStats, error) {
			loadCount++
			return &statshandle.ColumnDistributionStats{
				Column: loadedStats.GetCol(tblInfo.Columns[1].ID),
			}, nil
		}
		args := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{
			{AutoPreSplit: true},
			{AutoPreSplit: true},
		}}
		run := func() {
			capturedKeys = nil
			err := preSplitIndexRegions(
				context.Background(), sctx, nil, tblInfo, tblInfo.Indices,
				reorgMeta, args, provider)
			require.NoError(t, err)
			for _, idxInfo := range tblInfo.Indices {
				require.Equal(t, []string{"20", "30", "40"},
					splitKeyFirstValuesForIndex(t, capturedKeys, idxInfo.ID))
			}
		}

		run()
		require.Equal(t, 1, loadCount)
		run()
		require.Equal(t, 2, loadCount)
	})
	for _, tc := range []struct {
		name          string
		store         kv.Storage
		result        splitIndexRegionResult
		skipReason    string
		wantResultErr bool
	}{
		{
			name:   "split",
			store:  &fakeAutoPreSplitStore{regionIDs: []uint64{1, 2, 3}},
			result: splitIndexRegionResult{splitRegions: 3, scatterRegions: 3},
		},
		{name: "failed", store: &fakeAutoPreSplitStore{regionIDs: []uint64{1}, splitErr: context.DeadlineExceeded}, wantResultErr: true},
		{name: "unsupported", skipReason: "unsupported storage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, skipReason, resultErr := autoPreSplitIndexRegion(
				context.Background(), sctx, tc.store, tblInfo, idxInfo, hotStatsProvider,
				make(map[int64][][]types.Datum), false)
			if tc.wantResultErr {
				require.Error(t, resultErr)
			} else {
				require.NoError(t, resultErr)
			}
			require.Equal(t, tc.result, result)
			require.Equal(t, tc.skipReason, skipReason)

			err := runAutoPreSplit(nil, tc.store, hotStatsProvider)
			require.NoError(t, err)
			require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
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
			store := &fakeAutoPreSplitStore{
				splitFunc: func(splitCtx context.Context) ([]uint64, error) {
					cancel(tc.cause)
					<-splitCtx.Done()
					return nil, splitCtx.Err()
				},
			}

			err := runAutoPreSplit(ctx, store, hotStatsProvider)
			require.Same(t, tc.cause, err)
		})
	}

	t.Run("TopN load cancellation", func(t *testing.T) {
		statsTbl := buildAutoPreSplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
		ctx, cancel := context.WithCancelCause(context.Background())
		pauseErr := dbterror.ErrPausedDDLJob.FastGenByArgs(2)
		provider := &fakeAutoPreSplitStatsProvider{stats: statsTbl}
		provider.loadColumnStats = func(
			loadCtx context.Context,
			_ sessionctx.Context,
			_ int64,
			_ *model.ColumnInfo,
			_ int,
		) (*statshandle.ColumnDistributionStats, error) {
			cancel(pauseErr)
			<-loadCtx.Done()
			return nil, loadCtx.Err()
		}

		err := runAutoPreSplit(ctx, nil, provider)
		require.True(t, dbterror.ErrPausedDDLJob.Equal(err))
	})

	manualArgs := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{
		SplitOpt: &model.IndexArgSplitOpt{Num: 4},
	}}}
	splitOpt, autoPreSplit, err := buildIndexPreSplitOpt(&ast.IndexOption{
		SplitOpt:     &ast.SplitOption{Num: 4},
		AutoPreSplit: true,
	})
	require.NoError(t, err)
	require.NotNil(t, splitOpt)
	require.False(t, autoPreSplit)
	capturedKeys = nil
	err = preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, manualArgs, hotStatsProvider)
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
	capturedKeys = nil
	err = preSplitIndexRegions(
		context.Background(), sctx, &fakeAutoPreSplitStore{regionIDs: []uint64{1, 2, 3}},
		tblInfo, []*model.IndexInfo{idxInfo}, reorgMeta, manualArgs, hotStatsProvider)
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
	t.Run("manual split failure logs index name", func(t *testing.T) {
		core, observedLogs := observer.New(zap.ErrorLevel)
		restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{})
		defer restoreLog()

		capturedKeys = nil
		err := preSplitIndexRegions(
			context.Background(), sctx, &fakeAutoPreSplitStore{splitErr: context.DeadlineExceeded},
			tblInfo, []*model.IndexInfo{idxInfo}, reorgMeta, manualArgs, hotStatsProvider)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		failureLogs := observedLogs.FilterMessage("split table index region failed").All()
		require.Len(t, failureLogs, 1)
		fields := failureLogs[0].ContextMap()
		require.Equal(t, tblInfo.Name.L, fields["table"])
		require.Equal(t, idxInfo.Name.L, fields["index"])
	})

	err = runAutoPreSplit(nil, nil, statsProvider)
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
}

func buildAutoPreSplitTestTableInfoFromSQL(t *testing.T, createSQL string) (*model.TableInfo, *model.IndexInfo) {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	tblInfo, err := BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
	tblInfo.ID = 100
	return tblInfo, tblInfo.Indices[0]
}

func buildAutoPreSplitTestStats(
	physicalID int64,
	rowCount int64,
	modifyCount int64,
	colInfo *model.ColumnInfo,
	topN *statistics.TopN,
) *statistics.Table {
	histColl := statistics.NewHistColl(physicalID, rowCount, modifyCount, 1, 0)
	histogram := statistics.NewHistogram(colInfo.ID, 0, 0, 1, &colInfo.FieldType, 0, 0)
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

func setAutoPreSplitTestHistogram(
	t *testing.T,
	colStats *statistics.Column,
	colInfo *model.ColumnInfo,
	nullCount int64,
	upperBounds []types.Datum,
	bucketDeltas []int64,
) {
	t.Helper()
	require.Len(t, bucketDeltas, len(upperBounds))
	histogram := statistics.NewHistogram(
		colInfo.ID, int64(len(upperBounds)), nullCount, 1,
		&colInfo.FieldType, len(upperBounds), 0)
	var cumulative int64
	for i := range upperBounds {
		require.GreaterOrEqual(t, bucketDeltas[i], int64(0))
		cumulative += bucketDeltas[i]
		lower := upperBounds[i]
		upper := upperBounds[i]
		histogram.AppendBucket(&lower, &upper, cumulative, bucketDeltas[i])
	}
	colStats.Histogram = *histogram
}

func buildAutoPreSplitTopN[T int64 | string](
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
