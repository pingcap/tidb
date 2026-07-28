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
	"math/big"
	"testing"
	"time"

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
)

const autoPresplitTestTableSQL = "create table t(a bigint, b bigint, index idx(b))"

type fakeAutoPresplitStatsProvider struct {
	stats           *statistics.Table
	statsAccessed   *bool
	loadColumnStats func(context.Context, sessionctx.Context, int64, int64, *model.ColumnInfo, int) (*statshandle.AutoPresplitColumnStats, error)
}

func (p *fakeAutoPresplitStatsProvider) GetPhysicalTableStats(int64, *model.TableInfo) *statistics.Table {
	if p.statsAccessed != nil {
		*p.statsAccessed = true
	}
	return p.stats
}

func (p *fakeAutoPresplitStatsProvider) LoadColumnStatsForAutoPresplit(
	ctx context.Context,
	sctx sessionctx.Context,
	physicalTableID, columnID int64,
	colInfo *model.ColumnInfo,
	limit int,
) (*statshandle.AutoPresplitColumnStats, error) {
	if p.loadColumnStats == nil {
		return nil, errors.New("unexpected column stats load")
	}
	return p.loadColumnStats(ctx, sctx, physicalTableID, columnID, colInfo, limit)
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
		maxTopNKeysPerPhysical: 100_000,
		minStatsHealthy:        80,
	}
}

func TestPlanAutoPresplitIndexRegionsTopN(t *testing.T) {
	sctx := mock.NewContext()
	tblInfo, idxInfo := buildAutoPresplitTestTableInfoFromSQL(t, autoPresplitTestTableSQL)
	cfg := newAutoPresplitTestConfig()

	topN := buildAutoPresplitTopN(
		t, sctx.GetSessionVars().StmtCtx.TimeZone(),
		[]int64{10, 50, 90}, []uint64{25, 14, 11})
	statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)
	setAutoPresplitTestHistogram(
		t, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
		[]int64{20, 40, 60, 80, 100}, []int64{10, 10, 10, 10, 10})
	keys, _, err := planAutoPresplitIndexRegions(
		context.Background(), sctx, &fakeAutoPresplitStatsProvider{stats: statsTbl},
		tblInfo, idxInfo, cfg, "1/5")
	require.NoError(t, err)
	require.Equal(t, []string{"10", "40", "60", "90"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))

	t.Run("bounded cached TopN", func(t *testing.T) {
		const topNSize = 1000
		values := make([]int64, topNSize)
		counts := make([]uint64, topNSize)
		for i := range topNSize {
			values[i] = int64(i + 1)
			counts[i] = uint64(i + 1)
		}
		topN := buildAutoPresplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), values, counts)
		events, err := buildAutoPresplitTopNEvents(sctx, topN, tblInfo.Columns[1], 5)
		require.NoError(t, err)
		require.Len(t, events, 5)
		for i, event := range events {
			require.Equal(t, int64(i+1), event.value.GetInt64())
		}
	})

	t.Run("evicted stats use one provider result", func(t *testing.T) {
		loadedTopN := buildAutoPresplitTopN(
			t, sctx.GetSessionVars().StmtCtx.TimeZone(),
			[]int64{10, 50, 90}, []uint64{25, 14, 11})
		loadedStats := buildAutoPresplitTestStats(
			tblInfo.ID, 100, 0, tblInfo.Columns[1], loadedTopN)
		setAutoPresplitTestHistogram(
			t, loadedStats.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
			[]int64{20, 40, 60, 80, 100}, []int64{10, 10, 10, 10, 10})
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
		planCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		provider := &fakeAutoPresplitStatsProvider{stats: statsTbl}
		provider.loadColumnStats = func(
			loadCtx context.Context,
			_ sessionctx.Context,
			physicalTableID, columnID int64,
			colInfo *model.ColumnInfo,
			limit int,
		) (*statshandle.AutoPresplitColumnStats, error) {
			require.Equal(t, planCtx, loadCtx)
			require.Equal(t, tblInfo.ID, physicalTableID)
			require.Equal(t, tblInfo.Columns[1].ID, columnID)
			require.Equal(t, cfg.maxTopNKeysPerPhysical, limit)
			require.Same(t, tblInfo.Columns[1], colInfo)
			return &statshandle.AutoPresplitColumnStats{
				Column: loadedStats.GetCol(tblInfo.Columns[1].ID),
			}, nil
		}

		keys, _, err := planAutoPresplitIndexRegions(
			planCtx, sctx, provider, tblInfo, idxInfo, cfg, "1/5")
		require.NoError(t, err)
		require.Equal(t, []string{"10", "40", "60", "90"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
	})

	defaultCfg := getAutoPresplitConfig()
	require.Equal(t, int(vardef.MaxTiDBAnalyzeDefaultNumTopN), defaultCfg.maxTopNKeysPerPhysical)

	t.Run("one event crossing multiple thresholds is emitted once", func(t *testing.T) {
		topN := buildAutoPresplitTopN(
			t, sctx.GetSessionVars().StmtCtx.TimeZone(),
			[]int64{10}, []uint64{60})
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], topN)
		setAutoPresplitTestHistogram(
			t, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
			[]int64{20, 40}, []int64{20, 20})

		keys, _, err := planAutoPresplitIndexRegions(
			context.Background(), sctx, &fakeAutoPresplitStatsProvider{stats: statsTbl},
			tblInfo, idxInfo, cfg, "1/5")
		require.NoError(t, err)
		require.Equal(t, []string{"10", "20"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
	})

	t.Run("equal TopN and Histogram values are merged", func(t *testing.T) {
		topN := buildAutoPresplitTopN(
			t, sctx.GetSessionVars().StmtCtx.TimeZone(),
			[]int64{10}, []uint64{15})
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 40, 0, tblInfo.Columns[1], topN)
		setAutoPresplitTestHistogram(
			t, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
			[]int64{10, 20}, []int64{5, 20})

		keys, _, err := planAutoPresplitIndexRegions(
			context.Background(), sctx, &fakeAutoPresplitStatsProvider{stats: statsTbl},
			tblInfo, idxInfo, cfg, "1/2")
		require.NoError(t, err)
		require.Equal(t, []string{"10"}, splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
	})

	t.Run("NULL participates as the first event", func(t *testing.T) {
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		setAutoPresplitTestHistogram(
			t, statsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 25,
			[]int64{20, 40, 60}, []int64{25, 25, 25})
		colStats := statsTbl.GetCol(tblInfo.Columns[1].ID)
		nullEvent, err := newAutoPresplitEvent(
			sctx, types.NewDatum(nil), uint64(colStats.NullCount), tblInfo.Columns[1])
		require.NoError(t, err)
		histogramEvents, err := buildAutoPresplitHistogramEvents(
			sctx, &colStats.Histogram, tblInfo.Columns[1])
		require.NoError(t, err)
		events, total, err := mergeAutoPresplitEvents(
			append([]autoPresplitEvent{nullEvent}, histogramEvents...))
		require.NoError(t, err)
		rows := sampleAutoPresplitEvents(events, total, mustParseAutoPresplitInterval(t, "1/5"))
		require.NotEmpty(t, rows)
		require.True(t, rows[0][0].IsNull())
	})

	for _, tc := range []struct {
		interval string
		expected int
	}{
		{interval: "0.01", expected: 99},
		{interval: "0.02", expected: 49},
		{interval: "0.05", expected: 19},
		{interval: "1", expected: 0},
	} {
		t.Run("interval "+tc.interval, func(t *testing.T) {
			events := make([]autoPresplitEvent, 0, 100)
			for value := int64(1); value <= 100; value++ {
				event, err := newAutoPresplitEvent(
					sctx, types.NewIntDatum(value), 1, tblInfo.Columns[1])
				require.NoError(t, err)
				events = append(events, event)
			}
			rows := sampleAutoPresplitEvents(
				events, 100, mustParseAutoPresplitInterval(t, tc.interval))
			require.Len(t, rows, tc.expected)
		})
	}

	t.Run("component failures are independent", func(t *testing.T) {
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
				topN := buildAutoPresplitTopN(
					t, sctx.GetSessionVars().StmtCtx.TimeZone(),
					[]int64{10, 30}, []uint64{50, 50})
				loadedStats := buildAutoPresplitTestStats(
					tblInfo.ID, 250, 0, tblInfo.Columns[1], topN)
				setAutoPresplitTestHistogram(
					t, loadedStats.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 50,
					[]int64{20, 40}, []int64{50, 50})
				cachedStats := buildAutoPresplitTestStats(
					tblInfo.ID, 250, 0, tblInfo.Columns[1], nil)
				cachedStats.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus =
					statistics.NewStatsAllEvictedStatus()
				provider := &fakeAutoPresplitStatsProvider{stats: cachedStats}
				provider.loadColumnStats = func(
					context.Context,
					sessionctx.Context,
					int64, int64,
					*model.ColumnInfo,
					int,
				) (*statshandle.AutoPresplitColumnStats, error) {
					return &statshandle.AutoPresplitColumnStats{
						Column:         loadedStats.GetCol(tblInfo.Columns[1].ID),
						TopNError:      tc.topNError,
						HistogramError: tc.histogramError,
						NullCountError: tc.nullCountError,
					}, nil
				}

				keys, _, err := planAutoPresplitIndexRegions(
					context.Background(), sctx, provider, tblInfo, idxInfo, cfg, "1/2")
				require.NoError(t, err)
				require.Equal(
					t, tc.expectedBoundaries,
					splitKeyFirstValuesForIndex(t, keys, idxInfo.ID))
			})
		}
	})

	for _, collation := range []string{"utf8mb4_general_ci", "utf8mb4_bin"} {
		t.Run(collation, func(t *testing.T) {
			stringTblInfo, stringIdxInfo := buildAutoPresplitTestTableInfoFromSQL(t,
				"create table t(a bigint, b varchar(32) collate "+collation+", index idx(b))")
			values := []string{"A", "B"}
			topN := buildAutoPresplitTopN(
				t, sctx.GetSessionVars().StmtCtx.TimeZone(), values, []uint64{50, 40},
				&stringTblInfo.Columns[1].FieldType)
			statsTbl := buildAutoPresplitTestStats(
				stringTblInfo.ID, 90, 0, stringTblInfo.Columns[1], topN)

			events, err := buildAutoPresplitTopNEvents(
				sctx, topN, stringTblInfo.Columns[1], cfg.maxTopNKeysPerPhysical)
			require.NoError(t, err)
			require.Equal(t, types.KindBytes, events[0].value.Kind())

			keys, _, err := planAutoPresplitIndexRegions(
				context.Background(), sctx, &fakeAutoPresplitStatsProvider{stats: statsTbl},
				stringTblInfo, stringIdxInfo, cfg, "2/5")
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
			require.Equal(t, sortAndDedupeAutoPresplitKeys(expectedKeys), keys)
		})
	}

	t.Run("V1 statistics are skipped", func(t *testing.T) {
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsVer = statistics.Version1
		keys, reason, err := planAutoPresplitIndexRegions(
			context.Background(), sctx, &fakeAutoPresplitStatsProvider{stats: statsTbl},
			tblInfo, idxInfo, cfg, "1/5")
		require.NoError(t, err)
		require.Empty(t, keys)
		require.Equal(t, "leading column stats version 1 is not Analyze V2", reason)
	})

	t.Run("invalid internal interval is rejected", func(t *testing.T) {
		for _, value := range []string{"", "-0.01", "1.01", "nan"} {
			_, err := parseAutoPresplitInterval(value)
			require.Error(t, err)
		}
	})
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
	setAutoPresplitTestHistogram(
		t, outdatedStats.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
		[]int64{100}, []int64{100})
	smallStats := buildAutoPresplitTestStats(tblInfo.ID, 5, 0, tblInfo.Columns[1], nil)

	for _, tc := range []struct {
		provider autoPresplitStatsProvider
		tblInfo  *model.TableInfo
		idxInfo  *model.IndexInfo
		interval string
		reason   string
	}{
		{nil, tblInfo, idxInfo, "0", "auto presplit interval is zero"},
		{&fakeAutoPresplitStatsProvider{}, tblInfo, idxInfo, "1/5", "stats missing"},
		{&fakeAutoPresplitStatsProvider{stats: pseudoStats}, tblInfo, idxInfo, "1/5", "stats pseudo"},
		{&fakeAutoPresplitStatsProvider{stats: outdatedStats}, tblInfo, idxInfo, "1/5", "stats outdated"},
		{&fakeAutoPresplitStatsProvider{stats: smallStats}, tblInfo, idxInfo, "1/5", "row count 5 below threshold 10"},
		{nil, partitionTblInfo, partitionIdxInfo, "1/5", "partitioned table"},
		{nil, partialTblInfo, partialIdxInfo, "1/5", "partial index"},
		{&fakeAutoPresplitStatsProvider{stats: prefixStats}, prefixTblInfo, prefixIdxInfo, "1/5", "leading string column uses prefix index"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			keys, reason, err := planAutoPresplitIndexRegions(
				context.Background(), sctx, tc.provider, tc.tblInfo, tc.idxInfo, cfg, tc.interval)
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
	autoArgs := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{AutoPresplit: true}}}

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
			ctx, sctx, store, tblInfo, []*model.IndexInfo{idxInfo}, reorgMeta,
			autoArgs, statsProvider, "1/2")
	}

	statsAccessed := false
	noAutoProvider := &fakeAutoPresplitStatsProvider{
		stats:         statsTbl,
		statsAccessed: &statsAccessed,
	}
	err := preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, args, noAutoProvider, "1/2")
	require.NoError(t, err)
	require.Empty(t, capturedKeys)
	require.False(t, statsAccessed)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAutoPresplitConfig", "return(25)")
	badTopN := statistics.NewTopN(1)
	badTopN.AppendTopN([]byte{0xff}, 50)
	badStatsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], badTopN)
	err = runAutoPresplit(nil, nil, &fakeAutoPresplitStatsProvider{stats: badStatsTbl})
	require.NoError(t, err)
	require.Empty(t, capturedKeys)

	hotTopN := buildAutoPresplitTopN(t, sctx.GetSessionVars().StmtCtx.TimeZone(), []int64{20, 30}, []uint64{50, 40})
	hotStatsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], hotTopN)
	setAutoPresplitTestHistogram(
		t, hotStatsTbl.GetCol(tblInfo.Columns[1].ID), tblInfo.Columns[1], 0,
		[]int64{40}, []int64{10})
	hotStatsProvider := &fakeAutoPresplitStatsProvider{stats: hotStatsTbl}
	for _, tc := range []struct {
		name  string
		store kv.Storage
	}{
		{name: "split", store: &fakeAutoPresplitStore{regionIDs: []uint64{1, 2, 3}}},
		{name: "failed", store: &fakeAutoPresplitStore{regionIDs: []uint64{1}, splitErr: context.DeadlineExceeded}},
		{name: "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runAutoPresplit(nil, tc.store, hotStatsProvider)
			require.NoError(t, err)
			require.Equal(t, 1, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))
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
		})
	}

	t.Run("TopN load cancellation", func(t *testing.T) {
		statsTbl := buildAutoPresplitTestStats(tblInfo.ID, 100, 0, tblInfo.Columns[1], nil)
		statsTbl.GetCol(tblInfo.Columns[1].ID).StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
		ctx, cancel := context.WithCancelCause(context.Background())
		pauseErr := dbterror.ErrPausedDDLJob.FastGenByArgs(2)
		provider := &fakeAutoPresplitStatsProvider{stats: statsTbl}
		provider.loadColumnStats = func(
			loadCtx context.Context,
			_ sessionctx.Context,
			_, _ int64,
			_ *model.ColumnInfo,
			_ int,
		) (*statshandle.AutoPresplitColumnStats, error) {
			cancel(pauseErr)
			<-loadCtx.Done()
			return nil, loadCtx.Err()
		}

		err := runAutoPresplit(ctx, nil, provider)
		require.True(t, dbterror.ErrPausedDDLJob.Equal(err))
	})

	manualArgs := &model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{
		SplitOpt: &model.IndexArgSplitOpt{Num: 4},
	}}}
	capturedKeys = nil
	err = preSplitIndexRegions(
		context.Background(), sctx, nil, tblInfo, []*model.IndexInfo{idxInfo},
		reorgMeta, manualArgs, hotStatsProvider, "1/2")
	require.NoError(t, err)
	require.Equal(t, 3, countSplitKeysForIndex(t, capturedKeys, idxInfo.ID))

	err = runAutoPresplit(nil, nil, statsProvider)
	require.NoError(t, err)
	require.Empty(t, capturedKeys)

	t.Run("fixed interval persists and ignores old job values", func(t *testing.T) {
		job := &model.Job{}
		persistAutoPresplitInterval(job)
		persisted, ok := job.GetSystemVars(autoPresplitIntervalJobKey)
		require.True(t, ok)
		require.Equal(t, fixedAutoPresplitInterval, persisted)

		for _, oldValue := range []string{"0", "0.0001", "0.5"} {
			oldJob := &model.Job{SessionVars: map[string]string{
				autoPresplitIntervalJobKey: oldValue,
			}}
			require.Equal(t, fixedAutoPresplitInterval, autoPresplitIntervalForJob(oldJob))
		}
	})
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

func setAutoPresplitTestHistogram(
	t *testing.T,
	colStats *statistics.Column,
	colInfo *model.ColumnInfo,
	nullCount int64,
	upperBounds []int64,
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
		lower := types.NewIntDatum(upperBounds[i])
		upper := lower
		histogram.AppendBucket(&lower, &upper, cumulative, bucketDeltas[i])
	}
	colStats.Histogram = *histogram
}

func mustParseAutoPresplitInterval(t *testing.T, value string) *big.Rat {
	t.Helper()
	interval, err := parseAutoPresplitInterval(value)
	require.NoError(t, err)
	return interval
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
