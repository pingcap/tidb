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
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
)

type autoPreSplitStatsProvider interface {
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
	LoadColumnDistributionStats(
		ctx context.Context,
		sctx sessionctx.Context,
		physicalTableID int64,
		colInfo *model.ColumnInfo,
		maxTopNKeys int,
	) (*handle.ColumnDistributionStats, error)
}

type autoPreSplitConfig struct {
	minTableRows           int64
	maxTopNKeysPerPhysical int
	statsLoadTimeout       time.Duration
	minStatsHealthy        int64
	boundaryRatioStep      float64
}

func getAutoPreSplitConfig() autoPreSplitConfig {
	cfg := autoPreSplitConfig{
		minTableRows:           1_000_000,
		maxTopNKeysPerPhysical: int(vardef.MaxTiDBAnalyzeDefaultNumTopN),
		statsLoadTimeout:       30 * time.Second,
		minStatsHealthy:        80,
		boundaryRatioStep:      0.02,
	}
	failpoint.Inject("mockAutoPresplitConfig", func(val failpoint.Value) {
		if minRows, ok := val.(int); ok && minRows > 0 {
			cfg.applyTestConfigOverrides(minRows)
		}
	})
	return cfg
}

func (cfg *autoPreSplitConfig) applyTestConfigOverrides(minRows int) {
	cfg.minTableRows = int64(minRows)
	cfg.minStatsHealthy = 0
}

func planAutoPreSplitIndexRegions(
	ctx context.Context,
	sctx sessionctx.Context,
	statsProvider autoPreSplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	cfg autoPreSplitConfig,
) ([][]byte, string, error) {
	return planAutoPreSplitWithCache(
		ctx, sctx, statsProvider, tblInfo, idxInfo, cfg, nil)
}

func planAutoPreSplitWithCache(
	ctx context.Context,
	sctx sessionctx.Context,
	statsProvider autoPreSplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	cfg autoPreSplitConfig,
	boundaryCache map[int64][][]types.Datum,
) ([][]byte, string, error) {
	if tblInfo.GetPartitionInfo() != nil {
		return nil, "partitioned table", nil
	}
	// Ordinary column statistics describe the full table rather than the
	// predicate-filtered row set represented by a partial index.
	if idxInfo.HasCondition() {
		return nil, "partial index", nil
	}
	if statsProvider == nil {
		return nil, "stats handle is nil", nil
	}
	if len(idxInfo.Columns) == 0 {
		return nil, "index has no columns", nil
	}

	statsTbl := statsProvider.GetPhysicalTableStats(tblInfo.ID, tblInfo)
	if statsTbl == nil {
		return nil, "stats missing", nil
	}
	if statsTbl.Pseudo {
		return nil, "stats pseudo", nil
	}
	if statsTbl.IsOutdated() {
		return nil, "stats outdated", nil
	}
	healthy, ok := statsTbl.GetStatsHealthy()
	if !ok {
		return nil, "stats health unavailable", nil
	}
	if healthy < cfg.minStatsHealthy {
		return nil, fmt.Sprintf("stats health %d below threshold %d", healthy, cfg.minStatsHealthy), nil
	}
	if statsTbl.RealtimeCount < cfg.minTableRows {
		return nil, fmt.Sprintf("row count %d below threshold %d", statsTbl.RealtimeCount, cfg.minTableRows), nil
	}

	// Auto presplit intentionally uses only the leading index column. The available
	// per-column statistics cannot describe later-column distributions under a hot
	// leading-column value, and deriving such split keys would require reading table data.
	leadingIdxCol := idxInfo.Columns[0]
	offset := leadingIdxCol.Offset
	if offset < 0 || offset >= len(tblInfo.Columns) {
		return nil, "leading column not found", nil
	}
	leadingCol := tblInfo.Columns[offset]
	// A column TopN only keeps the full value's collation key, which cannot be
	// truncated by characters for a prefix index.
	if types.IsString(leadingCol.GetType()) && leadingIdxCol.Length != types.UnspecifiedLength {
		return nil, "leading string column uses prefix index", nil
	}
	// Reuse sampled boundaries for indexes sharing a leading column, avoiding duplicate
	// statistics loads within one add-index statement.
	if boundaryRows, ok := boundaryCache[leadingCol.ID]; ok {
		splitKeys, err := buildAutoPreSplitIndexKeys(sctx, tblInfo, idxInfo, boundaryRows)
		return splitKeys, "", err
	}
	colStats, loadNeeded, hasAnalyzed := statsTbl.ColumnIsLoadNeeded(leadingCol.ID, true)
	if !hasAnalyzed {
		return nil, "leading column stats missing or not analyzed", nil
	}

	loaded := &handle.ColumnDistributionStats{Column: colStats}
	if loadNeeded {
		loadCtx, cancel := context.WithTimeout(ctx, cfg.statsLoadTimeout)
		var err error
		loaded, err = statsProvider.LoadColumnDistributionStats(
			loadCtx, sctx, tblInfo.ID, leadingCol, cfg.maxTopNKeysPerPhysical)
		loadCause := context.Cause(loadCtx)
		cancel()
		if loadCause == context.DeadlineExceeded && context.Cause(ctx) == nil {
			return nil, "statistics loading timed out", nil
		}
		if err != nil {
			return nil, "", fmt.Errorf("failed to load leading column statistics from storage: %w", err)
		}
	}
	if loaded == nil || loaded.Column == nil {
		return nil, "leading column stats metadata missing", nil
	}
	if loaded.Column.StatsVer != statistics.Version2 {
		return nil, fmt.Sprintf("leading column stats version %d is not Analyze V2", loaded.Column.StatsVer), nil
	}

	// The configured TopN maximum equals Analyze's supported maximum, so all valid
	// TopN entries and Histogram buckets participate in boundary planning.
	events := make([]autoPreSplitEvent, 0, cfg.maxTopNKeysPerPhysical+loaded.Column.Histogram.Len()+1)
	if loaded.NullCountError != nil {
		logAutoPreSplitComponentFailure(tblInfo, idxInfo, "NullCount", loaded.NullCountError)
	} else if loaded.Column.NullCount > 0 {
		nullEvent, err := newAutoPreSplitEvent(
			sctx, types.NewDatum(nil), uint64(loaded.Column.NullCount), leadingCol)
		if err != nil {
			logAutoPreSplitComponentFailure(tblInfo, idxInfo, "NullCount", err)
		} else {
			events = append(events, nullEvent)
		}
	}

	if loaded.TopNError != nil {
		logAutoPreSplitComponentFailure(tblInfo, idxInfo, "TopN", loaded.TopNError)
	} else {
		topNEvents, err := buildAutoPreSplitTopNEvents(
			sctx, loaded.Column.TopN, leadingCol, cfg.maxTopNKeysPerPhysical)
		if err != nil {
			logAutoPreSplitComponentFailure(tblInfo, idxInfo, "TopN", err)
		} else {
			events = append(events, topNEvents...)
		}
	}

	if loaded.HistogramError != nil {
		logAutoPreSplitComponentFailure(tblInfo, idxInfo, "Histogram", loaded.HistogramError)
	} else {
		histogramEvents, err := buildAutoPreSplitHistogramEvents(
			sctx, &loaded.Column.Histogram, leadingCol)
		if err != nil {
			logAutoPreSplitComponentFailure(tblInfo, idxInfo, "Histogram", err)
		} else {
			events = append(events, histogramEvents...)
		}
	}
	events, totalCount, err := mergeAutoPreSplitEvents(events)
	if err != nil {
		return nil, "", err
	}
	if totalCount == 0 {
		return nil, "no usable leading column distribution", nil
	}
	boundaryRows := sampleAutoPreSplitEvents(events, totalCount, cfg.boundaryRatioStep)
	if len(boundaryRows) == 0 {
		return nil, "no internal distribution boundary", nil
	}
	splitKeys, err := buildAutoPreSplitIndexKeys(sctx, tblInfo, idxInfo, boundaryRows)
	if err == nil && boundaryCache != nil {
		boundaryCache[leadingCol.ID] = boundaryRows
	}
	return splitKeys, "", err
}

func buildAutoPreSplitIndexKeys(
	sctx sessionctx.Context,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	boundaryRows [][]types.Datum,
) ([][]byte, error) {
	rows := make([][]types.Datum, len(boundaryRows))
	for i := range boundaryRows {
		rows[i] = types.CloneRow(boundaryRows[i])
	}
	splitKeys, err := getSplitIdxKeysFromValueList(sctx, tblInfo, idxInfo, rows)
	if err != nil {
		return nil, fmt.Errorf("failed to build auto presplit keys: %w", err)
	}

	splitKeys = sortAndDedupeAutoPreSplitKeys(splitKeys)
	return splitKeys, nil
}

type autoPreSplitEvent struct {
	value   types.Datum
	encoded []byte
	count   uint64
}

func newAutoPreSplitEvent(
	sctx sessionctx.Context,
	value types.Datum,
	count uint64,
	colInfo *model.ColumnInfo,
) (autoPreSplitEvent, error) {
	splitValue, err := normalizeAutoPreSplitDatum(sctx, value, colInfo)
	if err != nil {
		return autoPreSplitEvent{}, err
	}
	encoded, err := codec.EncodeKey(sctx.GetSessionVars().Location(), nil, splitValue)
	if err != nil {
		return autoPreSplitEvent{}, err
	}
	return autoPreSplitEvent{value: splitValue, encoded: encoded, count: count}, nil
}

func normalizeAutoPreSplitDatum(
	sctx sessionctx.Context,
	value types.Datum,
	colInfo *model.ColumnInfo,
) (types.Datum, error) {
	if value.IsNull() {
		return value, nil
	}
	if types.IsString(colInfo.GetType()) && value.Kind() == types.KindBytes {
		// Analyze stores string TopN values and new-collation Histogram bounds as
		// comparison bytes. Keep bytes so index encoding does not apply collation twice.
		return types.NewBytesDatum(value.GetBytes()), nil
	}
	return value.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), &colInfo.FieldType)
}

func buildAutoPreSplitTopNEvents(
	sctx sessionctx.Context,
	topN *statistics.TopN,
	colInfo *model.ColumnInfo,
	limit int,
) ([]autoPreSplitEvent, error) {
	if limit <= 0 || topN == nil || topN.Num() == 0 {
		return nil, nil
	}
	num := min(topN.Num(), limit)
	events := make([]autoPreSplitEvent, 0, num)
	for i := range num {
		item := topN.TopN[i]
		datum, err := statistics.DecodeColumnTopNValue(
			item.Encoded, colInfo.GetType(), sctx.GetSessionVars().Location())
		if err != nil {
			return nil, err
		}
		if types.IsString(colInfo.GetType()) && datum.Kind() != types.KindBytes {
			return nil, fmt.Errorf("unexpected string TopN datum kind %d", datum.Kind())
		}
		event, err := newAutoPreSplitEvent(sctx, datum, item.Count, colInfo)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func buildAutoPreSplitHistogramEvents(
	sctx sessionctx.Context,
	histogram *statistics.Histogram,
	colInfo *model.ColumnInfo,
) ([]autoPreSplitEvent, error) {
	if histogram == nil || histogram.Len() == 0 {
		return nil, nil
	}
	events := make([]autoPreSplitEvent, 0, histogram.Len())
	var previous int64
	for i, bucket := range histogram.Buckets {
		if bucket.Count < previous {
			return nil, fmt.Errorf(
				"histogram bucket %d cumulative count %d is below previous count %d",
				i, bucket.Count, previous)
		}
		delta := bucket.Count - previous
		previous = bucket.Count
		if delta == 0 {
			continue
		}
		event, err := newAutoPreSplitEvent(sctx, *histogram.GetUpper(i), uint64(delta), colInfo)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func mergeAutoPreSplitEvents(events []autoPreSplitEvent) ([]autoPreSplitEvent, uint64, error) {
	events = slices.DeleteFunc(events, func(event autoPreSplitEvent) bool {
		return event.count == 0
	})
	slices.SortFunc(events, func(a, b autoPreSplitEvent) int {
		return bytes.Compare(a.encoded, b.encoded)
	})
	merged := events[:0]
	var total uint64
	for _, event := range events {
		if len(merged) > 0 && bytes.Equal(merged[len(merged)-1].encoded, event.encoded) {
			count, overflow := addAutoPreSplitCount(merged[len(merged)-1].count, event.count)
			if overflow {
				return nil, 0, fmt.Errorf("auto presplit count overflows while merging equal values")
			}
			merged[len(merged)-1].count = count
		} else {
			merged = append(merged, event)
		}
		var overflow bool
		total, overflow = addAutoPreSplitCount(total, event.count)
		if overflow {
			return nil, 0, fmt.Errorf("auto presplit distribution count overflows")
		}
	}
	return merged, total, nil
}

func addAutoPreSplitCount(a, b uint64) (uint64, bool) {
	if math.MaxUint64-a < b {
		return 0, true
	}
	return a + b, false
}

func sampleAutoPreSplitEvents(
	events []autoPreSplitEvent,
	totalCount uint64,
	boundaryRatioStep float64,
) [][]types.Datum {
	if totalCount == 0 || math.IsNaN(boundaryRatioStep) || math.IsInf(boundaryRatioStep, 0) ||
		boundaryRatioStep <= 0 || boundaryRatioStep > 1 {
		return nil
	}
	nextThresholdIndex := float64(1)
	var cumulative uint64
	rows := make([][]types.Datum, 0)
	// Emit only internal cumulative-distribution quantiles. One value is emitted
	// even if it crosses multiple thresholds, the terminal 100% boundary is
	// excluded, and Nextafter tolerates floating-point rounding at thresholds.
	for _, event := range events {
		cumulative += event.count
		nextThreshold := nextThresholdIndex * boundaryRatioStep
		if autoPreSplitThresholdReached(nextThreshold, 1) {
			break
		}
		cumulativeRatio := float64(cumulative) / float64(totalCount)
		if !autoPreSplitThresholdReached(cumulativeRatio, nextThreshold) {
			continue
		}
		rows = append(rows, []types.Datum{event.value})
		crossedThresholds := math.Floor(math.Nextafter(cumulativeRatio/boundaryRatioStep, math.Inf(1)))
		nextThresholdIndex = max(nextThresholdIndex+1, crossedThresholds+1)
	}
	return rows
}

func autoPreSplitThresholdReached(value, threshold float64) bool {
	return value >= threshold || math.Nextafter(value, math.Inf(1)) >= threshold
}

func logAutoPreSplitComponentFailure(
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	component string,
	err error,
) {
	logutil.DDLLogger().Warn("ignore unavailable auto presplit statistics component",
		zap.String("table", tblInfo.Name.L),
		zap.String("index", idxInfo.Name.L),
		zap.String("component", component),
		zap.Error(err))
}

func sortAndDedupeAutoPreSplitKeys(keys [][]byte) [][]byte {
	keys = slices.DeleteFunc(keys, func(key []byte) bool { return len(key) == 0 })
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}
