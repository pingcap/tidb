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

const (
	boundaryRatioStep                   float64 = 0.02
	defaultAutoPresplitStatsLoadTimeout         = 30 * time.Second
)

type autoPresplitStatsProvider interface {
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
	LoadColumnStatsForAutoPresplit(
		ctx context.Context,
		sctx sessionctx.Context,
		physicalTableID int64,
		colInfo *model.ColumnInfo,
		maxTopNKeys int,
	) (*handle.AutoPresplitColumnStats, error)
}

type autoPresplitConfig struct {
	minTableRows           int64
	maxTopNKeysPerPhysical int
	statsLoadTimeout       time.Duration
	minStatsHealthy        int64
	boundaryRatioStep      float64
}

func getAutoPresplitConfig() autoPresplitConfig {
	cfg := autoPresplitConfig{
		minTableRows:           1_000_000,
		maxTopNKeysPerPhysical: int(vardef.MaxTiDBAnalyzeDefaultNumTopN),
		statsLoadTimeout:       defaultAutoPresplitStatsLoadTimeout,
		minStatsHealthy:        80,
		boundaryRatioStep:      boundaryRatioStep,
	}
	failpoint.Inject("mockAutoPresplitConfig", func(val failpoint.Value) {
		if minRows, ok := val.(int); ok && minRows > 0 {
			cfg.applyTestConfigOverrides(minRows)
		}
	})
	return cfg
}

func (cfg *autoPresplitConfig) applyTestConfigOverrides(minRows int) {
	cfg.minTableRows = int64(minRows)
	cfg.minStatsHealthy = 0
}

func planAutoPresplitIndexRegions(
	ctx context.Context,
	sctx sessionctx.Context,
	statsProvider autoPresplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	cfg autoPresplitConfig,
) ([][]byte, string, error) {
	return planAutoPresplitWithCache(
		ctx, sctx, statsProvider, tblInfo, idxInfo, cfg, nil)
}

func planAutoPresplitWithCache(
	ctx context.Context,
	sctx sessionctx.Context,
	statsProvider autoPresplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	cfg autoPresplitConfig,
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
		splitKeys, err := buildAutoPresplitIndexKeys(sctx, tblInfo, idxInfo, boundaryRows)
		return splitKeys, "", err
	}
	colStats, loadNeeded, hasAnalyzed := statsTbl.ColumnIsLoadNeeded(leadingCol.ID, true)
	if !hasAnalyzed {
		return nil, "leading column stats missing or not analyzed", nil
	}

	loaded := &handle.AutoPresplitColumnStats{Column: colStats}
	if loadNeeded {
		loadCtx, cancel := context.WithTimeout(ctx, cfg.statsLoadTimeout)
		var err error
		loaded, err = statsProvider.LoadColumnStatsForAutoPresplit(
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
	events := make([]autoPresplitEvent, 0, cfg.maxTopNKeysPerPhysical+loaded.Column.Histogram.Len()+1)
	if loaded.NullCountError != nil {
		logAutoPresplitComponentFailure(tblInfo, idxInfo, "NullCount", loaded.NullCountError)
	} else if loaded.Column.NullCount > 0 {
		nullEvent, err := newAutoPresplitEvent(
			sctx, types.NewDatum(nil), uint64(loaded.Column.NullCount), leadingCol)
		if err != nil {
			logAutoPresplitComponentFailure(tblInfo, idxInfo, "NullCount", err)
		} else {
			events = append(events, nullEvent)
		}
	}

	topNEventCount := 0
	if loaded.TopNError != nil {
		logAutoPresplitComponentFailure(tblInfo, idxInfo, "TopN", loaded.TopNError)
	} else {
		topNEvents, err := buildAutoPresplitTopNEvents(
			sctx, loaded.Column.TopN, leadingCol, cfg.maxTopNKeysPerPhysical)
		if err != nil {
			logAutoPresplitComponentFailure(tblInfo, idxInfo, "TopN", err)
		} else {
			topNEventCount = len(topNEvents)
			events = append(events, topNEvents...)
		}
	}

	histogramEventCount := 0
	if loaded.HistogramError != nil {
		logAutoPresplitComponentFailure(tblInfo, idxInfo, "Histogram", loaded.HistogramError)
	} else {
		histogramEvents, err := buildAutoPresplitHistogramEvents(
			sctx, &loaded.Column.Histogram, leadingCol)
		if err != nil {
			logAutoPresplitComponentFailure(tblInfo, idxInfo, "Histogram", err)
		} else {
			histogramEventCount = len(histogramEvents)
			events = append(events, histogramEvents...)
		}
	}
	logutil.DDLLogger().Info("built auto presplit statistics events",
		zap.String("table", tblInfo.Name.L),
		zap.String("index", idxInfo.Name.L),
		zap.Int("topNEvents", topNEventCount),
		zap.Int("histogramEvents", histogramEventCount))
	events, totalCount, err := mergeAutoPresplitEvents(events)
	if err != nil {
		return nil, "", err
	}
	if totalCount == 0 {
		return nil, "no usable leading column distribution", nil
	}
	boundaryRows := sampleAutoPresplitEvents(events, totalCount, cfg.boundaryRatioStep)
	if len(boundaryRows) == 0 {
		return nil, "no internal distribution boundary", nil
	}
	splitKeys, err := buildAutoPresplitIndexKeys(sctx, tblInfo, idxInfo, boundaryRows)
	if err == nil && boundaryCache != nil {
		boundaryCache[leadingCol.ID] = boundaryRows
	}
	return splitKeys, "", err
}

func buildAutoPresplitIndexKeys(
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

	splitKeys = sortAndDedupeAutoPresplitKeys(splitKeys)
	return splitKeys, nil
}

type autoPresplitEvent struct {
	value   types.Datum
	encoded []byte
	count   uint64
}

func newAutoPresplitEvent(
	sctx sessionctx.Context,
	value types.Datum,
	count uint64,
	colInfo *model.ColumnInfo,
) (autoPresplitEvent, error) {
	splitValue, err := normalizeAutoPresplitDatum(sctx, value, colInfo)
	if err != nil {
		return autoPresplitEvent{}, err
	}
	encoded, err := codec.EncodeKey(sctx.GetSessionVars().Location(), nil, splitValue)
	if err != nil {
		return autoPresplitEvent{}, err
	}
	return autoPresplitEvent{value: splitValue, encoded: encoded, count: count}, nil
}

func normalizeAutoPresplitDatum(
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

func buildAutoPresplitTopNEvents(
	sctx sessionctx.Context,
	topN *statistics.TopN,
	colInfo *model.ColumnInfo,
	limit int,
) ([]autoPresplitEvent, error) {
	if limit <= 0 || topN == nil || topN.Num() == 0 {
		return nil, nil
	}
	num := min(topN.Num(), limit)
	events := make([]autoPresplitEvent, 0, num)
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
		event, err := newAutoPresplitEvent(sctx, datum, item.Count, colInfo)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func buildAutoPresplitHistogramEvents(
	sctx sessionctx.Context,
	histogram *statistics.Histogram,
	colInfo *model.ColumnInfo,
) ([]autoPresplitEvent, error) {
	if histogram == nil || histogram.Len() == 0 {
		return nil, nil
	}
	events := make([]autoPresplitEvent, 0, histogram.Len())
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
		event, err := newAutoPresplitEvent(sctx, *histogram.GetUpper(i), uint64(delta), colInfo)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func mergeAutoPresplitEvents(events []autoPresplitEvent) ([]autoPresplitEvent, uint64, error) {
	events = slices.DeleteFunc(events, func(event autoPresplitEvent) bool {
		return event.count == 0
	})
	slices.SortFunc(events, func(a, b autoPresplitEvent) int {
		return bytes.Compare(a.encoded, b.encoded)
	})
	merged := events[:0]
	var total uint64
	for _, event := range events {
		if len(merged) > 0 && bytes.Equal(merged[len(merged)-1].encoded, event.encoded) {
			count, overflow := addAutoPresplitCount(merged[len(merged)-1].count, event.count)
			if overflow {
				return nil, 0, fmt.Errorf("auto presplit count overflows while merging equal values")
			}
			merged[len(merged)-1].count = count
		} else {
			merged = append(merged, event)
		}
		var overflow bool
		total, overflow = addAutoPresplitCount(total, event.count)
		if overflow {
			return nil, 0, fmt.Errorf("auto presplit distribution count overflows")
		}
	}
	return merged, total, nil
}

func addAutoPresplitCount(a, b uint64) (uint64, bool) {
	if math.MaxUint64-a < b {
		return 0, true
	}
	return a + b, false
}

func sampleAutoPresplitEvents(
	events []autoPresplitEvent,
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
		if autoPresplitThresholdReached(nextThreshold, 1) {
			break
		}
		cumulativeRatio := float64(cumulative) / float64(totalCount)
		if !autoPresplitThresholdReached(cumulativeRatio, nextThreshold) {
			continue
		}
		rows = append(rows, []types.Datum{event.value})
		crossedThresholds := math.Floor(math.Nextafter(cumulativeRatio/boundaryRatioStep, math.Inf(1)))
		nextThresholdIndex = max(nextThresholdIndex+1, crossedThresholds+1)
	}
	return rows
}

func autoPresplitThresholdReached(value, threshold float64) bool {
	return value >= threshold || math.Nextafter(value, math.Inf(1)) >= threshold
}

func logAutoPresplitComponentFailure(
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

func sortAndDedupeAutoPresplitKeys(keys [][]byte) [][]byte {
	keys = slices.DeleteFunc(keys, func(key []byte) bool { return len(key) == 0 })
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}
