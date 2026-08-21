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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

type autoPreSplitStatsProvider interface {
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
	LoadColumnDistributionStats(
		ctx context.Context,
		sctx sessionctx.Context,
		physicalTableID int64,
		colInfo *model.ColumnInfo,
		maxTopNKeys int,
	) (*statistics.Column, error)
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
		// AUTO is intended for large tables, where distributing add-index writes
		// outweighs the statistics loading and Region operation overhead.
		minTableRows: 1_000_000,
		// Use Analyze's supported maximum so all stored TopN entries can
		// participate while the storage query remains bounded.
		maxTopNKeysPerPhysical: int(vardef.MaxTiDBAnalyzeDefaultNumTopN),
		// Bound the delay this optional optimization can add before add-index starts.
		statsLoadTimeout: 30 * time.Second,
		// Require statistics with no more than about 20% modified rows so stale
		// distributions do not produce poor split boundaries.
		minStatsHealthy: 80,
		// A 2% step produces at most 49 internal boundaries. Performance tests
		// showed that it had the smallest impact on online workload TPS during add-index.
		boundaryRatioStep: 0.02,
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
	statsTbl, leadingCol, reason := checkAutoPreSplitEligibility(
		statsProvider, tblInfo, idxInfo, cfg)
	if reason != "" {
		return nil, reason, nil
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

	loaded := colStats
	if loadNeeded {
		loadCtx, cancel := context.WithTimeout(ctx, cfg.statsLoadTimeout)
		var err error
		loaded, err = statsProvider.LoadColumnDistributionStats(
			loadCtx, sctx, tblInfo.ID, leadingCol, cfg.maxTopNKeysPerPhysical)
		cancel()
		if err != nil {
			return nil, "", fmt.Errorf("failed to load leading column statistics from storage: %w", err)
		}
	}
	if loaded == nil {
		return nil, "leading column stats metadata missing", nil
	}
	if loaded.StatsVer != statistics.Version2 {
		return nil, fmt.Sprintf("leading column stats version %d is not Analyze V2", loaded.StatsVer), nil
	}
	if loaded.NullCount < 0 {
		return nil, "", fmt.Errorf("leading column statistics have negative null count %d", loaded.NullCount)
	}

	// The configured TopN maximum equals Analyze's supported maximum, so all valid
	// TopN entries and Histogram buckets participate in boundary planning.
	events := make([]autoPreSplitEvent, 0, cfg.maxTopNKeysPerPhysical+loaded.Histogram.Len()+1)
	if loaded.NullCount > 0 {
		nullEvent, err := newAutoPreSplitEvent(
			sctx, types.NewDatum(nil), uint64(loaded.NullCount), leadingCol)
		if err != nil {
			return nil, "", fmt.Errorf("failed to build NullCount auto pre-split event: %w", err)
		}
		events = append(events, nullEvent)
	}

	topNEvents, err := buildAutoPreSplitTopNEvents(
		sctx, loaded.TopN, leadingCol, cfg.maxTopNKeysPerPhysical)
	if err != nil {
		return nil, "", fmt.Errorf("failed to build TopN auto pre-split events: %w", err)
	}
	events = append(events, topNEvents...)

	histogramEvents, err := buildAutoPreSplitHistogramEvents(
		sctx, &loaded.Histogram, leadingCol)
	if err != nil {
		return nil, "", fmt.Errorf("failed to build Histogram auto pre-split events: %w", err)
	}
	events = append(events, histogramEvents...)

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

func checkAutoPreSplitEligibility(
	statsProvider autoPreSplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	cfg autoPreSplitConfig,
) (*statistics.Table, *model.ColumnInfo, string) {
	if tblInfo.GetPartitionInfo() != nil {
		return nil, nil, "partitioned table"
	}
	// Ordinary column statistics describe the full table rather than the
	// predicate-filtered row set represented by a partial index.
	if idxInfo.HasCondition() {
		return nil, nil, "partial index"
	}
	if len(idxInfo.Columns) == 0 {
		return nil, nil, "index has no columns"
	}

	statsTbl := statsProvider.GetPhysicalTableStats(tblInfo.ID, tblInfo)
	// Provider implementations may report an uninitialized cache entry as nil
	// or pseudo statistics. Neither has a reliable distribution for AUTO.
	if statsTbl == nil {
		return nil, nil, "stats missing"
	}
	if statsTbl.Pseudo {
		return nil, nil, "stats pseudo"
	}
	// Cached statistics can remain usable for query planning after substantial
	// table changes, but AUTO skips them because its split boundaries cannot be
	// corrected after add-index starts.
	if statsTbl.IsOutdated() {
		return nil, nil, "stats outdated"
	}
	healthy, ok := statsTbl.GetStatsHealthy()
	if !ok {
		return nil, nil, "stats health unavailable"
	}
	if healthy < cfg.minStatsHealthy {
		return nil, nil, fmt.Sprintf("stats health %d below threshold %d", healthy, cfg.minStatsHealthy)
	}
	if statsTbl.RealtimeCount < cfg.minTableRows {
		return nil, nil, fmt.Sprintf("row count %d below threshold %d", statsTbl.RealtimeCount, cfg.minTableRows)
	}

	// Auto presplit intentionally uses only the leading index column. The available
	// per-column statistics cannot describe later-column distributions under a hot
	// leading-column value, and deriving such split keys would require reading table data.
	leadingIdxCol := idxInfo.Columns[0]
	offset := leadingIdxCol.Offset
	if offset < 0 || offset >= len(tblInfo.Columns) {
		return nil, nil, "leading column not found"
	}
	leadingCol := tblInfo.Columns[offset]
	// A column TopN only keeps the full value's collation key, which cannot be
	// truncated by characters for a prefix index.
	if types.IsString(leadingCol.GetType()) && leadingIdxCol.Length != types.UnspecifiedLength {
		return nil, nil, "leading string column uses prefix index"
	}
	return statsTbl, leadingCol, ""
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
			item.Encoded, &colInfo.FieldType, sctx.GetSessionVars().Location())
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
	if totalCount == 0 {
		return nil
	}
	nextThresholdIndex := float64(1)
	var cumulative uint64
	rows := make([][]types.Datum, 0)
	// Emit only internal cumulative-distribution quantiles. One value is emitted
	// even if it crosses multiple thresholds, the terminal 100% boundary is
	// excluded.
	for _, event := range events {
		cumulative += event.count
		nextThreshold := nextThresholdIndex * boundaryRatioStep
		if nextThreshold >= 1 {
			break
		}
		cumulativeRatio := float64(cumulative) / float64(totalCount)
		if cumulativeRatio < nextThreshold {
			continue
		}
		rows = append(rows, []types.Datum{event.value})
		crossedThresholds := math.Floor(cumulativeRatio / boundaryRatioStep)
		nextThresholdIndex = max(nextThresholdIndex+1, crossedThresholds+1)
	}
	return rows
}

func sortAndDedupeAutoPreSplitKeys(keys [][]byte) [][]byte {
	keys = slices.DeleteFunc(keys, func(key []byte) bool { return len(key) == 0 })
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}
