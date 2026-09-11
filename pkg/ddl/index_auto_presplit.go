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
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

type autoPreSplitStatsProvider interface {
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
	ReadColumnDistributionStats(
		ctx context.Context,
		sctx sessionctx.Context,
		physicalTableID int64,
		colInfo *model.ColumnInfo,
	) (*statistics.Column, error)
}

type autoPreSplitConfig struct {
	minTableRows      int64
	statsLoadTimeout  time.Duration
	minStatsHealthy   int64
	boundaryRatioStep float64
}

type autoPreSplitPlanState uint8

const (
	autoPreSplitPlanInvalid autoPreSplitPlanState = iota
	autoPreSplitPlanPlanned
	autoPreSplitPlanSkipped
)

type autoPreSplitPlanResult struct {
	state      autoPreSplitPlanState
	splitKeys  [][]byte
	skipReason string
}

type autoPreSplitBoundaryState uint8

const (
	autoPreSplitBoundaryReady autoPreSplitBoundaryState = iota
	autoPreSplitBoundarySkipped
	autoPreSplitBoundaryFailed
)

// Each cache entry preserves one original boundary-planning outcome for indexes
// sharing a leading column: boundaryRows, skipReason, or err, selected by state.
type autoPreSplitBoundaryCacheEntry struct {
	state        autoPreSplitBoundaryState
	boundaryRows [][]types.Datum
	skipReason   string
	err          error
}

func plannedAutoPreSplitResult(splitKeys [][]byte) (autoPreSplitPlanResult, error) {
	if len(splitKeys) == 0 {
		return autoPreSplitPlanResult{}, fmt.Errorf("planned auto pre-split has no split keys")
	}
	return autoPreSplitPlanResult{
		state:     autoPreSplitPlanPlanned,
		splitKeys: splitKeys,
	}, nil
}

func skippedAutoPreSplitResult(reason string) (autoPreSplitPlanResult, error) {
	if reason == "" {
		return autoPreSplitPlanResult{}, fmt.Errorf("skipped auto pre-split has no reason")
	}
	return autoPreSplitPlanResult{
		state:      autoPreSplitPlanSkipped,
		skipReason: reason,
	}, nil
}

func getAutoPreSplitConfig() autoPreSplitConfig {
	cfg := autoPreSplitConfig{
		// AUTO is intended for large tables, where distributing add-index writes
		// outweighs the statistics loading and Region operation overhead.
		minTableRows: 1_000_000,
		// Bound one leading-column statistics load. This allowance is also added to
		// the Region timeout to form the shared AUTO deadline.
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
			cfg.minTableRows = int64(minRows)
			cfg.minStatsHealthy = 0
		}
	})
	failpoint.Inject("mockAutoPresplitStatsLoadTimeout", func(val failpoint.Value) {
		if timeoutMS, ok := val.(int); ok && timeoutMS > 0 {
			cfg.statsLoadTimeout = time.Duration(timeoutMS) * time.Millisecond
		}
	})
	return cfg
}

func planAutoPreSplitWithCache(
	ctx context.Context,
	sctx sessionctx.Context,
	statsProvider autoPreSplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	cfg autoPreSplitConfig,
	boundaryCache map[int64]autoPreSplitBoundaryCacheEntry,
) (autoPreSplitPlanResult, error) {
	statsTbl, leadingCol, reason := checkAutoPreSplitEligibility(
		statsProvider, tblInfo, idxInfo, cfg)
	if reason != "" {
		return skippedAutoPreSplitResult(reason)
	}

	if _, ok := boundaryCache[leadingCol.ID]; !ok {
		boundaryCache[leadingCol.ID] = planAutoPreSplitBoundaries(
			ctx, sctx, statsProvider, tblInfo.ID, statsTbl, leadingCol, cfg)
	}
	boundaryResult := boundaryCache[leadingCol.ID]
	switch boundaryResult.state {
	case autoPreSplitBoundaryReady:
		// Continue with the cached boundary rows below.
	case autoPreSplitBoundarySkipped:
		return skippedAutoPreSplitResult(boundaryResult.skipReason)
	case autoPreSplitBoundaryFailed:
		return autoPreSplitPlanResult{}, boundaryResult.err
	}

	splitKeys, err := buildAutoPreSplitIndexKeys(
		sctx, tblInfo, idxInfo, boundaryResult.boundaryRows)
	if err != nil {
		return autoPreSplitPlanResult{}, err
	}
	return plannedAutoPreSplitResult(splitKeys)
}

func planAutoPreSplitBoundaries(
	ctx context.Context,
	sctx sessionctx.Context,
	statsProvider autoPreSplitStatsProvider,
	physicalTableID int64,
	statsTbl *statistics.Table,
	leadingCol *model.ColumnInfo,
	cfg autoPreSplitConfig,
) autoPreSplitBoundaryCacheEntry {
	skippedBoundary := func(reason string) autoPreSplitBoundaryCacheEntry {
		return autoPreSplitBoundaryCacheEntry{
			state:      autoPreSplitBoundarySkipped,
			skipReason: reason,
		}
	}
	failedBoundary := func(err error) autoPreSplitBoundaryCacheEntry {
		return autoPreSplitBoundaryCacheEntry{
			state: autoPreSplitBoundaryFailed,
			err:   err,
		}
	}

	colStats, loadNeeded, hasAnalyzed := statsTbl.ColumnIsLoadNeeded(leadingCol.ID, true)
	if !hasAnalyzed {
		return skippedBoundary("leading column stats missing or not analyzed")
	}

	loaded := colStats
	if loadNeeded {
		loadCtx, cancel := context.WithTimeout(ctx, cfg.statsLoadTimeout)
		var err error
		loaded, err = statsProvider.ReadColumnDistributionStats(
			loadCtx, sctx, physicalTableID, leadingCol)
		cancel()
		if err != nil {
			return failedBoundary(
				fmt.Errorf("failed to read leading column statistics from storage: %w", err))
		}
	}
	if loaded == nil {
		return skippedBoundary("leading column stats metadata missing")
	}
	if loaded.StatsVer != statistics.Version2 {
		return skippedBoundary(
			fmt.Sprintf("leading column stats version %d is not Analyze V2", loaded.StatsVer))
	}
	if loaded.NullCount < 0 {
		return failedBoundary(fmt.Errorf(
			"leading column statistics have negative null count %d", loaded.NullCount))
	}

	values := make([]autoPreSplitValue, 0, loaded.TopN.Num()+loaded.Histogram.Len()+1)
	if loaded.NullCount > 0 {
		nullValue, err := newAutoPreSplitValue(
			sctx, types.NewDatum(nil), uint64(loaded.NullCount), leadingCol)
		if err != nil {
			return failedBoundary(fmt.Errorf(
				"failed to build NullCount auto pre-split value: %w", err))
		}
		values = append(values, nullValue)
	}

	topNValues, err := buildAutoPreSplitTopNValues(sctx, loaded.TopN, leadingCol)
	if err != nil {
		return failedBoundary(fmt.Errorf(
			"failed to build TopN auto pre-split values: %w", err))
	}
	values = append(values, topNValues...)

	histogramValues, err := buildAutoPreSplitHistogramValues(
		sctx, &loaded.Histogram, leadingCol)
	if err != nil {
		return failedBoundary(fmt.Errorf(
			"failed to build Histogram auto pre-split values: %w", err))
	}
	values = append(values, histogramValues...)

	values, totalCount := mergeAutoPreSplitValues(values)
	if totalCount == 0 {
		return skippedBoundary("no usable leading column distribution")
	}
	boundaryRows := sampleAutoPreSplitValues(values, totalCount, cfg.boundaryRatioStep)
	if len(boundaryRows) == 0 {
		return skippedBoundary("no internal distribution boundary")
	}
	return autoPreSplitBoundaryCacheEntry{
		state:        autoPreSplitBoundaryReady,
		boundaryRows: boundaryRows,
	}
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

type autoPreSplitValue struct {
	value   types.Datum
	encoded []byte
	count   uint64
}

func newAutoPreSplitValue(
	sctx sessionctx.Context,
	value types.Datum,
	count uint64,
	colInfo *model.ColumnInfo,
) (autoPreSplitValue, error) {
	splitValue, err := normalizeAutoPreSplitDatum(sctx, value, colInfo)
	if err != nil {
		return autoPreSplitValue{}, err
	}
	encoded, err := codec.EncodeKey(sctx.GetSessionVars().Location(), nil, splitValue)
	if err != nil {
		return autoPreSplitValue{}, err
	}
	return autoPreSplitValue{value: splitValue, encoded: encoded, count: count}, nil
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

func buildAutoPreSplitTopNValues(
	sctx sessionctx.Context,
	topN *statistics.TopN,
	colInfo *model.ColumnInfo,
) ([]autoPreSplitValue, error) {
	if topN == nil || topN.Num() == 0 {
		return nil, nil
	}
	num := topN.Num()
	values := make([]autoPreSplitValue, 0, num)
	for i := range num {
		item := topN.TopN[i]
		datum, err := statistics.DecodeColumnTopNValue(
			item.Encoded, &colInfo.FieldType, sctx.GetSessionVars().Location())
		if err != nil {
			return nil, err
		}
		if types.IsString(colInfo.GetType()) && datum.Kind() != types.KindBytes {
			// String TopN values must remain collation comparison bytes. Any other
			// kind means the stored statistics and column type do not match.
			return nil, fmt.Errorf("unexpected string TopN datum kind %d", datum.Kind())
		}
		value, err := newAutoPreSplitValue(sctx, datum, item.Count, colInfo)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func buildAutoPreSplitHistogramValues(
	sctx sessionctx.Context,
	histogram *statistics.Histogram,
	colInfo *model.ColumnInfo,
) ([]autoPreSplitValue, error) {
	if histogram == nil || histogram.Len() == 0 {
		return nil, nil
	}
	values := make([]autoPreSplitValue, 0, histogram.Len())
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
		upper := *histogram.GetUpper(i)
		if types.IsString(colInfo.GetType()) {
			// String Histogram bounds are stored as collation comparison bytes.
			// GetUpper returns KindString, so restore bytes before index encoding.
			upper = types.NewBytesDatum(upper.GetBytes())
		}
		value, err := newAutoPreSplitValue(sctx, upper, uint64(delta), colInfo)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func mergeAutoPreSplitValues(values []autoPreSplitValue) ([]autoPreSplitValue, uint64) {
	values = slices.DeleteFunc(values, func(value autoPreSplitValue) bool {
		return value.count == 0
	})
	slices.SortFunc(values, func(a, b autoPreSplitValue) int {
		return bytes.Compare(a.encoded, b.encoded)
	})
	merged := values[:0]
	var total uint64
	for _, value := range values {
		if len(merged) > 0 && bytes.Equal(merged[len(merged)-1].encoded, value.encoded) {
			merged[len(merged)-1].count += value.count
		} else {
			merged = append(merged, value)
		}
		total += value.count
	}
	return merged, total
}

func sampleAutoPreSplitValues(
	values []autoPreSplitValue,
	totalCount uint64,
	boundaryRatioStep float64,
) [][]types.Datum {
	if totalCount == 0 {
		return nil
	}
	nextThresholdIndex := 1
	var cumulative uint64
	rows := make([][]types.Datum, 0)
	// Emit only internal cumulative-distribution quantiles. One value is emitted
	// even if it crosses multiple thresholds, the terminal 100% boundary is
	// excluded.
	for _, value := range values {
		cumulative += value.count
		nextThreshold := float64(nextThresholdIndex) * boundaryRatioStep
		if nextThreshold >= 1 {
			break
		}
		cumulativeRatio := float64(cumulative) / float64(totalCount)
		if cumulativeRatio < nextThreshold {
			continue
		}
		rows = append(rows, []types.Datum{value.value})
		crossedThresholds := int(math.Floor(cumulativeRatio / boundaryRatioStep))
		nextThresholdIndex = max(nextThresholdIndex+1, crossedThresholds+1)
	}
	return rows
}

func sortAndDedupeAutoPreSplitKeys(keys [][]byte) [][]byte {
	keys = slices.DeleteFunc(keys, func(key []byte) bool { return len(key) == 0 })
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}
