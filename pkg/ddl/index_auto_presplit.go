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
	"math/big"
	"math/bits"
	"slices"
	"strconv"

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

type autoPresplitStatsProvider interface {
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
	LoadColumnStatsForAutoPresplit(
		ctx context.Context,
		sctx sessionctx.Context,
		physicalTableID, columnID int64,
		colInfo *model.ColumnInfo,
		limit int,
	) (*handle.AutoPresplitColumnStats, error)
}

type autoPresplitConfig struct {
	minTableRows           int64
	maxTopNKeysPerPhysical int
	minStatsHealthy        int64
}

func getAutoPresplitConfig() autoPresplitConfig {
	cfg := autoPresplitConfig{
		minTableRows:           1_000_000,
		maxTopNKeysPerPhysical: int(vardef.MaxTiDBAnalyzeDefaultNumTopN),
		minStatsHealthy:        80,
	}
	failpoint.Inject("mockAutoPresplitConfig", func(val failpoint.Value) {
		if minRows, ok := val.(int); ok && minRows > 0 {
			cfg.minTableRows = int64(minRows)
			cfg.minStatsHealthy = 0
		}
	})
	return cfg
}

// captureAutoPresplitInterval captures the submission-time interval in the DDL job.
// This SESSION variable is temporary benchmark plumbing. Remove it and use the chosen
// production constant after benchmark results stabilize.
func captureAutoPresplitInterval(
	sctx sessionctx.Context,
	job *model.Job,
	maxSplitRegionNum uint64,
) error {
	value, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBDDLAutoPresplitInterval) //nolint:forbidigo
	if !ok {
		value = strconv.FormatFloat(vardef.DefTiDBDDLAutoPresplitInterval, 'f', -1, 64)
	}
	effective, clamped, err := effectiveAutoPresplitInterval(value, maxSplitRegionNum)
	if err != nil {
		return err
	}
	if job.SessionVars == nil {
		job.SessionVars = make(map[string]string)
	}
	job.AddSystemVars(vardef.TiDBDDLAutoPresplitInterval, effective)
	if clamped {
		logutil.DDLLogger().Info("raise auto presplit interval to respect split region limit",
			zap.String("requestedInterval", value),
			zap.String("effectiveInterval", effective),
			zap.Uint64("maxSplitRegionNum", maxSplitRegionNum))
	}
	return nil
}

func effectiveAutoPresplitInterval(value string, maxSplitRegionNum uint64) (string, bool, error) {
	interval, err := parseAutoPresplitInterval(value)
	if err != nil {
		return "", false, err
	}
	if interval.Sign() == 0 {
		return "0", false, nil
	}

	maxSplitRegionNum = max(maxSplitRegionNum, 1)
	minimum := new(big.Rat).SetFrac(
		big.NewInt(1),
		new(big.Int).SetUint64(maxSplitRegionNum),
	)
	if interval.Cmp(minimum) < 0 {
		return minimum.RatString(), true, nil
	}
	return interval.RatString(), false, nil
}

func autoPresplitIntervalFromJob(job *model.Job) string {
	if value, ok := job.GetSystemVars(vardef.TiDBDDLAutoPresplitInterval); ok {
		return value
	}
	return strconv.FormatFloat(vardef.DefTiDBDDLAutoPresplitInterval, 'f', -1, 64)
}

func parseAutoPresplitInterval(value string) (*big.Rat, error) {
	interval, ok := new(big.Rat).SetString(value)
	if !ok || interval.Sign() < 0 || interval.Cmp(big.NewRat(1, 1)) > 0 {
		return nil, fmt.Errorf("invalid auto presplit interval %q", value)
	}
	return interval, nil
}

func planAutoPresplitIndexRegions(
	ctx context.Context,
	sctx sessionctx.Context,
	statsProvider autoPresplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	cfg autoPresplitConfig,
	intervalValue string,
) ([][]byte, string, error) {
	interval, err := parseAutoPresplitInterval(intervalValue)
	if err != nil {
		return nil, "", err
	}
	if interval.Sign() == 0 {
		return nil, "auto presplit interval is zero", nil
	}
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
	colStats, loadNeeded, hasAnalyzed := statsTbl.ColumnIsLoadNeeded(leadingCol.ID, true)
	if !hasAnalyzed {
		return nil, "leading column stats missing or not analyzed", nil
	}

	loaded := &handle.AutoPresplitColumnStats{Column: colStats}
	if loadNeeded {
		loaded, err = statsProvider.LoadColumnStatsForAutoPresplit(
			ctx, sctx, tblInfo.ID, leadingCol.ID, leadingCol, cfg.maxTopNKeysPerPhysical)
		if cause := context.Cause(ctx); cause != nil {
			return nil, "", cause
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

	if loaded.TopNError != nil {
		logAutoPresplitComponentFailure(tblInfo, idxInfo, "TopN", loaded.TopNError)
	} else {
		topNEvents, err := buildAutoPresplitTopNEvents(
			sctx, loaded.Column.TopN, leadingCol, cfg.maxTopNKeysPerPhysical)
		if cause := context.Cause(ctx); cause != nil {
			return nil, "", cause
		}
		if err != nil {
			logAutoPresplitComponentFailure(tblInfo, idxInfo, "TopN", err)
		} else {
			events = append(events, topNEvents...)
		}
	}

	if loaded.HistogramError != nil {
		logAutoPresplitComponentFailure(tblInfo, idxInfo, "Histogram", loaded.HistogramError)
	} else {
		histogramEvents, err := buildAutoPresplitHistogramEvents(
			sctx, &loaded.Column.Histogram, leadingCol)
		if cause := context.Cause(ctx); cause != nil {
			return nil, "", cause
		}
		if err != nil {
			logAutoPresplitComponentFailure(tblInfo, idxInfo, "Histogram", err)
		} else {
			events = append(events, histogramEvents...)
		}
	}
	events, totalCount, err := mergeAutoPresplitEvents(events)
	if err != nil {
		return nil, "", err
	}
	if totalCount == 0 {
		return nil, "no usable leading column distribution", nil
	}
	boundaryRows := sampleAutoPresplitEvents(events, totalCount, interval)
	if len(boundaryRows) == 0 {
		return nil, "no internal distribution boundary", nil
	}
	splitKeys, err := getSplitIdxKeysFromValueList(sctx, tblInfo, idxInfo, boundaryRows)
	if err != nil {
		return nil, "", fmt.Errorf("failed to build auto presplit keys: %w", err)
	}

	splitKeys = sortAndDedupeAutoPresplitKeys(splitKeys)
	return splitKeys, "", nil
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
			count, carry := bits.Add64(merged[len(merged)-1].count, event.count, 0)
			if carry != 0 {
				return nil, 0, fmt.Errorf("auto presplit count overflows while merging equal values")
			}
			merged[len(merged)-1].count = count
			continue
		}
		merged = append(merged, event)
	}
	for _, event := range merged {
		var carry uint64
		total, carry = bits.Add64(total, event.count, 0)
		if carry != 0 {
			return nil, 0, fmt.Errorf("auto presplit distribution count overflows")
		}
	}
	return merged, total, nil
}

func sampleAutoPresplitEvents(
	events []autoPresplitEvent,
	totalCount uint64,
	interval *big.Rat,
) [][]types.Datum {
	if totalCount == 0 || interval == nil || interval.Sign() <= 0 {
		return nil
	}
	numerator := new(big.Int).Set(interval.Num())
	denominator := new(big.Int).Set(interval.Denom())
	maxThreshold := new(big.Int).Sub(new(big.Int).Set(denominator), big.NewInt(1))
	maxThreshold.Quo(maxThreshold, numerator)
	if maxThreshold.Sign() == 0 {
		return nil
	}

	totalTimesNumerator := new(big.Int).Mul(
		new(big.Int).SetUint64(totalCount),
		numerator,
	)
	nextThreshold := big.NewInt(1)
	var cumulative uint64
	rows := make([][]types.Datum, 0)
	for _, event := range events {
		cumulative += event.count
		crossed := new(big.Int).Mul(
			new(big.Int).SetUint64(cumulative),
			denominator,
		)
		crossed.Quo(crossed, totalTimesNumerator)
		if crossed.Cmp(maxThreshold) > 0 {
			crossed.Set(maxThreshold)
		}
		if crossed.Cmp(nextThreshold) >= 0 {
			rows = append(rows, []types.Datum{event.value})
			nextThreshold.Set(crossed)
			nextThreshold.Add(nextThreshold, big.NewInt(1))
			if nextThreshold.Cmp(maxThreshold) > 0 {
				break
			}
		}
	}
	return rows
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
