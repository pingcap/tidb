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
	"slices"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/generic"
)

type autoPresplitStatsProvider interface {
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
	LoadColumnTopN(
		ctx context.Context,
		sctx sessionctx.Context,
		physicalTableID, columnID int64,
		limit int,
	) (*statistics.TopN, error)
}

type autoPresplitConfig struct {
	minTableRows           int64
	maxTopNKeysPerPhysical int
	topNMinCount           uint64
	topNMinRatio           float64
	minStatsHealthy        int64
}

func getAutoPresplitConfig() autoPresplitConfig {
	cfg := autoPresplitConfig{
		minTableRows:           1_000_000,
		maxTopNKeysPerPhysical: 100,
		topNMinCount:           500_000,
		topNMinRatio:           0.01,
		minStatsHealthy:        80,
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
	cfg.maxTopNKeysPerPhysical = 4
	cfg.topNMinCount = uint64(minRows)
	cfg.topNMinRatio = 0
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
	var topN *statistics.TopN
	if loadNeeded {
		var err error
		topN, err = statsProvider.LoadColumnTopN(
			ctx, sctx, tblInfo.ID, leadingCol.ID, cfg.maxTopNKeysPerPhysical)
		if err != nil {
			return nil, "", fmt.Errorf("failed to load leading column TopN from storage: %w", err)
		}
	} else {
		topN = colStats.TopN
	}

	topNRows, err := buildAutoPresplitTopNRows(sctx, statsTbl, topN, leadingCol, cfg)
	if err != nil {
		return nil, "", fmt.Errorf("failed to build TopN split keys: %w", err)
	}
	if len(topNRows) == 0 {
		return nil, "no split strategy matched", nil
	}
	splitKeys, err := getSplitIdxKeysFromValueList(sctx, tblInfo, idxInfo, topNRows)
	if err != nil {
		return nil, "", fmt.Errorf("failed to build TopN split keys: %w", err)
	}

	splitKeys = sortAndDedupeAutoPresplitKeys(splitKeys)
	return splitKeys, "", nil
}

func buildAutoPresplitTopNRows(
	sctx sessionctx.Context,
	statsTbl *statistics.Table,
	topN *statistics.TopN,
	colInfo *model.ColumnInfo,
	cfg autoPresplitConfig,
) ([][]types.Datum, error) {
	if cfg.maxTopNKeysPerPhysical == 0 || topN == nil || topN.Num() == 0 {
		return nil, nil
	}

	topNCandidates := generic.NewBoundedMinHeap(
		cfg.maxTopNKeysPerPhysical,
		func(a, b statistics.TopNMeta) int { return -statistics.TopnMetaCompare(a, b) },
	)
	for _, topNItem := range topN.TopN {
		if topNItem.Count < cfg.topNMinCount {
			continue
		}
		if cfg.topNMinRatio > 0 && statsTbl.RealtimeCount > 0 &&
			float64(topNItem.Count)/float64(statsTbl.RealtimeCount) < cfg.topNMinRatio {
			continue
		}
		topNCandidates.Add(topNItem)
	}
	selectedTopN := topNCandidates.ToSortedSlice()
	if len(selectedTopN) == 0 {
		return nil, nil
	}

	topNRows := make([][]types.Datum, 0, len(selectedTopN))
	for _, topNItem := range selectedTopN {
		datum, err := statistics.DecodeColumnTopNValue(
			topNItem.Encoded, colInfo.GetType(), sctx.GetSessionVars().Location())
		if err != nil {
			return nil, err
		}
		var splitDatum types.Datum
		if types.IsString(colInfo.GetType()) {
			if datum.Kind() != types.KindBytes {
				return nil, fmt.Errorf("unexpected string TopN datum kind %d", datum.Kind())
			}
			// ANALYZE stores string-column TopN values as comparison bytes. ConvertTo is
			// redundant when the column charset is binary; otherwise it changes the datum
			// to KindString and can make index encoding apply the column collation again.
			topNComparisonBytes := datum.GetBytes()
			splitDatum = types.NewBytesDatum(topNComparisonBytes)
		} else {
			splitDatum, err = datum.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), &colInfo.FieldType)
			if err != nil {
				return nil, err
			}
		}
		topNRows = append(topNRows, []types.Datum{splitDatum})
	}
	return topNRows, nil
}

func sortAndDedupeAutoPresplitKeys(keys [][]byte) [][]byte {
	keys = slices.DeleteFunc(keys, func(key []byte) bool { return len(key) == 0 })
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}
