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
	"fmt"
	"slices"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/types"
)

type autoSplitStatsProvider interface {
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
}

type autoSplitHotRegionConfig struct {
	minTableRows           int64
	maxTopNKeysPerPhysical int
	topNMinCount           uint64
	topNMinRatio           float64
	minStatsHealthy        int64
}

func getAutoSplitHotRegionConfig() autoSplitHotRegionConfig {
	cfg := autoSplitHotRegionConfig{
		minTableRows:           1_000_000,
		maxTopNKeysPerPhysical: 100,
		topNMinCount:           500_000,
		topNMinRatio:           0.01,
		minStatsHealthy:        80,
	}
	failpoint.InjectCall("mockAutoSplitHotRegionConfig", &cfg, func(minRows int) {
		if minRows > 0 {
			cfg.applyTestMinRows(minRows)
		}
	})
	return cfg
}

func (cfg *autoSplitHotRegionConfig) applyTestMinRows(minRows int) {
	cfg.minTableRows = int64(minRows)
	cfg.maxTopNKeysPerPhysical = 4
	cfg.topNMinCount = uint64(minRows)
	cfg.topNMinRatio = 0
	cfg.minStatsHealthy = 0
}

func planAutoSplitIndexRegions(
	sctx sessionctx.Context,
	statsProvider autoSplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	cfg autoSplitHotRegionConfig,
) ([][]byte, string, error) {
	if tblInfo.GetPartitionInfo() != nil {
		return nil, "partitioned table", nil
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

	// Auto-splitting intentionally uses only the leading index column. The available
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
		topN, err = storage.TopNFromStorageWithPriority(sctx, tblInfo.ID, 0, leadingCol.ID, kv.PriorityNormal)
		if err != nil {
			return nil, "failed to load leading column TopN from storage", err
		}
	} else {
		topN = colStats.TopN
	}

	topNRows, err := buildAutoSplitTopNRows(sctx, statsTbl, topN, leadingCol, cfg)
	if err != nil {
		return nil, "failed to build TopN split keys", err
	}
	if len(topNRows) == 0 {
		return nil, "no split strategy matched", nil
	}
	splitKeys, err := getSplitIdxKeysFromValueList(sctx, tblInfo, idxInfo, topNRows)
	if err != nil {
		return nil, "failed to build TopN split keys", err
	}

	splitKeys = sortAndDedupeAutoSplitKeys(splitKeys)
	return splitKeys, "", nil
}

func buildAutoSplitTopNRows(
	sctx sessionctx.Context,
	statsTbl *statistics.Table,
	topN *statistics.TopN,
	colInfo *model.ColumnInfo,
	cfg autoSplitHotRegionConfig,
) ([][]types.Datum, error) {
	if cfg.maxTopNKeysPerPhysical == 0 || topN == nil || topN.Num() == 0 {
		return nil, nil
	}

	topNCandidates := make([]statistics.TopNMeta, 0, topN.Num())
	for _, topNItem := range topN.TopN {
		if topNItem.Count < cfg.topNMinCount {
			continue
		}
		if cfg.topNMinRatio > 0 && statsTbl.RealtimeCount > 0 &&
			float64(topNItem.Count)/float64(statsTbl.RealtimeCount) < cfg.topNMinRatio {
			continue
		}
		topNCandidates = append(topNCandidates, topNItem)
	}
	if len(topNCandidates) == 0 {
		return nil, nil
	}
	statistics.SortTopnMeta(topNCandidates)

	topNRows := make([][]types.Datum, 0, min(cfg.maxTopNKeysPerPhysical, len(topNCandidates)))
	for _, topNItem := range topNCandidates {
		datum, err := statistics.TopNMetaToDatum(
			topNItem, colInfo.GetType(), false, sctx.GetSessionVars().Location())
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
		if len(topNRows) >= cfg.maxTopNKeysPerPhysical {
			break
		}
	}
	return topNRows, nil
}

func sortAndDedupeAutoSplitKeys(keys [][]byte) [][]byte {
	keys = slices.DeleteFunc(keys, func(key []byte) bool { return len(key) == 0 })
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}
