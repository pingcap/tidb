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
	"strings"

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
	minTableRows                   int64
	rowsPerRegion                  int64
	maxFullRangeRegionsPerPhysical int
	maxTopNKeysPerPhysical         int
	// regionCandidateBudget is a soft table-level target shared by full-range and TopN splitting.
	// The final split-key count may exceed it because each eligible partition gets a minimum budget
	// and index boundary keys are added separately.
	regionCandidateBudget int
	topNMinCount          uint64
	topNMinRatio          float64
	minStatsHealthy       int64
}

func getAutoSplitHotRegionConfig() autoSplitHotRegionConfig {
	cfg := autoSplitHotRegionConfig{
		minTableRows:                   1_000_000,
		rowsPerRegion:                  500_000,
		maxFullRangeRegionsPerPhysical: 100,
		maxTopNKeysPerPhysical:         100,
		regionCandidateBudget:          2560,
		topNMinCount:                   500_000,
		topNMinRatio:                   0.01,
		minStatsHealthy:                80,
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
	cfg.rowsPerRegion = int64(minRows)
	cfg.maxFullRangeRegionsPerPhysical = 4
	cfg.maxTopNKeysPerPhysical = 4
	cfg.regionCandidateBudget = 32
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
	if statsProvider == nil {
		return nil, "stats handle is nil", nil
	}
	if len(idxInfo.Columns) == 0 {
		return nil, "index has no columns", nil
	}

	physicalIDs := getAutoSplitPhysicalTableIDs(tblInfo, idxInfo)
	statsTables := make([]*statistics.Table, len(physicalIDs))
	var totalRows int64
	for i, physicalID := range physicalIDs {
		statsTbl := statsProvider.GetPhysicalTableStats(physicalID, tblInfo)
		statsTables[i] = statsTbl
		if statsTbl != nil && !statsTbl.Pseudo && statsTbl.RealtimeCount > 0 {
			totalRows += statsTbl.RealtimeCount
		}
	}

	splitKeys := make([][]byte, 0)
	skipReasons := make([]string, 0, len(physicalIDs))
	for i, physicalID := range physicalIDs {
		rowsRatio := 1.0
		if len(physicalIDs) > 1 {
			rowsRatio = 1 / float64(len(physicalIDs))
			statsTbl := statsTables[i]
			if totalRows > 0 {
				rowsRatio = 0
				if statsTbl != nil && !statsTbl.Pseudo && statsTbl.RealtimeCount > 0 {
					rowsRatio = float64(statsTbl.RealtimeCount) / float64(totalRows)
				}
			}
		}
		physicalCfg := cfg.withRegionCandidateRatio(rowsRatio)
		keys, reason, err := planAutoSplitPhysicalIndexRegions(
			sctx, statsTables[i], tblInfo, idxInfo, physicalID, physicalCfg)
		if err != nil {
			return nil, reason, err
		}
		if len(keys) == 0 {
			skipReasons = append(skipReasons, fmt.Sprintf("%d:%s", physicalID, reason))
			continue
		}
		splitKeys = append(splitKeys, keys...)
	}

	splitKeys = sortAndDedupeAutoSplitKeys(splitKeys)
	if len(splitKeys) == 0 {
		if len(skipReasons) == 0 {
			return nil, "no split keys generated", nil
		}
		return nil, strings.Join(skipReasons, ","), nil
	}
	return splitKeys, "", nil
}

func (cfg autoSplitHotRegionConfig) withRegionCandidateRatio(rowsRatio float64) autoSplitHotRegionConfig {
	// Split each physical table's candidate budget evenly between full-range and TopN splitting.
	maxCandidatesPerMethod := int(float64(cfg.regionCandidateBudget) * rowsRatio * 0.5)
	if maxCandidatesPerMethod < 2 {
		maxCandidatesPerMethod = 2
	}
	cfg.maxFullRangeRegionsPerPhysical = min(cfg.maxFullRangeRegionsPerPhysical, maxCandidatesPerMethod)
	// N target regions need N-1 split keys, so TopN gets the same N-1 key budget as full-range.
	cfg.maxTopNKeysPerPhysical = min(cfg.maxTopNKeysPerPhysical, maxCandidatesPerMethod-1)
	return cfg
}

func getAutoSplitPhysicalTableIDs(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) []int64 {
	if tblInfo.GetPartitionInfo() == nil || idxInfo.Global {
		return []int64{tblInfo.ID}
	}
	return getPartitionIDs(tblInfo)
}

func planAutoSplitPhysicalIndexRegions(
	sctx sessionctx.Context,
	statsTbl *statistics.Table,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	physicalID int64,
	cfg autoSplitHotRegionConfig,
) ([][]byte, string, error) {
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

	offset := idxInfo.Columns[0].Offset
	if offset < 0 || offset >= len(tblInfo.Columns) {
		return nil, "leading column not found", nil
	}
	leadingCol := tblInfo.Columns[offset]
	colStats, loadNeeded, hasAnalyzed := statsTbl.ColumnIsLoadNeeded(leadingCol.ID, true)
	if !hasAnalyzed {
		return nil, "leading column stats missing or not analyzed", nil
	}
	var topN *statistics.TopN
	if loadNeeded {
		var err error
		topN, err = storage.TopNFromStorageWithPriority(sctx, physicalID, 0, leadingCol.ID, kv.PriorityNormal)
		if err != nil {
			return nil, "failed to load leading column TopN from storage", err
		}
	} else {
		topN = colStats.TopN
	}

	splitKeys := make([][]byte, 0)
	regionsCnt := min(
		int((statsTbl.RealtimeCount+cfg.rowsPerRegion-1)/cfg.rowsPerRegion),
		cfg.maxFullRangeRegionsPerPhysical)
	if regionsCnt > 1 {
		lower, upper := getSplitIdxFullRangeDatums(len(idxInfo.Columns))
		var err error
		splitKeys, err = getSplitIdxPhysicalKeysFromBound(
			sctx, tblInfo, idxInfo, physicalID, lower, upper, regionsCnt, splitKeys)
		if err != nil {
			return nil, "failed to build full-range split keys", err
		}
	}

	topNRows, err := buildAutoSplitTopNRows(sctx, statsTbl, topN, leadingCol, cfg)
	if err != nil {
		return nil, "failed to build TopN split keys", err
	}
	if len(topNRows) > 0 {
		splitKeys, err = getSplitIdxPhysicalKeysFromValueList(sctx, tblInfo, idxInfo, physicalID, topNRows, splitKeys)
		if err != nil {
			return nil, "failed to build TopN split keys", err
		}
	}

	splitKeys = sortAndDedupeAutoSplitKeys(splitKeys)
	if len(splitKeys) == 0 {
		return nil, "no split strategy matched", nil
	}
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
		convertedDatum, err := datum.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), &colInfo.FieldType)
		if err != nil {
			return nil, err
		}
		topNRows = append(topNRows, []types.Datum{convertedDatum})
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
