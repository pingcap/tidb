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
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
)

const (
	defaultAutoSplitHotRegionMinTableRows            = int64(1_000_000)
	defaultAutoSplitHotRegionRowsPerRegion           = int64(500_000)
	defaultAutoSplitHotRegionMaxRegionsPerPhysical   = 100
	defaultAutoSplitHotRegionMaxTopNKeysPerPhysical  = 100
	defaultAutoSplitHotRegionMaxSplitKeys            = 2560
	defaultAutoSplitHotRegionTopNMinCount            = uint64(500_000)
	defaultAutoSplitHotRegionTopNMinRatio            = 0.01
	defaultAutoSplitHotRegionMinStatsHealthy         = int64(80)
	defaultAutoSplitHotRegionMockMaxRegionsForTest   = 4
	defaultAutoSplitHotRegionMockMaxSplitKeysForTest = 32
)

type autoSplitStatsProvider interface {
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
}

type autoSplitHotRegionConfig struct {
	minTableRows           int64
	rowsPerRegion          int64
	maxRegionsPerPhysical  int
	maxTopNKeysPerPhysical int
	maxSplitKeys           int
	topNMinCount           uint64
	topNMinRatio           float64
	minStatsHealthy        int64
}

func getAutoSplitHotRegionConfig() autoSplitHotRegionConfig {
	cfg := autoSplitHotRegionConfig{
		minTableRows:           defaultAutoSplitHotRegionMinTableRows,
		rowsPerRegion:          defaultAutoSplitHotRegionRowsPerRegion,
		maxRegionsPerPhysical:  defaultAutoSplitHotRegionMaxRegionsPerPhysical,
		maxTopNKeysPerPhysical: defaultAutoSplitHotRegionMaxTopNKeysPerPhysical,
		maxSplitKeys:           defaultAutoSplitHotRegionMaxSplitKeys,
		topNMinCount:           defaultAutoSplitHotRegionTopNMinCount,
		topNMinRatio:           defaultAutoSplitHotRegionTopNMinRatio,
		minStatsHealthy:        defaultAutoSplitHotRegionMinStatsHealthy,
	}
	failpoint.Inject("mockAutoSplitHotRegionConfig", func(val failpoint.Value) {
		if minRows, ok := val.(int); ok && minRows > 0 {
			cfg.minTableRows = int64(minRows)
			cfg.rowsPerRegion = int64(minRows)
			cfg.maxRegionsPerPhysical = defaultAutoSplitHotRegionMockMaxRegionsForTest
			cfg.maxTopNKeysPerPhysical = defaultAutoSplitHotRegionMockMaxRegionsForTest
			cfg.maxSplitKeys = defaultAutoSplitHotRegionMockMaxSplitKeysForTest
			cfg.topNMinCount = uint64(minRows)
			cfg.topNMinRatio = 0
			cfg.minStatsHealthy = 0
		}
	})
	return cfg.normalize()
}

func (cfg autoSplitHotRegionConfig) normalize() autoSplitHotRegionConfig {
	if cfg.minTableRows < 1 {
		cfg.minTableRows = 1
	}
	if cfg.rowsPerRegion < 1 {
		cfg.rowsPerRegion = cfg.minTableRows
	}
	if cfg.maxRegionsPerPhysical < 2 {
		cfg.maxRegionsPerPhysical = 2
	}
	if cfg.maxTopNKeysPerPhysical < 0 {
		cfg.maxTopNKeysPerPhysical = 0
	}
	if cfg.maxSplitKeys < 1 {
		cfg.maxSplitKeys = 1
	}
	if cfg.topNMinRatio < 0 {
		cfg.topNMinRatio = 0
	}
	if cfg.minStatsHealthy < 0 {
		cfg.minStatsHealthy = 0
	}
	return cfg
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

	cfg = cfg.normalize()
	physicalIDs := getAutoSplitPhysicalTableIDs(tblInfo, idxInfo)
	splitKeys := make([][]byte, 0)
	skipReasons := make([]string, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		keys, reason, err := planAutoSplitPhysicalIndexRegions(sctx, statsProvider, tblInfo, idxInfo, physicalID, cfg)
		if err != nil {
			return nil, reason, err
		}
		if len(keys) == 0 {
			skipReasons = append(skipReasons, fmt.Sprintf("%d:%s", physicalID, reason))
			continue
		}
		splitKeys = append(splitKeys, keys...)
	}

	splitKeys = dedupeAutoSplitKeys(splitKeys, cfg.maxSplitKeys)
	if len(splitKeys) == 0 {
		if len(skipReasons) == 0 {
			return nil, "no split keys generated", nil
		}
		return nil, strings.Join(skipReasons, ","), nil
	}
	return splitKeys, "auto split keys generated", nil
}

func getAutoSplitPhysicalTableIDs(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) []int64 {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || idxInfo.Global {
		return []int64{tblInfo.ID}
	}
	physicalIDs := make([]int64, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		physicalIDs = append(physicalIDs, def.ID)
	}
	return physicalIDs
}

func planAutoSplitPhysicalIndexRegions(
	sctx sessionctx.Context,
	statsProvider autoSplitStatsProvider,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	physicalID int64,
	cfg autoSplitHotRegionConfig,
) ([][]byte, string, error) {
	statsTbl := statsProvider.GetPhysicalTableStats(physicalID, tblInfo)
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

	leadingCol, ok := getAutoSplitLeadingColumn(tblInfo, idxInfo)
	if !ok {
		return nil, "leading column not found", nil
	}
	colStats := statsTbl.GetCol(leadingCol.ID)
	if colStats == nil || !colStats.IsAnalyzed() || !colStats.IsFullLoad() {
		loadedColStats, err := storage.LoadColumnStatsFromStorage(sctx, physicalID, leadingCol, tblInfo.PKIsHandle, true, kv.PriorityNormal)
		if err != nil {
			return nil, "failed to load leading column stats from storage", err
		}
		colStats = loadedColStats
		if colStats == nil || !colStats.IsAnalyzed() || !colStats.IsFullLoad() {
			return nil, "leading column stats missing or not fully loaded", nil
		}
	}

	splitKeys := make([][]byte, 0)
	regionsCnt := calcAutoSplitRegionCount(statsTbl.RealtimeCount, cfg)
	if regionsCnt > 1 {
		lower, upper := getAutoSplitIndexBoundDatums(idxInfo)
		var err error
		splitKeys, err = getSplitIdxPhysicalKeysFromBound(
			sctx, tblInfo, idxInfo, physicalID, lower, upper, regionsCnt, splitKeys)
		if err != nil {
			return nil, "failed to build full-range split keys", err
		}
	}

	topNRows, err := buildAutoSplitTopNRows(sctx, statsTbl, colStats, leadingCol, cfg)
	if err != nil {
		return nil, "failed to build TopN split keys", err
	}
	if len(topNRows) > 0 {
		splitKeys, err = getSplitIdxPhysicalKeysFromValueList(sctx, tblInfo, idxInfo, physicalID, topNRows, splitKeys)
		if err != nil {
			return nil, "failed to build TopN split keys", err
		}
	}

	splitKeys = dedupeAutoSplitKeys(splitKeys, cfg.maxSplitKeys)
	if len(splitKeys) == 0 {
		return nil, "no split strategy matched", nil
	}
	return splitKeys, "split keys generated", nil
}

func getAutoSplitLeadingColumn(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) (*model.ColumnInfo, bool) {
	if len(idxInfo.Columns) == 0 {
		return nil, false
	}
	offset := idxInfo.Columns[0].Offset
	if offset < 0 || offset >= len(tblInfo.Columns) {
		return nil, false
	}
	return tblInfo.Columns[offset], true
}

func calcAutoSplitRegionCount(rowCount int64, cfg autoSplitHotRegionConfig) int {
	regionsCnt := int((rowCount + cfg.rowsPerRegion - 1) / cfg.rowsPerRegion)
	if regionsCnt > cfg.maxRegionsPerPhysical {
		regionsCnt = cfg.maxRegionsPerPhysical
	}
	if regionsCnt < 2 {
		return 0
	}
	return regionsCnt
}

func getAutoSplitIndexBoundDatums(idxInfo *model.IndexInfo) (lowerVals []types.Datum, upperVals []types.Datum) {
	lowerVals = make([]types.Datum, 0, len(idxInfo.Columns))
	upperVals = make([]types.Datum, 0, len(idxInfo.Columns))
	for range idxInfo.Columns {
		lowerVals = append(lowerVals, types.MinNotNullDatum())
		upperVals = append(upperVals, types.MaxValueDatum())
	}
	return lowerVals, upperVals
}

func buildAutoSplitTopNRows(
	sctx sessionctx.Context,
	statsTbl *statistics.Table,
	colStats *statistics.Column,
	colInfo *model.ColumnInfo,
	cfg autoSplitHotRegionConfig,
) ([][]types.Datum, error) {
	if cfg.maxTopNKeysPerPhysical == 0 {
		logutil.DDLLogger().Info("skip auto split hot index TopN split keys",
			zap.String("reason", "max TopN split keys is 0"),
			zap.Int64("physicalTableID", statsTbl.PhysicalID),
			zap.Int64("columnID", colInfo.ID),
			zap.String("column", colInfo.Name.L),
			zap.Int64("realtimeCount", statsTbl.RealtimeCount))
		return nil, nil
	}
	if colStats.TopN == nil {
		logutil.DDLLogger().Info("skip auto split hot index TopN split keys",
			zap.String("reason", "TopN is nil"),
			zap.Int64("physicalTableID", statsTbl.PhysicalID),
			zap.Int64("columnID", colInfo.ID),
			zap.String("column", colInfo.Name.L),
			zap.Int64("realtimeCount", statsTbl.RealtimeCount),
			zap.Bool("columnStatsAnalyzed", colStats.IsAnalyzed()),
			zap.Bool("columnStatsFullLoaded", colStats.IsFullLoad()))
		return nil, nil
	}
	topNNum := colStats.TopN.Num()
	if topNNum == 0 {
		logutil.DDLLogger().Info("skip auto split hot index TopN split keys",
			zap.String("reason", "TopN is empty"),
			zap.Int64("physicalTableID", statsTbl.PhysicalID),
			zap.Int64("columnID", colInfo.ID),
			zap.String("column", colInfo.Name.L),
			zap.Int64("realtimeCount", statsTbl.RealtimeCount),
			zap.Bool("columnStatsAnalyzed", colStats.IsAnalyzed()),
			zap.Bool("columnStatsFullLoaded", colStats.IsFullLoad()))
		return nil, nil
	}

	topNCandidates := make([]statistics.TopNMeta, 0, topNNum)
	filteredByCount := 0
	filteredByRatio := 0
	var maxTopNCount uint64
	var maxTopNRatio float64
	for _, topNItem := range colStats.TopN.TopN {
		if topNItem.Count > maxTopNCount {
			maxTopNCount = topNItem.Count
		}
		if statsTbl.RealtimeCount > 0 {
			maxTopNRatio = max(maxTopNRatio, float64(topNItem.Count)/float64(statsTbl.RealtimeCount))
		}
		if topNItem.Count < cfg.topNMinCount {
			filteredByCount++
			continue
		}
		if cfg.topNMinRatio > 0 && statsTbl.RealtimeCount > 0 &&
			float64(topNItem.Count)/float64(statsTbl.RealtimeCount) < cfg.topNMinRatio {
			filteredByRatio++
			continue
		}
		topNCandidates = append(topNCandidates, topNItem)
	}
	if len(topNCandidates) == 0 {
		logutil.DDLLogger().Info("skip auto split hot index TopN split keys",
			zap.String("reason", "no TopN item reaches threshold"),
			zap.Int64("physicalTableID", statsTbl.PhysicalID),
			zap.Int64("columnID", colInfo.ID),
			zap.String("column", colInfo.Name.L),
			zap.Int64("realtimeCount", statsTbl.RealtimeCount),
			zap.Int("topNItems", topNNum),
			zap.Uint64("topNMinCount", cfg.topNMinCount),
			zap.Float64("topNMinRatio", cfg.topNMinRatio),
			zap.Int("filteredByCount", filteredByCount),
			zap.Int("filteredByRatio", filteredByRatio),
			zap.Uint64("maxTopNCount", maxTopNCount),
			zap.Float64("maxTopNRatio", maxTopNRatio))
		return nil, nil
	}
	slices.SortFunc(topNCandidates, func(a, b statistics.TopNMeta) int {
		if a.Count > b.Count {
			return -1
		}
		if a.Count < b.Count {
			return 1
		}
		return bytes.Compare(a.Encoded, b.Encoded)
	})

	topNRows := make([][]types.Datum, 0, min(cfg.maxTopNKeysPerPhysical, len(topNCandidates)))
	for _, topNItem := range topNCandidates {
		datum, err := decodeAutoSplitTopNDatum(sctx, topNItem, colInfo)
		if err != nil {
			logutil.DDLLogger().Warn("failed to build auto split hot index TopN split key",
				zap.String("reason", "failed to decode TopN datum"),
				zap.Int64("physicalTableID", statsTbl.PhysicalID),
				zap.Int64("columnID", colInfo.ID),
				zap.String("column", colInfo.Name.L),
				zap.Int64("realtimeCount", statsTbl.RealtimeCount),
				zap.Uint64("topNCount", topNItem.Count),
				zap.Error(err))
			return nil, err
		}
		convertedDatum, err := datum.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), &colInfo.FieldType)
		if err != nil {
			logutil.DDLLogger().Warn("failed to build auto split hot index TopN split key",
				zap.String("reason", "failed to convert TopN datum"),
				zap.Int64("physicalTableID", statsTbl.PhysicalID),
				zap.Int64("columnID", colInfo.ID),
				zap.String("column", colInfo.Name.L),
				zap.Int64("realtimeCount", statsTbl.RealtimeCount),
				zap.Uint64("topNCount", topNItem.Count),
				zap.Error(err))
			return nil, err
		}
		topNRows = append(topNRows, []types.Datum{convertedDatum})
		if len(topNRows) >= cfg.maxTopNKeysPerPhysical {
			break
		}
	}
	logutil.DDLLogger().Info("build auto split hot index TopN split keys",
		zap.Int64("physicalTableID", statsTbl.PhysicalID),
		zap.Int64("columnID", colInfo.ID),
		zap.String("column", colInfo.Name.L),
		zap.Int64("realtimeCount", statsTbl.RealtimeCount),
		zap.Int("topNItems", topNNum),
		zap.Uint64("topNMinCount", cfg.topNMinCount),
		zap.Float64("topNMinRatio", cfg.topNMinRatio),
		zap.Int("filteredByCount", filteredByCount),
		zap.Int("filteredByRatio", filteredByRatio),
		zap.Int("topNCandidates", len(topNCandidates)),
		zap.Int("selectedTopNKeys", len(topNRows)),
		zap.Bool("truncatedByLimit", len(topNCandidates) > len(topNRows)),
		zap.Uint64("maxTopNCount", maxTopNCount),
		zap.Float64("maxTopNRatio", maxTopNRatio))
	return topNRows, nil
}

func decodeAutoSplitTopNDatum(
	sctx sessionctx.Context,
	topNItem statistics.TopNMeta,
	colInfo *model.ColumnInfo,
) (types.Datum, error) {
	tp := colInfo.GetType()
	if types.IsTypeTime(tp) {
		_, datum, err := codec.DecodeAsDateTime(topNItem.Encoded, tp, sctx.GetSessionVars().Location())
		return datum, err
	}
	if types.IsTypeFloat(tp) {
		_, datum, err := codec.DecodeAsFloat32(topNItem.Encoded, tp)
		return datum, err
	}
	_, datum, err := codec.DecodeOne(topNItem.Encoded)
	return datum, err
}

func dedupeAutoSplitKeys(keys [][]byte, limit int) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	slices.SortFunc(keys, bytes.Compare)
	deduped := keys[:0]
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		if len(deduped) == 0 || !bytes.Equal(deduped[len(deduped)-1], key) {
			deduped = append(deduped, key)
		}
	}
	if limit > 0 && len(deduped) > limit {
		return deduped[:limit]
	}
	return deduped
}
