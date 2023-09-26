// Copyright 2018 PingCAP, Inc.
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

package handle

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle/globalstats"
	handle_metrics "github.com/pingcap/tidb/statistics/handle/metrics"
	"github.com/pingcap/tidb/statistics/handle/storage"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// DumpStatsToJSON dumps statistic to json.
func (h *Handle) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo,
	historyStatsExec sqlexec.RestrictedSQLExecutor, dumpPartitionStats bool) (*storage.JSONTable, error) {
	var snapshot uint64
	if historyStatsExec != nil {
		sctx := historyStatsExec.(sessionctx.Context)
		snapshot = sctx.GetSessionVars().SnapshotTS
	}
	return h.DumpStatsToJSONBySnapshot(dbName, tableInfo, snapshot, dumpPartitionStats)
}

// DumpHistoricalStatsBySnapshot dumped json tables from mysql.stats_meta_history and mysql.stats_history.
// As implemented in getTableHistoricalStatsToJSONWithFallback, if historical stats are nonexistent, it will fall back
// to the latest stats, and these table names (and partition names) will be returned in fallbackTbls.
func (h *Handle) DumpHistoricalStatsBySnapshot(
	dbName string,
	tableInfo *model.TableInfo,
	snapshot uint64,
) (
	jt *storage.JSONTable,
	fallbackTbls []string,
	err error,
) {
	historicalStatsEnabled, err := h.CheckHistoricalStatsEnable()
	if err != nil {
		return nil, nil, errors.Errorf("check %v failed: %v", variable.TiDBEnableHistoricalStats, err)
	}
	if !historicalStatsEnabled {
		return nil, nil, errors.Errorf("%v should be enabled", variable.TiDBEnableHistoricalStats)
	}

	defer func() {
		if err == nil {
			handle_metrics.DumpHistoricalStatsSuccessCounter.Inc()
		} else {
			handle_metrics.DumpHistoricalStatsFailedCounter.Inc()
		}
	}()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		jt, fallback, err := h.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, tableInfo.ID, snapshot)
		if fallback {
			fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s", dbName, tableInfo.Name.O))
		}
		return jt, fallbackTbls, err
	}
	jsonTbl := &storage.JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*storage.JSONTable, len(pi.Definitions)),
	}
	for _, def := range pi.Definitions {
		tbl, fallback, err := h.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, def.ID, snapshot)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if fallback {
			fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s %s", dbName, tableInfo.Name.O, def.Name.O))
		}
		jsonTbl.Partitions[def.Name.L] = tbl
	}
	tbl, fallback, err := h.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if fallback {
		fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s global", dbName, tableInfo.Name.O))
	}
	// dump its global-stats if existed
	if tbl != nil {
		jsonTbl.Partitions[globalstats.TiDBGlobalStats] = tbl
	}
	return jsonTbl, fallbackTbls, nil
}

// DumpStatsToJSONBySnapshot dumps statistic to json.
func (h *Handle) DumpStatsToJSONBySnapshot(dbName string, tableInfo *model.TableInfo, snapshot uint64, dumpPartitionStats bool) (*storage.JSONTable, error) {
	pruneMode, err := h.GetCurrentPruneMode()
	if err != nil {
		return nil, err
	}
	isDynamicMode := variable.PartitionPruneMode(pruneMode) == variable.Dynamic
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return h.tableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	}
	jsonTbl := &storage.JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*storage.JSONTable, len(pi.Definitions)),
	}
	// dump partition stats only if in static mode or enable dumpPartitionStats flag in dynamic mode
	if !isDynamicMode || dumpPartitionStats {
		for _, def := range pi.Definitions {
			tbl, err := h.tableStatsToJSON(dbName, tableInfo, def.ID, snapshot)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if tbl == nil {
				continue
			}
			jsonTbl.Partitions[def.Name.L] = tbl
		}
	}
	// dump its global-stats if existed
	tbl, err := h.tableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tbl != nil {
		jsonTbl.Partitions[globalstats.TiDBGlobalStats] = tbl
	}
	return jsonTbl, nil
}

// getTableHistoricalStatsToJSONWithFallback try to get table historical stats, if not exist, directly fallback to the
// latest stats, and the second return value would be true.
func (h *Handle) getTableHistoricalStatsToJSONWithFallback(
	dbName string,
	tableInfo *model.TableInfo,
	physicalID int64,
	snapshot uint64,
) (
	*storage.JSONTable,
	bool,
	error,
) {
	jt, exist, err := h.tableHistoricalStatsToJSON(physicalID, snapshot)
	if err != nil {
		return nil, false, err
	}
	if !exist {
		jt, err = h.tableStatsToJSON(dbName, tableInfo, physicalID, 0)
		fallback := true
		if snapshot == 0 {
			fallback = false
		}
		return jt, fallback, err
	}
	return jt, false, nil
}

func (h *Handle) tableHistoricalStatsToJSON(physicalID int64, snapshot uint64) (*storage.JSONTable, bool, error) {
	reader, err := h.getGlobalStatsReader(0)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		err1 := h.releaseGlobalStatsReader(reader)
		if err == nil && err1 != nil {
			err = err1
		}
	}()

	// get meta version
	rows, _, err := reader.Read("select distinct version from mysql.stats_meta_history where table_id = %? and version <= %? order by version desc limit 1", physicalID, snapshot)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	if len(rows) < 1 {
		logutil.BgLogger().Warn("failed to get records of stats_meta_history",
			zap.Int64("table-id", physicalID),
			zap.Uint64("snapshotTS", snapshot))
		return nil, false, nil
	}
	statsMetaVersion := rows[0].GetInt64(0)
	// get stats meta
	rows, _, err = reader.Read("select modify_count, count from mysql.stats_meta_history where table_id = %? and version = %?", physicalID, statsMetaVersion)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	modifyCount, count := rows[0].GetInt64(0), rows[0].GetInt64(1)

	// get stats version
	rows, _, err = reader.Read("select distinct version from mysql.stats_history where table_id = %? and version <= %? order by version desc limit 1", physicalID, snapshot)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	if len(rows) < 1 {
		logutil.BgLogger().Warn("failed to get record of stats_history",
			zap.Int64("table-id", physicalID),
			zap.Uint64("snapshotTS", snapshot))
		return nil, false, nil
	}
	statsVersion := rows[0].GetInt64(0)

	// get stats
	rows, _, err = reader.Read("select stats_data from mysql.stats_history where table_id = %? and version = %? order by seq_no", physicalID, statsVersion)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	blocks := make([][]byte, 0)
	for _, row := range rows {
		blocks = append(blocks, row.GetBytes(0))
	}
	jsonTbl, err := storage.BlocksToJSONTable(blocks)
	if err != nil {
		return nil, false, errors.AddStack(err)
	}
	jsonTbl.Count = count
	jsonTbl.ModifyCount = modifyCount
	jsonTbl.IsHistoricalStats = true
	return jsonTbl, true, nil
}

func (h *Handle) tableStatsToJSON(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*storage.JSONTable, error) {
	tbl, err := h.TableStatsFromStorage(tableInfo, physicalID, true, snapshot)
	if err != nil || tbl == nil {
		return nil, err
	}
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		tbl.Version, tbl.ModifyCount, tbl.RealtimeCount, err = storage.StatsMetaByTableIDFromStorage(sctx, physicalID, snapshot)
		return err
	})
	if err != nil {
		return nil, err
	}
	jsonTbl, err := storage.GenJSONTableFromStats(dbName, tableInfo, tbl)
	if err != nil {
		return nil, err
	}
	return jsonTbl, nil
}

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
func (h *Handle) LoadStatsFromJSON(is infoschema.InfoSchema, jsonTbl *storage.JSONTable) error {
	table, err := is.TableByName(model.NewCIStr(jsonTbl.DatabaseName), model.NewCIStr(jsonTbl.TableName))
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil || jsonTbl.Partitions == nil {
		err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, jsonTbl)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		for _, def := range pi.Definitions {
			tbl := jsonTbl.Partitions[def.Name.L]
			if tbl == nil {
				continue
			}
			err := h.loadStatsFromJSON(tableInfo, def.ID, tbl)
			if err != nil {
				return errors.Trace(err)
			}
		}
		// load global-stats if existed
		if globalStats, ok := jsonTbl.Partitions[globalstats.TiDBGlobalStats]; ok {
			if err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, globalStats); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return errors.Trace(h.Update(is))
}

func (h *Handle) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *storage.JSONTable) error {
	tbl, err := storage.TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range tbl.Columns {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.GetStatsVer()), 1, false, StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.GetStatsVer()), 1, false, StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = h.SaveExtendedStatsToStorage(tbl.PhysicalID, tbl.ExtendedStats, true)
	if err != nil {
		return errors.Trace(err)
	}
	return h.SaveMetaToStorage(tbl.PhysicalID, tbl.RealtimeCount, tbl.ModifyCount, StatsMetaHistorySourceLoadStats)
}
