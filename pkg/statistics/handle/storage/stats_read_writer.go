// Copyright 2023 PingCAP, Inc.
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

package storage

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	handle_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// statsReadWriter implements the util.StatsReadWriter interface.
type statsReadWriter struct {
	statsHandler util.StatsHandle
}

// NewStatsReadWriter creates a new StatsReadWriter.
func NewStatsReadWriter(statsHandler util.StatsHandle) util.StatsReadWriter {
	return &statsReadWriter{statsHandler: statsHandler}
}

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func (s *statsReadWriter) StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		count, modifyCount, _, err = StatsMetaCountAndModifyCount(sctx, tableID)
		return err
	}, util.FlagWrapTxn)
	return
}

// TableStatsFromStorage loads table stats info from storage.
func (s *statsReadWriter) TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		var ok bool
		statsTbl, ok = s.statsHandler.Get(physicalID)
		if !ok {
			statsTbl = nil
		}
		statsTbl, err = TableStatsFromStorage(sctx, snapshot, tableInfo, physicalID, loadAll, s.statsHandler.Lease(), statsTbl)
		return err
	}, util.FlagWrapTxn)
	return
}

// SaveStatsToStorage saves the stats to storage.
// If count is negative, both count and modify count would not be used and not be written to the table. Unless, corresponding
// fields in the stats_meta table will be updated.
// TODO: refactor to reduce the number of parameters
func (s *statsReadWriter) SaveStatsToStorage(tableID int64, count, modifyCount int64, isIndex int, hg *statistics.Histogram,
	cms *statistics.CMSketch, topN *statistics.TopN, statsVersion int, isAnalyzed int64, updateAnalyzeTime bool, source string) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveStatsToStorage(sctx, tableID,
			count, modifyCount, isIndex, hg, cms, topN, statsVersion, isAnalyzed, updateAnalyzeTime)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, source)
	}
	return
}

// SaveMetaToStorage saves stats meta to the storage.
func (s *statsReadWriter) SaveMetaToStorage(tableID, count, modifyCount int64, source string) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveMetaToStorage(sctx, tableID, count, modifyCount)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, source)
	}
	return
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (s *statsReadWriter) InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = InsertExtendedStats(sctx, s.statsHandler, statsName, colIDs, tp, tableID, ifNotExists)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats")
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (s *statsReadWriter) MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = MarkExtendedStatsDeleted(sctx, s.statsHandler, statsName, tableID, ifExists)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats")
	}
	return
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func (s *statsReadWriter) SaveExtendedStatsToStorage(tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveExtendedStatsToStorage(sctx, tableID, extStats, isLoad)
		return err
	})
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats")
	}
	return
}

// SaveStatsFromJSON saves stats from JSON to the storage.
func (s *statsReadWriter) SaveStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTblI interface{}) error {
	jsonTbl := jsonTblI.(*util.JSONTable)
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range tbl.Columns {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = s.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.GetStatsVer()), 1, false, "load stats")
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = s.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.GetStatsVer()), 1, false, "load stats")
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = s.SaveExtendedStatsToStorage(tbl.PhysicalID, tbl.ExtendedStats, true)
	if err != nil {
		return errors.Trace(err)
	}
	return s.SaveMetaToStorage(tbl.PhysicalID, tbl.RealtimeCount, tbl.ModifyCount, "load stats")
}

// LoadNeededHistograms will load histograms for those needed columns/indices.
func (s *statsReadWriter) LoadNeededHistograms() (err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
		return LoadNeededHistograms(sctx, s.statsHandler, loadFMSketch)
	}, util.FlagWrapTxn)
	return err
}

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
func (s *statsReadWriter) ReloadExtendedStatistics() error {
	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		tables := make([]*statistics.Table, 0, s.statsHandler.Len())
		for _, tbl := range s.statsHandler.Values() {
			t, err := ExtendedStatsFromStorage(sctx, tbl.Copy(), tbl.PhysicalID, true)
			if err != nil {
				return err
			}
			tables = append(tables, t)
		}
		s.statsHandler.UpdateStatsCache(tables, nil)
		return nil
	}, util.FlagWrapTxn)
}

// DumpStatsToJSON dumps statistic to json.
func (s *statsReadWriter) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo,
	historyStatsExec sqlexec.RestrictedSQLExecutor, dumpPartitionStats bool) (*util.JSONTable, error) {
	var snapshot uint64
	if historyStatsExec != nil {
		sctx := historyStatsExec.(sessionctx.Context)
		snapshot = sctx.GetSessionVars().SnapshotTS
	}
	return s.DumpStatsToJSONBySnapshot(dbName, tableInfo, snapshot, dumpPartitionStats)
}

// DumpHistoricalStatsBySnapshot dumped json tables from mysql.stats_meta_history and mysql.stats_history.
// As implemented in getTableHistoricalStatsToJSONWithFallback, if historical stats are nonexistent, it will fall back
// to the latest stats, and these table names (and partition names) will be returned in fallbackTbls.
func (s *statsReadWriter) DumpHistoricalStatsBySnapshot(
	dbName string,
	tableInfo *model.TableInfo,
	snapshot uint64,
) (
	jt *util.JSONTable,
	fallbackTbls []string,
	err error,
) {
	historicalStatsEnabled, err := s.statsHandler.CheckHistoricalStatsEnable()
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
		jt, fallback, err := s.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, tableInfo.ID, snapshot)
		if fallback {
			fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s", dbName, tableInfo.Name.O))
		}
		return jt, fallbackTbls, err
	}
	jsonTbl := &util.JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*util.JSONTable, len(pi.Definitions)),
	}
	for _, def := range pi.Definitions {
		tbl, fallback, err := s.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, def.ID, snapshot)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if fallback {
			fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s %s", dbName, tableInfo.Name.O, def.Name.O))
		}
		jsonTbl.Partitions[def.Name.L] = tbl
	}
	tbl, fallback, err := s.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if fallback {
		fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s global", dbName, tableInfo.Name.O))
	}
	// dump its global-stats if existed
	if tbl != nil {
		jsonTbl.Partitions[util.TiDBGlobalStats] = tbl
	}
	return jsonTbl, fallbackTbls, nil
}

// DumpStatsToJSONBySnapshot dumps statistic to json.
func (s *statsReadWriter) DumpStatsToJSONBySnapshot(dbName string, tableInfo *model.TableInfo, snapshot uint64, dumpPartitionStats bool) (*util.JSONTable, error) {
	pruneMode, err := s.statsHandler.GetCurrentPruneMode()
	if err != nil {
		return nil, err
	}
	isDynamicMode := variable.PartitionPruneMode(pruneMode) == variable.Dynamic
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	}
	jsonTbl := &util.JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*util.JSONTable, len(pi.Definitions)),
	}
	// dump partition stats only if in static mode or enable dumpPartitionStats flag in dynamic mode
	if !isDynamicMode || dumpPartitionStats {
		for _, def := range pi.Definitions {
			tbl, err := s.TableStatsToJSON(dbName, tableInfo, def.ID, snapshot)
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
	tbl, err := s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tbl != nil {
		jsonTbl.Partitions[util.TiDBGlobalStats] = tbl
	}
	return jsonTbl, nil
}

// getTableHistoricalStatsToJSONWithFallback try to get table historical stats, if not exist, directly fallback to the
// latest stats, and the second return value would be true.
func (s *statsReadWriter) getTableHistoricalStatsToJSONWithFallback(
	dbName string,
	tableInfo *model.TableInfo,
	physicalID int64,
	snapshot uint64,
) (
	*util.JSONTable,
	bool,
	error,
) {
	jt, exist, err := s.tableHistoricalStatsToJSON(physicalID, snapshot)
	if err != nil {
		return nil, false, err
	}
	if !exist {
		jt, err = s.TableStatsToJSON(dbName, tableInfo, physicalID, 0)
		fallback := true
		if snapshot == 0 {
			fallback = false
		}
		return jt, fallback, err
	}
	return jt, false, nil
}

func (s *statsReadWriter) tableHistoricalStatsToJSON(physicalID int64, snapshot uint64) (jt *util.JSONTable, exist bool, err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		jt, exist, err = TableHistoricalStatsToJSON(sctx, physicalID, snapshot)
		return err
	}, util.FlagWrapTxn)
	return
}

// TableStatsToJSON dumps statistic to json.
func (s *statsReadWriter) TableStatsToJSON(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*util.JSONTable, error) {
	tbl, err := s.TableStatsFromStorage(tableInfo, physicalID, true, snapshot)
	if err != nil || tbl == nil {
		return nil, err
	}
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		tbl.Version, tbl.ModifyCount, tbl.RealtimeCount, err = StatsMetaByTableIDFromStorage(sctx, physicalID, snapshot)
		return err
	})
	if err != nil {
		return nil, err
	}
	jsonTbl, err := GenJSONTableFromStats(dbName, tableInfo, tbl)
	if err != nil {
		return nil, err
	}
	return jsonTbl, nil
}

// TestLoadStatsErr is only for test.
type TestLoadStatsErr struct{}

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
// In final, it will also udpate the stats cache.
func (s *statsReadWriter) LoadStatsFromJSON(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *util.JSONTable, concurrencyForPartition uint8) error {
	if err := s.LoadStatsFromJSONNoUpdate(ctx, is, jsonTbl, concurrencyForPartition); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(s.statsHandler.Update(is))
}

// LoadStatsFromJSONNoUpdate will load statistic from JSONTable, and save it to the storage.
func (s *statsReadWriter) LoadStatsFromJSONNoUpdate(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *util.JSONTable, concurrencyForPartition uint8) error {
	nCPU := uint8(runtime.GOMAXPROCS(0))
	if concurrencyForPartition == 0 {
		concurrencyForPartition = nCPU / 2 // default
	}
	if concurrencyForPartition > nCPU {
		concurrencyForPartition = nCPU // for safety
	}

	table, err := is.TableByName(model.NewCIStr(jsonTbl.DatabaseName), model.NewCIStr(jsonTbl.TableName))
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil || jsonTbl.Partitions == nil {
		err := s.loadStatsFromJSON(tableInfo, tableInfo.ID, jsonTbl)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// load partition statistics concurrently
		taskCh := make(chan model.PartitionDefinition, len(pi.Definitions))
		for _, def := range pi.Definitions {
			taskCh <- def
		}
		close(taskCh)
		var wg sync.WaitGroup
		e := new(atomic.Pointer[error])
		for i := 0; i < int(concurrencyForPartition); i++ {
			wg.Add(1)
			s.statsHandler.GPool().Go(func() {
				defer func() {
					if r := recover(); r != nil {
						err := fmt.Errorf("%v", r)
						e.CompareAndSwap(nil, &err)
					}
					wg.Done()
				}()

				for def := range taskCh {
					tbl := jsonTbl.Partitions[def.Name.L]
					if tbl == nil {
						continue
					}

					loadFunc := s.loadStatsFromJSON
					if intest.InTest && ctx.Value(TestLoadStatsErr{}) != nil {
						loadFunc = ctx.Value(TestLoadStatsErr{}).(func(*model.TableInfo, int64, *util.JSONTable) error)
					}

					err := loadFunc(tableInfo, def.ID, tbl)
					if err != nil {
						e.CompareAndSwap(nil, &err)
						return
					}
					if e.Load() != nil {
						return
					}
				}
			})
		}
		wg.Wait()
		if e.Load() != nil {
			return *e.Load()
		}

		// load global-stats if existed
		if globalStats, ok := jsonTbl.Partitions[util.TiDBGlobalStats]; ok {
			if err := s.loadStatsFromJSON(tableInfo, tableInfo.ID, globalStats); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (s *statsReadWriter) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *util.JSONTable) error {
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range tbl.Columns {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = s.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.GetStatsVer()), 1, false, util.StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = s.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.GetStatsVer()), 1, false, util.StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = s.SaveExtendedStatsToStorage(tbl.PhysicalID, tbl.ExtendedStats, true)
	if err != nil {
		return errors.Trace(err)
	}
	return s.SaveMetaToStorage(tbl.PhysicalID, tbl.RealtimeCount, tbl.ModifyCount, util.StatsMetaHistorySourceLoadStats)
}
