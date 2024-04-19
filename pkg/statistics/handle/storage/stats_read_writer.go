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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	handle_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// statsReadWriter implements the util.StatsReadWriter interface.
type statsReadWriter struct {
	statsHandler statstypes.StatsHandle
}

// NewStatsReadWriter creates a new StatsReadWriter.
func NewStatsReadWriter(statsHandler statstypes.StatsHandle) statstypes.StatsReadWriter {
	return &statsReadWriter{statsHandler: statsHandler}
}

// InsertColStats2KV insert a record to stats_histograms with distinct_count 1 and insert a bucket to stats_buckets with default value.
// This operation also updates version.
func (s *statsReadWriter) InsertColStats2KV(physicalID int64, colInfos []*model.ColumnInfo) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			s.statsHandler.RecordHistoricalStatsMeta(physicalID, statsVer, util.StatsMetaHistorySourceSchemaChange, false)
		}
	}()

	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		startTS, err := util.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}

		// First of all, we update the version.
		_, err = util.Exec(sctx, "update mysql.stats_meta set version = %? where table_id = %?", startTS, physicalID)
		if err != nil {
			return err
		}
		statsVer = startTS
		// If we didn't update anything by last SQL, it means the stats of this table does not exist.
		if sctx.GetSessionVars().StmtCtx.AffectedRows() > 0 {
			// By this step we can get the count of this table, then we can sure the count and repeats of bucket.
			var rs sqlexec.RecordSet
			rs, err = util.Exec(sctx, "select count from mysql.stats_meta where table_id = %?", physicalID)
			if err != nil {
				return err
			}
			defer terror.Call(rs.Close)
			req := rs.NewChunk(nil)
			err = rs.Next(context.Background(), req)
			if err != nil {
				return err
			}
			count := req.GetRow(0).GetInt64(0)
			for _, colInfo := range colInfos {
				value := types.NewDatum(colInfo.GetOriginDefaultValue())
				value, err = value.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), &colInfo.FieldType)
				if err != nil {
					return err
				}
				if value.IsNull() {
					// If the adding column has default value null, all the existing rows have null value on the newly added column.
					if _, err := util.Exec(sctx, "insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count, null_count) values (%?, %?, 0, %?, 0, %?)", startTS, physicalID, colInfo.ID, count); err != nil {
						return err
					}
				} else {
					// If this stats exists, we insert histogram meta first, the distinct_count will always be one.
					if _, err := util.Exec(sctx, "insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count, tot_col_size) values (%?, %?, 0, %?, 1, %?)", startTS, physicalID, colInfo.ID, int64(len(value.GetBytes()))*count); err != nil {
						return err
					}
					value, err = value.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), types.NewFieldType(mysql.TypeBlob))
					if err != nil {
						return err
					}
					// There must be only one bucket for this new column and the value is the default value.
					if _, err := util.Exec(sctx, "insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, repeats, count, lower_bound, upper_bound) values (%?, 0, %?, 0, %?, %?, %?, %?)", physicalID, colInfo.ID, count, count, value.GetBytes(), value.GetBytes()); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}, util.FlagWrapTxn)
}

// InsertTableStats2KV inserts a record standing for a new table to stats_meta and inserts some records standing for the
// new columns and indices which belong to this table.
func (s *statsReadWriter) InsertTableStats2KV(info *model.TableInfo, physicalID int64) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			s.statsHandler.RecordHistoricalStatsMeta(physicalID, statsVer, util.StatsMetaHistorySourceSchemaChange, false)
		}
	}()

	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		startTS, err := util.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}
		if _, err := util.Exec(sctx, "insert into mysql.stats_meta (version, table_id) values(%?, %?)", startTS, physicalID); err != nil {
			return err
		}
		statsVer = startTS
		for _, col := range info.Columns {
			if _, err := util.Exec(sctx, "insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%?, 0, %?, 0, %?)", physicalID, col.ID, startTS); err != nil {
				return err
			}
		}
		for _, idx := range info.Indices {
			if _, err := util.Exec(sctx, "insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%?, 1, %?, 0, %?)", physicalID, idx.ID, startTS); err != nil {
				return err
			}
		}
		return nil
	}, util.FlagWrapTxn)
}

// ChangeGlobalStatsID changes the table ID in global-stats to the new table ID.
func (s *statsReadWriter) ChangeGlobalStatsID(from, to int64) (err error) {
	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		for _, table := range []string{"stats_meta", "stats_top_n", "stats_fm_sketch", "stats_buckets", "stats_histograms", "column_stats_usage"} {
			_, err = util.Exec(sctx, "update mysql."+table+" set table_id = %? where table_id = %?", to, from)
			if err != nil {
				return err
			}
		}
		return nil
	}, util.FlagWrapTxn)
}

// UpdateStatsMetaVersionForGC update the version of mysql.stats_meta.
// See more details in the interface definition.
func (s *statsReadWriter) UpdateStatsMetaVersionForGC(physicalID int64) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			s.statsHandler.RecordHistoricalStatsMeta(physicalID, statsVer, util.StatsMetaHistorySourceSchemaChange, false)
		}
	}()

	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		startTS, err := util.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}
		if _, err := util.Exec(
			sctx,
			"update mysql.stats_meta set version=%? where table_id =%?",
			startTS, physicalID,
		); err != nil {
			return err
		}
		statsVer = startTS
		return nil
	}, util.FlagWrapTxn)
}

// UpdateStatsVersion will set statistics version to the newest TS,
// then tidb-server will reload automatic.
func (s *statsReadWriter) UpdateStatsVersion() error {
	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		return UpdateStatsVersion(sctx)
	}, util.FlagWrapTxn)
}

// SaveTableStatsToStorage saves the stats of a table to storage.
func (s *statsReadWriter) SaveTableStatsToStorage(results *statistics.AnalyzeResults, analyzeSnapshot bool, source string) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveTableStatsToStorage(sctx, results, analyzeSnapshot)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		tableID := results.TableID.GetStatisticsID()
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, source, true)
	}
	return err
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
func (s *statsReadWriter) SaveStatsToStorage(
	tableID int64,
	count, modifyCount int64,
	isIndex int,
	hg *statistics.Histogram,
	cms *statistics.CMSketch,
	topN *statistics.TopN,
	statsVersion int,
	isAnalyzed int64,
	updateAnalyzeTime bool,
	source string,
) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveStatsToStorage(sctx, tableID,
			count, modifyCount, isIndex, hg, cms, topN, statsVersion, isAnalyzed, updateAnalyzeTime)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, source, false)
	}
	return
}

// SaveMetaToStorage saves stats meta to the storage.
func (s *statsReadWriter) SaveMetaToStorage(tableID, count, modifyCount int64, source string) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveMetaToStorage(sctx, tableID, count, modifyCount)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, source, false)
	}
	return
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (s *statsReadWriter) InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = InsertExtendedStats(sctx, s.statsHandler, statsName, colIDs, tp, tableID, ifNotExists)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats", false)
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (s *statsReadWriter) MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = MarkExtendedStatsDeleted(sctx, s.statsHandler, statsName, tableID, ifExists)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats", false)
	}
	return
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func (s *statsReadWriter) SaveExtendedStatsToStorage(tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveExtendedStatsToStorage(sctx, tableID, extStats, isLoad)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(tableID, statsVer, "extended stats", false)
	}
	return
}

func (s *statsReadWriter) LoadTablePartitionStats(tableInfo *model.TableInfo, partitionDef *model.PartitionDefinition) (*statistics.Table, error) {
	var partitionStats *statistics.Table
	partitionStats, err := s.TableStatsFromStorage(tableInfo, partitionDef.ID, true, 0)
	if err != nil {
		return nil, err
	}
	// if the err == nil && partitionStats == nil, it means we lack the partition-level stats which the physicalID is equal to partitionID.
	if partitionStats == nil {
		errMsg := fmt.Sprintf("table `%s` partition `%s`", tableInfo.Name.L, partitionDef.Name.L)
		err = types.ErrPartitionStatsMissing.GenWithStackByArgs(errMsg)
		return nil, err
	}
	return partitionStats, nil
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

// PersistStatsBySnapshot dumps statistic to json and call the function for each partition statistic to persist.
// Notice:
//  1. It might call the function `persist` with nil jsontable.
//  2. It is only used by BR, so partitions' statistic are always dumped.
//
// TODO: once we support column-level statistic dump, it should replace the `PersistStatsBySnapshot` and `DumpStatsToJSON`.
func (s *statsReadWriter) PersistStatsBySnapshot(
	ctx context.Context,
	dbName string,
	tableInfo *model.TableInfo,
	snapshot uint64,
	persist statstypes.PersistFunc,
) error {
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		jsonTable, err := s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
		if err != nil {
			return errors.Trace(err)
		}
		return persist(ctx, jsonTable, tableInfo.ID)
	}

	for _, def := range pi.Definitions {
		tbl, err := s.TableStatsToJSON(dbName, tableInfo, def.ID, snapshot)
		if err != nil {
			return errors.Trace(err)
		}
		if tbl == nil {
			continue
		}
		if err := persist(ctx, tbl, def.ID); err != nil {
			return errors.Trace(err)
		}
	}
	// dump its global-stats if existed
	tbl, err := s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return errors.Trace(err)
	}
	if tbl != nil {
		return persist(ctx, tbl, tableInfo.ID)
	}
	return nil
}

// DumpStatsToJSONBySnapshot dumps statistic to json.
func (s *statsReadWriter) DumpStatsToJSONBySnapshot(dbName string, tableInfo *model.TableInfo, snapshot uint64, dumpPartitionStats bool) (*util.JSONTable, error) {
	pruneMode, err := util.GetCurrentPruneMode(s.statsHandler.SPool())
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
	var jsonTbl *util.JSONTable
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		tbl.Version, tbl.ModifyCount, tbl.RealtimeCount, err = StatsMetaByTableIDFromStorage(sctx, physicalID, snapshot)
		if err != nil {
			return err
		}
		jsonTbl, err = GenJSONTableFromStats(sctx, dbName, tableInfo, tbl)
		return err
	})
	if err != nil {
		return nil, err
	}
	return jsonTbl, nil
}

// TestLoadStatsErr is only for test.
type TestLoadStatsErr struct{}

// LoadStatsFromJSONConcurrently consumes concurrently the statistic task from `taskCh`.
func (s *statsReadWriter) LoadStatsFromJSONConcurrently(
	ctx context.Context,
	tableInfo *model.TableInfo,
	taskCh chan *statstypes.PartitionStatisticLoadTask,
	concurrencyForPartition int,
) error {
	nCPU := runtime.GOMAXPROCS(0)
	if concurrencyForPartition == 0 {
		concurrencyForPartition = (nCPU + 1) / 2 // default
	}
	concurrencyForPartition = min(concurrencyForPartition, nCPU) // for safety

	var wg sync.WaitGroup
	e := new(atomic.Pointer[error])
	for i := 0; i < concurrencyForPartition; i++ {
		wg.Add(1)
		s.statsHandler.GPool().Go(func() {
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("%v", r)
					e.CompareAndSwap(nil, &err)
				}
				wg.Done()
			}()

			for tbl := range taskCh {
				if tbl == nil {
					continue
				}

				loadFunc := s.loadStatsFromJSON
				if intest.InTest && ctx.Value(TestLoadStatsErr{}) != nil {
					loadFunc = ctx.Value(TestLoadStatsErr{}).(func(*model.TableInfo, int64, *util.JSONTable) error)
				}

				err := loadFunc(tableInfo, tbl.PhysicalID, tbl.JSONTable)
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

	return nil
}

// LoadStatsFromJSONNoUpdate will load statistic from JSONTable, and save it to the storage.
func (s *statsReadWriter) LoadStatsFromJSONNoUpdate(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *util.JSONTable, concurrencyForPartition int) error {
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
		taskCh := make(chan *statstypes.PartitionStatisticLoadTask, len(pi.Definitions)+1)
		for _, def := range pi.Definitions {
			tbl := jsonTbl.Partitions[def.Name.L]
			if tbl != nil {
				taskCh <- &statstypes.PartitionStatisticLoadTask{
					PhysicalID: def.ID,
					JSONTable:  tbl,
				}
			}
		}

		// load global-stats if existed
		if globalStats, ok := jsonTbl.Partitions[util.TiDBGlobalStats]; ok {
			taskCh <- &statstypes.PartitionStatisticLoadTask{
				PhysicalID: tableInfo.ID,
				JSONTable:  globalStats,
			}
		}
		close(taskCh)
		if err := s.LoadStatsFromJSONConcurrently(ctx, tableInfo, taskCh, concurrencyForPartition); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
// In final, it will also udpate the stats cache.
func (s *statsReadWriter) LoadStatsFromJSON(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *util.JSONTable, concurrencyForPartition int) error {
	if err := s.LoadStatsFromJSONNoUpdate(ctx, is, jsonTbl, concurrencyForPartition); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(s.statsHandler.Update(is))
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
		err = s.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.GetStatsVer()), statistics.AnalyzeFlag, false, util.StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = s.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.GetStatsVer()), statistics.AnalyzeFlag, false, util.StatsMetaHistorySourceLoadStats)
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
