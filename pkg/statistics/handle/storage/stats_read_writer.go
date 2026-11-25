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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/predicatecolumn"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	statsutil "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// StatsCache is the part of the stats cache that the stats read-writer reads and updates.
type StatsCache interface {
	statstypes.StatsCacheGetter
	statstypes.StatsCacheUpdater
	statstypes.StatsCacheLoader
}

// statsSaver saves analyze results and the stats of columns and indexes in the sessions of a
// pool. The lease tells when a save is so slow that other TiDB nodes can miss it.
type statsSaver struct {
	pool  util.SessionPool
	lease util.LeaseGetter
}

// newStatsSaver creates a statsSaver that saves in the sessions of pool. It needs no stats cache.
func newStatsSaver(pool util.SessionPool, lease util.LeaseGetter) *statsSaver {
	return &statsSaver{pool: pool, lease: lease}
}

// statsMetaReader implements statstypes.StatsMetaReader in the sessions of a pool.
type statsMetaReader struct {
	pool util.SessionPool
}

// newStatsMetaReader creates a statstypes.StatsMetaReader that reads in the sessions of pool.
// It needs no stats cache.
func newStatsMetaReader(pool util.SessionPool) *statsMetaReader {
	return &statsMetaReader{pool: pool}
}

// ReadWriter reads and writes the stats in the storage. It updates the stats cache only when it
// loads stats from JSON or loads the needed histograms.
type ReadWriter struct {
	*statsMetaReader
	*statsSaver
	pool       util.Pool
	tables     util.TableInfoGetter
	statsCache StatsCache
}

// NewReadWriter creates a ReadWriter that runs its SQL in the sessions of pool and loads JSON
// stats in the goroutines of pool. It reads and updates the cached stats through statsCache.
func NewReadWriter(
	pool util.Pool,
	lease util.LeaseGetter,
	tables util.TableInfoGetter,
	statsCache StatsCache,
) *ReadWriter {
	return &ReadWriter{
		statsMetaReader: newStatsMetaReader(pool),
		statsSaver:      newStatsSaver(pool, lease),
		pool:            pool,
		tables:          tables,
		statsCache:      statsCache,
	}
}

// ChangeGlobalStatsID changes the table ID in global-stats to the new table ID.
func (s *ReadWriter) ChangeGlobalStatsID(from, to int64) (err error) {
	return util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		return errors.Trace(ChangeGlobalStatsID(util.StatsCtx, sctx, from, to))
	}, util.FlagWrapTxn)
}

// UpdateStatsMetaVersionForGC updates the version of mysql.stats_meta,
// ensuring it is greater than the last garbage collection (GC) time.
// The GC worker deletes old stats based on a safe time point,
// calculated as now() - 10 * max(stats lease, ddl lease).
// The range [last GC time, safe time point) is chosen to prevent
// the simultaneous deletion of numerous stats, minimizing potential
// performance issues.
// This function ensures the version is updated beyond the last GC time,
// allowing the GC worker to delete outdated stats.
//
// Explanation:
// - ddl lease: 10
// - stats lease: 3
// - safe time point: now() - 10 * 10 = now() - 100
// - now: 200
// - last GC time: 90
// - [last GC time, safe time point) = [90, 100)
// - To trigger stats deletion, the version must be updated beyond 90.
//
// This safeguards scenarios where a table remains unchanged for an extended period.
// For instance, if a table was created at time 90, and it's now time 200,
// with the last GC time at 95 and the safe time point at 100,
// updating the version beyond 95 ensures eventual deletion of stats.
func (s *ReadWriter) UpdateStatsMetaVersionForGC(physicalID int64) (err error) {
	return util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		_, err := UpdateStatsMetaVerAndLastHistUpdateVer(util.StatsCtx, sctx, physicalID)
		return errors.Trace(err)
	}, util.FlagWrapTxn)
}

// IsStatsMetaWriteTooSlow reports whether a stats_meta write that took dur can be missed by the
// stats cache Update of other TiDB nodes, which looks back only LeaseOffset * lease from its newest version.
func IsStatsMetaWriteTooSlow(lease, dur time.Duration) bool {
	// Note: In unit tests, the lease is set to a value less than 0, which means the lease is disabled.
	// This is why we need to explicitly check the lease here. Without this check,
	// the duration validation would always evaluate to true, which is not the intended behavior.
	return lease > 0 && dur >= util.LeaseOffset*lease
}

// handleSlowStatsSaving is used to handle the slow stats saving process.
// Once the saving process is too slow, we need to log a warning.
// Also, we need to update the stats meta with a more recent version to avoid other nodes missing the delta update.
// Combined with the stats meta version update, we can ensure the stats cache on other TiDB nodes is consistent.
// See more at stats cache's Update function.
func (s *statsSaver) handleSlowStatsSaving(tableID int64, start time.Time) error {
	dur := time.Since(start)
	isLoadIntervalExceeded := IsStatsMetaWriteTooSlow(s.lease.Lease(), dur)
	// Use failpoint to simulate slow saving.
	failpoint.Inject("slowStatsSaving", func(val failpoint.Value) {
		if val.(bool) {
			isLoadIntervalExceeded = true
		}
	})

	if !isLoadIntervalExceeded {
		return nil
	}

	statslogutil.StatsLogger().Warn("Update stats cache is too slow",
		zap.Duration("duration", dur),
		zap.Int64("physicalID", tableID),
	)

	// Update stats meta to avoid other nodes missing the delta update.
	statsVer := uint64(0)
	err := util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		startTS, err := UpdateStatsMetaVerAndLastHistUpdateVer(util.StatsCtx, sctx, tableID)
		failpoint.Inject("failToSaveStats", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("mock update stats meta version failed")
			}
		})
		if err != nil {
			return errors.Trace(err)
		}
		statsVer = startTS
		return nil
	}, util.FlagWrapTxn)
	if err != nil {
		statslogutil.StatsLogger().Error("Failed to update stats meta version for slow saving, the stats cache on other TiDB nodes may be inconsistent",
			zap.Int64("physicalID", tableID),
			zap.Error(err),
		)
		return errors.Annotate(err, "failed to update stats meta version while saving statistics; the system may be too busy; please retry the operation later")
	}

	statslogutil.StatsLogger().Info("Successfully updated stats meta version for slow saving",
		zap.Uint64("statsVer", statsVer),
		zap.Int64("physicalID", tableID),
	)
	return nil
}

// SaveAnalyzeResultToStorage saves the stats of a table to storage.
func (s *statsSaver) SaveAnalyzeResultToStorage(results *statistics.AnalyzeResults, analyzeSnapshot bool) (err error) {
	var statsVer uint64
	start := time.Now()
	err = util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveAnalyzeResultToStorage(sctx, results, analyzeSnapshot)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		tableID := results.TableID.GetStatisticsID()
		// Check if saving was slow and update stats version if needed
		err2 := s.handleSlowStatsSaving(tableID, start)
		if err2 != nil {
			statslogutil.StatsLogger().Error("Failed to update stats meta version for slow saving during analyze job execution",
				zap.Int64("physicalID", tableID),
				zap.String("database", results.Job.DBName),
				zap.String("table", results.Job.TableName),
				zap.String("partition", results.Job.PartitionName),
				zap.String("jobInfo", results.Job.JobInfo),
				zap.Error(err2),
			)
			return err2
		}
	}
	return err
}

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func (s *statsMetaReader) StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error) {
	err = util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		count, modifyCount, _, err = StatsMetaCountAndModifyCount(util.StatsCtx, sctx, tableID)
		return err
	}, util.FlagWrapTxn)
	return
}

// TableStatsFromStorageWithCache loads table stats info from storage in a session of pool. It
// starts from the stats of the table in statsCache.
func TableStatsFromStorageWithCache(
	pool util.SessionPool,
	statsCache statstypes.StatsCacheGetter,
	lease util.LeaseGetter,
	tableInfo *model.TableInfo,
	physicalID int64,
	loadAll bool,
	snapshot uint64,
) (statsTbl *statistics.Table, err error) {
	err = util.CallWithSCtx(pool.SPool(), func(sctx sessionctx.Context) error {
		var ok bool
		statsTbl, ok = statsCache.Get(physicalID)
		if !ok {
			statsTbl = nil
		}
		statsTbl, err = TableStatsFromStorage(sctx, snapshot, tableInfo, physicalID, loadAll, lease.Lease(), statsTbl)
		return err
	}, util.FlagWrapTxn)
	return
}

// SaveColOrIdxStatsToStorage saves the stats to storage.
// If count is negative, both count and modify count would not be used and not be written to the table. Unless, corresponding
// fields in the stats_meta table will be updated.
// TODO: refactor to reduce the number of parameters
func (s *statsSaver) SaveColOrIdxStatsToStorage(
	tableID int64,
	count, modifyCount int64,
	isIndex int,
	hg *statistics.Histogram,
	cms *statistics.CMSketch,
	topN *statistics.TopN,
	fms *statistics.FMSketch,
	statsVersion int,
	updateAnalyzeTime bool,
) (err error) {
	var statsVer uint64
	start := time.Now()
	err = util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveColOrIdxStatsToStorage(sctx, tableID,
			count, modifyCount, isIndex, hg, cms, topN, fms, statsVersion, updateAnalyzeTime)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		failpoint.InjectCall("afterSaveColOrIdxStatsToStorage", tableID, isIndex)
		// Check if saving was slow and update stats version if needed
		err2 := s.handleSlowStatsSaving(tableID, start)
		if err2 != nil {
			return err2
		}
	}
	return
}

// SaveMetaToStorage saves stats meta to the storage.
// Use the param `refreshLastHistVer` to indicate whether we need to update the last_histograms_versions in stats_meta table.
// Set it to true if the column/index stats is updated.
func (s *ReadWriter) SaveMetaToStorage(
	refreshLastHistVer bool,
	metaUpdates ...statstypes.MetaUpdate,
) (err error) {
	intest.Assert(len(metaUpdates) > 0, "meta updates is empty")
	err = util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		return SaveMetaToStorage(sctx, refreshLastHistVer, metaUpdates)
	}, util.FlagWrapTxn)
	return
}

// LoadNeededHistograms will load histograms for those needed columns/indices.
func (s *ReadWriter) LoadNeededHistograms(is infoschema.InfoSchema) (err error) {
	err = util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		return LoadNeededHistograms(sctx, is, s.statsCache, s.tables)
	}, util.FlagWrapTxn)
	return err
}

// DumpStatsToJSON dumps statistic to json.
func (s *ReadWriter) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo,
	historyStatsExec sqlexec.RestrictedSQLExecutor, dumpPartitionStats bool) (*statsutil.JSONTable, error) {
	var snapshot uint64
	if historyStatsExec != nil {
		sctx := historyStatsExec.(sessionctx.Context)
		snapshot = sctx.GetSessionVars().SnapshotTS
	}
	return s.DumpStatsToJSONBySnapshot(dbName, tableInfo, snapshot, dumpPartitionStats)
}

// PersistStatsBySnapshot dumps statistic to json and call the function for each partition statistic to persist.
// Notice:
//  1. It might call the function `persist` with nil jsontable.
//  2. It is only used by BR, so partitions' statistic are always dumped.
//
// TODO: once we support column-level statistic dump, it should replace the `PersistStatsBySnapshot` and `DumpStatsToJSON`.
func (s *ReadWriter) PersistStatsBySnapshot(
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
func (s *ReadWriter) DumpStatsToJSONBySnapshot(dbName string, tableInfo *model.TableInfo, snapshot uint64, dumpPartitionStats bool) (*statsutil.JSONTable, error) {
	pruneMode, err := util.GetCurrentPruneMode(s.pool.SPool())
	if err != nil {
		return nil, err
	}
	isDynamicMode := variable.PartitionPruneMode(pruneMode) == variable.Dynamic
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	}
	jsonTbl := &statsutil.JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*statsutil.JSONTable, len(pi.Definitions)),
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
		jsonTbl.Partitions[statsutil.TiDBGlobalStats] = tbl
	}
	return jsonTbl, nil
}

// TableStatsToJSON dumps statistic to json.
func (s *ReadWriter) TableStatsToJSON(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*statsutil.JSONTable, error) {
	tbl, err := TableStatsFromStorageWithCache(s.pool, s.statsCache, s.lease, tableInfo, physicalID, true, snapshot)
	if err != nil || tbl == nil {
		return nil, err
	}
	var jsonTbl *statsutil.JSONTable
	err = util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		tbl.Version, tbl.ModifyCount, tbl.RealtimeCount, err = StatsMetaByTableIDFromStorage(sctx, physicalID, snapshot)
		if err != nil {
			return err
		}
		// Note: Because we don't show this information in the session directly, so we can always use UTC here.
		colStatsUsage, err := predicatecolumn.LoadColumnStatsUsageForTable(sctx, time.UTC, physicalID)
		if err != nil {
			return err
		}
		jsonTbl, err = GenJSONTableFromStats(sctx, dbName, tableInfo, tbl, colStatsUsage)
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
func (s *ReadWriter) LoadStatsFromJSONConcurrently(
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
	for range concurrencyForPartition {
		wg.Add(1)
		s.pool.GPool().Go(func() {
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
					loadFunc = ctx.Value(TestLoadStatsErr{}).(func(*model.TableInfo, int64, *statsutil.JSONTable) error)
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
func (s *ReadWriter) LoadStatsFromJSONNoUpdate(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *statsutil.JSONTable, concurrencyForPartition int) error {
	table, err := is.TableByName(context.Background(), ast.NewCIStr(jsonTbl.DatabaseName), ast.NewCIStr(jsonTbl.TableName))
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
		if globalStats, ok := jsonTbl.Partitions[statsutil.TiDBGlobalStats]; ok {
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
func (s *ReadWriter) LoadStatsFromJSON(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *statsutil.JSONTable, concurrencyForPartition int) error {
	if err := s.LoadStatsFromJSONNoUpdate(ctx, is, jsonTbl, concurrencyForPartition); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(s.statsCache.Update(ctx, is))
}

func (s *ReadWriter) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *statsutil.JSONTable) error {
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	var outerErr error
	tbl.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		if err := s.SaveColOrIdxStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, col.FMSketch, int(col.GetStatsVer()), false); err != nil {
			outerErr = err
			return true
		}
		return false
	})
	if outerErr != nil {
		return outerErr
	}
	tbl.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		if err := s.SaveColOrIdxStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, idx.FMSketch, int(idx.GetStatsVer()), false); err != nil {
			outerErr = err
			return true
		}
		return false
	})
	if outerErr != nil {
		return outerErr
	}
	err = s.SaveColumnStatsUsageToStorage(tbl.PhysicalID, jsonTbl.PredicateColumns)
	if err != nil {
		return errors.Trace(err)
	}
	return s.SaveMetaToStorage(true, statstypes.MetaUpdate{
		PhysicalID:  tbl.PhysicalID,
		Count:       tbl.RealtimeCount,
		ModifyCount: tbl.ModifyCount,
	})
}

// SaveColumnStatsUsageToStorage saves column statistics usage information for a table into mysql.column_stats_usage.
func (s *ReadWriter) SaveColumnStatsUsageToStorage(physicalID int64, predicateColumns []*statsutil.JSONPredicateColumn) error {
	return util.CallWithSCtx(s.pool.SPool(), func(sctx sessionctx.Context) error {
		colStatsUsage := make(map[model.TableItemID]statstypes.ColStatsTimeInfo, len(predicateColumns))
		for _, col := range predicateColumns {
			if col == nil {
				continue
			}
			itemID := model.TableItemID{TableID: physicalID, ID: col.ID}
			lastUsedAt, err := parseTimeOrNil(col.LastUsedAt)
			if err != nil {
				return err
			}
			lastAnalyzedAt, err := parseTimeOrNil(col.LastAnalyzedAt)
			if err != nil {
				return err
			}
			colStatsUsage[itemID] = statstypes.ColStatsTimeInfo{
				LastUsedAt:     lastUsedAt,
				LastAnalyzedAt: lastAnalyzedAt,
			}
		}
		return predicatecolumn.SaveColumnStatsUsageForTable(sctx, colStatsUsage)
	}, util.FlagWrapTxn)
}

func parseTimeOrNil(timeStr *string) (*types.Time, error) {
	if timeStr == nil {
		return nil, nil
	}
	// DefaultStmtNoWarningContext use UTC timezone.
	parsedTime, err := types.ParseTime(types.DefaultStmtNoWarningContext, *timeStr, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return nil, err
	}
	return &parsedTime, nil
}
