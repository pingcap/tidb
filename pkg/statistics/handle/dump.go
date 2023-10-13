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
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	handle_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	utilstats "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// TestLoadStatsErr is only for test.
type TestLoadStatsErr struct{}

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

func (h *Handle) tableHistoricalStatsToJSON(physicalID int64, snapshot uint64) (jt *storage.JSONTable, exist bool, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		jt, exist, err = storage.TableHistoricalStatsToJSON(sctx, physicalID, snapshot)
		return err
	}, utilstats.FlagWrapTxn)
	return
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
// In final, it will also udpate the stats cache.
func (h *Handle) LoadStatsFromJSON(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *storage.JSONTable, concurrencyForPartition uint8) error {
	if err := h.LoadStatsFromJSONNoUpdate(ctx, is, jsonTbl, concurrencyForPartition); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(h.Update(is))
}

// LoadStatsFromJSONNoUpdate will load statistic from JSONTable, and save it to the storage.
func (h *Handle) LoadStatsFromJSONNoUpdate(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *storage.JSONTable, concurrencyForPartition uint8) error {
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
		err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, jsonTbl)
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
			h.gpool.Go(func() {
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

					loadFunc := h.loadStatsFromJSON
					if intest.InTest && ctx.Value(TestLoadStatsErr{}) != nil {
						loadFunc = ctx.Value(TestLoadStatsErr{}).(func(*model.TableInfo, int64, *storage.JSONTable) error)
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
		if globalStats, ok := jsonTbl.Partitions[globalstats.TiDBGlobalStats]; ok {
			if err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, globalStats); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
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
