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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/predicatecolumn"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	statsutil "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

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
	for range concurrencyForPartition {
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
func (s *statsReadWriter) LoadStatsFromJSONNoUpdate(ctx context.Context, is infoschema.InfoSchema,
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
func (s *statsReadWriter) LoadStatsFromJSON(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *statsutil.JSONTable, concurrencyForPartition int) error {
	if err := s.LoadStatsFromJSONNoUpdate(ctx, is, jsonTbl, concurrencyForPartition); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(s.statsHandler.Update(ctx, is))
}

func (s *statsReadWriter) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *statsutil.JSONTable) error {
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	var outerErr error
	tbl.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		if err := s.SaveColOrIdxStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.GetStatsVer()), false, util.StatsMetaHistorySourceLoadStats); err != nil {
			outerErr = err
			return true
		}
		return false
	})
	if outerErr != nil {
		return outerErr
	}
	tbl.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		if err := s.SaveColOrIdxStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.GetStatsVer()), false, util.StatsMetaHistorySourceLoadStats); err != nil {
			outerErr = err
			return true
		}
		return false
	})
	if outerErr != nil {
		return outerErr
	}
	err = s.SaveExtendedStatsToStorage(tbl.PhysicalID, tbl.ExtendedStats, true)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.SaveColumnStatsUsageToStorage(tbl.PhysicalID, jsonTbl.PredicateColumns)
	if err != nil {
		return errors.Trace(err)
	}
	return s.SaveMetaToStorage(util.StatsMetaHistorySourceLoadStats, true, statstypes.MetaUpdate{
		PhysicalID:  tbl.PhysicalID,
		Count:       tbl.RealtimeCount,
		ModifyCount: tbl.ModifyCount,
	})
}

// SaveColumnStatsUsageToStorage saves column statistics usage information for a table into mysql.column_stats_usage.
func (s *statsReadWriter) SaveColumnStatsUsageToStorage(physicalID int64, predicateColumns []*statsutil.JSONPredicateColumn) error {
	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
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
