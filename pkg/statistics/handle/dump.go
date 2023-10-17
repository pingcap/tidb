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
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	utilstats "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// TestLoadStatsErr is only for test.
type TestLoadStatsErr struct{}

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
// In final, it will also udpate the stats cache.
func (h *Handle) LoadStatsFromJSON(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *utilstats.JSONTable, concurrencyForPartition uint8) error {
	if err := h.LoadStatsFromJSONNoUpdate(ctx, is, jsonTbl, concurrencyForPartition); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(h.Update(is))
}

// LoadStatsFromJSONNoUpdate will load statistic from JSONTable, and save it to the storage.
func (h *Handle) LoadStatsFromJSONNoUpdate(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *utilstats.JSONTable, concurrencyForPartition uint8) error {
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
						loadFunc = ctx.Value(TestLoadStatsErr{}).(func(*model.TableInfo, int64, *utilstats.JSONTable) error)
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
		if globalStats, ok := jsonTbl.Partitions[utilstats.TiDBGlobalStats]; ok {
			if err := h.loadStatsFromJSON(tableInfo, tableInfo.ID, globalStats); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (h *Handle) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *utilstats.JSONTable) error {
	tbl, err := storage.TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range tbl.Columns {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.GetStatsVer()), 1, false, utilstats.StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.GetStatsVer()), 1, false, utilstats.StatsMetaHistorySourceLoadStats)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = h.SaveExtendedStatsToStorage(tbl.PhysicalID, tbl.ExtendedStats, true)
	if err != nil {
		return errors.Trace(err)
	}
	return h.SaveMetaToStorage(tbl.PhysicalID, tbl.RealtimeCount, tbl.ModifyCount, utilstats.StatsMetaHistorySourceLoadStats)
}
