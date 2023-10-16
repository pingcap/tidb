// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a Copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/metrics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// StatsCacheImpl implements util.StatsCache.
type StatsCacheImpl struct {
	pool    util.SessionPool
	tblInfo util.TableInfoGetter
	atomic.Pointer[StatsCache]

	tableStatsFromStorage func(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error)
	statsLease            time.Duration
}

// NewStatsCacheImpl creates a new StatsCache.
func NewStatsCacheImpl(pool util.SessionPool, tblInfo util.TableInfoGetter, statsLease time.Duration,
	tableStatsFromStorage func(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error),
) (util.StatsCache, error) {
	newCache, err := NewStatsCache()
	if err != nil {
		return nil, err
	}
	result := &StatsCacheImpl{
		pool:                  pool,
		tblInfo:               tblInfo,
		statsLease:            statsLease,
		tableStatsFromStorage: tableStatsFromStorage,
	}
	result.Store(newCache)
	return result, nil
}

// NewStatsCacheImplForTest creates a new StatsCache for test.
func NewStatsCacheImplForTest() (util.StatsCache, error) {
	return NewStatsCacheImpl(nil, nil, 0, nil)
}

// Update reads stats meta from store and updates the stats map.
func (s *StatsCacheImpl) Update(is infoschema.InfoSchema) error {
	lastVersion := s.MaxTableStatsVersion()
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than three lease.
	offset := util.DurationToTS(3 * s.statsLease)
	if s.MaxTableStatsVersion() >= offset {
		lastVersion = lastVersion - offset
	} else {
		lastVersion = 0
	}

	var rows []chunk.Row
	var err error
	err = util.CallWithSCtx(s.pool, func(sctx sessionctx.Context) error {
		rows, _, err = util.ExecRows(sctx, "SELECT version, table_id, modify_count, count from mysql.stats_meta where version > %? order by version", lastVersion)
		return err
	})
	if err != nil {
		return errors.Trace(err)
	}
	tables := make([]*statistics.Table, 0, len(rows))
	deletedTableIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		version := row.GetUint64(0)
		physicalID := row.GetInt64(1)
		modifyCount := row.GetInt64(2)
		count := row.GetInt64(3)
		table, ok := s.tblInfo.TableInfoByID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tableInfo := table.Meta()
		if oldTbl, ok := s.Get(physicalID); ok && oldTbl.Version >= version && tableInfo.UpdateTS == oldTbl.TblInfoUpdateTS {
			continue
		}
		tbl, err := s.tableStatsFromStorage(tableInfo, physicalID, false, 0)
		// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
		if err != nil {
			logutil.BgLogger().Error("error occurred when read table stats", zap.String("category", "stats"), zap.String("table", tableInfo.Name.O), zap.Error(err))
			continue
		}
		if tbl == nil {
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tbl.Version = version
		tbl.RealtimeCount = count
		tbl.ModifyCount = modifyCount
		tbl.Name = util.GetFullTableName(is, tableInfo)
		tbl.TblInfoUpdateTS = tableInfo.UpdateTS
		tables = append(tables, tbl)
	}
	s.UpdateStatsCache(tables, deletedTableIDs)
	return nil
}

// Replace replaces this cache.
func (s *StatsCacheImpl) Replace(cache util.StatsCache) {
	x := cache.(*StatsCacheImpl)
	s.replace(x.Load())
}

// replace replaces the cache with the new cache.
func (s *StatsCacheImpl) replace(newCache *StatsCache) {
	old := s.Swap(newCache)
	if old != nil {
		old.Close()
	}
	metrics.CostGauge.Set(float64(newCache.Cost()))
}

// UpdateStatsCache updates the cache with the new cache.
func (s *StatsCacheImpl) UpdateStatsCache(tables []*statistics.Table, deletedIDs []int64) {
	if enableQuota := config.GetGlobalConfig().Performance.EnableStatsCacheMemQuota; enableQuota {
		s.Load().Update(tables, deletedIDs)
	} else {
		newCache := s.Load().CopyAndUpdate(tables, deletedIDs)
		s.replace(newCache)
	}
}

// Close closes this cache.
func (s *StatsCacheImpl) Close() {
	s.Load().Close()
}

// Clear clears this cache.
func (s *StatsCacheImpl) Clear() {
	cache, err := NewStatsCache()
	if err != nil {
		logutil.BgLogger().Warn("create stats cache failed", zap.Error(err))
		return
	}
	s.replace(cache)
}

// MemConsumed returns its memory usage.
func (s *StatsCacheImpl) MemConsumed() (size int64) {
	return s.Load().Cost()
}

// Get returns the specified table's stats.
func (s *StatsCacheImpl) Get(tableID int64) (*statistics.Table, bool) {
	return s.Load().Get(tableID)
}

// Put puts this table stats into the cache.
func (s *StatsCacheImpl) Put(id int64, t *statistics.Table) {
	s.Load().put(id, t)
}

// MaxTableStatsVersion returns the version of the current cache, which is defined as
// the max table stats version the cache has in its lifecycle.
func (s *StatsCacheImpl) MaxTableStatsVersion() uint64 {
	return s.Load().Version()
}

// Values returns all values in this cache.
func (s *StatsCacheImpl) Values() []*statistics.Table {
	return s.Load().Values()
}

// Len returns the length of this cache.
func (s *StatsCacheImpl) Len() int {
	return s.Load().Len()
}

// SetStatsCacheCapacity sets the cache's capacity.
func (s *StatsCacheImpl) SetStatsCacheCapacity(c int64) {
	s.Load().SetCapacity(c)
}
