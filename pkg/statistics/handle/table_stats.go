// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// tableStatsCache is the part of the stats cache that the table stats getter reads and fills.
type tableStatsCache interface {
	types.StatsCacheGetter
	types.StatsCacheLister
	types.StatsCacheUpdater
}

// tableStatsGetter gets the stats of tables from the cache, and puts pseudo stats into the cache
// for the tables that have none.
type tableStatsGetter struct {
	pool  util.SessionPool
	cache tableStatsCache

	// systemDBIDCache caches the database IDs that are confirmed as system schemas to avoid repeated session usage.
	systemDBIDCache sync.Map
}

func newTableStatsGetter(pool util.SessionPool, cache tableStatsCache) *tableStatsGetter {
	return &tableStatsGetter{pool: pool, cache: cache}
}

// GetPhysicalTableStats retrieves the statistics for a physical table from cache or creates a pseudo statistics table.
// physicalTableID can be a table ID or partition ID.
func (g *tableStatsGetter) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	tblStats, found := g.getStatsByPhysicalID(physicalTableID, tblInfo)
	intest.Assert(tblStats != nil, "stats should not be nil")
	intest.Assert(found, "stats should not be nil")
	return tblStats
}

// GetNonPseudoPhysicalTableStats retrieves the statistics for a physical table from cache. Unlike
// GetPhysicalTableStats, it does not create pseudo stats, but it can return pseudo stats that are already cached.
// physicalTableID can be a table ID or partition ID.
// Note: this function may return nil if the table is not found in the cache.
func (g *tableStatsGetter) GetNonPseudoPhysicalTableStats(physicalTableID int64) (*statistics.Table, bool) {
	return g.getStatsByPhysicalID(physicalTableID, nil)
}

func (g *tableStatsGetter) getStatsByPhysicalID(physicalTableID int64, tblInfo *model.TableInfo) (*statistics.Table, bool) {
	tbl, ok := g.cache.Get(physicalTableID)
	if ok {
		return tbl, true
	}
	if tblInfo == nil {
		return nil, false
	}

	tbl = statistics.PseudoTable(tblInfo, false, true)
	tbl.PhysicalID = physicalTableID

	// TODO: Determine whether we really need to cache pseudo table stats for non-partitioned tables.
	// If the memory overhead is manageable, we can remove this optimization.
	shouldCachePseudo := tblInfo.GetPartitionInfo() == nil || g.cache.Len() < 64
	if !shouldCachePseudo {
		return tbl, true
	}

	// NOTE: Sessions borrowed from the pool cannot fetch schema metadata for local temporary tables,
	// so skip caching their statistics.
	// Also skip global temporary tables for consistency.
	isTempTable := tblInfo.TempTableType != model.TempTableNone
	if isTempTable {
		return tbl, true
	}

	// In some test cases, we may need to skip the system table check.
	if intest.InTest {
		// The failpoint to skip system table check, for testing only.
		skipSystemTableCheck := false
		failpoint.Inject("SkipSystemTableCheck", func(val failpoint.Value) {
			skip, ok := val.(bool)
			if ok && skip {
				skipSystemTableCheck = true
			}
		})

		// In some test environments, the session pool may be nil.
		// In such cases, we cannot determine if it's a system table, so we skip the check.
		if se, ok := g.pool.SPool().(*syssession.AdvancedSessionPool); ok && se == nil {
			skipSystemTableCheck = true
		}
		if skipSystemTableCheck {
			g.cache.UpdateStatsCache(types.CacheUpdate{
				Updated: []*statistics.Table{tbl},
			})
			return tbl, true
		}
	}

	isSystemTable, err := g.isSystemTable(physicalTableID, tblInfo)
	if err != nil {
		dbID := tblInfo.DBID
		statslogutil.StatsErrVerboseSampleLogger().Warn("Check system table failed", zap.Int64("tableID", physicalTableID), zap.Int64("dbID", dbID), zap.Error(err))
		return tbl, true
	}

	if isSystemTable {
		return tbl, true
	}

	g.cache.UpdateStatsCache(types.CacheUpdate{
		Updated: []*statistics.Table{tbl},
	})
	return tbl, true
}

// isSystemTable determines whether the table should be treated as a system table.
// NOTE: You might worry that this slows down Get. It runs only once per non-partitioned table, or once per partition when the cache holds fewer than 64 entries, so the impact is negligible.
// Stats healthy metrics almost never show pseudo tables, because once a DDL event is processed or the table is updated, real statistics are loaded into the cache.
// We also cache the database IDs of system schemas to avoid repeated session usage.
func (g *tableStatsGetter) isSystemTable(physicalTableID int64, tblInfo *model.TableInfo) (bool, error) {
	intest.Assert(tblInfo != nil, "tblInfo should not be nil for tableID %d", physicalTableID)
	dbID := tblInfo.DBID
	intest.Assert(dbID > 0, "invalid dbID %d for tableID %d", dbID, physicalTableID)
	if autoid.IsMemSchemaID(dbID) {
		return true, nil
	}

	if _, ok := g.systemDBIDCache.Load(dbID); ok {
		return true, nil
	}

	isSystemTable := false
	err := g.pool.SPool().WithSession(func(session *syssession.Session) error {
		return session.WithSessionContext(func(sctx sessionctx.Context) error {
			is := sctx.GetLatestInfoSchema()
			db, ok := is.SchemaByID(dbID)
			// 1 is used for some unit tests where the database is not created but directly injected.
			intest.Assert(ok || dbID == 1, "cannot find db for table %d, dbID %d", physicalTableID, dbID)
			if ok && filter.IsSystemSchema(db.Name.L) {
				isSystemTable = true
			}
			return nil
		})
	})
	if err != nil {
		intest.Assert(err == nil, "unexpected error: %v, tableID %d, dbID %d", err, physicalTableID, dbID)
		return false, err
	}
	if isSystemTable {
		g.systemDBIDCache.Store(dbID, struct{}{})
	}

	return isSystemTable, nil
}

func (g *tableStatsGetter) resetSystemDBIDCache() {
	g.systemDBIDCache.Clear()
}

func (g *tableStatsGetter) systemDBIDCacheLen() int {
	length := 0
	g.systemDBIDCache.Range(func(_, _ any) bool {
		length++
		return true
	})
	return length
}
