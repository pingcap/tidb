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

package cache

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/cache/internal"
	"github.com/pingcap/tidb/statistics/handle/cache/internal/lru"
	"github.com/pingcap/tidb/statistics/handle/cache/internal/mapcache"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/syncutil"
)

// TableStatsOption used to indicate the way to get table stats
type TableStatsOption struct {
	byQuery bool
}

// ByQuery indicates whether the stats is got by query
func (t *TableStatsOption) ByQuery() bool {
	return t.byQuery
}

// TableStatsOpt used to edit getTableStatsOption
type TableStatsOpt func(*TableStatsOption)

// WithTableStatsByQuery indicates user needed
func WithTableStatsByQuery() TableStatsOpt {
	return func(option *TableStatsOption) {
		option.byQuery = true
	}
}

// NewStatsCache creates a new StatsCacheWrapper.
func NewStatsCache() *StatsCache {
	enableQuota := config.GetGlobalConfig().Performance.EnableStatsCacheMemQuota
	if enableQuota {
		capacity := variable.StatsCacheMemQuota.Load()
		var innerCache internal.StatsCacheInner = lru.NewStatsLruCache(capacity)
		sc := &StatsCache{}
		sc.c.Store(&innerCache)
		return sc
	}
	var innerCache internal.StatsCacheInner = mapcache.NewMapCache()
	sc := &StatsCache{}
	sc.c.Store(&innerCache)
	return sc
}

// StatsCache caches the tables in memory for Handle.
type StatsCache struct {
	// the current StatsCacheInner implementation cannot handle concurrent read/write with high performance,
	// so to eliminate these read/write conflicts, we update the cache in a COW manner, which is:
	// 1: copy and create a new cache; 2: update on the new cache; 3: swap it with the new cache pointer.
	// TODO: remove this COW implementation and support in-place updates.
	c atomic.Pointer[internal.StatsCacheInner]
	// the max table stats version the cache has in its lifecycle.
	maxTblStatsVer atomic.Uint64
}

func (sc *StatsCache) innerCache() internal.StatsCacheInner {
	return *sc.c.Load()
}

// Len returns the number of tables in the cache.
func (sc *StatsCache) Len() int {
	return sc.innerCache().Len()
}

// GetFromUser returns the statistics of the specified Table ID.
// The returned value should be read-only, if you update it, don't forget to use Put to put it back again, otherwise the memory trace can be inaccurate.
//
//	e.g. v := sc.Get(id); /* update the value */ v.Version = 123; sc.Put(id, v);
func (sc *StatsCache) GetFromUser(id int64) (*statistics.Table, bool) {
	return sc.innerCache().Get(id, true)
}

// GetFromInternal returns the statistics of the specified Table ID.
func (sc *StatsCache) GetFromInternal(id int64) (*statistics.Table, bool) {
	return sc.innerCache().Get(id, false)
}

// PutFromUser puts the table statistics to the cache from query.
func (sc *StatsCache) PutFromUser(id int64, t *statistics.Table) {
	sc.put(id, t, false)
}

// PutFromInternal puts the table statistics to the cache from internal.
func (sc *StatsCache) PutFromInternal(id int64, t *statistics.Table) {
	sc.put(id, t, false)
}

// Put puts the table statistics to the cache.
func (sc *StatsCache) put(id int64, t *statistics.Table, moveLRUFront bool) {
	// NOTE: put is only used when initializing the cache, so don't need to use COW here.
	sc.innerCache().Put(id, t, moveLRUFront)

	// update the maxTblStatsVer
	for v := sc.maxTblStatsVer.Load(); v < t.Version; v = sc.maxTblStatsVer.Load() {
		if sc.maxTblStatsVer.CompareAndSwap(v, t.Version) {
			break
		} // other goroutines have updated the sc.maxTblStatsVer, so we need to check again.
	}
}

// Values returns all the cached statistics tables.
func (sc *StatsCache) Values() []*statistics.Table {
	return sc.innerCache().Values()
}

// Cost returns the memory usage of the cache.
func (sc *StatsCache) Cost() int64 {
	return sc.innerCache().Cost()
}

// SetCapacity sets the memory capacity of the cache.
func (sc *StatsCache) SetCapacity(c int64) {
	sc.innerCache().SetCapacity(c)
}

// Version returns the version of the current cache, which is defined as
// the max table stats version the cache has in its lifecycle.
func (sc *StatsCache) Version() uint64 {
	return sc.maxTblStatsVer.Load()
}

// Front returns the front element's owner tableID, only used for test.
func (sc *StatsCache) Front() int64 {
	return sc.innerCache().Front()
}

// BatchUpdate updates the StatsCache.
func (sc *StatsCache) BatchUpdate(tables []*statistics.Table, deletedIDs []int64, opts ...TableStatsOpt) {
	// current implementation of StatsCacheInner cannot handle concurrent read/write with high performance,
	// so we update it in a COW manner here to eliminate read/write conflicts.
	// TODO: remove this COW implementation and support in-place updates.
	cowUpdate := func() (oldCache, newCache internal.StatsCacheInner) {
		option := &TableStatsOption{}
		for _, opt := range opts {
			opt(option)
		}
		oldCache = sc.innerCache()
		newCache = oldCache.Copy()
		for _, tbl := range tables {
			id := tbl.PhysicalID
			if option.byQuery {
				newCache.Put(id, tbl, true)
			} else {
				newCache.Put(id, tbl, false)
			}
		}
		for _, id := range deletedIDs {
			newCache.Del(id)
		}
		return oldCache, newCache
	}

	for {
		oldCache, newCache := cowUpdate()
		if sc.c.CompareAndSwap(&oldCache, &newCache) {
			break
		} // sc.c has been updated by some other thread, so we need to update it again.
	}

	// update the maxTblStatsVer
	var maxVer uint64
	for _, t := range tables {
		if maxVer < t.Version {
			maxVer = t.Version
		}
	}
	for v := sc.maxTblStatsVer.Load(); v < maxVer; v = sc.maxTblStatsVer.Load() {
		if sc.maxTblStatsVer.CompareAndSwap(v, maxVer) {
			break
		} // other goroutines have updated the sc.maxTblStatsVer, so we need to check again.
	}
}

// Clear removes all cached statistics tables.
// Only for test.
func (sc *StatsCache) Clear() {
	values := sc.Values()
	for _, tbl := range values {
		sc.innerCache().Del(tbl.PhysicalID)
	}
}

// TableRowStatsCache is the cache of table row count.
var TableRowStatsCache = &StatsTableRowCache{}

// StatsTableRowCache is used to cache the count of table rows.
type StatsTableRowCache struct {
	modifyTime time.Time
	tableRows  map[int64]uint64
	colLength  map[tableHistID]uint64
	dirtyIDs   []int64
	mu         syncutil.RWMutex
}

// tableStatsCacheExpiry is the expiry time for table stats cache.
var tableStatsCacheExpiry = 3 * time.Second

// Invalidate invalidates the cache of the table with id.
func (c *StatsTableRowCache) Invalidate(tblID int64) {
	c.mu.Lock()
	// To prevent the cache from becoming too large,
	// we only record the latest 100 dirty tables that have been modified.
	if len(c.dirtyIDs) < 100 {
		c.dirtyIDs = append(c.dirtyIDs, tblID)
	}
	c.mu.Unlock()
}

// GetTableRows gets the count of table rows.
func (c *StatsTableRowCache) GetTableRows(id int64) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tableRows[id]
}

// GetColLength gets the length of the column.
func (c *StatsTableRowCache) GetColLength(id tableHistID) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.colLength[id]
}

// Update tries to update the cache.
func (c *StatsTableRowCache) Update(ctx context.Context, sctx sessionctx.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	if time.Since(c.modifyTime) < tableStatsCacheExpiry {
		if len(c.dirtyIDs) > 0 {
			tableRows, err := getRowCountTables(ctx, sctx, c.dirtyIDs...)
			if err != nil {
				return err
			}
			for id, tr := range tableRows {
				c.tableRows[id] = tr
			}
			colLength, err := getColLengthTables(ctx, sctx, c.dirtyIDs...)
			if err != nil {
				return err
			}
			for id, cl := range colLength {
				c.colLength[id] = cl
			}
			c.dirtyIDs = c.dirtyIDs[:0]
		}
		return nil
	}
	tableRows, err := getRowCountTables(ctx, sctx)
	if err != nil {
		return err
	}
	colLength, err := getColLengthTables(ctx, sctx)
	if err != nil {
		return err
	}
	c.tableRows = tableRows
	c.colLength = colLength
	c.modifyTime = time.Now()
	c.dirtyIDs = c.dirtyIDs[:0]
	return nil
}

func getRowCountTables(ctx context.Context, sctx sessionctx.Context, tableIDs ...int64) (map[int64]uint64, error) {
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	var rows []chunk.Row
	var err error
	if len(tableIDs) == 0 {
		rows, _, err = exec.ExecRestrictedSQL(ctx, nil, "select table_id, count from mysql.stats_meta")
	} else {
		inTblIDs := buildInTableIDsString(tableIDs)
		sql := "select table_id, count from mysql.stats_meta where " + inTblIDs
		rows, _, err = exec.ExecRestrictedSQL(ctx, nil, sql)
	}
	if err != nil {
		return nil, err
	}

	rowCountMap := make(map[int64]uint64, len(rows))
	for _, row := range rows {
		tableID := row.GetInt64(0)
		rowCnt := row.GetUint64(1)
		rowCountMap[tableID] = rowCnt
	}
	return rowCountMap, nil
}

func buildInTableIDsString(tableIDs []int64) string {
	var whereBuilder strings.Builder
	whereBuilder.WriteString("table_id in (")
	for i, id := range tableIDs {
		whereBuilder.WriteString(strconv.FormatInt(id, 10))
		if i != len(tableIDs)-1 {
			whereBuilder.WriteString(",")
		}
	}
	whereBuilder.WriteString(")")
	return whereBuilder.String()
}

type tableHistID struct {
	tableID int64
	histID  int64
}

func getColLengthTables(ctx context.Context, sctx sessionctx.Context, tableIDs ...int64) (map[tableHistID]uint64, error) {
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	var rows []chunk.Row
	var err error
	if len(tableIDs) == 0 {
		sql := "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0"
		rows, _, err = exec.ExecRestrictedSQL(ctx, nil, sql)
	} else {
		inTblIDs := buildInTableIDsString(tableIDs)
		sql := "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0 and " + inTblIDs
		rows, _, err = exec.ExecRestrictedSQL(ctx, nil, sql)
	}
	if err != nil {
		return nil, err
	}

	colLengthMap := make(map[tableHistID]uint64, len(rows))
	for _, row := range rows {
		tableID := row.GetInt64(0)
		histID := row.GetInt64(1)
		totalSize := row.GetInt64(2)
		if totalSize < 0 {
			totalSize = 0
		}
		colLengthMap[tableHistID{tableID: tableID, histID: histID}] = uint64(totalSize)
	}
	return colLengthMap, nil
}

// GetDataAndIndexLength gets the data and index length of the table.
func (c *StatsTableRowCache) GetDataAndIndexLength(info *model.TableInfo, physicalID int64, rowCount uint64) (dataLength, indexLength uint64) {
	columnLength := make(map[string]uint64, len(info.Columns))
	for _, col := range info.Columns {
		if col.State != model.StatePublic {
			continue
		}
		length := col.FieldType.StorageLength()
		if length != types.VarStorageLen {
			columnLength[col.Name.L] = rowCount * uint64(length)
		} else {
			length := c.GetColLength(tableHistID{tableID: physicalID, histID: col.ID})
			columnLength[col.Name.L] = length
		}
	}
	for _, length := range columnLength {
		dataLength += length
	}
	for _, idx := range info.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		for _, col := range idx.Columns {
			if col.Length == types.UnspecifiedLength {
				indexLength += columnLength[col.Name.L]
			} else {
				indexLength += rowCount * uint64(col.Length)
			}
		}
	}
	return dataLength, indexLength
}
