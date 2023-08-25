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
	"github.com/pingcap/tidb/statistics/handle/cache/internal/lfu"
	"github.com/pingcap/tidb/statistics/handle/cache/internal/mapcache"
	"github.com/pingcap/tidb/statistics/handle/cache/internal/metrics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/syncutil"
	"go.uber.org/zap"
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
func NewStatsCache() (*StatsCache, error) {
	enableQuota := config.GetGlobalConfig().Performance.EnableStatsCacheMemQuota
	if enableQuota {
		capacity := variable.StatsCacheMemQuota.Load()
		stats, err := lfu.NewLFU(capacity)
		if err != nil {
			return nil, err
		}
		return &StatsCache{
			c: stats,
		}, nil
	}
	return &StatsCache{
		c: mapcache.NewMapCache(),
	}, nil
}

// StatsCache caches the tables in memory for Handle.
type StatsCache struct {
	c internal.StatsCacheInner
	// the max table stats version the cache has in its lifecycle.
	maxTblStatsVer atomic.Uint64
}

// Len returns the number of tables in the cache.
func (sc *StatsCache) Len() int {
	return sc.c.Len()
}

// GetFromUser returns the statistics of the specified Table ID.
// The returned value should be read-only, if you update it, don't forget to use Put to put it back again, otherwise the memory trace can be inaccurate.
//
//	e.g. v := sc.Get(id); /* update the value */ v.Version = 123; sc.Put(id, v);
func (sc *StatsCache) GetFromUser(id int64) (*statistics.Table, bool) {
	return sc.getCache(id, true)
}

func (sc *StatsCache) getCache(id int64, moveFront bool) (*statistics.Table, bool) {
	result, ok := sc.c.Get(id, moveFront)
	if ok {
		metrics.HitCounter.Add(1)
	} else {
		metrics.MissCounter.Add(1)
	}
	return result, ok
}

// GetFromInternal returns the statistics of the specified Table ID.
func (sc *StatsCache) GetFromInternal(id int64) (*statistics.Table, bool) {
	return sc.getCache(id, false)
}

// Put puts the table statistics to the cache from query.
func (sc *StatsCache) Put(id int64, t *statistics.Table) {
	sc.put(id, t)
}

func (sc *StatsCache) putCache(id int64, t *statistics.Table) bool {
	ok := sc.c.Put(id, t)
	if ok {
		return ok
	}
	// TODO(hawkingrei): If necessary, add asynchronous retries
	logutil.BgLogger().Warn("fail to put the stats cache", zap.Int64("id", id))
	return ok
}

// Put puts the table statistics to the cache.
func (sc *StatsCache) put(id int64, t *statistics.Table) {
	ok := sc.putCache(id, t)
	if !ok {
		logutil.BgLogger().Warn("fail to put the stats cache", zap.Int64("id", id))
		return
	}
	// update the maxTblStatsVer
	for v := sc.maxTblStatsVer.Load(); v < t.Version; v = sc.maxTblStatsVer.Load() {
		if sc.maxTblStatsVer.CompareAndSwap(v, t.Version) {
			break
		} // other goroutines have updated the sc.maxTblStatsVer, so we need to check again.
	}
}

// Values returns all the cached statistics tables.
func (sc *StatsCache) Values() []*statistics.Table {
	return sc.c.Values()
}

// Cost returns the memory usage of the cache.
func (sc *StatsCache) Cost() int64 {
	return sc.c.Cost()
}

// SetCapacity sets the memory capacity of the cache.
func (sc *StatsCache) SetCapacity(c int64) {
	// metrics will be updated in the SetCapacity function of the StatsCacheInner.
	sc.c.SetCapacity(c)
}

// Close stops the cache.
func (sc *StatsCache) Close() {
	sc.c.Close()
}

// Version returns the version of the current cache, which is defined as
// the max table stats version the cache has in its lifecycle.
func (sc *StatsCache) Version() uint64 {
	return sc.maxTblStatsVer.Load()
}

// CopyAndUpdate copies a new cache and updates the new statistics table cache. It is only used in the COW mode.
func (sc *StatsCache) CopyAndUpdate(tables []*statistics.Table, deletedIDs []int64, opts ...TableStatsOpt) *StatsCache {
	option := &TableStatsOption{}
	for _, opt := range opts {
		opt(option)
	}
	newCache := &StatsCache{c: sc.c.Copy()}
	newCache.maxTblStatsVer.Store(sc.maxTblStatsVer.Load())
	for _, tbl := range tables {
		id := tbl.PhysicalID
		newCache.c.Put(id, tbl)
	}
	for _, id := range deletedIDs {
		newCache.c.Del(id)
	}

	// update the maxTblStatsVer
	for _, t := range tables {
		if t.Version > newCache.maxTblStatsVer.Load() {
			newCache.maxTblStatsVer.Store(t.Version)
		}
	}
	return newCache
}

// Update updates the new statistics table cache.
func (sc *StatsCache) Update(tables []*statistics.Table, deletedIDs []int64, opts ...TableStatsOpt) {
	option := &TableStatsOption{}
	for _, opt := range opts {
		opt(option)
	}
	for _, tbl := range tables {
		id := tbl.PhysicalID
		sc.c.Put(id, tbl)
	}
	for _, id := range deletedIDs {
		sc.c.Del(id)
	}

	// update the maxTblStatsVer
	for _, t := range tables {
		if oldVersion := sc.maxTblStatsVer.Load(); t.Version > oldVersion {
			sc.maxTblStatsVer.CompareAndSwap(oldVersion, t.Version)
		}
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
