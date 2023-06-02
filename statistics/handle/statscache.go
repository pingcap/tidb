// Copyright 2022 PingCAP, Inc.
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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/refers"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/syncutil"
)

// statsCacheInner is the interface to manage the statsCache, it can be implemented by map, lru cache or other structures.
type statsCacheInner interface {
	GetByQuery(int64) (*statistics.Table, bool)
	Get(int64) (*statistics.Table, bool)
	PutByQuery(int64, *statistics.Table)
	Put(int64, *statistics.Table)
	Del(int64)
	Cost() int64
	Keys() []int64
	Values() []*statistics.Table
	Map() map[int64]*statistics.Table
	Len() int
	FreshMemUsage()
	Copy() statsCacheInner
	SetCapacity(int64)
	// Front returns the front element's owner tableID, only used for test
	Front() int64
}

func newStatsCache() statsCache {
	enableQuota := config.GetGlobalConfig().Performance.EnableStatsCacheMemQuota
	if enableQuota {
		capacity := variable.StatsCacheMemQuota.Load()
		return statsCache{
			statsCacheInner: newStatsLruCache(capacity),
		}
	}
	return statsCache{
		statsCacheInner: &mapCache{
			tables:   make(map[int64]cacheItem),
			memUsage: 0,
		},
	}
}

// statsCache caches the tables in memory for Handle.
type statsCache struct {
	statsCacheInner
	// version is the latest version of cache. It is bumped when new records of `mysql.stats_meta` are loaded into cache.
	version uint64
	// minorVersion is to differentiate the cache when the version is unchanged while the cache contents are
	// modified indeed. This can happen when we load extra column histograms into cache, or when we modify the cache with
	// statistics feedbacks, etc. We cannot bump the version then because no new changes of `mysql.stats_meta` are loaded,
	// while the override of statsCache is in a copy-on-write way, to make sure the statsCache is unchanged by others during the
	// the interval of 'copy' and 'write', every 'write' should bump / check this minorVersion if the version keeps
	// unchanged.
	// This bump / check logic is encapsulated in `statsCache.update` and `updateStatsCache`, callers don't need to care
	// about this minorVersion actually.
	minorVersion uint64
}

func (sc statsCache) copy() statsCache {
	newCache := statsCache{
		version:      sc.version,
		minorVersion: sc.minorVersion,
	}
	newCache.statsCacheInner = sc.statsCacheInner.Copy()
	return newCache
}

// update updates the statistics table cache using copy on write.
func (sc statsCache) update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64, opts ...TableStatsOpt) statsCache {
	option := &tableStatsOption{}
	for _, opt := range opts {
		opt(option)
	}
	newCache := sc.copy()
	if newVersion == newCache.version {
		newCache.minorVersion += uint64(1)
	} else {
		newCache.version = newVersion
		newCache.minorVersion = uint64(0)
	}
	for _, tbl := range tables {
		id := tbl.PhysicalID
		if option.byQuery {
			newCache.PutByQuery(id, tbl)
		} else {
			newCache.Put(id, tbl)
		}
	}
	for _, id := range deletedIDs {
		newCache.Del(id)
	}
	return newCache
}

type cacheInternalItem struct {
	value *statistics.Table
	key   int64
	cost  int64
}

func (c cacheInternalItem) copy() cacheInternalItem {
	return cacheInternalItem{
		key:   c.key,
		value: c.value,
		cost:  c.cost,
	}
}

type cacheItem struct {
	*cacheInternalItem
	refs refers.Refs[cacheInternalItem]
}

type mapCache struct {
	tables   map[int64]cacheItem
	memUsage int64
}

// GetByQuery implements statsCacheInner
func (m *mapCache) GetByQuery(k int64) (*statistics.Table, bool) {
	return m.Get(k)
}

// Get implements statsCacheInner
func (m *mapCache) Get(k int64) (*statistics.Table, bool) {
	v, ok := m.tables[k]
	return v.value, ok
}

// PutByQuery implements statsCacheInner
func (m *mapCache) PutByQuery(k int64, v *statistics.Table) {
	m.Put(k, v)
}

// Put implements statsCacheInner
func (m *mapCache) Put(k int64, v *statistics.Table) {
	item, ok := m.tables[k]
	if ok {
		oldCost := item.cost
		newCost := v.MemoryUsage().TotalMemUsage
		item.value = v
		item.cost = newCost
		m.tables[k] = item
		m.memUsage += newCost - oldCost
		return
	}
	cost := v.MemoryUsage().TotalMemUsage
	item = cacheItem{
		key:   k,
		value: v,
		cost:  cost,
	}
	m.tables[k] = item
	m.memUsage += cost
}

// Del implements statsCacheInner
func (m *mapCache) Del(k int64) {
	item, ok := m.tables[k]
	if !ok {
		return
	}
	delete(m.tables, k)
	m.memUsage -= item.cost
}

// Cost implements statsCacheInner
func (m *mapCache) Cost() int64 {
	return m.memUsage
}

// Keys implements statsCacheInner
func (m *mapCache) Keys() []int64 {
	ks := make([]int64, 0, len(m.tables))
	for k := range m.tables {
		ks = append(ks, k)
	}
	return ks
}

// Values implements statsCacheInner
func (m *mapCache) Values() []*statistics.Table {
	vs := make([]*statistics.Table, 0, len(m.tables))
	for _, v := range m.tables {
		vs = append(vs, v.value)
	}
	return vs
}

// Map implements statsCacheInner
func (m *mapCache) Map() map[int64]*statistics.Table {
	t := make(map[int64]*statistics.Table, len(m.tables))
	for k, v := range m.tables {
		t[k] = v.value
	}
	return t
}

// Len implements statsCacheInner
func (m *mapCache) Len() int {
	return len(m.tables)
}

// FreshMemUsage implements statsCacheInner
func (m *mapCache) FreshMemUsage() {
	for _, v := range m.tables {
		oldCost := v.cost
		newCost := v.value.MemoryUsage().TotalMemUsage
		m.memUsage += newCost - oldCost
	}
}

// Copy implements statsCacheInner
func (m *mapCache) Copy() statsCacheInner {
	newM := &mapCache{
		tables:   make(map[int64]cacheItem, len(m.tables)),
		memUsage: m.memUsage,
	}
	for k, v := range m.tables {
		newM.tables[k] = v.copy()
	}
	return newM
}

// SetCapacity implements statsCacheInner
func (m *mapCache) SetCapacity(int64) {}

// Front implements statsCacheInner
func (m *mapCache) Front() int64 {
	return 0
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
func (c *StatsTableRowCache) GetDataAndIndexLength(info *model.TableInfo, physicalID int64, rowCount uint64) (uint64, uint64) {
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
	dataLength, indexLength := uint64(0), uint64(0)
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
