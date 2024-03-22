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

// TableRowStatsCache is the cache of table row count.

package cache

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/syncutil"
)

// TableRowStatsCache is the cache of table row count.
var TableRowStatsCache = &StatsTableRowCache{
	tableRows: make(map[int64]uint64),
	colLength: make(map[tableHistID]uint64),
}

// tableStatsCacheExpiry is the expiry time for table stats cache.
var tableStatsCacheExpiry = 3 * time.Second

type tableHistID struct {
	tableID int64
	histID  int64
}

// StatsTableRowCache is used to cache the count of table rows.
type StatsTableRowCache struct {
	modifyTime time.Time
	tableRows  map[int64]uint64
	colLength  map[tableHistID]uint64
	dirtyIDs   []int64
	mu         syncutil.RWMutex
}

// Invalidate invalidates the cache of the table with id.
func (c *StatsTableRowCache) Invalidate(tblID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// To prevent the cache from becoming too large,
	// we only record the latest 100 dirty tables that have been modified.
	if len(c.dirtyIDs) < 100 {
		c.dirtyIDs = append(c.dirtyIDs, tblID)
	}
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

func (c *StatsTableRowCache) updateDirtyIDs(sctx sessionctx.Context) error {
	if len(c.dirtyIDs) > 0 {
		tableRows, err := getRowCountTables(sctx, c.dirtyIDs...)
		if err != nil {
			return err
		}
		for id, tr := range tableRows {
			c.tableRows[id] = tr
		}
		colLength, err := getColLengthTables(sctx, c.dirtyIDs...)
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

// UpdateByID tries to update the cache by table ID.
func (c *StatsTableRowCache) UpdateByID(sctx sessionctx.Context, id int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.modifyTime) < tableStatsCacheExpiry {
		return c.updateDirtyIDs(sctx)
	}
	tableRows, err := getRowCountTables(sctx, id)
	if err != nil {
		return err
	}
	colLength, err := getColLengthTables(sctx, id)
	if err != nil {
		return err
	}
	c.tableRows[id] = tableRows[id]
	for k, v := range colLength {
		c.colLength[k] = v
	}
	c.modifyTime = time.Now()
	return nil
}

// Update tries to update the cache.
func (c *StatsTableRowCache) Update(sctx sessionctx.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.modifyTime) < tableStatsCacheExpiry {
		return c.updateDirtyIDs(sctx)
	}
	tableRows, err := getRowCountTables(sctx)
	if err != nil {
		return err
	}
	colLength, err := getColLengthTables(sctx)
	if err != nil {
		return err
	}
	c.tableRows = tableRows
	c.colLength = colLength
	c.modifyTime = time.Now()
	c.dirtyIDs = c.dirtyIDs[:0]
	return nil
}

// EstimateDataLength returns the estimated data length in bytes of a given table info.
// Returns row count, average row length, total data length, and all indexed column length.
func (c *StatsTableRowCache) EstimateDataLength(table *model.TableInfo) (
	rowCount uint64, avgRowLength uint64, dataLength uint64, indexLength uint64) {
	if table.GetPartitionInfo() == nil {
		rowCount = c.GetTableRows(table.ID)
		dataLength, indexLength = c.GetDataAndIndexLength(table, table.ID, rowCount)
	} else {
		for _, pi := range table.GetPartitionInfo().Definitions {
			piRowCnt := c.GetTableRows(pi.ID)
			rowCount += piRowCnt
			parDataLen, parIndexLen := c.GetDataAndIndexLength(table, pi.ID, piRowCnt)
			dataLength += parDataLen
			indexLength += parIndexLen
		}
	}
	avgRowLength = uint64(0)
	if rowCount != 0 {
		avgRowLength = dataLength / rowCount
	}

	if table.IsSequence() {
		// sequence is always 1 row regardless of stats.
		rowCount = 1
	}
	return
}

func getRowCountTables(sctx sessionctx.Context, tableIDs ...int64) (map[int64]uint64, error) {
	var rows []chunk.Row
	var err error
	if len(tableIDs) == 0 {
		rows, _, err = util.ExecWithOpts(sctx, nil, "select table_id, count from mysql.stats_meta")
	} else {
		inTblIDs := buildInTableIDsString(tableIDs)
		sql := "select table_id, count from mysql.stats_meta where " + inTblIDs
		rows, _, err = util.ExecWithOpts(sctx, nil, sql)
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

func getColLengthTables(sctx sessionctx.Context, tableIDs ...int64) (map[tableHistID]uint64, error) {
	var rows []chunk.Row
	var err error
	if len(tableIDs) == 0 {
		sql := "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0"
		rows, _, err = util.ExecWithOpts(sctx, nil, sql)
	} else {
		inTblIDs := buildInTableIDsString(tableIDs)
		sql := "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0 and " + inTblIDs
		rows, _, err = util.ExecWithOpts(sctx, nil, sql)
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
