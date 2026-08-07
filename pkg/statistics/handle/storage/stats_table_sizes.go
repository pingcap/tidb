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

// StatsTableSizes is a per-query read of table row counts and column sizes.

package storage

import (
	"strconv"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type tableHistID struct {
	tableID int64
	histID  int64
}

// StatsTableSizes holds the row counts and per-column sizes of a fixed set of
// tables, used to fill the statistics-derived columns (TABLE_ROWS,
// AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH) of information_schema.tables and
// information_schema.partitions.
//
// It is read directly from mysql.stats_meta and mysql.stats_histograms per query
// (see GetStatsTableSizes) and is not shared between queries, so it needs no
// locking. The row counts and column sizes are read as two separate statements
// and are therefore not guaranteed mutually consistent, which is acceptable for
// these approximate estimate columns.
type StatsTableSizes struct {
	tableRows map[int64]uint64
	colLength map[tableHistID]uint64
}

// GetStatsTableSizes reads the row counts of the given tables from
// mysql.stats_meta and, when needColLength is true, their per-column sizes from
// mysql.stats_histograms. The read of mysql.stats_histograms is skipped when
// needColLength is false, since it can be expensive on clusters with many
// tables; see https://github.com/pingcap/tidb/issues/69818.
func GetStatsTableSizes(sctx sessionctx.Context, needColLength bool, ids ...int64) (*StatsTableSizes, error) {
	tableRows, err := getRowCountTables(sctx, ids...)
	if err != nil {
		return nil, err
	}
	colLength := map[tableHistID]uint64{}
	if needColLength {
		colLength, err = getColLengthTables(sctx, ids...)
		if err != nil {
			return nil, err
		}
	}
	return &StatsTableSizes{tableRows: tableRows, colLength: colLength}, nil
}

// GetTableRows returns the row count of the table. A nil receiver returns zero,
// so callers can skip building a StatsTableSizes when no statistics column is
// requested.
func (s *StatsTableSizes) GetTableRows(id int64) uint64 {
	if s == nil {
		return 0
	}
	return s.tableRows[id]
}

// GetColLength returns the total size in bytes of the column summed across all
// rows (mysql.stats_histograms.tot_col_size). A nil receiver returns zero.
func (s *StatsTableSizes) GetColLength(id tableHistID) uint64 {
	if s == nil {
		return 0
	}
	return s.colLength[id]
}

// EstimateDataLength returns the estimated data length in bytes of a given table info.
// Returns row count, average row length, total data length, and all indexed column length.
func (s *StatsTableSizes) EstimateDataLength(table *model.TableInfo) (
	rowCount uint64, avgRowLength uint64, dataLength uint64, indexLength uint64) {
	rowCount = s.GetTableRows(table.ID)
	dataLength, indexLength = s.GetDataAndIndexLength(table, table.ID, rowCount)
	if table.GetPartitionInfo() != nil {
		// For partition table, data only stores in partition level.
		// Keep `indexLength` for global index.
		rowCount, dataLength = 0, 0
		for _, pi := range table.GetPartitionInfo().Definitions {
			piRowCnt := s.GetTableRows(pi.ID)
			rowCount += piRowCnt
			parDataLen, parIndexLen := s.GetDataAndIndexLength(table, pi.ID, piRowCnt)
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
	// Signals that mysql.stats_histograms is about to be read, letting tests
	// assert that TABLE_ROWS-only queries skip this read. See issue #69818.
	failpoint.InjectCall("getColLengthTables")
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
		totalSize := max(row.GetInt64(2), 0)
		colLengthMap[tableHistID{tableID: tableID, histID: histID}] = uint64(totalSize)
	}
	return colLengthMap, nil
}

// GetDataAndIndexLength gets the data and index length of the table.
func (s *StatsTableSizes) GetDataAndIndexLength(info *model.TableInfo, physicalID int64, rowCount uint64) (dataLength, indexLength uint64) {
	columnLength := make([]uint64, len(info.Columns))
	for i, col := range info.Columns {
		if col.State != model.StatePublic {
			continue
		}
		var length uint64
		if storageLen := col.FieldType.StorageLength(); storageLen != types.VarStorageLen {
			length = rowCount * uint64(storageLen)
		} else {
			length = s.GetColLength(tableHistID{tableID: physicalID, histID: col.ID})
		}
		dataLength += length
		columnLength[i] = length
	}

	for _, idx := range info.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		if info.GetPartitionInfo() != nil {
			// Global indexes calculated in table level.
			if idx.Global && info.ID != physicalID {
				continue
			}
			// Normal indexes calculated in partition level.
			if !idx.Global && info.ID == physicalID {
				continue
			}
		}
		for _, col := range idx.Columns {
			if col.Length == types.UnspecifiedLength {
				indexLength += columnLength[col.Offset]
			} else {
				indexLength += rowCount * uint64(col.Length)
			}
		}
	}
	return dataLength, indexLength
}
