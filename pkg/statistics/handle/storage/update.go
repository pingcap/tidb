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
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// UpdateStatsVersion will set statistics version to the newest TS,
// then tidb-server will reload automatic.
func UpdateStatsVersion(sctx sessionctx.Context) error {
	startTS, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_meta set version = %?", startTS); err != nil {
		return err
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_extended set version = %?", startTS); err != nil {
		return err
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_histograms set version = %?", startTS); err != nil {
		return err
	}
	return nil
}

// UpdateStatsMeta update the stats meta stat for this Table.
func UpdateStatsMeta(
	sctx sessionctx.Context,
	startTS uint64,
	delta variable.TableDelta,
	id int64,
	isLocked bool,
) (err error) {
	if isLocked {
		// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_table_locked.
		// Note: For locked tables, it is possible that the record gets deleted. So it can be negative.
		_, err = statsutil.Exec(sctx, "insert into mysql.stats_table_locked (version, table_id, modify_count, count) values (%?, %?, %?, %?) on duplicate key "+
			"update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)",
			startTS, id, delta.Count, delta.Delta)
	} else {
		if delta.Delta < 0 {
			// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_meta.
			_, err = statsutil.Exec(sctx, "insert into mysql.stats_meta (version, table_id, modify_count, count) values (%?, %?, %?, 0) on duplicate key "+
				"update version = values(version), modify_count = modify_count + values(modify_count), count = if(count > %?, count - %?, 0)",
				startTS, id, delta.Count, -delta.Delta, -delta.Delta)
		} else {
			// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_meta.
			_, err = statsutil.Exec(sctx, "insert into mysql.stats_meta (version, table_id, modify_count, count) values (%?, %?, %?, %?) on duplicate key "+
				"update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)", startTS,
				id, delta.Count, delta.Delta)
		}
		cache.TableRowStatsCache.Invalidate(id)
	}
	return err
}

// DumpTableStatColSizeToKV dumps the column size stats to storage.
func DumpTableStatColSizeToKV(sctx sessionctx.Context, id int64, delta variable.TableDelta) error {
	if len(delta.ColSize) == 0 {
		return nil
	}
	values := make([]string, 0, len(delta.ColSize))
	for histID, deltaColSize := range delta.ColSize {
		if deltaColSize == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d, 0, %d, 0, %d)", id, histID, deltaColSize))
	}
	if len(values) == 0 {
		return nil
	}
	sql := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, tot_col_size) "+
		"values %s on duplicate key update tot_col_size = GREATEST(0, tot_col_size + values(tot_col_size))", strings.Join(values, ","))
	_, _, err := statsutil.ExecRows(sctx, sql)
	return errors.Trace(err)
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func InsertExtendedStats(sctx sessionctx.Context,
	statsCache types.StatsCache,
	statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (statsVer uint64, err error) {
	slices.Sort(colIDs)
	bytes, err := json.Marshal(colIDs)
	if err != nil {
		return 0, errors.Trace(err)
	}
	strColIDs := string(bytes)

	// No need to use `exec.ExecuteInternal` since we have acquired the lock.
	rows, _, err := statsutil.ExecRows(sctx, "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)", tableID, statistics.ExtendedStatsInited, statistics.ExtendedStatsAnalyzed)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for _, row := range rows {
		currStatsName := row.GetString(0)
		currTp := row.GetInt64(1)
		currStrColIDs := row.GetString(2)
		if currStatsName == statsName {
			if ifNotExists {
				return 0, nil
			}
			return 0, errors.Errorf("extended statistics '%s' for the specified table already exists", statsName)
		}
		if tp == int(currTp) && currStrColIDs == strColIDs {
			return 0, errors.Errorf("extended statistics '%s' with same type on same columns already exists", statsName)
		}
	}
	version, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// Bump version in `mysql.stats_meta` to trigger stats cache refresh.
	if _, err = statsutil.Exec(sctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
		return 0, err
	}
	statsVer = version
	// Remove the existing 'deleted' records.
	if _, err = statsutil.Exec(sctx, "DELETE FROM mysql.stats_extended WHERE name = %? and table_id = %?", statsName, tableID); err != nil {
		return 0, err
	}
	// Remove the cache item, which is necessary for cases like a cluster with 3 tidb instances, e.g, a, b and c.
	// If tidb-a executes `alter table drop stats_extended` to mark the record as 'deleted', and before this operation
	// is synchronized to other tidb instances, tidb-b executes `alter table add stats_extended`, which would delete
	// the record from the table, tidb-b should delete the cached item synchronously. While for tidb-c, it has to wait for
	// next `Update()` to remove the cached item then.
	removeExtendedStatsItem(statsCache, tableID, statsName)
	const sql = "INSERT INTO mysql.stats_extended(name, type, table_id, column_ids, version, status) VALUES (%?, %?, %?, %?, %?, %?)"
	if _, err = statsutil.Exec(sctx, sql, statsName, tp, tableID, strColIDs, version, statistics.ExtendedStatsInited); err != nil {
		return 0, err
	}
	return
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func SaveExtendedStatsToStorage(sctx sessionctx.Context,
	tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (statsVer uint64, err error) {
	if extStats == nil || len(extStats.Stats) == 0 {
		return 0, nil
	}

	version, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for name, item := range extStats.Stats {
		bytes, err := json.Marshal(item.ColIDs)
		if err != nil {
			return 0, errors.Trace(err)
		}
		strColIDs := string(bytes)
		var statsStr string
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			statsStr = fmt.Sprintf("%f", item.ScalarVals)
		case ast.StatsTypeDependency:
			statsStr = item.StringVals
		}
		// If isLoad is true, it's INSERT; otherwise, it's UPDATE.
		if _, err := statsutil.Exec(sctx, "replace into mysql.stats_extended values (%?, %?, %?, %?, %?, %?, %?)", name, item.Tp, tableID, strColIDs, statsStr, version, statistics.ExtendedStatsAnalyzed); err != nil {
			return 0, err
		}
	}
	if !isLoad {
		if _, err := statsutil.Exec(sctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
			return 0, err
		}
		statsVer = version
	}
	return statsVer, nil
}

func removeExtendedStatsItem(statsCache types.StatsCache,
	tableID int64, statsName string) {
	tbl, ok := statsCache.Get(tableID)
	if !ok || tbl.ExtendedStats == nil || len(tbl.ExtendedStats.Stats) == 0 {
		return
	}
	newTbl := tbl.Copy()
	delete(newTbl.ExtendedStats.Stats, statsName)
	statsCache.UpdateStatsCache([]*statistics.Table{newTbl}, nil)
}
