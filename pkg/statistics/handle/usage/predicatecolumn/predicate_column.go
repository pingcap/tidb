// Copyright 2024 PingCAP, Inc.
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

package predicatecolumn

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	utilstats "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// loadColumnStatsUsage is a helper function to load column stats usage information from disk.
func loadColumnStatsUsage(sctx sessionctx.Context, loc *time.Location, query string, args ...any) (map[model.TableItemID]statstypes.ColStatsTimeInfo, error) {
	rows, _, err := utilstats.ExecRows(sctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	colStatsMap := make(map[model.TableItemID]statstypes.ColStatsTimeInfo, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		tblColID := model.TableItemID{TableID: row.GetInt64(0), ID: row.GetInt64(1), IsIndex: false}
		var statsUsage statstypes.ColStatsTimeInfo
		if !row.IsNull(2) {
			gt, err := row.GetTime(2).GoTime(time.UTC)
			if err != nil {
				return nil, errors.Trace(err)
			}
			t := types.NewTime(types.FromGoTime(gt.In(loc)), mysql.TypeTimestamp, types.DefaultFsp)
			statsUsage.LastUsedAt = &t
		}
		if !row.IsNull(3) {
			gt, err := row.GetTime(3).GoTime(time.UTC)
			if err != nil {
				return nil, errors.Trace(err)
			}
			t := types.NewTime(types.FromGoTime(gt.In(loc)), mysql.TypeTimestamp, types.DefaultFsp)
			statsUsage.LastAnalyzedAt = &t
		}
		colStatsMap[tblColID] = statsUsage
	}
	return colStatsMap, nil
}

// LoadColumnStatsUsage loads column stats usage information from disk.
func LoadColumnStatsUsage(sctx sessionctx.Context, loc *time.Location) (map[model.TableItemID]statstypes.ColStatsTimeInfo, error) {
	query := "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage"
	return loadColumnStatsUsage(sctx, loc, query)
}

// LoadColumnStatsUsageForTable loads column stats usage information for a specific table from disk.
func LoadColumnStatsUsageForTable(sctx sessionctx.Context, loc *time.Location, tableID int64) (map[model.TableItemID]statstypes.ColStatsTimeInfo, error) {
	query := "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %?"
	return loadColumnStatsUsage(sctx, loc, query, tableID)
}

// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
func GetPredicateColumns(sctx sessionctx.Context, tableID int64) ([]int64, error) {
	// Each time we retrieve the predicate columns, we also attempt to remove any column stats usage information whose column is dropped.
	err := cleanupDroppedColumnStatsUsage(sctx, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rows, _, err := utilstats.ExecRows(
		sctx,
		"SELECT column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %? AND last_used_at IS NOT NULL",
		tableID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	columnIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		// Usually, it should not be NULL.
		// This only happens when the last_used_at is not a valid time.
		if row.IsNull(1) {
			continue
		}
		colID := row.GetInt64(0)
		columnIDs = append(columnIDs, colID)
	}
	return columnIDs, nil
}

// cleanupDroppedColumnStatsUsage deletes the column stats usage information whose column is dropped.
func cleanupDroppedColumnStatsUsage(sctx sessionctx.Context, tableID int64) error {
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	table, ok := is.TableByID(context.Background(), tableID)
	if !ok {
		// Usually, it should not happen.
		// But if it happens, we can safely do nothing.
		return nil
	}
	allColumns := table.Meta().Columns
	// Due to SQL limitations, column IDs must be converted to strings for proper escaping in the query :(
	columnIDs := make([]string, 0, len(allColumns))
	for _, col := range allColumns {
		columnIDs = append(columnIDs, fmt.Sprintf("%d", col.ID))
	}

	// Delete the column stats usage information whose column is dropped.
	_, _, err := utilstats.ExecRows(
		sctx,
		"DELETE FROM mysql.column_stats_usage WHERE table_id = %? AND column_id NOT IN (%?)",
		tableID,
		columnIDs,
	)

	return err
}

// SaveColumnStatsUsageForTable saves column stats usage information for a specific table to disk.
func SaveColumnStatsUsageForTable(
	sctx sessionctx.Context,
	colStatsUsage map[model.TableItemID]statstypes.ColStatsTimeInfo,
) error {
	for colID, statsUsage := range colStatsUsage {
		lastUsedAt := "NULL"
		if statsUsage.LastUsedAt != nil {
			lastUsedAt = statsUsage.LastUsedAt.String()
		}
		lastAnalyzedAt := "NULL"
		if statsUsage.LastAnalyzedAt != nil {
			lastAnalyzedAt = statsUsage.LastAnalyzedAt.String()
		}
		_, _, err := utilstats.ExecRows(
			sctx,
			"REPLACE INTO mysql.column_stats_usage (table_id, column_id, last_used_at, last_analyzed_at) VALUES (%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))",
			colID.TableID, colID.ID, lastUsedAt, lastAnalyzedAt,
		)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CollectColumnsInExtendedStats returns IDs of the columns involved in extended stats.
func CollectColumnsInExtendedStats(sctx sessionctx.Context, tableID int64) ([]int64, error) {
	const sql = "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)"
	rows, _, err := utilstats.ExecRows(sctx, sql, tableID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	columnIDs := make([]int64, 0, len(rows)*2)
	for _, row := range rows {
		twoIDs := make([]int64, 0, 2)
		data := row.GetString(2)
		err := json.Unmarshal([]byte(data), &twoIDs)
		if err != nil {
			logutil.BgLogger().Error("invalid column_ids in mysql.stats_extended, skip collecting extended stats for this row", zap.String("column_ids", data), zap.Error(err))
			continue
		}
		columnIDs = append(columnIDs, twoIDs...)
	}
	return columnIDs, nil
}
