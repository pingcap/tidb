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

package usage

import (
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	utilstats "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// statsUsageImpl implements statstypes.StatsUsage.
type statsUsageImpl struct {
	statsHandle statstypes.StatsHandle

	// idxUsageCollector contains all the index usage collectors required by session.
	idxUsageCollector *indexusage.Collector

	// SessionStatsList contains all the stats collector required by session.
	*SessionStatsList
}

// NewStatsUsageImpl creates a statstypes.StatsUsage.
func NewStatsUsageImpl(statsHandle statstypes.StatsHandle) statstypes.StatsUsage {
	return &statsUsageImpl{
		statsHandle:       statsHandle,
		idxUsageCollector: indexusage.NewCollector(),
		SessionStatsList:  NewSessionStatsList()}
}

// LoadColumnStatsUsage returns all columns' usage information.
func (u *statsUsageImpl) LoadColumnStatsUsage(loc *time.Location) (colStatsMap map[model.TableItemID]statstypes.ColStatsTimeInfo, err error) {
	err = utilstats.CallWithSCtx(u.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		colStatsMap, err = LoadColumnStatsUsage(sctx, loc)
		return err
	})
	return
}

// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
func (u *statsUsageImpl) GetPredicateColumns(tableID int64) (columnIDs []int64, err error) {
	err = utilstats.CallWithSCtx(u.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		columnIDs, err = GetPredicateColumns(sctx, tableID)
		return err
	})
	return
}

// CollectColumnsInExtendedStats returns IDs of the columns involved in extended stats.
func (u *statsUsageImpl) CollectColumnsInExtendedStats(tableID int64) (columnIDs []int64, err error) {
	err = utilstats.CallWithSCtx(u.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		columnIDs, err = CollectColumnsInExtendedStats(sctx, tableID)
		return err
	})
	return
}

// LoadColumnStatsUsage loads column stats usage information from disk.
func LoadColumnStatsUsage(sctx sessionctx.Context, loc *time.Location) (map[model.TableItemID]statstypes.ColStatsTimeInfo, error) {
	disableTime, err := getDisableColumnTrackingTime(sctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Since we use another session from session pool to read mysql.column_stats_usage, which may have different @@time_zone, so we do time zone conversion here.
	rows, _, err := utilstats.ExecRows(sctx, "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage")
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
			// If `last_used_at` is before the time when `set global enable_column_tracking = 0`, we should ignore it because
			// `set global enable_column_tracking = 0` indicates all the predicate columns collected before.
			if disableTime == nil || gt.After(*disableTime) {
				t := types.NewTime(types.FromGoTime(gt.In(loc)), mysql.TypeTimestamp, types.DefaultFsp)
				statsUsage.LastUsedAt = &t
			}
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

// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
func GetPredicateColumns(sctx sessionctx.Context, tableID int64) ([]int64, error) {
	disableTime, err := getDisableColumnTrackingTime(sctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rows, _, err := utilstats.ExecRows(sctx, "SELECT column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %? AND last_used_at IS NOT NULL", tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	columnIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		colID := row.GetInt64(0)
		gt, err := row.GetTime(1).GoTime(time.UTC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// If `last_used_at` is before the time when `set global enable_column_tracking = 0`, we don't regard the column as predicate column because
		// `set global enable_column_tracking = 0` indicates all the predicate columns collected before.
		if disableTime == nil || gt.After(*disableTime) {
			columnIDs = append(columnIDs, colID)
		}
	}
	return columnIDs, nil
}

// getDisableColumnTrackingTime reads the value of tidb_disable_column_tracking_time from mysql.tidb if it exists.
func getDisableColumnTrackingTime(sctx sessionctx.Context) (*time.Time, error) {
	rows, fields, err := utilstats.ExecRows(sctx, "SELECT variable_value FROM %n.%n WHERE variable_name = %?", mysql.SystemDB, mysql.TiDBTable, variable.TiDBDisableColumnTrackingTime)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	// The string represents the UTC time when tidb_enable_column_tracking is set to 0.
	value, err := d.ToString()
	if err != nil {
		return nil, err
	}
	t, err := time.Parse(types.UTCTimeFormat, value)
	if err != nil {
		return nil, err
	}
	return &t, nil
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
