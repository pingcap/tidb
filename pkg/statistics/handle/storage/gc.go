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
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// statsGCImpl implements StatsGC interface.
type statsGCImpl struct {
	statsHandle types.StatsHandle
}

// NewStatsGC creates a new StatsGC.
func NewStatsGC(statsHandle types.StatsHandle) types.StatsGC {
	return &statsGCImpl{
		statsHandle: statsHandle,
	}
}

// GCStats will garbage collect the useless stats' info.
// For dropped tables, we will first update their version
// so that other tidb could know that table is deleted.
func (gc *statsGCImpl) GCStats(is infoschema.InfoSchema, ddlLease time.Duration) (err error) {
	return util.CallWithSCtx(gc.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return GCStats(sctx, gc.statsHandle, is, ddlLease)
	})
}

// ClearOutdatedHistoryStats clear outdated historical stats.
// Only for test.
func (gc *statsGCImpl) ClearOutdatedHistoryStats() error {
	return util.CallWithSCtx(gc.statsHandle.SPool(), ClearOutdatedHistoryStats)
}

// DeleteTableStatsFromKV deletes table statistics from kv.
// A statsID refers to statistic of a table or a partition.
func (gc *statsGCImpl) DeleteTableStatsFromKV(statsIDs []int64) (err error) {
	return util.CallWithSCtx(gc.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return DeleteTableStatsFromKV(sctx, statsIDs)
	}, util.FlagWrapTxn)
}

// GCStats will garbage collect the useless stats' info.
// For dropped tables, we will first update their version
// so that other tidb could know that table is deleted.
func GCStats(sctx sessionctx.Context,
	statsHandle types.StatsHandle,
	is infoschema.InfoSchema, ddlLease time.Duration) (err error) {
	// To make sure that all the deleted tables' schema and stats info have been acknowledged to all tidb,
	// we only garbage collect version before 10 lease.
	lease := max(statsHandle.Lease(), ddlLease)
	offset := util.DurationToTS(10 * lease)
	now := oracle.GoTimeToTS(time.Now())
	if now < offset {
		return nil
	}

	// Get the last gc time.
	gcVer := now - offset
	lastGC, err := getLastGCTimestamp(sctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			return
		}
		err = writeGCTimestampToKV(sctx, gcVer)
	}()

	rows, _, err := util.ExecRows(sctx, "select table_id from mysql.stats_meta where version >= %? and version < %?", lastGC, gcVer)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		if err := gcTableStats(sctx, statsHandle, is, row.GetInt64(0)); err != nil {
			return errors.Trace(err)
		}
		_, existed := is.TableByID(row.GetInt64(0))
		if !existed {
			if err := gcHistoryStatsFromKV(sctx, row.GetInt64(0)); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if err := ClearOutdatedHistoryStats(sctx); err != nil {
		logutil.BgLogger().Warn("failed to gc outdated historical stats",
			zap.Duration("duration", variable.HistoricalStatsDuration.Load()),
			zap.Error(err))
	}

	return removeDeletedExtendedStats(sctx, gcVer)
}

// DeleteTableStatsFromKV deletes table statistics from kv.
// A statsID refers to statistic of a table or a partition.
func DeleteTableStatsFromKV(sctx sessionctx.Context, statsIDs []int64) (err error) {
	startTS, err := util.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	for _, statsID := range statsIDs {
		// We only update the version so that other tidb will know that this table is deleted.
		if _, err = util.Exec(sctx, "update mysql.stats_meta set version = %? where table_id = %? ", startTS, statsID); err != nil {
			return err
		}
		if _, err = util.Exec(sctx, "delete from mysql.stats_histograms where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = util.Exec(sctx, "delete from mysql.stats_buckets where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = util.Exec(sctx, "delete from mysql.stats_top_n where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = util.Exec(sctx, "update mysql.stats_extended set version = %?, status = %? where table_id = %? and status in (%?, %?)", startTS, statistics.ExtendedStatsDeleted, statsID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited); err != nil {
			return err
		}
		if _, err = util.Exec(sctx, "delete from mysql.stats_fm_sketch where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = util.Exec(sctx, "delete from mysql.column_stats_usage where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = util.Exec(sctx, "delete from mysql.analyze_options where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = util.Exec(sctx, lockstats.DeleteLockSQL, statsID); err != nil {
			return err
		}
	}
	return nil
}

func forCount(total int64, batch int64) int64 {
	result := total / batch
	if total%batch > 0 {
		result++
	}
	return result
}

// ClearOutdatedHistoryStats clear outdated historical stats
func ClearOutdatedHistoryStats(sctx sessionctx.Context) error {
	sql := "select count(*) from mysql.stats_meta_history use index (idx_create_time) where create_time <= NOW() - INTERVAL %? SECOND"
	rs, err := util.Exec(sctx, sql, variable.HistoricalStatsDuration.Load().Seconds())
	if err != nil {
		return err
	}
	if rs == nil {
		return nil
	}
	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(context.Background(), rs, 8); err != nil {
		return errors.Trace(err)
	}
	count := rows[0].GetInt64(0)
	if count > 0 {
		for n := int64(0); n < forCount(count, int64(1000)); n++ {
			sql = "delete from mysql.stats_meta_history use index (idx_create_time) where create_time <= NOW() - INTERVAL %? SECOND limit 1000 "
			_, err = util.Exec(sctx, sql, variable.HistoricalStatsDuration.Load().Seconds())
			if err != nil {
				return err
			}
		}
		for n := int64(0); n < forCount(count, int64(50)); n++ {
			sql = "delete from mysql.stats_history use index (idx_create_time) where create_time <= NOW() - INTERVAL %? SECOND limit 50 "
			_, err = util.Exec(sctx, sql, variable.HistoricalStatsDuration.Load().Seconds())
			return err
		}
		logutil.BgLogger().Info("clear outdated historical stats")
	}
	return nil
}

// gcHistoryStatsFromKV delete history stats from kv.
func gcHistoryStatsFromKV(sctx sessionctx.Context, physicalID int64) (err error) {
	sql := "delete from mysql.stats_history where table_id = %?"
	_, err = util.Exec(sctx, sql, physicalID)
	if err != nil {
		return errors.Trace(err)
	}
	sql = "delete from mysql.stats_meta_history where table_id = %?"
	_, err = util.Exec(sctx, sql, physicalID)
	return err
}

// deleteHistStatsFromKV deletes all records about a column or an index and updates version.
func deleteHistStatsFromKV(sctx sessionctx.Context, physicalID int64, histID int64, isIndex int) (err error) {
	startTS, err := util.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	// First of all, we update the version. If this table doesn't exist, it won't have any problem. Because we cannot delete anything.
	if _, err = util.Exec(sctx, "update mysql.stats_meta set version = %? where table_id = %? ", startTS, physicalID); err != nil {
		return err
	}
	// delete histogram meta
	if _, err = util.Exec(sctx, "delete from mysql.stats_histograms where table_id = %? and hist_id = %? and is_index = %?", physicalID, histID, isIndex); err != nil {
		return err
	}
	// delete top n data
	if _, err = util.Exec(sctx, "delete from mysql.stats_top_n where table_id = %? and hist_id = %? and is_index = %?", physicalID, histID, isIndex); err != nil {
		return err
	}
	// delete all buckets
	if _, err = util.Exec(sctx, "delete from mysql.stats_buckets where table_id = %? and hist_id = %? and is_index = %?", physicalID, histID, isIndex); err != nil {
		return err
	}
	// delete all fm sketch
	if _, err := util.Exec(sctx, "delete from mysql.stats_fm_sketch where table_id = %? and hist_id = %? and is_index = %?", physicalID, histID, isIndex); err != nil {
		return err
	}
	if isIndex == 0 {
		// delete the record in mysql.column_stats_usage
		if _, err = util.Exec(sctx, "delete from mysql.column_stats_usage where table_id = %? and column_id = %?", physicalID, histID); err != nil {
			return err
		}
	}
	return nil
}

// removeDeletedExtendedStats removes deleted extended stats.
func removeDeletedExtendedStats(sctx sessionctx.Context, version uint64) (err error) {
	const sql = "delete from mysql.stats_extended where status = %? and version < %?"
	_, err = util.Exec(sctx, sql, statistics.ExtendedStatsDeleted, version)
	return
}

// gcTableStats GC this table's stats.
func gcTableStats(sctx sessionctx.Context,
	statsHandler types.StatsHandle,
	is infoschema.InfoSchema, physicalID int64) error {
	rows, _, err := util.ExecRows(sctx, "select is_index, hist_id from mysql.stats_histograms where table_id = %?", physicalID)
	if err != nil {
		return errors.Trace(err)
	}
	// The table has already been deleted in stats and acknowledged to all tidb,
	// we can safely remove the meta info now.
	if len(rows) == 0 {
		_, _, err = util.ExecRows(sctx, "delete from mysql.stats_meta where table_id = %?", physicalID)
		if err != nil {
			return errors.Trace(err)
		}
		cache.TableRowStatsCache.Invalidate(physicalID)
	}
	tbl, ok := statsHandler.TableInfoByID(is, physicalID)
	if !ok {
		logutil.BgLogger().Info("remove stats in GC due to dropped table", zap.Int64("table_id", physicalID))
		return util.WrapTxn(sctx, func(sctx sessionctx.Context) error {
			return errors.Trace(DeleteTableStatsFromKV(sctx, []int64{physicalID}))
		})
	}
	tblInfo := tbl.Meta()
	for _, row := range rows {
		isIndex, histID := row.GetInt64(0), row.GetInt64(1)
		find := false
		if isIndex == 1 {
			for _, idx := range tblInfo.Indices {
				if idx.ID == histID {
					find = true
					break
				}
			}
		} else {
			for _, col := range tblInfo.Columns {
				if col.ID == histID {
					find = true
					break
				}
			}
		}
		if !find {
			err := util.WrapTxn(sctx, func(sctx sessionctx.Context) error {
				return errors.Trace(deleteHistStatsFromKV(sctx, physicalID, histID, int(isIndex)))
			})
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// Mark records in mysql.stats_extended as `deleted`.
	rows, _, err = util.ExecRows(sctx, "select name, column_ids from mysql.stats_extended where table_id = %? and status in (%?, %?)", physicalID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil
	}
	for _, row := range rows {
		statsName, strColIDs := row.GetString(0), row.GetString(1)
		var colIDs []int64
		err = json.Unmarshal([]byte(strColIDs), &colIDs)
		if err != nil {
			logutil.BgLogger().Debug("decode column IDs failed", zap.String("column_ids", strColIDs), zap.Error(err))
			return errors.Trace(err)
		}
		for _, colID := range colIDs {
			found := false
			for _, col := range tblInfo.Columns {
				if colID == col.ID {
					found = true
					break
				}
			}
			if !found {
				logutil.BgLogger().Info("mark mysql.stats_extended record as 'deleted' in GC due to dropped columns", zap.String("table_name", tblInfo.Name.L), zap.Int64("table_id", physicalID), zap.String("stats_name", statsName), zap.Int64("dropped_column_id", colID))
				err = statsHandler.MarkExtendedStatsDeleted(statsName, physicalID, true)
				if err != nil {
					logutil.BgLogger().Debug("update stats_extended status failed", zap.String("stats_name", statsName), zap.Error(err))
					return errors.Trace(err)
				}
				break
			}
		}
	}
	return nil
}

const gcLastTSVarName = "tidb_stats_gc_last_ts"

// getLastGCTimestamp loads the last gc time from mysql.tidb.
func getLastGCTimestamp(sctx sessionctx.Context) (uint64, error) {
	rows, _, err := util.ExecRows(sctx, "SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name=%?", gcLastTSVarName)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	lastGcTSString := rows[0].GetString(0)
	lastGcTS, err := strconv.ParseUint(lastGcTSString, 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return lastGcTS, nil
}

// writeGCTimestampToKV write the GC timestamp to the storage.
func writeGCTimestampToKV(sctx sessionctx.Context, newTS uint64) error {
	_, _, err := util.ExecRows(sctx,
		"insert into mysql.tidb (variable_name, variable_value) values (%?, %?) on duplicate key update variable_value = %?",
		gcLastTSVarName,
		newTS,
		newTS,
	)
	return err
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func MarkExtendedStatsDeleted(sctx sessionctx.Context,
	statsCache types.StatsCache,
	statsName string, tableID int64, ifExists bool) (statsVer uint64, err error) {
	rows, _, err := util.ExecRows(sctx, "SELECT name FROM mysql.stats_extended WHERE name = %? and table_id = %? and status in (%?, %?)", statsName, tableID, statistics.ExtendedStatsInited, statistics.ExtendedStatsAnalyzed)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) == 0 {
		if ifExists {
			return 0, nil
		}
		return 0, fmt.Errorf("extended statistics '%s' for the specified table does not exist", statsName)
	}
	if len(rows) > 1 {
		logutil.BgLogger().Warn("unexpected duplicate extended stats records found", zap.String("name", statsName), zap.Int64("table_id", tableID))
	}

	defer func() {
		if err == nil {
			removeExtendedStatsItem(statsCache, tableID, statsName)
		}
	}()
	version, err := util.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if _, err = util.Exec(sctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
		return 0, err
	}
	statsVer = version
	if _, err = util.Exec(sctx, "UPDATE mysql.stats_extended SET version = %?, status = %? WHERE name = %? and table_id = %?", version, statistics.ExtendedStatsDeleted, statsName, tableID); err != nil {
		return 0, err
	}
	return
}
