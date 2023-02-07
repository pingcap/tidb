// Copyright 2018 PingCAP, Inc.
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
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// GCStats will garbage collect the useless stats info. For dropped tables, we will first update their version so that
// other tidb could know that table is deleted.
func (h *Handle) GCStats(is infoschema.InfoSchema, ddlLease time.Duration) error {
	ctx := context.Background()
	// To make sure that all the deleted tables' schema and stats info have been acknowledged to all tidb,
	// we only garbage collect version before 10 lease.
	lease := mathutil.Max(h.Lease(), ddlLease)
	offset := DurationToTS(10 * lease)
	now := oracle.GoTimeToTS(time.Now())
	if now < offset {
		return nil
	}
	gcVer := now - offset
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_meta where version < %?", gcVer)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		if err := h.gcTableStats(is, row.GetInt64(0)); err != nil {
			return errors.Trace(err)
		}
		_, existed := is.TableByID(row.GetInt64(0))
		if !existed {
			if err := h.gcHistoryStatsFromKV(row.GetInt64(0)); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if err := h.ClearOutdatedHistoryStats(); err != nil {
		logutil.BgLogger().Warn("failed to gc outdated historical stats",
			zap.Duration("duration", variable.HistoricalStatsDuration.Load()),
			zap.Error(err))
	}
	return h.removeDeletedExtendedStats(gcVer)
}

func (h *Handle) gcTableStats(is infoschema.InfoSchema, physicalID int64) error {
	ctx := context.Background()
	rows, _, err := h.execRestrictedSQL(ctx, "select is_index, hist_id from mysql.stats_histograms where table_id = %?", physicalID)
	if err != nil {
		return errors.Trace(err)
	}
	// The table has already been deleted in stats and acknowledged to all tidb,
	// we can safely remove the meta info now.
	if len(rows) == 0 {
		_, _, err = h.execRestrictedSQL(ctx, "delete from mysql.stats_meta where table_id = %?", physicalID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	tbl, ok := h.getTableByPhysicalID(is, physicalID)
	if !ok {
		logutil.BgLogger().Info("remove stats in GC due to dropped table", zap.Int64("table_id", physicalID))
		return errors.Trace(h.DeleteTableStatsFromKV([]int64{physicalID}))
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
			if err := h.deleteHistStatsFromKV(physicalID, histID, int(isIndex)); err != nil {
				return errors.Trace(err)
			}
		}
	}
	// Mark records in mysql.stats_extended as `deleted`.
	rows, _, err = h.execRestrictedSQL(ctx, "select name, column_ids from mysql.stats_extended where table_id = %? and status in (%?, %?)", physicalID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited)
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
				err = h.MarkExtendedStatsDeleted(statsName, physicalID, true)
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

// ClearOutdatedHistoryStats clear outdated historical stats
func (h *Handle) ClearOutdatedHistoryStats() error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	sql := "select count(*) from mysql.stats_meta_history where NOW() - create_time >= %?"
	rs, err := exec.ExecuteInternal(ctx, sql, variable.HistoricalStatsDuration.Load().Seconds())
	if err != nil {
		return err
	}
	if rs == nil {
		return nil
	}
	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return errors.Trace(err)
	}
	count := rows[0].GetInt64(0)
	if count > 0 {
		sql = "delete from mysql.stats_meta_history where NOW() - create_time >= %?"
		_, err = exec.ExecuteInternal(ctx, sql, variable.HistoricalStatsDuration.Load().Seconds())
		if err != nil {
			return err
		}
		sql = "delete from mysql.stats_history where NOW() - create_time >= %? "
		_, err = exec.ExecuteInternal(ctx, sql, variable.HistoricalStatsDuration.Load().Seconds())
		logutil.BgLogger().Info("clear outdated historical stats")
		return err
	}
	return nil
}

func (h *Handle) gcHistoryStatsFromKV(physicalID int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	_, err := exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	sql := "delete from mysql.stats_history where table_id = %?"
	_, err = exec.ExecuteInternal(ctx, sql, physicalID)
	if err != nil {
		return errors.Trace(err)
	}
	sql = "delete from mysql.stats_meta_history where table_id = %?"
	_, err = exec.ExecuteInternal(ctx, sql, physicalID)
	return err
}

// deleteHistStatsFromKV deletes all records about a column or an index and updates version.
func (h *Handle) deleteHistStatsFromKV(physicalID int64, histID int64, isIndex int) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	// First of all, we update the version. If this table doesn't exist, it won't have any problem. Because we cannot delete anything.
	if _, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %? where table_id = %? ", startTS, physicalID); err != nil {
		return err
	}
	// delete histogram meta
	if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_histograms where table_id = %? and hist_id = %? and is_index = %?", physicalID, histID, isIndex); err != nil {
		return err
	}
	// delete top n data
	if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_top_n where table_id = %? and hist_id = %? and is_index = %?", physicalID, histID, isIndex); err != nil {
		return err
	}
	// delete all buckets
	if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_buckets where table_id = %? and hist_id = %? and is_index = %?", physicalID, histID, isIndex); err != nil {
		return err
	}
	// delete all fm sketch
	if _, err := exec.ExecuteInternal(ctx, "delete from mysql.stats_fm_sketch where table_id = %? and hist_id = %? and is_index = %?", physicalID, histID, isIndex); err != nil {
		return err
	}
	if isIndex == 0 {
		// delete the record in mysql.column_stats_usage
		if _, err = exec.ExecuteInternal(ctx, "delete from mysql.column_stats_usage where table_id = %? and column_id = %?", physicalID, histID); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTableStatsFromKV deletes table statistics from kv.
// A statsID refers to statistic of a table or a partition.
func (h *Handle) DeleteTableStatsFromKV(statsIDs []int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	for _, statsID := range statsIDs {
		// We only update the version so that other tidb will know that this table is deleted.
		if _, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %? where table_id = %? ", startTS, statsID); err != nil {
			return err
		}
		if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_histograms where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_buckets where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_top_n where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_feedback where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = exec.ExecuteInternal(ctx, "update mysql.stats_extended set version = %?, status = %? where table_id = %? and status in (%?, %?)", startTS, statistics.ExtendedStatsDeleted, statsID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited); err != nil {
			return err
		}
		if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_fm_sketch where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = exec.ExecuteInternal(ctx, "delete from mysql.column_stats_usage where table_id = %?", statsID); err != nil {
			return err
		}
		if _, err = exec.ExecuteInternal(ctx, "delete from mysql.analyze_options where table_id = %?", statsID); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handle) removeDeletedExtendedStats(version uint64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	const sql = "delete from mysql.stats_extended where status = %? and version < %?"
	_, err = exec.ExecuteInternal(ctx, sql, statistics.ExtendedStatsDeleted, version)
	return
}
