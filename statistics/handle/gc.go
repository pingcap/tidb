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
	"encoding/json"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/statistics/handle/storage"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const gcLastTSVarName = "tidb_stats_gc_last_ts"

// GCStats will garbage collect the useless stats' info.
// For dropped tables, we will first update their version
// so that other tidb could know that table is deleted.
func (h *Handle) GCStats(is infoschema.InfoSchema, ddlLease time.Duration) (err error) {
	// To make sure that all the deleted tables' schema and stats info have been acknowledged to all tidb,
	// we only garbage collect version before 10 lease.
	lease := mathutil.Max(h.Lease(), ddlLease)
	offset := DurationToTS(10 * lease)
	now := oracle.GoTimeToTS(time.Now())
	if now < offset {
		return nil
	}

	// Get the last gc time.
	gcVer := now - offset
	lastGC, err := h.GetLastGCTimestamp()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			return
		}
		err = h.writeGCTimestampToKV(gcVer)
	}()

	rows, _, err := h.execRows("select table_id from mysql.stats_meta where version >= %? and version < %?", lastGC, gcVer)
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

// GetLastGCTimestamp loads the last gc time from mysql.tidb.
func (h *Handle) GetLastGCTimestamp() (uint64, error) {
	rows, _, err := h.execRows("SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name=%?", gcLastTSVarName)
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

func (h *Handle) writeGCTimestampToKV(newTS uint64) error {
	_, _, err := h.execRows(
		"insert into mysql.tidb (variable_name, variable_value) values (%?, %?) on duplicate key update variable_value = %?",
		gcLastTSVarName,
		newTS,
		newTS,
	)
	return err
}

func (h *Handle) gcTableStats(is infoschema.InfoSchema, physicalID int64) error {
	rows, _, err := h.execRows("select is_index, hist_id from mysql.stats_histograms where table_id = %?", physicalID)
	if err != nil {
		return errors.Trace(err)
	}
	// The table has already been deleted in stats and acknowledged to all tidb,
	// we can safely remove the meta info now.
	if len(rows) == 0 {
		_, _, err = h.execRows("delete from mysql.stats_meta where table_id = %?", physicalID)
		if err != nil {
			return errors.Trace(err)
		}
		cache.TableRowStatsCache.Invalidate(physicalID)
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
	rows, _, err = h.execRows("select name, column_ids from mysql.stats_extended where table_id = %? and status in (%?, %?)", physicalID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited)
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
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.ClearOutdatedHistoryStats(sctx)
	})
}

func (h *Handle) gcHistoryStatsFromKV(physicalID int64) error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.GCHistoryStatsFromKV(sctx, physicalID)
	}, flagWrapTxn)
}

// deleteHistStatsFromKV deletes all records about a column or an index and updates version.
func (h *Handle) deleteHistStatsFromKV(physicalID int64, histID int64, isIndex int) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.DeleteHistStatsFromKV(sctx, physicalID, histID, isIndex)
	}, flagWrapTxn)
}

// DeleteTableStatsFromKV deletes table statistics from kv.
// A statsID refers to statistic of a table or a partition.
func (h *Handle) DeleteTableStatsFromKV(statsIDs []int64) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.DeleteTableStatsFromKV(sctx, statsIDs)
	}, flagWrapTxn)
}

func (h *Handle) removeDeletedExtendedStats(version uint64) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.RemoveDeletedExtendedStats(sctx, version)
	}, flagWrapTxn)
}
