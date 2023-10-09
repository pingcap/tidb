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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/lockstats"
	"github.com/pingcap/tidb/statistics/handle/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

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
		sql = "delete from mysql.stats_meta_history use index (idx_create_time) where create_time <= NOW() - INTERVAL %? SECOND"
		_, err = util.Exec(sctx, sql, variable.HistoricalStatsDuration.Load().Seconds())
		if err != nil {
			return err
		}
		sql = "delete from mysql.stats_history use index (idx_create_time) where create_time <= NOW() - INTERVAL %? SECOND"
		_, err = util.Exec(sctx, sql, variable.HistoricalStatsDuration.Load().Seconds())
		logutil.BgLogger().Info("clear outdated historical stats")
		return err
	}
	return nil
}

// GCHistoryStatsFromKV delete history stats from kv.
func GCHistoryStatsFromKV(sctx sessionctx.Context, physicalID int64) (err error) {
	sql := "delete from mysql.stats_history where table_id = %?"
	_, err = util.Exec(sctx, sql, physicalID)
	if err != nil {
		return errors.Trace(err)
	}
	sql = "delete from mysql.stats_meta_history where table_id = %?"
	_, err = util.Exec(sctx, sql, physicalID)
	return err
}

// DeleteHistStatsFromKV deletes all records about a column or an index and updates version.
func DeleteHistStatsFromKV(sctx sessionctx.Context, physicalID int64, histID int64, isIndex int) (err error) {
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

// RemoveDeletedExtendedStats removes deleted extended stats.
func RemoveDeletedExtendedStats(sctx sessionctx.Context, version uint64) (err error) {
	const sql = "delete from mysql.stats_extended where status = %? and version < %?"
	_, err = util.Exec(sctx, sql, statistics.ExtendedStatsDeleted, version)
	return
}
