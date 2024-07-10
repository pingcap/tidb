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

package lockstats

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

const (
	selectDeltaSQL = "SELECT count, modify_count, version FROM mysql.stats_table_locked WHERE table_id = %?"
	// Make sure the count won't be negative.
	updateDeltaSQL = "UPDATE mysql.stats_meta SET version = %?, count = IF(count + %? > 0, count + %?, 0), modify_count = modify_count + %? WHERE table_id = %?"
	// DeleteLockSQL is used to delete the locked table record.
	DeleteLockSQL = "DELETE FROM mysql.stats_table_locked WHERE table_id = %?"
)

// RemoveLockedTables remove tables from table locked records.
// - exec: sql executor.
// - tables: tables of which will be unlocked.
// Return the message of skipped tables and error.
func RemoveLockedTables(
	sctx sessionctx.Context,
	tables map[int64]*types.StatsLockTable,
) (string, error) {
	// Load tables to check locked before delete.
	lockedTables, err := QueryLockedTables(sctx)
	if err != nil {
		return "", err
	}
	skippedTables := make([]string, 0, len(tables))
	ids := make([]int64, 0, len(tables))
	for tid, table := range tables {
		ids = append(ids, tid)
		for pid := range table.PartitionInfo {
			ids = append(ids, pid)
		}
	}

	statslogutil.StatsLogger().Info("unlock table",
		zap.Any("tables", tables),
	)

	lockedTablesAndPartitions := GetLockedTables(lockedTables, ids...)

	for tid, table := range tables {
		if _, ok := lockedTablesAndPartitions[tid]; !ok {
			skippedTables = append(skippedTables, table.FullName)
			continue
		}
		if err := updateStatsAndUnlockTable(sctx, tid); err != nil {
			return "", err
		}

		// Delete related partitions while don't warning delete empty partitions
		for pid := range table.PartitionInfo {
			if _, ok := lockedTablesAndPartitions[pid]; !ok {
				continue
			}
			if err := updateStatsAndUnlockPartition(sctx, pid, tid); err != nil {
				return "", err
			}
		}
	}

	msg := generateStableSkippedTablesMessage(len(tables), skippedTables, unlockAction, unlockedStatus)
	// Note: defer commit transaction, so we can't use `return nil` here.
	return msg, err
}

// RemoveLockedPartitions remove partitions from table locked records.
// - exec: sql executor.
// - tid: table id of which will be unlocked.
// - tableName: table name of which will be unlocked.
// - pidNames: partition ids of which will be unlocked.
// Return the message of skipped tables and error.
func RemoveLockedPartitions(
	sctx sessionctx.Context,
	tid int64,
	tableName string,
	pidNames map[int64]string,
) (string, error) {
	// Load tables to check locked before delete.
	lockedTables, err := QueryLockedTables(sctx)
	if err != nil {
		return "", err
	}

	pids := make([]int64, 0, len(pidNames))
	for pid := range pidNames {
		pids = append(pids, pid)
	}
	statslogutil.StatsLogger().Info("unlock partitions",
		zap.Int64("tableID", tid),
		zap.String("tableName", tableName),
		zap.Int64s("partitionIDs", pids),
	)

	// Check if whole table is locked.
	// Then we can not unlock any partitions of the table.
	// It is invalid to unlock partitions if whole table is locked.
	checkedTables := GetLockedTables(lockedTables, tid)
	if _, locked := checkedTables[tid]; locked {
		return "skip unlocking partitions of locked table: " + tableName, err
	}

	// Delete related partitions and warning already unlocked partitions.
	skippedPartitions := make([]string, 0, len(pids))
	lockedPartitions := GetLockedTables(lockedTables, pids...)
	for _, pid := range pids {
		if _, ok := lockedPartitions[pid]; !ok {
			skippedPartitions = append(skippedPartitions, pidNames[pid])
			continue
		}
		if err := updateStatsAndUnlockPartition(sctx, pid, tid); err != nil {
			return "", err
		}
	}

	msg := generateStableSkippedPartitionsMessage(pids, tableName, skippedPartitions, unlockAction, unlockedStatus)
	// Note: defer commit transaction, so we can't use `return nil` here.
	return msg, err
}

func updateDelta(sctx sessionctx.Context, count, modifyCount int64, version uint64, tid int64) error {
	if _, _, err := util.ExecRows(sctx,
		updateDeltaSQL,
		version, count, count, modifyCount, tid,
	); err != nil {
		return err
	}

	return nil
}

func updateStatsAndUnlockTable(sctx sessionctx.Context, tid int64) error {
	count, modifyCount, version, err := getStatsDeltaFromTableLocked(sctx, tid)
	if err != nil {
		return err
	}

	if err := updateDelta(sctx, count, modifyCount, version, tid); err != nil {
		return err
	}
	cache.TableRowStatsCache.Invalidate(tid)

	_, _, err = util.ExecRows(
		sctx,
		DeleteLockSQL, tid,
	)
	return err
}

// updateStatsAndUnlockPartition also update the stats to the table level.
func updateStatsAndUnlockPartition(sctx sessionctx.Context, partitionID int64, tid int64) error {
	count, modifyCount, version, err := getStatsDeltaFromTableLocked(sctx, partitionID)
	if err != nil {
		return err
	}

	if err := updateDelta(sctx, count, modifyCount, version, partitionID); err != nil {
		return err
	}
	cache.TableRowStatsCache.Invalidate(partitionID)
	if err := updateDelta(sctx, count, modifyCount, version, tid); err != nil {
		return err
	}
	cache.TableRowStatsCache.Invalidate(tid)

	_, _, err = util.ExecRows(
		sctx,
		DeleteLockSQL, partitionID,
	)

	return err
}

// getStatsDeltaFromTableLocked get count, modify_count and version for the given table from mysql.stats_table_locked.
func getStatsDeltaFromTableLocked(sctx sessionctx.Context, tableID int64) (count, modifyCount int64, version uint64, err error) {
	rows, _, err := util.ExecRows(
		sctx,
		selectDeltaSQL, tableID,
	)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, 0, 0, nil
	}

	count = rows[0].GetInt64(0)
	modifyCount = rows[0].GetInt64(1)
	version = rows[0].GetUint64(2)
	return count, modifyCount, version, nil
}
