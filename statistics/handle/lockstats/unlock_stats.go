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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	selectDeltaSQL = "SELECT count, modify_count, version FROM mysql.stats_table_locked WHERE table_id = %?"
	updateDeltaSQL = "UPDATE mysql.stats_meta SET version = %?, count = count + %?, modify_count = modify_count + %? WHERE table_id = %?"
	// DeleteLockSQL is used to delete the locked table record.
	DeleteLockSQL = "DELETE FROM mysql.stats_table_locked WHERE table_id = %?"
)

// RemoveLockedTables remove tables from table locked records.
// - exec: sql executor.
// - tidAndNames: table ids and names of which will be unlocked.
// - pidAndNames: partition ids and names of which will be unlocked.
// Return the message of skipped tables and error.
func RemoveLockedTables(
	exec sqlexec.RestrictedSQLExecutor,
	tidAndNames map[int64]string,
	pidAndNames map[int64]string,
) (string, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	err := startTransaction(ctx, exec)
	if err != nil {
		return "", err
	}
	defer func() {
		// Commit or rollback the transaction.
		err = finishTransaction(ctx, exec, err)
	}()

	// Load tables to check locked before delete.
	lockedTables, err := QueryLockedTables(exec)
	if err != nil {
		return "", err
	}
	skippedTables := make([]string, 0, len(tidAndNames))
	tids := make([]int64, 0, len(tidAndNames))
	tables := make([]string, 0, len(tidAndNames))
	for tid, tableName := range tidAndNames {
		tids = append(tids, tid)
		tables = append(tables, tableName)
	}
	pids := make([]int64, 0, len(pidAndNames))
	partitions := make([]string, 0, len(pidAndNames))
	for pid, partitionName := range pidAndNames {
		pids = append(pids, pid)
		partitions = append(partitions, partitionName)
	}

	statsLogger.Info("unlock table",
		zap.Int64s("tableIDs", tids),
		zap.Strings("tableNames", tables),
		zap.Int64s("partitionIDs", pids),
		zap.Strings("partitionNames", partitions),
	)

	checkedTables := GetLockedTables(lockedTables, tids...)
	for _, tid := range tids {
		if _, ok := checkedTables[tid]; !ok {
			skippedTables = append(skippedTables, tidAndNames[tid])
			continue
		}
		if err := updateStatsAndUnlockTable(ctx, exec, tid); err != nil {
			return "", err
		}
	}

	// Delete related partitions while don't warning delete empty partitions
	checkedTables = GetLockedTables(lockedTables, pids...)
	for _, pid := range pids {
		if _, ok := checkedTables[pid]; !ok {
			continue
		}
		if err := updateStatsAndUnlockTable(ctx, exec, pid); err != nil {
			return "", err
		}
	}

	msg := generateStableSkippedTablesMessage(tids, skippedTables, unlockAction, unlockedStatus)
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
	exec sqlexec.RestrictedSQLExecutor,
	tid int64,
	tableName string,
	pidNames map[int64]string,
) (string, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	err := startTransaction(ctx, exec)
	if err != nil {
		return "", err
	}
	defer func() {
		// Commit or rollback the transaction.
		err = finishTransaction(ctx, exec, err)
	}()

	// Load tables to check locked before delete.
	lockedTables, err := QueryLockedTables(exec)
	if err != nil {
		return "", err
	}

	pids := make([]int64, 0, len(pidNames))
	for pid := range pidNames {
		pids = append(pids, pid)
	}
	statsLogger.Info("unlock partitions",
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
		if err := updateStatsAndUnlockTable(ctx, exec, pid); err != nil {
			return "", err
		}
	}

	msg := generateStableSkippedPartitionsMessage(pids, tableName, skippedPartitions, unlockAction, unlockedStatus)
	// Note: defer commit transaction, so we can't use `return nil` here.
	return msg, err
}

func updateStatsAndUnlockTable(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, tid int64) error {
	count, modifyCount, version, err := getStatsDeltaFromTableLocked(ctx, tid, exec)
	if err != nil {
		return err
	}

	if _, _, err := exec.ExecRestrictedSQL(
		ctx,
		useCurrentSession,
		updateDeltaSQL,
		version, count, modifyCount, tid,
	); err != nil {
		return err
	}
	cache.TableRowStatsCache.Invalidate(tid)

	_, _, err = exec.ExecRestrictedSQL(
		ctx,
		useCurrentSession,
		DeleteLockSQL, tid,
	)
	return err
}

// getStatsDeltaFromTableLocked get count, modify_count and version for the given table from mysql.stats_table_locked.
func getStatsDeltaFromTableLocked(ctx context.Context, tableID int64, exec sqlexec.RestrictedSQLExecutor) (count, modifyCount int64, version uint64, err error) {
	rows, _, err := exec.ExecRestrictedSQL(
		ctx,
		useCurrentSession,
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
