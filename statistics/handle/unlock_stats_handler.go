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

package handle

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// RemoveLockedTables remove tables from table locked array
func (h *Handle) RemoveLockedTables(tids []int64, pids []int64, tables []*ast.TableName, maxChunkSize int) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)

	_, err := exec.ExecuteInternal(ctx, "BEGIN PESSIMISTIC")
	if err != nil {
		return "", err
	}

	// Load tables to check locked before delete.
	tableLocked, err := loadLockedTables(ctx, exec, maxChunkSize)
	if err != nil {
		return "", err
	}
	unlockedTables := make([]string, 0, len(tables))

	statsLogger.Info("unlock table", zap.Int64s("tableIDs", tids))
	for i, tid := range tids {
		var exist bool
		exist, tableLocked = removeIfTableLocked(tableLocked, tid)
		if !exist {
			unlockedTables = append(unlockedTables, tables[i].Schema.L+"."+tables[i].Name.L)
			continue
		}
		if err := updateStatsAndUnlockTable(ctx, exec, tid, maxChunkSize); err != nil {
			return "", err
		}
	}

	// Delete related partitions while don't warning delete empty partitions
	for _, tid := range pids {
		var exist bool
		exist, tableLocked = removeIfTableLocked(tableLocked, tid)
		if !exist {
			continue
		}
		if err := updateStatsAndUnlockTable(ctx, exec, tid, maxChunkSize); err != nil {
			return "", err
		}
	}

	err = finishTransaction(ctx, exec, err)
	if err != nil {
		return "", err
	}

	// Update handle.tableLocked after transaction success, if txn failed, tableLocked won't be updated.
	h.tableLocked = tableLocked

	mag := generateDuplicateTablesMessage(tids, unlockedTables, unlockAction, unlockedStatus)
	return mag, nil
}

func updateStatsAndUnlockTable(ctx context.Context, exec sqlexec.SQLExecutor, tid int64, maxChunkSize int) error {
	count, modifyCount, version, err := getStatsDeltaFromTableLocked(ctx, tid, exec, maxChunkSize)
	if err != nil {
		return err
	}

	if _, err := exec.ExecuteInternal(ctx,
		"UPDATE mysql.stats_meta SET version = %?, count = count + %?, modify_count = modify_count + %? WHERE table_id = %?",
		version, count, modifyCount, tid); err != nil {
		return err
	}
	cache.TableRowStatsCache.Invalidate(tid)

	if _, err := exec.ExecuteInternal(ctx, "DELETE FROM mysql.stats_table_locked WHERE table_id = %?", tid); err != nil {
		return err
	}

	return nil
}

// getStatsDeltaFromTableLocked get count, modify_count and version for the given table from mysql.stats_table_locked.
func getStatsDeltaFromTableLocked(ctx context.Context, tableID int64, exec sqlexec.SQLExecutor, maxChunkSize int) (count, modifyCount int64, version uint64, err error) {
	recordSet, err := exec.ExecuteInternal(ctx, "SELECT count, modify_count, version FROM mysql.stats_table_locked WHERE table_id = %?", tableID)
	if err != nil {
		return 0, 0, 0, err
	}
	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, maxChunkSize)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, 0, nil
	}

	count = rows[0].GetInt64(0)
	modifyCount = rows[0].GetInt64(1)
	version = rows[0].GetUint64(2)
	return count, modifyCount, version, nil
}
