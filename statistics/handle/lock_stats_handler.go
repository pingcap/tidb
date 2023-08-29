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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// Stats logger.
var statsLogger = logutil.BgLogger().With(zap.String("category", "stats"))

// AddLockedTables add locked tables id to store.
func (h *Handle) AddLockedTables(tids []int64, pids []int64, tables []*ast.TableName, maxChunkSize int) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)

	_, err := exec.ExecuteInternal(ctx, "BEGIN PESSIMISTIC")
	if err != nil {
		return "", err
	}

	// Load tables to check duplicate before insert.
	recordSet, err := exec.ExecuteInternal(ctx, "SELECT table_id FROM mysql.stats_table_locked")
	if err != nil {
		return "", err
	}
	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, maxChunkSize)
	if err != nil {
		return "", err
	}

	dupTables := make([]string, 0, len(tables))
	tableLocked := make([]int64, 0, len(rows))
	for _, row := range rows {
		tableLocked = append(tableLocked, row.GetInt64(0))
	}

	statsLogger.Info("lock table", zap.Int64s("tableIDs", tids))

	// Insert locked tables.
	for i, tid := range tids {
		if !isTableLocked(tableLocked, tid) {
			if err := insertIntoStatsTableLocked(ctx, exec, tid); err != nil {
				return "", err
			}
			tableLocked = append(tableLocked, tid)
		} else {
			dupTables = append(dupTables, tables[i].Schema.L+"."+tables[i].Name.L)
		}
	}

	// Insert related partitions while don't warning duplicate partitions.
	for _, tid := range pids {
		if !isTableLocked(tableLocked, tid) {
			if err := insertIntoStatsTableLocked(ctx, exec, tid); err != nil {
				return "", err
			}
			tableLocked = append(tableLocked, tid)
		}
	}

	// Commit transaction.
	err = finishTransaction(ctx, exec, err)
	if err != nil {
		return "", err
	}

	// Update handle.tableLocked after transaction success, if txn failed, tableLocked won't be updated.
	h.tableLocked = tableLocked

	mag := generateDuplicateTablesMessage(tids, dupTables)
	return mag, nil
}

func generateDuplicateTablesMessage(tids []int64, dupTables []string) string {
	if len(dupTables) > 0 {
		tables := strings.Join(dupTables, ", ")
		var msg string
		if len(tids) > 1 {
			if len(tids) > len(dupTables) {
				msg = "skip locking locked tables: " + tables + ", other tables locked successfully"
			} else {
				msg = "skip locking locked tables: " + tables
			}
		} else {
			msg = "skip locking locked table: " + tables
		}
		return msg
	}

	return ""
}

func insertIntoStatsTableLocked(ctx context.Context, exec sqlexec.SQLExecutor, tid int64) error {
	_, err := exec.ExecuteInternal(ctx, "INSERT INTO mysql.stats_table_locked (table_id) VALUES (%?) ON DUPLICATE KEY UPDATE table_id = %?", tid, tid)
	if err != nil {
		logutil.BgLogger().Error("error occurred when insert mysql.stats_table_locked", zap.String("category", "stats"), zap.Error(err))
		return err
	}
	return nil
}

// RemoveLockedTables remove tables from table locked array
func (h *Handle) RemoveLockedTables(tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return "", err
	}

	//load tables to check unlock the unlock table
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_table_locked")
	if err != nil {
		return "", err
	}

	nonlockedTables := make([]string, 0)
	tableLocked := make([]int64, 0)
	for _, row := range rows {
		tableLocked = append(tableLocked, row.GetInt64(0))
	}

	strTids := fmt.Sprintf("%v", tids)
	logutil.BgLogger().Info("unlock table ", zap.String("category", "stats"), zap.String("tableIDs", strTids))
	for i, tid := range tids {
		// get stats delta during table locked
		count, modifyCount, version, err := h.getStatsDeltaFromTableLocked(ctx, tid)
		if err != nil {
			logutil.BgLogger().Error("error occurred when getStatsDeltaFromTableLocked", zap.String("category", "stats"), zap.Error(err))
			return "", err
		}
		// update stats_meta with stats delta
		_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?", version, count, modifyCount, tid)
		if err != nil {
			logutil.BgLogger().Error("error occurred when update mysql.stats_meta", zap.String("category", "stats"), zap.Error(err))
			return "", err
		}
		cache.TableRowStatsCache.Invalidate(tid)

		_, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_table_locked where table_id = %?", tid)
		if err != nil {
			logutil.BgLogger().Error("error occurred when delete from mysql.stats_table_locked ", zap.String("category", "stats"), zap.Error(err))
			return "", err
		}
		var exist bool
		exist, tableLocked = removeIfTableLocked(tableLocked, tid)
		if !exist {
			nonlockedTables = append(nonlockedTables, tables[i].Schema.L+"."+tables[i].Name.L)
		}
	}
	//delete related partitions while don't warning delete empty partitions
	for _, tid := range pids {
		// get stats delta during table locked
		count, modifyCount, version, err := h.getStatsDeltaFromTableLocked(ctx, tid)
		if err != nil {
			logutil.BgLogger().Error("error occurred when getStatsDeltaFromTableLocked", zap.String("category", "stats"), zap.Error(err))
			return "", err
		}
		// update stats_meta with stats delta
		_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?", version, count, modifyCount, tid)
		if err != nil {
			logutil.BgLogger().Error("error occurred when update mysql.stats_meta", zap.String("category", "stats"), zap.Error(err))
			return "", err
		}
		cache.TableRowStatsCache.Invalidate(tid)

		_, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_table_locked where table_id = %?", tid)
		if err != nil {
			logutil.BgLogger().Error("error occurred when delete from mysql.stats_table_locked ", zap.String("category", "stats"), zap.Error(err))
			return "", err
		}
		_, tableLocked = removeIfTableLocked(tableLocked, tid)
	}

	err = finishTransaction(ctx, exec, err)
	if err != nil {
		return "", err
	}
	// update handle.tableLocked after transaction success, if txn failed, tableLocked won't be updated
	h.tableLocked = tableLocked

	if len(nonlockedTables) > 0 {
		tables := nonlockedTables[0]
		for i, table := range nonlockedTables {
			if i == 0 {
				continue
			}
			tables += ", " + table
		}
		var msg string
		if len(tids) > 1 {
			if len(tids) > len(nonlockedTables) {
				msg = "skip unlocking non-locked tables: " + tables + ", other tables unlocked successfully"
			} else {
				msg = "skip unlocking non-locked tables: " + tables
			}
		} else {
			msg = "skip unlocking non-locked table: " + tables
		}
		return msg, err
	}
	return "", err
}

// getStatsDeltaFromTableLocked get count, modify_count and version for the given table from mysql.stats_table_locked.
func (h *Handle) getStatsDeltaFromTableLocked(ctx context.Context, tableID int64) (count, modifyCount int64, version uint64, err error) {
	rows, _, err := h.execRestrictedSQL(ctx, "select count, modify_count, version from mysql.stats_table_locked where table_id = %?", tableID)
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

// LoadLockedTables load locked tables from store
func (h *Handle) LoadLockedTables() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_table_locked")
	if err != nil {
		return errors.Trace(err)
	}

	h.tableLocked = make([]int64, len(rows))
	for i, row := range rows {
		h.tableLocked[i] = row.GetInt64(0)
	}

	return nil
}

// removeIfTableLocked try to remove the table from table locked array
func removeIfTableLocked(tableLocked []int64, tableID int64) (bool, []int64) {
	idx := lockTableIndexOf(tableLocked, tableID)
	if idx > -1 {
		tableLocked = append(tableLocked[:idx], tableLocked[idx+1:]...)
	}
	return idx > -1, tableLocked
}

// IsTableLocked check whether table is locked in handle with Handle.Mutex
func (h *Handle) IsTableLocked(tableID int64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.isTableLocked(tableID)
}

// IsTableLocked check whether table is locked in handle without Handle.Mutex
func (h *Handle) isTableLocked(tableID int64) bool {
	return isTableLocked(h.tableLocked, tableID)
}

// isTableLocked check whether table is locked
func isTableLocked(tableLocked []int64, tableID int64) bool {
	return lockTableIndexOf(tableLocked, tableID) > -1
}

// lockTableIndexOf get the locked table's index in the array
func lockTableIndexOf(tableLocked []int64, tableID int64) int {
	for idx, id := range tableLocked {
		if id == tableID {
			return idx
		}
	}
	return -1
}

// GetTableLockedAndClearForTest for unit test only
func (h *Handle) GetTableLockedAndClearForTest() []int64 {
	tableLocked := h.tableLocked
	h.tableLocked = make([]int64, 0)
	return tableLocked
}
