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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	lockAction     = "locking"
	unlockAction   = "unlocking"
	lockedStatus   = "locked"
	unlockedStatus = "unlocked"
)

var (
	// maxChunkSize is the max chunk size for load locked tables.
	// We use 1024 as the default value, which is the same as the default value of session.maxChunkSize.
	// The reason why we don't use session.maxChunkSize is that we don't want to introduce a new dependency.
	// See: https://github.com/pingcap/tidb/pull/46478#discussion_r1308786474
	maxChunkSize = 1024
	// Stats logger.
	statsLogger = logutil.BgLogger().With(zap.String("category", "stats"))
)

// AddLockedTables add locked tables id to store.
// - exec: sql executor.
// - tids: table ids of which will be locked.
// - pids: partition ids of which will be locked.
// - tables: table names of which will be locked.
// Return the message of skipped tables and error.
func AddLockedTables(exec sqlexec.SQLExecutor, tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	_, err := exec.ExecuteInternal(ctx, "BEGIN PESSIMISTIC")
	if err != nil {
		return "", err
	}
	defer func() {
		// Commit transaction.
		err = finishTransaction(ctx, exec, err)
	}()

	// Load tables to check duplicate before insert.
	lockedTables, err := QueryLockedTables(ctx, exec)
	if err != nil {
		return "", err
	}

	skippedTables := make([]string, 0, len(tables))
	statsLogger.Info("lock table", zap.Int64s("tableIDs", tids))

	// Insert locked tables.
	lockedStatuses := GetTablesLockedStatuses(lockedTables, tids...)
	for i, tid := range tids {
		if !lockedStatuses[tid] {
			if err := insertIntoStatsTableLocked(ctx, exec, tid); err != nil {
				return "", err
			}
		} else {
			skippedTables = append(skippedTables, tables[i].Schema.L+"."+tables[i].Name.L)
		}
	}

	// Insert related partitions while don't warning duplicate partitions.
	lockedStatuses = GetTablesLockedStatuses(lockedTables, pids...)
	for _, pid := range pids {
		if !lockedStatuses[pid] {
			if err := insertIntoStatsTableLocked(ctx, exec, pid); err != nil {
				return "", err
			}
		}
	}

	msg := generateSkippedTablesMessage(tids, skippedTables, lockAction, lockedStatus)
	// Note: defer commit transaction, so we can't use `return nil` here.
	return msg, err
}

func generateSkippedTablesMessage(tids []int64, dupTables []string, action, status string) string {
	if len(dupTables) > 0 {
		tables := strings.Join(dupTables, ", ")
		var msg string
		if len(tids) > 1 {
			if len(tids) > len(dupTables) {
				msg = fmt.Sprintf("skip %s %s tables: %s, other tables %s successfully", action, status, tables, status)
			} else {
				msg = fmt.Sprintf("skip %s %s tables: %s", action, status, tables)
			}
		} else {
			msg = fmt.Sprintf("skip %s %s table: %s", action, status, tables)
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

// QueryLockedTables loads locked tables from mysql.stats_table_locked.
// Return it as a map for fast query.
func QueryLockedTables(ctx context.Context, exec sqlexec.SQLExecutor) (map[int64]struct{}, error) {
	recordSet, err := exec.ExecuteInternal(ctx, "SELECT table_id FROM mysql.stats_table_locked")
	if err != nil {
		return nil, err
	}
	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, maxChunkSize)
	if err != nil {
		return nil, err
	}
	tableLocked := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		tableLocked[row.GetInt64(0)] = struct{}{}
	}
	return tableLocked, nil
}

// GetTablesLockedStatuses check whether table is locked.
func GetTablesLockedStatuses(tableLocked map[int64]struct{}, tableIDs ...int64) map[int64]bool {
	lockedTableStatus := make(map[int64]bool, len(tableIDs))

	for _, tid := range tableIDs {
		if _, ok := tableLocked[tid]; ok {
			lockedTableStatus[tid] = true
			continue
		}
		lockedTableStatus[tid] = false
	}

	return lockedTableStatus
}

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(ctx context.Context, exec sqlexec.SQLExecutor, err error) error {
	if err == nil {
		_, err = exec.ExecuteInternal(ctx, "commit")
	} else {
		_, err1 := exec.ExecuteInternal(ctx, "rollback")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}
