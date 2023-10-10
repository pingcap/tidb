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
	"slices"
	"strings"

	"github.com/pingcap/tidb/statistics/handle/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	lockAction     = "locking"
	unlockAction   = "unlocking"
	lockedStatus   = "locked"
	unlockedStatus = "unlocked"

	insertSQL = "INSERT INTO mysql.stats_table_locked (table_id) VALUES (%?) ON DUPLICATE KEY UPDATE table_id = %?"
)

var (
	// Stats logger.
	statsLogger = logutil.BgLogger().With(zap.String("category", "stats"))
	// useCurrentSession to make sure the sql is executed in current session.
	useCurrentSession = []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}
)

// TableInfo is the table info of which will be locked.
type TableInfo struct {
	PartitionInfo map[int64]string
	// schema name + table name.
	FullName string
}

// AddLockedTables add locked tables id to store.
// - exec: sql executor.
// - tables: tables that will be locked.
// Return the message of skipped tables and error.
func AddLockedTables(
	exec sqlexec.RestrictedSQLExecutor,
	tables map[int64]*TableInfo,
) (string, error) {
	ctx := util.StatsCtx(context.Background())
	err := startTransaction(ctx, exec)
	if err != nil {
		return "", err
	}
	defer func() {
		// Commit transaction.
		err = finishTransaction(ctx, exec, err)
	}()

	// Load tables to check duplicate before insert.
	lockedTables, err := QueryLockedTables(exec)
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
	statsLogger.Info("lock table",
		zap.Any("tables", tables),
	)

	// Lock tables and partitions.
	lockedTablesAndPartitions := GetLockedTables(lockedTables, ids...)
	for tid, table := range tables {
		if _, ok := lockedTablesAndPartitions[tid]; !ok {
			if err := insertIntoStatsTableLocked(ctx, exec, tid); err != nil {
				return "", err
			}
		} else {
			skippedTables = append(skippedTables, table.FullName)
		}

		for pid := range table.PartitionInfo {
			if _, ok := lockedTablesAndPartitions[pid]; !ok {
				if err := insertIntoStatsTableLocked(ctx, exec, pid); err != nil {
					return "", err
				}
			}
		}
	}

	msg := generateStableSkippedTablesMessage(len(tables), skippedTables, lockAction, lockedStatus)
	// Note: defer commit transaction, so we can't use `return nil` here.
	return msg, err
}

// AddLockedPartitions add locked partitions id to store.
// If the whole table is locked, then skip all partitions of the table.
// - exec: sql executor.
// - tid: table id of which will be locked.
// - tableName: table name of which will be locked.
// - pidNames: partition ids of which will be locked.
// Return the message of skipped tables and error.
func AddLockedPartitions(
	exec sqlexec.RestrictedSQLExecutor,
	tid int64,
	tableName string,
	pidNames map[int64]string,
) (string, error) {
	ctx := util.StatsCtx(context.Background())
	err := startTransaction(ctx, exec)
	if err != nil {
		return "", err
	}
	defer func() {
		// Commit transaction.
		err = finishTransaction(ctx, exec, err)
	}()

	// Load tables to check duplicate before insert.
	lockedTables, err := QueryLockedTables(exec)
	if err != nil {
		return "", err
	}
	pids := make([]int64, 0, len(pidNames))
	pNames := make([]string, 0, len(pidNames))
	for pid, pName := range pidNames {
		pids = append(pids, pid)
		pNames = append(pNames, pName)
	}

	statsLogger.Info("lock partitions",
		zap.Int64("tableID", tid),
		zap.String("tableName", tableName),
		zap.Int64s("partitionIDs", pids),
		zap.Strings("partitionNames", pNames),
	)

	// Check if whole table is locked.
	// Then we can skip locking partitions.
	// It is not necessary to lock partitions if whole table is locked.
	checkedTables := GetLockedTables(lockedTables, tid)
	if _, locked := checkedTables[tid]; locked {
		return "skip locking partitions of locked table: " + tableName, err
	}

	// Insert related partitions and warning already locked partitions.
	skippedPartitions := make([]string, 0, len(pids))
	lockedPartitions := GetLockedTables(lockedTables, pids...)
	for _, pid := range pids {
		if _, ok := lockedPartitions[pid]; !ok {
			if err := insertIntoStatsTableLocked(ctx, exec, pid); err != nil {
				return "", err
			}
		} else {
			skippedPartitions = append(skippedPartitions, pidNames[pid])
		}
	}

	msg := generateStableSkippedPartitionsMessage(pids, tableName, skippedPartitions, lockAction, lockedStatus)
	// Note: defer commit transaction, so we can't use `return nil` here.
	return msg, err
}

// generateStableSkippedTablesMessage generates stable skipped tables message.
func generateStableSkippedTablesMessage(tableCount int, skippedNames []string, action, status string) string {
	// Sort to stabilize the output.
	slices.Sort(skippedNames)

	if len(skippedNames) > 0 {
		tables := strings.Join(skippedNames, ", ")
		var msg string
		if tableCount > 1 {
			if tableCount > len(skippedNames) {
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

// generateStableSkippedPartitionsMessage generates stable skipped partitions message.
func generateStableSkippedPartitionsMessage(ids []int64, tableName string, skippedNames []string, action, status string) string {
	// Sort to stabilize the output.
	slices.Sort(skippedNames)

	if len(skippedNames) > 0 {
		partitions := strings.Join(skippedNames, ", ")
		var msg string
		if len(ids) > 1 {
			if len(ids) > len(skippedNames) {
				msg = fmt.Sprintf("skip %s %s partitions of table %s: %s, other partitions %s successfully", action, status, tableName, partitions, status)
			} else {
				msg = fmt.Sprintf("skip %s %s partitions of table %s: %s", action, status, tableName, partitions)
			}
		} else {
			msg = fmt.Sprintf("skip %s %s partition of table %s: %s", action, status, tableName, partitions)
		}
		return msg
	}
	return ""
}

func insertIntoStatsTableLocked(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, tid int64) error {
	_, _, err := exec.ExecRestrictedSQL(
		ctx,
		useCurrentSession,
		insertSQL, tid, tid,
	)
	if err != nil {
		logutil.BgLogger().Error("error occurred when insert mysql.stats_table_locked", zap.String("category", "stats"), zap.Error(err))
		return err
	}
	return nil
}
