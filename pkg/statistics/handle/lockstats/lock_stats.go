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
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	lockAction     = "locking"
	unlockAction   = "unlocking"
	lockedStatus   = "locked"
	unlockedStatus = "unlocked"

	insertSQL = "INSERT INTO mysql.stats_table_locked (table_id) VALUES (%?) ON DUPLICATE KEY UPDATE table_id = %?"
)

// statsLockImpl implements the util.StatsLock interface.
type statsLockImpl struct {
	pool util.SessionPool
}

// NewStatsLock creates a new StatsLock.
func NewStatsLock(pool util.SessionPool) types.StatsLock {
	return &statsLockImpl{pool: pool}
}

// LockTables add locked tables id to store.
// - tables: tables that will be locked.
// Return the message of skipped tables and error.
func (sl *statsLockImpl) LockTables(tables map[int64]*types.StatsLockTable) (skipped string, err error) {
	err = util.CallWithSCtx(sl.pool, func(sctx sessionctx.Context) error {
		skipped, err = AddLockedTables(sctx, tables)
		return err
	}, util.FlagWrapTxn)
	return
}

// LockPartitions add locked partitions id to store.
// If the whole table is locked, then skip all partitions of the table.
// - tid: table id of which will be locked.
// - tableName: table name of which will be locked.
// - pidNames: partition ids of which will be locked.
// Return the message of skipped tables and error.
// Note: If the whole table is locked, then skip all partitions of the table.
func (sl *statsLockImpl) LockPartitions(
	tid int64,
	tableName string,
	pidNames map[int64]string,
) (skipped string, err error) {
	err = util.CallWithSCtx(sl.pool, func(sctx sessionctx.Context) error {
		skipped, err = AddLockedPartitions(sctx, tid, tableName, pidNames)
		return err
	}, util.FlagWrapTxn)
	return
}

// RemoveLockedTables remove tables from table locked records.
// - tables: tables of which will be unlocked.
// Return the message of skipped tables and error.
func (sl *statsLockImpl) RemoveLockedTables(tables map[int64]*types.StatsLockTable) (skipped string, err error) {
	err = util.CallWithSCtx(sl.pool, func(sctx sessionctx.Context) error {
		skipped, err = RemoveLockedTables(sctx, tables)
		return err
	}, util.FlagWrapTxn)
	return
}

// RemoveLockedPartitions remove partitions from table locked records.
// - tid: table id of which will be unlocked.
// - tableName: table name of which will be unlocked.
// - pidNames: partition ids of which will be unlocked.
// Note: If the whole table is locked, then skip all partitions of the table.
func (sl *statsLockImpl) RemoveLockedPartitions(
	tid int64,
	tableName string,
	pidNames map[int64]string,
) (skipped string, err error) {
	err = util.CallWithSCtx(sl.pool, func(sctx sessionctx.Context) error {
		skipped, err = RemoveLockedPartitions(sctx, tid, tableName, pidNames)
		return err
	}, util.FlagWrapTxn)
	return
}

// queryLockedTables query locked tables from store.
func (sl *statsLockImpl) queryLockedTables() (tables map[int64]struct{}, err error) {
	err = util.CallWithSCtx(sl.pool, func(sctx sessionctx.Context) error {
		tables, err = QueryLockedTables(sctx)
		return err
	})
	return
}

// GetLockedTables returns the locked status of the given tables.
// Note: This function query locked tables from store, so please try to batch the query.
func (sl *statsLockImpl) GetLockedTables(tableIDs ...int64) (map[int64]struct{}, error) {
	tableLocked, err := sl.queryLockedTables()
	if err != nil {
		return nil, err
	}

	return GetLockedTables(tableLocked, tableIDs...), nil
}

// GetTableLockedAndClearForTest for unit test only.
func (sl *statsLockImpl) GetTableLockedAndClearForTest() (map[int64]struct{}, error) {
	return sl.queryLockedTables()
}

var (
	// useCurrentSession to make sure the sql is executed in current session.
	useCurrentSession = []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}
)

// AddLockedTables add locked tables id to store.
// - exec: sql executor.
// - tables: tables that will be locked.
// Return the message of skipped tables and error.
func AddLockedTables(
	sctx sessionctx.Context,
	tables map[int64]*types.StatsLockTable,
) (string, error) {
	// Load tables to check duplicate before insert.
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
	logutil.StatsLogger().Info("lock table",
		zap.Any("tables", tables),
	)

	// Lock tables and partitions.
	lockedTablesAndPartitions := GetLockedTables(lockedTables, ids...)
	for tid, table := range tables {
		if _, ok := lockedTablesAndPartitions[tid]; !ok {
			if err := insertIntoStatsTableLocked(sctx, tid); err != nil {
				return "", err
			}
		} else {
			skippedTables = append(skippedTables, table.FullName)
		}

		for pid := range table.PartitionInfo {
			if _, ok := lockedTablesAndPartitions[pid]; !ok {
				if err := insertIntoStatsTableLocked(sctx, pid); err != nil {
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
	sctx sessionctx.Context,
	tid int64,
	tableName string,
	pidNames map[int64]string,
) (string, error) {
	// Load tables to check duplicate before insert.
	lockedTables, err := QueryLockedTables(sctx)
	if err != nil {
		return "", err
	}
	pids := make([]int64, 0, len(pidNames))
	pNames := make([]string, 0, len(pidNames))
	for pid, pName := range pidNames {
		pids = append(pids, pid)
		pNames = append(pNames, pName)
	}

	logutil.StatsLogger().Info("lock partitions",
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
			if err := insertIntoStatsTableLocked(sctx, pid); err != nil {
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

func insertIntoStatsTableLocked(sctx sessionctx.Context, tid int64) error {
	_, _, err := util.ExecRows(sctx, insertSQL, tid, tid)
	if err != nil {
		logutil.StatsLogger().Error("error occurred when insert mysql.stats_table_locked", zap.Error(err))
		return err
	}
	return nil
}
