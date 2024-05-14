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

package importer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
)

const (
	maxRetryOnStatusConflict = 30
	maxBackoffTime           = 30 * time.Second
)

type metaMgrBuilder interface {
	Init(ctx context.Context) error
	TaskMetaMgr(pd *pdutil.PdController) taskMetaMgr
	TableMetaMgr(tr *TableImporter) tableMetaMgr
}

type dbMetaMgrBuilder struct {
	db           *sql.DB
	taskID       int64
	schema       string
	needChecksum bool
}

func (b *dbMetaMgrBuilder) Init(ctx context.Context) error {
	exec := common.SQLWithRetry{
		DB:           b.db,
		Logger:       log.FromContext(ctx),
		HideQueryLog: redact.NeedRedact(),
	}
	metaDBSQL := common.SprintfWithIdentifiers("CREATE DATABASE IF NOT EXISTS %s", b.schema)
	if err := exec.Exec(ctx, "create meta schema", metaDBSQL); err != nil {
		return errors.Annotate(err, "create meta schema failed")
	}
	taskMetaSQL := common.SprintfWithIdentifiers(CreateTaskMetaTable, b.schema, TaskMetaTableName)
	if err := exec.Exec(ctx, "create meta table", taskMetaSQL); err != nil {
		return errors.Annotate(err, "create task meta table failed")
	}
	tableMetaSQL := common.SprintfWithIdentifiers(CreateTableMetadataTable, b.schema, TableMetaTableName)
	if err := exec.Exec(ctx, "create meta table", tableMetaSQL); err != nil {
		return errors.Annotate(err, "create table meta table failed")
	}
	return nil
}

func (b *dbMetaMgrBuilder) TaskMetaMgr(pd *pdutil.PdController) taskMetaMgr {
	return &dbTaskMetaMgr{
		session:    b.db,
		taskID:     b.taskID,
		pd:         pd,
		tableName:  TaskMetaTableName,
		schemaName: b.schema,
	}
}

func (b *dbMetaMgrBuilder) TableMetaMgr(tr *TableImporter) tableMetaMgr {
	return &dbTableMetaMgr{
		session:      b.db,
		taskID:       b.taskID,
		tr:           tr,
		schemaName:   b.schema,
		tableName:    TableMetaTableName,
		needChecksum: b.needChecksum,
	}
}

type tableMetaMgr interface {
	InitTableMeta(ctx context.Context) error
	AllocTableRowIDs(ctx context.Context, rawRowIDMax int64) (*verify.KVChecksum, int64, error)
	UpdateTableStatus(ctx context.Context, status metaStatus) error
	UpdateTableBaseChecksum(ctx context.Context, checksum *verify.KVChecksum) error
	CheckAndUpdateLocalChecksum(ctx context.Context, checksum *verify.KVChecksum, hasLocalDupes bool) (
		otherHasDupe bool, needRemoteDupe bool, baseTotalChecksum *verify.KVChecksum, err error)
	FinishTable(ctx context.Context) error
}

type dbTableMetaMgr struct {
	session      *sql.DB
	taskID       int64
	tr           *TableImporter
	schemaName   string
	tableName    string
	needChecksum bool
}

func (m *dbTableMetaMgr) InitTableMeta(ctx context.Context) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	// avoid override existing metadata if the meta is already inserted.
	stmt := common.SprintfWithIdentifiers(`INSERT IGNORE INTO %s.%s (task_id, table_id, table_name, status) VALUES (?, ?, ?, ?)`, m.schemaName, m.tableName)
	task := m.tr.logger.Begin(zap.DebugLevel, "init table meta")
	err := exec.Exec(ctx, "init table meta", stmt, m.taskID, m.tr.tableInfo.ID, m.tr.tableName, metaStatusInitial.String())
	task.End(zap.ErrorLevel, err)
	return errors.Trace(err)
}

type metaStatus uint32

const (
	metaStatusInitial metaStatus = iota
	metaStatusRowIDAllocated
	metaStatusRestoreStarted
	metaStatusRestoreFinished
	metaStatusChecksuming
	metaStatusChecksumSkipped
	metaStatusFinished
)

func (m metaStatus) String() string {
	switch m {
	case metaStatusInitial:
		return "initialized"
	case metaStatusRowIDAllocated:
		return "allocated"
	case metaStatusRestoreStarted:
		return "restore"
	case metaStatusRestoreFinished:
		return "restore_finished"
	case metaStatusChecksuming:
		return "checksuming"
	case metaStatusChecksumSkipped:
		return "checksum_skipped"
	case metaStatusFinished:
		return "finish"
	default:
		panic(fmt.Sprintf("unexpected metaStatus value '%d'", m))
	}
}

func parseMetaStatus(s string) (metaStatus, error) {
	switch s {
	case "", "initialized":
		return metaStatusInitial, nil
	case "allocated":
		return metaStatusRowIDAllocated, nil
	case "restore":
		return metaStatusRestoreStarted, nil
	case "restore_finished":
		return metaStatusRestoreFinished, nil
	case "checksuming":
		return metaStatusChecksuming, nil
	case "checksum_skipped":
		return metaStatusChecksumSkipped, nil
	case "finish":
		return metaStatusFinished, nil
	default:
		return metaStatusInitial, common.ErrInvalidMetaStatus.GenWithStackByArgs(s)
	}
}

func (m *dbTableMetaMgr) AllocTableRowIDs(ctx context.Context, rawRowIDMax int64) (*verify.KVChecksum, int64, error) {
	conn, err := m.session.Conn(ctx)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	//nolint: errcheck
	defer conn.Close()
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	var newRowIDBase, newRowIDMax int64
	curStatus := metaStatusInitial
	newStatus := metaStatusRowIDAllocated
	var baseTotalKvs, baseTotalBytes, baseChecksum uint64
	err = exec.Exec(ctx, "enable pessimistic transaction", "SET SESSION tidb_txn_mode = 'pessimistic';")
	if err != nil {
		return nil, 0, errors.Annotate(err, "enable pessimistic transaction failed")
	}

	needAutoID := common.TableHasAutoID(m.tr.tableInfo.Core)
	tableChecksumingMsg := "Target table is calculating checksum. Please wait until the checksum is finished and try again."
	doAllocTableRowIDsFn := func() error {
		return exec.Transact(ctx, "init table allocator base", func(ctx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(
				ctx,
				common.SprintfWithIdentifiers("SELECT task_id, row_id_base, row_id_max, total_kvs_base, total_bytes_base, checksum_base, status FROM %s.%s WHERE table_id = ? FOR UPDATE", m.schemaName, m.tableName),
				m.tr.tableInfo.ID,
			)
			if err != nil {
				return errors.Trace(err)
			}
			defer rows.Close()
			var (
				metaTaskID, rowIDBase, rowIDMax, maxRowIDMax int64
				totalKvs, totalBytes, checksum               uint64
				statusValue                                  string
			)
			for rows.Next() {
				if err = rows.Scan(&metaTaskID, &rowIDBase, &rowIDMax, &totalKvs, &totalBytes, &checksum, &statusValue); err != nil {
					return errors.Trace(err)
				}
				status, err := parseMetaStatus(statusValue)
				if err != nil {
					return err
				}

				// skip finished meta
				if status >= metaStatusFinished {
					continue
				}

				if status == metaStatusChecksuming {
					return common.ErrAllocTableRowIDs.GenWithStack(tableChecksumingMsg)
				}

				if metaTaskID == m.taskID {
					curStatus = status
					baseChecksum = checksum
					baseTotalKvs = totalKvs
					baseTotalBytes = totalBytes
					if status >= metaStatusRowIDAllocated {
						if rowIDMax-rowIDBase != rawRowIDMax {
							return common.ErrAllocTableRowIDs.GenWithStack("verify allocator base failed. local: '%d', meta: '%d'", rawRowIDMax, rowIDMax-rowIDBase)
						}
						newRowIDBase = rowIDBase
						newRowIDMax = rowIDMax
						break
					}
					continue
				}

				// other tasks has finished this logic, we needn't do again.
				if status >= metaStatusRowIDAllocated {
					newStatus = metaStatusRestoreStarted
				}

				if rowIDMax > maxRowIDMax {
					maxRowIDMax = rowIDMax
				}
			}
			if err := rows.Err(); err != nil {
				return errors.Trace(err)
			}

			// no enough info are available, fetch row_id max for table
			if curStatus == metaStatusInitial {
				if needAutoID {
					// maxRowIDMax is the max row_id that other tasks has allocated, we need to rebase the global autoid base first.
					// TODO this is not right when AUTO_ID_CACHE=1 and have auto row id,
					// the id allocators are separated in this case.
					if err := common.RebaseGlobalAutoID(ctx, maxRowIDMax, m.tr, m.tr.dbInfo.ID, m.tr.tableInfo.Core); err != nil {
						return errors.Trace(err)
					}
					newRowIDBase, newRowIDMax, err = common.AllocGlobalAutoID(ctx, rawRowIDMax, m.tr, m.tr.dbInfo.ID, m.tr.tableInfo.Core)
					if err != nil {
						return errors.Trace(err)
					}
				} else {
					// Though we don't need auto ID, we still guarantee that the row ID is unique across all lightning instances.
					newRowIDBase = maxRowIDMax
					newRowIDMax = newRowIDBase + rawRowIDMax
				}

				// table contains no data, can skip checksum
				if needAutoID && newRowIDBase == 0 && newStatus < metaStatusRestoreStarted {
					newStatus = metaStatusRestoreStarted
				}

				query := common.SprintfWithIdentifiers("UPDATE %s.%s SET row_id_base = ?, row_id_max = ?, status = ? WHERE table_id = ? AND task_id = ?", m.schemaName, m.tableName)
				_, err := tx.ExecContext(ctx, query, newRowIDBase, newRowIDMax, newStatus.String(), m.tr.tableInfo.ID, m.taskID)
				if err != nil {
					return errors.Trace(err)
				}

				curStatus = newStatus
			}
			return nil
		})
	}
	// TODO: the retry logic is duplicate with code in local.writeAndIngestByRanges, should encapsulate it later.
	// max retry backoff time: 2+4+8+16+30*26=810s
	backOffTime := time.Second
	for i := 0; i < maxRetryOnStatusConflict; i++ {
		err = doAllocTableRowIDsFn()
		if err == nil || !strings.Contains(err.Error(), tableChecksumingMsg) {
			break
		}
		// we only retry if it's tableChecksuming error, it happens during parallel import.
		// for detail see https://docs.pingcap.com/tidb/stable/tidb-lightning-distributed-import
		log.FromContext(ctx).Warn("target table is doing checksum, will try again",
			zap.Int("retry time", i+1), log.ShortError(err))
		backOffTime *= 2
		if backOffTime > maxBackoffTime {
			backOffTime = maxBackoffTime
		}
		select {
		case <-time.After(backOffTime):
		case <-ctx.Done():
			return nil, 0, errors.Trace(ctx.Err())
		}
	}
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	var checksum *verify.KVChecksum
	// need to do checksum and update checksum meta since we are the first one.
	if curStatus < metaStatusRestoreStarted {
		// table contains data but haven't do checksum yet
		if (newRowIDBase > 0 || !needAutoID) && m.needChecksum && baseTotalKvs == 0 {
			remoteCk, err := DoChecksum(ctx, m.tr.tableInfo)
			if err != nil {
				return nil, 0, errors.Trace(err)
			}

			if remoteCk.Checksum != baseChecksum || remoteCk.TotalKVs != baseTotalKvs || remoteCk.TotalBytes != baseTotalBytes {
				ck := verify.MakeKVChecksum(remoteCk.TotalBytes, remoteCk.TotalKVs, remoteCk.Checksum)
				checksum = &ck
			}
		}

		if checksum != nil {
			if err = m.UpdateTableBaseChecksum(ctx, checksum); err != nil {
				return nil, 0, errors.Trace(err)
			}

			m.tr.logger.Info("checksum before restore table", zap.Object("checksum", checksum))
		} else if err = m.UpdateTableStatus(ctx, metaStatusRestoreStarted); err != nil {
			return nil, 0, errors.Trace(err)
		}
	}
	if checksum == nil && baseTotalKvs > 0 {
		ck := verify.MakeKVChecksum(baseTotalBytes, baseTotalKvs, baseChecksum)
		checksum = &ck
	}
	log.FromContext(ctx).Info("allocate table row_id base", zap.String("table", m.tr.tableName),
		zap.Int64("row_id_base", newRowIDBase))
	if checksum != nil {
		log.FromContext(ctx).Info("checksum base", zap.Any("checksum", checksum))
	}
	return checksum, newRowIDBase, nil
}

func (m *dbTableMetaMgr) UpdateTableBaseChecksum(ctx context.Context, checksum *verify.KVChecksum) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	query := common.SprintfWithIdentifiers("UPDATE %s.%s SET total_kvs_base = ?, total_bytes_base = ?, checksum_base = ?, status = ? WHERE table_id = ? AND task_id = ?", m.schemaName, m.tableName)

	return exec.Exec(ctx, "update base checksum", query, checksum.SumKVS(),
		checksum.SumSize(), checksum.Sum(), metaStatusRestoreStarted.String(), m.tr.tableInfo.ID, m.taskID)
}

func (m *dbTableMetaMgr) UpdateTableStatus(ctx context.Context, status metaStatus) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	query := common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE table_id = ? AND task_id = ?", m.schemaName, m.tableName)
	return exec.Exec(ctx, "update meta status", query, status.String(), m.tr.tableInfo.ID, m.taskID)
}

func (m *dbTableMetaMgr) CheckAndUpdateLocalChecksum(ctx context.Context, checksum *verify.KVChecksum, hasLocalDupes bool) (
	otherHasDupe bool, needRemoteDupe bool, baseTotalChecksum *verify.KVChecksum, err error,
) {
	conn, err := m.session.Conn(ctx)
	if err != nil {
		return false, false, nil, errors.Trace(err)
	}
	//nolint: errcheck
	defer conn.Close()
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	err = exec.Exec(ctx, "enable pessimistic transaction", "SET SESSION tidb_txn_mode = 'pessimistic';")
	if err != nil {
		return false, false, nil, errors.Annotate(err, "enable pessimistic transaction failed")
	}
	var (
		baseTotalKvs, baseTotalBytes, baseChecksum uint64
		taskKvs, taskBytes, taskChecksum           uint64
		totalKvs, totalBytes, totalChecksum        uint64
		taskHasDuplicates                          bool
	)
	newStatus := metaStatusChecksuming
	otherHasDupe = false
	needRemoteDupe = true
	err = exec.Transact(ctx, "checksum pre-check", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(
			ctx,
			common.SprintfWithIdentifiers("SELECT task_id, total_kvs_base, total_bytes_base, checksum_base, total_kvs, total_bytes, checksum, status, has_duplicates from %s.%s WHERE table_id = ? FOR UPDATE", m.schemaName, m.tableName),
			m.tr.tableInfo.ID,
		)
		if err != nil {
			return errors.Annotate(err, "fetch task meta failed")
		}
		closed := false
		defer func() {
			if !closed {
				rows.Close()
			}
		}()
		var (
			taskID      int64
			statusValue string
		)
		for rows.Next() {
			if err = rows.Scan(&taskID, &baseTotalKvs, &baseTotalBytes, &baseChecksum, &taskKvs, &taskBytes, &taskChecksum, &statusValue, &taskHasDuplicates); err != nil {
				return errors.Trace(err)
			}
			status, err := parseMetaStatus(statusValue)
			if err != nil {
				return err
			}

			otherHasDupe = otherHasDupe || taskHasDuplicates

			// skip finished meta
			if status >= metaStatusFinished {
				continue
			}

			if taskID == m.taskID {
				if status >= metaStatusChecksuming {
					newStatus = status
					needRemoteDupe = status == metaStatusChecksuming
					return nil
				}

				continue
			}

			if status < metaStatusChecksuming {
				newStatus = metaStatusChecksumSkipped
				needRemoteDupe = false
				break
			} else if status == metaStatusChecksuming {
				return common.ErrTableIsChecksuming.GenWithStackByArgs(common.UniqueTable(m.schemaName, m.tableName))
			}

			totalBytes += baseTotalBytes
			totalKvs += baseTotalKvs
			totalChecksum ^= baseChecksum

			totalBytes += taskBytes
			totalKvs += taskKvs
			totalChecksum ^= taskChecksum
		}
		rows.Close()
		closed = true
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}

		query := common.SprintfWithIdentifiers("UPDATE %s.%s SET total_kvs = ?, total_bytes = ?, checksum = ?, status = ?, has_duplicates = ? WHERE table_id = ? AND task_id = ?", m.schemaName, m.tableName)
		_, err = tx.ExecContext(ctx, query, checksum.SumKVS(), checksum.SumSize(), checksum.Sum(), newStatus.String(), hasLocalDupes, m.tr.tableInfo.ID, m.taskID)
		return errors.Annotate(err, "update local checksum failed")
	})
	if err != nil {
		return false, false, nil, err
	}

	if !otherHasDupe && needRemoteDupe {
		ck := verify.MakeKVChecksum(totalBytes, totalKvs, totalChecksum)
		baseTotalChecksum = &ck
	}
	log.FromContext(ctx).Info("check table checksum", zap.String("table", m.tr.tableName),
		zap.Bool("otherHasDupe", otherHasDupe), zap.Bool("needRemoteDupe", needRemoteDupe),
		zap.String("new_status", newStatus.String()))
	return
}

func (m *dbTableMetaMgr) FinishTable(ctx context.Context) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	query := common.SprintfWithIdentifiers("DELETE FROM %s.%s where table_id = ? and (status = 'checksuming' or status = 'checksum_skipped')", m.schemaName, m.tableName)
	return exec.Exec(ctx, "clean up metas", query, m.tr.tableInfo.ID)
}

// RemoveTableMetaByTableName remove table meta by table name
func RemoveTableMetaByTableName(ctx context.Context, db *sql.DB, metaTable, tableName string) error {
	exec := &common.SQLWithRetry{
		DB:     db,
		Logger: log.FromContext(ctx),
	}
	query := fmt.Sprintf("DELETE FROM %s", metaTable)
	var args []any
	if tableName != "" {
		query += " where table_name = ?"
		args = []any{tableName}
	}

	return exec.Exec(ctx, "clean up metas", query, args...)
}

type taskMetaMgr interface {
	InitTask(ctx context.Context, tikvSourceSize, tiflashSourceSize int64) error
	CheckTaskExist(ctx context.Context) (bool, error)
	// CheckTasksExclusively check all tasks exclusively. action is the function to check all tasks and returns the tasks
	// need to update or any new tasks. There is at most one lightning who can execute the action function at the same time.
	// Note that action may be executed multiple times due to transaction retry, caller should make sure it's idempotent.
	CheckTasksExclusively(ctx context.Context, action func(tasks []taskMeta) ([]taskMeta, error)) error
	// CanPauseSchedulerByKeyRange returns whether the scheduler can pause by the key range.
	CanPauseSchedulerByKeyRange() bool
	CheckAndPausePdSchedulers(ctx context.Context) (pdutil.UndoFunc, error)
	// CheckAndFinishRestore check task meta and return whether to switch cluster to normal state and clean up the metadata
	// Return values: first boolean indicates whether switch back tidb cluster to normal state (restore schedulers, switch tikv to normal)
	// the second boolean indicates whether to clean up the metadata in tidb
	CheckAndFinishRestore(ctx context.Context, finished bool) (shouldSwitchBack bool, shouldCleanupMeta bool, err error)
	Cleanup(ctx context.Context) error
	CleanupTask(ctx context.Context) error
	CleanupAllMetas(ctx context.Context) error
	Close()
}

type dbTaskMetaMgr struct {
	session    *sql.DB
	taskID     int64
	pd         *pdutil.PdController
	tableName  string
	schemaName string
}

type taskMetaStatus uint32

const (
	taskMetaStatusInitial taskMetaStatus = iota
	taskMetaStatusScheduleSet
	taskMetaStatusSwitchSkipped
	taskMetaStatusSwitchBack
)

const (
	taskStateNormal int = iota
	taskStateExited
)

func (m taskMetaStatus) String() string {
	switch m {
	case taskMetaStatusInitial:
		return "initialized"
	case taskMetaStatusScheduleSet:
		return "schedule_set"
	case taskMetaStatusSwitchSkipped:
		return "skip_switch"
	case taskMetaStatusSwitchBack:
		return "switched"
	default:
		panic(fmt.Sprintf("unexpected metaStatus value '%d'", m))
	}
}

func parseTaskMetaStatus(s string) (taskMetaStatus, error) {
	switch s {
	case "", "initialized":
		return taskMetaStatusInitial, nil
	case "schedule_set":
		return taskMetaStatusScheduleSet, nil
	case "skip_switch":
		return taskMetaStatusSwitchSkipped, nil
	case "switched":
		return taskMetaStatusSwitchBack, nil
	default:
		return taskMetaStatusInitial, common.ErrInvalidMetaStatus.GenWithStackByArgs(s)
	}
}

type taskMeta struct {
	taskID             int64
	pdCfgs             string
	status             taskMetaStatus
	state              int
	tikvSourceBytes    uint64
	tiflashSourceBytes uint64
	tikvAvail          uint64
	tiflashAvail       uint64
}

type storedCfgs struct {
	PauseCfg   pdutil.ClusterConfig `json:"paused"`
	RestoreCfg pdutil.ClusterConfig `json:"restore"`
}

func (m *dbTaskMetaMgr) InitTask(ctx context.Context, tikvSourceSize, tiflashSourceSize int64) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.FromContext(ctx),
	}
	// avoid override existing metadata if the meta is already inserted.
	stmt := common.SprintfWithIdentifiers(`
		INSERT INTO %s.%s (task_id, status, tikv_source_bytes, tiflash_source_bytes)
			VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE state = ?`,
		m.schemaName, m.tableName)
	err := exec.Exec(ctx, "init task meta", stmt, m.taskID, taskMetaStatusInitial.String(), tikvSourceSize, tiflashSourceSize, taskStateNormal)
	return errors.Trace(err)
}

func (m *dbTaskMetaMgr) CheckTaskExist(ctx context.Context) (bool, error) {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.FromContext(ctx),
	}
	// avoid override existing metadata if the meta is already inserted.
	exist := false
	err := exec.Transact(ctx, "check whether this task has started before", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx,
			common.SprintfWithIdentifiers("SELECT task_id from %s.%s WHERE task_id = ?", m.schemaName, m.tableName),
			m.taskID,
		)
		if err != nil {
			return errors.Annotate(err, "fetch task meta failed")
		}
		var taskID int64
		for rows.Next() {
			if err = rows.Scan(&taskID); err != nil {
				rows.Close()
				return errors.Trace(err)
			}
			if taskID == m.taskID {
				exist = true
			}
		}
		if err := rows.Close(); err != nil {
			return errors.Trace(err)
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}

		return nil
	})
	return exist, errors.Trace(err)
}

func (m *dbTaskMetaMgr) CheckTasksExclusively(ctx context.Context, action func(tasks []taskMeta) ([]taskMeta, error)) error {
	conn, err := m.session.Conn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	//nolint: errcheck
	defer conn.Close()
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.FromContext(ctx),
	}
	err = exec.Exec(ctx, "enable pessimistic transaction", "SET SESSION tidb_txn_mode = 'pessimistic';")
	if err != nil {
		return errors.Annotate(err, "enable pessimistic transaction failed")
	}
	return exec.Transact(ctx, "check tasks exclusively", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(
			ctx,
			common.SprintfWithIdentifiers(`
				SELECT
					task_id,
					pd_cfgs,
					status,
					state,
					tikv_source_bytes,
					tiflash_source_bytes,
					tikv_avail,
					tiflash_avail
				FROM %s.%s FOR UPDATE`, m.schemaName, m.tableName),
		)
		if err != nil {
			return errors.Annotate(err, "fetch task metas failed")
		}
		defer rows.Close()

		var tasks []taskMeta
		for rows.Next() {
			var task taskMeta
			var statusValue string
			if err = rows.Scan(&task.taskID, &task.pdCfgs, &statusValue, &task.state, &task.tikvSourceBytes, &task.tiflashSourceBytes, &task.tikvAvail, &task.tiflashAvail); err != nil {
				return errors.Trace(err)
			}
			status, err := parseTaskMetaStatus(statusValue)
			if err != nil {
				return err
			}
			task.status = status
			tasks = append(tasks, task)
		}
		if err = rows.Err(); err != nil {
			return errors.Trace(err)
		}
		newTasks, err := action(tasks)
		if err != nil {
			return errors.Trace(err)
		}
		for _, task := range newTasks {
			query := common.SprintfWithIdentifiers(`
				REPLACE INTO %s.%s (task_id, pd_cfgs, status, state, tikv_source_bytes, tiflash_source_bytes, tikv_avail, tiflash_avail)
				VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
				m.schemaName, m.tableName)
			if _, err = tx.ExecContext(ctx, query, task.taskID, task.pdCfgs, task.status.String(), task.state, task.tikvSourceBytes, task.tiflashSourceBytes, task.tikvAvail, task.tiflashAvail); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
}

func (m *dbTaskMetaMgr) CheckAndPausePdSchedulers(ctx context.Context) (pdutil.UndoFunc, error) {
	pauseCtx, cancel := context.WithCancel(ctx)
	conn, err := m.session.Conn(ctx)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	//nolint: errcheck
	defer conn.Close()
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.FromContext(ctx),
	}
	err = exec.Exec(ctx, "enable pessimistic transaction", "SET SESSION tidb_txn_mode = 'pessimistic';")
	if err != nil {
		cancel()
		return nil, errors.Annotate(err, "enable pessimistic transaction failed")
	}

	needSwitch := true
	paused := false
	var pausedCfg storedCfgs
	err = exec.Transact(ctx, "check and pause schedulers", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(
			ctx,
			common.SprintfWithIdentifiers(`
				SELECT task_id, pd_cfgs, status, state
				FROM %s.%s FOR UPDATE`,
				m.schemaName, m.tableName),
		)
		if err != nil {
			return errors.Annotate(err, "fetch task meta failed")
		}
		closed := false
		defer func() {
			if !closed {
				rows.Close()
			}
		}()
		var (
			taskID      int64
			cfg         string
			statusValue string
			state       int
		)
		var cfgStr string
		for rows.Next() {
			if err = rows.Scan(&taskID, &cfg, &statusValue, &state); err != nil {
				return errors.Trace(err)
			}
			status, err := parseTaskMetaStatus(statusValue)
			if err != nil {
				return err
			}

			if status == taskMetaStatusInitial {
				continue
			}

			if taskID == m.taskID {
				if status >= taskMetaStatusSwitchSkipped {
					needSwitch = false
					return nil
				}
			}

			if cfg != "" {
				cfgStr = cfg
				break
			}
		}
		if err = rows.Close(); err != nil {
			return errors.Trace(err)
		}
		closed = true

		if err = rows.Err(); err != nil {
			return errors.Trace(err)
		}

		if cfgStr != "" {
			err = json.Unmarshal([]byte(cfgStr), &pausedCfg)
			return errors.Trace(err)
		}

		orig, removed, err := m.pd.RemoveSchedulersWithOrigin(pauseCtx)
		if err != nil {
			return errors.Trace(err)
		}
		paused = true

		pausedCfg = storedCfgs{PauseCfg: removed, RestoreCfg: orig}
		jsonByts, err := json.Marshal(&pausedCfg)
		if err != nil {
			// try to rollback the stopped schedulers
			cancelFunc := m.pd.MakeUndoFunctionByConfig(pausedCfg.RestoreCfg)
			if err1 := cancelFunc(ctx); err1 != nil {
				log.FromContext(ctx).Warn("undo remove schedulers failed", zap.Error(err1))
			}
			return errors.Trace(err)
		}

		query := common.SprintfWithIdentifiers(`
			UPDATE %s.%s SET pd_cfgs = ?, status = ? WHERE task_id = ?`,
			m.schemaName, m.tableName)
		_, err = tx.ExecContext(ctx, query, string(jsonByts), taskMetaStatusScheduleSet.String(), m.taskID)

		return errors.Annotate(err, "update task pd configs failed")
	})
	if err != nil {
		cancel()
		return nil, err
	}

	if !needSwitch {
		cancel()
		return nil, nil
	}

	if !paused {
		if err = m.pd.RemoveSchedulersWithCfg(pauseCtx, pausedCfg.PauseCfg); err != nil {
			cancel()
			return nil, err
		}
	}

	cancelFunc := m.pd.MakeUndoFunctionByConfig(pausedCfg.RestoreCfg)

	return func(ctx context.Context) error {
		// close the periodic task ctx
		cancel()
		return cancelFunc(ctx)
	}, nil
}

func (m *dbTaskMetaMgr) CanPauseSchedulerByKeyRange() bool {
	return m.pd.CanPauseSchedulerByKeyRange()
}

// CheckAndFinishRestore check task meta and return whether to switch cluster to normal state and clean up the metadata
// Return values: first boolean indicates whether switch back tidb cluster to normal state (restore schedulers, switch tikv to normal)
// the second boolean indicates whether to clean up the metadata in tidb
func (m *dbTaskMetaMgr) CheckAndFinishRestore(ctx context.Context, finished bool) (switchBack bool, allFinished bool, err error) {
	conn, err := m.session.Conn(ctx)
	if err != nil {
		return false, false, errors.Trace(err)
	}
	//nolint: errcheck
	defer conn.Close()
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.FromContext(ctx),
	}
	err = exec.Exec(ctx, "enable pessimistic transaction", "SET SESSION tidb_txn_mode = 'pessimistic';")
	if err != nil {
		return false, false, errors.Annotate(err, "enable pessimistic transaction failed")
	}

	switchBack = true
	allFinished = finished
	err = exec.Transact(ctx, "check and finish schedulers", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(
			ctx,
			common.SprintfWithIdentifiers("SELECT task_id, status, state FROM %s.%s FOR UPDATE", m.schemaName, m.tableName),
		)
		if err != nil {
			return errors.Annotate(err, "fetch task meta failed")
		}
		closed := false
		defer func() {
			if !closed {
				rows.Close()
			}
		}()
		var (
			taskID      int64
			statusValue string
			state       int
		)

		taskStatus := taskMetaStatusInitial
		for rows.Next() {
			if err = rows.Scan(&taskID, &statusValue, &state); err != nil {
				return errors.Trace(err)
			}
			status, err := parseTaskMetaStatus(statusValue)
			if err != nil {
				return err
			}

			if taskID == m.taskID {
				taskStatus = status
				continue
			}

			if status < taskMetaStatusSwitchSkipped {
				allFinished = false
				// check if other task still running
				if state == taskStateNormal {
					log.FromContext(ctx).Info("unfinished task found", zap.Int64("task_id", taskID),
						zap.Stringer("status", status))
					switchBack = false
				}
			}
		}
		if err = rows.Close(); err != nil {
			return errors.Trace(err)
		}
		closed = true

		if err = rows.Err(); err != nil {
			return errors.Trace(err)
		}

		if taskStatus < taskMetaStatusSwitchSkipped {
			newStatus := taskMetaStatusSwitchBack
			newState := taskStateNormal
			if !finished {
				newStatus = taskStatus
				newState = taskStateExited
			} else if !allFinished {
				newStatus = taskMetaStatusSwitchSkipped
			}

			query := common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ?, state = ? WHERE task_id = ?", m.schemaName, m.tableName)
			if _, err = tx.ExecContext(ctx, query, newStatus.String(), newState, m.taskID); err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	})
	log.FromContext(ctx).Info("check all task finish status", zap.Bool("task_finished", finished),
		zap.Bool("all_finished", allFinished), zap.Bool("switch_back", switchBack))

	return switchBack, allFinished, err
}

func (m *dbTaskMetaMgr) Cleanup(ctx context.Context) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.FromContext(ctx),
	}
	// avoid override existing metadata if the meta is already inserted.
	stmt := common.SprintfWithIdentifiers("DROP TABLE %s.%s;", m.schemaName, m.tableName)
	if err := exec.Exec(ctx, "cleanup task meta tables", stmt); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *dbTaskMetaMgr) CleanupTask(ctx context.Context) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.FromContext(ctx),
	}
	stmt := common.SprintfWithIdentifiers("DELETE FROM %s.%s WHERE task_id = ?;", m.schemaName, m.tableName)
	err := exec.Exec(ctx, "clean up task", stmt, m.taskID)
	return errors.Trace(err)
}

func (m *dbTaskMetaMgr) Close() {
	m.pd.Close()
}

func (m *dbTaskMetaMgr) CleanupAllMetas(ctx context.Context) error {
	return MaybeCleanupAllMetas(ctx, log.FromContext(ctx), m.session, m.schemaName, true)
}

// MaybeCleanupAllMetas remove the meta schema if there is no unfinished tables
func MaybeCleanupAllMetas(
	ctx context.Context,
	logger log.Logger,
	db *sql.DB,
	schemaName string,
	tableMetaExist bool,
) error {
	exec := &common.SQLWithRetry{
		DB:     db,
		Logger: logger,
	}

	// check if all tables are finished
	if tableMetaExist {
		query := common.SprintfWithIdentifiers("SELECT COUNT(*) from %s.%s", schemaName, TableMetaTableName)
		var cnt int
		if err := exec.QueryRow(ctx, "fetch table meta row count", query, &cnt); err != nil {
			return errors.Trace(err)
		}
		if cnt > 0 {
			logger.Warn("there are unfinished table in table meta table, cleanup skipped.")
			return nil
		}
	}

	// avoid override existing metadata if the meta is already inserted.
	stmt := common.SprintfWithIdentifiers("DROP DATABASE %s;", schemaName)
	if err := exec.Exec(ctx, "cleanup task meta tables", stmt); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type noopMetaMgrBuilder struct{}

func (noopMetaMgrBuilder) Init(_ context.Context) error {
	return nil
}

func (noopMetaMgrBuilder) TaskMetaMgr(_ *pdutil.PdController) taskMetaMgr {
	return noopTaskMetaMgr{}
}

func (noopMetaMgrBuilder) TableMetaMgr(_ *TableImporter) tableMetaMgr {
	return noopTableMetaMgr{}
}

type noopTaskMetaMgr struct{}

func (noopTaskMetaMgr) InitTask(_ context.Context, _, _ int64) error {
	return nil
}

func (noopTaskMetaMgr) CheckTasksExclusively(_ context.Context, _ func(tasks []taskMeta) ([]taskMeta, error)) error {
	return nil
}

func (noopTaskMetaMgr) CheckAndPausePdSchedulers(_ context.Context) (pdutil.UndoFunc, error) {
	return func(context.Context) error {
		return nil
	}, nil
}

func (noopTaskMetaMgr) CanPauseSchedulerByKeyRange() bool {
	return false
}

func (noopTaskMetaMgr) CheckTaskExist(context.Context) (bool, error) {
	return true, nil
}

func (noopTaskMetaMgr) CheckAndFinishRestore(context.Context, bool) (
	needSwitchBack bool, needCleanup bool, err error) {
	return false, true, nil
}

func (noopTaskMetaMgr) Cleanup(_ context.Context) error {
	return nil
}

func (noopTaskMetaMgr) CleanupTask(_ context.Context) error {
	return nil
}

func (noopTaskMetaMgr) CleanupAllMetas(_ context.Context) error {
	return nil
}

func (noopTaskMetaMgr) Close() {
}

type noopTableMetaMgr struct{}

func (noopTableMetaMgr) InitTableMeta(_ context.Context) error {
	return nil
}

func (noopTableMetaMgr) AllocTableRowIDs(_ context.Context, _ int64) (*verify.KVChecksum, int64, error) {
	return nil, 0, nil
}

func (noopTableMetaMgr) UpdateTableStatus(_ context.Context, _ metaStatus) error {
	return nil
}

func (noopTableMetaMgr) UpdateTableBaseChecksum(_ context.Context, _ *verify.KVChecksum) error {
	return nil
}

func (noopTableMetaMgr) CheckAndUpdateLocalChecksum(_ context.Context, _ *verify.KVChecksum, _ bool) (
	otherHasDupe bool, needRemoteDupe bool, baseTotalChecksum *verify.KVChecksum, err error) {
	return false, true, &verify.KVChecksum{}, nil
}

func (noopTableMetaMgr) FinishTable(_ context.Context) error {
	return nil
}

type singleMgrBuilder struct {
	taskID int64
}

func (singleMgrBuilder) Init(context.Context) error {
	return nil
}

func (b singleMgrBuilder) TaskMetaMgr(pd *pdutil.PdController) taskMetaMgr {
	return &singleTaskMetaMgr{
		pd:     pd,
		taskID: b.taskID,
	}
}

func (singleMgrBuilder) TableMetaMgr(_ *TableImporter) tableMetaMgr {
	return noopTableMetaMgr{}
}

type singleTaskMetaMgr struct {
	pd                 *pdutil.PdController
	taskID             int64
	initialized        bool
	tikvSourceBytes    uint64
	tiflashSourceBytes uint64
	tikvAvail          uint64
	tiflashAvail       uint64
}

func (m *singleTaskMetaMgr) InitTask(_ context.Context, tikvSourceSize, tiflashSourceSize int64) error {
	m.tikvSourceBytes = uint64(tikvSourceSize)
	m.tiflashSourceBytes = uint64(tiflashSourceSize)
	m.initialized = true
	return nil
}

func (m *singleTaskMetaMgr) CheckTasksExclusively(_ context.Context, action func(tasks []taskMeta) ([]taskMeta, error)) error {
	newTasks, err := action([]taskMeta{
		{
			taskID:             m.taskID,
			status:             taskMetaStatusInitial,
			tikvSourceBytes:    m.tikvSourceBytes,
			tiflashSourceBytes: m.tiflashSourceBytes,
			tikvAvail:          m.tikvAvail,
			tiflashAvail:       m.tiflashAvail,
		},
	})
	for _, t := range newTasks {
		if m.taskID == t.taskID {
			m.tikvSourceBytes = t.tikvSourceBytes
			m.tiflashSourceBytes = t.tiflashSourceBytes
			m.tikvAvail = t.tikvAvail
			m.tiflashAvail = t.tiflashAvail
		}
	}
	return err
}

func (m *singleTaskMetaMgr) CheckAndPausePdSchedulers(ctx context.Context) (pdutil.UndoFunc, error) {
	return m.pd.RemoveSchedulers(ctx)
}

func (m *singleTaskMetaMgr) CanPauseSchedulerByKeyRange() bool {
	return m.pd.CanPauseSchedulerByKeyRange()
}

func (m *singleTaskMetaMgr) CheckTaskExist(_ context.Context) (bool, error) {
	return m.initialized, nil
}

func (*singleTaskMetaMgr) CheckAndFinishRestore(context.Context, bool) (shouldSwitchBack bool, shouldCleanupMeta bool, err error) {
	return true, true, nil
}

func (*singleTaskMetaMgr) Cleanup(_ context.Context) error {
	return nil
}

func (*singleTaskMetaMgr) CleanupTask(_ context.Context) error {
	return nil
}

func (*singleTaskMetaMgr) CleanupAllMetas(_ context.Context) error {
	return nil
}

func (*singleTaskMetaMgr) Close() {
}
