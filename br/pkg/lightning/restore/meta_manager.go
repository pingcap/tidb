// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"
)

type metaMgrBuilder interface {
	Init(ctx context.Context) error
	TaskMetaMgr(pd *pdutil.PdController) taskMetaMgr
	TableMetaMgr(tr *TableRestore) tableMetaMgr
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
		Logger:       log.L(),
		HideQueryLog: redact.NeedRedact(),
	}
	metaDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", common.EscapeIdentifier(b.schema))
	if err := exec.Exec(ctx, "create meta schema", metaDBSQL); err != nil {
		return errors.Annotate(err, "create meta schema failed")
	}
	taskMetaSQL := fmt.Sprintf(CreateTaskMetaTable, common.UniqueTable(b.schema, TaskMetaTableName))
	if err := exec.Exec(ctx, "create meta table", taskMetaSQL); err != nil {
		return errors.Annotate(err, "create task meta table failed")
	}
	tableMetaSQL := fmt.Sprintf(CreateTableMetadataTable, common.UniqueTable(b.schema, TableMetaTableName))
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
		tableName:  common.UniqueTable(b.schema, TaskMetaTableName),
		schemaName: b.schema,
	}
}

func (b *dbMetaMgrBuilder) TableMetaMgr(tr *TableRestore) tableMetaMgr {
	return &dbTableMetaMgr{
		session:      b.db,
		taskID:       b.taskID,
		tr:           tr,
		tableName:    common.UniqueTable(b.schema, TableMetaTableName),
		needChecksum: b.needChecksum,
	}
}

type tableMetaMgr interface {
	InitTableMeta(ctx context.Context) error
	AllocTableRowIDs(ctx context.Context, rawRowIDMax int64) (*verify.KVChecksum, int64, error)
	UpdateTableStatus(ctx context.Context, status metaStatus) error
	UpdateTableBaseChecksum(ctx context.Context, checksum *verify.KVChecksum) error
	CheckAndUpdateLocalChecksum(ctx context.Context, checksum *verify.KVChecksum, hasLocalDupes bool) (
		needChecksum bool, needRemoteDupe bool, baseTotalChecksum *verify.KVChecksum, err error)
	FinishTable(ctx context.Context) error
}

type dbTableMetaMgr struct {
	session      *sql.DB
	taskID       int64
	tr           *TableRestore
	tableName    string
	needChecksum bool
}

func (m *dbTableMetaMgr) InitTableMeta(ctx context.Context) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	// avoid override existing metadata if the meta is already inserted.
	stmt := fmt.Sprintf(`INSERT IGNORE INTO %s (task_id, table_id, table_name, status) values (?, ?, ?, ?)`, m.tableName)
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
	needAutoID := common.TableHasAutoRowID(m.tr.tableInfo.Core) || m.tr.tableInfo.Core.GetAutoIncrementColInfo() != nil || m.tr.tableInfo.Core.ContainsAutoRandomBits()
	err = exec.Transact(ctx, "init table allocator base", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(
			ctx,
			fmt.Sprintf("SELECT task_id, row_id_base, row_id_max, total_kvs_base, total_bytes_base, checksum_base, status from %s WHERE table_id = ? FOR UPDATE", m.tableName),
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
				return common.ErrAllocTableRowIDs.GenWithStack("Target table is calculating checksum. Please wait until the checksum is finished and try again.")
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
		if rows.Err() != nil {
			return errors.Trace(rows.Err())
		}

		// no enough info are available, fetch row_id max for table
		if curStatus == metaStatusInitial {
			if needAutoID && maxRowIDMax == 0 {
				// NOTE: currently, if a table contains auto_incremental unique key and _tidb_rowid,
				// the `show table next_row_id` will returns the unique key field only.
				var autoIDField string
				for _, col := range m.tr.tableInfo.Core.Columns {
					if mysql.HasAutoIncrementFlag(col.GetFlag()) {
						autoIDField = col.Name.L
						break
					} else if mysql.HasPriKeyFlag(col.GetFlag()) && m.tr.tableInfo.Core.AutoRandomBits > 0 {
						autoIDField = col.Name.L
						break
					}
				}
				if len(autoIDField) == 0 && common.TableHasAutoRowID(m.tr.tableInfo.Core) {
					autoIDField = model.ExtraHandleName.L
				}
				if len(autoIDField) == 0 {
					return common.ErrAllocTableRowIDs.GenWithStack("table %s contains auto increment id or _tidb_rowid, but target field not found", m.tr.tableName)
				}

				autoIDInfos, err := tidb.FetchTableAutoIDInfos(ctx, tx, m.tr.tableName)
				if err != nil {
					return errors.Trace(err)
				}
				found := false
				for _, info := range autoIDInfos {
					if strings.ToLower(info.Column) == autoIDField {
						maxRowIDMax = info.NextID - 1
						found = true
						break
					}
				}
				if !found {
					return common.ErrAllocTableRowIDs.GenWithStack("can't fetch previous auto id base for table %s field '%s'", m.tr.tableName, autoIDField)
				}
			}
			newRowIDBase = maxRowIDMax
			newRowIDMax = newRowIDBase + rawRowIDMax
			// table contains no data, can skip checksum
			if needAutoID && newRowIDBase == 0 && newStatus < metaStatusRestoreStarted {
				newStatus = metaStatusRestoreStarted
			}

			// nolint:gosec
			query := fmt.Sprintf("update %s set row_id_base = ?, row_id_max = ?, status = ? where table_id = ? and task_id = ?", m.tableName)
			_, err := tx.ExecContext(ctx, query, newRowIDBase, newRowIDMax, newStatus.String(), m.tr.tableInfo.ID, m.taskID)
			if err != nil {
				return errors.Trace(err)
			}

			curStatus = newStatus
		}
		return nil
	})
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
	log.L().Info("allocate table row_id base", zap.String("table", m.tr.tableName),
		zap.Int64("row_id_base", newRowIDBase))
	if checksum != nil {
		log.L().Info("checksum base", zap.Any("checksum", checksum))
	}
	return checksum, newRowIDBase, nil
}

func (m *dbTableMetaMgr) UpdateTableBaseChecksum(ctx context.Context, checksum *verify.KVChecksum) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	query := fmt.Sprintf("update %s set total_kvs_base = ?, total_bytes_base = ?, checksum_base = ?, status = ? where table_id = ? and task_id = ?", m.tableName)

	return exec.Exec(ctx, "update base checksum", query, checksum.SumKVS(),
		checksum.SumSize(), checksum.Sum(), metaStatusRestoreStarted.String(), m.tr.tableInfo.ID, m.taskID)
}

func (m *dbTableMetaMgr) UpdateTableStatus(ctx context.Context, status metaStatus) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	query := fmt.Sprintf("update %s set status = ? where table_id = ? and task_id = ?", m.tableName)
	return exec.Exec(ctx, "update meta status", query, status.String(), m.tr.tableInfo.ID, m.taskID)
}

func (m *dbTableMetaMgr) CheckAndUpdateLocalChecksum(ctx context.Context, checksum *verify.KVChecksum, hasLocalDupes bool) (
	needChecksum bool, needRemoteDupe bool, baseTotalChecksum *verify.KVChecksum, err error,
) {
	conn, err := m.session.Conn(ctx)
	if err != nil {
		return false, false, nil, errors.Trace(err)
	}
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
	needChecksum = true
	needRemoteDupe = true
	err = exec.Transact(ctx, "checksum pre-check", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(
			ctx,
			fmt.Sprintf("SELECT task_id, total_kvs_base, total_bytes_base, checksum_base, total_kvs, total_bytes, checksum, status, has_duplicates from %s WHERE table_id = ? FOR UPDATE", m.tableName),
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

			if taskHasDuplicates {
				needChecksum = false
			}

			// skip finished meta
			if status >= metaStatusFinished {
				continue
			}

			if taskID == m.taskID {
				if status >= metaStatusChecksuming {
					newStatus = status
					needRemoteDupe = status == metaStatusChecksuming
					needChecksum = needChecksum && needRemoteDupe
					return nil
				}

				continue
			}

			if status < metaStatusChecksuming {
				newStatus = metaStatusChecksumSkipped
				needChecksum = false
				needRemoteDupe = false
				break
			} else if status == metaStatusChecksuming {
				return common.ErrTableIsChecksuming.GenWithStackByArgs(m.tableName)
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
		if rows.Err() != nil {
			return errors.Trace(rows.Err())
		}

		// nolint:gosec
		query := fmt.Sprintf("update %s set total_kvs = ?, total_bytes = ?, checksum = ?, status = ?, has_duplicates = ? where table_id = ? and task_id = ?", m.tableName)
		_, err = tx.ExecContext(ctx, query, checksum.SumKVS(), checksum.SumSize(), checksum.Sum(), newStatus.String(), hasLocalDupes, m.tr.tableInfo.ID, m.taskID)
		return errors.Annotate(err, "update local checksum failed")
	})
	if err != nil {
		return false, false, nil, err
	}

	if needChecksum {
		ck := verify.MakeKVChecksum(totalBytes, totalKvs, totalChecksum)
		baseTotalChecksum = &ck
	}
	log.L().Info("check table checksum", zap.String("table", m.tr.tableName),
		zap.Bool("checksum", needChecksum), zap.String("new_status", newStatus.String()))
	return
}

func (m *dbTableMetaMgr) FinishTable(ctx context.Context) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: m.tr.logger,
	}
	query := fmt.Sprintf("DELETE FROM %s where table_id = ? and (status = 'checksuming' or status = 'checksum_skipped')", m.tableName)
	return exec.Exec(ctx, "clean up metas", query, m.tr.tableInfo.ID)
}

func RemoveTableMetaByTableName(ctx context.Context, db *sql.DB, metaTable, tableName string) error {
	exec := &common.SQLWithRetry{
		DB:     db,
		Logger: log.L(),
	}
	query := fmt.Sprintf("DELETE FROM %s", metaTable)
	var args []interface{}
	if tableName != "" {
		query += " where table_name = ?"
		args = []interface{}{tableName}
	}

	return exec.Exec(ctx, "clean up metas", query, args...)
}

type taskMetaMgr interface {
	InitTask(ctx context.Context, source int64) error
	CheckTaskExist(ctx context.Context) (bool, error)
	// CheckTasksExclusively check all tasks exclusively. action is the function to check all tasks and returns the tasks
	// need to update or any new tasks. There is at most one lightning who can execute the action function at the same time.
	// Note that action may be executed multiple times due to transaction retry, caller should make sure it's idempotent.
	CheckTasksExclusively(ctx context.Context, action func(tasks []taskMeta) ([]taskMeta, error)) error
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
	session *sql.DB
	taskID  int64
	pd      *pdutil.PdController
	// unique name of task meta table
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
	taskID       int64
	pdCfgs       string
	status       taskMetaStatus
	state        int
	sourceBytes  uint64
	clusterAvail uint64
}

type storedCfgs struct {
	PauseCfg   pdutil.ClusterConfig `json:"paused"`
	RestoreCfg pdutil.ClusterConfig `json:"restore"`
}

func (m *dbTaskMetaMgr) InitTask(ctx context.Context, source int64) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.L(),
	}
	// avoid override existing metadata if the meta is already inserted.
	stmt := fmt.Sprintf(`INSERT INTO %s (task_id, status, source_bytes) values (?, ?, ?) ON DUPLICATE KEY UPDATE state = ?`, m.tableName)
	err := exec.Exec(ctx, "init task meta", stmt, m.taskID, taskMetaStatusInitial.String(), source, taskStateNormal)
	return errors.Trace(err)
}

func (m *dbTaskMetaMgr) CheckTaskExist(ctx context.Context) (bool, error) {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.L(),
	}
	// avoid override existing metadata if the meta is already inserted.
	exist := false
	err := exec.Transact(ctx, "check whether this task has started before", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx,
			fmt.Sprintf("SELECT task_id from %s WHERE task_id = ?", m.tableName),
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
	defer conn.Close()
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.L(),
	}
	err = exec.Exec(ctx, "enable pessimistic transaction", "SET SESSION tidb_txn_mode = 'pessimistic';")
	if err != nil {
		return errors.Annotate(err, "enable pessimistic transaction failed")
	}
	return exec.Transact(ctx, "check tasks exclusively", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(
			ctx,
			fmt.Sprintf("SELECT task_id, pd_cfgs, status, state, source_bytes, cluster_avail from %s FOR UPDATE", m.tableName),
		)
		if err != nil {
			return errors.Annotate(err, "fetch task metas failed")
		}
		defer rows.Close()

		var tasks []taskMeta
		for rows.Next() {
			var task taskMeta
			var statusValue string
			if err = rows.Scan(&task.taskID, &task.pdCfgs, &statusValue, &task.state, &task.sourceBytes, &task.clusterAvail); err != nil {
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
			// nolint:gosec
			query := fmt.Sprintf("REPLACE INTO %s (task_id, pd_cfgs, status, state, source_bytes, cluster_avail) VALUES(?, ?, ?, ?, ?, ?)", m.tableName)
			if _, err = tx.ExecContext(ctx, query, task.taskID, task.pdCfgs, task.status.String(), task.state, task.sourceBytes, task.clusterAvail); err != nil {
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
	defer conn.Close()
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.L(),
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
			fmt.Sprintf("SELECT task_id, pd_cfgs, status, state from %s FOR UPDATE", m.tableName),
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
				log.L().Warn("undo remove schedulers failed", zap.Error(err1))
			}
			return errors.Trace(err)
		}

		// nolint:gosec
		query := fmt.Sprintf("update %s set pd_cfgs = ?, status = ? where task_id = ?", m.tableName)
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

// CheckAndFinishRestore check task meta and return whether to switch cluster to normal state and clean up the metadata
// Return values: first boolean indicates whether switch back tidb cluster to normal state (restore schedulers, switch tikv to normal)
// the second boolean indicates whether to clean up the metadata in tidb
func (m *dbTaskMetaMgr) CheckAndFinishRestore(ctx context.Context, finished bool) (bool, bool, error) {
	conn, err := m.session.Conn(ctx)
	if err != nil {
		return false, false, errors.Trace(err)
	}
	defer conn.Close()
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.L(),
	}
	err = exec.Exec(ctx, "enable pessimistic transaction", "SET SESSION tidb_txn_mode = 'pessimistic';")
	if err != nil {
		return false, false, errors.Annotate(err, "enable pessimistic transaction failed")
	}

	switchBack := true
	allFinished := finished
	err = exec.Transact(ctx, "check and finish schedulers", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT task_id, status, state from %s FOR UPDATE", m.tableName))
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
					log.L().Info("unfinished task found", zap.Int64("task_id", taskID),
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

			// nolint:gosec
			query := fmt.Sprintf("update %s set status = ?, state = ? where task_id = ?", m.tableName)
			if _, err = tx.ExecContext(ctx, query, newStatus.String(), newState, m.taskID); err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	})
	log.L().Info("check all task finish status", zap.Bool("task_finished", finished),
		zap.Bool("all_finished", allFinished), zap.Bool("switch_back", switchBack))

	return switchBack, allFinished, err
}

func (m *dbTaskMetaMgr) Cleanup(ctx context.Context) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.L(),
	}
	// avoid override existing metadata if the meta is already inserted.
	stmt := fmt.Sprintf("DROP TABLE %s;", m.tableName)
	if err := exec.Exec(ctx, "cleanup task meta tables", stmt); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *dbTaskMetaMgr) CleanupTask(ctx context.Context) error {
	exec := &common.SQLWithRetry{
		DB:     m.session,
		Logger: log.L(),
	}
	stmt := fmt.Sprintf("DELETE FROM %s WHERE task_id = %d;", m.tableName, m.taskID)
	err := exec.Exec(ctx, "clean up task", stmt)
	return errors.Trace(err)
}

func (m *dbTaskMetaMgr) Close() {
	m.pd.Close()
}

func (m *dbTaskMetaMgr) CleanupAllMetas(ctx context.Context) error {
	return MaybeCleanupAllMetas(ctx, m.session, m.schemaName, true)
}

// MaybeCleanupAllMetas remove the meta schema if there is no unfinished tables
func MaybeCleanupAllMetas(ctx context.Context, db *sql.DB, schemaName string, tableMetaExist bool) error {
	exec := &common.SQLWithRetry{
		DB:     db,
		Logger: log.L(),
	}

	// check if all tables are finished
	if tableMetaExist {
		query := fmt.Sprintf("SELECT COUNT(*) from %s", common.UniqueTable(schemaName, TableMetaTableName))
		var cnt int
		if err := exec.QueryRow(ctx, "fetch table meta row count", query, &cnt); err != nil {
			return errors.Trace(err)
		}
		if cnt > 0 {
			log.L().Warn("there are unfinished table in table meta table, cleanup skipped.")
			return nil
		}
	}

	// avoid override existing metadata if the meta is already inserted.
	stmt := fmt.Sprintf("DROP DATABASE %s;", common.EscapeIdentifier(schemaName))
	if err := exec.Exec(ctx, "cleanup task meta tables", stmt); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type noopMetaMgrBuilder struct{}

func (b noopMetaMgrBuilder) Init(ctx context.Context) error {
	return nil
}

func (b noopMetaMgrBuilder) TaskMetaMgr(pd *pdutil.PdController) taskMetaMgr {
	return noopTaskMetaMgr{}
}

func (b noopMetaMgrBuilder) TableMetaMgr(tr *TableRestore) tableMetaMgr {
	return noopTableMetaMgr{}
}

type noopTaskMetaMgr struct{}

func (m noopTaskMetaMgr) InitTask(ctx context.Context, source int64) error {
	return nil
}

func (m noopTaskMetaMgr) CheckTasksExclusively(ctx context.Context, action func(tasks []taskMeta) ([]taskMeta, error)) error {
	return nil
}

func (m noopTaskMetaMgr) CheckAndPausePdSchedulers(ctx context.Context) (pdutil.UndoFunc, error) {
	return func(ctx context.Context) error {
		return nil
	}, nil
}

func (m noopTaskMetaMgr) CheckTaskExist(ctx context.Context) (bool, error) {
	return true, nil
}

func (m noopTaskMetaMgr) CheckAndFinishRestore(context.Context, bool) (bool, bool, error) {
	return false, true, nil
}

func (m noopTaskMetaMgr) Cleanup(ctx context.Context) error {
	return nil
}

func (m noopTaskMetaMgr) CleanupTask(ctx context.Context) error {
	return nil
}

func (m noopTaskMetaMgr) CleanupAllMetas(ctx context.Context) error {
	return nil
}

func (m noopTaskMetaMgr) Close() {
}

type noopTableMetaMgr struct{}

func (m noopTableMetaMgr) InitTableMeta(ctx context.Context) error {
	return nil
}

func (m noopTableMetaMgr) AllocTableRowIDs(ctx context.Context, rawRowIDMax int64) (*verify.KVChecksum, int64, error) {
	return nil, 0, nil
}

func (m noopTableMetaMgr) UpdateTableStatus(ctx context.Context, status metaStatus) error {
	return nil
}

func (m noopTableMetaMgr) UpdateTableBaseChecksum(ctx context.Context, checksum *verify.KVChecksum) error {
	return nil
}

func (m noopTableMetaMgr) CheckAndUpdateLocalChecksum(ctx context.Context, checksum *verify.KVChecksum, hasLocalDupes bool) (bool, bool, *verify.KVChecksum, error) {
	return true, true, &verify.KVChecksum{}, nil
}

func (m noopTableMetaMgr) FinishTable(ctx context.Context) error {
	return nil
}

type singleMgrBuilder struct {
	taskID int64
}

func (b singleMgrBuilder) Init(context.Context) error {
	return nil
}

func (b singleMgrBuilder) TaskMetaMgr(pd *pdutil.PdController) taskMetaMgr {
	return &singleTaskMetaMgr{
		pd:     pd,
		taskID: b.taskID,
	}
}

func (b singleMgrBuilder) TableMetaMgr(tr *TableRestore) tableMetaMgr {
	return noopTableMetaMgr{}
}

type singleTaskMetaMgr struct {
	pd           *pdutil.PdController
	taskID       int64
	initialized  bool
	sourceBytes  uint64
	clusterAvail uint64
}

func (m *singleTaskMetaMgr) InitTask(ctx context.Context, source int64) error {
	m.sourceBytes = uint64(source)
	m.initialized = true
	return nil
}

func (m *singleTaskMetaMgr) CheckTasksExclusively(ctx context.Context, action func(tasks []taskMeta) ([]taskMeta, error)) error {
	newTasks, err := action([]taskMeta{
		{
			taskID:       m.taskID,
			status:       taskMetaStatusInitial,
			sourceBytes:  m.sourceBytes,
			clusterAvail: m.clusterAvail,
		},
	})
	for _, t := range newTasks {
		if m.taskID == t.taskID {
			m.sourceBytes = t.sourceBytes
			m.clusterAvail = t.clusterAvail
		}
	}
	return err
}

func (m *singleTaskMetaMgr) CheckAndPausePdSchedulers(ctx context.Context) (pdutil.UndoFunc, error) {
	return m.pd.RemoveSchedulers(ctx)
}

func (m *singleTaskMetaMgr) CheckTaskExist(ctx context.Context) (bool, error) {
	return m.initialized, nil
}

func (m *singleTaskMetaMgr) CheckAndFinishRestore(context.Context, bool) (shouldSwitchBack bool, shouldCleanupMeta bool, err error) {
	return true, true, nil
}

func (m *singleTaskMetaMgr) Cleanup(ctx context.Context) error {
	return nil
}

func (m *singleTaskMetaMgr) CleanupTask(ctx context.Context) error {
	return nil
}

func (m *singleTaskMetaMgr) CleanupAllMetas(ctx context.Context) error {
	return nil
}

func (m *singleTaskMetaMgr) Close() {
}
