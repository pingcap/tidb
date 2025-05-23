// Copyright 2025 PingCAP, Inc.
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

package registry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

const (
	RegistrationDBName    string = utils.TemporaryDBNamePrefix + "Restore_Registration_DB"
	RegistrationTableName        = "restore_registry"

	// createRegistrationTableSQL is the SQL to create the registration table
	// we use unique index to prevent race condition that two threads inserting tasks with same parameters.
	// we initialize auto increment id to be 1 to make sure default value 0 is not used when insertion failed.
	createRegistrationTableSQL = `CREATE TABLE IF NOT EXISTS %s.%s (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		filter_strings TEXT NOT NULL,
		filter_hash VARCHAR(64) NOT NULL,
		start_ts BIGINT UNSIGNED NOT NULL,
		restored_ts BIGINT UNSIGNED NOT NULL,
		upstream_cluster_id BIGINT UNSIGNED,
		with_sys_table BOOLEAN NOT NULL DEFAULT TRUE,
		status VARCHAR(20) NOT NULL DEFAULT 'running',
		cmd TEXT,
		start_timestamp BIGINT UNSIGNED NOT NULL,
		last_heartbeat BIGINT UNSIGNED NOT NULL,
		UNIQUE KEY unique_registration_params (
			filter_hash,
			start_ts,
			restored_ts,
			upstream_cluster_id,
			with_sys_table,
			cmd(256)
		)
	) AUTO_INCREMENT = 1`

	// lookupRegistrationSQLTemplate is the SQL template for looking up a registration by its parameters
	lookupRegistrationSQLTemplate = `
		SELECT id, status FROM %s.%s
		WHERE filter_hash = MD5(%%?)
		AND start_ts = %%?
		AND restored_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		ORDER BY id DESC LIMIT 1`

	// updateStatusSQLTemplate is the SQL template for updating a task's status
	updateStatusSQLTemplate = `
		UPDATE %s.%s
		SET status = %%?
		WHERE id = %%? AND status = %%?`

	// resumeTaskByIDSQLTemplate is the SQL template for resuming a paused task by its ID
	resumeTaskByIDSQLTemplate = `
		UPDATE %s.%s
		SET status = 'running'
		WHERE id = %%?`

	// deleteRegistrationSQLTemplate is the SQL template for deleting a registration
	deleteRegistrationSQLTemplate = `DELETE FROM %s.%s WHERE id = %%?`

	// selectRegistrationsByMaxIDSQLTemplate is the SQL template for selecting registrations by max ID
	selectRegistrationsByMaxIDSQLTemplate = `
		SELECT
		id, filter_strings, start_ts, restored_ts, upstream_cluster_id, with_sys_table, status, cmd, filter_hash
		FROM %s.%s
		WHERE id < %%?
		ORDER BY id ASC`

	// createNewTaskSQLTemplate is the SQL template for creating a new task
	createNewTaskSQLTemplate = `
		INSERT INTO %s.%s
		(filter_strings, filter_hash, start_ts, restored_ts, upstream_cluster_id, with_sys_table, status, cmd, start_timestamp, last_heartbeat)
		VALUES (%%?, MD5(%%?), %%?, %%?, %%?, %%?, 'running', %%?, %%?, %%?)`
)

// TaskStatus represents the current state of a restore task
type TaskStatus string

const (
	// TaskStatusRunning indicates the task is currently active
	TaskStatusRunning TaskStatus = "running"
	// TaskStatusPaused indicates the task is temporarily stopped
	TaskStatusPaused TaskStatus = "paused"
)

// RegistrationInfo contains information about a registered restore
type RegistrationInfo struct {
	// filter patterns
	FilterStrings []string

	// time range for restore
	StartTS    uint64
	RestoredTS uint64

	// identifier of the upstream cluster
	UpstreamClusterID uint64

	// whether to include system tables
	WithSysTable bool

	// restore command
	Cmd string
}

type RegistrationInfoWithID struct {
	RegistrationInfo
	restoreID uint64
}

// Registry manages registrations of restore tasks
type Registry struct {
	se               glue.Session
	heartbeatManager *HeartbeatManager
}

// NewRestoreRegistry creates a new registry using TiDB's session
func NewRestoreRegistry(g glue.Glue, dom *domain.Domain) (*Registry, error) {
	se, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Registry{
		se: se,
	}, nil
}

func (r *Registry) Close() {
	if r.se != nil {
		r.se.Close()
		r.se = nil
	}

	r.StopHeartbeatManager()
}

// createTableIfNotExist ensures that the registration table exists
func (r *Registry) createTableIfNotExist(ctx context.Context) error {
	createStmt := fmt.Sprintf(createRegistrationTableSQL, RegistrationDBName, RegistrationTableName)

	if err := r.se.ExecuteInternal(ctx, "CREATE DATABASE IF NOT EXISTS %n;", RegistrationDBName); err != nil {
		return errors.Annotatef(err, "failed to create registration database")
	}

	if err := r.se.ExecuteInternal(ctx, createStmt); err != nil {
		return errors.Annotatef(err, "failed to create registration table")
	}

	log.Info("ensured restore registration table exists",
		zap.String("db", RegistrationDBName),
		zap.String("table", RegistrationTableName))

	return nil
}

// ResumeOrCreateRegistration first looks for an existing registration with the given parameters.
// If found and paused, it tries to resume it. Otherwise, it creates a new registration.
func (r *Registry) ResumeOrCreateRegistration(ctx context.Context, info RegistrationInfo) (uint64, error) {
	if err := r.createTableIfNotExist(ctx); err != nil {
		return 0, errors.Trace(err)
	}

	filterStrings := strings.Join(info.FilterStrings, ",")

	log.Info("attempting to resume or create registration",
		zap.String("filter_strings", filterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.Uint64("restored_ts", info.RestoredTS),
		zap.Uint64("upstream_cluster_id", info.UpstreamClusterID),
		zap.Bool("with_sys_table", info.WithSysTable),
		zap.String("cmd", info.Cmd))

	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()

	// Set transaction mode to pessimistic
	_, _, err := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"SET @@tidb_txn_mode = 'pessimistic'")
	if err != nil {
		return 0, errors.Annotate(err, "failed to set pessimistic mode")
	}

	// Begin transaction
	_, _, err = execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"BEGIN")
	if err != nil {
		return 0, errors.Annotate(err, "failed to begin transaction")
	}

	defer func() {
		_, _, _ = execCtx.ExecRestrictedSQL(
			kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
			"ROLLBACK")
	}()

	// First look for an existing task with the same parameters
	lookupSQL := fmt.Sprintf(lookupRegistrationSQLTemplate, RegistrationDBName, RegistrationTableName)
	rows, _, err := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		lookupSQL,
		filterStrings, info.StartTS, info.RestoredTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd)
	if err != nil {
		return 0, errors.Annotate(err, "failed to look up existing task")
	}

	// If task found, check its status
	if len(rows) > 0 {
		taskID := rows[0].GetUint64(0)
		status := rows[0].GetString(1)

		if taskID == 0 {
			return 0, errors.New("invalid task ID: got 0 from lookup")
		}

		// If task exists and is running, return error
		if status == string(TaskStatusRunning) {
			log.Warn("task already exists and is running",
				zap.Uint64("restore_id", taskID))
			return 0, errors.Annotatef(berrors.ErrInvalidArgument,
				"task with ID %d already exists and is running", taskID)
		}

		// Strictly check for paused status
		if status == string(TaskStatusPaused) {
			updateSQL := fmt.Sprintf(resumeTaskByIDSQLTemplate, RegistrationDBName, RegistrationTableName)

			_, _, err = execCtx.ExecRestrictedSQL(
				kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
				[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
				updateSQL,
				taskID)
			if err != nil {
				return 0, errors.Annotate(err, "failed to resume paused task")
			}

			// Update the heartbeat
			currentTime := uint64(time.Now().Unix())
			updateHeartbeatSQL := fmt.Sprintf(UpdateHeartbeatSQLTemplate, RegistrationDBName, RegistrationTableName)
			_, _, err = execCtx.ExecRestrictedSQL(
				kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
				[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
				updateHeartbeatSQL,
				currentTime, taskID)
			if err != nil {
				log.Warn("failed to update heartbeat for resumed task",
					zap.Uint64("restore_id", taskID),
					zap.Error(err))
				// Continue despite heartbeat error
			}

			_, _, err = execCtx.ExecRestrictedSQL(
				kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
				[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
				"COMMIT")
			if err != nil {
				return 0, errors.Annotate(err, "failed to commit transaction")
			}

			log.Info("successfully resumed existing registration",
				zap.Uint64("restore_id", taskID),
				zap.Strings("filters", info.FilterStrings))
			return taskID, nil
		}

		// Task exists but is not running or paused - this is an unexpected state
		log.Warn("task exists but in unexpected state",
			zap.Uint64("restore_id", taskID),
			zap.String("status", status))
		return 0, errors.Annotatef(berrors.ErrInvalidArgument,
			"task with ID %d exists but is in unexpected state: %s", taskID, status)
	}

	// No existing task found, create a new one
	currentTime := uint64(time.Now().Unix())
	insertSQL := fmt.Sprintf(createNewTaskSQLTemplate, RegistrationDBName, RegistrationTableName)
	_, _, err = execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		insertSQL,
		filterStrings, filterStrings, info.StartTS, info.RestoredTS,
		info.UpstreamClusterID, info.WithSysTable, info.Cmd, currentTime, currentTime)
	if err != nil {
		return 0, errors.Annotate(err, "failed to create new registration")
	}

	rows, _, err = execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"SELECT LAST_INSERT_ID()")
	if err != nil {
		return 0, errors.Annotate(err, "failed to get ID of newly created task")
	}

	if len(rows) == 0 {
		return 0, errors.New("failed to get LAST_INSERT_ID()")
	}

	taskID := rows[0].GetUint64(0)

	if taskID == 0 {
		return 0, errors.New("invalid task ID: got 0 from LAST_INSERT_ID()")
	}

	_, _, err = execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"COMMIT")
	if err != nil {
		return 0, errors.Annotate(err, "failed to commit transaction")
	}

	log.Info("successfully created new registration",
		zap.Uint64("restore_id", taskID),
		zap.Strings("filters", info.FilterStrings))

	return taskID, nil
}

// updateTaskStatusConditional updates a task's status only if its current status matches the expected status
func (r *Registry) updateTaskStatusConditional(ctx context.Context, restoreID uint64, currentStatus,
	newStatus TaskStatus) error {
	log.Info("attempting to update task status",
		zap.Uint64("restore_id", restoreID),
		zap.String("current_status", string(currentStatus)),
		zap.String("new_status", string(newStatus)))

	// use where to update only when status is what we want
	updateSQL := fmt.Sprintf(updateStatusSQLTemplate, RegistrationDBName, RegistrationTableName)

	if err := r.se.ExecuteInternal(ctx, updateSQL, newStatus, restoreID, currentStatus); err != nil {
		return errors.Annotatef(err, "failed to conditionally update task status from %s to %s",
			currentStatus, newStatus)
	}

	return nil
}

// Unregister removes a restore registration
func (r *Registry) Unregister(ctx context.Context, restoreID uint64) error {
	deleteSQL := fmt.Sprintf(deleteRegistrationSQLTemplate, RegistrationDBName, RegistrationTableName)

	if err := r.se.ExecuteInternal(ctx, deleteSQL, restoreID); err != nil {
		return errors.Annotatef(err, "failed to unregister restore %d", restoreID)
	}

	log.Info("unregistered restore task", zap.Uint64("restore_id", restoreID))
	return nil
}

// PauseTask marks a task as paused only if it's currently running
func (r *Registry) PauseTask(ctx context.Context, restoreID uint64) error {
	return r.updateTaskStatusConditional(ctx, restoreID, TaskStatusRunning, TaskStatusPaused)
}

// GetRegistrationsByMaxID returns all registrations with IDs smaller than maxID
func (r *Registry) GetRegistrationsByMaxID(ctx context.Context, maxID uint64) ([]RegistrationInfoWithID, error) {
	if err := r.createTableIfNotExist(ctx); err != nil {
		return nil, errors.Trace(err)
	}

	selectSQL := fmt.Sprintf(selectRegistrationsByMaxIDSQLTemplate, RegistrationDBName, RegistrationTableName)
	registrations := make([]RegistrationInfoWithID, 0)

	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		selectSQL,
		maxID,
	)
	if errSQL != nil {
		return nil, errors.Annotatef(errSQL, "failed to query registrations with max ID %d", maxID)
	}

	for _, row := range rows {
		log.Info("found existing restore task", zap.Uint64("restore_id", row.GetUint64(0)),
			zap.Uint64("max_id", maxID))
		var (
			filterStrings     = row.GetString(1)
			startTS           = row.GetUint64(2)
			restoredTS        = row.GetUint64(3)
			upstreamClusterID = row.GetUint64(4)
			withSysTable      = row.GetInt64(5) != 0 // convert from int64 to bool
			cmd               = row.GetString(7)
		)

		info := RegistrationInfo{
			FilterStrings:     strings.Split(filterStrings, ","),
			StartTS:           startTS,
			RestoredTS:        restoredTS,
			UpstreamClusterID: upstreamClusterID,
			WithSysTable:      withSysTable,
			Cmd:               cmd,
		}

		infoWithID := RegistrationInfoWithID{
			info,
			row.GetUint64(0),
		}

		registrations = append(registrations, infoWithID)
	}

	return registrations, nil
}

// CheckTablesWithRegisteredTasks checks if tables and databases conflict with existing registered restore tasks
func (r *Registry) CheckTablesWithRegisteredTasks(
	ctx context.Context,
	restoreID uint64,
	tracker *utils.PiTRIdTracker,
	tables []*metautil.Table,
) error {
	registrations, err := r.GetRegistrationsByMaxID(ctx, restoreID)
	if err != nil {
		return errors.Annotatef(err, "failed to query existing registrations")
	}

	if len(registrations) == 0 {
		log.Info("found zero existing registered tasks")
		return nil
	}

	for _, regInfo := range registrations {
		f, err := filter.Parse(regInfo.FilterStrings)
		if err != nil {
			log.Warn("failed to parse filter strings from registration",
				zap.Strings("filter_strings", regInfo.FilterStrings),
				zap.Error(err))
			continue
		}

		f = filter.CaseInsensitive(f)

		// check if a table is already being restored
		if err := r.checkForTableConflicts(tracker, tables, regInfo, f, restoreID); err != nil {
			return err
		}
	}

	log.Info("no conflicts found with existing restore tasks",
		zap.Int("tables_count", len(tables)),
		zap.Uint64("current_restore_id", restoreID))

	return nil
}

// checkForTableConflicts checks if any tables (from either PiTRTableTracker or tables array)
// match with the given filter, indicating a conflict with an existing restore task
func (r *Registry) checkForTableConflicts(
	tracker *utils.PiTRIdTracker,
	tables []*metautil.Table,
	regInfo RegistrationInfoWithID,
	f filter.Filter,
	curRestoreID uint64,
) error {
	// function to handle conflict when found
	handleConflict := func(dbName, tableName string) error {
		log.Warn("table already covered by another restore task",
			zap.Uint64("existing_restore_id", regInfo.restoreID),
			zap.Uint64("current_restore_id", curRestoreID),
			zap.String("database", dbName),
			zap.String("table", tableName),
			zap.Strings("filter_strings", regInfo.FilterStrings),
			zap.Uint64("start_ts", regInfo.StartTS),
			zap.Uint64("restored_ts", regInfo.RestoredTS),
			zap.Uint64("upstream_cluster_id", regInfo.UpstreamClusterID),
			zap.Bool("with_sys_table", regInfo.WithSysTable),
			zap.String("cmd", regInfo.Cmd))
		return errors.Annotatef(berrors.ErrTablesAlreadyExisted,
			"table %s.%s cannot be restored by current task with ID %d "+
				"because it is already being restored by task (restoreId: %d, time range: %d->%d, cmd: %s)",
			dbName, tableName, curRestoreID, regInfo.restoreID, regInfo.StartTS, regInfo.RestoredTS, regInfo.Cmd)
	}

	// Use PiTRTableTracker if available for PiTR task
	if tracker != nil && len(tracker.GetDBNameToTableName()) > 0 {
		for dbName, tableNames := range tracker.GetDBNameToTableName() {
			for tableName := range tableNames {
				if utils.MatchTable(f, dbName, tableName, regInfo.WithSysTable) {
					return handleConflict(dbName, tableName)
				}
			}
		}
	} else {
		// use tables as this is a snapshot restore task
		for _, table := range tables {
			dbName := table.DB.Name.O
			tableName := table.Info.Name.O

			if utils.MatchTable(f, dbName, tableName, regInfo.WithSysTable) {
				return handleConflict(dbName, tableName)
			}
		}
	}

	return nil
}

// IsRestoreRegistryDB checks whether the dbname is a restore registry database.
func IsRestoreRegistryDB(dbname string) bool {
	return dbname == RegistrationDBName
}

// StartHeartbeatManager creates and starts a new heartbeat manager for the given restore ID
func (r *Registry) StartHeartbeatManager(ctx context.Context, restoreID uint64) {
	r.StopHeartbeatManager()

	manager := NewHeartbeatManager(r, restoreID)
	r.heartbeatManager = manager
	manager.Start(ctx)

	log.Info("started heartbeat manager for restore task", zap.Uint64("restore_id", restoreID))
}

// StopHeartbeatManager stops the heartbeat manager for the given restore ID
func (r *Registry) StopHeartbeatManager() {
	if r.heartbeatManager != nil {
		r.heartbeatManager.Stop()
		r.heartbeatManager = nil
		log.Info("stopped heartbeat manager for restore task")
	}
}
