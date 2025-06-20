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
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

const (
	// RestoreRegistryDBName is the database name for the restore registry table
	RestoreRegistryDBName = "mysql"
	// RestoreRegistryTableName is the table name for tracking restore tasks
	RestoreRegistryTableName = "tidb_restore_registry"

	// FilterSeparator is used to join/split filter strings safely.
	// Using ASCII Unit Separator (US) character which never appears in SQL identifiers or expressions.
	FilterSeparator = "\x1F"

	// StaleTaskThresholdMinutes is the threshold in minutes to consider a running task as potentially stale
	StaleTaskThresholdMinutes = 5

	// lookupRegistrationSQLTemplate is the SQL template for looking up a registration by its parameters
	lookupRegistrationSQLTemplate = `
		SELECT id, status FROM %s.%s
		WHERE filter_hash = MD5(%%?)
		AND start_ts = %%?
		AND restored_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		ORDER BY id DESC`

	// updateStatusSQLTemplate is the SQL template for updating a task's status
	updateStatusSQLTemplate = `
		UPDATE %s.%s
		SET status = %%?
		WHERE id = %%? AND status = %%?`

	// resumeTaskByIDSQLTemplate is the SQL template for resuming a paused task by its ID
	resumeTaskByIDSQLTemplate = `
		UPDATE %s.%s
		SET status = 'running', last_heartbeat_time = %%?
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
		(filter_strings, filter_hash, start_ts, restored_ts, upstream_cluster_id,
		 with_sys_table, status, cmd, task_start_time, last_heartbeat_time)
		VALUES (%%?, MD5(%%?), %%?, %%?, %%?, %%?, 'running', %%?, %%?, %%?)`

	// selectStaleRunningTasksSQLTemplate is the SQL template for finding potentially stale running tasks
	selectStaleRunningTasksSQLTemplate = `
		SELECT id, last_heartbeat_time
		FROM %s.%s
		WHERE status = 'running'
		AND last_heartbeat_time < DATE_SUB(NOW(), INTERVAL %%? MINUTE)
		ORDER BY id ASC`

	// selectTaskHeartbeatSQLTemplate is the SQL template for getting a specific task's heartbeat time
	selectTaskHeartbeatSQLTemplate = `
		SELECT last_heartbeat_time
		FROM %s.%s
		WHERE id = %%?`

	// selectConflictingTaskSQLTemplate is the SQL template for finding tasks with same parameters except restoredTS
	selectConflictingTaskSQLTemplate = `
		SELECT id, restored_ts, status FROM %s.%s
		WHERE filter_hash = MD5(%%?)
		AND start_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		AND restored_ts != %%?
		AND status IN ('running', 'paused')
		ORDER BY id DESC
		LIMIT 1`

	// transitionStaleTaskToPausedSQLTemplate is the SQL template for atomically transitioning a
	// stale running task to paused
	transitionStaleTaskToPausedSQLTemplate = `
		UPDATE %s.%s
		SET status = 'paused'
		WHERE id = %%? AND status = 'running' AND last_heartbeat_time = %%?`
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
	heartbeatSession glue.Session
	heartbeatManager *HeartbeatManager
}

// NewRestoreRegistry creates a new registry using TiDB's session
func NewRestoreRegistry(g glue.Glue, dom *domain.Domain) (*Registry, error) {
	se, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}
	heartbeatSession, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Registry{
		se:               se,
		heartbeatSession: heartbeatSession,
	}, nil
}

func (r *Registry) Close() {
	log.Info("closing registry")
	if r.se != nil {
		log.Info("closing registry session")
		r.se.Close()
		r.se = nil
	}
	if r.heartbeatSession != nil {
		log.Info("closing registry heartbeat session")
		r.heartbeatSession.Close()
		r.heartbeatSession = nil
	}

	r.StopHeartbeatManager()
}

// executeInTransaction executes a function within a pessimistic transaction
func (r *Registry) executeInTransaction(ctx context.Context, fn func(context.Context, sqlexec.RestrictedSQLExecutor,
	[]sqlexec.OptionFuncAlias) error) error {
	sessCtx := r.se.GetSessionCtx()
	execCtx := sessCtx.GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)

	// use ExecOptionUseCurSession to ensure all statements run in the same session
	sessionOpts := []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}

	_, _, err := execCtx.ExecRestrictedSQL(ctx, sessionOpts, "BEGIN PESSIMISTIC")
	if err != nil {
		return errors.Annotate(err, "failed to begin transaction")
	}

	// Execute the function and capture its error
	fnErr := fn(ctx, execCtx, sessionOpts)

	// Handle commit/rollback based on fn() result
	if fnErr != nil {
		if _, _, rollbackErr := execCtx.ExecRestrictedSQL(ctx, sessionOpts, "ROLLBACK"); rollbackErr != nil {
			log.Error("failed to rollback transaction", zap.Error(rollbackErr))
		}
		return fnErr
	}
	if _, _, commitErr := execCtx.ExecRestrictedSQL(ctx, sessionOpts, "COMMIT"); commitErr != nil {
		log.Error("failed to commit transaction", zap.Error(commitErr))
		return commitErr
	}

	return nil
}

// ResumeOrCreateRegistration first looks for an existing registration with the given parameters.
// If found and paused, it tries to resume it. Otherwise, it creates a new registration.
// Returns: (taskID, resolvedRestoreTS, error)
func (r *Registry) ResumeOrCreateRegistration(ctx context.Context, info RegistrationInfo,
	isRestoredTSUserSpecified bool) (uint64, uint64, error) {
	// resolve which restoredTS to use, handling auto-detection conflicts
	resolvedRestoreTS, err := r.resolveRestoreTS(ctx, info, isRestoredTSUserSpecified)
	if err != nil {
		return 0, 0, err
	}

	// update info with resolved restoredTS if different
	if resolvedRestoreTS != info.RestoredTS {
		log.Info("using resolved restoredTS from existing task",
			zap.Uint64("original_restored_ts", info.RestoredTS),
			zap.Uint64("resolved_restored_ts", resolvedRestoreTS))
		info.RestoredTS = resolvedRestoreTS
	}

	filterStrings := strings.Join(info.FilterStrings, FilterSeparator)

	log.Info("attempting to resume or create registration",
		zap.String("filter_strings", filterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.Uint64("restored_ts", info.RestoredTS),
		zap.Uint64("upstream_cluster_id", info.UpstreamClusterID),
		zap.Bool("with_sys_table", info.WithSysTable),
		zap.String("cmd", info.Cmd),
		zap.Bool("is_restored_ts_user_specified", isRestoredTSUserSpecified))

	var taskID uint64

	err = r.executeInTransaction(ctx, func(ctx context.Context, execCtx sqlexec.RestrictedSQLExecutor,
		sessionOpts []sqlexec.OptionFuncAlias) error {
		// first look for an existing task with the same parameters
		lookupSQL := fmt.Sprintf(lookupRegistrationSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
		rows, _, err := execCtx.ExecRestrictedSQL(ctx, sessionOpts, lookupSQL,
			filterStrings, info.StartTS, info.RestoredTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd)
		if err != nil {
			return errors.Annotate(err, "failed to look up existing task")
		}

		// if task found, check its status
		if len(rows) > 0 {
			existingTaskID := rows[0].GetUint64(0)
			status := rows[0].GetString(1)

			if existingTaskID == 0 {
				return errors.New("invalid task ID: got 0 from lookup")
			}

			// if task exists and is running, return error
			if status == string(TaskStatusRunning) {
				log.Warn("task already exists and is running",
					zap.Uint64("restore_id", existingTaskID))
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"task with ID %d already exists and is running", existingTaskID)
			}

			// strictly check for paused status
			if status == string(TaskStatusPaused) {
				currentTime := time.Now()
				updateSQL := fmt.Sprintf(resumeTaskByIDSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
				_, _, err = execCtx.ExecRestrictedSQL(ctx, sessionOpts, updateSQL, currentTime, existingTaskID)
				if err != nil {
					return errors.Annotate(err, "failed to resume paused task")
				}

				log.Info("successfully resumed existing registration",
					zap.Uint64("restore_id", existingTaskID),
					zap.Strings("filters", info.FilterStrings))

				taskID = existingTaskID
				return nil
			}

			// task exists but is not running or paused - this is an unexpected state
			log.Warn("task exists but in unexpected state",
				zap.Uint64("restore_id", existingTaskID),
				zap.String("status", status))
			return errors.Annotatef(berrors.ErrInvalidArgument,
				"task with ID %d exists but is in unexpected state: %s", existingTaskID, status)
		}

		// no existing task found, create a new one
		currentTime := time.Now()
		insertSQL := fmt.Sprintf(createNewTaskSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
		_, _, err = execCtx.ExecRestrictedSQL(ctx, sessionOpts, insertSQL,
			filterStrings, filterStrings, info.StartTS, info.RestoredTS,
			info.UpstreamClusterID, info.WithSysTable, info.Cmd, currentTime, currentTime)
		if err != nil {
			return errors.Annotate(err, "failed to create new registration")
		}

		lastIDRows, _, err := execCtx.ExecRestrictedSQL(ctx, sessionOpts, "SELECT LAST_INSERT_ID()")
		if err != nil {
			return errors.Annotate(err, "failed to get ID of newly created task")
		}

		if len(lastIDRows) == 0 {
			return errors.New("failed to get LAST_INSERT_ID()")
		}

		newTaskID := lastIDRows[0].GetUint64(0)
		if newTaskID == 0 {
			return errors.New("invalid task ID: got 0 from LAST_INSERT_ID()")
		}

		log.Info("successfully created new registration",
			zap.Uint64("restore_id", newTaskID),
			zap.Strings("filters", info.FilterStrings))

		taskID = newTaskID
		return nil
	})

	if err != nil {
		return 0, 0, err
	}

	return taskID, resolvedRestoreTS, nil
}

// updateTaskStatusConditional updates a task's status only if its current status matches the expected status
func (r *Registry) updateTaskStatusConditional(ctx context.Context, restoreID uint64, currentStatus,
	newStatus TaskStatus) error {
	log.Info("attempting to update task status",
		zap.Uint64("restore_id", restoreID),
		zap.String("current_status", string(currentStatus)),
		zap.String("new_status", string(newStatus)))

	// use where to update only when status is what we want
	updateSQL := fmt.Sprintf(updateStatusSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)

	if err := r.se.ExecuteInternal(ctx, updateSQL, newStatus, restoreID, currentStatus); err != nil {
		return errors.Annotatef(err, "failed to conditionally update task status from %s to %s",
			currentStatus, newStatus)
	}

	return nil
}

// Unregister removes a restore registration
func (r *Registry) Unregister(ctx context.Context, restoreID uint64) error {
	// first stop heartbeat manager
	r.StopHeartbeatManager()

	deleteSQL := fmt.Sprintf(deleteRegistrationSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
	if err := r.se.ExecuteInternal(ctx, deleteSQL, restoreID); err != nil {
		return errors.Annotatef(err, "failed to unregister restore %d", restoreID)
	}

	log.Info("unregistered restore task", zap.Uint64("restore_id", restoreID))
	return nil
}

// PauseTask marks a task as paused only if it's currently running
func (r *Registry) PauseTask(ctx context.Context, restoreID uint64) error {
	// first stop heartbeat manager
	r.StopHeartbeatManager()
	return r.updateTaskStatusConditional(ctx, restoreID, TaskStatusRunning, TaskStatusPaused)
}

// GetRegistrationsByMaxID returns all registrations with IDs smaller than maxID
func (r *Registry) GetRegistrationsByMaxID(ctx context.Context, maxID uint64) ([]RegistrationInfoWithID, error) {
	selectSQL := fmt.Sprintf(selectRegistrationsByMaxIDSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
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
			FilterStrings:     strings.Split(filterStrings, FilterSeparator),
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

// resolveRestoreTS determines which restoredTS to use, handling conflicts with existing tasks
// when restoredTS is not user-specified. Returns: (resolvedRestoreTS, error)
func (r *Registry) resolveRestoreTS(ctx context.Context,
	info RegistrationInfo, isRestoredTSUserSpecified bool) (uint64, error) {
	// if restoredTS is user-specified, use it directly without any conflict resolution
	if isRestoredTSUserSpecified {
		log.Info("restoredTS is user-specified, using it directly",
			zap.Uint64("restored_ts", info.RestoredTS))
		return info.RestoredTS, nil
	}

	filterStrings := strings.Join(info.FilterStrings, FilterSeparator)

	// look for tasks with same filter, startTS, cluster, sysTable, cmd but different restoredTS
	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)

	checkSQL := fmt.Sprintf(selectConflictingTaskSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
	rows, _, err := execCtx.ExecRestrictedSQL(ctx, nil, checkSQL,
		filterStrings, info.StartTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd, info.RestoredTS)
	if err != nil {
		return 0, errors.Annotate(err, "failed to check for existing tasks with different restoredTS")
	}

	// no conflicting task found, use the current restoredTS
	if len(rows) == 0 {
		log.Info("no existing tasks found with different restoredTS",
			zap.Uint64("restored_ts", info.RestoredTS))
		return info.RestoredTS, nil
	}

	conflictingTaskID := rows[0].GetUint64(0)
	existingRestoredTS := rows[0].GetUint64(1)
	existingStatus := rows[0].GetString(2)

	log.Info("found existing task with different restoredTS",
		zap.Uint64("existing_task_id", conflictingTaskID),
		zap.Uint64("existing_restored_ts", existingRestoredTS),
		zap.String("existing_status", existingStatus),
		zap.Uint64("current_restored_ts", info.RestoredTS),
		zap.Strings("filters", info.FilterStrings),
		zap.Uint64("start_ts", info.StartTS))

	// if existing task is paused, reuse its restoredTS
	if existingStatus == string(TaskStatusPaused) {
		log.Info("existing task is paused, reusing its restoredTS",
			zap.Uint64("existing_task_id", conflictingTaskID),
			zap.Uint64("existing_restored_ts", existingRestoredTS))
		return existingRestoredTS, nil
	}

	// if existing task is running, check if it's stale
	if existingStatus == string(TaskStatusRunning) {
		log.Info("existing task is running, checking if it's stale",
			zap.Uint64("existing_task_id", conflictingTaskID))

		// First, get the current heartbeat time for atomic transition
		selectHeartbeatSQL := fmt.Sprintf(selectTaskHeartbeatSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
		heartbeatRows, _, err := execCtx.ExecRestrictedSQL(ctx, nil, selectHeartbeatSQL, conflictingTaskID)
		if err != nil {
			log.Warn("failed to get task heartbeat, using current restoredTS",
				zap.Uint64("task_id", conflictingTaskID),
				zap.Error(err))
			return info.RestoredTS, nil
		}

		if len(heartbeatRows) == 0 {
			log.Info("task not found during heartbeat check, using current restoredTS",
				zap.Uint64("task_id", conflictingTaskID))
			return info.RestoredTS, nil
		}

		currentHeartbeatTime := heartbeatRows[0].GetTime(0).String()

		isStale, err := r.isTaskStale(ctx, conflictingTaskID)
		if err != nil {
			log.Warn("failed to check if task is stale, using current restoredTS",
				zap.Uint64("task_id", conflictingTaskID),
				zap.Error(err))
			return info.RestoredTS, nil
		}

		if isStale {
			log.Info("existing running task is stale, attempting to transition to paused",
				zap.Uint64("existing_task_id", conflictingTaskID),
				zap.Uint64("existing_restored_ts", existingRestoredTS))

			// atomically transition the stale task to paused state
			transitioned, transitionErr := r.transitionStaleTaskToPaused(ctx, conflictingTaskID, currentHeartbeatTime)
			if transitionErr != nil {
				log.Warn("failed to transition stale task to paused, using current restoredTS",
					zap.Uint64("task_id", conflictingTaskID),
					zap.Error(transitionErr))
				return info.RestoredTS, nil
			}

			if transitioned {
				log.Info("successfully transitioned stale task to paused, will reuse its restoredTS",
					zap.Uint64("existing_task_id", conflictingTaskID),
					zap.Uint64("existing_restored_ts", existingRestoredTS))
				return existingRestoredTS, nil
			}
			log.Info("task was not transitioned (concurrent update), using current restoredTS",
				zap.Uint64("existing_task_id", conflictingTaskID))
			return info.RestoredTS, nil
		}

		log.Info("existing running task is active, using current restoredTS",
			zap.Uint64("existing_task_id", conflictingTaskID))
		return info.RestoredTS, nil
	}

	// existing task is in unexpected state, use current restoredTS
	log.Warn("existing task is in unexpected state, using current restoredTS",
		zap.Uint64("existing_task_id", conflictingTaskID),
		zap.String("status", existingStatus))
	return info.RestoredTS, nil
}

// isTaskStale checks if a running task is stale by waiting up to 5 minutes and checking if heartbeat updates
func (r *Registry) isTaskStale(ctx context.Context, taskID uint64) (bool, error) {
	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)

	// get initial heartbeat time
	selectHeartbeatSQL := fmt.Sprintf(selectTaskHeartbeatSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
	initialRows, _, err := execCtx.ExecRestrictedSQL(ctx, nil, selectHeartbeatSQL, taskID)
	if err != nil {
		return false, errors.Annotate(err, "failed to get initial heartbeat time")
	}

	if len(initialRows) == 0 {
		return false, nil // task not found (might have been deleted), proceed with user's restoredTS
	}

	initialHeartbeatTime := initialRows[0].GetTime(0).String()

	log.Info("checking if task is stale, will check heartbeat every minute up to 5 minutes",
		zap.Uint64("task_id", taskID),
		zap.String("initial_heartbeat", initialHeartbeatTime))

	// check heartbeat every minute for up to 5 minutes
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	remainingMinutes := StaleTaskThresholdMinutes
	for remainingMinutes > 0 {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-ticker.C:
			remainingMinutes--

			// check heartbeat time at each tick
			currentRows, _, err := execCtx.ExecRestrictedSQL(ctx, nil, selectHeartbeatSQL, taskID)
			if err != nil {
				log.Warn("failed to check heartbeat during stale check, assuming task is active",
					zap.Uint64("task_id", taskID),
					zap.Error(err))
				return false, nil
			}

			if len(currentRows) == 0 {
				return false, nil // task not found (might have been deleted), proceed with user's restoredTS
			}

			currentHeartbeatTime := currentRows[0].GetTime(0).String()

			// if heartbeat changed, task is active - exit early
			if currentHeartbeatTime != initialHeartbeatTime {
				log.Info("task heartbeat updated, task is active",
					zap.Uint64("task_id", taskID),
					zap.String("initial_heartbeat", initialHeartbeatTime),
					zap.String("current_heartbeat", currentHeartbeatTime),
					zap.Int("minutes_waited", StaleTaskThresholdMinutes-remainingMinutes))
				return false, nil
			}

			if remainingMinutes > 0 {
				log.Info("task heartbeat unchanged, continuing to wait",
					zap.Int("remaining_minutes", remainingMinutes),
					zap.Uint64("task_id", taskID))
			}
		}
	}

	// if we get here, heartbeat hasn't changed for 5 minutes - task is stale
	log.Info("task heartbeat unchanged for 5 minutes, task is stale",
		zap.Uint64("task_id", taskID),
		zap.String("initial_heartbeat", initialHeartbeatTime))

	return true, nil
}

// transitionStaleTaskToPaused atomically transitions a stale running task to paused state
// if the heartbeat timestamp hasn't changed. Returns whether the transition was successful.
func (r *Registry) transitionStaleTaskToPaused(ctx context.Context, taskID uint64,
	expectedHeartbeatTime string) (bool, error) {
	log.Info("attempting to transition stale task to paused state",
		zap.Uint64("task_id", taskID),
		zap.String("expected_heartbeat", expectedHeartbeatTime))

	var transitioned bool
	err := r.executeInTransaction(ctx, func(ctx context.Context, execCtx sqlexec.RestrictedSQLExecutor,
		sessionOpts []sqlexec.OptionFuncAlias) error {
		// atomically update task to paused only if it's still running with the same heartbeat time
		updateSQL := fmt.Sprintf(transitionStaleTaskToPausedSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)

		// We need to parse the heartbeat time string back to time.Time for the SQL query
		// The expectedHeartbeatTime comes from MySQL's time format
		expectedTime, parseErr := time.Parse("2006-01-02 15:04:05", expectedHeartbeatTime)
		if parseErr != nil {
			return errors.Annotatef(parseErr, "failed to parse expected heartbeat time: %s", expectedHeartbeatTime)
		}

		_, _, updateErr := execCtx.ExecRestrictedSQL(ctx, sessionOpts, updateSQL, taskID, expectedTime)
		if updateErr != nil {
			return errors.Annotate(updateErr, "failed to transition stale task to paused")
		}

		// Check if the task was actually transitioned by querying its current status
		checkTaskSQL := fmt.Sprintf(
			"SELECT status FROM %s.%s WHERE id = %%?", RestoreRegistryDBName, RestoreRegistryTableName)
		var statusRows []chunk.Row
		var checkErr error
		statusRows, _, checkErr = execCtx.ExecRestrictedSQL(ctx, sessionOpts, checkTaskSQL, taskID)
		if checkErr != nil {
			return errors.Annotate(checkErr, "failed to check task status after transition attempt")
		}

		if len(statusRows) > 0 && statusRows[0].GetString(0) == string(TaskStatusPaused) {
			transitioned = true
			log.Info("successfully transitioned stale task to paused state",
				zap.Uint64("task_id", taskID))
		} else {
			log.Info("task was not transitioned (either already changed state or heartbeat was updated)",
				zap.Uint64("task_id", taskID))
		}

		return nil
	})

	if err != nil {
		return false, err
	}

	return transitioned, nil
}
