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

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

const (
	RegistrationDBName    string = utils.TemporaryDBNamePrefix + "Restore_Registration_DB"
	RegistrationTableName        = "restore_registry"

	// createRegistrationTableSQL is the SQL to create the registration table
	// we use unique index to prevent race condition that two threads inserting tasks with same parameters.
	// we use uuid to verify the task is indeed created by the current thread, since duplicate key errors thrown by
	// unique index checking might be silent and not returned to caller.
	// we initialize auto increment id to be 1 to make sure default value 0 is not used when insertion failed.
	createRegistrationTableSQL = `CREATE TABLE IF NOT EXISTS %s.%s (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		filter_strings TEXT NOT NULL,
		start_ts BIGINT UNSIGNED NOT NULL,
		restored_ts BIGINT UNSIGNED NOT NULL,
		upstream_cluster_id BIGINT UNSIGNED,
		with_sys_table BOOLEAN NOT NULL DEFAULT TRUE,
		status VARCHAR(20) NOT NULL DEFAULT 'running',
		cmd TEXT,
		uuid VARCHAR(64),
		UNIQUE KEY unique_registration_params (
			filter_strings(255),
			start_ts,
			restored_ts,
			upstream_cluster_id,
			with_sys_table,
			cmd(255)
		)
	) AUTO_INCREMENT = 1`

	// lookupRegistrationSQLTemplate is the SQL template for looking up a registration by its parameters
	lookupRegistrationSQLTemplate = `
		SELECT id, uuid FROM %s.%s
		WHERE filter_strings = %%?
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

	// deleteRegistrationSQLTemplate is the SQL template for deleting a registration
	deleteRegistrationSQLTemplate = `DELETE FROM %s.%s WHERE id = %%?`

	// selectRegistrationsByMaxIDSQLTemplate is the SQL template for selecting registrations by max ID
	selectRegistrationsByMaxIDSQLTemplate = `
		SELECT
		id, filter_strings, start_ts, restored_ts, upstream_cluster_id, with_sys_table, status, cmd
		FROM %s.%s
		WHERE id < %%?
		ORDER BY id ASC`

	// resumePausedTaskSQLTemplate is the SQL template for resuming a paused task
	resumePausedTaskSQLTemplate = `
		UPDATE %s.%s
		SET status = 'running', uuid = %%?
		WHERE filter_strings = %%?
		AND start_ts = %%?
		AND restored_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		AND status = 'paused'`

	// getRunningTaskIDSQLTemplate is the SQL template for getting restore ID of a running task
	getRunningTaskIDSQLTemplate = `
		SELECT id FROM %s.%s
		WHERE filter_strings = %%?
		AND start_ts = %%?
		AND restored_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		AND status = 'running'`

	// createNewTaskSQLTemplate is the SQL template for creating a new task
	createNewTaskSQLTemplate = `
		INSERT INTO %s.%s
		(filter_strings, start_ts, restored_ts, upstream_cluster_id, with_sys_table, status, cmd, uuid)
		VALUES (%%?, %%?, %%?, %%?, %%?, 'running', %%?, %%?)`
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
	se glue.Session
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

	operationUUID := uuid.New().String()

	filterStrings := strings.Join(info.FilterStrings, ",")

	log.Info("checking for task to resume",
		zap.String("filter_strings", filterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.Uint64("restored_ts", info.RestoredTS),
		zap.Uint64("upstream_cluster_id", info.UpstreamClusterID),
		zap.Bool("with_sys_table", info.WithSysTable),
		zap.String("cmd", info.Cmd),
		zap.String("uuid", operationUUID))

	// first try to update if exists and paused
	updateSQL := fmt.Sprintf(resumePausedTaskSQLTemplate, RegistrationDBName, RegistrationTableName)

	if err := r.se.ExecuteInternal(ctx, updateSQL,
		operationUUID, filterStrings, info.StartTS, info.RestoredTS, info.UpstreamClusterID,
		info.WithSysTable, info.Cmd); err != nil {
		return 0, errors.Annotatef(err, "failed to update existing registration")
	}

	// check if a task with our parameters and in running state exists
	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, err := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		fmt.Sprintf(lookupRegistrationSQLTemplate, RegistrationDBName, RegistrationTableName),
		filterStrings, info.StartTS, info.RestoredTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd)
	if err != nil {
		return 0, errors.Annotatef(err, "failed to look up task")
	}

	// if we found a task, check if it was resumed by us
	if len(rows) > 0 {
		taskID := rows[0].GetUint64(0)
		foundUUID := rows[0].GetString(1)

		if taskID == 0 {
			return 0, errors.New("invalid task ID: got 0 from lookup")
		}

		// check if this task has our UUID
		if foundUUID == operationUUID {
			log.Info("successfully resumed existing registration by this process",
				zap.Uint64("restore_id", taskID),
				zap.String("uuid", operationUUID),
				zap.Strings("filters", info.FilterStrings))
			return taskID, nil
		}
		// task exists but was either created by another process or has been running
		// check if it's in running state
		runningRows, _, err := execCtx.ExecRestrictedSQL(
			kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
			nil,
			fmt.Sprintf(getRunningTaskIDSQLTemplate, RegistrationDBName, RegistrationTableName),
			filterStrings, info.StartTS, info.RestoredTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd)
		if err != nil {
			return 0, errors.Annotatef(err, "failed to check for running task")
		}

		if len(runningRows) > 0 {
			taskID := runningRows[0].GetUint64(0)
			log.Warn("task already exists and is running",
				zap.Uint64("restore_id", taskID))
			return 0, errors.Annotatef(berrors.ErrInvalidArgument,
				"task with ID %d already exists and is running", taskID)
		}
		// Task exists but is not running - unexpected state
		log.Warn("task exists but is in an unexpected state",
			zap.Uint64("restore_id", taskID),
			zap.String("uuid", foundUUID))
		return 0, errors.New("task exists but is in an unexpected state")
	}

	// no existing task found, create a new one
	insertSQL := fmt.Sprintf(createNewTaskSQLTemplate, RegistrationDBName, RegistrationTableName)

	log.Info("attempting to create new registration",
		zap.String("filter_strings", filterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.Uint64("restored_ts", info.RestoredTS),
		zap.Uint64("upstream_cluster_id", info.UpstreamClusterID),
		zap.Bool("with_sys_table", info.WithSysTable),
		zap.String("cmd", info.Cmd),
		zap.String("uuid", operationUUID))

	if err := r.se.ExecuteInternal(ctx, insertSQL,
		filterStrings, info.StartTS, info.RestoredTS, info.UpstreamClusterID, info.WithSysTable,
		info.Cmd, operationUUID); err != nil {
		return 0, errors.Annotatef(err, "failed to create new registration")
	}

	// check if a row with our parameters exists
	rows, _, err = execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		fmt.Sprintf(lookupRegistrationSQLTemplate, RegistrationDBName, RegistrationTableName),
		filterStrings, info.StartTS, info.RestoredTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd)
	if err != nil {
		return 0, errors.Annotatef(err, "failed to look up inserted task")
	}

	if len(rows) == 0 {
		// this is really unexpected - record doesn't exist at all
		log.Error("failed to find task after insertion - record doesn't exist")
		return 0, errors.New("failed to find task after insertion")
	}

	taskID := rows[0].GetUint64(0)
	foundUUID := rows[0].GetString(1)

	if taskID == 0 {
		return 0, errors.New("invalid task ID: got 0 from lookup")
	}

	// check if this task was created by us or another process
	if foundUUID != operationUUID {
		log.Info("task was created by another process",
			zap.Uint64("restore_id", taskID),
			zap.String("our_uuid", operationUUID),
			zap.String("found_uuid", foundUUID),
			zap.Strings("filters", info.FilterStrings))
	} else {
		log.Info("successfully created new registration by this process",
			zap.Uint64("restore_id", taskID),
			zap.String("uuid", operationUUID),
			zap.Strings("filters", info.FilterStrings))
	}

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
