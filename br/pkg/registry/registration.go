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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore/nameroute"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/types"
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
		WHERE filter_hash = SHA2(%%?, 256)
		AND route_hash = %%?
		AND start_ts = %%?
		AND restored_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		ORDER BY id DESC
		FOR UPDATE`
	legacyLookupRegistrationSQLTemplate = `
		SELECT id, status FROM %s.%s
		WHERE filter_hash = MD5(%%?)
		AND start_ts = %%?
		AND restored_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		ORDER BY id DESC
		FOR UPDATE`

	// updateStatusSQLTemplate is the SQL template for updating a task's status
	updateStatusSQLTemplate = `
		UPDATE %s.%s
		SET status = %%?
		WHERE id = %%? AND status = %%?`

	// updateStatusFromMultipleSQLTemplate is the SQL template for updating a task's status
	// when the current status can be one of multiple values
	updateStatusFromMultipleSQLTemplate = `
		UPDATE %s.%s
		SET status = %%?
		WHERE id = %%? AND status IN (%s)`

	// resumeTaskByIDSQLTemplate is the SQL template for resuming a paused task by its ID
	resumeTaskByIDSQLTemplate = `
		UPDATE %s.%s
		SET status = 'running', last_heartbeat_time = FROM_UNIXTIME(%%?)
		WHERE id = %%?`

	// deleteRegistrationSQLTemplate is the SQL template for deleting a registration
	deleteRegistrationSQLTemplate = `DELETE FROM %s.%s WHERE id = %%?`

	// selectRegistrationsByMaxIDSQLTemplate is the SQL template for selecting registrations by max ID
	selectRegistrationsByMaxIDSQLTemplate = `
		SELECT
		id, filter_strings, source_filter_strings, route_strings, route_hash, start_ts, restored_ts, upstream_cluster_id, with_sys_table, status, cmd, filter_hash
		FROM %s.%s
		WHERE id < %%?
		ORDER BY id ASC`
	legacySelectRegistrationsByMaxIDSQLTemplate = `
		SELECT
		id, filter_strings, start_ts, restored_ts, upstream_cluster_id, with_sys_table, status, cmd, filter_hash
		FROM %s.%s
		WHERE id < %%?
		ORDER BY id ASC`

	// createNewTaskSQLTemplate is the SQL template for creating a new task
	createNewTaskSQLTemplate = `
		INSERT INTO %s.%s
		(filter_strings, filter_hash, source_filter_strings, route_strings, route_hash, start_ts, restored_ts, upstream_cluster_id,
		 with_sys_table, status, cmd, task_start_time, last_heartbeat_time)
		VALUES (%%?, SHA2(%%?, 256), %%?, %%?, %%?, %%?, %%?, %%?, %%?, 'running', %%?, FROM_UNIXTIME(%%?), FROM_UNIXTIME(%%?))`
	legacyCreateNewTaskSQLTemplate = `
		INSERT INTO %s.%s
		(filter_strings, filter_hash, start_ts, restored_ts, upstream_cluster_id,
		 with_sys_table, status, cmd, task_start_time, last_heartbeat_time)
		VALUES (%%?, MD5(%%?), %%?, %%?, %%?, %%?, 'running', %%?, FROM_UNIXTIME(%%?), FROM_UNIXTIME(%%?))`

	// selectTaskHeartbeatSQLTemplate is the SQL template for getting a specific task's heartbeat time
	selectTaskHeartbeatSQLTemplate = `
		SELECT CAST(UNIX_TIMESTAMP(last_heartbeat_time) AS UNSIGNED INTEGER)
		FROM %s.%s
		WHERE id = %%?`

	// selectConflictingTaskSQLTemplate is the SQL template for finding tasks with same parameters
	selectConflictingTaskSQLTemplate = `
		SELECT id, restored_ts, status, CAST(UNIX_TIMESTAMP(last_heartbeat_time) AS UNSIGNED INTEGER) FROM %s.%s
		WHERE filter_hash = SHA2(%%?, 256)
		AND route_hash = %%?
		AND start_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		ORDER BY id DESC
		LIMIT 1`
	legacySelectConflictingTaskSQLTemplate = `
		SELECT id, restored_ts, status, CAST(UNIX_TIMESTAMP(last_heartbeat_time) AS UNSIGNED INTEGER) FROM %s.%s
		WHERE filter_hash = MD5(%%?)
		AND start_ts = %%?
		AND upstream_cluster_id = %%?
		AND with_sys_table = %%?
		AND cmd = %%?
		ORDER BY id DESC
		LIMIT 1`

	// The following is a complete SQLs process to update configuration:
	// [1] INSERT INTO the task with status = 'running'
	// [2] waitIDs = $(SELECT id WHERE status = 'resetting')
	//
	// WAIT UNTIL any restore task with id of waitIDs is not in the status of 'resetting'
	// SET gc.ratio-threshold = -1.0
	// LOG RESTORE...
	//
	// [3] UPDATE status = 'resetting' WHERE this restore id
	// [4] anyID = $(SELECT id WHERE status != 'resetting' LIMIT 1)
	//
	// SET gc.ratio-threshold = 1.1 if no ID exists
	//
	// Case 1: There are 2 processes to update configuration
	// The process<1> is [1] [2] and the process<2> is [3] [4]
	// If commitTs[1] < commitTs[3], readTs[4] > commitTs[3] > commitTs[1] so [4] can get process<1>
	// If commitTs[1] > commitTs[3], readTs[2] > commitTs[1] > commitTs[3] so [2] can get process<2>
	//
	// Case 2: There are 2 process to reset configuration
	// The process<1> is [3] [4] and the process<2> is [3] [4]
	// If readTs<1>[4] < commitTs<2>[3] (<1>[4] can get process<2>{running} so that <1> won't reset),
	//   readTs<2>[4] > commitTs<2>[3] > readTs<1>[4] > commitTs<1>[3]
	// so <2>[4] can get process<1>{resetting} and reset.
	//

	// maxWaitRemainingResettingTasksCount is the retry count threshold to wait the resetting tasks finishing
	maxWaitRemainingResettingTasksTime = 75

	// selectResettingStatusTasksSQLTemplate is the SQL template for finding tasks with resetting status
	selectResettingStatusTasksSQLTemplate = `SELECT id FROM %s.%s WHERE status = 'resetting'`

	// selectLeftTasksSQLTemplate is the SQL template for finding the left tasks of the tasks whose IDs are given
	selectRemainingResettingTasksSQLTemplate = `SELECT id FROM %s.%s WHERE id in (%s) AND status = 'resetting'`

	// selectRunningTaskSQLTemplate is the SQL template for finding any running tasks
	selectAnyUnfinishedTaskSQLTemplate = `SELECT id FROM %s.%s WHERE status != 'resetting' LIMIT 1`

	// transitionStaleTaskToPausedSQLTemplate is the SQL template for atomically transitioning a
	// stale running task to paused
	transitionStaleTaskToPausedSQLTemplate = `
		UPDATE %s.%s
		SET status = 'paused'
		WHERE id = %%? AND status IN ('running', 'resetting') AND last_heartbeat_time = FROM_UNIXTIME(%%?)`

	// Old BR binaries do not understand routes. Routed rows expose a wildcard
	// filter to them so their conflict scan fails conservatively instead of
	// missing a target namespace occupied by a rename restore.
	legacyRoutedFilterStrings = "*.*"
)

var restoreRegistryRouteIndexColumns = []struct {
	name   string
	length int
}{
	{"filter_hash", types.UnspecifiedLength},
	{"route_hash", types.UnspecifiedLength},
	{"start_ts", types.UnspecifiedLength},
	{"restored_ts", types.UnspecifiedLength},
	{"upstream_cluster_id", types.UnspecifiedLength},
	{"with_sys_table", types.UnspecifiedLength},
	{"cmd", 256},
}

const (
	restoreRegistryLegacyIndexName = "unique_registration_params"
	restoreRegistryRouteIndexName  = "unique_registration_params_v2"
)

// TaskStatus represents the current state of a restore task
type TaskStatus string

const (
	// TaskStatusRunning indicates the task is currently active
	TaskStatusRunning TaskStatus = "running"
	// TaskStatusPaused indicates the task is temporarily stopped
	TaskStatusPaused TaskStatus = "paused"
	// TaskStatusResetting indicates the task is prepared to reset cluster configuration back before finishing
	TaskStatusResetting TaskStatus = "resetting"
)

// RegistrationInfo contains information about a registered restore
type RegistrationInfo struct {
	// filter patterns
	FilterStrings []string
	// RouteStrings contains canonical source-to-target schema/table routes.
	RouteStrings []string
	// RouteHash is the stable SHA-256 identity of RouteStrings.
	RouteHash string

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

	waitIDs []uint64

	tableExists bool
	// routeSchemaReady is false when a new BR binary connects to a TiDB that
	// predates restore name routing or is partway through its schema upgrade.
	// Identity restores keep using legacy SQL in either case.
	routeSchemaReady bool
}

func hasRestoreRegistryRouteSchema(tableInfo *model.TableInfo) bool {
	hasPublicColumn := func(name string) bool {
		for _, column := range tableInfo.Columns {
			if column != nil && column.Name.L == name && column.State == model.StatePublic {
				return true
			}
		}
		return false
	}
	for _, name := range []string{"source_filter_strings", "route_strings", "route_hash"} {
		if !hasPublicColumn(name) {
			return false
		}
	}

	// During v284 both indexes coexist briefly. The legacy index does not
	// include route_hash and would reject independent routes with the same
	// source filter, so keep rename disabled until it is fully removed.
	if tableInfo.FindIndexByName(restoreRegistryLegacyIndexName) != nil {
		return false
	}
	index := tableInfo.FindIndexByName(restoreRegistryRouteIndexName)
	if index == nil || !index.Unique || !index.IsPublic() || index.Primary || len(index.Columns) != len(restoreRegistryRouteIndexColumns) {
		return false
	}
	for i, expected := range restoreRegistryRouteIndexColumns {
		if index.Columns[i].Name.L != expected.name || index.Columns[i].Length != expected.length {
			return false
		}
	}
	return true
}

// NewRestoreRegistry creates a new registry using TiDB's session
func NewRestoreRegistry(ctx context.Context, g glue.Glue, dom *domain.Domain) (*Registry, error) {
	se, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}
	heartbeatSession, err := g.CreateSession(dom.Store())
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableExists := true
	tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr(RestoreRegistryDBName), ast.NewCIStr(RestoreRegistryTableName))
	if err != nil {
		if !infoschema.ErrTableNotExists.Equal(err) {
			return nil, errors.Trace(err)
		}
		tableExists = false
	}
	routeSchemaReady := tableExists && hasRestoreRegistryRouteSchema(tbl.Meta())

	return &Registry{
		se:               se,
		heartbeatSession: heartbeatSession,
		tableExists:      tableExists,
		routeSchemaReady: routeSchemaReady,
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

func normalizeRegistrationRoutes(info *RegistrationInfo) error {
	if len(info.RouteStrings) == 0 {
		if info.RouteHash != "" {
			return errors.Annotate(berrors.ErrInvalidArgument, "restore route hash is set without route rules")
		}
		return nil
	}
	router, err := nameroute.Parse(info.RouteStrings)
	if err != nil {
		return errors.Annotate(err, "invalid restore routes in registry identity")
	}
	info.RouteStrings = router.CanonicalRules()
	fingerprint := router.Fingerprint()
	expectedHash := hex.EncodeToString(fingerprint[:])
	if info.RouteHash != "" && info.RouteHash != expectedHash {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"restore route hash %q does not match route rules", info.RouteHash)
	}
	info.RouteHash = expectedHash
	return nil
}

func marshalRouteStrings(routes []string) (string, error) {
	if len(routes) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(routes)
	if err != nil {
		return "", errors.Annotate(err, "failed to encode restore routes")
	}
	return string(encoded), nil
}

func unmarshalRouteStrings(encoded string) ([]string, error) {
	if encoded == "" {
		return nil, nil
	}
	var routes []string
	if err := json.Unmarshal([]byte(encoded), &routes); err != nil {
		return nil, errors.Annotate(err, "failed to decode restore routes")
	}
	return routes, nil
}

func (r *Registry) checkRouteColumnCompatibility(info RegistrationInfo) error {
	if len(info.RouteStrings) > 0 && !r.routeSchemaReady {
		return errors.Annotate(berrors.ErrInvalidArgument,
			"restore rename requires a target TiDB whose mysql.tidb_restore_registry route schema is ready")
	}
	return nil
}

// ResumeOrCreateRegistration first looks for an existing registration with the given parameters.
// If found and paused, it tries to resume it. Otherwise, it creates a new registration.
// Returns: (taskID, resolvedRestoreTS, error)
func (r *Registry) ResumeOrCreateRegistration(ctx context.Context, info RegistrationInfo,
	isRestoredTSUserSpecified bool) (uint64, uint64, error) {
	if err := normalizeRegistrationRoutes(&info); err != nil {
		return 0, 0, err
	}
	if err := r.checkRouteColumnCompatibility(info); err != nil {
		return 0, 0, err
	}
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
	routeStrings, err := marshalRouteStrings(info.RouteStrings)
	if err != nil {
		return 0, 0, err
	}

	log.Info("attempting to resume or create registration",
		zap.String("filter_strings", filterStrings),
		zap.Strings("route_strings", info.RouteStrings),
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
		lookupTemplate := lookupRegistrationSQLTemplate
		lookupArgs := []any{filterStrings, info.RouteHash, info.StartTS, info.RestoredTS,
			info.UpstreamClusterID, info.WithSysTable, info.Cmd}
		if len(info.RouteStrings) == 0 {
			lookupTemplate = legacyLookupRegistrationSQLTemplate
			lookupArgs = []any{filterStrings, info.StartTS, info.RestoredTS,
				info.UpstreamClusterID, info.WithSysTable, info.Cmd}
		}
		lookupSQL := fmt.Sprintf(lookupTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
		rows, _, err := execCtx.ExecRestrictedSQL(ctx, sessionOpts, lookupSQL, lookupArgs...)
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

			// if task exists and is running or resetting, return error
			if status == string(TaskStatusRunning) || status == string(TaskStatusResetting) {
				log.Warn("task already exists and is running",
					zap.Uint64("restore_id", existingTaskID))
				return errors.Annotatef(berrors.ErrInvalidArgument,
					"task with ID %d already exists and is running", existingTaskID)
			}

			// strictly check for paused status
			if status == string(TaskStatusPaused) {
				currentTime := time.Now().UTC().Unix()
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
		currentTime := time.Now().UTC().Unix()
		insertTemplate := createNewTaskSQLTemplate
		insertArgs := []any{legacyRoutedFilterStrings, filterStrings, filterStrings, routeStrings, info.RouteHash,
			info.StartTS, info.RestoredTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd, currentTime, currentTime}
		if len(info.RouteStrings) == 0 {
			insertTemplate = legacyCreateNewTaskSQLTemplate
			insertArgs = []any{filterStrings, filterStrings, info.StartTS, info.RestoredTS,
				info.UpstreamClusterID, info.WithSysTable, info.Cmd, currentTime, currentTime}
		}
		insertSQL := fmt.Sprintf(insertTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
		_, _, err = execCtx.ExecRestrictedSQL(ctx, sessionOpts, insertSQL, insertArgs...)
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
		return 0, 0, errors.Trace(err)
	}

	if err := r.collectResettingStatusTasks(ctx); err != nil {
		return 0, 0, errors.Trace(err)
	}

	return taskID, resolvedRestoreTS, nil
}

func (r *Registry) collectResettingStatusTasks(ctx context.Context) error {
	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	// find the tasks with resetting status
	lookupSQL := fmt.Sprintf(selectResettingStatusTasksSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
	rows, _, err := execCtx.ExecRestrictedSQL(ctx, nil, lookupSQL)
	if err != nil {
		return errors.Annotate(err, "failed to look up tasks with resetting status")
	}
	var waitIDs []uint64
	if len(rows) > 0 {
		waitIDs = make([]uint64, 0, len(rows))
		for _, row := range rows {
			waitIDs = append(waitIDs, row.GetUint64(0))
		}
	}
	r.waitIDs = waitIDs
	return nil
}

// updateTaskStatusFromMultiple updates a task's status only if its current status matches one of the expected statuses
func (r *Registry) updateTaskStatusFromMultiple(ctx context.Context, restoreID uint64, currentStatuses []TaskStatus,
	newStatus TaskStatus) error {
	if len(currentStatuses) == 0 {
		return errors.New("currentStatuses cannot be empty")
	}

	// build the status list for the IN clause
	statusList := make([]string, len(currentStatuses))
	for i, status := range currentStatuses {
		statusList[i] = fmt.Sprintf("'%s'", string(status))
	}
	statusInClause := strings.Join(statusList, ", ")

	log.Info("attempting to update task status from multiple possible statuses",
		zap.Uint64("restore_id", restoreID),
		zap.Strings("current_statuses", func() []string {
			result := make([]string, len(currentStatuses))
			for i, s := range currentStatuses {
				result[i] = string(s)
			}
			return result
		}()),
		zap.String("new_status", string(newStatus)))

	// use where to update only when status is one of the expected values
	updateSQL := fmt.Sprintf(updateStatusFromMultipleSQLTemplate,
		RestoreRegistryDBName, RestoreRegistryTableName, statusInClause)

	if err := r.se.ExecuteInternal(ctx, updateSQL, newStatus, restoreID); err != nil {
		return errors.Annotatef(err, "failed to conditionally update task status from %v to %s",
			currentStatuses, newStatus)
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

// PauseTask marks a task as paused only if it's currently running or resetting
func (r *Registry) PauseTask(ctx context.Context, restoreID uint64) error {
	// first stop heartbeat manager
	r.StopHeartbeatManager()
	return r.updateTaskStatusFromMultiple(ctx, restoreID,
		[]TaskStatus{TaskStatusRunning, TaskStatusResetting}, TaskStatusPaused)
}

// GetRegistrationsByMaxID returns all registrations with IDs smaller than maxID
func (r *Registry) GetRegistrationsByMaxID(ctx context.Context, maxID uint64) ([]RegistrationInfoWithID, error) {
	selectTemplate := selectRegistrationsByMaxIDSQLTemplate
	if !r.routeSchemaReady {
		selectTemplate = legacySelectRegistrationsByMaxIDSQLTemplate
	}
	selectSQL := fmt.Sprintf(selectTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
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
		filterStrings := row.GetString(1)
		var routeStrings []string
		var routeHash string
		columnOffset := 0
		if r.routeSchemaReady {
			sourceFilterStrings := row.GetString(2)
			if sourceFilterStrings != "" {
				filterStrings = sourceFilterStrings
			}
			routeStrings, errSQL = unmarshalRouteStrings(row.GetString(3))
			if errSQL != nil {
				return nil, errors.Annotatef(errSQL, "invalid routes in restore registration %d", row.GetUint64(0))
			}
			routeHash = row.GetString(4)
			columnOffset = 3
		}
		startTS := row.GetUint64(2 + columnOffset)
		restoredTS := row.GetUint64(3 + columnOffset)
		upstreamClusterID := row.GetUint64(4 + columnOffset)
		withSysTable := row.GetInt64(5+columnOffset) != 0 // convert from int64 to bool
		cmd := row.GetString(7 + columnOffset)

		info := RegistrationInfo{
			FilterStrings:     strings.Split(filterStrings, FilterSeparator),
			RouteStrings:      routeStrings,
			RouteHash:         routeHash,
			StartTS:           startTS,
			RestoredTS:        restoredTS,
			UpstreamClusterID: upstreamClusterID,
			WithSysTable:      withSysTable,
			Cmd:               cmd,
		}
		if errSQL = normalizeRegistrationRoutes(&info); errSQL != nil {
			return nil, errors.Annotatef(errSQL, "invalid routes in restore registration %d", row.GetUint64(0))
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
	dbs []*metautil.Database,
	tables []*metautil.Table,
) error {
	return r.CheckTablesWithRegisteredTasksAndRoutes(ctx, restoreID, tracker, dbs, tables, nil)
}

// CheckTablesWithRegisteredTasksAndRoutes checks conflicts in the effective
// target namespace after applying the current and registered restore routes.
func (r *Registry) CheckTablesWithRegisteredTasksAndRoutes(
	ctx context.Context,
	restoreID uint64,
	tracker *utils.PiTRIdTracker,
	dbs []*metautil.Database,
	tables []*metautil.Table,
	currentRoutes []string,
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
		currentRouter, err := nameroute.Parse(currentRoutes)
		if err != nil {
			return errors.Annotate(err, "invalid current restore routes")
		}
		registeredRouter, err := nameroute.Parse(regInfo.RouteStrings)
		if err != nil {
			return errors.Annotatef(err, "invalid routes in restore registration %d", regInfo.restoreID)
		}

		// check if a table is already being restored
		if err := r.checkForTableConflicts(tracker, dbs, tables, regInfo, f,
			currentRouter, registeredRouter, restoreID); err != nil {
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
	dbs []*metautil.Database,
	tables []*metautil.Table,
	regInfo RegistrationInfoWithID,
	f filter.Filter,
	currentRouter *nameroute.Router,
	registeredRouter *nameroute.Router,
	curRestoreID uint64,
) error {
	// function to handle conflict when found
	handleTableConflict := func(dbName, tableName string) error {
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
	handleSchemaConflict := func(dbName string) error {
		log.Warn("schema already covered by another restore task",
			zap.Uint64("existing_restore_id", regInfo.restoreID),
			zap.Uint64("current_restore_id", curRestoreID),
			zap.String("database", dbName),
			zap.Strings("filter_strings", regInfo.FilterStrings),
		)
		return errors.Annotatef(berrors.ErrDatabasesAlreadyExisted,
			"database %s cannot be restored concurrently by current task with ID %d "+
				"because it is already being restored by task (restoreId: %d, time range: %d->%d, cmd: %s)",
			dbName, curRestoreID, regInfo.restoreID, regInfo.StartTS, regInfo.RestoredTS, regInfo.Cmd)
	}

	// Use PiTRTableTracker if available for PiTR task
	if tracker != nil && len(tracker.GetDBNameToTableName()) > 0 {
		checkedTargetSchemas := make(map[string]struct{})
		for dbName, tableNames := range tracker.GetDBNameToTableName() {
			// A log-only PiTR can restore a database that has no selected tables.
			// In that case the DBInfo metadata still owns the target schema.
			if len(tableNames) == 0 {
				targetDB, _, _ := currentRouter.Route(ast.NewCIStr(dbName), ast.CIStr{})
				if registrationClaimsTarget(registeredRouter, f, targetDB.O, "", regInfo.WithSysTable) {
					return handleSchemaConflict(targetDB.O)
				}
				checkedTargetSchemas[targetDB.L] = struct{}{}
			}
			for tableName := range tableNames {
				targetDB, targetTable, _ := currentRouter.Route(ast.NewCIStr(dbName), ast.NewCIStr(tableName))
				if _, checked := checkedTargetSchemas[targetDB.L]; !checked {
					if registrationClaimsTarget(registeredRouter, f, targetDB.O, "", regInfo.WithSysTable) {
						return handleSchemaConflict(targetDB.O)
					}
					checkedTargetSchemas[targetDB.L] = struct{}{}
				}
				if registrationClaimsTarget(registeredRouter, f, targetDB.O, targetTable.O, regInfo.WithSysTable) {
					return handleTableConflict(targetDB.O, targetTable.O)
				}
			}
		}
	} else {
		// for existing point restore task, we need to check database conflicts with snapshot restore.
		if regInfo.Cmd == "Point Restore" {
			for _, db := range dbs {
				targetDB, _, _ := currentRouter.Route(db.Info.Name, ast.CIStr{})
				if registrationClaimsTarget(registeredRouter, f, targetDB.O, "", regInfo.WithSysTable) {
					return handleSchemaConflict(targetDB.O)
				}
			}
		}
		// use tables as this is a snapshot restore task
		for _, table := range tables {
			targetDB, targetTable, _ := currentRouter.Route(table.DB.Name, table.Info.Name)
			if registrationClaimsTarget(registeredRouter, f, targetDB.O, targetTable.O, regInfo.WithSysTable) {
				return handleTableConflict(targetDB.O, targetTable.O)
			}
		}
	}

	return nil
}

func registrationClaimsTarget(
	router *nameroute.Router,
	f filter.Filter,
	targetDB string,
	targetTable string,
	withSysTable bool,
) bool {
	targetDBName := ast.NewCIStr(targetDB)
	targetTableName := ast.NewCIStr(targetTable)
	targetMatches := func(sourceDB, sourceTable ast.CIStr) bool {
		routedDB, routedTable, _ := router.Route(sourceDB, sourceTable)
		return routedDB.L == targetDBName.L && routedTable.L == targetTableName.L
	}

	for _, rule := range router.Rules() {
		if rule.Source.IsTable() {
			if rule.Target.Schema.L == targetDBName.L &&
				(targetTable == "" || rule.Target.Table.L == targetTableName.L) &&
				utils.MatchTable(f, rule.Source.Schema.O, rule.Source.Table.O, withSysTable) {
				return true
			}
			continue
		}
		if targetTable == "" {
			if rule.Target.Schema.L == targetDBName.L &&
				utils.MatchSchema(f, rule.Source.Schema.O, withSysTable) {
				return true
			}
			continue
		}
		if targetMatches(rule.Source.Schema, targetTableName) &&
			utils.MatchTable(f, rule.Source.Schema.O, targetTable, withSysTable) {
			return true
		}
	}

	if !targetMatches(targetDBName, targetTableName) {
		return false
	}
	if targetTable == "" {
		return utils.MatchSchema(f, targetDB, withSysTable)
	}
	return utils.MatchTable(f, targetDB, targetTable, withSysTable)
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
func (r *Registry) resolveRestoreTS(
	ctx context.Context,
	info RegistrationInfo,
	isRestoredTSUserSpecified bool,
) (uint64, error) {
	filterStrings := strings.Join(info.FilterStrings, FilterSeparator)

	// look for tasks with same filter, startTS, cluster, sysTable, cmd
	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)

	checkTemplate := selectConflictingTaskSQLTemplate
	checkArgs := []any{filterStrings, info.RouteHash, info.StartTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd}
	if len(info.RouteStrings) == 0 {
		checkTemplate = legacySelectConflictingTaskSQLTemplate
		checkArgs = []any{filterStrings, info.StartTS, info.UpstreamClusterID, info.WithSysTable, info.Cmd}
	}
	checkSQL := fmt.Sprintf(checkTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
	rows, _, err := execCtx.ExecRestrictedSQL(ctx, nil, checkSQL, checkArgs...)
	if err != nil {
		return 0, errors.Annotate(err, "failed to check for existing tasks with same parameters")
	}

	// no conflicting task found, use the current restoredTS
	if len(rows) == 0 {
		log.Info("no existing tasks found with same parameters",
			zap.Uint64("restored_ts", info.RestoredTS))
		return info.RestoredTS, nil
	}

	conflictingTaskID := rows[0].GetUint64(0)
	existingRestoredTS := rows[0].GetUint64(1)
	existingStatus := rows[0].GetString(2)
	initialHeartbeatTimestamp := rows[0].GetInt64(3)

	log.Info("found existing task with same parameters",
		zap.Uint64("existing_task_id", conflictingTaskID),
		zap.Uint64("existing_restored_ts", existingRestoredTS),
		zap.String("existing_status", existingStatus),
		zap.Uint64("current_restored_ts", info.RestoredTS),
		zap.Strings("filters", info.FilterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.String("last heartbeat time", time.Unix(initialHeartbeatTimestamp, 0).String()),
	)

	// if restoredTS values are different and user explicitly specified it, use current restoredTS
	if isRestoredTSUserSpecified && existingRestoredTS != info.RestoredTS {
		log.Error("existing task has different restoredTS from user-specified",
			zap.Uint64("existing_restored_ts", existingRestoredTS),
			zap.Uint64("user_specified_restored_ts", info.RestoredTS))
		return 0, errors.Annotatef(berrors.ErrInvalidArgument,
			"existing task has different restoredTS(%d) from user-specified(%d)", existingRestoredTS, info.RestoredTS)
	}

	// if existing task is paused, reuse its restoredTS
	if existingStatus == string(TaskStatusPaused) {
		log.Info("existing task is paused, reusing its restoredTS",
			zap.Uint64("existing_task_id", conflictingTaskID),
			zap.Uint64("existing_restored_ts", existingRestoredTS))
		return existingRestoredTS, nil
	}

	// if existing task is running, check if it's stale
	if existingStatus == string(TaskStatusRunning) || existingStatus == string(TaskStatusResetting) {
		log.Info("existing task is running, checking if it's stale",
			zap.Uint64("existing_task_id", conflictingTaskID))

		isStale, err := r.isTaskStale(ctx, conflictingTaskID, initialHeartbeatTimestamp)
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
			transitioned, transitionErr := r.transitionStaleTaskToPaused(ctx, conflictingTaskID, initialHeartbeatTimestamp)
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
func (r *Registry) isTaskStale(ctx context.Context, taskID uint64, initialHeartbeatTimestamp int64) (bool, error) {
	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	log.Info("checking if task is stale, will check heartbeat every minute up to 5 minutes",
		zap.Uint64("task_id", taskID),
		zap.String("initial_heartbeat", time.Unix(initialHeartbeatTimestamp, 0).String()))

	// check heartbeat every minute for up to 5 minutes
	ticker := time.NewTicker(time.Minute)
	failpoint.Inject("is-task-stale-ticker-duration", func(val failpoint.Value) {
		ticker.Stop()
		secs := val.(int)
		ticker = time.NewTicker(time.Second * time.Duration(secs))
	})
	defer ticker.Stop()

	selectHeartbeatSQL := fmt.Sprintf(selectTaskHeartbeatSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
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
			currentHeartbeatTimestamp := currentRows[0].GetInt64(0)

			// if heartbeat changed, task is active - exit early
			if currentHeartbeatTimestamp != initialHeartbeatTimestamp {
				log.Info("task heartbeat updated, task is active",
					zap.Uint64("task_id", taskID),
					zap.String("initial_heartbeat", time.Unix(initialHeartbeatTimestamp, 0).String()),
					zap.String("current_heartbeat", time.Unix(currentHeartbeatTimestamp, 0).String()),
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
		zap.String("initial_heartbeat", time.Unix(initialHeartbeatTimestamp, 0).String()))

	return true, nil
}

// transitionStaleTaskToPaused atomically transitions a stale running task to paused state
// if the heartbeat timestamp hasn't changed. Returns whether the transition was successful.
func (r *Registry) transitionStaleTaskToPaused(ctx context.Context, taskID uint64,
	expectedHeartbeatTimestamp int64) (bool, error) {
	log.Info("attempting to transition stale task to paused state",
		zap.Uint64("task_id", taskID),
		zap.String("expected_heartbeat", time.Unix(expectedHeartbeatTimestamp, 0).String()))

	var transitioned bool
	err := r.executeInTransaction(ctx, func(ctx context.Context, execCtx sqlexec.RestrictedSQLExecutor,
		sessionOpts []sqlexec.OptionFuncAlias) error {
		// atomically update task to paused only if it's still running with the same heartbeat time
		updateSQL := fmt.Sprintf(transitionStaleTaskToPausedSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
		_, _, updateErr := execCtx.ExecRestrictedSQL(ctx, sessionOpts, updateSQL, taskID, expectedHeartbeatTimestamp)
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

// OperationAfterWaitIDs do the specified operations until the resetting tasks is removed
func (r *Registry) OperationAfterWaitIDs(ctx context.Context, fn func() error) error {
	if !r.tableExists {
		return fn()
	}
	retryCount := 0
	for ids := range slices.Chunk(r.waitIDs, 10) {
		idStrs := make([]string, 0, len(ids))
		for _, id := range ids {
			idStrs = append(idStrs, fmt.Sprintf("%d", id))
		}
		idsStr := strings.Join(idStrs, ",")
		lookupSQL := fmt.Sprintf(selectRemainingResettingTasksSQLTemplate,
			RestoreRegistryDBName, RestoreRegistryTableName, idsStr)
		for {
			rows, _, err := r.se.GetSessionCtx().GetRestrictedSQLExecutor().ExecRestrictedSQL(
				kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
				nil,
				lookupSQL,
			)
			if err != nil {
				return errors.Trace(err)
			}
			if len(rows) == 0 {
				break
			}
			leftId := rows[0].GetUint64(0)
			retryCount += 1
			if retryCount > maxWaitRemainingResettingTasksTime {
				log.Warn("failed to wait for the task finishing resetting, timeout")
				return fn()
			}
			log.Info("wait for the task finishing resetting", zap.Uint64("task id", leftId), zap.Int("retry count", retryCount))
			time.Sleep(5 * time.Second)
		}
	}
	return fn()
}

// GlobalOperationAfterSetResettingStatus do the global operation if there is no running task and set resetting
// status for the task
func (r *Registry) GlobalOperationAfterSetResettingStatus(
	ctx context.Context, restoreID uint64, fn func() error,
) error {
	if !r.tableExists {
		return fn()
	}
	updateSQL := fmt.Sprintf(updateStatusSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
	if err := r.se.ExecuteInternal(ctx, updateSQL, TaskStatusResetting, restoreID, TaskStatusRunning); err != nil {
		return errors.Annotatef(err, "failed to conditionally update task status from %s to %s",
			TaskStatusRunning, TaskStatusResetting)
	}

	execCtx := r.se.GetSessionCtx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	lookupSQL := fmt.Sprintf(selectAnyUnfinishedTaskSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
	rows, _, err := execCtx.ExecRestrictedSQL(ctx, nil, lookupSQL)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		log.Info("there is no task running, so execute the global operation")
		return fn()
	}
	return nil
}

// FindAndDeleteMatchingTask finds and deletes the registry entry that matches the given restore configuration
// This is used for the abort functionality to clean up the matching task
// Similar to ResumeOrCreateRegistration, it first resolves the restoredTS then finds and deletes the matching
// paused task
// Returns the deleted task ID, or 0 if no matching task was found
func (r *Registry) FindAndDeleteMatchingTask(ctx context.Context,
	info RegistrationInfo, isRestoredTSUserSpecified bool) (uint64, error) {
	if err := normalizeRegistrationRoutes(&info); err != nil {
		return 0, err
	}
	if err := r.checkRouteColumnCompatibility(info); err != nil {
		return 0, err
	}
	// resolve which restoredTS to use
	resolvedRestoreTS, err := r.resolveRestoreTS(ctx, info, isRestoredTSUserSpecified)
	if err != nil {
		return 0, err
	}

	// update info with resolved restoredTS if different
	if resolvedRestoreTS != info.RestoredTS {
		log.Info("using resolved restoredTS for abort operation",
			zap.Uint64("original_restored_ts", info.RestoredTS),
			zap.Uint64("resolved_restored_ts", resolvedRestoreTS))
		info.RestoredTS = resolvedRestoreTS
	}

	filterStrings := strings.Join(info.FilterStrings, FilterSeparator)

	log.Info("searching for matching task to delete",
		zap.String("filter_strings", filterStrings),
		zap.Uint64("start_ts", info.StartTS),
		zap.Uint64("restored_ts", info.RestoredTS),
		zap.Uint64("upstream_cluster_id", info.UpstreamClusterID),
		zap.Bool("with_sys_table", info.WithSysTable),
		zap.String("cmd", info.Cmd))

	var deletedTaskID uint64

	err = r.executeInTransaction(ctx, func(ctx context.Context, execCtx sqlexec.RestrictedSQLExecutor,
		sessionOpts []sqlexec.OptionFuncAlias) error {
		// find and lock the task that matches the configuration
		lookupTemplate := lookupRegistrationSQLTemplate
		lookupArgs := []any{filterStrings, info.RouteHash, info.StartTS, info.RestoredTS,
			info.UpstreamClusterID, info.WithSysTable, info.Cmd}
		if len(info.RouteStrings) == 0 {
			lookupTemplate = legacyLookupRegistrationSQLTemplate
			lookupArgs = []any{filterStrings, info.StartTS, info.RestoredTS,
				info.UpstreamClusterID, info.WithSysTable, info.Cmd}
		}
		lookupSQL := fmt.Sprintf(lookupTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
		rows, _, err := execCtx.ExecRestrictedSQL(ctx, sessionOpts, lookupSQL, lookupArgs...)
		if err != nil {
			return errors.Annotate(err, "failed to lookup matching task")
		}

		if len(rows) == 0 {
			log.Info("no matching task found to delete")
			return nil
		}

		if len(rows) > 1 {
			log.Error("multiple matching tasks found, this is unexpected and indicates a bug",
				zap.Int("count", len(rows)))
			return errors.Annotatef(berrors.ErrInvalidArgument,
				"found %d matching tasks, expected exactly 1", len(rows))
		}

		// get the single matching task (now locked)
		taskID := rows[0].GetUint64(0)
		status := rows[0].GetString(1)

		log.Info("found and locked matching task",
			zap.Uint64("task_id", taskID),
			zap.String("status", status))

		// handle different task statuses
		if status == string(TaskStatusPaused) {
			// paused tasks can be directly deleted
		} else if status == string(TaskStatusRunning) || status == string(TaskStatusResetting) {
			// for running/resetting tasks, check if they are stale (dead processes)
			log.Info("task is running/resetting, checking if it's stale before abort",
				zap.Uint64("task_id", taskID),
				zap.String("status", status))

			// get the task's heartbeat time to check if it's stale
			heartbeatSQL := fmt.Sprintf(selectTaskHeartbeatSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
			heartbeatRows, _, heartbeatErr := execCtx.ExecRestrictedSQL(ctx, sessionOpts, heartbeatSQL, taskID)
			if heartbeatErr != nil {
				log.Warn("failed to check task heartbeat during abort, skipping",
					zap.Uint64("task_id", taskID),
					zap.Error(heartbeatErr))
				return nil
			}

			if len(heartbeatRows) == 0 {
				log.Warn("task not found when checking heartbeat, skipping abort",
					zap.Uint64("task_id", taskID))
				return nil
			}

			initialHeartbeatTimestamp := heartbeatRows[0].GetInt64(0)

			// check if the task is stale (not updating heartbeat)
			isStale, staleErr := r.isTaskStale(ctx, taskID, initialHeartbeatTimestamp)
			if staleErr != nil {
				log.Warn("failed to determine if task is stale, skipping abort",
					zap.Uint64("task_id", taskID),
					zap.Error(staleErr))
				return nil
			}

			if !isStale {
				log.Info("task is actively running, cannot abort",
					zap.Uint64("task_id", taskID),
					zap.String("status", status))
				return nil
			}

			log.Info("task is stale, proceeding with abort",
				zap.Uint64("task_id", taskID),
				zap.String("status", status))
		} else {
			log.Error("task is in unexpected status, cannot abort",
				zap.Uint64("task_id", taskID),
				zap.String("status", status))
			return nil
		}

		// delete the paused task
		deleteSQL := fmt.Sprintf(deleteRegistrationSQLTemplate, RestoreRegistryDBName, RestoreRegistryTableName)
		_, _, err = execCtx.ExecRestrictedSQL(ctx, sessionOpts, deleteSQL, taskID)
		if err != nil {
			return errors.Annotatef(err, "failed to delete task %d", taskID)
		}

		deletedTaskID = taskID
		log.Info("successfully deleted matching paused task", zap.Uint64("task_id", taskID))

		return nil
	})

	if err != nil {
		return 0, err
	}

	if deletedTaskID != 0 {
		log.Info("successfully deleted matching task", zap.Uint64("task_id", deletedTaskID))
	}

	return deletedTaskID, nil
}
