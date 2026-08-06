// Copyright 2024 PingCAP, Inc.
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

package storage

import (
	"context"
	goerrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	// DefaultHistoryTaskPageSize is the default page size for history task listing.
	DefaultHistoryTaskPageSize = 20
	// MinHistoryTaskPageSize is the minimum page size for history task listing.
	MinHistoryTaskPageSize = 1
	// MaxHistoryTaskPageSize is the maximum page size for history task listing.
	MaxHistoryTaskPageSize     = 200
	historyTaskSummaryColumns  = basicTaskColumns + `, t.error, t.start_time, t.state_update_time, t.end_time`
	taskErrorCategoryCancelled = "cancelled"
	taskErrorCategoryDataError = "data-error"
)

// TransferSubtasks2HistoryWithSession transfer the selected subtasks into tidb_background_subtask_history table by taskID.
func (*TaskManager) TransferSubtasks2HistoryWithSession(ctx context.Context, se sessionctx.Context, taskID int64) error {
	exec := se.GetSQLExecutor()
	_, err := sqlexec.ExecSQL(ctx, exec, `insert into mysql.tidb_background_subtask_history select * from mysql.tidb_background_subtask where task_key = %?`, TaskIDToKey(taskID))
	if err != nil {
		return err
	}
	// delete taskID subtask
	_, err = sqlexec.ExecSQL(ctx, exec, "delete from mysql.tidb_background_subtask where task_key = %?", TaskIDToKey(taskID))
	return err
}

// TransferTasks2History transfer the selected tasks into tidb_global_task_history table by taskIDs.
func (mgr *TaskManager) TransferTasks2History(ctx context.Context, tasks []*proto.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	taskIDStrs := make([]string, 0, len(tasks))
	taskKeyStrs := make([]string, 0, len(tasks))
	updateMetaArgs := make([]any, 0, len(tasks)*2)
	var updateMetaSQL strings.Builder
	updateMetaSQL.WriteString(`
		update mysql.tidb_global_task
		set meta = case id`)
	for _, task := range tasks {
		taskIDStrs = append(taskIDStrs, fmt.Sprintf("%d", task.ID))
		taskKeyStrs = append(taskKeyStrs, fmt.Sprintf("'%d'", task.ID))
		updateMetaSQL.WriteString(" when %? then %?")
		updateMetaArgs = append(updateMetaArgs, task.ID, task.Meta)
	}
	taskIDList := strings.Join(taskIDStrs, `, `)
	taskKeyList := strings.Join(taskKeyStrs, `, `)
	updateMetaSQL.WriteString(`
			end, state_update_time = CURRENT_TIMESTAMP()
		where id in(` + taskIDList + `)`)
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		// sensitive data in meta might be redacted, need update first.
		exec := se.GetSQLExecutor()
		_, err := sqlexec.ExecSQL(ctx, exec, updateMetaSQL.String(), updateMetaArgs...)
		if err != nil {
			return err
		}
		_, err = sqlexec.ExecSQL(ctx, exec, `
			insert into mysql.tidb_global_task_history
			select * from mysql.tidb_global_task
			where id in(`+taskIDList+`)`)
		if err != nil {
			return err
		}

		_, err = sqlexec.ExecSQL(ctx, exec, `
			delete from mysql.tidb_global_task
			where id in(`+taskIDList+`)`)
		if err != nil {
			return err
		}

		_, err = sqlexec.ExecSQL(ctx, exec, `
			insert into mysql.tidb_background_subtask_history
			select * from mysql.tidb_background_subtask
			where task_key in(`+taskKeyList+`)`)
		if err != nil {
			return err
		}

		_, err = sqlexec.ExecSQL(ctx, exec, `
			delete from mysql.tidb_background_subtask
			where task_key in(`+taskKeyList+`)`)
		return err
	})
}

// HistoryTaskSummary contains summary fields for one history task.
type HistoryTaskSummary struct {
	*proto.TaskBase
	ErrorCode       string
	ErrorCategory   string
	StartTime       time.Time
	StateUpdateTime time.Time
	EndTime         time.Time
}

// HistoryTaskPage is the paged result for history task listing.
type HistoryTaskPage struct {
	Items            []*HistoryTaskSummary
	HasMore          bool
	NextPageToken    int64
	ApproxTotalCount int64
}

// ValidateHistoryTaskPageSize validates page size for history task listing.
func ValidateHistoryTaskPageSize(pageSize int) error {
	if pageSize < MinHistoryTaskPageSize || pageSize > MaxHistoryTaskPageSize {
		return fmt.Errorf("page size should be within [%d, %d]", MinHistoryTaskPageSize, MaxHistoryTaskPageSize)
	}
	return nil
}

// ListHistoryTasks lists history tasks with keyset pagination and optional keyspace filter.
func (mgr *TaskManager) ListHistoryTasks(ctx context.Context, pageSize int, pageToken int64, keyspace string) (*HistoryTaskPage, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
	if err := ValidateHistoryTaskPageSize(pageSize); err != nil {
		return nil, err
	}

	whereParts := make([]string, 0, 2)
	countArgs := make([]any, 0, 1)
	if keyspace != "" {
		whereParts = append(whereParts, "t.keyspace = %?")
		countArgs = append(countArgs, keyspace)
	}
	dataArgs := append(make([]any, 0, len(countArgs)+2), countArgs...)
	if pageToken > 0 {
		whereParts = append(whereParts, "t.id < %?")
		dataArgs = append(dataArgs, pageToken)
	}
	whereSQL := ""
	if len(whereParts) > 0 {
		whereSQL = " where " + strings.Join(whereParts, " and ")
	}
	dataArgs = append(dataArgs, pageSize+1)

	rows, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select `+historyTaskSummaryColumns+` from mysql.tidb_global_task_history t`+whereSQL+` order by t.id desc limit %?`,
		dataArgs...)
	if err != nil {
		return nil, err
	}
	// Intentionally keep this as a separate best-effort read.
	// Under concurrent task transfers, Items and ApproxTotalCount may observe slightly
	// different snapshots, which is acceptable for this observability API.
	// Pagination correctness relies on page rows + HasMore/NextPageToken from the page query.
	countRows, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select count(1) from mysql.tidb_global_task_history`+func() string {
			if keyspace == "" {
				return ""
			}
			return " where keyspace = %?"
		}(),
		countArgs...)
	if err != nil {
		return nil, err
	}

	page := &HistoryTaskPage{
		Items:            make([]*HistoryTaskSummary, 0, min(len(rows), pageSize)),
		ApproxTotalCount: countRows[0].GetInt64(0),
	}
	hasMore := len(rows) > pageSize
	page.HasMore = hasMore
	if hasMore {
		rows = rows[:pageSize]
	}
	for _, row := range rows {
		page.Items = append(page.Items, row2HistoryTaskSummary(row))
	}
	if hasMore {
		page.NextPageToken = page.Items[len(page.Items)-1].ID
	}
	return page, nil
}

func row2HistoryTaskSummary(r chunk.Row) *HistoryTaskSummary {
	item := &HistoryTaskSummary{
		TaskBase: row2TaskBasic(r),
	}
	if taskErr := row2TaskError(r, 12); taskErr != nil {
		item.ErrorCode = taskErrorCode(taskErr)
		item.ErrorCategory = ClassifyTaskError(item.State, taskErr)
	}
	if !r.IsNull(13) {
		item.StartTime, _ = r.GetTime(13).GoTime(time.Local)
	}
	if !r.IsNull(14) {
		item.StateUpdateTime, _ = r.GetTime(14).GoTime(time.Local)
	}
	if !r.IsNull(15) {
		item.EndTime, _ = r.GetTime(15).GoTime(time.Local)
	}
	return item
}

// ClassifyTaskError returns a coarse category for a terminal task error.
// The category is exposed by the history API and must never contain the raw error text.
func ClassifyTaskError(state proto.TaskState, taskErr error) string {
	if taskErr == nil {
		return ""
	}
	switch state {
	case proto.TaskStateFailed:
		return proto.TaskStateFailed.String()
	case proto.TaskStateReverted:
		if IsCancelledErr(taskErr) {
			return taskErrorCategoryCancelled
		}
		if isDataError(taskErr) {
			return taskErrorCategoryDataError
		}
		return proto.TaskStateFailed.String()
	default:
		return ""
	}
}

func isDataError(taskErr error) bool {
	if taskErr == nil {
		return false
	}
	errMsg := taskErr.Error()
	// Keep these checks string-based to avoid depending on Lightning error definitions.
	// We can replace this when those error definitions are split out of the Lightning
	// package. DXF error serialization keeps only the outer error code, so nested data
	// errors must be identified by their canonical messages.
	//
	// import-into examples:
	// [Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk:
	// encode kv error in file orderlab/orderlab.shipment_events.000000000.csv.gz:0
	// at offset 0: Value conversion failed for column 'event_id'. Expected type:
	// bigint, received value: ?. Reason: [types:1292]Truncated incorrect DOUBLE value: '?'.
	//
	// add-index examples:
	// [kv:1062]Duplicate entry '1' for key 't.idx'
	isImportDataErr := strings.Contains(errMsg, "ErrEncodeKV") &&
		(strings.Contains(errMsg, "Value conversion failed for column") ||
			(strings.Contains(errMsg, "Check constraint '") && strings.Contains(errMsg, "' is violated")) ||
			strings.Contains(errMsg, "Table has no partition for value"))
	isImportConflictErr := (strings.Contains(errMsg, "[executor:8167]") && strings.Contains(errMsg, "Duplicate key conflict found")) ||
		(strings.Contains(errMsg, "ErrFoundDataConflictRecords") && strings.Contains(errMsg, "found data conflict records")) ||
		(strings.Contains(errMsg, "ErrFoundIndexConflictRecords") && strings.Contains(errMsg, "found index conflict records"))
	isUKDupEntryErr := strings.Contains(errMsg, "[kv:1062]") && strings.Contains(errMsg, "Duplicate entry")
	return isImportDataErr || isImportConflictErr || isUKDupEntryErr
}

func taskErrorCode(taskErr error) string {
	var normalizedErr *errors.Error
	if !goerrors.As(taskErr, &normalizedErr) {
		return ""
	}
	code := string(normalizedErr.RFCCode())
	// errors.Normalize without RFCCodeText or MySQLErrorCode leaves a zero-value
	// numeric code, which RFCCode returns as "0".
	if code == "0" {
		return ""
	}
	return code
}

// GCSubtasks deletes the history subtask which is older than the given days.
func (mgr *TaskManager) GCSubtasks(ctx context.Context) error {
	subtaskHistoryKeepSeconds := defaultSubtaskKeepDays * 24 * 60 * 60
	failpoint.InjectCall("subtaskHistoryKeepSeconds", &subtaskHistoryKeepSeconds)
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	_, err := mgr.ExecuteSQLWithNewSession(
		ctx,
		fmt.Sprintf("DELETE FROM mysql.tidb_background_subtask_history WHERE state_update_time < UNIX_TIMESTAMP() - %d ;", subtaskHistoryKeepSeconds),
	)
	return err
}
