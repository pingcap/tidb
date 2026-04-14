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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
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
	MaxHistoryTaskPageSize    = 200
	historyTaskSummaryColumns = basicTaskColumns + `, t.start_time, t.state_update_time, t.end_time`
)

// TransferSubtasks2HistoryWithSession transfer the selected subtasks into tidb_background_subtask_history table by taskID.
func (*TaskManager) TransferSubtasks2HistoryWithSession(ctx context.Context, se sessionctx.Context, taskID int64) error {
	exec := se.GetSQLExecutor()
	_, err := sqlexec.ExecSQL(ctx, exec, `insert into mysql.tidb_background_subtask_history select * from mysql.tidb_background_subtask where task_key = %?`, taskID)
	if err != nil {
		return err
	}
	// delete taskID subtask
	_, err = sqlexec.ExecSQL(ctx, exec, "delete from mysql.tidb_background_subtask where task_key = %?", taskID)
	return err
}

// TransferTasks2History transfer the selected tasks into tidb_global_task_history table by taskIDs.
func (mgr *TaskManager) TransferTasks2History(ctx context.Context, tasks []*proto.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	taskIDStrs := make([]string, 0, len(tasks))
	for _, task := range tasks {
		taskIDStrs = append(taskIDStrs, fmt.Sprintf("%d", task.ID))
	}
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		// sensitive data in meta might be redacted, need update first.
		exec := se.GetSQLExecutor()
		for _, t := range tasks {
			_, err := sqlexec.ExecSQL(ctx, exec, `
				update mysql.tidb_global_task
				set meta= %?, state_update_time = CURRENT_TIMESTAMP()
				where id = %?`, t.Meta, t.ID)
			if err != nil {
				return err
			}
		}
		_, err := sqlexec.ExecSQL(ctx, exec, `
			insert into mysql.tidb_global_task_history
			select * from mysql.tidb_global_task
			where id in(`+strings.Join(taskIDStrs, `, `)+`)`)
		if err != nil {
			return err
		}

		_, err = sqlexec.ExecSQL(ctx, exec, `
			delete from mysql.tidb_global_task
			where id in(`+strings.Join(taskIDStrs, `, `)+`)`)

		for _, t := range tasks {
			err = mgr.TransferSubtasks2HistoryWithSession(ctx, se, t.ID)
			if err != nil {
				return err
			}
		}
		return err
	})
}

// HistoryTaskSummary contains summary fields for one history task.
type HistoryTaskSummary struct {
	*proto.TaskBase
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
	if !r.IsNull(12) {
		item.StartTime, _ = r.GetTime(12).GoTime(time.Local)
	}
	if !r.IsNull(13) {
		item.StateUpdateTime, _ = r.GetTime(13).GoTime(time.Local)
	}
	if !r.IsNull(14) {
		item.EndTime, _ = r.GetTime(14).GoTime(time.Local)
	}
	return item
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
