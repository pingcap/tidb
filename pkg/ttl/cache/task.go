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

package cache

import (
	"encoding/json"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
)

const selectFromTTLTask = `SELECT LOW_PRIORITY
	job_id,
	job_type,
	table_id,
	scan_id,
	scan_range_start,
	scan_range_end,
	expire_time,
	owner_id,
	owner_addr,
	owner_hb_time,
	status,
	status_update_time,
	state,
	created_time FROM mysql.tidb_ttl_task`
const insertIntoTTLTask = `INSERT LOW_PRIORITY INTO mysql.tidb_ttl_task SET
	job_id = %?,
	job_type = %?,
	table_id = %?,
	scan_id = %?,
	scan_range_start = %?,
	scan_range_end = %?,
	expire_time = %?,
	created_time = %?`

// SelectFromTTLTaskWithJobID returns an SQL statement to get all tasks of the specified job in mysql.tidb_ttl_task
func SelectFromTTLTaskWithJobID(jobID string) (string, []any) {
	return selectFromTTLTask + " WHERE job_id = %?", []any{jobID}
}

// SelectFromTTLTaskWithID returns an SQL statement to get all tasks of the specified job
// and scanID in mysql.tidb_ttl_task
func SelectFromTTLTaskWithID(jobID string, scanID int64) (string, []any) {
	return selectFromTTLTask + " WHERE job_id = %? AND scan_id = %?", []any{jobID, scanID}
}

// PeekWaitingTTLTask returns an SQL statement to get `limit` waiting ttl tasks.
func PeekWaitingTTLTask(hbExpire time.Time) (string, []any) {
	return selectFromTTLTask +
			" WHERE status = 'waiting' OR (owner_hb_time < %? AND status = 'running') ORDER BY created_time ASC",
		[]any{hbExpire.Format(time.DateTime)}
}

// InsertIntoTTLTask returns an SQL statement to insert a ttl task into mysql.tidb_ttl_task
func InsertIntoTTLTask(sctx sessionctx.Context, jobID string, jobType session.TTLJobType, tableID int64, scanID int,
	scanRangeStart []types.Datum, scanRangeEnd []types.Datum,
	expireTime time.Time, createdTime time.Time) (string, []any, error) {
	rangeStart, err := codec.EncodeKey(sctx.GetSessionVars().StmtCtx.TimeZone(), []byte{}, scanRangeStart...)
	if err != nil {
		return "", nil, err
	}
	rangeEnd, err := codec.EncodeKey(sctx.GetSessionVars().StmtCtx.TimeZone(), []byte{}, scanRangeEnd...)
	if err != nil {
		return "", nil, err
	}
	return insertIntoTTLTask, []any{jobID, jobType, tableID, int64(scanID),
		rangeStart, rangeEnd, expireTime, createdTime}, nil
}

// TaskStatus represents the current status of a task
type TaskStatus string

const (
	// TaskStatusWaiting means the task hasn't started
	TaskStatusWaiting TaskStatus = "waiting"
	// TaskStatusRunning means this task is running
	TaskStatusRunning TaskStatus = "running"
	// TaskStatusFinished means this task has finished
	TaskStatusFinished TaskStatus = "finished"
)

// TTLTask is a row recorded in mysql.tidb_ttl_task
type TTLTask struct {
	JobID   string
	JobType session.TTLJobType // the type of this job: ttl or softdelete

	TableID          int64
	ScanID           int64
	ScanRangeStart   []types.Datum
	ScanRangeEnd     []types.Datum
	ExpireTime       time.Time
	OwnerID          string
	OwnerAddr        string
	OwnerHBTime      time.Time
	Status           TaskStatus
	StatusUpdateTime time.Time
	State            *TTLTaskState
	CreatedTime      time.Time
}

// TTLTaskState records the internal states of the ttl task
type TTLTaskState struct {
	TotalRows   uint64 `json:"total_rows"`
	SuccessRows uint64 `json:"success_rows"`
	ErrorRows   uint64 `json:"error_rows"`

	ScanTaskErr string `json:"scan_task_err"`

	// When PreviousOwner != "", it means this task is resigned from another owner
	PreviousOwner string `json:"prev_owner,omitempty"`
}

// RowToTTLTask converts a row into TTL task
func RowToTTLTask(sctx sessionctx.Context, row chunk.Row) (*TTLTask, error) {
	var err error
	timeZone := sctx.GetSessionVars().Location()

	task := &TTLTask{
		JobID:   row.GetString(0),
		JobType: row.GetString(1),
		TableID: row.GetInt64(2),
		ScanID:  row.GetInt64(3),
	}

	if !row.IsNull(4) {
		scanRangeStartBuf := row.GetBytes(4)
		// it's still posibble to be empty even this column is not NULL
		if len(scanRangeStartBuf) > 0 {
			task.ScanRangeStart, err = codec.Decode(scanRangeStartBuf, len(scanRangeStartBuf))
			if err != nil {
				return nil, err
			}
		}
	}
	if !row.IsNull(5) {
		scanRangeEndBuf := row.GetBytes(5)
		// it's still posibble to be empty even this column is not NULL
		if len(scanRangeEndBuf) > 0 {
			task.ScanRangeEnd, err = codec.Decode(scanRangeEndBuf, len(scanRangeEndBuf))
			if err != nil {
				return nil, err
			}
		}
	}

	task.ExpireTime, err = row.GetTime(6).GoTime(timeZone)
	if err != nil {
		return nil, err
	}

	if !row.IsNull(7) {
		task.OwnerID = row.GetString(7)
	}
	if !row.IsNull(8) {
		task.OwnerAddr = row.GetString(8)
	}
	if !row.IsNull(9) {
		task.OwnerHBTime, err = row.GetTime(9).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
	}
	if !row.IsNull(10) {
		status := row.GetString(10)
		if len(status) == 0 {
			status = "waiting"
		}
		task.Status = TaskStatus(status)
	}
	if !row.IsNull(11) {
		task.StatusUpdateTime, err = row.GetTime(11).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
	}
	if !row.IsNull(12) {
		stateStr := row.GetString(12)
		state := &TTLTaskState{}
		err = json.Unmarshal([]byte(stateStr), state)
		if err != nil {
			return nil, err
		}
		task.State = state
	}

	task.CreatedTime, err = row.GetTime(13).GoTime(timeZone)
	if err != nil {
		return nil, err
	}

	return task, nil
}
