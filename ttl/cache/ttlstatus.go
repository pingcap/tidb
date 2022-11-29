// Copyright 2022 PingCAP, Inc.
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
	"context"
	"time"

	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// JobStatus represents the current status of a job
type JobStatus string

const (
	// JobStatusWaiting means the job hasn't started
	JobStatusWaiting JobStatus = "waiting"
	// JobStatusRunning means this job is running
	JobStatusRunning = "running"
	// JobStatusCancelling means this job is being canceled, but not canceled yet
	JobStatusCancelling = "cancelling"
	// JobStatusCancelled means this job has been canceled successfully
	JobStatusCancelled = "cancelled"
	// JobStatusError means this job is in error status
	JobStatusError = "error"
)

const selectFromTTLTableStatus = "SELECT table_id,parent_table_id,table_statistics,last_job_id,last_job_start_time,last_job_finish_time,last_job_ttl_expire,last_job_summary,current_job_id,current_job_owner_id,current_job_owner_addr,current_job_owner_hb_time,current_job_start_time,current_job_ttl_expire,current_job_state,current_job_status,current_job_status_update_time FROM mysql.tidb_ttl_table_status"

// TableStatus contains the corresponding information in the system table `mysql.tidb_ttl_table_status`
type TableStatus struct {
	TableID       int64
	ParentTableID int64

	TableStatistics string

	LastJobID         string
	LastJobStartTime  types.Time
	LastJobFinishTime types.Time
	LastJobTTLExpire  types.Time
	LastJobSummary    string

	CurrentJobID          string
	CurrentJobOwnerID     string
	CurrentJobOwnerAddr   string
	CurrentJobOwnerHBTime types.Time
	CurrentJobStartTime   types.Time
	CurrentJobTTLExpire   types.Time

	CurrentJobState            string
	CurrentJobStatus           JobStatus
	CurrentJobStatusUpdateTime types.Time
}

// TableStatusCache is the cache for ttl table status, it builds a map from physical table id to the table status
type TableStatusCache struct {
	baseCache

	Tables map[int64]*TableStatus
}

// NewTableStatusCache creates cache for ttl table status
func NewTableStatusCache(updateInterval time.Duration) *TableStatusCache {
	return &TableStatusCache{
		baseCache: newBaseCache(updateInterval),
	}
}

// Update updates the table status cache
func (tsc *TableStatusCache) Update(ctx context.Context, se session.Session) error {
	rows, err := se.ExecuteSQL(ctx, selectFromTTLTableStatus)
	if err != nil {
		return err
	}

	newTables := make(map[int64]*TableStatus, len(rows))
	for _, row := range rows {
		status := rowToTableStatus(row)
		newTables[status.TableID] = status
	}
	tsc.Tables = newTables
	tsc.updateTime = time.Now()
	return nil
}

func rowToTableStatus(row chunk.Row) *TableStatus {
	status := &TableStatus{
		TableID: row.GetInt64(0),
	}
	if !row.IsNull(1) {
		status.ParentTableID = row.GetInt64(1)
	}
	if !row.IsNull(2) {
		status.TableStatistics = row.GetString(2)
	}
	if !row.IsNull(3) {
		status.LastJobID = row.GetString(3)
	}
	if !row.IsNull(4) {
		status.LastJobStartTime = row.GetTime(4)
	}
	if !row.IsNull(5) {
		status.LastJobFinishTime = row.GetTime(5)
	}
	if !row.IsNull(6) {
		status.LastJobTTLExpire = row.GetTime(6)
	}
	if !row.IsNull(7) {
		status.LastJobSummary = row.GetString(7)
	}
	if !row.IsNull(8) {
		status.CurrentJobID = row.GetString(8)
	}
	if !row.IsNull(9) {
		status.CurrentJobOwnerID = row.GetString(9)
	}
	if !row.IsNull(10) {
		status.CurrentJobOwnerAddr = row.GetString(10)
	}
	if !row.IsNull(11) {
		status.CurrentJobOwnerHBTime = row.GetTime(11)
	}
	if !row.IsNull(12) {
		status.CurrentJobStartTime = row.GetTime(12)
	}
	if !row.IsNull(13) {
		status.CurrentJobTTLExpire = row.GetTime(13)
	}
	if !row.IsNull(14) {
		status.CurrentJobState = row.GetString(14)
	}
	if !row.IsNull(15) {
		jobStatus := row.GetString(15)
		if len(jobStatus) == 0 {
			jobStatus = "waiting"
		}
		status.CurrentJobStatus = JobStatus(jobStatus)
	}
	if !row.IsNull(16) {
		status.CurrentJobStatusUpdateTime = row.GetTime(16)
	}

	return status
}
