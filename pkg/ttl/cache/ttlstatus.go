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

	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// JobStatus represents the current status of a job
type JobStatus string

const (
	// JobStatusWaiting means the job hasn't started
	JobStatusWaiting JobStatus = "waiting"
	// JobStatusRunning means this job is running
	JobStatusRunning JobStatus = "running"
	// JobStatusCancelling means this job is being canceled, but not canceled yet
	JobStatusCancelling JobStatus = "cancelling"
	// JobStatusCancelled means this job has been canceled successfully
	JobStatusCancelled JobStatus = "cancelled"
	// JobStatusTimeout means this job has timeout
	JobStatusTimeout JobStatus = "timeout"
	// JobStatusFinished means job has been finished
	JobStatusFinished JobStatus = "finished"
)

const selectFromTableStatusPrefix = "SELECT LOW_PRIORITY table_id,parent_table_id,table_statistics,last_job_id,last_job_start_time,last_job_finish_time,last_job_ttl_expire,last_job_summary,current_job_id,current_job_owner_id,current_job_owner_addr,current_job_owner_hb_time,current_job_start_time,current_job_ttl_expire,current_job_state,current_job_status,current_job_status_update_time FROM "

var (
	selectFromTTLTableStatus        = selectFromTableStatusPrefix + "mysql.tidb_ttl_table_status"
	selectFromSoftDeleteTableStatus = selectFromTableStatusPrefix + "mysql.tidb_softdelete_table_status"
)

// SelectFromTableStatusWithID returns an SQL statement to get the table status by table id for the specified job type.
func SelectFromTableStatusWithID(jobType session.TTLJobType, tableID int64) (string, []any) {
	if jobType == session.TTLJobTypeSoftDelete {
		return selectFromSoftDeleteTableStatus + " WHERE table_id = %?", []any{tableID}
	}
	return selectFromTTLTableStatus + " WHERE table_id = %?", []any{tableID}
}

// TableStatus contains the corresponding information in the system table `mysql.tidb_ttl_table_status`
type TableStatus struct {
	TableID       int64
	ParentTableID int64

	TableStatistics string

	LastJobID         string
	LastJobStartTime  time.Time
	LastJobFinishTime time.Time
	LastJobTTLExpire  time.Time
	LastJobSummary    string

	CurrentJobID          string
	CurrentJobOwnerID     string
	CurrentJobOwnerAddr   string
	CurrentJobOwnerHBTime time.Time
	CurrentJobStartTime   time.Time
	CurrentJobTTLExpire   time.Time

	CurrentJobState            string
	CurrentJobStatus           JobStatus
	CurrentJobStatusUpdateTime time.Time
}

// TTLTableStatusCache is the cache for ttl table status, it builds a map from physical table id to the table status
type TTLTableStatusCache struct {
	baseCache

	Tables map[int64]*TableStatus
}

// NewTableStatusCache creates cache for ttl table status
func NewTableStatusCache(updateInterval time.Duration) *TTLTableStatusCache {
	return &TTLTableStatusCache{
		baseCache: newBaseCache(updateInterval),
		Tables:    make(map[int64]*TableStatus),
	}
}

// Update updates the table status cache
func (tsc *TTLTableStatusCache) Update(ctx context.Context, se session.Session) error {
	newTables, err := updateTableStatusCache(ctx, se, selectFromTTLTableStatus, RowToTableStatus)
	if err != nil {
		return err
	}
	tsc.Tables = newTables
	tsc.updateTime = time.Now()
	return nil
}

// SoftDeleteTableStatusCache is the cache for softdelete table status, it builds a map from table id to the table status.
type SoftDeleteTableStatusCache struct {
	baseCache

	Tables map[int64]*TableStatus
}

// NewSoftDeleteTableStatusCache creates cache for softdelete table status.
func NewSoftDeleteTableStatusCache(updateInterval time.Duration) *SoftDeleteTableStatusCache {
	return &SoftDeleteTableStatusCache{
		baseCache: newBaseCache(updateInterval),
		Tables:    make(map[int64]*TableStatus),
	}
}

// Update updates the softdelete table status cache.
func (sc *SoftDeleteTableStatusCache) Update(ctx context.Context, se session.Session) error {
	newTables, err := updateTableStatusCache(ctx, se, selectFromSoftDeleteTableStatus, RowToTableStatus)
	if err != nil {
		return err
	}
	sc.Tables = newTables
	sc.updateTime = time.Now()
	return nil
}

func updateTableStatusCache(
	ctx context.Context,
	se session.Session,
	selectSQL string,
	rowToStatus func(timeZone *time.Location, row chunk.Row) (*TableStatus, error),
) (map[int64]*TableStatus, error) {
	rows, err := se.ExecuteSQL(ctx, selectSQL)
	if err != nil {
		return nil, err
	}

	newTables := make(map[int64]*TableStatus, len(rows))
	for _, row := range rows {
		status, err := rowToStatus(se.GetSessionVars().Location(), row)
		if err != nil {
			return nil, err
		}
		newTables[status.TableID] = status
	}

	return newTables, nil
}

// RowToTableStatus converts a row to table status
func RowToTableStatus(timeZone *time.Location, row chunk.Row) (*TableStatus, error) {
	var err error

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
		status.LastJobStartTime, err = row.GetTime(4).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
	}
	if !row.IsNull(5) {
		status.LastJobFinishTime, err = row.GetTime(5).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
	}
	if !row.IsNull(6) {
		status.LastJobTTLExpire, err = row.GetTime(6).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
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
		status.CurrentJobOwnerHBTime, err = row.GetTime(11).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
	}
	if !row.IsNull(12) {
		status.CurrentJobStartTime, err = row.GetTime(12).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
	}
	if !row.IsNull(13) {
		status.CurrentJobTTLExpire, err = row.GetTime(13).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
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
		status.CurrentJobStatusUpdateTime, err = row.GetTime(16).GoTime(timeZone)
		if err != nil {
			return nil, err
		}
	}

	return status, nil
}
