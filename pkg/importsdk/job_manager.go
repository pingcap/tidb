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

package importsdk

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
)

// JobManager defines the interface for managing import jobs
type JobManager interface {
	SubmitJob(ctx context.Context, query string) (int64, error)
	GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error)
	CancelJob(ctx context.Context, jobID int64) error
	GetGroupSummary(ctx context.Context, groupKey string) (*GroupStatus, error)
	GetJobsByGroup(ctx context.Context, groupKey string) ([]*JobStatus, error)
}

const timeLayout = "2006-01-02 15:04:05"

type jobManager struct {
	db *sql.DB
}

// NewJobManager creates a new JobManager
func NewJobManager(db *sql.DB) JobManager {
	return &jobManager{
		db: db,
	}
}

// SubmitJob submits an import job and returns the job ID
func (m *jobManager) SubmitJob(ctx context.Context, query string) (int64, error) {
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		jobID, err := scanJobID(rows)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return jobID, nil
	}

	if err := rows.Err(); err != nil {
		return 0, errors.Trace(err)
	}

	return 0, ErrNoJobIDReturned
}

// GetJobStatus gets the status of an import job
func (m *jobManager) GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	query := fmt.Sprintf("SHOW RAW IMPORT JOB %d", jobID)
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		return scanJobStatus(rows)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	return nil, ErrJobNotFound
}

// GetGroupSummary returns aggregated information for the specified group key.
func (m *jobManager) GetGroupSummary(ctx context.Context, groupKey string) (*GroupStatus, error) {
	if groupKey == "" {
		return nil, ErrInvalidOptions
	}
	query := fmt.Sprintf("SHOW IMPORT GROUP '%s'", strings.ReplaceAll(groupKey, "'", "''"))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		status, err := scanGroupStatus(rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return status, nil
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return nil, ErrJobNotFound
}

// GetJobsByGroup returns all jobs for the specified group key.
func (m *jobManager) GetJobsByGroup(ctx context.Context, groupKey string) ([]*JobStatus, error) {
	if groupKey == "" {
		return nil, ErrInvalidOptions
	}
	query := fmt.Sprintf("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = '%s'", strings.ReplaceAll(groupKey, "'", "''"))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var jobs []*JobStatus
	for rows.Next() {
		status, err := scanJobStatus(rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, status)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

func scanJobStatus(rows *sql.Rows) (*JobStatus, error) {
	var (
		jobID    int64
		groupKey sql.NullString
		rawStats []byte
	)

	if err := rows.Scan(&jobID, &groupKey, &rawStats); err != nil {
		return nil, errors.Trace(err)
	}
	if len(rawStats) == 0 {
		return nil, errors.New("Raw_Stats is empty")
	}

	status := &JobStatus{}
	if err := json.Unmarshal(rawStats, status); err != nil {
		return nil, errors.Trace(err)
	}
	// Ensure the top-level columns and JSON are consistent.
	status.JobID = jobID
	if groupKey.Valid {
		status.GroupKey = groupKey.String
	} else {
		status.GroupKey = ""
	}
	return status, nil
}

func scanJobID(rows *sql.Rows) (int64, error) {
	cols, err := rows.Columns()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(cols) == 0 {
		return 0, ErrNoJobIDReturned
	}

	dest := make([]any, len(cols))
	var jobID int64
	dest[0] = &jobID
	// We only need Job_ID; scan the rest into dummy values.
	for i := 1; i < len(cols); i++ {
		dest[i] = new(any)
	}
	if err := rows.Scan(dest...); err != nil {
		return 0, errors.Trace(err)
	}
	return jobID, nil
}

func scanGroupStatus(rows *sql.Rows) (*GroupStatus, error) {
	var (
		groupKey        string
		totalJobs       int64
		pending         int64
		running         int64
		completed       int64
		failed          int64
		cancelled       int64
		firstCreateTime sql.NullString
		lastUpdateTime  sql.NullString
	)

	if err := rows.Scan(&groupKey, &totalJobs, &pending, &running, &completed, &failed, &cancelled, &firstCreateTime, &lastUpdateTime); err != nil {
		return nil, errors.Trace(err)
	}

	return &GroupStatus{
		GroupKey:           groupKey,
		TotalJobs:          totalJobs,
		Pending:            pending,
		Running:            running,
		Completed:          completed,
		Failed:             failed,
		Cancelled:          cancelled,
		FirstJobCreateTime: parseNullTime(firstCreateTime),
		LastJobUpdateTime:  parseNullTime(lastUpdateTime),
	}, nil
}

// CancelJob cancels an import job
func (m *jobManager) CancelJob(ctx context.Context, jobID int64) error {
	query := fmt.Sprintf("CANCEL IMPORT JOB %d", jobID)
	_, err := m.db.ExecContext(ctx, query)
	return errors.Trace(err)
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(timeLayout, s)
	return t
}

func parseNullTime(ns sql.NullString) time.Time {
	if !ns.Valid {
		return time.Time{}
	}
	return parseTime(ns.String)
}
