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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestSubmitJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewJobManager(db)
	ctx := context.Background()
	sqlQuery := "IMPORT INTO ..."

	cols := []string{"Job_ID"}

	// Case 1: Success
	rows := sqlmock.NewRows(cols).AddRow(int64(123))
	mock.ExpectQuery(sqlQuery).WillReturnRows(rows)

	jobID, err := manager.SubmitJob(ctx, sqlQuery)
	require.NoError(t, err)
	require.Equal(t, int64(123), jobID)

	// Case 2: No rows returned
	mock.ExpectQuery(sqlQuery).WillReturnRows(sqlmock.NewRows(cols))
	_, err = manager.SubmitJob(ctx, sqlQuery)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no job id returned")

	// Case 3: Error
	mock.ExpectQuery(sqlQuery).WillReturnError(sql.ErrConnDone)
	_, err = manager.SubmitJob(ctx, sqlQuery)
	require.Error(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetJobStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewJobManager(db)
	ctx := context.Background()
	jobID := int64(123)

	cols := []string{"Job_ID", "Group_Key", "Raw_Stats"}

	// Case 1: Success
	raw := []byte(`{"job_id":123,"status":"finished","source_file_size_bytes":100}`)
	rows := sqlmock.NewRows(cols).AddRow(jobID, nil, raw)
	mock.ExpectQuery("SHOW RAW IMPORT JOB 123").WillReturnRows(rows)

	status, err := manager.GetJobStatus(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, jobID, status.JobID)
	require.Equal(t, "finished", status.Status)

	// Case 2: Job not found
	mock.ExpectQuery("SHOW RAW IMPORT JOB 123").WillReturnRows(sqlmock.NewRows(cols))
	_, err = manager.GetJobStatus(ctx, jobID)
	require.ErrorIs(t, err, ErrJobNotFound)

	// Case 3: Error
	mock.ExpectQuery("SHOW RAW IMPORT JOB 123").WillReturnError(sql.ErrConnDone)
	_, err = manager.GetJobStatus(ctx, jobID)
	require.Error(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCancelJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewJobManager(db)
	ctx := context.Background()
	jobID := int64(123)

	// Case 1: Success
	mock.ExpectExec("CANCEL IMPORT JOB 123").WillReturnResult(sqlmock.NewResult(0, 0))
	err = manager.CancelJob(ctx, jobID)
	require.NoError(t, err)

	// Case 2: Error
	mock.ExpectExec("CANCEL IMPORT JOB 123").WillReturnError(sql.ErrConnDone)
	err = manager.CancelJob(ctx, jobID)
	require.Error(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetGroupSummary(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewJobManager(db)
	ctx := context.Background()
	groupKey := "test_group"

	// Columns expected by scanGroupStatus
	cols := []string{
		"Group_Key", "Total_Jobs", "Pending", "Running", "Completed", "Failed", "Cancelled",
		"First_Job_Create_Time", "Last_Job_Update_Time",
	}

	// Case 1: Success
	rows := sqlmock.NewRows(cols).AddRow(
		groupKey, int64(10), int64(1), int64(2), int64(3), int64(2), int64(2),
		"2023-01-01 10:00:00", "2023-01-01 12:00:00",
	)
	mock.ExpectQuery("SHOW IMPORT GROUP 'test_group'").WillReturnRows(rows)

	summary, err := manager.GetGroupSummary(ctx, groupKey)
	require.NoError(t, err)
	require.Equal(t, groupKey, summary.GroupKey)
	require.Equal(t, int64(10), summary.TotalJobs)
	require.Equal(t, int64(1), summary.Pending)
	require.Equal(t, int64(2), summary.Running)
	require.Equal(t, int64(3), summary.Completed)
	require.Equal(t, int64(2), summary.Failed)
	require.Equal(t, int64(2), summary.Cancelled)

	// Case 2: Empty Group Key
	_, err = manager.GetGroupSummary(ctx, "")
	require.ErrorIs(t, err, ErrInvalidOptions)

	// Case 3: Group not found
	mock.ExpectQuery("SHOW IMPORT GROUP 'test_group'").WillReturnRows(sqlmock.NewRows(cols))
	_, err = manager.GetGroupSummary(ctx, groupKey)
	require.ErrorIs(t, err, ErrJobNotFound)

	// Case 4: Error
	mock.ExpectQuery("SHOW IMPORT GROUP 'test_group'").WillReturnError(sql.ErrConnDone)
	_, err = manager.GetGroupSummary(ctx, groupKey)
	require.Error(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetJobsByGroup(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewJobManager(db)
	ctx := context.Background()
	groupKey := "test_group"

	cols := []string{"Job_ID", "Group_Key", "Raw_Stats"}

	// Case 1: Success
	rows := sqlmock.NewRows(cols).
		AddRow(
			int64(1), groupKey, []byte(`{"job_id":1,"group_key":"test_group","status":"finished"}`),
		).
		AddRow(
			int64(2), groupKey, []byte(`{"job_id":2,"group_key":"test_group","status":"running"}`),
		)
	mock.ExpectQuery("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = 'test_group'").WillReturnRows(rows)

	jobs, err := manager.GetJobsByGroup(ctx, groupKey)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	require.Equal(t, int64(1), jobs[0].JobID)
	require.Equal(t, "finished", jobs[0].Status)
	require.Equal(t, int64(2), jobs[1].JobID)
	require.Equal(t, "running", jobs[1].Status)

	// Case 2: Empty Group Key
	_, err = manager.GetJobsByGroup(ctx, "")
	require.ErrorIs(t, err, ErrInvalidOptions)

	// Case 3: No jobs found (empty list)
	mock.ExpectQuery("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = 'test_group'").WillReturnRows(sqlmock.NewRows(cols))
	jobs, err = manager.GetJobsByGroup(ctx, groupKey)
	require.NoError(t, err)
	require.Empty(t, jobs)

	// Case 4: Error
	mock.ExpectQuery("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = 'test_group'").WillReturnError(sql.ErrConnDone)
	_, err = manager.GetJobsByGroup(ctx, groupKey)
	require.Error(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}
