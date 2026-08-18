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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	drivermysql "github.com/go-sql-driver/mysql"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/importinto/jobstats"
	"github.com/stretchr/testify/require"
)

type rawStatsMarshaler struct {
	data []byte
}

func (m rawStatsMarshaler) MarshalJSON() ([]byte, error) {
	return m.data, nil
}

func TestSubmitJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewJobManager(db)
	ctx := context.Background()
	sqlQuery := "IMPORT INTO ..."

	cols := []string{
		"Job_ID", "Group_Key", "Data_Source", "Target_Table", "Table_ID",
		"Phase", "Status", "Source_File_Size", "Imported_Rows", "Result_Message",
		"Create_Time", "Start_Time", "End_Time", "Created_By", "Update_Time",
		"Step", "Processed_Size", "Total_Size", "Percent", "Speed", "ETA",
	}

	// Case 1: Success
	rows := sqlmock.NewRows(cols).AddRow(
		int64(123), "", "s3://bucket/file.csv", "db.table", int64(1),
		"import", "finished", "100MB", int64(1000), "success",
		"2023-01-01 10:00:00", "2023-01-01 10:00:01", "2023-01-01 10:00:02", "user", "2023-01-01 10:00:02",
		"", "100MB", "100MB", "100", "10MB/s", "0s",
	)
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
	raw := []byte(`{"version":1,"status":"finished","status_category":"terminal","terminal":true,"source_file_size_bytes":100,"imported_rows":1000,"summary":{"imported_rows":1000,"conflict_rows":3,"too_many_conflicts":true},"create_time_unix":1672567200,"created_by":"root@%"}`)
	rows := sqlmock.NewRows(cols).AddRow(jobID, nil, raw)
	mock.ExpectQuery("SHOW RAW IMPORT JOB 123").WillReturnRows(rows)

	status, err := manager.GetJobStatus(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, jobID, status.JobID)
	require.Equal(t, "finished", status.Status)
	require.Equal(t, int64(100), status.SourceFileSizeBytes)
	require.Equal(t, int64(1000), status.ImportedRows)
	require.Equal(t, "100B", status.SourceFileSize)
	require.Equal(t, "3 conflicted rows. Too many conflicted rows, checksum skipped.", status.ResultMessage)
	require.Equal(t, jobstats.ContractVersion, status.ContractVersion)
	require.Equal(t, jobstats.StatusCategoryTerminal, status.StatusCategory)
	require.True(t, status.Terminal)
	require.NotNil(t, status.Summary)
	require.Equal(t, int64(1000), status.Summary.ImportedRows)
	require.Equal(t, "root@%", status.CreatedBy)
	require.False(t, status.CreateTime.IsZero())

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

func TestRawJobStatusRejectsUnsupportedContractVersion(t *testing.T) {
	for _, raw := range []string{
		`{"status":"running"}`,
		`{"version":0,"status":"running"}`,
		`{"version":2,"status":"running"}`,
	} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		manager := NewJobManager(db)
		rows := sqlmock.NewRows([]string{"Job_ID", "Group_Key", "Raw_Stats"}).AddRow(1, nil, []byte(raw))
		mock.ExpectQuery("SHOW RAW IMPORT JOB 1").WillReturnRows(rows)
		_, err = manager.GetJobStatus(context.Background(), 1)
		require.ErrorContains(t, err, "unsupported SHOW RAW IMPORT JOB stats contract version")
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestJobStatusFromRawStatsPreservesLegacyFields(t *testing.T) {
	remaining := int64(9)
	status := statusFromRaw(&jobstats.RawImportJobStats{
		Version:             jobstats.ContractVersion,
		Status:              importer.JobStatusRunning,
		SourceFileSizeBytes: 100 * 1024 * 1024,
		CurrentStep: &jobstats.RawImportJobStepStats{
			Name:             "import",
			ProcessedBytes:   50 * 1024 * 1024,
			TotalBytes:       100 * 1024 * 1024,
			SpeedBytesPerSec: 10 * 1024 * 1024,
			RemainingSeconds: &remaining,
		},
	})
	require.Equal(t, "100MiB", status.SourceFileSize)
	require.Equal(t, "50MiB", status.ProcessedSize)
	require.Equal(t, "100MiB", status.TotalSize)
	require.Equal(t, "50", status.Percent)
	require.Equal(t, "10MiB/s", status.Speed)
	require.Equal(t, "00:00:09", status.ETA)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	legacyCols := []string{
		"Job_ID", "Group_Key", "Data_Source", "Target_Table", "Table_ID",
		"Phase", "Status", "Source_File_Size", "Imported_Rows", "Result_Message",
		"Create_Time", "Start_Time", "End_Time", "Created_By", "Update_Time",
		"Step", "Processed_Size", "Total_Size", "Percent", "Speed", "ETA",
	}
	mock.ExpectQuery("legacy").WillReturnRows(sqlmock.NewRows(legacyCols).AddRow(
		int64(0), nil, "", "", int64(0), "", "running", "100MiB", nil, nil,
		"", nil, nil, "", nil, "import", "50MiB", "100MiB", "50", "10MiB/s", "00:00:09",
	))
	rows, err := db.Query("legacy")
	require.NoError(t, err)
	require.True(t, rows.Next())
	legacyStatus, err := scanLegacyStatus(rows)
	require.NoError(t, err)
	require.Equal(t, legacyStatus.SourceFileSize, status.SourceFileSize)
	require.Equal(t, legacyStatus.ProcessedSize, status.ProcessedSize)
	require.Equal(t, legacyStatus.TotalSize, status.TotalSize)
	require.Equal(t, legacyStatus.Percent, status.Percent)
	require.Equal(t, legacyStatus.Speed, status.Speed)
	require.Equal(t, legacyStatus.ETA, status.ETA)
	require.NoError(t, rows.Close())
	require.NoError(t, mock.ExpectationsWereMet())

	status = statusFromRaw(&jobstats.RawImportJobStats{
		Version: jobstats.ContractVersion,
		Status:  importer.JobStatusFinished,
		Summary: &jobstats.RawImportJobSummary{
			ConflictRows:     2,
			TooManyConflicts: true,
		},
	})
	require.Equal(t, "2 conflicted rows. Too many conflicted rows, checksum skipped.", status.ResultMessage)
}

func TestGetJobStatusFallbackToLegacy(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewJobManager(db)
	ctx := context.Background()
	jobID := int64(123)
	rawUnsupported := perrors.Normalize(
		"SHOW RAW IMPORT JOB is unsupported",
		perrors.MySQLErrorCode(1235),
	)
	legacyCols := []string{
		"Job_ID", "Group_Key", "Data_Source", "Target_Table", "Table_ID",
		"Phase", "Status", "Source_File_Size", "Imported_Rows", "Result_Message",
		"Create_Time", "Start_Time", "End_Time", "Created_By", "Update_Time",
		"Step", "Processed_Size", "Total_Size", "Percent", "Speed", "ETA",
	}
	legacyRows := func() *sqlmock.Rows {
		return sqlmock.NewRows(legacyCols).AddRow(
			jobID, "", "s3://bucket/file.csv", "db.table", int64(1),
			"import", "finished", "100MB", int64(1000), "success",
			"2023-01-01 10:00:00", "2023-01-01 10:00:01", "2023-01-01 10:00:02", "user", "2023-01-01 10:00:02",
			"", "100MB", "100MB", "100", "10MB/s", "0s",
		)
	}
	mock.ExpectQuery("SHOW RAW IMPORT JOB 123").WillReturnError(rawUnsupported)
	mock.ExpectQuery("SHOW IMPORT JOB 123").WillReturnRows(legacyRows())

	status, err := manager.GetJobStatus(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, jobID, status.JobID)
	require.Equal(t, "finished", status.Status)
	require.Equal(t, int64(1000), status.ImportedRows)

	// The unsupported capability is cached; subsequent polls skip RAW.
	mock.ExpectQuery("SHOW IMPORT JOB 123").WillReturnRows(legacyRows())
	status, err = manager.GetJobStatus(ctx, jobID)
	require.NoError(t, err)
	require.Equal(t, jobID, status.JobID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRawFallbackRequiresUnsupportedErrorCode(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := NewJobManager(db)
	authErr := &drivermysql.MySQLError{Number: 1045, Message: "syntax is not supported for this user"}
	mock.ExpectQuery("SHOW RAW IMPORT JOB 123").WillReturnError(authErr)
	_, err = manager.GetJobStatus(context.Background(), 123)
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
			int64(1), groupKey, []byte(`{"version":1,"status":"finished","status_category":"terminal","terminal":true}`),
		).
		AddRow(
			int64(2), groupKey, []byte(`{"version":1,"status":"running","status_category":"running","terminal":false,"job_phase":"importing","current_step":{"name":"import","processed_bytes":50,"total_bytes":100}}`),
		)
	mock.ExpectQuery("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = 'test_group'").WillReturnRows(rows)

	jobs, err := manager.GetJobsByGroup(ctx, groupKey)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	require.Equal(t, int64(1), jobs[0].JobID)
	require.Equal(t, "finished", jobs[0].Status)
	require.Equal(t, int64(2), jobs[1].JobID)
	require.Equal(t, groupKey, jobs[1].GroupKey)
	require.Equal(t, "running", jobs[1].Status)
	require.Equal(t, "importing", jobs[1].Phase)
	require.NotNil(t, jobs[1].CurrentStep)
	require.Equal(t, "import", jobs[1].Step)
	require.Equal(t, "50B", jobs[1].ProcessedSize)
	require.Equal(t, "100B", jobs[1].TotalSize)
	require.Equal(t, "50", jobs[1].Percent)
	require.Equal(t, "0B/s", jobs[1].Speed)
	require.Equal(t, "N/A", jobs[1].ETA)

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

func TestGetJobsByGroupLegacyCompatibility(t *testing.T) {
	legacyCols := []string{
		"Job_ID", "Group_Key", "Data_Source", "Target_Table", "Table_ID",
		"Phase", "Status", "Source_File_Size", "Imported_Rows", "Result_Message",
		"Create_Time", "Start_Time", "End_Time", "Created_By", "Update_Time",
		"Step", "Processed_Size", "Total_Size", "Percent", "Speed", "ETA",
	}
	legacyRows := func() *sqlmock.Rows {
		return sqlmock.NewRows(legacyCols).AddRow(
			int64(1), "test_group", "s3://bucket/file.csv", "db.table", int64(1),
			"import", "finished", "100MB", int64(1000), "success",
			"2023-01-01 10:00:00", "2023-01-01 10:00:01", "2023-01-01 10:00:02", "user", "2023-01-01 10:00:02",
			"", "100MB", "100MB", "100", "10MB/s", "0s",
		)
	}

	t.Run("unsupported RAW capability is cached", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		manager := NewJobManager(db)
		rawUnsupported := &drivermysql.MySQLError{Number: 1064, Message: "syntax error near RAW IMPORT JOBS"}
		mock.ExpectQuery("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = 'test_group'").WillReturnError(rawUnsupported)
		mock.ExpectQuery("SHOW IMPORT JOBS WHERE GROUP_KEY = 'test_group'").WillReturnRows(legacyRows())

		jobs, err := manager.GetJobsByGroup(context.Background(), "test_group")
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, int64(1), jobs[0].JobID)

		// A later poll skips the unsupported RAW statement.
		mock.ExpectQuery("SHOW IMPORT JOBS WHERE GROUP_KEY = 'test_group'").WillReturnRows(legacyRows())
		jobs, err = manager.GetJobsByGroup(context.Background(), "test_group")
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RAW query accepts legacy columns during rolling upgrade", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		manager := NewJobManager(db)
		mock.ExpectQuery("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = 'test_group'").WillReturnRows(legacyRows())
		jobs, err := manager.GetJobsByGroup(context.Background(), "test_group")
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Zero(t, jobs[0].ContractVersion)
		require.Equal(t, "finished", jobs[0].Status)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestRawBytes(t *testing.T) {
	raw := []byte(`{"version":1,"status":"finished"}`)

	for name, src := range map[string]any{
		"bytes":      raw,
		"string":     string(raw),
		"marshaler":  rawStatsMarshaler{data: raw},
		"rawMessage": json.RawMessage(raw),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := rawBytes(src)
			require.NoError(t, err)
			require.JSONEq(t, string(raw), string(got))
		})
	}

	_, err := rawBytes(123)
	require.Error(t, err)
}
