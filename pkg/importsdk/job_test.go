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
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestSubmitImportJob(t *testing.T) {
	cases := []struct {
		name              string
		importSQL         string
		prepare           func(sqlmock.Sqlmock)
		expectErr         error
		expectErrContains string
		assert            func(*testing.T, *JobDetail)
	}{
		{
			name:      "successWithExtraRows",
			importSQL: "IMPORT INTO t FROM 's3://bucket'",
			prepare: func(mock sqlmock.Sqlmock) {
				rows := newJobRows().
					AddRow(jobRowValues(42, "group-42")...).
					AddRow(jobRowValues(99, "group-99")...)
				mock.ExpectQuery("IMPORT INTO").WillReturnRows(rows)
			},
			assert: func(t *testing.T, job *JobDetail) {
				require.Equal(t, int64(42), job.JobID)
				require.Equal(t, "group-42", job.GroupKey.String)
				require.Equal(t, int64(420), job.ImportedRows.Int64)
				require.Equal(t, "ingest", job.CurrentStep.String)
			},
		},
		{
			name:      "noRowsReturned",
			importSQL: "IMPORT INTO t FROM 's3://bucket'",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("IMPORT INTO").WillReturnRows(newJobRows())
			},
			expectErr: ErrNoJobInfoReturned,
		},
		{
			name:      "queryError",
			importSQL: "IMPORT INTO t FROM 's3://bucket'",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("IMPORT INTO").WillReturnError(fmt.Errorf("query failed"))
			},
			expectErrContains: "query failed",
		},
		{
			name:      "successWithSessionVars",
			importSQL: "IMPORT INTO t FROM 's3://bucket'",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("SET SESSION foo = 'bar'").WillReturnResult(sqlmock.NewResult(0, 0))
				rows := newJobRows().AddRow(jobRowValues(100, "group-100")...)
				mock.ExpectQuery("IMPORT INTO").WillReturnRows(rows)
			},
			assert: func(t *testing.T, job *JobDetail) {
				require.Equal(t, int64(100), job.JobID)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockDB(t)
			var sdk *ImportSDK
			if tc.name == "successWithSessionVars" {
				// Manually construct ImportSDK to avoid S3 connection attempts in NewImportSDK
				sdk = &ImportSDK{
					db: db,
				}
			} else {
				sdk = &ImportSDK{db: db}
			}

			ctx := context.Background()
			if tc.prepare != nil {
				tc.prepare(mock)
			}

			job, err := sdk.SubmitImportJob(ctx, tc.importSQL)
			switch {
			case tc.expectErr != nil:
				require.ErrorIs(t, err, tc.expectErr)
				require.Nil(t, job)
			case tc.expectErrContains != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErrContains)
				require.Nil(t, job)
			default:
				require.NoError(t, err)
				require.NotNil(t, job)
				if tc.assert != nil {
					tc.assert(t, job)
				}
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestFetchImportJob(t *testing.T) {
	cases := []struct {
		name              string
		jobID             int64
		prepare           func(sqlmock.Sqlmock)
		expectErr         error
		expectErrContains string
		assert            func(*testing.T, *JobDetail)
	}{
		{
			name:  "success",
			jobID: 7,
			prepare: func(mock sqlmock.Sqlmock) {
				rows := newJobRows().AddRow(jobRowValues(7, "group-7")...)
				mock.ExpectQuery("SHOW IMPORT JOB 7").WillReturnRows(rows)
			},
			assert: func(t *testing.T, job *JobDetail) {
				require.Equal(t, int64(7), job.JobID)
			},
		},
		{
			name:      "notFound",
			jobID:     42,
			prepare:   func(mock sqlmock.Sqlmock) { mock.ExpectQuery("SHOW IMPORT JOB 42").WillReturnRows(newJobRows()) },
			expectErr: ErrJobNotFound,
		},
		{
			name:  "invalidRow",
			jobID: 55,
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW IMPORT JOB 55").WillReturnRows(newJobRows().AddRow(jobRowWithNullID()...))
			},
			expectErrContains: "Job_ID",
		},
		{
			name:              "queryError",
			jobID:             9,
			prepare:           func(mock sqlmock.Sqlmock) { mock.ExpectQuery("SHOW IMPORT JOB 9").WillReturnError(fmt.Errorf("boom")) },
			expectErrContains: "boom",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockDB(t)
			sdk := &ImportSDK{db: db}
			ctx := context.Background()
			if tc.prepare != nil {
				tc.prepare(mock)
			}

			job, err := sdk.FetchImportJob(ctx, tc.jobID)
			switch {
			case tc.expectErr != nil:
				require.ErrorIs(t, err, tc.expectErr)
				require.Nil(t, job)
			case tc.expectErrContains != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErrContains)
				require.Nil(t, job)
			default:
				require.NoError(t, err)
				require.NotNil(t, job)
				if tc.assert != nil {
					tc.assert(t, job)
				}
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestFetchImportJobs(t *testing.T) {
	cases := []struct {
		name              string
		prepare           func(sqlmock.Sqlmock)
		expectErrContains string
		assert            func(*testing.T, []*JobDetail)
	}{
		{
			name: "success",
			prepare: func(mock sqlmock.Sqlmock) {
				rows := newJobRows().
					AddRow(jobRowValues(1, "group-1")...).
					AddRow(jobRowValues(2, "group-2")...)
				mock.ExpectQuery("SHOW IMPORT JOBS").WillReturnRows(rows)
			},
			assert: func(t *testing.T, jobs []*JobDetail) {
				require.Len(t, jobs, 2)
				require.Equal(t, int64(1), jobs[0].JobID)
				require.Equal(t, int64(2), jobs[1].JobID)
			},
		},
		{
			name: "scanError",
			prepare: func(mock sqlmock.Sqlmock) {
				rows := newJobRows().AddRow(jobRowWithNullID()...)
				mock.ExpectQuery("SHOW IMPORT JOBS").WillReturnRows(rows)
			},
			expectErrContains: "Job_ID",
		},
		{
			name: "queryError",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW IMPORT JOBS").WillReturnError(fmt.Errorf("boom"))
			},
			expectErrContains: "boom",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockDB(t)
			sdk := &ImportSDK{db: db}
			ctx := context.Background()
			if tc.prepare != nil {
				tc.prepare(mock)
			}

			jobs, err := sdk.FetchImportJobs(ctx)
			if tc.expectErrContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErrContains)
				require.Nil(t, jobs)
			} else {
				require.NoError(t, err)
				require.NotNil(t, jobs)
				if tc.assert != nil {
					tc.assert(t, jobs)
				}
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestCancelImportJob(t *testing.T) {
	cases := []struct {
		name              string
		jobID             int64
		prepare           func(sqlmock.Sqlmock)
		expectErrContains string
	}{
		{
			name:  "success",
			jobID: 55,
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CANCEL IMPORT JOB 55").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:  "execError",
			jobID: 56,
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CANCEL IMPORT JOB 56").WillReturnError(fmt.Errorf("boom"))
			},
			expectErrContains: "boom",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockDB(t)
			sdk := &ImportSDK{db: db}
			ctx := context.Background()
			if tc.prepare != nil {
				tc.prepare(mock)
			}

			err := sdk.CancelImportJob(ctx, tc.jobID)
			if tc.expectErrContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErrContains)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPauseAndResumeImportJob(t *testing.T) {
	type testCase struct {
		name              string
		verb              string
		jobID             int64
		prepare           func(sqlmock.Sqlmock)
		expectErrContains string
	}
	cases := []testCase{
		{
			name:  "pauseSuccess",
			verb:  "PAUSE",
			jobID: 101,
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("PAUSE IMPORT JOB 101").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:  "pauseError",
			verb:  "PAUSE",
			jobID: 202,
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("PAUSE IMPORT JOB 202").WillReturnError(fmt.Errorf("boom"))
			},
			expectErrContains: "boom",
		},
		{
			name:  "resumeSuccess",
			verb:  "RESUME",
			jobID: 303,
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("RESUME IMPORT JOB 303").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:  "resumeError",
			verb:  "RESUME",
			jobID: 404,
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("RESUME IMPORT JOB 404").WillReturnError(fmt.Errorf("oops"))
			},
			expectErrContains: "oops",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockDB(t)
			sdk := &ImportSDK{db: db}
			ctx := context.Background()
			if tc.prepare != nil {
				tc.prepare(mock)
			}

			var err error
			switch tc.verb {
			case "PAUSE":
				err = sdk.PauseImportJob(ctx, tc.jobID)
			case "RESUME":
				err = sdk.ResumeImportJob(ctx, tc.jobID)
			default:
				t.Fatalf("unsupported verb %s", tc.verb)
			}

			if tc.expectErrContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErrContains)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})
	return db, mock
}

func newJobRows() *sqlmock.Rows {
	return sqlmock.NewRows(jobColumns())
}

func jobColumns() []string {
	return []string{
		"Job_ID",
		"Group_Key",
		"Data_Source",
		"Target_Table",
		"Table_ID",
		"Phase",
		"Status",
		"Source_File_Size",
		"Imported_Rows",
		"Result_Message",
		"Create_Time",
		"Start_Time",
		"End_Time",
		"Created_By",
		"Last_Update_Time",
		"Cur_Step",
		"Cur_Step_Processed_Size",
		"Cur_Step_Total_Size",
		"Cur_Step_Progress_Pct",
		"Cur_Step_Speed",
		"Cur_Step_ETA",
	}
}

func jobRowValues(jobID int64, group string) []driver.Value {
	ts := time.Unix(1700000000+jobID, 0).UTC()
	return []driver.Value{
		jobID,
		group,
		fmt.Sprintf("s3://bucket/%d", jobID),
		fmt.Sprintf("db.table_%d", jobID),
		jobID + 100,
		"write",
		"running",
		"64MB",
		jobID * 10,
		"ok",
		ts,
		ts,
		ts,
		"root",
		ts,
		"ingest",
		"32MB",
		"64MB",
		"50%",
		"10MB/s",
		"2m",
	}
}

func jobRowValuesWithStringTimes(jobID int64, group string) []driver.Value {
	values := jobRowValues(jobID, group)
	formatted := values[10].(time.Time).In(time.Local).Format("2006-01-02 15:04:05")
	for _, idx := range []int{10, 11, 12, 14} {
		values[idx] = formatted
	}
	return values
}

func jobRowValuesWithByteTimes(jobID int64, group string) []driver.Value {
	values := jobRowValuesWithStringTimes(jobID, group)
	for _, idx := range []int{10, 11, 12, 14} {
		values[idx] = []byte(values[idx].(string))
	}
	return values
}

func jobRowValuesWithInvalidTime(jobID int64, group string) []driver.Value {
	values := jobRowValuesWithStringTimes(jobID, group)
	values[10] = "not-a-time"
	return values
}

func TestNullableTimeScan(t *testing.T) {
	loc := time.FixedZone("UTC+1", 3600)
	original := time.Date(2025, 5, 1, 12, 0, 0, 0, loc)
	cases := []struct {
		name      string
		value     any
		expectErr bool
		valid     bool
	}{
		{"timeValue", original, false, true},
		{"stringValue", original.UTC().Format("2006-01-02 15:04:05"), false, true},
		{"bytesValue", []byte(original.UTC().Format(time.RFC3339Nano)), false, true},
		{"emptyString", "", false, false},
		{"invalid", "2025-13-99", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var nt nullableTime
			err := nt.Scan(tc.value)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.valid, nt.Valid)
			if tc.valid {
				require.False(t, nt.Time.IsZero())
			}
		})
	}
}

func jobRowWithNullID() []driver.Value {
	values := jobRowValues(1, "group-null")
	values[0] = nil
	return values
}
