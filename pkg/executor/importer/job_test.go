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

package importer_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func jobInfoEqual(t *testing.T, expected, got *importer.JobInfo) {
	cloned := *expected
	cloned.CreateTime = got.CreateTime
	cloned.StartTime = got.StartTime
	cloned.EndTime = got.EndTime
	require.Equal(t, &cloned, got)
}

func TestJobHappyPath(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	conn := tk.Session().GetSQLExecutor()

	cases := []struct {
		action          func(jobID int64)
		expectStatus    string
		expectStep      string
		expectedSummary *importer.JobSummary
		expectedErrMsg  string
	}{
		{
			action: func(jobID int64) {
				require.NoError(t, importer.FinishJob(ctx, conn, jobID, &importer.JobSummary{ImportedRows: 111}))
			},
			expectStatus:    "finished",
			expectStep:      "",
			expectedSummary: &importer.JobSummary{ImportedRows: 111},
		},
		{
			action: func(jobID int64) {
				require.NoError(t, importer.FailJob(ctx, conn, jobID, "some error"))
			},
			expectStatus:   "failed",
			expectStep:     importer.JobStepValidating,
			expectedErrMsg: "some error",
		},
	}
	for _, c := range cases {
		jobInfo := &importer.JobInfo{
			TableSchema: "test",
			TableName:   "t",
			TableID:     1,
			CreatedBy:   "root@%",
			Parameters: importer.ImportParameters{
				ColumnsAndVars: "(a, b, c)",
				SetClause:      "d = 1",
				Format:         importer.DataFormatCSV,
				Options: map[string]any{
					"skip_rows": float64(1), // json unmarshal will convert number to float64
					"detached":  nil,
				},
			},
			SourceFileSize: 123,
			Status:         "pending",
		}

		// create job
		jobID, err := importer.CreateJob(ctx, conn, jobInfo.TableSchema, jobInfo.TableName, jobInfo.TableID,
			jobInfo.CreatedBy, &jobInfo.Parameters, jobInfo.SourceFileSize)
		require.NoError(t, err)
		jobInfo.ID = jobID
		gotJobInfo, err := importer.GetJob(ctx, conn, jobID, jobInfo.CreatedBy, false)
		require.NoError(t, err)
		require.False(t, gotJobInfo.CreateTime.IsZero())
		require.True(t, gotJobInfo.StartTime.IsZero())
		require.True(t, gotJobInfo.EndTime.IsZero())
		jobInfoEqual(t, jobInfo, gotJobInfo)
		cnt, err := importer.GetActiveJobCnt(ctx, conn, gotJobInfo.TableSchema, gotJobInfo.TableName)
		require.NoError(t, err)
		require.Equal(t, int64(1), cnt)

		// action before start, no effect
		c.action(jobID)
		gotJobInfo, err = importer.GetJob(ctx, conn, jobID, jobInfo.CreatedBy, false)
		require.NoError(t, err)
		jobInfoEqual(t, jobInfo, gotJobInfo)

		// start job
		require.NoError(t, importer.StartJob(ctx, conn, jobID, importer.JobStepImporting))
		gotJobInfo, err = importer.GetJob(ctx, conn, jobID, jobInfo.CreatedBy, false)
		require.NoError(t, err)
		require.False(t, gotJobInfo.CreateTime.IsZero())
		require.False(t, gotJobInfo.StartTime.IsZero())
		require.True(t, gotJobInfo.EndTime.IsZero())
		jobInfo.Status = "running"
		jobInfo.Step = importer.JobStepImporting
		jobInfoEqual(t, jobInfo, gotJobInfo)
		cnt, err = importer.GetActiveJobCnt(ctx, conn, gotJobInfo.TableSchema, gotJobInfo.TableName)
		require.NoError(t, err)
		require.Equal(t, int64(1), cnt)

		// change job step
		require.NoError(t, importer.Job2Step(ctx, conn, jobID, importer.JobStepValidating))
		cnt, err = importer.GetActiveJobCnt(ctx, conn, gotJobInfo.TableSchema, gotJobInfo.TableName)
		require.NoError(t, err)
		require.Equal(t, int64(1), cnt)

		// do action
		c.action(jobID)
		gotJobInfo, err = importer.GetJob(ctx, conn, jobID, jobInfo.CreatedBy, false)
		require.NoError(t, err)
		require.False(t, gotJobInfo.CreateTime.IsZero())
		require.False(t, gotJobInfo.StartTime.IsZero())
		require.False(t, gotJobInfo.EndTime.IsZero())
		jobInfo.Status = c.expectStatus
		jobInfo.Step = c.expectStep
		jobInfo.Summary = c.expectedSummary
		jobInfo.ErrorMessage = c.expectedErrMsg
		jobInfoEqual(t, jobInfo, gotJobInfo)
		cnt, err = importer.GetActiveJobCnt(ctx, conn, gotJobInfo.TableSchema, gotJobInfo.TableName)
		require.NoError(t, err)
		require.Equal(t, int64(0), cnt)

		// do action again, no effect
		endTime := gotJobInfo.EndTime
		c.action(jobID)
		gotJobInfo, err = importer.GetJob(ctx, conn, jobID, jobInfo.CreatedBy, false)
		require.NoError(t, err)
		require.Equal(t, endTime, gotJobInfo.EndTime)
	}
}

func TestGetAndCancelJob(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	conn := tk.Session().GetSQLExecutor()
	jobInfo := &importer.JobInfo{
		TableSchema: "test",
		TableName:   "t",
		TableID:     1,
		CreatedBy:   "user-for-test@%",
		Parameters: importer.ImportParameters{
			ColumnsAndVars: "(a, b, c)",
			SetClause:      "d = 1",
			Format:         importer.DataFormatCSV,
			Options: map[string]any{
				"skip_rows": float64(1), // json unmarshal will convert number to float64
				"detached":  nil,
			},
		},
		SourceFileSize: 123,
		Status:         "pending",
	}

	// create job
	jobID1, err := importer.CreateJob(ctx, conn, jobInfo.TableSchema, jobInfo.TableName, jobInfo.TableID,
		jobInfo.CreatedBy, &jobInfo.Parameters, jobInfo.SourceFileSize)
	require.NoError(t, err)
	jobInfo.ID = jobID1
	gotJobInfo, err := importer.GetJob(ctx, conn, jobID1, jobInfo.CreatedBy, false)
	require.NoError(t, err)
	require.False(t, gotJobInfo.CreateTime.IsZero())
	require.True(t, gotJobInfo.StartTime.IsZero())
	require.True(t, gotJobInfo.EndTime.IsZero())
	jobInfoEqual(t, jobInfo, gotJobInfo)
	cnt, err := importer.GetActiveJobCnt(ctx, conn, gotJobInfo.TableSchema, gotJobInfo.TableName)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	// cancel job
	require.NoError(t, importer.CancelJob(ctx, conn, jobID1))
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID1, jobInfo.CreatedBy, false)
	require.NoError(t, err)
	require.False(t, gotJobInfo.CreateTime.IsZero())
	// we don't set start/end time for canceled job
	require.True(t, gotJobInfo.StartTime.IsZero())
	require.True(t, gotJobInfo.EndTime.IsZero())
	jobInfo.Status = "cancelled"
	jobInfo.ErrorMessage = "cancelled by user"
	jobInfoEqual(t, jobInfo, gotJobInfo)
	cnt, err = importer.GetActiveJobCnt(ctx, conn, gotJobInfo.TableSchema, gotJobInfo.TableName)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)

	// call cancel twice is ok, caller should check job status before cancel.
	require.NoError(t, importer.CancelJob(ctx, conn, jobID1))

	jobInfo.Status = "pending"
	jobInfo.ErrorMessage = ""
	jobInfo.CreatedBy = "user-for-test-2@%"

	// create another job
	jobID2, err := importer.CreateJob(ctx, conn, jobInfo.TableSchema, jobInfo.TableName, jobInfo.TableID,
		jobInfo.CreatedBy, &jobInfo.Parameters, jobInfo.SourceFileSize)
	require.NoError(t, err)
	jobInfo.ID = jobID2
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID2, jobInfo.CreatedBy, false)
	require.NoError(t, err)
	require.False(t, gotJobInfo.CreateTime.IsZero())
	require.True(t, gotJobInfo.StartTime.IsZero())
	require.True(t, gotJobInfo.EndTime.IsZero())
	jobInfoEqual(t, jobInfo, gotJobInfo)

	// start job
	require.NoError(t, importer.StartJob(ctx, conn, jobID2, importer.JobStepImporting))
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID2, jobInfo.CreatedBy, false)
	require.NoError(t, err)
	require.False(t, gotJobInfo.CreateTime.IsZero())
	require.False(t, gotJobInfo.StartTime.IsZero())
	require.True(t, gotJobInfo.EndTime.IsZero())
	jobInfo.Status = "running"
	jobInfo.Step = importer.JobStepImporting
	jobInfoEqual(t, jobInfo, gotJobInfo)

	// cancel job
	require.NoError(t, importer.CancelJob(ctx, conn, jobID2))
	gotJobInfo, err = importer.GetJob(ctx, conn, jobID2, jobInfo.CreatedBy, false)
	require.NoError(t, err)
	require.False(t, gotJobInfo.CreateTime.IsZero())
	require.False(t, gotJobInfo.StartTime.IsZero())
	require.True(t, gotJobInfo.EndTime.IsZero())
	jobInfo.Status = "cancelled"
	jobInfo.ErrorMessage = "cancelled by user"
	jobInfoEqual(t, jobInfo, gotJobInfo)

	_, err = importer.GetJob(ctx, conn, 999999999, jobInfo.CreatedBy, false)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataJobNotFound)
	_, err = importer.GetJob(ctx, conn, jobID2, "aaa", false)
	require.ErrorIs(t, err, plannererrors.ErrSpecificAccessDenied)
	_, err = importer.GetJob(ctx, conn, jobID2, "aaa", true)
	require.NoError(t, err)

	// only see job created by user-for-test-2@%
	jobs, err := importer.GetAllViewableJobs(ctx, conn, "user-for-test-2@%", false)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, jobID2, jobs[0].ID)
	// with super privilege, we can see all jobs
	jobs, err = importer.GetAllViewableJobs(ctx, conn, "user-for-test-2@%", true)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	require.Equal(t, jobID1, jobs[0].ID)
	require.Equal(t, jobID2, jobs[1].ID)
}

func TestJobInfo_CanCancel(t *testing.T) {
	jobInfo := &importer.JobInfo{}
	for _, c := range []struct {
		status    string
		canCancel bool
	}{
		{"pending", true},
		{"running", true},
		{"finished", false},
		{"failed", false},
		{"canceled", false},
	} {
		jobInfo.Status = c.status
		require.Equal(t, c.canCancel, jobInfo.CanCancel(), c.status)
	}
}

func TestGetJobInfoNullField(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	conn := tk.Session().GetSQLExecutor()
	jobInfo := &importer.JobInfo{
		TableSchema: "test",
		TableName:   "t",
		TableID:     1,
		CreatedBy:   "user-for-test@%",
		Parameters: importer.ImportParameters{
			ColumnsAndVars: "(a, b, c)",
			SetClause:      "d = 1",
			Format:         importer.DataFormatCSV,
			Options: map[string]any{
				"skip_rows": float64(1), // json unmarshal will convert number to float64
				"detached":  nil,
			},
		},
		SourceFileSize: 123,
		Status:         "pending",
	}
	// create jobs
	jobID1, err := importer.CreateJob(ctx, conn, jobInfo.TableSchema, jobInfo.TableName, jobInfo.TableID,
		jobInfo.CreatedBy, &jobInfo.Parameters, jobInfo.SourceFileSize)
	require.NoError(t, err)
	require.NoError(t, importer.StartJob(ctx, conn, jobID1, importer.JobStepImporting))
	require.NoError(t, importer.FailJob(ctx, conn, jobID1, "failed"))
	jobID2, err := importer.CreateJob(ctx, conn, jobInfo.TableSchema, jobInfo.TableName, jobInfo.TableID,
		jobInfo.CreatedBy, &jobInfo.Parameters, jobInfo.SourceFileSize)
	require.NoError(t, err)
	gotJobInfos, err := importer.GetAllViewableJobs(ctx, conn, "", true)
	require.NoError(t, err)
	require.Len(t, gotJobInfos, 2)
	// result should be in order, jobID1, jobID2
	jobInfo.ID = jobID1
	jobInfo.Status = "failed"
	jobInfo.Step = importer.JobStepImporting
	jobInfo.ErrorMessage = "failed"
	jobInfoEqual(t, jobInfo, gotJobInfos[0])
	require.False(t, gotJobInfos[0].StartTime.IsZero())
	require.False(t, gotJobInfos[0].EndTime.IsZero())
	jobInfo.ID = jobID2
	jobInfo.Status = "pending"
	jobInfo.Step = ""
	// err msg of jobID2 should be empty
	jobInfo.ErrorMessage = ""
	jobInfoEqual(t, jobInfo, gotJobInfos[1])
	// start/end time of jobID2 should be zero
	require.True(t, gotJobInfos[1].StartTime.IsZero())
	require.True(t, gotJobInfos[1].EndTime.IsZero())
}
