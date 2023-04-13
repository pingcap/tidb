// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package asyncloaddata_test

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func checkEqualIgnoreTimes(t *testing.T, expected, got *JobInfo) {
	cloned := *expected
	cloned.CreateTime = got.CreateTime
	cloned.StartTime = got.StartTime
	cloned.EndTime = got.EndTime
	require.Equal(t, &cloned, got)
}

func createJob(t *testing.T, conn sqlexec.SQLExecutor, user string) (*Job, *JobInfo) {
	job, err := CreateLoadDataJob(context.Background(), conn, "/tmp/test.csv", "test", "t", "logical", user)
	require.NoError(t, err)
	info, err := job.GetJobInfo(context.Background())
	require.NoError(t, err)
	expected := &JobInfo{
		JobID:         job.ID,
		User:          user,
		DataSource:    "/tmp/test.csv",
		TableSchema:   "test",
		TableName:     "t",
		ImportMode:    "logical",
		Progress:      "",
		Status:        JobPending,
		StatusMessage: "",
	}
	checkEqualIgnoreTimes(t, expected, info)
	return job, info
}

func TestHappyPath(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()

	// job is created

	job, expected := createJob(t, tk.Session(), "user")

	// job is started by a worker

	backup := OfflineThresholdInSec
	OfflineThresholdInSec = 1000
	t.Cleanup(func() {
		OfflineThresholdInSec = backup
	})
	err := job.StartJob(ctx)
	require.NoError(t, err)
	info, err := job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobRunning
	checkEqualIgnoreTimes(t, expected, info)

	// job is periodically updated by worker

	ok, err := job.UpdateJobProgress(ctx, "imported 10%")
	require.NoError(t, err)
	require.True(t, ok)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Progress = "imported 10%"
	checkEqualIgnoreTimes(t, expected, info)

	// job is paused

	err = UpdateJobExpectedStatus(ctx, tk.Session(), job.ID, JobExpectedPaused)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobPaused
	checkEqualIgnoreTimes(t, expected, info)

	// worker still can update progress, maybe response to pausing is delayed

	ok, err = job.UpdateJobProgress(ctx, "imported 20%")
	require.NoError(t, err)
	require.True(t, ok)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Progress = "imported 20%"
	checkEqualIgnoreTimes(t, expected, info)

	// job is resumed

	err = UpdateJobExpectedStatus(ctx, tk.Session(), job.ID, JobExpectedRunning)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobRunning
	checkEqualIgnoreTimes(t, expected, info)

	// job is finished

	err = job.FinishJob(ctx, "finished message")
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobFinished
	expected.StatusMessage = "finished message"
	checkEqualIgnoreTimes(t, expected, info)
}

func TestKeepAlive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()

	// job is created

	job, expected := createJob(t, tk.Session(), "user")

	backup := OfflineThresholdInSec
	OfflineThresholdInSec = 1
	t.Cleanup(func() {
		OfflineThresholdInSec = backup
	})

	// before job is started, worker don't need to keepalive
	// TODO:👆not correct!

	time.Sleep(2 * time.Second)
	info, err := job.GetJobInfo(ctx)
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)

	err = job.StartJob(ctx)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobRunning
	checkEqualIgnoreTimes(t, expected, info)

	// if worker failed to keepalive, job will fail

	time.Sleep(2 * time.Second)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobFailed
	expected.StatusMessage = "job expected running but the node is timeout"
	checkEqualIgnoreTimes(t, expected, info)

	// after the worker is failed to keepalive, further keepalive will fail

	ok, err := job.UpdateJobProgress(ctx, "imported 20%")
	require.NoError(t, err)
	require.False(t, ok)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)

	// when worker fails to keepalive, before it calls FailJob, it still can
	// change expected status to some extent.

	err = UpdateJobExpectedStatus(ctx, tk.Session(), job.ID, JobExpectedPaused)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.StatusMessage = "job expected paused but the node is timeout"
	checkEqualIgnoreTimes(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), job.ID, JobExpectedRunning)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.StatusMessage = "job expected running but the node is timeout"
	checkEqualIgnoreTimes(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), job.ID, JobExpectedCanceled)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobCanceled
	expected.StatusMessage = ""
	checkEqualIgnoreTimes(t, expected, info)

	// Now the worker calls FailJob, but the status should still be canceled,
	// that's more friendly.

	err = job.FailJob(ctx, "failed to keepalive")
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobCanceled
	expected.StatusMessage = "failed to keepalive"
	checkEqualIgnoreTimes(t, expected, info)
}

func TestJobIsFailedAndGetAllJobs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()

	// job is created

	job, expected := createJob(t, tk.Session(), "user")

	// job can be failed directly when it's pending

	err := job.FailJob(ctx, "failed message")
	require.NoError(t, err)
	info, err := job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobFailed
	expected.StatusMessage = "failed message"
	checkEqualIgnoreTimes(t, expected, info)

	// create another job and fail it

	job, expected = createJob(t, tk.Session(), "user")

	err = job.StartJob(ctx)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobRunning
	checkEqualIgnoreTimes(t, expected, info)

	err = job.FailJob(ctx, "failed message")
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	expected.Status = JobFailed
	expected.StatusMessage = "failed message"
	checkEqualIgnoreTimes(t, expected, info)

	// test change expected status of a failed job.

	err = UpdateJobExpectedStatus(ctx, tk.Session(), job.ID, JobExpectedPaused)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), job.ID, JobExpectedRunning)
	require.NoError(t, err)
	info, err = job.GetJobInfo(ctx)
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)
	err = job.CancelJob(ctx)
	require.ErrorContains(t, err, "The current job status cannot perform the operation. need status running or paused, but got failed")

	// add job of another user and test GetAllJobInfo

	job, _ = createJob(t, tk.Session(), "user2")

	jobs, err := GetAllJobInfo(ctx, tk.Session(), "user")
	require.NoError(t, err)
	require.Equal(t, 2, len(jobs))
	require.Equal(t, JobFailed, jobs[0].Status)
	require.Equal(t, JobFailed, jobs[1].Status)

	jobs, err = GetAllJobInfo(ctx, tk.Session(), "user2")
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	require.Equal(t, JobPending, jobs[0].Status)
	require.Equal(t, job.ID, jobs[0].JobID)

	job.User = "wrong_user"
	_, err = job.GetJobInfo(ctx)
	require.ErrorContains(t, err, "doesn't exist")
}

func TestGetJobStatus(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()

	// job is created

	job, _ := createJob(t, tk.Session(), "user")

	// job is pending

	status, msg, err := job.GetJobStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, JobPending, status)
	require.Equal(t, "", msg)

	// job is running

	backup := OfflineThresholdInSec
	OfflineThresholdInSec = 1000
	t.Cleanup(func() {
		OfflineThresholdInSec = backup
	})
	err = job.StartJob(ctx)
	require.NoError(t, err)
	status, msg, err = job.GetJobStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, JobRunning, status)
	require.Equal(t, "", msg)

	// job is finished

	err = job.FinishJob(ctx, "finished message")
	require.NoError(t, err)
	status, msg, err = job.GetJobStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, JobFinished, status)
	require.Equal(t, "finished message", msg)

	// wrong ID

	job.ID += 1
	status, msg, err = job.GetJobStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, JobFailed, status)
	require.Contains(t, msg, "doesn't exist")
}

func TestCreateLoadDataJobRedact(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()

	_, err := CreateLoadDataJob(ctx, tk.Session(),
		"s3://bucket/a.csv?access-key=hideme&secret-access-key=hideme",
		"db", "table", "mode", "user")
	require.NoError(t, err)
	result := tk.MustQuery("SELECT * FROM mysql.load_data_jobs;")
	result.CheckContain("a.csv")
	result.CheckNotContain("hideme")
}
