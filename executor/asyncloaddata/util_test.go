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

func createJob(t *testing.T, conn sqlexec.SQLExecutor, user string) (int64, *JobInfo) {
	id, err := CreateLoadDataJob(context.Background(), conn, "/tmp/test.csv", "test", "t", "logical", user)
	require.NoError(t, err)
	info, err := GetJobInfo(context.Background(), conn, id, user)
	require.NoError(t, err)
	expected := &JobInfo{
		JobID:         id,
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
	return id, info
}

func TestHappyPath(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(CreateLoadDataJobs)
	defer tk.MustExec("DROP TABLE IF EXISTS mysql.load_data_jobs")

	ctx := context.Background()

	// job is created

	id, expected := createJob(t, tk.Session(), "user")

	// job is started by a worker

	backup := OfflineThresholdInSec
	OfflineThresholdInSec = 1000
	t.Cleanup(func() {
		OfflineThresholdInSec = backup
	})
	err := StartJob(ctx, tk.Session(), id)
	require.NoError(t, err)
	info, err := GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobRunning
	checkEqualIgnoreTimes(t, expected, info)

	// job is periodically updated by worker

	ok, err := UpdateJobProgress(ctx, tk.Session(), id, "imported 10%")
	require.NoError(t, err)
	require.True(t, ok)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Progress = "imported 10%"
	checkEqualIgnoreTimes(t, expected, info)

	// job is paused

	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedPaused)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobPaused
	checkEqualIgnoreTimes(t, expected, info)

	// worker still can update progress, maybe response to pausing is delayed

	ok, err = UpdateJobProgress(ctx, tk.Session(), id, "imported 20%")
	require.NoError(t, err)
	require.True(t, ok)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Progress = "imported 20%"
	checkEqualIgnoreTimes(t, expected, info)

	// job is resumed

	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedRunning)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobRunning
	checkEqualIgnoreTimes(t, expected, info)

	// job is finished

	err = FinishJob(ctx, tk.Session(), id, "finished message")
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobFinished
	expected.StatusMessage = "finished message"
	checkEqualIgnoreTimes(t, expected, info)
}

func TestKeepAlive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(CreateLoadDataJobs)
	defer tk.MustExec("DROP TABLE IF EXISTS mysql.load_data_jobs")
	ctx := context.Background()

	// job is created

	id, expected := createJob(t, tk.Session(), "user")

	backup := OfflineThresholdInSec
	OfflineThresholdInSec = 1
	t.Cleanup(func() {
		OfflineThresholdInSec = backup
	})

	// before job is started, worker don't need to keepalive
	// TODO:👆not correct!

	time.Sleep(2 * time.Second)
	info, err := GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)

	err = StartJob(ctx, tk.Session(), id)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobRunning
	checkEqualIgnoreTimes(t, expected, info)

	// if worker failed to keepalive, job will fail

	time.Sleep(2 * time.Second)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobFailed
	expected.StatusMessage = "job expected running but the node is timeout"
	checkEqualIgnoreTimes(t, expected, info)

	// after the worker is failed to keepalive, further keepalive will fail

	ok, err := UpdateJobProgress(ctx, tk.Session(), id, "imported 20%")
	require.NoError(t, err)
	require.False(t, ok)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)

	// when worker fails to keepalive, before it calls FailJob, it still can
	// change expected status to some extent.

	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedPaused)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.StatusMessage = "job expected paused but the node is timeout"
	checkEqualIgnoreTimes(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedRunning)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.StatusMessage = "job expected running but the node is timeout"
	checkEqualIgnoreTimes(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedCanceled)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobCanceled
	expected.StatusMessage = ""
	checkEqualIgnoreTimes(t, expected, info)

	// Now the worker calls FailJob

	err = FailJob(ctx, tk.Session(), id, "failed to keepalive")
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobFailed
	expected.StatusMessage = "failed to keepalive"
	checkEqualIgnoreTimes(t, expected, info)
}

func TestJobIsFailedAndGetAllJobs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(CreateLoadDataJobs)
	defer tk.MustExec("DROP TABLE IF EXISTS mysql.load_data_jobs")
	ctx := context.Background()

	// job is created

	id, expected := createJob(t, tk.Session(), "user")

	// job can be failed directly when it's pending

	err := FailJob(ctx, tk.Session(), id, "failed message")
	require.NoError(t, err)
	info, err := GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobFailed
	expected.StatusMessage = "failed message"
	checkEqualIgnoreTimes(t, expected, info)

	// create another job and fail it

	id, expected = createJob(t, tk.Session(), "user")

	err = StartJob(ctx, tk.Session(), id)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobRunning
	checkEqualIgnoreTimes(t, expected, info)

	err = FailJob(ctx, tk.Session(), id, "failed message")
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	expected.Status = JobFailed
	expected.StatusMessage = "failed message"
	checkEqualIgnoreTimes(t, expected, info)

	// test change expected status of a failed job.

	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedPaused)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedRunning)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedCanceled)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id, "user")
	require.NoError(t, err)
	checkEqualIgnoreTimes(t, expected, info)

	// add job of another user and test GetAllJobInfo

	_, _ = createJob(t, tk.Session(), "user2")

	jobs, err := GetAllJobInfo(ctx, tk.Session(), "user")
	require.NoError(t, err)
	require.Equal(t, 2, len(jobs))
	require.Equal(t, JobFailed, jobs[0].Status)
	require.Equal(t, JobFailed, jobs[1].Status)

	jobs, err = GetAllJobInfo(ctx, tk.Session(), "user2")
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	require.Equal(t, JobPending, jobs[0].Status)

	_, err = GetJobInfo(ctx, tk.Session(), jobs[0].JobID, "wrong_user")
	require.ErrorContains(t, err, "not found")
}

func TestGetJobStatus(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(CreateLoadDataJobs)
	defer tk.MustExec("DROP TABLE IF EXISTS mysql.load_data_jobs")
	ctx := context.Background()

	// job is created

	id, _ := createJob(t, tk.Session(), "user")

	// job is pending

	status, msg, err := GetJobStatus(ctx, tk.Session(), id)
	require.NoError(t, err)
	require.Equal(t, JobPending, status)
	require.Equal(t, "", msg)

	// job is running

	backup := OfflineThresholdInSec
	OfflineThresholdInSec = 1000
	t.Cleanup(func() {
		OfflineThresholdInSec = backup
	})
	err = StartJob(ctx, tk.Session(), id)
	require.NoError(t, err)
	status, msg, err = GetJobStatus(ctx, tk.Session(), id)
	require.NoError(t, err)
	require.Equal(t, JobRunning, status)
	require.Equal(t, "", msg)

	// job is finished

	err = FinishJob(ctx, tk.Session(), id, "finished message")
	require.NoError(t, err)
	status, msg, err = GetJobStatus(ctx, tk.Session(), id)
	require.NoError(t, err)
	require.Equal(t, JobFinished, status)
	require.Equal(t, "finished message", msg)

	// wrong ID

	status, msg, err = GetJobStatus(ctx, tk.Session(), id+1)
	require.NoError(t, err)
	require.Equal(t, JobFailed, status)
	require.Contains(t, msg, "not found")
}
