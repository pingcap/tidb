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

package asyncloaddata

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestHappyPath(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(CreateLoadDataJobs)
	defer tk.MustExec("DROP TABLE IF EXISTS mysql.load_data_jobs")

	ctx := context.Background()

	// job is created

	id, err := CreateLoadDataJob(ctx, tk.Session(), "/tmp/test.csv", "test", "t", "logical", "user")
	require.NoError(t, err)
	info, err := GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected := &JobInfo{
		JobID:         id,
		User:          "user",
		DataSource:    "/tmp/test.csv",
		TableSchema:   "test",
		TableName:     "t",
		ImportMode:    "logical",
		Progress:      "",
		Status:        JobPending,
		StatusMessage: "",
	}
	require.Equal(t, expected, info)

	// job is started by a worker

	backup := OfflineThresholdInSec
	OfflineThresholdInSec = 1000
	t.Cleanup(func() {
		OfflineThresholdInSec = backup
	})
	err = StartJob(ctx, tk.Session(), id)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Status = JobRunning
	require.Equal(t, expected, info)

	// job is periodically updated by worker

	ok, err := UpdateJobProgress(ctx, tk.Session(), id, "imported 10%")
	require.NoError(t, err)
	require.True(t, ok)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Progress = "imported 10%"
	require.Equal(t, expected, info)

	// job is paused

	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedPaused)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Status = JobPaused
	require.Equal(t, expected, info)

	// worker still can update progress, maybe response to pausing is delayed

	ok, err = UpdateJobProgress(ctx, tk.Session(), id, "imported 20%")
	require.NoError(t, err)
	require.True(t, ok)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Progress = "imported 20%"
	require.Equal(t, expected, info)

	// job is resumed

	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedRunning)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Status = JobRunning
	require.Equal(t, expected, info)

	// job is finished

	err = FinishJob(ctx, tk.Session(), id, "finished message")
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Status = JobFinished
	expected.StatusMessage = "finished message"
	require.Equal(t, expected, info)
}

func TestKeepAlive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(CreateLoadDataJobs)
	defer tk.MustExec("DROP TABLE IF EXISTS mysql.load_data_jobs")
	ctx := context.Background()

	// job is created

	id, err := CreateLoadDataJob(ctx, tk.Session(), "/tmp/test.csv", "test", "t", "logical", "user")
	require.NoError(t, err)
	info, err := GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected := &JobInfo{
		JobID:         id,
		User:          "user",
		DataSource:    "/tmp/test.csv",
		TableSchema:   "test",
		TableName:     "t",
		ImportMode:    "logical",
		Progress:      "",
		Status:        JobPending,
		StatusMessage: "",
	}
	require.Equal(t, expected, info)

	backup := OfflineThresholdInSec
	OfflineThresholdInSec = 1
	t.Cleanup(func() {
		OfflineThresholdInSec = backup
	})

	// before job is started, worker don't need to keepalive

	time.Sleep(2 * time.Second)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	require.Equal(t, expected, info)

	err = StartJob(ctx, tk.Session(), id)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Status = JobRunning
	require.Equal(t, expected, info)

	// if worker failed to keepalive, job will fail

	time.Sleep(2 * time.Second)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Status = JobFailed
	expected.StatusMessage = "job expected running but the node is timeout"
	require.Equal(t, expected, info)

	// after the worker is failed to keepalive, further keepalive will fail

	ok, err := UpdateJobProgress(ctx, tk.Session(), id, "imported 20%")
	require.NoError(t, err)
	require.False(t, ok)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	require.Equal(t, expected, info)

	// can change expected status

	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedPaused)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.StatusMessage = "job expected paused but the node is timeout"
	require.Equal(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedRunning)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.StatusMessage = "job expected running but the node is timeout"
	require.Equal(t, expected, info)
	err = UpdateJobExpectedStatus(ctx, tk.Session(), id, JobExpectedCanceled)
	require.NoError(t, err)
	info, err = GetJobInfo(ctx, tk.Session(), id)
	require.NoError(t, err)
	expected.Status = JobCanceled
	expected.StatusMessage = ""
	require.Equal(t, expected, info)
}
