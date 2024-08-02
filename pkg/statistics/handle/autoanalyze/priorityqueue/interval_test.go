// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package priorityqueue_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGetAverageAnalysisDuration(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	// Empty table.
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	avgDuration, err := priorityqueue.GetAverageAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition",
	)
	require.NoError(t, err)
	require.Equal(t, time.Duration(priorityqueue.NoRecord), avgDuration)

	initJobs(tk)

	// Partitioned table.
	insertMultipleFinishedJobs(tk, "example_table", "example_partition")
	// Only one partition.
	avgDuration, err = priorityqueue.GetAverageAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition",
	)
	require.NoError(t, err)
	require.Equal(t, time.Duration(3600)*time.Second, avgDuration)
	// Multiple partitions.
	avgDuration, err = priorityqueue.GetAverageAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition", "example_partition1",
	)
	require.NoError(t, err)
	require.Equal(t, time.Duration(3600)*time.Second, avgDuration)
	// Non-partitioned table.
	insertMultipleFinishedJobs(tk, "example_table1", "")
	avgDuration, err = priorityqueue.GetAverageAnalysisDuration(sctx, "example_schema", "example_table1")
	require.NoError(t, err)
	require.Equal(t, time.Duration(3600)*time.Second, avgDuration)
}

func insertMultipleFinishedJobs(tk *testkit.TestKit, tableName string, partitionName string) {
	jobs := []struct {
		dbName        string
		tableName     string
		partitionName string
		startTime     string
		endTime       string
	}{
		// This is a special case its duration is 30 minutes.
		// But the id is smaller than the following records.
		// So it will not be selected.
		{"example_schema", tableName, partitionName, "2022-01-01 7:00:00", "2022-01-01 7:30:00"},
		// Other records.
		{"example_schema", tableName, partitionName, "2022-01-01 09:00:00", "2022-01-01 10:00:00"},
		{"example_schema", tableName, partitionName, "2022-01-01 10:00:00", "2022-01-01 11:00:00"},
		{"example_schema", tableName, partitionName, "2022-01-01 11:00:00", "2022-01-01 12:00:00"},
		{"example_schema", tableName, partitionName, "2022-01-01 13:00:00", "2022-01-01 14:00:00"},
		{"example_schema", tableName, partitionName, "2022-01-01 14:00:00", "2022-01-01 15:00:00"},
	}

	for _, job := range jobs {
		insertFinishedJob(tk, job.dbName, job.tableName, job.partitionName, job.startTime, job.endTime)
	}
}

func TestGetLastFailedAnalysisDuration(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	// Empty table.
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	lastFailedDuration, err := priorityqueue.GetLastFailedAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition",
	)
	require.NoError(t, err)
	require.Equal(t, time.Duration(priorityqueue.NoRecord), lastFailedDuration)
	initJobs(tk)

	// Partitioned table.
	insertFailedJob(tk, "example_schema", "example_table", "example_partition")
	insertFailedJob(tk, "example_schema", "example_table", "example_partition1")
	// Only one partition.
	lastFailedDuration, err = priorityqueue.GetLastFailedAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition",
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastFailedDuration, time.Duration(24)*time.Hour)
	// Multiple partitions.
	lastFailedDuration, err = priorityqueue.GetLastFailedAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition", "example_partition1",
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastFailedDuration, time.Duration(24)*time.Hour)
	// Non-partitioned table.
	insertFailedJob(tk, "example_schema", "example_table1", "")
	lastFailedDuration, err = priorityqueue.GetLastFailedAnalysisDuration(sctx, "example_schema", "example_table1")
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastFailedDuration, time.Duration(24)*time.Hour)
}

func initJobs(tk *testkit.TestKit) {
	tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		partition_name,
		job_info,
		start_time,
		state,
		instance
	) VALUES (
		'example_schema',
		'example_table',
		'example_partition',
		'Job information for pending job',
		NULL,
		'pending',
		'example_instance'
	);
`)

	tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		partition_name,
		job_info,
		start_time,
		state,
		instance
	) VALUES (
		'example_schema',
		'example_table',
		'example_partition',
		'Job information for running job',
		'2022-01-01 10:00:00',
		'running',
		'example_instance'
	);
`)

	tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		partition_name,
		job_info,
		start_time,
		end_time,
		state,
		fail_reason,
		instance
	) VALUES (
		'example_schema',
		'example_table',
		'example_partition',
		'Job information for failed job',
		'2022-01-01 09:00:00',
		'2022-01-01 10:00:00',
		'failed',
		'Some reason for failure',
		'example_instance'
	);
`)
}

func insertFinishedJob(
	tk *testkit.TestKit,
	dbName string,
	tableName string,
	partitionName string,
	startTime string,
	endTime string,
) {
	if partitionName == "" {
		tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		job_info,
		start_time,
		end_time,
		state,
		instance
	) VALUES (
		?,
		?,
		'Job information for finished job',
		?,
		?,
		'finished',
		'example_instance'
	);
		`,
			dbName,
			tableName,
			startTime,
			endTime,
		)
	} else {
		tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		partition_name,
		job_info,
		start_time,
		end_time,
		state,
		instance
	) VALUES (
		?,
		?,
		?,
		'Job information for finished job',
		?,
		?,
		'finished',
		'example_instance'
	);
		`,
			dbName,
			tableName,
			partitionName,
			startTime,
			endTime,
		)
	}
}

func insertFailedJob(
	tk *testkit.TestKit,
	dbName string,
	tableName string,
	partitionName string,
) {
	if partitionName == "" {
		tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		job_info,
		start_time,
		end_time,
		state,
		fail_reason,
		instance
	) VALUES (
		?,
		?,
		'Job information for failed job',
		'2024-01-01 09:00:00',
		'2024-01-01 10:00:00',
		'failed',
		'Some reason for failure',
		'example_instance'
	);
		`,
			dbName,
			tableName,
		)
	} else {
		tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		partition_name,
		job_info,
		start_time,
		end_time,
		state,
		fail_reason,
		instance
	) VALUES (
		?,
		?,
		?,
		'Job information for failed job',
		'2024-01-01 09:00:00',
		'2024-01-01 10:00:00',
		'failed',
		'Some reason for failure',
		'example_instance'
	);
		`,
			dbName,
			tableName,
			partitionName,
		)
	}
}

func insertFailedJobWithStartTime(
	tk *testkit.TestKit,
	dbName string,
	tableName string,
	partitionName string,
	startTime string,
) {
	if partitionName == "" {
		tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		job_info,
		start_time,
		end_time,
		state,
		fail_reason,
		instance
	) VALUES (
		?,
		?,
		'Job information for failed job',
		?,
		'2024-01-01 10:00:00',
		'failed',
		'Some reason for failure',
		'example_instance'
	);
		`,
			dbName,
			tableName,
			startTime,
		)
	} else {
		tk.MustExec(`
	INSERT INTO mysql.analyze_jobs (
		table_schema,
		table_name,
		partition_name,
		job_info,
		start_time,
		end_time,
		state,
		fail_reason,
		instance
	) VALUES (
		?,
		?,
		?,
		'Job information for failed job',
		?,
		'2024-01-01 10:00:00',
		'failed',
		'Some reason for failure',
		'example_instance'
	);
		`,
			dbName,
			tableName,
			partitionName,
			startTime,
		)
	}
}
