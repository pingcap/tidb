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

package analyzequeue

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGetAverageAnalysisDuration(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	initJobs(tk)

	// Partitioned table.
	insertFinishedJob(tk, "example_schema", "example_table", "example_partition")
	insertFinishedJob(tk, "example_schema", "example_table", "example_partition1")
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	// Only one partition.
	avgDuration, err := getAverageAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition",
	)
	require.NoError(t, err)
	require.Equal(t, time.Duration(3600)*time.Second, avgDuration)
	// Multiple partitions.
	avgDuration, err = getAverageAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition", "example_partition1",
	)
	require.NoError(t, err)
	require.Equal(t, time.Duration(3600)*time.Second, avgDuration)
	// Non-partitioned table.
	insertFinishedJob(tk, "example_schema1", "example_table1", "")
	avgDuration, err = getAverageAnalysisDuration(sctx, "example_schema1", "example_table1", "")
	require.NoError(t, err)
	require.Equal(t, time.Duration(3600)*time.Second, avgDuration)
}

func TestGetLastFailedAnalysisDuration(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(session.CreateAnalyzeJobs)
	initJobs(tk)

	// Partitioned table.
	insertFailedJob(tk, "example_schema", "example_table", "example_partition")
	insertFailedJob(tk, "example_schema", "example_table", "example_partition1")
	se := tk.Session()
	sctx := se.(sessionctx.Context)
	// Only one partition.
	lastFailedDuration, err := getLastFailedAnalysisDuration(sctx, "example_schema", "example_table", "example_partition")
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastFailedDuration, time.Duration(24)*time.Hour)
	// Multiple partitions.
	lastFailedDuration, err = getLastFailedAnalysisDuration(
		sctx,
		"example_schema", "example_table", "example_partition", "example_partition1",
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastFailedDuration, time.Duration(24)*time.Hour)
	// Non-partitioned table.
	insertFailedJob(tk, "example_schema1", "example_table1", "")
	lastFailedDuration, err = getLastFailedAnalysisDuration(sctx, "example_schema1", "example_table1", "")
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
		'2022-01-01 09:00:00',
		'2022-01-01 10:00:00',
		'finished',
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
		instance
	) VALUES (
		?,
		?,
		?,	
		'Job information for finished job',
		'2022-01-01 09:00:00',
		'2022-01-01 10:00:00',
		'finished',
		'example_instance'
	);
		`,
			dbName,
			tableName,
			partitionName,
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
