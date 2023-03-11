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
	"fmt"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
)

const (
	// CreateLoadDataJobs is a table that LOAD DATA uses
	// TODO: move it to bootstrap.go and create it in bootstrap
	CreateLoadDataJobs = `CREATE TABLE IF NOT EXISTS mysql.load_data_jobs (
       job_id bigint(64) NOT NULL AUTO_INCREMENT,
       expected_status ENUM('running', 'paused', 'canceled') NOT NULL DEFAULT 'running',
       create_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
       start_time TIMESTAMP(6) NULL DEFAULT NULL,
       update_time TIMESTAMP(6) NULL DEFAULT NULL,
       end_time TIMESTAMP(6) NULL DEFAULT NULL,
       data_source TEXT NOT NULL,
       table_schema VARCHAR(64) NOT NULL,
       table_name VARCHAR(64) NOT NULL,
       import_mode VARCHAR(64) NOT NULL,
       create_user VARCHAR(32) NOT NULL,
       progress TEXT DEFAULT NULL,
       result_message TEXT DEFAULT NULL,
       error_message TEXT DEFAULT NULL,
       PRIMARY KEY (job_id),
       KEY (create_time),
       KEY (create_user));`
)

// CreateLoadDataJob creates a load data job.
func CreateLoadDataJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	dataSource, db, table string,
	importMode string,
	user string,
) (int64, error) {
	// remove the params in data source URI because it may contains AK/SK
	// TODO: add UT
	u, err := url.Parse(dataSource)
	if err == nil && u.Scheme != "" {
		u.RawQuery = ""
		u.Fragment = ""
		dataSource = u.String()
	}
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err = conn.ExecuteInternal(ctx,
		`INSERT INTO mysql.load_data_jobs
    	(data_source, table_schema, table_name, import_mode, create_user)
		VALUES (%?, %?, %?, %?, %?);`,
		dataSource, db, table, importMode, user)
	if err != nil {
		return 0, err
	}
	rs, err := conn.ExecuteInternal(ctx, `SELECT LAST_INSERT_ID();`)
	if err != nil {
		return 0, err
	}
	//nolint: errcheck
	defer rs.Close()
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, errors.Errorf("unexpected result length: %d", len(rows))
	}
	return rows[0].GetInt64(0), nil
}

// StartJob starts a load data job. A job can only be started once.
func StartJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err := conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET start_time = CURRENT_TIMESTAMP(6), update_time = CURRENT_TIMESTAMP(6)
		WHERE job_id = %? AND start_time IS NULL AND end_time IS NULL;`,
		jobID)
	return err
}

var (
	// HeartBeatInSec is the interval of heartbeat.
	HeartBeatInSec = 5
	// OfflineThresholdInSec means after failing to update heartbeat for 3 times,
	// we treat the worker of the job as offline.
	OfflineThresholdInSec = HeartBeatInSec * 3
)

// UpdateJobProgress updates the progress of a load data job. It should be called
// periodically as heartbeat.
// The returned bool indicates whether the keepalive is succeeded. If not, the
// caller should call FailJob soon.
// TODO: this is only called after StartJob. What if the node is crashed when pending?
func UpdateJobProgress(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	progress string,
) (bool, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	// let TiDB handle heartbeat check for concurrent SQL
	// we tolerate 2 times of failure/timeout when updating heartbeat
	_, err := conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET progress = %?, update_time = CURRENT_TIMESTAMP(6)
		WHERE job_id = %?
			AND (update_time >= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL %? SECOND)
				OR update_time IS NULL);`,
		progress, jobID, OfflineThresholdInSec)
	if err != nil {
		return false, err
	}
	return conn.GetSessionVars().StmtCtx.AffectedRows() == 1, nil
}

// FinishJob finishes a load data job. A job can only be finished once.
func FinishJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	result string,
) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err := conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET end_time = CURRENT_TIMESTAMP(6), result_message = %?
		WHERE job_id = %? AND result_message IS NULL AND error_message IS NULL;`,
		result, jobID)
	return err
}

// FailJob fails a load data job. A job can only be failed once.
func FailJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	result string,
) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err := conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET end_time = CURRENT_TIMESTAMP(6), error_message = %?
		WHERE job_id = %? AND result_message IS NULL AND error_message IS NULL;`,
		result, jobID)
	return err
}

// JobExpectedStatus is the expected status of a load data job. User can set the
// expected status of a job and worker will respect it.
type JobExpectedStatus int

const (
	// JobExpectedRunning means the job is expected to be running.
	JobExpectedRunning JobExpectedStatus = iota
	// JobExpectedPaused means the job is expected to be paused.
	JobExpectedPaused
	// JobExpectedCanceled means the job is expected to be canceled.
	JobExpectedCanceled
)

// UpdateJobExpectedStatus updates the expected status of a load data job.
func UpdateJobExpectedStatus(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	status JobExpectedStatus,
) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	var sql string
	switch status {
	case JobExpectedRunning:
		sql = `UPDATE mysql.load_data_jobs
			SET expected_status = 'running'
			WHERE job_id = %? AND expected_status = 'paused';`
	case JobExpectedPaused:
		sql = `UPDATE mysql.load_data_jobs
			SET expected_status = 'paused'
			WHERE job_id = %? AND expected_status = 'running';`
	case JobExpectedCanceled:
		sql = `UPDATE mysql.load_data_jobs
			SET expected_status = 'canceled'
			WHERE job_id = %? AND expected_status != 'canceled';`
	}
	_, err := conn.ExecuteInternal(ctx, sql, jobID)
	return err
}

// JobStatus represents the status of a load data job.
type JobStatus int

const (
	// JobFailed means the job is failed and can't be resumed.
	JobFailed JobStatus = iota
	// JobCanceled means the job is canceled by user and can't be resumed. It
	// will finally convert to JobFailed with a message indicating the reason
	// is canceled.
	JobCanceled
	// JobPaused means the job is paused by user and can be resumed.
	JobPaused
	// JobFinished means the job is finished.
	JobFinished
	// JobPending means the job is pending to be started.
	JobPending
	// JobRunning means the job is running.
	JobRunning
)

func (s JobStatus) String() string {
	switch s {
	case JobFailed:
		return "failed"
	case JobCanceled:
		return "canceled"
	case JobPaused:
		return "paused"
	case JobFinished:
		return "finished"
	case JobPending:
		return "pending"
	case JobRunning:
		return "running"
	default:
		return "unknown JobStatus"
	}
}

// GetJobStatus gets the status of a load data job. The returned error means
// something wrong when querying the database. Other business logic errors are
// returned as JobFailed with message.
func GetJobStatus(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
) (JobStatus, string, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	rs, err := conn.ExecuteInternal(ctx,
		`SELECT
		expected_status,
		update_time >= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL %? SECOND) AS is_alive,
		end_time,
		result_message,
		error_message,
		start_time
		FROM mysql.load_data_jobs
		WHERE job_id = %?;`,
		OfflineThresholdInSec, jobID)
	if err != nil {
		return JobFailed, "", err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return JobFailed, "", err
	}
	if len(rows) != 1 {
		return JobFailed, fmt.Sprintf("job %d not found", jobID), nil
	}

	return getJobStatus(rows[0])
}

// getJobStatus expected the first 6 columns of input row is (expected_status,
// is_alive (derived from update_time), end_time, result_message, error_message,
// start_time).
func getJobStatus(row chunk.Row) (JobStatus, string, error) {
	// ending status has the highest priority
	endTimeIsNull := row.IsNull(2)
	if !endTimeIsNull {
		resultMsgIsNull := row.IsNull(3)
		if !resultMsgIsNull {
			resultMessage := row.GetString(3)
			return JobFinished, resultMessage, nil
		}
		errorMessage := row.GetString(4)
		return JobFailed, errorMessage, nil
	}

	isAlive := row.GetInt64(1) == 1
	startTimeIsNull := row.IsNull(5)
	expectedStatus := row.GetEnum(0).String()

	switch expectedStatus {
	case "canceled":
		return JobCanceled, "", nil
	case "paused":
		if startTimeIsNull || isAlive {
			return JobPaused, "", nil
		}
		return JobFailed, "job expected paused but the node is timeout", nil
	case "running":
		if startTimeIsNull {
			return JobPending, "", nil
		}
		if isAlive {
			return JobRunning, "", nil
		}
		return JobFailed, "job expected running but the node is timeout", nil
	default:
		return JobFailed, fmt.Sprintf("unexpected job status %s", expectedStatus), nil
	}
}

// JobInfo is the information of a load data job.
type JobInfo struct {
	JobID         int64
	User          string
	DataSource    string
	TableSchema   string
	TableName     string
	ImportMode    string
	Progress      string
	Status        JobStatus
	StatusMessage string
	CreateTime    types.Time
	StartTime     types.Time
	EndTime       types.Time
}

// GetJobInfo gets all needed information of a load data job.
func GetJobInfo(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	user string,
) (*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	rs, err := conn.ExecuteInternal(ctx,
		`SELECT
		expected_status,
		update_time >= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL %? SECOND) AS is_alive,
		end_time,
		result_message,
		error_message,
		start_time,

		job_id,
		data_source,
		table_schema,
		table_name,
		import_mode,
		progress,
		create_user,
		create_time
		FROM mysql.load_data_jobs
		WHERE job_id = %? AND create_user = %?;`,
		OfflineThresholdInSec, jobID, user)
	if err != nil {
		return nil, err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		// TODO: align error class
		// better pass user here to simplify
		return nil, fmt.Errorf("job %d not found", jobID)
	}

	return getJobInfo(rows[0])
}

// getJobInfo expected the columns of input row is (expected_status,
// is_alive (derived from update_time), end_time, result_message, error_message,
// start_time, job_id, data_source, table_schema, table_name, import_mode,
// progress, create_user).
func getJobInfo(row chunk.Row) (*JobInfo, error) {
	var err error
	jobInfo := JobInfo{
		JobID:       row.GetInt64(6),
		DataSource:  row.GetString(7),
		TableSchema: row.GetString(8),
		TableName:   row.GetString(9),
		ImportMode:  row.GetString(10),
		Progress:    row.GetString(11),
		User:        row.GetString(12),
		CreateTime:  row.GetTime(13),
		StartTime:   row.GetTime(5),
		EndTime:     row.GetTime(2),
	}
	jobInfo.Status, jobInfo.StatusMessage, err = getJobStatus(row)
	if err != nil {
		return nil, err
	}
	return &jobInfo, nil
}

// GetAllJobInfo gets all jobs status of a user.
func GetAllJobInfo(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	user string,
) ([]*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	rs, err := conn.ExecuteInternal(ctx,
		`SELECT
		expected_status,
		update_time >= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL %? SECOND) AS is_alive,
		end_time,
		result_message,
		error_message,
		start_time,

		job_id,
		data_source,
		table_schema,
		table_name,
		import_mode,
		progress,
		create_user,
		create_time
		FROM mysql.load_data_jobs
		WHERE create_user = %?;`,
		OfflineThresholdInSec, user)
	if err != nil {
		return nil, err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return nil, err
	}
	ret := make([]*JobInfo, 0, len(rows))
	for _, row := range rows {
		jobInfo, err := getJobInfo(row)
		if err != nil {
			return nil, err
		}
		ret = append(ret, jobInfo)
	}

	return ret, nil
}
