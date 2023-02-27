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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	// CreateLoadDataJobs is a table that LOAD DATA uses
	// TODO: move it to bootstrap.go and create it in bootstrap
	CreateLoadDataJobs = `CREATE TABLE IF NOT EXISTS mysql.load_data_jobs (
       job_id bigint(64) NOT NULL AUTO_INCREMENT,
       expected_status ENUM('running', 'paused', 'canceled') NOT NULL DEFAULT 'running',
       create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       start_time TIMESTAMP NULL DEFAULT NULL,
       update_time TIMESTAMP NULL DEFAULT NULL,
       end_time TIMESTAMP NULL DEFAULT NULL,
       data_source TEXT NOT NULL,
       table_schema VARCHAR(64) NOT NULL,
       table_name VARCHAR(64) NOT NULL,
       import_mode VARCHAR(64) NOT NULL,
       create_user VARCHAR(32) NOT NULL,
       progress TEXT DEFAULT NULL,
       result_message TEXT DEFAULT NULL,
       error_message TEXT DEFAULT NULL,
       PRIMARY KEY (job_id),
       KEY (create_user));`
)

// CreateLoadDataJob creates a load data job.
func CreateLoadDataJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	source, db, table string,
	importMode string,
	user string,
) (int64, error) {
	const insertSQL = `INSERT INTO mysql.load_data_jobs
    	(data_source, table_schema, table_name, import_mode, create_user)
		VALUES (%?, %?, %?, %?, %?);`
	_, err := conn.ExecuteInternal(ctx, insertSQL, source, db, table, importMode, user)
	if err != nil {
		return 0, err
	}
	const lastInsertID = `SELECT LAST_INSERT_ID();`
	rs, err := conn.ExecuteInternal(ctx, lastInsertID)
	if err != nil {
		return 0, err
	}
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
	const updateSQL = `UPDATE mysql.load_data_jobs
		SET start_time = CURRENT_TIMESTAMP
		WHERE job_id = %? AND start_time IS NULL;`
	_, err := conn.ExecuteInternal(ctx, updateSQL, jobID)
	return err
}

// HeartBeatInSec is the interval of heartbeat.
var HeartBeatInSec = 5

// UpdateJobProgress updates the progress of a load data job. It should be called
// periodically as heartbeat.
func UpdateJobProgress(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	progress string,
) (bool, error) {
	// let TiDB handle heartbeat check for concurrent SQL
	const updateSQL = `UPDATE mysql.load_data_jobs
		SET progress = %?, update_time = CURRENT_TIMESTAMP
		WHERE job_id = %?
			AND (
		    	update_time >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL %? SECOND)
				OR update_time IS NULL);`
	// we tolerate 2 times of failure/timeout when updating heartbeat
	rs, err := conn.ExecuteInternal(ctx, updateSQL, progress, jobID, HeartBeatInSec*3)
	if err != nil {
		return false, err
	}
	c := rs.NewChunk(nil)
	err = rs.Next(ctx, c)
	if err != nil {
		return false, err
	}
	return c.NumRows() == 1, nil
}

// FinishJob finishes a load data job. A job can only be started once.
func FinishJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	result string,
) error {
	const updateSQL = `UPDATE mysql.load_data_jobs
		SET end_time = CURRENT_TIMESTAMP, result_message = %?
		WHERE job_id = %? AND result_message IS NULL AND error_message IS NULL;`
	_, err := conn.ExecuteInternal(ctx, updateSQL, result, jobID)
	return err
}

// FailJob fails a load data job. A job can only be started once.
func FailJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	result string,
) error {
	const updateSQL = `UPDATE mysql.load_data_jobs
		SET end_time = CURRENT_TIMESTAMP, error_message = %?
		WHERE job_id = %? AND result_message IS NULL AND error_message IS NULL;`
	_, err := conn.ExecuteInternal(ctx, updateSQL, result, jobID)
	return err
}

type JobExpectedStatus int

const (
	JobExpectedRunning JobExpectedStatus = iota
	JobExpectedPaused
	JobExpectedCanceled
)

// UpdateJobExpectedStatus updates the expected status of a load data job.
func UpdateJobExpectedStatus(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	status JobExpectedStatus,
) error {
	const (
		toRunning = `UPDATE mysql.load_data_jobs
			SET expected_status = %?
			WHERE job_id = %? AND expected_status = 'paused';`
		toPaused = `UPDATE mysql.load_data_jobs
			SET expected_status = %?
			WHERE job_id = %? AND expected_status = 'running';`
		toCanceled = `UPDATE mysql.load_data_jobs
			SET expected_status = %?
			WHERE job_id = %? AND expected_status != 'canceled';`
	)

	var sql string
	switch status {
	case JobExpectedRunning:
		sql = toRunning
	case JobExpectedPaused:
		sql = toPaused
	case JobExpectedCanceled:
		sql = toCanceled
	}
	_, err := conn.ExecuteInternal(ctx, sql, jobID)
	return err
}

type JobStatus int

const (
	JobFailed JobStatus = iota
	JobCanceled
	JobPaused
	JobFinished
	JobPending
	JobRunning
)

// GetJobStatus gets the status of a load data job. The returned error means
// something wrong when querying the database. Other bussiness logic errors are
// returned as JobFailed with message.
func GetJobStatus(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
) (JobStatus, string, error) {
	const sql = `SELECT
		expected_status,
		update_time >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL %? SECOND) AS is_alive,
		end_time,
		result_message,
		error_message,
		start_time
		FROM mysql.load_data_jobs
		WHERE job_id = %?;`
	rs, err := conn.ExecuteInternal(ctx, sql, HeartBeatInSec*3, jobID)
	if err != nil {
		return JobFailed, "", err
	}
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return JobFailed, "", err
	}
	if len(rows) != 1 {
		return JobFailed, fmt.Sprintf("job %d not found", jobID), nil
	}
	row := rows[0]

}
