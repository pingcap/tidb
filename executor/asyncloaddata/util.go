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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// vars used for test.
var (
	// TestSyncCh is used in unit test to synchronize the execution of LOAD DATA.
	TestSyncCh = make(chan struct{})
	// TestLastLoadDataJobID last created job id, used in unit test.
	TestLastLoadDataJobID atomic.Int64
)

// Job import job.
type Job struct {
	ID int64
	// Job don't manage the life cycle of the connection.
	Conn sqlexec.SQLExecutor
	User string
}

// NewJob returns new Job.
func NewJob(id int64, conn sqlexec.SQLExecutor, user string) *Job {
	return &Job{ID: id, Conn: conn, User: user}
}

// CreateLoadDataJob creates a load data job by insert a record to system table.
// The AUTO_INCREMENT value will be returned as jobID.
func CreateLoadDataJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	dataSource, db, table string,
	importMode string,
	user string,
) (*Job, error) {
	// remove the params in data source URI because it may contains AK/SK
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
		return nil, err
	}
	rs, err := conn.ExecuteInternal(ctx, `SELECT LAST_INSERT_ID();`)
	if err != nil {
		return nil, err
	}
	//nolint: errcheck
	defer rs.Close()
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, errors.Errorf("unexpected result length: %d", len(rows))
	}

	failpoint.Inject("SaveLastLoadDataJobID", func() {
		TestLastLoadDataJobID.Store(rows[0].GetInt64(0))
	})
	return NewJob(rows[0].GetInt64(0), conn, user), nil
}

// StartJob tries to start a not-yet-started job with jobID. It will not return
// error when there's no matched job.
func (j *Job) StartJob(ctx context.Context) error {
	failpoint.Inject("AfterCreateLoadDataJob", nil)
	failpoint.Inject("SyncAfterCreateLoadDataJob", func() {
		TestSyncCh <- struct{}{}
		<-TestSyncCh
	})

	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err := j.Conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET start_time = CURRENT_TIMESTAMP(6), update_time = CURRENT_TIMESTAMP(6)
		WHERE job_id = %? AND start_time IS NULL AND end_time IS NULL;`,
		j.ID)
	if err != nil {
		return err
	}

	failpoint.Inject("AfterStartJob", nil)
	failpoint.Inject("SyncAfterStartJob", func() {
		TestSyncCh <- struct{}{}
		<-TestSyncCh
	})
	return nil
}

var (
	// HeartBeatInSec is the interval of heartbeat.
	HeartBeatInSec = 5
	// OfflineThresholdInSec means after failing to update heartbeat for 3 times,
	// we treat the worker of the job as offline.
	OfflineThresholdInSec = HeartBeatInSec * 3
)

// UpdateJobProgress updates the progress of a load data job. It should be called
// periodically as heartbeat after StartJob.
// The returned bool indicates whether the keepalive is succeeded. If not, the
// caller should call FailJob soon.
// TODO: Currently if the node is crashed after CreateLoadDataJob and before StartJob,
// it will always be in the status of pending. Maybe we should unify CreateLoadDataJob
// and StartJob.
func (j *Job) UpdateJobProgress(ctx context.Context, progress string) (bool, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	// let TiDB handle heartbeat check for concurrent SQL
	// we tolerate 2 times of failure/timeout when updating heartbeat
	_, err := j.Conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET progress = %?, update_time = CURRENT_TIMESTAMP(6)
		WHERE job_id = %?
			AND end_time IS NULL
			AND (update_time >= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL %? SECOND)
				OR update_time IS NULL);`,
		progress, j.ID, OfflineThresholdInSec)
	if err != nil {
		return false, err
	}
	return j.Conn.GetSessionVars().StmtCtx.AffectedRows() == 1, nil
}

// FinishJob finishes a load data job. A job can only be finished once.
func (j *Job) FinishJob(ctx context.Context, result string) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err := j.Conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET end_time = CURRENT_TIMESTAMP(6), result_message = %?
		WHERE job_id = %? AND result_message IS NULL AND error_message IS NULL;`,
		result, j.ID)
	return err
}

// FailJob fails a load data job. A job can only be failed once.
func (j *Job) FailJob(ctx context.Context, result string) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err := j.Conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET end_time = CURRENT_TIMESTAMP(6), error_message = %?
		WHERE job_id = %? AND result_message IS NULL AND error_message IS NULL;`,
		result, j.ID)
	return err
}

// CancelJob cancels a load data job. Only a running/paused job can be canceled.
func (j *Job) CancelJob(ctx context.Context) (err error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err = j.Conn.ExecuteInternal(ctx, "BEGIN PESSIMISTIC;")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, err1 := j.Conn.ExecuteInternal(ctx, "ROLLBACK;")
			terror.Log(err1)
			return
		}

		_, err = j.Conn.ExecuteInternal(ctx, "COMMIT;")
		if err != nil {
			return
		}
	}()

	var (
		rs   sqlexec.RecordSet
		rows []chunk.Row
	)
	rs, err = j.Conn.ExecuteInternal(ctx,
		`SELECT expected_status, end_time, error_message FROM mysql.load_data_jobs
		WHERE job_id = %? AND create_user = %?;`,
		j.ID, j.User)
	if err != nil {
		return err
	}
	defer terror.Call(rs.Close)
	rows, err = sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return err
	}

	if len(rows) < 1 {
		return exeerrors.ErrLoadDataJobNotFound.GenWithStackByArgs(j.ID)
	}
	status := rows[0].GetEnum(0).String()
	if status != "running" && status != "paused" {
		return exeerrors.ErrLoadDataInvalidOperation.GenWithStackByArgs(fmt.Sprintf("need status running or paused, but got %s", status))
	}
	endTimeIsNull := rows[0].IsNull(1)
	if !endTimeIsNull {
		hasError := !rows[0].IsNull(2)
		if hasError {
			return exeerrors.ErrLoadDataInvalidOperation.GenWithStackByArgs("need status running or paused, but got failed")
		}
		return exeerrors.ErrLoadDataInvalidOperation.GenWithStackByArgs("need status running or paused, but got finished")
	}

	_, err = j.Conn.ExecuteInternal(ctx,
		`UPDATE mysql.load_data_jobs
		SET expected_status = 'canceled',
		    end_time = CURRENT_TIMESTAMP(6),
		    error_message = 'canceled by user'
		WHERE job_id = %?;`,
		j.ID)
	return err
}

// DropJob drops a load data job.
func (j *Job) DropJob(ctx context.Context) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	_, err := j.Conn.ExecuteInternal(ctx,
		`DELETE FROM mysql.load_data_jobs
		WHERE job_id = %? AND create_user = %?;`,
		j.ID, j.User)
	if err == nil {
		return err
	}
	if j.Conn.GetSessionVars().StmtCtx.AffectedRows() < 1 {
		return exeerrors.ErrLoadDataJobNotFound.GenWithStackByArgs(j.ID)
	}
	return nil
}

// OnComplete is called when a job is finished or failed.
func (j *Job) OnComplete(inErr error, msg string) {
	// write the ending status even if user context is canceled.
	ctx2 := context.Background()
	ctx2 = kv.WithInternalSourceType(ctx2, kv.InternalLoadData)
	if inErr == nil {
		err2 := j.FinishJob(ctx2, msg)
		terror.Log(err2)
		return
	}
	errMsg := inErr.Error()
	if errImpl, ok := errors.Cause(inErr).(*errors.Error); ok {
		b, marshalErr := errImpl.MarshalJSON()
		if marshalErr == nil {
			errMsg = string(b)
		}
	}

	err2 := j.FailJob(ctx2, errMsg)
	terror.Log(err2)
}

// ProgressUpdateRoutineFn job progress update routine.
func (j *Job) ProgressUpdateRoutineFn(ctx context.Context, finishCh chan struct{}, errCh <-chan struct{}, progress *Progress) error {
	ticker := time.NewTicker(time.Duration(HeartBeatInSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-finishCh:
			// When done, try to update progress to reach 100%
			ok, err2 := j.UpdateJobProgress(ctx, progress.String())
			if !ok || err2 != nil {
				logutil.Logger(ctx).Warn("failed to update job progress when finished",
					zap.Bool("ok", ok), zap.Error(err2))
			}
			return nil
		case <-errCh:
			return nil
		case <-ticker.C:
			ok, err2 := j.UpdateJobProgress(ctx, progress.String())
			if err2 != nil {
				return err2
			}
			if !ok {
				return errors.Errorf("failed to update job progress, the job %d is interrupted by user or failed to keepalive", j.ID)
			}
		}
	}
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
// TODO: remove it?
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
func (j *Job) GetJobStatus(ctx context.Context) (JobStatus, string, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	rs, err := j.Conn.ExecuteInternal(ctx,
		`SELECT
		expected_status,
		update_time >= DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL %? SECOND) AS is_alive,
		end_time,
		result_message,
		error_message,
		start_time
		FROM mysql.load_data_jobs
		WHERE job_id = %?;`,
		OfflineThresholdInSec, j.ID)
	if err != nil {
		return JobFailed, "", err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return JobFailed, "", err
	}
	if len(rows) != 1 {
		return JobFailed, exeerrors.ErrLoadDataJobNotFound.GenWithStackByArgs(j.ID).Error(), nil
	}

	return getJobStatus(rows[0])
}

// getJobStatus expected the first 6 columns of input row is (expected_status,
// is_alive (derived from update_time), end_time, result_message, error_message,
// start_time).
func getJobStatus(row chunk.Row) (JobStatus, string, error) {
	// ending status has the highest priority
	expectedStatus := row.GetEnum(0).String()
	endTimeIsNull := row.IsNull(2)
	if !endTimeIsNull {
		resultMsgIsNull := row.IsNull(3)
		if !resultMsgIsNull {
			resultMessage := row.GetString(3)
			return JobFinished, resultMessage, nil
		}

		errorMessage := row.GetString(4)
		if expectedStatus == "canceled" {
			return JobCanceled, errorMessage, nil
		}
		return JobFailed, errorMessage, nil
	}

	isAlive := row.GetInt64(1) == 1
	startTimeIsNull := row.IsNull(5)

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
func (j *Job) GetJobInfo(ctx context.Context) (*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalLoadData)
	rs, err := j.Conn.ExecuteInternal(ctx,
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
		OfflineThresholdInSec, j.ID, j.User)
	if err != nil {
		return nil, err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, exeerrors.ErrLoadDataJobNotFound.GenWithStackByArgs(j.ID)
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
