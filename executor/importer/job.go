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

package importer

import (
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
)

// vars used for test.
var (
	// TestLastImportJobID last created job id, used in unit test.
	TestLastImportJobID atomic.Int64
)

// constants for job status and step.
const (
	// JobStatus
	// 	┌───────┐     ┌───────┐    ┌────────┐
	// 	│pending├────►│running├───►│finished│
	// 	└────┬──┘     └────┬──┘    └────────┘
	// 	     │             │       ┌──────┐
	// 	     │             ├──────►│failed│
	// 	     │             │       └──────┘
	// 	     │             │       ┌─────────┐
	// 	     └─────────────┴──────►│cancelled│
	// 	                           └─────────┘
	jobStatusPending   = "pending"
	jobStatusRunning   = "running"
	jogStatusCancelled = "cancelled"
	jobStatusFailed    = "failed"
	jobStatusFinished  = "finished"

	// when the job is finished, step will be set to none.
	jobStepNone        = ""
	JobStepImporting   = "importing"
	JobStepValidating  = "validating"
	JobStepAddingIndex = "adding-index"
	JobStepAnalyzing   = "analyzing"
)

type ImportParameters struct {
	ColumnsAndVars string `json:"columns_and_vars;omitempty"`
	SetClause      string `json:"set_clause;omitempty"`
	// for s3 URL, AK/SK is redacted for security
	FileLocation string                 `json:"file_location"`
	Format       string                 `json:"format"`
	Options      map[string]interface{} `json:"options;omitempty"`
}

// JobInfo is the information of import into job.
type JobInfo struct {
	ID          int64
	CreateTime  types.Time
	StartTime   types.Time
	EndTime     types.Time
	TableSchema string
	TableName   string
	TableID     int64
	CreatedBy   string
	Parameters  ImportParameters
	Status      string
	// in SHOW IMPORT JOB, we name it as phase.
	// here, we use the same name as in distributed framework.
	Step         string
	Progress     *asyncloaddata.Progress
	ErrorMessage string
}

// CanCancel returns whether the job can be cancelled.
func (j *JobInfo) CanCancel() bool {
	return j.Status == jobStatusPending || j.Status == jobStatusRunning
}

// Job import job.
type Job struct {
	ID int64
	// Job don't manage the life cycle of the connection.
	Conn sqlexec.SQLExecutor
	User string
	// whether the user has super privilege.
	// If the user has super privilege, the user can show or operate all jobs,
	// else the user can only show or operate his own jobs.
	HasSuperPriv bool
}

// NewJob returns new Job.
func NewJob(ID int64, conn sqlexec.SQLExecutor, user string, hasSuperPriv bool) *Job {
	return &Job{ID: ID, Conn: conn, User: user, HasSuperPriv: hasSuperPriv}
}

// CreateJob creates import into job by insert a record to system table.
// The AUTO_INCREMENT value will be returned as jobID.
func CreateJob(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	db, table string,
	tableID int64,
	user string,
	parameters *ImportParameters,
) (int64, error) {
	bytes, err := json.Marshal(parameters)
	if err != nil {
		return 0, err
	}
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err = conn.ExecuteInternal(ctx, `INSERT INTO mysql.tidb_import_jobs
		(table_schema, table_name, table_id, created_by, parameters, status, step)
		VALUES (%?, %?, %?, %?, %?, %?, %?);`,
		db, table, tableID, user, bytes, jobStatusPending, jobStepNone)
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

	failpoint.Inject("saveLastImportJobID", func() {
		TestLastImportJobID.Store(rows[0].GetInt64(0))
	})
	return rows[0].GetInt64(0), nil
}

// Start tries to start a pending job with jobID, change its status to runnning.
// It will not return error when there's no matched job or the job has already started.
func (j *Job) Start(ctx context.Context) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := j.Conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), start_time = CURRENT_TIMESTAMP(6), status = %?
		WHERE id = %? AND status = %?;`,
		jobStatusRunning, j.ID, jobStatusPending)
	if err != nil {
		return err
	}

	if j.Conn.GetSessionVars().StmtCtx.AffectedRows() == 0 {
		return errors.Errorf("start failed, job %d not exists or not in pending status", j.ID)
	}

	return nil
}

// UpdateProgress updates the progress of running job. It should be called
// periodically after StartJob.
// It will not return error when there's no matched job or the job is not running.
func (j *Job) UpdateProgress(ctx context.Context, progress string) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	// let TiDB handle heartbeat check for concurrent SQL
	// we tolerate 2 times of failure/timeout when updating heartbeat
	_, err := j.Conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), progress = %?
		WHERE id = %? and status = %?;`,
		progress, j.ID, jobStatusRunning)
	if err != nil {
		return err
	}
	if j.Conn.GetSessionVars().StmtCtx.AffectedRows() == 0 {
		return errors.Errorf("update progress failed, job %d not exists or not in running status", j.ID)
	}
	return nil
}

// Finish finishes import into job. A job can only be finished once.
func (j *Job) Finish(ctx context.Context, progress string) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := j.Conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), end_time = CURRENT_TIMESTAMP(6), status = %?, step = %?, progress = %?
		WHERE id = %? AND status = %?;`,
		jobStatusFinished, jobStepNone, progress, j.ID, jobStatusRunning)
	if err != nil {
		return err
	}
	if j.Conn.GetSessionVars().StmtCtx.AffectedRows() == 0 {
		return errors.Errorf("call Finish failed, job %d not exists or not in running status", j.ID)
	}
	return nil
}

// Fail fails import into job. A job can only be failed once.
func (j *Job) Fail(ctx context.Context, errorMsg string) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := j.Conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), end_time = CURRENT_TIMESTAMP(6), status = %?, error_message = %?
		WHERE id = %? AND status = %?;`,
		jobStatusFailed, errorMsg, j.ID, jobStatusRunning)
	if err != nil {
		return err
	}
	if j.Conn.GetSessionVars().StmtCtx.AffectedRows() == 0 {
		return errors.Errorf("call Fail failed, job %d not exists or not in running status", j.ID)
	}
	return nil
}

// Get gets all needed information of import into job.
func (j *Job) Get(ctx context.Context) (*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	return j.get(ctx)
}

func (j *Job) get(ctx context.Context) (*JobInfo, error) {
	sql := `SELECT
				id, create_time, start_time, end_time,
				table_schema, table_name, table_id, created_by, parameters,
				status, step, progress, error_message,
			FROM mysql.tidb_import_jobs
			WHERE id = %?`
	args := []interface{}{j.ID}
	rs, err := j.Conn.ExecuteInternal(ctx, sql, args...)
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

	info, err := convert2JobInfo(rows[0])
	if err != nil {
		return nil, err
	}
	if !j.HasSuperPriv && info.CreatedBy != j.User {
		return nil, core.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
	}
	return info, nil
}

func convert2JobInfo(row chunk.Row) (*JobInfo, error) {
	parameters := ImportParameters{}
	parametersStr := row.GetString(8)
	if err := json.Unmarshal([]byte(parametersStr), &parameters); err != nil {
		return nil, errors.Trace(err)
	}
	var progress *asyncloaddata.Progress
	progressStr := row.GetString(11)
	if len(progressStr) > 0 {
		progress = &asyncloaddata.Progress{}
		if err := json.Unmarshal([]byte(progressStr), progress); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return &JobInfo{
		ID:           row.GetInt64(0),
		CreateTime:   row.GetTime(1),
		StartTime:    row.GetTime(2),
		EndTime:      row.GetTime(3),
		TableSchema:  row.GetString(4),
		TableName:    row.GetString(5),
		TableID:      row.GetInt64(6),
		CreatedBy:    row.GetString(7),
		Parameters:   parameters,
		Status:       row.GetString(9),
		Step:         row.GetString(10),
		Progress:     progress,
		ErrorMessage: row.GetString(12),
	}, nil
}

// GetAllViewableJobs gets all viewable jobs.
func GetAllViewableJobs(ctx context.Context, conn sqlexec.SQLExecutor, user string, hasSuperPriv bool) ([]*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	sql := `SELECT
				id, create_time, start_time, end_time,
				table_schema, table_name, table_id, created_by, parameters,
				status, step, progress, error_message,
			FROM mysql.tidb_import_jobs`
	args := []interface{}{}
	if !hasSuperPriv {
		sql += " AND created_by = %?"
		args = append(args, user)
	}
	rs, err := conn.ExecuteInternal(ctx, sql, args)
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
		jobInfo, err2 := convert2JobInfo(row)
		if err2 != nil {
			return nil, err2
		}
		ret = append(ret, jobInfo)
	}

	return ret, nil
}

// CancelJob cancels import into job. Only a running/paused job can be canceled.
// check privileges using get before calling this method.
func CancelJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64) (err error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	sql := `UPDATE mysql.tidb_import_jobs
			SET update_time = CURRENT_TIMESTAMP(6), status = %?,
				end_time = CURRENT_TIMESTAMP(6), error_message = 'canceled by user'
			WHERE id = %? AND status IN (%?, %?);`
	args := []interface{}{jogStatusCancelled, jobID, jobStatusPending, jobStatusRunning}
	_, err = conn.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return err
	}
	if conn.GetSessionVars().StmtCtx.AffectedRows() == 0 {
		return errors.Errorf("cancel job failed, job %d not exists or not in pending or running status", jobID)
	}
	return nil
}
