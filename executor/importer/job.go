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
	jobStatusPending = "pending"
	// JobStatusRunning exported since it's used in show import jobs
	JobStatusRunning   = "running"
	jogStatusCancelled = "cancelled"
	jobStatusFailed    = "failed"
	jobStatusFinished  = "finished"

	// when the job is finished, step will be set to none.
	jobStepNone       = ""
	JobStepImporting  = "importing"
	JobStepValidating = "validating"
	// JobStepAddingIndex is the step that add indexes to the table.
	// todo: change to this step when it's implemented.
	JobStepAddingIndex = "adding-index"

	baseQuerySql = `SELECT
					id, create_time, start_time, end_time,
					table_schema, table_name, table_id, created_by, parameters, source_file_size,
					status, step, summary, error_message
				FROM mysql.tidb_import_jobs`
)

// ImportParameters is the parameters for import into statement.
// it's a minimal meta info to store in tidb_import_jobs for diagnose.
// for detailed info, see tidb_global_tasks.
type ImportParameters struct {
	ColumnsAndVars string `json:"columns-and-vars,omitempty"`
	SetClause      string `json:"set-clause,omitempty"`
	// for s3 URL, AK/SK is redacted for security
	FileLocation string `json:"file-location"`
	Format       string `json:"format"`
	// only include what user specified, not include default value.
	Options map[string]interface{} `json:"options,omitempty"`
}

// JobSummary is the summary info of import into job.
type JobSummary struct {
	// ImportedRows is the number of rows imported into TiKV.
	ImportedRows uint64 `json:"imported-rows,omitempty"`
}

// JobInfo is the information of import into job.
type JobInfo struct {
	ID             int64
	CreateTime     types.Time
	StartTime      types.Time
	EndTime        types.Time
	TableSchema    string
	TableName      string
	TableID        int64
	CreatedBy      string
	Parameters     ImportParameters
	SourceFileSize int64
	Status         string
	// in SHOW IMPORT JOB, we name it as phase.
	// here, we use the same name as in distributed framework.
	Step string
	// the summary info of the job, it's updated only when the job is finished.
	// for running job, we should query the progress from the distributed framework.
	Summary      *JobSummary
	ErrorMessage string
}

// CanCancel returns whether the job can be cancelled.
func (j *JobInfo) CanCancel() bool {
	return j.Status == jobStatusPending || j.Status == JobStatusRunning
}

// GetJob returns the job with the given id if the user has privilege.
// hasSuperPriv: whether the user has super privilege.
// If the user has super privilege, the user can show or operate all jobs,
// else the user can only show or operate his own jobs.
func GetJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, user string, hasSuperPriv bool) (*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)

	sql := baseQuerySql + ` WHERE id = %?`
	args := []interface{}{jobID}
	rs, err := conn.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, exeerrors.ErrLoadDataJobNotFound.GenWithStackByArgs(jobID)
	}

	info, err := convert2JobInfo(rows[0])
	if err != nil {
		return nil, err
	}
	if !hasSuperPriv && info.CreatedBy != user {
		return nil, core.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
	}
	return info, nil
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
	sourceFileSize int64,
) (int64, error) {
	bytes, err := json.Marshal(parameters)
	if err != nil {
		return 0, err
	}
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err = conn.ExecuteInternal(ctx, `INSERT INTO mysql.tidb_import_jobs
		(table_schema, table_name, table_id, created_by, parameters, source_file_size, status, step)
		VALUES (%?, %?, %?, %?, %?, %?, %?, %?);`,
		db, table, tableID, user, bytes, sourceFileSize, jobStatusPending, jobStepNone)
	if err != nil {
		return 0, err
	}
	rs, err := conn.ExecuteInternal(ctx, `SELECT LAST_INSERT_ID();`)
	if err != nil {
		return 0, err
	}
	defer terror.Call(rs.Close)

	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, errors.Errorf("unexpected result length: %d", len(rows))
	}

	failpoint.Inject("setLastImportJobID", func() {
		TestLastImportJobID.Store(rows[0].GetInt64(0))
	})
	return rows[0].GetInt64(0), nil
}

// StartJob tries to start a pending job with jobID, change its status/step to running/importing.
// It will not return error when there's no matched job or the job has already started.
func StartJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), start_time = CURRENT_TIMESTAMP(6), status = %?, step = %?
		WHERE id = %? AND status = %?;`,
		JobStatusRunning, JobStepImporting, jobID, jobStatusPending)

	return err
}

// Job2Step tries to change the step of a running job with jobID.
// It will not return error when there's no matched job.
func Job2Step(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, step string) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), step = %?
		WHERE id = %? AND status = %?;`,
		step, jobID, JobStatusRunning)

	return err
}

// FinishJob tries to finish a running job with jobID, change its status to finished, clear its step.
// It will not return error when there's no matched job.
func FinishJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, summary *JobSummary) error {
	bytes, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err = conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), end_time = CURRENT_TIMESTAMP(6), status = %?, step = %?, summary = %?
		WHERE id = %? AND status = %?;`,
		jobStatusFinished, jobStepNone, bytes, jobID, JobStatusRunning)
	return err
}

// FailJob fails import into job. A job can only be failed once.
// It will not return error when there's no matched job.
func FailJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, errorMsg string) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), end_time = CURRENT_TIMESTAMP(6), status = %?, error_message = %?
		WHERE id = %? AND status = %?;`,
		jobStatusFailed, errorMsg, jobID, JobStatusRunning)
	return err
}

func convert2JobInfo(row chunk.Row) (*JobInfo, error) {
	parameters := ImportParameters{}
	parametersStr := row.GetString(8)
	if err := json.Unmarshal([]byte(parametersStr), &parameters); err != nil {
		return nil, errors.Trace(err)
	}
	var summary *JobSummary
	summaryStr := row.GetString(12)
	if len(summaryStr) > 0 {
		summary = &JobSummary{}
		if err := json.Unmarshal([]byte(summaryStr), summary); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return &JobInfo{
		ID:             row.GetInt64(0),
		CreateTime:     row.GetTime(1),
		StartTime:      row.GetTime(2),
		EndTime:        row.GetTime(3),
		TableSchema:    row.GetString(4),
		TableName:      row.GetString(5),
		TableID:        row.GetInt64(6),
		CreatedBy:      row.GetString(7),
		Parameters:     parameters,
		SourceFileSize: row.GetInt64(9),
		Status:         row.GetString(10),
		Step:           row.GetString(11),
		Summary:        summary,
		ErrorMessage:   row.GetString(13),
	}, nil
}

// GetAllViewableJobs gets all viewable jobs.
func GetAllViewableJobs(ctx context.Context, conn sqlexec.SQLExecutor, user string, hasSuperPriv bool) ([]*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	sql := baseQuerySql
	args := []interface{}{}
	if !hasSuperPriv {
		sql += " WHERE created_by = %?"
		args = append(args, user)
	}
	rs, err := conn.ExecuteInternal(ctx, sql, args...)
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
			SET update_time = CURRENT_TIMESTAMP(6), status = %?, error_message = 'cancelled by user'
			WHERE id = %? AND status IN (%?, %?);`
	args := []interface{}{jogStatusCancelled, jobID, jobStatusPending, JobStatusRunning}
	_, err = conn.ExecuteInternal(ctx, sql, args...)
	return err
}
