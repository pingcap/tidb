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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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
	jobStepNone = ""
	// JobStepGlobalSorting is the first step when using global sort,
	// step goes from none -> global-sorting -> importing -> validating -> none.
	JobStepGlobalSorting = "global-sorting"
	JobStepMerging       = "merging"
	// JobStepImporting is the first step when using local sort,
	// step goes from none -> importing -> validating -> none.
	// when used in global sort, it means importing the sorted data.
	// when used in local sort, it means encode&sort data and then importing the data.
	JobStepImporting  = "importing"
	JobStepValidating = "validating"

	baseQuerySQL = `SELECT
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
	Options map[string]any `json:"options,omitempty"`
}

var _ fmt.Stringer = &ImportParameters{}

// String implements fmt.Stringer interface.
func (ip *ImportParameters) String() string {
	b, _ := json.Marshal(ip)
	return string(b)
}

// RuntimeInfo is the runtime information of the task for corresponding job.
type RuntimeInfo struct {
	Status     proto.TaskState
	ImportRows int64
	ErrorMsg   string

	Step               proto.Step
	FinishedSubtaskCnt int
	TotalSubtaskCnt    int
	Processed          int64
	Total              int64
	Unit               string
	Duration           int64
}

// String returns the string representation of the runtime info
func (r *RuntimeInfo) String() string {
	stepStr := proto.Step2Str(proto.ImportInto, r.Step)

	// Currently, we can't track the progress of post process
	if r.Step == proto.ImportStepPostProcess || r.Step == proto.StepInit {
		return fmt.Sprintf("[%s] N/A", stepStr)
	}

	percentage := 0.0
	if r.Total > 0 {
		percentage = float64(r.Processed) / float64(r.Total)
		percentage = min(percentage, 1.0)
	}

	speed := 0.0
	if r.Duration > 0 && r.Processed > 0 {
		speed = float64(r.Processed) / float64(r.Duration)
	}

	elapsed := time.Duration(r.Duration) * time.Second
	remainTime := "N/A"
	if int64(speed) > 0 {
		remainSecond := max((r.Total-r.Processed)/int64(speed), 0)
		remain := time.Duration(remainSecond) * time.Second
		remainTime = remain.String()
	}

	return fmt.Sprintf("[%s] subtasks: %d/%d, progress: %.2f%%, speed: %s%s/s, elapsed: %s, ETA: %s",
		stepStr, r.FinishedSubtaskCnt, r.TotalSubtaskCnt,
		percentage*100, units.HumanSize(speed), r.Unit,
		elapsed, remainTime,
	)
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
	Step         string
	ImportedRows int64
	Progress     string
	ErrorMessage string
}

// CanCancel returns whether the job can be cancelled.
func (j *JobInfo) CanCancel() bool {
	return j.Status == jobStatusPending || j.Status == JobStatusRunning
}

// GetJob returns the job with the given id if the user has privilege to show this job.
func GetJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, user string, hasSuperPriv bool) (*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)

	sql := baseQuerySQL + ` WHERE id = %?`
	rs, err := conn.ExecuteInternal(ctx, sql, jobID)
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
		return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
	}
	return info, nil
}

// GetActiveJobCnt returns the count of active import jobs.
// Active import jobs include pending and running jobs.
func GetActiveJobCnt(ctx context.Context, conn sqlexec.SQLExecutor, tableSchema, tableName string) (int64, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)

	sql := `select count(1) from mysql.tidb_import_jobs
			where status in (%?, %?)
				and table_schema = %? and table_name = %?;
			`
	rs, err := conn.ExecuteInternal(ctx, sql, jobStatusPending, JobStatusRunning,
		tableSchema, tableName)
	if err != nil {
		return 0, err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return 0, err
	}
	cnt := rows[0].GetInt64(0)
	return cnt, nil
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

// StartJob tries to start a pending job with jobID, change its status/step to running/input step.
// It will not return error when there's no matched job or the job has already started.
func StartJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, step string) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), start_time = CURRENT_TIMESTAMP(6), status = %?, step = %?
		WHERE id = %? AND status = %?;`,
		JobStatusRunning, step, jobID, jobStatusPending)

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

// FinishJob tries to finish a running job with jobID, change its status to finished, clear its step and update summary.
// It will not return error when there's no matched job.
func FinishJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, summary *Summary) error {
	summaryStr := "{}"
	if summary != nil {
		bytes, err := json.Marshal(summary)
		if err != nil {
			return err
		}
		summaryStr = string(bytes)
	}

	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), end_time = CURRENT_TIMESTAMP(6), status = %?, step = %?, summary = %?
		WHERE id = %? AND status = %?;`,
		jobStatusFinished, jobStepNone, summaryStr, jobID, JobStatusRunning)
	return err
}

// FailJob fails import into job. A job can only be failed once.
// It will not return error when there's no matched job.
func FailJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, errorMsg string, summary *Summary) error {
	summaryStr := "{}"
	if summary != nil {
		bytes, err := json.Marshal(summary)
		if err != nil {
			return err
		}
		summaryStr = string(bytes)
	}

	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err := conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
		SET update_time = CURRENT_TIMESTAMP(6), end_time = CURRENT_TIMESTAMP(6), status = %?, error_message = %?, summary = %?
		WHERE id = %? AND status = %?;`,
		jobStatusFailed, errorMsg, summaryStr, jobID, JobStatusRunning)
	return err
}

func convert2JobInfo(row chunk.Row) (*JobInfo, error) {
	// start_time, end_time, summary, error_message can be NULL, need to use row.IsNull() to check.
	var (
		startTime = types.ZeroTime
		endTime   = types.ZeroTime
	)

	if !row.IsNull(2) {
		startTime = row.GetTime(2)
	}
	if !row.IsNull(3) {
		endTime = row.GetTime(3)
	}

	parameters := ImportParameters{}
	parametersStr := row.GetString(8)
	if err := json.Unmarshal([]byte(parametersStr), &parameters); err != nil {
		return nil, errors.Trace(err)
	}

	importedRows := int64(-1)
	// Only finished/failed job has summary
	// For running job, we have to get importedRows from GetRuntimeInfoForJob.
	if !row.IsNull(12) {
		summaryStr := row.GetString(12)
		if len(summaryStr) > 0 {
			summary := &Summary{}
			if err := json.Unmarshal([]byte(summaryStr), summary); err != nil {
				return nil, errors.Trace(err)
			}
			// Only update importedRows if rowcnt > 0, which means ingest step is finished.
			if summary.PostProcessSummary.RowCnt > 0 {
				importedRows = summary.PostProcessSummary.RowCnt
			}
		}
	}

	var errMsg string
	if !row.IsNull(13) {
		errMsg = row.GetString(13)
	}
	return &JobInfo{
		ID:             row.GetInt64(0),
		CreateTime:     row.GetTime(1),
		StartTime:      startTime,
		EndTime:        endTime,
		TableSchema:    row.GetString(4),
		TableName:      row.GetString(5),
		TableID:        row.GetInt64(6),
		CreatedBy:      row.GetString(7),
		Parameters:     parameters,
		SourceFileSize: row.GetInt64(9),
		Status:         row.GetString(10),
		Step:           row.GetString(11),
		ImportedRows:   importedRows,
		ErrorMessage:   errMsg,
	}, nil
}

// GetAllViewableJobs gets all viewable jobs.
// If the user has super privilege, he can show all jobs, otherwise he can only show his jobs.
func GetAllViewableJobs(ctx context.Context, conn sqlexec.SQLExecutor, user string, hasSuperPriv bool) ([]*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	sql := baseQuerySQL
	args := []any{}
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
	args := []any{jogStatusCancelled, jobID, jobStatusPending, JobStatusRunning}
	_, err = conn.ExecuteInternal(ctx, sql, args...)
	return err
}
