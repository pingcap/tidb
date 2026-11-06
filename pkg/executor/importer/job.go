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
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/importinto"
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

const (
	baseQuerySQL = `SELECT
					id, create_time, start_time, update_time, end_time,
					table_schema, table_name, table_id, created_by, parameters, source_file_size,
					status, step, summary, error_message, group_key
				FROM mysql.tidb_import_jobs`
)

// ImportParameters describes the submitted statement for import-job diagnostics.
type ImportParameters = importinto.ImportParameters

// GetJob returns the job with the given id if the user has privilege.
// hasSuperPriv: whether the user has super privilege.
// If the user has super privilege, the user can show or operate all jobs,
// else the user can only show or operate his own jobs.
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
	groupKey string,
	parameters *ImportParameters,
	sourceFileSize int64,
) (int64, error) {
	bytes, err := json.Marshal(parameters)
	if err != nil {
		return 0, err
	}
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	_, err = conn.ExecuteInternal(ctx, `INSERT INTO mysql.tidb_import_jobs
		(table_schema, table_name, table_id, group_key, created_by, parameters, source_file_size, status, step)
		VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?);`,
		db, table, tableID, groupKey, user, bytes, sourceFileSize, jobStatusPending, jobStepNone)

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

// UpdateJobPreparedInfo updates import job fields that are known only after
// async prepare succeeds.
func UpdateJobPreparedInfo(
	ctx context.Context,
	conn sqlexec.SQLExecutor,
	jobID int64,
	sourceFileSize int64,
	format string,
) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	rs, err := conn.ExecuteInternal(ctx, `SELECT parameters FROM mysql.tidb_import_jobs
		WHERE id = %? AND status = %?;`,
		jobID, JobStatusRunning)
	if err != nil {
		return err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return err
	}
	// no matched running job, keep historical behavior: no-op.
	if len(rows) == 0 {
		return nil
	}

	parameters := &ImportParameters{}
	parametersStr := rows[0].GetString(0)
	if len(parametersStr) > 0 {
		if err = json.Unmarshal([]byte(parametersStr), parameters); err != nil {
			return err
		}
	}
	if format != "" {
		parameters.Format = format
	}

	paramsBytes, err := json.Marshal(parameters)
	if err != nil {
		return err
	}
	_, err = conn.ExecuteInternal(ctx, `UPDATE mysql.tidb_import_jobs
				SET update_time = CURRENT_TIMESTAMP(6), source_file_size = %?, parameters = %?
				WHERE id = %? AND status = %?;`,
		sourceFileSize, paramsBytes, jobID, JobStatusRunning)
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
		JobStatusFinished, jobStepNone, summaryStr, jobID, JobStatusRunning)
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
			WHERE id = %? AND status IN (%?, %?);`,
		jobStatusFailed, errorMsg, summaryStr, jobID, jobStatusPending, JobStatusRunning)
	return err
}

func convert2JobInfo(row chunk.Row) (*JobInfo, error) {
	// start_time, end_time, summary, error_message can be NULL, need to use row.IsNull() to check.
	startTime, updateTime, endTime := types.ZeroTime, types.ZeroTime, types.ZeroTime
	if !row.IsNull(2) {
		startTime = row.GetTime(2)
	}
	if !row.IsNull(3) {
		updateTime = row.GetTime(3)
	}
	if !row.IsNull(4) {
		endTime = row.GetTime(4)
	}

	parameters := ImportParameters{}
	parametersStr := row.GetString(9)
	if err := json.Unmarshal([]byte(parametersStr), &parameters); err != nil {
		return nil, errors.Trace(err)
	}

	var summary *Summary
	var summaryStr string
	if !row.IsNull(13) {
		summaryStr = row.GetString(13)
	}
	if len(summaryStr) > 0 {
		summary = &Summary{}
		if err := json.Unmarshal([]byte(summaryStr), summary); err != nil {
			return nil, errors.Trace(err)
		}
	}

	var errMsg string
	if !row.IsNull(14) {
		errMsg = row.GetString(14)
	}

	return &JobInfo{
		ID:             row.GetInt64(0),
		CreateTime:     row.GetTime(1),
		StartTime:      startTime,
		UpdateTime:     updateTime,
		EndTime:        endTime,
		TableSchema:    row.GetString(5),
		TableName:      row.GetString(6),
		TableID:        row.GetInt64(7),
		CreatedBy:      row.GetString(8),
		Parameters:     parameters,
		SourceFileSize: row.GetInt64(10),
		Status:         row.GetString(11),
		Step:           row.GetString(12),
		Summary:        summary,
		ErrorMessage:   errMsg,
		GroupKey:       row.GetString(15),
	}, nil
}

func getJobInfoFromSQL(ctx context.Context, conn sqlexec.SQLExecutor, sql string, args ...any) ([]*JobInfo, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
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

// GetJobsByGroupKey gets jobs with given group key.
// If group key is not specified, it will return all jobs with group key set.
func GetJobsByGroupKey(ctx context.Context, conn sqlexec.SQLExecutor, user, groupKey string, hasSuperPriv bool) ([]*JobInfo, error) {
	sql := baseQuerySQL
	args := []any{}
	var whereClause []string
	if !hasSuperPriv {
		whereClause = append(whereClause, "created_by = %?")
		args = append(args, user)
	}

	if groupKey != "" {
		whereClause = append(whereClause, "GROUP_KEY = %?")
		args = append(args, groupKey)
	} else {
		whereClause = append(whereClause, "GROUP_KEY != ''")
	}

	if len(whereClause) > 0 {
		sql = fmt.Sprintf("%s WHERE %s", sql, strings.Join(whereClause, " AND "))
	}

	return getJobInfoFromSQL(ctx, conn, sql, args...)
}

// GetAllViewableJobs gets all viewable jobs.
func GetAllViewableJobs(ctx context.Context, conn sqlexec.SQLExecutor, user string, hasSuperPriv bool) ([]*JobInfo, error) {
	sql := baseQuerySQL
	args := []any{}
	if !hasSuperPriv {
		sql += " WHERE created_by = %?"
		args = append(args, user)
	}

	return getJobInfoFromSQL(ctx, conn, sql, args...)
}

// CancelJob cancels import into job. Only a running/paused job can be canceled.
// check privileges using get before calling this method.
func CancelJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64) (err error) {
	return cancelJobInState(ctx, conn, jobID, jobStatusPending, JobStatusRunning)
}

// CancelPendingJob cancels a job in pending state.
func CancelPendingJob(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64) (err error) {
	return cancelJobInState(ctx, conn, jobID, jobStatusPending)
}

func cancelJobInState(ctx context.Context, conn sqlexec.SQLExecutor, jobID int64, states ...string) (err error) {
	var markerSB strings.Builder
	for i := range states {
		if i > 0 {
			markerSB.WriteString(",")
		}
		markerSB.WriteString("%?")
	}
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	sql := fmt.Sprintf(`UPDATE mysql.tidb_import_jobs
			SET update_time = CURRENT_TIMESTAMP(6), status = %%?, error_message = 'cancelled by user'
			WHERE id = %%? AND status IN (%s);`, markerSB.String())
	args := make([]any, 0, len(states)+2)
	args = append(args, jogStatusCancelled)
	args = append(args, jobID)
	for _, s := range states {
		args = append(args, s)
	}
	_, err = conn.ExecuteInternal(ctx, sql, args...)
	return err
}
