// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importsdk

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
)

// JobDetail captures the columns returned by SHOW IMPORT JOB(S).
type JobDetail struct {
	JobID                    int64
	GroupKey                 sql.NullString
	DataSource               sql.NullString
	TargetTable              sql.NullString
	TableID                  sql.NullInt64
	Phase                    sql.NullString
	Status                   sql.NullString
	SourceFileSize           sql.NullString
	ImportedRows             sql.NullInt64
	ResultMessage            sql.NullString
	CreateTime               sql.NullTime
	StartTime                sql.NullTime
	EndTime                  sql.NullTime
	CreatedBy                sql.NullString
	LastUpdateTime           sql.NullTime
	CurrentStep              sql.NullString
	CurrentStepProcessedSize sql.NullString
	CurrentStepTotalSize     sql.NullString
	CurrentStepProgressPct   sql.NullString
	CurrentStepSpeed         sql.NullString
	CurrentStepETA           sql.NullString
}

// SubmitImportJob runs the provided IMPORT INTO statement and returns the emitted job metadata.
func (sdk *ImportSDK) SubmitImportJob(ctx context.Context, importSQL string) (*JobDetail, error) {
	conn, err := sdk.db.Conn(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() { _ = conn.Close() }()

	rows, err := conn.QueryContext(ctx, importSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	job, err := scanSingleJob(rows)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, ErrNoJobInfoReturned
	}
	return job, rows.Err()
}

// FetchImportJob returns the latest information for a specific IMPORT job.
func (sdk *ImportSDK) FetchImportJob(ctx context.Context, jobID int64) (*JobDetail, error) {
	query := fmt.Sprintf("SHOW IMPORT JOB %d", jobID)
	rows, err := sdk.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	job, err := scanSingleJob(rows)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, ErrJobNotFound
	}
	return job, rows.Err()
}

// FetchImportJobs lists all visible IMPORT jobs for the current session.
func (sdk *ImportSDK) FetchImportJobs(ctx context.Context) ([]*JobDetail, error) {
	rows, err := sdk.db.QueryContext(ctx, "SHOW IMPORT JOBS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*JobDetail
	for rows.Next() {
		job, err := scanCurrentJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// CancelImportJob requests cancellation for a running IMPORT job.
func (sdk *ImportSDK) CancelImportJob(ctx context.Context, jobID int64) error {
	return sdk.execAdminJob(ctx, "CANCEL", jobID)
}

// PauseImportJob requests the server to pause a running IMPORT job.
func (sdk *ImportSDK) PauseImportJob(ctx context.Context, jobID int64) error {
	return sdk.execAdminJob(ctx, "PAUSE", jobID)
}

// ResumeImportJob requests the server to resume a paused IMPORT job.
func (sdk *ImportSDK) ResumeImportJob(ctx context.Context, jobID int64) error {
	return sdk.execAdminJob(ctx, "RESUME", jobID)
}

func (sdk *ImportSDK) execAdminJob(ctx context.Context, verb string, jobID int64) error {
	stmt := fmt.Sprintf("%s IMPORT JOB %d", verb, jobID)
	_, err := sdk.db.ExecContext(ctx, stmt)
	return err
}

func scanSingleJob(rows *sql.Rows) (*JobDetail, error) {
	if !rows.Next() {
		return nil, nil
	}
	job, err := scanCurrentJob(rows)
	if err != nil {
		return nil, err
	}
	// Drain remaining rows (if any) to allow the driver to reuse the connection.
	for rows.Next() {
		// ignore extra rows
	}
	return job, rows.Err()
}

func scanCurrentJob(rows *sql.Rows) (*JobDetail, error) {
	var (
		jobID          sql.NullInt64
		groupKey       sql.NullString
		dataSource     sql.NullString
		targetTable    sql.NullString
		tableID        sql.NullInt64
		phase          sql.NullString
		status         sql.NullString
		sourceFileSize sql.NullString
		importedRows   sql.NullInt64
		resultMessage  sql.NullString
		createTime     nullableTime
		startTime      nullableTime
		endTime        nullableTime
		createdBy      sql.NullString
		lastUpdateTime nullableTime
		curStep        sql.NullString
		curProcessed   sql.NullString
		curTotal       sql.NullString
		curProgress    sql.NullString
		curSpeed       sql.NullString
		curETA         sql.NullString
	)

	if err := rows.Scan(
		&jobID,
		&groupKey,
		&dataSource,
		&targetTable,
		&tableID,
		&phase,
		&status,
		&sourceFileSize,
		&importedRows,
		&resultMessage,
		&createTime,
		&startTime,
		&endTime,
		&createdBy,
		&lastUpdateTime,
		&curStep,
		&curProcessed,
		&curTotal,
		&curProgress,
		&curSpeed,
		&curETA,
	); err != nil {
		return nil, err
	}
	if !jobID.Valid {
		return nil, errors.New("SHOW IMPORT JOB returned row without Job_ID")
	}
	return &JobDetail{
		JobID:                    jobID.Int64,
		GroupKey:                 groupKey,
		DataSource:               dataSource,
		TargetTable:              targetTable,
		TableID:                  tableID,
		Phase:                    phase,
		Status:                   status,
		SourceFileSize:           sourceFileSize,
		ImportedRows:             importedRows,
		ResultMessage:            resultMessage,
		CreateTime:               createTime.NullTime,
		StartTime:                startTime.NullTime,
		EndTime:                  endTime.NullTime,
		CreatedBy:                createdBy,
		LastUpdateTime:           lastUpdateTime.NullTime,
		CurrentStep:              curStep,
		CurrentStepProcessedSize: curProcessed,
		CurrentStepTotalSize:     curTotal,
		CurrentStepProgressPct:   curProgress,
		CurrentStepSpeed:         curSpeed,
		CurrentStepETA:           curETA,
	}, nil
}

type nullableTime struct {
	sql.NullTime
}

func (nt *nullableTime) Scan(value any) error {
	if value == nil {
		nt.Valid = false
		return nil
	}
	switch v := value.(type) {
	case time.Time:
		nt.Time = v
		nt.Valid = true
		return nil
	case []byte:
		return nt.setFromString(string(v))
	case string:
		return nt.setFromString(v)
	default:
		return errors.Errorf("unsupported time value type %T", value)
	}
}

func (nt *nullableTime) setFromString(s string) error {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		nt.Valid = false
		return nil
	}
	t, err := parseTimestampString(s)
	if err != nil {
		return err
	}
	nt.Time = t
	nt.Valid = true
	return nil
}

func parseTimestampString(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		if ts, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, errors.Errorf("cannot parse time value %q", s)
}
