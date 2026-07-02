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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
)

// JobManager defines the interface for managing import jobs
type JobManager interface {
	SubmitJob(ctx context.Context, query string) (int64, error)
	GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error)
	CancelJob(ctx context.Context, jobID int64) error
	GetGroupSummary(ctx context.Context, groupKey string) (*GroupStatus, error)
	GetJobsByGroup(ctx context.Context, groupKey string) ([]*JobStatus, error)
}

const timeLayout = "2006-01-02 15:04:05"

type jobManager struct {
	db *sql.DB
}

// NewJobManager creates a new JobManager
func NewJobManager(db *sql.DB) JobManager {
	return &jobManager{
		db: db,
	}
}

// SubmitJob submits an import job and returns the job ID
func (m *jobManager) SubmitJob(ctx context.Context, query string) (int64, error) {
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		jobID, err := scanJobID(rows)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return jobID, nil
	}

	if err := rows.Err(); err != nil {
		return 0, errors.Trace(err)
	}

	return 0, ErrNoJobIDReturned
}

// GetJobStatus gets the status of an import job
func (m *jobManager) GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	query := fmt.Sprintf("SHOW RAW IMPORT JOB %d", jobID)
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		if isRawImportUnsupportedError(err) {
			return m.getLegacyJobStatus(ctx, jobID)
		}
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		return scanJobStatus(rows)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	return nil, ErrJobNotFound
}

func (m *jobManager) getLegacyJobStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	query := fmt.Sprintf("SHOW IMPORT JOB %d", jobID)
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		return scanJobStatus(rows)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return nil, ErrJobNotFound
}

// GetGroupSummary returns aggregated information for the specified group key.
func (m *jobManager) GetGroupSummary(ctx context.Context, groupKey string) (*GroupStatus, error) {
	if groupKey == "" {
		return nil, ErrInvalidOptions
	}
	query := fmt.Sprintf("SHOW IMPORT GROUP '%s'", strings.ReplaceAll(groupKey, "'", "''"))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		status, err := scanGroupStatus(rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return status, nil
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return nil, ErrJobNotFound
}

// GetJobsByGroup returns all jobs for the specified group key.
func (m *jobManager) GetJobsByGroup(ctx context.Context, groupKey string) ([]*JobStatus, error) {
	if groupKey == "" {
		return nil, ErrInvalidOptions
	}
	query := fmt.Sprintf("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = '%s'", strings.ReplaceAll(groupKey, "'", "''"))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		if isRawImportUnsupportedError(err) {
			return m.getLegacyJobsByGroup(ctx, groupKey)
		}
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var jobs []*JobStatus
	for rows.Next() {
		status, err := scanJobStatus(rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, status)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

func (m *jobManager) getLegacyJobsByGroup(ctx context.Context, groupKey string) ([]*JobStatus, error) {
	query := fmt.Sprintf("SHOW IMPORT JOBS WHERE GROUP_KEY = '%s'", strings.ReplaceAll(groupKey, "'", "''"))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var jobs []*JobStatus
	for rows.Next() {
		status, err := scanJobStatus(rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, status)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

func isRawImportUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "syntax") ||
		strings.Contains(msg, "parse") ||
		strings.Contains(msg, "near 'raw'") ||
		strings.Contains(msg, "near \"raw\"") ||
		strings.Contains(msg, "not supported")
}

func scanJobStatus(rows *sql.Rows) (*JobStatus, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(cols) == 3 && strings.EqualFold(cols[2], "Raw_Stats") {
		return scanRawJobStatus(rows)
	}
	return scanLegacyJobStatus(rows)
}

func scanRawJobStatus(rows *sql.Rows) (*JobStatus, error) {
	var (
		jobID    int64
		groupKey sql.NullString
		rawStats rawJSONBytes
	)

	if err := rows.Scan(&jobID, &groupKey, &rawStats); err != nil {
		return nil, errors.Trace(err)
	}
	if len(rawStats) == 0 {
		return nil, errors.New("raw stats is empty")
	}

	stats := &importer.RawImportJobStats{}
	if err := json.Unmarshal(rawStats, stats); err != nil {
		return nil, errors.Trace(err)
	}
	// Ensure the top-level columns and JSON are consistent.
	stats.JobID = jobID
	if groupKey.Valid {
		stats.GroupKey = groupKey.String
	} else {
		stats.GroupKey = ""
	}
	return jobStatusFromRawStats(stats), nil
}

type rawJSONBytes []byte

func (b *rawJSONBytes) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*b = nil
	case []byte:
		*b = append((*b)[:0], v...)
	case string:
		*b = append((*b)[:0], v...)
	case json.Marshaler:
		data, err := v.MarshalJSON()
		if err != nil {
			return errors.Trace(err)
		}
		*b = append((*b)[:0], data...)
	default:
		return errors.Errorf("cannot scan raw stats from %T", src)
	}
	return nil
}

func jobStatusFromRawStats(stats *importer.RawImportJobStats) *JobStatus {
	status := &JobStatus{
		JobID:               stats.JobID,
		GroupKey:            stats.GroupKey,
		DataSource:          stats.DataSource,
		TargetTable:         stats.TargetTable,
		TableID:             stats.TableID,
		Phase:               stats.Phase,
		Status:              stats.Status,
		ContractVersion:     stats.Version,
		StatusCategory:      stats.StatusCategory,
		Terminal:            stats.Terminal,
		SourceFileSizeBytes: stats.SourceFileSizeBytes,
		ResultMessage:       stats.ErrorMessage,
		ErrorMessage:        stats.ErrorMessage,
		Error:               stats.Error,
		Summary:             stats.Summary,
		CreatedBy:           stats.CreatedBy,
		CreatedByRedacted:   stats.CreatedByRedacted,
		CreateTimeUnix:      stats.CreateTimeUnix,
		StartTimeUnix:       stats.StartTimeUnix,
		EndTimeUnix:         stats.EndTimeUnix,
		UpdateTimeUnix:      stats.UpdateTimeUnix,
		CurrentStep:         stats.CurrentStep,
	}
	if stats.Error != nil && status.ErrorMessage == "" {
		status.ErrorMessage = stats.Error.Message
		status.ResultMessage = stats.Error.Message
	}
	if stats.ImportedRows != nil {
		status.ImportedRows = *stats.ImportedRows
	}
	if stats.SourceFileSizeBytes > 0 {
		status.SourceFileSize = strconv.FormatInt(stats.SourceFileSizeBytes, 10)
	}
	status.CreateTime = unixTime(stats.CreateTimeUnix)
	status.StartTime = unixTime(stats.StartTimeUnix)
	status.EndTime = unixTime(stats.EndTimeUnix)
	status.UpdateTime = unixTime(stats.UpdateTimeUnix)
	if stats.CurrentStep != nil {
		fillLegacyStepFields(status, stats.CurrentStep)
	}
	return status
}

func fillLegacyStepFields(status *JobStatus, step *importer.RawImportJobStepStats) {
	status.Step = step.Name
	if step.TotalBytes > 0 {
		status.ProcessedSize = strconv.FormatInt(step.ProcessedBytes, 10)
		status.TotalSize = strconv.FormatInt(step.TotalBytes, 10)
		status.Percent = strconv.FormatInt(int64(float64(step.ProcessedBytes)/float64(step.TotalBytes)*100), 10)
		status.Speed = strconv.FormatInt(step.SpeedBytesPerSec, 10)
		return
	}
	if step.TotalConflicts > 0 {
		status.ProcessedSize = strconv.FormatInt(step.ProcessedConflicts, 10)
		status.TotalSize = strconv.FormatInt(step.TotalConflicts, 10)
		status.Percent = strconv.FormatInt(int64(float64(step.ProcessedConflicts)/float64(step.TotalConflicts)*100), 10)
		status.Speed = strconv.FormatInt(step.SpeedConflictsPerSec, 10)
	}
}

func scanLegacyJobStatus(rows *sql.Rows) (*JobStatus, error) {
	var (
		id             int64
		groupKey       sql.NullString
		dataSource     string
		targetTable    string
		tableID        int64
		phase          string
		status         string
		sourceFileSize string
		importedRows   sql.NullInt64
		resultMessage  sql.NullString
		createTimeStr  string
		startTimeStr   sql.NullString
		endTimeStr     sql.NullString
		createdBy      string
		updateTimeStr  sql.NullString
		step           sql.NullString
		processedSize  sql.NullString
		totalSize      sql.NullString
		percent        sql.NullString
		speed          sql.NullString
		eta            sql.NullString
	)

	err := rows.Scan(
		&id, &groupKey, &dataSource, &targetTable, &tableID,
		&phase, &status, &sourceFileSize, &importedRows, &resultMessage,
		&createTimeStr, &startTimeStr, &endTimeStr, &createdBy, &updateTimeStr,
		&step, &processedSize, &totalSize, &percent, &speed, &eta,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Parse times
	createTime := parseTime(createTimeStr)
	startTime := parseNullTime(startTimeStr)
	endTime := parseNullTime(endTimeStr)
	updateTime := parseNullTime(updateTimeStr)

	return &JobStatus{
		JobID:          id,
		GroupKey:       groupKey.String,
		DataSource:     dataSource,
		TargetTable:    targetTable,
		TableID:        tableID,
		Phase:          phase,
		Status:         status,
		SourceFileSize: sourceFileSize,
		ImportedRows:   importedRows.Int64,
		ResultMessage:  resultMessage.String,
		CreateTime:     createTime,
		StartTime:      startTime,
		EndTime:        endTime,
		CreatedBy:      createdBy,
		UpdateTime:     updateTime,
		Step:           step.String,
		ProcessedSize:  processedSize.String,
		TotalSize:      totalSize.String,
		Percent:        percent.String,
		Speed:          speed.String,
		ETA:            eta.String,
	}, nil
}

func scanJobID(rows *sql.Rows) (int64, error) {
	cols, err := rows.Columns()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(cols) == 0 {
		return 0, ErrNoJobIDReturned
	}

	dest := make([]any, len(cols))
	var jobID int64
	dest[0] = &jobID
	// We only need Job_ID; scan the rest into dummy values.
	for i := 1; i < len(cols); i++ {
		dest[i] = new(any)
	}
	if err := rows.Scan(dest...); err != nil {
		return 0, errors.Trace(err)
	}
	return jobID, nil
}

func scanGroupStatus(rows *sql.Rows) (*GroupStatus, error) {
	var (
		groupKey        string
		totalJobs       int64
		pending         int64
		running         int64
		completed       int64
		failed          int64
		cancelled       int64
		firstCreateTime sql.NullString
		lastUpdateTime  sql.NullString
	)

	if err := rows.Scan(&groupKey, &totalJobs, &pending, &running, &completed, &failed, &cancelled, &firstCreateTime, &lastUpdateTime); err != nil {
		return nil, errors.Trace(err)
	}

	return &GroupStatus{
		GroupKey:           groupKey,
		TotalJobs:          totalJobs,
		Pending:            pending,
		Running:            running,
		Completed:          completed,
		Failed:             failed,
		Cancelled:          cancelled,
		FirstJobCreateTime: parseNullTime(firstCreateTime),
		LastJobUpdateTime:  parseNullTime(lastUpdateTime),
	}, nil
}

// CancelJob cancels an import job
func (m *jobManager) CancelJob(ctx context.Context, jobID int64) error {
	query := fmt.Sprintf("CANCEL IMPORT JOB %d", jobID)
	_, err := m.db.ExecContext(ctx, query)
	return errors.Trace(err)
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(timeLayout, s)
	return t
}

func parseNullTime(ns sql.NullString) time.Time {
	if !ns.Valid {
		return time.Time{}
	}
	return parseTime(ns.String)
}

func unixTime(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0)
}
