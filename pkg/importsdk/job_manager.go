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
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/importinto/jobstats"
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
	db             *sql.DB
	rawUnsupported atomic.Bool
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
		status, err := scanLegacyStatus(rows)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return status.JobID, nil
	}

	if err := rows.Err(); err != nil {
		return 0, errors.Trace(err)
	}

	return 0, ErrNoJobIDReturned
}

// GetJobStatus gets the status of an import job
func (m *jobManager) GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	if m.rawUnsupported.Load() {
		return m.getLegacyStatus(ctx, jobID)
	}
	query := fmt.Sprintf("SHOW RAW IMPORT JOB %d", jobID)
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		if isRawUnsupportedErr(err) {
			m.rawUnsupported.Store(true)
			return m.getLegacyStatus(ctx, jobID)
		}
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		return scanRawStatus(rows)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	return nil, ErrJobNotFound
}

func (m *jobManager) getLegacyStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	query := fmt.Sprintf("SHOW IMPORT JOB %d", jobID)
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	if rows.Next() {
		return scanLegacyStatus(rows)
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
	if m.rawUnsupported.Load() {
		return m.getLegacyGroupJobs(ctx, groupKey)
	}
	query := fmt.Sprintf("SHOW RAW IMPORT JOBS WHERE GROUP_KEY = '%s'", strings.ReplaceAll(groupKey, "'", "''"))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		if isRawUnsupportedErr(err) {
			m.rawUnsupported.Store(true)
			return m.getLegacyGroupJobs(ctx, groupKey)
		}
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var jobs []*JobStatus
	for rows.Next() {
		status, err := scanRawStatus(rows)
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

func (m *jobManager) getLegacyGroupJobs(ctx context.Context, groupKey string) ([]*JobStatus, error) {
	query := fmt.Sprintf("SHOW IMPORT JOBS WHERE GROUP_KEY = '%s'", strings.ReplaceAll(groupKey, "'", "''"))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var jobs []*JobStatus
	for rows.Next() {
		status, err := scanLegacyStatus(rows)
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

func isRawUnsupportedErr(err error) bool {
	// 1064 is returned by old servers that cannot parse SHOW RAW, while 1235
	// is returned by the classic kernel where SHOW RAW is intentionally disabled.
	var code int
	switch err := errors.Cause(err).(type) {
	case *drivermysql.MySQLError:
		code = int(err.Number)
	case interface{ Code() errors.ErrCode }:
		// The testkit database/sql driver returns a normalized TiDB error.
		code = int(err.Code())
	default:
		return false
	}
	return code == errno.ErrParse || code == errno.ErrNotSupportedYet
}

func scanRawStatus(rows *sql.Rows) (*JobStatus, error) {
	var (
		jobID    int64
		groupKey sql.NullString
		rawStats string
	)

	if err := rows.Scan(&jobID, &groupKey, &rawStats); err != nil {
		return nil, errors.Trace(err)
	}

	stats := &jobstats.RawImportJobStats{}
	if err := json.Unmarshal([]byte(rawStats), stats); err != nil {
		return nil, errors.Trace(err)
	}
	if stats.Version != jobstats.ContractVersion {
		return nil, errors.Errorf(
			"unsupported SHOW RAW IMPORT JOB stats contract version %d (supported version: %d)",
			stats.Version, jobstats.ContractVersion,
		)
	}
	// Populate identity fields that are intentionally omitted from Raw_Stats.
	stats.JobID = jobID
	if groupKey.Valid {
		stats.GroupKey = groupKey.String
	} else {
		stats.GroupKey = ""
	}
	return statusFromRaw(stats), nil
}

func statusFromRaw(stats *jobstats.RawImportJobStats) *JobStatus {
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
		Error:               stats.Error,
		Summary:             stats.Summary,
		CreatedBy:           stats.CreatedBy,
		CreateTimeUnix:      stats.CreateTimeUnix,
		StartTimeUnix:       stats.StartTimeUnix,
		EndTimeUnix:         stats.EndTimeUnix,
		UpdateTimeUnix:      stats.UpdateTimeUnix,
		CurrentStep:         stats.CurrentStep,
	}
	if stats.Error != nil {
		status.ErrorMessage = stats.Error.Message
	}
	if stats.ImportedRows != nil {
		status.ImportedRows = *stats.ImportedRows
	}
	if stats.SourceFileSizeBytes > 0 {
		status.SourceFileSize = units.BytesSize(float64(stats.SourceFileSizeBytes))
	} else if stats.Status == "pending" || (stats.Status == importer.JobStatusRunning && stats.Phase == importer.JobStepPreparing) {
		status.SourceFileSize = "N/A"
	} else {
		status.SourceFileSize = units.BytesSize(0)
	}
	status.ResultMessage = legacyResult(stats)
	if status.ResultMessage == "" && stats.Status != importer.JobStatusFinished {
		status.ResultMessage = status.ErrorMessage
	}
	status.CreateTime = unixTime(stats.CreateTimeUnix)
	status.StartTime = unixTime(stats.StartTimeUnix)
	status.EndTime = unixTime(stats.EndTimeUnix)
	status.UpdateTime = unixTime(stats.UpdateTimeUnix)
	if stats.CurrentStep != nil {
		fillLegacyProgress(status, stats.CurrentStep)
	}
	return status
}

func fillLegacyProgress(status *JobStatus, step *jobstats.RawImportJobStepStats) {
	status.Step = step.Name
	conflictStep := step.Name == "collect-conflicts" || step.Name == "conflict-resolution"
	var processed, total int64
	if conflictStep {
		processed, total = step.ProcessedConflicts, step.TotalConflicts
		status.ProcessedSize = fmt.Sprintf("%d conflicts", processed)
		status.TotalSize = fmt.Sprintf("%d conflicts", total)
		status.Speed = fmt.Sprintf("%d conflicts/s", step.SpeedConflictsPerSec)
	} else {
		processed, total = step.ProcessedBytes, step.TotalBytes
		status.ProcessedSize = units.BytesSize(float64(processed))
		status.TotalSize = units.BytesSize(float64(total))
		status.Speed = fmt.Sprintf("%s/s", units.BytesSize(float64(step.SpeedBytesPerSec)))
	}
	if step.Name == "post-process" || step.Name == "init" {
		status.Percent = "N/A"
	} else {
		percent := int64(0)
		if total > 0 {
			percent = min(int64(float64(processed)/float64(total)*100), 100)
		}
		status.Percent = strconv.FormatInt(percent, 10)
	}
	status.ETA = "N/A"
	if step.RemainingSeconds != nil {
		status.ETA = importinto.FormatSecondAsTime(*step.RemainingSeconds)
	}
}

func legacyResult(stats *jobstats.RawImportJobStats) string {
	if stats.Status != importer.JobStatusFinished {
		if stats.Error != nil {
			return stats.Error.Message
		}
		return ""
	}
	if stats.Summary == nil {
		return ""
	}
	items := make([]string, 0, 2)
	if stats.Summary.ConflictRows > 0 {
		items = append(items, fmt.Sprintf("%d conflicted rows.", stats.Summary.ConflictRows))
	}
	if stats.Summary.TooManyConflicts {
		items = append(items, "Too many conflicted rows, checksum skipped.")
	}
	return strings.Join(items, " ")
}

func scanLegacyStatus(rows *sql.Rows) (*JobStatus, error) {
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
