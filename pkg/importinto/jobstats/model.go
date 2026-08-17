// Copyright 2026 PingCAP, Inc.
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

// Package jobstats owns the versioned machine-readable contract returned by
// SHOW RAW IMPORT JOB(S). It is intentionally independent of the executor and
// SDK layers that produce and consume the contract.
package jobstats

// ContractVersion is the supported Raw_Stats JSON contract version.
const ContractVersion = 1

// Stable status categories for machine consumers.
const (
	StatusCategoryPending         = "pending"
	StatusCategoryRunning         = "running"
	StatusCategoryTerminal        = "terminal"
	StatusCategoryAttentionNeeded = "attention_needed"
	StatusCategoryUnknown         = "unknown"
)

// ClassifyStatus derives all stable status metadata from one source of truth.
func ClassifyStatus(status string) (category string, terminal bool) {
	switch status {
	case "pending":
		return StatusCategoryPending, false
	case "running":
		return StatusCategoryRunning, false
	case "finished", "failed", "cancelled":
		return StatusCategoryTerminal, true
	case "awaiting-resolution":
		return StatusCategoryAttentionNeeded, false
	default:
		return StatusCategoryUnknown, false
	}
}

// RawImportJobStats is the versioned machine-readable import-job contract.
type RawImportJobStats struct {
	// JobID and GroupKey are supplied by the top-level SQL columns and are not
	// duplicated in Raw_Stats JSON.
	JobID    int64  `json:"-"`
	GroupKey string `json:"-"`

	Version int `json:"version"`

	DataSource  string `json:"data_source,omitempty"`
	TargetTable string `json:"target_table,omitempty"`
	TableID     int64  `json:"table_id,omitempty"`

	// Phase is the public job phase; CurrentStep is runtime subtask progress.
	Phase          string `json:"job_phase,omitempty"`
	Status         string `json:"status,omitempty"`
	StatusCategory string `json:"status_category,omitempty"`
	Terminal       bool   `json:"terminal"`

	SourceFileSizeBytes int64  `json:"source_file_size_bytes,omitempty"`
	ImportedRows        *int64 `json:"imported_rows,omitempty"`

	Error   *RawImportJobError   `json:"error,omitempty"`
	Summary *RawImportJobSummary `json:"summary,omitempty"`

	CurrentStep *RawImportJobStepStats `json:"current_step,omitempty"`

	// Times are UNIX seconds.
	CreateTimeUnix int64 `json:"create_time_unix,omitempty"`
	StartTimeUnix  int64 `json:"start_time_unix,omitempty"`
	EndTimeUnix    int64 `json:"end_time_unix,omitempty"`
	UpdateTimeUnix int64 `json:"update_time_unix,omitempty"`

	CreatedBy string `json:"created_by,omitempty"`
}

// RawImportJobError contains one canonical diagnostic message plus actionable
// metadata. Job outcome and terminal state are represented by the parent stats.
type RawImportJobError struct {
	Message            string `json:"message,omitempty"`
	Retryable          *bool  `json:"retryable,omitempty"`
	UserActionRequired bool   `json:"user_action_required,omitempty"`
}

// RawImportJobSummary is a persisted summary snapshot. It may be partial while
// Terminal is false; consumers must use Terminal to determine finality.
type RawImportJobSummary struct {
	Steps            []RawImportJobStepSummary `json:"steps,omitempty"`
	ImportedRows     int64                     `json:"imported_rows,omitempty"`
	ConflictRows     uint64                    `json:"conflict_rows,omitempty"`
	TooManyConflicts bool                      `json:"too_many_conflicts,omitempty"`
}

// RawImportJobStepSummary describes persisted per-step job-domain totals.
type RawImportJobStepSummary struct {
	Name           string `json:"name,omitempty"`
	InputBytes     int64  `json:"input_bytes,omitempty"`
	InputRows      int64  `json:"input_rows,omitempty"`
	InputConflicts int64  `json:"input_conflicts,omitempty"`
}

// RawImportJobStepStats describes progress for the currently running step.
type RawImportJobStepStats struct {
	Name string `json:"name,omitempty"`

	ProcessedBytes   int64 `json:"processed_bytes,omitempty"`
	TotalBytes       int64 `json:"total_bytes,omitempty"`
	SpeedBytesPerSec int64 `json:"speed_bytes_per_sec,omitempty"`

	ProcessedConflicts   int64 `json:"processed_conflicts,omitempty"`
	TotalConflicts       int64 `json:"total_conflicts,omitempty"`
	SpeedConflictsPerSec int64 `json:"speed_conflicts_per_sec,omitempty"`

	RemainingSeconds *int64 `json:"remaining_seconds,omitempty"`
}

// IsFinished reports successful completion.
func (s *RawImportJobStats) IsFinished() bool { return s.Status == "finished" }

// IsFailed reports failed completion.
func (s *RawImportJobStats) IsFailed() bool { return s.Status == "failed" }

// IsCancelled reports cancelled completion.
func (s *RawImportJobStats) IsCancelled() bool { return s.Status == "cancelled" }

// IsCompleted consumes the contract's normalized terminal signal.
func (s *RawImportJobStats) IsCompleted() bool { return s.Terminal }
