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

package importinto

import (
	"github.com/pingcap/tidb/pkg/types"
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
	JobStatusPending = "pending"
	// JobStatusRunning exported since it's used in show import jobs
	JobStatusRunning   = "running"
	JobStatusCancelled = "cancelled"
	JobStatusFailed    = "failed"
	// JobStatusFinished exported since it's used in show import jobs
	JobStatusFinished = "finished"

	// when the job is finished, step will be set to none.
	JobStepNone = ""
	// JobStepPreparing is used by prepare-enabled jobs before generating
	// first business-step subtasks.
	JobStepPreparing = "preparing"
	// JobStepGlobalSorting is the first step when using global sort,
	// step goes from none -> global-sorting -> importing -> validating -> none.
	// for prepare-enabled global sort, step goes from none -> preparing ->
	// global-sorting -> importing -> validating -> none.
	JobStepGlobalSorting = "global-sorting"
	// JobStepImporting is the first step when using local sort,
	// step goes from none -> importing -> validating -> none.
	// when used in global sort, it means importing the sorted data.
	// when used in local sort, it means encode&sort data and then importing the data.
	JobStepImporting = "importing"
	// JobStepResolvingConflicts is the step after importing to resolve conflicts,
	// it's used in global sort.
	JobStepResolvingConflicts = "resolving-conflicts"
	JobStepValidating         = "validating"
)

// JobInfo is the information of import into job.
type JobInfo struct {
	ID             int64
	CreateTime     types.Time
	StartTime      types.Time
	UpdateTime     types.Time
	EndTime        types.Time
	TableSchema    string
	TableName      string
	TableID        int64
	CreatedBy      string
	Parameters     ImportParameters
	SourceFileSize int64
	Status         string
	// Step corresponds to the `phase` field in `SHOW IMPORT JOB`
	// Here we just use the same name as in distributed framework.
	Step string
	// The summary of the job, it will store info for each step of the import and
	// will be updated when switching to a new step.
	// If the ingest step is finished, the number of ingested rows will also stored in it.
	Summary      *Summary
	ErrorMessage string
	GroupKey     string
}

// CanCancel returns whether the job can be cancelled.
func (j *JobInfo) CanCancel() bool {
	return j.Status == JobStatusPending || j.Status == JobStatusRunning
}

// IsCancelled returns whether the job has been cancelled.
func (j *JobInfo) IsCancelled() bool {
	return j.Status == JobStatusCancelled
}

// IsSuccess returns whether the job is successful.
func (j *JobInfo) IsSuccess() bool {
	return j.Status == JobStatusFinished
}

// IsSourceFileSizeUnknown returns whether source_file_size has not been
// determined yet for async-prepare jobs.
func (j *JobInfo) IsSourceFileSizeUnknown() bool {
	// if the job is not using async prepare, source_file_size is determined
	// when creating the job, and it will never be 0 since we check total file
	// size > 0 in precheck.
	if j.SourceFileSize > 0 {
		return false
	}
	if j.Status == JobStatusPending {
		return true
	}
	return j.Status == JobStatusRunning && j.Step == JobStepPreparing
}
