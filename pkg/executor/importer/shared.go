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

package importer

import "github.com/pingcap/tidb/pkg/importinto"

type (
	// StepSummary records the data processed in one import step.
	StepSummary = importinto.StepSummary
	// Summary records progress across import steps.
	Summary = importinto.Summary
	// Chunk describes a file chunk to import.
	Chunk = importinto.Chunk
	// QueryChunk carries a chunk of query results to import.
	QueryChunk = importinto.QueryChunk
	// JobInfo describes an import job.
	JobInfo = importinto.JobInfo
)

// Compatibility names for the shared import job status and step values.
const (
	jobStatusPending          = importinto.JobStatusPending
	JobStatusRunning          = importinto.JobStatusRunning
	jogStatusCancelled        = importinto.JobStatusCancelled
	jobStatusFailed           = importinto.JobStatusFailed
	JobStatusFinished         = importinto.JobStatusFinished
	jobStepNone               = importinto.JobStepNone
	JobStepPreparing          = importinto.JobStepPreparing
	JobStepGlobalSorting      = importinto.JobStepGlobalSorting
	JobStepImporting          = importinto.JobStepImporting
	JobStepResolvingConflicts = importinto.JobStepResolvingConflicts
	JobStepValidating         = importinto.JobStepValidating
)
