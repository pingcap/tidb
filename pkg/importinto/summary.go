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

// StepSummary records the number of data involved in each step.
// The data stored might be inaccurate, such as the number of rows in encode step.
type StepSummary struct {
	Bytes  int64 `json:"input-bytes,omitempty"`
	RowCnt int64 `json:"input-rows,omitempty"`
}

// Summary records the amount of data to be processed in each import job step.
// This information will be saved into tidb_import_jobs table after the job is finished.
type Summary struct {
	// EncodeSummary stores source bytes and row counts for the encode step.
	EncodeSummary StepSummary `json:"encode-summary,omitempty"`

	// MergeSummary stores merged bytes and row counts for the merge step.
	MergeSummary StepSummary `json:"merge-summary,omitempty"`

	// IngestSummary stores bytes and row counts for the ingest step.
	IngestSummary StepSummary `json:"ingest-summary,omitempty"`

	// CollectConflictsSummary stores conflict KV pair counts for the
	// collect-conflicts step. RowCnt is used as the counter.
	CollectConflictsSummary StepSummary `json:"collect-conflicts-summary,omitempty"`

	// ResolveConflictsSummary stores conflict KV pair counts for the
	// resolve-conflicts step. RowCnt is used as the counter.
	ResolveConflictsSummary StepSummary `json:"resolve-conflicts-summary,omitempty"`

	// ImportedRows is the number of rows imported into TiKV.
	// conflicted rows are excluded from this count if using global-sort.
	ImportedRows   int64  `json:"row-count,omitempty"`
	ConflictRowCnt uint64 `json:"conflict-row-count,omitempty"`
	// TooManyConflicts indicates there are too many conflicted rows that we
	// cannot deduplicate during collecting its checksum, so we will skip later
	// checksum step.
	TooManyConflicts bool `json:"too-many-conflicts,omitempty"`
}
