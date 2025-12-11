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
	"time"

	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
)

// TableMeta contains metadata for a table to be imported
type TableMeta struct {
	Database     string
	Table        string
	DataFiles    []DataFileMeta
	TotalSize    int64  // In bytes
	WildcardPath string // Wildcard pattern that matches only this table's data files
	SchemaFile   string // Path to the table schema file, if available
}

// DataFileMeta contains metadata for a data file
type DataFileMeta struct {
	Path        string
	Size        int64
	Format      mydump.SourceType
	Compression mydump.Compression
}

// ImportOptions wraps the options for IMPORT INTO statement.
// It reuses structures from executor/importer where possible.
type ImportOptions struct {
	Format                string
	CSVConfig             *config.CSVConfig
	Thread                int
	DiskQuota             string
	MaxWriteSpeed         string
	SplitFile             bool
	RecordErrors          int64
	Detached              bool
	CloudStorageURI       string
	GroupKey              string
	SkipRows              int
	CharacterSet          string
	ChecksumTable         string
	DisableTiKVImportMode bool
	MaxEngineSize         string
	DisablePrecheck       bool
}

// GroupStatus represents the aggregated status for a group of import jobs.
type GroupStatus struct {
	GroupKey           string
	TotalJobs          int64
	Pending            int64
	Running            int64
	Completed          int64
	Failed             int64
	Cancelled          int64
	FirstJobCreateTime time.Time
	LastJobUpdateTime  time.Time
}

// JobStatus represents the status of an import job.
type JobStatus struct {
	JobID          int64
	GroupKey       string
	DataSource     string
	TargetTable    string
	TableID        int64
	Phase          string
	Status         string
	SourceFileSize string
	ImportedRows   int64
	ResultMessage  string
	CreateTime     time.Time
	StartTime      time.Time
	EndTime        time.Time
	CreatedBy      string
	UpdateTime     time.Time
	Step           string
	ProcessedSize  string
	TotalSize      string
	Percent        string
	Speed          string
	ETA            string
}

// IsFinished returns true if the job is finished successfully.
func (s *JobStatus) IsFinished() bool {
	return s.Status == "finished"
}

// IsFailed returns true if the job failed.
func (s *JobStatus) IsFailed() bool {
	return s.Status == "failed"
}

// IsCancelled returns true if the job was cancelled.
func (s *JobStatus) IsCancelled() bool {
	return s.Status == "cancelled"
}

// IsCompleted returns true if the job is in a terminal state.
func (s *JobStatus) IsCompleted() bool {
	return s.IsFinished() || s.IsFailed() || s.IsCancelled()
}
