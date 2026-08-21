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
	Database        string
	Table           string
	DataFiles       []DataFileMeta
	TotalSize       int64  // Estimated uncompressed/row-oriented size in bytes
	ObjectCount     int64  // Number of physical source data objects
	TotalObjectSize int64  // Storage-reported source object size in bytes
	WildcardPath    string // Wildcard pattern that matches only this table's data files
	SchemaFile      string // Path to the table schema file, if available
}

// DataFileMeta contains metadata for a data file
type DataFileMeta struct {
	Path        string
	Size        int64 // Estimated uncompressed/row-oriented size in bytes
	ObjectSize  int64 // Storage-reported object size in bytes
	Format      mydump.SourceType
	Compression mydump.Compression
}

// SourceLayout identifies the source-file layout selected by automatic
// mapping.
type SourceLayout string

const (
	// SourceLayoutDefault is the existing Dumpling/generic file layout.
	SourceLayoutDefault SourceLayout = "default"
	// SourceLayoutAuroraRDSSnapshot is the native Aurora/RDS snapshot-export
	// directory layout.
	SourceLayoutAuroraRDSSnapshot SourceLayout = "aurora-rds-snapshot"
)

// SourceInventory summarizes the source objects considered by automatic
// mapping.
type SourceInventory struct {
	Complete              bool
	ScannedObjectCount    int64
	ImportableObjectCount int64
	MappedObjectCount     int64
	TotalObjectBytes      int64
	// Digest identifies the importable object paths and storage-reported sizes.
	Digest string
}

// SourceLayoutEvidence describes the path evidence used to identify a source
// layout.
type SourceLayoutEvidence struct {
	// ExportRoot is relative to the configured source path and is empty when
	// that path already points at the export-task root.
	ExportRoot string
	// PathForm is "batched", "direct", or "mixed" for an Aurora/RDS snapshot
	// export.
	PathForm string
}

// SourceScanResult is the complete automatic-mapping result returned to Cloud
// Import callers.
type SourceScanResult struct {
	Layout    SourceLayout
	Tables    []*TableMeta
	Inventory SourceInventory
	Evidence  SourceLayoutEvidence
}

// TableDataSizeEstimate contains the size estimation for a table import.
type TableDataSizeEstimate struct {
	Database   string
	Table      string
	SourceSize int64
	// TiKVSize is the estimated encoded KV size for a single replica.
	TiKVSize int64
}

// ImportDataSizeEstimate contains the aggregated size estimation for an import.
type ImportDataSizeEstimate struct {
	Tables []TableDataSizeEstimate
	// TotalSourceSize is the aggregated source size of all tables.
	TotalSourceSize int64
	// TotalTiKVSize is the aggregated encoded KV size for a single replica.
	TotalTiKVSize int64
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
	DisablePrecheck       bool
	ResourceParameters    string
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
