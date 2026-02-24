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

	"github.com/pingcap/tidb/pkg/executor/importer"
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

// JobStatus is a machine-friendly contract returned by SHOW RAW IMPORT JOB(S).
// It is a type alias to keep the contract centralized in one place.
type JobStatus = importer.RawImportJobStats
