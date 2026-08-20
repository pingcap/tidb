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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
)

// SourceScanErrorCode is a stable machine-readable automatic-mapping failure.
type SourceScanErrorCode string

const (
	// SourceScanErrorMixedLayout indicates that only part of the importable
	// source follows the detected layout.
	SourceScanErrorMixedLayout SourceScanErrorCode = "mixed_layout"
	// SourceScanErrorIncompleteScan indicates that source enumeration did not
	// finish.
	SourceScanErrorIncompleteScan SourceScanErrorCode = "incomplete_scan"
	// SourceScanErrorAmbiguousLayout indicates that an object has more than one
	// valid source-layout interpretation.
	SourceScanErrorAmbiguousLayout SourceScanErrorCode = "ambiguous_layout"
	// SourceScanErrorMultipleExportRoots indicates that one source URI contains
	// objects from multiple snapshot export roots.
	SourceScanErrorMultipleExportRoots SourceScanErrorCode = "multiple_export_roots"
	// SourceScanErrorUnsupportedPath indicates that a snapshot-export object
	// cannot be interpreted safely.
	SourceScanErrorUnsupportedPath SourceScanErrorCode = "unsupported_path"
	// SourceScanErrorNoImportableFiles indicates that no source data object was
	// mapped.
	SourceScanErrorNoImportableFiles SourceScanErrorCode = "no_importable_files"
)

// SourceScanError reports an automatic-mapping failure without requiring Cloud
// Import callers to parse an error string.
type SourceScanError struct {
	Code    SourceScanErrorCode
	Count   int64
	Samples []string
	Cause   error
}

// Error implements error.
func (e *SourceScanError) Error() string {
	message := fmt.Sprintf("source scan failed: %s", e.Code)
	if e.Count > 0 {
		message += fmt.Sprintf(" (count: %d)", e.Count)
	}
	if len(e.Samples) > 0 {
		message += ": " + strings.Join(e.Samples, ", ")
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the underlying parsing or storage error, if any.
func (e *SourceScanError) Unwrap() error {
	return e.Cause
}

var (
	// ErrNoDatabasesFound indicates that the dump source contains no recognizable databases.
	ErrNoDatabasesFound = errors.New("no databases found in the source path")
	// ErrSchemaNotFound indicates the target schema doesn't exist in the dump source.
	ErrSchemaNotFound = errors.New("schema not found")
	// ErrTableNotFound indicates the target table doesn't exist in the dump source.
	ErrTableNotFound = errors.New("table not found")
	// ErrNoTableDataFiles indicates a table has zero data files and thus cannot proceed.
	ErrNoTableDataFiles = errors.New("no data files for table")
	// ErrWildcardNotSpecific indicates a wildcard cannot uniquely match the table's files.
	ErrWildcardNotSpecific = errors.New("cannot generate a unique wildcard pattern for the table's data files")
	// ErrJobNotFound indicates the job is not found.
	ErrJobNotFound = errors.New("job not found")
	// ErrNoJobIDReturned indicates that the submit job query did not return a job ID.
	ErrNoJobIDReturned = errors.New("no job id returned")
	// ErrInvalidOptions indicates the options are invalid.
	ErrInvalidOptions = errors.New("invalid options")
	// ErrMultipleFieldsDefinedNullBy indicates that multiple FIELDS_DEFINED_NULL_BY values are defined, which is not supported.
	ErrMultipleFieldsDefinedNullBy = errors.New("IMPORT INTO only supports one FIELDS_DEFINED_NULL_BY value")
	// ErrParseStorageURL indicates that the storage backend URL is invalid.
	ErrParseStorageURL = errors.New("failed to parse storage backend URL")
	// ErrCreateExternalStorage indicates that the external storage cannot be created.
	ErrCreateExternalStorage = errors.New("failed to create external storage")
	// ErrCreateLoader indicates that the MyDump loader cannot be created.
	ErrCreateLoader = errors.New("failed to create MyDump loader")
	// ErrCreateSchema indicates that creating schemas and tables failed.
	ErrCreateSchema = errors.New("failed to create schemas and tables")
)
