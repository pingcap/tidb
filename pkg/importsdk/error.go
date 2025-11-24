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

import "github.com/pingcap/errors"

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
)
