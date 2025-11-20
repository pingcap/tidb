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

// Sentinel errors to categorize common failure scenarios for clearer user messages.
var (
	// ErrNoDatabasesFound indicates that the dump source contains no recognizable databases.
	ErrNoDatabasesFound = errors.New("no databases found in the source path")
	// ErrSchemaNotFound indicates the requested schema is not present in the dump source.
	ErrSchemaNotFound = errors.New("schema not found")
	// ErrTableNotFound indicates the requested table is not present in the dump source.
	ErrTableNotFound = errors.New("table not found")
	// ErrNoTableDataFiles indicates a table has zero data files and thus cannot proceed.
	ErrNoTableDataFiles = errors.New("no data files for table")
	// ErrWildcardNotSpecific indicates a wildcard cannot uniquely match the table's files.
	ErrWildcardNotSpecific = errors.New("cannot generate a unique wildcard pattern for the table's data files")
	// ErrJobNotFound indicates the requested import job does not exist.
	ErrJobNotFound = errors.New("import job not found")
	// ErrNoJobInfoReturned indicates an IMPORT INTO statement finished without returning job metadata.
	ErrNoJobInfoReturned = errors.New("IMPORT INTO returned no job info")
)
