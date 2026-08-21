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

package mydump

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
)

// AuroraSnapshotFilePattern matches the table layout documented for Amazon
// Aurora and RDS snapshot exports. The source path may point at the export-task
// root (the first capture is empty) or at one of its ancestors.
//
// Captures:
//  1. path before the database directory
//  2. database directory
//  3. schema name
//  4. table name
//  5. directories below the table directory
//  6. parquet file name
const AuroraSnapshotFilePattern = `(?i)^/?((?:[^/]+/)*)([^/]+)/([^/.]+)\.([^/]+)/((?:[^/]+/)*)([^/]+\.parquet)$`

var auroraSnapshotFileRegexp = regexp.MustCompile(AuroraSnapshotFilePattern)

// ErrAmbiguousAuroraSnapshotPath indicates that more than one directory
// component could be interpreted as the schema.table base directory.
var ErrAmbiguousAuroraSnapshotPath = errors.New("ambiguous Aurora/RDS snapshot-export path")

// AuroraSnapshotPathForm describes which AWS snapshot-export leaf layout was
// observed.
type AuroraSnapshotPathForm string

const (
	// AuroraSnapshotPathFormDirect is the older form that places part files
	// directly below the schema.table directory.
	AuroraSnapshotPathFormDirect AuroraSnapshotPathForm = "direct"
	// AuroraSnapshotPathFormBatched is the current form that places part files
	// in one or more directories below the schema.table directory.
	AuroraSnapshotPathFormBatched AuroraSnapshotPathForm = "batched"
)

// AuroraSnapshotFilePath is the structural interpretation of an Aurora/RDS
// snapshot-export parquet object.
type AuroraSnapshotFilePath struct {
	// ExportRoot is relative to the configured source path. It is empty when
	// the source path already points at the export-task root.
	ExportRoot string
	Database   string
	Schema     string
	Table      string
	Form       AuroraSnapshotPathForm
}

// ParseAuroraSnapshotFilePath parses an Aurora/RDS snapshot-export parquet
// object path. The bool result is false when the path does not have the
// documented directory structure.
func ParseAuroraSnapshotFilePath(path string) (*AuroraSnapshotFilePath, bool, error) {
	normalizedPath := strings.TrimPrefix(strings.ReplaceAll(path, `\`, "/"), "/")
	if !strings.HasSuffix(strings.ToLower(normalizedPath), ".parquet") {
		return nil, false, nil
	}

	components := strings.Split(normalizedPath, "/")
	candidateCount := 0
	// A table base component needs a database component immediately before it
	// and at least the parquet leaf after it.
	for i := 1; i < len(components)-1; i++ {
		schema, table, found := strings.Cut(components[i], ".")
		if found && schema != "" && table != "" {
			candidateCount++
		}
	}
	if candidateCount > 1 {
		return nil, true, ErrAmbiguousAuroraSnapshotPath
	}

	matches := auroraSnapshotFileRegexp.FindStringSubmatch(normalizedPath)
	if len(matches) == 0 {
		return nil, false, nil
	}

	unescape := func(value string) (string, error) {
		result, err := url.PathUnescape(value)
		if err != nil {
			return "", errors.Trace(err)
		}
		return result, nil
	}

	database, err := unescape(matches[2])
	if err != nil {
		return nil, true, errors.Annotate(err, "invalid escaped database name")
	}
	schema, err := unescape(matches[3])
	if err != nil {
		return nil, true, errors.Annotate(err, "invalid escaped schema name")
	}
	table, err := unescape(matches[4])
	if err != nil {
		return nil, true, errors.Annotate(err, "invalid escaped table name")
	}

	form := AuroraSnapshotPathFormDirect
	if matches[5] != "" {
		form = AuroraSnapshotPathFormBatched
	}
	return &AuroraSnapshotFilePath{
		ExportRoot: strings.TrimSuffix(matches[1], "/"),
		Database:   database,
		Schema:     schema,
		Table:      table,
		Form:       form,
	}, true, nil
}
