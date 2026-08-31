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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/filter"
)

type auroraSnapshotRouter struct{}

func (auroraSnapshotRouter) Route(path string) (*RouteResult, error) {
	parsed, matched, err := ParseAuroraSnapshotFilePath(path)
	if err != nil || !matched {
		// SourceScanner performs source-wide validation and returns its stable,
		// machine-readable ambiguity/unsupported-path errors. Do not make loader
		// construction fail before that validation can run.
		return nil, nil
	}
	return &RouteResult{
		Table: filter.Table{Schema: parsed.Schema, Name: parsed.Table},
		Type:  SourceTypeParquet,
	}, nil
}

// ErrAmbiguousAuroraSnapshotPath indicates that more than one directory
// component could be interpreted as the schema.table base directory.
var ErrAmbiguousAuroraSnapshotPath = errors.New("ambiguous Aurora/RDS snapshot-export path")

// ErrUnsupportedAuroraSnapshotPath indicates that a path resembles an AWS
// snapshot-export object but its database/schema.table hierarchy is invalid.
var ErrUnsupportedAuroraSnapshotPath = errors.New("unsupported Aurora/RDS snapshot-export path")

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
	// AuroraSnapshotPathFormMixed means both supported leaf layouts were found
	// in one source inventory.
	AuroraSnapshotPathFormMixed AuroraSnapshotPathForm = "mixed"
)

// AuroraSnapshotFilePath is the structural interpretation of an Aurora/RDS
// snapshot-export parquet object.
type AuroraSnapshotFilePath struct {
	// ExportRoot is relative to the configured source path. It is empty when
	// the source path already points at the export-task root.
	ExportRoot string
	// Database is the AWS export-path database directory. It is distinct from
	// Schema, which is the MySQL schema used for import routing.
	Database string
	// Schema is the MySQL schema used for import routing.
	Schema string
	// Table is the MySQL table used for import routing.
	Table string
	// Form describes the observed AWS snapshot-export leaf layout.
	Form AuroraSnapshotPathForm
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
	if len(components) < 3 || !strings.HasPrefix(strings.ToLower(components[len(components)-1]), "part-") {
		return nil, false, nil
	}

	form := AuroraSnapshotPathFormDirect
	tableComponentIndex := len(components) - 2
	if len(components) >= 4 && isDecimal(components[len(components)-2]) {
		form = AuroraSnapshotPathFormBatched
		tableComponentIndex--
	}
	databaseComponentIndex := tableComponentIndex - 1
	if databaseComponentIndex < 0 {
		return nil, true, ErrUnsupportedAuroraSnapshotPath
	}

	unescape := func(value string) (string, error) {
		result, err := url.PathUnescape(value)
		if err != nil {
			return "", errors.Trace(err)
		}
		return result, nil
	}

	database, err := unescape(components[databaseComponentIndex])
	if err != nil {
		return nil, true, errors.Annotate(err, "invalid escaped database name")
	}

	// Aurora MySQL uses the database name as the schema name. Match the decoded
	// database against every possible separator so dots inside an identifier do
	// not make us split schema.table at the wrong position.
	tableComponent := components[tableComponentIndex]
	var schema, table string
	for separator := strings.IndexByte(tableComponent, '.'); separator >= 0; {
		decodedSchema, schemaErr := unescape(tableComponent[:separator])
		decodedTable, tableErr := unescape(tableComponent[separator+1:])
		if schemaErr != nil {
			return nil, true, errors.Annotate(schemaErr, "invalid escaped schema name")
		}
		if tableErr != nil {
			return nil, true, errors.Annotate(tableErr, "invalid escaped table name")
		}
		if decodedSchema == database && decodedTable != "" {
			if schema != "" {
				return nil, true, ErrAmbiguousAuroraSnapshotPath
			}
			schema, table = decodedSchema, decodedTable
		}
		next := strings.IndexByte(tableComponent[separator+1:], '.')
		if next < 0 {
			break
		}
		separator += next + 1
	}
	if schema == "" {
		return nil, true, ErrUnsupportedAuroraSnapshotPath
	}

	return &AuroraSnapshotFilePath{
		ExportRoot: strings.Join(components[:databaseComponentIndex], "/"),
		Database:   database,
		Schema:     schema,
		Table:      table,
		Form:       form,
	}, true, nil
}

func isDecimal(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
