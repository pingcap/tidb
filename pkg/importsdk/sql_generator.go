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
	"net/url"
	"strings"

	"github.com/pingcap/tidb/pkg/lightning/config"
)

// SQLGenerator defines the interface for generating IMPORT INTO SQL
type SQLGenerator interface {
	GenerateImportSQL(tableMeta *TableMeta, options *ImportOptions) (string, error)
}

type sqlGenerator struct{}

// NewSQLGenerator creates a new SQLGenerator
func NewSQLGenerator() SQLGenerator {
	return &sqlGenerator{}
}

// GenerateImportSQL generates the IMPORT INTO SQL statement
func (g *sqlGenerator) GenerateImportSQL(tableMeta *TableMeta, options *ImportOptions) (string, error) {
	var sb strings.Builder
	sb.WriteString("IMPORT INTO ")
	sb.WriteString(escapeIdentifier(tableMeta.Database))
	sb.WriteString(".")
	sb.WriteString(escapeIdentifier(tableMeta.Table))

	path := tableMeta.WildcardPath
	if options.ResourceParameters != "" {
		u, err := url.Parse(path)
		if err == nil {
			if u.RawQuery != "" {
				u.RawQuery += "&" + options.ResourceParameters
			} else {
				u.RawQuery = options.ResourceParameters
			}
			path = u.String()
		}
	}

	sb.WriteString(" FROM '")
	sb.WriteString(path)
	sb.WriteString("'")

	if options.Format != "" {
		sb.WriteString(" FORMAT '")
		sb.WriteString(options.Format)
		sb.WriteString("'")
	}

	opts, err := g.buildOptions(options)
	if err != nil {
		return "", err
	}
	if len(opts) > 0 {
		sb.WriteString(" WITH ")
		sb.WriteString(strings.Join(opts, ", "))
	}

	return sb.String(), nil
}

func (g *sqlGenerator) buildOptions(options *ImportOptions) ([]string, error) {
	var opts []string
	if options.Thread > 0 {
		opts = append(opts, fmt.Sprintf("THREAD=%d", options.Thread))
	}
	if options.DiskQuota != "" {
		opts = append(opts, fmt.Sprintf("DISK_QUOTA='%s'", options.DiskQuota))
	}
	if options.MaxWriteSpeed != "" {
		opts = append(opts, fmt.Sprintf("MAX_WRITE_SPEED='%s'", options.MaxWriteSpeed))
	}
	if options.SplitFile {
		opts = append(opts, "SPLIT_FILE")
	}
	if options.RecordErrors > 0 {
		opts = append(opts, fmt.Sprintf("RECORD_ERRORS=%d", options.RecordErrors))
	}
	if options.Detached {
		opts = append(opts, "DETACHED")
	}
	if options.CloudStorageURI != "" {
		opts = append(opts, fmt.Sprintf("CLOUD_STORAGE_URI='%s'", options.CloudStorageURI))
	}
	if options.GroupKey != "" {
		opts = append(opts, fmt.Sprintf("GROUP_KEY='%s'", escapeString(options.GroupKey)))
	}
	if options.SkipRows > 0 {
		opts = append(opts, fmt.Sprintf("SKIP_ROWS=%d", options.SkipRows))
	}
	if options.CharacterSet != "" {
		opts = append(opts, fmt.Sprintf("CHARACTER_SET='%s'", escapeString(options.CharacterSet)))
	}
	if options.ChecksumTable != "" {
		opts = append(opts, fmt.Sprintf("CHECKSUM_TABLE='%s'", escapeString(options.ChecksumTable)))
	}
	if options.DisableTiKVImportMode {
		opts = append(opts, "DISABLE_TIKV_IMPORT_MODE")
	}
	if options.MaxEngineSize != "" {
		opts = append(opts, fmt.Sprintf("MAX_ENGINE_SIZE='%s'", escapeString(options.MaxEngineSize)))
	}
	if options.DisablePrecheck {
		opts = append(opts, "DISABLE_PRECHECK")
	}

	if options.CSVConfig != nil && options.Format == "csv" {
		csvOpts, err := g.buildCSVOptions(options.CSVConfig)
		if err != nil {
			return nil, err
		}
		opts = append(opts, csvOpts...)
	}
	return opts, nil
}

func (g *sqlGenerator) buildCSVOptions(csvConfig *config.CSVConfig) ([]string, error) {
	var opts []string
	if csvConfig.FieldsTerminatedBy != "" {
		opts = append(opts, fmt.Sprintf("FIELDS_TERMINATED_BY='%s'", escapeString(csvConfig.FieldsTerminatedBy)))
	}
	if csvConfig.FieldsEnclosedBy != "" {
		opts = append(opts, fmt.Sprintf("FIELDS_ENCLOSED_BY='%s'", escapeString(csvConfig.FieldsEnclosedBy)))
	}
	if csvConfig.FieldsEscapedBy != "" {
		opts = append(opts, fmt.Sprintf("FIELDS_ESCAPED_BY='%s'", escapeString(csvConfig.FieldsEscapedBy)))
	}
	if csvConfig.LinesTerminatedBy != "" {
		opts = append(opts, fmt.Sprintf("LINES_TERMINATED_BY='%s'", escapeString(csvConfig.LinesTerminatedBy)))
	}
	if len(csvConfig.FieldNullDefinedBy) > 0 {
		if len(csvConfig.FieldNullDefinedBy) > 1 {
			return nil, ErrMultipleFieldsDefinedNullBy
		}
		opts = append(opts, fmt.Sprintf("FIELDS_DEFINED_NULL_BY='%s'", escapeString(csvConfig.FieldNullDefinedBy[0])))
	}
	return opts, nil
}

func escapeIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func escapeString(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "''")
}
