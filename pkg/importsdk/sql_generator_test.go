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
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
)

func TestGenerateImportSQL(t *testing.T) {
	gen := NewSQLGenerator()

	defaultTableMeta := &TableMeta{
		Database:     "test_db",
		Table:        "test_table",
		WildcardPath: "s3://bucket/path/*.csv",
	}

	tests := []struct {
		name          string
		tableMeta     *TableMeta
		options       *ImportOptions
		expected      string
		expectedError string
	}{
		{
			name: "Basic",
			options: &ImportOptions{
				Format: "csv",
			},
			expected: "IMPORT INTO `test_db`.`test_table` FROM 's3://bucket/path/*.csv' FORMAT 'csv'",
		},
		{
			name: "With S3 Credentials",
			options: &ImportOptions{
				Format:             "csv",
				ResourceParameters: "access-key=ak&endpoint=http%3A%2F%2Fminio%3A9000&secret-access-key=sk",
			},
			expected: "IMPORT INTO `test_db`.`test_table` FROM 's3://bucket/path/*.csv?access-key=ak&endpoint=http%3A%2F%2Fminio%3A9000&secret-access-key=sk' FORMAT 'csv'",
		},
		{
			name: "With Options",
			options: &ImportOptions{
				Format:        "csv",
				Thread:        4,
				Detached:      true,
				MaxWriteSpeed: "100MiB",
			},
			expected: "IMPORT INTO `test_db`.`test_table` FROM 's3://bucket/path/*.csv' FORMAT 'csv' WITH THREAD=4, MAX_WRITE_SPEED='100MiB', DETACHED",
		},
		{
			name: "All Options",
			options: &ImportOptions{
				Format:                "csv",
				Thread:                8,
				DiskQuota:             "100GiB",
				MaxWriteSpeed:         "200MiB",
				SplitFile:             true,
				RecordErrors:          100,
				Detached:              true,
				CloudStorageURI:       "s3://bucket/storage",
				GroupKey:              "group1",
				SkipRows:              1,
				CharacterSet:          "utf8mb4",
				ChecksumTable:         "test_db.checksum_table",
				DisableTiKVImportMode: true,
				DisablePrecheck:       true,
			},
			expected: "IMPORT INTO `test_db`.`test_table` FROM 's3://bucket/path/*.csv' FORMAT 'csv' WITH THREAD=8, DISK_QUOTA='100GiB', MAX_WRITE_SPEED='200MiB', SPLIT_FILE, RECORD_ERRORS=100, DETACHED, CLOUD_STORAGE_URI='s3://bucket/storage', GROUP_KEY='group1', SKIP_ROWS=1, CHARACTER_SET='utf8mb4', CHECKSUM_TABLE='test_db.checksum_table', DISABLE_TIKV_IMPORT_MODE, DISABLE_PRECHECK",
		},
		{
			name: "With CSV Config",
			options: &ImportOptions{
				Format: "csv",
				CSVConfig: &config.CSVConfig{
					FieldsTerminatedBy: ",",
					FieldsEnclosedBy:   "\"",
					FieldsEscapedBy:    "\\",
					LinesTerminatedBy:  "\n",
					FieldNullDefinedBy: []string{"NULL"},
				},
			},
			expected: "IMPORT INTO `test_db`.`test_table` FROM 's3://bucket/path/*.csv' FORMAT 'csv' WITH FIELDS_TERMINATED_BY=',', FIELDS_ENCLOSED_BY='\"', FIELDS_ESCAPED_BY='\\\\', LINES_TERMINATED_BY='\n', FIELDS_DEFINED_NULL_BY='NULL'",
		},
		{
			name: "With Cloud Storage URI",
			options: &ImportOptions{
				Format:          "parquet",
				CloudStorageURI: "s3://bucket/storage",
			},
			expected: "IMPORT INTO `test_db`.`test_table` FROM 's3://bucket/path/*.csv' FORMAT 'parquet' WITH CLOUD_STORAGE_URI='s3://bucket/storage'",
		},
		{
			name: "Multiple Null Defined By",
			options: &ImportOptions{
				Format: "csv",
				CSVConfig: &config.CSVConfig{
					FieldNullDefinedBy: []string{"NULL", "\\N"},
				},
			},
			expectedError: "IMPORT INTO only supports one FIELDS_DEFINED_NULL_BY value",
		},
		{
			name: "Resource Parameters Append",
			tableMeta: &TableMeta{
				Database:     "test_db",
				Table:        "test_table",
				WildcardPath: "s3://bucket/path/*.csv?foo=bar",
			},
			options: &ImportOptions{
				Format:             "csv",
				ResourceParameters: "access-key=ak",
			},
			expected: "IMPORT INTO `test_db`.`test_table` FROM 's3://bucket/path/*.csv?foo=bar&access-key=ak' FORMAT 'csv'",
		},
		{
			name: "Escaping",
			options: &ImportOptions{
				Format:       "csv",
				GroupKey:     "group'1",
				CharacterSet: "utf8'mb4",
				CSVConfig: &config.CSVConfig{
					FieldsTerminatedBy: "'",
					FieldsEnclosedBy:   "\\",
				},
			},
			expected: "IMPORT INTO `test_db`.`test_table` FROM 's3://bucket/path/*.csv' FORMAT 'csv' WITH GROUP_KEY='group''1', CHARACTER_SET='utf8''mb4', FIELDS_TERMINATED_BY='''', FIELDS_ENCLOSED_BY='\\\\'",
		},
		{
			name: "Identifier Escaping",
			tableMeta: &TableMeta{
				Database:     "test`db",
				Table:        "test`table",
				WildcardPath: "s3://bucket/path/*.csv",
			},
			options: &ImportOptions{
				Format: "csv",
			},
			expected: "IMPORT INTO `test``db`.`test``table` FROM 's3://bucket/path/*.csv' FORMAT 'csv'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := tt.tableMeta
			if tm == nil {
				tm = defaultTableMeta
			}
			sql, err := gen.GenerateImportSQL(tm, tt.options)
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, sql)
			}
		})
	}
}
