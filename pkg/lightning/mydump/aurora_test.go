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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAuroraSnapshotFilePath(t *testing.T) {
	testCases := []struct {
		name       string
		path       string
		matched    bool
		exportRoot string
		database   string
		schema     string
		table      string
		form       AuroraSnapshotPathForm
	}{
		{
			name:     "current layout at selected export root",
			path:     "rdststdb/rdststdb.users/1/part-00000-uuid-c000.gz.parquet",
			matched:  true,
			database: "rdststdb",
			schema:   "rdststdb",
			table:    "users",
			form:     AuroraSnapshotPathFormBatched,
		},
		{
			name:       "old layout below export identifier",
			path:       "export-123/rdststdb/rdststdb.users/part-00000-uuid.gz.parquet",
			matched:    true,
			exportRoot: "export-123",
			database:   "rdststdb",
			schema:     "rdststdb",
			table:      "users",
			form:       AuroraSnapshotPathFormDirect,
		},
		{
			name:     "escaped identifiers",
			path:     "my%20db/my%20db.order%2Eitems/1/part.parquet",
			matched:  true,
			database: "my db",
			schema:   "my db",
			table:    "order.items",
			form:     AuroraSnapshotPathFormBatched,
		},
		{
			name:    "generic dumpling parquet",
			path:    "dir/db.table.0001.gz.parquet",
			matched: false,
		},
		{
			name:    "non parquet",
			path:    "db/db.table/1/part.csv",
			matched: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			result, matched, err := ParseAuroraSnapshotFilePath(testCase.path)
			require.NoError(t, err)
			require.Equal(t, testCase.matched, matched)
			if !matched {
				require.Nil(t, result)
				return
			}
			require.Equal(t, testCase.exportRoot, result.ExportRoot)
			require.Equal(t, testCase.database, result.Database)
			require.Equal(t, testCase.schema, result.Schema)
			require.Equal(t, testCase.table, result.Table)
			require.Equal(t, testCase.form, result.Form)
		})
	}

	result, matched, err := ParseAuroraSnapshotFilePath(
		"parent/first.table/database/schema.table/part.parquet",
	)
	require.ErrorIs(t, err, ErrAmbiguousAuroraSnapshotPath)
	require.True(t, matched)
	require.Nil(t, result)
}
