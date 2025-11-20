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

	"github.com/pingcap/tidb/pkg/lightning/mydump"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestCreateDataFileMeta(t *testing.T) {
	fi := mydump.FileInfo{
		TableName: filter.Table{Schema: "db", Name: "table"},
		FileMeta: mydump.SourceFileMeta{
			Path:        "s3://bucket/path/to/f",
			FileSize:    123,
			Type:        mydump.SourceTypeCSV,
			Compression: mydump.CompressionGZ,
			RealSize:    456,
		},
	}
	df := createDataFileMeta(fi)
	require.Equal(t, "s3://bucket/path/to/f", df.Path)
	require.Equal(t, int64(456), df.Size)
	require.Equal(t, mydump.SourceTypeCSV, df.Format)
	require.Equal(t, mydump.CompressionGZ, df.Compression)
}

func TestProcessDataFiles(t *testing.T) {
	files := []mydump.FileInfo{
		{FileMeta: mydump.SourceFileMeta{Path: "s3://bucket/a", RealSize: 10}},
		{FileMeta: mydump.SourceFileMeta{Path: "s3://bucket/b", RealSize: 20}},
	}
	dfm, total := processDataFiles(files)
	require.Len(t, dfm, 2)
	require.Equal(t, int64(30), total)
	require.Equal(t, "s3://bucket/a", dfm[0].Path)
	require.Equal(t, "s3://bucket/b", dfm[1].Path)
}
