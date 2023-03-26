// Copyright 2023 PingCAP, Inc.
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

package loaddata

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

func TestTransformSourceType(t *testing.T) {
	testCases := []struct {
		tp       string
		expected mydump.SourceType
	}{
		{
			tp:       importer.LoadDataFormatParquet,
			expected: mydump.SourceTypeParquet,
		},
		{
			tp:       importer.LoadDataFormatSQLDump,
			expected: mydump.SourceTypeSQL,
		},
		{
			tp:       importer.LoadDataFormatDelimitedData,
			expected: mydump.SourceTypeCSV,
		},
	}
	for _, tc := range testCases {
		expected, err := transformSourceType(tc.tp)
		require.NoError(t, err)
		require.Equal(t, tc.expected, expected)
	}
	expected, err := transformSourceType("unknown")
	require.EqualError(t, err, "unknown source type: unknown")
	require.Equal(t, mydump.SourceTypeIgnore, expected)
}

func TestMakeTableRegions(t *testing.T) {
	regions, err := makeTableRegions(context.Background(), &TaskMeta{}, 0)
	require.EqualError(t, err, "concurrency must be greater than 0, but got 0")
	require.Nil(t, regions)

	regions, err = makeTableRegions(context.Background(), &TaskMeta{}, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty store is not allowed")
	require.Nil(t, regions)

	task := &TaskMeta{
		Table: Table{
			Info: &model.TableInfo{
				Name: model.NewCIStr("test"),
			},
		},
		Dir: "dir",
	}
	regions, err = makeTableRegions(context.Background(), task, 1)
	require.EqualError(t, err, "unknown source type: ")
	require.Nil(t, regions)

	// parquet
	dir := "../../br/pkg/lightning/mydump/parquet"
	filename := "000000_0.parquet"
	task = &TaskMeta{
		Table: Table{
			Info: &model.TableInfo{
				Name: model.NewCIStr("test"),
			},
			TargetColumns: []string{"a", "b"},
		},
		Format: Format{
			Type: importer.LoadDataFormatParquet,
		},
		Dir: dir,
		FileInfos: []FileInfo{
			{
				Path: filename,
			},
		},
	}
	regions, err = makeTableRegions(context.Background(), task, 1)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	require.Equal(t, regions[0].EngineID, int32(0))
	require.Equal(t, regions[0].Chunk.Offset, int64(0))
	require.Equal(t, regions[0].Chunk.EndOffset, int64(5))
	require.Equal(t, regions[0].Chunk.PrevRowIDMax, int64(0))
	require.Equal(t, regions[0].Chunk.RowIDMax, int64(5))

	// large csv
	originRegionSize := config.MaxRegionSize
	config.MaxRegionSize = 5
	originBatchSize := config.DefaultBatchSize
	config.DefaultBatchSize = 12
	defer func() {
		config.MaxRegionSize = originRegionSize
		config.DefaultBatchSize = originBatchSize
	}()
	dir = "../../br/pkg/lightning/mydump/csv"
	filename = "split_large_file.csv"
	dataFileInfo, err := os.Stat(filepath.Join(dir, filename))
	require.NoError(t, err)
	task = &TaskMeta{
		Table: Table{
			Info: &model.TableInfo{
				Name: model.NewCIStr("test"),
			},
			TargetColumns: []string{"a", "b", "c"},
		},
		Format: Format{
			Type: importer.LoadDataFormatDelimitedData,
			CSV: CSV{
				Config: config.CSVConfig{
					Separator:         ",",
					Delimiter:         "",
					Header:            true,
					HeaderSchemaMatch: true,
					TrimLastSep:       false,
					NotNull:           false,
					Null:              []string{"NULL"},
					EscapedBy:         `\`,
				},
				Strict: true,
			},
		},
		Dir: dir,
		FileInfos: []FileInfo{
			{
				Path:     filename,
				Size:     dataFileInfo.Size(),
				RealSize: dataFileInfo.Size(),
			},
		},
	}
	regions, err = makeTableRegions(context.Background(), task, 1)
	require.NoError(t, err)
	require.Len(t, regions, 4)
	chunks := []Chunk{{Offset: 6, EndOffset: 12}, {Offset: 12, EndOffset: 18}, {Offset: 18, EndOffset: 24}, {Offset: 24, EndOffset: 30}}
	for i, region := range regions {
		require.Equal(t, region.EngineID, int32(i/2))
		require.Equal(t, region.Chunk.Offset, chunks[i].Offset)
		require.Equal(t, region.Chunk.EndOffset, chunks[i].EndOffset)
		require.Equal(t, region.Chunk.RealOffset, int64(0))
		require.Equal(t, region.Chunk.PrevRowIDMax, int64(i))
		require.Equal(t, region.Chunk.RowIDMax, int64(i+1))
	}

	// compression
	filename = "split_large_file.csv.zst"
	dataFileInfo, err = os.Stat(filepath.Join(dir, filename))
	require.NoError(t, err)
	task.FileInfos[0].Path = filename
	task.FileInfos[0].Size = dataFileInfo.Size()
	task.Format.Compression = mydump.CompressionZStd
	regions, err = makeTableRegions(context.Background(), task, 1)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	require.Equal(t, regions[0].EngineID, int32(0))
	require.Equal(t, regions[0].Chunk.Offset, int64(0))
	require.Equal(t, regions[0].Chunk.EndOffset, mydump.TableFileSizeINF)
	require.Equal(t, regions[0].Chunk.RealOffset, int64(0))
	require.Equal(t, regions[0].Chunk.PrevRowIDMax, int64(0))
	require.Equal(t, regions[0].Chunk.RowIDMax, int64(50))
}
