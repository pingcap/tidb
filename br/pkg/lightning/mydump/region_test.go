// Copyright 2019 PingCAP, Inc.
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

package mydump_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	. "github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// var expectedTuplesCount = map[string]int64{
// 	"i":                     1,
// 	"report_case_high_risk": 1,
// 	"tbl_autoid":            10000,
// 	"tbl_multi_index":       10000,
// }

/*
	TODO : test with specified 'regionBlockSize' ...
*/
func TestTableRegion(t *testing.T) {
	cfg := newConfigWithSourceDir("./examples")
	// specify ReadBlockSize because we need to sample files
	cfg.Mydumper.ReadBlockSize = config.ReadBlockSize
	loader, _ := NewMyDumpLoader(context.Background(), cfg)
	dbMeta := loader.GetDatabases()[0]

	ioWorkers := worker.NewPool(context.Background(), 1, "io")
	for _, meta := range dbMeta.Tables {
		regions, err := MakeTableRegions(context.Background(), meta, 1, cfg, ioWorkers, loader.GetStore())
		require.NoError(t, err)

		// check - region-size vs file-size
		var tolFileSize int64 = 0
		for _, file := range meta.DataFiles {
			tolFileSize += file.FileMeta.FileSize
		}
		var tolRegionSize int64 = 0
		for _, region := range regions {
			tolRegionSize += region.Size()
		}
		require.Equal(t, tolFileSize, tolRegionSize)

		// // check - rows num
		// var tolRows int64 = 0
		// for _, region := range regions {
		// 	tolRows += region.Rows()
		// }
		// c.Assert(tolRows, Equals, expectedTuplesCount[table])

		// check - range
		regionNum := len(regions)
		preReg := regions[0]
		for i := 1; i < regionNum; i++ {
			reg := regions[i]
			if preReg.FileMeta.Path == reg.FileMeta.Path {
				require.Equal(t, preReg.Offset()+preReg.Size(), reg.Offset())
				require.Equal(t, preReg.RowIDMin()+preReg.Rows(), reg.RowIDMin())
			} else {
				require.Equal(t, 0, reg.Offset())
				require.Equal(t, 1, reg.RowIDMin())
			}
			preReg = reg
		}
	}
}

func TestAllocateEngineIDs(t *testing.T) {
	dataFileSizes := make([]float64, 700)
	for i := range dataFileSizes {
		dataFileSizes[i] = 1.0
	}
	filesRegions := make([]*TableRegion, 0, len(dataFileSizes))
	for range dataFileSizes {
		filesRegions = append(filesRegions, new(TableRegion))
	}

	checkEngineSizes := func(what string, expected map[int32]int) {
		actual := make(map[int32]int)
		for _, region := range filesRegions {
			actual[region.EngineID]++
		}
		require.Equal(t, expected, actual, what)
	}

	// Batch size > Total size => Everything in the zero batch.
	AllocateEngineIDs(filesRegions, dataFileSizes, 1000, 0.5, 1000)
	checkEngineSizes("no batching", map[int32]int{
		0: 700,
	})

	// Allocate 3 engines.
	AllocateEngineIDs(filesRegions, dataFileSizes, 200, 0.5, 1000)
	checkEngineSizes("batch size = 200", map[int32]int{
		0: 170,
		1: 213,
		2: 317,
	})

	// Allocate 3 engines with an alternative ratio
	AllocateEngineIDs(filesRegions, dataFileSizes, 200, 0.6, 1000)
	checkEngineSizes("batch size = 200, ratio = 0.6", map[int32]int{
		0: 160,
		1: 208,
		2: 332,
	})

	// Allocate 5 engines.
	AllocateEngineIDs(filesRegions, dataFileSizes, 100, 0.5, 1000)
	checkEngineSizes("batch size = 100", map[int32]int{
		0: 93,
		1: 105,
		2: 122,
		3: 153,
		4: 227,
	})

	// Number of engines > table concurrency
	AllocateEngineIDs(filesRegions, dataFileSizes, 50, 0.5, 4)
	checkEngineSizes("batch size = 50, limit table conc = 4", map[int32]int{
		0:  50,
		1:  59,
		2:  73,
		3:  110,
		4:  50,
		5:  50,
		6:  50,
		7:  50,
		8:  50,
		9:  50,
		10: 50,
		11: 50,
		12: 8,
	})

	// Zero ratio = Uniform
	AllocateEngineIDs(filesRegions, dataFileSizes, 100, 0.0, 1000)
	checkEngineSizes("batch size = 100, ratio = 0", map[int32]int{
		0: 100,
		1: 100,
		2: 100,
		3: 100,
		4: 100,
		5: 100,
		6: 100,
	})
}

func TestSplitLargeFile(t *testing.T) {
	meta := &MDTableMeta{
		DB:   "csv",
		Name: "large_csv_file",
	}
	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize: config.ReadBlockSize,
			CSV: config.CSVConfig{
				Separator:       ",",
				Delimiter:       "",
				Header:          true,
				TrimLastSep:     false,
				NotNull:         false,
				Null:            "NULL",
				BackslashEscape: true,
			},
			StrictFormat: true,
			Filter:       []string{"*.*"},
		},
	}
	filePath := "./csv/split_large_file.csv"
	dataFileInfo, err := os.Stat(filePath)
	require.NoError(t, err)
	fileSize := dataFileInfo.Size()
	fileInfo := FileInfo{FileMeta: SourceFileMeta{Path: filePath, Type: SourceTypeCSV, FileSize: fileSize}}
	colCnt := int64(3)
	columns := []string{"a", "b", "c"}
	for _, tc := range []struct {
		maxRegionSize config.ByteSize
		offsets       [][]int64
	}{
		{1, [][]int64{{6, 12}, {12, 18}, {18, 24}, {24, 30}}},
		{6, [][]int64{{6, 18}, {18, 30}}},
		{8, [][]int64{{6, 18}, {18, 30}}},
		{12, [][]int64{{6, 24}, {24, 30}}},
		{13, [][]int64{{6, 24}, {24, 30}}},
		{18, [][]int64{{6, 30}}},
		{19, [][]int64{{6, 30}}},
	} {
		cfg.Mydumper.MaxRegionSize = tc.maxRegionSize
		prevRowIdxMax := int64(0)
		ioWorker := worker.NewPool(context.Background(), 4, "io")

		store, err := storage.NewLocalStorage(".")
		assert.NoError(t, err)

		_, regions, _, err := SplitLargeFile(context.Background(), meta, cfg, fileInfo, colCnt, prevRowIdxMax, ioWorker, store)
		assert.NoError(t, err)
		assert.Len(t, regions, len(tc.offsets))
		for i := range tc.offsets {
			assert.Equal(t, tc.offsets[i][0], regions[i].Chunk.Offset)
			assert.Equal(t, tc.offsets[i][1], regions[i].Chunk.EndOffset)
			assert.Equal(t, columns, regions[i].Chunk.Columns)
		}
	}
}

func TestSplitLargeFileNoNewLineAtEOF(t *testing.T) {
	meta := &MDTableMeta{
		DB:   "csv",
		Name: "large_csv_file",
	}
	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize: config.ReadBlockSize,
			CSV: config.CSVConfig{
				Separator:       ",",
				Delimiter:       "",
				Header:          true,
				TrimLastSep:     false,
				NotNull:         false,
				Null:            "NULL",
				BackslashEscape: true,
			},
			StrictFormat:  true,
			Filter:        []string{"*.*"},
			MaxRegionSize: 1,
		},
	}

	dir := t.TempDir()

	fileName := "test.csv"
	filePath := filepath.Join(dir, fileName)

	content := []byte("a,b\r\n123,456\r\n789,101")
	err := os.WriteFile(filePath, content, 0o644)
	require.NoError(t, err)

	dataFileInfo, err := os.Stat(filePath)
	require.NoError(t, err)
	fileSize := dataFileInfo.Size()
	fileInfo := FileInfo{FileMeta: SourceFileMeta{Path: fileName, Type: SourceTypeCSV, FileSize: fileSize}}
	colCnt := int64(2)
	columns := []string{"a", "b"}
	prevRowIdxMax := int64(0)
	ioWorker := worker.NewPool(context.Background(), 4, "io")

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	offsets := [][]int64{{4, 13}, {13, 21}}

	_, regions, _, err := SplitLargeFile(context.Background(), meta, cfg, fileInfo, colCnt, prevRowIdxMax, ioWorker, store)
	require.NoError(t, err)
	require.Len(t, regions, len(offsets))
	for i := range offsets {
		require.Equal(t, offsets[i][0], regions[i].Chunk.Offset)
		require.Equal(t, offsets[i][1], regions[i].Chunk.EndOffset)
		require.Equal(t, columns, regions[i].Chunk.Columns)
	}
}

func TestSplitLargeFileWithCustomTerminator(t *testing.T) {
	meta := &MDTableMeta{
		DB:   "csv",
		Name: "large_csv_with_custom_terminator",
	}
	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize: config.ReadBlockSize,
			CSV: config.CSVConfig{
				Separator:  "|+|",
				Terminator: "|+|\n",
			},
			StrictFormat:  true,
			Filter:        []string{"*.*"},
			MaxRegionSize: 1,
		},
	}

	dir := t.TempDir()

	fileName := "test2.csv"
	filePath := filepath.Join(dir, fileName)

	content := []byte("5|+|abc\ndef\nghi|+|6|+|\n7|+|xyz|+|8|+|\n9|+||+|10")
	err := os.WriteFile(filePath, content, 0o644)
	require.NoError(t, err)

	dataFileInfo, err := os.Stat(filePath)
	require.NoError(t, err)
	fileSize := dataFileInfo.Size()
	fileInfo := FileInfo{FileMeta: SourceFileMeta{Path: fileName, Type: SourceTypeCSV, FileSize: fileSize}}
	colCnt := int64(3)
	prevRowIdxMax := int64(0)
	ioWorker := worker.NewPool(context.Background(), 4, "io")

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	offsets := [][]int64{{0, 23}, {23, 38}, {38, 47}}

	_, regions, _, err := SplitLargeFile(context.Background(), meta, cfg, fileInfo, colCnt, prevRowIdxMax, ioWorker, store)
	require.NoError(t, err)
	require.Len(t, regions, len(offsets))
	for i := range offsets {
		require.Equal(t, offsets[i][0], regions[i].Chunk.Offset)
		require.Equal(t, offsets[i][1], regions[i].Chunk.EndOffset)
	}
}

func TestSplitLargeFileOnlyOneChunk(t *testing.T) {
	meta := &MDTableMeta{
		DB:   "csv",
		Name: "large_csv_file",
	}
	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize: config.ReadBlockSize,
			CSV: config.CSVConfig{
				Separator:       ",",
				Delimiter:       "",
				Header:          true,
				TrimLastSep:     false,
				NotNull:         false,
				Null:            "NULL",
				BackslashEscape: true,
			},
			StrictFormat:  true,
			Filter:        []string{"*.*"},
			MaxRegionSize: 15,
		},
	}

	dir := t.TempDir()

	fileName := "test.csv"
	filePath := filepath.Join(dir, fileName)

	content := []byte("field1,field2\r\n123,456\r\n")
	err := os.WriteFile(filePath, content, 0o644)
	require.NoError(t, err)

	dataFileInfo, err := os.Stat(filePath)
	require.NoError(t, err)
	fileSize := dataFileInfo.Size()
	fileInfo := FileInfo{FileMeta: SourceFileMeta{Path: fileName, Type: SourceTypeCSV, FileSize: fileSize}}
	colCnt := int64(2)
	columns := []string{"field1", "field2"}
	prevRowIdxMax := int64(0)
	ioWorker := worker.NewPool(context.Background(), 4, "io")

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)

	offsets := [][]int64{{14, 24}}

	_, regions, _, err := SplitLargeFile(context.Background(), meta, cfg, fileInfo, colCnt, prevRowIdxMax, ioWorker, store)
	require.NoError(t, err)
	require.Len(t, regions, len(offsets))
	for i := range offsets {
		require.Equal(t, offsets[i][0], regions[i].Chunk.Offset)
		require.Equal(t, offsets[i][1], regions[i].Chunk.EndOffset)
		require.Equal(t, columns, regions[i].Chunk.Columns)
	}
}

func TestSampleAndGetAvgRowSize(t *testing.T) {
	// It's more difficult to estimate sizes of SQL files than csv files,
	// because when reading the first row of them, parser may read other info (e.g. table name)
	// so that make it hard to get good estimate, especially when files have few rows.
	sqlFiles := []string{
		// 1. long table name, values:
		// 1.1 short and even len
		"INSERT INTO `test_db_mock_long.test_table_very_long_name` VALUES (1),(2);",
		// 1.2 short and not even
		"INSERT INTO `test_db_mock_long.test_table_very_long_name` VALUES (123452123,1234123125),(2,1);",
		"INSERT INTO `test_db_mock_long.test_table_very_long_name` VALUES (2,1),(123452123,1234123125);",
		// 1.3 long and even
		"INSERT INTO `test_db_mock_long.test_table_very_long_name` VALUES (123452123,1234123125),(1234123125,12341231251);",
		// 1.4 long but not even
		"INSERT INTO `test_db_mock_long.test_table_very_long_name` VALUES ('abcdefghidgjla','lkjadsfasfdkjl'),('1111111','1');",
		// 2. short table name, values:
		// 2.1 short and even len
		"INSERT INTO `a` VALUES (1),(2);",
		// 2.2 short and not even
		"INSERT INTO `a` VALUES (123452123,1234123125),(2,1);",
		"INSERT INTO `a` VALUES (2,1),(123452123,1234123125);",
		// 2.3 long and even
		"INSERT INTO `a` VALUES (123452123,1234123125),(1234123125,12341231251);",
		// 2.4 long but not even
		"INSERT INTO `a` VALUES ('abcdefghidgjla','lkjadsfasfdkjl'),('1111111','1');",
	}

	csvFiles := []string{
		// even and short
		"a,b,c\r\n1,2,3\r\n4,5,6\r\n",
		// not even but short
		"a,b,c\r\n1112,1234,1923\r\n1,2,3",
		// even and long
		"a,b,c\r\n14712312,123122,1231233\r\n4456364,34525,423426\r\n",
		// not even but long
		"a,b,c\r\nsadlk;fja;lskdfj;alksdfj,sdlk;fjaksld;fja;l,qpoiwuepqou\r\n0,0,0\r\n",
	}
	testFunc := func(files []string, fileType SourceType) {
		for _, file := range files {
			dir := t.TempDir()

			var fileName string
			if fileType == SourceTypeCSV {
				fileName = "test.csv"
			} else {
				fileName = "test.sql"
			}
			filePath := filepath.Join(dir, fileName)

			content := []byte(file)
			err := os.WriteFile(filePath, content, 0o644)
			require.Nil(t, err)
			dataFileInfo, err := os.Stat(filePath)
			require.Nil(t, err)
			fileSize := dataFileInfo.Size()

			cfg := newConfigWithSourceDir(dir)
			loader, _ := NewMyDumpLoader(context.Background(), cfg)
			ioWorkers := worker.NewPool(context.Background(), 1, "io")

			// specify ReadBlockSize because we need to sample files
			cfg.Mydumper.ReadBlockSize = config.ReadBlockSize
			fileInfo := FileInfo{
				FileMeta: SourceFileMeta{
					Path:     fileName,
					Type:     fileType,
					FileSize: fileSize,
				},
			}
			cfg.Mydumper.CSV = config.CSVConfig{
				Separator:       ",",
				Delimiter:       `"`,
				Header:          true,
				NotNull:         false,
				Null:            `\N`,
				BackslashEscape: true,
				TrimLastSep:     false,
			}
			size, err := GetSampledAvgRowSize(&fileInfo, cfg, ioWorkers, loader.GetStore())
			require.Nil(t, err)
			require.GreaterOrEqual(t, fileSize/size, int64(2))
		}
	}
	testFunc(sqlFiles, SourceTypeSQL)
	testFunc(csvFiles, SourceTypeCSV)
}
