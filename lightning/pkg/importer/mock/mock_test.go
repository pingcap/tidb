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

package mock

import (
	"bytes"
	"context"
	"testing"

	"github.com/pingcap/tidb/lightning/pkg/importer"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestMockImportSourceBasic(t *testing.T) {
	mockDataMap := map[string]*DBSourceData{
		"db01": {
			Name: "db01",
			Tables: map[string]*TableSourceData{
				"tbl01": {
					DBName:    "db01",
					TableName: "tbl01",
					SchemaFile: &SourceFile{
						FileName: "/db01/tbl01/tbl01.schema.sql",
						Data:     []byte("CREATE TABLE db01.tbl01(id INTEGER PRIMARY KEY AUTO_INCREMENT, strval VARCHAR(64))"),
					},
				},
				"tbl02": {
					DBName:    "db01",
					TableName: "tbl02",
					SchemaFile: &SourceFile{
						FileName: "/db01/tbl02/tbl02.schema.sql",
						Data:     []byte("CREATE TABLE db01.tbl02(id INTEGER PRIMARY KEY AUTO_INCREMENT, val VARCHAR(64))"),
					},
					DataFiles: []*SourceFile{
						{
							FileName: "/db01/tbl02/tbl02.data.csv",
							Data:     []byte("val\naaa\nbbb"),
						},
						{
							FileName: "/db01/tbl02/tbl02.data.sql",
							Data:     []byte("INSERT INTO db01.tbl02 (val) VALUES ('ccc');"),
						},
					},
				},
			},
		},
		"db02": {
			Name: "db02",
			Tables: map[string]*TableSourceData{
				"tbl01": {
					DBName:    "db02",
					TableName: "tbl01",
					SchemaFile: &SourceFile{
						FileName: "/db02/tbl01/tbl01.schema.sql",
						Data:     []byte("CREATE TABLE db02.tbl01(id INTEGER PRIMARY KEY AUTO_INCREMENT, strval VARCHAR(64))"),
					},
				},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockEnv, err := NewImportSource(mockDataMap)
	require.Nil(t, err)
	dbFileMetas := mockEnv.GetAllDBFileMetas()
	require.Equal(t, len(mockDataMap), len(dbFileMetas), "compare db count")
	for _, dbFileMeta := range dbFileMetas {
		dbMockData, ok := mockDataMap[dbFileMeta.Name]
		require.Truef(t, ok, "get mock data by DB: %s", dbFileMeta.Name)
		require.Equalf(t, len(dbMockData.Tables), len(dbFileMeta.Tables), "compare table count: %s", dbFileMeta.Name)
		for _, tblFileMeta := range dbFileMeta.Tables {
			tblMockData, ok := dbMockData.Tables[tblFileMeta.Name]
			require.Truef(t, ok, "get mock data by Table: %s.%s", dbFileMeta.Name, tblFileMeta.Name)
			schemaFileMeta := tblFileMeta.SchemaFile
			mockSchemaFile := tblMockData.SchemaFile
			fileData, err := mockEnv.srcStorage.ReadFile(ctx, schemaFileMeta.FileMeta.Path)
			require.Nilf(t, err, "read schema file: %s.%s", dbFileMeta.Name, tblFileMeta.Name)
			require.Truef(t, bytes.Equal(mockSchemaFile.Data, fileData), "compare schema file: %s.%s", dbFileMeta.Name, tblFileMeta.Name)
			require.Equalf(t, len(tblMockData.DataFiles), len(tblFileMeta.DataFiles), "compare data file count: %s.%s", dbFileMeta.Name, tblFileMeta.Name)
			for i, dataFileMeta := range tblFileMeta.DataFiles {
				mockDataFile := tblMockData.DataFiles[i]
				fileData, err := mockEnv.srcStorage.ReadFile(ctx, dataFileMeta.FileMeta.Path)
				require.Nilf(t, err, "read data file: %s.%s: %s", dbFileMeta.Name, tblFileMeta.Name, dataFileMeta.FileMeta.Path)
				require.Truef(t, bytes.Equal(mockDataFile.Data, fileData), "compare data file: %s.%s: %s", dbFileMeta.Name, tblFileMeta.Name, dataFileMeta.FileMeta.Path)
			}
		}
	}
}

func TestMockTargetInfoBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ti := NewTargetInfo()
	var _ importer.TargetInfoGetter = ti
	const replicaCount = 3
	const emptyRegionCount = 5
	const s01TotalSize uint64 = 10 << 30
	const s01TotalSizeStr = "10GiB"
	const s01UsedSize uint64 = 7<<30 + 500<<20
	const s02TotalSize uint64 = 50 << 30
	const s02TotalSizeStr = "50GiB"
	const s02UsedSize uint64 = 35<<30 + 700<<20

	ti.SetSysVar("aaa", "111")
	ti.SetSysVar("bbb", "222")
	sysVars := ti.GetTargetSysVariablesForImport(ctx)
	v, ok := sysVars["aaa"]
	require.True(t, ok)
	require.Equal(t, "111", v)
	v, ok = sysVars["bbb"]
	require.True(t, ok)
	require.Equal(t, "222", v)

	ti.MaxReplicasPerRegion = replicaCount
	cnt, err := ti.GetMaxReplica(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(replicaCount), cnt)

	ti.StorageInfos = append(ti.StorageInfos,
		StorageInfo{
			TotalSize:     s01TotalSize,
			UsedSize:      s01UsedSize,
			AvailableSize: s01TotalSize - s01UsedSize,
		},
		StorageInfo{
			TotalSize:     s02TotalSize,
			UsedSize:      s02UsedSize,
			AvailableSize: s02TotalSize - s02UsedSize,
		},
	)
	si, err := ti.GetStorageInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, si.Count)
	store := si.Stores[0]
	require.Equal(t, s01TotalSizeStr, store.Status.Capacity)
	require.Equal(t, s01UsedSize, uint64(store.Status.RegionSize))
	store = si.Stores[1]
	require.Equal(t, s02TotalSizeStr, store.Status.Capacity)
	require.Equal(t, s02UsedSize, uint64(store.Status.RegionSize))

	ti.EmptyRegionCountMap = map[uint64]int{
		1: emptyRegionCount,
	}
	ri, err := ti.GetEmptyRegionsInfo(ctx)
	require.NoError(t, err)
	require.EqualValues(t, emptyRegionCount, ri.Count)
	require.Equal(t, emptyRegionCount, len(ri.Regions))

	ti.SetTableInfo("testdb", "testtbl1",
		&TableInfo{
			TableModel: &model.TableInfo{
				ID:   1,
				Name: model.NewCIStr("testtbl1"),
				Columns: []*model.ColumnInfo{
					{
						ID:     1,
						Name:   model.NewCIStr("c_1"),
						Offset: 0,
					},
					{
						ID:     2,
						Name:   model.NewCIStr("c_2"),
						Offset: 1,
					},
				},
			},
		},
	)
	ti.SetTableInfo("testdb", "testtbl2",
		&TableInfo{
			RowCount: 100,
		},
	)
	tblInfos, err := ti.FetchRemoteTableModels(ctx, "testdb")
	require.NoError(t, err)
	require.Equal(t, 2, len(tblInfos))
	for _, tblInfo := range tblInfos {
		if tblInfo == nil {
			continue
		}
		require.Equal(t, 2, len(tblInfo.Columns))
	}

	isEmptyPtr, err := ti.IsTableEmpty(ctx, "testdb", "testtbl1")
	require.NoError(t, err)
	require.NotNil(t, isEmptyPtr)
	require.True(t, *isEmptyPtr)
	isEmptyPtr, err = ti.IsTableEmpty(ctx, "testdb", "testtbl2")
	require.NoError(t, err)
	require.NotNil(t, isEmptyPtr)
	require.False(t, *isEmptyPtr)
}
