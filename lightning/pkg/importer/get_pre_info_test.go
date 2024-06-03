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

package importer

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysql_sql_driver "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/lightning/pkg/importer/mock"
	ropts "github.com/pingcap/tidb/lightning/pkg/importer/opts"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	pqt_buf_src "github.com/xitongsys/parquet-go-source/buffer"
	pqtwriter "github.com/xitongsys/parquet-go/writer"
)

type colDef struct {
	ColName string
	Def     string
	TypeStr string
}

type tableDef []*colDef

func tableDefsToMockDataMap(dbTableDefs map[string]map[string]tableDef) map[string]*mock.DBSourceData {
	dbMockDataMap := make(map[string]*mock.DBSourceData)
	for dbName, tblDefMap := range dbTableDefs {
		tblMockDataMap := make(map[string]*mock.TableSourceData)
		for tblName, colDefs := range tblDefMap {
			colDefStrs := make([]string, len(colDefs))
			for i, colDef := range colDefs {
				colDefStrs[i] = fmt.Sprintf("%s %s", colDef.ColName, colDef.Def)
			}
			createSQL := fmt.Sprintf("CREATE TABLE %s.%s (%s);", dbName, tblName, strings.Join(colDefStrs, ", "))
			tblMockDataMap[tblName] = &mock.TableSourceData{
				DBName:    dbName,
				TableName: tblName,
				SchemaFile: &mock.SourceFile{
					FileName: fmt.Sprintf("/%s/%s/%s.schema.sql", dbName, tblName, tblName),
					Data:     []byte(createSQL),
				},
			}
		}
		dbMockDataMap[dbName] = &mock.DBSourceData{
			Name:   dbName,
			Tables: tblMockDataMap,
		}
	}
	return dbMockDataMap
}

func TestGetPreInfoGenerateTableInfo(t *testing.T) {
	schemaName := "db1"
	tblName := "tbl1"
	createTblSQL := fmt.Sprintf("create table `%s`.`%s` (a varchar(16) not null, b varchar(8) default 'DEFA')", schemaName, tblName)
	tblInfo, err := newTableInfo(createTblSQL, 1)
	require.Nil(t, err)
	require.Equal(t, model.NewCIStr(tblName), tblInfo.Name)
	require.Equal(t, len(tblInfo.Columns), 2)
	require.Equal(t, model.NewCIStr("a"), tblInfo.Columns[0].Name)
	require.Nil(t, tblInfo.Columns[0].DefaultValue)
	require.False(t, hasDefault(tblInfo.Columns[0]))
	require.Equal(t, model.NewCIStr("b"), tblInfo.Columns[1].Name)
	require.NotNil(t, tblInfo.Columns[1].DefaultValue)

	createTblSQL = fmt.Sprintf("create table `%s`.`%s` (a varchar(16), b varchar(8) default 'DEFAULT_BBBBB')", schemaName, tblName) // default value exceeds the length
	tblInfo, err = newTableInfo(createTblSQL, 2)
	require.NotNil(t, err)
}

func TestGetPreInfoHasDefault(t *testing.T) {
	subCases := []struct {
		ColDef           string
		ExpectHasDefault bool
	}{
		{
			ColDef:           "varchar(16)",
			ExpectHasDefault: true,
		},
		{
			ColDef:           "varchar(16) NOT NULL",
			ExpectHasDefault: false,
		},
		{
			ColDef:           "INTEGER PRIMARY KEY",
			ExpectHasDefault: false,
		},
		{
			ColDef:           "INTEGER AUTO_INCREMENT",
			ExpectHasDefault: true,
		},
		{
			ColDef:           "INTEGER PRIMARY KEY AUTO_INCREMENT",
			ExpectHasDefault: true,
		},
		{
			ColDef:           "BIGINT PRIMARY KEY AUTO_RANDOM",
			ExpectHasDefault: false,
		},
	}
	for _, subCase := range subCases {
		createTblSQL := fmt.Sprintf("create table `db1`.`tbl1` (a %s)", subCase.ColDef)
		tblInfo, err := newTableInfo(createTblSQL, 1)
		require.Nil(t, err)
		require.Equal(t, subCase.ExpectHasDefault, hasDefault(tblInfo.Columns[0]), subCase.ColDef)
	}
}

func TestGetPreInfoAutoRandomBits(t *testing.T) {
	subCases := []struct {
		ColDef                    string
		ExpectAutoRandomBits      uint64
		ExpectAutoRandomRangeBits uint64
	}{
		{
			ColDef:                    "varchar(16)",
			ExpectAutoRandomBits:      0,
			ExpectAutoRandomRangeBits: 0,
		},
		{
			ColDef:                    "BIGINT PRIMARY KEY AUTO_RANDOM",
			ExpectAutoRandomBits:      5,
			ExpectAutoRandomRangeBits: 64,
		},
		{
			ColDef:                    "BIGINT PRIMARY KEY AUTO_RANDOM(3)",
			ExpectAutoRandomBits:      3,
			ExpectAutoRandomRangeBits: 64,
		},
		{
			ColDef:                    "BIGINT PRIMARY KEY AUTO_RANDOM",
			ExpectAutoRandomBits:      5,
			ExpectAutoRandomRangeBits: 64,
		},
		{
			ColDef:                    "BIGINT PRIMARY KEY AUTO_RANDOM(5, 64)",
			ExpectAutoRandomBits:      5,
			ExpectAutoRandomRangeBits: 64,
		},
		{
			ColDef:                    "BIGINT PRIMARY KEY AUTO_RANDOM(2, 32)",
			ExpectAutoRandomBits:      2,
			ExpectAutoRandomRangeBits: 32,
		},
	}
	for _, subCase := range subCases {
		createTblSQL := fmt.Sprintf("create table `db1`.`tbl1` (a %s)", subCase.ColDef)
		tblInfo, err := newTableInfo(createTblSQL, 1)
		require.Nil(t, err)
		require.Equal(t, subCase.ExpectAutoRandomBits, tblInfo.AutoRandomBits, subCase.ColDef)
		require.Equal(t, subCase.ExpectAutoRandomRangeBits, tblInfo.AutoRandomRangeBits, subCase.ColDef)
	}
}

func TestGetPreInfoGetAllTableStructures(t *testing.T) {
	dbTableDefs := map[string]map[string]tableDef{
		"db01": {
			"tbl01": {
				&colDef{
					ColName: "id",
					Def:     "INTEGER PRIMARY KEY AUTO_INCREMENT",
					TypeStr: "int",
				},
				&colDef{
					ColName: "strval",
					Def:     "VARCHAR(64)",
					TypeStr: "varchar",
				},
			},
			"tbl02": {
				&colDef{
					ColName: "id",
					Def:     "INTEGER PRIMARY KEY AUTO_INCREMENT",
					TypeStr: "int",
				},
				&colDef{
					ColName: "val",
					Def:     "VARCHAR(64)",
					TypeStr: "varchar",
				},
			},
		},
		"db02": {
			"tbl01": {
				&colDef{
					ColName: "id",
					Def:     "INTEGER PRIMARY KEY AUTO_INCREMENT",
					TypeStr: "int",
				},
				&colDef{
					ColName: "strval",
					Def:     "VARCHAR(64)",
					TypeStr: "varchar",
				},
			},
		},
	}
	testMockDataMap := tableDefsToMockDataMap(dbTableDefs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockSrc, err := mock.NewImportSource(testMockDataMap)
	require.Nil(t, err)

	mockTarget := mock.NewTargetInfo()

	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	ig, err := NewPreImportInfoGetter(cfg, mockSrc.GetAllDBFileMetas(), mockSrc.GetStorage(), mockTarget, nil, nil, ropts.WithIgnoreDBNotExist(true))
	require.NoError(t, err)
	tblStructMap, err := ig.GetAllTableStructures(ctx)
	require.Nil(t, err)
	require.Equal(t, len(dbTableDefs), len(tblStructMap), "compare db count")
	for dbName, dbInfo := range tblStructMap {
		tblDefMap, ok := dbTableDefs[dbName]
		require.Truef(t, ok, "check db exists in db definitions: %s", dbName)
		require.Equalf(t, len(tblDefMap), len(dbInfo.Tables), "compare table count: %s", dbName)
		for tblName, tblStruct := range dbInfo.Tables {
			tblDef, ok := tblDefMap[tblName]
			require.Truef(t, ok, "check table exists in table definitions: %s.%s", dbName, tblName)
			require.Equalf(t, len(tblDef), len(tblStruct.Core.Columns), "compare columns count: %s.%s", dbName, tblName)
			for i, colDef := range tblStruct.Core.Columns {
				expectColDef := tblDef[i]
				require.Equalf(t, strings.ToLower(expectColDef.ColName), colDef.Name.L, "check column name: %s.%s", dbName, tblName)
				require.Truef(t, strings.Contains(colDef.FieldType.String(), strings.ToLower(expectColDef.TypeStr)), "check column type: %s.%s", dbName, tblName)
			}
		}
	}
}

func generateParquetData(t *testing.T) []byte {
	type parquetStruct struct {
		ID   int64  `parquet:"name=id, type=INT64"`
		Name string `parquet:"name=name, type=BYTE_ARRAY"`
	}
	pf, err := pqt_buf_src.NewBufferFile(make([]byte, 0))
	require.NoError(t, err)
	pw, err := pqtwriter.NewParquetWriter(pf, new(parquetStruct), 4)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, pw.Write(parquetStruct{
			ID:   int64(i + 1),
			Name: fmt.Sprintf("name_%d", i+1),
		}))
	}
	require.NoError(t, pw.WriteStop())
	require.NoError(t, pf.Close())
	bf, ok := pf.(pqt_buf_src.BufferFile)
	require.True(t, ok)
	return append([]byte(nil), bf.Bytes()...)
}

func TestGetPreInfoReadFirstRow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var testCSVData01 []byte = []byte(`ival,sval
111,"aaa"
222,"bbb"
`)
	pqtData := generateParquetData(t)
	const testSQLData01 string = `INSERT INTO db01.tbl01 (ival, sval) VALUES (333, 'ccc');
INSERT INTO db01.tbl01 (ival, sval) VALUES (444, 'ddd');`
	testDataInfos := []struct {
		FileName             string
		Data                 []byte
		FirstN               int
		CSVConfig            *config.CSVConfig
		ExpectFirstRowDatums [][]types.Datum
		ExpectColumns        []string
	}{
		{
			FileName: "/db01/tbl01/data.001.csv",
			Data:     testCSVData01,
			FirstN:   1,
			ExpectFirstRowDatums: [][]types.Datum{
				{
					types.NewStringDatum("111"),
					types.NewStringDatum("aaa"),
				},
			},
			ExpectColumns: []string{"ival", "sval"},
		},
		{
			FileName: "/db01/tbl01/data.002.csv",
			Data:     testCSVData01,
			FirstN:   2,
			ExpectFirstRowDatums: [][]types.Datum{
				{
					types.NewStringDatum("111"),
					types.NewStringDatum("aaa"),
				},
				{
					types.NewStringDatum("222"),
					types.NewStringDatum("bbb"),
				},
			},
			ExpectColumns: []string{"ival", "sval"},
		},
		{
			FileName: "/db01/tbl01/data.001.sql",
			Data:     []byte(testSQLData01),
			FirstN:   1,
			ExpectFirstRowDatums: [][]types.Datum{
				{
					types.NewUintDatum(333),
					types.NewStringDatum("ccc"),
				},
			},
			ExpectColumns: []string{"ival", "sval"},
		},
		{
			FileName:             "/db01/tbl01/data.003.csv",
			Data:                 []byte(""),
			FirstN:               1,
			ExpectFirstRowDatums: [][]types.Datum{},
			ExpectColumns:        nil,
		},
		{
			FileName:             "/db01/tbl01/data.004.csv",
			Data:                 []byte("ival,sval"),
			FirstN:               1,
			ExpectFirstRowDatums: [][]types.Datum{},
			ExpectColumns:        []string{"ival", "sval"},
		},
		{
			FileName: "/db01/tbl01/data.005.parquet",
			Data:     pqtData,
			FirstN:   3,
			ExpectFirstRowDatums: [][]types.Datum{
				{
					types.NewIntDatum(1),
					types.NewCollationStringDatum("name_1", "utf8mb4_bin"),
				},
				{
					types.NewIntDatum(2),
					types.NewCollationStringDatum("name_2", "utf8mb4_bin"),
				},
				{
					types.NewIntDatum(3),
					types.NewCollationStringDatum("name_3", "utf8mb4_bin"),
				},
			},
			ExpectColumns: []string{"id", "name"},
		},
	}
	tblMockSourceData := &mock.TableSourceData{
		DBName:    "db01",
		TableName: "tbl01",
		SchemaFile: &mock.SourceFile{
			FileName: "/db01/tbl01/tbl01.schema.sql",
			Data:     []byte("CREATE TABLE db01.tbl01(id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64));"),
		},
		DataFiles: []*mock.SourceFile{},
	}
	for _, testInfo := range testDataInfos {
		tblMockSourceData.DataFiles = append(tblMockSourceData.DataFiles, &mock.SourceFile{
			FileName: testInfo.FileName,
			Data:     testInfo.Data,
		})
	}
	mockDataMap := map[string]*mock.DBSourceData{
		"db01": {
			Name: "db01",
			Tables: map[string]*mock.TableSourceData{
				"tbl01": tblMockSourceData,
			},
		},
	}
	mockSrc, err := mock.NewImportSource(mockDataMap)
	require.Nil(t, err)
	mockTarget := mock.NewTargetInfo()
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	ig, err := NewPreImportInfoGetter(cfg, mockSrc.GetAllDBFileMetas(), mockSrc.GetStorage(), mockTarget, nil, nil)
	require.NoError(t, err)

	cfg.Mydumper.CSV.Header = true
	tblMeta := mockSrc.GetDBMetaMap()["db01"].Tables[0]
	for i, dataFile := range tblMeta.DataFiles {
		theDataInfo := testDataInfos[i]
		cols, rowDatums, err := ig.ReadFirstNRowsByFileMeta(ctx, dataFile.FileMeta, theDataInfo.FirstN)
		require.Nil(t, err)
		require.Equal(t, theDataInfo.ExpectColumns, cols)
		require.Equal(t, theDataInfo.ExpectFirstRowDatums, rowDatums)
	}

	theDataInfo := testDataInfos[0]
	cols, rowDatums, err := ig.ReadFirstNRowsByTableName(ctx, "db01", "tbl01", theDataInfo.FirstN)
	require.NoError(t, err)
	require.Equal(t, theDataInfo.ExpectColumns, cols)
	require.Equal(t, theDataInfo.ExpectFirstRowDatums, rowDatums)
}

func compressGz(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func TestGetPreInfoReadCompressedFirstRow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		testCSVData01 = []byte(`ival,sval
111,"aaa"
222,"bbb"
`)
		testSQLData01 = []byte(`INSERT INTO db01.tbl01 (ival, sval) VALUES (333, 'ccc');
INSERT INTO db01.tbl01 (ival, sval) VALUES (444, 'ddd');`)
	)

	test1CSVCompressed := compressGz(t, testCSVData01)
	test1SQLCompressed := compressGz(t, testSQLData01)

	testDataInfos := []struct {
		FileName             string
		Data                 []byte
		FirstN               int
		CSVConfig            *config.CSVConfig
		ExpectFirstRowDatums [][]types.Datum
		ExpectColumns        []string
	}{
		{
			FileName: "/db01/tbl01/data.001.csv.gz",
			Data:     test1CSVCompressed,
			FirstN:   1,
			ExpectFirstRowDatums: [][]types.Datum{
				{
					types.NewStringDatum("111"),
					types.NewStringDatum("aaa"),
				},
			},
			ExpectColumns: []string{"ival", "sval"},
		},
		{
			FileName: "/db01/tbl01/data.001.sql.gz",
			Data:     test1SQLCompressed,
			FirstN:   1,
			ExpectFirstRowDatums: [][]types.Datum{
				{
					types.NewUintDatum(333),
					types.NewStringDatum("ccc"),
				},
			},
			ExpectColumns: []string{"ival", "sval"},
		},
	}

	tbl01SchemaBytes := []byte("CREATE TABLE db01.tbl01(id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64));")
	tbl01SchemaBytesCompressed := compressGz(t, tbl01SchemaBytes)

	tblMockSourceData := &mock.TableSourceData{
		DBName:    "db01",
		TableName: "tbl01",
		SchemaFile: &mock.SourceFile{
			FileName: "/db01/tbl01/tbl01.schema.sql.gz",
			Data:     tbl01SchemaBytesCompressed,
		},
		DataFiles: []*mock.SourceFile{},
	}
	for _, testInfo := range testDataInfos {
		tblMockSourceData.DataFiles = append(tblMockSourceData.DataFiles, &mock.SourceFile{
			FileName: testInfo.FileName,
			Data:     testInfo.Data,
		})
	}
	mockDataMap := map[string]*mock.DBSourceData{
		"db01": {
			Name: "db01",
			Tables: map[string]*mock.TableSourceData{
				"tbl01": tblMockSourceData,
			},
		},
	}
	mockSrc, err := mock.NewImportSource(mockDataMap)
	require.Nil(t, err)
	mockTarget := mock.NewTargetInfo()
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	ig, err := NewPreImportInfoGetter(cfg, mockSrc.GetAllDBFileMetas(), mockSrc.GetStorage(), mockTarget, nil, nil)
	require.NoError(t, err)

	cfg.Mydumper.CSV.Header = true
	tblMeta := mockSrc.GetDBMetaMap()["db01"].Tables[0]
	for i, dataFile := range tblMeta.DataFiles {
		theDataInfo := testDataInfos[i]
		dataFile.FileMeta.Compression = mydump.CompressionGZ
		cols, rowDatums, err := ig.ReadFirstNRowsByFileMeta(ctx, dataFile.FileMeta, theDataInfo.FirstN)
		require.Nil(t, err)
		t.Logf("%v, %v", cols, rowDatums)
		require.Equal(t, theDataInfo.ExpectColumns, cols)
		require.Equal(t, theDataInfo.ExpectFirstRowDatums, rowDatums)
	}

	theDataInfo := testDataInfos[0]
	cols, rowDatums, err := ig.ReadFirstNRowsByTableName(ctx, "db01", "tbl01", theDataInfo.FirstN)
	require.NoError(t, err)
	require.Equal(t, theDataInfo.ExpectColumns, cols)
	require.Equal(t, theDataInfo.ExpectFirstRowDatums, rowDatums)
}

func TestGetPreInfoSampleSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dataFileName := "/db01/tbl01/tbl01.data.001.csv"
	mockDataMap := map[string]*mock.DBSourceData{
		"db01": {
			Name: "db01",
			Tables: map[string]*mock.TableSourceData{
				"tbl01": {
					DBName:    "db01",
					TableName: "tbl01",
					SchemaFile: &mock.SourceFile{
						FileName: "/db01/tbl01/tbl01.schema.sql",
						Data:     []byte("CREATE TABLE db01.tbl01 (id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64));"),
					},
					DataFiles: []*mock.SourceFile{
						{
							FileName: dataFileName,
							Data:     []byte(nil),
						},
					},
				},
			},
		},
	}
	mockSrc, err := mock.NewImportSource(mockDataMap)
	require.Nil(t, err)
	mockTarget := mock.NewTargetInfo()
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	ig, err := NewPreImportInfoGetter(cfg, mockSrc.GetAllDBFileMetas(), mockSrc.GetStorage(), mockTarget, nil, nil, ropts.WithIgnoreDBNotExist(true))
	require.NoError(t, err)

	mdDBMeta := mockSrc.GetAllDBFileMetas()[0]
	mdTblMeta := mdDBMeta.Tables[0]
	dbInfos, err := ig.GetAllTableStructures(ctx)
	require.NoError(t, err)

	subTests := []struct {
		Data            []byte
		ExpectIsOrdered bool
	}{
		{
			Data: []byte(`id,ival,sval
1,111,"aaa"
2,222,"bbb"
`,
			),
			ExpectIsOrdered: true,
		},
		{
			Data: []byte(`sval,ival,id
"aaa",111,1
"bbb",222,2
`,
			),
			ExpectIsOrdered: true,
		},
		{
			Data: []byte(`id,ival,sval
2,222,"bbb"
1,111,"aaa"
`,
			),
			ExpectIsOrdered: false,
		},
		{
			Data: []byte(`sval,ival,id
"aaa",111,2
"bbb",222,1
`,
			),
			ExpectIsOrdered: false,
		},
	}
	for _, subTest := range subTests {
		require.NoError(t, mockSrc.GetStorage().WriteFile(ctx, dataFileName, subTest.Data))
		sampledIndexRatio, isRowOrderedFromSample, err := ig.sampleDataFromTable(ctx, "db01", mdTblMeta, dbInfos["db01"].Tables["tbl01"].Core, nil, common.DefaultImportantVariables)
		require.NoError(t, err)
		t.Logf("%v, %v", sampledIndexRatio, isRowOrderedFromSample)
		require.Greater(t, sampledIndexRatio, 1.0)
		require.Equal(t, subTest.ExpectIsOrdered, isRowOrderedFromSample)
	}
}

func TestGetPreInfoSampleSourceCompressed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dataFileName := "/db01/tbl01/tbl01.data.001.csv.gz"
	schemaFileData := []byte("CREATE TABLE db01.tbl01 (id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64));")
	schemaFileDataCompressed := compressGz(t, schemaFileData)
	mockDataMap := map[string]*mock.DBSourceData{
		"db01": {
			Name: "db01",
			Tables: map[string]*mock.TableSourceData{
				"tbl01": {
					DBName:    "db01",
					TableName: "tbl01",
					SchemaFile: &mock.SourceFile{
						FileName: "/db01/tbl01/tbl01.schema.sql.gz",
						Data:     schemaFileDataCompressed,
					},
					DataFiles: []*mock.SourceFile{
						{
							FileName: dataFileName,
							Data:     []byte(nil),
						},
					},
				},
			},
		},
	}
	mockSrc, err := mock.NewImportSource(mockDataMap)
	require.Nil(t, err)
	mockTarget := mock.NewTargetInfo()
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	ig, err := NewPreImportInfoGetter(cfg, mockSrc.GetAllDBFileMetas(), mockSrc.GetStorage(), mockTarget, nil, nil, ropts.WithIgnoreDBNotExist(true))
	require.NoError(t, err)

	mdDBMeta := mockSrc.GetAllDBFileMetas()[0]
	mdTblMeta := mdDBMeta.Tables[0]
	dbInfos, err := ig.GetAllTableStructures(ctx)
	require.NoError(t, err)

	data := [][]byte{
		[]byte(`id,ival,sval
1,111,"aaa"
2,222,"bbb"
`),
		[]byte(`sval,ival,id
"aaa",111,1
"bbb",222,2
`),
		[]byte(`id,ival,sval
2,222,"bbb"
1,111,"aaa"
`),
		[]byte(`sval,ival,id
"aaa",111,2
"bbb",222,1
`),
	}
	compressedData := make([][]byte, 0, 4)
	for _, d := range data {
		compressedData = append(compressedData, compressGz(t, d))
	}

	subTests := []struct {
		Data            []byte
		ExpectIsOrdered bool
	}{
		{
			Data:            compressedData[0],
			ExpectIsOrdered: true,
		},
		{
			Data:            compressedData[1],
			ExpectIsOrdered: true,
		},
		{
			Data:            compressedData[2],
			ExpectIsOrdered: false,
		},
		{
			Data:            compressedData[3],
			ExpectIsOrdered: false,
		},
	}
	for _, subTest := range subTests {
		require.NoError(t, mockSrc.GetStorage().WriteFile(ctx, dataFileName, subTest.Data))
		sampledIndexRatio, isRowOrderedFromSample, err := ig.sampleDataFromTable(ctx, "db01", mdTblMeta, dbInfos["db01"].Tables["tbl01"].Core, nil, common.DefaultImportantVariables)
		require.NoError(t, err)
		t.Logf("%v, %v", sampledIndexRatio, isRowOrderedFromSample)
		require.Greater(t, sampledIndexRatio, 1.0)
		require.Equal(t, subTest.ExpectIsOrdered, isRowOrderedFromSample)
	}
}

func TestGetPreInfoEstimateSourceSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dataFileName := "/db01/tbl01/tbl01.data.001.csv"
	testData := []byte(`id,ival,sval
1,111,"aaa"
2,222,"bbb"
`,
	)
	mockDataMap := map[string]*mock.DBSourceData{
		"db01": {
			Name: "db01",
			Tables: map[string]*mock.TableSourceData{
				"tbl01": {
					DBName:    "db01",
					TableName: "tbl01",
					SchemaFile: &mock.SourceFile{
						FileName: "/db01/tbl01/tbl01.schema.sql",
						Data:     []byte("CREATE TABLE db01.tbl01 (id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64));"),
					},
					DataFiles: []*mock.SourceFile{
						{
							FileName: dataFileName,
							Data:     testData,
						},
					},
				},
			},
		},
	}
	mockSrc, err := mock.NewImportSource(mockDataMap)
	require.Nil(t, err)
	mockTarget := mock.NewTargetInfo()
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	ig, err := NewPreImportInfoGetter(cfg, mockSrc.GetAllDBFileMetas(), mockSrc.GetStorage(), mockTarget, nil, nil, ropts.WithIgnoreDBNotExist(true))
	require.NoError(t, err)

	sizeResult, err := ig.EstimateSourceDataSize(ctx)
	require.NoError(t, err)
	t.Logf("estimate size: %v, file size: %v, has unsorted table: %v\n", sizeResult.SizeWithIndex, sizeResult.SizeWithoutIndex, sizeResult.HasUnsortedBigTables)
	require.GreaterOrEqual(t, sizeResult.SizeWithIndex, int64(float64(sizeResult.SizeWithoutIndex)*compressionRatio))
	require.Equal(t, int64(len(testData)), sizeResult.SizeWithoutIndex)
	require.False(t, sizeResult.HasUnsortedBigTables)
}

func TestGetPreInfoIsTableEmpty(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	lnConfig := config.NewConfig()
	lnConfig.TikvImporter.Backend = config.BackendLocal
	targetGetter, err := NewTargetInfoGetterImpl(lnConfig, db, nil)
	require.NoError(t, err)
	mock.ExpectQuery("SELECT version()").
		WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow("8.0.11-TiDB-v8.2.0-alpha-256-qweqweqw"))
	err = targetGetter.CheckVersionRequirements(ctx)
	require.ErrorContains(t, err, "pd HTTP client is required for component version check in local backend")
	require.NoError(t, mock.ExpectationsWereMet())

	lnConfig.TikvImporter.Backend = config.BackendTiDB
	targetGetter, err = NewTargetInfoGetterImpl(lnConfig, db, nil)
	require.NoError(t, err)
	require.Equal(t, lnConfig, targetGetter.cfg)

	mock.ExpectQuery("SELECT 1 FROM `test_db`.`test_tbl` USE INDEX\\(\\) LIMIT 1").
		WillReturnError(&mysql_sql_driver.MySQLError{
			Number:  errno.ErrNoSuchTable,
			Message: "Table 'test_db.test_tbl' doesn't exist",
		})
	pIsEmpty, err := targetGetter.IsTableEmpty(ctx, "test_db", "test_tbl")
	require.NoError(t, err)
	require.NotNil(t, pIsEmpty)
	require.Equal(t, true, *pIsEmpty)

	mock.ExpectQuery("SELECT 1 FROM `test_db`.`test_tbl` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(
			sqlmock.NewRows([]string{"1"}).
				RowError(0, sql.ErrNoRows),
		)
	pIsEmpty, err = targetGetter.IsTableEmpty(ctx, "test_db", "test_tbl")
	require.NoError(t, err)
	require.NotNil(t, pIsEmpty)
	require.Equal(t, true, *pIsEmpty)

	mock.ExpectQuery("SELECT 1 FROM `test_db`.`test_tbl` USE INDEX\\(\\) LIMIT 1").
		WillReturnRows(
			sqlmock.NewRows([]string{"1"}).AddRow(1),
		)
	pIsEmpty, err = targetGetter.IsTableEmpty(ctx, "test_db", "test_tbl")
	require.NoError(t, err)
	require.NotNil(t, pIsEmpty)
	require.Equal(t, false, *pIsEmpty)

	mock.ExpectQuery("SELECT 1 FROM `test_db`.`test_tbl` USE INDEX\\(\\) LIMIT 1").
		WillReturnError(errors.New("some dummy error"))
	_, err = targetGetter.IsTableEmpty(ctx, "test_db", "test_tbl")
	require.Error(t, err)
}
