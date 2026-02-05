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
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestCreateDataFileMeta(t *testing.T) {
	fi := mydump.FileInfo{
		TableName: filter.Table{
			Schema: "db",
			Name:   "table",
		},
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

func TestFileScanner(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "db1-schema-create.sql"), []byte("CREATE DATABASE db1;"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "db1.t1-schema.sql"), []byte("CREATE TABLE t1 (id INT);"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "db1.t1.001.csv"), []byte("1\n2"), 0644))

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	cfg := defaultSDKConfig()
	scanner, err := NewFileScanner(ctx, "file://"+tmpDir, db, cfg)
	require.NoError(t, err)
	defer scanner.Close()

	t.Run("GetTotalSize", func(t *testing.T) {
		size := scanner.GetTotalSize(ctx)
		require.Equal(t, int64(3), size)
	})

	t.Run("GetTableMetas", func(t *testing.T) {
		metas, err := scanner.GetTableMetas(ctx)
		require.NoError(t, err)
		require.Len(t, metas, 1)
		require.Equal(t, "db1", metas[0].Database)
		require.Equal(t, "t1", metas[0].Table)
		require.Equal(t, int64(3), metas[0].TotalSize)
		require.Len(t, metas[0].DataFiles, 1)
	})

	t.Run("GetTableMetaByName", func(t *testing.T) {
		meta, err := scanner.GetTableMetaByName(ctx, "db1", "t1")
		require.NoError(t, err)
		require.Equal(t, "db1", meta.Database)
		require.Equal(t, "t1", meta.Table)

		_, err = scanner.GetTableMetaByName(ctx, "db1", "nonexistent")
		require.Error(t, err)
	})

	t.Run("CreateSchemasAndTables", func(t *testing.T) {
		mock.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA.*").WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
		mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `db1`")).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db1`.`t1`")).WillReturnResult(sqlmock.NewResult(0, 0))

		err := scanner.CreateSchemasAndTables(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CreateSchemaAndTableByName", func(t *testing.T) {
		mock.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA.*").WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
		mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `db1`")).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db1`.`t1`")).WillReturnResult(sqlmock.NewResult(0, 0))

		err := scanner.CreateSchemaAndTableByName(ctx, "db1", "t1")
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())

		err = scanner.CreateSchemaAndTableByName(ctx, "db1", "nonexistent")
		require.Error(t, err)
	})
}

func TestFileScannerWithEstimateFileSize(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "db1-schema-create.sql"), []byte("CREATE DATABASE db1;"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "db1.t1-schema.sql"), []byte("CREATE TABLE t1 (id INT);"), 0644))

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	for range 1000 {
		_, err := gz.Write([]byte("aaaa\n"))
		require.NoError(t, err)
	}
	require.NoError(t, gz.Close())
	compressedData := buf.Bytes()
	compressedSize := int64(len(compressedData))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "db1.t1.001.csv.gz"), compressedData, 0644))

	db1, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db1.Close()

	cfg1 := defaultSDKConfig()
	scanner1, err := NewFileScanner(ctx, "file://"+tmpDir, db1, cfg1)
	require.NoError(t, err)
	defer scanner1.Close()

	metas1, err := scanner1.GetTableMetas(ctx)
	require.NoError(t, err)
	require.Len(t, metas1, 1)
	require.Greater(t, metas1[0].TotalSize, compressedSize)

	db2, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db2.Close()

	cfg2 := defaultSDKConfig()
	WithEstimateFileSize(false)(cfg2)
	scanner2, err := NewFileScanner(ctx, "file://"+tmpDir, db2, cfg2)
	require.NoError(t, err)
	defer scanner2.Close()

	metas2, err := scanner2.GetTableMetas(ctx)
	require.NoError(t, err)
	require.Len(t, metas2, 1)
	require.Equal(t, compressedSize, metas2[0].TotalSize)
	require.Len(t, metas2[0].DataFiles, 1)
	require.Equal(t, compressedSize, metas2[0].DataFiles[0].Size)
}

func TestFileScannerWithSkipInvalidFiles(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "data1.csv"), []byte("1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "data2.csv"), []byte("1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "data3.csv"), []byte("1"), 0644))

	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rules := []*config.FileRouteRule{
		{
			Pattern: "data[1-2].csv",
			Schema:  "db1",
			Table:   "t1",
			Type:    "csv",
		},
		{
			Pattern: "data3.csv",
			Schema:  "db1",
			Table:   "t2",
			Type:    "csv",
		},
	}

	cfg := defaultSDKConfig()
	WithFileRouters(rules)(cfg)

	scanner, err := NewFileScanner(ctx, "file://"+tmpDir, db, cfg)
	require.NoError(t, err)
	defer scanner.Close()

	metas, err := scanner.GetTableMetas(ctx)
	require.Error(t, err)
	require.Nil(t, metas)

	cfg.skipInvalidFiles = true
	scanner2, err := NewFileScanner(ctx, "file://"+tmpDir, db, cfg)
	require.NoError(t, err)
	defer scanner2.Close()

	metas2, err := scanner2.GetTableMetas(ctx)
	require.NoError(t, err)
	require.Len(t, metas2, 1)
	require.Equal(t, "t2", metas2[0].Table)
}
