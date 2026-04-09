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
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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

	t.Run("CreateSchemasAndTablesIgnoresDropTableInSchemaFile", func(t *testing.T) {
		dropDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dropDir, "db1-schema-create.sql"), []byte("CREATE DATABASE db1;"), 0o644))
		require.NoError(t, os.WriteFile(
			filepath.Join(dropDir, "db1.t_drop-schema.sql"),
			[]byte("DROP TABLE t_drop; CREATE TABLE t_drop (id INT);"),
			0o644,
		))

		dropDB, dropMock, err := sqlmock.New()
		require.NoError(t, err)
		defer dropDB.Close()

		dropScanner, err := NewFileScanner(ctx, "file://"+dropDir, dropDB, defaultSDKConfig())
		require.NoError(t, err)
		defer dropScanner.Close()

		dropMock.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA.*").WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
		dropMock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `db1`")).WillReturnResult(sqlmock.NewResult(0, 0))
		dropMock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db1`.`t_drop`")).WillReturnResult(sqlmock.NewResult(0, 0))

		err = dropScanner.CreateSchemasAndTables(ctx)
		require.NoError(t, err)
		require.NoError(t, dropMock.ExpectationsWereMet())
	})

	t.Run("EstimateImportDataSize", func(t *testing.T) {
		estimateDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(estimateDir, "db1-schema-create.sql"), []byte("CREATE DATABASE db1;"), 0o644))
		buildInsertSQL := func(table string) string {
			var sb strings.Builder
			for i := 1; i <= 200; i++ {
				payload := strings.Repeat(string(rune('a'+(i%26))), 128)
				_, err := fmt.Fprintf(&sb, "INSERT INTO db1.%s VALUES (%d, %d, '%s');\n", table, i, i, payload)
				require.NoError(t, err)
			}
			return sb.String()
		}
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.no_idx-schema.sql"),
			[]byte("CREATE TABLE db1.no_idx (id INT PRIMARY KEY, k INT, v VARCHAR(255));"),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.no_idx.001.sql"),
			[]byte(buildInsertSQL("no_idx")),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.with_idx-schema.sql"),
			[]byte("CREATE TABLE db1.with_idx (id INT PRIMARY KEY, k INT, v VARCHAR(255), KEY idx_k (k), KEY idx_v (v), KEY idx_kv (k, v));"),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.with_idx.001.sql"),
			[]byte(buildInsertSQL("with_idx")),
			0o644,
		))

		estimateScanner, err := NewFileScanner(ctx, "file://"+estimateDir, db, defaultSDKConfig())
		require.NoError(t, err)
		defer estimateScanner.Close()

		estimate, err := estimateScanner.EstimateImportDataSize(ctx)
		require.NoError(t, err)
		require.Len(t, estimate.Tables, 2)

		tableEstimates := make(map[string]TableDataSizeEstimate, len(estimate.Tables))
		var totalSourceSize, totalTiKVSize int64
		for _, tableEstimate := range estimate.Tables {
			tableEstimates[tableEstimate.Table] = tableEstimate
			totalSourceSize += tableEstimate.SourceSize
			totalTiKVSize += tableEstimate.TiKVSize
			require.Positive(t, tableEstimate.TiKVSize)
		}

		require.Equal(t, totalSourceSize, estimate.TotalSourceSize)
		require.Equal(t, totalTiKVSize, estimate.TotalTiKVSize)
		require.Greater(t, tableEstimates["with_idx"].TiKVSize, tableEstimates["no_idx"].TiKVSize)
	})

	t.Run("EstimateImportDataSizeCSV", func(t *testing.T) {
		estimateDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(estimateDir, "db1-schema-create.sql"), []byte("CREATE DATABASE db1;"), 0o644))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.empty_csv-schema.sql"),
			[]byte("CREATE TABLE db1.empty_csv (id INT PRIMARY KEY, v VARCHAR(255));"),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.empty_csv.001.csv"),
			[]byte("id,v\n"),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.with_csv-schema.sql"),
			[]byte("CREATE TABLE db1.with_csv (id INT PRIMARY KEY, v VARCHAR(255), KEY idx_v (v));"),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.with_csv.001.csv"),
			[]byte("id,v\n1,\"hello,world\"\n"),
			0o644,
		))

		cfg := defaultSDKConfig()
		cfg.csvConfig.Header = true
		cfg.dataCharacterSet = "utf8mb4"
		estimateScanner, err := NewFileScanner(ctx, "file://"+estimateDir, db, cfg)
		require.NoError(t, err)
		defer estimateScanner.Close()

		estimate, err := estimateScanner.EstimateImportDataSize(ctx)
		require.NoError(t, err)
		require.Len(t, estimate.Tables, 2)

		tableEstimates := make(map[string]TableDataSizeEstimate, len(estimate.Tables))
		for _, tableEstimate := range estimate.Tables {
			tableEstimates[tableEstimate.Table] = tableEstimate
		}
		require.Equal(t, int64(0), tableEstimates["empty_csv"].TiKVSize)
		require.Positive(t, tableEstimates["with_csv"].TiKVSize)
	})

	t.Run("EstimateImportDataSizeSkipInvalidFiles", func(t *testing.T) {
		estimateDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(estimateDir, "db1-schema-create.sql"), []byte("CREATE DATABASE db1;"), 0o644))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.good-schema.sql"),
			[]byte("CREATE TABLE db1.good (id INT PRIMARY KEY, v VARCHAR(255));"),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.good.001.csv"),
			[]byte("1,good\n"),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.bad-schema.sql"),
			[]byte("CREATE TABL db1.bad (id INT PRIMARY KEY);"),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "db1.bad.001.csv"),
			[]byte("1\n"),
			0o644,
		))

		cfg := defaultSDKConfig()
		cfg.skipInvalidFiles = true
		estimateScanner, err := NewFileScanner(ctx, "file://"+estimateDir, db, cfg)
		require.NoError(t, err)
		defer estimateScanner.Close()

		estimate, err := estimateScanner.EstimateImportDataSize(ctx)
		require.NoError(t, err)
		require.Len(t, estimate.Tables, 1)
		require.Equal(t, "good", estimate.Tables[0].Table)
		require.Positive(t, estimate.Tables[0].SourceSize)
		require.Equal(t, estimate.Tables[0].SourceSize, estimate.TotalSourceSize)
		require.Equal(t, estimate.Tables[0].TiKVSize, estimate.TotalTiKVSize)
	})

	t.Run("EstimateImportDataSizeMultiStatementSchema", func(t *testing.T) {
		estimateDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(estimateDir, "test_db-schema-create.sql"), []byte("CREATE DATABASE test_db;"), 0o644))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "test_db.users-schema.sql"),
			[]byte(strings.Join([]string{
				"CREATE DATABASE IF NOT EXISTS test_db;",
				"USE test_db;",
				"DROP TABLE IF EXISTS users;",
				"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), KEY idx_name (name));",
			}, "\n")),
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(estimateDir, "test_db.users.001.csv"),
			[]byte("1,alice\n2,bob\n"),
			0o644,
		))

		cfg := defaultSDKConfig()
		cfg.skipInvalidFiles = true
		estimateScanner, err := NewFileScanner(ctx, "file://"+estimateDir, db, cfg)
		require.NoError(t, err)
		defer estimateScanner.Close()

		estimate, err := estimateScanner.EstimateImportDataSize(ctx)
		require.NoError(t, err)
		require.Len(t, estimate.Tables, 1)
		require.Equal(t, "users", estimate.Tables[0].Table)
		require.Positive(t, estimate.Tables[0].SourceSize)
		require.Positive(t, estimate.Tables[0].TiKVSize)
		require.Equal(t, estimate.Tables[0].SourceSize, estimate.TotalSourceSize)
		require.Equal(t, estimate.Tables[0].TiKVSize, estimate.TotalTiKVSize)
	})
}

func TestFileScannerWithEstimateRealSize(t *testing.T) {
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
	WithEstimateRealSize(false)(cfg2)
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
