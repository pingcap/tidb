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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
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
	assertSecretsRedacted := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.ErrorContains(t, err, "access-key=xxxxxx")
		require.ErrorContains(t, err, "secret-access-key=xxxxxx")
		require.ErrorContains(t, err, "session-token=xxxxxx")
		require.NotContains(t, err.Error(), "access-key=ak")
		require.NotContains(t, err.Error(), "secret-access-key=sk")
		require.NotContains(t, err.Error(), "session-token=token")
	}

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

	t.Run("NewFileScannerRedactsSensitiveSourcePathInParseErrors", func(t *testing.T) {
		_, err := NewFileScanner(
			ctx,
			"s3://?access-key=ak&secret-access-key=sk&session-token=token",
			db,
			cfg,
		)
		assertSecretsRedacted(t, err)
	})

	t.Run("NewFileScannerHidesMalformedSensitiveSourcePathInParseErrors", func(t *testing.T) {
		_, err := NewFileScanner(
			ctx,
			"1invalid:?secret-access-key=sk",
			db,
			cfg,
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "source="+redactedInvalidSourcePath)
		require.NotContains(t, err.Error(), "secret-access-key=sk")
	})

	t.Run("CreateSchemasAndTablesRedactsSensitiveSourcePathOnError", func(t *testing.T) {
		invalidDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(invalidDir, "db1-schema-create.sql"), []byte("CREATE DATABASE db1;"), 0o644))
		require.NoError(t, os.WriteFile(
			filepath.Join(invalidDir, "db1.t1-schema.sql"),
			[]byte("CREATE TABLE t1 (id INT,);"),
			0o644,
		))

		invalidDB, invalidMock, err := sqlmock.New()
		require.NoError(t, err)
		defer invalidDB.Close()

		invalidScanner, err := NewFileScanner(ctx, "file://"+invalidDir, invalidDB, defaultSDKConfig())
		require.NoError(t, err)
		defer invalidScanner.Close()

		fs := invalidScanner.(*fileScanner)
		sourcePath := "s3://bucket/path?access-key=ak&secret-access-key=sk&session-token=token"
		fs.redactedSourcePath = ast.RedactURL(sourcePath)

		invalidMock.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA.*").WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
		invalidMock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE IF NOT EXISTS `db1`")).WillReturnResult(sqlmock.NewResult(0, 0))
		invalidMock.ExpectQuery("SHOW CREATE TABLE `db1`.`t1`").WillReturnError(&dmysql.MySQLError{Number: tmysql.ErrNoSuchTable})

		err = invalidScanner.CreateSchemasAndTables(ctx)
		assertSecretsRedacted(t, err)
		require.ErrorContains(t, err, "invalid schema statement")
		require.NoError(t, invalidMock.ExpectationsWereMet())
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

	t.Run("EstimateAuroraDataOnly", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "export/db/db.users/a/part-a.parquet")
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte("data"), 0o644))
		cfg := defaultSDKConfig()
		cfg.estimateRealSize = false
		scanner, err := NewFileScanner(ctx, "file://"+dir, nil, cfg)
		require.NoError(t, err)
		defer scanner.Close()
		for _, skip := range []bool{false, true} {
			cfg.skipInvalidFiles = skip
			estimate, err := scanner.EstimateImportDataSize(ctx)
			require.ErrorContains(t, err, "schema not found")
			require.Nil(t, estimate)
		}
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

func TestAuroraSourceSafety(t *testing.T) {
	const first = "export-a/db/db.users/1/part-a.parquet"
	for _, tc := range []struct {
		name    string
		paths   []string
		options []SDKOption
		want    string
		err     string
		count   int
	}{
		{name: "dotted table", paths: []string{"export/sales/sales.order.items/1/part-a.parquet"}, want: "sales/order.items"},
		{name: "dotted database", paths: []string{"export/sales.v1/sales.v1.order.items/part-a.parquet"}, want: "sales.v1/order.items"},
		{name: "regex metacharacters", paths: []string{"export+(v1)/db+$1/db+$1.order+(items)/part-a.parquet"}, want: "db+$1/order+(items)"},
		{name: "glob export root brackets", paths: []string{"export[1]/db/db.users/a/part-a.parquet"}, err: "glob"},
		{name: "glob export root star", paths: []string{"export*/db/db.users/a/part-a.parquet"}, err: "glob"},
		{name: "glob file name", paths: []string{"export/db/db.users/a/part-*.parquet"}, err: "glob"},
		{name: "literal percent", paths: []string{"export/db%20/db%20.order%2Eitems/1/part-a.parquet"}, want: "db%20/order%2Eitems"},
		{name: "single export scoped URL", paths: []string{"db/db.users/1/part-a.parquet", "db/db.users/2/part-b.parquet"}, want: "db/users"},
		{name: "alphanumeric partitions", paths: []string{"export/db/db.users/a/part-00000-id.gz.parquet", "export/db/db.users/A1/part-00000-id.gz.parquet"}, want: "db/users"},
		{name: "uppercase suffix", paths: []string{"prefix.with.dots/export/db/db.users/00042/PART-a.GZ.PARQUET"}, want: "db/users"},
		{name: "literal encoded slash", paths: []string{"export/db/db.order%2Fitems/part-a.parquet"}, want: "db/order%2Fitems"},
		{name: "ambiguous table", paths: []string{"export/db/db.order_items/1/part-a.parquet"}, err: "ambiguous"},
		{name: "ambiguous database", paths: []string{"export/my_db/my_db.users/1/part-a.parquet"}, err: "ambiguous"},
		{name: "non-native space", paths: []string{"export/my db/my db.users/1/part-a.parquet"}, err: "ambiguous"},
		{name: "two roots same table", paths: []string{first, "export-b/db/db.users/1/part-b.parquet"}, err: "multiple"},
		{name: "two roots different tables", paths: []string{first, "export-b/other/other.orders/part-b.parquet"}, err: "multiple"},
		{name: "root is full prefix", paths: []string{"prefix/a/export/db/db.users/part-a.parquet", "prefix/b/export/db/db.users/part-b.parquet"}, err: "multiple"},
		{name: "empty and nonempty roots", paths: []string{"db/db.users/part-a.parquet", first}, err: "multiple"},
		{name: "filter cannot hide root", paths: []string{first, "export-b/other/other.orders/part-b.parquet"}, options: []SDKOption{WithFilter([]string{"db.users"})}, err: "multiple"},
		{name: "skip cannot hide mixed", paths: []string{first, "db.orders.1.csv"}, options: []SDKOption{WithSkipInvalidFiles(true)}, err: "mixed"},
		{name: "unmatched parquet", paths: []string{first, "unmatched.parquet"}, err: "mixed"},
		{name: "compressed parquet", paths: []string{first, "db.orders.1.parquet.gz"}, err: "parquet"},
		{name: "inconsistent directory", paths: []string{"archive/customer/staging.users/1/part-a.parquet"}, err: "inconsistent"},
		{name: "invalid batch", paths: []string{first, "export-a/db/db.orders/batch-1/part-b.parquet"}, err: "mixed"},
		{name: "extra depth", paths: []string{first, "export-a/db/db.orders/1/extra/part-b.parquet"}, err: "mixed"},
		{name: "non-parquet table object", paths: []string{first, "export-a/db/db.orders/1/part-b.csv"}, err: "mixed"},
		{name: "truncated aurora", paths: []string{first, "export-a/db/db.users/2/part-b.parquet"}, options: []SDKOption{WithMaxScanFiles(1)}, err: "incomplete"},
		{name: "truncated before aurora", paths: []string{"aaa.tbl.1.csv", first}, options: []SDKOption{WithMaxScanFiles(1)}, err: "incomplete"},
		{name: "generic nested parquet", paths: []string{"backup/v1.0/db.users.0000.parquet"}, want: "db/users"},
		{name: "generic part file", paths: []string{"backup.v1/part-db.users.0001.sql"}, want: "part-db/users"},
		{name: "ignored backup", paths: []string{first, "backup.v1/part-old.parquet.bak"}, want: "db/users", count: 1},
		{name: "generic basename wins", paths: []string{"backup/customer/customer.orders/1/db.users.0000.parquet"}, want: "db/users"},
		{name: "existing table route", paths: []string{first}, options: []SDKOption{WithRoutes(config.Routes{{SchemaPattern: "db", TablePattern: "users", TargetSchema: "target", TargetTable: "people"}})}, want: "target/people"},
		{name: "single root filter", paths: []string{first, "export-a/other/other.orders/part-b.parquet"}, options: []SDKOption{WithFilter([]string{"db.users"})}, want: "db/users", count: 1},
		{name: "explicit file and table routes remain exclusive", paths: []string{first}, options: []SDKOption{WithFileRouters([]*config.FileRouteRule{{Pattern: `.*\.parquet$`, Schema: "db", Table: "users", Type: "parquet"}}), WithRoutes(config.Routes{{SchemaPattern: "db", TargetSchema: "other"}})}, err: "can't config both"},
		{name: "explicit router overrides detection", paths: []string{"a/my_db/my_db.table_name/part-a.parquet", "b/my_db/my_db.table_name/part-b.parquet"}, options: []SDKOption{WithFileRouters([]*config.FileRouteRule{{Pattern: `.*\.parquet$`, Schema: "target", Table: "chosen", Type: "parquet"}})}, want: "target/chosen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, path := range tc.paths {
				full := filepath.Join(dir, path)
				require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
				require.NoError(t, os.WriteFile(full, []byte("data"), 0o644))
			}
			cfg := defaultSDKConfig()
			WithEstimateRealSize(false)(cfg)
			for _, opt := range tc.options {
				opt(cfg)
			}
			scanner, err := NewFileScanner(context.Background(), "file://"+dir, nil, cfg)
			if scanner != nil {
				t.Cleanup(func() { require.NoError(t, scanner.Close()) })
			}
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				require.Nil(t, scanner)
				return
			}
			require.NoError(t, err)
			metas, err := scanner.GetTableMetas(context.Background())
			require.NoError(t, err)
			require.Len(t, metas, 1)
			require.Equal(t, tc.want, metas[0].Database+"/"+metas[0].Table)
			count := tc.count
			if count == 0 {
				count = len(tc.paths)
			}
			require.Len(t, metas[0].DataFiles, count)
		})
	}
}

type storageWithURI struct {
	storeapi.Storage
	uri string
}

func (s *storageWithURI) URI() string { return s.uri }

func TestAuroraWildcardURIPreservesRawKey(t *testing.T) {
	scanner := &fileScanner{
		auroraSource: true,
		store:        &storageWithURI{uri: "s3://bucket/prefix%2E/"},
	}
	key := "export/db/db.order%2Eitems/part-a.parquet"
	file := mydump.FileInfo{FileMeta: mydump.SourceFileMeta{Path: key, Type: mydump.SourceTypeParquet}}
	meta, err := scanner.buildTableMeta(&mydump.MDDatabaseMeta{Name: "db"},
		&mydump.MDTableMeta{Name: "order%2Eitems", DataFiles: []mydump.FileInfo{file}},
		map[string]mydump.FileInfo{key: file})
	require.NoError(t, err)
	u, err := url.Parse(meta.WildcardPath)
	require.NoError(t, err)
	require.Equal(t, "/prefix%2E/"+key, u.Path)
	for _, prefix := range []string{"tenant[1]", "tenant*", "tenant?", "tenant\\"} {
		scanner.store = &storageWithURI{uri: "s3://bucket/" + prefix + "/"}
		meta, err = scanner.buildTableMeta(&mydump.MDDatabaseMeta{Name: "db"},
			&mydump.MDTableMeta{Name: "order%2Eitems", DataFiles: []mydump.FileInfo{file}},
			map[string]mydump.FileInfo{key: file})
		require.ErrorContains(t, err, "glob")
		require.Nil(t, meta)
	}
}
