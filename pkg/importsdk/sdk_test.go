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
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type mockGCSSuite struct {
	suite.Suite

	server *fakestorage.Server
	pool   *pools.ResourcePool
}

var (
	gcsHost = "127.0.0.1"
	gcsPort = uint16(4443)
	// for fake gcs server, we must use this endpoint format
	// NOTE: must end with '/'
	gcsEndpointFormat = "http://%s:%d/storage/v1/"
	gcsEndpoint       = fmt.Sprintf(gcsEndpointFormat, gcsHost, gcsPort)
)

func TestCloudSDK(t *testing.T) {
	suite.Run(t, &mockGCSSuite{})
}

func (s *mockGCSSuite) SetupSuite() {
	var err error
	opt := fakestorage.Options{
		Scheme:     "http",
		Host:       gcsHost,
		Port:       gcsPort,
		PublicHost: gcsHost,
	}
	s.server, err = fakestorage.NewServerWithOptions(opt)
	s.NoError(err)
}

func (s *mockGCSSuite) TearDownSuite() {
	s.server.Stop()
}

func (s *mockGCSSuite) TestDumplingSource() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db1-schema-create.sql"},
		Content:     []byte("CREATE DATABASE IF NOT EXISTS db1;\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db2-schema-create.sql"},
		Content:     []byte("CREATE DATABASE IF NOT EXISTS db2;\n"),
	})
	// table1 in db1
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db1.tb1-schema.sql"},
		Content:     []byte("CREATE TABLE IF NOT EXISTS db1.tb1 (a INT, b VARCHAR(10));\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db1.tb1.001.sql"},
		Content:     []byte("INSERT INTO db1.tb1 VALUES (1,'a'),(2,'b');\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db1.tb1.002.sql"},
		Content:     []byte("INSERT INTO db1.tb1 VALUES (3,'c'),(4,'d');\n"),
	})
	// table2 in db2
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db2.tb2-schema.sql"},
		Content:     []byte("CREATE TABLE IF NOT EXISTS db2.tb2 (x INT, y VARCHAR(10));\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db2.tb2.001.sql"},
		Content:     []byte("INSERT INTO db2.tb2 VALUES (5,'e'),(6,'f');\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db2.tb2.002.sql"},
		Content:     []byte("INSERT INTO db2.tb2 VALUES (7,'g'),(8,'h');\n"),
	})

	db, mock, err := sqlmock.New()
	s.NoError(err)
	defer db.Close()

	mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `db1`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `db2`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db1`.`tb1` (`a` INT,`b` VARCHAR(10));")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db2`.`tb2` (`x` INT,`y` VARCHAR(10));")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	importSDK, err := NewImportSDK(
		context.Background(),
		fmt.Sprintf("gs://dumpling?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint),
		db,
		WithConcurrency(1),
	)
	s.NoError(err)
	defer importSDK.Close()

	err = importSDK.CreateSchemasAndTables(context.Background())
	s.NoError(err)

	tablesMeta, err := importSDK.GetTableMetas(context.Background())
	s.NoError(err)
	s.Len(tablesMeta, 2)

	expected1 := &TableMeta{
		Database:   "db1",
		Table:      "tb1",
		SchemaFile: "db1.tb1-schema.sql",
		DataFiles: []DataFileMeta{
			{Path: "db1.tb1.001.sql", Size: 44, Format: mydump.SourceTypeSQL, Compression: mydump.CompressionNone},
			{Path: "db1.tb1.002.sql", Size: 44, Format: mydump.SourceTypeSQL, Compression: mydump.CompressionNone},
		},
		TotalSize:    88,
		WildcardPath: "gcs://dumpling/db1.tb1.*.sql",
	}
	expected2 := &TableMeta{
		Database:   "db2",
		Table:      "tb2",
		SchemaFile: "db2.tb2-schema.sql",
		DataFiles: []DataFileMeta{
			{Path: "db2.tb2.001.sql", Size: 44, Format: mydump.SourceTypeSQL, Compression: mydump.CompressionNone},
			{Path: "db2.tb2.002.sql", Size: 44, Format: mydump.SourceTypeSQL, Compression: mydump.CompressionNone},
		},
		TotalSize:    88,
		WildcardPath: "gcs://dumpling/db2.tb2.*.sql",
	}
	s.Equal(expected1, tablesMeta[0])
	s.Equal(expected2, tablesMeta[1])

	// verify GetTableMetaByName for each db
	tm1, err := importSDK.GetTableMetaByName(context.Background(), "db1", "tb1")
	s.NoError(err)
	s.Equal(tm1, expected1)
	tm2, err := importSDK.GetTableMetaByName(context.Background(), "db2", "tb2")
	s.NoError(err)
	s.Equal(tm2, expected2)

	totalSize := importSDK.GetTotalSize(context.Background())
	s.Equal(totalSize, int64(176))

	// check meets expectations
	err = mock.ExpectationsWereMet()
	s.NoError(err)
}

func (s *mockGCSSuite) TestCSVSource() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "db1-schema-create.sql"},
		Content:     []byte("CREATE DATABASE IF NOT EXISTS db1;\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "db2-schema-create.sql"},
		Content:     []byte("CREATE DATABASE IF NOT EXISTS db2;\n"),
	})
	// table1 in db1
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "db1.tb1-schema.sql"},
		Content:     []byte("CREATE TABLE IF NOT EXISTS db1.tb1 (a INT, b VARCHAR(10));\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "db1.tb1.001.csv"},
		Content:     []byte("1,a\n2,b\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "db1.tb1.002.csv"},
		Content:     []byte("3,c\n4,d\n"),
	})
	// table2 in db2
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "db2.tb2-schema.sql"},
		Content:     []byte("CREATE TABLE IF NOT EXISTS db2.tb2 (x INT, y VARCHAR(10));\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "db2.tb2.001.csv"},
		Content:     []byte("5,e\n6,f\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "db2.tb2.002.csv"},
		Content:     []byte("7,g\n8,h\n"),
	})

	db, mock, err := sqlmock.New()
	s.NoError(err)
	defer db.Close()

	mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `db1`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `db2`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db1`.`tb1` (`a` INT,`b` VARCHAR(10));")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db2`.`tb2` (`x` INT,`y` VARCHAR(10));")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	importSDK, err := NewImportSDK(
		context.Background(),
		fmt.Sprintf("gs://csv?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint),
		db,
		WithConcurrency(1),
	)
	s.NoError(err)
	defer importSDK.Close()

	err = importSDK.CreateSchemasAndTables(context.Background())
	s.NoError(err)

	tablesMeta, err := importSDK.GetTableMetas(context.Background())
	s.NoError(err)
	s.Len(tablesMeta, 2)

	expected1 := &TableMeta{
		Database:   "db1",
		Table:      "tb1",
		SchemaFile: "db1.tb1-schema.sql",
		DataFiles: []DataFileMeta{
			{Path: "db1.tb1.001.csv", Size: 8, Format: mydump.SourceTypeCSV, Compression: mydump.CompressionNone},
			{Path: "db1.tb1.002.csv", Size: 8, Format: mydump.SourceTypeCSV, Compression: mydump.CompressionNone},
		},
		TotalSize:    16,
		WildcardPath: "gcs://csv/db1.tb1.*.csv",
	}
	expected2 := &TableMeta{
		Database:   "db2",
		Table:      "tb2",
		SchemaFile: "db2.tb2-schema.sql",
		DataFiles: []DataFileMeta{
			{Path: "db2.tb2.001.csv", Size: 8, Format: mydump.SourceTypeCSV, Compression: mydump.CompressionNone},
			{Path: "db2.tb2.002.csv", Size: 8, Format: mydump.SourceTypeCSV, Compression: mydump.CompressionNone},
		},
		TotalSize:    16,
		WildcardPath: "gcs://csv/db2.tb2.*.csv",
	}
	s.Equal(expected1, tablesMeta[0])
	s.Equal(expected2, tablesMeta[1])

	// verify GetTableMetaByName for each db
	tm1, err := importSDK.GetTableMetaByName(context.Background(), "db1", "tb1")
	s.NoError(err)
	s.Equal(tm1, expected1)
	tm2, err := importSDK.GetTableMetaByName(context.Background(), "db2", "tb2")
	s.NoError(err)
	s.Equal(tm2, expected2)

	totalSize := importSDK.GetTotalSize(context.Background())
	s.Equal(totalSize, int64(32))

	// check meets expectations
	err = mock.ExpectationsWereMet()
	s.NoError(err)
}

func (s *mockGCSSuite) TestOnlyDataFiles() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "onlydata"})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "onlydata", Name: "part1.csv"},
		Content:     []byte("a,b\n1,a\n2,b\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "onlydata", Name: "part2.csv"},
		Content:     []byte("a,b\n3,c\n4,d\n"),
	})

	db, mock, err := sqlmock.New()
	s.NoError(err)
	defer db.Close()

	mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `db`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SHOW CREATE TABLE `db`.`tb`").
		WillReturnRows(sqlmock.NewRows([]string{"Create Table"}).AddRow("CREATE TABLE `db`.`tb` (`a` INT, `b` VARCHAR(10));"))
	importSDK, err := NewImportSDK(
		context.Background(),
		fmt.Sprintf("gs://onlydata?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint),
		db,
		WithCharset("utf8"),
		WithConcurrency(8),
		WithFilter([]string{"*.*"}),
		WithSQLMode(mysql.ModeANSIQuotes),
		WithLogger(log.L()),
		WithFileRouters([]*config.FileRouteRule{
			{
				Pattern: `.*\.csv$`,
				Schema:  "db",
				Table:   "tb",
				Type:    "csv",
			},
		}),
	)
	s.NoError(err)
	defer importSDK.Close()

	err = importSDK.CreateSchemasAndTables(context.Background())
	s.NoError(err)

	tablesMeta, err := importSDK.GetTableMetas(context.Background())
	s.NoError(err)
	s.Len(tablesMeta, 1)
	s.Equal(&TableMeta{
		Database:   "db",
		Table:      "tb",
		SchemaFile: "",
		DataFiles: []DataFileMeta{
			{Path: "part1.csv", Size: 12, Format: mydump.SourceTypeCSV, Compression: mydump.CompressionNone},
			{Path: "part2.csv", Size: 12, Format: mydump.SourceTypeCSV, Compression: mydump.CompressionNone},
		},
		TotalSize:    24,
		WildcardPath: "gcs://onlydata/part*.csv",
	}, tablesMeta[0])

	tableMeta, err := importSDK.GetTableMetaByName(context.Background(), "db", "tb")
	s.NoError(err)
	s.Equal(tableMeta, tablesMeta[0])

	totalSize := importSDK.GetTotalSize(context.Background())
	s.Equal(totalSize, int64(24))

	err = mock.ExpectationsWereMet()
	s.NoError(err)
}

func TestLongestCommonPrefix(t *testing.T) {
	strs := []string{"s3://bucket/foo/bar/baz1", "s3://bucket/foo/bar/baz2", "s3://bucket/foo/bar/baz"}
	p := longestCommonPrefix(strs)
	require.Equal(t, "s3://bucket/foo/bar/baz", p)

	// no common prefix
	require.Equal(t, "", longestCommonPrefix([]string{"a", "b"}))

	// empty inputs
	require.Equal(t, "", longestCommonPrefix(nil))
	require.Equal(t, "", longestCommonPrefix([]string{}))
}

func TestLongestCommonSuffix(t *testing.T) {
	strs := []string{"abcXYZ", "defXYZ", "XYZ"}
	s := longestCommonSuffix(strs, 0)
	require.Equal(t, "XYZ", s)

	// no common suffix
	require.Equal(t, "", longestCommonSuffix([]string{"a", "b"}, 0))

	// empty inputs
	require.Equal(t, "", longestCommonSuffix(nil, 0))
	require.Equal(t, "", longestCommonSuffix([]string{}, 0))

	// same prefix
	require.Equal(t, "", longestCommonSuffix([]string{"abc", "abc"}, 3))
	require.Equal(t, "f", longestCommonSuffix([]string{"abcdf", "abcef"}, 3))
}

func TestGeneratePrefixSuffixPattern(t *testing.T) {
	paths := []string{"pre_middle_suf", "pre_most_suf"}
	pattern := generatePrefixSuffixPattern(paths)
	// common prefix "pre_m", suffix "_suf"
	require.Equal(t, "pre_m*_suf", pattern)

	// empty inputs
	require.Equal(t, "", generatePrefixSuffixPattern(nil))
	require.Equal(t, "", generatePrefixSuffixPattern([]string{}))

	// only one file
	require.Equal(t, "pre_middle_suf", generatePrefixSuffixPattern([]string{"pre_middle_suf"}))

	// no common prefix/suffix
	paths2 := []string{"foo", "bar"}
	require.Equal(t, "*", generatePrefixSuffixPattern(paths2))

	// overlapping prefix/suffix
	paths3 := []string{"aaabaaa", "aaa"}
	require.Equal(t, "aaa*", generatePrefixSuffixPattern(paths3))
}

func generateFileMetas(t *testing.T, paths []string) []mydump.FileInfo {
	t.Helper()

	files := make([]mydump.FileInfo, 0, len(paths))
	fileRouter, err := mydump.NewDefaultFileRouter(log.L())
	require.NoError(t, err)
	for _, p := range paths {
		res, err := fileRouter.Route(p)
		require.NoError(t, err)
		files = append(files, mydump.FileInfo{
			TableName: res.Table,
			FileMeta: mydump.SourceFileMeta{
				Path:        p,
				Type:        res.Type,
				Compression: res.Compression,
				SortKey:     res.Key,
			},
		})
	}
	return files
}

func TestGenerateMydumperPattern(t *testing.T) {
	paths := []string{"db.tb.0001.sql", "db.tb.0002.sql"}
	p := generateMydumperPattern(generateFileMetas(t, paths)[0])
	require.Equal(t, "db.tb.*.sql", p)

	paths2 := []string{"s3://bucket/dir/db.tb.0001.sql", "s3://bucket/dir/db.tb.0002.sql"}
	p2 := generateMydumperPattern(generateFileMetas(t, paths2)[0])
	require.Equal(t, "s3://bucket/dir/db.tb.*.sql", p2)

	// not mydumper pattern
	require.Equal(t, "", generateMydumperPattern(mydump.FileInfo{
		TableName: filter.Table{},
	}))
}

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

func TestValidatePattern(t *testing.T) {
	tableFiles := map[string]struct{}{
		"a.txt": {}, "b.txt": {},
	}
	// only table files in allFiles
	smallAll := map[string]mydump.FileInfo{
		"a.txt": {}, "b.txt": {},
	}
	require.True(t, isValidPattern("*.txt", tableFiles, smallAll))

	// allFiles includes an extra file => invalid
	fullAll := map[string]mydump.FileInfo{
		"a.txt": {}, "b.txt": {}, "c.txt": {},
	}
	require.False(t, isValidPattern("*.txt", tableFiles, fullAll))

	// If pattern doesn't match our table's file, it's also invalid
	require.False(t, isValidPattern("*.csv", tableFiles, smallAll))

	// empty pattern => invalid
	require.False(t, isValidPattern("", tableFiles, smallAll))
}

func TestGenerateWildcardPath(t *testing.T) {
	// Helper to create allFiles map
	createAllFiles := func(paths []string) map[string]mydump.FileInfo {
		allFiles := make(map[string]mydump.FileInfo)
		for _, p := range paths {
			allFiles[p] = mydump.FileInfo{
				FileMeta: mydump.SourceFileMeta{Path: p},
			}
		}
		return allFiles
	}

	// No files
	files1 := []mydump.FileInfo{}
	allFiles1 := createAllFiles([]string{})
	_, err := generateWildcardPath(files1, allFiles1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no data files for table")

	// Single file
	files2 := generateFileMetas(t, []string{"db.tb.0001.sql"})
	allFiles2 := createAllFiles([]string{"db.tb.0001.sql"})
	path2, err := generateWildcardPath(files2, allFiles2)
	require.NoError(t, err)
	require.Equal(t, "db.tb.0001.sql", path2)

	// Mydumper pattern succeeds
	files3 := generateFileMetas(t, []string{"db.tb.0001.sql.gz", "db.tb.0002.sql.gz"})
	allFiles3 := createAllFiles([]string{"db.tb.0001.sql.gz", "db.tb.0002.sql.gz"})
	path3, err := generateWildcardPath(files3, allFiles3)
	require.NoError(t, err)
	require.Equal(t, "db.tb.*.sql.gz", path3)

	// Mydumper pattern fails, fallback to prefix/suffix succeeds
	files4 := []mydump.FileInfo{
		{
			TableName: filter.Table{Schema: "db", Name: "tb"},
			FileMeta: mydump.SourceFileMeta{
				Path:        "a.sql",
				Type:        mydump.SourceTypeSQL,
				Compression: mydump.CompressionNone,
			},
		},
		{
			TableName: filter.Table{Schema: "db", Name: "tb"},
			FileMeta: mydump.SourceFileMeta{
				Path:        "b.sql",
				Type:        mydump.SourceTypeSQL,
				Compression: mydump.CompressionNone,
			},
		},
	}
	allFiles4 := map[string]mydump.FileInfo{
		files4[0].FileMeta.Path: files4[0],
		files4[1].FileMeta.Path: files4[1],
	}
	path4, err := generateWildcardPath(files4, allFiles4)
	require.NoError(t, err)
	require.Equal(t, "*.sql", path4)

	allFiles4["db-schema.sql"] = mydump.FileInfo{
		FileMeta: mydump.SourceFileMeta{
			Path:        "db-schema.sql",
			Type:        mydump.SourceTypeSQL,
			Compression: mydump.CompressionNone,
		},
	}
	_, err = generateWildcardPath(files4, allFiles4)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot generate a unique wildcard pattern")
}

func (s *mockGCSSuite) TestSkipInvalidFiles() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "invalid_files"})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "invalid_files", Name: "db1.tb1-schema.sql"},
		Content:     []byte("CREATE TABLE IF NOT EXISTS db1.tb1 (a INT, b VARCHAR(10));\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "invalid_files", Name: "uez.md"},
		Content:     []byte("test;\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "invalid_files", Name: "db2.tb2-schema.sql"},
		Content:     []byte("CREATE TABLE IF NOT EXISTS db2.tb2 (x INT, y VARCHAR(10));\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "invalid_files", Name: "db2.tb2.001.csv"},
		Content:     []byte("5,e\n6,f\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "invalid_files", Name: "db2.tb2.002.csv"},
		Content:     []byte("7,g\n8,h\n"),
	})

	db, mock, err := sqlmock.New()
	s.NoError(err)
	defer db.Close()

	mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `db`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SHOW CREATE TABLE `db`.`tb`").
		WillReturnRows(sqlmock.NewRows([]string{"Create Table"}).AddRow("CREATE TABLE `db`.`tb` (`a` INT, `b` VARCHAR(10));"))
	importSDK, err := NewImportSDK(
		context.Background(),
		fmt.Sprintf("gs://invalid_files?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint),
		db,
		WithCharset("utf8"),
		WithConcurrency(8),
		WithFilter([]string{"*.*"}),
		WithSQLMode(mysql.ModeANSIQuotes),
		WithLogger(log.L()),
		WithSkipInvalidFiles(true),
	)
	s.NoError(err)
	defer importSDK.Close()
	metas, err := importSDK.GetTableMetas(context.Background())
	s.NoError(err)
	s.Len(metas, 1)
}

func (s *mockGCSSuite) TestScanLimitation() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "limitation"})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "limitation", Name: "db2.tb2.001.csv"},
		Content:     []byte("5,e\n6,f\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "limitation", Name: "db2.tb2.002.csv"},
		Content:     []byte("7,g\n8,h\n"),
	})
	db, _, err := sqlmock.New()
	s.NoError(err)
	defer db.Close()
	importSDK, err := NewImportSDK(
		context.Background(),
		fmt.Sprintf("gs://limitation?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint),
		db,
		WithCharset("utf8"),
		WithConcurrency(8),
		WithFilter([]string{"*.*"}),
		WithSQLMode(mysql.ModeANSIQuotes),
		WithLogger(log.L()),
		WithSkipInvalidFiles(true),
		WithMaxScanFiles(1),
	)
	s.NoError(err)
	defer importSDK.Close()
	metas, err := importSDK.GetTableMetas(context.Background())
	s.NoError(err)
	s.Len(metas, 1)
	s.Len(metas[0].DataFiles, 1)
}

func (s *mockGCSSuite) TestCreateTableMetaByName() {
	for i := 0; i < 2; i++ {
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "specific-table-test", Name: fmt.Sprintf("db%d-schema-create.sql", i)},
			Content:     []byte(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS db%d;\n", i)),
		})
		for j := 0; j != 2; j++ {
			tableName := fmt.Sprintf("db%d.tb%d", i, j)
			s.server.CreateObject(fakestorage.Object{
				ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "specific-table-test", Name: fmt.Sprintf("%s-schema.sql", tableName)},
				Content:     []byte(fmt.Sprintf("CREATE TABLE IF NOT EXISTS db%d.tb%d (a INT, b VARCHAR(10));\n", i, j)),
			})
			s.server.CreateObject(fakestorage.Object{
				ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "specific-table-test", Name: fmt.Sprintf("%s.001.sql", tableName)},
				Content:     []byte(fmt.Sprintf("INSERT INTO db%d.tb%d VALUES (1,'a'),(2,'b');\n", i, j)),
			})
			s.server.CreateObject(fakestorage.Object{
				ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "specific-table-test", Name: fmt.Sprintf("%s.002.sql", tableName)},
				Content:     []byte(fmt.Sprintf("INSERT INTO db%d.tb%d VALUES (3,'c'),(4,'d');\n", i, j)),
			})
		}
	}

	db, mock, err := sqlmock.New()
	s.NoError(err)
	defer db.Close()

	mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `db1`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `db1`.`tb1` (`a` INT,`b` VARCHAR(10));")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	importSDK, err := NewImportSDK(
		context.Background(),
		fmt.Sprintf("gs://specific-table-test?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint),
		db,
		WithConcurrency(1),
	)
	s.NoError(err)
	defer importSDK.Close()

	err = importSDK.CreateSchemaAndTableByName(context.Background(), "db1", "tb1")
	s.NoError(err)
}
