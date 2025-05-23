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

package importer

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
	s.Require().NoError(err)
}

func (s *mockGCSSuite) TearDownSuite() {
	s.server.Stop()
}

func (s *mockGCSSuite) TestDumplingSource() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db1-schema-create.sql"},
		Content:     []byte("CREATE DATABASE IF NOT EXISTS db1;\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "dumpling", Name: "db1-schema-create.sql"},
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
	s.Require().NoError(err)
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
	s.Require().NoError(err)
	defer importSDK.Close()

	err = importSDK.CreateSchemasAndTables(context.Background())
	s.Require().NoError(err)

	tablesMeta, err := importSDK.GetTablesMeta(context.Background())
	s.Require().NoError(err)
	s.Require().Len(tablesMeta, 2)

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
	s.Require().Equal(expected1, tablesMeta[0])
	s.Require().Equal(expected2, tablesMeta[1])

	// verify GetTableMetaByName for each db
	tm1, err := importSDK.GetTableMetaByName(context.Background(), "db1", "tb1")
	s.Require().NoError(err)
	s.Require().Equal(tm1, expected1)
	tm2, err := importSDK.GetTableMetaByName(context.Background(), "db2", "tb2")
	s.Require().NoError(err)
	s.Require().Equal(tm2, expected2)

	totalSize, err := importSDK.GetTotalSize(context.Background())
	s.Require().NoError(err)
	s.Require().Equal(totalSize, int64(176))

	// check meets expectations
	err = mock.ExpectationsWereMet()
	s.Require().NoError(err)
}

func (s *mockGCSSuite) TestCSVSource() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "csv"})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "part1.csv"},
		Content:     []byte("a,b\n1,a\n2,b\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "csv", Name: "part2.csv"},
		Content:     []byte("a,b\n3,c\n4,d\n"),
	})

	db, mock, err := sqlmock.New()
	s.Require().NoError(err)
	defer db.Close()

	mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `db`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SHOW CREATE TABLE `db`.`tb`").
		WillReturnRows(sqlmock.NewRows([]string{"Create Table"}).AddRow("CREATE TABLE `db`.`tb` (`a` INT, `b` VARCHAR(10));"))
	importSDK, err := NewImportSDK(
		context.Background(),
		fmt.Sprintf("gs://csv?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint),
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
	s.Require().NoError(err)
	defer importSDK.Close()

	err = importSDK.CreateSchemasAndTables(context.Background())
	s.Require().NoError(err)

	tablesMeta, err := importSDK.GetTablesMeta(context.Background())
	s.Require().NoError(err)
	s.Require().Len(tablesMeta, 1)
	s.Require().Equal(&TableMeta{
		Database:   "db",
		Table:      "tb",
		SchemaFile: "",
		DataFiles: []DataFileMeta{
			{Path: "part1.csv", Size: 12, Format: mydump.SourceTypeCSV, Compression: mydump.CompressionNone},
			{Path: "part2.csv", Size: 12, Format: mydump.SourceTypeCSV, Compression: mydump.CompressionNone},
		},
		TotalSize:    24,
		WildcardPath: "gcs://csv/part*.csv",
	}, tablesMeta[0])

	tableMeta, err := importSDK.GetTableMetaByName(context.Background(), "db", "tb")
	s.Require().NoError(err)
	s.Require().Equal(tableMeta, tablesMeta[0])

	totalSize, err := importSDK.GetTotalSize(context.Background())
	s.Require().NoError(err)
	s.Require().Equal(totalSize, int64(24))

	err = mock.ExpectationsWereMet()
	s.Require().NoError(err)
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
	s := longestCommonSuffix(strs)
	require.Equal(t, "XYZ", s)

	// no common suffix
	require.Equal(t, "", longestCommonSuffix([]string{"a", "b"}))

	// empty inputs
	require.Equal(t, "", longestCommonSuffix(nil))
	require.Equal(t, "", longestCommonSuffix([]string{}))
}

func TestExtractCommonDirectory(t *testing.T) {
	paths := []string{"s3://bucket/a/b/c1.txt", "s3://bucket/a/b/c2.log"}
	dir := extractCommonDirectory(paths)
	require.Equal(t, "s3://bucket/a/b/", dir)

	// no slash
	require.Equal(t, "", extractCommonDirectory([]string{"foo", "bar"}))

	// empty inputs
	require.Equal(t, "", extractCommonDirectory(nil))
	require.Equal(t, "", extractCommonDirectory([]string{}))
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

func TestGenerateMydumperPattern(t *testing.T) {
	paths := []string{"db.tb.0001.sql", "db.tb.0002.sql"}
	p := generateMydumperPattern(paths)
	require.Equal(t, "db.tb.*.sql", p)

	paths2 := []string{"s3://bucket/dir/db.tb.0001.sql", "s3://bucket/dir/db.tb.0002.sql"}
	p2 := generateMydumperPattern(paths2)
	require.Equal(t, "s3://bucket/dir/db.tb.*.sql", p2)

	// empty inputs
	require.Equal(t, "", generateMydumperPattern(nil))
	require.Equal(t, "", generateMydumperPattern([]string{}))

	// not mydumper pattern
	paths3 := []string{"db-tb-sql", "db-tb-0001.sql"}
	require.Equal(t, "", generateMydumperPattern(paths3))
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
		},
	}
	df := createDataFileMeta(fi)
	require.Equal(t, "s3://bucket/path/to/f", df.Path)
	require.Equal(t, int64(123), df.Size)
	require.Equal(t, mydump.SourceTypeCSV, df.Format)
	require.Equal(t, mydump.CompressionGZ, df.Compression)
}

func TestProcessDataFiles(t *testing.T) {
	files := []mydump.FileInfo{
		{FileMeta: mydump.SourceFileMeta{Path: "s3://bucket/a", RealSize: 10}},
		{FileMeta: mydump.SourceFileMeta{Path: "s3://bucket/b", RealSize: 20}},
	}
	dfm, total, err := processDataFiles(files)
	require.NoError(t, err)
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
	require.True(t, validatePattern("*.txt", tableFiles, smallAll))

	// allFiles includes an extra file => invalid
	fullAll := map[string]mydump.FileInfo{
		"a.txt": {}, "b.txt": {}, "c.txt": {},
	}
	require.False(t, validatePattern("*.txt", tableFiles, fullAll))

	// If pattern doesn't match our table's file, it's also invalid
	require.False(t, validatePattern("*.csv", tableFiles, smallAll))

	// empty pattern => invalid
	require.False(t, validatePattern("", tableFiles, smallAll))
}

func TestExtractMydumperNames(t *testing.T) {
	paths := []string{
		"db1.tbl1.0001.sql",
		"db1.tbl1.0002.sql",
		"db1.tbl1-schema.sql",
	}
	db, tbl := extractMydumperNames(paths)
	require.Equal(t, "db1", db)
	require.Equal(t, "tbl1", tbl)

	// inconsistent naming => empty
	db2, tbl2 := extractMydumperNames([]string{
		"db.tbl1.0001.sql",
		"other.tbl1.0002.sql",
	})
	require.Equal(t, "", db2)
	require.Equal(t, "", tbl2)
}
