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

package importintotest

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func (s *mockGCSSuite) TestCSVSource() {
	// prepare source data
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_csv", Name: "t.1.csv"},
		Content:     []byte("1,foo1,bar1,123\n2,foo2,bar2,456\n3,foo3,bar3,789\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_csv", Name: "t.2.csv"},
		Content:     []byte("4,foo4,bar4,123\n5,foo5,bar5,223\n6,foo6,bar6,323\n"),
	})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	sortStorageURI := fmt.Sprintf("gs://sorted/cloud_csv?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
	sourceURI := fmt.Sprintf("gs://cloud_csv/?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)

	// create database and table
	s.prepareAndUseDB("cloud_csv")
	s.tk.MustExec(`create table t (a bigint primary key, b varchar(100), c varchar(100), d int,
		key(a), key(c,d), key(d));`)

	db, mock, err := sqlmock.New()
	s.Require().NoError(err)
	defer db.Close()
	mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("cloud_csv"))
	mock.ExpectQuery("SHOW CREATE TABLE `cloud_csv`.`t`").
		WillReturnRows(sqlmock.NewRows([]string{"Create Table"}).AddRow(`create table t (a bigint primary key, b varchar(100), c varchar(100), d int,
		key(a), key(c,d), key(d));`))

	cloudSDK, err := importsdk.NewImportSDK(context.Background(), sourceURI, db,
		importsdk.WithFileRouters([]*config.FileRouteRule{
			{Pattern: ".*", Table: "t", Schema: "cloud_csv", Type: "csv"},
		}))
	s.Require().NoError(err)
	defer cloudSDK.Close()
	s.Require().NoError(cloudSDK.CreateSchemasAndTables(context.Background()))
	tableMetas, err := cloudSDK.GetTableMetas(context.Background())
	s.Require().NoError(err)
	s.Len(tableMetas, 1)
	tableMeta := tableMetas[0]
	path := fmt.Sprintf("%s?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", tableMeta.WildcardPath, gcsEndpoint)
	importSQL := fmt.Sprintf("import into %s.%s from '%s' with cloud_storage_uri='%s'", tableMeta.Database, tableMeta.Table, path, sortStorageURI)
	result := s.tk.MustQuery(importSQL).Rows()
	s.Len(result, 1)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 foo1 bar1 123", "2 foo2 bar2 456", "3 foo3 bar3 789",
		"4 foo4 bar4 123", "5 foo5 bar5 223", "6 foo6 bar6 323",
	))
}

func (s *mockGCSSuite) TestDumplingSource() {
	// prepare source data
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "cloud_dumpling"})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_dumpling", Name: "cloud_dumpling1-schema-create.sql"},
		Content:     []byte("CREATE DATABASE IF NOT EXISTS cloud_dumpling1;\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_dumpling", Name: "cloud_dumpling2-schema-create.sql"},
		Content:     []byte("CREATE DATABASE IF NOT EXISTS cloud_dumpling2;\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_dumpling", Name: "cloud_dumpling1.tb1-schema.sql"},
		Content:     []byte("CREATE TABLE IF NOT EXISTS cloud_dumpling1.tb1 (a INT, b VARCHAR(10));\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_dumpling", Name: "cloud_dumpling1.tb1.001.sql"},
		Content:     []byte("INSERT INTO cloud_dumpling1.tb1 VALUES (1,'a'),(2,'b');\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_dumpling", Name: "cloud_dumpling1.tb1.002.sql"},
		Content:     []byte("INSERT INTO cloud_dumpling1.tb1 VALUES (3,'c'),(4,'d');\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_dumpling", Name: "cloud_dumpling2.tb2-schema.sql"},
		Content:     []byte("CREATE TABLE IF NOT EXISTS cloud_dumpling2.tb2 (x INT, y VARCHAR(10));\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_dumpling", Name: "cloud_dumpling2.tb2.001.sql"},
		Content:     []byte("INSERT INTO cloud_dumpling2.tb2 VALUES (5,'e'),(6,'f');\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "cloud_dumpling", Name: "cloud_dumpling2.tb2.002.sql"},
		Content:     []byte("INSERT INTO cloud_dumpling2.tb2 VALUES (7,'g'),(8,'h');\n"),
	})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	sourceURI := fmt.Sprintf("gs://cloud_dumpling?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
	sortStorageURI := fmt.Sprintf("gs://sorted/cloud_dumpling?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)

	db, mock, err := sqlmock.New()
	s.Require().NoError(err)
	defer db.Close()
	mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `cloud_dumpling1`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `cloud_dumpling2`;").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `cloud_dumpling1`.`tb1` (`a` INT,`b` VARCHAR(10));")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `cloud_dumpling2`.`tb2` (`x` INT,`y` VARCHAR(10));")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	cloudSDK, err := importsdk.NewImportSDK(context.Background(), sourceURI, db, importsdk.WithConcurrency(1))
	s.Require().NoError(err)
	defer cloudSDK.Close()

	s.Require().NoError(cloudSDK.CreateSchemasAndTables(context.Background()))
	tableMetas, err := cloudSDK.GetTableMetas(context.Background())
	s.Require().NoError(err)
	s.Len(tableMetas, 2)

	s.prepareAndUseDB("cloud_dumpling1")
	s.prepareAndUseDB("cloud_dumpling2")
	s.tk.MustExec("CREATE TABLE IF NOT EXISTS cloud_dumpling1.tb1 (a INT, b VARCHAR(10));")
	s.tk.MustExec("CREATE TABLE IF NOT EXISTS cloud_dumpling2.tb2 (x INT, y VARCHAR(10));")
	// import and validate data for each table
	for _, tm := range tableMetas {
		path := fmt.Sprintf("%s?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb",
			tm.WildcardPath, gcsEndpoint)
		importSQL := fmt.Sprintf("import into %s.%s from '%s' format 'sql' with cloud_storage_uri='%s'", tm.Database, tm.Table, path, sortStorageURI)
		result := s.tk.MustQuery(importSQL).Rows()
		s.Len(result, 1)
		// verify contents
		fullQuery := fmt.Sprintf("select * from %s.%s", tm.Database, tm.Table)
		switch tm.Table {
		case "tb1":
			s.tk.MustQuery(fullQuery).Sort().Check(testkit.Rows(
				"1 a", "2 b", "3 c", "4 d"))
		case "tb2":
			s.tk.MustQuery(fullQuery).Sort().Check(testkit.Rows(
				"5 e", "6 f", "7 g", "8 h"))
		}
	}

	s.Require().NoError(mock.ExpectationsWereMet())
}

func (s *mockGCSSuite) TestAutoDetectFileType() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "auto_detect"})

	files := []struct {
		name       string
		buf        func() []byte
		expectRows []string
	}{
		{name: "noext", buf: func() []byte { return []byte("1,foo\n2,bar\n") }, expectRows: []string{"1 foo", "2 bar"}},
		// SQL data but no suffix -> used for negative case
		{name: "sql_noext", buf: func() []byte { return []byte("INSERT INTO auto_detect.t VALUES (13,'m'),(14,'n');\n") }, expectRows: nil},
		// CSV data but with .sql suffix -> should be detected as SQL and trigger CSV-option errors
		{name: "csv_as_sql.sql", buf: func() []byte { return []byte("15,p\n16,q\n") }, expectRows: nil},
		{name: "f1.CSV", buf: func() []byte { return []byte("3,baz\n4,qux\n") }, expectRows: []string{"3 baz", "4 qux"}},
		{name: "data.sql", buf: func() []byte { return []byte("INSERT INTO auto_detect.t VALUES (5,'e'),(6,'f');\n") }, expectRows: []string{"5 e", "6 f"}},
		{name: "p.parquet", buf: func() []byte { return s.getParquetData() }, expectRows: []string{"1 one", "2 two"}},
		{name: "f2.csv.gz", buf: func() []byte { return s.getCompressedData(mydump.CompressionGZ, []byte("7,seven\n8,eight\n")) }, expectRows: []string{"7 seven", "8 eight"}},
		{name: "data.sql.zst", buf: func() []byte {
			return s.getCompressedData(mydump.CompressionZStd, []byte("INSERT INTO `auto_detect`.`t` VALUES (9,'i'),(10,'j');"))
		}, expectRows: []string{"10 j", "9 i"}},
		{name: "f3.csv.snappy", buf: func() []byte { return s.getCompressedData(mydump.CompressionSnappy, []byte("11,eleven\n12,twelve\n")) }, expectRows: []string{"11 eleven", "12 twelve"}},
	}

	for _, it := range files {
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "auto_detect", Name: it.name},
			Content:     it.buf(),
		})
	}

	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	sortStorageURI := fmt.Sprintf("gs://sorted/auto_detect?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)

	s.prepareAndUseDB("auto_detect")
	s.tk.MustExec("CREATE TABLE IF NOT EXISTS auto_detect.t (a INT, b VARCHAR(10));")

	for _, it := range files {
		if it.expectRows == nil {
			// for negative cases
			continue
		}
		path := fmt.Sprintf("gs://auto_detect/%s?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", it.name, gcsEndpoint)
		importSQL := fmt.Sprintf("import into auto_detect.t from '%s' with cloud_storage_uri='%s'", path, sortStorageURI)
		res := s.tk.MustQuery(importSQL).Rows()
		s.Len(res, 1)
		s.tk.MustQuery("select * from auto_detect.t").Sort().Check(testkit.Rows(it.expectRows...))
		s.tk.MustExec("TRUNCATE TABLE auto_detect.t;")
	}

	// negative cases: run a set of failing imports and assert error messages
	negativeCases := []struct {
		objectName string
		options    string
		wantSubstr string
	}{
		// CSV-only option applied to SQL file
		{objectName: "data.sql", options: "fields_enclosed_by='\"'", wantSubstr: "Unsupported option fields_enclosed_by for non-CSV"},
		// SQL data present but no suffix
		{objectName: "sql_noext", options: "", wantSubstr: "encode kv erro"},
		// CSV data but filename ends with .sql
		{objectName: "csv_as_sql.sql", options: "", wantSubstr: "encode kv erro"},
	}

	for _, nc := range negativeCases {
		s.tk.MustExec("CREATE TABLE IF NOT EXISTS auto_detect.t (a INT, b VARCHAR(10));")
		path := fmt.Sprintf("gs://auto_detect/%s?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", nc.objectName, gcsEndpoint)
		var badImportSQL string
		if nc.options == "" {
			badImportSQL = fmt.Sprintf("import into auto_detect.t from '%s' with cloud_storage_uri='%s'", path, sortStorageURI)
		} else {
			badImportSQL = fmt.Sprintf("import into auto_detect.t from '%s' with %s, cloud_storage_uri='%s'", path, nc.options, sortStorageURI)
		}
		err := s.tk.QueryToErr(badImportSQL)
		s.Require().ErrorContains(err, nc.wantSubstr)
		s.tk.MustExec("DROP TABLE auto_detect.t;")
	}
}

func (s *mockGCSSuite) getCompressedData(compression mydump.Compression, data []byte) []byte {
	var buf bytes.Buffer
	var w io.WriteCloser
	switch compression {
	case mydump.CompressionGZ:
		w = gzip.NewWriter(&buf)
	case mydump.CompressionZStd:
		var err error
		w, err = zstd.NewWriter(&buf)
		s.NoError(err)
	case mydump.CompressionSnappy:
		w = snappy.NewBufferedWriter(&buf)
	default:
		s.FailNow("unknown compression type", compression)
	}
	_, err := w.Write(data)
	s.NoError(err)
	s.NoError(w.Close())
	compressedData := buf.Bytes()
	s.NotEqual(data, compressedData)
	return compressedData
}

func (s *mockGCSSuite) getParquetData() []byte {
	type ParquetRow struct {
		A int32  `parquet:"name=a, type=INT32"`
		B string `parquet:"name=b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	}
	// create temp file inside the test's TempDir (managed by Go test)
	dir := s.T().TempDir()
	tmpPath := filepath.Join(dir, "test.parquet")

	fw, err := local.NewLocalFileWriter(tmpPath)
	s.Require().NoError(err)
	pw, err := writer.NewParquetWriter(fw, new(ParquetRow), 4)
	s.Require().NoError(err)

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	rows := []ParquetRow{{A: 1, B: "one"}, {A: 2, B: "two"}}
	for _, r := range rows {
		if err := pw.Write(r); err != nil {
			_ = pw.WriteStop()
			_ = fw.Close()
			s.Require().NoError(err)
		}
	}

	s.Require().NoError(pw.WriteStop())
	s.Require().NoError(fw.Close())

	data, err := os.ReadFile(tmpPath)
	s.Require().NoError(err)
	s.NotEmpty(data)
	return data
}
