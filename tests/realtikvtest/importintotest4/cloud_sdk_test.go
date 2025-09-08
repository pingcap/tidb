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

package importintotest

import (
	"context"
	"fmt"
	"regexp"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/testkit"
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
