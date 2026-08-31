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
