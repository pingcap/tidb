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
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/mock/mocklocal"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	pdhttp "github.com/tikv/pd/client/http"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func (s *mockGCSSuite) prepareAndUseDB(db string) {
	s.tk.MustExec("drop database if exists " + db)
	s.tk.MustExec("create database " + db)
	s.tk.MustExec("use " + db)
}

// NOTE: for negative cases, see TestImportIntoPrivilegeNegativeCase in privileges_test.go
func (s *mockGCSSuite) TestImportIntoPrivilegePositiveCase() {
	content := []byte("1,test1,11\n2,test2,22")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "privilege-test",
			Name:       "db.tbl.001.csv",
		},
		Content: content,
	})
	tempDir := s.T().TempDir()
	filePath := path.Join(tempDir, "file.csv")
	s.NoError(os.WriteFile(filePath, content, 0644))
	s.prepareAndUseDB("import_into")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int);")
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	s.tk.MustExec(`DROP USER IF EXISTS 'test_import_into'@'localhost';`)
	s.tk.MustExec(`CREATE USER 'test_import_into'@'localhost';`)
	s.tk.MustExec(`GRANT SELECT on import_into.t to 'test_import_into'@'localhost'`)
	s.tk.MustExec(`GRANT UPDATE on import_into.t to 'test_import_into'@'localhost'`)
	s.tk.MustExec(`GRANT INSERT on import_into.t to 'test_import_into'@'localhost'`)
	s.tk.MustExec(`GRANT DELETE on import_into.t to 'test_import_into'@'localhost'`)
	s.tk.MustExec(`GRANT ALTER on import_into.t to 'test_import_into'@'localhost'`)
	s.T().Cleanup(func() {
		_ = s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil)
	})
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_import_into", Hostname: "localhost"}, nil, nil, nil))
	// enough for gs storage
	sql := fmt.Sprintf(`import into t FROM 'gs://privilege-test/db.tbl.*.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1 test1 11", "2 test2 22"))
	// works even SEM enabled
	sem.Enable()
	s.T().Cleanup(func() {
		sem.Disable()
	})
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	s.tk.MustExec("truncate table t")
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_import_into", Hostname: "localhost"}, nil, nil, nil))
	s.tk.MustQuery(sql)
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1 test1 11", "2 test2 22"))

	sem.Disable()
	// requires FILE for server file
	importFromServerSQL := fmt.Sprintf("IMPORT INTO t FROM '%s'", filePath)
	// NOTE: we must use ExecToErr instead of QueryToErr here, because QueryToErr will cause the case fail always.
	s.True(terror.ErrorEqual(s.tk.ExecToErr(importFromServerSQL), plannererrors.ErrSpecificAccessDenied))

	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	s.tk.MustExec(`GRANT FILE on *.* to 'test_import_into'@'localhost'`)
	s.tk.MustExec("truncate table t")
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_import_into", Hostname: "localhost"}, nil, nil, nil))
	s.tk.MustQuery(importFromServerSQL)
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1 test1 11", "2 test2 22"))
}

func (s *mockGCSSuite) TestBasicImportInto() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.001.csv",
		},
		Content: []byte("1,test1,11\n" +
			"2,test2,22"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.002.csv",
		},
		Content: []byte("3,test3,33\n" +
			"4,test4,44"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.003.csv",
		},
		Content: []byte("5,test5,55\n" +
			"6,test6,66"),
	})
	s.prepareAndUseDB("import_into")

	allData := []string{"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44", "5 test5 55", "6 test6 66"}
	cases := []struct {
		createTableSQL string
		flags          string
		res            []string
		querySQL       string
		lastInsertID   uint64
	}{
		{"create table t (a bigint, b varchar(100), c int);", "", allData, "", 0},
		{"create table t (a bigint primary key, b varchar(100), c int);", "", allData, "", 0},
		{"create table t (a bigint primary key, b varchar(100), c int, key(b, a));", "", allData, "", 0},
		{"create table t (a bigint auto_increment primary key, b varchar(100), c int);", "", allData, "", 0},
		{"create table t (a bigint auto_random primary key, b varchar(100), c int);", "", allData, "", 0},
		{"create table t (a bigint, b varchar(100), c int, primary key(b,c));", "", allData, "", 0},

		{
			createTableSQL: "create table t (a bigint, b varchar(100), c int);",
			flags:          "(c, b, a)",
			res:            []string{"11 test1 1", "22 test2 2", "33 test3 3", "44 test4 4", "55 test5 5", "66 test6 6"},
		},
		{
			createTableSQL: "create table t (a bigint, b varchar(100), c int, d varchar(100));",
			flags:          "(c, d, a)",
			res:            []string{"11 <nil> 1 test1", "22 <nil> 2 test2", "33 <nil> 3 test3", "44 <nil> 4 test4", "55 <nil> 5 test5", "66 <nil> 6 test6"},
		},
		{
			createTableSQL: "create table t (a bigint auto_increment primary key, b varchar(100), c int, d varchar(100));",
			flags:          "(@1, @2, a) set c = @1+100, d=@2, b = concat(@2, '-aa')",
			res:            []string{"11 test1-aa 101 test1", "22 test2-aa 102 test2", "33 test3-aa 103 test3", "44 test4-aa 104 test4", "55 test5-aa 105 test5", "66 test6-aa 106 test6"},
		},
		// SHARD_ROW_ID_BITS
		{
			createTableSQL: "create table t (a bigint, b varchar(100), c int, d varchar(100)) SHARD_ROW_ID_BITS 10;",
			flags:          "(@1, @2, a) set c = @1+100, d=@2, b = concat(@2, '-aa')",
			res:            []string{"11 test1-aa 101 test1", "22 test2-aa 102 test2", "33 test3-aa 103 test3", "44 test4-aa 104 test4", "55 test5-aa 105 test5", "66 test6-aa 106 test6"},
			querySQL:       "select * from t order by a",
		},
		// default value for auto_increment
		{
			createTableSQL: "create table t (a bigint auto_increment primary key, b int, c varchar(100), d int);",
			flags:          "(b, c, d)",
			// row id is calculated by us, it's not continuous
			res:          []string{"1 1 test1 11", "2 2 test2 22", "6 3 test3 33", "7 4 test4 44", "11 5 test5 55", "12 6 test6 66"},
			lastInsertID: 1,
		},
		// default value for auto_random
		{
			createTableSQL: "create table t (a bigint auto_random primary key, b int, c varchar(100), d int);",
			flags:          "(b, c, d)",
			res: []string{
				"288230376151711745 1 test1 11",
				"288230376151711746 2 test2 22",
				"6 3 test3 33",
				"7 4 test4 44",
				"864691128455135243 5 test5 55",
				"864691128455135244 6 test6 66",
			},
			// auto_random id contains shard bit.
			querySQL:     "select * from t order by b",
			lastInsertID: 6,
		},
	}

	loadDataSQL := fmt.Sprintf(`import into t %%s FROM 'gs://test-multi-load/db.tbl.*.csv?endpoint=%s'
		with thread=1`, gcsEndpoint)
	for _, c := range cases {
		s.tk.MustExec("drop table if exists t;")
		s.tk.MustExec(c.createTableSQL)
		sql := fmt.Sprintf(loadDataSQL, c.flags)
		s.tk.MustQuery(sql)
		// todo: open it after we support it.
		//s.Equal("Records: 6  Deleted: 0  Skipped: 0  Warnings: 0", s.tk.Session().GetSessionVars().StmtCtx.GetMessage())
		//s.Equal(uint64(6), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
		querySQL := "SELECT * FROM t;"
		if c.querySQL != "" {
			querySQL = c.querySQL
		}
		s.tk.MustQuery(querySQL).Check(testkit.Rows(c.res...))
	}
}

func (s *mockGCSSuite) TestInputNull() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "nil-input.tsv",
		},
		Content: []byte("1\t\\N\t11\n" +
			"2\ttest2\t22"),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	// nil input
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int);")
	loadDataSQL := fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/nil-input.tsv?endpoint=%s'
		WITH fields_terminated_by='\t'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 <nil> 11", "2 test2 22"}...))
	// set to default on nil
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t (a,@1,c) set b=COALESCE(@1, 'def')
		FROM 'gs://test-multi-load/nil-input.tsv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 def 11", "2 test2 22"}...))
}

func (s *mockGCSSuite) TestIgnoreNLines() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-multi-load", Name: "skip-rows-1.csv"},
		Content: []byte(`1,test1,11
2,test2,22
3,test3,33
4,test4,44`),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-multi-load", Name: "skip-rows-2.csv"},
		Content: []byte(`5,test5,55
6,test6,66
7,test7,77
8,test8,88
9,test9,99`),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int);")
	loadDataSQL := fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/skip-rows-*.csv?endpoint=%s'
		with thread=1`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44",
		"5 test5 55", "6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/skip-rows-*.csv?endpoint=%s'
		with thread=1, skip_rows=1`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"2 test2 22", "3 test3 33", "4 test4 44",
		"6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/skip-rows-*.csv?endpoint=%s'
		with thread=1, skip_rows=3`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"4 test4 44",
		"8 test8 88", "9 test9 99",
	}...))
}

func (s *mockGCSSuite) TestGeneratedColumnsAndTSVFile() {
	// For issue https://github.com/pingcap/tidb/issues/39885
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("USE load_csv;")
	s.tk.MustExec("set @@sql_mode = ''")
	s.tk.MustExec(`CREATE TABLE load_csv.t_gen1 (a int, b int generated ALWAYS AS (a+1));`)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-bucket",
			Name:       "generated_columns.csv",
		},
		Content: []byte("1\t2\n2\t3"),
	})

	s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO load_csv.t_gen1
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))
	s.tk.MustExec("delete from t_gen1")

	// Specify the column, this should also work.
	s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO load_csv.t_gen1(a)
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))
	s.tk.MustExec("delete from t_gen1")
	s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO load_csv.t_gen1(a,@1)
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))

	// Swap the column and test again.
	s.tk.MustExec(`create table t_gen2 (a int generated ALWAYS AS (b+1), b int);`)
	s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO load_csv.t_gen2
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("3 2", "4 3"))
	s.tk.MustExec(`delete from t_gen2`)

	// Specify the column b
	s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO load_csv.t_gen2(b)
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("2 1", "3 2"))
	s.tk.MustExec(`delete from t_gen2`)

	// Specify the column a
	s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO load_csv.t_gen2(a)
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("<nil> <nil>", "<nil> <nil>"))
}

func (s *mockGCSSuite) TestInputCountMisMatchAndDefault() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "input-cnt-mismatch.csv",
		},
		Content: []byte("1,test1\n" +
			"2,test2,22,extra"),
	})
	s.prepareAndUseDB("load_data")
	// mapped column of the missed field default to null
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int);")
	loadDataSQL := fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/input-cnt-mismatch.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 test1 <nil>", "2 test2 22"}...))
	// mapped column of the missed field has non-null default
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int default 100);")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/input-cnt-mismatch.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	// we convert it to null all the time
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 test1 <nil>", "2 test2 22"}...))
	// mapped column of the missed field set to a variable
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int);")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t(a,b,@1) set c=COALESCE(@1, 100)
		FROM 'gs://test-multi-load/input-cnt-mismatch.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 test1 100", "2 test2 22"}...))
}

func (s *mockGCSSuite) TestDeliverBytesRows() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "min-deliver-bytes-rows.csv",
		},
		Content: []byte(`1,test1,11
2,test2,22
3,test3,33
4,test4,44
5,test5,55
6,test6,66
7,test7,77
8,test8,88
9,test9,99`),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int);")
	bak := importer.MinDeliverBytes
	importer.MinDeliverBytes = 10
	loadDataSQL := fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/min-deliver-bytes-rows.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44",
		"5 test5 55", "6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	importer.MinDeliverBytes = bak

	s.tk.MustExec("truncate table t")
	bakCnt := importer.MinDeliverRowCnt
	importer.MinDeliverRowCnt = 2
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/min-deliver-bytes-rows.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44",
		"5 test5 55", "6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	importer.MinDeliverRowCnt = bakCnt
}

func (s *mockGCSSuite) TestMultiValueIndex() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec(`CREATE TABLE load_csv.t (
		i INT, j JSON,
		KEY idx ((cast(j as signed array)))
		);`)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load-csv",
			Name:       "1.csv",
		},
		Content: []byte(`i,s
1,"[1,2,3]"
2,"[2,3,4]"`),
	})

	sql := fmt.Sprintf(`IMPORT INTO load_csv.t FROM 'gs://test-load-csv/1.csv?endpoint=%s'
		WITH skip_rows=1;`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"1 [1, 2, 3]",
		"2 [2, 3, 4]",
	))
}

func (s *mockGCSSuite) TestLoadSQLDump() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (id INT, c VARCHAR(20));")

	content := `insert into tbl values (1, 'a'), (2, 'b');`
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load-parquet", Name: "p"},
		Content:     []byte(content),
	})
	tempDir := s.T().TempDir()
	s.NoError(os.WriteFile(path.Join(tempDir, "test.sql"), []byte(content), 0o644))

	sql := fmt.Sprintf(`IMPORT INTO load_csv.t FROM 'gs://test-load-parquet/p?endpoint=%s' FORMAT 'SQL';`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows("1 a", "2 b"))

	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")
	sql = fmt.Sprintf(`IMPORT INTO load_csv.t FROM '%s' FORMAT 'SQL';`, path.Join(tempDir, "test.sql"))
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows("1 a", "2 b"))
}

func (s *mockGCSSuite) TestGBK() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_charset;")
	s.tk.MustExec("CREATE DATABASE load_charset;")
	s.tk.MustExec(`CREATE TABLE load_charset.gbk (
		i INT, j VARCHAR(255)
		) CHARACTER SET gbk;`)
	s.tk.MustExec(`CREATE TABLE load_charset.utf8mb4 (
		i INT, j VARCHAR(255)
		) CHARACTER SET utf8mb4;`)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "gbk.csv",
		},
		Content: []byte{
			// 1,一丁丂七丄丅丆万丈三上下丌不与丏
			0x31, 0x2c, 0xd2, 0xbb, 0xb6, 0xa1, 0x81, 0x40, 0xc6, 0xdf, 0x81,
			0x41, 0x81, 0x42, 0x81, 0x43, 0xcd, 0xf2, 0xd5, 0xc9, 0xc8, 0xfd,
			0xc9, 0xcf, 0xcf, 0xc2, 0xd8, 0xa2, 0xb2, 0xbb, 0xd3, 0xeb, 0x81,
			0x44, 0x0a,
			// 2,丐丑丒专且丕世丗丘丙业丛东丝丞丢
			0x32, 0x2c, 0xd8, 0xa4, 0xb3, 0xf3, 0x81, 0x45, 0xd7, 0xa8, 0xc7,
			0xd2, 0xd8, 0xa7, 0xca, 0xc0, 0x81, 0x46, 0xc7, 0xf0, 0xb1, 0xfb,
			0xd2, 0xb5, 0xb4, 0xd4, 0xb6, 0xab, 0xcb, 0xbf, 0xd8, 0xa9, 0xb6,
			0xaa,
		},
	})

	sql := fmt.Sprintf(`IMPORT INTO load_charset.gbk FROM 'gs://test-load/gbk.csv?endpoint=%s'
		WITH character_set='gbk'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.gbk;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))
	sql = fmt.Sprintf(`IMPORT INTO load_charset.utf8mb4 FROM 'gs://test-load/gbk.csv?endpoint=%s'
		WITH character_set='gbk'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8mb4;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "utf8mb4.csv",
		},
		Content: []byte("1,一丁丂七丄丅丆万丈三上下丌不与丏\n" +
			"2,丐丑丒专且丕世丗丘丙业丛东丝丞丢"),
	})

	s.tk.MustExec("TRUNCATE TABLE load_charset.gbk;")
	sql = fmt.Sprintf(`IMPORT INTO load_charset.gbk FROM 'gs://test-load/utf8mb4.csv?endpoint=%s'
		WITH character_set='utf8mb4'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.gbk;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "emoji.csv",
		},
		Content: []byte("1,一丁丂七😀😁😂😃\n" +
			"2,丐丑丒专😄😅😆😇"),
	})

	s.tk.MustExec("TRUNCATE TABLE load_charset.gbk;")
	sql = fmt.Sprintf(`IMPORT INTO load_charset.gbk FROM 'gs://test-load/emoji.csv?endpoint=%s'
		WITH character_set='utf8mb4'`, gcsEndpoint)
	err := s.tk.QueryToErr(sql)
	// FIXME: handle error
	s.ErrorContains(err, `Incorrect string value '\xF0\x9F\x98\x80' for column 'j'`)
}

func (s *mockGCSSuite) TestOtherCharset() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_charset;")
	s.tk.MustExec("CREATE DATABASE load_charset;")
	s.tk.MustExec(`CREATE TABLE load_charset.utf8 (
		i INT, j VARCHAR(255)
		) CHARACTER SET utf8;`)
	s.tk.MustExec(`CREATE TABLE load_charset.utf8mb4 (
		i INT, j VARCHAR(255)
		) CHARACTER SET utf8mb4;`)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "utf8.tsv",
		},
		Content: []byte("1,ကခဂဃ\n2,ငစဆဇ"),
	})

	sql := fmt.Sprintf(`IMPORT INTO load_charset.utf8 FROM 'gs://test-load/utf8.tsv?endpoint=%s'
		 WITH character_set='utf8'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8;").Check(testkit.Rows(
		"1 ကခဂဃ",
		"2 ငစဆဇ",
	))
	sql = fmt.Sprintf(`IMPORT INTO load_charset.utf8mb4 FROM 'gs://test-load/utf8.tsv?endpoint=%s'
		 WITH character_set='utf8'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8mb4;").Check(testkit.Rows(
		"1 ကခဂဃ",
		"2 ငစဆဇ",
	))

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "latin1.tsv",
		},
		// "1\t‘’“”\n2\t¡¢£¤"
		Content: []byte{0x31, 0x2c, 0x91, 0x92, 0x93, 0x94, 0x0a, 0x32, 0x2c, 0xa1, 0xa2, 0xa3, 0xa4},
	})
	s.tk.MustExec(`CREATE TABLE load_charset.latin1 (
		i INT, j VARCHAR(255)
		) CHARACTER SET latin1;`)
	sql = fmt.Sprintf(`IMPORT INTO load_charset.latin1 FROM 'gs://test-load/latin1.tsv?endpoint=%s'
		 WITH character_set='latin1'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.latin1;").Check(testkit.Rows(
		"1 ‘’“”",
		"2 ¡¢£¤",
	))

	s.tk.MustExec("TRUNCATE TABLE load_charset.utf8mb4;")
	sql = fmt.Sprintf(`IMPORT INTO load_charset.utf8mb4 FROM 'gs://test-load/latin1.tsv?endpoint=%s'
		 WITH character_set='latin1'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8mb4;").Check(testkit.Rows(
		"1 ‘’“”",
		"2 ¡¢£¤",
	))

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "ascii.tsv",
		},
		Content: []byte{0, 1, 2, 3, 4, 5, 6, 7},
	})
	s.tk.MustExec(`CREATE TABLE load_charset.ascii (
		j VARCHAR(255)
		) CHARACTER SET ascii;`)
	s.tk.MustExec(`CREATE TABLE load_charset.binary (
		j VARCHAR(255)
		) CHARACTER SET binary;`)
	sql = fmt.Sprintf(`IMPORT INTO load_charset.ascii FROM 'gs://test-load/ascii.tsv?endpoint=%s'
		 WITH character_set='ascii'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT HEX(j) FROM load_charset.ascii;").Check(testkit.Rows(
		"0001020304050607",
	))
	sql = fmt.Sprintf(`IMPORT INTO load_charset.binary FROM 'gs://test-load/ascii.tsv?endpoint=%s'
		 WITH character_set='binary'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT HEX(j) FROM load_charset.binary;").Check(testkit.Rows(
		"0001020304050607",
	))
}

func (s *mockGCSSuite) TestMaxWriteSpeed() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_test_write_speed;")
	s.tk.MustExec("CREATE DATABASE load_test_write_speed;")
	s.tk.MustExec(`CREATE TABLE load_test_write_speed.t(a int, b int)`)

	lineCount := 1000
	data := make([]byte, 0, 1<<13)
	for i := 0; i < lineCount; i++ {
		data = append(data, []byte(fmt.Sprintf("%d,%d\n", i, i))...)
	}

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "speed-test.csv",
		},
		Content: data,
	})

	// without speed limit
	start := time.Now()
	sql := fmt.Sprintf(`IMPORT INTO load_test_write_speed.t FROM 'gs://test-load/speed-test.csv?endpoint=%s'`,
		gcsEndpoint)
	result := s.tk.MustQuery(sql)
	fileSize := result.Rows()[0][6].(string)
	s.Equal("7.598KiB", fileSize)
	duration := time.Since(start).Seconds()
	s.tk.MustQuery("SELECT count(1) FROM load_test_write_speed.t;").Check(testkit.Rows(
		strconv.Itoa(lineCount),
	))

	// the encoded KV size is about 34744 bytes, so it would take about 5 more seconds to write all data.
	// with speed limit
	s.tk.MustExec("TRUNCATE TABLE load_test_write_speed.t;")
	start = time.Now()
	sql = fmt.Sprintf(`IMPORT INTO load_test_write_speed.t FROM 'gs://test-load/speed-test.csv?endpoint=%s'
		with max_write_speed='6000'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	durationWithLimit := time.Since(start).Seconds()
	s.tk.MustQuery("SELECT count(1) FROM load_test_write_speed.t;").Check(testkit.Rows(
		strconv.Itoa(lineCount),
	))
	// previous import might be slower depends on the environment, so we check using 4 seconds here.
	// might be unstable.
	require.Less(s.T(), duration+4, durationWithLimit)
}

func (s *mockGCSSuite) TestChecksumNotMatch() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "duplicate-pk-01.csv",
		},
		Content: []byte(`1,test1,11
2,test2,22
2,test3,33`),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "duplicate-pk-02.csv",
		},
		Content: []byte(`4,test4,44
4,test5,55
6,test6,66`),
	})

	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), c int);")
	loadDataSQL := fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/duplicate-pk-*.csv?endpoint=%s'
		with thread=1, __max_engine_size='1'`, gcsEndpoint)
	err := s.tk.QueryToErr(loadDataSQL)
	require.ErrorContains(s.T(), err, "ErrChecksumMismatch")
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "4 test4 44", "6 test6 66",
	}...))

	s.tk.MustExec("truncate table t;")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/duplicate-pk-*.csv?endpoint=%s'
		with thread=1, checksum_table='off', __max_engine_size='1'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "4 test4 44", "6 test6 66",
	}...))

	s.tk.MustExec("truncate table t;")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/duplicate-pk-*.csv?endpoint=%s'
		with thread=1, checksum_table='optional', __max_engine_size='1'`, gcsEndpoint)
	s.tk.MustQuery(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "4 test4 44", "6 test6 66",
	}...))
}

func (s *mockGCSSuite) TestColumnsAndUserVars() {
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/storage/testSetLastTaskID", "return(true)")
	s.tk.MustExec("DROP DATABASE IF EXISTS load_data;")
	s.tk.MustExec("CREATE DATABASE load_data;")
	s.tk.MustExec(`CREATE TABLE load_data.cols_and_vars (a INT, b INT, c int);`)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "cols_and_vars-1.tsv"},
		Content:     []byte("1,11,111\n2,22,222\n3,33,333\n4,44,444\n5,55,555\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "cols_and_vars-2.tsv"},
		Content:     []byte("6,66,666\n7,77,777\n8,88,888\n9,99,999\n"),
	})
	sql := fmt.Sprintf(`IMPORT INTO load_data.cols_and_vars (@V1, @v2, @v3) set a=@V1, b=@V2*10, c=123
		FROM 'gs://test-load/cols_and_vars-*.tsv?endpoint=%s' WITH thread=2`, gcsEndpoint)
	rows := s.tk.MustQuery(sql).Rows()
	s.Len(rows, 1)
	jobID1, err := strconv.Atoi(rows[0][0].(string))
	s.NoError(err)
	jobInfo, err := importer.GetJob(context.Background(), s.tk.Session(), int64(jobID1), "", true)
	s.NoError(err)
	s.Equal("(@`V1`, @`v2`, @`v3`)", jobInfo.Parameters.ColumnsAndVars)
	s.Equal("`a`=@`V1`, `b`=@`V2`*10, `c`=123", jobInfo.Parameters.SetClause)
	s.tk.MustQuery("SELECT * FROM load_data.cols_and_vars;").Sort().Check(testkit.Rows(
		"1 110 123",
		"2 220 123",
		"3 330 123",
		"4 440 123",
		"5 550 123",
		"6 660 123",
		"7 770 123",
		"8 880 123",
		"9 990 123",
	))
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return s.tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	taskManager := storage.NewTaskManager(pool)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	subtasks, err := taskManager.GetSubtasksWithHistory(ctx, storage.TestLastTaskID.Load(), proto.ImportStepImport)
	s.NoError(err)
	s.Len(subtasks, 1)
	serverInfo, err := infosync.GetServerInfo()
	s.NoError(err)
	for _, st := range subtasks {
		s.Equal(net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port))), st.ExecID)
	}
}

func (s *mockGCSSuite) checkTaskMetaRedacted(jobID int64) {
	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	taskKey := importinto.TaskKey(jobID)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	task, err2 := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	s.NoError(err2)
	s.Regexp(`[?&]access-key=xxxxxx`, string(task.Meta))
	s.Contains(string(task.Meta), "secret-access-key=xxxxxx")
	s.NotContains(string(task.Meta), "aaaaaa")
	s.NotContains(string(task.Meta), "bbbbbb")
}

func (s *mockGCSSuite) TestImportMode() {
	var intoImportTime, intoNormalTime time.Time
	controller := gomock.NewController(s.T())
	switcher := mocklocal.NewMockTiKVModeSwitcher(controller)
	toImportModeFn := func(ctx context.Context, ranges ...*import_sstpb.Range) error {
		log.L().Info("ToImportMode")
		intoImportTime = time.Now()
		return nil
	}
	toNormalModeFn := func(ctx context.Context, ranges ...*import_sstpb.Range) error {
		log.L().Info("ToNormalMode")
		intoNormalTime = time.Now()
		return nil
	}
	switcher.EXPECT().ToImportMode(gomock.Any(), gomock.Any()).DoAndReturn(toImportModeFn).Times(1)
	switcher.EXPECT().ToNormalMode(gomock.Any(), gomock.Any()).DoAndReturn(toNormalModeFn).Times(1)
	backup := importer.NewTiKVModeSwitcher
	importer.NewTiKVModeSwitcher = func(*tls.Config, pdhttp.Client, *zap.Logger) local.TiKVModeSwitcher {
		return switcher
	}
	s.T().Cleanup(func() {
		importer.NewTiKVModeSwitcher = backup
	})

	s.tk.MustExec("DROP DATABASE IF EXISTS load_data;")
	s.tk.MustExec("CREATE DATABASE load_data;")
	s.tk.MustExec(`CREATE TABLE load_data.import_mode (a INT, b INT, c int);`)
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "import_mode-1.tsv"},
		Content:     []byte("1,11,111"),
	})

	// NOTE: this case only runs when current instance is TiDB owner, if you run it locally,
	// better start a cluster without TiDB instance.
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/parser/ast/forceRedactURL", "return(true)")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", "return()")
	sql := fmt.Sprintf(`IMPORT INTO load_data.import_mode FROM 'gs://test-load/import_mode-*.tsv?access-key=aaaaaa&secret-access-key=bbbbbb&endpoint=%s'`, gcsEndpoint)
	rows := s.tk.MustQuery(sql).Rows()
	s.Len(rows, 1)
	jobID, err := strconv.Atoi(rows[0][0].(string))
	s.NoError(err)
	s.tk.MustQuery("SELECT * FROM load_data.import_mode;").Sort().Check(testkit.Rows("1 11 111"))
	s.Greater(intoNormalTime, intoImportTime)
	<-scheduler.WaitCleanUpFinished
	s.checkTaskMetaRedacted(int64(jobID))
	// after import step, we should enter normal mode, i.e. we only call ToImportMode once
	intoNormalTime, intoImportTime = time.Time{}, time.Time{}
	switcher.EXPECT().ToImportMode(gomock.Any(), gomock.Any()).DoAndReturn(toImportModeFn).Times(1)
	switcher.EXPECT().ToNormalMode(gomock.Any(), gomock.Any()).DoAndReturn(toNormalModeFn).Times(1)
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/clearLastSwitchTime", "return(true)")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/waitBeforePostProcess", "return(true)")
	s.tk.MustExec("truncate table load_data.import_mode;")
	sql = fmt.Sprintf(`IMPORT INTO load_data.import_mode FROM 'gs://test-load/import_mode-*.tsv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_data.import_mode;").Sort().Check(testkit.Rows("1 11 111"))
	s.tk.MustExec("truncate table load_data.import_mode;")
	s.Greater(intoNormalTime, intoImportTime)
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/clearLastSwitchTime"))
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/waitBeforePostProcess"))
	<-scheduler.WaitCleanUpFinished

	// test disable_tikv_import_mode, should not call ToImportMode and ToNormalMode
	s.tk.MustExec("truncate table load_data.import_mode;")
	sql = fmt.Sprintf(`IMPORT INTO load_data.import_mode FROM 'gs://test-load/import_mode-*.tsv?endpoint=%s' WITH disable_tikv_import_mode`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_data.import_mode;").Sort().Check(testkit.Rows("1 11 111"))
	s.tk.MustExec("truncate table load_data.import_mode;")
	<-scheduler.WaitCleanUpFinished

	// test with multirocksdb
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/ddl/util/IsRaftKv2", "return(true)")
	s.tk.MustExec("truncate table load_data.import_mode;")
	sql = fmt.Sprintf(`IMPORT INTO load_data.import_mode FROM 'gs://test-load/import_mode-*.tsv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_data.import_mode;").Sort().Check(testkit.Rows("1 11 111"))
	s.tk.MustExec("truncate table load_data.import_mode;")
	<-scheduler.WaitCleanUpFinished

	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/util/IsRaftKv2"))

	// to normal mode should be called on error
	intoNormalTime, intoImportTime = time.Time{}, time.Time{}
	switcher.EXPECT().ToImportMode(gomock.Any(), gomock.Any()).DoAndReturn(toImportModeFn).Times(1)
	switcher.EXPECT().ToNormalMode(gomock.Any(), gomock.Any()).DoAndReturn(toNormalModeFn).Times(1)
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/waitBeforeSortChunk", "return(true)")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/errorWhenSortChunk", "return(true)")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/executor/importer/setLastImportJobID", `return(true)`)

	sql = fmt.Sprintf(`IMPORT INTO load_data.import_mode FROM 'gs://test-load/import_mode-*.tsv?access-key=aaaaaa&secret-access-key=bbbbbb&endpoint=%s'`, gcsEndpoint)
	err = s.tk.QueryToErr(sql)
	s.Error(err)
	s.Greater(intoNormalTime, intoImportTime)
	<-scheduler.WaitCleanUpFinished
	s.checkTaskMetaRedacted(importer.TestLastImportJobID.Load())
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished"))
}

func (s *mockGCSSuite) TestRegisterTask() {
	var registerTime, unregisterTime time.Time
	var taskID atomic.String
	controller := gomock.NewController(s.T())
	taskRegister := mock.NewMockTaskRegister(controller)
	mockedRegister := func(ctx context.Context) error {
		log.L().Info("register task", zap.String("task_id", taskID.Load()))
		registerTime = time.Now()
		return nil
	}
	mockedClose := func(ctx context.Context) error {
		log.L().Info("unregister task", zap.String("task_id", taskID.Load()))
		unregisterTime = time.Now()
		return nil
	}
	taskRegister.EXPECT().RegisterTaskOnce(gomock.Any()).DoAndReturn(mockedRegister).Times(1)
	taskRegister.EXPECT().Close(gomock.Any()).DoAndReturn(mockedClose).Times(1)
	backup := importinto.NewTaskRegisterWithTTL
	importinto.NewTaskRegisterWithTTL = func(_ *clientv3.Client, _ time.Duration, _ utils.RegisterTaskType, name string) utils.TaskRegister {
		// we use taskID as the task name
		taskID.Store(name)
		return taskRegister
	}
	s.T().Cleanup(func() {
		importinto.NewTaskRegisterWithTTL = backup
	})

	s.tk.MustExec("DROP DATABASE IF EXISTS load_data;")
	s.tk.MustExec("CREATE DATABASE load_data;")
	s.tk.MustExec(`CREATE TABLE load_data.register_task (a INT, b INT, c int);`)
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "register_task-1.tsv"},
		Content:     []byte("1,11,111"),
	})

	// NOTE: this case only runs when current instance is TiDB owner, if you run it locally,
	// better start a cluster without TiDB instance.
	sql := fmt.Sprintf(`IMPORT INTO load_data.register_task FROM 'gs://test-load/register_task-*.tsv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_data.register_task;").Sort().Check(testkit.Rows("1 11 111"))
	s.Greater(unregisterTime, registerTime)

	// on error, we should also unregister the task
	registerTime, unregisterTime = time.Time{}, time.Time{}
	taskRegister.EXPECT().RegisterTaskOnce(gomock.Any()).DoAndReturn(mockedRegister).Times(1)
	taskRegister.EXPECT().Close(gomock.Any()).DoAndReturn(mockedClose).Times(1)
	s.tk.MustExec("truncate table load_data.register_task;")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/waitBeforeSortChunk", "return(true)")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/errorWhenSortChunk", "return(true)")
	err := s.tk.QueryToErr(sql)
	s.Error(err)
	s.Greater(unregisterTime, registerTime)

	client, err := importer.GetEtcdClient()
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = client.Close()
	})
	var etcdKey string
	importinto.NewTaskRegisterWithTTL = backup
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/waitBeforeSortChunk"))
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/errorWhenSortChunk"))
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/syncBeforeSortChunk",
		func() {
			// cannot run 2 import job to the same target table.
			tk2 := testkit.NewTestKit(s.T(), s.store)
			err = tk2.QueryToErr(sql)
			s.ErrorIs(err, exeerrors.ErrLoadDataPreCheckFailed)
			s.ErrorContains(err, "there is active job on the target table already")
			etcdKey = fmt.Sprintf("/tidb/brie/import/import-into/%d", storage.TestLastTaskID.Load())
			s.Eventually(func() bool {
				resp, err2 := client.GetClient().Get(context.Background(), etcdKey)
				s.NoError(err2)
				return len(resp.Kvs) == 1
			}, maxWaitTime, 300*time.Millisecond)
		},
	)
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/storage/testSetLastTaskID", "return(true)")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.tk.MustQuery(sql)
	}()
	wg.Wait()
	s.tk.MustQuery("SELECT * FROM load_data.register_task;").Sort().Check(testkit.Rows("1 11 111"))

	// the task should be unregistered
	resp, err2 := client.GetClient().Get(context.Background(), etcdKey)
	s.NoError(err2)
	s.Len(resp.Kvs, 0)
}

func (s *mockGCSSuite) TestAddIndexBySQL() {
	s.T().Skip("enable after we support add-index option")
	s.tk.MustExec("DROP DATABASE IF EXISTS load_data;")
	s.tk.MustExec("CREATE DATABASE load_data;")
	s.tk.MustExec(`CREATE TABLE load_data.add_index (a INT, b INT, c INT, PRIMARY KEY (a), unique key b(b), key c_1(c));`)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "add_index-1.tsv"},
		Content:     []byte("1,11,111\n2,22,222\n3,33,333\n4,44,444\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "add_index-2.tsv"},
		Content:     []byte("5,55,555,\n6,66,666\n7,77,777"),
	})
	sql := fmt.Sprintf(`IMPORT INTO load_data.add_index FROM 'gs://test-load/add_index-*.tsv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM load_data.add_index;").Sort().Check(testkit.Rows(
		"1 11 111",
		"2 22 222",
		"3 33 333",
		"4 44 444",
		"5 55 555",
		"6 66 666",
		"7 77 777",
	))
	s.tk.MustQuery("SHOW CREATE TABLE load_data.add_index;").Check(testkit.Rows(
		"add_index CREATE TABLE `add_index` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `b` int(11) DEFAULT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
			"  UNIQUE KEY `b` (`b`),\n" +
			"  KEY `c_1` (`c`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// encode error, rollback
	s.tk.MustExec("truncate table load_data.add_index")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "add_index-3.tsv"},
		Content:     []byte("8,8|8,888\n"),
	})
	err := s.tk.QueryToErr(sql + " WITH __max_engine_size='1'")
	require.ErrorContains(s.T(), err, "Truncated incorrect DOUBLE value")
	s.tk.MustQuery("SHOW CREATE TABLE load_data.add_index;").Check(testkit.Rows(
		"add_index CREATE TABLE `add_index` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `b` int(11) DEFAULT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
			"  UNIQUE KEY `b` (`b`),\n" +
			"  KEY `c_1` (`c`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	s.tk.MustQuery("SELECT COUNT(1) FROM load_data.add_index;").Sort().Check(testkit.Rows(
		"0",
	))

	// checksum error
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "add_index-3.tsv"},
		Content:     []byte("7,88,888\n"),
	})
	err = s.tk.QueryToErr(sql)
	require.ErrorContains(s.T(), err, "checksum mismatched")
	s.tk.MustQuery("SELECT COUNT(1) FROM load_data.add_index;").Sort().Check(testkit.Rows(
		"7",
	))
	s.tk.MustQuery("SHOW CREATE TABLE load_data.add_index;").Check(testkit.Rows(
		"add_index CREATE TABLE `add_index` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `b` int(11) DEFAULT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n" +
			"  UNIQUE KEY `b` (`b`),\n" +
			"  KEY `c_1` (`c`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// duplicate key error, return add index sql
	s.tk.MustExec("truncate table load_data.add_index")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "add_index-3.tsv"},
		Content:     []byte("8,77,777\n"),
	})
	err = s.tk.QueryToErr(sql)
	require.EqualError(s.T(), err, "Failed to create index: [kv:1062]Duplicate entry '77' for key 'add_index.b'"+
		", please execute the SQL manually, sql: ALTER TABLE `load_data`.`add_index` ADD UNIQUE KEY `b`(`b`), ADD KEY `c_1`(`c`)")
	s.tk.MustQuery("SHOW CREATE TABLE load_data.add_index;").Check(testkit.Rows(
		"add_index CREATE TABLE `add_index` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `b` int(11) DEFAULT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// checksum error, duplicate key error, return add index sql
	s.tk.MustExec("drop table load_data.add_index")
	s.tk.MustExec(`CREATE TABLE load_data.add_index (a INT, b INT, c INT, PRIMARY KEY (a), unique key b(b), key c_1(c));`)
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "add_index-3.tsv"},
		Content:     []byte("7,77,777\n8,77,777\n"),
	})
	err = s.tk.QueryToErr(sql)
	require.ErrorContains(s.T(), err, "checksum mismatched")
	require.ErrorContains(s.T(), err, "Failed to create index: [kv:1062]Duplicate entry '77' for key 'add_index.b'"+
		", please execute the SQL manually, sql: ALTER TABLE `load_data`.`add_index` ADD UNIQUE KEY `b`(`b`), ADD KEY `c_1`(`c`)")
	s.tk.MustQuery("SHOW CREATE TABLE load_data.add_index;").Check(testkit.Rows(
		"add_index CREATE TABLE `add_index` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `b` int(11) DEFAULT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

func (s *mockGCSSuite) TestDiskQuota() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_test_disk_quota;")
	s.tk.MustExec("CREATE DATABASE load_test_disk_quota;")
	s.tk.MustExec(`CREATE TABLE load_test_disk_quota.t(a int, b int)`)

	lineCount := 10000
	data := make([]byte, 0, 1<<13)
	for i := 0; i < lineCount; i++ {
		data = append(data, []byte(fmt.Sprintf("%d,%d\n", i, i))...)
	}

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "diskquota-test.csv",
		},
		Content: data,
	})

	backup := importer.CheckDiskQuotaInterval
	importer.CheckDiskQuotaInterval = time.Millisecond
	defer func() {
		importer.CheckDiskQuotaInterval = backup
	}()

	// the encoded KV size is about 347440 bytes
	sql := fmt.Sprintf(`IMPORT INTO load_test_disk_quota.t FROM 'gs://test-load/diskquota-test.csv?endpoint=%s'
		with disk_quota='1b'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT count(1) FROM load_test_disk_quota.t;").Check(testkit.Rows(
		strconv.Itoa(lineCount),
	))
}

func (s *mockGCSSuite) TestAnalyze() {
	s.T().Skip("skip for ci now")
	s.tk.MustExec("DROP DATABASE IF EXISTS load_data;")
	s.tk.MustExec("CREATE DATABASE load_data;")

	// test auto analyze
	s.tk.MustExec("create table load_data.analyze_table(a int, b int, c int, index idx_ac(a,c), index idx_b(b))")
	lineCount := 2000
	data := make([]byte, 0, 1<<13)
	for i := 0; i < lineCount; i++ {
		data = append(data, []byte(fmt.Sprintf("1,%d,1\n", i))...)
	}
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "analyze-1.tsv"},
		Content:     data,
	})

	// without analyze, use idx_ac
	s.tk.MustExec("SET GLOBAL tidb_enable_auto_analyze=ON;")
	s.tk.MustQuery("EXPLAIN SELECT * FROM load_data.analyze_table WHERE a=1 and b=1 and c=1;").CheckContain("idx_ac(a, c)")

	sql := fmt.Sprintf(`IMPORT INTO load_data.analyze_table FROM 'gs://test-load/analyze-1.tsv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	require.Eventually(s.T(), func() bool {
		result := s.tk.MustQuery("EXPLAIN SELECT * FROM load_data.analyze_table WHERE a=1 and b=1 and c=1;")
		return strings.Contains(result.Rows()[1][3].(string), "idx_b(b)")
	}, 60*time.Second, time.Second)
	s.tk.MustQuery("SHOW ANALYZE STATUS;").CheckContain("analyze_table")
}

func (s *mockGCSSuite) TestZeroDateTime() {
	s.tk.MustExec("DROP DATABASE IF EXISTS import_into;")
	s.tk.MustExec("CREATE DATABASE import_into;")

	s.tk.MustExec("create table import_into.zero_time_table(t datetime)")
	s.tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES'")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "zero_time.csv"},
		Content:     []byte("1990-01-00 00:00:00\n"),
	})

	sql := fmt.Sprintf(`IMPORT INTO import_into.zero_time_table FROM 'gs://test-load/zero_time.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM import_into.zero_time_table;").Sort().Check(testkit.Rows("1990-01-00 00:00:00"))

	// set default value
	s.tk.MustExec("set @@sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	s.tk.MustExec("truncate table import_into.zero_time_table")
	err := s.tk.QueryToErr(sql)
	s.ErrorContains(err, `Incorrect datetime value: '1990-01-00 00:00:00'`)
}
