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

package loaddatatest

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/disttask/loaddata"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func (s *mockGCSSuite) prepareAndUseDB(db string) {
	s.tk.MustExec("drop database if exists " + db)
	s.tk.MustExec("create database " + db)
	s.tk.MustExec("use " + db)
}

// NOTE: for negative cases, see TestImportIntoPrivilegeNegativeCase in privileges_test.go
func (s *mockGCSSuite) TestImportIntoPrivilegePositiveCase() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "privilege-test",
			Name:       "db.tbl.001.csv",
		},
		Content: []byte("1,test1,11\n" +
			"2,test2,22"),
	})
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
	sql := fmt.Sprintf(`import into t FROM 'gs://privilege-test/db.tbl.*.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 test1 11", "2 test2 22"))
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
		s.tk.MustExec(sql)
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
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 <nil> 11", "2 test2 22"}...))
	// set to default on nil
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t (a,@1,c) set b=COALESCE(@1, 'def')
		FROM 'gs://test-multi-load/nil-input.tsv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
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
	s.tk.MustExec(loadDataSQL)
	s.Equal("Records: 9  Deleted: 0  Skipped: 0  Warnings: 0", s.tk.Session().GetSessionVars().StmtCtx.GetMessage())
	s.Equal(uint64(9), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44",
		"5 test5 55", "6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/skip-rows-*.csv?endpoint=%s'
		with thread=1, skip_rows=1`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.Equal("Records: 7  Deleted: 0  Skipped: 0  Warnings: 0", s.tk.Session().GetSessionVars().StmtCtx.GetMessage())
	s.Equal(uint64(7), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"2 test2 22", "3 test3 33", "4 test4 44",
		"6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/skip-rows-*.csv?endpoint=%s'
		with thread=1, skip_rows=3`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.Equal("Records: 3  Deleted: 0  Skipped: 0  Warnings: 0", s.tk.Session().GetSessionVars().StmtCtx.GetMessage())
	s.Equal(uint64(3), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
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

	s.tk.MustExec(fmt.Sprintf(`IMPORT INTO load_csv.t_gen1 
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))
	s.tk.MustExec("delete from t_gen1")

	// Specify the column, this should also work.
	s.tk.MustExec(fmt.Sprintf(`IMPORT INTO load_csv.t_gen1(a) 
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))
	s.tk.MustExec("delete from t_gen1")
	s.tk.MustExec(fmt.Sprintf(`IMPORT INTO load_csv.t_gen1(a,@1) 
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))

	// Swap the column and test again.
	s.tk.MustExec(`create table t_gen2 (a int generated ALWAYS AS (b+1), b int);`)
	s.tk.MustExec(fmt.Sprintf(`IMPORT INTO load_csv.t_gen2 
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("3 2", "4 3"))
	s.tk.MustExec(`delete from t_gen2`)

	// Specify the column b
	s.tk.MustExec(fmt.Sprintf(`IMPORT INTO load_csv.t_gen2(b) 
		FROM 'gcs://test-bucket/generated_columns.csv?endpoint=%s' WITH fields_terminated_by='\t'`, gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("2 1", "3 2"))
	s.tk.MustExec(`delete from t_gen2`)

	// Specify the column a
	s.tk.MustExec(fmt.Sprintf(`IMPORT INTO load_csv.t_gen2(a) 
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
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 test1 <nil>", "2 test2 22"}...))
	// mapped column of the missed field has non-null default
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int default 100);")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/input-cnt-mismatch.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	// we convert it to null all the time
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 test1 <nil>", "2 test2 22"}...))
	// mapped column of the missed field set to a variable
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int);")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t(a,b,@1) set c=COALESCE(@1, 100)
		FROM 'gs://test-multi-load/input-cnt-mismatch.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
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
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44",
		"5 test5 55", "6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	importer.MinDeliverBytes = bak

	s.tk.MustExec("truncate table t")
	bakCnt := importer.MinDeliverRowCnt
	importer.MinDeliverRowCnt = 2
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/min-deliver-bytes-rows.csv?endpoint=%s'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
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
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"1 [1, 2, 3]",
		"2 [2, 3, 4]",
	))
}

func (s *mockGCSSuite) TestMixedCompression() {
	s.tk.MustExec("DROP DATABASE IF EXISTS multi_load;")
	s.tk.MustExec("CREATE DATABASE multi_load;")
	s.tk.MustExec("CREATE TABLE multi_load.t (i INT PRIMARY KEY, s varchar(32));")

	// gzip content
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write([]byte(`1,test1
2,test2
3,test3
4,test4`))
	require.NoError(s.T(), err)
	err = w.Close()
	require.NoError(s.T(), err)

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "compress.001.tsv.gz",
		},
		Content: buf.Bytes(),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "compress.002.tsv",
		},
		Content: []byte(`5,test5
6,test6
7,test7
8,test8
9,test9`),
	})

	sql := fmt.Sprintf(`IMPORT INTO multi_load.t FROM 'gs://test-multi-load/compress.*?endpoint=%s'
		WITH thread=1;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM multi_load.t;").Check(testkit.Rows(
		"1 test1", "2 test2", "3 test3", "4 test4",
		"5 test5", "6 test6", "7 test7", "8 test8", "9 test9",
	))

	// with ignore N rows
	s.tk.MustExec("truncate table multi_load.t")
	sql = fmt.Sprintf(`IMPORT INTO multi_load.t FROM 'gs://test-multi-load/compress.*?endpoint=%s'
		WITH skip_rows=3, thread=1;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM multi_load.t;").Check(testkit.Rows(
		"4 test4",
		"8 test8", "9 test9",
	))
}

func (s *mockGCSSuite) TestLoadSQLDump() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (" +
		"id INT, c VARCHAR(20));")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load-parquet",
			Name:       "p",
		},
		Content: []byte(`insert into tbl values (1, 'a'), (2, 'b');`),
	})

	sql := fmt.Sprintf(`IMPORT INTO load_csv.t FROM 'gs://test-load-parquet/p?endpoint=%s'
		FORMAT 'SQL';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"1 a",
		"2 b",
	))
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
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.gbk;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))
	sql = fmt.Sprintf(`IMPORT INTO load_charset.utf8mb4 FROM 'gs://test-load/gbk.csv?endpoint=%s'
		WITH character_set='gbk'`, gcsEndpoint)
	s.tk.MustExec(sql)
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
	s.tk.MustExec(sql)
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
	err := s.tk.ExecToErr(sql)
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
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8;").Check(testkit.Rows(
		"1 ကခဂဃ",
		"2 ငစဆဇ",
	))
	sql = fmt.Sprintf(`IMPORT INTO load_charset.utf8mb4 FROM 'gs://test-load/utf8.tsv?endpoint=%s'
		 WITH character_set='utf8'`, gcsEndpoint)
	s.tk.MustExec(sql)
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
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.latin1;").Check(testkit.Rows(
		"1 ‘’“”",
		"2 ¡¢£¤",
	))

	s.tk.MustExec("TRUNCATE TABLE load_charset.utf8mb4;")
	sql = fmt.Sprintf(`IMPORT INTO load_charset.utf8mb4 FROM 'gs://test-load/latin1.tsv?endpoint=%s'
		 WITH character_set='latin1'`, gcsEndpoint)
	s.tk.MustExec(sql)
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
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT HEX(j) FROM load_charset.ascii;").Check(testkit.Rows(
		"0001020304050607",
	))
	sql = fmt.Sprintf(`IMPORT INTO load_charset.binary FROM 'gs://test-load/ascii.tsv?endpoint=%s'
		 WITH character_set='binary'`, gcsEndpoint)
	s.tk.MustExec(sql)
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
	s.tk.MustExec(sql)
	duration := time.Since(start).Seconds()
	s.tk.MustQuery("SELECT count(1) FROM load_test_write_speed.t;").Check(testkit.Rows(
		strconv.Itoa(lineCount),
	))

	// the encoded KV size is about 34744 bytes, so it would take about 5 more seconds to write all data.
	// with speed limit
	s.tk.MustExec("TRUNCATE TABLE load_test_write_speed.t;")
	start = time.Now()
	sql = fmt.Sprintf(`IMPORT INTO load_test_write_speed.t FROM 'gs://test-load/speed-test.csv?endpoint=%s'
		with max_write_speed=6000`, gcsEndpoint)
	s.tk.MustExec(sql)
	durationWithLimit := time.Since(start).Seconds()
	s.tk.MustQuery("SELECT count(1) FROM load_test_write_speed.t;").Check(testkit.Rows(
		strconv.Itoa(lineCount),
	))
	require.Less(s.T(), duration+5, durationWithLimit)
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

	// populate into 2 engines
	backup := config.DefaultBatchSize
	config.DefaultBatchSize = 1
	s.T().Cleanup(func() {
		config.DefaultBatchSize = backup
	})

	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), c int);")
	loadDataSQL := fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/duplicate-pk-*.csv?endpoint=%s'
		with thread=1`, gcsEndpoint)
	err := s.tk.ExecToErr(loadDataSQL)
	require.ErrorContains(s.T(), err, "ErrChecksumMismatch")
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "4 test4 44", "6 test6 66",
	}...))

	s.tk.MustExec("truncate table t;")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/duplicate-pk-*.csv?endpoint=%s'
		with thread=1, checksum_table='off'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "4 test4 44", "6 test6 66",
	}...))

	s.tk.MustExec("truncate table t;")
	loadDataSQL = fmt.Sprintf(`IMPORT INTO t FROM 'gs://test-multi-load/duplicate-pk-*.csv?endpoint=%s'
		with thread=1, checksum_table='optional'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "4 test4 44", "6 test6 66",
	}...))
}

func (s *mockGCSSuite) TestColumnsAndUserVars() {
	s.enableFailpoint("github.com/pingcap/tidb/disttask/framework/storage/testSetLastTaskID", "return(true)")
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
	s.tk.MustExec(sql)
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
	taskManager := storage.NewTaskManager(context.Background(), pool)
	subtasks, err := taskManager.GetSucceedSubtasksByStep(storage.TestLastTaskID.Load(), loaddata.Import)
	s.NoError(err)
	s.Len(subtasks, 1)
	serverInfo, err := infosync.GetServerInfo()
	s.NoError(err)
	for _, st := range subtasks {
		s.Equal(net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port))), st.SchedulerID)
	}
}
