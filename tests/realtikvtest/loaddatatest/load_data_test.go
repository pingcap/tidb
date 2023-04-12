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
	"fmt"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/testkit"
)

func (s *mockGCSSuite) prepareAndUseDB(db string) {
	s.tk.MustExec("drop database if exists " + db)
	s.tk.MustExec("create database " + db)
	s.tk.MustExec("use " + db)
}

func (s *mockGCSSuite) TestPhysicalMode() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.001.tsv",
		},
		Content: []byte("1\ttest1\t11\n" +
			"2\ttest2\t22"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.002.tsv",
		},
		Content: []byte("3\ttest3\t33\n" +
			"4\ttest4\t44"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "db.tbl.003.tsv",
		},
		Content: []byte("5\ttest5\t55\n" +
			"6\ttest6\t66"),
	})
	s.prepareAndUseDB("load_data")

	allData := []string{"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44", "5 test5 55", "6 test6 66"}
	cases := []struct {
		createTableSQL string
		flags          string
		res            []string
		querySQL       string
	}{
		{"create table t (a bigint, b varchar(100), c int);", "", allData, ""},
		{"create table t (a bigint primary key, b varchar(100), c int);", "", allData, ""},
		{"create table t (a bigint primary key, b varchar(100), c int, key(b, a));", "", allData, ""},
		{"create table t (a bigint auto_increment primary key, b varchar(100), c int);", "", allData, ""},
		{"create table t (a bigint auto_random primary key, b varchar(100), c int);", "", allData, ""},
		{"create table t (a bigint, b varchar(100), c int, primary key(b,c));", "", allData, ""},

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
			res: []string{"1 1 test1 11", "2 2 test2 22", "4 3 test3 33", "5 4 test4 44", "7 5 test5 55", "8 6 test6 66"},
		},
		// default value for auto_random
		{
			createTableSQL: "create table t (a bigint auto_random primary key, b int, c varchar(100), d int);",
			flags:          "(b, c, d)",
			res: []string{
				"288230376151711745 1 test1 11",
				"288230376151711746 2 test2 22",
				"1441151880758558724 3 test3 33",
				"1441151880758558725 4 test4 44",
				"6052837899185946631 5 test5 55",
				"6052837899185946632 6 test6 66",
			},
			// auto_random id contains shard bit.
			querySQL: "select * from t order by b",
		},
	}

	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/db.tbl.*.tsv?endpoint=%s'
		INTO TABLE t %%s with import_mode='physical'`, gcsEndpoint)
	for _, c := range cases {
		s.tk.MustExec("drop table if exists t;")
		s.tk.MustExec(c.createTableSQL)
		sql := fmt.Sprintf(loadDataSQL, c.flags)
		s.tk.MustExec(sql)
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
	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/nil-input.tsv?endpoint=%s'
		INTO TABLE t with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 <nil> 11", "2 test2 22"}...))
	// set to default on nil
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/nil-input.tsv?endpoint=%s'
		INTO TABLE t (a,@1,c) set b=COALESCE(@1, 'def') with import_mode='physical'`, gcsEndpoint)
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
	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/skip-rows-*.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44",
		"5 test5 55", "6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/skip-rows-*.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' ignore 1 lines with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"2 test2 22", "3 test3 33", "4 test4 44",
		"6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	s.tk.MustExec("truncate table t")
	loadDataSQL = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/skip-rows-*.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' ignore 3 lines with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"4 test4 44",
		"8 test8 88", "9 test9 99",
	}...))
}

func (s *mockGCSSuite) TestGeneratedCol() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-multi-load",
			Name:       "generated-col.csv",
		},
		Content: []byte(`1,test1,11
2,test2,22
3,test3,33`),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int as (a+100));")
	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/generated-col.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' (a,b,@1) with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 101", "2 test2 102", "3 test3 103",
	}...))
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
	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/input-cnt-mismatch.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 test1 <nil>", "2 test2 22"}...))
	// mapped column of the missed field has non-null default
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int default 100);")
	loadDataSQL = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/input-cnt-mismatch.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	// we convert it to null all the time
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{"1 test1 <nil>", "2 test2 22"}...))
	// mapped column of the missed field set to a variable
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint, b varchar(100), c int);")
	loadDataSQL = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/input-cnt-mismatch.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' (a,b,@1) set c=COALESCE(@1, 100) with import_mode='physical'`, gcsEndpoint)
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
	loadDataSQL := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/min-deliver-bytes-rows.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44",
		"5 test5 55", "6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	importer.MinDeliverBytes = bak

	s.tk.MustExec("truncate table t")
	bakCnt := importer.MinDeliverRowCnt
	importer.MinDeliverRowCnt = 2
	loadDataSQL = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-multi-load/min-deliver-bytes-rows.csv?endpoint=%s'
		INTO TABLE t fields terminated by ',' with import_mode='physical'`, gcsEndpoint)
	s.tk.MustExec(loadDataSQL)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows([]string{
		"1 test1 11", "2 test2 22", "3 test3 33", "4 test4 44",
		"5 test5 55", "6 test6 66", "7 test7 77", "8 test8 88", "9 test9 99",
	}...))
	importer.MinDeliverRowCnt = bakCnt
}

func (s *mockGCSSuite) TestMultiValueIndex() {
	s.testMultiValueIndex(importer.LogicalImportMode)
	s.testMultiValueIndex(importer.PhysicalImportMode)
}

func (s *mockGCSSuite) testMultiValueIndex(importMode string) {
	withOptions := fmt.Sprintf("WITH import_mode='%s'", importMode)
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

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load-csv/1.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 1 LINES %s;`, gcsEndpoint, withOptions)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"1 [1, 2, 3]",
		"2 [2, 3, 4]",
	))
}
