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

package loadremotetest

import (
	"fmt"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/testkit"
)

func (s *mockGCSSuite) TestLoadCSV() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (i INT, s varchar(32));")

	// no-new-line-at-end

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load-csv",
			Name:       "no-new-line-at-end.csv",
		},
		Content: []byte(`i,s
100,"test100"
101,"\""
102,"😄😄😄😄😄"
104,""`),
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load-csv/no-new-line-at-end.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 1 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"100 test100",
		"101 \"",
		"102 😄😄😄😄😄",
		"104 ",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	// new-line-at-end

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load-csv",
			Name:       "new-line-at-end.csv",
		},
		Content: []byte(`i,s
100,"test100"
101,"\""
102,"😄😄😄😄😄"
104,""
`),
	})

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load-csv/new-line-at-end.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 1 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"100 test100",
		"101 \"",
		"102 😄😄😄😄😄",
		"104 ",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	// can't read file at tidb-server
	sql = "LOAD DATA INFILE '/etc/passwd' INTO TABLE load_csv.t;"
	s.tk.MustContainErrMsg(sql, "Don't support load data from tidb-server's disk. Or if you want to load local data via client, the path of INFILE '/etc/passwd' needs to specify the clause of LOCAL first")
}

func (s *mockGCSSuite) TestIgnoreNLines() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (s varchar(32), i INT);")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-bucket",
			Name:       "ignore-lines-bad-syntax.csv",
		},
		Content: []byte(`"bad syntax"1
"b",2
"c",3
`),
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/ignore-lines-bad-syntax.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 1 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"b 2",
		"c 3",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/ignore-lines-bad-syntax.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 100 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows())
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	// test IGNORE N LINES will directly find (line) terminator without checking it's inside quotes

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-bucket",
			Name:       "count-terminator-inside-quotes.csv",
		},
		Content: []byte(`"a
",1
"b
",2
"c",3
`),
	})

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/count-terminator-inside-quotes.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n' IGNORE 2 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"b\n 2",
		"c 3",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")
}

func (s *mockGCSSuite) TestCustomizeNULL() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (c varchar(32), c2 varchar(32));")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-bucket",
			Name:       "customize-null.csv",
		},
		Content: []byte(`\N,"\N"
!N,"!N"
NULL,"NULL"
mynull,"mynull"
`),
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ','
		LINES TERMINATED BY '\n';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`<nil> "N"`,
		`!N "!N"`,
		`NULL "NULL"`,
		`mynull "mynull"`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' ESCAPED BY '\\'
		LINES TERMINATED BY '\n';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`<nil> "N"`,
		`!N "!N"`,
		`NULL "NULL"`,
		`mynull "mynull"`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '!'
		LINES TERMINATED BY '\n';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`\N \N`,
		`<nil> <nil>`,
		`<nil> NULL`,
		`mynull mynull`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' DEFINED NULL BY 'NULL'
		LINES TERMINATED BY '\n';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`<nil> <nil>`,
		`!N !N`,
		`<nil> NULL`,
		`mynull mynull`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' DEFINED NULL BY 'NULL' OPTIONALLY ENCLOSED
		LINES TERMINATED BY '\n'`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`<nil> <nil>`,
		`!N !N`,
		`<nil> <nil>`,
		`mynull mynull`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '!' DEFINED NULL BY 'mynull' OPTIONALLY ENCLOSED
		LINES TERMINATED BY '\n'`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`\N \N`,
		`<nil> <nil>`,
		`NULL NULL`,
		`<nil> <nil>`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	ascii0 := string([]byte{0})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-bucket",
			Name:       "ascii-0.csv",
		},
		Content: []byte(fmt.Sprintf(`\0,"\0"
%s,"%s"`, ascii0, ascii0)),
	})
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/ascii-0.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' DEFINED NULL BY x'00' OPTIONALLY ENCLOSED
		LINES TERMINATED BY '\n';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`\0 \0`,
		`<nil> <nil>`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/ascii-0.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' DEFINED NULL BY x'00'
		LINES TERMINATED BY '\n';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"<nil> \000",
		"<nil> \000",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' DEFINED NULL BY 'mynull' OPTIONALLY ENCLOSED
		LINES TERMINATED BY '\n';`, gcsEndpoint)
	s.tk.MustMatchErrMsg(sql, `must specify FIELDS \[OPTIONALLY\] ENCLOSED BY`)
}

func (s *mockGCSSuite) TestLoadDataForGeneratedColumns() {
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
		Content: []byte("1	2\n2	3"),
	})

	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s' INTO TABLE load_csv.t_gen1", gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))
	s.tk.MustExec("delete from t_gen1")

	// Specify the column, this should also work.
	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s' INTO TABLE load_csv.t_gen1 (a)", gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))

	// Swap the column and test again.
	s.tk.MustExec(`create table t_gen2 (a int generated ALWAYS AS (b+1), b int);`)
	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s' INTO TABLE load_csv.t_gen2", gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("3 2", "4 3"))
	s.tk.MustExec(`delete from t_gen2`)

	// Specify the column b
	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s' INTO TABLE load_csv.t_gen2 (b)", gcsEndpoint))
	s.tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1262 Row 1 was truncated; it contained more data than there were input columns",
		"Warning 1262 Row 2 was truncated; it contained more data than there were input columns"))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("2 1", "3 2"))
	s.tk.MustExec(`delete from t_gen2`)

	// Specify the column a
	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s' INTO TABLE load_csv.t_gen2 (a)", gcsEndpoint))
	s.tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1262 Row 1 was truncated; it contained more data than there were input columns",
		"Warning 1262 Row 2 was truncated; it contained more data than there were input columns"))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("<nil> <nil>", "<nil> <nil>"))
}
