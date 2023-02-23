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
	s.tk.MustContainErrMsg(sql, "don't support load data from tidb-server")
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
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
		LINES TERMINATED BY '\n'
		NULL DEFINED BY 'NULL';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`<nil> <nil>`,
		`!N !N`,
		`<nil> NULL`,
		`mynull mynull`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
		LINES TERMINATED BY '\n'
		NULL DEFINED BY 'NULL' OPTIONALLY ENCLOSED;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`<nil> <nil>`,
		`!N !N`,
		`<nil> <nil>`,
		`mynull mynull`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '!'
		LINES TERMINATED BY '\n'
		NULL DEFINED BY 'mynull' OPTIONALLY ENCLOSED;`, gcsEndpoint)
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
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY ''
		LINES TERMINATED BY '\n'
		NULL DEFINED BY x'00' OPTIONALLY ENCLOSED;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		`\0 \0`,
		`<nil> <nil>`,
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/ascii-0.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
		LINES TERMINATED BY '\n'
		NULL DEFINED BY x'00';`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"<nil> \000",
		"<nil> \000",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gcs://test-bucket/customize-null.csv?endpoint=%s' INTO TABLE load_csv.t
		FIELDS TERMINATED BY ','
		LINES TERMINATED BY '\n'
		NULL DEFINED BY 'mynull' OPTIONALLY ENCLOSED;`, gcsEndpoint)
	s.tk.MustMatchErrMsg(sql, `must specify FIELDS \[OPTIONALLY\] ENCLOSED BY`)
}
