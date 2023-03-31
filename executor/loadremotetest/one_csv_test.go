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
	"github.com/stretchr/testify/require"
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

func (s *mockGCSSuite) TestMultiValueIndex() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec(`CREATE TABLE load_csv.t (
    	i INT, j JSON,
    	KEY idx ((cast(json_extract(j, '$[*]') as signed array)))
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
		LINES TERMINATED BY '\n' IGNORE 1 LINES;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"1 [1, 2, 3]",
		"2 [2, 3, 4]",
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
			Name:       "gbk.tsv",
		},
		Content: []byte{
			// 1	一丁丂七丄丅丆万丈三上下丌不与丏
			0x31, 0x09, 0xd2, 0xbb, 0xb6, 0xa1, 0x81, 0x40, 0xc6, 0xdf, 0x81,
			0x41, 0x81, 0x42, 0x81, 0x43, 0xcd, 0xf2, 0xd5, 0xc9, 0xc8, 0xfd,
			0xc9, 0xcf, 0xcf, 0xc2, 0xd8, 0xa2, 0xb2, 0xbb, 0xd3, 0xeb, 0x81,
			0x44, 0x0a,
			// 2	丐丑丒专且丕世丗丘丙业丛东丝丞丢
			0x32, 0x09, 0xd8, 0xa4, 0xb3, 0xf3, 0x81, 0x45, 0xd7, 0xa8, 0xc7,
			0xd2, 0xd8, 0xa7, 0xca, 0xc0, 0x81, 0x46, 0xc7, 0xf0, 0xb1, 0xfb,
			0xd2, 0xb5, 0xb4, 0xd4, 0xb6, 0xab, 0xcb, 0xbf, 0xd8, 0xa9, 0xb6,
			0xaa,
		},
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/gbk.tsv?endpoint=%s'
		INTO TABLE load_charset.gbk CHARACTER SET gbk`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.gbk;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/gbk.tsv?endpoint=%s'
		INTO TABLE load_charset.utf8mb4 CHARACTER SET gbk`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8mb4;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))

	s.tk.MustExec("TRUNCATE TABLE load_charset.utf8mb4;")
	s.tk.MustExec("SET SESSION character_set_database = 'gbk';")
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/gbk.tsv?endpoint=%s'
		INTO TABLE load_charset.utf8mb4;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8mb4;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/gbk.tsv?endpoint=%s'
		INTO TABLE load_charset.utf8mb4 CHARACTER SET unknown`, gcsEndpoint)
	err := s.tk.ExecToErr(sql)
	require.ErrorContains(s.T(), err, "Unknown character set: 'unknown'")
}
