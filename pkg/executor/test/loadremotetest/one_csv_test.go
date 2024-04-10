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
	"github.com/pingcap/tidb/pkg/testkit"
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

func (s *mockGCSSuite) TestLoadCsvInTransaction() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")
	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("CREATE TABLE load_csv.t (i INT, s varchar(32));")

	s.server.CreateObject(
		fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-load-csv",
				Name:       "data.csv",
			},
			Content: []byte("100, test100\n101, hello\n102, 😄😄😄😄😄\n104, bye"),
		},
	)

	s.tk.MustExec("begin pessimistic")
	sql := fmt.Sprintf(
		`LOAD DATA INFILE 'gs://test-load-csv/data.csv?endpoint=%s' INTO TABLE load_csv.t `+
			"FIELDS TERMINATED BY ','",
		gcsEndpoint,
	)
	// test: load data stmt doesn't commit it
	s.tk.MustExec("insert into load_csv.t values (1, 'a')")
	s.tk.MustExec(sql)
	s.tk.MustQuery("select i from load_csv.t order by i").Check(
		testkit.Rows(
			"1", "100", "101",
			"102", "104",
		),
	)
	// load data can be rolled back
	s.tk.MustExec("rollback")
	s.tk.MustQuery("select * from load_csv.t").Check(testkit.Rows())

	// load data commit
	s.tk.MustExec("begin pessimistic")
	s.tk.MustExec(sql)
	s.tk.MustExec("commit")
	s.tk.MustQuery("select i from load_csv.t").Check(testkit.Rows("100", "101", "102", "104"))
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

func (s *mockGCSSuite) TestGeneratedColumns() {
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

	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s'"+
		" INTO TABLE load_csv.t_gen1", gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))
	s.tk.MustExec("delete from t_gen1")

	// Specify the column, this should also work.
	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s'"+
		" INTO TABLE load_csv.t_gen1 (a)", gcsEndpoint))
	s.tk.MustQuery("select * from t_gen1").Check(testkit.Rows("1 2", "2 3"))

	// Swap the column and test again.
	s.tk.MustExec(`create table t_gen2 (a int generated ALWAYS AS (b+1), b int);`)
	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s'"+
		" INTO TABLE load_csv.t_gen2", gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("3 2", "4 3"))
	s.tk.MustExec(`delete from t_gen2`)

	// Specify the column b
	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s'"+
		" INTO TABLE load_csv.t_gen2 (b)", gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("2 1", "3 2"))
	s.tk.MustExec(`delete from t_gen2`)

	// Specify the column a
	s.tk.MustExec(fmt.Sprintf("LOAD DATA INFILE 'gcs://test-bucket/generated_columns.csv?endpoint=%s'"+
		" INTO TABLE load_csv.t_gen2 (a)", gcsEndpoint))
	s.tk.MustQuery("select * from t_gen2").Check(testkit.Rows("<nil> <nil>", "<nil> <nil>"))
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
		INTO TABLE load_charset.utf8mb4`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8mb4;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "utf8mb4.tsv",
		},
		Content: []byte("1\t一丁丂七丄丅丆万丈三上下丌不与丏\n" +
			"2\t丐丑丒专且丕世丗丘丙业丛东丝丞丢"),
	})

	s.tk.MustExec("TRUNCATE TABLE load_charset.gbk;")
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/utf8mb4.tsv?endpoint=%s'
		INTO TABLE load_charset.gbk CHARACTER SET utf8mb4`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.gbk;").Check(testkit.Rows(
		"1 一丁丂七丄丅丆万丈三上下丌不与丏",
		"2 丐丑丒专且丕世丗丘丙业丛东丝丞丢",
	))

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-load",
			Name:       "emoji.tsv",
		},
		Content: []byte("1\t一丁丂七😀😁😂😃\n" +
			"2\t丐丑丒专😄😅😆😇"),
	})

	s.tk.MustExec("TRUNCATE TABLE load_charset.gbk;")
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/emoji.tsv?endpoint=%s'
		INTO TABLE load_charset.gbk CHARACTER SET utf8mb4`, gcsEndpoint)
	err := s.tk.ExecToErr(sql)
	checkClientErrorMessage(s.T(), err, `ERROR 1366 (HY000): Incorrect string value '\xF0\x9F\x98\x80' for column 'j'`)

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/emoji.tsv?endpoint=%s'
		IGNORE INTO TABLE load_charset.gbk CHARACTER SET utf8mb4`, gcsEndpoint)
	s.tk.MustExec(sql)
	require.Equal(s.T(), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2", s.tk.Session().GetSessionVars().StmtCtx.GetMessage())
	s.tk.MustQuery("SELECT HEX(j) FROM load_charset.gbk;").Check(testkit.Rows(
		"D2BBB6A18140C6DF3F3F3F3F",
		"D8A4B3F38145D7A83F3F3F3F",
	))

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/gbk.tsv?endpoint=%s'
		INTO TABLE load_charset.utf8mb4 CHARACTER SET unknown`, gcsEndpoint)
	err = s.tk.ExecToErr(sql)
	require.ErrorContains(s.T(), err, "Unknown character set: 'unknown'")
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
		Content: []byte("1\tကခဂဃ\n2\tငစဆဇ"),
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/utf8.tsv?endpoint=%s'
		INTO TABLE load_charset.utf8 CHARACTER SET utf8`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.utf8;").Check(testkit.Rows(
		"1 ကခဂဃ",
		"2 ငစဆဇ",
	))
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/utf8.tsv?endpoint=%s'
		INTO TABLE load_charset.utf8mb4 CHARACTER SET utf8`, gcsEndpoint)
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
		Content: []byte{0x31, 0x09, 0x91, 0x92, 0x93, 0x94, 0x0a, 0x32, 0x09, 0xa1, 0xa2, 0xa3, 0xa4},
	})
	s.tk.MustExec(`CREATE TABLE load_charset.latin1 (
		i INT, j VARCHAR(255)
		) CHARACTER SET latin1;`)
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/latin1.tsv?endpoint=%s'
		INTO TABLE load_charset.latin1 CHARACTER SET latin1`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_charset.latin1;").Check(testkit.Rows(
		"1 ‘’“”",
		"2 ¡¢£¤",
	))

	s.tk.MustExec("TRUNCATE TABLE load_charset.utf8mb4;")
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/latin1.tsv?endpoint=%s'
		INTO TABLE load_charset.utf8mb4 CHARACTER SET latin1`, gcsEndpoint)
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
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/ascii.tsv?endpoint=%s'
		INTO TABLE load_charset.ascii CHARACTER SET ascii`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT HEX(j) FROM load_charset.ascii;").Check(testkit.Rows(
		"0001020304050607",
	))
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/ascii.tsv?endpoint=%s'
		INTO TABLE load_charset.binary CHARACTER SET binary`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT HEX(j) FROM load_charset.binary;").Check(testkit.Rows(
		"0001020304050607",
	))
}

func (s *mockGCSSuite) TestColumnsAndUserVars() {
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
	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load/cols_and_vars-*.tsv?endpoint=%s'
		INTO TABLE load_data.cols_and_vars fields terminated by ','
		(@V1, @v2, @v3) set a=@V1, b=@V2*10, c=123`, gcsEndpoint)
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
}
