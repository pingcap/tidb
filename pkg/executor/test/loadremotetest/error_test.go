// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loadremotetest

import (
	"fmt"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func checkClientErrorMessage(t *testing.T, err error, msg string) {
	require.Error(t, err)
	cause := errors.Cause(err)
	terr, ok := cause.(*errors.Error)
	require.True(t, ok, "%T", cause)
	require.Contains(t, terror.ToSQLError(terr).Error(), msg)
}

func (s *mockGCSSuite) TestErrorMessage() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")

	err := s.tk.ExecToErr("LOAD DATA INFILE 'gs://1' INTO TABLE t")
	checkClientErrorMessage(s.T(), err, "ERROR 1046 (3D000): No database selected")
	err = s.tk.ExecToErr("LOAD DATA INFILE 'gs://1' INTO TABLE wrongdb.t")
	checkClientErrorMessage(s.T(), err, "ERROR 1146 (42S02): Table 'wrongdb.t' doesn't exist")

	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("USE load_csv;")
	s.tk.MustExec("CREATE TABLE t (i INT PRIMARY KEY, s varchar(32));")

	err = s.tk.ExecToErr("LOAD DATA INFILE 'gs://1' INTO TABLE t (wrong)")
	checkClientErrorMessage(s.T(), err, "ERROR 1054 (42S22): Unknown column 'wrong' in 'field list'")
	// This behaviour is different from MySQL
	err = s.tk.ExecToErr("LOAD DATA INFILE 'gs://1' INTO TABLE t (i,i)")
	checkClientErrorMessage(s.T(), err, "ERROR 1110 (42000): Column 'i' specified twice")
	err = s.tk.ExecToErr("LOAD DATA INFILE 'gs://1' INTO TABLE t (@v) SET wrong=@v")
	checkClientErrorMessage(s.T(), err, "ERROR 1054 (42S22): Unknown column 'wrong' in 'field list'")
	err = s.tk.ExecToErr("LOAD DATA INFILE 'abc://1' INTO TABLE t;")
	checkClientErrorMessage(s.T(), err,
		"ERROR 8158 (HY000): The URI of data source is invalid. Reason: storage abc not support yet. Please provide a valid URI, such as")
	err = s.tk.ExecToErr("LOAD DATA INFILE 's3://no-network' INTO TABLE t;")
	checkClientErrorMessage(s.T(), err,
		"ERROR 8159 (HY000): Access to the data source has been denied. Reason: failed to get region of bucket no-network. Please check the URI, access key and secret access key are correct")
	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://wrong-bucket/p?endpoint=%s'
		INTO TABLE t;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 8160 (HY000): Failed to read source files. Reason: the object doesn't exist, file info: input.bucket='wrong-bucket', input.key='p'. Please check the file location is correct")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t.tsv",
		},
		Content: []byte("1\t2\n" +
			"1\t4\n"),
	})
	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
		INTO TABLE t LINES STARTING BY '\n';`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		`ERROR 8162 (HY000): STARTING BY '
' cannot contain LINES TERMINATED BY '
'`)
}

func (s *mockGCSSuite) TestColumnNumMismatch() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t2.tsv",
		},
		Content: []byte("1\t2\n" +
			"1\t4\n"),
	})

	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("USE load_csv;")

	// table has fewer columns than data

	s.tk.MustExec("CREATE TABLE t (c INT);")
	s.tk.MustExec("SET SESSION sql_mode = ''")
	err := s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t2.tsv?endpoint=%s'
		INTO TABLE t;`, gcsEndpoint))
	require.NoError(s.T(), err)
	require.Equal(s.T(), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2", s.tk.Session().LastMessage())
	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows(
		"Warning 1262 Row 1 was truncated; it contained more data than there were input columns",
		"Warning 1262 Row 2 was truncated; it contained more data than there were input columns"))

	s.tk.MustExec("SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t2.tsv?endpoint=%s'
		INTO TABLE t;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 1262 (01000): Row 1 was truncated; it contained more data than there were input columns")

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t2.tsv?endpoint=%s'
		REPLACE INTO TABLE t;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 1262 (01000): Row 1 was truncated; it contained more data than there were input columns")

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t2.tsv?endpoint=%s'
		IGNORE INTO TABLE t;`, gcsEndpoint))
	require.NoError(s.T(), err)
	require.Equal(s.T(), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2", s.tk.Session().LastMessage())
	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows(
		"Warning 1262 Row 1 was truncated; it contained more data than there were input columns",
		"Warning 1262 Row 2 was truncated; it contained more data than there were input columns"))

	// table has more columns than data

	s.tk.MustExec("CREATE TABLE t2 (c1 INT, c2 INT, c3 INT);")
	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t2.tsv?endpoint=%s'
		INTO TABLE t2;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 1261 (01000): Row 1 doesn't contain data for all columns")

	// fill default value for missing columns

	s.tk.MustExec(`CREATE TABLE t3 (
    	c1 INT NOT NULL,
    	c2 INT NOT NULL,
    	c3 INT NOT NULL DEFAULT 1);`)
	s.tk.MustExec(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t2.tsv?endpoint=%s'
		INTO TABLE t3 (c1, c2);`, gcsEndpoint))
	s.tk.MustQuery("SELECT * FROM t3;").Check(testkit.Rows(
		"1 2 1",
		"1 4 1"))
}

func (s *mockGCSSuite) TestEvalError() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t3.tsv",
		},
		Content: []byte("1\t2\n" +
			"1\t4\n"),
	})

	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("USE load_csv;")

	s.tk.MustExec("CREATE TABLE t (c INT, c2 INT UNIQUE);")
	s.tk.MustExec("SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	err := s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t3.tsv?endpoint=%s'
		INTO TABLE t (@v1, c2) SET c=@v1+'asd';`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 1292 (22007): Truncated incorrect DOUBLE value: 'asd'")

	// REPLACE does not help

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t3.tsv?endpoint=%s'
		REPLACE INTO TABLE t (@v1, c2) SET c=@v1+'asd';`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 1292 (22007): Truncated incorrect DOUBLE value: 'asd'")

	// IGNORE helps

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t3.tsv?endpoint=%s'
		IGNORE INTO TABLE t (@v1, c2) SET c=@v1+'asd';`, gcsEndpoint))
	require.NoError(s.T(), err)
	require.Equal(s.T(), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2", s.tk.Session().LastMessage())
	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows(
		"Warning 1292 Truncated incorrect DOUBLE value: 'asd'",
		"Warning 1292 Truncated incorrect DOUBLE value: 'asd'"))
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows(
		"1 2",
		"1 4"))
}

func (s *mockGCSSuite) TestDataError() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "null.tsv",
		},
		Content: []byte("1\t\\N\n" +
			"1\t4\n"),
	})

	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("USE load_csv;")

	s.tk.MustExec("CREATE TABLE t (c INT NOT NULL, c2 INT NOT NULL);")
	s.tk.MustExec("SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	err := s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/null.tsv?endpoint=%s'
		INTO TABLE t;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 1263 (22004): Column set to default value; NULL supplied to NOT NULL column 'c2' at row 1")

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/null.tsv?endpoint=%s'
		IGNORE INTO TABLE t;`, gcsEndpoint))
	require.NoError(s.T(), err)
	require.Equal(s.T(), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 1", s.tk.Session().LastMessage())
	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows(
		"Warning 1263 Column set to default value; NULL supplied to NOT NULL column 'c2' at row 1"))

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t4.tsv",
		},
		Content: []byte("1\t2\n" +
			"1\t2\n"),
	})

	s.tk.MustExec("CREATE TABLE t2 (c INT PRIMARY KEY, c2 INT NOT NULL);")

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t4.tsv?endpoint=%s'
		INTO TABLE t2;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err, "ERROR 1062 (23000): Duplicate entry '1' for key 't2.PRIMARY'")

	s.tk.MustExec("CREATE TABLE t3 (c INT, c2 INT UNIQUE);")

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t4.tsv?endpoint=%s'
		INTO TABLE t3;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err, "ERROR 1062 (23000): Duplicate entry '2' for key 't3.c2'")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t5.tsv",
		},
		Content: []byte("1\t100\n" +
			"2\t100\n"),
	})
	s.tk.MustExec(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t5.tsv?endpoint=%s'
		REPLACE INTO TABLE t3;`, gcsEndpoint))
	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows())
	s.tk.MustQuery("SELECT * FROM t3;").Check(testkit.Rows(
		"2 100"))

	s.tk.MustExec("UPDATE t3 SET c = 3;")
	s.tk.MustExec(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t5.tsv?endpoint=%s'
		IGNORE INTO TABLE t3;`, gcsEndpoint))
	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows(
		"Warning 1062 Duplicate entry '100' for key 't3.c2'",
		"Warning 1062 Duplicate entry '100' for key 't3.c2'"))
	s.tk.MustQuery("SELECT * FROM t3;").Check(testkit.Rows(
		"3 100"))
}

func (s *mockGCSSuite) TestIssue43555() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-csv",
			Name:       "43555.csv",
		},
		Content: []byte("6\n" +
			"7.1\n"),
	})

	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("USE load_csv;")

	s.tk.MustExec("CREATE TABLE t (id CHAR(1), id1 INT);")
	s.tk.MustExec("SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")

	err := s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-csv/43555.csv?endpoint=%s'
		IGNORE INTO TABLE t;`, gcsEndpoint))
	require.NoError(s.T(), err)
	require.Equal(s.T(), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 3", s.tk.Session().LastMessage())

	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows(
		"Warning 1261 Row 1 doesn't contain data for all columns",
		"Warning 1261 Row 2 doesn't contain data for all columns",
		"Warning 1265 Data truncated for column 'id' at row 2"))
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows(
		"6 <nil>",
		"7 <nil>"))

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-csv/43555.csv?endpoint=%s'
		INTO TABLE t (id);`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err, "ERROR 1265 (01000): Data truncated for column 'id' at row 2")

	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-csv/43555.csv?endpoint=%s'
		IGNORE INTO TABLE t (id1) SET id='7.1';`, gcsEndpoint))
	require.NoError(s.T(), err)
	require.Equal(s.T(), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2", s.tk.Session().LastMessage())
	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows(
		"Warning 1265 Data truncated for column 'id' at row 1",
		"Warning 1265 Data truncated for column 'id' at row 2"))
}
