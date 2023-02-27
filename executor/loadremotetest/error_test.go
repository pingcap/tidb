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
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
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
		"ERROR 8158 (HY000): The URI of INFILE is invalid. Reason: storage abc not support yet. Please provide a valid URI, such as 's3://import/test.csv?access_key_id={your_access_key_id ID}&secret_access_key={your_secret_access_key}&session_token={your_session_token}'")
	err = s.tk.ExecToErr("LOAD DATA INFILE 's3://no-network' INTO TABLE t;")
	checkClientErrorMessage(s.T(), err,
		"ERROR 8159 (HY000): Access to the source file has been denied. Please check the URI, access key and secret access key are correct")
	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://wrong-bucket/p?endpoint=%s'
		INTO TABLE t;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 8160 (HY000): Failed to read source files. Reason: failed to read gcs file, file info: input.bucket='wrong-bucket', input.key='p'. Please check the INFILE path is correct")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t.tsv",
		},
		Content: []byte("1\t2\n" +
			"1\t4\n"),
	})
	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
		FORMAT '123' INTO TABLE t;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 8157 (HY000): The FORMAT '123' is not supported")
	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
		FORMAT 'sql file' INTO TABLE t;`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		"ERROR 8160 (HY000): Failed to read source files. Reason: syntax error: unexpected Integer (1) at offset 1, expecting start of row. Only the following formats delimited text file (csv, tsv), parquet, sql are supported. Please provide the valid source file(s)")
	err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
		INTO TABLE t LINES STARTING BY '\n';`, gcsEndpoint))
	checkClientErrorMessage(s.T(), err,
		`ERROR 8162 (HY000): STARTING BY '
' cannot contain TERMINATED BY '
'`)

	// TODO: fix these tests
	//s.tk.MustExec("CREATE TABLE t2 (c1 INT, c2 INT, c3 INT);")
	//err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
	//	INTO TABLE t2;`, gcsEndpoint))
	//checkClientErrorMessage(s.T(), err,
	//	"ERROR 1261 (01000): Row 1 doesn't contain data for all columns")
	//s.tk.MustExec("CREATE TABLE t3 (c1 INT);")
	//err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
	//	INTO TABLE t3;`, gcsEndpoint))
	//checkClientErrorMessage(s.T(), err,
	//	"ERROR 1262 (01000): Row 1 was truncated; it contained more data than there were input columns")

	// TODO: don't use batchCheckAndInsert, mimic (*InsertExec).exec()

	//err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
	//	INTO TABLE t;`, gcsEndpoint))
	//checkClientErrorMessage(s.T(), err, "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'")

	//s.server.CreateObject(fakestorage.Object{
	//	ObjectAttrs: fakestorage.ObjectAttrs{
	//		BucketName: "test-tsv",
	//		Name:       "t2.tsv",
	//	},
	//	Content: []byte("null\t2\n" +
	//		"3\t4\n"),
	//})
	//err = s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t2.tsv?endpoint=%s'
	//	INTO TABLE t NULL DEFINED BY 'null';`, gcsEndpoint))
	//checkClientErrorMessage(s.T(), err, "ERROR 8154 (HY000): LOAD DATA raises error(s): xxx")
}
