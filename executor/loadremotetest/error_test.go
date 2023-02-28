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
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func checkClientErrorMessage(t *testing.T, err error, msg string) {
	require.Error(t, err)
	cause := errors.Cause(err)
	terr, ok := cause.(*errors.Error)
	require.True(t, ok, "%T", cause)
	require.Contains(t, terror.ToSQLError(terr).Error(), msg)
}

func (s *mockGCSSuite) TestWrongColumnNum() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_csv;")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t.tsv",
		},
		Content: []byte("1\t2\n" +
			"1\t4\n"),
	})

	s.tk.MustExec("CREATE DATABASE load_csv;")
	s.tk.MustExec("USE load_csv;")

	// table has fewer columns than data

	s.tk.MustExec("CREATE TABLE t (c INT);")
	s.tk.MustExec("SET SESSION sql_mode = ''")
	err := s.tk.ExecToErr(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
		INTO TABLE t;`, gcsEndpoint))
	require.NoError(s.T(), err)
	s.tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows(
		"Warning", "1262", "Row 1 was truncated; it contained more data than there were input columns"))
	msg := s.tk.Session().GetSessionVars().StmtCtx.GetMessage()
	require.Equal(s.T(), "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1", msg)
}
