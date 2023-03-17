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

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-load-parquet/p?endpoint=%s'
		FORMAT 'SQL file' INTO TABLE load_csv.t;`, gcsEndpoint)
	s.tk.MustExec(sql)
	s.tk.MustQuery("SELECT * FROM load_csv.t;").Check(testkit.Rows(
		"1 a",
		"2 b",
	))
	s.tk.MustExec("TRUNCATE TABLE load_csv.t;")

	rows := s.tk.MustQuery("SELECT job_id FROM mysql.load_data_jobs;").Rows()
	require.Greater(s.T(), len(rows), 0)
	jobID := rows[len(rows)-1][0].(string)
	err := s.tk.ExecToErr("CANCEL LOAD DATA JOB " + jobID)
	require.ErrorContains(s.T(), err, "The current job status cannot perform the operation. need status running or paused, but got finished")
	s.tk.MustExec("DROP LOAD DATA JOB " + jobID)
	s.tk.MustQuery("SELECT job_id FROM mysql.load_data_jobs WHERE job_id = " + jobID).Check(testkit.Rows())
}
