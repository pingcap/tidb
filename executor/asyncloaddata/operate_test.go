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

package asyncloaddata_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	. "github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/stretchr/testify/require"
)

func (s *mockGCSSuite) TestOperateRunningJob() {
	s.tk.MustExec("DROP DATABASE IF EXISTS test_operate;")
	s.tk.MustExec("CREATE DATABASE test_operate;")
	s.tk.MustExec("CREATE TABLE test_operate.t (i INT PRIMARY KEY);")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-operate",
			Name:       "t.tsv",
		},
		Content: []byte("1\n2\n3\n4\n5\n6\n7\n8\n9\n10"),
	})

	backup := HeartBeatInSec
	HeartBeatInSec = 1
	s.T().Cleanup(func() {
		HeartBeatInSec = backup
	})

	s.enableFailpoint("github.com/pingcap/tidb/executor/AfterCreateLoadDataJob", `sleep(1000)`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/AfterStartJob", `sleep(1000)`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/AfterCommitOneTask", `sleep(1000)`)
	s.tk.MustExec("SET SESSION tidb_dml_batch_size = 1;")
	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-operate/t.tsv?endpoint=%s'
		INTO TABLE test_operate.t;`, gcsEndpoint)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.tk.MustContainErrMsg(sql, "failed to keepalive")
	}()

	// TODO: remove this sleep after moving mysql.load_data_jobs to bootstrap
	time.Sleep(3 * time.Second)
	rows := s.tk.MustQuery("SHOW LOAD DATA JOBS;").Rows()
	require.Greater(s.T(), len(rows), 0)
	jobID := rows[len(rows)-1][0].(string)
	s.tk.MustExec("DROP LOAD DATA JOB " + jobID + ";")
	wg.Wait()
}
