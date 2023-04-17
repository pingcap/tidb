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

package loaddatatest

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func (s *mockGCSSuite) TestOperateRunningJob() {
	s.testOperateRunningJob(importer.LogicalImportMode)
	s.testOperateRunningJob(importer.PhysicalImportMode)
}

func (s *mockGCSSuite) testOperateRunningJob(importMode string) {
	withOptions := fmt.Sprintf("WITH import_mode='%s'", importMode)
	if importMode == importer.LogicalImportMode {
		withOptions += ", batch_size=1"
	}

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

	backup := asyncloaddata.HeartBeatInSec
	asyncloaddata.HeartBeatInSec = 1
	s.T().Cleanup(func() {
		asyncloaddata.HeartBeatInSec = backup
	})
	backup2 := config.DefaultBatchSize
	config.DefaultBatchSize = 1
	s.T().Cleanup(func() {
		config.DefaultBatchSize = backup2
	})

	s.enableFailpoint("github.com/pingcap/tidb/executor/asyncloaddata/SaveLastLoadDataJobID", `return(true)`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/asyncloaddata/AfterCreateLoadDataJob", `sleep(1000)`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/asyncloaddata/AfterStartJob", `sleep(1000)`)
	if importMode == importer.LogicalImportMode {
		s.enableFailpoint("github.com/pingcap/tidb/executor/AfterCommitOneTask", `sleep(1000)`)
	} else {
		s.enableFailpoint("github.com/pingcap/tidb/executor/importer/AfterImportDataEngine", `sleep(1000)`)
	}
	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-operate/t.tsv?endpoint=%s'
		INTO TABLE test_operate.t %s;`, gcsEndpoint, withOptions)

	// DROP can happen anytime
	user := &auth.UserIdentity{
		AuthUsername: "test-load-3",
		AuthHostname: "test-host",
	}
	backupUser := s.tk.Session().GetSessionVars().User
	s.tk.Session().GetSessionVars().User = user
	s.T().Cleanup(func() {
		s.tk.Session().GetSessionVars().User = backupUser
	})
	tk2 := testkit.NewTestKit(s.T(), s.store)
	tk2.Session().GetSessionVars().User = user
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk2.MustContainErrMsg(sql, "failed to keepalive")
	}()

	time.Sleep(3 * time.Second)
	jobID := asyncloaddata.TestLastLoadDataJobID.Load()
	s.tk.MustExec(fmt.Sprintf("DROP LOAD DATA JOB %d", jobID))
	wg.Wait()

	// test CANCEL

	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-operate/t.tsv?endpoint=%s'
		REPLACE INTO TABLE test_operate.t  %s;`, gcsEndpoint, withOptions)
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk2.MustContainErrMsg(sql, "failed to keepalive")
	}()

	time.Sleep(time.Second)
	jobID = asyncloaddata.TestLastLoadDataJobID.Load()
	rows := s.tk.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", jobID)).Rows()
	require.Greater(s.T(), len(rows), 0)
	s.tk.MustExec(fmt.Sprintf("CANCEL LOAD DATA JOB %d", jobID))
	wg.Wait()
	rows = s.tk.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d", jobID)).Rows()
	require.Len(s.T(), rows, 1)
	row := rows[0]
	e := expectedRecord{
		jobID:          strconv.Itoa(int(jobID)),
		dataSource:     "gs://test-operate/t.tsv",
		targetTable:    "`test_operate`.`t`",
		importMode:     importMode,
		createdBy:      "test-load-3@test-host",
		jobState:       "loading",
		jobStatus:      "canceled",
		sourceFileSize: row[10].(string),
		importedRowCnt: row[11].(string),
		resultCode:     "<nil>",
		resultMessage:  "canceled by user",
	}
	e.checkIgnoreTimes(s.T(), row)

	// cancel again is OK

	err := s.tk.ExecToErr(fmt.Sprintf("CANCEL LOAD DATA JOB %d", jobID))
	require.ErrorContains(s.T(), err, "The current job status cannot perform the operation. need status running or paused, but got canceled")
	rows = s.tk.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d", jobID)).Rows()
	require.Len(s.T(), rows, 1)
	row = rows[0]
	e.checkIgnoreTimes(s.T(), row)
}
