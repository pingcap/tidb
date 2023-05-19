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

package loaddatatest

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type expectedRecord struct {
	jobID          string
	dataSource     string
	targetTable    string
	importMode     string
	createdBy      string
	jobState       string
	jobStatus      string
	sourceFileSize string
	importedRowCnt string
	resultCode     string
	resultMessage  string
}

func (r *expectedRecord) checkIgnoreTimes(t *testing.T, row []interface{}) {
	require.Equal(t, r.jobID, row[0])
	require.Equal(t, r.dataSource, row[4])
	require.Equal(t, r.targetTable, row[5])
	require.Equal(t, r.importMode, row[6])
	require.Equal(t, r.createdBy, row[7])
	require.Equal(t, r.jobState, row[8])
	require.Equal(t, r.jobStatus, row[9])
	require.Equal(t, r.sourceFileSize, row[10])
	require.Equal(t, r.importedRowCnt, row[11])
	require.Equal(t, r.resultCode, row[12])
	require.Equal(t, r.resultMessage, row[13])
}

func (r *expectedRecord) check(t *testing.T, row []interface{}) {
	r.checkIgnoreTimes(t, row)
	require.NotEmpty(t, row[1])
	require.NotEmpty(t, row[2])
	require.NotEmpty(t, row[3])
}

func (s *mockGCSSuite) simpleShowLoadDataJobs(importMode string) {
	s.tk.MustExec("DROP DATABASE IF EXISTS test_show;")
	s.tk.MustExec("CREATE DATABASE test_show;")
	s.tk.MustExec("CREATE TABLE test_show.t (i INT PRIMARY KEY);")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-show",
			Name:       "t.tsv",
		},
		Content: []byte(`1
2`),
	})

	user := &auth.UserIdentity{
		AuthUsername: "test-load-2",
		AuthHostname: "test-host",
	}
	tk2 := testkit.NewTestKit(s.T(), s.store)
	tk2.Session().GetSessionVars().User = user

	backup := asyncloaddata.HeartBeatInSec
	asyncloaddata.HeartBeatInSec = 1
	s.T().Cleanup(func() {
		asyncloaddata.HeartBeatInSec = backup
	})

	resultMessage := "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"
	withOptions := "WITH thread=1, DETACHED"
	if importMode == importer.PhysicalImportMode {
		withOptions = "WITH thread=1, DETACHED, import_mode='PHYSICAL'"
	}

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-show/t.tsv?endpoint=%s'
		INTO TABLE test_show.t %s;`, gcsEndpoint, withOptions)
	rows := tk2.MustQuery(sql).Rows()
	require.Len(s.T(), rows, 1)
	row := rows[0]
	jobID := row[0].(string)

	require.Eventually(s.T(), func() bool {
		rows = tk2.MustQuery("SHOW LOAD DATA JOB " + jobID + ";").Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		return row[9] == "finished"
	}, 5*time.Second, time.Second)
	r := expectedRecord{
		jobID:          jobID,
		dataSource:     "gs://test-show/t.tsv",
		targetTable:    "`test_show`.`t`",
		importMode:     importMode,
		createdBy:      "test-load-2@test-host",
		jobState:       "loading",
		jobStatus:      "finished",
		sourceFileSize: "3B",
		importedRowCnt: "2",
		resultCode:     "0",
		resultMessage:  resultMessage,
	}
	r.check(s.T(), row)
}

func (s *mockGCSSuite) TestSimpleShowLoadDataJobs() {
	//s.simpleShowLoadDataJobs(importer.PhysicalImportMode)
	s.simpleShowLoadDataJobs(importer.LogicalImportMode)

	user := &auth.UserIdentity{
		AuthUsername: "test-load-2",
		AuthHostname: "test-host",
	}
	backupUser := s.tk.Session().GetSessionVars().User
	s.tk.Session().GetSessionVars().User = user
	s.T().Cleanup(func() {
		s.tk.Session().GetSessionVars().User = backupUser
	})
	err := s.tk.QueryToErr("SHOW LOAD DATA JOB 999999999")
	require.ErrorContains(s.T(), err, "Job ID 999999999 doesn't exist")

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-show/t.tsv?endpoint=%s'
		INTO TABLE test_show.t WITH thread=1, DETACHED;`, gcsEndpoint)
	// repeat LOAD DATA, will get duplicate entry error
	rows := s.tk.MustQuery(sql).Rows()
	require.Len(s.T(), rows, 1)
	row := rows[0]
	jobID := row[0].(string)
	require.Eventually(s.T(), func() bool {
		rows = s.tk.MustQuery("SHOW LOAD DATA JOB " + jobID + ";").Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		return row[9] == "failed"
	}, 5*time.Second, time.Second)

	r := expectedRecord{
		jobID:          jobID,
		dataSource:     "gs://test-show/t.tsv",
		targetTable:    "`test_show`.`t`",
		importMode:     "logical",
		createdBy:      "test-load-2@test-host",
		jobState:       "loading",
		jobStatus:      "failed",
		sourceFileSize: "<nil>",
		importedRowCnt: "<nil>",
		resultCode:     "1062",
		resultMessage:  "Duplicate entry '1' for key 't.PRIMARY'",
	}
	r.check(s.T(), row)

	// test IGNORE
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-show/t.tsv?endpoint=%s'
		IGNORE INTO TABLE test_show.t WITH thread=1, DETACHED;`, gcsEndpoint)
	rows = s.tk.MustQuery(sql).Rows()
	require.Len(s.T(), rows, 1)
	row = rows[0]
	jobID = row[0].(string)
	require.Eventually(s.T(), func() bool {
		rows = s.tk.MustQuery("SHOW LOAD DATA JOB " + jobID + ";").Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		return row[9] == "finished"
	}, 10*time.Second, time.Second)
	r = expectedRecord{
		jobID:          jobID,
		dataSource:     "gs://test-show/t.tsv",
		targetTable:    "`test_show`.`t`",
		importMode:     "logical",
		createdBy:      "test-load-2@test-host",
		jobState:       "loading",
		jobStatus:      "finished",
		sourceFileSize: "3B",
		importedRowCnt: "2",
		resultCode:     "0",
		resultMessage:  "Records: 2  Deleted: 0  Skipped: 2  Warnings: 2",
	}
	r.check(s.T(), row)

	// test REPLACE
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-show/t.tsv?endpoint=%s'
		REPLACE INTO TABLE test_show.t WITH thread=1, DETACHED;`, gcsEndpoint)
	rows = s.tk.MustQuery(sql).Rows()
	require.Len(s.T(), rows, 1)
	row = rows[0]
	jobID = row[0].(string)
	require.Eventually(s.T(), func() bool {
		rows = s.tk.MustQuery("SHOW LOAD DATA JOB " + jobID + ";").Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		return row[9] == "finished"
	}, 10*time.Second, time.Second)
	r.jobID = jobID
	r.resultMessage = "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"
	r.check(s.T(), row)
}

func (s *mockGCSSuite) TestInternalStatus() {
	s.testInternalStatus(importer.LogicalImportMode)
	//s.testInternalStatus(importer.PhysicalImportMode)
}

func (s *mockGCSSuite) testInternalStatus(importMode string) {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_tsv;")
	s.tk.MustExec("CREATE DATABASE load_tsv;")
	s.tk.MustExec("CREATE TABLE load_tsv.t (i INT);")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t1.tsv",
		},
		Content: []byte(`1`),
	})

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t2.tsv",
		},
		Content: []byte(`2`),
	})

	ctx := context.Background()
	user := &auth.UserIdentity{
		AuthUsername: "test-load",
		AuthHostname: "test-host",
	}
	tk3 := testkit.NewTestKit(s.T(), s.store)
	tk3.Session().GetSessionVars().User = user

	resultMessage := "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"
	withOptions := "WITH thread=1, DETACHED, batch_size=1"
	progressAfterFirstBatch := `{"SourceFileSize":2,"LoadedFileSize":1,"LoadedRowCnt":1}`
	progressAfterAll := `{"SourceFileSize":2,"LoadedFileSize":2,"LoadedRowCnt":2}`
	if importMode == importer.PhysicalImportMode {
		withOptions = fmt.Sprintf("WITH thread=1, DETACHED, import_mode='%s'", importMode)
		progressAfterFirstBatch = `{"SourceFileSize":2,"ReadRowCnt":1,"EncodeFileSize":1,"LoadedRowCnt":1}`
		progressAfterAll = `{"SourceFileSize":2,"ReadRowCnt":2,"EncodeFileSize":2,"LoadedRowCnt":2}`
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		tk2 := testkit.NewTestKit(s.T(), s.store)
		tk2.Session().GetSessionVars().User = user
		userStr := tk2.Session().GetSessionVars().User.String()

		// wait for the load data job to be created
		<-asyncloaddata.TestSyncCh

		id := asyncloaddata.TestLastLoadDataJobID.Load()
		expected := &asyncloaddata.JobInfo{
			JobID:         id,
			User:          "test-load@test-host",
			DataSource:    "gs://test-tsv/t*.tsv",
			TableSchema:   "load_tsv",
			TableName:     "t",
			ImportMode:    importMode,
			Progress:      "",
			Status:        asyncloaddata.JobPending,
			StatusMessage: "",
		}

		rows := tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row := rows[0]
		r := expectedRecord{
			jobID:          strconv.Itoa(int(id)),
			dataSource:     "gs://test-tsv/t*.tsv",
			targetTable:    "`load_tsv`.`t`",
			importMode:     importMode,
			createdBy:      "test-load@test-host",
			jobState:       "loading",
			jobStatus:      "pending",
			sourceFileSize: "<nil>",
			importedRowCnt: "<nil>",
			resultCode:     "<nil>",
			resultMessage:  "",
		}
		r.checkIgnoreTimes(s.T(), row)

		// resume the load data job
		asyncloaddata.TestSyncCh <- struct{}{}

		// wait for the load data job to be started
		<-asyncloaddata.TestSyncCh

		job := &asyncloaddata.Job{
			ID:   id,
			Conn: tk2.Session(),
			User: userStr,
		}

		info, err := job.GetJobInfo(ctx)
		require.NoError(s.T(), err)
		expected.CreateTime = info.CreateTime
		expected.StartTime = info.StartTime
		expected.EndTime = info.EndTime
		expected.Status = asyncloaddata.JobRunning
		require.Equal(s.T(), expected, info)

		rows = tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		r.jobStatus = "running"
		r.checkIgnoreTimes(s.T(), row)

		// resume the load data job
		asyncloaddata.TestSyncCh <- struct{}{}

		// wait for the first task to be committed
		<-importer.TestSyncCh

		// wait for UpdateJobProgress
		require.Eventually(s.T(), func() bool {
			info, err = job.GetJobInfo(ctx)
			if err != nil {
				return false
			}
			return info.Progress == progressAfterFirstBatch
		}, 6*time.Second, time.Millisecond*100)
		info, err = job.GetJobInfo(ctx)
		require.NoError(s.T(), err)
		expected.Progress = progressAfterFirstBatch
		require.Equal(s.T(), expected, info)

		rows = tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		r.sourceFileSize = "2B"
		r.importedRowCnt = "1"
		r.checkIgnoreTimes(s.T(), row)

		// resume the load data job
		importer.TestSyncCh <- struct{}{}

		// wait for the second task to be committed
		<-importer.TestSyncCh

		// wait for UpdateJobProgress
		require.Eventually(s.T(), func() bool {
			info, err = job.GetJobInfo(ctx)
			if err != nil {
				return false
			}
			return info.Progress == progressAfterAll
		}, 6*time.Second, time.Millisecond*100)

		rows = tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		r.importedRowCnt = "2"
		r.checkIgnoreTimes(s.T(), row)

		// resume the load data job
		importer.TestSyncCh <- struct{}{}

		require.Eventually(s.T(), func() bool {
			info, err = job.GetJobInfo(ctx)
			if err != nil {
				return false
			}
			return info.Status == asyncloaddata.JobFinished
		}, 6*time.Second, 100*time.Millisecond)

		info, err = job.GetJobInfo(ctx)
		require.NoError(s.T(), err)
		expected.Status = asyncloaddata.JobFinished
		expected.EndTime = info.EndTime
		expected.StatusMessage = resultMessage
		expected.Progress = progressAfterAll
		require.Equal(s.T(), expected, info)

		rows = tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		r.jobStatus = "finished"
		r.resultCode = "0"
		r.resultMessage = resultMessage
		r.checkIgnoreTimes(s.T(), row)
	}()

	backup := asyncloaddata.HeartBeatInSec
	asyncloaddata.HeartBeatInSec = 1
	s.T().Cleanup(func() {
		asyncloaddata.HeartBeatInSec = backup
	})
	backup2 := importer.LoadDataReadBlockSize
	importer.LoadDataReadBlockSize = 1
	s.T().Cleanup(func() {
		importer.LoadDataReadBlockSize = backup2
	})
	backup3 := config.BufferSizeScale
	config.BufferSizeScale = 1
	s.T().Cleanup(func() {
		config.BufferSizeScale = backup3
	})
	backup4 := config.DefaultBatchSize
	config.DefaultBatchSize = 1
	s.T().Cleanup(func() {
		config.DefaultBatchSize = backup4
	})

	s.enableFailpoint("github.com/pingcap/tidb/executor/asyncloaddata/SaveLastLoadDataJobID", `return`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/asyncloaddata/SyncAfterCreateLoadDataJob", `return`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/asyncloaddata/SyncAfterStartJob", `return`)
	if importMode == importer.LogicalImportMode {
		s.enableFailpoint("github.com/pingcap/tidb/executor/SyncAfterCommitOneTask", `return`)
	} else {
		s.enableFailpoint("github.com/pingcap/tidb/executor/importer/SyncAfterImportDataEngine", `return`)
	}
	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t*.tsv?endpoint=%s'
		INTO TABLE load_tsv.t %s;`, gcsEndpoint, withOptions)
	tk3.MustQuery(sql)
	wg.Wait()
}
