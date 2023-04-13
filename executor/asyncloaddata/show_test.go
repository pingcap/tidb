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
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/executor"
	. "github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type mockGCSSuite struct {
	suite.Suite

	server *fakestorage.Server
	store  kv.Storage
	tk     *testkit.TestKit
}

var (
	gcsHost = "127.0.0.1"
	gcsPort = uint16(4444)
	// for fake gcs server, we must use this endpoint format
	// NOTE: must end with '/'
	gcsEndpointFormat = "http://%s:%d/storage/v1/"
	gcsEndpoint       = fmt.Sprintf(gcsEndpointFormat, gcsHost, gcsPort)
)

func TestAsyncLoad(t *testing.T) {
	suite.Run(t, &mockGCSSuite{})
}

func (s *mockGCSSuite) SetupSuite() {
	var err error
	opt := fakestorage.Options{
		Scheme:     "http",
		Host:       gcsHost,
		Port:       gcsPort,
		PublicHost: gcsHost,
	}
	s.server, err = fakestorage.NewServerWithOptions(opt)
	s.Require().NoError(err)
	s.store = testkit.CreateMockStore(s.T(), mockstore.WithStoreType(mockstore.EmbedUnistore))
	s.tk = testkit.NewTestKit(s.T(), s.store)
}

func (s *mockGCSSuite) TearDownSuite() {
	s.server.Stop()
}

func (s *mockGCSSuite) enableFailpoint(path, term string) {
	require.NoError(s.T(), failpoint.Enable(path, term))
	s.T().Cleanup(func() {
		_ = failpoint.Disable(path)
	})
}

type expectedRecord struct {
	jobID          string
	dataSource     string
	targetTable    string
	importMode     string
	createdBy      string
	jobState       string
	jobStatus      string
	sourceFileSize string
	loadedFileSize string
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
	require.Equal(t, r.loadedFileSize, row[11])
	require.Equal(t, r.resultCode, row[12])
	require.Equal(t, r.resultMessage, row[13])
}

func (r *expectedRecord) check(t *testing.T, row []interface{}) {
	r.checkIgnoreTimes(t, row)
	require.NotEmpty(t, row[1])
	require.NotEmpty(t, row[2])
	require.NotEmpty(t, row[3])
}

func (s *mockGCSSuite) TestSimpleShowLoadDataJobs() {
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
	s.tk.Session().GetSessionVars().User = user

	backup := HeartBeatInSec
	HeartBeatInSec = 1
	s.T().Cleanup(func() {
		HeartBeatInSec = backup
	})

	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-show/t.tsv?endpoint=%s'
		INTO TABLE test_show.t WITH DETACHED;`, gcsEndpoint)
	rows := s.tk.MustQuery(sql).Rows()
	require.Len(s.T(), rows, 1)
	row := rows[0]
	jobID := row[0].(string)

	require.Eventually(s.T(), func() bool {
		rows = s.tk.MustQuery("SHOW LOAD DATA JOB " + jobID + ";").Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		return row[9] == "finished"
	}, 5*time.Second, time.Second)
	r := expectedRecord{
		jobID:          jobID,
		dataSource:     "gs://test-show/t.tsv",
		targetTable:    "`test_show`.`t`",
		importMode:     "logical",
		createdBy:      "test-load-2@test-host",
		jobState:       "loading",
		jobStatus:      "finished",
		sourceFileSize: "3B",
		loadedFileSize: "3B",
		resultCode:     "0",
		resultMessage:  "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0",
	}
	r.check(s.T(), row)

	err := s.tk.QueryToErr("SHOW LOAD DATA JOB 100")
	require.ErrorContains(s.T(), err, "Job ID 100 doesn't exist")

	// repeat LOAD DATA, will get duplicate entry error
	rows = s.tk.MustQuery(sql).Rows()
	require.Len(s.T(), rows, 1)
	row = rows[0]
	jobID = row[0].(string)
	require.Eventually(s.T(), func() bool {
		rows = s.tk.MustQuery("SHOW LOAD DATA JOB " + jobID + ";").Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		return row[9] == "failed"
	}, 5*time.Second, time.Second)

	r.jobID = jobID
	r.jobStatus = "failed"
	r.sourceFileSize = "<nil>"
	r.loadedFileSize = "<nil>"
	r.resultCode = "1062"
	r.resultMessage = "Duplicate entry '1' for key 't.PRIMARY'"
	r.check(s.T(), row)

	// test IGNORE
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-show/t.tsv?endpoint=%s'
		IGNORE INTO TABLE test_show.t WITH DETACHED;`, gcsEndpoint)
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
		loadedFileSize: "3B",
		resultCode:     "0",
		resultMessage:  "Records: 2  Deleted: 0  Skipped: 2  Warnings: 2",
	}
	r.check(s.T(), row)

	// test REPLACE
	sql = fmt.Sprintf(`LOAD DATA INFILE 'gs://test-show/t.tsv?endpoint=%s'
		REPLACE INTO TABLE test_show.t WITH DETACHED;`, gcsEndpoint)
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
	s.tk.Session().GetSessionVars().User = user

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(executor.TestSyncCh)

		tk2 := testkit.NewTestKit(s.T(), s.store)
		tk2.Session().GetSessionVars().User = user
		userStr := tk2.Session().GetSessionVars().User.String()

		// wait for the load data job to be created
		<-executor.TestSyncCh

		jobInfos, err := GetAllJobInfo(ctx, tk2.Session(), userStr)
		require.NoError(s.T(), err)
		require.Len(s.T(), jobInfos, 1)
		info := jobInfos[0]
		id := info.JobID
		expected := &JobInfo{
			JobID:         id,
			User:          "test-load@test-host",
			DataSource:    "gs://test-tsv/t*.tsv",
			TableSchema:   "load_tsv",
			TableName:     "t",
			ImportMode:    "logical",
			Progress:      "",
			Status:        JobPending,
			StatusMessage: "",
			CreateTime:    info.CreateTime,
			StartTime:     info.StartTime,
			EndTime:       info.EndTime,
		}
		require.Equal(s.T(), expected, info)

		rows := tk2.MustQuery("SHOW LOAD DATA JOBS;").Rows()
		require.Len(s.T(), rows, 1)
		row := rows[0]
		r := expectedRecord{
			jobID:          strconv.Itoa(int(id)),
			dataSource:     "gs://test-tsv/t*.tsv",
			targetTable:    "`load_tsv`.`t`",
			importMode:     "logical",
			createdBy:      "test-load@test-host",
			jobState:       "loading",
			jobStatus:      "pending",
			sourceFileSize: "<nil>",
			loadedFileSize: "<nil>",
			resultCode:     "<nil>",
			resultMessage:  "",
		}
		r.checkIgnoreTimes(s.T(), row)

		// resume the load data job
		executor.TestSyncCh <- struct{}{}

		// wait for the load data job to be started
		<-executor.TestSyncCh

		info, err = GetJobInfo(ctx, tk2.Session(), id, userStr)
		require.NoError(s.T(), err)
		expected.StartTime = info.StartTime
		expected.Status = JobRunning
		require.Equal(s.T(), expected, info)

		rows = tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		r.jobStatus = "running"
		r.checkIgnoreTimes(s.T(), row)

		// resume the load data job
		executor.TestSyncCh <- struct{}{}

		// wait for the first task to be committed
		<-executor.TestSyncCh

		// wait for UpdateJobProgress
		require.Eventually(s.T(), func() bool {
			info, err = GetJobInfo(ctx, tk2.Session(), id, userStr)
			if err != nil {
				return false
			}
			return info.Progress == `{"SourceFileSize":2,"LoadedFileSize":1,"LoadedRowCnt":1}`
		}, 6*time.Second, time.Millisecond*100)
		info, err = GetJobInfo(ctx, tk2.Session(), id, userStr)
		require.NoError(s.T(), err)
		expected.Progress = `{"SourceFileSize":2,"LoadedFileSize":1,"LoadedRowCnt":1}`
		require.Equal(s.T(), expected, info)

		rows = tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		r.sourceFileSize = "2B"
		r.loadedFileSize = "1B"
		r.checkIgnoreTimes(s.T(), row)

		// resume the load data job
		executor.TestSyncCh <- struct{}{}

		// wait for the second task to be committed
		<-executor.TestSyncCh

		// wait for UpdateJobProgress
		require.Eventually(s.T(), func() bool {
			info, err = GetJobInfo(ctx, tk2.Session(), id, userStr)
			if err != nil {
				return false
			}
			return info.Progress == `{"SourceFileSize":2,"LoadedFileSize":2,"LoadedRowCnt":2}`
		}, 6*time.Second, time.Millisecond*100)

		rows = tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		r.loadedFileSize = "2B"
		r.checkIgnoreTimes(s.T(), row)

		// resume the load data job
		executor.TestSyncCh <- struct{}{}

		require.Eventually(s.T(), func() bool {
			info, err = GetJobInfo(ctx, tk2.Session(), id, userStr)
			if err != nil {
				return false
			}
			return info.Status == JobFinished
		}, 6*time.Second, 100*time.Millisecond)

		info, err = GetJobInfo(ctx, tk2.Session(), id, userStr)
		require.NoError(s.T(), err)
		expected.Status = JobFinished
		expected.EndTime = info.EndTime
		expected.StatusMessage = "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"
		expected.Progress = `{"SourceFileSize":2,"LoadedFileSize":2,"LoadedRowCnt":2}`
		require.Equal(s.T(), expected, info)

		rows = tk2.MustQuery(fmt.Sprintf("SHOW LOAD DATA JOB %d;", id)).Rows()
		require.Len(s.T(), rows, 1)
		row = rows[0]
		r.jobStatus = "finished"
		r.resultCode = "0"
		r.resultMessage = "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"
		r.checkIgnoreTimes(s.T(), row)
	}()

	backup := HeartBeatInSec
	HeartBeatInSec = 1
	s.T().Cleanup(func() {
		HeartBeatInSec = backup
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

	s.enableFailpoint("github.com/pingcap/tidb/executor/SyncAfterCreateLoadDataJob", `return`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/SyncAfterStartJob", `return`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/SyncAfterCommitOneTask", `return`)
	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t*.tsv?endpoint=%s'
		INTO TABLE load_tsv.t WITH batch_size = 1;`, gcsEndpoint)
	s.tk.MustExec(sql)
	wg.Wait()
}
