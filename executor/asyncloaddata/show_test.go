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
	"sync"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/executor"
	. "github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
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
	s.store = testkit.CreateMockStore(s.T())
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

func (s *mockGCSSuite) TestInternalStatus() {
	s.tk.MustExec("DROP DATABASE IF EXISTS load_tsv;")
	s.tk.MustExec("CREATE DATABASE load_tsv;")
	s.tk.MustExec("CREATE TABLE load_tsv.t (i INT);")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "test-tsv",
			Name:       "t.tsv",
		},
		Content: []byte(`1
2`),
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
		tk2 := testkit.NewTestKit(s.T(), s.store)
		tk2.Session().GetSessionVars().User = user
		// tk @ 0:00
		// create load data job record in the system table and sleep 3 seconds
		time.Sleep(2 * time.Second)
		// tk2 @ 0:02
		jobInfos, err := GetAllJobInfo(ctx, tk2.Session(), tk2.Session().GetSessionVars().User.String())
		require.NoError(s.T(), err)
		require.Len(s.T(), jobInfos, 1)
		info := jobInfos[0]
		id := info.JobID
		expected := &JobInfo{
			JobID:         id,
			User:          "test-load@test-host",
			DataSource:    fmt.Sprintf("gs://test-tsv/t.tsv?endpoint=%s", gcsEndpoint),
			TableSchema:   "load_tsv",
			TableName:     "t",
			ImportMode:    "logical",
			Progress:      "",
			Status:        JobPending,
			StatusMessage: "",
		}
		require.Equal(s.T(), expected, info)
		// tk @ 0:03
		// start job and sleep 3 seconds
		time.Sleep(3 * time.Second)
		// tk2 @ 0:05
		info, err = GetJobInfo(ctx, tk2.Session(), id)
		require.NoError(s.T(), err)
		expected.Status = JobRunning
		require.Equal(s.T(), expected, info)
		// tk @ 0:06
		// commit one task and sleep 3 seconds
		time.Sleep(3 * time.Second)
		// tk2 @ 0:08
		info, err = GetJobInfo(ctx, tk2.Session(), id)
		require.NoError(s.T(), err)
		expected.Progress = `{"SourceFileSize":3,"LoadedFileSize":0,"LoadedRowCnt":1}`
		require.Equal(s.T(), expected, info)
		// tk @ 0:09
		// commit one task and sleep 3 seconds
		time.Sleep(3 * time.Second)
		// tk2 @ 0:11
		info, err = GetJobInfo(ctx, tk2.Session(), id)
		require.NoError(s.T(), err)
		expected.Progress = `{"SourceFileSize":3,"LoadedFileSize":2,"LoadedRowCnt":2}`
		require.Equal(s.T(), expected, info)
		// tk @ 0:12
		// finish job
		time.Sleep(3 * time.Second)
		// tk2 @ 0:14
		info, err = GetJobInfo(ctx, tk2.Session(), id)
		require.NoError(s.T(), err)
		expected.Status = JobFinished
		expected.StatusMessage = "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"
		expected.Progress = `{"SourceFileSize":3,"LoadedFileSize":3,"LoadedRowCnt":2}`
		require.Equal(s.T(), expected, info)
	}()

	backup := HeartBeatInSec
	HeartBeatInSec = 1
	s.T().Cleanup(func() {
		HeartBeatInSec = backup
	})
	backup2 := executor.LoadDataReadBlockSize
	executor.LoadDataReadBlockSize = 1
	s.T().Cleanup(func() {
		executor.LoadDataReadBlockSize = backup2
	})
	backup3 := config.BufferSizeScale
	config.BufferSizeScale = 1
	s.T().Cleanup(func() {
		config.BufferSizeScale = backup3
	})

	s.enableFailpoint("github.com/pingcap/tidb/executor/AfterCreateLoadDataJob", `sleep(3000)`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/AfterStartJob", `sleep(3000)`)
	s.enableFailpoint("github.com/pingcap/tidb/executor/AfterCommitOneTask", `sleep(3000)`)
	s.tk.MustExec("SET SESSION tidb_dml_batch_size = 1;")
	sql := fmt.Sprintf(`LOAD DATA INFILE 'gs://test-tsv/t.tsv?endpoint=%s'
		INTO TABLE load_tsv.t;`, gcsEndpoint)
	s.tk.MustExec(sql)
	wg.Wait()
}
