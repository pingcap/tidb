// Copyright 2025 PingCAP, Inc.
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

package importintotest

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func (s *mockGCSSuite) TestImportSDKSubmitAndShow() {
	s.prepareAndUseDB("sdk_show_job")
	s.tk.MustExec("create table t (id int primary key, v varchar(10));")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "sdk-show-job", Name: "data.csv"},
		Content:     []byte("1,alpha\n2,beta"),
	})

	db := s.newTestKitSQLDB("sdk_show_job")
	s.T().Cleanup(func() {
		s.Require().NoError(db.Close())
	})

	ctx := context.Background()
	sourcePath := fmt.Sprintf("gs://sdk-show-job/*.csv?endpoint=%s", gcsEndpoint)
	sdk, err := importsdk.NewImportSDK(context.Background(), sourcePath, db)
	require.NoError(s.T(), err)
	importSQL := fmt.Sprintf("import into t FROM 'gs://sdk-show-job/*.csv?endpoint=%s'", gcsEndpoint)
	job, err := sdk.SubmitImportJob(ctx, importSQL)
	s.Require().NoError(err)
	s.Require().NotNil(job)
	s.Require().True(job.Status.Valid)
	s.Equal("finished", job.Status.String)
	s.Require().True(job.ImportedRows.Valid)
	s.Equal(int64(2), job.ImportedRows.Int64)

	fetched, err := sdk.FetchImportJob(ctx, job.JobID)
	s.Require().NoError(err)
	s.Require().NotNil(fetched)
	s.Equal(job.JobID, fetched.JobID)
	s.Require().True(fetched.Status.Valid)
	s.Equal(job.Status.String, fetched.Status.String)

	jobs, err := sdk.FetchImportJobs(ctx)
	s.Require().NoError(err)
	var matched bool
	for _, j := range jobs {
		if j.JobID == job.JobID {
			matched = true
			s.Require().True(j.Status.Valid)
			s.Equal("finished", j.Status.String)
			break
		}
	}
	s.True(matched)

	s.tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 alpha", "2 beta"))
}

func (s *mockGCSSuite) TestImportSDKCancelJob() {
	s.prepareAndUseDB("sdk_cancel_job")
	s.tk.MustExec("create table t (id int primary key);")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "sdk-cancel-job", Name: "data.csv"},
		Content:     []byte("1\n2\n3"),
	})

	db := s.newTestKitSQLDB("sdk_cancel_job")
	s.T().Cleanup(func() {
		s.Require().NoError(db.Close())
	})

	ctx := context.Background()
	syncCh := make(chan struct{})
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/beforeSortChunk", "sleep(3000)")
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/syncAfterJobStarted", func() {
		close(syncCh)
	})

	sourcePath := fmt.Sprintf("gs://sdk-cancel-job/*.csv?endpoint=%s", gcsEndpoint)
	sdk, err := importsdk.NewImportSDK(context.Background(), sourcePath, db)
	s.Require().NoError(err)
	importSQL := fmt.Sprintf("import into t FROM 'gs://sdk-cancel-job/*.csv?endpoint=%s' with detached", gcsEndpoint)
	job, err := sdk.SubmitImportJob(ctx, importSQL)
	s.Require().NoError(err)
	s.Require().NotNil(job)

	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		s.T().Fatal("import job did not start in time")
	}

	s.Require().NoError(sdk.CancelImportJob(ctx, job.JobID))
	s.Require().Eventually(func() bool {
		current, err2 := sdk.FetchImportJob(ctx, job.JobID)
		if err2 != nil {
			return false
		}
		return current.Status.Valid && current.Status.String == "cancelled"
	}, maxWaitTime, 500*time.Millisecond)
}

func (s *mockGCSSuite) newTestKitSQLDB(dbName string) *sql.DB {
	return testkit.NewTestKitSQLDB(func() *testkit.TestKit {
		tk := testkit.NewTestKit(s.T(), s.store)
		tk.MustExec("use " + dbName)
		return tk
	})
}
