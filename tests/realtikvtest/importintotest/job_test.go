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

package importintotest

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/tikv/client-go/v2/util"
)

func (s *mockGCSSuite) compareJobInfoWithoutTime(jobInfo *importer.JobInfo, row []any) {
	s.Equal(strconv.Itoa(int(jobInfo.ID)), row[0])

	urlExpected, err := url.Parse(jobInfo.Parameters.FileLocation)
	s.NoError(err)
	urlGot, err := url.Parse(fmt.Sprintf("%v", row[1]))
	s.NoError(err)
	// order of query parameters might change
	s.Equal(urlExpected.Query(), urlGot.Query())
	urlExpected.RawQuery, urlGot.RawQuery = "", ""
	s.Equal(urlExpected.String(), urlGot.String())

	s.Equal(utils.EncloseDBAndTable(jobInfo.TableSchema, jobInfo.TableName), row[2])
	s.Equal(strconv.Itoa(int(jobInfo.TableID)), row[3])
	s.Equal(jobInfo.Step, row[4])
	s.Equal(jobInfo.Status, row[5])
	s.Equal(units.BytesSize(float64(jobInfo.SourceFileSize)), row[6])
	if jobInfo.Summary == nil {
		s.Equal("<nil>", row[7].(string))
	} else {
		s.Equal(strconv.Itoa(int(jobInfo.Summary.ImportedRows)), row[7])
	}
	s.Regexp(jobInfo.ErrorMessage, row[8])
	s.Equal(jobInfo.CreatedBy, row[12])
}

func (s *mockGCSSuite) TestShowJob() {
	s.tk.MustExec("delete from mysql.tidb_import_jobs")
	s.prepareAndUseDB("test_show_job")
	s.tk.MustExec("CREATE TABLE t1 (i INT PRIMARY KEY);")
	s.tk.MustExec("CREATE TABLE t2 (i INT PRIMARY KEY);")
	s.tk.MustExec("CREATE TABLE t3 (i INT PRIMARY KEY);")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-show-job", Name: "t.csv"},
		Content:     []byte("1\n2"),
	})
	s.T().Cleanup(func() {
		_ = s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil)
	})
	// create 2 user which don't have system table privileges
	s.tk.MustExec(`DROP USER IF EXISTS 'test_show_job1'@'localhost';`)
	s.tk.MustExec(`CREATE USER 'test_show_job1'@'localhost';`)
	s.tk.MustExec(`GRANT SELECT,UPDATE,INSERT,DELETE,ALTER on test_show_job.* to 'test_show_job1'@'localhost'`)
	s.tk.MustExec(`DROP USER IF EXISTS 'test_show_job2'@'localhost';`)
	s.tk.MustExec(`CREATE USER 'test_show_job2'@'localhost';`)
	s.tk.MustExec(`GRANT SELECT,UPDATE,INSERT,DELETE,ALTER on test_show_job.* to 'test_show_job2'@'localhost'`)
	do, err := session.GetDomain(s.store)
	s.NoError(err)
	tableID1 := do.MustGetTableID(s.T(), "test_show_job", "t1")
	tableID2 := do.MustGetTableID(s.T(), "test_show_job", "t2")
	tableID3 := do.MustGetTableID(s.T(), "test_show_job", "t3")

	// show non-exists job
	err = s.tk.QueryToErr("show import job 9999999999")
	s.ErrorIs(err, exeerrors.ErrLoadDataJobNotFound)

	// test show job by id using test_show_job1
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/executor/importer/setLastImportJobID", `return(true)`)
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/storage/testSetLastTaskID", "return(true)")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/parser/ast/forceRedactURL", "return(true)")
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_show_job1", Hostname: "localhost"}, nil, nil, nil))
	result1 := s.tk.MustQuery(fmt.Sprintf(`import into t1 FROM 'gs://test-show-job/t.csv?access-key=aaaaaa&secret-access-key=bbbbbb&endpoint=%s'`,
		gcsEndpoint)).Rows()
	s.Len(result1, 1)
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1", "2"))
	rows := s.tk.MustQuery(fmt.Sprintf("show import job %d", importer.TestLastImportJobID.Load())).Rows()
	s.Len(rows, 1)
	s.Equal(result1, rows)
	jobInfo := &importer.JobInfo{
		ID:          importer.TestLastImportJobID.Load(),
		TableSchema: "test_show_job",
		TableName:   "t1",
		TableID:     tableID1,
		CreatedBy:   "test_show_job1@localhost",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://test-show-job/t.csv?access-key=xxxxxx&secret-access-key=xxxxxx&endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "finished",
		Step:           "",
		Summary: &importer.JobSummary{
			ImportedRows: 2,
		},
		ErrorMessage: "",
	}
	s.compareJobInfoWithoutTime(jobInfo, rows[0])

	// test show job by id using test_show_job2
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_show_job2", Hostname: "localhost"}, nil, nil, nil))
	result2 := s.tk.MustQuery(fmt.Sprintf(`import into t2 FROM 'gs://test-show-job/t.csv?endpoint=%s'`, gcsEndpoint)).Rows()
	s.tk.MustQuery("select * from t2").Check(testkit.Rows("1", "2"))
	rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", importer.TestLastImportJobID.Load())).Rows()
	s.Len(rows, 1)
	s.Equal(result2, rows)
	jobInfo.ID = importer.TestLastImportJobID.Load()
	jobInfo.TableName = "t2"
	jobInfo.TableID = tableID2
	jobInfo.CreatedBy = "test_show_job2@localhost"
	jobInfo.Parameters.FileLocation = fmt.Sprintf(`gs://test-show-job/t.csv?endpoint=%s`, gcsEndpoint)
	s.compareJobInfoWithoutTime(jobInfo, rows[0])
	rows = s.tk.MustQuery("show import jobs").Rows()
	s.Len(rows, 1)
	s.Equal(result2, rows)

	// show import jobs with root
	checkJobsMatch := func(rows [][]any) {
		s.GreaterOrEqual(len(rows), 2) // other cases may create import jobs
		var matched int
		for _, r := range rows {
			if r[0] == result1[0][0] {
				s.Equal(result1[0], r)
				matched++
			}
			if r[0] == result2[0][0] {
				s.Equal(result2[0], r)
				matched++
			}
		}
		s.Equal(2, matched)
	}
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	rows = s.tk.MustQuery("show import jobs").Rows()
	checkJobsMatch(rows)
	// show import job by id with root
	rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", importer.TestLastImportJobID.Load())).Rows()
	s.Len(rows, 1)
	s.Equal(result2, rows)
	jobInfo.ID = importer.TestLastImportJobID.Load()
	jobInfo.TableName = "t2"
	jobInfo.TableID = tableID2
	jobInfo.CreatedBy = "test_show_job2@localhost"
	jobInfo.Parameters.FileLocation = fmt.Sprintf(`gs://test-show-job/t.csv?endpoint=%s`, gcsEndpoint)
	s.compareJobInfoWithoutTime(jobInfo, rows[0])

	// grant SUPER to test_show_job2, now it can see all jobs
	s.tk.MustExec(`GRANT SUPER on *.* to 'test_show_job2'@'localhost'`)
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_show_job2", Hostname: "localhost"}, nil, nil, nil))
	rows = s.tk.MustQuery("show import jobs").Rows()
	checkJobsMatch(rows)

	// show running jobs with 2 subtasks
	var counter atomic.Int32
	tk2 := testkit.NewTestKit(s.T(), s.store)
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/syncAfterSubtaskFinish",
		func() {
			newVal := counter.Add(1)
			if newVal == 1 {
				jobInfo = &importer.JobInfo{
					ID:          importer.TestLastImportJobID.Load(),
					TableSchema: "test_show_job",
					TableName:   "t3",
					TableID:     tableID3,
					CreatedBy:   "test_show_job2@localhost",
					Parameters: importer.ImportParameters{
						FileLocation: fmt.Sprintf(`gs://test-show-job/t*.csv?access-key=xxxxxx&secret-access-key=xxxxxx&endpoint=%s`, gcsEndpoint),
						Format:       importer.DataFormatCSV,
					},
					SourceFileSize: 6,
					Status:         "running",
					Step:           "importing",
					Summary: &importer.JobSummary{
						ImportedRows: 2,
					},
					ErrorMessage: "",
				}
				rows = tk2.MustQuery(fmt.Sprintf("show import job %d", importer.TestLastImportJobID.Load())).Rows()
				s.Len(rows, 1)
				s.compareJobInfoWithoutTime(jobInfo, rows[0])
				// show processlist, should be redacted too
				procRows := tk2.MustQuery("show full processlist").Rows()

				var got bool
				for _, r := range procRows {
					user := r[1].(string)
					sql := r[7].(string)
					if user == "test_show_job2" && strings.Contains(sql, "IMPORT INTO") {
						s.Contains(sql, "access-key=xxxxxx")
						s.Contains(sql, "secret-access-key=xxxxxx")
						s.NotContains(sql, "aaaaaa")
						s.NotContains(sql, "bbbbbb")
						got = true
					}
				}
				s.True(got)
			} else if newVal == 2 {
				rows = tk2.MustQuery(fmt.Sprintf("show import job %d", importer.TestLastImportJobID.Load())).Rows()
				s.Len(rows, 1)
				jobInfo.Summary.ImportedRows = 4
				s.compareJobInfoWithoutTime(jobInfo, rows[0])
				// resume the taskexecutor, need disable failpoint first, otherwise the post-process subtask will be blocked
				s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/syncAfterSubtaskFinish"))
			}
		},
	)
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-show-job", Name: "t2.csv"},
		Content:     []byte("3\n4"),
	})
	s.tk.MustQuery(fmt.Sprintf(`import into t3 FROM 'gs://test-show-job/t*.csv?access-key=aaaaaa&secret-access-key=bbbbbb&endpoint=%s' with thread=1, __max_engine_size='1'`, gcsEndpoint))
	s.tk.MustQuery("select * from t3").Sort().Check(testkit.Rows("1", "2", "3", "4"))
}

func (s *mockGCSSuite) TestShowDetachedJob() {
	s.prepareAndUseDB("show_detached_job")
	s.tk.MustExec("CREATE TABLE t1 (i INT PRIMARY KEY);")
	s.tk.MustExec("CREATE TABLE t2 (i INT PRIMARY KEY);")
	s.tk.MustExec("CREATE TABLE t3 (i INT PRIMARY KEY);")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-show-detached-job", Name: "t.csv"},
		Content:     []byte("1\n2"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-show-detached-job", Name: "t2.csv"},
		Content:     []byte("1\n1"),
	})
	do, err := session.GetDomain(s.store)
	s.NoError(err)
	tableID1 := do.MustGetTableID(s.T(), "show_detached_job", "t1")
	tableID2 := do.MustGetTableID(s.T(), "show_detached_job", "t2")
	tableID3 := do.MustGetTableID(s.T(), "show_detached_job", "t3")

	jobInfo := &importer.JobInfo{
		TableSchema: "show_detached_job",
		TableName:   "t1",
		TableID:     tableID1,
		CreatedBy:   "root@%",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://test-show-detached-job/t.csv?endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "pending",
		Step:           "",
	}
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	result1 := s.tk.MustQuery(fmt.Sprintf(`import into t1 FROM 'gs://test-show-detached-job/t.csv?endpoint=%s' with detached`,
		gcsEndpoint)).Rows()
	s.Len(result1, 1)
	jobID1, err := strconv.Atoi(result1[0][0].(string))
	s.NoError(err)
	jobInfo.ID = int64(jobID1)
	s.compareJobInfoWithoutTime(jobInfo, result1[0])

	s.Require().Eventually(func() bool {
		rows := s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID1)).Rows()
		return rows[0][5] == "finished"
	}, maxWaitTime, 500*time.Millisecond)
	rows := s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID1)).Rows()
	s.Len(rows, 1)
	jobInfo.Status = "finished"
	jobInfo.Summary = &importer.JobSummary{
		ImportedRows: 2,
	}
	s.compareJobInfoWithoutTime(jobInfo, rows[0])
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1", "2"))

	// job fail with checksum mismatch
	result2 := s.tk.MustQuery(fmt.Sprintf(`import into t2 FROM 'gs://test-show-detached-job/t2.csv?endpoint=%s' with detached`,
		gcsEndpoint)).Rows()
	s.Len(result2, 1)
	jobID2, err := strconv.Atoi(result2[0][0].(string))
	s.NoError(err)
	jobInfo = &importer.JobInfo{
		ID:          int64(jobID2),
		TableSchema: "show_detached_job",
		TableName:   "t2",
		TableID:     tableID2,
		CreatedBy:   "root@%",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://test-show-detached-job/t2.csv?endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "pending",
		Step:           "",
	}
	s.compareJobInfoWithoutTime(jobInfo, result2[0])
	s.Require().Eventually(func() bool {
		rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID2)).Rows()
		return rows[0][5] == "failed"
	}, maxWaitTime, 500*time.Millisecond)
	rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID2)).Rows()
	s.Len(rows, 1)
	jobInfo.Status = "failed"
	jobInfo.Step = importer.JobStepValidating
	jobInfo.ErrorMessage = `\[Lighting:Restore:ErrChecksumMismatch]checksum mismatched remote vs local.*`
	s.compareJobInfoWithoutTime(jobInfo, rows[0])

	// subtask fail with error
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/errorWhenSortChunk", "return(true)")
	result3 := s.tk.MustQuery(fmt.Sprintf(`import into t3 FROM 'gs://test-show-detached-job/t.csv?endpoint=%s' with detached`,
		gcsEndpoint)).Rows()
	s.Len(result3, 1)
	jobID3, err := strconv.Atoi(result3[0][0].(string))
	s.NoError(err)
	jobInfo = &importer.JobInfo{
		ID:          int64(jobID3),
		TableSchema: "show_detached_job",
		TableName:   "t3",
		TableID:     tableID3,
		CreatedBy:   "root@%",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://test-show-detached-job/t.csv?endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "pending",
		Step:           "",
	}
	s.compareJobInfoWithoutTime(jobInfo, result3[0])
	s.Require().Eventually(func() bool {
		rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID3)).Rows()
		return rows[0][5] == "failed"
	}, maxWaitTime, 500*time.Millisecond)
	rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID3)).Rows()
	s.Len(rows, 1)
	jobInfo.Status = "failed"
	jobInfo.Step = importer.JobStepImporting
	jobInfo.ErrorMessage = `occur an error when sort chunk.*`
	s.compareJobInfoWithoutTime(jobInfo, rows[0])
}

func (s *mockGCSSuite) getTaskByJobID(ctx context.Context, jobID int64) *proto.Task {
	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	taskKey := importinto.TaskKey(jobID)
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	s.NoError(err)
	return task
}

func (s *mockGCSSuite) TestCancelJob() {
	s.prepareAndUseDB("test_cancel_job")
	s.tk.MustExec("CREATE TABLE t1 (i INT PRIMARY KEY);")
	s.tk.MustExec("CREATE TABLE t2 (i INT PRIMARY KEY);")
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test_cancel_job", Name: "t.csv"},
		Content:     []byte("1\n2"),
	})
	s.T().Cleanup(func() {
		_ = s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil)
	})
	s.tk.MustExec(`DROP USER IF EXISTS 'test_cancel_job1'@'localhost';`)
	s.tk.MustExec(`CREATE USER 'test_cancel_job1'@'localhost';`)
	s.tk.MustExec(`GRANT SELECT,UPDATE,INSERT,DELETE,ALTER on test_cancel_job.* to 'test_cancel_job1'@'localhost'`)
	s.tk.MustExec(`DROP USER IF EXISTS 'test_cancel_job2'@'localhost';`)
	s.tk.MustExec(`CREATE USER 'test_cancel_job2'@'localhost';`)
	s.tk.MustExec(`GRANT SELECT,UPDATE,INSERT,DELETE,ALTER on test_cancel_job.* to 'test_cancel_job2'@'localhost'`)
	do, err := session.GetDomain(s.store)
	s.NoError(err)
	tableID1 := do.MustGetTableID(s.T(), "test_cancel_job", "t1")
	tableID2 := do.MustGetTableID(s.T(), "test_cancel_job", "t2")

	// cancel non-exists job
	err = s.tk.ExecToErr("cancel import job 9999999999")
	s.ErrorIs(err, exeerrors.ErrLoadDataJobNotFound)

	// cancel a running job created by self
	syncCh := make(chan struct{})
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/waitBeforeSortChunk", "return(true)")
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/syncAfterJobStarted",
		func() {
			close(syncCh)
		},
	)
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_cancel_job1", Hostname: "localhost"}, nil, nil, nil))
	result1 := s.tk.MustQuery(fmt.Sprintf(`import into t1 FROM 'gs://test_cancel_job/t.csv?endpoint=%s' with detached`,
		gcsEndpoint)).Rows()
	s.Len(result1, 1)
	jobID1, err := strconv.Atoi(result1[0][0].(string))
	s.NoError(err)
	// wait job started
	<-syncCh
	s.tk.MustExec(fmt.Sprintf("cancel import job %d", jobID1))
	rows := s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID1)).Rows()
	s.Len(rows, 1)
	jobInfo := &importer.JobInfo{
		ID:          int64(jobID1),
		TableSchema: "test_cancel_job",
		TableName:   "t1",
		TableID:     tableID1,
		CreatedBy:   "test_cancel_job1@localhost",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://test_cancel_job/t.csv?endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "cancelled",
		Step:           importer.JobStepImporting,
		ErrorMessage:   "cancelled by user",
	}
	s.compareJobInfoWithoutTime(jobInfo, rows[0])
	s.Require().Eventually(func() bool {
		task := s.getTaskByJobID(ctx, int64(jobID1))
		return task.State == proto.TaskStateReverted
	}, maxWaitTime, 500*time.Millisecond)

	// cancel again, should fail
	s.ErrorIs(s.tk.ExecToErr(fmt.Sprintf("cancel import job %d", jobID1)), exeerrors.ErrLoadDataInvalidOperation)

	// cancel a job created by test_cancel_job1 using test_cancel_job2, should fail
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_cancel_job2", Hostname: "localhost"}, nil, nil, nil))
	s.ErrorIs(s.tk.ExecToErr(fmt.Sprintf("cancel import job %d", jobID1)), plannererrors.ErrSpecificAccessDenied)
	// cancel by root, should pass privilege check
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	s.ErrorIs(s.tk.ExecToErr(fmt.Sprintf("cancel import job %d", jobID1)), exeerrors.ErrLoadDataInvalidOperation)

	// cancel job in post-process phase, using test_cancel_job2
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_cancel_job2", Hostname: "localhost"}, nil, nil, nil))
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/waitBeforeSortChunk"))
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/syncAfterJobStarted"))
	wg := sync.WaitGroup{}
	wg.Add(1)
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/syncBeforePostProcess",
		func(jobID int64) {
			go func() {
				defer wg.Done()
				s.tk.MustExec(fmt.Sprintf("cancel import job %d", jobID))
			}()
			s.Require().Eventually(func() bool {
				task := s.getTaskByJobID(ctx, jobID)
				return task.State != proto.TaskStatePending && task.State != proto.TaskStateRunning
			}, 10*time.Second, 500*time.Millisecond)
		},
	)
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/executor/importer/waitCtxDone", "return(true)")
	result2 := s.tk.MustQuery(fmt.Sprintf(`import into t2 FROM 'gs://test_cancel_job/t.csv?endpoint=%s' with detached`,
		gcsEndpoint)).Rows()
	s.Len(result2, 1)
	jobID2, err := strconv.Atoi(result2[0][0].(string))
	s.NoError(err)
	wg.Wait()
	// cancel import job will wait dist task done
	task := s.getTaskByJobID(ctx, int64(jobID2))
	s.Equal(proto.TaskStateReverted, task.State)
	rows2 := s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID2)).Rows()
	s.Len(rows2, 1)
	jobInfo = &importer.JobInfo{
		ID:          int64(jobID2),
		TableSchema: "test_cancel_job",
		TableName:   "t2",
		TableID:     tableID2,
		CreatedBy:   "test_cancel_job2@localhost",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://test_cancel_job/t.csv?endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "cancelled",
		Step:           importer.JobStepValidating,
		ErrorMessage:   "cancelled by user",
	}
	s.compareJobInfoWithoutTime(jobInfo, rows2[0])
	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	taskKey := importinto.TaskKey(int64(jobID2))
	s.NoError(err)
	s.Require().Eventually(func() bool {
		task2, err2 := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
		s.NoError(err2)
		subtasks, err2 := taskManager.GetSubtasksWithHistory(ctx, task2.ID, proto.ImportStepPostProcess)
		s.NoError(err2)
		s.Len(subtasks, 1)
		var cancelled bool
		for _, st := range subtasks {
			if st.State == proto.SubtaskStateCanceled {
				cancelled = true
				break
			}
		}
		return task2.State == proto.TaskStateReverted && cancelled
	}, maxWaitTime, 1*time.Second)

	// cancel a pending job created by test_cancel_job2 using root
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/syncBeforePostProcess"))
	s.NoError(failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/waitCtxDone"))
	wg = sync.WaitGroup{}
	wg.Add(1)
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/syncBeforeJobStarted",
		func(jobID int64) {
			s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
			go func() {
				defer wg.Done()
				s.tk.MustExec(fmt.Sprintf("cancel import job %d", jobID))
			}()
			s.Require().Eventually(func() bool {
				task := s.getTaskByJobID(ctx, jobID)
				return task.State != proto.TaskStatePending && task.State != proto.TaskStateRunning
			}, 10*time.Second, 500*time.Millisecond)
		},
	)
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	s.tk.MustExec("truncate table t2")
	tableID2 = do.MustGetTableID(s.T(), "test_cancel_job", "t2")
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_cancel_job2", Hostname: "localhost"}, nil, nil, nil))
	result2 = s.tk.MustQuery(fmt.Sprintf(`import into t2 FROM 'gs://test_cancel_job/t.csv?endpoint=%s' with detached`,
		gcsEndpoint)).Rows()
	s.Len(result2, 1)
	jobID2, err = strconv.Atoi(result2[0][0].(string))
	s.NoError(err)
	wg.Wait()
	task = s.getTaskByJobID(ctx, int64(jobID2))
	s.Equal(proto.TaskStateReverted, task.State)
	rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID2)).Rows()
	s.Len(rows, 1)
	jobInfo = &importer.JobInfo{
		ID:          int64(jobID2),
		TableSchema: "test_cancel_job",
		TableName:   "t2",
		TableID:     tableID2,
		CreatedBy:   "test_cancel_job2@localhost",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://test_cancel_job/t.csv?endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "cancelled",
		Step:           "importing",
		ErrorMessage:   "cancelled by user",
	}
	s.compareJobInfoWithoutTime(jobInfo, rows[0])
}

func (s *mockGCSSuite) TestJobFailWhenDispatchSubtask() {
	s.prepareAndUseDB("fail_job_after_import")
	s.tk.MustExec("CREATE TABLE t1 (i INT PRIMARY KEY);")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "fail_job_after_import", Name: "t.csv"},
		Content:     []byte("1\n2"),
	})
	do, err := session.GetDomain(s.store)
	s.NoError(err)
	tableID1 := do.MustGetTableID(s.T(), "fail_job_after_import", "t1")

	jobInfo := &importer.JobInfo{
		TableSchema: "fail_job_after_import",
		TableName:   "t1",
		TableID:     tableID1,
		CreatedBy:   "root@%",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://fail_job_after_import/t.csv?endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "failed",
		Step:           importer.JobStepValidating,
		ErrorMessage:   "injected error after ImportStepImport",
	}
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/failWhenDispatchPostProcessSubtask", "return(true)")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/executor/importer/setLastImportJobID", `return(true)`)
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	err = s.tk.QueryToErr(fmt.Sprintf(`import into t1 FROM 'gs://fail_job_after_import/t.csv?endpoint=%s'`, gcsEndpoint))
	s.ErrorContains(err, "injected error after ImportStepImport")
	result1 := s.tk.MustQuery(fmt.Sprintf("show import job %d", importer.TestLastImportJobID.Load())).Rows()
	s.Len(result1, 1)
	jobID1, err := strconv.Atoi(result1[0][0].(string))
	s.NoError(err)
	jobInfo.ID = int64(jobID1)
	s.compareJobInfoWithoutTime(jobInfo, result1[0])
}

func (s *mockGCSSuite) TestKillBeforeFinish() {
	s.cleanupSysTables()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	s.tk.MustExec("DROP DATABASE IF EXISTS kill_job;")
	s.tk.MustExec("CREATE DATABASE kill_job;")
	s.tk.MustExec(`CREATE TABLE kill_job.t (a INT, b INT, c int);`)
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-load", Name: "t-1.tsv"},
		Content:     []byte("1,11,111"),
	})

	var cancelFn atomic.Pointer[context.CancelFunc]
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/syncBeforeSortChunk",
		func() {
			// cancel the job when the task reach sort chunk
			(*cancelFn.Load())()
		},
	)
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/executor/cancellableCtx",
		func(ctxP *context.Context) {
			// KILL is not implemented in testkit, so we use a fail-point to simulate it.
			newCtx, cancel := context.WithCancel(*ctxP)
			*ctxP = newCtx
			cancelFn.Store(&cancel)
		},
	)
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/executor/importer/setLastImportJobID", `return(true)`)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		sql := fmt.Sprintf(`IMPORT INTO kill_job.t FROM 'gs://test-load/t-*.tsv?endpoint=%s'`, gcsEndpoint)
		err := s.tk.QueryToErr(sql)
		s.ErrorIs(errors.Cause(err), context.Canceled)
	}()
	wg.Wait()
	jobID := importer.TestLastImportJobID.Load()
	rows := s.tk.MustQuery(fmt.Sprintf("show import job %d", jobID)).Rows()
	s.Len(rows, 1)
	s.Equal("cancelled", rows[0][5])
	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	taskKey := importinto.TaskKey(jobID)
	s.NoError(err)
	s.Require().Eventually(func() bool {
		task, err2 := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
		s.NoError(err2)
		return task.State == proto.TaskStateReverted
	}, maxWaitTime, 1*time.Second)
}
