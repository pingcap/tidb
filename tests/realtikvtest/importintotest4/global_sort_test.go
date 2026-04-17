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
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest/testutils"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
)

func urlEqual(t *testing.T, expected, actual string) {
	urlExpected, err := url.Parse(expected)
	require.NoError(t, err)
	urlGot, err := url.Parse(actual)
	require.NoError(t, err)
	// order of query parameters might change
	require.Equal(t, urlExpected.Query(), urlGot.Query())
	urlExpected.RawQuery, urlGot.RawQuery = "", ""
	require.Equal(t, urlExpected.String(), urlGot.String())
}

func (s *mockGCSSuite) checkExternalFields(taskID int64, externalMetaCleanedUp bool) {
	s.T().Helper()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	mgr, err := storage.GetTaskManager()
	s.NoError(err)
	task, err := mgr.GetTaskByIDWithHistory(ctx, taskID)
	s.NoError(err)
	taskMeta := importinto.TaskMeta{}
	s.NoError(json.Unmarshal(task.Meta, &taskMeta))
	store, err := importer.GetSortStore(ctx, taskMeta.Plan.CloudStorageURI)
	s.NoError(err)
	defer store.Close()

	for _, step := range []proto.Step{
		proto.ImportStepEncodeAndSort,
		proto.ImportStepMergeSort,
		proto.ImportStepWriteAndIngest,
		proto.ImportStepCollectConflicts,
		proto.ImportStepConflictResolution,
	} {
		subtasks, err := mgr.GetSubtasksWithHistory(ctx, taskID, step)
		s.NoError(err)
		for _, subtask := range subtasks {
			switch step {
			case proto.ImportStepEncodeAndSort:
				var subtaskMeta importinto.ImportStepMeta
				s.NoError(json.Unmarshal(subtask.Meta, &subtaskMeta))
				testutils.AssertExternalField(s.T(), &subtaskMeta)
				s.NotEmpty(subtaskMeta.ExternalPath)
				if !externalMetaCleanedUp {
					s.NoError(subtaskMeta.BaseExternalMeta.ReadJSONFromExternalStorage(ctx, store, &subtaskMeta))
					sum := subtaskMeta.SortedDataMeta.ConflictInfo.Count
					for _, m := range subtaskMeta.SortedIndexMetas {
						sum += m.ConflictInfo.Count
					}
					s.EqualValues(subtaskMeta.RecordedConflictKVCount, sum)
				}
			case proto.ImportStepMergeSort:
				var subtaskMeta importinto.MergeSortStepMeta
				s.NoError(json.Unmarshal(subtask.Meta, &subtaskMeta))
				testutils.AssertExternalField(s.T(), &subtaskMeta)
				s.NotEmpty(subtaskMeta.ExternalPath)
				if !externalMetaCleanedUp {
					s.NoError(subtaskMeta.BaseExternalMeta.ReadJSONFromExternalStorage(ctx, store, &subtaskMeta))
					s.EqualValues(subtaskMeta.ConflictInfo.Count, subtaskMeta.RecordedConflictKVCount)
				}
			case proto.ImportStepWriteAndIngest:
				var subtaskMeta importinto.WriteIngestStepMeta
				s.NoError(json.Unmarshal(subtask.Meta, &subtaskMeta))
				testutils.AssertExternalField(s.T(), &subtaskMeta)
				s.NotEmpty(subtaskMeta.ExternalPath)
				if !externalMetaCleanedUp {
					s.NoError(subtaskMeta.BaseExternalMeta.ReadJSONFromExternalStorage(ctx, store, &subtaskMeta))
					s.EqualValues(subtaskMeta.ConflictInfo.Count, subtaskMeta.RecordedConflictKVCount)
				}
			case proto.ImportStepCollectConflicts:
				var subtaskMeta importinto.CollectConflictsStepMeta
				s.NoError(json.Unmarshal(subtask.Meta, &subtaskMeta))
				testutils.AssertExternalField(s.T(), &subtaskMeta)
				s.NotEmpty(subtaskMeta.ExternalPath)
				if !externalMetaCleanedUp {
					s.NoError(subtaskMeta.BaseExternalMeta.ReadJSONFromExternalStorage(ctx, store, &subtaskMeta))
					info, ok := subtaskMeta.Infos.ConflictInfos["data"]
					if !ok {
						s.Zero(subtaskMeta.RecordedDataKVConflicts)
					} else {
						s.EqualValues(info.Count, subtaskMeta.RecordedDataKVConflicts)
					}
				}
			case proto.ImportStepConflictResolution:
				var subtaskMeta importinto.ConflictResolutionStepMeta
				s.NoError(json.Unmarshal(subtask.Meta, &subtaskMeta))
				testutils.AssertExternalField(s.T(), &subtaskMeta)
				s.NotEmpty(subtaskMeta.ExternalPath)
				if !externalMetaCleanedUp {
					s.NoError(subtaskMeta.BaseExternalMeta.ReadJSONFromExternalStorage(ctx, store, &subtaskMeta))
				}
			default:
				s.Failf("unexpected step", "%v", step)
			}
		}
	}
}

func (s *mockGCSSuite) TestGlobalSortBasic() {
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gs-basic", Name: "t.1.csv"},
		Content:     []byte("1,foo1,bar1,123\n2,foo2,bar2,456\n3,foo3,bar3,789\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gs-basic", Name: "t.2.csv"},
		Content:     []byte("4,foo4,bar4,123\n5,foo5,bar5,223\n6,foo6,bar6,323\n"),
	})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.prepareAndUseDB("gsort_basic")
	s.tk.MustExec(`create table t (a bigint primary key, b varchar(100), c varchar(100), d int,
		key(a), key(c,d), key(d));`)
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/parser/ast/forceRedactURL", "return(true)")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", "return()")
	var counter atomic.Int32
	tk2 := testkit.NewTestKit(s.T(), s.store)
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/syncAfterSubtaskFinish",
		func() {
			newVal := counter.Add(1)
			if newVal == 1 {
				// show processlist, cloud_storage_uri should be redacted too
				// we only check cloud_storage_uri here, for the source data path
				// part, see TestShowJob
				procRows := tk2.MustQuery("show full processlist").Rows()
				var got bool
				for _, r := range procRows {
					sql := r[7].(string)
					if strings.Contains(sql, "IMPORT INTO") {
						index := strings.Index(sql, "cloud_storage_uri")
						s.Greater(index, 0)
						sql = sql[index:]
						s.Contains(sql, "access-key=xxxxxx")
						s.Contains(sql, "secret-access-key=xxxxxx")
						s.NotContains(sql, "aaaaaa")
						s.NotContains(sql, "bbbbbb")
						got = true
					}
				}
				s.True(got)
			}
		},
	)
	ch := make(chan struct{}, 1)
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", func() {
		ch <- struct{}{}
	})

	sortStorageURI := fmt.Sprintf("gs://sorted/import?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://gs-basic/t.*.csv?endpoint=%s'
		with __max_engine_size = '1', cloud_storage_uri='%s'`, gcsEndpoint, sortStorageURI)
	result := s.tk.MustQuery(importSQL).Rows()
	s.GreaterOrEqual(counter.Load(), int32(1))
	s.Len(result, 1)
	jobID, err := strconv.Atoi(result[0][0].(string))
	s.NoError(err)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 foo1 bar1 123", "2 foo2 bar2 456", "3 foo3 bar3 789",
		"4 foo4 bar4 123", "5 foo5 bar5 223", "6 foo6 bar6 323",
	))

	// check all sorted data cleaned up
	<-ch

	_, files, err := s.server.ListObjectsWithOptions("sorted", fakestorage.ListOptions{Prefix: "import"})
	s.NoError(err)
	s.Len(files, 0)
	// check sensitive info is redacted
	jobInfo, err := importer.GetJob(context.Background(), s.tk.Session(), int64(jobID), "", true)
	s.NoError(err)
	redactedSortStorageURI := fmt.Sprintf("gs://sorted/import?endpoint=%s&access-key=xxxxxx&secret-access-key=xxxxxx", gcsEndpoint)
	urlEqual(s.T(), redactedSortStorageURI, jobInfo.Parameters.Options["cloud_storage_uri"].(string))
	s.Equal(uint64(6), jobInfo.Summary.ImportedRows)
	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	taskKey := importinto.TaskKey(int64(jobID))
	task, err2 := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	s.NoError(err2)
	taskMeta := importinto.TaskMeta{}
	s.NoError(json.Unmarshal(task.Meta, &taskMeta))
	urlEqual(s.T(), redactedSortStorageURI, taskMeta.Plan.CloudStorageURI)
	require.True(s.T(), taskMeta.Plan.DisableTiKVImportMode)

	// merge-sort data kv
	s.tk.MustExec("truncate table t")
	result = s.tk.MustQuery(importSQL + `, __force_merge_step`).Rows()
	s.Len(result, 1)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 foo1 bar1 123", "2 foo2 bar2 456", "3 foo3 bar3 789",
		"4 foo4 bar4 123", "5 foo5 bar5 223", "6 foo6 bar6 323",
	))
	<-ch

	// failed task, should clean up all sorted data too.
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/failWhenDispatchWriteIngestSubtask", "return(true)")
	s.tk.MustExec("truncate table t")
	result = s.tk.MustQuery(importSQL + ", detached").Rows()
	s.Len(result, 1)
	jobID, err = strconv.Atoi(result[0][0].(string))
	s.NoError(err)
	var taskID int64
	s.Eventually(func() bool {
		task, err2 = taskManager.GetTaskByKeyWithHistory(ctx, importinto.TaskKey(int64(jobID)))
		s.NoError(err2)
		taskID = task.ID
		return task.State == proto.TaskStateReverted
	}, 30*time.Second, 300*time.Millisecond)
	// check all sorted data cleaned up
	<-ch

	_, files, err = s.server.ListObjectsWithOptions("sorted", fakestorage.ListOptions{Prefix: "import"})
	s.NoError(err)
	s.Len(files, 0)

	// check subtask external field
	s.checkExternalFields(taskID, true)
}

func (s *mockGCSSuite) TestGlobalSortMultiFiles() {
	var allData []string
	for i := 0; i < 10; i++ {
		var content []byte
		keyCnt := 1000
		for j := 0; j < keyCnt; j++ {
			idx := i*keyCnt + j
			content = append(content, []byte(fmt.Sprintf("%d,test-%d\n", idx, idx))...)
			allData = append(allData, fmt.Sprintf("%d test-%d", idx, idx))
		}
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gs-multi-files", Name: fmt.Sprintf("t.%d.csv", i)},
			Content:     content,
		})
	}
	slices.Sort(allData)
	s.prepareAndUseDB("gs_multi_files")
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.tk.MustExec("create table t (a bigint primary key , b varchar(100), key(b), key(a,b), key(b,a));")
	// 1 subtask, encoding 10 files using 4 threads.
	sortStorageURI := fmt.Sprintf("gs://sorted/gs_multi_files?endpoint=%s", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://gs-multi-files/t.*.csv?endpoint=%s'
		with cloud_storage_uri='%s'`, gcsEndpoint, sortStorageURI)
	s.tk.MustQuery(importSQL)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(allData...))
}

func (s *mockGCSSuite) TestGlobalSortUniqueKeyConflict() {
	var allData []string
	for i := 0; i < 10; i++ {
		var content []byte
		keyCnt := 1000
		for j := 0; j < keyCnt; j++ {
			idx := i*keyCnt + j
			content = append(content, []byte(fmt.Sprintf("%d,test-%d\n", idx, idx))...)
		}
		if i == 9 {
			// add a duplicate key "test-123"
			content = append(content, []byte("99999999,test-123\n")...)
		}
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gs-multi-files-uk", Name: fmt.Sprintf("t.%d.csv", i)},
			Content:     content,
		})
	}
	slices.Sort(allData)
	s.prepareAndUseDB("gs_multi_files")
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.tk.MustExec("create table t (a bigint primary key , b varchar(100) unique key);")
	// 1 subtask, encoding 10 files using 4 threads.
	sortStorageURI := fmt.Sprintf("gs://sorted/gs_multi_files?endpoint=%s", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://gs-multi-files-uk/t.*.csv?endpoint=%s'
		with cloud_storage_uri='%s', __max_engine_size='1', thread=8`, gcsEndpoint, sortStorageURI)
	err := s.tk.QueryToErr(importSQL)
	require.ErrorContains(s.T(), err, "duplicate key found")
	// this is the encoded value of "test-123". Because the table ID/ index ID may vary, we can't check the exact key
	// TODO: decode the key to use readable value in the error message.
	require.ErrorContains(s.T(), err, "746573742d313233")
}

func (s *mockGCSSuite) TestSplitRangeForTable() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gs-basic", Name: "t.1.csv"},
		Content:     []byte("1,foo1,bar1,123\n2,foo2,bar2,456\n3,foo3,bar3,789\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gs-basic", Name: "t.2.csv"},
		Content:     []byte("4,foo4,bar4,123\n5,foo5,bar5,223\n6,foo6,bar6,323\n"),
	})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.prepareAndUseDB("gsort_basic")
	s.tk.MustExec(`create table t (a bigint primary key, b varchar(100), c varchar(100), d int,
		key(a), key(c,d), key(d));`)

	var addCnt, removeCnt int
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/lightning/backend/local/AddPartitionRangeForTable", func() {
		addCnt += 1
	})
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/lightning/backend/local/RemovePartitionRangeRequest", func() {
		removeCnt += 1
	})
	dom, err := session.GetDomain(s.store)
	require.NoError(s.T(), err)
	stores, err := dom.GetPDClient().GetAllStores(context.Background(), pd.WithExcludeTombstone())
	require.NoError(s.T(), err)

	sortStorageURI := fmt.Sprintf("gs://sorted/import?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://gs-basic/t.*.csv?endpoint=%s' with cloud_storage_uri='%s'`, gcsEndpoint, sortStorageURI)
	result := s.tk.MustQuery(importSQL).Rows()
	s.Len(result, 1)
	require.Greater(s.T(), addCnt, 0)
	require.Equal(s.T(), removeCnt, addCnt)

	addCnt = 0
	removeCnt = 0
	s.tk.MustExec("truncate t")
	importSQL = fmt.Sprintf(`import into t FROM 'gs://gs-basic/t.*.csv?endpoint=%s'`, gcsEndpoint)
	result = s.tk.MustQuery(importSQL).Rows()
	s.Len(result, 1)
	require.Equal(s.T(), addCnt, 2*len(stores))
	require.Equal(s.T(), removeCnt, addCnt)

	addCnt = 0
	removeCnt = 0
	s.tk.MustExec("create table dst like t")
	s.tk.MustExec(`import into dst FROM select * from t`)
	require.Equal(s.T(), addCnt, 2*len(stores))
	require.Equal(s.T(), removeCnt, addCnt)
}

func (s *mockGCSSuite) TestGlobalSortWithGCSReadError() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gs-basic", Name: "t.1.csv"},
		Content:     []byte("1,foo1,bar1,123\n2,foo2,bar2,456\n3,foo3,bar3,789\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "gs-basic", Name: "t.2.csv"},
		Content:     []byte("4,foo4,bar4,123\n5,foo5,bar5,223\n6,foo6,bar6,323\n"),
	})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.prepareAndUseDB("gsort_basic")
	s.tk.MustExec(`create table t (a bigint primary key, b varchar(100), c varchar(100), d int,	key(a), key(c,d), key(d));`)
	defer func() {
		s.tk.MustExec("drop table t;")
	}()

	sortStorageURI := fmt.Sprintf("gs://sorted/import?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://gs-basic/t.*.csv?endpoint=%s'
		with __max_engine_size = '1', cloud_storage_uri='%s', thread=8`, gcsEndpoint, sortStorageURI)

	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/br/pkg/storage/GCSReadUnexpectedEOF", "return(0)")
	s.tk.MustExec("truncate table t")
	result := s.tk.MustQuery(importSQL).Rows()
	s.Len(result, 1)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 foo1 bar1 123", "2 foo2 bar2 456", "3 foo3 bar3 789",
		"4 foo4 bar4 123", "5 foo5 bar5 223", "6 foo6 bar6 323",
	))
}
