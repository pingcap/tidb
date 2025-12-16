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
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/metering"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/tests/realtikvtest/testutils"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/pd/client/opt"
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

func checkExternalFields(t *testing.T, tk *testkit.TestKit) {
	// fetch subtask meta from tk, and check fields with `external:"true"` tag
	rs := tk.MustQuery("select meta, step from mysql.tidb_background_subtask_history").Rows()
	for _, r := range rs {
		// convert string to int64
		step, err := strconv.Atoi(r[1].(string))
		require.NoError(t, err)
		switch proto.Step(step) {
		case proto.ImportStepEncodeAndSort:
			var subtaskMeta importinto.ImportStepMeta
			require.NoError(t, json.Unmarshal([]byte(r[0].(string)), &subtaskMeta))
			testutils.AssertExternalField(t, &subtaskMeta)
		case proto.ImportStepMergeSort:
			var subtaskMeta importinto.MergeSortStepMeta
			require.NoError(t, json.Unmarshal([]byte(r[0].(string)), &subtaskMeta))
			testutils.AssertExternalField(t, &subtaskMeta)
		case proto.ImportStepWriteAndIngest:
			var subtaskMeta importinto.WriteIngestStepMeta
			require.NoError(t, json.Unmarshal([]byte(r[0].(string)), &subtaskMeta))
			testutils.AssertExternalField(t, &subtaskMeta)
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
	ch := make(chan struct{}, 1)
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", func() {
		ch <- struct{}{}
	})
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
	s.EqualValues(6, jobInfo.Summary.ImportedRows)
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
	s.Eventually(func() bool {
		task, err2 = taskManager.GetTaskByKeyWithHistory(ctx, importinto.TaskKey(int64(jobID)))
		s.NoError(err2)
		return task.State == proto.TaskStateReverted
	}, 30*time.Second, 300*time.Millisecond)
	// check all sorted data cleaned up
	<-ch

	_, files, err = s.server.ListObjectsWithOptions("sorted", fakestorage.ListOptions{Prefix: "import"})
	s.NoError(err)
	s.Len(files, 0)

	// check subtask external field
	checkExternalFields(s.T(), s.tk)
}

func (s *mockGCSSuite) prepare10Files(bucket string) []string {
	s.T().Helper()
	var allData []string
	for i := range 10 {
		var content []byte
		keyCnt := 1000
		for j := range keyCnt {
			idx := i*keyCnt + j
			content = append(content, fmt.Appendf(nil, "%d,test-%d\n", idx, idx)...)
			allData = append(allData, fmt.Sprintf("%d test-%d", idx, idx))
		}
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: fmt.Sprintf("t.%d.csv", i)},
			Content:     content,
		})
	}
	slices.Sort(allData)
	return allData
}

func (s *mockGCSSuite) TestGlobalSortMultiFiles() {
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)")
	allData := s.prepare10Files("gs-multi-files")
	s.prepareAndUseDB("gs_multi_files")
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.tk.MustExec("create table t (a bigint primary key , b varchar(100), key(b), key(a,b), key(b,a));")
	// 1 subtask, encoding 10 files using 4 threads.
	sortStorageURI := fmt.Sprintf("gs://sorted/gs_multi_files?endpoint=%s", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://gs-multi-files/t.*.csv?endpoint=%s'
		with cloud_storage_uri='%s'`, gcsEndpoint, sortStorageURI)
	_ = s.tk.MustQuery(importSQL + ", __force_merge_step").Rows()
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(allData...))
}

func (s *mockGCSSuite) TestGlobalSortRecordedStepSummary() {
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)")
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	allData := s.prepare10Files("gsort_step_summary")
	s.prepareAndUseDB("gsort_step_summary")
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.tk.MustExec("create table t (a bigint primary key , b varchar(100), key(b), key(a,b), key(b,a));")
	// 1 subtask, encoding 10 files using 1 thread.
	sortStorageURI := fmt.Sprintf("gs://sorted/gsort_step_summary?endpoint=%s", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://gsort_step_summary/t.*.csv?endpoint=%s'
		with thread=1, cloud_storage_uri='%s'`, gcsEndpoint, sortStorageURI)
	result := s.tk.MustQuery(importSQL + ", __force_merge_step").Rows()

	s.Len(result, 1)
	jobID, err := strconv.Atoi(result[0][0].(string))
	s.NoError(err)
	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	var task *proto.Task
	s.Eventually(func() bool {
		task, err = taskManager.GetTaskByKeyWithHistory(ctx, importinto.TaskKey(int64(jobID)))
		s.NoError(err)
		return task.State == proto.TaskStateSucceed
	}, 30*time.Second, 300*time.Millisecond)

	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(allData...))

	sum := s.getStepSummary(ctx, taskManager, task.ID, proto.ImportStepEncodeAndSort)
	s.EqualValues(10000, sum.RowCnt.Load())
	s.EqualValues(147780, sum.Bytes.Load())
	// this GET comes from reading subtask meta from object storage.
	s.EqualValues(1, sum.GetReqCnt.Load())
	// we have 4 kv groups, 2 files per group, and 1 for the meta file.
	s.EqualValues(9, sum.PutReqCnt.Load())

	sum = s.getStepSummary(ctx, taskManager, task.ID, proto.ImportStepMergeSort)
	// 4 kv groups, each have 3 read(1 meta, 1 get kv file size, 1 read kv file)
	// and 3 write(1 kv, 1 stat, 1 meta)
	s.EqualValues(12, sum.GetReqCnt.Load())
	s.EqualValues(12, sum.PutReqCnt.Load())

	sum = s.getStepSummary(ctx, taskManager, task.ID, proto.ImportStepWriteAndIngest)
	s.EqualValues(sum.RowCnt.Load(), 10000)
	if kerneltype.IsClassic() {
		s.EqualValues(sum.Bytes.Load(), 2622604)
	} else {
		// There are total 10000 * 4 kv pairs, each with 4 bytes keyspace prefix.
		s.EqualValues(sum.Bytes.Load(), 2782604)
	}
	s.EqualValues(20, sum.GetReqCnt.Load())
	s.EqualValues(0, sum.PutReqCnt.Load())
}

func (s *mockGCSSuite) getStepSummary(ctx context.Context, taskMgr *storage.TaskManager, taskID int64, step proto.Step) *execute.SubtaskSummary {
	s.T().Helper()
	subtasks, err := taskMgr.GetSubtasksWithHistory(ctx, taskID, step)
	s.NoError(err)
	var accumSummary execute.SubtaskSummary
	for _, subtask := range subtasks {
		v := &execute.SubtaskSummary{}
		err = json.Unmarshal([]byte(subtask.Summary), &v)
		s.NoError(err)
		accumSummary.RowCnt.Add(v.RowCnt.Load())
		accumSummary.Bytes.Add(v.Bytes.Load())
		accumSummary.PutReqCnt.Add(v.PutReqCnt.Load())
		accumSummary.GetReqCnt.Add(v.GetReqCnt.Load())
	}
	return &accumSummary
}

func (s *mockGCSSuite) TestGlobalSortUniqueKeyConflict() {
	var allData []string
	for i := range 10 {
		var content []byte
		keyCnt := 1000
		for j := range keyCnt {
			idx := i*keyCnt + j
			content = append(content, fmt.Appendf(nil, "%d,test-%d\n", idx, idx)...)
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
		with __max_engine_size = '1', cloud_storage_uri='%s', thread=1`, gcsEndpoint, sortStorageURI)

	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/br/pkg/storage/GCSReadUnexpectedEOF", "return(0)")
	s.tk.MustExec("truncate table t")
	result := s.tk.MustQuery(importSQL).Rows()
	s.Len(result, 1)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 foo1 bar1 123", "2 foo2 bar2 456", "3 foo3 bar3 789",
		"4 foo4 bar4 123", "5 foo5 bar5 223", "6 foo6 bar6 323",
	))
}

func (s *mockGCSSuite) TestSplitRangeForTable() {
	if kerneltype.IsNextGen() {
		s.T().Skip("In next-gen scenario we don't need 'force_partition_range' to import data")
	}
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
	stores, err := dom.GetPDClient().GetAllStores(context.Background(), opt.WithExcludeTombstone())
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

func TestNextGenMetering(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("Metering is not supported in classic kernel type")
	}
	bak := metering.FlushInterval
	metering.FlushInterval = time.Second
	t.Cleanup(func() {
		metering.FlushInterval = bak
	})
	s := &mockGCSSuite{}
	s.SetT(t)
	s.SetupSuite()
	t.Cleanup(func() {
		s.TearDownSuite()
	})
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	srcDirURI := realtikvtest.GetNextGenObjStoreURI("meter-test")
	srcStore, err2 := handle.NewObjStore(ctx, srcDirURI)
	s.NoError(err2)
	s.NoError(srcStore.WriteFile(ctx, "t.1.csv", []byte("1,test-1\n2,test-2\n3,test-3\n")))
	s.prepareAndUseDB("metering")
	glSortURI := realtikvtest.GetNextGenObjStoreURI("gl-sort")
	s.tk.MustExec("create table t (a bigint primary key , b varchar(100), index(b));")

	baseTime := time.Now().Truncate(time.Minute).Unix()
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/metering/forceTSAtMinuteBoundary", func(ts *int64) {
		// the metering library requires the timestamp to be at minute boundary, but
		// during test, we want to reduce the flush interval.
		*ts = baseTime
		baseTime += 60
	})
	var gotMeterData atomic.String
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/metering/meteringFinalFlush", func(s fmt.Stringer) {
		gotMeterData.Store(s.String())
	})
	var rowAndSizeMeterItems atomic.Pointer[map[string]any]
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/framework/handle/afterSendRowAndSizeMeterData", func(items map[string]any) {
		rowAndSizeMeterItems.Store(&items)
	})

	// 1 subtask, encoding 10 files using 4 threads.
	importSQL := fmt.Sprintf(`import into t FROM '%s'
		with cloud_storage_uri='%s'`, realtikvtest.GetNextGenObjStoreURI("meter-test/*.csv"), glSortURI)
	result := s.tk.MustQuery(importSQL + ", __force_merge_step").Rows()
	s.Len(result, 1)
	jobID, err := strconv.Atoi(result[0][0].(string))
	s.NoError(err)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 test-1", "2 test-2", "3 test-3"))

	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, importinto.TaskKey(int64(jobID)))
	s.NoError(err)
	s.Eventually(func() bool {
		return gotMeterData.Load() != ""
	}, 30*time.Second, 300*time.Millisecond)

	s.Contains(gotMeterData.Load(), fmt.Sprintf("id: %d, ", task.ID))
	s.Contains(gotMeterData.Load(), "requests{get: 14, put: 11}")
	// note: the read/write of subtask meta file is also counted in obj_store part,
	// but meta file contains file name which contains task and subtask ID, so
	// the length may vary, we just use regexp to match here.
	s.Regexp(`obj_store{r: 2.\d*KiB, w: 3.\d*KiB}`, gotMeterData.Load())
	// the write bytes is also not stable, due to retry, but mostly 100B to a few KB.
	s.Regexp(`cluster{r: 0B, w: (\d{3}|.*Ki)B}`, gotMeterData.Load())

	sum := s.getStepSummary(ctx, taskManager, task.ID, proto.ImportStepEncodeAndSort)
	s.EqualValues(3, sum.RowCnt.Load())
	s.EqualValues(27, sum.Bytes.Load())
	s.EqualValues(2, sum.GetReqCnt.Load())
	s.EqualValues(5, sum.PutReqCnt.Load())

	sum = s.getStepSummary(ctx, taskManager, task.ID, proto.ImportStepMergeSort)
	s.EqualValues(288, sum.Bytes.Load())
	s.EqualValues(6, sum.GetReqCnt.Load())
	s.EqualValues(6, sum.PutReqCnt.Load())

	sum = s.getStepSummary(ctx, taskManager, task.ID, proto.ImportStepWriteAndIngest)
	// if we retry write, the bytes may be larger than 288
	s.GreaterOrEqual(sum.Bytes.Load(), int64(288))
	s.EqualValues(6, sum.GetReqCnt.Load())
	s.EqualValues(0, sum.PutReqCnt.Load())

	s.Eventually(func() bool {
		items := *rowAndSizeMeterItems.Load()
		return items != nil && items[metering.RowCountField].(int64) == 3 &&
			items[metering.DataKVBytesField].(int64) == 114 && items[metering.IndexKVBytesField].(int64) == 174 &&
			items[metering.ConcurrencyField].(int) == task.Concurrency &&
			items[metering.MaxNodeCountField].(int) == task.MaxNodeCount &&
			items[metering.DurationSecondsField].(int64) > 0
	}, 30*time.Second, 100*time.Millisecond)
}
