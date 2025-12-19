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

package addindextest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/phayes/freeport"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/metering"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	diststorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/tests/realtikvtest/testutils"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/pd/client/opt"
	uberatomic "go.uber.org/atomic"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func genStorageURI(t *testing.T) (host string, port uint16, uri string) {
	gcsHost := "127.0.0.1"
	// for fake gcs server, we must use this endpoint format
	// NOTE: must end with '/'
	gcsEndpointFormat := "http://%s:%d/storage/v1/"
	freePort, err := freeport.GetFreePort()
	require.NoError(t, err)
	gcsEndpoint := fmt.Sprintf(gcsEndpointFormat, gcsHost, freePort)
	return gcsHost, uint16(freePort),
		fmt.Sprintf("gs://sorted/addindex?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
}

func genServerWithStorage(t *testing.T) (*fakestorage.Server, string) {
	t.Helper()
	gcsHost, gcsPort, cloudStorageURI := genStorageURI(t)
	opt := fakestorage.Options{
		Scheme:     "http",
		Host:       gcsHost,
		Port:       gcsPort,
		PublicHost: gcsHost,
	}
	server, err := fakestorage.NewServerWithOptions(opt)
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	return server, cloudStorageURI
}

func checkFileCleaned(t *testing.T, jobID, taskID int64, sortStorageURI string) {
	storeBackend, err := storage.ParseBackend(sortStorageURI, nil)
	require.NoError(t, err)
	extStore, err := storage.NewWithDefaultOpt(context.Background(), storeBackend)
	require.NoError(t, err)
	for _, id := range []int64{jobID, taskID} {
		prefix := strconv.Itoa(int(id))
		files, err := external.GetAllFileNames(context.Background(), extStore, prefix)
		require.NoError(t, err)
		require.Greater(t, jobID, int64(0))
		require.Equal(t, 0, len(files))
	}
}

// check the file under dir or partitioned dir have files with keyword
func checkFileExist(t *testing.T, sortStorageURI string, dir, keyword string) {
	storeBackend, err := storage.ParseBackend(sortStorageURI, nil)
	require.NoError(t, err)
	extStore, err := storage.NewWithDefaultOpt(context.Background(), storeBackend)
	require.NoError(t, err)
	dataFiles, err := external.GetAllFileNames(context.Background(), extStore, dir)
	require.NoError(t, err)
	filteredFiles := make([]string, 0)
	for _, f := range dataFiles {
		if strings.Contains(f, keyword) {
			filteredFiles = append(filteredFiles, f)
		}
	}
	require.Greater(t, len(filteredFiles), 0)
}

func checkDataAndShowJobs(t *testing.T, tk *testkit.TestKit, count int) {
	tk.MustExec("admin check table t;")
	rs := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rs, 1)
	if kerneltype.IsClassic() {
		require.Contains(t, rs[0][12], "ingest")
		require.Contains(t, rs[0][12], "cloud")
	} else {
		require.Equal(t, rs[0][12], "")
	}
	require.Equal(t, rs[0][7], strconv.Itoa(count))
}

func checkExternalFields(t *testing.T, tk *testkit.TestKit) {
	// fetch subtask meta from tk, and check fields with `external:"true"` tag
	rs := tk.MustQuery("select meta from mysql.tidb_background_subtask").Rows()
	for _, r := range rs {
		var subtaskMeta ddl.BackfillSubTaskMeta
		require.NoError(t, json.Unmarshal([]byte(r[0].(string)), &subtaskMeta))
		testutils.AssertExternalField(t, &subtaskMeta)
	}
}

func getTaskID(t *testing.T, jobID int64) int64 {
	mgr, err := diststorage.GetTaskManager()
	require.NoError(t, err)
	ctx := util.WithInternalSourceType(context.Background(), "scheduler")
	tkBuilder := ddl.NewTaskKeyBuilder()
	task, err := mgr.GetTaskByKeyWithHistory(ctx, tkBuilder.Build(jobID))
	require.NoError(t, err)
	return task.ID
}

func TestGlobalSortBasic(t *testing.T) {
	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	ch := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/doCleanupTask", func() {
		ch <- struct{}{}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", func() {
		ch <- struct{}{}
	})
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	}
	tk.MustExec(fmt.Sprintf(`set @@global.tidb_cloud_storage_uri = "%s"`, cloudStorageURI))
	cloudStorageURI = handle.GetCloudStorageURI(context.Background(), store) // path with cluster id
	defer func() {
		vardef.CloudStorageURI.Store("")
	}()

	tk.MustExec("create table t (a int, b int, c int);")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	size := 100
	for i := range size {
		sb.WriteString(fmt.Sprintf("(%d, %d, %d)", i, i, i))
		if i != size-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString(";")
	tk.MustExec(sb.String())

	var jobID int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		jobID = job.ID
	})

	tk.MustExec("alter table t add index idx(a);")
	checkDataAndShowJobs(t, tk, size)
	checkExternalFields(t, tk)
	taskID := getTaskID(t, jobID)
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID)), "/plan/ingest")
	<-ch
	<-ch
	checkFileCleaned(t, jobID, taskID, cloudStorageURI)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()")
	tk.MustExec("alter table t add index idx1(a);")
	checkDataAndShowJobs(t, tk, size)
	checkExternalFields(t, tk)
	taskID = getTaskID(t, jobID)
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID)), "/plan/ingest")
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID)), "/plan/merge-sort")
	<-ch
	<-ch
	checkFileCleaned(t, jobID, taskID, cloudStorageURI)

	tk.MustExec("alter table t add unique index idx2(a);")
	checkDataAndShowJobs(t, tk, size)
	checkExternalFields(t, tk)
	taskID = getTaskID(t, jobID)
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID)), "/plan/ingest")
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID)), "/plan/merge-sort")
	<-ch
	<-ch
	checkFileCleaned(t, jobID, taskID, cloudStorageURI)
}

func TestGlobalSortMultiSchemaChange(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockRegionBatch", `return(1)`)

	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")

	tk.MustExec("create table t_rowid (a int, b bigint, c varchar(255));")
	tk.MustExec("create table t_int_handle (a bigint primary key, b varchar(255));")
	tk.MustExec("create table t_common_handle (a int, b bigint, c varchar(255), primary key (a, c) clustered);")
	tk.MustExec(`create table t_partition (a bigint primary key, b int, c char(10)) partition by hash(a) partitions 2;`)
	for i := range 10 {
		tk.MustExec(fmt.Sprintf("insert into t_rowid values (%d, %d, '%d');", i, i, i))
		tk.MustExec(fmt.Sprintf("insert into t_int_handle values (%d, '%d');", i, i))
		tk.MustExec(fmt.Sprintf("insert into t_common_handle values (%d, %d, '%d');", i, i, i))
		tk.MustExec(fmt.Sprintf("insert into t_partition values (%d, %d, '%d');", i, i, i))
	}
	tk.MustExec("create table t_dup (a int, b bigint);")
	tk.MustExec(fmt.Sprintf("insert into t_dup values (%d, %d), (%d, %d);", 1, 2, 2, 2))
	tk.MustExec("create table t_dup_2 (a int primary key, b bigint);")
	tk.MustQuery("split table t_dup_2 between (0) and (80000) regions 7;").Check(testkit.Rows("6 1"))
	tk.MustExec(fmt.Sprintf("insert into t_dup_2 values (%d, %d), (%d, %d);", 1, 2, 79999, 2))

	tableNames := []string{"t_rowid", "t_int_handle", "t_common_handle", "t_partition"}

	testCases := []struct {
		name            string
		enableFastReorg string
		enableDistTask  string
		cloudStorageURI string
	}{
		{"txn_backfill", "0", "0", ""},
		{"ingest_backfill", "1", "0", ""},
		{"ingest_dist_backfill", "1", "1", ""},
		{"ingest_dist_gs_backfill", "1", "1", cloudStorageURI},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if kerneltype.IsNextGen() {
				if tc.cloudStorageURI == "" {
					t.Skip("local sort might ingest duplicate KV, cause overlapped sst")
				}
				if tc.enableDistTask == "0" {
					t.Skip("DXF is always enabled on nextgen")
				}
			}
			if kerneltype.IsClassic() {
				tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = " + tc.enableFastReorg + ";")
				tk.MustExec("set @@global.tidb_enable_dist_task = " + tc.enableDistTask + ";")
			}
			tk.MustExec("set @@global.tidb_cloud_storage_uri = '" + tc.cloudStorageURI + "';")
			for _, tn := range tableNames {
				if kerneltype.IsNextGen() && tc.cloudStorageURI != "" && tn == "t_partition" {
					t.Log("partition table in global sort is ordered by index KV group, cause overlapped sst")
					continue
				}
				tk.MustExec("alter table " + tn + " add index idx_1(a), add index idx_2(b, a);")
				tk.MustExec("admin check table " + tn + ";")
				tk.MustExec("alter table " + tn + " drop index idx_1, drop index idx_2;")
			}

			tk.MustContainErrMsg(
				"alter table t_dup add index idx(a), add unique index idx2(b);",
				"Duplicate entry '2' for key 't_dup.idx2'",
			)
			tk.MustContainErrMsg(
				"alter table t_dup_2 add unique index idx2(b);",
				"Duplicate entry '2' for key 't_dup_2.idx2'",
			)
		})
	}

	if kerneltype.IsClassic() {
		tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
	}
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
}

func TestAddIndexIngestShowReorgTp(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled on nextgen")
	}
	_, cloudStorageURI := genServerWithStorage(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '" + cloudStorageURI + "';")
	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = 1;")
	t.Cleanup(func() {
		tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
		tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
	})

	tk.MustExec("create table t (a int);")
	tk.MustExec("alter table t add index idx(a);")
	tk.MustQuery("select * from t use index(idx);").Check(testkit.Rows())
	tk.MustExec("alter table t drop index idx;")

	tk.MustExec("insert into t values (1), (2), (3);")
	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
	tk.MustExec("alter table t add index idx(a);")

	rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rows, 1)
	jobType, rowCnt := rows[0][12].(string), rows[0][7].(string)
	if kerneltype.IsClassic() {
		require.True(t, strings.Contains(jobType, "ingest"), jobType)
		require.False(t, strings.Contains(jobType, "cloud"), jobType)
	} else {
		require.Equal(t, jobType, "")
	}
	require.Equal(t, rowCnt, "3")
}

func TestGlobalSortDuplicateErrMsg(t *testing.T) {
	testutil.ReduceCheckInterval(t)
	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	}
	tk.MustExec(fmt.Sprintf(`set @@global.tidb_cloud_storage_uri = "%s"`, cloudStorageURI))
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("set @@session.tidb_scatter_region = 'table'")
	t.Cleanup(func() {
		vardef.CloudStorageURI.Store("")
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
		tk.MustExec("set @@session.tidb_scatter_region = ''")
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockRegionBatch", `return(1)`)
	testErrStep := proto.StepInit
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterRunSubtask",
		func(e taskexecutor.TaskExecutor, errP *error, _ context.Context) {
			if errP != nil {
				testErrStep = e.GetTaskBase().Step
			}
		},
	)

	testcases := []struct {
		caseName        string
		createTableSQL  string
		splitTableSQL   string
		initDataSQL     string
		addUniqueKeySQL string
		errMsg          string
	}{
		{
			"varchar index",
			"create table t (id int, data varchar(255));",
			"",
			"insert into t values (1, '1'), (2, '1');",
			"alter table t add unique index idx(data);",
			"[kv:1062]Duplicate entry '1' for key 't.idx'",
		},
		{
			"int index on multi regions",
			"create table t (a int primary key, b int);",
			"split table t between (0) and (4000) regions 4;",
			"insert into t values (1, 1), (1001, 1), (2001, 2001), (4001, 1);",
			"alter table t add unique index idx(b);",
			"[kv:1062]Duplicate entry '1' for key 't.idx'",
		},
		{
			"combined index",
			"create table t (id int, data varchar(255));",
			"",
			"insert into t values (1, '1'), (1, '1');",
			"alter table t add unique index idx(id, data);",
			"[kv:1062]Duplicate entry '1-1' for key 't.idx'",
		},
		{
			"multi value index",
			"create table t (id int, data json);",
			"",
			`insert into t values (1, '{"code":[1,1]}'), (2, '{"code":[1,1]}');`,
			"alter table t add unique index idx( (CAST(data->'$.code' AS UNSIGNED ARRAY)));",
			"[kv:1062]Duplicate entry '1' for key 't.idx'",
		},
		{
			"global index",
			"create table t (k int, c int) partition by list (k) (partition odd values in (1,3,5,7,9), partition even values in (2,4,6,8,10));",
			"",
			"insert into t values (1, 1), (2, 1)",
			"alter table t add unique index idx(c) global",
			"[kv:1062]Duplicate entry '1' for key 't.idx'",
		},
	}

	checkSubtaskStepAndReset := func(t *testing.T, expectedStep proto.Step) {
		require.Equal(t, expectedStep, testErrStep)
		testErrStep = proto.StepInit
	}

	checkRedactMsgAndReset := func(addUniqueKeySQL string) {
		tk.MustExec("set global tidb_redact_log = on;")
		tk.MustContainErrMsg(addUniqueKeySQL, "[kv:1062]Duplicate entry '?' for key 't.idx'")
		tk.MustExec("set global tidb_redact_log = off;")
		testErrStep = proto.StepInit
	}

	for _, tc := range testcases {
		t.Run(tc.caseName, func(tt *testing.T) {
			// init
			tk.MustExec(tc.createTableSQL)
			tk.MustExec(tc.initDataSQL)
			tt.Cleanup(func() {
				tk.MustExec("drop table if exists t")
			})

			// pre-check
			multipleRegions := len(tc.splitTableSQL) > 0 || strings.Contains(tc.createTableSQL, "partition")
			if len(tc.splitTableSQL) > 0 {
				tk.MustQuery(tc.splitTableSQL).Check(testkit.Rows("3 1"))
			}
			if strings.Contains(tc.createTableSQL, "partition") {
				rs := tk.MustQuery("show table t regions")
				require.Len(tt, rs.Rows(), 2)
			}

			// 1. read index
			tk.MustContainErrMsg(tc.addUniqueKeySQL, tc.errMsg)
			if multipleRegions {
				checkSubtaskStepAndReset(tt, proto.BackfillStepWriteAndIngest)
			} else {
				checkSubtaskStepAndReset(tt, proto.BackfillStepReadIndex)
			}
			checkRedactMsgAndReset(tc.addUniqueKeySQL)

			// 2. merge sort
			testfailpoint.Enable(tt, "github.com/pingcap/tidb/pkg/ddl/ignoreReadIndexDupKey", `return(true)`)
			require.NoError(tt, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()"))
			tk.MustContainErrMsg(tc.addUniqueKeySQL, tc.errMsg)
			checkSubtaskStepAndReset(tt, proto.BackfillStepMergeSort)

			// 3. cloud import
			require.NoError(tt, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/forceMergeSort"))
			tk.MustContainErrMsg(tc.addUniqueKeySQL, tc.errMsg)
			checkSubtaskStepAndReset(tt, proto.BackfillStepWriteAndIngest)
		})
	}
}

// When meeting a retryable error, the subtask/job should be idempotent.
func TestGlobalSortAddIndexRecoverFromRetryableError(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("might cause overlapped data")
	}
	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	tk.MustExec(fmt.Sprintf(`set @@global.tidb_cloud_storage_uri = "%s"`, cloudStorageURI))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()")
	defer func() {
		tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
	}()
	failpoints := []string{
		"github.com/pingcap/tidb/pkg/ddl/mockCheckDuplicateForUniqueIndexError",
		"github.com/pingcap/tidb/pkg/ddl/mockCloudImportRunSubtaskError",
		"github.com/pingcap/tidb/pkg/ddl/mockMergeSortRunSubtaskError",
	}

	for _, fp := range failpoints {
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int);")
		tk.MustExec("insert into t values (1), (2), (3);")
		require.NoError(t, failpoint.Enable(fp, "1*return"))
		tk.MustExec("alter table t add unique index idx(a);")
		require.NoError(t, failpoint.Disable(fp))
	}
}

func TestIngestUseGivenTS(t *testing.T) {
	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	var tblInfo *model.TableInfo
	var idxInfo *model.IndexInfo
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if idxInfo == nil {
			tbl, _ := dom.InfoSchema().TableByID(context.Background(), job.TableID)
			tblInfo = tbl.Meta()
			if len(tblInfo.Indices) == 0 {
				return
			}
			idxInfo = tblInfo.Indices[0]
		}
	})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg = on;`)
	}
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '" + cloudStorageURI + "';")
	t.Cleanup(func() {
		tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
	})

	presetTS := oracle.GoTimeToTS(time.Now())
	failpointTerm := fmt.Sprintf(`return(%d)`, presetTS)

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockTSForGlobalSort", failpointTerm)
	require.NoError(t, err)

	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1), (2), (3);")
	tk.MustExec("alter table t add index idx(a);")

	err = failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockTSForGlobalSort")
	require.NoError(t, err)

	dts := []types.Datum{types.NewIntDatum(1)}
	sctx := tk.Session().GetSessionVars().StmtCtx
	idxKey, _, err := tablecodec.GenIndexKey(sctx.TimeZone(), tblInfo, idxInfo, tblInfo.ID, dts, kv.IntHandle(1), nil)
	require.NoError(t, err)
	tikvStore := dom.Store().(helper.Storage)
	newHelper := helper.NewHelper(tikvStore)
	mvccResp, err := newHelper.GetMvccByEncodedKeyWithTS(idxKey, presetTS)
	require.NoError(t, err)
	require.NotNil(t, mvccResp)
	require.NotNil(t, mvccResp.Info)
	require.Greater(t, len(mvccResp.Info.Writes), 0)
	require.Equal(t, presetTS, mvccResp.Info.Writes[0].CommitTs)
}

func TestAlterJobOnDXFWithGlobalSort(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", `return(16)`)
	testutil.ReduceCheckInterval(t)

	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg = on;`)
	}
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '" + cloudStorageURI + "';")
	t.Cleanup(func() {
		tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
	})

	tk.MustExec("drop database if exists testalter;")
	tk.MustExec("create database testalter;")
	tk.MustExec("use testalter;")
	tk.MustExec("create table gsort(a bigint auto_random primary key);")
	for range 16 {
		tk.MustExec("insert into gsort values (), (), (), ()")
	}
	tk.MustExec("split table gsort between (3) and (8646911284551352360) regions 50;")

	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1")
	tk.MustExec("set @@tidb_ddl_reorg_batch_size = 32")
	if kerneltype.IsClassic() {
		tk.MustExec("set global tidb_ddl_reorg_max_write_speed = '256MiB'")
		t.Cleanup(func() {
			tk.MustExec("set global tidb_ddl_reorg_max_write_speed = 0")
		})
	}

	var (
		modifiedReadIndex atomic.Bool
		modifiedMerge     atomic.Bool
	)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return(true)")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterDetectAndHandleParamModify", func(step proto.Step) {
		switch step {
		case proto.BackfillStepReadIndex:
			modifiedReadIndex.Store(true)
		case proto.BackfillStepMergeSort:
			modifiedMerge.Store(true)
		}
	})

	var pipeClosed bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", func(pipe *operator.AsyncPipeline) {
		pipeClosed = true
		reader, writer := pipe.GetReaderAndWriter()
		require.EqualValues(t, 4, reader.GetWorkerPoolSize())
		require.EqualValues(t, 6, writer.GetWorkerPoolSize())
	})

	// Change the batch size and concurrency during table scanning and check the modified parameters.
	var onceScan sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/scanRecordExec", func(reorgMeta *model.DDLReorgMeta) {
		onceScan.Do(func() {
			tk1 := testkit.NewTestKit(t, store)
			rows := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
			require.Len(t, rows, 1)
			tk1.MustExec(fmt.Sprintf("admin alter ddl jobs %s thread = 8, batch_size = 256", rows[0][0]))
			require.Eventually(t, func() bool {
				return modifiedReadIndex.Load()
			}, 30*time.Second, 100*time.Millisecond)
			require.Equal(t, 256, reorgMeta.GetBatchSize())
		})
	})

	// Change the concurrency during merge sort and check the modified parameters.
	var onceMerge sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mergeOverlappingFiles", func(op *external.MergeOperator) {
		onceMerge.Do(func() {
			tk1 := testkit.NewTestKit(t, store)
			rows := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
			require.Len(t, rows, 1)
			tk1.MustExec(fmt.Sprintf("admin alter ddl jobs %s thread = 2", rows[0][0]))
			require.Eventually(t, func() bool {
				return modifiedMerge.Load()
			}, 30*time.Second, 100*time.Millisecond)
			require.EqualValues(t, 2, op.GetWorkerPoolSize())
		})
	})

	tk.MustExec("alter table gsort add index idx(a)")
	require.True(t, pipeClosed)
	require.True(t, modifiedReadIndex.Load())
	require.True(t, modifiedMerge.Load())
	tk.MustExec("admin check index gsort idx")
}

func TestDXFAddIndexRealtimeSummary(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", `return(16)`)
	testutil.ReduceCheckInterval(t)

	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	if kerneltype.IsClassic() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg = on;`)
	}
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '" + cloudStorageURI + "';")
	t.Cleanup(func() {
		tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
	})

	tk.MustExec("use test;")

	tk.MustExec("create table t (id varchar(255), b int, c int, primary key(id) clustered);")
	tk.MustExec("insert into t values ('a',1,1),('b',2,2),('c',3,3);")

	var jobID int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionAddIndex {
			jobID = job.ID
		}
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/forceMergeSort", `return()`)
	tk.MustExec("alter table t add index idx(c);")
	sql := `with global_tasks as (table mysql.tidb_global_task union table mysql.tidb_global_task_history)
		select id from global_tasks where task_key like concat('%%/', '%d');`
	taskIDRows := tk.MustQuery(fmt.Sprintf(sql, jobID)).Rows()
	taskID := taskIDRows[0][0].(string)

	getSummary := func(taskID string, step int64) (getReqCnt, putReqCnt, readBytes, bytes int) {
		sql = `with subtasks as (table mysql.tidb_background_subtask union table mysql.tidb_background_subtask_history)
		select
			json_extract(summary, '$.get_request_count'),
			json_extract(summary, '$.put_request_count'),
			json_extract(summary, '$.read_bytes'),
			json_extract(summary, '$.bytes')
		from subtasks where task_key = '%s' and step = %d;`
		fmtSQL := fmt.Sprintf(sql, taskID, step)
		rs := tk.MustQuery(fmtSQL).Rows()
		require.Len(t, rs, 1)
		var err error
		getReqCnt, err = strconv.Atoi(rs[0][0].(string))
		require.NoError(t, err)
		putReqCnt, err = strconv.Atoi(rs[0][1].(string))
		require.NoError(t, err)
		readBytes, err = strconv.Atoi(rs[0][2].(string))
		require.NoError(t, err)
		bytes, err = strconv.Atoi(rs[0][3].(string))
		require.NoError(t, err)
		return
	}
	getReqCnt, putReqCnt, readBytes, bytes := getSummary(taskID, 1)
	// 0, because step 1 doesn't read s3
	require.Equal(t, 0, getReqCnt)
	// 1 data, 1 stats, 1 final meta
	require.Equal(t, 3, putReqCnt)
	// 143 bytes for reading table records
	require.Greater(t, readBytes, 0)
	// 153 bytes for writing index records
	require.Greater(t, bytes, 0)

	getReqCnt, putReqCnt, readBytes, bytes = getSummary(taskID, 2)
	// 1 meta, 1 get size(GCS handle.Attrs make it, others too), 1 read
	require.Equal(t, 3, getReqCnt)
	// 2 times (data + stats), 1 for final meta
	require.Equal(t, 3, putReqCnt)
	// 0, not suitable for merge sort
	require.Equal(t, 0, readBytes)
	// 0, not suitable for merge sort
	require.Equal(t, 0, bytes)

	getReqCnt, putReqCnt, readBytes, bytes = getSummary(taskID, 3)
	// 1 meta, 2 for get size, 2 for read
	require.Equal(t, 5, getReqCnt)
	// 0, because step 3 doesn't write s3
	require.Equal(t, 0, putReqCnt)
	// 0
	require.Equal(t, 0, readBytes)
	// 0
	require.Equal(t, 0, bytes)
}

func TestSplitRangeForTable(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("In next-gen scenario we don't need 'force_partition_range' to import data")
	}
	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	stores, err := dom.GetPDClient().GetAllStores(context.Background(), opt.WithExcludeTombstone())
	require.NoError(t, err)

	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	tk.MustExec("CREATE TABLE t (c int)")
	for i := range 1024 {
		tk.MustExec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}

	testcases := []struct {
		caseName       string
		enableDistTask string
		globalSort     string
	}{
		{"local ingest", "off", ""},
		{"dxf ingest", "on", ""},
		{"dxf global-sort", "on", cloudStorageURI},
	}
	var addCnt, removeCnt int
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/AddPartitionRangeForTable", func() {
		addCnt += 1
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/RemovePartitionRangeRequest", func() {
		removeCnt += 1
	})
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = on;")
		tk.MustExec("set global tidb_cloud_storage_uri = '';")
	})
	for _, tc := range testcases {
		t.Run(tc.caseName, func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set global tidb_enable_dist_task = %s;", tc.enableDistTask))
			tk.MustExec(fmt.Sprintf("set global tidb_cloud_storage_uri = '%s';", tc.globalSort))

			addCnt = 0
			removeCnt = 0
			tk.MustExec("alter table t add index i(c)")
			require.Equal(t, addCnt, len(stores))
			require.Equal(t, removeCnt, addCnt)
			tk.MustExec("alter table t drop index i")
		})
	}
}

func TestSplitRangeForPartitionTable(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("In next-gen scenario we don't need 'force_partition_range' to import data")
	}
	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	tk.MustExec("CREATE TABLE tp (id int primary key, c int) PARTITION BY HASH (id) PARTITIONS 2")
	for i := range 1024 {
		tk.MustExec(fmt.Sprintf("INSERT INTO tp VALUES (%d, %d)", i, i))
	}

	testcases := []struct {
		caseName       string
		enableDistTask string
		globalSort     string
	}{
		{"local ingest", "off", ""},
		{"dxf ingest", "on", ""},
		{"dxf global-sort", "on", cloudStorageURI},
	}
	var addCnt, removeCnt int
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/AddPartitionRangeForTable", func() {
		addCnt += 1
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/lightning/backend/local/RemovePartitionRangeRequest", func() {
		removeCnt += 1
	})
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = on;")
		tk.MustExec("set global tidb_cloud_storage_uri = '';")
	})
	for _, tc := range testcases {
		t.Run(tc.caseName, func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set global tidb_enable_dist_task = %s;", tc.enableDistTask))
			tk.MustExec(fmt.Sprintf("set global tidb_cloud_storage_uri = '%s';", tc.globalSort))

			addCnt = 0
			removeCnt = 0
			tk.MustExec("alter table tp add index i(c)")
			require.Greater(t, addCnt, 0)
			require.Equal(t, removeCnt, addCnt)
			tk.MustExec("alter table tp drop index i")

			addCnt = 0
			removeCnt = 0
			tk.MustExec("alter table tp add index gi(c) global")
			require.Greater(t, addCnt, 0)
			require.Equal(t, removeCnt, addCnt)
			tk.MustExec("alter table tp drop index gi")
		})
	}
}

func TestNextGenMetering(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("Metering for next-gen only")
	}
	testutil.ReduceCheckInterval(t)
	bak := metering.FlushInterval
	metering.FlushInterval = time.Second
	t.Cleanup(func() {
		metering.FlushInterval = bak
	})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	srcDirURI := realtikvtest.GetNextGenObjStoreURI("index/meter-test/")
	tk.MustExec(fmt.Sprintf("set @@global.tidb_cloud_storage_uri = '%s';", srcDirURI))
	t.Cleanup(func() {
		tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
	})

	tk.MustExec("use test;")

	tk.MustExec("create table t (id varchar(255), b int, c int, primary key(id) clustered);")
	tk.MustExec("insert into t values ('a',1,1),('b',2,2),('c',3,3);")

	baseTime := time.Now().Truncate(time.Minute).Unix()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/metering/forceTSAtMinuteBoundary", func(ts *int64) {
		// the metering library requires the timestamp to be at minute boundary, but
		// during test, we want to reduce the flush interval.
		*ts = baseTime
		baseTime += 60
	})
	var gotMeterData uberatomic.String
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/metering/meteringFinalFlush", func(s fmt.Stringer) {
		gotMeterData.Store(s.String())
	})
	var jobID int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionAddIndex {
			jobID = job.ID
		}
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/forceMergeSort", `return()`)
	var rowAndSizeMeterItems atomic.Pointer[map[string]any]
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/handle/afterSendRowAndSizeMeterData", func(items map[string]any) {
		rowAndSizeMeterItems.Store(&items)
	})
	tk.MustExec("alter table t add index idx(c);")
	taskManager, err := diststorage.GetTaskManager()
	require.NoError(t, err)
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, ddl.TaskKey(jobID, false))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return gotMeterData.Load() != ""
	}, 30*time.Second, 300*time.Millisecond)
	require.Contains(t, gotMeterData.Load(), fmt.Sprintf("id: %d, ", task.ID))
	require.Contains(t, gotMeterData.Load(), "requests{get: 7, put: 6}")
	// the read bytes is not stable, but it's more than 100B.
	// the write bytes is also not stable, due to retry, but mostly 100B to a few KB.
	require.Regexp(t, `cluster{r: 1\d\dB, w: (\d{3}|.*Ki)B}`, gotMeterData.Load())
	// note: the read/write of subtask meta file is also counted in obj_store part,
	// but meta file contains file name which contains task and subtask ID, so
	// the length may vary, we just use regexp to match here.
	require.Regexp(t, `obj_store{r: 1.\d+KiB, w: \d.\d+KiB}`, gotMeterData.Load())

	readIndexSum := getStepSummary(t, taskManager, task.ID, proto.BackfillStepReadIndex)
	mergeSum := getStepSummary(t, taskManager, task.ID, proto.BackfillStepMergeSort)
	ingestSum := getStepSummary(t, taskManager, task.ID, proto.BackfillStepWriteAndIngest)
	require.EqualValues(t, 1, readIndexSum.GetReqCnt.Load())
	require.EqualValues(t, 3, readIndexSum.PutReqCnt.Load())
	require.Greater(t, readIndexSum.ReadBytes.Load(), int64(0))
	require.EqualValues(t, 153, readIndexSum.Bytes.Load())
	require.EqualValues(t, 3, readIndexSum.RowCnt.Load())

	require.EqualValues(t, 3, mergeSum.GetReqCnt.Load())
	require.EqualValues(t, 3, mergeSum.PutReqCnt.Load())
	require.EqualValues(t, 0, mergeSum.ReadBytes.Load())
	require.EqualValues(t, 0, mergeSum.Bytes.Load())

	require.EqualValues(t, 3, ingestSum.GetReqCnt.Load())
	require.EqualValues(t, 0, ingestSum.PutReqCnt.Load())
	require.EqualValues(t, 0, ingestSum.ReadBytes.Load())
	require.EqualValues(t, 0, ingestSum.Bytes.Load())

	require.Eventually(t, func() bool {
		items := *rowAndSizeMeterItems.Load()
		return items != nil && items["row_count"].(int64) == 3 &&
			items["index_kv_bytes"].(int64) == 153 &&
			items[metering.ConcurrencyField].(int) == task.Concurrency &&
			items[metering.MaxNodeCountField].(int) == task.MaxNodeCount &&
			items[metering.DurationSecondsField].(int64) > 0
	}, 30*time.Second, 100*time.Millisecond)
}

func getStepSummary(t *testing.T, taskMgr *diststorage.TaskManager, taskID int64, step proto.Step) *execute.SubtaskSummary {
	t.Helper()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	subtasks, err := taskMgr.GetSubtasksWithHistory(ctx, taskID, step)
	require.NoError(t, err)
	var accumSummary execute.SubtaskSummary
	for _, subtask := range subtasks {
		v := &execute.SubtaskSummary{}
		require.NoError(t, json.Unmarshal([]byte(subtask.Summary), &v))
		accumSummary.RowCnt.Add(v.RowCnt.Load())
		accumSummary.Bytes.Add(v.Bytes.Load())
		accumSummary.ReadBytes.Add(v.ReadBytes.Load())
		accumSummary.PutReqCnt.Add(v.PutReqCnt.Load())
		accumSummary.GetReqCnt.Add(v.GetReqCnt.Load())
	}
	return &accumSummary
}
