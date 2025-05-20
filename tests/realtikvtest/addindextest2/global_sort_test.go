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
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	diststorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
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
		dataFiles, statFiles, err := external.GetAllFileNames(context.Background(), extStore, prefix)
		require.NoError(t, err)
		require.Greater(t, jobID, int64(0))
		require.Equal(t, 0, len(dataFiles))
		require.Equal(t, 0, len(statFiles))
	}
}

func checkFileExist(t *testing.T, sortStorageURI string, prefix string) {
	storeBackend, err := storage.ParseBackend(sortStorageURI, nil)
	require.NoError(t, err)
	extStore, err := storage.NewWithDefaultOpt(context.Background(), storeBackend)
	require.NoError(t, err)
	dataFiles, _, err := external.GetAllFileNames(context.Background(), extStore, prefix)
	require.NoError(t, err)
	require.Greater(t, len(dataFiles), 0)
}

func checkDataAndShowJobs(t *testing.T, tk *testkit.TestKit, count int) {
	tk.MustExec("admin check table t;")
	rs := tk.MustQuery("admin show ddl jobs 1;").Rows()
	require.Len(t, rs, 1)
	require.Contains(t, rs[0][12], "ingest")
	require.Contains(t, rs[0][12], "cloud")
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
	task, err := mgr.GetTaskByKeyWithHistory(ctx, fmt.Sprintf("ddl/backfill/%d", jobID))
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
	tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
	tk.MustExec(fmt.Sprintf(`set @@global.tidb_cloud_storage_uri = "%s"`, cloudStorageURI))
	defer func() {
		tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
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
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID))+"/plan/ingest")
	<-ch
	<-ch
	checkFileCleaned(t, jobID, taskID, cloudStorageURI)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()")
	tk.MustExec("alter table t add index idx1(a);")
	checkDataAndShowJobs(t, tk, size)
	checkExternalFields(t, tk)
	taskID = getTaskID(t, jobID)
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID))+"/plan/ingest")
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID))+"/plan/merge-sort")
	<-ch
	<-ch
	checkFileCleaned(t, jobID, taskID, cloudStorageURI)

	tk.MustExec("alter table t add unique index idx2(a);")
	checkDataAndShowJobs(t, tk, size)
	checkExternalFields(t, tk)
	taskID = getTaskID(t, jobID)
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID))+"/plan/ingest")
	checkFileExist(t, cloudStorageURI, strconv.Itoa(int(taskID))+"/plan/merge-sort")
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
			if kerneltype.IsNextGen() && tc.cloudStorageURI == "" {
				t.Skip("local sort might ingest duplicate KV, cause overlapped sst")
			}
			tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = " + tc.enableFastReorg + ";")
			tk.MustExec("set @@global.tidb_enable_dist_task = " + tc.enableDistTask + ";")
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

	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '';")
}

func TestAddIndexIngestShowReorgTp(t *testing.T) {
	_, cloudStorageURI := genServerWithStorage(t)

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec("set @@global.tidb_cloud_storage_uri = '" + cloudStorageURI + "';")
	tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = 1;")

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
	require.True(t, strings.Contains(jobType, "ingest"), jobType)
	require.False(t, strings.Contains(jobType, "cloud"), jobType)
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
	tk.MustExec(`set @@global.tidb_ddl_enable_fast_reorg = 1;`)
	tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
	tk.MustExec(fmt.Sprintf(`set @@global.tidb_cloud_storage_uri = "%s"`, cloudStorageURI))
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("set @@session.tidb_scatter_region = 'table'")
	t.Cleanup(func() {
		tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
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
		tk.MustExec("set session tidb_redact_log = on;")
		tk.MustContainErrMsg(addUniqueKeySQL, "[kv:1062]Duplicate entry '?' for key 't.idx'")
		tk.MustExec("set session tidb_redact_log = off;")
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
	tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
	tk.MustExec(fmt.Sprintf(`set @@global.tidb_cloud_storage_uri = "%s"`, cloudStorageURI))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/forceMergeSort", "return()")
	defer func() {
		tk.MustExec("set @@global.tidb_enable_dist_task = 0;")
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
	tk.MustExec("set global tidb_enable_dist_task = on;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = off;")
	})
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg = on;`)
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

func TestAlterJobOnDXWithGlobalSort(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", `return(16)`)
	testutil.ReduceCheckInterval(t)

	server, cloudStorageURI := genServerWithStorage(t)
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global tidb_enable_dist_task = on;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = off;")
	})
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg = on;`)
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
	tk.MustExec("set global tidb_ddl_reorg_max_write_speed = '256MiB'")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_ddl_reorg_max_write_speed = 0")
	})

	var pipeClosed bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", func(pipe *operator.AsyncPipeline) {
		pipeClosed = true
		reader, writer := pipe.GetReaderAndWriter()
		require.EqualValues(t, 4, reader.GetWorkerPoolSize())
		require.EqualValues(t, 6, writer.GetWorkerPoolSize())
	})

	// Change the batch size and concurrency during table scanning and check the modified parameters.
	var modified atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterDetectAndHandleParamModify", func() {
		require.False(t, modified.Load())
		modified.Store(true)
	})
	var onceScan sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/scanRecordExec", func(reorgMeta *model.DDLReorgMeta) {
		onceScan.Do(func() {
			tk1 := testkit.NewTestKit(t, store)
			rows := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
			require.Len(t, rows, 1)
			tk1.MustExec(fmt.Sprintf("admin alter ddl jobs %s thread = 8, batch_size = 256", rows[0][0]))
			require.Eventually(t, func() bool {
				return modified.Load()
			}, 20*time.Second, 100*time.Millisecond)
			require.Equal(t, 256, reorgMeta.GetBatchSize())
		})
	})

	tk.MustExec("alter table gsort add index idx(a)")
	require.True(t, pipeClosed)
	require.True(t, modified.Load())
	tk.MustExec("admin check index gsort idx")
}
