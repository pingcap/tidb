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

package addindextest_test

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func TestMultiSchemaChangeTwoIndexes(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg=on;")
	tk.MustExec("set @@global.tidb_enable_dist_task=on;")

	createTables := []string{
		"create table t (id int, b int, c int, primary key(id) clustered);",
	}
	createIndexes := []string{
		"alter table t add unique index b(b), add index c(c);",
	}

	for i := range createTables {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(createTables[i])
		tk.MustExec("insert into t values (1,1,1)")

		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test;")
		runDMLBeforeScan := false
		runDMLBeforeMerge := false

		var hexKey string
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionBeforeScanV2", func() {
			if runDMLBeforeScan {
				return
			}
			runDMLBeforeScan = true

			rows := tk1.MustQuery("select tidb_encode_index_key('test', 't', 'b', 1, null);").Rows()
			hexKey = rows[0][0].(string)
			_, err := tk1.Exec("delete from t where id = 1;")
			assert.NoError(t, err)
			_, err = tk1.Exec("insert into t values (2,1,1);")
			assert.NoError(t, err)
			rs := tk.MustQuery(fmt.Sprintf("select tidb_mvcc_info('%s')", hexKey)).Rows()
			t.Log("after first insertion", rs[0][0].(string))
			_, err = tk1.Exec("delete from t where id = 2;")
			assert.NoError(t, err)
			rs = tk.MustQuery(fmt.Sprintf("select tidb_mvcc_info('%s')", hexKey)).Rows()
			t.Log("after second insertion", rs[0][0].(string))
		})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/BeforeBackfillMerge", func() {
			if runDMLBeforeMerge {
				return
			}
			runDMLBeforeMerge = true
			_, err := tk1.Exec("insert into t values (3, 1, 1);")
			assert.NoError(t, err)
			rs := tk.MustQuery(fmt.Sprintf("select tidb_mvcc_info('%s')", hexKey)).Rows()
			t.Log("after third insertion", rs[0][0].(string))
		})

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/skipReorgWorkForTempIndex", "return(false)")

		tk.MustExec(createIndexes[i])
		tk.MustExec("admin check table t;")
	}
}

func TestFixAdminAlterDDLJobs(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec("create table t (a int);")
	tk1.MustExec("insert into t values (1);")
	tk1.MustExec("set @@global.tidb_enable_dist_task=off;")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(100)")

	testCases := []struct {
		stuckFp          string
		checkReorgMetaFp string
		sql              string
		setVars          string
		revertVars       string
	}{
		{
			stuckFp:          "github.com/pingcap/tidb/pkg/ddl/mockIndexIngestWorkerFault",
			checkReorgMetaFp: "github.com/pingcap/tidb/pkg/ddl/checkReorgConcurrency",
			sql:              "alter table t add index idx_a(a)",
		},
		{
			stuckFp:          "github.com/pingcap/tidb/pkg/ddl/mockUpdateColumnWorkerStuck",
			checkReorgMetaFp: "github.com/pingcap/tidb/pkg/ddl/checkReorgWorkerCnt",
			sql:              "alter table t modify a varchar(30)",
		},
		{
			stuckFp:          "github.com/pingcap/tidb/pkg/ddl/mockAddIndexTxnWorkerStuck",
			checkReorgMetaFp: "github.com/pingcap/tidb/pkg/ddl/checkReorgWorkerCnt",
			sql:              "alter table t add index idx(a)",
			setVars:          "set @@global.tidb_ddl_enable_fast_reorg=off",
			revertVars:       "set @@global.tidb_ddl_enable_fast_reorg=on",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.stuckFp, func(t *testing.T) {
			if tc.setVars != "" {
				tk1.MustExec(tc.setVars)
			}

			ch := make(chan struct{})
			testfailpoint.EnableCall(t, tc.stuckFp, func() {
				<-ch
			})
			var wg util.WaitGroupWrapper
			wg.Run(func() {
				tk1.MustExec(tc.sql)
			})
			var (
				realWorkerCnt     atomic.Int64
				realBatchSize     atomic.Int64
				realMaxWriteSpeed atomic.Int64
			)
			testfailpoint.EnableCall(t, tc.checkReorgMetaFp, func(j *model.Job) {
				realWorkerCnt.Store(int64(j.ReorgMeta.GetConcurrencyOrDefault(0)))
				realBatchSize.Store(int64(j.ReorgMeta.GetBatchSizeOrDefault(0)))
				realMaxWriteSpeed.Store(int64(j.ReorgMeta.GetMaxWriteSpeedOrDefault()))
			})

			jobID := ""
			tk2 := testkit.NewTestKit(t, store)
			for {
				row := tk2.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
				if len(row) == 1 {
					jobID = row[0][0].(string)
					break
				}
			}
			workerCnt := int64(7)
			batchSize := int64(89)
			maxWriteSpeed := int64(1011)
			tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s thread = %d", jobID, workerCnt))
			tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s batch_size = %d", jobID, batchSize))
			tk2.MustExec(fmt.Sprintf("admin alter ddl jobs %s max_write_speed = %d", jobID, maxWriteSpeed))
			require.Eventually(t, func() bool {
				return realWorkerCnt.Load() == workerCnt && realBatchSize.Load() == batchSize && realMaxWriteSpeed.Load() == maxWriteSpeed
			}, 30*time.Second, time.Millisecond*100)
			close(ch)
			wg.Wait()
			if tc.revertVars != "" {
				tk1.MustExec(tc.revertVars)
			}
		})
	}
	tk1.MustExec("set @@global.tidb_enable_dist_task = on;")
	tk1.MustExec("drop table t;")
}

func TestAddIndexShowAnalyzeProgress(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec("create table t (a int, b int, key idx_b(b));")
	tk1.MustExec("insert into t values (1, 1), (2, 2), (3, 3);")
	tk1.MustExec("set @@tidb_stats_update_during_ddl = 1;")
	beginRs := tk1.MustQuery("select now();").Rows()
	begin := beginRs[0][0].(string)
	jobID := int64(0)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if jobID == 0 && job.Type == model.ActionModifyColumn {
			jobID = job.ID
		}
	})
	analyzed := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/statistics/handle/storage/saveAnalyzeResultToStorage", func() {
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		analyzeStatusRs := tk2.MustQuery(
			fmt.Sprintf("show analyze status where start_time >= '%s';", begin)).Rows()
		require.Equal(t, analyzeStatusRs[0][7].(string), "running")
		showRs := tk2.MustQuery(fmt.Sprintf("admin show ddl jobs where job_id = %d", jobID)).Rows()
		show := showRs[0][12].(string)
		require.Contains(t, show, "analyzing")
		analyzed = true
	})
	tk1.MustExec("alter table t modify column b char(16);")
	require.True(t, analyzed)

	tk1.MustExec("drop table if exists t;")
	tk1.MustExec("create table t (a int, b int, key idx_b(b));")
	tk1.MustExec("insert into t values (1, 1), (2, 2), (3, 3);")
	beginRs = tk1.MustQuery("select now();").Rows()
	begin = beginRs[0][0].(string)
	jobID = int64(0)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if jobID == 0 && job.Type == model.ActionModifyColumn {
			jobID = job.ID
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterAnalyzeTable", func(err *error) {
		*err = errors.New("mock err")
	})
	tk1.MustExec("alter table t modify column b char(16);")
	require.True(t, analyzed)
	showRs := tk1.MustQuery(fmt.Sprintf("admin show ddl jobs where job_id = %d", jobID)).Rows()
	show := showRs[0][12].(string)
	require.Contains(t, show, "analyze_failed")
}

func TestAnalyzeTimeout(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t_timeout;")
	tk1.MustExec("create table t_timeout (a int, b varchar(16), key idx_b(b));")
	tk1.MustExec("insert into t_timeout values (1, '1'), (2, '2'), (3, '3');")
	tk1.MustExec("set @@tidb_stats_update_during_ddl = 1;")

	jobID := int64(0)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if jobID == 0 && job.Type == model.ActionModifyColumn {
			jobID = job.ID
		}
	})

	oldInterval := ddl.DefaultAnalyzeCheckInterval
	ddl.DefaultAnalyzeCheckInterval = 10 * time.Millisecond
	defer func() {
		ddl.DefaultAnalyzeCheckInterval = oldInterval
	}()

	analyzedNotify := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterAnalyzeTable", func(*error) {
		// wait an extra second because analyze start_time is compared at second granularity
		time.Sleep(1 * time.Second)
		close(analyzedNotify)
	})

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAnalyzeTable", func() {
		time.Sleep(100 * time.Millisecond)
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAnalyzeTimeout", "return(1)")

	tk1.MustExec("alter table t_timeout modify column b char(16);")

	require.Eventually(t, func() bool {
		if jobID == 0 {
			return false
		}
		rows := tk1.MustQuery(fmt.Sprintf("admin show ddl jobs where job_id = %d", jobID)).Rows()
		if len(rows) == 0 {
			return false
		}
		show := rows[0][12].(string)
		return strings.Contains(show, "analyze_timeout")
	}, 30*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		rows := tk1.MustQuery("show stats_meta where table_name = 't_timeout'").Rows()
		return len(rows) > 0
	}, time.Minute, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		select {
		case <-analyzedNotify:
			return true
		default:
			return false
		}
	}, 30*time.Second, 200*time.Millisecond)

	jobID = 0
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if jobID == 0 && job.Type == model.ActionAddIndex {
			jobID = job.ID
		}
	})
	tk1.MustExec("alter table t_timeout add index new_idx_b(b);")
	require.Eventually(t, func() bool {
		require.Greater(t, jobID, int64(0))
		rows := tk1.MustQuery(fmt.Sprintf("admin show ddl jobs where job_id = %d", jobID)).Rows()
		if len(rows) == 0 {
			return false
		}
		show := rows[0][12].(string)
		return strings.Contains(show, "analyze_timeout")
	}, 30*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		rows := tk1.MustQuery("show stats_meta where table_name = 't_timeout'").Rows()
		return len(rows) > 0
	}, time.Minute, 200*time.Millisecond)
}

func TestMultiSchemaChangeAnalyzeOnlyOnce(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_stats_update_during_ddl = true;")
	dbCnt := 0

	checkFn := func(sql, containRes string) {
		dbCnt++
		dbName := fmt.Sprintf("test_%d", dbCnt)
		tk1.MustExec("drop database if exists " + dbName)
		tk1.MustExec("create database " + dbName)
		defer tk1.MustExec("drop database " + dbName)
		tk1.MustExec("use " + dbName)
		tk1.MustExec("create table t (a bigint, b bigint, c bigint, d bigint, key i_a(a), key i_b(b), key i_c(c));")
		tk1.MustExec("insert into t values (1, 1, 11111, 1);")
		beginRs := tk1.MustQuery("select now();").Rows()
		begin := beginRs[0][0].(string)
		tk1.MustExec(sql)
		analyzeStatusRs := tk1.MustQuery(
			fmt.Sprintf("show analyze status where start_time >= '%s' and table_schema = '%s';", begin, dbName)).Rows()
		if len(containRes) == 0 {
			require.Len(t, analyzeStatusRs, 0)
			return
		}
		require.Len(t, analyzeStatusRs, 1)
		analyzeStr := analyzeStatusRs[0][3].(string)
		require.Contains(t, analyzeStr, containRes)
	}

	// Index reorg.
	checkFn("alter table t modify column a int unsigned", "a")
	checkFn("alter table t add index i_a_2(a), add index i_b_2(b), modify column c char(5), modify column d char(5)", "all columns")
	checkFn("alter table t modify column c int, modify column a char(5), add index i_d_1(d)", "all columns")
	checkFn("alter table t modify column c char(5), modify column a int, modify column b int", "all columns")
	checkFn("alter table t modify column a bigint, modify column c char(5), modify column b int unsigned", "all columns")
	checkFn("alter table t modify column a char(5), modify column d char(5)", "all columns")
	checkFn("alter table t modify column a int, modify column b int unsigned", "all columns")

	// No index reorg.
	checkFn("alter table t modify column a int", "")
	checkFn("alter table t modify column a bigint", "")
	checkFn("alter table t modify column a int, modify column d char(5)", "")
	checkFn("alter table t modify column a int, modify column d int", "")
}

func TestCancelAfterReorgTimeout(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("create view all_global_tasks as select * from mysql.tidb_global_task union all select * from mysql.tidb_global_task_history;")
	defer tk.MustExec("drop view if exists all_global_tasks;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1);")

	// Mock subtask executor encounter the same error continuously.
	afterMeetErr := false
	meetErr := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeReadIndexStepExecRunSubtask", func(err *error) {
		*err = errors.New("mock err")
		if !afterMeetErr {
			meetErr <- struct{}{}
			afterMeetErr = true
		}
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(10)") // Speed up the test.
	var jobID int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionAddIndex {
			return
		}
		jobID = job.ID
	})
	afterTimeout := false
	timeout := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/handle/afterDXFTaskSubmitted", func() {
		<-meetErr
		<-timeout
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test;")
		tk1.MustExec(fmt.Sprintf("admin cancel ddl jobs %d;", jobID))
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onRunReorgJobTimeout", func() {
		if !afterTimeout {
			timeout <- struct{}{}
			afterTimeout = true
		}
	})
	tk.MustGetErrCode("alter table t add index idx(a);", errno.ErrCancelledDDLJob)
	require.Eventually(t, func() bool {
		result := tk.MustQuery("select state from all_global_tasks;").Rows()
		require.Greater(t, len(result), 0)
		state := result[0][0].(string)
		done := state == proto.TaskStateSucceed.String() ||
			state == proto.TaskStateReverted.String() ||
			state == proto.TaskStateFailed.String()
		return done
	}, 10*time.Second, 300*time.Millisecond)
}
