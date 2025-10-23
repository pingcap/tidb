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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
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
	tk1.MustExec("alter table t modify column b smallint;")
	require.True(t, analyzed)
}

func TestMultiSchemaChangeAnalyzeOnlyOnce(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_stats_update_during_ddl = true;")
	tk1.MustExec("set @@sql_mode = '';")
	dbCnt := 0

	checkFn := func(sql, containRes string) {
		dbCnt++
		dbName := fmt.Sprintf("test_%d", dbCnt)
		tk1.MustExec("drop database if exists " + dbName)
		tk1.MustExec("create database " + dbName)
		defer tk1.MustExec("drop database " + dbName)
		tk1.MustExec("use " + dbName)
		tk1.MustExec("create table t (a int, b int, c char(6), key i_a(a), key i_b(b), key i_c(c));")
		tk1.MustExec("insert into t values (1, 1, '111111');")
		beginRs := tk1.MustQuery("select now();").Rows()
		begin := beginRs[0][0].(string)
		tk1.MustExec(sql)
		analyzeStatusRs := tk1.MustQuery(
			fmt.Sprintf("show analyze status where start_time >= '%s' and table_schema = '%s';", begin, dbName)).Rows()
		if containRes == "" {
			require.Len(t, analyzeStatusRs, 0)
			return
		}
		require.Len(t, analyzeStatusRs, 1)
		require.Contains(t, analyzeStatusRs[0][3].(string), containRes)
	}

	checkFn("alter table t add index i_a_2(a), add index i_b_2(b), modify column c char(5);", "all columns")
	checkFn("alter table t modify column c char(5), modify column a smallint;", "all columns")
	checkFn("alter table t modify column c char(5), modify column a bigint, modify column b bigint;", "all columns")
	checkFn("alter table t modify column a bigint, modify column c char(5), modify column b bigint;", "all columns")
	checkFn("alter table t modify column a bigint, modify column b bigint;", "") // no lossy change
}
