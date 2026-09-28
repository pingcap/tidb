// Copyright 2015 PingCAP, Inc.
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

package ddl_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const testLease = 5 * time.Second

func TestDDLJobRU(t *testing.T) {
	requireExpectedJobRU := func(t *testing.T, ru float64) {
		t.Helper()
		if kerneltype.IsNextGen() {
			require.Positive(t, ru)
			return
		}
		require.Zero(t, ru)
	}
	expectedMetricRU := func(ru float64) float64 {
		if kerneltype.IsNextGen() {
			return ru
		}
		return 0
	}

	t.Run("general job persists active RU unchanged", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		totalRUBefore := testutil.ToFloat64(metrics.RUV2Total)
		ddlRUBefore := testutil.ToFloat64(metrics.RUV2BySQLTypeDDL)
		tikvRUBefore := testutil.ToFloat64(metrics.RUV2ByEngineTiKV)

		var mu sync.Mutex
		var jobID int64
		var activeRU float64
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, updateErr *error) {
			if job.Type != model.ActionCreateTable || job.TableName != "t_ddl_ru_general" || *updateErr != nil {
				return
			}
			mu.Lock()
			jobID = job.ID
			activeRU = job.RU
			mu.Unlock()
		})

		tk.MustExec("create table t_ddl_ru_general (a int)")

		mu.Lock()
		capturedJobID, capturedActiveRU := jobID, activeRU
		mu.Unlock()
		require.NotZero(t, capturedJobID)
		requireExpectedJobRU(t, capturedActiveRU)
		historyJob, err := ddl.GetHistoryJobByID(tk.Session(), capturedJobID)
		require.NoError(t, err)
		require.NotNil(t, historyJob)
		require.Equal(t, capturedActiveRU, historyJob.RU)
		require.InDelta(t, expectedMetricRU(historyJob.RU),
			testutil.ToFloat64(metrics.RUV2Total)-totalRUBefore, 1e-9)
		require.InDelta(t, expectedMetricRU(historyJob.RU),
			testutil.ToFloat64(metrics.RUV2BySQLTypeDDL)-ddlRUBefore, 1e-9)
		require.InDelta(t, expectedMetricRU(historyJob.RU),
			testutil.ToFloat64(metrics.RUV2ByEngineTiKV)-tikvRUBefore, 1e-9)
	})

	t.Run("reorg job accounts transaction RU v2", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_ddl_ru_reorg (a int)")

		var mu sync.Mutex
		var jobID int64
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, updateErr *error) {
			if job.Type != model.ActionAddIndex || job.TableName != "t_ddl_ru_reorg" || *updateErr != nil {
				return
			}
			mu.Lock()
			jobID = job.ID
			mu.Unlock()
		})

		tk.MustExec("alter table t_ddl_ru_reorg add index idx(a)")

		mu.Lock()
		capturedJobID := jobID
		mu.Unlock()
		require.NotZero(t, capturedJobID)
		historyJob, err := ddl.GetHistoryJobByID(tk.Session(), capturedJobID)
		require.NoError(t, err)
		require.NotNil(t, historyJob)
		requireExpectedJobRU(t, historyJob.RU)
	})

	t.Run("transactional backfill jobs account RU", func(t *testing.T) {
		cases := []struct {
			name    string
			jobType model.ActionType
			table   string
			setup   []string
			alter   string
		}{
			{
				name:    "reorganize partition",
				jobType: model.ActionReorganizePartition,
				table:   "t_ddl_ru_reorg_part",
				setup: []string{
					"create table t_ddl_ru_reorg_part (a int, b int, key idx_b(b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))",
					"insert into t_ddl_ru_reorg_part values (1,1),(5,5),(11,11),(15,15)",
				},
				alter: "alter table t_ddl_ru_reorg_part reorganize partition p0,p1 into (partition p0 values less than (10), partition p1 values less than (15), partition p2 values less than (20))",
			},
			{
				name:    "remove partitioning",
				jobType: model.ActionRemovePartitioning,
				table:   "t_ddl_ru_rm_part",
				setup: []string{
					"create table t_ddl_ru_rm_part (a int, b int, key idx_b(b)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))",
					"insert into t_ddl_ru_rm_part values (1,1),(5,5),(11,11),(15,15)",
				},
				alter: "alter table t_ddl_ru_rm_part remove partitioning",
			},
			{
				name:    "convert to partitioned",
				jobType: model.ActionAlterTablePartitioning,
				table:   "t_ddl_ru_add_part",
				setup: []string{
					"create table t_ddl_ru_add_part (a int, b int, key idx_b(b))",
					"insert into t_ddl_ru_add_part values (1,1),(5,5),(11,11),(15,15)",
				},
				alter: "alter table t_ddl_ru_add_part partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))",
			},
			{
				name:    "drop partition global index cleanup",
				jobType: model.ActionDropTablePartition,
				table:   "t_ddl_ru_drop_part",
				setup: []string{
					"create table t_ddl_ru_drop_part (a int, b int, unique key idx_b(b) global) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))",
					"insert into t_ddl_ru_drop_part values (1,1),(5,5),(11,11),(15,15)",
				},
				alter: "alter table t_ddl_ru_drop_part drop partition p0",
			},
			{
				name:    "truncate partition global index cleanup",
				jobType: model.ActionTruncateTablePartition,
				table:   "t_ddl_ru_truncate_part",
				setup: []string{
					"create table t_ddl_ru_truncate_part (a int, b int, unique key idx_b(b) global) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))",
					"insert into t_ddl_ru_truncate_part values (1,1),(5,5),(11,11),(15,15)",
				},
				alter: "alter table t_ddl_ru_truncate_part truncate partition p0",
			},
			{
				name:    "modify column reorg",
				jobType: model.ActionModifyColumn,
				table:   "t_ddl_ru_mod_col",
				setup: []string{
					"create table t_ddl_ru_mod_col (a timestamp default '2020-07-10 01:05:08', b int, key idx_a(a))",
					"insert into t_ddl_ru_mod_col (b) values (1),(2),(3)",
				},
				alter: "alter table t_ddl_ru_mod_col modify column a bigint",
			},
			{
				// Without a related index only the row backfill worker runs, which
				// isolates that worker's transaction accounting.
				name:    "modify column reorg without index",
				jobType: model.ActionModifyColumn,
				table:   "t_ddl_ru_mod_col_no_idx",
				setup: []string{
					"create table t_ddl_ru_mod_col_no_idx (a timestamp default '2020-07-10 01:05:08', b int)",
					"insert into t_ddl_ru_mod_col_no_idx (b) values (1),(2),(3)",
				},
				alter: "alter table t_ddl_ru_mod_col_no_idx modify column a bigint",
			},
		}

		checkReorgRU := func(t *testing.T, jobType model.ActionType, table string, setup []string, alter string, beforeAlter func()) {
			t.Helper()
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for _, stmt := range setup {
				tk.MustExec(stmt)
			}
			if beforeAlter != nil {
				beforeAlter()
			}

			var mu sync.Mutex
			var jobID int64
			var backfillTxnCalls int
			var backfillTxnBytes int
			var stagedRU float64
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, updateErr *error) {
				if job.Type != jobType || job.TableName != table || *updateErr != nil {
					return
				}
				mu.Lock()
				jobID = job.ID
				mu.Unlock()
			})
			// The accounting hook fires for every committed reorg backfill
			// transaction, before the NextGen gate, so this also proves the
			// reorg workers actually route their txn bytes through it.
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/accountBackfillTxnRU", func(_ int64, writtenBytes int) {
				mu.Lock()
				backfillTxnCalls++
				if writtenBytes > 0 {
					backfillTxnBytes += writtenBytes
				}
				mu.Unlock()
			})
			// Records the RU actually persisted from the staged reorganization
			// result, so the test can prove every accounted byte reached Job.RU.
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/accountPendingReorgRU", func(_ int64, ru float64, persisted bool) {
				if !persisted {
					return
				}
				mu.Lock()
				stagedRU += ru
				mu.Unlock()
			})

			tk.MustExec(alter)

			mu.Lock()
			capturedJobID, capturedCalls, capturedBytes, capturedStagedRU := jobID, backfillTxnCalls, backfillTxnBytes, stagedRU
			mu.Unlock()
			require.NotZero(t, capturedJobID)
			require.Positive(t, capturedCalls)
			require.Positive(t, capturedBytes)
			historyJob, err := ddl.GetHistoryJobByID(tk.Session(), capturedJobID)
			require.NoError(t, err)
			require.NotNil(t, historyJob)
			requireExpectedJobRU(t, historyJob.RU)
			if kerneltype.IsNextGen() {
				weight := config.GetGlobalConfig().RUV2.DDLWeights.TxnKVBytes
				require.InDelta(t, float64(capturedBytes)*weight, capturedStagedRU, 1e-6,
					"every accounted reorg byte must be persisted to Job.RU")
			}
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				checkReorgRU(t, tc.jobType, tc.table, tc.setup, tc.alter, nil)
			})
		}

		t.Run("add index in txn mode", func(t *testing.T) {
			originalEnableDistTask := vardef.EnableDistTask.Load()
			originalEnableFastReorg := vardef.EnableFastReorg.Load()
			t.Cleanup(func() {
				vardef.EnableDistTask.Store(originalEnableDistTask)
				vardef.EnableFastReorg.Store(originalEnableFastReorg)
			})
			rows := make([]string, 0, 200)
			for i := range 200 {
				rows = append(rows, "("+strconv.Itoa(i)+")")
			}
			checkReorgRU(t, model.ActionAddIndex, "t_ddl_ru_add_idx_txn", []string{
				"create table t_ddl_ru_add_idx_txn (a int)",
				"insert into t_ddl_ru_add_idx_txn values " + strings.Join(rows, ","),
			}, "alter table t_ddl_ru_add_idx_txn add index idx_a(a)", func() {
				vardef.EnableDistTask.Store(false)
				vardef.EnableFastReorg.Store(false)
			})
		})

		t.Run("modify column reorg in txn mode", func(t *testing.T) {
			// With both optimizations off, the index reorg falls back to the
			// transaction backfill path that has no separate merge stage.
			originalEnableDistTask := vardef.EnableDistTask.Load()
			originalEnableFastReorg := vardef.EnableFastReorg.Load()
			t.Cleanup(func() {
				vardef.EnableDistTask.Store(originalEnableDistTask)
				vardef.EnableFastReorg.Store(originalEnableFastReorg)
			})
			// Use enough rows so both the row and index backfill stages report a
			// non-zero byte count.
			rows := make([]string, 0, 200)
			for i := range 200 {
				rows = append(rows, "("+strconv.Itoa(i)+")")
			}
			checkReorgRU(t, model.ActionModifyColumn, "t_ddl_ru_mod_col_txn", []string{
				"create table t_ddl_ru_mod_col_txn (a timestamp default '2020-07-10 01:05:08', b int, key idx_a(a))",
				"insert into t_ddl_ru_mod_col_txn (b) values " + strings.Join(rows, ","),
			}, "alter table t_ddl_ru_mod_col_txn modify column a bigint", func() {
				vardef.EnableDistTask.Store(false)
				vardef.EnableFastReorg.Store(false)
			})
		})
	})

	t.Run("commit retry reloads durable RU", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")

		const commitFailpoint = "github.com/pingcap/tidb/pkg/session/mockCommitError8942"
		var armOnce sync.Once
		var armMu sync.Mutex
		var armErr error
		var armed bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
			if job.Type != model.ActionCreateTable || job.TableName != "t_ddl_ru_retry" {
				return
			}
			armOnce.Do(func() {
				err := failpoint.Enable(commitFailpoint, `1*return(true)`)
				armMu.Lock()
				armErr = err
				armed = err == nil
				armMu.Unlock()
				if err == nil {
					t.Cleanup(func() {
						require.NoError(t, failpoint.Disable(commitFailpoint))
					})
				}
			})
		})

		var observationsMu sync.Mutex
		var jobIDs []int64
		var ruValues []float64
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, updateErr *error) {
			if job.Type != model.ActionCreateTable || job.TableName != "t_ddl_ru_retry" || *updateErr != nil {
				return
			}
			observationsMu.Lock()
			jobIDs = append(jobIDs, job.ID)
			ruValues = append(ruValues, job.RU)
			observationsMu.Unlock()
		})

		tk.MustExec("create table t_ddl_ru_retry (a int)")

		armMu.Lock()
		capturedArmErr := armErr
		capturedArmed := armed
		armMu.Unlock()
		require.NoError(t, capturedArmErr)
		require.True(t, capturedArmed)
		observationsMu.Lock()
		capturedJobIDs := append([]int64(nil), jobIDs...)
		capturedRUValues := append([]float64(nil), ruValues...)
		observationsMu.Unlock()
		require.GreaterOrEqual(t, len(capturedJobIDs), 2)
		require.Equal(t, capturedJobIDs[0], capturedJobIDs[1])
		require.Equal(t, capturedRUValues[0], capturedRUValues[1])
		requireExpectedJobRU(t, capturedRUValues[0])
		historyJob, err := ddl.GetHistoryJobByID(tk.Session(), capturedJobIDs[0])
		require.NoError(t, err)
		require.NotNil(t, historyJob)
		require.Equal(t, capturedRUValues[len(capturedRUValues)-1], historyJob.RU)
	})

	t.Run("history commit retry publishes RU once", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		totalRUBefore := testutil.ToFloat64(metrics.RUV2Total)
		ddlRUBefore := testutil.ToFloat64(metrics.RUV2BySQLTypeDDL)
		tikvRUBefore := testutil.ToFloat64(metrics.RUV2ByEngineTiKV)

		const commitFailpoint = "github.com/pingcap/tidb/pkg/session/mockCommitError8942"
		var armOnce sync.Once
		var mu sync.Mutex
		var armErr error
		var armed bool
		var jobID int64
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterFinishDDLJob", func(job *model.Job) {
			if job.Type != model.ActionCreateTable || job.TableName != "t_ddl_ru_history_retry" || !job.IsSynced() {
				return
			}
			mu.Lock()
			jobID = job.ID
			mu.Unlock()
			armOnce.Do(func() {
				err := failpoint.Enable(commitFailpoint, `1*return(true)`)
				mu.Lock()
				armErr = err
				armed = err == nil
				mu.Unlock()
				if err == nil {
					t.Cleanup(func() {
						require.NoError(t, failpoint.Disable(commitFailpoint))
					})
				}
			})
		})

		tk.MustExec("create table t_ddl_ru_history_retry (a int)")

		mu.Lock()
		capturedArmErr, capturedArmed, capturedJobID := armErr, armed, jobID
		mu.Unlock()
		require.NoError(t, capturedArmErr)
		require.True(t, capturedArmed)
		require.NotZero(t, capturedJobID)
		historyJob, err := ddl.GetHistoryJobByID(tk.Session(), capturedJobID)
		require.NoError(t, err)
		require.NotNil(t, historyJob)
		requireExpectedJobRU(t, historyJob.RU)
		require.InDelta(t, expectedMetricRU(historyJob.RU),
			testutil.ToFloat64(metrics.RUV2Total)-totalRUBefore, 1e-9)
		require.InDelta(t, expectedMetricRU(historyJob.RU),
			testutil.ToFloat64(metrics.RUV2BySQLTypeDDL)-ddlRUBefore, 1e-9)
		require.InDelta(t, expectedMetricRU(historyJob.RU),
			testutil.ToFloat64(metrics.RUV2ByEngineTiKV)-tikvRUBefore, 1e-9)
	})
}

func TestCheckOwner(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	time.Sleep(testLease)
	require.Equal(t, dom.DDL().OwnerManager().IsOwner(), true)
	require.Equal(t, dom.GetSchemaLease(), testLease)
}

func TestInvalidDDLJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            0,
		TableID:             0,
		Type:                model.ActionNone,
		BinlogInfo:          &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{Database: "db", Table: "table"}},
	}
	ctx := testkit.NewSession(t, store)
	ctx.SetValue(sessionctx.QueryString, "skip")
	de := dom.DDLExecutor().(ddl.ExecutorForTest)
	err := de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, &model.EmptyArgs{}, true))
	require.ErrorContains(t, err, "[ddl:8204]invalid ddl job type: none")
}

func TestAddBatchJobError(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	ctx := testkit.NewSession(t, store)

	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/jobsubmit/mockAddBatchDDLJobsErr", `return(true)`))
	// Test the job runner should not hang forever.
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            1,
		TableID:             1,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{Database: "db", Table: "table"}},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	de := dom.DDLExecutor().(ddl.ExecutorForTest)
	err := de.DoDDLJobWrapper(ctx, ddl.NewJobWrapper(job, true))
	require.Error(t, err)
	require.Equal(t, err.Error(), "mockAddBatchDDLJobsErr")
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/jobsubmit/mockAddBatchDDLJobsErr"))
}

func TestParallelDDL(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, testLease)
	ctx := context.Background()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	/*
		build structure:
			DBs -> {
			 db1: test_parallel_ddl_1
			 db2: test_parallel_ddl_2
			}
			Tables -> {
			 db1.t1 (c1 int, c2 int)
			 db1.t2 (c1 int primary key, c2 int, c3 int)
			 db2.t3 (c1 int, c2 int, c3 int, c4 int)
			}
	*/
	tk.MustExec("create database test_parallel_ddl_1")
	tk.MustExec("create database test_parallel_ddl_2")
	tk.MustExec("create table test_parallel_ddl_1.t1(c1 int, c2 int, key db1_idx2(c2))")
	tk.MustExec("create table test_parallel_ddl_1.t2(c1 int primary key, c2 int, c3 int)")
	tk.MustExec("create table test_parallel_ddl_2.t3(c1 int, c2 int, c3 int, c4 int)")

	// set hook to execute jobs after all jobs are in queue.
	jobCnt := 11

	once1 := sync.Once{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs", func() {
		once1.Do(func() {
			for {
				tk1 := testkit.NewTestKit(t, store)
				tk1.MustExec("begin")
				jobs, err := ddl.GetAllDDLJobs(ctx, tk1.Session())
				require.NoError(t, err)
				tk1.MustExec("rollback")
				var qLen1, qLen2 int
				for _, job := range jobs {
					if !job.MayNeedReorg() {
						qLen1++
					} else {
						qLen2++
					}
				}
				if qLen1+qLen2 == jobCnt {
					if qLen2 != 5 {
						require.FailNow(t, "add index jobs cnt %v != 6", qLen2)
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	})

	/*
		prepare jobs:
		/	job no.	/	database no.	/	table no.	/	action type	 /
		/     1		/	 	1			/		1		/	add index	 /
		/     2		/	 	1			/		1		/	add column	 /
		/     3		/	 	1			/		1		/	add index	 /
		/     4		/	 	1			/		2		/	drop column	 /
		/     5		/	 	1			/		1		/	drop index 	 /
		/     6		/	 	1			/		2		/	add index	 /
		/     7		/	 	2			/		3		/	drop column	 /
		/     8		/	 	2			/		3		/	rebase autoID/
		/     9		/	 	1			/		1		/	add index	 /
		/     10	/	 	2			/		null   	/	drop schema  /
		/     11	/	 	2			/		2		/	add index	 /
	*/
	var wg util.WaitGroupWrapper

	seqIDs := make([]int, 11)

	var enable atomic.Bool
	ch := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/waitJobSubmitted",
		func() {
			if enable.Load() {
				<-ch
			}
		},
	)
	enable.Store(true)
	for i, sql := range []string{
		"alter table test_parallel_ddl_1.t1 add index db1_idx1(c1)",
		"alter table test_parallel_ddl_1.t1 add column c3 int",
		"alter table test_parallel_ddl_1.t1 add index db1_idxx(c1)",
		"alter table test_parallel_ddl_1.t2 drop column c3",
		"alter table test_parallel_ddl_1.t1 drop index db1_idx2",
		"alter table test_parallel_ddl_1.t2 add index db1_idx2(c2)",
		"alter table test_parallel_ddl_2.t3 drop column c4",
		"alter table test_parallel_ddl_2.t3 auto_id_cache 1024",
		"alter table test_parallel_ddl_1.t1 add index db1_idx3(c2)",
		"drop database test_parallel_ddl_2",
	} {
		idx := i
		wg.Run(func() {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec(sql)
			rs := tk2.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
			seqIDs[idx], _ = strconv.Atoi(rs.Rows()[0][0].(string))
		})
		ch <- struct{}{}
	}
	enable.Store(false)
	wg.Run(func() {
		tk := testkit.NewTestKit(t, store)
		_ = tk.ExecToErr("alter table test_parallel_ddl_2.t3 add index db3_idx1(c2)")
		rs := tk.MustQuery("select json_extract(@@tidb_last_ddl_info, '$.seq_num')")
		seqIDs[10], _ = strconv.Atoi(rs.Rows()[0][0].(string))
	})

	wg.Wait()

	// Table 1 order.
	require.Less(t, seqIDs[0], seqIDs[1])
	require.Less(t, seqIDs[1], seqIDs[2])
	require.Less(t, seqIDs[2], seqIDs[4])
	require.Less(t, seqIDs[4], seqIDs[8])

	// Table 2 order.
	require.Less(t, seqIDs[3], seqIDs[5])

	// Table 3 order.
	require.Less(t, seqIDs[6], seqIDs[7])
	require.Less(t, seqIDs[7], seqIDs[9])
}

func TestJobNeedGC(t *testing.T) {
	job := &model.Job{Type: model.ActionAddIndex, State: model.JobStateCancelled}
	require.False(t, ddl.JobNeedGC(job))

	job = &model.Job{Type: model.ActionAddColumn, State: model.JobStateDone}
	require.False(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionAddIndex, State: model.JobStateDone}
	require.True(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionAddPrimaryKey, State: model.JobStateDone}
	require.True(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionAddIndex, State: model.JobStateRollbackDone}
	require.True(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionAddPrimaryKey, State: model.JobStateRollbackDone}
	require.True(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionDropMaterializedView, State: model.JobStateDone}
	require.True(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionDropMaterializedViewLog, State: model.JobStateDone}
	require.True(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionCreateMaterializedView, State: model.JobStateDone, TableID: 123}
	require.False(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionCreateMaterializedView, State: model.JobStateRollbackDone}
	require.False(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionCreateMaterializedView, State: model.JobStateRollbackDone, TableID: 123}
	require.True(t, ddl.JobNeedGC(job))

	job = &model.Job{Type: model.ActionMultiSchemaChange, State: model.JobStateDone, MultiSchemaInfo: &model.MultiSchemaInfo{
		SubJobs: []*model.SubJob{
			{Type: model.ActionAddColumn, State: model.JobStateDone},
			{Type: model.ActionRebaseAutoID, State: model.JobStateDone},
		}}}
	require.False(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionMultiSchemaChange, State: model.JobStateDone, MultiSchemaInfo: &model.MultiSchemaInfo{
		SubJobs: []*model.SubJob{
			{Type: model.ActionAddIndex, State: model.JobStateDone},
			{Type: model.ActionAddColumn, State: model.JobStateDone},
			{Type: model.ActionRebaseAutoID, State: model.JobStateDone},
		}}}
	require.True(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionMultiSchemaChange, State: model.JobStateDone, MultiSchemaInfo: &model.MultiSchemaInfo{
		SubJobs: []*model.SubJob{
			{Type: model.ActionAddIndex, State: model.JobStateDone},
			{Type: model.ActionDropColumn, State: model.JobStateDone},
			{Type: model.ActionRebaseAutoID, State: model.JobStateDone},
		}}}
	require.True(t, ddl.JobNeedGC(job))
	job = &model.Job{Type: model.ActionMultiSchemaChange, State: model.JobStateRollbackDone, MultiSchemaInfo: &model.MultiSchemaInfo{
		SubJobs: []*model.SubJob{
			{Type: model.ActionAddIndex, State: model.JobStateRollbackDone},
			{Type: model.ActionAddColumn, State: model.JobStateRollbackDone},
			{Type: model.ActionRebaseAutoID, State: model.JobStateCancelled},
		}}}
	require.True(t, ddl.JobNeedGC(job))
}
