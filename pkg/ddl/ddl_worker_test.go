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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

const testLease = 5 * time.Second

func TestCheckOwner(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	time.Sleep(testLease)
	require.Equal(t, dom.DDL().OwnerManager().IsOwner(), true)
	require.Equal(t, dom.DDL().GetLease(), testLease)
}

func TestInvalidDDLJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	job := &model.Job{
		SchemaID:   0,
		TableID:    0,
		Type:       model.ActionNone,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{},
	}
	ctx := testNewContext(store)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := dom.DDL().DoDDLJob(ctx, job)
	require.Equal(t, err.Error(), "[ddl:8204]invalid ddl job type: none")
}

func TestAddBatchJobError(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	ctx := testNewContext(store)

	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockAddBatchDDLJobsErr", `return(true)`))
	// Test the job runner should not hang forever.
	job := &model.Job{SchemaID: 1, TableID: 1}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := dom.DDL().DoDDLJob(ctx, job)
	require.Error(t, err)
	require.Equal(t, err.Error(), "mockAddBatchDDLJobsErr")
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockAddBatchDDLJobsErr"))
}

func TestParallelDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

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
	tc := &callback.TestDDLCallback{Do: dom}
	once := sync.Once{}
	var checkErr error
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		// TODO: extract a unified function for other tests.
		once.Do(func() {
			for {
				tk1 := testkit.NewTestKit(t, store)
				tk1.MustExec("begin")
				jobs, err := ddl.GetAllDDLJobs(tk1.Session())
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
				if checkErr != nil {
					break
				}
				if qLen1+qLen2 == jobCnt {
					if qLen2 != 5 {
						checkErr = errors.Errorf("add index jobs cnt %v != 6", qLen2)
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}

	once1 := sync.Once{}
	tc.OnGetJobBeforeExported = func(string) {
		once1.Do(func() {
			for {
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("begin")
				jobs, err := ddl.GetAllDDLJobs(tk.Session())
				require.NoError(t, err)
				tk.MustExec("rollback")
				if len(jobs) == jobCnt {
					break
				}
				time.Sleep(time.Millisecond * 20)
			}
		})
	}
	dom.DDL().SetHook(tc)

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

func TestUsingReorgCtx(t *testing.T) {
	_, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	d := domain.DDL()

	wg := util.WaitGroupWrapper{}
	wg.Run(func() {
		jobID := int64(1)
		for i := 0; i < 500; i++ {
			d.(ddl.DDLForTest).NewReorgCtx(jobID, 0)
			d.(ddl.DDLForTest).GetReorgCtx(jobID).IsReorgCanceled()
			d.(ddl.DDLForTest).RemoveReorgCtx(jobID)
		}
	})
	wg.Run(func() {
		jobID := int64(1)
		for i := 0; i < 500; i++ {
			d.(ddl.DDLForTest).NewReorgCtx(jobID, 0)
			d.(ddl.DDLForTest).GetReorgCtx(jobID).IsReorgCanceled()
			d.(ddl.DDLForTest).RemoveReorgCtx(jobID)
		}
	})
	wg.Wait()
}
