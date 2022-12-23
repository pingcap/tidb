// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

// TestDDLScheduling tests the DDL scheduling. See Concurrent DDL RFC for the rules of DDL scheduling.
// This test checks the chosen job records to see if there are wrong scheduling, if job A and job B cannot run concurrently,
// then the all the record of job A must before or after job B, no cross record between these 2 jobs should be in between.
func TestDDLScheduling(t *testing.T) {
	if !variable.EnableConcurrentDDL.Load() {
		t.Skipf("test requires concurrent ddl")
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE e (id INT NOT NULL) PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (50), PARTITION p2 VALUES LESS THAN (100));")
	tk.MustExec("CREATE TABLE e2 (id INT NOT NULL);")
	tk.MustExec("CREATE TABLE e3 (id INT NOT NULL);")

	d := dom.DDL()

	ddlJobs := []string{
		"alter table e2 add index idx(id)",
		"alter table e2 add index idx1(id)",
		"alter table e2 add index idx2(id)",
		"create table e5 (id int)",
		"ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e2;",
		"alter table e add index idx(id)",
		"alter table e add partition (partition p3 values less than (150))",
		"create table e4 (id int)",
		"alter table e3 add index idx1(id)",
		"ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e3;",
	}

	hook := &ddl.TestDDLCallback{}
	var wg util.WaitGroupWrapper
	wg.Add(1)
	var once sync.Once
	hook.OnGetJobBeforeExported = func(jobType string) {
		once.Do(func() {
			for i, job := range ddlJobs {
				wg.Run(func() {
					tk := testkit.NewTestKit(t, store)
					tk.MustExec("use test")
					tk.MustExec("set @@tidb_enable_exchange_partition=1")
					recordSet, _ := tk.Exec(job)
					if recordSet != nil {
						require.NoError(t, recordSet.Close())
					}
				})
				for {
					time.Sleep(time.Millisecond * 100)
					jobs, err := ddl.GetAllDDLJobs(testkit.NewTestKit(t, store).Session(), nil)
					require.NoError(t, err)
					if len(jobs) == i+1 {
						break
					}
				}
			}
			wg.Done()
		})
	}

	record := make([]int64, 0, 16)
	hook.OnGetJobAfterExported = func(jobType string, job *model.Job) {
		// record the job schedule order
		record = append(record, job.ID)
	}

	err := failpoint.Enable("github.com/pingcap/tidb/ddl/mockRunJobTime", `return(true)`)
	require.NoError(t, err)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/ddl/mockRunJobTime")
		require.NoError(t, err)
	}()

	d.SetHook(hook)
	wg.Wait()

	// sort all the job id.
	ids := make(map[int64]struct{}, 16)
	for _, id := range record {
		ids[id] = struct{}{}
	}

	sortedIDs := make([]int64, 0, 16)
	for id := range ids {
		sortedIDs = append(sortedIDs, id)
	}
	slices.Sort(sortedIDs)

	// map the job id to the DDL sequence.
	// sortedIDs may looks like [30, 32, 34, 36, ...], it is the same order with the job in `ddlJobs`, 30 is the first job in `ddlJobs`, 32 is second...
	// record may looks like [30, 30, 32, 32, 34, 32, 36, 34, ...]
	// and the we map the record to the DDL sequence, [0, 0, 1, 1, 2, 1, 3, 2, ...]
	for i := range record {
		idx, b := slices.BinarySearch(sortedIDs, record[i])
		require.True(t, b)
		record[i] = int64(idx)
	}

	check(t, record, 0, 1, 2)
	check(t, record, 0, 4)
	check(t, record, 1, 4)
	check(t, record, 2, 4)
	check(t, record, 4, 5)
	check(t, record, 4, 6)
	check(t, record, 4, 9)
	check(t, record, 5, 6)
	check(t, record, 5, 9)
	check(t, record, 6, 9)
	check(t, record, 8, 9)
}

// check will check if there are any cross between ids.
// e.g. if ids is [1, 2] this function checks all `1` is before or after than `2` in record.
func check(t *testing.T, record []int64, ids ...int64) {
	// have return true if there are any `i` is before `j`, false if there are any `j` is before `i`.
	have := func(i, j int64) bool {
		for _, id := range record {
			if id == i {
				return true
			}
			if id == j {
				return false
			}
		}
		require.FailNow(t, "should not reach here", record)
		return false
	}

	// all checks if all `i` is before `j`.
	all := func(i, j int64) {
		meet := false
		for _, id := range record {
			if id == j {
				meet = true
			}
			require.False(t, meet && id == i, record)
		}
	}

	for i := 0; i < len(ids)-1; i++ {
		for j := i + 1; j < len(ids); j++ {
			if have(ids[i], ids[j]) {
				all(ids[i], ids[j])
			} else {
				all(ids[j], ids[i])
			}
		}
	}
}

func makeAddIdxBackfillJobs(schemaID, tblID, jobID, eleID int64, cnt int, query string) []*ddl.BackfillJob {
	bJobs := make([]*ddl.BackfillJob, 0, cnt)
	for i := 0; i < cnt; i++ {
		sKey := []byte(fmt.Sprintf("%d", i))
		eKey := []byte(fmt.Sprintf("%d", i+1))
		bm := &model.BackfillMeta{
			EndInclude: true,
			JobMeta: &model.JobMeta{
				SchemaID: schemaID,
				TableID:  tblID,
				Query:    query,
			},
		}
		bj := &ddl.BackfillJob{
			ID:       int64(i),
			JobID:    jobID,
			EleID:    eleID,
			EleKey:   meta.IndexElementKey,
			State:    model.JobStateNone,
			CurrKey:  sKey,
			StartKey: sKey,
			EndKey:   eKey,
			Meta:     bm,
		}
		bJobs = append(bJobs, bj)
	}
	return bJobs
}

func equalBackfillJob(t *testing.T, a, b *ddl.BackfillJob, lessTime types.Time) {
	require.Equal(t, a.ID, b.ID)
	require.Equal(t, a.JobID, b.JobID)
	require.Equal(t, a.EleID, b.EleID)
	require.Equal(t, a.EleKey, b.EleKey)
	require.Equal(t, a.StoreID, b.StoreID)
	require.Equal(t, a.InstanceID, b.InstanceID)
	require.GreaterOrEqual(t, b.InstanceLease.Compare(lessTime), 0)
	require.Equal(t, a.State, b.State)
	require.Equal(t, a.Meta, b.Meta)
}

func getIdxConditionStr(jobID, eleID int64) string {
	return fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = '%s'",
		jobID, eleID, meta.IndexElementKey)
}

func readInTxn(se sessionctx.Context, f func(sessionctx.Context)) (err error) {
	err = sessiontxn.NewTxn(context.Background(), se)
	if err != nil {
		return err
	}
	f(se)
	se.RollbackTxn(context.Background())
	return nil
}

func TestSimpleExecBackfillJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	d := dom.DDL()
	se := ddl.NewSession(tk.Session())

	jobID1 := int64(2)
	jobID2 := int64(3)
	eleID1 := int64(4)
	eleID2 := int64(5)
	uuid := d.GetID()
	instanceLease := ddl.InstanceLease
	// test no backfill job
	bJobs, err := ddl.GetBackfillJobsForOneEle(se, 1, []int64{jobID1, jobID2}, instanceLease)
	require.NoError(t, err)
	require.Nil(t, bJobs)
	bJobs, err = ddl.GetAndMarkBackfillJobsForOneEle(se, 1, jobID1, uuid, instanceLease)
	require.EqualError(t, err, dbterror.ErrDDLJobNotFound.FastGen("get zero backfill job, lease is timeout").Error())
	require.Nil(t, bJobs)
	allCnt, err := ddl.GetBackfillJobCount(se, ddl.BackfillTable, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = '%s'",
		jobID1, eleID2, meta.IndexElementKey), "check_backfill_job_count")
	require.NoError(t, err)
	require.Equal(t, allCnt, 0)
	// Test some backfill jobs, add backfill jobs to the table.
	cnt := 2
	bjTestCases := make([]*ddl.BackfillJob, 0, cnt*2)
	bJobs1 := makeAddIdxBackfillJobs(1, 2, jobID1, eleID1, cnt, "alter table add index idx(a)")
	bJobs2 := makeAddIdxBackfillJobs(1, 2, jobID2, eleID2, cnt, "alter table add index idx(b)")
	bJobs3 := makeAddIdxBackfillJobs(1, 2, jobID2, eleID1, cnt, "alter table add index idx(c)")
	bjTestCases = append(bjTestCases, bJobs1...)
	bjTestCases = append(bjTestCases, bJobs2...)
	bjTestCases = append(bjTestCases, bJobs3...)
	err = ddl.AddBackfillJobs(se, bjTestCases)
	// ID     jobID     eleID    InstanceID
	// -------------------------------------
	// 0      jobID1     eleID1    uuid
	// 1      jobID1     eleID1    ""
	// 0      jobID2     eleID2    ""
	// 1      jobID2     eleID2    ""
	// 0      jobID2     eleID1    ""
	// 1      jobID2     eleID1    ""
	require.NoError(t, err)
	// test get some backfill jobs
	bJobs, err = ddl.GetBackfillJobsForOneEle(se, 1, []int64{jobID2 - 1, jobID2 + 1}, instanceLease)
	require.NoError(t, err)
	require.Len(t, bJobs, 1)
	expectJob := bjTestCases[4]
	if expectJob.ID != bJobs[0].ID {
		expectJob = bjTestCases[5]
	}
	require.Equal(t, expectJob, bJobs[0])
	previousTime, err := ddl.GetOracleTime(se)
	require.EqualError(t, err, "[kv:8024]invalid transaction")
	readInTxn(se, func(sessionctx.Context) {
		previousTime, err = ddl.GetOracleTime(se)
		require.NoError(t, err)
	})

	bJobs, err = ddl.GetAndMarkBackfillJobsForOneEle(se, 1, jobID2, uuid, instanceLease)
	require.NoError(t, err)
	require.Len(t, bJobs, 1)
	expectJob = bjTestCases[4]
	if expectJob.ID != bJobs[0].ID {
		expectJob = bjTestCases[5]
	}
	expectJob.InstanceID = uuid
	equalBackfillJob(t, expectJob, bJobs[0], ddl.GetLeaseGoTime(previousTime, instanceLease))
	var currTime time.Time
	readInTxn(se, func(sessionctx.Context) {
		currTime, err = ddl.GetOracleTime(se)
		require.NoError(t, err)
	})
	currGoTime := ddl.GetLeaseGoTime(currTime, instanceLease)
	require.GreaterOrEqual(t, currGoTime.Compare(bJobs[0].InstanceLease), 0)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackfillTable, getIdxConditionStr(jobID2, eleID2), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, cnt)

	// remove a backfill job
	err = ddl.RemoveBackfillJob(se, false, bJobs1[0])
	// ID     jobID     eleID
	// ------------------------
	// 1      jobID1     eleID1
	// 0      jobID2     eleID2
	// 1      jobID2     eleID2
	// 0      jobID2     eleID1
	// 1      jobID2     eleID1
	require.NoError(t, err)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackfillTable, getIdxConditionStr(jobID1, eleID1), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 1)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackfillTable, getIdxConditionStr(jobID2, eleID2), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, cnt)
	// remove all backfill jobs
	err = ddl.RemoveBackfillJob(se, true, bJobs2[0])
	// ID     jobID     eleID
	// ------------------------
	// 1      jobID1     eleID1
	// 0      jobID2     eleID1
	// 1      jobID2     eleID1
	require.NoError(t, err)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackfillTable, getIdxConditionStr(jobID1, eleID1), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 1)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackfillTable, getIdxConditionStr(jobID2, eleID2), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 0)
	// clean backfill job
	err = ddl.RemoveBackfillJob(se, true, bJobs1[1])
	require.NoError(t, err)
	err = ddl.RemoveBackfillJob(se, true, bJobs3[0])
	require.NoError(t, err)
	// ID     jobID     eleID
	// ------------------------

	// test history backfill jobs
	err = ddl.AddBackfillHistoryJob(se, []*ddl.BackfillJob{bJobs2[0]})
	require.NoError(t, err)
	// ID     jobID     eleID
	// ------------------------
	// 0      jobID2     eleID2
	readInTxn(se, func(sessionctx.Context) {
		currTime, err = ddl.GetOracleTime(se)
		require.NoError(t, err)
	})
	condition := fmt.Sprintf("exec_ID = '' or exec_lease < '%v' and ddl_job_id = %d order by ddl_job_id", currTime.Add(-instanceLease), jobID1)
	bJobs, err = ddl.GetBackfillJobs(se, ddl.BackfillHistoryTable, condition, "test_get_bj")
	require.NoError(t, err)
	require.Len(t, bJobs, 1)
	require.Greater(t, bJobs[0].FinishTS, uint64(0))

	// test GetInterruptedBackfillJobsForOneEle
	bJobs, err = ddl.GetInterruptedBackfillJobsForOneEle(se, jobID1, eleID1, meta.IndexElementKey)
	require.NoError(t, err)
	require.Nil(t, bJobs)
	// ID     jobID     eleID
	// ------------------------
	// 0      jobID1     eleID1
	// 1      jobID1     eleID1
	// 0      jobID2     eleID2
	// 1      jobID2     eleID2
	err = ddl.AddBackfillJobs(se, bjTestCases)
	require.NoError(t, err)
	bJobs, err = ddl.GetInterruptedBackfillJobsForOneEle(se, jobID1, eleID1, meta.IndexElementKey)
	require.NoError(t, err)
	require.Nil(t, bJobs)
	bJobs1[0].State = model.JobStateRollingback
	bJobs1[0].ID = 2
	bJobs1[0].InstanceID = uuid
	bJobs1[1].State = model.JobStateCancelling
	bJobs1[1].ID = 3
	err = ddl.AddBackfillJobs(se, bJobs1)
	require.NoError(t, err)
	// ID     jobID     eleID     state
	// --------------------------------
	// 0      jobID1     eleID1    JobStateNone
	// 1      jobID1     eleID1    JobStateNone
	// 0      jobID2     eleID2    JobStateNone
	// 1      jobID2     eleID2    JobStateNone
	// 0      jobID2     eleID1    JobStateNone
	// 1      jobID2     eleID1    JobStateNone
	// 2      jobID1     eleID1    JobStateRollingback
	// 3      jobID1     eleID1    JobStateCancelling
	bJobs, err = ddl.GetInterruptedBackfillJobsForOneEle(se, jobID1, eleID1, meta.IndexElementKey)
	require.NoError(t, err)
	require.Len(t, bJobs, 2)
	equalBackfillJob(t, bJobs1[0], bJobs[0], types.ZeroTime)
	equalBackfillJob(t, bJobs1[1], bJobs[1], types.ZeroTime)

	// test the BackfillJob's AbbrStr
	require.Equal(t, fmt.Sprintf("ID:2, JobID:2, EleID:4, Type:add index, State:rollingback, InstanceID:%s, InstanceLease:0000-00-00 00:00:00", uuid), bJobs1[0].AbbrStr())
	require.Equal(t, "ID:3, JobID:2, EleID:4, Type:add index, State:cancelling, InstanceID:, InstanceLease:0000-00-00 00:00:00", bJobs1[1].AbbrStr())
	require.Equal(t, "ID:0, JobID:3, EleID:5, Type:add index, State:none, InstanceID:, InstanceLease:0000-00-00 00:00:00", bJobs2[0].AbbrStr())
	require.Equal(t, "ID:1, JobID:3, EleID:5, Type:add index, State:none, InstanceID:, InstanceLease:0000-00-00 00:00:00", bJobs2[1].AbbrStr())
}
