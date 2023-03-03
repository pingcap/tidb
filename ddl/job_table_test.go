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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
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

	hook := &callback.TestDDLCallback{}
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
			StartKey: sKey,
			EndKey:   eKey,
		}
		bj := &ddl.BackfillJob{
			ID:              int64(i),
			JobID:           jobID,
			EleID:           eleID,
			EleKey:          meta.IndexElementKey,
			State:           model.JobStateNone,
			PhysicalTableID: 1,
			InstanceLease:   types.ZeroTimestamp,
			Meta:            bm,
		}
		bj.Meta.CurrKey = sKey
		bJobs = append(bJobs, bj)
	}
	return bJobs
}

func equalBackfillJob(t *testing.T, a, b *ddl.BackfillJob, lessTime types.Time) {
	require.Equal(t, a.ID, b.ID)
	require.Equal(t, a.JobID, b.JobID)
	require.Equal(t, a.EleID, b.EleID)
	require.Equal(t, a.EleKey, b.EleKey)
	require.Equal(t, a.PhysicalTableID, b.PhysicalTableID)
	require.Equal(t, a.InstanceID, b.InstanceID)
	require.GreaterOrEqual(t, b.InstanceLease.Compare(lessTime), 0)
	require.Equal(t, a.State, b.State)
	require.Equal(t, a.Meta, b.Meta)
}

func getIdxConditionStr(jobID, eleID int64) string {
	return fmt.Sprintf("task_key like '%s'", ddl.BackfillJobPrefixKeyString(jobID, kv.Key(meta.IndexElementKey), eleID))
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

func backfillJob2PTblMetaMap(bJob *ddl.BackfillJob) map[int64]*ddl.BackfillJobRangeMeta {
	m := &ddl.BackfillJobRangeMeta{
		ID:       bJob.ID,
		PhyTblID: bJob.PhysicalTableID,
		StartKey: bJob.Meta.StartKey,
		EndKey:   bJob.Meta.EndKey,
	}
	mMap := make(map[int64]*ddl.BackfillJobRangeMeta)
	mMap[m.PhyTblID] = m
	return mMap
}

func TestSimpleExecBackfillJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	d := dom.DDL()
	se := ddl.NewSession(tk.Session())

	jobID1 := int64(1)
	jobID2 := int64(2)
	eleID1 := int64(11)
	eleID2 := int64(22)
	eleID3 := int64(33)
	noPID := int64(0)
	uuid := d.GetID()
	eleKey := meta.IndexElementKey
	instanceLease := ddl.InstanceLease

	// test no backfill job
	bJob, err := ddl.GetBackfillJobForOneEle(se, []int64{jobID1, jobID2}, instanceLease)
	require.NoError(t, err)
	require.Nil(t, bJob)
	bJobs, err := ddl.GetAndMarkBackfillJobsForOneEle(se, 1, jobID1, uuid, noPID, instanceLease)
	require.EqualError(t, err, dbterror.ErrDDLJobNotFound.FastGen("get zero backfill job").Error())
	require.Nil(t, bJobs)
	allCnt, err := ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID1, eleID2), "check_backfill_job_count")
	require.NoError(t, err)
	require.Equal(t, allCnt, 0)
	// Test some backfill jobs, add backfill jobs to the table.
	cnt := 2
	bjTestCases := make([]*ddl.BackfillJob, 0, cnt*3)
	bJobs1 := makeAddIdxBackfillJobs(1, 2, jobID1, eleID1, cnt, "alter table t add index idx(a)")
	bJobs2 := makeAddIdxBackfillJobs(1, 2, jobID2, eleID2, cnt, "alter table t add index idx(b)")
	bJobs3 := makeAddIdxBackfillJobs(1, 2, jobID2, eleID3, cnt, "alter table t add index idx(c)")
	bjTestCases = append(bjTestCases, bJobs1...)
	bjTestCases = append(bjTestCases, bJobs2...)
	bjTestCases = append(bjTestCases, bJobs3...)
	err = ddl.AddBackfillJobs(se, bjTestCases)
	require.Equal(t, err.Error(), "[table:1292]Incorrect timestamp value: '0000-00-00 00:00:00' for column 'exec_expired' at row 1")
	tk.Session().GetSessionVars().SQLMode = mysql.ModeNone
	err = ddl.AddBackfillJobs(se, bjTestCases)
	// ID     jobID     eleID    InstanceID  PhysicalTableID
	// --------------------------------------------------
	// 0      jobID1     eleID1    uuid          1
	// 1      jobID1     eleID1    ""            1
	// 0      jobID2     eleID2    ""            1
	// 1      jobID2     eleID2    ""            1
	// 0      jobID2     eleID3    ""            1
	// 1      jobID2     eleID3    ""            1
	require.NoError(t, err)
	// test get some backfill jobs
	bJob, err = ddl.GetBackfillJobForOneEle(se, []int64{jobID2 - 1, jobID2 + 1}, instanceLease)
	require.NoError(t, err)
	expectJob := bjTestCases[2]
	if expectJob.ID != bJob.ID {
		expectJob = bjTestCases[3]
	}
	require.Equal(t, expectJob, bJob)
	previousTime, err := ddl.GetOracleTimeWithStartTS(se)
	require.EqualError(t, err, "[kv:8024]invalid transaction")
	readInTxn(se, func(sessionctx.Context) {
		previousTime, err = ddl.GetOracleTimeWithStartTS(se)
		require.NoError(t, err)
	})

	bJobs, err = ddl.GetAndMarkBackfillJobsForOneEle(se, 1, jobID2, uuid, noPID, instanceLease)
	require.NoError(t, err)
	require.Len(t, bJobs, 1)
	expectJob = bjTestCases[2]
	if expectJob.ID != bJobs[0].ID {
		expectJob = bjTestCases[3]
	}
	expectJob.InstanceID = uuid
	equalBackfillJob(t, expectJob, bJobs[0], ddl.GetLeaseGoTime(previousTime, instanceLease))
	var currTime time.Time
	readInTxn(se, func(sessionctx.Context) {
		currTime, err = ddl.GetOracleTimeWithStartTS(se)
		require.NoError(t, err)
	})
	currGoTime := ddl.GetLeaseGoTime(currTime, instanceLease)
	require.GreaterOrEqual(t, currGoTime.Compare(bJobs[0].InstanceLease), 0)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID2, eleID2), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, cnt)
	// test physical table
	err = ddl.RemoveBackfillJob(se, true, bJobs1[0])
	require.NoError(t, err)
	// ID     jobID     eleID    InstanceID  PhysicalTableID
	// --------------------------------------------------
	// 0      jobID2     eleID2    ""            1
	// 1      jobID2     eleID2    ""            1
	// 0      jobID2     eleID3    ""            1
	// 1      jobID2     eleID3    ""            1
	bPhyJobs := makeAddIdxBackfillJobs(1, 2, jobID1, eleID1, 10, "alter table t add index idx(a)")
	bPhyJobs[1].InstanceID = "uuid_1"
	bPhyJobs[2].PhysicalTableID = 2
	bPhyJobs[6].PhysicalTableID = 2
	bPhyJobs[4].PhysicalTableID = 3
	bPhyJobs[5].PhysicalTableID = 3
	bPhyJobs[8].PhysicalTableID = 3
	bPhyJobs[7].PhysicalTableID = 4
	bPhyJobs[9].PhysicalTableID = 4
	// ID     jobID     eleID    InstanceID   InstanceLease   PhysicalTableID
	// -----------------------------------------------------------------------
	// 0      jobID2     eleID2    ""                               1
	// 1      jobID2     eleID2    ""                               1
	// 0      jobID2     eleID3    ""                               1
	// 1      jobID2     eleID3    ""                               1
	// 0      jobID1     eleID1    ""                               1
	// 1      jobID1     eleID1    "uuid_1"                         1
	// 2      jobID1     eleID1    ""                               2
	// 3      jobID1     eleID1    ""                               1
	// 4      jobID1     eleID1    ""                               3
	// 5      jobID1     eleID1    ""                               3
	// 6      jobID1     eleID1    ""                               2
	// 7      jobID1     eleID1    ""                               4
	// 8      jobID1     eleID1    ""                               3
	// 9      jobID1     eleID1    ""                               4
	simpleCheck := func(batch, jobCnt int, bfJobIDs []int64, pID int64) {
		err = ddl.AddBackfillJobs(se, bPhyJobs)
		require.NoError(t, err)
		bJobs, err = ddl.GetAndMarkBackfillJobsForOneEle(se, batch, jobID1, uuid, pID, instanceLease)
		require.NoError(t, err)
		require.Len(t, bJobs, jobCnt)
		isExist := false
		for _, id := range bfJobIDs {
			if id == bJobs[0].ID {
				isExist = true
			}
		}
		require.True(t, isExist, fmt.Sprintf("expected ids:%v, actual id:%d", bfJobIDs, bJobs[0].ID))
		err = ddl.RemoveBackfillJob(se, true, bJobs1[0])
		require.NoError(t, err)
	}
	type cntAndID struct {
		batch    int
		bfJobCnt int
		bfJobID  []int64
	}
	checkAndClean := func(expectRet1, expectRet2 cntAndID) {
		simpleCheck(expectRet1.batch, expectRet1.bfJobCnt, expectRet1.bfJobID, noPID)
		simpleCheck(expectRet2.batch, expectRet2.bfJobCnt, expectRet2.bfJobID, ddl.GetJobWithoutPartition)
	}
	checkAndClean(cntAndID{3, 3, []int64{0, 1, 3}},
		cntAndID{3, 3, []int64{0, 1, 3}})
	bPhyJobs[1].InstanceLease = types.NewTime(types.FromGoTime(time.Now().Add(-time.Hour).UTC()), 0, 0)
	// ID     jobID     eleID    InstanceID   InstanceLease   PhysicalTableID
	// -----------------------------------------------------------------------
	// 0      jobID2     eleID2    ""                               1
	// 1      jobID2     eleID2    ""                               1
	// 0      jobID2     eleID3    ""                               1
	// 1      jobID2     eleID3    ""                               1
	// 0      jobID1     eleID1    ""                               1
	// 1      jobID1     eleID1    "uuid_1"   currentTime-hour      1
	// 2      jobID1     eleID1    ""                               2
	// 3      jobID1     eleID1    ""                               1
	// 4      jobID1     eleID1    ""                               3
	// 5      jobID1     eleID1    ""                               3
	// 6      jobID1     eleID1    ""                               2
	// 7      jobID1     eleID1    ""                               4
	// 8      jobID1     eleID1    ""                               3
	// 9      jobID1     eleID1    ""                               4
	checkAndClean(cntAndID{3, 3, []int64{0, 1, 3}},
		cntAndID{3, 3, []int64{0, 1, 3}})
	bPhyJobs[3].InstanceLease = types.NewTime(types.FromGoTime(time.Now().UTC()), 0, 0)
	// ID     jobID     eleID    InstanceID   InstanceLease   PhysicalTableID
	// -----------------------------------------------------------------------
	// 0      jobID2     eleID2    ""                               1
	// 1      jobID2     eleID2    ""                               1
	// 0      jobID2     eleID3    ""                               1
	// 1      jobID2     eleID3    ""                               1
	// 0      jobID1     eleID1    ""                               1
	// 1      jobID1     eleID1    "uuid_1"   currentTime-hour      1
	// 2      jobID1     eleID1    ""                               2
	// 3      jobID1     eleID1    ""         currentTime           1  // should not exist
	// 4      jobID1     eleID1    ""                               3
	// 5      jobID1     eleID1    ""                               3
	// 6      jobID1     eleID1    ""                               2
	// 7      jobID1     eleID1    ""                               4
	// 8      jobID1     eleID1    ""                               3
	// 9      jobID1     eleID1    ""                               4
	checkAndClean(cntAndID{3, 2, []int64{2, 6}},
		cntAndID{3, 3, []int64{0, 1, 3}})
	bPhyJobs[6].InstanceLease = types.NewTime(types.FromGoTime(time.Now().UTC()), 0, 0)
	// ID     jobID     eleID    InstanceID   InstanceLease   PhysicalTableID
	// -----------------------------------------------------------------------
	// 0      jobID2     eleID2    ""                               1
	// 1      jobID2     eleID2    ""                               1
	// 0      jobID2     eleID3    ""                               1
	// 1      jobID2     eleID3    ""                               1
	// 0      jobID1     eleID1    ""                               1
	// 1      jobID1     eleID1    "uuid_1"   currentTime-hour      1
	// 2      jobID1     eleID1    ""                               2
	// 3      jobID1     eleID1    ""         currentTime           1  // should not exist
	// 4      jobID1     eleID1    ""                               3
	// 5      jobID1     eleID1    ""                               3
	// 6      jobID1     eleID1    ""         currentTime           2  // should not exist
	// 7      jobID1     eleID1    ""                               4
	// 8      jobID1     eleID1    ""                               3
	// 9      jobID1     eleID1    ""                               4
	checkAndClean(cntAndID{3, 2, []int64{2, 6}},
		cntAndID{10, 10, []int64{0, 1, 3}})
	bPhyJobs[6].InstanceID = "uuid_2"
	// ID     jobID     eleID    InstanceID   InstanceLease   PhysicalTableID
	// -----------------------------------------------------------------------
	// 0      jobID2     eleID2    ""                               1
	// 1      jobID2     eleID2    ""                               1
	// 0      jobID2     eleID3    ""                               1
	// 1      jobID2     eleID3    ""                               1
	// 0      jobID1     eleID1    ""                               1
	// 1      jobID1     eleID1    "uuid_1"   currentTime-hour      1
	// 2      jobID1     eleID1    ""                               2
	// 3      jobID1     eleID1    ""         currentTime           1  // should not exist
	// 4      jobID1     eleID1    ""                               3
	// 5      jobID1     eleID1    ""                               3
	// 6      jobID1     eleID1    "uuid_2"   currentTime           2  // should not exist
	// 7      jobID1     eleID1    ""                               4
	// 8      jobID1     eleID1    ""                               3
	// 9      jobID1     eleID1    ""                               4
	checkAndClean(cntAndID{3, 3, []int64{4, 5, 8}},
		cntAndID{10, 9, []int64{0, 1, 3}})
	// ID     jobID     eleID    InstanceID   InstanceLease   PhysicalTableID
	// -----------------------------------------------------------------------
	// 0      jobID2     eleID2    ""                               1
	// 1      jobID2     eleID2    ""                               1
	// 0      jobID2     eleID3    ""                               1
	// 1      jobID2     eleID3    ""                               1

	err = ddl.AddBackfillJobs(se, bJobs1)
	require.NoError(t, err)
	// ID     jobID     eleID
	// ------------------------
	// 0      jobID1     eleID1
	// 1      jobID1     eleID1
	// 0      jobID2     eleID2
	// 1      jobID2     eleID2
	// 0      jobID2     eleID3
	// 1      jobID2     eleID3
	// remove a backfill job
	err = ddl.RemoveBackfillJob(se, false, bJobs1[0])
	// ID     jobID     eleID
	// ------------------------
	// 1      jobID1     eleID1
	// 0      jobID2     eleID2
	// 1      jobID2     eleID2
	// 0      jobID2     eleID3
	// 1      jobID2     eleID3
	require.NoError(t, err)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID1, eleID1), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 1)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID2, eleID2), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, cnt)
	// remove all backfill jobs
	err = ddl.RemoveBackfillJob(se, true, bJobs2[0])
	// ID     jobID     eleID
	// ------------------------
	// 1      jobID1     eleID1
	// 0      jobID2     eleID3
	// 1      jobID2     eleID3
	require.NoError(t, err)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID1, eleID1), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 1)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID2, eleID2), "test_get_bj")
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
		currTime, err = ddl.GetOracleTimeWithStartTS(se)
		require.NoError(t, err)
	})
	condition := fmt.Sprintf("exec_id = '' or exec_expired < '%v' and task_key like \"%d_%%\"", currTime.Add(-instanceLease), jobID2)
	bJobs, err = ddl.GetBackfillJobs(se, ddl.BackgroundSubtaskHistoryTable, condition, "test_get_bj")
	require.NoError(t, err)
	require.Len(t, bJobs, 1)
	require.Equal(t, bJobs[0].StateUpdateTS, uint64(0))

	// test GetMaxBackfillJob
	pTblMeta, err := ddl.GetPhysicalTableMetas(se, bJobs3[0].JobID, bJobs3[0].EleID, eleKey)
	require.NoError(t, err)
	require.Len(t, pTblMeta, 0)
	err = ddl.AddBackfillJobs(se, bjTestCases)
	require.NoError(t, err)
	// ID     jobID     eleID
	// ------------------------
	// 0      jobID1     eleID1
	// 1      jobID1     eleID1
	// 0      jobID2     eleID2
	// 1      jobID2     eleID2
	// 0      jobID2     eleID3
	// 1      jobID2     eleID3
	pTblMeta, err = ddl.GetPhysicalTableMetas(se, jobID2, eleID2, eleKey)
	require.NoError(t, err)
	require.Equal(t, backfillJob2PTblMetaMap(bJobs2[1]), pTblMeta)
	bJobs1[0].State = model.JobStateRollingback
	bJobs1[0].ID = 2
	bJobs1[0].InstanceID = uuid
	bJobs1[1].State = model.JobStateCancelled
	bJobs1[1].ID = 3
	bJobs1[1].Meta.Error = dbterror.ErrCancelledDDLJob
	err = ddl.AddBackfillJobs(se, bJobs1)
	require.NoError(t, err)
	// ID     jobID     eleID     state
	// --------------------------------
	// 0      jobID1     eleID1    JobStateNone
	// 1      jobID1     eleID1    JobStateNone
	// 0      jobID2     eleID2    JobStateNone
	// 1      jobID2     eleID2    JobStateNone
	// 0      jobID2     eleID3    JobStateNone
	// 1      jobID2     eleID3    JobStateNone
	// 2      jobID1     eleID1    JobStateRollingback
	// 3      jobID1     eleID1    JobStateCancelled
	pTblMeta, err = ddl.GetPhysicalTableMetas(se, jobID1, eleID1, eleKey)
	require.NoError(t, err)
	require.Equal(t, backfillJob2PTblMetaMap(bJobs1[1]), pTblMeta)
	// test the BackfillJob's AbbrStr
	require.Equal(t, fmt.Sprintf("ID:2, JobID:1, EleID:11, Type:add index, State:rollingback, InstanceID:%s, InstanceLease:0000-00-00 00:00:00", uuid), bJobs1[0].AbbrStr())
	require.Equal(t, "ID:3, JobID:1, EleID:11, Type:add index, State:cancelled, InstanceID:, InstanceLease:0000-00-00 00:00:00", bJobs1[1].AbbrStr())
	require.Equal(t, "ID:0, JobID:2, EleID:33, Type:add index, State:none, InstanceID:, InstanceLease:0000-00-00 00:00:00", bJobs3[0].AbbrStr())
	require.Equal(t, "ID:1, JobID:2, EleID:33, Type:add index, State:none, InstanceID:, InstanceLease:0000-00-00 00:00:00", bJobs3[1].AbbrStr())
	// test select tidb_background_subtask
	tk.MustQuery(fmt.Sprintf("select exec_id, exec_expired from mysql.tidb_background_subtask where task_key like \"%%%d\" and  %s", bJobs1[0].ID, getIdxConditionStr(jobID1, eleID1))).
		Check(testkit.Rows(fmt.Sprintf("%s 0000-00-00 00:00:00", uuid)))
	tk.MustQuery(fmt.Sprintf("select exec_id, exec_expired from mysql.tidb_background_subtask where task_key like \"%%%d\" and  %s", bJobs1[1].ID, getIdxConditionStr(jobID1, eleID1))).
		Check(testkit.Rows(" 0000-00-00 00:00:00"))
	// test GetBackfillMetas
	bfErr := ddl.GetBackfillErr(se, ddl.BackfillJobPrefixKeyString(jobID1, kv.Key(meta.IndexElementKey), eleID1))
	require.Error(t, bfErr, dbterror.ErrCancelledDDLJob)
	bfErr = ddl.GetBackfillErr(se, ddl.BackfillJobPrefixKeyString(jobID2, kv.Key(meta.IndexElementKey), eleID2))
	require.NoError(t, bfErr)
	bJobs1[0].State = model.JobStateNone
	bJobs1[0].ID = 5
	bJobs1[1].State = model.JobStateNone
	bJobs1[1].ID = 4
	err = ddl.AddBackfillHistoryJob(se, bJobs1)
	// BackgroundSubtaskTable
	// ID     jobID     eleID     state
	// --------------------------------
	// 0      jobID1     eleID1    JobStateNone
	// 1      jobID1     eleID1    JobStateNone
	// 0      jobID2     eleID2    JobStateNone
	// 1      jobID2     eleID2    JobStateNone
	// 0      jobID2     eleID3    JobStateNone
	// 1      jobID2     eleID3    JobStateNone
	// 2      jobID1     eleID1    JobStateRollingback
	// 3      jobID1     eleID1    JobStateCancelled
	//
	// BackgroundSubtaskHistoryTable
	// ID     jobID     eleID     state
	// --------------------------------
	// 5      jobID1     eleID1    JobStateNone
	// 4      jobID1     eleID1    JobStateNone
	pTblMeta, err = ddl.GetPhysicalTableMetas(se, jobID1, eleID1, eleKey)
	require.NoError(t, err)
	require.Equal(t, backfillJob2PTblMetaMap(bJobs1[0]), pTblMeta) // ???????????
	bJobs1[0].ID = 6
	bJobs1[1].ID = 7
	err = ddl.AddBackfillJobs(se, bJobs1)
	// BackgroundSubtaskTable
	// ID     jobID     eleID     state
	// --------------------------------
	// 0      jobID1     eleID1    JobStateNone
	// 1      jobID1     eleID1    JobStateNone
	// 0      jobID2     eleID2    JobStateNone
	// 1      jobID2     eleID2    JobStateNone
	// 0      jobID2     eleID3    JobStateNone
	// 1      jobID2     eleID3    JobStateNone
	// 2      jobID1     eleID1    JobStateRollingback
	// 3      jobID1     eleID1    JobStateCancelled
	// 6      jobID1     eleID1    JobStateNone
	// 7      jobID1     eleID1    JobStateNone
	//
	// BackgroundSubtaskHistoryTable
	// ID     jobID     eleID     state
	// --------------------------------
	// 5      jobID1     eleID1    JobStateNone
	// 4      jobID1     eleID1    JobStateNone
	pTblMeta, err = ddl.GetPhysicalTableMetas(se, jobID1, eleID1, eleKey)
	require.NoError(t, err)
	require.Equal(t, backfillJob2PTblMetaMap(bJobs1[1]), pTblMeta)

	// test MoveBackfillJobsToHistoryTable and GetInterruptedBackfillJobForOneEle
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID2, eleID3), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 2)
	err = ddl.MoveBackfillJobsToHistoryTable(se, bJobs3[0].PrefixKeyString())
	require.NoError(t, err)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID2, eleID3), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 0)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskHistoryTable, getIdxConditionStr(jobID2, eleID3), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 2)
	// BackgroundSubtaskTable
	// ID     jobID     eleID     state
	// --------------------------------
	// 0      jobID1     eleID1    JobStateNone
	// 1      jobID1     eleID1    JobStateNone
	// 0      jobID2     eleID2    JobStateNone
	// 1      jobID2     eleID2    JobStateNone
	// 2      jobID1     eleID1    JobStateRollingback
	// 3      jobID1     eleID1    JobStateCancelled
	// 6      jobID1     eleID1    JobStateNone
	// 7      jobID1     eleID1    JobStateNone
	//
	// BackgroundSubtaskHistoryTable
	// ID     jobID     eleID     state
	// --------------------------------
	// 5      jobID1     eleID1    JobStateNone
	// 4      jobID1     eleID1    JobStateNone
	// 0      jobID2     eleID3    JobStateCancelled
	// 1      jobID2     eleID3    JobStateCancelled
	bJobs, err = ddl.GetInterruptedBackfillJobForOneEle(se, jobID1, eleID1, eleKey)
	require.NoError(t, err)
	require.Len(t, bJobs, 0)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID1, eleID1), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 6)
	err = ddl.MoveBackfillJobsToHistoryTable(se, bJobs1[0].PrefixKeyString())
	require.NoError(t, err)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskTable, getIdxConditionStr(jobID1, eleID1), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 0)
	allCnt, err = ddl.GetBackfillJobCount(se, ddl.BackgroundSubtaskHistoryTable, getIdxConditionStr(jobID1, eleID1), "test_get_bj")
	require.NoError(t, err)
	require.Equal(t, allCnt, 8)
	bJobs, err = ddl.GetInterruptedBackfillJobForOneEle(se, jobID2, eleID3, eleKey)
	require.NoError(t, err)
	require.Len(t, bJobs, 1)
	expectJob = bJobs3[0]
	if expectJob.ID != bJob.ID {
		expectJob = bJobs3[1]
	}
	expectJob.State = model.JobStateCancelled
	equalBackfillJob(t, bJobs3[0], bJobs[0], types.ZeroTimestamp)
	// BackgroundSubtaskTable
	// ID     jobID     eleID     state
	// --------------------------------
	// 0      jobID2     eleID2    JobStateNone
	// 1      jobID2     eleID2    JobStateNone
	//
	// BackgroundSubtaskHistoryTable
	// ID     jobID     eleID     state
	// --------------------------------
	// 5      jobID1     eleID1    JobStateNone
	// 4      jobID1     eleID1    JobStateNone
	// 0      jobID2     eleID3    JobStateCancelled
	// 1      jobID2     eleID3    JobStateCancelled
	// 0      jobID1     eleID1    JobStateCancelled
	// 1      jobID1     eleID1    JobStateCancelled
	// 2      jobID1     eleID1    JobStateCancelled
	// 3      jobID1     eleID1    JobStateCancelled
	// 6      jobID1     eleID1    JobStateCancelled
	// 7      jobID1     eleID1    JobStateCancelled
}

func TestGetTasks(t *testing.T) {
	// TODO: update the variable of `enableDistReorg`
	isDistReorg := variable.DDLEnableDistributeReorg.Load()
	variable.DDLEnableDistributeReorg.Store(false)
	defer func() { variable.DDLEnableDistributeReorg.Store(isDistReorg) }()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := ddl.NewSession(tk.Session())
	se.GetSessionVars().SQLMode = mysql.ModeNone
	d := dom.DDL()

	jobID1 := int64(1)
	eleID1 := int64(11)
	uuid := d.GetID()
	cnt := 3
	instanceLease := ddl.InstanceLease
	bJobsTestCases := makeAddIdxBackfillJobs(1, 2, jobID1, eleID1, cnt, "alter table t add index idx(a)")
	err := ddl.AddBackfillJobs(se, bJobsTestCases)
	require.NoError(t, err)

	var wg util.WaitGroupWrapper
	// Mock GetAndMarkBackfillJobsForOneEle gets a writing conflict error.
	// Step 1: se1 begins txn1.
	// Step 2: se2 begins txn2.
	// Step 3: execute txn1 and txn2, then txn1 or txn2 returns a writing conflict error.
	var err1 error
	ch := make(chan struct{}, 1)
	wg.Run(func() {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/NotifyBeginTxnCh", `return(1)`))
		ch <- struct{}{}
		var bJobs []*ddl.BackfillJob
		bJobs, err = ddl.GetAndMarkBackfillJobsForOneEle(se, 1, jobID1, uuid, 1, instanceLease)
		require.Len(t, bJobs, 1)
	})
	<-ch
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/NotifyBeginTxnCh")) }()
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		se1 := ddl.NewSession(tk1.Session())
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/NotifyBeginTxnCh", `return(2)`))
		var bJobs1 []*ddl.BackfillJob
		bJobs1, err1 = ddl.GetAndMarkBackfillJobsForOneEle(se1, 1, jobID1, uuid, 1, instanceLease)
		require.Len(t, bJobs1, 1)
	})
	wg.Wait()
	if err == nil {
		require.NotNil(t, err1)
		require.True(t, strings.Contains(err1.Error(), "[kv:9007]Write conflict"))
	} else {
		require.Nil(t, err1)
		require.True(t, strings.Contains(err.Error(), "[kv:9007]Write conflict"))
	}

	err = ddl.RemoveBackfillJob(se, true, bJobsTestCases[0])
	require.NoError(t, err)
	err = ddl.AddBackfillJobs(se, bJobsTestCases)
	require.NoError(t, err)
	// get tbl
	tk.MustExec("create table t(a int, b int)")
	var tableID int64
	rs := tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where table_name='t' and table_schema='test';")
	tableIDi, err := strconv.Atoi(rs.Rows()[0][0].(string))
	require.Nil(t, err)
	tableID = int64(tableIDi)
	tbl := testGetTable(t, dom, tableID)
	pID := int64(0)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/NotifyBeginTxnCh", `return(0)`))
	// Mock GetAndMarkBackfillJobsForOneEle gets a writing conflict error, but getTasks is successful.
	// Step 1: se1 begins txn1.
	// Step 2: se2 begins txn2.
	// Step 3: execute txn1 and txn2, then txn1 or txn2 returns a writing conflict error.
	// Step 4: se2 begin txn3.
	// Step 5: getTasks(txn3) executes successfully.
	wg.Run(func() {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/NotifyBeginTxnCh", `return(1)`))
		ch <- struct{}{}
		bJobs, err := ddl.GetTasks(ddl.GetDDLCtx(d), se, tbl, jobID1, &pID, 1)
		require.Nil(t, err)
		require.Len(t, bJobs, 1)
	})
	<-ch
	wg.Run(func() {
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("use test")
		se1 := ddl.NewSession(tk1.Session())
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/NotifyBeginTxnCh", `return(2)`))
		bJobs1, err1 := ddl.GetTasks(ddl.GetDDLCtx(d), se1, tbl, jobID1, &pID, 1)
		require.Nil(t, err1)
		require.Len(t, bJobs1, 1)
	})
	wg.Wait()
}
