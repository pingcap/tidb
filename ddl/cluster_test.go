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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestGetFlashbackKeyRanges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	startTS, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	kvRanges, err := ddl.GetFlashbackKeyRanges(se, startTS)
	require.NoError(t, err)
	// The results are 12 key ranges
	// 0: `stats_meta` table
	// 1: `stats_histograms` table
	// 2: `stats_buckets` table
	// 3: `gc_delete_range` table
	// 4: `stats_feedback` table
	// 5: `stats_top_n` table
	// 6: `stats_extended` table
	// 7: `stats_fm_sketch` table
	// 8: `stats_history` table
	// 9: `stats_meta_history` table
	// 10: `stats_table_locked` table
	// 11: `test` schema
	require.Len(t, kvRanges, 12)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE employees (" +
		"    id INT NOT NULL," +
		"    store_id INT NOT NULL" +
		") PARTITION BY RANGE (store_id) (" +
		"    PARTITION p0 VALUES LESS THAN (6)," +
		"    PARTITION p1 VALUES LESS THAN (11)," +
		"    PARTITION p2 VALUES LESS THAN (16)," +
		"    PARTITION p3 VALUES LESS THAN (21)" +
		");")

	kvRanges, err = ddl.GetFlashbackKeyRanges(se, startTS)
	require.NoError(t, err)
	// 12 previous key ranges and 1 table with 4 partitions
	require.Len(t, kvRanges, 12+1+4)

	// truncate `employees` table, add more 5 key ranges.
	ts, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	time.Sleep(10 * time.Millisecond)
	tk.MustExec("truncate table test.employees")
	kvRanges, err = ddl.GetFlashbackKeyRanges(se, ts)
	require.NoError(t, err)
	require.Len(t, kvRanges, 12+1+4+5)

	kvRanges, err = ddl.GetFlashbackKeyRanges(se, startTS)
	require.NoError(t, err)
	require.Len(t, kvRanges, 12+1+4)

	/*
		ts, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)
		// ts is after truncate table operate, so only 4 ranges.
		kvRanges, err = ddl.GetFlashbackKeyRanges(context.Background(), se, ts)
		require.NoError(t, err)
		require.Len(t, kvRanges, 4)

		tk.MustExec("truncate table test.employees")
		ts, err = store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		// ts is after truncate table opreate, so only 3 ranges.
		kvRanges, err = ddl.GetFlashbackKeyRanges(context.Background(), se, ts)
		require.NoError(t, err)
		require.Len(t, kvRanges, 3)
		// startTS is before truncate table opreate, so need process dropped tables key ranges.
		// 3 current exists table key ranges, 11 truncate system table ranges, 4 partitions.
		kvRanges, err = ddl.GetFlashbackKeyRanges(context.Background(), se, startTS)
		require.NoError(t, err)
		require.Len(t, kvRanges, 3+11+4)
	*/
}

func TestFlashbackCloseAndResetPDSchedule(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)

	injectSafeTS := oracle.GoTimeToTS(time.Now().Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("tikvclient/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	oldValue := map[string]interface{}{
		"merge-schedule-limit": 1,
	}
	require.NoError(t, infosync.SetPDScheduleConfig(context.Background(), oldValue))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionFlashbackCluster, job.Type)
		if job.SchemaState == model.StateWriteReorganization {
			closeValue, err := infosync.GetPDScheduleConfig(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, closeValue["merge-schedule-limit"], 0)
			// cancel flashback job
			job.State = model.JobStateCancelled
			job.Error = dbterror.ErrCancelledDDLJob
		}
	}
	dom.DDL().SetHook(hook)

	time.Sleep(10 * time.Millisecond)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	tk.MustGetErrCode(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)), errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)

	finishValue, err := infosync.GetPDScheduleConfig(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, finishValue["merge-schedule-limit"], 1)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS"))
	require.NoError(t, failpoint.Disable("tikvclient/injectSafeTS"))
}

func TestAddDDLDuringFlashback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	time.Sleep(10 * time.Millisecond)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("tikvclient/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionFlashbackCluster, job.Type)
		if job.SchemaState == model.StateWriteOnly {
			_, err := tk.Exec("alter table t add column b int")
			assert.ErrorContains(t, err, "Can't add ddl job, have flashback cluster job")
		}
	}
	dom.DDL().SetHook(hook)
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)))

	dom.DDL().SetHook(originHook)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS"))
	require.NoError(t, failpoint.Disable("tikvclient/injectSafeTS"))
}

func TestGlobalVariablesOnFlashback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	time.Sleep(10 * time.Millisecond)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("tikvclient/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionFlashbackCluster, job.Type)
		if job.SchemaState == model.StateWriteReorganization {
			rs, err := tk.Exec("show variables like 'tidb_gc_enable'")
			assert.NoError(t, err)
			assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)
			rs, err = tk.Exec("show variables like 'tidb_enable_auto_analyze'")
			assert.NoError(t, err)
			assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)
			rs, err = tk.Exec("show variables like 'tidb_super_read_only'")
			assert.NoError(t, err)
			assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)
			rs, err = tk.Exec("show variables like 'tidb_ttl_job_enable'")
			assert.NoError(t, err)
			assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)
		}
	}
	dom.DDL().SetHook(hook)
	// first try with `tidb_gc_enable` = on and `tidb_super_read_only` = off and `tidb_ttl_job_enable` = on
	tk.MustExec("set global tidb_gc_enable = on")
	tk.MustExec("set global tidb_super_read_only = off")
	tk.MustExec("set global tidb_ttl_job_enable = on")

	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)))

	rs, err := tk.Exec("show variables like 'tidb_super_read_only'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)
	rs, err = tk.Exec("show variables like 'tidb_gc_enable'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)
	rs, err = tk.Exec("show variables like 'tidb_ttl_job_enable'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)

	// second try with `tidb_gc_enable` = off and `tidb_super_read_only` = on and `tidb_ttl_job_enable` = off
	tk.MustExec("set global tidb_gc_enable = off")
	tk.MustExec("set global tidb_super_read_only = on")
	tk.MustExec("set global tidb_ttl_job_enable = off")

	ts, err = tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)))
	rs, err = tk.Exec("show variables like 'tidb_super_read_only'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)
	rs, err = tk.Exec("show variables like 'tidb_gc_enable'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)
	rs, err = tk.Exec("show variables like 'tidb_ttl_job_enable'")
	assert.NoError(t, err)
	assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)

	dom.DDL().SetHook(originHook)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS"))
	require.NoError(t, failpoint.Disable("tikvclient/injectSafeTS"))
}

func TestCancelFlashbackCluster(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)

	time.Sleep(10 * time.Millisecond)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("tikvclient/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// Try canceled on StateDeleteOnly, cancel success
	hook := newCancelJobHook(t, store, dom, func(job *model.Job) bool {
		return job.SchemaState == model.StateDeleteOnly
	})
	dom.DDL().SetHook(hook)
	tk.MustExec("set global tidb_ttl_job_enable = on")
	tk.MustGetErrCode(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)), errno.ErrCancelledDDLJob)
	hook.MustCancelDone(t)

	rs, err := tk.Exec("show variables like 'tidb_ttl_job_enable'")
	assert.NoError(t, err)
	assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)

	// Try canceled on StateWriteReorganization, cancel failed
	hook = newCancelJobHook(t, store, dom, func(job *model.Job) bool {
		return job.SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)))
	hook.MustCancelFailed(t)

	rs, err = tk.Exec("show variables like 'tidb_ttl_job_enable'")
	assert.NoError(t, err)
	assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)

	dom.DDL().SetHook(originHook)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS"))
	require.NoError(t, failpoint.Disable("tikvclient/injectSafeTS"))
}
