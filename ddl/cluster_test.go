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

	kvRanges, err := ddl.GetFlashbackKeyRanges(se)
	require.NoError(t, err)
	// The results are 6 key ranges
	// 0: (stats_meta,stats_histograms,stats_buckets)
	// 1: (stats_feedback)
	// 2: (stats_top_n)
	// 3: (stats_extended)
	// 4: (stats_fm_sketch)
	// 5: (stats_history, stats_meta_history)
	// 6: (stats_table_locked)
	require.Len(t, kvRanges, 7)

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
	tk.MustExec("truncate table mysql.analyze_jobs")

	// truncate all `stats_` tables, make table ID consecutive.
	tk.MustExec("truncate table mysql.stats_meta")
	tk.MustExec("truncate table mysql.stats_histograms")
	tk.MustExec("truncate table mysql.stats_buckets")
	tk.MustExec("truncate table mysql.stats_feedback")
	tk.MustExec("truncate table mysql.stats_top_n")
	tk.MustExec("truncate table mysql.stats_extended")
	tk.MustExec("truncate table mysql.stats_fm_sketch")
	tk.MustExec("truncate table mysql.stats_history")
	tk.MustExec("truncate table mysql.stats_meta_history")
	tk.MustExec("truncate table mysql.stats_table_locked")
	kvRanges, err = ddl.GetFlashbackKeyRanges(se)
	require.NoError(t, err)
	require.Len(t, kvRanges, 2)

	tk.MustExec("truncate table test.employees")
	kvRanges, err = ddl.GetFlashbackKeyRanges(se)
	require.NoError(t, err)
	require.Len(t, kvRanges, 1)
}

func TestFlashbackCloseAndResetPDSchedule(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)

	injectSafeTS := oracle.GoTimeToTS(time.Now().Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/injectSafeTS",
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

	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	tk.MustGetErrCode(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)), errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)

	finishValue, err := infosync.GetPDScheduleConfig(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, finishValue["merge-schedule-limit"], 1)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/injectSafeTS"))
}

func TestAddDDLDuringFlashback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/injectSafeTS",
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
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/injectSafeTS"))
}

func TestGlobalVariablesOnFlashback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/injectSafeTS",
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
		}
	}
	dom.DDL().SetHook(hook)
	// first try with `tidb_gc_enable` = on and `tidb_super_read_only` = off
	tk.MustExec("set global tidb_gc_enable = on")
	tk.MustExec("set global tidb_super_read_only = off")

	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)))

	rs, err := tk.Exec("show variables like 'tidb_super_read_only'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)
	rs, err = tk.Exec("show variables like 'tidb_gc_enable'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)

	// second try with `tidb_gc_enable` = off and `tidb_super_read_only` = on
	tk.MustExec("set global tidb_gc_enable = off")
	tk.MustExec("set global tidb_super_read_only = on")

	ts, err = tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)))
	rs, err = tk.Exec("show variables like 'tidb_super_read_only'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)
	rs, err = tk.Exec("show variables like 'tidb_gc_enable'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)

	dom.DDL().SetHook(originHook)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/injectSafeTS"))
}

func TestCancelFlashbackCluster(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// Try canceled on StateDeleteOnly, cancel success
	hook := newCancelJobHook(t, store, dom, func(job *model.Job) bool {
		return job.SchemaState == model.StateDeleteOnly
	})
	dom.DDL().SetHook(hook)
	tk.MustGetErrCode(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)), errno.ErrCancelledDDLJob)
	hook.MustCancelDone(t)

	// Try canceled on StateWriteReorganization, cancel failed
	hook = newCancelJobHook(t, store, dom, func(job *model.Job) bool {
		return job.SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)))
	hook.MustCancelFailed(t)

	dom.DDL().SetHook(originHook)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/injectSafeTS"))
}
