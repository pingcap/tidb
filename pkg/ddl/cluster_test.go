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
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestFlashbackCloseAndResetPDSchedule(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	injectSafeTS := oracle.GoTimeToTS(time.Now().Add(10 * time.Second))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS))

	oldValue := map[string]any{
		"merge-schedule-limit": 1,
	}
	require.NoError(t, infosync.SetPDScheduleConfig(context.Background(), oldValue))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		assert.Equal(t, model.ActionFlashbackCluster, job.Type)
		if job.SchemaState == model.StateWriteReorganization {
			closeValue, err := infosync.GetPDScheduleConfig(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, closeValue["merge-schedule-limit"], 0)
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
		assert.Equal(t, model.ActionFlashbackCluster, job.Type)
		if job.SchemaState == model.StateWriteReorganization {
			// cancel flashback job
			job.State = model.JobStateCancelled
			job.Error = dbterror.ErrCancelledDDLJob
		}
	})

	time.Sleep(10 * time.Millisecond)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	tk.MustGetErrCode(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)), errno.ErrCancelledDDLJob)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter")

	finishValue, err := infosync.GetPDScheduleConfig(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, finishValue["merge-schedule-limit"], 1)
}

func TestAddDDLDuringFlashback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	time.Sleep(10 * time.Millisecond)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		assert.Equal(t, model.ActionFlashbackCluster, job.Type)
		if job.SchemaState == model.StateWriteOnly {
			tk1 := testkit.NewTestKit(t, store)
			_, err := tk1.Exec("alter table test.t add column b int")
			assert.ErrorContains(t, err, "Can't add ddl job, have flashback cluster job")
		}
	})
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
}

func TestGlobalVariablesOnFlashback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	time.Sleep(10 * time.Millisecond)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
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
	})
	// first try with `tidb_gc_enable` = on and `tidb_super_read_only` = off and `tidb_ttl_job_enable` = on
	tk.MustExec("set global tidb_gc_enable = on")
	tk.MustExec("set global tidb_super_read_only = off")
	tk.MustExec("set global tidb_ttl_job_enable = on")

	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

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
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))
	rs, err = tk.Exec("show variables like 'tidb_super_read_only'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)
	rs, err = tk.Exec("show variables like 'tidb_gc_enable'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)
	rs, err = tk.Exec("show variables like 'tidb_ttl_job_enable'")
	assert.NoError(t, err)
	assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)

	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
}

func TestCancelFlashbackCluster(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	time.Sleep(10 * time.Millisecond)
	ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	require.NoError(t, err)

	injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// Try canceled on StateDeleteOnly, cancel success
	hook := newCancelJobHook(t, store, func(job *model.Job) bool {
		return job.SchemaState == model.StateDeleteOnly
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	tk.MustExec("set global tidb_ttl_job_enable = on")
	tk.MustGetErrCode(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)), errno.ErrCancelledDDLJob)
	hook.MustCancelDone(t)

	rs, err := tk.Exec("show variables like 'tidb_ttl_job_enable'")
	assert.NoError(t, err)
	assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)

	// Try canceled on StateWriteReorganization, cancel failed
	hook = newCancelJobHook(t, store, func(job *model.Job) bool {
		return job.SchemaState == model.StateWriteReorganization
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobUpdated", hook.OnJobUpdated)
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))
	hook.MustCancelFailed(t)

	rs, err = tk.Exec("show variables like 'tidb_ttl_job_enable'")
	assert.NoError(t, err)
	assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
}

func TestFlashbackClusterWithMaterializedViews(t *testing.T) {
	const createLog = "create materialized view log on mv_guard.base (a)"
	const createMV = "create materialized view mv_guard.mv (a, cnt) as select a, count(*) from mv_guard.base group by a"
	const dropLog = "drop materialized view log on mv_guard.base"
	const dropMV = "drop materialized view mv_guard.mv"
	for _, tc := range []struct {
		name     string
		before   []string
		after    []string
		snapshot string
	}{
		{
			name:   "neither snapshot has materialized objects",
			before: []string{"create view mv_guard.mv as select a from mv_guard.base", "create table mv_guard.`$mlog$ordinary` (a int)"},
		},
		{
			name:     "current materialized view log without dependents",
			after:    []string{createLog},
			snapshot: "current state",
		},
		{
			name:     "current materialized views sharing a log",
			after:    []string{createLog, createMV, "create materialized view mv_guard.mv2 (a, cnt) as select a, count(*) from mv_guard.base group by a"},
			snapshot: "current state",
		},
		{
			name:     "target materialized view log dropped now",
			before:   []string{createLog},
			after:    []string{dropLog},
			snapshot: "target snapshot",
		},
		{
			name:     "target materialized view and log dropped now",
			before:   []string{createLog, createMV},
			after:    []string{dropMV, dropLog},
			snapshot: "target snapshot",
		},
		{
			name:     "target schema dropped now",
			before:   []string{createLog, createMV},
			after:    []string{"drop database mv_guard"},
			snapshot: "target snapshot",
		},
		{
			name:  "materialized objects only between snapshots",
			after: []string{createLog, createMV, dropMV, dropLog},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("create database mv_guard")
			tk.MustExec("create table mv_guard.base (a int)")
			tk.MustExec("create table test.flashback_guard (a int)")
			tk.MustExec("insert into test.flashback_guard values (1)")
			for _, sql := range tc.before {
				tk.MustExec(sql)
			}

			timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
			defer resetGC()
			tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
			// Historical DDL-job checks use the physical time of the target TSO.
			time.Sleep(10 * time.Millisecond)
			ts, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
			require.NoError(t, err)
			for _, sql := range tc.after {
				tk.MustExec(sql)
			}
			tk.MustExec("insert into test.flashback_guard values (2)")

			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`)
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
				fmt.Sprintf("return(%v)", oracle.GoTimeToTS(time.Now().Add(10*time.Second))))
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
				if job.Type == model.ActionFlashbackCluster && tc.snapshot != "" {
					assert.NotEqual(t, model.StateWriteOnly, job.SchemaState, "must reject before preparing flashback regions")
					assert.NotEqual(t, model.StateWriteReorganization, job.SchemaState, "must reject before restoring data")
				}
			})
			sql := fmt.Sprintf("flashback cluster to tso %d", ts)
			if tc.snapshot == "" {
				tk.MustExec(sql)
				return
			}
			err = tk.ExecToErr(sql)
			require.ErrorContains(t, err, "materialized view")
			require.ErrorContains(t, err, "mv_guard.")
			require.ErrorContains(t, err, tc.snapshot)
			if tc.snapshot == "target snapshot" {
				require.ErrorContains(t, err, fmt.Sprintf("TSO %d", ts))
			}
			tk.MustQuery("select * from test.flashback_guard order by a").Check(testkit.Rows("1", "2"))
			tk.MustExec("insert into test.flashback_guard values (3)")
		})
	}
}
