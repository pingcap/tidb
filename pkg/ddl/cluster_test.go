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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

func TestFlashbackClusterWithMaskingPolicyID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(c varchar(20))")
	tk.MustExec("create database flashback_policy_db")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`)
	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	schemaID := func(name string) int64 {
		db, ok := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema).SchemaByName(ast.NewCIStr(name))
		require.True(t, ok)
		return db.ID
	}
	// Policy IDs come from the AUTO_INCREMENT of mysql.tidb_masking_policy,
	// so they can equal the ID of any database.
	setNextPolicyID := func(id int64) {
		tk.MustExec(fmt.Sprintf("alter table mysql.tidb_masking_policy force auto_increment = %d", id))
	}
	createPolicyWithID := func(id int64) {
		tk.MustExec("create masking policy p on t(c) as mask_full(c)")
		tk.MustQuery("select policy_id from mysql.tidb_masking_policy").Check(testkit.Rows(fmt.Sprint(id)))
	}
	// flashback runs ddl and then flashes back to the timestamp before it.
	flashback := func(ddl func()) error {
		time.Sleep(10 * time.Millisecond)
		ts, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
			fmt.Sprintf("return(%v)", oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(10*time.Second))))
		ddl()
		return tk.ExecToErr(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))
	}

	mysqlID := schemaID("mysql")
	setNextPolicyID(mysqlID)
	require.NoError(t, flashback(func() { createPolicyWithID(mysqlID) }))
	require.NoError(t, flashback(func() { tk.MustExec("alter table t modify masking policy p set expression = mask_null(c)") }))
	require.NoError(t, flashback(func() { tk.MustExec("alter table t disable masking policy p") }))
	require.NoError(t, flashback(func() { tk.MustExec("alter table t drop masking policy p") }))

	// A policy ID that equals the ID of a user database is not special either.
	userDBID := schemaID("flashback_policy_db")
	setNextPolicyID(userDBID)
	require.NoError(t, flashback(func() { createPolicyWithID(userDBID) }))

	// DDL on a system schema is still rejected, including a policy change on
	// a table that was renamed into a system schema.
	err := flashback(func() { tk.MustExec("create table mysql.flashback_policy_sys(a int)") })
	require.ErrorContains(t, err, "Detected modified system table")
	tk.MustExec("rename table t to mysql.flashback_policy_t")
	err = flashback(func() { tk.MustExec("alter table mysql.flashback_policy_t drop masking policy p") })
	require.ErrorContains(t, err, "Detected modified system table")
}
