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
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestGetTableDataKeyRanges(t *testing.T) {
	// case 1, empty flashbackIDs
	keyRanges := ddl.GetTableDataKeyRanges([]int64{})
	require.Len(t, keyRanges, 1)
	require.Equal(t, keyRanges[0].StartKey, tablecodec.EncodeTablePrefix(0))
	require.Equal(t, keyRanges[0].EndKey, tablecodec.EncodeTablePrefix(meta.MaxGlobalID))

	// case 2, insert a execluded table ID
	keyRanges = ddl.GetTableDataKeyRanges([]int64{3})
	require.Len(t, keyRanges, 2)
	require.Equal(t, keyRanges[0].StartKey, tablecodec.EncodeTablePrefix(0))
	require.Equal(t, keyRanges[0].EndKey, tablecodec.EncodeTablePrefix(3))
	require.Equal(t, keyRanges[1].StartKey, tablecodec.EncodeTablePrefix(4))
	require.Equal(t, keyRanges[1].EndKey, tablecodec.EncodeTablePrefix(meta.MaxGlobalID))

	// case 3, insert some execluded table ID
	keyRanges = ddl.GetTableDataKeyRanges([]int64{3, 5, 9})
	require.Len(t, keyRanges, 4)
	require.Equal(t, keyRanges[0].StartKey, tablecodec.EncodeTablePrefix(0))
	require.Equal(t, keyRanges[0].EndKey, tablecodec.EncodeTablePrefix(3))
	require.Equal(t, keyRanges[1].StartKey, tablecodec.EncodeTablePrefix(4))
	require.Equal(t, keyRanges[1].EndKey, tablecodec.EncodeTablePrefix(5))
	require.Equal(t, keyRanges[2].StartKey, tablecodec.EncodeTablePrefix(6))
	require.Equal(t, keyRanges[2].EndKey, tablecodec.EncodeTablePrefix(9))
	require.Equal(t, keyRanges[3].StartKey, tablecodec.EncodeTablePrefix(10))
	require.Equal(t, keyRanges[3].EndKey, tablecodec.EncodeTablePrefix(meta.MaxGlobalID))
}

func TestFlashbackCloseAndResetPDSchedule(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)

	injectSafeTS := oracle.GoTimeToTS(time.Now().Add(10 * time.Second))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	oldValue := map[string]any{
		"merge-schedule-limit": 1,
	}
	require.NoError(t, infosync.SetPDScheduleConfig(context.Background(), oldValue))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	hook := &callback.TestDDLCallback{Do: dom}
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

	tk.MustGetErrCode(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)), errno.ErrCancelledDDLJob)
	dom.DDL().SetHook(originHook)

	finishValue, err := infosync.GetPDScheduleConfig(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, finishValue["merge-schedule-limit"], 1)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		assert.Equal(t, model.ActionFlashbackCluster, job.Type)
		if job.SchemaState == model.StateWriteOnly {
			tk1 := testkit.NewTestKit(t, store)
			_, err := tk1.Exec("alter table test.t add column b int")
			assert.ErrorContains(t, err, "Can't add ddl job, have flashback cluster job")
		}
	}
	dom.DDL().SetHook(hook)
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))

	dom.DDL().SetHook(originHook)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS)))

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	hook := &callback.TestDDLCallback{Do: dom}
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

	dom.DDL().SetHook(originHook)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
}

func TestCancelFlashbackCluster(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originHook := dom.DDL().GetHook()
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
	hook := newCancelJobHook(t, store, dom, func(job *model.Job) bool {
		return job.SchemaState == model.StateDeleteOnly
	})
	dom.DDL().SetHook(hook)
	tk.MustExec("set global tidb_ttl_job_enable = on")
	tk.MustGetErrCode(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)), errno.ErrCancelledDDLJob)
	hook.MustCancelDone(t)

	rs, err := tk.Exec("show variables like 'tidb_ttl_job_enable'")
	assert.NoError(t, err)
	assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.On)

	// Try canceled on StateWriteReorganization, cancel failed
	hook = newCancelJobHook(t, store, dom, func(job *model.Job) bool {
		return job.SchemaState == model.StateWriteReorganization
	})
	dom.DDL().SetHook(hook)
	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts).Format(types.TimeFSPFormat)))
	hook.MustCancelFailed(t)

	rs, err = tk.Exec("show variables like 'tidb_ttl_job_enable'")
	assert.NoError(t, err)
	assert.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][1], variable.Off)

	dom.DDL().SetHook(originHook)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectSafeTS"))
}
