// Copyright 2019 PingCAP, Inc.
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

package stmtctx_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestCopTasksDetails(t *testing.T) {
	ctx := stmtctx.NewStmtCtx()
	backoffs := []string{"tikvRPC", "pdRPC", "regionMiss"}
	for i := 0; i < 100; i++ {
		d := &execdetails.ExecDetails{
			DetailsNeedP90: execdetails.DetailsNeedP90{
				CalleeAddress: fmt.Sprintf("%v", i+1),
				BackoffSleep:  make(map[string]time.Duration),
				BackoffTimes:  make(map[string]int),
				TimeDetail: util.TimeDetail{
					ProcessTime: time.Second * time.Duration(i+1),
					WaitTime:    time.Millisecond * time.Duration(i+1),
				},
			},
		}
		for _, backoff := range backoffs {
			d.BackoffSleep[backoff] = time.Millisecond * 100 * time.Duration(i+1)
			d.BackoffTimes[backoff] = i + 1
		}
		ctx.MergeExecDetails(d, nil)
	}
	d := ctx.CopTasksDetails()
	require.Equal(t, 100, d.NumCopTasks)
	require.Equal(t, time.Second*101/2, d.AvgProcessTime)
	require.Equal(t, time.Second*91, d.P90ProcessTime)
	require.Equal(t, time.Second*100, d.MaxProcessTime)
	require.Equal(t, "100", d.MaxProcessAddress)
	require.Equal(t, time.Millisecond*101/2, d.AvgWaitTime)
	require.Equal(t, time.Millisecond*91, d.P90WaitTime)
	require.Equal(t, time.Millisecond*100, d.MaxWaitTime)
	require.Equal(t, "100", d.MaxWaitAddress)
	fields := d.ToZapFields()
	require.Equal(t, 9, len(fields))
	for _, backoff := range backoffs {
		require.Equal(t, "100", d.MaxBackoffAddress[backoff])
		require.Equal(t, 100*time.Millisecond*100, d.MaxBackoffTime[backoff])
		require.Equal(t, time.Millisecond*100*91, d.P90BackoffTime[backoff])
		require.Equal(t, time.Millisecond*100*101/2, d.AvgBackoffTime[backoff])
		require.Equal(t, 101*50, d.TotBackoffTimes[backoff])
		require.Equal(t, 101*50*100*time.Millisecond, d.TotBackoffTime[backoff])
	}
}

func TestStatementContextPushDownFLags(t *testing.T) {
	newStmtCtx := func(fn func(*stmtctx.StatementContext)) *stmtctx.StatementContext {
		sc := stmtctx.NewStmtCtx()
		fn(sc)
		return sc
	}

	testCases := []struct {
		in  *stmtctx.StatementContext
		out uint64
	}{
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.InInsertStmt = true }), 8},
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.InUpdateStmt = true }), 16},
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.InDeleteStmt = true }), 16},
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.InSelectStmt = true }), 32},
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.SetTypeFlags(sc.TypeFlags().WithIgnoreTruncateErr(true)) }), 1},
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.SetTypeFlags(sc.TypeFlags().WithTruncateAsWarning(true)) }), 66},
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.SetTypeFlags(sc.TypeFlags().WithIgnoreZeroInDate(true)) }), 128},
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.DividedByZeroAsWarning = true }), 256},
		{newStmtCtx(func(sc *stmtctx.StatementContext) { sc.InLoadDataStmt = true }), 1024},
		{newStmtCtx(func(sc *stmtctx.StatementContext) {
			sc.InSelectStmt = true
			sc.SetTypeFlags(sc.TypeFlags().WithTruncateAsWarning(true))
		}), 98},
		{newStmtCtx(func(sc *stmtctx.StatementContext) {
			sc.DividedByZeroAsWarning = true
			sc.SetTypeFlags(sc.TypeFlags().WithIgnoreTruncateErr(true))
		}), 257},
		{newStmtCtx(func(sc *stmtctx.StatementContext) {
			sc.InUpdateStmt = true
			sc.SetTypeFlags(sc.TypeFlags().WithIgnoreZeroInDate(true))
			sc.InLoadDataStmt = true
		}), 1168},
	}
	for _, tt := range testCases {
		got := tt.in.PushDownFlags()
		require.Equal(t, tt.out, got)
	}
}

func TestWeakConsistencyRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, c int, c1 int, unique index i(c))")

	execAndCheck := func(sql string, rows [][]interface{}, isolationLevel kv.IsoLevel) {
		ctx := context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
			require.Equal(t, req.IsolationLevel, isolationLevel)
		})
		rss, err := tk.Session().Execute(ctx, sql)
		require.Nil(t, err)
		for _, rs := range rss {
			rs.Close()
		}
		if rows != nil {
			tk.MustQuery(sql).Check(rows)
		}
		lastWeakConsistency := tk.Session().GetSessionVars().StmtCtx.WeakConsistency
		require.Equal(t, lastWeakConsistency, isolationLevel == kv.RC)
	}

	// strict
	execAndCheck("insert into t values(1, 1, 1)", nil, kv.SI)
	execAndCheck("select * from t", testkit.Rows("1 1 1"), kv.SI)
	tk.MustExec("prepare s from 'select * from t'")
	tk.MustExec("prepare u from 'update t set c1 = id + 1'")
	execAndCheck("execute s", testkit.Rows("1 1 1"), kv.SI)
	execAndCheck("execute u", nil, kv.SI)
	execAndCheck("admin check table t", nil, kv.SI)
	// weak
	tk.MustExec("set tidb_read_consistency = weak")
	execAndCheck("insert into t values(2, 2, 2)", nil, kv.SI)
	execAndCheck("select * from t", testkit.Rows("1 1 2", "2 2 2"), kv.RC)
	execAndCheck("execute s", testkit.Rows("1 1 2", "2 2 2"), kv.RC)
	execAndCheck("execute u", nil, kv.SI)
	// non-read-only queries should be strict
	execAndCheck("admin check table t", nil, kv.SI)
	execAndCheck("update t set c = c + 1 where id = 2", nil, kv.SI)
	execAndCheck("delete from t where id = 2", nil, kv.SI)
	// in-transaction queries should be strict
	tk.MustExec("begin")
	execAndCheck("select * from t", testkit.Rows("1 1 2"), kv.SI)
	execAndCheck("execute s", testkit.Rows("1 1 2"), kv.SI)
	tk.MustExec("rollback")
}

func TestMarshalSQLWarn(t *testing.T) {
	warns := []stmtctx.SQLWarn{
		{
			Level: stmtctx.WarnLevelError,
			Err:   errors.New("any error"),
		},
		{
			Level: stmtctx.WarnLevelError,
			Err:   errors.Trace(errors.New("any error")),
		},
		{
			Level: stmtctx.WarnLevelWarning,
			Err:   variable.ErrUnknownSystemVar.GenWithStackByArgs("unknown"),
		},
		{
			Level: stmtctx.WarnLevelWarning,
			Err:   errors.Trace(variable.ErrUnknownSystemVar.GenWithStackByArgs("unknown")),
		},
	}

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// First query can trigger loading global variables, which produces warnings.
	tk.MustQuery("select 1")
	tk.Session().GetSessionVars().StmtCtx.SetWarnings(warns)
	rows := tk.MustQuery("show warnings").Rows()
	require.Equal(t, len(warns), len(rows))

	// The unmarshalled result doesn't need to be exactly the same with the original one.
	// We only need that the results of `show warnings` are the same.
	bytes, err := json.Marshal(warns)
	require.NoError(t, err)
	var newWarns []stmtctx.SQLWarn
	err = json.Unmarshal(bytes, &newWarns)
	require.NoError(t, err)
	tk.Session().GetSessionVars().StmtCtx.SetWarnings(newWarns)
	tk.MustQuery("show warnings").Check(rows)
}

func TestApproxRuntimeInfo(t *testing.T) {
	var n = rand.Intn(19000) + 1000
	var valRange = rand.Int31n(10000) + 1000
	backoffs := []string{"tikvRPC", "pdRPC", "regionMiss"}
	details := []*execdetails.ExecDetails{}
	for i := 0; i < n; i++ {
		d := &execdetails.ExecDetails{
			DetailsNeedP90: execdetails.DetailsNeedP90{
				CalleeAddress: fmt.Sprintf("%v", i+1),
				BackoffSleep:  make(map[string]time.Duration),
				BackoffTimes:  make(map[string]int),
				TimeDetail: util.TimeDetail{
					ProcessTime: time.Second * time.Duration(rand.Int31n(valRange)),
					WaitTime:    time.Millisecond * time.Duration(rand.Int31n(valRange)),
				},
			},
		}
		details = append(details, d)
		for _, backoff := range backoffs {
			d.BackoffSleep[backoff] = time.Millisecond * 100 * time.Duration(rand.Int31n(valRange))
			d.BackoffTimes[backoff] = rand.Intn(int(valRange))
		}
	}

	// Make CalleeAddress for each max value is deterministic.
	details[rand.Intn(n)].DetailsNeedP90.TimeDetail.ProcessTime = time.Second * time.Duration(valRange)
	details[rand.Intn(n)].DetailsNeedP90.TimeDetail.WaitTime = time.Millisecond * time.Duration(valRange)
	for _, backoff := range backoffs {
		details[rand.Intn(n)].BackoffSleep[backoff] = time.Millisecond * 100 * time.Duration(valRange)
	}

	ctx := stmtctx.NewStmtCtx()
	for i := 0; i < n; i++ {
		ctx.MergeExecDetails(details[i], nil)
	}
	d := ctx.CopTasksDetails()

	require.Equal(t, d.NumCopTasks, n)
	sort.Slice(details, func(i, j int) bool {
		return details[i].TimeDetail.ProcessTime.Nanoseconds() < details[j].TimeDetail.ProcessTime.Nanoseconds()
	})
	var timeSum time.Duration
	for _, detail := range details {
		timeSum += detail.TimeDetail.ProcessTime
	}
	require.Equal(t, d.AvgProcessTime, timeSum/time.Duration(n))
	require.InEpsilon(t, d.P90ProcessTime.Nanoseconds(), details[n*9/10].TimeDetail.ProcessTime.Nanoseconds(), 0.05)
	require.Equal(t, d.MaxProcessTime, details[n-1].TimeDetail.ProcessTime)
	require.Equal(t, d.MaxProcessAddress, details[n-1].CalleeAddress)

	sort.Slice(details, func(i, j int) bool {
		return details[i].TimeDetail.WaitTime.Nanoseconds() < details[j].TimeDetail.WaitTime.Nanoseconds()
	})
	timeSum = 0
	for _, detail := range details {
		timeSum += detail.TimeDetail.WaitTime
	}
	require.Equal(t, d.AvgWaitTime, timeSum/time.Duration(n))
	require.InEpsilon(t, d.P90WaitTime.Nanoseconds(), details[n*9/10].TimeDetail.WaitTime.Nanoseconds(), 0.05)
	require.Equal(t, d.MaxWaitTime, details[n-1].TimeDetail.WaitTime)
	require.Equal(t, d.MaxWaitAddress, details[n-1].CalleeAddress)

	fields := d.ToZapFields()
	require.Equal(t, 9, len(fields))
	for _, backoff := range backoffs {
		sort.Slice(details, func(i, j int) bool {
			return details[i].BackoffSleep[backoff].Nanoseconds() < details[j].BackoffSleep[backoff].Nanoseconds()
		})
		timeSum = 0
		var timesSum = 0
		for _, detail := range details {
			timeSum += detail.BackoffSleep[backoff]
			timesSum += detail.BackoffTimes[backoff]
		}
		require.Equal(t, d.MaxBackoffAddress[backoff], details[n-1].CalleeAddress)
		require.Equal(t, d.MaxBackoffTime[backoff], details[n-1].BackoffSleep[backoff])
		require.InEpsilon(t, d.P90BackoffTime[backoff], details[n*9/10].BackoffSleep[backoff], 0.1)
		require.Equal(t, d.AvgBackoffTime[backoff], timeSum/time.Duration(n))

		require.Equal(t, d.TotBackoffTimes[backoff], timesSum)
		require.Equal(t, d.TotBackoffTime[backoff], timeSum)
	}
}

func TestStmtHintsClone(t *testing.T) {
	hints := stmtctx.StmtHints{}
	value := reflect.ValueOf(&hints).Elem()
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.Int, reflect.Int32, reflect.Int64:
			field.SetInt(1)
		case reflect.Uint, reflect.Uint32, reflect.Uint64:
			field.SetUint(1)
		case reflect.Uint8: // byte
			field.SetUint(1)
		case reflect.Bool:
			field.SetBool(true)
		case reflect.String:
			field.SetString("test")
		default:
		}
	}
	require.Equal(t, hints, *hints.Clone())
}

func TestNewStmtCtx(t *testing.T) {
	sc := stmtctx.NewStmtCtx()
	require.Equal(t, types.DefaultStmtFlags, sc.TypeFlags())
	require.Same(t, time.UTC, sc.TimeZone())
	require.Same(t, time.UTC, sc.TimeZone())
	sc.AppendWarning(errors.New("err1"))
	warnings := sc.GetWarnings()
	require.Equal(t, 1, len(warnings))
	require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
	require.Equal(t, "err1", warnings[0].Err.Error())

	tz := time.FixedZone("UTC+1", 2*60*60)
	sc = stmtctx.NewStmtCtxWithTimeZone(tz)
	require.Equal(t, types.DefaultStmtFlags, sc.TypeFlags())
	require.Same(t, tz, sc.TimeZone())
	require.Same(t, tz, sc.TimeZone())
	sc.AppendWarning(errors.New("err2"))
	warnings = sc.GetWarnings()
	require.Equal(t, 1, len(warnings))
	require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
	require.Equal(t, "err2", warnings[0].Err.Error())
}

func TestSetStmtCtxTimeZone(t *testing.T) {
	sc := stmtctx.NewStmtCtx()
	require.Same(t, time.UTC, sc.TimeZone())
	tz := time.FixedZone("UTC+1", 2*60*60)
	sc.SetTimeZone(tz)
	require.Same(t, tz, sc.TimeZone())
}

func TestSetStmtCtxTypeFlags(t *testing.T) {
	sc := stmtctx.NewStmtCtx()
	require.Equal(t, types.DefaultStmtFlags, sc.TypeFlags())

	sc.SetTypeFlags(types.FlagAllowNegativeToUnsigned | types.FlagSkipASCIICheck)
	require.Equal(t, types.FlagAllowNegativeToUnsigned|types.FlagSkipASCIICheck, sc.TypeFlags())
	require.Equal(t, sc.TypeFlags(), sc.TypeFlags())

	sc.SetTypeFlags(types.FlagSkipASCIICheck | types.FlagSkipUTF8Check | types.FlagTruncateAsWarning)
	require.Equal(t, types.FlagSkipASCIICheck|types.FlagSkipUTF8Check|types.FlagTruncateAsWarning, sc.TypeFlags())
	require.Equal(t, sc.TypeFlags(), sc.TypeFlags())
}

func TestResetStmtCtx(t *testing.T) {
	sc := stmtctx.NewStmtCtx()
	require.Equal(t, types.DefaultStmtFlags, sc.TypeFlags())

	tz := time.FixedZone("UTC+1", 2*60*60)
	sc.SetTimeZone(tz)
	sc.SetTypeFlags(types.FlagAllowNegativeToUnsigned | types.FlagSkipASCIICheck)
	sc.AppendWarning(errors.New("err1"))
	sc.InRestrictedSQL = true
	sc.StmtType = "Insert"

	require.Same(t, tz, sc.TimeZone())
	require.Equal(t, types.FlagAllowNegativeToUnsigned|types.FlagSkipASCIICheck, sc.TypeFlags())
	require.Equal(t, 1, len(sc.GetWarnings()))

	sc.Reset()
	require.Same(t, time.UTC, sc.TimeZone())
	require.Same(t, time.UTC, sc.TimeZone())
	require.Equal(t, types.DefaultStmtFlags, sc.TypeFlags())
	require.Equal(t, types.DefaultStmtFlags, sc.TypeFlags())
	require.False(t, sc.InRestrictedSQL)
	require.Empty(t, sc.StmtType)
	require.Equal(t, 0, len(sc.GetWarnings()))
	sc.AppendWarning(errors.New("err2"))
	warnings := sc.GetWarnings()
	require.Equal(t, 1, len(warnings))
	require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
	require.Equal(t, "err2", warnings[0].Err.Error())
}

func BenchmarkErrCtx(b *testing.B) {
	sc := stmtctx.NewStmtCtx()

	for i := 0; i < b.N; i++ {
		sc.ErrCtx()
	}
}
