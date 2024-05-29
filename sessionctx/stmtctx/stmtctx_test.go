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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestCopTasksDetails(t *testing.T) {
	ctx := new(stmtctx.StatementContext)
	backoffs := []string{"tikvRPC", "pdRPC", "regionMiss"}
	for i := 0; i < 100; i++ {
		d := &execdetails.ExecDetails{
			CalleeAddress: fmt.Sprintf("%v", i+1),
			BackoffSleep:  make(map[string]time.Duration),
			BackoffTimes:  make(map[string]int),
			TimeDetail: util.TimeDetail{
				ProcessTime: time.Second * time.Duration(i+1),
				WaitTime:    time.Millisecond * time.Duration(i+1),
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
	testCases := []struct {
		in  *stmtctx.StatementContext
		out uint64
	}{
		{&stmtctx.StatementContext{InInsertStmt: true}, 8},
		{&stmtctx.StatementContext{InUpdateStmt: true}, 16},
		{&stmtctx.StatementContext{InDeleteStmt: true}, 16},
		{&stmtctx.StatementContext{InSelectStmt: true}, 32},
		{&stmtctx.StatementContext{IgnoreTruncate: true}, 1},
		{&stmtctx.StatementContext{TruncateAsWarning: true}, 2},
		{&stmtctx.StatementContext{OverflowAsWarning: true}, 64},
		{&stmtctx.StatementContext{IgnoreZeroInDate: true}, 128},
		{&stmtctx.StatementContext{DividedByZeroAsWarning: true}, 256},
		{&stmtctx.StatementContext{InLoadDataStmt: true}, 1024},
		{&stmtctx.StatementContext{InSelectStmt: true, TruncateAsWarning: true}, 34},
		{&stmtctx.StatementContext{DividedByZeroAsWarning: true, IgnoreTruncate: true}, 257},
		{&stmtctx.StatementContext{InUpdateStmt: true, IgnoreZeroInDate: true, InLoadDataStmt: true}, 1168},
	}
	for _, tt := range testCases {
		got := tt.in.PushDownFlags()
		require.Equal(t, tt.out, got)
	}
}

func TestWeakConsistencyRead(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
