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

package executor_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestFormatSQL(t *testing.T) {
	val := executor.FormatSQL("aaaa")
	require.Equal(t, "aaaa", val.String())
	vardef.QueryLogMaxLen.Store(0)
	val = executor.FormatSQL("aaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaa", val.String())
	vardef.QueryLogMaxLen.Store(5)
	val = executor.FormatSQL("aaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaa(len:20)", val.String())
}

func TestContextCancelWhenReadFromCopIterator(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")

	syncCh := make(chan struct{})
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/store/copr/CtxCancelBeforeReceive",
		func(ctx context.Context) {
			if ctx.Value("TestContextCancel") == "test" {
				syncCh <- struct{}{}
				<-syncCh
			}
		},
	))
	ctx := context.WithValue(context.Background(), "TestContextCancel", "test")
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx = util.WithInternalSourceType(ctx, "scheduler")
		rs, err := tk.Session().ExecuteInternal(ctx, "select * from test.t")
		require.NoError(t, err)
		_, err2 := session.ResultSetToStringSlice(ctx, tk.Session(), rs)
		require.ErrorIs(t, err2, context.Canceled)
	}()
	<-syncCh
	cancelFunc()
	syncCh <- struct{}{}
	wg.Wait()
}

func TestPrepareAndCompleteSlowLogItemsForRules(t *testing.T) {
	ctx := mock.NewContext()
	sessVars := ctx.GetSessionVars()
	sessVars.ConnectionID = 123
	sessVars.SessionAlias = "alias1"
	sessVars.CurrentDB = "testdb"
	sessVars.DurationParse = time.Second
	sessVars.DurationCompile = 2 * time.Second
	sessVars.DurationOptimization = 3 * time.Second
	sessVars.DurationWaitTS = 4 * time.Second
	sessVars.StmtCtx.ExecRetryCount = 2
	sessVars.StmtCtx.ExecSuccess = true
	sessVars.MemTracker.Consume(1000)
	sessVars.DiskTracker.Consume(2000)

	copExec := execdetails.CopExecDetails{
		BackoffTime: time.Millisecond,
		ScanDetail: &util.ScanDetail{
			ProcessedKeys: 20001,
			TotalKeys:     10000,
		},
		TimeDetail: util.TimeDetail{
			ProcessTime: time.Second * time.Duration(2),
			WaitTime:    time.Minute,
		},
	}
	ctx.GetSessionVars().StmtCtx.MergeCopExecDetails(&copExec, 0)
	tikvExecDetail := &util.ExecDetails{
		WaitKVRespDuration: (10 * time.Second).Nanoseconds(),
		WaitPDRespDuration: (11 * time.Second).Nanoseconds(),
		BackoffDuration:    (12 * time.Second).Nanoseconds(),
	}
	goCtx := context.WithValue(ctx.GoCtx(), util.ExecDetailsKey, tikvExecDetail)

	// only require a subset of fields
	sessVars.SlowLogRules = &variable.SlowLogRules{
		AllConditionFields: map[string]struct{}{
			strings.ToLower(variable.SlowLogConnIDStr):  {},
			strings.ToLower(variable.SlowLogDBStr):      {},
			strings.ToLower(variable.SlowLogSucc):       {},
			strings.ToLower(execdetails.ProcessTimeStr): {},
		},
	}

	items := executor.PrepareSlowLogItemsForRules(goCtx, ctx.GetSessionVars())
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogConnIDStr)].Match(ctx.GetSessionVars(), items, uint64(123)))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogDBStr)].Match(ctx.GetSessionVars(), items, "testdb"))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogSucc)].Match(ctx.GetSessionVars(), items, true))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.ProcessTimeStr)].Match(ctx.GetSessionVars(), items, copExec.TimeDetail.ProcessTime.Seconds()))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.BackoffTimeStr)].Match(ctx.GetSessionVars(), items, copExec.BackoffTime.Seconds()))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.ProcessKeysStr)].Match(ctx.GetSessionVars(), items, copExec.ScanDetail.ProcessedKeys))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.TotalKeysStr)].Match(ctx.GetSessionVars(), items, copExec.ScanDetail.TotalKeys))

	// fields not in AllConditionFields should be zero at this point
	require.Equal(t, uint64(0), items.ExecRetryCount)
	// fields not in SlowLogRuleFieldAccessors should be zero at this point
	waitTimeAccessor, ok := variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.WaitTimeStr)]
	require.False(t, ok)
	require.Equal(t, variable.SlowLogFieldAccessor{}, waitTimeAccessor)

	// fill the rest
	executor.CompleteSlowLogItemsForRules(goCtx, ctx.GetSessionVars(), items)
	require.Equal(t, uint64(2), items.ExecRetryCount)
	require.Equal(t, int64(1000), items.MemMax)
	require.Equal(t, int64(2000), items.DiskMax)
	require.Equal(t, sessVars.StmtCtx.ExecSuccess, items.Succ)
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogKVTotal)].Match(ctx.GetSessionVars(), items, time.Duration(tikvExecDetail.WaitKVRespDuration).Seconds()))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogPDTotal)].Match(ctx.GetSessionVars(), items, time.Duration(tikvExecDetail.WaitPDRespDuration).Seconds()))
}
