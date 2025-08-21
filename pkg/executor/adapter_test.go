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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
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

func TestSlowLogRuleFieldsConsistency(t *testing.T) {
	require.Equal(t, len(variable.SlowLogFieldValParsers), len(executor.SlowLogRuleFieldAccessors),
		"SlowLogFieldValParsers and SlowLogRuleFieldAccessors should have the same number of keys")

	// check every field exists in accessors
	for field := range variable.SlowLogFieldValParsers {
		_, ok := executor.SlowLogRuleFieldAccessors[field]
		require.Truef(t, ok, "field %s exists in SlowLogFieldValParsers but missing in SlowLogRuleFieldAccessors", field)
	}
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
			variable.SlowLogConnIDStr:  {},
			variable.SlowLogDBStr:      {},
			variable.SlowLogSucc:       {},
			execdetails.ProcessTimeStr: {},
		},
	}

	items := executor.PrepareSlowLogItemsForRules(goCtx, ctx)
	require.True(t, executor.SlowLogRuleFieldAccessors[variable.SlowLogConnIDStr].Match(ctx, items, uint64(123)))
	require.True(t, executor.SlowLogRuleFieldAccessors[variable.SlowLogDBStr].Match(ctx, items, "testdb"))
	require.True(t, executor.SlowLogRuleFieldAccessors[variable.SlowLogSucc].Match(ctx, items, true))
	require.True(t, executor.SlowLogRuleFieldAccessors[execdetails.ProcessTimeStr].Match(ctx, items, float64(copExec.TimeDetail.ProcessTime.Seconds())))
	require.True(t, executor.SlowLogRuleFieldAccessors[execdetails.BackoffTimeStr].Match(ctx, items, float64(copExec.BackoffTime.Seconds())))
	require.True(t, executor.SlowLogRuleFieldAccessors[execdetails.ProcessKeysStr].Match(ctx, items, copExec.ScanDetail.ProcessedKeys))
	require.True(t, executor.SlowLogRuleFieldAccessors[execdetails.TotalKeysStr].Match(ctx, items, copExec.ScanDetail.TotalKeys))

	// fields not in AllConditionFields should be zero at this point
	require.Equal(t, uint64(0), items.ExecRetryCount)
	// fields not in SlowLogRuleFieldAccessors should be zero at this point
	waitTimeAccessor, ok := executor.SlowLogRuleFieldAccessors[execdetails.WaitTimeStr]
	require.False(t, ok)
	require.Equal(t, executor.SlowLogFieldAccessor{}, waitTimeAccessor)

	// fill the rest
	executor.CompleteSlowLogItemsForRules(goCtx, ctx, items)
	require.Equal(t, uint64(2), items.ExecRetryCount)
	require.Equal(t, int64(1000), items.MemMax)
	require.Equal(t, int64(2000), items.DiskMax)
	require.Equal(t, sessVars.StmtCtx.ExecSuccess, items.Succ)
	require.True(t, executor.SlowLogRuleFieldAccessors[variable.SlowLogKVTotal].Match(ctx, items, float64(time.Duration(tikvExecDetail.WaitKVRespDuration).Seconds())))
	require.True(t, executor.SlowLogRuleFieldAccessors[variable.SlowLogPDTotal].Match(ctx, items, float64(time.Duration(tikvExecDetail.WaitPDRespDuration).Seconds())))
	require.True(t, executor.SlowLogRuleFieldAccessors[variable.SlowLogBackoffTotal].Match(ctx, items, float64(time.Duration(tikvExecDetail.BackoffDuration).Seconds())))
}

func newMockCtx() sessionctx.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = &stmtctx.StatementContext{}
	ctx.GetSessionVars().SlowLogRules = &variable.SlowLogRules{
		Rules:              []variable.SlowLogRule{},
		AllConditionFields: make(map[string]struct{}),
	}
	return ctx
}

func TestMatchSingleRuleSingleCondition(t *testing.T) {
	ctx := newMockCtx()
	items := &variable.SlowQueryLogItems{MemMax: 200}

	rule := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{{
			Field:     variable.SlowLogMemMax,
			Threshold: int64(100),
		}},
	}
	ctx.GetSessionVars().SlowLogRules.Rules = []variable.SlowLogRule{rule}

	require.True(t, executor.Match(ctx, items)) // 200 >= 100
	items.MemMax = 50
	require.False(t, executor.Match(ctx, items)) // 50 < 100
}

func TestMatchSingleRuleMultipleConditions(t *testing.T) {
	ctx := newMockCtx()
	items := &variable.SlowQueryLogItems{
		MemMax: 200,
		Digest: "abc",
		Succ:   true,
	}

	rule := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{
			{Field: variable.SlowLogMemMax, Threshold: int64(100)},
			{Field: variable.SlowLogDigestStr, Threshold: "abc"},
			{Field: variable.SlowLogSucc, Threshold: true},
		},
	}
	ctx.GetSessionVars().SlowLogRules.Rules = []variable.SlowLogRule{rule}

	require.True(t, executor.Match(ctx, items))

	items.Succ = false
	require.False(t, executor.Match(ctx, items)) // 其中一个不满足 → false
}

func TestMatchMultipleRulesOR(t *testing.T) {
	ctx := newMockCtx()
	sessVars := ctx.GetSessionVars()
	items := &variable.SlowQueryLogItems{}
	items.ExecRetryCount = 5
	items.Digest = "abc"
	items.Succ = true
	items.MemMax = 200

	// rule 1: requires ExecRetryCount >= 3 AND Succ == true
	rule1 := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{
			{Field: variable.SlowLogExecRetryCount, Threshold: uint64(3)},
			{Field: variable.SlowLogSucc, Threshold: true},
		},
	}
	// rule 2: requires MemMax >= 500 (not satisfied)
	rule2 := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{
			{Field: variable.SlowLogMemMax, Threshold: int64(500)},
		},
	}

	sessVars.SlowLogRules = &variable.SlowLogRules{Rules: []variable.SlowLogRule{rule1, rule2}}

	// should match rule1, return true
	require.True(t, executor.Match(ctx, items))

	// change ExecRetryCount smaller -> no match
	items.ExecRetryCount = 1
	require.False(t, executor.Match(ctx, items))

	// test string matching
	items.Digest = "plan_digest"
	rule3 := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{
			{Field: variable.SlowLogDigestStr, Threshold: "plan_digest"},
		},
	}
	sessVars.SlowLogRules = &variable.SlowLogRules{Rules: []variable.SlowLogRule{rule3}}
	require.True(t, executor.Match(ctx, items))
}

func TestMatchDifferentTypesAfterParse(t *testing.T) {
	ctx := newMockCtx()
	items := &variable.SlowQueryLogItems{
		MemMax:            123,                     // int64
		DiskMax:           456,                     // int64
		ExecRetryCount:    uint64(789),             // uint64
		ResourceGroupName: "rg1",                   // string
		Succ:              true,                    // bool
		TimeTotal:         3140 * time.Millisecond, // time.Duration
	}

	slowLogRules, err := variable.ParseSlowLogRules(`Mem_max: 100, Exec_retry_count: 300, Succ: true, Query_time: 2.52, Resource_group: rg1`)
	require.NoError(t, err)
	ctx.GetSessionVars().SlowLogRules = slowLogRules
	require.True(t, executor.Match(ctx, items))
}
