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
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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
	sessVars.SlowLogRules = slowlogrule.NewSessionSlowLogRules(
		&slowlogrule.SlowLogRules{
			Fields: map[string]struct{}{
				strings.ToLower(variable.SlowLogConnIDStr):  {},
				strings.ToLower(variable.SlowLogDBStr):      {},
				strings.ToLower(variable.SlowLogSucc):       {},
				strings.ToLower(execdetails.ProcessTimeStr): {},
			},
		})

	sessVars.SlowLogRules.NeedUpdateEffectiveFields = false
	items := executor.PrepareSlowLogItemsForRules(goCtx, vardef.GlobalSlowLogRules.Load(), sessVars)
	require.Nil(t, items)
	sessVars.SlowLogRules.NeedUpdateEffectiveFields = true
	items = executor.PrepareSlowLogItemsForRules(goCtx, vardef.GlobalSlowLogRules.Load(), sessVars)
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogConnIDStr)].Match(ctx.GetSessionVars(), items, uint64(123)))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogDBStr)].Match(ctx.GetSessionVars(), items, "testdb"))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogSucc)].Match(ctx.GetSessionVars(), items, true))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.ProcessTimeStr)].Match(ctx.GetSessionVars(), items, copExec.TimeDetail.ProcessTime.Seconds()))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.BackoffTimeStr)].Match(ctx.GetSessionVars(), items, copExec.BackoffTime.Seconds()))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.ProcessKeysStr)].Match(ctx.GetSessionVars(), items, copExec.ScanDetail.ProcessedKeys))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.TotalKeysStr)].Match(ctx.GetSessionVars(), items, copExec.ScanDetail.TotalKeys))

	// fields not in Fields should be zero at this point
	require.Equal(t, uint64(0), items.ExecRetryCount)
	require.Equal(t, int64(0), items.MemMax)
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

func TestShouldWriteSlowLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	baseItems := &variable.SlowQueryLogItems{
		Succ:              true,
		MemMax:            200,
		ResourceGroupName: "testRG",
	}

	t.Run("no rules return false", func(t *testing.T) {
		// default value
		tk.MustQuery(`show variables like "tidb_slow_log_rules"`).Check(
			testkit.Rows("tidb_slow_log_rules "),
		)
		tk.MustQuery(`select @@SESSION.tidb_slow_log_rules`).Check(
			testkit.Rows(""),
		)
		tk.MustQuery(`select @@Global.tidb_slow_log_rules`).Check(
			testkit.Rows(""),
		)

		tk.MustExec(`set session tidb_slow_log_rules=""`)
		tk.MustExec(`set global tidb_slow_log_rules=""`)
		seVars := tk.Session().GetSessionVars()
		require.False(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))

		// show
		tk.MustQuery(`show variables like "tidb_slow_log_rules"`).Check(
			testkit.Rows("tidb_slow_log_rules "),
		)
		tk.MustQuery(`select @@SESSION.tidb_slow_log_rules`).Check(
			testkit.Rows(""),
		)
		tk.MustQuery(`select @@Global.tidb_slow_log_rules`).Check(
			testkit.Rows(""),
		)
	})

	t.Run("session rules match", func(t *testing.T) {
		tk.MustExec(`set session tidb_slow_log_rules="Resource_group:testRG"`)
		seVars := tk.Session().GetSessionVars()
		require.True(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))

		// show
		tk.MustQuery(`show variables like "tidb_slow_log_rules"`).Check(
			testkit.Rows("tidb_slow_log_rules resource_group:testRG"),
		)
		tk.MustQuery(`select @@SESSION.tidb_slow_log_rules`).Check(
			testkit.Rows("resource_group:testRG"),
		)
		tk.MustQuery(`select @@Global.tidb_slow_log_rules`).Check(
			testkit.Rows(""),
		)
	})

	t.Run("session rules not match, global ConnID rules match", func(t *testing.T) {
		tk.MustExec(`set session tidb_slow_log_rules="Resource_group:otherRG"`)
		connID := tk.Session().GetSessionVars().ConnectionID
		tk.MustExec(`set global tidb_slow_log_rules="Conn_id:` +
			fmt.Sprintf("%d", connID) + `,Resource_group:testRG"`)
		seVars := tk.Session().GetSessionVars()
		require.True(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))

		// show
		tk.MustQuery(`show variables like "tidb_slow_log_rules"`).Check(
			testkit.Rows("tidb_slow_log_rules resource_group:otherRG"),
		)
		tk.MustQuery(`select @@SESSION.tidb_slow_log_rules`).Check(
			testkit.Rows("resource_group:otherRG"),
		)
		ret := tk.MustQuery(`select @@Global.tidb_slow_log_rules`)
		require.True(t, strings.Contains(ret.String(), "resource_group:testRG"))
		require.True(t, strings.Contains(ret.String(), fmt.Sprintf("conn_id:%d", connID)))
	})

	t.Run("session rules not match, global ConnID rules match", func(t *testing.T) {
		tk.MustExec(`set session tidb_slow_log_rules="Resource_group:otherRG"`)
		connID := tk.Session().GetSessionVars().ConnectionID
		tk.MustExec(`set global tidb_slow_log_rules="Conn_id:` +
			fmt.Sprintf("%d", connID) + `,Resource_group:testRG"`)
		seVars := tk.Session().GetSessionVars()
		require.True(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))

		// show
		tk.MustQuery(`show variables like "tidb_slow_log_rules"`).Check(
			testkit.Rows("tidb_slow_log_rules resource_group:otherRG"),
		)
		tk.MustQuery(`select @@SESSION.tidb_slow_log_rules`).Check(
			testkit.Rows("resource_group:otherRG"),
		)
		ret := tk.MustQuery(`select @@Global.tidb_slow_log_rules`)
		require.True(t, strings.Contains(ret.String(), "resource_group:testRG"))
		require.True(t, strings.Contains(ret.String(), fmt.Sprintf("conn_id:%d", connID)))
	})

	t.Run("session not match, global ConnID not match, global unsetConnID match", func(t *testing.T) {
		tk.MustExec(`set session tidb_slow_log_rules="Resource_group:otherRG"`)
		tk.MustExec(`set global tidb_slow_log_rules="Resource_group:testRG"`)
		seVars := tk.Session().GetSessionVars()
		require.True(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))

		// show
		tk.MustQuery(`show variables like "tidb_slow_log_rules"`).Check(
			testkit.Rows("tidb_slow_log_rules resource_group:otherRG"),
		)
		tk.MustQuery(`select @@SESSION.tidb_slow_log_rules`).Check(
			testkit.Rows("resource_group:otherRG"),
		)
		tk.MustQuery(`select @@Global.tidb_slow_log_rules`).Check(
			testkit.Rows("resource_group:testRG"),
		)
	})

	t.Run("all rules not match return false", func(t *testing.T) {
		tk.MustExec(`set session tidb_slow_log_rules="Resource_group:otherRG2"`)
		tk.MustExec(`set global tidb_slow_log_rules="Resource_group:notmatch"`)
		seVars := tk.Session().GetSessionVars()
		require.False(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))

		// show
		tk.MustQuery(`show variables like "tidb_slow_log_rules"`).Check(
			testkit.Rows("tidb_slow_log_rules resource_group:otherRG2"),
		)
		tk.MustQuery(`select @@SESSION.tidb_slow_log_rules`).Check(
			testkit.Rows("resource_group:otherRG2"),
		)
		tk.MustQuery(`select @@Global.tidb_slow_log_rules`).Check(
			testkit.Rows("resource_group:notmatch"),
		)
	})

	t.Run("multiple rules one matches", func(t *testing.T) {
		tk.MustExec(`set session tidb_slow_log_rules="Resource_group:otherRG"`)
		connID := tk.Session().GetSessionVars().ConnectionID
		tk.MustExec(`set global tidb_slow_log_rules="Conn_id:` +
			fmt.Sprintf("%d", connID) + `,Resource_group:testRG;Succ:false"`)
		seVars := tk.Session().GetSessionVars()
		require.True(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))

		// show
		tk.MustQuery(`show variables like "tidb_slow_log_rules"`).Check(
			testkit.Rows("tidb_slow_log_rules resource_group:otherRG"),
		)
		tk.MustQuery(`select @@SESSION.tidb_slow_log_rules`).Check(
			testkit.Rows("resource_group:otherRG"),
		)
		ret := tk.MustQuery(`select @@Global.tidb_slow_log_rules`)
		require.True(t, strings.Contains(ret.String(), "resource_group:testRG"))
		require.True(t, strings.Contains(ret.String(), "succ:false"))
		require.True(t, strings.Contains(ret.String(), fmt.Sprintf("conn_id:%d", connID)))
	})

	t.Run("multiple rules with complex conditions, one matches", func(t *testing.T) {
		seVars := tk.Session().GetSessionVars()
		tk.MustExec(`set session tidb_slow_log_rules="Succ:false, Query_Time:1.5276, Resource_group:rg1, Exec_retry_count:10, DB:db1"`)
		gConditions := fmt.Sprintf(`"Conn_ID:%d, Exec_retry_count:8, Session_alias:sessA, PD_total:5.123, Succ:false, KV_total:12.123;
			Exec_retry_count:8, Session_alias:sessA, PD_total:5.123, Succ:false, Resource_group:rg1;
			Total_keys:54321, DB:dbA, PD_total:5.123, Is_internal:false, Resource_group:rg2"`, seVars.ConnectionID)
		tk.MustExec(`set global tidb_slow_log_rules=` + gConditions)
		require.False(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))

		tikvExecDetail := util.ExecDetails{
			WaitPDRespDuration: (6 * time.Second).Nanoseconds(),
		}
		baseItems = &variable.SlowQueryLogItems{
			Succ:              false,
			ResourceGroupName: "rg1",
			ExecRetryCount:    8,
			KVExecDetail:      &tikvExecDetail,
		}
		seVars.SessionAlias = "sessA"
		require.True(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, baseItems))
	})
}

func TestWriteSlowLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`create table t(a int);`)
	tk.MustExec(`insert into t values (1), (2)`)

	core, recorded := observer.New(zap.WarnLevel)
	logger := zap.New(core)
	prev := logutil.SlowQueryLogger
	logutil.SlowQueryLogger = logger
	defer func() { logutil.SlowQueryLogger = prev }()

	sql := "select * from t where a = 1;"
	checkWriteSlowLog := func(expectWrite bool) {
		tk.MustExec(sql)
		if !expectWrite {
			require.Equal(t, 0, recorded.Len())
		} else {
			require.NotEqual(t, 0, recorded.Len())
		}

		writeMsg := slices.ContainsFunc(recorded.All(), func(entry observer.LoggedEntry) bool {
			if entry.Level == zap.WarnLevel && strings.Contains(entry.Message, sql) {
				return true
			}
			return false
		})
		require.Equal(t, expectWrite, writeMsg)
	}

	// tidb_slow_log_threshold is 300ms and tidb_slow_log_rules is empty
	checkWriteSlowLog(false)

	tk.MustExec("set tidb_slow_log_threshold=0;")
	checkWriteSlowLog(true)

	tk.MustExec("set tidb_slow_log_threshold=5000;")
	tk.MustExec(`set session tidb_slow_log_rules="Succ:true"`)
	checkWriteSlowLog(true)

	tk.MustExec(`set global tidb_slow_log_rules="Succ:true"`)
	checkWriteSlowLog(true)
}

func TestSlowLogMaxPerSec(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// default value
	tk.MustQuery(`show variables like "tidb_slow_log_max_per_sec"`).Check(
		testkit.Rows("tidb_slow_log_max_per_sec 0"),
	)
	_, err := tk.Exec(`select @@SESSION.tidb_slow_log_max_per_sec`)
	require.Equal(t, "[variable:1238]Variable 'tidb_slow_log_max_per_sec' is a GLOBAL variable", err.Error())
	tk.MustQuery(`select @@Global.tidb_slow_log_max_per_sec`).Check(
		testkit.Rows("0"),
	)

	// test errors
	_, err = tk.Exec(`set session tidb_slow_log_max_per_sec="0"`)
	require.Equal(t, "[variable:1229]Variable 'tidb_slow_log_max_per_sec' is a GLOBAL variable and should be set with SET GLOBAL", err.Error())
	_, err = tk.Exec(`set global tidb_slow_log_max_per_sec=""`)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'tidb_slow_log_max_per_sec'", err.Error())
	_, err = tk.Exec(`set global tidb_slow_log_max_per_sec="1.23"`)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'tidb_slow_log_max_per_sec'", err.Error())

	// test warnings
	_, err = tk.Exec(`set global tidb_slow_log_max_per_sec="-1"`)
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_slow_log_max_per_sec value: '-1'"))
	tk.MustQuery(`select @@Global.tidb_slow_log_max_per_sec`).Check(
		testkit.Rows("0"),
	)
	tk.MustExec(`set global tidb_slow_log_max_per_sec="1234567"`)
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_slow_log_max_per_sec value: '1234567'"))
	tk.MustQuery(`show variables like "tidb_slow_log_max_per_sec"`).Check(
		testkit.Rows("tidb_slow_log_max_per_sec 1000000"),
	)

	// normal
	tk.MustExec(`set global tidb_slow_log_max_per_sec="2"`)
	require.True(t, vardef.GlobalSlowLogRateLimiter.Allow())
	require.True(t, vardef.GlobalSlowLogRateLimiter.Allow())
	require.False(t, vardef.GlobalSlowLogRateLimiter.Allow())
	tk.MustQuery(`show variables like "tidb_slow_log_max_per_sec"`).Check(
		testkit.Rows("tidb_slow_log_max_per_sec 2"),
	)
	tk.MustQuery(`select @@Global.tidb_slow_log_max_per_sec`).Check(
		testkit.Rows("2"),
	)
	// no limit
	tk.MustExec(`set global tidb_slow_log_max_per_sec="0"`)
	require.True(t, vardef.GlobalSlowLogRateLimiter.Allow())
	require.True(t, vardef.GlobalSlowLogRateLimiter.Allow())
	require.True(t, vardef.GlobalSlowLogRateLimiter.Allow())
	tk.MustQuery(`show variables like "tidb_slow_log_max_per_sec"`).Check(
		testkit.Rows("tidb_slow_log_max_per_sec 0"),
	)
}

func BenchmarkCheckSlowThreshold(b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()

	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1,1), (2,2)")
	se := tk.Session()
	stmt, err := parser.New().ParseOneStmt("select * from t", "", "")
	require.NoError(b, err)
	compiler := executor.Compiler{Ctx: se}
	execStmt, err := compiler.Compile(context.TODO(), stmt)
	require.NoError(b, err)

	tk.MustExec("set tidb_slow_log_threshold=300000;")
	se.GetSessionVars().SlowLogRules = slowlogrule.NewSessionSlowLogRules(&slowlogrule.SlowLogRules{})
	vardef.GlobalSlowLogRules.Store(&slowlogrule.GlobalSlowLogRules{RulesMap: make(map[int64]*slowlogrule.SlowLogRules)})

	ts := oracle.GoTimeToTS(time.Now())
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		execStmt.LogSlowQuery(ts, true, false)
	}
}

func BenchmarkCheckSlowLogRulesLazy(b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()

	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1,1), (2,2)")
	se := tk.Session()
	stmt, err := parser.New().ParseOneStmt("select * from t", "", "")
	require.NoError(b, err)
	compiler := executor.Compiler{Ctx: se}
	execStmt, err := compiler.Compile(context.TODO(), stmt)
	require.NoError(b, err)
	tk.MustExec("set tidb_slow_log_threshold=300000;")

	// EffectiveFields' length is 9,
	// rules length is 4 (where 1 is a Session-level, 3 Global-level rules: 1 specifies conn, 2 global rules)
	rawRule := `Parse_time: 6.82, DB: db11, Is_internal: true, Compile_time: 0.5276, Session_alias: sessX`
	slowLogRules, err := variable.ParseSessionSlowLogRules(rawRule)
	require.NoError(b, err)
	execStmt.Ctx.GetSessionVars().SlowLogRules = slowlogrule.NewSessionSlowLogRules(slowLogRules)
	gRawRule := `Is_internal: false, Session_alias: sessA, Optimize_time: 5.123, Compile_time: 8.1, Wait_TS: 0.5276;
				Is_internal: false, Session_alias: sessB, Parse_time: 56.78, Compile_time: 8.1, DB: db1;
				Is_internal: false, Session_alias: sessC, Parse_time: 9.123, Wait_TS: 54.321, DB: db2`
	gRawRule = fmt.Sprintf("Conn_ID: %d, %s", se.GetSessionVars().ConnectionID, gRawRule)
	gSLRules, err := variable.ParseGlobalSlowLogRules(gRawRule)
	require.NoError(b, err)
	vardef.GlobalSlowLogRules.Store(gSLRules)

	ts := oracle.GoTimeToTS(time.Now())
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		execStmt.LogSlowQuery(ts, true, false)
	}
}

func BenchmarkCheckSlowLogRulesPreAlloc(b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()

	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1,1), (2,2)")
	se := tk.Session()
	stmt, err := parser.New().ParseOneStmt("select * from t", "", "")
	require.NoError(b, err)
	compiler := executor.Compiler{Ctx: se}
	execStmt, err := compiler.Compile(context.TODO(), stmt)
	require.NoError(b, err)

	// EffectiveFields' length is 10,
	// rules length is 4 (where 1 is a Session-level, 3 Global-level rules: 1 specifies conn, 2 global rules)
	rawRule := `Exec_retry_count: 10, DB: db11, Succ: false, Query_time: 0.5276, Resource_group: rg1`
	slowLogRules, err := variable.ParseSessionSlowLogRules(rawRule)
	require.NoError(b, err)
	execStmt.Ctx.GetSessionVars().SlowLogRules = slowlogrule.NewSessionSlowLogRules(slowLogRules)
	gRawRule := `Exec_retry_count: 8, Session_alias: sessA, PD_total: 8.1, Backoff_time: 0.5276, Succ: false;
				Exec_retry_count: 10, Resource_group: rg1, Succ: false, Session_alias: sessA, PD_total: 8.1;
				Total_keys: 10, DB: db11, Is_internal: false, Backoff_time: 0.5276, Resource_group: rg2`
	gRawRule = fmt.Sprintf("Conn_ID: %d, %s", se.GetSessionVars().ConnectionID, gRawRule)
	gSLRules, err := variable.ParseGlobalSlowLogRules(gRawRule)
	require.NoError(b, err)
	vardef.GlobalSlowLogRules.Store(gSLRules)

	ts := oracle.GoTimeToTS(time.Now())
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		execStmt.LogSlowQuery(ts, true, false)
	}
}

func TestMaxExecutionTimeIncludesTSOWaitTime(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	testCases := []struct {
		name             string
		tsoDelayMs       int
		maxExecutionTime uint64 // in milliseconds
		expectTimeout    bool
		description      string
	}{
		{
			name:             "TSO delay 50ms, timeout 500ms - should not timeout",
			tsoDelayMs:       50,
			maxExecutionTime: 500,
			expectTimeout:    false,
			description:      "TSO wait time (50ms) should be included, total << 500ms",
		},
		{
			name:             "TSO delay 150ms, timeout 500ms - should not timeout",
			tsoDelayMs:       150,
			maxExecutionTime: 500,
			expectTimeout:    false,
			description:      "TSO wait time (150ms) should be included, total << 500ms",
		},
		{
			name:             "TSO delay 300ms, timeout 50ms - should timeout",
			tsoDelayMs:       300,
			maxExecutionTime: 50,
			expectTimeout:    true,
			description:      "TSO wait time (300ms) exceeds timeout (50ms) clearly",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Enable failpoint to inject delay in TSO Wait()
			failpointName := "github.com/pingcap/tidb/pkg/sessiontxn/isolation/injectTSOWaitDelay"
			require.NoError(t, failpoint.Enable(failpointName, `return(`+fmt.Sprintf("%d", tc.tsoDelayMs)+`)`))
			defer func() {
				require.NoError(t, failpoint.Disable(failpointName))
			}()
			// Set max_execution_time
			tk.MustExec("set @@max_execution_time = ?", tc.maxExecutionTime)

			// Execute a SELECT statement that will trigger TSO wait
			// Use range scan instead of point get to avoid optimization
			startTime := time.Now()
			if tc.expectTimeout {
				err := tk.QueryToErr("select * from t where a >= 1")
				if err != nil {
					require.Contains(t, err.Error(), "maximum statement execution time exceeded")
				} else {
					pi := tk.Session().ShowProcess()
					require.NotNil(t, pi)
					processElapsed := time.Since(pi.Time)
					require.GreaterOrEqual(t, processElapsed, time.Duration(tc.maxExecutionTime)*time.Millisecond,
						"ProcessInfo elapsed time should exceed max_execution_time. Got %v", processElapsed)
				}
			} else {
				tk.MustQuery("select * from t where a >= 1")
			}
			elapsed := time.Since(startTime)

			// Verify that the elapsed time includes the TSO delay
			// Allow some skew for CI scheduling / overhead.
			expectedMinTime := time.Duration(tc.tsoDelayMs) * time.Millisecond
			skew := 200 * time.Millisecond
			require.GreaterOrEqual(t, elapsed, expectedMinTime-skew,
				"Elapsed time should include TSO wait time. Expected at least %v, got %v", expectedMinTime, elapsed)

			// Check ProcessInfo to verify the start time was set before TSO wait
			pi := tk.Session().ShowProcess()
			require.NotNil(t, pi)
			if pi.MaxExecutionTime > 0 {
				processElapsed := time.Since(pi.Time)
				require.GreaterOrEqual(t, processElapsed, expectedMinTime-skew,
					"ProcessInfo elapsed time should include TSO wait time. Expected at least %v, got %v", expectedMinTime, processElapsed)
			}
		})
	}
}
