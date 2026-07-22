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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/config"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type mockRUV2ConsumptionReporter struct {
	group     string
	tikvRUV2  float64
	tidbRUV2  float64
	tiflashRU float64
}

func requireReadBillingDemoGeneralLogIdentity(t *testing.T, entry observer.LoggedEntry, sql string) {
	t.Helper()
	normalizedSQL, digest := parser.NormalizeDigest(sql)
	fields := entry.ContextMap()
	require.Equal(t, normalizedSQL, fields["normalized_sql"])
	require.Equal(t, digest.String(), fields["sql_digest"])
	require.NotContains(t, fields, "sql")
}

func (*mockRUV2ConsumptionReporter) ReportConsumption(_ string, _ *rmpb.Consumption) {}

func (m *mockRUV2ConsumptionReporter) ReportRUV2Consumption(resourceGroupName string, tikvRUV2, tidbRUV2, tiflashRUV2 float64) {
	m.group = resourceGroupName
	m.tikvRUV2 = tikvRUV2
	m.tidbRUV2 = tidbRUV2
	m.tiflashRU = tiflashRUV2
}

type mockRUV2ReportingContext struct {
	*mock.Context
	reporter resourcegroup.ConsumptionReporter
}

func (c *mockRUV2ReportingContext) GetDistSQLCtx() *distsqlctx.DistSQLContext {
	dctx := c.Context.GetDistSQLCtx()
	dctx.RUConsumptionReporter = c.reporter
	dctx.ResourceGroupName = c.GetSessionVars().StmtCtx.ResourceGroupName
	return dctx
}

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
	sessVars.DurationOptimizer.Total = 3 * time.Second
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
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.ProcessKeysStr)].Match(ctx.GetSessionVars(), items, uint64(copExec.ScanDetail.ProcessedKeys)))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.TotalKeysStr)].Match(ctx.GetSessionVars(), items, uint64(copExec.ScanDetail.TotalKeys)))
	require.True(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogCopMVCCReadAmplification)].Match(ctx.GetSessionVars(), items, 0.49))
	require.False(t, variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogCopMVCCReadAmplification)].Match(ctx.GetSessionVars(), items, 0.5))

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

	t.Run("session rules match by cop_mvcc_read_amplification", func(t *testing.T) {
		tk.MustExec(`set global tidb_slow_log_rules=""`)
		tk.MustExec(`set session tidb_slow_log_rules="cop_mvcc_read_amplification:10"`)
		seVars := tk.Session().GetSessionVars()
		items := &variable.SlowQueryLogItems{
			ExecDetail: &execdetails.ExecDetails{
				CopExecDetails: execdetails.CopExecDetails{
					ScanDetail: &util.ScanDetail{
						TotalKeys:     100,
						ProcessedKeys: 10,
					},
				},
			},
		}
		require.True(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, items))

		tk.MustExec(`set session tidb_slow_log_rules="cop_mvcc_read_amplification:10.01"`)
		require.False(t, executor.ShouldWriteSlowLog(vardef.GlobalSlowLogRules.Load(), seVars, items))
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
	readSlowQueryCounter := func() float64 {
		counter := metrics.SlowQueryCounter.WithLabelValues(metrics.LblGeneral)
		pb := &dto.Metric{}
		require.NoError(t, counter.Write(pb))
		return pb.GetCounter().GetValue()
	}
	checkWriteSlowLog := func(expectWrite bool) {
		before := readSlowQueryCounter()
		tk.MustExec(sql)
		after := readSlowQueryCounter()
		if !expectWrite {
			require.Equal(t, 0, recorded.Len())
			require.Equal(t, 0.0, after-before)
		} else {
			require.NotEqual(t, 0, recorded.Len())
			require.Equal(t, 1.0, after-before)
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

func TestFinishExecuteStmtSyncsTiDBRUV2FromRUDetails(t *testing.T) {
	original := config.GetGlobalConfig()
	originalGenerateBinaryPlan := variable.GenerateBinaryPlan.Load()
	t.Cleanup(func() {
		if original != nil {
			config.StoreGlobalConfig(original)
		}
		variable.GenerateBinaryPlan.Store(originalGenerateBinaryPlan)
	})
	variable.GenerateBinaryPlan.Store(false)

	cfg := config.NewConfig()
	cfg.RUV2 = config.DefaultRUV2Config()
	cfg.Instance.EnableSlowLog.Store(false)
	cfg.Instance.RecordPlanInSlowLog = 0
	config.StoreGlobalConfig(cfg)

	reporter := &mockRUV2ConsumptionReporter{}
	ctx := &mockRUV2ReportingContext{
		Context:  mock.NewContext(),
		reporter: reporter,
	}
	sessVars := ctx.GetSessionVars()
	sessVars.StartTime = time.Now()
	sessVars.StmtCtx.StmtType = "Select"
	sessVars.StmtCtx.OriginalSQL = "select 1"
	sessVars.StmtCtx.ResetSQLDigest(sessVars.StmtCtx.OriginalSQL)
	sessVars.StmtCtx.ResourceGroupName = "rg1"

	goCtx := execdetails.ContextWithInitializedExecDetails(context.Background())
	sessVars.RUV2Metrics = execdetails.RUV2MetricsFromContext(goCtx)
	require.NotNil(t, sessVars.RUV2Metrics)
	sessVars.RUV2Metrics.AddResultChunkCells(100)
	sessVars.RUV2Metrics.AddPlanCnt(2)
	sessVars.RUV2Metrics.AddSessionParserTotal(3)
	ruDetails := goCtx.Value(util.RUDetailsCtxKey).(*util.RUDetails)
	ruDetails.AddTiKVRUV2(23456)
	rawRUV2 := &kvrpcpb.RUV2{
		ReadRpcCount:                 5,
		WriteRpcCount:                7,
		StorageProcessedKeysBatchGet: 11,
	}
	ruDetails.AddRUV2(rawRUV2)
	ruDetails.UpdateTiFlash(&rmpb.Consumption{RRU: 345, WRU: 67})
	commitDetails := &util.CommitDetails{
		WriteKeys: 3,
		WriteSize: 66,
	}
	sessVars.StmtCtx.SyncExecDetails.MergeExecDetails(commitDetails)
	// Build expected metrics by cloning the current state and manually adding
	// the pending counters (without draining ruDetails, since FinishExecuteStmt will drain).
	expected := sessVars.RUV2Metrics.Clone()
	execdetails.UpdateRUV2MetricsFromRUV2(expected, rawRUV2)
	execdetails.UpdateRUV2MetricsFromCommitDetails(expected, commitDetails)

	execStmt := &executor.ExecStmt{
		Ctx:      ctx,
		GoCtx:    goCtx,
		StmtNode: &ast.SelectStmt{},
	}
	execStmt.FinishExecuteStmt(0, nil, false)

	require.Equal(t, float64(23456), ruDetails.TiKVRUV2())
	require.Equal(t, int64(5), sessVars.RUV2Metrics.ResourceManagerReadCnt())
	require.Equal(t, int64(7), sessVars.RUV2Metrics.ResourceManagerWriteCnt())
	require.Equal(t, int64(11), sessVars.RUV2Metrics.TiKVStorageProcessedKeysBatchGet())
	require.Equal(t, int64(3), sessVars.RUV2Metrics.WriteKeys())
	require.Equal(t, int64(66), sessVars.RUV2Metrics.WriteSize())
	require.Equal(t, "rg1", reporter.group)
	require.Equal(t, float64(23456), reporter.tikvRUV2)
	require.Equal(t, expected.CalculateRUValues(sessVars.RUV2Weights()), reporter.tidbRUV2)
	require.Equal(t, float64(412), reporter.tiflashRU)

	t.Run("stmt summary ignores optimistic autocommit retry count", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		// Toggle stmt summary off and back on to clear any in-memory rows left by earlier tests.
		tk.MustExec("set global tidb_enable_stmt_summary = 0")
		tk.MustExec("set global tidb_enable_stmt_summary = 1")

		tk = testkit.NewTestKit(t, store)
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		tk.MustExec("use test")
		tk.MustExec("set @@session.tidb_txn_mode = 'optimistic'")
		tk.MustExec("create table stmt_summary_retry (id int primary key, v int)")
		tk.MustExec("insert into stmt_summary_retry values (1, 1)")

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockCommitError8942", `1*return(true)->return(false)`))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockCommitError8942"))
		}()

		updateSQL := "update stmt_summary_retry set v = v + 1 where id = 1"
		tk.MustExec(updateSQL)
		tk.MustQuery(
			"select sum_exec_retry, sum_exec_retry_time from information_schema.statements_summary where digest_text like ?",
			"update `stmt_summary_retry`%",
		).Check(testkit.Rows("0 0"))
	})

	t.Run("bypass ru skips final reporting", func(t *testing.T) {
		reporter := &mockRUV2ConsumptionReporter{}
		ctx := &mockRUV2ReportingContext{
			Context:  mock.NewContext(),
			reporter: reporter,
		}
		sessVars := ctx.GetSessionVars()
		sessVars.StartTime = time.Now()
		sessVars.StmtCtx.StmtType = "Select"
		sessVars.StmtCtx.OriginalSQL = "select 1"
		sessVars.StmtCtx.ResetSQLDigest(sessVars.StmtCtx.OriginalSQL)
		sessVars.StmtCtx.ResourceGroupName = "rg1"

		goCtx := execdetails.ContextWithInitializedExecDetails(context.Background())
		sessVars.RUV2Metrics = execdetails.RUV2MetricsFromContext(goCtx)
		require.NotNil(t, sessVars.RUV2Metrics)
		sessVars.RUV2Metrics.SetBypass(true)
		sessVars.RUV2Metrics.AddResultChunkCells(100)

		ruDetails := goCtx.Value(util.RUDetailsCtxKey).(*util.RUDetails)
		ruDetails.AddTiKVRUV2(12345)
		ruDetails.UpdateTiFlash(&rmpb.Consumption{RRU: 10, WRU: 20})

		execStmt := &executor.ExecStmt{
			Ctx:      ctx,
			GoCtx:    goCtx,
			StmtNode: &ast.SelectStmt{},
		}
		execStmt.FinishExecuteStmt(0, nil, false)

		require.Empty(t, reporter.group)
		require.Zero(t, reporter.tikvRUV2)
		require.Zero(t, reporter.tidbRUV2)
		require.Zero(t, reporter.tiflashRU)
	})

	t.Run("network traffic stats are read atomically", func(t *testing.T) {
		reporter := &mockRUV2ConsumptionReporter{}
		ctx := &mockRUV2ReportingContext{
			Context:  mock.NewContext(),
			reporter: reporter,
		}
		sessVars := ctx.GetSessionVars()
		sessVars.StartTime = time.Now()
		sessVars.StmtCtx.StmtType = "Select"
		sessVars.StmtCtx.OriginalSQL = "select 1"
		sessVars.StmtCtx.ResetSQLDigest(sessVars.StmtCtx.OriginalSQL)
		sessVars.RUV2Metrics = execdetails.NewRUV2Metrics()

		goCtx := execdetails.ContextWithInitializedExecDetails(context.Background())
		tikvExecDetail := goCtx.Value(util.ExecDetailsKey).(*util.ExecDetails)
		execStmt := &executor.ExecStmt{
			Ctx:      ctx,
			GoCtx:    goCtx,
			StmtNode: &ast.SelectStmt{},
		}

		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					atomic.AddInt64(&tikvExecDetail.WaitKVRespDuration, int64(time.Millisecond))
					atomic.AddInt64(&tikvExecDetail.WaitPDRespDuration, int64(time.Millisecond))
					atomic.AddInt64(&tikvExecDetail.BackoffDuration, int64(time.Millisecond))
					atomic.AddInt64(&tikvExecDetail.UnpackedBytesSentKVTotal, 1)
					atomic.AddInt64(&tikvExecDetail.UnpackedBytesReceivedKVTotal, 1)
					atomic.AddInt64(&tikvExecDetail.UnpackedBytesSentKVCrossZone, 1)
					atomic.AddInt64(&tikvExecDetail.UnpackedBytesReceivedKVCrossZone, 1)
					atomic.AddInt64(&tikvExecDetail.UnpackedBytesSentMPPTotal, 1)
					atomic.AddInt64(&tikvExecDetail.UnpackedBytesReceivedMPPTotal, 1)
					atomic.AddInt64(&tikvExecDetail.UnpackedBytesSentMPPCrossZone, 1)
					atomic.AddInt64(&tikvExecDetail.UnpackedBytesReceivedMPPCrossZone, 1)
				}
			}
		}()

		for range 64 {
			execStmt.FinishExecuteStmt(0, nil, false)
		}

		close(done)
		wg.Wait()
	})

	t.Run("preview RU general log waits for SELECT close", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("set tidb_enable_read_billing_demo = on")
		vardef.ProcessGeneralLog.Store(true)
		recorded.TakeAll()

		selectSQL := "select 12345, 'secret_literal'"
		rs, err := tk.Exec(selectSQL)
		require.NoError(t, err)
		require.NotNil(t, rs)
		beforeClose := recorded.TakeAll()
		require.Len(t, beforeClose, 1)
		require.Equal(t, "GENERAL_LOG", beforeClose[0].Message)
		startFields := beforeClose[0].ContextMap()
		// Keep the existing start record's field contract unchanged; preview
		// units belong only to the separate completion record.
		require.Len(t, startFields, 11)
		require.Equal(t, selectSQL, startFields["sql"])
		require.NotContains(t, startFields, "model_version")
		require.NotContains(t, startFields, "weight_version")
		require.NotContains(t, startFields, "units")

		require.NoError(t, rs.Close())
		afterClose := recorded.TakeAll()
		require.Len(t, afterClose, 1)
		require.Equal(t, "GENERAL_LOG_RU_UNITS", afterClose[0].Message)
		completionFields := afterClose[0].ContextMap()
		require.Len(t, completionFields, 6)
		require.Contains(t, completionFields, "conn")
		require.Contains(t, completionFields, "model_version")
		require.Contains(t, completionFields, "weight_version")
		require.Contains(t, completionFields, "normalized_sql")
		require.Contains(t, completionFields, "sql_digest")
		require.Contains(t, completionFields, "units")
		require.NotContains(t, completionFields, "sql")
		requireReadBillingDemoGeneralLogIdentity(t, afterClose[0], selectSQL)
		require.NotContains(t, completionFields["normalized_sql"], "12345")
		require.NotContains(t, completionFields["normalized_sql"], "secret_literal")
	})

	t.Run("preview RU general log publishes point lookup rpc only", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table point_get_rpc_ru_units (id int primary key, v int)")
		tk.MustExec("insert into point_get_rpc_ru_units values (1, 10), (2, 20)")
		tk.MustExec("set tidb_enable_read_billing_demo = on")
		vardef.ProcessGeneralLog.Store(true)

		testCases := []struct {
			name         string
			querySQL     string
			operatorKind string
			requests     int64
			rows         []string
		}{
			{"point get", "select v from point_get_rpc_ru_units where id = 1", "point_get", 3, []string{"10"}},
			{"batch point get", "select v from point_get_rpc_ru_units where id in (1, 2) order by v", "batch_point_get", 4, []string{"10", "20"}},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				recorded.TakeAll()
				ctx := execdetails.ContextWithInitializedExecDetails(context.Background())
				execdetails.RUV2MetricsFromContext(ctx).AddResourceManagerReadCnt(tc.requests)
				tk.MustQueryWithContext(ctx, tc.querySQL).Check(testkit.Rows(tc.rows...))

				entries := recorded.TakeAll()
				var completionEntries []observer.LoggedEntry
				for _, entry := range entries {
					if entry.Message == "GENERAL_LOG_RU_UNITS" {
						completionEntries = append(completionEntries, entry)
					}
				}
				require.Len(t, completionEntries, 1)
				requireReadBillingDemoGeneralLogIdentity(t, completionEntries[0], tc.querySQL)
				rawUnits, ok := completionEntries[0].ContextMap()["units"].([]any)
				require.True(t, ok)
				pointUnits := 0
				for _, rawUnit := range rawUnits {
					unit, ok := rawUnit.(map[string]any)
					require.True(t, ok)
					if unit["op_class"] != "kv_point_lookup" {
						continue
					}
					pointUnits++
					require.Equal(t, "tikv", unit["site"])
					require.Equal(t, tc.operatorKind, unit["operator_kind"])
					require.Equal(t, "read_request_count", unit["unit"])
					require.Equal(t, "ruv2_metrics", unit["input_source"])
					require.Equal(t, "all", unit["input_side"])
					require.Equal(t, float64(tc.requests), unit["value"])
				}
				require.Equal(t, 1, pointUnits)
			})
		}
	})

	t.Run("preview RU general log DML completion is self describing", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table dml_ru_units_identity (id int primary key, v int)")
		tk.MustExec("insert into dml_ru_units_identity values (12345, 1)")
		tk.MustExec("set tidb_enable_read_billing_demo = on")
		vardef.ProcessGeneralLog.Store(true)
		recorded.TakeAll()

		updateSQL := "update dml_ru_units_identity set v = 98765 where id = 12345"
		tk.MustExec(updateSQL)
		entries := recorded.TakeAll()
		require.Len(t, entries, 2)
		// DML currently completes inside runStmt before its deferred legacy
		// GENERAL_LOG. Identity makes the completion independent of this order.
		require.Equal(t, "GENERAL_LOG_RU_UNITS", entries[0].Message)
		require.Equal(t, "GENERAL_LOG", entries[1].Message)
		requireReadBillingDemoGeneralLogIdentity(t, entries[0], updateSQL)
		normalizedSQL := entries[0].ContextMap()["normalized_sql"]
		require.NotContains(t, normalizedSQL, "98765")
		require.NotContains(t, normalizedSQL, "12345")
	})

	t.Run("preview RU general log skips pre-defer compile error", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("set tidb_enable_read_billing_demo = on")
		vardef.ProcessGeneralLog.Store(true)
		recorded.TakeAll()

		_, err := tk.Exec("select * from missing_ru_units_identity")
		require.Error(t, err)
		// Compile fails before legacy GENERAL_LOG is registered. Do not emit an
		// orphan completion that has no corresponding executed statement.
		require.Empty(t, recorded.TakeAll())
	})

	t.Run("preview RU general log covers prepared point get setup error", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table point_get_setup_error_ru_units (id int primary key, v int)")
		tk.MustExec("insert into point_get_setup_error_ru_units values (12345, 1)")
		tk.MustExec("set tidb_enable_read_billing_demo = on")
		vardef.ProcessGeneralLog.Store(true)
		recorded.TakeAll()

		querySQL := "select v from point_get_setup_error_ru_units where id = ?"
		failpointName := "github.com/pingcap/tidb/pkg/session/mockGetTSFail"
		require.NoError(t, failpoint.Enable(failpointName, "return"))
		failpointEnabled := true
		t.Cleanup(func() {
			if failpointEnabled {
				require.NoError(t, failpoint.Disable(failpointName))
			}
		})
		ctx := failpoint.WithHook(context.Background(), func(_ context.Context, name string) bool {
			return name == failpointName
		})
		_, err := tk.ExecWithContext(ctx, querySQL, 12345)
		require.NoError(t, failpoint.Disable(failpointName))
		failpointEnabled = false
		require.Error(t, err)

		entries := recorded.TakeAll()
		require.Len(t, entries, 2)
		require.Equal(t, "GENERAL_LOG", entries[0].Message)
		require.Equal(t, "GENERAL_LOG_RU_UNITS", entries[1].Message)
		requireReadBillingDemoGeneralLogIdentity(t, entries[1], querySQL)
		rawUnits, ok := entries[1].ContextMap()["units"].([]any)
		require.True(t, ok)
		require.Empty(t, rawUnits)
	})

	t.Run("preview RU general log disables cursor detach only when needed", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table detach_ru_units (a int)")
		tk.MustExec("insert into detach_ru_units values (1), (2)")
		tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)

		// General Log alone preserves the existing detach behavior.
		vardef.ProcessGeneralLog.Store(true)
		rs, err := tk.Exec("select * from detach_ru_units where a > ?", 0)
		require.NoError(t, err)
		detachable := rs.(sqlexec.DetachableRecordSet)
		detached, ok, err := detachable.TryDetach()
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, detached)
		require.NoError(t, detached.Close())

		// With both gates enabled, keep the live record set so Close can freeze
		// runtime evidence and emit exactly one completion event.
		vardef.ProcessGeneralLog.Store(false)
		tk.MustExec("set tidb_enable_read_billing_demo = on")
		vardef.ProcessGeneralLog.Store(true)
		recorded.TakeAll()
		rs, err = tk.Exec("select * from detach_ru_units where a > ?", 0)
		require.NoError(t, err)
		detachable = rs.(sqlexec.DetachableRecordSet)
		detached, ok, err = detachable.TryDetach()
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, detached)
		beforeClose := recorded.TakeAll()
		require.Len(t, beforeClose, 1)
		require.Equal(t, "GENERAL_LOG", beforeClose[0].Message)
		require.NoError(t, rs.Close())
		afterClose := recorded.TakeAll()
		require.Len(t, afterClose, 1)
		require.Equal(t, "GENERAL_LOG_RU_UNITS", afterClose[0].Message)
	})

	t.Run("preview RU general log covers early statement error", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table early_error_ru_units (a int primary key)")
		tk.MustExec("insert into early_error_ru_units values (1)")
		tk.MustExec("set tidb_enable_read_billing_demo = on")
		tk.MustExec("begin pessimistic")
		tk.MustQuery("select * from early_error_ru_units where a = 1 for update").Check(testkit.Rows("1"))
		vardef.ProcessGeneralLog.Store(true)
		recorded.TakeAll()
		atomic.StoreUint32(&tk.Session().GetSessionVars().TxnCtx.LockExpire, 1)
		t.Cleanup(func() {
			atomic.StoreUint32(&tk.Session().GetSessionVars().TxnCtx.LockExpire, 0)
		})

		_, err := tk.Exec("select 1")
		atomic.StoreUint32(&tk.Session().GetSessionVars().TxnCtx.LockExpire, 0)
		require.Error(t, err)
		entries := recorded.TakeAll()
		require.Len(t, entries, 2)
		require.Equal(t, "GENERAL_LOG_RU_UNITS", entries[0].Message)
		require.Equal(t, "GENERAL_LOG", entries[1].Message)
		require.Equal(t, "select 1", entries[1].ContextMap()["sql"])
		requireReadBillingDemoGeneralLogIdentity(t, entries[0], "select 1")
		rawUnits, ok := entries[0].ContextMap()["units"].([]any)
		require.True(t, ok)
		require.Empty(t, rawUnits)
	})

	t.Run("preview RU general log covers explain analyze DML commit error", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		tk.MustExec("use test")
		tk.MustExec("set tidb_retry_limit = 0")
		tk.MustExec("create table explain_commit_error_ru_units (id int primary key, v int)")
		tk.MustExec("insert into explain_commit_error_ru_units values (1, 1)")
		tk.MustExec("set tidb_enable_read_billing_demo = on")
		vardef.ProcessGeneralLog.Store(true)
		recorded.TakeAll()

		explainSQL := "explain analyze update explain_commit_error_ru_units set v = v + 1 where id = 1"
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockCommitError8942", `return(true)`))
		failpointEnabled := true
		t.Cleanup(func() {
			if failpointEnabled {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockCommitError8942"))
			}
		})
		rs, err := tk.Exec(explainSQL)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockCommitError8942"))
		failpointEnabled = false
		require.Nil(t, rs)
		require.Error(t, err)
		require.True(t, kv.ErrTxnRetryable.Equal(err), err)
		entries := recorded.TakeAll()
		require.Len(t, entries, 2)
		require.Equal(t, "GENERAL_LOG_RU_UNITS", entries[0].Message)
		require.Equal(t, "GENERAL_LOG", entries[1].Message)
		requireReadBillingDemoGeneralLogIdentity(t, entries[0], explainSQL)
		digest := entries[0].ContextMap()["sql_digest"]

		vardef.ProcessGeneralLog.Store(false)
		tk.MustQuery("select v from explain_commit_error_ru_units where id = 1").Check(testkit.Rows("1"))
		tk.MustQuery("select status, reason, count from information_schema.statements_summary_read_billing_demo_status where digest = ? and site = 'statement'", digest).
			Check(testkit.Rows("error statement_error 1"))
	})

	t.Run("preview RU general log emits once across repeated finish", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		oldLogger := logutil.GeneralLogger
		logutil.GeneralLogger = zap.New(core)
		oldGeneralLog := vardef.ProcessGeneralLog.Swap(true)
		t.Cleanup(func() {
			logutil.GeneralLogger = oldLogger
			vardef.ProcessGeneralLog.Store(oldGeneralLog)
		})

		ctx := mock.NewContext()
		sessVars := ctx.GetSessionVars()
		sessVars.EnableReadBillingDemo = true
		sessVars.StartTime = time.Now()
		sessVars.StmtCtx.StmtType = "Select"
		sessVars.StmtCtx.OriginalSQL = "select 1"
		sessVars.StmtCtx.ResetSQLDigest(sessVars.StmtCtx.OriginalSQL)
		goCtx := execdetails.ContextWithInitializedExecDetails(context.Background())
		sessVars.RUV2Metrics = execdetails.RUV2MetricsFromContext(goCtx)
		execStmt := &executor.ExecStmt{
			Ctx:      ctx,
			GoCtx:    goCtx,
			StmtNode: &ast.SelectStmt{},
		}

		execStmt.FinishExecuteStmt(0, nil, false)
		execStmt.FinishExecuteStmt(0, nil, false)
		entries := recorded.TakeAll()
		completionCount := 0
		for _, entry := range entries {
			if entry.Message == "GENERAL_LOG_RU_UNITS" {
				completionCount++
			}
		}
		require.Equal(t, 1, completionCount)
	})
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

func TestInsertRowsColMultiplyRUV2SQLPath(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("create table src(a int primary key, b int, c int)")
	tk.MustExec("insert into src values (10, 11, 12), (20, 21, 22)")

	runInsert := func(sql string) int64 {
		ctx := execdetails.ContextWithInitializedExecDetails(context.Background())
		tk.MustExecWithContext(ctx, sql)
		metrics := execdetails.RUV2MetricsFromContext(ctx)
		require.NotNil(t, metrics)
		return metrics.ExecutorL5InsertRows()
	}

	require.Equal(t, int64(6), runInsert("insert into t values (1, 2, 3), (2, 3, 4)"))
	require.Equal(t, int64(4), runInsert("insert into t(a, c) values (3, 5), (4, 6)"))
	require.Equal(t, int64(4), runInsert("insert into t(a, b) select a, b from src"))

	oldEnableBatchDML := vardef.EnableBatchDML.Load()
	vardef.EnableBatchDML.Store(true)
	defer vardef.EnableBatchDML.Store(oldEnableBatchDML)

	tk.MustExec("set @@session.tidb_batch_insert=1")
	tk.MustExec("set @@session.tidb_dml_batch_size=2")
	tk.MustExec("create table batch_t(a int primary key, b int, c int)")
	tk.MustExec("insert into batch_t values (100, 100, 100)")

	ctx := execdetails.ContextWithInitializedExecDetails(context.Background())
	_, err := tk.ExecWithContext(ctx, "insert into batch_t values (1, 2, 3), (2, 3, 4), (100, 5, 6), (3, 4, 5)")
	require.Error(t, err)
	metrics := execdetails.RUV2MetricsFromContext(ctx)
	require.NotNil(t, metrics)
	require.Equal(t, int64(12), metrics.ExecutorL5InsertRows())
	tk.MustQuery("select a, b, c from batch_t order by a").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"100 100 100",
	))
}

func TestDMLRowsColMultiplyRUV2SQLPath(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int)")

	runDML := func(sql string) int64 {
		ctx := execdetails.ContextWithInitializedExecDetails(context.Background())
		tk.MustExecWithContext(ctx, sql)
		metrics := execdetails.RUV2MetricsFromContext(ctx)
		require.NotNil(t, metrics)
		return metrics.ExecutorL5InsertRows()
	}

	require.Equal(t, int64(6), runDML("replace into t values (1, 2, 3), (2, 3, 4)"))
	require.Equal(t, int64(6), runDML("update t set b = b + 10 where a in (1, 2)"))
	require.Equal(t, int64(3), runDML("delete from t where a = 1"))

	tk.MustExec("create table multi_del_l(a int primary key)")
	tk.MustExec("create table multi_del_r(a int primary key)")
	tk.MustExec("insert into multi_del_l values (1), (2)")
	tk.MustExec("insert into multi_del_r values (1), (2)")
	require.Equal(t, int64(4), runDML("delete multi_del_l, multi_del_r from multi_del_l join multi_del_r on multi_del_l.a = multi_del_r.a"))

	tk.MustExec("create table outer_l(a int primary key, b int)")
	tk.MustExec("create table outer_r(a int primary key, b int)")
	tk.MustExec("insert into outer_l values (1, 10), (2, 20)")
	tk.MustExec("insert into outer_r values (1, 100)")
	require.Equal(t, int64(2), runDML("update outer_l left join outer_r on outer_l.a = outer_r.a set outer_r.b = outer_r.b + 1"))

	tk.MustExec("create table dup_t(a int primary key, b int)")
	tk.MustExec("create table dup_s(a int, b int)")
	tk.MustExec("insert into dup_t values (1, 10)")
	tk.MustExec("insert into dup_s values (1, 100), (1, 200)")
	require.Equal(t, int64(2), runDML("update dup_t join dup_s on dup_t.a = dup_s.a set dup_t.b = dup_t.b + 1"))
}
