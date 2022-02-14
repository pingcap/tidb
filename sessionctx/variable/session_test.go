// Copyright 2015 PingCAP, Inc.
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

package variable_test

import (
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestSetSystemVariable(t *testing.T) {
	v := variable.NewSessionVars()
	v.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	v.TimeZone = time.UTC
	mtx := new(sync.Mutex)

	testCases := []struct {
		key   string
		value string
		err   bool
	}{
		{variable.TxnIsolation, "SERIALIZABLE", true},
		{variable.TimeZone, "xyz", true},
		{variable.TiDBOptAggPushDown, "1", false},
		{variable.TiDBOptDistinctAggPushDown, "1", false},
		{variable.TiDBMemQuotaQuery, "1024", false},
		{variable.TiDBMemQuotaHashJoin, "1024", false},
		{variable.TiDBMemQuotaMergeJoin, "1024", false},
		{variable.TiDBMemQuotaSort, "1024", false},
		{variable.TiDBMemQuotaTopn, "1024", false},
		{variable.TiDBMemQuotaIndexLookupReader, "1024", false},
		{variable.TiDBMemQuotaIndexLookupJoin, "1024", false},
		{variable.TiDBMemQuotaApplyCache, "1024", false},
		{variable.TiDBEnableStmtSummary, "1", true}, // now global only
	}

	for _, tc := range testCases {
		// copy iterator variable into a new variable, see issue #27779
		tc := tc
		t.Run(tc.key, func(t *testing.T) {
			mtx.Lock()
			err := variable.SetSessionSystemVar(v, tc.key, tc.value)
			mtx.Unlock()
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSession(t *testing.T) {
	ctx := mock.NewContext()

	ss := ctx.GetSessionVars().StmtCtx
	require.NotNil(t, ss)

	// For AffectedRows
	ss.AddAffectedRows(1)
	require.Equal(t, uint64(1), ss.AffectedRows())
	ss.AddAffectedRows(1)
	require.Equal(t, uint64(2), ss.AffectedRows())

	// For RecordRows
	ss.AddRecordRows(1)
	require.Equal(t, uint64(1), ss.RecordRows())
	ss.AddRecordRows(1)
	require.Equal(t, uint64(2), ss.RecordRows())

	// For FoundRows
	ss.AddFoundRows(1)
	require.Equal(t, uint64(1), ss.FoundRows())
	ss.AddFoundRows(1)
	require.Equal(t, uint64(2), ss.FoundRows())

	// For UpdatedRows
	ss.AddUpdatedRows(1)
	require.Equal(t, uint64(1), ss.UpdatedRows())
	ss.AddUpdatedRows(1)
	require.Equal(t, uint64(2), ss.UpdatedRows())

	// For TouchedRows
	ss.AddTouchedRows(1)
	require.Equal(t, uint64(1), ss.TouchedRows())
	ss.AddTouchedRows(1)
	require.Equal(t, uint64(2), ss.TouchedRows())

	// For CopiedRows
	ss.AddCopiedRows(1)
	require.Equal(t, uint64(1), ss.CopiedRows())
	ss.AddCopiedRows(1)
	require.Equal(t, uint64(2), ss.CopiedRows())

	// For last insert id
	ctx.GetSessionVars().SetLastInsertID(1)
	require.Equal(t, uint64(1), ctx.GetSessionVars().StmtCtx.LastInsertID)

	ss.ResetForRetry()
	require.Equal(t, uint64(0), ss.AffectedRows())
	require.Equal(t, uint64(0), ss.FoundRows())
	require.Equal(t, uint64(0), ss.UpdatedRows())
	require.Equal(t, uint64(0), ss.RecordRows())
	require.Equal(t, uint64(0), ss.TouchedRows())
	require.Equal(t, uint64(0), ss.CopiedRows())
	require.Equal(t, uint16(0), ss.WarningCount())
}

func TestAllocMPPID(t *testing.T) {
	ctx := mock.NewContext()

	seVar := ctx.GetSessionVars()
	require.NotNil(t, seVar)

	require.Equal(t, int64(1), seVar.AllocMPPTaskID(1))
	require.Equal(t, int64(2), seVar.AllocMPPTaskID(1))
	require.Equal(t, int64(3), seVar.AllocMPPTaskID(1))
	require.Equal(t, int64(1), seVar.AllocMPPTaskID(2))
	require.Equal(t, int64(2), seVar.AllocMPPTaskID(2))
	require.Equal(t, int64(3), seVar.AllocMPPTaskID(2))
}

func TestSlowLogFormat(t *testing.T) {
	ctx := mock.NewContext()

	seVar := ctx.GetSessionVars()
	require.NotNil(t, seVar)

	seVar.User = &auth.UserIdentity{Username: "root", Hostname: "192.168.0.1"}
	seVar.ConnectionInfo = &variable.ConnectionInfo{ClientIP: "192.168.0.1"}
	seVar.ConnectionID = 1
	// the out put of the loged CurrentDB should be 'test', should be to lower cased.
	seVar.CurrentDB = "TeST"
	seVar.InRestrictedSQL = true
	seVar.StmtCtx.WaitLockLeaseTime = 1
	txnTS := uint64(406649736972468225)
	costTime := time.Second
	execDetail := execdetails.ExecDetails{
		BackoffTime:  time.Millisecond,
		RequestCount: 2,
		ScanDetail: &util.ScanDetail{
			ProcessedKeys: 20001,
			TotalKeys:     10000,
		},
		TimeDetail: util.TimeDetail{
			ProcessTime: time.Second * time.Duration(2),
			WaitTime:    time.Minute,
		},
	}
	statsInfos := make(map[string]uint64)
	statsInfos["t1"] = 0
	copTasks := &stmtctx.CopTasksDetails{
		NumCopTasks:       10,
		AvgProcessTime:    time.Second,
		P90ProcessTime:    time.Second * 2,
		MaxProcessAddress: "10.6.131.78",
		MaxProcessTime:    time.Second * 3,
		AvgWaitTime:       time.Millisecond * 10,
		P90WaitTime:       time.Millisecond * 20,
		MaxWaitTime:       time.Millisecond * 30,
		MaxWaitAddress:    "10.6.131.79",
		MaxBackoffTime:    make(map[string]time.Duration),
		AvgBackoffTime:    make(map[string]time.Duration),
		P90BackoffTime:    make(map[string]time.Duration),
		TotBackoffTime:    make(map[string]time.Duration),
		TotBackoffTimes:   make(map[string]int),
		MaxBackoffAddress: make(map[string]string),
	}

	backoffs := []string{"rpcTiKV", "rpcPD", "regionMiss"}
	for _, backoff := range backoffs {
		copTasks.MaxBackoffTime[backoff] = time.Millisecond * 200
		copTasks.MaxBackoffAddress[backoff] = "127.0.0.1"
		copTasks.AvgBackoffTime[backoff] = time.Millisecond * 200
		copTasks.P90BackoffTime[backoff] = time.Millisecond * 200
		copTasks.TotBackoffTime[backoff] = time.Millisecond * 200
		copTasks.TotBackoffTimes[backoff] = 200
	}

	var memMax int64 = 2333
	var diskMax int64 = 6666
	resultFields := `# Txn_start_ts: 406649736972468225
# User@Host: root[root] @ 192.168.0.1 [192.168.0.1]
# Conn_ID: 1
# Exec_retry_time: 5.1 Exec_retry_count: 3
# Query_time: 1
# Parse_time: 0.00000001
# Compile_time: 0.00000001
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Optimize_time: 0.00000001
# Wait_TS: 0.000000003
# Process_time: 2 Wait_time: 60 Backoff_time: 0.001 Request_count: 2 Process_keys: 20001 Total_keys: 10000
# DB: test
# Index_names: [t1:a,t2:b]
# Is_internal: true
# Digest: 01d00e6e93b28184beae487ac05841145d2a2f6a7b16de32a763bed27967e83d
# Stats: t1:pseudo
# Num_cop_tasks: 10
# Cop_proc_avg: 1 Cop_proc_p90: 2 Cop_proc_max: 3 Cop_proc_addr: 10.6.131.78
# Cop_wait_avg: 0.01 Cop_wait_p90: 0.02 Cop_wait_max: 0.03 Cop_wait_addr: 10.6.131.79
# Cop_backoff_regionMiss_total_times: 200 Cop_backoff_regionMiss_total_time: 0.2 Cop_backoff_regionMiss_max_time: 0.2 Cop_backoff_regionMiss_max_addr: 127.0.0.1 Cop_backoff_regionMiss_avg_time: 0.2 Cop_backoff_regionMiss_p90_time: 0.2
# Cop_backoff_rpcPD_total_times: 200 Cop_backoff_rpcPD_total_time: 0.2 Cop_backoff_rpcPD_max_time: 0.2 Cop_backoff_rpcPD_max_addr: 127.0.0.1 Cop_backoff_rpcPD_avg_time: 0.2 Cop_backoff_rpcPD_p90_time: 0.2
# Cop_backoff_rpcTiKV_total_times: 200 Cop_backoff_rpcTiKV_total_time: 0.2 Cop_backoff_rpcTiKV_max_time: 0.2 Cop_backoff_rpcTiKV_max_addr: 127.0.0.1 Cop_backoff_rpcTiKV_avg_time: 0.2 Cop_backoff_rpcTiKV_p90_time: 0.2
# Mem_max: 2333
# Disk_max: 6666
# Prepared: true
# Plan_from_cache: true
# Plan_from_binding: true
# Has_more_results: true
# KV_total: 10
# PD_total: 11
# Backoff_total: 12
# Write_sql_response_total: 1
# Result_rows: 12345
# Succ: true
# IsExplicitTxn: true
# IsWriteCacheTable: true`
	sql := "select * from t;"
	_, digest := parser.NormalizeDigest(sql)
	logItems := &variable.SlowQueryLogItems{
		TxnTS:             txnTS,
		SQL:               sql,
		Digest:            digest.String(),
		TimeTotal:         costTime,
		TimeParse:         time.Duration(10),
		TimeCompile:       time.Duration(10),
		TimeOptimize:      time.Duration(10),
		TimeWaitTS:        time.Duration(3),
		IndexNames:        "[t1:a,t2:b]",
		StatsInfos:        statsInfos,
		CopTasks:          copTasks,
		ExecDetail:        execDetail,
		MemMax:            memMax,
		DiskMax:           diskMax,
		Prepared:          true,
		PlanFromCache:     true,
		PlanFromBinding:   true,
		HasMoreResults:    true,
		KVTotal:           10 * time.Second,
		PDTotal:           11 * time.Second,
		BackoffTotal:      12 * time.Second,
		WriteSQLRespTotal: 1 * time.Second,
		ResultRows:        12345,
		Succ:              true,
		RewriteInfo: variable.RewritePhaseInfo{
			DurationRewrite:            3,
			DurationPreprocessSubQuery: 2,
			PreprocessSubQueries:       2,
		},
		ExecRetryCount:    3,
		ExecRetryTime:     5*time.Second + time.Millisecond*100,
		IsExplicitTxn:     true,
		IsWriteCacheTable: true,
	}
	logString := seVar.SlowLogFormat(logItems)
	require.Equal(t, resultFields+"\n"+sql, logString)

	seVar.CurrentDBChanged = true
	logString = seVar.SlowLogFormat(logItems)
	require.Equal(t, resultFields+"\n"+"use test;\n"+sql, logString)
	require.False(t, seVar.CurrentDBChanged)
}

func TestIsolationRead(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tiflash", "tidb"}
	})
	sessVars := variable.NewSessionVars()
	_, ok := sessVars.IsolationReadEngines[kv.TiDB]
	require.True(t, ok)
	_, ok = sessVars.IsolationReadEngines[kv.TiKV]
	require.False(t, ok)
	_, ok = sessVars.IsolationReadEngines[kv.TiFlash]
	require.True(t, ok)
}
