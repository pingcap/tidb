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
// See the License for the specific language governing permissions and
// limitations under the License.

package variable_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/mock"
)

var _ = SerialSuites(&testSessionSuite{})

type testSessionSuite struct {
}

func (*testSessionSuite) TestSetSystemVariable(c *C) {
	v := variable.NewSessionVars()
	v.GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	v.TimeZone = time.UTC
	tests := []struct {
		key   string
		value interface{}
		err   bool
	}{
		{variable.TxnIsolation, "SERIALIZABLE", true},
		{variable.TimeZone, "xyz", true},
		{variable.TiDBOptAggPushDown, "1", false},
		{variable.TiDBOptDistinctAggPushDown, "1", false},
		{variable.TIDBMemQuotaQuery, "1024", false},
		{variable.TIDBMemQuotaHashJoin, "1024", false},
		{variable.TIDBMemQuotaMergeJoin, "1024", false},
		{variable.TIDBMemQuotaSort, "1024", false},
		{variable.TIDBMemQuotaTopn, "1024", false},
		{variable.TIDBMemQuotaIndexLookupReader, "1024", false},
		{variable.TIDBMemQuotaIndexLookupJoin, "1024", false},
		{variable.TIDBMemQuotaNestedLoopApply, "1024", false},
		{variable.TiDBEnableStmtSummary, "1", false},
	}
	for _, t := range tests {
		err := variable.SetSessionSystemVar(v, t.key, types.NewDatum(t.value))
		if t.err {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
	}
}

func (*testSessionSuite) TestSession(c *C) {
	ctx := mock.NewContext()

	ss := ctx.GetSessionVars().StmtCtx
	c.Assert(ss, NotNil)

	// For AffectedRows
	ss.AddAffectedRows(1)
	c.Assert(ss.AffectedRows(), Equals, uint64(1))
	ss.AddAffectedRows(1)
	c.Assert(ss.AffectedRows(), Equals, uint64(2))

	// For RecordRows
	ss.AddRecordRows(1)
	c.Assert(ss.RecordRows(), Equals, uint64(1))
	ss.AddRecordRows(1)
	c.Assert(ss.RecordRows(), Equals, uint64(2))

	// For FoundRows
	ss.AddFoundRows(1)
	c.Assert(ss.FoundRows(), Equals, uint64(1))
	ss.AddFoundRows(1)
	c.Assert(ss.FoundRows(), Equals, uint64(2))

	// For UpdatedRows
	ss.AddUpdatedRows(1)
	c.Assert(ss.UpdatedRows(), Equals, uint64(1))
	ss.AddUpdatedRows(1)
	c.Assert(ss.UpdatedRows(), Equals, uint64(2))

	// For TouchedRows
	ss.AddTouchedRows(1)
	c.Assert(ss.TouchedRows(), Equals, uint64(1))
	ss.AddTouchedRows(1)
	c.Assert(ss.TouchedRows(), Equals, uint64(2))

	// For CopiedRows
	ss.AddCopiedRows(1)
	c.Assert(ss.CopiedRows(), Equals, uint64(1))
	ss.AddCopiedRows(1)
	c.Assert(ss.CopiedRows(), Equals, uint64(2))

	// For last insert id
	ctx.GetSessionVars().SetLastInsertID(1)
	c.Assert(ctx.GetSessionVars().StmtCtx.LastInsertID, Equals, uint64(1))

	ss.ResetForRetry()
	c.Assert(ss.AffectedRows(), Equals, uint64(0))
	c.Assert(ss.FoundRows(), Equals, uint64(0))
	c.Assert(ss.UpdatedRows(), Equals, uint64(0))
	c.Assert(ss.RecordRows(), Equals, uint64(0))
	c.Assert(ss.TouchedRows(), Equals, uint64(0))
	c.Assert(ss.CopiedRows(), Equals, uint64(0))
	c.Assert(ss.WarningCount(), Equals, uint16(0))
}

func (*testSessionSuite) TestSlowLogFormat(c *C) {
	ctx := mock.NewContext()

	seVar := ctx.GetSessionVars()
	c.Assert(seVar, NotNil)

	seVar.User = &auth.UserIdentity{Username: "root", Hostname: "192.168.0.1"}
	seVar.ConnectionInfo = &variable.ConnectionInfo{ClientIP: "192.168.0.1"}
	seVar.ConnectionID = 1
	seVar.CurrentDB = "test"
	seVar.InRestrictedSQL = true
	txnTS := uint64(406649736972468225)
	costTime := time.Second
	execDetail := execdetails.ExecDetails{
		ProcessTime:   time.Second * time.Duration(2),
		WaitTime:      time.Minute,
		BackoffTime:   time.Millisecond,
		RequestCount:  2,
		TotalKeys:     10000,
		ProcessedKeys: 20001,
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
	resultString := `# Txn_start_ts: 406649736972468225
# User@Host: root[root] @ 192.168.0.1 [192.168.0.1]
# Conn_ID: 1
# Query_time: 1
# Parse_time: 0.00000001
# Compile_time: 0.00000001
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Process_time: 2 Wait_time: 60 Backoff_time: 0.001 Request_count: 2 Total_keys: 10000 Process_keys: 20001
# DB: test
# Index_names: [t1:a,t2:b]
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
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
# Has_more_results: true
# KV_total: 10
# PD_total: 11
# Backoff_total: 12
# Write_sql_response_total: 1
# Succ: true
select * from t;`
	sql := "select * from t"
	_, digest := parser.NormalizeDigest(sql)
	logString := seVar.SlowLogFormat(&variable.SlowQueryLogItems{
		TxnTS:             txnTS,
		SQL:               sql,
		Digest:            digest,
		TimeTotal:         costTime,
		TimeParse:         time.Duration(10),
		TimeCompile:       time.Duration(10),
		IndexNames:        "[t1:a,t2:b]",
		StatsInfos:        statsInfos,
		CopTasks:          copTasks,
		ExecDetail:        execDetail,
		MemMax:            memMax,
		DiskMax:           diskMax,
		Prepared:          true,
		PlanFromCache:     true,
		HasMoreResults:    true,
		KVTotal:           10 * time.Second,
		PDTotal:           11 * time.Second,
		BackoffTotal:      12 * time.Second,
		WriteSQLRespTotal: 1 * time.Second,
		Succ:              true,
		RewriteInfo: variable.RewritePhaseInfo{
			DurationRewrite:            3,
			DurationPreprocessSubQuery: 2,
			PreprocessSubQueries:       2,
		},
	})
	c.Assert(logString, Equals, resultString)
}

func (*testSessionSuite) TestIsolationRead(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tiflash", "tidb"}
	})
	sessVars := variable.NewSessionVars()
	_, ok := sessVars.IsolationReadEngines[kv.TiDB]
	c.Assert(ok, Equals, true)
	_, ok = sessVars.IsolationReadEngines[kv.TiKV]
	c.Assert(ok, Equals, false)
	_, ok = sessVars.IsolationReadEngines[kv.TiFlash]
	c.Assert(ok, Equals, true)
}
