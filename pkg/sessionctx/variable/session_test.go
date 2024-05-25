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
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestSetSystemVariable(t *testing.T) {
	v := variable.NewSessionVars(nil)
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
		{variable.TiDBOptDeriveTopN, "1", false},
		{variable.TiDBOptDistinctAggPushDown, "1", false},
		{variable.TiDBMemQuotaQuery, "1024", false},
		{variable.TiDBMemQuotaApplyCache, "1024", false},
		{variable.TiDBEnableStmtSummary, "1", true}, // now global only
		{variable.TiDBEnableRowLevelChecksum, "1", true},
	}

	for _, tc := range testCases {
		// copy iterator variable into a new variable, see issue #27779
		tc := tc
		t.Run(tc.key, func(t *testing.T) {
			mtx.Lock()
			err := v.SetSystemVar(tc.key, tc.value)
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
	require.Equal(t, int64(1), plannercore.AllocMPPTaskID(ctx))
	require.Equal(t, int64(2), plannercore.AllocMPPTaskID(ctx))
	require.Equal(t, int64(3), plannercore.AllocMPPTaskID(ctx))
}

func TestSlowLogFormat(t *testing.T) {
	ctx := mock.NewContext()

	seVar := ctx.GetSessionVars()
	require.NotNil(t, seVar)

	seVar.User = &auth.UserIdentity{Username: "root", Hostname: "192.168.0.1"}
	seVar.ConnectionInfo = &variable.ConnectionInfo{ClientIP: "192.168.0.1"}
	seVar.ConnectionID = 1
	seVar.SessionAlias = "aliasabc"
	// the output of the logged CurrentDB should be 'test', should be to lower cased.
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
		DetailsNeedP90: execdetails.DetailsNeedP90{
			TimeDetail: util.TimeDetail{
				ProcessTime: time.Second * time.Duration(2),
				WaitTime:    time.Minute,
			},
		},
	}
	usedStats1 := &stmtctx.UsedStatsInfoForTable{
		Name:                  "t1",
		TblInfo:               nil,
		Version:               123,
		RealtimeCount:         1000,
		ModifyCount:           0,
		ColumnStatsLoadStatus: map[int64]string{2: "allEvicted", 3: "onlyCmsEvicted"},
		IndexStatsLoadStatus:  map[int64]string{1: "allLoaded", 2: "allLoaded"},
	}
	usedStats2 := &stmtctx.UsedStatsInfoForTable{
		Name:                  "t2",
		TblInfo:               nil,
		Version:               0,
		RealtimeCount:         10000,
		ModifyCount:           0,
		ColumnStatsLoadStatus: map[int64]string{2: "unInitialized"},
	}

	copTasks := &execdetails.CopTasksDetails{
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
# Keyspace_name: keyspace_a
# Keyspace_ID: 1
# User@Host: root[root] @ 192.168.0.1 [192.168.0.1]
# Conn_ID: 1
# Session_alias: aliasabc
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
# Digest: e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7
# Stats: t1:123[1000;0][ID 1:allLoaded,ID 2:allLoaded][ID 2:allEvicted,ID 3:onlyCmsEvicted],t2:pseudo[10000;0]
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
# IsSyncStatsFailed: false
# IsWriteCacheTable: true
# Resource_group: rg1
# Request_unit_read: 50
# Request_unit_write: 100.56
# Time_queued_by_rc: 0.134`
	sql := "select * from t;"
	_, digest := parser.NormalizeDigest(sql)
	logItems := &variable.SlowQueryLogItems{
		TxnTS:             txnTS,
		KeyspaceName:      "keyspace_a",
		KeyspaceID:        1,
		SQL:               sql,
		Digest:            digest.String(),
		TimeTotal:         costTime,
		TimeParse:         time.Duration(10),
		TimeCompile:       time.Duration(10),
		TimeOptimize:      time.Duration(10),
		TimeWaitTS:        time.Duration(3),
		IndexNames:        "[t1:a,t2:b]",
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
		UsedStats:         &stmtctx.UsedStatsInfo{},
		ResourceGroupName: "rg1",
		RRU:               50.0,
		WRU:               100.56,
		WaitRUDuration:    134 * time.Millisecond,
	}
	logItems.UsedStats.RecordUsedInfo(1, usedStats1)
	logItems.UsedStats.RecordUsedInfo(2, usedStats2)
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
	sessVars := variable.NewSessionVars(nil)
	_, ok := sessVars.IsolationReadEngines[kv.TiDB]
	require.True(t, ok)
	_, ok = sessVars.IsolationReadEngines[kv.TiKV]
	require.False(t, ok)
	_, ok = sessVars.IsolationReadEngines[kv.TiFlash]
	require.True(t, ok)
}

func TestTableDeltaClone(t *testing.T) {
	td0 := variable.TableDelta{
		Delta:    1,
		Count:    2,
		ColSize:  map[int64]int64{1: 1, 2: 2},
		InitTime: time.Now(),
		TableID:  5,
	}
	td1 := td0.Clone()
	require.Equal(t, td0, td1)
	td0.ColSize[3] = 3
	require.NotEqual(t, td0, td1)

	td2 := td0.Clone()
	require.Equal(t, td0, td2)
	td0.InitTime = td0.InitTime.Add(time.Second)
	require.NotEqual(t, td0, td2)
}

func TestTransactionContextSavepoint(t *testing.T) {
	tc := &variable.TransactionContext{
		TxnCtxNeedToRestore: variable.TxnCtxNeedToRestore{
			TableDeltaMap: map[int64]variable.TableDelta{
				1: {
					Delta:    1,
					Count:    2,
					ColSize:  map[int64]int64{1: 1},
					InitTime: time.Now(),
					TableID:  5,
				},
			},
		},
	}
	tc.SetPessimisticLockCache([]byte{'a'}, []byte{'a'})
	tc.FlushStmtPessimisticLockCache()

	tc.AddSavepoint("S1", nil)
	require.Equal(t, 1, len(tc.Savepoints))
	require.Equal(t, 1, len(tc.Savepoints[0].TxnCtxSavepoint.TableDeltaMap))
	require.Equal(t, "s1", tc.Savepoints[0].Name)

	succ := tc.DeleteSavepoint("s2")
	require.False(t, succ)
	require.Equal(t, 1, len(tc.Savepoints))

	tc.TableDeltaMap[1].ColSize[2] = 2
	tc.TableDeltaMap[2] = variable.TableDelta{
		Delta:    6,
		Count:    7,
		ColSize:  map[int64]int64{8: 8},
		InitTime: time.Now(),
		TableID:  9,
	}
	tc.SetPessimisticLockCache([]byte{'b'}, []byte{'b'})
	tc.FlushStmtPessimisticLockCache()

	tc.AddSavepoint("S2", nil)
	require.Equal(t, 2, len(tc.Savepoints))
	require.Equal(t, 1, len(tc.Savepoints[0].TxnCtxSavepoint.TableDeltaMap))
	require.Equal(t, 1, len(tc.Savepoints[0].TxnCtxSavepoint.TableDeltaMap[1].ColSize))
	require.Equal(t, "s1", tc.Savepoints[0].Name)
	require.Equal(t, 2, len(tc.Savepoints[1].TxnCtxSavepoint.TableDeltaMap))
	require.Equal(t, "s2", tc.Savepoints[1].Name)

	tc.TableDeltaMap[3] = variable.TableDelta{
		Delta:    10,
		Count:    11,
		ColSize:  map[int64]int64{12: 12},
		InitTime: time.Now(),
		TableID:  13,
	}
	tc.SetPessimisticLockCache([]byte{'c'}, []byte{'c'})
	tc.FlushStmtPessimisticLockCache()

	tc.AddSavepoint("s2", nil)
	require.Equal(t, 2, len(tc.Savepoints))
	require.Equal(t, 3, len(tc.Savepoints[1].TxnCtxSavepoint.TableDeltaMap))
	require.Equal(t, "s2", tc.Savepoints[1].Name)

	tc.RollbackToSavepoint("s1")
	require.Equal(t, 1, len(tc.Savepoints))
	require.Equal(t, 1, len(tc.Savepoints[0].TxnCtxSavepoint.TableDeltaMap))
	require.Equal(t, "s1", tc.Savepoints[0].Name)
	val, ok := tc.GetKeyInPessimisticLockCache([]byte{'a'})
	require.True(t, ok)
	require.Equal(t, []byte{'a'}, val)
	val, ok = tc.GetKeyInPessimisticLockCache([]byte{'b'})
	require.False(t, ok)
	require.Nil(t, val)

	succ = tc.DeleteSavepoint("s1")
	require.True(t, succ)
	require.Equal(t, 0, len(tc.Savepoints))
}

func TestNonPreparedPlanCacheStmt(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	sessVars.SessionPlanCacheSize = 100
	sql1 := "select * from t where a>?"
	sql2 := "select * from t where a<?"
	require.Nil(t, sessVars.GetNonPreparedPlanCacheStmt(sql1))
	require.Nil(t, sessVars.GetNonPreparedPlanCacheStmt(sql2))

	sessVars.AddNonPreparedPlanCacheStmt(sql1, new(plannercore.PlanCacheStmt))
	require.NotNil(t, sessVars.GetNonPreparedPlanCacheStmt(sql1))
	require.Nil(t, sessVars.GetNonPreparedPlanCacheStmt(sql2))

	sessVars.AddNonPreparedPlanCacheStmt(sql2, new(plannercore.PlanCacheStmt))
	require.NotNil(t, sessVars.GetNonPreparedPlanCacheStmt(sql1))
	require.NotNil(t, sessVars.GetNonPreparedPlanCacheStmt(sql2))
}

func TestHookContext(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ctx := mock.NewContext()
	ctx.Store = store
	sv := variable.SysVar{Scope: variable.ScopeGlobal | variable.ScopeSession, Name: "testhooksysvar", Value: variable.On, Type: variable.TypeBool, SetSession: func(s *variable.SessionVars, val string) error {
		require.Equal(t, s.GetStore(), store)
		return nil
	}}
	variable.RegisterSysVar(&sv)

	ctx.GetSessionVars().SetSystemVar("testhooksysvar", "test")
}

func TestGetReuseChunk(t *testing.T) {
	fieldTypes := []*types.FieldType{
		types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeJSON).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeFloat).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).BuildP(),
	}

	sessVars := variable.NewSessionVars(nil)

	// SetAlloc efficient
	sessVars.SetAlloc(nil)
	require.False(t, sessVars.IsAllocValid())
	require.False(t, sessVars.GetUseChunkAlloc())
	// alloc is nil ，Allocate memory from the system
	chk1 := sessVars.GetChunkAllocator().Alloc(fieldTypes, 10, 10)
	require.NotNil(t, chk1)

	chunkReuseMap := make(map[*chunk.Chunk]struct{}, 14)
	columnReuseMap := make(map[*chunk.Column]struct{}, 14)

	alloc := chunk.NewAllocator()
	sessVars.EnableReuseChunk = true
	sessVars.SetAlloc(alloc)
	require.True(t, sessVars.IsAllocValid())
	require.False(t, sessVars.GetUseChunkAlloc())

	//tries to apply from the cache
	initCap := 10
	chk1 = sessVars.GetChunkAllocator().Alloc(fieldTypes, initCap, initCap)
	require.NotNil(t, chk1)
	chunkReuseMap[chk1] = struct{}{}
	for i := 0; i < chk1.NumCols(); i++ {
		columnReuseMap[chk1.Column(i)] = struct{}{}
	}
	require.True(t, sessVars.GetUseChunkAlloc())

	alloc.Reset()
	chkres1 := sessVars.GetChunkAllocator().Alloc(fieldTypes, 10, 10)
	require.NotNil(t, chkres1)
	_, exist := chunkReuseMap[chkres1]
	require.True(t, exist)
	for i := 0; i < chkres1.NumCols(); i++ {
		_, exist := columnReuseMap[chkres1.Column(i)]
		require.True(t, exist)
	}

	var allocpool chunk.Allocator = alloc
	sessVars.ClearAlloc(&allocpool, false)
	require.Equal(t, alloc, allocpool)

	sessVars.ClearAlloc(&allocpool, true)
	require.NotEqual(t, allocpool, alloc)
	require.False(t, sessVars.IsAllocValid())
}

func TestUserVarConcurrently(t *testing.T) {
	sv := variable.NewSessionVars(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	var wg util2.WaitGroupWrapper
	wg.Run(func() {
		for i := 0; ; i++ {
			select {
			case <-time.After(time.Millisecond):
				name := strconv.Itoa(i)
				sv.SetUserVarVal(name, types.Datum{})
				sv.GetUserVarVal(name)
			case <-ctx.Done():
				return
			}
		}
	})
	wg.Run(func() {
		for {
			select {
			case <-time.After(time.Millisecond):
				var states sessionstates.SessionStates
				require.NoError(t, sv.EncodeSessionStates(ctx, &states))
				require.NoError(t, sv.DecodeSessionStates(ctx, &states))
			case <-ctx.Done():
				return
			}
		}
	})
	wg.Wait()
	cancel()
}

func TestSetStatus(t *testing.T) {
	sv := variable.NewSessionVars(nil)
	require.True(t, sv.IsAutocommit())
	sv.SetStatusFlag(mysql.ServerStatusInTrans, true)
	require.True(t, sv.InTxn())
	sv.SetStatusFlag(mysql.ServerStatusCursorExists, true)
	require.True(t, sv.InTxn())
	sv.SetStatusFlag(mysql.ServerStatusInTrans, false)
	require.True(t, sv.HasStatusFlag(mysql.ServerStatusCursorExists))
	require.False(t, sv.InTxn())
	require.Equal(t, mysql.ServerStatusAutocommit|mysql.ServerStatusCursorExists, sv.Status())
}
