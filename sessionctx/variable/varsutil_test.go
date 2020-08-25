// Copyright 2016 PingCAP, Inc.
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

package variable

import (
	"encoding/json"
	"reflect"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testVarsutilSuite{})

type testVarsutilSuite struct {
}

func (s *testVarsutilSuite) TestTiDBOptOn(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		val string
		on  bool
	}{
		{"ON", true},
		{"on", true},
		{"On", true},
		{"1", true},
		{"off", false},
		{"No", false},
		{"0", false},
		{"1.1", false},
		{"", false},
	}
	for _, t := range tbl {
		on := TiDBOptOn(t.val)
		c.Assert(on, Equals, t.on)
	}
}

func (s *testVarsutilSuite) TestNewSessionVars(c *C) {
	defer testleak.AfterTest(c)()
	vars := NewSessionVars()

	c.Assert(vars.IndexJoinBatchSize, Equals, DefIndexJoinBatchSize)
	c.Assert(vars.IndexLookupSize, Equals, DefIndexLookupSize)
	c.Assert(vars.indexLookupConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.indexSerialScanConcurrency, Equals, DefIndexSerialScanConcurrency)
	c.Assert(vars.indexLookupJoinConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.hashJoinConcurrency, Equals, DefTiDBHashJoinConcurrency)
	c.Assert(vars.IndexLookupConcurrency(), Equals, DefExecutorConcurrency)
	c.Assert(vars.IndexSerialScanConcurrency(), Equals, DefIndexSerialScanConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency(), Equals, DefExecutorConcurrency)
	c.Assert(vars.HashJoinConcurrency(), Equals, DefExecutorConcurrency)
	c.Assert(vars.AllowBatchCop, Equals, DefTiDBAllowBatchCop)
	c.Assert(vars.AllowBCJ, Equals, DefOptBCJ)
	c.Assert(vars.projectionConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.hashAggPartialConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.hashAggFinalConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.windowConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.distSQLScanConcurrency, Equals, DefDistSQLScanConcurrency)
	c.Assert(vars.ProjectionConcurrency(), Equals, DefExecutorConcurrency)
	c.Assert(vars.HashAggPartialConcurrency(), Equals, DefExecutorConcurrency)
	c.Assert(vars.HashAggFinalConcurrency(), Equals, DefExecutorConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, DefExecutorConcurrency)
	c.Assert(vars.DistSQLScanConcurrency(), Equals, DefDistSQLScanConcurrency)
	c.Assert(vars.ExecutorConcurrency, Equals, DefExecutorConcurrency)
	c.Assert(vars.MaxChunkSize, Equals, DefMaxChunkSize)
	c.Assert(vars.DMLBatchSize, Equals, DefDMLBatchSize)
	c.Assert(vars.MemQuotaQuery, Equals, config.GetGlobalConfig().MemQuotaQuery)
	c.Assert(vars.MemQuotaHashJoin, Equals, int64(DefTiDBMemQuotaHashJoin))
	c.Assert(vars.MemQuotaMergeJoin, Equals, int64(DefTiDBMemQuotaMergeJoin))
	c.Assert(vars.MemQuotaSort, Equals, int64(DefTiDBMemQuotaSort))
	c.Assert(vars.MemQuotaTopn, Equals, int64(DefTiDBMemQuotaTopn))
	c.Assert(vars.MemQuotaIndexLookupReader, Equals, int64(DefTiDBMemQuotaIndexLookupReader))
	c.Assert(vars.MemQuotaIndexLookupJoin, Equals, int64(DefTiDBMemQuotaIndexLookupJoin))
	c.Assert(vars.MemQuotaNestedLoopApply, Equals, int64(DefTiDBMemQuotaNestedLoopApply))
	c.Assert(vars.EnableRadixJoin, Equals, DefTiDBUseRadixJoin)
	c.Assert(vars.AllowWriteRowID, Equals, DefOptWriteRowID)
	c.Assert(vars.TiDBOptJoinReorderThreshold, Equals, DefTiDBOptJoinReorderThreshold)
	c.Assert(vars.EnableFastAnalyze, Equals, DefTiDBUseFastAnalyze)
	c.Assert(vars.FoundInPlanCache, Equals, DefTiDBFoundInPlanCache)
	c.Assert(vars.AllowAutoRandExplicitInsert, Equals, DefTiDBAllowAutoRandExplicitInsert)
	c.Assert(vars.ShardAllocateStep, Equals, int64(DefTiDBShardAllocateStep))

	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.MemQuota))
	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.BatchSize))
}

func assertFieldsGreaterThanZero(c *C, val reflect.Value) {
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		c.Assert(fieldVal.Int(), Greater, int64(0))
	}
}

func (s *testVarsutilSuite) TestVarsutil(c *C) {
	defer testleak.AfterTest(c)()
	v := NewSessionVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor()

	err := SetSessionSystemVar(v, "autocommit", types.NewStringDatum("1"))
	c.Assert(err, IsNil)
	val, err := GetSessionSystemVar(v, "autocommit")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(SetSessionSystemVar(v, "autocommit", types.Datum{}), NotNil)

	// 0 converts to OFF
	err = SetSessionSystemVar(v, "foreign_key_checks", types.NewStringDatum("0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, "foreign_key_checks")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "OFF")

	// 1/ON is not supported (generates a warning and sets to OFF)
	err = SetSessionSystemVar(v, "foreign_key_checks", types.NewStringDatum("1"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, "foreign_key_checks")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "OFF")

	err = SetSessionSystemVar(v, "sql_mode", types.NewStringDatum("strict_trans_tables"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, "sql_mode")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "STRICT_TRANS_TABLES")
	c.Assert(v.StrictSQLMode, IsTrue)
	SetSessionSystemVar(v, "sql_mode", types.NewStringDatum(""))
	c.Assert(v.StrictSQLMode, IsFalse)

	err = SetSessionSystemVar(v, "character_set_connection", types.NewStringDatum("utf8"))
	c.Assert(err, IsNil)
	err = SetSessionSystemVar(v, "collation_connection", types.NewStringDatum("utf8_general_ci"))
	c.Assert(err, IsNil)
	charset, collation := v.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(collation, Equals, "utf8_general_ci")

	c.Assert(SetSessionSystemVar(v, "character_set_results", types.Datum{}), IsNil)

	// Test case for time_zone session variable.
	tests := []struct {
		input        string
		expect       string
		compareValue bool
		diff         time.Duration
		err          error
	}{
		{"Europe/Helsinki", "Europe/Helsinki", true, -2 * time.Hour, nil},
		{"US/Eastern", "US/Eastern", true, 5 * time.Hour, nil},
		//TODO: Check it out and reopen this case.
		//{"SYSTEM", "Local", false, 0},
		{"+10:00", "", true, -10 * time.Hour, nil},
		{"-6:00", "", true, 6 * time.Hour, nil},
		{"+14:00", "", true, -14 * time.Hour, nil},
		{"-12:59", "", true, 12*time.Hour + 59*time.Minute, nil},
		{"+14:01", "", false, -14 * time.Hour, ErrUnknownTimeZone.GenWithStackByArgs("+14:01")},
		{"-13:00", "", false, 13 * time.Hour, ErrUnknownTimeZone.GenWithStackByArgs("-13:00")},
	}
	for _, tt := range tests {
		err = SetSessionSystemVar(v, TimeZone, types.NewStringDatum(tt.input))
		if tt.err != nil {
			c.Assert(err, NotNil)
			continue
		}

		c.Assert(err, IsNil)
		c.Assert(v.TimeZone.String(), Equals, tt.expect)
		if tt.compareValue {
			SetSessionSystemVar(v, TimeZone, types.NewStringDatum(tt.input))
			t1 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			t2 := time.Date(2000, 1, 1, 0, 0, 0, 0, v.TimeZone)
			c.Assert(t2.Sub(t1), Equals, tt.diff)
		}
	}
	err = SetSessionSystemVar(v, TimeZone, types.NewStringDatum("6:00"))
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ErrUnknownTimeZone), IsTrue)

	// Test case for sql mode.
	for str, mode := range mysql.Str2SQLMode {
		SetSessionSystemVar(v, "sql_mode", types.NewStringDatum(str))
		if modeParts, exists := mysql.CombinationSQLMode[str]; exists {
			for _, part := range modeParts {
				mode |= mysql.Str2SQLMode[part]
			}
		}
		c.Assert(v.SQLMode, Equals, mode)
	}

	err = SetSessionSystemVar(v, "tidb_opt_broadcast_join", types.NewStringDatum("1"))
	c.Assert(err, IsNil)
	err = SetSessionSystemVar(v, "tidb_allow_batch_cop", types.NewStringDatum("0"))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)
	err = SetSessionSystemVar(v, "tidb_opt_broadcast_join", types.NewStringDatum("0"))
	c.Assert(err, IsNil)
	err = SetSessionSystemVar(v, "tidb_allow_batch_cop", types.NewStringDatum("0"))
	c.Assert(err, IsNil)
	err = SetSessionSystemVar(v, "tidb_opt_broadcast_join", types.NewStringDatum("1"))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)

	// Combined sql_mode
	SetSessionSystemVar(v, "sql_mode", types.NewStringDatum("REAL_AS_FLOAT,ANSI_QUOTES"))
	c.Assert(v.SQLMode, Equals, mysql.ModeRealAsFloat|mysql.ModeANSIQuotes)

	// Test case for tidb_index_serial_scan_concurrency.
	c.Assert(v.IndexSerialScanConcurrency(), Equals, DefIndexSerialScanConcurrency)
	SetSessionSystemVar(v, TiDBIndexSerialScanConcurrency, types.NewStringDatum("4"))
	c.Assert(v.IndexSerialScanConcurrency(), Equals, 4)

	// Test case for tidb_batch_insert.
	c.Assert(v.BatchInsert, IsFalse)
	SetSessionSystemVar(v, TiDBBatchInsert, types.NewStringDatum("1"))
	c.Assert(v.BatchInsert, IsTrue)

	c.Assert(v.InitChunkSize, Equals, 32)
	c.Assert(v.MaxChunkSize, Equals, 1024)
	err = SetSessionSystemVar(v, TiDBMaxChunkSize, types.NewStringDatum("2"))
	c.Assert(err, NotNil)
	err = SetSessionSystemVar(v, TiDBInitChunkSize, types.NewStringDatum("1024"))
	c.Assert(err, NotNil)

	// Test case for TiDBConfig session variable.
	err = SetSessionSystemVar(v, TiDBConfig, types.NewStringDatum("abc"))
	c.Assert(terror.ErrorEqual(err, ErrReadOnly), IsTrue)
	val, err = GetSessionSystemVar(v, TiDBConfig)
	c.Assert(err, IsNil)
	bVal, err := json.MarshalIndent(config.GetGlobalConfig(), "", "\t")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, string(bVal))

	SetSessionSystemVar(v, TiDBEnableStreaming, types.NewStringDatum("1"))
	val, err = GetSessionSystemVar(v, TiDBEnableStreaming)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(v.EnableStreaming, Equals, true)
	SetSessionSystemVar(v, TiDBEnableStreaming, types.NewStringDatum("0"))
	val, err = GetSessionSystemVar(v, TiDBEnableStreaming)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.EnableStreaming, Equals, false)

	c.Assert(v.OptimizerSelectivityLevel, Equals, DefTiDBOptimizerSelectivityLevel)
	SetSessionSystemVar(v, TiDBOptimizerSelectivityLevel, types.NewIntDatum(1))
	c.Assert(v.OptimizerSelectivityLevel, Equals, 1)

	err = SetSessionSystemVar(v, TiDBDDLReorgWorkerCount, types.NewIntDatum(-1))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)

	SetSessionSystemVar(v, TiDBDDLReorgWorkerCount, types.NewIntDatum(int64(maxDDLReorgWorkerCount)+1))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)

	err = SetSessionSystemVar(v, TiDBRetryLimit, types.NewStringDatum("3"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBRetryLimit)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "3")
	c.Assert(v.RetryLimit, Equals, int64(3))

	c.Assert(v.EnableTablePartition, Equals, "")
	err = SetSessionSystemVar(v, TiDBEnableTablePartition, types.NewStringDatum("on"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBEnableTablePartition)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "on")
	c.Assert(v.EnableTablePartition, Equals, "on")

	c.Assert(v.TiDBOptJoinReorderThreshold, Equals, DefTiDBOptJoinReorderThreshold)
	err = SetSessionSystemVar(v, TiDBOptJoinReorderThreshold, types.NewIntDatum(5))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptJoinReorderThreshold)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5")
	c.Assert(v.TiDBOptJoinReorderThreshold, Equals, 5)

	err = SetSessionSystemVar(v, TiDBCheckMb4ValueInUTF8, types.NewStringDatum("1"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBCheckMb4ValueInUTF8)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(config.GetGlobalConfig().CheckMb4ValueInUTF8, Equals, true)
	err = SetSessionSystemVar(v, TiDBCheckMb4ValueInUTF8, types.NewStringDatum("0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBCheckMb4ValueInUTF8)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(config.GetGlobalConfig().CheckMb4ValueInUTF8, Equals, false)

	SetSessionSystemVar(v, TiDBLowResolutionTSO, types.NewStringDatum("1"))
	val, err = GetSessionSystemVar(v, TiDBLowResolutionTSO)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(v.LowResolutionTSO, Equals, true)
	SetSessionSystemVar(v, TiDBLowResolutionTSO, types.NewStringDatum("0"))
	val, err = GetSessionSystemVar(v, TiDBLowResolutionTSO)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.LowResolutionTSO, Equals, false)

	c.Assert(v.CorrelationThreshold, Equals, 0.9)
	err = SetSessionSystemVar(v, TiDBOptCorrelationThreshold, types.NewStringDatum("0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptCorrelationThreshold)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.CorrelationThreshold, Equals, float64(0))

	c.Assert(v.CPUFactor, Equals, 3.0)
	err = SetSessionSystemVar(v, TiDBOptCPUFactor, types.NewStringDatum("5.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptCPUFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.CPUFactor, Equals, 5.0)

	c.Assert(v.CopCPUFactor, Equals, 3.0)
	err = SetSessionSystemVar(v, TiDBOptCopCPUFactor, types.NewStringDatum("5.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptCopCPUFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.CopCPUFactor, Equals, 5.0)

	c.Assert(v.CopTiFlashConcurrencyFactor, Equals, 24.0)
	err = SetSessionSystemVar(v, TiDBOptTiFlashConcurrencyFactor, types.NewStringDatum("5.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptTiFlashConcurrencyFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.CopCPUFactor, Equals, 5.0)

	c.Assert(v.NetworkFactor, Equals, 1.0)
	err = SetSessionSystemVar(v, TiDBOptNetworkFactor, types.NewStringDatum("3.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptNetworkFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "3.0")
	c.Assert(v.NetworkFactor, Equals, 3.0)

	c.Assert(v.ScanFactor, Equals, 1.5)
	err = SetSessionSystemVar(v, TiDBOptScanFactor, types.NewStringDatum("3.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptScanFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "3.0")
	c.Assert(v.ScanFactor, Equals, 3.0)

	c.Assert(v.DescScanFactor, Equals, 3.0)
	err = SetSessionSystemVar(v, TiDBOptDescScanFactor, types.NewStringDatum("5.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptDescScanFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.DescScanFactor, Equals, 5.0)

	c.Assert(v.SeekFactor, Equals, 20.0)
	err = SetSessionSystemVar(v, TiDBOptSeekFactor, types.NewStringDatum("50.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptSeekFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "50.0")
	c.Assert(v.SeekFactor, Equals, 50.0)

	c.Assert(v.MemoryFactor, Equals, 0.001)
	err = SetSessionSystemVar(v, TiDBOptMemoryFactor, types.NewStringDatum("1.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptMemoryFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1.0")
	c.Assert(v.MemoryFactor, Equals, 1.0)

	c.Assert(v.DiskFactor, Equals, 1.5)
	err = SetSessionSystemVar(v, TiDBOptDiskFactor, types.NewStringDatum("1.1"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptDiskFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1.1")
	c.Assert(v.DiskFactor, Equals, 1.1)

	c.Assert(v.ConcurrencyFactor, Equals, 3.0)
	err = SetSessionSystemVar(v, TiDBOptConcurrencyFactor, types.NewStringDatum("5.0"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBOptConcurrencyFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.ConcurrencyFactor, Equals, 5.0)

	SetSessionSystemVar(v, TiDBReplicaRead, types.NewStringDatum("follower"))
	val, err = GetSessionSystemVar(v, TiDBReplicaRead)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "follower")
	c.Assert(v.GetReplicaRead(), Equals, kv.ReplicaReadFollower)
	SetSessionSystemVar(v, TiDBReplicaRead, types.NewStringDatum("leader"))
	val, err = GetSessionSystemVar(v, TiDBReplicaRead)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "leader")
	c.Assert(v.GetReplicaRead(), Equals, kv.ReplicaReadLeader)
	SetSessionSystemVar(v, TiDBReplicaRead, types.NewStringDatum("leader-and-follower"))
	val, err = GetSessionSystemVar(v, TiDBReplicaRead)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "leader-and-follower")
	c.Assert(v.GetReplicaRead(), Equals, kv.ReplicaReadMixed)

	err = SetSessionSystemVar(v, TiDBEnableStmtSummary, types.NewStringDatum("on"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBEnableStmtSummary)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")

	err = SetSessionSystemVar(v, TiDBStmtSummaryRefreshInterval, types.NewStringDatum("10"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBStmtSummaryRefreshInterval)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "10")

	err = SetSessionSystemVar(v, TiDBStmtSummaryHistorySize, types.NewStringDatum("10"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBStmtSummaryHistorySize)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "10")

	err = SetSessionSystemVar(v, TiDBStmtSummaryMaxStmtCount, types.NewStringDatum("10"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBStmtSummaryMaxStmtCount)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "10")
	err = SetSessionSystemVar(v, TiDBStmtSummaryMaxStmtCount, types.NewStringDatum("a"))
	c.Assert(err, ErrorMatches, ".*Incorrect argument type to variable 'tidb_stmt_summary_max_stmt_count'")

	err = SetSessionSystemVar(v, TiDBStmtSummaryMaxSQLLength, types.NewStringDatum("10"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBStmtSummaryMaxSQLLength)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "10")
	err = SetSessionSystemVar(v, TiDBStmtSummaryMaxSQLLength, types.NewStringDatum("a"))
	c.Assert(err, ErrorMatches, ".*Incorrect argument type to variable 'tidb_stmt_summary_max_sql_length'")

	err = SetSessionSystemVar(v, TiDBFoundInPlanCache, types.NewStringDatum("1"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBFoundInPlanCache)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.systems[TiDBFoundInPlanCache], Equals, "1")
}

func (s *testVarsutilSuite) TestSetOverflowBehave(c *C) {
	ddRegWorker := maxDDLReorgWorkerCount + 1
	SetDDLReorgWorkerCounter(ddRegWorker)
	c.Assert(maxDDLReorgWorkerCount, Equals, GetDDLReorgWorkerCounter())

	ddlReorgBatchSize := MaxDDLReorgBatchSize + 1
	SetDDLReorgBatchSize(ddlReorgBatchSize)
	c.Assert(MaxDDLReorgBatchSize, Equals, GetDDLReorgBatchSize())
	ddlReorgBatchSize = MinDDLReorgBatchSize - 1
	SetDDLReorgBatchSize(ddlReorgBatchSize)
	c.Assert(MinDDLReorgBatchSize, Equals, GetDDLReorgBatchSize())

	val := tidbOptInt64("a", 1)
	c.Assert(val, Equals, int64(1))
	val2 := tidbOptFloat64("b", 1.2)
	c.Assert(val2, Equals, 1.2)
}

func (s *testVarsutilSuite) TestValidate(c *C) {
	v := NewSessionVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor()
	v.TimeZone = time.UTC

	tests := []struct {
		key   string
		value string
		error bool
	}{
		{TiDBAutoAnalyzeStartTime, "15:04", false},
		{TiDBAutoAnalyzeStartTime, "15:04 -0700", false},
		{DelayKeyWrite, "ON", false},
		{DelayKeyWrite, "OFF", false},
		{DelayKeyWrite, "ALL", false},
		{DelayKeyWrite, "3", true},
		{ForeignKeyChecks, "3", true},
		{MaxSpRecursionDepth, "256", false},
		{SessionTrackGtids, "OFF", false},
		{SessionTrackGtids, "OWN_GTID", false},
		{SessionTrackGtids, "ALL_GTIDS", false},
		{SessionTrackGtids, "ON", true},
		{EnforceGtidConsistency, "OFF", false},
		{EnforceGtidConsistency, "ON", false},
		{EnforceGtidConsistency, "WARN", false},
		{QueryCacheType, "OFF", false},
		{QueryCacheType, "ON", false},
		{QueryCacheType, "DEMAND", false},
		{QueryCacheType, "3", true},
		{SecureAuth, "1", false},
		{SecureAuth, "3", true},
		{MyISAMUseMmap, "ON", false},
		{MyISAMUseMmap, "OFF", false},
		{TiDBEnableTablePartition, "ON", false},
		{TiDBEnableTablePartition, "OFF", false},
		{TiDBEnableTablePartition, "AUTO", false},
		{TiDBEnableTablePartition, "UN", true},
		{TiDBOptCorrelationExpFactor, "a", true},
		{TiDBOptCorrelationExpFactor, "-10", true},
		{TiDBOptCorrelationThreshold, "a", true},
		{TiDBOptCorrelationThreshold, "-2", true},
		{TiDBOptCPUFactor, "a", true},
		{TiDBOptCPUFactor, "-2", true},
		{TiDBOptTiFlashConcurrencyFactor, "-2", true},
		{TiDBOptCopCPUFactor, "a", true},
		{TiDBOptCopCPUFactor, "-2", true},
		{TiDBOptNetworkFactor, "a", true},
		{TiDBOptNetworkFactor, "-2", true},
		{TiDBOptScanFactor, "a", true},
		{TiDBOptScanFactor, "-2", true},
		{TiDBOptDescScanFactor, "a", true},
		{TiDBOptDescScanFactor, "-2", true},
		{TiDBOptSeekFactor, "a", true},
		{TiDBOptSeekFactor, "-2", true},
		{TiDBOptMemoryFactor, "a", true},
		{TiDBOptMemoryFactor, "-2", true},
		{TiDBOptDiskFactor, "a", true},
		{TiDBOptDiskFactor, "-2", true},
		{TiDBOptConcurrencyFactor, "a", true},
		{TiDBOptConcurrencyFactor, "-2", true},
		{TxnIsolation, "READ-UNCOMMITTED", true},
		{TiDBInitChunkSize, "a", true},
		{TiDBInitChunkSize, "-1", true},
		{TiDBMaxChunkSize, "a", true},
		{TiDBMaxChunkSize, "-1", true},
		{TiDBOptJoinReorderThreshold, "a", true},
		{TiDBOptJoinReorderThreshold, "-1", true},
		{TiDBReplicaRead, "invalid", true},
		{TiDBTxnMode, "invalid", true},
		{TiDBTxnMode, "pessimistic", false},
		{TiDBTxnMode, "optimistic", false},
		{TiDBTxnMode, "", false},
		{TiDBIsolationReadEngines, "", true},
		{TiDBIsolationReadEngines, "tikv", false},
		{TiDBIsolationReadEngines, "TiKV,tiflash", false},
		{TiDBIsolationReadEngines, "   tikv,   tiflash  ", false},
		{TiDBShardAllocateStep, "ad", true},
		{TiDBShardAllocateStep, "-123", false},
		{TiDBShardAllocateStep, "128", false},
		{TiDBEnableAmendPessimisticTxn, "0", false},
		{TiDBEnableAmendPessimisticTxn, "1", false},
		{TiDBEnableAmendPessimisticTxn, "256", true},
	}

	for _, t := range tests {
		_, err := ValidateSetSystemVar(v, t.key, t.value, ScopeGlobal)
		if t.error {
			c.Assert(err, NotNil, Commentf("%v got err=%v", t, err))
		} else {
			c.Assert(err, IsNil, Commentf("%v got err=%v", t, err))
		}
	}

}

func (s *testVarsutilSuite) TestValidateStmtSummary(c *C) {
	v := NewSessionVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor()
	v.TimeZone = time.UTC

	tests := []struct {
		key   string
		value string
		error bool
		scope ScopeFlag
	}{
		{TiDBEnableStmtSummary, "a", true, ScopeSession},
		{TiDBEnableStmtSummary, "-1", true, ScopeSession},
		{TiDBEnableStmtSummary, "", false, ScopeSession},
		{TiDBEnableStmtSummary, "", true, ScopeGlobal},
		{TiDBStmtSummaryInternalQuery, "a", true, ScopeSession},
		{TiDBStmtSummaryInternalQuery, "-1", true, ScopeSession},
		{TiDBStmtSummaryInternalQuery, "", false, ScopeSession},
		{TiDBStmtSummaryInternalQuery, "", true, ScopeGlobal},
		{TiDBStmtSummaryRefreshInterval, "a", true, ScopeSession},
		{TiDBStmtSummaryRefreshInterval, "", false, ScopeSession},
		{TiDBStmtSummaryRefreshInterval, "", true, ScopeGlobal},
		{TiDBStmtSummaryRefreshInterval, "0", true, ScopeGlobal},
		{TiDBStmtSummaryRefreshInterval, "99999999999", true, ScopeGlobal},
		{TiDBStmtSummaryHistorySize, "a", true, ScopeSession},
		{TiDBStmtSummaryHistorySize, "", false, ScopeSession},
		{TiDBStmtSummaryHistorySize, "", true, ScopeGlobal},
		{TiDBStmtSummaryHistorySize, "0", false, ScopeGlobal},
		{TiDBStmtSummaryHistorySize, "-1", true, ScopeGlobal},
		{TiDBStmtSummaryHistorySize, "99999999", true, ScopeGlobal},
		{TiDBStmtSummaryMaxStmtCount, "a", true, ScopeSession},
		{TiDBStmtSummaryMaxStmtCount, "", false, ScopeSession},
		{TiDBStmtSummaryMaxStmtCount, "", true, ScopeGlobal},
		{TiDBStmtSummaryMaxStmtCount, "0", true, ScopeGlobal},
		{TiDBStmtSummaryMaxStmtCount, "99999999", true, ScopeGlobal},
		{TiDBStmtSummaryMaxSQLLength, "a", true, ScopeSession},
		{TiDBStmtSummaryMaxSQLLength, "", false, ScopeSession},
		{TiDBStmtSummaryMaxSQLLength, "", true, ScopeGlobal},
		{TiDBStmtSummaryMaxSQLLength, "0", false, ScopeGlobal},
		{TiDBStmtSummaryMaxSQLLength, "-1", true, ScopeGlobal},
		{TiDBStmtSummaryMaxSQLLength, "99999999999", true, ScopeGlobal},
	}

	for _, t := range tests {
		_, err := ValidateSetSystemVar(v, t.key, t.value, t.scope)
		if t.error {
			c.Assert(err, NotNil, Commentf("%v got err=%v", t, err))
		} else {
			c.Assert(err, IsNil, Commentf("%v got err=%v", t, err))
		}
	}
}

func (s *testVarsutilSuite) TestConcurrencyVariables(c *C) {
	defer testleak.AfterTest(c)()
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor()

	wdConcurrency := 2
	c.Assert(vars.windowConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.WindowConcurrency(), Equals, DefExecutorConcurrency)
	err := SetSessionSystemVar(vars, TiDBWindowConcurrency, types.NewIntDatum(int64(wdConcurrency)))
	c.Assert(err, IsNil)
	c.Assert(vars.windowConcurrency, Equals, wdConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, wdConcurrency)

	c.Assert(vars.indexLookupConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.IndexLookupConcurrency(), Equals, DefExecutorConcurrency)
	exeConcurrency := DefExecutorConcurrency + 1
	err = SetSessionSystemVar(vars, TiDBExecutorConcurrency, types.NewIntDatum(int64(exeConcurrency)))
	c.Assert(err, IsNil)
	c.Assert(vars.indexLookupConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.IndexLookupConcurrency(), Equals, exeConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, wdConcurrency)
}
