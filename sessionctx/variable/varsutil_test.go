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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestTiDBOptOn(t *testing.T) {
	table := []struct {
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
	for _, tbl := range table {
		on := TiDBOptOn(tbl.val)
		require.Equal(t, tbl.on, on)
	}
}

func TestNewSessionVars(t *testing.T) {
	vars := NewSessionVars()

	require.Equal(t, DefIndexJoinBatchSize, vars.IndexJoinBatchSize)
	require.Equal(t, DefIndexLookupSize, vars.IndexLookupSize)
	require.Equal(t, ConcurrencyUnset, vars.indexLookupConcurrency)
	require.Equal(t, DefIndexSerialScanConcurrency, vars.indexSerialScanConcurrency)
	require.Equal(t, ConcurrencyUnset, vars.indexLookupJoinConcurrency)
	require.Equal(t, DefTiDBHashJoinConcurrency, vars.hashJoinConcurrency)
	require.Equal(t, DefExecutorConcurrency, vars.IndexLookupConcurrency())
	require.Equal(t, DefIndexSerialScanConcurrency, vars.IndexSerialScanConcurrency())
	require.Equal(t, DefExecutorConcurrency, vars.IndexLookupJoinConcurrency())
	require.Equal(t, DefExecutorConcurrency, vars.HashJoinConcurrency())
	require.Equal(t, DefTiDBAllowBatchCop, vars.AllowBatchCop)
	require.Equal(t, ConcurrencyUnset, vars.projectionConcurrency)
	require.Equal(t, ConcurrencyUnset, vars.hashAggPartialConcurrency)
	require.Equal(t, ConcurrencyUnset, vars.hashAggFinalConcurrency)
	require.Equal(t, ConcurrencyUnset, vars.windowConcurrency)
	require.Equal(t, DefTiDBMergeJoinConcurrency, vars.mergeJoinConcurrency)
	require.Equal(t, DefTiDBStreamAggConcurrency, vars.streamAggConcurrency)
	require.Equal(t, DefDistSQLScanConcurrency, vars.distSQLScanConcurrency)
	require.Equal(t, DefExecutorConcurrency, vars.ProjectionConcurrency())
	require.Equal(t, DefExecutorConcurrency, vars.HashAggPartialConcurrency())
	require.Equal(t, DefExecutorConcurrency, vars.HashAggFinalConcurrency())
	require.Equal(t, DefExecutorConcurrency, vars.WindowConcurrency())
	require.Equal(t, DefTiDBMergeJoinConcurrency, vars.MergeJoinConcurrency())
	require.Equal(t, DefTiDBStreamAggConcurrency, vars.StreamAggConcurrency())
	require.Equal(t, DefDistSQLScanConcurrency, vars.DistSQLScanConcurrency())
	require.Equal(t, DefExecutorConcurrency, vars.ExecutorConcurrency)
	require.Equal(t, DefMaxChunkSize, vars.MaxChunkSize)
	require.Equal(t, DefDMLBatchSize, vars.DMLBatchSize)
	require.Equal(t, int64(DefTiDBMemQuotaApplyCache), vars.MemQuotaApplyCache)
	require.Equal(t, DefOptWriteRowID, vars.AllowWriteRowID)
	require.Equal(t, DefTiDBOptJoinReorderThreshold, vars.TiDBOptJoinReorderThreshold)
	require.Equal(t, DefTiDBUseFastAnalyze, vars.EnableFastAnalyze)
	require.Equal(t, DefTiDBFoundInPlanCache, vars.FoundInPlanCache)
	require.Equal(t, DefTiDBFoundInBinding, vars.FoundInBinding)
	require.Equal(t, DefTiDBAllowAutoRandExplicitInsert, vars.AllowAutoRandExplicitInsert)
	require.Equal(t, int64(DefTiDBShardAllocateStep), vars.ShardAllocateStep)
	require.Equal(t, DefTiDBAnalyzeVersion, vars.AnalyzeVersion)
	require.Equal(t, DefCTEMaxRecursionDepth, vars.CTEMaxRecursionDepth)
	require.Equal(t, int64(DefTiDBTmpTableMaxSize), vars.TMPTableSize)

	assertFieldsGreaterThanZero(t, reflect.ValueOf(vars.MemQuota))
	assertFieldsGreaterThanZero(t, reflect.ValueOf(vars.BatchSize))
}

func assertFieldsGreaterThanZero(t *testing.T, val reflect.Value) {
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		require.Greater(t, fieldVal.Int(), int64(0))
	}
}

func TestVarsutil(t *testing.T) {
	v := NewSessionVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()

	err := SetSessionSystemVar(v, "autocommit", "1")
	require.NoError(t, err)
	val, err := GetSessionOrGlobalSystemVar(v, "autocommit")
	require.NoError(t, err)
	require.Equal(t, "ON", val)
	require.NotNil(t, SetSessionSystemVar(v, "autocommit", ""))

	// 0 converts to OFF
	err = SetSessionSystemVar(v, "foreign_key_checks", "0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, "foreign_key_checks")
	require.NoError(t, err)
	require.Equal(t, "OFF", val)

	// 1/ON is not supported (generates a warning and sets to OFF)
	err = SetSessionSystemVar(v, "foreign_key_checks", "1")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, "foreign_key_checks")
	require.NoError(t, err)
	require.Equal(t, "OFF", val)

	err = SetSessionSystemVar(v, "sql_mode", "strict_trans_tables")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, "sql_mode")
	require.NoError(t, err)
	require.Equal(t, "STRICT_TRANS_TABLES", val)
	require.True(t, v.StrictSQLMode)
	err = SetSessionSystemVar(v, "sql_mode", "")
	require.NoError(t, err)
	require.False(t, v.StrictSQLMode)

	err = SetSessionSystemVar(v, "character_set_connection", "utf8")
	require.NoError(t, err)
	err = SetSessionSystemVar(v, "collation_connection", "utf8_general_ci")
	require.NoError(t, err)
	charset, collation := v.GetCharsetInfo()
	require.Equal(t, "utf8", charset)
	require.Equal(t, "utf8_general_ci", collation)

	require.Nil(t, SetSessionSystemVar(v, "character_set_results", ""))

	// Test case for time_zone session variable.
	testCases := []struct {
		input        string
		expect       string
		compareValue bool
		diff         time.Duration
		err          error
	}{
		{"Europe/Helsinki", "Europe/Helsinki", true, -2 * time.Hour, nil},
		{"US/Eastern", "US/Eastern", true, 5 * time.Hour, nil},
		// TODO: Check it out and reopen this case.
		// {"SYSTEM", "Local", false, 0},
		{"+10:00", "", true, -10 * time.Hour, nil},
		{"-6:00", "", true, 6 * time.Hour, nil},
		{"+14:00", "", true, -14 * time.Hour, nil},
		{"-12:59", "", true, 12*time.Hour + 59*time.Minute, nil},
		{"+14:01", "", false, -14 * time.Hour, ErrUnknownTimeZone.GenWithStackByArgs("+14:01")},
		{"-13:00", "", false, 13 * time.Hour, ErrUnknownTimeZone.GenWithStackByArgs("-13:00")},
	}
	for _, tc := range testCases {
		err = SetSessionSystemVar(v, TimeZone, tc.input)
		if tc.err != nil {
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)
		require.Equal(t, tc.expect, v.TimeZone.String())
		if tc.compareValue {
			err = SetSessionSystemVar(v, TimeZone, tc.input)
			require.NoError(t, err)
			t1 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			t2 := time.Date(2000, 1, 1, 0, 0, 0, 0, v.TimeZone)
			require.Equal(t, tc.diff, t2.Sub(t1))
		}
	}
	err = SetSessionSystemVar(v, TimeZone, "6:00")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, ErrUnknownTimeZone))

	// Test case for sql mode.
	for str, mode := range mysql.Str2SQLMode {
		err = SetSessionSystemVar(v, "sql_mode", str)
		require.NoError(t, err)
		if modeParts, exists := mysql.CombinationSQLMode[str]; exists {
			for _, part := range modeParts {
				mode |= mysql.Str2SQLMode[part]
			}
		}
		require.Equal(t, mode, v.SQLMode)
	}

	// Combined sql_mode
	err = SetSessionSystemVar(v, "sql_mode", "REAL_AS_FLOAT,ANSI_QUOTES")
	require.NoError(t, err)
	require.Equal(t, mysql.ModeRealAsFloat|mysql.ModeANSIQuotes, v.SQLMode)

	// Test case for tidb_index_serial_scan_concurrency.
	require.Equal(t, DefIndexSerialScanConcurrency, v.IndexSerialScanConcurrency())
	err = SetSessionSystemVar(v, TiDBIndexSerialScanConcurrency, "4")
	require.NoError(t, err)
	require.Equal(t, 4, v.IndexSerialScanConcurrency())

	// Test case for tidb_batch_insert.
	require.False(t, v.BatchInsert)
	err = SetSessionSystemVar(v, TiDBBatchInsert, "1")
	require.NoError(t, err)
	require.True(t, v.BatchInsert)

	require.Equal(t, 32, v.InitChunkSize)
	require.Equal(t, 1024, v.MaxChunkSize)
	err = SetSessionSystemVar(v, TiDBMaxChunkSize, "2")
	require.NoError(t, err) // converts to min value
	err = SetSessionSystemVar(v, TiDBInitChunkSize, "1024")
	require.NoError(t, err) // converts to max value

	// Test case for TiDBConfig session variable.
	err = SetSessionSystemVar(v, TiDBConfig, "abc")
	require.True(t, terror.ErrorEqual(err, ErrIncorrectScope))
	val, err = GetSessionOrGlobalSystemVar(v, TiDBConfig)
	require.NoError(t, err)
	bVal, err := json.MarshalIndent(config.GetGlobalConfig(), "", "\t")
	require.NoError(t, err)
	require.Equal(t, config.HideConfig(string(bVal)), val)

	require.Equal(t, DefTiDBOptimizerSelectivityLevel, v.OptimizerSelectivityLevel)
	err = SetSessionSystemVar(v, TiDBOptimizerSelectivityLevel, "1")
	require.NoError(t, err)
	require.Equal(t, 1, v.OptimizerSelectivityLevel)

	require.Equal(t, DefTiDBEnableOuterJoinReorder, v.EnableOuterJoinReorder)
	err = SetSessionSystemVar(v, TiDBOptimizerEnableOuterJoinReorder, "OFF")
	require.NoError(t, err)
	require.Equal(t, false, v.EnableOuterJoinReorder)

	require.Equal(t, DefTiDBOptimizerEnableNewOFGB, v.OptimizerEnableNewOnlyFullGroupByCheck)
	err = SetSessionSystemVar(v, TiDBOptimizerEnableNewOnlyFullGroupByCheck, "off")
	require.NoError(t, err)
	require.Equal(t, false, v.OptimizerEnableNewOnlyFullGroupByCheck)

	err = SetSessionSystemVar(v, TiDBDDLReorgWorkerCount, "4") // wrong scope global only
	require.True(t, terror.ErrorEqual(err, errGlobalVariable))

	err = SetSessionSystemVar(v, TiDBRetryLimit, "3")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBRetryLimit)
	require.NoError(t, err)
	require.Equal(t, "3", val)
	require.Equal(t, int64(3), v.RetryLimit)

	require.Equal(t, "", v.EnableTablePartition)
	err = SetSessionSystemVar(v, TiDBEnableTablePartition, "on")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBEnableTablePartition)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
	require.Equal(t, "ON", v.EnableTablePartition)

	require.False(t, v.EnableListTablePartition)
	err = SetSessionSystemVar(v, TiDBEnableListTablePartition, "on")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBEnableListTablePartition)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
	require.True(t, v.EnableListTablePartition)

	require.Equal(t, DefTiDBOptJoinReorderThreshold, v.TiDBOptJoinReorderThreshold)
	err = SetSessionSystemVar(v, TiDBOptJoinReorderThreshold, "5")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptJoinReorderThreshold)
	require.NoError(t, err)
	require.Equal(t, "5", val)
	require.Equal(t, 5, v.TiDBOptJoinReorderThreshold)

	err = SetSessionSystemVar(v, TiDBLowResolutionTSO, "1")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBLowResolutionTSO)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
	require.True(t, v.LowResolutionTSO)
	err = SetSessionSystemVar(v, TiDBLowResolutionTSO, "0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBLowResolutionTSO)
	require.NoError(t, err)
	require.Equal(t, "OFF", val)
	require.False(t, v.LowResolutionTSO)

	require.Equal(t, 0.9, v.CorrelationThreshold)
	err = SetSessionSystemVar(v, TiDBOptCorrelationThreshold, "0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptCorrelationThreshold)
	require.NoError(t, err)
	require.Equal(t, "0", val)
	require.Equal(t, float64(0), v.CorrelationThreshold)

	require.Equal(t, 3.0, v.CPUFactor)
	err = SetSessionSystemVar(v, TiDBOptCPUFactor, "5.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptCPUFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.CPUFactor)

	require.Equal(t, 3.0, v.CopCPUFactor)
	err = SetSessionSystemVar(v, TiDBOptCopCPUFactor, "5.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptCopCPUFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.CopCPUFactor)

	require.Equal(t, 24.0, v.CopTiFlashConcurrencyFactor)
	err = SetSessionSystemVar(v, TiDBOptTiFlashConcurrencyFactor, "5.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptTiFlashConcurrencyFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.CopCPUFactor)

	require.Equal(t, 1.0, v.GetNetworkFactor(nil))
	err = SetSessionSystemVar(v, TiDBOptNetworkFactor, "3.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptNetworkFactor)
	require.NoError(t, err)
	require.Equal(t, "3.0", val)
	require.Equal(t, 3.0, v.GetNetworkFactor(nil))

	require.Equal(t, 1.5, v.GetScanFactor(nil))
	err = SetSessionSystemVar(v, TiDBOptScanFactor, "3.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptScanFactor)
	require.NoError(t, err)
	require.Equal(t, "3.0", val)
	require.Equal(t, 3.0, v.GetScanFactor(nil))

	require.Equal(t, 3.0, v.GetDescScanFactor(nil))
	err = SetSessionSystemVar(v, TiDBOptDescScanFactor, "5.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptDescScanFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.GetDescScanFactor(nil))

	require.Equal(t, 20.0, v.GetSeekFactor(nil))
	err = SetSessionSystemVar(v, TiDBOptSeekFactor, "50.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptSeekFactor)
	require.NoError(t, err)
	require.Equal(t, "50.0", val)
	require.Equal(t, 50.0, v.GetSeekFactor(nil))

	require.Equal(t, 0.001, v.MemoryFactor)
	err = SetSessionSystemVar(v, TiDBOptMemoryFactor, "1.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptMemoryFactor)
	require.NoError(t, err)
	require.Equal(t, "1.0", val)
	require.Equal(t, 1.0, v.MemoryFactor)

	require.Equal(t, 1.5, v.DiskFactor)
	err = SetSessionSystemVar(v, TiDBOptDiskFactor, "1.1")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptDiskFactor)
	require.NoError(t, err)
	require.Equal(t, "1.1", val)
	require.Equal(t, 1.1, v.DiskFactor)

	require.Equal(t, 3.0, v.ConcurrencyFactor)
	err = SetSessionSystemVar(v, TiDBOptConcurrencyFactor, "5.0")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBOptConcurrencyFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.ConcurrencyFactor)

	err = SetSessionSystemVar(v, TiDBReplicaRead, "follower")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "follower", val)
	require.Equal(t, kv.ReplicaReadFollower, v.GetReplicaRead())
	err = SetSessionSystemVar(v, TiDBReplicaRead, "leader")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "leader", val)
	require.Equal(t, kv.ReplicaReadLeader, v.GetReplicaRead())
	err = SetSessionSystemVar(v, TiDBReplicaRead, "leader-and-follower")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "leader-and-follower", val)
	require.Equal(t, kv.ReplicaReadMixed, v.GetReplicaRead())

	err = SetSessionSystemVar(v, TiDBRedactLog, "ON")
	require.NoError(t, err)
	val, err = GetSessionOrGlobalSystemVar(v, TiDBRedactLog)
	require.NoError(t, err)
	require.Equal(t, "ON", val)

	err = SetSessionSystemVar(v, TiDBFoundInPlanCache, "1")
	require.Error(t, err)
	require.Regexp(t, "]Variable 'last_plan_from_cache' is a read only variable$", err.Error())

	err = SetSessionSystemVar(v, TiDBFoundInBinding, "1")
	require.Error(t, err)
	require.Regexp(t, "]Variable 'last_plan_from_binding' is a read only variable$", err.Error())

	err = SetSessionSystemVar(v, "UnknownVariable", "on")
	require.Error(t, err)
	require.Regexp(t, "]Unknown system variable 'UnknownVariable'$", err.Error())

	// reset warnings
	v.StmtCtx.TruncateWarnings(0)
	require.Len(t, v.StmtCtx.GetWarnings(), 0)

	err = SetSessionSystemVar(v, TiDBAnalyzeVersion, "4")
	require.NoError(t, err) // converts to max value
	warn := v.StmtCtx.GetWarnings()[0]
	require.Error(t, warn.Err)
	require.Contains(t, warn.Err.Error(), "Truncated incorrect tidb_analyze_version value")

	err = SetSessionSystemVar(v, TiDBTableCacheLease, "123")
	require.Error(t, err)
	require.Regexp(t, "'tidb_table_cache_lease' is a GLOBAL variable and should be set with SET GLOBAL", err.Error())
}

func TestValidate(t *testing.T) {
	v := NewSessionVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	v.TimeZone = time.UTC

	testCases := []struct {
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
		{TiDBEnableListTablePartition, "ON", false},
		{TiDBEnableListTablePartition, "OFF", false},
		{TiDBEnableListTablePartition, "list", true},
		{TiDBOptCorrelationExpFactor, "a", true},
		{TiDBOptCorrelationExpFactor, "-10", false},
		{TiDBOptCorrelationThreshold, "a", true},
		{TiDBOptCorrelationThreshold, "-2", false},
		{TiDBOptCPUFactor, "a", true},
		{TiDBOptCPUFactor, "-2", false},
		{TiDBOptTiFlashConcurrencyFactor, "-2", false},
		{TiDBOptCopCPUFactor, "a", true},
		{TiDBOptCopCPUFactor, "-2", false},
		{TiDBOptNetworkFactor, "a", true},
		{TiDBOptNetworkFactor, "-2", false},
		{TiDBOptScanFactor, "a", true},
		{TiDBOptScanFactor, "-2", false},
		{TiDBOptDescScanFactor, "a", true},
		{TiDBOptDescScanFactor, "-2", false},
		{TiDBOptSeekFactor, "a", true},
		{TiDBOptSeekFactor, "-2", false},
		{TiDBOptMemoryFactor, "a", true},
		{TiDBOptMemoryFactor, "-2", false},
		{TiDBOptDiskFactor, "a", true},
		{TiDBOptDiskFactor, "-2", false},
		{TiDBOptConcurrencyFactor, "a", true},
		{TiDBOptConcurrencyFactor, "-2", false},
		{TxnIsolation, "READ-UNCOMMITTED", true},
		{TiDBInitChunkSize, "a", true},
		{TiDBInitChunkSize, "-1", false},
		{TiDBMaxChunkSize, "a", true},
		{TiDBMaxChunkSize, "-1", false},
		{TiDBOptJoinReorderThreshold, "a", true},
		{TiDBOptJoinReorderThreshold, "-1", false},
		{TiDBReplicaRead, "invalid", true},
		{TiDBTxnMode, "invalid", true},
		{TiDBTxnMode, "pessimistic", false},
		{TiDBTxnMode, "optimistic", false},
		{TiDBTxnMode, "", false},
		{TiDBShardAllocateStep, "ad", true},
		{TiDBShardAllocateStep, "-123", false},
		{TiDBShardAllocateStep, "128", false},
		{TiDBEnableAmendPessimisticTxn, "0", false},
		{TiDBEnableAmendPessimisticTxn, "1", false},
		{TiDBEnableAmendPessimisticTxn, "256", true},
		{TiDBAllowFallbackToTiKV, "", false},
		{TiDBAllowFallbackToTiKV, "tiflash", false},
		{TiDBAllowFallbackToTiKV, "  tiflash  ", false},
		{TiDBAllowFallbackToTiKV, "tikv", true},
		{TiDBAllowFallbackToTiKV, "tidb", true},
		{TiDBAllowFallbackToTiKV, "tiflash,tikv,tidb", true},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			_, err := GetSysVar(tc.key).Validate(v, tc.value, ScopeGlobal)
			if tc.error {
				require.Errorf(t, err, "%v got err=%v", tc, err)
			} else {
				require.NoErrorf(t, err, "%v got err=%v", tc, err)
			}
		})
	}

	// Test session scoped vars.
	testCases = []struct {
		key   string
		value string
		error bool
	}{
		{TiDBEnableListTablePartition, "ON", false},
		{TiDBEnableListTablePartition, "OFF", false},
		{TiDBEnableListTablePartition, "list", true},
		{TiDBIsolationReadEngines, "", true},
		{TiDBIsolationReadEngines, "tikv", false},
		{TiDBIsolationReadEngines, "TiKV,tiflash", false},
		{TiDBIsolationReadEngines, "   tikv,   tiflash  ", false},
	}

	for _, tc := range testCases {
		// copy iterator variable into a new variable, see issue #27779
		tc := tc
		t.Run(tc.key, func(t *testing.T) {
			_, err := GetSysVar(tc.key).Validate(v, tc.value, ScopeSession)
			if tc.error {
				require.Errorf(t, err, "%v got err=%v", tc, err)
			} else {
				require.NoErrorf(t, err, "%v got err=%v", tc, err)
			}
		})
	}

}

func TestValidateStmtSummary(t *testing.T) {
	v := NewSessionVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	v.TimeZone = time.UTC

	testCases := []struct {
		key   string
		value string
		error bool
	}{
		{TiDBEnableStmtSummary, "", true},
		{TiDBStmtSummaryInternalQuery, "", true},
		{TiDBStmtSummaryRefreshInterval, "", true},
		{TiDBStmtSummaryRefreshInterval, "0", false},
		{TiDBStmtSummaryRefreshInterval, "99999999999", false},
		{TiDBStmtSummaryHistorySize, "", true},
		{TiDBStmtSummaryHistorySize, "0", false},
		{TiDBStmtSummaryHistorySize, "-1", false},
		{TiDBStmtSummaryHistorySize, "99999999", false},
		{TiDBStmtSummaryMaxStmtCount, "", true},
		{TiDBStmtSummaryMaxStmtCount, "0", false},
		{TiDBStmtSummaryMaxStmtCount, "99999999", false},
		{TiDBStmtSummaryMaxSQLLength, "", true},
		{TiDBStmtSummaryMaxSQLLength, "0", false},
		{TiDBStmtSummaryMaxSQLLength, "-1", false},
		{TiDBStmtSummaryMaxSQLLength, "99999999999", false},
	}

	for _, tc := range testCases {
		// copy iterator variable into a new variable, see issue #27779
		tc := tc
		t.Run(tc.key, func(t *testing.T) {
			_, err := GetSysVar(tc.key).Validate(v, tc.value, ScopeGlobal)
			if tc.error {
				require.Errorf(t, err, "%v got err=%v", tc, err)
			} else {
				require.NoErrorf(t, err, "%v got err=%v", tc, err)
			}
		})
	}
}

func TestConcurrencyVariables(t *testing.T) {
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()

	wdConcurrency := 2
	require.Equal(t, ConcurrencyUnset, vars.windowConcurrency)
	require.Equal(t, DefExecutorConcurrency, vars.WindowConcurrency())
	err := SetSessionSystemVar(vars, TiDBWindowConcurrency, strconv.Itoa(wdConcurrency))
	require.NoError(t, err)
	require.Equal(t, wdConcurrency, vars.windowConcurrency)
	require.Equal(t, wdConcurrency, vars.WindowConcurrency())

	mjConcurrency := 2
	require.Equal(t, DefTiDBMergeJoinConcurrency, vars.mergeJoinConcurrency)
	require.Equal(t, DefTiDBMergeJoinConcurrency, vars.MergeJoinConcurrency())
	err = SetSessionSystemVar(vars, TiDBMergeJoinConcurrency, strconv.Itoa(mjConcurrency))
	require.NoError(t, err)
	require.Equal(t, mjConcurrency, vars.mergeJoinConcurrency)
	require.Equal(t, mjConcurrency, vars.MergeJoinConcurrency())

	saConcurrency := 2
	require.Equal(t, DefTiDBStreamAggConcurrency, vars.streamAggConcurrency)
	require.Equal(t, DefTiDBStreamAggConcurrency, vars.StreamAggConcurrency())
	err = SetSessionSystemVar(vars, TiDBStreamAggConcurrency, strconv.Itoa(saConcurrency))
	require.NoError(t, err)
	require.Equal(t, saConcurrency, vars.streamAggConcurrency)
	require.Equal(t, saConcurrency, vars.StreamAggConcurrency())

	require.Equal(t, ConcurrencyUnset, vars.indexLookupConcurrency)
	require.Equal(t, DefExecutorConcurrency, vars.IndexLookupConcurrency())
	exeConcurrency := DefExecutorConcurrency + 1
	err = SetSessionSystemVar(vars, TiDBExecutorConcurrency, strconv.Itoa(exeConcurrency))
	require.NoError(t, err)
	require.Equal(t, ConcurrencyUnset, vars.indexLookupConcurrency)
	require.Equal(t, exeConcurrency, vars.IndexLookupConcurrency())
	require.Equal(t, wdConcurrency, vars.WindowConcurrency())
	require.Equal(t, mjConcurrency, vars.MergeJoinConcurrency())
	require.Equal(t, saConcurrency, vars.StreamAggConcurrency())

}

func TestHelperFuncs(t *testing.T) {
	require.Equal(t, "ON", int32ToBoolStr(1))
	require.Equal(t, "OFF", int32ToBoolStr(0))

	require.Equal(t, ClusteredIndexDefModeOn, TiDBOptEnableClustered("ON"))
	require.Equal(t, ClusteredIndexDefModeOff, TiDBOptEnableClustered("OFF"))
	require.Equal(t, ClusteredIndexDefModeIntOnly, TiDBOptEnableClustered("bogus")) // default

	require.Equal(t, 1234, tidbOptPositiveInt32("1234", 5))
	require.Equal(t, 5, tidbOptPositiveInt32("-1234", 5))
	require.Equal(t, 5, tidbOptPositiveInt32("bogus", 5))

	require.Equal(t, 1234, TidbOptInt("1234", 5))
	require.Equal(t, -1234, TidbOptInt("-1234", 5))
	require.Equal(t, 5, TidbOptInt("bogus", 5))
}

func TestStmtVars(t *testing.T) {
	vars := NewSessionVars()
	err := SetStmtVar(vars, "bogussysvar", "1")
	require.Equal(t, "[variable:1193]Unknown system variable 'bogussysvar'", err.Error())
	err = SetStmtVar(vars, MaxExecutionTime, "ACDC")
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'max_execution_time'", err.Error())
	err = SetStmtVar(vars, MaxExecutionTime, "100")
	require.NoError(t, err)
}
