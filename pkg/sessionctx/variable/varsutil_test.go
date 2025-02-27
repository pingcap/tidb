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
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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
	vars := NewSessionVars(nil)

	require.Equal(t, vardef.DefIndexJoinBatchSize, vars.IndexJoinBatchSize)
	require.Equal(t, vardef.DefIndexLookupSize, vars.IndexLookupSize)
	require.Equal(t, vardef.ConcurrencyUnset, vars.indexLookupConcurrency)
	require.Equal(t, vardef.DefIndexSerialScanConcurrency, vars.indexSerialScanConcurrency)
	require.Equal(t, vardef.ConcurrencyUnset, vars.indexLookupJoinConcurrency)
	require.Equal(t, vardef.DefTiDBHashJoinConcurrency, vars.hashJoinConcurrency)
	require.Equal(t, vardef.DefExecutorConcurrency, vars.IndexLookupConcurrency())
	require.Equal(t, vardef.DefIndexSerialScanConcurrency, vars.IndexSerialScanConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.IndexLookupJoinConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashJoinConcurrency())
	require.Equal(t, vardef.DefTiDBAllowBatchCop, vars.AllowBatchCop)
	require.Equal(t, vardef.ConcurrencyUnset, vars.projectionConcurrency)
	require.Equal(t, vardef.ConcurrencyUnset, vars.hashAggPartialConcurrency)
	require.Equal(t, vardef.ConcurrencyUnset, vars.hashAggFinalConcurrency)
	require.Equal(t, vardef.ConcurrencyUnset, vars.windowConcurrency)
	require.Equal(t, vardef.DefTiDBMergeJoinConcurrency, vars.mergeJoinConcurrency)
	require.Equal(t, vardef.DefTiDBStreamAggConcurrency, vars.streamAggConcurrency)
	require.Equal(t, vardef.DefDistSQLScanConcurrency, vars.distSQLScanConcurrency)
	require.Equal(t, vardef.DefExecutorConcurrency, vars.ProjectionConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashAggPartialConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashAggFinalConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.WindowConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.IndexMergeIntersectionConcurrency())
	require.Equal(t, vardef.DefTiDBMergeJoinConcurrency, vars.MergeJoinConcurrency())
	require.Equal(t, vardef.DefTiDBStreamAggConcurrency, vars.StreamAggConcurrency())
	require.Equal(t, vardef.DefDistSQLScanConcurrency, vars.DistSQLScanConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.ExecutorConcurrency)
	require.Equal(t, vardef.DefMaxChunkSize, vars.MaxChunkSize)
	require.Equal(t, vardef.DefDMLBatchSize, vars.DMLBatchSize)
	require.Equal(t, int64(vardef.DefTiDBMemQuotaApplyCache), vars.MemQuotaApplyCache)
	require.Equal(t, vardef.DefOptWriteRowID, vars.AllowWriteRowID)
	require.Equal(t, vardef.DefTiDBOptJoinReorderThreshold, vars.TiDBOptJoinReorderThreshold)
	require.Equal(t, vardef.DefTiDBUseFastAnalyze, vars.EnableFastAnalyze)
	require.Equal(t, vardef.DefTiDBFoundInPlanCache, vars.FoundInPlanCache)
	require.Equal(t, vardef.DefTiDBFoundInBinding, vars.FoundInBinding)
	require.Equal(t, vardef.DefTiDBAllowAutoRandExplicitInsert, vars.AllowAutoRandExplicitInsert)
	require.Equal(t, int64(vardef.DefTiDBShardAllocateStep), vars.ShardAllocateStep)
	require.Equal(t, vardef.DefTiDBAnalyzeVersion, vars.AnalyzeVersion)
	require.Equal(t, vardef.DefCTEMaxRecursionDepth, vars.CTEMaxRecursionDepth)
	require.Equal(t, int64(vardef.DefTiDBTmpTableMaxSize), vars.TMPTableSize)

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
	v := NewSessionVars(nil)
	v.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()

	err := v.SetSystemVar("autocommit", "1")
	require.NoError(t, err)
	val, err := v.GetSessionOrGlobalSystemVar(context.Background(), "autocommit")
	require.NoError(t, err)
	require.Equal(t, "ON", val)
	require.NotNil(t, v.SetSystemVar("autocommit", ""))

	// 0 converts to OFF
	err = v.SetSystemVar("foreign_key_checks", "0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), "foreign_key_checks")
	require.NoError(t, err)
	require.Equal(t, "OFF", val)

	err = v.SetSystemVar("foreign_key_checks", "1")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), "foreign_key_checks")
	require.NoError(t, err)
	require.Equal(t, "ON", val)

	err = v.SetSystemVar("sql_mode", "strict_trans_tables")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), "sql_mode")
	require.NoError(t, err)
	require.Equal(t, "STRICT_TRANS_TABLES", val)
	require.True(t, v.SQLMode.HasStrictMode())
	err = v.SetSystemVar("sql_mode", "")
	require.NoError(t, err)
	require.False(t, v.SQLMode.HasStrictMode())

	err = v.SetSystemVar("character_set_connection", "utf8")
	require.NoError(t, err)
	err = v.SetSystemVar("collation_connection", "utf8_general_ci")
	require.NoError(t, err)
	charset, collation := v.GetCharsetInfo()
	require.Equal(t, "utf8", charset)
	require.Equal(t, "utf8_general_ci", collation)

	require.Nil(t, v.SetSystemVar("character_set_results", ""))

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
		err = v.SetSystemVar(vardef.TimeZone, tc.input)
		if tc.err != nil {
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)
		require.Equal(t, tc.expect, v.TimeZone.String())
		if tc.compareValue {
			err = v.SetSystemVar(vardef.TimeZone, tc.input)
			require.NoError(t, err)
			t1 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			t2 := time.Date(2000, 1, 1, 0, 0, 0, 0, v.TimeZone)
			require.Equal(t, tc.diff, t2.Sub(t1))
		}
	}
	err = v.SetSystemVar(vardef.TimeZone, "6:00")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, ErrUnknownTimeZone))

	// Test case for sql mode.
	for str, mode := range mysql.Str2SQLMode {
		err = v.SetSystemVar("sql_mode", str)
		require.NoError(t, err)
		if modeParts, exists := mysql.CombinationSQLMode[str]; exists {
			for _, part := range modeParts {
				mode |= mysql.Str2SQLMode[part]
			}
		}
		require.Equal(t, mode, v.SQLMode)
	}

	// Combined sql_mode
	err = v.SetSystemVar("sql_mode", "REAL_AS_FLOAT,ANSI_QUOTES")
	require.NoError(t, err)
	require.Equal(t, mysql.ModeRealAsFloat|mysql.ModeANSIQuotes, v.SQLMode)

	// Test case for tidb_index_serial_scan_concurrency.
	require.Equal(t, vardef.DefIndexSerialScanConcurrency, v.IndexSerialScanConcurrency())
	err = v.SetSystemVar(vardef.TiDBIndexSerialScanConcurrency, "4")
	require.NoError(t, err)
	require.Equal(t, 4, v.IndexSerialScanConcurrency())

	// Test case for tidb_batch_insert.
	require.False(t, v.BatchInsert)
	err = v.SetSystemVar(vardef.TiDBBatchInsert, "1")
	require.NoError(t, err)
	require.True(t, v.BatchInsert)

	require.Equal(t, 32, v.InitChunkSize)
	require.Equal(t, 1024, v.MaxChunkSize)
	err = v.SetSystemVar(vardef.TiDBMaxChunkSize, "2")
	require.NoError(t, err) // converts to min value
	err = v.SetSystemVar(vardef.TiDBInitChunkSize, "1024")
	require.NoError(t, err) // converts to max value

	// Test case for TiDBConfig session variable.
	err = v.SetSystemVar(vardef.TiDBConfig, "abc")
	require.True(t, terror.ErrorEqual(err, ErrIncorrectScope))
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBConfig)
	require.NoError(t, err)
	jsonConfig, err := config.GetJSONConfig()
	require.NoError(t, err)
	require.Equal(t, jsonConfig, val)

	require.Equal(t, vardef.DefTiDBOptimizerSelectivityLevel, v.OptimizerSelectivityLevel)
	err = v.SetSystemVar(vardef.TiDBOptimizerSelectivityLevel, "1")
	require.NoError(t, err)
	require.Equal(t, 1, v.OptimizerSelectivityLevel)

	require.Equal(t, vardef.DefTiDBEnableOuterJoinReorder, v.EnableOuterJoinReorder)
	err = v.SetSystemVar(vardef.TiDBOptimizerEnableOuterJoinReorder, "OFF")
	require.NoError(t, err)
	require.Equal(t, false, v.EnableOuterJoinReorder)
	err = v.SetSystemVar(vardef.TiDBOptimizerEnableOuterJoinReorder, "ON")
	require.NoError(t, err)
	require.Equal(t, true, v.EnableOuterJoinReorder)

	require.Equal(t, vardef.DefTiDBOptimizerEnableNewOFGB, v.OptimizerEnableNewOnlyFullGroupByCheck)
	err = v.SetSystemVar(vardef.TiDBOptimizerEnableNewOnlyFullGroupByCheck, "off")
	require.NoError(t, err)
	require.Equal(t, false, v.OptimizerEnableNewOnlyFullGroupByCheck)

	err = v.SetSystemVar(vardef.TiDBRetryLimit, "3")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBRetryLimit)
	require.NoError(t, err)
	require.Equal(t, "3", val)
	require.Equal(t, int64(3), v.RetryLimit)

	require.Equal(t, vardef.DefTiDBOptJoinReorderThreshold, v.TiDBOptJoinReorderThreshold)
	err = v.SetSystemVar(vardef.TiDBOptJoinReorderThreshold, "5")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptJoinReorderThreshold)
	require.NoError(t, err)
	require.Equal(t, "5", val)
	require.Equal(t, 5, v.TiDBOptJoinReorderThreshold)

	err = v.SetSystemVar(vardef.TiDBLowResolutionTSO, "1")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBLowResolutionTSO)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
	require.True(t, v.lowResolutionTSO)
	err = v.SetSystemVar(vardef.TiDBLowResolutionTSO, "0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBLowResolutionTSO)
	require.NoError(t, err)
	require.Equal(t, "OFF", val)
	require.False(t, v.lowResolutionTSO)

	require.Equal(t, 0.9, v.CorrelationThreshold)
	err = v.SetSystemVar(vardef.TiDBOptCorrelationThreshold, "0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptCorrelationThreshold)
	require.NoError(t, err)
	require.Equal(t, "0", val)
	require.Equal(t, float64(0), v.CorrelationThreshold)

	require.Equal(t, 3.0, v.GetCPUFactor())
	err = v.SetSystemVar(vardef.TiDBOptCPUFactor, "5.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptCPUFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.GetCPUFactor())

	require.Equal(t, 3.0, v.GetCopCPUFactor())
	err = v.SetSystemVar(vardef.TiDBOptCopCPUFactor, "5.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptCopCPUFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.GetCopCPUFactor())

	require.Equal(t, 24.0, v.CopTiFlashConcurrencyFactor)
	err = v.SetSystemVar(vardef.TiDBOptTiFlashConcurrencyFactor, "5.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptTiFlashConcurrencyFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.GetCopCPUFactor())

	require.Equal(t, 1.0, v.GetNetworkFactor(nil))
	err = v.SetSystemVar(vardef.TiDBOptNetworkFactor, "3.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptNetworkFactor)
	require.NoError(t, err)
	require.Equal(t, "3.0", val)
	require.Equal(t, 3.0, v.GetNetworkFactor(nil))

	require.Equal(t, 1.5, v.GetScanFactor(nil))
	err = v.SetSystemVar(vardef.TiDBOptScanFactor, "3.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptScanFactor)
	require.NoError(t, err)
	require.Equal(t, "3.0", val)
	require.Equal(t, 3.0, v.GetScanFactor(nil))

	require.Equal(t, 3.0, v.GetDescScanFactor(nil))
	err = v.SetSystemVar(vardef.TiDBOptDescScanFactor, "5.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptDescScanFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.GetDescScanFactor(nil))

	require.Equal(t, 20.0, v.GetSeekFactor(nil))
	err = v.SetSystemVar(vardef.TiDBOptSeekFactor, "50.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptSeekFactor)
	require.NoError(t, err)
	require.Equal(t, "50.0", val)
	require.Equal(t, 50.0, v.GetSeekFactor(nil))

	require.Equal(t, 0.001, v.GetMemoryFactor())
	err = v.SetSystemVar(vardef.TiDBOptMemoryFactor, "1.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptMemoryFactor)
	require.NoError(t, err)
	require.Equal(t, "1.0", val)
	require.Equal(t, 1.0, v.GetMemoryFactor())

	require.Equal(t, 1.5, v.GetDiskFactor())
	err = v.SetSystemVar(vardef.TiDBOptDiskFactor, "1.1")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptDiskFactor)
	require.NoError(t, err)
	require.Equal(t, "1.1", val)
	require.Equal(t, 1.1, v.GetDiskFactor())

	require.Equal(t, 3.0, v.GetConcurrencyFactor())
	err = v.SetSystemVar(vardef.TiDBOptConcurrencyFactor, "5.0")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptConcurrencyFactor)
	require.NoError(t, err)
	require.Equal(t, "5.0", val)
	require.Equal(t, 5.0, v.GetConcurrencyFactor())

	err = v.SetSystemVar(vardef.TiDBReplicaRead, "follower")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "follower", val)
	require.Equal(t, kv.ReplicaReadFollower, v.GetReplicaRead())
	err = v.SetSystemVar(vardef.TiDBReplicaRead, "leader")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "leader", val)
	require.Equal(t, kv.ReplicaReadLeader, v.GetReplicaRead())
	err = v.SetSystemVar(vardef.TiDBReplicaRead, "leader-and-follower")
	require.NoError(t, err)
	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "leader-and-follower", val)
	require.Equal(t, kv.ReplicaReadMixed, v.GetReplicaRead())

	for _, c := range []struct {
		a string
		b string
	}{
		// test 0,1 for old true/false value
		{"1", "ON"},
		{"0", "OFF"},
		{"oN", "ON"},
		{"OfF", "OFF"},
		{"marker", "MARKER"},
		{"2", "MARKER"},
	} {
		err = v.SetSystemVar(vardef.TiDBRedactLog, c.a)
		require.NoError(t, err)
		val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBRedactLog)
		require.NoError(t, err)
		require.Equal(t, c.b, val)
	}

	err = v.SetSystemVar(vardef.TiDBFoundInPlanCache, "1")
	require.Error(t, err)
	require.Regexp(t, "]Variable 'last_plan_from_cache' is a read only variable$", err.Error())

	err = v.SetSystemVar(vardef.TiDBFoundInBinding, "1")
	require.Error(t, err)
	require.Regexp(t, "]Variable 'last_plan_from_binding' is a read only variable$", err.Error())

	err = v.SetSystemVar("UnknownVariable", "on")
	require.Error(t, err)
	require.Regexp(t, "]Unknown system variable 'UnknownVariable'$", err.Error())

	// reset warnings
	v.StmtCtx.TruncateWarnings(0)
	require.Len(t, v.StmtCtx.GetWarnings(), 0)

	err = v.SetSystemVar(vardef.TiDBAnalyzeVersion, "4")
	require.NoError(t, err) // converts to max value
	warn := v.StmtCtx.GetWarnings()[0]
	require.Error(t, warn.Err)
	require.Contains(t, warn.Err.Error(), "Truncated incorrect tidb_analyze_version value")

	err = v.SetSystemVar(vardef.TiDBTableCacheLease, "123")
	require.Error(t, err)
	require.Regexp(t, "'tidb_table_cache_lease' is a GLOBAL variable and should be set with SET GLOBAL", err.Error())

	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBMinPagingSize)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(vardef.DefMinPagingSize), val)

	err = v.SetSystemVar(vardef.TiDBMinPagingSize, "123")
	require.NoError(t, err)
	require.Equal(t, v.MinPagingSize, 123)

	val, err = v.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBMaxPagingSize)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(vardef.DefMaxPagingSize), val)

	err = v.SetSystemVar(vardef.TiDBMaxPagingSize, "456")
	require.NoError(t, err)
	require.Equal(t, v.MaxPagingSize, 456)

	err = v.SetSystemVar(vardef.TiDBMaxPagingSize, "45678")
	require.NoError(t, err)
	require.Equal(t, v.MaxPagingSize, 45678)
}

func TestValidate(t *testing.T) {
	v := NewSessionVars(nil)
	v.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	v.TimeZone = time.UTC

	testCases := []struct {
		key   string
		value string
		error bool
	}{
		{vardef.TiDBAutoAnalyzeStartTime, "15:04", false},
		{vardef.TiDBAutoAnalyzeStartTime, "15:04 -0700", false},
		{vardef.DelayKeyWrite, "ON", false},
		{vardef.DelayKeyWrite, "OFF", false},
		{vardef.DelayKeyWrite, "ALL", false},
		{vardef.DelayKeyWrite, "3", true},
		{vardef.ForeignKeyChecks, "3", true},
		{vardef.MaxSpRecursionDepth, "256", false},
		{vardef.SessionTrackGtids, "OFF", false},
		{vardef.SessionTrackGtids, "OWN_GTID", false},
		{vardef.SessionTrackGtids, "ALL_GTIDS", false},
		{vardef.SessionTrackGtids, "ON", true},
		{vardef.EnforceGtidConsistency, "OFF", false},
		{vardef.EnforceGtidConsistency, "ON", false},
		{vardef.EnforceGtidConsistency, "WARN", false},
		{vardef.SecureAuth, "1", false},
		{vardef.SecureAuth, "3", true},
		{vardef.MyISAMUseMmap, "ON", false},
		{vardef.MyISAMUseMmap, "OFF", false},
		{vardef.TiDBOptCorrelationExpFactor, "a", true},
		{vardef.TiDBOptCorrelationExpFactor, "-10", false},
		{vardef.TiDBOptCorrelationThreshold, "a", true},
		{vardef.TiDBOptCorrelationThreshold, "-2", false},
		{vardef.TiDBOptCPUFactor, "a", true},
		{vardef.TiDBOptCPUFactor, "-2", false},
		{vardef.TiDBOptTiFlashConcurrencyFactor, "-2", false},
		{vardef.TiDBOptCopCPUFactor, "a", true},
		{vardef.TiDBOptCopCPUFactor, "-2", false},
		{vardef.TiDBOptNetworkFactor, "a", true},
		{vardef.TiDBOptNetworkFactor, "-2", false},
		{vardef.TiDBOptScanFactor, "a", true},
		{vardef.TiDBOptScanFactor, "-2", false},
		{vardef.TiDBOptDescScanFactor, "a", true},
		{vardef.TiDBOptDescScanFactor, "-2", false},
		{vardef.TiDBOptSeekFactor, "a", true},
		{vardef.TiDBOptSeekFactor, "-2", false},
		{vardef.TiDBOptMemoryFactor, "a", true},
		{vardef.TiDBOptMemoryFactor, "-2", false},
		{vardef.TiDBOptDiskFactor, "a", true},
		{vardef.TiDBOptDiskFactor, "-2", false},
		{vardef.TiDBOptConcurrencyFactor, "a", true},
		{vardef.TiDBOptConcurrencyFactor, "-2", false},
		{vardef.TxnIsolation, "READ-UNCOMMITTED", true},
		{vardef.TiDBInitChunkSize, "a", true},
		{vardef.TiDBInitChunkSize, "-1", false},
		{vardef.TiDBMaxChunkSize, "a", true},
		{vardef.TiDBMaxChunkSize, "-1", false},
		{vardef.TiDBOptJoinReorderThreshold, "a", true},
		{vardef.TiDBOptJoinReorderThreshold, "-1", false},
		{vardef.TiDBReplicaRead, "invalid", true},
		{vardef.TiDBTxnMode, "invalid", true},
		{vardef.TiDBTxnMode, "pessimistic", false},
		{vardef.TiDBTxnMode, "optimistic", false},
		{vardef.TiDBTxnMode, "", false},
		{vardef.TiDBShardAllocateStep, "ad", true},
		{vardef.TiDBShardAllocateStep, "-123", false},
		{vardef.TiDBShardAllocateStep, "128", false},
		{vardef.TiDBAllowFallbackToTiKV, "", false},
		{vardef.TiDBAllowFallbackToTiKV, "tiflash", false},
		{vardef.TiDBAllowFallbackToTiKV, "  tiflash  ", false},
		{vardef.TiDBAllowFallbackToTiKV, "tikv", true},
		{vardef.TiDBAllowFallbackToTiKV, "tidb", true},
		{vardef.TiDBAllowFallbackToTiKV, "tiflash,tikv,tidb", true},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			_, err := GetSysVar(tc.key).Validate(v, tc.value, vardef.ScopeGlobal)
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
		{vardef.TiDBIsolationReadEngines, "", true},
		{vardef.TiDBIsolationReadEngines, "tikv", false},
		{vardef.TiDBIsolationReadEngines, "TiKV,tiflash", false},
		{vardef.TiDBIsolationReadEngines, "   tikv,   tiflash  ", false},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			_, err := GetSysVar(tc.key).Validate(v, tc.value, vardef.ScopeSession)
			if tc.error {
				require.Errorf(t, err, "%v got err=%v", tc, err)
			} else {
				require.NoErrorf(t, err, "%v got err=%v", tc, err)
			}
		})
	}
}

func TestValidateStmtSummary(t *testing.T) {
	v := NewSessionVars(nil)
	v.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	v.TimeZone = time.UTC

	testCases := []struct {
		key   string
		value string
		error bool
	}{
		{vardef.TiDBEnableStmtSummary, "", true},
		{vardef.TiDBStmtSummaryInternalQuery, "", true},
		{vardef.TiDBStmtSummaryRefreshInterval, "", true},
		{vardef.TiDBStmtSummaryRefreshInterval, "0", false},
		{vardef.TiDBStmtSummaryRefreshInterval, "99999999999", false},
		{vardef.TiDBStmtSummaryHistorySize, "", true},
		{vardef.TiDBStmtSummaryHistorySize, "0", false},
		{vardef.TiDBStmtSummaryHistorySize, "-1", false},
		{vardef.TiDBStmtSummaryHistorySize, "99999999", false},
		{vardef.TiDBStmtSummaryMaxStmtCount, "", true},
		{vardef.TiDBStmtSummaryMaxStmtCount, "0", false},
		{vardef.TiDBStmtSummaryMaxStmtCount, "99999999", false},
		{vardef.TiDBStmtSummaryMaxSQLLength, "", true},
		{vardef.TiDBStmtSummaryMaxSQLLength, "0", false},
		{vardef.TiDBStmtSummaryMaxSQLLength, "-1", false},
		{vardef.TiDBStmtSummaryMaxSQLLength, "99999999999", false},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			_, err := GetSysVar(tc.key).Validate(v, tc.value, vardef.ScopeGlobal)
			if tc.error {
				require.Errorf(t, err, "%v got err=%v", tc, err)
			} else {
				require.NoErrorf(t, err, "%v got err=%v", tc, err)
			}
		})
	}
}

func TestConcurrencyVariables(t *testing.T) {
	vars := NewSessionVars(nil)
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()

	wdConcurrency := 2
	require.Equal(t, vardef.ConcurrencyUnset, vars.windowConcurrency)
	require.Equal(t, vardef.DefExecutorConcurrency, vars.WindowConcurrency())
	err := vars.SetSystemVar(vardef.TiDBWindowConcurrency, strconv.Itoa(wdConcurrency))
	require.NoError(t, err)
	require.Equal(t, wdConcurrency, vars.windowConcurrency)
	require.Equal(t, wdConcurrency, vars.WindowConcurrency())

	mjConcurrency := 2
	require.Equal(t, vardef.DefTiDBMergeJoinConcurrency, vars.mergeJoinConcurrency)
	require.Equal(t, vardef.DefTiDBMergeJoinConcurrency, vars.MergeJoinConcurrency())
	err = vars.SetSystemVar(vardef.TiDBMergeJoinConcurrency, strconv.Itoa(mjConcurrency))
	require.NoError(t, err)
	require.Equal(t, mjConcurrency, vars.mergeJoinConcurrency)
	require.Equal(t, mjConcurrency, vars.MergeJoinConcurrency())

	saConcurrency := 2
	require.Equal(t, vardef.DefTiDBStreamAggConcurrency, vars.streamAggConcurrency)
	require.Equal(t, vardef.DefTiDBStreamAggConcurrency, vars.StreamAggConcurrency())
	err = vars.SetSystemVar(vardef.TiDBStreamAggConcurrency, strconv.Itoa(saConcurrency))
	require.NoError(t, err)
	require.Equal(t, saConcurrency, vars.streamAggConcurrency)
	require.Equal(t, saConcurrency, vars.StreamAggConcurrency())

	require.Equal(t, vardef.ConcurrencyUnset, vars.indexLookupConcurrency)
	require.Equal(t, vardef.DefExecutorConcurrency, vars.IndexLookupConcurrency())
	exeConcurrency := vardef.DefExecutorConcurrency + 1
	err = vars.SetSystemVar(vardef.TiDBExecutorConcurrency, strconv.Itoa(exeConcurrency))
	require.NoError(t, err)
	require.Equal(t, vardef.ConcurrencyUnset, vars.indexLookupConcurrency)
	require.Equal(t, exeConcurrency, vars.IndexLookupConcurrency())
	require.Equal(t, wdConcurrency, vars.WindowConcurrency())
	require.Equal(t, mjConcurrency, vars.MergeJoinConcurrency())
	require.Equal(t, saConcurrency, vars.StreamAggConcurrency())
}

func TestHelperFuncs(t *testing.T) {
	require.Equal(t, "ON", int32ToBoolStr(1))
	require.Equal(t, "OFF", int32ToBoolStr(0))

	require.Equal(t, vardef.ClusteredIndexDefModeOn, vardef.TiDBOptEnableClustered("ON"))
	require.Equal(t, vardef.ClusteredIndexDefModeOff, vardef.TiDBOptEnableClustered("OFF"))
	require.Equal(t, vardef.ClusteredIndexDefModeIntOnly, vardef.TiDBOptEnableClustered("bogus")) // default

	require.Equal(t, 1234, tidbOptPositiveInt32("1234", 5))
	require.Equal(t, 5, tidbOptPositiveInt32("-1234", 5))
	require.Equal(t, 5, tidbOptPositiveInt32("bogus", 5))

	require.Equal(t, 1234, TidbOptInt("1234", 5))
	require.Equal(t, -1234, TidbOptInt("-1234", 5))
	require.Equal(t, 5, TidbOptInt("bogus", 5))
}

func TestSessionStatesSystemVar(t *testing.T) {
	vars := NewSessionVars(nil)
	err := vars.SetSystemVar("autocommit", "1")
	require.NoError(t, err)
	val, keep, err := vars.GetSessionStatesSystemVar("autocommit")
	require.NoError(t, err)
	require.Equal(t, "ON", val)
	require.Equal(t, true, keep)
	_, keep, err = vars.GetSessionStatesSystemVar(vardef.Timestamp)
	require.NoError(t, err)
	require.Equal(t, false, keep)
	err = vars.SetSystemVar(vardef.MaxAllowedPacket, "1024")
	require.NoError(t, err)
	val, keep, err = vars.GetSessionStatesSystemVar(vardef.MaxAllowedPacket)
	require.NoError(t, err)
	require.Equal(t, "1024", val)
	require.Equal(t, true, keep)
}

func TestOnOffHelpers(t *testing.T) {
	require.Equal(t, "ON", trueFalseToOnOff("TRUE"))
	require.Equal(t, "ON", trueFalseToOnOff("TRue"))
	require.Equal(t, "ON", trueFalseToOnOff("true"))
	require.Equal(t, "OFF", trueFalseToOnOff("FALSE"))
	require.Equal(t, "OFF", trueFalseToOnOff("False"))
	require.Equal(t, "OFF", trueFalseToOnOff("false"))
	require.Equal(t, "other", trueFalseToOnOff("other"))
	require.Equal(t, "true", OnOffToTrueFalse("ON"))
	require.Equal(t, "true", OnOffToTrueFalse("on"))
	require.Equal(t, "true", OnOffToTrueFalse("On"))
	require.Equal(t, "false", OnOffToTrueFalse("OFF"))
	require.Equal(t, "false", OnOffToTrueFalse("Off"))
	require.Equal(t, "false", OnOffToTrueFalse("off"))
	require.Equal(t, "other", OnOffToTrueFalse("other"))
}

func TestAssertionLevel(t *testing.T) {
	require.Equal(t, AssertionLevelStrict, tidbOptAssertionLevel(vardef.AssertionStrictStr))
	require.Equal(t, AssertionLevelOff, tidbOptAssertionLevel(vardef.AssertionOffStr))
	require.Equal(t, AssertionLevelFast, tidbOptAssertionLevel(vardef.AssertionFastStr))
	require.Equal(t, AssertionLevelOff, tidbOptAssertionLevel("bogus"))
}
