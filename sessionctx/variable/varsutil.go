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
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/timeutil"
)

// secondsPerYear represents seconds in a normal year. Leap year is not considered here.
const secondsPerYear = 60 * 60 * 24 * 365

// SetDDLReorgWorkerCounter sets ddlReorgWorkerCounter count.
// Max worker count is maxDDLReorgWorkerCount.
func SetDDLReorgWorkerCounter(cnt int32) {
	if cnt > maxDDLReorgWorkerCount {
		cnt = maxDDLReorgWorkerCount
	}
	atomic.StoreInt32(&ddlReorgWorkerCounter, cnt)
}

// GetDDLReorgWorkerCounter gets ddlReorgWorkerCounter.
func GetDDLReorgWorkerCounter() int32 {
	return atomic.LoadInt32(&ddlReorgWorkerCounter)
}

// SetDDLReorgBatchSize sets ddlReorgBatchSize size.
// Max batch size is MaxDDLReorgBatchSize.
func SetDDLReorgBatchSize(cnt int32) {
	if cnt > MaxDDLReorgBatchSize {
		cnt = MaxDDLReorgBatchSize
	}
	if cnt < MinDDLReorgBatchSize {
		cnt = MinDDLReorgBatchSize
	}
	atomic.StoreInt32(&ddlReorgBatchSize, cnt)
}

// GetDDLReorgBatchSize gets ddlReorgBatchSize.
func GetDDLReorgBatchSize() int32 {
	return atomic.LoadInt32(&ddlReorgBatchSize)
}

// SetDDLErrorCountLimit sets ddlErrorCountlimit size.
func SetDDLErrorCountLimit(cnt int64) {
	atomic.StoreInt64(&ddlErrorCountlimit, cnt)
}

// GetDDLErrorCountLimit gets ddlErrorCountlimit size.
func GetDDLErrorCountLimit() int64 {
	return atomic.LoadInt64(&ddlErrorCountlimit)
}

// SetMaxDeltaSchemaCount sets maxDeltaSchemaCount size.
func SetMaxDeltaSchemaCount(cnt int64) {
	atomic.StoreInt64(&maxDeltaSchemaCount, cnt)
}

// GetMaxDeltaSchemaCount gets maxDeltaSchemaCount size.
func GetMaxDeltaSchemaCount() int64 {
	return atomic.LoadInt64(&maxDeltaSchemaCount)
}

// GetSessionSystemVar gets a system variable.
// If it is a session only variable, use the default value defined in code.
// Returns error if there is no such variable.
func GetSessionSystemVar(s *SessionVars, key string) (string, error) {
	key = strings.ToLower(key)
	gVal, ok, err := GetSessionOnlySysVars(s, key)
	if err != nil || ok {
		return gVal, err
	}
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", err
	}
	s.systems[key] = gVal
	return gVal, nil
}

// GetSessionOnlySysVars get the default value defined in code for session only variable.
// The return bool value indicates whether it's a session only variable.
func GetSessionOnlySysVars(s *SessionVars, key string) (string, bool, error) {
	sysVar := SysVars[key]
	if sysVar == nil {
		return "", false, ErrUnknownSystemVar.GenWithStackByArgs(key)
	}
	// For virtual system variables:
	switch sysVar.Name {
	case TiDBCurrentTS:
		return fmt.Sprintf("%d", s.TxnCtx.StartTS), true, nil
	case TiDBGeneralLog:
		return fmt.Sprintf("%d", atomic.LoadUint32(&ProcessGeneralLog)), true, nil
	case TiDBPProfSQLCPU:
		val := "0"
		if EnablePProfSQLCPU.Load() {
			val = "1"
		}
		return val, true, nil
	case TiDBExpensiveQueryTimeThreshold:
		return fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveQueryTimeThreshold)), true, nil
	case TiDBConfig:
		conf := config.GetGlobalConfig()
		j, err := json.MarshalIndent(conf, "", "\t")
		if err != nil {
			return "", false, err
		}
		return string(j), true, nil
	case TiDBForcePriority:
		return mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&ForcePriority))], true, nil
	case TiDBDDLSlowOprThreshold:
		return strconv.FormatUint(uint64(atomic.LoadUint32(&DDLSlowOprThreshold)), 10), true, nil
	case PluginDir:
		return config.GetGlobalConfig().Plugin.Dir, true, nil
	case PluginLoad:
		return config.GetGlobalConfig().Plugin.Load, true, nil
	case TiDBSlowLogThreshold:
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.SlowThreshold), 10), true, nil
	case TiDBRecordPlanInSlowLog:
		return strconv.FormatUint(uint64(atomic.LoadUint32(&config.GetGlobalConfig().Log.RecordPlanInSlowLog)), 10), true, nil
	case TiDBEnableSlowLog:
		return BoolToIntStr(config.GetGlobalConfig().Log.EnableSlowLog), true, nil
	case TiDBQueryLogMaxLen:
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen), 10), true, nil
	case TiDBCheckMb4ValueInUTF8:
		return BoolToIntStr(config.GetGlobalConfig().CheckMb4ValueInUTF8), true, nil
	case TiDBCapturePlanBaseline:
		return CapturePlanBaseline.GetVal(), true, nil
	case TiDBFoundInPlanCache:
		return BoolToIntStr(s.PrevFoundInPlanCache), true, nil
	case TiDBEnableCollectExecutionInfo:
		return BoolToIntStr(config.GetGlobalConfig().EnableCollectExecutionInfo), true, nil
	}
	sVal, ok := s.GetSystemVar(key)
	if ok {
		return sVal, true, nil
	}
	if sysVar.Scope&ScopeGlobal == 0 {
		// None-Global variable can use pre-defined default value.
		return sysVar.Value, true, nil
	}
	return "", false, nil
}

// GetGlobalSystemVar gets a global system variable.
func GetGlobalSystemVar(s *SessionVars, key string) (string, error) {
	key = strings.ToLower(key)
	gVal, ok, err := GetScopeNoneSystemVar(key)
	if err != nil || ok {
		return gVal, err
	}
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", err
	}
	return gVal, nil
}

// GetScopeNoneSystemVar checks the validation of `key`,
// and return the default value if its scope is `ScopeNone`.
func GetScopeNoneSystemVar(key string) (string, bool, error) {
	sysVar := SysVars[key]
	if sysVar == nil {
		return "", false, ErrUnknownSystemVar.GenWithStackByArgs(key)
	}
	if sysVar.Scope == ScopeNone {
		return sysVar.Value, true, nil
	}
	return "", false, nil
}

// epochShiftBits is used to reserve logical part of the timestamp.
const epochShiftBits = 18

// SetSessionSystemVar sets system variable and updates SessionVars states.
func SetSessionSystemVar(vars *SessionVars, name string, value types.Datum) error {
	name = strings.ToLower(name)
	sysVar := SysVars[name]
	if sysVar == nil {
		return ErrUnknownSystemVar
	}
	sVal := ""
	var err error
	if !value.IsNull() {
		sVal, err = value.ToString()
	}
	if err != nil {
		return err
	}
	sVal, err = ValidateSetSystemVar(vars, name, sVal, ScopeSession)
	if err != nil {
		return err
	}
	return vars.SetSystemVar(name, sVal)
}

// ValidateGetSystemVar checks if system variable exists and validates its scope when get system variable.
func ValidateGetSystemVar(name string, isGlobal bool) error {
	sysVar, exists := SysVars[name]
	if !exists {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	switch sysVar.Scope {
	case ScopeGlobal, ScopeNone:
		if !isGlobal {
			return ErrIncorrectScope.GenWithStackByArgs(name, "GLOBAL")
		}
	case ScopeSession:
		if isGlobal {
			return ErrIncorrectScope.GenWithStackByArgs(name, "SESSION")
		}
	}
	return nil
}

func checkUInt64SystemVar(name, value string, min, max uint64, vars *SessionVars) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if value[0] == '-' {
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	if val > max {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", max), nil
	}
	return value, nil
}

func checkInt64SystemVar(name, value string, min, max int64, vars *SessionVars) (string, error) {
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	if val > max {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", max), nil
	}
	return value, nil
}

const (
	// initChunkSizeUpperBound indicates upper bound value of tidb_init_chunk_size.
	initChunkSizeUpperBound = 32
	// maxChunkSizeLowerBound indicates lower bound value of tidb_max_chunk_size.
	maxChunkSizeLowerBound = 32
)

// ValidateSetSystemVar checks if system variable satisfies specific restriction.
func ValidateSetSystemVar(vars *SessionVars, name string, value string, scope ScopeFlag) (string, error) {
	if strings.EqualFold(value, "DEFAULT") {
		if val := GetSysVar(name); val != nil {
			return val.Value, nil
		}
		return value, ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	switch name {
	case ConnectTimeout:
		return checkUInt64SystemVar(name, value, 2, secondsPerYear, vars)
	case DefaultWeekFormat:
		return checkUInt64SystemVar(name, value, 0, 7, vars)
	case DelayKeyWrite:
		if strings.EqualFold(value, "ON") || value == "1" {
			return "ON", nil
		} else if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "ALL") || value == "2" {
			return "ALL", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case FlushTime:
		return checkUInt64SystemVar(name, value, 0, secondsPerYear, vars)
	case ForeignKeyChecks:
		if strings.EqualFold(value, "ON") || value == "1" {
			// TiDB does not yet support foreign keys.
			// For now, resist the change and show a warning.
			vars.StmtCtx.AppendWarning(ErrUnsupportedValueForVar.GenWithStackByArgs(name, value))
			return "OFF", nil
		} else if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case GroupConcatMaxLen:
		// The reasonable range of 'group_concat_max_len' is 4~18446744073709551615(64-bit platforms)
		// See https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len for details
		return checkUInt64SystemVar(name, value, 4, math.MaxUint64, vars)
	case InteractiveTimeout:
		return checkUInt64SystemVar(name, value, 1, secondsPerYear, vars)
	case InnodbCommitConcurrency:
		return checkUInt64SystemVar(name, value, 0, 1000, vars)
	case InnodbFastShutdown:
		return checkUInt64SystemVar(name, value, 0, 2, vars)
	case InnodbLockWaitTimeout:
		return checkUInt64SystemVar(name, value, 1, 1073741824, vars)
	// See "https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet"
	case MaxAllowedPacket:
		return checkUInt64SystemVar(name, value, 1024, MaxOfMaxAllowedPacket, vars)
	case MaxConnections:
		return checkUInt64SystemVar(name, value, 1, 100000, vars)
	case MaxConnectErrors:
		return checkUInt64SystemVar(name, value, 1, math.MaxUint64, vars)
	case MaxSortLength:
		return checkUInt64SystemVar(name, value, 4, 8388608, vars)
	case MaxSpRecursionDepth:
		return checkUInt64SystemVar(name, value, 0, 255, vars)
	case MaxUserConnections:
		return checkUInt64SystemVar(name, value, 0, 4294967295, vars)
	case OldPasswords:
		return checkUInt64SystemVar(name, value, 0, 2, vars)
	case TiDBMaxDeltaSchemaCount:
		return checkInt64SystemVar(name, value, 100, 16384, vars)
	case SessionTrackGtids:
		if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "OWN_GTID") || value == "1" {
			return "OWN_GTID", nil
		} else if strings.EqualFold(value, "ALL_GTIDS") || value == "2" {
			return "ALL_GTIDS", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case SQLSelectLimit:
		return checkUInt64SystemVar(name, value, 0, math.MaxUint64, vars)
	case TiDBStoreLimit:
		return checkInt64SystemVar(name, value, 0, math.MaxInt64, vars)
	case SyncBinlog:
		return checkUInt64SystemVar(name, value, 0, 4294967295, vars)
	case TableDefinitionCache:
		return checkUInt64SystemVar(name, value, 400, 524288, vars)
	case TmpTableSize:
		return checkUInt64SystemVar(name, value, 1024, math.MaxUint64, vars)
	case WaitTimeout:
		return checkUInt64SystemVar(name, value, 0, 31536000, vars)
	case MaxPreparedStmtCount:
		return checkInt64SystemVar(name, value, -1, 1048576, vars)
	case TimeZone:
		if strings.EqualFold(value, "SYSTEM") {
			return "SYSTEM", nil
		}
		_, err := parseTimeZone(value)
		return value, err
	case ValidatePasswordLength, ValidatePasswordNumberCount:
		return checkUInt64SystemVar(name, value, 0, math.MaxUint64, vars)
	case WarningCount, ErrorCount:
		return value, ErrReadOnly.GenWithStackByArgs(name)
	case EnforceGtidConsistency:
		if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "ON") || value == "1" {
			return "ON", nil
		} else if strings.EqualFold(value, "WARN") || value == "2" {
			return "WARN", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case QueryCacheType:
		if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "ON") || value == "1" {
			return "ON", nil
		} else if strings.EqualFold(value, "DEMAND") || value == "2" {
			return "DEMAND", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case SecureAuth:
		if strings.EqualFold(value, "ON") || value == "1" {
			return "1", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case WindowingUseHighPrecision:
		if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "ON") || value == "1" {
			return "ON", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBSkipUTF8Check, TiDBOptAggPushDown, TiDBOptDistinctAggPushDown,
		TiDBOptInSubqToJoinAndAgg, TiDBEnableFastAnalyze,
		TiDBBatchInsert, TiDBDisableTxnAutoRetry, TiDBEnableStreaming, TiDBEnableChunkRPC,
		TiDBBatchDelete, TiDBBatchCommit, TiDBEnableCascadesPlanner, TiDBEnableWindowFunction, TiDBPProfSQLCPU,
		TiDBLowResolutionTSO, TiDBEnableIndexMerge, TiDBEnableNoopFuncs,
		TiDBCheckMb4ValueInUTF8, TiDBEnableSlowLog, TiDBRecordPlanInSlowLog,
		TiDBScatterRegion, TiDBGeneralLog, TiDBConstraintCheckInPlace,
		TiDBEnableVectorizedExpression, TiDBFoundInPlanCache, TiDBEnableCollectExecutionInfo:
		fallthrough
	case GeneralLog, AvoidTemporalUpgrade, BigTables, CheckProxyUsers, LogBin,
		CoreFile, EndMakersInJSON, SQLLogBin, OfflineMode, PseudoSlaveMode, LowPriorityUpdates,
		SkipNameResolve, SQLSafeUpdates, serverReadOnly, SlaveAllowBatching,
		Flush, PerformanceSchema, LocalInFile, ShowOldTemporals, KeepFilesOnCreate, AutoCommit,
		SQLWarnings, UniqueChecks, OldAlterTable, LogBinTrustFunctionCreators, SQLBigSelects,
		BinlogDirectNonTransactionalUpdates, SQLQuoteShowCreate, AutomaticSpPrivileges,
		RelayLogPurge, SQLAutoIsNull, QueryCacheWlockInvalidate, ValidatePasswordCheckUserName,
		SuperReadOnly, BinlogOrderCommits, MasterVerifyChecksum, BinlogRowQueryLogEvents, LogSlowSlaveStatements,
		LogSlowAdminStatements, LogQueriesNotUsingIndexes, Profiling:
		if strings.EqualFold(value, "ON") {
			return "1", nil
		} else if strings.EqualFold(value, "OFF") {
			return "0", nil
		}
		val, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			if val == 0 {
				return "0", nil
			} else if val == 1 {
				return "1", nil
			}
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MyISAMUseMmap, InnodbTableLocks, InnodbStatusOutput, InnodbAdaptiveFlushing, InnodbRandomReadAhead,
		InnodbStatsPersistent, InnodbBufferPoolLoadAbort, InnodbBufferPoolLoadNow, InnodbBufferPoolDumpNow,
		InnodbCmpPerIndexEnabled, InnodbFilePerTable, InnodbPrintAllDeadlocks,
		InnodbStrictMode, InnodbAdaptiveHashIndex, InnodbFtEnableStopword, InnodbStatusOutputLocks:
		if strings.EqualFold(value, "ON") {
			return "1", nil
		} else if strings.EqualFold(value, "OFF") {
			return "0", nil
		}
		val, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			if val == 1 || val < 0 {
				return "1", nil
			} else if val == 0 {
				return "0", nil
			}
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MaxExecutionTime:
		return checkUInt64SystemVar(name, value, 0, math.MaxUint64, vars)
	case ThreadPoolSize:
		return checkUInt64SystemVar(name, value, 1, 64, vars)
	case TiDBEnableTablePartition:
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			return "on", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			return "off", nil
		case strings.EqualFold(value, "AUTO"):
			return "auto", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBDDLReorgBatchSize:
		return checkUInt64SystemVar(name, value, uint64(MinDDLReorgBatchSize), uint64(MaxDDLReorgBatchSize), vars)
	case TiDBDDLErrorCountLimit:
		return checkUInt64SystemVar(name, value, uint64(0), math.MaxInt64, vars)
	case TiDBExpensiveQueryTimeThreshold:
		return checkUInt64SystemVar(name, value, MinExpensiveQueryTimeThreshold, math.MaxInt64, vars)
	case TiDBIndexLookupConcurrency, TiDBIndexLookupJoinConcurrency, TiDBIndexJoinBatchSize,
		TiDBIndexLookupSize,
		TiDBHashJoinConcurrency,
		TiDBHashAggPartialConcurrency,
		TiDBHashAggFinalConcurrency,
		TiDBWindowConcurrency,
		TiDBDistSQLScanConcurrency,
		TiDBIndexSerialScanConcurrency, TiDBDDLReorgWorkerCount,
		TiDBBackoffLockFast, TiDBBackOffWeight,
		TiDBDMLBatchSize, TiDBOptimizerSelectivityLevel:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v <= 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TiDBOptCorrelationExpFactor:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TiDBOptCorrelationThreshold:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 || v > 1 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TiDBAllowBatchCop:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 || v > 2 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TiDBOptCPUFactor,
		TiDBOptCopCPUFactor,
		TiDBOptNetworkFactor,
		TiDBOptScanFactor,
		TiDBOptDescScanFactor,
		TiDBOptSeekFactor,
		TiDBOptMemoryFactor,
		TiDBOptDiskFactor,
		TiDBOptConcurrencyFactor:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TiDBProjectionConcurrency,
		TIDBMemQuotaQuery,
		TIDBMemQuotaHashJoin,
		TIDBMemQuotaMergeJoin,
		TIDBMemQuotaSort,
		TIDBMemQuotaTopn,
		TIDBMemQuotaIndexLookupReader,
		TIDBMemQuotaIndexLookupJoin,
		TIDBMemQuotaNestedLoopApply,
		TiDBRetryLimit,
		TiDBSlowLogThreshold,
		TiDBQueryLogMaxLen,
		TiDBEvolvePlanTaskMaxTime:
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name)
		}
		return value, nil
	case TiDBAutoAnalyzeStartTime, TiDBAutoAnalyzeEndTime, TiDBEvolvePlanTaskStartTime, TiDBEvolvePlanTaskEndTime:
		v, err := setDayTime(vars, value)
		if err != nil {
			return "", err
		}
		return v, nil
	case TiDBAutoAnalyzeRatio:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil || v < 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TxnIsolation, TransactionIsolation:
		upVal := strings.ToUpper(value)
		_, exists := TxIsolationNames[upVal]
		if !exists {
			return "", ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		switch upVal {
		case "SERIALIZABLE", "READ-UNCOMMITTED":
			skipIsolationLevelCheck, err := GetSessionSystemVar(vars, TiDBSkipIsolationLevelCheck)
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(value)
			if err != nil {
				returnErr = err
			}
			if !TiDBOptOn(skipIsolationLevelCheck) || err != nil {
				return "", returnErr
			}
			//SET TRANSACTION ISOLATION LEVEL will affect two internal variables:
			// 1. tx_isolation
			// 2. transaction_isolation
			// The following if condition is used to deduplicate two same warnings.
			if name == "transaction_isolation" {
				vars.StmtCtx.AppendWarning(returnErr)
			}
		}
		return upVal, nil
	case TiDBInitChunkSize:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v <= 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		if v > initChunkSizeUpperBound {
			return value, errors.Errorf("tidb_init_chunk_size(%d) cannot be bigger than %d", v, initChunkSizeUpperBound)
		}
		return value, nil
	case TiDBMaxChunkSize:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < maxChunkSizeLowerBound {
			return value, errors.Errorf("tidb_max_chunk_size(%d) cannot be smaller than %d", v, maxChunkSizeLowerBound)
		}
		return value, nil
	case TiDBOptJoinReorderThreshold:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 || v >= 64 {
			return value, errors.Errorf("tidb_join_order_algo_threshold(%d) cannot be smaller than 0 or larger than 63", v)
		}
	case TiDBWaitSplitRegionTimeout:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v <= 0 {
			return value, errors.Errorf("tidb_wait_split_region_timeout(%d) cannot be smaller than 1", v)
		}
	case TiDBReplicaRead:
		if strings.EqualFold(value, "follower") {
			return "follower", nil
		} else if strings.EqualFold(value, "leader-and-follower") {
			return "leader-and-follower", nil
		} else if strings.EqualFold(value, "leader") || len(value) == 0 {
			return "leader", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBTxnMode:
		switch strings.ToUpper(value) {
		case ast.Pessimistic, ast.Optimistic, "":
		default:
			return value, ErrWrongValueForVar.GenWithStackByArgs(TiDBTxnMode, value)
		}
	case TiDBRowFormatVersion:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v != DefTiDBRowFormatV1 && v != DefTiDBRowFormatV2 {
			return value, errors.Errorf("Unsupported row format version %d", v)
		}
	case TiDBAllowRemoveAutoInc, TiDBUsePlanBaselines, TiDBEvolvePlanBaselines:
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			return "on", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			return "off", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBCapturePlanBaseline:
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			return "on", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			return "off", nil
		case value == "":
			return "", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBEnableStmtSummary, TiDBStmtSummaryInternalQuery:
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			return "1", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			return "0", nil
		case value == "":
			if scope == ScopeSession {
				return "", nil
			}
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBStmtSummaryRefreshInterval:
		if value == "" && scope == ScopeSession {
			return "", nil
		}
		return checkUInt64SystemVar(name, value, 1, math.MaxInt32, vars)
	case TiDBStmtSummaryHistorySize:
		if value == "" && scope == ScopeSession {
			return "", nil
		}
		return checkUInt64SystemVar(name, value, 0, math.MaxUint8, vars)
	case TiDBStmtSummaryMaxStmtCount:
		if value == "" && scope == ScopeSession {
			return "", nil
		}
		return checkInt64SystemVar(name, value, 1, math.MaxInt16, vars)
	case TiDBStmtSummaryMaxSQLLength:
		if value == "" && scope == ScopeSession {
			return "", nil
		}
		return checkInt64SystemVar(name, value, 0, math.MaxInt32, vars)
	case TiDBIsolationReadEngines:
		engines := strings.Split(value, ",")
		var formatVal string
		for i, engine := range engines {
			engine = strings.TrimSpace(engine)
			if i != 0 {
				formatVal += ","
			}
			switch {
			case strings.EqualFold(engine, kv.TiKV.Name()):
				formatVal += kv.TiKV.Name()
			case strings.EqualFold(engine, kv.TiFlash.Name()):
				formatVal += kv.TiFlash.Name()
			case strings.EqualFold(engine, kv.TiDB.Name()):
				formatVal += kv.TiDB.Name()
			default:
				return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
			}
		}
		return formatVal, nil
	case TiDBMetricSchemaStep, TiDBMetricSchemaRangeDuration:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		if v < 10 || v > 60*60*60 {
			return value, errors.Errorf("%v(%d) cannot be smaller than %v or larger than %v", name, v, 10, 60*60*60)
		}
		return value, nil
	case CollationConnection, CollationDatabase, CollationServer:
		if _, err := collate.GetCollationByName(value); err != nil {
			return value, errors.Trace(err)
		}
	}
	return value, nil
}

// TiDBOptOn could be used for all tidb session variable options, we use "ON"/1 to turn on those options.
func TiDBOptOn(opt string) bool {
	return strings.EqualFold(opt, "ON") || opt == "1"
}

func tidbOptPositiveInt32(opt string, defaultVal int) int {
	val, err := strconv.Atoi(opt)
	if err != nil || val <= 0 {
		return defaultVal
	}
	return val
}

func tidbOptInt64(opt string, defaultVal int64) int64 {
	val, err := strconv.ParseInt(opt, 10, 64)
	if err != nil {
		return defaultVal
	}
	return val
}

func tidbOptFloat64(opt string, defaultVal float64) float64 {
	val, err := strconv.ParseFloat(opt, 64)
	if err != nil {
		return defaultVal
	}
	return val
}

func parseTimeZone(s string) (*time.Location, error) {
	if strings.EqualFold(s, "SYSTEM") {
		return timeutil.SystemLocation(), nil
	}

	loc, err := time.LoadLocation(s)
	if err == nil {
		return loc, nil
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	// The time zone's value should in [-12:59,+14:00].
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		d, err := types.ParseDuration(nil, s[1:], 0)
		if err == nil {
			if s[0] == '-' {
				if d.Duration > 12*time.Hour+59*time.Minute {
					return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
				}
			} else {
				if d.Duration > 14*time.Hour {
					return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
				}
			}

			ofst := int(d.Duration / time.Second)
			if s[0] == '-' {
				ofst = -ofst
			}
			return time.FixedZone("", ofst), nil
		}
	}

	return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
}

func setSnapshotTS(s *SessionVars, sVal string) error {
	if sVal == "" {
		s.SnapshotTS = 0
		return nil
	}

	if tso, err := strconv.ParseUint(sVal, 10, 64); err == nil {
		s.SnapshotTS = tso
		return nil
	}

	t, err := types.ParseTime(s.StmtCtx, sVal, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return err
	}

	t1, err := t.GoTime(s.TimeZone)
	s.SnapshotTS = GoTimeToTS(t1)
	return err
}

// GoTimeToTS converts a Go time to uint64 timestamp.
func GoTimeToTS(t time.Time) uint64 {
	ts := (t.UnixNano() / int64(time.Millisecond)) << epochShiftBits
	return uint64(ts)
}

const (
	localDayTimeFormat = "15:04"
	// FullDayTimeFormat is the full format of analyze start time and end time.
	FullDayTimeFormat = "15:04 -0700"
)

func setDayTime(s *SessionVars, val string) (string, error) {
	var t time.Time
	var err error
	if len(val) <= len(localDayTimeFormat) {
		t, err = time.ParseInLocation(localDayTimeFormat, val, s.TimeZone)
	} else {
		t, err = time.ParseInLocation(FullDayTimeFormat, val, s.TimeZone)
	}
	if err != nil {
		return "", err
	}
	return t.Format(FullDayTimeFormat), nil
}

// serverGlobalVariable is used to handle variables that acts in server and global scope.
type serverGlobalVariable struct {
	sync.Mutex
	serverVal string
	globalVal string
}

// Set sets the value according to variable scope.
func (v *serverGlobalVariable) Set(val string, isServer bool) {
	v.Lock()
	if isServer {
		v.serverVal = val
	} else {
		v.globalVal = val
	}
	v.Unlock()
}

// GetVal gets the value.
func (v *serverGlobalVariable) GetVal() string {
	v.Lock()
	defer v.Unlock()
	if v.serverVal != "" {
		return v.serverVal
	}
	return v.globalVal
}
