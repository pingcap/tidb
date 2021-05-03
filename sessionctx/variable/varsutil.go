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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
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

// SetDDLReorgRowFormat sets ddlReorgRowFormat version.
func SetDDLReorgRowFormat(format int64) {
	atomic.StoreInt64(&ddlReorgRowFormat, format)
}

// GetDDLReorgRowFormat gets ddlReorgRowFormat version.
func GetDDLReorgRowFormat() int64 {
	return atomic.LoadInt64(&ddlReorgRowFormat)
}

// SetMaxDeltaSchemaCount sets maxDeltaSchemaCount size.
func SetMaxDeltaSchemaCount(cnt int64) {
	atomic.StoreInt64(&maxDeltaSchemaCount, cnt)
}

// GetMaxDeltaSchemaCount gets maxDeltaSchemaCount size.
func GetMaxDeltaSchemaCount() int64 {
	return atomic.LoadInt64(&maxDeltaSchemaCount)
}

// BoolToOnOff returns the string representation of a bool, i.e. "ON/OFF"
func BoolToOnOff(b bool) string {
	if b {
		return On
	}
	return Off
}

func int32ToBoolStr(i int32) string {
	if i == 1 {
		return On
	}
	return Off
}

func checkCollation(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
	if _, err := collate.GetCollationByName(normalizedValue); err != nil {
		return normalizedValue, errors.Trace(err)
	}
	return normalizedValue, nil
}

func checkCharacterSet(normalizedValue string, argName string) (string, error) {
	if normalizedValue == "" {
		return normalizedValue, errors.Trace(ErrWrongValueForVar.GenWithStackByArgs(argName, "NULL"))
	}
	cht, _, err := charset.GetCharsetInfo(normalizedValue)
	if err != nil {
		return normalizedValue, errors.Trace(err)
	}
	return cht, nil
}

// checkReadOnly requires TiDBEnableNoopFuncs=1 for the same scope otherwise an error will be returned.
func checkReadOnly(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag, offlineMode bool) (string, error) {
	feature := "READ ONLY"
	if offlineMode {
		feature = "OFFLINE MODE"
	}
	if TiDBOptOn(normalizedValue) {
		if !vars.EnableNoopFuncs && scope == ScopeSession {
			return Off, ErrFunctionsNoopImpl.GenWithStackByArgs(feature)
		}
		val, err := vars.GlobalVarsAccessor.GetGlobalSysVar(TiDBEnableNoopFuncs)
		if err != nil {
			return originalValue, errUnknownSystemVariable.GenWithStackByArgs(TiDBEnableNoopFuncs)
		}
		if scope == ScopeGlobal && !TiDBOptOn(val) {
			return Off, ErrFunctionsNoopImpl.GenWithStackByArgs(feature)
		}
	}
	return normalizedValue, nil
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
	sysVar := GetSysVar(key)
	if sysVar == nil {
		return "", false, ErrUnknownSystemVar.GenWithStackByArgs(key)
	}
	// For virtual system variables:
	switch sysVar.Name {
	case TiDBCurrentTS:
		return fmt.Sprintf("%d", s.TxnCtx.StartTS), true, nil
	case TiDBLastTxnInfo:
		return s.LastTxnInfo, true, nil
	case TiDBLastQueryInfo:
		info, err := json.Marshal(s.LastQueryInfo)
		if err != nil {
			return "", true, err
		}
		return string(info), true, nil
	case TiDBGeneralLog:
		return BoolToOnOff(ProcessGeneralLog.Load()), true, nil
	case TiDBPProfSQLCPU:
		val := "0"
		if EnablePProfSQLCPU.Load() {
			val = "1"
		}
		return val, true, nil
	case TiDBExpensiveQueryTimeThreshold:
		return fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveQueryTimeThreshold)), true, nil
	case TiDBMemoryUsageAlarmRatio:
		return fmt.Sprintf("%g", MemoryUsageAlarmRatio.Load()), true, nil
	case TiDBConfig:
		conf := config.GetGlobalConfig()
		j, err := json.MarshalIndent(conf, "", "\t")
		if err != nil {
			return "", false, err
		}
		return config.HideConfig(string(j)), true, nil
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
		return BoolToOnOff(config.GetGlobalConfig().Log.EnableSlowLog), true, nil
	case TiDBQueryLogMaxLen:
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen), 10), true, nil
	case TiDBCheckMb4ValueInUTF8:
		return BoolToOnOff(config.GetGlobalConfig().CheckMb4ValueInUTF8), true, nil
	case TiDBCapturePlanBaseline:
		return CapturePlanBaseline.GetVal(), true, nil
	case TiDBFoundInPlanCache:
		return BoolToOnOff(s.PrevFoundInPlanCache), true, nil
	case TiDBFoundInBinding:
		return BoolToOnOff(s.PrevFoundInBinding), true, nil
	case TiDBEnableCollectExecutionInfo:
		return BoolToOnOff(config.GetGlobalConfig().EnableCollectExecutionInfo), true, nil
	case TiDBTxnScope:
		return s.TxnScope.GetVarValue(), true, nil
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
	sysVar := GetSysVar(key)
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
func SetSessionSystemVar(vars *SessionVars, name string, value string) error {
	sysVar := GetSysVar(name)
	if sysVar == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	sVal, err := sysVar.Validate(vars, value, ScopeSession)
	if err != nil {
		return err
	}
	CheckDeprecationSetSystemVar(vars, name)
	return vars.SetSystemVar(name, sVal)
}

// SetStmtVar sets system variable and updates SessionVars states.
func SetStmtVar(vars *SessionVars, name string, value string) error {
	name = strings.ToLower(name)
	sysVar := GetSysVar(name)
	if sysVar == nil {
		return ErrUnknownSystemVar
	}
	sVal, err := sysVar.Validate(vars, value, ScopeSession)
	if err != nil {
		return err
	}
	CheckDeprecationSetSystemVar(vars, name)
	return vars.SetStmtVar(name, sVal)
}

// ValidateGetSystemVar checks if system variable exists and validates its scope when get system variable.
func ValidateGetSystemVar(name string, isGlobal bool) error {
	sysVar := GetSysVar(name)
	if sysVar == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	switch sysVar.Scope {
	case ScopeGlobal:
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

const (
	// initChunkSizeUpperBound indicates upper bound value of tidb_init_chunk_size.
	initChunkSizeUpperBound = 32
	// maxChunkSizeLowerBound indicates lower bound value of tidb_max_chunk_size.
	maxChunkSizeLowerBound = 32
)

// CheckDeprecationSetSystemVar checks if the system variable is deprecated.
func CheckDeprecationSetSystemVar(s *SessionVars, name string) {
	switch name {
	case TiDBIndexLookupConcurrency, TiDBIndexLookupJoinConcurrency,
		TiDBHashJoinConcurrency, TiDBHashAggPartialConcurrency, TiDBHashAggFinalConcurrency,
		TiDBProjectionConcurrency, TiDBWindowConcurrency, TiDBMergeJoinConcurrency, TiDBStreamAggConcurrency:
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TiDBExecutorConcurrency))
	case TIDBMemQuotaHashJoin, TIDBMemQuotaMergeJoin,
		TIDBMemQuotaSort, TIDBMemQuotaTopn,
		TIDBMemQuotaIndexLookupReader, TIDBMemQuotaIndexLookupJoin:
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	}
}

// TiDBOptOn could be used for all tidb session variable options, we use "ON"/1 to turn on those options.
func TiDBOptOn(opt string) bool {
	return strings.EqualFold(opt, "ON") || opt == "1"
}

const (
	// OffInt is used by TiDBMultiStatementMode
	OffInt = 0
	// OnInt is used TiDBMultiStatementMode
	OnInt = 1
	// WarnInt is used by TiDBMultiStatementMode
	WarnInt = 2
)

// TiDBOptMultiStmt converts multi-stmt options to int.
func TiDBOptMultiStmt(opt string) int {
	switch opt {
	case Off:
		return OffInt
	case On:
		return OnInt
	}
	return WarnInt
}

// ClusteredIndexDefMode controls the default clustered property for primary key.
type ClusteredIndexDefMode int

const (
	// ClusteredIndexDefModeIntOnly indicates only single int primary key will default be clustered.
	ClusteredIndexDefModeIntOnly ClusteredIndexDefMode = 0
	// ClusteredIndexDefModeOn indicates primary key will default be clustered.
	ClusteredIndexDefModeOn ClusteredIndexDefMode = 1
	// ClusteredIndexDefModeOff indicates primary key will default be non-clustered.
	ClusteredIndexDefModeOff ClusteredIndexDefMode = 2
)

// TiDBOptEnableClustered converts enable clustered options to ClusteredIndexDefMode.
func TiDBOptEnableClustered(opt string) ClusteredIndexDefMode {
	switch opt {
	case On:
		return ClusteredIndexDefModeOn
	case Off:
		return ClusteredIndexDefModeOff
	default:
		return ClusteredIndexDefModeIntOnly
	}
}

func tidbOptPositiveInt32(opt string, defaultVal int) int {
	val, err := strconv.Atoi(opt)
	if err != nil || val <= 0 {
		return defaultVal
	}
	return val
}

func tidbOptInt(opt string, defaultVal int) int {
	val, err := strconv.Atoi(opt)
	if err != nil {
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
