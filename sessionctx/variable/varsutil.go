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

	"github.com/cznic/mathutil"
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
	sysVar := GetSysVar(key)
	if sysVar == nil {
		return "", false, ErrUnknownSystemVar.GenWithStackByArgs(key)
	}
	// For virtual system variables:
	switch sysVar.Name {
	case TiDBCurrentTS:
		return fmt.Sprintf("%d", s.TxnCtx.StartTS), true, nil
	case TiDBLastTxnInfo:
		info, err := json.Marshal(s.LastTxnInfo)
		if err != nil {
			return "", true, err
		}
		return string(info), true, nil
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
func SetSessionSystemVar(vars *SessionVars, name string, value types.Datum) error {
	sysVar := GetSysVar(name)
	if sysVar == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
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
	CheckDeprecationSetSystemVar(vars, name)
	return vars.SetSystemVar(name, sVal)
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

func checkUInt64SystemVar(name, value string, min, max uint64, vars *SessionVars) (string, error) {
	// There are two types of validation behaviors for integer values. The default
	// is to return an error saying the value is out of range. For MySQL compatibility, some
	// values prefer convert the value to the min/max and return a warning.
	sv := GetSysVar(name)
	if sv != nil && !sv.AutoConvertOutOfRange {
		return checkUint64SystemVarWithError(name, value, min, max)
	}
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
	// There are two types of validation behaviors for integer values. The default
	// is to return an error saying the value is out of range. For MySQL compatibility, some
	// values prefer convert the value to the min/max and return a warning.
	sv := GetSysVar(name)
	if sv != nil && !sv.AutoConvertOutOfRange {
		return checkInt64SystemVarWithError(name, value, min, max)
	}
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

func checkEnumSystemVar(name, value string, vars *SessionVars) (string, error) {
	sv := GetSysVar(name)
	// The value could be either a string or the ordinal position in the PossibleValues.
	// This allows for the behavior 0 = OFF, 1 = ON, 2 = DEMAND etc.
	var iStr string
	for i, v := range sv.PossibleValues {
		iStr = fmt.Sprintf("%d", i)
		if strings.EqualFold(value, v) || strings.EqualFold(value, iStr) {
			return v, nil
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
}

func checkFloatSystemVar(name, value string, min, max float64, vars *SessionVars) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min || val > max {
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	}
	return value, nil
}

func checkBoolSystemVar(name, value string, vars *SessionVars) (string, error) {
	if strings.EqualFold(value, "ON") {
		return BoolOn, nil
	} else if strings.EqualFold(value, "OFF") {
		return BoolOff, nil
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		// Confusingly, there are two types of conversion rules for integer values.
		// The default only allows 0 || 1, but a subset of values convert any
		// negative integer to 1.
		sv := GetSysVar(name)
		if !sv.AutoConvertNegativeBool {
			if val == 0 {
				return BoolOff, nil
			} else if val == 1 {
				return BoolOn, nil
			}
		} else {
			if val == 1 || val < 0 {
				return BoolOn, nil
			} else if val == 0 {
				return BoolOff, nil
			}
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
}

func checkUint64SystemVarWithError(name, value string, min, max uint64) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if value[0] == '-' {
		// // in strict it expects the error WrongValue, but in non-strict it returns WrongType
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min || val > max {
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	}
	return value, nil
}

func checkInt64SystemVarWithError(name, value string, min, max int64) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min || val > max {
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	}
	return value, nil
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
		TiDBProjectionConcurrency, TiDBWindowConcurrency:
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TiDBExecutorConcurrency))
	case TIDBMemQuotaHashJoin, TIDBMemQuotaMergeJoin,
		TIDBMemQuotaSort, TIDBMemQuotaTopn,
		TIDBMemQuotaIndexLookupReader, TIDBMemQuotaIndexLookupJoin,
		TIDBMemQuotaNestedLoopApply:
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	}
}

// ValidateSetSystemVar checks if system variable satisfies specific restriction.
func ValidateSetSystemVar(vars *SessionVars, name string, value string, scope ScopeFlag) (string, error) {
	sv := GetSysVar(name)
	// Some sysvars are read-only. Attempting to set should always fail.
	if sv.ReadOnly || sv.Scope == ScopeNone {
		return value, ErrReadOnly.GenWithStackByArgs(name)
	}
	// The string "DEFAULT" is a special keyword in MySQL, which restores
	// the compiled sysvar value. In which case we can skip further validation.
	if strings.EqualFold(value, "DEFAULT") {
		if sv != nil {
			return sv.Value, nil
		}
		return value, ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	// Some sysvars in TiDB have a special behavior where the empty string means
	// "use the config file value". This needs to be cleaned up once the behavior
	// for instance variables is determined.
	if value == "" && ((sv.AllowEmpty && scope == ScopeSession) || sv.AllowEmptyAll) {
		return value, nil
	}
	// Attempt to provide validation using the SysVar struct.
	// Eventually the struct should handle all validation
	var err error
	if sv != nil {
		switch sv.Type {
		case TypeUnsigned:
			value, err = checkUInt64SystemVar(name, value, uint64(sv.MinValue), sv.MaxValue, vars)
		case TypeInt:
			value, err = checkInt64SystemVar(name, value, sv.MinValue, int64(sv.MaxValue), vars)
		case TypeBool:
			value, err = checkBoolSystemVar(name, value, vars)
		case TypeFloat:
			value, err = checkFloatSystemVar(name, value, float64(sv.MinValue), float64(sv.MaxValue), vars)
		case TypeEnum:
			value, err = checkEnumSystemVar(name, value, vars)
		}
		// If there is no error, follow through and handle legacy cases of validation that are not handled by the type.
		// TODO: Move each of these validations into the SysVar as an anonymous function.
		if err != nil {
			return value, err
		}
	}
	switch name {
	case ForeignKeyChecks:
		if TiDBOptOn(value) {
			// TiDB does not yet support foreign keys.
			// For now, resist the change and show a warning.
			vars.StmtCtx.AppendWarning(ErrUnsupportedValueForVar.GenWithStackByArgs(name, value))
			return BoolOff, nil
		} else if !TiDBOptOn(value) {
			return BoolOff, nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case GroupConcatMaxLen:
		// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len
		// Minimum Value 4
		// Maximum Value (64-bit platforms) 18446744073709551615
		// Maximum Value (32-bit platforms) 4294967295
		maxLen := uint64(math.MaxUint64)
		if mathutil.IntBits == 32 {
			maxLen = uint64(math.MaxUint32)
		}
		return checkUInt64SystemVar(name, value, 4, maxLen, vars)
	case TimeZone:
		if strings.EqualFold(value, "SYSTEM") {
			return "SYSTEM", nil
		}
		_, err := parseTimeZone(value)
		return value, err
	case SecureAuth:
		if TiDBOptOn(value) {
			return BoolOn, nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case TiDBOptBCJ:
		if TiDBOptOn(value) && vars.AllowBatchCop == 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs("Can't set Broadcast Join to 1 but tidb_allow_batch_cop is 0, please active batch cop at first.")
		}
		return value, nil
	case TiDBIndexLookupConcurrency,
		TiDBIndexLookupJoinConcurrency,
		TiDBHashJoinConcurrency,
		TiDBHashAggPartialConcurrency,
		TiDBHashAggFinalConcurrency,
		TiDBWindowConcurrency:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v <= 0 && v != ConcurrencyUnset {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TiDBAllowBatchCop:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v == 0 && vars.AllowBCJ {
			return value, ErrWrongValueForVar.GenWithStackByArgs("Can't set batch cop 0 but tidb_opt_broadcast_join is 1, please set tidb_opt_broadcast_join 0 at first")
		}
		if v < 0 || v > 2 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TiDBAutoAnalyzeStartTime, TiDBAutoAnalyzeEndTime, TiDBEvolvePlanTaskStartTime, TiDBEvolvePlanTaskEndTime:
		v, err := setDayTime(vars, value)
		if err != nil {
			return "", err
		}
		return v, nil
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
	case TiDBTxnMode:
		switch strings.ToUpper(value) {
		case ast.Pessimistic, ast.Optimistic, "":
		default:
			return value, ErrWrongValueForVar.GenWithStackByArgs(TiDBTxnMode, value)
		}
	case TiDBPartitionPruneMode:
		if !PartitionPruneMode(value).Valid() {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
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
