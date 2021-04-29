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

package variable

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	tikvstore "github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/versioninfo"
	atomic2 "go.uber.org/atomic"
)

// ScopeFlag is for system variable whether can be changed in global/session dynamically or not.
type ScopeFlag uint8

// TypeFlag is the SysVar type, which doesn't exactly match MySQL types.
type TypeFlag byte

const (
	// ScopeNone means the system variable can not be changed dynamically.
	ScopeNone ScopeFlag = 0
	// ScopeGlobal means the system variable can be changed globally.
	ScopeGlobal ScopeFlag = 1 << 0
	// ScopeSession means the system variable can only be changed in current session.
	ScopeSession ScopeFlag = 1 << 1

	// TypeStr is the default
	TypeStr TypeFlag = 0
	// TypeBool for boolean
	TypeBool TypeFlag = 1
	// TypeInt for integer
	TypeInt TypeFlag = 2
	// TypeEnum for Enum
	TypeEnum TypeFlag = 3
	// TypeFloat for Double
	TypeFloat TypeFlag = 4
	// TypeUnsigned for Unsigned integer
	TypeUnsigned TypeFlag = 5
	// TypeTime for time of day (a TiDB extension)
	TypeTime TypeFlag = 6
	// TypeDuration for a golang duration (a TiDB extension)
	TypeDuration TypeFlag = 7

	// On is the canonical string for ON
	On = "ON"
	// Off is the canonical string for OFF
	Off = "OFF"
	// Warn means return warnings
	Warn = "WARN"
	// IntOnly means enable for int type
	IntOnly = "INT_ONLY"
)

// SysVar is for system variable.
type SysVar struct {
	// Scope is for whether can be changed or not
	Scope ScopeFlag
	// Name is the variable name.
	Name string
	// Value is the variable value.
	Value string
	// Type is the MySQL type (optional)
	Type TypeFlag
	// MinValue will automatically be validated when specified (optional)
	MinValue int64
	// MaxValue will automatically be validated when specified (optional)
	MaxValue uint64
	// AutoConvertNegativeBool applies to boolean types (optional)
	AutoConvertNegativeBool bool
	// AutoConvertOutOfRange applies to int and unsigned types.
	AutoConvertOutOfRange bool
	// ReadOnly applies to all types
	ReadOnly bool
	// PossibleValues applies to ENUM type
	PossibleValues []string
	// AllowEmpty is a special TiDB behavior which means "read value from config" (do not use)
	AllowEmpty bool
	// AllowEmptyAll is a special behavior that only applies to TiDBCapturePlanBaseline, TiDBTxnMode (do not use)
	AllowEmptyAll bool
	// AllowAutoValue means that the special value "-1" is permitted, even when outside of range.
	AllowAutoValue bool
	// Validation is a callback after the type validation has been performed
	Validation func(*SessionVars, string, string, ScopeFlag) (string, error)
	// SetSession is called after validation
	SetSession func(*SessionVars, string) error
	// IsHintUpdatable indicate whether it's updatable via SET_VAR() hint (optional)
	IsHintUpdatable bool
	// Hidden means that it still responds to SET but doesn't show up in SHOW VARIABLES
	Hidden bool
}

// SetSessionFromHook calls the SetSession func if it exists.
func (sv *SysVar) SetSessionFromHook(s *SessionVars, val string) error {
	if sv.SetSession != nil {
		return sv.SetSession(s, val)
	}
	return nil
}

// Validate checks if system variable satisfies specific restriction.
func (sv *SysVar) Validate(vars *SessionVars, value string, scope ScopeFlag) (string, error) {
	// Normalize the value and apply validation based on type.
	// i.e. TypeBool converts 1/on/ON to ON.
	normalizedValue, err := sv.validateFromType(vars, value, scope)
	if err != nil {
		return normalizedValue, err
	}
	// If type validation was successful, call the (optional) validation function
	if sv.Validation != nil {
		return sv.Validation(vars, normalizedValue, value, scope)
	}
	return normalizedValue, nil
}

// validateFromType provides automatic validation based on the SysVar's type
func (sv *SysVar) validateFromType(vars *SessionVars, value string, scope ScopeFlag) (string, error) {
	// Some sysvars are read-only. Attempting to set should always fail.
	if sv.ReadOnly || sv.Scope == ScopeNone {
		return value, ErrIncorrectScope.GenWithStackByArgs(sv.Name, "read only")
	}
	// The string "DEFAULT" is a special keyword in MySQL, which restores
	// the compiled sysvar value. In which case we can skip further validation.
	if strings.EqualFold(value, "DEFAULT") {
		return sv.Value, nil
	}
	// Some sysvars in TiDB have a special behavior where the empty string means
	// "use the config file value". This needs to be cleaned up once the behavior
	// for instance variables is determined.
	if value == "" && ((sv.AllowEmpty && scope == ScopeSession) || sv.AllowEmptyAll) {
		return value, nil
	}
	// Provide validation using the SysVar struct
	switch sv.Type {
	case TypeUnsigned:
		return sv.checkUInt64SystemVar(value, vars)
	case TypeInt:
		return sv.checkInt64SystemVar(value, vars)
	case TypeBool:
		return sv.checkBoolSystemVar(value, vars)
	case TypeFloat:
		return sv.checkFloatSystemVar(value, vars)
	case TypeEnum:
		return sv.checkEnumSystemVar(value, vars)
	case TypeTime:
		return sv.checkTimeSystemVar(value, vars)
	case TypeDuration:
		return sv.checkDurationSystemVar(value, vars)
	}
	return value, nil // typeString
}

const (
	localDayTimeFormat = "15:04"
	// FullDayTimeFormat is the full format of analyze start time and end time.
	FullDayTimeFormat = "15:04 -0700"
)

func (sv *SysVar) checkTimeSystemVar(value string, vars *SessionVars) (string, error) {
	var t time.Time
	var err error
	if len(value) <= len(localDayTimeFormat) {
		t, err = time.ParseInLocation(localDayTimeFormat, value, vars.TimeZone)
	} else {
		t, err = time.ParseInLocation(FullDayTimeFormat, value, vars.TimeZone)
	}
	if err != nil {
		return "", err
	}
	return t.Format(FullDayTimeFormat), nil
}

func (sv *SysVar) checkDurationSystemVar(value string, vars *SessionVars) (string, error) {
	d, err := time.ParseDuration(value)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	// Check for min/max violations
	if int64(d) < sv.MinValue {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if uint64(d) > sv.MaxValue {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	// return a string representation of the duration
	return d.String(), nil
}

func (sv *SysVar) checkUInt64SystemVar(value string, vars *SessionVars) (string, error) {
	if sv.AllowAutoValue && value == "-1" {
		return value, nil
	}
	// There are two types of validation behaviors for integer values. The default
	// is to return an error saying the value is out of range. For MySQL compatibility, some
	// values prefer convert the value to the min/max and return a warning.
	if !sv.AutoConvertOutOfRange {
		return sv.checkUint64SystemVarWithError(value)
	}
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if value[0] == '-' {
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
		}
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < uint64(sv.MinValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	if val > sv.MaxValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MaxValue), nil
	}
	return value, nil
}

func (sv *SysVar) checkInt64SystemVar(value string, vars *SessionVars) (string, error) {
	if sv.AllowAutoValue && value == "-1" {
		return value, nil
	}
	// There are two types of validation behaviors for integer values. The default
	// is to return an error saying the value is out of range. For MySQL compatibility, some
	// values prefer convert the value to the min/max and return a warning.
	if !sv.AutoConvertOutOfRange {
		return sv.checkInt64SystemVarWithError(value)
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < sv.MinValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	if val > int64(sv.MaxValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MaxValue), nil
	}
	return value, nil
}

func (sv *SysVar) checkEnumSystemVar(value string, vars *SessionVars) (string, error) {
	// The value could be either a string or the ordinal position in the PossibleValues.
	// This allows for the behavior 0 = OFF, 1 = ON, 2 = DEMAND etc.
	var iStr string
	for i, v := range sv.PossibleValues {
		iStr = fmt.Sprintf("%d", i)
		if strings.EqualFold(value, v) || strings.EqualFold(value, iStr) {
			return v, nil
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
}

func (sv *SysVar) checkFloatSystemVar(value string, vars *SessionVars) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < float64(sv.MinValue) || val > float64(sv.MaxValue) {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

func (sv *SysVar) checkBoolSystemVar(value string, vars *SessionVars) (string, error) {
	if strings.EqualFold(value, "ON") {
		return On, nil
	} else if strings.EqualFold(value, "OFF") {
		return Off, nil
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		// There are two types of conversion rules for integer values.
		// The default only allows 0 || 1, but a subset of values convert any
		// negative integer to 1.
		if !sv.AutoConvertNegativeBool {
			if val == 0 {
				return Off, nil
			} else if val == 1 {
				return On, nil
			}
		} else {
			if val == 1 || val < 0 {
				return On, nil
			} else if val == 0 {
				return Off, nil
			}
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
}

func (sv *SysVar) checkUint64SystemVarWithError(value string) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if value[0] == '-' {
		// // in strict it expects the error WrongValue, but in non-strict it returns WrongType
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < uint64(sv.MinValue) || val > sv.MaxValue {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

func (sv *SysVar) checkInt64SystemVarWithError(value string) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < sv.MinValue || val > int64(sv.MaxValue) {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

// GetNativeValType attempts to convert the val to the approx MySQL non-string type
func (sv *SysVar) GetNativeValType(val string) (types.Datum, byte, uint) {
	switch sv.Type {
	case TypeUnsigned:
		u, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			u = 0
		}
		return types.NewUintDatum(u), mysql.TypeLonglong, mysql.UnsignedFlag
	case TypeBool:
		optVal := int64(0) // OFF
		if TiDBOptOn(val) {
			optVal = 1
		}
		return types.NewIntDatum(optVal), mysql.TypeLong, 0
	}
	return types.NewStringDatum(val), mysql.TypeVarString, 0
}

var sysVars map[string]*SysVar
var sysVarsLock sync.RWMutex

// RegisterSysVar adds a sysvar to the SysVars list
func RegisterSysVar(sv *SysVar) {
	name := strings.ToLower(sv.Name)
	sysVarsLock.Lock()
	sysVars[name] = sv
	sysVarsLock.Unlock()
}

// UnregisterSysVar removes a sysvar from the SysVars list
// currently only used in tests.
func UnregisterSysVar(name string) {
	name = strings.ToLower(name)
	sysVarsLock.Lock()
	delete(sysVars, name)
	sysVarsLock.Unlock()
}

// GetSysVar returns sys var info for name as key.
func GetSysVar(name string) *SysVar {
	name = strings.ToLower(name)
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()
	return sysVars[name]
}

// SetSysVar sets a sysvar. This will not propagate to the cluster, so it should only be
// used for instance scoped AUTO variables such as system_time_zone.
func SetSysVar(name string, value string) {
	name = strings.ToLower(name)
	sysVarsLock.Lock()
	defer sysVarsLock.Unlock()
	sysVars[name].Value = value
}

// GetSysVars returns the sysVars list under a RWLock
func GetSysVars() map[string]*SysVar {
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()
	return sysVars
}

// PluginVarNames is global plugin var names set.
var PluginVarNames []string

func init() {
	sysVars = make(map[string]*SysVar)
	for _, v := range defaultSysVars {
		RegisterSysVar(v)
	}
	for _, v := range noopSysVars {
		RegisterSysVar(v)
	}
	initSynonymsSysVariables()
}

var defaultSysVars = []*SysVar{
	{Scope: ScopeGlobal, Name: MaxConnections, Value: "151", Type: TypeUnsigned, MinValue: 1, MaxValue: 100000, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLSelectLimit, Value: "18446744073709551615", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		result, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		s.SelectLimit = result
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: DefaultWeekFormat, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 7, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLModeVar, Value: mysql.DefaultSQLMode, IsHintUpdatable: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		// Ensure the SQL mode parses
		normalizedValue = mysql.FormatSQLModeStr(normalizedValue)
		if _, err := mysql.GetSQLMode(normalizedValue); err != nil {
			return originalValue, err
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		val = mysql.FormatSQLModeStr(val)
		// Modes is a list of different modes separated by commas.
		sqlMode, err := mysql.GetSQLMode(val)
		if err != nil {
			return errors.Trace(err)
		}
		s.StrictSQLMode = sqlMode.HasStrictMode()
		s.SQLMode = sqlMode
		s.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxExecutionTime, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true, IsHintUpdatable: true, SetSession: func(s *SessionVars, val string) error {
		timeoutMS := tidbOptPositiveInt32(val, 0)
		s.MaxExecutionTime = uint64(timeoutMS)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationServer, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[CharacterSetServer] = coll.CharsetName
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLLogBin, Value: On, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TimeZone, Value: "SYSTEM", IsHintUpdatable: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if strings.EqualFold(normalizedValue, "SYSTEM") {
			return "SYSTEM", nil
		}
		_, err := parseTimeZone(normalizedValue)
		return normalizedValue, err
	}, SetSession: func(s *SessionVars, val string) error {
		tz, err := parseTimeZone(val)
		if err != nil {
			return err
		}
		s.TimeZone = tz
		return nil
	}},
	{Scope: ScopeNone, Name: SystemTimeZone, Value: "CST"},
	{Scope: ScopeGlobal | ScopeSession, Name: ForeignKeyChecks, Value: Off, Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) {
			// TiDB does not yet support foreign keys.
			// Return the original value in the warning, so that users are not confused.
			vars.StmtCtx.AppendWarning(ErrUnsupportedValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue))
			return Off, nil
		} else if !TiDBOptOn(normalizedValue) {
			return Off, nil
		}
		return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue)
	}},
	{Scope: ScopeNone, Name: Hostname, Value: ServerHostname},
	{Scope: ScopeSession, Name: Timestamp, Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetFilesystem, Value: "binary", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharacterSetFilesystem)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationDatabase, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[CharsetDatabase] = coll.CharsetName
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoIncrementIncrement, Value: strconv.FormatInt(DefAutoIncrementIncrement, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		// AutoIncrementIncrement is valid in [1, 65535].
		s.AutoIncrementIncrement = tidbOptPositiveInt32(val, DefAutoIncrementIncrement)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoIncrementOffset, Value: strconv.FormatInt(DefAutoIncrementOffset, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		// AutoIncrementOffset is valid in [1, 65535].
		s.AutoIncrementOffset = tidbOptPositiveInt32(val, DefAutoIncrementOffset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetClient, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharacterSetClient)
	}},
	{Scope: ScopeNone, Name: Port, Value: "4000", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: ScopeNone, Name: LowerCaseTableNames, Value: "2"},
	{Scope: ScopeNone, Name: LogBin, Value: Off, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetResults, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "" {
			return normalizedValue, nil
		}
		return checkCharacterSet(normalizedValue, "")
	}},
	{Scope: ScopeNone, Name: VersionComment, Value: "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 5.7 compatible"},
	{Scope: ScopeGlobal | ScopeSession, Name: TxnIsolation, Value: "REPEATABLE-READ", Type: TypeEnum, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "SERIALIZABLE" || normalizedValue == "READ-UNCOMMITTED" {
			if skipIsolationLevelCheck, err := GetSessionSystemVar(vars, TiDBSkipIsolationLevelCheck); err != nil {
				return normalizedValue, err
			} else if !TiDBOptOn(skipIsolationLevelCheck) {
				return normalizedValue, ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
			}
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TransactionIsolation, Value: "REPEATABLE-READ", Type: TypeEnum, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "SERIALIZABLE" || normalizedValue == "READ-UNCOMMITTED" {
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
			if skipIsolationLevelCheck, err := GetSessionSystemVar(vars, TiDBSkipIsolationLevelCheck); err != nil {
				return normalizedValue, err
			} else if !TiDBOptOn(skipIsolationLevelCheck) {
				return normalizedValue, returnErr
			}
			vars.StmtCtx.AppendWarning(returnErr)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationConnection, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[CharacterSetConnection] = coll.CharsetName
		}
		return nil
	}},
	{Scope: ScopeNone, Name: Version, Value: mysql.ServerVersion},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoCommit, Value: On, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		isAutocommit := TiDBOptOn(val)
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			s.SetInTxn(false)
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharsetDatabase, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharsetDatabase)
	}, SetSession: func(s *SessionVars, val string) error {
		if _, coll, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[CollationDatabase] = coll
		}
		return nil
	}},
	{Scope: ScopeGlobal, Name: MaxPreparedStmtCount, Value: strconv.FormatInt(DefMaxPreparedStmtCount, 10), Type: TypeInt, MinValue: -1, MaxValue: 1048576, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: DataDir, Value: "/usr/local/mysql/data/"},
	{Scope: ScopeGlobal | ScopeSession, Name: WaitTimeout, Value: strconv.FormatInt(DefWaitTimeout, 10), Type: TypeUnsigned, MinValue: 0, MaxValue: secondsPerYear, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: InteractiveTimeout, Value: "28800", Type: TypeUnsigned, MinValue: 1, MaxValue: secondsPerYear, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: InnodbLockWaitTimeout, Value: strconv.FormatInt(DefInnodbLockWaitTimeout, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 1073741824, AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		lockWaitSec := tidbOptInt64(val, DefInnodbLockWaitTimeout)
		s.LockWaitTimeout = lockWaitSec * 1000
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: GroupConcatMaxLen, Value: "1024", AutoConvertOutOfRange: true, IsHintUpdatable: true, Type: TypeUnsigned, MinValue: 4, MaxValue: math.MaxUint64, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len
		// Minimum Value 4
		// Maximum Value (64-bit platforms) 18446744073709551615
		// Maximum Value (32-bit platforms) 4294967295
		if mathutil.IntBits == 32 {
			if val, err := strconv.ParseUint(normalizedValue, 10, 64); err == nil {
				if val > uint64(math.MaxUint32) {
					vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(GroupConcatMaxLen, originalValue))
					return fmt.Sprintf("%d", math.MaxUint32), nil
				}
			}
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeNone, Name: Socket, Value: "/tmp/myssock"},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetConnection, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharacterSetConnection)
	}, SetSession: func(s *SessionVars, val string) error {
		if _, coll, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[CollationConnection] = coll
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetServer, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, CharacterSetServer)
	}, SetSession: func(s *SessionVars, val string) error {
		if _, coll, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[CollationServer] = coll
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxAllowedPacket, Value: "67108864", Type: TypeUnsigned, MinValue: 1024, MaxValue: MaxOfMaxAllowedPacket, AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: WarningCount, Value: "0", ReadOnly: true},
	{Scope: ScopeSession, Name: ErrorCount, Value: "0", ReadOnly: true},
	{Scope: ScopeGlobal | ScopeSession, Name: WindowingUseHighPrecision, Value: On, Type: TypeBool, IsHintUpdatable: true, SetSession: func(s *SessionVars, val string) error {
		s.WindowingUseHighPrecision = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeNone, Name: "license", Value: "Apache License 2.0"},
	{Scope: ScopeGlobal | ScopeSession, Name: BlockEncryptionMode, Value: "aes-128-ecb"},
	{Scope: ScopeSession, Name: "last_insert_id", Value: ""},
	{Scope: ScopeNone, Name: "have_ssl", Value: "DISABLED"},
	{Scope: ScopeNone, Name: "have_openssl", Value: "DISABLED"},
	{Scope: ScopeNone, Name: "ssl_ca", Value: ""},
	{Scope: ScopeNone, Name: "ssl_cert", Value: ""},
	{Scope: ScopeNone, Name: "ssl_key", Value: ""},
	{Scope: ScopeGlobal, Name: InitConnect, Value: ""},

	/* TiDB specific variables */
	{Scope: ScopeSession, Name: TiDBTxnScope, Value: func() string {
		if isGlobal, _ := config.GetTxnScopeFromConfig(); isGlobal {
			return oracle.GlobalTxnScope
		}
		return oracle.LocalTxnScope
	}(), SetSession: func(s *SessionVars, val string) error {
		switch val {
		case oracle.GlobalTxnScope:
			s.TxnScope = oracle.NewGlobalTxnScope()
		case oracle.LocalTxnScope:
			s.TxnScope = oracle.GetTxnScope()
		default:
			return ErrWrongValueForVar.GenWithStack("@@txn_scope value should be global or local")
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowMPPExecution, Type: TypeBool, Value: BoolToOnOff(DefTiDBAllowMPPExecution), SetSession: func(s *SessionVars, val string) error {
		s.AllowMPPExecution = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBCJThresholdCount, Value: strconv.Itoa(DefBroadcastJoinThresholdCount), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.BroadcastJoinThresholdCount = tidbOptInt64(val, DefBroadcastJoinThresholdCount)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBCJThresholdSize, Value: strconv.Itoa(DefBroadcastJoinThresholdSize), Type: TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.BroadcastJoinThresholdSize = tidbOptInt64(val, DefBroadcastJoinThresholdSize)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBSnapshot, Value: "", SetSession: func(s *SessionVars, val string) error {
		err := setSnapshotTS(s, val)
		if err != nil {
			return err
		}
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptAggPushDown, Value: BoolToOnOff(DefOptAggPushDown), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptBCJ, Value: BoolToOnOff(DefOptBCJ), Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) && vars.AllowBatchCop == 0 {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("Can't set Broadcast Join to 1 but tidb_allow_batch_cop is 0, please active batch cop at first.")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.AllowBCJ = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptDistinctAggPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.DistinctAggPushDown), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowDistinctAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptWriteRowID, Value: BoolToOnOff(DefOptWriteRowID), SetSession: func(s *SessionVars, val string) error {
		s.AllowWriteRowID = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBuildStatsConcurrency, Value: strconv.Itoa(DefBuildStatsConcurrency)},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeRatio, Value: strconv.FormatFloat(DefAutoAnalyzeRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeStartTime, Value: DefAutoAnalyzeStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeEndTime, Value: DefAutoAnalyzeEndTime, Type: TypeTime},
	{Scope: ScopeSession, Name: TiDBChecksumTableConcurrency, Value: strconv.Itoa(DefChecksumTableConcurrency)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBExecutorConcurrency, Value: strconv.Itoa(DefExecutorConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.ExecutorConcurrency = tidbOptPositiveInt32(val, DefExecutorConcurrency)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDistSQLScanConcurrency, Value: strconv.Itoa(DefDistSQLScanConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.distSQLScanConcurrency = tidbOptPositiveInt32(val, DefDistSQLScanConcurrency)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptInSubqToJoinAndAgg, Value: BoolToOnOff(DefOptInSubqToJoinAndAgg), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetAllowInSubqToJoinAndAgg(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptPreferRangeScan, Value: BoolToOnOff(DefOptPreferRangeScan), Type: TypeBool, IsHintUpdatable: true, SetSession: func(s *SessionVars, val string) error {
		s.SetAllowPreferRangeScan(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCorrelationThreshold, Value: strconv.FormatFloat(DefOptCorrelationThreshold, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.CorrelationThreshold = tidbOptFloat64(val, DefOptCorrelationThreshold)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCorrelationExpFactor, Value: strconv.Itoa(DefOptCorrelationExpFactor), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CorrelationExpFactor = int(tidbOptInt64(val, DefOptCorrelationExpFactor))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCPUFactor, Value: strconv.FormatFloat(DefOptCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CPUFactor = tidbOptFloat64(val, DefOptCPUFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptTiFlashConcurrencyFactor, Value: strconv.FormatFloat(DefOptTiFlashConcurrencyFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CopTiFlashConcurrencyFactor = tidbOptFloat64(val, DefOptTiFlashConcurrencyFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCopCPUFactor, Value: strconv.FormatFloat(DefOptCopCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CopCPUFactor = tidbOptFloat64(val, DefOptCopCPUFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptNetworkFactor, Value: strconv.FormatFloat(DefOptNetworkFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.NetworkFactor = tidbOptFloat64(val, DefOptNetworkFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptScanFactor, Value: strconv.FormatFloat(DefOptScanFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.ScanFactor = tidbOptFloat64(val, DefOptScanFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDescScanFactor, Value: strconv.FormatFloat(DefOptDescScanFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.DescScanFactor = tidbOptFloat64(val, DefOptDescScanFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptSeekFactor, Value: strconv.FormatFloat(DefOptSeekFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.SeekFactor = tidbOptFloat64(val, DefOptSeekFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptMemoryFactor, Value: strconv.FormatFloat(DefOptMemoryFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.MemoryFactor = tidbOptFloat64(val, DefOptMemoryFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDiskFactor, Value: strconv.FormatFloat(DefOptDiskFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.DiskFactor = tidbOptFloat64(val, DefOptDiskFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptConcurrencyFactor, Value: strconv.FormatFloat(DefOptConcurrencyFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.ConcurrencyFactor = tidbOptFloat64(val, DefOptConcurrencyFactor)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexJoinBatchSize, Value: strconv.Itoa(DefIndexJoinBatchSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexJoinBatchSize = tidbOptPositiveInt32(val, DefIndexJoinBatchSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupSize, Value: strconv.Itoa(DefIndexLookupSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexLookupSize = tidbOptPositiveInt32(val, DefIndexLookupSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupConcurrency, Value: strconv.Itoa(DefIndexLookupConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.indexLookupConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupJoinConcurrency, Value: strconv.Itoa(DefIndexLookupJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.indexLookupJoinConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexSerialScanConcurrency, Value: strconv.Itoa(DefIndexSerialScanConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.indexSerialScanConcurrency = tidbOptPositiveInt32(val, DefIndexSerialScanConcurrency)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipUTF8Check, Value: BoolToOnOff(DefSkipUTF8Check), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipUTF8Check = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipASCIICheck, Value: BoolToOnOff(DefSkipASCIICheck), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipASCIICheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBBatchInsert, Value: BoolToOnOff(DefBatchInsert), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBBatchDelete, Value: BoolToOnOff(DefBatchDelete), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchDelete = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBBatchCommit, Value: BoolToOnOff(DefBatchCommit), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDMLBatchSize, Value: strconv.Itoa(DefDMLBatchSize), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.DMLBatchSize = int(tidbOptInt64(val, DefOptCorrelationExpFactor))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBCurrentTS, Value: strconv.Itoa(DefCurretTS), ReadOnly: true},
	{Scope: ScopeSession, Name: TiDBLastTxnInfo, Value: strconv.Itoa(DefCurretTS), ReadOnly: true},
	{Scope: ScopeSession, Name: TiDBLastQueryInfo, Value: strconv.Itoa(DefCurretTS), ReadOnly: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxChunkSize, Value: strconv.Itoa(DefMaxChunkSize), Type: TypeUnsigned, MinValue: maxChunkSizeLowerBound, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.MaxChunkSize = tidbOptPositiveInt32(val, DefMaxChunkSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowBatchCop, Value: strconv.Itoa(DefTiDBAllowBatchCop), Type: TypeInt, MinValue: 0, MaxValue: 2, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "0" && vars.AllowBCJ {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("Can't set batch cop 0 but tidb_opt_broadcast_join is 1, please set tidb_opt_broadcast_join 0 at first")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.AllowBatchCop = int(tidbOptInt64(val, DefTiDBAllowBatchCop))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBInitChunkSize, Value: strconv.Itoa(DefInitChunkSize), Type: TypeUnsigned, MinValue: 1, MaxValue: initChunkSizeUpperBound, SetSession: func(s *SessionVars, val string) error {
		s.InitChunkSize = tidbOptPositiveInt32(val, DefInitChunkSize)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableCascadesPlanner, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetEnableCascadesPlanner(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableIndexMerge, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetEnableIndexMerge(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeSession, Name: TIDBMemQuotaQuery, Value: strconv.FormatInt(config.GetGlobalConfig().MemQuotaQuery, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaQuery = tidbOptInt64(val, config.GetGlobalConfig().MemQuotaQuery)
		return nil
	}},
	{Scope: ScopeSession, Name: TIDBMemQuotaHashJoin, Value: strconv.FormatInt(DefTiDBMemQuotaHashJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaHashJoin = tidbOptInt64(val, DefTiDBMemQuotaHashJoin)
		return nil
	}},
	{Scope: ScopeSession, Name: TIDBMemQuotaMergeJoin, Value: strconv.FormatInt(DefTiDBMemQuotaMergeJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaMergeJoin = tidbOptInt64(val, DefTiDBMemQuotaMergeJoin)
		return nil
	}},
	{Scope: ScopeSession, Name: TIDBMemQuotaSort, Value: strconv.FormatInt(DefTiDBMemQuotaSort, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaSort = tidbOptInt64(val, DefTiDBMemQuotaSort)
		return nil
	}},
	{Scope: ScopeSession, Name: TIDBMemQuotaTopn, Value: strconv.FormatInt(DefTiDBMemQuotaTopn, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaTopn = tidbOptInt64(val, DefTiDBMemQuotaTopn)
		return nil
	}},
	{Scope: ScopeSession, Name: TIDBMemQuotaIndexLookupReader, Value: strconv.FormatInt(DefTiDBMemQuotaIndexLookupReader, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaIndexLookupReader = tidbOptInt64(val, DefTiDBMemQuotaIndexLookupReader)
		return nil
	}},
	{Scope: ScopeSession, Name: TIDBMemQuotaIndexLookupJoin, Value: strconv.FormatInt(DefTiDBMemQuotaIndexLookupJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaIndexLookupJoin = tidbOptInt64(val, DefTiDBMemQuotaIndexLookupJoin)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBEnableStreaming, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableStreaming = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBEnableChunkRPC, Value: On, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableChunkRPC = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TxnIsolationOneShot, Value: "", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "SERIALIZABLE" || normalizedValue == "READ-UNCOMMITTED" {
			if skipIsolationLevelCheck, err := GetSessionSystemVar(vars, TiDBSkipIsolationLevelCheck); err != nil {
				return normalizedValue, err
			} else if !TiDBOptOn(skipIsolationLevelCheck) {
				return normalizedValue, ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
			}
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.txnIsolationLevelOneShot.state = oneShotSet
		s.txnIsolationLevelOneShot.value = val
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableTablePartition, Value: On, Type: TypeEnum, PossibleValues: []string{Off, On, "AUTO"}, SetSession: func(s *SessionVars, val string) error {
		s.EnableTablePartition = val
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBEnableListTablePartition, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableListTablePartition = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashJoinConcurrency, Value: strconv.Itoa(DefTiDBHashJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashJoinConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBProjectionConcurrency, Value: strconv.Itoa(DefTiDBProjectionConcurrency), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.projectionConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashAggPartialConcurrency, Value: strconv.Itoa(DefTiDBHashAggPartialConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashAggPartialConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashAggFinalConcurrency, Value: strconv.Itoa(DefTiDBHashAggFinalConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.hashAggFinalConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBWindowConcurrency, Value: strconv.Itoa(DefTiDBWindowConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.windowConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMergeJoinConcurrency, Value: strconv.Itoa(DefTiDBMergeJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.mergeJoinConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStreamAggConcurrency, Value: strconv.Itoa(DefTiDBStreamAggConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true, SetSession: func(s *SessionVars, val string) error {
		s.streamAggConcurrency = tidbOptPositiveInt32(val, ConcurrencyUnset)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableParallelApply, Value: BoolToOnOff(DefTiDBEnableParallelApply), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableParallelApply = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMemQuotaApplyCache, Value: strconv.Itoa(DefTiDBMemQuotaApplyCache), SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaApplyCache = tidbOptInt64(val, DefTiDBMemQuotaApplyCache)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBackoffLockFast, Value: strconv.Itoa(tikvstore.DefBackoffLockFast), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.KVVars.BackoffLockFast = tidbOptPositiveInt32(val, tikvstore.DefBackoffLockFast)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBackOffWeight, Value: strconv.Itoa(tikvstore.DefBackOffWeight), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.KVVars.BackOffWeight = tidbOptPositiveInt32(val, tikvstore.DefBackOffWeight)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRetryLimit, Value: strconv.Itoa(DefTiDBRetryLimit), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.RetryLimit = tidbOptInt64(val, DefTiDBRetryLimit)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDisableTxnAutoRetry, Value: BoolToOnOff(DefTiDBDisableTxnAutoRetry), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.DisableTxnAutoRetry = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBConstraintCheckInPlace, Value: BoolToOnOff(DefTiDBConstraintCheckInPlace), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ConstraintCheckInPlace = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTxnMode, Value: DefTiDBTxnMode, AllowEmptyAll: true, Type: TypeEnum, PossibleValues: []string{"pessimistic", "optimistic"}, SetSession: func(s *SessionVars, val string) error {
		s.TxnMode = strings.ToUpper(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBRowFormatVersion, Value: strconv.Itoa(DefTiDBRowFormatV1), Type: TypeUnsigned, MinValue: 1, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		formatVersion := int(tidbOptInt64(val, DefTiDBRowFormatV1))
		if formatVersion == DefTiDBRowFormatV1 {
			s.RowEncoder.Enable = false
		} else if formatVersion == DefTiDBRowFormatV2 {
			s.RowEncoder.Enable = true
		}
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBOptimizerSelectivityLevel, Value: strconv.Itoa(DefTiDBOptimizerSelectivityLevel), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerSelectivityLevel = tidbOptPositiveInt32(val, DefTiDBOptimizerSelectivityLevel)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableWindowFunction, Value: BoolToOnOff(DefEnableWindowFunction), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableWindowFunction = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableStrictDoubleTypeCheck, Value: BoolToOnOff(DefEnableStrictDoubleTypeCheck), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableStrictDoubleTypeCheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableVectorizedExpression, Value: BoolToOnOff(DefEnableVectorizedExpression), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableVectorizedExpression = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableFastAnalyze, Value: BoolToOnOff(DefTiDBUseFastAnalyze), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableFastAnalyze = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipIsolationLevelCheck, Value: BoolToOnOff(DefTiDBSkipIsolationLevelCheck), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableRateLimitAction, Value: BoolToOnOff(DefTiDBEnableRateLimitAction), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnabledRateLimitAction = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowFallbackToTiKV, Value: "", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "" {
			return "", nil
		}
		engines := strings.Split(normalizedValue, ",")
		var formatVal string
		storeTypes := make(map[kv.StoreType]struct{})
		for i, engine := range engines {
			engine = strings.TrimSpace(engine)
			switch {
			case strings.EqualFold(engine, kv.TiFlash.Name()):
				if _, ok := storeTypes[kv.TiFlash]; !ok {
					if i != 0 {
						formatVal += ","
					}
					formatVal += kv.TiFlash.Name()
					storeTypes[kv.TiFlash] = struct{}{}
				}
			default:
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(TiDBAllowFallbackToTiKV, normalizedValue)
			}
		}
		return formatVal, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.AllowFallbackToTiKV = make(map[kv.StoreType]struct{})
		for _, engine := range strings.Split(val, ",") {
			switch engine {
			case kv.TiFlash.Name():
				s.AllowFallbackToTiKV[kv.TiFlash] = struct{}{}
			}
		}
		return nil
	}},
	/* The following variable is defined as session scope but is actually server scope. */
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableDynamicPrivileges, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableDynamicPrivileges = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBGeneralLog, Value: BoolToOnOff(DefTiDBGeneralLog), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		ProcessGeneralLog.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBPProfSQLCPU, Value: strconv.Itoa(DefTiDBPProfSQLCPU), Type: TypeInt, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		EnablePProfSQLCPU.Store(uint32(tidbOptPositiveInt32(val, DefTiDBPProfSQLCPU)) > 0)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBDDLSlowOprThreshold, Value: strconv.Itoa(DefTiDBDDLSlowOprThreshold), SetSession: func(s *SessionVars, val string) error {
		atomic.StoreUint32(&DDLSlowOprThreshold, uint32(tidbOptPositiveInt32(val, DefTiDBDDLSlowOprThreshold)))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBConfig, Value: "", ReadOnly: true},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgWorkerCount, Value: strconv.Itoa(DefTiDBDDLReorgWorkerCount), Type: TypeUnsigned, MinValue: 1, MaxValue: uint64(maxDDLReorgWorkerCount)},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgBatchSize, Value: strconv.Itoa(DefTiDBDDLReorgBatchSize), Type: TypeUnsigned, MinValue: int64(MinDDLReorgBatchSize), MaxValue: uint64(MaxDDLReorgBatchSize), AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: TiDBDDLErrorCountLimit, Value: strconv.Itoa(DefTiDBDDLErrorCountLimit), Type: TypeUnsigned, MinValue: 0, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: TiDBDDLReorgPriority, Value: "PRIORITY_LOW", SetSession: func(s *SessionVars, val string) error {
		s.setDDLReorgPriority(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBMaxDeltaSchemaCount, Value: strconv.Itoa(DefTiDBMaxDeltaSchemaCount), Type: TypeUnsigned, MinValue: 100, MaxValue: 16384, AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		// It's a global variable, but it also wants to be cached in server.
		SetMaxDeltaSchemaCount(tidbOptInt64(val, DefTiDBMaxDeltaSchemaCount))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableChangeColumnType, Value: BoolToOnOff(DefTiDBChangeColumnType), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableChangeColumnType = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableChangeMultiSchema, Value: BoolToOnOff(DefTiDBChangeMultiSchema), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableChangeMultiSchema = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnablePointGetCache, Value: BoolToOnOff(DefTiDBPointGetCache), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePointGetCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableAlterPlacement, Value: BoolToOnOff(DefTiDBEnableAlterPlacement), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAlterPlacement = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBForcePriority, Value: mysql.Priority2Str[DefTiDBForcePriority], SetSession: func(s *SessionVars, val string) error {
		atomic.StoreInt32(&ForcePriority, int32(mysql.Str2Priority(val)))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBEnableRadixJoin, Value: BoolToOnOff(DefTiDBUseRadixJoin), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableRadixJoin = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptJoinReorderThreshold, Value: strconv.Itoa(DefTiDBOptJoinReorderThreshold), Type: TypeUnsigned, MinValue: 0, MaxValue: 63, SetSession: func(s *SessionVars, val string) error {
		s.TiDBOptJoinReorderThreshold = tidbOptPositiveInt32(val, DefTiDBOptJoinReorderThreshold)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBSlowQueryFile, Value: "", SetSession: func(s *SessionVars, val string) error {
		s.SlowQueryFile = val
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBScatterRegion, Value: BoolToOnOff(DefTiDBScatterRegion), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBWaitSplitRegionFinish, Value: BoolToOnOff(DefTiDBWaitSplitRegionFinish), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.WaitSplitRegionFinish = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBWaitSplitRegionTimeout, Value: strconv.Itoa(DefWaitSplitRegionTimeout), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.WaitSplitRegionTimeout = uint64(tidbOptPositiveInt32(val, DefWaitSplitRegionTimeout))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBLowResolutionTSO, Value: Off, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.LowResolutionTSO = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBExpensiveQueryTimeThreshold, Value: strconv.Itoa(DefTiDBExpensiveQueryTimeThreshold), Type: TypeUnsigned, MinValue: int64(MinExpensiveQueryTimeThreshold), MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		atomic.StoreUint64(&ExpensiveQueryTimeThreshold, uint64(tidbOptPositiveInt32(val, DefTiDBExpensiveQueryTimeThreshold)))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBMemoryUsageAlarmRatio, Value: strconv.FormatFloat(config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0.0, MaxValue: 1.0, SetSession: func(s *SessionVars, val string) error {
		MemoryUsageAlarmRatio.Store(tidbOptFloat64(val, 0.8))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNoopFuncs, Value: BoolToOnOff(DefTiDBEnableNoopFuncs), Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {

		// The behavior is very weird if someone can turn TiDBEnableNoopFuncs OFF, but keep any of the following on:
		// TxReadOnly, TransactionReadOnly, OfflineMode, SuperReadOnly, serverReadOnly
		// To prevent this strange position, prevent setting to OFF when any of these sysVars are ON of the same scope.

		if normalizedValue == Off {
			for _, potentialIncompatibleSysVar := range []string{TxReadOnly, TransactionReadOnly, OfflineMode, SuperReadOnly, serverReadOnly} {
				val, _ := vars.GetSystemVar(potentialIncompatibleSysVar) // session scope
				if scope == ScopeGlobal {                                // global scope
					var err error
					val, err = vars.GlobalVarsAccessor.GetGlobalSysVar(potentialIncompatibleSysVar)
					if err != nil {
						return originalValue, errUnknownSystemVariable.GenWithStackByArgs(potentialIncompatibleSysVar)
					}
				}
				if TiDBOptOn(val) {
					return originalValue, errValueNotSupportedWhen.GenWithStackByArgs(TiDBEnableNoopFuncs, potentialIncompatibleSysVar)
				}
			}
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EnableNoopFuncs = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBReplicaRead, Value: "leader", Type: TypeEnum, PossibleValues: []string{"leader", "follower", "leader-and-follower"}, SetSession: func(s *SessionVars, val string) error {
		if strings.EqualFold(val, "follower") {
			s.SetReplicaRead(tikvstore.ReplicaReadFollower)
		} else if strings.EqualFold(val, "leader-and-follower") {
			s.SetReplicaRead(tikvstore.ReplicaReadMixed)
		} else if strings.EqualFold(val, "leader") || len(val) == 0 {
			s.SetReplicaRead(tikvstore.ReplicaReadLeader)
		}
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBAllowRemoveAutoInc, Value: BoolToOnOff(DefTiDBAllowRemoveAutoInc), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowRemoveAutoInc = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableStmtSummary, Value: BoolToOnOff(config.GetGlobalConfig().StmtSummary.Enable), Type: TypeBool, AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryInternalQuery, Value: BoolToOnOff(config.GetGlobalConfig().StmtSummary.EnableInternalQuery), Type: TypeBool, AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryRefreshInterval, Value: strconv.Itoa(config.GetGlobalConfig().StmtSummary.RefreshInterval), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt32), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryHistorySize, Value: strconv.Itoa(config.GetGlobalConfig().StmtSummary.HistorySize), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxUint8), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryMaxStmtCount, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxStmtCount), 10), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt16), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryMaxSQLLength, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxSQLLength), 10), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxInt32), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBCapturePlanBaseline, Value: Off, Type: TypeBool, AllowEmptyAll: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBUsePlanBaselines, Value: BoolToOnOff(DefTiDBUsePlanBaselines), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.UsePlanBaselines = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEvolvePlanBaselines, Value: BoolToOnOff(DefTiDBEvolvePlanBaselines), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EvolvePlanBaselines = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExtendedStats, Value: BoolToOnOff(false), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableExtendedStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskMaxTime, Value: strconv.Itoa(DefTiDBEvolvePlanTaskMaxTime), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskStartTime, Value: DefTiDBEvolvePlanTaskStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBEvolvePlanTaskEndTime, Value: DefTiDBEvolvePlanTaskEndTime, Type: TypeTime},
	{Scope: ScopeSession, Name: TiDBIsolationReadEngines, Value: strings.Join(config.GetGlobalConfig().IsolationRead.Engines, ", "), Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		engines := strings.Split(normalizedValue, ",")
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
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(TiDBIsolationReadEngines, normalizedValue)
			}
		}
		return formatVal, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.IsolationReadEngines = make(map[kv.StoreType]struct{})
		for _, engine := range strings.Split(val, ",") {
			switch engine {
			case kv.TiKV.Name():
				s.IsolationReadEngines[kv.TiKV] = struct{}{}
			case kv.TiFlash.Name():
				s.IsolationReadEngines[kv.TiFlash] = struct{}{}
			case kv.TiDB.Name():
				s.IsolationReadEngines[kv.TiDB] = struct{}{}
			}
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStoreLimit, Value: strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		tikvstore.StoreLimit.Store(tidbOptInt64(val, DefTiDBStoreLimit))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBMetricSchemaStep, Value: strconv.Itoa(DefTiDBMetricSchemaStep), Type: TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaStep = tidbOptInt64(val, DefTiDBMetricSchemaStep)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBMetricSchemaRangeDuration, Value: strconv.Itoa(DefTiDBMetricSchemaRangeDuration), Type: TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaRangeDuration = tidbOptInt64(val, DefTiDBMetricSchemaRangeDuration)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBSlowLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowThreshold), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		atomic.StoreUint64(&config.GetGlobalConfig().Log.SlowThreshold, uint64(tidbOptInt64(val, logutil.DefaultSlowThreshold)))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBRecordPlanInSlowLog, Value: int32ToBoolStr(logutil.DefaultRecordPlanInSlowLog), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		atomic.StoreUint32(&config.GetGlobalConfig().Log.RecordPlanInSlowLog, uint32(tidbOptInt64(val, logutil.DefaultRecordPlanInSlowLog)))
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBEnableSlowLog, Value: BoolToOnOff(logutil.DefaultTiDBEnableSlowLog), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		config.GetGlobalConfig().Log.EnableSlowLog = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBQueryLogMaxLen, Value: strconv.Itoa(logutil.DefaultQueryLogMaxLen), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		atomic.StoreUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen, uint64(tidbOptInt64(val, logutil.DefaultQueryLogMaxLen)))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: CTEMaxRecursionDepth, Value: strconv.Itoa(DefCTEMaxRecursionDepth), Type: TypeInt, MinValue: 0, MaxValue: 4294967295, AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		s.CTEMaxRecursionDepth = tidbOptInt(val, DefCTEMaxRecursionDepth)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBCheckMb4ValueInUTF8, Value: BoolToOnOff(config.GetGlobalConfig().CheckMb4ValueInUTF8), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		config.GetGlobalConfig().CheckMb4ValueInUTF8 = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBFoundInPlanCache, Value: BoolToOnOff(DefTiDBFoundInPlanCache), Type: TypeBool, ReadOnly: true, SetSession: func(s *SessionVars, val string) error {
		s.FoundInPlanCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBFoundInBinding, Value: BoolToOnOff(DefTiDBFoundInBinding), Type: TypeBool, ReadOnly: true, SetSession: func(s *SessionVars, val string) error {
		s.FoundInBinding = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeSession, Name: TiDBEnableCollectExecutionInfo, Value: BoolToOnOff(DefTiDBEnableCollectExecutionInfo), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		oldConfig := config.GetGlobalConfig()
		newValue := TiDBOptOn(val)
		if oldConfig.EnableCollectExecutionInfo != newValue {
			newConfig := *oldConfig
			newConfig.EnableCollectExecutionInfo = newValue
			config.StoreGlobalConfig(&newConfig)
		}
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowAutoRandExplicitInsert, Value: BoolToOnOff(DefTiDBAllowAutoRandExplicitInsert), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowAutoRandExplicitInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableClusteredIndex, Value: IntOnly, Type: TypeEnum, PossibleValues: []string{Off, On, IntOnly}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == IntOnly {
			vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(normalizedValue, fmt.Sprintf("'%s' or '%s'", On, Off)))
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EnableClusteredIndex = TiDBOptEnableClustered(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPartitionPruneMode, Value: string(Static), Hidden: true, Type: TypeStr, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		mode := PartitionPruneMode(normalizedValue).Update()
		if !mode.Valid() {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(TiDBPartitionPruneMode)
		}
		return string(mode), nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.PartitionPruneMode.Store(strings.ToLower(strings.TrimSpace(val)))
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSlowLogMasking, Value: BoolToOnOff(DefTiDBRedactLog), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		// TiDBSlowLogMasking is deprecated and a alias of TiDBRedactLog.
		return s.SetSystemVar(TiDBRedactLog, val)
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRedactLog, Value: BoolToOnOff(DefTiDBRedactLog), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableRedactLog = TiDBOptOn(val)
		errors.RedactLogEnabled.Store(s.EnableRedactLog)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBShardAllocateStep, Value: strconv.Itoa(DefTiDBShardAllocateStep), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true, SetSession: func(s *SessionVars, val string) error {
		s.ShardAllocateStep = tidbOptInt64(val, DefTiDBShardAllocateStep)
		return nil
	}},
	{Scope: ScopeGlobal, Name: TiDBEnableTelemetry, Value: BoolToOnOff(DefTiDBEnableTelemetry), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAmendPessimisticTxn, Value: BoolToOnOff(DefTiDBEnableAmendPessimisticTxn), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAmendPessimisticTxn = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAsyncCommit, Value: BoolToOnOff(DefTiDBEnableAsyncCommit), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAsyncCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnable1PC, Value: BoolToOnOff(DefTiDBEnable1PC), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable1PC = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBGuaranteeLinearizability, Value: BoolToOnOff(DefTiDBGuaranteeLinearizability), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.GuaranteeLinearizability = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAnalyzeVersion, Value: strconv.Itoa(DefTiDBAnalyzeVersion), Hidden: true, Type: TypeInt, MinValue: 1, MaxValue: 2, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "2" && FeedbackProbability.Load() > 0 {
			var original string
			var err error
			if scope == ScopeGlobal {
				original, err = vars.GlobalVarsAccessor.GetGlobalSysVar(TiDBAnalyzeVersion)
				if err != nil {
					return normalizedValue, nil
				}
			} else {
				original = strconv.Itoa(vars.AnalyzeVersion)
			}
			vars.StmtCtx.AppendError(errors.New("variable tidb_analyze_version not updated because analyze version 2 is incompatible with query feedback. Please consider setting feedback-probability to 0.0 in config file to disable query feedback"))
			return original, nil
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.AnalyzeVersion = tidbOptPositiveInt32(val, DefTiDBAnalyzeVersion)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableIndexMergeJoin, Value: BoolToOnOff(DefTiDBEnableIndexMergeJoin), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableIndexMergeJoin = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTrackAggregateMemoryUsage, Value: BoolToOnOff(DefTiDBTrackAggregateMemoryUsage), Hidden: true, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.TrackAggregateMemoryUsage = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMultiStatementMode, Value: Off, Type: TypeEnum, PossibleValues: []string{Off, On, Warn}, SetSession: func(s *SessionVars, val string) error {
		s.MultiStatementMode = TiDBOptMultiStmt(val)
		return nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExchangePartition, Value: BoolToOnOff(DefTiDBEnableExchangePartition), Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.TiDBEnableExchangePartition = TiDBOptOn(val)
		return nil
	}},
	{Scope: ScopeNone, Name: TiDBEnableEnhancedSecurity, Value: Off, Type: TypeBool},

	/* tikv gc metrics */
	{Scope: ScopeGlobal, Name: TiDBGCEnable, Value: On, Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBGCRunInterval, Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal, Name: TiDBGCLifetime, Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal, Name: TiDBGCConcurrency, Value: "-1", Type: TypeInt, MinValue: 1, MaxValue: 128, AllowAutoValue: true},
	{Scope: ScopeGlobal, Name: TiDBGCScanLockMode, Value: "PHYSICAL", Type: TypeEnum, PossibleValues: []string{"PHYSICAL", "LEGACY"}},
}

// FeedbackProbability points to the FeedbackProbability in statistics package.
// It's initialized in init() in feedback.go to solve import cycle.
var FeedbackProbability *atomic2.Float64

// SynonymsSysVariables is synonyms of system variables.
var SynonymsSysVariables = map[string][]string{}

func addSynonymsSysVariables(synonyms ...string) {
	for _, s := range synonyms {
		SynonymsSysVariables[s] = synonyms
	}
}

func initSynonymsSysVariables() {
	addSynonymsSysVariables(TxnIsolation, TransactionIsolation)
	addSynonymsSysVariables(TxReadOnly, TransactionReadOnly)
}

// SetNamesVariables is the system variable names related to set names statements.
var SetNamesVariables = []string{
	CharacterSetClient,
	CharacterSetConnection,
	CharacterSetResults,
}

// SetCharsetVariables is the system variable names related to set charset statements.
var SetCharsetVariables = []string{
	CharacterSetClient,
	CharacterSetResults,
}

const (
	// CharacterSetConnection is the name for character_set_connection system variable.
	CharacterSetConnection = "character_set_connection"
	// CollationConnection is the name for collation_connection system variable.
	CollationConnection = "collation_connection"
	// CharsetDatabase is the name for character_set_database system variable.
	CharsetDatabase = "character_set_database"
	// CollationDatabase is the name for collation_database system variable.
	CollationDatabase = "collation_database"
	// CharacterSetFilesystem is the name for character_set_filesystem system variable.
	CharacterSetFilesystem = "character_set_filesystem"
	// CharacterSetClient is the name for character_set_client system variable.
	CharacterSetClient = "character_set_client"
	// CharacterSetSystem is the name for character_set_system system variable.
	CharacterSetSystem = "character_set_system"
	// GeneralLog is the name for 'general_log' system variable.
	GeneralLog = "general_log"
	// AvoidTemporalUpgrade is the name for 'avoid_temporal_upgrade' system variable.
	AvoidTemporalUpgrade = "avoid_temporal_upgrade"
	// MaxPreparedStmtCount is the name for 'max_prepared_stmt_count' system variable.
	MaxPreparedStmtCount = "max_prepared_stmt_count"
	// BigTables is the name for 'big_tables' system variable.
	BigTables = "big_tables"
	// CheckProxyUsers is the name for 'check_proxy_users' system variable.
	CheckProxyUsers = "check_proxy_users"
	// CoreFile is the name for 'core_file' system variable.
	CoreFile = "core_file"
	// DefaultWeekFormat is the name for 'default_week_format' system variable.
	DefaultWeekFormat = "default_week_format"
	// GroupConcatMaxLen is the name for 'group_concat_max_len' system variable.
	GroupConcatMaxLen = "group_concat_max_len"
	// DelayKeyWrite is the name for 'delay_key_write' system variable.
	DelayKeyWrite = "delay_key_write"
	// EndMarkersInJSON is the name for 'end_markers_in_json' system variable.
	EndMarkersInJSON = "end_markers_in_json"
	// Hostname is the name for 'hostname' system variable.
	Hostname = "hostname"
	// InnodbCommitConcurrency is the name for 'innodb_commit_concurrency' system variable.
	InnodbCommitConcurrency = "innodb_commit_concurrency"
	// InnodbFastShutdown is the name for 'innodb_fast_shutdown' system variable.
	InnodbFastShutdown = "innodb_fast_shutdown"
	// InnodbLockWaitTimeout is the name for 'innodb_lock_wait_timeout' system variable.
	InnodbLockWaitTimeout = "innodb_lock_wait_timeout"
	// SQLLogBin is the name for 'sql_log_bin' system variable.
	SQLLogBin = "sql_log_bin"
	// LogBin is the name for 'log_bin' system variable.
	LogBin = "log_bin"
	// MaxSortLength is the name for 'max_sort_length' system variable.
	MaxSortLength = "max_sort_length"
	// MaxSpRecursionDepth is the name for 'max_sp_recursion_depth' system variable.
	MaxSpRecursionDepth = "max_sp_recursion_depth"
	// MaxUserConnections is the name for 'max_user_connections' system variable.
	MaxUserConnections = "max_user_connections"
	// OfflineMode is the name for 'offline_mode' system variable.
	OfflineMode = "offline_mode"
	// InteractiveTimeout is the name for 'interactive_timeout' system variable.
	InteractiveTimeout = "interactive_timeout"
	// FlushTime is the name for 'flush_time' system variable.
	FlushTime = "flush_time"
	// PseudoSlaveMode is the name for 'pseudo_slave_mode' system variable.
	PseudoSlaveMode = "pseudo_slave_mode"
	// LowPriorityUpdates is the name for 'low_priority_updates' system variable.
	LowPriorityUpdates = "low_priority_updates"
	// LowerCaseTableNames is the name for 'lower_case_table_names' system variable.
	LowerCaseTableNames = "lower_case_table_names"
	// SessionTrackGtids is the name for 'session_track_gtids' system variable.
	SessionTrackGtids = "session_track_gtids"
	// OldPasswords is the name for 'old_passwords' system variable.
	OldPasswords = "old_passwords"
	// MaxConnections is the name for 'max_connections' system variable.
	MaxConnections = "max_connections"
	// SkipNameResolve is the name for 'skip_name_resolve' system variable.
	SkipNameResolve = "skip_name_resolve"
	// ForeignKeyChecks is the name for 'foreign_key_checks' system variable.
	ForeignKeyChecks = "foreign_key_checks"
	// SQLSafeUpdates is the name for 'sql_safe_updates' system variable.
	SQLSafeUpdates = "sql_safe_updates"
	// WarningCount is the name for 'warning_count' system variable.
	WarningCount = "warning_count"
	// ErrorCount is the name for 'error_count' system variable.
	ErrorCount = "error_count"
	// SQLSelectLimit is the name for 'sql_select_limit' system variable.
	SQLSelectLimit = "sql_select_limit"
	// MaxConnectErrors is the name for 'max_connect_errors' system variable.
	MaxConnectErrors = "max_connect_errors"
	// TableDefinitionCache is the name for 'table_definition_cache' system variable.
	TableDefinitionCache = "table_definition_cache"
	// TmpTableSize is the name for 'tmp_table_size' system variable.
	TmpTableSize = "tmp_table_size"
	// Timestamp is the name for 'timestamp' system variable.
	Timestamp = "timestamp"
	// ConnectTimeout is the name for 'connect_timeout' system variable.
	ConnectTimeout = "connect_timeout"
	// SyncBinlog is the name for 'sync_binlog' system variable.
	SyncBinlog = "sync_binlog"
	// BlockEncryptionMode is the name for 'block_encryption_mode' system variable.
	BlockEncryptionMode = "block_encryption_mode"
	// WaitTimeout is the name for 'wait_timeout' system variable.
	WaitTimeout = "wait_timeout"
	// ValidatePasswordNumberCount is the name of 'validate_password_number_count' system variable.
	ValidatePasswordNumberCount = "validate_password_number_count"
	// ValidatePasswordLength is the name of 'validate_password_length' system variable.
	ValidatePasswordLength = "validate_password_length"
	// Version is the name of 'version' system variable.
	Version = "version"
	// VersionComment is the name of 'version_comment' system variable.
	VersionComment = "version_comment"
	// PluginDir is the name of 'plugin_dir' system variable.
	PluginDir = "plugin_dir"
	// PluginLoad is the name of 'plugin_load' system variable.
	PluginLoad = "plugin_load"
	// Port is the name for 'port' system variable.
	Port = "port"
	// DataDir is the name for 'datadir' system variable.
	DataDir = "datadir"
	// Profiling is the name for 'Profiling' system variable.
	Profiling = "profiling"
	// Socket is the name for 'socket' system variable.
	Socket = "socket"
	// BinlogOrderCommits is the name for 'binlog_order_commits' system variable.
	BinlogOrderCommits = "binlog_order_commits"
	// MasterVerifyChecksum is the name for 'master_verify_checksum' system variable.
	MasterVerifyChecksum = "master_verify_checksum"
	// ValidatePasswordCheckUserName is the name for 'validate_password_check_user_name' system variable.
	ValidatePasswordCheckUserName = "validate_password_check_user_name"
	// SuperReadOnly is the name for 'super_read_only' system variable.
	SuperReadOnly = "super_read_only"
	// SQLNotes is the name for 'sql_notes' system variable.
	SQLNotes = "sql_notes"
	// QueryCacheType is the name for 'query_cache_type' system variable.
	QueryCacheType = "query_cache_type"
	// SlaveCompressedProtocol is the name for 'slave_compressed_protocol' system variable.
	SlaveCompressedProtocol = "slave_compressed_protocol"
	// BinlogRowQueryLogEvents is the name for 'binlog_rows_query_log_events' system variable.
	BinlogRowQueryLogEvents = "binlog_rows_query_log_events"
	// LogSlowSlaveStatements is the name for 'log_slow_slave_statements' system variable.
	LogSlowSlaveStatements = "log_slow_slave_statements"
	// LogSlowAdminStatements is the name for 'log_slow_admin_statements' system variable.
	LogSlowAdminStatements = "log_slow_admin_statements"
	// LogQueriesNotUsingIndexes is the name for 'log_queries_not_using_indexes' system variable.
	LogQueriesNotUsingIndexes = "log_queries_not_using_indexes"
	// QueryCacheWlockInvalidate is the name for 'query_cache_wlock_invalidate' system variable.
	QueryCacheWlockInvalidate = "query_cache_wlock_invalidate"
	// SQLAutoIsNull is the name for 'sql_auto_is_null' system variable.
	SQLAutoIsNull = "sql_auto_is_null"
	// RelayLogPurge is the name for 'relay_log_purge' system variable.
	RelayLogPurge = "relay_log_purge"
	// AutomaticSpPrivileges is the name for 'automatic_sp_privileges' system variable.
	AutomaticSpPrivileges = "automatic_sp_privileges"
	// SQLQuoteShowCreate is the name for 'sql_quote_show_create' system variable.
	SQLQuoteShowCreate = "sql_quote_show_create"
	// SlowQueryLog is the name for 'slow_query_log' system variable.
	SlowQueryLog = "slow_query_log"
	// BinlogDirectNonTransactionalUpdates is the name for 'binlog_direct_non_transactional_updates' system variable.
	BinlogDirectNonTransactionalUpdates = "binlog_direct_non_transactional_updates"
	// SQLBigSelects is the name for 'sql_big_selects' system variable.
	SQLBigSelects = "sql_big_selects"
	// LogBinTrustFunctionCreators is the name for 'log_bin_trust_function_creators' system variable.
	LogBinTrustFunctionCreators = "log_bin_trust_function_creators"
	// OldAlterTable is the name for 'old_alter_table' system variable.
	OldAlterTable = "old_alter_table"
	// EnforceGtidConsistency is the name for 'enforce_gtid_consistency' system variable.
	EnforceGtidConsistency = "enforce_gtid_consistency"
	// SecureAuth is the name for 'secure_auth' system variable.
	SecureAuth = "secure_auth"
	// UniqueChecks is the name for 'unique_checks' system variable.
	UniqueChecks = "unique_checks"
	// SQLWarnings is the name for 'sql_warnings' system variable.
	SQLWarnings = "sql_warnings"
	// AutoCommit is the name for 'autocommit' system variable.
	AutoCommit = "autocommit"
	// KeepFilesOnCreate is the name for 'keep_files_on_create' system variable.
	KeepFilesOnCreate = "keep_files_on_create"
	// ShowOldTemporals is the name for 'show_old_temporals' system variable.
	ShowOldTemporals = "show_old_temporals"
	// LocalInFile is the name for 'local_infile' system variable.
	LocalInFile = "local_infile"
	// PerformanceSchema is the name for 'performance_schema' system variable.
	PerformanceSchema = "performance_schema"
	// Flush is the name for 'flush' system variable.
	Flush = "flush"
	// SlaveAllowBatching is the name for 'slave_allow_batching' system variable.
	SlaveAllowBatching = "slave_allow_batching"
	// MyISAMUseMmap is the name for 'myisam_use_mmap' system variable.
	MyISAMUseMmap = "myisam_use_mmap"
	// InnodbFilePerTable is the name for 'innodb_file_per_table' system variable.
	InnodbFilePerTable = "innodb_file_per_table"
	// InnodbLogCompressedPages is the name for 'innodb_log_compressed_pages' system variable.
	InnodbLogCompressedPages = "innodb_log_compressed_pages"
	// InnodbPrintAllDeadlocks is the name for 'innodb_print_all_deadlocks' system variable.
	InnodbPrintAllDeadlocks = "innodb_print_all_deadlocks"
	// InnodbStrictMode is the name for 'innodb_strict_mode' system variable.
	InnodbStrictMode = "innodb_strict_mode"
	// InnodbCmpPerIndexEnabled is the name for 'innodb_cmp_per_index_enabled' system variable.
	InnodbCmpPerIndexEnabled = "innodb_cmp_per_index_enabled"
	// InnodbBufferPoolDumpAtShutdown is the name for 'innodb_buffer_pool_dump_at_shutdown' system variable.
	InnodbBufferPoolDumpAtShutdown = "innodb_buffer_pool_dump_at_shutdown"
	// InnodbAdaptiveHashIndex is the name for 'innodb_adaptive_hash_index' system variable.
	InnodbAdaptiveHashIndex = "innodb_adaptive_hash_index"
	// InnodbFtEnableStopword is the name for 'innodb_ft_enable_stopword' system variable.
	InnodbFtEnableStopword = "innodb_ft_enable_stopword"
	// InnodbSupportXA is the name for 'innodb_support_xa' system variable.
	InnodbSupportXA = "innodb_support_xa"
	// InnodbOptimizeFullTextOnly is the name for 'innodb_optimize_fulltext_only' system variable.
	InnodbOptimizeFullTextOnly = "innodb_optimize_fulltext_only"
	// InnodbStatusOutputLocks is the name for 'innodb_status_output_locks' system variable.
	InnodbStatusOutputLocks = "innodb_status_output_locks"
	// InnodbBufferPoolDumpNow is the name for 'innodb_buffer_pool_dump_now' system variable.
	InnodbBufferPoolDumpNow = "innodb_buffer_pool_dump_now"
	// InnodbBufferPoolLoadNow is the name for 'innodb_buffer_pool_load_now' system variable.
	InnodbBufferPoolLoadNow = "innodb_buffer_pool_load_now"
	// InnodbStatsOnMetadata is the name for 'innodb_stats_on_metadata' system variable.
	InnodbStatsOnMetadata = "innodb_stats_on_metadata"
	// InnodbDisableSortFileCache is the name for 'innodb_disable_sort_file_cache' system variable.
	InnodbDisableSortFileCache = "innodb_disable_sort_file_cache"
	// InnodbStatsAutoRecalc is the name for 'innodb_stats_auto_recalc' system variable.
	InnodbStatsAutoRecalc = "innodb_stats_auto_recalc"
	// InnodbBufferPoolLoadAbort is the name for 'innodb_buffer_pool_load_abort' system variable.
	InnodbBufferPoolLoadAbort = "innodb_buffer_pool_load_abort"
	// InnodbStatsPersistent is the name for 'innodb_stats_persistent' system variable.
	InnodbStatsPersistent = "innodb_stats_persistent"
	// InnodbRandomReadAhead is the name for 'innodb_random_read_ahead' system variable.
	InnodbRandomReadAhead = "innodb_random_read_ahead"
	// InnodbAdaptiveFlushing is the name for 'innodb_adaptive_flushing' system variable.
	InnodbAdaptiveFlushing = "innodb_adaptive_flushing"
	// InnodbTableLocks is the name for 'innodb_table_locks' system variable.
	InnodbTableLocks = "innodb_table_locks"
	// InnodbStatusOutput is the name for 'innodb_status_output' system variable.
	InnodbStatusOutput = "innodb_status_output"
	// NetBufferLength is the name for 'net_buffer_length' system variable.
	NetBufferLength = "net_buffer_length"
	// QueryCacheSize is the name of 'query_cache_size' system variable.
	QueryCacheSize = "query_cache_size"
	// TxReadOnly is the name of 'tx_read_only' system variable.
	TxReadOnly = "tx_read_only"
	// TransactionReadOnly is the name of 'transaction_read_only' system variable.
	TransactionReadOnly = "transaction_read_only"
	// CharacterSetServer is the name of 'character_set_server' system variable.
	CharacterSetServer = "character_set_server"
	// AutoIncrementIncrement is the name of 'auto_increment_increment' system variable.
	AutoIncrementIncrement = "auto_increment_increment"
	// AutoIncrementOffset is the name of 'auto_increment_offset' system variable.
	AutoIncrementOffset = "auto_increment_offset"
	// InitConnect is the name of 'init_connect' system variable.
	InitConnect = "init_connect"
	// CollationServer is the name of 'collation_server' variable.
	CollationServer = "collation_server"
	// NetWriteTimeout is the name of 'net_write_timeout' variable.
	NetWriteTimeout = "net_write_timeout"
	// ThreadPoolSize is the name of 'thread_pool_size' variable.
	ThreadPoolSize = "thread_pool_size"
	// WindowingUseHighPrecision is the name of 'windowing_use_high_precision' system variable.
	WindowingUseHighPrecision = "windowing_use_high_precision"
	// OptimizerSwitch is the name of 'optimizer_switch' system variable.
	OptimizerSwitch = "optimizer_switch"
	// SystemTimeZone is the name of 'system_time_zone' system variable.
	SystemTimeZone = "system_time_zone"
	// CTEMaxRecursionDepth is the name of 'cte_max_recursion_depth' system variable.
	CTEMaxRecursionDepth = "cte_max_recursion_depth"
)

// GlobalVarAccessor is the interface for accessing global scope system and status variables.
type GlobalVarAccessor interface {
	// GetGlobalSysVar gets the global system variable value for name.
	GetGlobalSysVar(name string) (string, error)
	// SetGlobalSysVar sets the global system variable name to value.
	SetGlobalSysVar(name string, value string) error
}
