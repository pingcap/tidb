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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/versioninfo"
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

	// BoolOff is the canonical string representation of a boolean false.
	BoolOff = "OFF"
	// BoolOn is the canonical string representation of a boolean true.
	BoolOn = "ON"
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
	// IsHintUpdatable indicate whether it's updatable via SET_VAR() hint (optional)
	IsHintUpdatable bool
}

// ValidateFromType provides automatic validation based on the SysVar's type
func (sv *SysVar) ValidateFromType(vars *SessionVars, value string, scope ScopeFlag) (string, error) {
	// Some sysvars are read-only. Attempting to set should always fail.
	if sv.ReadOnly || sv.Scope == ScopeNone {
		return value, ErrReadOnly.GenWithStackByArgs(sv.Name)
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
		return BoolOn, nil
	} else if strings.EqualFold(value, "OFF") {
		return BoolOff, nil
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		// There are two types of conversion rules for integer values.
		// The default only allows 0 || 1, but a subset of values convert any
		// negative integer to 1.
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

// ValidateFromHook calls the anonymous function on the sysvar if it exists.
func (sv *SysVar) ValidateFromHook(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
	if sv.Validation != nil {
		return sv.Validation(vars, normalizedValue, originalValue, scope)
	}
	return normalizedValue, nil
}

// GetNativeValType attempts to convert the val to the approx MySQL non-string type
func (sv *SysVar) GetNativeValType(val string) (types.Datum, byte) {
	switch sv.Type {
	case TypeBool:
		optVal := int64(0) // OFF
		if TiDBOptOn(val) {
			optVal = 1
		}
		return types.NewIntDatum(optVal), mysql.TypeLong
	}
	return types.NewStringDatum(val), mysql.TypeVarString
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
	initSynonymsSysVariables()
}

// BoolToOnOff returns the string representation of a bool, i.e. "ON/OFF"
func BoolToOnOff(b bool) string {
	if b {
		return BoolOn
	}
	return BoolOff
}

func int32ToBoolStr(i int32) string {
	if i == 1 {
		return BoolOn
	}
	return BoolOff
}

// we only support MySQL now
var defaultSysVars = []*SysVar{
	{Scope: ScopeGlobal, Name: "gtid_mode", Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: FlushTime, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: secondsPerYear, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "performance_schema_max_mutex_classes", Value: "200"},
	{Scope: ScopeGlobal | ScopeSession, Name: LowPriorityUpdates, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: SessionTrackGtids, Value: BoolOff, Type: TypeEnum, PossibleValues: []string{BoolOff, "OWN_GTID", "ALL_GTIDS"}},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndbinfo_max_rows", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndb_index_stat_option", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: OldPasswords, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 2, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "innodb_version", Value: "5.6.25"},
	{Scope: ScopeGlobal, Name: MaxConnections, Value: "151", Type: TypeUnsigned, MinValue: 1, MaxValue: 100000, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: BigTables, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "skip_external_locking", Value: "1"},
	{Scope: ScopeNone, Name: "innodb_sync_array_size", Value: "1"},
	{Scope: ScopeSession, Name: "rand_seed2", Value: ""},
	{Scope: ScopeGlobal, Name: ValidatePasswordCheckUserName, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: ValidatePasswordNumberCount, Value: "1", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: "gtid_next", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLSelectLimit, Value: "18446744073709551615", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: "ndb_show_foreign_key_mock_tables", Value: ""},
	{Scope: ScopeNone, Name: "multi_range_count", Value: "256"},
	{Scope: ScopeGlobal | ScopeSession, Name: DefaultWeekFormat, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 7, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "binlog_error_action", Value: "IGNORE_ERROR"},
	{Scope: ScopeGlobal | ScopeSession, Name: "default_storage_engine", Value: "InnoDB"},
	{Scope: ScopeNone, Name: "ft_query_expansion_limit", Value: "20"},
	{Scope: ScopeGlobal, Name: MaxConnectErrors, Value: "100", Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: SyncBinlog, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 4294967295, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "max_digest_length", Value: "1024"},
	{Scope: ScopeNone, Name: "innodb_force_load_corrupted", Value: "0"},
	{Scope: ScopeNone, Name: "performance_schema_max_table_handles", Value: "4000"},
	{Scope: ScopeGlobal, Name: InnodbFastShutdown, Value: "1", Type: TypeUnsigned, MinValue: 0, MaxValue: 2, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "ft_max_word_len", Value: "84"},
	{Scope: ScopeGlobal, Name: "log_backward_compatible_user_definitions", Value: ""},
	{Scope: ScopeNone, Name: "lc_messages_dir", Value: "/usr/local/mysql-5.6.25-osx10.8-x86_64/share/"},
	{Scope: ScopeGlobal, Name: "ft_boolean_syntax", Value: "+ -><()~*:\"\"&|"},
	{Scope: ScopeGlobal, Name: TableDefinitionCache, Value: "-1", Type: TypeUnsigned, MinValue: 400, MaxValue: 524288, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: SkipNameResolve, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "performance_schema_max_file_handles", Value: "32768"},
	{Scope: ScopeSession, Name: "transaction_allow_batching", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLModeVar, Value: mysql.DefaultSQLMode, IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "performance_schema_max_statement_classes", Value: "168"},
	{Scope: ScopeGlobal, Name: "server_id", Value: "0"},
	{Scope: ScopeGlobal, Name: "innodb_flushing_avg_loops", Value: "30"},
	{Scope: ScopeGlobal | ScopeSession, Name: TmpTableSize, Value: "16777216", Type: TypeUnsigned, MinValue: 1024, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true, IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: "innodb_max_purge_lag", Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: "preload_buffer_size", Value: "32768"},
	{Scope: ScopeGlobal, Name: CheckProxyUsers, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "have_query_cache", Value: "YES"},
	{Scope: ScopeGlobal, Name: "innodb_flush_log_at_timeout", Value: "1"},
	{Scope: ScopeGlobal, Name: "innodb_max_undo_log_size", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "range_alloc_block_size", Value: "4096", IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: ConnectTimeout, Value: "10", Type: TypeUnsigned, MinValue: 2, MaxValue: secondsPerYear, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxExecutionTime, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true, IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationServer, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if _, err := collate.GetCollationByName(normalizedValue); err != nil {
			return normalizedValue, errors.Trace(err)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeNone, Name: "have_rtree_keys", Value: "YES"},
	{Scope: ScopeGlobal, Name: "innodb_old_blocks_pct", Value: "37"},
	{Scope: ScopeGlobal, Name: "innodb_file_format", Value: "Antelope"},
	{Scope: ScopeGlobal, Name: "innodb_compression_failure_threshold_pct", Value: "5"},
	{Scope: ScopeNone, Name: "performance_schema_events_waits_history_long_size", Value: "10000"},
	{Scope: ScopeGlobal, Name: "innodb_checksum_algorithm", Value: "innodb"},
	{Scope: ScopeNone, Name: "innodb_ft_sort_pll_degree", Value: "2"},
	{Scope: ScopeNone, Name: "thread_stack", Value: "262144"},
	{Scope: ScopeGlobal, Name: "relay_log_info_repository", Value: "FILE"},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLLogBin, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal, Name: SuperReadOnly, Value: "0", Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_delayed_threads", Value: "20"},
	{Scope: ScopeNone, Name: "protocol_version", Value: "10"},
	{Scope: ScopeGlobal | ScopeSession, Name: "new", Value: BoolOff},
	{Scope: ScopeGlobal | ScopeSession, Name: "myisam_sort_buffer_size", Value: "8388608"},
	{Scope: ScopeGlobal | ScopeSession, Name: "optimizer_trace_offset", Value: "-1"},
	{Scope: ScopeGlobal, Name: InnodbBufferPoolDumpAtShutdown, Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLNotes, Value: "1"},
	{Scope: ScopeGlobal, Name: InnodbCmpPerIndexEnabled, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal, Name: "innodb_ft_server_stopword_table", Value: ""},
	{Scope: ScopeNone, Name: "performance_schema_max_file_instances", Value: "7693"},
	{Scope: ScopeNone, Name: "log_output", Value: "FILE"},
	{Scope: ScopeGlobal, Name: "binlog_group_commit_sync_delay", Value: ""},
	{Scope: ScopeGlobal, Name: "binlog_group_commit_sync_no_delay_count", Value: ""},
	{Scope: ScopeNone, Name: "have_crypt", Value: "YES"},
	{Scope: ScopeGlobal, Name: "innodb_log_write_ahead_size", Value: ""},
	{Scope: ScopeNone, Name: "innodb_log_group_home_dir", Value: "./"},
	{Scope: ScopeNone, Name: "performance_schema_events_statements_history_size", Value: "10"},
	{Scope: ScopeGlobal, Name: GeneralLog, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "validate_password_dictionary_file", Value: ""},
	{Scope: ScopeGlobal, Name: BinlogOrderCommits, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "key_cache_division_limit", Value: "100"},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_insert_delayed_threads", Value: "20"},
	{Scope: ScopeNone, Name: "performance_schema_session_connect_attrs_size", Value: "512"},
	{Scope: ScopeGlobal | ScopeSession, Name: TimeZone, Value: "SYSTEM", IsHintUpdatable: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if strings.EqualFold(normalizedValue, "SYSTEM") {
			return "SYSTEM", nil
		}
		_, err := parseTimeZone(normalizedValue)
		return normalizedValue, err
	}},
	{Scope: ScopeGlobal, Name: "innodb_max_dirty_pages_pct", Value: "75"},
	{Scope: ScopeGlobal, Name: InnodbFilePerTable, Value: BoolOn, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal, Name: InnodbLogCompressedPages, Value: "1"},
	{Scope: ScopeNone, Name: "skip_networking", Value: "0"},
	{Scope: ScopeGlobal, Name: "innodb_monitor_reset", Value: ""},
	{Scope: ScopeNone, Name: "have_ssl", Value: "DISABLED"},
	{Scope: ScopeNone, Name: "have_openssl", Value: "DISABLED"},
	{Scope: ScopeNone, Name: "ssl_ca", Value: ""},
	{Scope: ScopeNone, Name: "ssl_cert", Value: ""},
	{Scope: ScopeNone, Name: "ssl_key", Value: ""},
	{Scope: ScopeNone, Name: "ssl_cipher", Value: ""},
	{Scope: ScopeNone, Name: "tls_version", Value: "TLSv1,TLSv1.1,TLSv1.2"},
	{Scope: ScopeNone, Name: "system_time_zone", Value: "CST"},
	{Scope: ScopeGlobal, Name: InnodbPrintAllDeadlocks, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeNone, Name: "innodb_autoinc_lock_mode", Value: "1"},
	{Scope: ScopeGlobal, Name: "key_buffer_size", Value: "8388608"},
	{Scope: ScopeGlobal | ScopeSession, Name: ForeignKeyChecks, Value: BoolOff, Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) {
			// TiDB does not yet support foreign keys.
			// Return the original value in the warning, so that users are not confused.
			vars.StmtCtx.AppendWarning(ErrUnsupportedValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue))
			return BoolOff, nil
		} else if !TiDBOptOn(normalizedValue) {
			return BoolOff, nil
		}
		return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(ForeignKeyChecks, originalValue)
	}},
	{Scope: ScopeGlobal, Name: "host_cache_size", Value: "279"},
	{Scope: ScopeGlobal, Name: DelayKeyWrite, Value: BoolOn, Type: TypeEnum, PossibleValues: []string{BoolOff, BoolOn, "ALL"}},
	{Scope: ScopeNone, Name: "metadata_locks_cache_size", Value: "1024"},
	{Scope: ScopeNone, Name: "innodb_force_recovery", Value: "0"},
	{Scope: ScopeGlobal, Name: "innodb_file_format_max", Value: "Antelope"},
	{Scope: ScopeGlobal | ScopeSession, Name: "debug", Value: ""},
	{Scope: ScopeGlobal, Name: "log_warnings", Value: "1"},
	{Scope: ScopeGlobal, Name: OfflineMode, Value: "0", Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: InnodbStrictMode, Value: "1", Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal, Name: "innodb_rollback_segments", Value: "128"},
	{Scope: ScopeGlobal | ScopeSession, Name: "join_buffer_size", Value: "262144", IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "innodb_mirrored_log_groups", Value: "1"},
	{Scope: ScopeGlobal, Name: "max_binlog_size", Value: "1073741824"},
	{Scope: ScopeGlobal, Name: "concurrent_insert", Value: "AUTO"},
	{Scope: ScopeGlobal, Name: InnodbAdaptiveHashIndex, Value: BoolOn, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal, Name: InnodbFtEnableStopword, Value: BoolOn, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal, Name: "general_log_file", Value: "/usr/local/mysql/data/localhost.log"},
	{Scope: ScopeGlobal | ScopeSession, Name: InnodbSupportXA, Value: "1"},
	{Scope: ScopeGlobal, Name: "innodb_compression_level", Value: "6"},
	{Scope: ScopeNone, Name: "innodb_file_format_check", Value: "1"},
	{Scope: ScopeNone, Name: "myisam_mmap_size", Value: "18446744073709551615"},
	{Scope: ScopeNone, Name: "innodb_buffer_pool_instances", Value: "8"},
	{Scope: ScopeGlobal | ScopeSession, Name: BlockEncryptionMode, Value: "aes-128-ecb"},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_length_for_sort_data", Value: "1024", IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "character_set_system", Value: "utf8"},
	{Scope: ScopeGlobal | ScopeSession, Name: InteractiveTimeout, Value: "28800", Type: TypeUnsigned, MinValue: 1, MaxValue: secondsPerYear, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: InnodbOptimizeFullTextOnly, Value: "0"},
	{Scope: ScopeNone, Name: "character_sets_dir", Value: "/usr/local/mysql-5.6.25-osx10.8-x86_64/share/charsets/"},
	{Scope: ScopeGlobal | ScopeSession, Name: QueryCacheType, Value: BoolOff, Type: TypeEnum, PossibleValues: []string{BoolOff, BoolOn, "DEMAND"}},
	{Scope: ScopeNone, Name: "innodb_rollback_on_timeout", Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: "query_alloc_block_size", Value: "8192"},
	{Scope: ScopeGlobal | ScopeSession, Name: InitConnect, Value: ""},
	{Scope: ScopeNone, Name: "have_compress", Value: "YES"},
	{Scope: ScopeNone, Name: "thread_concurrency", Value: "10"},
	{Scope: ScopeGlobal | ScopeSession, Name: "query_prealloc_size", Value: "8192"},
	{Scope: ScopeNone, Name: "relay_log_space_limit", Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxUserConnections, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 4294967295, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "performance_schema_max_thread_classes", Value: "50"},
	{Scope: ScopeGlobal, Name: "innodb_api_trx_level", Value: "0"},
	{Scope: ScopeNone, Name: "disconnect_on_expired_password", Value: "1"},
	{Scope: ScopeNone, Name: "performance_schema_max_file_classes", Value: "50"},
	{Scope: ScopeGlobal, Name: "expire_logs_days", Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: BinlogRowQueryLogEvents, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "default_password_lifetime", Value: ""},
	{Scope: ScopeNone, Name: "pid_file", Value: "/usr/local/mysql/data/localhost.pid"},
	{Scope: ScopeNone, Name: "innodb_undo_tablespaces", Value: "0"},
	{Scope: ScopeGlobal, Name: InnodbStatusOutputLocks, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeNone, Name: "performance_schema_accounts_size", Value: "100"},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_error_count", Value: "64", IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: "max_write_lock_count", Value: "18446744073709551615"},
	{Scope: ScopeNone, Name: "performance_schema_max_socket_instances", Value: "322"},
	{Scope: ScopeNone, Name: "performance_schema_max_table_instances", Value: "12500"},
	{Scope: ScopeGlobal, Name: "innodb_stats_persistent_sample_pages", Value: "20"},
	{Scope: ScopeGlobal, Name: "show_compatibility_56", Value: ""},
	{Scope: ScopeNone, Name: "innodb_open_files", Value: "2000"},
	{Scope: ScopeGlobal, Name: "innodb_spin_wait_delay", Value: "6"},
	{Scope: ScopeGlobal, Name: "thread_cache_size", Value: "9"},
	{Scope: ScopeGlobal, Name: LogSlowAdminStatements, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "innodb_checksums", Type: TypeBool, Value: BoolOn},
	{Scope: ScopeNone, Name: "hostname", Value: ServerHostname},
	{Scope: ScopeGlobal | ScopeSession, Name: "auto_increment_offset", Value: "1"},
	{Scope: ScopeNone, Name: "ft_stopword_file", Value: "(built-in)"},
	{Scope: ScopeGlobal, Name: "innodb_max_dirty_pages_pct_lwm", Value: "0"},
	{Scope: ScopeGlobal, Name: LogQueriesNotUsingIndexes, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeSession, Name: "timestamp", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: QueryCacheWlockInvalidate, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: "sql_buffer_result", Value: BoolOff, IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "character_set_filesystem", Value: "binary"},
	{Scope: ScopeGlobal | ScopeSession, Name: CollationDatabase, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if _, err := collate.GetCollationByName(normalizedValue); err != nil {
			return normalizedValue, errors.Trace(err)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoIncrementIncrement, Value: strconv.FormatInt(DefAutoIncrementIncrement, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoIncrementOffset, Value: strconv.FormatInt(DefAutoIncrementOffset, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_heap_table_size", Value: "16777216", IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "div_precision_increment", Value: "4", IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: "innodb_lru_scan_depth", Value: "1024"},
	{Scope: ScopeGlobal, Name: "innodb_purge_rseg_truncate_frequency", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLAutoIsNull, Value: BoolOff, Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "innodb_api_enable_binlog", Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: "innodb_ft_user_stopword_table", Value: ""},
	{Scope: ScopeNone, Name: "server_id_bits", Value: "32"},
	{Scope: ScopeGlobal, Name: "innodb_log_checksum_algorithm", Value: ""},
	{Scope: ScopeNone, Name: "innodb_buffer_pool_load_at_startup", Value: "1"},
	{Scope: ScopeGlobal | ScopeSession, Name: "sort_buffer_size", Value: "262144", IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: "innodb_flush_neighbors", Value: "1"},
	{Scope: ScopeNone, Name: "innodb_use_sys_malloc", Value: "1"},
	{Scope: ScopeSession, Name: PluginLoad, Value: ""},
	{Scope: ScopeSession, Name: PluginDir, Value: "/data/deploy/plugin"},
	{Scope: ScopeNone, Name: "performance_schema_max_socket_classes", Value: "10"},
	{Scope: ScopeNone, Name: "performance_schema_max_stage_classes", Value: "150"},
	{Scope: ScopeGlobal, Name: "innodb_purge_batch_size", Value: "300"},
	{Scope: ScopeNone, Name: "have_profiling", Value: "NO"},
	{Scope: ScopeGlobal | ScopeSession, Name: "character_set_client", Value: mysql.DefaultCharset},
	{Scope: ScopeGlobal, Name: InnodbBufferPoolDumpNow, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal, Name: RelayLogPurge, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "ndb_distribution", Value: ""},
	{Scope: ScopeGlobal, Name: "myisam_data_pointer_size", Value: "6"},
	{Scope: ScopeGlobal, Name: "ndb_optimization_delay", Value: ""},
	{Scope: ScopeGlobal, Name: "innodb_ft_num_word_optimize", Value: "2000"},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_join_size", Value: "18446744073709551615", IsHintUpdatable: true},
	{Scope: ScopeNone, Name: CoreFile, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_seeks_for_key", Value: "18446744073709551615", IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "innodb_log_buffer_size", Value: "8388608"},
	{Scope: ScopeGlobal, Name: "delayed_insert_timeout", Value: "300"},
	{Scope: ScopeGlobal, Name: "max_relay_log_size", Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxSortLength, Value: "1024", Type: TypeUnsigned, MinValue: 4, MaxValue: 8388608, AutoConvertOutOfRange: true, IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "metadata_locks_hash_instances", Value: "8"},
	{Scope: ScopeGlobal, Name: "ndb_eventbuffer_free_percent", Value: ""},
	{Scope: ScopeNone, Name: "large_files_support", Value: "1"},
	{Scope: ScopeGlobal, Name: "binlog_max_flush_queue_time", Value: "0"},
	{Scope: ScopeGlobal, Name: "innodb_fill_factor", Value: ""},
	{Scope: ScopeGlobal, Name: "log_syslog_facility", Value: ""},
	{Scope: ScopeNone, Name: "innodb_ft_min_token_size", Value: "3"},
	{Scope: ScopeGlobal | ScopeSession, Name: "transaction_write_set_extraction", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndb_blob_write_batch_bytes", Value: ""},
	{Scope: ScopeGlobal, Name: "automatic_sp_privileges", Value: "1"},
	{Scope: ScopeGlobal, Name: "innodb_flush_sync", Value: ""},
	{Scope: ScopeNone, Name: "performance_schema_events_statements_history_long_size", Value: "10000"},
	{Scope: ScopeGlobal, Name: "innodb_monitor_disable", Value: ""},
	{Scope: ScopeNone, Name: "innodb_doublewrite", Value: "1"},
	{Scope: ScopeNone, Name: "log_bin_use_v1_row_events", Value: "0"},
	{Scope: ScopeSession, Name: "innodb_optimize_point_storage", Value: ""},
	{Scope: ScopeNone, Name: "innodb_api_disable_rowlock", Value: "0"},
	{Scope: ScopeGlobal, Name: "innodb_adaptive_flushing_lwm", Value: "10"},
	{Scope: ScopeNone, Name: "innodb_log_files_in_group", Value: "2"},
	{Scope: ScopeGlobal, Name: InnodbBufferPoolLoadNow, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeNone, Name: "performance_schema_max_rwlock_classes", Value: "40"},
	{Scope: ScopeNone, Name: "binlog_gtid_simple_recovery", Value: "1"},
	{Scope: ScopeNone, Name: Port, Value: "4000"},
	{Scope: ScopeNone, Name: "performance_schema_digests_size", Value: "10000"},
	{Scope: ScopeGlobal | ScopeSession, Name: Profiling, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "lower_case_table_names", Value: "2"},
	{Scope: ScopeSession, Name: "rand_seed1", Value: ""},
	{Scope: ScopeGlobal, Name: "sha256_password_proxy_users", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLQuoteShowCreate, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: "binlogging_impossible_mode", Value: "IGNORE_ERROR"},
	{Scope: ScopeGlobal | ScopeSession, Name: QueryCacheSize, Value: "1048576"},
	{Scope: ScopeGlobal, Name: "innodb_stats_transient_sample_pages", Value: "8"},
	{Scope: ScopeGlobal, Name: InnodbStatsOnMetadata, Value: "0"},
	{Scope: ScopeNone, Name: "server_uuid", Value: "00000000-0000-0000-0000-000000000000"},
	{Scope: ScopeNone, Name: "open_files_limit", Value: "5000"},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndb_force_send", Value: ""},
	{Scope: ScopeNone, Name: "skip_show_database", Value: "0"},
	{Scope: ScopeGlobal, Name: "log_timestamps", Value: ""},
	{Scope: ScopeNone, Name: "version_compile_machine", Value: "x86_64"},
	{Scope: ScopeGlobal, Name: "event_scheduler", Value: BoolOff},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndb_deferred_constraints", Value: ""},
	{Scope: ScopeGlobal, Name: "log_syslog_include_pid", Value: ""},
	{Scope: ScopeSession, Name: "last_insert_id", Value: ""},
	{Scope: ScopeNone, Name: "innodb_ft_cache_size", Value: "8000000"},
	{Scope: ScopeNone, Name: LogBin, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: InnodbDisableSortFileCache, Value: "0"},
	{Scope: ScopeGlobal, Name: "log_error_verbosity", Value: ""},
	{Scope: ScopeNone, Name: "performance_schema_hosts_size", Value: "100"},
	{Scope: ScopeGlobal, Name: "innodb_replication_delay", Value: "0"},
	{Scope: ScopeGlobal, Name: SlowQueryLog, Value: "0"},
	{Scope: ScopeSession, Name: "debug_sync", Value: ""},
	{Scope: ScopeGlobal, Name: InnodbStatsAutoRecalc, Value: "1"},
	{Scope: ScopeGlobal | ScopeSession, Name: "lc_messages", Value: "en_US"},
	{Scope: ScopeGlobal | ScopeSession, Name: "bulk_insert_buffer_size", Value: "8388608", IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: BinlogDirectNonTransactionalUpdates, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "innodb_change_buffering", Value: "all"},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLBigSelects, Value: BoolOn, Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetResults, Value: mysql.DefaultCharset},
	{Scope: ScopeGlobal, Name: "innodb_max_purge_lag_delay", Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: "session_track_schema", Value: ""},
	{Scope: ScopeGlobal, Name: "innodb_io_capacity_max", Value: "2000"},
	{Scope: ScopeGlobal, Name: "innodb_autoextend_increment", Value: "64"},
	{Scope: ScopeGlobal | ScopeSession, Name: "binlog_format", Value: "STATEMENT"},
	{Scope: ScopeGlobal | ScopeSession, Name: "optimizer_trace", Value: "enabled=off,one_line=off"},
	{Scope: ScopeGlobal | ScopeSession, Name: "read_rnd_buffer_size", Value: "262144", IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "version_comment", Value: "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 5.7 compatible"},
	{Scope: ScopeGlobal | ScopeSession, Name: NetWriteTimeout, Value: "60"},
	{Scope: ScopeGlobal, Name: InnodbBufferPoolLoadAbort, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
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
		if _, err := collate.GetCollationByName(normalizedValue); err != nil {
			return normalizedValue, errors.Trace(err)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: "transaction_prealloc_size", Value: "4096"},
	{Scope: ScopeNone, Name: "performance_schema_setup_objects_size", Value: "100"},
	{Scope: ScopeGlobal, Name: "sync_relay_log", Value: "10000"},
	{Scope: ScopeGlobal, Name: "innodb_ft_result_cache_limit", Value: "2000000000"},
	{Scope: ScopeNone, Name: "innodb_sort_buffer_size", Value: "1048576"},
	{Scope: ScopeGlobal, Name: "innodb_ft_enable_diag_print", Type: TypeBool, Value: BoolOff},
	{Scope: ScopeNone, Name: "thread_handling", Value: "one-thread-per-connection"},
	{Scope: ScopeGlobal, Name: "stored_program_cache", Value: "256"},
	{Scope: ScopeNone, Name: "performance_schema_max_mutex_instances", Value: "15906"},
	{Scope: ScopeGlobal, Name: "innodb_adaptive_max_sleep_delay", Value: "150000"},
	{Scope: ScopeNone, Name: "large_pages", Value: BoolOff},
	{Scope: ScopeGlobal | ScopeSession, Name: "session_track_system_variables", Value: ""},
	{Scope: ScopeGlobal, Name: "innodb_change_buffer_max_size", Value: "25"},
	{Scope: ScopeGlobal, Name: LogBinTrustFunctionCreators, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "innodb_write_io_threads", Value: "4"},
	{Scope: ScopeGlobal, Name: "mysql_native_password_proxy_users", Value: ""},
	{Scope: ScopeGlobal, Name: serverReadOnly, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "large_page_size", Value: "0"},
	{Scope: ScopeNone, Name: "table_open_cache_instances", Value: "1"},
	{Scope: ScopeGlobal, Name: InnodbStatsPersistent, Value: BoolOn, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "session_track_state_change", Value: ""},
	{Scope: ScopeNone, Name: OptimizerSwitch, Value: "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,subquery_materialization_cost_based=on,use_index_extensions=on", IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: "delayed_queue_size", Value: "1000"},
	{Scope: ScopeNone, Name: "innodb_read_only", Value: "0"},
	{Scope: ScopeNone, Name: "datetime_format", Value: "%Y-%m-%d %H:%i:%s"},
	{Scope: ScopeGlobal, Name: "log_syslog", Value: ""},
	{Scope: ScopeNone, Name: "version", Value: mysql.ServerVersion},
	{Scope: ScopeGlobal | ScopeSession, Name: "transaction_alloc_block_size", Value: "8192"},
	{Scope: ScopeGlobal, Name: "innodb_large_prefix", Type: TypeBool, Value: BoolOff},
	{Scope: ScopeNone, Name: "performance_schema_max_cond_classes", Value: "80"},
	{Scope: ScopeGlobal, Name: "innodb_io_capacity", Value: "200"},
	{Scope: ScopeGlobal, Name: "max_binlog_cache_size", Value: "18446744073709547520"},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndb_index_stat_enable", Value: ""},
	{Scope: ScopeGlobal, Name: "executed_gtids_compression_period", Value: ""},
	{Scope: ScopeNone, Name: "time_format", Value: "%H:%i:%s"},
	{Scope: ScopeGlobal | ScopeSession, Name: OldAlterTable, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: "long_query_time", Value: "10.000000"},
	{Scope: ScopeNone, Name: "innodb_use_native_aio", Value: "0"},
	{Scope: ScopeGlobal, Name: "log_throttle_queries_not_using_indexes", Value: "0"},
	{Scope: ScopeNone, Name: "locked_in_memory", Value: "0"},
	{Scope: ScopeNone, Name: "innodb_api_enable_mdl", Value: "0"},
	{Scope: ScopeGlobal, Name: "binlog_cache_size", Value: "32768"},
	{Scope: ScopeGlobal, Name: "innodb_compression_pad_pct_max", Value: "50"},
	{Scope: ScopeGlobal, Name: InnodbCommitConcurrency, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 1000, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "ft_min_word_len", Value: "4"},
	{Scope: ScopeGlobal, Name: EnforceGtidConsistency, Value: BoolOff, Type: TypeEnum, PossibleValues: []string{BoolOff, BoolOn, "WARN"}},
	{Scope: ScopeGlobal, Name: SecureAuth, Value: BoolOn, Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) {
			return BoolOn, nil
		}
		return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(SecureAuth, originalValue)
	}},
	{Scope: ScopeNone, Name: "max_tmp_tables", Value: "32"},
	{Scope: ScopeGlobal, Name: InnodbRandomReadAhead, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal | ScopeSession, Name: UniqueChecks, Value: BoolOn, Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: "internal_tmp_disk_storage_engine", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "myisam_repair_threads", Value: "1"},
	{Scope: ScopeGlobal, Name: "ndb_eventbuffer_max_alloc", Value: ""},
	{Scope: ScopeGlobal, Name: "innodb_read_ahead_threshold", Value: "56"},
	{Scope: ScopeGlobal, Name: "key_cache_block_size", Value: "1024"},
	{Scope: ScopeNone, Name: "ndb_recv_thread_cpu_mask", Value: ""},
	{Scope: ScopeGlobal, Name: "gtid_purged", Value: ""},
	{Scope: ScopeGlobal, Name: "max_binlog_stmt_cache_size", Value: "18446744073709547520"},
	{Scope: ScopeGlobal | ScopeSession, Name: "lock_wait_timeout", Value: "31536000"},
	{Scope: ScopeGlobal | ScopeSession, Name: "read_buffer_size", Value: "131072", IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "innodb_read_io_threads", Value: "4"},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxSpRecursionDepth, Value: "0", Type: TypeUnsigned, MinValue: 0, MaxValue: 255, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "ignore_builtin_innodb", Value: "0"},
	{Scope: ScopeGlobal, Name: "slow_query_log_file", Value: "/usr/local/mysql/data/localhost-slow.log"},
	{Scope: ScopeGlobal, Name: "innodb_thread_sleep_delay", Value: "10000"},
	{Scope: ScopeNone, Name: "license", Value: "Apache License 2.0"},
	{Scope: ScopeGlobal, Name: "innodb_ft_aux_table", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLWarnings, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: KeepFilesOnCreate, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "innodb_data_file_path", Value: "ibdata1:12M:autoextend"},
	{Scope: ScopeNone, Name: "performance_schema_setup_actors_size", Value: "100"},
	{Scope: ScopeNone, Name: "innodb_additional_mem_pool_size", Value: "8388608"},
	{Scope: ScopeNone, Name: "log_error", Value: "/usr/local/mysql/data/localhost.err"},
	{Scope: ScopeGlobal, Name: "binlog_stmt_cache_size", Value: "32768"},
	{Scope: ScopeNone, Name: "relay_log_info_file", Value: "relay-log.info"},
	{Scope: ScopeNone, Name: "innodb_ft_total_cache_size", Value: "640000000"},
	{Scope: ScopeNone, Name: "performance_schema_max_rwlock_instances", Value: "9102"},
	{Scope: ScopeGlobal, Name: "table_open_cache", Value: "2000"},
	{Scope: ScopeNone, Name: "performance_schema_events_stages_history_long_size", Value: "10000"},
	{Scope: ScopeGlobal | ScopeSession, Name: AutoCommit, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeSession, Name: "insert_id", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "default_tmp_storage_engine", Value: "InnoDB", IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "optimizer_search_depth", Value: "62", IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_points_in_geometry", Value: "65536", IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: "innodb_stats_sample_pages", Value: "8"},
	{Scope: ScopeGlobal | ScopeSession, Name: "profiling_history_size", Value: "15"},
	{Scope: ScopeGlobal | ScopeSession, Name: "character_set_database", Value: mysql.DefaultCharset},
	{Scope: ScopeNone, Name: "have_symlink", Value: "YES"},
	{Scope: ScopeGlobal | ScopeSession, Name: "storage_engine", Value: "InnoDB"},
	{Scope: ScopeGlobal | ScopeSession, Name: "sql_log_off", Value: "0"},
	// In MySQL, the default value of `explicit_defaults_for_timestamp` is `0`.
	// But In TiDB, it's set to `1` to be consistent with TiDB timestamp behavior.
	// See: https://github.com/pingcap/tidb/pull/6068 for details
	{Scope: ScopeNone, Name: "explicit_defaults_for_timestamp", Value: BoolOn, Type: TypeBool},
	{Scope: ScopeNone, Name: "performance_schema_events_waits_history_size", Value: "10"},
	{Scope: ScopeGlobal, Name: "log_syslog_tag", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: TxReadOnly, Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: TransactionReadOnly, Value: "0"},
	{Scope: ScopeGlobal, Name: "innodb_undo_log_truncate", Value: ""},
	{Scope: ScopeSession, Name: "innodb_create_intrinsic", Value: ""},
	{Scope: ScopeGlobal, Name: "gtid_executed_compression_period", Value: ""},
	{Scope: ScopeGlobal, Name: "ndb_log_empty_epochs", Value: ""},
	{Scope: ScopeGlobal, Name: MaxPreparedStmtCount, Value: strconv.FormatInt(DefMaxPreparedStmtCount, 10), Type: TypeInt, MinValue: -1, MaxValue: 1048576, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "have_geometry", Value: "YES"},
	{Scope: ScopeGlobal | ScopeSession, Name: "optimizer_trace_max_mem_size", Value: "16384"},
	{Scope: ScopeGlobal | ScopeSession, Name: "net_retry_count", Value: "10"},
	{Scope: ScopeSession, Name: "ndb_table_no_logging", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "optimizer_trace_features", Value: "greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on"},
	{Scope: ScopeGlobal, Name: "innodb_flush_log_at_trx_commit", Value: "1"},
	{Scope: ScopeGlobal, Name: "rewriter_enabled", Value: ""},
	{Scope: ScopeGlobal, Name: "query_cache_min_res_unit", Value: "4096"},
	{Scope: ScopeGlobal | ScopeSession, Name: "updatable_views_with_limit", Value: "YES", IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "optimizer_prune_level", Value: "1", IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "completion_type", Value: "NO_CHAIN"},
	{Scope: ScopeGlobal, Name: "binlog_checksum", Value: "CRC32"},
	{Scope: ScopeNone, Name: "report_port", Value: "3306"},
	{Scope: ScopeGlobal | ScopeSession, Name: ShowOldTemporals, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "query_cache_limit", Value: "1048576"},
	{Scope: ScopeGlobal, Name: "innodb_buffer_pool_size", Value: "134217728"},
	{Scope: ScopeGlobal, Name: InnodbAdaptiveFlushing, Value: BoolOn, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeNone, Name: "datadir", Value: "/usr/local/mysql/data/"},
	{Scope: ScopeGlobal | ScopeSession, Name: WaitTimeout, Value: strconv.FormatInt(DefWaitTimeout, 10), Type: TypeUnsigned, MinValue: 0, MaxValue: 31536000, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: "innodb_monitor_enable", Value: ""},
	{Scope: ScopeNone, Name: "date_format", Value: "%Y-%m-%d"},
	{Scope: ScopeGlobal, Name: "innodb_buffer_pool_filename", Value: "ib_buffer_pool"},
	{Scope: ScopeGlobal, Name: "slow_launch_time", Value: "2"},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndb_use_transactions", Value: ""},
	{Scope: ScopeNone, Name: "innodb_purge_threads", Value: "1"},
	{Scope: ScopeGlobal, Name: "innodb_concurrency_tickets", Value: "5000"},
	{Scope: ScopeGlobal, Name: "innodb_monitor_reset_all", Value: ""},
	{Scope: ScopeNone, Name: "performance_schema_users_size", Value: "100"},
	{Scope: ScopeGlobal, Name: "ndb_log_updated_only", Value: ""},
	{Scope: ScopeNone, Name: "basedir", Value: "/usr/local/mysql"},
	{Scope: ScopeGlobal, Name: "innodb_old_blocks_time", Value: "1000"},
	{Scope: ScopeGlobal, Name: "innodb_stats_method", Value: "nulls_equal"},
	{Scope: ScopeGlobal | ScopeSession, Name: InnodbLockWaitTimeout, Value: strconv.FormatInt(DefInnodbLockWaitTimeout, 10), Type: TypeUnsigned, MinValue: 1, MaxValue: 1073741824, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: LocalInFile, Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: "myisam_stats_method", Value: "nulls_unequal"},
	{Scope: ScopeNone, Name: "version_compile_os", Value: "osx10.8"},
	{Scope: ScopeNone, Name: "relay_log_recovery", Value: "0"},
	{Scope: ScopeNone, Name: "old", Value: "0"},
	{Scope: ScopeGlobal | ScopeSession, Name: InnodbTableLocks, Value: BoolOn, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeNone, Name: PerformanceSchema, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeNone, Name: "myisam_recover_options", Value: BoolOff},
	{Scope: ScopeGlobal | ScopeSession, Name: NetBufferLength, Value: "16384"},
	{Scope: ScopeGlobal | ScopeSession, Name: "binlog_row_image", Value: "FULL"},
	{Scope: ScopeNone, Name: "innodb_locks_unsafe_for_binlog", Value: "0"},
	{Scope: ScopeSession, Name: "rbr_exec_mode", Value: ""},
	{Scope: ScopeGlobal, Name: "myisam_max_sort_file_size", Value: "9223372036853727232"},
	{Scope: ScopeNone, Name: "back_log", Value: "80"},
	{Scope: ScopeNone, Name: "lower_case_file_system", Value: "1"},
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
	{Scope: ScopeSession, Name: "pseudo_thread_id", Value: ""},
	{Scope: ScopeNone, Name: "socket", Value: "/tmp/myssock"},
	{Scope: ScopeNone, Name: "have_dynamic_loading", Value: "YES"},
	{Scope: ScopeGlobal, Name: "rewriter_verbose", Value: ""},
	{Scope: ScopeGlobal, Name: "innodb_undo_logs", Value: "128"},
	{Scope: ScopeNone, Name: "performance_schema_max_cond_instances", Value: "3504"},
	{Scope: ScopeGlobal, Name: "delayed_insert_limit", Value: "100"},
	{Scope: ScopeGlobal, Name: Flush, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: "eq_range_index_dive_limit", Value: "200", IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "performance_schema_events_stages_history_size", Value: "10"},
	{Scope: ScopeGlobal | ScopeSession, Name: "character_set_connection", Value: mysql.DefaultCharset},
	{Scope: ScopeGlobal, Name: MyISAMUseMmap, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndb_join_pushdown", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: CharacterSetServer, Value: mysql.DefaultCharset},
	{Scope: ScopeGlobal, Name: "validate_password_special_char_count", Value: "1"},
	{Scope: ScopeNone, Name: "performance_schema_max_thread_instances", Value: "402"},
	{Scope: ScopeGlobal | ScopeSession, Name: "ndbinfo_show_hidden", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "net_read_timeout", Value: "30"},
	{Scope: ScopeNone, Name: "innodb_page_size", Value: "16384"},
	{Scope: ScopeGlobal | ScopeSession, Name: MaxAllowedPacket, Value: "67108864", Type: TypeUnsigned, MinValue: 1024, MaxValue: MaxOfMaxAllowedPacket, AutoConvertOutOfRange: true},
	{Scope: ScopeNone, Name: "innodb_log_file_size", Value: "50331648"},
	{Scope: ScopeGlobal, Name: "sync_relay_log_info", Value: "10000"},
	{Scope: ScopeGlobal | ScopeSession, Name: "optimizer_trace_limit", Value: "1"},
	{Scope: ScopeNone, Name: "innodb_ft_max_token_size", Value: "84"},
	{Scope: ScopeGlobal, Name: ValidatePasswordLength, Value: "8", Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: "ndb_log_binlog_index", Value: ""},
	{Scope: ScopeGlobal, Name: "innodb_api_bk_commit_interval", Value: "5"},
	{Scope: ScopeNone, Name: "innodb_undo_directory", Value: "."},
	{Scope: ScopeNone, Name: "bind_address", Value: "*"},
	{Scope: ScopeGlobal, Name: "innodb_sync_spin_loops", Value: "30"},
	{Scope: ScopeGlobal | ScopeSession, Name: SQLSafeUpdates, Value: BoolOff, Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeNone, Name: "tmpdir", Value: "/var/tmp/"},
	{Scope: ScopeGlobal, Name: "innodb_thread_concurrency", Value: "0"},
	{Scope: ScopeGlobal, Name: "innodb_buffer_pool_dump_pct", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "lc_time_names", Value: "en_US"},
	{Scope: ScopeGlobal | ScopeSession, Name: "max_statement_time", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: EndMarkersInJSON, Value: BoolOff, Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeGlobal, Name: AvoidTemporalUpgrade, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "key_cache_age_threshold", Value: "300"},
	{Scope: ScopeGlobal, Name: InnodbStatusOutput, Value: BoolOff, Type: TypeBool, AutoConvertNegativeBool: true},
	{Scope: ScopeSession, Name: "identity", Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: "min_examined_row_limit", Value: "0"},
	{Scope: ScopeGlobal, Name: "sync_frm", Type: TypeBool, Value: BoolOn},
	{Scope: ScopeGlobal, Name: "innodb_online_alter_log_max_size", Value: "134217728"},
	{Scope: ScopeSession, Name: WarningCount, Value: "0", ReadOnly: true},
	{Scope: ScopeSession, Name: ErrorCount, Value: "0", ReadOnly: true},
	{Scope: ScopeGlobal | ScopeSession, Name: "information_schema_stats_expiry", Value: "86400"},
	{Scope: ScopeGlobal, Name: ThreadPoolSize, Value: "16", Type: TypeUnsigned, MinValue: 1, MaxValue: 64, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal | ScopeSession, Name: WindowingUseHighPrecision, Value: BoolOn, Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeSession, Name: TiDBTxnScope, Value: config.GetGlobalConfig().TxnScope},
	/* TiDB specific variables */
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowMPPExecution, Type: TypeBool, Value: BoolToOnOff(DefTiDBAllowMPPExecution)},
	{Scope: ScopeSession, Name: TiDBSnapshot, Value: ""},
	{Scope: ScopeSession, Name: TiDBOptAggPushDown, Value: BoolToOnOff(DefOptAggPushDown), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptBCJ, Value: BoolToOnOff(DefOptBCJ), Type: TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) && vars.AllowBatchCop == 0 {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("Can't set Broadcast Join to 1 but tidb_allow_batch_cop is 0, please active batch cop at first.")
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeSession, Name: TiDBOptDistinctAggPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.DistinctAggPushDown), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBOptWriteRowID, Value: BoolToOnOff(DefOptWriteRowID)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBuildStatsConcurrency, Value: strconv.Itoa(DefBuildStatsConcurrency)},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeRatio, Value: strconv.FormatFloat(DefAutoAnalyzeRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeStartTime, Value: DefAutoAnalyzeStartTime, Type: TypeTime},
	{Scope: ScopeGlobal, Name: TiDBAutoAnalyzeEndTime, Value: DefAutoAnalyzeEndTime, Type: TypeTime},
	{Scope: ScopeSession, Name: TiDBChecksumTableConcurrency, Value: strconv.Itoa(DefChecksumTableConcurrency)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBExecutorConcurrency, Value: strconv.Itoa(DefExecutorConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDistSQLScanConcurrency, Value: strconv.Itoa(DefDistSQLScanConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptInSubqToJoinAndAgg, Value: BoolToOnOff(DefOptInSubqToJoinAndAgg), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBOptPreferRangeScan, Value: BoolToOnOff(DefOptPreferRangeScan), Type: TypeBool, IsHintUpdatable: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCorrelationThreshold, Value: strconv.FormatFloat(DefOptCorrelationThreshold, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: 1},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCorrelationExpFactor, Value: strconv.Itoa(DefOptCorrelationExpFactor), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCPUFactor, Value: strconv.FormatFloat(DefOptCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptTiFlashConcurrencyFactor, Value: strconv.FormatFloat(DefOptTiFlashConcurrencyFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptCopCPUFactor, Value: strconv.FormatFloat(DefOptCopCPUFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptNetworkFactor, Value: strconv.FormatFloat(DefOptNetworkFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptScanFactor, Value: strconv.FormatFloat(DefOptScanFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDescScanFactor, Value: strconv.FormatFloat(DefOptDescScanFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptSeekFactor, Value: strconv.FormatFloat(DefOptSeekFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptMemoryFactor, Value: strconv.FormatFloat(DefOptMemoryFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptDiskFactor, Value: strconv.FormatFloat(DefOptDiskFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptConcurrencyFactor, Value: strconv.FormatFloat(DefOptConcurrencyFactor, 'f', -1, 64), Type: TypeFloat, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexJoinBatchSize, Value: strconv.Itoa(DefIndexJoinBatchSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupSize, Value: strconv.Itoa(DefIndexLookupSize), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupConcurrency, Value: strconv.Itoa(DefIndexLookupConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexLookupJoinConcurrency, Value: strconv.Itoa(DefIndexLookupJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBIndexSerialScanConcurrency, Value: strconv.Itoa(DefIndexSerialScanConcurrency), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipUTF8Check, Value: BoolToOnOff(DefSkipUTF8Check), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipASCIICheck, Value: BoolToOnOff(DefSkipASCIICheck), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBBatchInsert, Value: BoolToOnOff(DefBatchInsert), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBBatchDelete, Value: BoolToOnOff(DefBatchDelete), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBBatchCommit, Value: BoolToOnOff(DefBatchCommit), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDMLBatchSize, Value: strconv.Itoa(DefDMLBatchSize), Type: TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64},
	{Scope: ScopeSession, Name: TiDBCurrentTS, Value: strconv.Itoa(DefCurretTS)},
	{Scope: ScopeSession, Name: TiDBLastTxnInfo, Value: strconv.Itoa(DefCurretTS)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMaxChunkSize, Value: strconv.Itoa(DefMaxChunkSize), Type: TypeUnsigned, MinValue: maxChunkSizeLowerBound, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowBatchCop, Value: strconv.Itoa(DefTiDBAllowBatchCop), Type: TypeInt, MinValue: 0, MaxValue: 2, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if normalizedValue == "0" && vars.AllowBCJ {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("Can't set batch cop 0 but tidb_opt_broadcast_join is 1, please set tidb_opt_broadcast_join 0 at first")
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBInitChunkSize, Value: strconv.Itoa(DefInitChunkSize), Type: TypeUnsigned, MinValue: 1, MaxValue: initChunkSizeUpperBound},

	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableCascadesPlanner, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableIndexMerge, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeSession, Name: TIDBMemQuotaQuery, Value: strconv.FormatInt(config.GetGlobalConfig().MemQuotaQuery, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaHashJoin, Value: strconv.FormatInt(DefTiDBMemQuotaHashJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaMergeJoin, Value: strconv.FormatInt(DefTiDBMemQuotaMergeJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaSort, Value: strconv.FormatInt(DefTiDBMemQuotaSort, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaTopn, Value: strconv.FormatInt(DefTiDBMemQuotaTopn, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaIndexLookupReader, Value: strconv.FormatInt(DefTiDBMemQuotaIndexLookupReader, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TIDBMemQuotaIndexLookupJoin, Value: strconv.FormatInt(DefTiDBMemQuotaIndexLookupJoin, 10), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBEnableStreaming, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBEnableChunkRPC, Value: BoolOn, Type: TypeBool},

	{Scope: ScopeSession, Name: TxnIsolationOneShot, Value: ""},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableTablePartition, Value: BoolOn, Type: TypeEnum, PossibleValues: []string{BoolOff, BoolOn, "AUTO"}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashJoinConcurrency, Value: strconv.Itoa(DefTiDBHashJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBProjectionConcurrency, Value: strconv.Itoa(DefTiDBProjectionConcurrency), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashAggPartialConcurrency, Value: strconv.Itoa(DefTiDBHashAggPartialConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBHashAggFinalConcurrency, Value: strconv.Itoa(DefTiDBHashAggFinalConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBWindowConcurrency, Value: strconv.Itoa(DefTiDBWindowConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMergeJoinConcurrency, Value: strconv.Itoa(DefTiDBMergeJoinConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStreamAggConcurrency, Value: strconv.Itoa(DefTiDBStreamAggConcurrency), Type: TypeInt, MinValue: 1, MaxValue: math.MaxInt64, AllowAutoValue: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableParallelApply, Value: BoolToOnOff(DefTiDBEnableParallelApply), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBMemQuotaApplyCache, Value: strconv.Itoa(DefTiDBMemQuotaApplyCache)},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBackoffLockFast, Value: strconv.Itoa(kv.DefBackoffLockFast), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBBackOffWeight, Value: strconv.Itoa(kv.DefBackOffWeight), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRetryLimit, Value: strconv.Itoa(DefTiDBRetryLimit), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBDisableTxnAutoRetry, Value: BoolToOnOff(DefTiDBDisableTxnAutoRetry), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBConstraintCheckInPlace, Value: BoolToOnOff(DefTiDBConstraintCheckInPlace), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTxnMode, Value: DefTiDBTxnMode, AllowEmptyAll: true, Type: TypeEnum, PossibleValues: []string{"pessimistic", "optimistic"}},
	{Scope: ScopeGlobal, Name: TiDBRowFormatVersion, Value: strconv.Itoa(DefTiDBRowFormatV1), Type: TypeUnsigned, MinValue: 1, MaxValue: 2},
	{Scope: ScopeSession, Name: TiDBOptimizerSelectivityLevel, Value: strconv.Itoa(DefTiDBOptimizerSelectivityLevel), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableWindowFunction, Value: BoolToOnOff(DefEnableWindowFunction), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableStrictDoubleTypeCheck, Value: BoolToOnOff(DefEnableStrictDoubleTypeCheck), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableVectorizedExpression, Value: BoolToOnOff(DefEnableVectorizedExpression), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableFastAnalyze, Value: BoolToOnOff(DefTiDBUseFastAnalyze), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSkipIsolationLevelCheck, Value: BoolToOnOff(DefTiDBSkipIsolationLevelCheck), Type: TypeBool},

	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableRateLimitAction, Value: BoolToOnOff(DefTiDBEnableRateLimitAction), Type: TypeBool},
	/* The following variable is defined as session scope but is actually server scope. */
	{Scope: ScopeSession, Name: TiDBGeneralLog, Value: BoolToOnOff(DefTiDBGeneralLog), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBPProfSQLCPU, Value: strconv.Itoa(DefTiDBPProfSQLCPU), Type: TypeInt, MinValue: 0, MaxValue: 1},
	{Scope: ScopeSession, Name: TiDBDDLSlowOprThreshold, Value: strconv.Itoa(DefTiDBDDLSlowOprThreshold)},
	{Scope: ScopeSession, Name: TiDBConfig, Value: ""},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgWorkerCount, Value: strconv.Itoa(DefTiDBDDLReorgWorkerCount), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64},
	{Scope: ScopeGlobal, Name: TiDBDDLReorgBatchSize, Value: strconv.Itoa(DefTiDBDDLReorgBatchSize), Type: TypeUnsigned, MinValue: int64(MinDDLReorgBatchSize), MaxValue: uint64(MaxDDLReorgBatchSize), AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: TiDBDDLErrorCountLimit, Value: strconv.Itoa(DefTiDBDDLErrorCountLimit), Type: TypeUnsigned, MinValue: 0, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: TiDBDDLReorgPriority, Value: "PRIORITY_LOW"},
	{Scope: ScopeGlobal, Name: TiDBMaxDeltaSchemaCount, Value: strconv.Itoa(DefTiDBMaxDeltaSchemaCount), Type: TypeUnsigned, MinValue: 100, MaxValue: 16384, AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: TiDBEnableChangeColumnType, Value: BoolToOnOff(DefTiDBChangeColumnType), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnableChangeMultiSchema, Value: BoolToOnOff(DefTiDBChangeMultiSchema), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnablePointGetCache, Value: BoolToOnOff(DefTiDBPointGetCache), Type: TypeBool},
	{Scope: ScopeGlobal, Name: TiDBEnableAlterPartitionPlacement, Value: BoolToOnOff(DefTiDBAlterPartitionPlacement), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBForcePriority, Value: mysql.Priority2Str[DefTiDBForcePriority]},
	{Scope: ScopeSession, Name: TiDBEnableRadixJoin, Value: BoolToOnOff(DefTiDBUseRadixJoin), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBOptJoinReorderThreshold, Value: strconv.Itoa(DefTiDBOptJoinReorderThreshold), Type: TypeUnsigned, MinValue: 0, MaxValue: 63},
	{Scope: ScopeSession, Name: TiDBSlowQueryFile, Value: ""},
	{Scope: ScopeGlobal, Name: TiDBScatterRegion, Value: BoolToOnOff(DefTiDBScatterRegion), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBWaitSplitRegionFinish, Value: BoolToOnOff(DefTiDBWaitSplitRegionFinish), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBWaitSplitRegionTimeout, Value: strconv.Itoa(DefWaitSplitRegionTimeout), Type: TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBLowResolutionTSO, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBExpensiveQueryTimeThreshold, Value: strconv.Itoa(DefTiDBExpensiveQueryTimeThreshold), Type: TypeUnsigned, MinValue: int64(MinExpensiveQueryTimeThreshold), MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: TiDBMemoryUsageAlarmRatio, Value: strconv.FormatFloat(config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio, 'f', -1, 64), Type: TypeFloat, MinValue: 0.0, MaxValue: 1.0},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableNoopFuncs, Value: BoolToOnOff(DefTiDBEnableNoopFuncs), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBReplicaRead, Value: "leader", Type: TypeEnum, PossibleValues: []string{"leader", "follower", "leader-and-follower"}},
	{Scope: ScopeSession, Name: TiDBAllowRemoveAutoInc, Value: BoolToOnOff(DefTiDBAllowRemoveAutoInc), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableStmtSummary, Value: BoolToOnOff(config.GetGlobalConfig().StmtSummary.Enable), Type: TypeBool, AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryInternalQuery, Value: BoolToOnOff(config.GetGlobalConfig().StmtSummary.EnableInternalQuery), Type: TypeBool, AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryRefreshInterval, Value: strconv.Itoa(config.GetGlobalConfig().StmtSummary.RefreshInterval), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt32), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryHistorySize, Value: strconv.Itoa(config.GetGlobalConfig().StmtSummary.HistorySize), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxUint8), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryMaxStmtCount, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxStmtCount), 10), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt16), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStmtSummaryMaxSQLLength, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxSQLLength), 10), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxInt32), AllowEmpty: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBCapturePlanBaseline, Value: BoolOff, Type: TypeBool, AllowEmptyAll: true},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBUsePlanBaselines, Value: BoolToOnOff(DefTiDBUsePlanBaselines), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEvolvePlanBaselines, Value: BoolToOnOff(DefTiDBEvolvePlanBaselines), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableExtendedStats, Value: BoolToOnOff(false), Type: TypeBool},
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
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBStoreLimit, Value: strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10), Type: TypeInt, MinValue: 0, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeSession, Name: TiDBMetricSchemaStep, Value: strconv.Itoa(DefTiDBMetricSchemaStep), Type: TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60},
	{Scope: ScopeSession, Name: TiDBMetricSchemaRangeDuration, Value: strconv.Itoa(DefTiDBMetricSchemaRangeDuration), Type: TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60},
	{Scope: ScopeSession, Name: TiDBSlowLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowThreshold), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBRecordPlanInSlowLog, Value: int32ToBoolStr(logutil.DefaultRecordPlanInSlowLog), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBEnableSlowLog, Value: BoolToOnOff(logutil.DefaultTiDBEnableSlowLog), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBQueryLogMaxLen, Value: strconv.Itoa(logutil.DefaultQueryLogMaxLen), Type: TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: ScopeSession, Name: TiDBCheckMb4ValueInUTF8, Value: BoolToOnOff(config.GetGlobalConfig().CheckMb4ValueInUTF8), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBFoundInPlanCache, Value: BoolToOnOff(DefTiDBFoundInPlanCache), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBFoundInBinding, Value: BoolToOnOff(DefTiDBFoundInBinding), Type: TypeBool},
	{Scope: ScopeSession, Name: TiDBEnableCollectExecutionInfo, Value: BoolToOnOff(DefTiDBEnableCollectExecutionInfo), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAllowAutoRandExplicitInsert, Value: BoolToOnOff(DefTiDBAllowAutoRandExplicitInsert), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableClusteredIndex, Value: BoolToOnOff(DefTiDBEnableClusteredIndex), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBPartitionPruneMode, Value: string(StaticOnly), Type: TypeStr, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
		if !PartitionPruneMode(normalizedValue).Valid() {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(TiDBPartitionPruneMode)
		}
		return normalizedValue, nil
	}},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBSlowLogMasking, Value: BoolToOnOff(DefTiDBRedactLog), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBRedactLog, Value: BoolToOnOff(DefTiDBRedactLog), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBShardAllocateStep, Value: strconv.Itoa(DefTiDBShardAllocateStep), Type: TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt64), AutoConvertOutOfRange: true},
	{Scope: ScopeGlobal, Name: TiDBEnableTelemetry, Value: BoolToOnOff(DefTiDBEnableTelemetry), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAmendPessimisticTxn, Value: BoolToOnOff(DefTiDBEnableAmendPessimisticTxn), Type: TypeBool},

	// for compatibility purpose, we should leave them alone.
	// TODO: Follow the Terminology Updates of MySQL after their changes arrived.
	// https://mysqlhighavailability.com/mysql-terminology-updates/
	{Scope: ScopeSession, Name: PseudoSlaveMode, Value: "", Type: TypeInt},
	{Scope: ScopeGlobal, Name: "slave_pending_jobs_size_max", Value: "16777216"},
	{Scope: ScopeGlobal, Name: "slave_transaction_retries", Value: "10"},
	{Scope: ScopeGlobal, Name: "slave_checkpoint_period", Value: "300"},
	{Scope: ScopeGlobal, Name: MasterVerifyChecksum, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "rpl_semi_sync_master_trace_level", Value: ""},
	{Scope: ScopeGlobal, Name: "master_info_repository", Value: "FILE"},
	{Scope: ScopeGlobal, Name: "rpl_stop_slave_timeout", Value: "31536000"},
	{Scope: ScopeGlobal, Name: "slave_net_timeout", Value: "3600"},
	{Scope: ScopeGlobal, Name: "sync_master_info", Value: "10000"},
	{Scope: ScopeGlobal, Name: "init_slave", Value: ""},
	{Scope: ScopeGlobal, Name: SlaveCompressedProtocol, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "rpl_semi_sync_slave_trace_level", Value: ""},
	{Scope: ScopeGlobal, Name: LogSlowSlaveStatements, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "slave_checkpoint_group", Value: "512"},
	{Scope: ScopeNone, Name: "slave_load_tmpdir", Value: "/var/tmp/"},
	{Scope: ScopeGlobal, Name: "slave_parallel_type", Value: ""},
	{Scope: ScopeGlobal, Name: "slave_parallel_workers", Value: "0"},
	{Scope: ScopeGlobal, Name: "rpl_semi_sync_master_timeout", Value: "10000", Type: TypeInt},
	{Scope: ScopeNone, Name: "slave_skip_errors", Value: BoolOff},
	{Scope: ScopeGlobal, Name: "sql_slave_skip_counter", Value: "0"},
	{Scope: ScopeGlobal, Name: "rpl_semi_sync_slave_enabled", Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "rpl_semi_sync_master_enabled", Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "slave_preserve_commit_order", Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "slave_exec_mode", Value: "STRICT"},
	{Scope: ScopeNone, Name: "log_slave_updates", Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "rpl_semi_sync_master_wait_point", Value: "AFTER_SYNC", Type: TypeEnum, PossibleValues: []string{"AFTER_SYNC", "AFTER_COMMIT"}},
	{Scope: ScopeGlobal, Name: "slave_sql_verify_checksum", Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "slave_max_allowed_packet", Value: "1073741824"},
	{Scope: ScopeGlobal, Name: "rpl_semi_sync_master_wait_for_slave_count", Value: "1", Type: TypeInt, MinValue: 1, MaxValue: 65535},
	{Scope: ScopeGlobal, Name: "rpl_semi_sync_master_wait_no_slave", Value: BoolOn, Type: TypeBool},
	{Scope: ScopeGlobal, Name: "slave_rows_search_algorithms", Value: "TABLE_SCAN,INDEX_SCAN"},
	{Scope: ScopeGlobal, Name: SlaveAllowBatching, Value: BoolOff, Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnableAsyncCommit, Value: BoolToOnOff(DefTiDBEnableAsyncCommit), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBEnable1PC, Value: BoolToOnOff(DefTiDBEnable1PC), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBGuaranteeExternalConsistency, Value: BoolToOnOff(DefTiDBGuaranteeExternalConsistency), Type: TypeBool},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBAnalyzeVersion, Value: strconv.Itoa(DefTiDBAnalyzeVersion), Type: TypeInt, MinValue: 1, MaxValue: 2},
	{Scope: ScopeGlobal | ScopeSession, Name: TiDBTrackAggregateMemoryUsage, Value: BoolToOnOff(DefTiDBTrackAggregateMemoryUsage), Type: TypeBool},
}

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
)

// GlobalVarAccessor is the interface for accessing global scope system and status variables.
type GlobalVarAccessor interface {
	// GetAllSysVars gets all the global system variable values.
	GetAllSysVars() (map[string]string, error)
	// GetGlobalSysVar gets the global system variable value for name.
	GetGlobalSysVar(name string) (string, error)
	// SetGlobalSysVar sets the global system variable name to value.
	SetGlobalSysVar(name string, value string) error
}
