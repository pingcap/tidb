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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/tikv/client-go/v2/oracle"
)

// secondsPerYear represents seconds in a normal year. Leap year is not considered here.
const secondsPerYear = 60 * 60 * 24 * 365

// SetDDLReorgWorkerCounter sets ddlReorgWorkerCounter count.
// Sysvar validation enforces the range to already be correct.
func SetDDLReorgWorkerCounter(cnt int32) {
	atomic.StoreInt32(&ddlReorgWorkerCounter, cnt)
}

// GetDDLReorgWorkerCounter gets ddlReorgWorkerCounter.
func GetDDLReorgWorkerCounter() int32 {
	return atomic.LoadInt32(&ddlReorgWorkerCounter)
}

// SetDDLReorgBatchSize sets ddlReorgBatchSize size.
// Sysvar validation enforces the range to already be correct.
func SetDDLReorgBatchSize(cnt int32) {
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
	coll, err := collate.GetCollationByName(normalizedValue)
	if err != nil {
		return normalizedValue, errors.Trace(err)
	}
	return coll.Name, nil
}

func checkCharacterSet(normalizedValue string, argName string) (string, error) {
	if normalizedValue == "" {
		return normalizedValue, errors.Trace(ErrWrongValueForVar.GenWithStackByArgs(argName, "NULL"))
	}
	cs, err := charset.GetCharsetInfo(normalizedValue)
	if err != nil {
		return normalizedValue, errors.Trace(err)
	}
	return cs.Name, nil
}

// checkReadOnly requires TiDBEnableNoopFuncs=1 for the same scope otherwise an error will be returned.
func checkReadOnly(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag, offlineMode bool) (string, error) {
	errMsg := ErrFunctionsNoopImpl.GenWithStackByArgs("READ ONLY")
	if offlineMode {
		errMsg = ErrFunctionsNoopImpl.GenWithStackByArgs("OFFLINE MODE")
	}
	if TiDBOptOn(normalizedValue) {
		if scope == ScopeSession && vars.NoopFuncsMode != OnInt {
			if vars.NoopFuncsMode == OffInt {
				return Off, errMsg
			}
			vars.StmtCtx.AppendWarning(errMsg)
		}
		if scope == ScopeGlobal {
			val, err := vars.GlobalVarsAccessor.GetGlobalSysVar(TiDBEnableNoopFuncs)
			if err != nil {
				return originalValue, errUnknownSystemVariable.GenWithStackByArgs(TiDBEnableNoopFuncs)
			}
			if val == Off {
				return Off, errMsg
			}
			if val == Warn {
				vars.StmtCtx.AppendWarning(errMsg)
			}
		}
	}
	return normalizedValue, nil
}

func checkIsolationLevel(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
	if normalizedValue == "SERIALIZABLE" || normalizedValue == "READ-UNCOMMITTED" {
		returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
		if !TiDBOptOn(vars.systems[TiDBSkipIsolationLevelCheck]) {
			return normalizedValue, ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
		}
		vars.StmtCtx.AppendWarning(returnErr)
	}
	return normalizedValue, nil
}

// GetSessionOrGlobalSystemVar gets a system variable.
// If it is a session only variable, use the default value defined in code.
// Returns error if there is no such variable.
func GetSessionOrGlobalSystemVar(s *SessionVars, name string) (string, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if sv.HasNoneScope() {
		return sv.Value, nil
	}
	if sv.HasSessionScope() {
		// Populate the value to s.systems if it is not there already.
		// in future should be already loaded on session init
		if sv.GetSession != nil {
			// shortcut to the getter, we won't use the value
			return sv.GetSessionFromHook(s)
		}
		if _, ok := s.systems[sv.Name]; !ok {
			if sv.HasGlobalScope() {
				if val, err := s.GlobalVarsAccessor.GetGlobalSysVar(sv.Name); err == nil {
					s.systems[sv.Name] = val
				}
			} else {
				s.systems[sv.Name] = sv.Value // no global scope, use default
			}
		}
		return sv.GetSessionFromHook(s)
	}
	return sv.GetGlobalFromHook(s)
}

// GetGlobalSystemVar gets a global system variable.
func GetGlobalSystemVar(s *SessionVars, name string) (string, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return sv.GetGlobalFromHook(s)
}

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
	return vars.SetSystemVar(name, sVal)
}

// SetStmtVar sets system variable and updates SessionVars states.
func SetStmtVar(vars *SessionVars, name string, value string) error {
	name = strings.ToLower(name)
	sysVar := GetSysVar(name)
	if sysVar == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	sVal, err := sysVar.Validate(vars, value, ScopeSession)
	if err != nil {
		return err
	}
	return vars.SetStmtVar(name, sVal)
}

// Deprecated: Read the value from the mysql.tidb table.
// This supports the use case that a TiDB server *older* than 5.0 is a member of the cluster.
// i.e. system variables such as tidb_gc_concurrency, tidb_gc_enable, tidb_gc_life_time
// do not exist.
func getTiDBTableValue(vars *SessionVars, name, defaultVal string) (string, error) {
	val, err := vars.GlobalVarsAccessor.GetTiDBTableValue(name)
	if err != nil { // handle empty result or other errors
		return defaultVal, nil
	}
	return trueFalseToOnOff(val), nil
}

// Deprecated: Set the value from the mysql.tidb table.
// This supports the use case that a TiDB server *older* than 5.0 is a member of the cluster.
// i.e. system variables such as tidb_gc_concurrency, tidb_gc_enable, tidb_gc_life_time
// do not exist.
func setTiDBTableValue(vars *SessionVars, name, value, comment string) error {
	value = OnOffToTrueFalse(value)
	return vars.GlobalVarsAccessor.SetTiDBTableValue(name, value, comment)
}

// In mysql.tidb the convention has been to store the string value "true"/"false",
// but sysvars use the convention ON/OFF.
func trueFalseToOnOff(str string) string {
	if strings.EqualFold("true", str) {
		return On
	} else if strings.EqualFold("false", str) {
		return Off
	}
	return str
}

// OnOffToTrueFalse convert "ON"/"OFF" to "true"/"false".
// In mysql.tidb the convention has been to store the string value "true"/"false",
// but sysvars use the convention ON/OFF.
func OnOffToTrueFalse(str string) string {
	if strings.EqualFold("ON", str) {
		return "true"
	} else if strings.EqualFold("OFF", str) {
		return "false"
	}
	return str
}

const (
	// initChunkSizeUpperBound indicates upper bound value of tidb_init_chunk_size.
	initChunkSizeUpperBound = 32
	// maxChunkSizeLowerBound indicates lower bound value of tidb_max_chunk_size.
	maxChunkSizeLowerBound = 32
)

// appendDeprecationWarning adds a warning that the item is deprecated.
func appendDeprecationWarning(s *SessionVars, name, replacement string) {
	s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, replacement))
}

// TiDBOptOn could be used for all tidb session variable options, we use "ON"/1 to turn on those options.
func TiDBOptOn(opt string) bool {
	return strings.EqualFold(opt, "ON") || opt == "1"
}

const (
	// OffInt is used by TiDBOptOnOffWarn
	OffInt = 0
	// OnInt is used TiDBOptOnOffWarn
	OnInt = 1
	// WarnInt is used by TiDBOptOnOffWarn
	WarnInt = 2
)

// TiDBOptOnOffWarn converts On/Off/Warn to an int.
// It is used for MultiStmtMode and NoopFunctionsMode
func TiDBOptOnOffWarn(opt string) int {
	switch opt {
	case Warn:
		return WarnInt
	case On:
		return OnInt
	}
	return OffInt
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

// AssertionLevel controls the assertion that will be performed during transactions.
type AssertionLevel int

const (
	// AssertionLevelOff indicates no assertion should be performed.
	AssertionLevelOff AssertionLevel = iota
	// AssertionLevelFast indicates assertions that doesn't affect performance should be performed.
	AssertionLevelFast
	// AssertionLevelStrict indicates full assertions should be performed, even if the performance might be slowed down.
	AssertionLevelStrict
)

func tidbOptAssertionLevel(opt string) AssertionLevel {
	switch opt {
	case AssertionStrictStr:
		return AssertionLevelStrict
	case AssertionFastStr:
		return AssertionLevelFast
	case AssertionOffStr:
		return AssertionLevelOff
	default:
		return AssertionLevelOff
	}
}

func tidbOptPositiveInt32(opt string, defaultVal int) int {
	val, err := strconv.Atoi(opt)
	if err != nil || val <= 0 {
		return defaultVal
	}
	return val
}

// TidbOptInt converts a string to an int
func TidbOptInt(opt string, defaultVal int) int {
	val, err := strconv.Atoi(opt)
	if err != nil {
		return defaultVal
	}
	return val
}

// TidbOptInt64 converts a string to an int64
func TidbOptInt64(opt string, defaultVal int64) int64 {
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
		s.SnapshotInfoschema = nil
		return nil
	}
	if s.ReadStaleness != 0 {
		return fmt.Errorf("tidb_read_staleness should be clear before setting tidb_snapshot")
	}

	if tso, err := strconv.ParseUint(sVal, 10, 64); err == nil {
		s.SnapshotTS = tso
		return nil
	}

	t, err := types.ParseTime(s.StmtCtx, sVal, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return err
	}

	t1, err := t.GoTime(s.Location())
	s.SnapshotTS = oracle.GoTimeToTS(t1)
	// tx_read_ts should be mutual exclusive with tidb_snapshot
	s.TxnReadTS = NewTxnReadTS(0)
	return err
}

func setTxnReadTS(s *SessionVars, sVal string) error {
	if sVal == "" {
		s.TxnReadTS = NewTxnReadTS(0)
		return nil
	}

	t, err := types.ParseTime(s.StmtCtx, sVal, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return err
	}
	t1, err := t.GoTime(s.Location())
	if err != nil {
		return err
	}
	s.TxnReadTS = NewTxnReadTS(oracle.GoTimeToTS(t1))
	// tx_read_ts should be mutual exclusive with tidb_snapshot
	s.SnapshotTS = 0
	s.SnapshotInfoschema = nil
	return err
}

func setReadStaleness(s *SessionVars, sVal string) error {
	if sVal == "" || sVal == "0" {
		s.ReadStaleness = 0
		return nil
	}
	if s.SnapshotTS != 0 {
		return fmt.Errorf("tidb_snapshot should be clear before setting tidb_read_staleness")
	}
	sValue, err := strconv.ParseInt(sVal, 10, 32)
	if err != nil {
		return err
	}
	s.ReadStaleness = time.Duration(sValue) * time.Second
	return nil
}

func collectAllowFuncName4ExpressionIndex() string {
	str := make([]string, 0, len(GAFunction4ExpressionIndex))
	for funcName := range GAFunction4ExpressionIndex {
		str = append(str, funcName)
	}
	sort.Strings(str)
	return strings.Join(str, ", ")
}

// GAFunction4ExpressionIndex stores functions GA for expression index.
var GAFunction4ExpressionIndex = map[string]struct{}{
	ast.Lower:      {},
	ast.Upper:      {},
	ast.MD5:        {},
	ast.Reverse:    {},
	ast.VitessHash: {},
	ast.TiDBShard:  {},
}
