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
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
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

// SetDDLFlashbackConcurrency sets ddlFlashbackConcurrency count.
// Sysvar validation enforces the range to already be correct.
func SetDDLFlashbackConcurrency(cnt int32) {
	atomic.StoreInt32(&ddlFlashbackConcurrency, cnt)
}

// GetDDLFlashbackConcurrency gets ddlFlashbackConcurrency count.
func GetDDLFlashbackConcurrency() int32 {
	return atomic.LoadInt32(&ddlFlashbackConcurrency)
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
	atomic.StoreInt64(&ddlErrorCountLimit, cnt)
}

// GetDDLErrorCountLimit gets ddlErrorCountlimit size.
func GetDDLErrorCountLimit() int64 {
	return atomic.LoadInt64(&ddlErrorCountLimit)
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

func checkDefaultCollationForUTF8MB4(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
	coll, err := collate.GetCollationByName(normalizedValue)
	if err != nil {
		return normalizedValue, errors.Trace(err)
	}
	if !collate.IsDefaultCollationForUTF8MB4(coll.Name) {
		return "", ErrInvalidDefaultUTF8MB4Collation.GenWithStackByArgs(coll.Name)
	}
	return coll.Name, nil
}

func checkCharacterSet(normalizedValue string, argName string) (string, error) {
	if normalizedValue == "" {
		return normalizedValue, errors.Trace(ErrWrongValueForVar.FastGenByArgs(argName, "NULL"))
	}
	cs, err := charset.GetCharsetInfo(normalizedValue)
	if err != nil {
		return normalizedValue, errors.Trace(err)
	}
	return cs.Name, nil
}

// checkReadOnly requires TiDBEnableNoopFuncs=1 for the same scope otherwise an error will be returned.
func checkReadOnly(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag, offlineMode bool) (string, error) {
	errMsg := ErrFunctionsNoopImpl.FastGenByArgs("READ ONLY")
	if offlineMode {
		errMsg = ErrFunctionsNoopImpl.FastGenByArgs("OFFLINE MODE")
	}
	if TiDBOptOn(normalizedValue) {
		if scope == ScopeSession && vars.NoopFuncsMode != OnInt {
			if vars.NoopFuncsMode == OffInt {
				return Off, errors.Trace(errMsg)
			}
			vars.StmtCtx.AppendWarning(errMsg)
		}
		if scope == ScopeGlobal {
			val, err := vars.GlobalVarsAccessor.GetGlobalSysVar(TiDBEnableNoopFuncs)
			if err != nil {
				return originalValue, errUnknownSystemVariable.GenWithStackByArgs(TiDBEnableNoopFuncs)
			}
			if val == Off {
				return Off, errors.Trace(errMsg)
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
		returnErr := ErrUnsupportedIsolationLevel.FastGenByArgs(normalizedValue)
		if !TiDBOptOn(vars.systems[TiDBSkipIsolationLevelCheck]) {
			return normalizedValue, ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
		}
		vars.StmtCtx.AppendWarning(returnErr)
	}
	return normalizedValue, nil
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

// TidbOptUint64 converts a string to an uint64.
func TidbOptUint64(opt string, defaultVal uint64) uint64 {
	val, err := strconv.ParseUint(opt, 10, 64)
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

func parseMemoryLimit(s *SessionVars, normalizedValue string, originalValue string) (byteSize uint64, normalizedStr string, err error) {
	defer func() {
		if err == nil && byteSize > 0 && byteSize < (512<<20) {
			s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(TiDBServerMemoryLimit, originalValue))
			byteSize = 512 << 20
			normalizedStr = "512MB"
		}
	}()

	// 1. Try parse percentage format: x%
	if total := memory.GetMemTotalIgnoreErr(); total != 0 {
		perc, str := parsePercentage(normalizedValue)
		if perc != 0 {
			intVal := total / 100 * perc
			return intVal, str, nil
		}
	}

	// 2. Try parse byteSize format: xKB/MB/GB/TB or byte number
	bt, str := parseByteSize(normalizedValue)
	if str != "" {
		return bt, str, nil
	}

	return 0, "", ErrTruncatedWrongValue.GenWithStackByArgs(TiDBServerMemoryLimit, originalValue)
}

func parsePercentage(s string) (percentage uint64, normalizedStr string) {
	defer func() {
		if percentage == 0 || percentage >= 100 {
			percentage, normalizedStr = 0, ""
		}
	}()
	var endString string
	if n, err := fmt.Sscanf(s, "%d%%%s", &percentage, &endString); n == 1 && err == io.EOF {
		return percentage, fmt.Sprintf("%d%%", percentage)
	}
	return 0, ""
}

func parseByteSize(s string) (byteSize uint64, normalizedStr string) {
	var endString string
	if n, err := fmt.Sscanf(s, "%d%s", &byteSize, &endString); n == 1 && err == io.EOF {
		return byteSize, fmt.Sprintf("%d", byteSize)
	}
	if n, err := fmt.Sscanf(s, "%dKB%s", &byteSize, &endString); n == 1 && err == io.EOF {
		return byteSize << 10, fmt.Sprintf("%dKB", byteSize)
	}
	if n, err := fmt.Sscanf(s, "%dMB%s", &byteSize, &endString); n == 1 && err == io.EOF {
		return byteSize << 20, fmt.Sprintf("%dMB", byteSize)
	}
	if n, err := fmt.Sscanf(s, "%dGB%s", &byteSize, &endString); n == 1 && err == io.EOF {
		return byteSize << 30, fmt.Sprintf("%dGB", byteSize)
	}
	if n, err := fmt.Sscanf(s, "%dTB%s", &byteSize, &endString); n == 1 && err == io.EOF {
		return byteSize << 40, fmt.Sprintf("%dTB", byteSize)
	}
	return 0, ""
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

	tso, err := parseTSFromNumberOrTime(s, sVal)
	s.SnapshotTS = tso
	// tx_read_ts should be mutual exclusive with tidb_snapshot
	s.TxnReadTS = NewTxnReadTS(0)
	return err
}

func parseTSFromNumberOrTime(s *SessionVars, sVal string) (uint64, error) {
	if tso, err := strconv.ParseUint(sVal, 10, 64); err == nil {
		return tso, nil
	}

	t, err := types.ParseTime(s.StmtCtx.TypeCtx(), sVal, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return 0, err
	}

	t1, err := t.GoTime(s.Location())
	return oracle.GoTimeToTS(t1), err
}

func setTxnReadTS(s *SessionVars, sVal string) error {
	if sVal == "" {
		s.TxnReadTS = NewTxnReadTS(0)
		return nil
	}

	t, err := types.ParseTime(s.StmtCtx.TypeCtx(), sVal, mysql.TypeTimestamp, types.MaxFsp)
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

// switchDDL turns on/off DDL in an instance.
func switchDDL(on bool) error {
	if on && EnableDDL != nil {
		return EnableDDL()
	} else if !on && DisableDDL != nil {
		return DisableDDL()
	}
	return nil
}

func collectAllowFuncName4ExpressionIndex() string {
	str := make([]string, 0, len(GAFunction4ExpressionIndex))
	for funcName := range GAFunction4ExpressionIndex {
		str = append(str, funcName)
	}
	slices.Sort(str)
	return strings.Join(str, ", ")
}

func updatePasswordValidationLength(s *SessionVars, length int32) error {
	err := s.GlobalVarsAccessor.SetGlobalSysVarOnly(context.Background(), ValidatePasswordLength, strconv.FormatInt(int64(length), 10), false)
	if err != nil {
		return err
	}
	PasswordValidationLength.Store(length)
	return nil
}

// GAFunction4ExpressionIndex stores functions GA for expression index.
var GAFunction4ExpressionIndex = map[string]struct{}{
	ast.Lower:      {},
	ast.Upper:      {},
	ast.MD5:        {},
	ast.Reverse:    {},
	ast.VitessHash: {},
	ast.TiDBShard:  {},
	// JSON functions.
	ast.JSONType:          {},
	ast.JSONExtract:       {},
	ast.JSONUnquote:       {},
	ast.JSONArray:         {},
	ast.JSONObject:        {},
	ast.JSONSet:           {},
	ast.JSONInsert:        {},
	ast.JSONReplace:       {},
	ast.JSONRemove:        {},
	ast.JSONContains:      {},
	ast.JSONContainsPath:  {},
	ast.JSONValid:         {},
	ast.JSONArrayAppend:   {},
	ast.JSONArrayInsert:   {},
	ast.JSONMergePatch:    {},
	ast.JSONMergePreserve: {},
	ast.JSONPretty:        {},
	ast.JSONQuote:         {},
	ast.JSONSchemaValid:   {},
	ast.JSONSearch:        {},
	ast.JSONStorageSize:   {},
	ast.JSONDepth:         {},
	ast.JSONKeys:          {},
	ast.JSONLength:        {},
}

var analyzeSkipAllowedTypes = map[string]struct{}{
	"json":       {},
	"text":       {},
	"mediumtext": {},
	"longtext":   {},
	"blob":       {},
	"mediumblob": {},
	"longblob":   {},
}

// ValidAnalyzeSkipColumnTypes makes validation for tidb_analyze_skip_column_types.
func ValidAnalyzeSkipColumnTypes(val string) (string, error) {
	if val == "" {
		return "", nil
	}
	items := strings.Split(strings.ToLower(val), ",")
	columnTypes := make([]string, 0, len(items))
	for _, item := range items {
		columnType := strings.TrimSpace(item)
		if _, ok := analyzeSkipAllowedTypes[columnType]; !ok {
			return val, ErrWrongValueForVar.GenWithStackByArgs(TiDBAnalyzeSkipColumnTypes, val)
		}
		columnTypes = append(columnTypes, columnType)
	}
	return strings.Join(columnTypes, ","), nil
}

// ParseAnalyzeSkipColumnTypes converts tidb_analyze_skip_column_types to the map form.
func ParseAnalyzeSkipColumnTypes(val string) map[string]struct{} {
	skipTypes := make(map[string]struct{})
	for _, columnType := range strings.Split(strings.ToLower(val), ",") {
		if _, ok := analyzeSkipAllowedTypes[columnType]; ok {
			skipTypes[columnType] = struct{}{}
		}
	}
	return skipTypes
}
