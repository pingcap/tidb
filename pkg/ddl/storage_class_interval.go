// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// storageClassIntervalMaxCount bounds <n> so that converting QUARTER and WEEK
// cannot overflow and the result stays within the range types.AddDate accepts.
const storageClassIntervalMaxCount = 10000 * 365

// storageClassInterval is a parsed `INTERVAL <n> <unit>` value of 'less_than'.
type storageClassInterval struct {
	count int64
	unit  ast.TimeUnitType
}

var storageClassIntervalUnits = map[string]ast.TimeUnitType{
	"YEAR":    ast.TimeUnitYear,
	"QUARTER": ast.TimeUnitQuarter,
	"MONTH":   ast.TimeUnitMonth,
	"WEEK":    ast.TimeUnitWeek,
	"DAY":     ast.TimeUnitDay,
}

// parseStorageClassInterval parses a 'less_than' value. isInterval is false
// when the value is a boundary literal rather than an interval.
func parseStorageClassInterval(value string) (_ storageClassInterval, isInterval bool, _ error) {
	fields := strings.Fields(value)
	if len(fields) == 0 || !strings.EqualFold(fields[0], "INTERVAL") {
		return storageClassInterval{}, false, nil
	}
	invalid := dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(fmt.Sprintf(
		"invalid 'less_than' interval: '%-.64s', expected 'INTERVAL <n> YEAR|QUARTER|MONTH|WEEK|DAY' with a positive integer <n>", value))
	if len(fields) != 3 {
		return storageClassInterval{}, true, invalid
	}
	for _, c := range fields[1] {
		if c < '0' || c > '9' {
			return storageClassInterval{}, true, invalid
		}
	}
	count, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || count <= 0 || count > storageClassIntervalMaxCount {
		return storageClassInterval{}, true, invalid
	}
	unit, ok := storageClassIntervalUnits[strings.ToUpper(fields[2])]
	if !ok {
		return storageClassInterval{}, true, invalid
	}
	return storageClassInterval{count: count, unit: unit}, true, nil
}

// cutoff returns now minus the interval in UTC, with MySQL month-end semantics.
func (i storageClassInterval) cutoff(now time.Time) (time.Time, error) {
	var years, months, days int64
	switch i.unit {
	case ast.TimeUnitYear:
		years = i.count
	case ast.TimeUnitQuarter:
		months = 3 * i.count
	case ast.TimeUnitMonth:
		months = i.count
	case ast.TimeUnitWeek:
		days = 7 * i.count
	default:
		days = i.count
	}
	cutoff, err := types.AddDate(-years, -months, -days, now.UTC())
	if err != nil || cutoff.Year() < 1 {
		return time.Time{}, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("'less_than' interval is out of range")
	}
	return cutoff, nil
}

// storageClassIntervalNow returns the time interval selectors are evaluated at.
func storageClassIntervalNow() time.Time {
	now := time.Now()
	failpoint.InjectCall("mockStorageClassIntervalNow", &now)
	return now.UTC()
}

type storageClassIntervalKeyKind int

const (
	storageClassIntervalKeyColumnTime storageClassIntervalKeyKind = iota
	storageClassIntervalKeyToDays
	storageClassIntervalKeyUnixTimestamp
)

func errStorageClassIntervalUnsupportedPartition() error {
	return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(
		"'less_than' interval only supports RANGE COLUMNS(DATE/DATETIME), RANGE(TO_DAYS(col)) or RANGE(UNIX_TIMESTAMP(col)) partitions")
}

// getStorageClassIntervalPartitionKey returns how a cutoff time maps onto the
// partition key of tbInfo, or an error if interval selectors are unsupported.
func getStorageClassIntervalPartitionKey(tbInfo *model.TableInfo) (storageClassIntervalKeyKind, *model.ColumnInfo, error) {
	pi := tbInfo.Partition
	if pi == nil || pi.Type != ast.PartitionTypeRange {
		return 0, nil, errStorageClassIntervalUnsupportedPartition()
	}
	if len(pi.Columns) > 0 {
		if len(pi.Columns) != 1 {
			return 0, nil, errStorageClassIntervalUnsupportedPartition()
		}
		col := findColumnByName(pi.Columns[0].L, tbInfo)
		if col == nil {
			return 0, nil, errStorageClassIntervalUnsupportedPartition()
		}
		switch col.GetType() {
		case mysql.TypeDate, mysql.TypeDatetime:
			return storageClassIntervalKeyColumnTime, col, nil
		}
		return 0, nil, errStorageClassIntervalUnsupportedPartition()
	}
	stmts, _, err := parser.New().ParseSQL("SELECT " + pi.Expr)
	if err != nil || len(stmts) != 1 {
		return 0, nil, errStorageClassIntervalUnsupportedPartition()
	}
	sel, ok := stmts[0].(*ast.SelectStmt)
	if !ok || sel.Fields == nil || len(sel.Fields.Fields) != 1 {
		return 0, nil, errStorageClassIntervalUnsupportedPartition()
	}
	fn, ok := sel.Fields.Fields[0].Expr.(*ast.FuncCallExpr)
	if !ok || len(fn.Args) != 1 {
		return 0, nil, errStorageClassIntervalUnsupportedPartition()
	}
	if _, ok := fn.Args[0].(*ast.ColumnNameExpr); !ok {
		return 0, nil, errStorageClassIntervalUnsupportedPartition()
	}
	switch fn.FnName.L {
	case ast.ToDays:
		return storageClassIntervalKeyToDays, nil, nil
	case ast.UnixTimestamp:
		return storageClassIntervalKeyUnixTimestamp, nil, nil
	}
	return 0, nil, errStorageClassIntervalUnsupportedPartition()
}

// storageClassIntervalCutoff is a cutoff time converted once per table to the
// partition key domain, so each partition boundary is compared by value.
type storageClassIntervalCutoff struct {
	kind storageClassIntervalKeyKind
	col  *model.ColumnInfo
	// time is used by storageClassIntervalKeyColumnTime.
	time types.Time
	// value is in days for TO_DAYS and in Unix seconds for UNIX_TIMESTAMP.
	value int64
}

func newStorageClassIntervalCutoff(tbInfo *model.TableInfo, interval storageClassInterval, now time.Time) (*storageClassIntervalCutoff, error) {
	kind, col, err := getStorageClassIntervalPartitionKey(tbInfo)
	if err != nil {
		return nil, err
	}
	cutoff, err := interval.cutoff(now)
	if err != nil {
		return nil, err
	}
	cutoffTime := types.NewTime(types.FromGoTime(cutoff), mysql.TypeDatetime, types.MaxFsp)
	result := &storageClassIntervalCutoff{kind: kind, col: col, time: cutoffTime}
	switch kind {
	case storageClassIntervalKeyToDays:
		// Same as TO_DAYS(cutoff): a partition matches only if all its dates
		// are before the cutoff date.
		result.value = types.TimestampDiff("DAY", types.ZeroDate, cutoffTime)
	case storageClassIntervalKeyUnixTimestamp:
		result.value = cutoff.Unix()
	}
	return result, nil
}

// matches reports whether a partition upper bound is <= the cutoff. MAXVALUE
// never matches an interval.
func (c *storageClassIntervalCutoff) matches(tbInfo *model.TableInfo, boundary string) (bool, error) {
	if strings.EqualFold(boundary, partitionMaxValue) {
		return false, nil
	}
	if c.kind != storageClassIntervalKeyColumnTime {
		cmp, err := compareNumericRangePartitionValues(tbInfo, boundary, strconv.FormatInt(c.value, 10))
		if err != nil {
			return false, err
		}
		return cmp <= 0, nil
	}
	literal := boundary
	if isSingleQuotedPartitionValue(literal) {
		literal = driver.UnwrapFromSingleQuotes(literal)
	}
	boundaryTime, err := types.ParseTime(types.StrictContext, literal, c.col.GetType(), types.MaxFsp)
	if err != nil {
		// Boundaries that are not plain literals fall back to expression evaluation.
		cmp, err := compareRangeColumnsPartitionValues(tbInfo, boundary, c.time.String())
		if err != nil {
			return false, err
		}
		return cmp <= 0, nil
	}
	return boundaryTime.Compare(c.time) <= 0, nil
}

// checkStorageClassIntervalDef validates the rule-level restrictions of an
// interval selector.
func checkStorageClassIntervalDef(def *model.StorageClassDef) error {
	if def.LessThan == nil {
		return nil
	}
	_, isInterval, err := parseStorageClassInterval(*def.LessThan)
	if err != nil || !isInterval {
		return err
	}
	if def.Tier != model.StorageClassTierIA {
		return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("'less_than' interval only supports tier 'IA'")
	}
	if len(def.Transitions) > 0 {
		return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("can not specify 'less_than' interval and 'transitions' together")
	}
	return nil
}

// checkStorageClassSelectorsForNewRequest validates selectors that only new
// CREATE/ALTER requests must satisfy. Existing rules keep their behavior when
// partitions are rebuilt or intervals are recalculated.
func checkStorageClassSelectorsForNewRequest(engineAttribute string, tbInfo *model.TableInfo) error {
	if engineAttribute == "" || tbInfo == nil {
		return nil
	}
	attr, err := model.ParseEngineAttributeFromString(engineAttribute)
	if err != nil {
		return dbterror.ErrEngineAttributeInvalidFormat.GenWithStackByArgs(fmt.Sprintf("'%v'", err))
	}
	if attr.StorageClass == nil {
		return nil
	}
	settings, err := BuildStorageClassSettingsFromJSON(attr.StorageClass)
	if err != nil {
		return err
	}
	for _, def := range settings.Defs {
		if def.LessThan == nil {
			continue
		}
		_, isInterval, err := parseStorageClassInterval(*def.LessThan)
		if err != nil {
			return err
		}
		if isInterval {
			if _, _, err := getStorageClassIntervalPartitionKey(tbInfo); err != nil {
				return err
			}
			continue
		}
		pi := tbInfo.Partition
		if pi != nil && pi.Type == ast.PartitionTypeRange && len(pi.Columns) == 0 && isStorageClassDateLiteral(*def.LessThan) {
			msg := fmt.Sprintf("'less_than' value '%-.64s' is a date, but the RANGE partition key is an integer; use an integer or an interval", *def.LessThan)
			return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
		}
	}
	return nil
}

// isStorageClassDateLiteral reports whether a 'less_than' value is a date or
// datetime literal rather than an integer or an expression.
func isStorageClassDateLiteral(value string) bool {
	value = strings.TrimSpace(value)
	if isSingleQuotedPartitionValue(value) {
		value = driver.UnwrapFromSingleQuotes(value)
	}
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return false
	}
	_, err := types.ParseDatetime(types.StrictContext, value)
	return err == nil
}
