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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
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

// getStorageClassIntervalKeyKind returns how a cutoff time maps onto the
// partition key of tbInfo, or an error if interval selectors are unsupported.
func getStorageClassIntervalKeyKind(tbInfo *model.TableInfo) (storageClassIntervalKeyKind, error) {
	pi := tbInfo.Partition
	if pi == nil || pi.Type != ast.PartitionTypeRange {
		return 0, errStorageClassIntervalUnsupportedPartition()
	}
	if len(pi.Columns) > 0 {
		if len(pi.Columns) != 1 {
			return 0, errStorageClassIntervalUnsupportedPartition()
		}
		col := findColumnByName(pi.Columns[0].L, tbInfo)
		if col == nil {
			return 0, errStorageClassIntervalUnsupportedPartition()
		}
		switch col.GetType() {
		case mysql.TypeDate, mysql.TypeDatetime:
			return storageClassIntervalKeyColumnTime, nil
		}
		return 0, errStorageClassIntervalUnsupportedPartition()
	}
	expr, err := generatedexpr.ParseExpression(pi.Expr)
	if err != nil {
		return 0, errStorageClassIntervalUnsupportedPartition()
	}
	fn, ok := expr.(*ast.FuncCallExpr)
	if !ok || len(fn.Args) != 1 {
		return 0, errStorageClassIntervalUnsupportedPartition()
	}
	if _, ok := fn.Args[0].(*ast.ColumnNameExpr); !ok {
		return 0, errStorageClassIntervalUnsupportedPartition()
	}
	switch fn.FnName.L {
	case ast.ToDays:
		return storageClassIntervalKeyToDays, nil
	case ast.UnixTimestamp:
		return storageClassIntervalKeyUnixTimestamp, nil
	}
	return 0, errStorageClassIntervalUnsupportedPartition()
}

// convertStorageClassIntervalToLessThan converts an interval to the 'less_than'
// literal it stands for at now, in the partition key domain, so partitions are
// matched exactly as for a literal selector.
func convertStorageClassIntervalToLessThan(tbInfo *model.TableInfo, interval storageClassInterval, now time.Time) (string, error) {
	kind, err := getStorageClassIntervalKeyKind(tbInfo)
	if err != nil {
		return "", err
	}
	cutoff, err := interval.cutoff(now)
	if err != nil {
		return "", err
	}
	cutoffTime := types.NewTime(types.FromGoTime(cutoff), mysql.TypeDatetime, types.MaxFsp)
	switch kind {
	case storageClassIntervalKeyToDays:
		// Same as TO_DAYS(cutoff): a partition matches only if all its dates
		// are before the cutoff date.
		return strconv.FormatInt(types.TimestampDiff("DAY", types.ZeroDate, cutoffTime), 10), nil
	case storageClassIntervalKeyUnixTimestamp:
		return strconv.FormatInt(cutoff.Unix(), 10), nil
	}
	return cutoffTime.String(), nil
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

// checkStorageClassIntervalPartitionKey rejects an interval selector on a table
// whose partition key it cannot map a cutoff onto. Only new CREATE/ALTER
// requests are checked; existing rules keep their behavior when partitions are
// rebuilt or intervals are recalculated.
func checkStorageClassIntervalPartitionKey(engineAttribute string, tbInfo *model.TableInfo) error {
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
		if !isInterval {
			continue
		}
		if _, err := getStorageClassIntervalKeyKind(tbInfo); err != nil {
			return err
		}
	}
	return nil
}
