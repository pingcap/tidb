// Copyright 2019 PingCAP, Inc.
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

package infoschema

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	log "github.com/sirupsen/logrus"
)

var slowQueryCols = []columnInfo{
	{variable.SlowLogTimeStr, mysql.TypeDatetime, -1, 0, nil, nil},
	{variable.SlowLogTxnStartTSStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogUserStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogConnIDStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogQueryTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.ProcessTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.WaitTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.BackoffTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.RequestCountStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.TotalKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.ProcessedKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogDBStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogIsInternalStr, mysql.TypeTiny, 1, 0, nil, nil},
	{variable.SlowLogQuerySQLStr, mysql.TypeVarchar, 4096, 0, nil, nil},
}

func dataForSlowLog(ctx sessionctx.Context) ([][]types.Datum, error) {
	rowsMap, err := parseSlowLogFile(ctx.GetSessionVars().SlowQueryFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var rows [][]types.Datum
	for _, row := range rowsMap {
		record := make([]types.Datum, 0, len(slowQueryCols))
		for _, col := range slowQueryCols {
			if v, ok := row[col.name]; ok {
				record = append(record, v)
			} else {
				record = append(record, types.NewDatum(nil))
			}
		}
		rows = append(rows, record)
	}
	return rows, nil
}

// TODO: Support parse multiple log-files.
// parseSlowLogFile uses to parse slow log file.
func parseSlowLogFile(filePath string) ([]map[string]types.Datum, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Error(err)
		}
	}()

	return parseSlowLog(bufio.NewScanner(file))
}

// TODO: optimize for parse huge log-file.
func parseSlowLog(scanner *bufio.Scanner) ([]map[string]types.Datum, error) {
	rows := make([]map[string]types.Datum, 0)
	rowMap := make(map[string]types.Datum, len(slowQueryCols))
	startFlag := false

	for scanner.Scan() {
		line := scanner.Text()
		// Check slow log entry start flag.
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			value, err := parseSlowLogField(variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):])
			if err != nil {
				log.Errorf("parse slow log error: %v", err)
				continue
			}
			rowMap[variable.SlowLogTimeStr] = *value
			startFlag = true
			continue
		}

		if startFlag {
			// Parse slow log field.
			if strings.Contains(line, variable.SlowLogPrefixStr) {
				line = line[len(variable.SlowLogPrefixStr):]
				fieldValues := strings.Split(line, " ")
				for i := 0; i < len(fieldValues)-1; i += 2 {
					field := fieldValues[i]
					if strings.HasSuffix(field, ":") {
						field = field[:len(field)-1]
					}
					value, err := parseSlowLogField(field, fieldValues[i+1])
					if err != nil {
						log.Errorf("parse slow log error: %v", err)
						continue
					}
					rowMap[field] = *value

				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				// Get the sql string, and mark the start flag to false.
				rowMap[variable.SlowLogQuerySQLStr] = types.NewStringDatum(string(hack.Slice(line)))
				rows = append(rows, rowMap)
				rowMap = make(map[string]types.Datum, len(slowQueryCols))
				startFlag = false
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func parseSlowLogField(field, value string) (*types.Datum, error) {
	col := findColumnByName(slowQueryCols, field)
	if col == nil {
		return nil, errors.Errorf("can't found column %v", field)
	}
	var val types.Datum
	switch col.tp {
	case mysql.TypeLonglong:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		val = types.NewUintDatum(num)
	case mysql.TypeVarchar:
		val = types.NewStringDatum(value)
	case mysql.TypeDouble:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		val = types.NewDatum(num)
	case mysql.TypeTiny:
		// parse bool
		val = types.NewDatum(value == "true")
	case mysql.TypeDatetime:
		t, err := parseTime(value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		val = types.NewTimeDatum(types.Time{
			Time: types.FromGoTime(t),
			Type: mysql.TypeDatetime,
			Fsp:  types.MaxFsp,
		})
	}
	return &val, nil
}

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(logutil.SlowLogTimeFormat, s)
	if err != nil {
		err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, logutil.SlowLogTimeFormat, err)
	}
	return t, err
}

func findColumnByName(cols []columnInfo, colName string) *columnInfo {
	for _, col := range cols {
		if col.name == colName {
			return &col
		}
	}
	return nil
}
