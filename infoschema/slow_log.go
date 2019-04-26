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
	{variable.SlowLogTimeStr, mysql.TypeTimestamp, 26, 0, nil, nil},
	{variable.SlowLogTxnStartTSStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogUserStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogConnIDStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogQueryTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.ProcessTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.WaitTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.BackoffTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.RequestCountStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.TotalKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.ProcessKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogDBStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogIndexIDsStr, mysql.TypeVarchar, 100, 0, nil, nil},
	{variable.SlowLogIsInternalStr, mysql.TypeTiny, 1, 0, nil, nil},
	{variable.SlowLogDigestStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogStatsInfoStr, mysql.TypeVarchar, 512, 0, nil, nil},
	{variable.SlowLogQuerySQLStr, mysql.TypeVarchar, 4096, 0, nil, nil},
}

func dataForSlowLog(ctx sessionctx.Context) ([][]types.Datum, error) {
	return parseSlowLogFile(ctx.GetSessionVars().Location(), ctx.GetSessionVars().SlowQueryFile)
}

// parseSlowLogFile uses to parse slow log file.
// TODO: Support parse multiple log-files.
func parseSlowLogFile(tz *time.Location, filePath string) ([][]types.Datum, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Error(err)
		}
	}()

	return ParseSlowLog(tz, bufio.NewScanner(file))
}

// ParseSlowLog exports for testing.
// TODO: optimize for parse huge log-file.
func ParseSlowLog(tz *time.Location, scanner *bufio.Scanner) ([][]types.Datum, error) {
	var rows [][]types.Datum
	startFlag := false
	var st *slowQueryTuple
	var err error
	for scanner.Scan() {
		line := scanner.Text()
		// Check slow log entry start flag.
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			st = &slowQueryTuple{}
			err = st.setFieldValue(tz, variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):])
			if err != nil {
				return rows, err
			}
			startFlag = true
			continue
		}

		if startFlag {
			// Parse slow log field.
			if strings.HasPrefix(line, variable.SlowLogPrefixStr) {
				line = line[len(variable.SlowLogPrefixStr):]
				fieldValues := strings.Split(line, " ")
				for i := 0; i < len(fieldValues)-1; i += 2 {
					field := fieldValues[i]
					if strings.HasSuffix(field, ":") {
						field = field[:len(field)-1]
					}
					err = st.setFieldValue(tz, field, fieldValues[i+1])
					if err != nil {
						return rows, err
					}
				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				// Get the sql string, and mark the start flag to false.
				err = st.setFieldValue(tz, variable.SlowLogQuerySQLStr, string(hack.Slice(line)))
				if err != nil {
					return rows, err
				}
				rows = append(rows, st.convertToDatumRow())
				startFlag = false
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.AddStack(err)
	}
	return rows, nil
}

type slowQueryTuple struct {
	time         time.Time
	txnStartTs   uint64
	user         string
	connID       uint64
	queryTime    float64
	processTime  float64
	waitTime     float64
	backOffTime  float64
	requestCount uint64
	totalKeys    uint64
	processKeys  uint64
	db           string
	indexNames   string
	isInternal   bool
	digest       string
	statsInfo    string
	sql          string
}

func (st *slowQueryTuple) setFieldValue(tz *time.Location, field, value string) error {
	switch field {
	case variable.SlowLogTimeStr:
		t, err := ParseTime(value)
		if err != nil {
			return err
		}
		if t.Location() != tz {
			t = t.In(tz)
		}
		st.time = t
	case variable.SlowLogTxnStartTSStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.txnStartTs = num
	case variable.SlowLogUserStr:
		st.user = value
	case variable.SlowLogConnIDStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.connID = num
	case variable.SlowLogQueryTimeStr:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.queryTime = num
	case execdetails.ProcessTimeStr:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.processTime = num
	case execdetails.WaitTimeStr:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.waitTime = num
	case execdetails.BackoffTimeStr:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.backOffTime = num
	case execdetails.RequestCountStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.requestCount = num
	case execdetails.TotalKeysStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.totalKeys = num
	case execdetails.ProcessKeysStr:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.processKeys = num
	case variable.SlowLogDBStr:
		st.db = value
	case variable.SlowLogIndexIDsStr:
		st.indexNames = value
	case variable.SlowLogIsInternalStr:
		st.isInternal = value == "true"
	case variable.SlowLogDigestStr:
		st.digest = value
	case variable.SlowLogStatsInfoStr:
		st.statsInfo = value
	case variable.SlowLogQuerySQLStr:
		st.sql = value
	}
	return nil
}

func (st *slowQueryTuple) convertToDatumRow() []types.Datum {
	record := make([]types.Datum, 0, len(slowQueryCols))
	record = append(record, types.NewTimeDatum(types.Time{
		Time: types.FromGoTime(st.time),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}))
	record = append(record, types.NewUintDatum(st.txnStartTs))
	record = append(record, types.NewStringDatum(st.user))
	record = append(record, types.NewUintDatum(st.connID))
	record = append(record, types.NewFloat64Datum(st.queryTime))
	record = append(record, types.NewFloat64Datum(st.processTime))
	record = append(record, types.NewFloat64Datum(st.waitTime))
	record = append(record, types.NewFloat64Datum(st.backOffTime))
	record = append(record, types.NewUintDatum(st.requestCount))
	record = append(record, types.NewUintDatum(st.totalKeys))
	record = append(record, types.NewUintDatum(st.processKeys))
	record = append(record, types.NewStringDatum(st.db))
	record = append(record, types.NewStringDatum(st.indexNames))
	record = append(record, types.NewDatum(st.isInternal))
	record = append(record, types.NewStringDatum(st.digest))
	record = append(record, types.NewStringDatum(st.statsInfo))
	record = append(record, types.NewStringDatum(st.sql))
	return record
}

// ParseTime exports for testing.
func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse(logutil.SlowLogTimeFormat, s)
	if err != nil {
		err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, logutil.SlowLogTimeFormat, err)
	}
	return t, err
}
