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
	"io"
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
	"github.com/pingcap/tidb/util/plancodec"
	"go.uber.org/zap"
)

var slowQueryCols = []columnInfo{
	{variable.SlowLogTimeStr, mysql.TypeTimestamp, 26, 0, nil, nil},
	{variable.SlowLogTxnStartTSStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogUserStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogHostStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogConnIDStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogQueryTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.ProcessTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.WaitTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.BackoffTimeStr, mysql.TypeDouble, 22, 0, nil, nil},
	{execdetails.RequestCountStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.TotalKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{execdetails.ProcessKeysStr, mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
	{variable.SlowLogDBStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogIndexNamesStr, mysql.TypeVarchar, 100, 0, nil, nil},
	{variable.SlowLogIsInternalStr, mysql.TypeTiny, 1, 0, nil, nil},
	{variable.SlowLogDigestStr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogStatsInfoStr, mysql.TypeVarchar, 512, 0, nil, nil},
	{variable.SlowLogCopProcAvg, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopProcP90, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopProcMax, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopProcAddr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogCopWaitAvg, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopWaitP90, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopWaitMax, mysql.TypeDouble, 22, 0, nil, nil},
	{variable.SlowLogCopWaitAddr, mysql.TypeVarchar, 64, 0, nil, nil},
	{variable.SlowLogMemMax, mysql.TypeLonglong, 20, 0, nil, nil},
	{variable.SlowLogSucc, mysql.TypeTiny, 1, 0, nil, nil},
	{variable.SlowLogPlan, mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{variable.SlowLogPrevStmt, mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
	{variable.SlowLogQuerySQLStr, mysql.TypeLongBlob, types.UnspecifiedLength, 0, nil, nil},
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
			logutil.BgLogger().Error("close slow log file failed.", zap.String("file", filePath), zap.Error(err))
		}
	}()
	return ParseSlowLog(tz, bufio.NewReader(file))
}

// ParseSlowLog exports for testing.
// TODO: optimize for parse huge log-file.
func ParseSlowLog(tz *time.Location, reader *bufio.Reader) ([][]types.Datum, error) {
	var rows [][]types.Datum
	startFlag := false
	var st *slowQueryTuple
	for {
		lineByte, err := getOneLine(reader)
		if err != nil {
			if err == io.EOF {
				return rows, nil
			}
			return rows, err
		}
		line := string(hack.String(lineByte))
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
			if strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
				line = line[len(variable.SlowLogRowPrefixStr):]
				if strings.HasPrefix(line, variable.SlowLogPrevStmtPrefix) {
					st.prevStmt = line[len(variable.SlowLogPrevStmtPrefix):]
				} else {
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
				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				// Get the sql string, and mark the start flag to false.
				err = st.setFieldValue(tz, variable.SlowLogQuerySQLStr, string(hack.Slice(line)))
				if err != nil {
					return rows, err
				}
				rows = append(rows, st.convertToDatumRow())
				startFlag = false
			} else {
				startFlag = false
			}
		}
	}
}

func getOneLine(reader *bufio.Reader) ([]byte, error) {
	var resByte []byte
	lineByte, isPrefix, err := reader.ReadLine()
	if isPrefix {
		// Need to read more data.
		resByte = make([]byte, len(lineByte), len(lineByte)*2)
	} else {
		resByte = make([]byte, len(lineByte))
	}
	// Use copy here to avoid shallow copy problem.
	copy(resByte, lineByte)
	if err != nil {
		return resByte, err
	}

	var tempLine []byte
	for isPrefix {
		tempLine, isPrefix, err = reader.ReadLine()
		resByte = append(resByte, tempLine...)

		// Use the max value of max_allowed_packet to check the single line length.
		if len(resByte) > int(variable.MaxOfMaxAllowedPacket) {
			return resByte, errors.Errorf("single line length exceeds limit: %v", variable.MaxOfMaxAllowedPacket)
		}
		if err != nil {
			return resByte, err
		}
	}
	return resByte, err
}

type slowQueryTuple struct {
	time              time.Time
	txnStartTs        uint64
	user              string
	host              string
	connID            uint64
	queryTime         float64
	processTime       float64
	waitTime          float64
	backOffTime       float64
	requestCount      uint64
	totalKeys         uint64
	processKeys       uint64
	db                string
	indexIDs          string
	digest            string
	statsInfo         string
	avgProcessTime    float64
	p90ProcessTime    float64
	maxProcessTime    float64
	maxProcessAddress string
	avgWaitTime       float64
	p90WaitTime       float64
	maxWaitTime       float64
	maxWaitAddress    string
	memMax            int64
	prevStmt          string
	sql               string
	isInternal        bool
	succ              bool
	plan              string
}

func (st *slowQueryTuple) setFieldValue(tz *time.Location, field, value string) error {
	var err error
	switch field {
	case variable.SlowLogTimeStr:
		st.time, err = ParseTime(value)
		if err != nil {
			break
		}
		if st.time.Location() != tz {
			st.time = st.time.In(tz)
		}
	case variable.SlowLogTxnStartTSStr:
		st.txnStartTs, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogUserStr:
		fields := strings.SplitN(value, "@", 2)
		if len(field) > 0 {
			st.user = fields[0]
		}
		if len(field) > 1 {
			st.host = fields[1]
		}
	case variable.SlowLogConnIDStr:
		st.connID, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogQueryTimeStr:
		st.queryTime, err = strconv.ParseFloat(value, 64)
	case execdetails.ProcessTimeStr:
		st.processTime, err = strconv.ParseFloat(value, 64)
	case execdetails.WaitTimeStr:
		st.waitTime, err = strconv.ParseFloat(value, 64)
	case execdetails.BackoffTimeStr:
		st.backOffTime, err = strconv.ParseFloat(value, 64)
	case execdetails.RequestCountStr:
		st.requestCount, err = strconv.ParseUint(value, 10, 64)
	case execdetails.TotalKeysStr:
		st.totalKeys, err = strconv.ParseUint(value, 10, 64)
	case execdetails.ProcessKeysStr:
		st.processKeys, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogDBStr:
		st.db = value
	case variable.SlowLogIndexNamesStr:
		st.indexIDs = value
	case variable.SlowLogIsInternalStr:
		st.isInternal = value == "true"
	case variable.SlowLogDigestStr:
		st.digest = value
	case variable.SlowLogStatsInfoStr:
		st.statsInfo = value
	case variable.SlowLogCopProcAvg:
		st.avgProcessTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCopProcP90:
		st.p90ProcessTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCopProcMax:
		st.maxProcessTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCopProcAddr:
		st.maxProcessAddress = value
	case variable.SlowLogCopWaitAvg:
		st.avgWaitTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCopWaitP90:
		st.p90WaitTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCopWaitMax:
		st.maxWaitTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCopWaitAddr:
		st.maxWaitAddress = value
	case variable.SlowLogMemMax:
		st.memMax, err = strconv.ParseInt(value, 10, 64)
	case variable.SlowLogSucc:
		st.succ, err = strconv.ParseBool(value)
	case variable.SlowLogPlan:
		st.plan = value
	case variable.SlowLogQuerySQLStr:
		st.sql = value
	}
	if err != nil {
		return errors.Wrap(err, "parse slow log failed `"+field+"` error")
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
	record = append(record, types.NewStringDatum(st.host))
	record = append(record, types.NewUintDatum(st.connID))
	record = append(record, types.NewFloat64Datum(st.queryTime))
	record = append(record, types.NewFloat64Datum(st.processTime))
	record = append(record, types.NewFloat64Datum(st.waitTime))
	record = append(record, types.NewFloat64Datum(st.backOffTime))
	record = append(record, types.NewUintDatum(st.requestCount))
	record = append(record, types.NewUintDatum(st.totalKeys))
	record = append(record, types.NewUintDatum(st.processKeys))
	record = append(record, types.NewStringDatum(st.db))
	record = append(record, types.NewStringDatum(st.indexIDs))
	record = append(record, types.NewDatum(st.isInternal))
	record = append(record, types.NewStringDatum(st.digest))
	record = append(record, types.NewStringDatum(st.statsInfo))
	record = append(record, types.NewFloat64Datum(st.avgProcessTime))
	record = append(record, types.NewFloat64Datum(st.p90ProcessTime))
	record = append(record, types.NewFloat64Datum(st.maxProcessTime))
	record = append(record, types.NewStringDatum(st.maxProcessAddress))
	record = append(record, types.NewFloat64Datum(st.avgWaitTime))
	record = append(record, types.NewFloat64Datum(st.p90WaitTime))
	record = append(record, types.NewFloat64Datum(st.maxWaitTime))
	record = append(record, types.NewStringDatum(st.maxWaitAddress))
	record = append(record, types.NewIntDatum(st.memMax))
	if st.succ {
		record = append(record, types.NewIntDatum(1))
	} else {
		record = append(record, types.NewIntDatum(0))
	}
	record = append(record, types.NewStringDatum(parsePlan(st.plan)))
	record = append(record, types.NewStringDatum(st.prevStmt))
	record = append(record, types.NewStringDatum(st.sql))
	return record
}

func parsePlan(planString string) string {
	if len(planString) <= len(variable.SlowLogPlanPrefix)+len(variable.SlowLogPlanSuffix) {
		return planString
	}
	planString = planString[len(variable.SlowLogPlanPrefix) : len(planString)-len(variable.SlowLogPlanSuffix)]
	decodePlanString, err := plancodec.DecodePlan(planString)
	if err == nil {
		planString = decodePlanString
	} else {
		logutil.BgLogger().Error("decode plan in slow log failed", zap.String("plan", planString), zap.Error(err))
	}
	return planString
}

// ParseTime exports for testing.
func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse(logutil.SlowLogTimeFormat, s)
	if err != nil {
		// This is for compatibility.
		t, err = time.Parse(logutil.OldSlowLogTimeFormat, s)
		if err != nil {
			err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, logutil.SlowLogTimeFormat, err)
		}
	}
	return t, err
}
