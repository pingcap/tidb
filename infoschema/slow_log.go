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
	"context"
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
	"github.com/pingcap/tidb/util/stringutil"
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
	{variable.SlowLogIndexIDsStr, mysql.TypeVarchar, 100, 0, nil, nil},
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
	{variable.SlowLogQuerySQLStr, mysql.TypeVarchar, 4096, 0, nil, nil},
}

func dataForSlowLog(ctx sessionctx.Context) ([][]types.Datum, error) {
	return ReadSlowLogData(ctx.GetSessionVars().SlowQueryFile, ctx.GetSessionVars().Location())
}

// ReadSlowLogData uses to read slow log data.
// It is exporting for testing.
// TODO: Support parse multiple log-files.
func ReadSlowLogData(filePath string, tz *time.Location) ([][]types.Datum, error) {
	return globalSlowQueryReader.getSlowLogData(filePath, tz)
}

// ParseSlowLogRows reads slow log data by parse slow log file.
// It won't use the cache data.
// It is exporting for testing.
func ParseSlowLogRows(filePath string, tz *time.Location) ([][]types.Datum, error) {
	tuples, err := parseSlowLogDataFromFile(tz, filePath, 0, nil, nil)
	if err != nil {
		return nil, err
	}
	return convertSlowLogTuplesToDatums(tuples), nil
}

// ReadSlowLogDataFromFile reads slow query data from slow log file.
// If filterFn(t) return false, will stop read and return directly.
// If bypassFn(t) return true, will bypass the current tuple.
// ReadSlowLogDataFromFile exports for testing.
func parseSlowLogDataFromFile(tz *time.Location, filePath string, offset int64, filterFn func(t time.Time) bool, bypassFn func(t time.Time) bool) ([]*slowQueryTuple, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			logutil.Logger(context.Background()).Error("close slow log file failed.", zap.String("file", filePath), zap.Error(err))
		}
	}()

	if offset > 0 {
		if _, err := file.Seek(offset, 0); err != nil {
			return nil, err
		}
	}
	var tuples []*slowQueryTuple
	reader := bufio.NewReader(file)
	startFlag := false
	currentOffset := offset
	var st *slowQueryTuple
	for {
		lineByte, err := getOneLine(reader)
		// `+ 1` means add the `\n` byte.
		currentOffset += int64(len(lineByte) + 1)
		if err != nil {
			if err == io.EOF {
				return tuples, nil
			}
			return tuples, err
		}
		line := string(hack.String(lineByte))
		// Check slow log entry start flag.
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			st = &slowQueryTuple{}
			err = st.setFieldValue(tz, variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):])
			if err != nil {
				return tuples, err
			}
			startFlag = true
			if filterFn != nil && !filterFn(st.time) {
				return tuples, err
			}
			if bypassFn != nil && bypassFn(st.time) {
				startFlag = false
			}
			continue
		}

		if startFlag {
			// Parse slow log field.
			if strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
				line = line[len(variable.SlowLogRowPrefixStr):]
				fieldValues := strings.Split(line, " ")
				for i := 0; i < len(fieldValues)-1; i += 2 {
					field := fieldValues[i]
					if strings.HasSuffix(field, ":") {
						field = field[:len(field)-1]
					}
					err = st.setFieldValue(tz, field, fieldValues[i+1])
					if err != nil {
						return tuples, err
					}
				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				// Get the sql string, and mark the start flag to false.
				err = st.setFieldValue(tz, variable.SlowLogQuerySQLStr, string(hack.Slice(line)))
				if err != nil {
					return tuples, err
				}
				st.endOffset = currentOffset
				tuples = append(tuples, st)
				startFlag = false
			} else {
				startFlag = false
			}
		}
	}
}

func getOneLine(reader *bufio.Reader) ([]byte, error) {
	lineByte, isPrefix, err := reader.ReadLine()
	if err != nil {
		return lineByte, err
	}
	var tempLine []byte
	for isPrefix {
		tempLine, isPrefix, err = reader.ReadLine()
		lineByte = append(lineByte, tempLine...)

		// Use the max value of max_allowed_packet to check the single line length.
		if len(lineByte) > int(variable.MaxOfMaxAllowedPacket) {
			return lineByte, errors.Errorf("single line length exceeds limit: %v", variable.MaxOfMaxAllowedPacket)
		}
		if err != nil {
			return lineByte, err
		}
	}
	return lineByte, err
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
	isInternal        bool
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
	sql               string
	// endOffset is the end offset in the file of this tuple.
	endOffset int64
}

func (st *slowQueryTuple) equal(st2 *slowQueryTuple) bool {
	return st.time.Equal(st2.time) &&
		st.txnStartTs == st2.txnStartTs &&
		st.user == st2.user &&
		st.host == st2.host &&
		st.connID == st2.connID &&
		st.sql == st2.sql
}

func (st *slowQueryTuple) setFieldValue(tz *time.Location, field, value string) error {
	value = stringutil.Copy(value)
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
		fields := strings.SplitN(value, "@", 2)
		if len(field) > 0 {
			st.user = fields[0]
		}
		if len(field) > 1 {
			st.host = fields[1]
		}
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
		st.indexIDs = value
	case variable.SlowLogIsInternalStr:
		st.isInternal = value == "true"
	case variable.SlowLogDigestStr:
		st.digest = value
	case variable.SlowLogStatsInfoStr:
		st.statsInfo = value
	case variable.SlowLogCopProcAvg:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.avgProcessTime = num
	case variable.SlowLogCopProcP90:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.p90ProcessTime = num
	case variable.SlowLogCopProcMax:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.maxProcessTime = num
	case variable.SlowLogCopProcAddr:
		st.maxProcessAddress = value
	case variable.SlowLogCopWaitAvg:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.avgWaitTime = num
	case variable.SlowLogCopWaitP90:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.p90WaitTime = num
	case variable.SlowLogCopWaitMax:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.maxWaitTime = num
	case variable.SlowLogCopWaitAddr:
		st.maxWaitAddress = value
	case variable.SlowLogMemMax:
		num, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return errors.AddStack(err)
		}
		st.memMax = num
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
	record = append(record, types.NewStringDatum(st.sql))
	return record
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

func convertSlowLogTuplesToDatums(tuples []*slowQueryTuple) [][]types.Datum {
	rows := make([][]types.Datum, len(tuples))
	for i := range tuples {
		rows[i] = tuples[i].convertToDatumRow()
	}
	return rows
}
