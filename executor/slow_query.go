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

package executor

import (
	"bufio"
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/plancodec"
	"go.uber.org/zap"
)

//SlowQueryRetriever is used to read slow log data.
type SlowQueryRetriever struct {
	table       *model.TableInfo
	outputCols  []*model.ColumnInfo
	retrieved   bool
	initialized bool
	file        *os.File
	fileLine    int
}

func (e *SlowQueryRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		var err error
		e.file, err = os.Open(sctx.GetSessionVars().SlowQueryFile)
		if err != nil {
			return nil, err
		}
		e.initialized = true
	}
	rows, err := e.dataForSlowLog(sctx)
	if err != nil {
		return nil, err
	}
	if len(e.outputCols) == len(e.table.Columns) {
		return rows, nil
	}
	retRows := make([][]types.Datum, len(rows))
	for i, fullRow := range rows {
		row := make([]types.Datum, len(e.outputCols))
		for j, col := range e.outputCols {
			row[j] = fullRow[col.Offset]
		}
		retRows[i] = row
	}
	return retRows, nil
}

func (e *SlowQueryRetriever) close() error {
	if e.file != nil {
		err := e.file.Close()
		if err != nil {
			logutil.BgLogger().Error("close slow log file failed.", zap.Error(err))
		}
	}
	return nil
}

func (e *SlowQueryRetriever) dataForSlowLog(ctx sessionctx.Context) ([][]types.Datum, error) {
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(ctx); pm != nil {
		if pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.ProcessPriv) {
			hasProcessPriv = true
		}
	}
	user := ctx.GetSessionVars().User
	checkValid := func(userName string) bool {
		if !hasProcessPriv && user != nil && userName != user.Username {
			return false
		}
		return true
	}
	rows, fileLine, err := ParseSlowLog(ctx, bufio.NewReader(e.file), e.fileLine, 1024, checkValid)
	if err != nil {
		if err == io.EOF {
			e.retrieved = true
		} else {
			return nil, err
		}
	}
	e.fileLine = fileLine
	if e.table.Name.L == strings.ToLower(infoschema.ClusterTableSlowLog) {
		return infoschema.AppendHostInfoToRows(rows)
	}
	return rows, nil
}

type checkValidFunc func(string) bool

// ParseSlowLog exports for testing.
// TODO: optimize for parse huge log-file.
func ParseSlowLog(ctx sessionctx.Context, reader *bufio.Reader, fileLine, maxRow int, checkValid checkValidFunc) ([][]types.Datum, int, error) {
	var rows [][]types.Datum
	startFlag := false
	lineNum := fileLine
	tz := ctx.GetSessionVars().Location()
	var st *slowQueryTuple
	for {
		if len(rows) >= maxRow {
			return rows, lineNum, nil
		}
		lineNum++
		lineByte, err := getOneLine(reader)
		if err != nil {
			return rows, lineNum, err
		}
		line := string(hack.String(lineByte))
		// Check slow log entry start flag.
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			st = &slowQueryTuple{}
			valid, err := st.setFieldValue(tz, variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):], lineNum, checkValid)
			if err != nil {
				ctx.GetSessionVars().StmtCtx.AppendWarning(err)
				continue
			}
			if valid {
				startFlag = true
			}
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
						valid, err := st.setFieldValue(tz, field, fieldValues[i+1], lineNum, checkValid)
						if err != nil {
							ctx.GetSessionVars().StmtCtx.AppendWarning(err)
							continue
						}
						if !valid {
							startFlag = false
						}
					}
				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				// Get the sql string, and mark the start flag to false.
				_, err = st.setFieldValue(tz, variable.SlowLogQuerySQLStr, string(hack.Slice(line)), lineNum, checkValid)
				if err != nil {
					ctx.GetSessionVars().StmtCtx.AppendWarning(err)
					continue
				}
				if checkValid == nil || checkValid(st.user) {
					rows = append(rows, st.convertToDatumRow())
				}
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
	time               time.Time
	txnStartTs         uint64
	user               string
	host               string
	connID             uint64
	queryTime          float64
	parseTime          float64
	compileTime        float64
	preWriteTime       float64
	binlogPrewriteTime float64
	commitTime         float64
	getCommitTSTime    float64
	commitBackoffTime  float64
	backoffTypes       string
	resolveLockTime    float64
	localLatchWaitTime float64
	writeKeys          uint64
	writeSize          uint64
	prewriteRegion     uint64
	txnRetry           uint64
	processTime        float64
	waitTime           float64
	backOffTime        float64
	lockKeysTime       float64
	requestCount       uint64
	totalKeys          uint64
	processKeys        uint64
	db                 string
	indexIDs           string
	digest             string
	statsInfo          string
	avgProcessTime     float64
	p90ProcessTime     float64
	maxProcessTime     float64
	maxProcessAddress  string
	avgWaitTime        float64
	p90WaitTime        float64
	maxWaitTime        float64
	maxWaitAddress     string
	memMax             int64
	prevStmt           string
	sql                string
	isInternal         bool
	succ               bool
	plan               string
	planDigest         string
}

func (st *slowQueryTuple) setFieldValue(tz *time.Location, field, value string, lineNum int, checkValid checkValidFunc) (valid bool, err error) {
	valid = true
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
		if checkValid != nil {
			valid = checkValid(st.user)
		}
	case variable.SlowLogConnIDStr:
		st.connID, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogQueryTimeStr:
		st.queryTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogParseTimeStr:
		st.parseTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCompileTimeStr:
		st.compileTime, err = strconv.ParseFloat(value, 64)
	case execdetails.PreWriteTimeStr:
		st.preWriteTime, err = strconv.ParseFloat(value, 64)
	case execdetails.BinlogPrewriteTimeStr:
		st.binlogPrewriteTime, err = strconv.ParseFloat(value, 64)
	case execdetails.CommitTimeStr:
		st.commitTime, err = strconv.ParseFloat(value, 64)
	case execdetails.GetCommitTSTimeStr:
		st.getCommitTSTime, err = strconv.ParseFloat(value, 64)
	case execdetails.CommitBackoffTimeStr:
		st.commitBackoffTime, err = strconv.ParseFloat(value, 64)
	case execdetails.BackoffTypesStr:
		st.backoffTypes = value
	case execdetails.ResolveLockTimeStr:
		st.resolveLockTime, err = strconv.ParseFloat(value, 64)
	case execdetails.LocalLatchWaitTimeStr:
		st.localLatchWaitTime, err = strconv.ParseFloat(value, 64)
	case execdetails.WriteKeysStr:
		st.writeKeys, err = strconv.ParseUint(value, 10, 64)
	case execdetails.WriteSizeStr:
		st.writeSize, err = strconv.ParseUint(value, 10, 64)
	case execdetails.PrewriteRegionStr:
		st.prewriteRegion, err = strconv.ParseUint(value, 10, 64)
	case execdetails.TxnRetryStr:
		st.txnRetry, err = strconv.ParseUint(value, 10, 64)
	case execdetails.ProcessTimeStr:
		st.processTime, err = strconv.ParseFloat(value, 64)
	case execdetails.WaitTimeStr:
		st.waitTime, err = strconv.ParseFloat(value, 64)
	case execdetails.BackoffTimeStr:
		st.backOffTime, err = strconv.ParseFloat(value, 64)
	case execdetails.LockKeysTimeStr:
		st.lockKeysTime, err = strconv.ParseFloat(value, 64)
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
	case variable.SlowLogPlanDigest:
		st.planDigest = value
	case variable.SlowLogQuerySQLStr:
		st.sql = value
	}
	if err != nil {
		return valid, errors.Wrap(err, "Parse slow log at line "+strconv.FormatInt(int64(lineNum), 10)+" failed. Field: `"+field+"`, error")
	}
	return valid, err
}

func (st *slowQueryTuple) convertToDatumRow() []types.Datum {
	record := make([]types.Datum, 0, 64)
	record = append(record, types.NewTimeDatum(types.NewTime(types.FromGoTime(st.time), mysql.TypeDatetime, types.MaxFsp)))
	record = append(record, types.NewUintDatum(st.txnStartTs))
	record = append(record, types.NewStringDatum(st.user))
	record = append(record, types.NewStringDatum(st.host))
	record = append(record, types.NewUintDatum(st.connID))
	record = append(record, types.NewFloat64Datum(st.queryTime))
	record = append(record, types.NewFloat64Datum(st.parseTime))
	record = append(record, types.NewFloat64Datum(st.compileTime))
	record = append(record, types.NewFloat64Datum(st.preWriteTime))
	record = append(record, types.NewFloat64Datum(st.binlogPrewriteTime))
	record = append(record, types.NewFloat64Datum(st.commitTime))
	record = append(record, types.NewFloat64Datum(st.getCommitTSTime))
	record = append(record, types.NewFloat64Datum(st.commitBackoffTime))
	record = append(record, types.NewStringDatum(st.backoffTypes))
	record = append(record, types.NewFloat64Datum(st.resolveLockTime))
	record = append(record, types.NewFloat64Datum(st.localLatchWaitTime))
	record = append(record, types.NewUintDatum(st.writeKeys))
	record = append(record, types.NewUintDatum(st.writeSize))
	record = append(record, types.NewUintDatum(st.prewriteRegion))
	record = append(record, types.NewUintDatum(st.txnRetry))
	record = append(record, types.NewFloat64Datum(st.processTime))
	record = append(record, types.NewFloat64Datum(st.waitTime))
	record = append(record, types.NewFloat64Datum(st.backOffTime))
	record = append(record, types.NewFloat64Datum(st.lockKeysTime))
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
	record = append(record, types.NewStringDatum(st.planDigest))
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
