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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/plancodec"
	"go.uber.org/zap"
)

// ParseSlowLogBatchSize is the batch size of slow-log lines for a worker to parse, exported for testing.
var ParseSlowLogBatchSize = 64

// slowQueryRetriever is used to read slow log data.
type slowQueryRetriever struct {
	table       *model.TableInfo
	outputCols  []*model.ColumnInfo
	initialized bool
	extractor   *plannercore.SlowQueryExtractor
	files       []logFile
	fileIdx     int
	fileLine    int
	checker     *slowLogChecker

	taskList chan slowLogTask
	stats    *slowQueryRuntimeStats
}

func (e *slowQueryRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if !e.initialized {
		err := e.initialize(ctx, sctx)
		if err != nil {
			return nil, err
		}
		e.initializeAsyncParsing(ctx, sctx)
	}
	rows, retrieved, err := e.dataForSlowLog(ctx, sctx)
	if err != nil {
		return nil, err
	}
	if retrieved {
		return nil, nil
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

func (e *slowQueryRetriever) initialize(ctx context.Context, sctx sessionctx.Context) error {
	var err error
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		hasProcessPriv = pm.RequestVerification(sctx.GetSessionVars().ActiveRoles, "", "", "", mysql.ProcessPriv)
	}
	e.checker = &slowLogChecker{
		hasProcessPriv: hasProcessPriv,
		user:           sctx.GetSessionVars().User,
	}
	e.stats = &slowQueryRuntimeStats{}
	if e.extractor != nil {
		e.checker.enableTimeCheck = e.extractor.Enable
		for _, tr := range e.extractor.TimeRanges {
			startTime := types.NewTime(types.FromGoTime(tr.StartTime), mysql.TypeDatetime, types.MaxFsp)
			endTime := types.NewTime(types.FromGoTime(tr.EndTime), mysql.TypeDatetime, types.MaxFsp)
			timeRange := &timeRange{
				startTime: startTime,
				endTime:   endTime,
			}
			e.checker.timeRanges = append(e.checker.timeRanges, timeRange)
		}
	} else {
		e.extractor = &plannercore.SlowQueryExtractor{}
	}
	e.initialized = true
	e.files, err = e.getAllFiles(ctx, sctx, sctx.GetSessionVars().SlowQueryFile)
	if e.extractor.Desc {
		e.reverseLogFiles()
	}
	return err
}

func (e *slowQueryRetriever) reverseLogFiles() {
	for i := 0; i < len(e.files)/2; i++ {
		j := len(e.files) - i - 1
		e.files[i], e.files[j] = e.files[j], e.files[i]
	}
}

func (e *slowQueryRetriever) close() error {
	for _, f := range e.files {
		err := f.file.Close()
		if err != nil {
			logutil.BgLogger().Error("close slow log file failed.", zap.Error(err))
		}
	}
	return nil
}

type parsedSlowLog struct {
	rows [][]types.Datum
	err  error
}

func (e *slowQueryRetriever) getNextFile() *os.File {
	if e.fileIdx >= len(e.files) {
		return nil
	}
	file := e.files[e.fileIdx].file
	e.fileIdx++
	if e.stats != nil {
		stat, err := file.Stat()
		if err == nil {
			// ignore the err will be ok.
			e.stats.readFileSize += stat.Size()
			e.stats.readFileNum++
		}
	}
	return file
}

func (e *slowQueryRetriever) getPreviousFile() *os.File {
	fileIdx := e.fileIdx
	// fileIdx refer to the next file which should be read
	// so we need to set fileIdx to fileIdx - 2 to get the previous file.
	fileIdx = fileIdx - 2
	if fileIdx < 0 {
		return nil
	}
	file := e.files[fileIdx].file
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return nil
	}
	return file
}

func (e *slowQueryRetriever) parseDataForSlowLog(ctx context.Context, sctx sessionctx.Context) {
	file := e.getNextFile()
	if file == nil {
		close(e.taskList)
		return
	}
	reader := bufio.NewReader(file)
	e.parseSlowLog(ctx, sctx, reader, ParseSlowLogBatchSize)
}

func (e *slowQueryRetriever) dataForSlowLog(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, bool, error) {
	var (
		task slowLogTask
		ok   bool
	)
	for {
		select {
		case task, ok = <-e.taskList:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
		if !ok {
			return nil, true, nil
		}
		result := <-task.resultCh
		rows, err := result.rows, result.err
		if err != nil {
			return nil, false, err
		}
		if len(rows) == 0 {
			continue
		}
		if e.table.Name.L == strings.ToLower(infoschema.ClusterTableSlowLog) {
			rows, err := infoschema.AppendHostInfoToRows(sctx, rows)
			return rows, false, err
		}
		return rows, false, nil
	}
}

type slowLogChecker struct {
	// Below fields is used to check privilege.
	hasProcessPriv bool
	user           *auth.UserIdentity
	// Below fields is used to check slow log time valid.
	enableTimeCheck bool
	timeRanges      []*timeRange
}

type timeRange struct {
	startTime types.Time
	endTime   types.Time
}

func (sc *slowLogChecker) hasPrivilege(userName string) bool {
	return sc.hasProcessPriv || sc.user == nil || userName == sc.user.Username
}

func (sc *slowLogChecker) isTimeValid(t types.Time) bool {
	for _, tr := range sc.timeRanges {
		if sc.enableTimeCheck && (t.Compare(tr.startTime) >= 0 && t.Compare(tr.endTime) <= 0) {
			return true
		}
	}
	return !sc.enableTimeCheck
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

type offset struct {
	offset int
	length int
}

type slowLogTask struct {
	resultCh chan parsedSlowLog
}

type slowLogBlock []string

func (e *slowQueryRetriever) getBatchLog(ctx context.Context, reader *bufio.Reader, offset *offset, num int) ([][]string, error) {
	var line string
	log := make([]string, 0, num)
	var err error
	for i := 0; i < num; i++ {
		for {
			if isCtxDone(ctx) {
				return nil, ctx.Err()
			}
			e.fileLine++
			lineByte, err := getOneLine(reader)
			if err != nil {
				if err == io.EOF {
					e.fileLine = 0
					file := e.getNextFile()
					if file == nil {
						return [][]string{log}, nil
					}
					offset.length = len(log)
					reader.Reset(file)
					continue
				}
				return [][]string{log}, err
			}
			line = string(hack.String(lineByte))
			log = append(log, line)
			if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				if strings.HasPrefix(line, "use") || strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
					continue
				}
				break
			}
		}
	}
	return [][]string{log}, err
}

func (e *slowQueryRetriever) getBatchLogForReversedScan(ctx context.Context, reader *bufio.Reader, offset *offset, num int) ([][]string, error) {
	// reader maybe change when read previous file.
	inputReader := reader
	defer func() {
		file := e.getNextFile()
		if file != nil {
			inputReader.Reset(file)
		}
	}()
	var line string
	var logs []slowLogBlock
	var log []string
	var err error
	hasStartFlag := false
	scanPreviousFile := false
	for {
		if isCtxDone(ctx) {
			return nil, ctx.Err()
		}
		e.fileLine++
		lineByte, err := getOneLine(reader)
		if err != nil {
			if err == io.EOF {
				if len(log) == 0 {
					decomposedSlowLogTasks := decomposeToSlowLogTasks(logs, num)
					offset.length = len(decomposedSlowLogTasks)
					return decomposedSlowLogTasks, nil
				}
				e.fileLine = 0
				file := e.getPreviousFile()
				if file == nil {
					return decomposeToSlowLogTasks(logs, num), nil
				}
				reader = bufio.NewReader(file)
				scanPreviousFile = true
				continue
			}
			return nil, err
		}
		line = string(hack.String(lineByte))
		if !hasStartFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			hasStartFlag = true
		}
		if hasStartFlag {
			log = append(log, line)
			if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				if strings.HasPrefix(line, "use") || strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
					continue
				}
				logs = append(logs, log)
				if scanPreviousFile {
					break
				}
				log = make([]string, 0, 8)
				hasStartFlag = false
			}
		}
	}
	return decomposeToSlowLogTasks(logs, num), err
}

func decomposeToSlowLogTasks(logs []slowLogBlock, num int) [][]string {
	if len(logs) == 0 {
		return nil
	}

	//In reversed scan, We should reverse the blocks.
	last := len(logs) - 1
	for i := 0; i < len(logs)/2; i++ {
		logs[i], logs[last-i] = logs[last-i], logs[i]
	}

	decomposedSlowLogTasks := make([][]string, 0)
	log := make([]string, 0, num*len(logs[0]))
	for i := range logs {
		log = append(log, logs[i]...)
		if i > 0 && i%num == 0 {
			decomposedSlowLogTasks = append(decomposedSlowLogTasks, log)
			log = make([]string, 0, len(log))
		}
	}
	if len(log) > 0 {
		decomposedSlowLogTasks = append(decomposedSlowLogTasks, log)
	}
	return decomposedSlowLogTasks
}

func (e *slowQueryRetriever) parseSlowLog(ctx context.Context, sctx sessionctx.Context, reader *bufio.Reader, logNum int) {
	defer close(e.taskList)
	var wg util.WaitGroupWrapper
	offset := offset{offset: 0, length: 0}
	// To limit the num of go routine
	concurrent := sctx.GetSessionVars().Concurrency.DistSQLScanConcurrency()
	ch := make(chan int, concurrent)
	if e.stats != nil {
		e.stats.concurrent = concurrent
	}
	defer close(ch)
	for {
		startTime := time.Now()
		var logs [][]string
		var err error
		if !e.extractor.Desc {
			logs, err = e.getBatchLog(ctx, reader, &offset, logNum)
		} else {
			logs, err = e.getBatchLogForReversedScan(ctx, reader, &offset, logNum)
		}
		if err != nil {
			t := slowLogTask{}
			t.resultCh = make(chan parsedSlowLog, 1)
			e.taskList <- t
			e.sendParsedSlowLogCh(ctx, t, parsedSlowLog{nil, err})
		}
		if len(logs) == 0 || len(logs[0]) == 0 {
			break
		}
		if e.stats != nil {
			e.stats.readFile += time.Since(startTime)
		}
		failpoint.Inject("mockReadSlowLogSlow", func(val failpoint.Value) {
			if val.(bool) {
				signals := ctx.Value("signals").([]chan int)
				signals[0] <- 1
				<-signals[1]
			}
		})
		for i := range logs {
			log := logs[i]
			t := slowLogTask{}
			t.resultCh = make(chan parsedSlowLog, 1)
			start := offset
			ch <- 1
			select {
			case <-ctx.Done():
				return
			case e.taskList <- t:
			}
			wg.Run(func() {
				result, err := e.parseLog(ctx, sctx, log, start)
				e.sendParsedSlowLogCh(ctx, t, parsedSlowLog{result, err})
				<-ch
			})
			offset.offset = e.fileLine
			offset.length = 0
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
	wg.Wait()
}

func (e *slowQueryRetriever) sendParsedSlowLogCh(ctx context.Context, t slowLogTask, re parsedSlowLog) {
	select {
	case t.resultCh <- re:
	case <-ctx.Done():
		return
	}
}

func getLineIndex(offset offset, index int) int {
	var fileLine int
	if offset.length <= index {
		fileLine = index - offset.length + 1
	} else {
		fileLine = offset.offset + index + 1
	}
	return fileLine
}

func (e *slowQueryRetriever) parseLog(ctx context.Context, sctx sessionctx.Context, log []string, offset offset) (data [][]types.Datum, err error) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
		if e.stats != nil {
			atomic.AddInt64(&e.stats.parseLog, int64(time.Since(start)))
		}
	}()
	failpoint.Inject("errorMockParseSlowLogPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})
	var st *slowQueryTuple
	tz := sctx.GetSessionVars().Location()
	startFlag := false
	for index, line := range log {
		if isCtxDone(ctx) {
			return nil, ctx.Err()
		}
		fileLine := getLineIndex(offset, index)
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			st = &slowQueryTuple{}
			valid, err := st.setFieldValue(tz, variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):], fileLine, e.checker)
			if err != nil {
				sctx.GetSessionVars().StmtCtx.AppendWarning(err)
				continue
			}
			if valid {
				startFlag = true
			}
			continue
		}
		if startFlag {
			if strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
				line = line[len(variable.SlowLogRowPrefixStr):]
				if strings.HasPrefix(line, variable.SlowLogPrevStmtPrefix) {
					st.prevStmt = line[len(variable.SlowLogPrevStmtPrefix):]
				} else if strings.HasPrefix(line, variable.SlowLogUserAndHostStr+variable.SlowLogSpaceMarkStr) {
					value := line[len(variable.SlowLogUserAndHostStr+variable.SlowLogSpaceMarkStr):]
					valid, err := st.setFieldValue(tz, variable.SlowLogUserAndHostStr, value, fileLine, e.checker)
					if err != nil {
						sctx.GetSessionVars().StmtCtx.AppendWarning(err)
						continue
					}
					if !valid {
						startFlag = false
					}
				} else if strings.HasPrefix(line, variable.SlowLogCopBackoffPrefix) {
					valid, err := st.setFieldValue(tz, variable.SlowLogBackoffDetail, line, fileLine, e.checker)
					if err != nil {
						sctx.GetSessionVars().StmtCtx.AppendWarning(err)
						continue
					}
					if !valid {
						startFlag = false
					}
				} else {
					fieldValues := strings.Split(line, " ")
					for i := 0; i < len(fieldValues)-1; i += 2 {
						field := fieldValues[i]
						if strings.HasSuffix(field, ":") {
							field = field[:len(field)-1]
						}
						valid, err := st.setFieldValue(tz, field, fieldValues[i+1], fileLine, e.checker)
						if err != nil {
							sctx.GetSessionVars().StmtCtx.AppendWarning(err)
							continue
						}
						if !valid {
							startFlag = false
						}
					}
				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				if strings.HasPrefix(line, "use") {
					// `use DB` statements in the slow log is used to keep it be compatible with MySQL,
					// since we already get the current DB from the `# DB` field, we can ignore it here,
					// please see https://github.com/pingcap/tidb/issues/17846 for more details.
					continue
				}
				// Get the sql string, and mark the start flag to false.
				_, err := st.setFieldValue(tz, variable.SlowLogQuerySQLStr, string(hack.Slice(line)), fileLine, e.checker)
				if err != nil {
					sctx.GetSessionVars().StmtCtx.AppendWarning(err)
					continue
				}
				if e.checker.hasPrivilege(st.user) {
					data = append(data, st.convertToDatumRow())
				}
				startFlag = false
			} else {
				startFlag = false
			}
		}
	}
	return data, nil
}

type slowQueryTuple struct {
	time                      types.Time
	txnStartTs                uint64
	user                      string
	host                      string
	connID                    uint64
	execRetryCount            uint64
	execRetryTime             float64
	queryTime                 float64
	parseTime                 float64
	compileTime               float64
	rewriteTime               float64
	preprocSubqueries         uint64
	preprocSubQueryTime       float64
	optimizeTime              float64
	waitTSTime                float64
	preWriteTime              float64
	waitPrewriteBinlogTime    float64
	commitTime                float64
	getCommitTSTime           float64
	commitBackoffTime         float64
	backoffTypes              string
	resolveLockTime           float64
	localLatchWaitTime        float64
	writeKeys                 uint64
	writeSize                 uint64
	prewriteRegion            uint64
	txnRetry                  uint64
	copTime                   float64
	processTime               float64
	waitTime                  float64
	backOffTime               float64
	lockKeysTime              float64
	requestCount              uint64
	totalKeys                 uint64
	processKeys               uint64
	db                        string
	indexIDs                  string
	digest                    string
	statsInfo                 string
	avgProcessTime            float64
	p90ProcessTime            float64
	maxProcessTime            float64
	maxProcessAddress         string
	avgWaitTime               float64
	p90WaitTime               float64
	maxWaitTime               float64
	maxWaitAddress            string
	memMax                    int64
	diskMax                   int64
	prevStmt                  string
	sql                       string
	isInternal                bool
	succ                      bool
	planFromCache             bool
	planFromBinding           bool
	prepared                  bool
	kvTotal                   float64
	pdTotal                   float64
	backoffTotal              float64
	writeSQLRespTotal         float64
	plan                      string
	planDigest                string
	backoffDetail             string
	rocksdbDeleteSkippedCount uint64
	rocksdbKeySkippedCount    uint64
	rocksdbBlockCacheCount    uint64
	rocksdbBlockReadCount     uint64
	rocksdbBlockReadByte      uint64
}

func (st *slowQueryTuple) setFieldValue(tz *time.Location, field, value string, lineNum int, checker *slowLogChecker) (valid bool, err error) {
	valid = true
	switch field {
	case variable.SlowLogTimeStr:
		var t time.Time
		t, err = ParseTime(value)
		if err != nil {
			break
		}
		st.time = types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, types.MaxFsp)
		if checker != nil {
			valid = checker.isTimeValid(st.time)
		}
		if t.Location() != tz {
			t = t.In(tz)
			st.time = types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, types.MaxFsp)
		}
	case variable.SlowLogTxnStartTSStr:
		st.txnStartTs, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogUserStr:
		// the old User format is kept for compatibility
		fields := strings.SplitN(value, "@", 2)
		if len(field) > 0 {
			st.user = fields[0]
		}
		if len(field) > 1 {
			st.host = fields[1]
		}
		if checker != nil {
			valid = checker.hasPrivilege(st.user)
		}
	case variable.SlowLogUserAndHostStr:
		// the new User&Host format: root[root] @ localhost [127.0.0.1]
		fields := strings.SplitN(value, "@", 2)
		if len(fields) > 0 {
			tmp := strings.Split(fields[0], "[")
			st.user = strings.TrimSpace(tmp[0])
		}
		if len(fields) > 1 {
			tmp := strings.Split(fields[1], "[")
			st.host = strings.TrimSpace(tmp[0])
		}
		if checker != nil {
			valid = checker.hasPrivilege(st.user)
		}
	case variable.SlowLogConnIDStr:
		st.connID, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogExecRetryCount:
		st.execRetryCount, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogExecRetryTime:
		st.execRetryTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogQueryTimeStr:
		st.queryTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogParseTimeStr:
		st.parseTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCompileTimeStr:
		st.compileTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogOptimizeTimeStr:
		st.optimizeTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogWaitTSTimeStr:
		st.waitTSTime, err = strconv.ParseFloat(value, 64)
	case execdetails.PreWriteTimeStr:
		st.preWriteTime, err = strconv.ParseFloat(value, 64)
	case execdetails.WaitPrewriteBinlogTimeStr:
		st.waitPrewriteBinlogTime, err = strconv.ParseFloat(value, 64)
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
	case execdetails.CopTimeStr:
		st.copTime, err = strconv.ParseFloat(value, 64)
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
	case execdetails.RocksdbDeleteSkippedCountStr:
		st.rocksdbDeleteSkippedCount, err = strconv.ParseUint(value, 10, 64)
	case execdetails.RocksdbKeySkippedCountStr:
		st.rocksdbKeySkippedCount, err = strconv.ParseUint(value, 10, 64)
	case execdetails.RocksdbBlockCacheHitCountStr:
		st.rocksdbBlockCacheCount, err = strconv.ParseUint(value, 10, 64)
	case execdetails.RocksdbBlockReadCountStr:
		st.rocksdbBlockReadCount, err = strconv.ParseUint(value, 10, 64)
	case execdetails.RocksdbBlockReadByteStr:
		st.rocksdbBlockReadByte, err = strconv.ParseUint(value, 10, 64)
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
	case variable.SlowLogPlanFromCache:
		st.planFromCache, err = strconv.ParseBool(value)
	case variable.SlowLogPlanFromBinding:
		st.planFromBinding, err = strconv.ParseBool(value)
	case variable.SlowLogPlan:
		st.plan = value
	case variable.SlowLogPlanDigest:
		st.planDigest = value
	case variable.SlowLogQuerySQLStr:
		st.sql = value
	case variable.SlowLogDiskMax:
		st.diskMax, err = strconv.ParseInt(value, 10, 64)
	case variable.SlowLogKVTotal:
		st.kvTotal, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogPDTotal:
		st.pdTotal, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogBackoffTotal:
		st.backoffTotal, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogWriteSQLRespTotal:
		st.writeSQLRespTotal, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogPrepared:
		st.prepared, err = strconv.ParseBool(value)
	case variable.SlowLogRewriteTimeStr:
		st.rewriteTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogPreprocSubQueriesStr:
		st.preprocSubqueries, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogPreProcSubQueryTimeStr:
		st.preprocSubQueryTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogBackoffDetail:
		if len(st.backoffDetail) > 0 {
			st.backoffDetail += " "
		}
		st.backoffDetail += value
	}
	if err != nil {
		return valid, fmt.Errorf("Parse slow log at line " + strconv.FormatInt(int64(lineNum), 10) + " failed. Field: `" + field + "`, error: " + err.Error())
	}
	return valid, err
}

func (st *slowQueryTuple) convertToDatumRow() []types.Datum {
	// Build the slow query result
	record := make([]types.Datum, 0, 64)
	record = append(record, types.NewTimeDatum(st.time))
	record = append(record, types.NewUintDatum(st.txnStartTs))
	record = append(record, types.NewStringDatum(st.user))
	record = append(record, types.NewStringDatum(st.host))
	record = append(record, types.NewUintDatum(st.connID))
	record = append(record, types.NewUintDatum(st.execRetryCount))
	record = append(record, types.NewFloat64Datum(st.execRetryTime))
	record = append(record, types.NewFloat64Datum(st.queryTime))
	record = append(record, types.NewFloat64Datum(st.parseTime))
	record = append(record, types.NewFloat64Datum(st.compileTime))
	record = append(record, types.NewFloat64Datum(st.rewriteTime))
	record = append(record, types.NewUintDatum(st.preprocSubqueries))
	record = append(record, types.NewFloat64Datum(st.preprocSubQueryTime))
	record = append(record, types.NewFloat64Datum(st.optimizeTime))
	record = append(record, types.NewFloat64Datum(st.waitTSTime))
	record = append(record, types.NewFloat64Datum(st.preWriteTime))
	record = append(record, types.NewFloat64Datum(st.waitPrewriteBinlogTime))
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
	record = append(record, types.NewFloat64Datum(st.copTime))
	record = append(record, types.NewFloat64Datum(st.processTime))
	record = append(record, types.NewFloat64Datum(st.waitTime))
	record = append(record, types.NewFloat64Datum(st.backOffTime))
	record = append(record, types.NewFloat64Datum(st.lockKeysTime))
	record = append(record, types.NewUintDatum(st.requestCount))
	record = append(record, types.NewUintDatum(st.totalKeys))
	record = append(record, types.NewUintDatum(st.processKeys))
	record = append(record, types.NewUintDatum(st.rocksdbDeleteSkippedCount))
	record = append(record, types.NewUintDatum(st.rocksdbKeySkippedCount))
	record = append(record, types.NewUintDatum(st.rocksdbBlockCacheCount))
	record = append(record, types.NewUintDatum(st.rocksdbBlockReadCount))
	record = append(record, types.NewUintDatum(st.rocksdbBlockReadByte))
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
	record = append(record, types.NewIntDatum(st.diskMax))
	record = append(record, types.NewFloat64Datum(st.kvTotal))
	record = append(record, types.NewFloat64Datum(st.pdTotal))
	record = append(record, types.NewFloat64Datum(st.backoffTotal))
	record = append(record, types.NewFloat64Datum(st.writeSQLRespTotal))
	record = append(record, types.NewStringDatum(st.backoffDetail))
	if st.prepared {
		record = append(record, types.NewIntDatum(1))
	} else {
		record = append(record, types.NewIntDatum(0))
	}
	if st.succ {
		record = append(record, types.NewIntDatum(1))
	} else {
		record = append(record, types.NewIntDatum(0))
	}
	if st.planFromCache {
		record = append(record, types.NewIntDatum(1))
	} else {
		record = append(record, types.NewIntDatum(0))
	}
	if st.planFromBinding {
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

type logFile struct {
	file       *os.File  // The opened file handle
	start, end time.Time // The start/end time of the log file
}

// getAllFiles is used to get all slow-log needed to parse, it is exported for test.
func (e *slowQueryRetriever) getAllFiles(ctx context.Context, sctx sessionctx.Context, logFilePath string) ([]logFile, error) {
	totalFileNum := 0
	if e.stats != nil {
		startTime := time.Now()
		defer func() {
			e.stats.initialize = time.Since(startTime)
			e.stats.totalFileNum = totalFileNum
		}()
	}
	if e.extractor == nil || !e.extractor.Enable {
		totalFileNum = 1
		file, err := os.Open(logFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
		return []logFile{{file: file}}, nil
	}
	var logFiles []logFile
	logDir := filepath.Dir(logFilePath)
	ext := filepath.Ext(logFilePath)
	prefix := logFilePath[:len(logFilePath)-len(ext)]
	handleErr := func(err error) error {
		// Ignore the error and append warning for usability.
		if err != io.EOF {
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return nil
	}
	files, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}
	walkFn := func(path string, info os.DirEntry) error {
		if info.IsDir() {
			return nil
		}
		// All rotated log files have the same prefix with the original file.
		if !strings.HasPrefix(path, prefix) {
			return nil
		}
		if isCtxDone(ctx) {
			return ctx.Err()
		}
		totalFileNum++
		file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return handleErr(err)
		}
		skip := false
		defer func() {
			if !skip {
				terror.Log(file.Close())
			}
		}()
		// Get the file start time.
		fileStartTime, err := e.getFileStartTime(ctx, file)
		if err != nil {
			return handleErr(err)
		}
		start := types.NewTime(types.FromGoTime(fileStartTime), mysql.TypeDatetime, types.MaxFsp)
		notInAllTimeRanges := true
		for _, tr := range e.checker.timeRanges {
			if start.Compare(tr.endTime) <= 0 {
				notInAllTimeRanges = false
				break
			}
		}
		if notInAllTimeRanges {
			return nil
		}

		// Get the file end time.
		fileEndTime, err := e.getFileEndTime(ctx, file)
		if err != nil {
			return handleErr(err)
		}
		end := types.NewTime(types.FromGoTime(fileEndTime), mysql.TypeDatetime, types.MaxFsp)
		inTimeRanges := false
		for _, tr := range e.checker.timeRanges {
			if !(start.Compare(tr.endTime) > 0 || end.Compare(tr.startTime) < 0) {
				inTimeRanges = true
				break
			}
		}
		if !inTimeRanges {
			return nil
		}
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return handleErr(err)
		}
		logFiles = append(logFiles, logFile{
			file:  file,
			start: fileStartTime,
			end:   fileEndTime,
		})
		skip = true
		return nil
	}
	for _, file := range files {
		err := walkFn(filepath.Join(logDir, file.Name()), file)
		if err != nil {
			return nil, err
		}
	}
	// Sort by start time
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].start.Before(logFiles[j].start)
	})
	return logFiles, err
}

func (e *slowQueryRetriever) getFileStartTime(ctx context.Context, file *os.File) (time.Time, error) {
	var t time.Time
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return t, err
	}
	reader := bufio.NewReader(file)
	maxNum := 128
	for {
		lineByte, err := getOneLine(reader)
		if err != nil {
			return t, err
		}
		line := string(lineByte)
		if strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			return ParseTime(line[len(variable.SlowLogStartPrefixStr):])
		}
		maxNum -= 1
		if maxNum <= 0 {
			break
		}
		if isCtxDone(ctx) {
			return t, ctx.Err()
		}
	}
	return t, errors.Errorf("malform slow query file %v", file.Name())
}

func (e *slowQueryRetriever) getRuntimeStats() execdetails.RuntimeStats {
	return e.stats
}

type slowQueryRuntimeStats struct {
	totalFileNum int
	readFileNum  int
	readFile     time.Duration
	initialize   time.Duration
	readFileSize int64
	parseLog     int64
	concurrent   int
}

// String implements the RuntimeStats interface.
func (s *slowQueryRuntimeStats) String() string {
	return fmt.Sprintf("initialize: %s, read_file: %s, parse_log: {time:%s, concurrency:%v}, total_file: %v, read_file: %v, read_size: %s",
		execdetails.FormatDuration(s.initialize), execdetails.FormatDuration(s.readFile),
		execdetails.FormatDuration(time.Duration(s.parseLog)), s.concurrent,
		s.totalFileNum, s.readFileNum, memory.FormatBytes(s.readFileSize))
}

// Merge implements the RuntimeStats interface.
func (s *slowQueryRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*slowQueryRuntimeStats)
	if !ok {
		return
	}
	s.totalFileNum += tmp.totalFileNum
	s.readFileNum += tmp.readFileNum
	s.readFile += tmp.readFile
	s.initialize += tmp.initialize
	s.readFileSize += tmp.readFileSize
	s.parseLog += tmp.parseLog
}

// Clone implements the RuntimeStats interface.
func (s *slowQueryRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := *s
	return &newRs
}

// Tp implements the RuntimeStats interface.
func (s *slowQueryRuntimeStats) Tp() int {
	return execdetails.TpSlowQueryRuntimeStat
}

func (e *slowQueryRetriever) getFileEndTime(ctx context.Context, file *os.File) (time.Time, error) {
	var t time.Time
	var tried int
	stat, err := file.Stat()
	if err != nil {
		return t, err
	}
	endCursor := stat.Size()
	maxLineNum := 128
	for {
		lines, readBytes, err := readLastLines(ctx, file, endCursor)
		if err != nil {
			return t, err
		}
		// read out the file
		if readBytes == 0 {
			break
		}
		endCursor -= int64(readBytes)
		for i := len(lines) - 1; i >= 0; i-- {
			if strings.HasPrefix(lines[i], variable.SlowLogStartPrefixStr) {
				return ParseTime(lines[i][len(variable.SlowLogStartPrefixStr):])
			}
		}
		tried += len(lines)
		if tried >= maxLineNum {
			break
		}
		if isCtxDone(ctx) {
			return t, ctx.Err()
		}
	}
	return t, errors.Errorf("invalid slow query file %v", file.Name())
}

const maxReadCacheSize = 1024 * 1024 * 64

// Read lines from the end of a file
// endCursor initial value should be the filesize
func readLastLines(ctx context.Context, file *os.File, endCursor int64) ([]string, int, error) {
	var lines []byte
	var firstNonNewlinePos int
	var cursor = endCursor
	var size int64 = 2048
	for {
		// stop if we are at the beginning
		// check it in the start to avoid read beyond the size
		if cursor <= 0 {
			break
		}
		if size < maxReadCacheSize {
			size = size * 2
		}
		if cursor < size {
			size = cursor
		}
		cursor -= size

		_, err := file.Seek(cursor, io.SeekStart)
		if err != nil {
			return nil, 0, err
		}
		chars := make([]byte, size)
		_, err = file.Read(chars)
		if err != nil {
			return nil, 0, err
		}
		lines = append(chars, lines...)

		// find first '\n' or '\r'
		for i := 0; i < len(chars); i++ {
			// reach the line end
			// the first newline may be in the line end at the first round
			if i >= len(lines)-1 {
				break
			}
			if (chars[i] == 10 || chars[i] == 13) && chars[i+1] != 10 && chars[i+1] != 13 {
				firstNonNewlinePos = i + 1
				break
			}
		}
		if firstNonNewlinePos > 0 {
			break
		}
		if isCtxDone(ctx) {
			return nil, 0, ctx.Err()
		}
	}
	finalStr := string(lines[firstNonNewlinePos:])
	return strings.Split(strings.ReplaceAll(finalStr, "\r\n", "\n"), "\n"), len(finalStr), nil
}

func (e *slowQueryRetriever) initializeAsyncParsing(ctx context.Context, sctx sessionctx.Context) {
	e.taskList = make(chan slowLogTask, 100)
	go e.parseDataForSlowLog(ctx, sctx)
}
