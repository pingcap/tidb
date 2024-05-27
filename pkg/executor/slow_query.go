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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"go.uber.org/zap"
)

type signalsKey struct{}

// ParseSlowLogBatchSize is the batch size of slow-log lines for a worker to parse, exported for testing.
var ParseSlowLogBatchSize = 64

// slowQueryRetriever is used to read slow log data.
type slowQueryRetriever struct {
	table                 *model.TableInfo
	outputCols            []*model.ColumnInfo
	initialized           bool
	extractor             *plannercore.SlowQueryExtractor
	files                 []logFile
	fileIdx               int
	fileLine              int
	checker               *slowLogChecker
	columnValueFactoryMap map[string]slowQueryColumnValueFactory
	instanceFactory       func([]types.Datum)

	taskList      chan slowLogTask
	stats         *slowQueryRuntimeStats
	memTracker    *memory.Tracker
	lastFetchSize int64
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func (e *slowQueryRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if !e.initialized {
		err := e.initialize(ctx, sctx)
		if err != nil {
			return nil, err
		}
		ctx, e.cancel = context.WithCancel(ctx)
		e.initializeAsyncParsing(ctx, sctx)
	}
	return e.dataForSlowLog(ctx)
}

func (e *slowQueryRetriever) initialize(ctx context.Context, sctx sessionctx.Context) error {
	var err error
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		hasProcessPriv = pm.RequestVerification(sctx.GetSessionVars().ActiveRoles, "", "", "", mysql.ProcessPriv)
	}
	// initialize column value factories.
	e.columnValueFactoryMap = make(map[string]slowQueryColumnValueFactory, len(e.outputCols))
	for idx, col := range e.outputCols {
		if col.Name.O == util.ClusterTableInstanceColumnName {
			e.instanceFactory, err = getInstanceColumnValueFactory(sctx, idx)
			if err != nil {
				return err
			}
			continue
		}
		factory, err := getColumnValueFactoryByName(col.Name.O, idx)
		if err != nil {
			return err
		}
		if factory == nil {
			panic(fmt.Sprintf("should never happen, should register new column %v into getColumnValueFactoryByName function", col.Name.O))
		}
		e.columnValueFactoryMap[col.Name.O] = factory
	}
	// initialize checker.
	e.checker = &slowLogChecker{
		hasProcessPriv: hasProcessPriv,
		user:           sctx.GetSessionVars().User,
	}
	e.stats = &slowQueryRuntimeStats{}
	if e.extractor != nil {
		e.checker.enableTimeCheck = e.extractor.Enable
		for _, tr := range e.extractor.TimeRanges {
			startTime := types.NewTime(types.FromGoTime(tr.StartTime.In(sctx.GetSessionVars().Location())), mysql.TypeDatetime, types.MaxFsp)
			endTime := types.NewTime(types.FromGoTime(tr.EndTime.In(sctx.GetSessionVars().Location())), mysql.TypeDatetime, types.MaxFsp)
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
		slices.Reverse(e.files)
	}
	return err
}

func (e *slowQueryRetriever) close() error {
	for _, f := range e.files {
		err := f.file.Close()
		if err != nil {
			logutil.BgLogger().Error("close slow log file failed.", zap.Error(err))
		}
	}
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	return nil
}

type parsedSlowLog struct {
	rows [][]types.Datum
	err  error
}

func (e *slowQueryRetriever) getNextFile() *logFile {
	if e.fileIdx >= len(e.files) {
		return nil
	}
	ret := &e.files[e.fileIdx]
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
	return ret
}

func (e *slowQueryRetriever) getPreviousReader() (*bufio.Reader, error) {
	fileIdx := e.fileIdx
	// fileIdx refer to the next file which should be read
	// so we need to set fileIdx to fileIdx - 2 to get the previous file.
	fileIdx = fileIdx - 2
	if fileIdx < 0 {
		return nil, nil
	}
	file := e.files[fileIdx]
	_, err := file.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	var reader *bufio.Reader
	if !file.compressed {
		reader = bufio.NewReader(file.file)
	} else {
		gr, err := gzip.NewReader(file.file)
		if err != nil {
			return nil, err
		}
		reader = bufio.NewReader(gr)
	}
	return reader, nil
}

func (e *slowQueryRetriever) getNextReader() (*bufio.Reader, error) {
	file := e.getNextFile()
	if file == nil {
		return nil, nil
	}
	var reader *bufio.Reader
	if !file.compressed {
		reader = bufio.NewReader(file.file)
	} else {
		gr, err := gzip.NewReader(file.file)
		if err != nil {
			return nil, err
		}
		reader = bufio.NewReader(gr)
	}
	return reader, nil
}

func (e *slowQueryRetriever) parseDataForSlowLog(ctx context.Context, sctx sessionctx.Context) {
	defer e.wg.Done()
	reader, _ := e.getNextReader()
	if reader == nil {
		close(e.taskList)
		return
	}
	e.parseSlowLog(ctx, sctx, reader, ParseSlowLogBatchSize)
}

func (e *slowQueryRetriever) dataForSlowLog(ctx context.Context) ([][]types.Datum, error) {
	var (
		task slowLogTask
		ok   bool
	)
	e.memConsume(-e.lastFetchSize)
	e.lastFetchSize = 0
	for {
		select {
		case task, ok = <-e.taskList:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if !ok {
			return nil, nil
		}
		result := <-task.resultCh
		rows, err := result.rows, result.err
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			continue
		}
		if e.instanceFactory != nil {
			for i := range rows {
				e.instanceFactory(rows[i])
			}
		}
		e.lastFetchSize = calculateDatumsSize(rows)
		return rows, nil
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
	return util.ReadLine(reader, int(variable.MaxOfMaxAllowedPacket))
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
					newReader, err := e.getNextReader()
					if newReader == nil || err != nil {
						return [][]string{log}, err
					}
					offset.length = len(log)
					reader.Reset(newReader)
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
		newReader, _ := e.getNextReader()
		if newReader != nil {
			inputReader.Reset(newReader)
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
				reader, err = e.getPreviousReader()
				if reader == nil || err != nil {
					return decomposeToSlowLogTasks(logs, num), nil
				}
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
			select {
			case <-ctx.Done():
				return
			case e.taskList <- t:
			}
			e.sendParsedSlowLogCh(t, parsedSlowLog{nil, err})
		}
		if len(logs) == 0 || len(logs[0]) == 0 {
			break
		}
		if e.stats != nil {
			e.stats.readFile += time.Since(startTime)
		}
		failpoint.Inject("mockReadSlowLogSlow", func(val failpoint.Value) {
			if val.(bool) {
				signals := ctx.Value(signalsKey{}).([]chan int)
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
			e.wg.Add(1)
			go func() {
				defer e.wg.Done()
				result, err := e.parseLog(ctx, sctx, log, start)
				e.sendParsedSlowLogCh(t, parsedSlowLog{result, err})
				<-ch
			}()
			offset.offset = e.fileLine
			offset.length = 0
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (*slowQueryRetriever) sendParsedSlowLogCh(t slowLogTask, re parsedSlowLog) {
	select {
	case t.resultCh <- re:
	default:
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

// kvSplitRegex: it was just for split "field: value field: value..."
var kvSplitRegex = regexp.MustCompile(`\w+: `)

// splitByColon split a line like "field: value field: value..."
func splitByColon(line string) (fields []string, values []string) {
	matches := kvSplitRegex.FindAllStringIndex(line, -1)
	fields = make([]string, 0, len(matches))
	values = make([]string, 0, len(matches))

	beg := 0
	end := 0
	for _, match := range matches {
		// trim ": "
		fields = append(fields, line[match[0]:match[1]-2])

		end = match[0]
		if beg != 0 {
			// trim " "
			values = append(values, line[beg:end-1])
		}
		beg = match[1]
	}

	if end != len(line) {
		// " " does not exist in the end
		values = append(values, line[beg:])
	}
	return fields, values
}

func (e *slowQueryRetriever) parseLog(ctx context.Context, sctx sessionctx.Context, log []string, offset offset) (data [][]types.Datum, err error) {
	start := time.Now()
	logSize := calculateLogSize(log)
	defer e.memConsume(-logSize)
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Warn("slow query parse slow log panic", zap.Error(err), zap.String("stack", string(buf)))
		}
		if e.stats != nil {
			atomic.AddInt64(&e.stats.parseLog, int64(time.Since(start)))
		}
	}()
	e.memConsume(logSize)
	failpoint.Inject("errorMockParseSlowLogPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})
	var row []types.Datum
	user := ""
	tz := sctx.GetSessionVars().Location()
	startFlag := false
	for index, line := range log {
		if isCtxDone(ctx) {
			return nil, ctx.Err()
		}
		fileLine := getLineIndex(offset, index)
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			row = make([]types.Datum, len(e.outputCols))
			user = ""
			valid := e.setColumnValue(sctx, row, tz, variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):], e.checker, fileLine)
			if valid {
				startFlag = true
			}
			continue
		}
		if startFlag {
			if strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
				line = line[len(variable.SlowLogRowPrefixStr):]
				valid := true
				if strings.HasPrefix(line, variable.SlowLogPrevStmtPrefix) {
					valid = e.setColumnValue(sctx, row, tz, variable.SlowLogPrevStmt, line[len(variable.SlowLogPrevStmtPrefix):], e.checker, fileLine)
				} else if strings.HasPrefix(line, variable.SlowLogUserAndHostStr+variable.SlowLogSpaceMarkStr) {
					value := line[len(variable.SlowLogUserAndHostStr+variable.SlowLogSpaceMarkStr):]
					fields := strings.SplitN(value, "@", 2)
					if len(fields) < 2 {
						continue
					}
					user = parseUserOrHostValue(fields[0])
					if e.checker != nil && !e.checker.hasPrivilege(user) {
						startFlag = false
						continue
					}
					valid = e.setColumnValue(sctx, row, tz, variable.SlowLogUserStr, user, e.checker, fileLine)
					if !valid {
						startFlag = false
						continue
					}
					host := parseUserOrHostValue(fields[1])
					valid = e.setColumnValue(sctx, row, tz, variable.SlowLogHostStr, host, e.checker, fileLine)
				} else if strings.HasPrefix(line, variable.SlowLogCopBackoffPrefix) {
					valid = e.setColumnValue(sctx, row, tz, variable.SlowLogBackoffDetail, line, e.checker, fileLine)
				} else if strings.HasPrefix(line, variable.SlowLogWarnings) {
					line = line[len(variable.SlowLogWarnings+variable.SlowLogSpaceMarkStr):]
					valid = e.setColumnValue(sctx, row, tz, variable.SlowLogWarnings, line, e.checker, fileLine)
				} else {
					fields, values := splitByColon(line)
					for i := 0; i < len(fields); i++ {
						valid := e.setColumnValue(sctx, row, tz, fields[i], values[i], e.checker, fileLine)
						if !valid {
							startFlag = false
							break
						}
					}
				}
				if !valid {
					startFlag = false
				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				if strings.HasPrefix(line, "use") {
					// `use DB` statements in the slow log is used to keep it be compatible with MySQL,
					// since we already get the current DB from the `# DB` field, we can ignore it here,
					// please see https://github.com/pingcap/tidb/issues/17846 for more details.
					continue
				}
				if e.checker != nil && !e.checker.hasPrivilege(user) {
					startFlag = false
					continue
				}
				// Get the sql string, and mark the start flag to false.
				_ = e.setColumnValue(sctx, row, tz, variable.SlowLogQuerySQLStr, string(hack.Slice(line)), e.checker, fileLine)
				e.setDefaultValue(row)
				e.memConsume(types.EstimatedMemUsage(row, 1))
				data = append(data, row)
				startFlag = false
			} else {
				startFlag = false
			}
		}
	}
	return data, nil
}

func (e *slowQueryRetriever) setColumnValue(sctx sessionctx.Context, row []types.Datum, tz *time.Location, field, value string, checker *slowLogChecker, lineNum int) bool {
	factory := e.columnValueFactoryMap[field]
	if factory == nil {
		// Fix issue 34320, when slow log time is not in the output columns, the time filter condition is mistakenly discard.
		if field == variable.SlowLogTimeStr && checker != nil {
			t, err := ParseTime(value)
			if err != nil {
				err = fmt.Errorf("Parse slow log at line %v, failed field is %v, failed value is %v, error is %v", lineNum, field, value, err)
				sctx.GetSessionVars().StmtCtx.AppendWarning(err)
				return false
			}
			timeValue := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, types.MaxFsp)
			return checker.isTimeValid(timeValue)
		}
		return true
	}
	valid, err := factory(row, value, tz, checker)
	if err != nil {
		err = fmt.Errorf("Parse slow log at line %v, failed field is %v, failed value is %v, error is %v", lineNum, field, value, err)
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return true
	}
	return valid
}

func (e *slowQueryRetriever) setDefaultValue(row []types.Datum) {
	for i := range row {
		if !row[i].IsNull() {
			continue
		}
		row[i] = table.GetZeroValue(e.outputCols[i])
	}
}

type slowQueryColumnValueFactory func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error)

func parseUserOrHostValue(value string) string {
	// the new User&Host format: root[root] @ localhost [127.0.0.1]
	tmp := strings.Split(value, "[")
	return strings.TrimSpace(tmp[0])
}

func getColumnValueFactoryByName(colName string, columnIdx int) (slowQueryColumnValueFactory, error) {
	switch colName {
	case variable.SlowLogTimeStr:
		return func(row []types.Datum, value string, tz *time.Location, checker *slowLogChecker) (bool, error) {
			t, err := ParseTime(value)
			if err != nil {
				return false, err
			}
			timeValue := types.NewTime(types.FromGoTime(t.In(tz)), mysql.TypeTimestamp, types.MaxFsp)
			if checker != nil {
				valid := checker.isTimeValid(timeValue)
				if !valid {
					return valid, nil
				}
			}
			row[columnIdx] = types.NewTimeDatum(timeValue)
			return true, nil
		}, nil
	case variable.SlowLogBackoffDetail:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (bool, error) {
			backoffDetail := row[columnIdx].GetString()
			if len(backoffDetail) > 0 {
				backoffDetail += " "
			}
			backoffDetail += value
			row[columnIdx] = types.NewStringDatum(backoffDetail)
			return true, nil
		}, nil
	case variable.SlowLogPlan:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (bool, error) {
			plan := parsePlan(value)
			row[columnIdx] = types.NewStringDatum(plan)
			return true, nil
		}, nil
	case variable.SlowLogBinaryPlan:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (bool, error) {
			if strings.HasPrefix(value, variable.SlowLogBinaryPlanPrefix) {
				value = value[len(variable.SlowLogBinaryPlanPrefix) : len(value)-len(variable.SlowLogPlanSuffix)]
			}
			row[columnIdx] = types.NewStringDatum(value)
			return true, nil
		}, nil
	case variable.SlowLogConnIDStr, variable.SlowLogExecRetryCount, variable.SlowLogPreprocSubQueriesStr,
		execdetails.WriteKeysStr, execdetails.WriteSizeStr, execdetails.PrewriteRegionStr, execdetails.TxnRetryStr,
		execdetails.RequestCountStr, execdetails.TotalKeysStr, execdetails.ProcessKeysStr,
		execdetails.RocksdbDeleteSkippedCountStr, execdetails.RocksdbKeySkippedCountStr,
		execdetails.RocksdbBlockCacheHitCountStr, execdetails.RocksdbBlockReadCountStr,
		variable.SlowLogTxnStartTSStr, execdetails.RocksdbBlockReadByteStr:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewUintDatum(v)
			return true, nil
		}, nil
	case variable.SlowLogExecRetryTime, variable.SlowLogQueryTimeStr, variable.SlowLogParseTimeStr,
		variable.SlowLogCompileTimeStr, variable.SlowLogRewriteTimeStr, variable.SlowLogPreProcSubQueryTimeStr,
		variable.SlowLogOptimizeTimeStr, variable.SlowLogWaitTSTimeStr, execdetails.PreWriteTimeStr,
		execdetails.WaitPrewriteBinlogTimeStr, execdetails.CommitTimeStr, execdetails.GetCommitTSTimeStr,
		execdetails.CommitBackoffTimeStr, execdetails.ResolveLockTimeStr, execdetails.LocalLatchWaitTimeStr,
		execdetails.CopTimeStr, execdetails.ProcessTimeStr, execdetails.WaitTimeStr, execdetails.BackoffTimeStr,
		execdetails.LockKeysTimeStr, variable.SlowLogCopProcAvg, variable.SlowLogCopProcP90, variable.SlowLogCopProcMax,
		variable.SlowLogCopWaitAvg, variable.SlowLogCopWaitP90, variable.SlowLogCopWaitMax, variable.SlowLogKVTotal,
		variable.SlowLogPDTotal, variable.SlowLogBackoffTotal, variable.SlowLogWriteSQLRespTotal, variable.SlowLogRRU,
		variable.SlowLogWRU, variable.SlowLogWaitRUDuration:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewFloat64Datum(v)
			return true, nil
		}, nil
	case variable.SlowLogUserStr, variable.SlowLogHostStr, execdetails.BackoffTypesStr, variable.SlowLogDBStr, variable.SlowLogIndexNamesStr, variable.SlowLogDigestStr,
		variable.SlowLogStatsInfoStr, variable.SlowLogCopProcAddr, variable.SlowLogCopWaitAddr, variable.SlowLogPlanDigest,
		variable.SlowLogPrevStmt, variable.SlowLogQuerySQLStr, variable.SlowLogWarnings, variable.SlowLogSessAliasStr,
		variable.SlowLogResourceGroup:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			row[columnIdx] = types.NewStringDatum(value)
			return true, nil
		}, nil
	case variable.SlowLogMemMax, variable.SlowLogDiskMax, variable.SlowLogResultRows:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			v, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewIntDatum(v)
			return true, nil
		}, nil
	case variable.SlowLogPrepared, variable.SlowLogSucc, variable.SlowLogPlanFromCache, variable.SlowLogPlanFromBinding,
		variable.SlowLogIsInternalStr, variable.SlowLogIsExplicitTxn, variable.SlowLogIsWriteCacheTable, variable.SlowLogHasMoreResults:
		return func(row []types.Datum, value string, _ *time.Location, _ *slowLogChecker) (valid bool, err error) {
			v, err := strconv.ParseBool(value)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewDatum(v)
			return true, nil
		}, nil
	}
	return nil, nil
}

func getInstanceColumnValueFactory(sctx sessionctx.Context, columnIdx int) (func(row []types.Datum), error) {
	instanceAddr, err := infoschema.GetInstanceAddr(sctx)
	if err != nil {
		return nil, err
	}
	return func(row []types.Datum) {
		row[columnIdx] = types.NewStringDatum(instanceAddr)
	}, nil
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
	start      time.Time // The start time of the log file
	compressed bool      // The file is compressed or not
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
		//nolint: gosec
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
		compressed := strings.HasSuffix(path, ".gz")
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
		fileStartTime, err := e.getFileStartTime(ctx, file, compressed)
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

		// If we want to get the end time from a compressed file,
		// we need uncompress the whole file which is very slow and consume a lot of memory.
		if !compressed {
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
		}
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return handleErr(err)
		}
		logFiles = append(logFiles, logFile{
			file:       file,
			start:      fileStartTime,
			compressed: compressed,
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
	slices.SortFunc(logFiles, func(i, j logFile) int {
		return i.start.Compare(j.start)
	})
	// Assume no time range overlap in log files and remove unnecessary log files for compressed files.
	var ret []logFile
	for i, file := range logFiles {
		if i == len(logFiles)-1 || !file.compressed {
			ret = append(ret, file)
			continue
		}
		start := types.NewTime(types.FromGoTime(logFiles[i].start), mysql.TypeDatetime, types.MaxFsp)
		// use next file.start as endTime
		end := types.NewTime(types.FromGoTime(logFiles[i+1].start), mysql.TypeDatetime, types.MaxFsp)
		inTimeRanges := false
		for _, tr := range e.checker.timeRanges {
			if !(start.Compare(tr.endTime) > 0 || end.Compare(tr.startTime) < 0) {
				inTimeRanges = true
				break
			}
		}
		if inTimeRanges {
			ret = append(ret, file)
		}
	}
	return ret, err
}

func (*slowQueryRetriever) getFileStartTime(ctx context.Context, file *os.File, compressed bool) (time.Time, error) {
	var t time.Time
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return t, err
	}
	var reader *bufio.Reader
	if !compressed {
		reader = bufio.NewReader(file)
	} else {
		gr, err := gzip.NewReader(file)
		if err != nil {
			return t, err
		}
		reader = bufio.NewReader(gr)
	}
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
		maxNum--
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
func (*slowQueryRuntimeStats) Tp() int {
	return execdetails.TpSlowQueryRuntimeStat
}

func (*slowQueryRetriever) getFileEndTime(ctx context.Context, file *os.File) (time.Time, error) {
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
		lines = append(chars, lines...) // nozero

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
	e.taskList = make(chan slowLogTask, 1)
	e.wg.Add(1)
	go e.parseDataForSlowLog(ctx, sctx)
}

func calculateLogSize(log []string) int64 {
	size := 0
	for _, line := range log {
		size += len(line)
	}
	return int64(size)
}

func calculateDatumsSize(rows [][]types.Datum) int64 {
	size := int64(0)
	for _, row := range rows {
		size += types.EstimatedMemUsage(row, 1)
	}
	return size
}

func (e *slowQueryRetriever) memConsume(bytes int64) {
	if e.memTracker != nil {
		e.memTracker.Consume(bytes)
	}
}
