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
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
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
	limit                 uint64
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
		if col.Name.O == metadef.ClusterTableInstanceColumnName {
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
	batchSize := uint64(ParseSlowLogBatchSize)
	if e.limit > 0 && e.limit < batchSize {
		batchSize = e.limit
	}
	if e.extractor.Desc {
		e.parseSlowLogReversed(ctx, sctx, batchSize)
		return
	}
	reader, _ := e.getNextReader()
	if reader == nil {
		close(e.taskList)
		return
	}
	e.parseSlowLog(ctx, sctx, reader, batchSize)
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
	line, err := util.ReadLine(reader, int(vardef.MaxOfMaxAllowedPacket))
	if err == io.EOF && len(line) > 0 {
		return line, nil
	}
	return line, err
}

type offset struct {
	offset int
	length int
}

type slowLogTask struct {
	resultCh chan parsedSlowLog
}

type slowLogBlock []string

func (e *slowQueryRetriever) getBatchLog(ctx context.Context, reader *bufio.Reader, offset *offset, batchSize uint64) ([]string, error) {
	var line string
	log := make([]string, 0, batchSize)
	for range batchSize {
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			e.fileLine++
			lineByte, err := getOneLine(reader)
			if err != nil {
				if err == io.EOF {
					e.fileLine = 0
					newReader, err2 := e.getNextReader()
					if newReader == nil || err2 != nil {
						return log, err2
					}
					offset.length = len(log)
					reader.Reset(newReader)
					continue
				}
				return log, err
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
	return log, nil
}

type slowLogBatchGetter func(ctx context.Context, batchSize uint64) ([]string, error)

func (e *slowQueryRetriever) parseSlowLog(
	ctx context.Context,
	sctx sessionctx.Context,
	reader *bufio.Reader,
	batchSize uint64,
) {
	offset := offset{offset: 0, length: 0}
	nextBatch := func(ctx context.Context, batchSize uint64) ([]string, error) {
		return e.getBatchLog(ctx, reader, &offset, batchSize)
	}
	afterBatch := func() {
		offset.offset = e.fileLine
		offset.length = 0
	}
	e.parseSlowLogByBatchGetter(ctx, sctx, batchSize, &offset, nextBatch, afterBatch)
}

func (e *slowQueryRetriever) parseSlowLogReversed(ctx context.Context, sctx sessionctx.Context, batchSize uint64) {
	scanner := newSlowLogReverseScanner(e, sctx)
	offset := offset{offset: 0, length: 0}
	e.parseSlowLogByBatchGetter(ctx, sctx, batchSize, &offset, scanner.nextBatch, nil)
}

func (e *slowQueryRetriever) parseSlowLogByBatchGetter(
	ctx context.Context,
	sctx sessionctx.Context,
	batchSize uint64,
	off *offset,
	nextBatch slowLogBatchGetter,
	afterBatch func(),
) {
	defer close(e.taskList)

	if e.limit > 0 {
		e.parseSlowLogByBatchGetterWithLimit(ctx, sctx, batchSize, off, nextBatch, afterBatch)
		return
	}

	concurrent := sctx.GetSessionVars().Concurrency.DistSQLScanConcurrency()
	ch := make(chan struct{}, concurrent)
	if e.stats != nil {
		e.stats.concurrent = concurrent
	}
	defer close(ch)

	for {
		startTime := time.Now()
		log, err := nextBatch(ctx, batchSize)
		if e.stats != nil {
			e.stats.readFile += time.Since(startTime)
		}
		if err != nil {
			t := slowLogTask{resultCh: make(chan parsedSlowLog, 1)}
			select {
			case <-ctx.Done():
				return
			case e.taskList <- t:
			}
			e.sendParsedSlowLogCh(t, parsedSlowLog{nil, err})
			return
		}
		if len(log) == 0 {
			return
		}
		failpoint.Inject("mockReadSlowLogSlow", func(val failpoint.Value) {
			if val.(bool) {
				signals := ctx.Value(signalsKey{}).([]chan int)
				signals[0] <- 1
				<-signals[1]
			}
		})
		t := slowLogTask{resultCh: make(chan parsedSlowLog, 1)}
		start := *off
		ch <- struct{}{}
		select {
		case <-ctx.Done():
			return
		case e.taskList <- t:
		}
		e.wg.Add(1)
		go func(log []string, start offset, t slowLogTask) {
			defer e.wg.Done()
			result, err := e.parseLog(ctx, sctx, log, start)
			e.sendParsedSlowLogCh(t, parsedSlowLog{result, err})
			<-ch
		}(log, start, t)
		if afterBatch != nil {
			afterBatch()
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// parseSlowLogByBatchGetterWithLimit has some differences with parseSlowLogByBatchGetter:
// 1. It guarantees that the number of parsed slow logs will not exceed e.limit.
// 2. It parses slow logs serially instead of concurrently.
func (e *slowQueryRetriever) parseSlowLogByBatchGetterWithLimit(
	ctx context.Context,
	sctx sessionctx.Context,
	batchSizeFromCaller uint64,
	off *offset,
	nextBatch slowLogBatchGetter,
	afterBatch func(),
) {
	target := e.limit
	var produced uint64
	for produced < target {
		remaining := target - produced
		batchSize := min(batchSizeFromCaller, remaining)
		startTime := time.Now()
		log, err := nextBatch(ctx, batchSize)
		if e.stats != nil {
			e.stats.readFile += time.Since(startTime)
		}
		if err != nil {
			t := slowLogTask{resultCh: make(chan parsedSlowLog, 1)}
			select {
			case <-ctx.Done():
				return
			case e.taskList <- t:
			}
			e.sendParsedSlowLogCh(t, parsedSlowLog{nil, err})
			return
		}
		if len(log) == 0 {
			return
		}
		failpoint.Inject("mockReadSlowLogSlow", func(val failpoint.Value) {
			if val.(bool) {
				signals := ctx.Value(signalsKey{}).([]chan int)
				signals[0] <- 1
				<-signals[1]
			}
		})
		t := slowLogTask{resultCh: make(chan parsedSlowLog, 1)}
		start := *off
		select {
		case <-ctx.Done():
			return
		case e.taskList <- t:
		}
		resultData, err := e.parseLog(ctx, sctx, log, start)
		if err != nil {
			e.sendParsedSlowLogCh(t, parsedSlowLog{nil, err})
			return
		}
		if remaining < uint64(len(resultData)) {
			resultData = resultData[:remaining]
		}
		produced += uint64(len(resultData))
		e.sendParsedSlowLogCh(t, parsedSlowLog{resultData, nil})
		if afterBatch != nil {
			afterBatch()
		}
		if ctx.Err() != nil {
			return
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

// findMatchedRightBracket returns the rightBracket index which matchs line[leftBracketIdx]
// leftBracketIdx should be valid string index for line
// Returns -1 if invalid inputs are given
func findMatchedRightBracket(line string, leftBracketIdx int) int {
	leftBracket := line[leftBracketIdx]
	rightBracket := byte('}')
	if leftBracket == '[' {
		rightBracket = ']'
	} else if leftBracket != '{' {
		return -1
	}
	lineLength := len(line)
	current := leftBracketIdx
	leftBracketCnt := 0
	for current < lineLength {
		b := line[current]
		if b == leftBracket {
			leftBracketCnt++
			current++
		} else if b == rightBracket {
			leftBracketCnt--
			if leftBracketCnt > 0 {
				current++
			} else if leftBracketCnt == 0 {
				if current+1 < lineLength && line[current+1] != ' ' {
					return -1
				}
				return current
			} else {
				return -1
			}
		} else {
			current++
		}
	}
	return -1
}

func isLetterOrNumeric(b byte) bool {
	return ('A' <= b && b <= 'Z') || ('a' <= b && b <= 'z') || ('0' <= b && b <= '9')
}

// splitByColon split a line like "field: value field: value..."
// Note:
// 1. field string's first character can only be ASCII letters or digits, and can't contain ':'
// 2. value string may be surrounded by brackets, allowed brackets includes "[]" and "{}",  like {key: value,{key: value}}
// "[]" can only be nested inside "[]"; "{}" can only be nested inside "{}"
// 3. value string can't contain ' ' character unless it is inside brackets
func splitByColon(line string) (fields []string, values []string) {
	fields = make([]string, 0, 1)
	values = make([]string, 0, 1)

	lineLength := len(line)
	parseKey := true
	start := 0
	errMsg := ""
	for current := 0; current < lineLength; {
		if parseKey {
			// Find key start
			for current < lineLength && !isLetterOrNumeric(line[current]) {
				current++
			}
			start = current
			if current >= lineLength {
				break
			}
			for current < lineLength && line[current] != ':' {
				current++
			}
			fields = append(fields, line[start:current])
			parseKey = false
			current += 2 // bypass ": "
			if current >= lineLength {
				// last empty value
				values = append(values, "")
			}
		} else {
			start = current
			if current < lineLength && (line[current] == '{' || line[current] == '[') {
				rBraceIdx := findMatchedRightBracket(line, current)
				if rBraceIdx == -1 {
					errMsg = "Braces matched error"
					break
				}
				current = rBraceIdx + 1
			} else {
				for current < lineLength && line[current] != ' ' {
					current++
				}
				// Meet empty value cases: "Key: Key:"
				if current > 0 && line[current-1] == ':' {
					values = append(values, "")
					current = start
					parseKey = true
					continue
				}
			}
			values = append(values, line[start:min(current, len(line))])
			parseKey = true
		}
	}
	if len(errMsg) > 0 {
		logutil.BgLogger().Warn("slow query parse slow log error", zap.String("Error", errMsg), zap.String("Log", line))
		return nil, nil
	}
	if len(fields) != len(values) {
		logutil.BgLogger().Warn("slow query parse slow log error", zap.Int("field_count", len(fields)), zap.Int("value_count", len(values)), zap.String("Log", line))
		return nil, nil
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
		if err2 := ctx.Err(); err2 != nil {
			return nil, err2
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
				} else if strings.HasPrefix(line, variable.SlowLogWarnings+variable.SlowLogSpaceMarkStr) {
					line = line[len(variable.SlowLogWarnings+variable.SlowLogSpaceMarkStr):]
					valid = e.setColumnValue(sctx, row, tz, variable.SlowLogWarnings, line, e.checker, fileLine)
				} else if strings.HasPrefix(line, variable.SlowLogDBStr+variable.SlowLogSpaceMarkStr) {
					line = line[len(variable.SlowLogDBStr+variable.SlowLogSpaceMarkStr):]
					valid = e.setColumnValue(sctx, row, tz, variable.SlowLogDBStr, line, e.checker, fileLine)
				} else {
					fields, values := splitByColon(line)
					for i := range fields {
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
			timeValue := types.NewTime(types.FromGoTime(t.In(tz)), mysql.TypeTimestamp, types.MaxFsp)
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
