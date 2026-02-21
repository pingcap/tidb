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
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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
