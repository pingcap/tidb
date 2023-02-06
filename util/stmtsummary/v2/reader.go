// Copyright 2022 PingCAP, Inc.
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

package stmtsummary

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/set"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	logFileTimeFormat        = "2006-01-02T15-04-05.000" // depends on lumberjack.go#backupTimeFormat
	maxLineSize              = 1073741824
	parseStatementsBatchSize = 8
)

// StmtTimeRange is the time range type used in the stmtsummary package.
// [Begin, End)
type StmtTimeRange struct {
	Begin int64
	End   int64
}

// MemReader is used to read the current window's data maintained in memory by StmtSummary.
type MemReader struct {
	s            *StmtSummary
	columns      []*model.ColumnInfo
	instanceAddr string
	timeLocation *time.Location

	columnFactories []columnFactory
	checker         *stmtChecker
}

// NewMemReader creates a MemReader from StmtSummary and other necessary parameters.
func NewMemReader(s *StmtSummary,
	columns []*model.ColumnInfo,
	instanceAddr string,
	timeLocation *time.Location,
	user *auth.UserIdentity,
	hasProcessPriv bool,
	digests set.StringSet,
	timeRanges []*StmtTimeRange) *MemReader {
	return &MemReader{
		s:               s,
		columns:         columns,
		instanceAddr:    instanceAddr,
		timeLocation:    timeLocation,
		columnFactories: makeColumnFactories(columns),
		checker: &stmtChecker{
			user:           user,
			hasProcessPriv: hasProcessPriv,
			digests:        digests,
			timeRanges:     timeRanges,
		},
	}
}

// Rows returns rows converted from the current window's data maintained
// in memory by StmtSummary. All evicted data will be aggregated into a
// single row appended at the end.
func (r *MemReader) Rows() [][]types.Datum {
	if r.s == nil {
		return nil
	}
	end := timeNow().Unix()
	w := r.s.window
	if !r.checker.isTimeValid(w.begin.Unix(), end) {
		return nil
	}
	w.Lock()
	values := w.lru.Values()
	evicted := w.evicted
	w.Unlock()
	rows := make([][]types.Datum, 0, len(values)+1)
	for _, v := range values {
		record := v.(*lockedStmtRecord)
		if !r.checker.isDigestValid(record.Digest) {
			continue
		}
		func() {
			record.Lock()
			defer record.Unlock()
			if !r.checker.hasPrivilege(record.AuthUsers) {
				return
			}
			record.Begin = w.begin.Unix()
			record.End = end
			row := make([]types.Datum, len(r.columnFactories))
			for i, factory := range r.columnFactories {
				row[i] = types.NewDatum(factory(r, record.StmtRecord))
			}
			rows = append(rows, row)
		}()
	}
	if r.checker.digests == nil {
		func() {
			evicted.Lock()
			defer evicted.Unlock()
			if evicted.other.ExecCount == 0 {
				return
			}
			if !r.checker.hasPrivilege(evicted.other.AuthUsers) {
				return
			}
			evicted.other.Begin = w.begin.Unix()
			evicted.other.End = end
			row := make([]types.Datum, len(r.columnFactories))
			for i, factory := range r.columnFactories {
				row[i] = types.NewDatum(factory(r, evicted.other))
			}
			rows = append(rows, row)
		}()
	}
	return rows
}

// getInstanceAddr implements columnInfo.
func (r *MemReader) getInstanceAddr() string {
	return r.instanceAddr
}

// getInstanceAddr implements columnInfo.
func (r *MemReader) getTimeLocation() *time.Location {
	return r.timeLocation
}

// HistoryReader is used to read data that has been persisted to files.
type HistoryReader struct {
	ctx          context.Context
	columns      []*model.ColumnInfo
	instanceAddr string
	timeLocation *time.Location

	columnFactories []columnFactory
	checker         *stmtChecker
	files           *stmtFiles
	cancel          context.CancelFunc
	taskList        chan stmtParseTask
	wg              sync.WaitGroup
}

// NewHistoryReader creates a HisroryReader from StmtSummary and other
// necessary parameters. If timeRanges is present, only files within
// the time range will be read.
func NewHistoryReader(ctx context.Context,
	columns []*model.ColumnInfo,
	instanceAddr string,
	timeLocation *time.Location,
	user *auth.UserIdentity,
	hasProcessPriv bool,
	digests set.StringSet,
	timeRanges []*StmtTimeRange) (*HistoryReader, error) {
	r := &HistoryReader{
		ctx:             ctx,
		columns:         columns,
		instanceAddr:    instanceAddr,
		timeLocation:    timeLocation,
		columnFactories: makeColumnFactories(columns),
		checker: &stmtChecker{
			user:           user,
			hasProcessPriv: hasProcessPriv,
			digests:        digests,
			timeRanges:     timeRanges,
		},
	}
	var err error
	r.files, err = newStmtFiles(ctx, timeRanges)
	if err != nil {
		return nil, err
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.taskList = make(chan stmtParseTask, 1)
	r.wg.Add(1)
	go r.fetchAndSendRows()
	return r, nil
}

// Rows returns rows converted from records in files. Reading and parsing
// works asynchronously. If (nil, nil) is returned, it means that the
// reading has been completed.
func (r *HistoryReader) Rows() ([][]types.Datum, error) {
	ctx := r.ctx
	var task stmtParseTask
	var ok bool
	for {
		select {
		case task, ok = <-r.taskList:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if !ok {
			return nil, nil
		}
		select {
		case result, ok := <-task.resultCh:
			if !ok {
				return nil, nil
			}
			if result.err != nil {
				return nil, result.err
			}
			if len(result.rows) == 0 {
				continue
			}
			return result.rows, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close ends reading and closes all files.
func (r *HistoryReader) Close() error {
	r.files.close()
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	return nil
}

func (r *HistoryReader) fetchAndSendRows() {
	defer r.wg.Done()
	defer close(r.taskList)
	ctx := r.ctx
	file := r.files.next()
	if file == nil {
		return
	}
	reader := bufio.NewReader(file)
	for {
		lines, err := r.readBatchLinesFromReader(reader)
		if err != nil {
			task := stmtParseTask{resultCh: make(chan stmtParseResult, 1)}
			select {
			case <-ctx.Done():
				return
			case r.taskList <- task:
			}
			select {
			case task.resultCh <- stmtParseResult{err: err}:
			default:
				return
			}
		}
		if len(lines) == 0 {
			break
		}
		task := stmtParseTask{resultCh: make(chan stmtParseResult, 1)}
		select {
		case <-ctx.Done():
			return
		case r.taskList <- task:
		}
		rows := make([][]types.Datum, 0, len(lines))
		for _, line := range lines {
			if len(line) == 0 || len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			row, stop, err := r.parseAndCheckRow(line)
			if err != nil {
				select {
				case task.resultCh <- stmtParseResult{err: err}:
				default:
				}
				return
			}
			if stop {
				if len(rows) > 0 {
					select {
					case task.resultCh <- stmtParseResult{rows: rows}:
					default:
					}
				} else {
					close(task.resultCh)
				}
				return
			}
			if len(row) > 0 {
				rows = append(rows, row)
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}
		select {
		case task.resultCh <- stmtParseResult{rows: rows}:
		default:
			return
		}
	}
}

func (r *HistoryReader) readBatchLinesFromReader(reader *bufio.Reader) ([][]byte, error) {
	ctx := r.ctx
	lines := make([][]byte, 0, parseStatementsBatchSize)
	for n := 0; n < parseStatementsBatchSize; n++ {
		if isCtxDone(ctx) {
			return nil, ctx.Err()
		}
		line, err := readLine(reader)
		if err != nil {
			if err == io.EOF {
				file := r.files.next()
				if file == nil {
					break
				}
				reader.Reset(file)
				continue
			}
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, nil
}

// The second value returned indicates whether the file should end reading.
func (r *HistoryReader) parseAndCheckRow(raw []byte) ([]types.Datum, bool, error) {
	var record StmtRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, false, err
	}
	if !r.checker.isTimeValid(record.Begin, record.End) {
		if r.checker.needStop(record.Begin) {
			return nil, true, nil
		}
		return nil, false, nil
	}
	if !r.checker.isDigestValid(record.Digest) {
		return nil, false, nil
	}
	if !r.checker.hasPrivilege(record.AuthUsers) {
		return nil, false, nil
	}
	row := make([]types.Datum, len(r.columnFactories))
	for n, factory := range r.columnFactories {
		row[n] = types.NewDatum(factory(r, &record))
	}
	return row, false, nil
}

// getInstanceAddr implements columnInfo.
func (r *HistoryReader) getInstanceAddr() string {
	return r.instanceAddr
}

// getInstanceAddr implements columnInfo.
func (r *HistoryReader) getTimeLocation() *time.Location {
	return r.timeLocation
}

type stmtChecker struct {
	user           *auth.UserIdentity
	hasProcessPriv bool // If the user has the 'PROCESS' privilege, he can read all statements.
	digests        set.StringSet
	timeRanges     []*StmtTimeRange
}

func (c *stmtChecker) hasPrivilege(authUsers map[string]struct{}) bool {
	authed := true
	if c.user != nil && !c.hasProcessPriv {
		if len(authUsers) == 0 {
			return false
		}
		_, authed = authUsers[c.user.Username]
	}
	return authed
}

func (c *stmtChecker) isDigestValid(digest string) bool {
	if c.digests == nil {
		return true
	}
	return c.digests.Exist(digest)
}

func (c *stmtChecker) isTimeValid(begin, end int64) bool {
	if len(c.timeRanges) == 0 {
		return true
	}
	for _, tr := range c.timeRanges {
		if timeRangeOverlap(begin, end, tr.Begin, tr.End) {
			return true
		}
	}
	return false
}

func (c *stmtChecker) needStop(curBegin int64) bool {
	if len(c.timeRanges) == 0 {
		return false
	}
	stop := true
	for _, tr := range c.timeRanges {
		if tr.End == 0 || tr.End >= curBegin {
			stop = false
		}
	}
	return stop
}

type stmtTinyRecord struct {
	Begin int64 `json:"begin"`
	End   int64 `json:"end"`
}

type stmtFile struct {
	file  *os.File
	begin int64
	end   int64
}

func openStmtFile(path string) (*stmtFile, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	f := &stmtFile{file: file}
	f.begin, err = f.parseBeginTsAndReseek()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}
	f.end, err = f.parseEndTs()
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (f *stmtFile) parseBeginTsAndReseek() (int64, error) {
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	firstLine, err := readLine(bufio.NewReader(f.file))
	if err != nil {
		return 0, err
	}
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if len(firstLine) == 0 {
		return 0, nil
	}
	var record stmtTinyRecord
	if err := json.Unmarshal(firstLine, &record); err != nil {
		return 0, err
	}
	return record.Begin, nil
}

func (f *stmtFile) parseEndTs() (int64, error) {
	filename := config.GetGlobalConfig().Instance.StmtSummaryFilename
	ext := filepath.Ext(filename)
	prefix := filename[:len(filename)-len(ext)]

	filename = filepath.Base(f.file.Name())
	ext = filepath.Ext(f.file.Name())
	filename = filename[:len(filename)-len(ext)]
	if strings.HasPrefix(filename, prefix+"-") {
		timeStr := strings.TrimPrefix(filename, prefix+"-")
		end, err := time.ParseInLocation(logFileTimeFormat, timeStr, time.Local)
		if err != nil {
			return 0, err
		}
		return end.Unix(), nil
	}
	return 0, nil
}

type stmtFiles struct {
	id    int
	files []*stmtFile
}

func newStmtFiles(ctx context.Context, timeRanges []*StmtTimeRange) (*stmtFiles, error) {
	filename := config.GetGlobalConfig().Instance.StmtSummaryFilename
	dir := filepath.Dir(filename)
	ext := filepath.Ext(filename)
	prefix := filename[:len(filename)-len(ext)]
	var files []*stmtFile
	walkFn := func(path string, info os.DirEntry) error {
		if info.IsDir() {
			return nil
		}
		if !strings.HasPrefix(path, prefix) {
			return nil
		}
		if isCtxDone(ctx) {
			return ctx.Err()
		}
		file, err := openStmtFile(path)
		if err != nil {
			logutil.BgLogger().Warn("failed to open or parse statements file", zap.Error(err), zap.String("path", path))
			return nil
		}
		if len(timeRanges) == 0 {
			files = append(files, file)
			return nil
		}
		for _, tr := range timeRanges {
			if timeRangeOverlap(file.begin, file.end, tr.Begin, tr.End) {
				files = append(files, file)
				return nil
			}
		}
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if err := walkFn(filepath.Join(dir, entry.Name()), entry); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(files, func(i, j *stmtFile) bool {
		return i.begin < j.begin
	})
	return &stmtFiles{files: files}, nil
}

func (f *stmtFiles) next() *os.File {
	if f.id >= len(f.files) {
		return nil
	}
	file := f.files[f.id].file
	f.id++
	return file
}

func (f *stmtFiles) close() {
	for _, f := range f.files {
		if err := f.file.Close(); err != nil {
			logutil.BgLogger().Error("failed to close statements file", zap.Error(err))
		}
	}
}

type stmtParseTask struct {
	resultCh chan stmtParseResult
}

type stmtParseResult struct {
	rows [][]types.Datum
	err  error
}

func isCtxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func readLine(reader *bufio.Reader) ([]byte, error) {
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
		resByte = append(resByte, tempLine...) // nozero
		// Use the max value of max_allowed_packet to check the single line length.
		if len(resByte) > maxLineSize {
			return resByte, errors.Errorf("single line length exceeds limit: %v", maxLineSize)
		}
		if err != nil {
			return resByte, err
		}
	}
	return resByte, err
}

func timeRangeOverlap(aBegin, aEnd, bBegin, bEnd int64) bool {
	if aEnd == 0 || aEnd < aBegin {
		aEnd = math.MaxInt64
	}
	if bEnd == 0 || bEnd < bBegin {
		bEnd = math.MaxInt64
	}
	// ------------------------------>   (timeline)
	//  |        |      |        |
	// aBegin   aEnd   bBegin   bEnd
	if aEnd < bBegin {
		return false
	}
	// ------------------------------>   (timeline)
	//  |        |      |        |
	// bBegin   bEnd   aBegin   aEnd
	if bEnd < aBegin {
		return false
	}
	// ------------------------------>   (timeline)
	//  |        |        |      |
	// aBegin   bBegin   aEnd   bEnd
	//
	// or
	//
	// ------------------------------>   (timeline)
	//  |        |        |      |
	// bBegin   aBegin   aEnd   bEnd
	//
	// or
	//
	// ------------------------------>   (timeline)
	//  |        |        |      |
	// bBegin   aBegin   bEnd   aEnd
	//
	// or
	//
	// ------------------------------>   (timeline)
	//  |        |        |      |
	// aBegin   bBegin   bEnd   aEnd
	return true
}
