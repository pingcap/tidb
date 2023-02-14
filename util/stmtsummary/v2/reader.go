// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	logFileTimeFormat = "2006-01-02T15-04-05.000" // depends on lumberjack.go#backupTimeFormat
	maxLineSize       = 1073741824

	batchScanSize = 64
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
	r.s.windowLock.Lock()
	w := r.s.window
	if !r.checker.isTimeValid(w.begin.Unix(), end) {
		r.s.windowLock.Unlock()
		return nil
	}
	values := w.lru.Values()
	evicted := w.evicted
	r.s.windowLock.Unlock()
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
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	instanceAddr string
	timeLocation *time.Location

	columnFactories []columnFactory
	checker         *stmtChecker
	files           *stmtFiles

	concurrent int
	rowsCh     <-chan [][]types.Datum
	errCh      <-chan error
}

// NewHistoryReader creates a HisroryReader from StmtSummary and other
// necessary parameters. If timeRanges is present, only files within
// the time range will be read.
func NewHistoryReader(
	ctx context.Context,
	columns []*model.ColumnInfo,
	instanceAddr string,
	timeLocation *time.Location,
	user *auth.UserIdentity,
	hasProcessPriv bool,
	digests set.StringSet,
	timeRanges []*StmtTimeRange,
	concurrent int,
) (*HistoryReader, error) {
	files, err := newStmtFiles(ctx, timeRanges)
	if err != nil {
		return nil, err
	}

	if concurrent < 2 {
		concurrent = 2
	}
	rowsCh := make(chan [][]types.Datum, concurrent)
	errCh := make(chan error, concurrent)

	ctx, cancel := context.WithCancel(ctx)
	r := &HistoryReader{
		ctx:    ctx,
		cancel: cancel,

		instanceAddr:    instanceAddr,
		timeLocation:    timeLocation,
		columnFactories: makeColumnFactories(columns),
		checker: &stmtChecker{
			user:           user,
			hasProcessPriv: hasProcessPriv,
			digests:        digests,
			timeRanges:     timeRanges,
		},
		files:      files,
		concurrent: concurrent,
		rowsCh:     rowsCh,
		errCh:      errCh,
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.scheduleTasks(rowsCh, errCh)
	}()
	return r, nil
}

// Rows returns rows converted from records in files. Reading and parsing
// works asynchronously. If (nil, nil) is returned, it means that the
// reading has been completed.
func (r *HistoryReader) Rows() ([][]types.Datum, error) {
	ctx := r.ctx
	for {
		select {
		case err := <-r.errCh:
			return nil, err
		case rows, ok := <-r.rowsCh:
			if !ok {
				select {
				case err := <-r.errCh:
					return nil, err
				default:
					return nil, nil
				}
			}
			if len(rows) == 0 {
				continue
			}
			return rows, nil
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

// 4 roles to handle the read task in pipeline:
//
// ## Pipeline
// .           +--------------+             +---------------+
// == files => | scan workers | == lines => | parse workers | == rows =>
// . filesCh   +--------------+   linesCh   +---------------+   rowsCh
//
// ## Roles
// +--------------+--------------+------------------------------------+
// |     ROLE     |    COUNT     |          DESCRIPTION               |
// +--------------+--------------+------------------------------------+
// | Scan Worker  | concurrent/2 | Scan files (I/O) first, then help  |
// |              |              | parse workers to parse lines (CPU) |
// +--------------+--------------+------------------------------------+
// | Parse Worker | concurrent-  | Parse lines (CPU) to rows          |
// |              | concurrent/2 |                                    |
// +--------------+--------------+------------------------------------+
// |   Manager    |      1       | Drive the whole process and notify |
// |              |              | scan workers to switch role        |
// +--------------+--------------+------------------------------------+
// |   Monitor    |      1       | Cover failures and notify workers  |
// |              |              | to exit                            |
// +--------------+--------------+------------------------------------+
func (r *HistoryReader) scheduleTasks(
	rowsCh chan<- [][]types.Datum,
	errCh chan<- error,
) {
	if r.files == nil || len(r.files.files) == 0 {
		close(rowsCh)
		return
	}

	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()

	scanWorker := &stmtScanWorker{
		ctx:       ctx,
		batchSize: batchScanSize,
		checker:   r.checker,
	}
	parseWorker := &stmtParseWorker{
		ctx:             ctx,
		instanceAddr:    r.instanceAddr,
		timeLocation:    r.timeLocation,
		checker:         r.checker,
		columnFactories: r.columnFactories,
	}

	concurrent := r.concurrent
	filesCh := make(chan *os.File, concurrent)
	linesCh := make(chan [][]byte, concurrent)
	innerErrCh := make(chan error, concurrent)

	var scanWg sync.WaitGroup
	scanWg.Add(concurrent / 2)
	scanDone := scanWg.Done
	waitScanAllDone := scanWg.Wait

	var parseWg sync.WaitGroup
	parseWg.Add(concurrent) // finally all workers will become parse workers
	parseDone := parseWg.Done
	waitParseAllDone := parseWg.Wait

	// Half of workers are scheduled to scan files and then parse lines.
	for i := 0; i < concurrent/2; i++ {
		go func() {
			scanWorker.run(filesCh, linesCh, innerErrCh)
			scanDone()

			parseWorker.run(linesCh, rowsCh, innerErrCh)
			parseDone()
		}()
	}

	// Remaining workers are scheduled to parse lines.
	for i := concurrent / 2; i < concurrent; i++ {
		go func() {
			parseWorker.run(linesCh, rowsCh, innerErrCh)
			parseDone()
		}()
	}

	// Manager drives the whole process
	var mgrWg sync.WaitGroup
	mgrWg.Add(1)
	go func() {
		defer mgrWg.Done()

		func() {
			for _, file := range r.files.files {
				select {
				case filesCh <- file.file:
				case <-ctx.Done():
					return
				}
			}
		}()
		// No scan tasks to be generating. Notify idle scan
		// workers to become parse workers
		close(filesCh)

		// No parse tasks to be generating once all scan
		// tasks are done. Notify idle parse workers to exit
		waitScanAllDone()
		close(linesCh)

		// No rows to be generating once all parse tasks
		// are done. Notify monitor to close rowsCh
		waitParseAllDone()
		cancel()
	}()

	// Monitor to cover failures and notify workers to exit
	select {
	case err := <-innerErrCh:
		select {
		case errCh <- err:
		default:
		}
		cancel() // notify workers to exit
	case <-ctx.Done():
		// notified by manager or parent ctx is canceled
	}
	mgrWg.Wait()
	close(rowsCh) // task done
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
	begin, err := parseBeginTsAndReseek(file)
	if err != nil {
		if err != io.EOF {
			_ = file.Close()
			return nil, err
		}
	}
	end, err := parseEndTs(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &stmtFile{
		file:  file,
		begin: begin,
		end:   end,
	}, nil
}

func parseBeginTsAndReseek(file *os.File) (int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	reader := bufio.NewReader(file)
	var record stmtTinyRecord
	for { // ignore invalid lines
		line, err := readLine(reader)
		if err != nil {
			return 0, err
		}
		err = json.Unmarshal(line, &record)
		if err == nil {
			break
		}
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	return record.Begin, nil
}

func parseEndTs(file *os.File) (int64, error) {
	// tidb-statements.log
	filename := config.GetGlobalConfig().Instance.StmtSummaryFilename
	// .log
	ext := filepath.Ext(filename)
	// tidb-statements
	prefix := filename[:len(filename)-len(ext)]

	// tidb-statements-2022-12-27T16-21-20.245.log
	filename = filepath.Base(file.Name())
	// .log
	ext = filepath.Ext(file.Name())
	// tidb-statements-2022-12-27T16-21-20.245
	filename = filename[:len(filename)-len(ext)]

	if strings.HasPrefix(filename, prefix+"-") {
		// 2022-12-27T16-21-20.245
		timeStr := strings.TrimPrefix(filename, prefix+"-")
		end, err := time.ParseInLocation(logFileTimeFormat, timeStr, time.Local)
		if err != nil {
			return 0, err
		}
		return end.Unix(), nil
	}
	return 0, nil
}

func (f *stmtFile) close() error {
	if f.file != nil {
		return f.file.Close()
	}
	return nil
}

type stmtFiles struct {
	files []*stmtFile
}

func newStmtFiles(ctx context.Context, timeRanges []*StmtTimeRange) (*stmtFiles, error) {
	filename := config.GetGlobalConfig().Instance.StmtSummaryFilename
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

	dir := filepath.Dir(filename)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if err := walkFn(filepath.Join(dir, entry.Name()), entry); err != nil {
			for _, f := range files {
				_ = f.close()
			}
			return nil, err
		}
	}
	slices.SortFunc(files, func(i, j *stmtFile) bool {
		return i.begin < j.begin
	})
	return &stmtFiles{files: files}, nil
}

func (f *stmtFiles) close() {
	for _, f := range f.files {
		_ = f.close()
	}
}

type stmtScanWorker struct {
	ctx       context.Context
	batchSize int
	checker   *stmtChecker
}

func (w *stmtScanWorker) run(
	fileCh <-chan *os.File,
	linesCh chan<- [][]byte,
	errCh chan<- error,
) {
	for {
		select {
		case file, ok := <-fileCh:
			if !ok {
				return
			}
			w.handleFile(file, linesCh, errCh)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *stmtScanWorker) handleFile(
	file *os.File,
	linesCh chan<- [][]byte,
	errCh chan<- error,
) {
	if file == nil {
		return
	}

	reader := bufio.NewReader(file)
	for {
		if isCtxDone(w.ctx) {
			return
		}

		lines, err := w.readlines(reader)
		if err == io.EOF {
			return
		}
		if err != nil {
			w.putErr(err, errCh)
			return
		}

		w.putLines(lines, linesCh)
	}
}

func (w *stmtScanWorker) putErr(
	err error,
	errCh chan<- error,
) {
	select {
	case errCh <- err:
	case <-w.ctx.Done():
	}
}

func (w *stmtScanWorker) putLines(
	lines [][]byte,
	linesCh chan<- [][]byte,
) {
	select {
	case linesCh <- lines:
	case <-w.ctx.Done():
	}
}

func (w *stmtScanWorker) readlines(reader *bufio.Reader) ([][]byte, error) {
	var firstLine []byte
	var record *stmtTinyRecord
	for { // ingore invalid lines
		var err error
		firstLine, err = readLine(reader)
		if err != nil {
			return nil, err
		}

		record, err = w.parse(firstLine)
		if err == nil {
			break
		}
	}

	if w.needStop(record) {
		// done because remaining lines in file
		// are not in the time range
		return nil, io.EOF
	}

	lines := make([][]byte, 0, w.batchSize)
	lines = append(lines, firstLine)

	newLines, err := readLines(reader, w.batchSize-1)
	if err == io.EOF {
		return lines, nil
	}
	if err != nil {
		return nil, err
	}

	lines = append(lines, newLines...)
	return lines, nil
}

func (*stmtScanWorker) parse(raw []byte) (*stmtTinyRecord, error) {
	var record stmtTinyRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (w *stmtScanWorker) needStop(record *stmtTinyRecord) bool {
	return w.checker.needStop(record.Begin)
}

type stmtParseWorker struct {
	ctx             context.Context
	instanceAddr    string
	timeLocation    *time.Location
	checker         *stmtChecker
	columnFactories []columnFactory
}

func (w *stmtParseWorker) run(
	linesCh <-chan [][]byte,
	rowsCh chan<- [][]types.Datum,
	errCh chan<- error,
) {
	for {
		select {
		case lines, ok := <-linesCh:
			if !ok {
				return
			}
			w.handleLines(lines, rowsCh, errCh)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *stmtParseWorker) handleLines(
	lines [][]byte,
	rowsCh chan<- [][]types.Datum,
	_ chan<- error,
) {
	if len(lines) == 0 {
		return
	}

	rows := make([][]types.Datum, 0, len(lines))
	for _, line := range lines {
		record, err := w.parse(line)
		if err != nil {
			// ignore invalid lines
			continue
		}

		if w.needStop(record) {
			break
		}

		if !w.matchConds(record) {
			continue
		}

		row := w.buildRow(record)
		rows = append(rows, row)
	}

	if len(rows) > 0 {
		w.putRows(rows, rowsCh)
	}
}

func (w *stmtParseWorker) putRows(
	rows [][]types.Datum,
	rowsCh chan<- [][]types.Datum,
) {
	select {
	case rowsCh <- rows:
	case <-w.ctx.Done():
	}
}

func (*stmtParseWorker) parse(raw []byte) (*StmtRecord, error) {
	var record StmtRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (w *stmtParseWorker) needStop(record *StmtRecord) bool {
	return w.checker.needStop(record.Begin)
}

func (w *stmtParseWorker) matchConds(record *StmtRecord) bool {
	if !w.checker.isTimeValid(record.Begin, record.End) {
		return false
	}
	if !w.checker.isDigestValid(record.Digest) {
		return false
	}
	if !w.checker.hasPrivilege(record.AuthUsers) {
		return false
	}
	return true
}

func (w *stmtParseWorker) buildRow(record *StmtRecord) []types.Datum {
	row := make([]types.Datum, len(w.columnFactories))
	for n, factory := range w.columnFactories {
		row[n] = types.NewDatum(factory(w, record))
	}
	return row
}

// getInstanceAddr implements columnInfo.
func (w *stmtParseWorker) getInstanceAddr() string {
	return w.instanceAddr
}

// getInstanceAddr implements columnInfo.
func (w *stmtParseWorker) getTimeLocation() *time.Location {
	return w.timeLocation
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
	return util.ReadLine(reader, maxLineSize)
}

func readLines(reader *bufio.Reader, count int) ([][]byte, error) {
	return util.ReadLines(reader, count, maxLineSize)
}

func timeRangeOverlap(aBegin, aEnd, bBegin, bEnd int64) bool {
	if aEnd == 0 || aEnd < aBegin {
		aEnd = math.MaxInt64
	}
	if bEnd == 0 || bEnd < bBegin {
		bEnd = math.MaxInt64
	}
	// https://stackoverflow.com/questions/3269434/whats-the-most-efficient-way-to-test-if-two-ranges-overlap
	return aBegin <= bEnd && aEnd >= bBegin
}
