// Copyright 2018 PingCAP, Inc.
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
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// LoadDataFormatSQLDump represents the data source file of LOAD DATA is
	// mydumper-format DML file
	LoadDataFormatSQLDump = "sqldumpfile"
	// LoadDataFormatParquet represents the data source file of LOAD DATA is
	// parquet
	LoadDataFormatParquet = "parquet"
)

var (
	taskQueueSize = 16 // the maximum number of pending tasks to commit in queue
	// InTest is a flag that bypass gcs authentication in unit tests.
	InTest bool
)

// LoadDataExec represents a load data executor.
type LoadDataExec struct {
	baseExecutor

	FileLocRef   ast.FileLocRefTp
	OnDuplicate  ast.OnDuplicateKeyHandlingType
	loadDataInfo *LoadDataInfo
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)

	if e.loadDataInfo.Path == "" {
		return errors.New("Load Data: infile path is empty")
	}
	if !e.loadDataInfo.Table.Meta().IsBaseTable() {
		return errors.New("can only load data into base tables")
	}

	// CSV-like
	if e.loadDataInfo.Format == "" {
		if e.loadDataInfo.NullInfo != nil && e.loadDataInfo.NullInfo.OptEnclosed &&
			(e.loadDataInfo.FieldsInfo == nil || e.loadDataInfo.FieldsInfo.Enclosed == nil) {
			return errors.New("must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED")
		}
		// TODO: support lines terminated is "".
		if len(e.loadDataInfo.LinesInfo.Terminated) == 0 {
			return errors.New("Load Data: don't support load data terminated is nil")
		}
	}

	switch e.FileLocRef {
	case ast.FileLocServerOrRemote:
		u, err := storage.ParseRawURL(e.loadDataInfo.Path)
		if err != nil {
			return err
		}
		var filename string
		u.Path, filename = filepath.Split(u.Path)
		b, err := storage.ParseBackendFromURL(u, nil)
		if err != nil {
			return err
		}
		if b.GetLocal() != nil {
			return errors.Errorf("Load Data: don't support load data from tidb-server's disk")
		}
		return e.loadFromRemote(ctx, b, filename)
	case ast.FileLocClient:
		// let caller use handleQuerySpecial to read data in this connection
		sctx := e.loadDataInfo.ctx
		val := sctx.Value(LoadDataVarKey)
		if val != nil {
			sctx.SetValue(LoadDataVarKey, nil)
			return errors.New("Load Data: previous load data option wasn't closed normally")
		}
		sctx.SetValue(LoadDataVarKey, e.loadDataInfo)
	}
	return nil
}

func (e *LoadDataExec) loadFromRemote(
	ctx context.Context,
	b *backup.StorageBackend,
	filename string,
) error {
	opt := &storage.ExternalStorageOptions{}
	if InTest {
		opt.NoCredentials = true
	}
	s, err := storage.New(ctx, b, opt)
	if err != nil {
		return err
	}
	fileReader, err := s.Open(ctx, filename)
	if err != nil {
		return err
	}
	defer fileReader.Close()

	e.loadDataInfo.loadRemoteInfo = loadRemoteInfo{
		store: s,
		path:  filename,
	}
	return e.loadDataInfo.Load(ctx, fileReader)
}

// Close implements the Executor Close interface.
func (e *LoadDataExec) Close() error {
	if e.runtimeStats != nil && e.loadDataInfo != nil && e.loadDataInfo.stats != nil {
		defer e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.loadDataInfo.stats)
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadDataExec) Open(ctx context.Context) error {
	if e.loadDataInfo.insertColumns != nil {
		e.loadDataInfo.initEvalBuffer()
	}
	// Init for runtime stats.
	e.loadDataInfo.collectRuntimeStatsEnabled()
	return nil
}

// commitTask is used for fetching data from data preparing routine into committing routine.
type commitTask struct {
	cnt  uint64
	rows [][]types.Datum
}

type loadRemoteInfo struct {
	store storage.ExternalStorage
	path  string
}

// LoadDataInfo saves the information of loading data operation.
// TODO: rename it and remove unnecessary public methods.
type LoadDataInfo struct {
	*InsertValues

	row         []types.Datum
	Path        string
	Format      string
	Table       table.Table
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	NullInfo    *ast.NullDefinedBy
	IgnoreLines uint64
	Ctx         sessionctx.Context
	rows        [][]types.Datum
	Drained     bool

	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	FieldMappings      []*FieldMapping

	commitTaskQueue chan commitTask
	StopCh          chan struct{}
	QuitCh          chan struct{}
	OnDuplicate     ast.OnDuplicateKeyHandlingType

	loadRemoteInfo loadRemoteInfo
}

// FieldMapping indicates the relationship between input field and table column or user variable
type FieldMapping struct {
	Column  *table.Column
	UserVar *ast.VariableExpr
}

// Load reads from readerFn and do load data job.
func (e *LoadDataInfo) Load(ctx context.Context, reader io.ReadSeekCloser) error {
	e.initQueues()
	e.SetMaxRowsInBatch(uint64(e.Ctx.GetSessionVars().DMLBatchSize))
	e.startStopWatcher()
	// let stop watcher goroutine quit
	defer e.forceQuit()
	err := sessiontxn.NewTxn(ctx, e.Ctx)
	if err != nil {
		return err
	}
	// processStream process input data, enqueue commit task
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go processStream(ctx, reader, e, wg)
	err = e.commitWork(ctx)
	wg.Wait()
	return err
}

// processStream process input stream from network
func processStream(ctx context.Context, reader io.ReadSeekCloser, loadDataInfo *LoadDataInfo, wg *sync.WaitGroup) {
	var (
		parser mydump.Parser
		err    error
	)
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("process routine panicked",
				zap.Reflect("r", r),
				zap.Stack("stack"))
		}
		if err != nil {
			logutil.Logger(ctx).Error("process routine meet error",
				zap.Error(err))
		}
		if err != nil || r != nil {
			loadDataInfo.forceQuit()
		} else {
			loadDataInfo.CloseTaskQueue()
		}
		wg.Done()
	}()

	switch strings.ToLower(loadDataInfo.Format) {
	case "":
		// CSV-like
		parser, err = mydump.NewCSVParser(
			ctx,
			loadDataInfo.GenerateCSVConfig(),
			reader,
			int64(config.ReadBlockSize),
			nil,
			false,
			// TODO: support charset conversion
			nil)
	case LoadDataFormatSQLDump:
		parser = mydump.NewChunkParser(
			ctx,
			loadDataInfo.Ctx.GetSessionVars().SQLMode,
			reader,
			int64(config.ReadBlockSize),
			nil,
		)
	case LoadDataFormatParquet:
		if loadDataInfo.loadRemoteInfo.store == nil {
			err = errors.New("parquet format requires remote storage")
			return
		}
		parser, err = mydump.NewParquetParser(
			ctx,
			loadDataInfo.loadRemoteInfo.store,
			reader,
			loadDataInfo.loadRemoteInfo.path,
		)
	default:
		err = errors.Errorf("unsupported format: %s", loadDataInfo.Format)
	}
	if err != nil {
		return
	}

	parser.SetLogger(log.Logger{Logger: logutil.Logger(ctx)})

	for {
		// prepare batch and enqueue task
		err = loadDataInfo.ReadRows(ctx, parser)
		if err != nil {
			logutil.Logger(ctx).Error("load data process stream error in ReadRows", zap.Error(err))
			return
		}
		if loadDataInfo.curBatchCnt == 0 {
			return
		}
		if err = loadDataInfo.enqOneTask(ctx); err != nil {
			logutil.Logger(ctx).Error("load data process stream error in enqOneTask", zap.Error(err))
			return
		}
	}
}

// reorderColumns reorder the e.insertColumns according to the order of columnNames
// Note: We must ensure there must be one-to-one mapping between e.insertColumns and columnNames in terms of column name.
func (e *LoadDataInfo) reorderColumns(columnNames []string) error {
	cols := e.insertColumns

	if len(cols) != len(columnNames) {
		return ErrColumnsNotMatched
	}

	reorderedColumns := make([]*table.Column, len(cols))

	if columnNames == nil {
		return nil
	}

	mapping := make(map[string]int)
	for idx, colName := range columnNames {
		mapping[strings.ToLower(colName)] = idx
	}

	for _, col := range cols {
		idx := mapping[col.Name.L]
		reorderedColumns[idx] = col
	}

	e.insertColumns = reorderedColumns

	return nil
}

// initLoadColumns sets columns which the input fields loaded to.
func (e *LoadDataInfo) initLoadColumns(columnNames []string) error {
	var cols []*table.Column
	var missingColName string
	var err error
	tableCols := e.Table.Cols()

	if len(columnNames) != len(tableCols) {
		for _, v := range e.ColumnAssignments {
			columnNames = append(columnNames, v.Column.Name.O)
		}
	}

	cols, missingColName = table.FindCols(tableCols, columnNames, e.Table.Meta().PKIsHandle)
	if missingColName != "" {
		return dbterror.ErrBadField.GenWithStackByArgs(missingColName, "field list")
	}

	for _, col := range cols {
		if !col.IsGenerated() {
			e.insertColumns = append(e.insertColumns, col)
		}
		if col.Name.L == model.ExtraHandleName.L {
			if !e.ctx.GetSessionVars().AllowWriteRowID {
				return errors.Errorf("load data statement for _tidb_rowid are not supported")
			}
			e.hasExtraHandle = true
			break
		}
	}

	// e.insertColumns is appended according to the original tables' column sequence.
	// We have to reorder it to follow the use-specified column order which is shown in the columnNames.
	if err = e.reorderColumns(columnNames); err != nil {
		return err
	}

	e.rowLen = len(e.insertColumns)
	// Check column whether is specified only once.
	err = table.CheckOnce(cols)
	if err != nil {
		return err
	}

	return nil
}

// initFieldMappings make a field mapping slice to implicitly map input field to table column or user defined variable
// the slice's order is the same as the order of the input fields.
// Returns a slice of same ordered column names without user defined variable names.
func (e *LoadDataInfo) initFieldMappings() []string {
	columns := make([]string, 0, len(e.ColumnsAndUserVars)+len(e.ColumnAssignments))
	tableCols := e.Table.Cols()

	if len(e.ColumnsAndUserVars) == 0 {
		for _, v := range tableCols {
			fieldMapping := &FieldMapping{
				Column: v,
			}
			e.FieldMappings = append(e.FieldMappings, fieldMapping)
			columns = append(columns, v.Name.O)
		}

		return columns
	}

	var column *table.Column

	for _, v := range e.ColumnsAndUserVars {
		if v.ColumnName != nil {
			column = table.FindCol(tableCols, v.ColumnName.Name.O)
			columns = append(columns, v.ColumnName.Name.O)
		} else {
			column = nil
		}

		fieldMapping := &FieldMapping{
			Column:  column,
			UserVar: v.UserVar,
		}
		e.FieldMappings = append(e.FieldMappings, fieldMapping)
	}

	return columns
}

// GetRows getter for rows
func (e *LoadDataInfo) GetRows() [][]types.Datum {
	return e.rows
}

// GetCurBatchCnt getter for curBatchCnt
func (e *LoadDataInfo) GetCurBatchCnt() uint64 {
	return e.curBatchCnt
}

// CloseTaskQueue preparing routine to inform commit routine no more data
func (e *LoadDataInfo) CloseTaskQueue() {
	close(e.commitTaskQueue)
}

// initQueues initialize task queue and error report queue
func (e *LoadDataInfo) initQueues() {
	e.commitTaskQueue = make(chan commitTask, taskQueueSize)
	e.StopCh = make(chan struct{}, 2)
	e.QuitCh = make(chan struct{})
}

// startStopWatcher monitor StopCh to force quit
func (e *LoadDataInfo) startStopWatcher() {
	go func() {
		<-e.StopCh
		close(e.QuitCh)
	}()
}

// forceQuit let commit quit directly
func (e *LoadDataInfo) forceQuit() {
	e.StopCh <- struct{}{}
}

// makeCommitTask produce commit task with data in LoadDataInfo.rows LoadDataInfo.curBatchCnt
func (e *LoadDataInfo) makeCommitTask() commitTask {
	return commitTask{e.curBatchCnt, e.rows}
}

// enqOneTask feed one batch commit task to commit work
func (e *LoadDataInfo) enqOneTask(ctx context.Context) error {
	var err error
	if e.curBatchCnt > 0 {
		select {
		case e.commitTaskQueue <- e.makeCommitTask():
		case <-e.QuitCh:
			err = errors.New("enqOneTask forced to quit")
			logutil.Logger(ctx).Error("enqOneTask forced to quit, possible commitWork error")
			return err
		}
		// reset rows buffer, will reallocate buffer but NOT reuse
		e.SetMaxRowsInBatch(e.maxRowsInBatch)
	}
	return err
}

// CommitOneTask insert Data from LoadDataInfo.rows, then make commit and refresh txn
func (e *LoadDataInfo) CommitOneTask(ctx context.Context, task commitTask) error {
	var err error
	defer func() {
		if err != nil {
			e.Ctx.StmtRollback(ctx, false)
		}
	}()
	err = e.CheckAndInsertOneBatch(ctx, task.rows, task.cnt)
	if err != nil {
		logutil.Logger(ctx).Error("commit error CheckAndInsert", zap.Error(err))
		return err
	}
	failpoint.Inject("commitOneTaskErr", func() error {
		return errors.New("mock commit one task error")
	})
	e.Ctx.StmtCommit(ctx)
	// Make sure process stream routine never use invalid txn
	e.txnInUse.Lock()
	defer e.txnInUse.Unlock()
	// Make sure that there are no retries when committing.
	if err = e.Ctx.RefreshTxnCtx(ctx); err != nil {
		logutil.Logger(ctx).Error("commit error refresh", zap.Error(err))
		return err
	}
	return err
}

// commitWork commit batch sequentially
func (e *LoadDataInfo) commitWork(ctx context.Context) error {
	var err error
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("commitWork panicked",
				zap.Reflect("r", r),
				zap.Stack("stack"))
		}
		if err != nil || r != nil {
			e.forceQuit()
		}
		if err != nil {
			e.ctx.StmtRollback(ctx, false)
		}
	}()
	var tasks uint64
	var end = false
	for !end {
		select {
		case <-e.QuitCh:
			err = errors.New("commit forced to quit")
			logutil.Logger(ctx).Error("commit forced to quit, possible preparation failed")
			return err
		case commitTask, ok := <-e.commitTaskQueue:
			if ok {
				start := time.Now()
				err = e.CommitOneTask(ctx, commitTask)
				if err != nil {
					break
				}
				tasks++
				logutil.Logger(ctx).Info("commit one task success",
					zap.Duration("commit time usage", time.Since(start)),
					zap.Uint64("keys processed", commitTask.cnt),
					zap.Uint64("tasks processed", tasks),
					zap.Int("tasks in queue", len(e.commitTaskQueue)))
			} else {
				end = true
			}
		}
		if err != nil {
			logutil.Logger(ctx).Error("load data commit work error", zap.Error(err))
			break
		}
		if atomic.CompareAndSwapUint32(&e.Ctx.GetSessionVars().Killed, 1, 0) {
			logutil.Logger(ctx).Info("load data query interrupted quit data processing")
			err = ErrQueryInterrupted
			break
		}
	}
	return err
}

// SetMaxRowsInBatch sets the max number of rows to insert in a batch.
func (e *LoadDataInfo) SetMaxRowsInBatch(limit uint64) {
	e.maxRowsInBatch = limit
	e.rows = make([][]types.Datum, 0, limit)
	e.curBatchCnt = 0
}

// ReadRows reads rows from parser. When parser's reader meet EOF, it will return
// nil. For other errors it will return directly. When the rows batch is full it
// will also return nil.
// The result rows are saved in e.rows and update some members, caller can check
// if curBatchCnt == 0 to know if reached EOF.
func (e *LoadDataInfo) ReadRows(ctx context.Context, parser mydump.Parser) error {
	ignoreOneLineFn := parser.ReadRow
	if csvParser, ok := parser.(*mydump.CSVParser); ok {
		ignoreOneLineFn = func() error {
			_, _, err := csvParser.ReadUntilTerminator()
			return err
		}
	}

	for e.IgnoreLines > 0 {
		err := ignoreOneLineFn()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}
			return err
		}

		e.IgnoreLines--
	}
	for {
		if err := parser.ReadRow(); err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}
			return err
		}
		// rowCount will be used in fillRow(), last insert ID will be assigned according to the rowCount = 1.
		// So should add first here.
		e.rowCount++
		e.rows = append(e.rows, e.colsToRow(ctx, parser.LastRow().Row))
		e.curBatchCnt++
		if e.maxRowsInBatch != 0 && e.rowCount%e.maxRowsInBatch == 0 {
			logutil.Logger(ctx).Info("batch limit hit when inserting rows", zap.Int("maxBatchRows", e.maxChunkSize),
				zap.Uint64("totalRows", e.rowCount))
			return nil
		}
	}
}

// CheckAndInsertOneBatch is used to commit one transaction batch full filled data
func (e *LoadDataInfo) CheckAndInsertOneBatch(ctx context.Context, rows [][]types.Datum, cnt uint64) error {
	if e.stats != nil && e.stats.BasicRuntimeStats != nil {
		// Since this method will not call by executor Next,
		// so we need record the basic executor runtime stats by ourself.
		start := time.Now()
		defer func() {
			e.stats.BasicRuntimeStats.Record(time.Since(start), 0)
		}()
	}
	var err error
	if cnt == 0 {
		return err
	}
	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(cnt)

	replace := false
	if e.OnDuplicate == ast.OnDuplicateKeyHandlingReplace {
		replace = true
	}

	err = e.batchCheckAndInsert(ctx, rows[0:cnt], e.addRecordLD, replace)
	if err != nil {
		return err
	}
	return err
}

// SetMessage sets info message(ERR_LOAD_INFO) generated by LOAD statement, it is public because of the special way that
// LOAD statement is handled.
func (e *LoadDataInfo) SetMessage() {
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	numDeletes := stmtCtx.DeletedRows()
	numSkipped := numRecords - stmtCtx.CopiedRows()
	numWarnings := stmtCtx.WarningCount()
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo].Raw, numRecords, numDeletes, numSkipped, numWarnings)
	e.ctx.GetSessionVars().StmtCtx.SetMessage(msg)
}

// colsToRow encodes the data of parser output.
func (e *LoadDataInfo) colsToRow(ctx context.Context, cols []types.Datum) []types.Datum {
	row := make([]types.Datum, 0, len(e.insertColumns))
	sessionVars := e.Ctx.GetSessionVars()
	setVar := func(name string, col *types.Datum) {
		// User variable names are not case-sensitive
		// https://dev.mysql.com/doc/refman/8.0/en/user-variables.html
		name = strings.ToLower(name)
		if col == nil || col.IsNull() {
			sessionVars.UnsetUserVar(name)
		} else {
			sessionVars.SetUserVarVal(name, *col)
		}
	}

	for i := 0; i < len(e.FieldMappings); i++ {
		if i >= len(cols) {
			if e.FieldMappings[i].Column == nil {
				setVar(e.FieldMappings[i].UserVar.Name, nil)
				continue
			}

			// If some columns is missing and their type is time and has not null flag, they should be set as current time.
			if types.IsTypeTime(e.FieldMappings[i].Column.GetType()) && mysql.HasNotNullFlag(e.FieldMappings[i].Column.GetFlag()) {
				row = append(row, types.NewTimeDatum(types.CurrentTime(e.FieldMappings[i].Column.GetType())))
				continue
			}

			row = append(row, types.NewDatum(nil))
			continue
		}

		if e.FieldMappings[i].Column == nil {
			setVar(e.FieldMappings[i].UserVar.Name, &cols[i])
			continue
		}

		if cols[i].IsNull() {
			row = append(row, types.NewDatum(nil))
			continue
		}

		row = append(row, cols[i])
	}
	for i := 0; i < len(e.ColumnAssignments); i++ {
		// eval expression of `SET` clause
		d, err := expression.EvalAstExpr(e.Ctx, e.ColumnAssignments[i].Expr)
		if err != nil {
			e.handleWarning(err)
			return nil
		}
		row = append(row, d)
	}

	// a new row buffer will be allocated in getRow
	newRow, err := e.getRow(ctx, row)
	if err != nil {
		e.handleWarning(err)
		return nil
	}

	return newRow
}

func (e *LoadDataInfo) addRecordLD(ctx context.Context, row []types.Datum) error {
	if row == nil {
		return nil
	}
	err := e.addRecord(ctx, row)
	if err != nil {
		e.handleWarning(err)
		return err
	}
	return nil
}

// GenerateCSVConfig generates a CSV config for parser from LoadDataInfo.
func (e *LoadDataInfo) GenerateCSVConfig() *config.CSVConfig {
	var (
		nullDef          []string
		quotedNullIsText = true
	)

	if e.NullInfo != nil {
		nullDef = append(nullDef, e.NullInfo.NullDef)
		quotedNullIsText = !e.NullInfo.OptEnclosed
	} else if e.FieldsInfo.Enclosed != nil {
		nullDef = append(nullDef, "NULL")
	}
	if e.FieldsInfo.Escaped != nil {
		nullDef = append(nullDef, string([]byte{*e.FieldsInfo.Escaped, 'N'}))
	}

	enclosed := ""
	if e.FieldsInfo.Enclosed != nil {
		enclosed = string([]byte{*e.FieldsInfo.Enclosed})
	}
	escaped := ""
	if e.FieldsInfo.Escaped != nil {
		escaped = string([]byte{*e.FieldsInfo.Escaped})
	}

	return &config.CSVConfig{
		Separator: e.FieldsInfo.Terminated,
		// ignore optionally enclosed
		Delimiter:        enclosed,
		Terminator:       e.LinesInfo.Terminated,
		NotNull:          false,
		Null:             nullDef,
		Header:           false,
		TrimLastSep:      false,
		EscapedBy:        escaped,
		StartingBy:       e.LinesInfo.Starting,
		AllowEmptyLine:   true,
		QuotedNullIsText: quotedNullIsText,
		UnescapedQuote:   true,
	}
}

var _ io.ReadSeekCloser = (*SimpleSeekerOnReadCloser)(nil)

// SimpleSeekerOnReadCloser provides Seek(0, SeekCurrent) on ReadCloser.
type SimpleSeekerOnReadCloser struct {
	r   io.ReadCloser
	pos int
}

// NewSimpleSeekerOnReadCloser creates a SimpleSeekerOnReadCloser.
func NewSimpleSeekerOnReadCloser(r io.ReadCloser) *SimpleSeekerOnReadCloser {
	return &SimpleSeekerOnReadCloser{r: r}
}

// Read implements io.Reader.
func (s *SimpleSeekerOnReadCloser) Read(p []byte) (n int, err error) {
	n, err = s.r.Read(p)
	s.pos += n
	return
}

// Seek implements io.Seeker.
func (s *SimpleSeekerOnReadCloser) Seek(offset int64, whence int) (int64, error) {
	// only support get reader's current offset
	if offset == 0 && whence == io.SeekCurrent {
		return int64(s.pos), nil
	}
	return 0, errors.Errorf("unsupported seek on SimpleSeekerOnReadCloser, offset: %d whence: %d", offset, whence)
}

// Close implements io.Closer.
func (s *SimpleSeekerOnReadCloser) Close() error {
	return s.r.Close()
}

// loadDataVarKeyType is a dummy type to avoid naming collision in context.
type loadDataVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadDataVarKeyType) String() string {
	return "load_data_var"
}

// LoadDataVarKey is a variable key for load data.
const LoadDataVarKey loadDataVarKeyType = 0
