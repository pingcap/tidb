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
	"math"
	"path/filepath"
	"runtime"
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
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// LoadDataFormatSQLDump represents the data source file of LOAD DATA is
	// mydumper-format DML file
	LoadDataFormatSQLDump = "sql file"
	// LoadDataFormatParquet represents the data source file of LOAD DATA is
	// parquet
	LoadDataFormatParquet = "parquet"

	logicalImportMode   = "logical"  // tidb backend
	physicalImportMode  = "physical" // local backend
	unlimitedWriteSpeed = config.ByteSize(math.MaxInt64)
	minDiskQuota        = config.ByteSize(10 << 30) // 10GiB
	minBatchSize        = config.ByteSize(1 << 10)  // 1KiB
	minWriteSpeed       = config.ByteSize(1 << 10)  // 1KiB/s

	importModeOption    = "import_mode"
	diskQuotaOption     = "disk_quota"
	checksumOption      = "checksum_table"
	addIndexOption      = "add_index"
	analyzeOption       = "analyze_table"
	threadOption        = "thread"
	batchSizeOption     = "batch_size"
	maxWriteSpeedOption = "max_write_speed"
	splitFileOption     = "split_file"
	recordErrorsOption  = "record_errors"
	detachedOption      = "detached"
)

var (
	taskQueueSize = 16 // the maximum number of pending tasks to commit in queue
	// InTest is a flag that bypass gcs authentication in unit tests.
	InTest bool

	// name -> whether the option has value
	supportedOptions = map[string]bool{
		importModeOption:    true,
		diskQuotaOption:     true,
		checksumOption:      true,
		addIndexOption:      true,
		analyzeOption:       true,
		threadOption:        true,
		batchSizeOption:     true,
		maxWriteSpeedOption: true,
		splitFileOption:     true,
		recordErrorsOption:  true,
		detachedOption:      false,
	}

	// options only allowed when import mode is physical
	optionsForPhysicalImport = map[string]struct{}{
		diskQuotaOption: {},
		checksumOption:  {},
		addIndexOption:  {},
		analyzeOption:   {},
	}
)

// LoadDataExec represents a load data executor.
type LoadDataExec struct {
	baseExecutor

	FileLocRef     ast.FileLocRefTp
	OnDuplicate    ast.OnDuplicateKeyHandlingType
	loadDataWorker *LoadDataWorker
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)

	if e.loadDataWorker.Path == "" {
		return ErrLoadDataEmptyPath
	}

	// CSV-like
	if e.loadDataWorker.format == "" {
		if e.loadDataWorker.NullInfo != nil && e.loadDataWorker.NullInfo.OptEnclosed &&
			(e.loadDataWorker.FieldsInfo == nil || e.loadDataWorker.FieldsInfo.Enclosed == nil) {
			return ErrLoadDataWrongFormatConfig.GenWithStackByArgs("must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED")
		}
		// TODO: support lines terminated is "".
		if len(e.loadDataWorker.LinesInfo.Terminated) == 0 {
			return ErrLoadDataWrongFormatConfig.GenWithStackByArgs("LINES TERMINATED BY is empty")
		}
	}

	switch e.FileLocRef {
	case ast.FileLocServerOrRemote:
		u, err := storage.ParseRawURL(e.loadDataWorker.Path)
		if err != nil {
			return ErrLoadDataInvalidURI.GenWithStackByArgs(err.Error())
		}
		var filename string
		u.Path, filename = filepath.Split(u.Path)
		b, err := storage.ParseBackendFromURL(u, nil)
		if err != nil {
			return ErrLoadDataInvalidURI.GenWithStackByArgs(getMsgFromBRError(err))
		}
		if b.GetLocal() != nil {
			return ErrLoadDataFromServerDisk.GenWithStackByArgs(e.loadDataWorker.Path)
		}
		return e.loadFromRemote(ctx, b, filename)
	case ast.FileLocClient:
		// let caller use handleQuerySpecial to read data in this connection
		sctx := e.loadDataWorker.ctx
		val := sctx.Value(LoadDataVarKey)
		if val != nil {
			sctx.SetValue(LoadDataVarKey, nil)
			return errors.New("previous load data option wasn't closed normally")
		}
		sctx.SetValue(LoadDataVarKey, e.loadDataWorker)
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
		return ErrLoadDataCantAccess
	}
	fileReader, err := s.Open(ctx, filename)
	if err != nil {
		return ErrLoadDataCantRead.GenWithStackByArgs(getMsgFromBRError(err), "Please check the INFILE path is correct")
	}
	defer fileReader.Close()

	e.loadDataWorker.loadRemoteInfo = loadRemoteInfo{
		store: s,
		path:  filename,
	}
	return e.loadDataWorker.Load(ctx, fileReader)
}

// Close implements the Executor Close interface.
func (e *LoadDataExec) Close() error {
	if e.runtimeStats != nil && e.loadDataWorker != nil && e.loadDataWorker.stats != nil {
		defer e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.loadDataWorker.stats)
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadDataExec) Open(ctx context.Context) error {
	if e.loadDataWorker.insertColumns != nil {
		e.loadDataWorker.initEvalBuffer()
	}
	// Init for runtime stats.
	e.loadDataWorker.collectRuntimeStatsEnabled()
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

// LoadDataWorker does a LOAD DATA job.
type LoadDataWorker struct {
	*InsertValues

	Path string
	Ctx  sessionctx.Context
	// expose some fields for test
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	NullInfo    *ast.NullDefinedBy
	IgnoreLines uint64

	format             string
	columnAssignments  []*ast.Assignment
	columnsAndUserVars []*ast.ColumnNameOrUserVar
	fieldMappings      []*FieldMapping
	onDuplicate        ast.OnDuplicateKeyHandlingType
	// Data interpretation is restrictive if the SQL mode is restrictive and neither
	// the IGNORE nor the LOCAL modifier is specified. Errors terminate the load
	// operation.
	// ref https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-column-assignments
	restrictive bool

	// import options
	importMode        string
	diskQuota         config.ByteSize
	checksum          config.PostOpLevel
	addIndex          bool
	analyze           config.PostOpLevel
	threadCnt         int64
	batchSize         config.ByteSize
	maxWriteSpeed     config.ByteSize // per second
	splitFile         bool
	maxRecordedErrors int64 // -1 means record all error
	detached          bool

	table           table.Table
	row             []types.Datum
	rows            [][]types.Datum
	commitTaskQueue chan commitTask
	loadRemoteInfo  loadRemoteInfo
}

// NewLoadDataWorker creates a new LoadDataWorker that is ready to work.
func NewLoadDataWorker(
	sctx sessionctx.Context,
	plan *plannercore.LoadData,
	tbl table.Table,
) (*LoadDataWorker, error) {
	insertVal := &InsertValues{
		baseExecutor:   newBaseExecutor(sctx, nil, plan.ID()),
		Table:          tbl,
		Columns:        plan.Columns,
		GenExprs:       plan.GenCols.Exprs,
		isLoadData:     true,
		txnInUse:       sync.Mutex{},
		maxRowsInBatch: uint64(sctx.GetSessionVars().DMLBatchSize),
	}
	restrictive := sctx.GetSessionVars().SQLMode.HasStrictMode() &&
		plan.OnDuplicate != ast.OnDuplicateKeyHandlingIgnore
	loadDataWorker := &LoadDataWorker{
		row:                make([]types.Datum, 0, len(insertVal.insertColumns)),
		commitTaskQueue:    make(chan commitTask, taskQueueSize),
		InsertValues:       insertVal,
		Path:               plan.Path,
		format:             plan.Format,
		table:              tbl,
		FieldsInfo:         plan.FieldsInfo,
		LinesInfo:          plan.LinesInfo,
		NullInfo:           plan.NullInfo,
		IgnoreLines:        plan.IgnoreLines,
		columnAssignments:  plan.ColumnAssignments,
		columnsAndUserVars: plan.ColumnsAndUserVars,
		onDuplicate:        plan.OnDuplicate,
		Ctx:                sctx,
		restrictive:        restrictive,
	}
	if !restrictive {
		// TODO: DupKeyAsWarning represents too many "ignore error" paths, the
		// meaning of this flag is not clear. I can only reuse it here.
		sctx.GetSessionVars().StmtCtx.DupKeyAsWarning = true
	}

	if err := loadDataWorker.initOptions(plan.Options); err != nil {
		return nil, err
	}
	columnNames := loadDataWorker.initFieldMappings()
	if err := loadDataWorker.initLoadColumns(columnNames); err != nil {
		return nil, err
	}
	loadDataWorker.ResetBatch()
	return loadDataWorker, nil
}

func (e *LoadDataWorker) initDefaultOptions() {
	threadCnt := runtime.NumCPU()
	if e.format == LoadDataFormatParquet {
		threadCnt = int(math.Max(1, float64(threadCnt)*0.75))
	}

	e.importMode = logicalImportMode
	_ = e.diskQuota.UnmarshalText([]byte("50GiB")) // todo confirm with pm
	e.checksum = config.OpLevelRequired
	e.addIndex = true
	e.analyze = config.OpLevelOptional
	e.threadCnt = int64(threadCnt)
	_ = e.batchSize.UnmarshalText([]byte("100MiB")) // todo confirm with pm
	e.maxWriteSpeed = unlimitedWriteSpeed
	e.splitFile = false
	e.maxRecordedErrors = 100
	e.detached = false
}

func (e *LoadDataWorker) initOptions(options []*plannercore.LoadDataOpt) error {
	e.initDefaultOptions()

	specifiedOptions := map[string]*plannercore.LoadDataOpt{}
	for _, opt := range options {
		hasValue, ok := supportedOptions[opt.Name]
		if !ok {
			return ErrUnknownOption.FastGenByArgs(opt.Name)
		}
		if hasValue && opt.Value == nil || !hasValue && opt.Value != nil {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if _, ok = specifiedOptions[opt.Name]; ok {
			return ErrDuplicateOption.FastGenByArgs(opt.Name)
		}
		specifiedOptions[opt.Name] = opt
	}

	var (
		v      string
		err    error
		isNull bool
	)
	if opt, ok := specifiedOptions[importModeOption]; ok {
		v, isNull, err = opt.Value.EvalString(e.Ctx, chunk.Row{})
		if err != nil || isNull {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		v = strings.ToLower(v)
		if v != logicalImportMode && v != physicalImportMode {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		e.importMode = v
	}

	if e.importMode == logicalImportMode {
		// some options are only allowed in physical mode
		for _, opt := range specifiedOptions {
			if _, ok := optionsForPhysicalImport[opt.Name]; ok {
				return ErrLoadDataUnsupportedOption.FastGenByArgs(opt.Name, e.importMode)
			}
		}
	}
	if opt, ok := specifiedOptions[diskQuotaOption]; ok {
		v, isNull, err = opt.Value.EvalString(e.Ctx, chunk.Row{})
		if err != nil || isNull {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.diskQuota.UnmarshalText([]byte(v)); err != nil || e.diskQuota <= 0 {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[checksumOption]; ok {
		v, isNull, err = opt.Value.EvalString(e.Ctx, chunk.Row{})
		if err != nil || isNull {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.checksum.FromStringValue(v); err != nil {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[addIndexOption]; ok {
		var vInt int64
		if !mysql.HasIsBooleanFlag(opt.Value.GetType().GetFlag()) {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		vInt, isNull, err = opt.Value.EvalInt(e.Ctx, chunk.Row{})
		if err != nil || isNull {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		e.addIndex = vInt == 1
	}
	if opt, ok := specifiedOptions[analyzeOption]; ok {
		v, isNull, err = opt.Value.EvalString(e.Ctx, chunk.Row{})
		if err != nil || isNull {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.analyze.FromStringValue(v); err != nil {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[threadOption]; ok {
		// boolean true will be taken as 1
		e.threadCnt, isNull, err = opt.Value.EvalInt(e.Ctx, chunk.Row{})
		if err != nil || isNull || e.threadCnt <= 0 {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[batchSizeOption]; ok {
		v, isNull, err = opt.Value.EvalString(e.Ctx, chunk.Row{})
		if err != nil || isNull {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.batchSize.UnmarshalText([]byte(v)); err != nil || e.batchSize <= 0 {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[maxWriteSpeedOption]; ok {
		v, isNull, err = opt.Value.EvalString(e.Ctx, chunk.Row{})
		if err != nil || isNull {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = e.maxWriteSpeed.UnmarshalText([]byte(v)); err != nil || e.maxWriteSpeed <= 0 {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[splitFileOption]; ok {
		if !mysql.HasIsBooleanFlag(opt.Value.GetType().GetFlag()) {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		var vInt int64
		vInt, isNull, err = opt.Value.EvalInt(e.Ctx, chunk.Row{})
		if err != nil || isNull {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		e.splitFile = vInt == 1
	}
	if opt, ok := specifiedOptions[recordErrorsOption]; ok {
		e.maxRecordedErrors, isNull, err = opt.Value.EvalInt(e.Ctx, chunk.Row{})
		if err != nil || isNull || e.maxRecordedErrors < -1 {
			return ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		// todo: set a max value for this param?
	}
	if _, ok := specifiedOptions[detachedOption]; ok {
		e.detached = true
	}

	e.adjustOptions()
	return nil
}

func (e *LoadDataWorker) adjustOptions() {
	if e.diskQuota < minDiskQuota {
		e.diskQuota = minDiskQuota
	}
	// max value is cpu-count
	numCPU := int64(runtime.NumCPU())
	if e.threadCnt > numCPU {
		e.threadCnt = numCPU
	}
	if e.batchSize < minBatchSize {
		e.batchSize = minBatchSize
	}
	if e.maxWriteSpeed < minWriteSpeed {
		e.maxWriteSpeed = minWriteSpeed
	}
}

// FieldMapping indicates the relationship between input field and table column or user variable
type FieldMapping struct {
	Column  *table.Column
	UserVar *ast.VariableExpr
}

// initFieldMappings make a field mapping slice to implicitly map input field to table column or user defined variable
// the slice's order is the same as the order of the input fields.
// Returns a slice of same ordered column names without user defined variable names.
func (e *LoadDataWorker) initFieldMappings() []string {
	columns := make([]string, 0, len(e.columnsAndUserVars)+len(e.columnAssignments))
	tableCols := e.table.Cols()

	if len(e.columnsAndUserVars) == 0 {
		for _, v := range tableCols {
			fieldMapping := &FieldMapping{
				Column: v,
			}
			e.fieldMappings = append(e.fieldMappings, fieldMapping)
			columns = append(columns, v.Name.O)
		}

		return columns
	}

	var column *table.Column

	for _, v := range e.columnsAndUserVars {
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
		e.fieldMappings = append(e.fieldMappings, fieldMapping)
	}

	return columns
}

// initLoadColumns sets columns which the input fields loaded to.
func (e *LoadDataWorker) initLoadColumns(columnNames []string) error {
	var cols []*table.Column
	var missingColName string
	var err error
	tableCols := e.table.Cols()

	if len(columnNames) != len(tableCols) {
		for _, v := range e.columnAssignments {
			columnNames = append(columnNames, v.Column.Name.O)
		}
	}

	cols, missingColName = table.FindCols(tableCols, columnNames, e.table.Meta().PKIsHandle)
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

// reorderColumns reorder the e.insertColumns according to the order of columnNames
// Note: We must ensure there must be one-to-one mapping between e.insertColumns and columnNames in terms of column name.
func (e *LoadDataWorker) reorderColumns(columnNames []string) error {
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

// Load reads from readerFn and do load data job.
func (e *LoadDataWorker) Load(ctx context.Context, reader io.ReadSeekCloser) error {
	var (
		parser mydump.Parser
		err    error
	)

	switch strings.ToLower(e.format) {
	case "":
		// CSV-like
		parser, err = mydump.NewCSVParser(
			ctx,
			e.GenerateCSVConfig(),
			reader,
			int64(config.ReadBlockSize),
			nil,
			false,
			// TODO: support charset conversion
			nil)
	case LoadDataFormatSQLDump:
		parser = mydump.NewChunkParser(
			ctx,
			e.Ctx.GetSessionVars().SQLMode,
			reader,
			int64(config.ReadBlockSize),
			nil,
		)
	case LoadDataFormatParquet:
		if e.loadRemoteInfo.store == nil {
			return ErrLoadParquetFromLocal
		}
		parser, err = mydump.NewParquetParser(
			ctx,
			e.loadRemoteInfo.store,
			reader,
			e.loadRemoteInfo.path,
		)
	default:
		return ErrLoadDataUnsupportedFormat.GenWithStackByArgs(e.format)
	}
	if err != nil {
		return ErrLoadDataWrongFormatConfig.GenWithStack(err.Error())
	}
	parser.SetLogger(log.Logger{Logger: logutil.Logger(ctx)})

	err = sessiontxn.NewTxn(ctx, e.Ctx)
	if err != nil {
		return err
	}
	group, groupCtx := errgroup.WithContext(ctx)

	// processStream process input data, enqueue commit task
	group.Go(func() error {
		return e.processStream(groupCtx, parser)
	})
	group.Go(func() error {
		return e.commitWork(groupCtx)
	})

	err = group.Wait()
	e.SetMessage()
	return err
}

// processStream process input stream from network
func (e *LoadDataWorker) processStream(
	ctx context.Context,
	parser mydump.Parser,
) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("process routine panicked",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			err = errors.Errorf("%v", r)
		}
	}()

	checkKilled := time.NewTicker(30 * time.Second)
	defer checkKilled.Stop()

	for {
		// prepare batch and enqueue task
		if err = e.ReadRows(ctx, parser); err != nil {
			return
		}
		if e.curBatchCnt == 0 {
			close(e.commitTaskQueue)
			return
		}

	TrySendTask:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-checkKilled.C:
			if atomic.CompareAndSwapUint32(&e.Ctx.GetSessionVars().Killed, 1, 0) {
				logutil.Logger(ctx).Info("load data query interrupted quit data processing")
				close(e.commitTaskQueue)
				return ErrQueryInterrupted
			}
			goto TrySendTask
		case e.commitTaskQueue <- commitTask{e.curBatchCnt, e.rows}:
		}
		// reset rows buffer, will reallocate buffer but NOT reuse
		e.ResetBatch()
	}
}

// ResetBatch reset the inner batch.
func (e *LoadDataWorker) ResetBatch() {
	e.rows = make([][]types.Datum, 0, e.maxRowsInBatch)
	e.curBatchCnt = 0
}

// commitWork commit batch sequentially
func (e *LoadDataWorker) commitWork(ctx context.Context) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("commitWork panicked",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			err = errors.Errorf("%v", r)
		}
	}()

	var tasks uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-e.commitTaskQueue:
			if !ok {
				return nil
			}
			start := time.Now()
			if err = e.commitOneTask(ctx, task); err != nil {
				return err
			}
			tasks++
			logutil.Logger(ctx).Info("commit one task success",
				zap.Duration("commit time usage", time.Since(start)),
				zap.Uint64("keys processed", task.cnt),
				zap.Uint64("tasks processed", tasks),
				zap.Int("tasks in queue", len(e.commitTaskQueue)))
		}
	}
}

// commitOneTask insert Data from LoadDataWorker.rows, then make commit and refresh txn
func (e *LoadDataWorker) commitOneTask(ctx context.Context, task commitTask) error {
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

// CheckAndInsertOneBatch is used to commit one transaction batch fulfilled data
func (e *LoadDataWorker) CheckAndInsertOneBatch(ctx context.Context, rows [][]types.Datum, cnt uint64) error {
	if e.stats != nil && e.stats.BasicRuntimeStats != nil {
		// Since this method will not call by executor Next,
		// so we need record the basic executor runtime stats by ourselves.
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

	switch e.onDuplicate {
	case ast.OnDuplicateKeyHandlingReplace:
		return e.batchCheckAndInsert(ctx, rows[0:cnt], e.addRecordLD, true)
	case ast.OnDuplicateKeyHandlingIgnore:
		return e.batchCheckAndInsert(ctx, rows[0:cnt], e.addRecordLD, false)
	case ast.OnDuplicateKeyHandlingError:
		for i, row := range rows[0:cnt] {
			sizeHintStep := int(e.Ctx.GetSessionVars().ShardAllocateStep)
			if sizeHintStep > 0 && i%sizeHintStep == 0 {
				sizeHint := sizeHintStep
				remain := len(rows[0:cnt]) - i
				if sizeHint > remain {
					sizeHint = remain
				}
				err = e.addRecordWithAutoIDHint(ctx, row, sizeHint)
			} else {
				err = e.addRecord(ctx, row)
			}
			if err != nil {
				return err
			}
			e.ctx.GetSessionVars().StmtCtx.AddCopiedRows(1)
		}
		return nil
	default:
		return errors.Errorf("unknown on duplicate key handling: %v", e.onDuplicate)
	}
}

func (e *LoadDataWorker) addRecordLD(ctx context.Context, row []types.Datum) error {
	if row == nil {
		return nil
	}
	err := e.addRecord(ctx, row)
	if err != nil {
		if e.restrictive {
			return err
		}
		e.handleWarning(err)
	}
	return nil
}

// ReadRows reads rows from parser. When parser's reader meet EOF, it will return
// nil. For other errors it will return directly. When the rows batch is full it
// will also return nil.
// The result rows are saved in e.rows and update some members, caller can check
// if curBatchCnt == 0 to know if reached EOF.
func (e *LoadDataWorker) ReadRows(ctx context.Context, parser mydump.Parser) error {
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
			return ErrLoadDataCantRead.GenWithStackByArgs(
				err.Error(),
				"Only the following formats delimited text file (csv, tsv), parquet, sql are supported. Please provide the valid source file(s)",
			)
		}
		// rowCount will be used in fillRow(), last insert ID will be assigned according to the rowCount = 1.
		// So should add first here.
		e.rowCount++
		r, ok, err := e.parserData2TableData(ctx, parser.LastRow().Row)
		if err != nil {
			return err
		}
		if ok {
			e.rows = append(e.rows, r)
			e.curBatchCnt++
		} else {
			e.rowCount--
		}
		if e.maxRowsInBatch != 0 && e.rowCount%e.maxRowsInBatch == 0 {
			logutil.Logger(ctx).Info("batch limit hit when inserting rows", zap.Int("maxBatchRows", e.maxChunkSize),
				zap.Uint64("totalRows", e.rowCount))
			return nil
		}
	}
}

// parserData2TableData encodes the data of parser output.
// If it meets ignorable internal error and can't produce a row, it will return
// (nil, false, nil).
func (e *LoadDataWorker) parserData2TableData(
	ctx context.Context,
	parserData []types.Datum,
) ([]types.Datum, bool, error) {
	var errColNumMismatch error
	switch {
	case len(parserData) < len(e.fieldMappings):
		errColNumMismatch = ErrWarnTooFewRecords.GenWithStackByArgs(e.rowCount)
	case len(parserData) > len(e.fieldMappings):
		errColNumMismatch = ErrWarnTooManyRecords.GenWithStackByArgs(e.rowCount)
	}

	if errColNumMismatch != nil {
		if e.restrictive {
			return nil, false, errColNumMismatch
		}
		e.handleWarning(errColNumMismatch)
	}

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

	for i := 0; i < len(e.fieldMappings); i++ {
		if i >= len(parserData) {
			if e.fieldMappings[i].Column == nil {
				setVar(e.fieldMappings[i].UserVar.Name, nil)
				continue
			}

			// If some columns is missing and their type is time and has not null flag, they should be set as current time.
			if types.IsTypeTime(e.fieldMappings[i].Column.GetType()) && mysql.HasNotNullFlag(e.fieldMappings[i].Column.GetFlag()) {
				row = append(row, types.NewTimeDatum(types.CurrentTime(e.fieldMappings[i].Column.GetType())))
				continue
			}

			row = append(row, types.NewDatum(nil))
			continue
		}

		if e.fieldMappings[i].Column == nil {
			setVar(e.fieldMappings[i].UserVar.Name, &parserData[i])
			continue
		}

		row = append(row, parserData[i])
	}
	for i := 0; i < len(e.columnAssignments); i++ {
		// eval expression of `SET` clause
		d, err := expression.EvalAstExpr(e.Ctx, e.columnAssignments[i].Expr)
		if err != nil {
			if e.restrictive {
				return nil, false, err
			}
			e.handleWarning(err)
			return nil, false, nil
		}
		row = append(row, d)
	}

	// a new row buffer will be allocated in getRow
	newRow, err := e.getRow(ctx, row)
	if err != nil {
		if e.restrictive {
			return nil, false, err
		}
		e.handleWarning(err)
		return nil, false, nil
	}

	return newRow, true, nil
}

// SetMessage sets info message(ERR_LOAD_INFO) generated by LOAD statement, it is public because of the special way that
// LOAD statement is handled.
func (e *LoadDataWorker) SetMessage() {
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	numDeletes := stmtCtx.DeletedRows()
	numSkipped := numRecords - stmtCtx.CopiedRows()
	numWarnings := stmtCtx.WarningCount()
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo].Raw, numRecords, numDeletes, numSkipped, numWarnings)
	e.ctx.GetSessionVars().StmtCtx.SetMessage(msg)
}

// GenerateCSVConfig generates a CSV config for parser from LoadDataWorker.
func (e *LoadDataWorker) GenerateCSVConfig() *config.CSVConfig {
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

// GetRows getter for rows
func (e *LoadDataWorker) GetRows() [][]types.Datum {
	return e.rows
}

// GetCurBatchCnt getter for curBatchCnt
func (e *LoadDataWorker) GetCurBatchCnt() uint64 {
	return e.curBatchCnt
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
