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
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
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
)

var (
	detachedOption = plannercore.DetachedOption
	taskQueueSize  = 16 // the maximum number of pending tasks to commit in queue

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
	detachHandled  bool
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.detachHandled {
		return nil
	}
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
		path := strings.Trim(u.Path, "/")
		u.Path = ""
		b, err := storage.ParseBackendFromURL(u, nil)
		if err != nil {
			return ErrLoadDataInvalidURI.GenWithStackByArgs(getMsgFromBRError(err))
		}
		if b.GetLocal() != nil {
			return ErrLoadDataFromServerDisk.GenWithStackByArgs(e.loadDataWorker.Path)
		}
		// try to find pattern error in advance
		_, err = filepath.Match(stringutil.EscapeGlobExceptAsterisk(path), "")
		if err != nil {
			return ErrLoadDataInvalidURI.GenWithStackByArgs("Glob pattern error: " + err.Error())
		}
		jobID, err := e.loadFromRemote(ctx, b, path)
		if err != nil {
			return err
		}
		if e.loadDataWorker.detached {
			req.AppendInt64(0, jobID)
			e.detachHandled = true
		}
	case ast.FileLocClient:
		if e.loadDataWorker.detached {
			return errors.New("LOCAL can't use WITH DETACHED")
		}

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
	path string,
) (int64, error) {
	opt := &storage.ExternalStorageOptions{}
	if intest.InTest {
		opt.NoCredentials = true
	}
	s, err := storage.New(ctx, b, opt)
	if err != nil {
		return 0, ErrLoadDataCantAccess
	}

	idx := strings.IndexByte(path, '*')
	// simple path when the INFILE represent one file
	if idx == -1 {
		opener := func(ctx context.Context) (io.ReadSeekCloser, error) {
			fileReader, err2 := s.Open(ctx, path)
			if err2 != nil {
				return nil, ErrLoadDataCantRead.GenWithStackByArgs(getMsgFromBRError(err2), "Please check the INFILE path is correct")
			}
			return fileReader, nil
		}

		// try to read the file size to report progress. Don't fail the main load
		// if this fails to tolerate transient errors.
		filesize := int64(-1)
		reader, err2 := opener(ctx)
		if err2 == nil {
			size, err3 := reader.Seek(0, io.SeekEnd)
			if err3 != nil {
				logutil.Logger(ctx).Warn("failed to read file size by seek in LOAD DATA",
					zap.Error(err3))
			} else {
				filesize = size
			}
			terror.Log(reader.Close())
		}

		return e.loadDataWorker.Load(ctx, []LoadDataReaderInfo{{
			Opener: opener,
			Remote: &loadRemoteInfo{store: s, path: path, size: filesize},
		}})
	}

	// when the INFILE represent multiple files
	readerInfos := make([]LoadDataReaderInfo, 0, 8)
	commonPrefix := path[:idx]
	// we only support '*', in order to reuse glob library manually escape the path
	escapedPath := stringutil.EscapeGlobExceptAsterisk(path)

	err = s.WalkDir(ctx, &storage.WalkOption{ObjPrefix: commonPrefix},
		func(remotePath string, size int64) error {
			// we have checked in LoadDataExec.Next
			//nolint: errcheck
			match, _ := filepath.Match(escapedPath, remotePath)
			if !match {
				return nil
			}
			readerInfos = append(readerInfos, LoadDataReaderInfo{
				Opener: func(ctx context.Context) (io.ReadSeekCloser, error) {
					fileReader, err2 := s.Open(ctx, remotePath)
					if err2 != nil {
						return nil, ErrLoadDataCantRead.GenWithStackByArgs(getMsgFromBRError(err2), "Please check the INFILE path is correct")
					}
					return fileReader, nil
				},
				Remote: &loadRemoteInfo{
					store: s,
					path:  remotePath,
					size:  size,
				},
			})
			return nil
		})
	if err != nil {
		return 0, err
	}

	return e.loadDataWorker.Load(ctx, readerInfos)
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

// commitTask is used for passing data from processStream goroutine to commitWork goroutine.
type commitTask struct {
	cnt  uint64
	rows [][]types.Datum

	loadedRowCnt    uint64
	scannedFileSize int64
}

type loadRemoteInfo struct {
	store storage.ExternalStorage
	path  string
	size  int64
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
	schemaName         string
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
	finishedSize    int64
	progress        atomic.Pointer[asyncloaddata.Progress]
	getSysSessionFn func() (sessionctx.Context, error)
	putSysSessionFn func(context.Context, sessionctx.Context)
}

// NewLoadDataWorker creates a new LoadDataWorker that is ready to work.
func NewLoadDataWorker(
	sctx sessionctx.Context,
	plan *plannercore.LoadData,
	tbl table.Table,
	getSysSessionFn func() (sessionctx.Context, error),
	putSysSessionFn func(context.Context, sessionctx.Context),
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
		schemaName:         plan.Table.Schema.O,
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
		getSysSessionFn:    getSysSessionFn,
		putSysSessionFn:    putSysSessionFn,
	}
	if !restrictive {
		// TODO: DupKeyAsWarning represents too many "ignore error" paths, the
		// meaning of this flag is not clear. I can only reuse it here.
		sctx.GetSessionVars().StmtCtx.DupKeyAsWarning = true
		sctx.GetSessionVars().StmtCtx.TruncateAsWarning = true
		sctx.GetSessionVars().StmtCtx.BadNullAsWarning = true
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

// LoadDataReadBlockSize is exposed for test.
var LoadDataReadBlockSize = int64(config.ReadBlockSize)

// LoadDataReaderInfo provides information for a data reader of LOAD DATA.
type LoadDataReaderInfo struct {
	// Opener can be called at needed to get a io.ReadSeekCloser. It will only
	// be called once.
	Opener func(ctx context.Context) (io.ReadSeekCloser, error)
	// Remote is not nil only if load from cloud storage.
	Remote *loadRemoteInfo
}

// Load reads from readerInfos and do load data job.
func (e *LoadDataWorker) Load(
	ctx context.Context,
	readerInfos []LoadDataReaderInfo,
) (int64, error) {
	var (
		jobID int64
		err   error
	)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalLoadData)

	s, err := e.getSysSessionFn()
	if err != nil {
		return 0, err
	}
	defer e.putSysSessionFn(ctx, s)

	sqlExec := s.(sqlexec.SQLExecutor)
	// TODO: move it to bootstrap
	_, err = sqlExec.ExecuteInternal(ctx, asyncloaddata.CreateLoadDataJobs)
	if err != nil {
		return 0, err
	}

	jobID, err = asyncloaddata.CreateLoadDataJob(
		ctx, sqlExec, e.Path, e.schemaName, e.table.Meta().Name.O,
		"logical", e.Ctx.GetSessionVars().User.String())
	if err != nil {
		return 0, err
	}

	if e.detached {
		// copy the related variables to the new session
		// I have no confident that all needed variables are copied
		from := e.Ctx.GetSessionVars()
		to := s.GetSessionVars()
		to.StmtCtx.InLoadDataStmt = from.StmtCtx.InLoadDataStmt
		to.StmtCtx.IgnoreNoPartition = from.StmtCtx.IgnoreNoPartition
		to.StmtCtx.DupKeyAsWarning = from.StmtCtx.DupKeyAsWarning
		to.StmtCtx.TruncateAsWarning = from.StmtCtx.TruncateAsWarning
		to.StmtCtx.BadNullAsWarning = from.StmtCtx.BadNullAsWarning
		to.SQLMode = from.SQLMode
		to.User = from.User

		go func() {
			// error is stored in system table
			//nolint: errcheck
			_ = e.doLoad(context.Background(), readerInfos, jobID)
		}()
		return jobID, nil
	}
	return jobID, e.doLoad(ctx, readerInfos, jobID)
}

func (e *LoadDataWorker) doLoad(
	ctx context.Context,
	readerInfos []LoadDataReaderInfo,
	jobID int64,
) (err error) {
	s, err := e.getSysSessionFn()
	if err != nil {
		return err
	}
	defer e.putSysSessionFn(ctx, s)

	sqlExec := s.(sqlexec.SQLExecutor)

	// TODO: use separated context
	defer func() {
		if err == nil {
			err2 := asyncloaddata.FinishJob(
				ctx,
				sqlExec,
				jobID,
				e.Ctx.GetSessionVars().StmtCtx.GetMessage())
			terror.Log(err2)
			return
		}
		errMsg := err.Error()
		if errImpl, ok := err.(*errors.Error); ok {
			errMsg = terror.ToSQLError(errImpl).Error()
		}

		err2 := asyncloaddata.FailJob(ctx, sqlExec, jobID, errMsg)
		terror.Log(err2)
	}()

	failpoint.Inject("AfterCreateLoadDataJob", nil)

	progress := asyncloaddata.Progress{
		SourceFileSize: -1,
		LoadedFileSize: 0,
		LoadedRowCnt:   0,
	}

	totalFilesize := int64(0)
	hasErr := false
	for _, readerInfo := range readerInfos {
		if readerInfo.Remote == nil {
			logutil.Logger(ctx).Warn("can not get total file size when LOAD DATA from local file")
			hasErr = true
			break
		}
		totalFilesize += readerInfo.Remote.size
	}
	if !hasErr {
		progress.SourceFileSize = totalFilesize
	}
	e.progress.Store(&progress)

	err = asyncloaddata.StartJob(ctx, sqlExec, jobID)
	if err != nil {
		return err
	}

	failpoint.Inject("AfterStartJob", nil)

	group, groupCtx := errgroup.WithContext(ctx)
	// done is used to let commitWork goroutine notify UpdateJobProgress
	// goroutine that the job is finished.
	done := make(chan struct{})

	// processStream goroutine.
	var parser mydump.Parser
	group.Go(func() error {
		for _, info := range readerInfos {
			reader, err2 := info.Opener(ctx)
			if err2 != nil {
				return err2
			}

			parser, err2 = e.buildParser(ctx, reader, info.Remote)
			if err2 != nil {
				terror.Log(reader.Close())
				return err2
			}
			err2 = e.processStream(groupCtx, parser, reader)
			terror.Log(reader.Close())
			if err2 != nil {
				return err2
			}
		}

		close(e.commitTaskQueue)
		return nil
	})
	// commitWork goroutine.
	group.Go(func() error {
		err2 := e.commitWork(groupCtx)
		if err2 == nil {
			close(done)
		}
		return err2
	})
	// UpdateJobProgress goroutine.
	group.Go(func() error {
		ticker := time.NewTicker(time.Duration(asyncloaddata.HeartBeatInSec) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				// try to update progress to set 100% progress
				p := e.progress.Load()
				ok, err2 := asyncloaddata.UpdateJobProgress(ctx, sqlExec, jobID, p.String())
				if !ok || err2 != nil {
					logutil.Logger(ctx).Warn("failed to update job progress when finished",
						zap.Bool("ok", ok), zap.Error(err2))
				}
				return nil
			case <-groupCtx.Done():
				return nil
			case <-ticker.C:
				p := e.progress.Load()
				ok, err2 := asyncloaddata.UpdateJobProgress(ctx, sqlExec, jobID, p.String())
				if err2 != nil {
					return err2
				}
				if !ok {
					return errors.Errorf("failed to keepalive for LOAD DATA job %d", jobID)
				}
			}
		}
	})

	err = group.Wait()
	e.SetMessage()
	return err
}

func (e *LoadDataWorker) buildParser(
	ctx context.Context,
	reader io.ReadSeekCloser,
	remote *loadRemoteInfo,
) (parser mydump.Parser, err error) {
	switch strings.ToLower(e.format) {
	case "":
		// CSV-like
		parser, err = mydump.NewCSVParser(
			ctx,
			e.GenerateCSVConfig(),
			reader,
			LoadDataReadBlockSize,
			nil,
			false,
			// TODO: support charset conversion
			nil)
	case LoadDataFormatSQLDump:
		parser = mydump.NewChunkParser(
			ctx,
			e.Ctx.GetSessionVars().SQLMode,
			reader,
			LoadDataReadBlockSize,
			nil,
		)
	case LoadDataFormatParquet:
		if remote == nil {
			return nil, ErrLoadParquetFromLocal
		}
		parser, err = mydump.NewParquetParser(
			ctx,
			remote.store,
			reader,
			remote.path,
		)
	default:
		return nil, ErrLoadDataUnsupportedFormat.GenWithStackByArgs(e.format)
	}
	if err != nil {
		return nil, ErrLoadDataWrongFormatConfig.GenWithStack(err.Error())
	}
	parser.SetLogger(log.Logger{Logger: logutil.Logger(ctx)})

	// handle IGNORE N LINES
	ignoreOneLineFn := parser.ReadRow
	if csvParser, ok := parser.(*mydump.CSVParser); ok {
		ignoreOneLineFn = func() error {
			_, _, err := csvParser.ReadUntilTerminator()
			return err
		}
	}

	ignoreLineCnt := e.IgnoreLines
	for ignoreLineCnt > 0 {
		err = ignoreOneLineFn()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return parser, nil
			}
			return nil, err
		}

		ignoreLineCnt--
	}
	return parser, nil
}

// processStream process input stream from parser. When returns nil, it means
// all data is read.
func (e *LoadDataWorker) processStream(
	ctx context.Context,
	parser mydump.Parser,
	seeker io.Seeker,
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

	var (
		loggedError     = false
		currScannedSize = int64(0)
	)
	for {
		// prepare batch and enqueue task
		if err = e.ReadOneBatchRows(ctx, parser); err != nil {
			return
		}
		if e.curBatchCnt == 0 {
			e.finishedSize += currScannedSize
			return
		}

	TrySendTask:
		currScannedSize, err = seeker.Seek(0, io.SeekCurrent)
		if err != nil && !loggedError {
			loggedError = true
			logutil.Logger(ctx).Error(" LOAD DATA failed to read current file offset by seek",
				zap.Error(err))
		}
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
		case e.commitTaskQueue <- commitTask{
			cnt:             e.curBatchCnt,
			rows:            e.rows,
			loadedRowCnt:    e.rowCount,
			scannedFileSize: e.finishedSize + currScannedSize,
		}:
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

// commitWork commit batch sequentially. When returns nil, it means the job is
// finished.
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

	err = sessiontxn.NewTxn(ctx, e.Ctx)
	if err != nil {
		return err
	}

	var (
		tasks               uint64
		lastScannedFileSize int64
		currScannedFileSize int64
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-e.commitTaskQueue:
			if !ok {
				p := e.progress.Load()
				newP := &asyncloaddata.Progress{
					SourceFileSize: p.SourceFileSize,
					LoadedFileSize: currScannedFileSize,
					LoadedRowCnt:   p.LoadedRowCnt,
				}
				e.progress.Store(newP)
				return nil
			}
			start := time.Now()
			if err = e.commitOneTask(ctx, task); err != nil {
				return err
			}
			// we want to report "loaded" progress, not "scanned" progress, the
			// difference of these two is the latest block we read is still in
			// processing. So when "scanned" progress get forward we know the
			// last block is processed.
			// The corner case is when a record is larger than the block size,
			// but that should be rare.
			if task.scannedFileSize != currScannedFileSize {
				lastScannedFileSize = currScannedFileSize
				currScannedFileSize = task.scannedFileSize
			}
			p := e.progress.Load()
			newP := &asyncloaddata.Progress{
				SourceFileSize: p.SourceFileSize,
				LoadedFileSize: lastScannedFileSize,
				LoadedRowCnt:   task.loadedRowCnt,
			}
			e.progress.Store(newP)
			tasks++
			logutil.Logger(ctx).Info("commit one task success",
				zap.Duration("commit time usage", time.Since(start)),
				zap.Uint64("keys processed", task.cnt),
				zap.Uint64("tasks processed", tasks),
				zap.Int("tasks in queue", len(e.commitTaskQueue)))
			failpoint.Inject("AfterCommitOneTask", nil)
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
	return nil
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

// ReadOneBatchRows reads rows from parser. When parser's reader meet EOF, it
// will return nil. For other errors it will return directly. When the rows
// batch is full it will also return nil.
// The result rows are saved in e.rows and update some members, caller can check
// if curBatchCnt == 0 to know if reached EOF.
func (e *LoadDataWorker) ReadOneBatchRows(ctx context.Context, parser mydump.Parser) error {
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
		r, err := e.parserData2TableData(ctx, parser.LastRow().Row)
		if err != nil {
			return err
		}
		e.rows = append(e.rows, r)
		e.curBatchCnt++
		if e.maxRowsInBatch != 0 && e.rowCount%e.maxRowsInBatch == 0 {
			logutil.Logger(ctx).Info("batch limit hit when inserting rows", zap.Int("maxBatchRows", e.maxChunkSize),
				zap.Uint64("totalRows", e.rowCount))
			return nil
		}
	}
}

// parserData2TableData encodes the data of parser output.
func (e *LoadDataWorker) parserData2TableData(
	ctx context.Context,
	parserData []types.Datum,
) ([]types.Datum, error) {
	var errColNumMismatch error
	switch {
	case len(parserData) < len(e.fieldMappings):
		errColNumMismatch = ErrWarnTooFewRecords.GenWithStackByArgs(e.rowCount)
	case len(parserData) > len(e.fieldMappings):
		errColNumMismatch = ErrWarnTooManyRecords.GenWithStackByArgs(e.rowCount)
	}

	if errColNumMismatch != nil {
		if e.restrictive {
			return nil, errColNumMismatch
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
				return nil, err
			}
			e.handleWarning(err)
		}
		row = append(row, d)
	}

	// a new row buffer will be allocated in getRow
	newRow, err := e.getRow(ctx, row)
	if err != nil {
		if e.restrictive {
			return nil, err
		}
		e.handleWarning(err)
		// TODO: should not return nil! caller will panic when lookup index
		return nil, nil
	}

	return newRow, nil
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
