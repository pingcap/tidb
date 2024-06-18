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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// LoadDataVarKey is a variable key for load data.
const LoadDataVarKey loadDataVarKeyType = 0

// LoadDataReaderBuilderKey stores the reader channel that reads from the connection.
const LoadDataReaderBuilderKey loadDataVarKeyType = 1

var (
	taskQueueSize = 16 // the maximum number of pending tasks to commit in queue
)

// LoadDataReaderBuilder is a function type that builds a reader from a file path.
type LoadDataReaderBuilder func(filepath string) (
	r io.ReadCloser, err error,
)

// LoadDataExec represents a load data executor.
type LoadDataExec struct {
	exec.BaseExecutor

	FileLocRef     ast.FileLocRefTp
	loadDataWorker *LoadDataWorker

	// fields for loading local file
	infileReader io.ReadCloser
}

// Open implements the Executor interface.
func (e *LoadDataExec) Open(_ context.Context) error {
	if rb, ok := e.Ctx().Value(LoadDataReaderBuilderKey).(LoadDataReaderBuilder); ok {
		var err error
		e.infileReader, err = rb(e.loadDataWorker.GetInfilePath())
		if err != nil {
			return err
		}
	}
	return nil
}

// Close implements the Executor interface.
func (e *LoadDataExec) Close() error {
	return e.closeLocalReader(nil)
}

func (e *LoadDataExec) closeLocalReader(originalErr error) error {
	err := originalErr
	if e.infileReader != nil {
		if err2 := e.infileReader.Close(); err2 != nil {
			logutil.BgLogger().Error(
				"close local reader failed", zap.Error(err2),
				zap.NamedError("original error", originalErr),
			)
			if err == nil {
				err = err2
			}
		}
		e.infileReader = nil
	}
	return err
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	switch e.FileLocRef {
	case ast.FileLocServerOrRemote:
		return e.loadDataWorker.loadRemote(ctx)
	case ast.FileLocClient:
		// This is for legacy test only
		// TODO: adjust tests to remove LoadDataVarKey
		sctx := e.loadDataWorker.UserSctx
		sctx.SetValue(LoadDataVarKey, e.loadDataWorker)

		err = e.loadDataWorker.LoadLocal(ctx, e.infileReader)
		if err != nil {
			logutil.Logger(ctx).Error("load local data failed", zap.Error(err))
			err = e.closeLocalReader(err)
			return err
		}
	}
	return nil
}

type planInfo struct {
	ID          int
	Columns     []*ast.ColumnName
	GenColExprs []expression.Expression
}

// LoadDataWorker does a LOAD DATA job.
type LoadDataWorker struct {
	UserSctx sessionctx.Context

	controller *importer.LoadDataController
	planInfo   planInfo

	table table.Table
}

func setNonRestrictiveFlags(stmtCtx *stmtctx.StatementContext) {
	// TODO: DupKeyAsWarning represents too many "ignore error" paths, the
	// meaning of this flag is not clear. I can only reuse it here.
	levels := stmtCtx.ErrLevels()
	levels[errctx.ErrGroupDupKey] = errctx.LevelWarn
	levels[errctx.ErrGroupBadNull] = errctx.LevelWarn
	stmtCtx.SetErrLevels(levels)
	stmtCtx.SetTypeFlags(stmtCtx.TypeFlags().WithTruncateAsWarning(true))
}

// NewLoadDataWorker creates a new LoadDataWorker that is ready to work.
func NewLoadDataWorker(
	userSctx sessionctx.Context,
	plan *plannercore.LoadData,
	tbl table.Table,
) (w *LoadDataWorker, err error) {
	importPlan, err := importer.NewPlanFromLoadDataPlan(userSctx, plan)
	if err != nil {
		return nil, err
	}
	astArgs := importer.ASTArgsFromPlan(plan)
	controller, err := importer.NewLoadDataController(importPlan, tbl, astArgs)
	if err != nil {
		return nil, err
	}

	if !controller.Restrictive {
		setNonRestrictiveFlags(userSctx.GetSessionVars().StmtCtx)
	}

	loadDataWorker := &LoadDataWorker{
		UserSctx:   userSctx,
		table:      tbl,
		controller: controller,
		planInfo: planInfo{
			ID:          plan.ID(),
			Columns:     plan.Columns,
			GenColExprs: plan.GenCols.Exprs,
		},
	}
	return loadDataWorker, nil
}

func (e *LoadDataWorker) loadRemote(ctx context.Context) error {
	if err2 := e.controller.InitDataFiles(ctx); err2 != nil {
		return err2
	}
	return e.load(ctx, e.controller.GetLoadDataReaderInfos())
}

// LoadLocal reads from client connection and do load data job.
func (e *LoadDataWorker) LoadLocal(ctx context.Context, r io.ReadCloser) error {
	if r == nil {
		return errors.New("load local data, reader is nil")
	}

	compressTp := mydump.ParseCompressionOnFileExtension(e.GetInfilePath())
	compressTp2, err := mydump.ToStorageCompressType(compressTp)
	if err != nil {
		return err
	}
	readers := []importer.LoadDataReaderInfo{{
		Opener: func(_ context.Context) (io.ReadSeekCloser, error) {
			addedSeekReader := NewSimpleSeekerOnReadCloser(r)
			return storage.InterceptDecompressReader(addedSeekReader, compressTp2, storage.DecompressConfig{
				ZStdDecodeConcurrency: 1,
			})
		}}}
	return e.load(ctx, readers)
}

func (e *LoadDataWorker) load(ctx context.Context, readerInfos []importer.LoadDataReaderInfo) error {
	group, groupCtx := errgroup.WithContext(ctx)

	encoder, committer, err := initEncodeCommitWorkers(e)
	if err != nil {
		return err
	}

	// main goroutine -> readerInfoCh -> processOneStream goroutines
	readerInfoCh := make(chan importer.LoadDataReaderInfo, 1)
	// processOneStream goroutines -> commitTaskCh -> commitWork goroutines
	commitTaskCh := make(chan commitTask, taskQueueSize)
	// commitWork goroutines -> done -> UpdateJobProgress goroutine

	// processOneStream goroutines.
	group.Go(func() error {
		err2 := encoder.processStream(groupCtx, readerInfoCh, commitTaskCh)
		if err2 == nil {
			close(commitTaskCh)
		}
		return err2
	})
	// commitWork goroutines.
	group.Go(func() error {
		failpoint.Inject("BeforeCommitWork", nil)
		return committer.commitWork(groupCtx, commitTaskCh)
	})

sendReaderInfoLoop:
	for _, info := range readerInfos {
		select {
		case <-groupCtx.Done():
			break sendReaderInfoLoop
		case readerInfoCh <- info:
		}
	}
	close(readerInfoCh)
	err = group.Wait()
	e.setResult(encoder.exprWarnings)
	return err
}

func (e *LoadDataWorker) setResult(colAssignExprWarnings []contextutil.SQLWarn) {
	stmtCtx := e.UserSctx.GetSessionVars().StmtCtx
	numWarnings := uint64(stmtCtx.WarningCount())
	numRecords := stmtCtx.RecordRows()
	numDeletes := stmtCtx.DeletedRows()
	numSkipped := stmtCtx.RecordRows() - stmtCtx.CopiedRows()

	// col assign expr warnings is generated during init, it's static
	// we need to generate it for each row processed.
	numWarnings += numRecords * uint64(len(colAssignExprWarnings))

	if numWarnings > math.MaxUint16 {
		numWarnings = math.MaxUint16
	}

	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo].Raw, numRecords, numDeletes, numSkipped, numWarnings)
	warns := make([]contextutil.SQLWarn, numWarnings)
	n := copy(warns, stmtCtx.GetWarnings())
	for i := 0; i < int(numRecords) && n < len(warns); i++ {
		n += copy(warns[n:], colAssignExprWarnings)
	}

	stmtCtx.SetMessage(msg)
	stmtCtx.SetWarnings(warns)
}

func initEncodeCommitWorkers(e *LoadDataWorker) (*encodeWorker, *commitWorker, error) {
	insertValues, err2 := createInsertValues(e)
	if err2 != nil {
		return nil, nil, err2
	}
	colAssignExprs, exprWarnings, err2 := e.controller.CreateColAssignExprs(insertValues.Ctx())
	if err2 != nil {
		return nil, nil, err2
	}
	enc := &encodeWorker{
		InsertValues:   insertValues,
		controller:     e.controller,
		colAssignExprs: colAssignExprs,
		exprWarnings:   exprWarnings,
		killer:         &e.UserSctx.GetSessionVars().SQLKiller,
	}
	enc.resetBatch()
	com := &commitWorker{
		InsertValues: insertValues,
		controller:   e.controller,
	}
	return enc, com, nil
}

// createInsertValues creates InsertValues from userSctx.
func createInsertValues(e *LoadDataWorker) (insertVal *InsertValues, err error) {
	insertColumns := e.controller.InsertColumns
	hasExtraHandle := false
	for _, col := range insertColumns {
		if col.Name.L == model.ExtraHandleName.L {
			if !e.UserSctx.GetSessionVars().AllowWriteRowID {
				return nil, errors.Errorf("load data statement for _tidb_rowid are not supported")
			}
			hasExtraHandle = true
			break
		}
	}
	ret := &InsertValues{
		BaseExecutor:   exec.NewBaseExecutor(e.UserSctx, nil, e.planInfo.ID),
		Table:          e.table,
		Columns:        e.planInfo.Columns,
		GenExprs:       e.planInfo.GenColExprs,
		maxRowsInBatch: 1000,
		insertColumns:  insertColumns,
		rowLen:         len(insertColumns),
		hasExtraHandle: hasExtraHandle,
	}
	if len(insertColumns) > 0 {
		ret.initEvalBuffer()
	}
	ret.collectRuntimeStatsEnabled()
	return ret, nil
}

// encodeWorker is a sub-worker of LoadDataWorker that dedicated to encode data.
type encodeWorker struct {
	*InsertValues
	controller     *importer.LoadDataController
	colAssignExprs []expression.Expression
	// sessionCtx generate warnings when rewrite AST node into expression.
	// we should generate such warnings for each row encoded.
	exprWarnings []contextutil.SQLWarn
	killer       *sqlkiller.SQLKiller
	rows         [][]types.Datum
}

// commitTask is used for passing data from processStream goroutine to commitWork goroutine.
type commitTask struct {
	cnt  uint64
	rows [][]types.Datum
}

// processStream always tries to build a parser from channel and process it. When
// it returns nil, it means all data is read.
func (w *encodeWorker) processStream(
	ctx context.Context,
	inCh <-chan importer.LoadDataReaderInfo,
	outCh chan<- commitTask,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case readerInfo, ok := <-inCh:
			if !ok {
				return nil
			}
			dataParser, err := w.controller.GetParser(ctx, readerInfo)
			if err != nil {
				return err
			}
			if err = w.controller.HandleSkipNRows(dataParser); err != nil {
				return err
			}
			err = w.processOneStream(ctx, dataParser, outCh)
			terror.Log(dataParser.Close())
			if err != nil {
				return err
			}
		}
	}
}

// processOneStream process input stream from parser. When returns nil, it means
// all data is read.
func (w *encodeWorker) processOneStream(
	ctx context.Context,
	parser mydump.Parser,
	outCh chan<- commitTask,
) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("process routine panicked",
				zap.Any("r", r),
				zap.Stack("stack"))
			err = util.GetRecoverError(r)
		}
	}()

	checkKilled := time.NewTicker(30 * time.Second)
	defer checkKilled.Stop()

	for {
		// prepare batch and enqueue task
		if err = w.readOneBatchRows(ctx, parser); err != nil {
			return
		}
		if w.curBatchCnt == 0 {
			return
		}

	TrySendTask:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-checkKilled.C:
			if err := w.killer.HandleSignal(); err != nil {
				logutil.Logger(ctx).Info("load data query interrupted quit data processing")
				return err
			}
			goto TrySendTask
		case outCh <- commitTask{
			cnt:  w.curBatchCnt,
			rows: w.rows,
		}:
		}
		// reset rows buffer, will reallocate buffer but NOT reuse
		w.resetBatch()
	}
}

func (w *encodeWorker) resetBatch() {
	w.rows = make([][]types.Datum, 0, w.maxRowsInBatch)
	w.curBatchCnt = 0
}

// readOneBatchRows reads rows from parser. When parser's reader meet EOF, it
// will return nil. For other errors it will return directly. When the rows
// batch is full it will also return nil.
// The result rows are saved in w.rows and update some members, caller can check
// if curBatchCnt == 0 to know if reached EOF.
func (w *encodeWorker) readOneBatchRows(ctx context.Context, parser mydump.Parser) error {
	for {
		if err := parser.ReadRow(); err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(
				err.Error(),
				"Only the following formats delimited text file (csv, tsv), parquet, sql are supported. Please provide the valid source file(s)",
			)
		}
		// rowCount will be used in fillRow(), last insert ID will be assigned according to the rowCount = 1.
		// So should add first here.
		w.rowCount++
		r, err := w.parserData2TableData(ctx, parser.LastRow().Row)
		if err != nil {
			return err
		}
		parser.RecycleRow(parser.LastRow())
		w.rows = append(w.rows, r)
		w.curBatchCnt++
		if w.maxRowsInBatch != 0 && w.rowCount%w.maxRowsInBatch == 0 {
			logutil.Logger(ctx).Info("batch limit hit when inserting rows", zap.Int("maxBatchRows", w.MaxChunkSize()),
				zap.Uint64("totalRows", w.rowCount))
			return nil
		}
	}
}

// parserData2TableData encodes the data of parser output.
func (w *encodeWorker) parserData2TableData(
	ctx context.Context,
	parserData []types.Datum,
) ([]types.Datum, error) {
	var errColNumMismatch error
	switch {
	case len(parserData) < w.controller.GetFieldCount():
		errColNumMismatch = exeerrors.ErrWarnTooFewRecords.GenWithStackByArgs(w.rowCount)
	case len(parserData) > w.controller.GetFieldCount():
		errColNumMismatch = exeerrors.ErrWarnTooManyRecords.GenWithStackByArgs(w.rowCount)
	}

	if errColNumMismatch != nil {
		if w.controller.Restrictive {
			return nil, errColNumMismatch
		}
		w.handleWarning(errColNumMismatch)
	}

	row := make([]types.Datum, 0, len(w.insertColumns))
	sessionVars := w.Ctx().GetSessionVars()
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

	fieldMappings := w.controller.FieldMappings
	for i := 0; i < len(fieldMappings); i++ {
		if i >= len(parserData) {
			if fieldMappings[i].Column == nil {
				setVar(fieldMappings[i].UserVar.Name, nil)
				continue
			}

			// If some columns is missing and their type is time and has not null flag, they should be set as current time.
			if types.IsTypeTime(fieldMappings[i].Column.GetType()) && mysql.HasNotNullFlag(fieldMappings[i].Column.GetFlag()) {
				row = append(row, types.NewTimeDatum(types.CurrentTime(fieldMappings[i].Column.GetType())))
				continue
			}

			row = append(row, types.NewDatum(nil))
			continue
		}

		if fieldMappings[i].Column == nil {
			setVar(fieldMappings[i].UserVar.Name, &parserData[i])
			continue
		}

		// Don't set the value for generated columns.
		if fieldMappings[i].Column.IsGenerated() {
			row = append(row, types.NewDatum(nil))
			continue
		}

		row = append(row, parserData[i])
	}
	for i := 0; i < len(w.colAssignExprs); i++ {
		// eval expression of `SET` clause
		d, err := w.colAssignExprs[i].Eval(w.Ctx().GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil {
			if w.controller.Restrictive {
				return nil, err
			}
			w.handleWarning(err)
		}
		row = append(row, d)
	}

	// a new row buffer will be allocated in getRow
	newRow, err := w.getRow(ctx, row)
	if err != nil {
		if w.controller.Restrictive {
			return nil, err
		}
		w.handleWarning(err)
		logutil.Logger(ctx).Error("failed to get row", zap.Error(err))
		// TODO: should not return nil! caller will panic when lookup index
		return nil, nil
	}

	return newRow, nil
}

// commitWorker is a sub-worker of LoadDataWorker that dedicated to commit data.
type commitWorker struct {
	*InsertValues
	controller *importer.LoadDataController
}

// commitWork commit batch sequentially. When returns nil, it means the job is
// finished.
func (w *commitWorker) commitWork(ctx context.Context, inCh <-chan commitTask) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("commitWork panicked",
				zap.Any("r", r),
				zap.Stack("stack"))
			err = util.GetRecoverError(r)
		}
	}()

	var (
		taskCnt uint64
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-inCh:
			if !ok {
				return nil
			}
			start := time.Now()
			if err = w.commitOneTask(ctx, task); err != nil {
				return err
			}
			taskCnt++
			logutil.Logger(ctx).Info("commit one task success",
				zap.Duration("commit time usage", time.Since(start)),
				zap.Uint64("keys processed", task.cnt),
				zap.Uint64("taskCnt processed", taskCnt),
			)
		}
	}
}

// commitOneTask insert Data from LoadDataWorker.rows, then commit the modification
// like a statement.
func (w *commitWorker) commitOneTask(ctx context.Context, task commitTask) error {
	err := w.checkAndInsertOneBatch(ctx, task.rows, task.cnt)
	if err != nil {
		logutil.Logger(ctx).Error("commit error CheckAndInsert", zap.Error(err))
		return err
	}
	failpoint.Inject("commitOneTaskErr", func() {
		failpoint.Return(errors.New("mock commit one task error"))
	})
	return nil
}

func (w *commitWorker) checkAndInsertOneBatch(ctx context.Context, rows [][]types.Datum, cnt uint64) error {
	if w.stats != nil && w.stats.BasicRuntimeStats != nil {
		// Since this method will not call by executor Next,
		// so we need record the basic executor runtime stats by ourselves.
		start := time.Now()
		defer func() {
			w.stats.BasicRuntimeStats.Record(time.Since(start), 0)
		}()
	}
	var err error
	if cnt == 0 {
		return err
	}
	w.Ctx().GetSessionVars().StmtCtx.AddRecordRows(cnt)

	switch w.controller.OnDuplicate {
	case ast.OnDuplicateKeyHandlingReplace:
		return w.batchCheckAndInsert(ctx, rows[0:cnt], w.addRecordLD, true)
	case ast.OnDuplicateKeyHandlingIgnore:
		return w.batchCheckAndInsert(ctx, rows[0:cnt], w.addRecordLD, false)
	case ast.OnDuplicateKeyHandlingError:
		for i, row := range rows[0:cnt] {
			sizeHintStep := int(w.Ctx().GetSessionVars().ShardAllocateStep)
			if sizeHintStep > 0 && i%sizeHintStep == 0 {
				sizeHint := sizeHintStep
				remain := len(rows[0:cnt]) - i
				if sizeHint > remain {
					sizeHint = remain
				}
				err = w.addRecordWithAutoIDHint(ctx, row, sizeHint)
			} else {
				err = w.addRecord(ctx, row)
			}
			if err != nil {
				return err
			}
			w.Ctx().GetSessionVars().StmtCtx.AddCopiedRows(1)
		}
		return nil
	default:
		return errors.Errorf("unknown on duplicate key handling: %v", w.controller.OnDuplicate)
	}
}

func (w *commitWorker) addRecordLD(ctx context.Context, row []types.Datum) error {
	if row == nil {
		return nil
	}
	return w.addRecord(ctx, row)
}

// GetInfilePath get infile path.
func (e *LoadDataWorker) GetInfilePath() string {
	return e.controller.Path
}

// GetController get load data controller.
// used in unit test.
func (e *LoadDataWorker) GetController() *importer.LoadDataController {
	return e.controller
}

// TestLoadLocal is a helper function for unit test.
func (e *LoadDataWorker) TestLoadLocal(parser mydump.Parser) error {
	if err := ResetContextOfStmt(e.UserSctx, &ast.LoadDataStmt{}); err != nil {
		return err
	}
	setNonRestrictiveFlags(e.UserSctx.GetSessionVars().StmtCtx)
	encoder, committer, err := initEncodeCommitWorkers(e)
	if err != nil {
		return err
	}

	ctx := context.Background()
	err = sessiontxn.NewTxn(ctx, e.UserSctx)
	if err != nil {
		return err
	}

	for i := uint64(0); i < e.controller.IgnoreLines; i++ {
		//nolint: errcheck
		_ = parser.ReadRow()
	}

	err = encoder.readOneBatchRows(ctx, parser)
	if err != nil {
		return err
	}

	err = committer.checkAndInsertOneBatch(
		ctx,
		encoder.rows,
		encoder.curBatchCnt)
	if err != nil {
		return err
	}
	encoder.resetBatch()
	committer.Ctx().StmtCommit(ctx)
	err = committer.Ctx().CommitTxn(ctx)
	if err != nil {
		return err
	}
	e.setResult(encoder.exprWarnings)
	return nil
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

// GetFileSize implements storage.ExternalFileReader.
func (*SimpleSeekerOnReadCloser) GetFileSize() (int64, error) {
	return 0, errors.Errorf("unsupported GetFileSize on SimpleSeekerOnReadCloser")
}

// loadDataVarKeyType is a dummy type to avoid naming collision in context.
type loadDataVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (loadDataVarKeyType) String() string {
	return "load_data_var"
}
