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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/disttask/loaddata"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	taskQueueSize = 16 // the maximum number of pending tasks to commit in queue
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
func (e *LoadDataExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.GrowAndReset(e.maxChunkSize)
	if e.detachHandled {
		// need to return an empty req to indicate all results have been written
		return nil
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalLoadData)

	switch e.FileLocRef {
	case ast.FileLocServerOrRemote:
		jobID, err2 := e.loadDataWorker.loadRemote(ctx)
		if err2 != nil {
			return err2
		}
		if e.loadDataWorker.controller.Detached {
			req.AppendInt64(0, jobID)
			e.detachHandled = true
		}
	case ast.FileLocClient:
		// let caller use handleQuerySpecial to read data in this connection
		sctx := e.loadDataWorker.UserSctx
		val := sctx.Value(LoadDataVarKey)
		if val != nil {
			sctx.SetValue(LoadDataVarKey, nil)
			return errors.New("previous load data option wasn't closed normally")
		}
		sctx.SetValue(LoadDataVarKey, e.loadDataWorker)
	}
	return nil
}

// commitTask is used for passing data from processStream goroutine to commitWork goroutine.
type commitTask struct {
	cnt  uint64
	rows [][]types.Datum

	fileSize int64
}

type planInfo struct {
	ID          int
	Columns     []*ast.ColumnName
	GenColExprs []expression.Expression
}

// LoadDataWorker does a LOAD DATA job.
type LoadDataWorker struct {
	UserSctx sessionctx.Context

	importPlan *importer.Plan
	controller *importer.LoadDataController
	planInfo   planInfo
	// only use in distributed load data
	stmt string

	table    table.Table
	progress *asyncloaddata.Progress
}

func setNonRestrictiveFlags(stmtCtx *stmtctx.StatementContext) {
	// TODO: DupKeyAsWarning represents too many "ignore error" paths, the
	// meaning of this flag is not clear. I can only reuse it here.
	stmtCtx.DupKeyAsWarning = true
	stmtCtx.TruncateAsWarning = true
	stmtCtx.BadNullAsWarning = true
}

// NewLoadDataWorker creates a new LoadDataWorker that is ready to work.
func NewLoadDataWorker(
	userSctx sessionctx.Context,
	plan *plannercore.LoadData,
	tbl table.Table,
) (w *LoadDataWorker, err error) {
	importPlan, err := importer.NewPlan(userSctx, plan, tbl)
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
		importPlan: importPlan,
		stmt:       plan.Stmt,
		controller: controller,
		planInfo: planInfo{
			ID:          plan.ID(),
			Columns:     plan.Columns,
			GenColExprs: plan.GenCols.Exprs,
		},
		progress: asyncloaddata.NewProgress(controller.ImportMode == importer.LogicalImportMode),
	}
	return loadDataWorker, nil
}

func (e *LoadDataWorker) loadRemote(ctx context.Context) (int64, error) {
	if err2 := e.controller.InitDataFiles(ctx); err2 != nil {
		return 0, err2
	}
	return e.load(ctx, nil)
}

// LoadLocal reads from client connection and do load data job.
func (e *LoadDataWorker) LoadLocal(ctx context.Context, r io.ReadCloser) error {
	_, err := e.load(ctx, r)
	return err
}

func (e *LoadDataWorker) load(ctx context.Context, r io.ReadCloser) (jboID int64, err error) {
	s, err2 := CreateSession(e.UserSctx)
	if err2 != nil {
		return 0, err2
	}
	defer func() {
		// if the job is detached and there's no error during init, we will close the session in the detached routine.
		// else we close the session here.
		if !e.controller.Detached || err != nil {
			CloseSession(s)
		}
	}()

	sqlExec := s.(sqlexec.SQLExecutor)
	if err2 = e.controller.CheckRequirements(ctx, sqlExec); err2 != nil {
		return 0, err2
	}

	job, err2 := asyncloaddata.CreateLoadDataJob(
		ctx,
		sqlExec,
		e.GetInfilePath(),
		e.controller.DBName,
		e.table.Meta().Name.O,
		e.controller.ImportMode,
		e.UserSctx.GetSessionVars().User.String(),
	)
	if err2 != nil {
		return 0, err2
	}

	importCtx := ctx
	if e.controller.Detached {
		importCtx = context.Background()
		importCtx = kv.WithInternalSourceType(importCtx, kv.InternalLoadData)
	}

	jobImporter, err2 := e.getJobImporter(importCtx, job, r)
	if err2 != nil {
		return 0, err2
	}

	if e.controller.Detached {
		go func() {
			defer CloseSession(s)
			// error is stored in system table, so we can ignore it here
			//nolint: errcheck
			_ = e.importJob(importCtx, jobImporter)
		}()
		return job.ID, nil
	}
	return job.ID, e.importJob(importCtx, jobImporter)
}

func (e *LoadDataWorker) importJob(ctx context.Context, jobImporter importer.JobImporter) (err error) {
	defer func() {
		_ = jobImporter.Close()
	}()
	if e.controller.FileLocRef == ast.FileLocServerOrRemote {
		e.progress.SourceFileSize = e.controller.TotalFileSize
	}

	param := jobImporter.Param()
	job, group, groupCtx, done := param.Job, param.Group, param.GroupCtx, param.Done

	var result importer.JobImportResult
	defer func() {
		job.OnComplete(err, result.Msg)
	}()

	err = job.StartJob(ctx)
	if err != nil {
		return err
	}

	// UpdateJobProgress goroutine.
	group.Go(func() error {
		// ProgressUpdateRoutineFn must be run in this group, since on job cancel/drop, we depend on it to trigger
		// the cancel of the other routines in this group.
		return job.ProgressUpdateRoutineFn(ctx, done, groupCtx.Done(), e.progress)
	})
	jobImporter.Import()
	err = group.Wait()
	result = jobImporter.Result()
	if !e.controller.Detached {
		e.setResult(result)
	}
	return err
}

func (e *LoadDataWorker) setResult(result importer.JobImportResult) {
	userStmtCtx := e.UserSctx.GetSessionVars().StmtCtx
	userStmtCtx.SetMessage(result.Msg)
	userStmtCtx.SetAffectedRows(result.Affected)
	userStmtCtx.SetWarnings(result.Warnings)
	userStmtCtx.LastInsertID = result.LastInsertID
}

func (e *LoadDataWorker) getJobImporter(ctx context.Context, job *asyncloaddata.Job, r io.ReadCloser) (importer.JobImporter, error) {
	group, groupCtx := errgroup.WithContext(ctx)
	param := &importer.JobImportParam{
		Job:      job,
		Group:    group,
		GroupCtx: groupCtx,
		Done:     make(chan struct{}),
		Progress: e.progress,
	}

	if e.importPlan.Distributed {
		// todo: right now some test cases will fail if we run single-node import using NewDistImporterCurrNode
		// directly, so we use EnableDistTask(false on default) to difference them now.
		if variable.EnableDistTask.Load() {
			return loaddata.NewDistImporter(param, e.importPlan, e.stmt)
		}
		return loaddata.NewDistImporterCurrNode(param, e.importPlan, e.stmt)
	}

	if e.controller.ImportMode == importer.LogicalImportMode {
		return newLogicalJobImporter(param, e, r)
	}
	// todo: replace it with NewDistImporterCurrNode after we fix the test cases.
	return importer.NewTableImporter(param, e.controller)
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

// TestLoad is a helper function for unit test.
func (e *LoadDataWorker) TestLoad(parser mydump.Parser) error {
	jobImporter, err2 := newLogicalJobImporter(nil, e, nil)
	if err2 != nil {
		return err2
	}
	err := ResetContextOfStmt(jobImporter.encodeWorkers[0].ctx, &ast.LoadDataStmt{})
	if err != nil {
		return err
	}
	setNonRestrictiveFlags(jobImporter.encodeWorkers[0].ctx.GetSessionVars().StmtCtx)
	err = ResetContextOfStmt(jobImporter.commitWorkers[0].ctx, &ast.LoadDataStmt{})
	if err != nil {
		return err
	}
	setNonRestrictiveFlags(jobImporter.commitWorkers[0].ctx.GetSessionVars().StmtCtx)

	ctx := context.Background()
	for i := uint64(0); i < jobImporter.controller.IgnoreLines; i++ {
		//nolint: errcheck
		_ = parser.ReadRow()
	}
	err = jobImporter.encodeWorkers[0].readOneBatchRows(ctx, parser)
	if err != nil {
		return err
	}
	err = sessiontxn.NewTxn(ctx, jobImporter.commitWorkers[0].ctx)
	if err != nil {
		return err
	}
	err = jobImporter.commitWorkers[0].checkAndInsertOneBatch(
		ctx,
		jobImporter.encodeWorkers[0].rows,
		jobImporter.encodeWorkers[0].curBatchCnt)
	if err != nil {
		return err
	}
	jobImporter.encodeWorkers[0].resetBatch()
	jobImporter.commitWorkers[0].ctx.StmtCommit(ctx)
	err = jobImporter.commitWorkers[0].ctx.CommitTxn(ctx)
	if err != nil {
		return err
	}
	result := jobImporter.Result()
	e.setResult(result)
	return nil
}

type logicalJobImporter struct {
	*importer.JobImportParam
	// only used on interactive load data
	userSctx   sessionctx.Context
	controller *importer.LoadDataController

	encodeWorkers []*encodeWorker
	commitWorkers []*commitWorker
	readerInfos   []importer.LoadDataReaderInfo
}

var _ importer.JobImporter = &logicalJobImporter{}

func newLogicalJobImporter(param *importer.JobImportParam, e *LoadDataWorker, r io.ReadCloser) (*logicalJobImporter, error) {
	ji := &logicalJobImporter{
		JobImportParam: param,
		userSctx:       e.UserSctx,
		controller:     e.controller,
	}
	compressTp := mydump.ParseCompressionOnFileExtension(e.GetInfilePath())
	compressTp2, err := mydump.ToStorageCompressType(compressTp)
	if err != nil {
		return nil, err
	}
	if err := ji.initEncodeCommitWorkers(e); err != nil {
		return nil, err
	}
	if e.controller.FileLocRef == ast.FileLocClient {
		ji.readerInfos = []importer.LoadDataReaderInfo{{
			Opener: func(_ context.Context) (io.ReadSeekCloser, error) {
				addedSeekReader := NewSimpleSeekerOnReadCloser(r)
				return storage.InterceptDecompressReader(addedSeekReader, compressTp2)
			}}}
	} else {
		ji.readerInfos = e.controller.GetLoadDataReaderInfos()
	}
	return ji, nil
}

func (ji *logicalJobImporter) initEncodeCommitWorkers(e *LoadDataWorker) (err error) {
	var createdSessions []sessionctx.Context
	defer func() {
		if err != nil {
			for _, s := range createdSessions {
				CloseSession(s)
			}
		}
	}()
	encodeWorkers := make([]*encodeWorker, 0, e.controller.ThreadCnt)
	commitWorkers := make([]*commitWorker, 0, e.controller.ThreadCnt)

	// TODO: create total ThreadCnt workers rather than 2*ThreadCnt?
	for i := int64(0); i < e.controller.ThreadCnt; i++ {
		encodeCore, err2 := ji.createInsertValues(e)
		if err2 != nil {
			return err2
		}
		createdSessions = append(createdSessions, encodeCore.ctx)
		commitCore, err2 := ji.createInsertValues(e)
		if err2 != nil {
			return err2
		}
		createdSessions = append(createdSessions, commitCore.ctx)
		colAssignExprs, exprWarnings, err2 := e.controller.CreateColAssignExprs(encodeCore.ctx)
		if err2 != nil {
			return err2
		}
		encode := &encodeWorker{
			InsertValues:   encodeCore,
			controller:     e.controller,
			colAssignExprs: colAssignExprs,
			exprWarnings:   exprWarnings,
			killed:         &e.UserSctx.GetSessionVars().Killed,
		}
		encode.resetBatch()
		encodeWorkers = append(encodeWorkers, encode)
		commit := &commitWorker{
			InsertValues: commitCore,
			controller:   e.controller,
			progress:     e.progress,
		}
		commitWorkers = append(commitWorkers, commit)
	}
	ji.encodeWorkers = encodeWorkers
	ji.commitWorkers = commitWorkers
	return nil
}

// createInsertValues creates InsertValues whose session context is a clone of
// userSctx.
func (ji *logicalJobImporter) createInsertValues(e *LoadDataWorker) (insertVal *InsertValues, err error) {
	sysSession, err2 := CreateSession(e.UserSctx)
	if err2 != nil {
		return nil, err2
	}
	defer func() {
		if err != nil {
			CloseSession(sysSession)
		}
	}()

	err = ResetContextOfStmt(sysSession, &ast.LoadDataStmt{})
	if err != nil {
		return nil, err
	}
	// copy the related variables to the new session
	// I have no confident that all needed variables are copied :(
	fromVars := e.UserSctx.GetSessionVars()
	toVars := sysSession.GetSessionVars()
	toVars.User = fromVars.User
	toVars.CurrentDB = fromVars.CurrentDB
	toVars.SQLMode = fromVars.SQLMode
	if !e.controller.Restrictive {
		setNonRestrictiveFlags(toVars.StmtCtx)
	}
	toVars.StmtCtx.InitSQLDigest(fromVars.StmtCtx.SQLDigest())

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
		baseExecutor:   newBaseExecutor(sysSession, nil, e.planInfo.ID),
		Table:          e.table,
		Columns:        e.planInfo.Columns,
		GenExprs:       e.planInfo.GenColExprs,
		maxRowsInBatch: uint64(e.controller.BatchSize),
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

func (ji *logicalJobImporter) Param() *importer.JobImportParam {
	return ji.JobImportParam
}

// Import implements importer.JobImporter interface.
func (ji *logicalJobImporter) Import() {
	// main goroutine -> readerInfoCh -> processOneStream goroutines
	readerInfoCh := make(chan importer.LoadDataReaderInfo, ji.controller.ThreadCnt)
	// processOneStream goroutines -> commitTaskCh -> commitWork goroutines
	commitTaskCh := make(chan commitTask, taskQueueSize)
	// commitWork goroutines -> done -> UpdateJobProgress goroutine

	param := ji.JobImportParam
	// processOneStream goroutines.
	param.Group.Go(func() error {
		encodeGroup, encodeCtx := errgroup.WithContext(param.GroupCtx)

		for i := range ji.encodeWorkers {
			worker := ji.encodeWorkers[i]
			encodeGroup.Go(func() error {
				err2 := sessiontxn.NewTxn(encodeCtx, worker.ctx)
				if err2 != nil {
					return err2
				}
				return worker.processStream(encodeCtx, readerInfoCh, commitTaskCh)
			})
		}

		err2 := encodeGroup.Wait()
		if err2 == nil {
			close(commitTaskCh)
		}
		return err2
	})
	// commitWork goroutines.
	param.Group.Go(func() error {
		commitGroup, commitCtx := errgroup.WithContext(param.GroupCtx)

		for i := range ji.commitWorkers {
			worker := ji.commitWorkers[i]
			commitGroup.Go(func() error {
				failpoint.Inject("BeforeCommitWork", nil)
				return worker.commitWork(commitCtx, commitTaskCh)
			})
		}

		err2 := commitGroup.Wait()
		if err2 == nil {
			close(param.Done)
		}
		return err2
	})

	for i := range ji.readerInfos {
		select {
		case <-param.GroupCtx.Done():
			return
		case readerInfoCh <- ji.readerInfos[i]:
		}
	}
	close(readerInfoCh)
}

// Result implements the importer.JobImporter interface.
func (ji *logicalJobImporter) Result() importer.JobImportResult {
	var (
		numWarnings uint64
		numAffected uint64
		numRecords  uint64
		numDeletes  uint64
		numSkipped  uint64
	)

	for _, w := range ji.encodeWorkers {
		numWarnings += uint64(w.ctx.GetSessionVars().StmtCtx.WarningCount())
	}
	for _, w := range ji.commitWorkers {
		commitStmtCtx := w.ctx.GetSessionVars().StmtCtx
		numWarnings += uint64(commitStmtCtx.WarningCount())
		numAffected += commitStmtCtx.AffectedRows()
		numRecords += commitStmtCtx.RecordRows()
		numDeletes += commitStmtCtx.DeletedRows()
		numSkipped += commitStmtCtx.RecordRows() - commitStmtCtx.CopiedRows()
	}

	// col assign expr warnings is generated during init, it's static
	// we need to generate it for each row processed.
	colAssignExprWarnings := ji.encodeWorkers[0].exprWarnings
	numWarnings += numRecords * uint64(len(colAssignExprWarnings))

	if numWarnings > math.MaxUint16 {
		numWarnings = math.MaxUint16
	}

	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo].Raw, numRecords, numDeletes, numSkipped, numWarnings)
	warns := make([]stmtctx.SQLWarn, numWarnings)
	n := 0
	for _, w := range ji.encodeWorkers {
		n += copy(warns[n:], w.ctx.GetSessionVars().StmtCtx.GetWarnings())
	}
	for _, w := range ji.commitWorkers {
		n += copy(warns[n:], w.ctx.GetSessionVars().StmtCtx.GetWarnings())
	}
	for i := 0; i < int(numRecords) && n < len(warns); i++ {
		n += copy(warns[n:], colAssignExprWarnings)
	}
	return importer.JobImportResult{
		Msg:          msg,
		LastInsertID: ji.getLastInsertID(),
		Affected:     numAffected,
		Warnings:     warns,
	}
}

func (ji *logicalJobImporter) getLastInsertID() uint64 {
	lastInsertID := uint64(0)
	for _, w := range ji.encodeWorkers {
		if w.lastInsertID != 0 && (lastInsertID == 0 || w.lastInsertID < lastInsertID) {
			lastInsertID = w.lastInsertID
		}
	}
	for _, w := range ji.commitWorkers {
		if w.lastInsertID != 0 && (lastInsertID == 0 || w.lastInsertID < lastInsertID) {
			lastInsertID = w.lastInsertID
		}
	}
	return lastInsertID
}

// Close implements the importer.JobImporter interface.
func (ji *logicalJobImporter) Close() error {
	for _, w := range ji.encodeWorkers {
		w.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(w.id, w.stats)
	}
	for _, w := range ji.commitWorkers {
		w.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(w.id, w.stats)
	}

	for _, w := range ji.encodeWorkers {
		CloseSession(w.ctx)
	}
	for _, w := range ji.commitWorkers {
		CloseSession(w.ctx)
	}
	return nil
}

// encodeWorker is a sub-worker of LoadDataWorker that dedicated to encode data.
type encodeWorker struct {
	*InsertValues
	controller     *importer.LoadDataController
	colAssignExprs []expression.Expression
	// sessionCtx generate warnings when rewrite AST node into expression.
	// we should generate such warnings for each row encoded.
	exprWarnings []stmtctx.SQLWarn
	killed       *uint32
	rows         [][]types.Datum
}

// processStream always trys to build a parser from channel and process it. When
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
				zap.Reflect("r", r),
				zap.Stack("stack"))
			err = errors.Errorf("%v", r)
		}
	}()

	checkKilled := time.NewTicker(30 * time.Second)
	defer checkKilled.Stop()

	var (
		loggedError     = false
		lastScannedSize = int64(0)
	)
	for {
		// prepare batch and enqueue task
		if err = w.readOneBatchRows(ctx, parser); err != nil {
			return
		}
		if w.curBatchCnt == 0 {
			return
		}

	TrySendTask:
		scannedSize, err := parser.ScannedPos()
		if err != nil && !loggedError {
			loggedError = true
			logutil.Logger(ctx).Error(" LOAD DATA failed to read current file offset by seek",
				zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-checkKilled.C:
			if atomic.CompareAndSwapUint32(w.killed, 1, 0) {
				logutil.Logger(ctx).Info("load data query interrupted quit data processing")
				return exeerrors.ErrQueryInterrupted
			}
			goto TrySendTask
		case outCh <- commitTask{
			cnt:      w.curBatchCnt,
			rows:     w.rows,
			fileSize: scannedSize - lastScannedSize,
		}:
		}
		lastScannedSize = scannedSize
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
			logutil.Logger(ctx).Info("batch limit hit when inserting rows", zap.Int("maxBatchRows", w.maxChunkSize),
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
	sessionVars := w.ctx.GetSessionVars()
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
		d, err := w.colAssignExprs[i].Eval(chunk.Row{})
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
	progress   *asyncloaddata.Progress
}

// commitWork commit batch sequentially. When returns nil, it means the job is
// finished.
func (w *commitWorker) commitWork(ctx context.Context, inCh <-chan commitTask) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("commitWork panicked",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			err = errors.Errorf("%v", r)
		}
	}()

	var (
		taskCnt       uint64
		backgroundCtx = context.Background()
	)
	err = sessiontxn.NewTxn(ctx, w.ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			w.ctx.StmtRollback(backgroundCtx, false)
			_ = w.ctx.RefreshTxnCtx(backgroundCtx)
			return ctx.Err()
		case task, ok := <-inCh:
			if !ok {
				return nil
			}
			start := time.Now()
			if err = w.commitOneTask(ctx, task); err != nil {
				return err
			}
			w.progress.LoadedRowCnt.Add(task.cnt)
			w.progress.LoadedFileSize.Add(task.fileSize)
			taskCnt++
			logutil.Logger(ctx).Info("commit one task success",
				zap.Duration("commit time usage", time.Since(start)),
				zap.Uint64("keys processed", task.cnt),
				zap.Uint64("taskCnt processed", taskCnt),
			)
			failpoint.Inject("AfterCommitOneTask", nil)
			failpoint.Inject("SyncAfterCommitOneTask", func() {
				importer.TestSyncCh <- struct{}{}
				<-importer.TestSyncCh
			})
		}
	}
}

// commitOneTask insert Data from LoadDataWorker.rows, then make commit and refresh txn
func (w *commitWorker) commitOneTask(ctx context.Context, task commitTask) error {
	var err error
	defer func() {
		if err != nil {
			w.ctx.StmtRollback(ctx, false)
		}
	}()
	err = w.checkAndInsertOneBatch(ctx, task.rows, task.cnt)
	if err != nil {
		logutil.Logger(ctx).Error("commit error CheckAndInsert", zap.Error(err))
		return err
	}
	failpoint.Inject("commitOneTaskErr", func() error {
		return errors.New("mock commit one task error")
	})
	w.ctx.StmtCommit(ctx)
	// Make sure that there are no retries when committing.
	if err = w.ctx.RefreshTxnCtx(ctx); err != nil {
		logutil.Logger(ctx).Error("commit error refresh", zap.Error(err))
		return err
	}
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
	w.ctx.GetSessionVars().StmtCtx.AddRecordRows(cnt)

	switch w.controller.OnDuplicate {
	case ast.OnDuplicateKeyHandlingReplace:
		return w.batchCheckAndInsert(ctx, rows[0:cnt], w.addRecordLD, true)
	case ast.OnDuplicateKeyHandlingIgnore:
		return w.batchCheckAndInsert(ctx, rows[0:cnt], w.addRecordLD, false)
	case ast.OnDuplicateKeyHandlingError:
		for i, row := range rows[0:cnt] {
			sizeHintStep := int(w.ctx.GetSessionVars().ShardAllocateStep)
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
			w.ctx.GetSessionVars().StmtCtx.AddCopiedRows(1)
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
	err := w.addRecord(ctx, row)
	if err != nil {
		if w.controller.Restrictive {
			return err
		}
		w.handleWarning(err)
	}
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

// loadDataVarKeyType is a dummy type to avoid naming collision in context.
type loadDataVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadDataVarKeyType) String() string {
	return "load_data_var"
}

// LoadDataVarKey is a variable key for load data.
const LoadDataVarKey loadDataVarKeyType = 0

var (
	_ Executor = (*LoadDataActionExec)(nil)
)

// LoadDataActionExec executes LoadDataActionStmt.
type LoadDataActionExec struct {
	baseExecutor

	tp    ast.LoadDataActionTp
	jobID int64
}

// Next implements the Executor Next interface.
func (e *LoadDataActionExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	sqlExec := e.ctx.(sqlexec.SQLExecutor)
	user := e.ctx.GetSessionVars().User.String()
	job := asyncloaddata.NewJob(e.jobID, sqlExec, user)

	switch e.tp {
	case ast.LoadDataCancel:
		return job.CancelJob(ctx)
	case ast.LoadDataDrop:
		return job.DropJob(ctx)
	default:
		return errors.Errorf("not implemented LOAD DATA action %v", e.tp)
	}
}
