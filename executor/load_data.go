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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
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
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
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

func buildCloseSessionOnErr(
	err *error,
	sctx sessionctx.Context,
	putBackFn func(context.Context, sessionctx.Context),
) func() {
	return func() {
		if err != nil && *err != nil {
			ctx := context.Background()
			ctx = kv.WithInternalSourceType(ctx, kv.InternalLoadData)
			putBackFn(ctx, sctx)
		}
	}
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	// in happy path the detached worker will release the system session in
	// e.loadDataWorker.doLoad, and it must have no error after e.loadDataWorker.doLoad.
	// Here we handle the case that error occurred before e.loadDataWorker.doLoad
	defer buildCloseSessionOnErr(&err, e.loadDataWorker.encodeWorker.ctx, e.loadDataWorker.putSysSessionFn)()
	defer buildCloseSessionOnErr(&err, e.loadDataWorker.commitWorker.ctx, e.loadDataWorker.putSysSessionFn)()

	req.GrowAndReset(e.maxChunkSize)
	if e.detachHandled {
		// need to return an empty req to indicate all results have been written
		return nil
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalLoadData)

	switch e.FileLocRef {
	case ast.FileLocServerOrRemote:
		u, err2 := storage.ParseRawURL(e.loadDataWorker.GetInfilePath())
		if err2 != nil {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(err2.Error())
		}
		path := strings.Trim(u.Path, "/")
		u.Path = ""
		b, err2 := storage.ParseBackendFromURL(u, nil)
		if err2 != nil {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(getMsgFromBRError(err2))
		}
		if b.GetLocal() != nil {
			return exeerrors.ErrLoadDataFromServerDisk.GenWithStackByArgs(e.loadDataWorker.GetInfilePath())
		}
		// try to find pattern error in advance
		_, err2 = filepath.Match(stringutil.EscapeGlobExceptAsterisk(path), "")
		if err2 != nil {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs("Glob pattern error: " + err2.Error())
		}
		jobID, err2 := e.loadFromRemote(ctx, b, path)
		if err2 != nil {
			return err2
		}
		if e.loadDataWorker.controller.Detached {
			req.AppendInt64(0, jobID)
			e.detachHandled = true
		}
	case ast.FileLocClient:
		if e.loadDataWorker.controller.Detached {
			return exeerrors.ErrLoadDataCantDetachWithLocal
		}

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
		return 0, exeerrors.ErrLoadDataCantAccess.GenWithStackByArgs(getMsgFromBRError(err))
	}

	idx := strings.IndexByte(path, '*')
	// simple path when the INFILE represent one file
	if idx == -1 {
		opener := func(ctx context.Context) (io.ReadSeekCloser, error) {
			fileReader, err2 := s.Open(ctx, path)
			if err2 != nil {
				return nil, exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(getMsgFromBRError(err2), "Please check the INFILE path is correct")
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
						return nil, exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(getMsgFromBRError(err2), "Please check the INFILE path is correct")
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

// commitTask is used for passing data from processStream goroutine to commitWork goroutine.
type commitTask struct {
	cnt  uint64
	rows [][]types.Datum

	fileSize int64
}

type loadRemoteInfo struct {
	store storage.ExternalStorage
	path  string
	size  int64
}

// LoadDataWorker does a LOAD DATA job.
type LoadDataWorker struct {
	UserSctx sessionctx.Context

	// TODO: they should be []T to support concurrent load data.
	encodeWorker *encodeWorker
	commitWorker *commitWorker

	controller *importer.LoadDataController

	table           table.Table
	progress        *asyncloaddata.Progress
	getSysSessionFn func() (sessionctx.Context, error)
	putSysSessionFn func(context.Context, sessionctx.Context)
}

// NewLoadDataWorker creates a new LoadDataWorker that is ready to work.
func NewLoadDataWorker(
	userSctx sessionctx.Context,
	plan *plannercore.LoadData,
	tbl table.Table,
	getSysSessionFn func() (sessionctx.Context, error),
	putSysSessionFn func(context.Context, sessionctx.Context),
) (w *LoadDataWorker, err error) {
	controller, err := importer.NewLoadDataController(userSctx, plan, tbl)
	if err != nil {
		return nil, err
	}

	if !controller.Restrictive {
		// TODO: DupKeyAsWarning represents too many "ignore error" paths, the
		// meaning of this flag is not clear. I can only reuse it here.
		userSctx.GetSessionVars().StmtCtx.DupKeyAsWarning = true
		userSctx.GetSessionVars().StmtCtx.TruncateAsWarning = true
		userSctx.GetSessionVars().StmtCtx.BadNullAsWarning = true
	}

	// TODO: create N InsertValues where N is threadCnt
	encodeCore, err2 := createInsertValues(userSctx, plan, tbl, controller, getSysSessionFn, putSysSessionFn)
	if err2 != nil {
		return nil, err2
	}
	defer buildCloseSessionOnErr(&err, encodeCore.ctx, putSysSessionFn)()
	commitCore, err2 := createInsertValues(userSctx, plan, tbl, controller, getSysSessionFn, putSysSessionFn)
	if err2 != nil {
		return nil, err2
	}
	defer buildCloseSessionOnErr(&err, commitCore.ctx, putSysSessionFn)()

	encode := &encodeWorker{
		InsertValues: encodeCore,
		controller:   controller,
		killed:       &userSctx.GetSessionVars().Killed,
	}
	encode.resetBatch()
	progress := &asyncloaddata.Progress{}
	commit := &commitWorker{
		InsertValues: commitCore,
		controller:   controller,
		progress:     progress,
	}
	loadDataWorker := &LoadDataWorker{
		UserSctx:        userSctx,
		encodeWorker:    encode,
		commitWorker:    commit,
		table:           tbl,
		controller:      controller,
		progress:        progress,
		getSysSessionFn: getSysSessionFn,
		putSysSessionFn: putSysSessionFn,
	}
	return loadDataWorker, nil
}

// createInsertValues creates InsertValues whose session context is a clone of
// userSctx.
func createInsertValues(
	userSctx sessionctx.Context,
	plan *plannercore.LoadData,
	tbl table.Table,
	controller *importer.LoadDataController,
	getSysSessionFn func() (sessionctx.Context, error),
	putSysSessionFn func(context.Context, sessionctx.Context),
) (insertVal *InsertValues, err error) {
	sysSession, err2 := getSysSessionFn()
	if err2 != nil {
		return nil, err2
	}
	defer buildCloseSessionOnErr(&err, sysSession, putSysSessionFn)()

	err = ResetContextOfStmt(sysSession, &ast.LoadDataStmt{})
	if err != nil {
		return nil, err
	}
	// copy the related variables to the new session
	// I have no confident that all needed variables are copied :(
	fromVars := userSctx.GetSessionVars()
	toVars := sysSession.GetSessionVars()
	toVars.User = fromVars.User
	toVars.CurrentDB = fromVars.CurrentDB
	toVars.SQLMode = fromVars.SQLMode

	insertColumns := controller.GetInsertColumns()
	hasExtraHandle := false
	for _, col := range insertColumns {
		if col.Name.L == model.ExtraHandleName.L {
			if !userSctx.GetSessionVars().AllowWriteRowID {
				return nil, errors.Errorf("load data statement for _tidb_rowid are not supported")
			}
			hasExtraHandle = true
			break
		}
	}
	ret := &InsertValues{
		baseExecutor:   newBaseExecutor(sysSession, nil, plan.ID()),
		Table:          tbl,
		Columns:        plan.Columns,
		GenExprs:       plan.GenCols.Exprs,
		maxRowsInBatch: uint64(controller.GetBatchSize()),
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

	s, err := e.getSysSessionFn()
	if err != nil {
		return 0, err
	}
	defer e.putSysSessionFn(ctx, s)

	sqlExec := s.(sqlexec.SQLExecutor)

	jobID, err = asyncloaddata.CreateLoadDataJob(
		ctx,
		sqlExec,
		e.GetInfilePath(),
		e.controller.SchemaName,
		e.table.Meta().Name.O,
		importer.LogicalImportMode,
		e.UserSctx.GetSessionVars().User.String(),
	)
	if err != nil {
		return 0, err
	}

	if e.controller.Detached {
		go func() {
			detachedCtx := context.Background()
			detachedCtx = kv.WithInternalSourceType(detachedCtx, kv.InternalLoadData)
			// error is stored in system table, so we can ignore it here
			//nolint: errcheck
			_ = e.doLoad(detachedCtx, readerInfos, jobID)
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
	defer func() {
		e.encodeWorker.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.encodeWorker.id, e.encodeWorker.stats)
		e.commitWorker.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.commitWorker.id, e.commitWorker.stats)

		ctx2 := context.Background()
		ctx2 = kv.WithInternalSourceType(ctx2, kv.InternalLoadData)
		e.putSysSessionFn(ctx2, e.encodeWorker.ctx)
		e.putSysSessionFn(ctx2, e.commitWorker.ctx)
	}()

	// get a session for UpdateJobProgress.
	s, err := e.getSysSessionFn()
	if err != nil {
		return err
	}
	defer e.putSysSessionFn(ctx, s)

	sqlExec := s.(sqlexec.SQLExecutor)

	var msg string
	defer func() {
		// write the ending status even if user context is canceled.
		ctx2 := context.Background()
		ctx2 = kv.WithInternalSourceType(ctx2, kv.InternalLoadData)
		if err == nil {
			err2 := asyncloaddata.FinishJob(
				ctx2,
				sqlExec,
				jobID,
				msg)
			terror.Log(err2)
			return
		}
		errMsg := err.Error()
		if errImpl, ok := errors.Cause(err).(*errors.Error); ok {
			b, marshalErr := errImpl.MarshalJSON()
			if marshalErr == nil {
				errMsg = string(b)
			}
		}

		err2 := asyncloaddata.FailJob(ctx2, sqlExec, jobID, errMsg)
		terror.Log(err2)
	}()

	failpoint.Inject("AfterCreateLoadDataJob", nil)

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
		e.progress.SourceFileSize = totalFilesize
	}

	err = asyncloaddata.StartJob(ctx, sqlExec, jobID)
	if err != nil {
		return err
	}

	failpoint.Inject("AfterStartJob", nil)

	group, groupCtx := errgroup.WithContext(ctx)
	// done is used to let commitWork goroutine notify UpdateJobProgress
	// goroutine that the job is finished.
	done := make(chan struct{})
	commitTaskCh := make(chan commitTask, taskQueueSize)

	// processStream goroutine.
	var parser mydump.Parser
	group.Go(func() error {
		err := sessiontxn.NewTxn(ctx, e.encodeWorker.ctx)
		if err != nil {
			return err
		}
		for _, info := range readerInfos {
			reader, err2 := info.Opener(ctx)
			if err2 != nil {
				return err2
			}

			parser, err2 = e.encodeWorker.buildParser(ctx, reader, info.Remote)
			if err2 != nil {
				terror.Log(reader.Close())
				return err2
			}
			err2 = e.encodeWorker.processStream(groupCtx, parser, reader, commitTaskCh)
			terror.Log(reader.Close())
			if err2 != nil {
				return err2
			}
		}

		close(commitTaskCh)
		return nil
	})
	// commitWork goroutine.
	group.Go(func() error {
		failpoint.Inject("BeforeCommitWork", nil)
		err2 := e.commitWorker.commitWork(groupCtx, commitTaskCh)
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
				// When done, try to update progress to reach 100%
				ok, err2 := asyncloaddata.UpdateJobProgress(ctx, sqlExec, jobID, e.progress.String())
				if !ok || err2 != nil {
					logutil.Logger(ctx).Warn("failed to update job progress when finished",
						zap.Bool("ok", ok), zap.Error(err2))
				}
				return nil
			case <-groupCtx.Done():
				return nil
			case <-ticker.C:
				ok, err2 := asyncloaddata.UpdateJobProgress(ctx, sqlExec, jobID, e.progress.String())
				if err2 != nil {
					return err2
				}
				if !ok {
					return errors.Errorf("failed to update job progress, the job %d is interrupted by user or failed to keepalive", jobID)
				}
			}
		}
	})

	err = group.Wait()
	msg = e.mergeAndSetMessage()
	return err
}

// encodeWorker is a sub-worker of LoadDataWorker that dedicated to encode data.
type encodeWorker struct {
	*InsertValues
	controller *importer.LoadDataController
	killed     *uint32
	rows       [][]types.Datum
}

func (w *encodeWorker) buildParser(
	ctx context.Context,
	reader io.ReadSeekCloser,
	remote *loadRemoteInfo,
) (parser mydump.Parser, err error) {
	switch w.controller.Format {
	case importer.LoadDataFormatDelimitedData:
		// CSV-like
		parser, err = mydump.NewCSVParser(
			ctx,
			w.controller.GenerateCSVConfig(),
			reader,
			importer.LoadDataReadBlockSize,
			nil,
			false,
			// TODO: support charset conversion
			nil)
	case importer.LoadDataFormatSQLDump:
		parser = mydump.NewChunkParser(
			ctx,
			w.ctx.GetSessionVars().SQLMode,
			reader,
			importer.LoadDataReadBlockSize,
			nil,
		)
	case importer.LoadDataFormatParquet:
		if remote == nil {
			return nil, exeerrors.ErrLoadParquetFromLocal
		}
		parser, err = mydump.NewParquetParser(
			ctx,
			remote.store,
			reader,
			remote.path,
		)
	}
	if err != nil {
		return nil, exeerrors.ErrLoadDataWrongFormatConfig.GenWithStack(err.Error())
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

	ignoreLineCnt := w.controller.IgnoreLines
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
func (w *encodeWorker) processStream(
	ctx context.Context,
	parser mydump.Parser,
	seeker io.Seeker,
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
		scannedSize, err := seeker.Seek(0, io.SeekCurrent)
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
				// TODO: after multiple encodeWorker, caller should close this channel
				close(outCh)
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

	fieldMappings := w.controller.GetFieldMapping()
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

		row = append(row, parserData[i])
	}
	for i := 0; i < len(w.controller.ColumnAssignments); i++ {
		// eval expression of `SET` clause
		d, err := expression.EvalAstExpr(w.ctx, w.controller.ColumnAssignments[i].Expr)
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
			w.progress.LoadedRowCntSetter.Add(task.cnt)
			w.progress.LoadedFileSizeSetter.Add(task.fileSize)
			taskCnt++
			logutil.Logger(ctx).Debug("commit one task success",
				zap.Duration("commit time usage", time.Since(start)),
				zap.Uint64("keys processed", task.cnt),
				zap.Uint64("taskCnt processed", taskCnt),
			)
			failpoint.Inject("AfterCommitOneTask", nil)
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

// mergeAndSetMessage merges stats from all used session context and sets info
// message(ERR_LOAD_INFO) generated by LOAD statement to UserSctx, it is
// public for test.
func (e *LoadDataWorker) mergeAndSetMessage() string {
	stmtCtx := e.encodeWorker.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	numDeletes := stmtCtx.DeletedRows()
	numSkipped := numRecords - stmtCtx.CopiedRows()
	numWarnings := stmtCtx.WarningCount()
	stmtCtx2 := e.commitWorker.ctx.GetSessionVars().StmtCtx
	numRecords += stmtCtx2.RecordRows()
	numDeletes += stmtCtx2.DeletedRows()
	numSkipped += numRecords - stmtCtx2.CopiedRows()
	numWarnings += stmtCtx2.WarningCount()

	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo].Raw, numRecords, numDeletes, numSkipped, numWarnings)
	if !e.controller.Detached {
		e.UserSctx.GetSessionVars().StmtCtx.SetMessage(msg)
	}
	return msg
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
	err := ResetContextOfStmt(e.encodeWorker.ctx, &ast.LoadDataStmt{})
	if err != nil {
		return err
	}
	err = ResetContextOfStmt(e.commitWorker.ctx, &ast.LoadDataStmt{})
	if err != nil {
		return err
	}

	ctx := context.Background()
	for i := uint64(0); i < e.controller.IgnoreLines; i++ {
		//nolint: errcheck
		_ = parser.ReadRow()
	}
	err = e.encodeWorker.readOneBatchRows(ctx, parser)
	if err != nil {
		return err
	}
	err = sessiontxn.NewTxn(ctx, e.commitWorker.ctx)
	if err != nil {
		return err
	}
	err = e.commitWorker.checkAndInsertOneBatch(
		ctx,
		e.encodeWorker.rows,
		e.encodeWorker.curBatchCnt)
	if err != nil {
		return err
	}
	e.encodeWorker.resetBatch()
	e.commitWorker.ctx.StmtCommit(ctx)
	err = e.commitWorker.ctx.CommitTxn(ctx)
	if err != nil {
		return err
	}
	e.mergeAndSetMessage()
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

	switch e.tp {
	case ast.LoadDataCancel:
		return asyncloaddata.CancelJob(ctx, sqlExec, e.jobID, user)
	case ast.LoadDataDrop:
		return asyncloaddata.DropJob(ctx, sqlExec, e.jobID, user)
	default:
		return errors.Errorf("not implemented LOAD DATA action %v", e.tp)
	}
}
