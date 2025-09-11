package ddl

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	ddllogutil "github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	_ operator.WithSource[TableScanTask] = (*KVScanOperator)(nil)
	_ operator.Operator                  = (*KVScanOperator)(nil)
	_ operator.WithSink[rowKVRecords]    = (*KVScanOperator)(nil)

	_ operator.WithSource[rowKVRecords]          = (*CommitTxnOperator)(nil)
	_ operator.Operator                          = (*CommitTxnOperator)(nil)
	_ operator.WithSink[modifyColumnWriteResult] = (*CommitTxnOperator)(nil)

	_ operator.WithSource[modifyColumnWriteResult] = (*modifyColumnWriteResultSink)(nil)
	_ operator.Operator                            = (*modifyColumnWriteResultSink)(nil)
)

func NewModifyColumnPipeline(
	ctx *OperatorCtx,
	store kv.Storage,
	sessPool opSessPool,
	jobID int64,
	tbl table.PhysicalTable,
	startKey, endKey kv.Key,
	reorgMeta *model.DDLReorgMeta,
	avgRowSize int,
	concurrency int,
	collector execute.Collector,
	reorgInfo *reorgInfo,
) (*operator.AsyncPipeline, error) {
	rowRecordPool := createRowRecordPool(reorgMeta)

	//TODO(fzzf678): calculate reader/writer count based on concurrency and avgRowSize
	readerCnt := int(vardef.DDLModifyColumnReaderCnt.Load())
	writerCnt := int(vardef.DDLModifyColumnWriterCnt.Load())

	failpoint.InjectCall("beforeAddIndexScan")
	jc := reorgInfo.NewJobContext()
	decodeColMap, err := makeupDecodeColMap(reorgInfo.dbInfo.Name, tbl)
	if err != nil {
		return nil, err
	}

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey, nil)
	scanOp := NewKVScanOperator(ctx, sessPool, rowRecordPool, readerCnt,
		reorgMeta.GetBatchSize(), reorgMeta, reorgInfo, tbl, jc, decodeColMap)
	ingestOp := NewTXNCommitOperator(ctx, sessPool,
		tbl, rowRecordPool, writerCnt, reorgMeta, reorgInfo, jc)
	sinkOp := newModifyColumnWriteResultSink(ctx, collector, reorgInfo.StartKey, sessPool, reorgInfo)

	operator.Compose(srcOp, scanOp)
	operator.Compose(scanOp, ingestOp)
	operator.Compose(ingestOp, sinkOp)

	logutil.Logger(ctx).Info("build modify column operators",
		zap.Int64("jobID", jobID),
		zap.Int("avgRowSize", avgRowSize),
		zap.Int("reader", readerCnt),
		zap.Int("writer", writerCnt))

	return operator.NewAsyncPipeline(
		srcOp, scanOp, ingestOp, sinkOp,
	), nil
}

type rowKVRecords struct {
	id      int
	chunk   []*rowRecord
	ctx     *OperatorCtx
	nextKey kv.Key
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (t rowKVRecords) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return metrics.LblModifyColumn, "RecoverArgs", func() {
		t.ctx.onError(dbterror.ErrReorgPanic)
	}, false
}

func createRowRecordPool(reorgMeta *model.DDLReorgMeta) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			return make([]*rowRecord, 0, reorgMeta.GetBatchSize())
		},
	}
}

type KVScanOperator struct {
	*operator.AsyncOperator[TableScanTask, rowKVRecords]
	logger     *zap.Logger
	totalCount *atomic.Int64
}

func NewKVScanOperator(
	ctx *OperatorCtx,
	sessPool opSessPool,
	rowRecordPool *sync.Pool,
	concurrency int,
	hintBatchSize int,
	reorgMeta *model.DDLReorgMeta,
	reorgInfo *reorgInfo,
	tbl table.PhysicalTable,
	jobCtx *ReorgContext,
	decodeColMap map[int64]decoder.Column,
) *KVScanOperator {
	var (
		id         atomic.Int32
		totalCount atomic.Int64
	)
	oldCol, newCol := getOldAndNewColumnsForUpdateColumn(tbl, reorgInfo.currElement.ID)

	pool := workerpool.NewWorkerPool(
		"KVScanOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[TableScanTask, rowKVRecords] {
			bCtx, err := newBackfillCtx(int(id.Add(1)), reorgInfo, reorgInfo.SchemaName, tbl, jobCtx, metrics.LblModifyColumn, true)
			if err != nil {
				logutil.Logger(ctx).Error("kvScanWorker new backfill context failed", zap.Error(err))
				return nil
			}
			rowDecoder := decoder.NewRowDecoder(tbl, tbl.WritableCols(), decodeColMap)
			return &kvScanWorker{
				ctx:            ctx,
				sessPool:       sessPool,
				rowRecordPool:  rowRecordPool,
				hintBatchSize:  hintBatchSize,
				totalCount:     &totalCount,
				reorgMeta:      reorgMeta,
				backfillCtx:    bCtx,
				tbl:            tbl,
				priority:       reorgInfo.Priority,
				jobID:          reorgInfo.Job.ID,
				rowDecoder:     rowDecoder,
				rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
				oldColInfo:     oldCol,
				newColInfo:     newCol,
				checksumNeeded: vardef.EnableRowLevelChecksum.Load(),
			}
		})
	return &KVScanOperator{
		AsyncOperator: operator.NewAsyncOperator[TableScanTask, rowKVRecords](ctx, pool),
		logger:        logutil.Logger(ctx),
		totalCount:    &totalCount,
	}
}

// Close implements operator.Operator interface.
func (o *KVScanOperator) Close() error {
	defer func() {
		o.logger.Info("kv scan operator total count", zap.Int64("count", o.totalCount.Load()))
	}()
	return o.AsyncOperator.Close()
}

type kvScanWorker struct {
	ctx           *OperatorCtx
	sessPool      opSessPool
	se            *session.Session
	rowRecordPool *sync.Pool
	reorgMeta     *model.DDLReorgMeta
	hintBatchSize int
	totalCount    *atomic.Int64
	*backfillCtx
	tbl            table.PhysicalTable
	priority       int
	jobID          int64
	rowDecoder     *decoder.RowDecoder
	rowMap         map[int64]types.Datum
	oldColInfo     *model.ColumnInfo
	newColInfo     *model.ColumnInfo
	checksumNeeded bool
	rowRecords     []*rowRecord
}

func (w *kvScanWorker) HandleTask(task TableScanTask, sender func(rowKVRecords)) {
	failpoint.Inject("injectPanicForTableScan", func() {
		panic("mock panic")
	})
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			logutil.Logger(w.ctx).Error("kvScanWorker get session from pool failed", zap.Error(err))
			w.ctx.onError(err)
			return
		}
		w.se = session.NewSession(sessCtx)
	}
	w.scanRowRecords(task, sender)
}

func (w *kvScanWorker) Close() {
	if w.se != nil {
		w.sessPool.Put(w.se.Context)
	}
}

func (w *kvScanWorker) scanRowRecords(task TableScanTask, sender func(rowKVRecords)) {
	logutil.Logger(w.ctx).Info("start a kv scan task",
		zap.Int("id", task.ID), zap.Stringer("task", task))

	err := wrapInBeginRollback(w.se, func(startTS uint64) error {
		failpoint.Inject("mockScanRecordError", func() {
			failpoint.Return(errors.New("mock scan record error"))
		})
		failpoint.InjectCall("scanRecordExec", w.reorgMeta)
		handleRange := reorgBackfillTask{
			physicalTable: w.tbl,
			startKey:      task.Start,
			endKey:        task.End,
			priority:      w.priority,
		}

		for {
			failpoint.InjectCall("beforeGetChunk")
			srcChk := w.getChunk()

			rowRecords, nextKey, done, err := w.fetchRowKVs(startTS, handleRange, srcChk)
			if err != nil {
				w.recycleChunk(srcChk)
				return err
			}
			idxResult := rowKVRecords{
				id:      task.ID,
				chunk:   rowRecords,
				ctx:     w.ctx,
				nextKey: nextKey,
			}
			rowCnt := len(idxResult.chunk)
			w.totalCount.Add(int64(rowCnt))
			sender(idxResult)
			handleRange.startKey = nextKey
			if done {
				break
			}
		}
		return nil
	})

	if err != nil {
		w.ctx.onError(err)
	}
}

func (w *kvScanWorker) fetchRowKVs(startTS uint64, taskRange reorgBackfillTask, rowRecords []*rowRecord) ([]*rowRecord, kv.Key, bool, error) {
	rowRecords = rowRecords[:0]
	w.rowRecords = rowRecords
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	var lastAccessedHandle kv.Key
	oprStartTime := startTime
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, taskRange.physicalTable.RecordPrefix(),
		startTS, taskRange.startKey, taskRange.endKey, func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in kvScanWorker fetchRowKVs", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.rowRecords) >= w.hintBatchSize {
				return false, nil
			}
			if err1 := w.getRowRecord(handle, recordKey, rawRow); err1 != nil {
				return false, errors.Trace(err1)
			}
			lastAccessedHandle = recordKey
			if recordKey.Cmp(taskRange.endKey) > 0 {
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.rowRecords) == 0 {
		taskDone = true
	}

	ddllogutil.DDLLogger().Debug("txn fetches row kvs",
		zap.Uint64("txnStartTS", startTS),
		zap.String("taskRange", taskRange.String()),
		zap.Duration("takeTime", time.Since(startTime)),
	)
	return w.rowRecords, getNextHandleKey(taskRange, taskDone, lastAccessedHandle), taskDone, errors.Trace(err)
}

func (w *kvScanWorker) getRowRecord(handle kv.Handle, recordKey []byte, rawRow []byte) error {
	sysTZ := w.loc
	_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, handle, rawRow, sysTZ, w.rowMap)
	if err != nil {
		return errors.Trace(dbterror.ErrCantDecodeRecord.GenWithStackByArgs("column", err))
	}

	if _, ok := w.rowMap[w.newColInfo.ID]; ok {
		// The column is already added by update or insert statement, skip it.
		w.cleanRowMap()
		return nil
	}

	var recordWarning *terror.Error
	// Since every updateColumnWorker handle their own work individually, we can cache warning in statement context when casting datum.
	oldWarn := w.warnings.GetWarnings()
	if oldWarn == nil {
		oldWarn = []contextutil.SQLWarn{}
	} else {
		oldWarn = oldWarn[:0]
	}
	w.warnings.SetWarnings(oldWarn)
	val := w.rowMap[w.oldColInfo.ID]
	col := w.newColInfo
	if val.Kind() == types.KindNull && col.FieldType.GetType() == mysql.TypeTimestamp && mysql.HasNotNullFlag(col.GetFlag()) {
		if v, err := expression.GetTimeCurrentTimestamp(w.exprCtx.GetEvalCtx(), col.GetType(), col.GetDecimal()); err == nil {
			// convert null value to timestamp should be substituted with current timestamp if NOT_NULL flag is set.
			w.rowMap[w.oldColInfo.ID] = v
		}
	}
	newColVal, err := table.CastColumnValue(w.exprCtx, w.rowMap[w.oldColInfo.ID], w.newColInfo, false, false)
	if err != nil {
		return w.reformatErrors(err)
	}
	warn := w.warnings.GetWarnings()
	if len(warn) != 0 {
		//nolint:forcetypeassert
		recordWarning = errors.Cause(w.reformatErrors(warn[0].Err)).(*terror.Error)
	}

	failpoint.Inject("MockReorgTimeoutInOneRegion", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			if handle.IntValue() == 3000 && atomic.CompareAndSwapInt32(&testCheckReorgTimeout, 0, 1) {
				failpoint.Return(errors.Trace(dbterror.ErrWaitReorgTimeout))
			}
		}
	})

	w.rowMap[w.newColInfo.ID] = newColVal
	_, err = w.rowDecoder.EvalRemainedExprColumnMap(w.exprCtx, w.rowMap)
	if err != nil {
		return errors.Trace(err)
	}
	newColumnIDs := make([]int64, 0, len(w.rowMap))
	newRow := make([]types.Datum, 0, len(w.rowMap))
	for colID, val := range w.rowMap {
		newColumnIDs = append(newColumnIDs, colID)
		newRow = append(newRow, val)
	}
	rd := w.tblCtx.GetRowEncodingConfig().RowEncoder
	ec := w.exprCtx.GetEvalCtx().ErrCtx()
	var checksum rowcodec.Checksum
	if w.checksumNeeded {
		checksum = rowcodec.RawChecksum{Handle: handle}
	}
	newRowVal, err := tablecodec.EncodeRow(sysTZ, newRow, newColumnIDs, nil, nil, checksum, rd)
	err = ec.HandleError(err)
	if err != nil {
		return errors.Trace(err)
	}

	w.rowRecords = append(w.rowRecords, &rowRecord{key: recordKey, vals: newRowVal, warning: recordWarning})
	w.cleanRowMap()
	return nil
}

func (w *kvScanWorker) getChunk() []*rowRecord {
	targetCap := ingest.CopReadBatchSize(w.hintBatchSize)
	if w.reorgMeta != nil {
		targetCap = ingest.CopReadBatchSize(w.reorgMeta.GetBatchSize())
	}
	rr := w.rowRecordPool.Get().([]*rowRecord)
	if cap(rr) != targetCap {
		rr = make([]*rowRecord, 0, targetCap)
		logutil.Logger(w.ctx).Info("adjust ddl job config success", zap.Int("current batch size", len(rr)))
	}
	return rr
}

func (w *kvScanWorker) recycleChunk(chk []*rowRecord) {
	w.rowRecordPool.Put(chk)
}

// reformatErrors casted error because `convertTo` function couldn't package column name and datum value for some errors.
func (w *kvScanWorker) reformatErrors(err error) error {
	// Since row count is not precious in concurrent reorganization, here we substitute row count with datum value.
	if types.ErrTruncated.Equal(err) || types.ErrDataTooLong.Equal(err) {
		dStr := datumToStringNoErr(w.rowMap[w.oldColInfo.ID])
		err = types.ErrTruncated.GenWithStack("Data truncated for column '%s', value is '%s'", w.oldColInfo.Name, dStr)
	}

	if types.ErrWarnDataOutOfRange.Equal(err) {
		dStr := datumToStringNoErr(w.rowMap[w.oldColInfo.ID])
		err = types.ErrWarnDataOutOfRange.GenWithStack("Out of range value for column '%s', the value is '%s'", w.oldColInfo.Name, dStr)
	}
	return err
}

func (w *kvScanWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

type CommitTxnOperator struct {
	*operator.AsyncOperator[rowKVRecords, modifyColumnWriteResult]
}

// NewTXNCommitOperator creates a new IndexIngestOperator.
func NewTXNCommitOperator(
	ctx *OperatorCtx,
	sessPool opSessPool,
	tbl table.PhysicalTable,
	rowRecordPool *sync.Pool,
	concurrency int,
	reorgMeta *model.DDLReorgMeta,
	reorgInfo *reorgInfo,
	jobCtx *ReorgContext,
) *CommitTxnOperator {
	var id atomic.Int32

	pool := workerpool.NewWorkerPool(
		"txnCommitOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[rowKVRecords, modifyColumnWriteResult] {
			bCtx, err := newBackfillCtx(int(id.Add(1)), reorgInfo, reorgInfo.SchemaName, tbl, jobCtx, metrics.LblModifyColumn, true)
			if err != nil {
				logutil.Logger(ctx).Error("kvScanWorker new backfill context failed", zap.Error(err))
				return nil
			}
			return &commitTxnWorker{
				ctx:           ctx,
				tbl:           tbl,
				sessPool:      sessPool,
				rowRecordPool: rowRecordPool,
				reorgMeta:     reorgMeta,
				backfillCtx:   bCtx,
				jobID:         reorgInfo.Job.ID,
				priority:      reorgInfo.Priority,
			}
		})
	return &CommitTxnOperator{
		AsyncOperator: operator.NewAsyncOperator[rowKVRecords, modifyColumnWriteResult](ctx, pool),
	}
}

type commitTxnWorker struct {
	ctx           *OperatorCtx
	tbl           table.PhysicalTable
	reorgMeta     *model.DDLReorgMeta
	sessPool      opSessPool
	se            *session.Session
	restore       func(sessionctx.Context)
	rowRecordPool *sync.Pool
	totalCount    *atomic.Int64
	*backfillCtx
	jobID    int64
	priority int
}

func (w *commitTxnWorker) HandleTask(ck rowKVRecords, send func(modifyColumnWriteResult)) {
	t := time.Now()
	defer func() {
		if ck.chunk != nil {
			w.rowRecordPool.Put(ck.chunk)
		}
		ddllogutil.DDLLogger().Debug("txn commit worker handle task",
			zap.Duration("takeTime", time.Since(t)),
		)
	}()
	failpoint.Inject("injectPanicForIndexIngest", func() {
		panic("mock panic")
	})

	w.initSessCtx()
	count, err := w.CommitTXN(&ck)
	if err != nil {
		w.ctx.onError(err)
		return
	}
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a txn commit task", zap.Int("id", ck.id))
		return
	}
	if w.totalCount != nil {
		w.totalCount.Add(int64(count))
	}
	result := modifyColumnWriteResult{
		id:      ck.id,
		added:   count,
		nextKey: ck.nextKey,
	}
	send(result)
}

func (w *commitTxnWorker) initSessCtx() {
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			w.ctx.onError(err)
			return
		}
		w.restore = restoreSessCtx(sessCtx)
		if err := initSessCtx(sessCtx, w.reorgMeta); err != nil {
			w.ctx.onError(err)
			return
		}
		w.se = session.NewSession(sessCtx)
	}
}

func (w *commitTxnWorker) Close() {
	if w.se != nil {
		w.restore(w.se.Context)
		w.sessPool.Put(w.se.Context)
	}
}

func (w *commitTxnWorker) CommitTXN(rr *rowKVRecords) (count int, errInTxn error) {
	rowRecords := rr.chunk
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)

	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		txn.SetOption(kv.Enable1PC, true)
		updateTxnEntrySizeLimitIfNeeded(txn)

		// Because TiCDC do not want this kind of change,
		// so we set the lossy DDL reorg txn source to 1 to
		// avoid TiCDC to replicate this kind of change.
		var txnSource uint64
		if val := txn.GetOption(kv.TxnSource); val != nil {
			txnSource, _ = val.(uint64)
		}
		err := kv.SetLossyDDLReorgSource(&txnSource, kv.LossyDDLColumnReorgSource)
		if err != nil {
			return errors.Trace(err)
		}
		txn.SetOption(kv.TxnSource, txnSource)

		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.backfillCtx.getResourceGroupTaggerForTopSQL(w.jobID); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		// Optimize for few warnings!
		warningsMap := make(map[errors.ErrorID]*terror.Error, 2)
		warningsCountMap := make(map[errors.ErrorID]int64, 2)
		for _, rowRecord := range rowRecords {
			err = txn.Set(rowRecord.key, rowRecord.vals)
			if err != nil {
				return errors.Trace(err)
			}
			if rowRecord.warning != nil {
				if _, ok := warningsCountMap[rowRecord.warning.ID()]; ok {
					warningsCountMap[rowRecord.warning.ID()]++
				} else {
					warningsCountMap[rowRecord.warning.ID()] = 1
					warningsMap[rowRecord.warning.ID()] = rowRecord.warning
				}
			}
		}
		rc := w.getReorgCtx(w.jobID)
		rc.mergeWarnings(warningsMap, warningsCountMap)
		return nil
	})

	logSlowOperations(time.Since(oprStartTime), "commitTxnWorker commit", 3000)
	count = len(rowRecords)
	return
}

type modifyColumnWriteResult struct {
	id      int
	added   int
	nextKey kv.Key
}

type modifyColumnWriteResultSink struct {
	ctx       *OperatorCtx
	collector execute.Collector
	errGroup  errgroup.Group
	source    operator.DataChannel[modifyColumnWriteResult]
	keeper    *doneTaskKeeper
	sessPool  opSessPool
	reorgInfo *reorgInfo
}

func newModifyColumnWriteResultSink(
	ctx *OperatorCtx,
	collector execute.Collector,
	startKey kv.Key,
	sessPool opSessPool,
	reorgInfo *reorgInfo,
) *modifyColumnWriteResultSink {
	keeper := newDoneTaskKeeper(startKey)

	return &modifyColumnWriteResultSink{
		ctx:       ctx,
		errGroup:  errgroup.Group{},
		collector: collector,
		keeper:    keeper,
		sessPool:  sessPool,
		reorgInfo: reorgInfo,
	}
}

func (s *modifyColumnWriteResultSink) SetSource(source operator.DataChannel[modifyColumnWriteResult]) {
	s.source = source
}

func (s *modifyColumnWriteResultSink) Open() error {
	s.errGroup.Go(s.collectResult)
	return nil
}

func (s *modifyColumnWriteResultSink) collectResult() error {
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case rs, ok := <-s.source.Channel():
			if !ok {
				return nil
			}
			s.collector.Add(0, int64(rs.added))
			s.keeper.updateNextKey(rs.id, rs.nextKey)
			err := s.reorgInfo.UpdateReorgMeta(s.keeper.nextKey, s.sessPool.(*session.Pool))
			if err != nil {
				ddllogutil.DDLLogger().Warn("update reorg meta failed",
					zap.Int64("job ID", s.reorgInfo.ID),
					zap.Error(err))
			}
		}
	}
}

func (s *modifyColumnWriteResultSink) Close() error {
	return s.errGroup.Wait()
}

func (*modifyColumnWriteResultSink) String() string {
	return "modifyColumnWriteResultSink"
}
