package ddl

import (
	"context"
	ddllogutil "github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	kvutil "github.com/tikv/client-go/v2/util"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	_ operator.WithSource[TableScanTask] = (*KVScanOperator)(nil)
	_ operator.Operator                  = (*KVScanOperator)(nil)
	_ operator.WithSink[RowRecords]      = (*KVScanOperator)(nil)

	_ operator.WithSource[RowRecords]     = (*TXNCommitOperator)(nil)
	_ operator.Operator                   = (*TXNCommitOperator)(nil)
	_ operator.WithSink[IndexWriteResult] = (*TXNCommitOperator)(nil)
)

func NewModifyColumnTxnPipeline(
	ctx *OperatorCtx,
	store kv.Storage,
	sessPool opSessPool,
	backendCtx ingest.BackendCtx,
	engines []ingest.Engine,
	jobID int64,
	tbl table.PhysicalTable,
	idxInfos []*model.IndexInfo,
	startKey, endKey kv.Key,
	reorgMeta *model.DDLReorgMeta,
	avgRowSize int,
	concurrency int,
	collector execute.Collector,
	reorgInfo *reorgInfo,
) (*operator.AsyncPipeline, error) {
	indexes := make([]table.Index, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		index := tables.NewIndex(tbl.GetPhysicalID(), tbl.Meta(), idxInfo)
		indexes = append(indexes, index)
	}
	reqSrc := getDDLRequestSource(model.ActionAddIndex)
	copCtx, err := NewReorgCopContext(store, reorgMeta, tbl.Meta(), idxInfos, reqSrc)
	if err != nil {
		return nil, err
	}
	srcChkPool := createChkPool(reorgMeta)
	//readerCnt, writerCnt := expectedIngestWorkerCnt(concurrency, avgRowSize)
	readerCnt := int(vardef.DDLModifyColumnReaderCnt.Load())
	writerCnt := int(vardef.DDLModifyColumnWriterCnt.Load())

	failpoint.InjectCall("beforeAddIndexScan")

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey, backendCtx)
	scanOp := NewKVScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt,
		reorgMeta.GetBatchSize(), reorgMeta, backendCtx, reorgInfo, tbl)
	ingestOp := NewTXNCommitOperator(ctx, copCtx, sessPool,
		tbl, indexes, engines, srcChkPool, writerCnt, reorgMeta, reorgInfo)
	sinkOp := newIndexWriteResultSink(ctx, backendCtx, tbl, indexes, collector)

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

func createChkPool(reorgMeta *model.DDLReorgMeta) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			return make([]*rowRecord, 0, reorgMeta.GetBatchSize())
		},
	}
}

type TXNCommitOperator struct {
	*operator.AsyncOperator[RowRecords, IndexWriteResult]
}

// NewTXNCommitOperator creates a new IndexIngestOperator.
func NewTXNCommitOperator(
	ctx *OperatorCtx,
	copCtx copr.CopContext,
	sessPool opSessPool,
	tbl table.PhysicalTable,
	indexes []table.Index,
	engines []ingest.Engine,
	srcChunkPool *sync.Pool,
	concurrency int,
	reorgMeta *model.DDLReorgMeta,
	reorgInfo *reorgInfo,
) *TXNCommitOperator {

	pool := workerpool.NewWorkerPool(
		"txnCommitOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[RowRecords, IndexWriteResult] {
			jc := reorgInfo.NewJobContext()
			bCtx, err := newBackfillCtx(112233, reorgInfo, reorgInfo.SchemaName, tbl, jc, metrics.LblUpdateColRate, true)
			if err != nil {
				panic(err)
			}
			return &txnCommitWorker{
				ctx:     ctx,
				tbl:     tbl,
				indexes: indexes,
				copCtx:  copCtx,

				se:       nil,
				sessPool: sessPool,
				//writers:      writers,
				srcChunkPool: srcChunkPool,
				reorgMeta:    reorgMeta,

				backfillCtx: bCtx,
				jobID:       reorgInfo.Job.ID,
				priority:    reorgInfo.Priority,
			}
		})
	return &TXNCommitOperator{
		AsyncOperator: operator.NewAsyncOperator[RowRecords, IndexWriteResult](ctx, pool),
	}
}

type txnCommitWorker struct {
	ctx *OperatorCtx

	tbl       table.PhysicalTable
	indexes   []table.Index
	reorgMeta *model.DDLReorgMeta

	copCtx   copr.CopContext
	sessPool opSessPool
	se       *session.Session
	restore  func(sessionctx.Context)

	writers      []ingest.Writer
	srcChunkPool *sync.Pool
	// only available in global sort
	totalCount *atomic.Int64

	*backfillCtx
	jobID    int64
	priority int

	totalDur time.Duration
	totalNum int

	totalDurCommit time.Duration
}

func (w *txnCommitWorker) HandleTask(ck RowRecords, send func(IndexWriteResult)) {
	t := time.Now()
	defer func() {
		if ck.Chunk != nil {
			w.srcChunkPool.Put(ck.Chunk)
		}
		ddllogutil.DDLLogger().Debug("txn commit worker handle task",
			zap.Duration("takeTime", time.Since(t)),
		)
		w.totalDur += time.Since(t)
		w.totalNum++
	}()
	failpoint.Inject("injectPanicForIndexIngest", func() {
		panic("mock panic")
	})
	if ck.Chunk == nil || len(ck.Chunk) == 0 {
		return
	}

	result := IndexWriteResult{
		ID: ck.ID,
	}
	w.initSessCtx()
	//count, _, err := w.WriteChunk(&ck)
	count, err := w.CommitTXN(&ck)
	if err != nil {
		w.ctx.onError(err)
		return
	}
	w.totalDurCommit += time.Since(t)
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a index ingest task", zap.Int("id", ck.ID))
		return
	}
	if w.totalCount != nil {
		w.totalCount.Add(int64(count))
	}
	result.Added = count
	if ResultCounterForTest != nil {
		ResultCounterForTest.Add(1)
	}
	send(result)
}

func (w *txnCommitWorker) initSessCtx() {
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

func (w *txnCommitWorker) Close() {
	// TODO(lance6716): unify the real write action for engineInfo and external
	// writer.
	ddllogutil.DDLLogger().Info("txnCommitWorker handle task",
		zap.Int("totalNum", w.totalNum),
		zap.Duration("totalDur_commit+send", w.totalDur),
		zap.Duration("avgTimePerChunk_commit+send", w.totalDur/time.Duration(w.totalNum)),
		zap.Duration("totalDur_commit", w.totalDurCommit),
		zap.Duration("avgTimePerChunk_commit", w.totalDurCommit/time.Duration(w.totalNum)),
	)
	for i, writer := range w.writers {
		ew, ok := writer.(*external.Writer)
		if !ok {
			break
		}
		err := ew.Close(w.ctx)
		if err != nil {
			err = ingest.TryConvertToKeyExistsErr(err, w.indexes[i].Meta(), w.tbl.Meta())
			w.ctx.onError(err)
		}
	}
	if w.se != nil {
		w.restore(w.se.Context)
		w.sessPool.Put(w.se.Context)
	}
}

func (w *txnCommitWorker) CommitTXN(rr *RowRecords) (count int, errInTxn error) {
	rowRecords := rr.Chunk
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {

		//txn.SetOption(kv.Enable1PC, true)
		
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
			//taskCtx.scanCount++

			err = txn.Set(rowRecord.key, rowRecord.vals)
			if err != nil {
				return errors.Trace(err)
			}
			//taskCtx.addedCount++
			if rowRecord.warning != nil {
				if _, ok := warningsCountMap[rowRecord.warning.ID()]; ok {
					warningsCountMap[rowRecord.warning.ID()]++
				} else {
					warningsCountMap[rowRecord.warning.ID()] = 1
					warningsMap[rowRecord.warning.ID()] = rowRecord.warning
				}
			}
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "BackfillData", 3000)
	count = len(rowRecords)
	return
}

type RowRecords struct {
	ID       int
	Chunk    []*rowRecord
	Err      error
	Done     bool
	ctx      *OperatorCtx
	NextKey  kv.Key
	TaskDone bool
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (t RowRecords) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return metrics.LblAddIndex, "RecoverArgs", func() {
		t.ctx.onError(dbterror.ErrReorgPanic)
	}, false
}

type KVScanOperator struct {
	*operator.AsyncOperator[TableScanTask, RowRecords]
	logger     *zap.Logger
	totalCount *atomic.Int64
}

func NewKVScanOperator(
	ctx *OperatorCtx,
	sessPool opSessPool,
	copCtx copr.CopContext,
	srcChkPool *sync.Pool,
	concurrency int,
	hintBatchSize int,
	reorgMeta *model.DDLReorgMeta,
	cpOp ingest.CheckpointOperator,
	reorgInfo *reorgInfo,
	tbl table.PhysicalTable,
) *KVScanOperator {
	totalCount := new(atomic.Int64)
	decodeColMap, err := makeupDecodeColMap(reorgInfo.dbInfo.Name, tbl)
	if err != nil {
		panic(err)
	}
	oldCol, newCol := getOldAndNewColumnsForUpdateColumn(tbl, reorgInfo.currElement.ID)
	rowDecoder := decoder.NewRowDecoder(tbl, tbl.WritableCols(), decodeColMap)

	pool := workerpool.NewWorkerPool(
		"KVScanOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[TableScanTask, RowRecords] {
			jc := reorgInfo.NewJobContext()
			bCtx, err := newBackfillCtx(445566, reorgInfo, reorgInfo.SchemaName, tbl, jc, metrics.LblUpdateColRate, true)
			if err != nil {
				panic(err)
			}
			return &kvScanWorker{
				ctx:           ctx,
				copCtx:        copCtx,
				sessPool:      sessPool,
				se:            nil,
				srcChkPool:    srcChkPool,
				cpOp:          cpOp,
				hintBatchSize: hintBatchSize,
				totalCount:    totalCount,
				reorgMeta:     reorgMeta,
				backfillCtx:   bCtx,
				tbl:           tbl,
				priority:      reorgInfo.Priority,
				jobID:         reorgInfo.Job.ID,
				rowDecoder:    rowDecoder,
				rowMap:        make(map[int64]types.Datum, len(decodeColMap)),
				oldColInfo:    oldCol,
				newColInfo:    newCol,
			}
		})
	return &KVScanOperator{
		AsyncOperator: operator.NewAsyncOperator[TableScanTask, RowRecords](ctx, pool),
		logger:        logutil.Logger(ctx),
		totalCount:    totalCount,
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
	ctx        *OperatorCtx
	copCtx     copr.CopContext
	sessPool   opSessPool
	se         *session.Session
	srcChkPool *sync.Pool

	cpOp          ingest.CheckpointOperator
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

	totalDur time.Duration
	totalNum int

	totalDurGet time.Duration
	totalNumGet int
}

func (w *kvScanWorker) HandleTask(task TableScanTask, sender func(RowRecords)) {
	t := time.Now()
	defer func() {
		ddllogutil.DDLLogger().Info("kv scan worker handle task",
			zap.Duration("takeTime", time.Since(t)),
		)
	}()
	failpoint.Inject("injectPanicForTableScan", func() {
		panic("mock panic")
	})
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			logutil.Logger(w.ctx).Error("tableScanWorker get session from pool failed", zap.Error(err))
			w.ctx.onError(err)
			return
		}
		w.se = session.NewSession(sessCtx)
	}
	w.scanRecords(task, sender)
}

func (w *kvScanWorker) Close() {
	ddllogutil.DDLLogger().Info("kvScanWorker handle task",
		zap.Int("totalNum_Scan+Send", w.totalNum),
		zap.Duration("totalDur_Scan+Send", w.totalDur),
		zap.Duration("avgTimePerChunk_Scan+Send", w.totalDur/time.Duration(w.totalNum)),
		zap.Int("totalNum_Scan", w.totalNumGet),
		zap.Duration("totalDur_Scan", w.totalDurGet),
		zap.Duration("avgTimePerChunk_Scan", w.totalDurGet/time.Duration(w.totalNumGet)),
	)
	if w.se != nil {
		w.sessPool.Put(w.se.Context)
	}
}

func (w *kvScanWorker) scanRecords(task TableScanTask, sender func(RowRecords)) {
	logutil.Logger(w.ctx).Info("start a table scan task",
		zap.Int("id", task.ID), zap.Stringer("task", task))

	var idxResult RowRecords
	chunkNum := 0
	t := time.Now()
	err := wrapInBeginRollback(w.se, func(startTS uint64) error {
		failpoint.Inject("mockScanRecordError", func() {
			failpoint.Return(errors.New("mock scan record error"))
		})
		failpoint.InjectCall("scanRecordExec", w.reorgMeta)
		if w.cpOp != nil {
			w.cpOp.AddChunk(task.ID, task.End)
		}
		handleRange := reorgBackfillTask{
			physicalTable: w.tbl,
			startKey:      task.Start,
			endKey:        task.End,
			priority:      w.priority,
			jobID:         w.jobID,
			id:            task.ID,
		}

		var done bool
		for !done {
			failpoint.InjectCall("beforeGetChunk")
			srcChk := w.getChunk()

			t1 := time.Now()
			rowRecords, nextKey, taskDone, err := w.fetchRowColVals(startTS, handleRange, srcChk)
			if err != nil {
				w.recycleChunk(srcChk)
				return err
			}
			idxResult = RowRecords{
				ID:       task.ID,
				Chunk:    rowRecords,
				Done:     taskDone,
				ctx:      w.ctx,
				NextKey:  nextKey,
				TaskDone: taskDone,
			}
			rowCnt := len(idxResult.Chunk)
			if w.cpOp != nil {
				w.cpOp.UpdateChunk(task.ID, rowCnt, done)
			}
			w.totalCount.Add(int64(rowCnt))
			sender(idxResult)
			chunkNum++
			w.totalDur += time.Since(t1)
			w.totalNum++
			//idxResults = append(idxResults, RowRecords{
			//	ID:       task.ID,
			//	Chunk:    rowRecords,
			//	Done:     taskDone,
			//	ctx:      w.ctx,
			//	NextKey:  nextKey,
			//	TaskDone: taskDone,
			//})
			handleRange.startKey = nextKey
			done = taskDone
		}
		return nil
	})

	logutil.Logger(w.ctx).Info("scanRecords",
		zap.Int("chunkNum", chunkNum),
		zap.Duration("takeTime", time.Since(t)),
		zap.Duration("avgTimePerChunkRead&Send", time.Since(t)/time.Duration(chunkNum)),
	)

	if err != nil {
		w.ctx.onError(err)
	}
	//for i, idxResult := range idxResults {
	//	sender(idxResult)
	//	rowCnt := len(idxResult.Chunk)
	//	if w.cpOp != nil {
	//		done := i == len(idxResults)-1
	//		w.cpOp.UpdateChunk(task.ID, rowCnt, done)
	//	}
	//	w.totalCount.Add(int64(rowCnt))
	//}
}

func (w *kvScanWorker) fetchRowColVals(startTS uint64, taskRange reorgBackfillTask, rowRecords []*rowRecord) ([]*rowRecord, kv.Key, bool, error) {
	rowRecords = rowRecords[:0]
	w.rowRecords = rowRecords
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	var lastAccessedHandle kv.Key
	oprStartTime := startTime
	encDecDur := time.Duration(0)
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, taskRange.physicalTable.RecordPrefix(),
		startTS, taskRange.startKey, taskRange.endKey, func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in updateColumnWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.rowRecords) >= w.hintBatchSize {
				return false, nil
			}
			t := time.Now()
			if err1 := w.getRowRecord(handle, recordKey, rawRow); err1 != nil {
				return false, errors.Trace(err1)
			}
			encDecDur += time.Since(t)
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

	ddllogutil.DDLLogger().Debug("txn fetches handle info",
		zap.Uint64("txnStartTS", startTS),
		zap.String("taskRange", taskRange.String()),
		zap.Duration("takeTime", time.Since(startTime)),
		zap.Duration("encodeDecodeCastTime", encDecDur),
	)
	w.totalDurGet += time.Since(startTime)
	w.totalNumGet++
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
	chk := w.srcChkPool.Get().([]*rowRecord)
	if cap(chk) != targetCap {
		chk = make([]*rowRecord, 0, targetCap)
		logutil.Logger(w.ctx).Info("adjust ddl job config success", zap.Int("current batch size", len(chk)))
	}
	return chk
}

func (w *kvScanWorker) recycleChunk(chk []*rowRecord) {
	w.srcChkPool.Put(chk)
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
