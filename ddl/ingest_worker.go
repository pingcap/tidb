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

package ddl

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/operator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"sync"
	"time"
)

type addIndexIngestWorker struct {
	ctx           context.Context
	d             *ddlCtx
	metricCounter prometheus.Counter
	sessCtx       sessionctx.Context

	tbl              table.PhysicalTable
	index            table.Index
	writer           ingest.Writer
	copReqSenderPool *copReqSenderPool
	checkpointMgr    *ingest.CheckpointManager
	flushLock        *sync.RWMutex

	resultCh   chan *backfillResult
	jobID      int64
	distribute bool

	// for operator.
	tableScan *tableScanOperator
	sink      operator.DataSink[*backfillResult]
}

func newAddIndexIngestWorker(ctx context.Context, t table.PhysicalTable, d *ddlCtx, ei ingest.Engine,
	resultCh chan *backfillResult, jobID int64, schemaName string, indexID int64, writerID int,
	copReqSenderPool *copReqSenderPool, sessCtx sessionctx.Context,
	checkpointMgr *ingest.CheckpointManager, distribute bool) (*addIndexIngestWorker, error) {
	indexInfo := model.FindIndexInfoByID(t.Meta().Indices, indexID)
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	lw, err := ei.CreateWriter(writerID, indexInfo.Unique)
	if err != nil {
		return nil, err
	}

	return &addIndexIngestWorker{
		ctx:     ctx,
		d:       d,
		sessCtx: sessCtx,
		metricCounter: metrics.BackfillTotalCounter.WithLabelValues(
			metrics.GenerateReorgLabel("add_idx_rate", schemaName, t.Meta().Name.O)),
		tbl:              t,
		index:            index,
		writer:           lw,
		copReqSenderPool: copReqSenderPool,
		resultCh:         resultCh,
		jobID:            jobID,
		checkpointMgr:    checkpointMgr,
		distribute:       distribute,
	}, nil
}

// WriteLocal will write index records to lightning engine.
func (w *addIndexIngestWorker) WriteLocal(rs *idxRecResult) (count int, nextKey kv.Key, err error) {
	oprStartTime := time.Now()
	var copCtx *copContext
	if w.copReqSenderPool != nil {
		copCtx = w.copReqSenderPool.copCtx
	} else {
		copCtx = w.tableScan.copCtx
	}

	vars := w.sessCtx.GetSessionVars()
	cnt, lastHandle, err := writeChunkToLocal(w.writer, w.index, copCtx, vars, rs.chunk)
	if err != nil || cnt == 0 {
		return 0, nil, err
	}
	w.metricCounter.Add(float64(cnt))
	logSlowOperations(time.Since(oprStartTime), "writeChunkToLocal", 3000)
	nextKey = tablecodec.EncodeRecordKey(w.tbl.RecordPrefix(), lastHandle)
	return cnt, nextKey, nil
}

func (w *addIndexIngestWorker) addRes(res *backfillResult) {
	if w.sink != nil {
		w.sink.Write(res)
	} else {
		w.resultCh <- res
	}
}

func (w *addIndexIngestWorker) HandleTask(rs idxRecResult) {
	logutil.BgLogger().Info("add index ingest worker handle task")
	defer util.Recover(metrics.LabelDDL, "ingestWorker.HandleTask", func() {
		res := &backfillResult{taskID: rs.id, err: dbterror.ErrReorgPanic}
		w.addRes(res)
	}, false)
	defer func() {
		if w.copReqSenderPool != nil {
			w.copReqSenderPool.recycleChunk(rs.chunk)
		} else {
			w.tableScan.recycleChunk(rs.chunk)
		}
	}()
	result := &backfillResult{
		taskID: rs.id,
		err:    rs.err,
	}
	if result.err != nil {
		logutil.Logger(w.ctx).Error("encounter error when handle index chunk",
			zap.Int("id", rs.id), zap.Error(rs.err))
		w.addRes(result)
		return
	}
	if !w.distribute {
		err := w.d.isReorgRunnable(w.jobID, false)
		if err != nil {
			result.err = err
			w.addRes(result)
			return
		}
	}
	count, nextKey, err := w.WriteLocal(&rs)
	if err != nil {
		result.err = err
		w.addRes(result)
		return
	}
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a cop-request task", zap.Int("id", rs.id))
		return
	}
	if w.checkpointMgr != nil {
		cnt, nextKey := w.checkpointMgr.Status()
		result.totalCount = cnt
		result.nextKey = nextKey
		result.err = w.checkpointMgr.UpdateCurrent(rs.id, count)
	} else {
		result.addedCount = count
		result.scanCount = count
		result.nextKey = nextKey
	}
	if ResultCounterForTest != nil && result.err == nil {
		ResultCounterForTest.Add(1)
	}
	w.addRes(result)
}

func (*addIndexIngestWorker) Close() {}

func writeChunkToLocal(writer ingest.Writer,
	index table.Index, copCtx *copContext, vars *variable.SessionVars,
	copChunk *chunk.Chunk) (int, kv.Handle, error) {
	sCtx, writeBufs := vars.StmtCtx, vars.GetWriteStmtBufs()
	iter := chunk.NewIterator4Chunk(copChunk)
	idxDataBuf := make([]types.Datum, len(copCtx.idxColOutputOffsets))
	handleDataBuf := make([]types.Datum, len(copCtx.handleOutputOffsets))
	count := 0
	var lastHandle kv.Handle
	unlock := writer.LockForWrite()
	defer unlock()
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		idxDataBuf, handleDataBuf = idxDataBuf[:0], handleDataBuf[:0]
		idxDataBuf = extractDatumByOffsets(row, copCtx.idxColOutputOffsets, copCtx.expColInfos, idxDataBuf)
		handleDataBuf := extractDatumByOffsets(row, copCtx.handleOutputOffsets, copCtx.expColInfos, handleDataBuf)
		handle, err := buildHandle(handleDataBuf, copCtx.tblInfo, copCtx.pkInfo, sCtx)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		rsData := getRestoreData(copCtx.tblInfo, copCtx.idxInfo, copCtx.pkInfo, handleDataBuf)
		err = writeOneKVToLocal(writer, index, sCtx, writeBufs, idxDataBuf, rsData, handle)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		count++
		lastHandle = handle
	}
	return count, lastHandle, nil
}

func writeOneKVToLocal(writer ingest.Writer,
	index table.Index, sCtx *stmtctx.StatementContext, writeBufs *variable.WriteStmtBufs,
	idxDt, rsData []types.Datum, handle kv.Handle) error {
	iter := index.GenIndexKVIter(sCtx, idxDt, handle, rsData)
	for iter.Valid() {
		key, idxVal, _, err := iter.Next(writeBufs.IndexKeyBuf)
		if err != nil {
			return errors.Trace(err)
		}
		failpoint.Inject("mockLocalWriterPanic", func() {
			panic("mock panic")
		})
		err = writer.WriteRow(key, idxVal, handle)
		if err != nil {
			return errors.Trace(err)
		}
		failpoint.Inject("mockLocalWriterError", func() {
			failpoint.Return(errors.New("mock engine error"))
		})
		writeBufs.IndexKeyBuf = key
	}
	return nil
}
