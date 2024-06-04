// Copyright 2022 PingCAP, Inc.
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
	"encoding/hex"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// copReadBatchSize is the batch size of coprocessor read.
// It multiplies the tidb_ddl_reorg_batch_size by 10 to avoid
// sending too many cop requests for the same handle range.
func copReadBatchSize() int {
	return 10 * int(variable.GetDDLReorgBatchSize())
}

// copReadChunkPoolSize is the size of chunk pool, which
// represents the max concurrent ongoing coprocessor requests.
// It multiplies the tidb_ddl_reorg_worker_cnt by 10.
func copReadChunkPoolSize() int {
	return 10 * int(variable.GetDDLReorgWorkerCounter())
}

// chunkSender is used to receive the result of coprocessor request.
type chunkSender interface {
	AddTask(IndexRecordChunk)
}

type copReqSenderPool struct {
	tasksCh       chan *reorgBackfillTask
	chunkSender   chunkSender
	checkpointMgr *ingest.CheckpointManager
	sessPool      *sess.Pool

	ctx    context.Context
	copCtx copr.CopContext
	store  kv.Storage

	senders []*copReqSender
	wg      sync.WaitGroup
	closed  bool

	srcChkPool chan *chunk.Chunk
}

type copReqSender struct {
	senderPool *copReqSenderPool

	ctx    context.Context
	cancel context.CancelFunc
}

func (c *copReqSender) run() {
	p := c.senderPool
	defer p.wg.Done()
	defer util.Recover(metrics.LabelDDL, "copReqSender.run", func() {
		p.chunkSender.AddTask(IndexRecordChunk{Err: dbterror.ErrReorgPanic})
	}, false)
	sessCtx, err := p.sessPool.Get()
	if err != nil {
		logutil.Logger(p.ctx).Error("copReqSender get session from pool failed", zap.Error(err))
		p.chunkSender.AddTask(IndexRecordChunk{Err: err})
		return
	}
	se := sess.NewSession(sessCtx)
	defer p.sessPool.Put(sessCtx)
	var (
		task *reorgBackfillTask
		ok   bool
	)

	for {
		select {
		case <-c.ctx.Done():
			return
		case task, ok = <-p.tasksCh:
		}
		if !ok {
			return
		}
		if p.checkpointMgr != nil && p.checkpointMgr.IsKeyProcessed(task.endKey) {
			logutil.Logger(p.ctx).Info("checkpoint detected, skip a cop-request task",
				zap.Int("task ID", task.id),
				zap.String("task end key", hex.EncodeToString(task.endKey)))
			continue
		}
		err := scanRecords(p, task, se)
		if err != nil {
			p.chunkSender.AddTask(IndexRecordChunk{ID: task.id, Err: err})
			return
		}
	}
}

func scanRecords(p *copReqSenderPool, task *reorgBackfillTask, se *sess.Session) error {
	logutil.Logger(p.ctx).Info("start a cop-request task",
		zap.Int("id", task.id), zap.Stringer("task", task))

	return wrapInBeginRollback(se, func(startTS uint64) error {
		rs, err := buildTableScan(p.ctx, p.copCtx.GetBase(), startTS, task.startKey, task.endKey)
		if err != nil {
			return err
		}
		failpoint.Inject("mockCopSenderPanic", func(val failpoint.Value) {
			if val.(bool) {
				panic("mock panic")
			}
		})
		if p.checkpointMgr != nil {
			p.checkpointMgr.Register(task.id, task.endKey)
		}
		var done bool
		startTime := time.Now()
		for !done {
			srcChk := p.getChunk()
			done, err = fetchTableScanResult(p.ctx, p.copCtx.GetBase(), rs, srcChk)
			if err != nil {
				p.recycleChunk(srcChk)
				terror.Call(rs.Close)
				return err
			}
			if p.checkpointMgr != nil {
				p.checkpointMgr.UpdateTotalKeys(task.id, srcChk.NumRows(), done)
			}
			idxRs := IndexRecordChunk{ID: task.id, Chunk: srcChk, Done: done}
			rate := float64(srcChk.MemoryUsage()) / 1024.0 / 1024.0 / time.Since(startTime).Seconds()
			metrics.AddIndexScanRate.WithLabelValues(metrics.LblAddIndex).Observe(rate)
			failpoint.Inject("mockCopSenderError", func() {
				idxRs.Err = errors.New("mock cop error")
			})
			p.chunkSender.AddTask(idxRs)
			startTime = time.Now()
		}
		terror.Call(rs.Close)
		return nil
	})
}

func wrapInBeginRollback(se *sess.Session, f func(startTS uint64) error) error {
	err := se.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Rollback()
	var startTS uint64
	sessVars := se.GetSessionVars()
	sessVars.TxnCtxMu.Lock()
	startTS = sessVars.TxnCtx.StartTS
	sessVars.TxnCtxMu.Unlock()
	return f(startTS)
}

func newCopReqSenderPool(ctx context.Context, copCtx copr.CopContext, store kv.Storage,
	taskCh chan *reorgBackfillTask, sessPool *sess.Pool,
	checkpointMgr *ingest.CheckpointManager) *copReqSenderPool {
	poolSize := copReadChunkPoolSize()
	srcChkPool := make(chan *chunk.Chunk, poolSize)
	for i := 0; i < poolSize; i++ {
		srcChkPool <- chunk.NewChunkWithCapacity(copCtx.GetBase().FieldTypes, copReadBatchSize())
	}
	return &copReqSenderPool{
		tasksCh:       taskCh,
		ctx:           ctx,
		copCtx:        copCtx,
		store:         store,
		senders:       make([]*copReqSender, 0, variable.GetDDLReorgWorkerCounter()),
		wg:            sync.WaitGroup{},
		srcChkPool:    srcChkPool,
		sessPool:      sessPool,
		checkpointMgr: checkpointMgr,
	}
}

func (c *copReqSenderPool) adjustSize(n int) {
	// Add some senders.
	for i := len(c.senders); i < n; i++ {
		ctx, cancel := context.WithCancel(c.ctx)
		c.senders = append(c.senders, &copReqSender{
			senderPool: c,
			ctx:        ctx,
			cancel:     cancel,
		})
		c.wg.Add(1)
		go c.senders[i].run()
	}
	// Remove some senders.
	if n < len(c.senders) {
		for i := n; i < len(c.senders); i++ {
			c.senders[i].cancel()
		}
		c.senders = c.senders[:n]
	}
}

func (c *copReqSenderPool) close(force bool) {
	if c.closed {
		return
	}
	logutil.Logger(c.ctx).Info("close cop-request sender pool", zap.Bool("force", force))
	if force {
		for _, w := range c.senders {
			w.cancel()
		}
	}
	// Wait for all cop-req senders to exit.
	c.wg.Wait()
	c.closed = true
}

func (c *copReqSenderPool) getChunk() *chunk.Chunk {
	chk := <-c.srcChkPool
	newCap := copReadBatchSize()
	if chk.Capacity() != newCap {
		chk = chunk.NewChunkWithCapacity(c.copCtx.GetBase().FieldTypes, newCap)
	}
	chk.Reset()
	return chk
}

// recycleChunk puts the index record slice and the chunk back to the pool for reuse.
func (c *copReqSenderPool) recycleChunk(chk *chunk.Chunk) {
	if chk == nil {
		return
	}
	c.srcChkPool <- chk
}

func buildTableScan(ctx context.Context, c *copr.CopContextBase, startTS uint64, start, end kv.Key) (distsql.SelectResult, error) {
	dagPB, err := buildDAGPB(c.ExprCtx, c.DistSQLCtx, c.PushDownFlags, c.TableInfo, c.ColumnInfos)
	if err != nil {
		return nil, err
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagPB).
		SetStartTS(startTS).
		SetKeyRanges([]kv.KeyRange{{StartKey: start, EndKey: end}}).
		SetKeepOrder(true).
		SetFromSessionVars(c.DistSQLCtx).
		SetConcurrency(1).
		Build()
	kvReq.RequestSource.RequestSourceInternal = true
	kvReq.RequestSource.RequestSourceType = getDDLRequestSource(model.ActionAddIndex)
	kvReq.RequestSource.ExplicitRequestSourceType = kvutil.ExplicitTypeDDL
	if err != nil {
		return nil, err
	}
	return distsql.Select(ctx, c.DistSQLCtx, kvReq, c.FieldTypes)
}

func fetchTableScanResult(
	ctx context.Context,
	copCtx *copr.CopContextBase,
	result distsql.SelectResult,
	chk *chunk.Chunk,
) (bool, error) {
	err := result.Next(ctx, chk)
	if err != nil {
		return false, errors.Trace(err)
	}
	if chk.NumRows() == 0 {
		return true, nil
	}
	err = table.FillVirtualColumnValue(
		copCtx.VirtualColumnsFieldTypes, copCtx.VirtualColumnsOutputOffsets,
		copCtx.ExprColumnInfos, copCtx.ColumnInfos, copCtx.ExprCtx, chk)
	return false, err
}

func completeErr(err error, idxInfo *model.IndexInfo) error {
	if expression.ErrInvalidJSONForFuncIndex.Equal(err) {
		err = expression.ErrInvalidJSONForFuncIndex.GenWithStackByArgs(idxInfo.Name.O)
	}
	return errors.Trace(err)
}

func getRestoreData(tblInfo *model.TableInfo, targetIdx, pkIdx *model.IndexInfo, handleDts []types.Datum) []types.Datum {
	if !collate.NewCollationEnabled() || !tblInfo.IsCommonHandle || tblInfo.CommonHandleVersion == 0 {
		return nil
	}
	if pkIdx == nil {
		return nil
	}
	for i, pkIdxCol := range pkIdx.Columns {
		pkCol := tblInfo.Columns[pkIdxCol.Offset]
		if !types.NeedRestoredData(&pkCol.FieldType) {
			// Since the handle data cannot be null, we can use SetNull to
			// indicate that this column does not need to be restored.
			handleDts[i].SetNull()
			continue
		}
		tables.TryTruncateRestoredData(&handleDts[i], pkCol, pkIdxCol, targetIdx)
		tables.ConvertDatumToTailSpaceCount(&handleDts[i], pkCol)
	}
	dtToRestored := handleDts[:0]
	for _, handleDt := range handleDts {
		if !handleDt.IsNull() {
			dtToRestored = append(dtToRestored, handleDt)
		}
	}
	return dtToRestored
}

func buildDAGPB(exprCtx exprctx.BuildContext, distSQLCtx *distsqlctx.DistSQLContext, pushDownFlags uint64, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(exprCtx.GetEvalCtx().Location())
	dagReq.Flags = pushDownFlags
	for i := range colInfos {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	execPB, err := constructTableScanPB(exprCtx, tblInfo, colInfos)
	if err != nil {
		return nil, err
	}
	dagReq.Executors = append(dagReq.Executors, execPB)
	distsql.SetEncodeType(distSQLCtx, dagReq)
	return dagReq, nil
}

func constructTableScanPB(ctx exprctx.BuildContext, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.Executor, error) {
	tblScan := tables.BuildTableScanFromInfos(tblInfo, colInfos)
	tblScan.TableId = tblInfo.ID
	err := tables.SetPBColumnsDefaultValue(ctx, tblScan.Columns, colInfos)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}, err
}

func extractDatumByOffsets(ctx expression.EvalContext, row chunk.Row, offsets []int, expCols []*expression.Column, buf []types.Datum) []types.Datum {
	for i, offset := range offsets {
		c := expCols[offset]
		row.DatumWithBuffer(offset, c.GetType(ctx), &buf[i])
	}
	return buf
}

func buildHandle(pkDts []types.Datum, tblInfo *model.TableInfo,
	pkInfo *model.IndexInfo, loc *time.Location, errCtx errctx.Context) (kv.Handle, error) {
	if tblInfo.IsCommonHandle {
		tablecodec.TruncateIndexValues(tblInfo, pkInfo, pkDts)
		handleBytes, err := codec.EncodeKey(loc, nil, pkDts...)
		err = errCtx.HandleError(err)
		if err != nil {
			return nil, err
		}
		return kv.NewCommonHandle(handleBytes)
	}
	return kv.IntHandle(pkDts[0].GetInt64()), nil
}
