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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/ingest"
	sess "github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
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
	copCtx *copContext
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
	for {
		if util.HasCancelled(c.ctx) {
			return
		}
		task, ok := <-p.tasksCh
		if !ok {
			return
		}
		if p.checkpointMgr != nil && p.checkpointMgr.IsComplete(task.endKey) {
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
		zap.Int("id", task.id), zap.String("task", task.String()))

	return wrapInBeginRollback(se, func(startTS uint64) error {
		rs, err := p.copCtx.buildTableScan(p.ctx, startTS, task.startKey, task.endKey)
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
		for !done {
			srcChk := p.getChunk()
			done, err = p.copCtx.fetchTableScanResult(p.ctx, rs, srcChk)
			if err != nil {
				p.recycleChunk(srcChk)
				terror.Call(rs.Close)
				return err
			}
			if p.checkpointMgr != nil {
				p.checkpointMgr.UpdateTotal(task.id, srcChk.NumRows(), done)
			}
			idxRs := IndexRecordChunk{ID: task.id, Chunk: srcChk, Done: done}
			failpoint.Inject("mockCopSenderError", func() {
				idxRs.Err = errors.New("mock cop error")
			})
			p.chunkSender.AddTask(idxRs)
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

func newCopReqSenderPool(ctx context.Context, copCtx *copContext, store kv.Storage,
	taskCh chan *reorgBackfillTask, sessPool *sess.Pool,
	checkpointMgr *ingest.CheckpointManager) *copReqSenderPool {
	poolSize := copReadChunkPoolSize()
	srcChkPool := make(chan *chunk.Chunk, poolSize)
	for i := 0; i < poolSize; i++ {
		srcChkPool <- chunk.NewChunkWithCapacity(copCtx.fieldTps, copReadBatchSize())
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
		chk = chunk.NewChunkWithCapacity(c.copCtx.fieldTps, newCap)
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

// copContext contains the information that is needed when building a coprocessor request.
// It is unchanged after initialization.
type copContext struct {
	tblInfo  *model.TableInfo
	idxInfo  *model.IndexInfo
	pkInfo   *model.IndexInfo
	colInfos []*model.ColumnInfo
	fieldTps []*types.FieldType
	sessCtx  sessionctx.Context

	expColInfos         []*expression.Column
	idxColOutputOffsets []int
	handleOutputOffsets []int
	virtualColOffsets   []int
	virtualColFieldTps  []*types.FieldType
}

// FieldTypes is only used for test.
// TODO(tangenta): refactor the operators to avoid using this method.
func (c *copContext) FieldTypes() []*types.FieldType {
	return c.fieldTps
}

// NewCopContext creates a copContext.
func NewCopContext(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, sessCtx sessionctx.Context) (*copContext, error) {
	var err error
	usedColumnIDs := make(map[int64]struct{}, len(idxInfo.Columns))
	usedColumnIDs, err = fillUsedColumns(usedColumnIDs, idxInfo, tblInfo)
	var handleIDs []int64
	if err != nil {
		return nil, err
	}
	var primaryIdx *model.IndexInfo
	if tblInfo.PKIsHandle {
		pkCol := tblInfo.GetPkColInfo()
		usedColumnIDs[pkCol.ID] = struct{}{}
		handleIDs = []int64{pkCol.ID}
	} else if tblInfo.IsCommonHandle {
		primaryIdx = tables.FindPrimaryIndex(tblInfo)
		handleIDs = make([]int64, 0, len(primaryIdx.Columns))
		for _, pkCol := range primaryIdx.Columns {
			col := tblInfo.Columns[pkCol.Offset]
			handleIDs = append(handleIDs, col.ID)
		}
		usedColumnIDs, err = fillUsedColumns(usedColumnIDs, primaryIdx, tblInfo)
		if err != nil {
			return nil, err
		}
	}

	// Only collect the columns that are used by the index.
	colInfos := make([]*model.ColumnInfo, 0, len(idxInfo.Columns))
	fieldTps := make([]*types.FieldType, 0, len(idxInfo.Columns))
	for i := range tblInfo.Columns {
		col := tblInfo.Columns[i]
		if _, found := usedColumnIDs[col.ID]; found {
			colInfos = append(colInfos, col)
			fieldTps = append(fieldTps, &col.FieldType)
		}
	}

	// Append the extra handle column when _tidb_rowid is used.
	if !tblInfo.HasClusteredIndex() {
		extra := model.NewExtraHandleColInfo()
		colInfos = append(colInfos, extra)
		fieldTps = append(fieldTps, &extra.FieldType)
		handleIDs = []int64{extra.ID}
	}

	expColInfos, _, err := expression.ColumnInfos2ColumnsAndNames(sessCtx,
		model.CIStr{} /* unused */, tblInfo.Name, colInfos, tblInfo)
	if err != nil {
		return nil, err
	}
	idxOffsets := resolveIndicesForIndex(expColInfos, idxInfo, tblInfo)
	hdColOffsets := resolveIndicesForHandle(expColInfos, handleIDs)
	vColOffsets, vColFts := collectVirtualColumnOffsetsAndTypes(expColInfos)

	copCtx := &copContext{
		tblInfo:  tblInfo,
		idxInfo:  idxInfo,
		pkInfo:   primaryIdx,
		colInfos: colInfos,
		fieldTps: fieldTps,
		sessCtx:  sessCtx,

		expColInfos:         expColInfos,
		idxColOutputOffsets: idxOffsets,
		handleOutputOffsets: hdColOffsets,
		virtualColOffsets:   vColOffsets,
		virtualColFieldTps:  vColFts,
	}
	return copCtx, nil
}

func fillUsedColumns(usedCols map[int64]struct{}, idxInfo *model.IndexInfo, tblInfo *model.TableInfo) (map[int64]struct{}, error) {
	colsToChecks := make([]*model.ColumnInfo, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		colsToChecks = append(colsToChecks, tblInfo.Columns[idxCol.Offset])
	}
	for len(colsToChecks) > 0 {
		next := colsToChecks[0]
		colsToChecks = colsToChecks[1:]
		usedCols[next.ID] = struct{}{}
		for depColName := range next.Dependences {
			// Expand the virtual generated columns.
			depCol := model.FindColumnInfo(tblInfo.Columns, depColName)
			if depCol == nil {
				return nil, errors.Trace(errors.Errorf("dependent column %s not found", depColName))
			}
			if _, ok := usedCols[depCol.ID]; !ok {
				colsToChecks = append(colsToChecks, depCol)
			}
		}
	}
	return usedCols, nil
}

func resolveIndicesForIndex(outputCols []*expression.Column, idxInfo *model.IndexInfo, tblInfo *model.TableInfo) []int {
	offsets := make([]int, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		hid := tblInfo.Columns[idxCol.Offset].ID
		for j, col := range outputCols {
			if col.ID == hid {
				offsets = append(offsets, j)
				break
			}
		}
	}
	return offsets
}

func resolveIndicesForHandle(cols []*expression.Column, handleIDs []int64) []int {
	offsets := make([]int, 0, len(handleIDs))
	for _, hid := range handleIDs {
		for j, col := range cols {
			if col.ID == hid {
				offsets = append(offsets, j)
				break
			}
		}
	}
	return offsets
}

func collectVirtualColumnOffsetsAndTypes(cols []*expression.Column) ([]int, []*types.FieldType) {
	var offsets []int
	var fts []*types.FieldType
	for i, col := range cols {
		if col.VirtualExpr != nil {
			offsets = append(offsets, i)
			fts = append(fts, col.GetType())
		}
	}
	return offsets, fts
}

func (c *copContext) buildTableScan(ctx context.Context, startTS uint64, start, end kv.Key) (distsql.SelectResult, error) {
	dagPB, err := buildDAGPB(c.sessCtx, c.tblInfo, c.colInfos)
	if err != nil {
		return nil, err
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagPB).
		SetStartTS(startTS).
		SetKeyRanges([]kv.KeyRange{{StartKey: start, EndKey: end}}).
		SetKeepOrder(true).
		SetFromSessionVars(c.sessCtx.GetSessionVars()).
		SetFromInfoSchema(c.sessCtx.GetDomainInfoSchema()).
		SetConcurrency(1).
		Build()
	kvReq.RequestSource.RequestSourceInternal = true
	kvReq.RequestSource.RequestSourceType = getDDLRequestSource(model.ActionAddIndex)
	kvReq.RequestSource.ExplicitRequestSourceType = kvutil.ExplicitTypeDDL
	if err != nil {
		return nil, err
	}
	return distsql.Select(ctx, c.sessCtx, kvReq, c.fieldTps)
}

func (c *copContext) fetchTableScanResult(ctx context.Context, result distsql.SelectResult,
	chk *chunk.Chunk) (bool, error) {
	err := result.Next(ctx, chk)
	if err != nil {
		return false, errors.Trace(err)
	}
	if chk.NumRows() == 0 {
		return true, nil
	}
	err = table.FillVirtualColumnValue(c.virtualColFieldTps, c.virtualColOffsets, c.expColInfos, c.colInfos, c.sessCtx, chk)
	if err != nil {
		return false, completeErr(err, c.idxInfo)
	}
	return false, nil
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

func buildDAGPB(sCtx sessionctx.Context, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	_, dagReq.TimeZoneOffset = timeutil.Zone(sCtx.GetSessionVars().Location())
	sc := sCtx.GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range colInfos {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	execPB, err := constructTableScanPB(sCtx, tblInfo, colInfos)
	if err != nil {
		return nil, err
	}
	dagReq.Executors = append(dagReq.Executors, execPB)
	distsql.SetEncodeType(sCtx, dagReq)
	return dagReq, nil
}

func constructTableScanPB(sCtx sessionctx.Context, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.Executor, error) {
	tblScan := tables.BuildTableScanFromInfos(tblInfo, colInfos)
	tblScan.TableId = tblInfo.ID
	err := tables.SetPBColumnsDefaultValue(sCtx, tblScan.Columns, colInfos)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}, err
}

func extractDatumByOffsets(row chunk.Row, offsets []int, expCols []*expression.Column, buf []types.Datum) []types.Datum {
	for _, offset := range offsets {
		c := expCols[offset]
		rowDt := row.GetDatum(offset, c.GetType())
		buf = append(buf, rowDt)
	}
	return buf
}

func buildHandle(pkDts []types.Datum, tblInfo *model.TableInfo,
	pkInfo *model.IndexInfo, stmtCtx *stmtctx.StatementContext) (kv.Handle, error) {
	if tblInfo.IsCommonHandle {
		tablecodec.TruncateIndexValues(tblInfo, pkInfo, pkDts)
		handleBytes, err := codec.EncodeKey(stmtCtx, nil, pkDts...)
		if err != nil {
			return nil, err
		}
		return kv.NewCommonHandle(handleBytes)
	}
	return kv.IntHandle(pkDts[0].GetInt64()), nil
}
