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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
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

func (c *copReqSenderPool) fetchRowColValsFromCop(handleRange reorgBackfillTask) ([]*indexRecord, *chunk.Chunk, kv.Key, bool, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case rs, ok := <-c.resultsCh:
			if !ok {
				logutil.BgLogger().Info("[ddl-ingest] cop-response channel is closed",
					zap.Int("id", handleRange.id), zap.String("task", handleRange.String()))
				return nil, nil, handleRange.endKey, true, nil
			}
			if rs.err != nil {
				return nil, nil, handleRange.startKey, false, rs.err
			}
			if rs.done {
				logutil.BgLogger().Info("[ddl-ingest] finish a cop-request task",
					zap.Int("id", rs.id), zap.Int("total", rs.total))
				c.results.Store(rs.id, struct{}{})
			}
			if _, found := c.results.Load(handleRange.id); found {
				logutil.BgLogger().Info("[ddl-ingest] task is found in results",
					zap.Int("id", handleRange.id), zap.String("task", handleRange.String()))
				c.results.Delete(handleRange.id)
				return rs.records, rs.chunk, handleRange.endKey, true, nil
			}
			return rs.records, rs.chunk, handleRange.startKey, false, nil
		case <-ticker.C:
			logutil.BgLogger().Info("[ddl-ingest] cop-request result channel is empty",
				zap.Int("id", handleRange.id))
			if _, found := c.results.Load(handleRange.id); found {
				c.results.Delete(handleRange.id)
				return nil, nil, handleRange.endKey, true, nil
			}
		}
	}
}

type copReqSenderPool struct {
	tasksCh   chan *reorgBackfillTask
	resultsCh chan idxRecResult
	results   generic.SyncMap[int, struct{}]

	ctx    context.Context
	copCtx *copContext
	store  kv.Storage

	senders []*copReqSender
	wg      sync.WaitGroup

	idxBufPool chan []*indexRecord
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
	var curTaskID int
	defer util.Recover(metrics.LabelDDL, "copReqSender.run", func() {
		p.resultsCh <- idxRecResult{id: curTaskID, err: dbterror.ErrReorgPanic}
	}, false)
	for {
		if util.HasCancelled(c.ctx) {
			return
		}
		task, ok := <-p.tasksCh
		if !ok {
			return
		}
		curTaskID = task.id
		logutil.BgLogger().Info("[ddl-ingest] start a cop-request task",
			zap.Int("id", task.id), zap.String("task", task.String()))
		ver, err := p.store.CurrentVersion(kv.GlobalTxnScope)
		if err != nil {
			p.resultsCh <- idxRecResult{id: task.id, err: err}
			return
		}
		rs, err := p.copCtx.buildTableScan(p.ctx, ver.Ver, task.startKey, task.excludedEndKey())
		if err != nil {
			p.resultsCh <- idxRecResult{id: task.id, err: err}
			return
		}
		failpoint.Inject("MockCopSenderPanic", func(val failpoint.Value) {
			if val.(bool) {
				panic("mock panic")
			}
		})
		var done bool
		var total int
		for !done {
			idxRec, srcChk := p.getIndexRecordsAndChunks()
			idxRec, done, err = p.copCtx.fetchTableScanResult(p.ctx, rs, srcChk, idxRec)
			if err != nil {
				p.resultsCh <- idxRecResult{id: task.id, err: err}
				p.recycleIdxRecordsAndChunk(idxRec, srcChk)
				terror.Call(rs.Close)
				return
			}
			total += len(idxRec)
			p.resultsCh <- idxRecResult{id: task.id, records: idxRec, chunk: srcChk, done: done, total: total}
		}
		terror.Call(rs.Close)
	}
}

func newCopReqSenderPool(ctx context.Context, copCtx *copContext, store kv.Storage) *copReqSenderPool {
	poolSize := copReadChunkPoolSize()
	idxBufPool := make(chan []*indexRecord, poolSize)
	srcChkPool := make(chan *chunk.Chunk, poolSize)
	for i := 0; i < poolSize; i++ {
		idxBufPool <- make([]*indexRecord, 0, copReadBatchSize())
		srcChkPool <- chunk.NewChunkWithCapacity(copCtx.fieldTps, copReadBatchSize())
	}
	return &copReqSenderPool{
		tasksCh:    make(chan *reorgBackfillTask, backfillTaskChanSize),
		resultsCh:  make(chan idxRecResult, backfillTaskChanSize),
		results:    generic.NewSyncMap[int, struct{}](10),
		ctx:        ctx,
		copCtx:     copCtx,
		store:      store,
		senders:    make([]*copReqSender, 0, variable.GetDDLReorgWorkerCounter()),
		wg:         sync.WaitGroup{},
		idxBufPool: idxBufPool,
		srcChkPool: srcChkPool,
	}
}

func (c *copReqSenderPool) sendTask(task *reorgBackfillTask) {
	c.tasksCh <- task
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

func (c *copReqSenderPool) close() {
	logutil.BgLogger().Info("[ddl-ingest] close cop-request sender pool", zap.Int("results not handled", len(c.results.Keys())))
	close(c.tasksCh)
	for _, w := range c.senders {
		w.cancel()
	}
	cleanupWg := util.WaitGroupWrapper{}
	cleanupWg.Run(c.drainResults)
	// Wait for all cop-req senders to exit.
	c.wg.Wait()
	close(c.resultsCh)
	cleanupWg.Wait()
	close(c.idxBufPool)
	close(c.srcChkPool)
}

func (c *copReqSenderPool) drainResults() {
	// Consume the rest results because the writers are inactive anymore.
	for rs := range c.resultsCh {
		c.recycleIdxRecordsAndChunk(rs.records, rs.chunk)
	}
}

func (c *copReqSenderPool) getIndexRecordsAndChunks() ([]*indexRecord, *chunk.Chunk) {
	ir := <-c.idxBufPool
	chk := <-c.srcChkPool
	newCap := copReadBatchSize()
	if chk.Capacity() != newCap {
		chk = chunk.NewChunkWithCapacity(c.copCtx.fieldTps, newCap)
	}
	chk.Reset()
	return ir[:0], chk
}

// recycleIdxRecordsAndChunk puts the index record slice and the chunk back to the pool for reuse.
func (c *copReqSenderPool) recycleIdxRecordsAndChunk(idxRecs []*indexRecord, chk *chunk.Chunk) {
	if idxRecs == nil || chk == nil {
		return
	}
	c.idxBufPool <- idxRecs
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

func newCopContext(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, sessCtx sessionctx.Context) (*copContext, error) {
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
	if err != nil {
		return nil, err
	}
	return distsql.Select(ctx, c.sessCtx, kvReq, c.fieldTps, statistics.NewQueryFeedback(0, nil, 0, false))
}

func (c *copContext) fetchTableScanResult(ctx context.Context, result distsql.SelectResult,
	chk *chunk.Chunk, buf []*indexRecord) ([]*indexRecord, bool, error) {
	sctx := c.sessCtx.GetSessionVars().StmtCtx
	err := result.Next(ctx, chk)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if chk.NumRows() == 0 {
		return buf, true, nil
	}
	iter := chunk.NewIterator4Chunk(chk)
	err = table.FillVirtualColumnValue(c.virtualColFieldTps, c.virtualColOffsets, c.expColInfos, c.colInfos, c.sessCtx, chk)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		idxDt := extractDatumByOffsets(row, c.idxColOutputOffsets, c.expColInfos)
		hdDt := extractDatumByOffsets(row, c.handleOutputOffsets, c.expColInfos)
		handle, err := buildHandle(hdDt, c.tblInfo, c.pkInfo, sctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		rsData := getRestoreData(c.tblInfo, c.idxInfo, c.pkInfo, hdDt)
		buf = append(buf, &indexRecord{handle: handle, key: nil, vals: idxDt, rsData: rsData, skip: false})
	}
	return buf, false, nil
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
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(sCtx.GetSessionVars().Location())
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

func extractDatumByOffsets(row chunk.Row, offsets []int, expCols []*expression.Column) []types.Datum {
	datumBuf := make([]types.Datum, 0, len(offsets))
	for _, offset := range offsets {
		c := expCols[offset]
		rowDt := row.GetDatum(offset, c.GetType())
		datumBuf = append(datumBuf, rowDt)
	}
	return datumBuf
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

type idxRecResult struct {
	id      int
	records []*indexRecord
	chunk   *chunk.Chunk
	err     error
	done    bool
	total   int
}
