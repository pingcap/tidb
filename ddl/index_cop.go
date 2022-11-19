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
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// copReadBatchFactor is the factor of batch size of coprocessor read.
// It multiplies the tidb_ddl_reorg_batch_size to avoid sending too many cop requests for the same handle range.
const copReadBatchFactor = 10

func (c *copReqSenderPool) fetchRowColValsFromCop(handleRange reorgBackfillTask) ([]*indexRecord, kv.Key, bool, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case rs, ok := <-c.resultsCh:
			if !ok {
				logutil.BgLogger().Info("[ddl-ingest] cop-response channel is closed",
					zap.Int("id", handleRange.id), zap.String("task", handleRange.String()))
				return nil, handleRange.endKey, true, nil
			}
			if rs.err != nil {
				return nil, handleRange.startKey, false, rs.err
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
				return rs.records, handleRange.endKey, true, nil
			}
			return rs.records, handleRange.startKey, false, nil
		case <-ticker.C:
			logutil.BgLogger().Info("[ddl-ingest] cop-request result channel is empty",
				zap.Int("id", handleRange.id))
			if _, found := c.results.Load(handleRange.id); found {
				c.results.Delete(handleRange.id)
				return nil, handleRange.endKey, true, nil
			}
		}
	}
}

type copReqSenderPool struct {
	tasksCh   chan *reorgBackfillTask
	resultsCh chan idxRecResult
	results   generic.SyncMap[int, struct{}]

	ctx     context.Context
	copCtx  *copContext
	startTS uint64

	senders []*copReqSender
	wg      sync.WaitGroup

	idxBufPool sync.Pool
}

type copReqSender struct {
	senderPool *copReqSenderPool

	ctx    context.Context
	cancel context.CancelFunc
}

func (c *copReqSender) run() {
	p := c.senderPool
	defer p.wg.Done()
	srcChk := renewChunk(nil, p.copCtx.fieldTps)
	for {
		if util.HasCancelled(c.ctx) {
			return
		}
		task, ok := <-p.tasksCh
		if !ok {
			return
		}
		logutil.BgLogger().Info("[ddl-ingest] start a cop-request task",
			zap.Int("id", task.id), zap.String("task", task.String()))
		rs, err := p.copCtx.buildTableScan(p.ctx, p.startTS, task.startKey, task.excludedEndKey())
		if err != nil {
			p.resultsCh <- idxRecResult{id: task.id, err: err}
			return
		}
		var done bool
		var total int
		for !done {
			idxRec := p.idxBufPool.Get().([]*indexRecord)
			idxRec = idxRec[:0]
			srcChk = renewChunk(srcChk, p.copCtx.fieldTps)
			idxRec, done, err = p.copCtx.fetchTableScanResult(p.ctx, rs, srcChk, idxRec)
			if err != nil {
				p.resultsCh <- idxRecResult{id: task.id, err: err}
				return
			}
			total += len(idxRec)
			p.resultsCh <- idxRecResult{id: task.id, records: idxRec, done: done, total: total}
		}
	}
}

// renewChunk creates a new chunk when the reorg batch size is changed.
func renewChunk(oldChk *chunk.Chunk, fts []*types.FieldType) *chunk.Chunk {
	newSize := variable.GetDDLReorgBatchSize()
	newCap := int(newSize) * copReadBatchFactor
	if oldChk == nil || oldChk.Capacity() != newCap {
		return chunk.NewChunkWithCapacity(fts, newCap)
	}
	oldChk.Reset()
	return oldChk
}

func newCopReqSenderPool(ctx context.Context, copCtx *copContext, startTS uint64) *copReqSenderPool {
	return &copReqSenderPool{
		tasksCh:   make(chan *reorgBackfillTask, backfillTaskChanSize),
		resultsCh: make(chan idxRecResult, backfillTaskChanSize),
		results:   generic.NewSyncMap[int, struct{}](10),
		ctx:       ctx,
		copCtx:    copCtx,
		startTS:   startTS,
		senders:   make([]*copReqSender, 0, variable.GetDDLReorgWorkerCounter()),
		wg:        sync.WaitGroup{},
		idxBufPool: sync.Pool{
			New: func() any {
				return make([]*indexRecord, 0, int(variable.GetDDLReorgBatchSize())*copReadBatchFactor)
			},
		},
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
	c.wg.Wait()
	close(c.resultsCh)
}

// recycleIdxRecords puts the index record slice back to the pool for reuse.
func (c *copReqSenderPool) recycleIdxRecords(idxRecs []*indexRecord) {
	if len(idxRecs) == 0 {
		return
	}
	c.idxBufPool.Put(idxRecs[:0])
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
}

func newCopContext(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, sessCtx sessionctx.Context) *copContext {
	colInfos := make([]*model.ColumnInfo, 0, len(idxInfo.Columns))
	fieldTps := make([]*types.FieldType, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		c := tblInfo.Columns[idxCol.Offset]
		if c.IsGenerated() && !c.GeneratedStored {
			// TODO(tangenta): support reading virtual generated columns.
			return nil
		}
		colInfos = append(colInfos, c)
		fieldTps = append(fieldTps, &c.FieldType)
	}

	pkColInfos, pkFieldTps, pkInfo := buildHandleColInfoAndFieldTypes(tblInfo)
	colInfos = append(colInfos, pkColInfos...)
	fieldTps = append(fieldTps, pkFieldTps...)

	copCtx := &copContext{
		tblInfo:  tblInfo,
		idxInfo:  idxInfo,
		pkInfo:   pkInfo,
		colInfos: colInfos,
		fieldTps: fieldTps,
		sessCtx:  sessCtx,
	}
	return copCtx
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
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		idxDt, hdDt := extractIdxValsAndHandle(row, c.idxInfo, c.fieldTps)
		handle, err := buildHandle(hdDt, c.tblInfo, c.pkInfo, sctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		rsData := tables.TryGetHandleRestoredDataWrapper(c.tblInfo, hdDt, nil, c.idxInfo)
		buf = append(buf, &indexRecord{handle: handle, key: nil, vals: idxDt, rsData: rsData, skip: false})
	}
	done := chk.NumRows() < chk.Capacity()
	return buf, done, nil
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

func buildHandleColInfoAndFieldTypes(tbInfo *model.TableInfo) ([]*model.ColumnInfo, []*types.FieldType, *model.IndexInfo) {
	if tbInfo.PKIsHandle {
		for i := range tbInfo.Columns {
			if mysql.HasPriKeyFlag(tbInfo.Columns[i].GetFlag()) {
				return []*model.ColumnInfo{tbInfo.Columns[i]}, []*types.FieldType{&tbInfo.Columns[i].FieldType}, nil
			}
		}
	} else if tbInfo.IsCommonHandle {
		primaryIdx := tables.FindPrimaryIndex(tbInfo)
		pkCols := make([]*model.ColumnInfo, 0, len(primaryIdx.Columns))
		pkFts := make([]*types.FieldType, 0, len(primaryIdx.Columns))
		for _, pkCol := range primaryIdx.Columns {
			pkCols = append(pkCols, tbInfo.Columns[pkCol.Offset])
			pkFts = append(pkFts, &tbInfo.Columns[pkCol.Offset].FieldType)
		}
		return pkCols, pkFts, primaryIdx
	}
	extra := model.NewExtraHandleColInfo()
	return []*model.ColumnInfo{extra}, []*types.FieldType{&extra.FieldType}, nil
}

func extractIdxValsAndHandle(row chunk.Row, idxInfo *model.IndexInfo, fieldTps []*types.FieldType) ([]types.Datum, []types.Datum) {
	datumBuf := make([]types.Datum, 0, len(fieldTps))
	idxColLen := len(idxInfo.Columns)
	for i, ft := range fieldTps {
		datumBuf = append(datumBuf, row.GetDatum(i, ft))
	}
	return datumBuf[:idxColLen], datumBuf[idxColLen:]
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
	err     error
	done    bool
	total   int
}
