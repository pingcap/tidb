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
	"fmt"
	"sync"
	"sync/atomic"
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
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

type copContext struct {
	tblInfo          *model.TableInfo
	idxInfo          *model.IndexInfo
	colInfos         []*model.ColumnInfo
	fieldTps         []*types.FieldType
	sessCtx          sessionctx.Context
	pushDownEncoding bool

	srcChunks       []*chunk.Chunk
	indexRecordChan chan *indexRecord
	doneChan        chan struct{}
	bfTasks         map[string]struct{}
	err             atomic.Value
	readerCnt       atomic.Int32
	mu              sync.Mutex
}

type copReqReaders struct {
	tasksCh chan *reorgBackfillTask
	readers []*copReqReader
}

func (p *copReqReaders) getReader(task *reorgBackfillTask) *copReqReader {
	for {
		for _, r := range p.readers {
			r.mu.Lock()
			if string(r.currentTask.endKey) == string(task.endKey) {
				r.mu.Unlock()
				return r
			}
			r.mu.Unlock()
		}
		logutil.BgLogger().Info("[ddl] coprocessor reader not found, wait a while",
			zap.String("task", task.String()))
		time.Sleep(time.Millisecond * 300)
	}
}

type copReqReader struct {
	id            int
	traceID       int64
	copCtx        *copContext
	idxRecordChan chan *indexRecord
	srcChunk      *chunk.Chunk
	err           error
	done          chan struct{}
	currentTask   *reorgBackfillTask
	mu            sync.Mutex
}

func (c *copReqReader) run(ctx context.Context, tasks chan *reorgBackfillTask) {
	for {
		select {
		case task, ok := <-tasks:
			if !ok {
				return
			}
			c.mu.Lock()
			c.currentTask = task
			c.idxRecordChan = make(chan *indexRecord, variable.MaxDDLReorgBatchSize)
			c.mu.Unlock()
			finish := injectSpan(c.traceID, fmt.Sprintf("%s-%d", "fetch-rows", c.id))
			err := kv.RunInNewTxn(ctx, c.copCtx.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
				if c.copCtx.pushDownEncoding {
					return c.copCtx.sendEncodedIdxRecords(ctx, c.idxRecordChan, txn, task.startKey, task.excludedEndKey())
				} else {
					return c.copCtx.sendIdxRecords(ctx, c.idxRecordChan, c.srcChunk, txn, task.startKey, task.excludedEndKey())
				}
			})
			finish()
			c.mu.Lock()
			c.err = err
			c.mu.Unlock()
			close(c.idxRecordChan)
			<-c.done
		}
	}
}

func newCopReqReaders(ctx context.Context, copCtx *copContext, jobID int64, readerCnt int, tasks chan *reorgBackfillTask) *copReqReaders {
	p := &copReqReaders{
		tasksCh: tasks,
	}
	for i := 0; i < readerCnt; i++ {
		r := &copReqReader{
			id:            i,
			traceID:       jobID,
			copCtx:        copCtx,
			idxRecordChan: make(chan *indexRecord, variable.MaxDDLReorgBatchSize),
			srcChunk:      chunk.NewChunkWithCapacity(copCtx.fieldTps, 1024),
			err:           nil,
			done:          make(chan struct{}),
			currentTask:   nil,
			mu:            sync.Mutex{},
		}
		p.readers = append(p.readers, r)
		go r.run(ctx, tasks)
	}
	return p
}

func newCopContext(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, sessCtx sessionctx.Context) *copContext {
	colInfos := make([]*model.ColumnInfo, 0, len(idxInfo.Columns))
	fieldTps := make([]*types.FieldType, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		c := tblInfo.Columns[idxCol.Offset]
		colInfos = append(colInfos, c)
		fieldTps = append(fieldTps, &c.FieldType)
	}

	pkColInfos, pkFieldTps := buildHandleColInfoAndFieldTypes(tblInfo)
	colInfos = append(colInfos, pkColInfos...)
	fieldTps = append(fieldTps, pkFieldTps...)

	return &copContext{
		tblInfo:          tblInfo,
		idxInfo:          idxInfo,
		colInfos:         colInfos,
		fieldTps:         fieldTps,
		sessCtx:          sessCtx,
		pushDownEncoding: variable.EnableCoprRead.Load() == "2",
		indexRecordChan:  make(chan *indexRecord, variable.MaxDDLReorgBatchSize),
		doneChan:         make(chan struct{}, 1),
		bfTasks:          make(map[string]struct{}, 16),
	}
}

func (c *copContext) buildTableScan(ctx context.Context, txn kv.Transaction, start, end kv.Key) (distsql.SelectResult, error) {
	dagPB, err := buildDAGPB(c.sessCtx, c.tblInfo, c.colInfos)
	if err != nil {
		return nil, err
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeyRanges([]kv.KeyRange{{StartKey: start, EndKey: end}}).
		SetKeepOrder(true).
		SetFromSessionVars(c.sessCtx.GetSessionVars()).
		SetFromInfoSchema(c.sessCtx.GetDomainInfoSchema()).
		Build()
	if err != nil {
		return nil, err
	}

	kvReq.Concurrency = 1
	return distsql.Select(ctx, c.sessCtx, kvReq, c.fieldTps, statistics.NewQueryFeedback(0, nil, 0, false))
}

func buildDDLPB(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, colInfos []*model.ColumnInfo) (*tipb.DDLRequest, error) {
	ddlReq := &tipb.DDLRequest{}
	ddlReq.TableInfo = new(tipb.TableInfo)
	ddlReq.IndexInfo = new(tipb.IndexInfo)
	ddlReq.TableInfo.TableId = tblInfo.ID
	ddlReq.TableInfo.Columns = util.ColumnsToProto(colInfos, tblInfo.PKIsHandle)
	ddlReq.IndexInfo.TableId = tblInfo.ID
	ddlReq.IndexInfo.IndexId = idxInfo.ID
	indexColInfos := make([]*model.ColumnInfo, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		indexColInfos = append(indexColInfos, tblInfo.Cols()[idxCol.Offset])
	}
	ddlReq.IndexInfo.Columns = util.ColumnsToProto(indexColInfos, tblInfo.PKIsHandle)
	ddlReq.Columns = ddlReq.TableInfo.Columns
	ddlReq.IndexInfo.Unique = idxInfo.Unique

	return ddlReq, nil
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
	err := setPBColumnsDefaultValue(sCtx, tblScan.Columns, colInfos)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}, err
}

func (c *copContext) buildScanIndexKV(ctx context.Context, txn kv.Transaction, start, end kv.Key) (kv.Response, error) {
	ddlPB, err := buildDDLPB(c.tblInfo, c.idxInfo, c.colInfos)
	if err != nil {
		return nil, err
	}

	ddlPB.Ranges = append(ddlPB.Ranges, tipb.KeyRange{Low: start, High: end})

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDDLRequest(ddlPB).
		SetStartTS(txn.StartTS()).
		SetKeyRanges([]kv.KeyRange{{StartKey: start, EndKey: end}}).
		SetKeepOrder(true).
		SetFromSessionVars(c.sessCtx.GetSessionVars()).
		SetFromInfoSchema(c.sessCtx.GetDomainInfoSchema()).
		Build()
	if err != nil {
		return nil, err
	}

	kvReq.Concurrency = 1
	option := &kv.ClientSendOption{
		SessionMemTracker: c.sessCtx.GetSessionVars().StmtCtx.MemTracker,
	}

	resp := c.sessCtx.GetClient().Send(ctx, kvReq, c.sessCtx.GetSessionVars().KVVars, option)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	return resp, nil
}

func (c *copContext) sendIdxRecords(ctx context.Context, ch chan *indexRecord, srcChk *chunk.Chunk,
	txn kv.Transaction, start, end kv.Key) error {
	sctx := c.sessCtx.GetSessionVars().StmtCtx
	srcResult, err := c.buildTableScan(ctx, txn, start, end)
	if err != nil {
		return errors.Trace(err)
	}
	for {
		err := srcResult.Next(ctx, srcChk)
		if err != nil {
			return errors.Trace(err)
		}
		if srcChk.NumRows() == 0 {
			return nil
		}
		iter := chunk.NewIterator4Chunk(srcChk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			idxDt, hdDt := extractIdxValsAndHandle(row, c.idxInfo, c.fieldTps)
			handle, err := buildHandle(hdDt, c.tblInfo, c.idxInfo, sctx)
			if err != nil {
				return errors.Trace(err)
			}
			rsData := tables.TryGetHandleRestoredDataWrapper(c.tblInfo, hdDt, nil, c.idxInfo)
			ch <- &indexRecord{handle: handle, key: nil, vals: idxDt, rsData: rsData, skip: false}
		}
	}
}

func (c *copContext) sendEncodedIdxRecords(ctx context.Context, ch chan *indexRecord, txn kv.Transaction, start, end kv.Key) error {
	resp, err := c.buildScanIndexKV(ctx, txn, start, end)
	if err != nil {
		return errors.Trace(err)
	}
	for {
		data, err := resp.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if data == nil {
			return nil
		}
		colResp := &tipb.DDLResponse{}
		if err = colResp.Unmarshal(data.GetData()); err != nil {
			return errors.Trace(err)
		}
		for i := 0; i < len(colResp.Keys); i++ {
			ch <- &indexRecord{idxKV: &indexKV{key: colResp.Keys[i], value: colResp.Values[i]}}
		}
	}
}

func (w *addIndexWorker) fetchRowColValsFromCop(handleRange reorgBackfillTask) ([]*indexRecord, kv.Key, bool, error) {
	copReader := w.copReqReaders.getReader(&handleRange)
	w.idxRecords = w.idxRecords[:0]
	taskDone := false
	for {
		select {
		case record, more := <-copReader.idxRecordChan:
			if !more {
				taskDone = true
				break
			}
			w.idxRecords = append(w.idxRecords, record)
		}
		if len(w.idxRecords) >= w.batchCnt {
			break
		}
		if taskDone {
			copReader.done <- struct{}{}
			break
		}
	}
	return w.idxRecords, handleRange.startKey, taskDone, copReader.err
}

func buildHandleColInfoAndFieldTypes(tbInfo *model.TableInfo) ([]*model.ColumnInfo, []*types.FieldType) {
	if tbInfo.PKIsHandle {
		for i := range tbInfo.Columns {
			if mysql.HasPriKeyFlag(tbInfo.Columns[i].GetFlag()) {
				return []*model.ColumnInfo{tbInfo.Columns[i]}, []*types.FieldType{&tbInfo.Columns[i].FieldType}
			}
		}
	} else if tbInfo.IsCommonHandle {
		primaryIdx := tables.FindPrimaryIndex(tbInfo)
		pkCols := make([]*model.ColumnInfo, 0, len(primaryIdx.Columns))
		pkFts := make([]*types.FieldType, 0, len(primaryIdx.Columns))
		for i := range tbInfo.Columns {
			pkCols = append(pkCols, tbInfo.Columns[i])
			pkFts = append(pkFts, &tbInfo.Columns[i].FieldType)
		}
		return pkCols, pkFts
	}
	extra := model.NewExtraHandleColInfo()
	return []*model.ColumnInfo{extra}, []*types.FieldType{&extra.FieldType}
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
	idxInfo *model.IndexInfo, stmtCtx *stmtctx.StatementContext) (kv.Handle, error) {
	if tblInfo.IsCommonHandle {
		tablecodec.TruncateIndexValues(tblInfo, idxInfo, pkDts)
		handleBytes, err := codec.EncodeKey(stmtCtx, nil, pkDts...)
		if err != nil {
			return nil, err
		}
		return kv.NewCommonHandle(handleBytes)
	}
	return kv.IntHandle(pkDts[0].GetInt64()), nil
}

// setPBColumnsDefaultValue sets the default values of tipb.ColumnInfos.
func setPBColumnsDefaultValue(ctx sessionctx.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that TiKV will return NULL properly,
		// They real values will be compute later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbColumns[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.GetOriginDefaultValue() == nil {
			continue
		}

		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, c)
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			return err
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(sessVars.StmtCtx, nil, d)
		if err != nil {
			return err
		}
	}
	return nil
}
