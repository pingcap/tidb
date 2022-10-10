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
	"sync/atomic"

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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

type copContext struct {
	colInfos        []*model.ColumnInfo
	fieldTps        []*types.FieldType
	srcChunk        *chunk.Chunk
	indexRecordChan chan *indexRecord
	bfTasks         map[string]struct{}
	err             atomic.Value
}

func (c *copContext) isNewTask(task reorgBackfillTask) bool {
	_, found := c.bfTasks[string(task.endKey)]
	return !found
}

func (c *copContext) recordTask(task reorgBackfillTask) {
	c.bfTasks[string(task.endKey)] = struct{}{}
}

func newCopContext(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) *copContext {
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
		colInfos: colInfos,
		fieldTps: fieldTps,
		srcChunk: chunk.NewChunkWithCapacity(fieldTps, 1),
		bfTasks:  make(map[string]struct{}, 16),
	}
}

func (w *addIndexWorker) buildTableScan(ctx context.Context, txn kv.Transaction, start, end kv.Key) (distsql.SelectResult, error) {
	dagPB, err := w.buildDAGPB(w.coprCtx.colInfos)
	if err != nil {
		return nil, err
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeyRanges([]kv.KeyRange{{StartKey: start, EndKey: end}}).
		SetKeepOrder(true).
		SetFromSessionVars(w.sessCtx.GetSessionVars()).
		SetFromInfoSchema(w.sessCtx.GetDomainInfoSchema()).
		Build()
	if err != nil {
		return nil, err
	}

	kvReq.Concurrency = 1
	return distsql.Select(ctx, w.sessCtx, kvReq, w.coprCtx.fieldTps, statistics.NewQueryFeedback(0, nil, 0, false))
}

func (w *addIndexWorker) buildDAGPB(colInfos []*model.ColumnInfo) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(w.sessCtx.GetSessionVars().Location())
	sc := w.sessCtx.GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range colInfos {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	execPB, err := w.constructTableScanPB(w.table.Meta(), colInfos)
	if err != nil {
		return nil, err
	}
	dagReq.Executors = append(dagReq.Executors, execPB)
	distsql.SetEncodeType(w.sessCtx, dagReq)
	return dagReq, nil
}

func (w *addIndexWorker) constructTableScanPB(tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.Executor, error) {
	tblScan := tables.BuildTableScanFromInfos(tblInfo, colInfos)
	tblScan.TableId = w.table.Meta().ID
	err := setPBColumnsDefaultValue(w.sessCtx, tblScan.Columns, colInfos)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}, err
}

func (w *addIndexWorker) fetchRowColValsFromSelect(ctx context.Context, txn kv.Transaction,
	handleRange reorgBackfillTask) ([]*indexRecord, kv.Key, bool, error) {
	sctx := w.sessCtx.GetSessionVars().StmtCtx
	if w.coprCtx.isNewTask(handleRange) {
		w.coprCtx.recordTask(handleRange)
		w.coprCtx.indexRecordChan = make(chan *indexRecord, variable.MaxDDLReorgBatchSize)
		go func() {
			defer injectSpan(w.reorgInfo.Job.ID, fmt.Sprintf("%s-%d", "fetch-rows", w.id))()
			defer close(w.coprCtx.indexRecordChan)
			srcResult, err := w.buildTableScan(w.jobContext.ddlJobCtx, txn, handleRange.startKey, handleRange.excludedEndKey())
			if err != nil {
				w.coprCtx.err.Store(err)
				return
			}
			srcChunk := w.coprCtx.srcChunk
			for {
				err := srcResult.Next(ctx, srcChunk)
				if err != nil {
					w.coprCtx.err.Store(err)
					return
				}
				if srcChunk.NumRows() == 0 {
					return
				}
				iter := chunk.NewIterator4Chunk(srcChunk)
				for row := iter.Begin(); row != iter.End(); row = iter.Next() {
					idxDt, hdDt := extractIdxValsAndHandle(row, w.index.Meta(), w.coprCtx.fieldTps)
					handle, err := buildHandle(hdDt, w.table.Meta(), w.index.Meta(), sctx)
					if err != nil {
						w.coprCtx.err.Store(err)
						return
					}
					rsData := tables.TryGetHandleRestoredDataWrapper(w.table, hdDt, nil, w.index.Meta())
					w.coprCtx.indexRecordChan <- &indexRecord{handle: handle, key: nil, vals: idxDt, rsData: rsData, skip: false}
				}
			}
		}()
	}
	w.idxRecords = w.idxRecords[:0]
	taskDone := false
	var current kv.Handle
	for {
		record, ok := <-w.coprCtx.indexRecordChan
		if !ok { // The channel is closed.
			taskDone = true
			break
		}
		w.idxRecords = append(w.idxRecords, record)
		current = record.handle
		if len(w.idxRecords) >= w.batchCnt {
			break
		}
	}
	nextKey := handleRange.endKey
	if current != nil {
		nextKey = tablecodec.EncodeRecordKey(w.table.RecordPrefix(), current).Next()
	}
	err := w.coprCtx.err.Load()
	if err != nil {
		return nil, nil, false, err.(error)
	}
	return w.idxRecords, nextKey, taskDone, nil
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
