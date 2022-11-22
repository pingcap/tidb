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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

// copReadBatchFactor is the factor of batch size of coprocessor read.
// It multiplies the tidb_ddl_reorg_batch_size to avoid sending too many cop requests for the same handle range.
const copReadBatchFactor = 10

func (w *addIndexWorker) fetchRowColValsFromCop(txn kv.Transaction, handleRange reorgBackfillTask) ([]*indexRecord, kv.Key, bool, error) {
	w.idxRecords = w.idxRecords[:0]
	start, end := handleRange.startKey, handleRange.excludedEndKey()
	batchCnt := w.batchCnt * copReadBatchFactor
	return fetchRowsFromCop(w.ctx, w.copCtx, start, end, txn.StartTS(), w.idxRecords, batchCnt)
}

// fetchRowsFromCop sends a coprocessor request and fetches the first batchCnt rows.
func fetchRowsFromCop(ctx context.Context, copCtx *copContext, startKey, endKey kv.Key, startTS uint64,
	buf []*indexRecord, batchCnt int) ([]*indexRecord, kv.Key, bool, error) {
	srcResult, err := copCtx.buildTableScan(ctx, startTS, startKey, endKey)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}
	var done bool
	buf, done, err = copCtx.fetchTableScanResult(ctx, srcResult, buf, batchCnt)
	nextKey := endKey
	if !done {
		lastHandle := buf[len(buf)-1].handle
		prefix := tablecodec.GenTableRecordPrefix(copCtx.tblInfo.ID)
		nextKey = tablecodec.EncodeRecordKey(prefix, lastHandle).Next()
	}
	return buf, nextKey, done, err
}

type copContext struct {
	tblInfo  *model.TableInfo
	idxInfo  *model.IndexInfo
	pkInfo   *model.IndexInfo
	colInfos []*model.ColumnInfo
	fieldTps []*types.FieldType
	sessCtx  sessionctx.Context

	srcChunk *chunk.Chunk
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
		srcChunk: chunk.NewChunkWithCapacity(fieldTps, variable.DefMaxChunkSize),
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
	buf []*indexRecord, batchCnt int) ([]*indexRecord, bool, error) {
	sctx := c.sessCtx.GetSessionVars().StmtCtx
	for {
		err := result.Next(ctx, c.srcChunk)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if c.srcChunk.NumRows() == 0 {
			return buf, true, nil
		}
		iter := chunk.NewIterator4Chunk(c.srcChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			idxDt, hdDt := extractIdxValsAndHandle(row, c.idxInfo, c.fieldTps)
			handle, err := buildHandle(hdDt, c.tblInfo, c.pkInfo, sctx)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			rsData := tables.TryGetHandleRestoredDataWrapper(c.tblInfo, hdDt, nil, c.idxInfo)
			buf = append(buf, &indexRecord{handle: handle, key: nil, vals: idxDt, rsData: rsData, skip: false})
			if len(buf) >= batchCnt {
				return buf, false, nil
			}
		}
	}
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
