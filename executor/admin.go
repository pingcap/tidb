// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ Executor = &CheckIndexRangeExec{}
	_ Executor = &RecoverIndexExec{}
	_ Executor = &CleanupIndexExec{}
)

// CheckIndexRangeExec outputs the index values which has handle between begin and end.
type CheckIndexRangeExec struct {
	baseExecutor

	table    *model.TableInfo
	index    *model.IndexInfo
	is       infoschema.InfoSchema
	startKey []types.Datum

	handleRanges []ast.HandleRange
	srcChunk     *chunk.Chunk

	result distsql.SelectResult
	cols   []*model.ColumnInfo
}

// Next implements the Executor Next interface.
func (e *CheckIndexRangeExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	handleIdx := e.schema.Len() - 1
	for {
		err := e.result.Next(ctx, e.srcChunk)
		if err != nil {
			return err
		}
		if e.srcChunk.NumRows() == 0 {
			return nil
		}
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle := row.GetInt64(handleIdx)
			for _, hr := range e.handleRanges {
				if handle >= hr.Begin && handle < hr.End {
					req.AppendRow(row)
					break
				}
			}
		}
		if req.NumRows() > 0 {
			return nil
		}
	}
}

// Open implements the Executor Open interface.
func (e *CheckIndexRangeExec) Open(ctx context.Context) error {
	tCols := e.table.Cols()
	for _, ic := range e.index.Columns {
		col := tCols[ic.Offset]
		e.cols = append(e.cols, col)
	}

	colTypeForHandle := e.schema.Columns[len(e.cols)].RetType
	e.cols = append(e.cols, &model.ColumnInfo{
		ID:        model.ExtraHandleID,
		Name:      model.ExtraHandleName,
		FieldType: *colTypeForHandle,
	})

	e.srcChunk = newFirstChunk(e)
	dagPB, err := e.buildDAGPB()
	if err != nil {
		return err
	}
	sc := e.ctx.GetSessionVars().StmtCtx
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return nil
	}
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(sc, e.table.ID, e.index.ID, ranger.FullRange()).
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return err
	}

	e.result, err = distsql.Select(ctx, e.ctx, kvReq, e.retFieldTypes, statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return err
	}
	e.result.Fetch(ctx)
	return nil
}

func (e *CheckIndexRangeExec) buildDAGPB() (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetSessionVars().Location())
	sc := e.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range e.schema.Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	execPB := e.constructIndexScanPB()
	dagReq.Executors = append(dagReq.Executors, execPB)

	err := plannercore.SetPBColumnsDefaultValue(e.ctx, dagReq.Executors[0].IdxScan.Columns, e.cols)
	if err != nil {
		return nil, err
	}
	distsql.SetEncodeType(e.ctx, dagReq)
	return dagReq, nil
}

func (e *CheckIndexRangeExec) constructIndexScanPB() *tipb.Executor {
	idxExec := &tipb.IndexScan{
		TableId: e.table.ID,
		IndexId: e.index.ID,
		Columns: util.ColumnsToProto(e.cols, e.table.PKIsHandle),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

// Close implements the Executor Close interface.
func (e *CheckIndexRangeExec) Close() error {
	return nil
}

// RecoverIndexExec represents a recover index executor.
// It is built from "admin recover index" statement, is used to backfill
// corrupted index.
type RecoverIndexExec struct {
	baseExecutor

	done bool

	index     table.Index
	table     table.Table
	batchSize int

	columns       []*model.ColumnInfo
	colFieldTypes []*types.FieldType
	srcChunk      *chunk.Chunk

	// below buf is used to reduce allocations.
	recoverRows []recoverRows
	idxValsBufs [][]types.Datum
	idxKeyBufs  [][]byte
	batchKeys   []kv.Key
}

func (e *RecoverIndexExec) columnsTypes() []*types.FieldType {
	if e.colFieldTypes != nil {
		return e.colFieldTypes
	}

	e.colFieldTypes = make([]*types.FieldType, 0, len(e.columns))
	for _, col := range e.columns {
		e.colFieldTypes = append(e.colFieldTypes, &col.FieldType)
	}
	return e.colFieldTypes
}

// Open implements the Executor Open interface.
func (e *RecoverIndexExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.srcChunk = chunk.New(e.columnsTypes(), e.initCap, e.maxChunkSize)
	e.batchSize = 2048
	e.recoverRows = make([]recoverRows, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Datum, e.batchSize)
	e.idxKeyBufs = make([][]byte, e.batchSize)
	return nil
}

func (e *RecoverIndexExec) constructTableScanPB(pbColumnInfos []*tipb.ColumnInfo) *tipb.Executor {
	tblScan := &tipb.TableScan{
		TableId: e.table.Meta().ID,
		Columns: pbColumnInfos,
	}

	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}
}

func (e *RecoverIndexExec) constructLimitPB(count uint64) *tipb.Executor {
	limitExec := &tipb.Limit{
		Limit: count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

func (e *RecoverIndexExec) buildDAGPB(txn kv.Transaction, limitCnt uint64) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetSessionVars().Location())
	sc := e.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range e.columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	tblInfo := e.table.Meta()
	pbColumnInfos := util.ColumnsToProto(e.columns, tblInfo.PKIsHandle)
	err := plannercore.SetPBColumnsDefaultValue(e.ctx, pbColumnInfos, e.columns)
	if err != nil {
		return nil, err
	}
	tblScanExec := e.constructTableScanPB(pbColumnInfos)
	dagReq.Executors = append(dagReq.Executors, tblScanExec)

	limitExec := e.constructLimitPB(limitCnt)
	dagReq.Executors = append(dagReq.Executors, limitExec)
	distsql.SetEncodeType(e.ctx, dagReq)
	return dagReq, nil
}

func (e *RecoverIndexExec) buildTableScan(ctx context.Context, txn kv.Transaction, t table.Table, startHandle int64, limitCnt uint64) (distsql.SelectResult, error) {
	dagPB, err := e.buildDAGPB(txn, limitCnt)
	if err != nil {
		return nil, err
	}
	tblInfo := e.table.Meta()
	ranges := []*ranger.Range{{LowVal: []types.Datum{types.NewIntDatum(startHandle)}, HighVal: []types.Datum{types.NewIntDatum(math.MaxInt64)}}}
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(tblInfo.ID, ranges, nil).
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, err
	}

	// Actually, with limitCnt, the match datas maybe only in one region, so let the concurrency to be 1,
	// avoid unnecessary region scan.
	kvReq.Concurrency = 1
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.columnsTypes(), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, nil
}

type backfillResult struct {
	nextHandle   int64
	addedCount   int64
	scanRowCount int64
}

func (e *RecoverIndexExec) backfillIndex(ctx context.Context) (int64, int64, error) {
	var (
		nextHandle    = int64(math.MinInt64)
		totalAddedCnt = int64(0)
		totalScanCnt  = int64(0)
		lastLogCnt    = int64(0)
		result        backfillResult
	)
	for {
		errInTxn := kv.RunInNewTxn(e.ctx.GetStore(), true, func(txn kv.Transaction) error {
			var err error
			result, err = e.backfillIndexInTxn(ctx, txn, nextHandle)
			return err
		})
		if errInTxn != nil {
			return totalAddedCnt, totalScanCnt, errInTxn
		}
		totalAddedCnt += result.addedCount
		totalScanCnt += result.scanRowCount
		if totalScanCnt-lastLogCnt >= 50000 {
			lastLogCnt = totalScanCnt
			logutil.Logger(ctx).Info("recover index", zap.String("table", e.table.Meta().Name.O),
				zap.String("index", e.index.Meta().Name.O), zap.Int64("totalAddedCnt", totalAddedCnt),
				zap.Int64("totalScanCnt", totalScanCnt), zap.Int64("nextHandle", result.nextHandle))
		}

		// no more rows
		if result.scanRowCount == 0 {
			break
		}
		nextHandle = result.nextHandle
	}
	return totalAddedCnt, totalScanCnt, nil
}

type recoverRows struct {
	handle  int64
	idxVals []types.Datum
	skip    bool
}

func (e *RecoverIndexExec) fetchRecoverRows(ctx context.Context, srcResult distsql.SelectResult, result *backfillResult) ([]recoverRows, error) {
	e.recoverRows = e.recoverRows[:0]
	handleIdx := len(e.columns) - 1
	result.scanRowCount = 0

	for {
		err := srcResult.Next(ctx, e.srcChunk)
		if err != nil {
			return nil, err
		}

		if e.srcChunk.NumRows() == 0 {
			break
		}
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			if result.scanRowCount >= int64(e.batchSize) {
				return e.recoverRows, nil
			}
			handle := row.GetInt64(handleIdx)
			idxVals := extractIdxVals(row, e.idxValsBufs[result.scanRowCount], e.colFieldTypes)
			e.idxValsBufs[result.scanRowCount] = idxVals
			e.recoverRows = append(e.recoverRows, recoverRows{handle: handle, idxVals: idxVals, skip: false})
			result.scanRowCount++
			result.nextHandle = handle + 1
		}
	}

	return e.recoverRows, nil
}

func (e *RecoverIndexExec) batchMarkDup(txn kv.Transaction, rows []recoverRows) error {
	if len(rows) == 0 {
		return nil
	}
	e.batchKeys = e.batchKeys[:0]
	sc := e.ctx.GetSessionVars().StmtCtx
	distinctFlags := make([]bool, len(rows))
	for i, row := range rows {
		idxKey, distinct, err := e.index.GenIndexKey(sc, row.idxVals, row.handle, e.idxKeyBufs[i])
		if err != nil {
			return err
		}
		e.idxKeyBufs[i] = idxKey

		e.batchKeys = append(e.batchKeys, idxKey)
		distinctFlags[i] = distinct
	}

	values, err := txn.BatchGet(context.Background(), e.batchKeys)
	if err != nil {
		return err
	}

	// 1. unique-key is duplicate and the handle is equal, skip it.
	// 2. unique-key is duplicate and the handle is not equal, data is not consistent, log it and skip it.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range e.batchKeys {
		if val, found := values[string(key)]; found {
			if distinctFlags[i] {
				handle, err1 := tables.DecodeHandle(val)
				if err1 != nil {
					return err1
				}

				if handle != rows[i].handle {
					logutil.BgLogger().Warn("recover index: the constraint of unique index is broken, handle in index is not equal to handle in table",
						zap.String("index", e.index.Meta().Name.O), zap.ByteString("indexKey", key),
						zap.Int64("handleInTable", rows[i].handle), zap.Int64("handleInIndex", handle))
				}
			}
			rows[i].skip = true
		}
	}
	return nil
}

func (e *RecoverIndexExec) backfillIndexInTxn(ctx context.Context, txn kv.Transaction, startHandle int64) (result backfillResult, err error) {
	result.nextHandle = startHandle
	srcResult, err := e.buildTableScan(ctx, txn, e.table, startHandle, uint64(e.batchSize))
	if err != nil {
		return result, err
	}
	defer terror.Call(srcResult.Close)

	rows, err := e.fetchRecoverRows(ctx, srcResult, &result)
	if err != nil {
		return result, err
	}

	err = e.batchMarkDup(txn, rows)
	if err != nil {
		return result, err
	}

	// Constrains is already checked.
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = true
	for _, row := range rows {
		if row.skip {
			continue
		}

		recordKey := e.table.RecordKey(row.handle)
		err := txn.LockKeys(ctx, new(kv.LockCtx), recordKey)
		if err != nil {
			return result, err
		}

		_, err = e.index.Create(e.ctx, txn, row.idxVals, row.handle)
		if err != nil {
			return result, err
		}
		result.addedCount++
	}
	return result, nil
}

// Next implements the Executor Next interface.
func (e *RecoverIndexExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}

	totalAddedCnt, totalScanCnt, err := e.backfillIndex(ctx)
	if err != nil {
		return err
	}

	req.AppendInt64(0, totalAddedCnt)
	req.AppendInt64(1, totalScanCnt)
	e.done = true
	return nil
}

// CleanupIndexExec represents a cleanup index executor.
// It is built from "admin cleanup index" statement, is used to delete
// dangling index data.
type CleanupIndexExec struct {
	baseExecutor

	done      bool
	removeCnt uint64

	index table.Index
	table table.Table

	idxCols          []*model.ColumnInfo
	idxColFieldTypes []*types.FieldType
	idxChunk         *chunk.Chunk

	idxValues   map[int64][][]types.Datum
	batchSize   uint64
	batchKeys   []kv.Key
	idxValsBufs [][]types.Datum
	lastIdxKey  []byte
	scanRowCnt  uint64
}

func (e *CleanupIndexExec) getIdxColTypes() []*types.FieldType {
	if e.idxColFieldTypes != nil {
		return e.idxColFieldTypes
	}
	e.idxColFieldTypes = make([]*types.FieldType, 0, len(e.idxCols))
	for _, col := range e.idxCols {
		e.idxColFieldTypes = append(e.idxColFieldTypes, &col.FieldType)
	}
	return e.idxColFieldTypes
}

func (e *CleanupIndexExec) batchGetRecord(txn kv.Transaction) (map[string][]byte, error) {
	for handle := range e.idxValues {
		e.batchKeys = append(e.batchKeys, e.table.RecordKey(handle))
	}
	values, err := txn.BatchGet(context.Background(), e.batchKeys)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (e *CleanupIndexExec) deleteDanglingIdx(txn kv.Transaction, values map[string][]byte) error {
	for _, k := range e.batchKeys {
		if _, found := values[string(k)]; !found {
			_, handle, err := tablecodec.DecodeRecordKey(k)
			if err != nil {
				return err
			}
			for _, idxVals := range e.idxValues[handle] {
				if err := e.index.Delete(e.ctx.GetSessionVars().StmtCtx, txn, idxVals, handle); err != nil {
					return err
				}
				e.removeCnt++
				if e.removeCnt%e.batchSize == 0 {
					logutil.BgLogger().Info("clean up dangling index", zap.String("table", e.table.Meta().Name.String()),
						zap.String("index", e.index.Meta().Name.String()), zap.Uint64("count", e.removeCnt))
				}
			}
		}
	}
	return nil
}

func extractIdxVals(row chunk.Row, idxVals []types.Datum,
	fieldTypes []*types.FieldType) []types.Datum {
	if cap(idxVals) < row.Len()-1 {
		idxVals = make([]types.Datum, row.Len()-1)
	} else {
		idxVals = idxVals[:row.Len()-1]
	}

	for i := 0; i < row.Len()-1; i++ {
		colVal := row.GetDatum(i, fieldTypes[i])
		colVal.Copy(&idxVals[i])
	}
	return idxVals
}

func (e *CleanupIndexExec) fetchIndex(ctx context.Context, txn kv.Transaction) error {
	result, err := e.buildIndexScan(ctx, txn)
	if err != nil {
		return err
	}
	defer terror.Call(result.Close)

	sc := e.ctx.GetSessionVars().StmtCtx
	for {
		err := result.Next(ctx, e.idxChunk)
		if err != nil {
			return err
		}
		if e.idxChunk.NumRows() == 0 {
			return nil
		}
		iter := chunk.NewIterator4Chunk(e.idxChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle := row.GetInt64(len(e.idxCols) - 1)
			idxVals := extractIdxVals(row, e.idxValsBufs[e.scanRowCnt], e.idxColFieldTypes)
			e.idxValsBufs[e.scanRowCnt] = idxVals
			e.idxValues[handle] = append(e.idxValues[handle], idxVals)
			idxKey, _, err := e.index.GenIndexKey(sc, idxVals, handle, nil)
			if err != nil {
				return err
			}
			e.scanRowCnt++
			e.lastIdxKey = idxKey
			if e.scanRowCnt >= e.batchSize {
				return nil
			}
		}
	}
}

// Next implements the Executor Next interface.
func (e *CleanupIndexExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	for {
		errInTxn := kv.RunInNewTxn(e.ctx.GetStore(), true, func(txn kv.Transaction) error {
			err := e.fetchIndex(ctx, txn)
			if err != nil {
				return err
			}
			values, err := e.batchGetRecord(txn)
			if err != nil {
				return err
			}
			err = e.deleteDanglingIdx(txn, values)
			if err != nil {
				return err
			}
			return nil
		})
		if errInTxn != nil {
			return errInTxn
		}
		if e.scanRowCnt == 0 {
			break
		}
		e.scanRowCnt = 0
		e.batchKeys = e.batchKeys[:0]
		for k := range e.idxValues {
			delete(e.idxValues, k)
		}
	}
	e.done = true
	req.AppendUint64(0, e.removeCnt)
	return nil
}

func (e *CleanupIndexExec) buildIndexScan(ctx context.Context, txn kv.Transaction) (distsql.SelectResult, error) {
	dagPB, err := e.buildIdxDAGPB(txn)
	if err != nil {
		return nil, err
	}
	sc := e.ctx.GetSessionVars().StmtCtx
	var builder distsql.RequestBuilder
	ranges := ranger.FullRange()
	kvReq, err := builder.SetIndexRanges(sc, e.table.Meta().ID, e.index.Meta().ID, ranges).
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, err
	}

	kvReq.KeyRanges[0].StartKey = kv.Key(e.lastIdxKey).PrefixNext()
	kvReq.Concurrency = 1
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.getIdxColTypes(), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, nil
}

// Open implements the Executor Open interface.
func (e *CleanupIndexExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.idxChunk = chunk.New(e.getIdxColTypes(), e.initCap, e.maxChunkSize)
	e.idxValues = make(map[int64][][]types.Datum, e.batchSize)
	e.batchKeys = make([]kv.Key, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Datum, e.batchSize)
	sc := e.ctx.GetSessionVars().StmtCtx
	idxKey, _, err := e.index.GenIndexKey(sc, []types.Datum{{}}, math.MinInt64, nil)
	if err != nil {
		return err
	}
	e.lastIdxKey = idxKey
	return nil
}

func (e *CleanupIndexExec) buildIdxDAGPB(txn kv.Transaction) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetSessionVars().Location())
	sc := e.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range e.idxCols {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	execPB := e.constructIndexScanPB()
	dagReq.Executors = append(dagReq.Executors, execPB)
	err := plannercore.SetPBColumnsDefaultValue(e.ctx, dagReq.Executors[0].IdxScan.Columns, e.idxCols)
	if err != nil {
		return nil, err
	}

	limitExec := e.constructLimitPB()
	dagReq.Executors = append(dagReq.Executors, limitExec)
	distsql.SetEncodeType(e.ctx, dagReq)
	return dagReq, nil
}

func (e *CleanupIndexExec) constructIndexScanPB() *tipb.Executor {
	idxExec := &tipb.IndexScan{
		TableId: e.table.Meta().ID,
		IndexId: e.index.Meta().ID,
		Columns: util.ColumnsToProto(e.idxCols, e.table.Meta().PKIsHandle),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

func (e *CleanupIndexExec) constructLimitPB() *tipb.Executor {
	limitExec := &tipb.Limit{
		Limit: e.batchSize,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

// Close implements the Executor Close interface.
func (e *CleanupIndexExec) Close() error {
	return nil
}
