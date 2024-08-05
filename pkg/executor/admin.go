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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ exec.Executor = &CheckIndexRangeExec{}
	_ exec.Executor = &RecoverIndexExec{}
	_ exec.Executor = &CleanupIndexExec{}
)

// CheckIndexRangeExec outputs the index values which has handle between begin and end.
type CheckIndexRangeExec struct {
	exec.BaseExecutor

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
	handleIdx := e.GetSchema().Len() - 1
	for {
		err := e.result.Next(ctx, e.srcChunk)
		if err != nil {
			return err
		}
		if e.srcChunk.NumRows() == 0 {
			return nil
		}
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		appendRows := make([]chunk.Row, 0, e.srcChunk.NumRows())
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle := row.GetInt64(handleIdx)
			for _, hr := range e.handleRanges {
				if handle >= hr.Begin && handle < hr.End {
					appendRows = append(appendRows, row)
					break
				}
			}
		}
		if len(appendRows) > 0 {
			req.AppendRows(appendRows)
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

	colTypeForHandle := e.GetSchema().Columns[len(e.cols)].RetType
	e.cols = append(e.cols, &model.ColumnInfo{
		ID:        model.ExtraHandleID,
		Name:      model.ExtraHandleName,
		FieldType: *colTypeForHandle,
	})

	e.srcChunk = exec.TryNewCacheChunk(e)
	dagPB, err := e.buildDAGPB()
	if err != nil {
		return err
	}
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return nil
	}
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(e.Ctx().GetDistSQLCtx(), e.table.ID, e.index.ID, ranger.FullRange()).
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromSessionVars(e.Ctx().GetDistSQLCtx()).
		SetFromInfoSchema(e.Ctx().GetInfoSchema()).
		SetConnIDAndConnAlias(e.Ctx().GetSessionVars().ConnectionID, e.Ctx().GetSessionVars().SessionAlias).
		Build()
	if err != nil {
		return err
	}

	e.result, err = distsql.Select(ctx, e.Ctx().GetDistSQLCtx(), kvReq, e.RetFieldTypes())
	if err != nil {
		return err
	}
	return nil
}

func (e *CheckIndexRangeExec) buildDAGPB() (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.Ctx().GetSessionVars().Location())
	sc := e.Ctx().GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range e.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	execPB := e.constructIndexScanPB()
	dagReq.Executors = append(dagReq.Executors, execPB)

	err := tables.SetPBColumnsDefaultValue(e.Ctx().GetExprCtx(), dagReq.Executors[0].IdxScan.Columns, e.cols)
	if err != nil {
		return nil, err
	}
	distsql.SetEncodeType(e.Ctx().GetDistSQLCtx(), dagReq)
	return dagReq, nil
}

func (e *CheckIndexRangeExec) constructIndexScanPB() *tipb.Executor {
	idxExec := &tipb.IndexScan{
		TableId: e.table.ID,
		IndexId: e.index.ID,
		Columns: util.ColumnsToProto(e.cols, e.table.PKIsHandle, true),
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}
}

// Close implements the Executor Close interface.
func (*CheckIndexRangeExec) Close() error {
	return nil
}

// RecoverIndexExec represents a recover index executor.
// It is built from "admin recover index" statement, is used to backfill
// corrupted index.
type RecoverIndexExec struct {
	exec.BaseExecutor

	done bool

	index      table.Index
	table      table.Table
	physicalID int64
	batchSize  int

	columns       []*model.ColumnInfo
	colFieldTypes []*types.FieldType
	srcChunk      *chunk.Chunk
	handleCols    plannercore.HandleCols

	containsGenedCol bool
	cols             []*expression.Column

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
	if err := exec.Open(ctx, &e.BaseExecutor); err != nil {
		return err
	}

	e.srcChunk = chunk.New(e.columnsTypes(), e.InitCap(), e.MaxChunkSize())
	e.batchSize = 2048
	e.recoverRows = make([]recoverRows, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Datum, e.batchSize)
	e.idxKeyBufs = make([][]byte, e.batchSize)
	return nil
}

func (e *RecoverIndexExec) constructTableScanPB(tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.Executor, error) {
	tblScan := tables.BuildTableScanFromInfos(tblInfo, colInfos)
	tblScan.TableId = e.physicalID
	err := tables.SetPBColumnsDefaultValue(e.Ctx().GetExprCtx(), tblScan.Columns, colInfos)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}, err
}

func (*RecoverIndexExec) constructLimitPB(count uint64) *tipb.Executor {
	limitExec := &tipb.Limit{
		Limit: count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

func (e *RecoverIndexExec) buildDAGPB(_ kv.Transaction, limitCnt uint64) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.Ctx().GetSessionVars().Location())
	sc := e.Ctx().GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range e.columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	tblScanExec, err := e.constructTableScanPB(e.table.Meta(), e.columns)
	if err != nil {
		return nil, err
	}
	dagReq.Executors = append(dagReq.Executors, tblScanExec)

	limitExec := e.constructLimitPB(limitCnt)
	dagReq.Executors = append(dagReq.Executors, limitExec)
	distsql.SetEncodeType(e.Ctx().GetDistSQLCtx(), dagReq)
	return dagReq, nil
}

func (e *RecoverIndexExec) buildTableScan(ctx context.Context, txn kv.Transaction, startHandle kv.Handle, limitCnt uint64) (distsql.SelectResult, error) {
	dagPB, err := e.buildDAGPB(txn, limitCnt)
	if err != nil {
		return nil, err
	}
	var builder distsql.RequestBuilder
	keyRanges, err := buildRecoverIndexKeyRanges(e.physicalID, startHandle)
	if err != nil {
		return nil, err
	}
	builder.KeyRanges = kv.NewNonPartitionedKeyRanges(keyRanges)
	kvReq, err := builder.
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromSessionVars(e.Ctx().GetDistSQLCtx()).
		SetFromInfoSchema(e.Ctx().GetInfoSchema()).
		SetConnIDAndConnAlias(e.Ctx().GetSessionVars().ConnectionID, e.Ctx().GetSessionVars().SessionAlias).
		Build()
	if err != nil {
		return nil, err
	}

	// Actually, with limitCnt, the match data maybe only in one region, so let the concurrency to be 1,
	// avoid unnecessary region scan.
	kvReq.Concurrency = 1
	result, err := distsql.Select(ctx, e.Ctx().GetDistSQLCtx(), kvReq, e.columnsTypes())
	if err != nil {
		return nil, err
	}
	return result, nil
}

// buildRecoverIndexKeyRanges build a KeyRange: (startHandle, unlimited).
func buildRecoverIndexKeyRanges(tid int64, startHandle kv.Handle) ([]kv.KeyRange, error) {
	var startKey []byte
	if startHandle == nil {
		startKey = tablecodec.GenTableRecordPrefix(tid).Next()
	} else {
		startKey = tablecodec.EncodeRowKey(tid, startHandle.Encoded()).PrefixNext()
	}
	endKey := tablecodec.GenTableRecordPrefix(tid).PrefixNext()
	return []kv.KeyRange{{StartKey: startKey, EndKey: endKey}}, nil
}

type backfillResult struct {
	currentHandle kv.Handle
	addedCount    int64
	scanRowCount  int64
}

func (e *RecoverIndexExec) backfillIndex(ctx context.Context) (totalAddedCnt, totalScanCnt int64, err error) {
	var (
		currentHandle kv.Handle
		lastLogCnt    int64
		result        backfillResult
	)
	for {
		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
		errInTxn := kv.RunInNewTxn(ctx, e.Ctx().GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
			setOptionForTopSQL(e.Ctx().GetSessionVars().StmtCtx, txn)
			var err error
			result, err = e.backfillIndexInTxn(ctx, txn, currentHandle)
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
				zap.Int64("totalScanCnt", totalScanCnt), zap.Stringer("currentHandle", result.currentHandle))
		}

		// no more rows
		if result.scanRowCount == 0 {
			break
		}
		currentHandle = result.currentHandle
		if currentHandle.Next().Compare(result.currentHandle) <= 0 {
			break // There is no more handles in the table.
		}
	}
	return totalAddedCnt, totalScanCnt, nil
}

type recoverRows struct {
	handle  kv.Handle
	idxVals []types.Datum
	rsData  []types.Datum
	skip    bool
}

func (e *RecoverIndexExec) fetchRecoverRows(ctx context.Context, srcResult distsql.SelectResult, result *backfillResult) ([]recoverRows, error) {
	e.recoverRows = e.recoverRows[:0]
	idxValLen := len(e.index.Meta().Columns)
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
			handle, err := e.handleCols.BuildHandle(row)
			if err != nil {
				return nil, err
			}
			if e.index.Meta().Global {
				handle = kv.NewPartitionHandle(e.physicalID, handle)
			}
			idxVals, err := e.buildIndexedValues(row, e.idxValsBufs[result.scanRowCount], e.colFieldTypes, idxValLen)
			if err != nil {
				return nil, err
			}
			e.idxValsBufs[result.scanRowCount] = idxVals
			rsData := tables.TryGetHandleRestoredDataWrapper(e.table.Meta(), plannercore.GetCommonHandleDatum(e.handleCols, row), nil, e.index.Meta())
			e.recoverRows = append(e.recoverRows, recoverRows{handle: handle, idxVals: idxVals, rsData: rsData, skip: true})
			result.scanRowCount++
			result.currentHandle = handle
		}
	}

	return e.recoverRows, nil
}

func (e *RecoverIndexExec) buildIndexedValues(row chunk.Row, idxVals []types.Datum, fieldTypes []*types.FieldType, idxValLen int) ([]types.Datum, error) {
	if !e.containsGenedCol {
		return extractIdxVals(row, idxVals, fieldTypes, idxValLen), nil
	}

	if e.cols == nil {
		columns, _, err := expression.ColumnInfos2ColumnsAndNames(e.Ctx().GetExprCtx(), model.NewCIStr("mock"), e.table.Meta().Name, e.table.Meta().Columns, e.table.Meta())
		if err != nil {
			return nil, err
		}
		e.cols = columns
	}

	if cap(idxVals) < idxValLen {
		idxVals = make([]types.Datum, idxValLen)
	} else {
		idxVals = idxVals[:idxValLen]
	}

	sctx := e.Ctx()
	for i, col := range e.index.Meta().Columns {
		if e.table.Meta().Columns[col.Offset].IsGenerated() {
			val, err := e.cols[col.Offset].EvalVirtualColumn(sctx.GetExprCtx().GetEvalCtx(), row)
			if err != nil {
				return nil, err
			}
			val.Copy(&idxVals[i])
		} else {
			val := row.GetDatum(col.Offset, &(e.table.Meta().Columns[col.Offset].FieldType))
			val.Copy(&idxVals[i])
		}
	}
	return idxVals, nil
}

func (e *RecoverIndexExec) batchMarkDup(txn kv.Transaction, rows []recoverRows) error {
	if len(rows) == 0 {
		return nil
	}
	e.batchKeys = e.batchKeys[:0]
	sc := e.Ctx().GetSessionVars().StmtCtx
	distinctFlags := make([]bool, 0, len(rows))
	rowIdx := make([]int, 0, len(rows))
	cnt := 0
	for i, row := range rows {
		iter := e.index.GenIndexKVIter(sc.ErrCtx(), sc.TimeZone(), row.idxVals, row.handle, nil)
		for iter.Valid() {
			var buf []byte
			if cnt < len(e.idxKeyBufs) {
				buf = e.idxKeyBufs[cnt]
			}
			key, _, distinct, err := iter.Next(buf, nil)
			if err != nil {
				return err
			}
			if cnt < len(e.idxKeyBufs) {
				e.idxKeyBufs[cnt] = key
			} else {
				e.idxKeyBufs = append(e.idxKeyBufs, key)
			}

			cnt++
			e.batchKeys = append(e.batchKeys, key)
			distinctFlags = append(distinctFlags, distinct)
			rowIdx = append(rowIdx, i)
		}
	}

	values, err := txn.BatchGet(context.Background(), e.batchKeys)
	if err != nil {
		return err
	}

	// 1. unique-key is duplicate and the handle is equal, skip it.
	// 2. unique-key is duplicate and the handle is not equal, data is not consistent, log it and skip it.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range e.batchKeys {
		val, found := values[string(key)]
		if found {
			if distinctFlags[i] {
				handle, err1 := tablecodec.DecodeHandleInIndexValue(val)
				if err1 != nil {
					return err1
				}

				if handle.Compare(rows[rowIdx[i]].handle) != 0 {
					logutil.BgLogger().Warn("recover index: the constraint of unique index is broken, handle in index is not equal to handle in table",
						zap.String("index", e.index.Meta().Name.O), zap.ByteString("indexKey", key),
						zap.Stringer("handleInTable", rows[rowIdx[i]].handle), zap.Stringer("handleInIndex", handle))
				}
			}
		}
		rows[rowIdx[i]].skip = found && rows[rowIdx[i]].skip
	}
	return nil
}

func (e *RecoverIndexExec) backfillIndexInTxn(ctx context.Context, txn kv.Transaction, currentHandle kv.Handle) (result backfillResult, err error) {
	srcResult, err := e.buildTableScan(ctx, txn, currentHandle, uint64(e.batchSize))
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
	e.Ctx().GetSessionVars().StmtCtx.BatchCheck = true
	for _, row := range rows {
		if row.skip {
			continue
		}

		recordKey := tablecodec.EncodeRecordKey(e.table.RecordPrefix(), row.handle)
		err := txn.LockKeys(ctx, new(kv.LockCtx), recordKey)
		if err != nil {
			return result, err
		}

		_, err = e.index.Create(e.Ctx().GetTableCtx(), txn, row.idxVals, row.handle, row.rsData, table.WithIgnoreAssertion)
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

	recoveringClusteredIndex := e.index.Meta().Primary && e.table.Meta().IsCommonHandle
	if recoveringClusteredIndex {
		req.AppendInt64(0, 0)
		req.AppendInt64(1, 0)
		e.done = true
		return nil
	}
	var totalAddedCnt, totalScanCnt int64
	var err error
	if tbl, ok := e.table.(table.PartitionedTable); ok {
		pi := e.table.Meta().GetPartitionInfo()
		for _, p := range pi.Definitions {
			e.table = tbl.GetPartition(p.ID)
			e.index = tables.GetWritableIndexByName(e.index.Meta().Name.L, e.table)
			e.physicalID = p.ID
			addedCnt, scanCnt, err := e.backfillIndex(ctx)
			totalAddedCnt += addedCnt
			totalScanCnt += scanCnt
			if err != nil {
				return err
			}
		}
	} else {
		totalAddedCnt, totalScanCnt, err = e.backfillIndex(ctx)
		if err != nil {
			return err
		}
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
	exec.BaseExecutor

	done      bool
	removeCnt uint64

	index      table.Index
	table      table.Table
	physicalID int64

	columns          []*model.ColumnInfo
	idxColFieldTypes []*types.FieldType
	idxChunk         *chunk.Chunk
	handleCols       plannercore.HandleCols

	idxValues   *kv.HandleMap // kv.Handle -> [][]types.Datum
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
	e.idxColFieldTypes = make([]*types.FieldType, 0, len(e.columns))
	for _, col := range e.columns {
		e.idxColFieldTypes = append(e.idxColFieldTypes, col.FieldType.ArrayType())
	}
	return e.idxColFieldTypes
}

func (e *CleanupIndexExec) batchGetRecord(txn kv.Transaction) (map[string][]byte, error) {
	e.idxValues.Range(func(h kv.Handle, _ any) bool {
		e.batchKeys = append(e.batchKeys, tablecodec.EncodeRecordKey(e.table.RecordPrefix(), h))
		return true
	})
	values, err := txn.BatchGet(context.Background(), e.batchKeys)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (e *CleanupIndexExec) deleteDanglingIdx(txn kv.Transaction, values map[string][]byte) error {
	for _, k := range e.batchKeys {
		if _, found := values[string(k)]; !found {
			pid, handle, err := tablecodec.DecodeRecordKey(k)
			if err != nil {
				return err
			}
			if e.index.Meta().Global {
				handle = kv.NewPartitionHandle(pid, handle)
			}
			handleIdxValsGroup, ok := e.idxValues.Get(handle)
			if !ok {
				return errors.Trace(errors.Errorf("batch keys are inconsistent with handles"))
			}
			for _, handleIdxVals := range handleIdxValsGroup.([][]types.Datum) {
				if err := e.index.Delete(e.Ctx().GetTableCtx(), txn, handleIdxVals, handle); err != nil {
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
	fieldTypes []*types.FieldType, idxValLen int) []types.Datum {
	if cap(idxVals) < idxValLen {
		idxVals = make([]types.Datum, idxValLen)
	} else {
		idxVals = idxVals[:idxValLen]
	}

	for i := 0; i < idxValLen; i++ {
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

	sc := e.Ctx().GetSessionVars().StmtCtx
	idxColLen := len(e.index.Meta().Columns)
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
			handle, err := e.handleCols.BuildHandle(row)
			if err != nil {
				return err
			}
			if e.index.Meta().Global {
				handle = kv.NewPartitionHandle(row.GetInt64(row.Len()-1), handle)
			}
			idxVals := extractIdxVals(row, e.idxValsBufs[e.scanRowCnt], e.idxColFieldTypes, idxColLen)
			e.idxValsBufs[e.scanRowCnt] = idxVals
			existingIdxVals, ok := e.idxValues.Get(handle)
			if ok {
				updatedIdxVals := append(existingIdxVals.([][]types.Datum), idxVals)
				e.idxValues.Set(handle, updatedIdxVals)
			} else {
				e.idxValues.Set(handle, [][]types.Datum{idxVals})
			}
			idxKey, _, err := e.index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), idxVals, handle, nil)
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
	cleaningClusteredPrimaryKey := e.table.Meta().IsCommonHandle && e.index.Meta().Primary
	if cleaningClusteredPrimaryKey {
		e.done = true
		req.AppendUint64(0, 0)
		return nil
	}

	var err error
	if tbl, ok := e.table.(table.PartitionedTable); ok && !e.index.Meta().Global {
		pi := e.table.Meta().GetPartitionInfo()
		for _, p := range pi.Definitions {
			e.table = tbl.GetPartition(p.ID)
			e.index = tables.GetWritableIndexByName(e.index.Meta().Name.L, e.table)
			e.physicalID = p.ID
			err = e.init()
			if err != nil {
				return err
			}
			err = e.cleanTableIndex(ctx)
			if err != nil {
				return err
			}
		}
	} else {
		err = e.cleanTableIndex(ctx)
		if err != nil {
			return err
		}
	}
	e.done = true
	req.AppendUint64(0, e.removeCnt)
	return nil
}

func (e *CleanupIndexExec) cleanTableIndex(ctx context.Context) error {
	for {
		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)
		errInTxn := kv.RunInNewTxn(ctx, e.Ctx().GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
			txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
			setOptionForTopSQL(e.Ctx().GetSessionVars().StmtCtx, txn)
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
		e.idxValues.Range(func(h kv.Handle, _ any) bool {
			e.idxValues.Delete(h)
			return true
		})
	}
	return nil
}

func (e *CleanupIndexExec) buildIndexScan(ctx context.Context, txn kv.Transaction) (distsql.SelectResult, error) {
	dagPB, err := e.buildIdxDAGPB()
	if err != nil {
		return nil, err
	}
	var builder distsql.RequestBuilder
	ranges := ranger.FullRange()
	keyRanges, err := distsql.IndexRangesToKVRanges(e.Ctx().GetDistSQLCtx(), e.physicalID, e.index.Meta().ID, ranges)
	if err != nil {
		return nil, err
	}
	err = keyRanges.SetToNonPartitioned()
	if err != nil {
		return nil, err
	}
	keyRanges.FirstPartitionRange()[0].StartKey = kv.Key(e.lastIdxKey).PrefixNext()
	kvReq, err := builder.SetWrappedKeyRanges(keyRanges).
		SetDAGRequest(dagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromSessionVars(e.Ctx().GetDistSQLCtx()).
		SetFromInfoSchema(e.Ctx().GetInfoSchema()).
		SetConnIDAndConnAlias(e.Ctx().GetSessionVars().ConnectionID, e.Ctx().GetSessionVars().SessionAlias).
		Build()
	if err != nil {
		return nil, err
	}

	kvReq.Concurrency = 1
	result, err := distsql.Select(ctx, e.Ctx().GetDistSQLCtx(), kvReq, e.getIdxColTypes())
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Open implements the Executor Open interface.
func (e *CleanupIndexExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.init()
}

func (e *CleanupIndexExec) init() error {
	e.idxChunk = chunk.New(e.getIdxColTypes(), e.InitCap(), e.MaxChunkSize())
	e.idxValues = kv.NewHandleMap()
	e.batchKeys = make([]kv.Key, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Datum, e.batchSize)
	sc := e.Ctx().GetSessionVars().StmtCtx
	idxKey, _, err := e.index.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), []types.Datum{{}}, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return err
	}
	e.lastIdxKey = idxKey
	return nil
}

func (e *CleanupIndexExec) buildIdxDAGPB() (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(e.Ctx().GetSessionVars().Location())
	sc := e.Ctx().GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	for i := range e.columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	execPB := e.constructIndexScanPB()
	dagReq.Executors = append(dagReq.Executors, execPB)
	err := tables.SetPBColumnsDefaultValue(e.Ctx().GetExprCtx(), dagReq.Executors[0].IdxScan.Columns, e.columns)
	if err != nil {
		return nil, err
	}

	limitExec := e.constructLimitPB()
	dagReq.Executors = append(dagReq.Executors, limitExec)
	distsql.SetEncodeType(e.Ctx().GetDistSQLCtx(), dagReq)
	return dagReq, nil
}

func (e *CleanupIndexExec) constructIndexScanPB() *tipb.Executor {
	idxExec := &tipb.IndexScan{
		TableId:          e.physicalID,
		IndexId:          e.index.Meta().ID,
		Columns:          util.ColumnsToProto(e.columns, e.table.Meta().PKIsHandle, true),
		PrimaryColumnIds: tables.TryGetCommonPkColumnIds(e.table.Meta()),
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
func (*CleanupIndexExec) Close() error {
	return nil
}
