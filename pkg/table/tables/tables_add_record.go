// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package tables

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

func addTemporaryTable(sctx table.MutateContext, tblInfo *model.TableInfo) (tblctx.TemporaryTableHandler, int64, bool) {
	if s, ok := sctx.GetTemporaryTableSupport(); ok {
		if h, ok := s.AddTemporaryTableToTxn(tblInfo); ok {
			return h, s.GetTemporaryTableSizeLimit(), ok
		}
	}
	return tblctx.TemporaryTableHandler{}, 0, false
}

// The size of a temporary table is calculated by accumulating the transaction size delta.
func handleTempTableSize(t tblctx.TemporaryTableHandler, txnSizeBefore int, txn kv.Transaction) {
	t.UpdateTxnDeltaSize(txn.Size() - txnSizeBefore)
}

func checkTempTableSize(tmpTable tblctx.TemporaryTableHandler, sizeLimit int64) error {
	if tmpTable.GetCommittedSize()+tmpTable.GetDirtySize() > sizeLimit {
		return table.ErrTempTableFull.GenWithStackByArgs(tmpTable.Meta().Name.O)
	}
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *TableCommon) AddRecord(sctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	// TODO: optimize the allocation (and calculation) of opt.
	opt := table.NewAddRecordOpt(opts...)
	return t.addRecord(sctx, txn, r, opt)
}

func (t *TableCommon) addRecord(sctx table.MutateContext, txn kv.Transaction, r []types.Datum, opt *table.AddRecordOpt) (recordID kv.Handle, err error) {
	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(sctx, m); ok {
			if err = checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return nil, err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	var ctx context.Context
	if ctx = opt.Ctx(); ctx != nil {
		var r tracing.Region
		r, ctx = tracing.StartRegionEx(ctx, "table.AddRecord")
		defer r.End()
	} else {
		ctx = context.Background()
	}

	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()

	var hasRecordID bool
	cols := t.Cols()
	// opt.GenerateRecordID is used for normal update.
	// If handle ID is changed when update, update will remove the old record first, and then call `AddRecord` to add a new record.
	// Currently, insert can set _tidb_rowid.
	// Update can only update _tidb_rowid during reorganize partition, to keep the generated _tidb_rowid
	// the same between the old/new sets of partitions, where possible.
	if len(r) > len(cols) && !opt.GenerateRecordID() {
		// The last value is _tidb_rowid.
		recordID = kv.IntHandle(r[len(r)-1].GetInt64())
		hasRecordID = true
	} else {
		tblInfo := t.Meta()
		txn.CacheTableInfo(t.physicalTableID, tblInfo)
		if tblInfo.PKIsHandle {
			recordID = kv.IntHandle(r[tblInfo.GetPkColInfo().Offset].GetInt64())
			hasRecordID = true
		} else if tblInfo.IsCommonHandle {
			pkIdx := FindPrimaryIndex(tblInfo)
			pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
			for _, idxCol := range pkIdx.Columns {
				pkDts = append(pkDts, r[idxCol.Offset])
			}
			tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
			var handleBytes []byte
			handleBytes, err = codec.EncodeKey(tc.Location(), nil, pkDts...)
			err = ec.HandleError(err)
			if err != nil {
				return
			}
			recordID, err = kv.NewCommonHandle(handleBytes)
			if err != nil {
				return
			}
			hasRecordID = true
		}
	}
	if !hasRecordID {
		if reserveAutoID := opt.ReserveAutoID(); reserveAutoID > 0 {
			// Reserve a batch of auto ID in the statement context.
			// The reserved ID could be used in the future within this statement, by the
			// following AddRecord() operation.
			// Make the IDs continuous benefit for the performance of TiKV.
			if reserved, ok := sctx.GetReservedRowIDAlloc(); ok {
				var baseRowID, maxRowID int64
				if baseRowID, maxRowID, err = AllocHandleIDs(ctx, sctx, t, uint64(reserveAutoID)); err != nil {
					return nil, err
				}
				reserved.Reset(baseRowID, maxRowID)
			}
		}

		recordID, err = AllocHandle(ctx, sctx, t)
		if err != nil {
			return nil, err
		}
	}

	// a reusable buffer to save malloc
	// Note: The buffer should not be referenced or modified outside this function.
	// It can only act as a temporary buffer for the current function call.
	mutateBuffers := sctx.GetMutateBuffers()
	encodeRowBuffer := mutateBuffers.GetEncodeRowBufferWithCap(len(r))
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	for _, col := range t.Columns {
		if err := checkDataForModifyColumn(r, col); err != nil {
			return nil, err
		}

		var value types.Datum
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			continue
		}
		// In column type change, since we have set the origin default value for changing col, but
		// for the new insert statement, we should use the casted value of relative column to insert.
		if col.ChangeStateInfo != nil && col.State != model.StatePublic {
			// TODO: Check overflow or ignoreTruncate.
			value, err = table.CastColumnValue(sctx.GetExprCtx(), r[col.DependencyColumnOffset], col.ColumnInfo, false, false)
			if err != nil {
				return nil, err
			}
			if len(r) < len(t.WritableCols()) {
				r = append(r, value)
			} else {
				r[col.Offset] = value
			}
			encodeRowBuffer.AddColVal(col.ID, value)
			continue
		}
		if col.State == model.StatePublic {
			value = r[col.Offset]
		} else {
			// col.ChangeStateInfo must be nil here.
			// because `col.State != model.StatePublic` is true here, if col.ChangeStateInfo is not nil, the col should
			// be handle by the previous if-block.

			if opt.IsUpdate() {
				// If `AddRecord` is called by an update, the default value should be handled the update.
				value = r[col.Offset]
			} else {
				// If `AddRecord` is called by an insert and the col is in write only or write reorganization state, we must
				// add it with its default value.
				value, err = table.GetColOriginDefaultValue(sctx.GetExprCtx(), col.ToInfo())
				if err != nil {
					return nil, err
				}
				// add value to `r` for dirty db in transaction.
				// Otherwise when update will panic cause by get value of column in write only state from dirty db.
				if col.Offset < len(r) {
					r[col.Offset] = value
				} else {
					r = append(r, value)
				}
			}
		}
		if !t.canSkip(col, &value) {
			encodeRowBuffer.AddColVal(col.ID, value)
		}
	}
	if err = table.CheckRowConstraintWithDatum(sctx.GetExprCtx(), t.WritableConstraint(), r, t.meta); err != nil {
		return nil, err
	}
	key := t.RecordKey(recordID)
	var setPresume bool
	if opt.DupKeyCheck() != table.DupKeyCheckSkip {
		if t.meta.TempTableType != model.TempTableNone {
			// Always check key for temporary table because it does not write to TiKV
			_, err = txn.Get(ctx, key)
		} else if opt.DupKeyCheck() == table.DupKeyCheckLazy {
			var v []byte
			v, err = txn.GetMemBuffer().GetLocal(ctx, key)
			if err != nil {
				setPresume = true
			}
			if err == nil && len(v) == 0 {
				err = kv.ErrNotExist
			}
		} else {
			_, err = txn.Get(ctx, key)
		}
		if err == nil {
			// If Global Index and reorganization truncate/drop partition, old partition,
			// Accept and set Assertion key to kv.AssertUnknown for overwrite instead
			dupErr := getDuplicateError(t.Meta(), recordID, r)
			return recordID, dupErr
		} else if !kv.ErrNotExist.Equal(err) {
			return recordID, err
		}
	}

	var flags []kv.FlagsOp
	if setPresume {
		flags = []kv.FlagsOp{kv.SetPresumeKeyNotExists}
		if opt.PessimisticLazyDupKeyCheck() == table.DupKeyCheckInPrewrite && txn.IsPessimistic() {
			flags = append(flags, kv.SetNeedConstraintCheckInPrewrite)
		}
	}

	err = encodeRowBuffer.WriteMemBufferEncoded(sctx.GetRowEncodingConfig(), tc.Location(), ec, memBuffer, key, recordID, flags...)
	if err != nil {
		return nil, err
	}

	failpoint.Inject("addRecordForceAssertExist", func() {
		// Assert the key exists while it actually doesn't. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if sctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting exist on AddRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = setAssertion(txn, key, kv.AssertExist); err != nil {
				failpoint.Return(nil, err)
			}
		}
	})
	if setPresume && !txn.IsPessimistic() {
		err = setAssertion(txn, key, kv.AssertUnknown)
	} else {
		err = setAssertion(txn, key, kv.AssertNotExist)
	}
	if err != nil {
		return nil, err
	}

	// Insert new entries into indices.
	h, err := t.addIndices(sctx, recordID, r, txn, opt.GetCreateIdxOpt())
	if err != nil {
		return h, err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return nil, err
	}
	if sctx.EnableMutationChecker() {
		if err = CheckDataConsistency(txn, tc, t, r, nil, memBuffer, sh); err != nil {
			return nil, errors.Trace(err)
		}
	}

	memBuffer.Release(sh)

	if s, ok := sctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(t.physicalTableID, 1, 1)
	}
	return recordID, nil
}

// genIndexKeyStrs generates index content strings representation.
func genIndexKeyStrs(colVals []types.Datum) ([]string, error) {
	// Pass pre-composed error to txn.
	strVals := make([]string, 0, len(colVals))
	for _, cv := range colVals {
		cvs := "NULL"
		var err error
		if !cv.IsNull() {
			cvs, err = types.ToString(cv.GetValue())
			if err != nil {
				return nil, err
			}
		}
		strVals = append(strVals, cvs)
	}
	return strVals, nil
}

// addIndices adds data into indices. If any key is duplicated, returns the original handle.
func (t *TableCommon) addIndices(sctx table.MutateContext, recordID kv.Handle, r []types.Datum, txn kv.Transaction, opt *table.CreateIdxOpt) (kv.Handle, error) {
	writeBufs := sctx.GetMutateBuffers().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	skipCheck := opt.DupKeyCheck() == table.DupKeyCheckSkip
	for _, v := range t.Indices() {
		if !IsIndexWritable(v) {
			continue
		}
		if v.Meta().IsColumnarIndex() {
			continue
		}
		if t.meta.IsCommonHandle && v.Meta().Primary {
			continue
		}

		meetPartialCondition, err := v.MeetPartialCondition(r)
		if err != nil {
			return nil, err
		}
		if !meetPartialCondition {
			continue
		}

		// We should make sure `indexVals` is assigned with `=` instead of `:=`.
		// The latter one will create a new variable that shadows the outside `indexVals` that makes `indexVals` outside
		// always nil, and we cannot reuse it.
		indexVals, err = v.FetchValues(r, indexVals)
		if err != nil {
			return nil, err
		}
		var dupErr error
		if !skipCheck && v.Meta().Unique {
			// Make error message consistent with MySQL.
			tablecodec.TruncateIndexValues(t.meta, v.Meta(), indexVals)
			colStrVals, err := genIndexKeyStrs(indexVals)
			if err != nil {
				return nil, err
			}
			dupErr = kv.GenKeyExistsErr(colStrVals, fmt.Sprintf("%s.%s", v.TableMeta().Name.String(), v.Meta().Name.String()))
		}
		rsData := TryGetHandleRestoredDataWrapper(t.meta, r, nil, v.Meta())
		if dupHandle, err := asIndex(v).create(sctx, txn, indexVals, recordID, rsData, false, opt); err != nil {
			if kv.ErrKeyExists.Equal(err) {
				return dupHandle, dupErr
			}
			return nil, err
		}
	}
	// save the buffer, multi rows insert can use it.
	writeBufs.IndexValsBuf = indexVals
	return nil, nil
}
