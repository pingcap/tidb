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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *TableCommon) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	opt := table.NewUpdateRecordOpt(opts...)
	return t.updateRecord(ctx, txn, h, oldData, newData, touched, opt)
}

func (t *TableCommon) updateRecord(sctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opt *table.UpdateRecordOpt) error {
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(sctx, m); ok {
			if err := checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	numColsCap := len(newData) + 1 // +1 for the extra handle column that we may need to append.

	// a reusable buffer to save malloc
	// Note: The buffer should not be referenced or modified outside this function.
	// It can only act as a temporary buffer for the current function call.
	mutateBuffers := sctx.GetMutateBuffers()
	encodeRowBuffer := mutateBuffers.GetEncodeRowBufferWithCap(numColsCap)
	checkRowBuffer := mutateBuffers.GetCheckRowBufferWithCap(numColsCap)

	for _, col := range t.Columns {
		var value types.Datum
		var err error
		if err := checkDataForModifyColumn(newData, col); err != nil {
			return err
		}

		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			if col.ChangeStateInfo != nil {
				// TODO: Check overflow or ignoreTruncate.
				value, err = table.CastColumnValue(sctx.GetExprCtx(), oldData[col.DependencyColumnOffset], col.ColumnInfo, false, false)
				if err != nil {
					logutil.BgLogger().Info("update record cast value failed", zap.Any("col", col), zap.Uint64("txnStartTS", txn.StartTS()),
						zap.String("handle", h.String()), zap.Any("val", oldData[col.DependencyColumnOffset]), zap.Error(err))
					return err
				}
				oldData = append(oldData, value)
				touched = append(touched, touched[col.DependencyColumnOffset])
			}
			continue
		}
		if col.State != model.StatePublic {
			// If col is in write only or write reorganization state we should keep the oldData.
			// Because the oldData must be the original data(it's changed by other TiDBs.) or the original default value.
			// TODO: Use newData directly.
			value = oldData[col.Offset]
			if col.ChangeStateInfo != nil {
				// TODO: Check overflow or ignoreTruncate.
				value, err = table.CastColumnValue(sctx.GetExprCtx(), newData[col.DependencyColumnOffset], col.ColumnInfo, false, false)
				if err != nil {
					return err
				}
				newData[col.Offset] = value
				touched[col.Offset] = touched[col.DependencyColumnOffset]
			}
		} else {
			value = newData[col.Offset]
		}
		if !t.canSkip(col, &value) {
			encodeRowBuffer.AddColVal(col.ID, value)
		}
		checkRowBuffer.AddColVal(value)
	}
	// check data constraint
	if constraints := t.WritableConstraint(); len(constraints) > 0 {
		if err := table.CheckRowConstraint(sctx.GetExprCtx(), constraints, checkRowBuffer.GetRowToCheck(), t.Meta()); err != nil {
			return err
		}
	}
	// rebuild index
	err := t.rebuildUpdateRecordIndices(sctx, txn, h, touched, oldData, newData, opt)
	if err != nil {
		return err
	}

	key := t.RecordKey(h)
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()
	err = encodeRowBuffer.WriteMemBufferEncoded(sctx.GetRowEncodingConfig(), tc.Location(), ec, memBuffer, key, h)
	if err != nil {
		return err
	}

	failpoint.Inject("updateRecordForceAssertNotExist", func() {
		// Assert the key doesn't exist while it actually exists. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if sctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting not exist on UpdateRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = setAssertion(txn, key, kv.AssertNotExist); err != nil {
				failpoint.Return(err)
			}
		}
	})

	if t.skipAssert {
		err = setAssertion(txn, key, kv.AssertUnknown)
	} else {
		err = setAssertion(txn, key, kv.AssertExist)
	}
	if err != nil {
		return err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return err
	}
	if sctx.EnableMutationChecker() {
		if err = CheckDataConsistency(txn, tc, t, newData, oldData, memBuffer, sh); err != nil {
			return errors.Trace(err)
		}
	}

	memBuffer.Release(sh)

	if s, ok := sctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(t.physicalTableID, 0, 1)
	}
	return nil
}

func (t *TableCommon) rebuildUpdateRecordIndices(
	ctx table.MutateContext, txn kv.Transaction,
	h kv.Handle, touched []bool, oldData []types.Datum, newData []types.Datum,
	opt *table.UpdateRecordOpt,
) error {
	for _, idx := range t.DeletableIndices() {
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
		if idx.Meta().IsColumnarIndex() {
			continue
		}

		oldDataMeetPartialCondition, err := idx.MeetPartialCondition(oldData)
		if err != nil {
			return err
		}
		if !oldDataMeetPartialCondition {
			// If the partial index condition is not met, we don't need to delete it because
			// it has never been written.
			continue
		}
		untouched := true
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}
		for _, ic := range idx.Meta().AffectColumn {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}

		if !untouched {
			oldVs, err := idx.FetchValues(oldData, nil)
			if err != nil {
				return err
			}
			if err = idx.Delete(ctx, txn, oldVs, h); err != nil {
				return err
			}
		}
	}
	createIdxOpt := opt.GetCreateIdxOpt()
	for _, idx := range t.Indices() {
		if !IsIndexWritable(idx) {
			continue
		}
		if idx.Meta().IsColumnarIndex() {
			continue
		}
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
		untouched := true
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}
		for _, ic := range idx.Meta().AffectColumn {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}
		if untouched && opt.SkipWriteUntouchedIndices() {
			continue
		}
		newDataMeetPartialCondition, err := idx.MeetPartialCondition(newData)
		if err != nil {
			return err
		}
		if !newDataMeetPartialCondition {
			// If the partial index condition is not met, we don't need to build the new index.
			continue
		}

		newVs, err := idx.FetchValues(newData, nil)
		if err != nil {
			return err
		}
		if err := t.buildIndexForRow(ctx, h, newVs, newData, asIndex(idx), txn, untouched, createIdxOpt); err != nil {
			return err
		}
	}
	return nil
}
