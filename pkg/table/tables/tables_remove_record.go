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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *TableCommon) RemoveRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	opt := table.NewRemoveRecordOpt(opts...)
	return t.removeRecord(ctx, txn, h, r, opt)
}

func (t *TableCommon) removeRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opt *table.RemoveRecordOpt) error {
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	err := t.removeRowData(ctx, txn, h)
	if err != nil {
		return err
	}

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(ctx, m); ok {
			if err = checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	// The table has non-public column and this column is doing the operation of "modify/change column".
	// DELETE will use deletable columns, which is the same as the full columns of the table.
	// INSERT and UPDATE will only use the writable columns. So they will not see columns under MODIFY/CHANGE state.
	// This if block is for the INSERT and UPDATE.
	// And, only DELETE will make opt.HasIndexesLayout() to be true currently.
	if !opt.HasIndexesLayout() && len(t.Columns) > len(r) && t.Columns[len(r)].ChangeStateInfo != nil {
		// The changing column datum derived from related column should be casted here.
		// Otherwise, the existed changing indexes will not be deleted.
		relatedColDatum := r[t.Columns[len(r)].ChangeStateInfo.DependencyColumnOffset]
		value, err := table.CastColumnValue(ctx.GetExprCtx(), relatedColDatum, t.Columns[len(r)].ColumnInfo, false, false)
		if err != nil {
			logutil.BgLogger().Info("remove record cast value failed", zap.Any("col", t.Columns[len(r)]),
				zap.String("handle", h.String()), zap.Any("val", relatedColDatum), zap.Error(err))
			return err
		}
		r = append(r, value)
	}
	err = t.removeRowIndices(ctx, txn, h, r, opt)
	if err != nil {
		return err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return err
	}
	failpoint.InjectCall("duringTableCommonRemoveRecord", t.meta)

	tc := ctx.GetExprCtx().GetEvalCtx().TypeCtx()
	if ctx.EnableMutationChecker() {
		if err = checkDataConsistency(txn, tc, t, nil, r, memBuffer, sh, opt.GetIndexesLayout()); err != nil {
			return errors.Trace(err)
		}
	}
	memBuffer.Release(sh)

	if s, ok := ctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(
			t.physicalTableID, -1, 1,
		)
	}
	return err
}

func (t *TableCommon) removeRowData(ctx table.MutateContext, txn kv.Transaction, h kv.Handle) (err error) {
	// Remove row data.
	key := t.RecordKey(h)
	failpoint.Inject("removeRecordForceAssertNotExist", func() {
		// Assert the key doesn't exist while it actually exists. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if ctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting not exist on RemoveRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
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
	return txn.Delete(key)
}

// removeRowIndices removes all the indices of a row.
func (t *TableCommon) removeRowIndices(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, rec []types.Datum, opt *table.RemoveRecordOpt) (err error) {
	for _, v := range t.DeletableIndices() {
		if v.Meta().Primary && (t.Meta().IsCommonHandle || t.Meta().PKIsHandle) {
			continue
		}
		if v.Meta().IsColumnarIndex() {
			continue
		}
		intest.AssertFunc(func() bool {
			// if the index is partial index, it shouldn't have index layout.
			return !(opt.HasIndexesLayout() && v.Meta().HasCondition())
		})
		meetPartialCondition, err := v.MeetPartialCondition(rec)
		if err != nil {
			return err
		}
		if !meetPartialCondition {
			// If the partial index condition is not met, we don't need to delete it because
			// it has never been written.
			continue
		}

		var vals []types.Datum
		if opt.HasIndexesLayout() {
			vals, err = fetchIndexRow(v.Meta(), rec, nil, opt.GetIndexLayout(v.Meta().ID))
		} else {
			vals, err = fetchIndexRow(v.Meta(), rec, nil, nil)
		}
		if err != nil {
			logutil.BgLogger().Info("remove row index failed", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()), zap.Any("record", rec), zap.Error(err))
			return err
		}
		if err = v.Delete(ctx, txn, vals, h); err != nil {
			if v.Meta().State != model.StatePublic && kv.ErrNotExist.Equal(err) {
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				logutil.BgLogger().Debug("row index not exists", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()))
				continue
			}
			return err
		}
	}
	return nil
}

func (t *TableCommon) buildIndexForRow(ctx table.MutateContext, h kv.Handle, vals []types.Datum, newData []types.Datum, idx *index, txn kv.Transaction, untouched bool, opt *table.CreateIdxOpt) error {
	rsData := TryGetHandleRestoredDataWrapper(t.meta, newData, nil, idx.Meta())
	if _, err := idx.create(ctx, txn, vals, h, rsData, untouched, opt); err != nil {
		if kv.ErrKeyExists.Equal(err) {
			// Make error message consistent with MySQL.
			tablecodec.TruncateIndexValues(t.meta, idx.Meta(), vals)
			colStrVals, err1 := genIndexKeyStrs(vals)
			if err1 != nil {
				// if genIndexKeyStrs failed, return the original error.
				return err
			}

			return kv.GenKeyExistsErr(colStrVals, fmt.Sprintf("%s.%s", idx.TableMeta().Name.String(), idx.Meta().Name.String()))
		}
		return err
	}
	return nil
}
