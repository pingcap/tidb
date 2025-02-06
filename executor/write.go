// Copyright 2016 PingCAP, Inc.
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
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/memory"
)

var (
	_ Executor = &UpdateExec{}
	_ Executor = &DeleteExec{}
	_ Executor = &InsertExec{}
	_ Executor = &ReplaceExec{}
	_ Executor = &LoadDataExec{}
)

/*
 * updateRecord updates the row specified by the handle `h`, from `oldData` to `newData`.
 * It is used both in update/insert on duplicate statements.
 *
 * The `modified` inputed indicates whether columns are explicitly set.
 * And this slice will be reused in this function to record which columns are really modified, which is used for secondary indices.
 *
 * offset, assignments, evalBuffer and errorHandler are used to update auto-generated columns.
 * We need to evaluate assignments, and set the result value in newData and evalBuffer respectively.
 * Since the column indices in assignments are based on evalbuffer, and newData may be a subset of evalBuffer,
 * offset is needed when assigning to newData.
 *
 *                     |<---- newData ---->|
 * -------------------------------------------------------
 * |        t1         |        t1         |     t3      |
 * -------------------------------------------------------
 * |<------------------ evalBuffer ---|----------------->|
 *                                    |
 *                                    |
 * |<------------------------- assign.Col.Idx
 *
 * Length of `oldData` and `newData` equals to length of `t.Cols()`.
 *
 * The return values:
 *  1. changed (bool): does the update really change the row values. e.g. update set i = 1 where i = 1;
 *  2. err (error): error in the update.
 */
func updateRecord(
	ctx context.Context, sctx sessionctx.Context,
	h kv.Handle, oldData, newData []types.Datum,
	offset int,
	assignments []*expression.Assignment,
	evalBuffer chunk.MutRow,
	errorHandler func(sctx sessionctx.Context, assign *expression.Assignment, val *types.Datum, err error) error,
	modified []bool,
	t table.Table,
	onDup bool,
	_ *memory.Tracker,
	fkChecks []*FKCheckExec,
	fkCascades []*FKCascadeExec,
) (bool, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("executor.updateRecord", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	sc := sctx.GetSessionVars().StmtCtx

	// changed, handleChanged indicated whether row/handle is changed
	changed, handleChanged := false, false
	// onUpdateNeedModify is for "UPDATE SET ts_field = old_value".
	// If the on-update-now timestamp field is explicitly set, we don't need to update it again.
	onUpdateNeedModify := make(map[int]bool)

	// We can iterate on public columns not writable columns,
	// because all of them are sorted by their `Offset`, which
	// causes all writable columns are after public columns.
	cols := t.Cols()

	// A wrapper function to check whether certain column is changed after evaluation.
	checkColumnFunc := func(i int, skipGenerated bool) error {
		col := cols[i]
		if col.IsGenerated() && skipGenerated {
			return nil
		}

		// modified[i] == false means this on-update-now field is not explicited set.
		if mysql.HasOnUpdateNowFlag(col.GetFlag()) {
			onUpdateNeedModify[i] = !modified[i]
		}

		// We should use binary collation to compare datum, otherwise the result will be incorrect.
		cmp, err := newData[i].Compare(sc, &oldData[i], collate.GetBinaryCollator())
		if err != nil {
			return err
		}
		modified[i] = cmp != 0
		if cmp != 0 {
			changed = true
			// Rebase auto increment id if the field is changed.
			if mysql.HasAutoIncrementFlag(col.GetFlag()) {
				recordID, err := getAutoRecordID(newData[i], &col.FieldType, false)
				if err != nil {
					return err
				}
				if err = t.Allocators(sctx).Get(autoid.AutoIncrementType).Rebase(ctx, recordID, true); err != nil {
					return err
				}
			}
			if col.IsPKHandleColumn(t.Meta()) {
				handleChanged = true
				// Rebase auto random id if the field is changed.
				if err := rebaseAutoRandomValue(ctx, sctx, t, &newData[i], col); err != nil {
					return err
				}
			}
			if col.IsCommonHandleColumn(t.Meta()) {
				handleChanged = true
			}
		}

		return nil
	}

	// Before do actual update, We need to ensure that all columns are evaluated in the following order:
	// Step 1: non-generated columns (These columns should be evaluated outside this function).
	// Step 2: check whether there are some columns changed.
	// Step 3: on-update-now columns if non-generated columns are changed.
	// Step 4: generated columns if non-generated columns are changed.
	// Step 5: handle bad null errors and exchange partition errors.
	// After these are done, we can finally update the record.

	// Step 2: compare already evaluated columns and update changed, handleChanged and handleChanged flags.
	for i := range cols {
		if err := checkColumnFunc(i, true); err != nil {
			return false, err
		}
	}

	sc.AddTouchedRows(1)
	// If no changes, nothing to do, return directly.
	if !changed {
		// See https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if sctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
			sc.AddAffectedRows(1)
		}
		_, err := appendUnchangedRowForLock(sctx, t, h, oldData)
		return false, err
	}

	// Step 3: fill values into on-update-now fields.
	for i, col := range t.Cols() {
		var err error
		if mysql.HasOnUpdateNowFlag(col.GetFlag()) && onUpdateNeedModify[i] {
			newData[i], err = expression.GetTimeValue(sctx, strings.ToUpper(ast.CurrentTimestamp), col.GetType(), col.GetDecimal(), nil)
			modified[i] = true
			// For update statement, evalBuffer is initialized on demand.
			if chunk.Row(evalBuffer).Chunk() != nil {
				evalBuffer.SetDatum(i+offset, newData[i])
			}
			if err != nil {
				return false, err
			}
			// Only TIMESTAMP and DATETIME columns can be automatically updated, so it cannot be PKIsHandle.
			// Ref: https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html
			if col.IsPKHandleColumn(t.Meta()) {
				return false, errors.Errorf("on-update-now column should never be pk-is-handle")
			}
			if col.IsCommonHandleColumn(t.Meta()) {
				handleChanged = true
			}
		}
	}

	// Step 4: fill auto generated columns
	for _, assign := range assignments {
		// Insert statements may have LazyErr, handle it first.
		if assign.LazyErr != nil {
			return false, assign.LazyErr
		}

		// For Update statements, Index may be larger than len(newData)
		// e.g. update t a, t b set a.c1 = 1, b.c2 = 2;
		idxInCols := assign.Col.Index - offset
		rawVal, err := assign.Expr.Eval(evalBuffer.ToRow())
		if err == nil {
			newData[idxInCols], err = table.CastValue(sctx, rawVal, assign.Col.ToInfo(), false, false)
		}
		evalBuffer.SetDatum(assign.Col.Index, newData[idxInCols])

		err = errorHandler(sctx, assign, &rawVal, err)
		if err != nil {
			return false, err
		}

		if err := checkColumnFunc(idxInCols, false); err != nil {
			return false, err
		}
	}

	// Step 5: handle bad null errors and exchange partition errors.
	for i, col := range t.Cols() {
		var err error
		if err = col.HandleBadNull(&newData[i], sc); err != nil {
			return false, err
		}
	}

	// Handle exchange partition
	tbl := t.Meta()
	if tbl.ExchangePartitionInfo != nil {
		is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
		pt, tableFound := is.TableByID(tbl.ExchangePartitionInfo.ExchangePartitionID)
		if !tableFound {
			return false, errors.Errorf("exchange partition process table by id failed")
		}
		p, ok := pt.(table.PartitionedTable)
		if !ok {
			return false, errors.Errorf("exchange partition process assert table partition failed")
		}
		err := p.CheckForExchangePartition(
			sctx,
			pt.Meta().Partition,
			newData,
			tbl.ExchangePartitionInfo.ExchangePartitionDefID,
		)
		if err != nil {
			return false, err
		}
	}

	// If handle changed, remove the old then add the new record, otherwise update the record.
	if handleChanged {
		// For `UPDATE IGNORE`/`INSERT IGNORE ON DUPLICATE KEY UPDATE`
		// we use the staging buffer so that we don't need to precheck the existence of handle or unique keys by sending
		// extra kv requests, and the remove action will not take effect if there are conflicts.
		if updated, err := func() (bool, error) {
			txn, err := sctx.Txn(true)
			if err != nil {
				return false, err
			}
			memBuffer := txn.GetMemBuffer()
			sh := memBuffer.Staging()
			defer memBuffer.Cleanup(sh)

			if err = t.RemoveRecord(sctx, h, oldData); err != nil {
				return false, err
			}

			_, err = t.AddRecord(sctx, newData, table.IsUpdate, table.WithCtx(ctx))
			if err != nil {
				return false, err
			}
			memBuffer.Release(sh)
			return true, nil
		}(); err != nil {
			if terr, ok := errors.Cause(err).(*terror.Error); sctx.GetSessionVars().StmtCtx.IgnoreNoPartition && ok && terr.Code() == errno.ErrNoPartitionForGivenValue {
				return false, nil
			}
			return updated, err
		}
	} else {
		// Update record to new value and update index.
		if err := t.UpdateRecord(ctx, sctx, h, oldData, newData, modified); err != nil {
			if terr, ok := errors.Cause(err).(*terror.Error); sctx.GetSessionVars().StmtCtx.IgnoreNoPartition && ok && terr.Code() == errno.ErrNoPartitionForGivenValue {
				return false, nil
			}
			return false, err
		}
	}
	for _, fkt := range fkChecks {
		err := fkt.updateRowNeedToCheck(sc, oldData, newData)
		if err != nil {
			return false, err
		}
	}
	for _, fkc := range fkCascades {
		err := fkc.onUpdateRow(sc, oldData, newData)
		if err != nil {
			return false, err
		}
	}
	if onDup {
		sc.AddAffectedRows(2)
	} else {
		sc.AddAffectedRows(1)
	}
	sc.AddUpdatedRows(1)
	sc.AddCopiedRows(1)

	return true, nil
}

func appendUnchangedRowForLock(sctx sessionctx.Context, t table.Table, h kv.Handle, row []types.Datum) (bool, error) {
	txnCtx := sctx.GetSessionVars().TxnCtx
	if !txnCtx.IsPessimistic {
		return false, nil
	}
	physicalID := t.Meta().ID
	if pt, ok := t.(table.PartitionedTable); ok {
		p, err := pt.GetPartitionByRow(sctx, row)
		if err != nil {
			return false, err
		}
		physicalID = p.GetPhysicalID()
	}
	unchangedRowKey := tablecodec.EncodeRowKeyWithHandle(physicalID, h)
	txnCtx.AddUnchangedRowKey(unchangedRowKey)
	return true, nil
}

func rebaseAutoRandomValue(ctx context.Context, sctx sessionctx.Context, t table.Table, newData *types.Datum, col *table.Column) error {
	tableInfo := t.Meta()
	if !tableInfo.ContainsAutoRandomBits() {
		return nil
	}
	recordID, err := getAutoRecordID(*newData, &col.FieldType, false)
	if err != nil {
		return err
	}
	if recordID < 0 {
		return nil
	}
	shardFmt := autoid.NewShardIDFormat(&col.FieldType, tableInfo.AutoRandomBits, tableInfo.AutoRandomRangeBits)
	// Set bits except incremental_bits to zero.
	recordID = recordID & shardFmt.IncrementalMask()
	return t.Allocators(sctx).Get(autoid.AutoRandomType).Rebase(ctx, recordID, true)
}

// resetErrDataTooLong reset ErrDataTooLong error msg.
// types.ErrDataTooLong is produced in types.ProduceStrWithSpecifiedTp, there is no column info in there,
// so we reset the error msg here, and wrap old err with errors.Wrap.
func resetErrDataTooLong(colName string, rowIdx int, err error) error {
	newErr := types.ErrDataTooLong.GenWithStack("Data too long for column '%v' at row %v", colName, rowIdx)
	return newErr
}
