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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

var (
	_ exec.Executor = &UpdateExec{}
	_ exec.Executor = &DeleteExec{}
	_ exec.Executor = &InsertExec{}
	_ exec.Executor = &ReplaceExec{}
	_ exec.Executor = &LoadDataExec{}
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
 *  2. ignored (bool): does the row is ignored during fkcheck
 *  3. err (error): error in the update.
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
	dupKeyMode table.DupKeyCheckMode,
	ignoreErr bool,
) (changed bool, ignored bool, retErr error) {
	r, ctx := tracing.StartRegionEx(ctx, "executor.updateRecord")
	defer r.End()

	sessVars := sctx.GetSessionVars()
	sc := sessVars.StmtCtx

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
		cmp, err := newData[i].Compare(sc.TypeCtx(), &oldData[i], collate.GetBinaryCollator())
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
				if err = t.Allocators(sctx.GetTableCtx()).Get(autoid.AutoIncrementType).Rebase(ctx, recordID, true); err != nil {
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
	// Step 5: handle foreign key errors, bad null errors and exchange partition errors.
	// After these are done, we can finally update the record.

	// Step 2: compare already evaluated columns and update changed, handleChanged and handleChanged flags.
	for i := range cols {
		if err := checkColumnFunc(i, true); err != nil {
			return false, false, err
		}
	}

	// If no changes, nothing to do, return directly.
	if !changed {
		sc.AddTouchedRows(1)
		// See https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if sessVars.ClientCapability&mysql.ClientFoundRows > 0 {
			sc.AddAffectedRows(1)
		}
		keySet := lockRowKey
		if sessVars.LockUnchangedKeys {
			keySet |= lockUniqueKeys
		}
		_, err := addUnchangedKeysForLockByRow(sctx, t, h, oldData, keySet)
		return false, false, err
	}

	// Step 3: fill values into on-update-now fields.
	for i, col := range t.Cols() {
		var err error
		if mysql.HasOnUpdateNowFlag(col.GetFlag()) && onUpdateNeedModify[i] {
			newData[i], err = expression.GetTimeValue(sctx.GetExprCtx(), strings.ToUpper(ast.CurrentTimestamp), col.GetType(), col.GetDecimal(), nil)
			modified[i] = true
			// For update statement, evalBuffer is initialized on demand.
			if chunk.Row(evalBuffer).Chunk() != nil {
				evalBuffer.SetDatum(i+offset, newData[i])
			}
			if err != nil {
				return false, false, err
			}
			// Only TIMESTAMP and DATETIME columns can be automatically updated, so it cannot be PKIsHandle.
			// Ref: https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html
			if col.IsPKHandleColumn(t.Meta()) {
				return false, false, errors.Errorf("on-update-now column should never be pk-is-handle")
			}
			if col.IsCommonHandleColumn(t.Meta()) {
				handleChanged = true
			}
		}
	}

	// Step 4: fill auto generated columns
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	for _, assign := range assignments {
		// Insert statements may have LazyErr, handle it first.
		if assign.LazyErr != nil {
			return false, false, assign.LazyErr
		}

		// For Update statements, Index may be larger than len(newData)
		// e.g. update t a, t b set a.c1 = 1, b.c2 = 2;
		idxInCols := assign.Col.Index - offset
		rawVal, err := assign.Expr.Eval(evalCtx, evalBuffer.ToRow())
		if err == nil {
			newData[idxInCols], err = table.CastValue(sctx, rawVal, assign.Col.ToInfo(), false, false)
		}
		evalBuffer.SetDatum(assign.Col.Index, newData[idxInCols])

		err = errorHandler(sctx, assign, &rawVal, err)
		if err != nil {
			return false, false, err
		}

		if err := checkColumnFunc(idxInCols, false); err != nil {
			return false, false, err
		}
	}

	// Step 5: handle foreign key errors, bad null errors and exchange partition errors.
	if ignoreErr {
		ignored, err := checkFKIgnoreErr(ctx, sctx, fkChecks, newData)
		if err != nil {
			return false, false, err
		}

		// meets an error, skip this row.
		if ignored {
			return false, true, nil
		}
	}

	for i, col := range t.Cols() {
		var err error
		if err = col.HandleBadNull(sc.ErrCtx(), &newData[i], 0); err != nil {
			return false, false, err
		}
	}

	tbl := t.Meta()
	if tbl.ExchangePartitionInfo != nil && tbl.GetPartitionInfo() == nil {
		if err := checkRowForExchangePartition(sctx, newData, tbl); err != nil {
			return false, false, err
		}
	}

	sc.AddTouchedRows(1)
	pessimisticLazyCheck := getPessimisticLazyCheckMode(sessVars)
	txn, err := sctx.Txn(true)
	if err != nil {
		return false, false, err
	}
	// If handle changed, remove the old then add the new record, otherwise update the record.
	if handleChanged {
		// For `UPDATE IGNORE`/`INSERT IGNORE ON DUPLICATE KEY UPDATE`
		// we use the staging buffer so that we don't need to precheck the existence of handle or unique keys by sending
		// extra kv requests, and the remove action will not take effect if there are conflicts.
		if updated, err := func() (bool, error) {
			memBuffer := txn.GetMemBuffer()
			sh := memBuffer.Staging()
			defer memBuffer.Cleanup(sh)

			if err = t.RemoveRecord(sctx.GetTableCtx(), txn, h, oldData); err != nil {
				return false, err
			}

			_, err = t.AddRecord(sctx.GetTableCtx(), txn, newData, table.IsUpdate, table.WithCtx(ctx), dupKeyMode, pessimisticLazyCheck)
			if err != nil {
				return false, err
			}
			memBuffer.Release(sh)
			return true, nil
		}(); err != nil {
			if terr, ok := errors.Cause(err).(*terror.Error); ok && (terr.Code() == errno.ErrNoPartitionForGivenValue || terr.Code() == errno.ErrRowDoesNotMatchGivenPartitionSet) {
				ec := sc.ErrCtx()
				return false, false, ec.HandleError(err)
			}
			return updated, false, err
		}
	} else {
		var opts []table.UpdateRecordOption
		if sessVars.InTxn() || sc.InHandleForeignKeyTrigger || sc.ForeignKeyTriggerCtx.HasFKCascades {
			// If txn is auto commit and index is untouched, no need to write index value.
			// If InHandleForeignKeyTrigger or ForeignKeyTriggerCtx.HasFKCascades is true indicate we may have
			// foreign key cascade need to handle later, then we still need to write index value,
			// otherwise, the later foreign cascade executor may see data-index inconsistency in txn-mem-buffer.
			opts = []table.UpdateRecordOption{table.WithCtx(ctx), dupKeyMode, pessimisticLazyCheck}
		} else {
			opts = []table.UpdateRecordOption{table.WithCtx(ctx), dupKeyMode, pessimisticLazyCheck, table.SkipWriteUntouchedIndices}
		}

		// Update record to new value and update index.
		if err := t.UpdateRecord(sctx.GetTableCtx(), txn, h, oldData, newData, modified, opts...); err != nil {
			if terr, ok := errors.Cause(err).(*terror.Error); ok && (terr.Code() == errno.ErrNoPartitionForGivenValue || terr.Code() == errno.ErrRowDoesNotMatchGivenPartitionSet) {
				ec := sc.ErrCtx()
				return false, false, ec.HandleError(err)
			}
			return false, false, err
		}
		if sessVars.LockUnchangedKeys {
			// Lock unique keys when handle unchanged
			if _, err := addUnchangedKeysForLockByRow(sctx, t, h, oldData, lockUniqueKeys); err != nil {
				return false, false, err
			}
		}
	}
	if !ignoreErr {
		for _, fkt := range fkChecks {
			err := fkt.updateRowNeedToCheck(sc, oldData, newData)
			if err != nil {
				return false, false, err
			}
		}
	}
	for _, fkc := range fkCascades {
		err := fkc.onUpdateRow(sc, oldData, newData)
		if err != nil {
			return false, false, err
		}
	}
	if onDup {
		sc.AddAffectedRows(2)
	} else {
		sc.AddAffectedRows(1)
	}
	sc.AddUpdatedRows(1)
	sc.AddCopiedRows(1)

	return true, false, nil
}

const (
	lockRowKey = 1 << iota
	lockUniqueKeys
)

func addUnchangedKeysForLockByRow(
	sctx sessionctx.Context, t table.Table, h kv.Handle, row []types.Datum, keySet int,
) (int, error) {
	txnCtx := sctx.GetSessionVars().TxnCtx
	if !txnCtx.IsPessimistic || keySet == 0 {
		return 0, nil
	}
	count := 0
	physicalID := t.Meta().ID
	if pt, ok := t.(table.PartitionedTable); ok {
		p, err := pt.GetPartitionByRow(sctx.GetExprCtx().GetEvalCtx(), row)
		if err != nil {
			return 0, err
		}
		physicalID = p.GetPhysicalID()
	}
	if keySet&lockRowKey > 0 {
		unchangedRowKey := tablecodec.EncodeRowKeyWithHandle(physicalID, h)
		txnCtx.AddUnchangedKeyForLock(unchangedRowKey)
		count++
	}
	if keySet&lockUniqueKeys > 0 {
		stmtCtx := sctx.GetSessionVars().StmtCtx
		clustered := t.Meta().HasClusteredIndex()
		for _, idx := range t.Indices() {
			meta := idx.Meta()
			if !meta.Unique || !meta.IsPublic() || (meta.Primary && clustered) {
				continue
			}
			ukVals, err := idx.FetchValues(row, nil)
			if err != nil {
				return count, err
			}
			unchangedUniqueKey, _, err := tablecodec.GenIndexKey(
				stmtCtx.TimeZone(),
				idx.TableMeta(),
				meta,
				physicalID,
				ukVals,
				h,
				nil,
			)
			err = stmtCtx.HandleError(err)
			if err != nil {
				return count, err
			}
			txnCtx.AddUnchangedKeyForLock(unchangedUniqueKey)
			count++
		}
	}
	return count, nil
}

func rebaseAutoRandomValue(
	ctx context.Context, sctx sessionctx.Context, t table.Table, newData *types.Datum, col *table.Column,
) error {
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
	return t.Allocators(sctx.GetTableCtx()).Get(autoid.AutoRandomType).Rebase(ctx, recordID, true)
}

// resetErrDataTooLong reset ErrDataTooLong error msg.
// types.ErrDataTooLong is produced in types.ProduceStrWithSpecifiedTp, there is no column info in there,
// so we reset the error msg here, and wrap old err with errors.Wrap.
func resetErrDataTooLong(colName string, rowIdx int, _ error) error {
	newErr := types.ErrDataTooLong.FastGen("Data too long for column '%v' at row %v", colName, rowIdx)
	return newErr
}

// checkRowForExchangePartition is only used for ExchangePartition by non-partitionTable during write only state.
// It check if rowData inserted or updated violate partition definition or checkConstraints of partitionTable.
func checkRowForExchangePartition(sctx sessionctx.Context, row []types.Datum, tbl *model.TableInfo) error {
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	pt, tableFound := is.TableByID(context.Background(), tbl.ExchangePartitionInfo.ExchangePartitionTableID)
	if !tableFound {
		return errors.Errorf("exchange partition process table by id failed")
	}
	p, ok := pt.(table.PartitionedTable)
	if !ok {
		return errors.Errorf("exchange partition process assert table partition failed")
	}
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	err := p.CheckForExchangePartition(
		evalCtx,
		pt.Meta().Partition,
		row,
		tbl.ExchangePartitionInfo.ExchangePartitionDefID,
		tbl.ID,
	)
	if err != nil {
		return err
	}
	if variable.EnableCheckConstraint.Load() {
		if err = table.CheckRowConstraintWithDatum(evalCtx, pt.WritableConstraint(), row); err != nil {
			// TODO: make error include ExchangePartition info.
			return err
		}
	}
	return nil
}
