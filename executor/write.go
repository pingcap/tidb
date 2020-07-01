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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
)

var (
	_ Executor = &UpdateExec{}
	_ Executor = &DeleteExec{}
	_ Executor = &InsertExec{}
	_ Executor = &ReplaceExec{}
	_ Executor = &LoadDataExec{}
)

// updateRecord updates the row specified by the handle `h`, from `oldData` to `newData`.
// `modified` means which columns are really modified. It's used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
// The return values:
//     1. changed (bool) : does the update really change the row values. e.g. update set i = 1 where i = 1;
//     2. err (error) : error in the update.
func updateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, oldData, newData []types.Datum, modified []bool, t table.Table,
	onDup bool, memTracker *memory.Tracker) (bool, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("executor.updateRecord", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	txn, err := sctx.Txn(false)
	if err != nil {
		return false, err
	}
	memUsageOfTxnState := txn.Size()
	defer memTracker.Consume(int64(txn.Size() - memUsageOfTxnState))
	sc := sctx.GetSessionVars().StmtCtx
	changed, handleChanged := false, false
	// onUpdateSpecified is for "UPDATE SET ts_field = old_value", the
	// timestamp field is explicitly set, but not changed in fact.
	onUpdateSpecified := make(map[int]bool)
	var newHandle kv.Handle

	// We can iterate on public columns not writable columns,
	// because all of them are sorted by their `Offset`, which
	// causes all writable columns are after public columns.

	// 1. Cast modified values.
	for i, col := range t.Cols() {
		if modified[i] {
			// Cast changed fields with respective columns.
			v, err := table.CastValue(sctx, newData[i], col.ToInfo(), false, false)
			if err != nil {
				return false, err
			}
			newData[i] = v
		}
	}

	// 2. Handle the bad null error.
	for i, col := range t.Cols() {
		var err error
		if err = col.HandleBadNull(&newData[i], sc); err != nil {
			return false, err
		}
	}

	// 3. Compare datum, then handle some flags.
	for i, col := range t.Cols() {
		cmp, err := newData[i].CompareDatum(sc, &oldData[i])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			changed = true
			modified[i] = true
			// Rebase auto increment id if the field is changed.
			if mysql.HasAutoIncrementFlag(col.Flag) {
				recordID, err := getAutoRecordID(newData[i], &col.FieldType, false)
				if err != nil {
					return false, err
				}
				if err = t.RebaseAutoID(sctx, recordID, true, autoid.RowIDAllocType); err != nil {
					return false, err
				}
			}
			if col.IsPKHandleColumn(t.Meta()) {
				handleChanged = true
				newHandle = kv.IntHandle(newData[i].GetInt64())
				// Rebase auto random id if the field is changed.
				if err := rebaseAutoRandomValue(sctx, t, &newData[i], col); err != nil {
					return false, err
				}
			}
			if col.IsCommonHandleColumn(t.Meta()) {
				pkIdx := tables.FindPrimaryIndex(t.Meta())
				handleChanged = true
				pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
				for _, idxCol := range pkIdx.Columns {
					pkDts = append(pkDts, newData[idxCol.Offset])
				}
				handleBytes, err := codec.EncodeKey(sctx.GetSessionVars().StmtCtx, nil, pkDts...)
				if err != nil {
					return false, err
				}
				newHandle, err = kv.NewCommonHandle(handleBytes)
				if err != nil {
					return false, err
				}
			}
		} else {
			if mysql.HasOnUpdateNowFlag(col.Flag) && modified[i] {
				// It's for "UPDATE t SET ts = ts" and ts is a timestamp.
				onUpdateSpecified[i] = true
			}
			modified[i] = false
		}
	}

	sc.AddTouchedRows(1)
	// If no changes, nothing to do, return directly.
	if !changed {
		// See https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if sctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
			sc.AddAffectedRows(1)
		}

		physicalID := t.Meta().ID
		if pt, ok := t.(table.PartitionedTable); ok {
			p, err := pt.GetPartitionByRow(sctx, oldData)
			if err != nil {
				return false, err
			}
			physicalID = p.GetPhysicalID()
		}

		unchangedRowKey := tablecodec.EncodeRowKeyWithHandle(physicalID, h)
		txnCtx := sctx.GetSessionVars().TxnCtx
		if txnCtx.IsPessimistic {
			txnCtx.AddUnchangedRowKey(unchangedRowKey)
		}
		return false, nil
	}

	// 4. Fill values into on-update-now fields, only if they are really changed.
	for i, col := range t.Cols() {
		if mysql.HasOnUpdateNowFlag(col.Flag) && !modified[i] && !onUpdateSpecified[i] {
			if v, err := expression.GetTimeValue(sctx, strings.ToUpper(ast.CurrentTimestamp), col.Tp, int8(col.Decimal)); err == nil {
				newData[i] = v
				modified[i] = true
			} else {
				return false, err
			}
		}
	}

	// 5. If handle changed, remove the old then add the new record, otherwise update the record.
	if handleChanged {
		if sc.DupKeyAsWarning {
			// For `UPDATE IGNORE`/`INSERT IGNORE ON DUPLICATE KEY UPDATE`
			// If the new handle exists, this will avoid to remove the record.
			err = tables.CheckHandleExists(ctx, sctx, t, newHandle, newData)
			if err != nil {
				return false, err
			}
		}
		if err = t.RemoveRecord(sctx, h, oldData); err != nil {
			return false, err
		}
		// the `affectedRows` is increased when adding new record.
		if sc.DupKeyAsWarning {
			_, err = t.AddRecord(sctx, newData, table.IsUpdate, table.SkipHandleCheck, table.WithCtx(ctx))
		} else {
			_, err = t.AddRecord(sctx, newData, table.IsUpdate, table.WithCtx(ctx))
		}

		if err != nil {
			return false, err
		}
		if onDup {
			sc.AddAffectedRows(1)
		}
	} else {
		// Update record to new value and update index.
		if err = t.UpdateRecord(ctx, sctx, h, oldData, newData, modified); err != nil {
			return false, err
		}
		if onDup {
			sc.AddAffectedRows(2)
		} else {
			sc.AddAffectedRows(1)
		}
	}
	sc.AddUpdatedRows(1)
	sc.AddCopiedRows(1)

	return true, nil
}

func rebaseAutoRandomValue(sctx sessionctx.Context, t table.Table, newData *types.Datum, col *table.Column) error {
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
	layout := autoid.NewAutoRandomIDLayout(&col.FieldType, tableInfo.AutoRandomBits)
	// Set bits except incremental_bits to zero.
	recordID = recordID & (1<<layout.IncrementalBits - 1)
	return t.Allocators(sctx).Get(autoid.AutoRandomType).Rebase(tableInfo.ID, recordID, true)
}

// resetErrDataTooLong reset ErrDataTooLong error msg.
// types.ErrDataTooLong is produced in types.ProduceStrWithSpecifiedTp, there is no column info in there,
// so we reset the error msg here, and wrap old err with errors.Wrap.
func resetErrDataTooLong(colName string, rowIdx int, err error) error {
	newErr := types.ErrDataTooLong.GenWithStack("Data too long for column '%v' at row %v", colName, rowIdx)
	return newErr
}
