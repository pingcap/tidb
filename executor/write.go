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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
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

	// We can iterate on public columns not writable columns,
	// because all of them are sorted by their `Offset`, which
	// causes all writable columns are after public columns.

	// Handle the bad null error.
	for i, col := range t.Cols() {
		var err error
		if err = col.HandleBadNull(&newData[i], sc); err != nil {
			return false, err
		}
	}

	// Compare datum, then handle some flags.
	for i, col := range t.Cols() {
		// We should use binary collation to compare datum, otherwise the result will be incorrect.
		cmp, err := newData[i].Compare(sc, &oldData[i], collate.GetBinaryCollator())
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			changed = true
			modified[i] = true
			// Rebase auto increment id if the field is changed.
			if mysql.HasAutoIncrementFlag(col.GetFlag()) {
				recordID, err := getAutoRecordID(newData[i], &col.FieldType, false)
				if err != nil {
					return false, err
				}
				if err = t.Allocators(sctx).Get(autoid.RowIDAllocType).Rebase(ctx, recordID, true); err != nil {
					return false, err
				}
			}
			if col.IsPKHandleColumn(t.Meta()) {
				handleChanged = true
				// Rebase auto random id if the field is changed.
				if err := rebaseAutoRandomValue(ctx, sctx, t, &newData[i], col); err != nil {
					return false, err
				}
			}
			if col.IsCommonHandleColumn(t.Meta()) {
				handleChanged = true
			}
		} else {
			if mysql.HasOnUpdateNowFlag(col.GetFlag()) && modified[i] {
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

	// Fill values into on-update-now fields, only if they are really changed.
	for i, col := range t.Cols() {
		if mysql.HasOnUpdateNowFlag(col.GetFlag()) && !modified[i] && !onUpdateSpecified[i] {
			if v, err := expression.GetTimeValue(sctx, strings.ToUpper(ast.CurrentTimestamp), col.GetType(), col.GetDecimal()); err == nil {
				newData[i] = v
				modified[i] = true
			} else {
				return false, err
			}
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
		if err = t.UpdateRecord(ctx, sctx, h, oldData, newData, modified); err != nil {
			if terr, ok := errors.Cause(err).(*terror.Error); sctx.GetSessionVars().StmtCtx.IgnoreNoPartition && ok && terr.Code() == errno.ErrNoPartitionForGivenValue {
				return false, nil
			}
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
	layout := autoid.NewShardIDLayout(&col.FieldType, tableInfo.AutoRandomBits)
	// Set bits except incremental_bits to zero.
	recordID = recordID & (1<<layout.IncrementalBits - 1)
	return t.Allocators(sctx).Get(autoid.AutoRandomType).Rebase(ctx, recordID, true)
}

// resetErrDataTooLong reset ErrDataTooLong error msg.
// types.ErrDataTooLong is produced in types.ProduceStrWithSpecifiedTp, there is no column info in there,
// so we reset the error msg here, and wrap old err with errors.Wrap.
func resetErrDataTooLong(colName string, rowIdx int, err error) error {
	newErr := types.ErrDataTooLong.GenWithStack("Data too long for column '%v' at row %v", colName, rowIdx)
	return newErr
}
