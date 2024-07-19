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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
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

// updateRecord updates the row specified by the handle `h`, from `oldData` to `newData`.
// `modified` means which columns are really modified. It's used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
// The return values:
//  1. changed (bool) : does the update really change the row values. e.g. update set i = 1 where i = 1;
//  2. err (error) : error in the update.
func updateRecord(
	ctx context.Context, sctx sessionctx.Context, h kv.Handle, oldData, newData []types.Datum, modified []bool,
	t table.Table,
	onDup bool, _ *memory.Tracker, fkChecks []*FKCheckExec, fkCascades []*FKCascadeExec,
) (bool, error) {
	r, ctx := tracing.StartRegionEx(ctx, "executor.updateRecord")
	defer r.End()

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
		if err = col.HandleBadNull(sc.ErrCtx(), &newData[i], 0); err != nil {
			return false, err
		}
	}

	// Handle exchange partition
	tbl := t.Meta()
	if tbl.ExchangePartitionInfo != nil && tbl.GetPartitionInfo() == nil {
		if err := checkRowForExchangePartition(sctx.GetTableCtx(), newData, tbl); err != nil {
			return false, err
		}
	}

	// Compare datum, then handle some flags.
	for i, col := range t.Cols() {
		// We should use binary collation to compare datum, otherwise the result will be incorrect.
		cmp, err := newData[i].Compare(sc.TypeCtx(), &oldData[i], collate.GetBinaryCollator())
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
				if err = t.Allocators(sctx.GetTableCtx()).Get(autoid.AutoIncrementType).Rebase(ctx, recordID, true); err != nil {
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
		keySet := lockRowKey
		if sctx.GetSessionVars().LockUnchangedKeys {
			keySet |= lockUniqueKeys
		}
		_, err := addUnchangedKeysForLockByRow(sctx, t, h, oldData, keySet)
		return false, err
	}

	// Fill values into on-update-now fields, only if they are really changed.
	for i, col := range t.Cols() {
		if mysql.HasOnUpdateNowFlag(col.GetFlag()) && !modified[i] && !onUpdateSpecified[i] {
			v, err := expression.GetTimeValue(sctx.GetExprCtx(), strings.ToUpper(ast.CurrentTimestamp), col.GetType(), col.GetDecimal(), nil)
			if err != nil {
				return false, err
			}
			newData[i] = v
			modified[i] = true
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

			if err = t.RemoveRecord(sctx.GetTableCtx(), h, oldData); err != nil {
				return false, err
			}

			_, err = t.AddRecord(sctx.GetTableCtx(), newData, table.IsUpdate, table.WithCtx(ctx))
			if err != nil {
				return false, err
			}
			memBuffer.Release(sh)
			return true, nil
		}(); err != nil {
			if terr, ok := errors.Cause(err).(*terror.Error); ok && (terr.Code() == errno.ErrNoPartitionForGivenValue || terr.Code() == errno.ErrRowDoesNotMatchGivenPartitionSet) {
				ec := sctx.GetSessionVars().StmtCtx.ErrCtx()
				return false, ec.HandleError(err)
			}
			return updated, err
		}
	} else {
		// Update record to new value and update index.
		if err := t.UpdateRecord(ctx, sctx.GetTableCtx(), h, oldData, newData, modified); err != nil {
			if terr, ok := errors.Cause(err).(*terror.Error); ok && (terr.Code() == errno.ErrNoPartitionForGivenValue || terr.Code() == errno.ErrRowDoesNotMatchGivenPartitionSet) {
				ec := sctx.GetSessionVars().StmtCtx.ErrCtx()
				return false, ec.HandleError(err)
			}
			return false, err
		}
		if sctx.GetSessionVars().LockUnchangedKeys {
			// Lock unique keys when handle unchanged
			if _, err := addUnchangedKeysForLockByRow(sctx, t, h, oldData, lockUniqueKeys); err != nil {
				return false, err
			}
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
func checkRowForExchangePartition(sctx table.MutateContext, row []types.Datum, tbl *model.TableInfo) error {
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	pt, tableFound := is.TableByID(tbl.ExchangePartitionInfo.ExchangePartitionTableID)
	if !tableFound {
		return errors.Errorf("exchange partition process table by id failed")
	}
	p, ok := pt.(table.PartitionedTable)
	if !ok {
		return errors.Errorf("exchange partition process assert table partition failed")
	}
	err := p.CheckForExchangePartition(
		sctx.GetExprCtx().GetEvalCtx(),
		pt.Meta().Partition,
		row,
		tbl.ExchangePartitionInfo.ExchangePartitionDefID,
		tbl.ID,
	)
	if err != nil {
		return err
	}
	if variable.EnableCheckConstraint.Load() {
		type CheckConstraintTable interface {
			CheckRowConstraint(ctx table.MutateContext, rowToCheck []types.Datum) error
		}
		cc, ok := pt.(CheckConstraintTable)
		if !ok {
			return errors.Errorf("exchange partition process assert check constraint failed")
		}
		err := cc.CheckRowConstraint(sctx, row)
		if err != nil {
			// TODO: make error include ExchangePartition info.
			return err
		}
	}
	return nil
}
