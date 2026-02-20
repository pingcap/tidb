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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

func (e *InsertValues) handleDuplicateKey(ctx context.Context, txn kv.Transaction, uk *keyValueWithDupInfo, replace bool, r toBeCheckedRow) (bool, error) {
	if !replace {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(uk.dupErr)
		if txnCtx := e.Ctx().GetSessionVars().TxnCtx; txnCtx.IsPessimistic && e.Ctx().GetSessionVars().LockUnchangedKeys {
			txnCtx.AddUnchangedKeyForLock(uk.newKey, false)
		}
		return true, nil
	}
	handle, err := tables.FetchDuplicatedHandle(ctx, uk.newKey, txn)
	if err != nil {
		return false, err
	}
	if handle == nil {
		return false, nil
	}
	return e.removeRow(ctx, txn, handle, r, true)
}

// batchCheckAndInsert checks rows with duplicate errors.
// All duplicate rows will be ignored and appended as duplicate warnings.
func (e *InsertValues) batchCheckAndInsert(
	ctx context.Context, rows [][]types.Datum,
	addRecord func(ctx context.Context, row []types.Datum, dupKeyCheck table.DupKeyCheckMode) error,
	replace bool,
) error {
	defer tracing.StartRegion(ctx, "InsertValues.batchCheckAndInsert").End()
	start := time.Now()
	// Get keys need to be checked.
	toBeCheckedRows, err := getKeysNeedCheck(e.Ctx(), e.Table, rows)
	if err != nil {
		return err
	}

	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	setOptionForTopSQL(e.Ctx().GetSessionVars().StmtCtx, txn)
	sc := e.Ctx().GetSessionVars().StmtCtx
	for _, fkc := range e.fkChecks {
		err = fkc.checkRows(ctx, sc, txn, toBeCheckedRows)
		if err != nil {
			return err
		}
	}
	prefetchStart := time.Now()
	// Fill cache using BatchGet, the following Get requests don't need to visit TiKV.
	// Temporary table need not to do prefetch because its all data are stored in the memory.
	if e.Table.Meta().TempTableType == model.TempTableNone {
		if _, err = prefetchUniqueIndices(ctx, txn, toBeCheckedRows); err != nil {
			return err
		}
	}

	if e.stats != nil {
		e.stats.FKCheckTime += prefetchStart.Sub(start)
		e.stats.Prefetch += time.Since(prefetchStart)
	}

	// append warnings and get no duplicated error rows
	for i, r := range toBeCheckedRows {
		if r.ignored {
			continue
		}
		if r.handleKey != nil {
			_, err := txn.Get(ctx, r.handleKey.newKey)
			if err == nil {
				if !replace {
					e.Ctx().GetSessionVars().StmtCtx.AppendWarning(r.handleKey.dupErr)
					if txnCtx := e.Ctx().GetSessionVars().TxnCtx; txnCtx.IsPessimistic &&
						e.Ctx().GetSessionVars().LockUnchangedKeys {
						// lock duplicated row key on insert-ignore
						txnCtx.AddUnchangedKeyForLock(r.handleKey.newKey, false)
					}
					continue
				}
				handle, err := tablecodec.DecodeRowKey(r.handleKey.newKey)
				if err != nil {
					return err
				}
				unchanged, err2 := e.removeRow(ctx, txn, handle, r, false)
				if err2 != nil {
					return err2
				}
				if unchanged {
					// we don't need to add the identical row again, but the
					// counter should act as if we did.
					e.Ctx().GetSessionVars().StmtCtx.AddCopiedRows(1)
					continue
				}
			} else if !kv.IsErrNotFound(err) {
				return err
			}
		}

		rowInserted := false
		for _, uk := range r.uniqueKeys {
			_, err := txn.Get(ctx, uk.newKey)
			if err != nil && !kv.IsErrNotFound(err) {
				return err
			}
			if err == nil {
				rowInserted, err = e.handleDuplicateKey(ctx, txn, uk, replace, r)
				if err != nil {
					return err
				}
				if rowInserted {
					break
				}
				continue
			}
			if tablecodec.IsTempIndexKey(uk.newKey) {
				tablecodec.TempIndexKey2IndexKey(uk.newKey)
				_, err = txn.Get(ctx, uk.newKey)
				if err != nil && !kv.IsErrNotFound(err) {
					return err
				}
				if err == nil {
					rowInserted, err = e.handleDuplicateKey(ctx, txn, uk, replace, r)
					if err != nil {
						return err
					}
					if rowInserted {
						break
					}
				}
			}
		}

		if rowInserted {
			continue
		}

		// If row was checked with no duplicate keys,
		// it should be added to values map for the further row check.
		// There may be duplicate keys inside the insert statement.
		e.Ctx().GetSessionVars().StmtCtx.AddCopiedRows(1)
		// all the rows have been checked, so it is safe to use DupKeyCheckSkip
		err = addRecord(ctx, rows[i], table.DupKeyCheckSkip)
		if err != nil {
			// throw warning when violate check constraint
			if table.ErrCheckConstraintViolated.Equal(err) {
				if !sc.InLoadDataStmt {
					sc.AppendWarning(err)
				}
				continue
			}
			return err
		}
	}
	if e.stats != nil {
		e.stats.CheckInsertTime += time.Since(start)
	}
	return nil
}

// removeRow removes the duplicate row and cleanup its keys in the key-value map.
// But if the to-be-removed row equals to the to-be-added row, no remove or add
// things to do and return (true, nil).
func (e *InsertValues) removeRow(
	ctx context.Context,
	txn kv.Transaction,
	handle kv.Handle,
	r toBeCheckedRow,
	inReplace bool,
) (bool, error) {
	newRow := r.row
	oldRow, err := getOldRow(ctx, e.Ctx(), txn, r.t, handle, e.GenExprs)
	if err != nil {
		logutil.BgLogger().Error(
			"get old row failed when replace",
			zap.String("handle", handle.String()),
			zap.String("toBeInsertedRow", types.DatumsToStrNoErr(r.row)),
		)
		if kv.IsErrNotFound(err) {
			err = errors.NotFoundf("can not be duplicated row, due to old row not found. handle %s", handle)
		}
		return false, err
	}

	identical, err := e.equalDatumsAsBinary(oldRow, newRow)
	if err != nil {
		return false, err
	}
	if identical {
		if inReplace {
			e.Ctx().GetSessionVars().StmtCtx.AddAffectedRows(1)
		}
		keySet := lockRowKey
		if e.Ctx().GetSessionVars().LockUnchangedKeys {
			keySet |= lockUniqueKeys
		}
		if _, err := addUnchangedKeysForLockByRow(e.Ctx(), r.t, handle, oldRow, keySet); err != nil {
			return false, err
		}
		return true, nil
	}

	if ph, ok := handle.(kv.PartitionHandle); ok {
		err = e.Table.(table.PartitionedTable).GetPartition(ph.PartitionID).RemoveRecord(e.Ctx().GetTableCtx(), txn, ph.Handle, oldRow)
	} else {
		err = r.t.RemoveRecord(e.Ctx().GetTableCtx(), txn, handle, oldRow)
	}
	if err != nil {
		return false, err
	}
	err = onRemoveRowForFK(e.Ctx(), oldRow, e.fkChecks, e.fkCascades, e.ignoreErr)
	if err != nil {
		return false, err
	}
	if inReplace {
		e.Ctx().GetSessionVars().StmtCtx.AddAffectedRows(1)
	} else {
		e.Ctx().GetSessionVars().StmtCtx.AddDeletedRows(1)
	}

	return false, nil
}

// equalDatumsAsBinary compare if a and b contains the same datum values in binary collation.
func (e *InsertValues) equalDatumsAsBinary(a []types.Datum, b []types.Datum) (bool, error) {
	if len(a) != len(b) {
		return false, nil
	}
	for i, ai := range a {
		v, err := ai.Compare(e.Ctx().GetSessionVars().StmtCtx.TypeCtx(), &b[i], collate.GetBinaryCollator())
		if err != nil {
			return false, errors.Trace(err)
		}
		if v != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (e *InsertValues) addRecord(ctx context.Context, row []types.Datum, dupKeyCheck table.DupKeyCheckMode) error {
	return e.addRecordWithAutoIDHint(ctx, row, 0, dupKeyCheck)
}

func (e *InsertValues) addRecordWithAutoIDHint(
	ctx context.Context, row []types.Datum, reserveAutoIDCount int, dupKeyCheck table.DupKeyCheckMode,
) (err error) {
	vars := e.Ctx().GetSessionVars()
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	pessimisticLazyCheck := getPessimisticLazyCheckMode(vars)
	if reserveAutoIDCount > 0 {
		_, err = e.Table.AddRecord(e.Ctx().GetTableCtx(), txn, row, table.WithCtx(ctx), table.WithReserveAutoIDHint(reserveAutoIDCount), dupKeyCheck, pessimisticLazyCheck)
	} else {
		_, err = e.Table.AddRecord(e.Ctx().GetTableCtx(), txn, row, table.WithCtx(ctx), dupKeyCheck, pessimisticLazyCheck)
	}
	if err != nil {
		return err
	}
	vars.StmtCtx.AddAffectedRows(1)
	if e.lastInsertID != 0 {
		vars.SetLastInsertID(e.lastInsertID)
	}
	if dupKeyCheck != table.DupKeyCheckSkip {
		for _, fkc := range e.fkChecks {
			err = fkc.insertRowNeedToCheck(vars.StmtCtx, row)
			if err != nil {
				return err
			}
		}
	}

	if e.Table.Meta().TTLInfo != nil {
		// update the TTL metrics if the table is a TTL table
		vars.TxnCtx.InsertTTLRowsCount++
	}

	return nil
}
