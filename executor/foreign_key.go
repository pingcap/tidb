// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"sync/atomic"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/set"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// WithForeignKeyTrigger indicates the executor has foreign key check or cascade.
type WithForeignKeyTrigger interface {
	GetFKChecks() []*FKCheckExec
}

// FKCheckExec uses to check foreign key constraint.
// When insert/update child table, need to check the row has related row exists in refer table.
// When insert/update parent table, need to check the row doesn't have related row exists in refer table.
type FKCheckExec struct {
	*plannercore.FKCheck
	*fkValueHelper
	ctx sessionctx.Context

	toBeLockedKeys []kv.Key

	checkRowsCache map[string]bool
}

func buildTblID2FKCheckExecs(sctx sessionctx.Context, tblID2Table map[int64]table.Table, tblID2FKChecks map[int64][]*plannercore.FKCheck) (map[int64][]*FKCheckExec, error) {
	var err error
	fkChecks := make(map[int64][]*FKCheckExec)
	for tid, tbl := range tblID2Table {
		fkChecks[tid], err = buildFKCheckExecs(sctx, tbl, tblID2FKChecks[tid])
		if err != nil {
			return nil, err
		}
	}
	return fkChecks, nil
}

func buildFKCheckExecs(sctx sessionctx.Context, tbl table.Table, fkChecks []*plannercore.FKCheck) ([]*FKCheckExec, error) {
	fkCheckExecs := make([]*FKCheckExec, 0, len(fkChecks))
	for _, fkCheck := range fkChecks {
		fkCheckExec, err := buildFKCheckExec(sctx, tbl, fkCheck)
		if err != nil {
			return nil, err
		}
		if fkCheckExec != nil {
			fkCheckExecs = append(fkCheckExecs, fkCheckExec)
		}
	}
	return fkCheckExecs, nil
}

func buildFKCheckExec(sctx sessionctx.Context, tbl table.Table, fkCheck *plannercore.FKCheck) (*FKCheckExec, error) {
	var cols []model.CIStr
	if fkCheck.FK != nil {
		cols = fkCheck.FK.Cols
	} else if fkCheck.ReferredFK != nil {
		cols = fkCheck.ReferredFK.Cols
	}
	colsOffsets, err := getFKColumnsOffsets(tbl.Meta(), cols)
	if err != nil {
		return nil, err
	}
	helper := &fkValueHelper{
		colsOffsets: colsOffsets,
		fkValuesSet: set.NewStringSet(),
	}
	fkCheck.ToBeCheckedKeys = plannercore.NewFKToBeCheckedKeys()
	return &FKCheckExec{
		ctx:           sctx,
		FKCheck:       fkCheck,
		fkValueHelper: helper,
	}, nil
}

func (fkc *FKCheckExec) insertRowNeedToCheck(sc *stmtctx.StatementContext, row []types.Datum) error {
	return fkc.addRowNeedToCheck(sc, row)
}

func (fkc *FKCheckExec) updateRowNeedToCheck(sc *stmtctx.StatementContext, oldRow, newRow []types.Datum) error {
	if fkc.FK != nil {
		return fkc.addRowNeedToCheck(sc, newRow)
	} else if fkc.ReferredFK != nil {
		return fkc.addRowNeedToCheck(sc, oldRow)
	}
	return nil
}

func (fkc *FKCheckExec) addRowNeedToCheck(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fkc.fetchFKValuesWithCheck(sc, row)
	if err != nil || len(vals) == 0 {
		return err
	}
	key, isPrefix, err := fkc.buildCheckKeyFromFKValue(sc, vals)
	if err != nil {
		return err
	}
	fkc.ToBeCheckedKeys.AddKey(key, isPrefix)
	return nil
}

func (fkc *FKCheckExec) doCheck(ctx context.Context) error {
	txn, err := fkc.ctx.Txn(false)
	if err != nil {
		return err
	}
	txnCtx := fkc.ctx.GetSessionVars().TxnCtx
	//txn.GetSnapshot().SetOption(kv.IsolationLevel, kv.RC)
	logutil.BgLogger().Info("------fk do check---------", zap.Uint64("start_ts", txnCtx.StartTS), zap.Uint64("update_ts", txnCtx.GetForUpdateTS()),
		zap.Int("unique_keys", len(fkc.ToBeCheckedKeys.Keys)),
		zap.Int("prefix_keys", len(fkc.ToBeCheckedKeys.PrefixKeys)))
	err = fkc.checkKeys(ctx, txn)
	if err != nil {
		return err
	}
	err = fkc.checkIndexKeys(ctx, txn)
	if err != nil {
		return err
	}
	if len(fkc.toBeLockedKeys) == 0 {
		return nil
	}
	sessVars := fkc.ctx.GetSessionVars()
	lockCtx, err := newLockCtx(fkc.ctx, sessVars.LockWaitTimeout, len(fkc.toBeLockedKeys))
	if err != nil {
		return err
	}
	// WARN: Since tidb current doesn't support `LOCK IN SHARE MODE`, therefore, performance will be very poor in concurrency cases.
	// TODO(crazycs520):After TiDB support `LOCK IN SHARE MODE`, use `LOCK IN SHARE MODE` here.
	forUpdate := atomic.LoadUint32(&sessVars.TxnCtx.ForUpdate)
	err = doLockKeys(ctx, fkc.ctx, lockCtx, fkc.toBeLockedKeys...)
	// doLockKeys may set TxnCtx.ForUpdate to 1, then if the lock meet write conflict, TiDB can't retry for update.
	// So reset TxnCtx.ForUpdate to 0 then can be retry if meet write conflict.
	atomic.StoreUint32(&sessVars.TxnCtx.ForUpdate, forUpdate)
	return err
}

func (fkc *FKCheckExec) buildCheckKeyFromFKValue(sc *stmtctx.StatementContext, vals []types.Datum) (key kv.Key, isPrefix bool, err error) {
	if fkc.IdxIsPrimaryKey {
		handleKey, err := fkc.buildHandleFromFKValues(sc, vals)
		if err != nil {
			return nil, false, err
		}
		key := tablecodec.EncodeRecordKey(fkc.Tbl.RecordPrefix(), handleKey)
		if fkc.IdxIsExclusive {
			return key, false, nil
		}
		return key, true, nil
	}
	key, distinct, err := fkc.Idx.GenIndexKey(sc, vals, nil, nil)
	if err != nil {
		return nil, false, err
	}
	if distinct && fkc.IdxIsExclusive {
		return key, false, nil
	}
	return key, true, nil
}

func (fkc *FKCheckExec) buildHandleFromFKValues(sc *stmtctx.StatementContext, vals []types.Datum) (kv.Handle, error) {
	if len(vals) == 1 && fkc.Idx == nil {
		return kv.IntHandle(vals[0].GetInt64()), nil
	}
	handleBytes, err := codec.EncodeKey(sc, nil, vals...)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

func (fkc *FKCheckExec) checkKeys(ctx context.Context, txn kv.Transaction) error {
	keys := fkc.ToBeCheckedKeys.Keys
	if len(keys) == 0 {
		return nil
	}
	err := fkc.prefetchKeys(ctx, txn, keys)
	if err != nil {
		return err
	}
	for _, k := range keys {
		err = fkc.checkKey(ctx, txn, k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fkc *FKCheckExec) prefetchKeys(ctx context.Context, txn kv.Transaction, keys []kv.Key) error {
	// Fill cache using BatchGet
	_, err := txn.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	return nil
}

func (fkc *FKCheckExec) checkKey(ctx context.Context, txn kv.Transaction, k kv.Key) error {
	if fkc.CheckExist {
		return fkc.checkKeyExist(ctx, txn, k)
	}
	return fkc.checkKeyNotExist(ctx, txn, k)
}

func (fkc *FKCheckExec) checkKeyExist(ctx context.Context, txn kv.Transaction, k kv.Key) error {
	_, err := txn.Get(ctx, k)
	if err == nil {
		fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, k)
		return nil
	}
	if kv.IsErrNotFound(err) {
		return fkc.FailedErr
	}
	return err
}

func (fkc *FKCheckExec) checkKeyNotExist(ctx context.Context, txn kv.Transaction, k kv.Key) error {
	_, err := txn.Get(ctx, k)
	if err == nil {
		return fkc.FailedErr
	}
	if kv.IsErrNotFound(err) {
		return nil
	}
	return err
}

func (fkc *FKCheckExec) checkIndexKeys(ctx context.Context, txn kv.Transaction) error {
	prefixKeys := fkc.ToBeCheckedKeys.PrefixKeys
	if len(prefixKeys) == 0 {
		return nil
	}
	memBuffer := txn.GetMemBuffer()
	snap := txn.GetSnapshot()
	snap.SetOption(kv.ScanBatchSize, 2)
	defer func() {
		snap.SetOption(kv.ScanBatchSize, txnsnapshot.DefaultScanBatchSize)
	}()
	for _, key := range prefixKeys {
		err := fkc.checkPrefixKey(ctx, memBuffer, snap, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fkc *FKCheckExec) checkPrefixKey(ctx context.Context, memBuffer kv.MemBuffer, snap kv.Snapshot, key kv.Key) error {
	key, value, err := fkc.getIndexKeyValueInTable(ctx, memBuffer, snap, key)
	if err != nil {
		return err
	}
	if fkc.CheckExist {
		return fkc.checkPrefixKeyExist(key, value)
	}
	if len(value) > 0 {
		// If check not exist, but the key is exist, return failedErr.
		return fkc.FailedErr
	}
	return nil
}

func (fkc *FKCheckExec) checkPrefixKeyExist(key kv.Key, value []byte) error {
	exist := len(value) > 0
	if !exist {
		return fkc.FailedErr
	}
	if fkc.Idx != nil && fkc.Idx.Meta().Primary && fkc.Tbl.Meta().IsCommonHandle {
		fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, key)
	} else {
		handle, err := tablecodec.DecodeIndexHandle(key, value, len(fkc.Idx.Meta().Columns))
		if err != nil {
			return err
		}
		handleKey := tablecodec.EncodeRecordKey(fkc.Tbl.RecordPrefix(), handle)
		fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, handleKey)
	}
	return nil
}

func (fkc *FKCheckExec) getIndexKeyValueInTable(ctx context.Context, memBuffer kv.MemBuffer, snap kv.Snapshot, key kv.Key) (k []byte, v []byte, _ error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	memIter, err := memBuffer.Iter(key, key.PrefixNext())
	if err != nil {
		return nil, nil, err
	}
	deletedKeys := set.NewStringSet()
	defer memIter.Close()
	for ; memIter.Valid(); err = memIter.Next() {
		if err != nil {
			return nil, nil, err
		}
		k := memIter.Key()
		if !k.HasPrefix(key) {
			break
		}
		// check whether the key was been deleted.
		if len(memIter.Value()) > 0 {
			return k, memIter.Value(), nil
		}
		deletedKeys.Insert(string(k))
	}

	it, err := snap.Iter(key, key.PrefixNext())
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	for ; it.Valid(); err = it.Next() {
		if err != nil {
			return nil, nil, err
		}
		k := it.Key()
		if !k.HasPrefix(key) {
			break
		}
		if !deletedKeys.Exist(string(k)) {
			return k, it.Value(), nil
		}
	}
	return nil, nil, nil
}

type fkValueHelper struct {
	colsOffsets []int
	fkValuesSet set.StringSet
}

func (h *fkValueHelper) fetchFKValuesWithCheck(sc *stmtctx.StatementContext, row []types.Datum) ([]types.Datum, error) {
	vals, err := h.fetchFKValues(row)
	if err != nil || h.hasNullValue(vals) {
		return nil, err
	}
	keyBuf, err := codec.EncodeKey(sc, nil, vals...)
	if err != nil {
		return nil, err
	}
	key := string(keyBuf)
	if h.fkValuesSet.Exist(key) {
		return nil, nil
	}
	h.fkValuesSet.Insert(key)
	return vals, nil
}

func (h *fkValueHelper) fetchFKValues(row []types.Datum) ([]types.Datum, error) {
	vals := make([]types.Datum, len(h.colsOffsets))
	for i, offset := range h.colsOffsets {
		if offset >= len(row) {
			return nil, table.ErrIndexOutBound.GenWithStackByArgs("", offset, row)
		}
		vals[i] = row[offset]
	}
	return vals, nil
}

func (h *fkValueHelper) hasNullValue(vals []types.Datum) bool {
	// If any foreign key column value is null, no need to check this row.
	// test case:
	// create table t1 (id int key,a int, b int, index(a, b));
	// create table t2 (id int key,a int, b int, foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);
	// > insert into t2 values (2, null, 1);
	// Query OK, 1 row affected
	// > insert into t2 values (3, 1, null);
	// Query OK, 1 row affected
	// > insert into t2 values (4, null, null);
	// Query OK, 1 row affected
	// > select * from t2;
	// 	+----+--------+--------+
	// 	| id | a      | b      |
	// 	+----+--------+--------+
	// 	| 4  | <null> | <null> |
	// 	| 2  | <null> | 1      |
	// 	| 3  | 1      | <null> |
	// 	+----+--------+--------+
	for _, val := range vals {
		if val.IsNull() {
			return true
		}
	}
	return false
}

func getFKColumnsOffsets(tbInfo *model.TableInfo, cols []model.CIStr) ([]int, error) {
	colsOffsets := make([]int, len(cols))
	for i, col := range cols {
		offset := -1
		for i := range tbInfo.Columns {
			if tbInfo.Columns[i].Name.L == col.L {
				offset = tbInfo.Columns[i].Offset
				break
			}
		}
		if offset < 0 {
			return nil, table.ErrUnknownColumn.GenWithStackByArgs(col.L)
		}
		colsOffsets[i] = offset
	}
	return colsOffsets, nil
}

type fkCheckKey struct {
	k        kv.Key
	isPrefix bool
}

func (fkc *FKCheckExec) checkRows(ctx context.Context, sc *stmtctx.StatementContext, txn kv.Transaction, rows []toBeCheckedRow) error {
	if len(rows) == 0 {
		return nil
	}
	if fkc.checkRowsCache == nil {
		fkc.checkRowsCache = map[string]bool{}
	}
	fkCheckKeys := make([]*fkCheckKey, len(rows))
	prefetchKeys := make([]kv.Key, 0, len(rows))
	for i, r := range rows {
		if r.ignored {
			continue
		}
		vals, err := fkc.fetchFKValues(r.row)
		if err != nil {
			return err
		}
		if fkc.hasNullValue(vals) {
			continue
		}
		key, isPrefix, err := fkc.buildCheckKeyFromFKValue(sc, vals)
		if err != nil {
			return err
		}
		fkCheckKeys[i] = &fkCheckKey{key, isPrefix}
		if !isPrefix {
			prefetchKeys = append(prefetchKeys, key)
		}
	}
	if len(prefetchKeys) > 0 {
		err := fkc.prefetchKeys(ctx, txn, prefetchKeys)
		if err != nil {
			return err
		}
	}
	memBuffer := txn.GetMemBuffer()
	snap := txn.GetSnapshot()
	snap.SetOption(kv.ScanBatchSize, 2)
	defer func() {
		snap.SetOption(kv.ScanBatchSize, 256)
	}()
	for i, fkCheckKey := range fkCheckKeys {
		if fkCheckKey == nil {
			continue
		}
		k := fkCheckKey.k
		if ignore, ok := fkc.checkRowsCache[string(k)]; ok {
			if ignore {
				rows[i].ignored = true
				sc.AppendWarning(fkc.FailedErr)
			}
			continue
		}
		var err error
		if fkCheckKey.isPrefix {
			err = fkc.checkPrefixKey(ctx, memBuffer, snap, k)
		} else {
			err = fkc.checkKey(ctx, txn, k)
		}
		if err != nil {
			rows[i].ignored = true
			sc.AppendWarning(fkc.FailedErr)
			fkc.checkRowsCache[string(k)] = true
		}
		fkc.checkRowsCache[string(k)] = false
	}
	return nil
}
