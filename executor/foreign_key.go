package executor

import (
	"context"
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
)

type ExecutorWithForeignKeyCheck interface {
	GetFKChecks() []*FKCheckExec
}

type FKCheckExec struct {
	*plannercore.FKCheck
	*FKValueHelper
	ctx sessionctx.Context

	toBeCheckedHandleKeys []kv.Handle
	toBeCheckedUniqueKeys []kv.Key
	toBeCheckedIndexKeys  []kv.Key
	toBeLockedKeys        []kv.Key
}

type FKValueHelper struct {
	colsOffsets []int
	fkValuesSet set.StringSet
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
	cols := fkCheck.FK.Cols
	colsOffsets, err := getColumnsOffsets(tbl.Meta(), cols)
	if err != nil {
		return nil, err
	}
	helper := &FKValueHelper{
		colsOffsets: colsOffsets,
		fkValuesSet: set.NewStringSet(),
	}
	return &FKCheckExec{
		ctx:           sctx,
		FKCheck:       fkCheck,
		FKValueHelper: helper,
	}, nil
}

func getColumnsOffsets(tbInfo *model.TableInfo, cols []model.CIStr) ([]int, error) {
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

func (fkc *FKCheckExec) addRowNeedToCheck(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fkc.fetchFKValuesWithCheck(sc, row)
	if len(vals) == 0 {
		return nil
	}
	if fkc.IdxIsPrimaryKey {
		handleKey, err := fkc.buildHandleFromFKValues(sc, vals)
		if err != nil {
			return err
		}
		if fkc.IdxIsExclusive {
			fkc.toBeCheckedHandleKeys = append(fkc.toBeCheckedHandleKeys, handleKey)
		} else {
			key := tablecodec.EncodeRecordKey(fkc.Tbl.RecordPrefix(), handleKey)
			fkc.toBeCheckedIndexKeys = append(fkc.toBeCheckedIndexKeys, key)
		}
		return nil
	}
	key, distinct, err := fkc.Idx.GenIndexKey(sc, vals, nil, nil)
	if err != nil {
		return err
	}
	if distinct && fkc.IdxIsExclusive {
		fkc.toBeCheckedUniqueKeys = append(fkc.toBeCheckedUniqueKeys, key)
	} else {
		fkc.toBeCheckedIndexKeys = append(fkc.toBeCheckedIndexKeys, key)
	}
	return nil
}

func (fkc *FKCheckExec) buildHandleFromFKValues(sc *stmtctx.StatementContext, vals []types.Datum) (kv.Handle, error) {
	if len(vals) == 1 && fkc.Idx == nil {
		return kv.IntHandle(vals[0].GetInt64()), nil
	}
	pkDts := make([]types.Datum, 0, len(vals))
	for i, val := range vals {
		if fkc.Idx != nil && len(fkc.HandleCols) > 0 {
			tablecodec.TruncateIndexValue(&val, fkc.Idx.Meta().Columns[i], fkc.HandleCols[i].ColumnInfo)
		}
		pkDts = append(pkDts, val)
	}
	handleBytes, err := codec.EncodeKey(sc, nil, pkDts...)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

func (h *FKValueHelper) fetchFKValuesWithCheck(sc *stmtctx.StatementContext, row []types.Datum) ([]types.Datum, error) {
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

func (h *FKValueHelper) fetchFKValues(row []types.Datum) ([]types.Datum, error) {
	vals := make([]types.Datum, len(h.colsOffsets))
	for i, offset := range h.colsOffsets {
		if offset >= len(row) {
			return nil, table.ErrIndexOutBound.GenWithStackByArgs("", offset, row)
		}
		vals[i] = row[offset]
	}
	return vals, nil
}

func (h *FKValueHelper) hasNullValue(vals []types.Datum) bool {
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
	// 		+----+--------+--------+
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

func (fkc FKCheckExec) doCheck(ctx context.Context) error {
	txn, err := fkc.ctx.Txn(false)
	if err != nil {
		return err
	}
	err = fkc.checkHandleKeys(ctx, txn)
	if err != nil {
		return err
	}
	err = fkc.checkUniqueKeys(ctx, txn)
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
	lockWaitTime := fkc.ctx.GetSessionVars().LockWaitTimeout
	lockCtx, err := newLockCtx(fkc.ctx, lockWaitTime, len(fkc.toBeLockedKeys))
	if err != nil {
		return err
	}
	return doLockKeys(ctx, fkc.ctx, lockCtx, fkc.toBeLockedKeys...)
}

func (fkc *FKCheckExec) checkHandleKeys(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedHandleKeys) == 0 {
		return nil
	}
	// Fill cache using BatchGet
	keys := make([]kv.Key, len(fkc.toBeCheckedHandleKeys))
	for i, handle := range fkc.toBeCheckedHandleKeys {
		keys[i] = tablecodec.EncodeRecordKey(fkc.Tbl.RecordPrefix(), handle)
	}

	_, err := txn.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	for _, k := range keys {
		_, err := txn.Get(ctx, k)
		if err == nil {
			if !fkc.CheckExist {
				return fkc.FailedErr
			}
			fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, k)
			continue
		}
		if kv.IsErrNotFound(err) {
			if fkc.CheckExist {
				return fkc.FailedErr
			}
			continue
		}
		return err
	}
	return nil
}

func (fkc *FKCheckExec) checkUniqueKeys(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedUniqueKeys) == 0 {
		return nil
	}
	// Fill cache using BatchGet
	_, err := txn.BatchGet(ctx, fkc.toBeCheckedUniqueKeys)
	if err != nil {
		return err
	}
	for _, uk := range fkc.toBeCheckedUniqueKeys {
		_, err := txn.Get(ctx, uk)
		if err == nil {
			if !fkc.CheckExist {
				return fkc.FailedErr
			}
			fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, uk)
			continue
		}
		if kv.IsErrNotFound(err) {
			if fkc.CheckExist {
				return fkc.FailedErr
			}
			continue
		}
		return err
	}
	return nil
}

func (fkc *FKCheckExec) checkIndexKeys(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedIndexKeys) == 0 {
		return nil
	}
	memBuffer := txn.GetMemBuffer()
	snap := txn.GetSnapshot()
	snap.SetOption(kv.ScanBatchSize, 2)
	defer func() {
		snap.SetOption(kv.ScanBatchSize, 256)
	}()
	for _, key := range fkc.toBeCheckedIndexKeys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		key, value, err := fkc.getIndexKeyExistInReferTable(memBuffer, snap, key)
		if err != nil {
			return err
		}
		exist := len(value) > 0
		if !exist && fkc.CheckExist {
			return fkc.FailedErr
		}
		if exist {
			if !fkc.CheckExist {
				return fkc.FailedErr
			}
			if fkc.Idx.Meta().Primary && fkc.Tbl.Meta().IsCommonHandle {
				fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, key)
			} else {
				handle, err := tablecodec.DecodeIndexHandle(key, value, len(fkc.Idx.Meta().Columns))
				if err != nil {
					return err
				}
				handleKey := tablecodec.EncodeRecordKey(fkc.Tbl.RecordPrefix(), handle)
				fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, handleKey)
			}
		}
	}
	return nil
}

func (fkc *FKCheckExec) getIndexKeyExistInReferTable(memBuffer kv.MemBuffer, snap kv.Snapshot, key kv.Key) (k []byte, v []byte, _ error) {
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
