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

	toBeCheckedFKValues [][]types.Datum
	toBeLockedKeys      []kv.Key
}

type fkValueHelper struct {
	colsOffsets []int
	fkValuesSet set.StringSet
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
	cols := fkCheck.FK.Cols
	colsOffsets, err := getColumnsOffsets(tbl.Meta(), cols)
	if err != nil {
		return nil, err
	}
	helper := &fkValueHelper{
		colsOffsets: colsOffsets,
		fkValuesSet: set.NewStringSet(),
	}
	return &FKCheckExec{
		ctx:           sctx,
		FKCheck:       fkCheck,
		fkValueHelper: helper,
	}, nil
}

func (fkc *FKCheckExec) addRowNeedToCheck(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fkc.fetchFKValuesWithCheck(sc, row)
	if len(vals) > 0 {
		fkc.toBeCheckedFKValues = append(fkc.toBeCheckedFKValues, vals)
	}
	return err
}

func (fkc *FKCheckExec) updateRowNeedToCheck(sc *stmtctx.StatementContext, oldRow, newRow []types.Datum) error {
	if fkc.FK != nil {
		return fkc.addRowNeedToCheck(sc, newRow)
	}
	return nil
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
	keys, prefixKeys, err := fkc.buildCheckKeysFromFKValues(fkc.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return err
	}
	txn, err := fkc.ctx.Txn(false)
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		err = fkc.checkKeys(ctx, txn, keys)
		if err != nil {
			return err
		}
	}
	if len(prefixKeys) > 0 {
		err = fkc.checkIndexKeys(ctx, txn, prefixKeys)
		if err != nil {
			return err
		}
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

func (fkc FKCheckExec) buildCheckKeysFromFKValues(sc *stmtctx.StatementContext) (keys []kv.Key, prefixKeys []kv.Key, _ error) {
	if fkc.IdxIsExclusive && (fkc.IdxIsPrimaryKey || fkc.Idx.Meta().Unique) {
		keys = make([]kv.Key, 0, len(fkc.toBeCheckedFKValues))
	} else {
		prefixKeys = make([]kv.Key, 0, len(fkc.toBeCheckedFKValues))
	}
	if fkc.IdxIsPrimaryKey {
		for _, vals := range fkc.toBeCheckedFKValues {
			handleKey, err := fkc.buildHandleFromFKValues(sc, vals)
			if err != nil {
				return nil, nil, err
			}
			key := tablecodec.EncodeRecordKey(fkc.Tbl.RecordPrefix(), handleKey)
			if fkc.IdxIsExclusive {
				keys = append(keys, key)
			} else {
				prefixKeys = append(prefixKeys, key)
			}
		}
		return keys, prefixKeys, nil
	}

	for _, vals := range fkc.toBeCheckedFKValues {
		key, distinct, err := fkc.Idx.GenIndexKey(sc, vals, nil, nil)
		if err != nil {
			return nil, nil, err
		}
		if distinct && fkc.IdxIsExclusive {
			keys = append(keys, key)
		} else {
			prefixKeys = append(prefixKeys, key)
		}
	}
	return keys, prefixKeys, nil
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

func (fkc *FKCheckExec) checkKeys(ctx context.Context, txn kv.Transaction, keys []kv.Key) error {
	// Fill cache using BatchGet
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

func (fkc *FKCheckExec) checkIndexKeys(ctx context.Context, txn kv.Transaction, prefixKeys []kv.Key) error {
	if len(prefixKeys) == 0 {
		return nil
	}
	memBuffer := txn.GetMemBuffer()
	snap := txn.GetSnapshot()
	snap.SetOption(kv.ScanBatchSize, 2)
	defer func() {
		snap.SetOption(kv.ScanBatchSize, 256)
	}()
	for _, key := range prefixKeys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		key, value, err := fkc.getIndexKeyValueInTable(memBuffer, snap, key)
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

func (fkc *FKCheckExec) getIndexKeyValueInTable(memBuffer kv.MemBuffer, snap kv.Snapshot, key kv.Key) (k []byte, v []byte, _ error) {
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
