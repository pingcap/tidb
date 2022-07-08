package executor

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

type foreignKeyChecker struct {
	dbName         string
	tbName         string
	fkInfo         *model.FKInfo
	colsOffsets    []int
	referTbInfo    *model.TableInfo
	referTbIdx     table.Index
	idxIsExclusive bool

	toBeCheckedUniqueKeys []kv.Key
	toBeCheckedIndexKeys  []kv.Key
}

func (fkc *foreignKeyChecker) resetToBeCheckedKeys() {
	fkc.toBeCheckedUniqueKeys = fkc.toBeCheckedUniqueKeys[:0]
	fkc.toBeCheckedIndexKeys = fkc.toBeCheckedIndexKeys[:0]
}

func (fkc *foreignKeyChecker) checkValueExistInReferTable(ctx context.Context, txn kv.Transaction) error {
	err := fkc.checkUniqueKeysExistInReferTable(ctx, txn)
	if err != nil {
		return err
	}
	return fkc.checkIndexKeysExistInReferTable(ctx, txn)
}

func (fkc *foreignKeyChecker) checkUniqueKeysExistInReferTable(ctx context.Context, txn kv.Transaction) error {
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
			// If keys were found in refer table, pass.
			continue
		}
		if kv.IsErrNotFound(err) {
			return ErrNoReferencedRow2.GenWithStackByArgs(fkc.fkInfo.String(fkc.dbName, fkc.tbName))
		}
		return err
	}
	return nil
}

func (fkc *foreignKeyChecker) checkIndexKeysExistInReferTable(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedIndexKeys) == 0 {
		return nil
	}
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

		exist, err := fkc.checkIndexKeyExistInReferTable(snap, key)
		if err != nil {
			return err
		}
		if !exist {
			return ErrNoReferencedRow2.GenWithStackByArgs(fkc.fkInfo.String(fkc.dbName, fkc.tbName))
		}
	}
	return nil
}

func (fkc *foreignKeyChecker) checkIndexKeyExistInReferTable(snap kv.Snapshot, key kv.Key) (bool, error) {
	it, err := snap.Iter(key, nil)
	if err != nil {
		return false, err
	}
	defer it.Close()
	if it.Valid() {
		k := it.Key()
		// TODO: better decode to column datum and compare the datum value
		if k.HasPrefix(key) {
			return true, nil
		}
	}
	return false, nil
}

func (fkc *foreignKeyChecker) addRowNeedToCheck(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fkc.fetchFKValues(row)
	if err != nil || vals == nil {
		return err
	}
	key, distinct, err := fkc.referTbIdx.GenIndexKey(sc, vals, nil, nil)
	if err != nil {
		return err
	}
	if distinct && fkc.idxIsExclusive {
		fkc.toBeCheckedUniqueKeys = append(fkc.toBeCheckedUniqueKeys, key)
	} else {
		fkc.toBeCheckedIndexKeys = append(fkc.toBeCheckedIndexKeys, key)
	}
	return nil
}

func (fkc *foreignKeyChecker) fetchFKValues(row []types.Datum) ([]types.Datum, error) {
	vals := make([]types.Datum, len(fkc.colsOffsets))
	for i, offset := range fkc.colsOffsets {
		if offset >= len(row) {
			return nil, table.ErrIndexOutBound.GenWithStackByArgs("", offset, row)
		}
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
		if row[offset].IsNull() {
			return nil, nil
		}
		vals[i] = row[offset]
	}
	return vals, nil
}
