package executor

import (
	"context"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
)

func initForeignKeyChecker(ctx sessionctx.Context, is infoschema.InfoSchema, tbInfo *model.TableInfo, dbName string) ([]*foreignKeyChecker, error) {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		logutil.BgLogger().Warn("----- foreign key check disabled")
		return nil, nil
	}
	if len(tbInfo.ForeignKeys) == 0 {
		return nil, nil
	}
	fkCheckers := make([]*foreignKeyChecker, 0, len(tbInfo.ForeignKeys))
	for _, fk := range tbInfo.ForeignKeys {
		idx := model.FindIndexByColumns(tbInfo.Indices, fk.Cols...)
		if idx == nil {
			return nil, ErrNoReferencedRow2.GenWithStackByArgs(fk.String(dbName, tbInfo.Name.L))
		}
		colsOffsets := make([]int, len(fk.Cols))
		for i := range fk.Cols {
			if idx.Columns[i].Offset < 0 {
				return nil, table.ErrIndexOutBound.GenWithStackByArgs(idx.Name, idx.Columns[i].Offset, nil)
			}
			colsOffsets[i] = idx.Columns[i].Offset
		}

		referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
		if err != nil {
			return nil, ErrNoReferencedRow2.GenWithStackByArgs(fk.String(dbName, tbInfo.Name.L))
		}
		referTbInfo := referTable.Meta()
		if referTbInfo.PKIsHandle && len(fk.RefCols) == 1 {
			refColInfo := model.FindColumnInfo(referTbInfo.Columns, fk.RefCols[0].L)
			if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
				refCol := table.FindCol(referTable.Cols(), refColInfo.Name.O)
				fkCheckers = append(fkCheckers, &foreignKeyChecker{
					dbName:          dbName,
					tbName:          tbInfo.Name.L,
					fkInfo:          fk,
					colsOffsets:     colsOffsets,
					referTable:      referTable,
					idxIsPrimaryKey: true,
					idxIsExclusive:  true,
					handleCols:      []*table.Column{refCol},
				})
				continue
			}
		}

		referTbIdxInfo := model.FindIndexByColumns(referTbInfo.Indices, fk.RefCols...)
		if referTbIdxInfo == nil {
			return nil, ErrNoReferencedRow2.GenWithStackByArgs(fk.String(dbName, tbInfo.Name.L))
		}
		var referTbIdx table.Index
		for _, idx := range referTable.Indices() {
			if idx.Meta().ID == referTbIdxInfo.ID {
				referTbIdx = idx
			}
		}
		if referTbIdx == nil {
			return nil, ErrNoReferencedRow2.GenWithStackByArgs(fk.String(dbName, tbInfo.Name.L))
		}

		var handleCols []*table.Column
		if referTbIdxInfo.Primary && referTbInfo.IsCommonHandle {
			cols := referTable.Cols()
			for _, idxCol := range referTbIdxInfo.Columns {
				handleCols = append(handleCols, cols[idxCol.Offset])
			}
		}

		fkCheckers = append(fkCheckers, &foreignKeyChecker{
			dbName:          dbName,
			tbName:          tbInfo.Name.L,
			fkInfo:          fk,
			colsOffsets:     colsOffsets,
			referTable:      referTable,
			referTbIdx:      referTbIdx,
			idxIsExclusive:  len(colsOffsets) == len(referTbIdxInfo.Columns),
			idxIsPrimaryKey: referTbIdxInfo.Primary && referTbInfo.IsCommonHandle,
		})
	}
	return fkCheckers, nil
}

type foreignKeyChecker struct {
	dbName          string
	tbName          string
	fkInfo          *model.FKInfo
	colsOffsets     []int
	referTable      table.Table
	referTbIdx      table.Index
	idxIsExclusive  bool
	idxIsPrimaryKey bool
	handleCols      []*table.Column

	toBeCheckedHandleKeys []kv.Handle
	toBeCheckedUniqueKeys []kv.Key
	toBeCheckedIndexKeys  []kv.Key
}

func (fkc *foreignKeyChecker) resetToBeCheckedKeys() {
	fkc.toBeCheckedUniqueKeys = fkc.toBeCheckedUniqueKeys[:0]
	fkc.toBeCheckedIndexKeys = fkc.toBeCheckedIndexKeys[:0]
}

func (fkc *foreignKeyChecker) checkValueExistInReferTable(ctx context.Context, txn kv.Transaction) error {
	err := fkc.checkHandleKeysExistInReferTable(ctx, txn)
	if err != nil {
		return err
	}
	err = fkc.checkUniqueKeysExistInReferTable(ctx, txn)
	if err != nil {
		return err
	}
	return fkc.checkIndexKeysExistInReferTable(ctx, txn)
}

func (fkc *foreignKeyChecker) checkHandleKeysExistInReferTable(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedHandleKeys) == 0 {
		return nil
	}
	// Fill cache using BatchGet
	keys := make([]kv.Key, len(fkc.toBeCheckedHandleKeys))
	for i, handle := range fkc.toBeCheckedHandleKeys {
		keys[i] = tablecodec.EncodeRecordKey(fkc.referTable.RecordPrefix(), handle)
	}

	_, err := txn.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	for _, k := range keys {
		_, err := txn.Get(ctx, k)
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
	if fkc.idxIsPrimaryKey {
		handle, err := fkc.buildHandleFromFKValues(sc, vals)
		if err != nil {
			return err
		}
		if fkc.idxIsExclusive {
			fkc.toBeCheckedHandleKeys = append(fkc.toBeCheckedHandleKeys, handle)
		} else {
			key := tablecodec.EncodeRecordKey(fkc.referTable.RecordPrefix(), handle)
			fkc.toBeCheckedIndexKeys = append(fkc.toBeCheckedIndexKeys, key)
		}
		return nil
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

func (fkc *foreignKeyChecker) buildHandleFromFKValues(sc *stmtctx.StatementContext, vals []types.Datum) (kv.Handle, error) {
	if len(vals) == 1 && fkc.referTbIdx == nil {
		return kv.IntHandle(vals[0].GetInt64()), nil
	}
	pkDts := make([]types.Datum, 0, len(vals))
	for i, val := range vals {
		if fkc.referTbIdx != nil && len(fkc.handleCols) > 0 {
			tablecodec.TruncateIndexValue(&val, fkc.referTbIdx.Meta().Columns[i], fkc.handleCols[i].ColumnInfo)
		}
		pkDts = append(pkDts, val)
	}
	handleBytes, err := codec.EncodeKey(sc, nil, pkDts...)
	if err != nil {
		return nil, err
	}
	handle, err := kv.NewCommonHandle(handleBytes)
	if err != nil {
		return nil, err
	}
	return handle, nil
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
