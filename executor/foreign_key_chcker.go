package executor

import (
	"context"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
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

func initAddRowForeignKeyChecker(ctx sessionctx.Context, is infoschema.InfoSchema, tbInfo *model.TableInfo, dbName string) ([]*foreignKeyChecker, error) {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		logutil.BgLogger().Warn("----- foreign key check disabled")
		return nil, nil
	}
	fkCheckers := make([]*foreignKeyChecker, 0, len(tbInfo.ForeignKeys)+len(tbInfo.ReferredForeignKeys))
	for _, fk := range tbInfo.ForeignKeys {
		colsOffsets, err := getForeignKeyColumnsOffsets(tbInfo, fk.Cols)
		if err != nil {
			return nil, err
		}
		referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
		if err != nil {
			// todo: append warning?
			continue
		}
		failedErr := ErrNoReferencedRow2.GenWithStackByArgs(fk.String(dbName, tbInfo.Name.L))
		fkChecker, err := buildForeignKeyChecker(referTable, fk.RefCols, colsOffsets, failedErr)
		if err != nil {
			return nil, err
		}
		if fkChecker != nil {
			fkChecker.expectedExist = true
			fkCheckers = append(fkCheckers, fkChecker)
		}
	}
	return fkCheckers, nil
}

func initDeleteRowForeignKeyChecker(ctx sessionctx.Context, is infoschema.InfoSchema, tbInfo *model.TableInfo) ([]*foreignKeyChecker, error) {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		logutil.BgLogger().Warn("----- foreign key check disabled")
		return nil, nil
	}
	logutil.BgLogger().Warn("init delete fk check 0", zap.Int64("schema-version", is.SchemaMetaVersion()), zap.Int("fkn", len(tbInfo.ReferredForeignKeys)), zap.String("name", tbInfo.Name.L))
	fkCheckers := make([]*foreignKeyChecker, 0, len(tbInfo.ReferredForeignKeys))
	for _, referredFK := range tbInfo.ReferredForeignKeys {
		colsOffsets, err := getForeignKeyColumnsOffsets(tbInfo, referredFK.Cols)
		if err != nil {
			return nil, err
		}
		childTable, err := is.TableByName(referredFK.ChildSchema, referredFK.ChildTable)
		if err != nil {
			// todo: append warning?
			logutil.BgLogger().Warn("init delete fk check 01 err")
			continue
		}
		logutil.BgLogger().Warn("init delete fk check 1")
		fk := model.FindFKInfoByName(childTable.Meta().ForeignKeys, referredFK.ChildFKName.L)
		if fk == nil {
			// todo: append warning?
			continue
		}
		logutil.BgLogger().Warn("init delete fk check 2")
		switch ast.ReferOptionType(fk.OnDelete) {
		case ast.ReferOptionCascade, ast.ReferOptionSetNull:
			continue
		}
		logutil.BgLogger().Warn("init delete fk check 3")
		failedErr := ErrRowIsReferenced2.GenWithStackByArgs(fk.String(referredFK.ChildSchema.L, referredFK.ChildTable.L))
		fkChecker, err := buildForeignKeyChecker(childTable, fk.Cols, colsOffsets, failedErr)
		if err != nil {
			return nil, err
		}
		logutil.BgLogger().Warn("init delete fk check 4", zap.Bool("fk-nil", fkChecker != nil))
		if fkChecker != nil {
			fkChecker.expectedExist = false
			fkCheckers = append(fkCheckers, fkChecker)
		}
	}
	return fkCheckers, nil
}

func getForeignKeyColumnsOffsets(tbInfo *model.TableInfo, cols []model.CIStr) ([]int, error) {
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

func buildForeignKeyChecker(tbl table.Table, cols []model.CIStr, colsOffsets []int, failedErr error) (*foreignKeyChecker, error) {
	tblInfo := tbl.Meta()
	if tblInfo.PKIsHandle && len(cols) == 1 {
		refColInfo := model.FindColumnInfo(tblInfo.Columns, cols[0].L)
		if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
			refCol := table.FindCol(tbl.Cols(), refColInfo.Name.O)
			return &foreignKeyChecker{
				colsOffsets:     colsOffsets,
				tbl:             tbl,
				idxIsPrimaryKey: true,
				idxIsExclusive:  true,
				handleCols:      []*table.Column{refCol},
				failedErr:       failedErr,
			}, nil
		}
	}

	referTbIdxInfo := model.FindIndexByColumns(tblInfo.Indices, cols...)
	if referTbIdxInfo == nil {
		return nil, failedErr
	}
	var tblIdx table.Index
	for _, idx := range tbl.Indices() {
		if idx.Meta().ID == referTbIdxInfo.ID {
			tblIdx = idx
		}
	}
	if tblIdx == nil {
		return nil, failedErr
	}

	var handleCols []*table.Column
	if referTbIdxInfo.Primary && tblInfo.IsCommonHandle {
		cols := tbl.Cols()
		for _, idxCol := range referTbIdxInfo.Columns {
			handleCols = append(handleCols, cols[idxCol.Offset])
		}
	}

	return &foreignKeyChecker{
		colsOffsets:     colsOffsets,
		tbl:             tbl,
		idx:             tblIdx,
		idxIsExclusive:  len(colsOffsets) == len(referTbIdxInfo.Columns),
		idxIsPrimaryKey: referTbIdxInfo.Primary && tblInfo.IsCommonHandle,
		failedErr:       failedErr,
	}, nil
}

type foreignKeyChecker struct {
	colsOffsets []int

	tbl             table.Table
	idx             table.Index
	idxIsExclusive  bool
	idxIsPrimaryKey bool
	handleCols      []*table.Column

	expectedExist bool
	failedErr     error

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
		keys[i] = tablecodec.EncodeRecordKey(fkc.tbl.RecordPrefix(), handle)
	}

	_, err := txn.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	for _, k := range keys {
		_, err := txn.Get(ctx, k)
		if err == nil {
			if !fkc.expectedExist {
				return fkc.failedErr
			}
			continue
		}
		if fkc.expectedExist && kv.IsErrNotFound(err) {
			return fkc.failedErr
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
		logutil.BgLogger().Warn("------ get uk", zap.Bool("expectExist", fkc.expectedExist), zap.Error(err))
		if err == nil {
			if !fkc.expectedExist {
				return fkc.failedErr
			}
			continue
		}
		if fkc.expectedExist && kv.IsErrNotFound(err) {
			return fkc.failedErr
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
		if !exist && fkc.expectedExist {
			return fkc.failedErr
		}
		if exist && !fkc.expectedExist {
			return fkc.failedErr
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
			key := tablecodec.EncodeRecordKey(fkc.tbl.RecordPrefix(), handle)
			fkc.toBeCheckedIndexKeys = append(fkc.toBeCheckedIndexKeys, key)
		}
		return nil
	}
	key, distinct, err := fkc.idx.GenIndexKey(sc, vals, nil, nil)
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
	if len(vals) == 1 && fkc.idx == nil {
		return kv.IntHandle(vals[0].GetInt64()), nil
	}
	pkDts := make([]types.Datum, 0, len(vals))
	for i, val := range vals {
		if fkc.idx != nil && len(fkc.handleCols) > 0 {
			tablecodec.TruncateIndexValue(&val, fkc.idx.Meta().Columns[i], fkc.handleCols[i].ColumnInfo)
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
