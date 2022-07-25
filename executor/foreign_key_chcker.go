package executor

import (
	"context"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/set"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type ExecutorWithForeignKeyTrigger interface {
	Executor
	GetForeignKeyTriggerExecs() []*ForeignKeyTriggerExec
}

type ForeignKeyCheckExec struct {
	baseExecutor

	tbl             table.Table
	idx             table.Index
	idxIsExclusive  bool
	idxIsPrimaryKey bool
	handleCols      []*table.Column

	checkExist bool
	failedErr  error

	toBeCheckedHandleKeys []kv.Handle
	toBeCheckedUniqueKeys []kv.Key
	toBeCheckedIndexKeys  []kv.Key

	checked bool
}

func (fkc ForeignKeyCheckExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if fkc.checked {
		return nil
	}
	fkc.checked = true

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
	return fkc.checkIndexKeys(ctx, txn)
}

func (fkc *ForeignKeyCheckExec) checkHandleKeys(ctx context.Context, txn kv.Transaction) error {
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
			if !fkc.checkExist {
				return fkc.failedErr
			}
			continue
		}
		if kv.IsErrNotFound(err) {
			if fkc.checkExist {
				return fkc.failedErr
			}
			return nil
		}
		return err
	}
	return nil
}

func (fkc *ForeignKeyCheckExec) checkUniqueKeys(ctx context.Context, txn kv.Transaction) error {
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
			if !fkc.checkExist {
				return fkc.failedErr
			}
			continue
		}
		if kv.IsErrNotFound(err) {
			if fkc.checkExist {
				return fkc.failedErr
			}
			return nil
		}
		return err
	}
	return nil
}

func (fkc *ForeignKeyCheckExec) checkIndexKeys(ctx context.Context, txn kv.Transaction) error {
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
		if !exist && fkc.checkExist {
			return fkc.failedErr
		}
		if exist && !fkc.checkExist {
			return fkc.failedErr
		}
	}
	return nil
}

func (fkc *ForeignKeyCheckExec) checkIndexKeyExistInReferTable(snap kv.Snapshot, key kv.Key) (bool, error) {
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

type ForeignKeyTriggerExec struct {
	b             *executorBuilder
	fkTriggerPlan plannercore.FKTriggerPlan

	colsOffsets []int
	fkValues    [][]types.Datum
	fkValuesSet set.StringSet
	// new-value-key => updatedValuesCouple
	fkUpdatedValuesMap map[string]*updatedValuesCouple
}

type updatedValuesCouple struct {
	newVals     []types.Datum
	oldValsList [][]types.Datum
}

func (fkt *ForeignKeyTriggerExec) addRowNeedToTrigger(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fetchFKValues(row, fkt.colsOffsets)
	if err != nil || len(vals) == 0 {
		return err
	}
	keyBuf, err := codec.EncodeKey(sc, nil, vals...)
	if err != nil {
		return err
	}
	key := string(keyBuf)
	if fkt.fkValuesSet.Exist(key) {
		return nil
	}
	fkt.fkValues = append(fkt.fkValues, vals)
	fkt.fkValuesSet.Insert(key)
	return nil
}

func (fkt *ForeignKeyTriggerExec) updateRowNeedToTrigger(sc *stmtctx.StatementContext, oldRow, newRow []types.Datum) error {
	oldVals, err := fetchFKValues(oldRow, fkt.colsOffsets)
	if err != nil || len(oldVals) == 0 {
		return err
	}
	newVals, err := fetchFKValues(newRow, fkt.colsOffsets)
	if err != nil || len(newVals) == 0 {
		return err
	}
	keyBuf, err := codec.EncodeKey(sc, nil, newVals...)
	if err != nil {
		return err
	}
	couple := fkt.fkUpdatedValuesMap[string(keyBuf)]
	if couple == nil {
		couple = &updatedValuesCouple{
			newVals: newVals,
		}
	}
	couple.oldValsList = append(couple.oldValsList, oldVals)
	fkt.fkUpdatedValuesMap[string(keyBuf)] = couple
	return nil
}

func fetchFKValues(row []types.Datum, colsOffsets []int) ([]types.Datum, error) {
	vals := make([]types.Datum, len(colsOffsets))
	for i, offset := range colsOffsets {
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

func (fkt *ForeignKeyTriggerExec) buildIndexReaderRange() error {
	valsList := make([][]types.Datum, 0, len(fkt.fkValues))
	valsList = append(valsList, fkt.fkValues...)
	for _, couple := range fkt.fkUpdatedValuesMap {
		valsList = append(valsList, couple.oldValsList...)
	}
	return fkt.fkTriggerPlan.SetRangeForSelectPlan(valsList)
}

func (fkt *ForeignKeyTriggerExec) buildExecutor() Executor {
	return fkt.b.build(fkt.fkTriggerPlan)
}

func (b *executorBuilder) buildTblID2ForeignKeyTriggerExecs(tblID2Table map[int64]table.Table, tblID2FKTriggerPlans map[int64][]plannercore.FKTriggerPlan) (map[int64][]*ForeignKeyTriggerExec, error) {
	var err error
	fkTriggerExecs := make(map[int64][]*ForeignKeyTriggerExec)
	for tid, tbl := range tblID2Table {
		fkTriggerExecs[tid], err = b.buildTblForeignKeyTriggerExecs(tbl, tblID2FKTriggerPlans[tid])
		if err != nil {
			return nil, err
		}
	}
	return fkTriggerExecs, nil
}

func (b *executorBuilder) buildTblForeignKeyTriggerExecs(tbl table.Table, fkTriggerPlans []plannercore.FKTriggerPlan) ([]*ForeignKeyTriggerExec, error) {
	fkTriggerExecs := make([]*ForeignKeyTriggerExec, 0, len(fkTriggerPlans))
	for _, fkTriggerPlan := range fkTriggerPlans {
		fkTriggerExec, err := b.buildForeignKeyTriggerExec(tbl.Meta(), fkTriggerPlan)
		if err != nil {
			return nil, err
		}
		fkTriggerExecs = append(fkTriggerExecs, fkTriggerExec)
	}

	return fkTriggerExecs, nil
}

func (b *executorBuilder) buildForeignKeyTriggerExec(tbInfo *model.TableInfo, fkTriggerPlan plannercore.FKTriggerPlan) (*ForeignKeyTriggerExec, error) {
	colsOffsets, err := getColumnsOffsets(tbInfo, fkTriggerPlan.GetCols())
	if err != nil {
		return nil, err
	}

	return &ForeignKeyTriggerExec{
		b:                  b,
		fkTriggerPlan:      fkTriggerPlan,
		colsOffsets:        colsOffsets,
		fkValuesSet:        set.NewStringSet(),
		fkUpdatedValuesMap: make(map[string]*updatedValuesCouple),
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
