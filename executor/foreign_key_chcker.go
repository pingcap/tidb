package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hint"
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
			continue
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
			continue
		}
		return err
	}
	return nil
}

func (fkc *ForeignKeyCheckExec) checkIndexKeys(ctx context.Context, txn kv.Transaction) error {
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

		exist, err := fkc.checkIndexKeyExistInReferTable(memBuffer, snap, key)
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

func (fkc *ForeignKeyCheckExec) checkIndexKeyExistInReferTable(memBuffer kv.MemBuffer, snap kv.Snapshot, key kv.Key) (bool, error) {
	memIter, err := memBuffer.Iter(key, key.PrefixNext())
	if err != nil {
		return false, err
	}
	deletedKeys := set.NewStringSet()
	defer memIter.Close()
	for ; memIter.Valid(); err = memIter.Next() {
		if err != nil {
			return false, err
		}
		k := memIter.Key()
		// TODO: better decode to column datum and compare the datum value
		if !k.HasPrefix(key) {
			break
		}
		// check whether the key was been deleted.
		if len(memIter.Value()) > 0 {
			return true, nil
		}
		deletedKeys.Insert(string(k))
	}

	it, err := snap.Iter(key, key.PrefixNext())
	if err != nil {
		return false, err
	}
	defer it.Close()
	for ; it.Valid(); err = it.Next() {
		if err != nil {
			return false, err
		}
		k := it.Key()
		if !k.HasPrefix(key) {
			break
		}
		// TODO: better decode to column datum and compare the datum value
		if k.HasPrefix(key) && !deletedKeys.Exist(string(k)) {
			return true, nil
		}
	}
	return false, nil
}

type ForeignKeyTriggerExec struct {
	b *executorBuilder

	fkTriggerPlan plannercore.FKTriggerPlan
	fkTrigger     *plannercore.ForeignKeyTrigger
	colsOffsets   []int
	fkValues      [][]types.Datum
	fkValuesSet   set.StringSet
	// new-value-key => updatedValuesCouple
	fkUpdatedValuesMap map[string]*updatedValuesCouple

	buildFKValues set.StringSet
}

type updatedValuesCouple struct {
	newVals     []types.Datum
	oldValsList [][]types.Datum
}

func (fkt *ForeignKeyTriggerExec) addRowNeedToTrigger(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fetchFKValues(row, fkt.colsOffsets)
	if err != nil || hasNullValue(vals) {
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
	if err != nil {
		return err
	}
	if fkt.fkTrigger.Tp != plannercore.FKTriggerOnInsertOrUpdateChildTable && hasNullValue(oldVals) {
		return nil
	}
	newVals, err := fetchFKValues(newRow, fkt.colsOffsets)
	if err != nil {
		return err
	}
	if fkt.fkTrigger.Tp == plannercore.FKTriggerOnInsertOrUpdateChildTable && hasNullValue(newVals) {
		return nil
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
		vals[i] = row[offset]
	}
	return vals, nil
}

func hasNullValue(vals []types.Datum) bool {
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

func (fkt *ForeignKeyTriggerExec) isNeedTrigger() bool {
	return len(fkt.fkValues) > 0 || len(fkt.fkUpdatedValuesMap) > 0
}

func (fkt *ForeignKeyTriggerExec) buildIndexReaderRange(fkTriggerPlan plannercore.FKTriggerPlan) (bool, error) {
	switch p := fkTriggerPlan.(type) {
	case *plannercore.FKOnUpdateCascadePlan:
		for key, couple := range fkt.fkUpdatedValuesMap {
			if fkt.buildFKValues.Exist(key) {
				continue
			}
			fkt.buildFKValues.Insert(key)
			err := p.SetRangeForSelectPlan(couple.oldValsList)
			if err != nil {
				return false, err
			}
			return len(fkt.buildFKValues) == len(fkt.fkUpdatedValuesMap), p.SetUpdatedValues(couple.newVals)
		}
		return true, nil
	}
	valsList := make([][]types.Datum, 0, len(fkt.fkValues))
	valsList = append(valsList, fkt.fkValues...)
	switch fkt.fkTrigger.Tp {
	case plannercore.FKTriggerOnInsertOrUpdateChildTable:
		for _, couple := range fkt.fkUpdatedValuesMap {
			valsList = append(valsList, couple.newVals)
		}
	default:
		for _, couple := range fkt.fkUpdatedValuesMap {
			valsList = append(valsList, couple.oldValsList...)
		}
	}
	return true, fkTriggerPlan.SetRangeForSelectPlan(valsList)
}

func (fkt *ForeignKeyTriggerExec) buildFKTriggerPlan(ctx context.Context) (plannercore.FKTriggerPlan, error) {
	planBuilder, _ := plannercore.NewPlanBuilder().Init(fkt.b.ctx, fkt.b.is, &hint.BlockHintProcessor{})
	switch fkt.fkTrigger.Tp {
	case plannercore.FKTriggerOnDelete:
		return planBuilder.BuildOnDeleteFKTriggerPlan(ctx, fkt.fkTrigger.OnModifyReferredTable)
	case plannercore.FKTriggerOnUpdate:
		return planBuilder.BuildOnUpdateFKTriggerPlan(ctx, fkt.fkTrigger.OnModifyReferredTable)
	case plannercore.FKTriggerOnInsertOrUpdateChildTable:
		return planBuilder.BuildOnInsertFKTriggerPlan(fkt.fkTrigger.OnModifyChildTable)
	default:
		return nil, fmt.Errorf("unknown foreign key trigger type %v", fkt.fkTrigger.Tp)
	}
}

func (fkt *ForeignKeyTriggerExec) buildExecutor(ctx context.Context) (Executor, bool, error) {
	if fkt.fkTriggerPlan == nil {
		p, err := fkt.buildFKTriggerPlan(ctx)
		if err != nil {
			return nil, false, err
		}
		if p == nil {
			return nil, true, nil
		}
		fkt.fkTriggerPlan = p
	}

	done, err := fkt.buildIndexReaderRange(fkt.fkTriggerPlan)
	if err != nil {
		return nil, false, err
	}

	var e Executor
	switch x := fkt.fkTriggerPlan.(type) {
	case *plannercore.FKOnDeleteCascadePlan:
		e = fkt.b.build(x.Delete)
	case *plannercore.FKOnUpdateCascadePlan:
		e = fkt.b.build(x.Update)
	case *plannercore.FKUpdateSetNullPlan:
		e = fkt.b.build(x.Update)
	case *plannercore.FKCheckPlan:
		e = fkt.b.buildFKCheck(x)
	default:
		e = fkt.b.build(x)
	}
	return e, done, fkt.b.err
}

func (b *executorBuilder) buildTblID2ForeignKeyTriggerExecs(tblID2Table map[int64]table.Table, tblID2FKTriggers map[int64][]*plannercore.ForeignKeyTrigger) (map[int64][]*ForeignKeyTriggerExec, error) {
	var err error
	fkTriggerExecs := make(map[int64][]*ForeignKeyTriggerExec)
	for tid, tbl := range tblID2Table {
		fkTriggerExecs[tid], err = b.buildTblForeignKeyTriggerExecs(tbl, tblID2FKTriggers[tid])
		if err != nil {
			return nil, err
		}
	}
	return fkTriggerExecs, nil
}

func (b *executorBuilder) buildTblForeignKeyTriggerExecs(tbl table.Table, fkTriggerPlans []*plannercore.ForeignKeyTrigger) ([]*ForeignKeyTriggerExec, error) {
	fkTriggerExecs := make([]*ForeignKeyTriggerExec, 0, len(fkTriggerPlans))
	for _, fkTriggers := range fkTriggerPlans {
		fkTriggerExec, err := b.buildForeignKeyTriggerExec(tbl.Meta(), fkTriggers)
		if err != nil {
			return nil, err
		}
		fkTriggerExecs = append(fkTriggerExecs, fkTriggerExec)
	}

	return fkTriggerExecs, nil
}

func (b *executorBuilder) buildForeignKeyTriggerExec(tbInfo *model.TableInfo, fkTrigger *plannercore.ForeignKeyTrigger) (*ForeignKeyTriggerExec, error) {
	var cols []model.CIStr
	if fkTrigger.OnModifyChildTable != nil {
		cols = fkTrigger.OnModifyChildTable.FK.Cols
	} else if fkTrigger.OnModifyReferredTable != nil {
		cols = fkTrigger.OnModifyReferredTable.ReferredFK.Cols
	}
	colsOffsets, err := getColumnsOffsets(tbInfo, cols)
	if err != nil {
		return nil, err
	}
	return &ForeignKeyTriggerExec{
		b:                  b,
		fkTrigger:          fkTrigger,
		colsOffsets:        colsOffsets,
		fkValuesSet:        set.NewStringSet(),
		buildFKValues:      set.NewStringSet(),
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
