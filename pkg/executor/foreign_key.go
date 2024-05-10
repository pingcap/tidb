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
	"bytes"
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// WithForeignKeyTrigger indicates the executor has foreign key check or cascade.
type WithForeignKeyTrigger interface {
	GetFKChecks() []*FKCheckExec
	GetFKCascades() []*FKCascadeExec
	HasFKCascades() bool
}

// FKCheckExec uses to check foreign key constraint.
// When insert/update child table, need to check the row has related row exists in refer table.
// When insert/update parent table, need to check the row doesn't have related row exists in refer table.
type FKCheckExec struct {
	*plannercore.FKCheck
	*fkValueHelper
	ctx sessionctx.Context

	toBeCheckedKeys       []kv.Key
	toBeCheckedPrefixKeys []kv.Key
	toBeLockedKeys        []kv.Key

	checkRowsCache map[string]bool
	stats          *FKCheckRuntimeStats
}

// FKCheckRuntimeStats contains the FKCheckExec runtime stats.
type FKCheckRuntimeStats struct {
	Total time.Duration
	Check time.Duration
	Lock  time.Duration
	Keys  int
}

// FKCascadeExec uses to execute foreign key cascade behaviour.
type FKCascadeExec struct {
	*fkValueHelper
	plan       *plannercore.FKCascade
	b          *executorBuilder
	tp         plannercore.FKCascadeType
	referredFK *model.ReferredFKInfo
	childTable *model.TableInfo
	fk         *model.FKInfo
	fkCols     []*model.ColumnInfo
	fkIdx      *model.IndexInfo
	// On delete statement, fkValues stores the delete foreign key values.
	// On update statement and the foreign key cascade is `SET NULL`, fkValues stores the old foreign key values.
	fkValues [][]types.Datum
	// new-value-key => UpdatedValuesCouple
	fkUpdatedValuesMap map[string]*UpdatedValuesCouple

	stats *FKCascadeRuntimeStats
}

// UpdatedValuesCouple contains the updated new row the old rows, exporting for test.
type UpdatedValuesCouple struct {
	NewValues     []types.Datum
	OldValuesList [][]types.Datum
}

// FKCascadeRuntimeStats contains the FKCascadeExec runtime stats.
type FKCascadeRuntimeStats struct {
	Total time.Duration
	Keys  int
}

func buildTblID2FKCheckExecs(sctx sessionctx.Context, tblID2Table map[int64]table.Table, tblID2FKChecks map[int64][]*plannercore.FKCheck) (map[int64][]*FKCheckExec, error) {
	fkChecksMap := make(map[int64][]*FKCheckExec)
	for tid, tbl := range tblID2Table {
		fkChecks, err := buildFKCheckExecs(sctx, tbl, tblID2FKChecks[tid])
		if err != nil {
			return nil, err
		}
		if len(fkChecks) > 0 {
			fkChecksMap[tid] = fkChecks
		}
	}
	return fkChecksMap, nil
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
	return &FKCheckExec{
		ctx:           sctx,
		FKCheck:       fkCheck,
		fkValueHelper: helper,
	}, nil
}

func (fkc *FKCheckExec) insertRowNeedToCheck(sc *stmtctx.StatementContext, row []types.Datum) error {
	if fkc.ReferredFK != nil {
		// Insert into parent table doesn't need to do foreign key check.
		return nil
	}
	return fkc.addRowNeedToCheck(sc, row)
}

func (fkc *FKCheckExec) updateRowNeedToCheck(sc *stmtctx.StatementContext, oldRow, newRow []types.Datum) error {
	newVals, err := fkc.fetchFKValues(newRow)
	if err != nil {
		return err
	}
	oldVals, err := fkc.fetchFKValues(oldRow)
	if err != nil {
		return err
	}
	if len(oldVals) == len(newVals) {
		isSameValue := true
		for i := range oldVals {
			cmp, err := oldVals[i].Compare(sc.TypeCtx(), &newVals[i], collate.GetCollator(oldVals[i].Collation()))
			if err != nil || cmp != 0 {
				isSameValue = false
				break
			}
		}
		if isSameValue {
			// If the old fk value and the new fk value are the same, no need to check.
			return nil
		}
	}

	if fkc.FK != nil {
		return fkc.addRowNeedToCheck(sc, newRow)
	} else if fkc.ReferredFK != nil {
		return fkc.addRowNeedToCheck(sc, oldRow)
	}
	return nil
}

func (fkc *FKCheckExec) deleteRowNeedToCheck(sc *stmtctx.StatementContext, row []types.Datum) error {
	return fkc.addRowNeedToCheck(sc, row)
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
	if isPrefix {
		fkc.toBeCheckedPrefixKeys = append(fkc.toBeCheckedPrefixKeys, key)
	} else {
		fkc.toBeCheckedKeys = append(fkc.toBeCheckedKeys, key)
	}
	return nil
}

func (fkc *FKCheckExec) doCheck(ctx context.Context) error {
	if fkc.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		fkc.stats = &FKCheckRuntimeStats{}
		defer fkc.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(fkc.ID(), fkc.stats)
	}
	if len(fkc.toBeCheckedKeys) == 0 && len(fkc.toBeCheckedPrefixKeys) == 0 {
		return nil
	}
	start := time.Now()
	if fkc.stats != nil {
		defer func() {
			fkc.stats.Keys = len(fkc.toBeCheckedKeys) + len(fkc.toBeCheckedPrefixKeys)
			fkc.stats.Total = time.Since(start)
		}()
	}
	txn, err := fkc.ctx.Txn(false)
	if err != nil {
		return err
	}
	err = fkc.checkKeys(ctx, txn)
	if err != nil {
		return err
	}
	err = fkc.checkIndexKeys(ctx, txn)
	if err != nil {
		return err
	}
	if fkc.stats != nil {
		fkc.stats.Check = time.Since(start)
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
	if fkc.stats != nil {
		fkc.stats.Lock = time.Since(start) - fkc.stats.Check
	}
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
	key, distinct, err := fkc.Idx.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), vals, nil, nil)
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
	handleBytes, err := codec.EncodeKey(sc.TimeZone(), nil, vals...)
	err = sc.HandleError(err)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

func (fkc *FKCheckExec) checkKeys(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedKeys) == 0 {
		return nil
	}
	err := fkc.prefetchKeys(ctx, txn, fkc.toBeCheckedKeys)
	if err != nil {
		return err
	}
	for _, k := range fkc.toBeCheckedKeys {
		err = fkc.checkKey(ctx, txn, k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (*FKCheckExec) prefetchKeys(ctx context.Context, txn kv.Transaction, keys []kv.Key) error {
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
	if len(fkc.toBeCheckedPrefixKeys) == 0 {
		return nil
	}
	memBuffer := txn.GetMemBuffer()
	snap := txn.GetSnapshot()
	snap.SetOption(kv.ScanBatchSize, 2)
	defer func() {
		snap.SetOption(kv.ScanBatchSize, txnsnapshot.DefaultScanBatchSize)
	}()
	for _, key := range fkc.toBeCheckedPrefixKeys {
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

func (*FKCheckExec) getIndexKeyValueInTable(ctx context.Context, memBuffer kv.MemBuffer, snap kv.Snapshot, key kv.Key) (k []byte, v []byte, _ error) {
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
	keyBuf, err := codec.EncodeKey(sc.TimeZone(), nil, vals...)
	err = sc.HandleError(err)
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

func (*fkValueHelper) hasNullValue(vals []types.Datum) bool {
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

func (fkc FKCheckExec) checkRows(ctx context.Context, sc *stmtctx.StatementContext, txn kv.Transaction, rows []toBeCheckedRow) error {
	if fkc.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		fkc.stats = &FKCheckRuntimeStats{}
		defer fkc.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(fkc.ID(), fkc.stats)
	}
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
		} else {
			fkc.checkRowsCache[string(k)] = false
		}
		if fkc.stats != nil {
			fkc.stats.Keys++
		}
	}
	return nil
}

func (b *executorBuilder) buildTblID2FKCascadeExecs(tblID2Table map[int64]table.Table, tblID2FKCascades map[int64][]*plannercore.FKCascade) (map[int64][]*FKCascadeExec, error) {
	fkCascadesMap := make(map[int64][]*FKCascadeExec)
	for tid, tbl := range tblID2Table {
		fkCascades, err := b.buildFKCascadeExecs(tbl, tblID2FKCascades[tid])
		if err != nil {
			return nil, err
		}
		if len(fkCascades) > 0 {
			fkCascadesMap[tid] = fkCascades
		}
	}
	return fkCascadesMap, nil
}

func (b *executorBuilder) buildFKCascadeExecs(tbl table.Table, fkCascades []*plannercore.FKCascade) ([]*FKCascadeExec, error) {
	fkCascadeExecs := make([]*FKCascadeExec, 0, len(fkCascades))
	for _, fkCascade := range fkCascades {
		fkCascadeExec, err := b.buildFKCascadeExec(tbl, fkCascade)
		if err != nil {
			return nil, err
		}
		if fkCascadeExec != nil {
			fkCascadeExecs = append(fkCascadeExecs, fkCascadeExec)
		}
	}
	return fkCascadeExecs, nil
}

func (b *executorBuilder) buildFKCascadeExec(tbl table.Table, fkCascade *plannercore.FKCascade) (*FKCascadeExec, error) {
	colsOffsets, err := getFKColumnsOffsets(tbl.Meta(), fkCascade.ReferredFK.Cols)
	if err != nil {
		return nil, err
	}
	helper := &fkValueHelper{
		colsOffsets: colsOffsets,
		fkValuesSet: set.NewStringSet(),
	}
	return &FKCascadeExec{
		b:                  b,
		fkValueHelper:      helper,
		plan:               fkCascade,
		tp:                 fkCascade.Tp,
		referredFK:         fkCascade.ReferredFK,
		childTable:         fkCascade.ChildTable.Meta(),
		fk:                 fkCascade.FK,
		fkCols:             fkCascade.FKCols,
		fkIdx:              fkCascade.FKIdx,
		fkUpdatedValuesMap: make(map[string]*UpdatedValuesCouple),
	}, nil
}

func (fkc *FKCascadeExec) onDeleteRow(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fkc.fetchFKValuesWithCheck(sc, row)
	if err != nil || len(vals) == 0 {
		return err
	}
	fkc.fkValues = append(fkc.fkValues, vals)
	return nil
}

func (fkc *FKCascadeExec) onUpdateRow(sc *stmtctx.StatementContext, oldRow, newRow []types.Datum) error {
	oldVals, err := fkc.fetchFKValuesWithCheck(sc, oldRow)
	if err != nil || len(oldVals) == 0 {
		return err
	}
	if model.ReferOptionType(fkc.fk.OnUpdate) == model.ReferOptionSetNull {
		fkc.fkValues = append(fkc.fkValues, oldVals)
		return nil
	}
	newVals, err := fkc.fetchFKValues(newRow)
	if err != nil {
		return err
	}
	newValsKey, err := codec.EncodeKey(sc.TimeZone(), nil, newVals...)
	err = sc.HandleError(err)
	if err != nil {
		return err
	}
	couple := fkc.fkUpdatedValuesMap[string(newValsKey)]
	if couple == nil {
		couple = &UpdatedValuesCouple{
			NewValues: newVals,
		}
	}
	couple.OldValuesList = append(couple.OldValuesList, oldVals)
	fkc.fkUpdatedValuesMap[string(newValsKey)] = couple
	return nil
}

func (fkc *FKCascadeExec) buildExecutor(ctx context.Context) (exec.Executor, error) {
	p, err := fkc.buildFKCascadePlan(ctx)
	if err != nil || p == nil {
		return nil, err
	}
	fkc.plan.CascadePlans = append(fkc.plan.CascadePlans, p)
	e := fkc.b.build(p)
	return e, fkc.b.err
}

// maxHandleFKValueInOneCascade uses to limit the max handle fk value in one cascade executor,
// this is to avoid performance issue, see: https://github.com/pingcap/tidb/issues/38631
var maxHandleFKValueInOneCascade = 1024

func (fkc *FKCascadeExec) buildFKCascadePlan(ctx context.Context) (base.Plan, error) {
	if len(fkc.fkValues) == 0 && len(fkc.fkUpdatedValuesMap) == 0 {
		return nil, nil
	}
	var indexName model.CIStr
	if fkc.fkIdx != nil {
		indexName = fkc.fkIdx.Name
	}
	var stmtNode ast.StmtNode
	switch fkc.tp {
	case plannercore.FKCascadeOnDelete:
		fkValues := fkc.fetchOnDeleteOrUpdateFKValues()
		switch model.ReferOptionType(fkc.fk.OnDelete) {
		case model.ReferOptionCascade:
			stmtNode = GenCascadeDeleteAST(fkc.referredFK.ChildSchema, fkc.childTable.Name, indexName, fkc.fkCols, fkValues)
		case model.ReferOptionSetNull:
			stmtNode = GenCascadeSetNullAST(fkc.referredFK.ChildSchema, fkc.childTable.Name, indexName, fkc.fkCols, fkValues)
		}
	case plannercore.FKCascadeOnUpdate:
		switch model.ReferOptionType(fkc.fk.OnUpdate) {
		case model.ReferOptionCascade:
			couple := fkc.fetchUpdatedValuesCouple()
			if couple != nil && len(couple.NewValues) != 0 {
				if fkc.stats != nil {
					fkc.stats.Keys += len(couple.OldValuesList)
				}
				stmtNode = GenCascadeUpdateAST(fkc.referredFK.ChildSchema, fkc.childTable.Name, indexName, fkc.fkCols, couple)
			}
		case model.ReferOptionSetNull:
			fkValues := fkc.fetchOnDeleteOrUpdateFKValues()
			stmtNode = GenCascadeSetNullAST(fkc.referredFK.ChildSchema, fkc.childTable.Name, indexName, fkc.fkCols, fkValues)
		}
	}
	if stmtNode == nil {
		return nil, errors.Errorf("generate foreign key cascade ast failed, %v", fkc.tp)
	}
	sctx := fkc.b.ctx
	err := plannercore.Preprocess(ctx, sctx, stmtNode)
	if err != nil {
		return nil, err
	}
	finalPlan, err := planner.OptimizeForForeignKeyCascade(ctx, sctx.GetPlanCtx(), stmtNode, fkc.b.is)
	if err != nil {
		return nil, err
	}
	return finalPlan, err
}

func (fkc *FKCascadeExec) fetchOnDeleteOrUpdateFKValues() [][]types.Datum {
	var fkValues [][]types.Datum
	if len(fkc.fkValues) <= maxHandleFKValueInOneCascade {
		fkValues = fkc.fkValues
		fkc.fkValues = nil
	} else {
		fkValues = fkc.fkValues[:maxHandleFKValueInOneCascade]
		fkc.fkValues = fkc.fkValues[maxHandleFKValueInOneCascade:]
	}
	if fkc.stats != nil {
		fkc.stats.Keys += len(fkValues)
	}
	return fkValues
}

func (fkc *FKCascadeExec) fetchUpdatedValuesCouple() *UpdatedValuesCouple {
	for k, couple := range fkc.fkUpdatedValuesMap {
		if len(couple.OldValuesList) <= maxHandleFKValueInOneCascade {
			delete(fkc.fkUpdatedValuesMap, k)
			return couple
		}
		result := &UpdatedValuesCouple{
			NewValues:     couple.NewValues,
			OldValuesList: couple.OldValuesList[:maxHandleFKValueInOneCascade],
		}
		couple.OldValuesList = couple.OldValuesList[maxHandleFKValueInOneCascade:]
		return result
	}
	return nil
}

// GenCascadeDeleteAST uses to generate cascade delete ast, export for test.
func GenCascadeDeleteAST(schema, table, idx model.CIStr, cols []*model.ColumnInfo, fkValues [][]types.Datum) *ast.DeleteStmt {
	deleteStmt := &ast.DeleteStmt{
		TableRefs: genTableRefsAST(schema, table, idx),
		Where:     genWhereConditionAst(cols, fkValues),
	}
	return deleteStmt
}

// GenCascadeSetNullAST uses to generate foreign key `SET NULL` ast, export for test.
func GenCascadeSetNullAST(schema, table, idx model.CIStr, cols []*model.ColumnInfo, fkValues [][]types.Datum) *ast.UpdateStmt {
	newValues := make([]types.Datum, len(cols))
	for i := range cols {
		newValues[i] = types.NewDatum(nil)
	}
	couple := &UpdatedValuesCouple{
		NewValues:     newValues,
		OldValuesList: fkValues,
	}
	return GenCascadeUpdateAST(schema, table, idx, cols, couple)
}

// GenCascadeUpdateAST uses to generate cascade update ast, export for test.
func GenCascadeUpdateAST(schema, table, idx model.CIStr, cols []*model.ColumnInfo, couple *UpdatedValuesCouple) *ast.UpdateStmt {
	list := make([]*ast.Assignment, 0, len(cols))
	for i, col := range cols {
		v := &driver.ValueExpr{Datum: couple.NewValues[i]}
		v.Type = col.FieldType
		assignment := &ast.Assignment{
			Column: &ast.ColumnName{Name: col.Name},
			Expr:   v,
		}
		list = append(list, assignment)
	}
	updateStmt := &ast.UpdateStmt{
		TableRefs: genTableRefsAST(schema, table, idx),
		Where:     genWhereConditionAst(cols, couple.OldValuesList),
		List:      list,
	}
	return updateStmt
}

func genTableRefsAST(schema, table, idx model.CIStr) *ast.TableRefsClause {
	tn := &ast.TableName{Schema: schema, Name: table}
	if idx.L != "" {
		tn.IndexHints = []*ast.IndexHint{{
			IndexNames: []model.CIStr{idx},
			HintType:   ast.HintUse,
			HintScope:  ast.HintForScan,
		}}
	}
	join := &ast.Join{Left: &ast.TableSource{Source: tn}}
	return &ast.TableRefsClause{TableRefs: join}
}

func genWhereConditionAst(cols []*model.ColumnInfo, fkValues [][]types.Datum) ast.ExprNode {
	if len(cols) > 1 {
		return genWhereConditionAstForMultiColumn(cols, fkValues)
	}
	valueList := make([]ast.ExprNode, 0, len(fkValues))
	for _, fkVals := range fkValues {
		v := &driver.ValueExpr{Datum: fkVals[0]}
		v.Type = cols[0].FieldType
		valueList = append(valueList, v)
	}
	return &ast.PatternInExpr{
		Expr: &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: cols[0].Name}},
		List: valueList,
	}
}

func genWhereConditionAstForMultiColumn(cols []*model.ColumnInfo, fkValues [][]types.Datum) ast.ExprNode {
	colValues := make([]ast.ExprNode, len(cols))
	for i := range cols {
		col := &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: cols[i].Name}}
		colValues[i] = col
	}
	valueList := make([]ast.ExprNode, 0, len(fkValues))
	for _, fkVals := range fkValues {
		values := make([]ast.ExprNode, len(fkVals))
		for i, v := range fkVals {
			val := &driver.ValueExpr{Datum: v}
			val.Type = cols[i].FieldType
			values[i] = val
		}
		row := &ast.RowExpr{Values: values}
		valueList = append(valueList, row)
	}
	return &ast.PatternInExpr{
		Expr: &ast.RowExpr{Values: colValues},
		List: valueList,
	}
}

// String implements the RuntimeStats interface.
func (s *FKCheckRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("total:")
	buf.WriteString(execdetails.FormatDuration(s.Total))
	if s.Check > 0 {
		buf.WriteString(", check:")
		buf.WriteString(execdetails.FormatDuration(s.Check))
	}
	if s.Lock > 0 {
		buf.WriteString(", lock:")
		buf.WriteString(execdetails.FormatDuration(s.Lock))
	}
	if s.Keys > 0 {
		buf.WriteString(", foreign_keys:")
		buf.WriteString(strconv.Itoa(s.Keys))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (s *FKCheckRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := &FKCheckRuntimeStats{
		Total: s.Total,
		Check: s.Check,
		Lock:  s.Lock,
		Keys:  s.Keys,
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (s *FKCheckRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*FKCheckRuntimeStats)
	if !ok {
		return
	}
	s.Total += tmp.Total
	s.Check += tmp.Check
	s.Lock += tmp.Lock
	s.Keys += tmp.Keys
}

// Tp implements the RuntimeStats interface.
func (*FKCheckRuntimeStats) Tp() int {
	return execdetails.TpFKCheckRuntimeStats
}

// String implements the RuntimeStats interface.
func (s *FKCascadeRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("total:")
	buf.WriteString(execdetails.FormatDuration(s.Total))
	if s.Keys > 0 {
		buf.WriteString(", foreign_keys:")
		buf.WriteString(strconv.Itoa(s.Keys))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (s *FKCascadeRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := &FKCascadeRuntimeStats{
		Total: s.Total,
		Keys:  s.Keys,
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (s *FKCascadeRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*FKCascadeRuntimeStats)
	if !ok {
		return
	}
	s.Total += tmp.Total
	s.Keys += tmp.Keys
}

// Tp implements the RuntimeStats interface.
func (*FKCascadeRuntimeStats) Tp() int {
	return execdetails.TpFKCascadeRuntimeStats
}
