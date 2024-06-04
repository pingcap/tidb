// Copyright 2016 PingCAP, Inc.
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

package tables

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// index is the data structure for index data in the KV store.
type index struct {
	idxInfo  *model.IndexInfo
	tblInfo  *model.TableInfo
	phyTblID int64
	// initNeedRestoreData is used to initialize `needRestoredData` in `index.Create()`.
	// This routine cannot be done in `NewIndex()` because `needRestoreData` relies on `NewCollationEnabled()` and
	// the collation global variable is initialized *after* `NewIndex()`.
	initNeedRestoreData sync.Once
	needRestoredData    bool
}

// NeedRestoredData checks whether the index columns needs restored data.
func NeedRestoredData(idxCols []*model.IndexColumn, colInfos []*model.ColumnInfo) bool {
	for _, idxCol := range idxCols {
		col := colInfos[idxCol.Offset]
		if types.NeedRestoredData(&col.FieldType) {
			return true
		}
	}
	return false
}

// NewIndex builds a new Index object.
func NewIndex(physicalID int64, tblInfo *model.TableInfo, indexInfo *model.IndexInfo) table.Index {
	index := &index{
		idxInfo:  indexInfo,
		tblInfo:  tblInfo,
		phyTblID: physicalID,
	}
	return index
}

// Meta returns index info.
func (c *index) Meta() *model.IndexInfo {
	return c.idxInfo
}

// TableMeta returns table info.
func (c *index) TableMeta() *model.TableInfo {
	return c.tblInfo
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func (c *index) GenIndexKey(ec errctx.Context, loc *time.Location, indexedValues []types.Datum, h kv.Handle, buf []byte) (key []byte, distinct bool, err error) {
	idxTblID := c.phyTblID
	if c.idxInfo.Global {
		idxTblID = c.tblInfo.ID
	}
	key, distinct, err = tablecodec.GenIndexKey(loc, c.tblInfo, c.idxInfo, idxTblID, indexedValues, h, buf)
	err = ec.HandleError(err)
	return
}

// GenIndexValue generates the index value.
func (c *index) GenIndexValue(ec errctx.Context, loc *time.Location, distinct bool, indexedValues []types.Datum,
	h kv.Handle, restoredData []types.Datum, buf []byte) ([]byte, error) {
	c.initNeedRestoreData.Do(func() {
		c.needRestoredData = NeedRestoredData(c.idxInfo.Columns, c.tblInfo.Columns)
	})
	idx, err := tablecodec.GenIndexValuePortal(loc, c.tblInfo, c.idxInfo, c.needRestoredData, distinct, false, indexedValues, h, c.phyTblID, restoredData, buf)
	err = ec.HandleError(err)
	return idx, err
}

// getIndexedValue will produce the result like:
// 1. If not multi-valued index, return directly.
// 2. (i1, [m1,m2], i2, ...) ==> [(i1, m1, i2, ...), (i1, m2, i2, ...)]
// 3. (i1, null, i2, ...) ==> [(i1, null, i2, ...)]
// 4. (i1, [], i2, ...) ==> nothing.
func (c *index) getIndexedValue(indexedValues []types.Datum) [][]types.Datum {
	if !c.idxInfo.MVIndex {
		return [][]types.Datum{indexedValues}
	}

	vals := make([][]types.Datum, 0, 16)
	jsonIdx := 0
	jsonIsNull := false
	existsVals := make(map[string]struct{})
	var buf []byte
	for !jsonIsNull {
		val := make([]types.Datum, 0, len(indexedValues))
		for i, v := range indexedValues {
			if !c.tblInfo.Columns[c.idxInfo.Columns[i].Offset].FieldType.IsArray() {
				val = append(val, v)
			} else {
				// if the datum type is not JSON, it must come from cleanup index.
				if v.IsNull() || v.Kind() != types.KindMysqlJSON {
					val = append(val, v)
					jsonIsNull = true
					continue
				}
				elemCount := v.GetMysqlJSON().GetElemCount()
				for {
					// JSON cannot be indexed, if the value is JSON type, it must be multi-valued index.
					if jsonIdx >= elemCount {
						goto out
					}
					binaryJSON := v.GetMysqlJSON().ArrayGetElem(jsonIdx)
					jsonIdx++
					buf = buf[:0]
					key := string(binaryJSON.HashValue(buf))
					if _, exists := existsVals[key]; exists {
						continue
					}
					existsVals[key] = struct{}{}
					val = append(val, types.NewDatum(binaryJSON.GetValue()))
					break
				}
			}
		}
		vals = append(vals, val)
	}
out:
	return vals
}

// Create creates a new entry in the kvIndex data.
// If the index is unique and there is an existing entry with the same key,
// Create will return the existing entry's handle as the first return value, ErrKeyExists as the second return value.
func (c *index) Create(sctx table.MutateContext, txn kv.Transaction, indexedValue []types.Datum, h kv.Handle, handleRestoreData []types.Datum, opts ...table.CreateIdxOptFunc) (kv.Handle, error) {
	if c.Meta().Unique {
		txn.CacheTableInfo(c.phyTblID, c.tblInfo)
	}
	var opt table.CreateIdxOpt
	for _, fn := range opts {
		fn(&opt)
	}

	indexedValues := c.getIndexedValue(indexedValue)
	ctx := opt.Ctx
	if ctx != nil {
		var r tracing.Region
		r, ctx = tracing.StartRegionEx(ctx, "index.Create")
		defer r.End()
	} else {
		ctx = context.TODO()
	}
	vars := sctx.GetSessionVars()
	writeBufs := vars.GetWriteStmtBufs()
	skipCheck := vars.StmtCtx.BatchCheck
	sc := sctx.GetSessionVars().StmtCtx
	for _, value := range indexedValues {
		key, distinct, err := c.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), value, h, writeBufs.IndexKeyBuf)
		if err != nil {
			return nil, err
		}

		var (
			tempKey         []byte
			keyVer          byte
			keyIsTempIdxKey bool
		)
		if !opt.FromBackFill {
			key, tempKey, keyVer = GenTempIdxKeyByState(c.idxInfo, key)
			if keyVer == TempIndexKeyTypeBackfill || keyVer == TempIndexKeyTypeDelete {
				key, tempKey = tempKey, nil
				keyIsTempIdxKey = true
			}
		}

		if txn.IsPipelined() {
			// For pipelined DML, disable the untouched optimization to avoid extra RPCs for MemBuffer.Get().
			// TODO: optimize this.
			opt.Untouched = false
		}

		if opt.Untouched {
			txn, err1 := sctx.Txn(true)
			if err1 != nil {
				return nil, err1
			}
			// If the index kv was untouched(unchanged), and the key/value already exists in mem-buffer,
			// should not overwrite the key with un-commit flag.
			// So if the key exists, just do nothing and return.
			v, err := txn.GetMemBuffer().Get(ctx, key)
			if err == nil {
				if len(v) != 0 {
					continue
				}
				// The key is marked as deleted in the memory buffer, as the existence check is done lazily
				// for optimistic transactions by default. The "untouched" key could still exist in the store,
				// it's needed to commit this key to do the existence check so unset the untouched flag.
				if !txn.IsPessimistic() {
					keyFlags, err := txn.GetMemBuffer().GetFlags(key)
					if err != nil {
						return nil, err
					}
					if keyFlags.HasPresumeKeyNotExists() {
						opt.Untouched = false
					}
				}
			}
		}

		// save the key buffer to reuse.
		writeBufs.IndexKeyBuf = key
		c.initNeedRestoreData.Do(func() {
			c.needRestoredData = NeedRestoredData(c.idxInfo.Columns, c.tblInfo.Columns)
		})
		idxVal, err := tablecodec.GenIndexValuePortal(sctx.GetSessionVars().StmtCtx.TimeZone(), c.tblInfo, c.idxInfo,
			c.needRestoredData, distinct, opt.Untouched, value, h, c.phyTblID, handleRestoreData, nil)
		err = sctx.GetSessionVars().StmtCtx.HandleError(err)
		if err != nil {
			return nil, err
		}

		opt.IgnoreAssertion = opt.IgnoreAssertion || c.idxInfo.State != model.StatePublic

		if !distinct || skipCheck || opt.Untouched {
			val := idxVal
			if opt.Untouched && (keyIsTempIdxKey || len(tempKey) > 0) {
				// Untouched key-values never occur in the storage and the temp index is not public.
				// It is unnecessary to write the untouched temp index key-values.
				continue
			}
			if keyIsTempIdxKey {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: distinct}
				val = tempVal.Encode(nil)
			}
			err = txn.GetMemBuffer().Set(key, val)
			if err != nil {
				return nil, err
			}
			if len(tempKey) > 0 {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: distinct}
				val = tempVal.Encode(nil)
				err = txn.GetMemBuffer().Set(tempKey, val)
				if err != nil {
					return nil, err
				}
			}
			if !opt.IgnoreAssertion && (!opt.Untouched) {
				if sctx.GetSessionVars().LazyCheckKeyNotExists() && !txn.IsPessimistic() {
					err = txn.SetAssertion(key, kv.SetAssertUnknown)
				} else {
					err = txn.SetAssertion(key, kv.SetAssertNotExist)
				}
			}
			if err != nil {
				return nil, err
			}
			continue
		}

		var value []byte
		if c.tblInfo.TempTableType != model.TempTableNone {
			// Always check key for temporary table because it does not write to TiKV
			value, err = txn.Get(ctx, key)
		} else if (txn.IsPipelined() || sctx.GetSessionVars().LazyCheckKeyNotExists()) && !keyIsTempIdxKey {
			// For temp index keys, we can't get the temp value from memory buffer, even if the lazy check is enabled.
			// Otherwise, it may cause the temp index value to be overwritten, leading to data inconsistency.
			value, err = txn.GetMemBuffer().GetLocal(ctx, key)
		} else {
			value, err = txn.Get(ctx, key)
		}
		if err != nil && !kv.IsErrNotFound(err) {
			return nil, err
		}
		var tempIdxVal tablecodec.TempIndexValue
		if len(value) > 0 && keyIsTempIdxKey {
			tempIdxVal, err = tablecodec.DecodeTempIndexValue(value)
			if err != nil {
				return nil, err
			}
		}
		// The index key value is not found or deleted.
		if err != nil || len(value) == 0 || (!tempIdxVal.IsEmpty() && tempIdxVal.Current().Delete) {
			val := idxVal
			lazyCheck := (txn.IsPipelined() || sctx.GetSessionVars().LazyCheckKeyNotExists()) && err != nil
			if keyIsTempIdxKey {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: true}
				val = tempVal.Encode(value)
			}
			needPresumeNotExists, err := needPresumeKeyNotExistsFlag(ctx, txn, key, tempKey, h,
				keyIsTempIdxKey, c.tblInfo.ID)
			if err != nil {
				return nil, err
			}
			if lazyCheck {
				var flags []kv.FlagsOp
				if needPresumeNotExists {
					flags = []kv.FlagsOp{kv.SetPresumeKeyNotExists}
				}
				if !vars.ConstraintCheckInPlacePessimistic && vars.TxnCtx.IsPessimistic && vars.InTxn() &&
					!vars.InRestrictedSQL && vars.ConnectionID > 0 {
					flags = append(flags, kv.SetNeedConstraintCheckInPrewrite)
				}
				err = txn.GetMemBuffer().SetWithFlags(key, val, flags...)
			} else {
				err = txn.GetMemBuffer().Set(key, val)
			}
			if err != nil {
				return nil, err
			}
			if len(tempKey) > 0 {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: true}
				val = tempVal.Encode(value)
				if lazyCheck && needPresumeNotExists {
					err = txn.GetMemBuffer().SetWithFlags(tempKey, val, kv.SetPresumeKeyNotExists)
				} else {
					err = txn.GetMemBuffer().Set(tempKey, val)
				}
				if err != nil {
					return nil, err
				}
			}
			if opt.IgnoreAssertion {
				continue
			}
			if lazyCheck && !txn.IsPessimistic() {
				err = txn.SetAssertion(key, kv.SetAssertUnknown)
			} else {
				err = txn.SetAssertion(key, kv.SetAssertNotExist)
			}
			if err != nil {
				return nil, err
			}
			continue
		}
		if keyIsTempIdxKey && !tempIdxVal.IsEmpty() {
			value = tempIdxVal.Current().Value
		}
		handle, err := tablecodec.DecodeHandleInIndexValue(value)
		if err != nil {
			return nil, err
		}
		return handle, kv.ErrKeyExists
	}
	return nil, nil
}

func needPresumeKeyNotExistsFlag(ctx context.Context, txn kv.Transaction, key, tempKey kv.Key,
	h kv.Handle, keyIsTempIdxKey bool, tblID int64) (needFlag bool, err error) {
	var uniqueTempKey kv.Key
	if keyIsTempIdxKey {
		uniqueTempKey = key
	} else if len(tempKey) > 0 {
		uniqueTempKey = tempKey
	} else {
		return true, nil
	}
	foundKey, dupHandle, err := FetchDuplicatedHandle(ctx, uniqueTempKey, true, txn, tblID)
	if err != nil {
		return false, err
	}
	if foundKey && dupHandle != nil && !dupHandle.Equal(h) {
		return false, kv.ErrKeyExists
	}
	return false, nil
}

// Delete removes the entry for handle h and indexedValues from KV index.
func (c *index) Delete(ctx table.MutateContext, txn kv.Transaction, indexedValue []types.Datum, h kv.Handle) error {
	indexedValues := c.getIndexedValue(indexedValue)
	sc := ctx.GetSessionVars().StmtCtx
	for _, value := range indexedValues {
		key, distinct, err := c.GenIndexKey(sc.ErrCtx(), sc.TimeZone(), value, h, nil)
		if err != nil {
			return err
		}

		key, tempKey, tempKeyVer := GenTempIdxKeyByState(c.idxInfo, key)
		var originTempVal []byte
		if len(tempKey) > 0 && c.idxInfo.Unique {
			// Get the origin value of the unique temporary index key.
			// Append the new delete operations to the end of the origin value.
			originTempVal, err = getKeyInTxn(context.TODO(), txn, tempKey)
			if err != nil {
				return err
			}
		}

		tempValElem := tablecodec.TempIndexValueElem{Handle: h, KeyVer: tempKeyVer, Delete: true, Distinct: distinct}
		if distinct {
			if len(key) > 0 {
				okToDelete := true
				if c.idxInfo.BackfillState != model.BackfillStateInapplicable {
					// #52914: the delete key is covered by the new ingested key, which shouldn't be deleted.
					originVal, err := getKeyInTxn(context.TODO(), txn, key)
					if err != nil {
						return err
					}
					if len(originVal) > 0 {
						oh, err := tablecodec.DecodeHandleInIndexValue(originVal)
						if err != nil {
							return err
						}
						if !h.Equal(oh) {
							okToDelete = false
						}
					}
				}
				if okToDelete {
					err = txn.GetMemBuffer().DeleteWithFlags(key, kv.SetNeedLocked)
					if err != nil {
						return err
					}
				}
			}
			if len(tempKey) > 0 {
				// Append to the end of the origin value for distinct value.
				tempVal := tempValElem.Encode(originTempVal)
				err = txn.GetMemBuffer().Set(tempKey, tempVal)
				if err != nil {
					return err
				}
			}
		} else {
			if len(key) > 0 {
				err = txn.GetMemBuffer().Delete(key)
				if err != nil {
					return err
				}
			}
			if len(tempKey) > 0 {
				tempVal := tempValElem.Encode(nil)
				err = txn.GetMemBuffer().Set(tempKey, tempVal)
				if err != nil {
					return err
				}
			}
		}
		if c.idxInfo.State == model.StatePublic {
			// If the index is in public state, delete this index means it must exists.
			err = txn.SetAssertion(key, kv.SetAssertExist)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *index) GenIndexKVIter(ec errctx.Context, loc *time.Location, indexedValue []types.Datum,
	h kv.Handle, handleRestoreData []types.Datum) table.IndexKVGenerator {
	var mvIndexValues [][]types.Datum
	if c.Meta().MVIndex {
		mvIndexValues = c.getIndexedValue(indexedValue)
		return table.NewMultiValueIndexKVGenerator(c, ec, loc, h, handleRestoreData, mvIndexValues)
	}
	return table.NewPlainIndexKVGenerator(c, ec, loc, h, handleRestoreData, indexedValue)
}

const (
	// TempIndexKeyTypeNone means the key is not a temporary index key.
	TempIndexKeyTypeNone byte = 0
	// TempIndexKeyTypeDelete indicates this value is written in the delete-only stage.
	TempIndexKeyTypeDelete byte = 'd'
	// TempIndexKeyTypeBackfill indicates this value is written in the backfill stage.
	TempIndexKeyTypeBackfill byte = 'b'
	// TempIndexKeyTypeMerge indicates this value is written in the merge stage.
	TempIndexKeyTypeMerge byte = 'm'
)

// GenTempIdxKeyByState is used to get the key version and the temporary key.
// The tempKeyVer means the temp index key/value version.
func GenTempIdxKeyByState(indexInfo *model.IndexInfo, indexKey kv.Key) (key, tempKey kv.Key, tempKeyVer byte) {
	if indexInfo.State != model.StatePublic {
		switch indexInfo.BackfillState {
		case model.BackfillStateInapplicable:
			return indexKey, nil, TempIndexKeyTypeNone
		case model.BackfillStateRunning:
			// Write to the temporary index.
			tablecodec.IndexKey2TempIndexKey(indexKey)
			if indexInfo.State == model.StateDeleteOnly {
				return nil, indexKey, TempIndexKeyTypeDelete
			}
			return nil, indexKey, TempIndexKeyTypeBackfill
		case model.BackfillStateReadyToMerge, model.BackfillStateMerging:
			// Double write
			tmp := make([]byte, len(indexKey))
			copy(tmp, indexKey)
			tablecodec.IndexKey2TempIndexKey(tmp)
			return indexKey, tmp, TempIndexKeyTypeMerge
		}
	}
	return indexKey, nil, TempIndexKeyTypeNone
}

func (c *index) Exist(ec errctx.Context, loc *time.Location, txn kv.Transaction, indexedValue []types.Datum, h kv.Handle) (bool, kv.Handle, error) {
	indexedValues := c.getIndexedValue(indexedValue)
	for _, val := range indexedValues {
		key, distinct, err := c.GenIndexKey(ec, loc, val, h, nil)
		if err != nil {
			return false, nil, err
		}
		// If index current is in creating status and using ingest mode, we need first
		// check key exist status in temp index.
		key, tempKey, _ := GenTempIdxKeyByState(c.idxInfo, key)
		if len(tempKey) > 0 {
			key = tempKey
		}
		foundKey, dupHandle, err := FetchDuplicatedHandle(context.TODO(), key, distinct, txn, c.tblInfo.ID)
		if err != nil || !foundKey {
			return false, nil, err
		}
		if dupHandle != nil && !dupHandle.Equal(h) {
			return false, nil, err
		}
		continue
	}
	return true, h, nil
}

// FetchDuplicatedHandle is used to find the duplicated row's handle for a given unique index key.
func FetchDuplicatedHandle(ctx context.Context, key kv.Key, distinct bool,
	txn kv.Transaction, tableID int64) (foundKey bool, dupHandle kv.Handle, err error) {
	if tablecodec.IsTempIndexKey(key) {
		return fetchDuplicatedHandleForTempIndexKey(ctx, key, distinct, txn, tableID)
	}
	// The index key is not from temp index.
	val, err := getKeyInTxn(ctx, txn, key)
	if err != nil || len(val) == 0 {
		return false, nil, err
	}
	if distinct {
		h, err := tablecodec.DecodeHandleInIndexValue(val)
		return true, h, err
	}
	return true, nil, nil
}

func fetchDuplicatedHandleForTempIndexKey(ctx context.Context, tempKey kv.Key, distinct bool,
	txn kv.Transaction, tableID int64) (foundKey bool, dupHandle kv.Handle, err error) {
	tempRawVal, err := getKeyInTxn(ctx, txn, tempKey)
	if err != nil {
		return false, nil, err
	}
	if tempRawVal == nil {
		originKey := tempKey.Clone()
		tablecodec.TempIndexKey2IndexKey(originKey)
		originVal, err := getKeyInTxn(ctx, txn, originKey)
		if err != nil || originVal == nil {
			return false, nil, err
		}
		if distinct {
			originHandle, err := tablecodec.DecodeHandleInIndexValue(originVal)
			if err != nil {
				return false, nil, err
			}
			return true, originHandle, err
		}
		return false, nil, nil
	}
	tempVal, err := tablecodec.DecodeTempIndexValue(tempRawVal)
	if err != nil {
		return false, nil, err
	}
	curElem := tempVal.Current()
	if curElem.Delete {
		originKey := tempKey.Clone()
		tablecodec.TempIndexKey2IndexKey(originKey)
		originVal, err := getKeyInTxn(ctx, txn, originKey)
		if err != nil || originVal == nil {
			return false, nil, err
		}
		if distinct {
			originHandle, err := tablecodec.DecodeHandleInIndexValue(originVal)
			if err != nil {
				return false, nil, err
			}
			if originHandle.Equal(curElem.Handle) {
				// The key has been deleted. This is not a duplicated key.
				return false, nil, nil
			}
			// The inequality means multiple modifications happened in the same key.
			// We use the handle in origin index value to check if the row exists.
			recPrefix := tablecodec.GenTableRecordPrefix(tableID)
			rowKey := tablecodec.EncodeRecordKey(recPrefix, originHandle)
			rowVal, err := getKeyInTxn(ctx, txn, rowKey)
			if err != nil || rowVal == nil {
				return false, nil, err
			}
			// The row exists. This is the duplicated key.
			return true, originHandle, nil
		}
		return false, nil, nil
	}
	// The value in temp index is not the delete marker.
	if distinct {
		h, err := tablecodec.DecodeHandleInIndexValue(curElem.Value)
		return true, h, err
	}
	return true, nil, nil
}

// getKeyInTxn gets the value of the key in the transaction, and ignore the ErrNotExist error.
func getKeyInTxn(ctx context.Context, txn kv.Transaction, key kv.Key) ([]byte, error) {
	val, err := txn.Get(ctx, key)
	if err != nil {
		if kv.IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (c *index) FetchValues(r []types.Datum, vals []types.Datum) ([]types.Datum, error) {
	needLength := len(c.idxInfo.Columns)
	if vals == nil || cap(vals) < needLength {
		vals = make([]types.Datum, needLength)
	}
	vals = vals[:needLength]
	for i, ic := range c.idxInfo.Columns {
		if ic.Offset < 0 || ic.Offset >= len(r) {
			return nil, table.ErrIndexOutBound.GenWithStackByArgs(ic.Name, ic.Offset, r)
		}
		vals[i] = r[ic.Offset]
	}
	return vals, nil
}

// FindChangingCol finds the changing column in idxInfo.
func FindChangingCol(cols []*table.Column, idxInfo *model.IndexInfo) *table.Column {
	for _, ic := range idxInfo.Columns {
		if col := cols[ic.Offset]; col.ChangeStateInfo != nil {
			return col
		}
	}
	return nil
}

// IsIndexWritable check whether the index is writable.
func IsIndexWritable(idx table.Index) bool {
	s := idx.Meta().State
	if s != model.StateDeleteOnly && s != model.StateDeleteReorganization {
		return true
	}
	return false
}

// BuildRowcodecColInfoForIndexColumns builds []rowcodec.ColInfo for the given index.
// The result can be used for decoding index key-values.
func BuildRowcodecColInfoForIndexColumns(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) []rowcodec.ColInfo {
	colInfo := make([]rowcodec.ColInfo, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := tblInfo.Columns[idxCol.Offset]
		colInfo = append(colInfo, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()),
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		})
	}
	return colInfo
}

// BuildFieldTypesForIndexColumns builds the index columns field types.
func BuildFieldTypesForIndexColumns(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) []*types.FieldType {
	tps := make([]*types.FieldType, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := tblInfo.Columns[idxCol.Offset]
		tps = append(tps, rowcodec.FieldTypeFromModelColumn(col))
	}
	return tps
}

// TryAppendCommonHandleRowcodecColInfos tries to append common handle columns to `colInfo`.
func TryAppendCommonHandleRowcodecColInfos(colInfo []rowcodec.ColInfo, tblInfo *model.TableInfo) []rowcodec.ColInfo {
	if !tblInfo.IsCommonHandle || tblInfo.CommonHandleVersion == 0 {
		return colInfo
	}
	if pkIdx := FindPrimaryIndex(tblInfo); pkIdx != nil {
		for _, idxCol := range pkIdx.Columns {
			col := tblInfo.Columns[idxCol.Offset]
			colInfo = append(colInfo, rowcodec.ColInfo{
				ID: col.ID,
				Ft: rowcodec.FieldTypeFromModelColumn(col),
			})
		}
	}
	return colInfo
}

// GenIndexValueFromIndex generate index value from index.
func GenIndexValueFromIndex(key []byte, value []byte, tblInfo *model.TableInfo, idxInfo *model.IndexInfo) ([]string, error) {
	idxColLen := len(idxInfo.Columns)
	colInfos := BuildRowcodecColInfoForIndexColumns(idxInfo, tblInfo)
	values, err := tablecodec.DecodeIndexKV(key, value, idxColLen, tablecodec.HandleNotNeeded, colInfos)
	if err != nil {
		return nil, errors.Trace(err)
	}
	valueStr := make([]string, 0, idxColLen)
	for i, val := range values[:idxColLen] {
		d, err := tablecodec.DecodeColumnValue(val, colInfos[i].Ft, time.Local)
		if err != nil {
			return nil, errors.Trace(err)
		}
		str, err := d.ToString()
		if err != nil {
			str = string(val)
		}
		if types.IsBinaryStr(colInfos[i].Ft) || types.IsTypeBit(colInfos[i].Ft) {
			str = util.FmtNonASCIIPrintableCharToHex(str)
		}
		valueStr = append(valueStr, str)
	}

	return valueStr, nil
}
