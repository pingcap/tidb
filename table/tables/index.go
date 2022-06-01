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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
)

// index is the data structure for index data in the KV store.
type index struct {
	idxInfo  *model.IndexInfo
	tblInfo  *model.TableInfo
	prefix   kv.Key
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
	// The prefix can't encode from tblInfo.ID, because table partition may change the id to partition id.
	var prefix kv.Key
	if indexInfo.Global {
		// In glabal index of partition table, prefix start with tblInfo.ID.
		prefix = tablecodec.EncodeTableIndexPrefix(tblInfo.ID, indexInfo.ID)
	} else {
		// Otherwise, start with physicalID.
		prefix = tablecodec.EncodeTableIndexPrefix(physicalID, indexInfo.ID)
	}
	index := &index{
		idxInfo:  indexInfo,
		tblInfo:  tblInfo,
		prefix:   prefix,
		phyTblID: physicalID,
	}
	return index
}

// Meta returns index info.
func (c *index) Meta() *model.IndexInfo {
	return c.idxInfo
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func (c *index) GenIndexKey(sc *stmtctx.StatementContext, indexedValues []types.Datum, h kv.Handle, buf []byte) (key []byte, distinct bool, err error) {
	idxTblID := c.phyTblID
	if c.idxInfo.Global {
		idxTblID = c.tblInfo.ID
	}
	return tablecodec.GenIndexKey(sc, c.tblInfo, c.idxInfo, idxTblID, indexedValues, h, buf)
}

// Create creates a new entry in the kvIndex data.
// If the index is unique and there is an existing entry with the same key,
// Create will return the existing entry's handle as the first return value, ErrKeyExists as the second return value.
func (c *index) Create(sctx sessionctx.Context, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle, handleRestoreData []types.Datum, opts ...table.CreateIdxOptFunc) (kv.Handle, error) {
	if c.Meta().Unique {
		txn.CacheTableInfo(c.phyTblID, c.tblInfo)
	}
	var opt table.CreateIdxOpt
	for _, fn := range opts {
		fn(&opt)
	}
	vars := sctx.GetSessionVars()
	writeBufs := vars.GetWriteStmtBufs()
	skipCheck := vars.StmtCtx.BatchCheck
	key, distinct, err := c.GenIndexKey(vars.StmtCtx, indexedValues, h, writeBufs.IndexKeyBuf)
	if err != nil {
		return nil, err
	}

	ctx := opt.Ctx
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
				return nil, nil
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
	idxVal, err := tablecodec.GenIndexValuePortal(sctx.GetSessionVars().StmtCtx, c.tblInfo, c.idxInfo, c.needRestoredData, distinct, opt.Untouched, indexedValues, h, c.phyTblID, handleRestoreData)
	if err != nil {
		return nil, err
	}

	opt.IgnoreAssertion = opt.IgnoreAssertion || c.idxInfo.State != model.StatePublic

	if !distinct || skipCheck || opt.Untouched {
		err = txn.GetMemBuffer().Set(key, idxVal)
		if err != nil {
			return nil, err
		}
		if !opt.IgnoreAssertion && (!opt.Untouched) {
			if sctx.GetSessionVars().LazyCheckKeyNotExists() && !txn.IsPessimistic() {
				err = txn.SetAssertion(key, kv.SetAssertUnknown)
			} else {
				err = txn.SetAssertion(key, kv.SetAssertNotExist)
			}
		}
		return nil, err
	}

	if ctx != nil {
		if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
			span1 := span.Tracer().StartSpan("index.Create", opentracing.ChildOf(span.Context()))
			defer span1.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span1)
		}
	} else {
		ctx = context.TODO()
	}

	var value []byte
	if c.tblInfo.TempTableType != model.TempTableNone {
		// Always check key for temporary table because it does not write to TiKV
		value, err = txn.Get(ctx, key)
	} else if sctx.GetSessionVars().LazyCheckKeyNotExists() {
		value, err = txn.GetMemBuffer().Get(ctx, key)
	} else {
		value, err = txn.Get(ctx, key)
	}
	if err != nil && !kv.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil || len(value) == 0 {
		lazyCheck := sctx.GetSessionVars().LazyCheckKeyNotExists() && err != nil
		if lazyCheck {
			err = txn.GetMemBuffer().SetWithFlags(key, idxVal, kv.SetPresumeKeyNotExists)
		} else {
			err = txn.GetMemBuffer().Set(key, idxVal)
		}
		if err != nil {
			return nil, err
		}
		if opt.IgnoreAssertion {
			return nil, nil
		}
		if lazyCheck && !txn.IsPessimistic() {
			err = txn.SetAssertion(key, kv.SetAssertUnknown)
		} else {
			err = txn.SetAssertion(key, kv.SetAssertNotExist)
		}
		return nil, err
	}

	handle, err := tablecodec.DecodeHandleInUniqueIndexValue(value, c.tblInfo.IsCommonHandle)
	if err != nil {
		return nil, err
	}
	return handle, kv.ErrKeyExists
}

// Delete removes the entry for handle h and indexedValues from KV index.
func (c *index) Delete(sc *stmtctx.StatementContext, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle) error {
	key, distinct, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		return err
	}
	if distinct {
		err = txn.GetMemBuffer().DeleteWithFlags(key, kv.SetNeedLocked)
	} else {
		err = txn.GetMemBuffer().Delete(key)
	}
	if err != nil {
		return err
	}
	if c.idxInfo.State == model.StatePublic {
		// If the index is in public state, delete this index means it must exists.
		err = txn.SetAssertion(key, kv.SetAssertExist)
	}
	return err
}

func (c *index) Exist(sc *stmtctx.StatementContext, txn kv.Transaction, indexedValues []types.Datum, h kv.Handle) (bool, kv.Handle, error) {
	key, distinct, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		return false, nil, err
	}

	value, err := txn.Get(context.TODO(), key)
	if kv.IsErrNotFound(err) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}

	// For distinct index, the value of key is handle.
	if distinct {
		var handle kv.Handle
		handle, err := tablecodec.DecodeHandleInUniqueIndexValue(value, c.tblInfo.IsCommonHandle)
		if err != nil {
			return false, nil, err
		}
		if !handle.Equal(h) {
			return true, handle, kv.ErrKeyExists
		}
		return true, handle, nil
	}

	return true, h, nil
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
