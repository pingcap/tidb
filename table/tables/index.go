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
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"context"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// indexIter is for KV store index iterator.
type indexIter struct {
	it     kv.Iterator
	idx    *index
	prefix kv.Key
}

// Close does the clean up works when KV store index iterator is closed.
func (c *indexIter) Close() {
	if c.it != nil {
		c.it.Close()
		c.it = nil
	}
}

// Next returns current key and moves iterator to the next step.
func (c *indexIter) Next() (val []types.Datum, h kv.Handle, err error) {
	if !c.it.Valid() {
		return nil, nil, errors.Trace(io.EOF)
	}
	if !c.it.Key().HasPrefix(c.prefix) {
		return nil, nil, errors.Trace(io.EOF)
	}
	// get indexedValues
	buf := c.it.Key()[len(c.prefix):]
	vv, err := codec.Decode(buf, len(c.idx.idxInfo.Columns))
	if err != nil {
		return nil, nil, err
	}
	if len(vv) > len(c.idx.idxInfo.Columns) {
		h = kv.IntHandle(vv[len(vv)-1].GetInt64())
		val = vv[0 : len(vv)-1]
	} else {
		// If the index is unique and the value isn't nil, the handle is in value.
		h, err = tablecodec.DecodeHandleInUniqueIndexValue(c.it.Value(), c.idx.tblInfo.IsCommonHandle)
		if err != nil {
			return nil, nil, err
		}
		val = vv
	}
	// update new iter to next
	err = c.it.Next()
	if err != nil {
		return nil, nil, err
	}
	return
}

// index is the data structure for index data in the KV store.
type index struct {
	idxInfo                *model.IndexInfo
	tblInfo                *model.TableInfo
	prefix                 kv.Key
	containNonBinaryString bool
	phyTblID               int64
}

// ContainsNonBinaryString checks whether the index columns contains non binary string column, the input
// colInfos should be column info correspond to the table contains the index.
func ContainsNonBinaryString(idxCols []*model.IndexColumn, colInfos []*model.ColumnInfo) bool {
	for _, idxCol := range idxCols {
		col := colInfos[idxCol.Offset]
		if col.EvalType() == types.ETString && !mysql.HasBinaryFlag(col.Flag) {
			return true
		}
	}
	return false
}

func (c *index) checkContainNonBinaryString() bool {
	return ContainsNonBinaryString(c.idxInfo.Columns, c.tblInfo.Columns)
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
	index.containNonBinaryString = index.checkContainNonBinaryString()
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

func (c *index) GenIndexValue(sc *stmtctx.StatementContext, indexedValues []types.Datum, distinct bool, untouched bool, h kv.Handle) (val []byte, err error) {
	return tablecodec.GenIndexValueNew(sc, c.tblInfo, c.idxInfo, c.containNonBinaryString, distinct, untouched, indexedValues, h, c.phyTblID)
}

// Create creates a new entry in the kvIndex data.
// If the index is unique and there is an existing entry with the same key,
// Create will return the existing entry's handle as the first return value, ErrKeyExists as the second return value.
// Value layout:
//		+--New Encoding (with restore data, or common handle, or index is global)
//		|
//		|  Layout: TailLen | Options      | Padding      | [IntHandle] | [UntouchedFlag]
//		|  Length:   1     | len(options) | len(padding) |    8        |     1
//		|
//		|  TailLen:       len(padding) + len(IntHandle) + len(UntouchedFlag)
//		|  Options:       Encode some value for new features, such as common handle, new collations or global index.
//		|                 See below for more information.
//		|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
//		|  IntHandle:     Only exists when table use int handles and index is unique.
//		|  UntouchedFlag: Only exists when index is untouched.
//		|
//		|  Layout of Options:
//		|
//		|     Segment:             Common Handle                 |     Global Index      | New Collation
// 		|     Layout:  CHandle Flag | CHandle Len | CHandle      | PidFlag | PartitionID | restoreData
//		|     Length:     1         | 2           | len(CHandle) |    1    |    8        | len(restoreData)
//		|
//		|     Common Handle Segment: Exists when unique index used common handles.
//		|     Global Index Segment:  Exists when index is global.
//		|     New Collation Segment: Exists when new collation is used and index contains non-binary string.
//		|
//		+--Old Encoding (without restore data, integer handle, local)
//
//		   Layout: [Handle] | [UntouchedFlag]
//		   Length:   8      |     1
//
//		   Handle:        Only exists in unique index.
//		   UntouchedFlag: Only exists when index is untouched.
//
//		   If neither Handle nor UntouchedFlag exists, value will be one single byte '0' (i.e. []byte{'0'}).
//		   Length of value <= 9, use to distinguish from the new encoding.
//
func (c *index) Create(sctx sessionctx.Context, us kv.UnionStore, indexedValues []types.Datum, h kv.Handle, opts ...table.CreateIdxOptFunc) (kv.Handle, error) {
	if c.Meta().Unique {
		us.CacheTableInfo(c.phyTblID, c.tblInfo)
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
		if err == nil && len(v) != 0 {
			return nil, nil
		}
	}

	// save the key buffer to reuse.
	writeBufs.IndexKeyBuf = key
	idxVal, err := tablecodec.GenIndexValueNew(sctx.GetSessionVars().StmtCtx, c.tblInfo, c.idxInfo,
		c.containNonBinaryString, distinct, opt.Untouched, indexedValues, h, c.phyTblID)
	if err != nil {
		return nil, err
	}

	if !distinct || skipCheck || opt.Untouched {
		err = us.GetMemBuffer().Set(key, idxVal)
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
	if sctx.GetSessionVars().LazyCheckKeyNotExists() {
		value, err = us.GetMemBuffer().Get(ctx, key)
	} else {
		value, err = us.Get(ctx, key)
	}
	if err != nil && !kv.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil || len(value) == 0 {
		if sctx.GetSessionVars().LazyCheckKeyNotExists() && err != nil {
			err = us.GetMemBuffer().SetWithFlags(key, idxVal, kv.SetPresumeKeyNotExists)
		} else {
			err = us.GetMemBuffer().Set(key, idxVal)
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
func (c *index) Delete(sc *stmtctx.StatementContext, us kv.UnionStore, indexedValues []types.Datum, h kv.Handle) error {
	key, distinct, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		return err
	}
	if distinct {
		err = us.GetMemBuffer().DeleteWithFlags(key, kv.SetNeedLocked)
	} else {
		err = us.GetMemBuffer().Delete(key)
	}
	return err
}

// Drop removes the KV index from store.
func (c *index) Drop(us kv.UnionStore) error {
	it, err := us.Iter(c.prefix, c.prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()

	// remove all indices
	for it.Valid() {
		if !it.Key().HasPrefix(c.prefix) {
			break
		}
		err := us.GetMemBuffer().Delete(it.Key())
		if err != nil {
			return err
		}
		err = it.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

// Seek searches KV index for the entry with indexedValues.
func (c *index) Seek(sc *stmtctx.StatementContext, r kv.Retriever, indexedValues []types.Datum) (iter table.IndexIterator, hit bool, err error) {
	key, _, err := c.GenIndexKey(sc, indexedValues, nil, nil)
	if err != nil {
		return nil, false, err
	}

	upperBound := c.prefix.PrefixNext()
	it, err := r.Iter(key, upperBound)
	if err != nil {
		return nil, false, err
	}
	// check if hit
	hit = false
	if it.Valid() && it.Key().Cmp(key) == 0 {
		hit = true
	}
	return &indexIter{it: it, idx: c, prefix: c.prefix}, hit, nil
}

// SeekFirst returns an iterator which points to the first entry of the KV index.
func (c *index) SeekFirst(r kv.Retriever) (iter table.IndexIterator, err error) {
	upperBound := c.prefix.PrefixNext()
	it, err := r.Iter(c.prefix, upperBound)
	if err != nil {
		return nil, err
	}
	return &indexIter{it: it, idx: c, prefix: c.prefix}, nil
}

func (c *index) Exist(sc *stmtctx.StatementContext, us kv.UnionStore, indexedValues []types.Datum, h kv.Handle) (bool, kv.Handle, error) {
	key, distinct, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		return false, nil, err
	}

	value, err := us.Get(context.TODO(), key)
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
