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
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"unicode/utf8"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/rowcodec"
)

// EncodeHandle encodes handle in data.
func EncodeHandle(h int64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], uint64(h))
	return data[:]
}

// DecodeHandle decodes handle in data.
func DecodeHandle(data []byte) (int64, error) {
	dLen := len(data)
	if dLen <= tablecodec.MaxOldEncodeValueLen {
		return int64(binary.BigEndian.Uint64(data)), nil
	}
	return int64(binary.BigEndian.Uint64(data[dLen-int(data[0]):])), nil
}

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
func (c *indexIter) Next() (val []types.Datum, h int64, err error) {
	if !c.it.Valid() {
		return nil, 0, errors.Trace(io.EOF)
	}
	if !c.it.Key().HasPrefix(c.prefix) {
		return nil, 0, errors.Trace(io.EOF)
	}
	// get indexedValues
	buf := c.it.Key()[len(c.prefix):]
	vv, err := codec.Decode(buf, len(c.idx.idxInfo.Columns))
	if err != nil {
		return nil, 0, err
	}
	if len(vv) > len(c.idx.idxInfo.Columns) {
		h = vv[len(vv)-1].GetInt64()
		val = vv[0 : len(vv)-1]
	} else {
		// If the index is unique and the value isn't nil, the handle is in value.
		h, err = DecodeHandle(c.it.Value())
		if err != nil {
			return nil, 0, err
		}
		val = vv
	}
	// update new iter to next
	err = c.it.Next()
	if err != nil {
		return nil, 0, err
	}
	return
}

// index is the data structure for index data in the KV store.
type index struct {
	idxInfo                *model.IndexInfo
	tblInfo                *model.TableInfo
	prefix                 kv.Key
	containNonBinaryString bool
}

func (c *index) checkContainNonBinaryString() bool {
	for _, idxCol := range c.idxInfo.Columns {
		col := c.tblInfo.Columns[idxCol.Offset]
		if col.EvalType() == types.ETString && !mysql.HasBinaryFlag(col.Flag) {
			return true
		}
	}
	return false
}

// NewIndex builds a new Index object.
func NewIndex(physicalID int64, tblInfo *model.TableInfo, indexInfo *model.IndexInfo) table.Index {
	index := &index{
		idxInfo: indexInfo,
		tblInfo: tblInfo,
		// The prefix can't encode from tblInfo.ID, because table partition may change the id to partition id.
		prefix: tablecodec.EncodeTableIndexPrefix(physicalID, indexInfo.ID),
	}
	index.containNonBinaryString = index.checkContainNonBinaryString()
	return index
}

// Meta returns index info.
func (c *index) Meta() *model.IndexInfo {
	return c.idxInfo
}

func (c *index) getIndexKeyBuf(buf []byte, defaultCap int) []byte {
	if buf != nil {
		return buf[:0]
	}

	return make([]byte, 0, defaultCap)
}

// FormatIndexValuesIfNeeded formats the index value if needed.
// For string column, truncates the index values created using only the leading part of column values.
// For decimal column, convert to the column info precision.
func FormatIndexValuesIfNeeded(sc *stmtctx.StatementContext, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, indexedValues []types.Datum) ([]types.Datum, error) {
	for i := 0; i < len(indexedValues); i++ {
		v := &indexedValues[i]
		switch v.Kind() {
		case types.KindString, types.KindBytes:
			ic := idxInfo.Columns[i]
			colCharset := tblInfo.Columns[ic.Offset].Charset
			colValue := v.GetBytes()
			isUTF8Charset := colCharset == charset.CharsetUTF8 || colCharset == charset.CharsetUTF8MB4
			origKind := v.Kind()
			if isUTF8Charset {
				if ic.Length != types.UnspecifiedLength && utf8.RuneCount(colValue) > ic.Length {
					rs := bytes.Runes(colValue)
					truncateStr := string(rs[:ic.Length])
					// truncate value and limit its length
					v.SetString(truncateStr, tblInfo.Columns[ic.Offset].Collate)
					if origKind == types.KindBytes {
						v.SetBytes(v.GetBytes())
					}
				}
			} else if ic.Length != types.UnspecifiedLength && len(colValue) > ic.Length {
				// truncate value and limit its length
				v.SetBytes(colValue[:ic.Length])
				if origKind == types.KindString {
					v.SetString(v.GetString(), tblInfo.Columns[ic.Offset].Collate)
				}
			}
		case types.KindMysqlDecimal:
			col := tblInfo.Columns[idxInfo.Columns[i].Offset]
			// Always use the column precision.
			value, err := v.ConvertTo(sc, &col.FieldType)
			if err != nil {
				return nil, err
			}
			indexedValues[i] = value
		}
	}

	return indexedValues, nil
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func (c *index) GenIndexKey(sc *stmtctx.StatementContext, indexedValues []types.Datum, h int64, buf []byte) (key []byte, distinct bool, err error) {
	if c.idxInfo.Unique {
		// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
		// A UNIQUE index creates a constraint such that all values in the index must be distinct.
		// An error occurs if you try to add a new row with a key value that matches an existing row.
		// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		distinct = true
		for _, cv := range indexedValues {
			if cv.IsNull() {
				distinct = false
				break
			}
		}
	}

	// For string columns, indexes can be created using only the leading part of column values,
	// using col_name(length) syntax to specify an index prefix length.
	// For decimal column, convert to the column info precision.
	indexedValues, err = FormatIndexValuesIfNeeded(sc, c.tblInfo, c.idxInfo, indexedValues)
	if err != nil {
		return nil, false, err
	}
	key = c.getIndexKeyBuf(buf, len(c.prefix)+len(indexedValues)*9+9)
	key = append(key, []byte(c.prefix)...)
	key, err = codec.EncodeKey(sc, key, indexedValues...)
	if !distinct && err == nil {
		key, err = codec.EncodeKey(sc, key, types.NewDatum(h))
	}
	if err != nil {
		return nil, false, err
	}
	return
}

// Create creates a new entry in the kvIndex data.
// If the index is unique and there is an existing entry with the same key,
// Create will return the existing entry's handle as the first return value, ErrKeyExists as the second return value.
// Value layout:
//		+--With Restore Data(for indices on string columns)
//		|  |
//		|  +--Non Unique (TailLen = len(PaddingData) + len(Flag), TailLen < 8 always)
//		|  |  |
//		|  |  +--Without Untouched Flag:
//		|  |  |
//		|  |  |  Layout: TailLen |      RestoreData  |      PaddingData
//		|  |  |  Length: 1       | size(RestoreData) | size(paddingData)
//		|  |  |
//		|  |  |  The length >= 10 always because of padding.
//		|  |  |
//		|  |  +--With Untouched Flag:
//		|  |
//		|  |     Layout: TailLen |    RestoreData    |      PaddingData  | Flag
//		|  |     Length: 1       | size(RestoreData) | size(paddingData) |  1
//		|  |
//		|  |     The length >= 11 always because of padding.
//		|  |
//		|  +--Unique (TailLen = len(Handle) + len(Flag), TailLen == 8 || TailLen == 9)
//		|     |
//		|     +--Without Untouched Flag:
//		|     |
//		|     |  Layout: 0x08 |    RestoreData    |  Handle
//		|     |  Length: 1    | size(RestoreData) |   8
//		|     |
//		|     |  The length >= 10 always since size(RestoreData) > 0.
//		|     |
//		|     +--With Untouched Flag:
//		|
//		|        Layout: 0x09 |      RestoreData  |  Handle  | Flag
//		|        Length: 1    | size(RestoreData) |   8      | 1
//		|
//		|   	 The length >= 11 always since size(RestoreData) > 0.
//		|
//		+--Without Restore Data(same with old layout)
//		|
//		+--Non Unique
//		|  |
//		|  +--Without Untouched Flag:
//		|  |
//		|  |  Layout: '0'
//		|  |  Length:  1
//		|  |
//		|  +--With Untouched Flag:
//		|
//		|     Layout: Flag
//		|     Length:  1
//		|
//		+--Unique
//		|
//		+--Without Untouched Flag:
//		|
//		|  Layout: Handle
//		|  Length:   8
//		|
//		+--With Untouched Flag:
//
//		Layout: Handle | Flag
//		Length:   8    |  1
func (c *index) Create(sctx sessionctx.Context, rm kv.RetrieverMutator, indexedValues []types.Datum, h int64, opts ...table.CreateIdxOptFunc) (int64, error) {
	var opt table.CreateIdxOpt
	for _, fn := range opts {
		fn(&opt)
	}
	vars := sctx.GetSessionVars()
	writeBufs := vars.GetWriteStmtBufs()
	skipCheck := vars.StmtCtx.BatchCheck
	key, distinct, err := c.GenIndexKey(vars.StmtCtx, indexedValues, h, writeBufs.IndexKeyBuf)
	if err != nil {
		return 0, err
	}

	ctx := opt.Ctx
	if opt.Untouched {
		txn, err1 := sctx.Txn(true)
		if err1 != nil {
			return 0, err1
		}
		// If the index kv was untouched(unchanged), and the key/value already exists in mem-buffer,
		// should not overwrite the key with un-commit flag.
		// So if the key exists, just do nothing and return.
		_, err = txn.GetMemBuffer().Get(ctx, key)
		if err == nil {
			return 0, nil
		}
	}

	// save the key buffer to reuse.
	writeBufs.IndexKeyBuf = key
	var idxVal []byte
	if collate.NewCollationEnabled() && c.containNonBinaryString {
		colIds := make([]int64, len(c.idxInfo.Columns))
		for i, col := range c.idxInfo.Columns {
			colIds[i] = c.tblInfo.Columns[col.Offset].ID
		}
		rd := rowcodec.Encoder{Enable: true}
		rowRestoredValue, err := rd.Encode(sctx.GetSessionVars().StmtCtx, colIds, indexedValues, nil)
		if err != nil {
			return 0, err
		}
		idxVal = make([]byte, 1+len(rowRestoredValue))
		copy(idxVal[1:], rowRestoredValue)
		tailLen := 0
		if distinct {
			// The len of the idxVal is always >= 10 since len (restoredValue) > 0.
			tailLen += 8
			idxVal = append(idxVal, EncodeHandle(h)...)
		} else if len(idxVal) < 10 {
			// Padding the len to 10
			paddingLen := 10 - len(idxVal)
			tailLen += paddingLen
			idxVal = append(idxVal, bytes.Repeat([]byte{0x0}, paddingLen)...)
		}
		if opt.Untouched {
			// If index is untouched and fetch here means the key is exists in TiKV, but not in txn mem-buffer,
			// then should also write the untouched index key/value to mem-buffer to make sure the data
			// is consistent with the index in txn mem-buffer.
			tailLen += 1
			idxVal = append(idxVal, kv.UnCommitIndexKVFlag)
		}
		idxVal[0] = byte(tailLen)
	} else {
		idxVal = make([]byte, 0)
		if distinct {
			idxVal = EncodeHandle(h)
		}
		if opt.Untouched {
			// If index is untouched and fetch here means the key is exists in TiKV, but not in txn mem-buffer,
			// then should also write the untouched index key/value to mem-buffer to make sure the data
			// is consistent with the index in txn mem-buffer.
			idxVal = append(idxVal, kv.UnCommitIndexKVFlag)
		}
		if len(idxVal) == 0 {
			idxVal = []byte{'0'}
		}
	}

	if !distinct || skipCheck || opt.Untouched {
		err = rm.Set(key, idxVal)
		return 0, err
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

	value, err := rm.Get(ctx, key)
	if err != nil {
		if kv.IsErrNotFound(err) {
			err = rm.Set(key, idxVal)
			return 0, err
		}
		return 0, err
	}

	handle, err := DecodeHandle(value)
	if err != nil {
		return 0, err
	}
	return handle, kv.ErrKeyExists
}

// Delete removes the entry for handle h and indexdValues from KV index.
func (c *index) Delete(sc *stmtctx.StatementContext, m kv.Mutator, indexedValues []types.Datum, h int64) error {
	key, _, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		return err
	}
	err = m.Delete(key)
	return err
}

// Drop removes the KV index from store.
func (c *index) Drop(rm kv.RetrieverMutator) error {
	it, err := rm.Iter(c.prefix, c.prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()

	// remove all indices
	for it.Valid() {
		if !it.Key().HasPrefix(c.prefix) {
			break
		}
		err := rm.Delete(it.Key())
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
	key, _, err := c.GenIndexKey(sc, indexedValues, 0, nil)
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

func (c *index) Exist(sc *stmtctx.StatementContext, rm kv.RetrieverMutator, indexedValues []types.Datum, h int64) (bool, int64, error) {
	key, distinct, err := c.GenIndexKey(sc, indexedValues, h, nil)
	if err != nil {
		return false, 0, err
	}

	value, err := rm.Get(context.TODO(), key)
	if kv.IsErrNotFound(err) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}

	// For distinct index, the value of key is handle.
	if distinct {
		handle, err := DecodeHandle(value)
		if err != nil {
			return false, 0, err
		}

		if handle != h {
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
