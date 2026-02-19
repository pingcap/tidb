// Copyright 2015 PingCAP, Inc.
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

package codec

import (
	"bytes"
	"hash"
	"io"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// HashChunkColumns writes the encoded value of each row's column, which of index `colIdx`, to h.
func HashChunkColumns(typeCtx types.Context, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, colIdx int, buf []byte, isNull []bool) (err error) {
	return HashChunkSelected(typeCtx, h, chk, tp, colIdx, buf, isNull, nil, false)
}

// HashChunkSelected writes the encoded value of selected row's column, which of index `colIdx`, to h.
// sel indicates which rows are selected. If it is nil, all rows are selected.
func HashChunkSelected(typeCtx types.Context, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, colIdx int, buf []byte,
	isNull, sel []bool, ignoreNull bool) (err error) {
	var b []byte
	column := chk.Column(colIdx)
	rows := chk.NumRows()
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		i64s := column.Int64s()
		for i, v := range i64s {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = uvarintFlag
				if !mysql.HasUnsignedFlag(tp.GetFlag()) && v < 0 {
					buf[0] = varintFlag
				}
				b = column.GetRaw(i)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeFloat:
		f32s := column.Float32s()
		for i, f := range f32s {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = floatFlag
				d := float64(f)
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				if d == 0 {
					d = 0
				}
				b = unsafe.Slice((*byte)(unsafe.Pointer(&d)), sizeFloat64)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeDouble:
		f64s := column.Float64s()
		for i := range f64s {
			f := f64s[i]
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = floatFlag
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				if f == 0 {
					f = 0
				}
				b = unsafe.Slice((*byte)(unsafe.Pointer(&f)), sizeFloat64)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		collator := collate.GetCollator(tp.GetCollate())
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				b = column.GetBytes(i)
				b = collator.ImmutableKey(string(hack.String(b)))
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		ts := column.Times()
		for i, t := range ts {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = uintFlag

				var v uint64
				v, err = t.ToPackedUint()
				if err != nil {
					return
				}
				b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeDuration:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = durationFlag
				// duration may have negative value, so we cannot use String to encode directly.
				b = column.GetRaw(i)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeNewDecimal:
		ds := column.Decimals()
		for i, d := range ds {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = decimalFlag
				// If hash is true, we only consider the original value of this decimal and ignore it's precision.
				b, err = d.ToHashKey()
				if err != nil {
					return
				}
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeEnum:
		var collator collate.Collator
		if !mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
			collator = collate.GetCollator(tp.GetCollate())
		}
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
				buf[0] = uvarintFlag
				v := column.GetEnum(i).Value
				b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)
			} else {
				buf[0] = compactBytesFlag
				v := column.GetEnum(i).Value
				str := ""
				if enum, err := types.ParseEnumValue(tp.GetElems(), v); err == nil {
					// str will be empty string if v out of definition of enum.
					str = enum.Name
				}
				b = collator.ImmutableKey(str)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeSet:
		collator := collate.GetCollator(tp.GetCollate())
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				s, err := types.ParseSetValue(tp.GetElems(), column.GetSet(i).Value)
				if err != nil {
					return err
				}
				b = collator.ImmutableKey(s.Name)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeBit:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
				buf[0] = uvarintFlag
				v, err1 := types.BinaryLiteral(column.GetBytes(i)).ToInt(typeCtx)
				terror.Log(errors.Trace(err1))
				b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeJSON:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = jsonFlag
				json := column.GetJSON(i)
				b = b[:0]
				b = json.HashValue(b)
			}

			// As the golang doc described, `Hash.Write` never returns an error..
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeTiDBVectorFloat32:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = vectorFloat32Flag
				v := column.GetVectorFloat32(i)
				b = v.SerializeTo(nil)
			}

			// As the golang doc described, `Hash.Write` never returns an error..
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeNull:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			isNull[i] = !ignoreNull
			buf[0] = NilFlag
			_, _ = h[i].Write(buf)
		}
	default:
		return errors.Errorf("unsupport column type for encode %d", tp.GetType())
	}
	return
}

// HashChunkRow writes the encoded values to w.
// If two rows are logically equal, it will generate the same bytes.
func HashChunkRow(typeCtx types.Context, w io.Writer, row chunk.Row, allTypes []*types.FieldType, colIdx []int, buf []byte) (err error) {
	var b []byte
	for i, idx := range colIdx {
		buf[0], b, err = encodeHashChunkRowIdx(typeCtx, row, allTypes[i], idx)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = w.Write(buf)
		if err != nil {
			return
		}
		_, err = w.Write(b)
		if err != nil {
			return
		}
	}
	return err
}

// EqualChunkRow returns a boolean reporting whether row1 and row2
// with their types and column index are logically equal.
func EqualChunkRow(typeCtx types.Context,
	row1 chunk.Row, allTypes1 []*types.FieldType, colIdx1 []int,
	row2 chunk.Row, allTypes2 []*types.FieldType, colIdx2 []int,
) (bool, error) {
	if len(colIdx1) != len(colIdx2) {
		return false, errors.Errorf("Internal error: Hash columns count mismatch, col1: %d, col2: %d", len(colIdx1), len(colIdx2))
	}
	for i := range colIdx1 {
		idx1, idx2 := colIdx1[i], colIdx2[i]
		flag1, b1, err := encodeHashChunkRowIdx(typeCtx, row1, allTypes1[i], idx1)
		if err != nil {
			return false, errors.Trace(err)
		}
		flag2, b2, err := encodeHashChunkRowIdx(typeCtx, row2, allTypes2[i], idx2)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !(flag1 == flag2 && bytes.Equal(b1, b2)) {
			return false, nil
		}
	}
	return true, nil
}
