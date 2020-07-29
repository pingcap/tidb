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
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
)

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag          byte = 0
	bytesFlag        byte = 1
	compactBytesFlag byte = 2
	intFlag          byte = 3
	uintFlag         byte = 4
	floatFlag        byte = 5
	decimalFlag      byte = 6
	durationFlag     byte = 7
	varintFlag       byte = 8
	uvarintFlag      byte = 9
	jsonFlag         byte = 10
	maxFlag          byte = 250
)

const (
	sizeUint64  = unsafe.Sizeof(uint64(0))
	sizeFloat64 = unsafe.Sizeof(float64(0))
)

func preRealloc(b []byte, vals []types.Datum, comparable bool) []byte {
	var size int
	for i := range vals {
		switch vals[i].Kind() {
		case types.KindInt64, types.KindUint64, types.KindMysqlEnum, types.KindMysqlSet, types.KindMysqlBit, types.KindBinaryLiteral:
			size += sizeInt(comparable)
		case types.KindString, types.KindBytes:
			size += sizeBytes(vals[i].GetBytes(), comparable)
		case types.KindMysqlTime, types.KindMysqlDuration, types.KindFloat32, types.KindFloat64:
			size += 9
		case types.KindNull, types.KindMinNotNull, types.KindMaxValue:
			size += 1
		case types.KindMysqlJSON:
			size += 2 + len(vals[i].GetBytes())
		case types.KindMysqlDecimal:
			size += 1 + types.MyDecimalStructSize
		default:
			return b
		}
	}
	return reallocBytes(b, size)
}

// encode will encode a datum and append it to a byte slice. If comparable is true, the encoded bytes can be sorted as it's original order.
// If hash is true, the encoded bytes can be checked equal as it's original value.
func encode(sc *stmtctx.StatementContext, b []byte, vals []types.Datum, comparable bool) (_ []byte, err error) {
	b = preRealloc(b, vals, comparable)
	for i, length := 0, len(vals); i < length; i++ {
		switch vals[i].Kind() {
		case types.KindInt64:
			b = encodeSignedInt(b, vals[i].GetInt64(), comparable)
		case types.KindUint64:
			b = encodeUnsignedInt(b, vals[i].GetUint64(), comparable)
		case types.KindFloat32, types.KindFloat64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, vals[i].GetFloat64())
		case types.KindString:
			b = encodeString(b, vals[i], comparable)
		case types.KindBytes:
			b = encodeBytes(b, vals[i].GetBytes(), comparable)
		case types.KindMysqlTime:
			b = append(b, uintFlag)
			b, err = EncodeMySQLTime(sc, vals[i].GetMysqlTime(), mysql.TypeUnspecified, b)
			if err != nil {
				return b, err
			}
		case types.KindMysqlDuration:
			// duration may have negative value, so we cannot use String to encode directly.
			b = append(b, durationFlag)
			b = EncodeInt(b, int64(vals[i].GetMysqlDuration().Duration))
		case types.KindMysqlDecimal:
			b = append(b, decimalFlag)
			b, err = EncodeDecimal(b, vals[i].GetMysqlDecimal(), vals[i].Length(), vals[i].Frac())
			if terror.ErrorEqual(err, types.ErrTruncated) {
				err = sc.HandleTruncate(err)
			} else if terror.ErrorEqual(err, types.ErrOverflow) {
				err = sc.HandleOverflow(err, err)
			}
		case types.KindMysqlEnum:
			b = encodeUnsignedInt(b, uint64(vals[i].GetMysqlEnum().ToNumber()), comparable)
		case types.KindMysqlSet:
			b = encodeUnsignedInt(b, uint64(vals[i].GetMysqlSet().ToNumber()), comparable)
		case types.KindMysqlBit, types.KindBinaryLiteral:
			// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
			var val uint64
			val, err = vals[i].GetBinaryLiteral().ToInt(sc)
			terror.Log(errors.Trace(err))
			b = encodeUnsignedInt(b, val, comparable)
		case types.KindMysqlJSON:
			b = append(b, jsonFlag)
			j := vals[i].GetMysqlJSON()
			b = append(b, j.TypeCode)
			b = append(b, j.Value...)
		case types.KindNull:
			b = append(b, NilFlag)
		case types.KindMinNotNull:
			b = append(b, bytesFlag)
		case types.KindMaxValue:
			b = append(b, maxFlag)
		default:
			return b, errors.Errorf("unsupport encode type %d", vals[i].Kind())
		}
	}

	return b, errors.Trace(err)
}

// EstimateValueSize uses to estimate the value  size of the encoded values.
func EstimateValueSize(sc *stmtctx.StatementContext, val types.Datum) (int, error) {
	l := 0
	switch val.Kind() {
	case types.KindInt64:
		l = valueSizeOfSignedInt(val.GetInt64())
	case types.KindUint64:
		l = valueSizeOfUnsignedInt(val.GetUint64())
	case types.KindFloat32, types.KindFloat64, types.KindMysqlTime, types.KindMysqlDuration:
		l = 9
	case types.KindString, types.KindBytes:
		l = valueSizeOfBytes(val.GetBytes())
	case types.KindMysqlDecimal:
		l = valueSizeOfDecimal(val.GetMysqlDecimal(), val.Length(), val.Frac()) + 1
	case types.KindMysqlEnum:
		l = valueSizeOfUnsignedInt(uint64(val.GetMysqlEnum().ToNumber()))
	case types.KindMysqlSet:
		l = valueSizeOfUnsignedInt(uint64(val.GetMysqlSet().ToNumber()))
	case types.KindMysqlBit, types.KindBinaryLiteral:
		val, err := val.GetBinaryLiteral().ToInt(sc)
		terror.Log(errors.Trace(err))
		l = valueSizeOfUnsignedInt(val)
	case types.KindMysqlJSON:
		l = 2 + len(val.GetMysqlJSON().Value)
	case types.KindNull, types.KindMinNotNull, types.KindMaxValue:
		l = 1
	default:
		return l, errors.Errorf("unsupported encode type %d", val.Kind())
	}
	return l, nil
}

// EncodeMySQLTime encodes datum of `KindMysqlTime` to []byte.
func EncodeMySQLTime(sc *stmtctx.StatementContext, t types.Time, tp byte, b []byte) (_ []byte, err error) {
	// Encoding timestamp need to consider timezone. If it's not in UTC, transform to UTC first.
	// This is compatible with `PBToExpr > convertTime`, and coprocessor assumes the passed timestamp is in UTC as well.
	if tp == mysql.TypeUnspecified {
		tp = t.Type()
	}
	if tp == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
		err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
		if err != nil {
			return nil, err
		}
	}
	var v uint64
	v, err = t.ToPackedUint()
	if err != nil {
		return nil, err
	}
	b = EncodeUint(b, v)
	return b, nil
}

func encodeString(b []byte, val types.Datum, comparable bool) []byte {
	if collate.NewCollationEnabled() && comparable {
		return encodeBytes(b, collate.GetCollator(val.Collation()).Key(val.GetString()), true)
	}
	return encodeBytes(b, val.GetBytes(), comparable)
}

func encodeBytes(b []byte, v []byte, comparable bool) []byte {
	if comparable {
		b = append(b, bytesFlag)
		b = EncodeBytes(b, v)
	} else {
		b = append(b, compactBytesFlag)
		b = EncodeCompactBytes(b, v)
	}
	return b
}

func valueSizeOfBytes(v []byte) int {
	return valueSizeOfSignedInt(int64(len(v))) + len(v)
}

func sizeBytes(v []byte, comparable bool) int {
	if comparable {
		reallocSize := (len(v)/encGroupSize + 1) * (encGroupSize + 1)
		return 1 + reallocSize
	}
	reallocSize := binary.MaxVarintLen64 + len(v)
	return 1 + reallocSize
}

func encodeSignedInt(b []byte, v int64, comparable bool) []byte {
	if comparable {
		b = append(b, intFlag)
		b = EncodeInt(b, v)
	} else {
		b = append(b, varintFlag)
		b = EncodeVarint(b, v)
	}
	return b
}

func valueSizeOfSignedInt(v int64) int {
	if v < 0 {
		v = 0 - v - 1
	}
	// Flag occupy 1 bit and at lease 1 bit.
	size := 2
	v = v >> 6
	for v > 0 {
		size++
		v = v >> 7
	}
	return size
}

func encodeUnsignedInt(b []byte, v uint64, comparable bool) []byte {
	if comparable {
		b = append(b, uintFlag)
		b = EncodeUint(b, v)
	} else {
		b = append(b, uvarintFlag)
		b = EncodeUvarint(b, v)
	}
	return b
}

func valueSizeOfUnsignedInt(v uint64) int {
	// Flag occupy 1 bit and at lease 1 bit.
	size := 2
	v = v >> 7
	for v > 0 {
		size++
		v = v >> 7
	}
	return size
}

func sizeInt(comparable bool) int {
	if comparable {
		return 9
	}
	return 1 + binary.MaxVarintLen64
}

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
// For Decimal type, datum must set datum's length and frac.
func EncodeKey(sc *stmtctx.StatementContext, b []byte, v ...types.Datum) ([]byte, error) {
	return encode(sc, b, v, true)
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(sc *stmtctx.StatementContext, b []byte, v ...types.Datum) ([]byte, error) {
	return encode(sc, b, v, false)
}

func encodeHashChunkRowIdx(sc *stmtctx.StatementContext, row chunk.Row, tp *types.FieldType, idx int) (flag byte, b []byte, err error) {
	if row.IsNull(idx) {
		flag = NilFlag
		return
	}
	switch tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		flag = varintFlag
		if mysql.HasUnsignedFlag(tp.Flag) {
			if integer := row.GetInt64(idx); integer < 0 {
				flag = uvarintFlag
			}
		}
		b = row.GetRaw(idx)
	case mysql.TypeFloat:
		flag = floatFlag
		f := float64(row.GetFloat32(idx))
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if f == 0 {
			f = 0
		}
		b = (*[unsafe.Sizeof(f)]byte)(unsafe.Pointer(&f))[:]
	case mysql.TypeDouble:
		flag = floatFlag
		f := row.GetFloat64(idx)
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if f == 0 {
			f = 0
		}
		b = (*[unsafe.Sizeof(f)]byte)(unsafe.Pointer(&f))[:]
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		flag = compactBytesFlag
		b = row.GetBytes(idx)
		b = ConvertByCollation(b, tp)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		flag = uintFlag
		t := row.GetTime(idx)
		// Encoding timestamp need to consider timezone.
		// If it's not in UTC, transform to UTC first.
		if t.Type() == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
			err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				return
			}
		}
		var v uint64
		v, err = t.ToPackedUint()
		if err != nil {
			return
		}
		b = (*[unsafe.Sizeof(v)]byte)(unsafe.Pointer(&v))[:]
	case mysql.TypeDuration:
		flag = durationFlag
		// duration may have negative value, so we cannot use String to encode directly.
		b = row.GetRaw(idx)
	case mysql.TypeNewDecimal:
		flag = decimalFlag
		// If hash is true, we only consider the original value of this decimal and ignore it's precision.
		dec := row.GetMyDecimal(idx)
		b, err = dec.ToHashKey()
		if err != nil {
			return
		}
	case mysql.TypeEnum:
		flag = compactBytesFlag
		v := uint64(row.GetEnum(idx).ToNumber())
		str := tp.Elems[v-1]
		b = ConvertByCollation(hack.Slice(str), tp)
	case mysql.TypeSet:
		flag = compactBytesFlag
		v := uint64(row.GetSet(idx).ToNumber())
		str := tp.Elems[v-1]
		b = ConvertByCollation(hack.Slice(str), tp)
	case mysql.TypeBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		flag = uvarintFlag
		v, err1 := types.BinaryLiteral(row.GetBytes(idx)).ToInt(sc)
		terror.Log(errors.Trace(err1))
		b = (*[unsafe.Sizeof(v)]byte)(unsafe.Pointer(&v))[:]
	case mysql.TypeJSON:
		flag = jsonFlag
		b = row.GetBytes(idx)
	default:
		return 0, nil, errors.Errorf("unsupport column type for encode %d", tp.Tp)
	}
	return
}

// HashChunkColumns writes the encoded value of each row's column, which of index `colIdx`, to h.
func HashChunkColumns(sc *stmtctx.StatementContext, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, colIdx int, buf []byte, isNull []bool) (err error) {
	return HashChunkSelected(sc, h, chk, tp, colIdx, buf, isNull, nil, false)
}

// HashChunkSelected writes the encoded value of selected row's column, which of index `colIdx`, to h.
// sel indicates which rows are selected. If it is nil, all rows are selected.
func HashChunkSelected(sc *stmtctx.StatementContext, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, colIdx int, buf []byte,
	isNull, sel []bool, ignoreNull bool) (err error) {
	var b []byte
	column := chk.Column(colIdx)
	rows := chk.NumRows()
	switch tp.Tp {
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
				buf[0] = varintFlag
				if mysql.HasUnsignedFlag(tp.Flag) && v < 0 {
					buf[0] = uvarintFlag
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
				b = (*[sizeFloat64]byte)(unsafe.Pointer(&d))[:]
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeDouble:
		f64s := column.Float64s()
		for i, f := range f64s {
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
				b = (*[sizeFloat64]byte)(unsafe.Pointer(&f))[:]
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				b = column.GetBytes(i)
				b = ConvertByCollation(b, tp)
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
				// Encoding timestamp need to consider timezone.
				// If it's not in UTC, transform to UTC first.
				if t.Type() == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
					err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
					if err != nil {
						return
					}
				}
				var v uint64
				v, err = t.ToPackedUint()
				if err != nil {
					return
				}
				b = (*[sizeUint64]byte)(unsafe.Pointer(&v))[:]
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeDuration:
		for i := 0; i < rows; i++ {
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
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				v := uint64(column.GetEnum(i).ToNumber())
				str := tp.Elems[v-1]
				b = ConvertByCollation(hack.Slice(str), tp)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeSet:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				v := uint64(column.GetSet(i).ToNumber())
				str := tp.Elems[v-1]
				b = ConvertByCollation(hack.Slice(str), tp)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeBit:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
				buf[0] = uvarintFlag
				v, err1 := types.BinaryLiteral(column.GetBytes(i)).ToInt(sc)
				terror.Log(errors.Trace(err1))
				b = (*[sizeUint64]byte)(unsafe.Pointer(&v))[:]
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeJSON:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = jsonFlag
				b = column.GetBytes(i)
			}

			// As the golang doc described, `Hash.Write` never returns an error..
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeNull:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			isNull[i] = !ignoreNull
			buf[0] = NilFlag
			_, _ = h[i].Write(buf)
		}
	default:
		return errors.Errorf("unsupport column type for encode %d", tp.Tp)
	}
	return
}

// HashChunkRow writes the encoded values to w.
// If two rows are logically equal, it will generate the same bytes.
func HashChunkRow(sc *stmtctx.StatementContext, w io.Writer, row chunk.Row, allTypes []*types.FieldType, colIdx []int, buf []byte) (err error) {
	var b []byte
	for _, idx := range colIdx {
		buf[0], b, err = encodeHashChunkRowIdx(sc, row, allTypes[idx], idx)
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
func EqualChunkRow(sc *stmtctx.StatementContext,
	row1 chunk.Row, allTypes1 []*types.FieldType, colIdx1 []int,
	row2 chunk.Row, allTypes2 []*types.FieldType, colIdx2 []int,
) (bool, error) {
	for i := range colIdx1 {
		idx1, idx2 := colIdx1[i], colIdx2[i]
		flag1, b1, err := encodeHashChunkRowIdx(sc, row1, allTypes1[idx1], idx1)
		if err != nil {
			return false, errors.Trace(err)
		}
		flag2, b2, err := encodeHashChunkRowIdx(sc, row2, allTypes2[idx2], idx2)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !(flag1 == flag2 && bytes.Equal(b1, b2)) {
			return false, nil
		}
	}
	return true, nil
}

// Decode decodes values from a byte slice generated with EncodeKey or EncodeValue
// before.
// size is the size of decoded datum slice.
func Decode(b []byte, size int) ([]types.Datum, error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}

	var (
		err    error
		values = make([]types.Datum, 0, size)
	)

	for len(b) > 0 {
		var d types.Datum
		b, d, err = DecodeOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}

		values = append(values, d)
	}

	return values, nil
}

// DecodeRange decodes the range values from a byte slice that generated by EncodeKey.
// It handles some special values like `MinNotNull` and `MaxValueDatum`.
func DecodeRange(b []byte, size int) ([]types.Datum, []byte, error) {
	if len(b) < 1 {
		return nil, b, errors.New("invalid encoded key: length of key is zero")
	}

	var (
		err    error
		values = make([]types.Datum, 0, size)
	)

	for len(b) > 1 {
		var d types.Datum
		b, d, err = DecodeOne(b)
		if err != nil {
			return values, b, errors.Trace(err)
		}
		values = append(values, d)
	}

	if len(b) == 1 {
		switch b[0] {
		case NilFlag:
			values = append(values, types.Datum{})
		case bytesFlag:
			values = append(values, types.MinNotNullDatum())
		// `maxFlag + 1` for PrefixNext
		case maxFlag, maxFlag + 1:
			values = append(values, types.MaxValueDatum())
		default:
			return values, b, errors.Errorf("invalid encoded key flag %v", b[0])
		}
	}
	return values, nil, nil
}

// DecodeOne decodes on datum from a byte slice generated with EncodeKey or EncodeValue.
func DecodeOne(b []byte) (remain []byte, d types.Datum, err error) {
	if len(b) < 1 {
		return nil, d, errors.New("invalid encoded key")
	}
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		var v int64
		b, v, err = DecodeInt(b)
		d.SetInt64(v)
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		d.SetUint64(v)
	case varintFlag:
		var v int64
		b, v, err = DecodeVarint(b)
		d.SetInt64(v)
	case uvarintFlag:
		var v uint64
		b, v, err = DecodeUvarint(b)
		d.SetUint64(v)
	case floatFlag:
		var v float64
		b, v, err = DecodeFloat(b)
		d.SetFloat64(v)
	case bytesFlag:
		var v []byte
		b, v, err = DecodeBytes(b, nil)
		d.SetBytes(v)
	case compactBytesFlag:
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		d.SetBytes(v)
	case decimalFlag:
		var (
			dec             *types.MyDecimal
			precision, frac int
		)
		b, dec, precision, frac, err = DecodeDecimal(b)
		if err == nil {
			d.SetMysqlDecimal(dec)
			d.SetLength(precision)
			d.SetFrac(frac)
		}
	case durationFlag:
		var r int64
		b, r, err = DecodeInt(b)
		if err == nil {
			// use max fsp, let outer to do round manually.
			v := types.Duration{Duration: time.Duration(r), Fsp: types.MaxFsp}
			d.SetMysqlDuration(v)
		}
	case jsonFlag:
		var size int
		size, err = json.PeekBytesAsJSON(b)
		if err != nil {
			return b, d, err
		}
		j := json.BinaryJSON{TypeCode: b[0], Value: b[1:size]}
		d.SetMysqlJSON(j)
		b = b[size:]
	case NilFlag:
	default:
		return b, d, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return b, d, errors.Trace(err)
	}
	return b, d, nil
}

// CutOne cuts the first encoded value from b.
// It will return the first encoded item and the remains as byte slice.
func CutOne(b []byte) (data []byte, remain []byte, err error) {
	l, err := peek(b)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return b[:l], b[l:], nil
}

// CutColumnID cuts the column ID from b.
// It will return the remains as byte slice and column ID
func CutColumnID(b []byte) (remain []byte, n int64, err error) {
	if len(b) < 1 {
		return nil, 0, errors.New("invalid encoded key")
	}
	// skip the flag
	b = b[1:]
	return DecodeVarint(b)
}

// SetRawValues set raw datum values from a row data.
func SetRawValues(data []byte, values []types.Datum) error {
	for i := 0; i < len(values); i++ {
		l, err := peek(data)
		if err != nil {
			return errors.Trace(err)
		}
		values[i].SetRaw(data[:l:l])
		data = data[l:]
	}
	return nil
}

// peek peeks the first encoded value from b and returns its length.
func peek(b []byte) (length int, err error) {
	if len(b) < 1 {
		return 0, errors.New("invalid encoded key")
	}
	flag := b[0]
	length++
	b = b[1:]
	var l int
	switch flag {
	case NilFlag:
	case intFlag, uintFlag, floatFlag, durationFlag:
		// Those types are stored in 8 bytes.
		l = 8
	case bytesFlag:
		l, err = peekBytes(b)
	case compactBytesFlag:
		l, err = peekCompactBytes(b)
	case decimalFlag:
		l, err = types.DecimalPeak(b)
	case varintFlag:
		l, err = peekVarint(b)
	case uvarintFlag:
		l, err = peekUvarint(b)
	case jsonFlag:
		l, err = json.PeekBytesAsJSON(b)
	default:
		return 0, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	length += l
	return
}

func peekBytes(b []byte) (int, error) {
	offset := 0
	for {
		if len(b) < offset+encGroupSize+1 {
			return 0, errors.New("insufficient bytes to decode value")
		}
		// The byte slice is encoded into many groups.
		// For each group, there are 8 bytes for data and 1 byte for marker.
		marker := b[offset+encGroupSize]
		padCount := encMarker - marker
		offset += encGroupSize + 1
		// When padCount is not zero, it means we get the end of the byte slice.
		if padCount != 0 {
			break
		}
	}
	return offset, nil
}

func peekCompactBytes(b []byte) (int, error) {
	// Get length.
	v, n := binary.Varint(b)
	vi := int(v)
	if n < 0 {
		return 0, errors.New("value larger than 64 bits")
	} else if n == 0 {
		return 0, errors.New("insufficient bytes to decode value")
	}
	if len(b) < vi+n {
		return 0, errors.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	return n + vi, nil
}

func peekVarint(b []byte) (int, error) {
	_, n := binary.Varint(b)
	if n < 0 {
		return 0, errors.New("value larger than 64 bits")
	}
	return n, nil
}

func peekUvarint(b []byte) (int, error) {
	_, n := binary.Uvarint(b)
	if n < 0 {
		return 0, errors.New("value larger than 64 bits")
	}
	return n, nil
}

// Decoder is used to decode value to chunk.
type Decoder struct {
	chk      *chunk.Chunk
	timezone *time.Location

	// buf is only used for DecodeBytes to avoid the cost of makeslice.
	buf []byte
}

// NewDecoder creates a Decoder.
func NewDecoder(chk *chunk.Chunk, timezone *time.Location) *Decoder {
	return &Decoder{
		chk:      chk,
		timezone: timezone,
	}
}

// DecodeOne decodes one value to chunk and returns the remained bytes.
func (decoder *Decoder) DecodeOne(b []byte, colIdx int, ft *types.FieldType) (remain []byte, err error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}
	chk := decoder.chk
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		var v int64
		b, v, err = DecodeInt(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		appendIntToChunk(v, chk, colIdx, ft)
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = appendUintToChunk(v, chk, colIdx, ft, decoder.timezone)
	case varintFlag:
		var v int64
		b, v, err = DecodeVarint(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		appendIntToChunk(v, chk, colIdx, ft)
	case uvarintFlag:
		var v uint64
		b, v, err = DecodeUvarint(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = appendUintToChunk(v, chk, colIdx, ft, decoder.timezone)
	case floatFlag:
		var v float64
		b, v, err = DecodeFloat(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		appendFloatToChunk(v, chk, colIdx, ft)
	case bytesFlag:
		b, decoder.buf, err = DecodeBytes(b, decoder.buf)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chk.AppendBytes(colIdx, decoder.buf)
	case compactBytesFlag:
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chk.AppendBytes(colIdx, v)
	case decimalFlag:
		var dec *types.MyDecimal
		var frac int
		b, dec, _, frac, err = DecodeDecimal(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ft.Decimal != types.UnspecifiedLength && frac > ft.Decimal {
			to := new(types.MyDecimal)
			err := dec.Round(to, ft.Decimal, types.ModeHalfEven)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dec = to
		}
		chk.AppendMyDecimal(colIdx, dec)
	case durationFlag:
		var r int64
		b, r, err = DecodeInt(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		v := types.Duration{Duration: time.Duration(r), Fsp: int8(ft.Decimal)}
		chk.AppendDuration(colIdx, v)
	case jsonFlag:
		var size int
		size, err = json.PeekBytesAsJSON(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chk.AppendJSON(colIdx, json.BinaryJSON{TypeCode: b[0], Value: b[1:size]})
		b = b[size:]
	case NilFlag:
		chk.AppendNull(colIdx)
	default:
		return nil, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return b, nil
}

func appendIntToChunk(val int64, chk *chunk.Chunk, colIdx int, ft *types.FieldType) {
	switch ft.Tp {
	case mysql.TypeDuration:
		v := types.Duration{Duration: time.Duration(val), Fsp: int8(ft.Decimal)}
		chk.AppendDuration(colIdx, v)
	default:
		chk.AppendInt64(colIdx, val)
	}
}

func appendUintToChunk(val uint64, chk *chunk.Chunk, colIdx int, ft *types.FieldType, loc *time.Location) error {
	switch ft.Tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t := types.NewTime(types.ZeroCoreTime, ft.Tp, int8(ft.Decimal))
		var err error
		err = t.FromPackedUint(val)
		if err != nil {
			return errors.Trace(err)
		}
		if ft.Tp == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return errors.Trace(err)
			}
		}
		chk.AppendTime(colIdx, t)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.Elems, val)
		if err != nil {
			enum = types.Enum{}
		}
		chk.AppendEnum(colIdx, enum)
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.Elems, val)
		if err != nil {
			return errors.Trace(err)
		}
		chk.AppendSet(colIdx, set)
	case mysql.TypeBit:
		byteSize := (ft.Flen + 7) >> 3
		chk.AppendBytes(colIdx, types.NewBinaryLiteralFromUint(val, byteSize))
	default:
		chk.AppendUint64(colIdx, val)
	}
	return nil
}

func appendFloatToChunk(val float64, chk *chunk.Chunk, colIdx int, ft *types.FieldType) {
	if ft.Tp == mysql.TypeFloat {
		chk.AppendFloat32(colIdx, float32(val))
	} else {
		chk.AppendFloat64(colIdx, val)
	}
}

// HashGroupKey encodes each row of this column and append encoded data into buf.
// Only use in the aggregate executor.
func HashGroupKey(sc *stmtctx.StatementContext, n int, col *chunk.Column, buf [][]byte, ft *types.FieldType) ([][]byte, error) {
	var err error
	switch ft.EvalType() {
	case types.ETInt:
		i64s := col.Int64s()
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = encodeSignedInt(buf[i], i64s[i], false)
			}
		}
	case types.ETReal:
		f64s := col.Float64s()
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], floatFlag)
				buf[i] = EncodeFloat(buf[i], f64s[i])
			}
		}
	case types.ETDecimal:
		ds := col.Decimals()
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], decimalFlag)
				buf[i], err = EncodeDecimal(buf[i], &ds[i], ft.Flen, ft.Decimal)
				if terror.ErrorEqual(err, types.ErrTruncated) {
					err = sc.HandleTruncate(err)
				} else if terror.ErrorEqual(err, types.ErrOverflow) {
					err = sc.HandleOverflow(err, err)
				}
				if err != nil {
					return nil, err
				}
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		ts := col.Times()
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], uintFlag)
				buf[i], err = EncodeMySQLTime(sc, ts[i], mysql.TypeUnspecified, buf[i])
				if err != nil {
					return nil, err
				}
			}
		}
	case types.ETDuration:
		ds := col.GoDurations()
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], durationFlag)
				buf[i] = EncodeInt(buf[i], int64(ds[i]))
			}
		}
	case types.ETJson:
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], jsonFlag)
				j := col.GetJSON(i)
				buf[i] = append(buf[i], j.TypeCode)
				buf[i] = append(buf[i], j.Value...)
			}
		}
	case types.ETString:
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = encodeBytes(buf[i], ConvertByCollation(col.GetBytes(i), ft), false)
			}
		}
	default:
		return nil, errors.New(fmt.Sprintf("invalid eval type %v", ft.EvalType()))
	}
	return buf, nil
}

// ConvertByCollation converts these bytes according to its collation.
func ConvertByCollation(raw []byte, tp *types.FieldType) []byte {
	collator := collate.GetCollator(tp.Collate)
	return collator.Key(string(hack.String(raw)))
}

// ConvertByCollationStr converts this string according to its collation.
func ConvertByCollationStr(str string, tp *types.FieldType) string {
	collator := collate.GetCollator(tp.Collate)
	return string(hack.String(collator.Key(str)))
}
