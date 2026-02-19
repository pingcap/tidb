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
	"encoding/binary"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag           byte = 0
	bytesFlag         byte = 1
	compactBytesFlag  byte = 2
	intFlag           byte = 3
	uintFlag          byte = 4
	floatFlag         byte = 5
	decimalFlag       byte = 6
	durationFlag      byte = 7
	varintFlag        byte = 8
	uvarintFlag       byte = 9
	jsonFlag          byte = 10
	vectorFloat32Flag byte = 20
	maxFlag           byte = 250
)

// IntHandleFlag is only used to encode int handle key.
const IntHandleFlag = intFlag

const (
	sizeUint64  = unsafe.Sizeof(uint64(0))
	sizeUint8   = unsafe.Sizeof(uint8(0))
	sizeUint32  = unsafe.Sizeof(uint32(0))
	sizeFloat64 = unsafe.Sizeof(float64(0))
)

func preRealloc(b []byte, vals []types.Datum, comparable1 bool) []byte {
	var size int
	for i := range vals {
		switch vals[i].Kind() {
		case types.KindInt64, types.KindUint64, types.KindMysqlEnum, types.KindMysqlSet, types.KindMysqlBit, types.KindBinaryLiteral:
			size += sizeInt(comparable1)
		case types.KindString, types.KindBytes:
			size += sizeBytes(vals[i].GetBytes(), comparable1)
		case types.KindMysqlTime, types.KindMysqlDuration, types.KindFloat32, types.KindFloat64:
			size += 9
		case types.KindNull, types.KindMinNotNull, types.KindMaxValue:
			size++
		case types.KindMysqlJSON:
			size += 2 + len(vals[i].GetBytes())
		case types.KindVectorFloat32:
			size += 1 + vals[i].GetVectorFloat32().SerializedSize()
		case types.KindMysqlDecimal:
			size += 1 + types.MyDecimalStructSize
		default:
			return b
		}
	}
	return reallocBytes(b, size)
}

// encode will encode a datum and append it to a byte slice. If comparable1 is true, the encoded bytes can be sorted as it's original order.
// If hash is true, the encoded bytes can be checked equal as it's original value.
func encode(loc *time.Location, b []byte, vals []types.Datum, comparable1 bool) (_ []byte, err error) {
	b = preRealloc(b, vals, comparable1)
	for i, length := 0, len(vals); i < length; i++ {
		switch vals[i].Kind() {
		case types.KindInt64:
			b = encodeSignedInt(b, vals[i].GetInt64(), comparable1)
		case types.KindUint64:
			b = encodeUnsignedInt(b, vals[i].GetUint64(), comparable1)
		case types.KindFloat32, types.KindFloat64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, vals[i].GetFloat64())
		case types.KindString:
			b = encodeString(b, vals[i], comparable1)
		case types.KindBytes:
			b = encodeBytes(b, vals[i].GetBytes(), comparable1)
		case types.KindMysqlTime:
			b = append(b, uintFlag)
			b, err = EncodeMySQLTime(loc, vals[i].GetMysqlTime(), mysql.TypeUnspecified, b)
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
		case types.KindMysqlEnum:
			b = encodeUnsignedInt(b, vals[i].GetMysqlEnum().Value, comparable1)
		case types.KindMysqlSet:
			b = encodeUnsignedInt(b, vals[i].GetMysqlSet().Value, comparable1)
		case types.KindMysqlBit, types.KindBinaryLiteral:
			// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
			var val uint64
			val, err = vals[i].GetBinaryLiteral().ToInt(types.StrictContext)
			terror.Log(errors.Trace(err))
			b = encodeUnsignedInt(b, val, comparable1)
		case types.KindMysqlJSON:
			b = append(b, jsonFlag)
			j := vals[i].GetMysqlJSON()
			b = append(b, j.TypeCode)
			b = append(b, j.Value...)
		case types.KindVectorFloat32:
			// Always do a small deser + ser for sanity check
			b = append(b, vectorFloat32Flag)
			v := vals[i].GetVectorFloat32()
			b = v.SerializeTo(b)
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
func EstimateValueSize(typeCtx types.Context, val types.Datum) (int, error) {
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
		var err error
		l, err = valueSizeOfDecimal(val.GetMysqlDecimal(), val.Length(), val.Frac())
		if err != nil {
			return 0, err
		}
		l = l + 1
	case types.KindMysqlEnum:
		l = valueSizeOfUnsignedInt(val.GetMysqlEnum().Value)
	case types.KindMysqlSet:
		l = valueSizeOfUnsignedInt(val.GetMysqlSet().Value)
	case types.KindMysqlBit, types.KindBinaryLiteral:
		val, err := val.GetBinaryLiteral().ToInt(typeCtx)
		terror.Log(errors.Trace(err))
		l = valueSizeOfUnsignedInt(val)
	case types.KindMysqlJSON:
		l = 2 + len(val.GetMysqlJSON().Value)
	case types.KindVectorFloat32:
		v := val.GetVectorFloat32()
		l = 1 + v.SerializedSize()
	case types.KindNull, types.KindMinNotNull, types.KindMaxValue:
		l = 1
	default:
		return l, errors.Errorf("unsupported encode type %d", val.Kind())
	}
	return l, nil
}

// EncodeMySQLTime encodes datum of `KindMysqlTime` to []byte.
func EncodeMySQLTime(loc *time.Location, t types.Time, tp byte, b []byte) (_ []byte, err error) {
	// Encoding timestamp need to consider timezone. If it's not in UTC, transform to UTC first.
	// This is compatible with `PBToExpr > convertTime`, and coprocessor assumes the passed timestamp is in UTC as well.
	if tp == mysql.TypeUnspecified {
		tp = t.Type()
	}
	if tp == mysql.TypeTimestamp && loc != time.UTC {
		err = t.ConvertTimeZone(loc, time.UTC)
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

func encodeString(b []byte, val types.Datum, comparable1 bool) []byte {
	if collate.NewCollationEnabled() && comparable1 {
		return encodeBytes(b, collate.GetCollator(val.Collation()).ImmutableKey(val.GetString()), true)
	}
	return encodeBytes(b, val.GetBytes(), comparable1)
}

func encodeBytes(b []byte, v []byte, comparable1 bool) []byte {
	if comparable1 {
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

func sizeBytes(v []byte, comparable1 bool) int {
	if comparable1 {
		reallocSize := (len(v)/encGroupSize + 1) * (encGroupSize + 1)
		return 1 + reallocSize
	}
	reallocSize := binary.MaxVarintLen64 + len(v)
	return 1 + reallocSize
}

func encodeSignedInt(b []byte, v int64, comparable1 bool) []byte {
	if comparable1 {
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
	// flag occupy 1 bit and at lease 1 bit.
	size := 2
	v = v >> 6
	for v > 0 {
		size++
		v = v >> 7
	}
	return size
}

func encodeUnsignedInt(b []byte, v uint64, comparable1 bool) []byte {
	if comparable1 {
		b = append(b, uintFlag)
		b = EncodeUint(b, v)
	} else {
		b = append(b, uvarintFlag)
		b = EncodeUvarint(b, v)
	}
	return b
}

func valueSizeOfUnsignedInt(v uint64) int {
	// flag occupy 1 bit and at lease 1 bit.
	size := 2
	v = v >> 7
	for v > 0 {
		size++
		v = v >> 7
	}
	return size
}

func sizeInt(comparable1 bool) int {
	if comparable1 {
		return 9
	}
	return 1 + binary.MaxVarintLen64
}

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
// For decimal type, datum must set datum's length and frac.
func EncodeKey(loc *time.Location, b []byte, v ...types.Datum) ([]byte, error) {
	return encode(loc, b, v, true)
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(loc *time.Location, b []byte, v ...types.Datum) ([]byte, error) {
	return encode(loc, b, v, false)
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
		if ft.GetDecimal() != types.UnspecifiedLength && frac > ft.GetDecimal() {
			to := new(types.MyDecimal)
			err := dec.Round(to, ft.GetDecimal(), types.ModeHalfUp)
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
		v := types.Duration{Duration: time.Duration(r), Fsp: ft.GetDecimal()}
		chk.AppendDuration(colIdx, v)
	case jsonFlag:
		var size int
		size, err = types.PeekBytesAsJSON(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chk.AppendJSON(colIdx, types.BinaryJSON{TypeCode: b[0], Value: b[1:size]})
		b = b[size:]
	case vectorFloat32Flag:
		v, remaining, err := types.ZeroCopyDeserializeVectorFloat32(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chk.AppendVectorFloat32(colIdx, v)
		b = remaining
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
	switch ft.GetType() {
	case mysql.TypeDuration:
		v := types.Duration{Duration: time.Duration(val), Fsp: ft.GetDecimal()}
		chk.AppendDuration(colIdx, v)
	default:
		chk.AppendInt64(colIdx, val)
	}
}

func appendUintToChunk(val uint64, chk *chunk.Chunk, colIdx int, ft *types.FieldType, loc *time.Location) error {
	switch ft.GetType() {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t := types.NewTime(types.ZeroCoreTime, ft.GetType(), ft.GetDecimal())
		var err error
		err = t.FromPackedUint(val)
		if err != nil {
			return errors.Trace(err)
		}
		if ft.GetType() == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return errors.Trace(err)
			}
		}
		chk.AppendTime(colIdx, t)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.GetElems(), val)
		if err != nil {
			enum = types.Enum{}
		}
		chk.AppendEnum(colIdx, enum)
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.GetElems(), val)
		if err != nil {
			return errors.Trace(err)
		}
		chk.AppendSet(colIdx, set)
	case mysql.TypeBit:
		byteSize := (ft.GetFlen() + 7) >> 3
		chk.AppendBytes(colIdx, types.NewBinaryLiteralFromUint(val, byteSize))
	default:
		chk.AppendUint64(colIdx, val)
	}
	return nil
}

func appendFloatToChunk(val float64, chk *chunk.Chunk, colIdx int, ft *types.FieldType) {
	if ft.GetType() == mysql.TypeFloat {
		chk.AppendFloat32(colIdx, float32(val))
	} else {
		chk.AppendFloat64(colIdx, val)
	}
}

// HashGroupKey encodes each row of this column and append encoded data into buf.
// Only use in the aggregate executor.
func HashGroupKey(loc *time.Location, n int, col *chunk.Column, buf [][]byte, ft *types.FieldType) ([][]byte, error) {
	var err error
	switch ft.EvalType() {
	case types.ETInt:
		i64s := col.Int64s()
		for i := range n {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = encodeSignedInt(buf[i], i64s[i], false)
			}
		}
	case types.ETReal:
		f64s := col.Float64s()
		for i := range n {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], floatFlag)
				buf[i] = EncodeFloat(buf[i], f64s[i])
			}
		}
	case types.ETDecimal:
		ds := col.Decimals()
		for i := range n {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], decimalFlag)
				buf[i], err = EncodeDecimal(buf[i], &ds[i], ft.GetFlen(), ft.GetDecimal())
				if err != nil {
					return buf, err
				}
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		ts := col.Times()
		for i := range n {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], uintFlag)
				buf[i], err = EncodeMySQLTime(loc, ts[i], mysql.TypeUnspecified, buf[i])
				if err != nil {
					return buf, err
				}
			}
		}
	case types.ETDuration:
		ds := col.GoDurations()
		for i := range n {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], durationFlag)
				buf[i] = EncodeInt(buf[i], int64(ds[i]))
			}
		}
	case types.ETJson:
		for i := range n {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], jsonFlag)
				buf[i] = col.GetJSON(i).HashValue(buf[i])
			}
		}
	case types.ETString:
		collator := collate.GetCollator(ft.GetCollate())
		for i := range n {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = encodeBytes(buf[i], collator.ImmutableKey(string(hack.String(col.GetBytes(i)))), false)
			}
		}
	case types.ETVectorFloat32:
		for i := range n {
			if col.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = col.GetVectorFloat32(i).SerializeTo(buf[i])
			}
		}
	default:
		return nil, errors.Errorf("unsupported type %s during evaluation", ft.EvalType())
	}
	return buf, nil
}

// ConvertByCollation converts these bytes according to its collation.
func ConvertByCollation(raw []byte, tp *types.FieldType) []byte {
	collator := collate.GetCollator(tp.GetCollate())
	return collator.Key(string(hack.String(raw)))
}

// ConvertByCollationStr converts this string according to its collation.
func ConvertByCollationStr(str string, tp *types.FieldType) string {
	collator := collate.GetCollator(tp.GetCollate())
	return string(hack.String(collator.Key(str)))
}

// Hash64 is for datum hash64 calculation.
func Hash64(h base.Hasher, d *types.Datum) {
	// let h.cache to receive datum hash value, which is potentially expendable.
	// clean the cache before using it.
	b := h.Cache()[:0]
	b = HashCode(b, *d)
	h.HashBytes(b)
	h.SetCache(b)
}

func init() {
	types.Hash64ForDatum = Hash64
}

// HashCode encodes a Datum into a unique byte slice.
// It is mostly the same as EncodeValue, but it doesn't contain truncation or verification logic in order to make the encoding lossless.
func HashCode(b []byte, d types.Datum) []byte {
	switch d.Kind() {
	case types.KindInt64:
		b = encodeSignedInt(b, d.GetInt64(), false)
	case types.KindUint64:
		b = encodeUnsignedInt(b, d.GetUint64(), false)
	case types.KindFloat32, types.KindFloat64:
		b = append(b, floatFlag)
		b = EncodeFloat(b, d.GetFloat64())
	case types.KindString:
		b = encodeString(b, d, false)
	case types.KindBytes:
		b = encodeBytes(b, d.GetBytes(), false)
	case types.KindMysqlTime:
		b = append(b, uintFlag)
		t := d.GetMysqlTime().CoreTime()
		b = encodeUnsignedInt(b, uint64(t), true)
	case types.KindMysqlDuration:
		// duration may have negative value, so we cannot use String to encode directly.
		b = append(b, durationFlag)
		b = EncodeInt(b, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		b = append(b, decimalFlag)
		decStr := d.GetMysqlDecimal().ToString()
		b = encodeBytes(b, decStr, false)
	case types.KindMysqlEnum:
		b = encodeUnsignedInt(b, d.GetMysqlEnum().Value, false)
	case types.KindMysqlSet:
		b = encodeUnsignedInt(b, d.GetMysqlSet().Value, false)
	case types.KindMysqlBit, types.KindBinaryLiteral:
		val := d.GetBinaryLiteral()
		b = encodeBytes(b, val, false)
	case types.KindMysqlJSON:
		b = append(b, jsonFlag)
		j := d.GetMysqlJSON()
		b = append(b, j.TypeCode)
		b = append(b, j.Value...)
	case types.KindVectorFloat32:
		b = append(b, vectorFloat32Flag)
		v := d.GetVectorFloat32()
		b = v.SerializeTo(b)
	case types.KindNull:
		b = append(b, NilFlag)
	case types.KindMinNotNull:
		b = append(b, bytesFlag)
	case types.KindMaxValue:
		b = append(b, maxFlag)
	default:
		logutil.BgLogger().Warn("trying to calculate HashCode of an unexpected type of Datum",
			zap.Uint8("Datum Kind", d.Kind()),
			zap.Stack("stack"))
	}
	return b
}
