// Copyright 2021 PingCAP, Inc.
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
	"encoding/binary"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/collate"
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
// loc can be nil and only used in when the corresponding type is `mysql.TypeTimestamp`.
func DecodeRange(b []byte, size int, idxColumnTypes []byte, loc *time.Location) ([]types.Datum, []byte, error) {
	if len(b) < 1 {
		return nil, b, errors.New("invalid encoded key: length of key is zero")
	}

	var (
		err    error
		values = make([]types.Datum, 0, size)
	)

	i := 0
	for len(b) > 1 {
		var d types.Datum
		if idxColumnTypes == nil {
			b, d, err = DecodeOne(b)
		} else {
			if i >= len(idxColumnTypes) {
				return values, b, errors.New("invalid length of index's columns")
			}
			if idxColumnTypes[i] == mysql.TypeDatetime || idxColumnTypes[i] == mysql.TypeTimestamp || idxColumnTypes[i] == mysql.TypeDate {
				b, d, err = DecodeAsDateTime(b, idxColumnTypes[i], loc)
			} else {
				b, d, err = DecodeOne(b)
			}
		}
		if err != nil {
			return values, b, errors.Trace(err)
		}
		values = append(values, d)
		i++
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

// DecodeAsDateTime decodes on datum from []byte of `KindMysqlTime`.
func DecodeAsDateTime(b []byte, tp byte, loc *time.Location) (remain []byte, d types.Datum, err error) {
	if len(b) < 1 {
		return nil, d, errors.New("invalid encoded key")
	}
	flag := b[0]
	b = b[1:]
	switch flag {
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		if err != nil {
			return b, d, err
		}
		t := types.NewTime(types.ZeroCoreTime, tp, 0)
		err = t.FromPackedUint(v)
		if err == nil {
			if tp == mysql.TypeTimestamp && !t.IsZero() && loc != nil {
				err = t.ConvertTimeZone(time.UTC, loc)
				if err != nil {
					return b, d, err
				}
			}
			d.SetMysqlTime(t)
		}
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
	originLength := len(b)
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
	if length > originLength {
		return 0, errors.Errorf("invalid encoded key, "+
			"expected length: %d, actual length: %d", length, originLength)
	}
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
