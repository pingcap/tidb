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
	"encoding/binary"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
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

// encode will encode a datum and append it to a byte slice. If comparable is true, the encoded bytes can be sorted as it's original order.
// If hash is true, the encoded bytes can be checked equal as it's original value.
func encode(b []byte, vals []types.Datum, comparable bool, hash bool) ([]byte, error) {
	for i, length := 0, len(vals); i < length; i++ {
		switch vals[i].Kind() {
		case types.KindInt64:
			b = encodeSignedInt(b, vals[i].GetInt64(), comparable)
		case types.KindUint64:
			if hash {
				int := vals[i].GetInt64()
				if int < 0 {
					b = encodeUnsignedInt(b, uint64(int), comparable)
				} else {
					b = encodeSignedInt(b, int, comparable)
				}
			} else {
				b = encodeUnsignedInt(b, vals[i].GetUint64(), comparable)
			}
		case types.KindFloat32, types.KindFloat64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, vals[i].GetFloat64())
		case types.KindString, types.KindBytes:
			b = encodeBytes(b, vals[i].GetBytes(), comparable)
		case types.KindMysqlTime:
			b = append(b, uintFlag)
			t := vals[i].GetMysqlTime()
			// Encoding timestamp need to consider timezone.
			// If it's not in UTC, transform to UTC first.
			if t.Type == mysql.TypeTimestamp && t.TimeZone != time.UTC {
				err := t.ConvertTimeZone(t.TimeZone, time.UTC)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			v, err := t.ToPackedUint()
			if err != nil {
				return nil, errors.Trace(err)
			}
			b = EncodeUint(b, v)
		case types.KindMysqlDuration:
			// duration may have negative value, so we cannot use String to encode directly.
			b = append(b, durationFlag)
			b = EncodeInt(b, int64(vals[i].GetMysqlDuration().Duration))
		case types.KindMysqlDecimal:
			b = append(b, decimalFlag)
			if hash {
				// If hash is true, we only consider the original value of this decimal and ignore it's precision.
				dec := vals[i].GetMysqlDecimal()
				precision, frac := dec.PrecisionAndFrac()
				bin, err := dec.ToBin(precision, frac)
				if err != nil {
					return nil, errors.Trace(err)
				}
				b = append(b, bin...)
			} else {
				b = EncodeDecimal(b, vals[i])
			}
		case types.KindMysqlEnum:
			b = encodeUnsignedInt(b, uint64(vals[i].GetMysqlEnum().ToNumber()), comparable)
		case types.KindMysqlSet:
			b = encodeUnsignedInt(b, uint64(vals[i].GetMysqlSet().ToNumber()), comparable)
		case types.KindMysqlBit, types.KindBinaryLiteral:
			// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
			val, err := vals[i].GetBinaryLiteral().ToInt()
			terror.Log(errors.Trace(err))
			b = encodeUnsignedInt(b, val, comparable)
		case types.KindMysqlJSON:
			b = append(b, jsonFlag)
			b = append(b, json.Serialize(vals[i].GetMysqlJSON())...)
		case types.KindNull:
			b = append(b, NilFlag)
		case types.KindMinNotNull:
			b = append(b, bytesFlag)
		case types.KindMaxValue:
			b = append(b, maxFlag)
		default:
			return nil, errors.Errorf("unsupport encode type %d", vals[i].Kind())
		}
	}

	return b, nil
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

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
// For Decimal type, datum must set datum's length and frac.
func EncodeKey(b []byte, v ...types.Datum) ([]byte, error) {
	return encode(b, v, true, false)
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(b []byte, v ...types.Datum) ([]byte, error) {
	return encode(b, v, false, false)
}

// HashValues appends the encoded values to byte slice b, returning the appended
// slice. If two datums are equal, they will generate the same bytes.
func HashValues(b []byte, v ...types.Datum) ([]byte, error) {
	return encode(b, v, false, true)
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
		b, v, err = DecodeBytes(b)
		d.SetBytes(v)
	case compactBytesFlag:
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		d.SetBytes(v)
	case decimalFlag:
		b, d, err = DecodeDecimal(b)
	case durationFlag:
		var r int64
		b, r, err = DecodeInt(b)
		if err == nil {
			// use max fsp, let outer to do round manually.
			v := types.Duration{Duration: time.Duration(r), Fsp: types.MaxFsp}
			d.SetValue(v)
		}
	case jsonFlag:
		var size int
		size, err = json.PeekBytesAsJSON(b)
		if err != nil {
			return b, d, err
		}

		var j json.JSON
		j, err = json.Deserialize(b)
		if err == nil {
			b = b[size:]
			d.SetMysqlJSON(j)
		}
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
		l, err = peekBytes(b, false)
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

func peekBytes(b []byte, reverse bool) (int, error) {
	offset := 0
	for {
		if len(b) < offset+encGroupSize+1 {
			return 0, errors.New("insufficient bytes to decode value")
		}
		// The byte slice is encoded into many groups.
		// For each group, there are 8 bytes for data and 1 byte for marker.
		marker := b[offset+encGroupSize]
		var padCount byte
		if reverse {
			padCount = marker
		} else {
			padCount = encMarker - marker
		}
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
