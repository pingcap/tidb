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
	"github.com/pingcap/tidb/util/types"
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
	maxFlag          byte = 250
)

func encode(b []byte, vals []types.Datum, comparable bool) ([]byte, error) {
	for _, val := range vals {
		switch val.Kind() {
		case types.KindInt64:
			b = append(b, intFlag)
			b = EncodeInt(b, val.GetInt64())
		case types.KindUint64:
			b = append(b, uintFlag)
			b = EncodeUint(b, val.GetUint64())
		case types.KindFloat32, types.KindFloat64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, val.GetFloat64())
		case types.KindString, types.KindBytes:
			b = encodeBytes(b, val.GetBytes(), comparable)
		case types.KindMysqlTime:
			b = encodeBytes(b, []byte(val.GetMysqlTime().String()), comparable)
		case types.KindMysqlDuration:
			// duration may have negative value, so we cannot use String to encode directly.
			b = append(b, durationFlag)
			b = EncodeInt(b, int64(val.GetMysqlDuration().Duration))
		case types.KindMysqlDecimal:
			b = append(b, decimalFlag)
			b = EncodeDecimal(b, val.GetMysqlDecimal())
		case types.KindMysqlHex:
			b = append(b, intFlag)
			b = EncodeInt(b, int64(val.GetMysqlHex().ToNumber()))
		case types.KindMysqlBit:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(val.GetMysqlBit().ToNumber()))
		case types.KindMysqlEnum:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(val.GetMysqlEnum().ToNumber()))
		case types.KindMysqlSet:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(val.GetMysqlSet().ToNumber()))
		case types.KindNull:
			b = append(b, NilFlag)
		case types.KindMinNotNull:
			b = append(b, bytesFlag)
		case types.KindMaxValue:
			b = append(b, maxFlag)
		default:
			return nil, errors.Errorf("unsupport encode type %d", val.Kind())
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

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
func EncodeKey(b []byte, v ...types.Datum) ([]byte, error) {
	return encode(b, v, true)
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(b []byte, v ...types.Datum) ([]byte, error) {
	return encode(b, v, false)
}

// Decode decodes values from a byte slice generated with EncodeKey or EncodeValue
// before.
func Decode(b []byte) ([]types.Datum, error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}

	var (
		err    error
		values = make([]types.Datum, 0, 1)
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
		var v mysql.Decimal
		b, v, err = DecodeDecimal(b)
		d.SetValue(v)
	case durationFlag:
		var r int64
		b, r, err = DecodeInt(b)
		if err == nil {
			// use max fsp, let outer to do round manually.
			v := mysql.Duration{Duration: time.Duration(r), Fsp: mysql.MaxFsp}
			d.SetValue(v)
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
func CutOne(b []byte) (first []byte, remain []byte, err error) {
	if len(b) < 1 {
		return nil, nil, errors.New("invalid encoded key")
	}
	flag := b[0]
	first = b[:1]
	b = b[1:]
	var d []byte
	switch flag {
	case NilFlag:
		remain = b
	case intFlag, uintFlag, floatFlag, durationFlag:
		d = b[:8]
		remain = b[8:]
	case bytesFlag:
		d, remain, err = cutBytes(b, false)
	case compactBytesFlag:
		d, remain, err = cutCompactBytes(b)
	case decimalFlag:
		d, remain, err = cutDecimal(b)
	default:
		return b, d, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return b, d, errors.Trace(err)
	}
	// Append encoded data after type flag.
	first = append(first, d...)
	return
}

func cutBytes(b []byte, reverse bool) (data []byte, remain []byte, err error) {
	offset := 0
	for {
		if len(b) < offset+encGroupSize+1 {
			return nil, nil, errors.New("insufficient bytes to decode value")
		}
		marker := b[offset+encGroupSize]
		var padCount byte
		if reverse {
			padCount = marker
		} else {
			padCount = encMarker - marker
		}
		offset += encGroupSize + 1
		if padCount != 0 {
			break
		}
	}
	data = b[:offset]
	remain = b[offset:]
	return
}

func cutCompactBytes(b []byte) (data []byte, remain []byte, err error) {
	v, n := binary.Varint(b)
	ni := int64(n)
	if n < 0 {
		return nil, nil, errors.New("value larger than 64 bits")
	} else if n == 0 {
		return nil, nil, errors.New("insufficient bytes to decode value")
	}
	if int64(len(b)) < ni+v {
		return nil, nil, errors.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	return b[:ni+v], b[ni+v:], nil
}

func cutDecimal(b []byte) (data []byte, remain []byte, err error) {
	if len(b) < 9 {
		return nil, nil, errors.New("cutDecimal error: Invalid decimal data")
	}
	data = append(data, b[:9]...) // value sign
	b = b[9:]
	desc := int64(data[0]) == negativeSign
	var d []byte
	d, remain, err = cutBytes(remain, desc)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	data = append(data, d...)
	return
}
