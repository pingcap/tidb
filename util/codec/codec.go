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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
)

const (
	nilFlag byte = iota
	bytesFlag
	compactBytesFlag
	intFlag
	uintFlag
	floatFlag
	decimalFlag
	durationFlag
)

func encode(b []byte, vals []interface{}, comparable bool) ([]byte, error) {
	for _, val := range vals {
		switch v := val.(type) {
		case bool:
			b = append(b, intFlag)
			if v {
				b = EncodeInt(b, int64(1))
			} else {
				b = EncodeInt(b, int64(0))
			}
		case int:
			b = append(b, intFlag)
			b = EncodeInt(b, int64(v))
		case int8:
			b = append(b, intFlag)
			b = EncodeInt(b, int64(v))
		case int16:
			b = append(b, intFlag)
			b = EncodeInt(b, int64(v))
		case int32:
			b = append(b, intFlag)
			b = EncodeInt(b, int64(v))
		case int64:
			b = append(b, intFlag)
			b = EncodeInt(b, int64(v))
		case uint:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(v))
		case uint8:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(v))
		case uint16:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(v))
		case uint32:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(v))
		case uint64:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(v))
		case float32:
			b = append(b, floatFlag)
			b = EncodeFloat(b, float64(v))
		case float64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, float64(v))
		case string:
			b = encodeBytes(b, []byte(v), comparable)
		case []byte:
			b = encodeBytes(b, v, comparable)
		case mysql.Time:
			b = encodeBytes(b, []byte(v.String()), comparable)
		case mysql.Duration:
			// duration may have negative value, so we cannot use String to encode directly.
			b = append(b, durationFlag)
			b = EncodeInt(b, int64(v.Duration))
		case mysql.Decimal:
			b = append(b, decimalFlag)
			b = EncodeDecimal(b, v)
		case mysql.Hex:
			b = append(b, intFlag)
			b = EncodeInt(b, int64(v.ToNumber()))
		case mysql.Bit:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(v.ToNumber()))
		case mysql.Enum:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(v.ToNumber()))
		case mysql.Set:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(v.ToNumber()))
		case nil:
			b = append(b, nilFlag)
		default:
			return nil, errors.Errorf("unsupport encode type %T", val)
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
func EncodeKey(b []byte, v ...interface{}) ([]byte, error) {
	return encode(b, v, true)
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(b []byte, v ...interface{}) ([]byte, error) {
	return encode(b, v, false)
}

// Decode decodes values from a byte slice generated with EncodeKey or EncodeValue
// before.
func Decode(b []byte) ([]interface{}, error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}

	var (
		flag   byte
		err    error
		v      interface{}
		values = make([]interface{}, 0, 1)
	)

	for len(b) > 0 {
		flag = b[0]
		b = b[1:]
		switch flag {
		case intFlag:
			b, v, err = DecodeInt(b)
		case uintFlag:
			b, v, err = DecodeUint(b)
		case floatFlag:
			b, v, err = DecodeFloat(b)
		case bytesFlag:
			b, v, err = DecodeBytes(b)
		case compactBytesFlag:
			b, v, err = DecodeCompactBytes(b)
		case decimalFlag:
			b, v, err = DecodeDecimal(b)
		case durationFlag:
			var r int64
			b, r, err = DecodeInt(b)
			if err == nil {
				// use max fsp, let outer to do round manually.
				v = mysql.Duration{Duration: time.Duration(r), Fsp: mysql.MaxFsp}
			}
		case nilFlag:
			v = nil
		default:
			return nil, errors.Errorf("invalid encoded key flag %v", flag)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		values = append(values, v)
	}

	return values, nil
}
