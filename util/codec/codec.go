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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
)

const (
	nilFlag byte = iota
	bytesFlag
	negativeNumFlag
	nonNegativeNumFlag
	floatFlag
	decimalFlag
)

// EncodeKey encodes args to a slice which can be sorted lexicographically later.
// EncodeKey guarantees the encoded slice is in ascending order for comparison.
func EncodeKey(args ...interface{}) ([]byte, error) {
	var b []byte
	for _, arg := range args {
		switch v := arg.(type) {
		case bool:
			b = append(b, nonNegativeNumFlag)
			if v {
				b = EncodeInt(b, int64(1))
			} else {
				b = EncodeInt(b, int64(0))
			}
		case int:
			if v >= 0 {
				b = append(b, nonNegativeNumFlag)
			} else {
				b = append(b, negativeNumFlag)
			}
			b = EncodeInt(b, int64(v))
		case int8:
			if v >= 0 {
				b = append(b, nonNegativeNumFlag)
			} else {
				b = append(b, negativeNumFlag)
			}
			b = EncodeInt(b, int64(v))
		case int16:
			if v >= 0 {
				b = append(b, nonNegativeNumFlag)
			} else {
				b = append(b, negativeNumFlag)
			}
			b = EncodeInt(b, int64(v))
		case int32:
			if v >= 0 {
				b = append(b, nonNegativeNumFlag)
			} else {
				b = append(b, negativeNumFlag)
			}
			b = EncodeInt(b, int64(v))
		case int64:
			if v >= 0 {
				b = append(b, nonNegativeNumFlag)
			} else {
				b = append(b, negativeNumFlag)
			}
			b = EncodeInt(b, int64(v))
		case uint:
			b = append(b, nonNegativeNumFlag)
			b = EncodeUint(b, uint64(v))
		case uint8:
			b = append(b, nonNegativeNumFlag)
			b = EncodeUint(b, uint64(v))
		case uint16:
			b = append(b, nonNegativeNumFlag)
			b = EncodeUint(b, uint64(v))
		case uint32:
			b = append(b, nonNegativeNumFlag)
			b = EncodeUint(b, uint64(v))
		case uint64:
			b = append(b, nonNegativeNumFlag)
			b = EncodeUint(b, uint64(v))
		case float32:
			b = append(b, floatFlag)
			b = EncodeFloat(b, float64(v))
		case float64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, float64(v))
		case string:
			b = append(b, bytesFlag)
			b = EncodeBytes(b, []byte(v))
		case []byte:
			b = append(b, bytesFlag)
			b = EncodeBytes(b, v)
		case mysql.Time:
			b = append(b, bytesFlag)
			b = EncodeBytes(b, []byte(v.String()))
		case mysql.Duration:
			// duration may have negative value, so we cannot use String to encode directly.
			val := int64(v.Duration)
			if val >= 0 {
				b = append(b, nonNegativeNumFlag)
			} else {
				b = append(b, negativeNumFlag)
			}
			b = EncodeInt(b, val)
		case mysql.Decimal:
			b = append(b, decimalFlag)
			b = EncodeDecimal(b, v)
		case mysql.Hex:
			val := int64(v.ToNumber())
			if val >= 0 {
				b = append(b, nonNegativeNumFlag)
			} else {
				b = append(b, negativeNumFlag)
			}
			b = EncodeInt(b, val)
		case mysql.Bit:
			b = append(b, nonNegativeNumFlag)
			b = EncodeUint(b, uint64(v.ToNumber()))
		case mysql.Enum:
			b = append(b, nonNegativeNumFlag)
			b = EncodeUint(b, uint64(v.ToNumber()))
		case mysql.Set:
			b = append(b, nonNegativeNumFlag)
			b = EncodeUint(b, uint64(v.ToNumber()))
		case nil:
			b = append(b, nilFlag)
		default:
			return nil, errors.Errorf("unsupport encode type %T", arg)
		}
	}

	return b, nil
}

// DecodeKey decodes values from a byte slice generated with EncodeKey before.
func DecodeKey(b []byte) ([]interface{}, error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}

	var (
		flag   byte
		err    error
		v      interface{}
		values = make([]interface{}, 0)
	)

	for len(b) > 0 {
		flag = b[0]
		b = b[1:]
		switch flag {
		case negativeNumFlag:
			b, v, err = DecodeInt(b)
		case nonNegativeNumFlag:
			b, v, err = DecodeUint(b)
		case floatFlag:
			b, v, err = DecodeFloat(b)
		case bytesFlag:
			b, v, err = DecodeBytes(b)
		case decimalFlag:
			b, v, err = DecodeDecimal(b)
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
