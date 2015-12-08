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
	stringFlag
	intFlag
	uintFlag
	floatFlag
	decimalFlag
	durationFlag
)

// EncodeKey encodes args to a slice which can be sorted lexicographically later.
// EncodeKey guarantees the encoded slice is in ascending order for comparison.
func EncodeKey(args ...interface{}) ([]byte, error) {
	var b []byte
	for _, arg := range args {
		switch v := arg.(type) {
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
			b = append(b, stringFlag)
			b = EncodeBytes(b, []byte(v))
		case []byte:
			b = append(b, bytesFlag)
			b = EncodeBytes(b, v)
		case mysql.Time:
			b = append(b, stringFlag)
			b = EncodeBytes(b, []byte(v.String()))
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
		case intFlag:
			b, v, err = DecodeInt(b)
		case uintFlag:
			b, v, err = DecodeUint(b)
		case floatFlag:
			b, v, err = DecodeFloat(b)
		case bytesFlag:
			b, v, err = DecodeBytes(b)
		case stringFlag:
			var r []byte
			b, r, err = DecodeBytes(b)
			if err == nil {
				v = string(r)
			}
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
