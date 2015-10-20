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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
)

var (
	// InfiniteValue is the greatest than any other encoded value.
	InfiniteValue = []byte{0xFF, 0xFF}
	// NilValue is the smallest than any other encoded value.
	NilValue = []byte{0x00, 0x00}
	// SmallestNoneNilValue is smaller than any other encoded value except nil value.
	SmallestNoneNilValue = []byte{0x00, 0x01}
)

// TODO: use iota + 1 instead?
const (
	formatNilFlag      = 'n'
	formatIntFlag      = 'd'
	formatUintFlag     = 'u'
	formatFloatFlag    = 'f'
	formatStringFlag   = 's'
	formatBytesFlag    = 'b'
	formatDurationFlag = 't'
	formatDecimalFlag  = 'c'
)

var sepKey = []byte{0x00, 0x00}

// EncodeKey encodes args to a slice which can be sorted lexicographically later.
// EncodeKey guarantees the encoded slice is in ascending order for comparison.
// TODO: we may add more test to check its valiadation, especially for null type and multi indices.
func EncodeKey(args ...interface{}) ([]byte, error) {
	var b []byte
	format := make([]byte, 0, len(args))
	for _, arg := range args {
		switch v := arg.(type) {
		case bool:
			if v {
				b = EncodeInt(b, int64(1))
			} else {
				b = EncodeInt(b, int64(0))
			}
			format = append(format, formatIntFlag)
		case int:
			b = EncodeInt(b, int64(v))
			format = append(format, formatIntFlag)
		case int8:
			b = EncodeInt(b, int64(v))
			format = append(format, formatIntFlag)
		case int16:
			b = EncodeInt(b, int64(v))
			format = append(format, formatIntFlag)
		case int32:
			b = EncodeInt(b, int64(v))
			format = append(format, formatIntFlag)
		case int64:
			b = EncodeInt(b, int64(v))
			format = append(format, formatIntFlag)
		case uint:
			b = EncodeUint(b, uint64(v))
			format = append(format, formatUintFlag)
		case uint8:
			b = EncodeUint(b, uint64(v))
			format = append(format, formatUintFlag)
		case uint16:
			b = EncodeUint(b, uint64(v))
			format = append(format, formatUintFlag)
		case uint32:
			b = EncodeUint(b, uint64(v))
			format = append(format, formatUintFlag)
		case uint64:
			b = EncodeUint(b, uint64(v))
			format = append(format, formatUintFlag)
		case float32:
			b = EncodeFloat(b, float64(v))
			format = append(format, formatFloatFlag)
		case float64:
			b = EncodeFloat(b, float64(v))
			format = append(format, formatFloatFlag)
		case string:
			b = EncodeBytes(b, []byte(v))
			format = append(format, formatStringFlag)
		case []byte:
			b = EncodeBytes(b, v)
			format = append(format, formatBytesFlag)
		case mysql.Time:
			b = EncodeBytes(b, []byte(v.String()))
			format = append(format, formatStringFlag)
		case mysql.Duration:
			// duration may have negative value, so we cannot use String to encode directly.
			b = EncodeInt(b, int64(v.Duration))
			format = append(format, formatDurationFlag)
		case mysql.Decimal:
			b = EncodeDecimal(b, v)
			format = append(format, formatDecimalFlag)
		case mysql.Hex:
			b = EncodeInt(b, int64(v.ToNumber()))
			format = append(format, formatIntFlag)
		case mysql.Bit:
			b = EncodeUint(b, uint64(v.ToNumber()))
			format = append(format, formatUintFlag)
		case mysql.Enum:
			b = EncodeUint(b, uint64(v.ToNumber()))
			format = append(format, formatUintFlag)
		case mysql.Set:
			b = EncodeUint(b, uint64(v.ToNumber()))
			format = append(format, formatUintFlag)
		case nil:
			// We will 0x00, 0x00 for nil.
			// The []byte{} will be encoded as 0x00, 0x01.
			// The []byte{0x00} will be encode as 0x00, 0xFF, 0x00, 0x01.
			// And any integer and float encoded values are greater than 0x00, 0x01.
			// So maybe the smallest none null value is []byte{} and we can use it to skip null values.
			b = append(b, sepKey...)
			format = append(format, formatNilFlag)
		default:
			return nil, errors.Errorf("unsupport encode type %T", arg)
		}
	}

	// The comma is the seperator,
	// e.g:		0x00, 0x00
	// We need more tests to check its validation.
	b = append(b, sepKey...)
	b = append(b, format...)
	return b, nil
}

// StripEnd splits a slice b into two substrings separated by sepKey
// and returns a slice byte of the previous substrings.
func StripEnd(b []byte) ([]byte, error) {
	n := bytes.LastIndex(b, sepKey)
	if n == -1 || n+2 >= len(b) {
		// No seperator or no proper format.
		return nil, errors.Errorf("invalid encoded key")
	}

	return b[:n], nil
}

// DecodeKey decodes values from a byte slice generated with EncodeKey before.
func DecodeKey(b []byte) ([]interface{}, error) {
	// At first read the format.
	n := bytes.LastIndex(b, sepKey)
	if n == -1 || n+2 >= len(b) {
		// No seperator or no proper format.
		return nil, errors.Errorf("invalid encoded key")
	}

	format := b[n+2:]
	b = b[0:n]

	v := make([]interface{}, len(format))
	var err error
	for i, flag := range format {
		switch flag {
		case formatIntFlag:
			b, v[i], err = DecodeInt(b)
		case formatUintFlag:
			b, v[i], err = DecodeUint(b)
		case formatFloatFlag:
			b, v[i], err = DecodeFloat(b)
		case formatStringFlag:
			var r []byte
			b, r, err = DecodeBytes(b)
			if err == nil {
				v[i] = string(r)
			}
		case formatBytesFlag:
			b, v[i], err = DecodeBytes(b)
		case formatDurationFlag:
			var r int64
			b, r, err = DecodeInt(b)
			if err == nil {
				// use max fsp, let outer to do round manually.
				v[i] = mysql.Duration{Duration: time.Duration(r), Fsp: mysql.MaxFsp}
			}
		case formatDecimalFlag:
			b, v[i], err = DecodeDecimal(b)
		case formatNilFlag:
			if len(b) < 2 || (b[0] != 0x00 && b[1] != 0x00) {
				return nil, errors.Errorf("malformed encoded nil")
			}
			b, v[i] = b[2:], nil
		default:
			return nil, errors.Errorf("invalid encoded key format %v in %s", flag, format)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return v, nil
}
