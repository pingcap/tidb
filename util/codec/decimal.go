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
	"math/big"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
)

const (
	negativeSign int64 = 8
	zeroSign     int64 = 16
	positiveSign int64 = 24
)

func codecSign(value int64) int64 {
	if value < 0 {
		return negativeSign
	}

	return positiveSign
}

// EncodeDecimal encodes a decimal d into a byte slice which can be sorted lexicographically later.
// EncodeDecimal guarantees that the encoded value is in ascending order for comparison.
// Decimal encoding:
// Byte -> value sign
// EncodeInt -> exp value
// EncodeBytes -> abs value bytes
func EncodeDecimal(b []byte, d mysql.Decimal) []byte {
	if d.Equals(mysql.ZeroDecimal) {
		return append(b, byte(zeroSign))
	}

	v := d.BigIntValue()
	valSign := codecSign(int64(v.Sign()))

	absVal := new(big.Int)
	absVal.Abs(v)

	// Get exp and value, format is "value":"exp".
	// like "12.34" -> "0.1234":"2".
	// like "-0.01234" -> "-0.1234":"-1".
	exp := int64(0)
	div := big.NewInt(10)
	mod := big.NewInt(0)
	value := []byte{}
	for ; ; exp++ {
		if absVal.Sign() == 0 {
			break
		}

		mod.Mod(absVal, div)
		absVal.Div(absVal, div)
		value = append([]byte(strconv.Itoa(int(mod.Int64()))), value...)
	}

	value = bytes.TrimRight(value, "0")

	expVal := exp + int64(d.Exponent())
	if valSign == negativeSign {
		expVal = -expVal
	}

	b = append(b, byte(valSign))
	b = EncodeInt(b, expVal)
	if valSign == negativeSign {
		b = EncodeBytesDesc(b, value)
	} else {
		b = EncodeBytes(b, value)
	}
	return b
}

// DecodeDecimal decodes bytes to decimal.
// DecodeFloat decodes a float from a byte slice
// Decimal decoding:
// Byte -> value sign
// Byte -> exp sign
// DecodeInt -> exp value
// DecodeBytes -> abs value bytes
func DecodeDecimal(b []byte) ([]byte, mysql.Decimal, error) {
	var (
		r   = b
		d   mysql.Decimal
		err error
	)

	// Decode value sign.
	valSign := int64(r[0])
	r = r[1:]
	if valSign == zeroSign {
		d, err = mysql.ParseDecimal("0")
		return r, d, errors.Trace(err)
	}

	// Decode exp value.
	expVal := int64(0)
	r, expVal, err = DecodeInt(r)
	if err != nil {
		return r, d, errors.Trace(err)
	}

	// Decode abs value bytes.
	value := []byte{}
	if valSign == negativeSign {
		expVal = -expVal
		r, value, err = DecodeBytesDesc(r)
	} else {
		r, value, err = DecodeBytes(r)
	}
	if err != nil {
		return r, d, errors.Trace(err)
	}

	// Set decimal sign and point to value.
	if valSign == negativeSign {
		value = append([]byte("-0."), value...)
	} else {
		value = append([]byte("0."), value...)
	}

	numberDecimal, err := mysql.ParseDecimal(string(value))
	if err != nil {
		return r, d, errors.Trace(err)
	}

	expDecimal := mysql.NewDecimalFromInt(1, int32(expVal))
	d = numberDecimal.Mul(expDecimal)
	return r, d, nil
}
