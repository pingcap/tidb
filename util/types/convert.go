// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

package types

import (
	"strconv"
	"time"
	"unicode"

	"github.com/juju/errors"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/util/charset"
)

// InvConv returns a failed convertion error.
func InvConv(val interface{}, tp byte) (interface{}, error) {
	return nil, errors.Errorf("cannot convert %v (type %T) to type %s", val, val, TypeStr(tp))
}

func truncateStr(str string, flen int) string {
	if flen != UnspecifiedLength && len(str) > flen {
		str = str[:flen]
	}
	return str
}

// Convert converts the val with type tp.
func Convert(val interface{}, target *FieldType) (v interface{}, err error) { //NTYPE
	tp := target.Tp
	if val == nil {
		return nil, nil
	}
	switch tp { // TODO: implement mysql types convert when "CAST() AS" syntax are supported.
	case mysql.TypeFloat:
		x, err := ToFloat64(val)
		if err != nil {
			return InvConv(val, tp)
		}
		if target.Flen != UnspecifiedLength {
			x, err = TruncateFloat(x, target.Flen, target.Decimal)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return float32(x), nil
	case mysql.TypeDouble:
		x, err := ToFloat64(val)
		if err != nil {
			return InvConv(val, tp)
		}
		if target.Flen != UnspecifiedLength {
			x, err = TruncateFloat(x, target.Flen, target.Decimal)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return float64(x), nil
	case mysql.TypeString:
		x, err := ToString(val)
		if err != nil {
			return InvConv(val, tp)
		}
		// TODO: consider target.Charset/Collate
		x = truncateStr(x, target.Flen)
		if target.Charset == charset.CharsetBin {
			return []byte(x), nil
		}
		return x, nil
	case mysql.TypeBlob:
		x, err := ToString(val)
		if err != nil {
			return InvConv(val, tp)
		}
		x = truncateStr(x, target.Flen)
		if target.Charset == charset.CharsetBin {
			return []byte(x), nil
		}
		return x, nil
	case mysql.TypeDuration:
		fsp := mysql.DefaultFsp
		if target.Decimal != UnspecifiedLength {
			fsp = target.Decimal
		}
		switch x := val.(type) {
		case mysql.Duration:
			return x.RoundFrac(fsp)
		case mysql.Time:
			t, err := x.ConvertToDuration()
			if err != nil {
				return nil, errors.Trace(err)
			}

			return t.RoundFrac(fsp)
		case string:
			return mysql.ParseDuration(x, fsp)
		default:
			return InvConv(val, tp)
		}
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate:
		fsp := mysql.DefaultFsp
		if target.Decimal != UnspecifiedLength {
			fsp = target.Decimal
		}
		switch x := val.(type) {
		case mysql.Time:
			t, err := x.Convert(tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return t.RoundFrac(fsp)
		case mysql.Duration:
			t, err := x.ConvertToTime(tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return t.RoundFrac(fsp)
		case string:
			return mysql.ParseTime(x, tp, fsp)
		default:
			return InvConv(val, tp)
		}
	case mysql.TypeLonglong:
		x, err := ToInt64(val)
		if err != nil {
			return InvConv(val, tp)
		}
		// TODO: We should first convert to uint64 then check unsigned flag.
		if mysql.HasUnsignedFlag(target.Flag) {
			return uint64(x), nil
		}
		return x, nil
	case mysql.TypeNewDecimal:
		x, err := ToDecimal(val)
		if err != nil {
			return InvConv(val, tp)
		}
		if target.Decimal != UnspecifiedLength {
			x = x.Round(int32(target.Decimal))
		}
		// TODO: check Flen
		return x, nil
	case mysql.TypeYear:
		var (
			intVal int64
			err    error
		)
		switch x := val.(type) {
		case string:
			intVal, err = StrToInt(x)
		case mysql.Time:
			return int16(x.Year()), nil
		case mysql.Duration:
			return int16(time.Now().Year()), nil
		default:
			intVal, err = ToInt64(x)
		}
		if err != nil {
			return InvConv(val, tp)
		}
		y, err := mysql.AdjustYear(int(intVal))
		if err != nil {
			return InvConv(val, tp)
		}
		return int16(y), nil
	default:
		panic("should never happen")
	}
}

// StrToInt converts a string to an integer in best effort.
// TODO: handle overflow and add unittest.
func StrToInt(str string) (int64, error) {
	if len(str) == 0 {
		return 0, nil
	}
	negative := false
	i := 0
	if str[i] == '-' {
		negative = true
		i++
	} else if str[i] == '+' {
		i++
	}
	r := int64(0)
	for ; i < len(str); i++ {
		if !unicode.IsDigit(rune(str[i])) {
			break
		}
		r = r*10 + int64(str[i]-'0')
	}
	if negative {
		r = -r
	}
	// TODO: if i < len(str), we should return an error.
	return r, nil
}

// StrToUint converts a string to an unsigned integer in best effort.
// TODO: handle overflow and add unittest.
func StrToUint(str string) (uint64, error) {
	if len(str) == 0 {
		return uint64(0), nil
	}
	i := 0
	if str[i] == '-' {
		// TODO: return an error.
		v, err := StrToInt(str)
		if err != nil {
			return uint64(0), err
		}
		return uint64(v), nil
	} else if str[i] == '+' {
		i++
	}
	r := uint64(0)
	for ; i < len(str); i++ {
		if !unicode.IsDigit(rune(str[i])) {
			break
		}
		r = r*10 + uint64(str[i]-'0')
	}
	// TODO: if i < len(str), we should return an error.
	return r, nil
}

// StrToFloat converts a string to a float64 in best effort.
func StrToFloat(str string) (float64, error) {
	if len(str) == 0 {
		return 0, nil
	}

	// MySQL uses a very loose conversation, e.g, 123.abc -> 123
	// We should do a trade off whether supporting this feature or using a strict mode.
	// Now we use a strict mode.

	i := 0
	if str[0] == '-' || str[0] == '+' {
		// It is like +0xaa, -0xaa, only for following hexadecimal check.
		i++
	}

	// A hexadecimal literal like 0x12 is a string and should be converted to a float too.
	if len(str[i:]) > 2 && str[i] == '0' && (str[i+1] == 'x' || str[i+1] == 'X') {
		n, err := strconv.ParseInt(str, 0, 64)
		if err != nil {
			return 0, err
		}
		return float64(n), nil
	}
	return strconv.ParseFloat(str, 64)
}

// ToInt64 converts a interface to an int64.
func ToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(RoundFloat((float64(v)))), nil
	case float64:
		return int64(RoundFloat(v)), nil
	case string:
		f, err := StrToFloat(v)
		return int64(f), err
	case []byte:
		f, err := StrToFloat(string(v))
		return int64(f), err
	case mysql.Time:
		// 2011-11-10 11:11:11.999999 -> 20111110111112
		return v.ToNumber().Round(0).IntPart(), nil
	case mysql.Duration:
		// 11:11:11.999999 -> 111112
		return v.ToNumber().Round(0).IntPart(), nil
	case mysql.Decimal:
		return v.Round(0).IntPart(), nil
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", value, value)
	}
}

// ToFloat64 converts a interface to a float64.
func ToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case mysql.Time:
		f, _ := v.ToNumber().Float64()
		return f, nil
	case mysql.Duration:
		f, _ := v.ToNumber().Float64()
		return f, nil
	case mysql.Decimal:
		vv, _ := v.Float64()
		return vv, nil
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to float64", value, value)
	}
}

// ToDecimal converts a interface to the Decimal.
func ToDecimal(value interface{}) (mysql.Decimal, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return mysql.ConvertToDecimal(1)
		}

		return mysql.ConvertToDecimal(0)
	case []byte:
		return mysql.ConvertToDecimal(string(v))
	default:
		return mysql.ConvertToDecimal(value)
	}
}

// ToString converts a interface to a string.
func ToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(int64(v), 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(uint64(v), 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(float64(v), 'f', -1, 64), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case mysql.Time:
		return v.String(), nil
	case mysql.Duration:
		return v.String(), nil
	case mysql.Decimal:
		return v.String(), nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", value, value)
	}
}

// ToBool converts a interface to a bool.
// We will use 1 for true, and 0 for false.
func ToBool(value interface{}) (int8, error) {
	isZero := false
	switch v := value.(type) {
	case bool:
		isZero = (v == false)
	case int:
		isZero = (v == 0)
	case int8:
		isZero = (v == 0)
	case int16:
		isZero = (v == 0)
	case int32:
		isZero = (v == 0)
	case int64:
		isZero = (v == 0)
	case uint:
		isZero = (v == 0)
	case uint8:
		isZero = (v == 0)
	case uint16:
		isZero = (v == 0)
	case uint32:
		isZero = (v == 0)
	case uint64:
		isZero = (v == 0)
	case float32:
		isZero = (v == 0)
	case float64:
		isZero = (v == 0)
	case string:
		if len(v) == 0 {
			isZero = true
		} else {
			n, err := StrToInt(v)
			if err != nil {
				return 0, err
			}
			isZero = (n == 0)
		}
	case []byte:
		if len(v) == 0 {
			isZero = true
		} else {
			n, err := StrToInt(string(v))
			if err != nil {
				return 0, err
			}
			isZero = (n == 0)
		}
	case mysql.Time:
		isZero = v.IsZero()
	case mysql.Duration:
		isZero = (v.Duration == 0)
	case mysql.Decimal:
		vv, _ := v.Float64()
		isZero = (vv == 0)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to bool", value, value)
	}

	if isZero {
		return 0, nil
	}

	return 1, nil
}
