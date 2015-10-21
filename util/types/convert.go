// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
)

// InvConv returns a failed convertion error.
func invConv(val interface{}, tp byte) (interface{}, error) {
	return nil, errors.Errorf("cannot convert %v (type %T) to type %s", val, val, TypeStr(tp))
}

func truncateStr(str string, flen int) string {
	if flen != UnspecifiedLength && len(str) > flen {
		str = str[:flen]
	}
	return str
}

var unsignedUpperBound = map[byte]uint64{
	mysql.TypeTiny:     math.MaxUint8,
	mysql.TypeShort:    math.MaxUint16,
	mysql.TypeInt24:    mysql.MaxUint24,
	mysql.TypeLong:     math.MaxUint32,
	mysql.TypeLonglong: math.MaxUint64,
	mysql.TypeBit:      math.MaxUint64,
}

var signedUpperBound = map[byte]int64{
	mysql.TypeTiny:     math.MaxInt8,
	mysql.TypeShort:    math.MaxInt16,
	mysql.TypeInt24:    mysql.MaxInt24,
	mysql.TypeLong:     math.MaxInt32,
	mysql.TypeLonglong: math.MaxInt64,
}

var signedLowerBound = map[byte]int64{
	mysql.TypeTiny:     math.MinInt8,
	mysql.TypeShort:    math.MinInt16,
	mysql.TypeInt24:    mysql.MinInt24,
	mysql.TypeLong:     math.MinInt32,
	mysql.TypeLonglong: math.MinInt64,
}

func convertFloatToInt(val float64, lowerBound, upperBound int64, tp byte) (converted int64, err error) {
	val = RoundFloat(val)
	if val < float64(lowerBound) {
		converted = lowerBound
		err = overflow(val, tp)
		return
	}
	if val > float64(upperBound) {
		converted = upperBound
		err = overflow(val, tp)
		return
	}
	converted = int64(val)
	return
}

func convertIntToInt(val, lowerBound, upperBound int64, tp byte) (converted int64, err error) {
	if val < lowerBound {
		converted = lowerBound
		err = overflow(val, tp)
		return
	}
	if val > upperBound {
		converted = upperBound
		err = overflow(val, tp)
		return
	}
	converted = val
	return
}

func convertToInt(val interface{}, target *FieldType) (converted int64, err error) {
	tp := target.Tp
	lowerBound := signedLowerBound[tp]
	upperBound := signedUpperBound[tp]
	switch v := val.(type) {
	case uint64:
		if v > uint64(upperBound) {
			converted = upperBound
			err = overflow(val, tp)
			return
		}
		converted = int64(v)
		return
	case int:
		return convertIntToInt(int64(v), lowerBound, upperBound, tp)
	case int64:
		return convertIntToInt(int64(v), lowerBound, upperBound, tp)
	case float32:
		return convertFloatToInt(float64(v), lowerBound, upperBound, tp)
	case float64:
		return convertFloatToInt(float64(v), lowerBound, upperBound, tp)
	case string:
		fval, err := StrToFloat(v)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return convertFloatToInt(fval, lowerBound, upperBound, tp)
	case mysql.Decimal:
		fval, _ := v.Float64()
		return convertFloatToInt(fval, lowerBound, upperBound, tp)
	case mysql.Hex:
		return convertFloatToInt(v.ToNumber(), lowerBound, upperBound, tp)
	case mysql.Bit:
		return convertFloatToInt(v.ToNumber(), lowerBound, upperBound, tp)
	case mysql.Enum:
		return convertFloatToInt(v.ToNumber(), lowerBound, upperBound, tp)
	case mysql.Set:
		return convertFloatToInt(v.ToNumber(), lowerBound, upperBound, tp)
	}
	return 0, typeError(val, target)
}

func convertIntToUint(val int64, upperBound uint64, tp byte) (converted uint64, err error) {
	if val < 0 {
		converted = 0
		err = overflow(val, tp)
		return
	}
	if uint64(val) > upperBound {
		converted = upperBound
		err = overflow(val, tp)
		return
	}
	converted = uint64(val)
	return
}

func convertFloatToUint(val float64, upperBound uint64, tp byte) (converted uint64, err error) {
	val = RoundFloat(val)
	if val < 0 {
		converted = 0
		err = overflow(val, tp)
		return
	}
	if val > float64(upperBound) {
		converted = upperBound
		err = overflow(val, tp)
		return
	}
	converted = uint64(val)
	return
}

func convertToUint(val interface{}, target *FieldType) (converted uint64, err error) {
	tp := target.Tp
	upperBound := unsignedUpperBound[tp]
	switch v := val.(type) {
	case uint64:
		if v > upperBound {
			converted = upperBound
			err = overflow(val, tp)
			return
		}
		converted = v
		return
	case int:
		return convertIntToUint(int64(v), upperBound, tp)
	case int64:
		return convertIntToUint(int64(v), upperBound, tp)
	case float32:
		return convertFloatToUint(float64(v), upperBound, tp)
	case float64:
		return convertFloatToUint(float64(v), upperBound, tp)
	case string:
		fval, err := StrToFloat(v)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return convertFloatToUint(float64(fval), upperBound, tp)
	case mysql.Decimal:
		fval, _ := v.Float64()
		return convertFloatToUint(fval, upperBound, tp)
	case mysql.Hex:
		return convertFloatToUint(v.ToNumber(), upperBound, tp)
	case mysql.Bit:
		return convertFloatToUint(v.ToNumber(), upperBound, tp)
	case mysql.Enum:
		return convertFloatToUint(v.ToNumber(), upperBound, tp)
	case mysql.Set:
		return convertFloatToUint(v.ToNumber(), upperBound, tp)
	}
	return 0, typeError(val, target)
}

// typeError returns error for invalid value type.
func typeError(v interface{}, target *FieldType) error {
	return errors.Errorf("cannot use %v (type %T) in assignment to, or comparison with, column type %s)",
		v, v, target.String())
}

// Cast casts val to certain types and does not return error.
func Cast(val interface{}, target *FieldType) (v interface{}) {
	tp := target.Tp
	switch tp {
	case mysql.TypeString:
		x, _ := ToString(val)
		// TODO: consider target.Charset/Collate
		x = truncateStr(x, target.Flen)
		if target.Charset == charset.CharsetBin {
			return []byte(x)
		}
		return x
	case mysql.TypeDuration:
		var dur mysql.Duration
		fsp := mysql.DefaultFsp
		if target.Decimal != UnspecifiedLength {
			fsp = target.Decimal
		}
		switch x := val.(type) {
		case mysql.Duration:
			dur, _ = x.RoundFrac(fsp)
		case mysql.Time:
			dur, _ = x.ConvertToDuration()
			dur, _ = dur.RoundFrac(fsp)
		case string:
			dur, _ = mysql.ParseDuration(x, fsp)
		case *DataItem:
			return Cast(x.Data, target)
		}
		return dur
	case mysql.TypeDatetime, mysql.TypeDate:
		fsp := mysql.DefaultFsp
		if target.Decimal != UnspecifiedLength {
			fsp = target.Decimal
		}
		var t mysql.Time
		t.Type = tp
		switch x := val.(type) {
		case mysql.Time:
			t, _ = x.Convert(tp)
			t, _ = t.RoundFrac(fsp)
		case mysql.Duration:
			t, _ = x.ConvertToTime(tp)
			t, _ = t.RoundFrac(fsp)
		case string:
			t, _ = mysql.ParseTime(x, tp, fsp)
		case int64:
			t, _ = mysql.ParseTimeFromNum(x, tp, fsp)
		case *DataItem:
			return Cast(x.Data, target)
		}
		return t
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(target.Flag) {
			v, _ = ToUint64(val)
		} else {
			v, _ = ToInt64(val)
		}
		return
	case mysql.TypeNewDecimal:
		x, _ := ToDecimal(val)
		if target.Decimal != UnspecifiedLength {
			x = x.Round(int32(target.Decimal))
		}
		// TODO: check Flen
		return x
	default:
		panic("should never happen")
	}
}

// Convert converts the val with type tp.
func Convert(val interface{}, target *FieldType) (v interface{}, err error) {
	tp := target.Tp
	if val == nil {
		return nil, nil
	}
	vdi, ok := val.(*DataItem)
	if ok {
		return Convert(vdi.Data, target)
	}
	switch tp { // TODO: implement mysql types convert when "CAST() AS" syntax are supported.
	case mysql.TypeFloat:
		x, err := ToFloat64(val)
		if err != nil {
			return invConv(val, tp)
		}
		// For float and following double type, we will only truncate it for float(M, D) format.
		// If no D is set, we will handle it like origin float whether M is set or not.
		if target.Flen != UnspecifiedLength && target.Decimal != UnspecifiedLength {
			x, err = TruncateFloat(x, target.Flen, target.Decimal)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return float32(x), nil
	case mysql.TypeDouble:
		x, err := ToFloat64(val)
		if err != nil {
			return invConv(val, tp)
		}
		if target.Flen != UnspecifiedLength && target.Decimal != UnspecifiedLength {
			x, err = TruncateFloat(x, target.Flen, target.Decimal)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return float64(x), nil
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		x, err := ToString(val)
		if err != nil {
			return invConv(val, tp)
		}
		// TODO: consider target.Charset/Collate
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
			return invConv(val, tp)
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
		case int64:
			return mysql.ParseTimeFromNum(x, tp, fsp)
		default:
			return invConv(val, tp)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		unsigned := mysql.HasUnsignedFlag(target.Flag)
		if unsigned {
			return convertToUint(val, target)
		}
		return convertToInt(val, target)
	case mysql.TypeBit:
		x, err := convertToUint(val, target)
		if err != nil {
			return x, errors.Trace(err)
		}

		// check bit boundary, if bit has n width, the boundary is
		// in [0, (1 << n) - 1]
		width := target.Flen
		if width == 0 || width == mysql.UnspecifiedBitWidth {
			width = mysql.MinBitWidth
		}

		maxValue := uint64(1)<<uint64(width) - 1

		if x > maxValue {
			return maxValue, overflow(val, tp)
		}
		return mysql.Bit{Value: x, Width: width}, nil
	case mysql.TypeDecimal, mysql.TypeNewDecimal:
		x, err := ToDecimal(val)
		if err != nil {
			return invConv(val, tp)
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
			return int64(x.Year()), nil
		case mysql.Duration:
			return int64(time.Now().Year()), nil
		default:
			intVal, err = ToInt64(x)
		}
		if err != nil {
			return invConv(val, tp)
		}
		y, err := mysql.AdjustYear(int(intVal))
		if err != nil {
			return invConv(val, tp)
		}
		return int64(y), nil
	case mysql.TypeEnum:
		var (
			e   mysql.Enum
			err error
		)
		switch x := val.(type) {
		case string:
			e, err = mysql.ParseEnumName(target.Elems, x)
		case []byte:
			e, err = mysql.ParseEnumName(target.Elems, string(x))
		default:
			var number uint64
			number, err = ToUint64(x)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e, err = mysql.ParseEnumValue(target.Elems, number)
		}

		if err != nil {
			return invConv(val, tp)
		}
		return e, nil
	case mysql.TypeSet:
		var (
			s   mysql.Set
			err error
		)
		switch x := val.(type) {
		case string:
			s, err = mysql.ParseSetName(target.Elems, x)
		case []byte:
			s, err = mysql.ParseSetName(target.Elems, string(x))
		default:
			var number uint64
			number, err = ToUint64(x)
			if err != nil {
				return nil, errors.Trace(err)
			}
			s, err = mysql.ParseSetValue(target.Elems, number)
		}

		if err != nil {
			return invConv(val, tp)
		}
		return s, nil
	case mysql.TypeNull:
		return nil, nil
	default:
		panic("should never happen")
	}
}

// StrToInt converts a string to an integer in best effort.
// TODO: handle overflow and add unittest.
func StrToInt(str string) (int64, error) {
	str = strings.TrimSpace(str)
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
	str = strings.TrimSpace(str)
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
	str = strings.TrimSpace(str)
	if len(str) == 0 {
		return 0, nil
	}

	// MySQL uses a very loose conversation, e.g, 123.abc -> 123
	// We should do a trade off whether supporting this feature or using a strict mode.
	// Now we use a strict mode.
	return strconv.ParseFloat(str, 64)
}

// ToUint64 converts an interface to an uint64.
func ToUint64(value interface{}) (uint64, error) {
	switch v := value.(type) {
	case int:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint64:
		return uint64(v), nil
	case float32:
		return uint64(RoundFloat((float64(v)))), nil
	case float64:
		return uint64(RoundFloat(v)), nil
	case string:
		f, err := StrToFloat(v)
		return uint64(f), err
	case []byte:
		f, err := StrToFloat(string(v))
		return uint64(f), err
	case mysql.Time:
		// 2011-11-10 11:11:11.999999 -> 20111110111112
		return uint64(v.ToNumber().Round(0).IntPart()), nil
	case mysql.Duration:
		// 11:11:11.999999 -> 111112
		return uint64(v.ToNumber().Round(0).IntPart()), nil
	case mysql.Decimal:
		return uint64(v.Round(0).IntPart()), nil
	case mysql.Hex:
		// we don't need RoundFloat here because hex can not have fractional part.
		return uint64(v.ToNumber()), nil
	case mysql.Bit:
		return uint64(v.ToNumber()), nil
	case mysql.Enum:
		return uint64(v.ToNumber()), nil
	case mysql.Set:
		return uint64(v.ToNumber()), nil
	case *DataItem:
		return ToUint64(v.Data)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", value, value)
	}
}

// ToInt64 converts an interface to an int64.
func ToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case int:
		return int64(v), nil
	case int64:
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
	case mysql.Hex:
		// we don't need RoundFloat here because hex can not have fractional part.
		return int64(v.ToNumber()), nil
	case mysql.Bit:
		return int64(v.ToNumber()), nil
	case mysql.Enum:
		return int64(v.ToNumber()), nil
	case mysql.Set:
		return int64(v.ToNumber()), nil
	case *DataItem:
		return ToInt64(v.Data)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", value, value)
	}
}

// ToFloat64 converts an interface to a float64.
func ToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return float64(v), nil
	case string:
		return StrToFloat(v)
	case []byte:
		return StrToFloat(string(v))
	case mysql.Time:
		f, _ := v.ToNumber().Float64()
		return f, nil
	case mysql.Duration:
		f, _ := v.ToNumber().Float64()
		return f, nil
	case mysql.Decimal:
		vv, _ := v.Float64()
		return vv, nil
	case mysql.Hex:
		return v.ToNumber(), nil
	case mysql.Bit:
		return v.ToNumber(), nil
	case mysql.Enum:
		return v.ToNumber(), nil
	case mysql.Set:
		return v.ToNumber(), nil
	case *DataItem:
		return ToFloat64(v.Data)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to float64", value, value)
	}
}

// ToDecimal converts an interface to a Decimal.
func ToDecimal(value interface{}) (mysql.Decimal, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return mysql.ConvertToDecimal(1)
		}
		return mysql.ConvertToDecimal(0)
	case []byte:
		return mysql.ConvertToDecimal(string(v))
	case mysql.Time:
		return v.ToNumber(), nil
	case mysql.Duration:
		return v.ToNumber(), nil
	case *DataItem:
		return ToDecimal(v.Data)
	default:
		return mysql.ConvertToDecimal(value)
	}
}

// ToString converts an interface to a string.
func ToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(int64(v), 10), nil
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
	case mysql.Hex:
		return v.ToString(), nil
	case mysql.Bit:
		return v.ToString(), nil
	case mysql.Enum:
		return v.String(), nil
	case mysql.Set:
		return v.String(), nil
	case *DataItem:
		return ToString(v.Data)
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", value, value)
	}
}

// ToBool converts an interface to a bool.
// We will use 1 for true, and 0 for false.
func ToBool(value interface{}) (int64, error) {
	isZero := false
	switch v := value.(type) {
	case bool:
		isZero = (v == false)
	case int:
		isZero = (v == 0)
	case int64:
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
	case mysql.Hex:
		isZero = (v.ToNumber() == 0)
	case mysql.Bit:
		isZero = (v.ToNumber() == 0)
	case mysql.Enum:
		isZero = (v.ToNumber() == 0)
	case mysql.Set:
		isZero = (v.ToNumber() == 0)
	case *DataItem:
		return ToBool(v.Data)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to bool", value, value)
	}

	if isZero {
		return 0, nil
	}

	return 1, nil
}
