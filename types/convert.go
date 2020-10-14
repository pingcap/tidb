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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

func truncateStr(str string, flen int) string {
	if flen != UnspecifiedLength && len(str) > flen {
		str = str[:flen]
	}
	return str
}

// IntergerUnsignedUpperBound indicates the max uint64 values of different mysql types.
func IntergerUnsignedUpperBound(intType byte) uint64 {
	switch intType {
	case mysql.TypeTiny:
		return math.MaxUint8
	case mysql.TypeShort:
		return math.MaxUint16
	case mysql.TypeInt24:
		return mysql.MaxUint24
	case mysql.TypeLong:
		return math.MaxUint32
	case mysql.TypeLonglong:
		return math.MaxUint64
	case mysql.TypeBit:
		return math.MaxUint64
	case mysql.TypeEnum:
		return math.MaxUint64
	case mysql.TypeSet:
		return math.MaxUint64
	default:
		panic("Input byte is not a mysql type")
	}
}

// IntergerSignedUpperBound indicates the max int64 values of different mysql types.
func IntergerSignedUpperBound(intType byte) int64 {
	switch intType {
	case mysql.TypeTiny:
		return math.MaxInt8
	case mysql.TypeShort:
		return math.MaxInt16
	case mysql.TypeInt24:
		return mysql.MaxInt24
	case mysql.TypeLong:
		return math.MaxInt32
	case mysql.TypeLonglong:
		return math.MaxInt64
	default:
		panic("Input byte is not a mysql type")
	}
}

// IntergerSignedLowerBound indicates the min int64 values of different mysql types.
func IntergerSignedLowerBound(intType byte) int64 {
	switch intType {
	case mysql.TypeTiny:
		return math.MinInt8
	case mysql.TypeShort:
		return math.MinInt16
	case mysql.TypeInt24:
		return mysql.MinInt24
	case mysql.TypeLong:
		return math.MinInt32
	case mysql.TypeLonglong:
		return math.MinInt64
	default:
		panic("Input byte is not a mysql type")
	}
}

// ConvertFloatToInt converts a float64 value to a int value.
// `tp` is used in err msg, if there is overflow, this func will report err according to `tp`
func ConvertFloatToInt(fval float64, lowerBound, upperBound int64, tp byte) (int64, error) {
	val := RoundFloat(fval)
	if val < float64(lowerBound) {
		return lowerBound, overflow(val, tp)
	}

	if val >= float64(upperBound) {
		if val == float64(upperBound) {
			return upperBound, nil
		}
		return upperBound, overflow(val, tp)
	}
	return int64(val), nil
}

// ConvertIntToInt converts an int value to another int value of different precision.
func ConvertIntToInt(val int64, lowerBound int64, upperBound int64, tp byte) (int64, error) {
	if val < lowerBound {
		return lowerBound, overflow(val, tp)
	}

	if val > upperBound {
		return upperBound, overflow(val, tp)
	}

	return val, nil
}

// ConvertUintToInt converts an uint value to an int value.
func ConvertUintToInt(val uint64, upperBound int64, tp byte) (int64, error) {
	if val > uint64(upperBound) {
		return upperBound, overflow(val, tp)
	}

	return int64(val), nil
}

// ConvertIntToUint converts an int value to an uint value.
func ConvertIntToUint(sc *stmtctx.StatementContext, val int64, upperBound uint64, tp byte) (uint64, error) {
	if sc.ShouldClipToZero() && val < 0 {
		return 0, overflow(val, tp)
	}

	if uint64(val) > upperBound {
		return upperBound, overflow(val, tp)
	}

	return uint64(val), nil
}

// ConvertUintToUint converts an uint value to another uint value of different precision.
func ConvertUintToUint(val uint64, upperBound uint64, tp byte) (uint64, error) {
	if val > upperBound {
		return upperBound, overflow(val, tp)
	}

	return val, nil
}

// ConvertFloatToUint converts a float value to an uint value.
func ConvertFloatToUint(sc *stmtctx.StatementContext, fval float64, upperBound uint64, tp byte) (uint64, error) {
	val := RoundFloat(fval)
	if val < 0 {
		if sc.ShouldClipToZero() {
			return 0, overflow(val, tp)
		}
		return uint64(int64(val)), overflow(val, tp)
	}

	ubf := float64(upperBound)
	// Because math.MaxUint64 can not be represented precisely in iee754(64bit),
	// so `float64(math.MaxUint64)` will make a num bigger than math.MaxUint64,
	// which can not be represented by 64bit integer.
	// So `uint64(float64(math.MaxUint64))` is undefined behavior.
	if val == ubf {
		return math.MaxUint64, nil
	}
	if val > ubf {
		return math.MaxUint64, overflow(val, tp)
	}
	return uint64(val), nil
}

// convertScientificNotation converts a decimal string with scientific notation to a normal decimal string.
// 1E6 => 1000000, .12345E+5 => 12345
func convertScientificNotation(str string) (string, error) {
	// https://golang.org/ref/spec#Floating-point_literals
	eIdx := -1
	point := -1
	for i := 0; i < len(str); i++ {
		if str[i] == '.' {
			point = i
		}
		if str[i] == 'e' || str[i] == 'E' {
			eIdx = i
			if point == -1 {
				point = i
			}
			break
		}
	}
	if eIdx == -1 {
		return str, nil
	}
	exp, err := strconv.ParseInt(str[eIdx+1:], 10, 64)
	if err != nil {
		return "", errors.WithStack(err)
	}

	f := str[:eIdx]
	if exp == 0 {
		return f, nil
	} else if exp > 0 { // move point right
		if point+int(exp) == len(f)-1 { // 123.456 >> 3 = 123456. = 123456
			return f[:point] + f[point+1:], nil
		} else if point+int(exp) < len(f)-1 { // 123.456 >> 2 = 12345.6
			return f[:point] + f[point+1:point+1+int(exp)] + "." + f[point+1+int(exp):], nil
		}
		// 123.456 >> 5 = 12345600
		return f[:point] + f[point+1:] + strings.Repeat("0", point+int(exp)-len(f)+1), nil
	} else { // move point left
		exp = -exp
		if int(exp) < point { // 123.456 << 2 = 1.23456
			return f[:point-int(exp)] + "." + f[point-int(exp):point] + f[point+1:], nil
		}
		// 123.456 << 5 = 0.00123456
		return "0." + strings.Repeat("0", int(exp)-point) + f[:point] + f[point+1:], nil
	}
}

func convertDecimalStrToUint(sc *stmtctx.StatementContext, str string, upperBound uint64, tp byte) (uint64, error) {
	str, err := convertScientificNotation(str)
	if err != nil {
		return 0, err
	}

	var intStr, fracStr string
	p := strings.Index(str, ".")
	if p == -1 {
		intStr = str
	} else {
		intStr = str[:p]
		fracStr = str[p+1:]
	}
	intStr = strings.TrimLeft(intStr, "0")
	if intStr == "" {
		intStr = "0"
	}
	if sc.ShouldClipToZero() && intStr[0] == '-' {
		return 0, overflow(str, tp)
	}

	var round uint64
	if fracStr != "" && fracStr[0] >= '5' {
		round++
	}

	upperBound -= round
	upperStr := strconv.FormatUint(upperBound, 10)
	if len(intStr) > len(upperStr) ||
		(len(intStr) == len(upperStr) && intStr > upperStr) {
		return upperBound, overflow(str, tp)
	}

	val, err := strconv.ParseUint(intStr, 10, 64)
	if err != nil {
		return val, errors.Trace(err)
	}
	return val + round, nil
}

// ConvertDecimalToUint converts a decimal to a uint by converting it to a string first to avoid float overflow (#10181).
func ConvertDecimalToUint(sc *stmtctx.StatementContext, d *MyDecimal, upperBound uint64, tp byte) (uint64, error) {
	return convertDecimalStrToUint(sc, string(d.ToString()), upperBound, tp)
}

// StrToInt converts a string to an integer at the best-effort.
func StrToInt(sc *stmtctx.StatementContext, str string, isFuncCast bool) (int64, error) {
	str = strings.TrimSpace(str)
	validPrefix, err := getValidIntPrefix(sc, str, isFuncCast)
	iVal, err1 := strconv.ParseInt(validPrefix, 10, 64)
	if err1 != nil {
		return iVal, ErrOverflow.GenWithStackByArgs("BIGINT", validPrefix)
	}
	return iVal, errors.Trace(err)
}

// StrToUint converts a string to an unsigned integer at the best-effortt.
func StrToUint(sc *stmtctx.StatementContext, str string, isFuncCast bool) (uint64, error) {
	str = strings.TrimSpace(str)
	validPrefix, err := getValidIntPrefix(sc, str, isFuncCast)
	if validPrefix[0] == '+' {
		validPrefix = validPrefix[1:]
	}
	uVal, err1 := strconv.ParseUint(validPrefix, 10, 64)
	if err1 != nil {
		return uVal, ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", validPrefix)
	}
	return uVal, errors.Trace(err)
}

// StrToDateTime converts str to MySQL DateTime.
func StrToDateTime(sc *stmtctx.StatementContext, str string, fsp int8) (Time, error) {
	return ParseTime(sc, str, mysql.TypeDatetime, fsp)
}

// StrToDuration converts str to Duration. It returns Duration in normal case,
// and returns Time when str is in datetime format.
// when isDuration is true, the d is returned, when it is false, the t is returned.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-literals.html.
func StrToDuration(sc *stmtctx.StatementContext, str string, fsp int8) (d Duration, t Time, isDuration bool, err error) {
	str = strings.TrimSpace(str)
	length := len(str)
	if length > 0 && str[0] == '-' {
		length--
	}
	// Timestamp format is 'YYYYMMDDHHMMSS' or 'YYMMDDHHMMSS', which length is 12.
	// See #3923, it explains what we do here.
	if length >= 12 {
		t, err = StrToDateTime(sc, str, fsp)
		if err == nil {
			return d, t, false, nil
		}
	}

	d, err = ParseDuration(sc, str, fsp)
	if ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return d, t, true, errors.Trace(err)
}

// NumberToDuration converts number to Duration.
func NumberToDuration(number int64, fsp int8) (Duration, error) {
	if number > TimeMaxValue {
		// Try to parse DATETIME.
		if number >= 10000000000 { // '2001-00-00 00-00-00'
			if t, err := ParseDatetimeFromNum(nil, number); err == nil {
				dur, err1 := t.ConvertToDuration()
				return dur, errors.Trace(err1)
			}
		}
		dur := MaxMySQLDuration(fsp)
		return dur, ErrOverflow.GenWithStackByArgs("Duration", strconv.Itoa(int(number)))
	} else if number < -TimeMaxValue {
		dur := MaxMySQLDuration(fsp)
		dur.Duration = -dur.Duration
		return dur, ErrOverflow.GenWithStackByArgs("Duration", strconv.Itoa(int(number)))
	}
	var neg bool
	if neg = number < 0; neg {
		number = -number
	}

	if number/10000 > TimeMaxHour || number%100 >= 60 || (number/100)%100 >= 60 {
		return ZeroDuration, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(number, 10)))
	}
	dur := NewDuration(int(number/10000), int((number/100)%100), int(number%100), 0, fsp)
	if neg {
		dur.Duration = -dur.Duration
	}
	return dur, nil
}

// getValidIntPrefix gets prefix of the string which can be successfully parsed as int.
func getValidIntPrefix(sc *stmtctx.StatementContext, str string, isFuncCast bool) (string, error) {
	if !isFuncCast {
		floatPrefix, err := getValidFloatPrefix(sc, str, isFuncCast)
		if err != nil {
			return floatPrefix, errors.Trace(err)
		}
		return floatStrToIntStr(sc, floatPrefix, str)
	}

	validLen := 0

	for i := 0; i < len(str); i++ {
		c := str[i]
		if (c == '+' || c == '-') && i == 0 {
			continue
		}

		if c >= '0' && c <= '9' {
			validLen = i + 1
			continue
		}

		break
	}
	valid := str[:validLen]
	if valid == "" {
		valid = "0"
	}
	if validLen == 0 || validLen != len(str) {
		return valid, errors.Trace(handleTruncateError(sc, ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", str)))
	}
	return valid, nil
}

// roundIntStr is to round a **valid int string** base on the number following dot.
func roundIntStr(numNextDot byte, intStr string) string {
	if numNextDot < '5' {
		return intStr
	}
	retStr := []byte(intStr)
	idx := len(intStr) - 1
	for ; idx >= 1; idx-- {
		if retStr[idx] != '9' {
			retStr[idx]++
			break
		}
		retStr[idx] = '0'
	}
	if idx == 0 {
		if intStr[0] == '9' {
			retStr[0] = '1'
			retStr = append(retStr, '0')
		} else if isDigit(intStr[0]) {
			retStr[0]++
		} else {
			retStr[1] = '1'
			retStr = append(retStr, '0')
		}
	}
	return string(retStr)
}

// floatStrToIntStr converts a valid float string into valid integer string which can be parsed by
// strconv.ParseInt, we can't parse float first then convert it to string because precision will
// be lost. For example, the string value "18446744073709551615" which is the max number of unsigned
// int will cause some precision to lose. intStr[0] may be a positive and negative sign like '+' or '-'.
//
// This func will find serious overflow such as the len of intStr > 20 (without prefix `+/-`)
// however, it will not check whether the intStr overflow BIGINT.
func floatStrToIntStr(sc *stmtctx.StatementContext, validFloat string, oriStr string) (intStr string, _ error) {
	var dotIdx = -1
	var eIdx = -1
	for i := 0; i < len(validFloat); i++ {
		switch validFloat[i] {
		case '.':
			dotIdx = i
		case 'e', 'E':
			eIdx = i
		}
	}
	if eIdx == -1 {
		if dotIdx == -1 {
			return validFloat, nil
		}
		var digits []byte
		if validFloat[0] == '-' || validFloat[0] == '+' {
			dotIdx--
			digits = []byte(validFloat[1:])
		} else {
			digits = []byte(validFloat)
		}
		if dotIdx == 0 {
			intStr = "0"
		} else {
			intStr = string(digits)[:dotIdx]
		}
		if len(digits) > dotIdx+1 {
			intStr = roundIntStr(digits[dotIdx+1], intStr)
		}
		if (len(intStr) > 1 || intStr[0] != '0') && validFloat[0] == '-' {
			intStr = "-" + intStr
		}
		return intStr, nil
	}
	// intCnt and digits contain the prefix `+/-` if validFloat[0] is `+/-`
	var intCnt int
	digits := make([]byte, 0, len(validFloat))
	if dotIdx == -1 {
		digits = append(digits, validFloat[:eIdx]...)
		intCnt = len(digits)
	} else {
		digits = append(digits, validFloat[:dotIdx]...)
		intCnt = len(digits)
		digits = append(digits, validFloat[dotIdx+1:eIdx]...)
	}
	exp, err := strconv.Atoi(validFloat[eIdx+1:])
	if err != nil {
		return validFloat, errors.Trace(err)
	}
	intCnt += exp
	if exp >= 0 && (intCnt > 21 || intCnt < 0) {
		// MaxInt64 has 19 decimal digits.
		// MaxUint64 has 20 decimal digits.
		// And the intCnt may contain the len of `+/-`,
		// so I use 21 here as the early detection.
		sc.AppendWarning(ErrOverflow.GenWithStackByArgs("BIGINT", oriStr))
		return validFloat[:eIdx], nil
	}
	if intCnt <= 0 {
		intStr = "0"
		if intCnt == 0 && len(digits) > 0 && isDigit(digits[0]) {
			intStr = roundIntStr(digits[0], intStr)
		}
		return intStr, nil
	}
	if intCnt == 1 && (digits[0] == '-' || digits[0] == '+') {
		intStr = "0"
		if len(digits) > 1 {
			intStr = roundIntStr(digits[1], intStr)
		}
		if intStr[0] == '1' {
			intStr = string(digits[:1]) + intStr
		}
		return intStr, nil
	}
	if intCnt <= len(digits) {
		intStr = string(digits[:intCnt])
		if intCnt < len(digits) {
			intStr = roundIntStr(digits[intCnt], intStr)
		}
	} else {
		// convert scientific notation decimal number
		extraZeroCount := intCnt - len(digits)
		intStr = string(digits) + strings.Repeat("0", extraZeroCount)
	}
	return intStr, nil
}

// StrToFloat converts a string to a float64 at the best-effort.
func StrToFloat(sc *stmtctx.StatementContext, str string, isFuncCast bool) (float64, error) {
	str = strings.TrimSpace(str)
	validStr, err := getValidFloatPrefix(sc, str, isFuncCast)
	f, err1 := strconv.ParseFloat(validStr, 64)
	if err1 != nil {
		if err2, ok := err1.(*strconv.NumError); ok {
			// value will truncate to MAX/MIN if out of range.
			if err2.Err == strconv.ErrRange {
				err1 = sc.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("DOUBLE", str))
				if math.IsInf(f, 1) {
					f = math.MaxFloat64
				} else if math.IsInf(f, -1) {
					f = -math.MaxFloat64
				}
			}
		}
		return f, errors.Trace(err1)
	}
	return f, errors.Trace(err)
}

// ConvertJSONToInt casts JSON into int64.
func ConvertJSONToInt(sc *stmtctx.StatementContext, j json.BinaryJSON, unsigned bool) (int64, error) {
	switch j.TypeCode {
	case json.TypeCodeObject, json.TypeCodeArray:
		return 0, nil
	case json.TypeCodeLiteral:
		switch j.Value[0] {
		case json.LiteralNil, json.LiteralFalse:
			return 0, nil
		default:
			return 1, nil
		}
	case json.TypeCodeInt64, json.TypeCodeUint64:
		return j.GetInt64(), nil
	case json.TypeCodeFloat64:
		f := j.GetFloat64()
		if !unsigned {
			lBound := IntergerSignedLowerBound(mysql.TypeLonglong)
			uBound := IntergerSignedUpperBound(mysql.TypeLonglong)
			u, e := ConvertFloatToInt(f, lBound, uBound, mysql.TypeLonglong)
			return u, sc.HandleOverflow(e, e)
		}
		bound := IntergerUnsignedUpperBound(mysql.TypeLonglong)
		u, err := ConvertFloatToUint(sc, f, bound, mysql.TypeLonglong)
		return int64(u), sc.HandleOverflow(err, err)
	case json.TypeCodeString:
		str := string(hack.String(j.GetString()))
		if !unsigned {
			r, e := StrToInt(sc, str, false)
			return r, sc.HandleOverflow(e, e)
		}
		u, err := StrToUint(sc, str, false)
		return int64(u), sc.HandleOverflow(err, err)
	}
	return 0, errors.New("Unknown type code in JSON")
}

// ConvertJSONToFloat casts JSON into float64.
func ConvertJSONToFloat(sc *stmtctx.StatementContext, j json.BinaryJSON) (float64, error) {
	switch j.TypeCode {
	case json.TypeCodeObject, json.TypeCodeArray:
		return 0, nil
	case json.TypeCodeLiteral:
		switch j.Value[0] {
		case json.LiteralNil, json.LiteralFalse:
			return 0, nil
		default:
			return 1, nil
		}
	case json.TypeCodeInt64:
		return float64(j.GetInt64()), nil
	case json.TypeCodeUint64:
		return float64(j.GetUint64()), nil
	case json.TypeCodeFloat64:
		return j.GetFloat64(), nil
	case json.TypeCodeString:
		str := string(hack.String(j.GetString()))
		return StrToFloat(sc, str, false)
	}
	return 0, errors.New("Unknown type code in JSON")
}

// ConvertJSONToDecimal casts JSON into decimal.
func ConvertJSONToDecimal(sc *stmtctx.StatementContext, j json.BinaryJSON) (*MyDecimal, error) {
	res := new(MyDecimal)
	if j.TypeCode != json.TypeCodeString {
		f64, err := ConvertJSONToFloat(sc, j)
		if err != nil {
			return res, errors.Trace(err)
		}
		err = res.FromFloat64(f64)
		return res, errors.Trace(err)
	}
	err := sc.HandleTruncate(res.FromString(j.GetString()))
	return res, errors.Trace(err)
}

// getValidFloatPrefix gets prefix of string which can be successfully parsed as float.
func getValidFloatPrefix(sc *stmtctx.StatementContext, s string, isFuncCast bool) (valid string, err error) {
	if isFuncCast && s == "" {
		return "0", nil
	}

	var (
		sawDot   bool
		sawDigit bool
		validLen int
		eIdx     = -1
	)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '+' || c == '-' {
			if i != 0 && i != eIdx+1 { // "1e+1" is valid.
				break
			}
		} else if c == '.' {
			if sawDot || eIdx > 0 { // "1.1." or "1e1.1"
				break
			}
			sawDot = true
			if sawDigit { // "123." is valid.
				validLen = i + 1
			}
		} else if c == 'e' || c == 'E' {
			if !sawDigit { // "+.e"
				break
			}
			if eIdx != -1 { // "1e5e"
				break
			}
			eIdx = i
		} else if c == '\u0000' {
			s = s[:validLen]
			break
		} else if c < '0' || c > '9' {
			break
		} else {
			sawDigit = true
			validLen = i + 1
		}
	}
	valid = s[:validLen]
	if valid == "" {
		valid = "0"
	}
	if validLen == 0 || validLen != len(s) {
		err = errors.Trace(handleTruncateError(sc, ErrTruncatedWrongVal.GenWithStackByArgs("FLOAT", s)))
	}
	return valid, err
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
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case Time:
		return v.String(), nil
	case Duration:
		return v.String(), nil
	case *MyDecimal:
		return v.String(), nil
	case BinaryLiteral:
		return v.ToString(), nil
	case Enum:
		return v.String(), nil
	case Set:
		return v.String(), nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", value, value)
	}
}
