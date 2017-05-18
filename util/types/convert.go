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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
)

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
	mysql.TypeEnum:     math.MaxUint64,
	mysql.TypeSet:      math.MaxUint64,
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

func convertFloatToInt(sc *variable.StatementContext, fval float64, lowerBound, upperBound int64, tp byte) (int64, error) {
	val := RoundFloat(fval)
	if val < float64(lowerBound) {
		return lowerBound, overflow(val, tp)
	}

	if val > float64(upperBound) {
		return upperBound, overflow(val, tp)
	}
	return int64(val), nil
}

func convertIntToInt(val int64, lowerBound int64, upperBound int64, tp byte) (int64, error) {
	if val < lowerBound {
		return lowerBound, overflow(val, tp)
	}

	if val > upperBound {
		return upperBound, overflow(val, tp)
	}

	return val, nil
}

func convertUintToInt(val uint64, upperBound int64, tp byte) (int64, error) {
	if val > uint64(upperBound) {
		return upperBound, overflow(val, tp)
	}

	return int64(val), nil
}

func convertIntToUint(val int64, upperBound uint64, tp byte) (uint64, error) {
	if val < 0 {
		return 0, overflow(val, tp)
	}

	if uint64(val) > upperBound {
		return upperBound, overflow(val, tp)
	}

	return uint64(val), nil
}

func convertUintToUint(val uint64, upperBound uint64, tp byte) (uint64, error) {
	if val > upperBound {
		return upperBound, overflow(val, tp)
	}

	return val, nil
}

func convertFloatToUint(sc *variable.StatementContext, fval float64, upperBound uint64, tp byte) (uint64, error) {
	val := RoundFloat(fval)
	if val < 0 {
		return uint64(int64(val)), overflow(val, tp)
	}

	if val > float64(upperBound) {
		return upperBound, overflow(val, tp)
	}
	return uint64(val), nil
}

func isCastType(tp byte) bool {
	switch tp {
	case mysql.TypeString, mysql.TypeDuration, mysql.TypeDatetime,
		mysql.TypeDate, mysql.TypeLonglong, mysql.TypeNewDecimal:
		return true
	}
	return false
}

// StrToInt converts a string to an integer at the best-effort.
func StrToInt(sc *variable.StatementContext, str string) (int64, error) {
	str = strings.TrimSpace(str)
	validPrefix, err := getValidIntPrefix(sc, str)
	iVal, err1 := strconv.ParseInt(validPrefix, 10, 64)
	if err1 != nil {
		return iVal, ErrOverflow.GenByArgs("BIGINT", validPrefix)
	}
	return iVal, errors.Trace(err)
}

// StrToUint converts a string to an unsigned integer at the best-effortt.
func StrToUint(sc *variable.StatementContext, str string) (uint64, error) {
	str = strings.TrimSpace(str)
	validPrefix, err := getValidIntPrefix(sc, str)
	if validPrefix[0] == '+' {
		validPrefix = validPrefix[1:]
	}
	uVal, err1 := strconv.ParseUint(validPrefix, 10, 64)
	if err1 != nil {
		return uVal, ErrOverflow.GenByArgs("BIGINT UNSIGNED", validPrefix)
	}
	return uVal, errors.Trace(err)
}

// getValidIntPrefix gets prefix of the string which can be successfully parsed as int.
func getValidIntPrefix(sc *variable.StatementContext, str string) (string, error) {
	floatPrefix, err := getValidFloatPrefix(sc, str)
	if err != nil {
		return floatPrefix, errors.Trace(err)
	}
	return floatStrToIntStr(floatPrefix)
}

// floatStrToIntStr converts a valid float string into valid integer string which can be parsed by
// strconv.ParseInt, we can't parse float first then convert it to string because precision will
// be lost.
func floatStrToIntStr(validFloat string) (string, error) {
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
		return validFloat[:dotIdx], nil
	}
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
	if exp > 0 && int64(intCnt) > (math.MaxInt64-int64(exp)) {
		// (exp + incCnt) overflows MaxInt64.
		return validFloat, ErrOverflow.GenByArgs("BIGINT", validFloat)
	}
	intCnt += exp
	if intCnt <= 0 {
		return "0", nil
	}
	if intCnt == 1 && (digits[0] == '-' || digits[0] == '+') {
		return "0", nil
	}
	var validInt string
	if intCnt <= len(digits) {
		validInt = string(digits[:intCnt])
	} else {
		extraZeroCount := intCnt - len(digits)
		if extraZeroCount > 20 {
			// Return overflow to avoid allocating too much memory.
			return validFloat, ErrOverflow.GenByArgs("BIGINT", validFloat)
		}
		validInt = string(digits) + strings.Repeat("0", extraZeroCount)
	}
	return validInt, nil
}

// StrToFloat converts a string to a float64 at the best-effort.
func StrToFloat(sc *variable.StatementContext, str string) (float64, error) {
	str = strings.TrimSpace(str)
	validStr, err := getValidFloatPrefix(sc, str)
	f, err1 := strconv.ParseFloat(validStr, 64)
	if err1 != nil {
		return f, errors.Trace(err1)
	}
	return f, errors.Trace(err)
}

// getValidFloatPrefix gets prefix of string which can be successfully parsed as float.
func getValidFloatPrefix(sc *variable.StatementContext, s string) (valid string, err error) {
	var (
		sawDot   bool
		sawDigit bool
		validLen int
		eIdx     int
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
			if eIdx != 0 { // "1e5e"
				break
			}
			eIdx = i
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
		err = errors.Trace(handleTruncateError(sc))
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
	case Time:
		return v.String(), nil
	case Duration:
		return v.String(), nil
	case *MyDecimal:
		return v.String(), nil
	case Hex:
		return v.ToString(), nil
	case Bit:
		return v.ToString(), nil
	case Enum:
		return v.String(), nil
	case Set:
		return v.String(), nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", value, value)
	}
}
