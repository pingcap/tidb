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
	"unicode"

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

func convertFloatToInt(val float64, lowerBound int64, upperBound int64, tp byte) (int64, error) {
	val = RoundFloat(val)
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

func convertFloatToUint(val float64, upperBound uint64, tp byte) (uint64, error) {
	val = RoundFloat(val)
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

// StrToInt converts a string to an integer in best effort.
// TODO: handle overflow and add unittest.
func StrToInt(sc *variable.StatementContext, str string) (int64, error) {
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

// StrToFloat converts a string to a float64 in best effort.
func StrToFloat(sc *variable.StatementContext, str string) (float64, error) {
	str = strings.TrimSpace(str)
	if len(str) == 0 {
		return 0, nil
	}
	validStr := getValidFloatPrefix(str)
	var err error
	if validStr != str {
		err = ErrTruncated
	}
	f, err1 := strconv.ParseFloat(validStr, 64)
	if err == nil {
		err = err1
	}
	return f, errors.Trace(err)
}

func getValidFloatPrefix(str string) string {
	var (
		hasDot bool
		eIdx   = -1
	)
	for i := 0; i < len(str); i++ {
		c := str[i]
		if c == '-' || c == '+' {
			if i != 0 && i != eIdx+1 {
				return str[:i]
			}
		} else if c == '.' {
			if hasDot {
				return str[:i]
			}
			hasDot = true
		} else if c == 'e' || c == 'E' {
			if eIdx != -1 {
				return str[:i]
			}
			eIdx = i
		} else if c < '0' || c > '9' {
			return str[:i]
		}
	}
	return str
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
