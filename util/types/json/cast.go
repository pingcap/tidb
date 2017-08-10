// Copyright 2017 PingCAP, Inc.
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

package json

import (
	"fmt"
	"strconv"
	"unsafe"
)

// CastToInt casts JSON into int64.
func (j JSON) CastToInt() (int64, error) {
	switch j.typeCode {
	case typeCodeObject, typeCodeArray:
		return 0, nil
	case typeCodeLiteral:
		switch byte(j.i64) {
		case jsonLiteralNil, jsonLiteralFalse:
			return 0, nil
		default:
			return 1, nil
		}
	case typeCodeInt64, typeCodeUint64:
		return j.i64, nil
	case typeCodeFloat64:
		f := *(*float64)(unsafe.Pointer(&j.i64))
		return int64(f), nil
	case typeCodeString:
		val, err := strconv.Atoi(j.str)
		if err != nil {
			val = 0
		}
		return int64(val), nil
	}
	panic(fmt.Sprintf(unknownTypeCodeErrorMsg, j.typeCode))
}

// CastToReal casts JSON into float64.
func (j JSON) CastToReal() (float64, error) {
	switch j.typeCode {
	case typeCodeObject, typeCodeArray:
		return 0, nil
	case typeCodeLiteral:
		switch byte(j.i64) {
		case jsonLiteralNil, jsonLiteralFalse:
			return 0, nil
		default:
			return 1, nil
		}
	case typeCodeInt64, typeCodeUint64:
		return float64(j.i64), nil
	case typeCodeFloat64:
		f := *(*float64)(unsafe.Pointer(&j.i64))
		return f, nil
	case typeCodeString:
		val, err := strconv.ParseFloat(j.str, 64)
		if err != nil {
			val = 0
		}
		return val, nil
	}
	panic(fmt.Sprintf(unknownTypeCodeErrorMsg, j.typeCode))
}
