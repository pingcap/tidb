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
	"bytes"
	"fmt"
	"strings"
	"unsafe"

	"github.com/juju/errors"
)

// floatEpsilon is the acceptable error quantity when comparing two float numbers.
const floatEpsilon float64 = 1.e-8

// compareFloat64 returns an integer comparing the float64 x to y,
// allowing precision loss.
func compareFloat64PrecisionLoss(x, y float64) int {
	if x-y < floatEpsilon && y-x < floatEpsilon {
		return 0
	} else if x-y < 0 {
		return -1
	}
	return 1
}

// jsonTypePrecedences is for comparing two json.
// See: https://dev.mysql.com/doc/refman/5.7/en/json.html#json-comparison
var jsonTypePrecedences = map[string]int{
	"BLOB":             -1,
	"BIT":              -2,
	"OPAQUE":           -3,
	"DATETIME":         -4,
	"TIME":             -5,
	"DATE":             -6,
	"BOOLEAN":          -7,
	"ARRAY":            -8,
	"OBJECT":           -9,
	"STRING":           -10,
	"INTEGER":          -11,
	"UNSIGNED INTEGER": -11,
	"DOUBLE":           -11,
	"NULL":             -12,
}

func i64AsFloat64(i64 int64, typeCode TypeCode) float64 {
	switch typeCode {
	case TypeCodeLiteral, TypeCodeInt64:
		return float64(i64)
	case TypeCodeUint64:
		u64 := *(*uint64)(unsafe.Pointer(&i64))
		return float64(u64)
	case TypeCodeFloat64:
		return *(*float64)(unsafe.Pointer(&i64))
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, typeCode)
		panic(msg)
	}
}

// CompareJSON compares two json objects. Returns -1 if j1 < j2,
// 0 if j1 == j2, else returns 1.
func CompareJSON(j1 JSON, j2 JSON) (cmp int, err error) {
	precedence1 := jsonTypePrecedences[j1.Type()]
	precedence2 := jsonTypePrecedences[j2.Type()]

	if precedence1 == precedence2 {
		if precedence1 == jsonTypePrecedences["NULL"] {
			// for JSON null.
			cmp = 0
		}
		switch j1.TypeCode {
		case TypeCodeLiteral:
			left := j1.I64
			right := j2.I64
			// false is less than true.
			cmp = int(right - left)
		case TypeCodeInt64, TypeCodeUint64, TypeCodeFloat64:
			left := i64AsFloat64(j1.I64, j1.TypeCode)
			right := i64AsFloat64(j2.I64, j2.TypeCode)
			cmp = compareFloat64PrecisionLoss(left, right)
		case TypeCodeString:
			left := j1.Str
			right := j2.Str
			cmp = strings.Compare(left, right)
		case TypeCodeArray:
			left := j1.Array
			right := j2.Array
			for i := 0; i < len(left) && i < len(right); i++ {
				elem1 := left[i]
				elem2 := right[i]
				cmp, err = CompareJSON(elem1, elem2)
				if cmp != 0 || err != nil {
					return cmp, errors.Trace(err)
				}
			}
			cmp = len(left) - len(right)
		case TypeCodeObject:
			// only equal is defined on two json objects.
			// larger and smaller are not defined.
			s1 := Serialize(j1)
			s2 := Serialize(j2)
			cmp = bytes.Compare(s1, s2)
		default:
			err = errors.Errorf(unknownTypeCodeErrorMsg, j1.TypeCode)
			return
		}
	} else {
		cmp = precedence1 - precedence2
	}
	return
}
