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
	"strings"
	"unsafe"
)

// floatEpsilon is the acceptable error precision when comparing two floating numbers.
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
	"BLOB":     -1,
	"BIT":      -2,
	"OPAQUE":   -3,
	"DATETIME": -4,
	"TIME":     -5,
	"DATE":     -6,
	"BOOLEAN":  -7,
	"ARRAY":    -8,
	"OBJECT":   -9,
	"STRING":   -10,
	"INTEGER":  -11,
	"DOUBLE":   -11,
	"NULL":     -12,
}

func bit64AsFloat64(bit64 *int64, typeCode byte) float64 {
	switch typeCode {
	case typeCodeLiteral, typeCodeInt64:
		return float64(*bit64)
	case typeCodeFloat64:
		return *(*float64)(unsafe.Pointer(bit64))
	default:
		panic(internalErrorUnknownTypeCode)
	}
}

// CompareJSON compares two json object.
func CompareJSON(j1 JSON, j2 JSON) (cmp int, err error) {
	precedence1 := jsonTypePrecedences[j1.Type()]
	precedence2 := jsonTypePrecedences[j2.Type()]

	if precedence1 == precedence2 {
		if precedence1 == jsonTypePrecedences["NULL"] {
			// for JSON null.
			cmp = 0
		}
		switch j1.typeCode {
		case typeCodeLiteral:
			left := j1.bit64
			right := j2.bit64
			// false is less than true.
			cmp = int(right - left)
		case typeCodeInt64, typeCodeFloat64:
			left := bit64AsFloat64(&j1.bit64, j1.typeCode)
			right := bit64AsFloat64(&j2.bit64, j2.typeCode)
			cmp = compareFloat64PrecisionLoss(left, right)
		case typeCodeString:
			left := j1.str
			right := j2.str
			cmp = strings.Compare(left, right)
		case typeCodeArray:
			left := j1.array
			right := j2.array
			for i := 0; i < len(left) && i < len(right); i++ {
				elem1 := left[i]
				elem2 := right[i]
				cmp, _ = CompareJSON(elem1, elem2)
				if cmp != 0 {
					return
				}
			}
			cmp = len(left) - len(right)
		case typeCodeObject:
			// only equal is defined on two json objects.
			// larger and smaller are not defined.
			s1 := Serialize(j1)
			s2 := Serialize(j2)
			cmp = bytes.Compare(s1, s2)
		default:
			cmp = 0
		}
	} else if (precedence1 == jsonTypePrecedences["BOOLEAN"] && precedence2 == jsonTypePrecedences["INTEGER"]) ||
		(precedence1 == jsonTypePrecedences["INTEGER"] && precedence2 == jsonTypePrecedences["BOOLEAN"]) {
		// tidb treat boolean as integer, but boolean is different from integer in JSON.
		// so we need convert them to same type and then compare.
		left := bit64AsFloat64(&j1.bit64, j1.typeCode)
		right := bit64AsFloat64(&j2.bit64, j2.typeCode)
		cmp = compareFloat64PrecisionLoss(left, right)
	} else {
		cmp = precedence1 - precedence2
	}
	return
}
