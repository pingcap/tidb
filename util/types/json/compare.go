// Copyright 2017 The ql Authors. All rights reserved.
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

package json

import (
	"bytes"
	"reflect"
	"strings"
)

// floatEpsilon is for compare two float value allowing precision loss.
const floatEpsilon float64 = 0.00000001

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

// jsonTypePrecedences is for compare two json.
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

func jsonAsFloat64(j JSON) float64 {
	if j.Type() == "INTEGER" {
		if reflect.TypeOf(j).Kind() == reflect.Ptr {
			return float64(*j.(*jsonInt64))
		} else {
			return float64(j.(jsonInt64))
		}
	} else if j.Type() == "DOUBLE" {
		if reflect.TypeOf(j).Kind() == reflect.Ptr {
			return float64(*j.(*jsonDouble))
		} else {
			return float64(j.(jsonDouble))
		}
	} else {
		panic("can not convert to float64")
	}
}

func jsonAsString(j JSON) string {
	if reflect.TypeOf(j).Kind() == reflect.Ptr {
		return string(*j.(*jsonString))
	} else {
		return string(j.(jsonString))
	}
}

// CompareJSON compares two json object.
func CompareJSON(j1 JSON, j2 JSON) (cmp int, err error) {
	precedence1 := jsonTypePrecedences[j1.Type()]
	precedence2 := jsonTypePrecedences[j2.Type()]

	if precedence1 == precedence2 {
		if precedence1 == -12 {
			// for JSON null.
			cmp = 0
		}
		switch x := j1.(type) {
		case jsonLiteral:
			// false is less than true.
			left := int(x)
			right := int(j2.(jsonLiteral))
			cmp = left - right
		case jsonInt64, jsonDouble:
			left := jsonAsFloat64(j1)
			right := jsonAsFloat64(j2)
			cmp = compareFloat64PrecisionLoss(left, right)
		case jsonString:
			left := jsonAsString(j1)
			right := jsonAsString(j2)
			cmp = strings.Compare(left, right)
		case jsonArray:
			y := j2.(jsonArray)
			for i := 0; i < len(x) && i < len(y); i++ {
				elem1 := x[i]
				elem2 := y[i]
				cmp, _ = CompareJSON(elem1, elem2)
				if cmp != 0 {
					return
				}
			}
			cmp = len(x) - len(y)
		case jsonObject:
			// only equal is defined on two json objects.
			// larger and smaller are not defined.
			s1 := Serialize(x)
			s2 := Serialize(j2)
			cmp = bytes.Compare(s1, s2)
		default:
			cmp = 0
		}
	} else if (precedence1 == -7 && precedence2 == -11) || (precedence1 == -11 && precedence2 == -7) {
		// tidb treat boolean as integer, but boolean is different from integer in JSON.
		// so we need convert them to same type and then compare.
		var x, y float64
		if precedence1 == -7 {
			x = float64(j1.(jsonLiteral))
			y = jsonAsFloat64(j2)
		} else {
			x = jsonAsFloat64(j1)
			y = float64(*j2.(*jsonLiteral))
		}
		cmp = compareFloat64PrecisionLoss(x, y)
	} else {
		cmp = precedence1 - precedence2
	}
	return
}
