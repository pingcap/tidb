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
	"reflect"
	"strings"
)

// floatEpsilon is for compare two float value allowing precision loss.
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

func jsonAsFloat64(j JSON) float64 {
	if j.Type() == "INTEGER" {
		if reflect.TypeOf(j).Kind() == reflect.Ptr {
			return float64(*j.(*jsonInt64))
		}
		return float64(j.(jsonInt64))
	} else if j.Type() == "DOUBLE" {
		if reflect.TypeOf(j).Kind() == reflect.Ptr {
			return float64(*j.(*jsonDouble))
		}
		return float64(j.(jsonDouble))
	} else if j.Type() == "BOOLEAN" {
		if reflect.TypeOf(j).Kind() == reflect.Ptr {
			return float64(*j.(*jsonLiteral))
		}
		return float64(j.(jsonLiteral))
	}
	panic("can not convert to float64")
}

func jsonAsString(j JSON) string {
	if reflect.TypeOf(j).Kind() == reflect.Ptr {
		return string(*j.(*jsonString))
	}
	return string(j.(jsonString))
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
		switch x := j1.(type) {
		case jsonLiteral:
			left := int(x)
			right := int(j2.(jsonLiteral))
			// false is less than true.
			cmp = right - left
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
		cmp = compareFloat64PrecisionLoss(jsonAsFloat64(j1), jsonAsFloat64(j2))
	} else {
		cmp = precedence1 - precedence2
	}
	return
}
