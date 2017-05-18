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
	"strings"
)

// compareFloat64 returns an integer comparing the float64 x to y,
// allowing precision loss.
func compareFloat64PrecisionLoss(x, y float64) int {
	var EPSILON = 0.00000001
	if x-y < EPSILON && y-x < EPSILON {
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
			y := *j2.(*jsonLiteral)
			// false is less than true.
			cmp = int(y) - int(x)
		case jsonDouble:
			y := *j2.(*jsonDouble)
			cmp = compareFloat64PrecisionLoss(float64(x), float64(y))
		case jsonString:
			y := *j2.(*jsonString)
			cmp = strings.Compare(string(x), string(y))
		case jsonArray:
			y := *j2.(*jsonArray)
			for i := 0; i < len(x) && i < len(y); i++ {
				elem1 := x[i]
				elem2 := y[i]
				cmp, _ = CompareJSON(elem1, elem2)
				if cmp != 0 {
					break
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
		var x, y float64
		if precedence1 == -7 {
			x = float64(j1.(jsonLiteral))
			y = float64(*j2.(*jsonDouble))
		} else {
			x = float64(j1.(jsonDouble))
			y = float64(*j2.(*jsonLiteral))
		}
		cmp = compareFloat64PrecisionLoss(x, y)
	} else {
		cmp = precedence1 - precedence2
	}
	return
}
