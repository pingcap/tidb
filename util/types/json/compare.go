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

	"github.com/ngaut/log"
)

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
	log.Errorf("compare two j1: %v, j2: %v\n", j1, j2)
	precedence1 := jsonTypePrecedences[j1.Type()]
	precedence2 := jsonTypePrecedences[j2.Type()]
	if precedence1 == precedence2 {
		switch x := j1.(type) {
		case jsonLiteral:
			y := j2.(jsonLiteral)
			if x == y {
				cmp = 0
			} else {
				// false is less than true.
				cmp = int(y - x)
			}
		case jsonDouble:
			y := j2.(*jsonDouble)
			cmp = int(x - *y)
		case jsonString:
			y := j2.(jsonString)
			cmp = strings.Compare(string(x), string(y))
		case jsonArray:
			y := j2.(jsonArray)
			var minLen int
			if len(x) < len(y) {
				minLen = len(x)
			} else {
				minLen = len(y)
			}
			for i := 0; i < minLen; i++ {
				elem1 := x[i]
				elem2 := y[i]
				cmp, _ = CompareJSON(elem1, elem2)
				if cmp != 0 {
					break
				}
			}
			if len(x) == len(y) {
				cmp = 0
			}
			if len(x) == minLen {
				cmp = -1
			} else {
				cmp = 1
			}
		case jsonObject:
			// only equal is defined on two json objects.
			// larger and smaller are not defined.
			s1 := Serialize(x)
			s2 := Serialize(j2)
			cmp = bytes.Compare(s1, s2)
		default:
			cmp = 0
		}
	} else {
		cmp = precedence1 - precedence2
	}
	return
}
