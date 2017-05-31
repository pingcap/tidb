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

import "fmt"

// Type returns type of JSON as string.
func (j JSON) Type() string {
	switch j.typeCode {
	case typeCodeObject:
		return "OBJECT"
	case typeCodeArray:
		return "ARRAY"
	case typeCodeLiteral:
		switch byte(j.i64) {
		case jsonLiteralNil:
			return "NULL"
		default:
			return "BOOLEAN"
		}
	case typeCodeInt64:
		return "INTEGER"
	case typeCodeFloat64:
		return "DOUBLE"
	case typeCodeString:
		return "STRING"
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, j.typeCode)
		panic(msg)
	}
}

// Extract receives several path expressions as arguments, matches them in j, and returns:
//  ret: target JSON matched any path expressions. maybe autowrapped as an array.
//  found: true if any path expressions matched.
func (j JSON) Extract(pathExprList []PathExpression) (ret JSON, found bool) {
	elemList := make([]JSON, 0, len(pathExprList))
	for _, pathExpr := range pathExprList {
		elemList = append(elemList, extract(j, pathExpr)...)
	}
	if len(elemList) == 0 {
		found = false
	} else if len(pathExprList) == 1 && len(elemList) == 1 {
		// If pathExpr contains asterisks, len(elemList) won't be 1
		// even if len(pathExprList) equals to 1.
		found = true
		ret = elemList[0]
	} else {
		found = true
		ret.typeCode = typeCodeArray
		ret.array = append(ret.array, elemList...)
	}
	return
}

// Unquote is for JSON_UNQUOTE.
func (j JSON) Unquote() string {
	switch j.typeCode {
	case typeCodeString:
		return j.str
	default:
		return j.String()
	}
}

// extract is used by Extract.
// NOTE: the return value will share something with j.
func extract(j JSON, pathExpr PathExpression) (ret []JSON) {
	if len(pathExpr.legs) == 0 {
		return []JSON{j}
	}
	var currentLeg = pathExpr.legs[0]
	pathExpr.legs = pathExpr.legs[1:]
	if currentLeg.isArrayIndex && j.typeCode == typeCodeArray {
		if currentLeg.arrayIndex == arrayIndexAsterisk {
			for _, child := range j.array {
				ret = append(ret, extract(child, pathExpr)...)
			}
		} else if currentLeg.arrayIndex < len(j.array) {
			childRet := extract(j.array[currentLeg.arrayIndex], pathExpr)
			ret = append(ret, childRet...)
		}
	} else if !currentLeg.isArrayIndex && j.typeCode == typeCodeObject {
		if len(currentLeg.dotKey) == 1 && currentLeg.dotKey[0] == '*' {
			var sortedKeys = getSortedKeys(j.object) // iterate over sorted keys.
			for _, child := range sortedKeys {
				ret = append(ret, extract(j.object[child], pathExpr)...)
			}
		} else if child, ok := j.object[currentLeg.dotKey]; ok {
			childRet := extract(child, pathExpr)
			ret = append(ret, childRet...)
		}
	}
	return
}
