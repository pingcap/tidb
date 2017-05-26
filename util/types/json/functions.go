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

	"github.com/juju/errors"
)

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

// Merge merges suffixes into j according the following rules:
// 1) adjacent arrays are merged to a single array;
// 2) adjacent object are merged to a single object;
// 3) a scalar value is autowrapped as an array before merge;
// 4) an adjacent array and object are merged by autowrapping the object as an array.
func (j *JSON) Merge(suffixes ...JSON) {
	switch j.typeCode {
	case typeCodeArray, typeCodeObject:
	default:
		firstElem := *j
		*j = CreateJSON(nil)
		j.typeCode = typeCodeArray
		j.array = []JSON{firstElem}
	}
	for i := 0; i < len(suffixes); i++ {
		suffix := suffixes[i]
		switch j.typeCode {
		case typeCodeArray:
			if suffix.typeCode == typeCodeArray {
				// rule (1)
				for _, elem := range suffix.array {
					j.array = append(j.array, elem)
				}
			} else {
				// rule (3), (4)
				j.array = append(j.array, suffix)
			}
		case typeCodeObject:
			if suffix.typeCode == typeCodeObject {
				// rule (2)
				for key := range suffix.object {
					j.object[key] = suffix.object[key]
				}
			} else {
				// rule (4)
				firstElem := *j
				*j = CreateJSON(nil)
				j.typeCode = typeCodeArray
				j.array = []JSON{firstElem}
				i--
			}
		}
	}
	return
}

// Set inserts or updates data in j. All path expressions cannot contains * or ** wildcard.
func (j *JSON) Set(pathExprs []string, values []JSON) (err error) {
	if len(pathExprs) != len(values) {
		// TODO should return 1582(42000)
		return errors.New("Incorrect parameter count")
	}
	for i := 0; i < len(pathExprs); i++ {
		pathExpr := pathExprs[i]
		value := values[i]
		if err = set(j, pathExpr, value); err != nil {
			return errors.Trace(err)
		}
	}
	return
}

// Extract receives several path expressions as arguments, matches them in j, and returns:
//  ret: target JSON matched any path expressions. maybe autowrapped as an array.
//  found: true if any path expressions matched.
func (j JSON) Extract(pathExprList ...string) (ret JSON, found bool, err error) {
	elemList := make([]JSON, 0, len(pathExprList))
	for _, pathExpr := range pathExprList {
		elem, elemFound, elemErr := extract(j, pathExpr)
		if elemErr != nil {
			err = errors.Trace(elemErr)
			return
		}
		if elemFound {
			elemList = append(elemList, elem)
			found = true
		}
	}
	if len(elemList) == 0 {
		return
	} else if len(pathExprList) == 1 {
		ret = elemList[0]
	} else {
		ret.typeCode = typeCodeArray
		for _, elem := range elemList {
			ret.array = append(ret.array, elem)
		}
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
func extract(j JSON, pathExpr string) (ret JSON, found bool, err error) {
	var indices [][]int
	if indices, err = validateJSONPathExpr(pathExpr); err != nil {
		err = errors.Trace(err)
		return
	}

	ret = j
	for _, indice := range indices {
		found = false
		leg := pathExpr[indice[0]:indice[1]]
		switch leg[0] {
		case '[':
			index, atoiErr := strconv.Atoi(string(leg[1 : len(leg)-1]))
			if atoiErr != nil {
				err = errors.Trace(atoiErr)
				return
			}
			if ret.typeCode == typeCodeArray && len(ret.array) > index {
				ret = ret.array[index]
				found = true
			}
		case '.':
			key := string(leg[1:])
			if ret.typeCode == typeCodeObject {
				ret, found = ret.object[key]
			}
		}
		if !found {
			break
		}
	}
	return
}

func set(j *JSON, pathExpr string, value JSON) (err error) {
}
