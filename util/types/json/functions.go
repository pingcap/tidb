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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"unicode/utf8"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/hack"
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
func (j JSON) Unquote() (string, error) {
	switch j.typeCode {
	case typeCodeString:
		return unquoteString(j.str)
	default:
		return j.String(), nil
	}
}

// unquoteString recognizes the escape sequences shown in:
// https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#json-unquote-character-escape-sequences
func unquoteString(s string) (string, error) {
	ret := new(bytes.Buffer)
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			if i == len(s) {
				return "", errors.New("Missing a closing quotation mark in string")
			}
			switch s[i] {
			case '"':
				ret.WriteByte('"')
			case 'b':
				ret.WriteByte('\b')
			case 'f':
				ret.WriteByte('\f')
			case 'n':
				ret.WriteByte('\n')
			case 'r':
				ret.WriteByte('\r')
			case 't':
				ret.WriteByte('\t')
			case '\\':
				ret.WriteByte('\\')
			case 'u':
				if i+5 > len(s) {
					return "", errors.Errorf("Invalid unicode: %s", s[i+1:])
				}
				char, size, err := decodeEscapedUnicode(hack.Slice(s[i+1 : i+5]))
				if err != nil {
					return "", errors.Trace(err)
				}
				ret.Write(char[0:size])
				i += 5
			default:
				// For all other escape sequences, backslash is ignored.
				ret.WriteByte(s[i])
			}
		} else {
			ret.WriteByte(s[i])
		}
	}
	return ret.String(), nil
}

// decodeEscapedUnicode decodes unicode into utf8 bytes specified in RFC 3629.
// According RFC 3629, the max length of utf8 characters is 4 bytes.
// And MySQL use 4 bytes to represent the unicode which must be in [0, 65536).
func decodeEscapedUnicode(s []byte) (char [4]byte, size int, err error) {
	size, err = hex.Decode(char[0:2], s)
	if err != nil || size != 2 {
		// The unicode must can be represented in 2 bytes.
		return char, 0, errors.Trace(err)
	}
	var unicode uint16
	binary.Read(bytes.NewReader(char[0:2]), binary.BigEndian, &unicode)
	size = utf8.RuneLen(rune(unicode))
	utf8.EncodeRune(char[0:size], rune(unicode))
	return
}

// extract is used by Extract.
// NOTE: the return value will share something with j.
func extract(j JSON, pathExpr PathExpression) (ret []JSON) {
	if len(pathExpr.legs) == 0 {
		return []JSON{j}
	}
	currentLeg, subPathExpr := pathExpr.popOneLeg()
	if currentLeg.typ == pathLegIndex {
		// If j is not an array, autowrap that into array.
		if j.typeCode != typeCodeArray {
			j = autoWrapAsArray(j, 1)
		}
		if currentLeg.arrayIndex == arrayIndexAsterisk {
			for _, child := range j.array {
				ret = append(ret, extract(child, subPathExpr)...)
			}
		} else if currentLeg.arrayIndex < len(j.array) {
			childRet := extract(j.array[currentLeg.arrayIndex], subPathExpr)
			ret = append(ret, childRet...)
		}
	} else if currentLeg.typ == pathLegKey && j.typeCode == typeCodeObject {
		if len(currentLeg.dotKey) == 1 && currentLeg.dotKey[0] == '*' {
			var sortedKeys = getSortedKeys(j.object) // iterate over sorted keys.
			for _, child := range sortedKeys {
				ret = append(ret, extract(j.object[child], subPathExpr)...)
			}
		} else if child, ok := j.object[currentLeg.dotKey]; ok {
			childRet := extract(child, subPathExpr)
			ret = append(ret, childRet...)
		}
	} else if currentLeg.typ == pathLegDoubleAsterisk {
		ret = append(ret, extract(j, subPathExpr)...)
		if j.typeCode == typeCodeArray {
			for _, child := range j.array {
				ret = append(ret, extract(child, pathExpr)...)
			}
		} else if j.typeCode == typeCodeObject {
			var sortedKeys = getSortedKeys(j.object)
			for _, child := range sortedKeys {
				ret = append(ret, extract(j.object[child], pathExpr)...)
			}
		}
	}
	return
}

// autoWrapAsArray wraps input JSON into an array if needed.
func autoWrapAsArray(j JSON, hintLength int) JSON {
	jnew := CreateJSON(nil)
	jnew.typeCode = typeCodeArray
	jnew.array = make([]JSON, 0, hintLength)
	jnew.array = append(jnew.array, j)
	return jnew
}

// Merge merges suffixes into j according the following rules:
// 1) adjacent arrays are merged to a single array;
// 2) adjacent object are merged to a single object;
// 3) a scalar value is autowrapped as an array before merge;
// 4) an adjacent array and object are merged by autowrapping the object as an array.
func (j JSON) Merge(suffixes []JSON) JSON {
	if j.typeCode != typeCodeArray && j.typeCode != typeCodeObject {
		j = autoWrapAsArray(j, len(suffixes)+1)
	}
	for i := 0; i < len(suffixes); i++ {
		suffix := suffixes[i]
		switch j.typeCode {
		case typeCodeArray:
			if suffix.typeCode == typeCodeArray {
				// rule (1)
				j.array = append(j.array, suffix.array...)
			} else {
				// rule (3), (4)
				j.array = append(j.array, suffix)
			}
		case typeCodeObject:
			if suffix.typeCode == typeCodeObject {
				// rule (2)
				for key := range suffix.object {
					if child, ok := j.object[key]; ok {
						j.object[key] = child.Merge([]JSON{suffix.object[key]})
					} else {
						j.object[key] = suffix.object[key]
					}
				}
			} else {
				// rule (4)
				j = autoWrapAsArray(j, len(suffixes)+1-i)
				if suffix.typeCode == typeCodeArray {
					j.array = append(j.array, suffix.array...)
				} else {
					j.array = append(j.array, suffix)
				}
			}
		}
	}
	return j
}

// ModifyType is for modify a JSON. There are three valid values:
// ModifyInsert, ModifyReplace and ModifySet.
type ModifyType byte

const (
	// ModifyInsert is for insert a new element into a JSON.
	ModifyInsert ModifyType = 0x01
	// ModifyReplace is for replace an old elemList from a JSON.
	ModifyReplace ModifyType = 0x02
	// ModifySet = ModifyInsert | ModifyReplace
	ModifySet ModifyType = 0x03
)

// Modify modifies a JSON object by insert, replace or set.
// All path expressions cannot contain * or ** wildcard.
// If any error occurs, the input won't be changed.
func (j JSON) Modify(pathExprList []PathExpression, values []JSON, mt ModifyType) (retj JSON, err error) {
	if len(pathExprList) != len(values) {
		// TODO should return 1582(42000)
		return retj, errors.New("Incorrect parameter count")
	}
	for _, pathExpr := range pathExprList {
		if pathExpr.flags.containsAnyAsterisk() {
			// TODO should return 3149(42000)
			return retj, errors.New("Invalid path expression")
		}
	}
	for i := 0; i < len(pathExprList); i++ {
		pathExpr, value := pathExprList[i], values[i]
		j = set(j, pathExpr, value, mt)
	}
	return j, nil
}

// set is for Modify. The result JSON maybe share something with input JSON.
func set(j JSON, pathExpr PathExpression, value JSON, mt ModifyType) JSON {
	if len(pathExpr.legs) == 0 {
		if mt&ModifyReplace != 0 {
			return value
		}
		return j
	}
	currentLeg, subPathExpr := pathExpr.popOneLeg()
	if currentLeg.typ == pathLegIndex {
		// If j is not an array, we should autowrap that as array.
		// Then if its length equals to 1, we unwrap it back.
		var shouldUnwrap = false
		if j.typeCode != typeCodeArray {
			j = autoWrapAsArray(j, 1)
			shouldUnwrap = true
		}
		var index = currentLeg.arrayIndex
		if len(j.array) > index {
			// e.g. json_replace('[1, 2, 3]', '$[0]', "x") => '["x", 2, 3]'
			j.array[index] = set(j.array[index], subPathExpr, value, mt)
		} else if len(subPathExpr.legs) == 0 && mt&ModifyInsert != 0 {
			// e.g. json_insert('[1, 2, 3]', '$[3]', "x") => '[1, 2, 3, "x"]'
			j.array = append(j.array, value)
		}
		if len(j.array) == 1 && shouldUnwrap {
			j = j.array[0]
		}
	} else if currentLeg.typ == pathLegKey && j.typeCode == typeCodeObject {
		var key = currentLeg.dotKey
		if child, ok := j.object[key]; ok {
			// e.g. json_replace('{"a": 1}', '$.a', 2) => '{"a": 2}'
			j.object[key] = set(child, subPathExpr, value, mt)
		} else if len(subPathExpr.legs) == 0 && mt&ModifyInsert != 0 {
			// e.g. json_insert('{"a": 1}', '$.b', 2) => '{"a": 1, "b": 2}'
			j.object[key] = value
		}
	}
	// For these cases, we just return the input JSON back without any change:
	// 1) we want to insert a new element, but the full path has already exists;
	// 2) we want to replace an old element, but the full path doesn't exist;
	// 3) we want to insert or replace something, but the path without last leg doesn't exist.
	return j
}
