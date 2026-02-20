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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// floatEpsilon is the acceptable error quantity when comparing two float numbers.
const floatEpsilon = 1.e-8

// compareFloat64PrecisionLoss returns an integer comparing the float64 x to y,
// allowing precision loss.
func compareFloat64PrecisionLoss(x, y float64) int {
	if x-y < floatEpsilon && y-x < floatEpsilon {
		return 0
	} else if x-y < 0 {
		return -1
	}
	return 1
}

func compareInt64(x int64, y int64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func compareFloat64(x float64, y float64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func compareUint64(x uint64, y uint64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func compareInt64Uint64(x int64, y uint64) int {
	if x < 0 {
		return -1
	}
	return compareUint64(uint64(x), y)
}

func compareFloat64Int64(x float64, y int64) int {
	return compareFloat64PrecisionLoss(x, float64(y))
}

func compareFloat64Uint64(x float64, y uint64) int {
	return compareFloat64PrecisionLoss(x, float64(y))
}

// CompareBinaryJSON compares two binary json objects. Returns -1 if left < right,
// 0 if left == right, else returns 1.
func CompareBinaryJSON(left, right BinaryJSON) int {
	precedence1 := jsonTypePrecedences[left.Type()]
	precedence2 := jsonTypePrecedences[right.Type()]
	var cmp int
	if precedence1 == precedence2 {
		if precedence1 == jsonTypePrecedences["NULL"] {
			// for JSON null.
			cmp = 0
		}
		switch left.TypeCode {
		case JSONTypeCodeLiteral:
			// false is less than true.
			cmp = int(right.Value[0]) - int(left.Value[0])
		case JSONTypeCodeInt64:
			switch right.TypeCode {
			case JSONTypeCodeInt64:
				cmp = compareInt64(left.GetInt64(), right.GetInt64())
			case JSONTypeCodeUint64:
				cmp = compareInt64Uint64(left.GetInt64(), right.GetUint64())
			case JSONTypeCodeFloat64:
				cmp = -compareFloat64Int64(right.GetFloat64(), left.GetInt64())
			}
		case JSONTypeCodeUint64:
			switch right.TypeCode {
			case JSONTypeCodeInt64:
				cmp = -compareInt64Uint64(right.GetInt64(), left.GetUint64())
			case JSONTypeCodeUint64:
				cmp = compareUint64(left.GetUint64(), right.GetUint64())
			case JSONTypeCodeFloat64:
				cmp = -compareFloat64Uint64(right.GetFloat64(), left.GetUint64())
			}
		case JSONTypeCodeFloat64:
			switch right.TypeCode {
			case JSONTypeCodeInt64:
				cmp = compareFloat64Int64(left.GetFloat64(), right.GetInt64())
			case JSONTypeCodeUint64:
				cmp = compareFloat64Uint64(left.GetFloat64(), right.GetUint64())
			case JSONTypeCodeFloat64:
				cmp = compareFloat64(left.GetFloat64(), right.GetFloat64())
			}
		case JSONTypeCodeString:
			cmp = bytes.Compare(left.GetString(), right.GetString())
		case JSONTypeCodeArray:
			leftCount := left.GetElemCount()
			rightCount := right.GetElemCount()
			for i := 0; i < leftCount && i < rightCount; i++ {
				elem1 := left.ArrayGetElem(i)
				elem2 := right.ArrayGetElem(i)
				cmp = CompareBinaryJSON(elem1, elem2)
				if cmp != 0 {
					return cmp
				}
			}
			cmp = leftCount - rightCount
		case JSONTypeCodeObject:
			// reference:
			// https://github.com/mysql/mysql-server/blob/ee4455a33b10f1b1886044322e4893f587b319ed/sql/json_dom.cc#L2561
			leftCount, rightCount := left.GetElemCount(), right.GetElemCount()
			cmp := compareInt64(int64(leftCount), int64(rightCount))
			if cmp != 0 {
				return cmp
			}
			for i := range leftCount {
				leftKey, rightKey := left.objectGetKey(i), right.objectGetKey(i)
				cmp = bytes.Compare(leftKey, rightKey)
				if cmp != 0 {
					return cmp
				}
				cmp = CompareBinaryJSON(left.objectGetVal(i), right.objectGetVal(i))
				if cmp != 0 {
					return cmp
				}
			}
		case JSONTypeCodeOpaque:
			cmp = bytes.Compare(left.GetOpaque().Buf, right.GetOpaque().Buf)
		case JSONTypeCodeDate, JSONTypeCodeDatetime, JSONTypeCodeTimestamp:
			// the jsonTypePrecedences guarantees that the DATE is only
			// comparable with the DATE, and the DATETIME and TIMESTAMP will compare with each other
			// as the `Type()` of `JSONTypeCodeTimestamp` is also `DATETIME`.
			leftTime := left.GetTime()
			rightTime := right.GetTime()
			cmp = leftTime.Compare(rightTime)
		case JSONTypeCodeDuration:
			leftDuration := left.GetDuration()
			rightDuration := right.GetDuration()
			cmp = leftDuration.Compare(rightDuration)
		}
	} else {
		cmp = precedence1 - precedence2
		if cmp > 0 {
			cmp = 1
		} else if cmp < 0 {
			cmp = -1
		}
	}
	return cmp
}

// MergePatchBinaryJSON implements RFC7396
// https://datatracker.ietf.org/doc/html/rfc7396
func MergePatchBinaryJSON(bjs []*BinaryJSON) (*BinaryJSON, error) {
	var err error
	length := len(bjs)

	// according to the implements of RFC7396
	// when the last item is not object
	// we can return the last item directly
	for i := length - 1; i >= 0; i-- {
		if bjs[i] == nil || bjs[i].TypeCode != JSONTypeCodeObject {
			bjs = bjs[i:]
			break
		}
	}

	target := bjs[0]
	for _, patch := range bjs[1:] {
		target, err = mergePatchBinaryJSON(target, patch)
		if err != nil {
			return nil, err
		}
	}
	return target, nil
}

func mergePatchBinaryJSON(target, patch *BinaryJSON) (result *BinaryJSON, err error) {
	if patch == nil {
		return nil, nil
	}

	if patch.TypeCode == JSONTypeCodeObject {
		if target == nil {
			return nil, nil
		}

		keyValMap := make(map[string]BinaryJSON)
		if target.TypeCode == JSONTypeCodeObject {
			elemCount := target.GetElemCount()
			for i := range elemCount {
				key := target.objectGetKey(i)
				val := target.objectGetVal(i)
				keyValMap[string(key)] = val
			}
		}
		var tmp *BinaryJSON
		elemCount := patch.GetElemCount()
		for i := range elemCount {
			key := patch.objectGetKey(i)
			val := patch.objectGetVal(i)
			k := string(key)

			targetKV, exists := keyValMap[k]
			if val.TypeCode == JSONTypeCodeLiteral && val.Value[0] == JSONLiteralNil {
				if exists {
					delete(keyValMap, k)
				}
			} else {
				tmp, err = mergePatchBinaryJSON(&targetKV, &val)
				if err != nil {
					return result, err
				}

				keyValMap[k] = *tmp
			}
		}

		length := len(keyValMap)
		keys := make([][]byte, 0, length)
		for key := range keyValMap {
			keys = append(keys, []byte(key))
		}
		slices.SortFunc(keys, bytes.Compare)
		length = len(keys)
		values := make([]BinaryJSON, 0, len(keys))
		for i := range length {
			values = append(values, keyValMap[string(keys[i])])
		}

		binaryObject, e := buildBinaryJSONObject(keys, values)
		if e != nil {
			return nil, e
		}
		return &binaryObject, nil
	}
	return patch, nil
}

// MergeBinaryJSON merges multiple BinaryJSON into one according the following rules:
// 1) adjacent arrays are merged to a single array;
// 2) adjacent object are merged to a single object;
// 3) a scalar value is autowrapped as an array before merge;
// 4) an adjacent array and object are merged by autowrapping the object as an array.
func MergeBinaryJSON(bjs []BinaryJSON) BinaryJSON {
	var remain = bjs
	var objects []BinaryJSON
	var results []BinaryJSON
	for len(remain) > 0 {
		if remain[0].TypeCode != JSONTypeCodeObject {
			results = append(results, remain[0])
			remain = remain[1:]
		} else {
			objects, remain = getAdjacentObjects(remain)
			results = append(results, mergeBinaryObject(objects))
		}
	}
	if len(results) == 1 {
		return results[0]
	}
	return mergeBinaryArray(results)
}

func getAdjacentObjects(bjs []BinaryJSON) (objects, remain []BinaryJSON) {
	for i := range bjs {
		if bjs[i].TypeCode != JSONTypeCodeObject {
			return bjs[:i], bjs[i:]
		}
	}
	return bjs, nil
}

func mergeBinaryArray(elems []BinaryJSON) BinaryJSON {
	buf := make([]BinaryJSON, 0, len(elems))
	for i := range elems {
		elem := elems[i]
		if elem.TypeCode != JSONTypeCodeArray {
			buf = append(buf, elem)
		} else {
			childCount := elem.GetElemCount()
			for j := range childCount {
				buf = append(buf, elem.ArrayGetElem(j))
			}
		}
	}
	return buildBinaryJSONArray(buf)
}

func mergeBinaryObject(objects []BinaryJSON) BinaryJSON {
	keyValMap := make(map[string]BinaryJSON)
	keys := make([][]byte, 0, len(keyValMap))
	for _, obj := range objects {
		elemCount := obj.GetElemCount()
		for i := range elemCount {
			key := obj.objectGetKey(i)
			val := obj.objectGetVal(i)
			if old, ok := keyValMap[string(key)]; ok {
				keyValMap[string(key)] = MergeBinaryJSON([]BinaryJSON{old, val})
			} else {
				keyValMap[string(key)] = val
				keys = append(keys, key)
			}
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	values := make([]BinaryJSON, len(keys))
	for i, key := range keys {
		values[i] = keyValMap[string(key)]
	}
	binaryObject, err := buildBinaryJSONObject(keys, values)
	if err != nil {
		panic("mergeBinaryObject should never panic, please contact the TiDB team for help")
	}
	return binaryObject
}

// PeekBytesAsJSON trys to peek some bytes from b, until
// we can deserialize a JSON from those bytes.
func PeekBytesAsJSON(b []byte) (n int, err error) {
	if len(b) <= 0 {
		err = errors.New("Cant peek from empty bytes")
		return
	}
	switch c := b[0]; c {
	case JSONTypeCodeObject, JSONTypeCodeArray:
		if len(b) >= valTypeSize+headerSize {
			size := jsonEndian.Uint32(b[valTypeSize+dataSizeOff:])
			n = valTypeSize + int(size)
			return
		}
	case JSONTypeCodeString:
		strLen, lenLen := binary.Uvarint(b[valTypeSize:])
		return valTypeSize + int(strLen) + lenLen, nil
	case JSONTypeCodeInt64, JSONTypeCodeUint64, JSONTypeCodeFloat64, JSONTypeCodeDate, JSONTypeCodeDatetime, JSONTypeCodeTimestamp:
		n = valTypeSize + 8
		return
	case JSONTypeCodeLiteral:
		n = valTypeSize + 1
		return
	case JSONTypeCodeOpaque:
		bufLen, lenLen := binary.Uvarint(b[valTypeSize+1:])
		return valTypeSize + 1 + int(bufLen) + lenLen, nil
	case JSONTypeCodeDuration:
		n = valTypeSize + 12
		return
	}
	err = errors.New("Invalid JSON bytes")
	return
}

// ContainsBinaryJSON check whether JSON document contains specific target according the following rules:
// 1) object contains a target object if and only if every key is contained in source object and the value associated with the target key is contained in the value associated with the source key;
// 2) array contains a target nonarray if and only if the target is contained in some element of the array;
// 3) array contains a target array if and only if every element is contained in some element of the array;
// 4) scalar contains a target scalar if and only if they are comparable and are equal;
func ContainsBinaryJSON(obj, target BinaryJSON) bool {
	switch obj.TypeCode {
	case JSONTypeCodeObject:
		if target.TypeCode == JSONTypeCodeObject {
			elemCount := target.GetElemCount()
			for i := range elemCount {
				key := target.objectGetKey(i)
				val := target.objectGetVal(i)
				if exp, exists := obj.objectSearchKey(key); !exists || !ContainsBinaryJSON(exp, val) {
					return false
				}
			}
			return true
		}
		return false
	case JSONTypeCodeArray:
		if target.TypeCode == JSONTypeCodeArray {
			elemCount := target.GetElemCount()
			for i := range elemCount {
				if !ContainsBinaryJSON(obj, target.ArrayGetElem(i)) {
					return false
				}
			}
			return true
		}
		elemCount := obj.GetElemCount()
		for i := range elemCount {
			if ContainsBinaryJSON(obj.ArrayGetElem(i), target) {
				return true
			}
		}
		return false
	default:
		return CompareBinaryJSON(obj, target) == 0
	}
}

// OverlapsBinaryJSON is similar with ContainsBinaryJSON, but it checks the `OR` relationship.
func OverlapsBinaryJSON(obj, target BinaryJSON) bool {
	if obj.TypeCode != JSONTypeCodeArray && target.TypeCode == JSONTypeCodeArray {
		obj, target = target, obj
	}
	switch obj.TypeCode {
	case JSONTypeCodeObject:
		if target.TypeCode == JSONTypeCodeObject {
			elemCount := target.GetElemCount()
			for i := range elemCount {
				key := target.objectGetKey(i)
				val := target.objectGetVal(i)
				if exp, exists := obj.objectSearchKey(key); exists && CompareBinaryJSON(exp, val) == 0 {
					return true
				}
			}
		}
		return false
	case JSONTypeCodeArray:
		if target.TypeCode == JSONTypeCodeArray {
			for i := range obj.GetElemCount() {
				o := obj.ArrayGetElem(i)
				for j := range target.GetElemCount() {
					if CompareBinaryJSON(o, target.ArrayGetElem(j)) == 0 {
						return true
					}
				}
			}
			return false
		}
		elemCount := obj.GetElemCount()
		for i := range elemCount {
			if CompareBinaryJSON(obj.ArrayGetElem(i), target) == 0 {
				return true
			}
		}
		return false
	default:
		return CompareBinaryJSON(obj, target) == 0
	}
}

// GetElemDepth for JSON_DEPTH
// Returns the maximum depth of a JSON document
// rules referenced by MySQL JSON_DEPTH function
// [https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-depth]
// 1) An empty array, empty object, or scalar value has depth 1.
// 2) A nonempty array containing only elements of depth 1 or nonempty object containing only member values of depth 1 has depth 2.
// 3) Otherwise, a JSON document has depth greater than 2.
// e.g. depth of '{}', '[]', 'true': 1
// e.g. depth of '[10, 20]', '[[], {}]': 2
// e.g. depth of '[10, {"a": 20}]': 3
func (bj BinaryJSON) GetElemDepth() int {
	switch bj.TypeCode {
	case JSONTypeCodeObject:
		elemCount := bj.GetElemCount()
		maxDepth := 0
		for i := range elemCount {
			obj := bj.objectGetVal(i)
			depth := obj.GetElemDepth()
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return maxDepth + 1
	case JSONTypeCodeArray:
		elemCount := bj.GetElemCount()
		maxDepth := 0
		for i := range elemCount {
			obj := bj.ArrayGetElem(i)
			depth := obj.GetElemDepth()
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return maxDepth + 1
	default:
		return 1
	}
}

// Search for JSON_Search
// rules referenced by MySQL JSON_SEARCH function
// [https://dev.mysql.com/doc/refman/5.7/en/json-search-functions.html#function_json-search]
func (bj BinaryJSON) Search(containType string, search string, escape byte, pathExpres []JSONPathExpression) (res BinaryJSON, isNull bool, err error) {
	if containType != JSONContainsPathOne && containType != JSONContainsPathAll {
		return res, true, ErrJSONBadOneOrAllArg.GenWithStackByArgs("json_search")
	}
	patChars, patTypes := stringutil.CompilePattern(search, escape)

	result := make([]any, 0)
	walkFn := func(fullpath JSONPathExpression, bj BinaryJSON) (stop bool, err error) {
		if bj.TypeCode == JSONTypeCodeString && stringutil.DoMatch(string(bj.GetString()), patChars, patTypes) {
			result = append(result, fullpath.String())
			if containType == JSONContainsPathOne {
				return true, nil
			}
		}
		return false, nil
	}
	if len(pathExpres) != 0 {
		err := bj.Walk(walkFn, pathExpres...)
		if err != nil {
			return res, true, err
		}
	} else {
		err := bj.Walk(walkFn)
		if err != nil {
			return res, true, err
		}
	}
	switch len(result) {
	case 0:
		return res, true, nil
	case 1:
		return CreateBinaryJSON(result[0]), false, nil
	default:
		return CreateBinaryJSON(result), false, nil
	}
}

// extractCallbackFn the type of CALLBACK function for extractToCallback
type extractCallbackFn func(fullpath JSONPathExpression, bj BinaryJSON) (stop bool, err error)

// extractToCallback callback alternative of extractTo
//
//	would be more effective when walk through the whole JSON is unnecessary
//
// NOTICE: path [0] & [*] for JSON object other than array is INVALID, which is different from extractTo.
func (bj BinaryJSON) extractToCallback(pathExpr JSONPathExpression, callbackFn extractCallbackFn, fullpath JSONPathExpression) (stop bool, err error) {
	if len(pathExpr.legs) == 0 {
		return callbackFn(fullpath, bj)
	}

	currentLeg, subPathExpr := pathExpr.popOneLeg()
	if currentLeg.typ == jsonPathLegArraySelection && bj.TypeCode == JSONTypeCodeArray {
		elemCount := bj.GetElemCount()
		switch selection := currentLeg.arraySelection.(type) {
		case jsonPathArraySelectionAsterisk:
			for i := range elemCount {
				// buf = bj.ArrayGetElem(i).extractTo(buf, subPathExpr)
				path := fullpath.pushBackOneArraySelectionLeg(jsonPathArraySelectionIndex{jsonPathArrayIndexFromStart(i)})
				stop, err = bj.ArrayGetElem(i).extractToCallback(subPathExpr, callbackFn, path)
				if stop || err != nil {
					return
				}
			}
		case jsonPathArraySelectionIndex:
			idx := selection.index.getIndexFromStart(bj)
			if idx < elemCount && idx >= 0 {
				// buf = bj.ArrayGetElem(currentLeg.arraySelection).extractTo(buf, subPathExpr)
				path := fullpath.pushBackOneArraySelectionLeg(currentLeg.arraySelection)
				stop, err = bj.ArrayGetElem(idx).extractToCallback(subPathExpr, callbackFn, path)
				if stop || err != nil {
					return
				}
			}
		case jsonPathArraySelectionRange:
			start := selection.start.getIndexFromStart(bj)
			end := selection.end.getIndexFromStart(bj)
			if end >= elemCount {
				end = elemCount - 1
			}
			if start <= end && start >= 0 {
				for i := start; i <= end; i++ {
					path := fullpath.pushBackOneArraySelectionLeg(jsonPathArraySelectionIndex{jsonPathArrayIndexFromStart(i)})
					stop, err = bj.ArrayGetElem(i).extractToCallback(subPathExpr, callbackFn, path)
					if stop || err != nil {
						return
					}
				}
			}
		}
	} else if currentLeg.typ == jsonPathLegKey && bj.TypeCode == JSONTypeCodeObject {
		elemCount := bj.GetElemCount()
		if currentLeg.dotKey == "*" {
			for i := range elemCount {
				// buf = bj.objectGetVal(i).extractTo(buf, subPathExpr)
				path := fullpath.pushBackOneKeyLeg(string(bj.objectGetKey(i)))
				stop, err = bj.objectGetVal(i).extractToCallback(subPathExpr, callbackFn, path)
				if stop || err != nil {
					return
				}
			}
		} else {
			child, ok := bj.objectSearchKey(hack.Slice(currentLeg.dotKey))
			if ok {
				// buf = child.extractTo(buf, subPathExpr)
				path := fullpath.pushBackOneKeyLeg(currentLeg.dotKey)
				stop, err = child.extractToCallback(subPathExpr, callbackFn, path)
				if stop || err != nil {
					return
				}
			}
		}
	} else if currentLeg.typ == jsonPathLegDoubleAsterisk {
		// buf = bj.extractTo(buf, subPathExpr)
		stop, err = bj.extractToCallback(subPathExpr, callbackFn, fullpath)
		if stop || err != nil {
			return
		}

		if bj.TypeCode == JSONTypeCodeArray {
			elemCount := bj.GetElemCount()
			for i := range elemCount {
				// buf = bj.ArrayGetElem(i).extractTo(buf, pathExpr)
				path := fullpath.pushBackOneArraySelectionLeg(jsonPathArraySelectionIndex{jsonPathArrayIndexFromStart(i)})
				stop, err = bj.ArrayGetElem(i).extractToCallback(pathExpr, callbackFn, path)
				if stop || err != nil {
					return
				}
			}
		} else if bj.TypeCode == JSONTypeCodeObject {
			elemCount := bj.GetElemCount()
			for i := range elemCount {
				// buf = bj.objectGetVal(i).extractTo(buf, pathExpr)
				path := fullpath.pushBackOneKeyLeg(string(bj.objectGetKey(i)))
				stop, err = bj.objectGetVal(i).extractToCallback(pathExpr, callbackFn, path)
				if stop || err != nil {
					return
				}
			}
		}
	}
	return false, nil
}

// BinaryJSONWalkFunc is used as callback function for BinaryJSON.Walk
type BinaryJSONWalkFunc func(fullpath JSONPathExpression, bj BinaryJSON) (stop bool, err error)

// Walk traverse BinaryJSON objects
func (bj BinaryJSON) Walk(walkFn BinaryJSONWalkFunc, pathExprList ...JSONPathExpression) (err error) {
	pathSet := make(map[string]bool)

	var doWalk extractCallbackFn
	doWalk = func(fullpath JSONPathExpression, bj BinaryJSON) (stop bool, err error) {
		pathStr := fullpath.String()
		if _, ok := pathSet[pathStr]; ok {
			return false, nil
		}

		stop, err = walkFn(fullpath, bj)
		pathSet[pathStr] = true
		if stop || err != nil {
			return
		}

		if bj.TypeCode == JSONTypeCodeArray {
			elemCount := bj.GetElemCount()
			for i := range elemCount {
				path := fullpath.pushBackOneArraySelectionLeg(jsonPathArraySelectionIndex{jsonPathArrayIndexFromStart(i)})
				stop, err = doWalk(path, bj.ArrayGetElem(i))
				if stop || err != nil {
					return
				}
			}
		} else if bj.TypeCode == JSONTypeCodeObject {
			elemCount := bj.GetElemCount()
			for i := range elemCount {
				path := fullpath.pushBackOneKeyLeg(string(bj.objectGetKey(i)))
				stop, err = doWalk(path, bj.objectGetVal(i))
				if stop || err != nil {
					return
				}
			}
		}
		return false, nil
	}

	fullpath := JSONPathExpression{legs: make([]jsonPathLeg, 0, 32), flags: jsonPathExpressionFlag(0)}
	if len(pathExprList) > 0 {
		for _, pathExpr := range pathExprList {
			var stop bool
			stop, err = bj.extractToCallback(pathExpr, doWalk, fullpath)
			if stop || err != nil {
				return err
			}
		}
	} else {
		_, err = doWalk(fullpath, bj)
		if err != nil {
			return
		}
	}
	return nil
}
