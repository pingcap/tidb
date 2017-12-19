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
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/hack"
)

// Type returns type of BinaryJSON as string.
func (bj BinaryJSON) Type() string {
	switch bj.TypeCode {
	case TypeCodeObject:
		return "OBJECT"
	case TypeCodeArray:
		return "ARRAY"
	case TypeCodeLiteral:
		switch bj.Value[0] {
		case LiteralNil:
			return "NULL"
		default:
			return "BOOLEAN"
		}
	case TypeCodeInt64:
		return "INTEGER"
	case TypeCodeUint64:
		return "UNSIGNED INTEGER"
	case TypeCodeFloat64:
		return "DOUBLE"
	case TypeCodeString:
		return "STRING"
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, bj.TypeCode)
		panic(msg)
	}
}

// Unquote is for JSON_UNQUOTE.
func (bj BinaryJSON) Unquote() (string, error) {
	switch bj.TypeCode {
	case TypeCodeString:
		s, err := unquoteString(hack.String(bj.getString()))
		if err != nil {
			return "", errors.Trace(err)
		}
		// Remove prefix and suffix '"'.
		slen := len(s)
		if slen > 1 {
			head, tail := s[0], s[slen-1]
			if head == '"' && tail == '"' {
				return s[1 : slen-1], nil
			}
		}
		return s, nil
	default:
		return bj.String(), nil
	}
}

// Extract receives several path expressions as arguments, matches them in bj, and returns:
//  ret: target JSON matched any path expressions. maybe autowrapped as an array.
//  found: true if any path expressions matched.
func (bj BinaryJSON) Extract(pathExprList []PathExpression) (ret BinaryJSON, found bool) {
	buf := make([]BinaryJSON, 0, 1)
	for _, pathExpr := range pathExprList {
		buf = bj.extractTo(buf, pathExpr)
	}
	if len(buf) == 0 {
		found = false
	} else if len(pathExprList) == 1 && len(buf) == 1 {
		// If pathExpr contains asterisks, len(elemList) won't be 1
		// even if len(pathExprList) equals to 1.
		found = true
		ret = buf[0]
	} else {
		found = true
		ret = buildBinaryArray(buf)
	}
	return
}

func (bj BinaryJSON) extractTo(buf []BinaryJSON, pathExpr PathExpression) []BinaryJSON {
	if len(pathExpr.legs) == 0 {
		return append(buf, bj)
	}
	currentLeg, subPathExpr := pathExpr.popOneLeg()
	if currentLeg.typ == pathLegIndex {
		if bj.TypeCode != TypeCodeArray {
			if currentLeg.arrayIndex <= 0 {
				buf = bj.extractTo(buf, subPathExpr)
			}
			return buf
		}
		elemCount := bj.getElemCount()
		if currentLeg.arrayIndex == arrayIndexAsterisk {
			for i := 0; i < elemCount; i++ {
				buf = bj.arrayGetElem(i).extractTo(buf, subPathExpr)
			}
		} else if currentLeg.arrayIndex < elemCount {
			buf = bj.arrayGetElem(currentLeg.arrayIndex).extractTo(buf, subPathExpr)
		}
	} else if currentLeg.typ == pathLegKey && bj.TypeCode == TypeCodeObject {
		elemCount := bj.getElemCount()
		if currentLeg.dotKey == "*" {
			for i := 0; i < elemCount; i++ {
				buf = bj.objectGetVal(i).extractTo(buf, subPathExpr)
			}
		} else {
			child, ok := bj.objectSearchKey(hack.Slice(currentLeg.dotKey))
			if ok {
				buf = child.extractTo(buf, subPathExpr)
			}
		}
	} else if currentLeg.typ == pathLegDoubleAsterisk {
		buf = bj.extractTo(buf, subPathExpr)
		if bj.TypeCode == TypeCodeArray {
			elemCount := bj.getElemCount()
			for i := 0; i < elemCount; i++ {
				buf = bj.arrayGetElem(i).extractTo(buf, pathExpr)
			}
		} else if bj.TypeCode == TypeCodeObject {
			elemCount := bj.getElemCount()
			for i := 0; i < elemCount; i++ {
				buf = bj.objectGetVal(i).extractTo(buf, pathExpr)
			}
		}
	}
	return buf
}

func (bj BinaryJSON) objectSearchKey(key []byte) (BinaryJSON, bool) {
	elemCount := bj.getElemCount()
	idx := sort.Search(elemCount, func(i int) bool {
		return bytes.Compare(bj.objectGetKey(i), key) >= 0
	})
	if idx < elemCount && bytes.Compare(bj.objectGetKey(idx), key) == 0 {
		return bj.objectGetVal(idx), true
	}
	return BinaryJSON{}, false
}

func buildBinaryArray(elems []BinaryJSON) BinaryJSON {
	totalSize := headerSize + len(elems)*valEntrySize
	for _, elem := range elems {
		if elem.TypeCode != TypeCodeLiteral {
			totalSize += len(elem.Value)
		}
	}
	buf := make([]byte, headerSize+len(elems)*valEntrySize, totalSize)
	endian.PutUint32(buf, uint32(len(elems)))
	endian.PutUint32(buf[dataSizeOff:], uint32(totalSize))
	buf = buildBinaryElements(buf, headerSize, elems)
	return BinaryJSON{TypeCode: TypeCodeArray, Value: buf}
}

func buildBinaryElements(buf []byte, entryStart int, elems []BinaryJSON) []byte {
	for i, elem := range elems {
		buf[entryStart+i*valEntrySize] = elem.TypeCode
		if elem.TypeCode == TypeCodeLiteral {
			buf[entryStart+i*valEntrySize+valTypeSize] = elem.Value[0]
		} else {
			endian.PutUint32(buf[entryStart+i*valEntrySize+valTypeSize:], uint32(len(buf)))
			buf = append(buf, elem.Value...)
		}
	}
	return buf
}

func buildBinaryObject(keys [][]byte, elems []BinaryJSON) BinaryJSON {
	totalSize := headerSize + len(elems)*(keyEntrySize+valEntrySize)
	for i, elem := range elems {
		if elem.TypeCode != TypeCodeLiteral {
			totalSize += len(elem.Value)
		}
		totalSize += len(keys[i])
	}
	buf := make([]byte, headerSize+len(elems)*(keyEntrySize+valEntrySize), totalSize)
	endian.PutUint32(buf, uint32(len(elems)))
	endian.PutUint32(buf[dataSizeOff:], uint32(totalSize))
	for i, key := range keys {
		endian.PutUint32(buf[headerSize+i*keyEntrySize:], uint32(len(buf)))
		endian.PutUint16(buf[headerSize+i*keyEntrySize+keyLenOff:], uint16(len(key)))
		buf = append(buf, key...)
	}
	entryStart := headerSize + len(elems)*keyEntrySize
	buf = buildBinaryElements(buf, entryStart, elems)
	return BinaryJSON{TypeCode: TypeCodeObject, Value: buf}
}

// Modify modifies a JSON object by insert, replace or set.
// All path expressions cannot contain * or ** wildcard.
// If any error occurs, the input won't be changed.
func (bj BinaryJSON) Modify(pathExprList []PathExpression, values []BinaryJSON, mt ModifyType) (retj BinaryJSON, err error) {
	if len(pathExprList) != len(values) {
		// TODO: should return 1582(42000)
		return retj, errors.New("Incorrect parameter count")
	}
	for _, pathExpr := range pathExprList {
		if pathExpr.flags.containsAnyAsterisk() {
			// TODO: should return 3149(42000)
			return retj, errors.New("Invalid path expression")
		}
	}
	for i := 0; i < len(pathExprList); i++ {
		pathExpr, value := pathExprList[i], values[i]
		modifier := &binaryModifier{bj: bj}
		switch mt {
		case ModifyInsert:
			bj = modifier.insert(pathExpr, value)
		case ModifyReplace:
			bj = modifier.replace(pathExpr, value)
		case ModifySet:
			bj = modifier.set(pathExpr, value)
		}
	}
	return bj, nil
}

// Remove removes the elements indicated by pathExprList from JSON.
func (bj BinaryJSON) Remove(pathExprList []PathExpression) (BinaryJSON, error) {
	for _, pathExpr := range pathExprList {
		if len(pathExpr.legs) == 0 {
			// TODO: should return 3153(42000)
			return bj, errors.New("Invalid path expression")
		}
		if pathExpr.flags.containsAnyAsterisk() {
			// TODO: should return 3149(42000)
			return bj, errors.New("Invalid path expression")
		}
		modifer := &binaryModifier{bj: bj}
		bj = modifer.remove(pathExpr)
	}
	return bj, nil
}

type binaryModifier struct {
	bj          BinaryJSON
	modifyPtr   *byte
	modifyValue BinaryJSON
}

func (bm *binaryModifier) set(path PathExpression, newBj BinaryJSON) BinaryJSON {
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, path)
	if len(result) > 0 {
		bm.modifyPtr = &result[0].Value[0]
		bm.modifyValue = newBj
		return bm.rebuild()
	}
	bm.doInsert(path, newBj)
	return bm.rebuild()
}

func (bm *binaryModifier) replace(path PathExpression, newBj BinaryJSON) BinaryJSON {
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, path)
	if len(result) == 0 {
		return bm.bj
	}
	bm.modifyPtr = &result[0].Value[0]
	bm.modifyValue = newBj
	return bm.rebuild()
}

func (bm *binaryModifier) insert(path PathExpression, newBj BinaryJSON) BinaryJSON {
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, path)
	if len(result) > 0 {
		return bm.bj
	}
	bm.doInsert(path, newBj)
	return bm.rebuild()
}

// doInsert inserts the newBj to its parent, and builds the new parent.
func (bm *binaryModifier) doInsert(path PathExpression, newBj BinaryJSON) {
	parentPath, lastLeg := path.popOneLastLeg()
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, parentPath)
	if len(result) == 0 {
		return
	}
	parentBj := result[0]
	if lastLeg.typ == pathLegIndex {
		bm.modifyPtr = &parentBj.Value[0]
		if parentBj.TypeCode != TypeCodeArray {
			bm.modifyValue = buildBinaryArray([]BinaryJSON{parentBj, newBj})
			return
		}
		elemCount := parentBj.getElemCount()
		elems := make([]BinaryJSON, 0, elemCount+1)
		for i := 0; i < elemCount; i++ {
			elems = append(elems, parentBj.arrayGetElem(i))
		}
		elems = append(elems, newBj)
		bm.modifyValue = buildBinaryArray(elems)
		return
	}
	if parentBj.TypeCode != TypeCodeObject {
		return
	}
	bm.modifyPtr = &parentBj.Value[0]
	elemCount := parentBj.getElemCount()
	insertKey := hack.Slice(lastLeg.dotKey)
	insertIdx := sort.Search(elemCount, func(i int) bool {
		return bytes.Compare(newBj.objectGetKey(i), insertKey) >= 0
	})
	keys := make([][]byte, 0, elemCount+1)
	elems := make([]BinaryJSON, 0, elemCount+1)
	for i := 0; i < elemCount; i++ {
		if i == insertIdx {
			keys = append(keys, insertKey)
			elems = append(elems, newBj)
		}
		keys = append(keys, parentBj.objectGetKey(i))
		elems = append(elems, parentBj.objectGetVal(i))
	}
	if insertIdx == elemCount {
		keys = append(keys, insertKey)
		elems = append(elems, newBj)
	}
	bm.modifyValue = buildBinaryObject(keys, elems)
}

func (bm *binaryModifier) remove(path PathExpression) BinaryJSON {
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, path)
	if len(result) == 0 {
		return bm.bj
	}
	bm.doRemove(path)
	return bm.rebuild()
}

func (bm *binaryModifier) doRemove(path PathExpression) {
	parentPath, lastLeg := path.popOneLastLeg()
	result := make([]BinaryJSON, 0, 1)
	result = bm.bj.extractTo(result, parentPath)
	if len(result) == 0 {
		return
	}
	parentBj := result[0]
	if lastLeg.typ == pathLegIndex {
		if parentBj.TypeCode != TypeCodeArray {
			return
		}
		bm.modifyPtr = &parentBj.Value[0]
		elemCount := parentBj.getElemCount()
		elems := make([]BinaryJSON, 0, elemCount-1)
		for i := 0; i < elemCount; i++ {
			if i != lastLeg.arrayIndex {
				elems = append(elems, parentBj.arrayGetElem(i))
			}
		}
		bm.modifyValue = buildBinaryArray(elems)
		return
	}
	if parentBj.TypeCode != TypeCodeObject {
		return
	}
	bm.modifyPtr = &parentBj.Value[0]
	elemCount := parentBj.getElemCount()
	removeKey := hack.Slice(lastLeg.dotKey)
	keys := make([][]byte, 0, elemCount+1)
	elems := make([]BinaryJSON, 0, elemCount+1)
	for i := 0; i < elemCount; i++ {
		key := parentBj.objectGetKey(i)
		if !bytes.Equal(key, removeKey) {
			keys = append(keys, parentBj.objectGetKey(i))
			elems = append(elems, parentBj.objectGetVal(i))
		}
	}
	bm.modifyValue = buildBinaryObject(keys, elems)
}

// rebuild merges the old and the modified JSON into a new BinaryJSON
func (bm *binaryModifier) rebuild() BinaryJSON {
	if bm.modifyPtr == &bm.bj.Value[0] {
		return bm.modifyValue
	} else if bm.modifyPtr == nil {
		return bm.bj
	}
	bj := bm.bj
	switch bj.TypeCode {
	case TypeCodeLiteral, TypeCodeInt64, TypeCodeUint64, TypeCodeFloat64, TypeCodeString:
		return bj
	}
	buf := make([]byte, 0, len(bm.bj.Value)+len(bm.modifyValue.Value))
	elemCount := bj.getElemCount()
	var size, valEntryStart int
	if bj.TypeCode == TypeCodeArray {
		size = headerSize + elemCount*valEntrySize
		valEntryStart = headerSize
		buf = append(buf, bj.Value[:size]...)
	} else {
		size = headerSize + elemCount*(keyEntrySize+valEntrySize)
		valEntryStart = headerSize + elemCount*keyEntrySize
		buf = append(buf, bj.Value[:size]...)
		if elemCount > 0 {
			firstKeyOff := int(endian.Uint32(bj.Value[headerSize:]))
			lastKeyOff := int(endian.Uint32(bj.Value[headerSize+(elemCount-1)*keyEntrySize:]))
			lastKeyLen := int(endian.Uint16(bj.Value[headerSize+(elemCount-1)*keyEntrySize+keyLenOff:]))
			buf = append(buf, bj.Value[firstKeyOff:lastKeyOff+lastKeyLen]...)
		}
	}
	for i := 0; i < elemCount; i++ {
		valEntryOff := valEntryStart + i*valEntrySize
		elem := bj.valEntryGet(valEntryOff)
		if bm.modifyPtr == &elem.Value[0] {
			elem = bm.modifyValue
		}
		buf[valEntryOff] = elem.TypeCode
		if elem.TypeCode == TypeCodeLiteral {
			endian.PutUint32(buf[valEntryOff+valTypeSize:], uint32(elem.Value[0]))
			continue
		}
		endian.PutUint32(buf[valEntryOff+valTypeSize:], uint32(len(buf)))
		buf = append(buf, elem.Value...)
		size += len(elem.Value)
	}
	endian.PutUint32(buf[dataSizeOff:], uint32(size))
	return BinaryJSON{TypeCode: bj.TypeCode, Value: buf}
}

// CompareBinary compares two binary json objects. Returns -1 if left < right,
// 0 if left == right, else returns 1.
func CompareBinary(left, right BinaryJSON) int {
	precedence1 := jsonTypePrecedences[left.Type()]
	precedence2 := jsonTypePrecedences[right.Type()]
	var cmp int
	if precedence1 == precedence2 {
		if precedence1 == jsonTypePrecedences["NULL"] {
			// for JSON null.
			cmp = 0
		}
		switch left.TypeCode {
		case TypeCodeLiteral:
			// false is less than true.
			cmp = int(right.Value[0]) - int(left.Value[0])
		case TypeCodeInt64, TypeCodeUint64, TypeCodeFloat64:
			leftFloat := i64AsFloat64(left.getInt64(), left.TypeCode)
			rightFloat := i64AsFloat64(right.getInt64(), right.TypeCode)
			cmp = compareFloat64PrecisionLoss(leftFloat, rightFloat)
		case TypeCodeString:
			cmp = bytes.Compare(left.getString(), right.getString())
		case TypeCodeArray:
			leftCount := left.getElemCount()
			rightCount := right.getElemCount()
			for i := 0; i < leftCount && i < rightCount; i++ {
				elem1 := left.arrayGetElem(i)
				elem2 := right.arrayGetElem(i)
				cmp = CompareBinary(elem1, elem2)
				if cmp != 0 {
					return cmp
				}
			}
			cmp = leftCount - rightCount
		case TypeCodeObject:
			// only equal is defined on two json objects.
			// larger and smaller are not defined.
			cmp = bytes.Compare(left.Value, right.Value)
		}
	} else {
		cmp = precedence1 - precedence2
	}
	return cmp
}

// MergeBinary merges multiple BinaryJSON into one according the following rules:
// 1) adjacent arrays are merged to a single array;
// 2) adjacent object are merged to a single object;
// 3) a scalar value is autowrapped as an array before merge;
// 4) an adjacent array and object are merged by autowrapping the object as an array.
func MergeBinary(bjs []BinaryJSON) BinaryJSON {
	var remain = bjs
	var arrays, objects []BinaryJSON
	var results []BinaryJSON
	for len(remain) > 0 {
		arrays, remain = getAdjacentArrays(remain)
		if len(arrays) > 0 {
			results = append(results, mergeBinaryArray(arrays))
		}
		objects, remain = getAdjacentObjects(remain)
		if len(objects) > 0 {
			results = append(results, mergeBinaryObject(objects))
		}
	}
	if len(results) == 1 {
		return results[0]
	}
	return buildBinaryArray(results)
}

func getAdjacentArrays(bjs []BinaryJSON) (arrays, remain []BinaryJSON) {
	for i := 0; i < len(bjs); i++ {
		if bjs[i].TypeCode == TypeCodeObject {
			return bjs[:i], bjs[i:]
		}
	}
	return bjs, nil
}

func getAdjacentObjects(bjs []BinaryJSON) (objects, remain []BinaryJSON) {
	for i := 0; i < len(bjs); i++ {
		if bjs[i].TypeCode != TypeCodeObject {
			return bjs[:i], bjs[i:]
		}
	}
	return bjs, nil
}

func mergeBinaryArray(elems []BinaryJSON) BinaryJSON {
	buf := make([]BinaryJSON, 0, len(elems))
	for i := 0; i < len(elems); i++ {
		elem := elems[i]
		if elem.TypeCode != TypeCodeArray {
			buf = append(buf, elem)
		} else {
			childCount := elem.getElemCount()
			for j := 0; j < childCount; j++ {
				buf = append(buf, elem.arrayGetElem(j))
			}
		}
	}
	if len(buf) == 1 {
		return buf[0]
	}
	return buildBinaryArray(buf)
}

func mergeBinaryObject(objects []BinaryJSON) BinaryJSON {
	keyValMap := make(map[string]BinaryJSON)
	keys := make([][]byte, 0, len(keyValMap))
	for _, obj := range objects {
		elemCount := obj.getElemCount()
		for i := 0; i < elemCount; i++ {
			key := obj.objectGetKey(i)
			val := obj.objectGetVal(i)
			if old, ok := keyValMap[string(key)]; ok {
				keyValMap[string(key)] = MergeBinary([]BinaryJSON{old, val})
			} else {
				keyValMap[string(key)] = val
				keys = append(keys, key)
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	values := make([]BinaryJSON, len(keys))
	for i, key := range keys {
		values[i] = keyValMap[string(key)]
	}
	return buildBinaryObject(keys, values)
}
