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
	"math"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// Extract receives several path expressions as arguments, matches them in bj, and returns:
//
//	ret: target JSON matched any path expressions. maybe autowrapped as an array.
//	found: true if any path expressions matched.
func (bj BinaryJSON) Extract(pathExprList []JSONPathExpression) (ret BinaryJSON, found bool) {
	buf := make([]BinaryJSON, 0, 1)
	for _, pathExpr := range pathExprList {
		buf = bj.extractTo(buf, pathExpr, make(map[*byte]struct{}), false)
	}

	if len(buf) == 0 {
		found = false
	} else if len(pathExprList) == 1 && len(buf) == 1 {
		// If pathExpr contains asterisks, len(elemList) won't be 1
		// even if len(pathExprList) equals to 1.
		found = true
		ret = buf[0]
		// Fix https://github.com/pingcap/tidb/issues/30352
		if pathExprList[0].CouldMatchMultipleValues() {
			ret = buildBinaryJSONArray(buf)
		}
	} else {
		found = true
		ret = buildBinaryJSONArray(buf)
	}
	return
}

func (bj BinaryJSON) extractOne(pathExpr JSONPathExpression) []BinaryJSON {
	result := make([]BinaryJSON, 0, 1)
	return bj.extractTo(result, pathExpr, nil, true)
}

func (bj BinaryJSON) extractTo(buf []BinaryJSON, pathExpr JSONPathExpression, dup map[*byte]struct{}, one bool) []BinaryJSON {
	if len(pathExpr.legs) == 0 {
		if dup != nil {
			if _, exists := dup[&bj.Value[0]]; exists {
				return buf
			}
			dup[&bj.Value[0]] = struct{}{}
		}
		return append(buf, bj)
	}
	currentLeg, subPathExpr := pathExpr.popOneLeg()
	if currentLeg.typ == jsonPathLegArraySelection {
		if bj.TypeCode != JSONTypeCodeArray {
			// If the current object is not an array, still append them if the selection includes
			// 0 or last. But for asterisk, it still returns NULL.
			//
			// don't call `getIndexRange` or `getIndexFromStart`, they will panic if the argument
			// is not array.
			switch selection := currentLeg.arraySelection.(type) {
			case jsonPathArraySelectionIndex:
				if selection.index == 0 || selection.index == -1 {
					buf = bj.extractTo(buf, subPathExpr, dup, one)
				}
			case jsonPathArraySelectionRange:
				// for [0 to Non-negative Number] and [0 to last], it extracts itself
				if selection.start == 0 && selection.end >= -1 {
					buf = bj.extractTo(buf, subPathExpr, dup, one)
				}
			}
			return buf
		}

		start, end := currentLeg.arraySelection.getIndexRange(bj)
		if start >= 0 && start <= end {
			for i := start; i <= end; i++ {
				buf = bj.ArrayGetElem(i).extractTo(buf, subPathExpr, dup, one)
			}
		}
	} else if currentLeg.typ == jsonPathLegKey && bj.TypeCode == JSONTypeCodeObject {
		elemCount := bj.GetElemCount()
		if currentLeg.dotKey == "*" {
			for i := 0; i < elemCount && !jsonFinished(buf, one); i++ {
				buf = bj.objectGetVal(i).extractTo(buf, subPathExpr, dup, one)
			}
		} else {
			child, ok := bj.objectSearchKey(hack.Slice(currentLeg.dotKey))
			if ok {
				buf = child.extractTo(buf, subPathExpr, dup, one)
			}
		}
	} else if currentLeg.typ == jsonPathLegDoubleAsterisk {
		buf = bj.extractTo(buf, subPathExpr, dup, one)
		if bj.TypeCode == JSONTypeCodeArray {
			elemCount := bj.GetElemCount()
			for i := 0; i < elemCount && !jsonFinished(buf, one); i++ {
				buf = bj.ArrayGetElem(i).extractTo(buf, pathExpr, dup, one)
			}
		} else if bj.TypeCode == JSONTypeCodeObject {
			elemCount := bj.GetElemCount()
			for i := 0; i < elemCount && !jsonFinished(buf, one); i++ {
				buf = bj.objectGetVal(i).extractTo(buf, pathExpr, dup, one)
			}
		}
	}
	return buf
}

func jsonFinished(buf []BinaryJSON, one bool) bool {
	return one && len(buf) > 0
}

func (bj BinaryJSON) objectSearchKey(key []byte) (BinaryJSON, bool) {
	elemCount := bj.GetElemCount()
	idx := sort.Search(elemCount, func(i int) bool {
		return bytes.Compare(bj.objectGetKey(i), key) >= 0
	})
	if idx < elemCount && bytes.Equal(bj.objectGetKey(idx), key) {
		return bj.objectGetVal(idx), true
	}
	return BinaryJSON{}, false
}

func buildBinaryJSONArray(elems []BinaryJSON) BinaryJSON {
	totalSize := headerSize + len(elems)*valEntrySize
	for _, elem := range elems {
		if elem.TypeCode != JSONTypeCodeLiteral {
			totalSize += len(elem.Value)
		}
	}
	buf := make([]byte, headerSize+len(elems)*valEntrySize, totalSize)
	jsonEndian.PutUint32(buf, uint32(len(elems)))
	jsonEndian.PutUint32(buf[dataSizeOff:], uint32(totalSize))
	buf = buildBinaryJSONElements(buf, headerSize, elems)
	return BinaryJSON{TypeCode: JSONTypeCodeArray, Value: buf}
}

func buildBinaryJSONElements(buf []byte, entryStart int, elems []BinaryJSON) []byte {
	for i, elem := range elems {
		buf[entryStart+i*valEntrySize] = elem.TypeCode
		if elem.TypeCode == JSONTypeCodeLiteral {
			buf[entryStart+i*valEntrySize+valTypeSize] = elem.Value[0]
		} else {
			jsonEndian.PutUint32(buf[entryStart+i*valEntrySize+valTypeSize:], uint32(len(buf)))
			buf = append(buf, elem.Value...)
		}
	}
	return buf
}

func buildBinaryJSONObject(keys [][]byte, elems []BinaryJSON) (BinaryJSON, error) {
	totalSize := headerSize + len(elems)*(keyEntrySize+valEntrySize)
	for i, elem := range elems {
		if elem.TypeCode != JSONTypeCodeLiteral {
			totalSize += len(elem.Value)
		}
		totalSize += len(keys[i])
	}
	buf := make([]byte, headerSize+len(elems)*(keyEntrySize+valEntrySize), totalSize)
	jsonEndian.PutUint32(buf, uint32(len(elems)))
	jsonEndian.PutUint32(buf[dataSizeOff:], uint32(totalSize))
	for i, key := range keys {
		if len(key) > math.MaxUint16 {
			return BinaryJSON{}, ErrJSONObjectKeyTooLong
		}
		jsonEndian.PutUint32(buf[headerSize+i*keyEntrySize:], uint32(len(buf)))
		jsonEndian.PutUint16(buf[headerSize+i*keyEntrySize+keyLenOff:], uint16(len(key)))
		buf = append(buf, key...)
	}
	entryStart := headerSize + len(elems)*keyEntrySize
	buf = buildBinaryJSONElements(buf, entryStart, elems)
	return BinaryJSON{TypeCode: JSONTypeCodeObject, Value: buf}, nil
}

// Modify modifies a JSON object by insert, replace or set.
// All path expressions cannot contain * or ** wildcard.
// If any error occurs, the input won't be changed.
func (bj BinaryJSON) Modify(pathExprList []JSONPathExpression, values []BinaryJSON, mt JSONModifyType) (retj BinaryJSON, err error) {
	if len(pathExprList) != len(values) {
		// TODO: should return 1582(42000)
		return retj, errors.New("Incorrect parameter count")
	}
	for _, pathExpr := range pathExprList {
		if pathExpr.flags.containsAnyAsterisk() || pathExpr.flags.containsAnyRange() {
			return retj, ErrInvalidJSONPathMultipleSelection
		}
	}
	for i := range pathExprList {
		pathExpr, value := pathExprList[i], values[i]
		modifier := &binaryModifier{bj: bj}
		switch mt {
		case JSONModifyInsert:
			bj = modifier.insert(pathExpr, value)
		case JSONModifyReplace:
			bj = modifier.replace(pathExpr, value)
		case JSONModifySet:
			bj = modifier.set(pathExpr, value)
		}
		if modifier.err != nil {
			return BinaryJSON{}, modifier.err
		}
	}
	if bj.GetElemDepth()-1 > maxJSONDepth {
		return bj, ErrJSONDocumentTooDeep
	}
	return bj, nil
}

// ArrayInsert insert a BinaryJSON into the given array cell.
// All path expressions cannot contain * or ** wildcard.
// If any error occurs, the input won't be changed.
func (bj BinaryJSON) ArrayInsert(pathExpr JSONPathExpression, value BinaryJSON) (res BinaryJSON, err error) {
	// Check the path is a index
	if len(pathExpr.legs) < 1 {
		return bj, ErrInvalidJSONPathArrayCell
	}
	parentPath, lastLeg := pathExpr.popOneLastLeg()
	if lastLeg.typ != jsonPathLegArraySelection {
		return bj, ErrInvalidJSONPathArrayCell
	}
	// Find the target array
	obj, exists := bj.Extract([]JSONPathExpression{parentPath})
	if !exists || obj.TypeCode != JSONTypeCodeArray {
		return bj, nil
	}

	idx := 0
	switch selection := lastLeg.arraySelection.(type) {
	case jsonPathArraySelectionIndex:
		idx = selection.index.getIndexFromStart(obj)
	default:
		return bj, ErrInvalidJSONPathArrayCell
	}
	count := obj.GetElemCount()
	if idx >= count {
		idx = count
	}
	// Insert into the array
	newArray := make([]BinaryJSON, 0, count+1)
	for i := range idx {
		elem := obj.ArrayGetElem(i)
		newArray = append(newArray, elem)
	}
	newArray = append(newArray, value)
	for i := idx; i < count; i++ {
		elem := obj.ArrayGetElem(i)
		newArray = append(newArray, elem)
	}
	obj = buildBinaryJSONArray(newArray)

	bj, err = bj.Modify([]JSONPathExpression{parentPath}, []BinaryJSON{obj}, JSONModifySet)
	if err != nil {
		return bj, err
	}
	return bj, nil
}

// Remove removes the elements indicated by pathExprList from JSON.
func (bj BinaryJSON) Remove(pathExprList []JSONPathExpression) (BinaryJSON, error) {
	for _, pathExpr := range pathExprList {
		if len(pathExpr.legs) == 0 {
			return bj, ErrJSONVacuousPath
		}
		if pathExpr.flags.containsAnyAsterisk() || pathExpr.flags.containsAnyRange() {
			return bj, ErrInvalidJSONPathMultipleSelection
		}
		modifer := &binaryModifier{bj: bj}
		bj = modifer.remove(pathExpr)
		if modifer.err != nil {
			return BinaryJSON{}, modifer.err
		}
	}
	return bj, nil
}

type binaryModifier struct {
	bj          BinaryJSON
	modifyPtr   *byte
	modifyValue BinaryJSON
	err         error
}

func (bm *binaryModifier) set(path JSONPathExpression, newBj BinaryJSON) BinaryJSON {
	result := bm.bj.extractOne(path)
	if len(result) > 0 {
		bm.modifyPtr = &result[0].Value[0]
		bm.modifyValue = newBj
		return bm.rebuild()
	}
	bm.doInsert(path, newBj)
	if bm.err != nil {
		return BinaryJSON{}
	}
	return bm.rebuild()
}

func (bm *binaryModifier) replace(path JSONPathExpression, newBj BinaryJSON) BinaryJSON {
	result := bm.bj.extractOne(path)
	if len(result) == 0 {
		return bm.bj
	}
	bm.modifyPtr = &result[0].Value[0]
	bm.modifyValue = newBj
	return bm.rebuild()
}

func (bm *binaryModifier) insert(path JSONPathExpression, newBj BinaryJSON) BinaryJSON {
	result := bm.bj.extractOne(path)
	if len(result) > 0 {
		return bm.bj
	}
	bm.doInsert(path, newBj)
	if bm.err != nil {
		return BinaryJSON{}
	}
	return bm.rebuild()
}

// doInsert inserts the newBj to its parent, and builds the new parent.
func (bm *binaryModifier) doInsert(path JSONPathExpression, newBj BinaryJSON) {
	parentPath, lastLeg := path.popOneLastLeg()
	result := bm.bj.extractOne(parentPath)
	if len(result) == 0 {
		return
	}
	parentBj := result[0]
	if lastLeg.typ == jsonPathLegArraySelection {
		bm.modifyPtr = &parentBj.Value[0]
		if parentBj.TypeCode != JSONTypeCodeArray {
			bm.modifyValue = buildBinaryJSONArray([]BinaryJSON{parentBj, newBj})
			return
		}
		elemCount := parentBj.GetElemCount()
		elems := make([]BinaryJSON, 0, elemCount+1)
		for i := range elemCount {
			elems = append(elems, parentBj.ArrayGetElem(i))
		}
		elems = append(elems, newBj)
		bm.modifyValue = buildBinaryJSONArray(elems)
		return
	}
	if parentBj.TypeCode != JSONTypeCodeObject {
		return
	}
	bm.modifyPtr = &parentBj.Value[0]
	elemCount := parentBj.GetElemCount()
	insertKey := hack.Slice(lastLeg.dotKey)
	insertIdx := sort.Search(elemCount, func(i int) bool {
		return bytes.Compare(parentBj.objectGetKey(i), insertKey) >= 0
	})
	keys := make([][]byte, 0, elemCount+1)
	elems := make([]BinaryJSON, 0, elemCount+1)
	for i := range elemCount {
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
	bm.modifyValue, bm.err = buildBinaryJSONObject(keys, elems)
}

func (bm *binaryModifier) remove(path JSONPathExpression) BinaryJSON {
	result := bm.bj.extractOne(path)
	if len(result) == 0 {
		return bm.bj
	}
	bm.doRemove(path)
	if bm.err != nil {
		return BinaryJSON{}
	}
	return bm.rebuild()
}

func (bm *binaryModifier) doRemove(path JSONPathExpression) {
	parentPath, lastLeg := path.popOneLastLeg()
	result := bm.bj.extractOne(parentPath)
	if len(result) == 0 {
		return
	}
	parentBj := result[0]
	if lastLeg.typ == jsonPathLegArraySelection {
		if parentBj.TypeCode != JSONTypeCodeArray {
			return
		}
		selectionIndex, ok := lastLeg.arraySelection.(jsonPathArraySelectionIndex)
		if !ok {
			return
		}
		idx := selectionIndex.index.getIndexFromStart(parentBj)

		bm.modifyPtr = &parentBj.Value[0]
		elemCount := parentBj.GetElemCount()
		elems := make([]BinaryJSON, 0, elemCount-1)
		for i := range elemCount {
			if i != idx {
				elems = append(elems, parentBj.ArrayGetElem(i))
			}
		}
		bm.modifyValue = buildBinaryJSONArray(elems)
		return
	}
	if parentBj.TypeCode != JSONTypeCodeObject {
		return
	}
	bm.modifyPtr = &parentBj.Value[0]
	elemCount := parentBj.GetElemCount()
	removeKey := hack.Slice(lastLeg.dotKey)
	keys := make([][]byte, 0, elemCount+1)
	elems := make([]BinaryJSON, 0, elemCount+1)
	for i := range elemCount {
		key := parentBj.objectGetKey(i)
		if !bytes.Equal(key, removeKey) {
			keys = append(keys, parentBj.objectGetKey(i))
			elems = append(elems, parentBj.objectGetVal(i))
		}
	}
	bm.modifyValue, bm.err = buildBinaryJSONObject(keys, elems)
}

// rebuild merges the old and the modified JSON into a new BinaryJSON
func (bm *binaryModifier) rebuild() BinaryJSON {
	buf := make([]byte, 0, len(bm.bj.Value)+len(bm.modifyValue.Value))
	value, tpCode := bm.rebuildTo(buf)
	return BinaryJSON{TypeCode: tpCode, Value: value}
}

func (bm *binaryModifier) rebuildTo(buf []byte) ([]byte, JSONTypeCode) {
	if bm.modifyPtr == &bm.bj.Value[0] {
		bm.modifyPtr = nil
		return append(buf, bm.modifyValue.Value...), bm.modifyValue.TypeCode
	} else if bm.modifyPtr == nil {
		return append(buf, bm.bj.Value...), bm.bj.TypeCode
	}
	bj := bm.bj
	if bj.TypeCode != JSONTypeCodeArray && bj.TypeCode != JSONTypeCodeObject {
		return append(buf, bj.Value...), bj.TypeCode
	}
	docOff := len(buf)
	elemCount := bj.GetElemCount()
	var valEntryStart int
	if bj.TypeCode == JSONTypeCodeArray {
		copySize := headerSize + elemCount*valEntrySize
		valEntryStart = headerSize
		buf = append(buf, bj.Value[:copySize]...)
	} else {
		copySize := headerSize + elemCount*(keyEntrySize+valEntrySize)
		valEntryStart = headerSize + elemCount*keyEntrySize
		buf = append(buf, bj.Value[:copySize]...)
		if elemCount > 0 {
			firstKeyOff := int(jsonEndian.Uint32(bj.Value[headerSize:]))
			lastKeyOff := int(jsonEndian.Uint32(bj.Value[headerSize+(elemCount-1)*keyEntrySize:]))
			lastKeyLen := int(jsonEndian.Uint16(bj.Value[headerSize+(elemCount-1)*keyEntrySize+keyLenOff:]))
			buf = append(buf, bj.Value[firstKeyOff:lastKeyOff+lastKeyLen]...)
		}
	}
	for i := range elemCount {
		valEntryOff := valEntryStart + i*valEntrySize
		elem := bj.valEntryGet(valEntryOff)
		bm.bj = elem
		var tpCode JSONTypeCode
		valOff := len(buf) - docOff
		buf, tpCode = bm.rebuildTo(buf)
		buf[docOff+valEntryOff] = tpCode
		if tpCode == JSONTypeCodeLiteral {
			lastIdx := len(buf) - 1
			jsonEndian.PutUint32(buf[docOff+valEntryOff+valTypeSize:], uint32(buf[lastIdx]))
			buf = buf[:lastIdx]
		} else {
			jsonEndian.PutUint32(buf[docOff+valEntryOff+valTypeSize:], uint32(valOff))
		}
	}
	jsonEndian.PutUint32(buf[docOff+dataSizeOff:], uint32(len(buf)-docOff))
	return buf, bj.TypeCode
}
