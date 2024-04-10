// Copyright 2023 PingCAP, Inc.
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

package aggfuncs

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

var testLongStr1 string = getLongString("平p凯k星x辰c")
var testLongStr2 string = getLongString("123aa啊啊aa")

func getChunk() *chunk.Chunk {
	fieldTypes := make([]*types.FieldType, 1)
	fieldTypes[0] = types.NewFieldType(mysql.TypeBit)
	return chunk.NewChunkWithCapacity(fieldTypes, 100)
}

func getLongString(originStr string) string {
	returnStr := originStr
	for i := 0; i < 10; i++ {
		returnStr += returnStr
	}
	return returnStr
}

func getLargeRandBuffer() []byte {
	byteLen := 10000
	ret := make([]byte, byteLen)
	randStart := rand.Int31()
	for i := 0; i < byteLen; i++ {
		ret[i] = byte((int(randStart) + i) % 8)
	}
	return ret
}

type bufferSizeChecker struct {
	lastCap int
}

func newBufferSizeChecker() *bufferSizeChecker {
	return &bufferSizeChecker{lastCap: -1}
}

// We need to ensure that buffer in `SerializeHelper` should be enlarged
// when it serialize an object whose size is larger than it's capacity.
func (b *bufferSizeChecker) checkBufferCapacity(helper *SerializeHelper) bool {
	newCap := cap(helper.buf)
	retVal := (newCap > b.lastCap)
	b.lastCap = newCap
	return retVal
}

func TestPartialResult4Count(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4Count{-123, 0, 123}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4Count)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4Count(*(*partialResult4Count)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4Count, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4Count(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4Count)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinInt(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4MaxMinInt{
		{val: -123, isNull: true},
		{val: 0, isNull: false},
		{val: 123, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinInt)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinInt(*(*partialResult4MaxMinInt)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinInt, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinInt(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinInt)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinUint(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4MaxMinUint{
		{val: 0, isNull: true},
		{val: 1, isNull: false},
		{val: 2, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinUint)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinUint(*(*partialResult4MaxMinUint)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinUint, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinUint(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinUint)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinDecimal(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4MaxMinDecimal{
		{val: *types.NewDecFromInt(0), isNull: true},
		{val: *types.NewDecFromUint(123456), isNull: false},
		{val: *types.NewDecFromInt(99999), isNull: true},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinDecimal)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinDecimal(*(*partialResult4MaxMinDecimal)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinDecimal, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinDecimal(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinDecimal)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinFloat32(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4MaxMinFloat32{
		{val: -123.123, isNull: true},
		{val: 0.0, isNull: false},
		{val: 123.123, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinFloat32)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinFloat32(*(*partialResult4MaxMinFloat32)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinFloat32, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinFloat32(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinFloat32)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinFloat64(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4MaxMinFloat64{
		{val: -123.123, isNull: true},
		{val: 0.0, isNull: false},
		{val: 123.123, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinFloat64)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinFloat64(*(*partialResult4MaxMinFloat64)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinFloat64, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinFloat64(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinFloat64)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinTime(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4MaxMinTime{
		{val: types.NewTime(123, 10, 9), isNull: true},
		{val: types.NewTime(0, 0, 0), isNull: false},
		{val: types.NewTime(9876, 12, 10), isNull: true},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinTime)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinTime(*(*partialResult4MaxMinTime)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinTime, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinTime(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinTime)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinString(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4MaxMinString{
		{val: string("12312412312"), isNull: true},
		{val: testLongStr1, isNull: false},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinString)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinString(*(*partialResult4MaxMinString)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinString, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinString(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinString)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinJSON(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4MaxMinJSON{
		{val: types.BinaryJSON{TypeCode: 3, Value: []byte{}}, isNull: false},
		{val: types.BinaryJSON{TypeCode: 6, Value: getLargeRandBuffer()}, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinJSON)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinJSON(*(*partialResult4MaxMinJSON)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinJSON, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinJSON(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinJSON)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinEnum(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4MaxMinEnum{
		{val: types.Enum{Name: string(""), Value: 123}, isNull: true},
		{val: types.Enum{Name: testLongStr1, Value: 0}, isNull: false},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinEnum)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinEnum(*(*partialResult4MaxMinEnum)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinEnum, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinEnum(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinEnum)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinSet(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4MaxMinSet{
		{val: types.Set{Name: string(""), Value: 123}, isNull: true},
		{val: types.Set{Name: testLongStr1, Value: 0}, isNull: false},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4MaxMinSet)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4MaxMinSet(*(*partialResult4MaxMinSet)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4MaxMinSet, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4MaxMinSet(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinSet)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4AvgDecimal(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4AvgDecimal{
		{sum: *types.NewDecFromInt(0), count: 0},
		{sum: *types.NewDecFromInt(12345), count: 123},
		{sum: *types.NewDecFromInt(87654), count: -123},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4AvgDecimal)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4AvgDecimal(*(*partialResult4AvgDecimal)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4AvgDecimal, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4AvgDecimal(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4AvgDecimal)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4AvgFloat64(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4AvgFloat64{
		{sum: 0.0, count: 0},
		{sum: 123.123, count: 123},
		{sum: -123.123, count: -123},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4AvgFloat64)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4AvgFloat64(*(*partialResult4AvgFloat64)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4AvgFloat64, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4AvgFloat64(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4AvgFloat64)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4SumDecimal(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4SumDecimal{
		{val: *types.NewDecFromInt(0), notNullRowCount: 0},
		{val: *types.NewDecFromInt(12345), notNullRowCount: 123},
		{val: *types.NewDecFromInt(87654), notNullRowCount: -123},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4SumDecimal)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4SumDecimal(*(*partialResult4SumDecimal)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4SumDecimal, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4SumDecimal(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4SumDecimal)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4SumFloat64(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4SumFloat64{
		{val: 0.0, notNullRowCount: 0},
		{val: 123.123, notNullRowCount: 123},
		{val: -123.123, notNullRowCount: -123},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4SumFloat64)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4SumFloat64(*(*partialResult4SumFloat64)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4SumFloat64, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4SumFloat64(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4SumFloat64)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestBasePartialResult4GroupConcat(t *testing.T) {
	var serializeHelper = NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []basePartialResult4GroupConcat{
		{valsBuf: bytes.NewBufferString(""), buffer: bytes.NewBufferString("")},
		{valsBuf: bytes.NewBufferString("xzxx"), buffer: bytes.NewBufferString(testLongStr2)},
		{valsBuf: bytes.NewBufferString(testLongStr1), buffer: bytes.NewBufferString(testLongStr2)},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(basePartialResult4GroupConcat)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializeBasePartialResult4GroupConcat(*(*basePartialResult4GroupConcat)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]basePartialResult4GroupConcat, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializeBasePartialResult4GroupConcat(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, (*basePartialResult4GroupConcat)(serializedPartialResults[i]).valsBuf.String(), deserializedPartialResults[i].valsBuf.String())
		require.Equal(t, (*basePartialResult4GroupConcat)(serializedPartialResults[i]).buffer.String(), deserializedPartialResults[i].buffer.String())
	}
}

func TestPartialResult4BitFunc(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4BitFunc{0, 1, 2}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4BitFunc)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4BitFunc(*(*partialResult4BitFunc)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4BitFunc, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4BitFunc(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4BitFunc)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4JsonArrayagg(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4JsonArrayagg{
		{entries: []any{int64(1), float64(1.1), "", true, types.Opaque{TypeCode: 1, Buf: getLargeRandBuffer()}, types.NewTime(9876, 12, 10)}},
		{entries: []any{int64(1), float64(1.1), false, types.NewDuration(1, 2, 3, 4, 5), testLongStr1}},
		{entries: []any{"dw啊q", float64(-1.1), int64(0), types.NewDuration(1, 2, 3, 4, 5), types.NewTime(123, 1, 2), testLongStr1, types.BinaryJSON{TypeCode: 1, Value: []byte(testLongStr2)}, types.Opaque{TypeCode: 6, Buf: getLargeRandBuffer()}}},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4JsonArrayagg)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4JsonArrayagg(*(*partialResult4JsonArrayagg)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4JsonArrayagg, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4JsonArrayagg(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4JsonArrayagg)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4JsonObjectAgg(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4JsonObjectAgg{
		{entries: map[string]any{"123": int64(1), "234": float64(1.1), "999": true, "235": "123"}, bInMap: 0},
		{entries: map[string]any{"啊": testLongStr1, "我": float64(1.1), "反": int64(456)}, bInMap: 0},
		{entries: map[string]any{"fe": testLongStr1, " ": int64(36798), "888": false, "": testLongStr2}, bInMap: 0},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4JsonObjectAgg)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4JsonObjectAgg(*(*partialResult4JsonObjectAgg)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4JsonObjectAgg, testDataNum+1)
	index := 0
	for {
		success, _ := deserializeHelper.deserializePartialResult4JsonObjectAgg(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4JsonObjectAgg)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowDecimal(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4FirstRowDecimal{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: *types.NewDecFromInt(0)},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: false, gotFirstRow: false}, val: *types.NewDecFromInt(123)},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: true}, val: *types.NewDecFromInt(12345)},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowDecimal)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowDecimal(*(*partialResult4FirstRowDecimal)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowDecimal, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowDecimal(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowDecimal)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowInt(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4FirstRowInt{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: -123},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: false, gotFirstRow: false}, val: 0},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: true}, val: 123},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowInt)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowInt(*(*partialResult4FirstRowInt)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowInt, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowInt(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowInt)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowTime(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4FirstRowTime{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: types.NewTime(0, 0, 1)},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: false, gotFirstRow: false}, val: types.NewTime(123, 0, 1)},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: true}, val: types.NewTime(456, 0, 1)},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowTime)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowTime(*(*partialResult4FirstRowTime)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowTime, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowTime(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowTime)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowString(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4FirstRowString{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: ""},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: false, gotFirstRow: false}, val: testLongStr1},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowString)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowString(*(*partialResult4FirstRowString)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowString, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowString(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowString)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowFloat32(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4FirstRowFloat32{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: -1.1},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: false, gotFirstRow: false}, val: 0},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: true}, val: 1.1},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowFloat32)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowFloat32(*(*partialResult4FirstRowFloat32)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowFloat32, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowFloat32(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowFloat32)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowFloat64(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4FirstRowFloat64{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: -1.1},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: false, gotFirstRow: false}, val: 0},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: true}, val: 1.1},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowFloat64)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowFloat64(*(*partialResult4FirstRowFloat64)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowFloat64, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowFloat64(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowFloat64)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowDuration(t *testing.T) {
	serializeHelper := NewSerializeHelper()

	// Initialize test data
	expectData := []partialResult4FirstRowDuration{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: types.NewDuration(1, 2, 3, 4, 5)},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: false, gotFirstRow: false}, val: types.NewDuration(0, 0, 0, 0, 0)},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: true}, val: types.NewDuration(10, 20, 30, 40, 50)},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowDuration)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowDuration(*(*partialResult4FirstRowDuration)(pr))
		chunk.AppendBytes(0, serializedData)
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowDuration, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowDuration(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowDuration)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowJSON(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4FirstRowJSON{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: false, gotFirstRow: false}, val: types.BinaryJSON{TypeCode: 6, Value: []byte{}}},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: types.BinaryJSON{TypeCode: 8, Value: getLargeRandBuffer()}},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowJSON)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowJSON(*(*partialResult4FirstRowJSON)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowJSON, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowJSON(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowJSON)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowEnum(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4FirstRowEnum{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: types.Enum{Name: string(""), Value: 123}},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: types.Enum{Name: testLongStr2, Value: 999}},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowEnum)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowEnum(*(*partialResult4FirstRowEnum)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowEnum, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowEnum(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowEnum)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4FirstRowSet(t *testing.T) {
	serializeHelper := NewSerializeHelper()
	bufSizeChecker := newBufferSizeChecker()

	// Initialize test data
	expectData := []partialResult4FirstRowSet{
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: types.Set{Name: string(""), Value: 123}},
		{basePartialResult4FirstRow: basePartialResult4FirstRow{isNull: true, gotFirstRow: false}, val: types.Set{Name: testLongStr1, Value: 999}},
	}
	serializedPartialResults := make([]PartialResult, len(expectData))
	testDataNum := len(serializedPartialResults)
	for i := range serializedPartialResults {
		pr := new(partialResult4FirstRowSet)
		*pr = expectData[i]
		serializedPartialResults[i] = PartialResult(pr)
	}

	// Serialize test data
	chunk := getChunk()
	for _, pr := range serializedPartialResults {
		serializedData := serializeHelper.serializePartialResult4FirstRowSet(*(*partialResult4FirstRowSet)(pr))
		chunk.AppendBytes(0, serializedData)
		require.True(t, bufSizeChecker.checkBufferCapacity(serializeHelper))
	}

	// Deserialize test data
	deserializeHelper := newDeserializeHelper(chunk.Column(0), testDataNum)
	deserializedPartialResults := make([]partialResult4FirstRowSet, testDataNum+1)
	index := 0
	for {
		success := deserializeHelper.deserializePartialResult4FirstRowSet(&deserializedPartialResults[index])
		if !success {
			break
		}
		index++
	}

	chunk.Column(0).DestroyDataForTest()

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4FirstRowSet)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}
