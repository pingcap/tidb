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
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

var serializeHelper = SpillSerializeHelper{}

func getChunk() *chunk.Chunk {
	fieldTypes := make([]*types.FieldType, 1)
	fieldTypes[0] = types.NewFieldType(mysql.TypeBit)
	return chunk.NewChunkWithCapacity(fieldTypes, 100)
}

func TestPartialResult4Count(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4Count{-123, 0, 123}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4Count)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinInt(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinInt{
		{val: -123, isNull: true},
		{val: 0, isNull: false},
		{val: 123, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinInt)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinUint(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinUint{
		{val: 0, isNull: true},
		{val: 1, isNull: false},
		{val: 2, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinUint)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinDecimal(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinDecimal{
		{val: *types.NewDecFromInt(0), isNull: true},
		{val: *types.NewDecFromUint(123456), isNull: false},
		{val: *types.NewDecFromInt(99999), isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinDecimal)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinFloat32(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinFloat32{
		{val: -123.123, isNull: true},
		{val: 0.0, isNull: false},
		{val: 123.123, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinFloat32)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinFloat64(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinFloat64{
		{val: -123.123, isNull: true},
		{val: 0.0, isNull: false},
		{val: 123.123, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinFloat64)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinTime(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinTime{
		{val: types.NewTime(123, 10, 9), isNull: true},
		{val: types.NewTime(0, 0, 0), isNull: false},
		{val: types.NewTime(9876, 12, 10), isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinTime)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinString(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinString{
		{val: string("12312412312"), isNull: true},
		{val: string(""), isNull: false},
		{val: string("平p凯k星x辰c"), isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinString)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinJSON(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinJSON{
		{val: types.BinaryJSON{TypeCode: 1, Value: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0}}, isNull: true},
		{val: types.BinaryJSON{TypeCode: 3, Value: []byte{}}, isNull: false},
		{val: types.BinaryJSON{TypeCode: 6, Value: []byte{0, 4, 2, 3, 0}}, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinJSON)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinEnum(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinEnum{
		{val: types.Enum{Name: string(""), Value: 123}, isNull: true},
		{val: types.Enum{Name: string("123aa啊啊aa"), Value: 0}, isNull: false},
		{val: types.Enum{Name: string("123aaa"), Value: 0}, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinEnum)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4MaxMinSet(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4MaxMinSet{
		{val: types.Set{Name: string(""), Value: 123}, isNull: true},
		{val: types.Set{Name: string("123aa啊啊aa"), Value: 0}, isNull: false},
		{val: types.Set{Name: string("123aaa"), Value: 0}, isNull: true},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4MaxMinSet)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4AvgDecimal(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4AvgDecimal{
		{sum: *types.NewDecFromInt(0), count: 0},
		{sum: *types.NewDecFromInt(12345), count: 123},
		{sum: *types.NewDecFromInt(87654), count: -123},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4AvgDecimal)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4AvgFloat64(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4AvgFloat64{
		{sum: 0.0, count: 0},
		{sum: 123.123, count: 123},
		{sum: -123.123, count: -123},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4AvgFloat64)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4SumDecimal(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4SumDecimal{
		{val: *types.NewDecFromInt(0), notNullRowCount: 0},
		{val: *types.NewDecFromInt(12345), notNullRowCount: 123},
		{val: *types.NewDecFromInt(87654), notNullRowCount: -123},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4SumDecimal)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestPartialResult4SumFloat64(t *testing.T) {
	// Initialize test data
	expectData := []partialResult4SumFloat64{
		{val: 0.0, notNullRowCount: 0},
		{val: 123.123, notNullRowCount: 123},
		{val: -123.123, notNullRowCount: -123},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, *(*partialResult4SumFloat64)(serializedPartialResults[i]), deserializedPartialResults[i])
	}
}

func TestBasePartialResult4GroupConcat(t *testing.T) {
	// Initialize test data
	expectData := []basePartialResult4GroupConcat{
		{valsBuf: bytes.NewBufferString("xzxx"), buffer: bytes.NewBufferString("dwaa啊啊a啊")},
		{valsBuf: bytes.NewBufferString(""), buffer: bytes.NewBufferString("")},
		{valsBuf: bytes.NewBufferString("x啊za啊xx"), buffer: bytes.NewBufferString("da啊a啊啊a啊")},
	}
	serializedPartialResults := make([]PartialResult, 3)
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

	// Check some results
	require.Equal(t, testDataNum, index)
	for i := 0; i < testDataNum; i++ {
		require.Equal(t, (*basePartialResult4GroupConcat)(serializedPartialResults[i]).valsBuf.String(), deserializedPartialResults[i].valsBuf.String())
		require.Equal(t, (*basePartialResult4GroupConcat)(serializedPartialResults[i]).buffer.String(), deserializedPartialResults[i].buffer.String())
	}
}

// TODO tests:
//
//
// partialResult4BitFunc
// partialResult4JsonArrayagg
// partialResult4JsonObjectAgg
// basePartialResult4FirstRow
// partialResult4FirstRowDecimal
// partialResult4FirstRowInt
// partialResult4FirstRowTime
// partialResult4FirstRowString
// partialResult4FirstRowFloat32
// partialResult4FirstRowFloat64
// partialResult4FirstRowDuration
// partialResult4FirstRowJSON
// partialResult4FirstRowEnum
// partialResult4FirstRowSet
