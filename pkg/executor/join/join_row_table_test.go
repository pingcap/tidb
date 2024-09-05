// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"strconv"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestHeapObjectCanMove(t *testing.T) {
	require.Equal(t, false, heapObjectsCanMove())
}

func TestFixedOffsetInRowLayout(t *testing.T) {
	require.Equal(t, 8, sizeOfNextPtr)
	require.Equal(t, 8, sizeOfLengthField)
}

func TestBitMaskInUint32(t *testing.T) {
	testData := make([]byte, 4)
	for i := 0; i < 32; i++ {
		testData[i/8] = 1 << (7 - i%8)
		testUint32 := atomic.LoadUint32((*uint32)(unsafe.Pointer(&testData[0])))
		ref := testUint32 & bitMaskInUint32[i]
		require.Equal(t, true, ref != 0)
		testData[i/8] = 0
	}
}

func TestUintptrCanHoldPointer(t *testing.T) {
	require.Equal(t, true, sizeOfUintptr >= sizeOfUnsafePointer)
}

func TestJoinTableMetaKeyMode(t *testing.T) {
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	yearTp := types.NewFieldType(mysql.TypeYear)
	durationTp := types.NewFieldType(mysql.TypeDuration)
	enumTp := types.NewFieldType(mysql.TypeEnum)
	enumWithIntFlag := types.NewFieldType(mysql.TypeEnum)
	enumWithIntFlag.AddFlag(mysql.EnumSetAsIntFlag)
	setTp := types.NewFieldType(mysql.TypeSet)
	bitTp := types.NewFieldType(mysql.TypeBit)
	jsonTp := types.NewFieldType(mysql.TypeJSON)
	floatTp := types.NewFieldType(mysql.TypeFloat)
	doubleTp := types.NewFieldType(mysql.TypeDouble)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	dateTp := types.NewFieldType(mysql.TypeDatetime)
	decimalTp := types.NewFieldType(mysql.TypeNewDecimal)

	type testCase struct {
		buildKeyIndex []int
		buildTypes    []*types.FieldType
		buildKeyTypes []*types.FieldType
		probeKeyTypes []*types.FieldType
		keyMode       keyMode
	}

	testCases := []testCase{
		// OneInt64 for some basic fixed size type
		{[]int{0}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, OneInt64},
		{[]int{0}, []*types.FieldType{yearTp}, []*types.FieldType{yearTp}, []*types.FieldType{yearTp}, OneInt64},
		{[]int{0}, []*types.FieldType{durationTp}, []*types.FieldType{durationTp}, []*types.FieldType{durationTp}, OneInt64},
		{[]int{0}, []*types.FieldType{bitTp}, []*types.FieldType{bitTp}, []*types.FieldType{bitTp}, OneInt64},
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, OneInt64},
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, OneInt64},
		{[]int{0}, []*types.FieldType{dateTp}, []*types.FieldType{dateTp}, []*types.FieldType{dateTp}, OneInt64},
		{[]int{0}, []*types.FieldType{enumWithIntFlag}, []*types.FieldType{enumWithIntFlag}, []*types.FieldType{enumWithIntFlag}, OneInt64},
		// fixed serialized key for uint = int
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{uintTp}, FixedSerializedKey},
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{intTp}, FixedSerializedKey},
		// fixed serialized key for float/double
		{[]int{0}, []*types.FieldType{floatTp}, []*types.FieldType{floatTp}, []*types.FieldType{floatTp}, FixedSerializedKey},
		{[]int{0}, []*types.FieldType{doubleTp}, []*types.FieldType{doubleTp}, []*types.FieldType{doubleTp}, FixedSerializedKey},
		// fixed serialized key for multiple fixed size join keys
		{[]int{0, 1}, []*types.FieldType{dateTp, intTp}, []*types.FieldType{dateTp, intTp}, []*types.FieldType{dateTp, intTp}, FixedSerializedKey},
		{[]int{0, 1}, []*types.FieldType{intTp, intTp}, []*types.FieldType{intTp, intTp}, []*types.FieldType{intTp, intTp}, FixedSerializedKey},
		// variable serialized key for decimal type
		{[]int{0}, []*types.FieldType{decimalTp}, []*types.FieldType{decimalTp}, []*types.FieldType{decimalTp}, VariableSerializedKey},
		// variable serialized key for string related type
		{[]int{0}, []*types.FieldType{enumTp}, []*types.FieldType{enumTp}, []*types.FieldType{enumTp}, VariableSerializedKey},
		{[]int{0}, []*types.FieldType{setTp}, []*types.FieldType{setTp}, []*types.FieldType{setTp}, VariableSerializedKey},
		{[]int{0}, []*types.FieldType{jsonTp}, []*types.FieldType{jsonTp}, []*types.FieldType{jsonTp}, VariableSerializedKey},
		{[]int{0}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, VariableSerializedKey},
		{[]int{0, 1}, []*types.FieldType{intTp, stringTp}, []*types.FieldType{intTp, stringTp}, []*types.FieldType{intTp, stringTp}, VariableSerializedKey},
	}

	for index, test := range testCases {
		meta := newTableMeta(test.buildKeyIndex, test.buildTypes, test.buildKeyTypes, test.probeKeyTypes, nil, []int{}, false)
		require.Equal(t, test.keyMode, meta.keyMode, "test index: "+strconv.Itoa(index))
	}
}

func TestJoinTableMetaKeyInlinedAndFixed(t *testing.T) {
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	yearTp := types.NewFieldType(mysql.TypeYear)
	durationTp := types.NewFieldType(mysql.TypeDuration)
	enumTp := types.NewFieldType(mysql.TypeEnum)
	enumWithIntFlag := types.NewFieldType(mysql.TypeEnum)
	enumWithIntFlag.AddFlag(mysql.EnumSetAsIntFlag)
	setTp := types.NewFieldType(mysql.TypeSet)
	bitTp := types.NewFieldType(mysql.TypeBit)
	jsonTp := types.NewFieldType(mysql.TypeJSON)
	floatTp := types.NewFieldType(mysql.TypeFloat)
	doubleTp := types.NewFieldType(mysql.TypeDouble)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	binaryStringTp := types.NewFieldType(mysql.TypeBlob)
	dateTp := types.NewFieldType(mysql.TypeDatetime)
	decimalTp := types.NewFieldType(mysql.TypeNewDecimal)

	type testCase struct {
		buildKeyIndex         []int
		buildTypes            []*types.FieldType
		buildKeyTypes         []*types.FieldType
		probeKeyTypes         []*types.FieldType
		isJoinKeysInlined     bool
		isJoinKeysFixedLength bool
		joinKeysLength        int
	}

	testCases := []testCase{
		// inlined and fixed for int related type
		{[]int{0}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, true, true, 8},
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, true, true, 8},
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, true, true, 8},
		{[]int{0}, []*types.FieldType{yearTp}, []*types.FieldType{yearTp}, []*types.FieldType{yearTp}, true, true, 8},
		{[]int{0}, []*types.FieldType{durationTp}, []*types.FieldType{durationTp}, []*types.FieldType{durationTp}, true, true, 8},
		// inlined and fixed for multiple fixed join keys
		{[]int{0, 1}, []*types.FieldType{intTp, durationTp}, []*types.FieldType{intTp, durationTp}, []*types.FieldType{intTp, durationTp}, true, true, 16},
		// inlined but not fixed for binary string
		{[]int{0}, []*types.FieldType{binaryStringTp}, []*types.FieldType{binaryStringTp}, []*types.FieldType{binaryStringTp}, true, false, -1},
		// inlined but not fixed for multiple join keys
		{[]int{0, 1}, []*types.FieldType{binaryStringTp, intTp}, []*types.FieldType{binaryStringTp, intTp}, []*types.FieldType{binaryStringTp, intTp}, true, false, -1},
		// not inlined but fixed for some fixed size join key
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{intTp}, false, true, 9},
		{[]int{0}, []*types.FieldType{enumWithIntFlag}, []*types.FieldType{enumWithIntFlag}, []*types.FieldType{enumWithIntFlag}, false, true, 8},
		{[]int{0}, []*types.FieldType{doubleTp}, []*types.FieldType{doubleTp}, []*types.FieldType{doubleTp}, false, true, 8},
		{[]int{0}, []*types.FieldType{floatTp}, []*types.FieldType{floatTp}, []*types.FieldType{floatTp}, false, true, 8},
		{[]int{0}, []*types.FieldType{dateTp}, []*types.FieldType{dateTp}, []*types.FieldType{dateTp}, false, true, 8},
		{[]int{0}, []*types.FieldType{bitTp}, []*types.FieldType{bitTp}, []*types.FieldType{bitTp}, false, true, 8},
		{[]int{0, 1}, []*types.FieldType{bitTp, intTp}, []*types.FieldType{bitTp, intTp}, []*types.FieldType{bitTp, intTp}, false, true, 16},
		// not inlined and not fixed for decimal
		{[]int{0}, []*types.FieldType{decimalTp}, []*types.FieldType{decimalTp}, []*types.FieldType{decimalTp}, false, false, -1},
		// not inlined and not fixed for non-binary string related types
		{[]int{0}, []*types.FieldType{enumTp}, []*types.FieldType{enumTp}, []*types.FieldType{enumTp}, false, false, -1},
		{[]int{0}, []*types.FieldType{setTp}, []*types.FieldType{setTp}, []*types.FieldType{setTp}, false, false, -1},
		{[]int{0}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, false, false, -1},
		{[]int{0}, []*types.FieldType{jsonTp}, []*types.FieldType{jsonTp}, []*types.FieldType{jsonTp}, false, false, -1},
		// not inlined and not fixed for multiple join keys
		{[]int{0, 1}, []*types.FieldType{decimalTp, intTp}, []*types.FieldType{decimalTp, intTp}, []*types.FieldType{decimalTp, intTp}, false, false, -1},
		{[]int{0, 1}, []*types.FieldType{enumTp, intTp}, []*types.FieldType{enumTp, intTp}, []*types.FieldType{enumTp, intTp}, false, false, -1},
		{[]int{0, 1}, []*types.FieldType{enumTp, decimalTp}, []*types.FieldType{enumTp, decimalTp}, []*types.FieldType{enumTp, decimalTp}, false, false, -1},
	}

	for index, test := range testCases {
		meta := newTableMeta(test.buildKeyIndex, test.buildTypes, test.buildKeyTypes, test.probeKeyTypes, nil, []int{}, false)
		require.Equal(t, test.isJoinKeysInlined, meta.isJoinKeysInlined, "test index: "+strconv.Itoa(index))
		require.Equal(t, test.isJoinKeysFixedLength, meta.isJoinKeysFixedLength, "test index: "+strconv.Itoa(index))
		require.Equal(t, test.joinKeysLength, meta.joinKeysLength, "test index: "+strconv.Itoa(index))
	}
}

func TestReadNullMapThreadSafe(t *testing.T) {
	// meta with usedFlag
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	metaWithUsedFlag := newTableMeta([]int{0}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, nil, []int{}, true)
	for columnIndex := 0; columnIndex < 100; columnIndex++ {
		require.Equal(t, columnIndex >= 31, metaWithUsedFlag.isReadNullMapThreadSafe(columnIndex))
	}
	// meta without usedFlag
	metaWithoutUsedFlag := newTableMeta([]int{0}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, nil, []int{}, false)
	for columnIndex := 0; columnIndex < 100; columnIndex++ {
		require.Equal(t, true, metaWithoutUsedFlag.isReadNullMapThreadSafe(columnIndex))
	}
}

func TestJoinTableMetaSerializedMode(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	binaryStringTp := types.NewFieldType(mysql.TypeBlob)
	decimalTp := types.NewFieldType(mysql.TypeNewDecimal)
	enumTp := types.NewFieldType(mysql.TypeEnum)
	enumWithIntFlag := types.NewFieldType(mysql.TypeEnum)
	enumWithIntFlag.AddFlag(mysql.EnumSetAsIntFlag)
	setTp := types.NewFieldType(mysql.TypeSet)
	jsonTp := types.NewFieldType(mysql.TypeJSON)

	type testCase struct {
		buildKeyIndex  []int
		buildTypes     []*types.FieldType
		buildKeyTypes  []*types.FieldType
		probeKeyTypes  []*types.FieldType
		serializeModes []codec.SerializeMode
	}
	testCases := []testCase{
		// normal case, no special serialize mode
		{[]int{0, 1}, []*types.FieldType{decimalTp, intTp}, []*types.FieldType{decimalTp, intTp}, []*types.FieldType{decimalTp, intTp}, []codec.SerializeMode{codec.Normal, codec.Normal}},
		// test NeedSignFlag
		{[]int{0, 1}, []*types.FieldType{uintTp, intTp}, []*types.FieldType{uintTp, intTp}, []*types.FieldType{intTp, intTp}, []codec.SerializeMode{codec.NeedSignFlag, codec.Normal}},
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{intTp}, []codec.SerializeMode{codec.NeedSignFlag}},
		// test KeepVarColumnLength
		{[]int{0, 1}, []*types.FieldType{intTp, binaryStringTp}, []*types.FieldType{intTp, binaryStringTp}, []*types.FieldType{intTp, binaryStringTp}, []codec.SerializeMode{codec.Normal, codec.KeepVarColumnLength}},
		{[]int{0}, []*types.FieldType{binaryStringTp}, []*types.FieldType{binaryStringTp}, []*types.FieldType{binaryStringTp}, []codec.SerializeMode{codec.KeepVarColumnLength}},
		// binaryString is not inlined, no need to keep var column length
		{[]int{0, 1}, []*types.FieldType{intTp, binaryStringTp}, []*types.FieldType{intTp, binaryStringTp}, []*types.FieldType{uintTp, binaryStringTp}, []codec.SerializeMode{codec.NeedSignFlag, codec.Normal}},
		// multiple var-length column, need keep var column length
		{[]int{0, 1}, []*types.FieldType{stringTp, binaryStringTp}, []*types.FieldType{stringTp, binaryStringTp}, []*types.FieldType{stringTp, binaryStringTp}, []codec.SerializeMode{codec.KeepVarColumnLength, codec.KeepVarColumnLength}},
		{[]int{0, 1}, []*types.FieldType{stringTp, decimalTp}, []*types.FieldType{stringTp, decimalTp}, []*types.FieldType{stringTp, decimalTp}, []codec.SerializeMode{codec.KeepVarColumnLength, codec.KeepVarColumnLength}},
		// set/json/decimal/enum is treated as var-length column
		{[]int{0, 1}, []*types.FieldType{setTp, jsonTp, decimalTp, enumTp}, []*types.FieldType{setTp, jsonTp, decimalTp, enumTp}, []*types.FieldType{setTp, jsonTp, decimalTp, enumTp}, []codec.SerializeMode{codec.KeepVarColumnLength, codec.KeepVarColumnLength, codec.KeepVarColumnLength, codec.KeepVarColumnLength}},
		{[]int{0, 1}, []*types.FieldType{setTp, jsonTp, decimalTp}, []*types.FieldType{setTp, jsonTp, decimalTp}, []*types.FieldType{setTp, jsonTp, decimalTp}, []codec.SerializeMode{codec.KeepVarColumnLength, codec.KeepVarColumnLength, codec.KeepVarColumnLength}},
		{[]int{0, 1}, []*types.FieldType{jsonTp, decimalTp}, []*types.FieldType{jsonTp, decimalTp}, []*types.FieldType{jsonTp, decimalTp}, []codec.SerializeMode{codec.KeepVarColumnLength, codec.KeepVarColumnLength}},
		{[]int{0, 1}, []*types.FieldType{setTp, enumTp}, []*types.FieldType{setTp, enumTp}, []*types.FieldType{setTp, enumTp}, []codec.SerializeMode{codec.KeepVarColumnLength, codec.KeepVarColumnLength}},
		// enumWithIntFlag is fix length column
		{[]int{0, 1}, []*types.FieldType{enumWithIntFlag, enumTp}, []*types.FieldType{enumWithIntFlag, enumTp}, []*types.FieldType{enumWithIntFlag, enumTp}, []codec.SerializeMode{codec.Normal, codec.Normal}},
		// single non-inlined var length column don't need keep var column length
		{[]int{0, 1}, []*types.FieldType{setTp, enumWithIntFlag}, []*types.FieldType{setTp, enumWithIntFlag}, []*types.FieldType{setTp, enumWithIntFlag}, []codec.SerializeMode{codec.Normal, codec.Normal}},
	}
	for index, test := range testCases {
		meta := newTableMeta(test.buildKeyIndex, test.buildTypes, test.buildKeyTypes, test.probeKeyTypes, nil, []int{}, false)
		for modeIndex, mode := range meta.serializeModes {
			require.Equal(t, test.serializeModes[modeIndex], mode, meta.isJoinKeysFixedLength, "test index: "+strconv.Itoa(index)+", key index: "+strconv.Itoa(modeIndex))
		}
	}
}

func TestJoinTableMetaRowColumnsOrder(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	enumWithIntFlag := types.NewFieldType(mysql.TypeEnum)
	enumWithIntFlag.AddFlag(mysql.EnumSetAsIntFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	dateTp := types.NewFieldType(mysql.TypeDatetime)
	decimalTp := types.NewFieldType(mysql.TypeNewDecimal)
	type testCase struct {
		buildKeyIndex               []int
		buildTypes                  []*types.FieldType
		buildKeyTypes               []*types.FieldType
		probeKeyTypes               []*types.FieldType
		columnsUsedByOtherCondition []int
		outputColumns               []int
		rowColumnOrder              []int
	}
	testCases := []testCase{
		// columns not used will not be converted to row format
		{[]int{0}, []*types.FieldType{stringTp, intTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, nil, []int{}, []int{}},
		// inlined keys will be converted to row format even is not needed by output columns
		{[]int{1}, []*types.FieldType{intTp, intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, nil, []int{}, []int{1}},
		// inlined keys is the first columns
		{[]int{2}, []*types.FieldType{intTp, intTp, intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, nil, []int{0, 1, 2}, []int{2, 0, 1}},
		// other condition columns will be first columns if key is not inlined
		{[]int{0}, []*types.FieldType{stringTp, stringTp, dateTp, decimalTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []int{2, 3}, []int{0, 1, 2, 3}, []int{2, 3, 0, 1}},
		{[]int{0}, []*types.FieldType{stringTp, stringTp, dateTp, decimalTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []int{3, 2}, []int{0, 1, 2, 3}, []int{3, 2, 0, 1}},
		// other condition columns will be converted to row format even if not needed by output columns
		{[]int{0}, []*types.FieldType{stringTp, stringTp, dateTp, decimalTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []int{3, 2}, []int{}, []int{3, 2}},
		// inlined keys + other condition columns + other columns
		{[]int{4}, []*types.FieldType{stringTp, stringTp, dateTp, decimalTp, intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []int{2, 0}, []int{0, 1, 2, 3, 4}, []int{4, 2, 0, 1, 3}},
		// not inlined key + no other condition, follow the same order in output columns
		{[]int{0}, []*types.FieldType{stringTp, stringTp, dateTp, decimalTp, intTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, nil, []int{4, 1, 0, 2, 3}, []int{4, 1, 0, 2, 3}},
		// not inlined key + no other condition + nil output columns
		{[]int{0}, []*types.FieldType{stringTp, stringTp, dateTp, decimalTp, intTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, nil, nil, []int{0, 1, 2, 3, 4}},
	}

	for index, test := range testCases {
		meta := newTableMeta(test.buildKeyIndex, test.buildTypes, test.buildKeyTypes, test.probeKeyTypes, test.columnsUsedByOtherCondition, test.outputColumns, false)
		require.Equal(t, len(test.rowColumnOrder), len(meta.rowColumnsOrder), "test index: "+strconv.Itoa(index))
		for rowIndex, order := range test.rowColumnOrder {
			require.Equal(t, order, meta.rowColumnsOrder[rowIndex], "test index: "+strconv.Itoa(index)+", row index: "+strconv.Itoa(rowIndex))
		}
	}
}

func TestJoinTableMetaNullMapLength(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	notNullIntTp := types.NewFieldType(mysql.TypeLonglong)
	notNullIntTp.SetFlag(mysql.NotNullFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	type testCase struct {
		buildKeyIndex []int
		buildTypes    []*types.FieldType
		buildKeyTypes []*types.FieldType
		probeKeyTypes []*types.FieldType
		outputColumns []int
		needUsedFlag  bool
		nullMapLength int
	}
	testCases := []testCase{
		// usedFlag is false
		// nullmap is 1 byte alignment
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, nil, false, 1},
		{[]int{0}, []*types.FieldType{intTp, intTp, intTp, intTp, intTp, intTp, intTp, intTp, intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, nil, false, 2},
		// even if columns is not null, nullmap is still needed
		{[]int{0}, []*types.FieldType{notNullIntTp}, []*types.FieldType{notNullIntTp}, []*types.FieldType{notNullIntTp}, nil, false, 1},
		// nullmap only used for columns that needed to be converted to rows
		{[]int{0}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []int{}, false, 0},
		{[]int{0}, []*types.FieldType{stringTp, intTp, intTp, intTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []int{}, false, 0},
		// usedFlag is true
		// the row length is at least 4 bytes, so nullmap is 1 byte alignment
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, nil, true, 1},
		{[]int{0}, []*types.FieldType{stringTp, intTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []int{1}, true, 1},
		{[]int{0}, []*types.FieldType{stringTp, intTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []int{0}, true, 1},
		{[]int{0, 1}, []*types.FieldType{stringTp, intTp}, []*types.FieldType{stringTp, intTp}, []*types.FieldType{stringTp, intTp}, []int{}, true, 1},
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{uintTp}, []int{}, true, 1},
		// the row length is not guaranteed to be at least 4 bytes, nullmap is 4 bytes alignment
		{[]int{0}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []int{}, true, 4},
		{[]int{0, 1}, []*types.FieldType{stringTp, stringTp}, []*types.FieldType{stringTp, stringTp}, []*types.FieldType{stringTp, stringTp}, []int{}, true, 4},
	}
	for index, test := range testCases {
		meta := newTableMeta(test.buildKeyIndex, test.buildTypes, test.buildKeyTypes, test.probeKeyTypes, nil, test.outputColumns, test.needUsedFlag)
		require.Equal(t, test.nullMapLength, meta.nullMapLength, "test index: "+strconv.Itoa(index))
	}
}
