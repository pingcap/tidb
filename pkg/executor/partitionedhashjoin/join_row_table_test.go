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

package partitionedhashjoin

import (
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestFixedOffsetInRowLayout(t *testing.T) {
	require.Equal(t, 8, SizeOfNextPtr)
	require.Equal(t, 8, SizeOfLengthField)
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
		{[]int{0}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, []*types.FieldType{tinyTp}, OneInt64},
		{[]int{0}, []*types.FieldType{yearTp}, []*types.FieldType{yearTp}, []*types.FieldType{yearTp}, OneInt64},
		{[]int{0}, []*types.FieldType{durationTp}, []*types.FieldType{durationTp}, []*types.FieldType{yearTp}, OneInt64},
		{[]int{0}, []*types.FieldType{enumTp}, []*types.FieldType{enumTp}, []*types.FieldType{enumTp}, VariableSerializedKey},
		{[]int{0}, []*types.FieldType{enumWithIntFlag}, []*types.FieldType{enumWithIntFlag}, []*types.FieldType{enumWithIntFlag}, OneInt64},
		{[]int{0}, []*types.FieldType{setTp}, []*types.FieldType{setTp}, []*types.FieldType{setTp}, VariableSerializedKey},
		{[]int{0}, []*types.FieldType{bitTp}, []*types.FieldType{bitTp}, []*types.FieldType{bitTp}, OneInt64},
		{[]int{0}, []*types.FieldType{jsonTp}, []*types.FieldType{jsonTp}, []*types.FieldType{jsonTp}, VariableSerializedKey},
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, OneInt64},
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, OneInt64},
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{uintTp}, FixedSerializedKey},
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{intTp}, FixedSerializedKey},
		{[]int{0}, []*types.FieldType{floatTp}, []*types.FieldType{floatTp}, []*types.FieldType{floatTp}, FixedSerializedKey},
		{[]int{0}, []*types.FieldType{doubleTp}, []*types.FieldType{doubleTp}, []*types.FieldType{doubleTp}, FixedSerializedKey},
		{[]int{0}, []*types.FieldType{dateTp}, []*types.FieldType{dateTp}, []*types.FieldType{dateTp}, OneInt64},
		{[]int{0}, []*types.FieldType{decimalTp}, []*types.FieldType{decimalTp}, []*types.FieldType{decimalTp}, VariableSerializedKey},
		{[]int{0}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, []*types.FieldType{stringTp}, VariableSerializedKey},
		{[]int{0, 1}, []*types.FieldType{dateTp, intTp}, []*types.FieldType{dateTp, intTp}, []*types.FieldType{dateTp, intTp}, FixedSerializedKey},
		{[]int{0, 1}, []*types.FieldType{intTp, intTp}, []*types.FieldType{intTp, intTp}, []*types.FieldType{intTp, intTp}, FixedSerializedKey},
		{[]int{0, 1}, []*types.FieldType{intTp, stringTp}, []*types.FieldType{intTp, stringTp}, []*types.FieldType{intTp, stringTp}, VariableSerializedKey},
	}

	for index, test := range testCases {
		meta := newTableMeta(test.buildKeyIndex, test.buildTypes, test.buildKeyTypes, test.probeKeyTypes, nil, []int{}, false)
		require.Equal(t, test.keyMode, meta.keyMode, "test index: "+strconv.Itoa(index))
	}
}

func TestJoinTableMetaKeyInlinedAndFixed(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	//floatTp := types.NewFieldType(mysql.TypeFloat)
	doubleTp := types.NewFieldType(mysql.TypeDouble)
	//stringTp := types.NewFieldType(mysql.TypeVarString)
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
		{[]int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, true, true, 8},
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, true, true, 8},
		{[]int{0}, []*types.FieldType{uintTp}, []*types.FieldType{uintTp}, []*types.FieldType{intTp}, false, true, 9},
		{[]int{0}, []*types.FieldType{binaryStringTp}, []*types.FieldType{binaryStringTp}, []*types.FieldType{binaryStringTp}, true, false, -1},
		{[]int{0}, []*types.FieldType{doubleTp}, []*types.FieldType{doubleTp}, []*types.FieldType{doubleTp}, false, true, 8},
		{[]int{0}, []*types.FieldType{dateTp}, []*types.FieldType{dateTp}, []*types.FieldType{dateTp}, false, true, 8},
		{[]int{0}, []*types.FieldType{decimalTp}, []*types.FieldType{decimalTp}, []*types.FieldType{decimalTp}, false, false, -1},
	}

	for index, test := range testCases {
		meta := newTableMeta(test.buildKeyIndex, test.buildTypes, test.buildKeyTypes, test.probeKeyTypes, nil, []int{}, false)
		require.Equal(t, test.isJoinKeysInlined, meta.isJoinKeysInlined, "test index: "+strconv.Itoa(index))
		require.Equal(t, test.isJoinKeysFixedLength, meta.isJoinKeysFixedLength, "test index: "+strconv.Itoa(index))
		require.Equal(t, test.joinKeysLength, meta.joinKeysLength, "test index: "+strconv.Itoa(index))
	}
}
