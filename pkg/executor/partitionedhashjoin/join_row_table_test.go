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
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	doubleTp := types.NewFieldType(mysql.TypeDouble)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	dateTp := types.NewFieldType(mysql.TypeDatetime)
	decimalTp := types.NewFieldType(mysql.TypeNewDecimal)

	buildKeyIndex := make([]int, 0)
	buildTypes := make([]*types.FieldType, 0)
	buildKeyTypes := make([]*types.FieldType, 0)
	probeKeyTypes := make([]*types.FieldType, 0)
	columnsUsedByOtherCondition := make([]int, 0)
	outputColumns := make([]int, 0)
	needUsedFlag := false

	resetInputArgs := func() {
		buildKeyIndex = buildKeyIndex[:0]
		buildTypes = buildTypes[:0]
		buildKeyTypes = buildKeyTypes[:0]
		probeKeyTypes = probeKeyTypes[:0]
		columnsUsedByOtherCondition = columnsUsedByOtherCondition[:0]
		outputColumns = outputColumns[:0]
		needUsedFlag = false
	}

	// intkey = intkey, OneInt64
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, intTp)
	buildTypes = append(buildTypes, uintTp)
	buildKeyTypes = append(buildKeyTypes, intTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, OneInt64, meta.keyMode)
	// uintkey = uintkey, OneInt64
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, uintTp)
	buildTypes = append(buildTypes, intTp)
	buildKeyTypes = append(buildKeyTypes, uintTp)
	probeKeyTypes = append(probeKeyTypes, uintTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, OneInt64, meta.keyMode)
	// intkey = uintkey, FixedSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, uintTp)
	buildTypes = append(buildTypes, intTp)
	buildKeyTypes = append(buildKeyTypes, uintTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, FixedSerializedKey, meta.keyMode)
	// uintkey = intkey, FixedSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, intTp)
	buildTypes = append(buildTypes, uintTp)
	buildKeyTypes = append(buildKeyTypes, intTp)
	probeKeyTypes = append(probeKeyTypes, uintTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, FixedSerializedKey, meta.keyMode)
	// multiple intkey, FixedSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildKeyIndex = append(buildKeyIndex, 1)
	buildTypes = append(buildTypes, intTp)
	buildTypes = append(buildTypes, intTp)
	buildKeyTypes = append(buildKeyTypes, intTp)
	buildKeyTypes = append(buildKeyTypes, intTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, FixedSerializedKey, meta.keyMode)
	// doublekey = doublekey, FixedSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, doubleTp)
	buildKeyTypes = append(buildKeyTypes, doubleTp)
	probeKeyTypes = append(probeKeyTypes, doubleTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, FixedSerializedKey, meta.keyMode)
	// datekey = datekey, FixedSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, dateTp)
	buildKeyTypes = append(buildKeyTypes, dateTp)
	probeKeyTypes = append(probeKeyTypes, dateTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, FixedSerializedKey, meta.keyMode)
	// decimalkey = decimalkey, FixedSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, decimalTp)
	buildKeyTypes = append(buildKeyTypes, decimalTp)
	probeKeyTypes = append(probeKeyTypes, decimalTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, FixedSerializedKey, meta.keyMode)
	// multiple fixed key, FixedSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildKeyIndex = append(buildKeyIndex, 1)
	buildTypes = append(buildTypes, decimalTp)
	buildTypes = append(buildTypes, dateTp)
	buildKeyTypes = append(buildKeyTypes, decimalTp)
	buildKeyTypes = append(buildKeyTypes, dateTp)
	probeKeyTypes = append(probeKeyTypes, decimalTp)
	probeKeyTypes = append(probeKeyTypes, dateTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, FixedSerializedKey, meta.keyMode)
	// stringkey = stringkey, variableSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, stringTp)
	buildKeyTypes = append(buildKeyTypes, stringTp)
	probeKeyTypes = append(probeKeyTypes, stringTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, VariableSerializedKey, meta.keyMode)
	// mixed of fixed key and var length key, variableSerializedKey
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildKeyIndex = append(buildKeyIndex, 1)
	buildTypes = append(buildTypes, intTp)
	buildTypes = append(buildTypes, stringTp)
	buildKeyTypes = append(buildKeyTypes, intTp)
	buildKeyTypes = append(buildKeyTypes, stringTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	probeKeyTypes = append(probeKeyTypes, stringTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, VariableSerializedKey, meta.keyMode)
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

	buildKeyIndex := make([]int, 0)
	buildTypes := make([]*types.FieldType, 0)
	buildKeyTypes := make([]*types.FieldType, 0)
	probeKeyTypes := make([]*types.FieldType, 0)
	columnsUsedByOtherCondition := make([]int, 0)
	outputColumns := make([]int, 0)
	needUsedFlag := false

	resetInputArgs := func() {
		buildKeyIndex = buildKeyIndex[:0]
		buildTypes = buildTypes[:0]
		buildKeyTypes = buildKeyTypes[:0]
		probeKeyTypes = probeKeyTypes[:0]
		columnsUsedByOtherCondition = columnsUsedByOtherCondition[:0]
		outputColumns = outputColumns[:0]
		needUsedFlag = false
	}
	// intkey = intkey is inlined fix size key
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, intTp)
	buildKeyTypes = append(buildKeyTypes, intTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, true, meta.isJoinKeysInlined)
	require.Equal(t, true, meta.isJoinKeysFixedLength)
	require.Equal(t, 8, meta.joinKeysLength)
	// uintkey = uintkey is inlined fix size key
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, uintTp)
	buildKeyTypes = append(buildKeyTypes, uintTp)
	probeKeyTypes = append(probeKeyTypes, uintTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, true, meta.isJoinKeysInlined)
	require.Equal(t, true, meta.isJoinKeysFixedLength)
	require.Equal(t, 8, meta.joinKeysLength)
	// multiple intkey is inlined fix size key
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildKeyIndex = append(buildKeyIndex, 1)
	buildTypes = append(buildTypes, intTp)
	buildTypes = append(buildTypes, intTp)
	buildKeyTypes = append(buildKeyTypes, intTp)
	buildKeyTypes = append(buildKeyTypes, intTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, true, meta.isJoinKeysInlined)
	require.Equal(t, true, meta.isJoinKeysFixedLength)
	require.Equal(t, 16, meta.joinKeysLength)
	// intkey = uintkey is non-inlined fix size key
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, uintTp)
	buildKeyTypes = append(buildKeyTypes, uintTp)
	probeKeyTypes = append(probeKeyTypes, intTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, false, meta.isJoinKeysInlined)
	require.Equal(t, true, meta.isJoinKeysFixedLength)
	// sign flag + raw data
	require.Equal(t, 9, meta.joinKeysLength)
	// binary string is inlined variable size key
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, binaryStringTp)
	buildKeyTypes = append(buildKeyTypes, binaryStringTp)
	probeKeyTypes = append(probeKeyTypes, binaryStringTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, true, meta.isJoinKeysInlined)
	require.Equal(t, false, meta.isJoinKeysFixedLength)
	require.Equal(t, -1, meta.joinKeysLength)
	// double key is non-inlined fix size key
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, doubleTp)
	buildKeyTypes = append(buildKeyTypes, doubleTp)
	probeKeyTypes = append(probeKeyTypes, doubleTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, false, meta.isJoinKeysInlined)
	require.Equal(t, true, meta.isJoinKeysFixedLength)
	require.Equal(t, 8, meta.joinKeysLength)
	// date key is non-inlined fix size key
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, dateTp)
	buildKeyTypes = append(buildKeyTypes, dateTp)
	probeKeyTypes = append(probeKeyTypes, dateTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, false, meta.isJoinKeysInlined)
	require.Equal(t, true, meta.isJoinKeysFixedLength)
	require.Equal(t, 8, meta.joinKeysLength)
	// decimal is non-inlined variable size key
	resetInputArgs()
	buildKeyIndex = append(buildKeyIndex, 0)
	buildTypes = append(buildTypes, decimalTp)
	buildKeyTypes = append(buildKeyTypes, decimalTp)
	probeKeyTypes = append(probeKeyTypes, decimalTp)
	meta = newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, outputColumns, needUsedFlag)
	require.Equal(t, false, meta.isJoinKeysInlined)
	require.Equal(t, false, meta.isJoinKeysFixedLength)
	require.Equal(t, -1, meta.joinKeysLength)
}
