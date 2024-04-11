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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	binaryStringTp := types.NewFieldType(mysql.TypeBlob)

	testFunc := func(buildKeyIndex []int, buildTypes []*types.FieldType, buildKeyTypes []*types.FieldType, probeKeyTypes []*types.FieldType) {
		meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, []int{}, false)
		buildSchema := &expression.Schema{}
		for _, tp := range buildTypes {
			buildSchema.Append(&expression.Column{
				RetType: tp,
			})
		}
		builder := createRowTableBuilder(buildKeyIndex, buildSchema, meta, 1, true, false, false)
		chk := chunk.GenRandomChunks(buildTypes, 2049)
		hashJoinCtx := &PartitionedHashJoinCtx{
			SessCtx:         mock.NewContext(),
			PartitionNumber: 1,
			hashTableMeta:   meta,
		}
		rowTables := make([]*rowTable, hashJoinCtx.PartitionNumber)
		err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, rowTables)
		builder.appendRemainingRowLocations(rowTables)
		require.NoError(t, err, "processOneChunk returns error")
		// check valid key pos, filtered rows is not kept
		for _, seg := range rowTables[0].segments {
			for index, value := range seg.validJoinKeyPos {
				require.Equal(t, index, value, "wrong result in validJoinKeyPos")
			}
		}
		rowIndex := 0
		for i := 0; i < chk.NumRows(); i++ {
			if (builder.filterVector != nil && !builder.filterVector[i]) || (builder.nullKeyVector != nil && builder.nullKeyVector[i]) {
				continue
			}
			rowStart := rowTables[0].getRowStart(rowIndex)
			require.NotEqual(t, 0, rowStart, "row start must not be nil, index = "+strconv.Itoa(i))
			require.Equal(t, builder.serializedKeyVectorBuffer[i], meta.getKeyBytes(rowStart), "key not matched, index = "+strconv.Itoa(i))
			rowIndex++
		}
		rowStart := rowTables[0].getRowStart(rowIndex)
		require.Equal(t, uintptr(0), rowStart, "row start must be nil at the end of test")
	}

	// inlined fixed length keys
	buildKeyIndex := []int{0}
	buildTypes := []*types.FieldType{intTp, uintTp, uintTp}
	buildKeyTypes := []*types.FieldType{intTp}
	probeKeyTypes := []*types.FieldType{intTp}
	testFunc(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes)
	// inlined variable length keys
	buildKeyIndex = []int{0, 1}
	buildTypes = []*types.FieldType{binaryStringTp, intTp, uintTp}
	buildKeyTypes = []*types.FieldType{binaryStringTp, intTp}
	probeKeyTypes = []*types.FieldType{binaryStringTp, intTp}
	testFunc(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes)

	// not inlined fixed length keys
	buildKeyIndex = []int{0}
	buildTypes = []*types.FieldType{intTp, uintTp, uintTp}
	buildKeyTypes = []*types.FieldType{intTp}
	probeKeyTypes = []*types.FieldType{uintTp}
	testFunc(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes)

	// not inlined variable length keys
	buildKeyIndex = []int{0, 1}
	buildTypes = []*types.FieldType{stringTp, intTp, uintTp}
	buildKeyTypes = []*types.FieldType{stringTp, intTp}
	probeKeyTypes = []*types.FieldType{stringTp, intTp}
	testFunc(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes)
}

func TestKeepRows(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	buildKeyIndex := []int{0}
	buildTypes := []*types.FieldType{intTp, uintTp, uintTp}
	buildKeyTypes := []*types.FieldType{intTp}
	probeKeyTypes := []*types.FieldType{intTp}

	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, []int{}, false)
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}
	builder := createRowTableBuilder(buildKeyIndex, buildSchema, meta, 1, true, false, true)
	chk := chunk.GenRandomChunks(buildTypes, 2049)
	hashJoinCtx := &PartitionedHashJoinCtx{
		SessCtx:         mock.NewContext(),
		PartitionNumber: 1,
		hashTableMeta:   meta,
	}
	rowTables := make([]*rowTable, hashJoinCtx.PartitionNumber)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, rowTables)
	builder.appendRemainingRowLocations(rowTables)
	require.NoError(t, err, "processOneChunk returns error")
	// all rows should be converted to row format, even for the rows that contains null key
	require.Equal(t, uint64(2049), rowTables[0].rowCount())
	validJoinKeyRowIndex := 0
	for i := 0; i < chk.NumRows(); i++ {
		if (builder.filterVector != nil && !builder.filterVector[i]) || (builder.nullKeyVector != nil && builder.nullKeyVector[i]) {
			continue
		}
		validKeyPos := rowTables[0].getValidJoinKeyPos(validJoinKeyRowIndex)
		require.Equal(t, i, validKeyPos, "valid key pos not match, index = "+strconv.Itoa(i))
		validJoinKeyRowIndex++
	}
	validKeyPos := rowTables[0].getValidJoinKeyPos(validJoinKeyRowIndex)
	require.Equal(t, -1, validKeyPos, "validKeyPos must be -1 at the end of test")
}
