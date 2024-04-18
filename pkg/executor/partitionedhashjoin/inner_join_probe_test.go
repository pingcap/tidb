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
	"sort"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func evalOtherCondition(sessCtx sessionctx.Context, probeIsLeft bool, probeRow chunk.Row, buildRow chunk.Row, shallowRow chunk.MutRow, otherCondition expression.CNFExprs) (bool, error) {
	if probeIsLeft {
		shallowRow.ShallowCopyPartialRow(0, probeRow)
		shallowRow.ShallowCopyPartialRow(probeRow.Len(), buildRow)
	} else {
		shallowRow.ShallowCopyPartialRow(0, buildRow)
		shallowRow.ShallowCopyPartialRow(buildRow.Len(), probeRow)
	}
	valid, _, err := expression.EvalBool(sessCtx.GetExprCtx().GetEvalCtx(), otherCondition, shallowRow.ToRow())
	return valid, err
}

func appendToResultChk(probeIsLeft bool, probeRow chunk.Row, buildRow chunk.Row, probeUsedColumns []int, buildUsedColumns []int, resultChunk *chunk.Chunk) {
	if probeIsLeft {
		lWide := resultChunk.AppendRowByColIdxs(probeRow, probeUsedColumns)
		resultChunk.AppendPartialRowByColIdxs(lWide, buildRow, buildUsedColumns)
	} else {
		lWide := resultChunk.AppendRowByColIdxs(buildRow, buildUsedColumns)
		resultChunk.AppendPartialRowByColIdxs(lWide, probeRow, probeUsedColumns)
	}
}

// generate inner join result using nested loop
func genInnerJoinResult(t *testing.T, sessCtx sessionctx.Context, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int, rightUsedColumns []int, otherConditions expression.CNFExprs) []*chunk.Chunk {
	resultTypes := make([]*types.FieldType, 0, len(leftUsedColumns)+len(rightUsedColumns))
	for _, colIndex := range leftUsedColumns {
		resultTypes = append(resultTypes, leftTypes[colIndex])
	}
	for _, colIndex := range rightUsedColumns {
		resultTypes = append(resultTypes, rightTypes[colIndex])
	}
	returnChks := make([]*chunk.Chunk, 0, 1)
	resultChk := chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
	shallowRowTypes := make([]*types.FieldType, 0, len(leftTypes)+len(rightTypes))
	shallowRowTypes = append(shallowRowTypes, leftTypes...)
	shallowRowTypes = append(shallowRowTypes, rightTypes...)
	shallowRow := chunk.MutRowFromTypes(shallowRowTypes)
	// for right out join, use left as build, for other join, always use right as build
	probeChunks, buildChunks := leftChunks, rightChunks
	probeKeyTypes, buildKeyTypes := leftKeyTypes, rightKeyTypes
	probeKeyIndex, buildKeyIndex := leftKeyIndex, rightKeyIndex
	probeUsedColumns, buildUsedColumns := leftUsedColumns, rightUsedColumns
	for _, probeChunk := range probeChunks {
		for probeIndex := 0; probeIndex < probeChunk.NumRows(); probeIndex++ {
			probeRow := probeChunk.GetRow(probeIndex)
			for _, buildChunk := range buildChunks {
				for buildIndex := 0; buildIndex < buildChunk.NumRows(); buildIndex++ {
					if resultChk.IsFull() {
						returnChks = append(returnChks, resultChk)
						resultChk = chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
					}
					buildRow := buildChunk.GetRow(buildIndex)
					ok, err := codec.EqualChunkRow(sessCtx.GetSessionVars().StmtCtx.TypeCtx(), probeRow, probeKeyTypes, probeKeyIndex,
						buildRow, buildKeyTypes, buildKeyIndex)
					require.NoError(t, err)
					if ok && otherConditions != nil {
						// key is match, check other condition
						ok, err = evalOtherCondition(sessCtx, true, probeRow, buildRow, shallowRow, otherConditions)
						require.NoError(t, err)
					}
					if ok {
						// construct result chunk
						appendToResultChk(true, probeRow, buildRow, probeUsedColumns, buildUsedColumns, resultChk)
					}
				}
			}
		}
	}
	if resultChk.NumRows() > 0 {
		returnChks = append(returnChks, resultChk)
	}
	return returnChks
}

func checkChunksEqual(t *testing.T, expectedChunks []*chunk.Chunk, resultChunks []*chunk.Chunk, schema []*types.FieldType) {
	expectedNum := 0
	resultNum := 0
	for _, chk := range expectedChunks {
		expectedNum += chk.NumRows()
	}
	for _, chk := range resultChunks {
		resultNum += chk.NumRows()
	}
	require.Equal(t, expectedNum, resultNum)
	if expectedNum == 0 || len(schema) == 0 {
		return
	}
	cmpFuncs := make([]chunk.CompareFunc, 0, len(schema))
	for _, colType := range schema {
		cmpFuncs = append(cmpFuncs, chunk.GetCompareFunc(colType))
	}

	resultRows := make([]chunk.Row, 0, expectedNum)
	expectedRows := make([]chunk.Row, 0, expectedNum)
	for _, chk := range expectedChunks {
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			expectedRows = append(expectedRows, row)
		}
	}
	for _, chk := range resultChunks {
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			resultRows = append(resultRows, row)
		}
	}
	cmp := func(rowI, rowJ chunk.Row) int {
		for i, cmpFunc := range cmpFuncs {
			cmp := cmpFunc(rowI, i, rowJ, i)
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	}
	sort.Slice(expectedRows, func(i, j int) bool {
		return cmp(expectedRows[i], expectedRows[j]) < 0
	})
	sort.Slice(resultRows, func(i, j int) bool {
		return cmp(resultRows[i], resultRows[j]) < 0
	})

	for i := 0; i < len(expectedRows); i++ {
		x := cmp(expectedRows[i], resultRows[i])
		if x != 0 {
			x = cmp(expectedRows[i], resultRows[i])
		}
		require.Equal(t, 0, x, "result index = "+strconv.Itoa(i))
	}
}

func testJoinProbe(t *testing.T, leftKeyIndex []int, rightKeyIndex []int, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, rightAsBuildSide bool, leftUsed []int, rightUsed []int,
	leftUsedByOtherCondition []int, rightUsedByOtherCondition []int, leftFilter expression.CNFExprs, rightFilter expression.CNFExprs,
	otherCondition expression.CNFExprs, partitionNumber int, joinType plannercore.JoinType) {
	buildKeyIndex, probeKeyIndex := leftKeyIndex, rightKeyIndex
	buildKeyTypes, probeKeyTypes := leftKeyTypes, rightKeyTypes
	buildTypes, probeTypes := leftTypes, rightTypes
	buildUsed := leftUsed
	buildUsedByOtherCondition := leftUsedByOtherCondition
	buildFilter, probeFilter := leftFilter, rightFilter
	if rightAsBuildSide {
		probeKeyIndex, buildKeyIndex = leftKeyIndex, rightKeyIndex
		probeKeyTypes, buildKeyTypes = leftKeyTypes, rightKeyTypes
		probeTypes, buildTypes = leftTypes, rightTypes
		buildUsed = rightUsed
		buildUsedByOtherCondition = rightUsedByOtherCondition
		buildFilter, probeFilter = rightFilter, leftFilter
	}
	needUsedFlag := joinType == plannercore.LeftOuterJoin && !rightAsBuildSide
	joinedTypes := make([]*types.FieldType, 0, len(leftTypes)+len(rightTypes))
	joinedTypes = append(joinedTypes, leftTypes...)
	joinedTypes = append(joinedTypes, rightTypes...)
	resultTypes := make([]*types.FieldType, 0, len(leftUsed)+len(rightUsed))
	for _, colIndex := range leftUsed {
		resultTypes = append(resultTypes, leftTypes[colIndex])
	}
	for _, colIndex := range rightUsed {
		resultTypes = append(resultTypes, rightTypes[colIndex])
	}

	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, buildUsedByOtherCondition, buildUsed, needUsedFlag)
	hashJoinCtx := &PartitionedHashJoinCtx{
		SessCtx:               mock.NewContext(),
		JoinType:              joinType,
		hashTableMeta:         meta,
		Concurrency:           uint(partitionNumber),
		BuildFilter:           buildFilter,
		ProbeFilter:           probeFilter,
		OtherCondition:        otherCondition,
		PartitionNumber:       partitionNumber,
		BuildKeyTypes:         buildKeyTypes,
		ProbeKeyTypes:         probeKeyTypes,
		RightAsBuildSide:      rightAsBuildSide,
		LUsed:                 leftUsed,
		RUsed:                 rightUsed,
		LUsedInOtherCondition: leftUsedByOtherCondition,
		RUsedInOtherCondition: rightUsedByOtherCondition,
	}
	joinProbe := NewJoinProbe(hashJoinCtx, 0, joinType, probeKeyIndex, joinedTypes, probeTypes, rightAsBuildSide)
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}
	hasNullableKey := false
	for _, buildKeyType := range buildKeyTypes {
		if !mysql.HasNotNullFlag(buildKeyType.GetFlag()) {
			hasNullableKey = true
			break
		}
	}
	builder := createRowTableBuilder(buildKeyIndex, buildSchema, meta, partitionNumber, hasNullableKey, buildFilter != nil, joinProbe.NeedScanRowTable())
	chunkNumber := 3
	buildChunks := make([]*chunk.Chunk, 0, chunkNumber)
	probeChunks := make([]*chunk.Chunk, 0, chunkNumber)
	selected := make([]bool, 0, 200)
	for i := 0; i < 200; i++ {
		if i%3 == 0 {
			selected = append(selected, true)
		} else {
			selected = append(selected, false)
		}
	}
	// check if build column can be inserted to probe column directly
	for i := 0; i < len(buildTypes); i++ {
		buildLength := chunk.GetFixedLen(buildTypes[i])
		probeLength := chunk.GetFixedLen(probeTypes[i])
		require.Equal(t, buildLength, probeLength, "build type and probe type is not compatible")
	}
	for i := 0; i < chunkNumber; i++ {
		buildChunks = append(buildChunks, chunk.GenRandomChunks(buildTypes, 200))
		probeChunk := chunk.GenRandomChunks(probeTypes, 200*2/3)
		// copy some build data to probe side, to make sure there is some matched rows
		_, err := chunk.CopySelectedJoinRowsDirect(buildChunks[i], selected, probeChunk)
		probeChunks = append(probeChunks, probeChunk)
		require.NoError(t, err)
	}

	leftChunks, rightChunks := probeChunks, buildChunks
	if !rightAsBuildSide {
		leftChunks, rightChunks = buildChunks, probeChunks
	}
	rowTables := make([]*rowTable, hashJoinCtx.PartitionNumber)
	for i := 0; i < chunkNumber; i++ {
		err := builder.processOneChunk(buildChunks[i], hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, rowTables)
		require.NoError(t, err)
	}
	builder.appendRemainingRowLocations(rowTables)
	// build hash table
	hashJoinCtx.joinHashTable = newJoinHashTable(rowTables)
	for i := 0; i < partitionNumber; i++ {
		hashJoinCtx.joinHashTable.buildHashTable(i, 0, len(rowTables[i].segments))
	}
	// probe
	resultChunks := make([]*chunk.Chunk, 0)
	joinResult := &util.HashjoinWorkerResult{
		Chk: chunk.New(resultTypes, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize),
	}
	for _, probeChunk := range probeChunks {
		err := joinProbe.SetChunkForProbe(probeChunk)
		require.NoError(t, err, "unexpected error during SetChunkForProbe")
		for !joinProbe.IsCurrentChunkProbeDone() {
			_, joinResult = joinProbe.Probe(joinResult)
			require.NoError(t, joinResult.Err, "unexpected error during join probe")
			if joinResult.Chk.IsFull() {
				resultChunks = append(resultChunks, joinResult.Chk)
				joinResult.Chk = chunk.New(resultTypes, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize)
			}
		}
	}
	if joinResult.Chk.NumRows() > 0 {
		resultChunks = append(resultChunks, joinResult.Chk)
	}
	expectedChunks := genInnerJoinResult(t, hashJoinCtx.SessCtx, leftChunks, rightChunks, leftKeyIndex, rightKeyIndex, leftTypes, rightTypes, leftKeyTypes, rightKeyTypes, leftUsed, rightUsed, otherCondition)
	checkChunksEqual(t, expectedChunks, resultChunks, resultTypes)
}

func TestInnerJoinProbeBasic(t *testing.T) {
	// todo test nullable type after builder support nullable type
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	tinyTp.AddFlag(mysql.NotNullFlag)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.NotNullFlag)
	uintTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)

	type testCase struct {
		leftKeyIndex              []int
		rightKeyIndex             []int
		leftKeyTypes              []*types.FieldType
		rightKeyTypes             []*types.FieldType
		leftTypes                 []*types.FieldType
		rightTypes                []*types.FieldType
		leftUsed                  []int
		rightUsed                 []int
		otherCondition            expression.CNFExprs
		leftUsedByOtherCondition  []int
		rightUsedByOtherCondition []int
	}

	lTypes := []*types.FieldType{intTp, stringTp, uintTp, stringTp, tinyTp}
	rTypes := []*types.FieldType{intTp, stringTp, uintTp, stringTp, tinyTp}
	rTypes1 := []*types.FieldType{uintTp, stringTp, intTp, stringTp, tinyTp}

	rightAsBuildSide := []bool{true, false}
	partitionNumber := 3

	testCases := []testCase{
		// normal case
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{0, 1, 2, 3}, []int{0, 1, 2, 3}, nil, nil, nil},
		// rightUsed is empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{0, 1, 2, 3}, []int{}, nil, nil, nil},
		// leftUsed is empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{}, []int{0, 1, 2, 3}, nil, nil, nil},
		// both left/right Used are empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{}, []int{}, nil, nil, nil},
		// int join uint
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{uintTp}, lTypes, rTypes1, []int{0, 1, 2, 3}, []int{0, 1, 2, 3}, nil, nil, nil},
		// multiple join keys
		{[]int{0, 1}, []int{0, 1}, []*types.FieldType{intTp, stringTp}, []*types.FieldType{intTp, stringTp}, lTypes, rTypes, []int{0, 1, 2, 3}, []int{0, 1, 2, 3}, nil, nil, nil},
	}

	for _, tc := range testCases {
		// inner join does not have left/right Filter
		for _, value := range rightAsBuildSide {
			testJoinProbe(t, tc.leftKeyIndex, tc.rightKeyIndex, tc.leftKeyTypes, tc.rightKeyTypes, tc.leftTypes, tc.rightTypes, value, tc.leftUsed,
				tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition, nil, nil, tc.otherCondition, partitionNumber, plannercore.InnerJoin)
		}
	}
}
